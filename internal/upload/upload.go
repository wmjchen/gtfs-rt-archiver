package upload

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"math/rand/v2"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"gtfs-rt-archiver/internal/compact"
	"gtfs-rt-archiver/internal/config"
	"gtfs-rt-archiver/internal/model"
	"gtfs-rt-archiver/internal/state"
)

type Observer interface {
	ObserveUpload(destination, result string, duration time.Duration)
}
type noopObserver struct{}

func (noopObserver) ObserveUpload(string, string, time.Duration) {}

type Uploader struct {
	cfg      *config.Config
	state    *state.Store
	log      *slog.Logger
	observer Observer
}

func New(cfg *config.Config, store *state.Store, log *slog.Logger, observer Observer) *Uploader {
	if observer == nil {
		observer = noopObserver{}
	}
	return &Uploader{cfg: cfg, state: store, log: log, observer: observer}
}

func (u *Uploader) Preflight(ctx context.Context) error {
	if len(u.cfg.Destinations) == 0 {
		return nil
	}
	if _, err := os.Stat(u.cfg.Rclone.ConfigFile); err != nil {
		return fmt.Errorf("rclone config: %w", err)
	}
	cmdCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	cmd := exec.CommandContext(cmdCtx, u.cfg.Rclone.Binary, "version")
	cmd.Env = minimalEnv()
	var stdout bytes.Buffer
	cmd.Stdout = &limitedWriter{writer: &stdout, remaining: 4096}
	cmd.Stderr = io.Discard
	if err := cmd.Run(); err != nil {
		if cmdCtx.Err() != nil {
			return errors.New("rclone preflight failed: timeout")
		}
		return fmt.Errorf("rclone preflight failed: %s", classifyError(err, ""))
	}
	first, _, _ := strings.Cut(strings.TrimSpace(stdout.String()), "\n")
	u.log.Info("rclone available", "version", first)
	var remotes bytes.Buffer
	listCtx, listCancel := context.WithTimeout(ctx, 15*time.Second)
	defer listCancel()
	list := exec.CommandContext(listCtx, u.cfg.Rclone.Binary, "listremotes", "--config", u.cfg.Rclone.ConfigFile)
	list.Env = minimalEnv()
	list.Stdout = &limitedWriter{writer: &remotes, remaining: 64 << 10}
	list.Stderr = io.Discard
	if err := list.Run(); err != nil {
		if listCtx.Err() != nil {
			return errors.New("rclone remote validation failed: timeout")
		}
		return fmt.Errorf("rclone remote validation failed: %s", classifyError(err, ""))
	}
	configured := map[string]bool{}
	for _, line := range strings.Split(remotes.String(), "\n") {
		configured[strings.TrimSpace(line)] = true
	}
	for _, destination := range u.cfg.Destinations {
		name, _, _ := strings.Cut(destination.Remote, ":")
		if !configured[name+":"] {
			return fmt.Errorf("rclone remote for destination %s is not configured", destination.ID)
		}
	}
	return nil
}

func (u *Uploader) ProcessPending(ctx context.Context, destination string) (int, error) {
	pending, err := u.state.PendingUploads(ctx, destination)
	if err != nil {
		return 0, err
	}
	processed := 0
	for _, item := range pending {
		if err := ctx.Err(); err != nil {
			return processed, err
		}
		dest, ok := findDestination(u.cfg, item.DestinationID)
		if !ok {
			if err := u.state.MarkUploadFailed(ctx, item.ID, "retry", "destination_not_configured", time.Now().Add(backoff(item.AttemptCount+1))); err != nil {
				return processed, err
			}
			processed++
			continue
		}
		compaction, err := u.state.CompactionByID(ctx, item.CompactionID)
		if err != nil {
			return processed, err
		}
		started := time.Now()
		remoteDir := remoteJoin(dest.Remote, compaction.Directory)
		if err := u.state.MarkUploadAttempt(ctx, item.ID, "uploading", "", remoteDir, time.Now()); err != nil {
			return processed, err
		}
		err = u.publish(ctx, *dest, compaction, remoteDir)
		if err == nil {
			if err := u.state.MarkUploadVerified(ctx, item.ID, remoteDir); err != nil {
				return processed, err
			}
			u.observer.ObserveUpload(dest.ID, "verified", time.Since(started))
			u.log.Info("revision uploaded and verified", "destination", dest.ID, "source", compaction.SourceID,
				"stream", compaction.StreamID, "date", compaction.ArchiveDate, "revision", compaction.Revision)
		} else {
			category := errorCategory(err)
			status := "retry"
			next := time.Now().Add(backoff(item.AttemptCount + 1))
			if isPermanent(category) {
				status = "permanent_failure"
				next = time.Now().AddDate(100, 0, 0)
			}
			if stateErr := u.state.MarkUploadFailed(context.WithoutCancel(ctx), item.ID, status, category, next); stateErr != nil {
				return processed, stateErr
			}
			u.observer.ObserveUpload(dest.ID, status, time.Since(started))
			u.log.Warn("revision upload failed", "destination", dest.ID, "category", category,
				"source", compaction.SourceID, "stream", compaction.StreamID,
				"date", compaction.ArchiveDate, "revision", compaction.Revision)
		}
		processed++
	}
	return processed, nil
}

func (u *Uploader) publish(ctx context.Context, dest config.Destination, compaction *model.Compaction, remoteDir string) error {
	if err := compact.VerifyDirectory(u.cfg.Storage.Root, compaction); err != nil {
		return fmt.Errorf("local_integrity: %w", err)
	}
	manifestAbs := filepath.Join(u.cfg.Storage.Root, filepath.FromSlash(compaction.ManifestPath))
	b, err := os.ReadFile(manifestAbs)
	if err != nil {
		return fmt.Errorf("local_integrity: %w", err)
	}
	var manifest model.Manifest
	if err := json.Unmarshal(b, &manifest); err != nil {
		return fmt.Errorf("local_integrity: %w", err)
	}
	for _, artifact := range manifest.Files {
		local := filepath.Join(filepath.Dir(manifestAbs), filepath.Base(artifact.RelativePath))
		remote := remoteJoin(remoteDir, filepath.Base(artifact.RelativePath))
		if err := u.copyAndVerify(ctx, local, remote, artifact.SHA256); err != nil {
			return err
		}
	}
	manifestHash := sha256.Sum256(b)
	return u.copyAndVerify(ctx, manifestAbs, remoteJoin(remoteDir, "manifest.json"), hex.EncodeToString(manifestHash[:]))
}

func (u *Uploader) copyAndVerify(ctx context.Context, local, remote, expectedHash string) error {
	args := []string{"copyto", local, remote, "--immutable", "--checksum", "--retries", "3", "--low-level-retries", "10", "--config", u.cfg.Rclone.ConfigFile}
	if err := u.run(ctx, args...); err != nil {
		return err
	}
	actual, err := u.remoteHash(ctx, remote)
	if err != nil {
		return err
	}
	if !strings.EqualFold(actual, expectedHash) {
		return errors.New("remote_integrity: sha256_mismatch")
	}
	return nil
}

func (u *Uploader) remoteHash(ctx context.Context, remote string) (string, error) {
	var output bytes.Buffer
	err := u.runInto(ctx, &output, "hashsum", "SHA-256", remote, "--config", u.cfg.Rclone.ConfigFile)
	if err == nil {
		scanner := bufio.NewScanner(&output)
		if scanner.Scan() {
			fields := strings.Fields(scanner.Text())
			if len(fields) > 0 && sha256Pattern.MatchString(fields[0]) {
				return strings.ToLower(fields[0]), nil
			}
		}
	}
	// Some object backends do not expose SHA-256. Stream the remote object back
	// through rclone so verification never degrades to a size-only comparison.
	cmdCtx, cancel := context.WithTimeout(ctx, u.cfg.Rclone.ProcessTimeout.Duration)
	defer cancel()
	cmd := exec.CommandContext(cmdCtx, u.cfg.Rclone.Binary, "cat", remote, "--config", u.cfg.Rclone.ConfigFile)
	cmd.Env = minimalEnv()
	stdout, pipeErr := cmd.StdoutPipe()
	if pipeErr != nil {
		return "", fmt.Errorf("remote_verify: pipe")
	}
	var stderr bytes.Buffer
	cmd.Stderr = &limitedWriter{writer: &stderr, remaining: 8192}
	if err := cmd.Start(); err != nil {
		return "", fmt.Errorf("remote_verify: %s", classifyError(err, stderr.String()))
	}
	h := sha256.New()
	if _, err := io.Copy(h, stdout); err != nil {
		_ = cmd.Wait()
		return "", errors.New("remote_verify: read")
	}
	if err := cmd.Wait(); err != nil {
		if cmdCtx.Err() != nil {
			return "", errors.New("remote_verify: timeout")
		}
		return "", fmt.Errorf("remote_verify: %s", classifyError(err, stderr.String()))
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

func (u *Uploader) run(ctx context.Context, args ...string) error {
	return u.runInto(ctx, io.Discard, args...)
}

func (u *Uploader) runInto(ctx context.Context, stdout io.Writer, args ...string) error {
	cmdCtx, cancel := context.WithTimeout(ctx, u.cfg.Rclone.ProcessTimeout.Duration)
	defer cancel()
	cmd := exec.CommandContext(cmdCtx, u.cfg.Rclone.Binary, args...)
	cmd.Env = minimalEnv()
	var stderr bytes.Buffer
	cmd.Stdout = &limitedWriter{writer: stdout, remaining: 64 << 10}
	cmd.Stderr = &limitedWriter{writer: &stderr, remaining: 16 << 10}
	if err := cmd.Run(); err != nil {
		if cmdCtx.Err() != nil {
			return errors.New("timeout")
		}
		return errors.New(classifyError(err, stderr.String()))
	}
	return nil
}

var sha256Pattern = regexp.MustCompile(`^[a-fA-F0-9]{64}$`)

func findDestination(cfg *config.Config, id string) (*config.Destination, bool) {
	for i := range cfg.Destinations {
		if cfg.Destinations[i].ID == id {
			return &cfg.Destinations[i], true
		}
	}
	return nil, false
}

func remoteJoin(base string, elements ...string) string {
	base = strings.TrimRight(base, "/")
	clean := make([]string, 0, len(elements))
	for _, element := range elements {
		clean = append(clean, strings.TrimLeft(filepath.ToSlash(element), "/"))
	}
	return base + "/" + path.Join(clean...)
}

func backoff(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	hours := math.Pow(2, float64(min(attempt-1, 6)))
	maximum := time.Duration(hours * float64(5*time.Minute))
	if maximum > 6*time.Hour {
		maximum = 6 * time.Hour
	}
	return time.Duration(rand.Int64N(int64(maximum) + 1))
}

func errorCategory(err error) string {
	if err == nil {
		return ""
	}
	v := strings.ToLower(err.Error())
	for _, category := range []string{"stream_not_configured", "dataset_invalid", "local_integrity", "remote_integrity", "authentication", "permission", "remote_not_found", "configuration", "timeout", "throttled", "network"} {
		if strings.Contains(v, category) {
			return category
		}
	}
	return "rclone_failure"
}

func isPermanent(category string) bool {
	// Credential, permission, remote, and configuration failures can all be
	// repaired outside the process, so keep retrying them. Integrity failures
	// require explicit operator investigation to avoid overwriting evidence.
	return category == "local_integrity" || category == "remote_integrity" || category == "dataset_invalid"
}

func classifyError(err error, stderr string) string {
	if errors.Is(err, context.DeadlineExceeded) {
		return "timeout"
	}
	v := strings.ToLower(stderr)
	switch {
	case strings.Contains(v, "immutable") && (strings.Contains(v, "modified") || strings.Contains(v, "different")):
		return "remote_integrity"
	case strings.Contains(v, "didn't find section") || strings.Contains(v, "config file") && strings.Contains(v, "not found"):
		return "configuration"
	case strings.Contains(v, "access denied") || strings.Contains(v, "forbidden") || strings.Contains(v, "permission denied"):
		return "permission"
	case strings.Contains(v, "invalidaccesskey") || strings.Contains(v, "authentication") || strings.Contains(v, "unauthorized"):
		return "authentication"
	case strings.Contains(v, "couldn't find remote") || strings.Contains(v, "unknown remote"):
		return "remote_not_found"
	case strings.Contains(v, "too many requests") || strings.Contains(v, "rate limit"):
		return "throttled"
	case strings.Contains(v, "timeout") || strings.Contains(v, "deadline"):
		return "timeout"
	default:
		return "network"
	}
}

func minimalEnv() []string {
	// Backend credentials are commonly supplied as RCLONE_* or provider
	// environment variables. They are inherited by the child but never logged.
	return os.Environ()
}

type limitedWriter struct {
	writer    io.Writer
	remaining int64
}

func (w *limitedWriter) Write(p []byte) (int, error) {
	original := len(p)
	if w.remaining > 0 {
		write := p
		if int64(len(write)) > w.remaining {
			write = write[:w.remaining]
		}
		_, _ = w.writer.Write(write)
		w.remaining -= int64(len(write))
	}
	return original, nil
}
