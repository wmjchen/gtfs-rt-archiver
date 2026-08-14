package rawstore

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"gtfs-rt-archiver/internal/model"
	"gtfs-rt-archiver/internal/state"
)

type Store struct {
	root    string
	staging string
}

type Staged struct {
	Path     string
	Size     int64
	SHA256   string
	TooLarge bool
	ReadErr  error
}

type Lock struct{ file *os.File }

func Open(root string) (*Store, error) {
	if err := os.MkdirAll(root, 0o750); err != nil {
		return nil, fmt.Errorf("create storage root: %w", err)
	}
	canonical, err := filepath.EvalSymlinks(root)
	if err != nil {
		return nil, fmt.Errorf("resolve storage root: %w", err)
	}
	for _, name := range []string{"staging", "raw", "parquet"} {
		dir := filepath.Join(canonical, name)
		info, statErr := os.Lstat(dir)
		switch {
		case errors.Is(statErr, os.ErrNotExist):
			if err := os.Mkdir(dir, 0o750); err != nil {
				return nil, fmt.Errorf("create storage directory: %w", err)
			}
		case statErr != nil:
			return nil, fmt.Errorf("inspect storage directory: %w", statErr)
		case info.Mode()&os.ModeSymlink != 0 || !info.IsDir():
			return nil, fmt.Errorf("storage path %s is not a real directory", name)
		}
	}
	return &Store{root: canonical, staging: filepath.Join(canonical, "staging")}, nil
}

func AcquireLock(root string) (*Lock, error) {
	if err := os.MkdirAll(root, 0o750); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(filepath.Join(root, "lock"), os.O_CREATE|os.O_RDWR|syscall.O_NOFOLLOW, 0o640)
	if err != nil {
		return nil, fmt.Errorf("open storage lock: %w", err)
	}
	info, err := f.Stat()
	if err != nil || !info.Mode().IsRegular() {
		f.Close()
		return nil, errors.New("storage lock is not a regular file")
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		f.Close()
		if errors.Is(err, syscall.EWOULDBLOCK) {
			return nil, errors.New("storage is already locked by another archiver")
		}
		return nil, fmt.Errorf("acquire storage lock: %w", err)
	}
	return &Lock{file: f}, nil
}

func (l *Lock) Close() error {
	if l == nil || l.file == nil {
		return nil
	}
	err1 := syscall.Flock(int(l.file.Fd()), syscall.LOCK_UN)
	err2 := l.file.Close()
	if err1 != nil {
		return err1
	}
	return err2
}

func (s *Store) Root() string { return s.root }

func (s *Store) Stage(r io.Reader, maxBytes int64) (*Staged, error) {
	f, err := os.CreateTemp(s.staging, ".capture-*.tmp")
	if err != nil {
		return nil, fmt.Errorf("create staging file: %w", err)
	}
	path := f.Name()
	keep := false
	defer func() {
		_ = f.Close()
		if !keep {
			_ = os.Remove(path)
		}
	}()
	h := sha256.New()
	limited := &io.LimitedReader{R: r, N: maxBytes + 1}
	n, readErr := io.Copy(io.MultiWriter(f, h), limited)
	if syncErr := f.Sync(); syncErr != nil {
		return nil, fmt.Errorf("sync staged body: %w", syncErr)
	}
	if closeErr := f.Close(); closeErr != nil {
		return nil, fmt.Errorf("close staged body: %w", closeErr)
	}
	if n > maxBytes {
		return &Staged{Size: n, TooLarge: true, ReadErr: readErr}, nil
	}
	keep = true
	return &Staged{Path: path, Size: n, SHA256: hex.EncodeToString(h.Sum(nil)), ReadErr: readErr}, nil
}

func (s *Store) Commit(staged *Staged, capture *model.Capture, loc *time.Location) error {
	if staged == nil || staged.Path == "" || staged.TooLarge {
		return errors.New("invalid staged capture")
	}
	if len(staged.SHA256) < 12 {
		return errors.New("invalid staged hash")
	}
	local := capture.ScheduledAt.In(loc)
	relDir := filepath.Join("raw", "source="+capture.SourceID, "stream="+capture.StreamID,
		"date="+capture.ArchiveDate, "hour="+local.Format("15"))
	absDir, err := s.safePath(relDir)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(absDir, 0o750); err != nil {
		return fmt.Errorf("create raw directory: %w", err)
	}
	base := capture.ID + "_" + staged.SHA256[:12]
	rawRel := filepath.Join(relDir, base+".pb")
	sidecarRel := filepath.Join(relDir, base+".json")
	rawAbs, _ := s.safePath(rawRel)
	sidecarAbs, _ := s.safePath(sidecarRel)
	if _, err := os.Lstat(rawAbs); err == nil {
		return fmt.Errorf("raw capture already exists: %s", rawRel)
	}
	capture.RawPath = filepath.ToSlash(rawRel)
	capture.SidecarPath = filepath.ToSlash(sidecarRel)
	capture.BodySHA256 = staged.SHA256
	capture.DecodedLength = staged.Size
	b, err := json.MarshalIndent(capture, "", "  ")
	if err != nil {
		return fmt.Errorf("encode sidecar: %w", err)
	}
	if strings.ContainsAny(capture.ID, `/\\`) {
		return errors.New("capture id is not safe for staging")
	}
	rawStaged := filepath.Join(s.staging, capture.ID+".pb.tmp")
	sidecarStaged := filepath.Join(s.staging, capture.ID+".json.tmp")
	if err := os.Rename(staged.Path, rawStaged); err != nil {
		return fmt.Errorf("name staged raw payload: %w", err)
	}
	staged.Path = rawStaged
	if err := os.Chmod(rawStaged, 0o640); err != nil {
		return fmt.Errorf("set staged payload permissions: %w", err)
	}
	tmp, err := os.OpenFile(sidecarStaged, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o640)
	if err != nil {
		return fmt.Errorf("create staged sidecar: %w", err)
	}
	rawCommitted := false
	defer func() {
		if !rawCommitted {
			_ = os.Remove(sidecarStaged)
		}
	}()
	if _, err := tmp.Write(append(b, '\n')); err != nil {
		tmp.Close()
		return fmt.Errorf("write sidecar: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return fmt.Errorf("sync sidecar: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close sidecar: %w", err)
	}
	if err := syncDir(s.staging); err != nil {
		return fmt.Errorf("sync staging directory: %w", err)
	}
	if err := os.Rename(rawStaged, rawAbs); err != nil {
		return fmt.Errorf("commit raw payload: %w", err)
	}
	staged.Path = ""
	rawCommitted = true
	// Make the first half of the recoverable two-file commit durable before
	// publishing the sidecar that points at it.
	if err := syncDir(absDir); err != nil {
		return fmt.Errorf("sync committed raw payload: %w", err)
	}
	if err := os.Rename(sidecarStaged, sidecarAbs); err != nil {
		return fmt.Errorf("commit sidecar: %w", err)
	}
	if err := syncDir(absDir); err != nil {
		return fmt.Errorf("sync raw directory: %w", err)
	}
	return nil
}

func (s *Store) Abort(staged *Staged) {
	if staged != nil && staged.Path != "" {
		_ = os.Remove(staged.Path)
		staged.Path = ""
	}
}

func (s *Store) Read(relative string) ([]byte, error) {
	p, err := s.safePath(filepath.FromSlash(relative))
	if err != nil {
		return nil, err
	}
	info, err := os.Lstat(p)
	if err != nil {
		return nil, err
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
		return nil, errors.New("capture path is not a regular file")
	}
	return os.ReadFile(p)
}

func (s *Store) Absolute(relative string) (string, error) {
	return s.safePath(filepath.FromSlash(relative))
}

func (s *Store) Reconcile(ctx context.Context, db *state.Store) (registered, corrupt int, err error) {
	interruptedCorrupt, finishErr := s.finishInterruptedCommits()
	if finishErr != nil {
		return 0, interruptedCorrupt, finishErr
	}
	corrupt += interruptedCorrupt
	rawRoot := filepath.Join(s.root, "raw")
	err = filepath.WalkDir(rawRoot, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if entry.Type()&os.ModeSymlink != 0 {
			if entry.IsDir() {
				return filepath.SkipDir
			}
			corrupt++
			return nil
		}
		if entry.IsDir() {
			return nil
		}
		if filepath.Ext(path) == ".pb" {
			if _, statErr := os.Stat(strings.TrimSuffix(path, ".pb") + ".json"); errors.Is(statErr, os.ErrNotExist) {
				corrupt++
			}
			return nil
		}
		if filepath.Ext(path) != ".json" {
			return nil
		}
		b, readErr := os.ReadFile(path)
		if readErr != nil {
			corrupt++
			return nil
		}
		var c model.Capture
		if json.Unmarshal(b, &c) != nil || validateCapturePaths(c) != nil {
			corrupt++
			return nil
		}
		sidecarRelative, relErr := filepath.Rel(s.root, path)
		if relErr != nil || filepath.ToSlash(sidecarRelative) != c.SidecarPath {
			corrupt++
			return nil
		}
		has, lookupErr := db.HasCapture(ctx, c.ID)
		if lookupErr != nil {
			return lookupErr
		}
		rawPath, safeErr := s.safePath(filepath.FromSlash(c.RawPath))
		if safeErr != nil || rawPath != strings.TrimSuffix(path, ".json")+".pb" {
			corrupt++
			return nil
		}
		hash, _, hashErr := hashFile(rawPath)
		if hashErr != nil || hash != c.BodySHA256 {
			corrupt++
			return nil
		}
		if has {
			return nil
		}
		started, finished := c.StartedAt, c.CompletedAt
		tick := model.Tick{ID: c.TickID, SourceID: c.SourceID, StreamID: c.StreamID,
			ScheduledAt: c.ScheduledAt, StartedAt: &started, FinishedAt: &finished,
			Result: "captured", HTTPStatus: c.HTTPStatus, Attempts: c.AttemptCount,
			ConfigFingerprint: c.ConfigFingerprint}
		if saveErr := db.SaveCapture(ctx, tick, c); saveErr != nil {
			return saveErr
		}
		registered++
		return nil
	})
	if err != nil {
		return registered, corrupt, err
	}
	// Walking sidecars detects corrupt files that exist. This second pass also
	// detects a recorded pair that is completely absent from the filesystem.
	live, liveErr := db.LiveCaptureFiles(ctx)
	if liveErr != nil {
		return registered, corrupt, liveErr
	}
	for _, capture := range live {
		missing := false
		for _, relative := range []string{capture.RawPath, capture.SidecarPath} {
			absolute, pathErr := s.safePath(filepath.FromSlash(relative))
			if pathErr != nil {
				missing = true
				break
			}
			info, statErr := os.Lstat(absolute)
			if statErr != nil || info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
				missing = true
				break
			}
		}
		if missing {
			corrupt++
		}
	}
	return
}

func (s *Store) finishInterruptedCommits() (int, error) {
	entries, err := os.ReadDir(s.staging)
	if err != nil {
		return 0, err
	}
	corrupt := 0
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json.tmp") {
			continue
		}
		sidecarStaged := filepath.Join(s.staging, entry.Name())
		b, err := os.ReadFile(sidecarStaged)
		if err != nil {
			corrupt++
			continue
		}
		var capture model.Capture
		if err := json.Unmarshal(b, &capture); err != nil || validateCapturePaths(capture) != nil || entry.Name() != capture.ID+".json.tmp" {
			corrupt++
			continue
		}
		rawFinal, err := s.safePath(filepath.FromSlash(capture.RawPath))
		if err != nil {
			corrupt++
			continue
		}
		sidecarFinal, err := s.safePath(filepath.FromSlash(capture.SidecarPath))
		if err != nil {
			corrupt++
			continue
		}
		if info, err := os.Lstat(sidecarFinal); err == nil {
			hash, _, hashErr := hashFile(rawFinal)
			if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() || hashErr != nil || hash != capture.BodySHA256 {
				corrupt++
				continue
			}
			_ = os.Remove(sidecarStaged)
			_ = os.Remove(filepath.Join(s.staging, capture.ID+".pb.tmp"))
			continue
		}
		rawSource := rawFinal
		if _, err := os.Stat(rawSource); errors.Is(err, os.ErrNotExist) {
			rawSource = filepath.Join(s.staging, capture.ID+".pb.tmp")
		}
		hash, _, err := hashFile(rawSource)
		if err != nil || hash != capture.BodySHA256 {
			corrupt++
			continue
		}
		if err := os.MkdirAll(filepath.Dir(rawFinal), 0o750); err != nil {
			return corrupt, err
		}
		if rawSource != rawFinal {
			if err := os.Rename(rawSource, rawFinal); err != nil {
				return corrupt, err
			}
			if err := syncDir(filepath.Dir(rawFinal)); err != nil {
				return corrupt, err
			}
		}
		if err := os.Rename(sidecarStaged, sidecarFinal); err != nil {
			return corrupt, err
		}
		if err := syncDir(filepath.Dir(rawFinal)); err != nil {
			return corrupt, err
		}
	}
	return corrupt, nil
}

func validateCapturePaths(c model.Capture) error {
	if c.FormatVersion != model.SidecarFormatVersion || c.ID == "" || c.SourceID == "" || c.StreamID == "" ||
		strings.ContainsAny(c.ID+c.SourceID+c.StreamID, `/\\\x00`) || len(c.BodySHA256) != sha256.Size*2 {
		return errors.New("capture identity is invalid")
	}
	if _, err := hex.DecodeString(c.BodySHA256); err != nil {
		return errors.New("capture hash is invalid")
	}
	location, err := time.LoadLocation(c.Timezone)
	if err != nil || c.ScheduledAt.IsZero() {
		return errors.New("capture timezone or scheduled time is invalid")
	}
	local := c.ScheduledAt.In(location)
	if local.Format(time.DateOnly) != c.ArchiveDate {
		return errors.New("capture archive date is inconsistent")
	}
	directory := filepath.Join("raw", "source="+c.SourceID, "stream="+c.StreamID,
		"date="+c.ArchiveDate, "hour="+local.Format("15"))
	base := c.ID + "_" + c.BodySHA256[:12]
	expectedRaw := filepath.ToSlash(filepath.Join(directory, base+".pb"))
	expectedSidecar := filepath.ToSlash(filepath.Join(directory, base+".json"))
	if c.RawPath != expectedRaw || c.SidecarPath != expectedSidecar {
		return errors.New("capture paths are inconsistent")
	}
	return nil
}

func (s *Store) RemoveExpiredStaging(olderThan time.Time) (int, error) {
	entries, err := os.ReadDir(s.staging)
	if err != nil {
		return 0, err
	}
	removed := 0
	for _, entry := range entries {
		if entry.Type()&os.ModeSymlink != 0 {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			continue
		}
		if info.ModTime().Before(olderThan) {
			path := filepath.Join(s.staging, entry.Name())
			attempted := !entry.IsDir() || strings.HasPrefix(entry.Name(), ".compaction-")
			if !attempted {
				continue
			}
			removeErr := os.Remove(path)
			if entry.IsDir() {
				removeErr = os.RemoveAll(path)
			}
			if removeErr == nil {
				removed++
			}
		}
	}
	return removed, nil
}

type DiskUsage struct {
	Total       uint64  `json:"total_bytes"`
	Free        uint64  `json:"free_bytes"`
	Used        uint64  `json:"used_bytes"`
	UsedPercent float64 `json:"used_percent"`
}

func (s *Store) DiskUsage() (DiskUsage, error) {
	var stat syscall.Statfs_t
	if err := syscall.Statfs(s.root, &stat); err != nil {
		return DiskUsage{}, err
	}
	total := stat.Blocks * uint64(stat.Bsize)
	free := stat.Bavail * uint64(stat.Bsize)
	used := total - free
	pct := float64(0)
	if total != 0 {
		pct = float64(used) * 100 / float64(total)
	}
	return DiskUsage{Total: total, Free: free, Used: used, UsedPercent: pct}, nil
}

func (s *Store) safePath(relative string) (string, error) {
	if filepath.IsAbs(relative) {
		return "", errors.New("absolute storage path is forbidden")
	}
	clean := filepath.Clean(relative)
	if clean == "." || clean == ".." || strings.HasPrefix(clean, ".."+string(filepath.Separator)) {
		return "", errors.New("storage path escapes root")
	}
	p := filepath.Join(s.root, clean)
	rel, err := filepath.Rel(s.root, p)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return "", errors.New("storage path escapes root")
	}
	current := s.root
	parts := strings.Split(rel, string(filepath.Separator))
	for _, part := range parts {
		current = filepath.Join(current, part)
		info, statErr := os.Lstat(current)
		if errors.Is(statErr, os.ErrNotExist) {
			break
		}
		if statErr != nil {
			return "", statErr
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return "", errors.New("storage path contains a symbolic link")
		}
	}
	return p, nil
}

func hashFile(path string) (string, int64, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", 0, err
	}
	defer f.Close()
	h := sha256.New()
	n, err := io.Copy(h, f)
	return hex.EncodeToString(h.Sum(nil)), n, err
}

func syncDir(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Sync()
}
