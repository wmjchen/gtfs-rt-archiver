package rawstore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"gtfs-rt-archiver/internal/model"
	"gtfs-rt-archiver/internal/state"
)

func TestStageLimitAndCommit(t *testing.T) {
	store, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	tooBig, err := store.Stage(bytes.NewReader([]byte("12345")), 4)
	if err != nil {
		t.Fatal(err)
	}
	if !tooBig.TooLarge || tooBig.Path != "" {
		t.Fatalf("unexpected oversized stage: %+v", tooBig)
	}

	staged, err := store.Stage(bytes.NewReader([]byte("payload")), 100)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 8, 12, 21, 1, 2, 3, time.UTC)
	c := &model.Capture{FormatVersion: 1, ID: "capture_1", SourceID: "source", StreamID: "stream", ArchiveDate: "2026-08-12", CompletedAt: now}
	if err := store.Commit(staged, c, time.UTC); err != nil {
		t.Fatal(err)
	}
	if c.RawPath == "" || c.SidecarPath == "" {
		t.Fatal("paths were not populated")
	}
	b, err := store.Read(c.RawPath)
	if err != nil {
		t.Fatal(err)
	}
	if string(b) != "payload" {
		t.Fatalf("payload = %q", b)
	}
	if _, err := os.Stat(store.root + "/" + c.SidecarPath); err != nil {
		t.Fatal(err)
	}
}

func TestReconcileFinishesInterruptedRawCommit(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	store, err := Open(root)
	if err != nil {
		t.Fatal(err)
	}
	db, err := state.Open(ctx, filepath.Join(root, "state.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	body := []byte("captured response")
	h := sha256.Sum256(body)
	now := time.Date(2026, 8, 12, 12, 0, 0, 0, time.UTC)
	capture := model.Capture{
		FormatVersion: 1, ID: "interrupted", TickID: "tick-interrupted", SourceID: "demo", StreamID: "feed",
		ScheduledAt: now, StartedAt: now, CompletedAt: now, ArchiveDate: "2026-08-12", Timezone: "UTC",
		HTTPStatus: 200, BodySHA256: hex.EncodeToString(h[:]), DecodedLength: int64(len(body)), ParseStatus: "protobuf_decode",
		RawPath:           "raw/source=demo/stream=feed/date=2026-08-12/hour=12/interrupted_" + hex.EncodeToString(h[:])[:12] + ".pb",
		SidecarPath:       "raw/source=demo/stream=feed/date=2026-08-12/hour=12/interrupted_" + hex.EncodeToString(h[:])[:12] + ".json",
		ConfigFingerprint: "test", ApplicationVersion: "test", ProtobufRevision: "test",
	}
	rawFinal, err := store.Absolute(capture.RawPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Dir(rawFinal), 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(rawFinal, body, 0o640); err != nil {
		t.Fatal(err)
	}
	sidecar, _ := json.Marshal(capture)
	if err := os.WriteFile(filepath.Join(root, "staging", capture.ID+".json.tmp"), sidecar, 0o640); err != nil {
		t.Fatal(err)
	}
	registered, corrupt, err := store.Reconcile(ctx, db)
	if err != nil {
		t.Fatal(err)
	}
	if registered != 1 || corrupt != 0 {
		t.Fatalf("registered=%d corrupt=%d", registered, corrupt)
	}
	if _, err := os.Stat(filepath.Join(root, filepath.FromSlash(capture.SidecarPath))); err != nil {
		t.Fatal(err)
	}
}

func TestReconcileFlagsRecordedCaptureWithMissingFiles(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	store, err := Open(root)
	if err != nil {
		t.Fatal(err)
	}
	db, err := state.Open(ctx, filepath.Join(root, "state.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	now := time.Now().UTC()
	started, finished := now, now
	tick := model.Tick{ID: "tick-missing", SourceID: "demo", StreamID: "feed", ScheduledAt: now,
		StartedAt: &started, FinishedAt: &finished, Result: "captured", ConfigFingerprint: "test"}
	capture := model.Capture{ID: "capture-missing", TickID: tick.ID, SourceID: "demo", StreamID: "feed",
		ExpectedKind: "mixed", ScheduledAt: now, StartedAt: now, CompletedAt: now,
		ArchiveDate: now.Format(time.DateOnly), Timezone: "UTC", SanitizedURL: "https://example.test/feed",
		HTTPStatus: 200, RawPath: "raw/missing.pb", SidecarPath: "raw/missing.json",
		TransportComplete: true, ParseStatus: "valid", ValidationFlags: []string{}, ConfigFingerprint: "test",
		ApplicationVersion: "test", ProtobufRevision: "test"}
	if err := db.SaveCapture(ctx, tick, capture); err != nil {
		t.Fatal(err)
	}
	registered, corrupt, err := store.Reconcile(ctx, db)
	if err != nil {
		t.Fatal(err)
	}
	if registered != 0 || corrupt != 1 {
		t.Fatalf("registered=%d corrupt=%d", registered, corrupt)
	}
}

func TestExclusiveLock(t *testing.T) {
	root := t.TempDir()
	one, err := AcquireLock(root)
	if err != nil {
		t.Fatal(err)
	}
	defer one.Close()
	if _, err := AcquireLock(root); err == nil {
		t.Fatal("second lock unexpectedly succeeded")
	}
}

func TestOpenAndLockRejectSymlinkTargets(t *testing.T) {
	root := t.TempDir()
	outside := t.TempDir()
	if err := os.Symlink(outside, filepath.Join(root, "staging")); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(root); err == nil {
		t.Fatal("symlinked staging directory was accepted")
	}

	lockRoot := t.TempDir()
	outsideFile := filepath.Join(t.TempDir(), "outside-lock")
	if err := os.WriteFile(outsideFile, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outsideFile, filepath.Join(lockRoot, "lock")); err != nil {
		t.Fatal(err)
	}
	if _, err := AcquireLock(lockRoot); err == nil {
		t.Fatal("symlinked lock file was accepted")
	}
}
