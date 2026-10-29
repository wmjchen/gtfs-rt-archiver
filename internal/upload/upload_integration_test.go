package upload

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"gtfs-rt-archiver/internal/config"
	"gtfs-rt-archiver/internal/model"
	"gtfs-rt-archiver/internal/state"
)

// writeFakeRclone returns the path of a fake rclone binary. It logs
// "$1 $2 $3" per invocation to $FAKE_RCLONE_LOG, understands version /
// listremotes / copyto / hashsum / cat, and — when $FAKE_FAIL_UNDER is set —
// fails any copyto whose target (after stripping "fake:") lives under that
// directory while the sentinel file exists; remove the file to heal it.
func writeFakeRclone(t *testing.T, root string) string {
	t.Helper()
	script := filepath.Join(root, "rclone")
	scriptBody := `#!/bin/sh
set -eu
echo "$1 ${2-} ${3-}" >> "$FAKE_RCLONE_LOG"
case "$1" in
  version) echo "rclone vtest" ;;
  listremotes) echo "fake:" ;;
  copyto)
    target="${3#fake:}"
    if [ -n "${FAKE_FAIL_UNDER:-}" ] && [ -f "${FAKE_FAIL_FILE:-/nonexistent}" ]; then
      case "$target" in "$FAKE_FAIL_UNDER"/*) exit 3 ;; esac
    fi
    mkdir -p "$(dirname "$target")"; cp "$2" "$target" ;;
  hashsum) target="${3#fake:}"; sha256sum "$target" ;;
  cat) target="${2#fake:}"; cat "$target" ;;
  *) exit 2 ;;
esac
`
	if err := os.WriteFile(script, []byte(scriptBody), 0o750); err != nil {
		t.Fatal(err)
	}
	return script
}

func TestPublishesDataBeforeManifestAndVerifiesBoth(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	remoteRoot := filepath.Join(root, "remote")
	logPath := filepath.Join(root, "rclone.log")
	t.Setenv("FAKE_RCLONE_LOG", logPath)
	script := writeFakeRclone(t, root)
	configFile := filepath.Join(root, "rclone.conf")
	if err := os.WriteFile(configFile, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	cfg := config.Defaults()
	cfg.Storage.Root = root
	cfg.Storage.StateDB = filepath.Join(root, "state.sqlite")
	cfg.Rclone.Binary = script
	cfg.Rclone.ConfigFile = configFile
	cfg.Destinations = []config.Destination{{ID: "primary", Remote: "fake:" + remoteRoot}}
	db, err := state.Open(ctx, cfg.Storage.StateDB)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	directory := filepath.Join("parquet", "format=v1", "source=demo", "stream=feed", "date=2026-08-12", "revision=1")
	localDirectory := filepath.Join(root, directory)
	if err := os.MkdirAll(localDirectory, 0o750); err != nil {
		t.Fatal(err)
	}
	data := []byte("parquet fixture")
	dataHash := sha256.Sum256(data)
	dataName := "data-" + hex.EncodeToString(dataHash[:]) + ".parquet"
	if err := os.WriteFile(filepath.Join(localDirectory, dataName), data, 0o640); err != nil {
		t.Fatal(err)
	}
	manifest := model.Manifest{ManifestVersion: 1, DatasetStatus: "ready", SourceID: "demo", StreamID: "feed",
		ExpectedKind: "alert", SanitizedURL: "https://example.test/feed",
		ArchiveDate: "2026-08-12", Revision: 1, FormatVersion: 1, SchemaVersion: 1, CapturedResponses: 1,
		RequiredDestinations: []string{"primary"}, Destinations: []model.ManifestDestination{{ID: "primary", Required: true}},
		InputCaptures: []model.InputCapture{{ID: "capture", SHA256: "fixture", Bytes: 7}},
		Files:         []model.Artifact{{RelativePath: dataName, Bytes: int64(len(data)), SHA256: hex.EncodeToString(dataHash[:]), Rows: 1}}}
	manifestBytes, _ := json.Marshal(manifest)
	if err := os.WriteFile(filepath.Join(localDirectory, "manifest.json"), manifestBytes, 0o640); err != nil {
		t.Fatal(err)
	}
	compaction := &model.Compaction{SourceID: "demo", StreamID: "feed", ArchiveDate: "2026-08-12", Revision: 1,
		Status: "ready", Directory: filepath.ToSlash(directory), DataPath: filepath.ToSlash(filepath.Join(directory, dataName)),
		ManifestPath: filepath.ToSlash(filepath.Join(directory, "manifest.json")), DataSHA256: hex.EncodeToString(dataHash[:]),
		DataBytes: int64(len(data)), Rows: 1, RequiredDestinations: []string{"primary"}, CreatedAt: time.Now()}
	if err := db.SaveCompaction(ctx, compaction, nil, map[string]bool{"primary": true}); err != nil {
		t.Fatal(err)
	}
	uploader := New(&cfg, db, slog.New(slog.NewTextHandler(os.Stderr, nil)), nil)
	if err := uploader.Preflight(ctx); err != nil {
		t.Fatal(err)
	}
	processed, err := uploader.ProcessPending(ctx, "primary")
	if err != nil {
		t.Fatal(err)
	}
	if processed != 1 {
		t.Fatalf("processed = %d", processed)
	}
	enc := base64.RawURLEncoding.EncodeToString([]byte("https://example.test/feed"))
	remoteDirectory := filepath.Join(remoteRoot, "service_alerts", "date=2026-08-12", "base64url="+enc, "revision=1")
	for _, name := range []string{dataName, "manifest.json"} {
		if _, err := os.Stat(filepath.Join(remoteDirectory, name)); err != nil {
			t.Fatal(err)
		}
	}
	commands, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatal(err)
	}
	text := string(commands)
	if strings.Index(text, "copyto "+filepath.Join(localDirectory, dataName)) > strings.Index(text, "copyto "+filepath.Join(localDirectory, "manifest.json")) {
		t.Fatal("manifest was uploaded before data")
	}
	if !strings.Contains(text, "fake:"+filepath.ToSlash(remoteDirectory)+"/manifest.json") {
		t.Fatalf("hive manifest path missing from rclone argv: %s", text)
	}
	status, err := db.Status(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if status.Verified != 1 {
		t.Fatalf("status = %+v", status)
	}
}

func TestRemovedStreamStaysPendingWithoutPublishing(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	logPath := filepath.Join(root, "rclone.log")
	t.Setenv("FAKE_RCLONE_LOG", logPath)
	script := writeFakeRclone(t, root)
	configFile := filepath.Join(root, "rclone.conf")
	if err := os.WriteFile(configFile, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	cfg := config.Defaults()
	cfg.Storage.Root = root
	cfg.Storage.StateDB = filepath.Join(root, "state.sqlite")
	cfg.Rclone.Binary = script
	cfg.Rclone.ConfigFile = configFile
	cfg.Destinations = []config.Destination{{ID: "primary", Remote: "fake:" + filepath.Join(root, "remote")}}
	// Deliberately no cfg.Sources: the stream left configuration, and the
	// pre-feature manifest fixture below carries no sanitized_url.
	db, err := state.Open(ctx, cfg.Storage.StateDB)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	directory := filepath.Join("parquet", "format=v1", "source=gone", "stream=feed", "date=2026-08-12", "revision=1")
	localDirectory := filepath.Join(root, directory)
	if err := os.MkdirAll(localDirectory, 0o750); err != nil {
		t.Fatal(err)
	}
	manifest := model.Manifest{ManifestVersion: 1, DatasetStatus: "no_captured_responses", SourceID: "gone", StreamID: "feed",
		ExpectedKind: "alert", ArchiveDate: "2026-08-12", Revision: 1, FormatVersion: 1, SchemaVersion: 1,
		RequiredDestinations: []string{"primary"}, Destinations: []model.ManifestDestination{{ID: "primary", Required: true}},
		Files: []model.Artifact{}}
	manifestBytes, _ := json.Marshal(manifest)
	if err := os.WriteFile(filepath.Join(localDirectory, "manifest.json"), manifestBytes, 0o640); err != nil {
		t.Fatal(err)
	}
	compaction := &model.Compaction{SourceID: "gone", StreamID: "feed", ArchiveDate: "2026-08-12", Revision: 1,
		Status: "ready", Directory: filepath.ToSlash(directory),
		ManifestPath:         filepath.ToSlash(filepath.Join(directory, "manifest.json")),
		RequiredDestinations: []string{"primary"}, CreatedAt: time.Now()}
	if err := db.SaveCompaction(ctx, compaction, nil, map[string]bool{"primary": true}); err != nil {
		t.Fatal(err)
	}
	uploader := New(&cfg, db, slog.New(slog.NewTextHandler(os.Stderr, nil)), nil)
	if err := uploader.Preflight(ctx); err != nil {
		t.Fatal(err)
	}
	processed, err := uploader.ProcessPending(ctx, "primary")
	if err != nil {
		t.Fatal(err)
	}
	if processed != 1 {
		t.Fatalf("processed = %d", processed)
	}
	// Nothing was attempted against the remote.
	if logBytes, err := os.ReadFile(logPath); err != nil || strings.Contains(string(logBytes), "copyto") {
		t.Fatalf("unexpected rclone copyto (err=%v): %s", err, logBytes)
	}
	// The row was rescheduled (not verified, not permanently failed), so it is
	// not due again right now.
	pending, err := db.PendingUploads(ctx, "primary")
	if err != nil {
		t.Fatal(err)
	}
	if len(pending) != 0 {
		t.Fatalf("pending = %+v", pending)
	}
}

func TestFailingDestinationDoesNotBlockOthers(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	primaryRoot := filepath.Join(root, "primary")
	secondaryRoot := filepath.Join(root, "secondary")
	logPath := filepath.Join(root, "rclone.log")
	failFile := filepath.Join(root, "fail-secondary")
	t.Setenv("FAKE_RCLONE_LOG", logPath)
	t.Setenv("FAKE_FAIL_UNDER", secondaryRoot)
	t.Setenv("FAKE_FAIL_FILE", failFile)
	if err := os.WriteFile(failFile, []byte{}, 0o640); err != nil {
		t.Fatal(err)
	}
	script := writeFakeRclone(t, root)
	configFile := filepath.Join(root, "rclone.conf")
	if err := os.WriteFile(configFile, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	cfg := config.Defaults()
	cfg.Storage.Root = root
	cfg.Storage.StateDB = filepath.Join(root, "state.sqlite")
	cfg.Rclone.Binary = script
	cfg.Rclone.ConfigFile = configFile
	cfg.Destinations = []config.Destination{
		{ID: "primary", Remote: "fake:" + primaryRoot},
		{ID: "secondary", Remote: "fake:" + secondaryRoot},
	}
	db, err := state.Open(ctx, cfg.Storage.StateDB)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	directory := filepath.Join("parquet", "format=v1", "source=demo", "stream=feed", "date=2026-08-12", "revision=1")
	localDirectory := filepath.Join(root, directory)
	if err := os.MkdirAll(localDirectory, 0o750); err != nil {
		t.Fatal(err)
	}
	data := []byte("parquet fixture")
	dataHash := sha256.Sum256(data)
	dataName := "data-" + hex.EncodeToString(dataHash[:]) + ".parquet"
	stuTotal := int64(1)
	if err := os.WriteFile(filepath.Join(localDirectory, dataName), data, 0o640); err != nil {
		t.Fatal(err)
	}
	manifest := model.Manifest{ManifestVersion: 1, DatasetStatus: "ready", SourceID: "demo", StreamID: "feed",
		ExpectedKind: "trip_update", SanitizedURL: "https://example.test/feed",
		ArchiveDate: "2026-08-12", Revision: 1, FormatVersion: 1, SchemaVersion: 2, CapturedResponses: 1,
		StopTimeUpdateTotal:  &stuTotal,
		RequiredDestinations: []string{"primary", "secondary"},
		Destinations:         []model.ManifestDestination{{ID: "primary", Required: true}, {ID: "secondary", Required: true}},
		InputCaptures:        []model.InputCapture{{ID: "capture", SHA256: "fixture", Bytes: 7}},
		Files:                []model.Artifact{{RelativePath: dataName, Bytes: int64(len(data)), SHA256: hex.EncodeToString(dataHash[:]), Rows: 1}}}
	manifestBytes, _ := json.Marshal(manifest)
	if err := os.WriteFile(filepath.Join(localDirectory, "manifest.json"), manifestBytes, 0o640); err != nil {
		t.Fatal(err)
	}
	compaction := &model.Compaction{SourceID: "demo", StreamID: "feed", ArchiveDate: "2026-08-12", Revision: 1,
		Status: "ready", Directory: filepath.ToSlash(directory), DataPath: filepath.ToSlash(filepath.Join(directory, dataName)),
		ManifestPath: filepath.ToSlash(filepath.Join(directory, "manifest.json")), DataSHA256: hex.EncodeToString(dataHash[:]),
		DataBytes: int64(len(data)), Rows: 1, RequiredDestinations: []string{"primary", "secondary"}, CreatedAt: time.Now()}
	if err := db.SaveCompaction(ctx, compaction, nil, map[string]bool{"primary": true, "secondary": true}); err != nil {
		t.Fatal(err)
	}
	uploader := New(&cfg, db, slog.New(slog.NewTextHandler(os.Stderr, nil)), nil)
	if err := uploader.Preflight(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := uploader.ProcessPending(ctx, "primary"); err != nil {
		t.Fatal(err)
	}
	if _, err := uploader.ProcessPending(ctx, "secondary"); err != nil {
		t.Fatal(err)
	}
	enc := base64.RawURLEncoding.EncodeToString([]byte("https://example.test/feed"))
	hiveDirLocal := filepath.Join("trip_updates", "date=2026-08-12", "base64url="+enc, "revision=1")
	// The healthy destination landed the complete revision, manifest last.
	for _, name := range []string{dataName, "manifest.json"} {
		if _, err := os.Stat(filepath.Join(primaryRoot, hiveDirLocal, name)); err != nil {
			t.Fatalf("primary missing %s: %v", name, err)
		}
	}
	// The failing destination never received a manifest (failed on the data).
	if _, err := os.Stat(filepath.Join(secondaryRoot, hiveDirLocal, "manifest.json")); !os.IsNotExist(err) {
		t.Fatalf("secondary should lack a manifest, stat err = %v", err)
	}
	// Failure isolation: the primary upload is still verified.
	status, err := db.Status(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if status.Verified != 1 {
		t.Fatalf("verified = %+v", status)
	}
}

func TestBackfillPublishesToHivePath(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	logPath := filepath.Join(root, "rclone.log")
	t.Setenv("FAKE_RCLONE_LOG", logPath)
	script := writeFakeRclone(t, root)
	configFile := filepath.Join(root, "rclone.conf")
	if err := os.WriteFile(configFile, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	cfg := config.Defaults()
	cfg.Storage.Root = root
	cfg.Storage.StateDB = filepath.Join(root, "state.sqlite")
	cfg.Rclone.Binary = script
	cfg.Rclone.ConfigFile = configFile
	cfg.Destinations = []config.Destination{{ID: "primary", Remote: "fake:" + filepath.Join(root, "primary")}}
	db, err := state.Open(ctx, cfg.Storage.StateDB)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	directory := filepath.Join("parquet", "format=v1", "source=demo", "stream=feed", "date=2026-08-12", "revision=1")
	localDirectory := filepath.Join(root, directory)
	if err := os.MkdirAll(localDirectory, 0o750); err != nil {
		t.Fatal(err)
	}
	data := []byte("parquet fixture")
	dataHash := sha256.Sum256(data)
	dataName := "data-" + hex.EncodeToString(dataHash[:]) + ".parquet"
	if err := os.WriteFile(filepath.Join(localDirectory, dataName), data, 0o640); err != nil {
		t.Fatal(err)
	}
	manifest := model.Manifest{ManifestVersion: 1, DatasetStatus: "ready", SourceID: "demo", StreamID: "feed",
		ExpectedKind: "vehicle_position", SanitizedURL: "https://example.test/feed",
		ArchiveDate: "2026-08-12", Revision: 1, FormatVersion: 1, SchemaVersion: 1, CapturedResponses: 1,
		RequiredDestinations: []string{"primary"},
		Destinations:         []model.ManifestDestination{{ID: "primary", Required: true}},
		InputCaptures:        []model.InputCapture{{ID: "capture", SHA256: "fixture", Bytes: 7}},
		Files:                []model.Artifact{{RelativePath: dataName, Bytes: int64(len(data)), SHA256: hex.EncodeToString(dataHash[:]), Rows: 1}}}
	manifestBytes, _ := json.Marshal(manifest)
	if err := os.WriteFile(filepath.Join(localDirectory, "manifest.json"), manifestBytes, 0o640); err != nil {
		t.Fatal(err)
	}
	compaction := &model.Compaction{SourceID: "demo", StreamID: "feed", ArchiveDate: "2026-08-12", Revision: 1,
		Status: "ready", Directory: filepath.ToSlash(directory), DataPath: filepath.ToSlash(filepath.Join(directory, dataName)),
		ManifestPath: filepath.ToSlash(filepath.Join(directory, "manifest.json")), DataSHA256: hex.EncodeToString(dataHash[:]),
		DataBytes: int64(len(data)), Rows: 1, RequiredDestinations: []string{"primary"}, CreatedAt: time.Now()}
	if err := db.SaveCompaction(ctx, compaction, nil, map[string]bool{"primary": true}); err != nil {
		t.Fatal(err)
	}
	uploader := New(&cfg, db, slog.New(slog.NewTextHandler(os.Stderr, nil)), nil)
	if err := uploader.Preflight(ctx); err != nil {
		t.Fatal(err)
	}
	if processed, err := uploader.ProcessPending(ctx, "primary"); err != nil || processed != 1 {
		t.Fatalf("primary processed = %d, err = %v", processed, err)
	}
	// A new destination appears; backfill must publish to its hive path.
	cfg.Destinations = append(cfg.Destinations, config.Destination{ID: "secondary", Remote: "fake:" + filepath.Join(root, "secondary")})
	if queued, err := db.EnsureDestinationBackfill(ctx, "secondary"); err != nil || queued == 0 {
		t.Fatalf("backfill queued = %d, err = %v", queued, err)
	}
	if processed, err := uploader.ProcessPending(ctx, "secondary"); err != nil || processed != 1 {
		t.Fatalf("secondary processed = %d, err = %v", processed, err)
	}
	enc := base64.RawURLEncoding.EncodeToString([]byte("https://example.test/feed"))
	manifestPath := filepath.Join(root, "secondary", "vehicle_positions", "date=2026-08-12", "base64url="+enc, "revision=1", "manifest.json")
	if _, err := os.Stat(manifestPath); err != nil {
		t.Fatalf("backfilled manifest missing at hive path: %v", err)
	}
}
