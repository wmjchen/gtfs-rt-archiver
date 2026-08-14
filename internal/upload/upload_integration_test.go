package upload

import (
	"context"
	"crypto/sha256"
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

func TestPublishesDataBeforeManifestAndVerifiesBoth(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	remoteRoot := filepath.Join(root, "remote")
	logPath := filepath.Join(root, "rclone.log")
	t.Setenv("FAKE_RCLONE_LOG", logPath)
	script := filepath.Join(root, "rclone")
	scriptBody := `#!/bin/sh
set -eu
echo "$1 ${2-} ${3-}" >> "$FAKE_RCLONE_LOG"
case "$1" in
  version) echo "rclone vtest" ;;
  listremotes) echo "fake:" ;;
  copyto) target="${3#fake:}"; mkdir -p "$(dirname "$target")"; cp "$2" "$target" ;;
  hashsum) target="${3#fake:}"; sha256sum "$target" ;;
  cat) target="${2#fake:}"; cat "$target" ;;
  *) exit 2 ;;
esac
`
	if err := os.WriteFile(script, []byte(scriptBody), 0o750); err != nil {
		t.Fatal(err)
	}
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
	remoteDirectory := filepath.Join(remoteRoot, directory)
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
	status, err := db.Status(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if status.Verified != 1 {
		t.Fatalf("status = %+v", status)
	}
}
