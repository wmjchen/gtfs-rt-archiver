package lifecycle

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"gtfs-rt-archiver/internal/config"
	"gtfs-rt-archiver/internal/model"
	"gtfs-rt-archiver/internal/rawstore"
	"gtfs-rt-archiver/internal/state"
)

func TestCleanupWaitsForRequiredPublicationThenDeletesExactFiles(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	cfg := config.Defaults()
	cfg.Storage.Root = root
	cfg.Storage.StateDB = filepath.Join(root, "state.sqlite")
	cfg.Storage.MetadataRetention = config.Duration{Duration: 7 * 24 * time.Hour}
	cfg.Destinations = []config.Destination{{ID: "primary", Remote: "fake:archive"}}
	cfg.Sources = []config.Source{{ID: "demo", Timezone: "UTC", Location: time.UTC, Streams: []config.Stream{{ID: "feed"}}}}
	raw, err := rawstore.Open(root)
	if err != nil {
		t.Fatal(err)
	}
	db, err := state.Open(ctx, cfg.Storage.StateDB)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	now := time.Now().UTC()
	staged, err := raw.Stage(bytes.NewReader([]byte("body")), 100)
	if err != nil {
		t.Fatal(err)
	}
	capture := model.Capture{FormatVersion: 1, ID: "capture", TickID: "tick", SourceID: "demo", StreamID: "feed",
		ScheduledAt: now, StartedAt: now, CompletedAt: now, ArchiveDate: now.Format(time.DateOnly), Timezone: "UTC",
		HTTPStatus: 200, ParseStatus: "protobuf_decode", ValidationFlags: []string{}, ConfigFingerprint: "test"}
	if err := raw.Commit(staged, &capture, time.UTC); err != nil {
		t.Fatal(err)
	}
	started, finished := now, now
	tick := model.Tick{ID: "tick", SourceID: "demo", StreamID: "feed", ScheduledAt: now, StartedAt: &started, FinishedAt: &finished, Result: "captured", ConfigFingerprint: "test"}
	if err := db.SaveCapture(ctx, tick, capture); err != nil {
		t.Fatal(err)
	}
	dataRel := "parquet/data.parquet"
	dataAbs, err := raw.Absolute(dataRel)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(dataAbs, []byte("parquet"), 0o640); err != nil {
		t.Fatal(err)
	}
	manifestRel := "parquet/manifest.json"
	manifestAbs, err := raw.Absolute(manifestRel)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(manifestAbs, []byte("{}"), 0o640); err != nil {
		t.Fatal(err)
	}
	compaction := &model.Compaction{SourceID: "demo", StreamID: "feed", ArchiveDate: capture.ArchiveDate, Revision: 1,
		Status: "ready", Directory: "parquet", DataPath: dataRel, ManifestPath: manifestRel, DataBytes: 7,
		Rows: 1, RequiredDestinations: []string{"primary"}, CreatedAt: now}
	if err := db.SaveCompaction(ctx, compaction, []model.Capture{capture}, map[string]bool{"primary": true}); err != nil {
		t.Fatal(err)
	}
	pending, err := db.PendingUploads(ctx, "primary")
	if err != nil || len(pending) != 1 {
		t.Fatalf("pending=%v err=%v", pending, err)
	}
	if err := db.MarkUploadAttempt(ctx, pending[0].ID, "uploading", "", "fake:path", now); err != nil {
		t.Fatal(err)
	}
	if err := db.MarkUploadVerified(ctx, pending[0].ID, "fake:path"); err != nil {
		t.Fatal(err)
	}
	result, err := NewRetention(&cfg, raw, db).Cleanup(ctx, now.Add(8*24*time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if result.RawFiles != 2 || result.ParquetFiles != 2 || result.SummarizedDays != 1 {
		t.Fatalf("cleanup = %+v", result)
	}
	for _, path := range []string{capture.RawPath, capture.SidecarPath, dataRel, manifestRel} {
		absolute, _ := raw.Absolute(path)
		if _, err := os.Stat(absolute); !os.IsNotExist(err) {
			t.Fatalf("retained path still exists: %s (%v)", path, err)
		}
	}
	status, err := db.Status(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if status.Ticks != 0 || status.Captures != 0 || status.SummarizedDays != 1 {
		t.Fatalf("state was not summarized: %+v", status)
	}
}
