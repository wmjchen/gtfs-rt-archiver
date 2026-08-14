package state

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"gtfs-rt-archiver/internal/model"
)

func TestCaptureRoundTripPreservesUint64FeedTimestamp(t *testing.T) {
	ctx := context.Background()
	db, err := Open(ctx, filepath.Join(t.TempDir(), "state.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	now := time.Date(2026, 8, 12, 12, 0, 0, 0, time.UTC)
	feedTimestamp := ^uint64(0)
	started, finished := now, now.Add(time.Second)
	tick := model.Tick{ID: "tick", SourceID: "demo", StreamID: "feed", ScheduledAt: now,
		StartedAt: &started, FinishedAt: &finished, Result: "captured", Attempts: 1, ConfigFingerprint: "test"}
	capture := model.Capture{ID: "capture", TickID: tick.ID, SourceID: "demo", StreamID: "feed",
		ExpectedKind: "mixed", ScheduledAt: now, StartedAt: started, CompletedAt: finished,
		ArchiveDate: "2026-08-12", Timezone: "UTC", SanitizedURL: "https://example.test/feed",
		HTTPStatus: 200, RawPath: "raw/capture.pb", SidecarPath: "raw/capture.json",
		TransportComplete: true, ParseStatus: "valid", FeedTimestamp: &feedTimestamp,
		ValidationFlags: []string{}, ConfigFingerprint: "test", ApplicationVersion: "test", ProtobufRevision: "test"}
	if err := db.SaveCapture(ctx, tick, capture); err != nil {
		t.Fatal(err)
	}
	got, err := db.CapturesForDay(ctx, "demo", "feed", "2026-08-12")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].FeedTimestamp == nil || *got[0].FeedTimestamp != feedTimestamp {
		t.Fatalf("feed timestamp did not round trip: %+v", got)
	}
}

func TestDayStatsIncludesEveryFailureCategory(t *testing.T) {
	ctx := context.Background()
	db, err := Open(ctx, filepath.Join(t.TempDir(), "state.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	start := time.Date(2026, 8, 12, 0, 0, 0, 0, time.UTC)
	for index, category := range []string{"http_status", "body_too_large", "timeout"} {
		at := start.Add(time.Duration(index) * time.Minute)
		if err := db.RecordTick(ctx, model.Tick{ID: category, SourceID: "demo", StreamID: "feed", ScheduledAt: at,
			Result: "failed", ErrorCategory: category, ConfigFingerprint: "test"}); err != nil {
			t.Fatal(err)
		}
	}
	stats, err := db.DayStats(ctx, "demo", "feed", start, start.Add(24*time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if stats.Scheduled != 3 || stats.HTTPFailures != 1 || stats.NetworkFailures != 1 || stats.FailureCategoryCounts["body_too_large"] != 1 {
		t.Fatalf("unexpected day stats: %+v", stats)
	}
}

func TestBackfillIsOptionalAndRetirementReleasesRequiredPublication(t *testing.T) {
	ctx := context.Background()
	db, err := Open(ctx, filepath.Join(t.TempDir(), "state.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	now := time.Now().UTC()
	compaction := &model.Compaction{SourceID: "demo", StreamID: "feed", ArchiveDate: "2026-08-12", Revision: 1,
		Status: "ready", Directory: "parquet/revision=1", ManifestPath: "parquet/revision=1/manifest.json",
		RequiredDestinations: []string{"primary"}, CreatedAt: now}
	if err := db.SaveCompaction(ctx, compaction, nil, map[string]bool{"primary": true}); err != nil {
		t.Fatal(err)
	}
	queued, err := db.EnsureDestinationBackfill(ctx, "secondary")
	if err != nil || queued != 1 {
		t.Fatalf("queued=%d err=%v", queued, err)
	}
	secondary, err := db.PendingUploads(ctx, "secondary")
	if err != nil || len(secondary) != 1 || secondary[0].Required {
		t.Fatalf("historical queue must be optional: %+v err=%v", secondary, err)
	}
	retired, err := db.RetireDestination(ctx, "primary", "remote decommissioned")
	if err != nil || retired != 1 {
		t.Fatalf("retired=%d err=%v", retired, err)
	}
	got, err := db.CompactionByID(ctx, compaction.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.PublishedAt == nil || got.Status != "published" {
		t.Fatalf("retirement did not release publication: %+v", got)
	}
	status, err := db.Status(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if status.Retired != 1 || status.Pending != 1 {
		t.Fatalf("unexpected status: %+v", status)
	}
}
