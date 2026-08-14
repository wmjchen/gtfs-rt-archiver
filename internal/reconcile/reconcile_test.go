package reconcile

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"gtfs-rt-archiver/internal/compact"
	"gtfs-rt-archiver/internal/config"
	"gtfs-rt-archiver/internal/model"
	"gtfs-rt-archiver/internal/rawstore"
	"gtfs-rt-archiver/internal/state"
)

func TestAdoptsCompleteRevisionMissingFromState(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	cfg := config.Defaults()
	cfg.Storage.Root = root
	cfg.Storage.StateDB = filepath.Join(root, "state-one.sqlite")
	cfg.Sources = []config.Source{{ID: "demo", Timezone: "UTC", Location: time.UTC, Streams: []config.Stream{{
		ID: "feed", ExpectedKind: "mixed", URL: "https://example.test/feed", Interval: config.Duration{Duration: 30 * time.Second},
	}}}}
	optional := false
	cfg.Destinations = []config.Destination{{ID: "primary", Remote: "fake:archive"}, {ID: "secondary", Remote: "fake:backup", Required: &optional}}
	raw, err := rawstore.Open(root)
	if err != nil {
		t.Fatal(err)
	}
	one, err := state.Open(ctx, cfg.Storage.StateDB)
	if err != nil {
		t.Fatal(err)
	}
	scheduled := time.Date(2026, 8, 12, 12, 0, 0, 0, time.UTC)
	started, finished := scheduled, scheduled.Add(time.Second)
	if err := one.RecordTick(ctx, model.Tick{ID: "tick", SourceID: "demo", StreamID: "feed", ScheduledAt: scheduled,
		StartedAt: &started, FinishedAt: &finished, Result: "failed", ErrorCategory: "network", ConfigFingerprint: "test"}); err != nil {
		t.Fatal(err)
	}
	if _, err := compact.New(&cfg, raw, one).Compact(ctx, compact.Request{SourceID: "demo", StreamID: "feed", Date: "2026-08-12"}); err != nil {
		t.Fatal(err)
	}
	if err := one.Close(); err != nil {
		t.Fatal(err)
	}

	two, err := state.Open(ctx, filepath.Join(root, "state-two.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer two.Close()
	result, err := All(ctx, &cfg, raw, two)
	if err != nil {
		t.Fatal(err)
	}
	if result.CompactionsAdopted != 1 || result.Corrupt != 0 {
		t.Fatalf("result = %+v", result)
	}
	has, err := two.HasCompaction(ctx, "demo", "feed", "2026-08-12", 1)
	if err != nil || !has {
		t.Fatalf("adopted=%v err=%v", has, err)
	}
	status, err := two.Status(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if status.Pending != 2 {
		t.Fatalf("destination queues were not recovered: %+v", status)
	}
}
