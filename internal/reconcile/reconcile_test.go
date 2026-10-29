package reconcile

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	gtfs "github.com/MobilityData/gtfs-realtime-bindings/golang/gtfs"
	"google.golang.org/protobuf/proto"

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

func TestAdoptsPartialRevisionAfterStateRebuild(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	cfg := config.Defaults()
	cfg.Storage.Root = root
	cfg.Storage.StateDB = filepath.Join(root, "state-one.sqlite")
	cfg.Sources = []config.Source{{ID: "demo", Timezone: "UTC", Location: time.UTC, Streams: []config.Stream{{
		ID: "feed", ExpectedKind: "mixed", URL: "https://example.test/feed", Interval: config.Duration{Duration: 30 * time.Second},
	}}}}
	raw, err := rawstore.Open(root)
	if err != nil {
		t.Fatal(err)
	}
	one, err := state.Open(ctx, cfg.Storage.StateDB)
	if err != nil {
		t.Fatal(err)
	}
	version, vehicleID := "2.0", "v1"
	body, err := proto.Marshal(&gtfs.FeedMessage{
		Header: &gtfs.FeedHeader{GtfsRealtimeVersion: &version},
		Entity: []*gtfs.FeedEntity{{Id: &vehicleID, Vehicle: &gtfs.VehiclePosition{}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	first := saveReconcileCapture(t, ctx, raw, one, body, 1)
	second := saveReconcileCapture(t, ctx, raw, one, body, 2)
	secondRaw, err := raw.Absolute(second.RawPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(secondRaw); err != nil {
		t.Fatal(err)
	}
	if _, err := compact.New(&cfg, raw, one).Compact(ctx, compact.Request{SourceID: "demo", StreamID: "feed", Date: "2026-08-12"}); err != nil {
		t.Fatalf("partial compaction: %v", err)
	}
	if err := one.Close(); err != nil {
		t.Fatal(err)
	}

	// A rebuilt state DB can only reconstruct the capture whose raw payload
	// survives. Adoption must still accept the revision that intentionally
	// excluded the other capture.
	two, err := state.Open(ctx, filepath.Join(root, "state-two.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer two.Close()
	result, err := All(ctx, &cfg, raw, two)
	if err != nil {
		t.Fatalf("adopt partial revision after state rebuild: %v", err)
	}
	if result.CompactionsAdopted != 1 {
		t.Fatalf("result = %+v", result)
	}
	has, err := two.HasCompaction(ctx, "demo", "feed", "2026-08-12", 1)
	if err != nil || !has {
		t.Fatalf("adopted=%v err=%v", has, err)
	}
	if _, err := two.CapturesForDay(ctx, "demo", "feed", "2026-08-12"); err != nil {
		t.Fatal(err)
	}
	if first.ID != "capture-1" {
		t.Fatalf("unexpected first capture id %q", first.ID)
	}
}

func saveReconcileCapture(t *testing.T, ctx context.Context, raw *rawstore.Store, db *state.Store, body []byte, second int) model.Capture {
	t.Helper()
	scheduled := time.Date(2026, 8, 12, 12, 0, second, 0, time.UTC)
	staged, err := raw.Stage(bytes.NewReader(body), 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	count := int32(1)
	capture := model.Capture{
		FormatVersion: 1, ID: fmt.Sprintf("capture-%d", second), TickID: fmt.Sprintf("tick-%d", second),
		SourceID: "demo", StreamID: "feed", ExpectedKind: "vehicle_position",
		ScheduledAt: scheduled, StartedAt: scheduled, CompletedAt: scheduled.Add(time.Millisecond),
		ArchiveDate: "2026-08-12", Timezone: "UTC", SanitizedURL: "https://example.test/feed",
		HTTPStatus: 200, AdvertisedLength: int64(len(body)), EncodedLength: int64(len(body)),
		TransportComplete: true, ParseStatus: "valid", EntityCount: &count, ValidationFlags: []string{},
		ConfigFingerprint: "test", ApplicationVersion: "test", ProtobufRevision: "test",
	}
	if err := raw.Commit(staged, &capture, time.UTC); err != nil {
		t.Fatal(err)
	}
	started, finished := capture.StartedAt, capture.CompletedAt
	tick := model.Tick{ID: capture.TickID, SourceID: capture.SourceID, StreamID: capture.StreamID,
		ScheduledAt: scheduled, StartedAt: &started, FinishedAt: &finished, Result: "captured", Attempts: 1, ConfigFingerprint: "test"}
	if err := db.SaveCapture(ctx, tick, capture); err != nil {
		t.Fatal(err)
	}
	return capture
}

func TestAdoptsFlattenedTripUpdateRevision(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	cfg := config.Defaults()
	cfg.Storage.Root = root
	cfg.Storage.StateDB = filepath.Join(root, "state-one.sqlite")
	cfg.Sources = []config.Source{{ID: "demo", Timezone: "UTC", Location: time.UTC, Streams: []config.Stream{{
		ID: "trips", ExpectedKind: "trip_update", URL: "https://example.test/trips", Interval: config.Duration{Duration: 30 * time.Second},
	}}}}
	raw, err := rawstore.Open(root)
	if err != nil {
		t.Fatal(err)
	}
	one, err := state.Open(ctx, cfg.Storage.StateDB)
	if err != nil {
		t.Fatal(err)
	}
	// One valid trip-update capture.
	version := "2.0"
	ts := uint64(1786600000)
	tripID, entityID, stopID := "trip-1", "e1", "stop-a"
	seq := uint32(10)
	body, err := proto.Marshal(&gtfs.FeedMessage{
		Header: &gtfs.FeedHeader{GtfsRealtimeVersion: &version, Timestamp: &ts},
		Entity: []*gtfs.FeedEntity{{Id: &entityID, TripUpdate: &gtfs.TripUpdate{
			Trip:           &gtfs.TripDescriptor{TripId: &tripID},
			StopTimeUpdate: []*gtfs.TripUpdate_StopTimeUpdate{{StopSequence: &seq, StopId: &stopID}}}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	scheduled := time.Date(2026, 8, 12, 12, 0, 0, 0, time.UTC)
	staged, err := raw.Stage(bytes.NewReader(body), 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	count := int32(1)
	started, finished := scheduled, scheduled.Add(time.Second)
	capture := model.Capture{
		FormatVersion: 1, ID: "capture-1", TickID: "tick-1", SourceID: "demo", StreamID: "trips",
		ExpectedKind: "trip_update", ScheduledAt: scheduled, StartedAt: started, CompletedAt: finished,
		ArchiveDate: "2026-08-12", Timezone: "UTC", SanitizedURL: "https://example.test/trips", HTTPStatus: 200,
		TransportComplete: true, ParseStatus: "valid", EntityCount: &count, ValidationFlags: []string{},
		ConfigFingerprint: "test", ApplicationVersion: "test", ProtobufRevision: "test",
	}
	if err := raw.Commit(staged, &capture, time.UTC); err != nil {
		t.Fatal(err)
	}
	if err := one.SaveCapture(ctx, model.Tick{ID: "tick-1", SourceID: "demo", StreamID: "trips", ScheduledAt: scheduled,
		StartedAt: &started, FinishedAt: &finished, Result: "captured", Attempts: 1, ConfigFingerprint: "test"}, capture); err != nil {
		t.Fatal(err)
	}
	if _, err := compact.New(&cfg, raw, one).Compact(ctx, compact.Request{SourceID: "demo", StreamID: "trips", Date: "2026-08-12"}); err != nil {
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
		t.Fatalf("adoption of v2 trip-update revision failed: %+v", result)
	}
	has, err := two.HasCompaction(ctx, "demo", "trips", "2026-08-12", 1)
	if err != nil || !has {
		t.Fatalf("adopted=%v err=%v", has, err)
	}
}
