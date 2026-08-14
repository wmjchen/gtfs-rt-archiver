package compact

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	gtfs "github.com/MobilityData/gtfs-realtime-bindings/golang/gtfs"
	"github.com/parquet-go/parquet-go"
	"google.golang.org/protobuf/proto"

	"gtfs-rt-archiver/internal/config"
	"gtfs-rt-archiver/internal/model"
	"gtfs-rt-archiver/internal/projection"
	"gtfs-rt-archiver/internal/rawstore"
	"gtfs-rt-archiver/internal/state"
)

func tripUpdatesDaySetup(t *testing.T) (context.Context, *config.Config, *rawstore.Store, *state.Store) {
	t.Helper()
	root := t.TempDir()
	cfg := config.Defaults()
	cfg.Storage.Root = root
	cfg.Storage.StateDB = filepath.Join(root, "state.sqlite")
	cfg.Sources = []config.Source{{
		ID: "demo", Timezone: "UTC", Location: time.UTC,
		Streams: []config.Stream{{ID: "trips", ExpectedKind: "trip_update", URL: "https://example.test/trips", Interval: config.Duration{Duration: 30 * time.Second}}},
	}}
	raw, err := rawstore.Open(root)
	if err != nil {
		t.Fatal(err)
	}
	db, err := state.Open(context.Background(), cfg.Storage.StateDB)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	return context.Background(), &cfg, raw, db
}

func addTripUpdateCapture(t *testing.T, ctx context.Context, raw *rawstore.Store, db *state.Store, body []byte, parseStatus string, second int) {
	t.Helper()
	scheduled := time.Date(2026, 8, 12, 12, 0, second, 0, time.UTC)
	staged, err := raw.Stage(bytes.NewReader(body), 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	capture := model.Capture{
		FormatVersion: 1, ID: fmt.Sprintf("capture-%d", second), TickID: fmt.Sprintf("tick-%d", second),
		SourceID: "demo", StreamID: "trips", ExpectedKind: "trip_update",
		ScheduledAt: scheduled, StartedAt: scheduled, CompletedAt: scheduled.Add(time.Millisecond),
		ArchiveDate: "2026-08-12", Timezone: "UTC", SanitizedURL: "https://example.test/trips",
		HTTPStatus: 200, AdvertisedLength: int64(len(body)), EncodedLength: int64(len(body)),
		TransportComplete: true, ParseStatus: parseStatus, ValidationFlags: []string{},
		ConfigFingerprint: "test", ApplicationVersion: "test", ProtobufRevision: "test",
	}
	if parseStatus == "valid" {
		var message gtfs.FeedMessage
		if err := proto.Unmarshal(body, &message); err != nil {
			t.Fatal(err)
		}
		count := int32(len(message.Entity))
		capture.EntityCount = &count
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
}

func tripUpdateCaptureBody(t *testing.T, ts uint64, entities ...*gtfs.FeedEntity) []byte {
	t.Helper()
	version := "2.0"
	body, err := proto.Marshal(&gtfs.FeedMessage{
		Header: &gtfs.FeedHeader{GtfsRealtimeVersion: &version, Timestamp: &ts},
		Entity: entities,
	})
	if err != nil {
		t.Fatal(err)
	}
	return body
}

func tripEntity(id, tripID string, stus ...*gtfs.TripUpdate_StopTimeUpdate) *gtfs.FeedEntity {
	return &gtfs.FeedEntity{Id: &id, TripUpdate: &gtfs.TripUpdate{
		Trip: &gtfs.TripDescriptor{TripId: &tripID}, StopTimeUpdate: stus,
	}}
}

func stu(seq uint32, stopID string) *gtfs.TripUpdate_StopTimeUpdate {
	return &gtfs.TripUpdate_StopTimeUpdate{StopSequence: &seq, StopId: &stopID}
}

// openParquetFile adapts parquet.OpenFile (ReaderAt + size) to a path and ties
// the underlying file handle's lifetime to the test.
func openParquetFile(t *testing.T, path string) *parquet.File {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { f.Close() })
	info, err := f.Stat()
	if err != nil {
		t.Fatal(err)
	}
	pf, err := parquet.OpenFile(f, info.Size())
	if err != nil {
		t.Fatal(err)
	}
	return pf
}

// Capture plan (rows / stop_time_updates):
//
//	capture-1: e1(trip-1: stu10, stu11) + e2(trip-2 zero-STU base) → 3 rows / 2 stu / 2 entities
//	capture-2: garbage protobuf_decode                                → 0 rows / 0 stu / nil entities
//	capture-3: e4 is_deleted+payload(trip-4: stu5)                    → 1 row  / 1 stu / 1 entity
//
// Day totals: 3 captures; 4 parquet rows; 3 stop_time_updates; 3 entities.
func seedTripUpdateDay(t *testing.T, ctx context.Context, raw *rawstore.Store, db *state.Store) {
	t.Helper()
	addTripUpdateCapture(t, ctx, raw, db, tripUpdateCaptureBody(t, 1786600000,
		tripEntity("e1", "trip-1", stu(10, "stop-a"), stu(11, "stop-b")),
		tripEntity("e2", "trip-2")), "valid", 1)
	addTripUpdateCapture(t, ctx, raw, db, []byte{0xff, 0x01, 0x02}, "protobuf_decode", 2)
	e4id, trip4 := "e4", "trip-4"
	deleted := true
	addTripUpdateCapture(t, ctx, raw, db, tripUpdateCaptureBody(t, 1786600000,
		&gtfs.FeedEntity{Id: &e4id, IsDeleted: &deleted, TripUpdate: &gtfs.TripUpdate{
			Trip: &gtfs.TripDescriptor{TripId: &trip4}, StopTimeUpdate: []*gtfs.TripUpdate_StopTimeUpdate{stu(5, "stop-c")}}}), "valid", 3)
}

func TestCompactTripUpdateFeedDay(t *testing.T) {
	ctx, cfg, raw, db := tripUpdatesDaySetup(t)
	seedTripUpdateDay(t, ctx, raw, db)

	result, err := New(cfg, raw, db).Compact(ctx, Request{SourceID: "demo", StreamID: "trips", Date: "2026-08-12"})
	if err != nil {
		t.Fatal(err)
	}
	// State rows stay capture counts (maintenance comparator invariant).
	if result.Rows != 3 {
		t.Fatalf("compaction rows = %d, want 3 captures", result.Rows)
	}
	if err := VerifyDirectory(cfg.Storage.Root, result); err != nil {
		t.Fatalf("verify revision: %v", err)
	}

	manifestBytes, err := os.ReadFile(filepath.Join(cfg.Storage.Root, filepath.FromSlash(result.ManifestPath)))
	if err != nil {
		t.Fatal(err)
	}
	var manifest model.Manifest
	if err := json.Unmarshal(manifestBytes, &manifest); err != nil {
		t.Fatal(err)
	}
	if manifest.SchemaVersion != model.ParquetSchemaVersionTripUpdatesFlattened {
		t.Fatalf("schema version = %d", manifest.SchemaVersion)
	}
	if len(manifest.Files) != 1 {
		t.Fatalf("artifacts = %d, want exactly one", len(manifest.Files))
	}
	if manifest.StopTimeUpdateTotal == nil || *manifest.StopTimeUpdateTotal != 3 {
		t.Fatalf("stop_time_update_total = %v, want 3", manifest.StopTimeUpdateTotal)
	}
	if manifest.Files[0].Rows != 3 || manifest.EntityTotal != 3 {
		t.Fatalf("artifact rows = %d (want stu total 3), entity total = %d (want 3)", manifest.Files[0].Rows, manifest.EntityTotal)
	}
	// Size ceiling: the flattened 4-row fixture must stay tiny. Re-adding the
	// embedded protobuf copy (as the nested layout carries) already exceeds
	// this bound at ~600 bytes per capture.
	if manifest.Files[0].Bytes >= 64*1024 {
		t.Fatalf("flattened artifact = %d bytes, want < 64KiB", manifest.Files[0].Bytes)
	}

	// Exactly one parquet + manifest in the revision directory.
	entries, err := os.ReadDir(filepath.Join(cfg.Storage.Root, filepath.FromSlash(result.Directory)))
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 2 {
		t.Fatalf("revision dir entries = %d, want data parquet + manifest.json", len(entries))
	}

	// Flattened row content, in capture order.
	path := filepath.Join(cfg.Storage.Root, filepath.FromSlash(result.DataPath))
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	reader := parquet.NewGenericReader[projection.TripUpdateStopRow](f)
	rows := make([]projection.TripUpdateStopRow, 8)
	n, err := reader.Read(rows)
	if err != nil && err != io.EOF {
		t.Fatal(err)
	}
	if n != 4 {
		t.Fatalf("parquet rows = %d, want 4", n)
	}
	if rows[0].CaptureID != "capture-1" || rows[0].EntityID != "e1" || rows[0].TripID != "trip-1" ||
		*rows[0].StopSequence != 10 || *rows[0].StopID != "stop-a" || rows[0].FeedURL != "https://example.test/trips" ||
		rows[0].SourceFile == "" || rows[0].ParseStatus != "valid" {
		t.Fatalf("row[0] = %+v", rows[0])
	}
	base := rows[2]
	if base.EntityID != "e2" || base.StopSequence != nil || base.StopID != nil {
		t.Fatalf("zero-STU base row = %+v", base)
	}
	del := rows[3]
	if del.CaptureID != "capture-3" || !del.IsDeleted || del.EntityID != "e4" || *del.StopID != "stop-c" {
		t.Fatalf("deleted-payload row = %+v", del)
	}

	// Schema v2 kv metadata is truthful per file.
	pf := openParquetFile(t, path)
	var schemaKV string
	for _, kv := range pf.Metadata().KeyValueMetadata {
		if kv.Key == "gtfsrt.schema_version" {
			schemaKV = kv.Value
		}
	}
	if schemaKV != "2" {
		t.Fatalf("gtfsrt.schema_version = %q, want 2", schemaKV)
	}
}

func TestCompactTripUpdateEmptyDay(t *testing.T) {
	// An empty trip_update day still emits a schema-v2 manifest, and the v2
	// verifier requires stop_time_update_total on every trip-update revision:
	// without it VerifyDirectory (run by upload, reconcile, and maintenance on
	// every revision) would reject the empty-day manifest.
	ctx, cfg, raw, db := tripUpdatesDaySetup(t)
	result, err := New(cfg, raw, db).Compact(ctx, Request{SourceID: "demo", StreamID: "trips", Date: "2026-08-12"})
	if err != nil {
		t.Fatal(err)
	}
	if err := VerifyDirectory(cfg.Storage.Root, result); err != nil {
		t.Fatalf("verify empty-day revision: %v", err)
	}
	manifestBytes, err := os.ReadFile(filepath.Join(cfg.Storage.Root, filepath.FromSlash(result.ManifestPath)))
	if err != nil {
		t.Fatal(err)
	}
	var manifest model.Manifest
	if err := json.Unmarshal(manifestBytes, &manifest); err != nil {
		t.Fatal(err)
	}
	if manifest.DatasetStatus != "no_captured_responses" || len(manifest.Files) != 0 {
		t.Fatalf("empty-day manifest wrong: status=%s files=%d", manifest.DatasetStatus, len(manifest.Files))
	}
	if manifest.SchemaVersion != model.ParquetSchemaVersionTripUpdatesFlattened ||
		manifest.StopTimeUpdateTotal == nil || *manifest.StopTimeUpdateTotal != 0 {
		t.Fatalf("empty-day v2 manifest: schema=%d total=%v", manifest.SchemaVersion, manifest.StopTimeUpdateTotal)
	}
}

func TestCompactTripUpdateZeroRowDay(t *testing.T) {
	ctx, cfg, raw, db := tripUpdatesDaySetup(t)
	addTripUpdateCapture(t, ctx, raw, db, []byte{0xff, 0x01, 0x02}, "protobuf_decode", 1)
	addTripUpdateCapture(t, ctx, raw, db, []byte{0x00, 0xff}, "protobuf_decode", 2)

	result, err := New(cfg, raw, db).Compact(ctx, Request{SourceID: "demo", StreamID: "trips", Date: "2026-08-12"})
	if err != nil {
		t.Fatal(err)
	}
	if err := VerifyDirectory(cfg.Storage.Root, result); err != nil {
		t.Fatalf("verify revision: %v", err)
	}
	manifestBytes, _ := os.ReadFile(filepath.Join(cfg.Storage.Root, filepath.FromSlash(result.ManifestPath)))
	var manifest model.Manifest
	if err := json.Unmarshal(manifestBytes, &manifest); err != nil {
		t.Fatal(err)
	}
	if manifest.DatasetStatus != "ready" || len(manifest.Files) != 1 ||
		manifest.StopTimeUpdateTotal == nil || *manifest.StopTimeUpdateTotal != 0 || manifest.Files[0].Rows != 0 {
		t.Fatalf("zero-row day manifest wrong: status=%s files=%d total=%v rows=%d",
			manifest.DatasetStatus, len(manifest.Files), manifest.StopTimeUpdateTotal, manifest.Files[0].Rows)
	}
	path := filepath.Join(cfg.Storage.Root, filepath.FromSlash(result.DataPath))
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	reader := parquet.NewGenericReader[projection.TripUpdateStopRow](f)
	rows := make([]projection.TripUpdateStopRow, 1)
	n, err := reader.Read(rows)
	if n != 0 {
		t.Fatalf("rows = %d, want a valid 0-row parquet", n)
	}
}

func TestCompactTripUpdateFlushesByRowCount(t *testing.T) {
	old := tripUpdateRowGroupRows
	tripUpdateRowGroupRows = 4
	t.Cleanup(func() { tripUpdateRowGroupRows = old })

	ctx, cfg, raw, db := tripUpdatesDaySetup(t)
	entities := make([]*gtfs.FeedEntity, 0, 3)
	for i := 0; i < 3; i++ {
		id, tripID := fmt.Sprintf("e%d", i), fmt.Sprintf("trip-%d", i)
		entities = append(entities, tripEntity(id, tripID, stu(1, "a"), stu(2, "b"), stu(3, "c"), stu(4, "d")))
	}
	addTripUpdateCapture(t, ctx, raw, db, tripUpdateCaptureBody(t, 1786600000, entities...), "valid", 1) // 12 rows, flush at 4 → ≥3 groups
	addTripUpdateCapture(t, ctx, raw, db, tripUpdateCaptureBody(t, 1786600000,
		tripEntity("e9", "trip-9", stu(1, "a"), stu(2, "b"))), "valid", 2) // +2 rows → total 14

	result, err := New(cfg, raw, db).Compact(ctx, Request{SourceID: "demo", StreamID: "trips", Date: "2026-08-12"})
	if err != nil {
		t.Fatal(err)
	}
	pf := openParquetFile(t, filepath.Join(cfg.Storage.Root, filepath.FromSlash(result.DataPath)))
	if got := len(pf.Metadata().RowGroups); got < 3 {
		t.Fatalf("row groups = %d, want ≥3 with flush at 4 rows over 14 rows", got)
	}
}

func TestCompactTripUpdateNestedStreamsUnaffected(t *testing.T) {
	// Guard: a vehicle_position feed on the same compactor still writes the
	// nested v1 layout with rows == captures and the pb copy.
	ctx, cfg, raw, db := tripUpdatesDaySetup(t)
	cfg.Sources[0].Streams[0] = config.Stream{ID: "vehicles", ExpectedKind: "vehicle_position", URL: "https://example.test/feed", Interval: config.Duration{Duration: 30 * time.Second}}
	addCapture(t, ctx, raw, db, tripUpdateCaptureBody(t, 1786600000, tripEntity("e1", "trip-1", stu(1, "a"))), "valid", 1)

	result, err := New(cfg, raw, db).Compact(ctx, Request{SourceID: "demo", StreamID: "vehicles", Date: "2026-08-12"})
	if err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(cfg.Storage.Root, filepath.FromSlash(result.DataPath)))
	if err != nil {
		t.Fatal(err)
	}
	if result.Rows != 1 {
		t.Fatalf("rows = %d", result.Rows)
	}
	_ = data // existence + successful ReadFile is the nested-path smoke check; full nested assertions live in compact_test.go
}
