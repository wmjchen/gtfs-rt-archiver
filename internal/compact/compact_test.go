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
	"gtfs-rt-archiver/internal/rawstore"
	"gtfs-rt-archiver/internal/state"
)

func TestManifestUsesAgencyLocalDSTDayBoundaries(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	location, err := time.LoadLocation("America/Vancouver")
	if err != nil {
		t.Fatal(err)
	}
	cfg := config.Defaults()
	cfg.Storage.Root = root
	cfg.Storage.StateDB = filepath.Join(root, "state.sqlite")
	cfg.Sources = []config.Source{{ID: "demo", Timezone: location.String(), Location: location, Streams: []config.Stream{{
		ID: "feed", ExpectedKind: "mixed", URL: "https://example.test/feed", Interval: config.Duration{Duration: 30 * time.Second},
	}}}}
	raw, err := rawstore.Open(root)
	if err != nil {
		t.Fatal(err)
	}
	db, err := state.Open(ctx, cfg.Storage.StateDB)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	compactor := New(&cfg, raw, db)
	for date, expected := range map[string]time.Duration{"2025-03-09": 23 * time.Hour, "2025-11-02": 25 * time.Hour} {
		result, err := compactor.Compact(ctx, Request{SourceID: "demo", StreamID: "feed", Date: date})
		if err != nil {
			t.Fatal(err)
		}
		b, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(result.ManifestPath)))
		if err != nil {
			t.Fatal(err)
		}
		var manifest model.Manifest
		if err := json.Unmarshal(b, &manifest); err != nil {
			t.Fatal(err)
		}
		if got := manifest.DayEndUTC.Sub(manifest.DayStartUTC); got != expected {
			t.Fatalf("%s day length = %s", date, got)
		}
	}
}

func TestCompactKeepsValidAndMalformedCaptures(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	cfg := config.Defaults()
	cfg.Storage.Root = root
	cfg.Storage.StateDB = filepath.Join(root, "state.sqlite")
	cfg.Parquet.TargetRowGroupBytes = 1
	cfg.Sources = []config.Source{{
		ID: "demo", Timezone: "UTC", Location: time.UTC,
		Streams: []config.Stream{{ID: "vehicles", ExpectedKind: "mixed", URL: "https://example.test/feed", Interval: config.Duration{Duration: 30 * time.Second}}},
	}}
	raw, err := rawstore.Open(root)
	if err != nil {
		t.Fatal(err)
	}
	db, err := state.Open(ctx, cfg.Storage.StateDB)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	version := "2.0"
	timestamp := uint64(time.Date(2026, 8, 12, 12, 0, 0, 0, time.UTC).Unix())
	incrementality := gtfs.FeedHeader_DIFFERENTIAL
	vehicleID, tripEntityID, alertID, deletedID := "vehicle-1", "trip-1", "alert-1", "deleted-1"
	tripID, alertText, language, deleted := "scheduled-trip-1", "Service disruption", "en", true
	valid, err := proto.Marshal(&gtfs.FeedMessage{
		Header: &gtfs.FeedHeader{GtfsRealtimeVersion: &version, Timestamp: &timestamp, Incrementality: &incrementality},
		Entity: []*gtfs.FeedEntity{
			{Id: &vehicleID, Vehicle: &gtfs.VehiclePosition{Timestamp: &timestamp}},
			{Id: &tripEntityID, TripUpdate: &gtfs.TripUpdate{Trip: &gtfs.TripDescriptor{TripId: &tripID}, Timestamp: &timestamp}},
			{Id: &alertID, Alert: &gtfs.Alert{HeaderText: &gtfs.TranslatedString{Translation: []*gtfs.TranslatedString_Translation{{Text: &alertText, Language: &language}}}}},
			{Id: &deletedID, IsDeleted: &deleted},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	addCapture(t, ctx, raw, db, valid, "valid", 1)
	addCapture(t, ctx, raw, db, []byte{0xff, 0x01, 0x02}, "protobuf_decode", 2)
	addCaptureWithTransport(t, ctx, raw, db, valid, "valid", 3, false)

	compactor := New(&cfg, raw, db)
	result, err := compactor.Compact(ctx, Request{SourceID: "demo", StreamID: "vehicles", Date: "2026-08-12"})
	if err != nil {
		t.Fatal(err)
	}
	if result.Rows != 3 {
		t.Fatalf("rows = %d", result.Rows)
	}
	if err := VerifyDirectory(root, result); err != nil {
		t.Fatal(err)
	}

	dataPath := filepath.Join(root, filepath.FromSlash(result.DataPath))
	f, err := os.Open(dataPath)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	reader := parquet.NewGenericReader[Row](f)
	rows := make([]Row, 3)
	n, err := reader.Read(rows)
	if err != nil && err != io.EOF {
		t.Fatal(err)
	}
	if n != 3 {
		t.Fatalf("read rows = %d", n)
	}
	if rows[0].Header == nil || len(rows[0].Entities) != 4 || rows[0].Entities[0].Vehicle == nil ||
		rows[0].Entities[1].TripUpdate == nil || rows[0].Entities[2].Alert == nil ||
		rows[0].Entities[3].IsDeleted == nil || !*rows[0].Entities[3].IsDeleted {
		t.Fatal("valid nested projection is absent")
	}
	if rows[1].Header != nil || string(rows[1].FeedMessagePB) != string([]byte{0xff, 0x01, 0x02}) {
		t.Fatal("malformed row was not preserved")
	}
	manifestBytes, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(result.ManifestPath)))
	if err != nil {
		t.Fatal(err)
	}
	var manifest model.Manifest
	if err := json.Unmarshal(manifestBytes, &manifest); err != nil {
		t.Fatal(err)
	}
	if manifest.ValidSnapshots != 1 || manifest.InvalidPayloads != 2 {
		t.Fatalf("transport-incomplete capture was misclassified: %+v", manifest)
	}
}

func addCapture(t *testing.T, ctx context.Context, raw *rawstore.Store, db *state.Store, body []byte, parseStatus string, second int) {
	t.Helper()
	addCaptureWithTransport(t, ctx, raw, db, body, parseStatus, second, true)
}

func addCaptureWithTransport(t *testing.T, ctx context.Context, raw *rawstore.Store, db *state.Store, body []byte, parseStatus string, second int, transportComplete bool) {
	t.Helper()
	scheduled := time.Date(2026, 8, 12, 12, 0, second, 0, time.UTC)
	staged, err := raw.Stage(bytes.NewReader(body), 1024)
	if err != nil {
		t.Fatal(err)
	}
	capture := model.Capture{
		FormatVersion: 1, ID: fmt.Sprintf("capture-%d", second), TickID: fmt.Sprintf("tick-%d", second),
		SourceID: "demo", StreamID: "vehicles", ExpectedKind: "vehicle_position",
		ScheduledAt: scheduled, StartedAt: scheduled, CompletedAt: scheduled.Add(time.Millisecond),
		ArchiveDate: "2026-08-12", Timezone: "UTC", SanitizedURL: "https://example.test/feed",
		HTTPStatus: 200, AdvertisedLength: int64(len(body)), EncodedLength: int64(len(body)),
		TransportComplete: transportComplete, ParseStatus: parseStatus, ValidationFlags: []string{},
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

func TestManifestSanitizedURL(t *testing.T) {
	stream := &config.Stream{URL: "https://example.test/feed?apikey=removed"}

	// No captures (no_captured_responses dataset): value comes from the
	// stream's configured URL, sanitized.
	got, err := manifestSanitizedURL(nil, stream)
	if err != nil {
		t.Fatal(err)
	}
	if got != "https://example.test/feed" {
		t.Fatalf("config fallback = %q", got)
	}

	// With captures: value comes from the (constant) capture metadata.
	got, err = manifestSanitizedURL([]model.Capture{
		{SanitizedURL: "https://example.test/feed"},
		{SanitizedURL: "https://example.test/feed"},
	}, stream)
	if err != nil || got != "https://example.test/feed" {
		t.Fatalf("capture-derived = %q, err = %v", got, err)
	}

	// Disagreeing captures are an integrity failure, never a silent pick.
	if _, err := manifestSanitizedURL([]model.Capture{
		{SanitizedURL: "https://a.test/x"},
		{SanitizedURL: "https://b.test/y"},
	}, stream); err == nil {
		t.Fatal("expected error for disagreeing sanitized URLs")
	}
}

func TestManifestFileCarriesSanitizedURL(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	cfg := config.Defaults()
	cfg.Storage.Root = root
	cfg.Storage.StateDB = filepath.Join(root, "state.sqlite")
	cfg.Sources = []config.Source{{ID: "demo", Timezone: "UTC", Location: time.UTC, Streams: []config.Stream{{
		ID: "feed", ExpectedKind: "mixed", URL: "https://example.test/feed?apikey=removed", Interval: config.Duration{Duration: 30 * time.Second},
	}}}}
	raw, err := rawstore.Open(root)
	if err != nil {
		t.Fatal(err)
	}
	db, err := state.Open(ctx, cfg.Storage.StateDB)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	result, err := New(&cfg, raw, db).Compact(ctx, Request{SourceID: "demo", StreamID: "feed", Date: "2026-08-12"})
	if err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(result.ManifestPath)))
	if err != nil {
		t.Fatal(err)
	}
	var manifest model.Manifest
	if err := json.Unmarshal(b, &manifest); err != nil {
		t.Fatal(err)
	}
	if manifest.DatasetStatus != "no_captured_responses" {
		t.Fatalf("status = %q", manifest.DatasetStatus)
	}
	if manifest.SanitizedURL != "https://example.test/feed" {
		t.Fatalf("manifest sanitized_url = %q", manifest.SanitizedURL)
	}
}
