package projection

import (
	"slices"
	"testing"
	"time"

	gtfs "github.com/MobilityData/gtfs-realtime-bindings/golang/gtfs"
	"github.com/parquet-go/parquet-go"
)

func fixtureProvenance() TripUpdateProvenance {
	return TripUpdateProvenance{
		SourceFile: "raw/source=demo/stream=trips/date=2026-08-12/hour=12/cap.pb",
		FeedURL:    "https://example.test/trips", FetchTimestamp: time.Date(2026, 8, 12, 12, 0, 0, 0, time.UTC),
		SourceID: "demo", StreamID: "trips", CaptureID: "capture-1", ArchiveDate: "2026-08-12",
		ArchiveTimezone: "UTC", ScheduledAt: time.Date(2026, 8, 12, 12, 0, 0, 0, time.UTC),
		ParseStatus: "valid", ValidationFlags: []string{"missing_feed_timestamp"},
	}
}

func fixtureMessage() *gtfs.FeedMessage {
	version := "2.2.0"
	ts := uint64(1786600000)
	diff := gtfs.FeedHeader_DIFFERENTIAL
	e1id, e2id, e3id, e4id, e5id := "e1", "e2", "e3", "e4", "e5"
	tripID, routeID := "trip-9", "route-2"
	dir := uint32(1)
	start, date := "07:30:00", "20260812"
	sched := gtfs.TripDescriptor_SCHEDULED
	vehID, label, plate := "veh-1", "Bus 1", "ABC123"
	tuTs := ts + 5
	delay := int32(42)
	seq1, seq2 := uint32(10), uint32(11)
	stopA, stopB := "100", "200"
	arrDelay, arrUnc := int32(3), int32(15)
	arrTime := int64(1786600030)
	depDelay, depUnc := int32(4), int32(16)
	depTime := int64(1786600060)
	skipRel := gtfs.TripUpdate_StopTimeUpdate_SKIPPED
	assigned := "100-plat2"
	deleted := true
	return &gtfs.FeedMessage{
		Header: &gtfs.FeedHeader{GtfsRealtimeVersion: &version, Timestamp: &ts, Incrementality: &diff},
		Entity: []*gtfs.FeedEntity{
			{Id: &e1id, TripUpdate: &gtfs.TripUpdate{
				Trip:      &gtfs.TripDescriptor{TripId: &tripID, RouteId: &routeID, DirectionId: &dir, StartTime: &start, StartDate: &date, ScheduleRelationship: &sched},
				Vehicle:   &gtfs.VehicleDescriptor{Id: &vehID, Label: &label, LicensePlate: &plate},
				Timestamp: &tuTs, Delay: &delay,
				StopTimeUpdate: []*gtfs.TripUpdate_StopTimeUpdate{
					{StopSequence: &seq1, StopId: &stopA,
						Arrival:              &gtfs.TripUpdate_StopTimeEvent{Delay: &arrDelay, Time: &arrTime, Uncertainty: &arrUnc},
						Departure:            &gtfs.TripUpdate_StopTimeEvent{Delay: &depDelay, Time: &depTime, Uncertainty: &depUnc},
						ScheduleRelationship: &skipRel,
						StopTimeProperties:   &gtfs.TripUpdate_StopTimeUpdate_StopTimeProperties{AssignedStopId: &assigned}},
					{StopSequence: &seq2, StopId: &stopB},
				}}},
			{Id: &e2id, TripUpdate: &gtfs.TripUpdate{Trip: &gtfs.TripDescriptor{TripId: &tripID}}}, // zero-STU base row
			{Id: &e3id, IsDeleted: &deleted}, // bare tombstone: no row
			{Id: &e4id, IsDeleted: &deleted, TripUpdate: &gtfs.TripUpdate{Trip: &gtfs.TripDescriptor{TripId: &tripID},
				StopTimeUpdate: []*gtfs.TripUpdate_StopTimeUpdate{{StopSequence: &seq1}}}}, // 1 row, is_deleted=true
			{Id: &e5id, Vehicle: &gtfs.VehiclePosition{Timestamp: &ts}}, // vehicle-only: no row
		},
	}
}

func TestProjectTripUpdateStopsGrainAndContent(t *testing.T) {
	rows := ProjectTripUpdateStops(fixtureMessage(), fixtureProvenance())
	if len(rows) != 4 {
		t.Fatalf("rows = %d, want 4 (2 STU + 1 base + 1 deleted-payload)", len(rows))
	}
	a := rows[0]
	if a.EntityID != "e1" || a.TripID != "trip-9" || a.RouteID != "route-2" || *a.DirectionID != 1 ||
		a.VehicleID != "veh-1" || *a.StopSequence != 10 || *a.StopID != "100" ||
		*a.ArrivalDelay != 3 || *a.ArrivalTime != 1786600030 || *a.StopScheduleRelationship != 1 ||
		*a.AssignedStopID != "100-plat2" || *a.TripDelay != 42 || a.TripTimestamp == nil {
		t.Fatalf("row[0] = %+v", a)
	}
	if a.CaptureID != "capture-1" || a.SourceID != "demo" || a.FeedURL != "https://example.test/trips" ||
		a.ParseStatus != "valid" || len(a.ValidationFlags) != 1 || a.FeedVersion != "2.2.0" ||
		a.Incrementality == nil || *a.Incrementality != 1 || a.FeedTimestamp == nil || *a.FeedTimestamp != 1786600000 ||
		a.IsDeleted {
		t.Fatalf("row[0] provenance = %+v", a)
	}
	if *rows[1].StopSequence != 11 || rows[1].AssignedStopID != nil {
		t.Fatalf("second STU row = %+v", rows[1])
	}
	base := rows[2]
	if base.EntityID != "e2" || base.StopSequence != nil || base.StopID != nil || base.ArrivalTime != nil || base.TripDelay != nil {
		t.Fatalf("base row must carry NULL stop-level columns: %+v", base)
	}
	del := rows[3]
	if !del.IsDeleted || del.EntityID != "e4" || *del.StopSequence != 10 {
		t.Fatalf("deleted-payload row = %+v", del)
	}
}

func TestProjectTripUpdateStopsPresenceSemantics(t *testing.T) {
	version := "2.0"
	zero := uint64(0)
	tripID := "t"
	id := "e"
	msg := &gtfs.FeedMessage{
		Header: &gtfs.FeedHeader{GtfsRealtimeVersion: &version, Timestamp: &zero}, // explicit 0 must be preserved, not NULL
		Entity: []*gtfs.FeedEntity{{Id: &id, TripUpdate: &gtfs.TripUpdate{Trip: &gtfs.TripDescriptor{TripId: &tripID}}}},
	}
	rows := ProjectTripUpdateStops(msg, fixtureProvenance())
	if len(rows) != 1 {
		t.Fatalf("rows = %d", len(rows))
	}
	r := rows[0]
	if r.ScheduleRelationship != 0 {
		t.Fatal("unset schedule_relationship must materialize proto default 0")
	}
	if r.FeedTimestamp == nil || *r.FeedTimestamp != 0 {
		t.Fatal("explicit feed_timestamp=0 must record 0")
	}
	if r.LicensePlate != nil || r.TripTimestamp != nil || r.TripDelay != nil || r.DirectionID != nil {
		t.Fatal("sparse unset fields must be NULL")
	}
	if r.RouteID != "" || r.VehicleID != "" {
		t.Fatal("unset strings on populated-parent convention read empty string")
	}
	// Bindings-gap registry columns stay NULL until the pinned protobuf exposes them.
	if r.ModifiedTripModificationsID != nil || r.TripPropertiesShapeID != nil || r.WheelchairAccessible != nil ||
		r.ArrivalScheduledTime != nil || r.DepartureOccupancyStatus != nil || r.StopHeadsign != nil || r.PickupType != nil {
		t.Fatal("bindings-gap columns must be NULL")
	}
}

func TestTripUpdateStopRowCounters(t *testing.T) {
	msg := fixtureMessage()
	if got := CountProjectedTripUpdateRows(msg); got != 4 {
		t.Fatalf("CountProjectedTripUpdateRows = %d, want 4", got)
	}
	if got := CountStopTimeUpdates(msg); got != 3 {
		t.Fatalf("CountStopTimeUpdates = %d, want 3", got)
	}
	if CountProjectedTripUpdateRows(nil) != 0 || CountStopTimeUpdates(nil) != 0 {
		t.Fatal("nil message must count zero")
	}
}

func TestTripUpdateStopRowSchemaMatchesGtfsRTIORegistry(t *testing.T) {
	want := []string{
		// Reference registry (schemas.py TRIP_UPDATES_SCHEMA), in order:
		"source_file", "feed_url", "feed_timestamp", "fetch_timestamp", "entity_id",
		"trip_id", "route_id", "direction_id", "start_time", "start_date", "schedule_relationship",
		"vehicle_id", "vehicle_label",
		"trip_timestamp", "trip_delay",
		"stop_sequence", "stop_id", "arrival_delay", "arrival_time", "arrival_uncertainty",
		"departure_delay", "departure_time", "departure_uncertainty", "stop_schedule_relationship",
		"license_plate", "arrival_scheduled_time", "departure_scheduled_time", "departure_occupancy_status",
		"assigned_stop_id", "stop_headsign", "pickup_type", "drop_off_type",
		"trip_properties_trip_id", "trip_properties_start_date", "trip_properties_start_time",
		"trip_properties_shape_id", "trip_properties_trip_headsign", "trip_properties_trip_short_name",
		"modified_trip_modifications_id", "modified_trip_affected_trip_id",
		"modified_trip_start_date", "modified_trip_start_time",
		"wheelchair_accessible",
		"feed_version", "incrementality", "is_deleted",
		// Archiver provenance extras appended at the end:
		"source_id", "stream_id", "capture_id", "archive_date", "archive_timezone",
		"scheduled_at", "parse_status", "validation_flags",
	}
	schema := parquet.SchemaOf(TripUpdateStopRow{})
	got := make([]string, 0, len(want))
	for _, field := range schema.Fields() {
		got = append(got, field.Name())
	}
	if !slices.Equal(got, want) {
		t.Fatalf("columns mismatch\n got: %v\nwant: %v", got, want)
	}
}
