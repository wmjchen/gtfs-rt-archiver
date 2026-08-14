package projection

import (
	"time"

	gtfs "github.com/MobilityData/gtfs-realtime-bindings/golang/gtfs"
)

// TripUpdateProvenance carries capture-level metadata copied verbatim onto
// every flattened row emitted for that capture.
type TripUpdateProvenance struct {
	SourceFile      string    // raw-store relative path of the source .pb (gtfsrt.io: source_file)
	FeedURL         string    // sanitized feed URL (gtfsrt.io: feed_url)
	FetchTimestamp  time.Time // capture completion time (gtfsrt.io: fetch_timestamp)
	ScheduledAt     time.Time
	SourceID        string
	StreamID        string
	CaptureID       string
	ArchiveDate     string
	ArchiveTimezone string
	ParseStatus     string
	ValidationFlags []string
}

// TripUpdateStopRow is the flattened trip_updates Parquet row: one row per
// stop_time_update, or one base row for a trip-update entity that carries
// none. Columns and order mirror gtfsrt.io's TRIP_UPDATES_SCHEMA
// (JarvusInnovations/gtfs-realtime-archiver src/dagster_pipeline/defs/assets/
// schemas.py); the trailing eight archiver provenance columns are additive.
// Presence semantics (NULL vs "", enum defaults) follow specs/behaviors/
// compaction.md and are asserted by TestProjectTripUpdateStopsPresenceSemantics.
//
// Columns whose proto fields the pinned bindings do not expose yet
// (modified_trip_*, trip_properties_shape_id/_trip_headsign/_trip_short_name,
// wheelchair_accessible, *_scheduled_time, departure_occupancy_status,
// stop_headsign, pickup_type, drop_off_type) exist for registry parity but are
// always NULL until the binding revision is bumped; that bump is out of scope.
type TripUpdateStopRow struct {
	SourceFile     string    `parquet:"source_file,dict"`
	FeedURL        string    `parquet:"feed_url,dict"`
	FeedTimestamp  *uint64   `parquet:"feed_timestamp,optional"`
	FetchTimestamp time.Time `parquet:"fetch_timestamp,timestamp(microsecond)"`
	EntityID       string    `parquet:"entity_id"`

	TripID               string  `parquet:"trip_id,dict"`
	RouteID              string  `parquet:"route_id,dict"`
	DirectionID          *uint32 `parquet:"direction_id,optional"`
	StartTime            string  `parquet:"start_time"`
	StartDate            string  `parquet:"start_date,dict"`
	ScheduleRelationship int32   `parquet:"schedule_relationship"`

	VehicleID    string `parquet:"vehicle_id,dict"`
	VehicleLabel string `parquet:"vehicle_label,dict"`

	TripTimestamp *uint64 `parquet:"trip_timestamp,optional"`
	TripDelay     *int32  `parquet:"trip_delay,optional"`

	StopSequence             *uint32 `parquet:"stop_sequence,optional"`
	StopID                   *string `parquet:"stop_id,optional,dict"`
	ArrivalDelay             *int32  `parquet:"arrival_delay,optional"`
	ArrivalTime              *int64  `parquet:"arrival_time,optional"`
	ArrivalUncertainty       *int32  `parquet:"arrival_uncertainty,optional"`
	DepartureDelay           *int32  `parquet:"departure_delay,optional"`
	DepartureTime            *int64  `parquet:"departure_time,optional"`
	DepartureUncertainty     *int32  `parquet:"departure_uncertainty,optional"`
	StopScheduleRelationship *int32  `parquet:"stop_schedule_relationship,optional"`

	LicensePlate             *string `parquet:"license_plate,optional,dict"`
	ArrivalScheduledTime     *int64  `parquet:"arrival_scheduled_time,optional"`
	DepartureScheduledTime   *int64  `parquet:"departure_scheduled_time,optional"`
	DepartureOccupancyStatus *int32  `parquet:"departure_occupancy_status,optional"`
	AssignedStopID           *string `parquet:"assigned_stop_id,optional,dict"`
	StopHeadsign             *string `parquet:"stop_headsign,optional"`
	PickupType               *int32  `parquet:"pickup_type,optional"`
	DropOffType              *int32  `parquet:"drop_off_type,optional"`

	TripPropertiesTripID        *string `parquet:"trip_properties_trip_id,optional,dict"`
	TripPropertiesStartDate     *string `parquet:"trip_properties_start_date,optional"`
	TripPropertiesStartTime     *string `parquet:"trip_properties_start_time,optional"`
	TripPropertiesShapeID       *string `parquet:"trip_properties_shape_id,optional"`
	TripPropertiesTripHeadsign  *string `parquet:"trip_properties_trip_headsign,optional"`
	TripPropertiesTripShortName *string `parquet:"trip_properties_trip_short_name,optional"`

	ModifiedTripModificationsID *string `parquet:"modified_trip_modifications_id,optional"`
	ModifiedTripAffectedTripID  *string `parquet:"modified_trip_affected_trip_id,optional"`
	ModifiedTripStartDate       *string `parquet:"modified_trip_start_date,optional"`
	ModifiedTripStartTime       *string `parquet:"modified_trip_start_time,optional"`

	WheelchairAccessible *int32 `parquet:"wheelchair_accessible,optional"`

	FeedVersion    string `parquet:"feed_version,dict"`
	Incrementality *int32 `parquet:"incrementality,optional"`
	IsDeleted      bool   `parquet:"is_deleted"`

	SourceID        string    `parquet:"source_id,dict"`
	StreamID        string    `parquet:"stream_id,dict"`
	CaptureID       string    `parquet:"capture_id"`
	ArchiveDate     string    `parquet:"archive_date,dict"`
	ArchiveTimezone string    `parquet:"archive_timezone,dict"`
	ScheduledAt     time.Time `parquet:"scheduled_at,timestamp(microsecond)"`
	ParseStatus     string    `parquet:"parse_status,dict"`
	ValidationFlags []string  `parquet:"validation_flags"`
}

// ProjectTripUpdateStops flattens decoded entities into stop-time-update rows.
// Entities whose TripUpdate is nil (including bare is_deleted tombstones and
// vehicle/alert payloads in a trip_update stream) produce no row. A
// payload-bearing deleted entity still yields rows with IsDeleted set.
func ProjectTripUpdateStops(message *gtfs.FeedMessage, prov TripUpdateProvenance) []TripUpdateStopRow {
	n := CountProjectedTripUpdateRows(message)
	out := make([]TripUpdateStopRow, 0, n)
	header := message.GetHeader()
	var feedVersion string
	var feedTimestamp *uint64
	var incrementality *int32
	if header != nil {
		feedVersion = header.GetGtfsRealtimeVersion()
		feedTimestamp = header.Timestamp // preserves explicitly published 0
		if header.Incrementality != nil {
			v := int32(header.GetIncrementality())
			incrementality = &v
		}
	}
	for _, entity := range message.GetEntity() {
		tu := entity.GetTripUpdate()
		if tu == nil {
			continue
		}
		base := TripUpdateStopRow{
			SourceFile:      prov.SourceFile,
			FeedURL:         prov.FeedURL,
			FeedTimestamp:   feedTimestamp,
			FetchTimestamp:  prov.FetchTimestamp,
			EntityID:        entity.GetId(),
			FeedVersion:     feedVersion,
			Incrementality:  incrementality,
			IsDeleted:       entity.GetIsDeleted(),
			SourceID:        prov.SourceID,
			StreamID:        prov.StreamID,
			CaptureID:       prov.CaptureID,
			ArchiveDate:     prov.ArchiveDate,
			ArchiveTimezone: prov.ArchiveTimezone,
			ScheduledAt:     prov.ScheduledAt,
			ParseStatus:     prov.ParseStatus,
			ValidationFlags: append([]string(nil), prov.ValidationFlags...),
		}
		if td := tu.GetTrip(); td != nil {
			base.TripID = td.GetTripId() // "" when unset on a present parent
			base.RouteID = td.GetRouteId()
			base.DirectionID = td.DirectionId
			base.StartTime = td.GetStartTime()
			base.StartDate = td.GetStartDate()
			base.ScheduleRelationship = int32(td.GetScheduleRelationship()) // proto default 0 when unset
		}
		if vd := tu.GetVehicle(); vd != nil {
			base.VehicleID = vd.GetId()
			base.VehicleLabel = vd.GetLabel()
			base.LicensePlate = vd.LicensePlate // sparse string: NULL when unset
		}
		base.TripTimestamp = tu.Timestamp
		base.TripDelay = tu.Delay
		if tp := tu.GetTripProperties(); tp != nil {
			base.TripPropertiesTripID = tp.TripId
			base.TripPropertiesStartDate = tp.StartDate
			base.TripPropertiesStartTime = tp.StartTime
		}
		stus := tu.GetStopTimeUpdate()
		if len(stus) == 0 {
			out = append(out, base) // zero-STU base row keeps the entity visible
			continue
		}
		for _, stu := range stus {
			row := base
			row.StopSequence = stu.StopSequence
			row.StopID = stu.StopId
			if stu.ScheduleRelationship != nil {
				v := int32(stu.GetScheduleRelationship())
				row.StopScheduleRelationship = &v // NULL when unset
			}
			if event := stu.GetArrival(); event != nil {
				row.ArrivalDelay = event.Delay
				row.ArrivalTime = event.Time
				row.ArrivalUncertainty = event.Uncertainty
			}
			if event := stu.GetDeparture(); event != nil {
				row.DepartureDelay = event.Delay
				row.DepartureTime = event.Time
				row.DepartureUncertainty = event.Uncertainty
			}
			if props := stu.GetStopTimeProperties(); props != nil {
				row.AssignedStopID = props.AssignedStopId
			}
			out = append(out, row)
		}
	}
	return out
}

// CountProjectedTripUpdateRows returns how many rows ProjectTripUpdateStops
// would emit. Used by compaction-time validation to recount expectations from
// the raw captures without materializing rows.
func CountProjectedTripUpdateRows(message *gtfs.FeedMessage) int64 {
	if message == nil {
		return 0
	}
	var total int64
	for _, entity := range message.GetEntity() {
		tu := entity.GetTripUpdate()
		if tu == nil {
			continue
		}
		if n := len(tu.GetStopTimeUpdate()); n > 0 {
			total += int64(n)
		} else {
			total++ // zero-STU base row
		}
	}
	return total
}

// CountStopTimeUpdates returns the Σ stop_time_update count (excluding base
// rows) across trip-update entities — the manifest stop_time_update_total.
func CountStopTimeUpdates(message *gtfs.FeedMessage) int64 {
	if message == nil {
		return 0
	}
	var total int64
	for _, entity := range message.GetEntity() {
		if tu := entity.GetTripUpdate(); tu != nil {
			total += int64(len(tu.GetStopTimeUpdate()))
		}
	}
	return total
}
