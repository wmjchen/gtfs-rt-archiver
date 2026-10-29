package gtfsrt

import (
	"fmt"
	"sort"
	"time"

	gtfs "github.com/MobilityData/gtfs-realtime-bindings/golang/gtfs"
	"google.golang.org/protobuf/proto"
)

type Metadata struct {
	Message        *gtfs.FeedMessage
	ParseStatus    string
	ParseError     string
	FeedVersion    string
	Incrementality *int32
	FeedTimestamp  *uint64
	EntityCount    *int32
	Flags          []string
}

func Decode(body []byte, expectedKind string, observedAt time.Time) Metadata {
	meta := Metadata{ParseStatus: "valid", Message: &gtfs.FeedMessage{}, Flags: []string{}}
	if len(body) == 0 {
		meta.ParseStatus = "empty_body"
		meta.ParseError = "response body is empty"
		return meta
	}
	if err := (proto.UnmarshalOptions{DiscardUnknown: false, AllowPartial: true}).Unmarshal(body, meta.Message); err != nil {
		meta.ParseStatus = "protobuf_decode"
		meta.ParseError = boundedError(err)
		meta.Message = nil
		return meta
	}
	if err := proto.CheckInitialized(meta.Message); err != nil {
		meta.ParseStatus = "protobuf_required_field"
		meta.ParseError = boundedError(err)
	}
	header := meta.Message.GetHeader()
	if header == nil || header.GetGtfsRealtimeVersion() == "" {
		meta.ParseStatus = "protobuf_required_field"
		meta.ParseError = "feed header or gtfs_realtime_version is absent"
	} else {
		meta.FeedVersion = header.GetGtfsRealtimeVersion()
		if header.Incrementality != nil {
			v := int32(header.GetIncrementality())
			meta.Incrementality = &v
			if header.GetIncrementality() == gtfs.FeedHeader_DIFFERENTIAL {
				meta.Flags = append(meta.Flags, "differential_incrementality")
			}
		}
		if header.Timestamp != nil {
			v := header.GetTimestamp()
			meta.FeedTimestamp = &v
			feedTime := time.Unix(int64(v), 0)
			if feedTime.After(observedAt.Add(5 * time.Minute)) {
				meta.Flags = append(meta.Flags, "feed_timestamp_future")
			}
			if feedTime.Before(observedAt.Add(-24 * time.Hour)) {
				meta.Flags = append(meta.Flags, "feed_timestamp_stale")
			}
		} else {
			meta.Flags = append(meta.Flags, "missing_feed_timestamp")
		}
	}
	count := int32(len(meta.Message.GetEntity()))
	meta.EntityCount = &count
	for _, entity := range meta.Message.GetEntity() {
		payloads := 0
		actual := "unknown"
		if entity.GetTripUpdate() != nil {
			payloads++
			actual = "trip_update"
		}
		if entity.GetVehicle() != nil {
			payloads++
			actual = "vehicle_position"
		}
		if entity.GetAlert() != nil {
			payloads++
			actual = "alert"
		}
		if payloads > 1 {
			meta.Flags = append(meta.Flags, "multiple_entity_payloads")
		}
		if payloads == 0 && !entity.GetIsDeleted() {
			meta.Flags = append(meta.Flags, "entity_without_payload")
		}
		if expectedKind != "mixed" && expectedKind != "auto" && actual != "unknown" && actual != expectedKind {
			meta.Flags = append(meta.Flags, "unexpected_entity_kind")
		}
	}
	meta.Flags = unique(meta.Flags)
	return meta
}

func boundedError(err error) string {
	v := fmt.Sprintf("%T", err)
	if len(v) > 160 {
		return v[:160]
	}
	return v
}

func unique(values []string) []string {
	if len(values) == 0 {
		return []string{}
	}
	sort.Strings(values)
	out := values[:0]
	for _, value := range values {
		if len(out) == 0 || out[len(out)-1] != value {
			out = append(out, value)
		}
	}
	return out
}
