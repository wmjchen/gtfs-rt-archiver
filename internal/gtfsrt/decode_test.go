package gtfsrt

import (
	"testing"
	"time"

	gtfs "github.com/MobilityData/gtfs-realtime-bindings/golang/gtfs"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

func TestDecodeSeparatesRequiredFieldFailureFromWireFailure(t *testing.T) {
	partial, err := proto.MarshalOptions{AllowPartial: true}.Marshal(&gtfs.FeedMessage{Header: &gtfs.FeedHeader{}})
	if err != nil {
		t.Fatal(err)
	}
	meta := Decode(partial, "mixed", time.Now())
	if meta.ParseStatus != "protobuf_required_field" || meta.Message == nil {
		t.Fatalf("metadata = %+v", meta)
	}
	malformed := Decode([]byte{0xff}, "mixed", time.Now())
	if malformed.ParseStatus != "protobuf_decode" || malformed.Message != nil {
		t.Fatalf("metadata = %+v", malformed)
	}
}

func TestDecodePreservesUnknownFields(t *testing.T) {
	version := "2.0"
	body, err := proto.Marshal(&gtfs.FeedMessage{Header: &gtfs.FeedHeader{GtfsRealtimeVersion: &version}})
	if err != nil {
		t.Fatal(err)
	}
	body = protowire.AppendTag(body, 999, protowire.VarintType)
	body = protowire.AppendVarint(body, 42)
	meta := Decode(body, "mixed", time.Now())
	if meta.ParseStatus != "valid" {
		t.Fatalf("parse status = %s", meta.ParseStatus)
	}
	if len(meta.Message.ProtoReflect().GetUnknown()) == 0 {
		t.Fatal("unknown field was discarded")
	}
}

func TestDecodeFlagsDifferentialWithoutRejectingIt(t *testing.T) {
	version := "2.0"
	incrementality := gtfs.FeedHeader_DIFFERENTIAL
	body, err := proto.Marshal(&gtfs.FeedMessage{Header: &gtfs.FeedHeader{
		GtfsRealtimeVersion: &version, Incrementality: &incrementality,
	}})
	if err != nil {
		t.Fatal(err)
	}
	meta := Decode(body, "mixed", time.Now())
	if meta.ParseStatus != "valid" || !hasFlag(meta.Flags, "differential_incrementality") {
		t.Fatalf("metadata = %+v", meta)
	}
}

func hasFlag(flags []string, want string) bool {
	for _, flag := range flags {
		if flag == want {
			return true
		}
	}
	return false
}
