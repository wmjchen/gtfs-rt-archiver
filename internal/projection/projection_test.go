package projection

import (
	"crypto/sha256"
	"encoding/hex"
	"testing"

	gtfs "github.com/MobilityData/gtfs-realtime-bindings/golang/gtfs"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
)

func TestGeneratedProjectionMatchesPinnedDescriptor(t *testing.T) {
	b, err := proto.MarshalOptions{Deterministic: true}.Marshal(protodesc.ToFileDescriptorProto(gtfs.File_gtfs_realtime_proto))
	if err != nil {
		t.Fatal(err)
	}
	h := sha256.Sum256(b)
	if got := hex.EncodeToString(h[:]); got != DescriptorSHA256 {
		t.Fatalf("GTFS-Realtime descriptor changed (%s != %s); run go generate ./internal/projection and review the schema change", got, DescriptorSHA256)
	}
}
