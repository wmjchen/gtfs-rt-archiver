package version

import "runtime"

var (
	Version          = "dev"
	Commit           = "unknown"
	BuildTime        = "unknown"
	ProtobufRevision = "github.com/MobilityData/gtfs-realtime-bindings/golang/gtfs@v1.0.0"
)

type Info struct {
	Version          string `json:"version"`
	Commit           string `json:"commit"`
	BuildTime        string `json:"build_time"`
	GoVersion        string `json:"go_version"`
	ProtobufRevision string `json:"protobuf_revision"`
	ParquetFormat    int    `json:"parquet_format"`
	StateSchema      int    `json:"state_schema"`
}

func Current() Info {
	return Info{
		Version: Version, Commit: Commit, BuildTime: BuildTime,
		GoVersion: runtime.Version(), ProtobufRevision: ProtobufRevision,
		ParquetFormat: 1, StateSchema: 1,
	}
}
