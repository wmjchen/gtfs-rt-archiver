package model

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestParquetSchemaVersions(t *testing.T) {
	if !IsSupportedParquetSchemaVersion(ParquetSchemaVersionNested) || !IsSupportedParquetSchemaVersion(ParquetSchemaVersionTripUpdatesFlattened) {
		t.Fatalf("supported versions = %v", SupportedParquetSchemaVersions)
	}
	if IsSupportedParquetSchemaVersion(3) {
		t.Fatal("schema version 3 must not be supported yet")
	}
	if ParquetSchemaVersionNested != 1 || ParquetSchemaVersionTripUpdatesFlattened != 2 {
		t.Fatalf("nested=%d flattened=%d — constants must be 1 and 2 or old revisions orphan", ParquetSchemaVersionNested, ParquetSchemaVersionTripUpdatesFlattened)
	}
}

func TestManifestStopTimeUpdateTotalIsAdditive(t *testing.T) {
	b, err := json.Marshal(Manifest{})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(b), "stop_time_update_total") {
		t.Fatalf("field must be omitted when unset: %s", b)
	}
	total := int64(42)
	b, err = json.Marshal(Manifest{StopTimeUpdateTotal: &total})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(b), `"stop_time_update_total":42`) {
		t.Fatalf("field must serialize when set: %s", b)
	}
}
