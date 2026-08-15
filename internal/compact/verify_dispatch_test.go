package compact

import (
	"os"
	"path/filepath"
	"testing"

	"gtfs-rt-archiver/internal/model"
)

// fabricateRevisionMaterializes creates a minimal revision directory whose
// artifact is arbitrary bytes (VerifyDirectory checks hash/size/name, not
// parquet content), plus a matching manifest and Compaction state record.
func fabricateRevisionMaterializes(t *testing.T, root string, schemaVersion int, expectedKind string, stuTotal *int64) *model.Compaction {
	t.Helper()
	relDir := filepath.Join("parquet", "format=v1", "source=demo", "stream=trips", "date=2026-08-12", "revision=1")
	finalDir := filepath.Join(root, relDir)
	if err := os.MkdirAll(finalDir, 0o750); err != nil {
		t.Fatal(err)
	}
	payload := []byte("PAR1fabricated-bytes")
	staged := filepath.Join(finalDir, "artifact.tmp")
	if err := os.WriteFile(staged, payload, 0o640); err != nil {
		t.Fatal(err)
	}
	hash, size, err := hashFile(staged)
	if err != nil {
		t.Fatal(err)
	}
	name := "data-" + hash + ".parquet"
	if err := os.Rename(staged, filepath.Join(finalDir, name)); err != nil {
		t.Fatal(err)
	}
	// captured (3) must differ from stuTotal (5) in the caller so artifact-row
	// dispatch is discriminating; with equal numbers the positive case would
	// pass under both the old and new rule.
	entityTotal, captured := int64(2), int64(3)
	artifactRows := captured
	if stuTotal != nil {
		artifactRows = *stuTotal
	}
	manifest := model.Manifest{
		ManifestVersion: model.ManifestFormatVersion, DatasetStatus: "ready",
		SourceID: "demo", StreamID: "trips", ExpectedKind: expectedKind,
		ArchiveDate: "2026-08-12", Timezone: "UTC",
		FormatVersion: model.ParquetFormatVersion, SchemaVersion: schemaVersion,
		Revision: 1, CapturedResponses: captured, EntityTotal: entityTotal,
		StopTimeUpdateTotal: stuTotal,
		Files:               []model.Artifact{{RelativePath: name, Part: 0, Bytes: size, SHA256: hash, Rows: artifactRows, Entities: entityTotal}},
		InputCaptures:       []model.InputCapture{{ID: "c1", SHA256: "ignored-by-verify"}, {ID: "c2", SHA256: "ignored-by-verify"}, {ID: "c3", SHA256: "ignored-by-verify"}},
	}
	if err := writeJSONFile(filepath.Join(finalDir, "manifest.json"), manifest); err != nil {
		t.Fatal(err)
	}
	return &model.Compaction{
		SourceID: "demo", StreamID: "trips", ArchiveDate: "2026-08-12", Revision: 1,
		Directory: filepath.ToSlash(relDir), DataPath: filepath.ToSlash(filepath.Join(relDir, name)),
		ManifestPath: filepath.ToSlash(filepath.Join(relDir, "manifest.json")),
		DataSHA256:   hash, DataBytes: size, Rows: captured, Entities: entityTotal,
	}
}

func TestVerifyDirectoryFlatTripUpdateV2(t *testing.T) {
	root := t.TempDir()
	stu := int64(5)
	// Valid v2 trip-update revision: artifact rows == stop_time_update_total.
	if err := VerifyDirectory(root, fabricateRevisionMaterializes(t, root, model.ParquetSchemaVersionTripUpdatesFlattened, "trip_update", &stu)); err != nil {
		t.Fatalf("valid v2 revision rejected: %v", err)
	}
}

func TestVerifyDirectoryLegacyNestedV1TripUpdate(t *testing.T) {
	root := t.TempDir()
	// Valid v1 trip-update revision: nested layout, rows == captures, no total.
	if err := VerifyDirectory(root, fabricateRevisionMaterializes(t, root, model.ParquetSchemaVersionNested, "trip_update", nil)); err != nil {
		t.Fatalf("valid v1 trip_update revision rejected: %v", err)
	}
}

func TestVerifyDirectoryDispatchRejections(t *testing.T) {
	stu := int64(5)
	cases := []struct {
		name          string
		schemaVersion int
		expectedKind  string
		stuTotal      *int64
	}{
		{"unsupported schema version 3", 3, "trip_update", &stu},
		{"flattened layout on mixed stream", model.ParquetSchemaVersionTripUpdatesFlattened, "mixed", &stu},
		{"flattened layout without total", model.ParquetSchemaVersionTripUpdatesFlattened, "trip_update", nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			if err := VerifyDirectory(root, fabricateRevisionMaterializes(t, root, tc.schemaVersion, tc.expectedKind, tc.stuTotal)); err == nil {
				t.Fatal("expected rejection")
			}
		})
	}
}
