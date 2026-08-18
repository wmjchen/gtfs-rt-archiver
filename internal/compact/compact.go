package compact

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/compress/snappy"
	"github.com/parquet-go/parquet-go/compress/uncompressed"
	"github.com/parquet-go/parquet-go/compress/zstd"

	"gtfs-rt-archiver/internal/config"
	"gtfs-rt-archiver/internal/gtfsrt"
	"gtfs-rt-archiver/internal/model"
	"gtfs-rt-archiver/internal/projection"
	"gtfs-rt-archiver/internal/rawstore"
	"gtfs-rt-archiver/internal/state"
	"gtfs-rt-archiver/internal/version"
)

type Row struct {
	SourceID           string                   `parquet:"source_id,dict"`
	StreamID           string                   `parquet:"stream_id,dict"`
	ExpectedKind       string                   `parquet:"expected_kind,dict"`
	CaptureID          string                   `parquet:"capture_id"`
	ArchiveDate        string                   `parquet:"archive_date,dict"`
	ArchiveTimezone    string                   `parquet:"archive_timezone,dict"`
	SanitizedURL       string                   `parquet:"sanitized_url"`
	RawPath            string                   `parquet:"raw_path"`
	ConfigFingerprint  string                   `parquet:"config_fingerprint"`
	ApplicationVersion string                   `parquet:"application_version,dict"`
	ProtobufRevision   string                   `parquet:"protobuf_revision,dict"`
	ScheduledAt        time.Time                `parquet:"scheduled_at,timestamp(microsecond)"`
	StartedAt          time.Time                `parquet:"started_at,timestamp(microsecond)"`
	ObservedAt         time.Time                `parquet:"observed_at,timestamp(microsecond)"`
	HTTPStatus         int32                    `parquet:"http_status"`
	ResponseHeaders    string                   `parquet:"response_headers_json"`
	DurationMS         int64                    `parquet:"duration_ms"`
	AttemptCount       int32                    `parquet:"attempt_count"`
	AdvertisedLength   int64                    `parquet:"advertised_length"`
	EncodedLength      int64                    `parquet:"encoded_length"`
	DecodedLength      int64                    `parquet:"decoded_length"`
	ContentEncoding    string                   `parquet:"content_encoding,dict"`
	TransportComplete  bool                     `parquet:"transport_complete"`
	ParseStatus        string                   `parquet:"parse_status,dict"`
	ParseError         *string                  `parquet:"parse_error,optional"`
	BodySHA256         string                   `parquet:"body_sha256"`
	EntityCount        *int32                   `parquet:"entity_count,optional"`
	ValidationFlags    []string                 `parquet:"validation_flags"`
	Header             *projection.FeedHeader   `parquet:"header,optional"`
	Entities           []*projection.FeedEntity `parquet:"entities"`
	FeedMessagePB      []byte                   `parquet:"feed_message_pb"`
}

type Compactor struct {
	cfg   *config.Config
	raw   *rawstore.Store
	state *state.Store
	now   func() time.Time
}

func New(cfg *config.Config, raw *rawstore.Store, stateStore *state.Store) *Compactor {
	return &Compactor{cfg: cfg, raw: raw, state: stateStore, now: time.Now}
}

type Request struct {
	SourceID string
	StreamID string
	Date     string
	Revision int
}

func (c *Compactor) Compact(ctx context.Context, req Request) (*model.Compaction, error) {
	source, stream, err := c.cfg.FindSourceStream(req.SourceID, req.StreamID)
	if err != nil {
		return nil, err
	}
	loc, err := stream.EffectiveLocation(*source)
	if err != nil {
		return nil, err
	}
	day, err := time.ParseInLocation(time.DateOnly, req.Date, loc)
	if err != nil {
		return nil, fmt.Errorf("parse archive date: %w", err)
	}
	dayEnd := day.AddDate(0, 0, 1)
	nextRevision, err := c.state.NextRevision(ctx, source.ID, stream.ID, req.Date)
	if err != nil {
		return nil, fmt.Errorf("select revision: %w", err)
	}
	if req.Revision <= 0 {
		req.Revision = nextRevision
	} else if req.Revision != nextRevision {
		return nil, fmt.Errorf("revision must be the next immutable revision (%d)", nextRevision)
	}
	captures, err := c.state.CapturesForDay(ctx, source.ID, stream.ID, req.Date)
	if err != nil {
		return nil, err
	}
	stats, err := c.state.DayStats(ctx, source.ID, stream.ID, day, dayEnd)
	if err != nil {
		return nil, fmt.Errorf("query day statistics: %w", err)
	}

	relDir := filepath.Join("parquet", "format=v1", "source="+source.ID, "stream="+stream.ID,
		"date="+req.Date, fmt.Sprintf("revision=%d", req.Revision))
	finalDir, err := c.raw.Absolute(relDir)
	if err != nil {
		return nil, err
	}
	if _, err := os.Stat(finalDir); err == nil {
		return nil, fmt.Errorf("revision directory already exists: %s", relDir)
	}
	parent := filepath.Dir(finalDir)
	if err := os.MkdirAll(parent, 0o750); err != nil {
		return nil, fmt.Errorf("create compaction parent: %w", err)
	}
	workDir, err := os.MkdirTemp(filepath.Join(c.raw.Root(), "staging"), ".compaction-*")
	if err != nil {
		return nil, fmt.Errorf("create compaction staging directory: %w", err)
	}
	defer os.RemoveAll(workDir)

	created := c.now().UTC()
	manifest := model.Manifest{
		ManifestVersion: model.ManifestFormatVersion, DatasetStatus: "ready",
		SourceID: source.ID, StreamID: stream.ID, RegistryID: source.RegistryID,
		ExpectedKind: stream.ExpectedKind, ArchiveDate: req.Date, Timezone: loc.String(),
		DayStartUTC: day.UTC(), DayEndUTC: dayEnd.UTC(), FormatVersion: model.ParquetFormatVersion,
		SchemaVersion: schemaVersionForKind(stream.ExpectedKind), Revision: req.Revision,
		ApplicationVersion: version.Current().Version, ProtobufRevision: version.Current().ProtobufRevision,
		ConfigFingerprint: c.cfg.Fingerprint(), CreatedAt: created, ScheduledTicks: stats.Scheduled,
		SkippedTicks: stats.Skipped, CapturedResponses: int64(len(captures)), HTTPFailures: stats.HTTPFailures,
		NetworkFailures: stats.NetworkFailures, FailureCategoryCounts: stats.FailureCategoryCounts,
		ParseStatusCounts:    map[string]int64{},
		ValidationFlagCounts: map[string]int64{}, Files: []model.Artifact{}, InputCaptures: []model.InputCapture{},
		LicenseURL: source.LicenseURL,
	}
	if req.Revision > 1 {
		predecessor := req.Revision - 1
		manifest.PredecessorRevision = &predecessor
	}
	manifest.RequiredDestinations = requiredDestinationIDs(c.cfg.Destinations)
	manifest.Destinations = manifestDestinations(c.cfg.Destinations)
	sanitizedURL, err := manifestSanitizedURL(captures, stream)
	if err != nil {
		return nil, err
	}
	manifest.SanitizedURL = sanitizedURL
	if source.AttributionTextFile != "" {
		b, err := os.ReadFile(source.AttributionTextFile)
		if err != nil {
			return nil, fmt.Errorf("read attribution text: %w", err)
		}
		manifest.Attribution = strings.TrimSpace(string(b))
	}

	var dataStaged string
	if len(captures) == 0 {
		manifest.DatasetStatus = "no_captured_responses"
		if stream.ExpectedKind == "trip_update" {
			// The schema-v2 verifier requires the total on every trip-update
			// revision, including days with no captured responses.
			zero := int64(0)
			manifest.StopTimeUpdateTotal = &zero
		}
	} else {
		dataStaged = filepath.Join(workDir, "data.parquet")
		var entities int64
		artifactRows := int64(len(captures))
		if stream.ExpectedKind == "trip_update" {
			var stuTotal int64
			entities, stuTotal, err = c.writeTripUpdateParquet(ctx, dataStaged, captures, &manifest)
			if err != nil {
				return nil, err
			}
			manifest.StopTimeUpdateTotal = &stuTotal
			artifactRows = stuTotal
			if err := c.validateTripUpdateParquet(dataStaged, captures); err != nil {
				return nil, fmt.Errorf("validate parquet: %w", err)
			}
		} else {
			entities, err = c.writeParquet(ctx, dataStaged, captures, &manifest)
			if err != nil {
				return nil, err
			}
			if err := validateParquet(dataStaged, captures); err != nil {
				return nil, fmt.Errorf("validate parquet: %w", err)
			}
		}
		manifest.EntityTotal = entities
		hash, size, err := hashFile(dataStaged)
		if err != nil {
			return nil, fmt.Errorf("hash parquet: %w", err)
		}
		name := "data-" + hash + ".parquet"
		contentPath := filepath.Join(workDir, name)
		if err := os.Rename(dataStaged, contentPath); err != nil {
			return nil, fmt.Errorf("name content-addressed parquet: %w", err)
		}
		dataStaged = contentPath
		manifest.Files = append(manifest.Files, model.Artifact{RelativePath: name, Part: 0, Bytes: size, SHA256: hash, Rows: artifactRows, Entities: entities})
	}

	for _, capture := range captures {
		manifest.InputCaptures = append(manifest.InputCaptures, model.InputCapture{ID: capture.ID, SHA256: capture.BodySHA256, Bytes: capture.DecodedLength})
		manifest.ParseStatusCounts[capture.ParseStatus]++
		if capture.ParseStatus == "valid" && capture.TransportComplete {
			manifest.ValidSnapshots++
			if capture.FeedTimestamp != nil {
				if manifest.EarliestFeedTimestamp == nil || *capture.FeedTimestamp < *manifest.EarliestFeedTimestamp {
					value := *capture.FeedTimestamp
					manifest.EarliestFeedTimestamp = &value
				}
				if manifest.LatestFeedTimestamp == nil || *capture.FeedTimestamp > *manifest.LatestFeedTimestamp {
					value := *capture.FeedTimestamp
					manifest.LatestFeedTimestamp = &value
				}
			}
		} else {
			manifest.InvalidPayloads++
		}
		for _, flag := range capture.ValidationFlags {
			manifest.ValidationFlagCounts[flag]++
		}
		observed := capture.CompletedAt
		if manifest.EarliestCapture == nil {
			first := observed
			manifest.EarliestCapture = &first
		}
		if manifest.LatestCapture != nil {
			gap := observed.Sub(*manifest.LatestCapture).Milliseconds()
			if gap > manifest.LargestFetchGapMS {
				manifest.LargestFetchGapMS = gap
			}
		}
		last := observed
		manifest.LatestCapture = &last
	}

	manifestStaged := filepath.Join(workDir, "manifest.json")
	if err := writeJSONFile(manifestStaged, manifest); err != nil {
		return nil, err
	}
	var dataPath string
	var dataHash string
	var dataBytes int64
	if len(manifest.Files) != 0 {
		artifact := manifest.Files[0]
		dataPath = filepath.Join(relDir, artifact.RelativePath)
		dataHash, dataBytes = artifact.SHA256, artifact.Bytes
	}
	if err := syncDir(workDir); err != nil {
		return nil, fmt.Errorf("sync staged revision: %w", err)
	}
	if err := os.Rename(workDir, finalDir); err != nil {
		return nil, fmt.Errorf("commit revision directory: %w", err)
	}
	if err := syncDir(finalDir); err != nil {
		return nil, fmt.Errorf("sync revision: %w", err)
	}
	if err := syncDir(parent); err != nil {
		return nil, fmt.Errorf("sync revision parent: %w", err)
	}

	compaction := &model.Compaction{
		SourceID: source.ID, StreamID: stream.ID, ArchiveDate: req.Date, Revision: req.Revision,
		Status: "ready", Directory: filepath.ToSlash(relDir), DataPath: filepath.ToSlash(dataPath),
		ManifestPath: filepath.ToSlash(filepath.Join(relDir, "manifest.json")), DataSHA256: dataHash,
		DataBytes: dataBytes, Rows: int64(len(captures)), Entities: manifest.EntityTotal,
		RequiredDestinations: manifest.RequiredDestinations, CreatedAt: created,
	}
	if len(manifest.RequiredDestinations) == 0 && c.cfg.Storage.AllowLocalOnlyCleanup {
		published := created
		compaction.Status = "published"
		compaction.PublishedAt = &published
	}
	destinations := map[string]bool{}
	for _, dest := range c.cfg.Destinations {
		destinations[dest.ID] = dest.IsRequired()
	}
	// Once a complete manifest exists, preserve the immutable revision even if
	// the state transaction fails. Reconciliation can safely adopt it later.
	if err := c.state.SaveCompaction(ctx, compaction, captures, destinations); err != nil {
		return nil, fmt.Errorf("record compaction (files left for repair at %s): %w", relDir, err)
	}
	return compaction, nil
}

func (c *Compactor) writeParquet(ctx context.Context, path string, captures []model.Capture, manifest *model.Manifest) (int64, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o640)
	if err != nil {
		return 0, fmt.Errorf("create parquet: %w", err)
	}
	closeFile := true
	defer func() {
		if closeFile {
			_ = f.Close()
		}
	}()
	options, err := c.parquetWriterOptions()
	if err != nil {
		return 0, err
	}
	writer := parquet.NewGenericWriter[Row](f, options...)
	setManifestKeyValueMetadata(writer, manifest)
	var groupBytes int64
	var entityTotal int64
	for _, capture := range captures {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		body, err := readVerifiedCapture(c.raw, capture)
		if err != nil {
			return 0, err
		}
		meta := gtfsrt.Decode(body, capture.ExpectedKind, capture.CompletedAt)
		row := Row{
			SourceID: capture.SourceID, StreamID: capture.StreamID, ExpectedKind: capture.ExpectedKind,
			CaptureID: capture.ID, ArchiveDate: capture.ArchiveDate, ArchiveTimezone: capture.Timezone,
			SanitizedURL: capture.SanitizedURL,
			RawPath:      capture.RawPath, ConfigFingerprint: capture.ConfigFingerprint,
			ApplicationVersion: capture.ApplicationVersion, ProtobufRevision: capture.ProtobufRevision,
			ScheduledAt: capture.ScheduledAt,
			StartedAt:   capture.StartedAt, ObservedAt: capture.CompletedAt, HTTPStatus: int32(capture.HTTPStatus),
			ResponseHeaders: stableJSON(capture.ResponseHeaders), DurationMS: capture.DurationMS,
			AttemptCount: int32(capture.AttemptCount), AdvertisedLength: capture.AdvertisedLength,
			EncodedLength: capture.EncodedLength, DecodedLength: capture.DecodedLength,
			ContentEncoding: capture.ContentEncoding, TransportComplete: capture.TransportComplete,
			ParseStatus: capture.ParseStatus, BodySHA256: capture.BodySHA256,
			ValidationFlags: append([]string(nil), capture.ValidationFlags...), FeedMessagePB: body,
		}
		if capture.ParseError != "" {
			v := capture.ParseError
			row.ParseError = &v
		}
		if capture.EntityCount != nil {
			v := *capture.EntityCount
			row.EntityCount = &v
		}
		if meta.Message != nil {
			projected := projection.ProjectFeedMessage(meta.Message.ProtoReflect())
			row.Header, row.Entities = projected.Header, projected.Entity
		}
		if capture.EntityCount != nil {
			entityTotal += int64(*capture.EntityCount)
		}
		if _, err := writer.Write([]Row{row}); err != nil {
			return 0, fmt.Errorf("write parquet row %s: %w", capture.ID, err)
		}
		groupBytes += capture.DecodedLength
		if groupBytes >= c.cfg.Parquet.TargetRowGroupBytes {
			if err := writer.Flush(); err != nil {
				return 0, fmt.Errorf("flush parquet row group: %w", err)
			}
			groupBytes = 0
		}
	}
	if err := writer.Close(); err != nil {
		return 0, fmt.Errorf("close parquet writer: %w", err)
	}
	if err := f.Sync(); err != nil {
		return 0, fmt.Errorf("sync parquet: %w", err)
	}
	if err := f.Close(); err != nil {
		return 0, fmt.Errorf("close parquet file: %w", err)
	}
	closeFile = false
	return entityTotal, nil
}

// parquetWriterOptions builds the shared writer options.
func (c *Compactor) parquetWriterOptions() ([]parquet.WriterOption, error) {
	options := []parquet.WriterOption{parquet.MaxRowsPerRowGroup(100_000)}
	switch c.cfg.Parquet.Compression {
	case "zstd":
		options = append(options, parquet.Compression(&zstd.Codec{Concurrency: 1}))
	case "snappy":
		options = append(options, parquet.Compression(&snappy.Codec{}))
	case "uncompressed":
		options = append(options, parquet.Compression(&uncompressed.Codec{}))
	default:
		return nil, fmt.Errorf("unsupported parquet compression %q", c.cfg.Parquet.Compression)
	}
	return options, nil
}

// setManifestKeyValueMetadata stamps the manifest-derived key/value metadata
// onto a parquet writer. Generic so both writers reuse it.
func setManifestKeyValueMetadata[T any](w *parquet.GenericWriter[T], manifest *model.Manifest) {
	w.SetKeyValueMetadata("gtfsrt.format_version", fmt.Sprint(model.ParquetFormatVersion))
	w.SetKeyValueMetadata("gtfsrt.schema_version", fmt.Sprint(manifest.SchemaVersion))
	w.SetKeyValueMetadata("gtfsrt.application_version", manifest.ApplicationVersion)
	w.SetKeyValueMetadata("gtfsrt.protobuf_revision", manifest.ProtobufRevision)
	w.SetKeyValueMetadata("gtfsrt.config_fingerprint", manifest.ConfigFingerprint)
	w.SetKeyValueMetadata("gtfsrt.source_id", manifest.SourceID)
	w.SetKeyValueMetadata("gtfsrt.stream_id", manifest.StreamID)
	w.SetKeyValueMetadata("gtfsrt.archive_date", manifest.ArchiveDate)
	w.SetKeyValueMetadata("gtfsrt.timezone", manifest.Timezone)
	w.SetKeyValueMetadata("gtfsrt.generated_at", manifest.CreatedAt.Format(time.RFC3339Nano))
	w.SetKeyValueMetadata("gtfsrt.license_url", manifest.LicenseURL)
	w.SetKeyValueMetadata("gtfsrt.attribution", manifest.Attribution)
}

// readVerifiedCapture reads a capture body from the raw store and re-checks its
// recorded SHA-256 (SPEC §12.2).
func readVerifiedCapture(raw *rawstore.Store, capture model.Capture) ([]byte, error) {
	body, err := raw.Read(capture.RawPath)
	if err != nil {
		return nil, fmt.Errorf("read capture %s: %w", capture.ID, err)
	}
	h := sha256.Sum256(body)
	if hex.EncodeToString(h[:]) != capture.BodySHA256 {
		return nil, fmt.Errorf("capture %s hash mismatch", capture.ID)
	}
	return body, nil
}

// tripUpdateRowGroupRows bounds flattened trip-update row groups by row count
// rather than decoded bytes, since one 7 MB capture can hold 100k+
// stop_time_update rows. Test-overridable.
var tripUpdateRowGroupRows = 100_000

func tripUpdateProvenanceFromCapture(capture model.Capture) projection.TripUpdateProvenance {
	return projection.TripUpdateProvenance{
		SourceFile:      capture.RawPath,
		FeedURL:         capture.SanitizedURL,
		FetchTimestamp:  capture.CompletedAt,
		SourceID:        capture.SourceID,
		StreamID:        capture.StreamID,
		CaptureID:       capture.ID,
		ArchiveDate:     capture.ArchiveDate,
		ArchiveTimezone: capture.Timezone,
		ScheduledAt:     capture.ScheduledAt,
		ParseStatus:     capture.ParseStatus,
		ValidationFlags: capture.ValidationFlags,
	}
}

func (c *Compactor) writeTripUpdateParquet(ctx context.Context, path string, captures []model.Capture, manifest *model.Manifest) (entityTotal, stopTimeUpdateTotal int64, err error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o640)
	if err != nil {
		return 0, 0, fmt.Errorf("create parquet: %w", err)
	}
	closeFile := true
	defer func() {
		if closeFile {
			_ = f.Close()
		}
	}()
	options, err := c.parquetWriterOptions()
	if err != nil {
		return 0, 0, err
	}
	// Bind the writer's own row-group limit to the row-count var too: the
	// explicit Flush below only fires between captures, so without this a
	// single capture larger than tripUpdateRowGroupRows would land in one
	// row group instead of splitting at the row-count bound.
	options = append(options, parquet.MaxRowsPerRowGroup(int64(tripUpdateRowGroupRows)))
	writer := parquet.NewGenericWriter[projection.TripUpdateStopRow](f, options...)
	setManifestKeyValueMetadata(writer, manifest)
	var rowsInGroup int
	for _, capture := range captures {
		if err := ctx.Err(); err != nil {
			return 0, 0, err
		}
		body, err := readVerifiedCapture(c.raw, capture)
		if err != nil {
			return 0, 0, err
		}
		meta := gtfsrt.Decode(body, capture.ExpectedKind, capture.CompletedAt)
		if capture.EntityCount != nil {
			entityTotal += int64(*capture.EntityCount)
		}
		if meta.Message == nil {
			continue // undecodable captures keep raw-store + manifest accounting only
		}
		rows := projection.ProjectTripUpdateStops(meta.Message, tripUpdateProvenanceFromCapture(capture))
		stopTimeUpdateTotal += projection.CountStopTimeUpdates(meta.Message)
		if len(rows) == 0 {
			continue
		}
		if _, err := writer.Write(rows); err != nil {
			return 0, 0, fmt.Errorf("write parquet rows for %s: %w", capture.ID, err)
		}
		rowsInGroup += len(rows)
		if rowsInGroup >= tripUpdateRowGroupRows {
			if err := writer.Flush(); err != nil {
				return 0, 0, fmt.Errorf("flush parquet row group: %w", err)
			}
			rowsInGroup = 0
		}
	}
	if err := writer.Close(); err != nil {
		return 0, 0, fmt.Errorf("close parquet writer: %w", err)
	}
	if err := f.Sync(); err != nil {
		return 0, 0, fmt.Errorf("sync parquet: %w", err)
	}
	if err := f.Close(); err != nil {
		return 0, 0, fmt.Errorf("close parquet file: %w", err)
	}
	closeFile = false
	return entityTotal, stopTimeUpdateTotal, nil
}

func (c *Compactor) validateTripUpdateParquet(path string, captures []model.Capture) error {
	// Phase A: recompute per-capture expectations from the re-decoded raw
	// inputs. Only counts are kept — memory is bounded by capture count/day.
	type expectation struct{ rows int64 }
	expected := make([]expectation, len(captures))
	var totalRows, totalStu int64
	for i, capture := range captures {
		body, err := readVerifiedCapture(c.raw, capture)
		if err != nil {
			return fmt.Errorf("validate parquet: %w", err)
		}
		meta := gtfsrt.Decode(body, capture.ExpectedKind, capture.CompletedAt)
		if meta.Message == nil {
			continue
		}
		expected[i].rows = projection.CountProjectedTripUpdateRows(meta.Message)
		totalStu += projection.CountStopTimeUpdates(meta.Message)
		totalRows += expected[i].rows
	}
	_ = totalStu // stu total is accounted by the write path; row check covers it

	// Phase B: stream parquet rows, asserting capture grouping order and
	// per-capture counts.
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	reader := parquet.NewGenericReader[projection.TripUpdateStopRow](f)
	defer reader.Close()
	buf := make([]projection.TripUpdateStopRow, 1024)
	captureIdx, rowsLeftInCapture, seen := 0, int64(0), int64(0)
	for {
		n, readErr := reader.Read(buf)
		for i := 0; i < n; i++ {
			for rowsLeftInCapture == 0 {
				if captureIdx >= len(expected) {
					return errors.New("parquet contains extra rows")
				}
				rowsLeftInCapture = expected[captureIdx].rows
				captureIdx++
			}
			row := buf[i]
			capture := captures[captureIdx-1]
			if row.CaptureID != capture.ID {
				return fmt.Errorf("row %d provenance mismatch: capture %s rows carry capture %s", seen, capture.ID, row.CaptureID)
			}
			rowsLeftInCapture--
			seen++
		}
		if errors.Is(readErr, io.EOF) {
			break
		}
		if readErr != nil {
			return readErr
		}
	}
	if seen != totalRows {
		return fmt.Errorf("parquet row count %d does not match %d expected from inputs", seen, totalRows)
	}
	return nil
}

func validateParquet(path string, expected []model.Capture) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	reader := parquet.NewGenericReader[Row](f)
	defer reader.Close()
	// A row can contain the maximum-sized response. Keeping this buffer at one
	// prevents verification from multiplying that memory bound by a batch.
	buf := make([]Row, 1)
	index := 0
	var entityTotal int64
	for {
		n, readErr := reader.Read(buf)
		for i := 0; i < n; i++ {
			if index >= len(expected) {
				return errors.New("parquet contains extra rows")
			}
			row := buf[i]
			capture := expected[index]
			if row.CaptureID != capture.ID || row.BodySHA256 != capture.BodySHA256 {
				return fmt.Errorf("row %d provenance mismatch", index)
			}
			h := sha256.Sum256(row.FeedMessagePB)
			if hex.EncodeToString(h[:]) != capture.BodySHA256 {
				return fmt.Errorf("row %d protobuf hash mismatch", index)
			}
			if row.EntityCount == nil && capture.EntityCount != nil || row.EntityCount != nil && capture.EntityCount == nil ||
				row.EntityCount != nil && capture.EntityCount != nil && *row.EntityCount != *capture.EntityCount {
				return fmt.Errorf("row %d entity count mismatch", index)
			}
			if row.EntityCount != nil {
				entityTotal += int64(*row.EntityCount)
			}
			index++
		}
		if errors.Is(readErr, io.EOF) {
			break
		}
		if readErr != nil {
			return readErr
		}
	}
	if index != len(expected) {
		return fmt.Errorf("parquet row count %d does not match %d inputs", index, len(expected))
	}
	var expectedEntities int64
	for _, capture := range expected {
		if capture.EntityCount != nil {
			expectedEntities += int64(*capture.EntityCount)
		}
	}
	if entityTotal != expectedEntities {
		return fmt.Errorf("parquet entity count %d does not match %d inputs", entityTotal, expectedEntities)
	}
	return nil
}

// expectedArtifactRows derives the row count the revision's single parquet
// artifact must contain, by schema version. Nested revisions hold one row per
// captured response; flattened trip-update revisions hold one row per
// stop_time_update (plus zero-STU base rows, which are not counted here).
func expectedArtifactRows(manifest model.Manifest) (int64, error) {
	if manifest.SchemaVersion == model.ParquetSchemaVersionTripUpdatesFlattened {
		if manifest.ExpectedKind != "trip_update" {
			return 0, fmt.Errorf("schema version %d is only valid for trip_update streams, got %q", model.ParquetSchemaVersionTripUpdatesFlattened, manifest.ExpectedKind)
		}
		if manifest.StopTimeUpdateTotal == nil {
			return 0, fmt.Errorf("flattened trip_update manifest lacks stop_time_update_total")
		}
		return *manifest.StopTimeUpdateTotal, nil
	}
	return manifest.CapturedResponses, nil
}

func VerifyDirectory(root string, compaction *model.Compaction) error {
	manifestPath, err := safeRootPath(root, compaction.ManifestPath)
	if err != nil {
		return err
	}
	b, err := os.ReadFile(manifestPath)
	if err != nil {
		return fmt.Errorf("read manifest: %w", err)
	}
	var manifest model.Manifest
	if err := json.Unmarshal(b, &manifest); err != nil {
		return fmt.Errorf("decode manifest: %w", err)
	}
	if manifest.ManifestVersion != model.ManifestFormatVersion || manifest.FormatVersion != model.ParquetFormatVersion {
		return errors.New("manifest uses an unsupported format version")
	}
	if !model.IsSupportedParquetSchemaVersion(manifest.SchemaVersion) {
		return errors.New("manifest uses an unsupported schema version")
	}
	if manifest.SourceID != compaction.SourceID || manifest.StreamID != compaction.StreamID || manifest.ArchiveDate != compaction.ArchiveDate || manifest.Revision != compaction.Revision {
		return errors.New("manifest identity does not match state")
	}
	if int64(len(manifest.InputCaptures)) != manifest.CapturedResponses || manifest.CapturedResponses != compaction.Rows || manifest.EntityTotal != compaction.Entities {
		return errors.New("manifest aggregate counts do not match state")
	}
	switch manifest.DatasetStatus {
	case "ready":
		if manifest.CapturedResponses <= 0 || len(manifest.Files) != 1 {
			return errors.New("ready manifest must contain one non-empty capture artifact")
		}
	case "no_captured_responses":
		if manifest.CapturedResponses != 0 || len(manifest.Files) != 0 {
			return errors.New("empty manifest contains capture data")
		}
	default:
		return errors.New("manifest has an invalid dataset status")
	}
	if len(manifest.Destinations) != 0 {
		required := make([]string, 0, len(manifest.Destinations))
		seen := map[string]bool{}
		for _, destination := range manifest.Destinations {
			if destination.ID == "" || seen[destination.ID] {
				return errors.New("manifest has an invalid destination snapshot")
			}
			seen[destination.ID] = true
			if destination.Required {
				required = append(required, destination.ID)
			}
		}
		sort.Strings(required)
		expectedRequired := append([]string(nil), manifest.RequiredDestinations...)
		sort.Strings(expectedRequired)
		if strings.Join(required, "\x00") != strings.Join(expectedRequired, "\x00") {
			return errors.New("manifest destination snapshot is inconsistent")
		}
	}
	expectedRows, err := expectedArtifactRows(manifest)
	if err != nil {
		return err
	}
	for _, artifact := range manifest.Files {
		if filepath.Base(artifact.RelativePath) != artifact.RelativePath {
			return errors.New("manifest artifact path is not a basename")
		}
		path := filepath.Join(filepath.Dir(manifestPath), artifact.RelativePath)
		info, err := os.Lstat(path)
		if err != nil {
			return err
		}
		if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
			return errors.New("manifest artifact is not a regular file")
		}
		hash, size, err := hashFile(path)
		if err != nil {
			return err
		}
		if hash != artifact.SHA256 || size != artifact.Bytes {
			return fmt.Errorf("artifact %s integrity mismatch", artifact.RelativePath)
		}
		if artifact.RelativePath != "data-"+artifact.SHA256+".parquet" || artifact.Rows != expectedRows || artifact.Entities != manifest.EntityTotal {
			return fmt.Errorf("artifact %s metadata mismatch", artifact.RelativePath)
		}
		if compaction.DataPath != filepath.ToSlash(filepath.Join(compaction.Directory, artifact.RelativePath)) || compaction.DataSHA256 != artifact.SHA256 || compaction.DataBytes != artifact.Bytes {
			return errors.New("manifest artifact does not match compaction state")
		}
	}
	return nil
}

func safeRootPath(root, relative string) (string, error) {
	if filepath.IsAbs(relative) {
		return "", errors.New("artifact path must be relative")
	}
	root = filepath.Clean(root)
	artifactPath := filepath.Join(root, filepath.FromSlash(relative))
	rel, err := filepath.Rel(root, artifactPath)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return "", errors.New("artifact path escapes storage root")
	}
	return artifactPath, nil
}

// schemaVersionForKind selects the parquet row-layout version a revision of
// the given stream kind is written in: trip_update streams flatten to one row
// per stop_time_update; every other kind stays nested.
func schemaVersionForKind(expectedKind string) int {
	if expectedKind == "trip_update" {
		return model.ParquetSchemaVersionTripUpdatesFlattened
	}
	return model.ParquetSchemaVersionNested
}

func requiredDestinationIDs(destinations []config.Destination) []string {
	var ids []string
	for _, dest := range destinations {
		if dest.IsRequired() {
			ids = append(ids, dest.ID)
		}
	}
	sort.Strings(ids)
	return ids
}

func manifestDestinations(destinations []config.Destination) []model.ManifestDestination {
	result := make([]model.ManifestDestination, 0, len(destinations))
	for _, destination := range destinations {
		result = append(result, model.ManifestDestination{ID: destination.ID, Required: destination.IsRequired()})
	}
	sort.Slice(result, func(i, j int) bool { return result[i].ID < result[j].ID })
	return result
}

func stableJSON(v any) string { b, _ := json.Marshal(v); return string(b) }

func writeJSONFile(path string, value any) error {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o640)
	if err != nil {
		return err
	}
	enc := json.NewEncoder(f)
	enc.SetEscapeHTML(false)
	enc.SetIndent("", "  ")
	if err := enc.Encode(value); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}

func hashFile(path string) (string, int64, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", 0, err
	}
	defer f.Close()
	h := sha256.New()
	n, err := io.Copy(h, f)
	return hex.EncodeToString(h.Sum(nil)), n, err
}

func syncDir(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Sync()
}

// manifestSanitizedURL pins the feed's sanitized URL into the manifest so the
// publication partition identity survives later config URL edits. Captures of
// one stream/day always agree; a disagreement is an integrity failure.
func manifestSanitizedURL(captures []model.Capture, stream *config.Stream) (string, error) {
	if len(captures) == 0 {
		return config.SanitizedStreamURL(stream.URL)
	}
	value := captures[0].SanitizedURL
	for _, capture := range captures[1:] {
		if capture.SanitizedURL != value {
			return "", fmt.Errorf("captures disagree on sanitized URL: %q vs %q", value, capture.SanitizedURL)
		}
	}
	return value, nil
}
