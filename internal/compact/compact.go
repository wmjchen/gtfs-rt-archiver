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
	} else {
		dataStaged = filepath.Join(workDir, "data.parquet")
		entities, err := c.writeParquet(ctx, dataStaged, captures, &manifest)
		if err != nil {
			return nil, err
		}
		manifest.EntityTotal = entities
		if err := validateParquet(dataStaged, captures); err != nil {
			return nil, fmt.Errorf("validate parquet: %w", err)
		}
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
		manifest.Files = append(manifest.Files, model.Artifact{RelativePath: name, Part: 0, Bytes: size, SHA256: hash, Rows: int64(len(captures)), Entities: entities})
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
	options := []parquet.WriterOption{parquet.MaxRowsPerRowGroup(100_000)}
	switch c.cfg.Parquet.Compression {
	case "zstd":
		options = append(options, parquet.Compression(&zstd.Codec{Concurrency: 1}))
	case "snappy":
		options = append(options, parquet.Compression(&snappy.Codec{}))
	case "uncompressed":
		options = append(options, parquet.Compression(&uncompressed.Codec{}))
	default:
		return 0, fmt.Errorf("unsupported parquet compression %q", c.cfg.Parquet.Compression)
	}
	writer := parquet.NewGenericWriter[Row](f, options...)
	writer.SetKeyValueMetadata("gtfsrt.format_version", fmt.Sprint(model.ParquetFormatVersion))
	writer.SetKeyValueMetadata("gtfsrt.schema_version", fmt.Sprint(manifest.SchemaVersion))
	writer.SetKeyValueMetadata("gtfsrt.application_version", manifest.ApplicationVersion)
	writer.SetKeyValueMetadata("gtfsrt.protobuf_revision", manifest.ProtobufRevision)
	writer.SetKeyValueMetadata("gtfsrt.config_fingerprint", manifest.ConfigFingerprint)
	writer.SetKeyValueMetadata("gtfsrt.source_id", manifest.SourceID)
	writer.SetKeyValueMetadata("gtfsrt.stream_id", manifest.StreamID)
	writer.SetKeyValueMetadata("gtfsrt.archive_date", manifest.ArchiveDate)
	writer.SetKeyValueMetadata("gtfsrt.timezone", manifest.Timezone)
	writer.SetKeyValueMetadata("gtfsrt.generated_at", manifest.CreatedAt.Format(time.RFC3339Nano))
	writer.SetKeyValueMetadata("gtfsrt.license_url", manifest.LicenseURL)
	writer.SetKeyValueMetadata("gtfsrt.attribution", manifest.Attribution)
	var groupBytes int64
	var entityTotal int64
	for _, capture := range captures {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		body, err := c.raw.Read(capture.RawPath)
		if err != nil {
			return 0, fmt.Errorf("read capture %s: %w", capture.ID, err)
		}
		h := sha256.Sum256(body)
		if hex.EncodeToString(h[:]) != capture.BodySHA256 {
			return 0, fmt.Errorf("capture %s hash mismatch", capture.ID)
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
// the given stream kind is written in. Trip-update flattening is wired in the
// compaction task of this plan; every other kind stays nested.
func schemaVersionForKind(expectedKind string) int {
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
