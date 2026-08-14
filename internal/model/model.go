package model

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"time"
)

const (
	SidecarFormatVersion  = 1
	ManifestFormatVersion = 1
	ParquetFormatVersion  = 1
	ParquetSchemaVersion  = 1
)

type Tick struct {
	ID                string     `json:"tick_id"`
	SourceID          string     `json:"source_id"`
	StreamID          string     `json:"stream_id"`
	ScheduledAt       time.Time  `json:"scheduled_at"`
	StartedAt         *time.Time `json:"started_at,omitempty"`
	FinishedAt        *time.Time `json:"finished_at,omitempty"`
	Result            string     `json:"result"`
	SkipReason        string     `json:"skip_reason,omitempty"`
	ErrorCategory     string     `json:"error_category,omitempty"`
	ErrorDetail       string     `json:"error_detail,omitempty"`
	HTTPStatus        int        `json:"http_status,omitempty"`
	Attempts          int        `json:"attempts"`
	ConfigFingerprint string     `json:"config_fingerprint"`
}

type Capture struct {
	FormatVersion      int               `json:"format_version"`
	ID                 string            `json:"capture_id"`
	TickID             string            `json:"tick_id"`
	SourceID           string            `json:"source_id"`
	StreamID           string            `json:"stream_id"`
	ExpectedKind       string            `json:"expected_kind"`
	ScheduledAt        time.Time         `json:"scheduled_at"`
	StartedAt          time.Time         `json:"started_at"`
	CompletedAt        time.Time         `json:"completed_at"`
	ArchiveDate        string            `json:"archive_date"`
	Timezone           string            `json:"timezone"`
	SanitizedURL       string            `json:"sanitized_url"`
	HTTPStatus         int               `json:"http_status"`
	ResponseHeaders    map[string]string `json:"response_headers,omitempty"`
	DurationMS         int64             `json:"duration_ms"`
	AttemptCount       int               `json:"attempt_count"`
	AdvertisedLength   int64             `json:"advertised_length"`
	EncodedLength      int64             `json:"encoded_length"`
	DecodedLength      int64             `json:"decoded_length"`
	ContentEncoding    string            `json:"content_encoding,omitempty"`
	BodySHA256         string            `json:"body_sha256"`
	RawPath            string            `json:"raw_path"`
	SidecarPath        string            `json:"sidecar_path"`
	TransportComplete  bool              `json:"transport_complete"`
	ParseStatus        string            `json:"parse_status"`
	ParseError         string            `json:"parse_error,omitempty"`
	FeedVersion        string            `json:"feed_version,omitempty"`
	Incrementality     *int32            `json:"incrementality,omitempty"`
	FeedTimestamp      *uint64           `json:"feed_timestamp,omitempty"`
	EntityCount        *int32            `json:"entity_count,omitempty"`
	ValidationFlags    []string          `json:"validation_flags"`
	ConfigFingerprint  string            `json:"config_fingerprint"`
	ApplicationVersion string            `json:"application_version"`
	ProtobufRevision   string            `json:"protobuf_revision"`
}

type Artifact struct {
	RelativePath string `json:"relative_path"`
	Part         int    `json:"part"`
	Bytes        int64  `json:"bytes"`
	SHA256       string `json:"sha256"`
	Rows         int64  `json:"rows"`
	Entities     int64  `json:"entities"`
}

type Manifest struct {
	ManifestVersion       int                   `json:"manifest_version"`
	DatasetStatus         string                `json:"dataset_status"`
	SourceID              string                `json:"source_id"`
	StreamID              string                `json:"stream_id"`
	RegistryID            string                `json:"registry_id,omitempty"`
	ExpectedKind          string                `json:"expected_kind"`
	ArchiveDate           string                `json:"archive_date"`
	Timezone              string                `json:"timezone"`
	DayStartUTC           time.Time             `json:"day_start_utc"`
	DayEndUTC             time.Time             `json:"day_end_utc"`
	FormatVersion         int                   `json:"format_version"`
	SchemaVersion         int                   `json:"schema_version"`
	Revision              int                   `json:"revision"`
	PredecessorRevision   *int                  `json:"predecessor_revision,omitempty"`
	ApplicationVersion    string                `json:"application_version"`
	ProtobufRevision      string                `json:"protobuf_revision"`
	ConfigFingerprint     string                `json:"config_fingerprint"`
	CreatedAt             time.Time             `json:"created_at"`
	ScheduledTicks        int64                 `json:"scheduled_ticks"`
	SkippedTicks          int64                 `json:"skipped_ticks"`
	CapturedResponses     int64                 `json:"captured_responses"`
	ValidSnapshots        int64                 `json:"valid_snapshots"`
	InvalidPayloads       int64                 `json:"invalid_payloads"`
	HTTPFailures          int64                 `json:"http_failures"`
	NetworkFailures       int64                 `json:"network_failures"`
	FailureCategoryCounts map[string]int64      `json:"failure_category_counts"`
	EntityTotal           int64                 `json:"entity_total"`
	EarliestCapture       *time.Time            `json:"earliest_capture,omitempty"`
	LatestCapture         *time.Time            `json:"latest_capture,omitempty"`
	EarliestFeedTimestamp *uint64               `json:"earliest_feed_timestamp,omitempty"`
	LatestFeedTimestamp   *uint64               `json:"latest_feed_timestamp,omitempty"`
	LargestFetchGapMS     int64                 `json:"largest_fetch_gap_ms"`
	ParseStatusCounts     map[string]int64      `json:"parse_status_counts"`
	ValidationFlagCounts  map[string]int64      `json:"validation_flag_counts"`
	Files                 []Artifact            `json:"files"`
	InputCaptures         []InputCapture        `json:"input_captures"`
	LicenseURL            string                `json:"license_url,omitempty"`
	Attribution           string                `json:"attribution,omitempty"`
	RequiredDestinations  []string              `json:"required_destinations"`
	Destinations          []ManifestDestination `json:"destinations"`
}

type ManifestDestination struct {
	ID       string `json:"id"`
	Required bool   `json:"required"`
}

type InputCapture struct {
	ID     string `json:"id"`
	SHA256 string `json:"sha256"`
	Bytes  int64  `json:"bytes"`
}

type Compaction struct {
	ID                   int64
	SourceID             string
	StreamID             string
	ArchiveDate          string
	Revision             int
	Status               string
	Directory            string
	DataPath             string
	ManifestPath         string
	DataSHA256           string
	DataBytes            int64
	Rows                 int64
	Entities             int64
	RequiredDestinations []string
	CreatedAt            time.Time
	PublishedAt          *time.Time
}

type Upload struct {
	ID            int64
	CompactionID  int64
	DestinationID string
	Required      bool
	Status        string
	AttemptCount  int
	NextAttemptAt time.Time
	LastError     string
	RemotePath    string
	CompletedAt   *time.Time
}

func NewID(now time.Time) (string, error) {
	var random [12]byte
	if _, err := rand.Read(random[:]); err != nil {
		return "", fmt.Errorf("generate id: %w", err)
	}
	return now.UTC().Format("20060102T150405.000000000Z_") + hex.EncodeToString(random[:]), nil
}
