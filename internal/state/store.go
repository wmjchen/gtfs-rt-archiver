package state

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"gtfs-rt-archiver/internal/model"

	_ "modernc.org/sqlite"
)

const timeLayout = time.RFC3339Nano

type Store struct {
	db *sql.DB
}

func Open(ctx context.Context, path string) (*Store, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		return nil, fmt.Errorf("create state directory: %w", err)
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("open state database: %w", err)
	}
	db.SetMaxOpenConns(1)
	s := &Store{db: db}
	if err := s.initialize(ctx); err != nil {
		db.Close()
		return nil, err
	}
	return s, nil
}

func (s *Store) initialize(ctx context.Context) error {
	for _, pragma := range []string{
		"PRAGMA journal_mode=WAL",
		"PRAGMA synchronous=FULL",
		"PRAGMA foreign_keys=ON",
		"PRAGMA busy_timeout=5000",
	} {
		if _, err := s.db.ExecContext(ctx, pragma); err != nil {
			return fmt.Errorf("state %s: %w", pragma, err)
		}
	}
	if _, err := s.db.ExecContext(ctx, schemaV1); err != nil {
		return fmt.Errorf("create state schema: %w", err)
	}
	if _, err := s.db.ExecContext(ctx, `INSERT OR IGNORE INTO schema_migrations(version, applied_at) VALUES (1, ?)`, encodeTime(time.Now())); err != nil {
		return fmt.Errorf("record state migration: %w", err)
	}
	return nil
}

func (s *Store) Close() error { return s.db.Close() }

func (s *Store) Checkpoint(ctx context.Context) error {
	_, err := s.db.ExecContext(ctx, "PRAGMA wal_checkpoint(TRUNCATE)")
	return err
}

func (s *Store) IntegrityCheck(ctx context.Context) error {
	var result string
	if err := s.db.QueryRowContext(ctx, "PRAGMA integrity_check").Scan(&result); err != nil {
		return err
	}
	if result != "ok" {
		return fmt.Errorf("sqlite integrity check: %s", result)
	}
	return nil
}

func (s *Store) RecordTick(ctx context.Context, tick model.Tick) error {
	_, err := s.db.ExecContext(ctx, `
INSERT INTO ticks(id, source_id, stream_id, scheduled_at, started_at, finished_at, result,
                  skip_reason, error_category, error_detail, http_status, attempts, config_fingerprint)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(id) DO UPDATE SET
  started_at=excluded.started_at, finished_at=excluded.finished_at, result=excluded.result,
  skip_reason=excluded.skip_reason, error_category=excluded.error_category,
  error_detail=excluded.error_detail, http_status=excluded.http_status, attempts=excluded.attempts`,
		tick.ID, tick.SourceID, tick.StreamID, encodeTime(tick.ScheduledAt), encodeTimePtr(tick.StartedAt),
		encodeTimePtr(tick.FinishedAt), tick.Result, tick.SkipReason, tick.ErrorCategory,
		tick.ErrorDetail, tick.HTTPStatus, tick.Attempts, tick.ConfigFingerprint)
	if err != nil {
		return fmt.Errorf("record tick: %w", err)
	}
	return nil
}

func (s *Store) SaveCapture(ctx context.Context, tick model.Tick, capture model.Capture) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin capture transaction: %w", err)
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `
INSERT INTO ticks(id, source_id, stream_id, scheduled_at, started_at, finished_at, result,
                  skip_reason, error_category, error_detail, http_status, attempts, config_fingerprint)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(id) DO UPDATE SET
  started_at=excluded.started_at, finished_at=excluded.finished_at, result=excluded.result,
  skip_reason=excluded.skip_reason, error_category=excluded.error_category,
  error_detail=excluded.error_detail, http_status=excluded.http_status, attempts=excluded.attempts`,
		tick.ID, tick.SourceID, tick.StreamID, encodeTime(tick.ScheduledAt), encodeTimePtr(tick.StartedAt),
		encodeTimePtr(tick.FinishedAt), tick.Result, tick.SkipReason, tick.ErrorCategory,
		tick.ErrorDetail, tick.HTTPStatus, tick.Attempts, tick.ConfigFingerprint); err != nil {
		return fmt.Errorf("record capture tick: %w", err)
	}
	flags, _ := json.Marshal(capture.ValidationFlags)
	if _, err := tx.ExecContext(ctx, `
INSERT INTO captures(
  id, tick_id, source_id, stream_id, expected_kind, scheduled_at, started_at, completed_at,
  archive_date, timezone, sanitized_url, http_status, response_headers, duration_ms,
  attempt_count, advertised_length, encoded_length, decoded_length, content_encoding,
  body_sha256, raw_path, sidecar_path, transport_complete, parse_status, parse_error,
  feed_version, incrementality, feed_timestamp, entity_count, validation_flags,
  config_fingerprint, application_version, protobuf_revision, created_at)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		capture.ID, capture.TickID, capture.SourceID, capture.StreamID, capture.ExpectedKind,
		encodeTime(capture.ScheduledAt), encodeTime(capture.StartedAt), encodeTime(capture.CompletedAt),
		capture.ArchiveDate, capture.Timezone, capture.SanitizedURL, capture.HTTPStatus,
		mustJSON(capture.ResponseHeaders), capture.DurationMS, capture.AttemptCount,
		capture.AdvertisedLength, capture.EncodedLength, capture.DecodedLength, capture.ContentEncoding,
		capture.BodySHA256, capture.RawPath, capture.SidecarPath, boolInt(capture.TransportComplete),
		capture.ParseStatus, capture.ParseError, capture.FeedVersion, capture.Incrementality,
		encodeUint64Ptr(capture.FeedTimestamp), capture.EntityCount, string(flags), capture.ConfigFingerprint,
		capture.ApplicationVersion, capture.ProtobufRevision, encodeTime(time.Now())); err != nil {
		return fmt.Errorf("record capture: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit capture: %w", err)
	}
	return nil
}

func (s *Store) HasCapture(ctx context.Context, id string) (bool, error) {
	var one int
	err := s.db.QueryRowContext(ctx, `SELECT 1 FROM captures WHERE id=?`, id).Scan(&one)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	return err == nil, err
}

// LiveCaptureFiles returns the exact paths that should still exist locally.
// Retained captures use a sentinel path and are intentionally omitted.
func (s *Store) LiveCaptureFiles(ctx context.Context) ([]RetentionCapture, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT id, raw_path, sidecar_path FROM captures WHERE raw_path NOT LIKE '@retained/%' ORDER BY id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []RetentionCapture
	for rows.Next() {
		var item RetentionCapture
		if err := rows.Scan(&item.ID, &item.RawPath, &item.SidecarPath); err != nil {
			return nil, err
		}
		out = append(out, item)
	}
	return out, rows.Err()
}

func (s *Store) CapturesForDay(ctx context.Context, sourceID, streamID, date string) ([]model.Capture, error) {
	rows, err := s.db.QueryContext(ctx, `
SELECT id, tick_id, source_id, stream_id, expected_kind, scheduled_at, started_at, completed_at,
 archive_date, timezone, sanitized_url, http_status, response_headers, duration_ms,
 attempt_count, advertised_length, encoded_length, decoded_length, content_encoding,
 body_sha256, raw_path, sidecar_path, transport_complete, parse_status, parse_error,
 feed_version, incrementality, feed_timestamp, entity_count, validation_flags,
 config_fingerprint, application_version, protobuf_revision
FROM captures WHERE source_id=? AND stream_id=? AND archive_date=?
ORDER BY scheduled_at, id`, sourceID, streamID, date)
	if err != nil {
		return nil, fmt.Errorf("query captures: %w", err)
	}
	defer rows.Close()
	var captures []model.Capture
	for rows.Next() {
		var c model.Capture
		var scheduled, started, completed string
		var headers, flags string
		var transport int
		var incrementality sql.NullInt64
		var feedTimestamp sql.NullString
		var entityCount sql.NullInt64
		if err := rows.Scan(&c.ID, &c.TickID, &c.SourceID, &c.StreamID, &c.ExpectedKind,
			&scheduled, &started, &completed, &c.ArchiveDate, &c.Timezone, &c.SanitizedURL,
			&c.HTTPStatus, &headers, &c.DurationMS, &c.AttemptCount, &c.AdvertisedLength,
			&c.EncodedLength, &c.DecodedLength, &c.ContentEncoding, &c.BodySHA256,
			&c.RawPath, &c.SidecarPath, &transport, &c.ParseStatus, &c.ParseError,
			&c.FeedVersion, &incrementality, &feedTimestamp, &entityCount, &flags,
			&c.ConfigFingerprint, &c.ApplicationVersion, &c.ProtobufRevision); err != nil {
			return nil, fmt.Errorf("scan capture: %w", err)
		}
		c.FormatVersion = model.SidecarFormatVersion
		if c.ScheduledAt, err = time.Parse(timeLayout, scheduled); err != nil {
			return nil, fmt.Errorf("parse capture %s scheduled time: %w", c.ID, err)
		}
		if c.StartedAt, err = time.Parse(timeLayout, started); err != nil {
			return nil, fmt.Errorf("parse capture %s start time: %w", c.ID, err)
		}
		if c.CompletedAt, err = time.Parse(timeLayout, completed); err != nil {
			return nil, fmt.Errorf("parse capture %s completion time: %w", c.ID, err)
		}
		c.TransportComplete = transport != 0
		if incrementality.Valid {
			v := int32(incrementality.Int64)
			c.Incrementality = &v
		}
		if feedTimestamp.Valid {
			v, parseErr := strconv.ParseUint(feedTimestamp.String, 10, 64)
			if parseErr != nil {
				return nil, fmt.Errorf("parse capture %s feed timestamp: %w", c.ID, parseErr)
			}
			c.FeedTimestamp = &v
		}
		if entityCount.Valid {
			v := int32(entityCount.Int64)
			c.EntityCount = &v
		}
		if err := json.Unmarshal([]byte(headers), &c.ResponseHeaders); err != nil {
			return nil, fmt.Errorf("decode capture %s response headers: %w", c.ID, err)
		}
		if err := json.Unmarshal([]byte(flags), &c.ValidationFlags); err != nil {
			return nil, fmt.Errorf("decode capture %s validation flags: %w", c.ID, err)
		}
		captures = append(captures, c)
	}
	return captures, rows.Err()
}

type DayStats struct {
	Scheduled, Skipped, HTTPFailures, NetworkFailures int64
	FailureCategoryCounts                             map[string]int64
}

func (s *Store) EarliestTick(ctx context.Context, sourceID, streamID string) (*time.Time, error) {
	var value sql.NullString
	if err := s.db.QueryRowContext(ctx, `SELECT MIN(scheduled_at) FROM ticks WHERE source_id=? AND stream_id=?`, sourceID, streamID).Scan(&value); err != nil {
		return nil, err
	}
	if !value.Valid {
		return nil, nil
	}
	parsed, err := time.Parse(timeLayout, value.String)
	if err != nil {
		return nil, err
	}
	return &parsed, nil
}

func (s *Store) DayNeedsCompaction(ctx context.Context, sourceID, streamID string, start, end time.Time) (bool, error) {
	var summarized int
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM daily_summaries WHERE source_id=? AND stream_id=? AND archive_date=?`, sourceID, streamID, start.In(start.Location()).Format(time.DateOnly)).Scan(&summarized); err != nil {
		return false, err
	}
	if summarized != 0 {
		return false, nil
	}
	var ticks, captures int64
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM ticks WHERE source_id=? AND stream_id=? AND scheduled_at>=? AND scheduled_at<?`, sourceID, streamID, encodeTime(start), encodeTime(end)).Scan(&ticks); err != nil {
		return false, err
	}
	if ticks == 0 {
		return false, nil
	}
	date := start.In(start.Location()).Format(time.DateOnly)
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM captures WHERE source_id=? AND stream_id=? AND archive_date=?`, sourceID, streamID, date).Scan(&captures); err != nil {
		return false, err
	}
	var latestID, latestRows int64
	err := s.db.QueryRowContext(ctx, `SELECT id, rows FROM compactions WHERE source_id=? AND stream_id=? AND archive_date=? ORDER BY revision DESC LIMIT 1`, sourceID, streamID, date).Scan(&latestID, &latestRows)
	if errors.Is(err, sql.ErrNoRows) {
		return true, nil
	}
	if err != nil {
		return false, err
	}
	if latestRows != captures {
		return true, nil
	}
	var missing int64
	if err := s.db.QueryRowContext(ctx, `
SELECT COUNT(*) FROM captures c
WHERE c.source_id=? AND c.stream_id=? AND c.archive_date=?
  AND NOT EXISTS (SELECT 1 FROM compaction_inputs ci WHERE ci.compaction_id=? AND ci.capture_id=c.id)`,
		sourceID, streamID, date, latestID).Scan(&missing); err != nil {
		return false, err
	}
	return missing != 0, nil
}

type RetentionCapture struct{ ID, RawPath, SidecarPath string }
type RetentionCompaction struct {
	ID                              int64
	SourceID, StreamID, ArchiveDate string
	DataPath, ManifestPath          string
}

func (s *Store) RawRetentionCandidates(ctx context.Context, publishedBefore time.Time) ([]RetentionCapture, error) {
	rows, err := s.db.QueryContext(ctx, `
SELECT c.id, c.raw_path, c.sidecar_path
FROM captures c
JOIN compaction_inputs ci ON ci.capture_id=c.id
JOIN compactions cp ON cp.id=ci.compaction_id
WHERE c.raw_path NOT LIKE '@retained/%'
GROUP BY c.id, c.raw_path, c.sidecar_path
HAVING SUM(CASE WHEN cp.published_at IS NULL THEN 1 ELSE 0 END)=0
   AND MAX(cp.published_at)<=?`, encodeTime(publishedBefore))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []RetentionCapture
	for rows.Next() {
		var item RetentionCapture
		if err := rows.Scan(&item.ID, &item.RawPath, &item.SidecarPath); err != nil {
			return nil, err
		}
		out = append(out, item)
	}
	return out, rows.Err()
}

func (s *Store) ParquetRetentionCandidates(ctx context.Context, publishedBefore time.Time) ([]RetentionCompaction, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT id, source_id, stream_id, archive_date, data_path, manifest_path FROM compactions WHERE published_at IS NOT NULL AND published_at<=? AND manifest_path!=''`, encodeTime(publishedBefore))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []RetentionCompaction
	for rows.Next() {
		var item RetentionCompaction
		if err := rows.Scan(&item.ID, &item.SourceID, &item.StreamID, &item.ArchiveDate, &item.DataPath, &item.ManifestPath); err != nil {
			return nil, err
		}
		out = append(out, item)
	}
	return out, rows.Err()
}

type SummaryCandidate struct{ SourceID, StreamID, ArchiveDate string }

func (s *Store) SummaryCandidates(ctx context.Context, publishedBefore time.Time) ([]SummaryCandidate, error) {
	rows, err := s.db.QueryContext(ctx, `
SELECT source_id, stream_id, archive_date
FROM compactions c
WHERE NOT EXISTS (
  SELECT 1 FROM daily_summaries ds
  WHERE ds.source_id=c.source_id AND ds.stream_id=c.stream_id AND ds.archive_date=c.archive_date
)
GROUP BY source_id, stream_id, archive_date
HAVING SUM(CASE WHEN published_at IS NULL THEN 1 ELSE 0 END)=0
   AND MAX(published_at)<=?`, encodeTime(publishedBefore))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []SummaryCandidate
	for rows.Next() {
		var item SummaryCandidate
		if err := rows.Scan(&item.SourceID, &item.StreamID, &item.ArchiveDate); err != nil {
			return nil, err
		}
		out = append(out, item)
	}
	return out, rows.Err()
}

func (s *Store) SummarizeDay(ctx context.Context, sourceID, streamID, date string, start, end time.Time) (bool, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	var liveRaw int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM captures WHERE source_id=? AND stream_id=? AND archive_date=? AND raw_path NOT LIKE '@retained/%'`, sourceID, streamID, date).Scan(&liveRaw); err != nil {
		return false, err
	}
	if liveRaw != 0 {
		return false, nil
	}
	var ticks, skipped, httpFailures, networkFailures int64
	if err := tx.QueryRowContext(ctx, `
SELECT COUNT(*),
 COALESCE(SUM(CASE WHEN result='skipped' THEN 1 ELSE 0 END),0),
 COALESCE(SUM(CASE WHEN error_category='http_status' THEN 1 ELSE 0 END),0),
 COALESCE(SUM(CASE WHEN error_category IN ('dns','connect','tls','timeout','network') THEN 1 ELSE 0 END),0)
FROM ticks WHERE source_id=? AND stream_id=? AND scheduled_at>=? AND scheduled_at<?`, sourceID, streamID, encodeTime(start), encodeTime(end)).Scan(&ticks, &skipped, &httpFailures, &networkFailures); err != nil {
		return false, err
	}
	var captures, valid int64
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*), COALESCE(SUM(CASE WHEN parse_status='valid' AND transport_complete=1 THEN 1 ELSE 0 END),0) FROM captures WHERE source_id=? AND stream_id=? AND archive_date=?`, sourceID, streamID, date).Scan(&captures, &valid); err != nil {
		return false, err
	}
	parseCounts := map[string]int64{}
	rows, err := tx.QueryContext(ctx, `SELECT parse_status, COUNT(*) FROM captures WHERE source_id=? AND stream_id=? AND archive_date=? GROUP BY parse_status`, sourceID, streamID, date)
	if err != nil {
		return false, err
	}
	for rows.Next() {
		var status string
		var count int64
		if err := rows.Scan(&status, &count); err != nil {
			rows.Close()
			return false, err
		}
		parseCounts[status] = count
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return false, err
	}
	if err := rows.Close(); err != nil {
		return false, err
	}
	countsJSON, _ := json.Marshal(parseCounts)
	if _, err := tx.ExecContext(ctx, `
INSERT INTO daily_summaries(source_id, stream_id, archive_date, scheduled_ticks, skipped_ticks,
 captured_responses, valid_snapshots, invalid_payloads, http_failures, network_failures,
 parse_status_counts, summarized_at)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, sourceID, streamID, date, ticks, skipped,
		captures, valid, captures-valid, httpFailures, networkFailures, string(countsJSON), encodeTime(time.Now())); err != nil {
		return false, err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM compaction_inputs WHERE capture_id IN (SELECT id FROM captures WHERE source_id=? AND stream_id=? AND archive_date=?)`, sourceID, streamID, date); err != nil {
		return false, err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM captures WHERE source_id=? AND stream_id=? AND archive_date=?`, sourceID, streamID, date); err != nil {
		return false, err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM ticks WHERE source_id=? AND stream_id=? AND scheduled_at>=? AND scheduled_at<?`, sourceID, streamID, encodeTime(start), encodeTime(end)); err != nil {
		return false, err
	}
	if err := tx.Commit(); err != nil {
		return false, err
	}
	return true, nil
}

func (s *Store) MarkCaptureRetained(ctx context.Context, id string) error {
	_, err := s.db.ExecContext(ctx, `UPDATE captures SET raw_path='@retained/' || id || '.pb', sidecar_path='@retained/' || id || '.json' WHERE id=?`, id)
	return err
}

func (s *Store) MarkParquetRetained(ctx context.Context, id int64) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `UPDATE uploads SET status='expired', last_error='local_retention_elapsed' WHERE compaction_id=? AND required=0 AND status!='verified'`, id); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE compactions SET data_path='', manifest_path='', data_bytes=0 WHERE id=?`, id); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *Store) DayStats(ctx context.Context, sourceID, streamID string, start, end time.Time) (DayStats, error) {
	d := DayStats{FailureCategoryCounts: map[string]int64{}}
	err := s.db.QueryRowContext(ctx, `
SELECT COUNT(*),
       COALESCE(SUM(CASE WHEN result='skipped' THEN 1 ELSE 0 END),0),
       COALESCE(SUM(CASE WHEN error_category='http_status' THEN 1 ELSE 0 END),0),
       COALESCE(SUM(CASE WHEN error_category IN ('dns','connect','tls','timeout','network') THEN 1 ELSE 0 END),0)
FROM ticks WHERE source_id=? AND stream_id=? AND scheduled_at>=? AND scheduled_at<?`,
		sourceID, streamID, encodeTime(start), encodeTime(end)).Scan(&d.Scheduled, &d.Skipped, &d.HTTPFailures, &d.NetworkFailures)
	if err != nil {
		return d, err
	}
	rows, err := s.db.QueryContext(ctx, `SELECT error_category, COUNT(*) FROM ticks WHERE source_id=? AND stream_id=? AND scheduled_at>=? AND scheduled_at<? AND error_category!='' GROUP BY error_category`, sourceID, streamID, encodeTime(start), encodeTime(end))
	if err != nil {
		return d, err
	}
	defer rows.Close()
	for rows.Next() {
		var category string
		var count int64
		if err := rows.Scan(&category, &count); err != nil {
			return d, err
		}
		d.FailureCategoryCounts[category] = count
	}
	return d, rows.Err()
}

func (s *Store) NextRevision(ctx context.Context, sourceID, streamID, date string) (int, error) {
	var max sql.NullInt64
	if err := s.db.QueryRowContext(ctx, `SELECT MAX(revision) FROM compactions WHERE source_id=? AND stream_id=? AND archive_date=?`, sourceID, streamID, date).Scan(&max); err != nil {
		return 0, err
	}
	if !max.Valid {
		return 1, nil
	}
	return int(max.Int64) + 1, nil
}

func (s *Store) SaveCompaction(ctx context.Context, c *model.Compaction, inputs []model.Capture, destinations map[string]bool) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	requiredJSON, _ := json.Marshal(c.RequiredDestinations)
	result, err := tx.ExecContext(ctx, `
INSERT INTO compactions(source_id, stream_id, archive_date, format_version, revision, status,
 directory, data_path, manifest_path, data_sha256, data_bytes, rows, entities,
 required_destinations, created_at, published_at)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		c.SourceID, c.StreamID, c.ArchiveDate, model.ParquetFormatVersion, c.Revision, c.Status,
		c.Directory, c.DataPath, c.ManifestPath, c.DataSHA256, c.DataBytes, c.Rows,
		c.Entities, string(requiredJSON), encodeTime(c.CreatedAt), encodeTimePtr(c.PublishedAt))
	if err != nil {
		return fmt.Errorf("record compaction: %w", err)
	}
	c.ID, err = result.LastInsertId()
	if err != nil {
		return err
	}
	for _, input := range inputs {
		if _, err := tx.ExecContext(ctx, `INSERT INTO compaction_inputs(compaction_id, capture_id) VALUES (?, ?)`, c.ID, input.ID); err != nil {
			return fmt.Errorf("record compaction input: %w", err)
		}
	}
	for id, required := range destinations {
		if _, err := tx.ExecContext(ctx, `INSERT INTO uploads(compaction_id, destination_id, required, status, attempt_count, next_attempt_at) VALUES (?, ?, ?, 'pending', 0, ?)`, c.ID, id, boolInt(required), encodeTime(time.Now())); err != nil {
			return fmt.Errorf("record upload: %w", err)
		}
	}
	return tx.Commit()
}

func (s *Store) HasCompaction(ctx context.Context, sourceID, streamID, date string, revision int) (bool, error) {
	var one int
	err := s.db.QueryRowContext(ctx, `SELECT 1 FROM compactions WHERE source_id=? AND stream_id=? AND archive_date=? AND revision=?`, sourceID, streamID, date, revision).Scan(&one)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	return err == nil, err
}

func (s *Store) AdoptCompaction(ctx context.Context, c *model.Compaction, inputs []model.InputCapture, destinations map[string]bool) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for _, input := range inputs {
		var hash string
		if err := tx.QueryRowContext(ctx, `SELECT body_sha256 FROM captures WHERE id=?`, input.ID).Scan(&hash); err != nil {
			return fmt.Errorf("adopt input %s: %w", input.ID, err)
		}
		if hash != input.SHA256 {
			return fmt.Errorf("adopt input %s hash mismatch", input.ID)
		}
	}
	requiredJSON, _ := json.Marshal(c.RequiredDestinations)
	result, err := tx.ExecContext(ctx, `
INSERT INTO compactions(source_id, stream_id, archive_date, format_version, revision, status,
 directory, data_path, manifest_path, data_sha256, data_bytes, rows, entities,
 required_destinations, created_at, published_at)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, c.SourceID, c.StreamID,
		c.ArchiveDate, model.ParquetFormatVersion, c.Revision, c.Status, c.Directory, c.DataPath,
		c.ManifestPath, c.DataSHA256, c.DataBytes, c.Rows, c.Entities, string(requiredJSON),
		encodeTime(c.CreatedAt), encodeTimePtr(c.PublishedAt))
	if err != nil {
		return err
	}
	c.ID, err = result.LastInsertId()
	if err != nil {
		return err
	}
	for _, input := range inputs {
		if _, err := tx.ExecContext(ctx, `INSERT INTO compaction_inputs(compaction_id, capture_id) VALUES (?, ?)`, c.ID, input.ID); err != nil {
			return err
		}
	}
	for id, required := range destinations {
		if _, err := tx.ExecContext(ctx, `INSERT INTO uploads(compaction_id, destination_id, required, status, attempt_count, next_attempt_at) VALUES (?, ?, ?, 'pending', 0, ?)`, c.ID, id, boolInt(required), encodeTime(time.Now())); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (s *Store) LatestCompaction(ctx context.Context, sourceID, streamID, date string) (*model.Compaction, error) {
	row := s.db.QueryRowContext(ctx, `
SELECT id, source_id, stream_id, archive_date, revision, status, directory, data_path,
 manifest_path, data_sha256, data_bytes, rows, entities, required_destinations, created_at, published_at
FROM compactions WHERE source_id=? AND stream_id=? AND archive_date=? ORDER BY revision DESC LIMIT 1`, sourceID, streamID, date)
	return scanCompaction(row)
}

func (s *Store) CompactionByID(ctx context.Context, id int64) (*model.Compaction, error) {
	row := s.db.QueryRowContext(ctx, `
SELECT id, source_id, stream_id, archive_date, revision, status, directory, data_path,
 manifest_path, data_sha256, data_bytes, rows, entities, required_destinations, created_at, published_at
FROM compactions WHERE id=?`, id)
	return scanCompaction(row)
}

func (s *Store) CompactionForRevision(ctx context.Context, sourceID, streamID, date string, revision int) (*model.Compaction, error) {
	row := s.db.QueryRowContext(ctx, `
SELECT id, source_id, stream_id, archive_date, revision, status, directory, data_path,
 manifest_path, data_sha256, data_bytes, rows, entities, required_destinations, created_at, published_at
FROM compactions WHERE source_id=? AND stream_id=? AND archive_date=? AND revision=?`, sourceID, streamID, date, revision)
	return scanCompaction(row)
}

type scanner interface{ Scan(...any) error }

func scanCompaction(row scanner) (*model.Compaction, error) {
	var c model.Compaction
	var required, created string
	var published sql.NullString
	if err := row.Scan(&c.ID, &c.SourceID, &c.StreamID, &c.ArchiveDate, &c.Revision, &c.Status,
		&c.Directory, &c.DataPath, &c.ManifestPath, &c.DataSHA256, &c.DataBytes, &c.Rows,
		&c.Entities, &required, &created, &published); err != nil {
		return nil, err
	}
	_ = json.Unmarshal([]byte(required), &c.RequiredDestinations)
	c.CreatedAt, _ = time.Parse(timeLayout, created)
	if published.Valid {
		v, _ := time.Parse(timeLayout, published.String)
		c.PublishedAt = &v
	}
	return &c, nil
}

func (s *Store) PendingUploads(ctx context.Context, destination string) ([]model.Upload, error) {
	query := `SELECT id, compaction_id, destination_id, required, status, attempt_count, next_attempt_at, last_error, remote_path, completed_at FROM uploads WHERE status IN ('pending','retry','uploading') AND next_attempt_at<=?`
	args := []any{encodeTime(time.Now())}
	if destination != "" {
		query += " AND destination_id=?"
		args = append(args, destination)
	}
	query += " ORDER BY next_attempt_at, id"
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []model.Upload
	for rows.Next() {
		var u model.Upload
		var required int
		var next string
		var completed sql.NullString
		if err := rows.Scan(&u.ID, &u.CompactionID, &u.DestinationID, &required, &u.Status,
			&u.AttemptCount, &next, &u.LastError, &u.RemotePath, &completed); err != nil {
			return nil, err
		}
		u.Required = required != 0
		u.NextAttemptAt, _ = time.Parse(timeLayout, next)
		if completed.Valid {
			v, _ := time.Parse(timeLayout, completed.String)
			u.CompletedAt = &v
		}
		out = append(out, u)
	}
	return out, rows.Err()
}

func (s *Store) MarkUploadAttempt(ctx context.Context, id int64, status, detail, remote string, next time.Time) error {
	_, err := s.db.ExecContext(ctx, `UPDATE uploads SET status=?, attempt_count=attempt_count+1, last_error=?, remote_path=?, next_attempt_at=? WHERE id=?`, status, detail, remote, encodeTime(next), id)
	return err
}

func (s *Store) MarkUploadFailed(ctx context.Context, id int64, status, detail string, next time.Time) error {
	_, err := s.db.ExecContext(ctx, `UPDATE uploads SET status=?, last_error=?, next_attempt_at=? WHERE id=?`, status, detail, encodeTime(next), id)
	return err
}

func (s *Store) MarkUploadVerified(ctx context.Context, id int64, remote string) error {
	now := encodeTime(time.Now())
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var compactionID int64
	if err := tx.QueryRowContext(ctx, `UPDATE uploads SET status='verified', last_error='', remote_path=?, completed_at=? WHERE id=? RETURNING compaction_id`, remote, now, id).Scan(&compactionID); err != nil {
		return err
	}
	var pending int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM uploads WHERE compaction_id=? AND required=1 AND status NOT IN ('verified','retired')`, compactionID).Scan(&pending); err != nil {
		return err
	}
	if pending == 0 {
		if _, err := tx.ExecContext(ctx, `UPDATE compactions SET status='published', published_at=? WHERE id=?`, now, compactionID); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// EnsureDestinationBackfill queues an explicitly requested destination for
// every locally retained revision that did not already capture it. Historical
// backfills are optional: adding a destination must not retroactively gate
// retention for revisions created under an older configuration.
func (s *Store) EnsureDestinationBackfill(ctx context.Context, destinationID string) (int64, error) {
	result, err := s.db.ExecContext(ctx, `
INSERT INTO uploads(compaction_id, destination_id, required, status, attempt_count, next_attempt_at)
SELECT c.id, ?, 0, 'pending', 0, ?
FROM compactions c
WHERE c.manifest_path!=''
  AND NOT EXISTS (SELECT 1 FROM uploads u WHERE u.compaction_id=c.id AND u.destination_id=?)`,
		destinationID, encodeTime(time.Now()), destinationID)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}

// RetireDestination is the explicit acknowledgement required to release
// historical revisions after a captured required destination is abandoned.
func (s *Store) RetireDestination(ctx context.Context, destinationID, reason string) (int64, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	now := encodeTime(time.Now())
	result, err := tx.ExecContext(ctx, `
UPDATE uploads
SET status='retired', last_error=?, completed_at=?, next_attempt_at=?
WHERE destination_id=? AND required=1 AND status NOT IN ('verified','retired')`,
		"retired: "+reason, now, now, destinationID)
	if err != nil {
		return 0, err
	}
	count, err := result.RowsAffected()
	if err != nil {
		return 0, err
	}
	if _, err := tx.ExecContext(ctx, `
UPDATE compactions
SET status='published', published_at=?
WHERE published_at IS NULL
  AND id IN (SELECT compaction_id FROM uploads WHERE destination_id=? AND status='retired')
  AND NOT EXISTS (
    SELECT 1 FROM uploads u
    WHERE u.compaction_id=compactions.id AND u.required=1 AND u.status NOT IN ('verified','retired')
  )`, now, destinationID); err != nil {
		return 0, err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO repair_audit(action, target, reason, created_at) VALUES ('retire_destination', ?, ?, ?)`, destinationID, reason, now); err != nil {
		return 0, err
	}
	if err := tx.Commit(); err != nil {
		return 0, err
	}
	return count, nil
}

type Status struct {
	Ticks          int64 `json:"ticks"`
	Captures       int64 `json:"captures"`
	Compactions    int64 `json:"compactions"`
	Pending        int64 `json:"pending_uploads"`
	Failed         int64 `json:"failed_uploads"`
	Verified       int64 `json:"verified_uploads"`
	Retired        int64 `json:"retired_uploads"`
	SummarizedDays int64 `json:"summarized_days"`
}

func (s *Store) Status(ctx context.Context) (Status, error) {
	var out Status
	queries := []struct {
		q   string
		dst *int64
	}{
		{`SELECT COUNT(*) FROM ticks`, &out.Ticks}, {`SELECT COUNT(*) FROM captures`, &out.Captures},
		{`SELECT COUNT(*) FROM compactions`, &out.Compactions},
		{`SELECT COUNT(*) FROM uploads WHERE status IN ('pending','uploading')`, &out.Pending},
		{`SELECT COUNT(*) FROM uploads WHERE status IN ('retry','permanent_failure')`, &out.Failed},
		{`SELECT COUNT(*) FROM uploads WHERE status='verified'`, &out.Verified},
		{`SELECT COUNT(*) FROM uploads WHERE status='retired'`, &out.Retired},
		{`SELECT COUNT(*) FROM daily_summaries`, &out.SummarizedDays},
	}
	for _, item := range queries {
		if err := s.db.QueryRowContext(ctx, item.q).Scan(item.dst); err != nil {
			return out, err
		}
	}
	return out, nil
}

func (s *Store) RecordRepair(ctx context.Context, action, target, reason string) error {
	_, err := s.db.ExecContext(ctx, `INSERT INTO repair_audit(action, target, reason, created_at) VALUES (?, ?, ?, ?)`, action, target, reason, encodeTime(time.Now()))
	return err
}

func encodeTime(t time.Time) string { return t.UTC().Format(timeLayout) }
func encodeTimePtr(t *time.Time) any {
	if t == nil {
		return nil
	}
	return encodeTime(*t)
}
func encodeUint64Ptr(v *uint64) any {
	if v == nil {
		return nil
	}
	return strconv.FormatUint(*v, 10)
}
func boolInt(v bool) int {
	if v {
		return 1
	}
	return 0
}
func mustJSON(v any) string { b, _ := json.Marshal(v); return string(b) }

const schemaV1 = `
CREATE TABLE IF NOT EXISTS schema_migrations (
  version INTEGER PRIMARY KEY,
  applied_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS ticks (
  id TEXT PRIMARY KEY,
  source_id TEXT NOT NULL,
  stream_id TEXT NOT NULL,
  scheduled_at TEXT NOT NULL,
  started_at TEXT,
  finished_at TEXT,
  result TEXT NOT NULL,
  skip_reason TEXT NOT NULL DEFAULT '',
  error_category TEXT NOT NULL DEFAULT '',
  error_detail TEXT NOT NULL DEFAULT '',
  http_status INTEGER NOT NULL DEFAULT 0,
  attempts INTEGER NOT NULL DEFAULT 0,
  config_fingerprint TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS ticks_stream_time ON ticks(source_id, stream_id, scheduled_at);
CREATE TABLE IF NOT EXISTS captures (
  id TEXT PRIMARY KEY,
  tick_id TEXT NOT NULL UNIQUE REFERENCES ticks(id),
  source_id TEXT NOT NULL,
  stream_id TEXT NOT NULL,
  expected_kind TEXT NOT NULL,
  scheduled_at TEXT NOT NULL,
  started_at TEXT NOT NULL,
  completed_at TEXT NOT NULL,
  archive_date TEXT NOT NULL,
  timezone TEXT NOT NULL,
  sanitized_url TEXT NOT NULL,
  http_status INTEGER NOT NULL,
  response_headers TEXT NOT NULL,
  duration_ms INTEGER NOT NULL,
  attempt_count INTEGER NOT NULL,
  advertised_length INTEGER NOT NULL,
  encoded_length INTEGER NOT NULL,
  decoded_length INTEGER NOT NULL,
  content_encoding TEXT NOT NULL,
  body_sha256 TEXT NOT NULL,
  raw_path TEXT NOT NULL UNIQUE,
  sidecar_path TEXT NOT NULL UNIQUE,
  transport_complete INTEGER NOT NULL,
  parse_status TEXT NOT NULL,
  parse_error TEXT NOT NULL,
  feed_version TEXT NOT NULL,
  incrementality INTEGER,
	feed_timestamp TEXT,
  entity_count INTEGER,
  validation_flags TEXT NOT NULL,
  config_fingerprint TEXT NOT NULL,
  application_version TEXT NOT NULL,
  protobuf_revision TEXT NOT NULL,
  created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS captures_day ON captures(source_id, stream_id, archive_date, scheduled_at, id);
CREATE TABLE IF NOT EXISTS compactions (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  source_id TEXT NOT NULL,
  stream_id TEXT NOT NULL,
  archive_date TEXT NOT NULL,
  format_version INTEGER NOT NULL,
  revision INTEGER NOT NULL,
  status TEXT NOT NULL,
  directory TEXT NOT NULL,
  data_path TEXT NOT NULL,
  manifest_path TEXT NOT NULL,
  data_sha256 TEXT NOT NULL,
  data_bytes INTEGER NOT NULL,
  rows INTEGER NOT NULL,
  entities INTEGER NOT NULL,
  required_destinations TEXT NOT NULL,
  created_at TEXT NOT NULL,
  published_at TEXT,
  UNIQUE(source_id, stream_id, archive_date, format_version, revision)
);
CREATE TABLE IF NOT EXISTS compaction_inputs (
  compaction_id INTEGER NOT NULL REFERENCES compactions(id),
  capture_id TEXT NOT NULL REFERENCES captures(id),
  PRIMARY KEY(compaction_id, capture_id)
);
CREATE TABLE IF NOT EXISTS uploads (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  compaction_id INTEGER NOT NULL REFERENCES compactions(id),
  destination_id TEXT NOT NULL,
  required INTEGER NOT NULL,
  status TEXT NOT NULL,
  attempt_count INTEGER NOT NULL,
  next_attempt_at TEXT NOT NULL,
  last_error TEXT NOT NULL DEFAULT '',
  remote_path TEXT NOT NULL DEFAULT '',
  completed_at TEXT,
  UNIQUE(compaction_id, destination_id)
);
CREATE INDEX IF NOT EXISTS uploads_pending ON uploads(status, next_attempt_at);
CREATE TABLE IF NOT EXISTS repair_audit (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  action TEXT NOT NULL,
  target TEXT NOT NULL,
  reason TEXT NOT NULL,
  created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS daily_summaries (
  source_id TEXT NOT NULL,
  stream_id TEXT NOT NULL,
  archive_date TEXT NOT NULL,
  scheduled_ticks INTEGER NOT NULL,
  skipped_ticks INTEGER NOT NULL,
  captured_responses INTEGER NOT NULL,
  valid_snapshots INTEGER NOT NULL,
  invalid_payloads INTEGER NOT NULL,
  http_failures INTEGER NOT NULL,
  network_failures INTEGER NOT NULL,
  parse_status_counts TEXT NOT NULL,
  summarized_at TEXT NOT NULL,
  PRIMARY KEY(source_id, stream_id, archive_date)
);
`

func IsNotFound(err error) bool { return errors.Is(err, sql.ErrNoRows) }

func IsPermanentSQLiteError(err error) bool {
	if err == nil {
		return false
	}
	v := strings.ToLower(err.Error())
	return strings.Contains(v, "malformed") || strings.Contains(v, "corrupt")
}
