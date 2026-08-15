package upload

import (
	"encoding/base64"
	"errors"
	"fmt"
	"path"
	"regexp"
	"strconv"
	"strings"

	"gtfs-rt-archiver/internal/config"
	"gtfs-rt-archiver/internal/model"
)

// feedTypeDir maps stream expected_kind to the publication feed_type
// partition directory (gtfsrt.io plural names; mixed/auto share "mixed").
var feedTypeDir = map[string]string{
	"vehicle_position": "vehicle_positions",
	"trip_update":      "trip_updates",
	"alert":            "service_alerts",
	"mixed":            "mixed",
	"auto":             "mixed",
}

var (
	hiveDatePattern = regexp.MustCompile(`^\d{4}-\d{2}-\d{2}$`)
	// Every emitted segment stays compute-safe on all rclone backends:
	// feed_type, date=YYYY-MM-DD, base64url=…, revision=N.
	hiveSegmentPattern = regexp.MustCompile(`^[A-Za-z0-9_.=-]+$`)
)

// errDatasetInvalid marks manifests that can never map to a hive path
// (operator fixes the projection/data, not retries).
var errDatasetInvalid = errors.New("dataset_invalid")

// errStreamNotConfigured marks upload rows whose stream left the config
// before a legacy (no sanitized_url) manifest could be published.
var errStreamNotConfigured = errors.New("stream_not_configured")

// hiveDir computes the hive-partitioned remote directory for one dataset
// revision. It is a pure function of the destination base and the manifest
// (plus config only for the legacy fallback), so retries, restarts, and
// backfills all recompute the identical destination.
func hiveDir(destBase string, manifest *model.Manifest, cfg *config.Config) (string, error) {
	feedType, ok := feedTypeDir[manifest.ExpectedKind]
	if !ok {
		return "", fmt.Errorf("%w: expected_kind %q has no feed type mapping", errDatasetInvalid, manifest.ExpectedKind)
	}
	if !hiveDatePattern.MatchString(manifest.ArchiveDate) {
		return "", fmt.Errorf("%w: archive_date %q is not YYYY-MM-DD", errDatasetInvalid, manifest.ArchiveDate)
	}
	if manifest.Revision < 1 {
		return "", fmt.Errorf("%w: revision %d", errDatasetInvalid, manifest.Revision)
	}
	sanitized := manifest.SanitizedURL
	if sanitized == "" {
		var err error
		sanitized, err = streamSanitizedURL(cfg, manifest.SourceID, manifest.StreamID)
		if err != nil {
			return "", err
		}
	}
	key := base64.RawURLEncoding.EncodeToString([]byte(sanitized))
	dir := path.Join(feedType, "date="+manifest.ArchiveDate, "base64url="+key,
		"revision="+strconv.Itoa(manifest.Revision))
	for _, seg := range strings.Split(dir, "/") {
		if !hiveSegmentPattern.MatchString(seg) {
			return "", fmt.Errorf("%w: unsafe path segment %q", errDatasetInvalid, seg)
		}
	}
	return remoteJoin(destBase, dir), nil
}

// streamSanitizedURL backs the legacy-manifest fallback: revisions compacted
// before manifests carried sanitized_url derive their partition from the
// stream's current configured URL.
func streamSanitizedURL(cfg *config.Config, sourceID, streamID string) (string, error) {
	_, stream, err := cfg.FindSourceStream(sourceID, streamID)
	if err != nil {
		return "", fmt.Errorf("%w: %s/%s", errStreamNotConfigured, sourceID, streamID)
	}
	return config.SanitizedStreamURL(stream.URL)
}
