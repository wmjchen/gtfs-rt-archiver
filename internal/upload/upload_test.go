package upload

import (
	"encoding/base64"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"gtfs-rt-archiver/internal/config"
	"gtfs-rt-archiver/internal/model"
)

func TestRemoteJoinAndErrorCategories(t *testing.T) {
	got := remoteJoin("remote:bucket/root/", "parquet/format=v1", "manifest.json")
	if got != "remote:bucket/root/parquet/format=v1/manifest.json" {
		t.Fatalf("remote path = %q", got)
	}
	if got := classifyError(errors.New("exit status 1"), "Access Denied"); got != "permission" {
		t.Fatalf("category = %q", got)
	}
	if !isPermanent("remote_integrity") || isPermanent("network") || isPermanent("authentication") {
		t.Fatal("permanence classification is wrong")
	}
}

func TestBackoffIsBounded(t *testing.T) {
	for attempt := 1; attempt < 20; attempt++ {
		if got := backoff(attempt); got < 0 || got > 6*time.Hour {
			t.Fatalf("attempt %d backoff %s", attempt, got)
		}
	}
}

func TestHiveDirGoldenVectors(t *testing.T) {
	cfg := &config.Config{}
	mk := func(kind, sanitized string, revision int) *model.Manifest {
		return &model.Manifest{ExpectedKind: kind, SanitizedURL: sanitized,
			ArchiveDate: "2026-08-14", Revision: revision, SourceID: "s", StreamID: "st"}
	}
	cases := []struct {
		name     string
		manifest *model.Manifest
		want     string
	}{
		// gtfsrt.io parity: these base64url values are hand-computed per the
		// spec's golden-vector rule and must remain stable.
		{"trip updates", mk("trip_update", "https://gtfsapi.translink.ca/v3/gtfsrealtime", 1),
			"remote:bucket/prefix/trip_updates/date=2026-08-14/base64url=aHR0cHM6Ly9ndGZzYXBpLnRyYW5zbGluay5jYS92My9ndGZzcmVhbHRpbWU/revision=1"},
		{"alerts revision 3", mk("alert", "https://gtfsapi.translink.ca/v3/gtfsalerts", 3),
			"remote:bucket/prefix/service_alerts/date=2026-08-14/base64url=aHR0cHM6Ly9ndGZzYXBpLnRyYW5zbGluay5jYS92My9ndGZzYWxlcnRz/revision=3"},
		{"vehicle positions", mk("vehicle_position", "https://example.test/feed", 1),
			"remote:bucket/prefix/vehicle_positions/date=2026-08-14/base64url=aHR0cHM6Ly9leGFtcGxlLnRlc3QvZmVlZA/revision=1"},
		{"mixed", mk("mixed", "https://example.test/feed", 2),
			"remote:bucket/prefix/mixed/date=2026-08-14/base64url=aHR0cHM6Ly9leGFtcGxlLnRlc3QvZmVlZA/revision=2"},
		{"auto maps to mixed", mk("auto", "https://example.test/feed", 1),
			"remote:bucket/prefix/mixed/date=2026-08-14/base64url=aHR0cHM6Ly9leGFtcGxlLnRlc3QvZmVlZA/revision=1"},
	}
	for _, tc := range cases {
		got, err := hiveDir("remote:bucket/prefix", tc.manifest, cfg)
		if err != nil {
			t.Fatalf("%s: %v", tc.name, err)
		}
		if got != tc.want {
			t.Errorf("%s: got %q, want %q", tc.name, got, tc.want)
		}
		// Every directory segment must be a safe, encoding-clean component.
		for _, seg := range strings.Split(strings.TrimPrefix(got, "remote:bucket/prefix/"), "/") {
			if !hiveSegmentPattern.MatchString(seg) {
				t.Errorf("%s: unsafe segment %q", tc.name, seg)
			}
		}
	}
}

func TestHiveDirFallbackAndFailures(t *testing.T) {
	cfgWith := &config.Config{Sources: []config.Source{{ID: "demo", Streams: []config.Stream{
		{ID: "feed", URL: "https://example.test/feed?apikey=removed"},
	}}}}
	legacy := &model.Manifest{ExpectedKind: "alert", ArchiveDate: "2026-08-14",
		Revision: 1, SourceID: "demo", StreamID: "feed"} // no SanitizedURL (pre-feature manifest)

	// Fallback: derive the partition from the configured stream URL.
	got, err := hiveDir("remote:bucket/prefix", legacy, cfgWith)
	if err != nil {
		t.Fatal(err)
	}
	wantKey := base64.RawURLEncoding.EncodeToString([]byte("https://example.test/feed"))
	want := "remote:bucket/prefix/service_alerts/date=2026-08-14/base64url=" + wantKey + "/revision=1"
	if got != want {
		t.Fatalf("fallback dir = %q, want %q", got, want)
	}

	// The stream was removed from config: retriable stream_not_configured.
	if _, err := hiveDir("remote:bucket/prefix", legacy, &config.Config{}); !errors.Is(err, errStreamNotConfigured) {
		t.Fatalf("want stream_not_configured, got %v", err)
	}

	// Unmappable manifests: permanent dataset_invalid.
	badKind := *legacy
	badKind.SanitizedURL = "https://example.test/feed"
	badKind.ExpectedKind = "nonsense"
	if _, err := hiveDir("remote:bucket/prefix", &badKind, cfgWith); !errors.Is(err, errDatasetInvalid) {
		t.Fatalf("want dataset_invalid, got %v", err)
	}
	badDate := badKind
	badDate.ExpectedKind = "alert"
	badDate.ArchiveDate = "14.08.2026"
	if _, err := hiveDir("remote:bucket/prefix", &badDate, cfgWith); !errors.Is(err, errDatasetInvalid) {
		t.Fatalf("want dataset_invalid, got %v", err)
	}
}

func TestUploadErrorCategoriesForHiveMapping(t *testing.T) {
	if got := errorCategory(fmt.Errorf("publish: %w", errStreamNotConfigured)); got != "stream_not_configured" {
		t.Fatalf("category = %q", got)
	}
	if got := errorCategory(fmt.Errorf("publish: %w", errDatasetInvalid)); got != "dataset_invalid" {
		t.Fatalf("category = %q", got)
	}
	if !isPermanent("dataset_invalid") {
		t.Fatal("dataset_invalid must be permanent")
	}
	if isPermanent("stream_not_configured") {
		t.Fatal("stream_not_configured must be retried")
	}
}
