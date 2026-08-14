package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func writeConfig(t *testing.T, body string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestDurationAcceptsWholeDays(t *testing.T) {
	p := writeConfig(t, `
version: 1
storage:
  root: /tmp/archive
  raw_retention_after_upload: 7d
  metadata_retention: 30d
sources:
  - id: demo
    timezone: UTC
    streams:
      - id: feed
        expected_kind: mixed
        url: https://example.test/feed
        interval: 30s
`)
	cfg, err := Load(p)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Storage.RawRetentionAfterUpload.Duration != 7*24*time.Hour || cfg.Storage.MetadataRetention.Duration != 30*24*time.Hour {
		t.Fatalf("day durations were not parsed: %+v", cfg.Storage)
	}
}

func TestRejectsNegativeStreamOverrides(t *testing.T) {
	p := writeConfig(t, `
version: 1
storage: {root: /tmp/archive}
sources:
  - id: demo
    timezone: UTC
    streams:
      - id: feed
        expected_kind: mixed
        url: https://example.test/feed
        interval: 30s
        request_timeout: -1s
`)
	if _, err := Load(p); err == nil || !strings.Contains(err.Error(), "overrides cannot be negative") {
		t.Fatalf("expected invalid override error, got %v", err)
	}
}

func TestFingerprintIncludesEffectiveOverridesWithoutSecretValues(t *testing.T) {
	cfg := Defaults()
	cfg.Sources = []Source{{ID: "demo", Timezone: "UTC", Streams: []Stream{{
		ID: "feed", URL: "https://example.test/feed?region=north", ExpectedKind: "mixed", Interval: Duration{30 * time.Second},
		Auth: Auth{Query: map[string]SecretRef{"apikey": {Env: "FEED_KEY"}}},
	}}}}
	one := cfg.Fingerprint()
	cfg.Sources[0].Streams[0].RequestTimeout = Duration{3 * time.Second}
	two := cfg.Fingerprint()
	if one == two || strings.Contains(one, "FEED_KEY") || strings.Contains(two, "FEED_KEY") {
		t.Fatalf("unexpected fingerprints: %q %q", one, two)
	}
}

func TestLoadDefaultsAndStrictFields(t *testing.T) {
	p := writeConfig(t, `
version: 1
storage: {root: /tmp/archive}
sources:
  - id: demo
    timezone: America/Vancouver
    streams:
      - id: vehicles
        expected_kind: vehicle_position
        url: https://example.test/feed
        interval: 30s
`)
	cfg, err := Load(p)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Storage.StateDB != "/tmp/archive/state.sqlite" {
		t.Fatalf("state db = %q", cfg.Storage.StateDB)
	}
	if cfg.HTTP.MaxResponseBytes != 32<<20 {
		t.Fatalf("max bytes = %d", cfg.HTTP.MaxResponseBytes)
	}
	if cfg.Sources[0].Location.String() != "America/Vancouver" {
		t.Fatal("timezone was not loaded")
	}

	p = writeConfig(t, strings.ReplaceAll(string(mustRead(t, p)), "interval: 30s", "interval: 30s\n        mystery: true"))
	if _, err := Load(p); err == nil || !strings.Contains(err.Error(), "mystery") {
		t.Fatalf("expected strict-field error, got %v", err)
	}
}

func TestDestinationDefaults(t *testing.T) {
	d := Destination{}
	if !d.IsRequired() {
		t.Fatal("destination should default required")
	}
}

func TestRejectsInlineSecretsAndDuplicateYAMLKeys(t *testing.T) {
	inline := writeConfig(t, `
version: 1
storage: {root: /tmp/archive}
sources:
  - id: demo
    timezone: UTC
    streams:
      - id: feed
        expected_kind: mixed
        url: https://example.test/feed?apikey=secret
        interval: 30s
`)
	if _, err := Load(inline); err == nil || !strings.Contains(err.Error(), "likely secret") {
		t.Fatalf("expected inline-secret error, got %v", err)
	}
	duplicate := writeConfig(t, `
version: 1
version: 1
storage: {root: /tmp/archive}
sources: []
`)
	if _, err := Load(duplicate); err == nil {
		t.Fatal("duplicate YAML key was accepted")
	}
}

func mustRead(t *testing.T, path string) []byte {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return b
}
