package app

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"gtfs-rt-archiver/internal/rawstore"
)

func TestValidateConfigAndLiveStatus(t *testing.T) {
	root := t.TempDir()
	configPath := filepath.Join(t.TempDir(), "config.yaml")
	body := fmt.Sprintf(`
version: 1
storage:
  root: %s
sources:
  - id: demo
    timezone: UTC
    streams:
      - id: mixed
        expected_kind: mixed
        url: https://example.test/feed
        interval: 30s
`, root)
	if err := os.WriteFile(configPath, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	var stdout, stderr bytes.Buffer
	application := New(&stdout, &stderr)
	if err := application.Run(context.Background(), []string{"validate-config", "--config", configPath}); err != nil {
		t.Fatalf("validate-config: %v (%s)", err, stderr.String())
	}
	if !strings.Contains(stdout.String(), "configuration valid") {
		t.Fatalf("unexpected validation output: %q", stdout.String())
	}

	lock, err := rawstore.AcquireLock(root)
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Close()
	stdout.Reset()
	stderr.Reset()
	if err := application.Run(context.Background(), []string{"status", "--config", configPath, "--json"}); err != nil {
		t.Fatalf("status while writer lock is held: %v (%s)", err, stderr.String())
	}
	if !strings.Contains(stdout.String(), `"captures":0`) {
		t.Fatalf("unexpected status output: %q", stdout.String())
	}
}
