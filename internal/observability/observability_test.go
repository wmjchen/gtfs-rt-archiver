package observability

import (
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"gtfs-rt-archiver/internal/config"
	"gtfs-rt-archiver/internal/rawstore"
	"gtfs-rt-archiver/internal/state"
)

func TestReadinessReportsDiskPressure(t *testing.T) {
	root := t.TempDir()
	cfg := config.Defaults()
	cfg.Storage.Root = root
	cfg.Storage.StateDB = filepath.Join(root, "state.sqlite")
	cfg.Storage.PauseFetchingAtPercent = 0
	cfg.Storage.ResumeFetchingAtPercent = -1
	raw, err := rawstore.Open(root)
	if err != nil {
		t.Fatal(err)
	}
	db, err := state.Open(context.Background(), cfg.Storage.StateDB)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	monitor := NewMonitor(&cfg, raw, db, NewMetrics(), slog.New(slog.NewTextHandler(os.Stderr, nil)))
	monitor.Check(context.Background())
	request := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	recorder := httptest.NewRecorder()
	monitor.Handler().ServeHTTP(recorder, request)
	if recorder.Code != http.StatusServiceUnavailable || !strings.Contains(recorder.Body.String(), "disk_pressure") {
		t.Fatalf("status=%d body=%s", recorder.Code, recorder.Body.String())
	}
}
