package observability

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"gtfs-rt-archiver/internal/config"
	"gtfs-rt-archiver/internal/rawstore"
	"gtfs-rt-archiver/internal/state"
)

type Metrics struct {
	Registry           *prometheus.Registry
	fetchAttempts      *prometheus.CounterVec
	fetchDuration      *prometheus.HistogramVec
	fetchBytes         *prometheus.HistogramVec
	lastCapture        *prometheus.GaugeVec
	lastValidCapture   *prometheus.GaugeVec
	lastFeedTimestamp  *prometheus.GaugeVec
	validation         *prometheus.CounterVec
	skipped            *prometheus.CounterVec
	compactions        *prometheus.CounterVec
	compactionDuration *prometheus.HistogramVec
	uploads            *prometheus.CounterVec
	uploadDuration     *prometheus.HistogramVec
	pendingUploads     prometheus.Gauge
	storageUsed        prometheus.Gauge
	storageFree        prometheus.Gauge
	storagePercent     prometheus.Gauge
	paused             prometheus.Gauge
}

func NewMetrics() *Metrics {
	m := &Metrics{
		Registry:           prometheus.NewRegistry(),
		fetchAttempts:      prometheus.NewCounterVec(prometheus.CounterOpts{Name: "gtfsrt_fetch_attempts_total", Help: "Scheduled fetch outcomes."}, []string{"source", "stream", "result"}),
		fetchDuration:      prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "gtfsrt_fetch_duration_seconds", Help: "Fetch duration.", Buckets: prometheus.DefBuckets}, []string{"source", "stream"}),
		fetchBytes:         prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "gtfsrt_fetch_body_bytes", Help: "Decoded response sizes.", Buckets: prometheus.ExponentialBuckets(256, 4, 10)}, []string{"source", "stream"}),
		lastCapture:        prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "gtfsrt_last_capture_unixtime", Help: "Most recent durable capture time."}, []string{"source", "stream"}),
		lastValidCapture:   prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "gtfsrt_last_valid_snapshot_unixtime", Help: "Most recent complete, valid GTFS-Realtime snapshot time."}, []string{"source", "stream"}),
		lastFeedTimestamp:  prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "gtfsrt_last_feed_timestamp_unixtime", Help: "Producer timestamp on the most recent complete, valid snapshot that provided one."}, []string{"source", "stream"}),
		validation:         prometheus.NewCounterVec(prometheus.CounterOpts{Name: "gtfsrt_validation_flags_total", Help: "Capture validation warnings."}, []string{"source", "stream", "flag"}),
		skipped:            prometheus.NewCounterVec(prometheus.CounterOpts{Name: "gtfsrt_skipped_ticks_total", Help: "Fixed-rate ticks skipped before starting."}, []string{"source", "stream", "reason"}),
		compactions:        prometheus.NewCounterVec(prometheus.CounterOpts{Name: "gtfsrt_compactions_total", Help: "Daily compaction outcomes."}, []string{"source", "stream", "result"}),
		compactionDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "gtfsrt_compaction_duration_seconds", Help: "Daily compaction duration.", Buckets: prometheus.ExponentialBuckets(0.1, 4, 9)}, []string{"source", "stream"}),
		uploads:            prometheus.NewCounterVec(prometheus.CounterOpts{Name: "gtfsrt_uploads_total", Help: "Revision publication outcomes."}, []string{"destination", "result"}),
		uploadDuration:     prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "gtfsrt_upload_duration_seconds", Help: "Revision publication duration.", Buckets: prometheus.ExponentialBuckets(0.25, 4, 10)}, []string{"destination"}),
		pendingUploads:     prometheus.NewGauge(prometheus.GaugeOpts{Name: "gtfsrt_pending_uploads", Help: "Pending or failed revision publications."}),
		storageUsed:        prometheus.NewGauge(prometheus.GaugeOpts{Name: "gtfsrt_storage_used_bytes", Help: "Used bytes on the archive filesystem."}),
		storageFree:        prometheus.NewGauge(prometheus.GaugeOpts{Name: "gtfsrt_storage_free_bytes", Help: "Available bytes on the archive filesystem."}),
		storagePercent:     prometheus.NewGauge(prometheus.GaugeOpts{Name: "gtfsrt_storage_used_percent", Help: "Used percentage of the archive filesystem."}),
		paused:             prometheus.NewGauge(prometheus.GaugeOpts{Name: "gtfsrt_fetching_paused", Help: "Whether disk pressure has paused fetching."}),
	}
	m.Registry.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
		m.fetchAttempts, m.fetchDuration, m.fetchBytes, m.lastCapture, m.lastValidCapture, m.lastFeedTimestamp, m.validation, m.skipped,
		m.compactions, m.compactionDuration, m.uploads, m.uploadDuration, m.pendingUploads,
		m.storageUsed, m.storageFree, m.storagePercent, m.paused)
	return m
}

func (m *Metrics) ObserveFetch(source, stream, result string, duration time.Duration, bytes int64) {
	m.fetchAttempts.WithLabelValues(source, stream, result).Inc()
	m.fetchDuration.WithLabelValues(source, stream).Observe(duration.Seconds())
	if bytes >= 0 {
		m.fetchBytes.WithLabelValues(source, stream).Observe(float64(bytes))
	}
	if result == "captured" || result == "captured_invalid" {
		m.lastCapture.WithLabelValues(source, stream).SetToCurrentTime()
	}
}
func (m *Metrics) ObserveValidation(source, stream, flag string) {
	m.validation.WithLabelValues(source, stream, flag).Inc()
}
func (m *Metrics) ObserveFeedMetadata(source, stream string, observed time.Time, feedTimestamp *uint64, valid bool) {
	if !valid {
		return
	}
	m.lastValidCapture.WithLabelValues(source, stream).Set(float64(observed.Unix()))
	if feedTimestamp != nil {
		m.lastFeedTimestamp.WithLabelValues(source, stream).Set(float64(*feedTimestamp))
	}
}
func (m *Metrics) ObserveSkipped(source, stream, reason string) {
	m.skipped.WithLabelValues(source, stream, reason).Inc()
	m.fetchAttempts.WithLabelValues(source, stream, "skipped").Inc()
}
func (m *Metrics) ObserveCompaction(source, stream, result string, duration time.Duration) {
	m.compactions.WithLabelValues(source, stream, result).Inc()
	m.compactionDuration.WithLabelValues(source, stream).Observe(duration.Seconds())
}
func (m *Metrics) ObserveUpload(destination, result string, duration time.Duration) {
	m.uploads.WithLabelValues(destination, result).Inc()
	m.uploadDuration.WithLabelValues(destination).Observe(duration.Seconds())
}

type Monitor struct {
	cfg     *config.Config
	raw     *rawstore.Store
	state   *state.Store
	metrics *Metrics
	log     *slog.Logger
	paused  atomic.Bool
	healthy atomic.Bool
	lastErr atomic.Value
}

func NewMonitor(cfg *config.Config, raw *rawstore.Store, store *state.Store, metrics *Metrics, log *slog.Logger) *Monitor {
	m := &Monitor{cfg: cfg, raw: raw, state: store, metrics: metrics, log: log}
	m.healthy.Store(true)
	m.lastErr.Store("")
	return m
}

func (m *Monitor) Paused() bool { return m.paused.Load() }
func (m *Monitor) Ready() bool  { return m.healthy.Load() && !m.paused.Load() }

func (m *Monitor) Check(ctx context.Context) {
	usage, err := m.raw.DiskUsage()
	if err != nil {
		m.healthy.Store(false)
		m.lastErr.Store("storage_unavailable")
		return
	}
	m.metrics.storageUsed.Set(float64(usage.Used))
	m.metrics.storageFree.Set(float64(usage.Free))
	m.metrics.storagePercent.Set(usage.UsedPercent)
	wasPaused := m.paused.Load()
	if !wasPaused && usage.UsedPercent >= m.cfg.Storage.PauseFetchingAtPercent {
		m.paused.Store(true)
		m.log.Error("fetching paused for disk pressure", "used_percent", usage.UsedPercent)
	}
	if wasPaused && usage.UsedPercent <= m.cfg.Storage.ResumeFetchingAtPercent {
		m.paused.Store(false)
		m.log.Info("fetching resumed after disk pressure", "used_percent", usage.UsedPercent)
	}
	if m.paused.Load() {
		m.metrics.paused.Set(1)
	} else {
		m.metrics.paused.Set(0)
	}
	status, err := m.state.Status(ctx)
	if err != nil {
		m.healthy.Store(false)
		m.lastErr.Store("state_unavailable")
		return
	}
	m.metrics.pendingUploads.Set(float64(status.Pending + status.Failed))
	m.healthy.Store(true)
	m.lastErr.Store("")
}

func (m *Monitor) Run(ctx context.Context) {
	m.Check(ctx)
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			m.Check(ctx)
		}
	}
}

func (m *Monitor) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { writeHealth(w, http.StatusOK, true, "") })
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, _ *http.Request) {
		if !m.Ready() {
			reason := m.lastErr.Load().(string)
			if reason == "" && m.paused.Load() {
				reason = "disk_pressure"
			}
			writeHealth(w, http.StatusServiceUnavailable, false, reason)
			return
		}
		writeHealth(w, http.StatusOK, true, "")
	})
	mux.Handle("GET /metrics", promhttp.HandlerFor(m.metrics.Registry, promhttp.HandlerOpts{}))
	return mux
}

func writeHealth(w http.ResponseWriter, status int, ok bool, reason string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]any{"ok": ok, "reason": reason})
}
