package app

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"gtfs-rt-archiver/internal/compact"
	"gtfs-rt-archiver/internal/config"
	"gtfs-rt-archiver/internal/fetch"
	"gtfs-rt-archiver/internal/lifecycle"
	"gtfs-rt-archiver/internal/observability"
	"gtfs-rt-archiver/internal/rawstore"
	"gtfs-rt-archiver/internal/reconcile"
	"gtfs-rt-archiver/internal/scheduler"
	"gtfs-rt-archiver/internal/service"
	"gtfs-rt-archiver/internal/state"
	"gtfs-rt-archiver/internal/upload"
	"gtfs-rt-archiver/internal/version"
)

type App struct {
	stdout, stderr io.Writer
	log            *slog.Logger
}

func New(stdout, stderr io.Writer) *App {
	return &App{stdout: stdout, stderr: stderr, log: slog.New(slog.NewJSONHandler(stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))}
}

func (a *App) Run(ctx context.Context, args []string) error {
	if len(args) == 0 {
		a.usage()
		return errors.New("a command is required")
	}
	switch args[0] {
	case "run":
		return a.runService(ctx, args[1:])
	case "validate-config":
		return a.validateConfig(ctx, args[1:])
	case "fetch-once":
		return a.fetchOnce(ctx, args[1:])
	case "compact":
		return a.compact(ctx, args[1:])
	case "upload":
		return a.upload(ctx, args[1:])
	case "verify":
		return a.verify(ctx, args[1:])
	case "status":
		return a.status(ctx, args[1:])
	case "repair":
		return a.repair(ctx, args[1:])
	case "retire-destination":
		return a.retireDestination(ctx, args[1:])
	case "version":
		return json.NewEncoder(a.stdout).Encode(version.Current())
	case "help", "-h", "--help":
		a.usage()
		return nil
	default:
		a.usage()
		return fmt.Errorf("unknown command %q", args[0])
	}
}

func (a *App) validateConfig(ctx context.Context, args []string) error {
	fs := newFlagSet("validate-config", a.stderr)
	path := fs.String("config", "", "configuration file")
	if err := fs.Parse(args); err != nil {
		return err
	}
	cfg, err := loadConfig(*path)
	if err != nil {
		return err
	}
	if err := cfg.ResolveSecrets(); err != nil {
		return err
	}
	if err := upload.New(cfg, nil, a.log, nil).Preflight(ctx); err != nil {
		return err
	}
	_, _ = fmt.Fprintf(a.stdout, "configuration valid: %d sources, %d streams, %d destinations, fingerprint %s\n", len(cfg.Sources), countStreams(cfg), len(cfg.Destinations), cfg.Fingerprint())
	return nil
}

func (a *App) runService(ctx context.Context, args []string) error {
	fs := newFlagSet("run", a.stderr)
	path := fs.String("config", "", "configuration file")
	if err := fs.Parse(args); err != nil {
		return err
	}
	cfg, err := loadConfig(*path)
	if err != nil {
		return err
	}
	if err := cfg.ResolveSecrets(); err != nil {
		return err
	}
	runtime, err := a.openRuntime(ctx, cfg, true)
	if err != nil {
		return err
	}
	defer runtime.Close()
	if err := runtime.uploader.Preflight(ctx); err != nil {
		return err
	}
	reconciled, err := reconcile.All(ctx, cfg, runtime.raw, runtime.state)
	if err != nil {
		return fmt.Errorf("startup reconciliation: %w", err)
	}
	removed, err := runtime.raw.RemoveExpiredStaging(time.Now().Add(-24 * time.Hour))
	if err != nil {
		return fmt.Errorf("clean staging: %w", err)
	}
	a.log.Info("startup reconciliation complete", "captures_registered", reconciled.CapturesRegistered,
		"compactions_adopted", reconciled.CompactionsAdopted, "corrupt", reconciled.Corrupt, "staging_removed", removed)

	metrics := observability.NewMetrics()
	monitor := observability.NewMonitor(cfg, runtime.raw, runtime.state, metrics, a.log)
	fetcher := fetch.New(cfg, runtime.raw, runtime.state, a.log, metrics)
	sched := scheduler.New(cfg, fetcher, runtime.state, a.log, monitor, metrics)
	compactor := compact.New(cfg, runtime.raw, runtime.state)
	retention := lifecycle.NewRetention(cfg, runtime.raw, runtime.state)
	maintenance := service.NewMaintenance(cfg, runtime.state, compactor, runtime.uploaderWithObserver(metrics), retention, metrics, a.log)
	runCtx, stop := context.WithCancel(ctx)
	defer stop()

	server := &http.Server{Addr: cfg.Runtime.HTTPAddress, Handler: monitor.Handler(), ReadHeaderTimeout: 5 * time.Second, IdleTimeout: 30 * time.Second}
	serverErr := make(chan error, 1)
	go func() {
		a.log.Info("health server listening", "address", cfg.Runtime.HTTPAddress)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serverErr <- err
		}
	}()
	var workers sync.WaitGroup
	workers.Add(3)
	go func() { defer workers.Done(); monitor.Run(runCtx) }()
	go func() { defer workers.Done(); maintenance.Run(runCtx) }()
	go func() { defer workers.Done(); _ = sched.Run(runCtx) }()

	var runErr error
	select {
	case <-ctx.Done():
	case err := <-serverErr:
		runErr = fmt.Errorf("health server: %w", err)
	}
	stop()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.Runtime.ShutdownTimeout.Duration)
	defer cancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		a.log.Warn("health server shutdown", "error", err)
	}
	workersDone := make(chan struct{})
	go func() { workers.Wait(); close(workersDone) }()
	select {
	case <-workersDone:
	case <-shutdownCtx.Done():
		return errors.New("workers did not stop within shutdown timeout")
	}
	if err := runtime.state.Checkpoint(shutdownCtx); err != nil {
		return fmt.Errorf("checkpoint state: %w", err)
	}
	return runErr
}

func (a *App) fetchOnce(ctx context.Context, args []string) error {
	fs := newFlagSet("fetch-once", a.stderr)
	path := fs.String("config", "", "configuration file")
	sourceID := fs.String("source", "", "source ID")
	streamID := fs.String("stream", "", "stream ID")
	if err := fs.Parse(args); err != nil {
		return err
	}
	cfg, err := loadConfig(*path)
	if err != nil {
		return err
	}
	if err := cfg.ResolveSecrets(); err != nil {
		return err
	}
	runtime, err := a.openRuntime(ctx, cfg, true)
	if err != nil {
		return err
	}
	defer runtime.Close()
	fetcher := fetch.New(cfg, runtime.raw, runtime.state, a.log, nil)
	bindings, err := selectStreams(cfg, *sourceID, *streamID)
	if err != nil {
		return err
	}
	for _, binding := range bindings {
		result, err := fetcher.Fetch(ctx, binding.source, binding.stream, time.Now())
		if err != nil {
			return err
		}
		_, _ = fmt.Fprintf(a.stdout, "%s/%s capture=%s status=%s bytes=%d\n", binding.source.ID, binding.stream.ID, result.Capture.ID, result.Capture.ParseStatus, result.Capture.DecodedLength)
	}
	return nil
}

func (a *App) compact(ctx context.Context, args []string) error {
	fs := newFlagSet("compact", a.stderr)
	path := fs.String("config", "", "configuration file")
	date := fs.String("date", "", "archive date (YYYY-MM-DD)")
	sourceID := fs.String("source", "", "source ID")
	streamID := fs.String("stream", "", "stream ID")
	revision := fs.Int("revision", 0, "explicit immutable revision")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if _, err := time.Parse(time.DateOnly, *date); err != nil {
		return errors.New("--date must be YYYY-MM-DD")
	}
	cfg, err := loadConfig(*path)
	if err != nil {
		return err
	}
	runtime, err := a.openRuntime(ctx, cfg, true)
	if err != nil {
		return err
	}
	defer runtime.Close()
	bindings, err := selectStreams(cfg, *sourceID, *streamID)
	if err != nil {
		return err
	}
	compactor := compact.New(cfg, runtime.raw, runtime.state)
	for _, binding := range bindings {
		result, err := compactor.Compact(ctx, compact.Request{SourceID: binding.source.ID, StreamID: binding.stream.ID, Date: *date, Revision: *revision})
		if err != nil {
			return err
		}
		_, _ = fmt.Fprintf(a.stdout, "%s/%s date=%s revision=%d rows=%d manifest=%s\n", result.SourceID, result.StreamID, result.ArchiveDate, result.Revision, result.Rows, result.ManifestPath)
	}
	return nil
}

func (a *App) upload(ctx context.Context, args []string) error {
	fs := newFlagSet("upload", a.stderr)
	path := fs.String("config", "", "configuration file")
	destination := fs.String("destination", "", "destination ID")
	if err := fs.Parse(args); err != nil {
		return err
	}
	cfg, err := loadConfig(*path)
	if err != nil {
		return err
	}
	runtime, err := a.openRuntime(ctx, cfg, true)
	if err != nil {
		return err
	}
	defer runtime.Close()
	queued := int64(0)
	if *destination != "" {
		if _, ok := configuredDestination(cfg, *destination); !ok {
			return fmt.Errorf("destination %q is not configured", *destination)
		}
		queued, err = runtime.state.EnsureDestinationBackfill(ctx, *destination)
		if err != nil {
			return fmt.Errorf("queue destination backfill: %w", err)
		}
	}
	if err := runtime.uploader.Preflight(ctx); err != nil {
		return err
	}
	count, err := runtime.uploader.ProcessPending(ctx, *destination)
	if err != nil {
		return err
	}
	_, _ = fmt.Fprintf(a.stdout, "queued %d historical upload(s), processed %d upload(s)\n", queued, count)
	return nil
}

func (a *App) verify(ctx context.Context, args []string) error {
	fs := newFlagSet("verify", a.stderr)
	path := fs.String("config", "", "configuration file")
	date := fs.String("date", "", "archive date (YYYY-MM-DD)")
	sourceID := fs.String("source", "", "source ID")
	streamID := fs.String("stream", "", "stream ID")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if _, err := time.Parse(time.DateOnly, *date); err != nil {
		return errors.New("--date must be YYYY-MM-DD")
	}
	cfg, err := loadConfig(*path)
	if err != nil {
		return err
	}
	runtime, err := a.openRuntime(ctx, cfg, false)
	if err != nil {
		return err
	}
	defer runtime.Close()
	bindings, err := selectStreams(cfg, *sourceID, *streamID)
	if err != nil {
		return err
	}
	for _, binding := range bindings {
		item, err := runtime.state.LatestCompaction(ctx, binding.source.ID, binding.stream.ID, *date)
		if err != nil {
			return err
		}
		if err := compact.VerifyDirectory(cfg.Storage.Root, item); err != nil {
			return err
		}
		_, _ = fmt.Fprintf(a.stdout, "%s/%s date=%s revision=%d verified\n", item.SourceID, item.StreamID, item.ArchiveDate, item.Revision)
	}
	return nil
}

func (a *App) status(ctx context.Context, args []string) error {
	fs := newFlagSet("status", a.stderr)
	path := fs.String("config", "", "configuration file")
	asJSON := fs.Bool("json", false, "emit JSON")
	if err := fs.Parse(args); err != nil {
		return err
	}
	cfg, err := loadConfig(*path)
	if err != nil {
		return err
	}
	runtime, err := a.openRuntime(ctx, cfg, false)
	if err != nil {
		return err
	}
	defer runtime.Close()
	status, err := runtime.state.Status(ctx)
	if err != nil {
		return err
	}
	usage, err := runtime.raw.DiskUsage()
	if err != nil {
		return err
	}
	result := struct {
		State state.Status       `json:"state"`
		Disk  rawstore.DiskUsage `json:"disk"`
	}{status, usage}
	if *asJSON {
		return json.NewEncoder(a.stdout).Encode(result)
	}
	_, _ = fmt.Fprintf(a.stdout, "ticks=%d captures=%d summarized_days=%d compactions=%d uploads_pending=%d uploads_failed=%d uploads_verified=%d uploads_retired=%d disk_used=%.1f%%\n", status.Ticks, status.Captures, status.SummarizedDays, status.Compactions, status.Pending, status.Failed, status.Verified, status.Retired, usage.UsedPercent)
	return nil
}

func (a *App) repair(ctx context.Context, args []string) error {
	if len(args) == 0 || args[0] != "reconcile" {
		return errors.New("repair currently requires the reconcile subcommand")
	}
	fs := newFlagSet("repair reconcile", a.stderr)
	path := fs.String("config", "", "configuration file")
	if err := fs.Parse(args[1:]); err != nil {
		return err
	}
	cfg, err := loadConfig(*path)
	if err != nil {
		return err
	}
	runtime, err := a.openRuntime(ctx, cfg, true)
	if err != nil {
		return err
	}
	defer runtime.Close()
	result, err := reconcile.All(ctx, cfg, runtime.raw, runtime.state)
	if err != nil {
		return err
	}
	if err := runtime.state.RecordRepair(ctx, "reconcile", fmt.Sprintf("captures=%d compactions=%d corrupt=%d", result.CapturesRegistered, result.CompactionsAdopted, result.Corrupt), "manual repair command"); err != nil {
		return err
	}
	_, _ = fmt.Fprintf(a.stdout, "captures_registered=%d compactions_adopted=%d corrupt=%d\n", result.CapturesRegistered, result.CompactionsAdopted, result.Corrupt)
	return nil
}

func (a *App) retireDestination(ctx context.Context, args []string) error {
	fs := newFlagSet("retire-destination", a.stderr)
	path := fs.String("config", "", "configuration file")
	destination := fs.String("destination", "", "captured destination ID")
	reason := fs.String("reason", "", "operator reason recorded in the repair audit")
	if err := fs.Parse(args); err != nil {
		return err
	}
	cleanReason := strings.TrimSpace(*reason)
	if !config.ValidID(*destination) {
		return errors.New("--destination must be a configured-style safe ID")
	}
	if cleanReason == "" {
		return errors.New("--reason is required")
	}
	if len(cleanReason) > 500 || strings.ContainsAny(cleanReason, "\r\n\x00") {
		return errors.New("--reason must be at most 500 characters without control newlines")
	}
	cfg, err := loadConfig(*path)
	if err != nil {
		return err
	}
	runtime, err := a.openRuntime(ctx, cfg, true)
	if err != nil {
		return err
	}
	defer runtime.Close()
	count, err := runtime.state.RetireDestination(ctx, *destination, cleanReason)
	if err != nil {
		return err
	}
	_, _ = fmt.Fprintf(a.stdout, "retired %d required upload(s) for destination %s\n", count, *destination)
	return nil
}

type runtimeComponents struct {
	cfg      *config.Config
	lock     *rawstore.Lock
	raw      *rawstore.Store
	state    *state.Store
	uploader *upload.Uploader
	log      *slog.Logger
}

func (a *App) openRuntime(ctx context.Context, cfg *config.Config, exclusive bool) (*runtimeComponents, error) {
	var lock *rawstore.Lock
	var err error
	if exclusive {
		lock, err = rawstore.AcquireLock(cfg.Storage.Root)
		if err != nil {
			return nil, err
		}
	}
	fail := func(err error) (*runtimeComponents, error) {
		if lock != nil {
			_ = lock.Close()
		}
		return nil, err
	}
	raw, err := rawstore.Open(cfg.Storage.Root)
	if err != nil {
		return fail(err)
	}
	store, err := state.Open(ctx, cfg.Storage.StateDB)
	if err != nil {
		return fail(err)
	}
	if err := store.IntegrityCheck(ctx); err != nil {
		store.Close()
		return fail(err)
	}
	uploader := upload.New(cfg, store, a.log, nil)
	return &runtimeComponents{cfg: cfg, lock: lock, raw: raw, state: store, uploader: uploader, log: a.log}, nil
}

func (r *runtimeComponents) uploaderWithObserver(observer upload.Observer) *upload.Uploader {
	return upload.New(r.cfg, r.state, r.log, observer)
}
func (r *runtimeComponents) Close() {
	if r.state != nil {
		_ = r.state.Close()
	}
	if r.lock != nil {
		_ = r.lock.Close()
	}
}

type binding struct {
	source config.Source
	stream config.Stream
}

func selectStreams(cfg *config.Config, sourceID, streamID string) ([]binding, error) {
	var result []binding
	for _, source := range cfg.Sources {
		if sourceID != "" && source.ID != sourceID {
			continue
		}
		for _, stream := range source.Streams {
			if streamID != "" && stream.ID != streamID {
				continue
			}
			result = append(result, binding{source: source, stream: stream})
		}
	}
	if len(result) == 0 {
		return nil, errors.New("no configured streams match the selection")
	}
	return result, nil
}

func configuredDestination(cfg *config.Config, id string) (*config.Destination, bool) {
	for index := range cfg.Destinations {
		if cfg.Destinations[index].ID == id {
			return &cfg.Destinations[index], true
		}
	}
	return nil, false
}

func loadConfig(path string) (*config.Config, error) {
	if strings.TrimSpace(path) == "" {
		return nil, errors.New("--config is required")
	}
	return config.Load(filepath.Clean(path))
}

func newFlagSet(name string, output io.Writer) *flag.FlagSet {
	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	fs.SetOutput(output)
	return fs
}

func countStreams(cfg *config.Config) int {
	n := 0
	for _, source := range cfg.Sources {
		n += len(source.Streams)
	}
	return n
}

func (a *App) usage() {
	_, _ = fmt.Fprintln(a.stderr, `usage: gtfs-rt-archiver COMMAND [OPTIONS]

commands:
  run, validate-config, fetch-once, compact, upload, verify, status,
  repair reconcile, retire-destination, version`)
}
