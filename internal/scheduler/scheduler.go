package scheduler

import (
	"context"
	"fmt"
	"hash/fnv"
	"log/slog"
	"net/url"
	"sync"
	"time"

	"gtfs-rt-archiver/internal/config"
	"gtfs-rt-archiver/internal/fetch"
	"gtfs-rt-archiver/internal/model"
	"gtfs-rt-archiver/internal/state"
)

type PauseChecker interface{ Paused() bool }

type Observer interface {
	ObserveSkipped(source, stream, reason string)
}

type Fetcher interface {
	Fetch(context.Context, config.Source, config.Stream, time.Time) (fetch.Result, error)
}

type noopObserver struct{}

func (noopObserver) ObserveSkipped(string, string, string) {}

type Scheduler struct {
	cfg      *config.Config
	fetcher  Fetcher
	state    *state.Store
	log      *slog.Logger
	global   chan struct{}
	perHost  map[string]chan struct{}
	paused   PauseChecker
	observer Observer
	now      func() time.Time
}

func New(cfg *config.Config, fetcher Fetcher, store *state.Store, log *slog.Logger, paused PauseChecker, observer Observer) *Scheduler {
	if observer == nil {
		observer = noopObserver{}
	}
	perHost := map[string]chan struct{}{}
	for _, source := range cfg.Sources {
		for _, stream := range source.Streams {
			u, _ := url.Parse(stream.URL)
			if u != nil && perHost[u.Hostname()] == nil {
				perHost[u.Hostname()] = make(chan struct{}, cfg.Runtime.PerHostConcurrency)
			}
		}
	}
	return &Scheduler{cfg: cfg, fetcher: fetcher, state: store, log: log,
		global: make(chan struct{}, cfg.Runtime.FetchConcurrency), perHost: perHost,
		paused: paused, observer: observer, now: time.Now}
}

func (s *Scheduler) Run(ctx context.Context) error {
	var wg sync.WaitGroup
	for _, configuredSource := range s.cfg.Sources {
		source := configuredSource
		for _, configuredStream := range source.Streams {
			stream := configuredStream
			if !stream.IsEnabled() {
				continue
			}
			wg.Add(1)
			go func() {
				defer wg.Done()
				s.runStream(ctx, source, stream)
			}()
		}
	}
	<-ctx.Done()
	wg.Wait()
	return nil
}

func (s *Scheduler) runStream(ctx context.Context, source config.Source, stream config.Stream) {
	interval := stream.Interval.Duration
	phase := StablePhase(source.ID+"/"+stream.ID, interval)
	next := NextTick(s.now(), interval, phase)
	host := ""
	if u, err := url.Parse(stream.URL); err == nil {
		host = u.Hostname()
	}
	hostSem := s.perHost[host]
	for {
		if err := waitUntil(ctx, next, s.now); err != nil {
			return
		}
		started := s.now()
		lateness := effectiveLateness(stream.EffectiveLateness(s.cfg.HTTP), interval)
		if started.Sub(next) > lateness {
			s.recordSkip(ctx, source.ID, stream.ID, next, "late")
			next = next.Add(interval)
			continue
		}
		if s.paused != nil && s.paused.Paused() {
			s.recordSkip(ctx, source.ID, stream.ID, next, "disk_pressure")
			next = next.Add(interval)
			continue
		}
		deadline := next.Add(lateness)
		if !acquireUntil(ctx, s.global, deadline) {
			s.recordSkip(ctx, source.ID, stream.ID, next, "global_concurrency")
			next = next.Add(interval)
			continue
		}
		if hostSem != nil && !acquireUntil(ctx, hostSem, deadline) {
			<-s.global
			s.recordSkip(ctx, source.ID, stream.ID, next, "host_concurrency")
			next = next.Add(interval)
			continue
		}
		if ctx.Err() != nil {
			if hostSem != nil {
				<-hostSem
			}
			<-s.global
			return
		}
		// The scheduler cancellation stops new work. Once a request has started,
		// let the fetcher's own timeout bound it so shutdown can finish a durable
		// capture during the service grace period.
		_, err := s.fetcher.Fetch(context.WithoutCancel(ctx), source, stream, next)
		if hostSem != nil {
			<-hostSem
		}
		<-s.global
		if err != nil && ctx.Err() == nil {
			s.log.Debug("scheduled fetch ended without a capture", "source", source.ID, "stream", stream.ID)
		}
		next = next.Add(interval)
	}
}

func (s *Scheduler) recordSkip(ctx context.Context, sourceID, streamID string, scheduled time.Time, reason string) {
	id, err := model.NewID(s.now())
	if err != nil {
		s.log.Error("generate skipped tick id", "error", err)
		return
	}
	now := s.now().UTC()
	tick := model.Tick{ID: id, SourceID: sourceID, StreamID: streamID, ScheduledAt: scheduled.UTC(),
		StartedAt: &now, FinishedAt: &now, Result: "skipped", SkipReason: reason,
		ConfigFingerprint: s.cfg.Fingerprint()}
	if err := s.state.RecordTick(context.WithoutCancel(ctx), tick); err != nil {
		s.log.Error("record skipped tick", "source", sourceID, "stream", streamID, "reason", reason, "error", err)
		return
	}
	s.observer.ObserveSkipped(sourceID, streamID, reason)
	s.log.Warn("scheduled tick skipped", "source", sourceID, "stream", streamID, "reason", reason,
		"scheduled_at", scheduled.UTC().Format(time.RFC3339Nano))
}

func StablePhase(key string, interval time.Duration) time.Duration {
	if interval <= 0 {
		return 0
	}
	h := fnv.New64a()
	_, _ = h.Write([]byte(key))
	return time.Duration(h.Sum64() % uint64(interval))
}

func NextTick(now time.Time, interval, phase time.Duration) time.Time {
	if interval <= 0 {
		panic("scheduler interval must be positive")
	}
	n := now.UnixNano()
	p := int64(phase % interval)
	base := n - floorMod(n-p, int64(interval))
	if base <= n {
		base += int64(interval)
	}
	return time.Unix(0, base).In(now.Location())
}

func floorMod(value, divisor int64) int64 {
	r := value % divisor
	if r < 0 {
		r += divisor
	}
	return r
}

func effectiveLateness(configured, interval time.Duration) time.Duration {
	if configured <= 0 {
		return 0
	}
	if configured >= interval {
		return interval / 2
	}
	return configured
}

func waitUntil(ctx context.Context, target time.Time, now func() time.Time) error {
	d := target.Sub(now())
	if d <= 0 {
		return nil
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func acquireUntil(ctx context.Context, semaphore chan struct{}, deadline time.Time) bool {
	d := time.Until(deadline)
	if d <= 0 {
		return false
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case semaphore <- struct{}{}:
		return true
	case <-timer.C:
		return false
	case <-ctx.Done():
		return false
	}
}

func (s *Scheduler) String() string { return fmt.Sprintf("scheduler(streams=%d)", streamCount(s.cfg)) }

func streamCount(cfg *config.Config) int {
	n := 0
	for _, source := range cfg.Sources {
		for _, stream := range source.Streams {
			if stream.IsEnabled() {
				n++
			}
		}
	}
	return n
}
