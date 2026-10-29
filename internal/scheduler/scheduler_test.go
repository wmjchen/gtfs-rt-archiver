package scheduler

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"gtfs-rt-archiver/internal/config"
	"gtfs-rt-archiver/internal/fetch"
	"gtfs-rt-archiver/internal/state"
)

func TestStablePhaseAndNextTick(t *testing.T) {
	interval := 30 * time.Second
	a := StablePhase("source/stream", interval)
	if a != StablePhase("source/stream", interval) || a < 0 || a >= interval {
		t.Fatalf("unstable phase: %s", a)
	}
	now := time.Unix(1_700_000_000, 123).UTC()
	next := NextTick(now, interval, a)
	if !next.After(now) || next.Sub(now) > interval {
		t.Fatalf("next tick %s is out of range", next)
	}
	if floorMod(next.UnixNano()-int64(a), int64(interval)) != 0 {
		t.Fatal("tick is not phase aligned")
	}
}

func TestStreamsNeverSelfOverlapButRunConcurrently(t *testing.T) {
	root := t.TempDir()
	cfg := config.Defaults()
	cfg.Storage.Root = root
	cfg.Storage.StateDB = filepath.Join(root, "state.sqlite")
	cfg.Runtime.FetchConcurrency = 10
	cfg.Runtime.PerHostConcurrency = 10
	cfg.HTTP.MaxStartLateness = config.Duration{Duration: 20 * time.Millisecond}
	source := config.Source{ID: "demo", Timezone: "UTC", Location: time.UTC}
	for i := 0; i < 10; i++ {
		source.Streams = append(source.Streams, config.Stream{ID: fmt.Sprintf("feed_%d", i), URL: "https://example.test/feed", ExpectedKind: "mixed", Interval: config.Duration{Duration: 50 * time.Millisecond}})
	}
	cfg.Sources = []config.Source{source}
	db, err := state.Open(context.Background(), cfg.Storage.StateDB)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	fake := &trackingFetcher{delay: 30 * time.Millisecond, active: map[string]int{}}
	scheduler := New(&cfg, fake, db, slog.New(slog.NewTextHandler(os.Stderr, nil)), nil, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	if err := scheduler.Run(ctx); err != nil {
		t.Fatal(err)
	}
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if fake.selfOverlap {
		t.Fatal("a stream overlapped one of its own requests")
	}
	if fake.maxGlobal < 2 {
		t.Fatalf("unrelated streams did not run concurrently; max=%d", fake.maxGlobal)
	}
}

type trackingFetcher struct {
	mu                sync.Mutex
	delay             time.Duration
	active            map[string]int
	global, maxGlobal int
	selfOverlap       bool
}

func (f *trackingFetcher) Fetch(ctx context.Context, source config.Source, stream config.Stream, _ time.Time) (fetch.Result, error) {
	key := source.ID + "/" + stream.ID
	f.mu.Lock()
	f.active[key]++
	f.global++
	if f.active[key] > 1 {
		f.selfOverlap = true
	}
	if f.global > f.maxGlobal {
		f.maxGlobal = f.global
	}
	f.mu.Unlock()
	timer := time.NewTimer(f.delay)
	select {
	case <-ctx.Done():
		timer.Stop()
	case <-timer.C:
	}
	f.mu.Lock()
	f.active[key]--
	f.global--
	f.mu.Unlock()
	return fetch.Result{}, nil
}

func TestEffectiveLatenessIsBounded(t *testing.T) {
	if got := effectiveLateness(5*time.Second, time.Second); got != 500*time.Millisecond {
		t.Fatalf("got %s", got)
	}
	if got := effectiveLateness(2*time.Second, 30*time.Second); got != 2*time.Second {
		t.Fatalf("got %s", got)
	}
}

func TestShutdownLetsStartedFetchFinish(t *testing.T) {
	root := t.TempDir()
	cfg := config.Defaults()
	cfg.Storage.Root = root
	cfg.Storage.StateDB = filepath.Join(root, "state.sqlite")
	cfg.HTTP.MaxStartLateness = config.Duration{Duration: 20 * time.Millisecond}
	cfg.Sources = []config.Source{{ID: "demo", Timezone: "UTC", Location: time.UTC, Streams: []config.Stream{{
		ID: "feed", URL: "https://example.test/feed", ExpectedKind: "mixed", Interval: config.Duration{Duration: 50 * time.Millisecond},
	}}}}
	db, err := state.Open(context.Background(), cfg.Storage.StateDB)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	fake := &gracefulFetcher{started: make(chan struct{}), release: make(chan struct{})}
	scheduler := New(&cfg, fake, db, slog.New(slog.NewTextHandler(os.Stderr, nil)), nil, nil)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- scheduler.Run(ctx) }()
	select {
	case <-fake.started:
	case <-time.After(time.Second):
		t.Fatal("fetch did not start")
	}
	cancel()
	close(fake.release)
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("scheduler did not finish")
	}
	if fake.cancelled {
		t.Fatal("started fetch context was cancelled instead of receiving the grace period")
	}
}

type gracefulFetcher struct {
	started   chan struct{}
	release   chan struct{}
	once      sync.Once
	cancelled bool
}

func (f *gracefulFetcher) Fetch(ctx context.Context, _ config.Source, _ config.Stream, _ time.Time) (fetch.Result, error) {
	f.once.Do(func() { close(f.started) })
	select {
	case <-ctx.Done():
		f.cancelled = true
	case <-f.release:
	}
	return fetch.Result{}, nil
}
