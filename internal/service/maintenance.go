package service

import (
	"context"
	"log/slog"
	"sort"
	"sync"
	"time"

	"gtfs-rt-archiver/internal/compact"
	"gtfs-rt-archiver/internal/config"
	"gtfs-rt-archiver/internal/lifecycle"
	"gtfs-rt-archiver/internal/observability"
	"gtfs-rt-archiver/internal/state"
	"gtfs-rt-archiver/internal/upload"
)

type Maintenance struct {
	cfg       *config.Config
	state     *state.Store
	compactor *compact.Compactor
	uploader  *upload.Uploader
	retention *lifecycle.Retention
	metrics   *observability.Metrics
	log       *slog.Logger
	archiveMu sync.Mutex
}

func NewMaintenance(cfg *config.Config, store *state.Store, compactor *compact.Compactor,
	uploader *upload.Uploader, retention *lifecycle.Retention, metrics *observability.Metrics,
	log *slog.Logger) *Maintenance {
	return &Maintenance{cfg: cfg, state: store, compactor: compactor, uploader: uploader,
		retention: retention, metrics: metrics, log: log}
}

func (m *Maintenance) Run(ctx context.Context) {
	var wg sync.WaitGroup
	wg.Add(3)
	go func() {
		defer wg.Done()
		m.compactionLoop(ctx)
	}()
	go func() {
		defer wg.Done()
		m.uploadLoop(ctx)
	}()
	go func() {
		defer wg.Done()
		m.retentionLoop(ctx)
	}()
	<-ctx.Done()
	wg.Wait()
}

func (m *Maintenance) compactionLoop(ctx context.Context) {
	m.compactEligible(ctx)
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			m.compactEligible(ctx)
		}
	}
}

func (m *Maintenance) compactEligible(ctx context.Context) {
	m.archiveMu.Lock()
	defer m.archiveMu.Unlock()
	now := time.Now()
	type job struct {
		sourceID, streamID, date string
	}
	var jobs []job
	for _, source := range m.cfg.Sources {
		for _, stream := range source.Streams {
			if err := ctx.Err(); err != nil {
				return
			}
			earliest, err := m.state.EarliestTick(ctx, source.ID, stream.ID)
			if err != nil {
				m.log.Error("find compaction backlog", "source", source.ID, "stream", stream.ID, "error", err)
				continue
			}
			if earliest == nil {
				continue
			}
			loc, err := stream.EffectiveLocation(source)
			if err != nil {
				continue
			}
			local := earliest.In(loc)
			day := time.Date(local.Year(), local.Month(), local.Day(), 0, 0, 0, 0, loc)
			for ; !now.Before(day.AddDate(0, 0, 1).Add(m.cfg.Storage.CloseDelay.Duration)); day = day.AddDate(0, 0, 1) {
				end := day.AddDate(0, 0, 1)
				needs, err := m.state.DayNeedsCompaction(ctx, source.ID, stream.ID, day, end)
				if err != nil {
					m.log.Error("check daily compaction", "source", source.ID, "stream", stream.ID, "date", day.Format(time.DateOnly), "error", err)
					break
				}
				if !needs {
					continue
				}
				jobs = append(jobs, job{sourceID: source.ID, streamID: stream.ID, date: day.Format(time.DateOnly)})
			}
		}
	}
	if len(jobs) == 0 {
		return
	}
	sort.Slice(jobs, func(i, j int) bool {
		if jobs[i].date != jobs[j].date {
			return jobs[i].date < jobs[j].date
		}
		if jobs[i].sourceID != jobs[j].sourceID {
			return jobs[i].sourceID < jobs[j].sourceID
		}
		return jobs[i].streamID < jobs[j].streamID
	})
	queue := make(chan job)
	var workers sync.WaitGroup
	streamLocks := map[string]*sync.Mutex{}
	for _, item := range jobs {
		key := item.sourceID + "/" + item.streamID
		if streamLocks[key] == nil {
			streamLocks[key] = &sync.Mutex{}
		}
	}
	workerCount := min(m.cfg.Runtime.CompactionConcurrency, len(jobs))
	workers.Add(workerCount)
	for range workerCount {
		go func() {
			defer workers.Done()
			for item := range queue {
				if ctx.Err() != nil {
					return
				}
				started := time.Now()
				streamLock := streamLocks[item.sourceID+"/"+item.streamID]
				streamLock.Lock()
				if ctx.Err() != nil {
					streamLock.Unlock()
					return
				}
				_, err := m.compactor.Compact(ctx, compact.Request{SourceID: item.sourceID, StreamID: item.streamID, Date: item.date})
				streamLock.Unlock()
				result := "success"
				if err != nil {
					result = "failure"
					m.log.Error("daily compaction failed", "source", item.sourceID, "stream", item.streamID, "date", item.date, "error", err)
				}
				m.metrics.ObserveCompaction(item.sourceID, item.streamID, result, time.Since(started))
			}
		}()
	}
	for _, item := range jobs {
		select {
		case queue <- item:
		case <-ctx.Done():
			close(queue)
			workers.Wait()
			return
		}
	}
	close(queue)
	workers.Wait()
}

func (m *Maintenance) uploadLoop(ctx context.Context) {
	m.uploadAll(ctx)
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			m.uploadAll(ctx)
		}
	}
}

func (m *Maintenance) uploadAll(ctx context.Context) {
	if len(m.cfg.Destinations) == 0 {
		return
	}
	sem := make(chan struct{}, m.cfg.Runtime.UploadConcurrency)
	var wg sync.WaitGroup
	for _, configured := range m.cfg.Destinations {
		dest := configured
		wg.Add(1)
		go func() {
			defer wg.Done()
			select {
			case sem <- struct{}{}:
			case <-ctx.Done():
				return
			}
			defer func() { <-sem }()
			if _, err := m.uploader.ProcessPending(ctx, dest.ID); err != nil && ctx.Err() == nil {
				m.log.Error("upload worker failed", "destination", dest.ID, "error", err)
			}
		}()
	}
	wg.Wait()
}

func (m *Maintenance) retentionLoop(ctx context.Context) {
	m.cleanup(ctx)
	ticker := time.NewTicker(time.Hour)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			m.cleanup(ctx)
		}
	}
}

func (m *Maintenance) cleanup(ctx context.Context) {
	m.archiveMu.Lock()
	defer m.archiveMu.Unlock()
	result, err := m.retention.Cleanup(ctx, time.Now())
	if err != nil {
		if ctx.Err() == nil {
			m.log.Error("retention cleanup failed", "error", err)
		}
		return
	}
	if result.RawFiles+result.ParquetFiles+result.SummarizedDays > 0 {
		m.log.Info("retention cleanup completed", "raw_files", result.RawFiles, "parquet_files", result.ParquetFiles, "summarized_days", result.SummarizedDays, "bytes", result.Bytes)
	}
}
