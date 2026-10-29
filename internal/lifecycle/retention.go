package lifecycle

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"gtfs-rt-archiver/internal/config"
	"gtfs-rt-archiver/internal/rawstore"
	"gtfs-rt-archiver/internal/state"
)

type Retention struct {
	cfg   *config.Config
	raw   *rawstore.Store
	state *state.Store
}

type CleanupResult struct {
	RawFiles, ParquetFiles int
	SummarizedDays         int
	Bytes                  int64
}

func NewRetention(cfg *config.Config, raw *rawstore.Store, store *state.Store) *Retention {
	return &Retention{cfg: cfg, raw: raw, state: store}
}

func (r *Retention) Cleanup(ctx context.Context, now time.Time) (CleanupResult, error) {
	var result CleanupResult
	if !hasRequired(r.cfg) && !r.cfg.Storage.AllowLocalOnlyCleanup {
		return result, nil
	}
	rawCandidates, err := r.state.RawRetentionCandidates(ctx, now.Add(-r.cfg.Storage.RawRetentionAfterUpload.Duration))
	if err != nil {
		return result, err
	}
	for _, candidate := range rawCandidates {
		var deleted int
		var bytes int64
		for _, relative := range []string{candidate.RawPath, candidate.SidecarPath} {
			if relative == "" {
				continue
			}
			removed, size, err := removeRegular(r.raw, relative)
			if err != nil {
				return result, fmt.Errorf("remove retained capture %s: %w", candidate.ID, err)
			}
			if removed {
				deleted++
				bytes += size
			}
		}
		if err := r.state.MarkCaptureRetained(ctx, candidate.ID); err != nil {
			return result, err
		}
		result.RawFiles += deleted
		result.Bytes += bytes
	}
	parquetCandidates, err := r.state.ParquetRetentionCandidates(ctx, now.Add(-r.cfg.Storage.ParquetRetentionAfterUpload.Duration))
	if err != nil {
		return result, err
	}
	for _, candidate := range parquetCandidates {
		var deleted int
		var bytes int64
		for _, relative := range []string{candidate.DataPath, candidate.ManifestPath} {
			if relative == "" {
				continue
			}
			removed, size, err := removeRegular(r.raw, relative)
			if err != nil {
				return result, fmt.Errorf("remove retained dataset %d: %w", candidate.ID, err)
			}
			if removed {
				deleted++
				bytes += size
			}
		}
		if err := r.state.MarkParquetRetained(ctx, candidate.ID); err != nil {
			return result, err
		}
		result.ParquetFiles += deleted
		result.Bytes += bytes
	}
	summaryCandidates, err := r.state.SummaryCandidates(ctx, now.Add(-r.cfg.Storage.MetadataRetention.Duration))
	if err != nil {
		return result, err
	}
	for _, candidate := range summaryCandidates {
		location, ok := configuredLocation(r.cfg, candidate.SourceID, candidate.StreamID)
		if !ok {
			continue
		}
		day, err := time.ParseInLocation(time.DateOnly, candidate.ArchiveDate, location)
		if err != nil {
			return result, err
		}
		done, err := r.state.SummarizeDay(ctx, candidate.SourceID, candidate.StreamID, candidate.ArchiveDate, day, day.AddDate(0, 0, 1))
		if err != nil {
			return result, err
		}
		if done {
			result.SummarizedDays++
		}
	}
	if result.RawFiles+result.ParquetFiles+result.SummarizedDays > 0 {
		_ = r.state.RecordRepair(ctx, "retention_cleanup", fmt.Sprintf("raw=%d parquet=%d summarized=%d bytes=%d", result.RawFiles, result.ParquetFiles, result.SummarizedDays, result.Bytes), "policy elapsed")
	}
	return result, nil
}

func configuredLocation(cfg *config.Config, sourceID, streamID string) (*time.Location, bool) {
	for _, source := range cfg.Sources {
		if source.ID != sourceID {
			continue
		}
		for _, stream := range source.Streams {
			if stream.ID != streamID {
				continue
			}
			location, err := stream.EffectiveLocation(source)
			return location, err == nil
		}
	}
	return nil, false
}

func removeRegular(raw *rawstore.Store, relative string) (bool, int64, error) {
	path, err := raw.Absolute(relative)
	if err != nil {
		return false, 0, err
	}
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return false, 0, nil
	}
	if err != nil {
		return false, 0, err
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
		return false, 0, errors.New("retention target is not a regular file")
	}
	if err := os.Remove(path); err != nil {
		return false, 0, err
	}
	return true, info.Size(), nil
}

func hasRequired(cfg *config.Config) bool {
	for _, dest := range cfg.Destinations {
		if dest.IsRequired() {
			return true
		}
	}
	return false
}
