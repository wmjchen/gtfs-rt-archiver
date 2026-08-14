package reconcile

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"gtfs-rt-archiver/internal/compact"
	"gtfs-rt-archiver/internal/config"
	"gtfs-rt-archiver/internal/model"
	"gtfs-rt-archiver/internal/rawstore"
	"gtfs-rt-archiver/internal/state"
)

type Result struct{ CapturesRegistered, CompactionsAdopted, Corrupt int }

func All(ctx context.Context, cfg *config.Config, raw *rawstore.Store, store *state.Store) (Result, error) {
	registered, corrupt, err := raw.Reconcile(ctx, store)
	result := Result{CapturesRegistered: registered, Corrupt: corrupt}
	if err != nil {
		return result, err
	}
	parquetRoot := filepath.Join(raw.Root(), "parquet")
	err = filepath.WalkDir(parquetRoot, func(manifestPath string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			if errors.Is(walkErr, os.ErrNotExist) {
				return nil
			}
			return walkErr
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if entry.Type()&os.ModeSymlink != 0 {
			if entry.IsDir() {
				return filepath.SkipDir
			}
			result.Corrupt++
			return nil
		}
		if entry.IsDir() || entry.Name() != "manifest.json" {
			return nil
		}
		b, err := os.ReadFile(manifestPath)
		if err != nil {
			result.Corrupt++
			return nil
		}
		var manifest model.Manifest
		if err := json.Unmarshal(b, &manifest); err != nil {
			result.Corrupt++
			return nil
		}
		if manifest.FormatVersion != model.ParquetFormatVersion || manifest.Revision < 1 || manifest.SourceID == "" || manifest.StreamID == "" || manifest.ArchiveDate == "" {
			result.Corrupt++
			return nil
		}
		has, err := store.HasCompaction(ctx, manifest.SourceID, manifest.StreamID, manifest.ArchiveDate, manifest.Revision)
		if err != nil {
			return err
		}
		if has {
			existing, lookupErr := store.CompactionForRevision(ctx, manifest.SourceID, manifest.StreamID, manifest.ArchiveDate, manifest.Revision)
			if lookupErr != nil || compact.VerifyDirectory(raw.Root(), existing) != nil {
				result.Corrupt++
			}
			return nil
		}
		directory, err := filepath.Rel(raw.Root(), filepath.Dir(manifestPath))
		if err != nil || strings.HasPrefix(directory, "..") {
			result.Corrupt++
			return nil
		}
		expectedDirectory := filepath.Join("parquet", "format=v1", "source="+manifest.SourceID,
			"stream="+manifest.StreamID, "date="+manifest.ArchiveDate, fmt.Sprintf("revision=%d", manifest.Revision))
		if filepath.Clean(directory) != expectedDirectory || int64(len(manifest.InputCaptures)) != manifest.CapturedResponses {
			result.Corrupt++
			return nil
		}
		compaction := &model.Compaction{
			SourceID: manifest.SourceID, StreamID: manifest.StreamID, ArchiveDate: manifest.ArchiveDate,
			Revision: manifest.Revision, Status: "ready", Directory: filepath.ToSlash(directory),
			ManifestPath: filepath.ToSlash(filepath.Join(directory, "manifest.json")), Rows: manifest.CapturedResponses,
			Entities: manifest.EntityTotal, RequiredDestinations: manifest.RequiredDestinations, CreatedAt: manifest.CreatedAt,
		}
		if len(manifest.Files) > 1 {
			result.Corrupt++
			return nil
		}
		if len(manifest.Files) == 1 {
			artifact := manifest.Files[0]
			if filepath.Base(artifact.RelativePath) != artifact.RelativePath {
				result.Corrupt++
				return nil
			}
			compaction.DataPath = filepath.ToSlash(filepath.Join(directory, artifact.RelativePath))
			compaction.DataSHA256, compaction.DataBytes = artifact.SHA256, artifact.Bytes
		}
		if err := compact.VerifyDirectory(raw.Root(), compaction); err != nil {
			result.Corrupt++
			return nil
		}
		destinations := map[string]bool{}
		for _, destination := range manifest.Destinations {
			destinations[destination.ID] = destination.Required
		}
		// Compatibility with manifests created before the complete destination
		// snapshot was added. Required queues must always be recoverable.
		if len(manifest.Destinations) == 0 {
			for _, id := range manifest.RequiredDestinations {
				destinations[id] = true
			}
		}
		if len(manifest.RequiredDestinations) == 0 && cfg.Storage.AllowLocalOnlyCleanup {
			published := manifest.CreatedAt
			compaction.Status = "published"
			compaction.PublishedAt = &published
		}
		if err := store.AdoptCompaction(ctx, compaction, manifest.InputCaptures, destinations); err != nil {
			return fmt.Errorf("adopt compaction %s: %w", directory, err)
		}
		result.CompactionsAdopted++
		return nil
	})
	if errors.Is(err, os.ErrNotExist) {
		err = nil
	}
	return result, err
}
