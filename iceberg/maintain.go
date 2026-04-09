package iceberg

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/pg2iceberg/pg2iceberg/pipeline"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

var maintainTracer = otel.Tracer("pg2iceberg/maintain")

// MaintenanceConfig holds parameters for table maintenance.
type MaintenanceConfig struct {
	SnapshotRetention time.Duration // remove snapshots older than this
	OrphanGracePeriod time.Duration // don't delete files newer than this
}

// MaintainTable runs snapshot expiry and orphan file deletion for a single table.
func MaintainTable(ctx context.Context, catalog *MetadataStore, s3 ObjectStorage, ns, table string, cfg MaintenanceConfig) error {
	ctx, span := maintainTracer.Start(ctx, "maintain.Table "+table, trace.WithAttributes(
		attribute.String("iceberg.namespace", ns),
		attribute.String("iceberg.table", table),
	))
	defer span.End()

	start := time.Now()
	defer func() {
		pipeline.MaintenanceDurationSeconds.WithLabelValues(table).Observe(time.Since(start).Seconds())
	}()
	pipeline.MaintenanceRunsTotal.WithLabelValues(table).Inc()

	// 1. Load table metadata.
	tm, err := catalog.LoadTable(ctx, ns, table)
	if err != nil {
		pipeline.MaintenanceErrorsTotal.WithLabelValues(table).Inc()
		return fmt.Errorf("load table %s: %w", table, err)
	}
	if tm == nil {
		return nil // table doesn't exist
	}

	// 2. Expire old snapshots.
	expired, err := expireSnapshots(ctx, catalog, ns, table, tm, cfg.SnapshotRetention)
	if err != nil {
		pipeline.MaintenanceErrorsTotal.WithLabelValues(table).Inc()
		return fmt.Errorf("expire snapshots for %s: %w", table, err)
	}
	if expired > 0 {
		pipeline.MaintenanceSnapshotsExpiredTotal.WithLabelValues(table).Add(float64(expired))
		log.Printf("[maintain] %s: expired %d snapshots", table, expired)

		// Reload metadata to reflect post-expiry state.
		tm, err = catalog.Inner().LoadTable(ctx, ns, table)
		if err != nil {
			pipeline.MaintenanceErrorsTotal.WithLabelValues(table).Inc()
			return fmt.Errorf("reload table %s after expiry: %w", table, err)
		}
	}

	// 3. Clean orphan files.
	deleted, err := cleanOrphanFiles(ctx, s3, ns, table, tm, cfg.OrphanGracePeriod)
	if err != nil {
		pipeline.MaintenanceErrorsTotal.WithLabelValues(table).Inc()
		return fmt.Errorf("clean orphans for %s: %w", table, err)
	}
	if deleted > 0 {
		pipeline.MaintenanceOrphansDeletedTotal.WithLabelValues(table).Add(float64(deleted))
		log.Printf("[maintain] %s: deleted %d orphan files", table, deleted)
	}

	return nil
}

// expireSnapshots removes snapshots older than retention, never removing the current snapshot.
func expireSnapshots(ctx context.Context, catalog *MetadataStore, ns, table string, tm *TableMetadata, retention time.Duration) (int, error) {
	if tm.Metadata.CurrentSnapshotID <= 0 || len(tm.Metadata.Snapshots) <= 1 {
		return 0, nil
	}

	cutoff := time.Now().Add(-retention).UnixMilli()
	var toRemove []int64

	for _, snap := range tm.Metadata.Snapshots {
		if snap.SnapshotID == tm.Metadata.CurrentSnapshotID {
			continue // never remove current
		}
		if snap.TimestampMs < cutoff {
			toRemove = append(toRemove, snap.SnapshotID)
		}
	}

	if len(toRemove) == 0 {
		return 0, nil
	}

	if err := catalog.RemoveSnapshots(ctx, ns, table, toRemove); err != nil {
		return 0, err
	}
	return len(toRemove), nil
}

// cleanOrphanFiles lists all S3 objects under the table path, compares against
// files referenced by surviving snapshots, and deletes unreferenced files that
// are older than the grace period.
func cleanOrphanFiles(ctx context.Context, s3 ObjectStorage, ns, table string, tm *TableMetadata, gracePeriod time.Duration) (int, error) {
	if tm == nil || tm.Metadata.CurrentSnapshotID <= 0 {
		return 0, nil
	}

	// Collect all files referenced by any surviving snapshot.
	referenced := make(map[string]bool)
	for _, snap := range tm.Metadata.Snapshots {
		if snap.ManifestList == "" {
			continue
		}

		mlKey, err := KeyFromURI(snap.ManifestList)
		if err != nil {
			return 0, fmt.Errorf("parse manifest list URI %s: %w", snap.ManifestList, err)
		}
		referenced[mlKey] = true

		mlData, err := DownloadWithRetry(ctx, s3, mlKey)
		if err != nil {
			return 0, fmt.Errorf("download manifest list %s: %w", snap.ManifestList, err)
		}
		manifests, err := ReadManifestList(mlData)
		if err != nil {
			return 0, fmt.Errorf("read manifest list %s: %w", snap.ManifestList, err)
		}

		for _, mfi := range manifests {
			mKey, err := KeyFromURI(mfi.Path)
			if err != nil {
				return 0, fmt.Errorf("parse manifest URI %s: %w", mfi.Path, err)
			}
			referenced[mKey] = true

			mData, err := DownloadWithRetry(ctx, s3, mKey)
			if err != nil {
				return 0, fmt.Errorf("download manifest %s: %w", mfi.Path, err)
			}
			entries, err := ReadManifest(mData)
			if err != nil {
				return 0, fmt.Errorf("read manifest %s: %w", mfi.Path, err)
			}

			for _, e := range entries {
				if e.Status == 2 { // deleted entry
					continue
				}
				dfKey, err := KeyFromURI(e.DataFile.Path)
				if err != nil {
					continue
				}
				referenced[dfKey] = true
			}
		}
	}

	// List all objects under the table's S3 prefix.
	prefix := fmt.Sprintf("%s.db/%s/", ns, table)
	objects, err := s3.ListObjects(ctx, prefix)
	if err != nil {
		return 0, fmt.Errorf("list objects under %s: %w", prefix, err)
	}

	// Identify orphans: not referenced and older than grace period.
	graceCutoff := time.Now().Add(-gracePeriod)
	var orphanKeys []string
	for _, obj := range objects {
		if referenced[obj.Key] {
			continue
		}
		if obj.LastModified.After(graceCutoff) {
			continue // within grace period
		}
		orphanKeys = append(orphanKeys, obj.Key)
	}

	if len(orphanKeys) == 0 {
		return 0, nil
	}

	if err := s3.DeleteObjects(ctx, orphanKeys); err != nil {
		return 0, fmt.Errorf("delete orphan files: %w", err)
	}
	return len(orphanKeys), nil
}
