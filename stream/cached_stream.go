package stream

import (
	"context"
	"fmt"
	"sync"

	"github.com/google/uuid"
	"github.com/pg2iceberg/pg2iceberg/iceberg"
	"golang.org/x/sync/errgroup"
)

// CachedStream wraps a Stream with an in-memory cache of recently staged files.
// In combined mode (single process running both WAL writer and materializer),
// the materializer reads staged Parquet data from the cache instead of
// downloading it from S3, avoiding a redundant round-trip for data that was
// just uploaded.
//
// On cold start (restart/recovery), the cache is empty and Download falls back
// to S3 — this is the correct behavior for crash recovery.
type CachedStream struct {
	coord     Coordinator
	s3        iceberg.ObjectStorage
	namespace string

	mu    sync.Mutex
	cache map[string][]byte // s3Key -> parquet data
}

// NewCachedStream creates a Stream with an in-memory download cache.
func NewCachedStream(coord Coordinator, s3 iceberg.ObjectStorage, namespace string) *CachedStream {
	return &CachedStream{
		coord:     coord,
		s3:        s3,
		namespace: namespace,
		cache:     make(map[string][]byte),
	}
}

// Coordinator returns the underlying coordinator for direct cursor/lock access.
func (cs *CachedStream) Coordinator() Coordinator { return cs.coord }

// Append stages Parquet files to S3, atomically registers them in the log
// index, and caches the data in memory so the co-located materializer can
// read it without an S3 round-trip.
func (cs *CachedStream) Append(ctx context.Context, batches []WriteBatch) error {
	if len(batches) == 0 {
		return nil
	}

	type staged struct {
		key      string
		byteSize int64
	}
	files := make([]staged, len(batches))

	g, gctx := errgroup.WithContext(ctx)
	for i, b := range batches {
		i, b := i, b
		g.Go(func() error {
			id, err := uuid.NewV7()
			if err != nil {
				return fmt.Errorf("generate uuid: %w", err)
			}
			key := fmt.Sprintf("staged/%s/%s.parquet", b.Table, id.String())
			if _, err := cs.s3.Upload(gctx, key, b.Data); err != nil {
				return fmt.Errorf("upload staged file for %s: %w", b.Table, err)
			}
			files[i] = staged{key: key, byteSize: int64(len(b.Data))}
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return err
	}

	appends := make([]LogAppend, len(batches))
	for i, b := range batches {
		appends[i] = LogAppend{
			Table:       b.Table,
			S3Path:      files[i].key,
			RecordCount: b.RecordCount,
			ByteSize:    files[i].byteSize,
		}
	}

	if _, err := cs.coord.ClaimOffsets(ctx, appends); err != nil {
		return fmt.Errorf("claim offsets: %w", err)
	}

	// Cache the data for the co-located materializer.
	cs.mu.Lock()
	for i, b := range batches {
		cp := make([]byte, len(b.Data))
		copy(cp, b.Data)
		cs.cache[files[i].key] = cp
	}
	cs.mu.Unlock()

	return nil
}

// Read returns log entries after the given offset for a table.
func (cs *CachedStream) Read(ctx context.Context, table string, afterOffset int64) ([]LogEntry, error) {
	return cs.coord.ReadLog(ctx, table, afterOffset)
}

// Download returns staged Parquet data from the in-memory cache if available,
// falling back to S3 on cache miss (cold start / recovery).
func (cs *CachedStream) Download(ctx context.Context, s3Path string) ([]byte, error) {
	cs.mu.Lock()
	data, ok := cs.cache[s3Path]
	cs.mu.Unlock()
	if ok {
		return data, nil
	}
	// Cache miss — fall back to S3 (recovery path).
	return cs.s3.Download(ctx, s3Path)
}

// Evict removes entries from the cache. Call after the materializer
// successfully commits to keep the cache bounded.
func (cs *CachedStream) Evict(paths []string) {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	for _, p := range paths {
		delete(cs.cache, p)
	}
}

// Truncate removes processed log entries and deletes their S3 files.
// Also evicts the entries from the cache.
func (cs *CachedStream) Truncate(ctx context.Context, table string, atOrBeforeOffset int64) error {
	paths, err := cs.coord.TruncateLog(ctx, table, atOrBeforeOffset)
	if err != nil {
		return err
	}
	cs.Evict(paths)
	if len(paths) > 0 {
		if err := cs.s3.DeleteObjects(ctx, paths); err != nil {
			return fmt.Errorf("delete staged files: %w", err)
		}
	}
	return nil
}
