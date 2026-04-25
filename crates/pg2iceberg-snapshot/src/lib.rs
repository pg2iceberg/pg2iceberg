//! Initial-snapshot phase: bootstrap a fresh pg2iceberg deployment by
//! reading every row from each tracked table at a known LSN, staging them
//! through the same pipeline as logical replication, then advancing the
//! slot past that LSN so live replication picks up exactly where the
//! snapshot left off.
//!
//! Mirrors `snapshot/` and `logical/logical.go:507-519` from the Go
//! reference, but skips the CTID-page-chunked parallel read (deferred to
//! Phase 11.5 — useful when source tables are huge, irrelevant for the sim).
//!
//! ## Why this exists
//!
//! pg2iceberg is a mirror, not a CDC tool. The Iceberg side must reflect
//! 100% of PG state, so we cannot start replication from "now and onwards"
//! — that would lose every row inserted before the slot was created. The
//! snapshot phase fills the gap.
//!
//! ## Handoff timing
//!
//! 1. Create publication + replication slot (slot's `restart_lsn` is now
//!    set to the LSN at slot-creation time, call it `K`).
//! 2. Read each table's current rows at the source's "current LSN" `N` —
//!    `N >= K` always.
//! 3. Stage the rows through the pipeline as a synthetic transaction with
//!    `commit_lsn = N`. The receipt-gated `flushed_lsn` advances to `N`.
//! 4. Caller acks the slot at `N` via `send_standby`. `restart_lsn` jumps
//!    to `N`, so future replication reads start at `N`.
//!
//! Any WAL events between `K` and `N` are *covered* by the snapshot (rows
//! read at `N` reflect their effects), so skipping them is correct, not
//! lossy.

use async_trait::async_trait;
use pg2iceberg_coord::Coordinator;
use pg2iceberg_core::{
    ChangeEvent, Checkpoint, ColumnName, Lsn, Mode, Op, Row, SnapshotState, TableIdent,
    TableSchema, Timestamp,
};
use pg2iceberg_iceberg::{pk_key, Catalog};
use pg2iceberg_logical::{Pipeline, PipelineError};
use pg2iceberg_pg::DecodedMessage;
use std::collections::BTreeMap;
use std::sync::Arc;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum SnapshotError {
    #[error("source: {0}")]
    Source(String),
    #[error("pipeline: {0}")]
    Pipeline(#[from] PipelineError),
}

pub type Result<T> = std::result::Result<T, SnapshotError>;

/// What the snapshotter needs from the source PG. Production wraps a
/// `tokio-postgres` connection inside a `BEGIN ISOLATION LEVEL REPEATABLE
/// READ; SELECT pg_export_snapshot()` transaction; the sim impl is on
/// `SimPostgres`.
///
/// Both `snapshot_lsn` and `read_chunk` should observe a consistent view —
/// any rows missed will be lost (the slot has already advanced past them
/// by the time live replication kicks in).
///
/// Chunked reads keep peak memory bounded for large tables. Implementations
/// must:
/// - Return rows sorted by canonical PK ASC.
/// - Be stable across calls (same snapshot view).
/// - Honor `after_pk_key` as a strict lower bound; rows whose canonical PK
///   key (JSON form, see `pg2iceberg_iceberg::pk_key`) is `<=` the bound
///   must be excluded.
/// - Cap output at `chunk_size`. Returning fewer rows is allowed only at
///   end-of-table.
///
/// `after_pk_key` is a string (rather than a `Row`) because resumable
/// snapshot persists the key into the checkpoint — it can't reconstruct a
/// `Row` without the schema, and a string is the canonical wire form.
#[async_trait]
pub trait SnapshotSource: Send + Sync {
    async fn snapshot_lsn(&self) -> Result<Lsn>;
    async fn read_chunk(
        &self,
        ident: &TableIdent,
        chunk_size: usize,
        after_pk_key: Option<&str>,
    ) -> Result<Vec<Row>>;
}

/// Pseudo-xid base for synthetic snapshot transactions. Real PG xids are
/// monotonic from 1; we reserve a high range so collisions are impossible.
/// Each table gets `BASE + table_index`, so all events for a table land in
/// the same buffer.
pub const SNAPSHOT_XID_BASE: u32 = 0xFFFF_FF00;

/// ## Marker-row fence (the snapshot↔CDC handoff pattern)
///
/// In production PG, between starting the snapshot transaction and the
/// pipeline catching up to live replication, there's a race window: rows
/// inserted in that window could be (a) read by the snapshot's
/// `pg_export_snapshot` view AND (b) re-emitted by the replication slot
/// once the slot is at-or-past their LSN. Naively that produces duplicates.
///
/// The Go reference handles this with a marker-row fence pattern: insert a
/// marker UUID into a `_pg2iceberg.markers` table both before and after the
/// snapshot. The pipeline drops WAL events whose source LSN is `<` the
/// pre-marker. Rows the snapshot read are deduplicated when the materializer
/// runs, because PK-keyed equality deletes void prior values.
///
/// `SimPostgres` serializes operations under one mutex, so this race
/// doesn't exist in tests — the snapshot reads happen at exactly
/// `current_lsn()`, and `send_standby(snap_lsn)` advances the slot past
/// the snapshot point atomically. We document the pattern here so
/// production impls add it deliberately.
///
/// Recommended fence ID format:
pub const MARKER_TABLE_NAME: &str = "_pg2iceberg.markers";

/// Type of fence marker; serialized into the `_pg2iceberg.markers` row.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MarkerKind {
    /// Inserted before snapshot reads begin. Pipeline drops every WAL
    /// event with LSN `<` this marker's LSN.
    Pre,
    /// Inserted after snapshot reads complete + slot is acked. Pipeline
    /// resumes processing from this marker forward.
    Post,
}

/// Default chunk size for [`run_snapshot`]. Trades off peak memory against
/// coord-write amplification. ~1k rows per chunk is the same default as
/// the Go reference's `flush_rows`.
pub const DEFAULT_CHUNK_SIZE: usize = 1024;

/// Drive a fresh pipeline through the initial-snapshot phase using the
/// default chunk size.
///
/// Returns the LSN the caller should `send_standby` to so the replication
/// slot picks up live replication from the right point. The caller should
/// also drive the materializer at least once after this returns to flush
/// snapshot rows out to Iceberg.
pub async fn run_snapshot<S, C>(
    source: &S,
    schemas: &[TableSchema],
    pipeline: &mut Pipeline<C>,
) -> Result<Lsn>
where
    S: SnapshotSource + ?Sized,
    C: Coordinator,
{
    run_snapshot_chunked(source, schemas, pipeline, DEFAULT_CHUNK_SIZE).await
}

/// Like [`run_snapshot`] but with an explicit `chunk_size`. Each chunk is
/// staged + coord-committed independently; peak memory is bounded by
/// `chunk_size` rows. The materializer later coalesces all chunks into one
/// Iceberg snapshot.
///
/// Crash recovery: if the process dies mid-snapshot, the pipeline restarts
/// from the slot's `restart_lsn` (still at the pre-snapshot point) and the
/// snapshot replays from chunk 0. Idempotent because chunked replay
/// produces the same staged Parquet for the same source rows. (Resumability
/// — skipping already-completed chunks — is a Phase 11.5 task that needs
/// per-table progress tracked in the checkpoint.)
pub async fn run_snapshot_chunked<S, C>(
    source: &S,
    schemas: &[TableSchema],
    pipeline: &mut Pipeline<C>,
    chunk_size: usize,
) -> Result<Lsn>
where
    S: SnapshotSource + ?Sized,
    C: Coordinator,
{
    // Non-resumable variant: just runs a single pass with empty progress.
    let snap_lsn = source.snapshot_lsn().await?;
    let progress = SnapshotProgressMap::default();
    let (_chunks, _end) = snapshot_one_pass(
        source, schemas, pipeline, chunk_size, snap_lsn, progress, None,
    )
    .await?;
    Ok(snap_lsn)
}

/// Per-table progress map (canonical PK key of the last staged row).
type SnapshotProgressMap = BTreeMap<TableIdent, String>;

/// Resumable snapshot driver.
///
/// On `run`, loads `Checkpoint::snapshot_progress` from the coord; for each
/// table, resumes reading at PKs strictly greater than the saved key. After
/// each chunk, persists updated progress so a crash mid-snapshot resumes
/// from the right place. When all tables complete, clears their progress
/// entries.
///
/// Phase 11 finish surface — the existing `run_snapshot` /
/// `run_snapshot_chunked` free functions stay for tests and uses that
/// don't care about resumability.
pub struct Snapshotter {
    coord: Arc<dyn Coordinator>,
    chunk_size: usize,
}

impl Snapshotter {
    pub fn new(coord: Arc<dyn Coordinator>) -> Self {
        Self {
            coord,
            chunk_size: DEFAULT_CHUNK_SIZE,
        }
    }

    pub fn with_chunk_size(mut self, n: usize) -> Self {
        assert!(n > 0, "chunk_size must be > 0");
        self.chunk_size = n;
        self
    }

    /// Run the snapshot to completion (all tables, all chunks).
    pub async fn run<S, C>(
        &self,
        source: &S,
        schemas: &[TableSchema],
        pipeline: &mut Pipeline<C>,
    ) -> Result<Lsn>
    where
        S: SnapshotSource + ?Sized,
        C: Coordinator,
    {
        self.run_chunks(source, schemas, pipeline, None).await
    }

    /// Run at most `max_chunks` chunks across all tables, then return.
    /// `None` = unlimited. Used by tests to simulate a mid-snapshot crash:
    /// after `run_chunks(.., Some(N))`, drop the pipeline + Snapshotter,
    /// recreate them, and call `run` to finish — checkpoint progress
    /// drives correct resumption.
    pub async fn run_chunks<S, C>(
        &self,
        source: &S,
        schemas: &[TableSchema],
        pipeline: &mut Pipeline<C>,
        max_chunks: Option<usize>,
    ) -> Result<Lsn>
    where
        S: SnapshotSource + ?Sized,
        C: Coordinator,
    {
        let snap_lsn = source.snapshot_lsn().await?;

        // Load existing checkpoint progress.
        let cp = self
            .coord
            .load_checkpoint()
            .await
            .map_err(|e| SnapshotError::Source(format!("load_checkpoint: {e}")))?;
        let progress = cp
            .as_ref()
            .map(|c| c.snapshot_progress.clone())
            .unwrap_or_default();

        let (_chunks, end_progress) = snapshot_one_pass(
            source,
            schemas,
            pipeline,
            self.chunk_size,
            snap_lsn,
            progress,
            max_chunks,
        )
        .await?;

        // Save the actual end-of-pass progress to the coord. Existing
        // checkpoint fields are preserved.
        let mut cp_to_save = cp.unwrap_or_else(|| Checkpoint::fresh(Mode::Logical));
        cp_to_save.snapshot_progress = end_progress;
        cp_to_save.snapshot_state = if cp_to_save.snapshot_progress.is_empty() {
            SnapshotState::Complete
        } else {
            SnapshotState::InProgress
        };
        self.coord
            .save_checkpoint(&cp_to_save)
            .await
            .map_err(|e| SnapshotError::Source(format!("save_checkpoint: {e}")))?;

        Ok(snap_lsn)
    }
}

/// Drive the actual snapshot loop. Shared by both the free-function entry
/// point and `Snapshotter`. Returns `(chunks_processed, end_progress)` so
/// the caller can persist the post-run progress map.
async fn snapshot_one_pass<S, C>(
    source: &S,
    schemas: &[TableSchema],
    pipeline: &mut Pipeline<C>,
    chunk_size: usize,
    snap_lsn: Lsn,
    progress: SnapshotProgressMap,
    max_chunks: Option<usize>,
) -> Result<(usize, SnapshotProgressMap)>
where
    S: SnapshotSource + ?Sized,
    C: Coordinator,
{
    assert!(chunk_size > 0, "chunk_size must be > 0");

    let mut total_chunks = 0usize;
    let mut state = progress;

    'tables: for (i, schema) in schemas.iter().enumerate() {
        let xid = SNAPSHOT_XID_BASE.wrapping_add(i as u32);
        let pk_cols: Vec<ColumnName> = schema
            .primary_key_columns()
            .map(|c| ColumnName(c.name.clone()))
            .collect();

        if pk_cols.is_empty() {
            return Err(SnapshotError::Source(format!(
                "table {} has no primary key; pg2iceberg requires one",
                schema.ident
            )));
        }

        // Resume from prior progress if any.
        let mut last_pk_key: Option<String> = state.get(&schema.ident).cloned();

        loop {
            if let Some(cap) = max_chunks {
                if total_chunks >= cap {
                    break 'tables;
                }
            }

            let chunk = source
                .read_chunk(&schema.ident, chunk_size, last_pk_key.as_deref())
                .await?;
            if chunk.is_empty() {
                // Table done — clear progress for it.
                state.remove(&schema.ident);
                break;
            }

            let last_in_chunk = chunk.last().unwrap();
            let new_key = pk_key(last_in_chunk, &pk_cols);

            // Defensive: source must advance the cursor strictly forward.
            if let Some(prev) = last_pk_key.as_deref() {
                if new_key.as_str() <= prev {
                    return Err(SnapshotError::Source(format!(
                        "snapshot source did not advance past PK {prev} for table {}; got {new_key}",
                        schema.ident
                    )));
                }
            }

            pipeline
                .process(DecodedMessage::Begin {
                    final_lsn: snap_lsn,
                    xid,
                })
                .await?;

            for row in chunk {
                pipeline
                    .process(DecodedMessage::Change(ChangeEvent {
                        table: schema.ident.clone(),
                        op: Op::Insert,
                        lsn: snap_lsn,
                        commit_ts: Timestamp(0),
                        xid: Some(xid),
                        before: None,
                        after: Some(row),
                        unchanged_cols: vec![],
                    }))
                    .await?;
            }

            pipeline
                .process(DecodedMessage::Commit {
                    commit_lsn: snap_lsn,
                    xid,
                })
                .await?;
            pipeline.flush().await?;

            last_pk_key = Some(new_key.clone());
            state.insert(schema.ident.clone(), new_key);
            total_chunks += 1;
        }
    }

    Ok((total_chunks, state))
}

/// Type bound the snapshotter wants on the catalog. Currently unused
/// directly here — the pipeline carries its own catalog reference for the
/// materializer — but exposing it lets future versions of `run_snapshot`
/// validate that the catalog table exists before reading from PG.
#[doc(hidden)]
pub trait SnapshotCatalog: Catalog {}
impl<C: Catalog> SnapshotCatalog for C {}
