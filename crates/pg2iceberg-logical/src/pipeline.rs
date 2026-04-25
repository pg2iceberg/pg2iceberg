//! Logical-replication pipeline orchestrator.
//!
//! `Pipeline::process(msg)` advances the sink for a single decoded message;
//! `flush()` drains, uploads, claims, and advances `flushedLSN` — the last
//! step gated by [`CoordCommitReceipt`].

use crate::sink::{FlushOutput, Sink, SinkError, TableChunk};
use async_trait::async_trait;
use bytes::Bytes;
use pg2iceberg_coord::{CommitBatch, CoordCommitReceipt, CoordError, Coordinator, OffsetClaim};
use pg2iceberg_core::Lsn;
use pg2iceberg_pg::DecodedMessage;
use pg2iceberg_stream::{BlobStore, StreamError};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use thiserror::Error;

#[derive(Clone, Debug, Error)]
pub enum PipelineError {
    #[error("sink: {0}")]
    Sink(#[from] SinkError),
    #[error("blob: {0}")]
    Blob(#[from] StreamError),
    #[error("coord: {0}")]
    Coord(#[from] CoordError),
}

pub type Result<T> = std::result::Result<T, PipelineError>;

/// Names a staged Parquet object in the blob store. Real production uses
/// `IdGen::new_uuid` for unique suffixes; the sim uses a deterministic counter
/// so DST runs are reproducible.
#[async_trait]
pub trait BlobNamer: Send + Sync {
    async fn next_blob_path(&self, table: &str) -> String;
}

/// Deterministic blob namer for sim/test paths. Uses a monotonic counter.
#[derive(Default)]
pub struct CounterBlobNamer {
    counter: AtomicU64,
    base: String,
}

impl CounterBlobNamer {
    pub fn new(base: impl Into<String>) -> Self {
        Self {
            counter: AtomicU64::new(0),
            base: base.into(),
        }
    }
}

#[async_trait]
impl BlobNamer for CounterBlobNamer {
    async fn next_blob_path(&self, table: &str) -> String {
        let n = self.counter.fetch_add(1, Ordering::SeqCst);
        format!("{}/{}/{:010}.parquet", self.base, table, n)
    }
}

/// The pipeline. Generic over the coord impl so the type system carries the
/// `Coordinator` choice all the way through.
pub struct Pipeline<C: Coordinator> {
    sink: Sink,
    coord: Arc<C>,
    blob_store: Arc<dyn BlobStore>,
    namer: Arc<dyn BlobNamer>,
    flushed_lsn: AtomicU64,
}

impl<C: Coordinator> Pipeline<C> {
    pub fn new(
        coord: Arc<C>,
        blob_store: Arc<dyn BlobStore>,
        namer: Arc<dyn BlobNamer>,
        flush_threshold: usize,
    ) -> Self {
        Self {
            sink: Sink::new(flush_threshold),
            coord,
            blob_store,
            namer,
            flushed_lsn: AtomicU64::new(0),
        }
    }

    /// Highest LSN whose underlying batch has committed in the coordinator.
    /// This is what the pipeline hands to its `send_standby` ticker (Phase 5
    /// keeps that out of band — the sim test calls `send_standby` directly).
    pub fn flushed_lsn(&self) -> Lsn {
        Lsn(self.flushed_lsn.load(Ordering::SeqCst))
    }

    pub async fn process(&mut self, msg: DecodedMessage) -> Result<()> {
        match msg {
            DecodedMessage::Begin { xid, .. } => self.sink.begin_tx(xid),
            DecodedMessage::Commit { xid, commit_lsn } => self.sink.commit_tx(xid, commit_lsn),
            DecodedMessage::Change(evt) => self.sink.record_change(evt)?,
            DecodedMessage::Relation { .. } => {
                // Schema evolution lands in Phase 7.
            }
            DecodedMessage::Keepalive { .. } => {
                // The standby ticker reads `flushed_lsn()` directly.
            }
        }
        Ok(())
    }

    /// Drain all committed-but-unflushed transactions: encode → upload →
    /// `claim_offsets` → advance `flushedLSN` via the receipt. No-op if
    /// nothing is ready.
    pub async fn flush(&mut self) -> Result<Option<Lsn>> {
        let Some(FlushOutput {
            chunks,
            flushable_lsn,
        }) = self.sink.flush()?
        else {
            return Ok(None);
        };
        if chunks.is_empty() {
            // Possible if every committed tx was empty. Still advance the LSN
            // (we know all tx commits up to this point are durable in PG, but
            // there's nothing for the coord to register).
            // For Phase 5 we still go through claim_offsets with an empty
            // batch so the receipt is the single LSN-advance code path.
        }

        let mut claims = Vec::with_capacity(chunks.len());
        for TableChunk { table, chunk } in chunks {
            let path = self.namer.next_blob_path(&table.name).await;
            let byte_size = chunk.bytes.len() as u64;
            self.blob_store
                .put(&path, Bytes::clone(&chunk.bytes))
                .await?;
            claims.push(OffsetClaim {
                table,
                record_count: chunk.record_count,
                byte_size,
                s3_path: path,
            });
        }

        let batch = CommitBatch {
            claims,
            flushable_lsn,
        };
        let receipt = self.coord.claim_offsets(&batch).await?;
        self.advance_flushed_lsn(receipt);
        Ok(Some(flushable_lsn))
    }

    /// **Receipt-gated LSN advance.** Consumes a [`CoordCommitReceipt`] —
    /// since the receipt cannot be constructed outside `pg2iceberg-coord`'s
    /// internals, no caller can advance the slot LSN without the coord
    /// having committed.
    fn advance_flushed_lsn(&self, receipt: CoordCommitReceipt) {
        // The receipt carries the LSN that the pipeline supplied via
        // `CommitBatch::flushable_lsn`. After the coord commit, that LSN is
        // safely durable downstream of the slot.
        self.flushed_lsn
            .store(receipt.flushable_lsn.0, Ordering::SeqCst);
    }
}
