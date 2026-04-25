use crate::lsn::Lsn;
use crate::schema::TableIdent;
use crate::value::PgValue;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Copy, Clone, Eq, PartialEq, Debug, Serialize, Deserialize)]
pub enum Mode {
    Logical,
    Query,
}

#[derive(Copy, Clone, Eq, PartialEq, Debug, Serialize, Deserialize)]
pub enum SnapshotState {
    NotStarted,
    InProgress,
    Complete,
}

/// Persisted in `_pg2iceberg.checkpoints`. Mirrors `pipeline/checkpoint.go`.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Checkpoint {
    pub mode: Mode,
    pub flushed_lsn: Lsn,
    pub snapshot_state: SnapshotState,
    /// Per-table watermark for query mode. The value is the highest
    /// watermark column observed; `query` mode resumes from here on the
    /// next poll.
    pub query_watermarks: BTreeMap<TableIdent, PgValue>,
    /// Per-table snapshot progress. The value is the canonical PK key
    /// (JSON-encoded) of the last row staged through the pipeline. Used
    /// by [`Snapshotter`] to resume mid-snapshot after a crash without
    /// re-staging already-completed chunks. `Complete` snapshots clear
    /// their entry; resumable snapshots keep one until the table is done.
    pub snapshot_progress: BTreeMap<TableIdent, String>,
    /// Tables we've seen at least once; startup validation uses this to detect
    /// removed tables.
    pub tracked_tables: Vec<TableIdent>,
}

impl Checkpoint {
    pub fn fresh(mode: Mode) -> Self {
        Self {
            mode,
            flushed_lsn: Lsn::ZERO,
            snapshot_state: SnapshotState::NotStarted,
            query_watermarks: BTreeMap::new(),
            snapshot_progress: BTreeMap::new(),
            tracked_tables: Vec::new(),
        }
    }
}
