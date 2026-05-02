//! Production [`Coordinator`](crate::Coordinator) impl: a Postgres-backed
//! coordinator that runs against the source PG (or a sidecar PG) using
//! `tokio-postgres` in regular (non-replication) mode.
//!
//! This is the durability boundary in production. Every advance of
//! `flushedLSN` rides on a `Coordinator::claim_offsets` PG transaction
//! that mints a [`crate::CoordCommitReceipt`]; if the PG commit fails,
//! the receipt is never minted and the slot can't move.
//!
//! The replication-mode prod client lives in `pg2iceberg-pg/src/prod/`.
//! That's a **separate connection** from the coord one: replication
//! mode supports a different command subset than regular SQL mode, so
//! sharing one connection would restrict the SQL surface
//! unnecessarily.

pub mod connect;
pub mod coord;

pub use connect::{connect, connect_with, PgConn, TlsMode};
pub use coord::PostgresCoordinator;
