//! Connection helper for [`super::PostgresCoordinator`].
//!
//! Symmetric to `pg2iceberg-pg/src/prod/client.rs`'s `connect()` but
//! **without** logical-replication mode — the coordinator uses the
//! extended query protocol (parameterized queries, transactions) which
//! isn't available in replication-mode connections.
//!
//! TLS is intentionally not wired yet; managed Postgres rollouts will
//! need it.

use crate::CoordError;
use tokio::task::AbortHandle;
use tokio_postgres::{Client, NoTls};

pub struct PgConn {
    pub client: Client,
    pub abort: AbortHandle,
}

/// Open a regular-mode `tokio_postgres::Client` to `conn_str`. The
/// connection task is spawned on tokio and stored as an `AbortHandle`
/// so the caller can tear it down deterministically.
pub async fn connect(conn_str: &str) -> Result<PgConn, CoordError> {
    let (client, connection) = tokio_postgres::connect(conn_str, NoTls)
        .await
        .map_err(|e| CoordError::Pg(e.to_string()))?;
    let handle = tokio::spawn(async move {
        let _ = connection.await;
    });
    Ok(PgConn {
        client,
        abort: handle.abort_handle(),
    })
}
