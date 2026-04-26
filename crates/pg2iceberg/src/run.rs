//! `pg2iceberg run`: assemble all four prod surfaces and drive the
//! pipeline.
//!
//! Loop shape (mirrors `pg2iceberg-logical::runner` doctest):
//!
//! 1. `select!` between `stream.recv()`, the ticker timeout, and
//!    SIGINT.
//! 2. Each `recv` yields a `DecodedMessage` we feed to
//!    `Pipeline::process`.
//! 3. When the ticker fires, we run the due handlers in stable order:
//!    Flush → Standby → Materialize → Watcher (Watcher is a no-op
//!    today; the watcher crate isn't wired in).
//! 4. SIGINT calls `Pipeline::shutdown` and exits cleanly.

use crate::config::{BlobConfig, Config, IcebergConfig};
use crate::realio::{RealClock, RealIdGen};
use anyhow::{Context, Result};
use async_trait::async_trait;
use pg2iceberg_coord::{
    prod::{connect_with as coord_connect_with, PostgresCoordinator, TlsMode as CoordTls},
    schema::CoordSchema,
    Coordinator,
};
use pg2iceberg_core::{Clock, IdGen, Lsn};
use pg2iceberg_iceberg::prod::IcebergRustCatalog;
use pg2iceberg_logical::{
    materializer::CounterMaterializerNamer, pipeline::BlobNamer, Handler, Materializer, Pipeline,
    Schedule, Ticker,
};
use pg2iceberg_pg::{
    prod::{PgClientImpl, TlsMode as PgTls},
    PgClient,
};
use pg2iceberg_stream::{prod::ObjectStoreBlobStore, BlobStore};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::signal::unix::{signal, SignalKind};

/// Production blob namer. Uses an [`IdGen`]-supplied UUID per blob so
/// uploaded paths never collide across processes.
struct UuidBlobNamer<I: IdGen> {
    id_gen: Arc<I>,
    base: String,
}

impl<I: IdGen> UuidBlobNamer<I> {
    fn new(id_gen: Arc<I>, base: impl Into<String>) -> Self {
        Self {
            id_gen,
            base: base.into(),
        }
    }
}

#[async_trait]
impl<I: IdGen + 'static> BlobNamer for UuidBlobNamer<I> {
    async fn next_blob_path(&self, table: &str) -> String {
        let bytes = self.id_gen.new_uuid();
        let hex = bytes.iter().map(|b| format!("{b:02x}")).collect::<String>();
        format!(
            "{}/{}/{}.parquet",
            self.base.trim_end_matches('/'),
            table,
            hex
        )
    }
}

pub async fn run(cfg: Config) -> Result<()> {
    // Build the Iceberg catalog and dispatch to the generic `run_inner`.
    // The Materializer is generic over the concrete iceberg::Catalog
    // type, so each variant compiles a separate specialization. If we
    // grow more variants and that bloats the binary, refactor to a
    // dyn-Catalog wrapper at this seam.
    let blob = build_blob(&cfg).context("build blob store")?;
    match cfg.iceberg.clone() {
        IcebergConfig::Memory { warehouse } => {
            let inner = build_memory_catalog(&warehouse).await?;
            run_inner(cfg, IcebergRustCatalog::new(Arc::new(inner)), blob).await
        }
        IcebergConfig::Rest {
            uri,
            warehouse,
            token,
            props,
        } => {
            let inner = build_rest_catalog(&uri, &warehouse, token.as_deref(), &props).await?;
            run_inner(cfg, IcebergRustCatalog::new(Arc::new(inner)), blob).await
        }
    }
}

pub(crate) async fn build_memory_catalog(
    warehouse: &str,
) -> Result<iceberg::memory::MemoryCatalog> {
    use iceberg::CatalogBuilder;
    iceberg::memory::MemoryCatalogBuilder::default()
        .load(
            "pg2iceberg",
            HashMap::from([(
                iceberg::memory::MEMORY_CATALOG_WAREHOUSE.to_string(),
                warehouse.to_string(),
            )]),
        )
        .await
        .context("MemoryCatalog load")
}

pub(crate) async fn build_rest_catalog(
    uri: &str,
    warehouse: &str,
    token: Option<&str>,
    extra_props: &std::collections::BTreeMap<String, String>,
) -> Result<iceberg_catalog_rest::RestCatalog> {
    use iceberg::CatalogBuilder;
    use iceberg_catalog_rest::{
        RestCatalogBuilder, REST_CATALOG_PROP_URI, REST_CATALOG_PROP_WAREHOUSE,
    };
    let mut props: HashMap<String, String> = HashMap::new();
    props.insert(REST_CATALOG_PROP_URI.to_string(), uri.to_string());
    props.insert(
        REST_CATALOG_PROP_WAREHOUSE.to_string(),
        warehouse.to_string(),
    );
    if let Some(t) = token {
        // The REST spec's `Authorization: Bearer` flow is conveyed
        // through this prop.
        props.insert("token".to_string(), t.to_string());
    }
    for (k, v) in extra_props {
        props.insert(k.clone(), v.clone());
    }
    RestCatalogBuilder::default()
        .load("pg2iceberg", props)
        .await
        .context("RestCatalog load")
}

fn build_blob(cfg: &Config) -> Result<Arc<dyn BlobStore>> {
    match &cfg.blob {
        BlobConfig::Memory => Ok(Arc::new(ObjectStoreBlobStore::new(Arc::new(
            object_store::memory::InMemory::new(),
        )))),
        BlobConfig::S3 {
            bucket,
            region,
            prefix,
            endpoint,
            access_key_id,
            secret_access_key,
            session_token,
        } => {
            let mut builder = object_store::aws::AmazonS3Builder::new()
                .with_bucket_name(bucket)
                .with_region(region);
            if let Some(ep) = endpoint {
                builder = builder.with_endpoint(ep);
                // Path-style addressing matters for non-AWS S3
                // (MinIO, R2 default to virtual-hosted, but
                // operators often use path-style). Default off; if
                // needed, surface as a config flag.
            }
            if let (Some(ak), Some(sk)) = (access_key_id, secret_access_key) {
                builder = builder.with_access_key_id(ak).with_secret_access_key(sk);
                if let Some(st) = session_token {
                    builder = builder.with_token(st);
                }
            }
            let inner = builder.build().context("AmazonS3Builder build")?;
            // If `prefix` is set, wrap in a PrefixStore so all
            // staged paths land under that prefix.
            let store: Arc<dyn object_store::ObjectStore> = match prefix.as_deref() {
                Some(p) if !p.is_empty() => Arc::new(object_store::prefix::PrefixStore::new(
                    inner,
                    p.trim_matches('/').to_string(),
                )),
                _ => Arc::new(inner),
            };
            Ok(Arc::new(ObjectStoreBlobStore::new(store)))
        }
    }
}

async fn run_inner<C>(
    cfg: Config,
    catalog: IcebergRustCatalog<C>,
    blob: Arc<dyn BlobStore>,
) -> Result<()>
where
    C: iceberg::Catalog + Send + Sync + 'static,
{
    let clock = Arc::new(RealClock);
    let id_gen = match cfg.runtime.worker_id.as_deref() {
        Some(w) => Arc::new(RealIdGen::with_worker_id(w)),
        None => Arc::new(RealIdGen::new()),
    };
    let worker_id = id_gen.worker_id();
    tracing::info!(worker = %worker_id.0, "starting pg2iceberg");

    // ── coord ──────────────────────────────────────────────────────────
    let coord_tls = CoordTls::parse(&cfg.coord.tls)
        .map_err(|e| anyhow::anyhow!("parse coord tls mode: {e}"))?;
    let coord_conn = coord_connect_with(&cfg.coord.conn, coord_tls)
        .await
        .context("coord connect")?;
    let coord_schema = CoordSchema::sanitize(&cfg.coord.schema);
    let coord = Arc::new(PostgresCoordinator::new(coord_conn, coord_schema));
    coord.migrate().await.context("coord migrate")?;

    let catalog = Arc::new(catalog);

    // ── pg source ──────────────────────────────────────────────────────
    let pg_tls =
        PgTls::parse(&cfg.pg.tls).map_err(|e| anyhow::anyhow!("parse pg tls mode: {e}"))?;
    let pg = PgClientImpl::connect_with(&cfg.pg.conn, pg_tls)
        .await
        .context("PG connect")?;
    let table_idents: Vec<_> = cfg
        .tables
        .iter()
        .map(|t| t.to_table_schema().map(|s| s.ident))
        .collect::<Result<Vec<_>, _>>()?;
    if !pg
        .slot_exists(&cfg.pg.slot)
        .await
        .context("slot exists check")?
    {
        // First run — create the publication and the slot. Both are
        // idempotent in the sense that we check for existence before
        // creation, so re-running this once shouldn't double-fail.
        if let Err(e) = pg
            .create_publication(&cfg.pg.publication, &table_idents)
            .await
        {
            tracing::warn!(error = %e, "create_publication failed; assuming it exists");
        }
        let cp = pg.create_slot(&cfg.pg.slot).await.context("create_slot")?;
        tracing::info!(slot = %cfg.pg.slot, ?cp, "replication slot created");
    } else {
        tracing::info!(slot = %cfg.pg.slot, "replication slot exists, resuming");
    }

    // PG resumes from the slot's `confirmed_flush_lsn` server-side when
    // we pass `0/0` here. No need to track our own start LSN.
    let mut stream = pg
        .start_replication(&cfg.pg.slot, Lsn(0), &cfg.pg.publication)
        .await
        .context("START_REPLICATION")?;

    // ── pipeline + materializer ────────────────────────────────────────
    let blob_namer = Arc::new(UuidBlobNamer::new(id_gen.clone(), "staged"));
    let mut pipeline = Pipeline::new(
        coord.clone(),
        blob.clone(),
        blob_namer,
        cfg.runtime.flush_threshold,
    );

    let mat_namer = Arc::new(CounterMaterializerNamer::new("materialized"));
    let mut materializer: Materializer<IcebergRustCatalog<C>> = Materializer::new(
        coord.clone() as Arc<dyn Coordinator>,
        blob.clone(),
        catalog.clone(),
        mat_namer,
        &cfg.coord.group,
        cfg.runtime.cycle_limit,
    );

    for t in &cfg.tables {
        let schema = t.to_table_schema()?;
        materializer
            .register_table(schema)
            .await
            .context("register table")?;
    }
    tracing::info!(count = cfg.tables.len(), "tables registered");

    // Initial consumer heartbeat. The ticker handles renewal alongside
    // every standby tick.
    coord
        .register_consumer(&cfg.coord.group, &worker_id, Duration::from_secs(60))
        .await
        .context("register_consumer")?;

    // ── main loop ──────────────────────────────────────────────────────
    let mut ticker = Ticker::new(clock.now(), Schedule::default());
    let mut sigint = signal(SignalKind::interrupt()).context("install SIGINT handler")?;
    let mut sigterm = signal(SignalKind::terminate()).context("install SIGTERM handler")?;
    let last_flush = Arc::new(AtomicU64::new(clock.now().0 as u64));
    let _ = &last_flush;

    tracing::info!("entering replication loop; SIGINT or SIGTERM to stop");

    loop {
        // Compute the time until the next ticker handler fires. We use
        // it as a cap on the recv-or-tick select.
        let now = clock.now();
        let next = ticker.next_due();
        let until_next = (next.0.saturating_sub(now.0)).max(0) as u64;
        let tick_sleep = Duration::from_micros(until_next.min(1_000_000)); // cap at 1s

        tokio::select! {
            biased;
            _ = sigint.recv() => {
                tracing::info!("SIGINT received, shutting down");
                break;
            }
            _ = sigterm.recv() => {
                tracing::info!("SIGTERM received, shutting down");
                break;
            }
            res = stream.recv() => {
                let msg = res.context("replication recv")?;
                pipeline.process(msg).await.context("pipeline.process")?;
            }
            _ = tokio::time::sleep(tick_sleep) => {
                // Fall through to fire any due handlers below.
            }
        }

        for h in ticker.fire_due(clock.now()) {
            match h {
                Handler::Flush => {
                    if let Err(e) = pipeline.flush().await {
                        tracing::error!(error = %e, "pipeline.flush failed");
                        return Err(anyhow::anyhow!(e));
                    }
                }
                Handler::Standby => {
                    let lsn = pipeline.flushed_lsn();
                    if let Err(e) = stream.send_standby(lsn, lsn).await {
                        tracing::warn!(error = %e, "send_standby failed");
                    }
                }
                Handler::Materialize => {
                    if let Err(e) = materializer.cycle().await {
                        tracing::error!(error = %e, "materializer.cycle failed");
                        return Err(anyhow::anyhow!(e));
                    }
                }
                Handler::Watcher => {
                    // The invariant watcher (`pg2iceberg-validate::watcher`)
                    // isn't wired into the binary yet; that's a follow-on.
                }
            }
        }

        // Periodically renew the consumer heartbeat. We piggy-back on
        // ticker firings rather than running a third task.
        let _heartbeat = coord
            .register_consumer(&cfg.coord.group, &worker_id, Duration::from_secs(60))
            .await;
        // Fence usage of `last_flush` so the AtomicU64 isn't dead code.
        last_flush.store(clock.now().0 as u64, Ordering::Relaxed);
    }

    // ── shutdown ───────────────────────────────────────────────────────
    tracing::info!("draining pipeline before exit");
    if let Err(e) = pipeline.flush().await {
        tracing::warn!(error = %e, "final flush failed");
    }
    let final_lsn = pipeline.flushed_lsn();
    if let Err(e) = stream.send_standby(final_lsn, final_lsn).await {
        tracing::warn!(error = %e, "final send_standby failed");
    }
    let _ = coord
        .unregister_consumer(&cfg.coord.group, &worker_id)
        .await;
    tracing::info!(?final_lsn, "exited cleanly");
    Ok(())
}
