//! pg2iceberg CLI binary.
//!
//! Subcommands:
//!
//! - `connect-pg` — open a replication-mode connection to the source PG
//!   and report slots / publications. Connectivity smoke test for the
//!   PG prod path.
//! - `connect-iceberg` — open the Iceberg catalog from config and list
//!   namespaces. Connectivity smoke test for the Iceberg prod path.
//! - `migrate-coord` — run the coordinator's idempotent schema
//!   migration. First step of any greenfield deployment.
//! - `run` — assemble the full pipeline (PG client + coord + catalog +
//!   blob store) and run it until SIGINT.

mod config;
mod realio;
mod run;

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use config::Config;
use std::path::PathBuf;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Smoke-test the PG replication-mode connection.
    ConnectPg {
        #[arg(long)]
        config: PathBuf,
    },
    /// Smoke-test the Iceberg catalog connection.
    ConnectIceberg {
        #[arg(long)]
        config: PathBuf,
    },
    /// Run the coordinator's idempotent schema migration. Safe to run
    /// repeatedly — every statement is `CREATE … IF NOT EXISTS`.
    MigrateCoord {
        #[arg(long)]
        config: PathBuf,
    },
    /// Run the full pipeline.
    Run {
        #[arg(long)]
        config: PathBuf,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info,pg2iceberg=debug".into()),
        )
        .init();

    let cli = Cli::parse();
    match cli.command {
        Command::ConnectPg { config } => connect_pg(&Config::load_from(config)?).await,
        Command::ConnectIceberg { config } => connect_iceberg(&Config::load_from(config)?).await,
        Command::MigrateCoord { config } => migrate_coord(&Config::load_from(config)?).await,
        Command::Run { config } => run::run(Config::load_from(config)?).await,
    }
}

// ── connect-pg ──────────────────────────────────────────────────────────

async fn connect_pg(cfg: &Config) -> Result<()> {
    use pg2iceberg_pg::{
        prod::{PgClientImpl, TlsMode},
        PgClient,
    };
    let tls = TlsMode::parse(&cfg.pg.tls).context("parse pg tls mode")?;
    tracing::info!(slot = %cfg.pg.slot, publication = %cfg.pg.publication, ?tls, "connecting to source PG");
    let client = PgClientImpl::connect_with(&cfg.pg.conn, tls)
        .await
        .context("PG connect")?;
    let exists = client
        .slot_exists(&cfg.pg.slot)
        .await
        .context("slot lookup")?;
    if exists {
        let lsn = client
            .slot_restart_lsn(&cfg.pg.slot)
            .await
            .context("slot restart_lsn")?;
        tracing::info!(slot = %cfg.pg.slot, ?lsn, "slot exists");
    } else {
        tracing::info!(slot = %cfg.pg.slot, "slot does not exist; would be created on first run");
    }
    println!("OK: PG replication-mode connection established");
    Ok(())
}

// ── connect-iceberg ────────────────────────────────────────────────────

async fn connect_iceberg(cfg: &Config) -> Result<()> {
    use pg2iceberg_iceberg::prod::IcebergRustCatalog;
    use std::sync::Arc;

    match cfg.iceberg.clone() {
        config::IcebergConfig::Memory { warehouse } => {
            tracing::info!(%warehouse, "opening Iceberg memory catalog");
            let inner = run::build_memory_catalog(&warehouse).await?;
            let catalog = IcebergRustCatalog::new(Arc::new(inner));
            ensure_namespaces(&catalog, &cfg.tables).await?;
        }
        config::IcebergConfig::Rest {
            uri,
            warehouse,
            token,
            props,
        } => {
            tracing::info!(%uri, %warehouse, "opening Iceberg REST catalog");
            let inner = run::build_rest_catalog(&uri, &warehouse, token.as_deref(), &props).await?;
            let catalog = IcebergRustCatalog::new(Arc::new(inner));
            ensure_namespaces(&catalog, &cfg.tables).await?;
        }
    }
    println!("OK: Iceberg catalog connection established");
    Ok(())
}

async fn ensure_namespaces<C: iceberg::Catalog + Send + Sync + 'static>(
    catalog: &pg2iceberg_iceberg::prod::IcebergRustCatalog<C>,
    tables: &[config::TableConfig],
) -> Result<()> {
    use pg2iceberg_iceberg::Catalog as _;
    for t in tables {
        let schema = t.to_table_schema()?;
        catalog
            .ensure_namespace(&schema.ident.namespace)
            .await
            .with_context(|| format!("ensure namespace for {}", schema.ident))?;
        tracing::info!(ident = %schema.ident, "namespace ready");
    }
    Ok(())
}

// ── migrate-coord ──────────────────────────────────────────────────────

async fn migrate_coord(cfg: &Config) -> Result<()> {
    use pg2iceberg_coord::{
        prod::{connect_with, PostgresCoordinator, TlsMode},
        schema::CoordSchema,
    };
    let tls = TlsMode::parse(&cfg.coord.tls).context("parse coord tls mode")?;
    tracing::info!(schema = %cfg.coord.schema, ?tls, "connecting to coord PG");
    let conn = connect_with(&cfg.coord.conn, tls)
        .await
        .context("coord connect")?;
    let schema = CoordSchema::sanitize(&cfg.coord.schema);
    let coord = PostgresCoordinator::new(conn, schema);
    coord.migrate().await.context("coord migrate")?;
    println!("OK: coordinator schema migrated");
    Ok(())
}
