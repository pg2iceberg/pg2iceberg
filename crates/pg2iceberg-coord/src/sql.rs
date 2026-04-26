//! SQL statements used by the production `Coordinator` impl.
//!
//! These are kept here so they're testable as strings (snapshot tests, future
//! sqllogictest, etc.) and shareable between the prod impl (Phase 3 PG) and
//! any tooling that needs to introspect the wire SQL.
//!
//! All statements take the schema as input rather than embedding it. Schema
//! names are pre-sanitized via [`crate::schema::CoordSchema`].

use crate::schema::CoordSchema;

/// `CREATE SCHEMA IF NOT EXISTS …`. Idempotent.
pub fn create_schema(schema: &CoordSchema) -> String {
    format!("CREATE SCHEMA IF NOT EXISTS {}", schema)
}

/// DDL for the six coord tables. Each statement is idempotent
/// (`CREATE TABLE IF NOT EXISTS`). Mirrors `stream/coordinator_pg.go:73-104`
/// plus a single-row `checkpoints` table for [`crate::Coordinator::save_checkpoint`].
pub fn migrate(schema: &CoordSchema) -> Vec<String> {
    vec![
        format!(
            "CREATE TABLE IF NOT EXISTS {} (
                table_name  TEXT PRIMARY KEY,
                next_offset BIGINT NOT NULL DEFAULT 0
            )",
            schema.qualify("log_seq")
        ),
        format!(
            "CREATE TABLE IF NOT EXISTS {} (
                table_name   TEXT   NOT NULL,
                end_offset   BIGINT NOT NULL,
                start_offset BIGINT NOT NULL,
                s3_path      TEXT   NOT NULL,
                record_count INT    NOT NULL,
                byte_size    BIGINT NOT NULL,
                created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
                PRIMARY KEY (table_name, end_offset)
            )",
            schema.qualify("log_index")
        ),
        format!(
            "CREATE TABLE IF NOT EXISTS {} (
                group_name     TEXT NOT NULL DEFAULT 'default',
                table_name     TEXT NOT NULL,
                last_offset    BIGINT NOT NULL DEFAULT -1,
                last_committed TIMESTAMPTZ NOT NULL DEFAULT now(),
                PRIMARY KEY (group_name, table_name)
            )",
            schema.qualify("mat_cursor")
        ),
        format!(
            "CREATE TABLE IF NOT EXISTS {} (
                table_name TEXT PRIMARY KEY,
                worker_id  TEXT NOT NULL,
                expires_at TIMESTAMPTZ NOT NULL
            )",
            schema.qualify("lock")
        ),
        format!(
            "CREATE TABLE IF NOT EXISTS {} (
                group_name TEXT NOT NULL,
                worker_id  TEXT NOT NULL,
                expires_at TIMESTAMPTZ NOT NULL,
                PRIMARY KEY (group_name, worker_id)
            )",
            schema.qualify("consumer")
        ),
        // Single-row table holding the serialized `Checkpoint` blob.
        // `id` is hardcoded to 1 so the upsert is unambiguous and
        // multi-writer races resolve to last-write-wins.
        format!(
            "CREATE TABLE IF NOT EXISTS {} (
                id         INT  PRIMARY KEY,
                payload    JSONB NOT NULL,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
            )",
            schema.qualify("checkpoints")
        ),
    ]
}

/// `INSERT INTO log_seq … ON CONFLICT DO NOTHING`. Idempotent ensure-row.
pub fn ensure_log_seq_row(schema: &CoordSchema) -> String {
    format!(
        "INSERT INTO {} (table_name, next_offset) VALUES ($1, 0) \
         ON CONFLICT DO NOTHING",
        schema.qualify("log_seq")
    )
}

/// `UPDATE log_seq SET next_offset = next_offset + $2 WHERE table_name = $1
/// RETURNING next_offset`. Atomic per-row claim.
pub fn claim_log_seq(schema: &CoordSchema) -> String {
    format!(
        "UPDATE {} SET next_offset = next_offset + $2 \
         WHERE table_name = $1 RETURNING next_offset",
        schema.qualify("log_seq")
    )
}

/// Insert into `log_index`. Six positional args:
/// `(table_name, end_offset, start_offset, s3_path, record_count, byte_size)`.
pub fn insert_log_index(schema: &CoordSchema) -> String {
    format!(
        "INSERT INTO {} (table_name, end_offset, start_offset, s3_path, record_count, byte_size) \
         VALUES ($1, $2, $3, $4, $5, $6)",
        schema.qualify("log_index")
    )
}

pub fn read_log(schema: &CoordSchema) -> String {
    format!(
        "SELECT table_name, start_offset, end_offset, s3_path, record_count, byte_size \
         FROM {} WHERE table_name = $1 AND end_offset > $2 ORDER BY end_offset ASC LIMIT $3",
        schema.qualify("log_index")
    )
}

pub fn truncate_log(schema: &CoordSchema) -> String {
    format!(
        "DELETE FROM {} WHERE table_name = $1 AND end_offset <= $2 RETURNING s3_path",
        schema.qualify("log_index")
    )
}

pub fn ensure_cursor(schema: &CoordSchema) -> String {
    format!(
        "INSERT INTO {} (group_name, table_name, last_offset) VALUES ($1, $2, -1) \
         ON CONFLICT DO NOTHING",
        schema.qualify("mat_cursor")
    )
}

pub fn get_cursor(schema: &CoordSchema) -> String {
    format!(
        "SELECT last_offset FROM {} WHERE group_name = $1 AND table_name = $2",
        schema.qualify("mat_cursor")
    )
}

pub fn set_cursor(schema: &CoordSchema) -> String {
    format!(
        "UPDATE {} SET last_offset = $3, last_committed = now() \
         WHERE group_name = $1 AND table_name = $2",
        schema.qualify("mat_cursor")
    )
}

pub fn register_consumer(schema: &CoordSchema) -> String {
    format!(
        "INSERT INTO {} (group_name, worker_id, expires_at) \
         VALUES ($1, $2, now() + $3::interval) \
         ON CONFLICT (group_name, worker_id) DO UPDATE SET expires_at = now() + $3::interval",
        schema.qualify("consumer")
    )
}

pub fn unregister_consumer(schema: &CoordSchema) -> String {
    format!(
        "DELETE FROM {} WHERE group_name = $1 AND worker_id = $2",
        schema.qualify("consumer")
    )
}

pub fn expire_consumers(schema: &CoordSchema) -> String {
    format!(
        "DELETE FROM {} WHERE expires_at < now()",
        schema.qualify("consumer")
    )
}

pub fn active_consumers(schema: &CoordSchema) -> String {
    format!(
        "SELECT worker_id FROM {} WHERE group_name = $1 ORDER BY worker_id",
        schema.qualify("consumer")
    )
}

pub fn expire_locks(schema: &CoordSchema) -> String {
    format!(
        "DELETE FROM {} WHERE table_name = $1 AND expires_at < now()",
        schema.qualify("lock")
    )
}

pub fn try_lock(schema: &CoordSchema) -> String {
    format!(
        "INSERT INTO {} (table_name, worker_id, expires_at) \
         VALUES ($1, $2, now() + $3::interval) ON CONFLICT DO NOTHING",
        schema.qualify("lock")
    )
}

pub fn renew_lock(schema: &CoordSchema) -> String {
    format!(
        "UPDATE {} SET expires_at = now() + $3::interval \
         WHERE table_name = $1 AND worker_id = $2",
        schema.qualify("lock")
    )
}

pub fn release_lock(schema: &CoordSchema) -> String {
    format!(
        "DELETE FROM {} WHERE table_name = $1 AND worker_id = $2",
        schema.qualify("lock")
    )
}

/// Upsert the single-row checkpoint blob (`id = 1`).
pub fn upsert_checkpoint(schema: &CoordSchema) -> String {
    format!(
        "INSERT INTO {} (id, payload, updated_at) VALUES (1, $1, now()) \
         ON CONFLICT (id) DO UPDATE SET payload = $1, updated_at = now()",
        schema.qualify("checkpoints")
    )
}

/// Read the single-row checkpoint blob. Returns 0 or 1 row.
pub fn load_checkpoint(schema: &CoordSchema) -> String {
    format!(
        "SELECT payload FROM {} WHERE id = 1",
        schema.qualify("checkpoints")
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn migrate_returns_six_idempotent_statements() {
        let s = CoordSchema::default_name();
        let stmts = migrate(&s);
        assert_eq!(stmts.len(), 6);
        for stmt in &stmts {
            assert!(
                stmt.contains("CREATE TABLE IF NOT EXISTS"),
                "stmt is not idempotent: {stmt}"
            );
            assert!(stmt.contains("_pg2iceberg."));
        }
        // The new `checkpoints` table uses JSONB for the payload.
        let last = stmts.last().unwrap();
        assert!(last.contains("checkpoints"));
        assert!(last.contains("JSONB"));
    }

    #[test]
    fn upsert_checkpoint_uses_id_1_pk() {
        let s = CoordSchema::default_name();
        let q = upsert_checkpoint(&s);
        assert!(q.contains("VALUES (1,"));
        assert!(q.contains("ON CONFLICT (id) DO UPDATE"));
    }

    #[test]
    fn claim_log_seq_uses_returning_clause() {
        let s = CoordSchema::default_name();
        let q = claim_log_seq(&s);
        assert!(q.contains("RETURNING next_offset"));
        assert!(q.contains("WHERE table_name = $1"));
    }

    #[test]
    fn schema_name_propagates_into_qualified_identifiers() {
        let s = CoordSchema::sanitize("Tenant_42");
        let q = read_log(&s);
        assert!(q.contains("tenant_42.log_index"));
    }
}
