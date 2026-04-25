//! In-memory PK ↔ file map for a single materialized table.
//!
//! Mirrors `iceberg/tablewriter.go:84-126`. The materializer maintains one
//! `FileIndex` per table and updates it after each commit:
//! - Newly-written data files contribute their PKs ([`add_file`]).
//! - Equality-deleted PKs get removed ([`remove_pks`]).
//!
//! Used for two correctness reasons:
//! 1. **TOAST resolution.** TOAST `unchanged_cols` placeholders need the prior
//!    column values, which live in some prior data file. The materializer
//!    asks the index for the file path, fetches it, and copies the unchanged
//!    columns in.
//! 2. **Re-insert promotion.** An `Insert` whose PK already lives in a prior
//!    data file must be downgraded to `Update` so the writer emits an
//!    equality delete; otherwise readers would see two rows for that PK.
//!
//! On materializer restart, the index is rebuilt by reading manifest entries
//! for the current snapshot. Phase 8 wires that path; Phase 7.5 only owns
//! the in-memory data structure.

use std::collections::{BTreeMap, BTreeSet};

#[derive(Default, Debug, Clone)]
pub struct FileIndex {
    /// pk_key → file path. Single source of truth for "which file contains
    /// this PK."
    pk_to_file: BTreeMap<String, String>,
    /// path → set of pk_keys it contains. Used to GC empty files when every
    /// PK in a file has been deleted.
    file_pks: BTreeMap<String, BTreeSet<String>>,
}

impl FileIndex {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register all PKs in a freshly written data file. Replaces any prior
    /// PK→file mapping (the new file is now authoritative for these PKs).
    pub fn add_file(&mut self, path: String, pk_keys: Vec<String>) {
        let mut set = BTreeSet::new();
        for pk in pk_keys {
            // If this PK was previously associated with another file, leave
            // the old `file_pks` entry alone — that file will GC when its
            // last live PK is removed.
            self.pk_to_file.insert(pk.clone(), path.clone());
            set.insert(pk);
        }
        self.file_pks.entry(path).or_default().extend(set);
    }

    pub fn lookup(&self, pk_key: &str) -> Option<&str> {
        self.pk_to_file.get(pk_key).map(String::as_str)
    }

    pub fn contains_pk(&self, pk_key: &str) -> bool {
        self.pk_to_file.contains_key(pk_key)
    }

    /// Mark these PKs as deleted. The PK→file mapping is cleared. The file
    /// path is also dropped from `file_pks` once all its PKs are gone.
    pub fn remove_pks(&mut self, pk_keys: &[String]) {
        for pk in pk_keys {
            if let Some(path) = self.pk_to_file.remove(pk) {
                if let Some(set) = self.file_pks.get_mut(&path) {
                    set.remove(pk);
                    if set.is_empty() {
                        self.file_pks.remove(&path);
                    }
                }
            }
        }
    }

    /// Returns the set of file paths that contain at least one of the given
    /// PKs. Used by the materializer to know which files to fetch for TOAST
    /// resolution.
    pub fn affected_files(&self, pk_keys: &[String]) -> BTreeSet<String> {
        let mut out = BTreeSet::new();
        for pk in pk_keys {
            if let Some(path) = self.pk_to_file.get(pk) {
                out.insert(path.clone());
            }
        }
        out
    }

    pub fn live_files(&self) -> Vec<&str> {
        self.file_pks.keys().map(String::as_str).collect()
    }

    pub fn live_pk_count(&self) -> usize {
        self.pk_to_file.len()
    }
}

/// Rebuild a `FileIndex` for `ident` from the catalog's snapshot history.
///
/// Used on materializer / query-pipeline restart so re-insert promotion
/// keeps working — without this, a freshly-booted process has an empty
/// FileIndex and a re-insert of a previously-materialized PK won't emit
/// the equality delete that's needed to void the prior data file row,
/// producing duplicate rows in MoR readers.
///
/// MoR semantics: an equality-delete file at snapshot `N` voids data file
/// rows whose PK matches at snapshots `< N`. So a PK is "live" iff it's
/// in some data file at snap `S` AND no equality-delete at snap `> S`
/// targets it. This walks the snapshots in order and tracks the latest
/// data file each live PK lives in.
pub async fn rebuild_from_catalog(
    catalog: &dyn pg2iceberg_iceberg_dyn::DynCatalog,
    blob_store: &dyn pg2iceberg_stream::BlobStore,
    ident: &pg2iceberg_core::TableIdent,
    schema: &pg2iceberg_core::TableSchema,
    pk_cols: &[pg2iceberg_core::ColumnName],
) -> std::result::Result<FileIndex, crate::verify::VerifyError> {
    use crate::reader::read_data_file;
    use crate::verify::VerifyError;

    let snapshots = catalog
        .snapshots(ident)
        .await
        .map_err(VerifyError::from_dyn)?;

    let pk_schema: Vec<pg2iceberg_core::ColumnSchema> = schema
        .columns
        .iter()
        .filter(|c| c.is_primary_key)
        .cloned()
        .collect();

    // Per-snapshot deleted-PK sets, ordered by snap id.
    let mut deletes_per_snap: Vec<(i64, BTreeSet<String>)> = Vec::with_capacity(snapshots.len());
    for snap in &snapshots {
        let mut snap_deleted = BTreeSet::new();
        for df in &snap.delete_files {
            let bytes = blob_store.get(&df.path).await.map_err(VerifyError::Blob)?;
            let rows = read_data_file(&bytes, &pk_schema).map_err(VerifyError::Decode)?;
            for row in rows {
                snap_deleted.insert(crate::fold::pk_key(&row, pk_cols));
            }
        }
        deletes_per_snap.push((snap.id, snap_deleted));
    }

    let mut fi = FileIndex::new();
    for snap in &snapshots {
        for df in &snap.data_files {
            let bytes = blob_store.get(&df.path).await.map_err(VerifyError::Blob)?;
            let rows = read_data_file(&bytes, &schema.columns).map_err(VerifyError::Decode)?;
            let mut live_in_file = Vec::new();
            for row in rows {
                let key = crate::fold::pk_key(&row, pk_cols);
                let deleted_later = deletes_per_snap
                    .iter()
                    .any(|(sid, set)| *sid > snap.id && set.contains(&key));
                if !deleted_later {
                    live_in_file.push(key);
                }
            }
            if !live_in_file.is_empty() {
                fi.add_file(df.path.clone(), live_in_file);
            }
        }
    }
    Ok(fi)
}

/// Avoid a circular module reference by re-exporting `DynCatalog` through a
/// private module. `verify::DynCatalog` is the canonical name.
mod pg2iceberg_iceberg_dyn {
    pub use crate::verify::DynCatalog;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn add_then_lookup() {
        let mut fi = FileIndex::new();
        fi.add_file("p0".into(), vec!["k1".into(), "k2".into()]);
        assert_eq!(fi.lookup("k1"), Some("p0"));
        assert_eq!(fi.lookup("k2"), Some("p0"));
        assert_eq!(fi.lookup("missing"), None);
        assert!(fi.contains_pk("k1"));
        assert!(!fi.contains_pk("missing"));
    }

    #[test]
    fn remove_pks_clears_mapping_and_drops_empty_files() {
        let mut fi = FileIndex::new();
        fi.add_file("p0".into(), vec!["k1".into(), "k2".into()]);
        fi.remove_pks(&["k1".into()]);
        assert_eq!(fi.lookup("k1"), None);
        assert_eq!(fi.lookup("k2"), Some("p0"));
        assert_eq!(fi.live_pk_count(), 1);

        fi.remove_pks(&["k2".into()]);
        assert!(fi.live_files().is_empty());
        assert_eq!(fi.live_pk_count(), 0);
    }

    #[test]
    fn add_file_with_overlapping_pk_remaps_to_new_file() {
        // Re-insert flow: a PK lives in p0, then a new file p1 covers it.
        let mut fi = FileIndex::new();
        fi.add_file("p0".into(), vec!["k1".into()]);
        fi.add_file("p1".into(), vec!["k1".into()]);
        // The PK now points to p1.
        assert_eq!(fi.lookup("k1"), Some("p1"));
        // p0 still appears in live_files (it has the stale entry); it'll be
        // GC'd when the materializer's equality delete removes that PK.
        // What matters is the lookup is fresh.
    }

    #[test]
    fn affected_files_collects_distinct_paths() {
        let mut fi = FileIndex::new();
        fi.add_file("p0".into(), vec!["k1".into(), "k2".into()]);
        fi.add_file("p1".into(), vec!["k3".into()]);
        let s = fi.affected_files(&["k1".into(), "k3".into(), "missing".into()]);
        let v: Vec<&String> = s.iter().collect();
        assert_eq!(v, vec![&"p0".to_string(), &"p1".to_string()]);
    }
}
