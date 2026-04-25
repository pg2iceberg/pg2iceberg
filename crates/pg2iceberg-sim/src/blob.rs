//! In-memory `BlobStore` impl for tests and the DST harness.

use async_trait::async_trait;
use bytes::Bytes;
use pg2iceberg_stream::{BlobStore, Result, StreamError};
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

#[derive(Default, Clone)]
pub struct MemoryBlobStore {
    inner: Arc<Mutex<BTreeMap<String, Bytes>>>,
}

impl MemoryBlobStore {
    pub fn new() -> Self {
        Self::default()
    }

    /// Test-only: enumerate every stored path.
    pub fn paths(&self) -> Vec<String> {
        self.inner.lock().unwrap().keys().cloned().collect()
    }

    /// Test-only: total stored byte count.
    pub fn total_bytes(&self) -> usize {
        self.inner.lock().unwrap().values().map(|b| b.len()).sum()
    }
}

#[async_trait]
impl BlobStore for MemoryBlobStore {
    async fn put(&self, path: &str, bytes: Bytes) -> Result<()> {
        self.inner.lock().unwrap().insert(path.to_string(), bytes);
        Ok(())
    }

    async fn get(&self, path: &str) -> Result<Bytes> {
        self.inner
            .lock()
            .unwrap()
            .get(path)
            .cloned()
            .ok_or_else(|| StreamError::Io(format!("not found: {path}")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pollster::block_on;

    #[test]
    fn put_then_get_round_trips_bytes() {
        let store = MemoryBlobStore::new();
        let payload = Bytes::from_static(b"hello");
        block_on(store.put("foo/bar.parquet", payload.clone())).unwrap();
        let got = block_on(store.get("foo/bar.parquet")).unwrap();
        assert_eq!(got, payload);
    }

    #[test]
    fn get_missing_path_errors() {
        let store = MemoryBlobStore::new();
        let err = block_on(store.get("does/not/exist")).unwrap_err();
        assert!(matches!(err, StreamError::Io(_)));
    }

    #[test]
    fn paths_lists_all_stored_keys() {
        let store = MemoryBlobStore::new();
        block_on(store.put("a", Bytes::from_static(b"1"))).unwrap();
        block_on(store.put("b", Bytes::from_static(b"22"))).unwrap();
        let mut paths = store.paths();
        paths.sort();
        assert_eq!(paths, vec!["a".to_string(), "b".to_string()]);
        assert_eq!(store.total_bytes(), 3);
    }
}
