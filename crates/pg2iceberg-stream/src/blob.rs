//! Object-storage trait used by the staging layer.
//!
//! Production wraps `object_store` (S3); sim is in-memory. Kept narrower than
//! `object_store` itself because the pipeline only ever needs put/get of
//! whole-blob bytes — no streaming, no listing, no atomic-rename gymnastics.

use crate::Result;
use async_trait::async_trait;
use bytes::Bytes;

#[async_trait]
pub trait BlobStore: Send + Sync {
    async fn put(&self, path: &str, bytes: Bytes) -> Result<()>;
    async fn get(&self, path: &str) -> Result<Bytes>;
}
