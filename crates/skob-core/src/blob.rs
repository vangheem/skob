use async_trait::async_trait;
use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::error::Result;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlobMeta {
    pub key: String,
    pub size: u64,
    pub etag: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListPage {
    pub objects: Vec<BlobMeta>,
    pub cursor: Option<String>,
}

/// Object-storage port. Compute nodes never talk to a cloud SDK except through this trait.
///
/// Adapters live in `skob-storage`. S3 (including RustFS, MinIO, R2, and GCS's S3
/// interoperability endpoint) is the first implementation. Native GCS and Azure
/// adapters implement the same trait without changing callers.
#[async_trait]
pub trait BlobStore: Send + Sync {
    async fn put(&self, key: &str, body: Bytes, content_type: Option<&str>) -> Result<BlobMeta>;
    async fn get(&self, key: &str) -> Result<Option<Bytes>>;
    async fn head(&self, key: &str) -> Result<Option<BlobMeta>>;
    /// Missing keys are not an error.
    async fn delete(&self, key: &str) -> Result<()>;
    async fn list(&self, prefix: &str, cursor: Option<&str>) -> Result<ListPage>;
    async fn delete_prefix(&self, prefix: &str) -> Result<u64>;
}
