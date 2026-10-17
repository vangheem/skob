//! Blob storage adapters for skob.
//!
//! [`skob_core::BlobStore`] is the only store trait. This crate implements it
//! for the local filesystem and for the S3 API (AWS, RustFS, MinIO, Cloudflare
//! R2, and GCS XML/S3 interoperability). Callers never talk to a cloud SDK
//! except through that trait.
//!
//! Native Azure Blob and native GCS JSON APIs are new `BlobStore`
//! implementations beside `s3.rs`, not new traits. See [`adapt`] for how those
//! adapters should be shaped.

pub mod adapt;
mod fs;
mod s3;

pub use adapt::CloudBackend;
pub use fs::FsBlobStore;
pub use s3::{S3BlobStore, S3Config};
pub use skob_core::MemoryBlobStore;

use std::path::PathBuf;
use std::sync::Arc;

use skob_core::{BlobStore, Result};

/// Which blob backend `open` should build.
#[derive(Clone, Debug)]
pub enum StorageConfig {
    Memory,
    Fs { root: PathBuf },
    S3(S3Config),
}

/// Open the blob store selected by `config`.
///
/// The returned handle is the core [`BlobStore`] trait. Filesystem and S3
/// adapters are also constructable directly when the caller needs the concrete
/// type.
pub async fn open(config: StorageConfig) -> Result<Arc<dyn BlobStore>> {
    match config {
        StorageConfig::Memory => {
            let store: Arc<dyn BlobStore> = MemoryBlobStore::new();
            Ok(store)
        }
        StorageConfig::Fs { root } => {
            let store: Arc<dyn BlobStore> = FsBlobStore::open(root)?;
            Ok(store)
        }
        StorageConfig::S3(config) => {
            let store: Arc<dyn BlobStore> = S3BlobStore::connect(config).await?;
            Ok(store)
        }
    }
}
