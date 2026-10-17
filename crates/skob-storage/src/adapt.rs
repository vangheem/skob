//! How to add a native cloud adapter without a second store trait.
//!
//! `skob_core::BlobStore` is the port. Compute code depends on that trait and
//! on the key helpers in `skob_core::keys`. A new backend is a new type that
//! implements the same trait, living in its own module beside `s3.rs`:
//!
//! - `gcs.rs` for the native GCS JSON API (`storage.googleapis.com` JSON, not
//!   the XML/S3 interoperability endpoint already covered by `S3BlobStore`)
//! - `azure.rs` for native Azure Blob Storage
//!
//! Do not introduce `GcsStore` / `AzureStore` traits, and do not wrap those
//! SDKs behind a fake S3 client. The S3 API path (AWS, RustFS, MinIO, R2, and
//! GCS XML interoperability) stays in `s3.rs`. Native APIs are separate
//! `BlobStore` impls because their auth, paging, and conditional headers are
//! not the S3 model.
//!
//! A native adapter should:
//!
//! 1. Take an explicit config struct (endpoint, bucket/container, credentials).
//!    Do not read ambient cloud env chains unless the config says to.
//! 2. Implement `put`, `get`, `head`, `delete`, `list`, and `delete_prefix`.
//! 3. Map a missing object to `Ok(None)` on `get` and `head`. `delete` of a
//!    missing key is success.
//! 4. Return the caller's key, not a provider-prefixed or URL-encoded form.
//!    `list` is sorted by key. The cursor is whatever token the provider uses
//!    to resume; pass it back unchanged on the next call.
//! 5. `delete_prefix` pages through `list` and deletes in batches. It must not
//!    follow a prefix outside the configured bucket/container.
//! 6. Surface provider failures as `SkobError::storage`. Do not invent a
//!    parallel error enum.
//! 7. Keep object bytes as the `StoredObject` envelope the rest of skob already
//!    writes. This crate does not interpret payloads.
//!
//! Wire the new type into [`crate::StorageConfig`] only when the process flags
//! grow a matching `--blob` value. Until then, construct the adapter directly
//! and hand out `Arc<dyn BlobStore>`.

use crate::S3Config;

/// Cloud backends this crate can open without a fake client.
///
/// Native Azure Blob and native GCS JSON are not variants here. They are
/// additional [`skob_core::BlobStore`] impls, added beside `s3.rs` when those
/// SDKs are actually depended on.
#[derive(Clone, Debug)]
pub enum CloudBackend {
    /// S3 API. Covers AWS, RustFS, MinIO, Cloudflare R2, and GCS XML/S3 interoperability.
    S3(S3Config),
}

impl CloudBackend {
    /// Connect the selected backend and return it as the shared store trait.
    pub async fn open(self) -> skob_core::Result<std::sync::Arc<dyn skob_core::BlobStore>> {
        match self {
            Self::S3(config) => {
                let store: std::sync::Arc<dyn skob_core::BlobStore> =
                    crate::S3BlobStore::connect(config).await?;
                Ok(store)
            }
        }
    }
}
