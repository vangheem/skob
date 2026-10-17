//! S3 API blob store.
//!
//! One client covers AWS S3 and S3-compatible endpoints (RustFS, MinIO,
//! Cloudflare R2, GCS XML interoperability). Credentials come from
//! [`S3Config`], never from the ambient AWS credential chain. Native GCS JSON
//! and Azure Blob are not implemented here; they are separate `BlobStore`
//! impls. See `adapt`.

use std::sync::Arc;

use async_trait::async_trait;
use aws_credential_types::Credentials;
use aws_sdk_s3::error::ProvideErrorMetadata;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{CreateBucketConfiguration, Delete, ObjectIdentifier};
use aws_sdk_s3::Client;
use bytes::Bytes;
use skob_core::{BlobMeta, BlobStore, ListPage, Result, SkobError};

const PAGE: i32 = 1000;
const DELETE_BATCH: usize = 1000;

/// Connection settings for an S3 API endpoint.
///
/// `prefix`, when non-empty, is stored in front of every key and stripped
/// before keys are returned. `connect` normalizes a non-empty prefix so it
/// ends with `/`.
#[derive(Clone, Debug)]
pub struct S3Config {
    pub endpoint: Option<String>,
    pub region: String,
    pub bucket: String,
    pub access_key: String,
    pub secret_key: String,
    pub path_style: bool,
    pub prefix: String,
}

impl S3Config {
    /// AWS S3. Virtual-hosted-style requests, no custom endpoint.
    ///
    /// Credentials are the caller's. This does not read the environment.
    pub fn for_aws(
        region: impl Into<String>,
        bucket: impl Into<String>,
        access_key: impl Into<String>,
        secret_key: impl Into<String>,
    ) -> Self {
        Self {
            endpoint: None,
            region: region.into(),
            bucket: bucket.into(),
            access_key: access_key.into(),
            secret_key: secret_key.into(),
            path_style: false,
            prefix: String::new(),
        }
    }

    /// RustFS. Path-style against the given endpoint. Region defaults to
    /// `us-east-1`, which RustFS ignores but the SDK requires.
    pub fn for_rustfs(
        endpoint: impl Into<String>,
        bucket: impl Into<String>,
        access_key: impl Into<String>,
        secret_key: impl Into<String>,
    ) -> Self {
        Self::path_style_endpoint(endpoint, "us-east-1", bucket, access_key, secret_key)
    }

    /// MinIO. Same defaults as RustFS: path-style, region `us-east-1`.
    pub fn for_minio(
        endpoint: impl Into<String>,
        bucket: impl Into<String>,
        access_key: impl Into<String>,
        secret_key: impl Into<String>,
    ) -> Self {
        Self::path_style_endpoint(endpoint, "us-east-1", bucket, access_key, secret_key)
    }

    /// Cloudflare R2. `endpoint` is the account endpoint the caller already
    /// has (`https://<accountid>.r2.cloudflarestorage.com`). Region defaults
    /// to `auto`. Path-style is on so the account endpoint does not depend on
    /// bucket DNS.
    pub fn for_r2(
        endpoint: impl Into<String>,
        bucket: impl Into<String>,
        access_key: impl Into<String>,
        secret_key: impl Into<String>,
    ) -> Self {
        Self::path_style_endpoint(endpoint, "auto", bucket, access_key, secret_key)
    }

    /// GCS XML / S3 interoperability. `endpoint` is the caller-supplied
    /// interoperability endpoint (usually `https://storage.googleapis.com`).
    /// Region defaults to `auto`. Path-style is on.
    ///
    /// This is not the native GCS JSON API. That adapter would be a separate
    /// `BlobStore` impl beside this module.
    pub fn for_gcs_interop(
        endpoint: impl Into<String>,
        bucket: impl Into<String>,
        access_key: impl Into<String>,
        secret_key: impl Into<String>,
    ) -> Self {
        Self::path_style_endpoint(endpoint, "auto", bucket, access_key, secret_key)
    }

    /// Set a key prefix. A non-empty prefix is normalized to end with `/`
    /// when the store connects.
    pub fn with_prefix(mut self, prefix: impl Into<String>) -> Self {
        self.prefix = prefix.into();
        self
    }

    fn path_style_endpoint(
        endpoint: impl Into<String>,
        region: &str,
        bucket: impl Into<String>,
        access_key: impl Into<String>,
        secret_key: impl Into<String>,
    ) -> Self {
        Self {
            endpoint: Some(endpoint.into()),
            region: region.to_string(),
            bucket: bucket.into(),
            access_key: access_key.into(),
            secret_key: secret_key.into(),
            path_style: true,
            prefix: String::new(),
        }
    }
}

/// S3-compatible blob store. Keys are `{prefix}{key}` in the bucket.
pub struct S3BlobStore {
    client: Client,
    bucket: String,
    prefix: String,
}

impl std::fmt::Debug for S3BlobStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("S3BlobStore")
            .field("bucket", &self.bucket)
            .field("prefix", &self.prefix)
            .finish_non_exhaustive()
    }
}

impl S3BlobStore {
    /// Build a client from static credentials and create the bucket if it is missing.
    ///
    /// "Already exists" and "already owned" responses are success. Other create
    /// failures are `SkobError::storage`.
    pub async fn connect(config: S3Config) -> Result<Arc<Self>> {
        if config.bucket.is_empty() {
            return Err(SkobError::storage("s3 bucket name is empty"));
        }
        if config.region.is_empty() {
            return Err(SkobError::storage("s3 region is empty"));
        }

        let creds = Credentials::from_keys(config.access_key.clone(), config.secret_key.clone(), None);
        // `defaults` supplies the HTTP client and retry stack. The credentials
        // provider replaces the ambient AWS chain; access key and secret come
        // only from `config`.
        let sdk_config = aws_config::defaults(aws_config::BehaviorVersion::latest())
            .region(aws_sdk_s3::config::Region::new(config.region.clone()))
            .credentials_provider(creds)
            .load()
            .await;

        let mut builder = aws_sdk_s3::config::Builder::from(&sdk_config)
            .force_path_style(config.path_style)
            // WHEN_SUPPORTED sends checksum headers that RustFS, MinIO, and
            // other S3-compatible servers reject. Checksums stay on when the
            // operation actually requires them.
            .request_checksum_calculation(aws_sdk_s3::config::RequestChecksumCalculation::WhenRequired)
            .response_checksum_validation(aws_sdk_s3::config::ResponseChecksumValidation::WhenRequired);

        if let Some(endpoint) = config.endpoint.as_deref().filter(|endpoint| !endpoint.is_empty()) {
            builder = builder.endpoint_url(endpoint);
        }

        let client = Client::from_conf(builder.build());
        ensure_bucket(&client, &config).await?;

        Ok(Arc::new(Self {
            client,
            bucket: config.bucket,
            prefix: normalize_prefix(&config.prefix),
        }))
    }

    fn stored_key(&self, key: &str) -> Result<String> {
        if key.is_empty() {
            return Err(SkobError::BadRequest("blob key is empty".into()));
        }
        Ok(format!("{}{key}", self.prefix))
    }

    fn user_key<'a>(&self, stored: &'a str) -> Option<&'a str> {
        if self.prefix.is_empty() {
            Some(stored)
        } else {
            stored.strip_prefix(&self.prefix)
        }
    }

    async fn list_stored(&self, stored_prefix: &str, token: Option<String>) -> Result<StoredPage> {
        let mut req = self
            .client
            .list_objects_v2()
            .bucket(&self.bucket)
            .max_keys(PAGE);
        if !stored_prefix.is_empty() {
            req = req.prefix(stored_prefix);
        }
        if let Some(token) = token {
            req = req.continuation_token(token);
        }
        let out = req.send().await.map_err(SkobError::storage)?;
        let mut keys = Vec::new();
        for object in out.contents() {
            let Some(key) = object.key() else {
                continue;
            };
            keys.push(StoredObject {
                key: key.to_string(),
                size: object.size().unwrap_or(0).max(0) as u64,
                etag: object.e_tag().map(str::to_string),
            });
        }
        let next = if out.is_truncated().unwrap_or(false) {
            out.next_continuation_token().map(str::to_string)
        } else {
            None
        };
        Ok(StoredPage { keys, next })
    }
}

struct StoredObject {
    key: String,
    size: u64,
    etag: Option<String>,
}

struct StoredPage {
    keys: Vec<StoredObject>,
    next: Option<String>,
}

fn normalize_prefix(prefix: &str) -> String {
    if prefix.is_empty() || prefix.ends_with('/') {
        prefix.to_string()
    } else {
        format!("{prefix}/")
    }
}

async fn ensure_bucket(client: &Client, config: &S3Config) -> Result<()> {
    let mut req = client.create_bucket().bucket(&config.bucket);
    // AWS rejects a location constraint of us-east-1, and path-style
    // compatible servers (RustFS, MinIO) often reject the header entirely.
    let custom_endpoint = config.endpoint.as_deref().is_some_and(|endpoint| !endpoint.is_empty());
    if !custom_endpoint && config.region != "us-east-1" {
        let location = CreateBucketConfiguration::builder()
            .location_constraint(config.region.as_str().into())
            .build();
        req = req.create_bucket_configuration(location);
    }
    match req.send().await {
        Ok(_) => Ok(()),
        Err(err) if bucket_already_exists(&err) => {
            tracing::debug!(bucket = %config.bucket, "s3 bucket already exists");
            Ok(())
        }
        Err(err) => Err(SkobError::storage(err)),
    }
}

fn bucket_already_exists(err: &(impl ProvideErrorMetadata + std::fmt::Display)) -> bool {
    match err.code() {
        Some("BucketAlreadyExists" | "BucketAlreadyOwnedByYou") => true,
        _ => {
            let text = err.to_string();
            text.contains("BucketAlreadyExists")
                || text.contains("BucketAlreadyOwnedByYou")
                || text.contains("already owned")
        }
    }
}

fn is_missing(err: &(impl ProvideErrorMetadata + std::fmt::Display)) -> bool {
    match err.code() {
        Some("NoSuchKey" | "NotFound" | "NoSuchBucket" | "404") => true,
        _ => {
            let text = err.to_string();
            text.contains("NoSuchKey")
                || text.contains("NotFound")
                || text.contains("Not Found")
                || text.contains("status code: 404")
                || text.contains("StatusCode: 404")
        }
    }
}

fn batch_unsupported(err: &(impl ProvideErrorMetadata + std::fmt::Display)) -> bool {
    match err.code() {
        Some("NotImplemented" | "NotSupported" | "MethodNotAllowed" | "XNotImplemented") => true,
        _ => {
            let text = err.to_string();
            text.contains("NotImplemented") || text.contains("not implemented")
        }
    }
}

fn meta_from_stored(store: &S3BlobStore, object: StoredObject) -> Option<BlobMeta> {
    let key = store.user_key(&object.key)?.to_string();
    Some(BlobMeta {
        key,
        size: object.size,
        etag: object.etag,
    })
}

#[async_trait]
impl BlobStore for S3BlobStore {
    async fn put(&self, key: &str, body: Bytes, content_type: Option<&str>) -> Result<BlobMeta> {
        let stored = self.stored_key(key)?;
        let size = body.len() as u64;
        let mut req = self
            .client
            .put_object()
            .bucket(&self.bucket)
            .key(&stored)
            .body(ByteStream::from(body));
        if let Some(content_type) = content_type.filter(|value| !value.is_empty()) {
            req = req.content_type(content_type);
        }
        let out = req.send().await.map_err(SkobError::storage)?;
        Ok(BlobMeta {
            key: key.to_string(),
            size,
            etag: out.e_tag().map(str::to_string),
        })
    }

    async fn get(&self, key: &str) -> Result<Option<Bytes>> {
        let stored = self.stored_key(key)?;
        match self.client.get_object().bucket(&self.bucket).key(&stored).send().await {
            Ok(out) => {
                let bytes = out.body.collect().await.map_err(SkobError::storage)?.into_bytes();
                Ok(Some(bytes))
            }
            Err(err) if is_missing(&err) => Ok(None),
            Err(err) => Err(SkobError::storage(err)),
        }
    }

    async fn head(&self, key: &str) -> Result<Option<BlobMeta>> {
        let stored = self.stored_key(key)?;
        match self.client.head_object().bucket(&self.bucket).key(&stored).send().await {
            Ok(out) => Ok(Some(BlobMeta {
                key: key.to_string(),
                size: out.content_length().unwrap_or(0).max(0) as u64,
                etag: out.e_tag().map(str::to_string),
            })),
            Err(err) if is_missing(&err) => Ok(None),
            Err(err) => Err(SkobError::storage(err)),
        }
    }

    async fn delete(&self, key: &str) -> Result<()> {
        let stored = self.stored_key(key)?;
        match self.client.delete_object().bucket(&self.bucket).key(&stored).send().await {
            Ok(_) => Ok(()),
            Err(err) if is_missing(&err) => Ok(()),
            Err(err) => Err(SkobError::storage(err)),
        }
    }

    async fn list(&self, prefix: &str, cursor: Option<&str>) -> Result<ListPage> {
        let stored_prefix = format!("{}{prefix}", self.prefix);
        let page = self
            .list_stored(&stored_prefix, cursor.map(str::to_string))
            .await?;
        let mut objects = Vec::with_capacity(page.keys.len());
        for stored in page.keys {
            let Some(meta) = meta_from_stored(self, stored) else {
                continue;
            };
            if meta.key.starts_with(prefix) {
                objects.push(meta);
            }
        }
        Ok(ListPage {
            objects,
            cursor: page.next,
        })
    }

    async fn delete_prefix(&self, prefix: &str) -> Result<u64> {
        let stored_prefix = format!("{}{prefix}", self.prefix);
        let mut token = None;
        let mut deleted = 0u64;
        loop {
            let page = self.list_stored(&stored_prefix, token).await?;
            let keys: Vec<String> = page
                .keys
                .into_iter()
                .map(|object| object.key)
                .filter(|key| self.user_key(key).is_some_and(|user| user.starts_with(prefix)))
                .collect();
            deleted += delete_keys(&self.client, &self.bucket, &keys).await?;
            match page.next {
                Some(next) => token = Some(next),
                None => break,
            }
        }
        Ok(deleted)
    }
}

async fn delete_keys(client: &Client, bucket: &str, keys: &[String]) -> Result<u64> {
    if keys.is_empty() {
        return Ok(0);
    }
    let mut deleted = 0u64;
    for chunk in keys.chunks(DELETE_BATCH) {
        match delete_batch(client, bucket, chunk).await {
            Ok(n) => deleted += n,
            Err(err) if batch_unsupported_msg(&err) => {
                for key in chunk {
                    match client.delete_object().bucket(bucket).key(key).send().await {
                        Ok(_) => deleted += 1,
                        Err(err) if is_missing(&err) => deleted += 1,
                        Err(err) => return Err(SkobError::storage(err)),
                    }
                }
            }
            Err(err) => return Err(err),
        }
    }
    Ok(deleted)
}

fn batch_unsupported_msg(err: &SkobError) -> bool {
    match err {
        SkobError::Storage(message) => {
            message.contains("NotImplemented")
                || message.contains("not implemented")
                || message.contains("MethodNotAllowed")
                || message.contains("NotSupported")
        }
        _ => false,
    }
}

async fn delete_batch(client: &Client, bucket: &str, keys: &[String]) -> Result<u64> {
    let mut objects = Vec::with_capacity(keys.len());
    for key in keys {
        let ident = ObjectIdentifier::builder()
            .key(key)
            .build()
            .map_err(SkobError::storage)?;
        objects.push(ident);
    }
    let delete = Delete::builder()
        .set_objects(Some(objects))
        .quiet(false)
        .build()
        .map_err(SkobError::storage)?;
    let out = match client.delete_objects().bucket(bucket).delete(delete).send().await {
        Ok(out) => out,
        // Include the code in the message so `delete_keys` can fall back to
        // one DeleteObject per key on servers that do not implement the batch API.
        Err(err) if batch_unsupported(&err) => {
            return Err(SkobError::storage(format!("NotImplemented: {err}")));
        }
        Err(err) => return Err(SkobError::storage(err)),
    };
    let mut failed = Vec::new();
    for err in out.errors() {
        let code = err.code().unwrap_or("");
        if code == "NoSuchKey" || code == "NotFound" {
            continue;
        }
        failed.push(format!(
            "{}: {}",
            err.key().unwrap_or(""),
            err.message().unwrap_or(code)
        ));
    }
    if !failed.is_empty() {
        return Err(SkobError::storage(format!(
            "s3 delete_objects failed: {}",
            failed.join("; ")
        )));
    }
    Ok(keys.len() as u64)
}
