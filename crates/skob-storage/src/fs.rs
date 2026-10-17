//! Filesystem blob store. Keys are relative paths under a root directory.
//! Blob storage is the files themselves; the root is a cache root or the
//! durability root for `--blob fs`.

use std::path::{Component, Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use skob_core::{BlobMeta, BlobStore, ListPage, Result, SkobError};
use tokio::io::AsyncWriteExt;

const PAGE: usize = 1000;
const TMP_PREFIX: &str = ".skob-tmp-";

static TMP_SEQ: AtomicU64 = AtomicU64::new(0);

/// Files under `root`. Path segments of a key become directories.
///
/// `put` writes a temp file in the destination directory and renames it into
/// place, so readers never observe a partial object. Keys that would escape
/// the root (`..`, absolute, or a segment that resets the path) are rejected.
pub struct FsBlobStore {
    root: PathBuf,
}

impl std::fmt::Debug for FsBlobStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FsBlobStore").field("root", &self.root).finish()
    }
}

impl FsBlobStore {
    /// Create `root` if needed and return a store rooted there.
    pub fn open(root: impl Into<PathBuf>) -> Result<Arc<Self>> {
        let root = root.into();
        std::fs::create_dir_all(&root).map_err(SkobError::storage)?;
        let root = std::fs::canonicalize(&root).map_err(SkobError::storage)?;
        Ok(Arc::new(Self { root }))
    }

    fn resolve(&self, key: &str) -> Result<PathBuf> {
        validate_key(key)?;
        let mut path = self.root.clone();
        for segment in key.split('/') {
            push_segment(&mut path, segment).map_err(|_| key_rejected(key))?;
        }
        if !path.starts_with(&self.root) {
            return Err(key_rejected(key));
        }
        Ok(path)
    }

    fn walk_dir_for_prefix(&self, prefix: &str) -> Result<PathBuf> {
        validate_prefix(prefix)?;
        let dir_part = match prefix.rfind('/') {
            Some(idx) => &prefix[..idx],
            None => "",
        };
        let mut path = self.root.clone();
        for segment in dir_part.split('/') {
            if segment.is_empty() {
                continue;
            }
            push_segment(&mut path, segment).map_err(|_| key_rejected(prefix))?;
        }
        if !path.starts_with(&self.root) {
            return Err(key_rejected(prefix));
        }
        Ok(path)
    }

    async fn collect_keys(&self, prefix: &str) -> Result<Vec<String>> {
        let start = self.walk_dir_for_prefix(prefix)?;
        let mut keys = Vec::new();
        let mut stack = vec![start];
        while let Some(dir) = stack.pop() {
            let mut rd = match tokio::fs::read_dir(&dir).await {
                Ok(rd) => rd,
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => continue,
                Err(err) => return Err(SkobError::storage(err)),
            };
            while let Some(entry) = rd.next_entry().await.map_err(SkobError::storage)? {
                let file_type = entry.file_type().await.map_err(SkobError::storage)?;
                // Do not follow symlinks. A link inside the root could point outside it.
                if file_type.is_symlink() {
                    continue;
                }
                if file_type.is_dir() {
                    let child = entry.path();
                    if child.starts_with(&self.root) {
                        stack.push(child);
                    }
                    continue;
                }
                if !file_type.is_file() {
                    continue;
                }
                let name = entry.file_name();
                if name.to_string_lossy().starts_with(TMP_PREFIX) {
                    continue;
                }
                let path = entry.path();
                let rel = path.strip_prefix(&self.root).map_err(SkobError::storage)?;
                let key = key_from_relative(rel);
                if key.starts_with(prefix) {
                    keys.push(key);
                }
            }
        }
        keys.sort();
        Ok(keys)
    }
}

fn key_rejected(key: &str) -> SkobError {
    SkobError::BadRequest(format!("blob key `{key}` escapes storage root"))
}

fn validate_key(key: &str) -> Result<()> {
    if key.is_empty() || key.contains('\0') || key.contains('\\') || Path::new(key).is_absolute() {
        return Err(key_rejected(key));
    }
    let mut saw_segment = false;
    for segment in key.split('/') {
        if segment.is_empty() || segment == "." || segment == ".." || Path::new(segment).is_absolute() {
            return Err(key_rejected(key));
        }
        saw_segment = true;
    }
    if !saw_segment {
        return Err(key_rejected(key));
    }
    Ok(())
}

fn validate_prefix(prefix: &str) -> Result<()> {
    if prefix.is_empty() {
        return Ok(());
    }
    if prefix.contains('\0') || prefix.contains('\\') || Path::new(prefix).is_absolute() {
        return Err(key_rejected(prefix));
    }
    for segment in prefix.split('/') {
        if segment.is_empty() {
            continue;
        }
        if segment == "." || segment == ".." || Path::new(segment).is_absolute() {
            return Err(key_rejected(prefix));
        }
    }
    Ok(())
}

fn push_segment(path: &mut PathBuf, segment: &str) -> std::result::Result<(), ()> {
    if segment.is_empty() || segment == "." || segment == ".." || segment.contains('\0') || segment.contains('\\') {
        return Err(());
    }
    let as_path = Path::new(segment);
    if as_path.is_absolute() || as_path.components().any(|c| !matches!(c, Component::Normal(_))) {
        return Err(());
    }
    path.push(segment);
    Ok(())
}

fn key_from_relative(rel: &Path) -> String {
    rel.components()
        .filter_map(|component| match component {
            Component::Normal(part) => Some(part.to_string_lossy().into_owned()),
            _ => None,
        })
        .collect::<Vec<_>>()
        .join("/")
}

fn temp_name() -> String {
    let n = TMP_SEQ.fetch_add(1, Ordering::Relaxed);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    format!("{TMP_PREFIX}{}-{nanos}-{n}", std::process::id())
}

async fn prune_empty_parents(root: &Path, start: &Path) {
    let mut current = start.to_path_buf();
    while current.starts_with(root) && current != root {
        match tokio::fs::remove_dir(&current).await {
            Ok(()) => {
                current = match current.parent() {
                    Some(parent) => parent.to_path_buf(),
                    None => break,
                };
            }
            Err(_) => break,
        }
    }
}

#[async_trait]
impl BlobStore for FsBlobStore {
    async fn put(&self, key: &str, body: Bytes, _content_type: Option<&str>) -> Result<BlobMeta> {
        let path = self.resolve(key)?;
        let parent = path
            .parent()
            .ok_or_else(|| key_rejected(key))?
            .to_path_buf();
        if !parent.starts_with(&self.root) {
            return Err(key_rejected(key));
        }
        tokio::fs::create_dir_all(&parent).await.map_err(SkobError::storage)?;

        let tmp = parent.join(temp_name());
        let write_result = async {
            let mut file = tokio::fs::File::create(&tmp).await.map_err(SkobError::storage)?;
            file.write_all(&body).await.map_err(SkobError::storage)?;
            file.sync_all().await.map_err(SkobError::storage)?;
            drop(file);
            tokio::fs::rename(&tmp, &path).await.map_err(SkobError::storage)?;
            Ok::<(), SkobError>(())
        }
        .await;
        if write_result.is_err() {
            let _ = tokio::fs::remove_file(&tmp).await;
            return write_result.map(|_| BlobMeta {
                key: key.to_string(),
                size: 0,
                etag: None,
            });
        }
        // Durability: the rename is atomic, and syncing the directory makes the
        // directory entry itself durable on crash.
        if let Ok(dir) = std::fs::File::open(&parent) {
            let _ = dir.sync_all();
        }
        Ok(BlobMeta {
            key: key.to_string(),
            size: body.len() as u64,
            etag: None,
        })
    }

    async fn get(&self, key: &str) -> Result<Option<Bytes>> {
        let path = self.resolve(key)?;
        match tokio::fs::read(&path).await {
            Ok(bytes) => Ok(Some(Bytes::from(bytes))),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(err) => Err(SkobError::storage(err)),
        }
    }

    async fn head(&self, key: &str) -> Result<Option<BlobMeta>> {
        let path = self.resolve(key)?;
        match tokio::fs::metadata(&path).await {
            Ok(meta) if meta.is_file() => Ok(Some(BlobMeta {
                key: key.to_string(),
                size: meta.len(),
                etag: None,
            })),
            Ok(_) => Ok(None),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(err) => Err(SkobError::storage(err)),
        }
    }

    async fn delete(&self, key: &str) -> Result<()> {
        let path = self.resolve(key)?;
        match tokio::fs::remove_file(&path).await {
            Ok(()) => {
                if let Some(parent) = path.parent() {
                    prune_empty_parents(&self.root, parent).await;
                }
                Ok(())
            }
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(err) => Err(SkobError::storage(err)),
        }
    }

    async fn list(&self, prefix: &str, cursor: Option<&str>) -> Result<ListPage> {
        let keys = self.collect_keys(prefix).await?;
        let mut matched = Vec::new();
        for key in keys {
            if cursor.is_some_and(|c| key.as_str() <= c) {
                continue;
            }
            let size = match self.head(&key).await? {
                Some(meta) => meta.size,
                None => continue,
            };
            matched.push(BlobMeta {
                key,
                size,
                etag: None,
            });
            if matched.len() > PAGE {
                break;
            }
        }
        let cursor = if matched.len() > PAGE {
            matched.pop();
            matched.last().map(|meta| meta.key.clone())
        } else {
            None
        };
        Ok(ListPage {
            objects: matched,
            cursor,
        })
    }

    async fn delete_prefix(&self, prefix: &str) -> Result<u64> {
        let keys = self.collect_keys(prefix).await?;
        let mut deleted = 0u64;
        for key in keys {
            self.delete(&key).await?;
            deleted += 1;
        }
        Ok(deleted)
    }
}
