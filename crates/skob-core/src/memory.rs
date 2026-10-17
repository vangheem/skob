use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use bytes::Bytes;

use crate::blob::{BlobMeta, BlobStore, ListPage};
use crate::error::Result;

/// In-process blob store for tests and the `--blob memory` deployment.
#[derive(Debug, Default)]
pub struct MemoryBlobStore {
    objects: Mutex<BTreeMap<String, Bytes>>,
}

impl MemoryBlobStore {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }
}

#[async_trait]
impl BlobStore for MemoryBlobStore {
    async fn put(&self, key: &str, body: Bytes, _content_type: Option<&str>) -> Result<BlobMeta> {
        let size = body.len() as u64;
        self.objects.lock().expect("memory blob lock").insert(key.to_string(), body);
        Ok(BlobMeta {
            key: key.to_string(),
            size,
            etag: None,
        })
    }

    async fn get(&self, key: &str) -> Result<Option<Bytes>> {
        Ok(self.objects.lock().expect("memory blob lock").get(key).cloned())
    }

    async fn head(&self, key: &str) -> Result<Option<BlobMeta>> {
        Ok(self.objects.lock().expect("memory blob lock").get(key).map(|body| BlobMeta {
            key: key.to_string(),
            size: body.len() as u64,
            etag: None,
        }))
    }

    async fn delete(&self, key: &str) -> Result<()> {
        self.objects.lock().expect("memory blob lock").remove(key);
        Ok(())
    }

    async fn list(&self, prefix: &str, cursor: Option<&str>) -> Result<ListPage> {
        const PAGE: usize = 1000;
        let objects = self.objects.lock().expect("memory blob lock");
        let mut matched: Vec<BlobMeta> = objects
            .iter()
            .filter(|(key, _)| key.starts_with(prefix) && cursor.is_none_or(|c| key.as_str() > c))
            .take(PAGE + 1)
            .map(|(key, body)| BlobMeta {
                key: key.clone(),
                size: body.len() as u64,
                etag: None,
            })
            .collect();
        let next = if matched.len() > PAGE {
            matched.pop();
            matched.last().map(|meta| meta.key.clone())
        } else {
            None
        };
        Ok(ListPage {
            objects: matched,
            cursor: next,
        })
    }

    async fn delete_prefix(&self, prefix: &str) -> Result<u64> {
        let mut objects = self.objects.lock().expect("memory blob lock");
        let keys: Vec<String> = objects
            .keys()
            .filter(|key| key.starts_with(prefix))
            .cloned()
            .collect();
        let n = keys.len() as u64;
        for key in keys {
            objects.remove(&key);
        }
        Ok(n)
    }
}
