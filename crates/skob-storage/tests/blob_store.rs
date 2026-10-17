//! Filesystem store round-trip, plus optional live S3 when `SKOB_S3_ENDPOINT` is set.

use std::sync::Arc;

use bytes::Bytes;
use skob_core::BlobStore;
use skob_storage::{open, S3BlobStore, S3Config, StorageConfig};

#[tokio::test]
async fn filesystem_store_and_rustfs_config() {
    let dir = std::env::temp_dir().join(format!(
        "skob-storage-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0)
    ));
    let _cleanup = TempDir(dir.clone());

    let memory = open(StorageConfig::Memory).await.expect("memory store");
    memory
        .put("memory-key", Bytes::from_static(b"mem"), None)
        .await
        .expect("memory put");
    let mem_body = memory.get("memory-key").await.expect("memory get").expect("present");
    assert_eq!(mem_body.as_ref(), b"mem");

    let store = open(StorageConfig::Fs { root: dir.clone() })
        .await
        .expect("fs store");

    let a = "projects/p1/collections/c1/objects/a.json";
    let b = "projects/p1/collections/c1/objects/b.json";
    let dotted = "projects/p1/collections/c1/objects/obj.v1.json";
    let other = "projects/p2/meta.json";

    store
        .put(a, Bytes::from_static(br#"{"n":1}"#), Some("application/json"))
        .await
        .expect("put a");
    store
        .put(b, Bytes::from_static(b"bbb"), None)
        .await
        .expect("put b");
    store
        .put(dotted, Bytes::from_static(b"dotted"), None)
        .await
        .expect("put dotted name");
    store
        .put(other, Bytes::from_static(b"p2"), None)
        .await
        .expect("put other project");

    // Overwrite is the same key, new bytes.
    store
        .put(a, Bytes::from_static(br#"{"n":2}"#), Some("application/json"))
        .await
        .expect("overwrite a");

    let body = store.get(a).await.expect("get").expect("object exists");
    assert_eq!(body.as_ref(), br#"{"n":2}"#);
    assert!(store.get("projects/missing.json").await.expect("missing get").is_none());

    let head = store.head(a).await.expect("head").expect("head exists");
    assert_eq!(head.key, a);
    assert_eq!(head.size, body.len() as u64);
    assert!(store.head("projects/missing.json").await.expect("missing head").is_none());

    let page = store.list("projects/p1/", None).await.expect("list prefix");
    let listed: Vec<_> = page.objects.iter().map(|meta| meta.key.as_str()).collect();
    assert_eq!(
        listed,
        vec![
            "projects/p1/collections/c1/objects/a.json",
            "projects/p1/collections/c1/objects/b.json",
            "projects/p1/collections/c1/objects/obj.v1.json",
        ]
    );
    assert!(page.cursor.is_none());
    assert!(page.objects.iter().all(|meta| meta.key.starts_with("projects/p1/")));

    let after_first = store
        .list("projects/p1/", Some(listed[0]))
        .await
        .expect("list with cursor");
    assert!(after_first.objects.iter().all(|meta| meta.key.as_str() > listed[0]));
    assert!(!after_first.objects.iter().any(|meta| meta.key == listed[0]));

    store.delete(a).await.expect("delete a");
    assert!(store.get(a).await.expect("get deleted").is_none());
    store.delete("projects/does-not-exist.json").await.expect("delete missing");

    let removed = store.delete_prefix("projects/p1/").await.expect("delete prefix");
    assert_eq!(removed, 2);
    assert!(store.list("projects/p1/", None).await.expect("list after delete").objects.is_empty());
    assert!(!dir.join("projects/p1").exists(), "empty parents inside the root should be removed");
    let other_body = store.get(other).await.expect("other get").expect("p2 remains");
    assert_eq!(other_body.as_ref(), b"p2");

    let escape_name = format!("skob-escape-{}", std::process::id());
    let outside = dir
        .parent()
        .expect("temp parent")
        .join(&escape_name);
    assert!(store
        .put(&format!("../{escape_name}"), Bytes::from_static(b"nope"), None)
        .await
        .is_err());
    assert!(store
        .put(&format!("foo/../../{escape_name}"), Bytes::from_static(b"nope"), None)
        .await
        .is_err());
    assert!(store
        .put(&format!("/{escape_name}"), Bytes::from_static(b"nope"), None)
        .await
        .is_err());
    assert!(store.get("..").await.is_err());
    assert!(store.list("../", None).await.is_err());
    assert!(!outside.exists(), "rejected key must not create a file outside the root");

    let rustfs = S3Config::for_rustfs("http://127.0.0.1:9000", "skob", "rustfsadmin", "rustfsadmin");
    assert!(rustfs.path_style);
    assert_eq!(rustfs.endpoint.as_deref(), Some("http://127.0.0.1:9000"));

    if std::env::var("SKOB_S3_ENDPOINT").is_err() {
        return;
    }

    live_s3().await;
}

async fn live_s3() {
    let endpoint = std::env::var("SKOB_S3_ENDPOINT").expect("endpoint");
    let bucket = std::env::var("SKOB_S3_BUCKET").unwrap_or_else(|_| "skob".into());
    let access_key = std::env::var("SKOB_S3_ACCESS_KEY").unwrap_or_else(|_| "rustfsadmin".into());
    let secret_key = std::env::var("SKOB_S3_SECRET_KEY").unwrap_or_else(|_| "rustfsadmin".into());
    let prefix = format!(
        "skob-it-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0)
    );

    let config = S3Config::for_rustfs(&endpoint, &bucket, &access_key, &secret_key).with_prefix(&prefix);
    assert!(config.path_style);
    assert_eq!(config.endpoint.as_deref(), Some(endpoint.as_str()));

    let store = S3BlobStore::connect(config.clone()).await.expect("s3 connect");
    // Second connect must tolerate the bucket already existing.
    let again: Arc<dyn BlobStore> = S3BlobStore::connect(config).await.expect("s3 connect again");

    let key = "projects/live/objects/one.json";
    let other = "projects/live/objects/two.json";
    let keep = "projects/other/meta.json";
    store
        .put(key, Bytes::from_static(br#"{"live":true}"#), Some("application/json"))
        .await
        .expect("s3 put");
    store
        .put(other, Bytes::from_static(b"two"), None)
        .await
        .expect("s3 put two");
    again.put(keep, Bytes::from_static(b"keep"), None).await.expect("s3 put keep");

    let body = store.get(key).await.expect("s3 get").expect("s3 object");
    assert_eq!(body.as_ref(), br#"{"live":true}"#);
    assert!(store.get("projects/live/missing.json").await.expect("s3 missing").is_none());

    let head = store.head(key).await.expect("s3 head").expect("s3 head meta");
    assert_eq!(head.key, key);
    assert_eq!(head.size, body.len() as u64);

    let page = store.list("projects/live/", None).await.expect("s3 list");
    let mut keys: Vec<_> = page.objects.iter().map(|meta| meta.key.clone()).collect();
    keys.sort();
    assert_eq!(
        keys,
        vec![
            "projects/live/objects/one.json".to_string(),
            "projects/live/objects/two.json".to_string(),
        ]
    );

    store.delete(key).await.expect("s3 delete");
    assert!(store.get(key).await.expect("s3 get deleted").is_none());
    store.delete(key).await.expect("s3 delete missing");

    let removed = store.delete_prefix("projects/live/").await.expect("s3 delete prefix");
    assert!(removed >= 1);
    assert!(store
        .list("projects/live/", None)
        .await
        .expect("s3 list after delete")
        .objects
        .is_empty());
    assert!(again.get(keep).await.expect("kept get").is_some());
    again.delete_prefix("projects/other/").await.expect("cleanup keep");
}

struct TempDir(std::path::PathBuf);

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
