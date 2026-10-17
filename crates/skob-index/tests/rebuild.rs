use std::time::{SystemTime, UNIX_EPOCH};

use chrono::Utc;
use serde_json::json;
use skob_core::{
    object_key, to_json_bytes, BlobStore, CollectionId, FieldKind, IndexField, IndexSpec,
    IndexStatus, MemoryBlobStore, ObjectId, ProjectId, SearchRequest, SkobError, StoredObject,
};
use skob_index::IndexManager;

#[tokio::test]
async fn rebuild_searches_nested_keyword_and_title_then_drop() {
    let project = ProjectId::parse("p1").unwrap();
    let collection = CollectionId::parse("docs").unwrap();
    let store = MemoryBlobStore::new();
    put_object(
        &store,
        &project,
        &collection,
        "obj-city",
        json!({ "user": { "address": { "city": "portland" } } }),
    )
    .await;
    put_object(
        &store,
        &project,
        &collection,
        "obj-title",
        json!({ "title": "lighthouse" }),
    )
    .await;

    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let dir = std::env::temp_dir().join(format!("skob-index-{}-{nanos}", std::process::id()));
    let _cleanup = TempDir(dir.clone());
    let manager = IndexManager::open(store, dir, false).unwrap();
    let spec = IndexSpec::parse(
        "places",
        vec![
            IndexField::parse("user.address.city", FieldKind::Keyword).unwrap(),
            IndexField::parse("title", FieldKind::Text).unwrap(),
        ],
    )
    .unwrap();

    let building = manager
        .upsert_index(&project, &collection, spec)
        .await
        .unwrap();
    assert_eq!(building.status, IndexStatus::Building);

    manager.start();
    manager.wait_idle().await.unwrap();

    let city = manager
        .search(
            &project,
            &collection,
            SearchRequest {
                index: "places".into(),
                query: "portland".into(),
                field: Some("user.address.city".into()),
                limit: 10,
            },
        )
        .await
        .unwrap();
    assert_eq!(city.len(), 1);
    assert_eq!(city[0].object_id.as_str(), "obj-city");

    let title = manager
        .search(
            &project,
            &collection,
            SearchRequest {
                index: "places".into(),
                query: "lighthouse".into(),
                field: Some("title".into()),
                limit: 10,
            },
        )
        .await
        .unwrap();
    assert_eq!(title.len(), 1);
    assert_eq!(title[0].object_id.as_str(), "obj-title");

    manager
        .remove_index(&project, &collection, "places")
        .await
        .unwrap();
    manager.wait_idle().await.unwrap();
    assert!(manager
        .get_index(&project, &collection, "places")
        .await
        .unwrap()
        .is_none());
    let err = manager
        .search(
            &project,
            &collection,
            SearchRequest {
                index: "places".into(),
                query: "portland".into(),
                field: Some("user.address.city".into()),
                limit: 10,
            },
        )
        .await
        .unwrap_err();
    match err {
        SkobError::NotFound { entity, id } => {
            assert_eq!(entity, "index");
            assert_eq!(id, "places");
        }
        other => panic!("expected not found, got {other}"),
    }
}

async fn put_object(
    store: &MemoryBlobStore,
    project: &ProjectId,
    collection: &CollectionId,
    id: &str,
    body: serde_json::Value,
) {
    let object_id = ObjectId::parse(id).unwrap();
    let now = Utc::now();
    let stored = StoredObject {
        id: object_id.clone(),
        project_id: project.clone(),
        collection_id: collection.clone(),
        revision: 1,
        created_at: now,
        updated_at: now,
        body,
    };
    store
        .put(
            &object_key(project, collection, &object_id),
            to_json_bytes(&stored).unwrap(),
            Some("application/json"),
        )
        .await
        .unwrap();
}

struct TempDir(std::path::PathBuf);

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
