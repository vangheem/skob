use std::sync::Arc;

use axum::body::Body;
use axum::http::Request;
use axum::http::StatusCode;
use http_body_util::BodyExt;
use serde_json::json;
use serde_json::Value;
use skob_core::Role;
use skob_raft::RaftConfig;
use skob_raft::SegmentCoordinator;
use skob_server::router;
use skob_server::AppState;
use skob_storage::StorageConfig;
use tower::ServiceExt;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn crud_round_trip() {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let root = std::env::temp_dir().join(format!("skob-server-{nanos}"));
    std::fs::create_dir_all(root.join("indexes")).unwrap();
    std::fs::create_dir_all(root.join("raft")).unwrap();

    let blob = skob_storage::open(StorageConfig::Memory).await.expect("memory blob");
    let index = skob_index::IndexManager::open(blob.clone(), root.join("indexes"), false).expect("index");
    index.start();
    let raft = SegmentCoordinator::bootstrap(RaftConfig::new(1, "http://127.0.0.1:8080", root.join("raft"), 16))
    .await
    .expect("raft");

    let state = Arc::new(AppState {
        blob,
        index,
        raft: Some(raft),
        role: Role::All,
    });
    let app = router(state.clone());

    let (status, project) = send(
        &app,
        json_req("POST", "/v1/projects", Some(json!({"name": "Demo", "id": "p1"}))),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "{project}");
    assert_eq!(project["id"], "p1");

    let (status, collection) = send(
        &app,
        json_req(
            "POST",
            "/v1/projects/p1/collections",
            Some(json!({"name": "Docs", "id": "c1"})),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "{collection}");
    assert_eq!(collection["id"], "c1");

    let body = json!({
        "title": "harbor",
        "user": { "address": { "city": "portland" } }
    });
    let (status, created) = send(
        &app,
        json_req("PUT", "/v1/projects/p1/collections/c1/objects/obj1", Some(body.clone())),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "{created}");
    assert_eq!(created["id"], "obj1");
    assert_eq!(created["revision"], 1);
    assert_eq!(created["body"], body);

    let (status, index_state) = send(
        &app,
        json_req(
            "POST",
            "/v1/projects/p1/collections/c1/indexes",
            Some(json!({
                "name": "main",
                "fields": [
                    {"path": "user.address.city", "kind": "text"},
                    {"path": "missing_field", "kind": "keyword"}
                ]
            })),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "{index_state}");
    assert_eq!(index_state["spec"]["name"], "main");

    state.index.wait_idle().await.expect("index idle");

    let (status, query) = send(
        &app,
        json_req(
            "POST",
            "/v1/projects/p1/collections/c1/query",
            Some(json!({
                "index": "main",
                "query": "portland",
                "field": "user.address.city"
            })),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{query}");
    let hits = query["hits"].as_array().expect("hits");
    assert!(
        hits.iter().any(|hit| hit["object_id"] == "obj1"),
        "expected obj1 in {query}"
    );

    let (status, fetched) = send(
        &app,
        json_req("GET", "/v1/projects/p1/collections/c1/objects/obj1", None),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{fetched}");
    assert_eq!(fetched["body"], body);
    assert_eq!(fetched["revision"], 1);
    assert!(fetched["body"].get("missing_field").is_none());

    for uri in [
        "/v1/projects/p1/collections/c1/objects/obj1",
        "/v1/projects/p1/collections/c1",
        "/v1/projects/p1",
    ] {
        let (status, deleted) = send(&app, json_req("DELETE", uri, None)).await;
        assert_eq!(status, StatusCode::OK, "{uri} {deleted}");
        assert_eq!(deleted["deleted"], true, "{uri}");
    }

    let (status, missing) = send(&app, json_req("GET", "/v1/projects/p1", None)).await;
    assert_eq!(status, StatusCode::NOT_FOUND, "{missing}");

    let _ = std::fs::remove_dir_all(root);
}

fn json_req(method: &str, uri: &str, body: Option<Value>) -> Request<Body> {
    let mut builder = Request::builder().method(method).uri(uri);
    let body = if let Some(body) = body {
        builder = builder.header("content-type", "application/json");
        Body::from(serde_json::to_vec(&body).unwrap())
    } else {
        Body::empty()
    };
    builder.body(body).unwrap()
}

async fn send(app: &axum::Router, request: Request<Body>) -> (StatusCode, Value) {
    let response = app.clone().oneshot(request).await.expect("router");
    let status = response.status();
    let bytes = response.into_body().collect().await.expect("body").to_bytes();
    let value = if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes).unwrap_or_else(|err| {
            json!({
                "raw": String::from_utf8_lossy(&bytes),
                "parse_error": err.to_string(),
            })
        })
    };
    (status, value)
}
