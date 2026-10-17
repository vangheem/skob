use std::sync::Arc;
use std::time::Duration;

use skob_core::to_json_bytes;
use skob_core::Collection;
use skob_core::CollectionId;
use skob_core::Project;
use skob_core::ProjectId;
use skob_core::Role;
use skob_core::{collection_meta_key, project_meta_key};
use skob_raft::RaftConfig;
use skob_raft::SegmentCoordinator;
use skob_server::execute_sql;
use skob_server::serve_postgres_listener;
use skob_server::AppState;
use skob_server::SqlResult;
use skob_storage::StorageConfig;
use tokio::net::TcpListener;
use tokio_postgres::NoTls;
use tokio_postgres::SimpleQueryMessage;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn insert_update_select_round_trip() {
    let (state, _root) = app().await;
    let inserted = execute_sql(
        &state,
        "INSERT INTO docs (id, title, \"user.address.city\") VALUES ('obj1', 'harbor', 'portland')",
        Some("p1"),
    )
    .await
    .expect("insert");
    assert_eq!(
        inserted,
        vec![SqlResult::Command {
            tag: "INSERT",
            rows: 1
        }]
    );

    let updated = execute_sql(
        &state,
        "UPDATE docs SET title = 'pier', \"user.address.city\" = 'seattle' WHERE id = 'obj1'",
        Some("p1"),
    )
    .await
    .expect("update");
    assert_eq!(
        updated,
        vec![SqlResult::Command {
            tag: "UPDATE",
            rows: 1
        }]
    );

    let selected = execute_sql(
        &state,
        "SELECT id, title, \"user.address.city\" FROM docs WHERE id = 'obj1'",
        Some("p1"),
    )
    .await
    .expect("select");
    let SqlResult::Rows(table) = &selected[0] else {
        panic!("expected rows, got {selected:?}");
    };
    assert_eq!(table.columns, vec!["id", "title", "user.address.city"]);
    assert_eq!(
        table.rows,
        vec![vec![
            Some("obj1".into()),
            Some("pier".into()),
            Some("seattle".into())
        ]]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn postgres_wire_insert_update_select() {
    let (state, _root) = app().await;
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let port = listener.local_addr().expect("addr").port();
    tokio::spawn(async move {
        let _ = serve_postgres_listener(state, listener).await;
    });

    let config = format!("host=127.0.0.1 port={port} user=skob dbname=p1 sslmode=disable");
    let (client, connection) = tokio::time::timeout(Duration::from_secs(5), tokio_postgres::connect(&config, NoTls))
        .await
        .expect("connect timeout")
        .expect("connect");
    tokio::spawn(async move {
        let _ = connection.await;
    });

    client
        .batch_execute("INSERT INTO docs (id, title) VALUES ('wire1', 'harbor')")
        .await
        .expect("wire insert");
    client
        .batch_execute("UPDATE docs SET title = 'pier' WHERE id = 'wire1'")
        .await
        .expect("wire update");
    let messages = client
        .simple_query("SELECT id, title FROM docs WHERE id = 'wire1'")
        .await
        .expect("wire select");
    let row = messages.iter().find_map(|message| match message {
        SimpleQueryMessage::Row(row) => Some(row),
        _ => None,
    });
    let row = row.expect("row");
    assert_eq!(row.get(0), Some("wire1"));
    assert_eq!(row.get(1), Some("pier"));
}

async fn app() -> (Arc<AppState>, std::path::PathBuf) {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let root = std::env::temp_dir().join(format!("skob-sql-{nanos}"));
    std::fs::create_dir_all(root.join("indexes")).unwrap();
    std::fs::create_dir_all(root.join("raft")).unwrap();

    let blob = skob_storage::open(StorageConfig::Memory).await.expect("memory blob");
    let project = Project::new(ProjectId::parse("p1").unwrap(), "Demo").unwrap();
    let collection = Collection::new(CollectionId::parse("docs").unwrap(), project.id.clone(), "Docs").unwrap();
    blob.put(
        &project_meta_key(&project.id),
        to_json_bytes(&project).unwrap(),
        Some("application/json"),
    )
    .await
    .unwrap();
    blob.put(
        &collection_meta_key(&project.id, &collection.id),
        to_json_bytes(&collection).unwrap(),
        Some("application/json"),
    )
    .await
    .unwrap();

    let index = skob_index::IndexManager::open(blob.clone(), root.join("indexes"), false).expect("index");
    index.start();
    let raft = SegmentCoordinator::bootstrap(RaftConfig {
        node_id: 1,
        advertise_addr: "http://127.0.0.1:8080".into(),
        data_dir: root.join("raft"),
        segment_count: 16,
    })
    .await
    .expect("raft");
    (
        Arc::new(AppState {
            blob,
            index,
            raft: Some(raft),
            role: Role::All,
        }),
        root,
    )
}
