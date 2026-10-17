//! HTTP read and write routers for the `skob` process.

mod error;
mod pg;
mod read;
mod sql;
mod util;
mod write;

use std::path::PathBuf;
use std::sync::Arc;

use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::get;
use axum::Json;
use axum::Router;
use serde_json::json;
use serde_json::Value;
use skob_core::BlobStore;
use skob_core::Result;
use skob_core::Role;
use skob_core::SkobError;
use skob_index::IndexManager;
use skob_raft::RaftConfig;
use skob_raft::SegmentCoordinator;
use skob_storage::S3Config;
use skob_storage::StorageConfig;
use tower_http::trace::TraceLayer;

pub use error::ApiError;
pub use pg::run_listener as serve_postgres_listener;
pub use sql::execute_sql;
pub use sql::SqlResult;
pub use sql::SqlTable;

/// Shared process state. Tests construct this and call [`router`].
pub struct AppState {
    pub blob: Arc<dyn BlobStore>,
    pub index: Arc<IndexManager>,
    pub raft: Option<Arc<SegmentCoordinator>>,
    pub role: Role,
}

/// Flags for `skob serve`, already parsed from clap / environment.
#[derive(Clone, Debug)]
pub struct ServeConfig {
    pub role: Role,
    pub bind: String,
    pub data_dir: PathBuf,
    pub node_id: u64,
    pub advertise: String,
    pub blob: String,
    pub s3_endpoint: Option<String>,
    pub s3_bucket: String,
    pub s3_region: String,
    pub s3_access_key: Option<String>,
    pub s3_secret_key: Option<String>,
    pub s3_path_style: bool,
    pub segment_count: u32,
}

/// Open blob storage, start the index worker, and join Raft when this role writes.
pub async fn build(config: ServeConfig) -> Result<Arc<AppState>> {
    std::fs::create_dir_all(&config.data_dir).map_err(SkobError::storage)?;
    std::fs::create_dir_all(config.data_dir.join("indexes")).map_err(SkobError::storage)?;
    let blob = skob_storage::open(storage_config(&config)?).await?;
    let read_only = matches!(config.role, Role::Reader);
    let index = IndexManager::open(blob.clone(), config.data_dir.join("indexes"), read_only)?;
    index.start();
    let raft = if config.role.serves_writes() {
        Some(
            SegmentCoordinator::bootstrap(RaftConfig {
                node_id: config.node_id,
                advertise_addr: config.advertise.clone(),
                data_dir: config.data_dir.join("raft"),
                segment_count: config.segment_count,
            })
            .await?,
        )
    } else {
        None
    };
    Ok(Arc::new(AppState {
        blob,
        index,
        raft,
        role: config.role,
    }))
}

fn storage_config(config: &ServeConfig) -> Result<StorageConfig> {
    match config.blob.as_str() {
        "memory" => Ok(StorageConfig::Memory),
        "fs" => Ok(StorageConfig::Fs {
            root: config.data_dir.join("blobs"),
        }),
        "s3" => {
            let access_key = config
                .s3_access_key
                .clone()
                .filter(|value| !value.is_empty())
                .ok_or_else(|| SkobError::BadRequest("s3 blob requires --s3-access-key / SKOB_S3_ACCESS_KEY".into()))?;
            let secret_key = config
                .s3_secret_key
                .clone()
                .filter(|value| !value.is_empty())
                .ok_or_else(|| SkobError::BadRequest("s3 blob requires --s3-secret-key / SKOB_S3_SECRET_KEY".into()))?;
            Ok(StorageConfig::S3(S3Config {
                endpoint: config.s3_endpoint.clone(),
                region: config.s3_region.clone(),
                bucket: config.s3_bucket.clone(),
                access_key,
                secret_key,
                path_style: config.s3_path_style,
                prefix: String::new(),
            }))
        }
        other => Err(SkobError::BadRequest(format!(
            "unknown blob `{other}`, expected memory|fs|s3"
        ))),
    }
}

/// Mount the read router, the write router, or both, depending on `state.role`.
pub fn router(state: Arc<AppState>) -> Router {
    let mut app = Router::new().route("/health", get(health));
    if state.role.serves_reads() {
        app = app.merge(read::router());
    }
    if state.role.serves_writes() {
        app = app.merge(write::router());
    }
    app.method_not_allowed_fallback(method_not_allowed)
        .fallback(not_found)
        .layer(TraceLayer::new_for_http())
        .with_state(state)
}

/// Bind the HTTP API and, unless disabled, the PostgreSQL wire listener.
pub async fn serve(state: Arc<AppState>, bind: &str, pg_bind: Option<&str>) -> Result<()> {
    if let Some(pg_bind) = pg_bind {
        let listener = tokio::net::TcpListener::bind(pg_bind).await.map_err(SkobError::storage)?;
        let pg_state = state.clone();
        tokio::spawn(async move {
            if let Err(err) = pg::run_listener(pg_state, listener).await {
                tracing::error!(error = %err, "postgres listener stopped");
            }
        });
        tracing::info!(%pg_bind, "postgres wire listening");
    }
    let listener = tokio::net::TcpListener::bind(bind).await.map_err(SkobError::storage)?;
    tracing::info!(%bind, role = state.role.as_str(), "skob listening");
    axum::serve(listener, router(state))
        .await
        .map_err(SkobError::storage)?;
    Ok(())
}

async fn health(State(state): State<Arc<AppState>>) -> Json<Value> {
    Json(json!({
        "status": "ok",
        "role": state.role.as_str(),
    }))
}

async fn not_found() -> (StatusCode, Json<Value>) {
    (
        StatusCode::NOT_FOUND,
        Json(json!({
            "error": "not_found",
            "message": "route not found",
        })),
    )
}

async fn method_not_allowed() -> (StatusCode, Json<Value>) {
    (
        StatusCode::METHOD_NOT_ALLOWED,
        Json(json!({
            "error": "bad_request",
            "message": "method not allowed",
        })),
    )
}
