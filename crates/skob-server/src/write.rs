use std::sync::Arc;

use axum::extract::Path;
use axum::extract::Query;
use axum::extract::State;
use axum::http::HeaderMap;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::delete;
use axum::routing::get;
use axum::routing::post;
use axum::routing::put;
use axum::Json;
use axum::Router;
use bytes::Bytes;
use chrono::Utc;
use serde::Deserialize;
use serde_json::json;
use serde_json::Value;
use skob_core::collection_meta_key;
use skob_core::collection_tree_prefix;
use skob_core::object_key;
use skob_core::project_meta_key;
use skob_core::project_tree_prefix;
use skob_core::validate_index_name;
use skob_core::Collection;
use skob_core::CollectionId;
use skob_core::IndexSpec;
use skob_core::ObjectId;
use skob_core::Project;
use skob_core::ProjectId;
use skob_core::SkobError;
use skob_core::StoredObject;

use crate::error::ApiError;
use crate::util::load_json;
use crate::util::object_view;
use crate::util::put_json;
use crate::util::read_json;
use crate::util::read_object;
use crate::util::require_collection;
use crate::util::require_project;
use crate::util::IdQuery;
use crate::util::NameOnly;
use crate::util::NamedId;
use crate::AppState;

pub fn router() -> Router<Arc<AppState>> {
    Router::new()
        .route("/v1/projects", post(create_project))
        .route("/v1/projects/{id}", put(update_project).delete(delete_project))
        .route("/v1/projects/{pid}/collections", post(create_collection))
        .route(
            "/v1/projects/{pid}/collections/{cid}",
            put(update_collection).delete(delete_collection),
        )
        .route("/v1/projects/{pid}/collections/{cid}/objects", post(create_object))
        .route(
            "/v1/projects/{pid}/collections/{cid}/objects/{oid}",
            put(put_object).delete(delete_object),
        )
        .route("/v1/projects/{pid}/collections/{cid}/indexes", post(create_index))
        .route(
            "/v1/projects/{pid}/collections/{cid}/indexes/{name}",
            delete(delete_index),
        )
        .route("/v1/raft/metrics", get(raft_metrics))
        .route("/v1/raft/join", post(join_voter))
        .route("/v1/raft/rebalance", post(rebalance))
        .route("/internal/raft/append", post(raft_append))
        .route("/internal/raft/vote", post(raft_vote))
        .route("/internal/raft/snapshot", post(raft_snapshot))
}

async fn raft_metrics(State(state): State<Arc<AppState>>) -> Result<Json<Value>, ApiError> {
    Ok(Json(raft_of(&state)?.metrics_json().await))
}

#[derive(Deserialize)]
struct JoinBody {
    node_id: u64,
    advertise_addr: String,
}

async fn join_voter(State(state): State<Arc<AppState>>, Json(body): Json<JoinBody>) -> Result<Json<Value>, ApiError> {
    let raft = raft_of(&state)?;
    raft.add_voter(body.node_id, body.advertise_addr).await?;
    raft.rebalance().await?;
    Ok(Json(raft.metrics_json().await))
}

async fn rebalance(State(state): State<Arc<AppState>>) -> Result<Json<Value>, ApiError> {
    let raft = raft_of(&state)?;
    raft.rebalance().await?;
    Ok(Json(raft.metrics_json().await))
}

async fn raft_append(State(state): State<Arc<AppState>>, body: Bytes) -> Result<impl IntoResponse, ApiError> {
    raft_bytes(raft_of(&state)?.handle_append(&body).await?)
}

async fn raft_vote(State(state): State<Arc<AppState>>, body: Bytes) -> Result<impl IntoResponse, ApiError> {
    raft_bytes(raft_of(&state)?.handle_vote(&body).await?)
}

async fn raft_snapshot(State(state): State<Arc<AppState>>, body: Bytes) -> Result<impl IntoResponse, ApiError> {
    raft_bytes(raft_of(&state)?.handle_snapshot(&body).await?)
}

fn raft_bytes(body: Vec<u8>) -> Result<impl IntoResponse, ApiError> {
    Ok((
        [(axum::http::header::CONTENT_TYPE, "application/json")],
        body,
    ))
}

fn raft_of(state: &AppState) -> Result<&Arc<skob_raft::SegmentCoordinator>, ApiError> {
    state
        .raft
        .as_ref()
        .ok_or_else(|| SkobError::raft("writer is not joined to raft").into())
}

async fn create_project(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<(StatusCode, Json<Project>), ApiError> {
    owns_catalog(&state).await?;
    let input: NamedId = read_json(&headers, &body)?;
    let id = match input.id {
        Some(raw) => ProjectId::parse(raw)?,
        None => ProjectId::generate(),
    };
    if load_json::<Project>(state.blob.as_ref(), &project_meta_key(&id))
        .await?
        .is_some()
    {
        return Err(SkobError::AlreadyExists {
            entity: "project",
            id: id.to_string(),
        }
        .into());
    }
    let project = Project::new(id, input.name)?;
    put_json(state.blob.as_ref(), &project_meta_key(&project.id), &project).await?;
    Ok((StatusCode::CREATED, Json(project)))
}

async fn update_project(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Json<Project>, ApiError> {
    owns_catalog(&state).await?;
    let id = ProjectId::parse(id)?;
    let existing = require_project(state.blob.as_ref(), &id).await?;
    let input: NameOnly = read_json(&headers, &body)?;
    let mut project = Project::new(existing.id.clone(), input.name)?;
    project.created_at = existing.created_at;
    project.updated_at = Utc::now();
    put_json(state.blob.as_ref(), &project_meta_key(&project.id), &project).await?;
    Ok(Json(project))
}

async fn delete_project(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    owns_catalog(&state).await?;
    let id = ProjectId::parse(id)?;
    require_project(state.blob.as_ref(), &id).await?;
    state.blob.delete_prefix(&project_tree_prefix(&id)).await?;
    state.index.forget_local(&id, None).await?;
    Ok(deleted())
}

async fn create_collection(
    State(state): State<Arc<AppState>>,
    Path(pid): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<(StatusCode, Json<Collection>), ApiError> {
    owns_catalog(&state).await?;
    let project = ProjectId::parse(pid)?;
    require_project(state.blob.as_ref(), &project).await?;
    let input: NamedId = read_json(&headers, &body)?;
    let id = match input.id {
        Some(raw) => CollectionId::parse(raw)?,
        None => CollectionId::generate(),
    };
    if load_json::<Collection>(state.blob.as_ref(), &collection_meta_key(&project, &id))
        .await?
        .is_some()
    {
        return Err(SkobError::AlreadyExists {
            entity: "collection",
            id: id.to_string(),
        }
        .into());
    }
    let collection = Collection::new(id, project.clone(), input.name)?;
    put_json(
        state.blob.as_ref(),
        &collection_meta_key(&project, &collection.id),
        &collection,
    )
    .await?;
    Ok((StatusCode::CREATED, Json(collection)))
}

async fn update_collection(
    State(state): State<Arc<AppState>>,
    Path((pid, cid)): Path<(String, String)>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Json<Collection>, ApiError> {
    owns_catalog(&state).await?;
    let project = ProjectId::parse(pid)?;
    let collection_id = CollectionId::parse(cid)?;
    let existing = require_collection(state.blob.as_ref(), &project, &collection_id).await?;
    let input: NameOnly = read_json(&headers, &body)?;
    let mut collection = Collection::new(existing.id.clone(), existing.project_id.clone(), input.name)?;
    collection.created_at = existing.created_at;
    collection.indexes = existing.indexes;
    collection.updated_at = Utc::now();
    put_json(
        state.blob.as_ref(),
        &collection_meta_key(&project, &collection.id),
        &collection,
    )
    .await?;
    Ok(Json(collection))
}

async fn delete_collection(
    State(state): State<Arc<AppState>>,
    Path((pid, cid)): Path<(String, String)>,
) -> Result<Json<Value>, ApiError> {
    owns_catalog(&state).await?;
    let project = ProjectId::parse(pid)?;
    let collection = CollectionId::parse(cid)?;
    require_collection(state.blob.as_ref(), &project, &collection).await?;
    state
        .blob
        .delete_prefix(&collection_tree_prefix(&project, &collection))
        .await?;
    state.index.forget_local(&project, Some(&collection)).await?;
    Ok(deleted())
}

async fn create_object(
    State(state): State<Arc<AppState>>,
    Path((pid, cid)): Path<(String, String)>,
    Query(id_query): Query<IdQuery>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<(StatusCode, Json<Value>), ApiError> {
    let project = ProjectId::parse(pid)?;
    let collection = CollectionId::parse(cid)?;
    let id = match id_query.id {
        Some(raw) => ObjectId::parse(raw)?,
        None => ObjectId::generate(),
    };
    owns_object(&state, &id).await?;
    require_collection(state.blob.as_ref(), &project, &collection).await?;
    let key = object_key(&project, &collection, &id);
    if load_json::<StoredObject>(state.blob.as_ref(), &key).await?.is_some() {
        return Err(SkobError::AlreadyExists {
            entity: "object",
            id: id.to_string(),
        }
        .into());
    }
    let user = read_object(&headers, &body)?;
    let now = Utc::now();
    let stored = StoredObject {
        id: id.clone(),
        project_id: project.clone(),
        collection_id: collection.clone(),
        revision: 1,
        created_at: now,
        updated_at: now,
        body: Value::Object(user),
    };
    put_json(state.blob.as_ref(), &key, &stored).await?;
    state
        .index
        .sync_object(&project, &collection, &id, &stored.body)
        .await?;
    Ok((StatusCode::CREATED, Json(object_view(&stored))))
}

async fn put_object(
    State(state): State<Arc<AppState>>,
    Path((pid, cid, oid)): Path<(String, String, String)>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<(StatusCode, Json<Value>), ApiError> {
    let project = ProjectId::parse(pid)?;
    let collection = CollectionId::parse(cid)?;
    let id = ObjectId::parse(oid)?;
    owns_object(&state, &id).await?;
    require_collection(state.blob.as_ref(), &project, &collection).await?;
    let user = read_object(&headers, &body)?;
    let key = object_key(&project, &collection, &id);
    let existing = load_json::<StoredObject>(state.blob.as_ref(), &key).await?;
    let now = Utc::now();
    let (status, stored) = if let Some(existing) = existing {
        (
            StatusCode::OK,
            StoredObject {
                revision: existing.revision.saturating_add(1),
                updated_at: now,
                body: Value::Object(user),
                ..existing
            },
        )
    } else {
        (
            StatusCode::CREATED,
            StoredObject {
                id: id.clone(),
                project_id: project.clone(),
                collection_id: collection.clone(),
                revision: 1,
                created_at: now,
                updated_at: now,
                body: Value::Object(user),
            },
        )
    };
    put_json(state.blob.as_ref(), &key, &stored).await?;
    state
        .index
        .sync_object(&project, &collection, &id, &stored.body)
        .await?;
    Ok((status, Json(object_view(&stored))))
}

async fn delete_object(
    State(state): State<Arc<AppState>>,
    Path((pid, cid, oid)): Path<(String, String, String)>,
) -> Result<Json<Value>, ApiError> {
    let project = ProjectId::parse(pid)?;
    let collection = CollectionId::parse(cid)?;
    let id = ObjectId::parse(oid)?;
    owns_object(&state, &id).await?;
    require_collection(state.blob.as_ref(), &project, &collection).await?;
    let key = object_key(&project, &collection, &id);
    if load_json::<StoredObject>(state.blob.as_ref(), &key).await?.is_none() {
        return Err(SkobError::NotFound {
            entity: "object",
            id: id.to_string(),
        }
        .into());
    }
    state.blob.delete(&key).await?;
    state.index.remove_object(&project, &collection, &id).await?;
    Ok(deleted())
}

async fn create_index(
    State(state): State<Arc<AppState>>,
    Path((pid, cid)): Path<(String, String)>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<(StatusCode, Json<skob_core::IndexState>), ApiError> {
    owns_catalog(&state).await?;
    let project = ProjectId::parse(pid)?;
    let collection_id = CollectionId::parse(cid)?;
    let mut collection = require_collection(state.blob.as_ref(), &project, &collection_id).await?;
    let raw: IndexSpec = read_json(&headers, &body)?;
    let spec = IndexSpec::parse(raw.name, raw.fields)?;
    let index_state = state
        .index
        .upsert_index(&project, &collection_id, spec.clone())
        .await?;
    if let Some(slot) = collection.indexes.iter_mut().find(|existing| existing.name == spec.name) {
        *slot = spec;
    } else {
        collection.indexes.push(spec);
    }
    collection.updated_at = Utc::now();
    put_json(
        state.blob.as_ref(),
        &collection_meta_key(&project, &collection_id),
        &collection,
    )
    .await?;
    Ok((StatusCode::CREATED, Json(index_state)))
}

async fn delete_index(
    State(state): State<Arc<AppState>>,
    Path((pid, cid, name)): Path<(String, String, String)>,
) -> Result<Json<Value>, ApiError> {
    owns_catalog(&state).await?;
    let project = ProjectId::parse(pid)?;
    let collection_id = CollectionId::parse(cid)?;
    validate_index_name(&name)?;
    let mut collection = require_collection(state.blob.as_ref(), &project, &collection_id).await?;
    let had_spec = collection.indexes.iter().any(|spec| spec.name == name);
    let manager_state = state.index.get_index(&project, &collection_id, &name).await?;
    if !had_spec && manager_state.is_none() {
        return Err(SkobError::NotFound {
            entity: "index",
            id: name,
        }
        .into());
    }
    if let Err(err) = state.index.remove_index(&project, &collection_id, &name).await {
        if err.code() != "not_found" || !had_spec {
            return Err(err.into());
        }
    }
    collection.indexes.retain(|spec| spec.name != name);
    collection.updated_at = Utc::now();
    put_json(
        state.blob.as_ref(),
        &collection_meta_key(&project, &collection_id),
        &collection,
    )
    .await?;
    Ok(deleted())
}

async fn owns_catalog(state: &AppState) -> Result<(), ApiError> {
    let raft = state
        .raft
        .as_ref()
        .ok_or_else(|| SkobError::raft("writer is not joined to raft"))?;
    raft.ensure_owns_catalog().await?;
    Ok(())
}

async fn owns_object(state: &AppState, object_id: &ObjectId) -> Result<(), ApiError> {
    let raft = state
        .raft
        .as_ref()
        .ok_or_else(|| SkobError::raft("writer is not joined to raft"))?;
    raft.ensure_owns_object(object_id).await?;
    Ok(())
}

fn deleted() -> Json<Value> {
    Json(json!({ "deleted": true }))
}
