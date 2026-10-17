use std::sync::Arc;

use axum::extract::Path;
use axum::extract::Query;
use axum::extract::State;
use axum::http::HeaderMap;
use axum::routing::get;
use axum::routing::post;
use axum::Json;
use axum::Router;
use bytes::Bytes;
use serde_json::json;
use serde_json::Value;
use skob_core::object_key;
use skob_core::validate_index_name;
use skob_core::CollectionId;
use skob_core::ObjectId;
use skob_core::ProjectId;
use skob_core::SearchRequest;
use skob_core::StoredObject;
use skob_core::{
    collection_prefix, object_prefix, parse_collection_meta_key, parse_object_key, parse_project_meta_key,
    project_prefix,
};

use crate::error::ApiError;
use crate::util::decode;
use crate::util::list_matching;
use crate::util::load_json;
use crate::util::object_view;
use crate::util::read_json;
use crate::util::require_collection;
use crate::util::require_project;
use crate::util::CursorQuery;
use crate::util::IncludeQuery;
use crate::util::ListBody;
use crate::AppState;

pub fn router() -> Router<Arc<AppState>> {
    Router::new()
        .route("/v1/projects", get(list_projects))
        .route("/v1/projects/{id}", get(get_project))
        .route("/v1/projects/{pid}/collections", get(list_collections))
        .route("/v1/projects/{pid}/collections/{cid}", get(get_collection))
        .route("/v1/projects/{pid}/collections/{cid}/objects", get(list_objects))
        .route("/v1/projects/{pid}/collections/{cid}/objects/{oid}", get(get_object))
        .route("/v1/projects/{pid}/collections/{cid}/indexes", get(list_indexes))
        .route("/v1/projects/{pid}/collections/{cid}/indexes/{name}", get(get_index))
        .route("/v1/projects/{pid}/collections/{cid}/query", post(query))
}

async fn list_projects(
    State(state): State<Arc<AppState>>,
    Query(query): Query<CursorQuery>,
) -> Result<Json<ListBody<skob_core::Project>>, ApiError> {
    let (items, cursor) = list_matching(state.blob.as_ref(), project_prefix(), query.cursor, |key, bytes| {
        if parse_project_meta_key(key).is_none() {
            return Ok(None);
        }
        decode(bytes).map(Some)
    })
    .await?;
    Ok(Json(ListBody { items, cursor }))
}

async fn get_project(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<Json<skob_core::Project>, ApiError> {
    let id = ProjectId::parse(id)?;
    Ok(Json(require_project(state.blob.as_ref(), &id).await?))
}

async fn list_collections(
    State(state): State<Arc<AppState>>,
    Path(pid): Path<String>,
    Query(query): Query<CursorQuery>,
) -> Result<Json<ListBody<skob_core::Collection>>, ApiError> {
    let project = ProjectId::parse(pid)?;
    require_project(state.blob.as_ref(), &project).await?;
    let (items, cursor) = list_matching(
        state.blob.as_ref(),
        &collection_prefix(&project),
        query.cursor,
        |key, bytes| {
            let Some((parsed_project, _)) = parse_collection_meta_key(key) else {
                return Ok(None);
            };
            if parsed_project != project {
                return Ok(None);
            }
            decode(bytes).map(Some)
        },
    )
    .await?;
    Ok(Json(ListBody { items, cursor }))
}

async fn get_collection(
    State(state): State<Arc<AppState>>,
    Path((pid, cid)): Path<(String, String)>,
) -> Result<Json<skob_core::Collection>, ApiError> {
    let project = ProjectId::parse(pid)?;
    let collection = CollectionId::parse(cid)?;
    Ok(Json(require_collection(state.blob.as_ref(), &project, &collection).await?))
}

async fn list_objects(
    State(state): State<Arc<AppState>>,
    Path((pid, cid)): Path<(String, String)>,
    Query(query): Query<CursorQuery>,
) -> Result<Json<ListBody<Value>>, ApiError> {
    let project = ProjectId::parse(pid)?;
    let collection = CollectionId::parse(cid)?;
    require_collection(state.blob.as_ref(), &project, &collection).await?;
    let (items, cursor) = list_matching(
        state.blob.as_ref(),
        &object_prefix(&project, &collection),
        query.cursor,
        |key, bytes| {
            let Some((parsed_project, parsed_collection, _)) = parse_object_key(key) else {
                return Ok(None);
            };
            if parsed_project != project || parsed_collection != collection {
                return Ok(None);
            }
            let stored: StoredObject = decode(bytes)?;
            Ok(Some(object_view(&stored)))
        },
    )
    .await?;
    Ok(Json(ListBody { items, cursor }))
}

async fn get_object(
    State(state): State<Arc<AppState>>,
    Path((pid, cid, oid)): Path<(String, String, String)>,
) -> Result<Json<Value>, ApiError> {
    let project = ProjectId::parse(pid)?;
    let collection = CollectionId::parse(cid)?;
    let object = ObjectId::parse(oid)?;
    let stored = load_object(state.blob.as_ref(), &project, &collection, &object).await?;
    Ok(Json(object_view(&stored)))
}

async fn list_indexes(
    State(state): State<Arc<AppState>>,
    Path((pid, cid)): Path<(String, String)>,
) -> Result<Json<ListBody<skob_core::IndexState>>, ApiError> {
    let project = ProjectId::parse(pid)?;
    let collection = CollectionId::parse(cid)?;
    require_collection(state.blob.as_ref(), &project, &collection).await?;
    let items = state.index.list_indexes(&project, &collection).await?;
    Ok(Json(ListBody { items, cursor: None }))
}

async fn get_index(
    State(state): State<Arc<AppState>>,
    Path((pid, cid, name)): Path<(String, String, String)>,
) -> Result<Json<skob_core::IndexState>, ApiError> {
    let project = ProjectId::parse(pid)?;
    let collection = CollectionId::parse(cid)?;
    validate_index_name(&name)?;
    require_collection(state.blob.as_ref(), &project, &collection).await?;
    let state = state
        .index
        .get_index(&project, &collection, &name)
        .await?
        .ok_or_else(|| skob_core::SkobError::NotFound {
            entity: "index",
            id: name,
        })?;
    Ok(Json(state))
}

async fn query(
    State(state): State<Arc<AppState>>,
    Path((pid, cid)): Path<(String, String)>,
    Query(include): Query<IncludeQuery>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Json<Value>, ApiError> {
    let project = ProjectId::parse(pid)?;
    let collection = CollectionId::parse(cid)?;
    require_collection(state.blob.as_ref(), &project, &collection).await?;
    let request: SearchRequest = read_json(&headers, &body)?;
    let request = request.normalized()?;
    let hits = state.index.search(&project, &collection, request).await?;
    let include_body = include.include.as_deref() == Some("body");
    let mut rendered = Vec::with_capacity(hits.len());
    for hit in hits {
        let mut value = serde_json::to_value(&hit).map_err(skob_core::SkobError::storage)?;
        if include_body {
            if let Ok(stored) = load_object(state.blob.as_ref(), &project, &collection, &hit.object_id).await {
                value["body"] = stored.body;
            }
            rendered.push(value);
        } else {
            rendered.push(value);
        }
    }
    Ok(Json(json!({ "hits": rendered })))
}

async fn load_object(
    blob: &dyn skob_core::BlobStore,
    project: &ProjectId,
    collection: &CollectionId,
    object: &ObjectId,
) -> skob_core::Result<StoredObject> {
    load_json(blob, &object_key(project, collection, object))
        .await?
        .ok_or_else(|| skob_core::SkobError::NotFound {
            entity: "object",
            id: object.to_string(),
        })
}
