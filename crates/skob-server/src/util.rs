use axum::http::header;
use axum::http::HeaderMap;
use bytes::Bytes;
use serde::de::DeserializeOwned;
use serde::Deserialize;
use serde::Serialize;
use serde_json::json;
use serde_json::Map;
use serde_json::Value;
use skob_core::to_json_bytes;
use skob_core::BlobStore;
use skob_core::Collection;
use skob_core::CollectionId;
use skob_core::Project;
use skob_core::ProjectId;
use skob_core::Result;
use skob_core::SkobError;
use skob_core::StoredObject;
use skob_core::{collection_meta_key, parse_json_object, project_meta_key, require_json_content_type};

#[derive(Debug, Deserialize)]
pub struct NamedId {
    pub name: String,
    #[serde(default)]
    pub id: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct NameOnly {
    pub name: String,
}

#[derive(Debug, Deserialize)]
pub struct CursorQuery {
    pub cursor: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct IdQuery {
    pub id: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct IncludeQuery {
    pub include: Option<String>,
}

#[derive(Serialize)]
pub struct ListBody<T: Serialize> {
    pub items: Vec<T>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cursor: Option<String>,
}

pub fn read_json<T: DeserializeOwned>(headers: &HeaderMap, body: &[u8]) -> Result<T> {
    require_json(headers)?;
    serde_json::from_slice(body).map_err(|err| SkobError::InvalidJson(err.to_string()))
}

pub fn read_object(headers: &HeaderMap, body: &[u8]) -> Result<Map<String, Value>> {
    require_json(headers)?;
    parse_json_object(body)
}

fn require_json(headers: &HeaderMap) -> Result<()> {
    let content_type = headers.get(header::CONTENT_TYPE).and_then(|value| value.to_str().ok());
    require_json_content_type(content_type)
}

pub fn object_view(obj: &StoredObject) -> Value {
    json!({
        "id": obj.id,
        "revision": obj.revision,
        "created_at": obj.created_at,
        "updated_at": obj.updated_at,
        "body": obj.body,
    })
}

pub async fn put_json(blob: &dyn BlobStore, key: &str, value: &impl Serialize) -> Result<()> {
    let bytes = to_json_bytes(value)?;
    blob.put(key, bytes, Some("application/json")).await?;
    Ok(())
}

pub async fn load_json<T: DeserializeOwned>(blob: &dyn BlobStore, key: &str) -> Result<Option<T>> {
    match blob.get(key).await? {
        Some(bytes) => Ok(Some(decode(&bytes)?)),
        None => Ok(None),
    }
}

pub fn decode<T: DeserializeOwned>(bytes: &[u8]) -> Result<T> {
    serde_json::from_slice(bytes).map_err(SkobError::storage)
}

pub async fn require_project(blob: &dyn BlobStore, id: &ProjectId) -> Result<Project> {
    load_json(blob, &project_meta_key(id))
        .await?
        .ok_or_else(|| SkobError::NotFound {
            entity: "project",
            id: id.to_string(),
        })
}

pub async fn require_collection(
    blob: &dyn BlobStore,
    project: &ProjectId,
    collection: &CollectionId,
) -> Result<Collection> {
    load_json(blob, &collection_meta_key(project, collection))
        .await?
        .ok_or_else(|| SkobError::NotFound {
            entity: "collection",
            id: collection.to_string(),
        })
}

/// Page through blob keys until at least one parsed item is found, or the prefix is exhausted.
pub async fn list_matching<T>(
    blob: &dyn BlobStore,
    prefix: &str,
    cursor: Option<String>,
    mut accept: impl FnMut(&str, &Bytes) -> Result<Option<T>>,
) -> Result<(Vec<T>, Option<String>)> {
    let mut cursor = cursor;
    let mut items = Vec::new();
    let mut pages = 0u32;
    loop {
        let page = blob.list(prefix, cursor.as_deref()).await?;
        let next = page.cursor.clone();
        for meta in page.objects {
            let Some(bytes) = blob.get(&meta.key).await? else {
                continue;
            };
            if let Some(item) = accept(&meta.key, &bytes)? {
                items.push(item);
            }
        }
        pages += 1;
        if next.is_none() || !items.is_empty() || pages >= 32 {
            return Ok((items, next));
        }
        cursor = next;
    }
}
