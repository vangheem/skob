use crate::ids::{CollectionId, ObjectId, ProjectId};

pub fn project_prefix() -> &'static str {
    "projects/"
}

pub fn project_meta_key(id: &ProjectId) -> String {
    format!("projects/{id}/meta.json")
}

pub fn project_tree_prefix(id: &ProjectId) -> String {
    format!("projects/{id}/")
}

pub fn collection_prefix(project: &ProjectId) -> String {
    format!("projects/{project}/collections/")
}

pub fn collection_meta_key(project: &ProjectId, collection: &CollectionId) -> String {
    format!("projects/{project}/collections/{collection}/meta.json")
}

pub fn collection_tree_prefix(project: &ProjectId, collection: &CollectionId) -> String {
    format!("projects/{project}/collections/{collection}/")
}

pub fn object_prefix(project: &ProjectId, collection: &CollectionId) -> String {
    format!("projects/{project}/collections/{collection}/objects/")
}

pub fn object_key(project: &ProjectId, collection: &CollectionId, object: &ObjectId) -> String {
    format!("projects/{project}/collections/{collection}/objects/{object}.json")
}

pub fn index_prefix(project: &ProjectId, collection: &CollectionId) -> String {
    format!("projects/{project}/collections/{collection}/indexes/")
}

pub fn index_tree_prefix(project: &ProjectId, collection: &CollectionId, name: &str) -> String {
    format!("projects/{project}/collections/{collection}/indexes/{name}/")
}

pub fn index_status_key(project: &ProjectId, collection: &CollectionId, name: &str) -> String {
    format!("projects/{project}/collections/{collection}/indexes/{name}/status.json")
}

pub fn index_segment_prefix(project: &ProjectId, collection: &CollectionId, name: &str) -> String {
    format!("projects/{project}/collections/{collection}/indexes/{name}/segments/")
}

/// `projects/{id}/meta.json` -> project id. Ignores collection and object keys.
pub fn parse_project_meta_key(key: &str) -> Option<ProjectId> {
    let rest = key.strip_prefix("projects/")?;
    let (id, tail) = rest.split_once('/')?;
    if tail != "meta.json" {
        return None;
    }
    ProjectId::parse(id).ok()
}

pub fn parse_collection_meta_key(key: &str) -> Option<(ProjectId, CollectionId)> {
    let rest = key.strip_prefix("projects/")?;
    let (project, rest) = rest.split_once("/collections/")?;
    let (collection, tail) = rest.split_once('/')?;
    if tail != "meta.json" {
        return None;
    }
    Some((ProjectId::parse(project).ok()?, CollectionId::parse(collection).ok()?))
}

pub fn parse_object_key(key: &str) -> Option<(ProjectId, CollectionId, ObjectId)> {
    let rest = key.strip_prefix("projects/")?;
    let (project, rest) = rest.split_once("/collections/")?;
    let (collection, rest) = rest.split_once("/objects/")?;
    let id = rest.strip_suffix(".json")?;
    if id.contains('/') {
        return None;
    }
    Some((
        ProjectId::parse(project).ok()?,
        CollectionId::parse(collection).ok()?,
        ObjectId::parse(id).ok()?,
    ))
}
