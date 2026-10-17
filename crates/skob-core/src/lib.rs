//! Shared types for skob. Storage, indexing, raft, and the HTTP process all
//! depend on this crate and must not invent parallel models.

mod blob;
mod error;
mod ids;
mod jsonutil;
mod keys;
mod memory;
mod model;
mod segment;

pub use blob::{BlobMeta, BlobStore, ListPage};
pub use error::{Result, SkobError};
pub use ids::{
    validate_field_path, validate_index_name, validate_name, CollectionId, ObjectId, ProjectId,
};
pub use jsonutil::{parse_json_object, require_json_content_type, to_json_bytes, values_at_path};
pub use keys::{
    collection_meta_key, collection_prefix, collection_tree_prefix, index_prefix, index_segment_prefix,
    index_status_key, index_tree_prefix, object_key, object_prefix, parse_collection_meta_key,
    parse_object_key, parse_project_meta_key, project_meta_key, project_prefix, project_tree_prefix,
};
pub use memory::MemoryBlobStore;
pub use model::{
    Collection, FieldKind, IndexField, IndexSpec, IndexState, IndexStatus, Project, Role, SearchHit,
    SearchRequest, SegmentOwner, StoredObject,
};
pub use segment::{segment_for, CATALOG_SEGMENT, DEFAULT_SEGMENT_COUNT};

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn nested_paths_fan_out_and_missing_fields_are_empty() {
        let doc = json!({
            "title": "harbor",
            "tags": ["a", "b"],
            "user": {
                "address": { "city": "portland" },
                "aliases": [{ "name": "pdx" }, { "name": "rose" }]
            }
        });

        let cities: Vec<String> = values_at_path(&doc, "user.address.city")
            .into_iter()
            .map(|v| v.as_str().unwrap().to_string())
            .collect();
        assert_eq!(cities, vec!["portland".to_string()]);

        let aliases: Vec<String> = values_at_path(&doc, "user.aliases.name")
            .into_iter()
            .map(|v| v.as_str().unwrap().to_string())
            .collect();
        assert_eq!(aliases, vec!["pdx".to_string(), "rose".to_string()]);

        let tags: Vec<String> = values_at_path(&doc, "tags")
            .into_iter()
            .map(|v| v.as_str().unwrap().to_string())
            .collect();
        assert_eq!(tags, vec!["a".to_string(), "b".to_string()]);

        assert!(values_at_path(&doc, "user.address.zip").is_empty());
        assert!(values_at_path(&doc, "missing.nested").is_empty());
    }

    #[test]
    fn object_segments_are_stable_and_catalog_is_fixed() {
        let id = ObjectId::parse("obj-1").unwrap();
        let first = segment_for(&id, DEFAULT_SEGMENT_COUNT);
        assert_eq!(first, segment_for(&id, DEFAULT_SEGMENT_COUNT));
        assert!(first < DEFAULT_SEGMENT_COUNT);
        assert_eq!(CATALOG_SEGMENT, 0);

        let bytes = br#"{"name":"docs"}"#;
        let parsed = parse_json_object(bytes).unwrap();
        assert_eq!(parsed.get("name").and_then(|v| v.as_str()), Some("docs"));
        assert!(parse_json_object(b"[1,2]").is_err());
        assert!(require_json_content_type(Some("application/json; charset=utf-8")).is_ok());
        assert!(require_json_content_type(Some("text/plain")).is_err());
    }
}
