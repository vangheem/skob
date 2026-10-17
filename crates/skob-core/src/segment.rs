use sha2::{Digest, Sha256};

use crate::ids::ObjectId;

/// Catalog mutations (projects, collections, index config) always hash to this segment.
pub const CATALOG_SEGMENT: u32 = 0;

pub const DEFAULT_SEGMENT_COUNT: u32 = 256;

/// Stable write-segment for an object. The same id always maps to the same segment.
pub fn segment_for(object_id: &ObjectId, segment_count: u32) -> u32 {
    assert!(segment_count > 0, "segment_count must be non-zero");
    let digest = Sha256::digest(object_id.as_str().as_bytes());
    let n = u32::from_be_bytes([digest[0], digest[1], digest[2], digest[3]]);
    n % segment_count
}
