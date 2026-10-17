use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::error::{Result, SkobError};
use crate::ids::{validate_field_path, validate_index_name, validate_name, CollectionId, ObjectId, ProjectId};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Role {
    All,
    Reader,
    Writer,
}

impl Role {
    pub fn parse(raw: &str) -> Result<Self> {
        match raw {
            "all" => Ok(Self::All),
            "reader" => Ok(Self::Reader),
            "writer" => Ok(Self::Writer),
            other => Err(SkobError::BadRequest(format!(
                "unknown role `{other}`, expected all|reader|writer"
            ))),
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::All => "all",
            Self::Reader => "reader",
            Self::Writer => "writer",
        }
    }

    pub fn serves_reads(self) -> bool {
        matches!(self, Self::All | Self::Reader)
    }

    pub fn serves_writes(self) -> bool {
        matches!(self, Self::All | Self::Writer)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FieldKind {
    Text,
    Keyword,
    I64,
    U64,
    F64,
    Bool,
    Datetime,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IndexField {
    pub path: String,
    pub kind: FieldKind,
}

impl IndexField {
    pub fn parse(path: impl Into<String>, kind: FieldKind) -> Result<Self> {
        let path = path.into();
        validate_field_path(&path)?;
        Ok(Self { path, kind })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IndexSpec {
    pub name: String,
    pub fields: Vec<IndexField>,
}

impl IndexSpec {
    pub fn parse(name: impl Into<String>, fields: Vec<IndexField>) -> Result<Self> {
        let name = name.into();
        validate_index_name(&name)?;
        if fields.is_empty() {
            return Err(SkobError::InvalidName {
                kind: "index",
                value: name,
                reason: "must declare at least one field",
            });
        }
        Ok(Self { name, fields })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IndexStatus {
    Building,
    Ready,
    Failed,
    Dropping,
}

impl IndexStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Building => "building",
            Self::Ready => "ready",
            Self::Failed => "failed",
            Self::Dropping => "dropping",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IndexState {
    pub spec: IndexSpec,
    pub status: IndexStatus,
    pub generation: u64,
    pub indexed: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Project {
    pub id: ProjectId,
    pub name: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl Project {
    pub fn new(id: ProjectId, name: impl Into<String>) -> Result<Self> {
        let name = name.into();
        validate_name("project name", &name)?;
        let now = Utc::now();
        Ok(Self {
            id,
            name,
            created_at: now,
            updated_at: now,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Collection {
    pub id: CollectionId,
    pub project_id: ProjectId,
    pub name: String,
    pub indexes: Vec<IndexSpec>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl Collection {
    pub fn new(id: CollectionId, project_id: ProjectId, name: impl Into<String>) -> Result<Self> {
        let name = name.into();
        validate_name("collection name", &name)?;
        let now = Utc::now();
        Ok(Self {
            id,
            project_id,
            name,
            indexes: Vec::new(),
            created_at: now,
            updated_at: now,
        })
    }
}

/// Stored object envelope. The user document lives in `body` and must be a JSON object.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StoredObject {
    pub id: ObjectId,
    pub project_id: ProjectId,
    pub collection_id: CollectionId,
    pub revision: u64,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub body: Value,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SearchRequest {
    pub index: String,
    pub query: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub field: Option<String>,
    #[serde(default = "default_search_limit")]
    pub limit: usize,
}

fn default_search_limit() -> usize {
    20
}

impl SearchRequest {
    pub const MAX_LIMIT: usize = 100;

    pub fn normalized(mut self) -> Result<Self> {
        validate_index_name(&self.index)?;
        if self.query.trim().is_empty() {
            return Err(SkobError::BadRequest("query must not be empty".into()));
        }
        if let Some(field) = &self.field {
            validate_field_path(field)?;
        }
        if self.limit == 0 {
            self.limit = default_search_limit();
        }
        self.limit = self.limit.min(Self::MAX_LIMIT);
        Ok(self)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SearchHit {
    pub object_id: ObjectId,
    pub score: f32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SegmentOwner {
    pub segment: u32,
    pub node_id: u64,
    pub advertise_addr: String,
}
