use std::fmt::Display;

/// Recoverable failures surfaced by every skob crate.
#[derive(Debug, thiserror::Error)]
pub enum SkobError {
    #[error("invalid json: {0}")]
    InvalidJson(String),

    #[error("invalid {kind} id `{value}`")]
    InvalidId { kind: &'static str, value: String },

    #[error("invalid {kind} `{value}`: {reason}")]
    InvalidName {
        kind: &'static str,
        value: String,
        reason: &'static str,
    },

    #[error("{entity} `{id}` not found")]
    NotFound { entity: &'static str, id: String },

    #[error("{entity} `{id}` already exists")]
    AlreadyExists { entity: &'static str, id: String },

    #[error("write segment {segment} is owned by node {owner_id} at {owner_addr}")]
    NotOwner {
        segment: u32,
        owner_id: u64,
        owner_addr: String,
    },

    #[error("index `{name}` is not ready ({status})")]
    IndexNotReady { name: String, status: String },

    #[error("request rejected: {0}")]
    BadRequest(String),

    #[error("conflict: {0}")]
    Conflict(String),

    #[error("storage error: {0}")]
    Storage(String),

    #[error("index error: {0}")]
    Index(String),

    #[error("raft error: {0}")]
    Raft(String),

    #[error("this process does not serve {0}")]
    WrongRole(&'static str),
}

impl SkobError {
    pub fn storage(err: impl Display) -> Self {
        Self::Storage(err.to_string())
    }

    pub fn index(err: impl Display) -> Self {
        Self::Index(err.to_string())
    }

    pub fn raft(err: impl Display) -> Self {
        Self::Raft(err.to_string())
    }

    pub fn code(&self) -> &'static str {
        match self {
            Self::InvalidJson(_) => "invalid_json",
            Self::InvalidId { .. } => "invalid_id",
            Self::InvalidName { .. } => "invalid_name",
            Self::NotFound { .. } => "not_found",
            Self::AlreadyExists { .. } => "already_exists",
            Self::NotOwner { .. } => "not_owner",
            Self::IndexNotReady { .. } => "index_not_ready",
            Self::BadRequest(_) => "bad_request",
            Self::Conflict(_) => "conflict",
            Self::Storage(_) => "storage",
            Self::Index(_) => "index",
            Self::Raft(_) => "raft",
            Self::WrongRole(_) => "wrong_role",
        }
    }

    pub fn http_status(&self) -> u16 {
        match self {
            Self::InvalidJson(_) | Self::InvalidId { .. } | Self::InvalidName { .. } | Self::BadRequest(_) => 400,
            Self::WrongRole(_) => 421,
            Self::NotFound { .. } => 404,
            Self::AlreadyExists { .. } | Self::Conflict(_) | Self::NotOwner { .. } | Self::IndexNotReady { .. } => 409,
            Self::Storage(_) | Self::Index(_) | Self::Raft(_) => 500,
        }
    }
}

pub type Result<T> = std::result::Result<T, SkobError>;
