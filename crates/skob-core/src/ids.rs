use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::error::{Result, SkobError};

macro_rules! id_type {
    ($name:ident, $kind:literal) => {
        #[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
        #[serde(transparent)]
        pub struct $name(String);

        impl $name {
            pub fn generate() -> Self {
                Self(Uuid::now_v7().to_string())
            }

            pub fn parse(raw: impl Into<String>) -> Result<Self> {
                let value = raw.into();
                validate_id($kind, &value)?;
                Ok(Self(value))
            }

            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl std::fmt::Display for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str(&self.0)
            }
        }

        impl AsRef<str> for $name {
            fn as_ref(&self) -> &str {
                &self.0
            }
        }
    };
}

id_type!(ProjectId, "project");
id_type!(CollectionId, "collection");
id_type!(ObjectId, "object");

fn validate_id(kind: &'static str, value: &str) -> Result<()> {
    let ok = !value.is_empty()
        && value.len() <= 128
        && value
            .chars()
            .next()
            .is_some_and(|c| c.is_ascii_alphanumeric())
        && value
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '.' || c == '_' || c == '-');
    if ok {
        Ok(())
    } else {
        Err(SkobError::InvalidId {
            kind,
            value: value.to_string(),
        })
    }
}

pub fn validate_name(kind: &'static str, value: &str) -> Result<()> {
    let ok = !value.is_empty()
        && value.len() <= 256
        && !value.chars().any(|c| c.is_control());
    if ok {
        Ok(())
    } else {
        Err(SkobError::InvalidName {
            kind,
            value: value.to_string(),
            reason: "must be 1-256 characters without control characters",
        })
    }
}

pub fn validate_index_name(value: &str) -> Result<()> {
    let ok = !value.is_empty()
        && value.len() <= 64
        && value
            .chars()
            .next()
            .is_some_and(|c| c.is_ascii_lowercase())
        && value
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_' || c == '-');
    if ok {
        Ok(())
    } else {
        Err(SkobError::InvalidName {
            kind: "index",
            value: value.to_string(),
            reason: "must match [a-z][a-z0-9_-]{0,63}",
        })
    }
}

pub fn validate_field_path(value: &str) -> Result<()> {
    let ok = !value.is_empty()
        && value.split('.').all(|segment| {
            !segment.is_empty()
                && segment
                    .chars()
                    .next()
                    .is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
                && segment
                    .chars()
                    .all(|c| c.is_ascii_alphanumeric() || c == '_')
        });
    if ok {
        Ok(())
    } else {
        Err(SkobError::InvalidName {
            kind: "field path",
            value: value.to_string(),
            reason: "must be a dotted path of identifiers",
        })
    }
}
