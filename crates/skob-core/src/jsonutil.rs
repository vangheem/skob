use bytes::Bytes;
use serde::Serialize;
use serde_json::{Map, Value};

use crate::error::{Result, SkobError};

/// Accept `application/json` with an optional charset parameter.
pub fn require_json_content_type(content_type: Option<&str>) -> Result<()> {
    let Some(raw) = content_type else {
        return Err(SkobError::InvalidJson(
            "content-type must be application/json".into(),
        ));
    };
    let mime = raw.split(';').next().unwrap_or(raw).trim();
    if mime.eq_ignore_ascii_case("application/json") {
        Ok(())
    } else {
        Err(SkobError::InvalidJson(format!(
            "content-type must be application/json, got `{raw}`"
        )))
    }
}

/// Parse bytes as a JSON object. Arrays and scalars are rejected.
pub fn parse_json_object(bytes: &[u8]) -> Result<Map<String, Value>> {
    let value: Value = serde_json::from_slice(bytes).map_err(|err| SkobError::InvalidJson(err.to_string()))?;
    match value {
        Value::Object(map) => Ok(map),
        other => Err(SkobError::InvalidJson(format!(
            "expected a JSON object, got {}",
            json_kind(&other)
        ))),
    }
}

pub fn to_json_bytes(value: &impl Serialize) -> Result<Bytes> {
    let bytes = serde_json::to_vec(value).map_err(|err| SkobError::InvalidJson(err.to_string()))?;
    Ok(Bytes::from(bytes))
}

fn json_kind(value: &Value) -> &'static str {
    match value {
        Value::Null => "null",
        Value::Bool(_) => "bool",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}

/// Walk a dotted path through nested objects.
///
/// Arrays fan out: `items.name` against `{"items":[{"name":"a"},{"name":"b"}]}`
/// yields both names. A missing field yields an empty vec. Terminal arrays are
/// flattened so an indexed array field contributes one value per element.
pub fn values_at_path<'a>(value: &'a Value, path: &str) -> Vec<&'a Value> {
    if path.is_empty() {
        return flatten_terminal(value);
    }
    let segments: Vec<&str> = path.split('.').collect();
    let mut found = Vec::new();
    walk(value, &segments, &mut found);
    found
}

fn walk<'a>(value: &'a Value, segments: &[&str], out: &mut Vec<&'a Value>) {
    if segments.is_empty() {
        out.extend(flatten_terminal(value));
        return;
    }
    match value {
        Value::Object(map) => {
            if let Some(child) = map.get(segments[0]) {
                walk(child, &segments[1..], out);
            }
        }
        Value::Array(items) => {
            for item in items {
                walk(item, segments, out);
            }
        }
        _ => {}
    }
}

fn flatten_terminal<'a>(value: &'a Value) -> Vec<&'a Value> {
    match value {
        Value::Array(items) => items.iter().flat_map(flatten_terminal).collect(),
        other => vec![other],
    }
}
