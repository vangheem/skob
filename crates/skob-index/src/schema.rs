use std::collections::HashSet;

use serde_json::Value;
use tantivy::schema::{
    DateOptions, Field, NumericOptions, Schema, SchemaBuilder, TantivyDocument, STORED, STRING, TEXT,
};
use tantivy::Index;

use skob_core::{values_at_path, FieldKind, IndexSpec, Result, SkobError};

const OBJECT_ID: &str = "object_id";

#[derive(Clone, Debug)]
pub(crate) struct MappedField {
    pub path: String,
    pub kind: FieldKind,
    pub field: Field,
}

#[derive(Clone, Debug)]
pub(crate) struct BuiltSchema {
    pub schema: Schema,
    pub object_id: Field,
    pub fields: Vec<MappedField>,
}

impl BuiltSchema {
    pub fn create(spec: &IndexSpec) -> Result<Self> {
        if spec.fields.is_empty() {
            return Err(SkobError::BadRequest(
                "index must declare at least one field".into(),
            ));
        }
        let mut builder = Schema::builder();
        let object_id = builder.add_text_field(OBJECT_ID, STRING | STORED);
        let mut fields = Vec::with_capacity(spec.fields.len());
        let mut seen = HashSet::new();
        seen.insert(OBJECT_ID.to_string());
        for field in &spec.fields {
            let tantivy_name = tantivy_name(&field.path);
            if !seen.insert(tantivy_name.clone()) {
                return Err(SkobError::BadRequest(format!(
                    "field `{}` collides with another indexed field",
                    field.path
                )));
            }
            let handle = add_field(&mut builder, &tantivy_name, field.kind);
            fields.push(MappedField {
                path: field.path.clone(),
                kind: field.kind,
                field: handle,
            });
        }
        Ok(Self {
            schema: builder.build(),
            object_id,
            fields,
        })
    }

    pub fn bind(index: &Index, spec: &IndexSpec) -> Result<Self> {
        let schema = index.schema();
        let object_id = schema
            .get_field(OBJECT_ID)
            .map_err(|err| SkobError::index(format!("index is missing object_id: {err}")))?;
        let mut fields = Vec::with_capacity(spec.fields.len());
        for field in &spec.fields {
            let tantivy_name = tantivy_name(&field.path);
            let handle = schema.get_field(&tantivy_name).map_err(|err| {
                SkobError::index(format!(
                    "index is missing field `{}` ({tantivy_name}): {err}",
                    field.path
                ))
            })?;
            fields.push(MappedField {
                path: field.path.clone(),
                kind: field.kind,
                field: handle,
            });
        }
        Ok(Self {
            schema,
            object_id,
            fields,
        })
    }

    pub fn document(&self, object_id: &str, body: &Value) -> TantivyDocument {
        let mut doc = TantivyDocument::new();
        doc.add_text(self.object_id, object_id);
        for field in &self.fields {
            for value in values_at_path(body, &field.path) {
                add_value(&mut doc, field, value);
            }
        }
        doc
    }

    pub fn query_fields(&self, field: Option<&str>) -> Result<Vec<Field>> {
        if let Some(path) = field {
            let mapped = self
                .fields
                .iter()
                .find(|candidate| candidate.path == path)
                .ok_or_else(|| {
                    SkobError::BadRequest(format!("field `{path}` is not in this index"))
                })?;
            return Ok(vec![mapped.field]);
        }
        Ok(self.fields.iter().map(|field| field.field).collect())
    }
}

pub(crate) fn tantivy_name(path: &str) -> String {
    path.replace('.', "__")
}

fn add_field(builder: &mut SchemaBuilder, name: &str, kind: FieldKind) -> Field {
    let numeric = || NumericOptions::default().set_indexed().set_fast();
    match kind {
        FieldKind::Text => builder.add_text_field(name, TEXT),
        FieldKind::Keyword => builder.add_text_field(name, STRING),
        FieldKind::I64 => builder.add_i64_field(name, numeric()),
        FieldKind::U64 => builder.add_u64_field(name, numeric()),
        FieldKind::F64 => builder.add_f64_field(name, numeric()),
        FieldKind::Bool => builder.add_bool_field(name, numeric()),
        FieldKind::Datetime => {
            builder.add_date_field(name, DateOptions::default().set_indexed().set_fast())
        }
    }
}

fn add_value(doc: &mut TantivyDocument, field: &MappedField, value: &Value) {
    match field.kind {
        FieldKind::Text => {
            if let Some(text) = value.as_str() {
                doc.add_text(field.field, text);
            }
        }
        FieldKind::Keyword => {
            if let Some(text) = keyword_text(value) {
                doc.add_text(field.field, text);
            }
        }
        FieldKind::I64 => {
            if let Some(number) = parse_i64(value) {
                doc.add_i64(field.field, number);
            }
        }
        FieldKind::U64 => {
            if let Some(number) = parse_u64(value) {
                doc.add_u64(field.field, number);
            }
        }
        FieldKind::F64 => {
            if let Some(number) = parse_f64(value) {
                doc.add_f64(field.field, number);
            }
        }
        FieldKind::Bool => {
            if let Some(flag) = value.as_bool() {
                doc.add_bool(field.field, flag);
            }
        }
        FieldKind::Datetime => {
            if let Some(stamp) = parse_datetime(value) {
                doc.add_date(field.field, stamp);
            }
        }
    }
}

fn keyword_text(value: &Value) -> Option<String> {
    match value {
        Value::String(text) => Some(text.clone()),
        Value::Number(number) => Some(number.to_string()),
        Value::Bool(flag) => Some(flag.to_string()),
        _ => None,
    }
}

fn parse_i64(value: &Value) -> Option<i64> {
    match value {
        Value::Number(number) => number.as_i64(),
        Value::String(text) => text.trim().parse().ok(),
        _ => None,
    }
}

fn parse_u64(value: &Value) -> Option<u64> {
    match value {
        Value::Number(number) => number.as_u64(),
        Value::String(text) => text.trim().parse().ok(),
        _ => None,
    }
}

fn parse_f64(value: &Value) -> Option<f64> {
    let number = match value {
        Value::Number(number) => number.as_f64()?,
        Value::String(text) => text.trim().parse().ok()?,
        _ => return None,
    };
    number.is_finite().then_some(number)
}

fn parse_datetime(value: &Value) -> Option<tantivy::DateTime> {
    let raw = value.as_str()?;
    let parsed = chrono::DateTime::parse_from_rfc3339(raw).ok()?;
    let nanos = parsed.timestamp_nanos_opt()?;
    Some(tantivy::DateTime::from_timestamp_nanos(nanos))
}
