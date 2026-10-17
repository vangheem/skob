//! PostgreSQL-flavored SQL for skob objects.
//!
//! A Postgres database name is a project id. A table is a collection. Columns
//! other than `id`, `revision`, and `body` are JSON field paths. Unquoted
//! identifiers are folded to lowercase, matching PostgreSQL.

use chrono::Utc;
use serde_json::Map;
use serde_json::Number;
use serde_json::Value;
use skob_core::object_key;
use skob_core::object_prefix;
use skob_core::parse_object_key;
use skob_core::values_at_path;
use skob_core::BlobStore;
use skob_core::CollectionId;
use skob_core::IndexStatus;
use skob_core::ObjectId;
use skob_core::ProjectId;
use skob_core::Result;
use skob_core::SearchRequest;
use skob_core::SkobError;
use skob_core::StoredObject;

use crate::util::load_json;
use crate::util::put_json;
use crate::util::require_collection;
use crate::util::require_project;
use crate::AppState;

const DEFAULT_LIMIT: usize = 100;
const MAX_LIMIT: usize = 1000;
const MAX_SCAN: usize = 10_000;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlTable {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<Option<String>>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SqlResult {
    Command { tag: &'static str, rows: usize },
    Rows(SqlTable),
}

/// Run one or more statements. `project` is the session database from the
/// Postgres startup packet. Qualified names (`project.collection`) override it.
pub async fn execute_sql(state: &AppState, sql: &str, project: Option<&str>) -> Result<Vec<SqlResult>> {
    let mut out = Vec::new();
    for statement in split_statements(sql) {
        if statement.trim().is_empty() {
            continue;
        }
        out.push(execute_one(state, &statement, project).await?);
    }
    Ok(out)
}

async fn execute_one(state: &AppState, sql: &str, project: Option<&str>) -> Result<SqlResult> {
    let tokens = tokenize(sql)?;
    if tokens.is_empty() {
        return Ok(SqlResult::Command { tag: "EMPTY", rows: 0 });
    }
    let head = tokens[0].as_ident().unwrap_or("");
    match head {
        "set" | "reset" | "discard" | "begin" | "commit" | "rollback" | "start" | "show" => {
            Ok(session_command(head))
        }
        "select" if is_session_select(&tokens) => session_select(&tokens, project),
        "insert" => insert(state, &tokens, project).await,
        "update" => update(state, &tokens, project).await,
        "select" => select(state, &tokens, project).await,
        other => Err(SkobError::BadRequest(format!(
            "unsupported statement `{other}`; expected INSERT, UPDATE, or SELECT"
        ))),
    }
}

fn session_command(head: &str) -> SqlResult {
    let tag = match head {
        "begin" | "start" => "BEGIN",
        "commit" => "COMMIT",
        "rollback" => "ROLLBACK",
        "show" => "SHOW",
        _ => "SET",
    };
    SqlResult::Command { tag, rows: 0 }
}

fn is_session_select(tokens: &[Tok]) -> bool {
    !tokens.iter().any(|tok| tok.as_ident() == Some("from"))
}

fn session_select(tokens: &[Tok], project: Option<&str>) -> Result<SqlResult> {
    let text = tokens
        .iter()
        .filter_map(|tok| tok.as_ident().or(tok.as_string()))
        .collect::<Vec<_>>()
        .join(" ");
    let (column, value) = if text.contains("version") {
        ("version", "skob postgres 0.1")
    } else if text.contains("current_database") {
        ("current_database", project.unwrap_or(""))
    } else if text.contains("current_schema") {
        ("current_schema", "public")
    } else if text.contains("set_config") {
        ("set_config", "")
    } else if let Some(literal) = tokens.iter().find_map(|tok| match tok {
        Tok::String(value) => Some(value.clone()),
        Tok::Number(value) => Some(value.clone()),
        _ => None,
    }) {
        return Ok(SqlResult::Rows(SqlTable {
            columns: vec!["?column?".into()],
            rows: vec![vec![Some(literal)]],
        }));
    } else {
        return Err(SkobError::BadRequest(
            "SELECT without FROM must be a literal or a session function".into(),
        ));
    };
    Ok(SqlResult::Rows(SqlTable {
        columns: vec![column.into()],
        rows: vec![vec![Some(value.into())]],
    }))
}

async fn insert(state: &AppState, tokens: &[Tok], session: Option<&str>) -> Result<SqlResult> {
    require_writes(state)?;
    let mut cursor = Cursor::new(tokens);
    cursor.expect_ident("insert")?;
    cursor.expect_ident("into")?;
    let table = cursor.table()?;
    let (project, collection) = resolve_table(state, &table, session).await?;
    cursor.expect_symbol("(")?;
    let mut columns = Vec::new();
    loop {
        columns.push(cursor.path()?);
        if cursor.eat_symbol(")") {
            break;
        }
        cursor.expect_symbol(",")?;
    }
    if columns.is_empty() {
        return Err(SkobError::BadRequest("INSERT requires a column list".into()));
    }
    cursor.expect_ident("values")?;
    let mut written = 0usize;
    loop {
        cursor.expect_symbol("(")?;
        let mut values = Vec::new();
        loop {
            values.push(cursor.literal()?);
            if cursor.eat_symbol(")") {
                break;
            }
            cursor.expect_symbol(",")?;
        }
        if values.len() != columns.len() {
            return Err(SkobError::BadRequest(format!(
                "INSERT has {} columns and {} values",
                columns.len(),
                values.len()
            )));
        }
        write_insert(state, &project, &collection, &columns, &values).await?;
        written += 1;
        if !cursor.eat_symbol(",") {
            break;
        }
    }
    cursor.finish()?;
    Ok(SqlResult::Command {
        tag: "INSERT",
        rows: written,
    })
}

async fn write_insert(
    state: &AppState,
    project: &ProjectId,
    collection: &CollectionId,
    columns: &[String],
    values: &[Literal],
) -> Result<()> {
    let mut body = Map::new();
    let mut id = None;
    for (column, value) in columns.iter().zip(values) {
        match column.as_str() {
            "id" => {
                let raw = value
                    .as_str()
                    .ok_or_else(|| SkobError::BadRequest("id must be a string".into()))?;
                id = Some(ObjectId::parse(raw)?);
            }
            "revision" => {
                return Err(SkobError::BadRequest("revision is assigned by skob".into()));
            }
            "body" => body = literal_object(value)?,
            path => set_path(&mut body, path, value.to_json())?,
        }
    }
    let id = id.unwrap_or_else(ObjectId::generate);
    owns_object(state, &id).await?;
    let key = object_key(project, collection, &id);
    if load_json::<StoredObject>(state.blob.as_ref(), &key).await?.is_some() {
        return Err(SkobError::AlreadyExists {
            entity: "object",
            id: id.to_string(),
        });
    }
    let now = Utc::now();
    let stored = StoredObject {
        id: id.clone(),
        project_id: project.clone(),
        collection_id: collection.clone(),
        revision: 1,
        created_at: now,
        updated_at: now,
        body: Value::Object(body),
    };
    put_json(state.blob.as_ref(), &key, &stored).await?;
    state.index.sync_object(project, collection, &id, &stored.body).await?;
    Ok(())
}

async fn update(state: &AppState, tokens: &[Tok], session: Option<&str>) -> Result<SqlResult> {
    require_writes(state)?;
    let mut cursor = Cursor::new(tokens);
    cursor.expect_ident("update")?;
    let table = cursor.table()?;
    let (project, collection) = resolve_table(state, &table, session).await?;
    cursor.expect_ident("set")?;
    let mut sets = Vec::new();
    loop {
        let path = cursor.path()?;
        cursor.expect_symbol("=")?;
        sets.push((path, cursor.literal()?));
        if !cursor.eat_symbol(",") {
            break;
        }
    }
    cursor.expect_ident("where")?;
    let filter = cursor.filter()?;
    cursor.finish()?;
    if sets.iter().any(|(path, _)| path == "id" || path == "revision") {
        return Err(SkobError::BadRequest("id and revision cannot be updated".into()));
    }

    let matches = matching_objects(state, &project, &collection, &filter, MAX_SCAN).await?;
    for stored in &matches {
        owns_object(state, &stored.id).await?;
        let mut body = stored.body.as_object().cloned().unwrap_or_default();
        for (path, value) in &sets {
            if path == "body" {
                body = literal_object(value)?;
            } else {
                set_path(&mut body, path, value.to_json())?;
            }
        }
        let updated = StoredObject {
            revision: stored.revision.saturating_add(1),
            updated_at: Utc::now(),
            body: Value::Object(body),
            ..stored.clone()
        };
        put_json(
            state.blob.as_ref(),
            &object_key(&project, &collection, &updated.id),
            &updated,
        )
        .await?;
        state
            .index
            .sync_object(&project, &collection, &updated.id, &updated.body)
            .await?;
    }
    Ok(SqlResult::Command {
        tag: "UPDATE",
        rows: matches.len(),
    })
}

async fn select(state: &AppState, tokens: &[Tok], session: Option<&str>) -> Result<SqlResult> {
    if !state.role.serves_reads() {
        return Err(SkobError::WrongRole("reads"));
    }
    let mut cursor = Cursor::new(tokens);
    cursor.expect_ident("select")?;
    let star = cursor.eat_symbol("*");
    let mut columns = Vec::new();
    if !star {
        loop {
            columns.push(cursor.path()?);
            if !cursor.eat_symbol(",") {
                break;
            }
        }
    }
    cursor.expect_ident("from")?;
    let table = cursor.table()?;
    let (project, collection) = resolve_table(state, &table, session).await?;
    let filter = if cursor.eat_ident("where") {
        cursor.filter()?
    } else {
        Filter { preds: Vec::new() }
    };
    let limit = if cursor.eat_ident("limit") {
        cursor.number()?.min(MAX_LIMIT)
    } else {
        DEFAULT_LIMIT
    };
    cursor.finish()?;
    if star {
        columns = vec!["id".into(), "revision".into(), "body".into()];
    }
    if columns.is_empty() {
        return Err(SkobError::BadRequest("SELECT requires a column list".into()));
    }

    let objects = matching_objects(state, &project, &collection, &filter, limit).await?;
    let rows = objects
        .iter()
        .map(|stored| columns.iter().map(|column| project_cell(stored, column)).collect())
        .collect();
    Ok(SqlResult::Rows(SqlTable { columns, rows }))
}

fn project_cell(stored: &StoredObject, column: &str) -> Option<String> {
    match column {
        "id" => Some(stored.id.to_string()),
        "revision" => Some(stored.revision.to_string()),
        "body" => Some(stored.body.to_string()),
        path => values_at_path(&stored.body, path).first().map(|value| render(value)),
    }
}

fn render(value: &Value) -> String {
    match value {
        Value::Null => "null".into(),
        Value::Bool(flag) => flag.to_string(),
        Value::Number(number) => number.to_string(),
        Value::String(text) => text.clone(),
        other => other.to_string(),
    }
}

async fn matching_objects(
    state: &AppState,
    project: &ProjectId,
    collection: &CollectionId,
    filter: &Filter,
    limit: usize,
) -> Result<Vec<StoredObject>> {
    if let Some(id) = filter.id_eq() {
        let id = ObjectId::parse(id)?;
        let Some(stored) = load_json(state.blob.as_ref(), &object_key(project, collection, &id)).await? else {
            return Ok(Vec::new());
        };
        return Ok(filter.matches(&stored).then_some(stored).into_iter().collect());
    }
    if let Some(ids) = indexed_ids(state, project, collection, filter).await? {
        let mut matched = Vec::new();
        for id in ids {
            if matched.len() >= limit {
                break;
            }
            let Some(stored) = load_json(state.blob.as_ref(), &object_key(project, collection, &id)).await? else {
                continue;
            };
            if filter.matches(&stored) {
                matched.push(stored);
            }
        }
        return Ok(matched);
    }
    scan_objects(state.blob.as_ref(), project, collection, filter, limit).await
}

async fn indexed_ids(
    state: &AppState,
    project: &ProjectId,
    collection: &CollectionId,
    filter: &Filter,
) -> Result<Option<Vec<ObjectId>>> {
    let Some((path, literal)) = filter.preds.iter().find_map(|pred| match pred {
        Pred::Eq { path, value } if path != "id" && path != "revision" => Some((path, value)),
        _ => None,
    }) else {
        return Ok(None);
    };
    let indexes = state.index.list_indexes(project, collection).await?;
    let Some(index) = indexes.into_iter().find(|state| {
        state.status == IndexStatus::Ready && state.spec.fields.iter().any(|field| field.path == *path)
    }) else {
        return Ok(None);
    };
    let request = SearchRequest {
        index: index.spec.name,
        query: literal.query_text(),
        field: Some(path.clone()),
        limit: MAX_LIMIT,
    }
    .normalized()?;
    match state.index.search(project, collection, request).await {
        Ok(hits) => Ok(Some(hits.into_iter().map(|hit| hit.object_id).collect())),
        Err(SkobError::IndexNotReady { .. }) => Ok(None),
        Err(err) => Err(err),
    }
}

async fn scan_objects(
    blob: &dyn BlobStore,
    project: &ProjectId,
    collection: &CollectionId,
    filter: &Filter,
    limit: usize,
) -> Result<Vec<StoredObject>> {
    let prefix = object_prefix(project, collection);
    let mut cursor = None;
    let mut matched = Vec::new();
    let mut seen = 0usize;
    loop {
        let page = blob.list(&prefix, cursor.as_deref()).await?;
        let next = page.cursor.clone();
        for meta in page.objects {
            let Some((parsed_project, parsed_collection, _)) = parse_object_key(&meta.key) else {
                continue;
            };
            if parsed_project != *project || parsed_collection != *collection {
                continue;
            }
            seen += 1;
            if seen > MAX_SCAN {
                return Err(SkobError::BadRequest(format!(
                    "scan exceeded {MAX_SCAN} objects; add a WHERE id or indexed equality"
                )));
            }
            let Some(stored) = load_json(blob, &meta.key).await? else {
                continue;
            };
            if filter.matches(&stored) {
                matched.push(stored);
                if matched.len() >= limit {
                    return Ok(matched);
                }
            }
        }
        match next {
            Some(token) => cursor = Some(token),
            None => return Ok(matched),
        }
    }
}

async fn resolve_table(
    state: &AppState,
    table: &TableRef,
    session: Option<&str>,
) -> Result<(ProjectId, CollectionId)> {
    let project_raw = table.project.as_deref().or(session).ok_or_else(|| {
        SkobError::BadRequest(
            "no project in scope; connect with dbname=<project> or qualify the table as project.collection".into(),
        )
    })?;
    let project = ProjectId::parse(project_raw)?;
    let collection = CollectionId::parse(&table.collection)?;
    require_project(state.blob.as_ref(), &project).await?;
    require_collection(state.blob.as_ref(), &project, &collection).await?;
    Ok((project, collection))
}

fn require_writes(state: &AppState) -> Result<()> {
    if state.role.serves_writes() {
        Ok(())
    } else {
        Err(SkobError::WrongRole("writes"))
    }
}

async fn owns_object(state: &AppState, object_id: &ObjectId) -> Result<()> {
    let raft = state
        .raft
        .as_ref()
        .ok_or_else(|| SkobError::raft("writer is not joined to raft"))?;
    raft.ensure_owns_object(object_id).await?;
    Ok(())
}

fn literal_object(value: &Literal) -> Result<Map<String, Value>> {
    let Literal::String(text) = value else {
        return Err(SkobError::BadRequest("body must be a JSON object string".into()));
    };
    let parsed: Value = serde_json::from_str(text).map_err(|err| SkobError::InvalidJson(err.to_string()))?;
    match parsed {
        Value::Object(map) => Ok(map),
        _ => Err(SkobError::InvalidJson("body must be a JSON object".into())),
    }
}

fn set_path(root: &mut Map<String, Value>, path: &str, value: Value) -> Result<()> {
    if path.is_empty() || path.split('.').any(|part| part.is_empty()) {
        return Err(SkobError::BadRequest(format!("invalid column `{path}`")));
    }
    let mut parts = path.split('.').collect::<Vec<_>>();
    let last = parts.pop().expect("path has a segment");
    let mut cursor = root;
    for part in parts {
        let entry = cursor
            .entry(part.to_string())
            .or_insert_with(|| Value::Object(Map::new()));
        if !entry.is_object() {
            *entry = Value::Object(Map::new());
        }
        cursor = entry.as_object_mut().expect("object just inserted");
    }
    cursor.insert(last.to_string(), value);
    Ok(())
}

#[derive(Debug, Clone)]
struct TableRef {
    project: Option<String>,
    collection: String,
}

#[derive(Debug, Clone)]
enum Literal {
    Null,
    Bool(bool),
    Number(Number),
    String(String),
}

impl Literal {
    fn to_json(&self) -> Value {
        match self {
            Self::Null => Value::Null,
            Self::Bool(flag) => Value::Bool(*flag),
            Self::Number(number) => Value::Number(number.clone()),
            Self::String(text) => Value::String(text.clone()),
        }
    }

    fn as_str(&self) -> Option<&str> {
        match self {
            Self::String(text) => Some(text),
            _ => None,
        }
    }

    fn query_text(&self) -> String {
        let raw = match self {
            Self::Null => "null".into(),
            Self::Bool(flag) => flag.to_string(),
            Self::Number(number) => number.to_string(),
            Self::String(text) => text.clone(),
        };
        if raw.chars().any(|ch| ch.is_whitespace()) {
            format!("\"{}\"", raw.replace('\\', "\\\\").replace('"', "\\\""))
        } else {
            raw
        }
    }
}

#[derive(Debug, Clone)]
struct Filter {
    preds: Vec<Pred>,
}

#[derive(Debug, Clone)]
enum Pred {
    Eq { path: String, value: Literal },
}

impl Filter {
    fn id_eq(&self) -> Option<&str> {
        self.preds.iter().find_map(|pred| match pred {
            Pred::Eq { path, value } if path == "id" => value.as_str(),
            _ => None,
        })
    }

    fn matches(&self, stored: &StoredObject) -> bool {
        self.preds.iter().all(|pred| match pred {
            Pred::Eq { path, value } => match path.as_str() {
                "id" => value.as_str() == Some(stored.id.as_str()),
                "revision" => value.query_text() == stored.revision.to_string(),
                "body" => stored.body.to_string() == value.query_text() || stored.body == value.to_json(),
                field => values_at_path(&stored.body, field)
                    .iter()
                    .any(|found| json_eq(found, value)),
            },
        })
    }
}

fn json_eq(value: &Value, literal: &Literal) -> bool {
    match literal {
        Literal::Null => value.is_null(),
        Literal::Bool(flag) => value.as_bool() == Some(*flag),
        Literal::Number(number) => value.as_number() == Some(number),
        Literal::String(text) => value.as_str() == Some(text),
    }
}

#[derive(Debug, Clone)]
enum Tok {
    Ident(String),
    String(String),
    Number(String),
    Symbol(char),
}

impl Tok {
    fn as_ident(&self) -> Option<&str> {
        match self {
            Self::Ident(value) => Some(value),
            _ => None,
        }
    }

    fn as_string(&self) -> Option<&str> {
        match self {
            Self::String(value) => Some(value),
            _ => None,
        }
    }
}

struct Cursor<'a> {
    tokens: &'a [Tok],
    index: usize,
}

impl<'a> Cursor<'a> {
    fn new(tokens: &'a [Tok]) -> Self {
        Self { tokens, index: 0 }
    }

    fn peek(&self) -> Option<&Tok> {
        self.tokens.get(self.index)
    }

    fn bump(&mut self) -> Result<&Tok> {
        let tok = self.tokens.get(self.index).ok_or_else(|| SkobError::BadRequest("unexpected end of SQL".into()))?;
        self.index += 1;
        Ok(tok)
    }

    fn expect_ident(&mut self, expected: &str) -> Result<()> {
        match self.bump()? {
            Tok::Ident(value) if value == expected => Ok(()),
            other => Err(SkobError::BadRequest(format!("expected {expected}, got {other:?}"))),
        }
    }

    fn eat_ident(&mut self, expected: &str) -> bool {
        match self.peek() {
            Some(Tok::Ident(value)) if value == expected => {
                self.index += 1;
                true
            }
            _ => false,
        }
    }

    fn expect_symbol(&mut self, expected: &str) -> Result<()> {
        if self.eat_symbol(expected) {
            Ok(())
        } else {
            Err(SkobError::BadRequest(format!(
                "expected `{expected}`, got {:?}",
                self.peek()
            )))
        }
    }

    fn eat_symbol(&mut self, expected: &str) -> bool {
        let Some(ch) = expected.chars().next() else {
            return false;
        };
        match self.peek() {
            Some(Tok::Symbol(found)) if *found == ch => {
                self.index += 1;
                true
            }
            _ => false,
        }
    }

    fn path(&mut self) -> Result<String> {
        let mut parts = vec![self.ident()?];
        while self.eat_symbol(".") {
            parts.push(self.ident()?);
        }
        Ok(parts.join("."))
    }

    fn ident(&mut self) -> Result<String> {
        match self.bump()? {
            Tok::Ident(value) => Ok(value.clone()),
            other => Err(SkobError::BadRequest(format!("expected identifier, got {other:?}"))),
        }
    }

    fn table(&mut self) -> Result<TableRef> {
        let first = self.ident()?;
        if self.eat_symbol(".") {
            Ok(TableRef {
                project: Some(first),
                collection: self.ident()?,
            })
        } else {
            Ok(TableRef {
                project: None,
                collection: first,
            })
        }
    }

    fn literal(&mut self) -> Result<Literal> {
        match self.bump()? {
            Tok::String(value) => Ok(Literal::String(value.clone())),
            Tok::Number(value) => parse_number(value),
            Tok::Ident(value) if value == "null" => Ok(Literal::Null),
            Tok::Ident(value) if value == "true" => Ok(Literal::Bool(true)),
            Tok::Ident(value) if value == "false" => Ok(Literal::Bool(false)),
            other => Err(SkobError::BadRequest(format!("expected literal, got {other:?}"))),
        }
    }

    fn number(&mut self) -> Result<usize> {
        match self.bump()? {
            Tok::Number(value) => value
                .parse()
                .map_err(|_| SkobError::BadRequest(format!("invalid limit `{value}`"))),
            other => Err(SkobError::BadRequest(format!("expected number, got {other:?}"))),
        }
    }

    fn filter(&mut self) -> Result<Filter> {
        let mut preds = Vec::new();
        loop {
            let path = self.path()?;
            self.expect_symbol("=")?;
            preds.push(Pred::Eq {
                path,
                value: self.literal()?,
            });
            if !self.eat_ident("and") {
                break;
            }
        }
        Ok(Filter { preds })
    }

    fn finish(&self) -> Result<()> {
        if self.index == self.tokens.len() {
            Ok(())
        } else {
            Err(SkobError::BadRequest(format!(
                "unexpected token {:?}",
                self.peek()
            )))
        }
    }
}

fn parse_number(raw: &str) -> Result<Literal> {
    if let Ok(number) = raw.parse::<i64>() {
        return Ok(Literal::Number(Number::from(number)));
    }
    if let Ok(number) = raw.parse::<u64>() {
        return Ok(Literal::Number(Number::from(number)));
    }
    let number = raw
        .parse::<f64>()
        .ok()
        .and_then(Number::from_f64)
        .ok_or_else(|| SkobError::BadRequest(format!("invalid number `{raw}`")))?;
    Ok(Literal::Number(number))
}

fn tokenize(sql: &str) -> Result<Vec<Tok>> {
    let bytes = sql.as_bytes();
    let mut index = 0;
    let mut tokens = Vec::new();
    while index < bytes.len() {
        let ch = bytes[index] as char;
        if ch.is_whitespace() {
            index += 1;
            continue;
        }
        if ch == '-' && bytes.get(index + 1) == Some(&b'-') {
            index += 2;
            while index < bytes.len() && bytes[index] != b'\n' {
                index += 1;
            }
            continue;
        }
        if ch == '/' && bytes.get(index + 1) == Some(&b'*') {
            index += 2;
            while index + 1 < bytes.len() && !(bytes[index] == b'*' && bytes[index + 1] == b'/') {
                index += 1;
            }
            index = (index + 2).min(bytes.len());
            continue;
        }
        if ch == '\'' {
            index += 1;
            let mut value = String::new();
            while index < bytes.len() {
                if bytes[index] == b'\'' {
                    if bytes.get(index + 1) == Some(&b'\'') {
                        value.push('\'');
                        index += 2;
                        continue;
                    }
                    index += 1;
                    break;
                }
                value.push(bytes[index] as char);
                index += 1;
            }
            tokens.push(Tok::String(value));
            continue;
        }
        if ch == '"' {
            index += 1;
            let start = index;
            while index < bytes.len() && bytes[index] != b'"' {
                index += 1;
            }
            let value = sql[start..index].to_string();
            if index < bytes.len() {
                index += 1;
            }
            tokens.push(Tok::Ident(value));
            continue;
        }
        if ch.is_ascii_digit() || (ch == '.' && bytes.get(index + 1).is_some_and(|b| b.is_ascii_digit())) {
            let start = index;
            index += 1;
            while index < bytes.len() && (bytes[index].is_ascii_digit() || bytes[index] == b'.') {
                index += 1;
            }
            tokens.push(Tok::Number(sql[start..index].to_string()));
            continue;
        }
        if ch == '_' || ch.is_ascii_alphabetic() {
            let start = index;
            index += 1;
            while index < bytes.len() {
                let next = bytes[index] as char;
                if next == '_' || next.is_ascii_alphanumeric() {
                    index += 1;
                } else {
                    break;
                }
            }
            tokens.push(Tok::Ident(sql[start..index].to_ascii_lowercase()));
            continue;
        }
        if "(),.=*;".contains(ch) {
            tokens.push(Tok::Symbol(ch));
            index += 1;
            continue;
        }
        return Err(SkobError::BadRequest(format!("unexpected character `{ch}` in SQL")));
    }
    Ok(tokens)
}

fn split_statements(sql: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut current = String::new();
    let mut chars = sql.chars().peekable();
    let mut in_string = false;
    while let Some(ch) = chars.next() {
        if in_string {
            current.push(ch);
            if ch == '\'' {
                if chars.peek() == Some(&'\'') {
                    current.push(chars.next().unwrap());
                } else {
                    in_string = false;
                }
            }
            continue;
        }
        if ch == '\'' {
            in_string = true;
            current.push(ch);
            continue;
        }
        if ch == ';' {
            if !current.trim().is_empty() {
                out.push(std::mem::take(&mut current));
            }
            continue;
        }
        current.push(ch);
    }
    if !current.trim().is_empty() {
        out.push(current);
    }
    out
}
