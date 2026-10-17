//! PostgreSQL wire protocol in front of [`crate::sql`].
//!
//! Trust authentication. The startup database name is the skob project id.
//! `psql` simple queries are the supported client path.

use std::sync::Arc;

use async_trait::async_trait;
use futures::stream;
use pgwire::api::auth::StartupHandler;
use pgwire::api::query::SimpleQueryHandler;
use pgwire::api::results::DataRowEncoder;
use pgwire::api::results::FieldFormat;
use pgwire::api::results::FieldInfo;
use pgwire::api::results::QueryResponse;
use pgwire::api::results::Response;
use pgwire::api::results::Tag;
use pgwire::api::ClientInfo;
use pgwire::api::NoopHandler;
use pgwire::api::PgWireServerHandlers;
use pgwire::api::METADATA_DATABASE;
use pgwire::error::ErrorInfo;
use pgwire::error::PgWireError;
use pgwire::error::PgWireResult;
use pgwire::tokio::process_socket;
use skob_core::Result;
use skob_core::SkobError;
use tokio::net::TcpListener;

use crate::sql::execute_sql;
use crate::sql::SqlResult;
use crate::AppState;

pub async fn run_listener(state: Arc<AppState>, listener: TcpListener) -> Result<()> {
    let factory = Arc::new(SqlFactory { state });
    loop {
        let (socket, _) = listener.accept().await.map_err(SkobError::storage)?;
        let factory = factory.clone();
        tokio::spawn(async move {
            if let Err(err) = process_socket(socket, None, factory).await {
                tracing::debug!(error = %err, "postgres connection closed");
            }
        });
    }
}

struct SqlFactory {
    state: Arc<AppState>,
}

struct SqlHandler {
    state: Arc<AppState>,
}

impl PgWireServerHandlers for SqlFactory {
    fn simple_query_handler(&self) -> Arc<impl SimpleQueryHandler> {
        Arc::new(SqlHandler {
            state: self.state.clone(),
        })
    }

    fn startup_handler(&self) -> Arc<impl StartupHandler> {
        Arc::new(NoopHandler)
    }
}

#[async_trait]
impl SimpleQueryHandler for SqlHandler {
    async fn do_query<C>(&self, client: &mut C, query: &str) -> PgWireResult<Vec<Response>>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        let project = client.metadata().get(METADATA_DATABASE).map(String::as_str);
        let project = project.filter(|name| !name.is_empty());
        let results = execute_sql(&self.state, query, project).await.map_err(pg_error)?;
        results.into_iter().map(to_response).collect()
    }
}

fn to_response(result: SqlResult) -> PgWireResult<Response> {
    match result {
        SqlResult::Command { tag, rows } => Ok(Response::Execution(Tag::new(tag).with_rows(rows))),
        SqlResult::Rows(table) => {
            let fields: Arc<Vec<FieldInfo>> = Arc::new(
                table
                    .columns
                    .iter()
                    .map(|name| FieldInfo::new(name.clone(), None, None, pgwire::api::Type::TEXT, FieldFormat::Text))
                    .collect(),
            );
            let schema = fields.clone();
            let rows = stream::iter(table.rows.into_iter().map(move |row| encode_row(schema.clone(), row)));
            Ok(Response::Query(QueryResponse::new(fields, rows)))
        }
    }
}

fn encode_row(schema: Arc<Vec<FieldInfo>>, row: Vec<Option<String>>) -> PgWireResult<pgwire::messages::data::DataRow> {
    let mut encoder = DataRowEncoder::new(schema);
    for cell in &row {
        encoder.encode_field(&cell.as_deref()).map_err(pg_error)?;
    }
    Ok(encoder.take_row())
}

fn pg_error(err: impl ToString) -> PgWireError {
    PgWireError::UserError(Box::new(ErrorInfo::new(
        "ERROR".into(),
        "42601".into(),
        err.to_string(),
    )))
}
