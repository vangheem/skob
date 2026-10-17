use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::response::Response;
use axum::Json;
use serde_json::json;
use serde_json::Value;
use skob_core::SkobError;

pub struct ApiError(pub SkobError);

impl From<SkobError> for ApiError {
    fn from(err: SkobError) -> Self {
        Self(err)
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (status, body) = error_body(&self.0);
        (status, Json(body)).into_response()
    }
}

pub fn error_body(err: &SkobError) -> (StatusCode, Value) {
    let status = StatusCode::from_u16(err.http_status()).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
    let mut body = json!({
        "error": err.code(),
        "message": err.to_string(),
    });
    if let SkobError::NotOwner {
        segment,
        owner_id,
        owner_addr,
    } = err
    {
        body["segment"] = json!(segment);
        body["owner_id"] = json!(owner_id);
        body["owner_addr"] = json!(owner_addr);
    }
    (status, body)
}
