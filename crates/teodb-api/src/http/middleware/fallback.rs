//! RFC 9457 fallback and panic handlers.
//!
//! Returns `application/problem+json` for:
//! - Unmatched routes (404)
//! - Wrong HTTP method (405)
//! - Panics (500) — prevents stack traces leaking to clients

use std::any::Any;

use axum::Json;
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};

use teodb_core::problem::ProblemDetail;

const PROBLEM_JSON: &str = "application/problem+json";

/// Returns 404 ProblemDetail for unmatched routes.
pub async fn handle_fallback() -> Response {
    let problem = ProblemDetail::new(404).with_detail("The requested resource was not found");

    (
        StatusCode::NOT_FOUND,
        [(header::CONTENT_TYPE, PROBLEM_JSON)],
        Json(problem),
    )
        .into_response()
}

/// Returns 405 ProblemDetail when the HTTP method is not allowed.
pub async fn handle_method_not_allowed() -> Response {
    let problem = ProblemDetail::new(405).with_detail("The request method is not allowed for this resource");

    (
        StatusCode::METHOD_NOT_ALLOWED,
        [(header::CONTENT_TYPE, PROBLEM_JSON)],
        Json(problem),
    )
        .into_response()
}

/// Catches panics and returns a clean 500 ProblemDetail.
///
/// Prevents raw stack traces from leaking to clients while
/// logging the panic details server-side.
pub fn handle_panic(err: Box<dyn Any + Send + 'static>) -> Response {
    let details = if let Some(s) = err.downcast_ref::<String>() {
        s.clone()
    } else if let Some(s) = err.downcast_ref::<&str>() {
        s.to_string()
    } else {
        "Unknown panic".to_owned()
    };

    tracing::error!(panic = %details, "handler panicked");

    let problem = ProblemDetail::new(500).with_detail("An unexpected error occurred while processing your request");

    (
        StatusCode::INTERNAL_SERVER_ERROR,
        [(header::CONTENT_TYPE, PROBLEM_JSON)],
        Json(problem),
    )
        .into_response()
}
