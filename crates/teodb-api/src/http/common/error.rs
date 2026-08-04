//! The single handler-facing error type and the one place RFC 9457 problem
//! responses are rendered.
//!
//! Handlers return `Result<T, ApiError>` and lean on `?`: any [`TeoDBError`]
//! (or an authorization denial) converts into an [`ApiError`]. The one
//! thing a `?`-thrown error can't know at the throw site is the request path
//! (the RFC 9457 `instance`), so [`ApiError`] defers rendering: it stashes the
//! problem detail on the response and the [`render_problem_details`] layer —
//! which *does* see the request — fills `instance` and serializes the body.
//!
//! Trace IDs are captured eagerly in [`ApiError::into_response`], while the
//! request span is still active. This mirrors the A6 `SecurityContext`/`Denied`
//! pattern and extends it to every error surface.

use axum::extract::{OriginalUri, Request};
use axum::http::StatusCode;
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};

use teodb_core::error::TeoDBError;
use teodb_core::problem::ProblemDetail;

use super::problem::{ProblemResponse, current_trace_id};
use super::security::Denied;

/// A handler-level error that renders as an RFC 9457 problem detail.
///
/// Construct one with `?` from a [`TeoDBError`] or an authorization denial, or directly
/// from a [`ProblemDetail`] when a handler needs a bespoke shape. The problem
/// is boxed so the `Err` variant of every handler's `Result` stays pointer-sized.
pub struct ApiError {
    problem: Box<ProblemDetail>,
    diagnostics: Option<ErrorDiagnostics>,
}

impl ApiError {
    /// Wrap a pre-built problem detail.
    #[track_caller]
    pub fn new(problem: ProblemDetail) -> Self {
        Self {
            problem: Box::new(problem),
            diagnostics: None,
        }
    }
}

impl From<TeoDBError> for ApiError {
    #[track_caller]
    fn from(error: TeoDBError) -> Self {
        let diagnostics = ErrorDiagnostics::capture(&error, std::panic::Location::caller());
        Self {
            problem: Box::new(error.to_problem_detail()),
            diagnostics: Some(diagnostics),
        }
    }
}

impl From<Denied> for ApiError {
    fn from(d: Denied) -> Self {
        // `Denied` already carries the request path, so its `instance` is set;
        // `render_problem_details` will leave it untouched.
        Self::new(d.into_problem())
    }
}

/// Server-only error chain and application call site carried through Axum
/// response extensions.
///
/// This is deliberately separate from [`ProblemDetail`]: detailed backend
/// errors belong in trusted logs, never in client responses.
#[derive(Clone)]
pub(crate) struct ErrorDiagnostics {
    pub error_code: &'static str,
    pub chain: std::sync::Arc<str>,
    pub origin_file: &'static str,
    pub origin_line: u32,
    pub origin_column: u32,
}

impl ErrorDiagnostics {
    fn capture(error: &TeoDBError, origin: &'static std::panic::Location<'static>) -> Self {
        let mut chain = error.to_string();
        let mut source = std::error::Error::source(error);
        while let Some(current) = source {
            chain.push_str(" -> caused by: ");
            chain.push_str(&current.to_string());
            source = current.source();
        }

        Self {
            error_code: error.code(),
            chain: std::sync::Arc::from(chain),
            origin_file: origin.file(),
            origin_line: origin.line(),
            origin_column: origin.column(),
        }
    }
}

/// Final diagnostic context consumed by the outer access-log middleware.
#[derive(Clone)]
pub(crate) struct ErrorLogContext {
    pub diagnostics: ErrorDiagnostics,
    pub trace_id: Option<String>,
}

/// Carries a deferred problem detail on the response from the throw site out to
/// [`render_problem_details`]. Cloneable so it can live in `http::Extensions`.
#[derive(Clone)]
struct ProblemEnvelope {
    problem: Box<ProblemDetail>,
    diagnostics: Option<ErrorDiagnostics>,
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let mut pd = self.problem;
        if pd.trace_id.is_none()
            && let Some(tid) = current_trace_id()
        {
            pd.trace_id = Some(tid);
        }
        let status = StatusCode::from_u16(pd.status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
        let mut resp = status.into_response();
        resp.extensions_mut().insert(ProblemEnvelope {
            problem: pd,
            diagnostics: self.diagnostics,
        });
        resp
    }
}

/// Render any deferred [`ApiError`] into its RFC 9457 body, filling `instance`
/// from the request path when the throw site didn't set it.
///
/// This is the single point where `ApiError`s become HTTP bodies. Responses
/// without a deferred problem (success paths, `Denied` rendered directly by the
/// admin guard, `ApiJson` rejections) pass through untouched.
pub async fn render_problem_details(req: Request, next: Next) -> Response {
    let instance = req
        .extensions()
        .get::<OriginalUri>()
        .map_or_else(|| req.uri().path().to_string(), |o| o.0.path().to_string());

    let mut resp = next.run(req).await;

    if let Some(ProblemEnvelope {
        mut problem,
        diagnostics,
    }) = resp.extensions_mut().remove::<ProblemEnvelope>()
    {
        if problem.instance.is_none() {
            problem.instance = Some(instance);
        }
        let trace_id = problem.trace_id.clone();
        let mut rendered = ProblemResponse::new(*problem).into_response();
        if let Some(diagnostics) = diagnostics {
            rendered
                .extensions_mut()
                .insert(ErrorLogContext { diagnostics, trace_id });
        }
        return rendered;
    }
    resp
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn teodb_error_keeps_origin_and_chain_outside_problem_body() {
        let expected_line = line!() + 1;
        let response = ApiError::from(TeoDBError::wal_source(
            "checkpoint persistence failed",
            std::io::Error::other("disk is read-only"),
        ))
        .into_response();
        let envelope = response
            .extensions()
            .get::<ProblemEnvelope>()
            .expect("deferred problem envelope");
        let diagnostics = envelope
            .diagnostics
            .as_ref()
            .expect("TeoDBError diagnostics");

        assert_eq!(diagnostics.error_code, "Wal");
        assert_eq!(
            diagnostics.chain.as_ref(),
            "wal: checkpoint persistence failed -> caused by: disk is read-only"
        );
        assert_eq!(diagnostics.origin_file, file!());
        assert_eq!(diagnostics.origin_line, expected_line);
        assert!(diagnostics.origin_column > 0);
        assert_eq!(envelope.problem.status, 500);
        assert!(
            !serde_json::to_string(&envelope.problem)
                .unwrap()
                .contains("disk is read-only")
        );
    }
}
