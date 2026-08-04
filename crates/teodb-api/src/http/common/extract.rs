//! Request context and typed JSON extractors for RFC 9457-compliant error responses.
//!
//! - `RequestContext`: Extracts the request URI (instance) and request ID from
//!   request parts for use in error responses and audit logs.
//! - `ApiJson<T>`: Custom JSON body extractor that returns RFC 9457 ProblemDetail
//!   on deserialization failure instead of Axum's default plain-text rejection.

use std::convert::Infallible;

use axum::Json;
use axum::body::Body;
use axum::extract::{FromRequest, FromRequestParts, OriginalUri};
use axum::http::Request;
use axum::http::header;
use axum::response::{IntoResponse, Response};
use serde::de::DeserializeOwned;
use tower_http::request_id::RequestId;

use teodb_core::problem::ProblemDetail;

use crate::http::middleware::request_id::REQUEST_ID_HEADER;

const PROBLEM_JSON: &str = "application/problem+json";

// RequestContext

/// Extracted from every request: the URI instance and optional request ID.
///
/// Handlers accept this as an extractor parameter to enrich error responses
/// with the correct `instance` and `x-request-id` fields.
#[derive(Debug, Clone)]
pub struct RequestContext {
    /// The full URI path (+ query) that produced this request.
    pub instance: String,
    /// The `x-request-id` header value, if present.
    pub request_id: Option<String>,
}

impl<S> FromRequestParts<S> for RequestContext
where
    S: Send + Sync,
{
    type Rejection = Infallible;

    async fn from_request_parts(parts: &mut axum::http::request::Parts, _state: &S) -> Result<Self, Self::Rejection> {
        let uri = parts
            .extensions
            .get::<OriginalUri>()
            .map_or_else(|| parts.uri.clone(), |o| o.0.clone());

        let instance = uri
            .path_and_query()
            .map_or_else(|| uri.path().to_string(), |pq| pq.as_str().to_string());

        let request_id = parts
            .extensions
            .get::<RequestId>()
            .and_then(|id| id.header_value().to_str().ok())
            .map(ToString::to_string);

        Ok(Self { instance, request_id })
    }
}

// ApiJson

/// Custom JSON extractor that returns RFC 9457 `ProblemDetail` on parse failure.
///
/// Replaces `axum::Json<T>` for request bodies so that malformed JSON produces
/// `application/problem+json` responses instead of Axum's default plain text.
pub struct ApiJson<T>(pub T);

impl<S, T> FromRequest<S> for ApiJson<T>
where
    T: DeserializeOwned,
    S: Send + Sync,
{
    type Rejection = Response;

    async fn from_request(req: Request<Body>, state: &S) -> Result<Self, Self::Rejection> {
        let instance = req
            .uri()
            .path_and_query()
            .map_or_else(|| req.uri().path().to_string(), |pq| pq.as_str().to_string());

        let request_id = req
            .headers()
            .get(REQUEST_ID_HEADER)
            .and_then(|v| v.to_str().ok())
            .map(ToString::to_string);

        match Json::<T>::from_request(req, state).await {
            Ok(Json(value)) => Ok(ApiJson(value)),
            Err(rejection) => {
                let status = rejection.status();
                let problem = ProblemDetail::new(status.as_u16())
                    .with_title(status.canonical_reason().unwrap_or("Bad Request"))
                    .with_detail(rejection.body_text())
                    .with_instance(&instance);

                let mut resp = (status, [(header::CONTENT_TYPE, PROBLEM_JSON)], Json(problem)).into_response();

                if let Some(rid) = request_id
                    && let Ok(val) = axum::http::HeaderValue::from_str(&rid)
                {
                    resp.headers_mut().insert(REQUEST_ID_HEADER, val);
                }

                Err(resp)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn request_context_defaults_to_infallible() {
        // Just verify the type compiles — the actual extraction
        // requires an Axum request pipeline.
        let _ctx = RequestContext {
            instance: "/api/v1/query".into(),
            request_id: Some("test-id".into()),
        };
        assert_eq!(_ctx.instance, "/api/v1/query");
    }
}
