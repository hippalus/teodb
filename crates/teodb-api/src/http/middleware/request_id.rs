//! Request ID generation and propagation.
//!
//! Every incoming request receives a unique UUID via the `x-request-id` header.
//! If the client already provides one, it is preserved. The same header is
//! propagated on the response, enabling end-to-end tracing (I6 invariant).

use axum::http::{HeaderName, HeaderValue, Request};
use tower_http::request_id::{MakeRequestId, PropagateRequestIdLayer, RequestId, SetRequestIdLayer};
use uuid::Uuid;

pub const REQUEST_ID_HEADER: HeaderName = HeaderName::from_static("x-request-id");

/// Generates a UUIDv7 (time-ordered) for each request.
#[derive(Clone, Default)]
pub struct RequestUuid;

impl MakeRequestId for RequestUuid {
    fn make_request_id<B>(&mut self, _request: &Request<B>) -> Option<RequestId> {
        let value = HeaderValue::from_str(&Uuid::now_v7().to_string()).ok()?;
        Some(RequestId::new(value))
    }
}

/// Factory for request-id layers.
pub struct RequestIdLayer;

impl RequestIdLayer {
    /// Layer that assigns a request ID if none exists.
    pub fn assign() -> SetRequestIdLayer<RequestUuid> {
        SetRequestIdLayer::new(REQUEST_ID_HEADER, RequestUuid)
    }

    /// Layer that copies the request ID to the response.
    pub fn propagate() -> PropagateRequestIdLayer {
        PropagateRequestIdLayer::new(REQUEST_ID_HEADER)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn request_uuid_generates_valid_id() {
        let mut maker = RequestUuid;
        let req = Request::builder().body(()).unwrap();
        let id = maker
            .make_request_id(&req)
            .expect("should produce an ID");
        let value = id.header_value().to_str().unwrap();
        assert!(Uuid::parse_str(value).is_ok(), "must be a valid UUID");
    }
}
