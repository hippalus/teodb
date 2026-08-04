use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use axum::body::{Body, HttpBody};
use axum::extract::State;
use axum::http::{Method, Request};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use tokio::sync::OwnedSemaphorePermit;

use crate::admission::{RateScope, principal_key};
use crate::http::AppState;
use crate::observer::ApiTransport;

use super::request_id::REQUEST_ID_HEADER;

fn classify_scope(req: &Request<Body>) -> RateScope {
    let path = req.uri().path();
    if matches!(path, "/live" | "/ready" | "/metrics") {
        return RateScope::Public;
    }
    match *req.method() {
        Method::GET | Method::HEAD | Method::OPTIONS => RateScope::Read,
        _ => RateScope::Write,
    }
}

fn peer_address(req: &Request<Body>) -> IpAddr {
    req.extensions()
        .get::<axum::extract::connect_info::ConnectInfo<SocketAddr>>()
        .map(|address| address.0.ip())
        .or_else(|| {
            req.extensions()
                .get::<SocketAddr>()
                .map(SocketAddr::ip)
        })
        .unwrap_or(IpAddr::V4(Ipv4Addr::LOCALHOST))
}

pub struct RateLimitLayer;

impl RateLimitLayer {
    pub async fn handle(State(state): State<Arc<AppState>>, mut req: Request<Body>, next: Next) -> Response {
        let scope = classify_scope(&req);
        let peer = peer_address(&req);
        let client = state.admission.client_ip(peer, req.headers());

        if scope == RateScope::Public {
            let key = format!("public:{client}");
            if let Err(retry_after) = state.admission.check_rate(scope, &key) {
                return rate_limited(&state, &req, retry_after);
            }
            return next.run(req).await;
        }

        let authenticated = match state
            .security
            .authenticator
            .authenticate_headers(ApiTransport::Rest, req.headers())
        {
            Ok(authenticated) => authenticated,
            Err(error) => {
                let key = format!("invalid:{client}");
                if let Err(retry_after) = state.admission.check_rate(scope, &key) {
                    return rate_limited(&state, &req, retry_after);
                }
                return crate::http::common::problem::problem_from_error(error, req.uri().path()).into_response();
            }
        };

        let rate_key = format!("principal:{}", principal_key(&authenticated.principal.subject));
        if let Err(retry_after) = state.admission.check_rate(scope, &rate_key) {
            return rate_limited(&state, &req, retry_after);
        }
        let Some(permit) = state
            .admission
            .acquire_principal(&authenticated.principal.subject)
        else {
            state
                .security
                .authorization
                .admission_rejection(ApiTransport::Rest, "principal_concurrency");
            return admission_rejected(&req, "Principal concurrency limit exceeded");
        };

        req.extensions_mut().insert(authenticated);
        hold_principal_permit(next.run(req).await, permit)
    }
}

fn hold_principal_permit(response: Response, permit: OwnedSemaphorePermit) -> Response {
    let (parts, body) = response.into_parts();
    let body = Body::new(PrincipalPermitBody {
        inner: body,
        permit: Some(permit),
        finished: false,
    });
    Response::from_parts(parts, body)
}

struct PrincipalPermitBody {
    inner: Body,
    permit: Option<OwnedSemaphorePermit>,
    finished: bool,
}

impl HttpBody for PrincipalPermitBody {
    type Data = <Body as HttpBody>::Data;
    type Error = <Body as HttpBody>::Error;

    fn poll_frame(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<Option<Result<http_body::Frame<Self::Data>, Self::Error>>> {
        let this = self.get_mut();
        if this.finished {
            return Poll::Ready(None);
        }

        let result = Pin::new(&mut this.inner).poll_frame(context);
        if matches!(&result, Poll::Ready(None)) {
            this.finished = true;
            this.permit.take();
        }
        result
    }

    fn is_end_stream(&self) -> bool {
        self.finished || self.inner.is_end_stream()
    }

    fn size_hint(&self) -> http_body::SizeHint {
        self.inner.size_hint()
    }
}

fn rate_limited(state: &AppState, req: &Request<Body>, retry_after: std::time::Duration) -> Response {
    state
        .security
        .authorization
        .admission_rejection(ApiTransport::Rest, "rate");
    let retry_after_secs = retry_after.as_secs().max(1);
    let request_id = req
        .headers()
        .get(REQUEST_ID_HEADER)
        .and_then(|value| value.to_str().ok())
        .unwrap_or("-");
    tracing::warn!(request_id, path = %req.uri().path(), "rate_limit_exceeded");
    let problem = teodb_core::problem::ProblemDetail::new(429)
        .with_title("Too Many Requests")
        .with_detail("Rate limit exceeded; retry later")
        .with_instance(req.uri().path());
    (
        axum::http::StatusCode::TOO_MANY_REQUESTS,
        [
            (
                axum::http::header::CONTENT_TYPE,
                axum::http::HeaderValue::from_static("application/problem+json"),
            ),
            (
                axum::http::header::RETRY_AFTER,
                axum::http::HeaderValue::from_str(&retry_after_secs.to_string())
                    .unwrap_or_else(|_| axum::http::HeaderValue::from_static("1")),
            ),
        ],
        axum::Json(problem),
    )
        .into_response()
}

fn admission_rejected(req: &Request<Body>, detail: &'static str) -> Response {
    let problem = teodb_core::problem::ProblemDetail::new(429)
        .with_title("Too Many Requests")
        .with_detail(detail)
        .with_instance(req.uri().path());
    (
        axum::http::StatusCode::TOO_MANY_REQUESTS,
        [(axum::http::header::RETRY_AFTER, "1")],
        axum::Json(problem),
    )
        .into_response()
}

#[cfg(test)]
mod tests {
    use futures::future::poll_fn;

    use super::*;

    #[test]
    fn canonical_metrics_endpoint_uses_the_public_scope() {
        let metrics = Request::get("/metrics")
            .body(Body::empty())
            .unwrap();

        assert_eq!(classify_scope(&metrics), RateScope::Public);
    }

    #[tokio::test]
    async fn principal_permit_lives_until_response_body_finishes() {
        let semaphore = Arc::new(tokio::sync::Semaphore::new(1));
        let permit = semaphore.clone().try_acquire_owned().unwrap();
        let response = hold_principal_permit(Response::new(Body::from("ok")), permit);
        assert_eq!(semaphore.available_permits(), 0);

        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        assert_eq!(&body[..], b"ok");
        assert_eq!(semaphore.available_permits(), 1);
    }

    #[tokio::test]
    async fn principal_permit_body_is_safe_to_poll_after_end() {
        let semaphore = Arc::new(tokio::sync::Semaphore::new(1));
        let permit = semaphore.clone().try_acquire_owned().unwrap();
        let response = hold_principal_permit(Response::new(Body::from("ok")), permit);
        let mut body = response.into_body();

        let frame = poll_fn(|context| Pin::new(&mut body).poll_frame(context))
            .await
            .expect("data frame")
            .expect("valid data frame");
        assert_eq!(frame.data_ref().expect("frame data"), "ok");
        assert_eq!(semaphore.available_permits(), 0);

        assert!(
            poll_fn(|context| Pin::new(&mut body).poll_frame(context))
                .await
                .is_none()
        );
        assert_eq!(semaphore.available_permits(), 1);

        assert!(
            poll_fn(|context| Pin::new(&mut body).poll_frame(context))
                .await
                .is_none()
        );
        assert_eq!(semaphore.available_permits(), 1);
    }

    #[test]
    fn dropping_response_body_releases_principal_permit() {
        let semaphore = Arc::new(tokio::sync::Semaphore::new(1));
        let permit = semaphore.clone().try_acquire_owned().unwrap();
        let response = hold_principal_permit(Response::new(Body::from("ok")), permit);
        assert_eq!(semaphore.available_permits(), 0);

        drop(response);
        assert_eq!(semaphore.available_permits(), 1);
    }
}
