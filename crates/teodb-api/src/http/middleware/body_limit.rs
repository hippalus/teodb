use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use axum::body::{Body, HttpBody};
use axum::extract::State;
use axum::http::{Request, StatusCode, header};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use http_body::{Frame, SizeHint};
use http_body_util::{LengthLimitError, Limited};

use crate::ApiAuthorization;
use crate::http::AppState;
use crate::observer::ApiTransport;

pub async fn enforce_body_limit(State(state): State<Arc<AppState>>, request: Request<Body>, next: Next) -> Response {
    let limit = usize::try_from(state.services.config.max_body_bytes).unwrap_or(usize::MAX);
    let content_length = request
        .headers()
        .get(header::CONTENT_LENGTH)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<u64>().ok());

    if content_length.is_some_and(|length| length > state.services.config.max_body_bytes) {
        state
            .security
            .authorization
            .admission_rejection(ApiTransport::Rest, "request_body");
        return payload_too_large(request.uri().path());
    }

    let (parts, body) = request.into_parts();
    let body = Body::new(ObservedLimitedBody::new(
        body,
        limit,
        state.security.authorization.clone(),
    ));
    next.run(Request::from_parts(parts, body)).await
}

fn payload_too_large(instance: &str) -> Response {
    let problem = teodb_core::problem::ProblemDetail::new(413)
        .with_title("Payload Too Large")
        .with_detail("Request body exceeds the configured byte limit")
        .with_instance(instance);
    (
        StatusCode::PAYLOAD_TOO_LARGE,
        [(header::CONTENT_TYPE, "application/problem+json")],
        axum::Json(problem),
    )
        .into_response()
}

struct ObservedLimitedBody {
    inner: Limited<Body>,
    authorization: Arc<ApiAuthorization>,
    recorded: bool,
}

impl ObservedLimitedBody {
    fn new(body: Body, limit: usize, authorization: Arc<ApiAuthorization>) -> Self {
        Self {
            inner: Limited::new(body, limit),
            authorization,
            recorded: false,
        }
    }
}

impl HttpBody for ObservedLimitedBody {
    type Data = <Limited<Body> as HttpBody>::Data;
    type Error = axum::Error;

    fn poll_frame(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        let this = self.get_mut();
        match Pin::new(&mut this.inner).poll_frame(context) {
            Poll::Ready(Some(Err(error))) => {
                if !this.recorded && error.downcast_ref::<LengthLimitError>().is_some() {
                    this.recorded = true;
                    this.authorization
                        .admission_rejection(ApiTransport::Rest, "request_body");
                }
                Poll::Ready(Some(Err(axum::Error::new(error))))
            }
            Poll::Ready(Some(Ok(frame))) => Poll::Ready(Some(Ok(frame))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }

    fn is_end_stream(&self) -> bool {
        self.inner.is_end_stream()
    }

    fn size_hint(&self) -> SizeHint {
        self.inner.size_hint()
    }
}

#[cfg(test)]
mod tests {
    use std::convert::Infallible;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use axum::body::Bytes;

    use super::*;
    use crate::observer::ApiObserver;

    #[derive(Default)]
    struct CountingObserver {
        body_rejections: AtomicUsize,
    }

    impl ApiObserver for CountingObserver {
        fn on_admission_rejection(&self, transport: ApiTransport, reason: &'static str) {
            assert_eq!(transport, ApiTransport::Rest);
            assert_eq!(reason, "request_body");
            self.body_rejections
                .fetch_add(1, Ordering::Relaxed);
        }
    }

    #[tokio::test]
    async fn chunked_body_records_limit_once() {
        let observer = Arc::new(CountingObserver::default());
        let authorization = Arc::new(ApiAuthorization::new(None, observer.clone()));
        let source = Body::from_stream(futures::stream::iter([
            Ok::<_, Infallible>(Bytes::from_static(b"1234")),
            Ok(Bytes::from_static(b"5678")),
        ]));
        let body = Body::new(ObservedLimitedBody::new(source, 5, authorization));

        assert!(
            axum::body::to_bytes(body, usize::MAX)
                .await
                .is_err()
        );
        assert_eq!(observer.body_rejections.load(Ordering::Relaxed), 1);
    }
}
