use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tower::{Layer, Service};

#[derive(Clone)]
pub struct FlightConcurrencyLayer {
    permits: Arc<Semaphore>,
    authorization: Arc<teodb_api::ApiAuthorization>,
}

impl FlightConcurrencyLayer {
    pub fn new(max_in_flight: usize, authorization: Arc<teodb_api::ApiAuthorization>) -> Self {
        Self {
            permits: Arc::new(Semaphore::new(max_in_flight)),
            authorization,
        }
    }
}

impl<S> Layer<S> for FlightConcurrencyLayer {
    type Service = FlightConcurrency<S>;

    fn layer(&self, inner: S) -> Self::Service {
        FlightConcurrency {
            inner,
            permits: self.permits.clone(),
            authorization: self.authorization.clone(),
        }
    }
}

#[derive(Clone)]
pub struct FlightConcurrency<S> {
    inner: S,
    permits: Arc<Semaphore>,
    authorization: Arc<teodb_api::ApiAuthorization>,
}

impl<S, RequestBody> Service<http::Request<RequestBody>> for FlightConcurrency<S>
where
    S: Service<http::Request<RequestBody>, Response = http::Response<tonic::body::Body>> + Send + 'static,
    S::Future: Send + 'static,
    S::Error: Send + 'static,
    RequestBody: Send + 'static,
{
    type Response = http::Response<tonic::body::Body>;
    type Error = S::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, context: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(context)
    }

    fn call(&mut self, request: http::Request<RequestBody>) -> Self::Future {
        let Ok(permit) = self.permits.clone().try_acquire_owned() else {
            self.authorization
                .admission_rejection(teodb_api::ApiTransport::Flight, "global_concurrency");
            return Box::pin(async move {
                Ok(tonic::Status::resource_exhausted("Flight RPC concurrency limit reached").into_http())
            });
        };

        let future = self.inner.call(request);
        Box::pin(async move {
            let response = future.await?;
            let (parts, body) = response.into_parts();
            let body = tonic::body::Body::new(PermitBody {
                inner: body,
                permit: Some(permit),
            });
            Ok(http::Response::from_parts(parts, body))
        })
    }
}

struct PermitBody {
    inner: tonic::body::Body,
    permit: Option<OwnedSemaphorePermit>,
}

impl http_body::Body for PermitBody {
    type Data = <tonic::body::Body as http_body::Body>::Data;
    type Error = <tonic::body::Body as http_body::Body>::Error;

    fn poll_frame(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<Option<Result<http_body::Frame<Self::Data>, Self::Error>>> {
        let this = self.get_mut();
        let result = Pin::new(&mut this.inner).poll_frame(context);
        if matches!(&result, Poll::Ready(None)) {
            this.permit.take();
        }
        result
    }

    fn is_end_stream(&self) -> bool {
        self.inner.is_end_stream()
    }

    fn size_hint(&self) -> http_body::SizeHint {
        self.inner.size_hint()
    }
}

#[cfg(test)]
mod tests {
    use std::convert::Infallible;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use tower::ServiceExt;

    use super::*;

    #[derive(Default)]
    struct CountingObserver {
        rejections: AtomicUsize,
    }

    impl teodb_api::ApiObserver for CountingObserver {
        fn on_admission_rejection(&self, transport: teodb_api::ApiTransport, reason: &'static str) {
            assert_eq!(transport, teodb_api::ApiTransport::Flight);
            assert_eq!(reason, "global_concurrency");
            self.rejections.fetch_add(1, Ordering::Relaxed);
        }
    }

    struct OneFrameBody {
        emitted: bool,
    }

    impl http_body::Body for OneFrameBody {
        type Data = <tonic::body::Body as http_body::Body>::Data;
        type Error = tonic::Status;

        fn poll_frame(
            mut self: Pin<&mut Self>,
            _context: &mut Context<'_>,
        ) -> Poll<Option<Result<http_body::Frame<Self::Data>, Self::Error>>> {
            if self.emitted {
                Poll::Ready(None)
            } else {
                self.emitted = true;
                Poll::Ready(Some(Ok(http_body::Frame::data(Default::default()))))
            }
        }

        fn is_end_stream(&self) -> bool {
            false
        }
    }

    #[tokio::test]
    async fn saturation_rejects_immediately_and_counts_once() {
        let observer = Arc::new(CountingObserver::default());
        let authorization = Arc::new(teodb_api::ApiAuthorization::new(None, observer.clone()));
        let layer = FlightConcurrencyLayer::new(1, authorization);
        let service = layer.layer(tower::service_fn(|_request: http::Request<()>| async move {
            Ok::<_, Infallible>(http::Response::new(tonic::body::Body::new(OneFrameBody {
                emitted: false,
            })))
        }));

        let first = service
            .clone()
            .oneshot(http::Request::new(()))
            .await
            .unwrap();
        assert_eq!(layer.permits.available_permits(), 0);

        let rejected = service
            .clone()
            .oneshot(http::Request::new(()))
            .await
            .unwrap();
        assert_eq!(rejected.headers()["grpc-status"], "8");
        assert_eq!(observer.rejections.load(Ordering::Relaxed), 1);

        drop(first);
        assert_eq!(layer.permits.available_permits(), 1);
    }
}
