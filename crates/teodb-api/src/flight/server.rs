//! Arrow Flight service implementation.

use std::pin::Pin;
use std::sync::Arc;

use arrow_flight::flight_service_server::FlightService;
use arrow_flight::{
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo, HandshakeRequest, HandshakeResponse,
    PollInfo, PutResult, SchemaResult, Ticket,
};
use futures::Stream;
use tonic::{Request, Response, Status, Streaming};

use super::{ingest, prepared, query, trace};
use crate::admission::RateScope;
use crate::http::AppState;
use crate::observer::ApiTransport;

/// TeoDB's Flight service for high-throughput batch ingestion and FlightSQL queries.
pub struct TeoFlightService {
    state: Arc<AppState>,
    prepared: prepared::PreparedStatementStore,
}

impl TeoFlightService {
    pub fn new(state: Arc<AppState>) -> Self {
        Self {
            state,
            prepared: prepared::PreparedStatementStore::new(),
        }
    }

    fn admit(
        &self,
        principal: &teodb_core::traits::authz::Principal,
    ) -> Result<tokio::sync::OwnedSemaphorePermit, Status> {
        self.state
            .admission
            .acquire_principal(&principal.subject)
            .ok_or_else(|| {
                self.state
                    .security
                    .authorization
                    .admission_rejection(ApiTransport::Flight, "principal_concurrency");
                Status::resource_exhausted("principal concurrency limit reached")
            })
    }

    fn hold_unary_permit<T>(response: &mut Response<T>, permit: tokio::sync::OwnedSemaphorePermit) {
        response.extensions_mut().insert(Arc::new(permit));
    }

    fn hold_stream_permit<T>(response: &mut Response<BoxStream<T>>, permit: tokio::sync::OwnedSemaphorePermit)
    where
        T: Send + 'static,
    {
        let stream = std::mem::replace(response.get_mut(), Box::pin(futures::stream::empty()));
        *response.get_mut() = Box::pin(PermitStream {
            inner: stream,
            permit: Some(permit),
        });
    }
}

type BoxStream<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send>>;

struct PermitStream<T> {
    inner: BoxStream<T>,
    permit: Option<tokio::sync::OwnedSemaphorePermit>,
}

impl<T> Stream for PermitStream<T> {
    type Item = Result<T, Status>;

    fn poll_next(self: Pin<&mut Self>, context: &mut std::task::Context<'_>) -> std::task::Poll<Option<Self::Item>> {
        let this = self.get_mut();
        let result = this.inner.as_mut().poll_next(context);
        if matches!(&result, std::task::Poll::Ready(None)) {
            this.permit.take();
        }
        result
    }
}

#[tonic::async_trait]
impl FlightService for TeoFlightService {
    type HandshakeStream = BoxStream<HandshakeResponse>;

    async fn handshake(
        &self,
        request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        let mut stream = request.into_inner();
        let _req = stream
            .message()
            .await
            .map_err(|e| Status::internal(format!("handshake stream error: {e}")))?;

        let response = HandshakeResponse {
            protocol_version: 0,
            payload: Default::default(),
        };
        let output = futures::stream::once(async { Ok(response) });
        Ok(Response::new(Box::pin(output) as Self::HandshakeStream))
    }

    type ListFlightsStream = BoxStream<FlightInfo>;

    async fn list_flights(&self, _request: Request<Criteria>) -> Result<Response<Self::ListFlightsStream>, Status> {
        Ok(Response::new(
            Box::pin(futures::stream::empty()) as Self::ListFlightsStream
        ))
    }

    #[tracing::instrument(name = "flight.get_info", skip_all)]
    async fn get_flight_info(&self, request: Request<FlightDescriptor>) -> Result<Response<FlightInfo>, Status> {
        let span = tracing::Span::current();
        trace::extract_trace_context(request.metadata(), &span);
        let principal = super::auth::principal_from_request(&self.state, &request, RateScope::Read)?;
        let permit = self.admit(&principal)?;
        let mut response = query::get_flight_info(&self.state, &principal, request.into_inner()).await?;
        Self::hold_unary_permit(&mut response, permit);
        Ok(response)
    }

    #[tracing::instrument(name = "flight.poll_info", skip_all)]
    async fn poll_flight_info(&self, request: Request<FlightDescriptor>) -> Result<Response<PollInfo>, Status> {
        let span = tracing::Span::current();
        trace::extract_trace_context(request.metadata(), &span);
        let principal = super::auth::principal_from_request(&self.state, &request, RateScope::Read)?;
        let permit = self.admit(&principal)?;
        let mut response = query::poll_flight_info(&self.state, &principal, request.into_inner()).await?;
        Self::hold_unary_permit(&mut response, permit);
        Ok(response)
    }

    #[tracing::instrument(name = "flight.get_schema", skip_all)]
    async fn get_schema(&self, request: Request<FlightDescriptor>) -> Result<Response<SchemaResult>, Status> {
        let span = tracing::Span::current();
        trace::extract_trace_context(request.metadata(), &span);
        let principal = super::auth::principal_from_request(&self.state, &request, RateScope::Read)?;
        let permit = self.admit(&principal)?;
        let mut response = query::get_schema(&self.state, &principal, request.into_inner()).await?;
        Self::hold_unary_permit(&mut response, permit);
        Ok(response)
    }

    type DoGetStream = BoxStream<FlightData>;

    #[tracing::instrument(name = "flight.do_get", skip_all)]
    async fn do_get(&self, request: Request<Ticket>) -> Result<Response<Self::DoGetStream>, Status> {
        let span = tracing::Span::current();
        trace::extract_trace_context(request.metadata(), &span);
        let principal = super::auth::principal_from_request(&self.state, &request, RateScope::Read)?;
        let permit = self.admit(&principal)?;
        let mut response = query::do_get(&self.state, &self.prepared, &principal, request.into_inner()).await?;
        Self::hold_stream_permit(&mut response, permit);
        Ok(response)
    }

    type DoPutStream = BoxStream<PutResult>;

    #[tracing::instrument(name = "flight.do_put", skip_all)]
    async fn do_put(&self, request: Request<Streaming<FlightData>>) -> Result<Response<Self::DoPutStream>, Status> {
        let span = tracing::Span::current();
        trace::extract_trace_context(request.metadata(), &span);
        let principal = super::auth::principal_from_request(&self.state, &request, RateScope::Write)?;
        let permit = self.admit(&principal)?;
        let mut response = ingest::do_put(&self.state, &principal, request).await?;
        Self::hold_stream_permit(&mut response, permit);
        Ok(response)
    }

    type DoExchangeStream = BoxStream<FlightData>;

    async fn do_exchange(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        Err(Status::unimplemented(
            "do_exchange is not supported; use do_put for ingest and do_get for queries",
        ))
    }

    type DoActionStream = BoxStream<arrow_flight::Result>;

    #[tracing::instrument(name = "flight.do_action", skip_all)]
    async fn do_action(&self, request: Request<Action>) -> Result<Response<Self::DoActionStream>, Status> {
        let span = tracing::Span::current();
        trace::extract_trace_context(request.metadata(), &span);
        let principal = super::auth::principal_from_request(&self.state, &request, RateScope::Write)?;
        let permit = self.admit(&principal)?;
        let action = request.into_inner();
        let mut response = match action.r#type.as_str() {
            "CreatePreparedStatement" => {
                self.prepared
                    .handle_create(&action.body, &principal, &self.state)
                    .await
            }
            "ClosePreparedStatement" => self.prepared.handle_close(&action.body),
            other => Err(Status::unimplemented(format!("action not supported: {other}"))),
        }?;
        Self::hold_stream_permit(&mut response, permit);
        Ok(response)
    }

    type ListActionsStream = BoxStream<ActionType>;

    async fn list_actions(&self, _request: Request<Empty>) -> Result<Response<Self::ListActionsStream>, Status> {
        let actions = vec![
            Ok(ActionType {
                r#type: "CreatePreparedStatement".into(),
                description: "Create a prepared statement for a SQL query".into(),
            }),
            Ok(ActionType {
                r#type: "ClosePreparedStatement".into(),
                description: "Close a previously created prepared statement".into(),
            }),
        ];
        Ok(Response::new(
            Box::pin(futures::stream::iter(actions)) as Self::ListActionsStream
        ))
    }
}

#[cfg(test)]
mod tests {
    use futures::StreamExt;

    use super::*;

    #[tokio::test]
    async fn flight_permit_lives_until_stream_completion() {
        let semaphore = Arc::new(tokio::sync::Semaphore::new(1));
        let permit = semaphore.clone().try_acquire_owned().unwrap();
        let mut response: Response<BoxStream<u8>> = Response::new(Box::pin(futures::stream::iter([Ok(7)])));
        TeoFlightService::hold_stream_permit(&mut response, permit);
        let mut stream = response.into_inner();

        assert_eq!(semaphore.available_permits(), 0);
        assert_eq!(stream.next().await.unwrap().unwrap(), 7);
        assert_eq!(semaphore.available_permits(), 0);
        assert!(stream.next().await.is_none());
        assert_eq!(semaphore.available_permits(), 1);
    }
}
