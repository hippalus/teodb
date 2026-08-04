//! Query engine abstraction.
//!
//! All public transports (REST, FlightSQL) call through `QueryEngine`
//! rather than directly creating and collecting local DataFusion dataframes.
//! Both standalone (embedded Ballista) and distributed (remote Ballista)
//! provide a concrete implementation.

use arrow::record_batch::RecordBatch;
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use futures::stream::BoxStream;

use teodb_core::error::TeoDBResult;
use teodb_core::query_id::QueryId;
use teodb_core::traits::authz::Principal;
use teodb_core::traits::query_engine::QueryStatus;

/// Context for a query request.
#[derive(Debug, Clone)]
pub struct QueryRequest {
    pub sql: String,
    pub principal: Principal,
    pub query_id: QueryId,
    /// Optional row limit pushed into the execution plan.
    pub limit: Option<usize>,
}

/// Opaque handle to a prepared query.
#[derive(Debug)]
pub struct QueryHandle {
    pub query_id: QueryId,
    pub schema: SchemaRef,
    /// Opaque state the engine uses when executing.
    pub state: Box<dyn std::any::Any + Send>,
}

/// A streaming query result delivering Arrow record batches.
pub struct QueryResultStream {
    inner: BoxStream<'static, Result<RecordBatch, teodb_core::TeoDBError>>,
    schema: SchemaRef,
}

impl QueryResultStream {
    pub fn new(schema: SchemaRef, stream: BoxStream<'static, Result<RecordBatch, teodb_core::TeoDBError>>) -> Self {
        Self { inner: stream, schema }
    }

    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    pub fn into_inner(self) -> BoxStream<'static, Result<RecordBatch, teodb_core::TeoDBError>> {
        self.inner
    }
}

impl futures::Stream for QueryResultStream {
    type Item = Result<RecordBatch, teodb_core::TeoDBError>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        std::pin::Pin::new(&mut self.inner).poll_next(cx)
    }
}

/// The query engine trait — all public query entry points call through this.
///
/// The production implementation is `BallistaQueryEngine`, used for both
/// standalone embedded and remote distributed execution.
#[async_trait]
pub trait QueryEngine: Send + Sync + 'static {
    /// Prepare a query: parse SQL, resolve tables, pin snapshots.
    async fn prepare(&self, req: QueryRequest) -> TeoDBResult<QueryHandle>;

    /// Execute a prepared query and return a streaming result.
    async fn execute_stream(&self, handle: QueryHandle) -> TeoDBResult<QueryResultStream>;

    /// Cancel a running query.
    async fn cancel(&self, query_id: &QueryId) -> TeoDBResult<()>;

    /// Check the status of a query.
    async fn status(&self, query_id: &QueryId) -> TeoDBResult<QueryStatus>;
}
