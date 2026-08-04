use std::collections::HashSet;
use std::time::Duration;

use async_trait::async_trait;
use backon::{ConstantBuilder, ExponentialBuilder, Retryable};
use ballista_core::serde::protobuf::scheduler_grpc_client::SchedulerGrpcClient;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::dataframe::DataFrame;
use datafusion::datasource::source_as_provider;
use datafusion::logical_expr::LogicalPlan;
use futures::{StreamExt, TryStreamExt};
use tracing::warn;

use teodb_core::error::{TeoDBError, TeoDBResult};
use teodb_core::ident::{SnapshotId, TableIdent};
use teodb_core::query_id::QueryId;
use teodb_core::traits::query_engine::QueryStatus;
use teodb_query::{QueryEngine, QueryHandle, QueryRequest, QueryResultStream, TeoTableProvider};

use super::{BallistaQueryEngine, BallistaQueryState, PinReleaser};

type BatchStream = futures::stream::BoxStream<'static, Result<arrow::record_batch::RecordBatch, TeoDBError>>;

struct ExecutionInput {
    query_id: QueryId,
    schema: arrow::datatypes::SchemaRef,
    dataframe: DataFrame,
    principal: teodb_core::traits::authz::Principal,
    pin_releaser: Option<PinReleaser>,
}

impl BallistaQueryEngine {
    async fn prepare_with_retries(&self, request: &QueryRequest) -> TeoDBResult<QueryHandle> {
        let mut attempt = 0u32;

        (|| async { self.try_prepare(request).await })
            .retry(
                ExponentialBuilder::default()
                    .with_min_delay(PREPARE_BASE_BACKOFF)
                    .with_max_times(PREPARE_MAX_RETRIES as usize),
            )
            .when(|error| error.is_retryable())
            .notify(|error, backoff| {
                attempt += 1;
                warn!(
                    query_id = %request.query_id,
                    attempt,
                    max = PREPARE_MAX_RETRIES,
                    backoff_ms = backoff.as_millis() as u64,
                    %error,
                    "retrying prepare after transient failure"
                );
            })
            .await
    }

    async fn take_execution_input(&self, mut handle: QueryHandle) -> TeoDBResult<ExecutionInput> {
        let state = handle
            .state
            .downcast_mut::<BallistaQueryState>()
            .ok_or_else(|| TeoDBError::Internal("invalid query handle state".into()))?;
        let query_id = handle.query_id;
        let dataframe = state
            .dataframe
            .take()
            .ok_or_else(|| TeoDBError::Internal("query handle already executed".into()))?;
        let limit = state.limit;
        let dataframe = if let Some(limit) = limit {
            match dataframe.limit(0, Some(limit)) {
                Ok(dataframe) => dataframe,
                Err(error) => {
                    let error = TeoDBError::QueryExecution(format!("failed to apply limit: {error}"));
                    self.queries.failed(query_id, &error).await;
                    return Err(error);
                }
            }
        } else {
            dataframe
        };

        Ok(ExecutionInput {
            query_id,
            schema: handle.schema,
            dataframe,
            principal: state.principal.clone(),
            pin_releaser: state.pin_releaser.take(),
        })
    }

    async fn local_fallback_stream(&self, input: &ExecutionInput, reason: &str) -> TeoDBResult<BatchStream> {
        self.record_fallback(&input.query_id, reason);
        match self
            .execute_prepared_local_stream(&input.dataframe, &input.principal)
            .await
        {
            Ok(stream) => Ok(stream
                .map_err(|error| TeoDBError::QueryExecution(error.to_string()))
                .boxed()),
            Err(error) => {
                self.queries.failed(input.query_id, &error).await;
                Err(error)
            }
        }
    }

    async fn execute_with_fallback(&self, input: &ExecutionInput) -> TeoDBResult<BatchStream> {
        match input.dataframe.clone().execute_stream().await {
            Ok(mut remote) if self.fallback_applies() => match remote.next().await {
                Some(Err(error)) if is_scheduler_unreachable(&error) => {
                    self.local_fallback_stream(input, &error.to_string())
                        .await
                }
                first => Ok(futures::stream::iter(first)
                    .chain(remote)
                    .map_err(|error| TeoDBError::QueryExecution(error.to_string()))
                    .boxed()),
            },
            Ok(remote) => Ok(remote
                .map_err(|error| TeoDBError::QueryExecution(error.to_string()))
                .boxed()),
            Err(error) if self.fallback_applies() && is_scheduler_unreachable(&error) => {
                self.local_fallback_stream(input, &error.to_string())
                    .await
            }
            Err(error) => Err(TeoDBError::QueryExecution(error.to_string())),
        }
    }

    fn track_stream(
        &self,
        query_id: QueryId,
        stream: BatchStream,
        pin_releaser: Option<PinReleaser>,
    ) -> impl futures::Stream<Item = Result<arrow::record_batch::RecordBatch, TeoDBError>> + use<> {
        let queries = self.queries.clone();
        stream
            .map(Some)
            .chain(futures::stream::once(async { None }))
            .scan(false, move |failed, item| {
                let _pins_live_with_stream = &pin_releaser;
                let queries = queries.clone();
                let (output, status_update) = if *failed {
                    (None, None)
                } else {
                    match item {
                        None => (None, Some(QueryStatus::Completed)),
                        Some(result) => {
                            let status = result
                                .as_ref()
                                .err()
                                .map(|error| QueryStatus::Failed(error.to_string()));
                            if status.is_some() {
                                *failed = true;
                            }
                            (Some(result), status)
                        }
                    }
                };
                async move {
                    if let Some(status) = status_update {
                        queries.set(query_id, status).await;
                    }
                    output
                }
            })
    }
}

#[async_trait]
impl QueryEngine for BallistaQueryEngine {
    #[tracing::instrument(name = "query.prepare", skip_all, fields(query_id = %req.query_id))]
    async fn prepare(&self, req: QueryRequest) -> TeoDBResult<QueryHandle> {
        self.queries
            .set(req.query_id, QueryStatus::Planning)
            .await;
        match self.prepare_with_retries(&req).await {
            Ok(handle) => Ok(handle),
            Err(error) => {
                self.queries.failed(req.query_id, &error).await;
                Err(error)
            }
        }
    }

    #[tracing::instrument(name = "query.start_stream", skip_all, fields(query_id = %handle.query_id))]
    async fn execute_stream(&self, handle: QueryHandle) -> TeoDBResult<QueryResultStream> {
        let input = self.take_execution_input(handle).await?;
        let stream = match self.execute_with_fallback(&input).await {
            Ok(stream) => stream,
            Err(error) => {
                self.queries.failed(input.query_id, &error).await;
                return Err(error);
            }
        };
        self.queries
            .set(input.query_id, QueryStatus::Running)
            .await;
        let tracked = self.track_stream(input.query_id, stream, input.pin_releaser);
        Ok(QueryResultStream::new(input.schema, Box::pin(tracked)))
    }

    #[tracing::instrument(name = "query.cancel", skip_all, fields(query_id = %query_id))]
    async fn cancel(&self, query_id: &QueryId) -> TeoDBResult<()> {
        self.queries
            .set(*query_id, QueryStatus::Cancelled)
            .await;
        self.release_pins(query_id);
        Ok(())
    }

    async fn status(&self, query_id: &QueryId) -> TeoDBResult<QueryStatus> {
        self.queries
            .get(query_id)
            .await
            .ok_or_else(|| TeoDBError::NotFound {
                resource: format!("query {query_id}"),
            })
    }
}

/// True when a DataFusion error from the Ballista execution path means the
/// scheduler could not be reached (tonic transport failures, surfaced as
/// `DataFusionError::Execution` with the transport error's debug text).
/// Scheduler-side query failures ("Fail to execute query due to ...") are
/// *not* connectivity errors and must fail the query, not fall back.
pub(super) fn is_scheduler_unreachable(e: &datafusion::error::DataFusionError) -> bool {
    let msg = e.to_string().to_ascii_lowercase();
    [
        "connection refused",
        "connecterror",
        "transport error",
        "tcp connect error",
        "dns error",
    ]
    .iter()
    .any(|needle| msg.contains(needle))
}

/// Collect `(table, snapshot_id)` for every TeoDB table scanned by the plan,
/// subqueries included. Tables without a current snapshot have nothing to pin.
pub(super) fn collect_scan_targets(plan: &LogicalPlan) -> Vec<(TableIdent, SnapshotId)> {
    let mut seen: HashSet<(TableIdent, SnapshotId)> = HashSet::new();
    let mut targets = Vec::new();

    // The visitor never returns an error, so traversal cannot fail.
    let _ = plan.apply_with_subqueries(|node| {
        if let LogicalPlan::TableScan(scan) = node
            && let Ok(provider) = source_as_provider(&scan.source)
            && let Some(teo) = provider.downcast_ref::<TeoTableProvider>()
            && let Some(snapshot_id) = teo.current_snapshot_id()
        {
            let target = (teo.ident().clone(), snapshot_id);
            if seen.insert(target.clone()) {
                targets.push(target);
            }
        }
        Ok(TreeNodeRecursion::Continue)
    });

    targets
}

/// Connect to the embedded scheduler with a retry loop.
pub(super) async fn connect_embedded_scheduler(
    scheduler_url: &str,
) -> TeoDBResult<SchedulerGrpcClient<tonic::transport::Channel>> {
    (|| async { SchedulerGrpcClient::connect(scheduler_url.to_owned()).await })
        .retry(
            ConstantBuilder::default()
                .with_delay(Duration::from_millis(100))
                .with_max_times(EMBEDDED_SCHEDULER_CONNECT_RETRIES),
        )
        .await
        .map_err(|error| {
            TeoDBError::Unavailable(format!(
                "embedded Ballista scheduler did not become reachable at {scheduler_url}: {error}"
            ))
        })
}

/// The internal DataFusion catalog name used by `register_teodb_bindings()`.
const DF_CATALOG_PREFIX: &str = "datafusion.";

/// Maximum number of retries for transient failures during `prepare()`.
const PREPARE_MAX_RETRIES: u32 = 3;
/// Base backoff delay between retries (doubled each attempt).
const PREPARE_BASE_BACKOFF: Duration = Duration::from_millis(100);
/// Retry count after the initial embedded scheduler connection attempt.
const EMBEDDED_SCHEDULER_CONNECT_RETRIES: usize = 49;

/// Classify a DataFusion planning error into the appropriate `TeoDBError`.
///
/// Specifically, "table not found" errors are mapped to `NotFound` (HTTP 404)
/// instead of `QueryExecution` (HTTP 500), and the internal `datafusion.`
/// catalog prefix is stripped so users see `perf.events` not `datafusion.perf.events`.
pub(super) fn classify_planning_error(e: datafusion::error::DataFusionError) -> TeoDBError {
    let msg = e.to_string();

    // DataFusion emits "table 'datafusion.ns.table' not found" for missing tables.
    if msg.contains("not found") && msg.contains("table '") {
        let cleaned = msg.replace(&format!("'{DF_CATALOG_PREFIX}"), "'");
        // Strip leading "Error during planning: " to keep the message user-friendly.
        let cleaned = cleaned
            .strip_prefix("Error during planning: ")
            .unwrap_or(&cleaned);
        return TeoDBError::NotFound {
            resource: cleaned.to_string(),
        };
    }

    // DataFusion emits "schema 'datafusion.ns' not found" for missing schemas.
    if msg.contains("not found") && msg.contains("schema '") {
        let cleaned = msg.replace(&format!("'{DF_CATALOG_PREFIX}"), "'");
        let cleaned = cleaned
            .strip_prefix("Error during planning: ")
            .unwrap_or(&cleaned);
        return TeoDBError::NotFound {
            resource: cleaned.to_string(),
        };
    }

    TeoDBError::QueryExecution(msg)
}
