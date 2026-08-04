//! Ballista-backed query engine — the single query execution path for TeoDB.
//!
//! Both standalone (embedded scheduler + executor) and distributed (remote
//! scheduler) deployments use this engine. There is no separate local-only
//! code path in production.

use std::sync::Arc;
use std::time::Duration;

use ballista_core::extension::{SessionConfigExt, SessionStateExt};
use datafusion::dataframe::DataFrame;
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::LogicalPlan;
use datafusion::physical_plan::SendableRecordBatchStream;
use moka::future::Cache;
use papaya::HashMap as PapayaHashMap;
use tokio::sync::OnceCell;
use tracing::{debug, info, warn};

use teodb_core::error::{TeoDBError, TeoDBResult};
use teodb_core::query_id::QueryId;
use teodb_core::snapshot_pin::{ActiveSnapshotRegistry, SnapshotPin};
use teodb_core::traits::query_engine::QueryStatus;
use teodb_query::{QueryHandle, QueryRequest};

use super::execution::{classify_planning_error, collect_scan_targets, connect_embedded_scheduler};

/// Execution mode for the Ballista query engine.
#[derive(Debug, Clone)]
pub enum BallistaMode {
    /// In-process scheduler + executor. Used for standalone / single-node.
    Standalone { parallelism: usize },
    /// Connect to an external Ballista scheduler.
    Remote { scheduler_url: String },
}

const DEFAULT_QUERY_STATUS_MAX_ENTRIES: u64 = 100_000;
const DEFAULT_QUERY_STATUS_TTL: Duration = Duration::from_secs(60 * 60);

#[derive(Clone)]
pub(super) struct QueryStatusRegistry {
    cache: Cache<QueryId, QueryStatus>,
}

impl QueryStatusRegistry {
    fn new(max_entries: u64, ttl: Duration) -> Self {
        Self {
            cache: Cache::builder()
                .max_capacity(max_entries.max(1))
                .time_to_live(ttl.max(Duration::from_secs(1)))
                .build(),
        }
    }

    pub(super) async fn set(&self, query_id: QueryId, status: QueryStatus) {
        self.cache.insert(query_id, status).await;
    }

    pub(super) async fn failed(&self, query_id: QueryId, error: impl ToString) {
        self.set(query_id, QueryStatus::Failed(error.to_string()))
            .await;
    }

    pub(super) async fn get(&self, query_id: &QueryId) -> Option<QueryStatus> {
        self.cache.get(query_id).await
    }

    #[cfg(test)]
    pub(super) fn entry_count(&self) -> u64 {
        self.cache.entry_count()
    }

    #[cfg(test)]
    pub(super) async fn run_pending_tasks(&self) {
        self.cache.run_pending_tasks().await;
    }
}

impl Default for QueryStatusRegistry {
    fn default() -> Self {
        Self::new(DEFAULT_QUERY_STATUS_MAX_ENTRIES, DEFAULT_QUERY_STATUS_TTL)
    }
}

pub struct BallistaQueryEngineBuilder {
    mode: BallistaMode,
    session_factory: Arc<teodb_query::DataFusionSessionFactory>,
    // Optional builder inputs; build() applies mode-specific defaults.
    snapshot_registry: Option<Arc<dyn ActiveSnapshotRegistry>>,
    local_fallback: Option<bool>,
    status_retention: Option<(u64, Duration)>,
    event_observer: Option<Arc<dyn EngineEventObserver>>,
}

impl BallistaQueryEngineBuilder {
    pub fn new(mode: BallistaMode, session_factory: Arc<teodb_query::DataFusionSessionFactory>) -> Self {
        Self {
            mode,
            session_factory,
            snapshot_registry: None,
            local_fallback: None,
            status_retention: None,
            event_observer: None,
        }
    }

    pub fn standalone(session_factory: Arc<teodb_query::DataFusionSessionFactory>, parallelism: usize) -> Self {
        Self::new(
            BallistaMode::Standalone {
                parallelism: parallelism.max(1),
            },
            session_factory,
        )
    }

    pub fn remote(
        session_factory: Arc<teodb_query::DataFusionSessionFactory>,
        scheduler_endpoint: &str,
    ) -> TeoDBResult<Self> {
        let scheduler_url = crate::ballista::HostPort::parse(scheduler_endpoint, "cluster.scheduler_addr")?.http_url();
        Ok(Self::new(BallistaMode::Remote { scheduler_url }, session_factory))
    }

    pub fn snapshot_registry(mut self, registry: Arc<dyn ActiveSnapshotRegistry>) -> Self {
        self.snapshot_registry = Some(registry);
        self
    }

    pub fn local_fallback(mut self, enabled: bool) -> Self {
        self.local_fallback = Some(enabled);
        self
    }

    pub fn status_retention(mut self, max_entries: u64, ttl: Duration) -> Self {
        self.status_retention = Some((max_entries, ttl));
        self
    }

    pub fn event_observer(mut self, observer: Arc<dyn EngineEventObserver>) -> Self {
        self.event_observer = Some(observer);
        self
    }

    pub fn build(self) -> BallistaQueryEngine {
        let default_fallback = matches!(self.mode, BallistaMode::Remote { .. });
        let mut engine = BallistaQueryEngine::new(
            self.mode,
            self.session_factory,
            self.local_fallback.unwrap_or(default_fallback),
        );
        engine.snapshot_registry = self.snapshot_registry;
        if let Some((max_entries, ttl)) = self.status_retention {
            engine.queries = QueryStatusRegistry::new(max_entries, ttl);
        }
        engine.event_observer = self.event_observer;
        engine
    }
}

/// State stored inside a `QueryHandle` for deferred execution.
///
/// The DataFrame is resolved once during `prepare()` and reused in
/// `execute_stream()` so that snapshot-pinned table providers are never
/// re-resolved from the catalog.
pub(super) struct BallistaQueryState {
    /// The resolved, snapshot-pinned plan. `Option` so `execute_stream` can
    /// `take()` it without an async placeholder swap (see `take_execution_input`).
    pub(super) dataframe: Option<DataFrame>,
    pub(super) limit: Option<usize>,
    /// Principal used to build a local fallback session for the already
    /// prepared logical plan when the scheduler is unreachable.
    pub(super) principal: teodb_core::traits::authz::Principal,
    /// Releases the query's snapshot pins when dropped. Travels from the
    /// prepared handle into the result stream so pins survive exactly as
    /// long as the query: completion, failure, cancellation, or a dropped
    /// handle/stream all release them.
    pub(super) pin_releaser: Option<PinReleaser>,
}

/// Observer for engine-level events that the server layer turns into
/// metrics (same pattern as `FlushObserver` / `ReplayObserver`).
pub trait EngineEventObserver: Send + Sync + 'static {
    /// A query fell back to node-local execution because the scheduler was
    /// unreachable.
    fn on_local_fallback(&self, query_id: &QueryId, error: &str);
}

/// Snapshot pins held for a running query. Stored in the engine's pin
/// registry so they survive across the prepare→execute boundary and are
/// released only when the query completes, fails, or is cancelled.
struct QueryPins {
    pins: Vec<SnapshotPin>,
}

/// Drop guard that removes a query's pins from the engine map and releases
/// the query id explicitly. Epoch-based maps can defer value destruction, so
/// correctness must not depend on `SnapshotPin::drop` running immediately.
pub(super) struct PinReleaser {
    query_pins: Arc<PapayaHashMap<QueryId, QueryPins>>,
    query_id: QueryId,
    snapshot_registry: Option<Arc<dyn ActiveSnapshotRegistry>>,
}

impl Drop for PinReleaser {
    fn drop(&mut self) {
        if self
            .query_pins
            .pin()
            .remove(&self.query_id)
            .is_some()
        {
            release_query_snapshot_pins(&self.snapshot_registry, self.query_id);
        }
    }
}

fn release_query_snapshot_pins(registry: &Option<Arc<dyn ActiveSnapshotRegistry>>, query_id: QueryId) {
    let Some(registry) = registry else {
        return;
    };

    if registry.release_sync(query_id) {
        return;
    }

    let registry = registry.clone();
    match tokio::runtime::Handle::try_current() {
        Ok(handle) => {
            handle.spawn(async move {
                if let Err(e) = registry.release(query_id).await {
                    warn!(query_id = %query_id, error = %e, "failed to release query snapshot pins");
                }
            });
        }
        Err(_) => warn!(
            query_id = %query_id,
            "no async runtime available to release query snapshot pins; pins may linger until registry GC"
        ),
    }
}

/// Production query engine backed by Apache Ballista.
///
/// Supports both standalone (embedded) and remote (distributed) modes
/// through a single abstraction. All REST and FlightSQL query execution
/// goes through this engine.
pub struct BallistaQueryEngine {
    pub(super) mode: BallistaMode,
    session_factory: Arc<teodb_query::DataFusionSessionFactory>,
    pub(super) queries: QueryStatusRegistry,
    /// Snapshot pins held for active queries. Pins are inserted on `prepare()`
    /// and removed on completion/failure/cancel. The RAII `SnapshotPin` guards
    /// fire on drop, releasing pins in the `ActiveSnapshotRegistry`.
    query_pins: Arc<PapayaHashMap<QueryId, QueryPins>>,
    /// Registry shared with the maintenance loop. `None` disables pinning.
    snapshot_registry: Option<Arc<dyn ActiveSnapshotRegistry>>,
    /// Fall back to node-local DataFusion execution when the remote
    /// scheduler is unreachable (remote mode only, before any batch is
    /// produced — never after partial results).
    local_fallback: bool,
    /// Notified on engine events (local fallback) for metrics.
    event_observer: Option<Arc<dyn EngineEventObserver>>,
    standalone_url: OnceCell<String>,
}

impl BallistaQueryEngine {
    fn new(
        mode: BallistaMode,
        session_factory: Arc<teodb_query::DataFusionSessionFactory>,
        local_fallback: bool,
    ) -> Self {
        Self {
            mode,
            session_factory,
            queries: QueryStatusRegistry::default(),
            query_pins: Arc::new(PapayaHashMap::new()),
            snapshot_registry: None,
            local_fallback,
            event_observer: None,
            standalone_url: OnceCell::new(),
        }
    }

    /// Release snapshot pins for a query.
    pub(super) fn release_pins(&self, query_id: &QueryId) {
        if let Some(pins) = self.query_pins.pin().remove(query_id) {
            debug!(query_id = %query_id, pin_count = pins.pins.len(), "releasing snapshot pins");
            release_query_snapshot_pins(&self.snapshot_registry, *query_id);
        }
    }

    /// Single attempt at preparing a query. Separated from `prepare()` to
    /// allow retry wrapping for transient failures.
    pub(super) async fn try_prepare(&self, req: &QueryRequest) -> TeoDBResult<QueryHandle> {
        let ctx = self.build_session(&req.principal).await?;

        let df = ctx
            .sql(&req.sql)
            .await
            .map_err(classify_planning_error)?;

        let schema = df.schema().inner().clone();

        let pin_releaser = self
            .pin_scanned_snapshots(req.query_id, df.logical_plan())
            .await;

        debug!(
            query_id = %req.query_id,
            sql = %req.sql,
            "query prepared"
        );

        Ok(QueryHandle {
            query_id: req.query_id,
            schema,
            state: Box::new(BallistaQueryState {
                dataframe: Some(df),
                limit: req.limit,
                principal: req.principal.clone(),
                pin_releaser,
            }),
        })
    }

    /// Execute the already-prepared logical plan on a plain node-local
    /// DataFusion session (no Ballista upgrade). Used when the remote scheduler
    /// is unreachable. This must not reparse SQL or re-resolve tables from the
    /// live catalog; the prepared plan carries frozen table providers.
    pub(super) async fn execute_prepared_local_stream(
        &self,
        dataframe: &DataFrame,
        principal: &teodb_core::traits::authz::Principal,
    ) -> TeoDBResult<SendableRecordBatchStream> {
        let state = self
            .session_factory
            .session_state_for_principal(principal)?;
        let ctx = SessionContext::new_with_state(state);
        let df = ctx
            .execute_logical_plan(dataframe.logical_plan().clone())
            .await
            .map_err(classify_planning_error)?;
        df.execute_stream()
            .await
            .map_err(|e| TeoDBError::QueryExecution(format!("local fallback execution failed: {e}")))
    }

    /// True when this engine should retry a scheduler-connectivity failure
    /// on the node-local engine.
    pub(super) fn fallback_applies(&self) -> bool {
        self.local_fallback && matches!(self.mode, BallistaMode::Remote { .. })
    }

    pub(super) fn record_fallback(&self, query_id: &QueryId, error: &str) {
        warn!(
            query_id = %query_id,
            error,
            "scheduler unreachable; falling back to node-local query execution"
        );
        if let Some(observer) = &self.event_observer {
            observer.on_local_fallback(query_id, error);
        }
    }

    /// Pin the snapshot of every TeoDB table scanned by the prepared plan so
    /// snapshot expiration never reclaims files the query still reads. Pin
    /// failures are logged, not fatal — an unpinned query still has the
    /// retention window protecting recent snapshots.
    pub(super) async fn pin_scanned_snapshots(&self, query_id: QueryId, plan: &LogicalPlan) -> Option<PinReleaser> {
        let registry = self.snapshot_registry.as_ref()?;

        let mut pins = Vec::new();
        for (table, snapshot_id) in collect_scan_targets(plan) {
            match registry
                .pin(query_id, table.clone(), snapshot_id)
                .await
            {
                Ok(()) => pins.push(SnapshotPin::new(query_id, table, snapshot_id, registry.clone())),
                Err(e) => warn!(
                    query_id = %query_id,
                    table = %table,
                    snapshot_id,
                    error = %e,
                    "failed to pin snapshot for query"
                ),
            }
        }

        if pins.is_empty() {
            return None;
        }
        debug!(query_id = %query_id, pin_count = pins.len(), "pinned snapshots for query");
        self.query_pins
            .pin()
            .insert(query_id, QueryPins { pins });
        Some(PinReleaser {
            query_pins: self.query_pins.clone(),
            query_id,
            snapshot_registry: self.snapshot_registry.clone(),
        })
    }

    /// Ensure the embedded Ballista scheduler + executor are running.
    /// Returns the scheduler URL. Only used for standalone mode.
    async fn ensure_standalone_started(
        &self,
        state: &datafusion::execution::session_state::SessionState,
        parallelism: usize,
    ) -> TeoDBResult<String> {
        self.standalone_url
            .get_or_try_init(|| async move {
                let addr = ballista_scheduler::standalone::new_standalone_scheduler_from_state(state)
                    .await
                    .map_err(|e| TeoDBError::Internal(format!("failed to start embedded Ballista scheduler: {e}")))?;
                let scheduler_url = format!("http://localhost:{}", addr.port());

                let scheduler = connect_embedded_scheduler(&scheduler_url).await?;
                ballista_executor::new_standalone_executor_from_state(scheduler, parallelism, state)
                    .await
                    .map_err(|e| TeoDBError::Internal(format!("failed to start embedded Ballista executor: {e}")))?;

                info!(
                    scheduler = %scheduler_url,
                    parallelism,
                    "embedded Ballista standalone engine ready"
                );
                Ok(scheduler_url)
            })
            .await
            .cloned()
    }

    /// Build a Ballista-upgraded SessionContext for the given principal.
    async fn build_session(&self, principal: &teodb_core::traits::authz::Principal) -> TeoDBResult<SessionContext> {
        let state = self
            .session_factory
            .session_state_for_principal(principal)?;

        // Inject the TeoLogicalExtensionCodec into the session config so
        // Ballista can serialize/deserialize TeoTableProvider via frozen
        // SnapshotScanDescriptor → PinnedScanTableProvider.
        let config_with_codec = state
            .config()
            .clone()
            .with_ballista_logical_extension_codec(Arc::new(crate::codec::TeoLogicalExtensionCodec::new()));
        let state = datafusion::execution::session_state::SessionStateBuilder::new_from_existing(state)
            .with_config(config_with_codec)
            .build();

        let scheduler_url = match &self.mode {
            BallistaMode::Standalone { parallelism } => {
                self.ensure_standalone_started(&state, *parallelism)
                    .await?
            }
            BallistaMode::Remote { scheduler_url } => scheduler_url.clone(),
        };

        let upgraded = state
            .upgrade_for_ballista(scheduler_url)
            .map_err(|e| TeoDBError::QueryExecution(format!("Ballista session setup failed: {e}")))?;

        Ok(SessionContext::new_with_state(upgraded))
    }
}
