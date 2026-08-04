//! Flight do_put handler: high-throughput batch ingestion via Arrow Flight.

use std::pin::Pin;
use std::sync::Arc;

use arrow_flight::sql::{Any, CommandStatementUpdate, DoPutUpdateResult, ProstMessageExt};
use arrow_flight::{FlightData, PutResult};
use futures::{Stream, StreamExt};
use prost::Message;
use tonic::{Request, Response, Status, Streaming};
use tracing::debug;

use arrow_flight::utils::flight_data_to_arrow_batch;

use crate::http::AppState;
use crate::service::SqlRouting;

use super::codec;

type BoxStream<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send>>;

/// Execute a SQL statement (DDL or DML) and return the affected row count.
async fn execute_statement_update(
    state: &Arc<AppState>,
    principal: &teodb_core::traits::authz::Principal,
    sql: &str,
) -> Result<Response<BoxStream<PutResult>>, Status> {
    debug!(sql = %sql, "Flight SQL do_put: CommandStatementUpdate");
    let deadline = tokio::time::Instant::now() + state.lifecycle.query_timeout;
    let query_id = teodb_core::query_id::QueryId::new();

    // Same permission the REST query endpoint requires for DDL/DML.
    super::auth::authorize(
        state,
        principal,
        teodb_core::traits::authz::Action::Query,
        teodb_core::traits::authz::Resource::Cluster,
    )
    .await?;

    // DDL is routed through the shared query service (same classify → execute →
    // buffer/WAL side-effect path the REST endpoint uses); anything else runs
    // on the engine as DML.
    let routing = tokio::time::timeout_at(deadline, state.services.ddl.route_sql(sql))
        .await
        .map_err(|_| Status::deadline_exceeded("statement update deadline exceeded during DDL routing"))?
        .map_err(crate::flight::error::status)?;

    let affected_rows: i64 = match routing {
        SqlRouting::Ddl(_) => 0,
        SqlRouting::Engine => {
            // DML or statements DataFusion can handle (INSERT INTO, etc.)
            let engine = &state.services.query_engine;
            let req = teodb_query::QueryRequest {
                sql: sql.to_string(),
                principal: principal.clone(),
                query_id,
                limit: None,
            };
            let handle = match tokio::time::timeout_at(deadline, engine.prepare(req)).await {
                Ok(result) => result.map_err(crate::flight::error::status)?,
                Err(_) => {
                    let _ = engine.cancel(&query_id).await;
                    return Err(Status::deadline_exceeded(
                        "statement update deadline exceeded during planning",
                    ));
                }
            };
            let mut stream = match tokio::time::timeout_at(deadline, engine.execute_stream(handle)).await {
                Ok(result) => result.map_err(crate::flight::error::status)?,
                Err(_) => {
                    let _ = engine.cancel(&query_id).await;
                    return Err(Status::deadline_exceeded(
                        "statement update deadline exceeded before streaming",
                    ));
                }
            };
            let mut total_rows: i64 = 0;
            loop {
                let next = match tokio::time::timeout_at(deadline, stream.next()).await {
                    Ok(next) => next,
                    Err(_) => {
                        drop(stream);
                        let _ = engine.cancel(&query_id).await;
                        return Err(Status::deadline_exceeded(
                            "statement update deadline exceeded while streaming",
                        ));
                    }
                };
                let Some(result) = next else { break };
                let batch = result.map_err(crate::flight::error::status)?;
                total_rows += batch.num_rows() as i64;
            }
            total_rows
        }
    };

    let update_result = DoPutUpdateResult {
        record_count: affected_rows,
    };
    let any_result = update_result.as_any();
    let result = PutResult {
        app_metadata: any_result.encode_to_vec().into(),
    };
    state.security.authorization.result_bytes(
        crate::observer::ApiTransport::Flight,
        "statement_update",
        result.app_metadata.len() as u64,
    );

    let stream = futures::stream::iter(vec![Ok(result)]);
    Ok(Response::new(Box::pin(stream) as BoxStream<PutResult>))
}

/// Process a `do_put` ingestion stream: decode batches, validate, WAL-append, buffer-insert.
/// Also handles FlightSQL `CommandStatementUpdate` for DDL/DML execution.
pub async fn do_put(
    state: &Arc<AppState>,
    principal: &teodb_core::traits::authz::Principal,
    request: Request<Streaming<FlightData>>,
) -> Result<Response<BoxStream<PutResult>>, Status> {
    let mut stream = request.into_inner();

    // First message: descriptor with table identity + schema.
    let first = stream
        .next()
        .await
        .ok_or_else(|| Status::invalid_argument("empty stream"))?
        .map_err(|e| Status::internal(format!("stream error: {e}")))?;

    let descriptor = first
        .flight_descriptor
        .clone()
        .ok_or_else(|| Status::invalid_argument("missing flight descriptor"))?;

    // Check if this is a FlightSQL CommandStatementUpdate (DDL/DML).
    if let Ok(any_cmd) = Any::decode(&*descriptor.cmd)
        && let Ok(Some(cmd)) = any_cmd.unpack::<CommandStatementUpdate>()
    {
        let sql = &cmd.query;
        if sql.trim().is_empty() {
            return Err(Status::invalid_argument("SQL statement must not be empty"));
        }
        return execute_statement_update(state, principal, sql).await;
    }

    let ident = codec::parse_descriptor(&descriptor)?;

    // Authorize the ingest action (I5 invariant) for the request's identity.
    super::auth::authorize(
        state,
        principal,
        teodb_core::traits::authz::Action::Ingest,
        teodb_core::traits::authz::Resource::Table(ident.clone()),
    )
    .await?;

    let buffer = state
        .services
        .buffers
        .get_or_load(&ident, &*state.services.catalog)
        .await
        .map_err(crate::flight::error::status)?;

    let schema = buffer
        .metadata()
        .current_schema()
        .map_err(crate::flight::error::status)?
        .clone();
    let arrow_schema = teodb_query::schema_to_arrow(&schema);

    // Pipeline each message as it arrives — decode → validate → reserve → WAL
    // append → buffer insert → emit PutResult — instead of collecting the whole
    // ingest stream. This restores backpressure (the buffer reservation rejects
    // when full) and bounds memory to one batch in flight at a time.
    let ctx = Arc::new(PutCtx {
        state: state.clone(),
        buffer,
        ident,
        arrow_schema,
    });

    let out = futures::stream::unfold(
        (Some(first), stream, ctx, false),
        move |(mut first_opt, mut stream, ctx, done)| async move {
            if done {
                return None;
            }
            loop {
                let data = if let Some(f) = first_opt.take() {
                    f
                } else {
                    match stream.next().await {
                        Some(Ok(d)) => d,
                        Some(Err(e)) => {
                            let err = Status::internal(format!("stream error: {e}"));
                            return Some((Err(err), (None, stream, ctx, true)));
                        }
                        None => return None,
                    }
                };
                // The descriptor-only first message (and any keepalive) carries
                // no batch — skip it and pull the next.
                if data.data_body.is_empty() {
                    continue;
                }
                let result = ctx.ingest_flight_batch(&data).await;
                // Stop after the first error; the WAL/buffer state is unchanged
                // beyond what already succeeded.
                let done = result.is_err();
                return Some((result, (None, stream, ctx, done)));
            }
        },
    );

    Ok(Response::new(Box::pin(out) as BoxStream<PutResult>))
}

/// Shared collaborators for processing one `do_put` ingest batch.
struct PutCtx {
    state: Arc<AppState>,
    buffer: Arc<teodb_ingest::buffer::TableBuffer>,
    ident: teodb_core::ident::TableIdent,
    arrow_schema: arrow::datatypes::SchemaRef,
}

impl PutCtx {
    /// Decode, validate, reserve, WAL-append, and buffer-insert one batch.
    async fn ingest_flight_batch(&self, data: &FlightData) -> Result<PutResult, Status> {
        let dictionaries: std::collections::HashMap<i64, Arc<dyn arrow::array::Array>> =
            std::collections::HashMap::new();
        let batch = flight_data_to_arrow_batch(data, self.arrow_schema.clone(), &dictionaries)
            .map_err(|e| Status::invalid_argument(format!("batch decode error: {e}")))?;

        // Schema validation per §13.4.
        crate::flight::validate::validate_batch(&batch, &self.arrow_schema).map_err(crate::flight::error::status)?;

        let batch_id = uuid::Uuid::now_v7();
        let row_count = batch.num_rows() as u64;
        let created_at_ms = chrono::Utc::now().timestamp_millis();

        // Reserve capacity + generation BEFORE WAL so once the record is
        // durable, post-WAL buffer admission cannot fail (invariant I1).
        let reservation = self.buffer.reserve(&batch).map_err(|error| {
            if let Some(reason) = write_rejection_reason(&error) {
                self.state
                    .security
                    .authorization
                    .write_rejection(reason);
            }
            crate::flight::error::status(error)
        })?;

        // WAL-before-ACK (I1 invariant).
        let wal_record = teodb_storage::wal::WalRecord {
            header: teodb_storage::wal::WalHeader {
                protocol_version: teodb_core::write_protocol::WRITE_PROTOCOL_VERSION,
                table_uuid: Some(self.buffer.metadata().table_uuid),
                batch_id,
                table: self.ident.clone(),
                schema_id: self.buffer.metadata().current_schema_id,
                generation: reservation.generation,
                created_at_ms,
                idempotency_key: None,
                row_count,
                byte_count: batch.get_array_memory_size() as u64,
                op: teodb_storage::wal::WalOp::Append,
            },
            batch: batch.clone(),
        };
        if let Err(e) = self.state.services.wal.append(&wal_record).await {
            self.buffer.release_reservation(reservation);
            self.state
                .security
                .authorization
                .write_rejection("wal_capacity");
            return Err(crate::flight::error::status(e));
        }

        let ok = self
            .buffer
            .insert_reserved_at(batch_id, reservation, created_at_ms, batch);
        let metadata = serde_json::json!({
            "batch_id": batch_id.to_string(),
            "writer_id": self.state.services.wal.writer_identity().writer_id.to_string(),
            "generation": ok.generation,
            "accepted_rows": row_count,
            "backpressure": ok.backpressure_signal,
        });
        let result = PutResult {
            app_metadata: metadata.to_string().into(),
        };
        self.state.security.authorization.result_bytes(
            crate::observer::ApiTransport::Flight,
            "ingest",
            result.app_metadata.len() as u64,
        );
        Ok(result)
    }
}

fn write_rejection_reason(error: &teodb_core::error::TeoDBError) -> Option<&'static str> {
    use teodb_core::error::TeoDBError;
    match error {
        TeoDBError::Backpressure(_) => Some("buffer_capacity"),
        TeoDBError::Wal { .. } => Some("wal_capacity"),
        TeoDBError::FlushBlocked { .. } => Some("flush_blocked"),
        TeoDBError::WriterRegistryFull { .. } => Some("writer_registry"),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::time::{Duration, Instant};

    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use async_trait::async_trait;
    use futures::StreamExt;
    use tempfile::TempDir;
    use teodb_core::error::{TeoDBError, TeoDBResult};
    use teodb_core::lifecycle::RoleLifecycle;
    use teodb_core::query_id::QueryId;
    use teodb_core::traits::authz::Principal;
    use teodb_core::traits::catalog::Catalog;
    use teodb_query::{QueryEngine, QueryHandle, QueryRequest, QueryResultStream};
    use teodb_storage::wal::{WalConfig, WalManager, WalRecoveryMode};
    use teodb_test_support::{MockCatalog, stub_storage_factory, table_metadata};

    use super::*;

    struct StubQueryEngine;

    #[async_trait]
    impl QueryEngine for StubQueryEngine {
        async fn prepare(&self, _req: QueryRequest) -> TeoDBResult<QueryHandle> {
            Err(TeoDBError::Internal("stub query engine".into()))
        }

        async fn execute_stream(&self, _handle: QueryHandle) -> TeoDBResult<QueryResultStream> {
            Err(TeoDBError::Internal("stub query engine".into()))
        }

        async fn cancel(&self, _query_id: &QueryId) -> TeoDBResult<()> {
            Ok(())
        }

        async fn status(&self, _query_id: &QueryId) -> TeoDBResult<teodb_core::traits::query_engine::QueryStatus> {
            Err(TeoDBError::NotFound {
                resource: "query".into(),
            })
        }
    }

    enum TimeoutMode {
        Planning,
        Streaming,
    }

    struct TimeoutQueryEngine {
        mode: TimeoutMode,
        cancellations: AtomicUsize,
    }

    impl TimeoutQueryEngine {
        fn new(mode: TimeoutMode) -> Arc<Self> {
            Arc::new(Self {
                mode,
                cancellations: AtomicUsize::new(0),
            })
        }
    }

    #[async_trait]
    impl QueryEngine for TimeoutQueryEngine {
        async fn prepare(&self, request: QueryRequest) -> TeoDBResult<QueryHandle> {
            if matches!(self.mode, TimeoutMode::Planning) {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
            Ok(QueryHandle {
                query_id: request.query_id,
                schema,
                state: Box::new(()),
            })
        }

        async fn execute_stream(&self, handle: QueryHandle) -> TeoDBResult<QueryResultStream> {
            let schema = handle.schema;
            let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(vec![1_i64]))]).unwrap();
            let stream = futures::stream::unfold((0_u8, batch), |(step, batch)| async move {
                match step {
                    0 => Some((Ok(batch.clone()), (1, batch))),
                    1 => {
                        tokio::time::sleep(Duration::from_millis(100)).await;
                        Some((Ok(batch.clone()), (2, batch)))
                    }
                    _ => None,
                }
            });
            Ok(QueryResultStream::new(schema, Box::pin(stream)))
        }

        async fn cancel(&self, _query_id: &QueryId) -> TeoDBResult<()> {
            self.cancellations.fetch_add(1, Ordering::Relaxed);
            Ok(())
        }

        async fn status(&self, _query_id: &QueryId) -> TeoDBResult<teodb_core::traits::query_engine::QueryStatus> {
            Err(TeoDBError::NotFound {
                resource: "query".into(),
            })
        }
    }

    struct SlowCreateNamespaceCatalog {
        inner: MockCatalog,
    }

    #[async_trait]
    impl Catalog for SlowCreateNamespaceCatalog {
        async fn list_namespaces(&self) -> TeoDBResult<Vec<String>> {
            self.inner.list_namespaces().await
        }

        async fn create_namespace(&self, namespace: &str, properties: HashMap<String, String>) -> TeoDBResult<()> {
            tokio::time::sleep(Duration::from_millis(100)).await;
            self.inner
                .create_namespace(namespace, properties)
                .await
        }

        async fn drop_namespace(&self, namespace: &str) -> TeoDBResult<()> {
            self.inner.drop_namespace(namespace).await
        }

        async fn list_tables(&self, namespace: &str) -> TeoDBResult<Vec<teodb_core::ident::TableIdent>> {
            self.inner.list_tables(namespace).await
        }

        async fn load_table(
            &self,
            ident: &teodb_core::ident::TableIdent,
        ) -> TeoDBResult<Arc<teodb_core::file::TableMetadata>> {
            self.inner.load_table(ident).await
        }

        async fn create_table(
            &self,
            request: teodb_core::traits::catalog::CreateTableRequest,
        ) -> TeoDBResult<Arc<teodb_core::file::TableMetadata>> {
            self.inner.create_table(request).await
        }

        async fn drop_table(&self, ident: &teodb_core::ident::TableIdent) -> TeoDBResult<()> {
            self.inner.drop_table(ident).await
        }

        async fn load_live_files(
            &self,
            ident: &teodb_core::ident::TableIdent,
        ) -> TeoDBResult<Vec<teodb_core::file::DataFile>> {
            self.inner.load_live_files(ident).await
        }

        async fn commit_append(
            &self,
            request: teodb_core::traits::catalog::CommitAppend,
        ) -> TeoDBResult<Arc<teodb_core::file::TableMetadata>> {
            self.inner.commit_append(request).await
        }

        async fn check_append_status(
            &self,
            request: &teodb_core::traits::catalog::CommitAppend,
        ) -> TeoDBResult<teodb_core::traits::catalog::CommitStatus> {
            self.inner.check_append_status(request).await
        }

        async fn commit_replace(
            &self,
            request: teodb_core::traits::catalog::CommitReplace,
        ) -> TeoDBResult<Arc<teodb_core::file::TableMetadata>> {
            self.inner.commit_replace(request).await
        }

        async fn update_table_properties(
            &self,
            ident: &teodb_core::ident::TableIdent,
            expected: HashMap<String, String>,
            updates: HashMap<String, String>,
            removals: Vec<String>,
        ) -> TeoDBResult<Arc<teodb_core::file::TableMetadata>> {
            self.inner
                .update_table_properties(ident, expected, updates, removals)
                .await
        }
    }

    async fn test_state_with_engine(
        catalog: Arc<dyn Catalog>,
        query_engine: Arc<dyn QueryEngine>,
        query_timeout: Duration,
    ) -> (Arc<AppState>, TempDir) {
        let wal_dir = tempfile::tempdir().unwrap();
        let config = teodb_ingest::config::IngestConfig {
            buffer_max_bytes: 1024 * 1024,
            buffer_soft_watermark_bytes: 768 * 1024,
            flush_interval: Duration::from_secs(60),
            default_warehouse_uri: "s3://test-warehouse".into(),
            idempotency_ttl: Duration::from_secs(60),
            idempotency_max_keys_per_table: 1000,
            commit_status_check: Default::default(),
        };
        let wal = Arc::new(
            WalManager::open(WalConfig {
                root_dir: wal_dir.path().to_path_buf(),
                max_segment_bytes: 16 * 1024 * 1024,
                fsync_on_append: false,
                soft_watermark_bytes: 64 * 1024 * 1024,
                hard_cap_bytes: 256 * 1024 * 1024,
                recovery_mode: WalRecoveryMode::Fail,
                ..Default::default()
            })
            .await
            .unwrap(),
        );
        let buffers = Arc::new(teodb_ingest::buffer::BufferRegistry::new(
            wal.clone(),
            config.buffer_max_bytes,
            config.buffer_soft_watermark_bytes,
        ));
        let idempotency = Arc::new(teodb_ingest::idempotency::IdempotencyIndex::new(
            config.idempotency_ttl,
            config.idempotency_max_keys_per_table,
        ));
        let warehouse_uri: Arc<str> = Arc::from(config.default_warehouse_uri.as_str());
        let storage_factory = stub_storage_factory();
        let lifecycle = RoleLifecycle::new();
        lifecycle.mark_ready();

        let ingest = teodb_ingest::service::IngestService::new(
            catalog.clone(),
            buffers.clone(),
            wal.clone(),
            idempotency.clone(),
            warehouse_uri.clone(),
        );
        let ddl = crate::service::DdlService::new(
            catalog.clone(),
            storage_factory.clone(),
            buffers.clone(),
            wal.clone(),
            idempotency.clone(),
            warehouse_uri,
        );
        let flusher =
            teodb_ingest::flush::Flusher::new(buffers.clone(), catalog.clone(), storage_factory.clone(), wal.clone());
        let api_config = Arc::new(crate::ApiConfig {
            max_body_bytes: 1024 * 1024,
            ..crate::ApiConfig::default()
        });
        let admission = crate::admission::ApiAdmission::new(&api_config);
        let observer: Arc<dyn crate::ApiObserver> = Arc::new(crate::NoopApiObserver);

        (
            Arc::new(AppState {
                services: crate::http::AppServices {
                    catalog,
                    buffers,
                    config: api_config,
                    ingest,
                    ddl,
                    flusher,
                    wal,
                    idempotency,
                    query_engine,
                },
                security: crate::http::AppSecurity {
                    authorization: Arc::new(crate::ApiAuthorization::new(None, observer.clone())),
                    authenticator: Arc::new(crate::security::ApiAuthenticator::new(None, observer)),
                    admin_token: None,
                },
                admission,
                readiness: crate::http::AppReadiness {
                    probes: Vec::new(),
                    cluster_topology: None,
                },
                lifecycle: crate::http::AppLifecycle {
                    role: "test".into(),
                    role_lifecycle: lifecycle,
                    draining: Arc::new(AtomicBool::new(false)),
                    query_timeout,
                    slow_query_threshold: Duration::from_millis(5000),
                    started_at: Instant::now(),
                },
            }),
            wal_dir,
        )
    }

    async fn test_state(catalog: Arc<dyn Catalog>) -> (Arc<AppState>, TempDir) {
        test_state_with_engine(catalog, Arc::new(StubQueryEngine), Duration::from_secs(60)).await
    }

    #[tokio::test]
    async fn flight_statement_update_uses_current_warehouse_location_policy() {
        let catalog = Arc::new(
            MockCatalog::builder()
                .commit_result(table_metadata("s3://test-warehouse/default/flight_location_probe"))
                .build(),
        );
        let catalog_ref: Arc<dyn Catalog> = catalog.clone();
        let (state, _wal_dir) = test_state(catalog_ref).await;
        let principal = Principal {
            subject: "anonymous".into(),
            roles: Vec::new(),
            claims: HashMap::new(),
        };

        let response = execute_statement_update(
            &state,
            &principal,
            "CREATE TABLE default.flight_location_probe (id INTEGER NOT NULL)",
        )
        .await
        .unwrap();
        let mut stream = response.into_inner();
        assert!(stream.next().await.unwrap().is_ok());

        let created = catalog.created_tables();
        assert_eq!(created.len(), 1);
        assert_eq!(
            created[0].location.to_uri(),
            "s3://test-warehouse/default/flight_location_probe"
        );
    }

    fn test_principal() -> Principal {
        Principal {
            subject: "anonymous".into(),
            roles: Vec::new(),
            claims: HashMap::new(),
        }
    }

    #[tokio::test]
    async fn statement_update_cancels_when_planning_times_out() {
        let engine = TimeoutQueryEngine::new(TimeoutMode::Planning);
        let catalog: Arc<dyn Catalog> = Arc::new(MockCatalog::empty());
        let (state, _wal_dir) = test_state_with_engine(catalog, engine.clone(), Duration::from_millis(20)).await;

        let error = execute_statement_update(&state, &test_principal(), "INSERT INTO default.events VALUES (1)")
            .await
            .err()
            .expect("planning must exceed the shared deadline");

        assert_eq!(error.code(), tonic::Code::DeadlineExceeded);
        assert_eq!(engine.cancellations.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn statement_update_cancels_after_first_stream_batch() {
        let engine = TimeoutQueryEngine::new(TimeoutMode::Streaming);
        let catalog: Arc<dyn Catalog> = Arc::new(MockCatalog::empty());
        let (state, _wal_dir) = test_state_with_engine(catalog, engine.clone(), Duration::from_millis(20)).await;

        let error = execute_statement_update(&state, &test_principal(), "INSERT INTO default.events VALUES (1)")
            .await
            .err()
            .expect("streaming must exceed the shared deadline");

        assert_eq!(error.code(), tonic::Code::DeadlineExceeded);
        assert_eq!(engine.cancellations.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn statement_update_deadline_covers_ddl_routing() {
        let catalog: Arc<dyn Catalog> = Arc::new(SlowCreateNamespaceCatalog {
            inner: MockCatalog::empty(),
        });
        let (state, _wal_dir) =
            test_state_with_engine(catalog, Arc::new(StubQueryEngine), Duration::from_millis(20)).await;

        let error = execute_statement_update(&state, &test_principal(), "CREATE SCHEMA delayed")
            .await
            .err()
            .expect("DDL routing must exceed the shared deadline");

        assert_eq!(error.code(), tonic::Code::DeadlineExceeded);
    }
}
