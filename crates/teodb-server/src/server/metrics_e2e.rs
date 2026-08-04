//! End-to-end verification for the production metrics wiring.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use arrow_flight::flight_descriptor::DescriptorType;
use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::sql::{CommandStatementQuery, ProstMessageExt};
use arrow_flight::{FlightDescriptor, Ticket};
use axum::body::Body;
use axum::http::{Request, StatusCode};
use prost::Message as _;
use teodb_core::error::{TeoDBError, TeoDBResult};
use teodb_core::ident::TableIdent;
use teodb_core::traits::authz::{Action, Authorizer, Principal, Resource};
use teodb_ingest::flush::{FlushOutcome, Flusher};
use teodb_test_support::server::TestAppBuilder;
use teodb_test_support::{MockCatalog, in_memory_backend, single_backend_factory, table_metadata};
use tonic::Code;
use tower::ServiceExt as _;

use crate::config::TeoDBConfig;
use crate::metrics::Metrics;

use super::collectors::{self, MetricsApiObserver, MetricsFlushObserver};
use super::incoming::{ConnectionMetrics, IncomingSettings, flight_incoming};

const RESULT_VALUE: &str = "known-rest-and-flight-result";

struct KnownResultEngine;

#[async_trait::async_trait]
impl teodb_query::QueryEngine for KnownResultEngine {
    async fn prepare(&self, request: teodb_query::QueryRequest) -> TeoDBResult<teodb_query::QueryHandle> {
        Ok(teodb_query::QueryHandle {
            query_id: request.query_id,
            schema: Arc::new(Schema::new(vec![Field::new("payload", DataType::Utf8, false)])),
            state: Box::new(()),
        })
    }

    async fn execute_stream(&self, handle: teodb_query::QueryHandle) -> TeoDBResult<teodb_query::QueryResultStream> {
        let batch = RecordBatch::try_new(
            handle.schema.clone(),
            vec![Arc::new(StringArray::from(vec![RESULT_VALUE]))],
        )
        .expect("known result batch");
        Ok(teodb_query::QueryResultStream::new(
            handle.schema,
            Box::pin(futures::stream::iter([Ok(batch)])),
        ))
    }

    async fn cancel(&self, _query_id: &teodb_core::query_id::QueryId) -> TeoDBResult<()> {
        Ok(())
    }

    async fn status(
        &self,
        _query_id: &teodb_core::query_id::QueryId,
    ) -> TeoDBResult<teodb_core::traits::query_engine::QueryStatus> {
        Err(TeoDBError::NotFound {
            resource: "query".into(),
        })
    }
}

struct DenyAction(Action);

#[async_trait::async_trait]
impl Authorizer for DenyAction {
    async fn authorize(&self, _principal: &Principal, action: &Action, _resource: &Resource) -> TeoDBResult<()> {
        if action == &self.0 {
            Err(TeoDBError::Forbidden(format!("{action:?} denied by test policy")))
        } else {
            Ok(())
        }
    }
}

struct FlightHarness {
    client: Option<FlightServiceClient<tonic::transport::Channel>>,
    shutdown: Option<tokio::sync::oneshot::Sender<()>>,
    server: tokio::task::JoinHandle<()>,
}

impl FlightHarness {
    async fn start(state: Arc<teodb_api::http::AppState>, metrics: Arc<Metrics>) -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind Flight test listener");
        let address = listener
            .local_addr()
            .expect("Flight listener address");
        let incoming = flight_incoming(
            listener,
            IncomingSettings::new(
                8,
                Duration::from_secs(30),
                ConnectionMetrics::new(
                    metrics.transport.active_connections.clone(),
                    metrics
                        .transport
                        .admission_rejections_total
                        .clone(),
                ),
                "flight",
            ),
        );
        let service = super::flight::build_flight_server(&state, &TeoDBConfig::default());
        let (shutdown, shutdown_rx) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(service)
                .serve_with_incoming_shutdown(incoming, async {
                    let _ = shutdown_rx.await;
                })
                .await
                .expect("serve Flight test endpoint");
        });
        let channel = tokio::time::timeout(
            Duration::from_secs(5),
            tonic::transport::Endpoint::from_shared(format!("http://{address}"))
                .expect("valid Flight endpoint")
                .connect(),
        )
        .await
        .expect("Flight client connection timed out")
        .expect("connect Flight client");

        Self {
            client: Some(FlightServiceClient::new(channel)),
            shutdown: Some(shutdown),
            server,
        }
    }

    fn client(&mut self) -> &mut FlightServiceClient<tonic::transport::Channel> {
        self.client
            .as_mut()
            .expect("Flight harness is running")
    }

    async fn stop(mut self) {
        self.client.take();
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        tokio::time::timeout(Duration::from_secs(5), self.server)
            .await
            .expect("Flight server shutdown timed out")
            .expect("Flight server task panicked");
    }
}

fn statement_descriptor(sql: &str) -> FlightDescriptor {
    FlightDescriptor {
        r#type: DescriptorType::Cmd as i32,
        cmd: CommandStatementQuery {
            query: sql.into(),
            transaction_id: None,
        }
        .as_any()
        .encode_to_vec()
        .into(),
        path: Vec::new(),
    }
}

fn statement_ticket(sql: &str) -> Ticket {
    Ticket::new(
        CommandStatementQuery {
            query: sql.into(),
            transaction_id: None,
        }
        .as_any()
        .encode_to_vec(),
    )
}

async fn scrape(router: &axum::Router) -> String {
    let response = router
        .clone()
        .oneshot(
            Request::get("/metrics")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("scrape metrics");
    assert_eq!(response.status(), StatusCode::OK);
    let bytes = axum::body::to_bytes(response.into_body(), 4 * 1024 * 1024)
        .await
        .expect("read metrics response");
    String::from_utf8(bytes.to_vec()).expect("Prometheus text is UTF-8")
}

#[tokio::test]
async fn production_metrics_advance_through_real_rest_flight_flush_and_scrape_paths() {
    let metrics = Arc::new(Metrics::new());
    let api_observer: Arc<dyn teodb_api::ApiObserver> = Arc::new(MetricsApiObserver {
        metrics: metrics.clone(),
    });
    let table = TableIdent::new("default", "events");
    let iceberg = table_metadata("file:///metrics/default/events");
    let catalog = Arc::new(
        MockCatalog::builder()
            .namespaces(["default"])
            .tables([table.clone()])
            .serves("events", iceberg.clone())
            .commit_result(iceberg)
            .build(),
    );
    let backend = in_memory_backend();
    let storage_factory = single_backend_factory(backend);
    let ingest_config = teodb_ingest::config::IngestConfig {
        buffer_max_bytes: 4 * 1024,
        buffer_soft_watermark_bytes: 2 * 1024,
        ..teodb_ingest::config::IngestConfig::default()
    };
    let api_config = teodb_api::ApiConfig {
        max_body_bytes: 32 * 1024,
        ..teodb_api::ApiConfig::default()
    };
    let app = TestAppBuilder::rest_api()
        .catalog(catalog.clone())
        .config(ingest_config)
        .storage_factory(storage_factory.clone())
        .query_engine(Arc::new(KnownResultEngine))
        .authorizer(Some(Arc::new(DenyAction(Action::CreateTable))))
        .api_config(api_config)
        .observer(api_observer.clone())
        .build()
        .await;
    let router = super::http::build_http_router(&app.state, &metrics, None, &TeoDBConfig::default());

    let query_body = serde_json::json!({ "sql": "SELECT payload FROM default.events" });
    let query_response = router
        .clone()
        .oneshot(
            Request::post("/api/v1/query")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&query_body).unwrap()))
                .unwrap(),
        )
        .await
        .expect("REST query response");
    assert_eq!(query_response.status(), StatusCode::OK);
    let query_response = axum::body::to_bytes(query_response.into_body(), 1024 * 1024)
        .await
        .expect("read REST query response");
    let query_json: serde_json::Value = serde_json::from_slice(&query_response).expect("REST query JSON");
    let expected_rows = serde_json::json!([{ "payload": RESULT_VALUE }]);
    assert_eq!(query_json["rows"], expected_rows);
    let rest_result_bytes = serde_json::to_vec(&expected_rows).unwrap().len() as u64;

    let rest_authn_denial = router
        .clone()
        .oneshot(
            Request::post("/api/v1/query")
                .header("content-type", "application/json")
                .header("authorization", "Basic invalid")
                .body(Body::from(serde_json::to_vec(&query_body).unwrap()))
                .unwrap(),
        )
        .await
        .expect("REST authentication denial");
    assert_eq!(rest_authn_denial.status(), StatusCode::UNAUTHORIZED);

    let rest_authz_denial = router
        .clone()
        .oneshot(
            Request::post("/api/v1/namespaces")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"namespace":"denied"}"#))
                .unwrap(),
        )
        .await
        .expect("REST authorization denial");
    assert_eq!(rest_authz_denial.status(), StatusCode::FORBIDDEN);

    let ingest_body = serde_json::json!({ "rows": [{ "id": 1 }] });
    let accepted = router
        .clone()
        .oneshot(
            Request::post("/api/v1/tables/default/events/ingest")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&ingest_body).unwrap()))
                .unwrap(),
        )
        .await
        .expect("accepted ingest response");
    assert_eq!(accepted.status(), StatusCode::ACCEPTED);

    tokio::time::sleep(Duration::from_millis(1_100)).await;
    let buffer = app
        .state
        .services
        .buffers
        .get(&table)
        .expect("ingest loaded the table buffer");
    let reservation_batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)])),
        vec![Arc::new(Int64Array::from(vec![2_i64]))],
    )
    .expect("reservation batch");
    let held_reservation = buffer
        .reserve(&reservation_batch)
        .expect("hold capacity reservation");
    let mut previous_tables = HashSet::new();
    collectors::collect_gauges_once(&metrics, &app.state.services.buffers, None, &mut previous_tables);
    assert!(metrics.buffer.reserved_bytes.get() > 0);
    assert!(
        metrics
            .buffer
            .oldest_pending_age_seconds
            .with_label_values(&["default", "events"])
            .get()
            >= 1
    );
    let pending_scrape = scrape(&router).await;
    assert!(pending_scrape.contains("teodb_buffer_reserved_bytes"));
    assert!(pending_scrape.contains("teodb_buffer_oldest_pending_age_seconds"));
    assert!(pending_scrape.contains("namespace=\"default\""));
    assert!(pending_scrape.contains("table=\"events\""));

    let too_many_rows = (0..1_000)
        .map(|id| serde_json::json!({ "id": id }))
        .collect::<Vec<_>>();
    let rejected_write_body = serde_json::json!({ "rows": too_many_rows });
    let rejected_write = router
        .clone()
        .oneshot(
            Request::post("/api/v1/tables/default/events/ingest")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&rejected_write_body).unwrap()))
                .unwrap(),
        )
        .await
        .expect("write rejection response");
    assert_eq!(rejected_write.status(), StatusCode::SERVICE_UNAVAILABLE);

    let oversized_body = vec![b'x'; 32 * 1024 + 1];
    let admission_rejection = router
        .clone()
        .oneshot(
            Request::post("/api/v1/tables/default/events/ingest")
                .header("content-type", "application/json")
                .header("content-length", oversized_body.len().to_string())
                .body(Body::from(oversized_body))
                .unwrap(),
        )
        .await
        .expect("API admission rejection response");
    assert_eq!(admission_rejection.status(), StatusCode::PAYLOAD_TOO_LARGE);

    buffer.release_reservation(held_reservation);
    collectors::collect_gauges_once(&metrics, &app.state.services.buffers, None, &mut previous_tables);
    assert_eq!(metrics.buffer.reserved_bytes.get(), 0);

    let flusher = Flusher::new(
        app.state.services.buffers.clone(),
        app.state.services.catalog.clone(),
        storage_factory,
        app.state.services.wal.clone(),
    )
    .with_observer(Arc::new(MetricsFlushObserver {
        metrics: metrics.clone(),
    }));
    assert!(matches!(
        flusher
            .flush_table(&table)
            .await
            .expect("flush pending batch"),
        FlushOutcome::Committed { record_count: 1, .. }
    ));
    assert_eq!(catalog.commit_append_calls(), 1);
    assert!(
        metrics
            .flush
            .visibility_lag_seconds
            .with_label_values(&["default", "events"])
            .get()
            >= 1
    );

    let mut flight = FlightHarness::start(app.state.clone(), metrics.clone()).await;
    let mut flight_stream = flight
        .client()
        .do_get(statement_ticket("SELECT payload FROM default.events"))
        .await
        .expect("Flight query response")
        .into_inner();
    assert!(
        metrics
            .transport
            .active_connections
            .with_label_values(&["flight"])
            .get()
            > 0
    );
    let mut flight_result_bytes = 0_u64;
    while let Some(data) = flight_stream
        .message()
        .await
        .expect("read Flight result")
    {
        flight_result_bytes = flight_result_bytes.saturating_add(
            u64::try_from(
                data.data_header
                    .len()
                    .saturating_add(data.data_body.len())
                    .saturating_add(data.app_metadata.len()),
            )
            .unwrap(),
        );
    }
    assert!(flight_result_bytes > 0);
    flight.stop().await;

    let denied_flight_app = TestAppBuilder::rest_api()
        .query_engine(Arc::new(KnownResultEngine))
        .authorizer(Some(Arc::new(DenyAction(Action::Query))))
        .observer(api_observer.clone())
        .build()
        .await;
    let mut denied_flight = FlightHarness::start(denied_flight_app.state.clone(), metrics.clone()).await;
    let flight_authz_denial = denied_flight
        .client()
        .get_flight_info(statement_descriptor("SELECT payload FROM default.events"))
        .await
        .expect_err("Flight query authorization must be denied");
    assert_eq!(flight_authz_denial.code(), Code::PermissionDenied);
    denied_flight.stop().await;

    let jwt_flight_app = TestAppBuilder::rest_api()
        .observer(api_observer)
        .jwt_validator(Arc::new(teodb_api::security::JwtValidator::with_secret(
            b"metrics-e2e-secret-key-at-least-32-bytes",
            Default::default(),
        )))
        .build()
        .await;
    let mut jwt_flight = FlightHarness::start(jwt_flight_app.state.clone(), metrics.clone()).await;
    let flight_authn_denial = jwt_flight
        .client()
        .get_flight_info(statement_descriptor("SELECT 1"))
        .await
        .expect_err("Flight request without required JWT must be denied");
    assert_eq!(flight_authn_denial.code(), Code::Unauthenticated);
    jwt_flight.stop().await;

    assert_eq!(
        metrics
            .transport
            .active_connections
            .with_label_values(&["flight"])
            .get(),
        0
    );
    assert_eq!(
        metrics
            .transport
            .result_bytes_total
            .with_label_values(&["rest", "query"])
            .get(),
        rest_result_bytes
    );
    assert_eq!(
        metrics
            .transport
            .result_bytes_total
            .with_label_values(&["flight", "query"])
            .get(),
        flight_result_bytes
    );
    assert_eq!(
        metrics
            .transport
            .admission_rejections_total
            .with_label_values(&["rest", "request_body"])
            .get(),
        1
    );
    assert_eq!(
        metrics
            .ingest
            .rejected_writes_total
            .with_label_values(&["buffer_capacity"])
            .get(),
        1
    );
    assert_eq!(
        metrics
            .security
            .auth_total
            .with_label_values(&["rest", "failed", "malformed"])
            .get(),
        1
    );
    assert_eq!(
        metrics
            .security
            .auth_total
            .with_label_values(&["flight", "failed", "missing"])
            .get(),
        1
    );
    assert_eq!(
        metrics
            .security
            .authz_total
            .with_label_values(&["rest", "denied", "create_table", "namespace"])
            .get(),
        1
    );
    assert_eq!(
        metrics
            .security
            .authz_total
            .with_label_values(&["flight", "denied", "query", "cluster"])
            .get(),
        1
    );
    assert_eq!(metrics.flush.rows_total.get(), 1);

    collectors::collect_gauges_once(&metrics, &app.state.services.buffers, None, &mut previous_tables);
    let committed_scrape = scrape(&router).await;
    for family in [
        "teodb_buffer_reserved_bytes",
        "teodb_flush_visibility_lag_seconds",
        "teodb_ingest_rejected_writes_total",
        "teodb_transport_admission_rejections_total",
        "teodb_auth_total",
        "teodb_authz_total",
        "teodb_transport_result_bytes_total",
        "teodb_transport_active_connections",
    ] {
        assert!(committed_scrape.contains(family), "missing metric family {family}");
    }
    assert!(committed_scrape.contains("transport=\"rest\""));
    assert!(committed_scrape.contains("transport=\"flight\""));
    assert!(committed_scrape.contains("teodb_flush_visibility_lag_seconds"));
    assert!(committed_scrape.contains("namespace=\"default\""));
    assert!(committed_scrape.contains("table=\"events\""));

    let dropped = router
        .clone()
        .oneshot(
            Request::delete("/api/v1/namespaces/default/tables/events")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("drop table response");
    assert_eq!(dropped.status(), StatusCode::NO_CONTENT);
    collectors::collect_gauges_once(&metrics, &app.state.services.buffers, None, &mut previous_tables);
    let final_scrape = scrape(&router).await;
    assert!(!final_scrape.contains("namespace=\"default\""));
    assert!(!final_scrape.contains("table=\"events\""));
    assert!(final_scrape.contains("teodb_transport_active_connections{transport=\"flight\"} 0"));
}
