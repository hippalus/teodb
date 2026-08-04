//! Integration tests for the TeoDB REST API.
//!
//! These tests spin up an in-memory router (no network) using
//! `axum::body::Body` + `tower::ServiceExt::oneshot` and validate
//! RFC 9457, Richardson Maturity Model compliance, and endpoint behavior.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use arrow::array::StringArray;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use axum::body::{Body, Bytes};
use axum::http::{Request, StatusCode};
use tower::ServiceExt;

use teodb_core::error::{TeoDBError, TeoDBResult};
use teodb_core::location::ObjectPath;
use teodb_core::traits::storage::Storage;
use teodb_test_support::server::TestAppBuilder;
use teodb_test_support::{MockCatalog, in_memory_backend, single_backend_factory, table_metadata};

// Deny-all authorizer

struct DenyAllAuthorizer;

#[async_trait::async_trait]
impl teodb_core::traits::authz::Authorizer for DenyAllAuthorizer {
    async fn authorize(
        &self,
        principal: &teodb_core::traits::authz::Principal,
        action: &teodb_core::traits::authz::Action,
        _resource: &teodb_core::traits::authz::Resource,
    ) -> TeoDBResult<()> {
        Err(TeoDBError::Forbidden(format!(
            "{} may not {action:?}",
            principal.subject
        )))
    }
}

struct LargeResultEngine {
    cancellations: AtomicUsize,
}

#[async_trait::async_trait]
impl teodb_query::QueryEngine for LargeResultEngine {
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
            vec![Arc::new(StringArray::from(vec!["x".repeat(4_096)]))],
        )
        .unwrap();
        Ok(teodb_query::QueryResultStream::new(
            handle.schema,
            Box::pin(futures::stream::iter([Ok(batch)])),
        ))
    }

    async fn cancel(&self, _query_id: &teodb_core::query_id::QueryId) -> TeoDBResult<()> {
        self.cancellations.fetch_add(1, Ordering::Relaxed);
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

/// Build an in-memory app. The returned `TempDir` owns the WAL directory and
/// must stay alive for the lifetime of the router.
async fn build_app_with_authorizer(
    authorizer: Option<Arc<dyn teodb_core::traits::authz::Authorizer>>,
) -> (axum::Router, tempfile::TempDir) {
    build_app_with_security(authorizer, None).await
}

async fn build_app_with_security(
    authorizer: Option<Arc<dyn teodb_core::traits::authz::Authorizer>>,
    admin_token: Option<String>,
) -> (axum::Router, tempfile::TempDir) {
    TestAppBuilder::rest_api()
        .authorizer(authorizer)
        .admin_token(admin_token)
        .build()
        .await
        .into_router_and_wal_dir()
}

async fn build_app() -> (axum::Router, tempfile::TempDir) {
    build_app_with_authorizer(None).await
}

// Tests

#[tokio::test]
async fn liveness_probe_returns_ok() {
    let (app, _wal_dir) = build_app().await;
    let req = Request::get("/live").body(Body::empty()).unwrap();
    let res = app.oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
}

#[tokio::test]
async fn readiness_probe_returns_ok_with_all_components() {
    let (app, _wal_dir) = build_app().await;
    let req = Request::get("/ready")
        .body(Body::empty())
        .unwrap();
    let res = app.oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);

    let body = axum::body::to_bytes(res.into_body(), 1024 * 1024)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["status"], "ready");
    let checks = json["checks"].as_array().unwrap();
    for name in ["catalog", "wal", "query_engine", "storage"] {
        assert!(
            checks
                .iter()
                .any(|c| c["name"] == name && c["status"] == "pass"),
            "readiness must report '{name}' as pass"
        );
    }
}

#[tokio::test]
async fn list_namespaces_returns_json_with_links() {
    let (app, _wal_dir) = build_app().await;
    let req = Request::get("/api/v1/namespaces")
        .body(Body::empty())
        .unwrap();
    let res = app.oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);

    let body = axum::body::to_bytes(res.into_body(), 1024 * 1024)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

    // Richardson Level 3: HATEOAS — response has _links
    assert!(json.get("_links").is_some(), "response must contain _links for HATEOAS");
    // Data is flattened into root via #[serde(flatten)]
    assert!(json.get("namespaces").is_some(), "response must contain namespaces");
}

#[tokio::test]
async fn get_namespace_returns_tables_link() {
    let (app, _wal_dir) = build_app().await;
    let req = Request::get("/api/v1/namespaces/default")
        .body(Body::empty())
        .unwrap();
    let res = app.oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);

    let body = axum::body::to_bytes(res.into_body(), 1024 * 1024)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let links = json.get("_links").expect("must have _links");
    assert!(links.get("tables").is_some(), "namespace response must link to tables");
}

#[tokio::test]
async fn list_tables_returns_table_entries() {
    let (app, _wal_dir) = build_app().await;
    let req = Request::get("/api/v1/namespaces/default/tables")
        .body(Body::empty())
        .unwrap();
    let res = app.oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);

    let body = axum::body::to_bytes(res.into_body(), 1024 * 1024)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    // Tables are flattened at root level
    let tables = json.get("tables").expect("must have tables");
    assert!(tables.is_array());
}

#[tokio::test]
async fn get_table_returns_schema() {
    let (app, _wal_dir) = build_app().await;
    let req = Request::get("/api/v1/namespaces/default/tables/events")
        .body(Body::empty())
        .unwrap();
    let res = app.oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);

    let body = axum::body::to_bytes(res.into_body(), 1024 * 1024)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    // TableMetadataResponse fields are flattened at root
    assert!(json.get("name").is_some(), "table metadata must include name");
    assert!(
        json.get("current_schema_id").is_some(),
        "table metadata must include current_schema_id"
    );
}

#[tokio::test]
async fn not_found_returns_problem_json() {
    let (app, _wal_dir) = build_app().await;
    let req = Request::get("/api/v1/namespaces/default/tables/nonexistent")
        .body(Body::empty())
        .unwrap();
    let res = app.oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::NOT_FOUND);

    let content_type = res
        .headers()
        .get("content-type")
        .expect("must have content-type");
    assert!(
        content_type
            .to_str()
            .unwrap()
            .contains("application/problem+json"),
        "error responses must use application/problem+json per RFC 9457"
    );

    let body = axum::body::to_bytes(res.into_body(), 1024 * 1024)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

    // RFC 9457 required fields
    assert!(json.get("type").is_some(), "problem detail must have 'type'");
    assert!(json.get("status").is_some(), "problem detail must have 'status'");
    assert!(json.get("title").is_some(), "problem detail must have 'title'");
}

#[tokio::test]
async fn create_table_returns_201() {
    let (app, _wal_dir) = build_app().await;
    let body = serde_json::json!({
        "name": "new_table",
        "columns": [
            { "name": "id", "data_type": "int64", "nullable": false }
        ]
    });

    let req = Request::post("/api/v1/namespaces/default/tables")
        .header("content-type", "application/json")
        .body(Body::from(serde_json::to_vec(&body).unwrap()))
        .unwrap();

    let res = app.oneshot(req).await.unwrap();
    // Richardson Level 2: 201 Created for resource creation
    assert_eq!(res.status(), StatusCode::CREATED);

    // Richardson Level 2: Location header for created resource
    assert!(
        res.headers().get("location").is_some(),
        "201 Created must include Location header per Richardson Level 2"
    );
}

#[tokio::test]
async fn create_table_uses_current_warehouse_location_policy() {
    let catalog = Arc::new(
        MockCatalog::builder()
            .namespaces(["default"])
            .commit_result(table_metadata("s3://unused/result"))
            .build(),
    );
    let test_app = TestAppBuilder::rest_api()
        .catalog(catalog.clone())
        .build()
        .await;
    let app = test_app.router;
    let _wal_dir = test_app.wal_dir;
    let body = serde_json::json!({
        "name": "location_probe",
        "columns": [
            { "name": "id", "data_type": "int64", "nullable": false }
        ]
    });

    let req = Request::post("/api/v1/namespaces/default/tables")
        .header("content-type", "application/json")
        .body(Body::from(serde_json::to_vec(&body).unwrap()))
        .unwrap();

    let res = app.oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::CREATED);

    let created = catalog.created_tables();
    assert_eq!(created.len(), 1);
    assert_eq!(
        created[0].location.to_uri(),
        "s3://test-warehouse/default/location_probe"
    );
}

#[tokio::test]
async fn ingest_json_rows_returns_202() {
    let (app, _wal_dir) = build_app().await;
    let body = serde_json::json!({
        "rows": [
            { "id": 1 },
            { "id": 2 }
        ]
    });

    let req = Request::post("/api/v1/tables/default/events/ingest")
        .header("content-type", "application/json")
        .body(Body::from(serde_json::to_vec(&body).unwrap()))
        .unwrap();

    let res = app.oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::ACCEPTED);

    let body = axum::body::to_bytes(res.into_body(), 1024 * 1024)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["accepted_rows"], 2);
    assert!(json.get("batch_id").is_some(), "ingest response must include batch_id");
    assert!(
        uuid::Uuid::parse_str(
            json["writer_id"]
                .as_str()
                .expect("writer_id string")
        )
        .is_ok(),
        "ingest response must include the stable writer identity"
    );
    assert_eq!(json["generation"], 1);
}

#[tokio::test]
async fn configured_request_body_limit_returns_problem_413() {
    let (app, _wal_dir) = build_app().await;
    let body = vec![b'x'; 1024 * 1024 + 1];
    let req = Request::post("/api/v1/tables/default/events/ingest")
        .header("content-type", "application/json")
        .header("content-length", body.len().to_string())
        .body(Body::from(body))
        .unwrap();

    let response = app.oneshot(req).await.unwrap();
    assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
    assert_eq!(response.headers()["content-type"], "application/problem+json");
}

#[tokio::test]
async fn encoded_result_limit_cancels_query_and_returns_problem_413() {
    let engine = Arc::new(LargeResultEngine {
        cancellations: AtomicUsize::new(0),
    });
    let app = TestAppBuilder::rest_api()
        .query_engine(engine.clone())
        .api_config(teodb_api::ApiConfig {
            max_body_bytes: 1024 * 1024,
            max_result_bytes: 128,
            ..teodb_api::ApiConfig::default()
        })
        .build()
        .await;
    let body = serde_json::json!({
        "sql": "SELECT payload FROM default.events"
    });
    let request = Request::post("/api/v1/query")
        .header("content-type", "application/json")
        .body(Body::from(serde_json::to_vec(&body).unwrap()))
        .unwrap();

    let response = app.router.oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
    let body = axum::body::to_bytes(response.into_body(), 1024 * 1024)
        .await
        .unwrap();
    let problem: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert!(
        problem["detail"]
            .as_str()
            .unwrap()
            .contains("Arrow Flight")
    );
    assert_eq!(engine.cancellations.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn ingest_with_same_idempotency_key_deduplicates() {
    let (app, _wal_dir) = build_app().await;
    let body = serde_json::json!({
        "rows": [ { "id": 1 }, { "id": 2 } ],
        "idempotency_key": "retry-123"
    });
    let request = || {
        Request::post("/api/v1/tables/default/events/ingest")
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_vec(&body).unwrap()))
            .unwrap()
    };

    let first = app.clone().oneshot(request()).await.unwrap();
    assert_eq!(first.status(), StatusCode::ACCEPTED);
    let first_body = axum::body::to_bytes(first.into_body(), 1024 * 1024)
        .await
        .unwrap();
    let first_json: serde_json::Value = serde_json::from_slice(&first_body).unwrap();
    assert_eq!(first_json["deduplicated"], false);

    // Retry with the same key: 200 (not 202), original receipt, no new rows.
    let second = app.clone().oneshot(request()).await.unwrap();
    assert_eq!(second.status(), StatusCode::OK);
    let second_body = axum::body::to_bytes(second.into_body(), 1024 * 1024)
        .await
        .unwrap();
    let second_json: serde_json::Value = serde_json::from_slice(&second_body).unwrap();
    assert_eq!(second_json["deduplicated"], true);
    assert_eq!(second_json["accepted_rows"], 2);
    assert_eq!(
        second_json["batch_id"], first_json["batch_id"],
        "duplicate must return the original receipt"
    );

    // A different key ingests normally.
    let other = serde_json::json!({
        "rows": [ { "id": 3 } ],
        "idempotency_key": "retry-456"
    });
    let req = Request::post("/api/v1/tables/default/events/ingest")
        .header("content-type", "application/json")
        .body(Body::from(serde_json::to_vec(&other).unwrap()))
        .unwrap();
    let third = app.oneshot(req).await.unwrap();
    assert_eq!(third.status(), StatusCode::ACCEPTED);
}

#[tokio::test]
async fn create_namespace_denied_by_authorizer() {
    let (app, _wal_dir) = build_app_with_authorizer(Some(Arc::new(DenyAllAuthorizer))).await;
    let body = serde_json::json!({ "namespace": "blocked" });

    let req = Request::post("/api/v1/namespaces")
        .header("content-type", "application/json")
        .body(Body::from(serde_json::to_vec(&body).unwrap()))
        .unwrap();

    let res = app.oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn drop_namespace_denied_by_authorizer() {
    let (app, _wal_dir) = build_app_with_authorizer(Some(Arc::new(DenyAllAuthorizer))).await;
    let req = Request::delete("/api/v1/namespaces/default")
        .body(Body::empty())
        .unwrap();

    let res = app.oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn admin_endpoints_denied_by_authorizer() {
    for path in [
        "/api/v1/admin/status",
        "/api/v1/admin/tables",
        "/api/v1/admin/cluster",
        "/api/v1/admin/flush-blocked",
    ] {
        let (app, _wal_dir) = build_app_with_authorizer(Some(Arc::new(DenyAllAuthorizer))).await;
        let req = Request::get(path).body(Body::empty()).unwrap();
        let res = app.oneshot(req).await.unwrap();
        assert_eq!(res.status(), StatusCode::FORBIDDEN, "{path} must require authorization");
    }
}

#[tokio::test]
async fn admin_cluster_reports_role_in_anonymous_mode() {
    let (app, _wal_dir) = build_app().await;
    let req = Request::get("/api/v1/admin/cluster")
        .body(Body::empty())
        .unwrap();
    let res = app.oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);

    let body = axum::body::to_bytes(res.into_body(), 1024 * 1024)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["mode"], "test");
    assert!(uuid::Uuid::parse_str(json["cluster_id"].as_str().unwrap()).is_ok());
    assert!(uuid::Uuid::parse_str(json["writer_id"].as_str().unwrap()).is_ok());
    assert!(
        json["writer_epoch"]
            .as_u64()
            .is_some_and(|epoch| epoch > 0)
    );
    assert_eq!(json["recovery_status"], "complete");
    assert!(json["pending_tables"].as_u64().is_some());
    assert!(json["blocked_tables"].as_u64().is_some());
    assert!(json["wal_segments"].as_u64().is_some());
    assert!(json["wal_bytes"].as_u64().is_some());
}

#[tokio::test]
async fn blocked_flush_admin_surface_is_exact_and_has_no_force_resolve() {
    let (app, _wal_dir) = build_app().await;
    let req = Request::get("/api/v1/admin/flush-blocked")
        .body(Body::empty())
        .unwrap();
    let res = app.clone().oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let body = axum::body::to_bytes(res.into_body(), 1024 * 1024)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json, serde_json::json!([]));

    let req = Request::post("/api/v1/admin/flush-blocked/default/events/recheck")
        .body(Body::empty())
        .unwrap();
    let res = app.oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn drop_table_returns_204() {
    let (app, _wal_dir) = build_app().await;
    let req = Request::delete("/api/v1/namespaces/default/tables/events")
        .body(Body::empty())
        .unwrap();
    let res = app.oneshot(req).await.unwrap();
    // Richardson Level 2: 204 No Content for deletion
    assert_eq!(res.status(), StatusCode::NO_CONTENT);
}

#[tokio::test]
async fn drop_table_without_purge_keeps_object_store_data() {
    let backend = in_memory_backend();
    backend
        .put(
            &ObjectPath::new("default/events/data/file.parquet"),
            Bytes::from_static(b"table data"),
        )
        .await
        .unwrap();

    let catalog = Arc::new(
        MockCatalog::builder()
            .serves("events", table_metadata("s3://test-warehouse/default/events"))
            .build(),
    );
    let test_app = TestAppBuilder::rest_api()
        .catalog(catalog.clone())
        .storage_factory(single_backend_factory(backend.clone()))
        .build()
        .await;

    let req = Request::delete("/api/v1/namespaces/default/tables/events")
        .body(Body::empty())
        .unwrap();
    let res = test_app.router.oneshot(req).await.unwrap();

    assert_eq!(res.status(), StatusCode::NO_CONTENT);
    assert_eq!(catalog.drop_table_calls(), 1);
    assert_eq!(catalog.load_table_calls(), 0);
    assert!(
        backend
            .head(&ObjectPath::new("default/events/data/file.parquet"))
            .await
            .is_ok()
    );
}

#[tokio::test]
async fn drop_table_with_purge_deletes_only_table_prefix() {
    let backend = in_memory_backend();
    for (path, body) in [
        ("default/events/data/file.parquet", b"table data".as_slice()),
        ("default/events/metadata/v1.metadata.json", b"metadata".as_slice()),
        ("default/events2/data/file.parquet", b"sibling".as_slice()),
    ] {
        backend
            .put(&ObjectPath::new(path), Bytes::copy_from_slice(body))
            .await
            .unwrap();
    }

    let catalog = Arc::new(
        MockCatalog::builder()
            .serves("events", table_metadata("s3://test-warehouse/default/events"))
            .build(),
    );
    let test_app = TestAppBuilder::rest_api()
        .catalog(catalog.clone())
        .storage_factory(single_backend_factory(backend.clone()))
        .build()
        .await;

    let req = Request::delete("/api/v1/namespaces/default/tables/events?purge=true")
        .body(Body::empty())
        .unwrap();
    let res = test_app.router.oneshot(req).await.unwrap();

    assert_eq!(res.status(), StatusCode::NO_CONTENT);
    assert_eq!(catalog.drop_table_calls(), 1);
    assert_eq!(catalog.load_table_calls(), 1);
    assert!(
        backend
            .head(&ObjectPath::new("default/events/data/file.parquet"))
            .await
            .is_err()
    );
    assert!(
        backend
            .head(&ObjectPath::new("default/events/metadata/v1.metadata.json"))
            .await
            .is_err()
    );
    assert!(
        backend
            .head(&ObjectPath::new("default/events2/data/file.parquet"))
            .await
            .is_ok()
    );
}

#[tokio::test]
async fn admin_endpoints_require_configured_token() {
    let (app, _wal_dir) = build_app_with_security(None, Some("s3cret".into())).await;

    for path in [
        "/api/v1/admin/status",
        "/api/v1/admin/tables",
        "/api/v1/admin/cluster",
        "/api/v1/admin/flush-blocked",
    ] {
        // No token → 401 with a ProblemDetail body.
        let req = Request::get(path).body(Body::empty()).unwrap();
        let res = app.clone().oneshot(req).await.unwrap();
        assert_eq!(res.status(), StatusCode::UNAUTHORIZED, "{path} without token");

        // Wrong token → 401.
        let req = Request::get(path)
            .header("authorization", "Bearer wrong")
            .body(Body::empty())
            .unwrap();
        let res = app.clone().oneshot(req).await.unwrap();
        assert_eq!(res.status(), StatusCode::UNAUTHORIZED, "{path} with wrong token");

        // Correct token → allowed.
        let req = Request::get(path)
            .header("authorization", "Bearer s3cret")
            .body(Body::empty())
            .unwrap();
        let res = app.clone().oneshot(req).await.unwrap();
        assert_eq!(res.status(), StatusCode::OK, "{path} with correct token");
    }
}

#[tokio::test]
async fn admin_endpoints_open_without_configured_token() {
    // Dev/standalone default: no admin token configured → endpoints stay
    // open (the server warns at startup).
    let (app, _wal_dir) = build_app_with_security(None, None).await;
    let req = Request::get("/api/v1/admin/status")
        .body(Body::empty())
        .unwrap();
    let res = app.oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
}

#[tokio::test]
async fn admin_token_does_not_gate_data_endpoints() {
    let (app, _wal_dir) = build_app_with_security(None, Some("s3cret".into())).await;
    let req = Request::get("/api/v1/namespaces")
        .body(Body::empty())
        .unwrap();
    let res = app.oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK, "data endpoints unaffected by admin token");
}
