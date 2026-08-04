use super::*;
use std::sync::Arc;

use teodb_core::error::TeoDBError;

#[test]
fn parse_host_port_accepts_raw_host_port() {
    let parsed = HostPort::parse("scheduler:50050", "cluster.scheduler_addr").unwrap();
    assert_eq!(parsed.host, "scheduler");
    assert_eq!(parsed.port, 50050);
}

#[test]
fn parse_host_port_accepts_http_url() {
    let parsed = HostPort::parse("http://scheduler:50050", "cluster.scheduler_addr").unwrap();
    assert_eq!(parsed.host, "scheduler");
    assert_eq!(parsed.port, 50050);
}

#[test]
fn parse_host_port_accepts_bracketed_ipv6() {
    let parsed = HostPort::parse("[::1]:50050", "cluster.scheduler_addr").unwrap();
    assert_eq!(parsed.host, "::1");
    assert_eq!(parsed.port, 50050);
}

#[test]
fn parse_host_port_rejects_missing_port() {
    let err = HostPort::parse("http://scheduler", "cluster.scheduler_addr").unwrap_err();
    assert!(err.to_string().contains("explicit port"));
}

#[test]
fn parse_host_port_rejects_paths() {
    let err = HostPort::parse("http://scheduler:50050/path", "cluster.scheduler_addr").unwrap_err();
    assert!(
        err.to_string()
            .contains("must not include a path")
    );
}

#[test]
fn parse_host_port_rejects_unbracketed_ipv6() {
    let err = HostPort::parse("::1:50050", "cluster.scheduler_addr").unwrap_err();
    assert!(err.to_string().contains("bracket IPv6"));
}

#[test]
fn format_host_port_brackets_ipv6_for_socket_addr() {
    assert_eq!(
        HostPort {
            host: "::1".into(),
            port: 50050,
        }
        .authority(),
        "[::1]:50050"
    );
    assert_eq!(
        HostPort {
            host: "127.0.0.1".into(),
            port: 50050,
        }
        .authority(),
        "127.0.0.1:50050"
    );
}

#[test]
fn runtime_producer_registers_object_store() {
    let store: Arc<dyn object_store::ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let registration = teodb_query::ObjectStoreRegistration::new("s3://teodb", store).unwrap();
    let object_store = parse_object_store_url(&registration).unwrap();
    let producer = build_runtime_producer(std::env::temp_dir(), object_store, None);

    let runtime_env = producer(&datafusion::prelude::SessionConfig::new()).unwrap();
    let registered = url::Url::parse("s3://teodb/data/file.parquet").unwrap();
    assert!(
        runtime_env
            .object_store_registry
            .get_store(&registered)
            .is_ok()
    );

    let unregistered = url::Url::parse("s3://other-bucket/file.parquet").unwrap();
    assert!(
        runtime_env
            .object_store_registry
            .get_store(&unregistered)
            .is_err()
    );
}

#[test]
fn runtime_producer_applies_memory_pool_limit() {
    let store: Arc<dyn object_store::ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let registration = teodb_query::ObjectStoreRegistration::new("s3://teodb", store).unwrap();
    let object_store = parse_object_store_url(&registration).unwrap();
    let producer = build_runtime_producer(std::env::temp_dir(), object_store, Some(1024));
    let runtime_env = producer(&datafusion::prelude::SessionConfig::new()).unwrap();
    let reservation =
        datafusion::execution::memory_pool::MemoryConsumer::new("executor-test").register(&runtime_env.memory_pool);

    reservation.try_grow(1024).unwrap();
    assert!(
        reservation.try_grow(1).is_err(),
        "executor RuntimeEnv must enforce configured memory_pool_bytes"
    );
}

#[test]
fn object_store_registration_rejects_invalid_url() {
    let store: Arc<dyn object_store::ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let err = teodb_query::ObjectStoreRegistration::new("not a url", store).unwrap_err();
    assert!(matches!(err, TeoDBError::Config(_)));
}

// Graceful drain

#[tokio::test]
async fn wait_then_abort_returns_as_soon_as_task_finishes() {
    let started = std::time::Instant::now();
    let handle = tokio::spawn(async {
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    });
    wait_then_abort(handle, std::time::Duration::from_secs(30), "test").await;
    assert!(
        started.elapsed() < std::time::Duration::from_secs(5),
        "must not wait out the full drain window for a finished task"
    );
}

#[tokio::test]
async fn wait_then_abort_aborts_after_drain_window() {
    let handle = tokio::spawn(async {
        futures::future::pending::<()>().await;
    });
    let started = std::time::Instant::now();
    wait_then_abort(handle, std::time::Duration::from_millis(50), "test").await;
    let elapsed = started.elapsed();
    assert!(
        elapsed >= std::time::Duration::from_millis(50),
        "must hold the drain window before aborting, waited {elapsed:?}"
    );
    assert!(
        elapsed < std::time::Duration::from_secs(5),
        "abort must follow promptly after the window, waited {elapsed:?}"
    );
}

#[tokio::test]
async fn wait_then_abort_zero_window_aborts_immediately() {
    let handle = tokio::spawn(async {
        futures::future::pending::<()>().await;
    });
    let started = std::time::Instant::now();
    wait_then_abort(handle, std::time::Duration::ZERO, "test").await;
    assert!(started.elapsed() < std::time::Duration::from_secs(1));
}

/// Stub scheduler REST API: first `/api/jobs` request reports a running
/// job, every later one reports none. Returns the request counter.
async fn spawn_jobs_api_stub() -> (std::net::SocketAddr, Arc<std::sync::atomic::AtomicUsize>) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .unwrap();
    let addr = listener.local_addr().unwrap();
    let requests = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let counter = requests.clone();
    tokio::spawn(async move {
        loop {
            let (mut socket, _) = listener.accept().await.unwrap();
            let n = counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let body = if n == 0 {
                r#"[{"job_id":"j1","status":"Running"}]"#
            } else {
                "[]"
            };
            let mut buf = [0u8; 1024];
            let _ = socket.read(&mut buf).await;
            let response = format!(
                "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\nconnection: close\r\ncontent-length: {}\r\n\r\n{}",
                body.len(),
                body
            );
            let _ = socket.write_all(response.as_bytes()).await;
        }
    });
    (addr, requests)
}

#[tokio::test]
async fn drain_scheduler_jobs_waits_until_active_jobs_settle() {
    let (addr, requests) = spawn_jobs_api_stub().await;
    let started = std::time::Instant::now();
    drain_scheduler_jobs(
        &addr.to_string(),
        std::time::Duration::from_secs(30),
        std::time::Duration::from_millis(20),
    )
    .await;
    assert!(
        started.elapsed() < std::time::Duration::from_secs(10),
        "drain must finish once no jobs are active"
    );
    assert!(
        requests.load(std::sync::atomic::Ordering::SeqCst) >= 2,
        "drain must poll past the initial running job"
    );
}

#[tokio::test]
async fn drain_scheduler_jobs_gives_up_when_api_unreachable() {
    // Port 1 on localhost: connection refused, immediately.
    let started = std::time::Instant::now();
    drain_scheduler_jobs(
        "127.0.0.1:1",
        std::time::Duration::from_secs(30),
        std::time::Duration::from_millis(20),
    )
    .await;
    assert!(
        started.elapsed() < std::time::Duration::from_secs(10),
        "an unreachable API must not hold the drain window open"
    );
}

#[tokio::test]
async fn drain_scheduler_jobs_zero_window_returns_immediately() {
    let started = std::time::Instant::now();
    drain_scheduler_jobs(
        "127.0.0.1:1",
        std::time::Duration::ZERO,
        std::time::Duration::from_millis(20),
    )
    .await;
    assert!(started.elapsed() < std::time::Duration::from_secs(1));
}

#[test]
fn scheduler_http_url_normalizes_raw_endpoints() {
    assert_eq!(
        HostPort::parse("scheduler:50050", "cluster.scheduler_addr")
            .unwrap()
            .http_url(),
        "http://scheduler:50050"
    );
    assert_eq!(
        HostPort::parse("[::1]:50050", "cluster.scheduler_addr")
            .unwrap()
            .http_url(),
        "http://[::1]:50050"
    );
}
