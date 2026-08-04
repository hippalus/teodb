use std::sync::Arc;

use datafusion_proto::logical_plan::LogicalExtensionCodec;
use teodb_core::error::{TeoDBError, TeoDBResult};
use tracing::{info, warn};

use super::{ExecutorConfig, HostPort};

/// Build the `RuntimeProducer` for executor tasks with the registered object store.
pub(super) fn build_runtime_producer(
    temp_dir: std::path::PathBuf,
    object_store: (url::Url, Arc<dyn object_store::ObjectStore>),
    memory_pool_bytes: Option<u64>,
) -> ballista_core::RuntimeProducer {
    Arc::new(move |_session_config| {
        use datafusion::execution::memory_pool::FairSpillPool;
        use datafusion::execution::runtime_env::RuntimeEnvBuilder;

        let mut builder = RuntimeEnvBuilder::new().with_temp_file_path(temp_dir.clone());
        if let Some(bytes) = memory_pool_bytes.filter(|bytes| *bytes > 0) {
            builder = builder.with_memory_pool(Arc::new(FairSpillPool::new(bytes as usize)));
        }
        let runtime_env = builder.build()?;
        runtime_env.register_object_store(&object_store.0, object_store.1.clone());
        Ok(Arc::new(runtime_env))
    })
}

pub(super) fn parse_object_store_url(
    registration: &teodb_query::ObjectStoreRegistration,
) -> TeoDBResult<(url::Url, Arc<dyn object_store::ObjectStore>)> {
    Ok((registration.parsed_url().clone(), registration.store()))
}

/// Start a Ballista executor.
///
/// Connects to the scheduler and registers as an executor node, then runs
/// the task execution loop until shutdown is signalled.
pub async fn start_executor(
    config: ExecutorConfig,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) -> TeoDBResult<()> {
    let scheduler = HostPort::parse(&config.scheduler_url, "cluster.scheduler_addr")?;

    let codec: Arc<dyn LogicalExtensionCodec> = Arc::new(crate::codec::TeoLogicalExtensionCodec::new());
    let object_store = parse_object_store_url(&config.object_store)?;
    let runtime_producer = build_runtime_producer(config.spill_dir.clone(), object_store, config.memory_pool_bytes);

    let executor_config = Arc::new(ballista_executor::executor_process::ExecutorProcessConfig {
        bind_host: config.bind_addr.clone(),
        port: config.bind_port,
        grpc_port: config.grpc_bind_port,
        external_host: config.external_host.clone(),
        scheduler_host: scheduler.host,
        scheduler_port: scheduler.port,
        scheduler_connect_timeout_seconds: config.scheduler_connect_timeout_seconds,
        concurrent_tasks: config.concurrent_tasks as usize,
        work_dir: Some(config.spill_dir.to_string_lossy().to_string()),
        executor_heartbeat_interval_seconds: config.heartbeat_interval_secs,
        memory_pool_size: config.memory_pool_bytes,
        override_logical_codec: Some(codec),
        override_runtime_producer: Some(runtime_producer),
        ..Default::default()
    });

    info!(
        bind = format!("{}:{}", config.bind_addr, config.bind_port),
        advertised = config.external_host.as_deref().unwrap_or("<none>"),
        scheduler = %config.scheduler_url,
        concurrent_tasks = config.concurrent_tasks,
        object_store = config.object_store.url(),
        "starting Ballista executor"
    );

    // Retry forever until shutdown: registration can race the executor's own
    // gRPC server startup, and the scheduler may be temporarily unavailable
    // (restart, network partition). A node must keep rejoining the cluster.
    const MAX_BACKOFF: std::time::Duration = std::time::Duration::from_secs(30);
    let mut backoff = std::time::Duration::from_secs(1);

    loop {
        let attempt_config = executor_config.clone();
        let attempt_started = std::time::Instant::now();
        let mut handle = tokio::spawn(async move {
            ballista_executor::executor_process::start_executor_process(attempt_config)
                .await
                .map_err(|e| TeoDBError::Internal(format!("Ballista executor error: {e}")))
        });

        tokio::select! {
            result = &mut handle => {
                let error = match result {
                    Ok(Ok(())) => {
                        info!("Ballista executor stopped");
                        return Ok(());
                    }
                    Ok(Err(e)) => e.to_string(),
                    Err(e) => format!("executor task panicked: {e}"),
                };

                // A long-lived run means the previous failure streak is over.
                if attempt_started.elapsed() > std::time::Duration::from_secs(60) {
                    backoff = std::time::Duration::from_secs(1);
                }
                warn!(
                    error = %error,
                    retry_in_secs = backoff.as_secs(),
                    "Ballista executor exited; retrying"
                );
            }
            _ = shutdown.changed() => {
                info!("Ballista executor stopping on shutdown signal");
                // Ballista's executor process listens for the same
                // SIGTERM/SIGINT this process received and runs its own
                // graceful stop: fence (Terminating heartbeat), notify the
                // scheduler, drain in-flight tasks, clean shuffle data.
                // Give that drain a bounded window before aborting — an
                // immediate abort kills tasks mid-run and litters shuffle
                // files (F-11).
                wait_then_abort(handle, config.drain_timeout, "executor").await;
                return Ok(());
            }
        }

        tokio::select! {
            _ = tokio::time::sleep(backoff) => {}
            _ = shutdown.changed() => {
                info!("Ballista executor stopping on shutdown signal");
                return Ok(());
            }
        }
        backoff = (backoff * 2).min(MAX_BACKOFF);
    }
}

/// Give a spawned Ballista task `drain_timeout` to exit on its own, then
/// abort it. Zero timeout aborts immediately (the pre-drain behavior).
pub(super) async fn wait_then_abort<T>(
    mut handle: tokio::task::JoinHandle<T>,
    drain_timeout: std::time::Duration,
    component: &str,
) {
    if !drain_timeout.is_zero() {
        match tokio::time::timeout(drain_timeout, &mut handle).await {
            Ok(_) => {
                info!(component, "drained and stopped gracefully");
                return;
            }
            Err(_) => {
                warn!(
                    component,
                    timeout_secs = drain_timeout.as_secs(),
                    "drain window elapsed; aborting"
                );
            }
        }
    }
    handle.abort();
    let _ = handle.await;
}
