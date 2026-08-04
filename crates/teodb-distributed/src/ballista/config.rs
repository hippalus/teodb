/// Configuration for the Ballista scheduler.
#[derive(Debug, Clone)]
pub struct SchedulerConfig {
    pub bind_addr: String,
    pub bind_port: u16,
    pub external_host: String,
    pub executor_timeout_seconds: u64,
    pub expire_dead_executor_interval_seconds: u64,
    /// On shutdown, keep serving until no job is queued or running (polled
    /// via the scheduler's own REST API), bounded by this window. Zero
    /// aborts immediately. Ballista 53 exposes no graceful-stop API, so
    /// this is the only way in-flight distributed queries survive a
    /// scheduler restart.
    pub drain_timeout: std::time::Duration,
}

impl Default for SchedulerConfig {
    fn default() -> Self {
        Self {
            bind_addr: "0.0.0.0".into(),
            bind_port: 50050,
            external_host: "localhost".into(),
            executor_timeout_seconds: 15,
            expire_dead_executor_interval_seconds: 5,
            drain_timeout: std::time::Duration::from_secs(20),
        }
    }
}

/// Configuration for a Ballista executor.
#[derive(Debug, Clone)]
pub struct ExecutorConfig {
    pub scheduler_url: String,
    pub bind_addr: String,
    pub bind_port: u16,
    pub grpc_bind_port: u16,
    /// Hostname advertised to the scheduler and other executors. Must be
    /// routable from every cluster member (e.g. the Docker service name or
    /// the pod DNS name) — the bind address is typically `0.0.0.0` and
    /// cannot serve as an identity.
    pub external_host: Option<String>,
    pub concurrent_tasks: u32,
    pub spill_dir: std::path::PathBuf,
    pub object_store: teodb_query::ObjectStoreRegistration,
    pub scheduler_connect_timeout_seconds: u16,
    pub heartbeat_interval_secs: u64,
    pub memory_pool_bytes: Option<u64>,
    /// On shutdown, give Ballista's own graceful stop (triggered by the same
    /// SIGTERM/SIGINT this process received: fence → notify scheduler →
    /// drain in-flight tasks → clean shuffle data) this long to finish
    /// before aborting. Zero aborts immediately.
    pub drain_timeout: std::time::Duration,
}
