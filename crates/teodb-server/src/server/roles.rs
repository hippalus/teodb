//! Cluster role helpers: config builders for Ballista scheduler and executor.

use crate::config::TeoDBConfig;

pub(crate) fn scheduler_config_from(
    cfg: &TeoDBConfig,
) -> teodb_core::TeoDBResult<teodb_distributed::ballista::SchedulerConfig> {
    let bind = teodb_distributed::ballista::HostPort::parse(&cfg.cluster.scheduler_bind, "cluster.scheduler_bind")?;
    let advertised =
        teodb_distributed::ballista::HostPort::parse(&cfg.cluster.scheduler_addr, "cluster.scheduler_addr")?;
    let executor_timeout_seconds =
        cfg.cluster.heartbeat_interval_secs * u64::from(cfg.cluster.heartbeat_miss_threshold);

    Ok(teodb_distributed::ballista::SchedulerConfig {
        bind_addr: bind.host,
        bind_port: bind.port,
        external_host: advertised.host,
        executor_timeout_seconds,
        expire_dead_executor_interval_seconds: cfg.cluster.heartbeat_interval_secs,
        drain_timeout: std::time::Duration::from_secs(cfg.cluster.drain_timeout_secs),
    })
}

/// Resolve the hostname this data node's executor advertises to the cluster.
/// Explicit config wins; otherwise fall back to the system hostname so the
/// executor never registers with an unroutable bind address like `0.0.0.0`.
fn resolve_advertise_host(cfg: &TeoDBConfig) -> teodb_core::TeoDBResult<String> {
    if let Some(ref host) = cfg.cluster.executor_advertise_host {
        let trimmed = host.trim();
        if trimmed.is_empty() {
            return Err(teodb_core::error::TeoDBError::Config(
                "cluster.executor_advertise_host must not be empty".into(),
            ));
        }
        return Ok(trimmed.to_owned());
    }
    gethostname::gethostname()
        .into_string()
        .map_err(|_| teodb_core::error::TeoDBError::Config("system hostname is not valid UTF-8".into()))
}

pub(crate) fn executor_config_from(
    cfg: &TeoDBConfig,
    object_store: teodb_query::ObjectStoreRegistration,
) -> teodb_core::TeoDBResult<teodb_distributed::ballista::ExecutorConfig> {
    let bind = teodb_distributed::ballista::HostPort::parse(&cfg.cluster.executor_bind, "cluster.executor_bind")?;
    let concurrent_tasks = if cfg.cluster.executor_task_slots == 0 {
        std::thread::available_parallelism().map_or(4, |n| n.get())
    } else {
        cfg.cluster.executor_task_slots
    };
    let concurrent_tasks = u32::try_from(concurrent_tasks)
        .map_err(|e| teodb_core::error::TeoDBError::Config(format!("cluster.executor_task_slots is too large: {e}")))?;

    Ok(teodb_distributed::ballista::ExecutorConfig {
        scheduler_url: cfg.cluster.scheduler_addr.clone(),
        bind_addr: bind.host,
        bind_port: bind.port,
        grpc_bind_port: cfg.cluster.executor_grpc_bind_port,
        external_host: Some(resolve_advertise_host(cfg)?),
        concurrent_tasks,
        spill_dir: cfg.storage.spill_dir.clone(),
        object_store,
        scheduler_connect_timeout_seconds: 0,
        heartbeat_interval_secs: cfg.cluster.heartbeat_interval_secs,
        memory_pool_bytes: Some(cfg.query.memory_pool_bytes),
        drain_timeout: std::time::Duration::from_secs(cfg.cluster.drain_timeout_secs),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::TeoDBConfig;
    use std::sync::Arc;

    fn test_object_store() -> teodb_query::ObjectStoreRegistration {
        teodb_query::ObjectStoreRegistration::new("s3://teodb", Arc::new(object_store::memory::InMemory::new()))
            .unwrap()
    }

    #[test]
    fn scheduler_config_uses_cluster_config() {
        let mut cfg = TeoDBConfig::default();
        cfg.cluster.scheduler_bind = "0.0.0.0:50070".into();
        cfg.cluster.scheduler_addr = "control-plane:50070".into();
        cfg.cluster.heartbeat_interval_secs = 7;
        cfg.cluster.heartbeat_miss_threshold = 4;
        cfg.cluster.drain_timeout_secs = 11;

        let config = scheduler_config_from(&cfg).unwrap();

        assert_eq!(config.bind_addr, "0.0.0.0");
        assert_eq!(config.bind_port, 50070);
        assert_eq!(config.external_host, "control-plane");
        assert_eq!(config.executor_timeout_seconds, 28);
        assert_eq!(config.expire_dead_executor_interval_seconds, 7);
        assert_eq!(config.drain_timeout, std::time::Duration::from_secs(11));
    }

    #[test]
    fn executor_config_uses_cluster_query_and_storage_config() {
        let mut cfg = TeoDBConfig::default();
        cfg.cluster.scheduler_addr = "control-plane:50050".into();
        cfg.cluster.executor_bind = "0.0.0.0:50071".into();
        cfg.cluster.executor_grpc_bind_port = 50072;
        cfg.cluster.executor_task_slots = 6;
        cfg.cluster.heartbeat_interval_secs = 9;
        cfg.cluster.drain_timeout_secs = 13;
        cfg.query.memory_pool_bytes = 123_456;
        cfg.storage.spill_dir = "/tmp/teodb-test-spill".into();

        let config = executor_config_from(&cfg, test_object_store()).unwrap();

        assert_eq!(config.scheduler_url, "control-plane:50050");
        assert_eq!(config.bind_addr, "0.0.0.0");
        assert_eq!(config.bind_port, 50071);
        assert_eq!(config.grpc_bind_port, 50072);
        assert_eq!(config.concurrent_tasks, 6);
        assert_eq!(config.heartbeat_interval_secs, 9);
        assert_eq!(config.memory_pool_bytes, Some(123_456));
        assert_eq!(config.spill_dir, std::path::PathBuf::from("/tmp/teodb-test-spill"));
        assert_eq!(config.drain_timeout, std::time::Duration::from_secs(13));
        assert_eq!(config.object_store.url(), "s3://teodb");
    }

    #[test]
    fn executor_config_rejects_malformed_bind() {
        let mut cfg = TeoDBConfig::default();
        cfg.cluster.executor_bind = "0.0.0.0".into();

        let err = executor_config_from(&cfg, test_object_store()).unwrap_err();

        assert!(err.to_string().contains("host:port"));
    }

    #[test]
    fn executor_config_uses_configured_advertise_host() {
        let mut cfg = TeoDBConfig::default();
        cfg.cluster.executor_advertise_host = Some("teodb-data-node-2".into());

        let config = executor_config_from(&cfg, test_object_store()).unwrap();

        assert_eq!(config.external_host.as_deref(), Some("teodb-data-node-2"));
    }

    #[test]
    fn executor_config_falls_back_to_system_hostname() {
        let cfg = TeoDBConfig::default();

        let config = executor_config_from(&cfg, test_object_store()).unwrap();

        let host = config
            .external_host
            .expect("advertise host must always be set");
        assert!(!host.is_empty());
    }

    #[test]
    fn executor_config_rejects_blank_advertise_host() {
        let mut cfg = TeoDBConfig::default();
        cfg.cluster.executor_advertise_host = Some("   ".into());

        let err = executor_config_from(&cfg, test_object_store()).unwrap_err();

        assert!(
            err.to_string()
                .contains("executor_advertise_host")
        );
    }
}
