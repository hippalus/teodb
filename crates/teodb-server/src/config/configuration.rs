//! Layered TeoDB configuration loading and validation.

use super::cli::{CliArgs, ProcessRole};
use super::error::{ConfigError, ConfigValidationError};
#[cfg(test)]
use super::sections::LogFormat;
use super::sections::{
    CatalogConfig, ClusterConfig, IngestConfig, MaintenanceConfig, ObservabilityConfig, QueryConfig, RuntimeConfig,
    SecurityConfig, ServerConfig, ShutdownConfig, StorageConfig, WalConfig,
};

use std::path::{Path, PathBuf};
use std::time::Duration;

use config::{Config, Environment, File, FileFormat};
use serde::{Deserialize, Serialize};

/// Root configuration — maps 1:1 to the TOML file.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct TeoDBConfig {
    pub role: ProcessRole,
    pub data_dir: PathBuf,
    pub server: ServerConfig,
    pub catalog: CatalogConfig,
    pub storage: StorageConfig,
    pub wal: WalConfig,
    pub query: QueryConfig,
    pub ingest: IngestConfig,
    pub security: SecurityConfig,
    pub observability: ObservabilityConfig,
    pub runtime: RuntimeConfig,
    pub shutdown: ShutdownConfig,
    pub cluster: ClusterConfig,
    pub maintenance: MaintenanceConfig,
}

impl Default for TeoDBConfig {
    fn default() -> Self {
        Self {
            role: ProcessRole::default(),
            data_dir: PathBuf::from("./data"),
            server: ServerConfig::default(),
            catalog: CatalogConfig::default(),
            storage: StorageConfig::default(),
            wal: WalConfig::default(),
            query: QueryConfig::default(),
            ingest: IngestConfig::default(),
            security: SecurityConfig::default(),
            observability: ObservabilityConfig::default(),
            runtime: RuntimeConfig::default(),
            shutdown: ShutdownConfig::default(),
            cluster: ClusterConfig::default(),
            maintenance: MaintenanceConfig::default(),
        }
    }
}

impl TeoDBConfig {
    /// Load configuration with layered precedence via the `config` crate:
    ///   defaults → TOML file → env vars (TEODB__ prefix) → CLI flags.
    pub fn load(cli: &CliArgs) -> Result<Self, ConfigError> {
        let defaults_toml = toml::to_string(&TeoDBConfig::default()).map_err(ConfigError::SerializeDefaults)?;

        let mut builder = Config::builder()
            // Layer 1: compiled defaults
            .add_source(File::from_str(&defaults_toml, FileFormat::Toml));

        // Layer 2: TOML config file (optional)
        if let Some(ref path) = cli.config {
            if !path.exists() {
                return Err(ConfigError::FileNotFound { path: path.clone() });
            }
            builder = builder.add_source(File::from(path.as_ref()));
        }

        // Layer 3: environment variables — TEODB__SERVER__REST_BIND → server.rest_bind
        builder = builder.add_source(
            Environment::with_prefix("TEODB")
                .separator("__")
                .try_parsing(true),
        );

        let mut config: TeoDBConfig = builder
            .build()
            .map_err(ConfigError::Build)?
            .try_deserialize()
            .map_err(ConfigError::Deserialize)?;

        // Layer 4: CLI overrides (highest priority)
        config.apply_cli_overrides(cli);
        config.normalize();
        config.resolve_paths();
        config.validate()?;

        Ok(config)
    }

    /// Validate configuration bounds and invariants.
    fn validate(&self) -> Result<(), ConfigValidationError> {
        let mut errors: Vec<String> = Vec::new();
        validate_wal(&self.wal, &mut errors);
        validate_ingest(&self.ingest, &mut errors);
        validate_query_and_server(&self.query, &self.server, &mut errors);
        validate_cluster(self.role, &self.cluster, &self.shutdown, &mut errors);
        validate_maintenance(&self.maintenance, &mut errors);

        if errors.is_empty() {
            Ok(())
        } else {
            Err(ConfigValidationError::new(errors))
        }
    }

    /// Treat a blank admin token as unset. The `config` env layer parses an
    /// empty env var (`TEODB__SECURITY__ADMIN_TOKEN=`, e.g. compose's
    /// `${TEODB_ADMIN_TOKEN:-}` left unconfigured) into `Some("")` rather than
    /// `None`. Left as-is that enables admin auth with an unsatisfiable empty
    /// token — every admin/`/metrics` call 401s — instead of the intended
    /// open-with-warning default.
    fn normalize(&mut self) {
        if self
            .security
            .admin_token
            .as_deref()
            .is_some_and(|t| t.trim().is_empty())
        {
            self.security.admin_token = None;
        }
    }

    fn apply_cli_overrides(&mut self, cli: &CliArgs) {
        if let Some(role) = cli.role {
            self.role = role;
        }
        if let Some(mode) = cli.security_mode {
            self.security.mode = mode;
        }
        if let Some(ref v) = cli.rest_bind {
            self.server.rest_bind = v.clone();
        }
        if let Some(ref v) = cli.flight_bind {
            self.server.flight_bind = v.clone();
        }
        if let Some(ref v) = cli.executor_advertise_host {
            self.cluster.executor_advertise_host = Some(v.clone());
        }
        if let Some(v) = cli.log_level {
            self.observability.log_level = v;
        }
        if let Some(v) = cli.log_format {
            self.observability.log_format = v;
        }
    }

    fn resolve_paths(&mut self) {
        let base = &self.data_dir;
        if self.storage.cache_dir == Path::new("./data/cache") {
            self.storage.cache_dir = base.join("cache");
        }
        if self.storage.spill_dir == Path::new("./data/spill") {
            self.storage.spill_dir = base.join("spill");
        }
    }

    pub fn wal_dir(&self) -> PathBuf {
        self.data_dir.join("wal")
    }

    pub fn effective_worker_threads(&self) -> Option<usize> {
        if self.runtime.worker_threads == 0 {
            None
        } else {
            Some(self.runtime.worker_threads)
        }
    }

    pub fn wal_identity_config(&self) -> teodb_core::error::TeoDBResult<teodb_storage::wal::WalIdentityConfig> {
        Ok(teodb_storage::wal::WalIdentityConfig {
            cluster_id: self
                .cluster
                .cluster_id
                .map(teodb_core::write_protocol::ClusterId::from_uuid),
            node_id: self
                .cluster
                .node_id
                .as_deref()
                .map(teodb_core::write_protocol::NodeId::new)
                .transpose()?,
            writer_slot: self
                .cluster
                .writer_slot
                .as_deref()
                .map(teodb_core::write_protocol::WriterSlot::new)
                .transpose()?,
        })
    }

    pub fn to_ingest_config(&self) -> teodb_ingest::config::IngestConfig {
        teodb_ingest::config::IngestConfig {
            buffer_max_bytes: self.ingest.buffer_max_bytes,
            buffer_soft_watermark_bytes: self.ingest.buffer_max_bytes * 3 / 4,
            flush_interval: Duration::from_secs(self.ingest.flush_interval_secs),
            default_warehouse_uri: self
                .catalog
                .warehouse
                .clone()
                .unwrap_or_else(|| self.ingest.default_warehouse_uri.clone()),
            idempotency_ttl: Duration::from_secs(self.ingest.idempotency_ttl_secs),
            idempotency_max_keys_per_table: self.ingest.idempotency_max_keys_per_table,
            commit_status_check: self.ingest.commit_status_check.clone(),
        }
    }

    pub fn to_api_config(&self) -> teodb_api::ApiConfig {
        teodb_api::ApiConfig {
            max_body_bytes: self.ingest.max_body_bytes,
            max_result_bytes: self.server.max_result_bytes,
            cors_allowed_origins: self.server.cors_allowed_origins.clone(),
            read_requests_per_window: self.server.read_requests_per_window,
            write_requests_per_window: self.server.write_requests_per_window,
            public_requests_per_window: self.server.public_requests_per_window,
            rate_limit_window: Duration::from_secs(self.server.rate_limit_window_secs),
            max_rate_limit_keys: self.server.max_rate_limit_keys,
            max_concurrent_operations_per_principal: self
                .server
                .max_concurrent_operations_per_principal,
            trusted_proxy_cidrs: self
                .server
                .trusted_proxy_cidrs
                .iter()
                .map(|cidr| {
                    cidr.parse()
                        .expect("trusted proxy CIDRs were startup-validated")
                })
                .collect(),
        }
    }
}

fn validate_wal(config: &WalConfig, errors: &mut Vec<String>) {
    if config.hard_cap_bytes == 0 {
        errors.push("wal.hard_cap_bytes must be > 0 (it is the durability backpressure ceiling)".into());
    }
    if config.soft_watermark_bytes > config.hard_cap_bytes {
        errors.push(format!(
            "wal.soft_watermark_bytes ({}) must be <= wal.hard_cap_bytes ({})",
            config.soft_watermark_bytes, config.hard_cap_bytes
        ));
    }
    if config.max_prepared_files == 0 {
        errors.push("wal.max_prepared_files must be > 0".into());
    }
    if config.max_prepared_bytes == 0 {
        errors.push("wal.max_prepared_bytes must be > 0".into());
    }
}

fn validate_ingest(config: &IngestConfig, errors: &mut Vec<String>) {
    if config.flush_interval_secs == 0 {
        errors.push("ingest.flush_interval_secs must be > 0".into());
    }
    if config.buffer_max_bytes == 0 {
        errors.push("ingest.buffer_max_bytes must be > 0".into());
    }
    if config.max_body_bytes == 0 {
        errors.push("ingest.max_body_bytes must be > 0".into());
    }
    let status_check = &config.commit_status_check;
    if status_check.min_wait > status_check.max_wait {
        errors.push("ingest.commit_status_check.min_wait_ms must be <= ingest.commit_status_check.max_wait_ms".into());
    }
    if status_check.total_timeout.is_zero() {
        errors.push("ingest.commit_status_check.total_timeout_ms must be > 0".into());
    }
    if status_check.blocked_recheck_interval.is_zero() {
        errors.push("ingest.commit_status_check.blocked_recheck_interval_ms must be > 0".into());
    }
    if status_check.blocked_recheck_jitter_percent > 100 {
        errors.push("ingest.commit_status_check.blocked_recheck_jitter_percent must be <= 100".into());
    }
    if status_check.max_concurrent_blocked_rechecks == 0 {
        errors.push("ingest.commit_status_check.max_concurrent_blocked_rechecks must be > 0".into());
    }
}

fn validate_query_and_server(query: &QueryConfig, server: &ServerConfig, errors: &mut Vec<String>) {
    if query.batch_size == 0 {
        errors.push("query.batch_size must be > 0".into());
    }
    if query.query_timeout_secs == 0 {
        errors.push("query.query_timeout_secs must be > 0 (0 would time out every query immediately)".into());
    }
    if query.memory_pool_bytes == 0 {
        errors.push("query.memory_pool_bytes must be > 0 (0 disables the query memory ceiling)".into());
    }
    if query.metadata_refresh_secs == 0 {
        errors.push("query.metadata_refresh_secs must be > 0 (0 would reload catalog metadata on every query)".into());
    }
    if query.query_status_max_entries == 0 {
        errors.push("query.query_status_max_entries must be > 0".into());
    }
    if query.query_status_ttl_secs == 0 {
        errors.push("query.query_status_ttl_secs must be > 0".into());
    }
    if server.idle_timeout_secs == 0 {
        errors.push("server.idle_timeout_secs must be > 0".into());
    }
    for (name, value) in [
        ("max_http_connections", server.max_http_connections),
        ("max_http_in_flight_requests", server.max_http_in_flight_requests),
        ("max_flight_connections", server.max_flight_connections),
        ("max_flight_in_flight_requests", server.max_flight_in_flight_requests),
        (
            "max_flight_streams_per_connection",
            server.max_flight_streams_per_connection as usize,
        ),
        (
            "flight_max_decoding_message_bytes",
            server.flight_max_decoding_message_bytes,
        ),
        (
            "flight_max_encoding_message_bytes",
            server.flight_max_encoding_message_bytes,
        ),
        (
            "max_concurrent_operations_per_principal",
            server.max_concurrent_operations_per_principal,
        ),
    ] {
        if value == 0 {
            errors.push(format!("server.{name} must be > 0"));
        }
    }
    if server.max_result_bytes == 0 {
        errors.push("server.max_result_bytes must be > 0".into());
    }
    if server.read_requests_per_window == 0
        || server.write_requests_per_window == 0
        || server.public_requests_per_window == 0
    {
        errors.push("server request rate budgets must be > 0".into());
    }
    if server.rate_limit_window_secs == 0 {
        errors.push("server.rate_limit_window_secs must be > 0".into());
    }
    if server.max_rate_limit_keys == 0 {
        errors.push("server.max_rate_limit_keys must be > 0".into());
    }
    for cidr in &server.trusted_proxy_cidrs {
        if let Err(error) = cidr.parse::<teodb_api::security::TrustedProxyCidr>() {
            errors.push(format!("server.trusted_proxy_cidrs: {error}"));
        }
    }
}

fn validate_cluster(role: ProcessRole, cluster: &ClusterConfig, shutdown: &ShutdownConfig, errors: &mut Vec<String>) {
    if matches!(role, ProcessRole::DataNode) {
        if cluster.cluster_id.is_none() {
            errors.push("cluster.cluster_id is required for role=data-node".into());
        } else if cluster
            .cluster_id
            .is_some_and(|cluster_id| cluster_id.is_nil())
        {
            errors.push("cluster.cluster_id must not be the nil UUID".into());
        }
        if cluster
            .node_id
            .as_deref()
            .is_none_or(str::is_empty)
        {
            errors.push("cluster.node_id is required for role=data-node".into());
        }
        if cluster
            .writer_slot
            .as_deref()
            .is_none_or(str::is_empty)
        {
            errors.push("cluster.writer_slot is required for role=data-node".into());
        }
    }
    if cluster.max_writer_checkpoints_per_table == 0 {
        errors.push("cluster.max_writer_checkpoints_per_table must be > 0".into());
    }
    if let Some(node_id) = &cluster.node_id
        && let Err(error) = teodb_core::write_protocol::NodeId::new(node_id)
    {
        errors.push(error.to_string());
    }
    if let Some(writer_slot) = &cluster.writer_slot
        && let Err(error) = teodb_core::write_protocol::WriterSlot::new(writer_slot)
    {
        errors.push(error.to_string());
    }
    if cluster.heartbeat_interval_secs == 0 {
        errors.push("cluster.heartbeat_interval_secs must be > 0".into());
    }
    if cluster.heartbeat_miss_threshold == 0 {
        errors.push("cluster.heartbeat_miss_threshold must be > 0".into());
    }
    if cluster.executor_grpc_bind_port == 0 {
        errors.push("cluster.executor_grpc_bind_port must be > 0".into());
    }
    if shutdown.drain_timeout_secs == 0 {
        errors.push("shutdown.drain_timeout_secs must be > 0".into());
    }
}

fn validate_maintenance(config: &MaintenanceConfig, errors: &mut Vec<String>) {
    if config.orphan_sweep_interval_secs == 0 {
        errors.push("maintenance.orphan_sweep_interval_secs must be > 0".into());
    }
    if config.compaction_enabled {
        if config.min_files_per_compaction > config.max_files_per_compaction {
            errors.push(format!(
                "maintenance.min_files_per_compaction ({}) must be <= max_files_per_compaction ({})",
                config.min_files_per_compaction, config.max_files_per_compaction
            ));
        }
        if config.compaction_interval_secs == 0 {
            errors.push("maintenance.compaction_interval_secs must be > 0 when compaction is enabled".into());
        }
        if config.max_bytes_per_compaction == 0 {
            errors.push("maintenance.max_bytes_per_compaction must be > 0 when compaction is enabled".into());
        }
        if config.compaction_memory_bytes == 0 {
            errors.push(
                "maintenance.compaction_memory_bytes must be > 0 when compaction is enabled (it bounds the compaction sort)"
                    .into(),
            );
        }
        if config.lock_ttl_secs < config.compaction_interval_secs * 2 {
            errors.push(format!(
                "maintenance.lock_ttl_secs ({}) should be >= 2 * compaction_interval_secs ({})",
                config.lock_ttl_secs,
                config.compaction_interval_secs * 2
            ));
        }
        if let Err(error) = teodb_storage::parquet::CompressionCodec::from_str_config(&config.compression) {
            errors.push(format!("maintenance.compression: {error}"));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    #[test]
    fn default_config_roundtrips_toml() {
        let cfg = TeoDBConfig::default();
        let toml_str = toml::to_string_pretty(&cfg).unwrap();
        let parsed: TeoDBConfig = toml::from_str(&toml_str).unwrap();
        assert_eq!(parsed.role, ProcessRole::Standalone);
        assert_eq!(parsed.server.rest_bind, "0.0.0.0:8080");
        assert_eq!(parsed.catalog.catalog_type.to_string(), "rest");
        assert!(!parsed.cluster.local_query_fallback);
        assert!(!parsed.maintenance.compaction_enabled);
        assert!(toml_str.contains("[ingest.commit_status_check]"));
        assert!(toml_str.contains("min_wait_ms = 100"));
        assert!(!toml_str.contains("commit_status_check_min_wait_ms"));
    }

    #[test]
    fn cli_overrides_apply() {
        let mut cfg = TeoDBConfig::default();
        let cli = CliArgs::parse_from(["teodb", "--role", "data-node", "--rest-bind", "127.0.0.1:9090"]);
        cfg.apply_cli_overrides(&cli);
        assert_eq!(cfg.role, ProcessRole::DataNode);
        assert_eq!(cfg.server.rest_bind, "127.0.0.1:9090");
    }

    #[test]
    fn partial_toml_merges_defaults() {
        let toml_str = r#"
role = "data-node"

[server]
rest_bind = "0.0.0.0:9090"

[query]
memory_pool_bytes = 8589934592
"#;
        let cfg: TeoDBConfig = toml::from_str(toml_str).unwrap();
        assert_eq!(cfg.role, ProcessRole::DataNode);
        assert_eq!(cfg.server.rest_bind, "0.0.0.0:9090");
        assert_eq!(cfg.server.flight_bind, "0.0.0.0:8815");
        assert_eq!(cfg.query.memory_pool_bytes, 8_589_934_592);
        assert_eq!(cfg.query.batch_size, 8192);
    }

    #[test]
    fn lowercase_log_format_parses_from_toml() {
        let cfg: TeoDBConfig = toml::from_str(
            r#"
[observability]
log_format = "pretty"
"#,
        )
        .unwrap();

        assert_eq!(cfg.observability.log_format, LogFormat::Pretty);
    }

    #[test]
    fn unsupported_memory_catalog_is_rejected_during_deserialization() {
        let result = toml::from_str::<TeoDBConfig>(
            r#"
[catalog]
type = "memory"
"#,
        );

        assert!(result.is_err());
    }

    #[test]
    fn data_node_role_and_embedded_scheduler_parse_from_toml() {
        let toml_str = r#"
role = "data-node"

[cluster]
scheduler_enabled = true
scheduler_bind = "0.0.0.0:50050"
scheduler_addr = "teodb-data-node-1:50050"
executor_bind = "0.0.0.0:50051"
executor_grpc_bind_port = 50052
executor_task_slots = 8
"#;
        let cfg: TeoDBConfig = toml::from_str(toml_str).unwrap();
        assert_eq!(cfg.role, ProcessRole::DataNode);
        assert!(cfg.cluster.scheduler_enabled);
        assert_eq!(cfg.cluster.scheduler_addr, "teodb-data-node-1:50050");
        assert_eq!(cfg.cluster.executor_task_slots, 8);
    }

    #[test]
    fn control_plane_role_parses_from_toml() {
        let cfg: TeoDBConfig = toml::from_str("role = \"control-plane\"").unwrap();
        assert_eq!(cfg.role, ProcessRole::ControlPlane);
    }

    #[test]
    fn removed_maintenance_node_id_is_rejected() {
        let error = toml::from_str::<TeoDBConfig>(
            r#"
            [maintenance]
            node_id = "old-node"
            "#,
        )
        .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("unknown field `node_id`")
        );
    }

    #[test]
    fn cli_executor_advertise_host_override_applies() {
        let mut cfg = TeoDBConfig::default();
        assert_eq!(cfg.cluster.executor_advertise_host, None);
        let cli = CliArgs::parse_from(["teodb", "--executor-advertise-host", "teodb-data-node-2"]);
        cfg.apply_cli_overrides(&cli);
        assert_eq!(
            cfg.cluster.executor_advertise_host.as_deref(),
            Some("teodb-data-node-2")
        );
    }

    #[test]
    fn wal_dir_derived_from_data_dir() {
        let cfg = TeoDBConfig {
            data_dir: PathBuf::from("/opt/teodb"),
            ..Default::default()
        };
        assert_eq!(cfg.wal_dir(), PathBuf::from("/opt/teodb/wal"));
    }

    #[test]
    fn load_without_config_file_uses_defaults() {
        let cli = CliArgs::parse_from(["teodb"]);
        let cfg = TeoDBConfig::load(&cli).unwrap();
        assert_eq!(cfg.role, ProcessRole::Standalone);
        assert_eq!(cfg.server.rest_bind, "0.0.0.0:8080");
    }

    #[test]
    fn blank_admin_token_normalizes_to_unset() {
        // A blank env override (compose `${TEODB_ADMIN_TOKEN:-}`) must leave
        // admin endpoints open, not lock them with an unsatisfiable "" token.
        for blank in ["", "   "] {
            let mut cfg = TeoDBConfig::default();
            cfg.security.admin_token = Some(blank.to_string());
            cfg.normalize();
            assert_eq!(cfg.security.admin_token, None);
        }

        let mut cfg = TeoDBConfig::default();
        cfg.security.admin_token = Some("s3cret".to_string());
        cfg.normalize();
        assert_eq!(cfg.security.admin_token.as_deref(), Some("s3cret"));
    }

    #[test]
    fn cli_data_node_role_requires_stable_writer_identity() {
        let cli = CliArgs::parse_from(["teodb", "--role", "data-node", "--security-mode", "plaintext"]);
        let error = TeoDBConfig::load(&cli).unwrap_err();
        let message = error.to_string();
        assert!(message.contains("cluster.cluster_id"));
        assert!(message.contains("cluster.node_id"));
        assert!(message.contains("cluster.writer_slot"));
    }

    #[test]
    fn data_node_rejects_nil_cluster_uuid() {
        let mut cfg = TeoDBConfig {
            role: ProcessRole::DataNode,
            ..TeoDBConfig::default()
        };
        cfg.cluster.cluster_id = Some(uuid::Uuid::nil());
        cfg.cluster.node_id = Some("data-node-0".into());
        cfg.cluster.writer_slot = Some("data-node-0".into());
        let error = cfg.validate().unwrap_err();
        assert!(
            error
                .issues
                .iter()
                .any(|issue| issue.contains("must not be the nil UUID"))
        );
    }

    #[test]
    fn nested_commit_status_check_config_parses_and_reaches_ingest() {
        let cfg: TeoDBConfig = toml::from_str(
            r#"
            [ingest.commit_status_check]
            num_retries = 9
            min_wait_ms = 250
            max_wait_ms = 7500
            total_timeout_ms = 45000
            blocked_recheck_interval_ms = 90000
            blocked_recheck_jitter_percent = 20
            max_concurrent_blocked_rechecks = 8
            "#,
        )
        .unwrap();

        let ingest = cfg.to_ingest_config();
        assert_eq!(ingest.commit_status_check, cfg.ingest.commit_status_check);
        assert_eq!(ingest.commit_status_check.num_retries, 9);
        assert_eq!(ingest.commit_status_check.min_wait, Duration::from_millis(250));
        assert_eq!(ingest.commit_status_check.max_wait, Duration::from_millis(7_500));
        assert_eq!(ingest.commit_status_check.total_timeout, Duration::from_millis(45_000));
    }

    #[test]
    fn removed_flat_commit_status_check_fields_are_rejected() {
        let error = toml::from_str::<TeoDBConfig>(
            r#"
            [ingest]
            commit_status_check_num_retries = 9
            "#,
        )
        .unwrap_err();

        assert!(
            error
                .to_string()
                .contains("unknown field `commit_status_check_num_retries`")
        );
    }

    #[test]
    fn nested_commit_status_check_config_is_validated() {
        let mut cfg = TeoDBConfig::default();
        cfg.ingest.commit_status_check.min_wait = Duration::from_secs(2);
        cfg.ingest.commit_status_check.max_wait = Duration::from_secs(1);
        cfg.ingest.commit_status_check.total_timeout = Duration::ZERO;

        let error = cfg.validate().unwrap_err();
        assert!(
            error
                .issues
                .iter()
                .any(|issue| issue.contains("ingest.commit_status_check.min_wait_ms"))
        );
        assert!(
            error
                .issues
                .iter()
                .any(|issue| issue.contains("ingest.commit_status_check.total_timeout_ms"))
        );
    }

    #[test]
    fn validation_error_exposes_individual_issues() {
        let mut cfg = TeoDBConfig::default();
        cfg.query.batch_size = 0;
        cfg.ingest.flush_interval_secs = 0;

        let error = cfg.validate().unwrap_err();

        assert_eq!(error.issues.len(), 2);
        assert!(
            error
                .issues
                .iter()
                .any(|issue| issue.contains("query.batch_size"))
        );
        assert!(
            error
                .issues
                .iter()
                .any(|issue| issue.contains("ingest.flush_interval_secs"))
        );
    }

    #[test]
    fn compaction_knobs_are_validated_only_when_compaction_enabled() {
        let mut cfg = TeoDBConfig::default();
        cfg.maintenance.compaction_interval_secs = 0;
        cfg.maintenance.max_bytes_per_compaction = 0;
        cfg.maintenance.compaction_memory_bytes = 0;
        cfg.maintenance.lock_ttl_secs = 0;
        cfg.maintenance.compression = "invalid".into();

        assert!(
            cfg.validate().is_ok(),
            "compaction-only knobs are inert while compaction is disabled"
        );

        cfg.maintenance.compaction_enabled = true;
        let error = cfg.validate().unwrap_err();
        assert!(
            error
                .issues
                .iter()
                .any(|issue| issue.contains("compaction_interval_secs"))
        );
        assert!(
            error
                .issues
                .iter()
                .any(|issue| issue.contains("max_bytes_per_compaction"))
        );
        assert!(
            error
                .issues
                .iter()
                .any(|issue| issue.contains("compaction_memory_bytes"))
        );
        assert!(
            error
                .issues
                .iter()
                .any(|issue| issue.contains("maintenance.compression"))
        );
    }
}
