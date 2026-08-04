//! Startup banner and configuration summary.

use crate::config::TeoDBConfig;

const VERSION: &str = env!("CARGO_PKG_VERSION");

/// Print the TeoDB startup banner to stdout.
pub fn print_banner() {
    let banner = format!(
        r#"
  ╔══════════════════════════════════════════════════════════════╗
  ║                                                              ║
  ║   ████████╗███████╗ ██████╗ ██████╗ ██████╗                  ║
  ║   ╚══██╔══╝██╔════╝██╔═══██╗██╔══██╗██╔══██╗                 ║
  ║      ██║   █████╗  ██║   ██║██║  ██║██████╔╝                 ║
  ║      ██║   ██╔══╝  ██║   ██║██║  ██║██╔══██╗                 ║
  ║      ██║   ███████╗╚██████╔╝██████╔╝██████╔╝                 ║
  ║      ╚═╝   ╚══════╝ ╚═════╝ ╚═════╝ ╚═════╝                  ║
  ║                                                              ║
  ║   Columnar OLAP Database on the FDAP Stack                   ║
  ║   Arrow Flight · DataFusion · Arrow · Parquet · Iceberg      ║
  ║   Version {ver:<48}   ║
  ║                                                              ║
  ╚══════════════════════════════════════════════════════════════╝
"#,
        ver = VERSION,
    );
    eprintln!("{banner}");
}

/// Format a byte count as a human-readable string.
fn fmt_bytes(bytes: u64) -> String {
    const KIB: u64 = 1024;
    const MIB: u64 = KIB * 1024;
    const GIB: u64 = MIB * 1024;

    if bytes >= GIB {
        format!("{:.1} GiB", bytes as f64 / GIB as f64)
    } else if bytes >= MIB {
        format!("{:.0} MiB", bytes as f64 / MIB as f64)
    } else if bytes >= KIB {
        format!("{:.0} KiB", bytes as f64 / KIB as f64)
    } else {
        format!("{bytes} B")
    }
}

/// Print a configuration summary showing endpoints and key settings.
pub fn print_config_summary(cfg: &TeoDBConfig) {
    let rest = &cfg.server.rest_bind;
    let flight = &cfg.server.flight_bind;

    let rest_url = endpoint_url(rest, cfg.security.tls_cert.is_some());
    let flight_url = flight_endpoint_url(flight);

    let tls_status = if cfg.security.tls_cert.is_some() {
        "✓ enabled"
    } else if cfg.security.mode.is_insecure() {
        "✗ disabled (plaintext mode)"
    } else {
        "✗ disabled"
    };

    let auth_status = if cfg.security.mode.allows_anonymous() {
        "anonymous (plaintext mode)"
    } else if cfg.security.allow_list_path.is_some() {
        "allow-list"
    } else {
        "none configured"
    };

    let cache_status = if cfg.storage.cache_max_bytes > 0 {
        format!("enabled ({})", fmt_bytes(cfg.storage.cache_max_bytes))
    } else {
        "disabled".into()
    };

    let cpus = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1);
    let workers = if cfg.runtime.worker_threads > 0 {
        cfg.runtime.worker_threads
    } else {
        cpus
    };

    let summary = format!(
        r#"
  ┌──────────────────────────────────────────────────────────────┐
  │  ENDPOINTS                                                   │
  ├──────────────────────────────────────────────────────────────┤
  │  REST API     {rest_url:<47}│
  │  Web UI       {ui_url:<47}│
  │  Flight gRPC  {flight_url:<47}│
  │  Metrics      {metrics_url:<47}│
  ├──────────────────────────────────────────────────────────────┤
  │  CONFIGURATION                                               │
  ├──────────────────────────────────────────────────────────────┤
  │  Role         {role:<47}│
  │  Data dir     {data_dir:<47}│
  │  Catalog      {catalog:<47}│
  │  TLS          {tls:<47}│
  │  Auth         {auth:<47}│
  ├──────────────────────────────────────────────────────────────┤
  │  RESOURCES                                                   │
  ├──────────────────────────────────────────────────────────────┤
  │  Workers      {workers:<47}│
  │  Query mem    {query_mem:<47}│
  │  Buffer       {buffer:<47}│
  │  SSD cache    {cache:<47}│
  │  WAL fsync    {fsync:<47}│
  │  Max conns    {conns:<47}│
  │  Body limit   {body:<47}│
  │  Query timeo  {timeout:<47}│
  │  Log level    {log_level:<47}│
  │  Log format   {log_format:<47}│
  └──────────────────────────────────────────────────────────────┘
"#,
        rest_url = format!("{rest_url}/api/v1"),
        ui_url = rest_url,
        flight_url = flight_url,
        metrics_url = format!("{rest_url}/metrics"),
        role = format!("{}", cfg.role),
        data_dir = cfg.data_dir.display(),
        catalog = format!("{} @ {}", cfg.catalog.catalog_type, cfg.catalog.uri),
        tls = tls_status,
        auth = auth_status,
        workers = format!("{workers} threads ({cpus} CPUs available)"),
        query_mem = fmt_bytes(cfg.query.memory_pool_bytes),
        buffer = fmt_bytes(cfg.ingest.buffer_max_bytes),
        cache = cache_status,
        fsync = if cfg.wal.fsync_on_append {
            "✓ enabled"
        } else {
            "✗ disabled"
        },
        conns = format!(
            "HTTP {} / Flight {}",
            cfg.server.max_http_connections, cfg.server.max_flight_connections
        ),
        body = fmt_bytes(cfg.ingest.max_body_bytes),
        timeout = format!("{}s", cfg.query.query_timeout_secs),
        log_level = format!("{}", cfg.observability.log_level),
        log_format = format!("{}", cfg.observability.log_format),
    );
    eprintln!("{summary}");
}

/// Build an HTTP(S) URL from a bind address.
fn endpoint_url(bind: &str, has_tls: bool) -> String {
    let scheme = if has_tls { "https" } else { "http" };
    let display_addr = bind.replace("0.0.0.0", "localhost");
    format!("{scheme}://{display_addr}")
}

/// Build a gRPC URL from a bind address.
fn flight_endpoint_url(bind: &str) -> String {
    let display_addr = bind.replace("0.0.0.0", "localhost");
    format!("grpc://{display_addr}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fmt_bytes_formats_correctly() {
        assert_eq!(fmt_bytes(512), "512 B");
        assert_eq!(fmt_bytes(1024), "1 KiB");
        assert_eq!(fmt_bytes(64 * 1024 * 1024), "64 MiB");
        assert_eq!(fmt_bytes(4 * 1024 * 1024 * 1024), "4.0 GiB");
        assert_eq!(fmt_bytes(10 * 1024 * 1024 * 1024), "10.0 GiB");
    }

    #[test]
    fn endpoint_url_replaces_wildcard() {
        assert_eq!(endpoint_url("0.0.0.0:8080", false), "http://localhost:8080");
        assert_eq!(endpoint_url("0.0.0.0:8080", true), "https://localhost:8080");
        assert_eq!(endpoint_url("127.0.0.1:9090", false), "http://127.0.0.1:9090");
    }

    #[test]
    fn flight_url_replaces_wildcard() {
        assert_eq!(flight_endpoint_url("0.0.0.0:8815"), "grpc://localhost:8815");
    }

    #[test]
    fn banner_prints_without_panic() {
        print_banner();
    }

    #[test]
    fn config_summary_prints_without_panic() {
        let cfg = TeoDBConfig::default();
        print_config_summary(&cfg);
    }
}
