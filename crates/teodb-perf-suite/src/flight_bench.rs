//! Flight SQL operation benchmarks with warmup/measured runs and latency stats.

use std::time::Instant;

use arrow_flight::sql::SqlInfo;
use color_eyre::eyre::Result;
use comfy_table::presets::UTF8_FULL;
use comfy_table::{Cell, CellAlignment, Table};
use serde::{Deserialize, Serialize};
use teodb_client::flight::FlightClient;

use crate::report::QueryResult;

// Benchmark configuration

/// A single Flight SQL operation to benchmark.
#[derive(Debug, Clone, Deserialize)]
pub struct FlightOp {
    /// Human-readable label shown in reports.
    pub name: String,
    /// The operation kind.
    #[serde(flatten)]
    pub kind: FlightOpKind,
}

/// Discriminated union of Flight SQL operation types.
#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum FlightOpKind {
    Handshake {
        #[serde(default = "default_user")]
        user: String,
        #[serde(default = "default_password")]
        password: String,
    },
    Query {
        sql: String,
    },
    ExecuteUpdate {
        sql: String,
    },
    GetCatalogs,
    GetSchemas {
        catalog: Option<String>,
        schema_filter: Option<String>,
    },
    GetTables {
        catalog: Option<String>,
        schema_filter: Option<String>,
        table_filter: Option<String>,
        #[serde(default)]
        include_schema: bool,
    },
    GetSqlInfo {
        #[serde(default)]
        info_ids: Vec<u32>,
    },
}

fn default_user() -> String {
    "admin".to_string()
}
fn default_password() -> String {
    "password".to_string()
}

/// Manifest for a Flight SQL benchmark suite.
#[derive(Debug, Clone, Deserialize)]
pub struct FlightBenchManifest {
    pub name: String,
    pub flight_endpoint: Option<String>,
    #[serde(default = "default_warmup")]
    pub warmup_runs: u32,
    #[serde(default = "default_measured")]
    pub measured_runs: u32,
    pub ops: Vec<FlightOp>,
}

fn default_warmup() -> u32 {
    2
}
fn default_measured() -> u32 {
    5
}

// Benchmark results

/// Full report for a Flight SQL benchmark run.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FlightBenchReport {
    pub suite: String,
    pub timestamp: String,
    pub warmup_runs: u32,
    pub measured_runs: u32,
    pub results: Vec<QueryResult>,
}

impl FlightBenchReport {
    /// Print a comfy-table summary to stdout.
    pub fn print_table(&self) {
        let mut table = Table::new();
        table.load_preset(UTF8_FULL);
        table.set_header(vec![
            Cell::new("Operation"),
            Cell::new("Min (ms)").set_alignment(CellAlignment::Right),
            Cell::new("Avg (ms)").set_alignment(CellAlignment::Right),
            Cell::new("P50 (ms)").set_alignment(CellAlignment::Right),
            Cell::new("P99 (ms)").set_alignment(CellAlignment::Right),
            Cell::new("Max (ms)").set_alignment(CellAlignment::Right),
            Cell::new("Status"),
        ]);

        for q in &self.results {
            let status = if q.error.is_some() { "ERROR" } else { "OK" };
            table.add_row(vec![
                Cell::new(&q.name),
                Cell::new(format!("{:.1}", q.min_ms)).set_alignment(CellAlignment::Right),
                Cell::new(format!("{:.1}", q.avg_ms)).set_alignment(CellAlignment::Right),
                Cell::new(format!("{:.1}", q.p50_ms)).set_alignment(CellAlignment::Right),
                Cell::new(format!("{:.1}", q.p99_ms)).set_alignment(CellAlignment::Right),
                Cell::new(format!("{:.1}", q.max_ms)).set_alignment(CellAlignment::Right),
                Cell::new(status),
            ]);
        }

        println!();
        println!(
            "Suite: {} | Transport: flight-sql | Runs: {} warmup + {} measured | {}",
            self.suite, self.warmup_runs, self.measured_runs, self.timestamp
        );
        println!("{table}");
    }
}

// Benchmark executor

/// Run a complete Flight SQL benchmark suite with warmup/measured runs.
pub async fn run_flight_bench(flight_endpoint: &str, manifest: &FlightBenchManifest) -> Result<FlightBenchReport> {
    println!(
        "Running Flight SQL bench '{}': {} ops, {} warmup + {} measured runs",
        manifest.name,
        manifest.ops.len(),
        manifest.warmup_runs,
        manifest.measured_runs
    );

    let mut results = Vec::new();

    for op in &manifest.ops {
        println!("  {}", op.name);

        // Warmup
        for _ in 0..manifest.warmup_runs {
            if let Err(e) = run_single_op(flight_endpoint, &op.kind).await {
                eprintln!("    warmup error: {e}");
            }
        }

        // Measured runs
        let mut timings = Vec::new();
        let mut error = None;

        for _ in 0..manifest.measured_runs {
            match run_single_op(flight_endpoint, &op.kind).await {
                Ok(ms) => timings.push(ms),
                Err(e) => {
                    error = Some(format!("{e}"));
                    break;
                }
            }
        }

        if timings.is_empty() {
            results.push(QueryResult {
                name: op.name.clone(),
                min_ms: 0.0,
                avg_ms: 0.0,
                max_ms: 0.0,
                p50_ms: 0.0,
                p99_ms: 0.0,
                runs: vec![],
                error,
            });
            continue;
        }

        timings.sort_by(|a, b| {
            a.partial_cmp(b)
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        let min_ms = timings[0];
        let max_ms = timings[timings.len() - 1];
        let avg_ms = timings.iter().sum::<f64>() / timings.len() as f64;
        let p50_ms = percentile(&timings, 50.0);
        let p99_ms = percentile(&timings, 99.0);

        println!("    min={min_ms:.1}ms  avg={avg_ms:.1}ms  max={max_ms:.1}ms  p50={p50_ms:.1}ms  p99={p99_ms:.1}ms");

        results.push(QueryResult {
            name: op.name.clone(),
            min_ms,
            avg_ms,
            max_ms,
            p50_ms,
            p99_ms,
            runs: timings,
            error,
        });
    }

    Ok(FlightBenchReport {
        suite: manifest.name.clone(),
        timestamp: chrono::Utc::now().to_rfc3339(),
        warmup_runs: manifest.warmup_runs,
        measured_runs: manifest.measured_runs,
        results,
    })
}

async fn run_single_op(endpoint: &str, kind: &FlightOpKind) -> Result<f64> {
    let mut client = FlightClient::connect(endpoint).await?;
    let start = Instant::now();

    match kind {
        FlightOpKind::Handshake { user, password } => {
            client.handshake(user, password).await?;
        }
        FlightOpKind::Query { sql } => {
            let _ = client.handshake("admin", "password").await;
            let _batches = client.query(sql).await?;
        }
        FlightOpKind::ExecuteUpdate { sql } => {
            let _ = client.handshake("admin", "password").await;
            let _affected = client.execute_update(sql).await?;
        }
        FlightOpKind::GetCatalogs => {
            let _ = client.handshake("admin", "password").await;
            let _batches = client.get_catalogs().await?;
        }
        FlightOpKind::GetSchemas { catalog, schema_filter } => {
            let _ = client.handshake("admin", "password").await;
            let _batches = client
                .get_schemas(catalog.as_deref(), schema_filter.as_deref())
                .await?;
        }
        FlightOpKind::GetTables {
            catalog,
            schema_filter,
            table_filter,
            include_schema,
        } => {
            let _ = client.handshake("admin", "password").await;
            let _batches = client
                .get_tables(
                    catalog.as_deref(),
                    schema_filter.as_deref(),
                    table_filter.as_deref(),
                    *include_schema,
                )
                .await?;
        }
        FlightOpKind::GetSqlInfo { info_ids } => {
            let _ = client.handshake("admin", "password").await;
            let sql_infos: Vec<SqlInfo> = info_ids
                .iter()
                .filter_map(|id| SqlInfo::try_from(*id as i32).ok())
                .collect();
            let _batches = client.get_sql_info(sql_infos).await?;
        }
    }

    Ok(start.elapsed().as_secs_f64() * 1000.0)
}

fn percentile(sorted: &[f64], pct: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    if sorted.len() == 1 {
        return sorted[0];
    }
    let rank = (pct / 100.0) * (sorted.len() - 1) as f64;
    let lower = rank.floor() as usize;
    let upper = rank.ceil() as usize;
    let frac = rank - lower as f64;
    sorted[lower] * (1.0 - frac) + sorted[upper] * frac
}

/// Load a flight benchmark manifest from a TOML file.
pub fn load_manifest(path: &std::path::Path) -> Result<FlightBenchManifest> {
    let raw = std::fs::read_to_string(path)?;
    Ok(toml::from_str(&raw)?)
}
