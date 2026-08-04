use std::fs;
use std::path::Path;

use color_eyre::eyre::{Result, WrapErr};
use comfy_table::presets::UTF8_FULL;
use comfy_table::{Cell, CellAlignment, Table};
use serde::{Deserialize, Serialize};

/// A single query timing result with warmup/measured stats.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryResult {
    pub name: String,
    pub min_ms: f64,
    pub avg_ms: f64,
    pub max_ms: f64,
    pub p50_ms: f64,
    pub p99_ms: f64,
    pub runs: Vec<f64>,
    pub error: Option<String>,
}

/// Aggregated results from a benchmark suite run.
#[derive(Debug, Serialize, Deserialize)]
pub struct SuiteResults {
    pub suite_name: String,
    pub timestamp: String,
    pub transport: String,
    pub warmup_runs: u32,
    pub measured_runs: u32,
    pub queries: Vec<QueryResult>,
}

impl SuiteResults {
    pub fn from_path(path: &Path) -> Result<Self> {
        let raw = fs::read_to_string(path).wrap_err_with(|| format!("Failed to read results: {}", path.display()))?;
        Ok(serde_json::from_str(&raw)?)
    }
}

/// Save results to a JSON file.
pub fn save_results(results: &SuiteResults, path: &str) -> Result<()> {
    if let Some(parent) = Path::new(path).parent() {
        fs::create_dir_all(parent)
            .wrap_err_with(|| format!("Failed to create output directory: {}", parent.display()))?;
    }
    let json = serde_json::to_string_pretty(results).wrap_err("Failed to serialize results")?;
    fs::write(path, json).wrap_err_with(|| format!("Failed to write results to {path}"))?;
    println!("Results saved to {path}");
    Ok(())
}

/// Print a summary table to stdout using comfy-table.
pub fn print_table(results: &SuiteResults) {
    let mut table = Table::new();
    table.load_preset(UTF8_FULL);
    table.set_header(vec![
        Cell::new("Query"),
        Cell::new("Min (ms)").set_alignment(CellAlignment::Right),
        Cell::new("Avg (ms)").set_alignment(CellAlignment::Right),
        Cell::new("P50 (ms)").set_alignment(CellAlignment::Right),
        Cell::new("P99 (ms)").set_alignment(CellAlignment::Right),
        Cell::new("Max (ms)").set_alignment(CellAlignment::Right),
        Cell::new("Status"),
    ]);

    for q in &results.queries {
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
        "Suite: {} | Transport: {} | Runs: {} warmup + {} measured | {}",
        results.suite_name, results.transport, results.warmup_runs, results.measured_runs, results.timestamp
    );
    println!("{table}");
}

/// Print results as JSON to stdout.
#[allow(dead_code)]
pub fn print_json(results: &SuiteResults) -> Result<()> {
    let json = serde_json::to_string_pretty(results).wrap_err("Failed to serialize results")?;
    println!("{json}");
    Ok(())
}
