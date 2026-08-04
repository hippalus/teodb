use std::fs;
use std::path::{Path, PathBuf};
use std::time::Instant;

use color_eyre::eyre::{Result, WrapErr, eyre};
use serde_json::json;
use teodb_client::flight::FlightClient;

use crate::report::{QueryResult, SuiteResults};
use crate::suite_config::ScenarioManifest;

// Transport classification

#[derive(Clone, Copy)]
enum Transport {
    Flight,
    Rest,
}

impl Transport {
    fn label(self) -> &'static str {
        match self {
            Transport::Flight => "flight-sql",
            Transport::Rest => "rest",
        }
    }
}

/// Resolve the configured transport string into the protocols to exercise.
/// `both`/`all` run REST first, then Flight; anything unrecognized is REST.
fn classify_transports(requested: &str) -> Vec<Transport> {
    match requested {
        "both" | "all" => vec![Transport::Rest, Transport::Flight],
        "flight" => vec![Transport::Flight],
        _ => vec![Transport::Rest],
    }
}

/// True when the scenario exercises Flight SQL on at least one transport.
pub fn needs_flight(requested: &str) -> bool {
    classify_transports(requested)
        .iter()
        .any(|t| matches!(t, Transport::Flight))
}

// Single-query execution returning elapsed milliseconds

async fn execute_rest_query(client: &reqwest::Client, base_url: &str, sql: &str) -> Result<f64> {
    let url = format!("{base_url}/api/v1/query");
    let start = Instant::now();
    let resp = client
        .post(&url)
        .json(&json!({ "sql": sql, "limit": 1000 }))
        .send()
        .await
        .wrap_err("Failed to send query")?;
    // A failed query still answers quickly, so an unchecked status records the
    // error as the fastest run in the suite instead of reporting it.
    let status = resp.status();
    let body = resp
        .bytes()
        .await
        .wrap_err("Failed to read response body")?;
    if !status.is_success() {
        let detail = String::from_utf8_lossy(&body);
        return Err(eyre!("REST query failed with HTTP {status}: {detail}"));
    }
    Ok(start.elapsed().as_secs_f64() * 1000.0)
}

async fn execute_flight_query(flight: &mut FlightClient, sql: &str) -> Result<f64> {
    let start = Instant::now();
    let _batches = flight
        .query(sql)
        .await
        .wrap_err("Flight SQL execute failed")?;
    Ok(start.elapsed().as_secs_f64() * 1000.0)
}

// Suite runner

pub async fn run_suite(
    http_url: &str,
    mut flight: Option<&mut FlightClient>,
    config: &ScenarioManifest,
    base_dir: &Path,
) -> Result<Vec<SuiteResults>> {
    let sql_dir = resolve_relative(base_dir, &config.sql_dir);

    let mut sql_files = Vec::new();
    for entry in fs::read_dir(&sql_dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.extension().and_then(|ext| ext.to_str()) == Some("sql") {
            sql_files.push(path);
        }
    }
    sql_files.sort();

    let transports = classify_transports(&config.transport);
    let mut suites = Vec::with_capacity(transports.len());
    for transport in transports {
        let suite = run_one_transport(http_url, flight.as_deref_mut(), config, &sql_files, transport).await?;
        suites.push(suite);
    }
    Ok(suites)
}

/// Execute every SQL file in the suite over a single transport.
async fn run_one_transport(
    http_url: &str,
    mut flight: Option<&mut FlightClient>,
    config: &ScenarioManifest,
    sql_files: &[PathBuf],
    transport: Transport,
) -> Result<SuiteResults> {
    let transport_label = transport.label();

    println!(
        "Running suite '{}' [{}]: {} queries, {} warmup + {} measured runs",
        config.name,
        transport_label,
        sql_files.len(),
        config.warmup_runs,
        config.measured_runs
    );

    let rest_client = reqwest::Client::new();
    let mut results = Vec::new();

    for file in sql_files {
        let name = file
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("unknown")
            .to_string();
        let sql = fs::read_to_string(file)?;
        let sql = sql.trim();

        println!("  {name}");

        // Warmup
        for _ in 0..config.warmup_runs {
            let err = match transport {
                Transport::Flight => {
                    let Some(ref mut f) = flight else {
                        break;
                    };
                    execute_flight_query(f, sql).await.err()
                }
                Transport::Rest => execute_rest_query(&rest_client, http_url, sql)
                    .await
                    .err(),
            };
            if let Some(e) = err {
                eprintln!("    warmup error: {e}");
            }
        }

        // Measured runs
        let mut timings = Vec::new();
        let mut error = None;

        for _ in 0..config.measured_runs {
            let result = match transport {
                Transport::Flight => {
                    let Some(ref mut f) = flight else {
                        error = Some("Flight SQL client not connected".to_string());
                        break;
                    };
                    execute_flight_query(f, sql).await
                }
                Transport::Rest => execute_rest_query(&rest_client, http_url, sql).await,
            };
            match result {
                Ok(ms) => timings.push(ms),
                Err(e) => {
                    error = Some(format!("{e}"));
                    break;
                }
            }
        }

        if timings.is_empty() {
            results.push(QueryResult {
                name,
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
            name,
            min_ms,
            avg_ms,
            max_ms,
            p50_ms,
            p99_ms,
            runs: timings,
            error,
        });
    }

    Ok(SuiteResults {
        suite_name: config.name.clone(),
        timestamp: chrono::Utc::now().to_rfc3339(),
        transport: transport_label.to_string(),
        warmup_runs: config.warmup_runs,
        measured_runs: config.measured_runs,
        queries: results,
    })
}

/// Compute a percentile from sorted values.
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

fn resolve_relative(base_dir: &Path, relative: &str) -> PathBuf {
    let path = PathBuf::from(relative);
    if path.is_absolute() { path } else { base_dir.join(path) }
}
