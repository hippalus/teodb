use std::time::Duration;

use backon::{ExponentialBuilder, Retryable};
use color_eyre::eyre::{Result, eyre};
use teodb_client::ClientError;
use teodb_client::flight::FlightClient;
use teodb_client::http::HttpClient;

use crate::suite_config::{DatasetManifest, LoadMode};
use crate::{datasets, suite_config, tpch_gen};

pub async fn load_prepared_dataset(
    http: &HttpClient,
    flight: Option<&mut FlightClient>,
    manifest: &DatasetManifest,
    prepared: &datasets::PreparedDataset,
) -> Result<()> {
    if let Some(tables) = &manifest.tables {
        match manifest.load_mode {
            LoadMode::ManagedIngest => {
                let _ = http
                    .query("CREATE SCHEMA IF NOT EXISTS tpch", None)
                    .await;

                let ddl_map: std::collections::HashMap<&str, String> =
                    tpch_gen::create_table_ddl().into_iter().collect();
                let batch_size = manifest.ingest_batch_size.unwrap_or(500);

                for entry in tables {
                    let create_sql = entry
                        .create_sql
                        .as_deref()
                        .or_else(|| {
                            ddl_map
                                .get(entry.name.as_str())
                                .map(|s| s.as_str())
                        })
                        .ok_or_else(|| eyre!("no CREATE TABLE DDL for table '{}'", entry.name))?;

                    let drop_sql = format!("DROP TABLE IF EXISTS tpch.{}", entry.name);
                    let _ = http.query(&drop_sql, None).await;

                    println!("  creating table={}", entry.name);
                    let _ = http.query(create_sql, None).await?;

                    let parquet_path = prepared.primary_path.join(&entry.file);
                    let json_batches = tpch_gen::parquet_to_json_batches(&entry.name, &parquet_path, batch_size)?;
                    let mut total_rows = 0usize;
                    for batch in &json_batches {
                        if let serde_json::Value::Array(arr) = batch {
                            total_rows += arr.len();
                            ingest_with_retry(http, "tpch", &entry.name, arr.clone()).await?;
                        }
                    }
                    println!("  ingested table={} rows={total_rows}", entry.name);
                }
            }
            LoadMode::FlightInsert => {
                let flight = flight.ok_or_else(|| eyre!("Flight SQL client required for flight_insert load mode"))?;

                let _ = http
                    .query("CREATE SCHEMA IF NOT EXISTS tpch", None)
                    .await;

                let ddl_map: std::collections::HashMap<&str, String> =
                    tpch_gen::create_table_ddl().into_iter().collect();
                let batch_size = manifest.ingest_batch_size.unwrap_or(100);

                for entry in tables {
                    let create_sql = entry
                        .create_sql
                        .as_deref()
                        .or_else(|| {
                            ddl_map
                                .get(entry.name.as_str())
                                .map(|s| s.as_str())
                        })
                        .ok_or_else(|| eyre!("no CREATE TABLE DDL for table '{}'", entry.name))?;

                    let drop_sql = format!("DROP TABLE IF EXISTS tpch.{}", entry.name);
                    let _ = flight.execute_update(&drop_sql).await;

                    println!("  creating table={} (via Flight SQL)", entry.name);
                    flight.execute_update(create_sql).await?;

                    let parquet_path = prepared.primary_path.join(&entry.file);
                    let insert_stmts = tpch_gen::parquet_to_insert_statements(&entry.name, &parquet_path, batch_size)?;
                    let mut total_rows = 0usize;
                    for stmt in &insert_stmts {
                        let affected = flight.execute_update(stmt).await?;
                        total_rows += affected as usize;
                    }
                    println!("  inserted table={} rows={total_rows} (via Flight SQL)", entry.name);
                }
            }
            LoadMode::ExternalParquet | LoadMode::ManagedJson => {
                return Err(eyre!(
                    "load mode '{:?}' is not supported for multi-table datasets with this server",
                    manifest.load_mode
                ));
            }
        }

        println!("  flushing all tables...");
        for entry in tables {
            flush_with_retry(http, "tpch", &entry.name).await?;
        }

        return Ok(());
    }

    if matches!(
        manifest.source_kind,
        suite_config::DatasetSourceKind::SyntheticNestedJson
    ) {
        return load_nested_json_dataset(http, manifest, prepared).await;
    }

    match manifest.load_mode {
        LoadMode::ManagedJson | LoadMode::ManagedIngest => {
            let create_sql = manifest
                .create_sql
                .as_deref()
                .ok_or_else(|| eyre!("managed datasets require create_sql"))?;

            let table_name = manifest
                .single_table_name()
                .ok_or_else(|| eyre!("single-table datasets must set table_name"))?;

            let drop_sql = format!("DROP TABLE IF EXISTS default.{table_name}");
            let _ = http.query(&drop_sql, None).await;

            let _ = http.query(create_sql, None).await?;

            let rows = datasets::read_json_rows(&prepared.primary_path)?;
            for chunk in rows.chunks(manifest.ingest_batch_size.unwrap_or(500)) {
                let _ = http
                    .ingest("default", table_name, chunk.to_vec())
                    .await?;
            }
            flush_with_retry(http, "default", table_name).await?;
        }
        LoadMode::ExternalParquet => {
            let create_sql = manifest
                .create_sql
                .as_deref()
                .ok_or_else(|| eyre!("external_parquet datasets require create_sql"))?;
            let table_name = manifest
                .single_table_name()
                .ok_or_else(|| eyre!("single-table datasets must set table_name"))?;

            let drop_sql = format!("DROP TABLE IF EXISTS default.{table_name}");
            let _ = http.query(&drop_sql, None).await;
            let _ = http.query(create_sql, None).await?;

            let batch_size = manifest.ingest_batch_size.unwrap_or(2000);
            let json_batches = tpch_gen::parquet_to_json_batches(table_name, &prepared.primary_path, batch_size)?;
            let mut total = 0usize;
            for batch in &json_batches {
                if let serde_json::Value::Array(arr) = batch
                    && !arr.is_empty()
                {
                    total += arr.len();
                    ingest_with_retry(http, "default", table_name, arr.clone()).await?;
                }
            }
            flush_with_retry(http, "default", table_name).await?;
            println!("  ingested table={table_name} rows={total} (from external parquet via JSON ingest)");
        }
        LoadMode::FlightInsert => {
            let flight = flight.ok_or_else(|| eyre!("Flight SQL client required for flight_insert load mode"))?;
            let create_sql = manifest
                .create_sql
                .as_deref()
                .ok_or_else(|| eyre!("flight_insert datasets require create_sql"))?;
            flight.execute_update(create_sql).await?;
            return Err(eyre!(
                "flight_insert load mode is only supported for multi-table TPC-H datasets"
            ));
        }
    }
    Ok(())
}

async fn load_nested_json_dataset(
    http: &HttpClient,
    manifest: &DatasetManifest,
    prepared: &datasets::PreparedDataset,
) -> Result<()> {
    let create_sql = manifest
        .create_sql
        .as_deref()
        .ok_or_else(|| eyre!("nested JSON datasets require create_sql"))?;

    http.query("CREATE SCHEMA IF NOT EXISTS perf", None)
        .await
        .map_err(|e| eyre!("failed to create namespace 'perf': {e}"))?;

    let table_fqn = format!(
        "perf.{}",
        manifest
            .single_table_name()
            .ok_or_else(|| eyre!("single-table datasets must set table_name"))?
    );
    let _ = http
        .query(&format!("DROP TABLE IF EXISTS {table_fqn}"), None)
        .await;

    http.query(create_sql, None)
        .await
        .map_err(|e| eyre!("failed to create table: {e}"))?;

    let table_name = manifest
        .single_table_name()
        .ok_or_else(|| eyre!("single-table datasets must set table_name"))?;

    let batch_size = manifest.ingest_batch_size.unwrap_or(1000);
    let mut batch_files: Vec<_> = std::fs::read_dir(&prepared.primary_path)?
        .filter_map(|e| e.ok())
        .filter(|e| {
            e.path()
                .extension()
                .is_some_and(|ext| ext == "json")
        })
        .collect();
    batch_files.sort_by_key(|e| e.file_name());

    let start = std::time::Instant::now();
    let mut total_rows = 0usize;

    for (idx, entry) in batch_files.iter().enumerate() {
        let raw = std::fs::read_to_string(entry.path())?;
        let rows: Vec<serde_json::Value> = serde_json::from_str(&raw)?;
        let file_rows = rows.len();

        for chunk in rows.chunks(batch_size) {
            ingest_with_retry(http, "perf", table_name, chunk.to_vec()).await?;
        }

        total_rows += file_rows;
        let elapsed = start.elapsed().as_secs_f64();
        let rate = total_rows as f64 / elapsed;
        println!(
            "  [{}/{}] ingested {} rows ({total_rows} total, {rate:.0} rows/s)",
            idx + 1,
            batch_files.len(),
            file_rows
        );
    }

    flush_with_retry(http, "perf", table_name).await?;

    let elapsed = start.elapsed();
    println!(
        "  loaded {total_rows} rows in {:.1}s ({:.0} rows/s)",
        elapsed.as_secs_f64(),
        total_rows as f64 / elapsed.as_secs_f64()
    );

    Ok(())
}

async fn ingest_with_retry(
    http: &HttpClient,
    namespace: &str,
    table: &str,
    rows: Vec<serde_json::Value>,
) -> Result<()> {
    (|| async {
        http.ingest(namespace, table, rows.clone())
            .await
            .map(|_| ())
    })
    .retry(perf_retry_backoff(Duration::from_millis(100)))
    .when(is_ingest_retryable)
    .await?;

    Ok(())
}

async fn flush_with_retry(http: &HttpClient, namespace: &str, table: &str) -> Result<()> {
    let mut retry = 0usize;

    (|| async { http.flush(namespace, table).await.map(|_| ()) })
        .retry(perf_retry_backoff(Duration::from_millis(200)))
        .when(is_flush_retryable)
        .notify(|_error, delay| {
            retry += 1;
            eprintln!(
                "  flush attempt {}/{} failed (transient), retrying in {:?}",
                retry, PERF_HTTP_MAX_ATTEMPTS, delay
            );
        })
        .await?;

    Ok(())
}

fn perf_retry_backoff(initial_delay: Duration) -> ExponentialBuilder {
    ExponentialBuilder::default()
        .with_min_delay(initial_delay)
        .with_max_times(PERF_HTTP_MAX_RETRIES)
}

fn is_ingest_retryable(error: &ClientError) -> bool {
    matches!(error, ClientError::Server { status: 429 | 500, .. })
}

fn is_flush_retryable(error: &ClientError) -> bool {
    match error {
        ClientError::Server { status, body } => {
            *status == 500 || body.contains("Unexpected") || body.contains("commit state")
        }
        _ => false,
    }
}

const PERF_HTTP_MAX_ATTEMPTS: usize = 5;
const PERF_HTTP_MAX_RETRIES: usize = PERF_HTTP_MAX_ATTEMPTS - 1;
