use std::path::Path;

use color_eyre::eyre::{Result, eyre};
use teodb_client::flight::FlightClient;
use teodb_client::http::HttpClient;

use crate::cli::{LoadArgs, PrepareDataArgs, ReportArgs, RunFlightBenchArgs, RunSuiteArgs};
use crate::{datasets, flight_bench, report, runner, suite_config, workload};

pub async fn prepare_data(args: PrepareDataArgs) -> Result<()> {
    let manifest = suite_config::load_dataset_manifest(&args.dataset)?;
    let prepared = datasets::prepare_dataset(&manifest, &args.common.work_dir).await?;
    println!("{}", serde_json::to_string_pretty(&prepared)?);
    Ok(())
}

pub async fn load_dataset(args: LoadArgs) -> Result<()> {
    let manifest = suite_config::load_dataset_manifest(&args.dataset)?;
    let prepared = datasets::prepare_dataset(&manifest, &args.common.work_dir).await?;
    let http = HttpClient::new(&args.common.http)?;

    let needs_flight = matches!(manifest.load_mode, suite_config::LoadMode::FlightInsert);
    let mut flight = if needs_flight {
        let mut client = FlightClient::connect(&args.common.flight).await?;
        let _ = client
            .handshake(&args.common.user, &args.common.password)
            .await;
        Some(client)
    } else {
        None
    };

    workload::load_prepared_dataset(&http, flight.as_mut(), &manifest, &prepared).await?;

    if let Some(tables) = &manifest.tables {
        println!("loaded dataset={} tables={}", manifest.name, tables.len());
    } else if let Some(name) = manifest.single_table_name() {
        println!("loaded dataset={} table={}", manifest.name, name);
    } else {
        println!("loaded dataset={}", manifest.name);
    }
    Ok(())
}

pub async fn run_suite(args: RunSuiteArgs) -> Result<()> {
    let mut scenario = suite_config::load_scenario_manifest(&args.bench)?;
    if let Some(transport) = &args.transport {
        scenario.transport = transport.clone();
    }

    let mut flight = if runner::needs_flight(&scenario.transport) {
        let mut client = FlightClient::connect(&args.common.flight).await?;
        let _ = client
            .handshake(&args.common.user, &args.common.password)
            .await;
        Some(client)
    } else {
        None
    };

    let suites = runner::run_suite(
        &args.common.http,
        flight.as_mut(),
        &scenario,
        args.bench.parent().unwrap_or(Path::new(".")),
    )
    .await?;

    let base_output = args
        .output
        .unwrap_or_else(|| "./perf-results/suite-results.json".to_string());
    let multi = suites.len() > 1;

    let mut failures = Vec::new();
    for suite in &suites {
        let output_path = if multi {
            suffix_path(&base_output, &suite.transport)
        } else {
            base_output.clone()
        };
        report::save_results(suite, &output_path)?;
        report::print_table(suite);

        let failed: Vec<&str> = suite
            .queries
            .iter()
            .filter(|q| q.error.is_some())
            .map(|q| q.name.as_str())
            .collect();
        if !failed.is_empty() {
            failures.push(format!(
                "{} [{}]: {}",
                suite.suite_name,
                suite.transport,
                failed.join(", ")
            ));
        }
    }

    if !failures.is_empty() {
        return Err(eyre!("suite queries failed:\n  {}", failures.join("\n  ")));
    }

    Ok(())
}

pub async fn run_flight_bench(args: RunFlightBenchArgs) -> Result<()> {
    let manifest = flight_bench::load_manifest(&args.bench)?;
    let endpoint = manifest
        .flight_endpoint
        .as_deref()
        .unwrap_or(&args.common.flight);

    let bench_report = flight_bench::run_flight_bench(endpoint, &manifest).await?;

    let output_path = args
        .output
        .unwrap_or_else(|| "./perf-results/flight-results.json".to_string());
    if let Some(parent) = Path::new(&output_path).parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(&output_path, serde_json::to_string_pretty(&bench_report)?)?;
    println!("Results saved to {output_path}");

    bench_report.print_table();

    let failed: Vec<&str> = bench_report
        .results
        .iter()
        .filter(|r| r.error.is_some())
        .map(|r| r.name.as_str())
        .collect();
    if !failed.is_empty() {
        return Err(eyre!(
            "{} of {} Flight SQL ops failed: {}",
            failed.len(),
            bench_report.results.len(),
            failed.join(", ")
        ));
    }

    Ok(())
}

pub fn print_report(args: ReportArgs) -> Result<()> {
    let results = report::SuiteResults::from_path(&args.input)?;
    report::print_table(&results);
    Ok(())
}

fn suffix_path(path: &str, tag: &str) -> String {
    match path.rsplit_once('.') {
        Some((stem, ext)) => format!("{stem}.{tag}.{ext}"),
        None => format!("{path}.{tag}"),
    }
}
