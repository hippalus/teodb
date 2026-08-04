use std::path::PathBuf;

use clap::{Args, Parser, Subcommand};

#[derive(Debug, Parser)]
#[command(name = "teodb-perf-suite", about = "External perf and functional suite for TeoDB")]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Debug, Subcommand)]
pub enum Command {
    PrepareData(PrepareDataArgs),
    Load(LoadArgs),
    RunSuite(RunSuiteArgs),
    RunFlightBench(RunFlightBenchArgs),
    Report(ReportArgs),
}

#[derive(Debug, Args)]
pub struct CommonArgs {
    #[arg(long, default_value = "http://127.0.0.1:8080")]
    pub http: String,
    #[arg(long, default_value = "http://127.0.0.1:8815")]
    pub flight: String,
    #[arg(long, default_value = "admin")]
    pub user: String,
    #[arg(long, default_value = "password")]
    pub password: String,
    #[arg(long, default_value = "artifacts/perf-suite")]
    pub work_dir: PathBuf,
}

#[derive(Debug, Args)]
pub struct PrepareDataArgs {
    #[arg(long)]
    pub dataset: PathBuf,
    #[command(flatten)]
    pub common: CommonArgs,
}

#[derive(Debug, Args)]
pub struct LoadArgs {
    #[arg(long)]
    pub dataset: PathBuf,
    #[command(flatten)]
    pub common: CommonArgs,
}

#[derive(Debug, Args)]
pub struct RunSuiteArgs {
    #[arg(long)]
    pub bench: PathBuf,
    #[arg(long)]
    pub output: Option<String>,
    /// Override the scenario's transport: `rest`, `flight`, or `both`.
    #[arg(long)]
    pub transport: Option<String>,
    #[command(flatten)]
    pub common: CommonArgs,
}

#[derive(Debug, Args)]
pub struct RunFlightBenchArgs {
    #[arg(long)]
    pub bench: PathBuf,
    #[arg(long)]
    pub output: Option<String>,
    #[command(flatten)]
    pub common: CommonArgs,
}

#[derive(Debug, Args)]
pub struct ReportArgs {
    #[arg(long)]
    pub input: PathBuf,
}
