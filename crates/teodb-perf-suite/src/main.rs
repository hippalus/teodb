mod cli;
mod commands;
mod datasets;
mod flight_bench;
mod nested_gen;
mod report;
mod runner;
mod suite_config;
mod tpch_gen;
mod workload;

use clap::Parser;
use color_eyre::eyre::Result;

use cli::{Cli, Command};

#[tokio::main]
async fn main() -> Result<()> {
    color_eyre::install()?;
    let cli = Cli::parse();

    match cli.command {
        Command::PrepareData(args) => commands::prepare_data(args).await?,
        Command::Load(args) => commands::load_dataset(args).await?,
        Command::RunSuite(args) => commands::run_suite(args).await?,
        Command::RunFlightBench(args) => commands::run_flight_bench(args).await?,
        Command::Report(args) => commands::print_report(args)?,
    }

    Ok(())
}
