//! TeoDB server binary — thin entrypoint.
//!
//! All logic lives in submodules; main() only parses CLI args,
//! loads config, initializes tracing, builds the Tokio runtime, and delegates.

use clap::Parser;

mod builder;
mod config;
mod maintenance;
mod metrics;
mod security;
mod server;
mod startup;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

fn main() -> eyre::Result<()> {
    color_eyre::install()?;

    let cli = config::CliArgs::parse();
    let cfg = config::TeoDBConfig::load(&cli)?;

    startup::print_banner();

    startup::init_tracing(&cfg.observability)?;

    startup::print_config_summary(&cfg);

    let rt = startup::build_runtime(&cfg)?;
    rt.block_on(server::run(cfg))?;
    Ok(())
}
