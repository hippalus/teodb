//! Tokio runtime construction.
//!
//! Builds a multi-thread runtime with configurable worker threads,
//! blocking pool size, and stack size. Avoids the `#[tokio::main]` macro,
//! so every parameter is explicit and tunable via config file/env/CLI.

use tokio::runtime::Runtime;

use crate::config::TeoDBConfig;

/// Build a Tokio multi-thread runtime from configuration.
pub fn build_runtime(cfg: &TeoDBConfig) -> eyre::Result<Runtime> {
    let mut builder = tokio::runtime::Builder::new_multi_thread();

    builder.enable_all();
    builder.max_blocking_threads(cfg.runtime.max_blocking_threads);
    builder.thread_stack_size(cfg.runtime.thread_stack_size);
    builder.thread_name("teodb-worker");

    if let Some(n) = cfg.effective_worker_threads() {
        builder.worker_threads(n);
    }

    let runtime = builder
        .build()
        .map_err(|e| eyre::eyre!("failed to build tokio runtime: {e}"))?;

    Ok(runtime)
}
