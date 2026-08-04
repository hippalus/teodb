//! Observability setup: tracing subscriber with optional OpenTelemetry OTLP export.
//!
//! When `otlp_endpoint` is configured, traces are exported via OTLP gRPC.
//! When absent, only local structured logging is enabled.

use opentelemetry::trace::TracerProvider;
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::trace::SdkTracerProvider;
use tracing_subscriber::fmt;
use tracing_subscriber::prelude::*;

use crate::config::{LogFormat, ObservabilityConfig};

/// Holds the OTel provider so we can shut it down gracefully.
static PROVIDER: std::sync::OnceLock<SdkTracerProvider> = std::sync::OnceLock::new();

/// Initialize the tracing subscriber based on configuration.
///
/// Returns an error only if the OTLP exporter cannot be created (e.g., invalid endpoint).
pub fn init_tracing(config: &ObservabilityConfig) -> eyre::Result<()> {
    // Only use RUST_LOG if explicitly set to a non-empty value;
    // otherwise fall back to the config file's log_level.
    let filter = match std::env::var("RUST_LOG") {
        Ok(v) if !v.trim().is_empty() => tracing_subscriber::EnvFilter::new(v),
        _ => {
            // Scope noisy dependencies to warn even when TeoDB is at debug/trace.
            let base = config.log_level.to_string();
            let directives = format!("{base},hyper=warn,h2=warn,tower=warn,reqwest=warn,iceberg=info");
            tracing_subscriber::EnvFilter::new(directives)
        }
    };

    let fmt_layer = match config.log_format {
        LogFormat::Json => fmt::layer()
            .json()
            .with_thread_ids(true)
            .with_file(true)
            .with_thread_names(true)
            .with_line_number(true)
            .with_target(false)
            .with_timer(fmt::time::UtcTime::rfc_3339())
            .boxed(),
        LogFormat::Compact => fmt::layer()
            .compact()
            .with_thread_ids(true)
            .with_file(true)
            .with_thread_names(true)
            .with_line_number(true)
            .with_target(false)
            .with_timer(fmt::time::UtcTime::rfc_3339())
            .boxed(),
        LogFormat::Pretty => fmt::layer()
            .with_thread_ids(true)
            .with_file(true)
            .with_thread_names(true)
            .with_line_number(true)
            .with_target(false)
            .with_timer(fmt::time::UtcTime::rfc_3339())
            .boxed(),
    };

    if let Some(ref endpoint) = config.otlp_endpoint {
        let resource = Resource::builder()
            .with_service_name(config.service_name.clone())
            .build();

        let exporter = opentelemetry_otlp::SpanExporter::builder()
            .with_tonic()
            .with_endpoint(endpoint)
            .build()?;

        let provider = SdkTracerProvider::builder()
            .with_resource(resource)
            .with_batch_exporter(exporter)
            .build();

        let tracer = provider.tracer("teodb");
        let _ = PROVIDER.set(provider);

        let otel_layer = tracing_opentelemetry::layer().with_tracer(tracer);

        tracing_subscriber::registry()
            .with(filter)
            .with(fmt_layer)
            .with(otel_layer)
            .init();
    } else {
        tracing_subscriber::registry()
            .with(filter)
            .with(fmt_layer)
            .init();
    }

    Ok(())
}

/// Flush and shut down the OTel provider. Call during graceful shutdown.
pub fn shutdown_tracing() {
    if let Some(provider) = PROVIDER.get()
        && let Err(e) = provider.shutdown()
    {
        eprintln!("OTel shutdown error: {e}");
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn module_compiles() {
        // Tracing init is global state — the subscriber can only be set once per process.
    }
}
