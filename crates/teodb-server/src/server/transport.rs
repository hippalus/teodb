//! Transport layer — spawn REST and Flight gRPC servers with optional TLS.

use std::sync::Arc;

use axum_server::tls_rustls::{RustlsAcceptor, RustlsConfig};
use tracing::{Instrument, error, info};

use crate::metrics::Metrics;

use super::flight_admission::FlightConcurrencyLayer;
use super::incoming::{ConnectionMetrics, IncomingSettings, LimitedAcceptor, flight_incoming};
use super::tls::TlsBundle;

pub struct RestTransportConfig {
    pub addr: String,
    pub tls_bundle: Option<Arc<TlsBundle>>,
    pub max_connections: usize,
    pub idle_timeout: std::time::Duration,
}

pub struct FlightTransportConfig {
    pub addr: String,
    pub tls_bundle: Option<Arc<TlsBundle>>,
    pub max_connections: usize,
    pub max_in_flight_requests: usize,
    pub max_streams_per_connection: u32,
    pub idle_timeout: std::time::Duration,
}

fn incoming_settings(
    max_connections: usize,
    idle_timeout: std::time::Duration,
    metrics: &Metrics,
    transport: &'static str,
) -> IncomingSettings {
    IncomingSettings::new(
        max_connections,
        idle_timeout,
        ConnectionMetrics::new(
            metrics.transport.active_connections.clone(),
            metrics
                .transport
                .admission_rejections_total
                .clone(),
        ),
        transport,
    )
}

/// Spawn the Axum REST server with optional TLS and graceful shutdown.
pub fn spawn_rest_server(
    config: RestTransportConfig,
    router: axum::Router,
    metrics: Arc<Metrics>,
    shutdown_rx: tokio::sync::watch::Receiver<bool>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(
        async move {
            let listener = match tokio::net::TcpListener::bind(&config.addr).await {
                Ok(l) => l,
                Err(e) => {
                    error!(addr = %config.addr, error = %e, "failed to bind REST listener");
                    return;
                }
            };
            let incoming = incoming_settings(config.max_connections, config.idle_timeout, &metrics, "rest");

            if let Some(bundle) = config.tls_bundle {
                info!(addr = %config.addr, "REST server listening with TLS");
                serve_rest_tls(listener, bundle, router, incoming, shutdown_rx).await;
            } else {
                info!(addr = %config.addr, "REST server listening (plaintext)");
                serve_rest_plain(listener, router, incoming, shutdown_rx).await;
            }
        }
        .instrument(tracing::info_span!("rest_server")),
    )
}

fn rest_shutdown_task(
    handle: axum_server::Handle<std::net::SocketAddr>,
    mut shutdown_rx: tokio::sync::watch::Receiver<bool>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        if *shutdown_rx.borrow()
            || shutdown_rx
                .wait_for(|shutdown| *shutdown)
                .await
                .is_ok()
        {
            handle.graceful_shutdown(None);
        }
    })
}

async fn serve_rest_plain(
    listener: tokio::net::TcpListener,
    router: axum::Router,
    incoming: IncomingSettings,
    shutdown_rx: tokio::sync::watch::Receiver<bool>,
) {
    let handle = axum_server::Handle::new();
    let shutdown_task = rest_shutdown_task(handle.clone(), shutdown_rx);
    let result = axum_server::Server::<std::net::SocketAddr>::from_listener(listener)
        .acceptor(LimitedAcceptor::new(incoming))
        .handle(handle)
        .serve(router.into_make_service_with_connect_info::<std::net::SocketAddr>())
        .await;
    shutdown_task.abort();

    if let Err(error) = result {
        error!(%error, "REST server exited with error");
    }
}

/// Serve REST over TLS with concurrent handshakes and bounded connections.
async fn serve_rest_tls(
    listener: tokio::net::TcpListener,
    bundle: Arc<TlsBundle>,
    router: axum::Router,
    incoming: IncomingSettings,
    shutdown_rx: tokio::sync::watch::Receiver<bool>,
) {
    let handle = axum_server::Handle::new();
    let shutdown_task = rest_shutdown_task(handle.clone(), shutdown_rx);
    let tls = RustlsAcceptor::new(RustlsConfig::from_config(bundle.rustls_config.clone()))
        .acceptor(LimitedAcceptor::new(incoming));
    let result = axum_server::Server::<std::net::SocketAddr>::from_listener(listener)
        .acceptor(tls)
        .handle(handle)
        .serve(router.into_make_service_with_connect_info::<std::net::SocketAddr>())
        .await;
    shutdown_task.abort();

    if let Err(error) = result {
        error!(%error, "REST TLS server exited with error");
    }
}

/// Spawn the Arrow Flight gRPC server with optional TLS and graceful shutdown.
pub fn spawn_flight_server(
    config: FlightTransportConfig,
    flight_server: arrow_flight::flight_service_server::FlightServiceServer<teodb_api::flight::TeoFlightService>,
    authorization: Arc<teodb_api::ApiAuthorization>,
    metrics: Arc<Metrics>,
    mut shutdown_rx: tokio::sync::watch::Receiver<bool>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(
        async move {
            let listener = match tokio::net::TcpListener::bind(&config.addr).await {
                Ok(listener) => listener,
                Err(error) => {
                    error!(addr = %config.addr, %error, "failed to bind Flight listener");
                    return;
                }
            };
            let incoming = flight_incoming(
                listener,
                incoming_settings(config.max_connections, config.idle_timeout, &metrics, "flight"),
            );
            let keepalive_interval = (config.idle_timeout / 3).max(std::time::Duration::from_secs(1));
            let keepalive_timeout = (config.idle_timeout / 6).max(std::time::Duration::from_secs(1));
            let mut builder = tonic::transport::Server::builder()
                .concurrency_limit_per_connection(config.max_streams_per_connection as usize)
                .load_shed(true)
                .max_concurrent_streams(Some(config.max_streams_per_connection))
                .http2_keepalive_interval(Some(keepalive_interval))
                .http2_keepalive_timeout(Some(keepalive_timeout))
                .layer(FlightConcurrencyLayer::new(
                    config.max_in_flight_requests,
                    authorization,
                ));

            if let Some(bundle) = config.tls_bundle {
                let identity = tonic::transport::Identity::from_pem(&bundle.cert_pem, &bundle.key_pem);
                let mut tls = tonic::transport::ServerTlsConfig::new().identity(identity);

                // Enable mTLS if a client CA certificate is available.
                if let Some(ref ca_pem) = bundle.client_ca_pem {
                    let ca = tonic::transport::Certificate::from_pem(ca_pem);
                    tls = tls.client_ca_root(ca);
                    info!(addr = %config.addr, "Flight gRPC mTLS enabled");
                }

                match builder.tls_config(tls) {
                    Ok(mut configured) => {
                        info!(addr = %config.addr, "Flight gRPC listening with TLS");
                        configured
                            .add_service(flight_server)
                            .serve_with_incoming_shutdown(incoming, async move {
                                let _ = shutdown_rx.changed().await;
                            })
                            .await
                            .unwrap_or_else(|e| error!(error = %e, "Flight server exited with error"));
                        return;
                    }
                    Err(e) => {
                        error!(error = %e, "failed to configure Flight TLS");
                        return;
                    }
                }
            }

            info!(addr = %config.addr, "Flight gRPC listening (plaintext)");
            builder
                .add_service(flight_server)
                .serve_with_incoming_shutdown(incoming, async move {
                    let _ = shutdown_rx.changed().await;
                })
                .await
                .unwrap_or_else(|e| error!(error = %e, "Flight server exited with error"));
        }
        .instrument(tracing::info_span!("flight_server")),
    )
}
