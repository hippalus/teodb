//! Graceful shutdown coordination.
//!
//! Listens for shutdown signals and coordinates drain across subsystems.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use tokio::sync::watch;
use tracing::{info, warn};

/// Shared shutdown coordinator.
pub struct ShutdownCoordinator {
    /// True when drain has been requested.
    drain_requested: Arc<AtomicBool>,
    /// Watch channel: becomes `true` when drain starts.
    shutdown_tx: watch::Sender<bool>,
    /// Receivers clone this.
    shutdown_rx: watch::Receiver<bool>,
    /// Drain timeout.
    drain_timeout: Duration,
}

impl ShutdownCoordinator {
    pub fn new(drain_timeout: Duration) -> Self {
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        Self {
            drain_requested: Arc::new(AtomicBool::new(false)),
            shutdown_tx,
            shutdown_rx,
            drain_timeout,
        }
    }

    /// Get a watch receiver that subsystems can use to detect shutdown.
    pub fn subscribe(&self) -> watch::Receiver<bool> {
        self.shutdown_rx.clone()
    }

    /// Check if drain has been requested.
    pub fn is_draining(&self) -> bool {
        self.drain_requested.load(Ordering::Relaxed)
    }

    /// Get a shared reference to the drain flag for use by health probes.
    pub fn drain_flag(&self) -> Arc<AtomicBool> {
        self.drain_requested.clone()
    }

    /// Request drain. Idempotent.
    pub fn request_drain(&self) {
        if !self.drain_requested.swap(true, Ordering::SeqCst) {
            info!("drain requested, signaling subsystems");
            let _ = self.shutdown_tx.send(true);
        }
    }

    /// Wait for an OS shutdown signal and trigger drain.
    pub async fn wait_for_signal(&self) {
        wait_for_shutdown_signal().await;
        self.request_drain();
    }

    /// Run the drain sequence with timeout. Returns `true` if drain
    /// completed within the timeout.
    pub async fn drain_with_timeout<F, Fut>(&self, drain_fn: F) -> bool
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = ()>,
    {
        match tokio::time::timeout(self.drain_timeout, drain_fn()).await {
            Ok(()) => {
                info!("drain completed successfully");
                true
            }
            Err(_) => {
                warn!(timeout_secs = self.drain_timeout.as_secs(), "drain timeout exceeded");
                false
            }
        }
    }
}

#[cfg(unix)]
async fn wait_for_shutdown_signal() {
    use tokio::signal::unix::{Signal, SignalKind, signal};

    let mut sigterm = register_unix_signal(SignalKind::terminate(), "SIGTERM");
    let mut sigint = register_unix_signal(SignalKind::interrupt(), "SIGINT");
    let mut sigquit = register_unix_signal(SignalKind::quit(), "SIGQUIT");

    if sigterm.is_none() && sigint.is_none() && sigquit.is_none() {
        wait_for_ctrl_c().await;
        return;
    }

    tokio::select! {
        _ = recv_unix_signal(&mut sigterm), if sigterm.is_some() => info!("received SIGTERM"),
        _ = recv_unix_signal(&mut sigint), if sigint.is_some() => info!("received SIGINT"),
        _ = recv_unix_signal(&mut sigquit), if sigquit.is_some() => info!("received SIGQUIT"),
    }

    async fn recv_unix_signal(signal: &mut Option<Signal>) {
        if let Some(signal) = signal.as_mut() {
            signal.recv().await;
        } else {
            std::future::pending::<()>().await;
        }
    }

    fn register_unix_signal(kind: SignalKind, name: &'static str) -> Option<Signal> {
        match signal(kind) {
            Ok(signal) => Some(signal),
            Err(error) => {
                warn!(signal = name, %error, "failed to register shutdown signal handler");
                None
            }
        }
    }
}

#[cfg(not(unix))]
async fn wait_for_shutdown_signal() {
    wait_for_ctrl_c().await;
}

async fn wait_for_ctrl_c() {
    match tokio::signal::ctrl_c().await {
        Ok(()) => info!("received SIGINT (Ctrl+C)"),
        Err(error) => warn!(%error, "failed to wait for Ctrl+C"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn request_drain_is_idempotent() {
        let coord = ShutdownCoordinator::new(Duration::from_secs(10));
        assert!(!coord.is_draining());
        coord.request_drain();
        assert!(coord.is_draining());
        coord.request_drain(); // Idempotent
        assert!(coord.is_draining());
    }

    #[test]
    fn subscribe_receives_signal() {
        let coord = ShutdownCoordinator::new(Duration::from_secs(10));
        let mut rx = coord.subscribe();
        assert!(!*rx.borrow());
        coord.request_drain();
        assert!(*rx.borrow_and_update());
    }

    #[tokio::test]
    async fn drain_with_timeout_succeeds() {
        let coord = ShutdownCoordinator::new(Duration::from_secs(5));
        let ok = coord
            .drain_with_timeout(|| async {
                // Simulated fast drain
            })
            .await;
        assert!(ok);
    }

    #[tokio::test]
    async fn drain_with_timeout_exceeds() {
        let coord = ShutdownCoordinator::new(Duration::from_millis(50));
        let ok = coord
            .drain_with_timeout(|| async {
                tokio::time::sleep(Duration::from_secs(10)).await;
            })
            .await;
        assert!(!ok);
    }
}
