use tracing::info;

use super::Flusher;

pub struct FlushLoopConfig {
    pub flusher: Flusher,
    pub interval: std::time::Duration,
}

#[tracing::instrument(name = "ingest.flush_loop", skip_all)]
pub async fn flush_loop(config: FlushLoopConfig, mut shutdown: tokio::sync::watch::Receiver<bool>) {
    let mut ticker = tokio::time::interval(config.interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            _ = ticker.tick() => {
                config.flusher.flush_all_tables().await;
            }
            _ = shutdown.changed() => {
                if *shutdown.borrow() {
                    info!("flush loop shutting down, final flush");
                    config.flusher.flush_all_tables().await;
                    return;
                }
            }
        }
    }
}
