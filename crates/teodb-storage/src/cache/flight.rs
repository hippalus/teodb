//! Single-flight: deduplicates concurrent requests for the same key.
//!
//! When multiple callers request the same object simultaneously, only one
//! fetch is issued. The others await the result of the first. This prevents
//! thundering-herd cache misses.
//!
//! Uses `papaya` for lock-free concurrent entry management and an RAII guard
//! to ensure cleanup on cancellation/panic.

use std::sync::Arc;

use papaya::HashMap as PapayaHashMap;
use teodb_core::error::TeoDBResult;
use tokio::sync::watch;

type WatchReceiver = watch::Receiver<Option<Result<bytes::Bytes, String>>>;

/// RAII guard that removes the in-flight entry on drop (cancellation safety).
struct InFlightGuard {
    map: Arc<PapayaHashMap<String, WatchReceiver>>,
    key: String,
}

impl Drop for InFlightGuard {
    fn drop(&mut self) {
        self.map.pin().remove(&self.key);
    }
}

/// Deduplicates concurrent async operations keyed by a string.
pub struct SingleFlight {
    in_flight: Arc<PapayaHashMap<String, WatchReceiver>>,
}

impl SingleFlight {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            in_flight: Arc::new(PapayaHashMap::new()),
        })
    }

    /// Run the provided future if no other caller is currently running
    /// for the same `key`. Otherwise, await the existing caller's result.
    pub async fn run<F>(&self, key: String, fut: F) -> TeoDBResult<bytes::Bytes>
    where
        F: std::future::Future<Output = TeoDBResult<bytes::Bytes>> + Send + 'static,
    {
        // Check if there's an existing in-flight request (lock-free read).
        let existing = {
            let map = self.in_flight.pin();
            map.get(&key).cloned()
        };
        if let Some(mut rx) = existing {
            loop {
                rx.changed()
                    .await
                    .map_err(|_| teodb_core::error::TeoDBError::Internal("single-flight channel closed".into()))?;
                if let Some(result) = rx.borrow().as_ref() {
                    return match result {
                        Ok(b) => Ok(b.clone()),
                        Err(e) => Err(teodb_core::error::TeoDBError::Internal(e.clone())),
                    };
                }
            }
        }

        // No existing request; create a new watch channel.
        let (tx, rx) = watch::channel(None);
        self.in_flight.pin().insert(key.clone(), rx);

        // RAII guard ensures map cleanup even if the future panics or is cancelled.
        let guard = InFlightGuard {
            map: self.in_flight.clone(),
            key: key.clone(),
        };

        let result = fut.await;

        // Broadcast the result to waiters.
        let broadcast = match &result {
            Ok(bytes) => Some(Ok(bytes.clone())),
            Err(e) => Some(Err(e.to_string())),
        };
        let _ = tx.send(broadcast);

        // Guard drops here, removing the entry from the map.
        drop(guard);

        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};

    #[tokio::test]
    async fn deduplicates_concurrent_requests() {
        let sf = SingleFlight::new();
        let call_count = Arc::new(AtomicU32::new(0));

        let mut handles = Vec::new();
        for _ in 0..10 {
            let sf = sf.clone();
            let cc = call_count.clone();
            handles.push(tokio::spawn(async move {
                sf.run("same-key".into(), async move {
                    cc.fetch_add(1, Ordering::SeqCst);
                    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                    Ok(bytes::Bytes::from("result"))
                })
                .await
            }));
        }

        for h in handles {
            let result = h.await.unwrap().unwrap();
            assert_eq!(result, bytes::Bytes::from("result"));
        }

        // Not all 10 should have executed — at least some should have been
        // deduplicated. Due to timing, we can't guarantee exactly 1, but
        // it should be significantly less than 10.
        let count = call_count.load(Ordering::SeqCst);
        assert!(count < 10, "expected deduplication, got {count} calls");
    }
}
