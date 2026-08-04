use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::sync::{Mutex as StdMutex, Weak};
use std::time::{Duration, Instant};

use datafusion::common::Result as DFResult;
use moka::sync::Cache;

use super::table::TeoTableProvider;
use super::table_loader::DataFusionTableLoader;

const DEFAULT_METADATA_PROVIDER_CACHE_MAX_ENTRIES: u64 = 10_000;
const DEFAULT_LOAD_LOCK_OVERFLOW_STRIPES: usize = 64;

struct LoadLockRegistry {
    entries: StdMutex<HashMap<String, Weak<tokio::sync::Mutex<()>>>>,
    max_entries: usize,
    overflow: Vec<Arc<tokio::sync::Mutex<()>>>,
}

impl LoadLockRegistry {
    fn new(max_entries: usize, overflow_stripes: usize) -> Self {
        assert!(max_entries > 0, "load-lock registry requires a positive cap");
        assert!(overflow_stripes > 0, "load-lock registry requires overflow stripes");
        Self {
            entries: StdMutex::new(HashMap::new()),
            max_entries,
            overflow: (0..overflow_stripes)
                .map(|_| Arc::new(tokio::sync::Mutex::new(())))
                .collect(),
        }
    }

    fn lock(&self, key: &str) -> Arc<tokio::sync::Mutex<()>> {
        let mut entries = self
            .entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(existing) = entries.get(key).and_then(Weak::upgrade) {
            return existing;
        }
        entries.remove(key);
        entries.retain(|_, lock| lock.strong_count() > 0);

        if entries.len() < self.max_entries {
            let lock = Arc::new(tokio::sync::Mutex::new(()));
            entries.insert(key.to_owned(), Arc::downgrade(&lock));
            return lock;
        }
        drop(entries);

        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        key.hash(&mut hasher);
        let index = (hasher.finish() as usize) % self.overflow.len();
        self.overflow[index].clone()
    }

    #[cfg(test)]
    fn tracked_len(&self) -> usize {
        self.entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .len()
    }
}

pub(super) struct MetadataCache {
    metadata_ttl: Duration,
    // Store load timestamps manually instead of using Moka TTL so expired
    // values can be served stale while a single refresh is in flight.
    table_providers: Cache<String, (Arc<TeoTableProvider>, Instant)>,
    load_locks: LoadLockRegistry,
    table_names: Cache<String, (Vec<String>, Instant)>,
    table_names_refreshing: Arc<std::sync::atomic::AtomicBool>,
    metrics: Arc<MetadataMetrics>,
}

#[derive(Debug, Default)]
struct MetadataMetrics {
    refresh_success: std::sync::atomic::AtomicU64,
    refresh_failure: std::sync::atomic::AtomicU64,
    stale_serves: std::sync::atomic::AtomicU64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MetadataMetricsSnapshot {
    pub refresh_success: u64,
    pub refresh_failure: u64,
    pub stale_serves: u64,
}

impl MetadataCache {
    pub(super) fn new(metadata_ttl: Duration) -> Self {
        Self {
            metadata_ttl,
            table_providers: Cache::builder()
                .max_capacity(DEFAULT_METADATA_PROVIDER_CACHE_MAX_ENTRIES)
                .build(),
            load_locks: LoadLockRegistry::new(
                DEFAULT_METADATA_PROVIDER_CACHE_MAX_ENTRIES as usize,
                DEFAULT_LOAD_LOCK_OVERFLOW_STRIPES,
            ),
            table_names: Cache::builder().max_capacity(1_000).build(),
            table_names_refreshing: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            metrics: Arc::new(MetadataMetrics::default()),
        }
    }

    pub(super) fn metrics_snapshot(&self) -> MetadataMetricsSnapshot {
        use std::sync::atomic::Ordering::Relaxed;
        MetadataMetricsSnapshot {
            refresh_success: self.metrics.refresh_success.load(Relaxed),
            refresh_failure: self.metrics.refresh_failure.load(Relaxed),
            stale_serves: self.metrics.stale_serves.load(Relaxed),
        }
    }

    pub(super) fn table_names_snapshot(&self, loader: &DataFusionTableLoader) -> Vec<String> {
        let (cached, fresh) = self.cached_table_names(loader.namespace());
        match cached {
            Some(names) if fresh => names,
            Some(stale) => {
                self.record_stale_serve();
                self.spawn_table_names_refresh(loader.clone());
                stale
            }
            None => {
                let names = loader.blocking_load_table_names();
                if !self.metadata_ttl.is_zero() {
                    self.table_names
                        .insert(loader.namespace().to_owned(), (names.clone(), Instant::now()));
                }
                names
            }
        }
    }

    pub(super) async fn table(
        &self,
        name: &str,
        loader: &DataFusionTableLoader,
    ) -> DFResult<Option<Arc<TeoTableProvider>>> {
        if self.metadata_ttl.is_zero() {
            return loader.load_provider(name).await;
        }

        if let Some((provider, loaded_at)) = self.cached(name) {
            if loaded_at.elapsed() < self.metadata_ttl {
                return Ok(Some(provider));
            }

            let lock = self.load_lock(name);
            let Ok(_guard) = lock.try_lock() else {
                self.record_stale_serve();
                return Ok(Some(provider));
            };
            return match loader.load_provider(name).await {
                Ok(Some(fresh)) => {
                    self.record_refresh_success();
                    self.table_providers
                        .insert(name.to_owned(), (fresh.clone(), Instant::now()));
                    Ok(Some(fresh))
                }
                Ok(None) => {
                    self.record_refresh_success();
                    self.table_providers.invalidate(name);
                    Ok(None)
                }
                Err(e) => {
                    self.record_refresh_failure();
                    self.record_stale_serve();
                    tracing::warn!(
                        target: "teodb::metadata",
                        table = %name, error = %e,
                        "metadata refresh failed, serving stale provider"
                    );
                    Ok(Some(provider))
                }
            };
        }

        let lock = self.load_lock(name);
        let _guard = lock.lock().await;
        if let Some((provider, loaded_at)) = self.cached(name)
            && loaded_at.elapsed() < self.metadata_ttl
        {
            return Ok(Some(provider));
        }
        let loaded = loader.load_provider(name).await?;
        if let Some(provider) = &loaded {
            self.table_providers
                .insert(name.to_owned(), (provider.clone(), Instant::now()));
        }
        Ok(loaded)
    }

    fn cached(&self, name: &str) -> Option<(Arc<TeoTableProvider>, Instant)> {
        self.table_providers.get(name)
    }

    fn load_lock(&self, name: &str) -> Arc<tokio::sync::Mutex<()>> {
        self.load_locks.lock(name)
    }

    fn cached_table_names(&self, namespace: &str) -> (Option<Vec<String>>, bool) {
        match self.table_names.get(namespace) {
            Some((names, loaded_at)) => {
                let fresh = !self.metadata_ttl.is_zero() && loaded_at.elapsed() < self.metadata_ttl;
                (Some(names.clone()), fresh)
            }
            None => (None, false),
        }
    }

    fn spawn_table_names_refresh(&self, loader: DataFusionTableLoader) {
        use std::sync::atomic::Ordering;
        if self
            .table_names_refreshing
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return;
        }
        let namespace = loader.namespace().to_owned();
        let cache = self.table_names.clone();
        let refreshing = self.table_names_refreshing.clone();
        let metrics = self.metrics.clone();
        tokio::spawn(async move {
            match loader.load_table_names().await {
                Ok(names) => {
                    metrics
                        .refresh_success
                        .fetch_add(1, Ordering::Relaxed);
                    cache.insert(namespace.clone(), (names, Instant::now()));
                }
                Err(error) => {
                    metrics
                        .refresh_failure
                        .fetch_add(1, Ordering::Relaxed);
                    tracing::warn!(target: "teodb::metadata", namespace = %namespace, %error, "table_names: background refresh failed, serving stale");
                }
            }
            refreshing.store(false, Ordering::Release);
        });
    }

    fn record_refresh_success(&self) {
        self.metrics
            .refresh_success
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    fn record_refresh_failure(&self) {
        self.metrics
            .refresh_failure
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    fn record_stale_serve(&self) {
        self.metrics
            .stale_serves
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }
}

#[cfg(test)]
mod load_lock_tests {
    use super::*;

    #[test]
    fn same_live_key_returns_the_same_lock() {
        let registry = LoadLockRegistry::new(4, 2);
        let first = registry.lock("events");
        let second = registry.lock("events");
        assert!(Arc::ptr_eq(&first, &second));
    }

    #[test]
    fn dead_entries_are_reclaimed() {
        let registry = LoadLockRegistry::new(1, 2);
        let first = registry.lock("events");
        assert_eq!(registry.tracked_len(), 1);
        drop(first);

        let replacement = registry.lock("users");
        assert_eq!(registry.tracked_len(), 1);
        assert_eq!(Arc::strong_count(&replacement), 1);
    }

    #[test]
    fn live_cap_uses_stable_bounded_overflow_locks() {
        let registry = LoadLockRegistry::new(2, 2);
        let _first = registry.lock("one");
        let _second = registry.lock("two");

        let overflow_a = registry.lock("overflow-key");
        let overflow_b = registry.lock("overflow-key");
        assert!(Arc::ptr_eq(&overflow_a, &overflow_b));
        assert_eq!(registry.tracked_len(), 2);
        assert_eq!(registry.overflow.len(), 2);
    }
}
