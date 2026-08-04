//! Cache index backed by the local filesystem.
//!
//! Each cached object is stored as a `.bin` file under a content-addressed
//! layout: `{root}/v1/{xxh3_hex[0..2]}/{xxh3_hex}.bin`. An in-memory index
//! tracks metadata (URI, size, etag, checksum, last access) for fast lookups.
//!
//! Eviction uses a `BTreeSet` ordered by `(last_access_ms, uri)` for O(log n)
//! LRU removal instead of sorting the entire entry set.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use teodb_core::error::{TeoDBError, TeoDBResult};

/// Configuration for the cache index.
#[derive(Debug, Clone)]
pub struct CacheConfig {
    pub root_dir: PathBuf,
    pub max_total_bytes: u64,
    pub max_per_object_bytes: u64,
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            root_dir: PathBuf::from("/tmp/teodb-cache"),
            max_total_bytes: 10 * 1024 * 1024 * 1024, // 10 GiB
            max_per_object_bytes: 512 * 1024 * 1024,  // 512 MiB
        }
    }
}

/// Metadata for a cached object.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheEntry {
    pub uri: String,
    pub size: u64,
    pub etag: Option<String>,
    pub checksum: u64,
    pub last_access_ms: i64,
}

/// Key for the LRU ordering set: (access_time, uri). Oldest first.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct LruKey {
    last_access_ms: i64,
    uri: String,
}

struct IndexState {
    entries: std::collections::HashMap<String, CacheEntry>,
    /// Sorted by (last_access_ms, uri) for O(log n) LRU eviction.
    lru_order: BTreeSet<LruKey>,
    total_bytes: u64,
}

/// In-memory cache index with file-backed persistence and O(log n) LRU eviction.
pub struct CacheIndex {
    config: CacheConfig,
    state: Mutex<IndexState>,
    hits: AtomicU64,
    misses: AtomicU64,
    /// Set on every mutation; cleared when the index is persisted. The
    /// periodic + shutdown persister (run via `spawn_blocking`) flushes when
    /// set, so put/remove no longer rewrite + fsync the whole index inline on
    /// the async runtime.
    dirty: std::sync::atomic::AtomicBool,
}

impl CacheIndex {
    /// Open or create a cache index at the given directory.
    pub fn open(root: &Path) -> TeoDBResult<Arc<Self>> {
        Self::open_with_config(CacheConfig {
            root_dir: root.to_path_buf(),
            ..Default::default()
        })
    }

    /// Open with explicit configuration.
    pub fn open_with_config(config: CacheConfig) -> TeoDBResult<Arc<Self>> {
        std::fs::create_dir_all(&config.root_dir)
            .map_err(|e| TeoDBError::Internal(format!("create cache dir: {e}")))?;

        let data_dir = config.root_dir.join("v1");
        std::fs::create_dir_all(&data_dir).map_err(|e| TeoDBError::Internal(format!("create cache data dir: {e}")))?;

        // Clean up stale temp files from interrupted writes.
        Self::cleanup_temp_files(&data_dir);

        // Try to load persisted index.
        let index_path = config.root_dir.join("index.json");
        let (entries, lru_order, total_bytes) = if index_path.exists() {
            match std::fs::read_to_string(&index_path) {
                Ok(json) => {
                    let loaded: Vec<CacheEntry> = serde_json::from_str(&json).unwrap_or_else(|e| {
                        tracing::warn!(error = %e, "corrupt cache index — starting fresh");
                        Vec::new()
                    });
                    let total = loaded.iter().map(|e| e.size).sum();
                    let mut order = BTreeSet::new();
                    let map: std::collections::HashMap<_, _> = loaded
                        .into_iter()
                        .map(|e| {
                            order.insert(LruKey {
                                last_access_ms: e.last_access_ms,
                                uri: e.uri.clone(),
                            });
                            (e.uri.clone(), e)
                        })
                        .collect();
                    (map, order, total)
                }
                Err(_) => (std::collections::HashMap::new(), BTreeSet::new(), 0),
            }
        } else {
            (std::collections::HashMap::new(), BTreeSet::new(), 0)
        };

        Ok(Arc::new(Self {
            config,
            state: Mutex::new(IndexState {
                entries,
                lru_order,
                total_bytes,
            }),
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
            dirty: std::sync::atomic::AtomicBool::new(false),
        }))
    }

    /// Remove stale `.tmp` files left behind by interrupted writes.
    fn cleanup_temp_files(data_dir: &Path) {
        let mut cleaned = 0u64;
        if let Ok(entries) = std::fs::read_dir(data_dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.is_dir() {
                    // Recurse into bucket subdirectories.
                    if let Ok(sub_entries) = std::fs::read_dir(&path) {
                        for sub in sub_entries.flatten() {
                            let sub_path = sub.path();
                            if sub_path
                                .extension()
                                .is_some_and(|ext| ext == "tmp")
                                && std::fs::remove_file(&sub_path).is_ok()
                            {
                                cleaned += 1;
                            }
                        }
                    }
                } else if path.extension().is_some_and(|ext| ext == "tmp") && std::fs::remove_file(&path).is_ok() {
                    cleaned += 1;
                }
            }
        }
        if cleaned > 0 {
            tracing::info!(cleaned, "cache: removed stale temp files from previous crash");
        }
    }

    /// Get cached bytes for a URI, reading from the local file.
    pub async fn get_cached(&self, uri: &str) -> TeoDBResult<Option<Bytes>> {
        let entry = {
            let state = self.state.lock();
            state.entries.get(uri).cloned()
        };

        let entry = match entry {
            Some(e) => e,
            None => {
                self.misses.fetch_add(1, Ordering::Relaxed);
                return Ok(None);
            }
        };

        let bin_path = self.bin_path_for_checksum(entry.checksum);
        match tokio::fs::read(&bin_path).await {
            Ok(data) => {
                // Verify checksum.
                let actual = xxhash_rust::xxh3::xxh3_64(&data);
                if actual != entry.checksum {
                    tracing::warn!(uri, "cache checksum mismatch, evicting");
                    self.remove(uri).await?;
                    return Ok(None);
                }
                // Update access time for LRU.
                self.touch(uri);
                self.hits.fetch_add(1, Ordering::Relaxed);
                Ok(Some(Bytes::from(data)))
            }
            Err(_) => {
                // File missing; remove stale entry.
                let mut state = self.state.lock();
                if let Some(removed) = state.entries.remove(uri) {
                    state.lru_order.remove(&LruKey {
                        last_access_ms: removed.last_access_ms,
                        uri: uri.to_owned(),
                    });
                    state.total_bytes = state.total_bytes.saturating_sub(removed.size);
                }
                Ok(None)
            }
        }
    }

    /// Store bytes in the cache.
    pub async fn put_cached(&self, uri: &str, data: &Bytes, etag: Option<&str>) -> TeoDBResult<()> {
        if data.len() as u64 > self.config.max_per_object_bytes {
            tracing::warn!(
                uri,
                object_bytes = data.len(),
                max_per_object_bytes = self.config.max_per_object_bytes,
                "cache: skipping oversized object"
            );
            return Ok(());
        }

        let checksum = xxhash_rust::xxh3::xxh3_64(data);
        let bin_path = self.bin_path_for_checksum(checksum);

        if let Some(parent) = bin_path.parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .map_err(|e| TeoDBError::Internal(format!("create cache bucket dir: {e}")))?;
        }

        // Write atomically via temp file + rename.
        let tmp_path = bin_path.with_extension("tmp");
        tokio::fs::write(&tmp_path, data.as_ref())
            .await
            .map_err(|e| TeoDBError::Internal(format!("write cache file: {e}")))?;
        tokio::fs::rename(&tmp_path, &bin_path)
            .await
            .map_err(|e| TeoDBError::Internal(format!("rename cache file: {e}")))?;

        let now = chrono::Utc::now().timestamp_millis();
        let entry = CacheEntry {
            uri: uri.to_owned(),
            size: data.len() as u64,
            etag: etag.map(|s| s.to_owned()),
            checksum,
            last_access_ms: now,
        };

        let needs_eviction = {
            let mut state = self.state.lock();
            // Remove old entry if updating
            if let Some(old) = state.entries.insert(uri.to_owned(), entry) {
                state.lru_order.remove(&LruKey {
                    last_access_ms: old.last_access_ms,
                    uri: uri.to_owned(),
                });
                state.total_bytes = state.total_bytes.saturating_sub(old.size);
            }
            state.lru_order.insert(LruKey {
                last_access_ms: now,
                uri: uri.to_owned(),
            });
            state.total_bytes += data.len() as u64;
            state.total_bytes > self.config.max_total_bytes
        };

        if needs_eviction {
            self.maybe_evict().await?;
        }

        // Mark dirty; the periodic/shutdown persister flushes off the runtime.
        self.dirty.store(true, Ordering::Relaxed);

        Ok(())
    }

    /// Remove a cached object.
    pub async fn remove(&self, uri: &str) -> TeoDBResult<()> {
        let entry = {
            let mut state = self.state.lock();
            let removed = state.entries.remove(uri);
            if let Some(ref e) = removed {
                state.lru_order.remove(&LruKey {
                    last_access_ms: e.last_access_ms,
                    uri: uri.to_owned(),
                });
                state.total_bytes = state.total_bytes.saturating_sub(e.size);
            }
            removed
        };

        if let Some(entry) = entry {
            let bin_path = self.bin_path_for_checksum(entry.checksum);
            if let Err(e) = tokio::fs::remove_file(&bin_path).await {
                tracing::warn!(path = %bin_path.display(), error = %e, "cache: failed to remove evicted file");
            }
            // Mark dirty; the periodic/shutdown persister flushes off the runtime.
            self.dirty.store(true, Ordering::Relaxed);
        }

        Ok(())
    }

    /// Update last-access timestamp for a URI.
    pub fn touch(&self, uri: &str) {
        let mut state = self.state.lock();
        let old_ts = match state.entries.get(uri) {
            Some(entry) => entry.last_access_ms,
            None => return,
        };
        let old_key = LruKey {
            last_access_ms: old_ts,
            uri: uri.to_owned(),
        };
        state.lru_order.remove(&old_key);
        let now = chrono::Utc::now().timestamp_millis();
        state.lru_order.insert(LruKey {
            last_access_ms: now,
            uri: uri.to_owned(),
        });
        // Entry is guaranteed present; we checked above and hold the lock.
        if let Some(entry) = state.entries.get_mut(uri) {
            entry.last_access_ms = now;
        }
    }

    /// Total cached bytes.
    #[inline]
    pub fn total_bytes(&self) -> u64 {
        self.state.lock().total_bytes
    }

    /// Number of cached entries.
    #[inline]
    pub fn entry_count(&self) -> usize {
        self.state.lock().entries.len()
    }

    /// Total cache hits since process start.
    #[inline]
    pub fn hits(&self) -> u64 {
        self.hits.load(Ordering::Relaxed)
    }

    /// Total cache misses since process start.
    #[inline]
    pub fn misses(&self) -> u64 {
        self.misses.load(Ordering::Relaxed)
    }

    /// Evict LRU entries if the cache exceeds `max_total_bytes`.
    /// Uses the BTreeSet for O(log n) per-eviction instead of full sort.
    pub async fn maybe_evict(&self) -> TeoDBResult<()> {
        let target = (self.config.max_total_bytes as f64 * 0.9) as u64;
        let entries_to_evict: Vec<String> = {
            let state = self.state.lock();
            if state.total_bytes <= self.config.max_total_bytes {
                return Ok(());
            }

            let mut to_free = state.total_bytes - target;
            let mut evict_uris = Vec::new();
            // Iterate in LRU order (oldest first from BTreeSet)
            for lru_key in &state.lru_order {
                if to_free == 0 {
                    break;
                }
                if let Some(entry) = state.entries.get(&lru_key.uri) {
                    evict_uris.push(lru_key.uri.clone());
                    to_free = to_free.saturating_sub(entry.size);
                }
            }
            evict_uris
        };

        for uri in entries_to_evict {
            self.remove(&uri).await?;
        }

        Ok(())
    }

    /// Persist the index only if it changed since the last persist. Returns
    /// `true` if a write happened. Cheap to call frequently from the periodic
    /// persister.
    pub fn persist_if_dirty(&self) -> TeoDBResult<bool> {
        // Clear first so a concurrent mutation after the snapshot re-marks dirty
        // rather than being silently dropped by a later clear.
        if !self.dirty.swap(false, Ordering::AcqRel) {
            return Ok(false);
        }
        if let Err(e) = self.persist() {
            // Persist failed — keep the dirty flag so we retry next tick.
            self.dirty.store(true, Ordering::Release);
            return Err(e);
        }
        Ok(true)
    }

    /// Persist the index to disk for crash recovery.
    pub fn persist(&self) -> TeoDBResult<()> {
        self.dirty.store(false, Ordering::Release);
        let entries: Vec<CacheEntry> = {
            let state = self.state.lock();
            state.entries.values().cloned().collect()
        };

        let json = serde_json::to_string_pretty(&entries)
            .map_err(|e| TeoDBError::Internal(format!("serialize index: {e}")))?;

        let index_path = self.config.root_dir.join("index.json");
        let tmp_path = index_path.with_extension("json.tmp");
        std::fs::write(&tmp_path, &json).map_err(|e| TeoDBError::Internal(format!("write index: {e}")))?;

        // fsync before rename for durability.
        let file = std::fs::File::open(&tmp_path).map_err(|e| TeoDBError::Internal(format!("open for fsync: {e}")))?;
        file.sync_all()
            .map_err(|e| TeoDBError::Internal(format!("fsync index: {e}")))?;

        std::fs::rename(&tmp_path, &index_path).map_err(|e| TeoDBError::Internal(format!("rename index: {e}")))?;

        Ok(())
    }

    fn bin_path_for_checksum(&self, checksum: u64) -> PathBuf {
        let hex = format!("{checksum:016x}");
        let bucket = &hex[..2];
        self.config
            .root_dir
            .join("v1")
            .join(bucket)
            .join(format!("{hex}.bin"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn put_and_get() {
        let dir = tempfile::tempdir().unwrap();
        let index = CacheIndex::open(dir.path()).unwrap();

        let data = Bytes::from("hello world");
        index
            .put_cached("s3://b/key", &data, Some("etag1"))
            .await
            .unwrap();

        let got = index
            .get_cached("s3://b/key")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(got, data);
        assert_eq!(index.entry_count(), 1);
        assert_eq!(index.total_bytes(), 11);
    }

    #[tokio::test]
    async fn remove_clears_entry_and_file() {
        let dir = tempfile::tempdir().unwrap();
        let index = CacheIndex::open(dir.path()).unwrap();

        let data = Bytes::from("data");
        index
            .put_cached("s3://b/rm", &data, None)
            .await
            .unwrap();
        index.remove("s3://b/rm").await.unwrap();

        assert!(
            index
                .get_cached("s3://b/rm")
                .await
                .unwrap()
                .is_none()
        );
        assert_eq!(index.total_bytes(), 0);
    }

    #[tokio::test]
    async fn eviction_under_pressure() {
        let dir = tempfile::tempdir().unwrap();
        let config = CacheConfig {
            root_dir: dir.path().to_path_buf(),
            max_total_bytes: 100,
            max_per_object_bytes: 50,
        };
        let index = CacheIndex::open_with_config(config).unwrap();

        // Insert 5 objects of 30 bytes each (150 total without eviction).
        // Auto-eviction in put_cached should keep total ≤ max.
        for i in 0..5 {
            let data = Bytes::from(vec![0u8; 30]);
            index
                .put_cached(&format!("s3://b/obj{i}"), &data, None)
                .await
                .unwrap();
        }

        // Auto-eviction keeps total_bytes ≤ max_total_bytes.
        assert!(index.total_bytes() <= 100);
        // At least some entries were evicted (150 > 100).
        assert!(index.entry_count() < 5);
    }

    #[test]
    fn persist_and_reload() {
        let dir = tempfile::tempdir().unwrap();
        let index = CacheIndex::open(dir.path()).unwrap();

        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            index
                .put_cached("s3://b/persist", &Bytes::from("data"), None)
                .await
                .unwrap();
        });

        index.persist().unwrap();
        drop(index);

        // Reopen and verify entry persisted.
        let index2 = CacheIndex::open(dir.path()).unwrap();
        assert_eq!(index2.entry_count(), 1);
    }

    #[test]
    fn put_does_not_persist_inline_until_flushed() {
        let dir = tempfile::tempdir().unwrap();
        let index_path = dir.path().join("index.json");
        let index = CacheIndex::open(dir.path()).unwrap();

        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            index
                .put_cached("s3://b/lazy", &Bytes::from("data"), None)
                .await
                .unwrap();
        });

        // The mutation marked the index dirty but did not rewrite it inline.
        assert!(
            !index_path.exists(),
            "put_cached must not persist the index inline on the async runtime"
        );

        // The periodic persister writes exactly once, then no-ops until the
        // next change.
        assert!(index.persist_if_dirty().unwrap(), "first flush writes the dirty index");
        assert!(index_path.exists());
        assert!(
            !index.persist_if_dirty().unwrap(),
            "a clean index must not be rewritten"
        );
    }
}
