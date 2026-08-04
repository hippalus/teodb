use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::net::IpAddr;
use std::sync::{Arc, Mutex, Weak};
use std::time::{Duration, Instant};

use sha2::{Digest, Sha256};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::config::ApiConfig;
use crate::security::ClientIdentityResolver;

const OVERFLOW_STRIPES: usize = 64;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RateScope {
    Public,
    Read,
    Write,
}

struct WindowState {
    started: Instant,
    count: u32,
}

struct RateRegistry {
    windows: Mutex<HashMap<String, WindowState>>,
    overflow: Vec<Mutex<WindowState>>,
    max_keys: usize,
    window: Duration,
}

impl RateRegistry {
    fn new(max_keys: u64, window: Duration) -> Self {
        let now = Instant::now();
        Self {
            windows: Mutex::new(HashMap::new()),
            overflow: (0..OVERFLOW_STRIPES)
                .map(|_| Mutex::new(WindowState { started: now, count: 0 }))
                .collect(),
            max_keys: usize::try_from(max_keys).unwrap_or(usize::MAX),
            window,
        }
    }

    fn check(&self, key: &str, limit: u32, now: Instant) -> Result<(), Duration> {
        let mut windows = self
            .windows
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        windows.retain(|_, state| now.duration_since(state.started) < self.window);
        if let Some(state) = windows.get_mut(key) {
            return check_window(state, limit, self.window, now);
        }
        if windows.len() < self.max_keys {
            windows.insert(key.to_owned(), WindowState { started: now, count: 1 });
            return Ok(());
        }
        drop(windows);

        let stripe = stable_index(key, self.overflow.len());
        let mut state = self.overflow[stripe]
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        check_window(&mut state, limit, self.window, now)
    }

    #[cfg(test)]
    fn tracked_len(&self) -> usize {
        self.windows
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .len()
    }
}

fn check_window(state: &mut WindowState, limit: u32, window: Duration, now: Instant) -> Result<(), Duration> {
    if now.duration_since(state.started) >= window {
        state.started = now;
        state.count = 0;
    }
    if state.count >= limit {
        return Err(window.saturating_sub(now.duration_since(state.started)));
    }
    state.count = state.count.saturating_add(1);
    Ok(())
}

struct PrincipalRegistry {
    entries: Mutex<HashMap<String, Weak<Semaphore>>>,
    overflow: Vec<Arc<Semaphore>>,
    max_entries: usize,
    permits: usize,
}

impl PrincipalRegistry {
    fn new(max_entries: u64, permits: usize) -> Self {
        Self {
            entries: Mutex::new(HashMap::new()),
            overflow: (0..OVERFLOW_STRIPES)
                .map(|_| Arc::new(Semaphore::new(permits)))
                .collect(),
            max_entries: usize::try_from(max_entries).unwrap_or(usize::MAX),
            permits,
        }
    }

    fn acquire(&self, key: &str) -> Option<OwnedSemaphorePermit> {
        let semaphore = {
            let mut entries = self
                .entries
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if let Some(existing) = entries.get(key).and_then(Weak::upgrade) {
                existing
            } else {
                entries.remove(key);
                entries.retain(|_, semaphore| semaphore.strong_count() > 0);
                if entries.len() < self.max_entries {
                    let semaphore = Arc::new(Semaphore::new(self.permits));
                    entries.insert(key.to_owned(), Arc::downgrade(&semaphore));
                    semaphore
                } else {
                    drop(entries);
                    self.overflow[stable_index(key, self.overflow.len())].clone()
                }
            }
        };
        semaphore.try_acquire_owned().ok()
    }
}

pub struct ApiAdmission {
    rates: RateRegistry,
    principals: PrincipalRegistry,
    resolver: ClientIdentityResolver,
    read_limit: u32,
    write_limit: u32,
    public_limit: u32,
}

impl ApiAdmission {
    pub fn new(config: &ApiConfig) -> Self {
        Self {
            rates: RateRegistry::new(config.max_rate_limit_keys, config.rate_limit_window),
            principals: PrincipalRegistry::new(
                config.max_rate_limit_keys,
                config.max_concurrent_operations_per_principal,
            ),
            resolver: ClientIdentityResolver::new(config.trusted_proxy_cidrs.clone()),
            read_limit: config.read_requests_per_window,
            write_limit: config.write_requests_per_window,
            public_limit: config.public_requests_per_window,
        }
    }

    pub fn client_ip(&self, peer: IpAddr, headers: &axum::http::HeaderMap) -> IpAddr {
        self.resolver.resolve(peer, headers)
    }

    pub fn check_rate(&self, scope: RateScope, key: &str) -> Result<(), Duration> {
        let limit = match scope {
            RateScope::Public => self.public_limit,
            RateScope::Read => self.read_limit,
            RateScope::Write => self.write_limit,
        };
        self.rates.check(key, limit, Instant::now())
    }

    pub fn acquire_principal(&self, subject: &str) -> Option<OwnedSemaphorePermit> {
        self.principals.acquire(&principal_key(subject))
    }
}

pub fn principal_key(subject: &str) -> String {
    let digest = Sha256::digest(subject.as_bytes());
    hex::encode(digest)
}

fn stable_index(key: &str, len: usize) -> usize {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    key.hash(&mut hasher);
    hasher.finish() as usize % len
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn full_rate_registry_uses_bounded_overflow() {
        let registry = RateRegistry::new(2, Duration::from_secs(60));
        let now = Instant::now();
        assert!(registry.check("one", 10, now).is_ok());
        assert!(registry.check("two", 10, now).is_ok());
        for index in 0..1_000 {
            let _ = registry.check(&format!("overflow-{index}"), 10, now);
        }
        assert_eq!(registry.tracked_len(), 2);
    }

    #[test]
    fn one_principal_does_not_block_another() {
        let registry = PrincipalRegistry::new(10, 1);
        let first = registry.acquire("one").unwrap();
        assert!(registry.acquire("one").is_none());
        assert!(registry.acquire("two").is_some());
        drop(first);
        assert!(registry.acquire("one").is_some());
    }
}
