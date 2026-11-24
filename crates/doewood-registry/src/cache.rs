//! The response cache, with fetch dates (ADR 0002 section 2).
//!
//! A cache stores the capped raw body of a successful answer, or the fact that the registry
//! had no such record, under a key such as `gleif:lei-records/<LEI>`, with the time it was
//! fetched. Failures are never cached. Raw bodies (not parsed values) are kept so that a better
//! parser applies to cached answers too.
//!
//! [`MemoryCache`] is the in-process implementation. A Postgres implementation, shared by
//! every instance (ADR 0002 section 6), needs a new table in the shared schema and comes in a
//! later step.

use std::collections::HashMap;
use std::fmt;
use std::sync::{Mutex, PoisonError};

use async_trait::async_trait;
use time::OffsetDateTime;

/// A cached answer.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Cached {
    /// A successful answer: the raw body (already within the cap).
    Found(Vec<u8>),
    /// The registry has no such record (404).
    NotFound,
}

/// A cached answer with its fetch date.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CacheEntry {
    /// The answer.
    pub value: Cached,
    /// When it was fetched (UTC).
    pub fetched_at: OffsetDateTime,
}

/// A response cache. A backend failure is a miss on `get` and is dropped on `put`: the cache
/// never fails a lookup.
#[async_trait]
pub trait Cache: Send + Sync + fmt::Debug {
    /// The entry under `key`, fresh or not (the caller checks the age).
    async fn get(&self, key: &str) -> Option<CacheEntry>;
    /// Stores `entry` under `key`, replacing any older one.
    async fn put(&self, key: &str, entry: CacheEntry);
}

/// No cache: every lookup goes to the registry.
#[derive(Debug, Default)]
pub struct NoCache;

#[async_trait]
impl Cache for NoCache {
    async fn get(&self, _key: &str) -> Option<CacheEntry> {
        None
    }

    async fn put(&self, _key: &str, _entry: CacheEntry) {}
}

/// An in-process cache of at most `capacity` entries; when full, the oldest fetch is evicted.
#[derive(Debug)]
pub struct MemoryCache {
    capacity: usize,
    map: Mutex<HashMap<String, CacheEntry>>,
}

impl MemoryCache {
    /// The default capacity.
    pub const DEFAULT_CAPACITY: usize = 10_000;

    /// A cache of at most `capacity` entries (at least 1).
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity: capacity.max(1),
            map: Mutex::new(HashMap::new()),
        }
    }

    /// How many entries it holds.
    pub fn len(&self) -> usize {
        self.map
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .len()
    }

    /// Whether it is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl Default for MemoryCache {
    fn default() -> Self {
        Self::new(Self::DEFAULT_CAPACITY)
    }
}

#[async_trait]
impl Cache for MemoryCache {
    async fn get(&self, key: &str) -> Option<CacheEntry> {
        self.map
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(key)
            .cloned()
    }

    async fn put(&self, key: &str, entry: CacheEntry) {
        let mut map = self.map.lock().unwrap_or_else(PoisonError::into_inner);
        if !map.contains_key(key) && map.len() >= self.capacity {
            let oldest = map
                .iter()
                .min_by_key(|(_, e)| e.fetched_at)
                .map(|(k, _)| k.clone());
            if let Some(k) = oldest {
                map.remove(&k);
            }
        }
        map.insert(key.to_owned(), entry);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn at(secs: u64) -> CacheEntry {
        CacheEntry {
            value: Cached::NotFound,
            fetched_at: OffsetDateTime::UNIX_EPOCH + Duration::from_secs(secs),
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn evicts_the_oldest_fetch() {
        let c = MemoryCache::new(2);
        c.put("a", at(1)).await;
        c.put("b", at(2)).await;
        c.put("a", at(3)).await;
        assert_eq!(c.len(), 2);
        c.put("c", at(4)).await;
        assert_eq!(c.len(), 2);
        assert!(c.get("b").await.is_none());
        assert_eq!(c.get("a").await, Some(at(3)));
        assert_eq!(c.get("c").await, Some(at(4)));
        assert!(NoCache.get("a").await.is_none());
    }
}
