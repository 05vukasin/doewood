//! The response cache, with fetch dates (ADR 0002 section 2).
//!
//! A cache stores the capped raw body of a successful answer, or the fact that the registry
//! had no such record, under a key such as `gleif:lei-records/<LEI>`, with the time it was
//! fetched. Failures are never cached. Raw bodies (not parsed values) are kept so that a better
//! parser applies to cached answers too.
//!
//! - [`MemoryCache`]: in-process, bounded.
//! - [`StoreCache`]: the store's `registry_cache` table through `doewood-model`'s
//!   [`CacheStore`] trait: the shared Postgres in shared mode, so every instance reuses an
//!   answer (ADR 0002 sections 2 and 6), or the instance's SQLite file standalone. This crate
//!   stays free of any database: the backends implement the trait.
//!
//! Every entry carries an expiry (the fetch date plus the client's freshness for that kind of
//! answer). The store never returns an expired entry and purges them on a schedule; callers
//! still check the age themselves, as they do with [`MemoryCache`].

use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use async_trait::async_trait;
use doewood_model::{CacheRow, CacheStore, MAX_TIME};
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
    /// When it stops being fresh; a store drops it from then on.
    pub expires_at: OffsetDateTime,
}

impl CacheEntry {
    /// An entry fetched at `fetched_at` and fresh for `ttl`.
    pub fn new(value: Cached, fetched_at: OffsetDateTime, ttl: Duration) -> Self {
        let ttl = time::Duration::try_from(ttl).unwrap_or(time::Duration::MAX);
        Self {
            value,
            fetched_at,
            expires_at: fetched_at.saturating_add(ttl),
        }
    }
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

/// Where a store-backed cache finds its store, asked on every lookup: `None` while there is
/// none yet (the shared Postgres not connected). The cache then works in memory.
pub type StoreSource = Arc<dyn Fn() -> Option<Arc<dyn CacheStore>> + Send + Sync>;

/// Unix seconds, within what a store accepts.
pub(crate) fn unix(t: OffsetDateTime) -> i64 {
    t.unix_timestamp().clamp(0, MAX_TIME)
}

pub(crate) fn from_unix(t: i64) -> Option<OffsetDateTime> {
    OffsetDateTime::from_unix_timestamp(t).ok()
}

/// The registry cache in the store (`registry_cache`). A key `<registry>:<request>` (as the
/// clients build them) is stored under that registry and request; a key without a registry
/// stays in memory. A store failure is a miss on `get` and is dropped on `put`, logged at
/// debug level. Times are kept to the second.
pub struct StoreCache {
    source: StoreSource,
    memory: MemoryCache,
}

impl fmt::Debug for StoreCache {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StoreCache").finish_non_exhaustive()
    }
}

impl StoreCache {
    /// A cache in the store `source` gives, in memory while it gives none.
    pub fn new(source: StoreSource) -> Self {
        Self {
            source,
            memory: MemoryCache::default(),
        }
    }

    /// A cache in `store`.
    pub fn fixed(store: Arc<dyn CacheStore>) -> Self {
        Self::new(Arc::new(move || Some(Arc::clone(&store))))
    }
}

#[async_trait]
impl Cache for StoreCache {
    async fn get(&self, key: &str) -> Option<CacheEntry> {
        let (Some((registry, request)), Some(store)) = (key.split_once(':'), (self.source)())
        else {
            return self.memory.get(key).await;
        };
        let row = match store.cache_get(registry, request).await {
            Ok(row) => row?,
            Err(e) => {
                tracing::debug!(key, error = %e, "registry cache read failed; a miss");
                return None;
            }
        };
        Some(CacheEntry {
            value: row.body.map_or(Cached::NotFound, Cached::Found),
            fetched_at: from_unix(row.fetched_at)?,
            expires_at: from_unix(row.expires_at)?,
        })
    }

    async fn put(&self, key: &str, entry: CacheEntry) {
        let (Some((registry, request)), Some(store)) = (key.split_once(':'), (self.source)())
        else {
            return self.memory.put(key, entry).await;
        };
        let row = CacheRow {
            registry: registry.to_owned(),
            key: request.to_owned(),
            body: match entry.value {
                Cached::Found(b) => Some(b),
                Cached::NotFound => None,
            },
            fetched_at: unix(entry.fetched_at),
            expires_at: unix(entry.expires_at),
        };
        if let Err(e) = store.cache_put(&row).await {
            tracing::debug!(key, error = %e, "registry cache write failed; not cached");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at(secs: u64) -> CacheEntry {
        CacheEntry::new(
            Cached::NotFound,
            OffsetDateTime::UNIX_EPOCH + Duration::from_secs(secs),
            Duration::from_mins(1),
        )
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
