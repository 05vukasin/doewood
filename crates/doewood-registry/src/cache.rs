//! The response cache, with fetch dates (ADR 0002 section 2).
//!
//! A cache stores the capped raw body of a successful answer, or the fact that the registry
//! had no such record, under a key such as `gleif:lei-records/<LEI>`, with the time it was
//! fetched. Failures are never cached. Raw bodies (not parsed values) are kept so that a better
//! parser applies to cached answers too.
//!
//! - [`MemoryCache`]: in-process, bounded in entries and bytes.
//! - [`StoreCache`]: the store's `registry_cache` table through `doewood-model`'s
//!   [`CacheStore`] trait: the shared Postgres in shared mode, so every instance reuses an
//!   answer (ADR 0002 sections 2 and 6), or the instance's SQLite file standalone. This crate
//!   stays free of any database: the backends implement the trait.
//!
//! Every entry carries an expiry (the fetch date plus the client's freshness for that kind of
//! answer). The store never returns an expired entry and purges them on a schedule; callers
//! still check the age themselves, as they do with [`MemoryCache`].

use std::collections::{BTreeMap, HashMap};
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

/// An in-process cache of at most `capacity` entries and `max_bytes` bytes (keys and bodies,
/// plus [`MemoryCache::ENTRY_OVERHEAD`] per entry); when full, the oldest fetch is evicted
/// first. An entry bigger than the whole budget is not kept.
///
/// Expired entries are dropped on lookup and on insert. The cache has no clock of its own: an
/// entry counts as expired once it expires before the newest fetch date put so far, which is
/// the callers' clock (they put what they just fetched).
#[derive(Debug)]
pub struct MemoryCache {
    capacity: usize,
    max_bytes: usize,
    inner: Mutex<Inner>,
}

/// The entries, with two ordered indexes for eviction (oldest fetch) and expiry. Each key is
/// `(time, seq)`, `seq` unique per insert, so equal times do not collide.
#[derive(Debug, Default)]
struct Inner {
    map: HashMap<String, Slot>,
    by_fetch: BTreeMap<(OffsetDateTime, u64), String>,
    by_expiry: BTreeMap<(OffsetDateTime, u64), String>,
    bytes: usize,
    seq: u64,
    /// The newest fetch date put: entries that expired before it are dropped.
    now: Option<OffsetDateTime>,
}

#[derive(Debug)]
struct Slot {
    entry: CacheEntry,
    seq: u64,
    bytes: usize,
}

impl Inner {
    fn remove(&mut self, key: &str) -> Option<Slot> {
        let slot = self.map.remove(key)?;
        self.by_fetch.remove(&(slot.entry.fetched_at, slot.seq));
        self.by_expiry.remove(&(slot.entry.expires_at, slot.seq));
        self.bytes -= slot.bytes;
        Some(slot)
    }

    /// Drops every entry that expired before [`Inner::now`] (as callers do: an age equal to
    /// the freshness is still fresh).
    fn purge_expired(&mut self) {
        let Some(now) = self.now else { return };
        while let Some(((expires_at, _), key)) = self.by_expiry.first_key_value() {
            if *expires_at >= now {
                break;
            }
            let key = key.clone();
            self.remove(&key);
        }
    }

    fn evict_oldest(&mut self) {
        if let Some((_, key)) = self.by_fetch.first_key_value() {
            let key = key.clone();
            self.remove(&key);
        }
    }

    fn expired(&self, entry: &CacheEntry) -> bool {
        self.now.is_some_and(|now| entry.expires_at < now)
    }
}

impl MemoryCache {
    /// The default capacity.
    pub const DEFAULT_CAPACITY: usize = 10_000;
    /// The default byte budget: 16 MiB. The binary has two such caches while the shared store
    /// is not connected (registry answers and icons), 32 MiB together.
    pub const DEFAULT_MAX_BYTES: usize = 16 << 20;
    /// What an entry counts on top of its key and body: the map slot, both index entries and
    /// the key's second copy, roughly.
    pub const ENTRY_OVERHEAD: usize = 160;

    /// A cache of at most `capacity` entries (at least 1) and the default byte budget.
    pub fn new(capacity: usize) -> Self {
        Self::with_budget(capacity, Self::DEFAULT_MAX_BYTES)
    }

    /// A cache of at most `capacity` entries (at least 1) and `max_bytes` bytes.
    pub fn with_budget(capacity: usize, max_bytes: usize) -> Self {
        Self {
            capacity: capacity.max(1),
            max_bytes,
            inner: Mutex::new(Inner::default()),
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Inner> {
        self.inner.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// How many entries it holds.
    pub fn len(&self) -> usize {
        self.lock().map.len()
    }

    /// Whether it is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// The bytes its entries count against the budget.
    pub fn bytes(&self) -> usize {
        self.lock().bytes
    }

    /// What `entry` under `key` counts against the budget.
    fn size(key: &str, entry: &CacheEntry) -> usize {
        let body = match &entry.value {
            Cached::Found(b) => b.len(),
            Cached::NotFound => 0,
        };
        // The key is held three times: the map and both indexes.
        Self::ENTRY_OVERHEAD + 3 * key.len() + body
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
        let mut inner = self.lock();
        let slot = inner.map.get(key)?;
        if inner.expired(&slot.entry) {
            inner.remove(key);
            return None;
        }
        Some(slot.entry.clone())
    }

    async fn put(&self, key: &str, entry: CacheEntry) {
        let mut inner = self.lock();
        inner.remove(key);
        inner.now = Some(
            inner
                .now
                .map_or(entry.fetched_at, |n| n.max(entry.fetched_at)),
        );
        inner.purge_expired();
        let bytes = Self::size(key, &entry);
        if bytes > self.max_bytes || inner.expired(&entry) {
            return;
        }
        while !inner.map.is_empty()
            && (inner.map.len() >= self.capacity || inner.bytes + bytes > self.max_bytes)
        {
            inner.evict_oldest();
        }
        inner.seq += 1;
        let seq = inner.seq;
        inner
            .by_fetch
            .insert((entry.fetched_at, seq), key.to_owned());
        inner
            .by_expiry
            .insert((entry.expires_at, seq), key.to_owned());
        inner.bytes += bytes;
        inner.map.insert(key.to_owned(), Slot { entry, seq, bytes });
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

    fn icon(secs: u64, len: usize) -> CacheEntry {
        CacheEntry::new(
            Cached::Found(vec![7; len]),
            OffsetDateTime::UNIX_EPOCH + Duration::from_secs(secs),
            Duration::from_hours(720),
        )
    }

    /// A flood of distinct icon domains (as `GET /icons/{domain}` could bring while the shared
    /// store is not connected) stays within the byte budget, keeping the newest.
    #[tokio::test(flavor = "current_thread")]
    async fn a_flood_stays_within_the_byte_budget() {
        let budget = 2 << 20;
        let c = MemoryCache::with_budget(MemoryCache::DEFAULT_CAPACITY, budget);
        for i in 0..5_000u64 {
            c.put(&format!("favicon:site-{i}.example"), icon(i, 200 << 10))
                .await;
            assert!(c.bytes() <= budget, "{} bytes after {i}", c.bytes());
        }
        // About ten 200 KiB icons fit; the newest are kept, the oldest went first.
        assert!((8..=10).contains(&c.len()), "{} entries", c.len());
        assert!(c.get("favicon:site-4999.example").await.is_some());
        assert!(c.get("favicon:site-0.example").await.is_none());
        // Many small answers are bounded by the entry cap as before.
        let c = MemoryCache::with_budget(100, budget);
        for i in 0..1_000u64 {
            c.put(&format!("k{i}"), icon(i, 0)).await;
        }
        assert_eq!(c.len(), 100);
        // A body bigger than the whole budget is not kept, and evicts nothing.
        c.put("big", icon(1_000, budget)).await;
        assert!(c.get("big").await.is_none());
        assert_eq!(c.len(), 100);
        // Replacing an entry counts its new size only.
        let c = MemoryCache::with_budget(10, budget);
        c.put("a", icon(1, 1000)).await;
        c.put("a", icon(2, 10)).await;
        assert_eq!(c.bytes(), MemoryCache::size("a", &icon(2, 10)));
    }

    /// Entries that expired before the newest fetch date are dropped on insert and lookup.
    #[tokio::test(flavor = "current_thread")]
    async fn drops_expired_entries() {
        let c = MemoryCache::new(10);
        // Fresh for one minute.
        c.put("a", at(0)).await;
        c.put("b", at(30)).await;
        // An age of exactly the freshness is still fresh.
        c.put("c", at(60)).await;
        assert_eq!(c.len(), 3);
        assert_eq!(c.get("a").await, Some(at(0)));
        // At 61 s "a" has expired: dropped on insert.
        c.put("d", at(61)).await;
        assert!(c.get("a").await.is_none());
        assert_eq!(c.len(), 3);
        // At 100 s "b" has expired; it goes on lookup or the next insert, whichever is first.
        c.put("e", at(100)).await;
        assert!(c.get("b").await.is_none());
        assert_eq!(c.len(), 3);
        assert_eq!(c.bytes(), 3 * MemoryCache::size("c", &at(0)));
        // An entry put already expired (an old fetch) is not kept.
        c.put("old", at(1)).await;
        assert!(c.get("old").await.is_none());
    }
}
