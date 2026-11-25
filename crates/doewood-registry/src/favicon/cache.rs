//! The favicon cache in the store's `favicons` table (ADR 0002 section 5.5): the icon's bytes
//! and format, or a miss, with the lookup date and expiry. In shared mode it is the shared
//! Postgres, so an icon is fetched once for every instance.

use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use doewood_model::{CacheStore, IconRow, StoredIcon};

use super::content;
use crate::cache::{StoreSource, from_unix, unix};
use crate::{Cache, CacheEntry, Cached, MemoryCache};

/// The prefix of the resolver's cache keys: `favicon:<domain>`.
pub const CACHE_PREFIX: &str = "favicon:";

/// The [`super::FaviconResolver`]'s cache in the store. Keys other than `favicon:<domain>`,
/// and every key while the source gives no store, stay in memory. Bytes that are not an icon
/// format are not stored. A store failure is a miss on `get` and is dropped on `put`.
pub struct StoreIconCache {
    source: StoreSource,
    memory: MemoryCache,
}

impl fmt::Debug for StoreIconCache {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StoreIconCache").finish_non_exhaustive()
    }
}

impl StoreIconCache {
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
impl Cache for StoreIconCache {
    async fn get(&self, key: &str) -> Option<CacheEntry> {
        let (Some(domain), Some(store)) = (key.strip_prefix(CACHE_PREFIX), (self.source)()) else {
            return self.memory.get(key).await;
        };
        let row = match store.icon_get(domain).await {
            Ok(row) => row?,
            Err(e) => {
                tracing::debug!(domain, error = %e, "favicon cache read failed; a miss");
                return None;
            }
        };
        Some(CacheEntry {
            value: row
                .icon
                .map_or(Cached::NotFound, |i| Cached::Found(i.bytes)),
            fetched_at: from_unix(row.fetched_at)?,
            expires_at: from_unix(row.expires_at)?,
        })
    }

    async fn put(&self, key: &str, entry: CacheEntry) {
        let (Some(domain), Some(store)) = (key.strip_prefix(CACHE_PREFIX), (self.source)()) else {
            return self.memory.put(key, entry).await;
        };
        let icon = match entry.value {
            Cached::Found(bytes) => {
                let Some(format) = content::sniff(&bytes) else {
                    tracing::debug!(domain, "favicon cache: not an icon format; not cached");
                    return;
                };
                Some(StoredIcon {
                    format: format.as_str().to_owned(),
                    bytes,
                })
            }
            Cached::NotFound => None,
        };
        let row = IconRow {
            domain: domain.to_owned(),
            icon,
            fetched_at: unix(entry.fetched_at),
            expires_at: unix(entry.expires_at),
        };
        if let Err(e) = store.icon_put(&row).await {
            tracing::debug!(domain, error = %e, "favicon cache write failed; not cached");
        }
    }
}
