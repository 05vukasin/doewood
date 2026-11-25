//! The store-backed caches ([`StoreCache`], [`StoreIconCache`]) over the standalone SQLite
//! backend: two clients sharing one store reuse each other's answers, expired entries are not
//! read, and the cache works in memory while there is no store. The shared Postgres backend
//! passes the same store checks (`doewood-model`'s conformance suite).

mod support;

use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use doewood_core::MasterKey;
use doewood_model::CacheStore;
use doewood_registry::favicon::{CACHE_PREFIX, StoreIconCache};
use doewood_registry::gleif::{Gleif, GleifConfig};
use doewood_registry::{
    Cache, CacheEntry, Cached, HttpTransport, ManualClock, StoreCache, StoreSource,
};
use doewood_store::{Standalone, Store};
use serde_json::json;
use support::{MockServer, Reply, lei, record};
use time::OffsetDateTime;

fn store() -> Arc<Standalone> {
    let store = Store::open_in_memory(&MasterKey::from_bytes([3; 32])).unwrap();
    Arc::new(Standalone::new(Arc::new(store)))
}

fn gleif(server: &MockServer, cache: Arc<dyn Cache>) -> Gleif {
    let config = GleifConfig {
        base_url: server.base.clone(),
        ..GleifConfig::default()
    };
    // The store compares expiry with the real time, so the client's clock starts now.
    let clock = Arc::new(ManualClock::new(OffsetDateTime::now_utc()));
    let transport = HttpTransport::new(config.timeout, config.connect_timeout).unwrap();
    Gleif::with_parts(&config, Arc::new(transport), cache, clock).unwrap()
}

#[tokio::test]
async fn a_second_instance_reuses_the_answer() {
    let target = lei("SHARED");
    let body = json!({"data": record(&target, "Fictive Shared Ltd", "GB")});
    let server = MockServer::start(move |_, _| Reply::json(&body)).await;
    let store = store();
    let one = gleif(&server, Arc::new(StoreCache::fixed(store.clone())));
    let two = gleif(&server, Arc::new(StoreCache::fixed(store.clone())));
    let first = one.lookup(&target).await.unwrap();
    let second = two.lookup(&target).await.unwrap();
    assert_eq!(server.requests().len(), 1, "{:?}", server.requests());
    assert_eq!(second.value, first.value);
    // Kept to the second.
    assert_eq!(
        second.fetched_at.unix_timestamp(),
        first.fetched_at.unix_timestamp()
    );
    // Stored under the registry and the request, expiring after GLEIF's 30 days.
    let row = store
        .cache_get("gleif", &format!("lei-records/{target}"))
        .await
        .unwrap()
        .expect("cached");
    assert_eq!(row.expires_at - row.fetched_at, 30 * 86_400);
    assert!(row.body.is_some());
}

#[tokio::test]
async fn expired_entries_are_not_read() {
    let cache = StoreCache::fixed(store());
    let now = OffsetDateTime::now_utc();
    let old = CacheEntry::new(
        Cached::Found(b"x".to_vec()),
        now - Duration::from_hours(2),
        Duration::from_hours(1),
    );
    cache.put("vies:check/X", old).await;
    assert_eq!(cache.get("vies:check/X").await, None);
    let fresh = CacheEntry::new(Cached::NotFound, now, Duration::from_hours(1));
    cache.put("vies:check/X", fresh).await;
    let got = cache.get("vies:check/X").await.expect("fresh");
    assert_eq!(got.value, Cached::NotFound);
    assert_eq!(got.expires_at.unix_timestamp(), now.unix_timestamp() + 3600);
}

#[tokio::test]
async fn without_a_store_the_cache_is_in_memory() {
    let slot: Arc<Mutex<Option<Arc<dyn CacheStore>>>> = Arc::new(Mutex::new(None));
    let source: StoreSource = {
        let slot = slot.clone();
        Arc::new(move || slot.lock().unwrap_or_else(PoisonError::into_inner).clone())
    };
    let cache = StoreCache::new(source);
    let entry = CacheEntry::new(
        Cached::Found(b"a".to_vec()),
        OffsetDateTime::now_utc(),
        Duration::from_hours(1),
    );
    cache.put("gleif:k", entry.clone()).await;
    assert_eq!(cache.get("gleif:k").await, Some(entry));
    // Once the store is there, it is used (and it does not have the memory's entry).
    let backend = store();
    *slot.lock().unwrap() = Some(backend.clone() as Arc<dyn CacheStore>);
    assert_eq!(cache.get("gleif:k").await, None);
    // A store failure (here an invalid registry name) is a miss and is not cached.
    let bad = CacheEntry::new(
        Cached::NotFound,
        OffsetDateTime::now_utc(),
        Duration::from_hours(1),
    );
    cache.put("GLEIF:k", bad).await;
    assert_eq!(cache.get("GLEIF:k").await, None);
}

#[tokio::test]
async fn icons_are_stored_with_their_format() {
    let backend = store();
    let cache = StoreIconCache::fixed(backend.clone());
    let now = OffsetDateTime::now_utc();
    let png = b"\x89PNG\r\n\x1a\nrest".to_vec();
    let key = format!("{CACHE_PREFIX}acme.example");
    cache
        .put(
            &key,
            CacheEntry::new(Cached::Found(png.clone()), now, Duration::from_hours(720)),
        )
        .await;
    let row = backend
        .icon_get("acme.example")
        .await
        .unwrap()
        .expect("row");
    let icon = row.icon.expect("an icon");
    assert_eq!((icon.format.as_str(), &icon.bytes), ("png", &png));
    assert_eq!(row.expires_at - row.fetched_at, 720 * 3600);
    assert_eq!(
        cache.get(&key).await.map(|e| e.value),
        Some(Cached::Found(png))
    );
    // A miss.
    cache
        .put(
            &key,
            CacheEntry::new(Cached::NotFound, now, Duration::from_hours(6)),
        )
        .await;
    assert_eq!(
        cache.get(&key).await.map(|e| e.value),
        Some(Cached::NotFound)
    );
    // Bytes of no icon format are not stored.
    let other = format!("{CACHE_PREFIX}other.example");
    cache
        .put(
            &other,
            CacheEntry::new(
                Cached::Found(b"<svg/>".to_vec()),
                now,
                Duration::from_hours(1),
            ),
        )
        .await;
    assert_eq!(cache.get(&other).await, None);
    assert_eq!(backend.icon_get("other.example").await.unwrap(), None);
}
