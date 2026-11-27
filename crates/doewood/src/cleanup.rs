//! The cache purge (ADR 0002 sections 2 and 5): the backend never returns an expired registry
//! answer or icon, and this task deletes them, a minute after the start and then every hour.
//! With a shared store every instance runs it; the delete is idempotent. The same task
//! refreshes the instance store's query planner statistics (`PRAGMA optimize`). The instance
//! store's WAL is kept bounded by the store itself (its checkpoint thread, issue #151); its
//! metrics are registered here. A second task folds the company graph's change log into its
//! counts every few seconds (issue #172, [`start_fold`]).

use std::sync::{Arc, Weak};
use std::time::Duration;

use doewood_model::Backend;
use doewood_store::Store;
use prometheus::core::{Collector, Desc};
use prometheus::proto::MetricFamily;
use prometheus::{IntGauge, Registry};
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tracing::{debug, info, warn};

/// The first purge, after the start.
const FIRST: Duration = Duration::from_mins(1);
/// Between purges.
pub const INTERVAL: Duration = Duration::from_hours(1);

/// Purges the backend's expired cache rows once. Returns how many went.
pub async fn purge(backend: &dyn Backend) -> u64 {
    match backend.purge_expired().await {
        Ok(0) => {
            debug!("no expired cache rows");
            0
        }
        Ok(n) => {
            info!(rows = n, "expired cache rows purged");
            n
        }
        Err(e) => {
            warn!(error = %e, "cache purge failed; retried at the next interval");
            0
        }
    }
}

/// Refreshes the instance store's planner statistics where they are stale.
pub async fn optimize(store: &Arc<Store>) {
    let store = Arc::clone(store);
    match tokio::task::spawn_blocking(move || store.optimize()).await {
        Ok(Ok(())) => debug!("instance store optimized"),
        Ok(Err(e)) => warn!(error = %e, "PRAGMA optimize failed; retried at the next interval"),
        Err(e) => warn!(error = %e, "PRAGMA optimize did not finish"),
    }
}

/// Refreshes the planner statistics of every table that needs it, once after the start
/// ([`Store::optimize_all`]); off the start's critical path, as it can take seconds on a big
/// store (issue #118).
pub async fn optimize_all(store: &Arc<Store>) {
    let store = Arc::clone(store);
    let ms = |d: Duration| u64::try_from(d.as_millis()).unwrap_or(u64::MAX);
    match tokio::task::spawn_blocking(move || store.optimize_all()).await {
        Ok(Ok(done)) => info!(
            tables = done.tables,
            took_ms = ms(done.took),
            longest_writer_hold_ms = ms(done.longest_hold),
            slowest = done.slowest.as_deref(),
            "instance store statistics refreshed"
        ),
        Ok(Err(e)) => warn!(error = %e, "PRAGMA optimize failed; retried at the next interval"),
        Err(e) => warn!(error = %e, "PRAGMA optimize did not finish"),
    }
}

/// Between folds of the company graph's log.
pub const FOLD_INTERVAL: Duration = Duration::from_secs(5);
/// Most batches one fold pass takes before it waits for the next interval.
const FOLD_PASS_BATCHES: u32 = 50;

/// Folds the company graph's change log into its counts once (issue #172): batches until it
/// is empty, at most [`FOLD_PASS_BATCHES`]. Returns the rows folded.
pub async fn fold(backend: &dyn Backend) -> u64 {
    let mut rows = 0;
    for _ in 0..FOLD_PASS_BATCHES {
        match backend
            .fold_company_graph(doewood_model::explore::FOLD_BATCH)
            .await
        {
            Ok(f) => {
                rows += f.rows;
                if !f.more {
                    break;
                }
            }
            Err(e) => {
                warn!(error = %e, "company graph fold failed; retried at the next interval");
                break;
            }
        }
    }
    if rows > 0 {
        debug!(rows, "company graph log folded");
    }
    rows
}

/// Runs [`fold`] on whatever backend `backend` holds every [`FOLD_INTERVAL`], until
/// shutdown. With a shared store every instance runs it; folds take turns on a lock.
pub fn start_fold(
    backend: watch::Receiver<Option<Arc<dyn Backend>>>,
    mut shutdown: watch::Receiver<bool>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            tokio::select! {
                () = tokio::time::sleep(FOLD_INTERVAL) => {}
                _ = shutdown.wait_for(|stop| *stop) => return,
            }
            let current = backend.borrow().clone();
            if let Some(b) = current {
                fold(b.as_ref()).await;
            }
        }
    })
}

/// Runs [`optimize_all`] on the instance store once, then [`purge`] on whatever backend
/// `backend` holds and [`optimize`] on the instance store every [`INTERVAL`], until
/// shutdown.
pub fn start(
    store: Arc<Store>,
    backend: watch::Receiver<Option<Arc<dyn Backend>>>,
    mut shutdown: watch::Receiver<bool>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        optimize_all(&store).await;
        let mut wait = FIRST;
        loop {
            tokio::select! {
                () = tokio::time::sleep(wait) => {}
                _ = shutdown.wait_for(|stop| *stop) => return,
            }
            wait = INTERVAL;
            let current = backend.borrow().clone();
            if let Some(b) = current {
                purge(b.as_ref()).await;
            }
            optimize(&store).await;
        }
    })
}

/// Registers the instance store's WAL metrics on the ops port's registry (issue #151), read
/// when `/metrics` is scraped: `doewood_store_wal_bytes`, and the checkpoint thread's
/// `doewood_store_wal_truncations_total` and `doewood_store_wal_truncations_busy_total`. The
/// registry does not keep the store alive.
pub fn register_metrics(store: &Arc<Store>, registry: &Registry) -> prometheus::Result<()> {
    let read = |f: fn(&Store) -> u64| {
        let store = Arc::downgrade(store);
        move || Weak::upgrade(&store).map_or(0, |s| f(&s))
    };
    let bytes = StatGauge {
        gauge: IntGauge::new(
            "doewood_store_wal_bytes",
            "Size of the instance store's WAL file (doewood.sqlite-wal), in bytes.",
        )?,
        read: read(|s| s.wal_stats().bytes),
    };
    let truncated = crate::ingest::StatCounter::new(
        "doewood_store_wal_truncations_total",
        "TRUNCATE checkpoints that emptied the instance store's WAL (over 64 MiB, or idle).",
        read(|s| s.wal_stats().truncated),
    )?;
    let busy = crate::ingest::StatCounter::new(
        "doewood_store_wal_truncations_busy_total",
        "TRUNCATE checkpoints that gave up after their 50 ms wait for a read or write; retried \
         5 s later.",
        read(|s| s.wal_stats().busy),
    )?;
    registry.register(Box::new(bytes))?;
    registry.register(Box::new(truncated))?;
    registry.register(Box::new(busy))
}

/// A gauge that follows a value kept elsewhere, read when it is collected.
struct StatGauge<F> {
    gauge: IntGauge,
    read: F,
}

impl<F: Fn() -> u64 + Send + Sync> Collector for StatGauge<F> {
    fn desc(&self) -> Vec<&Desc> {
        self.gauge.desc()
    }

    fn collect(&self) -> Vec<MetricFamily> {
        self.gauge
            .set(i64::try_from((self.read)()).unwrap_or(i64::MAX));
        self.gauge.collect()
    }
}

#[cfg(test)]
mod tests {
    use doewood_core::MasterKey;
    use doewood_model::CacheRow;
    use doewood_store::{Standalone, Store};

    use super::*;

    #[tokio::test]
    async fn purges_expired_rows_and_stops_on_shutdown() {
        let store = Arc::new(Store::open_in_memory(&MasterKey::from_bytes([9; 32])).unwrap());
        let backend: Arc<dyn Backend> = Arc::new(Standalone::new(Arc::clone(&store)));
        let row = |key: &str, expires_at| CacheRow {
            registry: "gleif".to_owned(),
            key: key.to_owned(),
            body: None,
            fetched_at: 1,
            expires_at,
        };
        backend.cache_put(&row("old", 2)).await.unwrap();
        backend
            .cache_put(&row("new", doewood_model::MAX_TIME))
            .await
            .unwrap();
        assert_eq!(purge(backend.as_ref()).await, 1);
        assert_eq!(purge(backend.as_ref()).await, 0);
        assert!(backend.cache_get("gleif", "new").await.unwrap().is_some());

        let (_backend_tx, backend_rx) = watch::channel(Some(backend));
        let (stop_tx, stop_rx) = watch::channel(false);
        optimize(&store).await;
        optimize_all(&store).await;
        let task = start(store, backend_rx, stop_rx);
        stop_tx.send(true).unwrap();
        tokio::time::timeout(Duration::from_secs(5), task)
            .await
            .expect("stops")
            .unwrap();
    }

    #[test]
    fn the_wal_metrics_follow_the_store() {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(
            Store::open(
                &dir.path().join(doewood_store::STORE_FILE),
                &MasterKey::from_bytes([9; 32]),
            )
            .unwrap(),
        );
        let registry = Registry::new();
        register_metrics(&store, &registry).unwrap();
        // The value of the metric `name`, from the text format.
        let value = |name: &str| -> u64 {
            let mut text = Vec::new();
            prometheus::Encoder::encode(
                &prometheus::TextEncoder::new(),
                &registry.gather(),
                &mut text,
            )
            .unwrap();
            String::from_utf8(text)
                .unwrap()
                .lines()
                .find_map(|l| l.strip_prefix(name)?.strip_prefix(' ')?.parse().ok())
                .expect(name)
        };
        store
            .set_setting("x", &serde_json::json!("y".repeat(10_000)))
            .unwrap();
        assert!(value("doewood_store_wal_bytes") > 0);
        store.checkpoint_wal().unwrap();
        store.checkpoint_wal().unwrap();
        assert_eq!(value("doewood_store_wal_bytes"), 0);
        assert_eq!(value("doewood_store_wal_truncations_total"), 1);
        assert_eq!(value("doewood_store_wal_truncations_busy_total"), 0);
        drop(store);
        // The registry does not keep the store alive.
        assert_eq!(value("doewood_store_wal_truncations_total"), 1);
    }
}
