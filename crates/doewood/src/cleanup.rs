//! The cache purge (ADR 0002 sections 2 and 5): the backend never returns an expired registry
//! answer or icon, and this task deletes them, a minute after the start and then every hour.
//! With a shared store every instance runs it; the delete is idempotent. The same task
//! refreshes the instance store's query planner statistics (`PRAGMA optimize`).

use std::sync::Arc;
use std::time::Duration;

use doewood_model::Backend;
use doewood_store::Store;
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
    let t = std::time::Instant::now();
    match tokio::task::spawn_blocking(move || store.optimize_all()).await {
        Ok(Ok(())) => info!(
            took_ms = u64::try_from(t.elapsed().as_millis()).unwrap_or(u64::MAX),
            "instance store statistics refreshed"
        ),
        Ok(Err(e)) => warn!(error = %e, "PRAGMA optimize failed; retried at the next interval"),
        Err(e) => warn!(error = %e, "PRAGMA optimize did not finish"),
    }
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
}
