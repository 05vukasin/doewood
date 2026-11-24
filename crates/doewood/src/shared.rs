//! The shared Postgres store's supervisor: connects in the background with backoff, keeps
//! `/readyz` honest while the database is away, and publishes the backend once it is up.
//!
//! The instance never fails to start because of Postgres: it runs degraded (not ready, with
//! the reason) and keeps retrying until it connects or shuts down.

use std::sync::Arc;
use std::time::{Duration, SystemTime};

use doewood_core::PostgresUrl;
use doewood_model::Backend;
use doewood_pg::PgStore;
use tokio::sync::watch;
use tracing::{info, warn};

use crate::ops::OpsState;

/// The backend the API serves from, once there is one.
pub type BackendSlot = watch::Sender<Option<Arc<dyn Backend>>>;

/// First retry delay; it doubles up to [`MAX_BACKOFF`].
const MIN_BACKOFF: Duration = Duration::from_secs(1);
const MAX_BACKOFF: Duration = Duration::from_mins(1);
/// How often a connected store is checked.
const HEALTH_INTERVAL: Duration = Duration::from_secs(15);
const PING_TIMEOUT: Duration = Duration::from_secs(10);

/// Exponential backoff with up to 25% jitter, so instances that lost the database together do
/// not come back in lockstep.
struct Backoff(Duration);

impl Backoff {
    fn new() -> Self {
        Self(MIN_BACKOFF)
    }

    fn next(&mut self) -> Duration {
        let base = self.0;
        self.0 = (self.0 * 2).min(MAX_BACKOFF);
        let nanos = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .map_or(0, |d| d.subsec_nanos());
        base + base.mul_f64(f64::from(nanos % 1000) / 4000.0)
    }

    fn reset(&mut self) {
        self.0 = MIN_BACKOFF;
    }
}

/// Waits `d`, or returns `true` early when shutdown starts.
async fn sleep_or_shutdown(d: Duration, shutdown: &mut watch::Receiver<bool>) -> bool {
    let stopped = tokio::select! {
        () = tokio::time::sleep(d) => false,
        _ = shutdown.wait_for(|stop| *stop) => true,
    };
    stopped || *shutdown.borrow()
}

/// Runs until shutdown. Connects (retrying with backoff), publishes the backend into `slot`,
/// then pings it and flips readiness when it goes away or comes back.
pub async fn supervise(
    url: PostgresUrl,
    ops: Arc<OpsState>,
    slot: BackendSlot,
    mut shutdown: watch::Receiver<bool>,
) {
    let mut backoff = Backoff::new();
    ops.set_not_ready("connecting to the shared Postgres");
    let store = loop {
        let attempt = tokio::select! {
            r = PgStore::connect(&url) => r,
            _ = shutdown.wait_for(|stop| *stop) => return,
        };
        match attempt {
            Ok(store) => break store,
            Err(e) => {
                let wait = backoff.next();
                warn!(
                    url = %url,
                    error = %e,
                    retry_in_s = wait.as_secs(),
                    "shared Postgres unavailable; retrying"
                );
                // Unreachable, or reachable but unusable (a bad URL, a newer schema).
                let what = if matches!(e, doewood_model::Error::Unavailable(_)) {
                    "unreachable"
                } else {
                    "unusable"
                };
                ops.set_not_ready(format!(
                    "shared Postgres {what}: {e} (retrying in {}s)",
                    wait.as_secs().max(1)
                ));
                if sleep_or_shutdown(wait, &mut shutdown).await {
                    return;
                }
            }
        }
    };
    if *shutdown.borrow() {
        return;
    }
    info!(url = %url, pgvector = store.has_vector(), "shared Postgres connected");
    let backend: Arc<dyn Backend> = Arc::new(store);
    slot.send_replace(Some(Arc::clone(&backend)));
    ops.set_ready();
    info!("ready");

    let mut healthy = true;
    backoff.reset();
    loop {
        let wait = if healthy {
            HEALTH_INTERVAL
        } else {
            backoff.next()
        };
        if sleep_or_shutdown(wait, &mut shutdown).await {
            return;
        }
        let result = tokio::time::timeout(PING_TIMEOUT, backend.ping())
            .await
            .unwrap_or_else(|_| {
                Err(doewood_model::Error::Unavailable(
                    "no answer within 10s".to_owned(),
                ))
            });
        if *shutdown.borrow() {
            return;
        }
        match (result, healthy) {
            (Ok(()), false) => {
                info!("shared Postgres reachable again");
                healthy = true;
                backoff.reset();
                ops.set_ready();
            }
            (Err(e), _) => {
                if healthy {
                    warn!(error = %e, "shared Postgres lost; not ready until it answers");
                }
                healthy = false;
                ops.set_not_ready(format!("shared Postgres unreachable: {e}"));
            }
            (Ok(()), true) => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_doubles_to_a_cap_with_bounded_jitter() {
        let mut b = Backoff::new();
        let mut last = Duration::ZERO;
        for _ in 0..12 {
            let d = b.next();
            assert!(d >= last.min(MAX_BACKOFF), "{d:?} < {last:?}");
            assert!(d <= MAX_BACKOFF.mul_f64(1.25), "{d:?}");
            last = d.min(MAX_BACKOFF);
        }
        assert!(b.next() >= MAX_BACKOFF);
        b.reset();
        assert!(b.next() < Duration::from_secs(2));
    }
}
