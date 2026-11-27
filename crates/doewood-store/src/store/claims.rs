//! The claim view: which of this instance's sources another instance has claimed in the shared
//! store (ADR 0001 section 2, issues #12 and #25).
//!
//! The claims themselves live in the shared store. This is the instance's copy of what it
//! last learned about them, kept in memory next to the store so that ingest, the scanners and
//! the event inlets can check a source without a database round trip:
//!
//! - the start-up claim pass, `ResumeSource` and `AddSource` write it: a source another
//!   instance owns is marked with that owner, one claimed for this instance is cleared;
//! - ingest drops the queued objects of a marked source instead of ingesting them, and the
//!   webhook inlet ignores its notifications (the owner scans the bucket itself);
//! - until the start-up claim pass has checked the claims ([`ClaimView::expect_check`],
//!   [`ClaimView::set_checked`]) ingest holds its work, so that nothing is ingested for a
//!   source that turns out to be owned elsewhere. Standalone it is checked from the start.
//!
//! A marked source is also in the `error` state with the reason `claimed by instance ...`
//! (see [`CLAIMED_BY`]); this view adds paused sources (which keep their state) and costs one
//! uncontended read lock, or one atomic load while no source is marked.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{PoisonError, RwLock};

use doewood_core::{InstanceId, SourceId};
use tokio::sync::watch;

/// How the `state_reason` of a source claimed by another instance starts.
pub const CLAIMED_BY: &str = "claimed by instance ";

/// See the module documentation. Every [`crate::Store`] has one ([`crate::Store::claims`]).
#[derive(Debug)]
pub struct ClaimView {
    checked: watch::Sender<bool>,
    elsewhere: RwLock<HashMap<SourceId, InstanceId>>,
    /// `elsewhere.len()`, read without the lock.
    marked: AtomicUsize,
    dropped_queued: AtomicU64,
    dropped_events: AtomicU64,
}

/// What was dropped because another instance owns the source, since start.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ClaimDrops {
    /// Objects of ingest's local queue (pending objects and their events) dropped.
    pub queued: u64,
    /// Event notification records (webhook, SQS) ignored.
    pub events: u64,
}

impl Default for ClaimView {
    fn default() -> Self {
        Self {
            checked: watch::Sender::new(true),
            elsewhere: RwLock::default(),
            marked: AtomicUsize::new(0),
            dropped_queued: AtomicU64::new(0),
            dropped_events: AtomicU64::new(0),
        }
    }
}

impl ClaimView {
    /// The claims are kept in a shared store and are not checked yet: ingest holds its work
    /// until [`set_checked`](Self::set_checked).
    pub fn expect_check(&self) {
        self.checked.send_replace(false);
    }

    /// The start-up claim pass has checked every source.
    pub fn set_checked(&self) {
        self.checked.send_replace(true);
    }

    /// Whether the claims are checked (always, standalone).
    pub fn is_checked(&self) -> bool {
        *self.checked.borrow()
    }

    /// Waits until the claims are checked.
    pub async fn checked(&self) {
        let mut rx = self.checked.subscribe();
        // The sender lives as long as `self`.
        let _ = rx.wait_for(|c| *c).await;
    }

    /// The other instance that owns `source`, if one does.
    pub fn owner_elsewhere(&self, source: SourceId) -> Option<InstanceId> {
        if self.marked.load(Ordering::Acquire) == 0 {
            return None;
        }
        self.elsewhere
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&source)
            .cloned()
    }

    /// Marks `source` as owned by the other instance `owner`.
    pub fn set_elsewhere(&self, source: SourceId, owner: InstanceId) {
        let mut map = self
            .elsewhere
            .write()
            .unwrap_or_else(PoisonError::into_inner);
        map.insert(source, owner);
        self.marked.store(map.len(), Ordering::Release);
    }

    /// `source` is claimed for this instance (or gone): clears its mark.
    pub fn set_mine(&self, source: SourceId) {
        if self.marked.load(Ordering::Acquire) == 0 {
            return;
        }
        let mut map = self
            .elsewhere
            .write()
            .unwrap_or_else(PoisonError::into_inner);
        map.remove(&source);
        self.marked.store(map.len(), Ordering::Release);
    }

    /// Counts `n` objects dropped from ingest's local queue.
    pub fn count_dropped_queued(&self, n: u64) {
        self.dropped_queued.fetch_add(n, Ordering::Relaxed);
    }

    /// Counts `n` event notification records ignored.
    pub fn count_dropped_events(&self, n: u64) {
        self.dropped_events.fetch_add(n, Ordering::Relaxed);
    }

    /// The counts since start.
    pub fn drops(&self) -> ClaimDrops {
        ClaimDrops {
            queued: self.dropped_queued.load(Ordering::Relaxed),
            events: self.dropped_events.load(Ordering::Relaxed),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn marks_and_the_check() {
        let v = ClaimView::default();
        assert!(v.is_checked());
        v.checked().await;
        let (a, b) = (SourceId::generate(), SourceId::generate());
        let owner = InstanceId::new("doewood-1").unwrap();
        assert_eq!(v.owner_elsewhere(a), None);
        v.set_mine(a);
        v.set_elsewhere(a, owner.clone());
        assert_eq!(v.owner_elsewhere(a), Some(owner.clone()));
        assert_eq!(v.owner_elsewhere(b), None);
        v.set_mine(a);
        assert_eq!(v.owner_elsewhere(a), None);
        assert_eq!(v.marked.load(Ordering::Relaxed), 0);

        v.expect_check();
        assert!(!v.is_checked());
        let wait = tokio::time::timeout(std::time::Duration::from_millis(20), v.checked());
        assert!(wait.await.is_err(), "holds until checked");
        v.set_checked();
        v.checked().await;

        v.count_dropped_queued(3);
        v.count_dropped_events(2);
        assert_eq!(
            v.drops(),
            ClaimDrops {
                queued: 3,
                events: 2
            }
        );
    }
}
