//! Progress of the sources, for `JobsService.WatchProgress`.
//!
//! The counters come from two places:
//! - [`Progress`] counts the discovery events of each source since this instance started
//!   (`new`, `changed`, `deleted`); the binary feeds it every event the scanners and the
//!   webhook send;
//! - the store holds what is durable: the objects seen and their ingest status
//!   (`processed`, `failed`, `pending`), the state and the scan checkpoint.
//!
//! A watch recomputes a snapshot when [`Progress`] reports a change (debounced) and at a
//! fixed interval (state changes the scanners write to the store), and sends what changed.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use doewood_core::SourceId;
use doewood_s3::{EventKind, ObjectEvent};
use doewood_store::{Source, Store, StoreError};
use tokio::sync::{mpsc, watch};
use tonic::Status;

use crate::convert;
use crate::pb::{self, ProgressCounters, ProgressUpdate, SourceProgress, progress_update::Kind};

/// Events of one source since the instance started.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct EventCounts {
    /// Keys not seen before.
    pub new: u64,
    /// Keys whose ETag or size changed.
    pub changed: u64,
    /// Keys that are gone.
    pub deleted: u64,
}

/// The live event counters, shared by the binary (which feeds them) and the API.
#[derive(Debug)]
pub struct Progress {
    counts: Mutex<HashMap<SourceId, EventCounts>>,
    version: watch::Sender<u64>,
}

impl Default for Progress {
    fn default() -> Self {
        Self::new()
    }
}

impl Progress {
    /// No counts yet.
    pub fn new() -> Self {
        Self {
            counts: Mutex::new(HashMap::new()),
            version: watch::Sender::new(0),
        }
    }

    fn counts(&self) -> MutexGuard<'_, HashMap<SourceId, EventCounts>> {
        self.counts.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Counts one discovery event.
    pub fn record(&self, event: &ObjectEvent) {
        {
            let mut counts = self.counts();
            let c = counts.entry(event.source).or_default();
            match event.kind {
                EventKind::New(_) => c.new += 1,
                EventKind::Changed(_) => c.changed += 1,
                EventKind::Deleted => c.deleted += 1,
            }
        }
        self.touch();
    }

    /// The counts of one source.
    pub fn get(&self, source: SourceId) -> EventCounts {
        self.counts().get(&source).copied().unwrap_or_default()
    }

    /// Drops a removed source's counts.
    pub fn forget(&self, source: SourceId) {
        self.counts().remove(&source);
        self.touch();
    }

    /// Wakes the watchers: something changed (a count, or a state the API set).
    pub fn touch(&self) {
        self.version.send_modify(|v| *v = v.wrapping_add(1));
    }

    fn subscribe(&self) -> watch::Receiver<u64> {
        self.version.subscribe()
    }
}

/// How a watch runs.
pub(crate) struct Watch {
    pub store: Arc<Store>,
    pub progress: Arc<Progress>,
    pub running: Arc<dyn Fn(SourceId) -> bool + Send + Sync>,
    /// Only these sources; empty means all.
    pub only: Vec<SourceId>,
    /// How often the store is looked at without a change notification.
    pub interval: Duration,
    pub shutdown: watch::Receiver<bool>,
}

/// Shortest time between two snapshots, so a busy scan does not flood the stream.
const DEBOUNCE: Duration = Duration::from_millis(100);

impl Watch {
    /// Sends updates to `tx` until the client goes away or shutdown.
    pub(crate) async fn run(mut self, tx: mpsc::Sender<Result<ProgressUpdate, Status>>) {
        let mut changes = self.progress.subscribe();
        let mut last: BTreeMap<SourceId, SourceProgress> = BTreeMap::new();
        let mut first = true;
        loop {
            if *self.shutdown.borrow() {
                return;
            }
            changes.mark_unchanged();
            let now = match self.snapshot().await {
                Ok(now) => now,
                Err(status) => {
                    let _ = tx.try_send(Err(status));
                    return;
                }
            };
            for update in diff(&last, &now, first) {
                // A client that stops reading must not hold the task (and the store) past
                // shutdown.
                let sent = tokio::select! {
                    sent = tx.send(Ok(update)) => sent.is_ok(),
                    _ = self.shutdown.wait_for(|s| *s) => false,
                };
                if !sent {
                    return;
                }
            }
            last = now;
            first = false;
            tokio::select! {
                _ = changes.changed() => {}
                () = tokio::time::sleep(self.interval) => {}
                () = tx.closed() => return,
                _ = self.shutdown.wait_for(|s| *s) => return,
            }
            tokio::select! {
                () = tokio::time::sleep(DEBOUNCE.min(self.interval)) => {}
                () = tx.closed() => return,
                _ = self.shutdown.wait_for(|s| *s) => return,
            }
        }
    }

    async fn snapshot(&self) -> Result<BTreeMap<SourceId, SourceProgress>, Status> {
        let store = Arc::clone(&self.store);
        let only = self.only.clone();
        let rows = tokio::task::spawn_blocking(move || -> Result<_, StoreError> {
            let sources: Vec<Source> = store
                .sources()?
                .into_iter()
                .filter(|s| only.is_empty() || only.contains(&s.id))
                .collect();
            sources
                .into_iter()
                .map(|s| {
                    let counts = store.object_counts(s.id)?;
                    let checkpoint = store.scan_checkpoint(s.id)?.unwrap_or_default();
                    let llm = store.llm_counts(s.id)?;
                    let spent = store.llm_usage(s.id, doewood_store::utc_day(unix_now()))?;
                    Ok((s, counts, checkpoint, (llm, spent)))
                })
                .collect::<Result<Vec<_>, _>>()
        })
        .await
        .map_err(|e| crate::error::join(&e))?
        .map_err(crate::error::store)?;
        Ok(rows
            .into_iter()
            .map(|(s, counts, cp, (llm, spent))| {
                let events = self.progress.get(s.id);
                let p = SourceProgress {
                    source_id: s.id.to_string(),
                    state: convert::source_state(s.state) as i32,
                    state_reason: s.state_reason.clone().unwrap_or_default(),
                    counters: Some(ProgressCounters {
                        seen: counts.live(),
                        new: events.new,
                        changed: events.changed,
                        deleted: events.deleted,
                        processed: counts.done,
                        failed: counts.failed,
                        queued_llm: llm.pending,
                        pending: counts.pending,
                        summarized: llm.done,
                        llm_failed: llm.failed,
                        llm_tokens_today: spent.tokens,
                    }),
                    pass: cp.pass,
                    pass_in_progress: cp.in_progress,
                    last_pass_completed_at: cp.last_completed_at.unwrap_or(0),
                    scanner_running: (self.running)(s.id),
                    llm_last_error: llm.last_error.unwrap_or_default(),
                };
                (s.id, p)
            })
            .collect())
    }
}

/// The updates that take a client from `last` to `now`.
fn diff(
    last: &BTreeMap<SourceId, SourceProgress>,
    now: &BTreeMap<SourceId, SourceProgress>,
    first: bool,
) -> Vec<ProgressUpdate> {
    let at = unix_now();
    let mut out = Vec::new();
    for (id, p) in now {
        let (kind, previous) = match last.get(id) {
            _ if first => (Kind::Snapshot, 0),
            None => (Kind::Snapshot, 0),
            Some(old) if old.state != p.state => (Kind::StateChanged, old.state),
            Some(old) if old != p => (Kind::Counters, 0),
            Some(_) => continue,
        };
        out.push(ProgressUpdate {
            kind: kind as i32,
            progress: Some(p.clone()),
            previous_state: previous,
            at,
        });
    }
    for (id, old) in last {
        if !now.contains_key(id) {
            out.push(ProgressUpdate {
                kind: Kind::Removed as i32,
                progress: Some(SourceProgress {
                    state: pb::SourceState::Removed as i32,
                    scanner_running: false,
                    ..old.clone()
                }),
                previous_state: old.state,
                at,
            });
        }
    }
    out
}

fn unix_now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| i64::try_from(d.as_secs()).unwrap_or(i64::MAX))
}

#[cfg(test)]
mod tests {
    use doewood_s3::{EventOrigin, ObjectVersion};

    use super::*;

    fn event(source: SourceId, kind: EventKind) -> ObjectEvent {
        ObjectEvent {
            source,
            key: "k".into(),
            kind,
            origin: EventOrigin::Scan,
        }
    }

    #[test]
    fn counts_events_per_source() {
        let p = Progress::new();
        let (a, b) = (SourceId::generate(), SourceId::generate());
        let v = ObjectVersion {
            etag: None,
            size: 1,
            last_modified: None,
        };
        let mut rx = p.subscribe();
        p.record(&event(a, EventKind::New(v.clone())));
        p.record(&event(a, EventKind::New(v.clone())));
        p.record(&event(a, EventKind::Changed(v)));
        p.record(&event(b, EventKind::Deleted));
        assert!(rx.has_changed().unwrap());
        rx.mark_unchanged();
        assert_eq!(
            p.get(a),
            EventCounts {
                new: 2,
                changed: 1,
                ..EventCounts::default()
            }
        );
        assert_eq!(p.get(b).deleted, 1);
        p.forget(a);
        assert_eq!(p.get(a), EventCounts::default());
        assert!(rx.has_changed().unwrap());
    }

    #[test]
    fn diffs_name_what_changed() {
        let id = SourceId::generate();
        let p = |state: pb::SourceState, seen| SourceProgress {
            source_id: id.to_string(),
            state: state as i32,
            counters: Some(ProgressCounters {
                seen,
                ..ProgressCounters::default()
            }),
            ..SourceProgress::default()
        };
        let one = |s, n| BTreeMap::from([(id, p(s, n))]);
        let empty = BTreeMap::new();
        let kinds = |u: Vec<ProgressUpdate>| u.iter().map(|u| u.kind).collect::<Vec<_>>();
        let active = pb::SourceState::Active;
        assert_eq!(
            kinds(diff(&empty, &one(active, 0), true)),
            [Kind::Snapshot as i32]
        );
        assert!(diff(&one(active, 0), &one(active, 0), false).is_empty());
        assert_eq!(
            kinds(diff(&one(active, 0), &one(active, 5), false)),
            [Kind::Counters as i32]
        );
        let changed = diff(&one(active, 5), &one(pb::SourceState::Error, 5), false);
        assert_eq!(kinds(changed.clone()), [Kind::StateChanged as i32]);
        assert_eq!(changed[0].previous_state, active as i32);
        let removed = diff(&one(active, 5), &empty, false);
        assert_eq!(kinds(removed.clone()), [Kind::Removed as i32]);
        assert_eq!(
            removed[0].progress.as_ref().unwrap().state,
            pb::SourceState::Removed as i32
        );
    }
}
