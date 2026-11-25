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
//! A watch of every source also gets the enrichment job's counters ([`EnrichmentCounts`]),
//! when the instance runs one.

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

/// The enrichment job's counters since the instance started (ADR 0002 section 6).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct EnrichmentCounts {
    /// Companies enriched.
    pub enriched: u64,
    /// Companies merged by the gate.
    pub merged: u64,
    /// Proposals recorded for a person.
    pub proposed: u64,
    /// Companies whose enrichment failed.
    pub failed: u64,
    /// Registry warnings.
    pub registry_warnings: u64,
    /// Edges written.
    pub edges: u64,
    /// Edges redirected onto the company an end was merged into.
    pub redirected: u64,
    /// Websites verified.
    pub websites: u64,
    /// Passes completed.
    pub passes: u64,
    /// When the last pass completed, Unix seconds; 0 before the first.
    pub last_pass_completed_at: i64,
    /// Whether a pass is running.
    pub running: bool,
    /// The registry delta feeds (issue #49).
    pub feeds: [FeedCounts; 4],
}

/// One registry delta feed on this instance (ADR 0002 section 6, issue #49).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct FeedCounts {
    /// `gleif`, `edgar`, `apr` or `companies_house`; empty for an unused slot.
    pub feed: &'static str,
    /// Switched on here (and its registry asked).
    pub enabled: bool,
    /// This instance holds the feed's lease.
    pub leader: bool,
    /// Runs completed.
    pub runs: u64,
    /// Records read.
    pub records: u64,
    /// Companies made due.
    pub companies_due: u64,
    /// Runs that failed.
    pub failures: u64,
    /// When a run last completed, Unix seconds; 0 before the first.
    pub last_run_at: i64,
    /// Where the feed stands (a time, or Companies House's timepoint); 0: none.
    pub position: i64,
}

impl From<FeedCounts> for pb::FeedProgress {
    fn from(c: FeedCounts) -> Self {
        Self {
            feed: c.feed.to_owned(),
            enabled: c.enabled,
            leader: c.leader,
            runs: c.runs,
            records: c.records,
            companies_due: c.companies_due,
            failures: c.failures,
            last_run_at: c.last_run_at,
            position: c.position,
        }
    }
}

impl From<EnrichmentCounts> for pb::EnrichmentProgress {
    fn from(c: EnrichmentCounts) -> Self {
        Self {
            enriched: c.enriched,
            merged: c.merged,
            proposed: c.proposed,
            failed: c.failed,
            registry_warnings: c.registry_warnings,
            edges: c.edges,
            redirected: c.redirected,
            websites: c.websites,
            passes: c.passes,
            last_pass_completed_at: c.last_pass_completed_at,
            running: c.running,
            feeds: c
                .feeds
                .into_iter()
                .filter(|f| !f.feed.is_empty())
                .map(Into::into)
                .collect(),
        }
    }
}

/// The live event counters, shared by the binary (which feeds them) and the API.
#[derive(Debug)]
pub struct Progress {
    counts: Mutex<HashMap<SourceId, EventCounts>>,
    enrichment: Mutex<Option<EnrichmentCounts>>,
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
            enrichment: Mutex::new(None),
            version: watch::Sender::new(0),
        }
    }

    /// Sets the enrichment job's counters (the binary calls it as they change).
    pub fn set_enrichment(&self, counts: EnrichmentCounts) {
        let changed = {
            let mut e = self
                .enrichment
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            let changed = *e != Some(counts);
            *e = Some(counts);
            changed
        };
        if changed {
            self.touch();
        }
    }

    /// The enrichment job's counters; `None` when the instance runs none.
    pub fn enrichment(&self) -> Option<EnrichmentCounts> {
        *self
            .enrichment
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
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
        let mut last_enrichment: Option<EnrichmentCounts> = None;
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
            let mut updates = diff(&last, &now, first);
            if self.only.is_empty()
                && let Some(e) = self.progress.enrichment()
                && last_enrichment != Some(e)
            {
                last_enrichment = Some(e);
                updates.push(ProgressUpdate {
                    kind: Kind::Enrichment as i32,
                    progress: None,
                    previous_state: 0,
                    at: unix_now(),
                    enrichment: Some(e.into()),
                });
            }
            for update in updates {
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
            enrichment: None,
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
                enrichment: None,
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
    fn enrichment_counts_wake_watchers_when_they_change() {
        let p = Progress::new();
        assert_eq!(p.enrichment(), None);
        let mut rx = p.subscribe();
        rx.mark_unchanged();
        let c = EnrichmentCounts {
            enriched: 2,
            merged: 1,
            ..EnrichmentCounts::default()
        };
        p.set_enrichment(c);
        assert!(rx.has_changed().unwrap());
        rx.mark_unchanged();
        p.set_enrichment(c);
        assert!(!rx.has_changed().unwrap(), "the same counts wake nobody");
        assert_eq!(p.enrichment(), Some(c));
        let pb: pb::EnrichmentProgress = c.into();
        assert_eq!((pb.enriched, pb.merged), (2, 1));
        assert!(pb.feeds.is_empty(), "unused slots are left out");
        let mut c = c;
        c.feeds[0] = FeedCounts {
            feed: "gleif",
            enabled: true,
            leader: true,
            runs: 3,
            companies_due: 2,
            ..FeedCounts::default()
        };
        p.set_enrichment(c);
        assert!(
            rx.has_changed().unwrap(),
            "a feed's counts wake watchers too"
        );
        let pb: pb::EnrichmentProgress = c.into();
        assert_eq!(pb.feeds.len(), 1);
        let f = &pb.feeds[0];
        assert_eq!(
            (
                f.feed.as_str(),
                f.enabled,
                f.leader,
                f.runs,
                f.companies_due
            ),
            ("gleif", true, true, 3, 2)
        );
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
