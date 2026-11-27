//! `PurgeSource`, `PurgeOrphans` and `WatchPurge` (issue #168): jobs that delete what no live
//! location holds, a bounded batch at a time (`doewood_model::purge`).
//!
//! A source purge marks the source `removed` first, so that nothing scans or ingests it, and
//! keeps its claim, so that no other instance adds it meanwhile. It waits for the files
//! ingest still holds, removes the source's locations and purges what they leave, then the
//! ended locations under its prefix, and last removes the source as `RemoveSource` does. An
//! interrupted purge leaves the source `removed`; purging it again, or the next start
//! ([`resume_purges`]), carries on. Every batch is idempotent, so a batch that failed (a
//! deadlock with ingest, the database away) is simply run again.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use doewood_core::SourceId;
use doewood_model::{Backend, Purged, SourceKey};
use doewood_store::{Source, SourceState};
use tokio::sync::watch;
use tonic::Status;
use tracing::{info, warn};
use uuid::Uuid;

use crate::error;
use crate::state::ApiState;

/// Rows a batch examines.
pub(crate) const BATCH: u32 = 200;

/// How often a batch that failed is tried again before the job fails.
const ATTEMPTS: u32 = 6;

/// How often a source purge looks whether ingest still holds files of the source.
const IN_FLIGHT_POLL: Duration = Duration::from_millis(100);

/// What a purge job is doing.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum PurgePhase {
    /// Waiting for the shared store, or for ingest to let go of the source's files.
    Waiting,
    /// Deleting.
    Running,
    /// Nothing is left.
    Done,
    /// It stopped; see the error.
    Failed,
}

impl PurgePhase {
    fn is_final(self) -> bool {
        matches!(self, Self::Done | Self::Failed)
    }
}

/// Where a purge job is.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct PurgeView {
    pub job: Uuid,
    pub source: Option<SourceId>,
    pub phase: PurgePhase,
    pub batches: u64,
    pub done: Purged,
    pub llm_jobs: u64,
    pub error: Option<String>,
}

/// The purge jobs this instance started.
#[derive(Default)]
pub(crate) struct Purges {
    jobs: Mutex<Jobs>,
}

#[derive(Default)]
struct Jobs {
    all: HashMap<Uuid, watch::Receiver<PurgeView>>,
    /// The running job of each source (`None`: the orphan purge).
    running: HashMap<Option<SourceId>, Uuid>,
}

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

impl Purges {
    /// A job this instance started, if it still knows it.
    pub(crate) fn watch(&self, job: Uuid) -> Option<watch::Receiver<PurgeView>> {
        lock(&self.jobs).all.get(&job).cloned()
    }

    /// The running job of `source`, if any.
    fn running(&self, source: Option<SourceId>) -> Option<Uuid> {
        let jobs = lock(&self.jobs);
        jobs.running.get(&source).copied().filter(|id| {
            jobs.all
                .get(id)
                .is_some_and(|rx| !rx.borrow().phase.is_final())
        })
    }

    fn add(&self, source: Option<SourceId>) -> (Uuid, watch::Sender<PurgeView>) {
        let job = Uuid::now_v7();
        let (tx, rx) = watch::channel(PurgeView {
            job,
            source,
            phase: PurgePhase::Waiting,
            batches: 0,
            done: Purged::default(),
            llm_jobs: 0,
            error: None,
        });
        let mut jobs = lock(&self.jobs);
        jobs.all.insert(job, rx);
        jobs.running.insert(source, job);
        (job, tx)
    }
}

/// Starts (or finds) the purge of a source of this instance.
pub(crate) async fn start_source(state: &ApiState, id: SourceId) -> Result<Uuid, Status> {
    let _guard = state.lock_sources().await;
    let source = state.source(id).await?;
    if let Some(job) = state.purges().running(Some(id)) {
        return Ok(job);
    }
    let backend = state.backend()?;
    let key = SourceKey::new(&source.endpoint, &source.bucket, source.prefix.clone())
        .map_err(error::model)?;
    check_overlaps(state, &backend, &source, &key).await?;
    // The claim is kept until the end, so no other instance adds the source while its rows
    // go. One held by another instance means the data is that instance's to purge.
    match backend.claim_source(&key, state.instance()).await {
        Ok(_) => state.store().claims().set_mine(id),
        Err(doewood_model::Error::SourceClaimed { owner, .. }) => {
            return Err(Status::failed_precondition(format!(
                "source {id} is claimed by instance {owner}: purge it there, or release its \
                 claim (ReleaseSourceClaim) if that instance is gone"
            )));
        }
        Err(e) => return Err(error::model(e)),
    }
    if source.state != SourceState::Removed {
        state
            .db(move |s| s.set_source_state(id, SourceState::Removed, None))
            .await?;
    }
    if let Some(d) = state.discovery() {
        d.stop_source(id);
    }
    state.progress().touch();
    let (job, tx) = state.purges().add(Some(id));
    info!(source = %id, job = %job, "source purge started");
    let state = state.clone();
    tokio::spawn(async move {
        let result = run_source(&state, id, &key, &tx).await;
        finish(&tx, result);
    });
    Ok(job)
}

/// Starts (or finds) the orphan purge.
pub(crate) fn start_orphans(state: &ApiState) -> Uuid {
    let purges = state.purges();
    // Under the jobs lock, so two calls start one job.
    let mut jobs = lock(&purges.jobs);
    if let Some(job) = jobs.running.get(&None).copied().filter(|id| {
        jobs.all
            .get(id)
            .is_some_and(|rx| !rx.borrow().phase.is_final())
    }) {
        return job;
    }
    let job = Uuid::now_v7();
    let (tx, rx) = watch::channel(PurgeView {
        job,
        source: None,
        phase: PurgePhase::Waiting,
        batches: 0,
        done: Purged::default(),
        llm_jobs: 0,
        error: None,
    });
    jobs.all.insert(job, rx);
    jobs.running.insert(None, job);
    drop(jobs);
    info!(job = %job, "orphan purge started");
    let state = state.clone();
    tokio::spawn(async move {
        let result = run_orphans(&state, &tx).await;
        finish(&tx, result);
    });
    job
}

/// Starts the purges of the sources left `removed` by an interrupted purge, once the shared
/// store answers. `serve` runs it after the start.
pub async fn resume_purges(state: &ApiState, mut shutdown: watch::Receiver<bool>) {
    let mut backend = state.parts().backend.clone();
    tokio::select! {
        ready = backend.wait_for(Option::is_some) => if ready.is_err() { return },
        _ = shutdown.wait_for(|s| *s) => return,
    }
    let sources = match state.db(doewood_store::Store::sources).await {
        Ok(s) => s,
        Err(e) => {
            warn!(error = %e, "cannot list the sources to resume their purges");
            return;
        }
    };
    for s in sources.iter().filter(|s| s.state == SourceState::Removed) {
        match start_source(state, s.id).await {
            Ok(job) => info!(source = %s.id, job = %job, "source purge resumed"),
            Err(e) => warn!(source = %s.id, error = %e.message(), "cannot resume the source purge"),
        }
    }
}

/// Another source of this instance, or another claim in the shared store, whose objects
/// overlap the source's: its locations would go too, so the purge is refused.
async fn check_overlaps(
    state: &ApiState,
    backend: &Arc<dyn Backend>,
    source: &Source,
    key: &SourceKey,
) -> Result<(), Status> {
    let overlaps = |a: &str, b: &str| a.starts_with(b) || b.starts_with(a);
    let local = state.db(doewood_store::Store::sources).await?;
    if let Some(other) = local.iter().find(|o| {
        o.id != source.id
            && o.endpoint.trim_end_matches('/') == key.endpoint()
            && o.bucket.trim() == key.bucket()
            && overlaps(&o.prefix, key.prefix())
    }) {
        return Err(Status::failed_precondition(format!(
            "source {} (prefix {:?}) overlaps this source's objects; remove or purge it first",
            other.id, other.prefix
        )));
    }
    if let Some(claim) = backend
        .overlapping_claims(key)
        .await
        .map_err(error::model)?
        .first()
    {
        return Err(Status::failed_precondition(format!(
            "instance {} claims source {} whose objects overlap this source's",
            claim.owner, claim.key
        )));
    }
    Ok(())
}

fn finish(tx: &watch::Sender<PurgeView>, result: Result<(), String>) {
    tx.send_modify(|v| match result {
        Ok(()) => v.phase = PurgePhase::Done,
        Err(e) => {
            v.phase = PurgePhase::Failed;
            v.error = Some(e);
        }
    });
    let v = tx.borrow().clone();
    if v.phase == PurgePhase::Done {
        info!(
            job = %v.job,
            source = ?v.source.map(|s| s.to_string()),
            batches = v.batches,
            records = v.done.records,
            locations = v.done.locations,
            companies = v.done.companies,
            "purge done"
        );
    } else {
        warn!(job = %v.job, error = ?v.error, "purge failed");
    }
}

/// The backend, once connected (`None` on shutdown).
async fn backend(state: &ApiState, tx: &watch::Sender<PurgeView>) -> Option<Arc<dyn Backend>> {
    let mut rx = state.parts().backend.clone();
    if let Some(b) = rx.borrow().clone() {
        return Some(b);
    }
    tx.send_modify(|v| v.phase = PurgePhase::Waiting);
    let mut shutdown = state.shutdown();
    tokio::select! {
        ready = rx.wait_for(Option::is_some) => ready.ok().and_then(|b| b.clone()),
        _ = shutdown.wait_for(|s| *s) => None,
    }
}

/// Runs batches of `step` from `cursor` until one says it was the last.
async fn batches<F, Fut>(
    state: &ApiState,
    tx: &watch::Sender<PurgeView>,
    mut cursor: Option<String>,
    step: F,
) -> Result<(), String>
where
    F: Fn(Arc<dyn Backend>, Option<String>) -> Fut,
    Fut: std::future::Future<Output = doewood_model::Result<Purged>>,
{
    loop {
        let mut attempt = 0;
        let done = loop {
            if *state.shutdown().borrow() {
                return Err("the server shut down; purge again to carry on".to_owned());
            }
            let Some(b) = backend(state, tx).await else {
                return Err("the server shut down; purge again to carry on".to_owned());
            };
            tx.send_modify(|v| v.phase = PurgePhase::Running);
            match step(b, cursor.clone()).await {
                Ok(done) => break done,
                Err(e) if attempt + 1 < ATTEMPTS => {
                    attempt += 1;
                    warn!(error = %e, attempt, "purge batch failed; trying again");
                    tokio::time::sleep(Duration::from_millis(100 << attempt)).await;
                }
                Err(e) => return Err(format!("store: {e}")),
            }
        };
        let hashes: Vec<[u8; 32]> = done.hashes.iter().map(|h| h.0).collect();
        let llm = match state.db(move |s| s.forget_llm_jobs(&hashes)).await {
            Ok(n) => n,
            Err(e) => {
                // The queue finds the record gone and skips the job.
                warn!(error = %e.message(), "cannot delete the LLM jobs of purged records");
                0
            }
        };
        tx.send_modify(|v| {
            v.batches += 1;
            v.done.add(&done);
            v.llm_jobs += llm;
        });
        match done.next {
            Some(next) => cursor = Some(next),
            None => return Ok(()),
        }
    }
}

async fn run_source(
    state: &ApiState,
    id: SourceId,
    key: &SourceKey,
    tx: &watch::Sender<PurgeView>,
) -> Result<(), String> {
    // Nothing new of the source is queued once it is removed; what ingest holds finishes
    // without writing (it finds the source removed) or before the locations go.
    if let Some(r) = state.parts().reprocess.clone() {
        let mut shutdown = state.shutdown();
        while r.in_flight(id) > 0 {
            tokio::select! {
                () = tokio::time::sleep(IN_FLIGHT_POLL) => {}
                _ = shutdown.wait_for(|s| *s) => {
                    return Err("the server shut down; purge again to carry on".to_owned());
                }
            }
        }
    }
    let k = key.clone();
    batches(state, tx, None, move |b, _| {
        let k = k.clone();
        async move { b.purge_source_locations(&k, BATCH).await }
    })
    .await?;
    let k = key.clone();
    batches(state, tx, None, move |b, after| {
        let k = k.clone();
        async move {
            b.purge_ended_locations(Some(&k), after.as_deref(), BATCH)
                .await
        }
    })
    .await?;
    // Last, the source itself, as RemoveSource removes it.
    let _guard = state.lock_sources().await;
    let backend = backend(state, tx)
        .await
        .ok_or("the server shut down; purge again to carry on")?;
    match backend.release_source(key, state.instance()).await {
        Ok(_) => {}
        Err(doewood_model::Error::SourceClaimed { owner, .. }) => {
            warn!(source = %id, owner = %owner, "another instance owns the purged source's claim now");
        }
        Err(e) => return Err(format!("cannot release the claim: {e}")),
    }
    state
        .db(move |s| s.delete_source(id))
        .await
        .map_err(|e| e.message().to_owned())?;
    state.store().claims().set_mine(id);
    state.progress().forget(id);
    Ok(())
}

async fn run_orphans(state: &ApiState, tx: &watch::Sender<PurgeView>) -> Result<(), String> {
    batches(state, tx, None, |b, after| async move {
        b.purge_orphan_records(after.as_deref(), BATCH).await
    })
    .await?;
    batches(state, tx, None, |b, after| async move {
        b.purge_ended_locations(None, after.as_deref(), BATCH).await
    })
    .await
}
