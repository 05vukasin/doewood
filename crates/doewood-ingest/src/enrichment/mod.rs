//! The enrichment job (ADR 0002 sections 1 to 3, 5 and 6): one per instance, next to ingest.
//!
//! - **What it works through:** the companies the store says are due
//!   ([`FreshnessStore::companies_due`]): new ones right away, known ones on each registry's
//!   refresh schedule and as soon as their document count doubles. Each company is worked on
//!   under its lease ([`doewood_model::ClaimStore::lease_company`]), so with a shared Postgres one
//!   instance enriches it and the others reuse the result.
//! - **Per company** ([`company`]): its identifiers drive the registries, in jurisdiction order
//!   (the identifier's country first, then GLEIF, then Wikidata); without an identifier a name
//!   search gives candidates, scored with doewood-enrich's fuzzy score and veto. The evidence is
//!   fused with noisy-OR and passed through the gate ([`gate`]): merge at 0.90 or more, review
//!   in between (a proposal in the proposals table, for a person; never a merge), nothing
//!   below 0.50. A dismissed pair is never merged or proposed.
//! - **What it writes** ([`write`]): the registry identifiers on the company, its registry facts
//!   in its props, and dated edges (`parent_of` / `subsidiary_of`, `former_parent_of`,
//!   `merged_into`, `renamed_to`, `acquired`, `liquidated`) with `source = registry`, the
//!   registry and record id as evidence, dates, confidence and state. Official registries'
//!   edges are active; Wikidata's and Exhibit 21's are proposed (the trust order of section 3).
//! - **Website and favicon** ([`website`], section 5): an official website from Wikidata, or
//!   the email domains of the company's documents (never free mail), verified before it is
//!   stored, then its favicon through the SSRF guard into the icon cache.
//! - **Failure-soft:** a registry failure is a warning on that company (and an earlier retry of
//!   that registry); it never stops the pass or ingest.
//!
//! Only identifiers and names go to registries, and domains to websites; documents never leave
//! the instance (ADR 0002, the privacy rule). No LLM is involved.

mod clients;
mod company;
mod gate;
mod subject;
mod website;
mod write;

use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use std::time::Duration;

use doewood_core::InstanceId;
use doewood_model::{Backend, CacheStore, DueCompany, DueReason};
use doewood_registry::Registry;
use doewood_store::{EnrichmentConfig, Store};
use tokio::sync::{Notify, Semaphore, watch};
use tokio::task::{JoinHandle, JoinSet};
use tracing::{debug, info, warn};
use uuid::Uuid;

pub use clients::{Clients, Templates, WebClientFactory};
pub use company::refresh_interval;

/// Where the caches live: the backend's `CacheStore` once there is one (in memory before).
pub(crate) type CacheSource = Arc<dyn Fn() -> Option<Arc<dyn CacheStore>> + Send + Sync>;

/// How the job runs. The defaults are the production values.
#[derive(Clone, Debug)]
pub struct EnrichmentOptions {
    /// Companies worked on at the same time (each registry's own rate limit still applies).
    pub concurrency: usize,
    /// The wait between two passes over the due companies.
    pub interval: Duration,
    /// The lease on a company while it is worked on; renewed between registries.
    pub lease: Duration,
    /// Companies asked for per page of `companies_due`.
    pub page: u32,
    /// How long after a failed registry it is asked again.
    pub retry_after_failure: Duration,
    /// How long to wait for the companies in flight at shutdown.
    pub drain: Duration,
}

impl Default for EnrichmentOptions {
    fn default() -> Self {
        Self {
            concurrency: 4,
            interval: Duration::from_secs(30),
            lease: Duration::from_mins(15),
            page: 100,
            retry_after_failure: Duration::from_hours(6),
            drain: Duration::from_secs(10),
        }
    }
}

/// The job's counters since the instance started, for `WatchProgress`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct EnrichmentStats {
    /// Companies enriched (every registry asked, whatever it answered).
    pub enriched: u64,
    /// Companies merged by the gate.
    pub merged: u64,
    /// Proposals recorded for a person (review band, conflicts, dismissed pairs).
    pub proposed: u64,
    /// Companies whose enrichment failed (a store error); retried on the next pass.
    pub failed: u64,
    /// Registry warnings (failures, refusals), each also logged.
    pub registry_warnings: u64,
    /// Edges written.
    pub edges: u64,
    /// Edges that landed on the company an end was merged into while the company was worked
    /// on (issue #60).
    pub redirected: u64,
    /// Websites verified and stored.
    pub websites: u64,
    /// Passes completed.
    pub passes: u64,
    /// When the last pass completed, Unix seconds; 0 before the first.
    pub last_pass_completed_at: i64,
    /// Whether a pass is running now.
    pub running: bool,
}

#[derive(Default)]
struct Counters {
    enriched: AtomicU64,
    merged: AtomicU64,
    proposed: AtomicU64,
    failed: AtomicU64,
    registry_warnings: AtomicU64,
    edges: AtomicU64,
    redirected: AtomicU64,
    websites: AtomicU64,
    passes: AtomicU64,
    last_pass_completed_at: AtomicI64,
    running: AtomicBool,
}

impl Counters {
    fn snapshot(&self) -> EnrichmentStats {
        let get = |a: &AtomicU64| a.load(Ordering::Relaxed);
        EnrichmentStats {
            enriched: get(&self.enriched),
            merged: get(&self.merged),
            proposed: get(&self.proposed),
            failed: get(&self.failed),
            registry_warnings: get(&self.registry_warnings),
            edges: get(&self.edges),
            redirected: get(&self.redirected),
            websites: get(&self.websites),
            passes: get(&self.passes),
            last_pass_completed_at: self.last_pass_completed_at.load(Ordering::Relaxed),
            running: self.running.load(Ordering::Relaxed),
        }
    }
}

/// Reads the enrichment settings for a pass. Production: [`Store::enrichment_config`].
pub type SettingsSource = Arc<dyn Fn() -> Result<EnrichmentConfig, String> + Send + Sync>;

/// What the job is built from.
pub struct EnrichmentParts {
    /// This instance: the lease owner and the merge actor.
    pub instance: InstanceId,
    /// The graph, claims, caches and freshness; `None` while the shared Postgres is not
    /// connected.
    pub backend: watch::Receiver<Option<Arc<dyn Backend>>>,
    /// The runtime settings, read on every pass.
    pub settings: SettingsSource,
    /// The clients' endpoints and limits.
    pub templates: Templates,
    /// The guarded web client for websites and favicons.
    pub web: WebClientFactory,
    /// Tuning.
    pub options: EnrichmentOptions,
    /// Called when the counters change.
    pub on_change: Arc<dyn Fn(EnrichmentStats) + Send + Sync>,
}

impl EnrichmentParts {
    /// The settings of `store`, read off the async threads' way (a short blocking read).
    pub fn store_settings(store: Arc<Store>) -> SettingsSource {
        Arc::new(move || store.enrichment_config().map_err(|e| e.to_string()))
    }
}

/// The enrichment job. Cheap to clone.
#[derive(Clone)]
pub struct Enrichment {
    inner: Arc<Inner>,
}

struct Inner {
    instance: InstanceId,
    backend: watch::Receiver<Option<Arc<dyn Backend>>>,
    settings: SettingsSource,
    clients: tokio::sync::Mutex<clients::ClientSet>,
    options: EnrichmentOptions,
    counters: Counters,
    on_change: Arc<dyn Fn(EnrichmentStats) + Send + Sync>,
    wake: Notify,
    /// The companies this instance's tasks work on now: a lease is the instance's, so this
    /// keeps two of its own tasks off one company (a merge takes the other company too).
    busy: Busy,
}

/// The companies in work in this instance.
pub(crate) type Busy = std::sync::Mutex<std::collections::HashSet<Uuid>>;

/// Marks `id` busy; `false` when another task has it.
pub(crate) fn take(busy: &Busy, id: Uuid) -> bool {
    busy.lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .insert(id)
}

pub(crate) fn give_back(busy: &Busy, ids: &[Uuid]) {
    let mut b = busy
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    for id in ids {
        b.remove(id);
    }
}

impl std::fmt::Debug for Enrichment {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Enrichment")
            .field("instance", &self.inner.instance)
            .field("options", &self.inner.options)
            .finish_non_exhaustive()
    }
}

/// What one company's enrichment did, for the counters.
// Independent facts about one run, not states of one machine.
#[allow(clippy::struct_excessive_bools)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Outcome {
    /// Another instance held the lease: nothing was done.
    pub skipped: bool,
    /// Merges done.
    pub merged: u64,
    /// Proposals recorded.
    pub proposed: u64,
    /// Registry warnings.
    pub warnings: u64,
    /// Edges written.
    pub edges: u64,
    /// Edges redirected onto the company an end was merged into.
    pub redirected: u64,
    /// Whether a website was verified and stored.
    pub website: bool,
    /// A merge waited for a company another task of this instance worked on: the company is
    /// run again once the page's tasks are done.
    pub deferred: bool,
    /// The company was merged away while it was worked on (issue #62): its job stopped and
    /// the company it was merged into is due.
    pub merged_away: bool,
}

/// What [`Enrichment::one`] did.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Worked {
    /// Not worked on (leased elsewhere, or by another task).
    Skipped,
    /// Worked on.
    Done,
    /// Worked on; to be run again once the page's tasks are done (a merge waited for one).
    Again,
}

impl Enrichment {
    /// The job; [`start`](Self::start) runs it.
    pub fn new(parts: EnrichmentParts) -> Self {
        let backend = parts.backend.clone();
        let caches: CacheSource = Arc::new(move || {
            backend
                .borrow()
                .as_ref()
                .map(|b| Arc::clone(b) as Arc<dyn CacheStore>)
        });
        Self {
            inner: Arc::new(Inner {
                instance: parts.instance,
                backend: parts.backend,
                settings: parts.settings,
                clients: tokio::sync::Mutex::new(clients::ClientSet::new(
                    parts.templates,
                    parts.web,
                    &caches,
                )),
                options: parts.options,
                counters: Counters::default(),
                on_change: parts.on_change,
                wake: Notify::new(),
                busy: Busy::default(),
            }),
        }
    }

    /// The counters since start.
    pub fn stats(&self) -> EnrichmentStats {
        self.inner.counters.snapshot()
    }

    /// Starts the next pass now instead of after the interval.
    pub fn wake(&self) {
        self.inner.wake.notify_one();
    }

    /// Runs passes until shutdown: one, then one every [`EnrichmentOptions::interval`] (or on
    /// [`wake`](Self::wake)).
    pub fn start(&self, mut shutdown: watch::Receiver<bool>) -> JoinHandle<()> {
        let me = self.clone();
        tokio::spawn(async move {
            info!(
                concurrency = me.inner.options.concurrency,
                interval_s = me.inner.options.interval.as_secs(),
                "enrichment started"
            );
            loop {
                if stopping(&shutdown) {
                    break;
                }
                let backend = current(&me.inner.backend);
                if let Some(backend) = backend {
                    me.pass(&backend, &shutdown).await;
                }
                tokio::select! {
                    () = tokio::time::sleep(me.inner.options.interval) => {}
                    () = me.inner.wake.notified() => {}
                    _ = shutdown.wait_for(|s| *s) => break,
                }
            }
            info!("enrichment stopped");
        })
    }

    fn changed(&self) {
        (self.inner.on_change)(self.stats());
    }

    async fn settings(&self) -> Option<EnrichmentConfig> {
        let source = Arc::clone(&self.inner.settings);
        match tokio::task::spawn_blocking(move || source()).await {
            Ok(Ok(s)) => Some(s),
            Ok(Err(e)) => {
                warn!(error = %e, "enrichment settings unreadable; this pass is skipped");
                None
            }
            Err(e) => {
                warn!(error = %e, "enrichment settings task failed; this pass is skipped");
                None
            }
        }
    }

    /// One pass over every due company, page by page. Returns how many were worked on.
    pub async fn pass(&self, backend: &Arc<dyn Backend>, shutdown: &watch::Receiver<bool>) -> u64 {
        let Some(settings) = self.settings().await else {
            return 0;
        };
        let (clients, warnings) = self.inner.clients.lock().await.clients(&settings);
        for w in &warnings {
            warn!(registry = %w.registry, warning = %w.kind, "registry client not built; it is skipped");
        }
        let c = &self.inner.counters;
        c.running.store(true, Ordering::Relaxed);
        self.changed();
        let permits = Arc::new(Semaphore::new(self.inner.options.concurrency.max(1)));
        let mut after: Option<Uuid> = None;
        let mut worked = 0;
        'pages: loop {
            let page = match backend
                .companies_due(&self.inner.instance, after, self.inner.options.page)
                .await
            {
                Ok(p) => p,
                Err(e) => {
                    warn!(error = %e, "due companies unreadable; the pass ends");
                    break;
                }
            };
            let Some(last) = page.last() else {
                break;
            };
            after = Some(last.company);
            let mut tasks = JoinSet::new();
            for due in page {
                if stopping(shutdown) {
                    break;
                }
                let Ok(permit) = Arc::clone(&permits).acquire_owned().await else {
                    break;
                };
                let me = self.clone();
                let backend = Arc::clone(backend);
                let clients = clients.clone();
                tasks.spawn(async move {
                    let _permit = permit;
                    let worked = me.one(&backend, &clients, &due, false).await;
                    (due, worked)
                });
            }
            // Companies whose merge waited for another task of this pass.
            let mut again = Vec::new();
            let mut stop = shutdown.clone();
            let mut closed = false;
            loop {
                let next = tokio::select! {
                    next = tasks.join_next() => Some(next),
                    r = stop.changed(), if !closed => {
                        closed = r.is_err();
                        None
                    }
                };
                match next {
                    Some(None) => break,
                    Some(Some(Ok((_, Worked::Done)))) => worked += 1,
                    Some(Some(Ok((due, Worked::Again)))) => {
                        worked += 1;
                        again.push(due);
                    }
                    Some(Some(Ok((_, Worked::Skipped)))) => {}
                    Some(Some(Err(e))) => warn!(error = %e, "an enrichment task failed"),
                    None if !stopping(&stop) => {}
                    None => {
                        // Shutdown: the companies in flight get a moment, then are dropped
                        // (their leases expire).
                        let drain = tokio::time::timeout(self.inner.options.drain, async {
                            while tasks.join_next().await.is_some() {}
                        });
                        if drain.await.is_err() {
                            tasks.abort_all();
                        }
                        break 'pages;
                    }
                }
            }
            // The page's tasks are done, so the companies those merges waited for are free
            // now (issue #60): one more run each, one at a time.
            for due in again {
                if stopping(shutdown) {
                    break 'pages;
                }
                self.one(backend, &clients, &due, true).await;
            }
        }
        c.running.store(false, Ordering::Relaxed);
        c.passes.fetch_add(1, Ordering::Relaxed);
        c.last_pass_completed_at
            .store(unix_now(), Ordering::Relaxed);
        self.changed();
        if worked > 0 {
            info!(companies = worked, "enrichment pass done");
        } else {
            debug!("enrichment pass: nothing due");
        }
        worked
    }

    /// One company: lease, enrich, release. `retried`: its second run in this pass, after a
    /// merge waited for another task.
    async fn one(
        &self,
        backend: &Arc<dyn Backend>,
        clients: &Clients,
        due: &DueCompany,
        retried: bool,
    ) -> Worked {
        let owner = &self.inner.instance;
        let ttl = self.inner.options.lease;
        if !take(&self.inner.busy, due.company) {
            debug!(company = %due.company, "another task is enriching this company");
            return Worked::Skipped;
        }
        match backend.lease_company(due.company, owner, ttl).await {
            Ok(true) => {}
            Ok(false) => {
                give_back(&self.inner.busy, &[due.company]);
                debug!(company = %due.company, "another instance is enriching this company");
                return Worked::Skipped;
            }
            Err(e) => {
                give_back(&self.inner.busy, &[due.company]);
                warn!(company = %due.company, error = %e, "company lease failed; skipped");
                return Worked::Skipped;
            }
        }
        let job = company::Job::new(
            Arc::clone(backend),
            clients,
            owner,
            &self.inner.busy,
            ttl,
            self.inner.options.retry_after_failure,
        );
        let c = &self.inner.counters;
        let result = job.run(due).await;
        let mut held = job.leased();
        held.push(due.company);
        for id in &held {
            if let Err(e) = backend.release_company(*id, owner).await {
                debug!(company = %id, error = %e, "company lease not released; it expires");
            }
        }
        give_back(&self.inner.busy, &held);
        let mut worked = Worked::Done;
        match result {
            Ok(o) => {
                if o.deferred && !retried {
                    // Counted once, on its second run.
                    worked = Worked::Again;
                } else if !o.merged_away {
                    c.enriched.fetch_add(1, Ordering::Relaxed);
                }
                c.merged.fetch_add(o.merged, Ordering::Relaxed);
                c.proposed.fetch_add(o.proposed, Ordering::Relaxed);
                c.registry_warnings.fetch_add(o.warnings, Ordering::Relaxed);
                c.edges.fetch_add(o.edges, Ordering::Relaxed);
                c.redirected.fetch_add(o.redirected, Ordering::Relaxed);
                if o.website {
                    c.websites.fetch_add(1, Ordering::Relaxed);
                }
                info!(
                    company = %due.company,
                    reason = reason(due.reason),
                    merged = o.merged,
                    proposed = o.proposed,
                    edges = o.edges,
                    redirected = o.redirected,
                    warnings = o.warnings,
                    website = o.website,
                    merge_waits = o.deferred,
                    merged_away = o.merged_away,
                    "company enriched"
                );
            }
            Err(e) => {
                c.failed.fetch_add(1, Ordering::Relaxed);
                warn!(company = %due.company, error = %e, "company enrichment failed; retried on the next pass");
            }
        }
        self.changed();
        worked
    }
}

fn current(rx: &watch::Receiver<Option<Arc<dyn Backend>>>) -> Option<Arc<dyn Backend>> {
    rx.borrow().clone()
}

fn stopping(rx: &watch::Receiver<bool>) -> bool {
    *rx.borrow()
}

fn reason(r: DueReason) -> &'static str {
    match r {
        DueReason::New => "new",
        DueReason::Doubled => "documents_doubled",
        DueReason::Scheduled => "scheduled",
    }
}

/// The registries a company is due for: every registry for a new company or one whose
/// document count doubled, else the scheduled ones (`None`: all).
pub(crate) fn due_registries(due: &DueCompany) -> Option<BTreeSet<String>> {
    match due.reason {
        DueReason::New | DueReason::Doubled => None,
        DueReason::Scheduled => Some(due.due.iter().cloned().collect()),
    }
}

/// The registry's name in the freshness schedule.
pub(crate) fn schedule_name(r: Registry) -> &'static str {
    r.as_str()
}

pub(crate) fn unix_now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| i64::try_from(d.as_secs()).unwrap_or(i64::MAX))
}
