//! The pipeline: scanner events → fetch → analyze → store, on a pool of workers.
//!
//! - **The queue is durable.** `objects_seen` rows with status `pending` are the work; events
//!   only make ingest look sooner. Discovery commits an object's `pending` row before it
//!   sends the event (issue #125), so a worker always finds the row it finishes. A sweep (at
//!   start, every [`IngestConfig::sweep_interval`], and after a reprocess request) feeds every
//!   pending object, so a restart, a row whose event was never sent (a crash or stop between
//!   the commit and the send), or a source added while ingest was busy, are all picked up. A
//!   finish that matches no pending row of its version is counted
//!   ([`Stats::finish_unmatched`], `doewood_ingest_finish_unmatched_total`).
//! - **Backpressure.** The dispatcher hands jobs to the workers through a bounded queue and
//!   waits while it is full, so it stops reading the scanners' bounded event channel, and the
//!   scanners wait in turn.
//! - **Workers** ([`IngestConfig::jobs`], sized from the CPUs and the memory) fetch the object
//!   (GetObject, streamed under the source's size cap), analyze it on a blocking thread, and
//!   write the record, the graph and the search entry. Content already stored with current
//!   step versions is not analyzed again: only its new location is recorded.
//! - **Status.** Each object ends `done`, `skipped` (no extractor: identity and format only)
//!   or `failed` with the reason and the attempts. Transient failures (S3, the store) are
//!   retried with backoff, at most [`IngestConfig::max_attempts`] times; a file quail or
//!   hedgehog cannot read is not retried. An object that changed meanwhile keeps its newer
//!   state (the ETag guard of `finish_object`).
//! - **Deleted objects** end their `located_at` edge; the document stays, with its other
//!   locations.
//! - **Bounded analysis.** A file is hashed once, on a blocking thread. Its analysis has one
//!   deadline (quail's engine, hedgehog's read and normalizer, [`Analyzer::analyze_hashed`]),
//!   and a watchdog of the file's time cap plus [`IngestConfig::watchdog_grace`] backs it up
//!   for a step that does not stop on time: the file fails, and the worker goes on while the
//!   analysis finishes on its thread (nothing can stop it from outside). Each worker leaves at
//!   most [`MAX_OVERRUNS`] such analyses behind; one more and it waits for the oldest to end
//!   before taking the next file, so stuck analyses cannot pile up files in memory.
//! - **Memory budget** ([`IngestConfig::memory_budget`]). A file larger than
//!   [`IngestConfig::small_file`] is admitted only once its listed size (at most the whole
//!   budget) fits in the bytes the other big files leave free, and keeps that share until its
//!   analysis really ends (an overrun's too). Small files are not charged and never wait. A big
//!   file that does not fit waits in a FIFO list of its own while its worker takes other work;
//!   only when that list is full does a worker wait for the budget itself.
//! - **Shutdown** lets every worker finish the file in hand; pending objects stay pending.

use std::borrow::Cow;
use std::collections::{HashMap, HashSet, VecDeque};
use std::path::Path;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use doewood_core::SourceId;
use doewood_model::{Backend, Blake3, ObjectLocation};
use doewood_s3::{ClientOptions, EventKind, ObjectEvent, S3Client, S3Config, S3Error};
use doewood_store::{IngestOutcome, Source, SourceState, Store, StoreError};
use serde_json::value::RawValue;
use tokio::sync::{Notify, mpsc, watch};
use tokio::task::JoinHandle;
use tracing::{debug, info, warn};
use uuid::Uuid;

use crate::analyze::{Analysis, Analyzer, AnalyzerConfig, Outcome, content_hash};
use crate::facts::DocFacts;
use crate::graph::{GraphWriter, SimilarityConfig};

/// Memory one worker is planned for: a file in memory (up to the source cap, usually far
/// less), the engine's working set, and the extracted text.
pub const WORKER_BYTES: u64 = 512 << 20;
/// Share of the available memory the workers are planned for.
pub const MEMORY_SHARE: u64 = 2; // one half
/// Most workers sized automatically.
pub const MAX_AUTO_JOBS: usize = 32;
/// Objects read per page of the pending sweep.
const SWEEP_PAGE: usize = 256;
/// Objects compared per page of a reprocess selection.
const REPROCESS_PAGE: usize = 500;
/// Reprocess jobs remembered for `WatchReprocess`.
const MAX_JOBS: usize = 100;
/// Analyses past the watchdog a worker may leave running while it takes the next files.
pub const MAX_OVERRUNS: usize = 1;
/// Files up to this size are not charged to the memory budget (16 MiB): at most this much per
/// worker, inside its [`WORKER_BYTES`].
pub const SMALL_FILE_BYTES: u64 = 16 << 20;
/// The smallest default memory budget (256 MiB).
pub const MIN_BUDGET: u64 = WORKER_BYTES / 2;

/// How ingest runs.
#[derive(Clone, Debug)]
pub struct IngestConfig {
    /// Files analyzed at the same time.
    pub jobs: usize,
    /// Jobs waiting for a worker; the dispatcher waits beyond that.
    pub queue: usize,
    /// Attempts per file for transient failures.
    pub max_attempts: u32,
    /// The first retry's delay; each next one is four times longer.
    pub retry_base: Duration,
    /// How often the pending queue is swept without an event.
    pub sweep_interval: Duration,
    /// How progress is polled while a reprocess job runs.
    pub reprocess_poll: Duration,
    /// How long past the file's time cap (`Limits::timeout_cap`) the watchdog waits before it
    /// gives up on an analysis and fails the file.
    pub watchdog_grace: Duration,
    /// Bytes of big files held at once, from their fetch to the end of their analysis
    /// (`DOEWOOD_INGEST_BUDGET_MIB`, else [`plan_budget`]). A file larger than the whole budget
    /// takes all of it.
    pub memory_budget: u64,
    /// Files up to this size are not charged to the budget ([`SMALL_FILE_BYTES`]).
    pub small_file: u64,
    /// `similar_to` edges.
    pub similarity: SimilarityConfig,
    /// The engine.
    pub analyzer: AnalyzerConfig,
}

impl IngestConfig {
    /// The configuration for an instance: `jobs` from `DOEWOOD_INGEST_JOBS` (`None`: from the
    /// CPUs and memory), the memory budget and the spreadsheet cell cap from the jobs and
    /// memory, and the OCR models directory. Tuning values such as the `similar_to` threshold
    /// are runtime settings, read from the store for every file.
    pub fn new(jobs: Option<usize>, ocr_models: &Path) -> Self {
        let jobs = jobs.unwrap_or_else(default_jobs).max(1);
        let memory = available_memory();
        Self {
            memory_budget: plan_budget(jobs, memory),
            small_file: SMALL_FILE_BYTES,
            jobs,
            queue: jobs * 2,
            max_attempts: 3,
            retry_base: Duration::from_secs(2),
            sweep_interval: Duration::from_secs(30),
            reprocess_poll: Duration::from_millis(500),
            watchdog_grace: Duration::from_mins(1),
            similarity: SimilarityConfig::default(),
            analyzer: AnalyzerConfig {
                sheet_max_cells: plan_cells(jobs, memory),
                workers: jobs,
                cpus: crate::cpus::available().min(jobs),
                ocr_models: ocr_models.to_owned(),
                ..AnalyzerConfig::default()
            },
        }
    }
}

/// Workers for this machine: one per CPU, at most what half the available memory holds at
/// [`WORKER_BYTES`] each, between 1 and [`MAX_AUTO_JOBS`].
pub fn default_jobs() -> usize {
    plan_jobs(crate::cpus::available(), available_memory())
}

/// [`default_jobs`] for `cpus` and `memory` bytes available (`None`: unknown).
pub fn plan_jobs(cpus: usize, memory: Option<u64>) -> usize {
    let by_memory = memory.map_or(cpus, |m| {
        usize::try_from(m / MEMORY_SHARE / WORKER_BYTES).unwrap_or(usize::MAX)
    });
    cpus.min(by_memory).clamp(1, MAX_AUTO_JOBS)
}

/// The default memory budget for `jobs` workers and `memory` bytes available (`None`:
/// unknown): half of the workers' plan ([`WORKER_BYTES`] each, at most half the available
/// memory, as [`plan_jobs`]), so the files in flight and the working sets beside them fit the
/// plan; at least [`MIN_BUDGET`].
pub fn plan_budget(jobs: usize, memory: Option<u64>) -> u64 {
    let plan = (jobs.max(1) as u64).saturating_mul(WORKER_BYTES);
    let plan = memory.map_or(plan, |m| plan.min(m / MEMORY_SHARE));
    (plan / 2).max(MIN_BUDGET)
}

/// The default spreadsheet cell cap for `jobs` workers and `memory` bytes available (`None`:
/// unknown): what each worker's share of half the available memory holds
/// ([`sheet::cells_for`](crate::sheet::cells_for): 320 bytes a cell, rounded down to 100,000),
/// at least 1,600,000 (one [`WORKER_BYTES`] plan, also when the memory is unknown) and at most
/// hedgehog's 10,000,000.
pub fn plan_cells(jobs: usize, memory: Option<u64>) -> u64 {
    let worker = memory.map_or(WORKER_BYTES, |m| m / MEMORY_SHARE / jobs.max(1) as u64);
    crate::sheet::cells_for(worker)
}

/// `MemAvailable`, lowered to the room under a cgroup v2 limit. `None` off Linux.
fn available_memory() -> Option<u64> {
    let field = |text: &str, name: &str| -> Option<u64> {
        let line = text.lines().find(|l| l.starts_with(name))?;
        let kb: u64 = line.split_whitespace().nth(1)?.parse().ok()?;
        Some(kb * 1024)
    };
    let meminfo = std::fs::read_to_string("/proc/meminfo").ok()?;
    let mut avail = field(&meminfo, "MemAvailable:")?;
    let read = |f: &str| {
        std::fs::read_to_string(format!("/sys/fs/cgroup/{f}"))
            .ok()?
            .trim()
            .parse::<u64>()
            .ok()
    };
    if let (Some(max), Some(cur)) = (read("memory.max"), read("memory.current")) {
        avail = avail.min(max.saturating_sub(cur));
    }
    Some(avail)
}

/// A failure, and whether trying again may help.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IngestError {
    /// One line, never a secret or a presigned URL.
    pub message: String,
    /// Worth a retry.
    pub transient: bool,
}

impl IngestError {
    fn transient(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            transient: true,
        }
    }

    fn permanent(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            transient: false,
        }
    }
}

impl std::fmt::Display for IngestError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

impl From<doewood_model::Error> for IngestError {
    fn from(e: doewood_model::Error) -> Self {
        let transient = matches!(
            e,
            doewood_model::Error::Unavailable(_) | doewood_model::Error::Backend { .. }
        );
        Self {
            message: format!("store: {e}"),
            transient,
        }
    }
}

impl From<StoreError> for IngestError {
    fn from(e: StoreError) -> Self {
        Self::transient(format!("instance store: {e}"))
    }
}

impl From<S3Error> for IngestError {
    fn from(e: S3Error) -> Self {
        let transient = match &e {
            S3Error::Status { status, .. } => *status == 429 || *status >= 500,
            S3Error::Transport { .. } | S3Error::Response { .. } | S3Error::Store(_) => true,
            _ => false,
        };
        Self {
            message: e.to_string(),
            transient,
        }
    }
}

/// Where object bytes come from: S3 in the service, a map in tests.
#[async_trait]
pub trait Fetch: Send + Sync {
    /// The object's bytes, at most the source's size cap.
    async fn fetch(&self, source: &Source, key: &str) -> Result<Vec<u8>, IngestError>;
}

/// GetObject from the source's S3 endpoint, streamed under its size cap. One client per
/// source, rebuilt when the source changes.
pub struct S3Fetch {
    store: Arc<Store>,
    options: ClientOptions,
    clients: Mutex<HashMap<SourceId, (i64, Arc<S3Client>)>>,
}

impl S3Fetch {
    /// Fetches with the sources and credentials of `store`.
    pub fn new(store: Arc<Store>, options: ClientOptions) -> Self {
        Self {
            store,
            options,
            clients: Mutex::new(HashMap::new()),
        }
    }

    async fn client(&self, source: &Source) -> Result<Arc<S3Client>, IngestError> {
        if let Some((at, c)) = lock(&self.clients).get(&source.id)
            && *at == source.updated_at
        {
            return Ok(Arc::clone(c));
        }
        let store = Arc::clone(&self.store);
        let id = source.id;
        let credentials = tokio::task::spawn_blocking(move || store.source_credentials(id))
            .await
            .map_err(|e| IngestError::transient(format!("blocking task: {e}")))??;
        let client = Arc::new(S3Client::new(
            &S3Config::for_source(source, credentials),
            self.options,
        )?);
        lock(&self.clients).insert(source.id, (source.updated_at, Arc::clone(&client)));
        Ok(client)
    }
}

#[async_trait]
impl Fetch for S3Fetch {
    async fn fetch(&self, source: &Source, key: &str) -> Result<Vec<u8>, IngestError> {
        let client = self.client(source).await?;
        let body = client.get_object(key, source.max_object_bytes()).await?;
        Ok(body.bytes().await?)
    }
}

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

/// The state of a reprocess job.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReprocessState {
    /// Comparing stored step versions with the running ones.
    Selecting,
    /// The selected files are being analyzed again.
    Running,
    /// Every selected file is through.
    Done,
    /// It stopped; see the error.
    Failed,
}

/// Where a reprocess job is.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReprocessStatus {
    /// The job.
    pub job: Uuid,
    /// Its source.
    pub source: SourceId,
    /// Every file, current or not.
    pub force: bool,
    /// What it is doing.
    pub state: ReprocessState,
    /// Ingested files compared so far.
    pub examined: u64,
    /// Files put back in the queue.
    pub selected: u64,
    /// The source's ingested files (done or skipped) now.
    pub processed: u64,
    /// The source's failed files now.
    pub failed: u64,
    /// The source's files waiting now.
    pub pending: u64,
    /// Why it failed.
    pub error: Option<String>,
}

/// What ingest is built from.
pub struct IngestParts {
    /// The instance store: sources and `objects_seen`.
    pub store: Arc<Store>,
    /// Records, graph and search; `None` while the shared Postgres is not connected.
    pub backend: watch::Receiver<Option<Arc<dyn Backend>>>,
    /// Where bytes come from.
    pub fetch: Arc<dyn Fetch>,
    /// The engine.
    pub analyzer: Arc<Analyzer>,
    /// Tuning.
    pub config: IngestConfig,
    /// Called after each object changes status (the progress counters).
    pub on_change: Arc<dyn Fn(SourceId) + Send + Sync>,
    /// Called after a document was queued for an LLM summary (wakes the LLM queue).
    pub on_llm_queued: Arc<dyn Fn(SourceId) + Send + Sync>,
}

/// The ingest service. Cheap to clone.
#[derive(Clone)]
pub struct Ingest {
    inner: Arc<Inner>,
}

struct Inner {
    store: Arc<Store>,
    backend: watch::Receiver<Option<Arc<dyn Backend>>>,
    fetch: Arc<dyn Fetch>,
    analyzer: Arc<Analyzer>,
    config: IngestConfig,
    on_change: Arc<dyn Fn(SourceId) + Send + Sync>,
    on_llm_queued: Arc<dyn Fn(SourceId) + Send + Sync>,
    writer: Mutex<Option<(usize, Arc<GraphWriter>)>>,
    inflight: Mutex<HashSet<(SourceId, String)>>,
    /// Analyses on a blocking thread now, overruns included, and their files' bytes.
    analyzing: Arc<Analyzing>,
    /// Bytes of big files in flight.
    budget: Arc<Budget>,
    /// Runs on the blocking thread before each analysis (tests stall it there).
    #[cfg(test)]
    hook: Option<Hook>,
    sweep_now: Notify,
    jobs: Mutex<HashMap<Uuid, watch::Sender<ReprocessStatus>>>,
    started: Instant,
    stats: Mutex<Stats>,
}

/// Counts since start, for the logs.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Stats {
    /// Objects finished `done`.
    pub done: u64,
    /// Objects finished `skipped`.
    pub skipped: u64,
    /// Objects finished `failed`.
    pub failed: u64,
    /// Analyses run (the rest reused a stored record).
    pub analyzed: u64,
    /// Bytes fetched.
    pub bytes: u64,
    /// Locations ended by deletes.
    pub deleted: u64,
    /// `party` edges written to the survivor of a merge that committed after the match
    /// (issue #48).
    pub redirected: u64,
    /// Analyses the watchdog gave up on (their files failed; each ran on until it returned).
    pub overruns: u64,
    /// Finished objects whose status matched no `pending` row of that version, so the result
    /// was not recorded: the object changed or was deleted meanwhile. (Until issue #125 an
    /// event could also come before its row was committed; the sweep then ingested it again.)
    pub finish_unmatched: u64,
    /// Where the workers' time went, summed over files (issue #69).
    pub time: StageTimes,
}

/// Time the ingest workers spent per stage, summed over all files and attempts. The stages
/// run one after another per file, so with `jobs` workers their sum is at most `jobs` times
/// the wall time; the rest is waiting for work.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct StageTimes {
    /// Instance-store calls: the status check, the source and settings, the final status.
    pub store: Duration,
    /// Fetching the object.
    pub fetch: Duration,
    /// Hashing and analyzing (quail, hedgehog, OCR) on blocking threads.
    pub analyze: Duration,
    /// Reading and writing the record in the shared store.
    pub record: Duration,
    /// Writing the graph and search (the company lock's wait and hold included).
    pub graph: Duration,
    /// Of `graph`: waiting for the instance's company-resolution lock.
    pub company_lock_wait: Duration,
    /// Of `graph`: holding it.
    pub company_lock_hold: Duration,
}

/// Analyses running on blocking threads, and the bytes of their files.
#[derive(Debug, Default)]
struct Analyzing {
    files: AtomicUsize,
    bytes: AtomicU64,
}

/// One analysis counted in [`Analyzing`] until it is dropped, on its blocking thread, when the
/// analysis returns (with its file's bytes already freed).
struct Counted {
    of: Arc<Analyzing>,
    bytes: u64,
}

impl Counted {
    fn new(of: &Arc<Analyzing>, bytes: usize) -> Self {
        let bytes = bytes as u64;
        of.files.fetch_add(1, Ordering::SeqCst);
        of.bytes.fetch_add(bytes, Ordering::SeqCst);
        Self {
            of: Arc::clone(of),
            bytes,
        }
    }
}

impl Drop for Counted {
    fn drop(&mut self) {
        self.of.files.fetch_sub(1, Ordering::SeqCst);
        self.of.bytes.fetch_sub(self.bytes, Ordering::SeqCst);
    }
}

/// The memory budget: bytes of big files held at once, and the big files waiting for it.
#[derive(Debug)]
struct Budget {
    total: u64,
    small: u64,
    /// Waiting big files beyond which the workers stop taking new jobs and wait for the budget.
    max_waiting: usize,
    state: Mutex<BudgetState>,
    released: Notify,
}

#[derive(Debug)]
struct BudgetState {
    free: u64,
    /// Big files put aside until their share is free, oldest first.
    waiting: VecDeque<Job>,
}

/// A big file's share of the budget. Held by its worker and by its analysis (which may outlive
/// the watchdog); the share is free again when the last one drops it.
#[derive(Debug)]
struct Charge {
    budget: Arc<Budget>,
    bytes: u64,
}

impl Drop for Charge {
    fn drop(&mut self) {
        lock(&self.budget.state).free += self.bytes;
        self.budget.released.notify_waiters();
    }
}

impl Budget {
    fn new(total: u64, small: u64, max_waiting: usize) -> Self {
        let total = total.max(1);
        Self {
            total,
            small,
            max_waiting,
            state: Mutex::new(BudgetState {
                free: total,
                waiting: VecDeque::new(),
            }),
            released: Notify::new(),
        }
    }

    /// What a job costs: `None` for a small file or a delete (not charged), else its listed
    /// size, at most the whole budget.
    fn cost(&self, job: &Job) -> Option<u64> {
        match job {
            Job::Ingest { size, .. } if *size > self.small => Some((*size).min(self.total)),
            _ => None,
        }
    }

    /// Admits a big job if nothing waits before it and its share is free; else puts it at the
    /// end of the waiting list.
    fn offer(self: &Arc<Self>, job: Job, cost: u64) -> Option<(Job, Arc<Charge>)> {
        let mut st = lock(&self.state);
        if st.waiting.is_empty() && st.free >= cost {
            st.free -= cost;
            drop(st);
            return Some((job, self.charge(cost)));
        }
        st.waiting.push_back(job);
        None
    }

    /// The oldest waiting job, if its share is free now.
    fn next_waiting(self: &Arc<Self>) -> Option<(Job, Arc<Charge>)> {
        let mut st = lock(&self.state);
        let cost = self.cost(st.waiting.front()?).unwrap_or(0);
        if st.free < cost {
            return None;
        }
        st.free -= cost;
        let job = st.waiting.pop_front()?;
        drop(st);
        Some((job, self.charge(cost)))
    }

    fn charge(self: &Arc<Self>, bytes: u64) -> Arc<Charge> {
        Arc::new(Charge {
            budget: Arc::clone(self),
            bytes,
        })
    }

    /// Whether so many big files wait that the workers wait for the budget, not for new jobs.
    fn full(&self) -> bool {
        lock(&self.state).waiting.len() > self.max_waiting
    }

    /// Bytes charged now.
    #[cfg(test)]
    fn in_use(&self) -> u64 {
        self.total - lock(&self.state).free
    }
}

/// A worker's analyses that overran the watchdog and still run (each awaited by a task that
/// logs when it ends), oldest first.
type Overruns = VecDeque<JoinHandle<()>>;

/// Called with the key on the blocking thread before an analysis.
#[cfg(test)]
type Hook = Arc<dyn Fn(&str) + Send + Sync>;

/// What woke the dispatcher.
enum Wake {
    Stop,
    Event(Option<ObjectEvent>),
    Sweep,
}

#[derive(Clone, Debug)]
enum Job {
    Ingest {
        source: SourceId,
        key: String,
        etag: Option<String>,
        /// The listed size, charged to the memory budget.
        size: u64,
    },
    Delete {
        source: SourceId,
        key: String,
    },
}

/// The running tasks.
pub struct IngestHandle {
    dispatcher: JoinHandle<()>,
    workers: Vec<JoinHandle<()>>,
}

impl IngestHandle {
    /// Waits for the dispatcher and the workers (after shutdown, each finishes its file).
    pub async fn join(self) {
        let _ = self.dispatcher.await;
        for w in self.workers {
            let _ = w.await;
        }
    }
}

impl Ingest {
    /// The service; [`start`](Self::start) runs it.
    pub fn new(parts: IngestParts) -> Self {
        let c = &parts.config;
        let budget = Arc::new(Budget::new(c.memory_budget, c.small_file, c.jobs.max(1)));
        Self {
            inner: Arc::new(Inner {
                store: parts.store,
                backend: parts.backend,
                fetch: parts.fetch,
                analyzer: parts.analyzer,
                config: parts.config,
                on_change: parts.on_change,
                on_llm_queued: parts.on_llm_queued,
                writer: Mutex::new(None),
                inflight: Mutex::new(HashSet::new()),
                analyzing: Arc::default(),
                budget,
                #[cfg(test)]
                hook: None,
                sweep_now: Notify::new(),
                jobs: Mutex::new(HashMap::new()),
                started: Instant::now(),
                stats: Mutex::new(Stats::default()),
            }),
        }
    }

    /// The configuration.
    pub fn config(&self) -> &IngestConfig {
        &self.inner.config
    }

    /// Counts since start.
    pub fn stats(&self) -> Stats {
        *lock(&self.inner.stats)
    }

    /// Reads [`stats`](Self::stats) without keeping ingest (and its store) alive, for metrics
    /// that outlive it; `None` once ingest is gone.
    pub fn stats_reader(&self) -> impl Fn() -> Option<Stats> + Send + Sync + 'static {
        let inner = Arc::downgrade(&self.inner);
        move || inner.upgrade().map(|i| *lock(&i.stats))
    }

    /// Starts the dispatcher (events and sweeps) and the workers.
    pub fn start(
        &self,
        events: mpsc::Receiver<ObjectEvent>,
        shutdown: watch::Receiver<bool>,
    ) -> IngestHandle {
        let c = &self.inner.config;
        let (tx, rx) = mpsc::channel::<Job>(c.queue.max(1));
        let rx = Arc::new(tokio::sync::Mutex::new(rx));
        let workers = (0..c.jobs)
            .map(|_| {
                let me = self.clone();
                let rx = Arc::clone(&rx);
                let shutdown = shutdown.clone();
                tokio::spawn(async move { me.worker(rx, shutdown).await })
            })
            .collect();
        info!(
            jobs = c.jobs,
            queue = c.queue,
            memory_budget_mib = c.memory_budget >> 20,
            sheet_max_cells = c.analyzer.sheet_max_cells,
            cpu_slots = self.inner.analyzer.cpus().total(),
            ocr = ?self.inner.analyzer.ocr(),
            "ingest started"
        );
        let me = self.clone();
        let dispatcher = tokio::spawn(async move { me.dispatch(events, tx, shutdown).await });
        IngestHandle {
            dispatcher,
            workers,
        }
    }

    /// Asks for a sweep of the pending queue now.
    pub fn sweep_soon(&self) {
        self.inner.sweep_now.notify_one();
    }

    async fn dispatch(
        &self,
        mut events: mpsc::Receiver<ObjectEvent>,
        tx: mpsc::Sender<Job>,
        mut shutdown: watch::Receiver<bool>,
    ) {
        let mut timer = tokio::time::interval(self.inner.config.sweep_interval);
        timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut events_open = true;
        loop {
            let wake = tokio::select! {
                biased;
                _ = shutdown.wait_for(|s| *s) => Wake::Stop,
                e = events.recv(), if events_open => Wake::Event(e),
                _ = timer.tick() => Wake::Sweep,
                () = self.inner.sweep_now.notified() => Wake::Sweep,
            };
            let stop = match wake {
                Wake::Stop => true,
                Wake::Event(Some(e)) => self.on_event(e, &tx, &mut shutdown).await,
                Wake::Event(None) => {
                    events_open = false;
                    false
                }
                Wake::Sweep => self.sweep(&tx, &mut shutdown).await,
            };
            if stop {
                break;
            }
        }
        let s = self.stats();
        info!(
            done = s.done,
            skipped = s.skipped,
            failed = s.failed,
            analyzed = s.analyzed,
            redirected = s.redirected,
            finish_unmatched = s.finish_unmatched,
            mib = s.bytes / (1 << 20),
            secs = self.inner.started.elapsed().as_secs(),
            time = ?s.time,
            "ingest dispatcher stopped"
        );
    }

    /// Queues the job for an event. Returns true on shutdown.
    async fn on_event(
        &self,
        e: ObjectEvent,
        tx: &mpsc::Sender<Job>,
        shutdown: &mut watch::Receiver<bool>,
    ) -> bool {
        let job = match e.kind {
            EventKind::New(v) | EventKind::Changed(v) => Job::Ingest {
                source: e.source,
                key: e.key,
                etag: v.etag,
                size: v.size,
            },
            EventKind::Deleted => Job::Delete {
                source: e.source,
                key: e.key,
            },
        };
        self.enqueue(job, tx, shutdown).await
    }

    /// Hands a job to the workers, waiting while the queue is full. An object already in
    /// flight is not queued twice. Returns true on shutdown.
    async fn enqueue(
        &self,
        job: Job,
        tx: &mpsc::Sender<Job>,
        shutdown: &mut watch::Receiver<bool>,
    ) -> bool {
        if let Job::Ingest { source, key, .. } = &job
            && !lock(&self.inner.inflight).insert((*source, key.clone()))
        {
            return false;
        }
        tokio::select! {
            biased;
            _ = shutdown.wait_for(|s| *s) => true,
            sent = tx.send(job) => sent.is_err(),
        }
    }

    /// Queues every pending object of every source. Returns true on shutdown.
    async fn sweep(&self, tx: &mpsc::Sender<Job>, shutdown: &mut watch::Receiver<bool>) -> bool {
        let sources = match self.db(Store::sources).await {
            Ok(s) => s,
            Err(e) => {
                warn!(error = %e, "ingest sweep: cannot list the sources");
                return false;
            }
        };
        for source in sources
            .into_iter()
            .filter(|s| s.state != SourceState::Removed)
        {
            let mut after: Option<String> = None;
            loop {
                let (id, from) = (source.id, after.clone());
                let page = match self
                    .db(move |s| s.pending_objects(id, from.as_deref(), SWEEP_PAGE))
                    .await
                {
                    Ok(p) => p,
                    Err(e) => {
                        warn!(source = %source.id, error = %e, "ingest sweep failed");
                        break;
                    }
                };
                let full = page.len() == SWEEP_PAGE;
                after = page.last().map(|o| o.key.clone());
                for o in page {
                    let job = Job::Ingest {
                        source: source.id,
                        key: o.key,
                        etag: o.etag,
                        size: o.size,
                    };
                    if self.enqueue(job, tx, shutdown).await {
                        return true;
                    }
                }
                if !full {
                    break;
                }
            }
        }
        false
    }

    async fn worker(
        &self,
        rx: Arc<tokio::sync::Mutex<mpsc::Receiver<Job>>>,
        mut shutdown: watch::Receiver<bool>,
    ) {
        let budget = Arc::clone(&self.inner.budget);
        let mut overruns = Overruns::new();
        loop {
            // Too many analyses left running: wait for the oldest before taking more work.
            overruns.retain(|h| !h.is_finished());
            while overruns.len() > MAX_OVERRUNS {
                let Some(oldest) = overruns.pop_front() else {
                    break;
                };
                info!(
                    running = overruns.len() + 1,
                    "ingest worker waits for an overrun analysis to end"
                );
                tokio::select! {
                    biased;
                    _ = shutdown.wait_for(|s| *s) => return,
                    _ = oldest => {}
                }
            }
            // Registered before the budget is looked at, so no release is missed.
            let released = budget.released.notified();
            tokio::pin!(released);
            released.as_mut().enable();
            // A big file whose share of the budget is free now goes first (oldest first).
            let (job, charge) = if let Some((job, charge)) = budget.next_waiting() {
                (job, Some(charge))
            } else if budget.full() {
                // Too many big files wait: wait for the budget, not for new work.
                tokio::select! {
                    biased;
                    _ = shutdown.wait_for(|s| *s) => return,
                    () = &mut released => continue,
                }
            } else {
                let job = {
                    let mut rx = rx.lock().await;
                    tokio::select! {
                        biased;
                        _ = shutdown.wait_for(|s| *s) => None,
                        () = &mut released => continue,
                        j = rx.recv() => j,
                    }
                };
                let Some(job) = job else { return };
                match budget.cost(&job) {
                    None => (job, None),
                    Some(cost) => {
                        let Some((job, charge)) = budget.offer(job, cost) else {
                            // Put aside until its share is free; this worker takes other work.
                            debug!(cost, "a big file waits for the ingest memory budget");
                            continue;
                        };
                        (job, Some(charge))
                    }
                }
            };
            match job {
                Job::Ingest {
                    source, key, etag, ..
                } => {
                    self.run_ingest(
                        source,
                        &key,
                        etag.as_deref(),
                        charge,
                        &mut shutdown,
                        &mut overruns,
                    )
                    .await;
                    lock(&self.inner.inflight).remove(&(source, key));
                    (self.inner.on_change)(source);
                }
                Job::Delete { source, key } => {
                    self.run_delete(source, &key, &mut shutdown).await;
                    (self.inner.on_change)(source);
                }
            }
        }
    }

    /// Ingests one object version, with retries, and records how it ended. `charge` is the
    /// file's share of the memory budget (`None`: a small file), held until it returns and by
    /// its analysis until that ends.
    async fn run_ingest(
        &self,
        source: SourceId,
        key: &str,
        etag: Option<&str>,
        charge: Option<Arc<Charge>>,
        shutdown: &mut watch::Receiver<bool>,
        overruns: &mut Overruns,
    ) {
        let max = self.inner.config.max_attempts.max(1);
        for attempt in 1..=max {
            let Some(backend) = self.backend(shutdown).await else {
                return; // shutdown: stays pending
            };
            let result = self
                .ingest_once(&backend, source, key, etag, charge.as_ref(), overruns)
                .await;
            let (k, e) = (key.to_owned(), etag.map(str::to_owned));
            let outcome = match result {
                Ok(Some(outcome)) => outcome,
                Ok(None) => return, // already through, or the source is gone
                Err(err) if err.transient && attempt < max => {
                    let delay = self.inner.config.retry_base * 4u32.saturating_pow(attempt - 1);
                    debug!(source = %source, key, attempt, error = %err, ?delay, "retrying");
                    let msg = err.message.clone();
                    let _ = self
                        .db(move |s| s.note_attempt(source, &k, e.as_deref(), attempt, &msg))
                        .await;
                    tokio::select! {
                        () = tokio::time::sleep(delay) => continue,
                        _ = shutdown.wait_for(|s| *s) => return,
                    }
                }
                Err(err) => IngestOutcome::Failed {
                    error: err.message,
                    attempts: attempt,
                },
            };
            {
                let mut s = lock(&self.inner.stats);
                match &outcome {
                    IngestOutcome::Done { .. } => s.done += 1,
                    IngestOutcome::Skipped { .. } => s.skipped += 1,
                    IngestOutcome::Failed { .. } => s.failed += 1,
                }
            }
            if let IngestOutcome::Failed { error, attempts } = &outcome {
                warn!(source = %source, key, attempts, error = %error, "ingest failed");
            }
            let (k, e) = (key.to_owned(), etag.map(str::to_owned));
            let t = Instant::now();
            let finished = self
                .db(move |s| s.finish_object(source, &k, e.as_deref(), &outcome))
                .await;
            lock(&self.inner.stats).time.store += t.elapsed();
            match finished {
                Ok(true) => {}
                Ok(false) => {
                    lock(&self.inner.stats).finish_unmatched += 1;
                    debug!(source = %source, key, "no pending row to finish");
                }
                Err(err) => {
                    warn!(source = %source, key, error = %err, "cannot record the ingest status");
                }
            }
            return;
        }
    }

    /// One attempt at the version `etag`. `Ok(None)`: nothing to record, as that version is
    /// already through (a duplicate event, a stale sweep page) or the source was removed
    /// meanwhile.
    async fn ingest_once(
        &self,
        backend: &Arc<dyn Backend>,
        id: SourceId,
        key: &str,
        etag: Option<&str>,
        charge: Option<&Arc<Charge>>,
        overruns: &mut Overruns,
    ) -> Result<Option<IngestOutcome>, IngestError> {
        // The object's status, its source and the runtime settings in one read on a read-only
        // connection, so it does not wait behind the writer's commits (issue #128). The
        // settings are read per file, so a change applies without a restart. The status needs
        // no read of our own writes: `inflight` holds the key until its attempt is recorded.
        let t = Instant::now();
        let k = key.to_owned();
        let (status, source, settings) = self
            .db(move |s| {
                s.snapshot(|s| {
                    Ok((
                        s.ingest_status(id, &k)?,
                        s.source(id)?,
                        s.ingest_settings()?,
                    ))
                })
            })
            .await?;
        let mut t = self.stage(t, |s| &mut s.store);
        if status.is_some_and(|st| st.etag.as_deref() == etag && st.status != "pending") {
            return Ok(None);
        }
        let Some(source) = source else {
            return Ok(None);
        };
        let bytes = self.inner.fetch.fetch(&source, key).await?;
        lock(&self.inner.stats).bytes += bytes.len() as u64;
        t = self.stage(t, |s| &mut s.fetch);
        let location = ObjectLocation {
            endpoint: source.endpoint.clone(),
            bucket: source.bucket.clone(),
            key: key.to_owned(),
        };
        let name = key.rsplit('/').next().unwrap_or(key).to_owned();
        let (bytes, blake3) = hash(bytes).await?;
        t = self.stage(t, |s| &mut s.analyze);
        // Content already stored with the current step versions: reuse its record.
        let stored = backend
            .get_record(&blake3)
            .await?
            .filter(|r| !self.inner.analyzer.is_stale(&r.versions));
        t = self.stage(t, |s| &mut s.record);
        let (body, outcome) = if let Some(r) = stored {
            let outcome = stored_outcome(&r.body);
            (r.body, outcome)
        } else {
            let a = self
                .analyze(bytes, blake3, key, charge.cloned(), overruns)
                .await?;
            lock(&self.inner.stats).analyzed += 1;
            t = self.stage(t, |s| &mut s.analyze);
            backend.put_record(&a.blake3, &a.body, &a.versions).await?;
            t = self.stage(t, |s| &mut s.record);
            (a.body, a.outcome)
        };
        let facts = DocFacts::from_record(&body);
        let stats = self
            .writer(backend)
            .write(
                &blake3,
                &location,
                &facts,
                &name,
                settings.similar_min_jaccard,
            )
            .await?;
        self.stage(t, |s| &mut s.graph);
        {
            let mut s = lock(&self.inner.stats);
            s.time.company_lock_wait += stats.company_lock_wait;
            s.time.company_lock_hold += stats.company_lock_hold;
            s.redirected += u64::try_from(stats.redirects).unwrap_or(u64::MAX);
        }
        debug!(
            source = %id,
            key,
            blake3 = %blake3,
            format = %facts.format,
            parties = stats.parties,
            refs = stats.refs,
            similar = stats.similar,
            redirects = stats.redirects,
            "ingested"
        );
        // The record is stored: a summary, if the source's policy asks for one, comes later
        // through the LLM queue, and never fails this file.
        if outcome == Outcome::Analyzed && crate::llm::has_digest(&body) {
            let (store, kind) = (Arc::clone(&self.inner.store), facts.kind.clone());
            let queued = tokio::task::spawn_blocking(move || {
                crate::llm::queue_if_wanted(&store, &source, &blake3, &body, kind.as_deref())
            })
            .await
            .unwrap_or(false);
            if queued {
                (self.inner.on_llm_queued)(id);
            }
        }
        let blake3 = blake3.0;
        Ok(Some(match outcome {
            Outcome::Analyzed => IngestOutcome::Done { blake3 },
            Outcome::Unsupported => IngestOutcome::Skipped { blake3 },
            Outcome::Failed(error) => IngestOutcome::Failed { error, attempts: 1 },
        }))
    }

    /// Analyzes one file on a blocking thread, under the watchdog. Past it the file fails as
    /// before, and the analysis, which nothing can stop from outside, is added to `overruns`
    /// (the worker bounds those): its bytes and its share of the memory budget are freed, and a
    /// `WARN` logged, when it returns.
    async fn analyze(
        &self,
        bytes: Vec<u8>,
        blake3: Blake3,
        key: &str,
        charge: Option<Arc<Charge>>,
        overruns: &mut Overruns,
    ) -> Result<Analysis, IngestError> {
        let analyzer = Arc::clone(&self.inner.analyzer);
        let limit = analyzer.limits().timeout_cap() + self.inner.config.watchdog_grace;
        let counted = Counted::new(&self.inner.analyzing, bytes.len());
        let k = key.to_owned();
        #[cfg(test)]
        let hook = self.inner.hook.clone();
        let mut task = tokio::task::spawn_blocking(move || {
            let _counted = counted;
            let _charge = charge;
            #[cfg(test)]
            if let Some(h) = hook {
                h(&k);
            }
            analyzer.analyze_hashed(bytes, blake3, &k)
        });
        let started = Instant::now();
        match tokio::time::timeout(limit, &mut task).await {
            Ok(Ok(a)) => Ok(a),
            Ok(Err(e)) => Err(IngestError::permanent(format!("analysis panicked: {e}"))),
            Err(_) => {
                lock(&self.inner.stats).overruns += 1;
                let key = key.to_owned();
                let analyzing = Arc::clone(&self.inner.analyzing);
                overruns.push_back(tokio::spawn(async move {
                    let _ = task.await;
                    warn!(
                        key,
                        ran = ?started.elapsed(),
                        analyzing = analyzing.files.load(Ordering::SeqCst),
                        analyzing_bytes = analyzing.bytes.load(Ordering::SeqCst),
                        "an analysis the watchdog gave up on has ended; its memory is free"
                    );
                }));
                Err(IngestError::permanent(format!(
                    "analysis did not finish within {}s",
                    limit.as_secs()
                )))
            }
        }
    }

    /// Ends the location of a deleted object, with retries.
    async fn run_delete(&self, id: SourceId, key: &str, shutdown: &mut watch::Receiver<bool>) {
        let max = self.inner.config.max_attempts.max(1);
        for attempt in 1..=max {
            let Some(backend) = self.backend(shutdown).await else {
                return;
            };
            let result = async {
                let Some(source) = self.db(move |s| s.source(id)).await? else {
                    return Ok(None);
                };
                let at = ObjectLocation {
                    endpoint: source.endpoint,
                    bucket: source.bucket,
                    key: key.to_owned(),
                };
                let today = time::OffsetDateTime::now_utc().date();
                Ok::<_, IngestError>(backend.end_location(&at, today).await?)
            }
            .await;
            match result {
                Ok(ended) => {
                    if let Some(e) = ended {
                        lock(&self.inner.stats).deleted += 1;
                        debug!(source = %id, key, remaining = e.remaining, "location ended");
                    }
                    return;
                }
                Err(e) if e.transient && attempt < max => {
                    let delay = self.inner.config.retry_base * 4u32.saturating_pow(attempt - 1);
                    tokio::select! {
                        () = tokio::time::sleep(delay) => {}
                        _ = shutdown.wait_for(|s| *s) => return,
                    }
                }
                Err(e) => {
                    warn!(source = %id, key, error = %e, "cannot end the deleted object's location");
                    return;
                }
            }
        }
    }

    /// The backend, waiting for it while the shared Postgres is not connected. `None` on
    /// shutdown.
    async fn backend(&self, shutdown: &mut watch::Receiver<bool>) -> Option<Arc<dyn Backend>> {
        let mut rx = self.inner.backend.clone();
        loop {
            if let Some(b) = rx.borrow_and_update().clone() {
                return Some(b);
            }
            tokio::select! {
                changed = rx.changed() => if changed.is_err() { return None },
                _ = shutdown.wait_for(|s| *s) => return None,
            }
        }
    }

    /// Adds the time since `since` to one stage of [`Stats::time`]; returns now.
    fn stage(
        &self,
        since: Instant,
        field: impl FnOnce(&mut StageTimes) -> &mut Duration,
    ) -> Instant {
        let now = Instant::now();
        *field(&mut lock(&self.inner.stats).time) += now - since;
        now
    }

    /// The graph writer of this backend (one per backend, so company matching is serialized).
    fn writer(&self, backend: &Arc<dyn Backend>) -> Arc<GraphWriter> {
        let ptr = Arc::as_ptr(backend).cast::<()>() as usize;
        let mut w = lock(&self.inner.writer);
        if let Some((p, writer)) = w.as_ref()
            && *p == ptr
        {
            return Arc::clone(writer);
        }
        let writer = Arc::new(GraphWriter::new(
            Arc::clone(backend),
            self.inner.config.similarity,
        ));
        *w = Some((ptr, Arc::clone(&writer)));
        writer
    }

    async fn db<T: Send + 'static>(
        &self,
        f: impl FnOnce(&Store) -> Result<T, StoreError> + Send + 'static,
    ) -> Result<T, IngestError> {
        let store = Arc::clone(&self.inner.store);
        tokio::task::spawn_blocking(move || f(&store))
            .await
            .map_err(|e| IngestError::transient(format!("blocking task: {e}")))?
            .map_err(IngestError::from)
    }

    /// Starts a reprocess job: every ingested file of `source` whose stored step versions
    /// differ from the running engine's (all of them with `force`) goes back to the queue.
    /// Progress goes to the returned receiver; [`watch_job`](Self::watch_job) finds it again.
    pub fn reprocess(&self, source: SourceId, force: bool) -> watch::Receiver<ReprocessStatus> {
        let job = Uuid::now_v7();
        let (tx, rx) = watch::channel(ReprocessStatus {
            job,
            source,
            force,
            state: ReprocessState::Selecting,
            examined: 0,
            selected: 0,
            processed: 0,
            failed: 0,
            pending: 0,
            error: None,
        });
        {
            let mut jobs = lock(&self.inner.jobs);
            if jobs.len() >= MAX_JOBS {
                jobs.retain(|_, tx| {
                    !matches!(
                        tx.borrow().state,
                        ReprocessState::Done | ReprocessState::Failed
                    )
                });
            }
            jobs.insert(job, tx.clone());
        }
        let me = self.clone();
        tokio::spawn(async move {
            if let Err(e) = me.run_reprocess(&tx).await {
                tx.send_modify(|s| {
                    s.state = ReprocessState::Failed;
                    s.error = Some(e.message);
                });
            }
        });
        rx
    }

    /// A reprocess job's progress, if it is known.
    pub fn watch_job(&self, job: Uuid) -> Option<watch::Receiver<ReprocessStatus>> {
        lock(&self.inner.jobs)
            .get(&job)
            .map(watch::Sender::subscribe)
    }

    async fn run_reprocess(&self, tx: &watch::Sender<ReprocessStatus>) -> Result<(), IngestError> {
        let (id, force) = {
            let s = tx.borrow();
            (s.source, s.force)
        };
        if force {
            let n = self.db(move |s| s.requeue_objects(id, None)).await?;
            tx.send_modify(|s| {
                s.examined = n;
                s.selected = n;
            });
        } else {
            let backend = self
                .inner
                .backend
                .borrow()
                .clone()
                .ok_or_else(|| IngestError::transient("the shared store is not connected"))?;
            let mut after: Option<String> = None;
            loop {
                let from = after.clone();
                let page = self
                    .db(move |s| s.ingested_objects(id, from.as_deref(), REPROCESS_PAGE))
                    .await?;
                let full = page.len() == REPROCESS_PAGE;
                after = page.last().map(|(k, _)| k.clone());
                let examined = page.len() as u64;
                // One query per page, versions only (issue #75).
                let hashes: Vec<Blake3> = page.iter().map(|(_, h)| Blake3(*h)).collect();
                let versions = backend.record_versions(&hashes).await?;
                let stale: Vec<String> = page
                    .into_iter()
                    .filter(|(_, hash)| {
                        versions
                            .get(&Blake3(*hash))
                            .is_none_or(|v| self.inner.analyzer.is_stale(v))
                    })
                    .map(|(key, _)| key)
                    .collect();
                let n = if stale.is_empty() {
                    0
                } else {
                    self.db(move |s| s.requeue_objects(id, Some(&stale)))
                        .await?
                };
                tx.send_modify(|s| {
                    s.examined += examined;
                    s.selected += n;
                });
                if !full {
                    break;
                }
            }
        }
        tx.send_modify(|s| s.state = ReprocessState::Running);
        self.sweep_soon();
        (self.inner.on_change)(id);
        loop {
            let counts = self.db(move |s| s.object_counts(id)).await?;
            let done = counts.pending == 0;
            tx.send_modify(|s| {
                s.processed = counts.done + counts.skipped;
                s.failed = counts.failed;
                s.pending = counts.pending;
                if done {
                    s.state = ReprocessState::Done;
                }
            });
            if done {
                return Ok(());
            }
            tokio::time::sleep(self.inner.config.reprocess_poll).await;
        }
    }
}

/// How a stored record ended, for content reused without analysis.
/// The file's hash, on a blocking thread: a full pass over up to the size cap would stall
/// the runtime thread (and the API and the other workers on it).
pub(crate) async fn hash(bytes: Vec<u8>) -> Result<(Vec<u8>, Blake3), IngestError> {
    tokio::task::spawn_blocking(move || {
        let blake3 = content_hash(&bytes);
        (bytes, blake3)
    })
    .await
    .map_err(|e| IngestError::permanent(format!("hashing panicked: {e}")))
}

fn stored_outcome(body: &RawValue) -> Outcome {
    use crate::facts::{member, text};
    // Only `format` and `status` are read, the rest skipped (issue #98).
    let status = member(body, "status");
    if text(status.and_then(|s| member(s, "state"))).as_deref() == Some("failed") {
        return Outcome::Failed(
            text(status.and_then(|s| member(s, "error")))
                .map_or_else(|| "analysis failed".to_owned(), Cow::into_owned),
        );
    }
    match text(member(body, "format")).as_deref() {
        Some("unknown" | "zip" | "image") | None => Outcome::Unsupported,
        Some(_) => Outcome::Analyzed,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Condvar;

    use doewood_core::MasterKey;
    use doewood_store::{NewSource, ObjectChange, SeenObject, Standalone};
    use serde_json::json;

    use super::*;

    /// Objects by key.
    #[derive(Default)]
    struct Objects(Mutex<HashMap<String, Vec<u8>>>);

    #[async_trait]
    impl Fetch for Objects {
        async fn fetch(&self, _source: &Source, key: &str) -> Result<Vec<u8>, IngestError> {
            lock(&self.0)
                .get(key)
                .cloned()
                .ok_or_else(|| IngestError::permanent("no such key"))
        }
    }

    /// Holds the analyses of some keys until released: a step that does not stop on time.
    #[derive(Default)]
    struct Gate {
        held: Mutex<HashSet<String>>,
        changed: Condvar,
    }

    impl Gate {
        fn wait(&self, key: &str) {
            let mut held = lock(&self.held);
            let until = Instant::now() + Duration::from_mins(1);
            while held.contains(key) && Instant::now() < until {
                held = self
                    .changed
                    .wait_timeout(held, Duration::from_millis(50))
                    .unwrap_or_else(PoisonError::into_inner)
                    .0;
            }
        }

        fn release(&self, key: &str) {
            lock(&self.held).remove(key);
            self.changed.notify_all();
        }
    }

    struct Run {
        ingest: Ingest,
        store: Arc<Store>,
        source: SourceId,
        gate: Arc<Gate>,
        shutdown: watch::Sender<bool>,
        handle: IngestHandle,
        _events: mpsc::Sender<ObjectEvent>,
    }

    /// One worker, a 300 ms file timeout and a 200 ms watchdog grace; the keys in `held`
    /// stall on the gate. Every object is pending before the workers start, so the first
    /// sweep queues them in key order.
    fn run(objects: &[(&str, Vec<u8>)], held: &[&str]) -> Run {
        run_with(objects, held, 1, |_| {}, None)
    }

    /// [`run`] with `jobs` workers, the configuration changed by `tune`, and `seen` called
    /// on the blocking thread before each analysis (before the gate).
    fn run_with(
        objects: &[(&str, Vec<u8>)],
        held: &[&str],
        jobs: usize,
        tune: impl FnOnce(&mut IngestConfig),
        seen: Option<Hook>,
    ) -> Run {
        let store = Arc::new(Store::open_in_memory(&MasterKey::from_bytes([9; 32])).unwrap());
        let backend: Arc<dyn Backend> = Arc::new(Standalone::new(Arc::clone(&store)));
        let source = store
            .add_source(&NewSource::new("http://objects.test:9000", "docs"), None)
            .unwrap();
        let fetch = Objects::default();
        let mut changes = Vec::new();
        for (key, bytes) in objects {
            lock(&fetch.0).insert((*key).to_owned(), bytes.clone());
            changes.push(ObjectChange::Upsert(SeenObject {
                key: (*key).to_owned(),
                etag: Some(format!("{:x}", bytes.len())),
                size: bytes.len() as u64,
                last_modified: Some(1),
            }));
        }
        store.record_objects(source, &changes, None).unwrap();
        let mut config = IngestConfig::new(Some(jobs), Path::new("/nonexistent"));
        config.watchdog_grace = Duration::from_millis(200);
        config.analyzer.file_timeout = Duration::from_millis(300);
        config.analyzer.ocr_models = "/nonexistent/doewood-ocr-models".into();
        tune(&mut config);
        let analyzer = Arc::new(Analyzer::new(&config.analyzer).unwrap());
        let gate = Arc::new(Gate::default());
        lock(&gate.held).extend(held.iter().map(|k| (*k).to_owned()));
        let (_backend_tx, backend_rx) = watch::channel(Some(backend));
        let mut ingest = Ingest::new(IngestParts {
            store: Arc::clone(&store),
            backend: backend_rx,
            fetch: Arc::new(fetch),
            analyzer,
            config,
            on_change: Arc::new(|_| {}),
            on_llm_queued: Arc::new(|_| {}),
        });
        let g = Arc::clone(&gate);
        Arc::get_mut(&mut ingest.inner).unwrap().hook = Some(Arc::new(move |k| {
            if let Some(seen) = &seen {
                seen(k);
            }
            g.wait(k);
        }));
        let (events, events_rx) = mpsc::channel(16);
        let (shutdown, shutdown_rx) = watch::channel(false);
        let handle = ingest.start(events_rx, shutdown_rx);
        Run {
            ingest,
            store,
            source,
            gate,
            shutdown,
            handle,
            _events: events,
        }
    }

    impl Run {
        fn status(&self, key: &str) -> doewood_store::IngestStatus {
            self.store.ingest_status(self.source, key).unwrap().unwrap()
        }

        /// Analyses running and their bytes.
        fn analyzing(&self) -> (usize, u64) {
            let a = &self.ingest.inner.analyzing;
            (
                a.files.load(Ordering::SeqCst),
                a.bytes.load(Ordering::SeqCst),
            )
        }

        async fn until(&self, what: &str, f: impl Fn(&Self) -> bool) {
            let until = Instant::now() + Duration::from_secs(30);
            while !f(self) {
                assert!(Instant::now() < until, "timed out waiting: {what}");
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }

        async fn stop(self) {
            self.shutdown.send_replace(true);
            tokio::time::timeout(Duration::from_secs(10), self.handle.join())
                .await
                .expect("shutdown is prompt");
        }
    }

    fn hashed(bytes: &[u8]) -> u32 {
        let h = blake3::hash(bytes);
        lock(&crate::analyze::HASHED)
            .get(h.as_bytes())
            .copied()
            .unwrap_or(0)
    }

    /// Distinct text per seed (no two tests share content: the hash spy is keyed by it).
    fn text(seed: u64, len: usize) -> Vec<u8> {
        let mut out = format!("seed {seed} pipeline test\n").into_bytes();
        let mut x = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
        while out.len() < len {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            let word = ["alpha", "beta", "gamma", "delta", "ugovor", "faktura"][(x % 6) as usize];
            out.extend_from_slice(word.as_bytes());
            out.push(if x.is_multiple_of(11) { b'\n' } else { b' ' });
        }
        out
    }

    /// Past the watchdog the file fails as before and the worker takes the next file at
    /// once; the stalled analysis keeps its bytes counted until it returns, then frees them.
    /// Each file is hashed exactly once.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn an_overrun_fails_its_file_and_frees_its_bytes_when_it_ends() {
        let (stuck, next) = (text(1001, 4 << 20), text(1002, 2000));
        let r = run(
            &[("a-stuck.txt", stuck.clone()), ("b-next.txt", next.clone())],
            &["a-stuck.txt"],
        );
        r.until("the next file is done", |r| {
            r.status("b-next.txt").status == "done"
        })
        .await;
        let s = r.status("a-stuck.txt");
        assert_eq!(s.status, "failed");
        assert_eq!(
            s.last_error.as_deref(),
            Some("analysis did not finish within 0s")
        );
        assert_eq!(s.attempts, 1);
        // Moved on while the stalled analysis still holds its file.
        assert_eq!(r.analyzing(), (1, stuck.len() as u64));
        assert_eq!(r.ingest.stats().overruns, 1);
        assert_eq!(r.ingest.stats().analyzed, 1);

        r.gate.release("a-stuck.txt");
        r.until("the overrun analysis ends", |r| r.analyzing() == (0, 0))
            .await;
        // Hashed once each: in the pipeline, not again in the analyzer.
        assert_eq!(hashed(&stuck), 1);
        assert_eq!(hashed(&next), 1);
        // The record of the overrun file was never written: its status stays failed.
        assert_eq!(r.status("a-stuck.txt").status, "failed");
        r.stop().await;
    }

    /// A worker leaves at most one overrun behind: after a second one it waits for the
    /// first to end before it takes another file.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_worker_leaves_at_most_one_overrun_behind() {
        let (a, b, c) = (text(2001, 1 << 20), text(2002, 1 << 20), text(2003, 2000));
        let r = run(
            &[
                ("a-stuck.txt", a.clone()),
                ("b-stuck.txt", b.clone()),
                ("c-next.txt", c),
            ],
            &["a-stuck.txt", "b-stuck.txt"],
        );
        r.until("both stalled files failed", |r| {
            r.status("a-stuck.txt").status == "failed" && r.status("b-stuck.txt").status == "failed"
        })
        .await;
        assert_eq!(r.analyzing(), (2, (a.len() + b.len()) as u64));
        // Well past another watchdog period: the worker is waiting, the next file is not taken.
        tokio::time::sleep(Duration::from_secs(1)).await;
        assert_eq!(r.status("c-next.txt").status, "pending");
        assert_eq!(r.analyzing().0, 2);

        r.gate.release("a-stuck.txt");
        r.until("the next file is done", |r| {
            r.status("c-next.txt").status == "done"
        })
        .await;
        assert_eq!(r.analyzing(), (1, b.len() as u64));
        r.gate.release("b-stuck.txt");
        r.until("every analysis ended", |r| r.analyzing() == (0, 0))
            .await;
        assert_eq!(r.ingest.stats().overruns, 2);
        r.stop().await;
    }

    /// The memory budget: a big file waits while another holds the budget, until that one's
    /// analysis really ends (here: an overrun the watchdog gave up on), while small files flow
    /// on the other worker. A file bigger than the budget takes all of it.
    ///
    /// Only the gate decides that `a` overruns; every other file must finish well inside the
    /// watchdog, also on a slow, shared CI runner. So the files are small (sizes only matter
    /// against the budget) and the watchdog is 3 s, not the 500 ms of [`run`]: with 5 MiB
    /// for `b` and 500 ms, `b` itself overran on macOS and Windows (#119).
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_big_file_waits_for_the_budget_while_small_files_flow() {
        const KIB: usize = 1 << 10;
        const BUDGET: usize = 384 * KIB;
        let a = text(3001, 256 * KIB);
        let b = text(3002, 640 * KIB);
        let small: Vec<(String, Vec<u8>)> = (0..6)
            .map(|i| (format!("c-small-{i}.txt"), text(3100 + i, 2000)))
            .collect();
        let mut objects = vec![("a-big.txt", a.clone()), ("b-bigger.txt", b.clone())];
        objects.extend(small.iter().map(|(k, d)| (k.as_str(), d.clone())));
        // What was charged when each analysis started.
        let charged: Arc<Mutex<Vec<(String, u64)>>> = Arc::default();
        let budget: Arc<Mutex<Option<Arc<Budget>>>> = Arc::default();
        let seen: Hook = {
            let (charged, budget) = (Arc::clone(&charged), Arc::clone(&budget));
            Arc::new(move |k| {
                if let Some(b) = lock(&budget).as_ref() {
                    lock(&charged).push((k.to_owned(), b.in_use()));
                }
            })
        };
        let r = run_with(
            &objects,
            &["a-big.txt"],
            2,
            |c| {
                c.memory_budget = BUDGET as u64;
                c.small_file = 16 << 10;
                c.analyzer.file_timeout = Duration::from_secs(1);
                c.watchdog_grace = Duration::from_secs(2);
            },
            Some(seen),
        );
        *lock(&budget) = Some(Arc::clone(&r.ingest.inner.budget));

        // a holds 256 KiB past the watchdog; b (640 KiB, more than the whole budget) waits
        // for all of it, and the small files go through meanwhile.
        r.until("a failed and the small files are done", |r| {
            r.status("a-big.txt").status == "failed"
                && small.iter().all(|(k, _)| r.status(k).status == "done")
        })
        .await;
        tokio::time::sleep(Duration::from_millis(500)).await;
        assert_eq!(r.status("b-bigger.txt").status, "pending");
        assert_eq!(r.analyzing(), (1, a.len() as u64));
        assert_eq!(r.ingest.inner.budget.in_use(), a.len() as u64);

        r.gate.release("a-big.txt");
        r.until("b ended", |r| r.status("b-bigger.txt").status != "pending")
            .await;
        let s = r.status("b-bigger.txt");
        assert_eq!(s.status, "done", "b: {:?}", s.last_error);
        r.until("every share is free", |r| {
            r.ingest.inner.budget.in_use() == 0
        })
        .await;
        let charged = lock(&charged).clone();
        let at = |key: &str| charged.iter().find(|(k, _)| k == key).map(|(_, c)| *c);
        assert_eq!(at("a-big.txt"), Some(a.len() as u64));
        // b ran alone, with the whole budget; small files are never charged.
        assert_eq!(at("b-bigger.txt"), Some(BUDGET as u64));
        r.stop().await;
    }

    #[test]
    fn budget_is_fifo_and_small_files_are_free() {
        let job = |key: &str, size: u64| Job::Ingest {
            source: SourceId::generate(),
            key: key.to_owned(),
            etag: None,
            size,
        };
        let key = |j: &Job| match j {
            Job::Ingest { key, .. } => key.clone(),
            Job::Delete { .. } => unreachable!(),
        };
        let b = Arc::new(Budget::new(100, 10, 1));
        assert_eq!(b.cost(&job("s", 10)), None);
        assert_eq!(b.cost(&job("m", 11)), Some(11));
        assert_eq!(b.cost(&job("h", 1000)), Some(100));
        let (_, first) = b.offer(job("first", 60), 60).expect("fits");
        // 50 does not fit beside 60: it waits, and so does a 20 behind it (no overtaking).
        assert!(b.offer(job("second", 50), 50).is_none());
        assert!(b.offer(job("third", 20), 20).is_none());
        assert!(b.full());
        assert!(b.next_waiting().is_none());
        drop(first);
        let (j, second) = b.next_waiting().expect("second's turn");
        assert_eq!(key(&j), "second");
        let (j, third) = b.next_waiting().expect("third fits beside it");
        assert_eq!(key(&j), "third");
        assert_eq!(b.in_use(), 70);
        drop((second, third));
        assert_eq!(b.in_use(), 0);
        assert!(!b.full());
    }

    #[test]
    fn budget_follows_jobs_and_memory() {
        const GIB: u64 = 1 << 30;
        // 4 workers with room for them: half of 4 x 512 MiB.
        assert_eq!(plan_budget(4, Some(8 * GIB)), GIB);
        assert_eq!(plan_budget(4, None), GIB);
        // A 4 GiB limit: 3 workers, 768 MiB.
        assert_eq!(plan_jobs(4, Some(4 * GIB - (100 << 20))), 3);
        assert_eq!(plan_budget(3, Some(4 * GIB - (100 << 20))), 768 << 20);
        // More workers than the memory holds: half of half the memory.
        assert_eq!(plan_budget(16, Some(4 * GIB)), GIB);
        assert_eq!(plan_budget(1, Some(GIB)), MIN_BUDGET);
        assert_eq!(plan_budget(0, Some(100 << 20)), MIN_BUDGET);
        assert!(IngestConfig::new(Some(2), Path::new("/x")).memory_budget >= MIN_BUDGET);
    }

    #[test]
    fn cell_cap_follows_jobs_and_memory() {
        const GIB: u64 = 1 << 30;
        // One worker's plan (512 MiB) when the memory is unknown, whatever the jobs.
        assert_eq!(plan_cells(4, None), 1_600_000);
        assert_eq!(plan_cells(1, None), 1_600_000);
        // 1 GiB a worker (half of 8 GiB over 4), 2 GiB a worker (half of 16 GiB over 4).
        assert_eq!(plan_cells(4, Some(8 * GIB)), 3_300_000);
        assert_eq!(plan_cells(4, Some(16 * GIB)), 6_700_000);
        // Twice the workers, half the cells each.
        assert_eq!(plan_cells(8, Some(16 * GIB)), 3_300_000);
        // The chart's 4 GiB limit with 3 workers.
        assert_eq!(plan_cells(3, Some(4 * GIB - (100 << 20))), 2_100_000);
        // Floor and ceiling.
        assert_eq!(plan_cells(4, Some(GIB)), crate::sheet::MIN_AUTO_CELLS);
        assert_eq!(plan_cells(0, Some(100 << 20)), crate::sheet::MIN_AUTO_CELLS);
        assert_eq!(plan_cells(2, Some(128 * GIB)), 10_000_000);
        assert_eq!(crate::sheet::MAX_AUTO_CELLS, 10_000_000);
        let cells = IngestConfig::new(Some(2), Path::new("/x"))
            .analyzer
            .sheet_max_cells;
        assert!((1_600_000..=10_000_000).contains(&cells), "{cells}");
    }

    #[test]
    fn jobs_follow_cpus_and_memory() {
        assert_eq!(plan_jobs(8, None), 8);
        assert_eq!(plan_jobs(8, Some(64 << 30)), 8);
        // 4 GiB available: half of it at 512 MiB a worker.
        assert_eq!(plan_jobs(16, Some(4 << 30)), 4);
        assert_eq!(plan_jobs(16, Some(100 << 20)), 1);
        assert_eq!(plan_jobs(0, None), 1);
        assert_eq!(plan_jobs(512, None), MAX_AUTO_JOBS);
        assert!(default_jobs() >= 1);
    }

    #[test]
    fn errors_say_whether_to_retry() {
        let s3 = |status| S3Error::Status {
            op: "GetObject",
            status,
            code: None,
            message: None,
        };
        assert!(IngestError::from(s3(503)).transient);
        assert!(IngestError::from(s3(429)).transient);
        assert!(!IngestError::from(s3(404)).transient);
        assert!(!IngestError::from(S3Error::TooLarge { limit: 1 }).transient);
        assert!(IngestError::from(doewood_model::Error::Unavailable("x".into())).transient);
        assert!(!IngestError::from(doewood_model::Error::Invalid("x".into())).transient);
    }

    #[test]
    fn stored_records_keep_their_outcome() {
        let raw = |v: &serde_json::Value| doewood_model::canonical::to_canonical(v).unwrap();
        assert_eq!(
            stored_outcome(&raw(&json!({"format": "pdf", "status": {"state": "ok"}}))),
            Outcome::Analyzed
        );
        assert_eq!(
            stored_outcome(&raw(
                &json!({"format": "xlsx", "status": {"state": "partial"}})
            )),
            Outcome::Analyzed
        );
        assert_eq!(
            stored_outcome(&raw(
                &json!({"format": "unknown", "status": {"state": "ok"}})
            )),
            Outcome::Unsupported
        );
        assert_eq!(
            stored_outcome(&raw(
                &json!({"format": "pdf", "status": {"state": "failed", "error": "bad"}})
            )),
            Outcome::Failed("bad".into())
        );
        // Not a record, or members of the wrong type: as `Value` indexing read them.
        assert_eq!(stored_outcome(&raw(&json!(42))), Outcome::Unsupported);
        assert_eq!(
            stored_outcome(&raw(&json!(["failed", "pdf"]))),
            Outcome::Unsupported
        );
        assert_eq!(
            stored_outcome(&raw(&json!({"format": 7, "status": "failed"}))),
            Outcome::Unsupported
        );
        assert_eq!(
            stored_outcome(&raw(&json!({"status": {"state": "failed", "error": 1}}))),
            Outcome::Failed("analysis failed".into())
        );
    }
}
