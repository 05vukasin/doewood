//! The pipeline: scanner events → fetch → analyze → store, on a pool of workers.
//!
//! - **The queue is durable.** `objects_seen` rows with status `pending` are the work; events
//!   only make ingest look sooner. A sweep (at start, every [`IngestConfig::sweep_interval`],
//!   and after a reprocess request) feeds every pending object, so a restart, an event that
//!   raced its row's commit, or a source added while ingest was busy, are all picked up.
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
//! - **Shutdown** lets every worker finish the file in hand; pending objects stay pending.

use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use doewood_core::SourceId;
use doewood_model::{Backend, Blake3, ObjectLocation};
use doewood_s3::{ClientOptions, EventKind, ObjectEvent, S3Client, S3Config, S3Error};
use doewood_store::{IngestOutcome, Source, SourceState, Store, StoreError};
use serde_json::Value as Json;
use tokio::sync::{Notify, mpsc, watch};
use tokio::task::JoinHandle;
use tracing::{debug, info, warn};
use uuid::Uuid;

use crate::analyze::{Analyzer, AnalyzerConfig, Outcome};
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
    /// `similar_to` edges.
    pub similarity: SimilarityConfig,
    /// The engine.
    pub analyzer: AnalyzerConfig,
}

impl IngestConfig {
    /// The configuration for an instance: `jobs` from `DOEWOOD_INGEST_JOBS` (`None`: from the
    /// CPUs and memory) and the OCR models directory. Tuning values such as the `similar_to`
    /// threshold are runtime settings, read from the store for every file.
    pub fn new(jobs: Option<usize>, ocr_models: &Path) -> Self {
        let jobs = jobs.unwrap_or_else(default_jobs).max(1);
        Self {
            jobs,
            queue: jobs * 2,
            max_attempts: 3,
            retry_base: Duration::from_secs(2),
            sweep_interval: Duration::from_secs(30),
            reprocess_poll: Duration::from_millis(500),
            similarity: SimilarityConfig::default(),
            analyzer: AnalyzerConfig {
                workers: jobs,
                ocr_models: ocr_models.to_owned(),
                ..AnalyzerConfig::default()
            },
        }
    }
}

/// Workers for this machine: one per CPU, at most what half the available memory holds at
/// [`WORKER_BYTES`] each, between 1 and [`MAX_AUTO_JOBS`].
pub fn default_jobs() -> usize {
    let cpus = std::thread::available_parallelism().map_or(1, std::num::NonZero::get);
    plan_jobs(cpus, available_memory())
}

/// [`default_jobs`] for `cpus` and `memory` bytes available (`None`: unknown).
pub fn plan_jobs(cpus: usize, memory: Option<u64>) -> usize {
    let by_memory = memory.map_or(cpus, |m| {
        usize::try_from(m / MEMORY_SHARE / WORKER_BYTES).unwrap_or(usize::MAX)
    });
    cpus.min(by_memory).clamp(1, MAX_AUTO_JOBS)
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
}

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
            mib = s.bytes / (1 << 20),
            secs = self.inner.started.elapsed().as_secs(),
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
        loop {
            let job = {
                let mut rx = rx.lock().await;
                tokio::select! {
                    biased;
                    _ = shutdown.wait_for(|s| *s) => None,
                    j = rx.recv() => j,
                }
            };
            let Some(job) = job else { return };
            match job {
                Job::Ingest { source, key, etag } => {
                    self.run_ingest(source, &key, etag.as_deref(), &mut shutdown)
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

    /// Ingests one object version, with retries, and records how it ended.
    async fn run_ingest(
        &self,
        source: SourceId,
        key: &str,
        etag: Option<&str>,
        shutdown: &mut watch::Receiver<bool>,
    ) {
        // Already through (a duplicate event, a stale sweep page): nothing to do.
        let (k, e) = (key.to_owned(), etag.map(str::to_owned));
        if let Ok(Some(st)) = self.db(move |s| s.ingest_status(source, &k)).await
            && st.etag == e
            && st.status != "pending"
        {
            return;
        }
        let max = self.inner.config.max_attempts.max(1);
        for attempt in 1..=max {
            let Some(backend) = self.backend(shutdown).await else {
                return; // shutdown: stays pending
            };
            let result = self.ingest_once(&backend, source, key).await;
            let (k, e) = (key.to_owned(), etag.map(str::to_owned));
            let outcome = match result {
                Ok(Some(outcome)) => outcome,
                Ok(None) => return, // the source is gone
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
            if let Err(err) = self
                .db(move |s| s.finish_object(source, &k, e.as_deref(), &outcome))
                .await
            {
                warn!(source = %source, key, error = %err, "cannot record the ingest status");
            }
            return;
        }
    }

    /// One attempt. `Ok(None)`: the source was removed meanwhile.
    async fn ingest_once(
        &self,
        backend: &Arc<dyn Backend>,
        id: SourceId,
        key: &str,
    ) -> Result<Option<IngestOutcome>, IngestError> {
        // The runtime settings are read per file, so a change applies without a restart.
        let (source, settings) = self
            .db(move |s| Ok((s.source(id)?, s.ingest_settings()?)))
            .await?;
        let Some(source) = source else {
            return Ok(None);
        };
        let bytes = self.inner.fetch.fetch(&source, key).await?;
        lock(&self.inner.stats).bytes += bytes.len() as u64;
        let location = ObjectLocation {
            endpoint: source.endpoint.clone(),
            bucket: source.bucket.clone(),
            key: key.to_owned(),
        };
        let name = key.rsplit('/').next().unwrap_or(key).to_owned();
        let blake3 = Blake3(*blake3::hash(&bytes).as_bytes());
        // Content already stored with the current step versions: reuse its record.
        let stored = backend
            .get_record(&blake3)
            .await?
            .filter(|r| !self.inner.analyzer.is_stale(&r.versions));
        let (body, outcome) = if let Some(r) = stored {
            let outcome = stored_outcome(&r.body);
            (r.body, outcome)
        } else {
            let analyzer = Arc::clone(&self.inner.analyzer);
            let k = key.to_owned();
            let limit = analyzer.limits().timeout_cap() + Duration::from_mins(1);
            let task = tokio::task::spawn_blocking(move || analyzer.analyze(bytes, &k));
            let a = match tokio::time::timeout(limit, task).await {
                Ok(Ok(a)) => a,
                Ok(Err(e)) => {
                    return Err(IngestError::permanent(format!("analysis panicked: {e}")));
                }
                Err(_) => {
                    return Err(IngestError::permanent(format!(
                        "analysis did not finish within {}s",
                        limit.as_secs()
                    )));
                }
            };
            lock(&self.inner.stats).analyzed += 1;
            backend.put_record(&a.blake3, &a.body, &a.versions).await?;
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
        debug!(
            source = %id,
            key,
            blake3 = %blake3,
            format = %facts.format,
            parties = stats.parties,
            refs = stats.refs,
            similar = stats.similar,
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
                let mut stale = Vec::new();
                for (key, hash) in page {
                    let current = backend
                        .get_record(&Blake3(hash))
                        .await?
                        .is_some_and(|r| !self.inner.analyzer.is_stale(&r.versions));
                    if !current {
                        stale.push(key);
                    }
                }
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
fn stored_outcome(body: &Json) -> Outcome {
    if body["status"]["state"] == "failed" {
        return Outcome::Failed(
            body["status"]["error"]
                .as_str()
                .unwrap_or("analysis failed")
                .to_owned(),
        );
    }
    match body["format"].as_str() {
        Some("unknown" | "zip" | "image") | None => Outcome::Unsupported,
        Some(_) => Outcome::Analyzed,
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

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
        assert_eq!(
            stored_outcome(&json!({"format": "pdf", "status": {"state": "ok"}})),
            Outcome::Analyzed
        );
        assert_eq!(
            stored_outcome(&json!({"format": "xlsx", "status": {"state": "partial"}})),
            Outcome::Analyzed
        );
        assert_eq!(
            stored_outcome(&json!({"format": "unknown", "status": {"state": "ok"}})),
            Outcome::Unsupported
        );
        assert_eq!(
            stored_outcome(
                &json!({"format": "pdf", "status": {"state": "failed", "error": "bad"}})
            ),
            Outcome::Failed("bad".into())
        );
    }
}
