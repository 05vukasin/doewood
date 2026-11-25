//! LLM summaries (ADR 0001, section 6): the queue and on-demand `Summarize`.
//!
//! - **Ingest stores the record first.** When a source's policy asks for a summary of a
//!   document (`all`, or `kinds` naming its kind), ingest queues it in the store
//!   (`llm_jobs`) after the record, the graph and search are written, and wakes the queue.
//!   `off` (the default) and `on_demand` queue nothing, so nothing is sent anywhere.
//! - **The queue** reads the pending jobs, oldest first, and summarizes each from its stored
//!   record: quail's `summary.digest` (kept in every document's record) is what the model
//!   reads, through quail's [`ApiSummarizer`] with its prompt, its fact check and its
//!   extractive fallback. The summary goes into the record as quail's `summary.llm` fact.
//! - **Caps.** At most [`LlmPolicy::concurrency`] summaries of a source are in flight, and at
//!   most [`QueueConfig::max_concurrent`] provider calls in all. With a daily token budget,
//!   no summary of the source starts once the tokens (prompt plus completion, as the provider
//!   reports them, else estimated) it spent on the current UTC day reach the budget; summaries
//!   already in flight finish, so a day can end up to `concurrency - 1` summaries over. The
//!   jobs wait, and go on the next UTC day.
//! - **Soft failures.** A failing or slow provider never fails or blocks ingest: the job
//!   ends `failed` with the provider's error (keys redacted), the record stays without a
//!   summary, and the error shows in the source's progress. Without a configured provider the
//!   jobs wait.
//! - **Providers** are the runtime settings `ConfigService.SetAiProvider` stores (preset,
//!   model, base URL, the API key sealed in the store). The queue uses the default provider.
//!   The process environment is never read (ADR 0001, section 3).
//! - **`Summarize`** ([`Summaries::summarize`]) skips the queue and stores nothing: it reads
//!   the file (the bytes given, or an object of a source whose policy is not `off`), analyzes
//!   it without storing the record, and summarizes it with the named or default provider.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::{Duration, Instant};

use doewood_core::SourceId;
use doewood_model::{Backend, Blake3};
use doewood_store::{LlmJob, LlmJobStatus, LlmPolicy, Source, Store, StoreError, utc_day};
use quail_core::{AnalyzerId, Evidence, Fact, FactKey, Value};
use quail_summary::api::{Connect, Limiter};
use quail_summary::extractive::SubjectMethod;
use quail_summary::summarizer::checked_or_fallback;
use quail_summary::{
    ApiConfig, ApiSummarizer, Endpoint, Env, FileCard, Method, Secret, Subject, Summarizer,
    Summary, SummaryInput, SummaryLlm, factcheck, keys, prompt, tokens,
};
use serde::Deserialize;
use serde_json::Value as Json;
use tokio::sync::{Notify, watch};
use tokio::task::JoinHandle;
use tracing::{debug, info, warn};

use crate::analyze::Analyzer;
use crate::facts::DocFacts;
use crate::pipeline::Fetch;

/// The record key of a summary: quail's `summary.llm`.
pub const SUMMARY_KEY: &str = "summary.llm";

/// Where the time comes from: the budget's UTC day. Tests inject their own.
pub trait Clock: Send + Sync {
    /// Unix seconds now.
    fn unix_now(&self) -> i64;
}

/// The system clock.
#[derive(Clone, Copy, Debug, Default)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn unix_now(&self) -> i64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| i64::try_from(d.as_secs()).unwrap_or(i64::MAX))
    }
}

/// Tuning of the queue.
#[derive(Clone, Debug)]
pub struct QueueConfig {
    /// How often the queue looks for pending jobs when nothing woke it.
    pub sweep_interval: Duration,
    /// Pending jobs read per pass.
    pub batch: u32,
    /// Provider calls in flight at most, over every source and `Summarize`.
    pub max_concurrent: usize,
    /// quail's API settings: output tokens, timeouts, retries. Its limiter is replaced by the
    /// queue's own ([`Self::max_concurrent`]) and its training log is never set.
    pub api: ApiConfig,
}

impl Default for QueueConfig {
    /// A pass every 30 s, 256 jobs a pass, 64 calls at once, quail's API defaults (60 s per
    /// attempt, 120 s per summary, 3 retries).
    fn default() -> Self {
        Self {
            sweep_interval: Duration::from_secs(30),
            batch: 256,
            max_concurrent: 64,
            api: ApiConfig::default(),
        }
    }
}

/// What the queue and `Summarize` are built from.
pub struct SummariesParts {
    /// The instance store: sources, providers, the queue and the spend.
    pub store: Arc<Store>,
    /// Records; `None` while the shared Postgres is not connected (jobs then wait).
    pub backend: watch::Receiver<Option<Arc<dyn Backend>>>,
    /// Where `Summarize` reads an object from.
    pub fetch: Arc<dyn Fetch>,
    /// The engine `Summarize` analyzes a file with.
    pub analyzer: Arc<Analyzer>,
    /// Tuning.
    pub config: QueueConfig,
    /// The budget's clock.
    pub clock: Arc<dyn Clock>,
    /// Called after a job ends (the progress counters).
    pub on_change: Arc<dyn Fn(SourceId) + Send + Sync>,
}

/// Why no summary could be made.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SummarizeError {
    /// No AI provider is configured, or the one named is incomplete.
    NoProvider(String),
    /// The named provider or the source does not exist.
    NotFound(String),
    /// The request cannot be served as it is (the source's policy is `off`, the file has no
    /// text).
    FailedPrecondition(String),
    /// Reading the object failed, or a dependency is away.
    Unavailable(String),
    /// Anything else.
    Internal(String),
}

impl std::fmt::Display for SummarizeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NoProvider(m)
            | Self::NotFound(m)
            | Self::FailedPrecondition(m)
            | Self::Unavailable(m)
            | Self::Internal(m) => f.write_str(m),
        }
    }
}

impl std::error::Error for SummarizeError {}

impl From<StoreError> for SummarizeError {
    fn from(e: StoreError) -> Self {
        Self::Internal(format!("instance store: {e}"))
    }
}

/// What `Summarize` reads.
#[derive(Clone, Debug)]
pub enum SummarizeInput {
    /// The file's bytes and its name.
    Content {
        /// The bytes.
        bytes: Vec<u8>,
        /// The file name (its extension is a hint for spreadsheet containers).
        name: String,
    },
    /// An object of one of this instance's sources.
    Location {
        /// The source.
        source: SourceId,
        /// The object key.
        key: String,
    },
}

/// An on-demand summary.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct OnDemandSummary {
    /// The summary.
    pub digest: String,
    /// Claims of the model's output that were checked against the document.
    pub checked: u32,
    /// Of those, the ones the document supports.
    pub supported: u32,
    /// The claims it does not support (`kind:value`).
    pub unsupported: Vec<String>,
    /// The model was unavailable or failed the fact check: the digest is the extractive
    /// summary.
    pub extractive: bool,
    /// The provider name and model that wrote it; empty for an extractive summary.
    pub provider: String,
    /// The model.
    pub model: String,
    /// The file's BLAKE3, hex.
    pub blake3: String,
    /// Its kind, as quail classified it.
    pub kind: String,
    /// Why the model's output was not used, when it was not.
    pub warnings: Vec<String>,
}

/// The LLM queue and `Summarize`. Cheap to clone.
#[derive(Clone)]
pub struct Summaries {
    inner: Arc<Inner>,
}

struct Inner {
    store: Arc<Store>,
    backend: watch::Receiver<Option<Arc<dyn Backend>>>,
    fetch: Arc<dyn Fetch>,
    analyzer: Arc<Analyzer>,
    config: QueueConfig,
    clock: Arc<dyn Clock>,
    on_change: Arc<dyn Fn(SourceId) + Send + Sync>,
    limiter: Arc<Limiter>,
    providers: Mutex<HashMap<String, Arc<Provider>>>,
    wake: Notify,
    sched: Mutex<Sched>,
    calls: AtomicU64,
    /// The last reason the queue could not run, logged once until it changes.
    blocked: Mutex<Option<String>>,
}

#[derive(Default)]
struct Sched {
    running: HashSet<(SourceId, [u8; 32])>,
    per_source: HashMap<SourceId, u32>,
}

/// A provider as configured, and its summarizer.
struct Provider {
    name: String,
    endpoint: Endpoint,
    summarizer: Arc<ApiSummarizer>,
}

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

/// How a job ended.
enum JobEnd {
    /// Leave it pending (the shared store is away, shutdown).
    Wait,
    /// End it.
    End(LlmJobStatus, Option<String>, bool),
}

impl Summaries {
    /// The service; [`start`](Self::start) runs the queue.
    pub fn new(parts: SummariesParts) -> Self {
        let limiter = Arc::new(Limiter::new(parts.config.max_concurrent));
        Self {
            inner: Arc::new(Inner {
                store: parts.store,
                backend: parts.backend,
                fetch: parts.fetch,
                analyzer: parts.analyzer,
                config: parts.config,
                clock: parts.clock,
                on_change: parts.on_change,
                limiter,
                providers: Mutex::new(HashMap::new()),
                wake: Notify::new(),
                sched: Mutex::new(Sched::default()),
                calls: AtomicU64::new(0),
                blocked: Mutex::new(None),
            }),
        }
    }

    /// Wakes the queue (a job was queued, a policy, a provider or the clock changed).
    pub fn notify(&self) {
        self.inner.wake.notify_one();
    }

    /// Provider calls made since start (queue and `Summarize`).
    pub fn calls(&self) -> u64 {
        self.inner.calls.load(Ordering::Relaxed)
    }

    /// Summaries in flight now.
    pub fn in_flight(&self) -> usize {
        lock(&self.inner.sched).running.len()
    }

    /// Runs the queue until shutdown. Summaries in flight then are abandoned (their jobs stay
    /// pending for the next start); nothing waits for a slow provider.
    pub fn start(&self, mut shutdown: watch::Receiver<bool>) -> JoinHandle<()> {
        let me = self.clone();
        tokio::spawn(async move {
            info!(
                max_concurrent = me.inner.config.max_concurrent,
                "LLM queue started"
            );
            let mut tasks = tokio::task::JoinSet::new();
            loop {
                if *shutdown.borrow() {
                    break;
                }
                me.pass(&mut tasks).await;
                while tasks.try_join_next().is_some() {}
                tokio::select! {
                    () = me.inner.wake.notified() => {}
                    () = tokio::time::sleep(me.inner.config.sweep_interval) => {}
                    _ = shutdown.wait_for(|s| *s) => break,
                }
            }
            tasks.abort_all();
            while tasks.join_next().await.is_some() {}
            info!("LLM queue stopped");
        })
    }

    fn blocked(&self, why: Option<String>) {
        let mut b = lock(&self.inner.blocked);
        if *b != why {
            match &why {
                Some(w) => warn!(reason = %w, "the LLM queue waits"),
                None if b.is_some() => info!("the LLM queue runs again"),
                None => {}
            }
            *b = why;
        }
    }

    async fn db<T: Send + 'static>(
        &self,
        f: impl FnOnce(&Store) -> Result<T, StoreError> + Send + 'static,
    ) -> Result<T, SummarizeError> {
        let store = Arc::clone(&self.inner.store);
        tokio::task::spawn_blocking(move || f(&store))
            .await
            .map_err(|e| SummarizeError::Internal(format!("blocking task: {e}")))?
            .map_err(SummarizeError::from)
    }

    /// One look at the pending jobs: starts every one its source's cap and budget allow.
    async fn pass(&self, tasks: &mut tokio::task::JoinSet<()>) {
        let batch = self.inner.config.batch.max(1);
        let jobs = match self.db(move |s| s.pending_llm_jobs(batch)).await {
            Ok(j) => j,
            Err(e) => {
                warn!(error = %e, "cannot read the LLM queue");
                return;
            }
        };
        if jobs.is_empty() {
            return;
        }
        let provider = match self.provider(None).await {
            Ok(p) => p,
            Err(e) => {
                self.blocked(Some(e.to_string()));
                return;
            }
        };
        self.blocked(None);
        let day = utc_day(self.inner.clock.unix_now());
        let mut sources: HashMap<SourceId, Option<Source>> = HashMap::new();
        for job in jobs {
            if !sources.contains_key(&job.source) {
                let id = job.source;
                let s = self.db(move |s| s.source(id)).await.ok().flatten();
                sources.insert(id, s);
            }
            let Some(Some(source)) = sources.get(&job.source).cloned() else {
                continue;
            };
            let policy = &source.llm_policy;
            if !policy.queues(job.kind.as_deref()) {
                let (id, b) = (job.source, job.blake3);
                let _ = self
                    .db(move |s| {
                        s.finish_llm_job(
                            id,
                            &b,
                            LlmJobStatus::Skipped,
                            Some("the source's LLM policy no longer asks for it"),
                            false,
                        )
                    })
                    .await;
                (self.inner.on_change)(id);
                continue;
            }
            {
                let sched = lock(&self.inner.sched);
                if sched.running.contains(&(job.source, job.blake3))
                    || sched.per_source.get(&job.source).copied().unwrap_or(0)
                        >= policy.concurrency.max(1)
                {
                    continue;
                }
            }
            if let Some(budget) = policy.daily_token_budget {
                let id = job.source;
                let spent = self
                    .db(move |s| s.llm_usage(id, day))
                    .await
                    .map_or(u64::MAX, |u| u.tokens);
                if spent >= budget {
                    debug!(source = %id, spent, budget, "daily LLM budget spent; jobs wait");
                    continue;
                }
            }
            {
                let mut sched = lock(&self.inner.sched);
                sched.running.insert((job.source, job.blake3));
                *sched.per_source.entry(job.source).or_insert(0) += 1;
            }
            tasks.spawn(self.clone().job_task(job, Arc::clone(&provider)));
        }
    }

    /// Runs one job, ends it in the store and frees its slot.
    async fn job_task(self, job: LlmJob, provider: Arc<Provider>) {
        let (id, b) = (job.source, job.blake3);
        let end = self.run(&job, &provider).await;
        if let JobEnd::End(status, error, called) = end {
            if status == LlmJobStatus::Failed {
                warn!(
                    source = %id,
                    blake3 = %Blake3(b),
                    error = error.as_deref().unwrap_or(""),
                    "LLM summary failed; the record stays without one"
                );
            }
            if let Err(e) = self
                .db(move |s| s.finish_llm_job(id, &b, status, error.as_deref(), called))
                .await
            {
                warn!(source = %id, error = %e, "cannot record the LLM job");
            }
        }
        {
            let mut sched = lock(&self.inner.sched);
            sched.running.remove(&(id, b));
            if let Some(n) = sched.per_source.get_mut(&id) {
                *n = n.saturating_sub(1);
                if *n == 0 {
                    sched.per_source.remove(&id);
                }
            }
        }
        (self.inner.on_change)(id);
        self.notify();
    }

    /// One job, from its stored record.
    async fn run(&self, job: &LlmJob, provider: &Arc<Provider>) -> JobEnd {
        // The pass read the job before an earlier run of it may have ended it.
        let (id, b) = (job.source, job.blake3);
        match self.db(move |s| s.llm_job(id, &b)).await {
            Ok(Some(j)) if j.status == LlmJobStatus::Pending => {}
            _ => return JobEnd::Wait,
        }
        let Some(backend) = self.inner.backend.borrow().clone() else {
            return JobEnd::Wait;
        };
        let blake3 = Blake3(job.blake3);
        let record = match backend.get_record(&blake3).await {
            Ok(Some(r)) => r,
            Ok(None) => {
                return JobEnd::End(
                    LlmJobStatus::Skipped,
                    Some("the record is gone".into()),
                    false,
                );
            }
            Err(e) => return JobEnd::End(LlmJobStatus::Failed, Some(e.to_string()), false),
        };
        if has_summary(&record.body) {
            return JobEnd::End(LlmJobStatus::Done, None, false);
        }
        let Some(prepared) = Prepared::from_record(&record.body) else {
            return JobEnd::End(
                LlmJobStatus::Skipped,
                Some("no text to summarize".into()),
                false,
            );
        };
        let called = self.call(Arc::clone(provider), prepared).await;
        let (summary, tokens) = match called {
            Ok(Called {
                summary, tokens, ..
            }) => (summary, tokens),
            Err(e) => {
                let id = job.source;
                let day = utc_day(self.inner.clock.unix_now());
                let _ = self.db(move |s| s.add_llm_usage(id, day, 0)).await;
                return JobEnd::End(LlmJobStatus::Failed, Some(e), true);
            }
        };
        let id = job.source;
        let day = utc_day(self.inner.clock.unix_now());
        if let Err(e) = self.db(move |s| s.add_llm_usage(id, day, tokens)).await {
            warn!(source = %id, error = %e, "cannot record the LLM spend");
        }
        let model = provider.summarizer.id().name;
        match write_summary(backend.as_ref(), &blake3, &summary, &model, tokens).await {
            Ok(()) => {
                debug!(source = %id, blake3 = %blake3, method = summary.method.as_str(), "summarized");
                let note = (summary.method != Method::Llm).then(|| summary.warnings.join("; "));
                JobEnd::End(LlmJobStatus::Done, note, true)
            }
            Err(e) => JobEnd::End(
                LlmJobStatus::Failed,
                Some(format!("cannot store the summary: {e}")),
                true,
            ),
        }
    }

    /// Asks the model on a blocking thread. `Err` is the provider's failure (no summary);
    /// an output that fails the fact check comes back as the extractive fallback.
    async fn call(&self, provider: Arc<Provider>, prepared: Prepared) -> Result<Called, String> {
        self.inner.calls.fetch_add(1, Ordering::Relaxed);
        let timeout = self.inner.config.api.timeout;
        tokio::task::spawn_blocking(move || {
            let input = prepared.input(Some(Instant::now() + timeout));
            let got = provider.summarizer.summarize(&input);
            let raw = match got {
                Ok(s) => s,
                Err(e) => return Err(format!("{}: {e}", provider.name)),
            };
            let tokens = if raw.usage.is_empty() {
                let user = prompt::user_message_for(
                    &prepared.card_text,
                    &prepared.body,
                    prepared.card.lang.as_deref(),
                    prepared.card.kind.as_deref(),
                );
                (tokens::estimate(&user) + tokens::estimate(&raw.text)) as u64
            } else {
                raw.usage
                    .prompt_tokens
                    .saturating_add(raw.usage.output_tokens)
            };
            let check = prepared.check(&raw);
            let summary = checked_or_fallback(provider.summarizer.as_ref(), &input, Ok(raw));
            Ok(Called {
                summary,
                tokens,
                check,
            })
        })
        .await
        .map_err(|e| format!("the summary task failed: {e}"))?
    }

    /// The named provider (or the default), with its summarizer. The summarizer is kept while
    /// the provider's settings do not change.
    async fn provider(&self, name: Option<&str>) -> Result<Arc<Provider>, SummarizeError> {
        let name = name.map(str::to_owned);
        let (provider, key) = self
            .db(move |s| {
                let name = match name {
                    Some(n) => Some(n),
                    None => s.default_ai_provider()?,
                };
                let Some(name) = name else {
                    return Ok(None);
                };
                let provider = s.ai_provider(&name)?;
                let key = match &provider {
                    Some(_) => s.ai_api_key(&name)?,
                    None => None,
                };
                Ok(Some((name, provider, key)))
            })
            .await?
            .map(|(name, p, key)| (p.ok_or(name), key))
            .ok_or_else(|| {
                SummarizeError::NoProvider(
                    "no AI provider is configured: add one with ConfigService.SetAiProvider"
                        .to_owned(),
                )
            })?;
        let provider = provider.map_err(|name| {
            SummarizeError::NotFound(format!("AI provider {name:?} is not configured"))
        })?;
        let endpoint = endpoint(&provider, key.as_ref().map(|k| Secret::new(k.expose())))
            .map_err(SummarizeError::NoProvider)?;
        let mut cache = lock(&self.inner.providers);
        if let Some(p) = cache.get(&provider.name)
            && same_endpoint(&p.endpoint, &endpoint)
        {
            return Ok(Arc::clone(p));
        }
        let cfg = ApiConfig {
            limiter: Some(Arc::clone(&self.inner.limiter)),
            log: None,
            ..self.inner.config.api.clone()
        };
        let p = Arc::new(Provider {
            name: provider.name.clone(),
            summarizer: Arc::new(ApiSummarizer::new(endpoint.clone(), cfg)),
            endpoint,
        });
        cache.insert(provider.name, Arc::clone(&p));
        Ok(p)
    }

    /// The record of a file, not stored: the stored one when `from_store` and it is current,
    /// else a fresh analysis.
    async fn record_of(
        &self,
        bytes: Vec<u8>,
        name: String,
        from_store: bool,
    ) -> Result<(Blake3, Json), SummarizeError> {
        let (bytes, blake3) = crate::pipeline::hash(bytes)
            .await
            .map_err(|e| SummarizeError::Internal(e.message))?;
        // An object already stored with current step versions is read from its record.
        let backend = self.inner.backend.borrow().clone();
        let stored = if from_store {
            match backend {
                Some(b) => b
                    .get_record(&blake3)
                    .await
                    .ok()
                    .flatten()
                    .filter(|r| !self.inner.analyzer.is_stale(&r.versions))
                    .map(|r| r.body),
                None => None,
            }
        } else {
            None
        };
        let body = if let Some(b) = stored {
            b
        } else {
            let analyzer = Arc::clone(&self.inner.analyzer);
            let limit = analyzer.limits().timeout_cap() + Duration::from_mins(1);
            let task = tokio::task::spawn_blocking(move || {
                analyzer.analyze_hashed(bytes, blake3, &name).body
            });
            match tokio::time::timeout(limit, task).await {
                Ok(Ok(b)) => b,
                Ok(Err(e)) => {
                    return Err(SummarizeError::Internal(format!("analysis failed: {e}")));
                }
                Err(_) => {
                    return Err(SummarizeError::Unavailable(format!(
                        "analysis did not finish within {}s",
                        limit.as_secs()
                    )));
                }
            }
        };
        Ok((blake3, body))
    }

    /// `Summarize`: one summary now, outside the queue. Stores nothing: not the record, not a
    /// job, not the spend (the daily budget governs the queue only).
    pub async fn summarize(
        &self,
        input: SummarizeInput,
        provider: Option<&str>,
    ) -> Result<OnDemandSummary, SummarizeError> {
        let provider = self.provider(provider).await?;
        let (bytes, name, from_store) = match input {
            SummarizeInput::Content { bytes, name } => (bytes, name, false),
            SummarizeInput::Location { source, key } => {
                let src = self.db(move |s| s.source(source)).await?.ok_or_else(|| {
                    SummarizeError::NotFound(format!("source {source} does not exist"))
                })?;
                if !src.llm_policy.allows_on_demand() {
                    return Err(SummarizeError::FailedPrecondition(format!(
                        "the LLM policy of source {source} is off: set it to on_demand (or all, \
                         or kinds) with ConfigService.SetLlmPolicy to summarize its files"
                    )));
                }
                let bytes = self
                    .inner
                    .fetch
                    .fetch(&src, &key)
                    .await
                    .map_err(|e| SummarizeError::Unavailable(e.message))?;
                let name = key.rsplit('/').next().unwrap_or(&key).to_owned();
                (bytes, name, true)
            }
        };
        let (blake3, body) = self.record_of(bytes, name, from_store).await?;
        let kind = DocFacts::from_record(&body).kind.unwrap_or_default();
        let mut out = OnDemandSummary {
            blake3: blake3.to_hex(),
            kind,
            ..OnDemandSummary::default()
        };
        let Some(prepared) = Prepared::from_record(&body) else {
            // Too little text for a model: quail's extractive summary stands, if there is one.
            let Some(text) = fact_text(&body, keys::EXTRACTIVE.as_str()) else {
                return Err(SummarizeError::FailedPrecondition(
                    "the file has no text to summarize".to_owned(),
                ));
            };
            out.digest = text;
            out.extractive = true;
            out.warnings
                .push("too little text for a model: the extractive summary stands".to_owned());
            return Ok(out);
        };
        let model = provider.summarizer.id().name;
        let provider_name = provider.name.clone();
        match self.call(provider, prepared).await {
            Ok(Called { summary, check, .. }) => {
                out.digest = summary.text;
                out.checked = check.0;
                out.supported = check
                    .0
                    .saturating_sub(u32::try_from(check.1.len()).unwrap_or(u32::MAX));
                out.unsupported = check.1;
                out.extractive = summary.method != Method::Llm;
                out.warnings = summary.warnings;
                if !out.extractive {
                    out.provider = provider_name;
                    out.model = model;
                }
            }
            Err(e) => {
                // The provider failed: the extractive summary, and why.
                out.digest = fact_text(&body, keys::EXTRACTIVE.as_str()).unwrap_or_default();
                out.extractive = true;
                out.warnings.push(e);
            }
        }
        Ok(out)
    }
}

/// What a model call gave.
struct Called {
    /// The summary: the model's, or the extractive fallback.
    summary: Summary,
    /// Tokens spent (as reported, else estimated).
    tokens: u64,
    /// The fact check of the model's output: claims checked, and the unsupported ones.
    check: (u32, Vec<String>),
}

/// quail's endpoint for a configured provider. The process environment is never read: every
/// setting comes from the store.
fn endpoint(p: &doewood_store::AiProvider, key: Option<Secret>) -> Result<Endpoint, String> {
    let region = if p.preset == "bedrock" {
        let url = p.base_url.as_deref().ok_or_else(|| {
            format!(
                "AI provider {:?} (bedrock) needs a base URL \
                 https://bedrock-runtime.<region>.amazonaws.com: its region is read from it",
                p.name
            )
        })?;
        Some(bedrock_region(url).ok_or_else(|| {
            format!(
                "AI provider {:?} (bedrock): the base URL must be \
                 https://bedrock-runtime.<region>.amazonaws.com",
                p.name
            )
        })?)
    } else {
        None
    };
    let connect = Connect {
        provider: Some(p.preset.clone()),
        model: Some(p.model.clone()),
        base_url: p.base_url.clone(),
        api_key: key,
        region,
    };
    let none: [(&str, &str); 0] = [];
    Endpoint::resolve(&connect, &Env::from_map(none)).map_err(|e| {
        let msg = e.to_string();
        if msg.contains("needs an API key") {
            format!(
                "AI provider {:?} ({}) has no API key: set one with ConfigService.SetAiProvider",
                p.name, p.preset
            )
        } else if msg.contains("needs a base URL") {
            format!(
                "AI provider {:?} ({}) needs a base URL: set one with ConfigService.SetAiProvider",
                p.name, p.preset
            )
        } else {
            format!("AI provider {:?}: {msg}", p.name)
        }
    })
}

/// The region of `https://bedrock-runtime.<region>.amazonaws.com[...]`.
fn bedrock_region(url: &str) -> Option<String> {
    let host = url
        .split_once("://")?
        .1
        .split(['/', ':', '?', '#'])
        .next()?;
    let region = host
        .strip_prefix("bedrock-runtime.")?
        .strip_suffix(".amazonaws.com")?;
    (!region.is_empty()
        && region
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-'))
    .then(|| region.to_owned())
}

fn same_endpoint(a: &Endpoint, b: &Endpoint) -> bool {
    a.preset == b.preset
        && a.base_url == b.base_url
        && a.model == b.model
        && a.key == b.key
        && a.region == b.region
}

/// A fact of a stored record.
#[derive(Deserialize)]
struct FactView {
    key: String,
    value: Value,
    #[serde(default)]
    conf: f32,
    #[serde(default)]
    evidence: Option<Evidence>,
}

#[derive(Deserialize)]
struct RecordView {
    #[serde(default)]
    pages: Option<u32>,
    #[serde(default)]
    facts: Vec<FactView>,
}

/// Facts read back from a record carry this source (the step that made them is not needed,
/// and an [`AnalyzerId`] read from JSON would be leaked).
const READ_BACK: AnalyzerId = AnalyzerId::new("doewood.record", 1);

fn fact_text(body: &Json, key: &str) -> Option<String> {
    body.get("facts")?.as_array()?.iter().find_map(|f| {
        if f.get("key")?.as_str()? != key {
            return None;
        }
        let v: Value = serde_json::from_value(f.get("value")?.clone()).ok()?;
        v.as_text().map(str::to_owned)
    })
}

/// Whether the record already has a model's summary.
fn has_summary(body: &Json) -> bool {
    body.get("facts")
        .and_then(Json::as_array)
        .is_some_and(|facts| {
            facts.iter().any(|f| {
                f.get("key").and_then(Json::as_str) == Some(SUMMARY_KEY)
                    && f.pointer("/value/v/method/v").and_then(Json::as_str) == Some("llm")
            })
        })
}

/// Whether ingest should queue this record: a document with a digest.
pub fn has_digest(body: &Json) -> bool {
    body.get("facts")
        .and_then(Json::as_array)
        .is_some_and(|facts| {
            facts
                .iter()
                .any(|f| f.get("key").and_then(Json::as_str) == Some(keys::DIGEST.as_str()))
        })
}

/// What a model reads, from a stored record.
struct Prepared {
    card: FileCard,
    card_text: String,
    body: String,
    subject: Option<Subject>,
}

impl Prepared {
    /// `None` when the record has no digest with text enough for a model (as `summary.llm`:
    /// at least 20 words).
    fn from_record(body: &Json) -> Option<Self> {
        let r = RecordView::deserialize(body).ok()?;
        let mut digest = None;
        let mut subject = None;
        let mut facts = Vec::with_capacity(r.facts.len());
        for f in r.facts {
            if f.key == keys::DIGEST.as_str() {
                digest = f.value.as_text().map(str::to_owned);
            } else if f.key == keys::SUBJECT.as_str() {
                subject = subject_of(&f.value, f.evidence.as_ref());
            }
            let Ok(key) = serde_json::from_value::<FactKey>(Json::String(f.key)) else {
                continue;
            };
            facts.push(Fact {
                key,
                value: f.value,
                conf: f.conf,
                src: READ_BACK,
                evidence: f.evidence,
            });
        }
        let digest = digest?;
        let card = FileCard::from_facts(&facts, r.pages.map(|p| p as usize));
        // The digest is `FACTS:\n<card>\n\n<body>`: the card it was built with, then the text.
        let (card_text, text) = match digest.strip_prefix("FACTS:\n") {
            Some(rest) => match rest.split_once("\n\n") {
                Some((c, t)) => (c.to_owned(), t.to_owned()),
                None => (rest.to_owned(), String::new()),
            },
            None => (
                card.render(quail_summary::LlmConfig::default().card_tokens)
                    .text,
                digest,
            ),
        };
        if text.split_whitespace().count() < 20 {
            return None;
        }
        Some(Self {
            card,
            card_text,
            body: text,
            subject,
        })
    }

    fn input(&self, deadline: Option<Instant>) -> SummaryInput<'_> {
        SummaryInput {
            card: &self.card,
            card_text: &self.card_text,
            subject: self.subject.as_ref(),
            text_head: &self.body,
            lang: self.card.lang.as_deref(),
            deadline,
            document: None,
        }
    }

    /// quail's fact check of a model's output against what it read: claims checked, and
    /// the ones not found.
    fn check(&self, s: &Summary) -> (u32, Vec<String>) {
        if s.method != Method::Llm {
            return (0, Vec::new());
        }
        let subject_src = self.subject.as_ref().map_or("", |s| s.text.as_str());
        let sources = [self.card_text.as_str(), self.body.as_str(), subject_src];
        let mut fc = factcheck::check(
            &format!("{} {}", s.text, s.subject.as_deref().unwrap_or("")),
            &sources,
            true,
        );
        if let Some(t) = &s.title_en {
            let title = factcheck::check(t, &sources, false);
            fc.claims += title.claims;
            fc.unsupported.extend(title.unsupported);
        }
        (u32::try_from(fc.claims).unwrap_or(u32::MAX), fc.unsupported)
    }
}

fn subject_of(v: &Value, evidence: Option<&Evidence>) -> Option<Subject> {
    let text = v.field("text")?.as_text()?.to_owned();
    let method = match v.field("method").and_then(Value::as_text) {
        Some("recital") => SubjectMethod::Recital,
        Some("operative") => SubjectMethod::Operative,
        Some("textrank") => SubjectMethod::TextRank,
        _ => SubjectMethod::Clause,
    };
    Some(Subject {
        text,
        page: evidence.and_then(|e| e.page).unwrap_or(0),
        method,
        label: None,
    })
}

/// Puts the summary into the stored record as quail's `summary.llm` fact (replacing an older
/// one), keeping the record's step versions.
async fn write_summary(
    backend: &dyn Backend,
    blake3: &Blake3,
    summary: &Summary,
    model: &str,
    tokens: u64,
) -> Result<(), String> {
    // Read again: the record may have been rewritten while the model was writing.
    let record = backend
        .get_record(blake3)
        .await
        .map_err(|e| e.to_string())?
        .ok_or("the record is gone")?;
    let int = |n: u64| Value::Int(i64::try_from(n).unwrap_or(i64::MAX));
    let mut f = vec![("text", Value::text(summary.text.as_str()))];
    if let Some(s) = &summary.subject {
        f.push(("subject", Value::text(s.as_str())));
    }
    if let Some(t) = &summary.title_en {
        f.push(("title_en", Value::text(t.as_str())));
    }
    f.push(("method", Value::text(summary.method.as_str())));
    f.push(("model", Value::text(model)));
    f.push(("checked", Value::Bool(summary.checked)));
    f.push(("cached", Value::Bool(false)));
    f.push(("input", Value::text("digest")));
    f.push(("calls", Value::Int(1)));
    if summary.usage.is_empty() {
        f.push(("estimated_tokens", int(tokens)));
    } else {
        f.push(("prompt_tokens", int(summary.usage.prompt_tokens)));
        f.push(("output_tokens", int(summary.usage.output_tokens)));
    }
    let fact = Fact {
        key: keys::LLM,
        value: Value::record(f),
        conf: if summary.method == Method::Llm {
            0.8
        } else {
            0.6
        },
        src: SummaryLlm::ID,
        evidence: None,
    };
    let fact = serde_json::to_value(&fact).map_err(|e| e.to_string())?;
    let mut body = record.body;
    let Some(facts) = body.get_mut("facts").and_then(Json::as_array_mut) else {
        return Err("the record has no facts".to_owned());
    };
    facts.retain(|f| f.get("key").and_then(Json::as_str) != Some(SUMMARY_KEY));
    facts.push(fact);
    let step = serde_json::to_value(SummaryLlm::ID).map_err(|e| e.to_string())?;
    if let Some(steps) = body.get_mut("steps").and_then(Json::as_array_mut)
        && !steps.contains(&step)
    {
        steps.push(step);
    }
    backend
        .put_record(blake3, &body, &record.versions)
        .await
        .map(|_| ())
        .map_err(|e| e.to_string())
}

/// Queues a document for a summary when its source's policy asks for one. Errors are
/// logged, never returned: the summary is optional, the record is already stored.
pub(crate) fn queue_if_wanted(
    store: &Store,
    source: &Source,
    blake3: &Blake3,
    body: &Json,
    kind: Option<&str>,
) -> bool {
    if !wants(&source.llm_policy, body, kind) {
        return false;
    }
    match store.queue_llm_job(source.id, &blake3.0, kind) {
        Ok(()) => true,
        Err(e) => {
            warn!(source = %source.id, blake3 = %blake3, error = %e, "cannot queue the LLM summary");
            false
        }
    }
}

fn wants(policy: &LlmPolicy, body: &Json, kind: Option<&str>) -> bool {
    policy.queues(kind) && has_digest(body)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bedrock_regions_come_from_the_url() {
        assert_eq!(
            bedrock_region("https://bedrock-runtime.eu-west-2.amazonaws.com").as_deref(),
            Some("eu-west-2")
        );
        assert_eq!(
            bedrock_region("https://bedrock-runtime.us-east-1.amazonaws.com/").as_deref(),
            Some("us-east-1")
        );
        assert!(bedrock_region("https://example.com").is_none());
        assert!(bedrock_region("https://bedrock-runtime..amazonaws.com").is_none());
    }

    #[test]
    fn records_without_a_digest_are_not_prepared() {
        let body = serde_json::json!({"facts": []});
        assert!(Prepared::from_record(&body).is_none());
        assert!(!has_digest(&body));
        assert!(!has_summary(&body));
        let short = serde_json::json!({"facts": [
            {"key": "summary.digest", "value": {"t": "text", "v": "FACTS:\ndoc: letter\n\nTEXT:\nToo short."}, "conf": 1.0, "src": {"name": "summary.digest", "version": 3}}
        ]});
        assert!(has_digest(&short));
        assert!(Prepared::from_record(&short).is_none());
    }

    #[test]
    fn the_digest_splits_into_card_and_text() {
        let text = "word ".repeat(30);
        let body = serde_json::json!({"pages": 1, "facts": [
            {"key": "summary.digest", "value": {"t": "text", "v": format!("FACTS:\ndoc: letter\nlang: en\n\nTEXT:\n{text}")}, "conf": 1.0, "src": {"name": "summary.digest", "version": 3}},
            {"key": "summary.subject", "value": {"t": "record", "v": {"text": {"t": "text", "v": "The subject."}, "method": {"t": "text", "v": "clause"}}}, "conf": 0.7, "src": {"name": "summary.extractive", "version": 2}},
            {"key": "summary.llm", "value": {"t": "record", "v": {"text": {"t": "text", "v": "x"}, "method": {"t": "text", "v": "extractive_fallback"}}}, "conf": 0.6, "src": {"name": "summary.llm", "version": 5}}
        ]});
        let p = Prepared::from_record(&body).unwrap();
        assert_eq!(p.card_text, "doc: letter\nlang: en");
        assert!(p.body.starts_with("TEXT:\nword word"));
        assert_eq!(p.subject.as_ref().unwrap().text, "The subject.");
        // A fallback is not a model's summary: the queue tries again.
        assert!(!has_summary(&body));
        let done = serde_json::json!({"facts": [
            {"key": "summary.llm", "value": {"t": "record", "v": {"method": {"t": "text", "v": "llm"}}}, "conf": 0.8, "src": {"name": "summary.llm", "version": 5}}
        ]});
        assert!(has_summary(&done));
    }
}
