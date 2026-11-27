//! Ingest in the service: the analyzer and the workers built from the bootstrap
//! configuration, its reprocess jobs offered to the API, and the LLM queue with on-demand
//! summaries.

use std::sync::{Arc, Mutex, PoisonError, Weak};
use std::time::Instant;

use doewood_api::{
    BoxFuture, OnDemand, OnDemandError, OnDemandInput, OnDemandResult, Progress, ReprocessPhase,
    ReprocessView, Reprocessor,
};
use doewood_core::{BootstrapConfig, SourceId};
use doewood_ingest::{
    Analyzer, Fetch, Ingest, IngestConfig, IngestParts, QueueConfig, ReprocessState,
    ReprocessStatus, S3Fetch, Summaries, SummariesParts, SummarizeError, SummarizeInput,
    SystemClock,
};
use doewood_model::Backend;
use doewood_s3::ClientOptions;
use doewood_store::Store;
use prometheus::core::{Collector, Desc};
use prometheus::proto::MetricFamily;
use prometheus::{IntCounter, IntCounterVec, Opts, Registry};
use tokio::sync::watch;
use tracing::{error, info};
use uuid::Uuid;

use crate::ops::OpsState;

/// The analyzer being built, by [`start_analyzer`].
pub type AnalyzerTask = tokio::task::JoinHandle<Result<Analyzer, String>>;

/// Starts building the analyzer (quail engine, OCR when built and its models are there) on a
/// blocking thread: checking the OCR models reads and hashes them, and the engine takes a
/// while too. It needs nothing but the configuration, so it runs while the store opens
/// (issue #118); [`build`] waits for it.
pub fn start_analyzer(config: &BootstrapConfig, ops: &Arc<OpsState>) -> AnalyzerTask {
    let analyzer_config = ingest_config(config).analyzer;
    let ops = Arc::clone(ops);
    tokio::task::spawn_blocking(move || {
        let t = Instant::now();
        let analyzer = Analyzer::new(&analyzer_config);
        ops.step("analyzer", t);
        analyzer
    })
}

/// Builds ingest from the analyzer `start_analyzer` started: the workers and the LLM queue.
/// `None` when the engine cannot be built; the service then runs without ingest (and without
/// summaries).
pub async fn build(
    config: &BootstrapConfig,
    store: &Arc<Store>,
    backend: watch::Receiver<Option<Arc<dyn Backend>>>,
    progress: &Arc<Progress>,
    analyzer: AnalyzerTask,
) -> Option<(Ingest, Summaries)> {
    let ingest_config = ingest_config(config);
    let analyzer = match analyzer.await {
        Ok(Ok(a)) => Arc::new(a),
        Ok(Err(e)) => {
            error!(error = %e, "the analyzer cannot be built; ingest does not run");
            return None;
        }
        Err(e) => {
            error!(error = %e, "the analyzer cannot be built; ingest does not run");
            return None;
        }
    };
    info!(
        jobs = ingest_config.jobs,
        memory_budget_mib = ingest_config.memory_budget >> 20,
        sheet_max_cells = ingest_config.analyzer.sheet_max_cells,
        ocr = ?analyzer.ocr(),
        "ingest configured"
    );
    let fetch: Arc<dyn Fetch> = Arc::new(S3Fetch::new(Arc::clone(store), ClientOptions::default()));
    let summaries = {
        let progress = Arc::clone(progress);
        Summaries::new(SummariesParts {
            store: Arc::clone(store),
            backend: backend.clone(),
            fetch: Arc::clone(&fetch),
            analyzer: Arc::clone(&analyzer),
            config: QueueConfig::default(),
            clock: Arc::new(SystemClock),
            on_change: Arc::new(move |_source: SourceId| progress.touch()),
        })
    };
    let progress = Arc::clone(progress);
    let queue = summaries.clone();
    let ingest = Ingest::new(IngestParts {
        store: Arc::clone(store),
        backend,
        fetch,
        analyzer,
        config: ingest_config,
        on_change: Arc::new(move |_source: SourceId| progress.touch()),
        on_llm_queued: Arc::new(move |_source: SourceId| queue.notify()),
    });
    Some((ingest, summaries))
}

/// Registers ingest's metrics on the ops port's registry: `doewood_ingest_finish_unmatched_total`
/// (issue #125), `doewood_ingest_similar_buckets_skipped_total` (issue #148), and
/// `doewood_ingest_known_copies_total` with `doewood_ingest_known_copy_bytes_total` (issue
/// #131), read from
/// [`Ingest::stats`] when `/metrics` is scraped. The registry does not keep ingest (and so the
/// store) alive.
pub fn register_metrics(ingest: &Ingest, registry: &Registry) -> prometheus::Result<()> {
    let stats = ingest.stats_reader();
    let unmatched = StatCounter::new(
        "doewood_ingest_finish_unmatched_total",
        "Finished objects whose status matched no pending row of that version (changed or \
         deleted meanwhile, or an event ahead of its row).",
        move || stats().map_or(0, |s| s.finish_unmatched),
    )?;
    registry.register(Box::new(unmatched))?;
    let stats = ingest.stats_reader();
    let skipped = StatCounter::new(
        "doewood_ingest_similar_buckets_skipped_total",
        "Band keys the similar_to candidate query skipped because more documents than \
         similar_max_bucket share them (template boilerplate).",
        move || stats().map_or(0, |s| s.similar_buckets_skipped),
    )?;
    registry.register(Box::new(skipped))?;
    let stats = ingest.stats_reader();
    let copies = StatCounter::new(
        "doewood_ingest_known_copies_total",
        "Objects recorded against content already stored, by their single-part MD5 ETag and \
         size, without a GetObject (sources with skip_known_copies on).",
        move || stats().map_or(0, |s| s.known_copies),
    )?;
    registry.register(Box::new(copies))?;
    let stats = ingest.stats_reader();
    let bytes = StatCounter::new(
        "doewood_ingest_known_copy_bytes_total",
        "Listed bytes of the known copies, not fetched.",
        move || stats().map_or(0, |s| s.known_copy_bytes),
    )?;
    registry.register(Box::new(bytes))
}

/// Registers `doewood_ingest_claimed_elsewhere_dropped_total` on the ops port's registry
/// (issue #25), read from the store's claim view when `/metrics` is scraped. It counts with or
/// without ingest (the webhook drops too). The registry does not keep the store alive.
pub fn register_claim_metrics(store: &Arc<Store>, registry: &Registry) -> prometheus::Result<()> {
    registry.register(Box::new(ClaimDropCounter::new(store)?))
}

/// `doewood_ingest_claimed_elsewhere_dropped_total{path}` (issue #25): queued objects
/// (`path="queue"`) and event notification records (`path="webhook"`) of sources another
/// instance owns, dropped instead of ingested. Read from the store's claim view when collected.
struct ClaimDropCounter {
    counters: IntCounterVec,
    store: Weak<Store>,
    /// Collections one at a time, so that a count is added once.
    sync: Mutex<()>,
}

impl ClaimDropCounter {
    fn new(store: &Arc<Store>) -> prometheus::Result<Self> {
        let counters = IntCounterVec::new(
            Opts::new(
                "doewood_ingest_claimed_elsewhere_dropped_total",
                "Work dropped because another instance owns the source in the shared store: \
                 queued objects (path=queue) and event notification records (path=webhook).",
            ),
            &["path"],
        )?;
        for path in ["queue", "webhook"] {
            counters.with_label_values(&[path]);
        }
        Ok(Self {
            counters,
            store: Arc::downgrade(store),
            sync: Mutex::new(()),
        })
    }
}

impl Collector for ClaimDropCounter {
    fn desc(&self) -> Vec<&Desc> {
        self.counters.desc()
    }

    fn collect(&self) -> Vec<MetricFamily> {
        let _one = self.sync.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(store) = Weak::upgrade(&self.store) {
            let drops = store.claims().drops();
            for (path, now) in [("queue", drops.queued), ("webhook", drops.events)] {
                let c = self.counters.with_label_values(&[path]);
                if now > c.get() {
                    c.inc_by(now - c.get());
                }
            }
        }
        self.counters.collect()
    }
}

/// A counter that follows a count kept elsewhere, read when it is collected.
pub(crate) struct StatCounter<F> {
    counter: IntCounter,
    read: F,
    /// Collections one at a time, so that a count is added once.
    sync: Mutex<()>,
}

impl<F: Fn() -> u64 + Send + Sync> StatCounter<F> {
    pub(crate) fn new(name: &str, help: &str, read: F) -> prometheus::Result<Self> {
        Ok(Self {
            counter: IntCounter::new(name, help)?,
            read,
            sync: Mutex::new(()),
        })
    }
}

impl<F: Fn() -> u64 + Send + Sync> Collector for StatCounter<F> {
    fn desc(&self) -> Vec<&Desc> {
        self.counter.desc()
    }

    fn collect(&self) -> Vec<MetricFamily> {
        let _one = self.sync.lock().unwrap_or_else(PoisonError::into_inner);
        let (now, shown) = ((self.read)(), self.counter.get());
        if now > shown {
            self.counter.inc_by(now - shown);
        }
        self.counter.collect()
    }
}

/// On-demand summaries and the queue's wake-up, as the API sees them.
pub struct OnDemandSummaries(pub Summaries);

impl OnDemand for OnDemandSummaries {
    fn summarize(
        &self,
        input: OnDemandInput,
        provider: Option<String>,
    ) -> BoxFuture<'_, Result<OnDemandResult, OnDemandError>> {
        Box::pin(async move {
            let input = match input {
                OnDemandInput::Content { bytes, name } => SummarizeInput::Content { bytes, name },
                OnDemandInput::Location { source, key } => SummarizeInput::Location { source, key },
            };
            let s = self
                .0
                .summarize(input, provider.as_deref())
                .await
                .map_err(|e| match e {
                    SummarizeError::NoProvider(m) => OnDemandError::NoProvider(m),
                    SummarizeError::NotFound(m) => OnDemandError::NotFound(m),
                    SummarizeError::FailedPrecondition(m) => OnDemandError::FailedPrecondition(m),
                    SummarizeError::Unavailable(m) => OnDemandError::Unavailable(m),
                    SummarizeError::ResourceExhausted(m) => OnDemandError::ResourceExhausted(m),
                    SummarizeError::Internal(m) => OnDemandError::Internal(m),
                })?;
            Ok(OnDemandResult {
                digest: s.digest,
                checked: s.checked,
                supported: s.supported,
                unsupported: s.unsupported,
                extractive: s.extractive,
                provider: s.provider,
                model: s.model,
                blake3: s.blake3,
                kind: s.kind,
                warnings: s.warnings,
            })
        })
    }

    fn settings_changed(&self) {
        self.0.notify();
    }
}

/// Ingest's reprocess jobs, as the API sees them.
pub struct Reprocess(pub Ingest);

impl Reprocessor for Reprocess {
    fn start(&self, source: SourceId, force: bool) -> watch::Receiver<ReprocessView> {
        forward(self.0.reprocess(source, force))
    }

    fn watch(&self, job: Uuid) -> Option<watch::Receiver<ReprocessView>> {
        self.0.watch_job(job).map(forward)
    }
}

/// Mirrors a job's status into the API's view until its last state.
fn forward(mut from: watch::Receiver<ReprocessStatus>) -> watch::Receiver<ReprocessView> {
    let (tx, rx) = watch::channel(view(&from.borrow_and_update()));
    tokio::spawn(async move {
        loop {
            if tx.borrow().phase.is_final() {
                // Hold the last state until every watcher has gone.
                tx.closed().await;
                return;
            }
            tokio::select! {
                changed = from.changed() => {
                    if changed.is_err() {
                        return;
                    }
                    tx.send_replace(view(&from.borrow_and_update()));
                }
                () = tx.closed() => return,
            }
        }
    });
    rx
}

/// Ingest's configuration: sized from the CPUs and memory, with the bootstrap overrides
/// (`DOEWOOD_INGEST_BUDGET_MIB`, `DOEWOOD_INGEST_MAX_CELLS`, `DOEWOOD_OCR_JOBS`) on top.
fn ingest_config(config: &BootstrapConfig) -> IngestConfig {
    let mut ingest_config = IngestConfig::new(config.ingest_jobs, &config.ocr_models);
    if let Some(bytes) = config.ingest_budget_bytes {
        ingest_config.memory_budget = bytes;
    }
    if let Some(cells) = config.ingest_max_cells {
        ingest_config.analyzer.sheet_max_cells = cells;
    }
    ingest_config.analyzer.ocr_jobs = config.ocr_jobs;
    ingest_config
}

fn view(s: &ReprocessStatus) -> ReprocessView {
    ReprocessView {
        job: s.job,
        source: s.source,
        force: s.force,
        phase: match s.state {
            ReprocessState::Selecting => ReprocessPhase::Selecting,
            ReprocessState::Running => ReprocessPhase::Running,
            ReprocessState::Done => ReprocessPhase::Done,
            ReprocessState::Failed => ReprocessPhase::Failed,
        },
        examined: s.examined,
        selected: s.selected,
        processed: s.processed,
        failed: s.failed,
        pending: s.pending,
        error: s.error.clone(),
    }
}

#[cfg(test)]
mod tests {
    use std::ffi::OsString;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};

    use doewood_core::BootstrapConfig;
    use doewood_ingest::sheet;
    use prometheus::{Encoder as _, Registry, TextEncoder};

    use super::{StatCounter, ingest_config};

    #[test]
    fn a_stat_counter_follows_its_count() {
        let count = Arc::new(AtomicU64::new(0));
        let c = Arc::clone(&count);
        let registry = Registry::new();
        let counter = StatCounter::new("doewood_test_total", "A test count.", move || {
            c.load(Ordering::SeqCst)
        })
        .unwrap();
        registry.register(Box::new(counter)).unwrap();
        let text = || {
            let mut out = Vec::new();
            TextEncoder::new()
                .encode(&registry.gather(), &mut out)
                .unwrap();
            String::from_utf8(out).unwrap()
        };
        assert!(text().contains("doewood_test_total 0"), "{}", text());
        count.store(3, Ordering::SeqCst);
        assert!(text().contains("doewood_test_total 3"));
        assert!(text().contains("# TYPE doewood_test_total counter"));
        // Read twice, added once.
        assert!(text().contains("doewood_test_total 3"));
    }

    fn config(env: &[(&str, &str)]) -> BootstrapConfig {
        let env: Vec<(String, OsString)> = env
            .iter()
            .map(|(k, v)| ((*k).to_owned(), OsString::from(v)))
            .collect();
        let lookup = |var: &str| env.iter().find(|(k, _)| k == var).map(|(_, v)| v.clone());
        BootstrapConfig::from_lookup(lookup, || Some(OsString::from("h"))).config
    }

    #[test]
    fn the_bootstrap_overrides_win() {
        let auto = ingest_config(&config(&[("DOEWOOD_INGEST_JOBS", "2")]));
        let cells = auto.analyzer.sheet_max_cells;
        assert!(
            (sheet::MIN_AUTO_CELLS..=sheet::MAX_AUTO_CELLS).contains(&cells),
            "{cells}"
        );
        let set = ingest_config(&config(&[
            ("DOEWOOD_INGEST_JOBS", "2"),
            ("DOEWOOD_INGEST_BUDGET_MIB", "64"),
            ("DOEWOOD_INGEST_MAX_CELLS", "20000000"),
            ("DOEWOOD_OCR_JOBS", "2"),
        ]));
        assert_eq!(set.memory_budget, 64 << 20);
        assert_eq!(set.analyzer.sheet_max_cells, 20_000_000);
        assert_eq!(set.analyzer.ocr_jobs, Some(2));
        assert_eq!(auto.analyzer.ocr_jobs, None);
        // An invalid value warns (see doewood-core) and leaves the automatic cap.
        let bad = ingest_config(&config(&[
            ("DOEWOOD_INGEST_JOBS", "2"),
            ("DOEWOOD_INGEST_MAX_CELLS", "lots"),
        ]));
        let cells = bad.analyzer.sheet_max_cells;
        assert!(
            (sheet::MIN_AUTO_CELLS..=sheet::MAX_AUTO_CELLS).contains(&cells),
            "{cells}"
        );
    }
}
