//! Ingest in the service: the analyzer and the workers built from the bootstrap
//! configuration, its reprocess jobs offered to the API, and the LLM queue with on-demand
//! summaries.

use std::sync::Arc;
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

    use doewood_core::BootstrapConfig;
    use doewood_ingest::sheet;

    use super::ingest_config;

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
