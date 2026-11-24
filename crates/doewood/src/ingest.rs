//! Ingest in the service: the analyzer and the workers built from the bootstrap
//! configuration, and its reprocess jobs offered to the API.

use std::sync::Arc;

use doewood_api::{Progress, ReprocessPhase, ReprocessView, Reprocessor};
use doewood_core::{BootstrapConfig, SourceId};
use doewood_ingest::{
    Analyzer, Ingest, IngestConfig, IngestParts, ReprocessState, ReprocessStatus, S3Fetch,
};
use doewood_model::Backend;
use doewood_s3::ClientOptions;
use doewood_store::Store;
use tokio::sync::watch;
use tracing::{error, info};
use uuid::Uuid;

/// Builds ingest: one analyzer (quail engine, OCR when built and its models are there) and the
/// workers. `None` when the engine cannot be built; the service then runs without ingest.
pub async fn build(
    config: &BootstrapConfig,
    store: &Arc<Store>,
    backend: watch::Receiver<Option<Arc<dyn Backend>>>,
    progress: &Arc<Progress>,
) -> Option<Ingest> {
    let ingest_config = IngestConfig::new(
        config.ingest_jobs,
        &config.ocr_models,
        config.similar_min_jaccard,
    );
    let analyzer_config = ingest_config.analyzer.clone();
    // Checking the OCR models reads (and hashes) them: off the async threads.
    let analyzer = tokio::task::spawn_blocking(move || Analyzer::new(&analyzer_config)).await;
    let analyzer = match analyzer {
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
    info!(jobs = ingest_config.jobs, ocr = ?analyzer.ocr(), "ingest configured");
    let progress = Arc::clone(progress);
    Some(Ingest::new(IngestParts {
        store: Arc::clone(store),
        backend,
        fetch: Arc::new(S3Fetch::new(Arc::clone(store), ClientOptions::default())),
        analyzer,
        config: ingest_config,
        on_change: Arc::new(move |_source: SourceId| progress.touch()),
    }))
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
