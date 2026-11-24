//! What `ReprocessJob` and `WatchReprocess` need from ingest, so the API does not depend on
//! the analyzers: the binary hands in an implementation.

use doewood_core::SourceId;
use tokio::sync::watch;
use uuid::Uuid;

/// What a reprocess job is doing.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReprocessPhase {
    /// Comparing stored step versions with the running engine's.
    Selecting,
    /// The selected files are being analyzed again.
    Running,
    /// Nothing of the source is waiting any more.
    Done,
    /// It stopped; see the error.
    Failed,
}

impl ReprocessPhase {
    /// Whether this is the last state.
    pub fn is_final(self) -> bool {
        matches!(self, Self::Done | Self::Failed)
    }
}

/// Where a reprocess job is.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReprocessView {
    /// The job.
    pub job: Uuid,
    /// Its source.
    pub source: SourceId,
    /// Every file, current or not.
    pub force: bool,
    /// What it is doing.
    pub phase: ReprocessPhase,
    /// Ingested files compared so far.
    pub examined: u64,
    /// Files put back in the queue.
    pub selected: u64,
    /// The source's ingested files now.
    pub processed: u64,
    /// The source's failed files now.
    pub failed: u64,
    /// The source's waiting files now.
    pub pending: u64,
    /// Why it failed.
    pub error: Option<String>,
}

/// Starts and finds reprocess jobs (ingest implements it).
pub trait Reprocessor: Send + Sync {
    /// Starts a job for a source of this instance.
    fn start(&self, source: SourceId, force: bool) -> watch::Receiver<ReprocessView>;

    /// A job this instance started, if it still knows it.
    fn watch(&self, job: Uuid) -> Option<watch::Receiver<ReprocessView>>;
}
