//! What every service shares: the instance store, the backend, discovery and the progress
//! counters.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use doewood_core::{InstanceId, SourceId};
use doewood_model::Backend;
use doewood_s3::Discovery;
use doewood_store::{Source, Store, StoreError};
use tokio::sync::watch;
use tonic::Status;

use crate::icons::Icons;
use crate::progress::{Progress, Snapshots};
use crate::reprocess::Reprocessor;
use crate::summarize::OnDemand;
use crate::{convert, error, pb};

/// Tuning of the API server.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ApiOptions {
    /// Largest message decoded or encoded, in bytes (`DOEWOOD_API_MAX_MESSAGE_MIB`).
    pub max_message_bytes: usize,
    /// How often `WatchProgress` looks at the store when nothing notified it.
    pub progress_interval: Duration,
    /// `serve` with a shared store: the sources are re-claimed at start
    /// ([`crate::reclaim_sources`]). Until that pass finishes no scanner starts through the
    /// API, and resuming a source claims it first. Off standalone (the default).
    pub shared_claims: bool,
}

impl Default for ApiOptions {
    fn default() -> Self {
        Self {
            max_message_bytes: 64 * 1024 * 1024,
            progress_interval: Duration::from_secs(1),
            shared_claims: false,
        }
    }
}

/// The parts the API is built from.
pub struct ApiParts {
    /// The instance store: sources, settings, secrets, progress.
    pub store: Arc<Store>,
    /// Records, graph, claims and search; `None` until the shared Postgres is connected.
    pub backend: watch::Receiver<Option<Arc<dyn Backend>>>,
    /// This instance's name: the owner of the sources it claims.
    pub instance: InstanceId,
    /// The scanners; `None` when discovery could not start (sources are then only stored).
    pub discovery: Option<Discovery>,
    /// The live event counters.
    pub progress: Arc<Progress>,
    /// Reprocess jobs; `None` when ingest does not run (`ReprocessJob` is then UNAVAILABLE).
    pub reprocess: Option<Arc<dyn Reprocessor>>,
    /// On-demand summaries and the LLM queue; `None` when ingest does not run (`Summarize` is
    /// then UNAVAILABLE).
    pub summarize: Option<Arc<dyn OnDemand>>,
    /// Company icons for `GET /icons/{domain}`; `None` answers every domain with a miss.
    pub icons: Option<Arc<dyn Icons>>,
    /// Tuning.
    pub options: ApiOptions,
    /// Turns true on shutdown; streams end then.
    pub shutdown: watch::Receiver<bool>,
}

/// The shared state of the services. Cheap to clone.
#[derive(Clone)]
pub struct ApiState {
    inner: Arc<Inner>,
}

struct Inner {
    parts: ApiParts,
    /// The progress snapshots every `WatchProgress` stream shares.
    snapshots: Arc<Snapshots>,
    /// Serializes source changes (add, update, remove), so a claim and the row that owns it
    /// never race.
    sources: tokio::sync::Mutex<()>,
    /// Whether the start-up claim pass has finished (always, without shared claims).
    claims_checked: AtomicBool,
}

impl ApiState {
    /// The state of an API built from these parts.
    pub fn new(parts: ApiParts) -> Self {
        let claims_checked = AtomicBool::new(!parts.options.shared_claims);
        let discovery = parts.discovery.clone();
        let snapshots = Snapshots::new(
            Arc::clone(&parts.store),
            Arc::clone(&parts.progress),
            Arc::new(move |id| discovery.as_ref().is_some_and(|d| d.is_running(id))),
            parts.options.progress_interval,
            parts.shutdown.clone(),
        );
        Self {
            inner: Arc::new(Inner {
                parts,
                snapshots,
                sources: tokio::sync::Mutex::new(()),
                claims_checked,
            }),
        }
    }

    /// The shutdown signal the API was built with.
    pub fn shutdown(&self) -> watch::Receiver<bool> {
        self.inner.parts.shutdown.clone()
    }

    pub(crate) fn parts(&self) -> &ApiParts {
        &self.inner.parts
    }

    pub(crate) async fn lock_sources(&self) -> tokio::sync::MutexGuard<'_, ()> {
        self.inner.sources.lock().await
    }

    /// Whether sources are claimed in a shared store (see [`ApiOptions::shared_claims`]).
    pub(crate) fn shared_claims(&self) -> bool {
        self.inner.parts.options.shared_claims
    }

    /// Whether the start-up claim pass has finished.
    pub(crate) fn claims_checked(&self) -> bool {
        self.inner.claims_checked.load(Ordering::Acquire)
    }

    pub(crate) fn claims_done(&self) {
        self.inner.claims_checked.store(true, Ordering::Release);
    }

    pub(crate) fn instance(&self) -> &InstanceId {
        &self.inner.parts.instance
    }

    pub(crate) fn store(&self) -> &Arc<Store> {
        &self.inner.parts.store
    }

    /// The backend, or UNAVAILABLE while the shared Postgres is not connected.
    pub(crate) fn backend(&self) -> Result<Arc<dyn Backend>, Status> {
        self.inner.parts.backend.borrow().clone().ok_or_else(|| {
            Status::unavailable("the shared store is not connected yet; try again shortly")
        })
    }

    pub(crate) fn discovery(&self) -> Option<&Discovery> {
        self.inner.parts.discovery.as_ref()
    }

    pub(crate) fn snapshots(&self) -> &Arc<Snapshots> {
        &self.inner.snapshots
    }

    pub(crate) fn progress(&self) -> &Arc<Progress> {
        &self.inner.parts.progress
    }

    /// Tells the LLM queue that a policy or a provider changed.
    pub(crate) fn llm_settings_changed(&self) {
        if let Some(s) = &self.inner.parts.summarize {
            s.settings_changed();
        }
    }

    /// Runs a store call on a blocking thread.
    pub(crate) async fn db<T: Send + 'static>(
        &self,
        f: impl FnOnce(&Store) -> Result<T, StoreError> + Send + 'static,
    ) -> Result<T, Status> {
        let store = Arc::clone(self.store());
        tokio::task::spawn_blocking(move || f(&store))
            .await
            .map_err(|e| error::join(&e))?
            .map_err(error::store)
    }

    /// A source of this instance, or NOT_FOUND.
    pub(crate) async fn source(&self, id: SourceId) -> Result<Source, Status> {
        self.db(move |s| s.source(id))
            .await?
            .ok_or_else(|| Status::not_found(format!("source {id} does not exist")))
    }

    pub(crate) fn source_pb(&self, s: &Source) -> pb::Source {
        let running = self
            .discovery()
            .map(|d| convert::Running {
                scanner: d.is_running(s.id),
                inlet: d.is_inlet_running(s.id),
                inlet_stats: d.inlet_stats(s.id),
            })
            .unwrap_or_default();
        convert::source(s, self.instance(), running)
    }
}
