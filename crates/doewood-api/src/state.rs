//! What every service shares: the instance store, the backend, discovery and the progress
//! counters.

use std::sync::Arc;
use std::time::Duration;

use doewood_core::{InstanceId, SourceId};
use doewood_model::Backend;
use doewood_s3::Discovery;
use doewood_store::{Source, Store, StoreError};
use tokio::sync::watch;
use tonic::Status;

use crate::progress::Progress;
use crate::{convert, error, pb};

/// Tuning of the API server.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ApiOptions {
    /// Largest message decoded or encoded, in bytes (`DOEWOOD_API_MAX_MESSAGE_MIB`).
    pub max_message_bytes: usize,
    /// How often `WatchProgress` looks at the store when nothing notified it.
    pub progress_interval: Duration,
}

impl Default for ApiOptions {
    fn default() -> Self {
        Self {
            max_message_bytes: 64 * 1024 * 1024,
            progress_interval: Duration::from_secs(1),
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
    /// Serializes source changes (add, update, remove), so a claim and the row that owns it
    /// never race.
    sources: tokio::sync::Mutex<()>,
}

impl ApiState {
    /// The state of an API built from these parts.
    pub fn new(parts: ApiParts) -> Self {
        Self {
            inner: Arc::new(Inner {
                parts,
                sources: tokio::sync::Mutex::new(()),
            }),
        }
    }

    pub(crate) fn parts(&self) -> &ApiParts {
        &self.inner.parts
    }

    pub(crate) async fn lock_sources(&self) -> tokio::sync::MutexGuard<'_, ()> {
        self.inner.sources.lock().await
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

    pub(crate) fn progress(&self) -> &Arc<Progress> {
        &self.inner.parts.progress
    }

    pub(crate) fn running(&self, id: SourceId) -> bool {
        self.discovery().is_some_and(|d| d.is_running(id))
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
        convert::source(s, self.instance(), self.running(s.id))
    }
}
