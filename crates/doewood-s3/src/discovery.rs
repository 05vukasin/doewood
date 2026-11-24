//! Runs one scanner task per scanned source, starts and stops them while the service runs
//! (the API adds, pauses and removes sources without a restart), and stops them all on
//! shutdown.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use doewood_core::SourceId;
use doewood_store::{Source, Store};
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;
use tracing::{info, warn};

use crate::scanner::{Connect, ScanOptions, Scanner};
use crate::{ClientOptions, ObjectEvent, S3Client, S3Config, S3Error, http_client};

/// Capacity of the event channel the binary creates: a scanner waits when ingest falls this
/// far behind.
pub const EVENT_CHANNEL_CAPACITY: usize = 1024;

/// Options of discovery.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct DiscoveryOptions {
    /// HTTP timeouts and retries.
    pub client: ClientOptions,
    /// Scanner tuning.
    pub scan: ScanOptions,
}

/// The running scanners. Cheap to clone: clones control the same scanners.
#[derive(Clone)]
pub struct Discovery {
    inner: Arc<Inner>,
}

impl std::fmt::Debug for Discovery {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Discovery")
            .field("scanners", &self.len())
            .finish_non_exhaustive()
    }
}

struct Inner {
    store: Arc<Store>,
    connect: Connect<S3Client>,
    options: ScanOptions,
    shutdown: watch::Receiver<bool>,
    state: Mutex<State>,
}

struct State {
    /// `None` once [`Discovery::join`] ran: no scanner starts after that.
    events: Option<mpsc::Sender<ObjectEvent>>,
    tasks: HashMap<SourceId, JoinHandle<()>>,
}

impl Discovery {
    /// Starts a scanner for every source that is `active` or in `error` (a failing source keeps
    /// being retried; a paused one is not scanned). With no such source it starts nothing. The
    /// scanners stop when `shutdown` turns true.
    pub async fn start(
        store: Arc<Store>,
        events: mpsc::Sender<ObjectEvent>,
        shutdown: watch::Receiver<bool>,
        options: DiscoveryOptions,
    ) -> Result<Self, S3Error> {
        let sources = {
            let store = Arc::clone(&store);
            tokio::task::spawn_blocking(move || store.sources())
                .await
                .map_err(|e| S3Error::Transport {
                    op: "store",
                    reason: e.to_string(),
                })??
        };
        let discovery = Self::idle(store, events, shutdown, options)?;
        let to_scan: Vec<Source> = sources
            .into_iter()
            .filter(|s| s.state.is_scanned())
            .collect();
        if to_scan.is_empty() {
            info!("no active sources: discovery is idle");
        }
        for source in &to_scan {
            discovery.start_source(source);
        }
        Ok(discovery)
    }

    /// Discovery with no scanner running yet.
    pub fn idle(
        store: Arc<Store>,
        events: mpsc::Sender<ObjectEvent>,
        shutdown: watch::Receiver<bool>,
        options: DiscoveryOptions,
    ) -> Result<Self, S3Error> {
        let http = http_client(&options.client)?;
        let client_options = options.client;
        let connect: Connect<S3Client> = Arc::new(move |store: &Store, source: &Source| {
            let credentials = store.source_credentials(source.id)?;
            S3Client::with_http(
                &S3Config::for_source(source, credentials),
                client_options,
                http.clone(),
            )
        });
        Ok(Self {
            inner: Arc::new(Inner {
                store,
                connect,
                options: options.scan,
                shutdown,
                state: Mutex::new(State {
                    events: Some(events),
                    tasks: HashMap::new(),
                }),
            }),
        })
    }

    fn state(&self) -> MutexGuard<'_, State> {
        self.inner
            .state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }

    /// Starts the source's scanner unless one is running. Returns whether one started: not
    /// when it already runs, when the source is not scanned (paused, removed), or after
    /// shutdown.
    pub fn start_source(&self, source: &Source) -> bool {
        if !source.state.is_scanned() || *self.inner.shutdown.borrow() {
            return false;
        }
        let mut state = self.state();
        let Some(events) = state.events.clone() else {
            return false;
        };
        if state
            .tasks
            .get(&source.id)
            .is_some_and(|t| !t.is_finished())
        {
            return false;
        }
        info!(
            source = %source.id,
            bucket = %source.bucket,
            prefix = %source.prefix,
            interval_s = source.scan_interval.as_secs(),
            state = %source.state,
            "starting scanner"
        );
        let scanner = Scanner::new(
            Arc::clone(&self.inner.store),
            source,
            events,
            self.inner.options,
        );
        let task =
            tokio::spawn(scanner.run(Arc::clone(&self.inner.connect), self.inner.shutdown.clone()));
        state.tasks.insert(source.id, task);
        true
    }

    /// Stops the source's scanner now, if one runs. A pass under way stops at its next await
    /// point; its checkpoint keeps it resumable, and events sent but not yet committed are sent
    /// again (delivery is at least once). Returns whether a scanner was running.
    pub fn stop_source(&self, id: SourceId) -> bool {
        let task = self.state().tasks.remove(&id);
        match task {
            Some(t) if !t.is_finished() => {
                t.abort();
                info!(source = %id, "scanner stopped");
                true
            }
            _ => false,
        }
    }

    /// Restarts the source's scanner, so changed options (the interval) apply at once.
    pub fn restart_source(&self, source: &Source) -> bool {
        self.stop_source(source.id);
        self.start_source(source)
    }

    /// Whether the source's scanner runs.
    pub fn is_running(&self, id: SourceId) -> bool {
        self.state()
            .tasks
            .get(&id)
            .is_some_and(|t| !t.is_finished())
    }

    /// The number of scanners running.
    pub fn len(&self) -> usize {
        self.state()
            .tasks
            .values()
            .filter(|t| !t.is_finished())
            .count()
    }

    /// Whether no scanner runs.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Waits for every scanner to stop (after shutdown was requested). From then on no
    /// scanner starts, and this handle no longer holds the event channel open.
    pub async fn join(self) {
        let tasks: Vec<_> = {
            let mut state = self.state();
            state.events = None;
            state.tasks.drain().map(|(_, t)| t).collect()
        };
        for task in tasks {
            match task.await {
                Ok(()) => {}
                Err(e) if e.is_cancelled() => {}
                Err(e) => warn!(error = %e, "a scanner task failed"),
            }
        }
    }
}
