//! Runs one scanner task per scanned source, starts and stops them while the service runs
//! (the API adds, pauses and removes sources without a restart), and stops them all on
//! shutdown. A source with an enabled SQS inlet gets a second task, the inlet, which starts
//! and stops with its scanner.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use doewood_core::SourceId;
use doewood_store::{Source, Store};
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;
use tracing::{info, warn};

use crate::inlet::{InletStats, InletStatsSnapshot};
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
    /// SQS inlet tuning.
    #[cfg(feature = "sqs")]
    pub sqs: crate::inlet::sqs::SqsOptions,
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
    #[cfg_attr(not(feature = "sqs"), allow(dead_code))]
    http: reqwest::Client,
    #[cfg(feature = "sqs")]
    sqs: crate::inlet::sqs::SqsOptions,
    shutdown: watch::Receiver<bool>,
    state: Mutex<State>,
}

struct State {
    /// `None` once [`Discovery::join`] ran: no scanner starts after that.
    events: Option<mpsc::Sender<ObjectEvent>>,
    tasks: HashMap<SourceId, JoinHandle<()>>,
    /// The SQS inlets, with what each did since it started.
    inlets: HashMap<SourceId, (JoinHandle<()>, Arc<InletStats>)>,
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
        let s3_http = http.clone();
        let connect: Connect<S3Client> = Arc::new(move |store: &Store, source: &Source| {
            let credentials = store.source_credentials(source.id)?;
            S3Client::with_http(
                &S3Config::for_source(source, credentials),
                client_options,
                s3_http.clone(),
            )
        });
        Ok(Self {
            inner: Arc::new(Inner {
                store,
                connect,
                options: options.scan,
                http,
                #[cfg(feature = "sqs")]
                sqs: options.sqs,
                shutdown,
                state: Mutex::new(State {
                    events: Some(events),
                    tasks: HashMap::new(),
                    inlets: HashMap::new(),
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
        self.start_inlet(&mut state, source);
        true
    }

    /// Starts the source's SQS inlet when it has an enabled one and none runs.
    #[cfg(feature = "sqs")]
    fn start_inlet(&self, state: &mut State, source: &Source) {
        use crate::inlet::EventInlet as _;
        use crate::inlet::sqs::{SqsClient, SqsConfig, SqsInlet};

        let Some(sqs) = source.sqs.as_ref().filter(|s| s.enabled) else {
            return;
        };
        let Some(events) = state.events.clone() else {
            return;
        };
        if state
            .inlets
            .get(&source.id)
            .is_some_and(|(t, _)| !t.is_finished())
        {
            return;
        }
        let counters = Arc::new(InletStats::default());
        let (store, http, options, shutdown) = (
            Arc::clone(&self.inner.store),
            self.inner.http.clone(),
            self.inner.sqs,
            self.inner.shutdown.clone(),
        );
        let (id, queue_url, region) = (source.id, sqs.queue_url.clone(), sqs.region().to_owned());
        let task_stats = Arc::clone(&counters);
        info!(source = %id, region = %region, "starting SQS inlet");
        let task = tokio::spawn(async move {
            let s = Arc::clone(&store);
            let credentials = match tokio::task::spawn_blocking(move || s.sqs_credentials(id)).await
            {
                Ok(Ok(c)) => c,
                Ok(Err(e)) => {
                    warn!(source = %id, error = %e, "SQS inlet not started: no credentials");
                    task_stats.error(format!("credentials: {e}"));
                    return;
                }
                Err(e) => {
                    warn!(source = %id, error = %e, "SQS inlet not started");
                    return;
                }
            };
            let config = SqsConfig {
                queue_url,
                region,
                credentials,
            };
            let client = match SqsClient::new(&config, http) {
                Ok(c) => c,
                Err(e) => {
                    warn!(source = %id, error = %e, "SQS inlet not started");
                    task_stats.error(e.to_string());
                    return;
                }
            };
            let inlet = SqsInlet::new(store, id, client, options, task_stats);
            if let Err(e) = inlet.run(events, shutdown).await {
                warn!(source = %id, error = %e, "SQS inlet failed");
            }
        });
        state.inlets.insert(source.id, (task, counters));
    }

    #[cfg(not(feature = "sqs"))]
    #[allow(clippy::unused_self)]
    fn start_inlet(&self, _state: &mut State, _source: &Source) {}

    /// Restarts only the source's SQS inlet, so a changed inlet (queue, credentials, on/off)
    /// applies at once; the scanner keeps its pass. Nothing starts unless the source's
    /// scanner runs. Returns whether an inlet runs now.
    pub fn restart_inlet(&self, source: &Source) -> bool {
        let mut state = self.state();
        if let Some((t, _)) = state.inlets.remove(&source.id) {
            t.abort();
        }
        let scanning = state
            .tasks
            .get(&source.id)
            .is_some_and(|t| !t.is_finished());
        if scanning && !*self.inner.shutdown.borrow() {
            self.start_inlet(&mut state, source);
        }
        state
            .inlets
            .get(&source.id)
            .is_some_and(|(t, _)| !t.is_finished())
    }

    /// Whether the source's SQS inlet runs.
    pub fn is_inlet_running(&self, id: SourceId) -> bool {
        self.state()
            .inlets
            .get(&id)
            .is_some_and(|(t, _)| !t.is_finished())
    }

    /// What the source's SQS inlet did since it last started, if one was started.
    pub fn inlet_stats(&self, id: SourceId) -> Option<InletStatsSnapshot> {
        self.state().inlets.get(&id).map(|(_, s)| s.snapshot())
    }

    /// Stops the source's scanner now, if one runs. A pass under way stops at its next await
    /// point; its checkpoint keeps it resumable, and events sent but not yet committed are sent
    /// again (delivery is at least once). Returns whether a scanner was running.
    pub fn stop_source(&self, id: SourceId) -> bool {
        let (task, inlet) = {
            let mut state = self.state();
            (state.tasks.remove(&id), state.inlets.remove(&id))
        };
        if let Some((t, _)) = inlet
            && !t.is_finished()
        {
            // Messages it received and did not delete come back after the visibility timeout.
            t.abort();
            info!(source = %id, "SQS inlet stopped");
        }
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
            let inlets: Vec<_> = state.inlets.drain().map(|(_, (t, _))| t).collect();
            state.tasks.drain().map(|(_, t)| t).chain(inlets).collect()
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
