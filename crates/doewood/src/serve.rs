//! `doewood serve`: bootstrap, the ops server, the store, and graceful shutdown.
//!
//! Start order:
//! 1. log the effective configuration and every warning;
//! 2. prepare the data directory (the only fatal configuration check);
//! 3. register the signal handlers, so a SIGTERM during startup still shuts down cleanly;
//! 4. start the ops server (`/healthz` answers, `/readyz` says not ready) and bind the gRPC
//!    address (both fatal when the address cannot be bound);
//! 5. resolve the master key and open the store;
//! 6. standalone: serve records, graph and search from the store and report ready. With
//!    `DOEWOOD_POSTGRES_URL`: connect to the shared Postgres in the background, and report ready
//!    once it answers. Until then the instance runs degraded: `/readyz` says why, and the
//!    connection is retried with backoff (see [`crate::shared`]).
//! 7. build ingest (the quail engine, with OCR when built and its models are there) and start
//!    its workers, and start the enrichment job (ADR 0002 section 6); they wait for the backend
//!    while the shared Postgres is not connected.
//! 8. start one scanner per active S3 source (none configured: nothing starts), accept S3
//!    event notifications on the ops server, and serve the gRPC API. Sources added through
//!    the API start scanning at once. With `DOEWOOD_POSTGRES_URL`, no scanner starts before
//!    the shared Postgres answers: the instance then re-claims each of its sources there and
//!    scans those it owns; a source another instance claimed is marked and not scanned (see
//!    [`doewood_api::reclaim_sources`]). Discovery events go to ingest through bounded
//!    channels, so a busy ingest slows the scanners down instead of growing a queue.
//!
//! On SIGTERM or SIGINT: report not ready, stop the servers (the API finishes the calls in
//! flight and ends its streams), the scanners (each keeps its checkpoint), ingest (each
//! worker finishes its file; the rest stays pending) and enrichment (the companies in flight
//! get a few seconds; an unfinished one is enriched again later), close the store, exit 0.

use std::process::ExitCode;
use std::sync::Arc;
use std::time::Duration;

use doewood_api::{ApiOptions, ApiParts, ApiState, Progress};
use doewood_core::{BootstrapConfig, Loaded, MasterKey, MasterKeySource};
use doewood_model::Backend;
use doewood_s3::inlet::webhook::WebhookInlet;
use doewood_s3::{Discovery, DiscoveryOptions, EVENT_CHANNEL_CAPACITY, ObjectEvent};
use doewood_store::keyfile::{self, KeyFileState};
use doewood_store::{STORE_FILE, Standalone, Store};
use tokio::net::TcpListener;
use tokio::sync::{mpsc, watch};
use tracing::{debug, error, info, warn};

/// How long the API may take to finish its calls after shutdown starts.
const API_DRAIN: Duration = Duration::from_secs(10);

use crate::ops::{self, OpsState};
use crate::{cleanup, enrichment, icons, ingest, shared};

/// Builds the runtime and runs the service until a shutdown signal.
pub fn main(loaded: Loaded) -> ExitCode {
    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_name("doewood")
        .build()
    {
        Ok(rt) => rt,
        Err(e) => {
            error!(error = %e, "cannot start the async runtime");
            return ExitCode::FAILURE;
        }
    };
    match runtime.block_on(run(loaded)) {
        Ok(()) => ExitCode::SUCCESS,
        Err(message) => {
            error!(error = %message, "doewood stopped");
            ExitCode::FAILURE
        }
    }
}

// The start and stop order reads top to bottom, step by step as the module documentation lists
// it; splitting it would only scatter that order.
#[allow(clippy::too_many_lines)]
async fn run(Loaded { config, warnings }: Loaded) -> Result<(), String> {
    info!(
        version = crate::VERSION,
        instance_id = %config.instance_id,
        "doewood starting"
    );
    for entry in config.entries() {
        info!(var = entry.var, value = %entry.value, origin = %entry.origin, "bootstrap setting");
    }
    for w in &warnings {
        warn!(var = w.var, "{}", w.message);
    }
    if config.is_standalone() {
        info!("no shared Postgres configured: running standalone");
    }

    config.prepare_data_dir().map_err(|e| e.to_string())?;
    let mut signals = Signals::register().map_err(|e| format!("signal handlers: {e}"))?;

    let state = Arc::new(OpsState::new(
        config.instance_id.as_str(),
        config.is_standalone(),
    ));
    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    let listener = TcpListener::bind(config.ops_listen)
        .await
        .map_err(|e| format!("cannot bind the ops server to {}: {e}", config.ops_listen))?;
    let ops_addr = listener.local_addr().map_err(|e| e.to_string())?;
    info!(addr = %ops_addr, "ops server listening (/healthz, /readyz, /metrics)");
    let ops_server = tokio::spawn(
        axum::serve(listener, ops::router(Arc::clone(&state)))
            .with_graceful_shutdown(wait_for(shutdown_rx.clone()))
            .into_future(),
    );

    // The gRPC API binds now, so a taken port fails the start; it serves once the store is
    // open.
    let api_listener = match TcpListener::bind(config.listen).await {
        Ok(l) => l,
        Err(e) => {
            let _ = shutdown_tx.send(true);
            let _ = ops_server.await;
            return Err(format!(
                "cannot bind the gRPC API to {}: {e}",
                config.listen
            ));
        }
    };

    let store = tokio::select! {
        store = open_store(&config) => store,
        signal = signals.recv() => {
            info!(signal, "shutdown requested during startup");
            let _ = shutdown_tx.send(true);
            let _ = ops_server.await;
            return Ok(());
        }
    };
    let store = match store {
        Ok(store) => Arc::new(store),
        Err(e) => {
            let _ = shutdown_tx.send(true);
            let _ = ops_server.await;
            return Err(e);
        }
    };

    // Records, graph, claims and search: the shared Postgres when configured, else this file.
    let (backend_tx, backend_rx) = watch::channel::<Option<Arc<dyn Backend>>>(None);
    let supervisor = if let Some(url) = config.postgres_url.clone() {
        // Degraded until the supervisor connects; it retries for as long as it takes.
        Some(tokio::spawn(shared::supervise(
            url,
            Arc::clone(&state),
            backend_tx,
            shutdown_rx.clone(),
        )))
    } else {
        backend_tx.send_replace(Some(Arc::new(Standalone::new(Arc::clone(&store)))));
        // The value stays readable through `backend_rx`.
        drop(backend_tx);
        state.set_ready();
        info!(addr = %ops_addr, "ready");
        None
    };

    // Expired registry answers and icons are purged from the backend every hour, and the
    // instance store's planner statistics refreshed.
    let purging = cleanup::start(Arc::clone(&store), backend_rx.clone(), shutdown_rx.clone());

    let progress = Arc::new(Progress::new());
    let ingest = ingest::build(&config, &store, backend_rx.clone(), &progress).await;
    let (ingest_tx, ingest_rx) = mpsc::channel(EVENT_CHANNEL_CAPACITY);
    // Without ingest the receiver is dropped here, and no events are sent to it.
    let ingesting = ingest
        .as_ref()
        .map(|(i, _)| i.start(ingest_rx, shutdown_rx.clone()));
    let summarizing = ingest.as_ref().map(|(_, s)| s.start(shutdown_rx.clone()));
    let enriching = enrichment::start(
        &config,
        &store,
        backend_rx.clone(),
        &progress,
        shutdown_rx.clone(),
    );
    let shared = !config.is_standalone();
    let scanning = start_discovery(
        Arc::clone(&store),
        Arc::clone(&progress),
        ingest.is_some().then_some(ingest_tx),
        shutdown_rx.clone(),
        shared,
    )
    .await;
    state.set_webhook(WebhookInlet::new(
        Arc::clone(&store),
        scanning.events.clone(),
    ));
    let api = ApiState::new(ApiParts {
        store: Arc::clone(&store),
        backend: backend_rx.clone(),
        instance: config.instance_id.clone(),
        discovery: scanning.discovery.clone(),
        progress,
        reprocess: ingest.as_ref().map(|(i, _)| {
            Arc::new(ingest::Reprocess(i.clone())) as Arc<dyn doewood_api::Reprocessor>
        }),
        summarize: ingest
            .map(|(_, s)| Arc::new(ingest::OnDemandSummaries(s)) as Arc<dyn doewood_api::OnDemand>),
        icons: Some(Arc::new(icons::FaviconIcons::new(
            Arc::clone(&store),
            backend_rx.clone(),
        ))),
        options: ApiOptions {
            max_message_bytes: config.api_max_message_bytes,
            shared_claims: shared,
            ..ApiOptions::default()
        },
        shutdown: shutdown_rx.clone(),
    });
    // With a shared store, the sources are re-claimed once it answers, and only then scanned.
    let reclaiming = shared.then(|| {
        let (api, shutdown) = (api.clone(), shutdown_rx.clone());
        tokio::spawn(async move { doewood_api::reclaim_sources(&api, shutdown).await })
    });
    let api_server = start_api(&config, api_listener, api)?;

    let signal = signals.recv().await;
    info!(signal, "shutting down");
    state.set_not_ready("shutting down");
    let _ = shutdown_tx.send(true);
    match ops_server.await {
        Ok(Ok(())) => {}
        Ok(Err(e)) => warn!(error = %e, "ops server ended with an error"),
        Err(e) => warn!(error = %e, "ops server task failed"),
    }
    stop_api(api_server).await;
    if let Some(task) = reclaiming
        && let Err(e) = task.await
    {
        warn!(error = %e, "source claim task failed");
    }
    state.clear_webhook();
    scanning.stop().await;
    if let Some(handle) = ingesting {
        handle.join().await;
        info!("ingest stopped");
    }
    if let Some(task) = summarizing
        && let Err(e) = task.await
    {
        warn!(error = %e, "LLM queue task failed");
    }
    if let Err(e) = enriching.await {
        warn!(error = %e, "enrichment task failed");
    }
    if let Some(task) = supervisor
        && let Err(e) = task.await
    {
        warn!(error = %e, "shared store supervisor failed");
    }
    if let Err(e) = purging.await {
        warn!(error = %e, "cache purge task failed");
    }
    let backend = backend_rx.borrow().as_ref().map(|b| b.name());
    info!(backend, "servers stopped");
    drop(backend_rx);

    // Nothing else holds the store once the servers and the backend are gone; a blocking
    // store call that was in flight may still be finishing.
    let store = release(store).await?;
    tokio::task::spawn_blocking(move || store.close())
        .await
        .map_err(|e| e.to_string())?
        .map_err(|e| format!("closing the store: {e}"))?;
    info!("store closed; bye");
    Ok(())
}

async fn open_store(config: &BootstrapConfig) -> Result<Store, String> {
    let source = config.master_key.clone();
    let path = config.data_dir.join(STORE_FILE);
    tokio::task::spawn_blocking(move || {
        let key = master_key(source)?;
        let store = Store::open(&path, &key)
            .map_err(|e| format!("cannot open the store {}: {e}", path.display()))?;
        let version = store.schema_version().map_err(|e| e.to_string())?;
        info!(path = %path.display(), schema = version, "store open");
        Ok(store)
    })
    .await
    .map_err(|e| e.to_string())?
}

pub(crate) fn master_key(source: MasterKeySource) -> Result<MasterKey, String> {
    match source {
        MasterKeySource::Env(key) => {
            info!("master key from DOEWOOD_MASTER_KEY");
            Ok(key)
        }
        MasterKeySource::File(path) => {
            let (key, state) = keyfile::load_or_generate(&path).map_err(|e| e.to_string())?;
            match state {
                KeyFileState::Generated => warn!(
                    path = %path.display(),
                    "!!! DOEWOOD_MASTER_KEY is not set: GENERATED a master key into the data \
                     directory. This is for DEVELOPMENT ONLY. Anyone who can read this file can \
                     decrypt every stored secret, and losing it loses them all. In production set \
                     DOEWOOD_MASTER_KEY (base64 of 32 random bytes) from a secret store. !!!"
                ),
                KeyFileState::Loaded { loose_permissions } => {
                    warn!(
                        path = %path.display(),
                        "!!! DOEWOOD_MASTER_KEY is not set: using the master key file in the \
                         data directory. This is for DEVELOPMENT ONLY; in production set \
                         DOEWOOD_MASTER_KEY. !!!"
                    );
                    if loose_permissions {
                        warn!(path = %path.display(), "the key file is readable by group or others; chmod 600 it");
                    }
                }
            }
            Ok(key)
        }
    }
}

type ApiTask = tokio::task::JoinHandle<Result<(), doewood_api::ServeError>>;

/// Serves the gRPC API until the shutdown signal.
fn start_api(
    config: &BootstrapConfig,
    listener: TcpListener,
    state: ApiState,
) -> Result<ApiTask, String> {
    let addr = listener.local_addr().map_err(|e| e.to_string())?;
    let shutdown = state.shutdown();
    info!(
        addr = %addr,
        max_message_mib = config.api_max_message_bytes / (1024 * 1024),
        "gRPC API listening (gRPC and gRPC-Web, health, reflection)"
    );
    Ok(tokio::spawn(doewood_api::serve(
        listener,
        state,
        wait_for(shutdown),
    )))
}

/// Waits for the API to finish its calls, or gives up after [`API_DRAIN`].
async fn stop_api(task: ApiTask) {
    let abort = task.abort_handle();
    match tokio::time::timeout(API_DRAIN, task).await {
        Ok(Ok(Ok(()))) => {}
        Ok(Ok(Err(e))) => warn!(error = %e, "gRPC API ended with an error"),
        Ok(Err(e)) => warn!(error = %e, "gRPC API task failed"),
        Err(_) => {
            warn!("gRPC API calls did not finish in time; dropping them");
            abort.abort();
        }
    }
}

/// Takes the store back from its last `Arc`, waiting up to 5 s for blocking calls in flight.
async fn release(mut store: Arc<Store>) -> Result<Store, String> {
    for _ in 0..50 {
        match Arc::try_unwrap(store) {
            Ok(s) => return Ok(s),
            Err(still_shared) => store = still_shared,
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    Err("store still in use at shutdown".to_owned())
}

/// The scanners and the task that takes their events.
struct Scanning {
    discovery: Option<Discovery>,
    /// For the webhook inlet; dropped with this struct.
    events: mpsc::Sender<ObjectEvent>,
    sink: tokio::task::JoinHandle<()>,
}

impl Scanning {
    /// Waits for the scanners (told to stop by the shutdown signal), then for the sink.
    async fn stop(self) {
        let Self {
            discovery,
            events,
            sink,
        } = self;
        drop(events);
        if let Some(discovery) = discovery {
            discovery.join().await;
            info!("scanners stopped");
        }
        // Every sender is gone once the scanners and the webhook have stopped.
        let _ = sink.await;
    }
}

/// Starts one scanner per active source; with a shared store, none yet (the start-up claim
/// pass starts them).
async fn start_discovery(
    store: Arc<Store>,
    progress: Arc<Progress>,
    ingest: Option<mpsc::Sender<ObjectEvent>>,
    shutdown: watch::Receiver<bool>,
    shared: bool,
) -> Scanning {
    // Every event is counted for `WatchProgress`, then handed to ingest (or, without it,
    // logged and dropped).
    let (events_tx, events_rx) = mpsc::channel(EVENT_CHANNEL_CAPACITY);
    let sink = tokio::spawn(forward_events(events_rx, progress, ingest));
    let options = DiscoveryOptions::default();
    let started = if shared {
        Discovery::idle(store, events_tx.clone(), shutdown, options)
    } else {
        Discovery::start(store, events_tx.clone(), shutdown, options).await
    };
    let discovery = match started {
        Ok(d) if shared => {
            info!(
                "discovery started; the scanners start once the sources are re-claimed in the shared store"
            );
            Some(d)
        }
        Ok(d) => {
            info!(scanners = d.len(), "discovery started");
            Some(d)
        }
        Err(e) => {
            error!(error = %e, "discovery did not start; no source is scanned");
            None
        }
    };
    Scanning {
        discovery,
        events: events_tx,
        sink,
    }
}

async fn forward_events(
    mut events: mpsc::Receiver<ObjectEvent>,
    progress: Arc<Progress>,
    ingest: Option<mpsc::Sender<ObjectEvent>>,
) {
    while let Some(e) = events.recv().await {
        progress.record(&e);
        debug!(source = %e.source, key = %e.key, kind = e.kind.name(), "object event");
        // Waits while ingest's queue is full: the scanners wait in turn. A closed ingest
        // (shutdown) drops the event; the object stays pending for the next start.
        if let Some(tx) = &ingest {
            let _ = tx.send(e).await;
        }
    }
}

async fn wait_for(mut rx: watch::Receiver<bool>) {
    let _ = rx.wait_for(|stop| *stop).await;
}

/// SIGTERM and SIGINT on Unix; Ctrl-C elsewhere.
struct Signals {
    #[cfg(unix)]
    term: tokio::signal::unix::Signal,
    #[cfg(unix)]
    int: tokio::signal::unix::Signal,
}

impl Signals {
    // Registration can fail only on Unix; elsewhere the Result is always Ok.
    #[cfg_attr(not(unix), allow(clippy::unnecessary_wraps))]
    fn register() -> std::io::Result<Self> {
        #[cfg(unix)]
        {
            use tokio::signal::unix::{SignalKind, signal};
            Ok(Self {
                term: signal(SignalKind::terminate())?,
                int: signal(SignalKind::interrupt())?,
            })
        }
        #[cfg(not(unix))]
        {
            Ok(Self {})
        }
    }

    async fn recv(&mut self) -> &'static str {
        #[cfg(unix)]
        {
            tokio::select! {
                _ = self.term.recv() => "SIGTERM",
                _ = self.int.recv() => "SIGINT",
            }
        }
        #[cfg(not(unix))]
        {
            match tokio::signal::ctrl_c().await {
                Ok(()) => "ctrl-c",
                Err(e) => {
                    error!(error = %e, "cannot listen for ctrl-c; running until killed");
                    std::future::pending().await
                }
            }
        }
    }
}
