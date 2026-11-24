//! `doewood serve`: bootstrap, the ops server, the store, and graceful shutdown.
//!
//! Start order:
//! 1. log the effective configuration and every warning;
//! 2. prepare the data directory (the only fatal configuration check);
//! 3. register the signal handlers, so a SIGTERM during startup still shuts down cleanly;
//! 4. start the ops server (`/healthz` answers, `/readyz` says not ready);
//! 5. resolve the master key and open the store, then report ready.
//!
//! On SIGTERM or SIGINT: report not ready, stop the servers, close the store, exit 0.

use std::process::ExitCode;
use std::sync::Arc;

use doewood_core::{BootstrapConfig, Loaded, MasterKey, MasterKeySource};
use doewood_store::keyfile::{self, KeyFileState};
use doewood_store::{STORE_FILE, Store};
use tokio::net::TcpListener;
use tokio::sync::watch;
use tracing::{error, info, warn};

use crate::ops::{self, OpsState};

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

    // Step 5 of the plan: the gRPC API binds `config.listen` here and stops on `shutdown_rx`.
    info!(listen = %config.listen, "gRPC API is not part of this build yet");

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
    state.set_ready(true);
    info!(addr = %ops_addr, "ready");

    let signal = signals.recv().await;
    info!(signal, "shutting down");
    state.set_ready(false);
    let _ = shutdown_tx.send(true);
    match ops_server.await {
        Ok(Ok(())) => {}
        Ok(Err(e)) => warn!(error = %e, "ops server ended with an error"),
        Err(e) => warn!(error = %e, "ops server task failed"),
    }

    // Nothing else holds the store once the servers have stopped.
    let store = Arc::try_unwrap(store).map_err(|_| "store still in use at shutdown".to_owned())?;
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

fn master_key(source: MasterKeySource) -> Result<MasterKey, String> {
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
