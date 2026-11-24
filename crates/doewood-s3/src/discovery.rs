//! Starts one scanner task per scanned source and stops them all on shutdown.

use std::sync::Arc;

use doewood_store::{Source, Store};
use tokio::sync::{mpsc, watch};
use tokio::task::JoinSet;
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

/// The running scanners.
#[derive(Debug)]
pub struct Discovery {
    tasks: JoinSet<()>,
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
        let mut tasks = JoinSet::new();
        let to_scan: Vec<Source> = sources
            .into_iter()
            .filter(|s| s.state.is_scanned())
            .collect();
        if to_scan.is_empty() {
            info!("no active sources: discovery is idle");
            return Ok(Self { tasks });
        }
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
        for source in &to_scan {
            info!(
                source = %source.id,
                bucket = %source.bucket,
                prefix = %source.prefix,
                interval_s = source.scan_interval.as_secs(),
                state = %source.state,
                "starting scanner"
            );
            let scanner = Scanner::new(Arc::clone(&store), source, events.clone(), options.scan);
            tasks.spawn(scanner.run(Arc::clone(&connect), shutdown.clone()));
        }
        Ok(Self { tasks })
    }

    /// The number of scanners running.
    pub fn len(&self) -> usize {
        self.tasks.len()
    }

    /// Whether no scanner runs.
    pub fn is_empty(&self) -> bool {
        self.tasks.is_empty()
    }

    /// Waits for every scanner to stop (after shutdown was requested).
    pub async fn join(mut self) {
        while let Some(result) = self.tasks.join_next().await {
            if let Err(e) = result {
                warn!(error = %e, "a scanner task failed");
            }
        }
    }
}
