//! An in-process API server per "instance", a tonic client, and the shared backends: SQLite
//! always, and Postgres when `DOEWOOD_TEST_POSTGRES_URL` is set (each test in a fresh schema).

#![allow(dead_code, clippy::missing_panics_doc)]

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use doewood_api::{ApiOptions, ApiParts, ApiState, Icons, OnDemand, Progress, Reprocessor};
use doewood_core::{InstanceId, MasterKey};
use doewood_model::Backend;
use doewood_s3::{ClientOptions, Discovery, DiscoveryOptions, ObjectEvent, RetryPolicy};
use doewood_store::{Standalone, Store};
use tokio::net::TcpListener;
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;
use tonic::transport::Channel;

pub const PG_VAR: &str = "DOEWOOD_TEST_POSTGRES_URL";

/// One running API server.
pub struct Instance {
    pub addr: SocketAddr,
    pub store: Arc<Store>,
    pub progress: Arc<Progress>,
    pub discovery: Discovery,
    /// Sends events as the scanners would; they reach `progress`.
    pub events: mpsc::Sender<ObjectEvent>,
    pub backend: watch::Sender<Option<Arc<dyn Backend>>>,
    /// The API's state, for calls the services do not expose (the start-up claim pass).
    pub state: ApiState,
    shutdown: watch::Sender<bool>,
    server: JoinHandle<Result<(), doewood_api::ServeError>>,
}

pub struct Options {
    pub max_message_bytes: usize,
    pub reprocess: Option<Arc<dyn Reprocessor>>,
    pub summarize: Option<Arc<dyn OnDemand>>,
    /// Answers `GET /icons/{domain}`.
    pub icons: Option<Arc<dyn Icons>>,
    /// As `serve` with a shared store: sources are re-claimed at start.
    pub shared_claims: bool,
    /// How often `WatchProgress` looks at the store unprompted (50 ms here, 1 s in `serve`).
    pub progress_interval: Duration,
    /// The instance store; a new in-memory one when `None`.
    pub store: Option<Arc<Store>>,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            max_message_bytes: 64 * 1024 * 1024,
            reprocess: None,
            summarize: None,
            icons: None,
            shared_claims: false,
            progress_interval: Duration::from_millis(50),
            store: None,
        }
    }
}

impl Instance {
    pub async fn start(name: &str, backend: Option<Arc<dyn Backend>>) -> Self {
        Self::start_with(name, backend, Options::default()).await
    }

    pub async fn start_with(
        name: &str,
        backend: Option<Arc<dyn Backend>>,
        options: Options,
    ) -> Self {
        let store = options.store.clone().unwrap_or_else(|| {
            Arc::new(Store::open_in_memory(&MasterKey::from_bytes([7; 32])).unwrap())
        });
        let (shutdown, shutdown_rx) = watch::channel(false);
        let (events, mut events_rx) = mpsc::channel(1024);
        let progress = Arc::new(Progress::new());
        {
            let progress = Arc::clone(&progress);
            tokio::spawn(async move {
                while let Some(e) = events_rx.recv().await {
                    progress.record(&e);
                }
            });
        }
        // Scans of unreachable test endpoints fail at once.
        let discovery_options = DiscoveryOptions {
            client: ClientOptions {
                connect_timeout: Duration::from_secs(2),
                retry: RetryPolicy {
                    max_attempts: 1,
                    ..RetryPolicy::default()
                },
                ..ClientOptions::default()
            },
            ..DiscoveryOptions::default()
        };
        let discovery = Discovery::idle(
            Arc::clone(&store),
            events.clone(),
            shutdown_rx.clone(),
            discovery_options,
        )
        .unwrap();
        let (backend_tx, backend_rx) = watch::channel(backend);
        let state = ApiState::new(ApiParts {
            store: Arc::clone(&store),
            backend: backend_rx,
            instance: InstanceId::new(name).unwrap(),
            discovery: Some(discovery.clone()),
            progress: Arc::clone(&progress),
            reprocess: options.reprocess,
            summarize: options.summarize,
            icons: options.icons,
            options: ApiOptions {
                max_message_bytes: options.max_message_bytes,
                progress_interval: options.progress_interval,
                shared_claims: options.shared_claims,
            },
            shutdown: shutdown_rx.clone(),
        });
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let mut stop = shutdown_rx;
        let server = tokio::spawn(doewood_api::serve(listener, state.clone(), async move {
            let _ = stop.wait_for(|s| *s).await;
        }));
        Self {
            addr,
            store,
            progress,
            discovery,
            events,
            backend: backend_tx,
            state,
            shutdown,
            server,
        }
    }

    pub async fn channel(&self) -> Channel {
        tonic::transport::Endpoint::from_shared(format!("http://{}", self.addr))
            .unwrap()
            .connect()
            .await
            .unwrap()
    }

    /// A shutdown receiver, as `serve` hands out.
    pub fn shutdown_rx(&self) -> watch::Receiver<bool> {
        self.shutdown.subscribe()
    }

    /// Stops the server; it must stop within a few seconds, streams included.
    pub async fn stop(self) {
        self.shutdown.send_replace(true);
        tokio::time::timeout(Duration::from_secs(10), self.server)
            .await
            .expect("the server stops on shutdown")
            .unwrap()
            .unwrap();
        self.discovery.join().await;
    }
}

/// The "shared database" two instances use: a SQLite store of its own, or a Postgres schema.
pub struct Shared {
    pub name: &'static str,
    pub backend: Arc<dyn Backend>,
    cleanup: Option<(tokio_postgres::Client, String)>,
}

impl Shared {
    pub fn sqlite() -> Self {
        let store = Arc::new(Store::open_in_memory(&MasterKey::from_bytes([9; 32])).unwrap());
        Self {
            name: "sqlite",
            backend: Arc::new(Standalone::new(store)),
            cleanup: None,
        }
    }

    pub async fn postgres() -> Option<Self> {
        let url = std::env::var(PG_VAR).ok().filter(|v| !v.trim().is_empty());
        let Some(url) = url else {
            eprintln!("skipping: {PG_VAR} is not set; the Postgres variant did not run");
            return None;
        };
        let (admin, conn) = tokio_postgres::connect(&url, tokio_postgres::NoTls)
            .await
            .expect("connect to the test database");
        tokio::spawn(conn);
        let schema = format!("dw_api_{}", uuid::Uuid::now_v7().simple());
        admin
            .batch_execute(&format!("CREATE SCHEMA {schema}"))
            .await
            .unwrap();
        let _ = admin
            .batch_execute("CREATE EXTENSION IF NOT EXISTS vector SCHEMA public")
            .await;
        let sep = if url.contains('?') { '&' } else { '?' };
        let scoped = format!("{url}{sep}options=-c%20search_path%3D{schema}");
        let pg = doewood_pg::PgStore::connect(&doewood_core::PostgresUrl::parse(&scoped).unwrap())
            .await
            .unwrap();
        Some(Self {
            name: "postgres",
            backend: Arc::new(pg),
            cleanup: Some((admin, schema)),
        })
    }

    /// SQLite, plus Postgres when configured.
    pub async fn all() -> Vec<Self> {
        let mut out = vec![Self::sqlite()];
        out.extend(Self::postgres().await);
        out
    }

    /// Postgres: `ANALYZE` the schema's tables (after seeding, as autovacuum would).
    pub async fn analyze(&self) {
        if let Some((admin, schema)) = &self.cleanup {
            let tables = admin
                .query(
                    "SELECT tablename FROM pg_tables WHERE schemaname = $1",
                    &[schema],
                )
                .await
                .unwrap();
            for t in tables {
                let table: String = t.get(0);
                admin
                    .batch_execute(&format!("ANALYZE {schema}.{table}"))
                    .await
                    .unwrap();
            }
        }
    }

    pub async fn drop_schema(self) {
        if let Some((admin, schema)) = self.cleanup {
            admin
                .batch_execute(&format!("DROP SCHEMA {schema} CASCADE"))
                .await
                .unwrap();
        }
    }
}

/// A local port nothing listens on.
pub async fn closed_port() -> u16 {
    let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
    l.local_addr().unwrap().port()
}

/// Polls `f` until it returns true, for up to 10 s.
pub async fn eventually<F, Fut>(what: &str, mut f: F)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while !f().await {
        assert!(tokio::time::Instant::now() < deadline, "timed out: {what}");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}
