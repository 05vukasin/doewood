//! doewood's gRPC API (ADR 0001, section 4), served with tonic on `DOEWOOD_LISTEN`.
//!
//! - **Services** (package `doewood.v1`, `proto/doewood/v1/*.proto`): `ConfigService`
//!   (sources, LLM policies, AI providers), `JobsService` (the progress stream and
//!   reprocessing), `RecordsService`, `GraphService`, `SearchService` and `SummarizeService` (its
//!   messages are final; the work arrives with the LLM step).
//! - **gRPC-Web** (`tonic-web`, HTTP/1.1 accepted), so a browser frontend calls the API
//!   directly. No CORS headers are sent: serve the frontend from the same origin.
//! - **Health** (`grpc.health.v1`) and **reflection** (v1 and v1alpha), for probes and
//!   `grpcurl`.
//! - **Auth:** none in v1, but every doewood service runs behind the [`Authorize`]
//!   interceptor, where tokens or OIDC go later.
//! - **Limits:** messages up to [`ApiOptions::max_message_bytes`] (64 MiB by default, room
//!   for a file sent to `Summarize`); every input is validated, and problems come back as
//!   INVALID_ARGUMENT with their field paths. Streams per connection, keepalive and the
//!   connection age (drawn per connection within ±10%) are constants; calls in flight (shed
//!   with UNAVAILABLE beyond the cap) and the server deadline are runtime settings
//!   (`SetApiSettings`). See docs/api.md, "Limits".
//! - **Metrics:** calls by method and code, their duration, calls in flight, shed calls and
//!   connections ([`ApiState::register_metrics`], docs/api.md "Metrics").
//! - **Secrets** are write-only: no response or error carries one.
//! - **`GET /icons/{domain}`** (plain HTTP, on the same port): company icons, always proxied
//!   through doewood (ADR 0002 section 5), behind the [`Icons`] trait.

mod auth;
mod claims;
mod config;
mod convert;
mod enrichment;
mod error;
mod graph;
mod icons;
mod jobs;
mod limits;
mod merges;
mod metrics;
mod progress;
mod proposals;
mod records;
mod reprocess;
mod search;
mod state;
mod summarize;
mod validate;

use std::future::Future;
use std::pin::pin;
use std::sync::Arc;
use std::time::Duration;

use doewood_model::Backend;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;
use tokio::task::JoinSet;
use tokio_stream::{Stream, StreamExt as _};
use tonic::service::Routes;
use tonic::service::interceptor::InterceptedService;
use tonic::transport::Server;
use tonic::transport::server::TcpIncoming;
use tonic_health::ServingStatus;
use tonic_health::server::HealthReporter;
use tonic_web::GrpcWebLayer;
use tower_layer::Layer as _;
use tracing::{debug, warn};

use limits::{Aged, Jitter, Limited};
use metrics::{ApiMetrics, Metered};

pub use auth::Authorize;
pub use claims::{Reclaimed, reclaim_sources};
pub use config::add_source;
pub use icons::{
    CACHE_HIT, CACHE_MISS, CSP, ICON_CONTENT_TYPES, ICONS_PATH, IconAnswer, Icons,
    respond as icon_response,
};
pub use limits::{
    HTTP2_KEEPALIVE_INTERVAL, HTTP2_KEEPALIVE_TIMEOUT, MAX_CONCURRENT_STREAMS, MAX_CONNECTION_AGE,
    MAX_CONNECTION_AGE_GRACE, MAX_CONNECTION_AGE_JITTER_PERCENT, TCP_KEEPALIVE,
};
pub use progress::{EnrichmentCounts, EventCounts, FeedCounts, Progress};
pub use reprocess::{ReprocessPhase, ReprocessView, Reprocessor};
pub use state::{ApiOptions, ApiParts, ApiState};
pub use summarize::{BoxFuture, OnDemand, OnDemandError, OnDemandInput, OnDemandResult};

/// The generated protobuf types, servers and clients of package `doewood.v1`.
#[allow(missing_docs, clippy::all, clippy::pedantic)]
pub mod pb {
    tonic::include_proto!("doewood.v1");

    /// The encoded `FileDescriptorSet` of `doewood.v1`, served by gRPC reflection.
    pub const FILE_DESCRIPTOR_SET: &[u8] =
        include_bytes!(concat!(env!("OUT_DIR"), "/doewood_v1_descriptor.bin"));
}

use pb::config_service_server::ConfigServiceServer;
use pb::graph_service_server::GraphServiceServer;
use pb::jobs_service_server::JobsServiceServer;
use pb::records_service_server::RecordsServiceServer;
use pb::search_service_server::SearchServiceServer;
use pb::summarize_service_server::SummarizeServiceServer;

/// Every service implementation shares this one handle.
#[derive(Clone)]
pub(crate) struct Api {
    state: ApiState,
}

/// Services that work without the records/graph/search backend.
const LOCAL_SERVICES: &[&str] = &[
    pb::config_service_server::SERVICE_NAME,
    pb::jobs_service_server::SERVICE_NAME,
    pb::summarize_service_server::SERVICE_NAME,
];
/// Services that need the backend (the shared Postgres, when configured). The overall
/// status (`""`) follows them.
const BACKED_SERVICES: &[&str] = &[
    "",
    pb::records_service_server::SERVICE_NAME,
    pb::graph_service_server::SERVICE_NAME,
    pb::search_service_server::SERVICE_NAME,
];

/// Why the server stopped or did not start.
#[derive(Debug)]
pub enum ServeError {
    /// The reflection service could not be built from the descriptors.
    Reflection(tonic_reflection::server::Error),
    /// The server failed.
    Transport(tonic::transport::Error),
}

impl std::fmt::Display for ServeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Reflection(e) => write!(f, "gRPC reflection: {e}"),
            Self::Transport(e) => write!(f, "gRPC server: {e}"),
        }
    }
}

impl std::error::Error for ServeError {}

/// Serves the API on `listener` until `shutdown` completes, then stops accepting and waits
/// for the calls in flight. Progress streams end when the state's shutdown signal turns
/// true, so pass a future that completes with it.
pub async fn serve(
    listener: TcpListener,
    state: ApiState,
    shutdown: impl Future<Output = ()> + Send,
) -> Result<(), ServeError> {
    let limit = state.parts().options.max_message_bytes;
    match state.db(doewood_store::Store::api_settings).await {
        Ok(settings) => state.call_limits().set(settings),
        Err(e) => {
            warn!(error = %e.message(), "API limits not read from the store; using the defaults");
        }
    }
    let api = Api {
        state: state.clone(),
    };

    let (reporter, health) = tonic_health::server::health_reporter();
    for name in LOCAL_SERVICES {
        reporter
            .set_service_status(*name, ServingStatus::Serving)
            .await;
    }
    tokio::spawn(track_health(
        reporter,
        state.parts().backend.clone(),
        state.parts().shutdown.clone(),
    ));

    let reflection = || {
        tonic_reflection::server::Builder::configure()
            .register_encoded_file_descriptor_set(pb::FILE_DESCRIPTOR_SET)
            .register_encoded_file_descriptor_set(tonic_health::pb::FILE_DESCRIPTOR_SET)
    };
    let reflection_v1 = reflection().build_v1().map_err(ServeError::Reflection)?;
    let reflection_v1alpha = reflection()
        .build_v1alpha()
        .map_err(ServeError::Reflection)?;

    // The size limits are set on each server, then the auth interceptor wraps it.
    macro_rules! guarded {
        ($server:ident) => {
            InterceptedService::new(
                $server::new(api.clone())
                    .max_decoding_message_size(limit)
                    .max_encoding_message_size(limit),
                Authorize,
            )
        };
    }

    let mut grpc = Routes::builder();
    grpc.add_service(health)
        .add_service(reflection_v1)
        .add_service(reflection_v1alpha)
        .add_service(guarded!(ConfigServiceServer))
        .add_service(guarded!(JobsServiceServer))
        .add_service(guarded!(RecordsServiceServer))
        .add_service(guarded!(GraphServiceServer))
        .add_service(guarded!(SearchServiceServer))
        .add_service(guarded!(SummarizeServiceServer));
    // gRPC-Web wraps the gRPC services only: it answers 400 to any other HTTP/1.1 request,
    // so the icon route sits next to it, and everything else falls through to it. The
    // metrics and the call limits sit inside gRPC-Web, so its calls get them too; the
    // metrics outside the limits, so shed calls are counted; the icon route has its own.
    let metrics = Arc::clone(state.metrics());
    let grpc = Limited::new(
        grpc.routes().prepare(),
        Arc::clone(state.call_limits()),
        Arc::clone(&metrics),
    );
    let grpc = GrpcWebLayer::new().layer(Metered::new(grpc, Arc::clone(&metrics)));
    let app =
        icons::router(state.parts().icons.clone(), Arc::clone(&metrics)).fallback_service(grpc);

    let incoming = TcpIncoming::from(listener)
        .with_nodelay(Some(true))
        .with_keepalive(Some(limits::TCP_KEEPALIVE));
    let ages = Ages {
        age: limits::MAX_CONNECTION_AGE,
        grace: limits::MAX_CONNECTION_AGE_GRACE,
        jitter: Jitter::random(),
    };
    serve_connections(incoming, Routes::from(app), &metrics, ages, shutdown).await;
    Ok(())
}

/// How long connections live: GOAWAY at `age` (jittered per connection), closed `grace`
/// later.
struct Ages {
    age: Duration,
    grace: Duration,
    jitter: Jitter,
}

/// Serves each accepted connection with a tonic server of its own, so each gets its own
/// age (tonic's `max_connection_age` is one value for a whole server): tonic sends GOAWAY at
/// that age, [`Aged`] closes the connection after the grace period. On `shutdown`, stops
/// accepting (the listener closes), lets every connection finish its calls and returns.
async fn serve_connections(
    incoming: impl Stream<Item = std::io::Result<TcpStream>>,
    routes: Routes,
    metrics: &Arc<ApiMetrics>,
    mut ages: Ages,
    shutdown: impl Future<Output = ()>,
) {
    let (stop, stopping) = watch::channel(false);
    let mut connections = JoinSet::new();
    {
        let mut incoming = pin!(incoming);
        let mut shutdown = pin!(shutdown);
        loop {
            tokio::select! {
                () = &mut shutdown => break,
                Some(done) = connections.join_next(), if !connections.is_empty() => {
                    log_connection_end(done);
                }
                io = incoming.next() => match io {
                    None => break,
                    Some(Err(e)) => debug!(error = %e, "accepting a connection failed"),
                    Some(Ok(io)) => {
                        let age = ages.jitter.age(ages.age);
                        let conn = Aged::new(io, age + ages.grace, metrics);
                        let mut stopping = stopping.clone();
                        connections.spawn(
                            server_builder(age)
                                .add_routes(routes.clone())
                                .serve_with_incoming_shutdown(conn, async move {
                                    let _ = stopping.wait_for(|s| *s).await;
                                }),
                        );
                    }
                },
            }
        }
    }
    stop.send_replace(true);
    while let Some(done) = connections.join_next().await {
        log_connection_end(done);
    }
}

fn log_connection_end(done: Result<Result<(), tonic::transport::Error>, tokio::task::JoinError>) {
    match done {
        Ok(Ok(())) => {}
        Ok(Err(e)) => debug!(error = %e, "gRPC connection ended with an error"),
        Err(e) => warn!(error = %e, "gRPC connection task failed"),
    }
}

/// A server for one connection, with the connection limits ([`MAX_CONCURRENT_STREAMS`] and
/// the rest) and GOAWAY at `age`.
fn server_builder(age: Duration) -> Server {
    Server::builder()
        .accept_http1(true)
        .max_concurrent_streams(limits::MAX_CONCURRENT_STREAMS)
        .http2_keepalive_interval(Some(limits::HTTP2_KEEPALIVE_INTERVAL))
        .http2_keepalive_timeout(Some(limits::HTTP2_KEEPALIVE_TIMEOUT))
        .max_connection_age(age)
}

/// Keeps the backed services' health in step with the backend, and reports everything not
/// serving on shutdown.
async fn track_health(
    reporter: HealthReporter,
    mut backend: watch::Receiver<Option<Arc<dyn Backend>>>,
    mut shutdown: watch::Receiver<bool>,
) {
    loop {
        let status = if backend.borrow_and_update().is_some() {
            ServingStatus::Serving
        } else {
            ServingStatus::NotServing
        };
        for name in BACKED_SERVICES {
            reporter.set_service_status(*name, status).await;
        }
        let changed = tokio::select! {
            changed = backend.changed() => changed.is_ok(),
            _ = shutdown.wait_for(|s| *s) => break,
        };
        if !changed {
            // The backend is final (standalone): wait for shutdown only.
            let _ = shutdown.wait_for(|s| *s).await;
            break;
        }
    }
    for name in LOCAL_SERVICES.iter().chain(BACKED_SERVICES) {
        reporter
            .set_service_status(*name, ServingStatus::NotServing)
            .await;
    }
}

#[cfg(test)]
mod tests {
    use tonic_health::pb::HealthCheckRequest;
    use tonic_health::pb::health_client::HealthClient;

    use super::*;

    /// Connections get GOAWAY at their own (jittered) age; the client reconnects for its next
    /// call, and the old connection closes once its calls are done.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn connections_get_goaway_at_their_own_age() {
        let (_reporter, health) = tonic_health::server::health_reporter();
        let mut routes = Routes::builder();
        routes.add_service(health);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let metrics = Arc::new(ApiMetrics::new());
        let (stop, mut stopped) = watch::channel(false);
        let ages = Ages {
            age: Duration::from_millis(400),
            grace: Duration::from_mins(1),
            jitter: Jitter::seeded(1),
        };
        let server = {
            let metrics = Arc::clone(&metrics);
            tokio::spawn(async move {
                serve_connections(
                    TcpIncoming::from(listener),
                    routes.routes(),
                    &metrics,
                    ages,
                    async move {
                        let _ = stopped.wait_for(|s| *s).await;
                    },
                )
                .await;
            })
        };
        let channel = tonic::transport::Endpoint::from_shared(format!("http://{addr}"))
            .unwrap()
            .connect()
            .await
            .unwrap();
        let mut client = HealthClient::new(channel);
        let check = HealthCheckRequest {
            service: String::new(),
        };
        client.check(check.clone()).await.unwrap();
        assert_eq!(metrics.connections_total(), 1);
        // Past the longest age (440 ms): the first connection got GOAWAY and is closed.
        tokio::time::sleep(Duration::from_secs(1)).await;
        assert_eq!(metrics.connections_open(), 0);
        client.check(check).await.unwrap();
        assert_eq!(metrics.connections_total(), 2);
        assert_eq!(metrics.connections_open(), 1);
        drop(client);
        stop.send_replace(true);
        tokio::time::timeout(Duration::from_secs(10), server)
            .await
            .expect("the server stops on shutdown")
            .unwrap();
        assert_eq!(metrics.connections_open(), 0);
    }
}
