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
//!   INVALID_ARGUMENT with their field paths.
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
mod merges;
mod progress;
mod proposals;
mod records;
mod reprocess;
mod search;
mod state;
mod summarize;
mod validate;

use std::future::Future;
use std::sync::Arc;

use doewood_model::Backend;
use tokio::net::TcpListener;
use tokio::sync::watch;
use tonic::service::Routes;
use tonic::service::interceptor::InterceptedService;
use tonic::transport::Server;
use tonic::transport::server::TcpIncoming;
use tonic_health::ServingStatus;
use tonic_health::server::HealthReporter;
use tonic_web::GrpcWebLayer;
use tower_layer::Layer as _;

pub use auth::Authorize;
pub use claims::{Reclaimed, reclaim_sources};
pub use config::add_source;
pub use icons::{
    CACHE_HIT, CACHE_MISS, CSP, ICON_CONTENT_TYPES, ICONS_PATH, IconAnswer, Icons,
    respond as icon_response,
};
pub use progress::{EnrichmentCounts, EventCounts, Progress};
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
    // so the icon route sits next to it, and everything else falls through to it.
    let grpc = GrpcWebLayer::new().layer(grpc.routes().prepare());
    let app = icons::router(state.parts().icons.clone()).fallback_service(grpc);

    Server::builder()
        .accept_http1(true)
        .add_routes(Routes::from(app))
        .serve_with_incoming_shutdown(
            TcpIncoming::from(listener).with_nodelay(Some(true)),
            shutdown,
        )
        .await
        .map_err(ServeError::Transport)
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
