//! The ops HTTP server for Kubernetes: `/healthz`, `/readyz` and `/metrics`.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use axum::Router;
use axum::extract::State;
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use prometheus::{Encoder as _, Gauge, IntCounterVec, IntGauge, IntGaugeVec, Opts, Registry};

/// Shared state of the ops endpoints.
pub struct OpsState {
    ready: AtomicBool,
    metrics: Metrics,
}

struct Metrics {
    registry: Registry,
    ready: IntGauge,
    requests: IntCounterVec,
}

impl OpsState {
    pub fn new(instance_id: &str, standalone: bool) -> Self {
        Self {
            ready: AtomicBool::new(false),
            metrics: Metrics::new(instance_id, standalone),
        }
    }

    /// Marks the instance ready (or not) for `/readyz`.
    pub fn set_ready(&self, ready: bool) {
        self.ready.store(ready, Ordering::SeqCst);
        self.metrics.ready.set(i64::from(ready));
    }

    fn is_ready(&self) -> bool {
        self.ready.load(Ordering::SeqCst)
    }
}

impl Metrics {
    fn new(instance_id: &str, standalone: bool) -> Self {
        let registry = Registry::new();
        let build_info = IntGaugeVec::new(
            Opts::new(
                "doewood_build_info",
                "Build and instance information; always 1.",
            ),
            &["version", "instance_id", "mode"],
        )
        .expect("valid metric");
        build_info
            .with_label_values(&[
                crate::VERSION,
                instance_id,
                if standalone { "standalone" } else { "shared" },
            ])
            .set(1);
        let ready = IntGauge::new("doewood_ready", "1 when the instance is ready to serve.")
            .expect("valid metric");
        let start = Gauge::new(
            "doewood_start_time_seconds",
            "Start time of the process, in Unix seconds.",
        )
        .expect("valid metric");
        start.set(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_or(0.0, |d| d.as_secs_f64()),
        );
        let requests = IntCounterVec::new(
            Opts::new(
                "doewood_ops_requests_total",
                "Requests to the ops endpoints.",
            ),
            &["path"],
        )
        .expect("valid metric");
        for m in [
            Box::new(build_info) as Box<dyn prometheus::core::Collector>,
            Box::new(ready.clone()),
            Box::new(start),
            Box::new(requests.clone()),
        ] {
            registry.register(m).expect("unique metric names");
        }
        Self {
            registry,
            ready,
            requests,
        }
    }
}

pub fn router(state: Arc<OpsState>) -> Router {
    Router::new()
        .route("/healthz", get(healthz))
        .route("/readyz", get(readyz))
        .route("/metrics", get(metrics))
        .with_state(state)
}

/// Alive: the process answers.
async fn healthz(State(s): State<Arc<OpsState>>) -> &'static str {
    s.metrics.requests.with_label_values(&["/healthz"]).inc();
    "ok\n"
}

/// Ready: the store is open, and the instance is not shutting down.
async fn readyz(State(s): State<Arc<OpsState>>) -> (StatusCode, &'static str) {
    s.metrics.requests.with_label_values(&["/readyz"]).inc();
    if s.is_ready() {
        (StatusCode::OK, "ready\n")
    } else {
        (StatusCode::SERVICE_UNAVAILABLE, "not ready\n")
    }
}

async fn metrics(State(s): State<Arc<OpsState>>) -> Response {
    s.metrics.requests.with_label_values(&["/metrics"]).inc();
    let encoder = prometheus::TextEncoder::new();
    let mut body = Vec::new();
    match encoder.encode(&s.metrics.registry.gather(), &mut body) {
        Ok(()) => ([(header::CONTENT_TYPE, prometheus::TEXT_FORMAT)], body).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}
