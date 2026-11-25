//! The ops HTTP server: `/healthz`, `/readyz` and `/metrics` for Kubernetes, and the S3 event
//! webhook, `POST /events/s3/{source_id}` (ADR 0001, section 5).
//!
//! The webhook lives here, not on the gRPC port: MinIO and S3 notification targets post plain
//! JSON over HTTP. Like the API, it has no authentication in v1 and binds to localhost by
//! default; on Kubernetes, expose the ops port only inside the cluster.

use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock, PoisonError};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use axum::Router;
use axum::body::Bytes;
use axum::extract::{DefaultBodyLimit, Path, State};
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use doewood_core::SourceId;
use doewood_s3::S3Error;
use doewood_s3::inlet::webhook::{self, WebhookError, WebhookInlet};
use prometheus::{Encoder as _, Gauge, IntCounterVec, IntGauge, IntGaugeVec, Opts, Registry};

/// Shared state of the ops endpoints.
///
/// Ready means the instance can serve: the gRPC API is serving ([`OpsState::set_serving`])
/// and its backend is usable ([`OpsState::set_ready`]; [`OpsState::set_not_ready`] says why
/// not). Either alone is not ready.
pub struct OpsState {
    /// Why the instance is not ready, for `/readyz`, and the two halves of ready.
    gate: Mutex<Gate>,
    metrics: Metrics,
    /// Set once the store is open; cleared on shutdown, so the event channel can close.
    webhook: Mutex<Option<WebhookInlet>>,
    /// When the process started, for the `startup step` and `ready` log lines.
    started: Instant,
    /// The ops server's address, for the `ready` log line.
    addr: OnceLock<SocketAddr>,
    /// Whether the first `ready` was logged.
    announced: AtomicBool,
}

struct Gate {
    /// The gRPC API serves.
    serving: bool,
    /// The backend is usable and the instance is not shutting down.
    healthy: bool,
    /// Why not healthy.
    reason: String,
}

struct Metrics {
    registry: Registry,
    ready: IntGauge,
    requests: IntCounterVec,
}

impl OpsState {
    /// `started`: when the process started (for the startup timings).
    pub fn new(instance_id: &str, standalone: bool, started: Instant) -> Self {
        Self {
            gate: Mutex::new(Gate {
                serving: false,
                healthy: false,
                reason: "starting".to_owned(),
            }),
            metrics: Metrics::new(instance_id, standalone),
            webhook: Mutex::new(None),
            started,
            addr: OnceLock::new(),
            announced: AtomicBool::new(false),
        }
    }

    /// Records the ops server's address, for the `ready` log line.
    pub fn set_addr(&self, addr: SocketAddr) {
        let _ = self.addr.set(addr);
    }

    /// Time since the process started.
    pub fn since_start(&self) -> Duration {
        self.started.elapsed()
    }

    /// Logs one startup step: how long it took (from `since`) and when it ended.
    pub fn step(&self, step: &'static str, since: Instant) {
        tracing::info!(
            step,
            took_ms = millis(since.elapsed()),
            at_ms = millis(self.since_start()),
            "startup step"
        );
    }

    /// Starts accepting webhook notifications.
    pub fn set_webhook(&self, inlet: WebhookInlet) {
        *self.webhook.lock().unwrap_or_else(PoisonError::into_inner) = Some(inlet);
    }

    /// Stops accepting them (503) and drops the inlet's event sender.
    pub fn clear_webhook(&self) {
        self.webhook
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take();
    }

    fn webhook(&self) -> Option<WebhookInlet> {
        self.webhook
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    /// Marks the gRPC API serving: ready from here once the backend is usable.
    pub fn set_serving(&self) {
        let mut gate = self.gate();
        gate.serving = true;
        self.update(&gate);
    }

    /// Marks the backend usable: ready once the gRPC API serves too.
    pub fn set_ready(&self) {
        let mut gate = self.gate();
        gate.healthy = true;
        gate.reason.clear();
        self.update(&gate);
    }

    /// Marks the instance not ready; `/readyz` shows the reason. It must not hold a secret.
    pub fn set_not_ready(&self, why: impl Into<String>) {
        let mut gate = self.gate();
        gate.healthy = false;
        gate.reason = why.into();
        self.update(&gate);
    }

    /// Mirrors the gate into the gauge, and logs the first time the instance is ready.
    fn update(&self, gate: &Gate) {
        let ready = gate.serving && gate.healthy;
        self.metrics.ready.set(i64::from(ready));
        if ready && !self.announced.swap(true, Ordering::SeqCst) {
            tracing::info!(
                addr = self.addr.get().map(ToString::to_string),
                startup_ms = millis(self.since_start()),
                rss_mib = rss_mib(),
                "ready"
            );
        }
    }

    /// The registry `/metrics` serves; the API registers its metrics there.
    pub fn registry(&self) -> &Registry {
        &self.metrics.registry
    }

    fn gate(&self) -> MutexGuard<'_, Gate> {
        self.gate.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// `None` when ready, otherwise why not.
    fn not_ready(&self) -> Option<String> {
        let gate = self.gate();
        if !gate.healthy {
            Some(gate.reason.clone())
        } else if !gate.serving {
            Some("starting: the gRPC API is not serving yet".to_owned())
        } else {
            None
        }
    }
}

/// Whole milliseconds, for the log.
fn millis(d: Duration) -> u64 {
    u64::try_from(d.as_millis()).unwrap_or(u64::MAX)
}

/// The process's resident memory in MiB (`VmRSS`), where `/proc` has it.
pub fn rss_mib() -> Option<u64> {
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    let kib: u64 = status
        .lines()
        .find_map(|l| l.strip_prefix("VmRSS:"))?
        .trim()
        .strip_suffix("kB")?
        .trim()
        .parse()
        .ok()?;
    Some(kib.div_ceil(1024))
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
        .route(
            "/events/s3/{source_id}",
            post(s3_event).layer(DefaultBodyLimit::max(webhook::MAX_BODY)),
        )
        .with_state(state)
}

/// Alive: the process answers.
async fn healthz(State(s): State<Arc<OpsState>>) -> &'static str {
    s.metrics.requests.with_label_values(&["/healthz"]).inc();
    "ok\n"
}

/// Ready: the local store is open, the gRPC API serves, the shared store (when configured)
/// answers, and the instance is not shutting down. Otherwise 503 with the reason.
async fn readyz(State(s): State<Arc<OpsState>>) -> (StatusCode, String) {
    s.metrics.requests.with_label_values(&["/readyz"]).inc();
    match s.not_ready() {
        None => (StatusCode::OK, "ready\n".to_owned()),
        Some(reason) => (
            StatusCode::SERVICE_UNAVAILABLE,
            format!("not ready: {reason}\n"),
        ),
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

/// A MinIO / S3 event notification for one source. Answers 200 with what was done, 400 for a
/// body that is not a notification, 404 for an unknown source, 413 for a body over 1 MiB, and
/// 503 before the store is open or during shutdown.
async fn s3_event(
    State(s): State<Arc<OpsState>>,
    Path(source_id): Path<String>,
    body: Bytes,
) -> Response {
    s.metrics.requests.with_label_values(&["/events/s3"]).inc();
    let text = |status: StatusCode, body: &str| (status, body.to_owned()).into_response();
    let Ok(source) = source_id.parse::<SourceId>() else {
        return text(StatusCode::NOT_FOUND, "no such source\n");
    };
    let Some(inlet) = s.webhook() else {
        return text(
            StatusCode::SERVICE_UNAVAILABLE,
            "not accepting events yet\n",
        );
    };
    match inlet.handle(source, &body).await {
        Ok(outcome) => (
            [(header::CONTENT_TYPE, "application/json")],
            format!(
                "{{\"emitted\":{},\"ignored\":{}}}\n",
                outcome.emitted, outcome.ignored
            ),
        )
            .into_response(),
        Err(WebhookError::Parse(e)) => text(
            StatusCode::BAD_REQUEST,
            &format!("not an S3 event notification: {e}\n"),
        ),
        Err(WebhookError::TooLarge) => text(
            StatusCode::PAYLOAD_TOO_LARGE,
            "notification body too large\n",
        ),
        Err(WebhookError::UnknownSource(_)) => text(StatusCode::NOT_FOUND, "no such source\n"),
        Err(WebhookError::S3(S3Error::ChannelClosed)) => {
            text(StatusCode::SERVICE_UNAVAILABLE, "shutting down\n")
        }
        Err(e) => {
            tracing::warn!(source = %source, error = %e, "webhook notification failed");
            text(
                StatusCode::INTERNAL_SERVER_ERROR,
                "could not record the notification\n",
            )
        }
    }
}

#[cfg(test)]
mod tests {
    use doewood_core::MasterKey;
    use doewood_store::{NewSource, Store};
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
    use tokio::sync::mpsc;

    use super::*;

    async fn post(addr: std::net::SocketAddr, path: &str, body: &str) -> (u16, String) {
        let mut s = tokio::net::TcpStream::connect(addr).await.unwrap();
        let req = format!(
            "POST {path} HTTP/1.1\r\nHost: test\r\nContent-Type: application/json\r\n\
             Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        );
        // A body over the limit may be refused before it is all written.
        let _ = s.write_all(req.as_bytes()).await;
        let mut resp = Vec::new();
        let _ = s.read_to_end(&mut resp).await;
        let resp = String::from_utf8_lossy(&resp);
        let status = resp
            .split(' ')
            .nth(1)
            .and_then(|c| c.parse().ok())
            .unwrap_or(0);
        let body = resp
            .split_once("\r\n\r\n")
            .map_or("", |(_, b)| b)
            .to_owned();
        (status, body)
    }

    #[test]
    fn ready_needs_the_api_serving_and_a_usable_backend() {
        let ready = |s: &OpsState| s.metrics.ready.get();
        // Backend first (standalone; or Postgres connecting before the API serves).
        let s = OpsState::new("test", true, Instant::now());
        assert_eq!(s.not_ready().as_deref(), Some("starting"));
        s.set_ready();
        assert_eq!(
            s.not_ready().as_deref(),
            Some("starting: the gRPC API is not serving yet")
        );
        assert_eq!(ready(&s), 0);
        s.set_serving();
        assert_eq!(s.not_ready(), None);
        assert_eq!(ready(&s), 1);
        s.set_not_ready("shutting down");
        assert_eq!(s.not_ready().as_deref(), Some("shutting down"));
        assert_eq!(ready(&s), 0);

        // The API first (Postgres slower than the local start): the backend's reason shows.
        let s = OpsState::new("test", false, Instant::now());
        s.set_not_ready("connecting to the shared Postgres");
        s.set_serving();
        assert_eq!(
            s.not_ready().as_deref(),
            Some("connecting to the shared Postgres")
        );
        assert_eq!(ready(&s), 0);
        s.set_ready();
        assert_eq!(s.not_ready(), None);
        assert_eq!(ready(&s), 1);
        assert!(s.announced.load(Ordering::SeqCst));
    }

    #[test]
    fn rss_is_read_where_proc_has_it() {
        if cfg!(target_os = "linux") {
            assert!(rss_mib().is_some_and(|m| m > 0));
        }
    }

    const PUT: &str = r#"{"Records":[{"eventName":"s3:ObjectCreated:Put",
        "s3":{"bucket":{"name":"docs"},"object":{"key":"in/a.pdf","size":42,"eTag":"e1"}}}]}"#;

    #[tokio::test]
    async fn the_webhook_turns_notifications_into_events() {
        let store = Arc::new(Store::open_in_memory(&MasterKey::from_bytes([1; 32])).unwrap());
        let mut new = NewSource::new("http://minio:9000", "docs");
        new.prefix = "in/".into();
        let id = store.add_source(&new, None).unwrap();
        let state = Arc::new(OpsState::new("test", true, Instant::now()));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(axum::serve(listener, router(Arc::clone(&state))).into_future());
        let path = format!("/events/s3/{id}");

        // Before the store is open.
        assert_eq!(post(addr, &path, PUT).await.0, 503);
        let (tx, mut rx) = mpsc::channel(8);
        state.set_webhook(WebhookInlet::new(Arc::clone(&store), tx));

        let (status, body) = post(addr, &path, PUT).await;
        assert_eq!(status, 200, "{body}");
        assert_eq!(body, "{\"emitted\":1,\"ignored\":0}\n");
        let event = rx.recv().await.unwrap();
        assert_eq!((event.source, event.key.as_str()), (id, "in/a.pdf"));
        // The same object again is not new.
        assert_eq!(
            post(addr, &path, PUT).await.1,
            "{\"emitted\":0,\"ignored\":1}\n"
        );

        assert_eq!(post(addr, &path, "not json").await.0, 400);
        let other = format!("/events/s3/{}", SourceId::generate());
        assert_eq!(post(addr, &other, PUT).await.0, 404);
        assert_eq!(post(addr, "/events/s3/not-an-id", PUT).await.0, 404);
        let big = format!(
            "{{\"Records\":[],\"pad\":\"{}\"}}",
            "x".repeat(webhook::MAX_BODY)
        );
        assert_eq!(post(addr, &path, &big).await.0, 413);

        // Shutdown: refused again, and the inlet's sender is gone.
        state.clear_webhook();
        assert_eq!(post(addr, &path, PUT).await.0, 503);
        assert!(rx.recv().await.is_none());
    }
}
