//! gRPC request metrics (issue #79, docs/api.md "Metrics"), registered on the ops `/metrics`
//! endpoint with [`crate::ApiState::register_metrics`].
//!
//! [`Metered`] sits inside gRPC-Web and outside the call limits, so gRPC-Web calls and shed
//! calls are counted like the others. A call is counted when its response ends: a unary call
//! when its answer is sent, a stream when it closes (its duration is its lifetime). The code
//! is the `grpc-status` of the trailers (or of the headers, for an error without a body);
//! a call the client dropped before the end is `CANCELLED`.
//!
//! The `method` label comes from the fixed list of served methods (generated from the
//! descriptors by build.rs); any other path is `other`, so the label set stays bounded. The
//! label handles are looked up once per method and code, then a call costs a few atomic
//! operations and one small allocation (the response body wrapper).

use std::convert::Infallible;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, OnceLock};
use std::task::{Context, Poll, ready};
use std::time::Instant;

use http_body::{Frame, SizeHint};
use prometheus::{
    Histogram, HistogramOpts, HistogramVec, IntCounter, IntCounterVec, IntGauge, IntGaugeVec, Opts,
    Registry,
};
use tonic::Status;
use tonic::body::Body;
use tonic::codegen::Bytes;
use tonic::codegen::Service;
use tonic::codegen::http::{HeaderMap, Request, Response};

include!(concat!(env!("OUT_DIR"), "/grpc_methods.rs"));

/// The `method` label of a path that is not a served method.
const OTHER: &str = "other";

/// Buckets of the call duration, in seconds: from 1 ms (a cached read) to 30 min (a
/// `WatchProgress` that lives as long as its connection).
const DURATION_BUCKETS: &[f64] = &[
    0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0,
    300.0, 1800.0,
];

/// The `code` label of each gRPC status code, by number.
const CODES: [&str; 17] = [
    "OK",
    "CANCELLED",
    "UNKNOWN",
    "INVALID_ARGUMENT",
    "DEADLINE_EXCEEDED",
    "NOT_FOUND",
    "ALREADY_EXISTS",
    "PERMISSION_DENIED",
    "RESOURCE_EXHAUSTED",
    "FAILED_PRECONDITION",
    "ABORTED",
    "OUT_OF_RANGE",
    "UNIMPLEMENTED",
    "INTERNAL",
    "UNAVAILABLE",
    "DATA_LOSS",
    "UNAUTHENTICATED",
];
const CANCELLED: u8 = 1;
const UNKNOWN: u8 = 2;

/// The outcome of one `GET /icons/{domain}`: 200, 404, 400 or 503.
#[derive(Clone, Copy, Debug)]
pub(crate) enum IconOutcome {
    Found,
    Missing,
    Invalid,
    Busy,
}

/// The `outcome` labels, in the order of [`IconOutcome`].
const ICON_OUTCOMES: [&str; 4] = ["found", "missing", "invalid", "busy"];

/// The API's metrics. Cheap to share; registered once on the ops registry.
pub struct ApiMetrics {
    calls: IntCounterVec,
    duration: HistogramVec,
    in_flight: IntGaugeVec,
    shed: IntCounter,
    connections_open: IntGauge,
    connections: IntCounter,
    icons: [IntCounter; ICON_OUTCOMES.len()],
    icons_vec: IntCounterVec,
    /// One per entry of [`METHODS`], then `other`.
    methods: Box<[Method]>,
}

/// The handles of one method, made on its first call.
struct Method {
    label: &'static str,
    handles: OnceLock<Handles>,
}

struct Handles {
    in_flight: IntGauge,
    duration: Histogram,
    codes: [OnceLock<IntCounter>; CODES.len()],
}

impl std::fmt::Debug for ApiMetrics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ApiMetrics").finish_non_exhaustive()
    }
}

impl ApiMetrics {
    pub(crate) fn new() -> Self {
        let calls = IntCounterVec::new(
            Opts::new(
                "doewood_grpc_calls_total",
                "gRPC calls (gRPC-Web included) that ended, by method and status code.",
            ),
            &["method", "code"],
        )
        .expect("valid metric");
        let duration = HistogramVec::new(
            HistogramOpts::new(
                "doewood_grpc_call_duration_seconds",
                "Duration of gRPC calls, by method: until the answer for a unary call, \
                 the lifetime of a stream.",
            )
            .buckets(DURATION_BUCKETS.to_vec()),
            &["method"],
        )
        .expect("valid metric");
        let in_flight = IntGaugeVec::new(
            Opts::new(
                "doewood_grpc_calls_in_flight",
                "gRPC calls running now, open streams included, by method.",
            ),
            &["method"],
        )
        .expect("valid metric");
        let shed = IntCounter::new(
            "doewood_grpc_shed_total",
            "gRPC calls refused with UNAVAILABLE because too many were in flight.",
        )
        .expect("valid metric");
        let connections_open = IntGauge::new(
            "doewood_grpc_connections_open",
            "Connections to the API port accepted and not yet closed.",
        )
        .expect("valid metric");
        let connections = IntCounter::new(
            "doewood_grpc_connections_total",
            "Connections accepted on the API port.",
        )
        .expect("valid metric");
        let icons_vec = IntCounterVec::new(
            Opts::new(
                "doewood_icons_requests_total",
                "Requests to GET /icons/{domain}, by outcome.",
            ),
            &["outcome"],
        )
        .expect("valid metric");
        let icons = ICON_OUTCOMES.map(|o| icons_vec.with_label_values(&[o]));
        let methods = METHODS
            .iter()
            .map(|m| m.strip_prefix('/').unwrap_or(m))
            .chain([OTHER])
            .map(|label| Method {
                label,
                handles: OnceLock::new(),
            })
            .collect();
        Self {
            calls,
            duration,
            in_flight,
            shed,
            connections_open,
            connections,
            icons,
            icons_vec,
            methods,
        }
    }

    /// Registers every metric on `registry`.
    pub(crate) fn register(&self, registry: &Registry) -> prometheus::Result<()> {
        registry.register(Box::new(self.calls.clone()))?;
        registry.register(Box::new(self.duration.clone()))?;
        registry.register(Box::new(self.in_flight.clone()))?;
        registry.register(Box::new(self.shed.clone()))?;
        registry.register(Box::new(self.connections_open.clone()))?;
        registry.register(Box::new(self.connections.clone()))?;
        registry.register(Box::new(self.icons_vec.clone()))
    }

    /// The index of a path's method in `methods` (`other` when it is not served).
    fn method_index(path: &str) -> usize {
        METHODS.binary_search(&path).unwrap_or(METHODS.len())
    }

    fn handles(&self, index: usize) -> &Handles {
        let m = &self.methods[index];
        m.handles.get_or_init(|| Handles {
            in_flight: self.in_flight.with_label_values(&[m.label]),
            duration: self.duration.with_label_values(&[m.label]),
            codes: Default::default(),
        })
    }

    fn count(&self, index: usize, code: u8) {
        let code = usize::from(code).min(CODES.len() - 1);
        let m = &self.methods[index];
        self.handles(index).codes[code]
            .get_or_init(|| self.calls.with_label_values(&[m.label, CODES[code]]))
            .inc();
    }

    #[cfg(test)]
    pub(crate) fn shed_count(&self) -> u64 {
        self.shed.get()
    }

    #[cfg(test)]
    pub(crate) fn connections_open(&self) -> i64 {
        self.connections_open.get()
    }

    #[cfg(test)]
    pub(crate) fn connections_total(&self) -> u64 {
        self.connections.get()
    }

    /// One call refused by the in-flight cap.
    pub(crate) fn shed(&self) {
        self.shed.inc();
    }

    /// One answer of the icon route.
    pub(crate) fn icon(&self, outcome: IconOutcome) {
        self.icons[outcome as usize].inc();
    }

    /// A connection was accepted; it counts as open until the guard is dropped.
    pub(crate) fn connection(self: &Arc<Self>) -> OpenConnection {
        self.connections.inc();
        self.connections_open.inc();
        OpenConnection(Arc::clone(self))
    }
}

/// An open connection, for `doewood_grpc_connections_open`.
pub(crate) struct OpenConnection(Arc<ApiMetrics>);

impl Drop for OpenConnection {
    fn drop(&mut self) {
        self.0.connections_open.dec();
    }
}

/// One call in flight; recorded when it is dropped.
struct Call {
    metrics: Arc<ApiMetrics>,
    method: usize,
    start: Instant,
    /// The status code, once the response or its trailers carried it.
    code: Option<u8>,
    /// Whether the body ended (without a status, that is UNKNOWN, not CANCELLED).
    ended: bool,
}

impl Call {
    fn start(metrics: &Arc<ApiMetrics>, path: &str) -> Self {
        let method = ApiMetrics::method_index(path);
        metrics.handles(method).in_flight.inc();
        Self {
            metrics: Arc::clone(metrics),
            method,
            start: Instant::now(),
            code: None,
            ended: false,
        }
    }

    fn status_from(&mut self, headers: &HeaderMap) {
        if let Some(v) = headers.get("grpc-status") {
            self.code = Some(parse_code(v.as_bytes()));
        }
    }
}

impl Drop for Call {
    fn drop(&mut self) {
        let handles = self.metrics.handles(self.method);
        handles.in_flight.dec();
        handles.duration.observe(self.start.elapsed().as_secs_f64());
        let code = self
            .code
            .unwrap_or(if self.ended { UNKNOWN } else { CANCELLED });
        self.metrics.count(self.method, code);
    }
}

/// A `grpc-status` value as a code number (UNKNOWN when it is not one).
fn parse_code(v: &[u8]) -> u8 {
    std::str::from_utf8(v)
        .ok()
        .and_then(|s| s.trim().parse::<u8>().ok())
        .filter(|c| usize::from(*c) < CODES.len())
        .unwrap_or(UNKNOWN)
}

/// The gRPC services, measured.
#[derive(Clone)]
pub(crate) struct Metered<S> {
    inner: S,
    metrics: Arc<ApiMetrics>,
}

impl<S> Metered<S> {
    pub(crate) fn new(inner: S, metrics: Arc<ApiMetrics>) -> Self {
        Self { inner, metrics }
    }
}

impl<S, B> Service<Request<B>> for Metered<S>
where
    S: Service<Request<B>, Response = Response<Body>, Error = Infallible>,
    S::Future: Unpin,
{
    type Response = Response<Body>;
    type Error = Infallible;
    type Future = MeteredFuture<S::Future>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Infallible>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: Request<B>) -> Self::Future {
        let call = Call::start(&self.metrics, req.uri().path());
        MeteredFuture {
            inner: self.inner.call(req),
            call: Some(call),
        }
    }
}

/// The response future of [`Metered`].
pub(crate) struct MeteredFuture<F> {
    inner: F,
    call: Option<Call>,
}

impl<F> Future for MeteredFuture<F>
where
    F: Future<Output = Result<Response<Body>, Infallible>> + Unpin,
{
    type Output = Result<Response<Body>, Infallible>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let Ok(resp) = ready!(Pin::new(&mut self.inner).poll(cx));
        let mut call = self.call.take().expect("polled after completion");
        call.status_from(resp.headers());
        // An empty body (an error without one) is dropped at once, and the call recorded.
        Poll::Ready(Ok(resp.map(|body| {
            Body::new(MeteredBody {
                inner: body,
                call: Some(call),
            })
        })))
    }
}

/// A response body that records its call when it ends or is dropped.
struct MeteredBody {
    inner: Body,
    call: Option<Call>,
}

impl http_body::Body for MeteredBody {
    type Data = Bytes;
    type Error = Status;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Status>>> {
        let polled = ready!(Pin::new(&mut self.inner).poll_frame(cx));
        match &polled {
            Some(Ok(frame)) => {
                if let (Some(trailers), Some(call)) = (frame.trailers_ref(), self.call.as_mut()) {
                    call.status_from(trailers);
                }
            }
            Some(Err(status)) => {
                if let Some(call) = self.call.as_mut() {
                    call.code = u8::try_from(i32::from(status.code())).ok();
                }
                self.call = None;
            }
            None => {
                if let Some(mut call) = self.call.take() {
                    call.ended = true;
                }
            }
        }
        Poll::Ready(polled)
    }

    fn is_end_stream(&self) -> bool {
        self.inner.is_end_stream()
    }

    fn size_hint(&self) -> SizeHint {
        self.inner.size_hint()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn methods_are_sorted_and_complete() {
        assert!(METHODS.windows(2).all(|w| w[0] < w[1]));
        for m in [
            "/doewood.v1.ConfigService/ListSources",
            "/doewood.v1.JobsService/WatchProgress",
            "/grpc.health.v1.Health/Check",
            "/grpc.reflection.v1.ServerReflection/ServerReflectionInfo",
        ] {
            assert!(METHODS.contains(&m), "{m}");
        }
        let m = ApiMetrics::new();
        let i = ApiMetrics::method_index("/doewood.v1.ConfigService/ListSources");
        assert_eq!(m.methods[i].label, "doewood.v1.ConfigService/ListSources");
        for path in ["/nope/Nope", "", "/doewood.v1.ConfigService/ListSourcesX"] {
            assert_eq!(m.methods[ApiMetrics::method_index(path)].label, OTHER);
        }
    }

    #[test]
    fn status_codes_parse() {
        assert_eq!(parse_code(b"0"), 0);
        assert_eq!(parse_code(b"14"), 14);
        assert_eq!(parse_code(b"16"), 16);
        assert_eq!(parse_code(b"17"), UNKNOWN);
        assert_eq!(parse_code(b"x"), UNKNOWN);
        assert_eq!(CODES[usize::from(CANCELLED)], "CANCELLED");
        assert_eq!(CODES[14], "UNAVAILABLE");
    }
}
