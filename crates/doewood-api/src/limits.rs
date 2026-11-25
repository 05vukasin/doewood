//! The server's limits (issue #82, docs/api.md "Limits").
//!
//! - **Per connection** (constants: hyper and the listener take them when the server starts):
//!   [`MAX_CONCURRENT_STREAMS`], HTTP/2 PING keepalive ([`HTTP2_KEEPALIVE_INTERVAL`],
//!   [`HTTP2_KEEPALIVE_TIMEOUT`]), TCP keepalive ([`TCP_KEEPALIVE`]) and the connection age:
//!   GOAWAY after [`MAX_CONNECTION_AGE`] give or take 10% ([`Jitter`], drawn per connection,
//!   so clients that connected together do not all reconnect together), then closed after
//!   [`MAX_CONNECTION_AGE_GRACE`] more ([`Aged`]), so long-lived clients reconnect and spread
//!   over the replicas.
//! - **Per call** (runtime settings, `ConfigService.SetApiSettings`, read live by [`Limited`]):
//!   a cap on calls in flight, beyond which a call is refused at once with `UNAVAILABLE`, and a
//!   server deadline (`DEADLINE_EXCEEDED`). Both bound the response future: a unary call whole,
//!   a stream only while it is set up, so an open `WatchProgress` is neither counted nor cut.
//!   Health checks are exempt from both; `Summarize` is counted but has no server deadline
//!   (its analysis and provider calls are bounded on their own and can take minutes).

use std::convert::Infallible;
use std::future::Future as _;
use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::task::{Context, Poll};
use std::time::Duration;

use doewood_store::ApiSettings;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::sync::oneshot;
use tokio::time::Sleep;
use tokio_stream::Stream;
use tonic::Status;
use tonic::body::Body;
use tonic::codegen::http::{Request, Response};
use tonic::codegen::{BoxFuture, Service};
use tonic::transport::server::Connected;

use crate::metrics::{ApiMetrics, OpenConnection};

/// Concurrent HTTP/2 streams (calls) one connection may open; more wait in the client. hyper's
/// own default is 200, but tonic replaces it with "unlimited" unless it is set.
pub const MAX_CONCURRENT_STREAMS: u32 = 256;
/// How long an HTTP/2 connection may be quiet before the server PINGs it.
pub const HTTP2_KEEPALIVE_INTERVAL: Duration = Duration::from_secs(30);
/// How long the server waits for the PING's answer before it closes the connection.
pub const HTTP2_KEEPALIVE_TIMEOUT: Duration = Duration::from_secs(20);
/// Idle time before the kernel sends TCP keepalive probes (half-open connections behind a
/// NAT or a load balancer that forgot them).
pub const TCP_KEEPALIVE: Duration = Duration::from_mins(1);
/// Age at which a connection gets GOAWAY: its client opens a new one (which a load
/// balancer may send to another replica) for new calls. Each connection gets its own age,
/// uniformly within ±[`MAX_CONNECTION_AGE_JITTER_PERCENT`] of this.
pub const MAX_CONNECTION_AGE: Duration = Duration::from_mins(30);
/// How far a connection's age may be from [`MAX_CONNECTION_AGE`], in percent.
pub const MAX_CONNECTION_AGE_JITTER_PERCENT: u32 = 10;
/// How long calls already on an aged connection may still run (streams included) before
/// the connection is closed.
pub const MAX_CONNECTION_AGE_GRACE: Duration = Duration::from_mins(5);

/// Paths exempt from the call limits: health checks must answer under load.
const HEALTH_PREFIX: &str = "/grpc.health.v1.Health/";
/// Paths counted but without a server deadline.
const SUMMARIZE_PREFIX: &str = "/doewood.v1.SummarizeService/";

/// The live per-call limits, shared by every connection and changed by `SetApiSettings`.
#[derive(Debug)]
pub(crate) struct CallLimits {
    max_in_flight: AtomicU32,
    deadline_seconds: AtomicU32,
    in_flight: AtomicU32,
}

impl CallLimits {
    pub(crate) fn new(settings: ApiSettings) -> Self {
        let limits = Self {
            max_in_flight: AtomicU32::new(0),
            deadline_seconds: AtomicU32::new(0),
            in_flight: AtomicU32::new(0),
        };
        limits.set(settings);
        limits
    }

    /// Applies new settings to the next call.
    pub(crate) fn set(&self, settings: ApiSettings) {
        self.max_in_flight
            .store(settings.max_calls_in_flight, Ordering::Relaxed);
        self.deadline_seconds
            .store(settings.deadline_seconds, Ordering::Relaxed);
    }

    fn deadline(&self) -> Duration {
        Duration::from_secs(u64::from(self.deadline_seconds.load(Ordering::Relaxed)))
    }

    /// A slot for one call, or `None` when the cap is reached.
    fn enter(self: &Arc<Self>) -> Option<InFlight> {
        let max = self.max_in_flight.load(Ordering::Relaxed);
        self.in_flight
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| {
                (n < max).then_some(n + 1)
            })
            .ok()
            .map(|_| InFlight(Arc::clone(self)))
    }

    /// Calls in flight now.
    #[cfg(test)]
    fn in_flight(&self) -> u32 {
        self.in_flight.load(Ordering::Acquire)
    }
}

/// One counted call; the slot is freed when it is dropped.
struct InFlight(Arc<CallLimits>);

impl Drop for InFlight {
    fn drop(&mut self) {
        self.0.in_flight.fetch_sub(1, Ordering::AcqRel);
    }
}

/// The gRPC services behind the per-call limits (gRPC-Web is translated outside, so it gets
/// the same limits and errors).
#[derive(Clone)]
pub(crate) struct Limited<S> {
    inner: S,
    limits: Arc<CallLimits>,
    metrics: Arc<ApiMetrics>,
}

impl<S> Limited<S> {
    pub(crate) fn new(inner: S, limits: Arc<CallLimits>, metrics: Arc<ApiMetrics>) -> Self {
        Self {
            inner,
            limits,
            metrics,
        }
    }
}

/// The status as a complete gRPC response (trailers-only).
fn reply(status: Status) -> Response<Body> {
    status.into_http()
}

impl<S, B> Service<Request<B>> for Limited<S>
where
    S: Service<Request<B>, Response = Response<Body>, Error = Infallible> + Clone + Send + 'static,
    S::Future: Send + 'static,
{
    type Response = Response<Body>;
    type Error = Infallible;
    type Future = BoxFuture<Response<Body>, Infallible>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Infallible>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: Request<B>) -> Self::Future {
        let path = req.uri().path();
        if path.starts_with(HEALTH_PREFIX) {
            return Box::pin(self.inner.call(req));
        }
        let deadline = (!path.starts_with(SUMMARIZE_PREFIX)).then(|| self.limits.deadline());
        let Some(slot) = self.limits.enter() else {
            tracing::debug!(path, "call shed: too many calls in flight");
            self.metrics.shed();
            let max = self.limits.max_in_flight.load(Ordering::Relaxed);
            return Box::pin(std::future::ready(Ok(reply(Status::unavailable(format!(
                "the server is overloaded ({max} calls in flight); retry with backoff, \
                 another replica may answer"
            ))))));
        };
        // The service that was made ready answers this call; a fresh clone waits for the next.
        let clone = self.inner.clone();
        let mut inner = std::mem::replace(&mut self.inner, clone);
        let call = inner.call(req);
        Box::pin(async move {
            let _slot = slot;
            match deadline {
                None => call.await,
                Some(d) => tokio::time::timeout(d, call).await.unwrap_or_else(|_| {
                    Ok(reply(Status::deadline_exceeded(format!(
                        "the call ran longer than the server's deadline of {}s",
                        d.as_secs()
                    ))))
                }),
            }
        })
    }
}

/// The source of each connection's age: a small seeded generator (splitmix64), random in
/// the server and fixed in tests.
#[derive(Clone, Debug)]
pub(crate) struct Jitter(u64);

impl Jitter {
    /// A generator with a fixed seed (tests).
    #[cfg(test)]
    pub(crate) fn seeded(seed: u64) -> Self {
        Self(seed)
    }

    /// A generator seeded from the process's random hash keys.
    pub(crate) fn random() -> Self {
        use std::hash::BuildHasher as _;
        Self(std::hash::RandomState::new().hash_one(0u64))
    }

    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }

    /// `age` times a factor drawn uniformly from [1 − p, 1 + p], p =
    /// [`MAX_CONNECTION_AGE_JITTER_PERCENT`], in steps of a millionth.
    pub(crate) fn age(&mut self, age: Duration) -> Duration {
        const MILLION: u64 = 1_000_000;
        let spread = u64::from(MAX_CONNECTION_AGE_JITTER_PERCENT) * MILLION / 100;
        let factor = MILLION - spread + self.next() % (2 * spread + 1);
        let nanos = age.as_nanos() * u128::from(factor) / u128::from(MILLION);
        Duration::from_nanos(u64::try_from(nanos).unwrap_or(u64::MAX))
    }
}

/// An accepted connection that is closed its own age + [`MAX_CONNECTION_AGE_GRACE`] after it
/// was opened: tonic sends GOAWAY at the age but would then wait for every open stream, so a
/// `WatchProgress` would pin it forever. Reads and writes fail after the deadline, and hyper
/// drops the connection. Dropping it ends the connection's [`OneConnection`] stream and
/// its count in `doewood_grpc_connections_open`.
pub(crate) struct Aged<IO> {
    io: IO,
    close: Pin<Box<Sleep>>,
    _open: OpenConnection,
    _closed: oneshot::Sender<()>,
}

impl<IO> Aged<IO> {
    /// The connection and the stream that yields it once to a tonic server, then ends when
    /// the connection is closed (so that server stops too).
    pub(crate) fn new(
        io: IO,
        lifetime: Duration,
        metrics: &Arc<ApiMetrics>,
    ) -> OneConnection<Self> {
        let (closed, rx) = oneshot::channel();
        let aged = Self {
            io,
            close: Box::pin(tokio::time::sleep(lifetime)),
            _open: metrics.connection(),
            _closed: closed,
        };
        OneConnection {
            io: Some(aged),
            closed: rx,
        }
    }

    /// An error once the lifetime is over (and registers the wake-up until then).
    fn expired(&mut self, cx: &mut Context<'_>) -> Option<io::Error> {
        self.close.as_mut().poll(cx).is_ready().then(|| {
            io::Error::new(
                io::ErrorKind::TimedOut,
                "connection closed: maximum connection age and its grace period are over",
            )
        })
    }
}

/// The incoming stream of a tonic server that serves one connection: the connection, then
/// the end once it is closed.
pub(crate) struct OneConnection<IO> {
    io: Option<IO>,
    closed: oneshot::Receiver<()>,
}

impl<IO: Unpin> Stream for OneConnection<IO> {
    type Item = io::Result<IO>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if let Some(io) = self.io.take() {
            return Poll::Ready(Some(Ok(io)));
        }
        // The sender is never used: it is dropped with the connection.
        Pin::new(&mut self.closed).poll(cx).map(|_| None)
    }
}

impl<IO: Connected> Connected for Aged<IO> {
    type ConnectInfo = IO::ConnectInfo;

    fn connect_info(&self) -> Self::ConnectInfo {
        self.io.connect_info()
    }
}

impl<IO: AsyncRead + Unpin> AsyncRead for Aged<IO> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if let Some(e) = self.expired(cx) {
            return Poll::Ready(Err(e));
        }
        Pin::new(&mut self.io).poll_read(cx, buf)
    }
}

impl<IO: AsyncWrite + Unpin> AsyncWrite for Aged<IO> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        if let Some(e) = self.expired(cx) {
            return Poll::Ready(Err(e));
        }
        Pin::new(&mut self.io).poll_write(cx, buf)
    }

    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        if let Some(e) = self.expired(cx) {
            return Poll::Ready(Err(e));
        }
        Pin::new(&mut self.io).poll_write_vectored(cx, bufs)
    }

    fn is_write_vectored(&self) -> bool {
        self.io.is_write_vectored()
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.io).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.io).poll_shutdown(cx)
    }
}

#[cfg(test)]
mod tests {
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
    use tokio::sync::Notify;
    use tokio_stream::StreamExt as _;

    use super::*;

    fn limits(max: u32, deadline: u32) -> Arc<CallLimits> {
        Arc::new(CallLimits::new(ApiSettings {
            max_calls_in_flight: max,
            deadline_seconds: deadline,
        }))
    }

    /// A handler that answers OK once `release` is notified (or after `delay`).
    #[derive(Clone)]
    struct Held {
        release: Arc<Notify>,
        delay: Duration,
    }

    impl Service<Request<()>> for Held {
        type Response = Response<Body>;
        type Error = Infallible;
        type Future = BoxFuture<Response<Body>, Infallible>;

        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Infallible>> {
            Poll::Ready(Ok(()))
        }

        fn call(&mut self, _req: Request<()>) -> Self::Future {
            let release = Arc::clone(&self.release);
            let delay = self.delay;
            Box::pin(async move {
                tokio::select! {
                    () = release.notified() => {}
                    () = tokio::time::sleep(delay) => {}
                }
                Ok(reply(Status::ok("")))
            })
        }
    }

    fn request(path: &str) -> Request<()> {
        Request::builder().uri(path).body(()).unwrap()
    }

    fn code(r: &Response<Body>) -> tonic::Code {
        Status::from_header_map(r.headers()).map_or(tonic::Code::Ok, |s| s.code())
    }

    #[tokio::test(start_paused = true)]
    async fn sheds_beyond_the_cap_and_frees_slots() {
        let release = Arc::new(Notify::new());
        let l = limits(2, 60);
        let metrics = Arc::new(ApiMetrics::new());
        let mut svc = Limited::new(
            Held {
                release: Arc::clone(&release),
                delay: Duration::from_hours(1),
            },
            Arc::clone(&l),
            Arc::clone(&metrics),
        );
        let a = tokio::spawn(svc.call(request("/doewood.v1.RecordsService/GetRecord")));
        let b = tokio::spawn(svc.call(request("/doewood.v1.SummarizeService/Summarize")));
        assert_eq!(l.in_flight(), 2);
        // The third is refused at once, without waiting.
        let shed = svc
            .call(request("/doewood.v1.ConfigService/ListSources"))
            .await
            .unwrap();
        assert_eq!(code(&shed), tonic::Code::Unavailable);
        let status = Status::from_header_map(shed.headers()).unwrap();
        assert!(status.message().contains("overloaded"), "{status:?}");
        // Health checks are never shed.
        let health = tokio::spawn(svc.call(request("/grpc.health.v1.Health/Check")));
        tokio::task::yield_now().await;
        assert_eq!(l.in_flight(), 2);
        release.notify_waiters();
        assert_eq!(code(&a.await.unwrap().unwrap()), tonic::Code::Ok);
        assert_eq!(code(&b.await.unwrap().unwrap()), tonic::Code::Ok);
        assert_eq!(code(&health.await.unwrap().unwrap()), tonic::Code::Ok);
        assert_eq!(l.in_flight(), 0);
        // The cap changes live.
        l.set(ApiSettings {
            max_calls_in_flight: 1,
            deadline_seconds: 60,
        });
        let c = tokio::spawn(svc.call(request("/doewood.v1.RecordsService/GetRecord")));
        tokio::task::yield_now().await;
        let shed = svc
            .call(request("/doewood.v1.RecordsService/GetRecord"))
            .await
            .unwrap();
        assert_eq!(code(&shed), tonic::Code::Unavailable);
        release.notify_waiters();
        assert_eq!(code(&c.await.unwrap().unwrap()), tonic::Code::Ok);
        assert_eq!(metrics.shed_count(), 2);
    }

    #[tokio::test(start_paused = true)]
    async fn the_deadline_ends_slow_calls_but_not_summarize() {
        let l = limits(16, 60);
        let mut svc = Limited::new(
            Held {
                release: Arc::new(Notify::new()),
                delay: Duration::from_secs(61),
            },
            Arc::clone(&l),
            Arc::new(ApiMetrics::new()),
        );
        let slow = svc
            .call(request("/doewood.v1.GraphService/Neighbors"))
            .await
            .unwrap();
        assert_eq!(code(&slow), tonic::Code::DeadlineExceeded);
        let status = Status::from_header_map(slow.headers()).unwrap();
        assert!(status.message().contains("60s"), "{status:?}");
        assert_eq!(l.in_flight(), 0, "a timed-out call frees its slot");
        // Summarize has no server deadline.
        let summary = svc
            .call(request("/doewood.v1.SummarizeService/Summarize"))
            .await
            .unwrap();
        assert_eq!(code(&summary), tonic::Code::Ok);
        // A longer deadline, set live, lets the same call finish.
        l.set(ApiSettings {
            max_calls_in_flight: 16,
            deadline_seconds: 120,
        });
        let ok = svc
            .call(request("/doewood.v1.GraphService/Neighbors"))
            .await
            .unwrap();
        assert_eq!(code(&ok), tonic::Code::Ok);
    }

    #[tokio::test(start_paused = true)]
    async fn an_aged_connection_fails_after_its_lifetime() {
        let (a, mut b) = tokio::io::duplex(64);
        let metrics = Arc::new(ApiMetrics::new());
        let mut conn = Aged::new(a, Duration::from_secs(10), &metrics);
        let mut aged = conn.next().await.unwrap().unwrap();
        assert_eq!(metrics.connections_open(), 1);
        aged.write_all(b"ping").await.unwrap();
        let mut buf = [0u8; 4];
        b.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"ping");
        // A read waiting for data is woken by the deadline and fails.
        let read = tokio::spawn(async move {
            let mut buf = [0u8; 4];
            (aged.read(&mut buf).await, aged)
        });
        tokio::time::sleep(Duration::from_secs(11)).await;
        let (res, mut aged) = read.await.unwrap();
        assert_eq!(res.unwrap_err().kind(), io::ErrorKind::TimedOut);
        assert_eq!(
            aged.write(b"x").await.unwrap_err().kind(),
            io::ErrorKind::TimedOut
        );
        // The stream that handed it out ends once it is dropped, and it no longer counts.
        let end = tokio::spawn(async move { conn.next().await.is_none() });
        tokio::task::yield_now().await;
        assert!(!end.is_finished());
        drop(aged);
        assert!(end.await.unwrap());
        assert_eq!(metrics.connections_open(), 0);
    }

    #[test]
    fn jitter_spreads_connection_ages_within_ten_percent() {
        let age = MAX_CONNECTION_AGE;
        let (low, high) = (age * 9 / 10, age * 11 / 10);
        let mut jitter = Jitter::seeded(7);
        let ages: Vec<Duration> = (0..1000).map(|_| jitter.age(age)).collect();
        assert!(
            ages.iter().all(|a| (low..=high).contains(a)),
            "out of bounds"
        );
        // Spread over the whole range: each tenth of it gets a share.
        let step = age / 50;
        for i in 0..10 {
            let (from, to) = (low + step * i, low + step * (i + 1));
            let n = ages.iter().filter(|a| (from..to).contains(*a)).count();
            assert!((50..=150).contains(&n), "tenth {i}: {n} of 1000");
        }
        let mut distinct = ages.clone();
        distinct.sort();
        distinct.dedup();
        assert!(distinct.len() > 990, "{} distinct ages", distinct.len());
        // The same seed gives the same ages; another seed others.
        let mut again = Jitter::seeded(7);
        assert!(ages.iter().all(|a| *a == again.age(age)));
        let mut other = Jitter::seeded(8);
        assert_ne!(
            ages[..10],
            (0..10).map(|_| other.age(age)).collect::<Vec<_>>()[..]
        );
        // The random seed differs between generators.
        let draws = |mut j: Jitter| (0..4).map(|_| j.age(age)).collect::<Vec<_>>();
        assert_ne!(draws(Jitter::random()), draws(Jitter::random()));
    }

    #[test]
    fn connection_limits_fit_together() {
        // A dead peer is noticed within a minute of quiet.
        assert!(HTTP2_KEEPALIVE_INTERVAL + HTTP2_KEEPALIVE_TIMEOUT <= Duration::from_mins(1));
        assert!(HTTP2_KEEPALIVE_TIMEOUT < HTTP2_KEEPALIVE_INTERVAL);
        // A unary call that starts just before the GOAWAY finishes within the grace period.
        assert!(
            Duration::from_secs(u64::from(doewood_store::DEFAULT_DEADLINE_SECONDS))
                < MAX_CONNECTION_AGE_GRACE
        );
    }
}
