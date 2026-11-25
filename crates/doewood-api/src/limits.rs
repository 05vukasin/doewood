//! The server's limits (issue #82, docs/api.md "Limits").
//!
//! - **Per connection** (constants: hyper and the listener take them when the server starts):
//!   [`MAX_CONCURRENT_STREAMS`], HTTP/2 PING keepalive ([`HTTP2_KEEPALIVE_INTERVAL`],
//!   [`HTTP2_KEEPALIVE_TIMEOUT`]), TCP keepalive ([`TCP_KEEPALIVE`]) and the connection age:
//!   GOAWAY after [`MAX_CONNECTION_AGE`] (tonic), then closed after
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
use tokio::time::Sleep;
use tonic::Status;
use tonic::body::Body;
use tonic::codegen::http::{Request, Response};
use tonic::codegen::{BoxFuture, Service};
use tonic::transport::server::Connected;

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
/// balancer may send to another replica) for new calls.
pub const MAX_CONNECTION_AGE: Duration = Duration::from_mins(30);
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
}

impl<S> Limited<S> {
    pub(crate) fn new(inner: S, limits: Arc<CallLimits>) -> Self {
        Self { inner, limits }
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

/// An accepted connection that is closed [`MAX_CONNECTION_AGE`] +
/// [`MAX_CONNECTION_AGE_GRACE`] after it was opened: tonic sends GOAWAY at the age but would
/// then wait for every open stream, so a `WatchProgress` would pin it forever. Reads and
/// writes fail after the deadline, and hyper drops the connection.
pub(crate) struct Aged<IO> {
    io: IO,
    close: Pin<Box<Sleep>>,
}

impl<IO> Aged<IO> {
    pub(crate) fn new(io: IO, lifetime: Duration) -> Self {
        Self {
            io,
            close: Box::pin(tokio::time::sleep(lifetime)),
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
        let mut svc = Limited::new(
            Held {
                release: Arc::clone(&release),
                delay: Duration::from_hours(1),
            },
            Arc::clone(&l),
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
        let mut aged = Aged::new(a, Duration::from_secs(10));
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
