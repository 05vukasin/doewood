//! The request loop every registry client shares: cache, rate limiter, capped `GET` or JSON
//! `POST`, retries with backoff, `Retry-After`, and a client's own reading of error answers.

use std::sync::Arc;
use std::time::Duration;

use url::Url;

use crate::cache::{Cache, CacheEntry, Cached};
use crate::clock::Clock;
use crate::http::{Request, Response, Transport, TransportError};
use crate::limit::TokenBucket;
use crate::warning::{Registry, Warning, WarningKind};

/// The limits of one client's requests.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FetchPolicy {
    /// The largest body read; a larger one is a [`WarningKind::TooLarge`] and is not parsed.
    pub max_body: usize,
    /// Retries after a timeout, a transport error, a 5xx or a 429 (0: no retry).
    pub max_retries: u32,
    /// The first backoff; it doubles on each retry, up to 60 s.
    pub backoff: Duration,
    /// The longest `Retry-After` honoured. A longer one gives up at once with
    /// [`WarningKind::RateLimited`], so a lookup never hangs for an hour.
    pub max_retry_after: Duration,
    /// How long a found record is fresh in the cache.
    pub ttl_found: Duration,
    /// How long a "no such record" is fresh in the cache.
    pub ttl_missing: Duration,
}

/// The answer of [`Fetcher::get`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Body {
    /// A successful answer (2xx): the body, within the cap.
    Found(Vec<u8>),
    /// 404: no such record.
    NotFound,
}

/// An answer, with its fetch date and whether it came from the cache.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Fetch {
    /// The answer.
    pub body: Body,
    /// When the registry gave it.
    pub fetched_at: time::OffsetDateTime,
    /// Whether it came from the cache.
    pub cached: bool,
}

/// A client's answer with its fetch date (from the cache, or now).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Fetched<T> {
    /// The answer.
    pub value: T,
    /// When the registry gave it.
    pub fetched_at: time::OffsetDateTime,
}

impl<T> Fetched<T> {
    /// `value`, dated by `fetch`.
    pub fn new(value: T, fetch: &Fetch) -> Self {
        Self {
            value,
            fetched_at: fetch.fetched_at,
        }
    }
}

/// How a client reads an HTTP answer before the default status handling (see
/// [`Fetcher::send`]). Registries such as VIES report their own errors in the body, sometimes
/// with a 200.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Inspect {
    /// The default: 2xx is the answer, 404 is "no such record", 429 and 5xx are retried, any
    /// other status is a [`WarningKind::Status`].
    Status,
    /// A transient failure: retried with the backoff, then this warning.
    Retry(WarningKind),
    /// The registry is overloaded: the bucket is emptied, then as [`Inspect::Retry`].
    Busy(WarningKind),
    /// A failure that a retry does not fix: this warning at once.
    Fail(WarningKind),
}

impl Fetch {
    fn new(body: Body, fetched_at: time::OffsetDateTime) -> Self {
        Self {
            body,
            fetched_at,
            cached: false,
        }
    }
}

/// The cache, the limiter and the transport of one registry client.
#[derive(Debug)]
pub struct Fetcher {
    registry: Registry,
    transport: Arc<dyn Transport>,
    cache: Arc<dyn Cache>,
    clock: Arc<dyn Clock>,
    bucket: Arc<TokenBucket>,
    policy: FetchPolicy,
}

const MAX_BACKOFF: Duration = Duration::from_mins(1);

impl Fetcher {
    /// A fetcher with its own token bucket.
    pub fn new(
        registry: Registry,
        transport: Arc<dyn Transport>,
        cache: Arc<dyn Cache>,
        clock: Arc<dyn Clock>,
        bucket: TokenBucket,
        policy: FetchPolicy,
    ) -> Self {
        Self::sharing(registry, transport, cache, clock, Arc::new(bucket), policy)
    }

    /// A fetcher whose token bucket is shared with other fetchers: one limit over several
    /// endpoints or document types of a registry, each with its own policy (EDGAR).
    pub fn sharing(
        registry: Registry,
        transport: Arc<dyn Transport>,
        cache: Arc<dyn Cache>,
        clock: Arc<dyn Clock>,
        bucket: Arc<TokenBucket>,
        policy: FetchPolicy,
    ) -> Self {
        Self {
            registry,
            transport,
            cache,
            clock,
            bucket,
            policy,
        }
    }

    /// The cached answer under `key` while it is fresh, without sending anything.
    pub async fn cached(&self, key: &str) -> Option<Fetch> {
        self.fresh(&self.key(key)).await
    }

    /// The cached answer under `key` while fresh, else `GET url`. A fresh answer is not cached
    /// yet: the client calls [`Fetcher::remember`] once it has parsed it, so a malformed body
    /// is never cached.
    pub async fn get(&self, key: &str, url: &Url, accept: &str) -> Result<Fetch, Warning> {
        self.send(key, &Request::get(url, accept), &|_| Inspect::Status)
            .await
    }

    /// As [`Fetcher::get`], for any request, with `inspect` reading each HTTP answer first.
    pub async fn send(
        &self,
        key: &str,
        request: &Request<'_>,
        inspect: &(dyn Fn(&Response) -> Inspect + Sync),
    ) -> Result<Fetch, Warning> {
        let key = self.key(key);
        if let Some(hit) = self.fresh(&key).await {
            return Ok(hit);
        }
        let result = self.fetch(request, inspect).await;
        if let Err(w) = &result {
            tracing::warn!(registry = %self.registry, key, warning = %w.kind,
                "registry request failed");
        }
        result
    }

    /// Caches a fetched answer under `key` (an answer from the cache is left alone).
    pub async fn remember(&self, key: &str, fetch: &Fetch) {
        if fetch.cached {
            return;
        }
        let value = match &fetch.body {
            Body::Found(b) => Cached::Found(b.clone()),
            Body::NotFound => Cached::NotFound,
        };
        let entry = CacheEntry {
            value,
            fetched_at: fetch.fetched_at,
        };
        self.cache.put(&self.key(key), entry).await;
    }

    fn key(&self, key: &str) -> String {
        format!("{}:{key}", self.registry)
    }

    /// A warning of this registry.
    pub fn warning(&self, kind: WarningKind) -> Warning {
        Warning::new(self.registry, kind)
    }

    async fn fresh(&self, key: &str) -> Option<Fetch> {
        let entry = self.cache.get(key).await?;
        let ttl = match entry.value {
            Cached::Found(_) => self.policy.ttl_found,
            Cached::NotFound => self.policy.ttl_missing,
        };
        let age = self.clock.now_utc() - entry.fetched_at;
        // A fetch date in the future (clock skew between instances) counts as fresh.
        if age > ttl {
            return None;
        }
        let body = match entry.value {
            Cached::Found(b) => Body::Found(b),
            Cached::NotFound => Body::NotFound,
        };
        Some(Fetch {
            body,
            fetched_at: entry.fetched_at,
            cached: true,
        })
    }

    async fn fetch(
        &self,
        request: &Request<'_>,
        inspect: &(dyn Fn(&Response) -> Inspect + Sync),
    ) -> Result<Fetch, Warning> {
        let mut attempt = 0u32;
        loop {
            self.bucket.acquire(self.clock.as_ref()).await;
            let answer = self.transport.send(request, self.policy.max_body).await;
            let fetched_at = self.clock.now_utc();
            let backoff = self.backoff(attempt);
            let verdict = match &answer {
                Ok(r) => inspect(r),
                Err(_) => Inspect::Status,
            };
            let (kind, wait) = match (verdict, answer) {
                (Inspect::Retry(kind), _) => (kind, backoff),
                (Inspect::Busy(kind), _) => {
                    self.bucket.drain(self.clock.as_ref()).await;
                    (kind, backoff)
                }
                (Inspect::Fail(kind), _) => return Err(self.warning(kind)),
                (Inspect::Status, answer) => match answer {
                    Ok(r) if (200..300).contains(&r.status) => {
                        return Ok(Fetch::new(Body::Found(r.body), fetched_at));
                    }
                    Ok(r) if r.status == 404 => return Ok(Fetch::new(Body::NotFound, fetched_at)),
                    Ok(r) if r.status == 429 => {
                        let after = r.retry_after.as_deref().and_then(retry_after);
                        let kind = WarningKind::RateLimited { retry_after: after };
                        if after.is_some_and(|a| a > self.policy.max_retry_after) {
                            return Err(self.warning(kind));
                        }
                        self.bucket.drain(self.clock.as_ref()).await;
                        (kind, after.unwrap_or(backoff))
                    }
                    Ok(r) if r.status >= 500 => (WarningKind::Status(r.status), backoff),
                    Ok(r) => return Err(self.warning(WarningKind::Status(r.status))),
                    Err(TransportError::TooLarge) => {
                        return Err(self.warning(WarningKind::TooLarge {
                            limit: self.policy.max_body,
                        }));
                    }
                    Err(TransportError::Timeout) => (WarningKind::Timeout, backoff),
                    Err(TransportError::Other(e)) => (WarningKind::Transport(e), backoff),
                },
            };
            if attempt >= self.policy.max_retries {
                return Err(self.warning(kind));
            }
            tracing::debug!(registry = %self.registry, attempt, ?wait, reason = %kind,
                "registry request retried");
            attempt += 1;
            self.clock.sleep(wait).await;
        }
    }

    fn backoff(&self, attempt: u32) -> Duration {
        self.policy
            .backoff
            .saturating_mul(1 << attempt.min(16))
            .min(MAX_BACKOFF)
    }
}

/// `Retry-After` in seconds. The HTTP-date form is not parsed; the backoff applies instead.
fn retry_after(v: &str) -> Option<Duration> {
    v.trim().parse::<u64>().ok().map(Duration::from_secs)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retry_after_seconds_only() {
        assert_eq!(retry_after("3"), Some(Duration::from_secs(3)));
        assert_eq!(retry_after(" 120 "), Some(Duration::from_mins(2)));
        assert_eq!(retry_after("Wed, 21 Oct 2026 07:28:00 GMT"), None);
        assert_eq!(retry_after("-1"), None);
        assert_eq!(retry_after(""), None);
    }
}
