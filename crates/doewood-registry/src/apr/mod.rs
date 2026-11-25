//! The Serbian APR client (ADR 0002 section 2): the Business Registers Agency's (Агенција за
//! привредне регистре) official open data, keyless.
//!
//! **Source.** The open-data set "АПИ за Регистар привредних друштава" on data.gov.rs,
//! served by APR at `https://openapi.apr.gov.rs/api/opendata/companies`: one JSON document of
//! every registered company that is active, in liquidation, in forced liquidation or in
//! bankruptcy, with the snapshot date, MB, business name, municipality of the seat, status,
//! founding date, legal form and activity code. APR updates it monthly. Its licence is the
//! Serbian open data licence (data.gov.rs, "Лиценца за коришћење података"): reuse, also
//! commercial, with attribution (the publishing body, the download date, the address, and any
//! change made). APR's search web pages forbid robots and are not used.
//!
//! - [`Apr::company`] (and [`Apr::lookup`]): a company by MB. The MB is validated with
//!   `doewood_enrich` (MOD 11) first; a malformed one is [`WarningKind::InvalidInput`] and
//!   nothing is sent. The set has **no PIB**: a valid PIB is [`WarningKind::Unsupported`].
//! - [`Apr::search`]: companies whose name has every word of a name (script-blind). The set is
//!   searched here: the name never leaves the instance.
//!
//! The set is downloaded whole (capped, [`AprConfig::max_body`]), read as a stream into a
//! compact index ([`AprConfig::max_companies`]), and kept for [`AprConfig::ttl`] (30 days, the
//! ADR's refresh). Each company looked up is also cached by MB with the download's date. A
//! failed download is never cached; the client then sends nothing for
//! [`AprConfig::failure_pause`] and answers with the same warning meanwhile.
//!
//! **Not in the set:** PIB, deleted companies (a deleted company is absent, so `None`), the
//! date of a status change, statutory changes, mergers and divisions. Those are in APR's
//! paid data service only.

mod model;
mod snapshot;

use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use doewood_enrich::{IdKind, Identifier, normalize};
use url::Url;

pub use model::{Company, LegalForm, Municipality, Script, Status};
use snapshot::Snapshot;

use crate::cache::Cache;
use crate::clock::{Clock, SystemClock};
pub use crate::fetch::Fetched;
use crate::fetch::{Body, Fetch, FetchPolicy, Fetcher};
use crate::http::{HttpTransport, Transport};
use crate::limit::TokenBucket;
pub use crate::trust::TrustTier;
use crate::warning::{Registry, Warning, WarningKind};

/// The companies set of APR's open-data API.
pub const DEFAULT_URL: &str = "https://openapi.apr.gov.rs/api/opendata/companies";

/// APR's open data is keyless, so the client is on when nothing is configured (ADR 0002
/// section 7).
pub const DEFAULT_ENABLED: bool = true;

/// Every APR answer's tier.
pub const TIER: TrustTier = TrustTier::Official;

const ACCEPT: &str = "application/json";

/// The longest name searched for.
const MAX_NAME_CHARS: usize = 256;

/// The APR client's settings. The defaults are the production values.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AprConfig {
    /// Whether the client sends anything (default [`DEFAULT_ENABLED`]).
    pub enabled: bool,
    /// The companies set (default [`DEFAULT_URL`]).
    pub url: String,
    /// The whole download, body included (default 5 minutes: about 58 MB).
    pub timeout: Duration,
    /// Connecting (default 10 s).
    pub connect_timeout: Duration,
    /// Downloads a minute (default 1). APR publishes no limit; the set changes monthly.
    pub per_minute: u32,
    /// The bucket size (default 1).
    pub burst: u32,
    /// The largest set read (default 128 MiB; it is about 58 MB in 2026). The body is held
    /// while it is indexed, then dropped.
    pub max_body: usize,
    /// The most companies indexed (default 400,000; about 134,000 in 2026). A larger set is
    /// refused as `Malformed`.
    pub max_companies: usize,
    /// Retries after a timeout, a transport error, a 5xx or a 429 (default 2).
    pub max_retries: u32,
    /// The first backoff, doubling each retry (default 10 s).
    pub backoff: Duration,
    /// The longest `Retry-After` honoured (default 60 s); a longer one gives up.
    pub max_retry_after: Duration,
    /// How long the set and a company found in it stay fresh (default 30 days, ADR 0002
    /// section 2).
    pub ttl: Duration,
    /// How long "no such MB" stays fresh in the cache (default 1 day).
    pub ttl_missing: Duration,
    /// After a failed download, nothing is sent for this long (default 15 minutes).
    pub failure_pause: Duration,
    /// Candidates a name search returns (default 10, at most 100).
    pub search_size: u32,
    /// Extra certificates to trust for the download, as a PEM bundle, next to the system's
    /// roots (default none). APR's server sends its certificate without the issuing
    /// intermediate, so a client that does not fetch intermediates (this one) cannot verify
    /// it; the operator may supply that intermediate here. See docs/registries.md.
    pub extra_roots_pem: Option<String>,
}

impl Default for AprConfig {
    fn default() -> Self {
        const DAY: u64 = 24 * 60 * 60;
        Self {
            enabled: DEFAULT_ENABLED,
            url: DEFAULT_URL.to_owned(),
            timeout: Duration::from_mins(5),
            connect_timeout: Duration::from_secs(10),
            per_minute: 1,
            burst: 1,
            max_body: 128 * 1024 * 1024,
            max_companies: 400_000,
            max_retries: 2,
            backoff: Duration::from_secs(10),
            max_retry_after: Duration::from_mins(1),
            ttl: Duration::from_secs(30 * DAY),
            ttl_missing: Duration::from_secs(DAY),
            failure_pause: Duration::from_mins(15),
            search_size: 10,
            extra_roots_pem: None,
        }
    }
}

/// What [`Apr::refresh`] downloaded.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SetInfo {
    /// The set's date (`DatumPreseka`), when it has a valid one.
    pub as_of: Option<time::Date>,
    /// When it was downloaded.
    pub fetched_at: time::OffsetDateTime,
    /// Companies indexed.
    pub companies: usize,
}

/// The APR client. Cheap to share (`Arc`); the snapshot and the rate limiter are shared by
/// every caller, and one download serves them all.
#[derive(Debug)]
pub struct Apr {
    enabled: bool,
    url: Url,
    fetcher: Fetcher,
    clock: Arc<dyn Clock>,
    ttl: Duration,
    failure_pause: Duration,
    max_companies: usize,
    search_size: usize,
    /// The current snapshot; the lock is held during a download, so callers wait for one.
    snapshot: tokio::sync::Mutex<Option<Arc<Snapshot>>>,
    /// After a failed download: until when (monotonic) nothing is sent, and why.
    paused: Mutex<Option<(Duration, WarningKind)>>,
}

impl Apr {
    /// A client on the real network and clock, caching in `cache`.
    pub fn new(config: &AprConfig, cache: Arc<dyn Cache>) -> Result<Self, Warning> {
        let transport = match config.extra_roots_pem.as_deref() {
            None => HttpTransport::new(config.timeout, config.connect_timeout),
            Some(pem) => {
                HttpTransport::with_extra_roots(config.timeout, config.connect_timeout, pem)
            }
        }
        .map_err(|e| warning(WarningKind::Config(e)))?;
        Self::with_parts(
            config,
            Arc::new(transport),
            cache,
            Arc::new(SystemClock::default()),
        )
    }

    /// A client on the given transport, cache and clock.
    pub fn with_parts(
        config: &AprConfig,
        transport: Arc<dyn Transport>,
        cache: Arc<dyn Cache>,
        clock: Arc<dyn Clock>,
    ) -> Result<Self, Warning> {
        let url: Url = config
            .url
            .parse()
            .map_err(|e| warning(WarningKind::Config(format!("URL: {e}"))))?;
        if !matches!(url.scheme(), "https" | "http") || url.cannot_be_a_base() {
            return Err(warning(WarningKind::Config(
                "URL must be http(s)".to_owned(),
            )));
        }
        let policy = FetchPolicy {
            max_body: config.max_body,
            max_retries: config.max_retries,
            backoff: config.backoff,
            max_retry_after: config.max_retry_after,
            ttl_found: config.ttl,
            ttl_missing: config.ttl_missing,
        };
        Ok(Self {
            enabled: config.enabled,
            url,
            fetcher: Fetcher::new(
                Registry::Apr,
                transport,
                cache,
                clock.clone(),
                TokenBucket::new(config.per_minute, config.burst),
                policy,
            ),
            clock,
            ttl: config.ttl,
            failure_pause: config.failure_pause,
            max_companies: config.max_companies.max(1),
            search_size: config.search_size.clamp(1, 100) as usize,
            snapshot: tokio::sync::Mutex::new(None),
            paused: Mutex::new(None),
        })
    }

    /// The company with MB `mb` (spaces, dots and dashes allowed); `None` when the set has
    /// none (never registered, or deleted). An invalid MB is [`WarningKind::InvalidInput`],
    /// and nothing is sent.
    pub async fn company(&self, mb: &str) -> Result<Fetched<Option<Company>>, Warning> {
        self.lookup(IdKind::RsMb, mb).await
    }

    /// The company with the Serbian identifier `raw` of `kind`. An MB is looked up. A PIB is
    /// validated, then [`WarningKind::Unsupported`]: the open-data set has no PIB. Any other
    /// kind, or an identifier that fails its check digits, is [`WarningKind::InvalidInput`].
    /// Nothing is sent unless an MB is looked up.
    pub async fn lookup(
        &self,
        kind: IdKind,
        raw: &str,
    ) -> Result<Fetched<Option<Company>>, Warning> {
        self.on()?;
        let id = match kind {
            IdKind::RsMb | IdKind::RsPib => Identifier::parse(kind, raw).ok_or_else(|| {
                warning(WarningKind::InvalidInput(format!(
                    "not a valid {}",
                    if kind == IdKind::RsMb { "MB" } else { "PIB" }
                )))
            })?,
            _ => {
                return Err(warning(WarningKind::InvalidInput(
                    "not a Serbian MB or PIB".to_owned(),
                )));
            }
        };
        if kind == IdKind::RsPib {
            return Err(warning(WarningKind::Unsupported(
                "the APR open data has no PIB; look up by MB".to_owned(),
            )));
        }
        self.by_mb(&id).await
    }

    async fn by_mb(&self, mb: &Identifier) -> Result<Fetched<Option<Company>>, Warning> {
        let key = format!("companies/{}", mb.value);
        if let Some(hit) = self.fetcher.cached(&key).await {
            match &hit.body {
                Body::NotFound => return Ok(Fetched::new(None, &hit)),
                // A cached record that no longer reads is a miss.
                Body::Found(b) => {
                    if let Ok(c) = model::company(b, mb) {
                        return Ok(Fetched::new(Some(c), &hit));
                    }
                }
            }
        }
        let snapshot = self.snapshot().await?;
        let fetch = |body| Fetch {
            body,
            fetched_at: snapshot.fetched_at,
            cached: false,
        };
        let Some(raw) = snapshot.record(&mb.value) else {
            let f = fetch(Body::NotFound);
            self.fetcher.remember(&key, &f).await;
            return Ok(Fetched::new(None, &f));
        };
        let bytes = serde_json::to_vec(&raw).map_err(|e| self.malformed(e.to_string()))?;
        let company = model::from_raw(raw, mb.clone()).map_err(|e| self.malformed(e))?;
        let f = fetch(Body::Found(bytes));
        self.fetcher.remember(&key, &f).await;
        Ok(Fetched::new(Some(company), &f))
    }

    /// The companies whose name has every word of `name` (Cyrillic or Latin, case, marks and
    /// punctuation ignored, as [`doewood_enrich::normalize`]), exact names first, then by MB;
    /// at most [`AprConfig::search_size`]. Matching them to a company is the caller's
    /// (doewood-enrich's fuzzy score). The name is never sent.
    pub async fn search(&self, name: &str) -> Result<Fetched<Vec<Company>>, Warning> {
        self.on()?;
        if name.chars().count() > MAX_NAME_CHARS {
            return Err(warning(WarningKind::InvalidInput(format!(
                "name longer than {MAX_NAME_CHARS} characters"
            ))));
        }
        let want = words(name);
        if want.is_empty() {
            return Err(warning(WarningKind::InvalidInput("empty name".to_owned())));
        }
        let snapshot = self.snapshot().await?;
        let fetched_at = snapshot.fetched_at;
        let size = self.search_size;
        let hits = blocking(move || {
            let mut hits: Vec<(bool, Company)> = snapshot
                .filter(|n| {
                    let have = words(n);
                    want.iter().all(|w| have.contains(w))
                })
                .into_iter()
                .filter_map(|raw| {
                    let mb = Identifier::parse(IdKind::RsMb, raw.mb.as_deref()?)?;
                    let exact = words(raw.name.as_deref()?) == want;
                    model::from_raw(raw, mb).ok().map(|c| (!exact, c))
                })
                .collect();
            hits.sort_by(|a, b| (a.0, &a.1.mb.value).cmp(&(b.0, &b.1.mb.value)));
            hits.into_iter().take(size).map(|(_, c)| c).collect()
        })
        .await?;
        Ok(Fetched {
            value: hits,
            fetched_at,
        })
    }

    /// Downloads the set again now, whatever its age, and swaps it in for every caller (APR's
    /// monthly refresh: it publishes no deltas, ADR 0002 section 6). A failed download keeps
    /// the current set (and pauses the client, as any failed download).
    pub async fn refresh(&self) -> Result<SetInfo, Warning> {
        self.on()?;
        let mut current = self.snapshot.lock().await;
        match self.download().await {
            Ok(s) => {
                *current = Some(s.clone());
                *self.paused.lock().unwrap_or_else(PoisonError::into_inner) = None;
                tracing::info!(
                    companies = s.len(),
                    as_of = s.as_of().unwrap_or(""),
                    "apr: set downloaded again and swapped in"
                );
                Ok(SetInfo {
                    as_of: crate::parse::date(s.as_of()),
                    fetched_at: s.fetched_at,
                    companies: s.len(),
                })
            }
            Err(w) => {
                let until = self.clock.monotonic() + self.failure_pause;
                *self.paused.lock().unwrap_or_else(PoisonError::into_inner) =
                    Some((until, w.kind.clone()));
                Err(w)
            }
        }
    }

    /// After a [`refresh`](Self::refresh): stores the current set's record of `mb` (or "no
    /// such MB") in the cache, so every instance's next lookup reads it, and says whether it
    /// differs from the record the cache held (the snapshot date aside). `false` when the cache
    /// held nothing: then the company is due on its own schedule anyway.
    pub async fn update_cached(&self, mb: &Identifier) -> Result<bool, Warning> {
        self.on()?;
        if mb.kind != IdKind::RsMb {
            return Err(warning(WarningKind::InvalidInput("not an MB".to_owned())));
        }
        let key = format!("companies/{}", mb.value);
        let snapshot = self.snapshot().await?;
        let now = snapshot.record(&mb.value);
        let before = self.fetcher.cached(&key).await;
        let changed = match (&before, &now) {
            (None, _) => false,
            (
                Some(Fetch {
                    body: Body::NotFound,
                    ..
                }),
                n,
            ) => n.is_some(),
            (
                Some(Fetch {
                    body: Body::Found(_),
                    ..
                }),
                None,
            ) => true,
            (
                Some(Fetch {
                    body: Body::Found(b),
                    ..
                }),
                Some(n),
            ) => match serde_json::from_slice::<model::RawRecord>(b) {
                Ok(mut old) => {
                    let mut new = n.clone();
                    old.as_of = None;
                    new.as_of = None;
                    old != new
                }
                Err(_) => true,
            },
        };
        let body = match now {
            None => Body::NotFound,
            Some(raw) => {
                Body::Found(serde_json::to_vec(&raw).map_err(|e| self.malformed(e.to_string()))?)
            }
        };
        let f = Fetch {
            body,
            fetched_at: snapshot.fetched_at,
            cached: false,
        };
        self.fetcher.remember(&key, &f).await;
        Ok(changed)
    }

    /// The current snapshot, downloaded and indexed when there is none or it is older than
    /// the TTL.
    async fn snapshot(&self) -> Result<Arc<Snapshot>, Warning> {
        let mut current = self.snapshot.lock().await;
        if let Some(s) = current.as_ref() {
            // A download date in the future (clock skew) counts as fresh.
            if self.clock.now_utc() - s.fetched_at <= self.ttl {
                return Ok(s.clone());
            }
        }
        if let Some((until, kind)) = self
            .paused
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
            && self.clock.monotonic() < until
        {
            return Err(warning(kind));
        }
        let result = self.download().await;
        match result {
            Ok(s) => {
                *current = Some(s.clone());
                *self.paused.lock().unwrap_or_else(PoisonError::into_inner) = None;
                tracing::info!(companies = s.len(), skipped = s.skipped, "apr: set indexed");
                Ok(s)
            }
            Err(w) => {
                let until = self.clock.monotonic() + self.failure_pause;
                *self.paused.lock().unwrap_or_else(PoisonError::into_inner) =
                    Some((until, w.kind.clone()));
                Err(w)
            }
        }
    }

    async fn download(&self) -> Result<Arc<Snapshot>, Warning> {
        // Never remembered: the set is kept as the index, not as a cached body.
        let fetch = self.fetcher.get("companies", &self.url, ACCEPT).await?;
        let Body::Found(body) = fetch.body else {
            return Err(self.fetcher.warning(WarningKind::Status(404)));
        };
        let max = self.max_companies;
        let at = fetch.fetched_at;
        let parsed = blocking(move || snapshot::parse(&body, max, at)).await?;
        parsed
            .map(Arc::new)
            .map_err(|e| self.malformed(format!("companies set: {e}")))
    }

    fn on(&self) -> Result<(), Warning> {
        if self.enabled {
            Ok(())
        } else {
            Err(warning(WarningKind::Disabled))
        }
    }

    fn malformed(&self, e: String) -> Warning {
        let w = self.fetcher.warning(WarningKind::Malformed(e));
        tracing::warn!(warning = %w, "apr: unexpected response");
        w
    }
}

/// Runs CPU-bound work (indexing tens of megabytes, a search over every name) off the async
/// workers.
async fn blocking<T: Send + 'static>(f: impl FnOnce() -> T + Send + 'static) -> Result<T, Warning> {
    tokio::task::spawn_blocking(f)
        .await
        .map_err(|e| warning(WarningKind::Malformed(format!("indexing task: {e}"))))
}

/// The words of a name for search: normalized (script-blind), split on spaces.
fn words(s: &str) -> Vec<String> {
    normalize(s)
        .split(' ')
        .filter(|w| !w.is_empty())
        .map(str::to_owned)
        .collect()
}

fn warning(kind: WarningKind) -> Warning {
    Warning::new(Registry::Apr, kind)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn search_words_are_script_blind() {
        assert_eq!(
            words("Пекара Ђурђевак д.о.о."),
            ["pekara", "djurdjevak", "doo"]
        );
        assert_eq!(
            words("PEKARA Đurđevak d.o.o."),
            ["pekara", "djurdjevak", "doo"]
        );
        assert!(words(" .,- ").is_empty());
    }
}
