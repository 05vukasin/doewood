//! The UK Companies House client (ADR 0002 section 2): the public data API
//! (`api.company-information.service.gov.uk`), with the operator's free API key.
//!
//! - [`CompaniesHouse::company`]: a company profile by number: name, status, type,
//!   jurisdiction, creation and cessation dates, **previous names with dates**, SIC codes and
//!   the registered office.
//! - [`CompaniesHouse::officers`]: its officers, every page (up to a cap).
//! - [`CompaniesHouse::significant_control`]: its people with significant control (PSCs),
//!   every page: individuals, corporate entities (with their registration number, to follow
//!   ownership chains), legal persons and super-secure PSCs.
//! - [`CompaniesHouse::filing_events`]: the name changes and merger filings of its filing
//!   history, with dates.
//! - [`CompaniesHouse::search`]: companies by name: candidates only (matching is
//!   doewood-enrich's).
//!
//! **The key** ([`ApiKey`]) is runtime configuration (ADR 0002 section 7), sent as the user
//! name of HTTP basic auth with an empty password, as Companies House specifies. With no key,
//! every call is [`WarningKind::NotConfigured`] and nothing is sent. A 401 is
//! [`WarningKind::KeyRefused`] and the client then sends nothing for
//! [`CompaniesHouseConfig::key_pause`]. The key is never in a warning, a log or a `Debug`.
//!
//! **Personal data** is kept minimal: officers and PSCs are mostly natural persons. Only the
//! name, the role, the dates, the nationality, the country of residence and the natures of
//! control are kept; dates of birth, addresses, occupations and officer ids are dropped when
//! the answer is read and never cached (see [`model`](self) and docs/registries.md).
//!
//! Every answer is [`TrustTier::Official`]. A company number is validated
//! ([`CompanyNumber::parse`]) before anything is sent.

mod model;

use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use serde::Serialize;
use serde::de::DeserializeOwned;
use url::Url;

pub use model::{
    Company, CompanyNumber, Filing, FilingEvent, Listing, Office, Officer, PreviousName, Psc,
    PscKind, Registration, SearchHit, Status,
};
use model::{KeptFiling, KeptOfficer, KeptPsc, Stored};

use crate::cache::Cache;
use crate::clock::{Clock, SystemClock};
pub use crate::fetch::Fetched;
use crate::fetch::{Body, Fetch, FetchPolicy, Fetcher, Inspect};
pub use crate::http::ApiKey;
use crate::http::{HttpTransport, Request, Response, Transport};
use crate::limit::TokenBucket;
pub use crate::trust::TrustTier;
use crate::warning::{Registry, Warning, WarningKind};

/// The public data API.
pub const DEFAULT_URL: &str = "https://api.company-information.service.gov.uk/";

/// On by default; it still sends nothing until a key is configured (ADR 0002 section 7).
pub const DEFAULT_ENABLED: bool = true;

/// Every Companies House answer's tier.
pub const TIER: TrustTier = TrustTier::Official;

const JSON: &str = "application/json";

/// The longest name searched for.
const MAX_NAME_CHARS: usize = 256;

const DAY: u64 = 24 * 60 * 60;

/// The Companies House client's settings. The defaults are the production values.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CompaniesHouseConfig {
    /// Whether the client sends anything (default [`DEFAULT_ENABLED`]).
    pub enabled: bool,
    /// The operator's API key (default `None`: every call is
    /// [`WarningKind::NotConfigured`]). Its `Debug` is redacted.
    pub api_key: Option<ApiKey>,
    /// The API root (default [`DEFAULT_URL`]).
    pub base_url: String,
    /// The whole request, body included (default 15 s).
    pub timeout: Duration,
    /// Connecting (default 5 s).
    pub connect_timeout: Duration,
    /// Requests a minute, every endpoint together (default 100). Companies House allows 600
    /// in 5 minutes per key.
    pub per_minute: u32,
    /// The bucket size (default 10: at most 510 requests in any 5 minutes).
    pub burst: u32,
    /// The largest body (default 1 MiB).
    pub max_body: usize,
    /// Retries after a timeout, a transport error, a 5xx or a 429 (default 2).
    pub max_retries: u32,
    /// The first backoff, doubling each retry (default 5 s).
    pub backoff: Duration,
    /// The longest `Retry-After` honoured (default 60 s); a longer one gives up.
    pub max_retry_after: Duration,
    /// How long after a 401 nothing is sent (default 30 minutes).
    pub key_pause: Duration,
    /// How long an answer stays fresh (default 30 days, the ADR's refresh).
    pub ttl: Duration,
    /// How long a "no such company" stays fresh (default 1 day).
    pub ttl_missing: Duration,
    /// Items asked for per page (default 100, at most 100).
    pub page_size: u32,
    /// The most officers or PSCs read (default 1,000).
    pub max_items: usize,
    /// The most filings read to find the name changes and mergers (default 2,000, newest
    /// first).
    pub max_filings: usize,
    /// Candidates a name search returns (default 10, at most 100).
    pub search_size: u32,
}

impl Default for CompaniesHouseConfig {
    fn default() -> Self {
        Self {
            enabled: DEFAULT_ENABLED,
            api_key: None,
            base_url: DEFAULT_URL.to_owned(),
            timeout: Duration::from_secs(15),
            connect_timeout: Duration::from_secs(5),
            per_minute: 100,
            burst: 10,
            max_body: 1024 * 1024,
            max_retries: 2,
            backoff: Duration::from_secs(5),
            max_retry_after: Duration::from_mins(1),
            key_pause: Duration::from_mins(30),
            ttl: Duration::from_secs(30 * DAY),
            ttl_missing: Duration::from_secs(DAY),
            page_size: 100,
            max_items: 1000,
            max_filings: 2000,
            search_size: 10,
        }
    }
}

/// The Companies House client. Cheap to share (`Arc`); its limits are shared by every caller.
#[derive(Debug)]
pub struct CompaniesHouse {
    enabled: bool,
    key: Option<ApiKey>,
    base: Url,
    page_size: usize,
    max_items: usize,
    max_filings: usize,
    search_size: usize,
    key_pause: Duration,
    clock: Arc<dyn Clock>,
    /// Nothing is sent before this monotonic instant (after a 401).
    paused_until: Mutex<Option<Duration>>,
    fetcher: Fetcher,
}

impl CompaniesHouse {
    /// A client on the real network and clock, caching in `cache`.
    pub fn new(config: &CompaniesHouseConfig, cache: Arc<dyn Cache>) -> Result<Self, Warning> {
        let transport = HttpTransport::new(config.timeout, config.connect_timeout)
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
        config: &CompaniesHouseConfig,
        transport: Arc<dyn Transport>,
        cache: Arc<dyn Cache>,
        clock: Arc<dyn Clock>,
    ) -> Result<Self, Warning> {
        let base = root(&config.base_url)?;
        let policy = FetchPolicy {
            max_body: config.max_body,
            max_retries: config.max_retries,
            backoff: config.backoff,
            max_retry_after: config.max_retry_after,
            ttl_found: config.ttl,
            ttl_missing: config.ttl_missing,
        };
        let fetcher = Fetcher::new(
            Registry::CompaniesHouse,
            transport,
            cache,
            clock.clone(),
            TokenBucket::new(config.per_minute, config.burst),
            policy,
        );
        Ok(Self {
            enabled: config.enabled,
            key: config.api_key.clone(),
            base,
            page_size: config.page_size.clamp(1, 100) as usize,
            max_items: config.max_items.max(1),
            max_filings: config.max_filings.max(1),
            search_size: config.search_size.clamp(1, 100) as usize,
            key_pause: config.key_pause,
            clock,
            paused_until: Mutex::new(None),
            fetcher,
        })
    }

    /// The company with number `number` (`99000001`, `990001`, `SC990001`); `None` when
    /// Companies House has none. An invalid number is [`WarningKind::InvalidInput`], and
    /// nothing is sent.
    pub async fn company(&self, number: &str) -> Result<Fetched<Option<Company>>, Warning> {
        let r = async {
            let (key, number) = self.ready(number)?;
            let path = format!("company/{number}");
            let url = self.url(&path, &[])?;
            let fetch = self.send(key, &path, &url).await?;
            let value = match &fetch.body {
                Body::NotFound => None,
                Body::Found(b) => Some(model::company(b, &number).map_err(malformed)?),
            };
            self.fetcher.remember(&path, &fetch).await;
            Ok(Fetched::new(value, &fetch))
        };
        r.await.map_err(|w| self.redact(w))
    }

    /// The officers of company `number`, current and resigned, every page up to
    /// [`CompaniesHouseConfig::max_items`]. A natural person's date of birth, address and
    /// occupation are not kept. An unknown company gives an empty list (Companies House
    /// answers 404).
    pub async fn officers(&self, number: &str) -> Result<Fetched<Listing<Officer>>, Warning> {
        let r = async {
            let (key, number) = self.ready(number)?;
            let read = self
                .list(
                    key,
                    &format!("company/{number}/officers"),
                    self.max_items,
                    KeptOfficer::minimise,
                )
                .await?;
            Ok(read.listing(|items| items.into_iter().filter_map(KeptOfficer::officer).collect()))
        };
        r.await.map_err(|w| self.redact(w))
    }

    /// The people with significant control of company `number`, current and ceased, every
    /// page up to [`CompaniesHouseConfig::max_items`]. An individual's date of birth and
    /// address are not kept; a corporate entity keeps its registration. An unknown company,
    /// or one with no PSC, gives an empty list.
    pub async fn significant_control(
        &self,
        number: &str,
    ) -> Result<Fetched<Listing<Psc>>, Warning> {
        let r = async {
            let (key, number) = self.ready(number)?;
            let read = self
                .list(
                    key,
                    &format!("company/{number}/persons-with-significant-control"),
                    self.max_items,
                    KeptPsc::minimise,
                )
                .await?;
            Ok(read.listing(|items| items.into_iter().map(KeptPsc::psc).collect()))
        };
        r.await.map_err(|w| self.redact(w))
    }

    /// The name changes and merger filings in the filing history of company `number`, newest
    /// first, from at most [`CompaniesHouseConfig::max_filings`] filings. The dated renames
    /// themselves are also in [`Company::previous_names`].
    pub async fn filing_events(&self, number: &str) -> Result<Fetched<Listing<Filing>>, Warning> {
        let r = async {
            let (key, number) = self.ready(number)?;
            let read = self
                .list(
                    key,
                    &format!("company/{number}/filing-history"),
                    self.max_filings,
                    KeptFiling::minimise,
                )
                .await?;
            Ok(read.listing(|items| items.into_iter().filter_map(KeptFiling::filing).collect()))
        };
        r.await.map_err(|w| self.redact(w))
    }

    /// Companies matching `name`, in Companies House's order, at most
    /// [`CompaniesHouseConfig::search_size`]: candidates only. Hits without a valid company
    /// number or a name are skipped.
    pub async fn search(&self, name: &str) -> Result<Fetched<Vec<SearchHit>>, Warning> {
        let r = async {
            let key = self.key()?;
            let name = name.split_whitespace().collect::<Vec<_>>().join(" ");
            if name.is_empty() {
                return Err(warning(WarningKind::InvalidInput("empty name".to_owned())));
            }
            if name.chars().count() > MAX_NAME_CHARS || name.chars().any(char::is_control) {
                return Err(warning(WarningKind::InvalidInput(format!(
                    "a name is at most {MAX_NAME_CHARS} characters, without control characters"
                ))));
            }
            let size = self.search_size.to_string();
            let url = self.url(
                "search/companies",
                &[
                    ("q", &name),
                    ("items_per_page", &size),
                    ("start_index", "0"),
                ],
            )?;
            let cache_key = format!("search/companies?{}", url.query().unwrap_or_default());
            let fetch = self.send(key, &cache_key, &url).await?;
            let value = match &fetch.body {
                Body::NotFound => Vec::new(),
                Body::Found(b) => {
                    let mut hits = model::search(b).map_err(malformed)?;
                    hits.truncate(self.search_size);
                    hits
                }
            };
            self.fetcher.remember(&cache_key, &fetch).await;
            Ok(Fetched::new(value, &fetch))
        };
        r.await.map_err(|w| self.redact(w))
    }

    /// Every page of the list at `path`, each item read as `T` and kept by `minimise`, up to
    /// `cap` raw items. Each page is cached as the kept items only.
    async fn list<T, F>(
        &self,
        key: &ApiKey,
        path: &str,
        cap: usize,
        minimise: F,
    ) -> Result<Read<T>, Warning>
    where
        T: DeserializeOwned + Serialize,
        F: Fn(T) -> Option<T> + Copy,
    {
        let mut items = Vec::new();
        let mut total = None;
        let mut start = 0usize;
        let mut oldest: Option<time::OffsetDateTime> = None;
        let size = self.page_size.to_string();
        let mut truncated = loop {
            let index = start.to_string();
            let query = [("items_per_page", size.as_str()), ("start_index", &index)];
            let url = self.url(path, &query)?;
            let cache_key = format!("{path}?{}", url.query().unwrap_or_default());
            let fetch = self.send(key, &cache_key, &url).await?;
            oldest = Some(oldest.map_or(fetch.fetched_at, |o| o.min(fetch.fetched_at)));
            let page: Stored<T> = match &fetch.body {
                Body::NotFound => {
                    self.fetcher.remember(&cache_key, &fetch).await;
                    break false;
                }
                Body::Found(b) if fetch.cached => model::stored(b).map_err(malformed)?,
                Body::Found(b) => {
                    let raw: Stored<T> = model::page(b).map_err(malformed)?;
                    let kept = Stored {
                        items: raw.items.into_iter().filter_map(minimise).collect(),
                        total: raw.total,
                        consumed: raw.consumed,
                    };
                    let bytes = serde_json::to_vec(&kept)
                        .map_err(|e| malformed(format!("re-encoding a page: {e}")))?;
                    let minimal = Fetch {
                        body: Body::Found(bytes),
                        fetched_at: fetch.fetched_at,
                        cached: false,
                    };
                    self.fetcher.remember(&cache_key, &minimal).await;
                    kept
                }
            };
            total = page.total.or(total);
            items.extend(page.items);
            let consumed = page.consumed;
            start += consumed;
            let more = match total {
                Some(t) => (start as u64) < t,
                None => consumed >= self.page_size,
            };
            if consumed == 0 || !more {
                break false;
            }
            if start >= cap {
                break true;
            }
        };
        if items.len() > cap {
            items.truncate(cap);
            truncated = true;
        }
        Ok(Read {
            fetched_at: oldest.unwrap_or_else(|| self.clock.now_utc()),
            items,
            total,
            truncated,
        })
    }

    /// The cached answer, else (unless paused after a 401) the request with the key.
    async fn send(&self, key: &ApiKey, cache_key: &str, url: &Url) -> Result<Fetch, Warning> {
        if let Some(hit) = self.fetcher.cached(cache_key).await {
            return Ok(hit);
        }
        let now = self.clock.monotonic();
        let until = *self
            .paused_until
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if until.is_some_and(|u| u > now) {
            return Err(warning(WarningKind::KeyRefused));
        }
        let request = Request::get(url, JSON).with_basic_auth(key);
        let result = self.fetcher.send(cache_key, &request, &inspect).await;
        if let Err(Warning {
            kind: WarningKind::KeyRefused,
            ..
        }) = &result
        {
            let until = self.clock.monotonic() + self.key_pause;
            *self
                .paused_until
                .lock()
                .unwrap_or_else(PoisonError::into_inner) = Some(until);
        }
        result
    }

    /// The key and the validated number, or why nothing is sent.
    fn ready(&self, number: &str) -> Result<(&ApiKey, CompanyNumber), Warning> {
        let key = self.key()?;
        let number = CompanyNumber::parse(number).ok_or_else(|| {
            warning(WarningKind::InvalidInput(
                "not a company number (8 digits, or 2 letters and 6 digits)".to_owned(),
            ))
        })?;
        Ok((key, number))
    }

    fn key(&self) -> Result<&ApiKey, Warning> {
        if !self.enabled {
            return Err(warning(WarningKind::Disabled));
        }
        self.key
            .as_ref()
            .ok_or_else(|| warning(WarningKind::NotConfigured))
    }

    /// `path` (built from validated parts only) under the root, with `query`.
    fn url(&self, path: &str, query: &[(&str, &str)]) -> Result<Url, Warning> {
        let mut url = self
            .base
            .join(path)
            .map_err(|e| warning(WarningKind::Config(format!("URL: {e}"))))?;
        if !query.is_empty() {
            url.query_pairs_mut().extend_pairs(query);
        }
        Ok(url)
    }

    /// `w` without the key in any of its texts.
    fn redact(&self, w: Warning) -> Warning {
        let Some(key) = &self.key else {
            return w;
        };
        let kind = match w.kind {
            WarningKind::Unsupported(s) => WarningKind::Unsupported(key.redact(&s)),
            WarningKind::Config(s) => WarningKind::Config(key.redact(&s)),
            WarningKind::InvalidInput(s) => WarningKind::InvalidInput(key.redact(&s)),
            WarningKind::Transport(s) => WarningKind::Transport(key.redact(&s)),
            WarningKind::Malformed(s) => WarningKind::Malformed(key.redact(&s)),
            WarningKind::Wikidata(s) => WarningKind::Wikidata(key.redact(&s)),
            other => other,
        };
        Warning { kind, ..w }
    }
}

/// A list as read by [`CompaniesHouse::list`].
struct Read<T> {
    fetched_at: time::OffsetDateTime,
    items: Vec<T>,
    total: Option<u64>,
    truncated: bool,
}

impl<T> Read<T> {
    fn listing<U>(self, convert: impl FnOnce(Vec<T>) -> Vec<U>) -> Fetched<Listing<U>> {
        Fetched {
            value: Listing {
                items: convert(self.items),
                total: self.total,
                truncated: self.truncated,
            },
            fetched_at: self.fetched_at,
        }
    }
}

/// A 401 is the key refused: retrying does not fix it, so it fails at once and the client
/// pauses.
fn inspect(r: &Response) -> Inspect {
    if r.status == 401 {
        Inspect::Fail(WarningKind::KeyRefused)
    } else {
        Inspect::Status
    }
}

/// A root URL (`http(s)`, no query, no credentials), ending in `/`.
fn root(raw: &str) -> Result<Url, Warning> {
    let mut url: Url = raw
        .parse()
        .map_err(|e| warning(WarningKind::Config(format!("base URL: {e}"))))?;
    if !matches!(url.scheme(), "https" | "http")
        || url.cannot_be_a_base()
        || url.query().is_some()
        || !url.username().is_empty()
        || url.password().is_some()
    {
        return Err(warning(WarningKind::Config(
            "base URL must be http(s), without a query or credentials".to_owned(),
        )));
    }
    if !url.path().ends_with('/') {
        let path = format!("{}/", url.path());
        url.set_path(&path);
    }
    Ok(url)
}

fn warning(kind: WarningKind) -> Warning {
    Warning::new(Registry::CompaniesHouse, kind)
}

fn malformed(e: String) -> Warning {
    let e = if e.chars().count() > 300 {
        e.chars().take(300).collect()
    } else {
        e
    };
    let w = warning(WarningKind::Malformed(e));
    tracing::warn!(warning = %w, "companies house: unexpected response");
    w
}
