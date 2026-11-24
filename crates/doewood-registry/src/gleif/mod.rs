//! The GLEIF client (ADR 0002 section 2): `api.gleif.org/api/v1`, keyless.
//!
//! - [`Gleif::lookup`]: an LEI record by LEI (Level 1), with its successor entities and
//!   legal-entity events ([`LeiRecord::successions`], [`LeiRecord::renames`]).
//! - [`Gleif::search`]: candidates by legal name, optionally in one country.
//! - [`Gleif::parent`]: the direct or ultimate parent (Level 2): a dated relationship record,
//!   or the reporting exception that says why none is reported.
//!
//! An LEI is validated (ISO 17442) before it is sent; an invalid one is a
//! [`WarningKind::InvalidInput`] and no request is made. Every failure is a [`Warning`].

mod model;

use std::sync::Arc;
use std::time::Duration;

use doewood_enrich::{IdKind, Identifier};
use url::Url;

pub use model::{
    EntityEvent, EntityStatus, EventKind, ExceptionReason, Expiration, LeiRecord, OtherName,
    Parent, ParentKind, ParentRelationship, RelationshipStatus, ReportingException, Succession,
    Successor,
};

use crate::cache::Cache;
use crate::clock::{Clock, SystemClock};
use crate::fetch::{Body, Fetch, FetchPolicy, Fetcher};
use crate::http::{HttpTransport, Transport};
use crate::limit::TokenBucket;
use crate::warning::{Registry, Warning, WarningKind};

/// GLEIF's public API.
pub const DEFAULT_BASE_URL: &str = "https://api.gleif.org/api/v1/";

/// GLEIF is a keyless registry, so it is on when nothing is configured (ADR 0002 section 7).
pub const DEFAULT_ENABLED: bool = true;

/// The JSON:API media type GLEIF answers with.
const ACCEPT: &str = "application/vnd.api+json";

/// The longest legal name sent to a search.
const MAX_NAME_CHARS: usize = 256;

/// The GLEIF client's settings. The defaults are the production values.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GleifConfig {
    /// Whether the client sends anything (default [`DEFAULT_ENABLED`]).
    pub enabled: bool,
    /// The API root, ending in `/` (default [`DEFAULT_BASE_URL`]).
    pub base_url: String,
    /// The whole request, body included (default 10 s).
    pub timeout: Duration,
    /// Connecting (default 5 s).
    pub connect_timeout: Duration,
    /// Requests a minute (default 50; GLEIF allows 60 a minute).
    pub per_minute: u32,
    /// The bucket size (default 5): with 50 a minute, at most 55 in any minute.
    pub burst: u32,
    /// The largest body read (default 2 MiB). A page of 50 records is well under it.
    pub max_body: usize,
    /// Retries after a timeout, a transport error, a 5xx or a 429 (default 2).
    pub max_retries: u32,
    /// The first backoff, doubling each retry (default 1 s).
    pub backoff: Duration,
    /// The longest `Retry-After` honoured (default 60 s); a longer one gives up.
    pub max_retry_after: Duration,
    /// How long a record stays fresh in the cache (default 30 days, ADR 0002 section 2).
    pub ttl_found: Duration,
    /// How long "no such record" stays fresh (default 1 day: a new LEI can appear any day).
    pub ttl_missing: Duration,
    /// Candidates a name search returns (default 10, at most 200).
    pub search_size: u32,
}

impl Default for GleifConfig {
    fn default() -> Self {
        const DAY: u64 = 24 * 60 * 60;
        Self {
            enabled: DEFAULT_ENABLED,
            base_url: DEFAULT_BASE_URL.to_owned(),
            timeout: Duration::from_secs(10),
            connect_timeout: Duration::from_secs(5),
            per_minute: 50,
            burst: 5,
            max_body: 2 * 1024 * 1024,
            max_retries: 2,
            backoff: Duration::from_secs(1),
            max_retry_after: Duration::from_mins(1),
            ttl_found: Duration::from_secs(30 * DAY),
            ttl_missing: Duration::from_secs(DAY),
            search_size: 10,
        }
    }
}

/// An answer with its fetch date (from the cache, or now).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Fetched<T> {
    /// The answer.
    pub value: T,
    /// When GLEIF gave it.
    pub fetched_at: time::OffsetDateTime,
}

impl<T> Fetched<T> {
    fn new(value: T, fetch: &Fetch) -> Self {
        Self {
            value,
            fetched_at: fetch.fetched_at,
        }
    }
}

/// The GLEIF client. Cheap to share (`Arc`); its rate limiter is shared by every caller.
#[derive(Debug)]
pub struct Gleif {
    enabled: bool,
    base: Url,
    search_size: u32,
    fetcher: Fetcher,
}

impl Gleif {
    /// A client on the real network and clock, caching in `cache`.
    pub fn new(config: &GleifConfig, cache: Arc<dyn Cache>) -> Result<Self, Warning> {
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
        config: &GleifConfig,
        transport: Arc<dyn Transport>,
        cache: Arc<dyn Cache>,
        clock: Arc<dyn Clock>,
    ) -> Result<Self, Warning> {
        let mut base: Url = config
            .base_url
            .parse()
            .map_err(|e| warning(WarningKind::Config(format!("base URL: {e}"))))?;
        if !matches!(base.scheme(), "https" | "http") || base.cannot_be_a_base() {
            return Err(warning(WarningKind::Config(
                "base URL must be http(s)".to_owned(),
            )));
        }
        if !base.path().ends_with('/') {
            let path = format!("{}/", base.path());
            base.set_path(&path);
        }
        let policy = FetchPolicy {
            max_body: config.max_body,
            max_retries: config.max_retries,
            backoff: config.backoff,
            max_retry_after: config.max_retry_after,
            ttl_found: config.ttl_found,
            ttl_missing: config.ttl_missing,
        };
        Ok(Self {
            enabled: config.enabled,
            base,
            search_size: config.search_size.clamp(1, 200),
            fetcher: Fetcher::new(
                Registry::Gleif,
                transport,
                cache,
                clock,
                TokenBucket::new(config.per_minute, config.burst),
                policy,
            ),
        })
    }

    /// The LEI record of `lei`; `None` when GLEIF has none.
    pub async fn lookup(&self, lei: &str) -> Result<Fetched<Option<LeiRecord>>, Warning> {
        let lei = self.valid(lei)?;
        let rel = format!("lei-records/{}", lei.value);
        let fetch = self.get(&rel, self.url(&rel)?).await?;
        let value = match &fetch.body {
            Body::NotFound => None,
            Body::Found(b) => {
                let r = model::record(b).map_err(malformed)?;
                if let Some(r) = &r
                    && r.lei != lei
                {
                    return Err(malformed(format!(
                        "record of {} returned for {}",
                        r.lei.value, lei.value
                    )));
                }
                r
            }
        };
        self.fetcher.remember(&rel, &fetch).await;
        Ok(Fetched::new(value, &fetch))
    }

    /// Candidates whose legal name matches `name`, optionally with the legal address in
    /// `country` (ISO 3166-1 alpha-2). Records with an invalid LEI or no legal name are
    /// skipped. Matching them to a company is the caller's (doewood-enrich's fuzzy score).
    pub async fn search(
        &self,
        name: &str,
        country: Option<&str>,
    ) -> Result<Fetched<Vec<LeiRecord>>, Warning> {
        self.on()?;
        let name = name.split_whitespace().collect::<Vec<_>>().join(" ");
        if name.is_empty() {
            return Err(warning(WarningKind::InvalidInput("empty name".to_owned())));
        }
        if name.chars().count() > MAX_NAME_CHARS {
            return Err(warning(WarningKind::InvalidInput(format!(
                "name longer than {MAX_NAME_CHARS} characters"
            ))));
        }
        let country = match country.map(str::trim) {
            None | Some("") => None,
            Some(c) if c.len() == 2 && c.bytes().all(|b| b.is_ascii_alphabetic()) => {
                Some(c.to_ascii_uppercase())
            }
            Some(c) => {
                return Err(warning(WarningKind::InvalidInput(format!(
                    "not a two-letter country code: {c:?}"
                ))));
            }
        };
        let mut url = self.url("lei-records")?;
        {
            let mut q = url.query_pairs_mut();
            q.append_pair("filter[entity.legalName]", &name);
            if let Some(c) = &country {
                q.append_pair("filter[entity.legalAddress.country]", c);
            }
            q.append_pair("page[size]", &self.search_size.to_string());
            q.append_pair("page[number]", "1");
        }
        let rel = format!("lei-records?{}", url.query().unwrap_or_default());
        let fetch = self.get(&rel, url).await?;
        let value = match &fetch.body {
            Body::NotFound => Vec::new(),
            Body::Found(b) => {
                let (records, skipped) = model::records(b).map_err(malformed)?;
                if skipped > 0 {
                    tracing::debug!(skipped, "gleif: search results without a valid LEI or name");
                }
                records
            }
        };
        self.fetcher.remember(&rel, &fetch).await;
        Ok(Fetched::new(value, &fetch))
    }

    /// The direct or ultimate parent of `lei` (Level 2): the relationship record if one is
    /// reported, else the reporting exception, else [`Parent::NotReported`].
    pub async fn parent(&self, lei: &str, kind: ParentKind) -> Result<Fetched<Parent>, Warning> {
        let lei = self.valid(lei)?;
        let rel = format!(
            "lei-records/{}/{}-parent-relationship",
            lei.value,
            kind.as_str()
        );
        let fetch = self.get(&rel, self.url(&rel)?).await?;
        let reported = match &fetch.body {
            Body::Found(b) => model::relationship(b, kind, &lei).map_err(malformed)?,
            Body::NotFound => None,
        };
        self.fetcher.remember(&rel, &fetch).await;
        if let Some(r) = reported {
            return Ok(Fetched::new(Parent::Reported(r), &fetch));
        }
        let rel = format!(
            "lei-records/{}/{}-parent-reporting-exception",
            lei.value,
            kind.as_str()
        );
        let fetch = self.get(&rel, self.url(&rel)?).await?;
        let value = match &fetch.body {
            Body::Found(b) => model::exception(b, kind, &lei)
                .map_err(malformed)?
                .map_or(Parent::NotReported, Parent::Exception),
            Body::NotFound => Parent::NotReported,
        };
        self.fetcher.remember(&rel, &fetch).await;
        Ok(Fetched::new(value, &fetch))
    }

    fn on(&self) -> Result<(), Warning> {
        if self.enabled {
            Ok(())
        } else {
            Err(warning(WarningKind::Disabled))
        }
    }

    /// Checks the switch, then the LEI (before anything is sent).
    fn valid(&self, raw: &str) -> Result<Identifier, Warning> {
        self.on()?;
        Identifier::parse(IdKind::Lei, raw).ok_or_else(|| {
            warning(WarningKind::InvalidInput(
                "not a valid LEI (ISO 17442)".to_owned(),
            ))
        })
    }

    fn url(&self, rel: &str) -> Result<Url, Warning> {
        self.base
            .join(rel)
            .map_err(|e| warning(WarningKind::Config(format!("URL: {e}"))))
    }

    async fn get(&self, key: &str, url: Url) -> Result<Fetch, Warning> {
        self.fetcher.get(key, &url, ACCEPT).await
    }
}

fn warning(kind: WarningKind) -> Warning {
    Warning::new(Registry::Gleif, kind)
}

fn malformed(e: String) -> Warning {
    let w = warning(WarningKind::Malformed(e));
    tracing::warn!(warning = %w, "gleif: unexpected response");
    w
}
