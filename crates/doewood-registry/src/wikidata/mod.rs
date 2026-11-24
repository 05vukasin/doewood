//! The Wikidata client (ADR 0002 section 2): the query service (SPARQL) and the
//! `wbsearchentities` API, keyless. Wikidata is community-edited: **the lowest trust tier**
//! ([`TrustTier::Community`]); every answer carries that marker.
//!
//! - [`Wikidata::by_lei`]: the items whose LEI (P1278) is this one (normally one or none).
//! - [`Wikidata::item`]: the item with this Q-id.
//! - [`Wikidata::search`]: candidates by name (`wbsearchentities`); matching them is
//!   doewood-enrich's.
//!
//! Both lookups give the item's label, its LEIs, and its parent organization (P749), owned by
//! (P127), replaced by (P1366), followed by (P156), official website (P856) and logo (P154,
//! as a Commons URL), each with its start (P580) and end (P582) time and its rank. Deprecated
//! statements are left out.
//!
//! **No caller text reaches a SPARQL query.** A Q-id is parsed into a number ([`QId`]), an
//! LEI is validated (ISO 17442: 20 letters and digits), the language is a validated tag; the
//! query is built from those alone and percent-encoded into the URL. A name is only ever sent
//! to `wbsearchentities`, as a percent-encoded URL parameter, never into SPARQL.
//!
//! The query service's usage policy: at most 5 parallel queries and 60 s of query time a
//! minute per client, 429 with `Retry-After` beyond that, and a descriptive `User-Agent`
//! ([`crate::USER_AGENT`]). This client runs one request at a time per endpoint, 20 a minute.

mod model;
mod query;

use std::sync::Arc;
use std::time::Duration;

use doewood_enrich::{IdKind, Identifier};
use tokio::sync::Semaphore;
use url::Url;

pub use model::{Logo, Period, Precision, Relation, SearchHit, Website, WikiDate, WikidataItem};
pub use query::QId;

use crate::cache::Cache;
use crate::clock::{Clock, SystemClock};
pub use crate::fetch::Fetched;
use crate::fetch::{Body, Fetch, FetchPolicy, Fetcher, Inspect};
use crate::http::{HttpTransport, Request, Response, Transport};
use crate::limit::TokenBucket;
pub use crate::trust::TrustTier;
use crate::warning::{Registry, Warning, WarningKind};

/// The query service's SPARQL endpoint.
pub const DEFAULT_SPARQL_URL: &str = "https://query.wikidata.org/sparql";

/// The `MediaWiki` API of Wikidata (for `wbsearchentities`).
pub const DEFAULT_API_URL: &str = "https://www.wikidata.org/w/api.php";

/// Wikidata is keyless, so it is on when nothing is configured (ADR 0002 section 7).
pub const DEFAULT_ENABLED: bool = true;

/// Every Wikidata answer's tier.
pub const TIER: TrustTier = TrustTier::Community;

const SPARQL_ACCEPT: &str = "application/sparql-results+json";
const API_ACCEPT: &str = "application/json";

/// The longest name sent to a search.
const MAX_NAME_CHARS: usize = 256;

/// The Wikidata client's settings. The defaults are the production values.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WikidataConfig {
    /// Whether the client sends anything (default [`DEFAULT_ENABLED`]).
    pub enabled: bool,
    /// The SPARQL endpoint (default [`DEFAULT_SPARQL_URL`]).
    pub sparql_url: String,
    /// The `MediaWiki` API (default [`DEFAULT_API_URL`]).
    pub api_url: String,
    /// The language of labels and of the search (default `en`); the `mul` label is the
    /// fallback. A lowercase tag such as `en`, `de` or `sr-el`.
    pub language: String,
    /// The whole request, body included (default 20 s; the service stops a query at 60 s).
    pub timeout: Duration,
    /// Connecting (default 5 s).
    pub connect_timeout: Duration,
    /// Requests a minute, per endpoint (default 20). One request runs at a time per endpoint.
    pub per_minute: u32,
    /// The bucket size (default 1: no bursts).
    pub burst: u32,
    /// The largest body read (default 1 MiB; an item's answer is a few KiB).
    pub max_body: usize,
    /// Retries after a timeout, a transport error, a 5xx, a 429 or an API `maxlag` /
    /// `ratelimited` (default 2).
    pub max_retries: u32,
    /// The first backoff, doubling each retry (default 2 s).
    pub backoff: Duration,
    /// The longest `Retry-After` honoured (default 60 s); a longer one gives up.
    pub max_retry_after: Duration,
    /// How long an answer stays fresh, found or empty (default 30 days, ADR 0002 section 2).
    pub ttl: Duration,
    /// Candidates a name search returns (default 10, at most 50).
    pub search_size: u32,
}

impl Default for WikidataConfig {
    fn default() -> Self {
        const DAY: u64 = 24 * 60 * 60;
        Self {
            enabled: DEFAULT_ENABLED,
            sparql_url: DEFAULT_SPARQL_URL.to_owned(),
            api_url: DEFAULT_API_URL.to_owned(),
            language: "en".to_owned(),
            timeout: Duration::from_secs(20),
            connect_timeout: Duration::from_secs(5),
            per_minute: 20,
            burst: 1,
            max_body: 1024 * 1024,
            max_retries: 2,
            backoff: Duration::from_secs(2),
            max_retry_after: Duration::from_mins(1),
            ttl: Duration::from_secs(30 * DAY),
            search_size: 10,
        }
    }
}

/// One endpoint: its fetcher (cache, bucket, backoff) and a gate of one request at a time.
#[derive(Debug)]
struct Endpoint {
    url: Url,
    fetcher: Fetcher,
    gate: Semaphore,
}

impl Endpoint {
    async fn send(
        &self,
        key: &str,
        url: &Url,
        accept: &str,
        inspect: &(dyn Fn(&Response) -> Inspect + Sync),
    ) -> Result<Fetch, Warning> {
        // Never closed, so acquiring cannot fail; held through retries and waits.
        let _permit = self.gate.acquire().await;
        self.fetcher
            .send(key, &Request::get(url, accept), inspect)
            .await
    }
}

/// The Wikidata client. Cheap to share (`Arc`); its limits are shared by every caller.
#[derive(Debug)]
pub struct Wikidata {
    enabled: bool,
    language: String,
    search_size: u32,
    sparql: Endpoint,
    api: Endpoint,
}

impl Wikidata {
    /// A client on the real network and clock, caching in `cache`.
    pub fn new(config: &WikidataConfig, cache: Arc<dyn Cache>) -> Result<Self, Warning> {
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
        config: &WikidataConfig,
        transport: Arc<dyn Transport>,
        cache: Arc<dyn Cache>,
        clock: Arc<dyn Clock>,
    ) -> Result<Self, Warning> {
        if !query::valid_language(&config.language) {
            return Err(warning(WarningKind::Config(
                "language must be a lowercase tag such as en or sr-el".to_owned(),
            )));
        }
        let policy = FetchPolicy {
            max_body: config.max_body,
            max_retries: config.max_retries,
            backoff: config.backoff,
            max_retry_after: config.max_retry_after,
            ttl_found: config.ttl,
            // Neither endpoint answers 404 for "nothing found"; a 404 is a Status warning.
            ttl_missing: Duration::ZERO,
        };
        let sparql = endpoint(
            &config.sparql_url,
            "SPARQL",
            config,
            policy,
            (transport.clone(), cache.clone(), clock.clone()),
        )?;
        let api = endpoint(
            &config.api_url,
            "API",
            config,
            policy,
            (transport, cache, clock),
        )?;
        Ok(Self {
            enabled: config.enabled,
            language: config.language.clone(),
            search_size: config.search_size.clamp(1, 50),
            sparql,
            api,
        })
    }

    /// The items whose LEI (P1278) is `lei`, sorted by id: normally one or none. Each one
    /// lists `lei` among its [`WikidataItem::leis`].
    pub async fn by_lei(&self, lei: &str) -> Result<Fetched<Vec<WikidataItem>>, Warning> {
        self.on()?;
        let lei = Identifier::parse(IdKind::Lei, lei)
            .filter(|l| l.value.len() == 20 && l.value.bytes().all(|b| b.is_ascii_alphanumeric()))
            .ok_or_else(|| {
                warning(WarningKind::InvalidInput(
                    "not a valid LEI (ISO 17442)".to_owned(),
                ))
            })?;
        let key = format!("sparql/lei/{}?lang={}", lei.value, self.language);
        let q = query::sparql(query::Subject::Lei(&lei.value), &self.language);
        let (fetch, items) = self.query(&key, &q).await?;
        if let Some(i) = items.iter().find(|i| !i.leis.contains(&lei)) {
            return Err(malformed(format!(
                "{} returned for LEI {} without it",
                i.qid, lei.value
            )));
        }
        self.sparql.fetcher.remember(&key, &fetch).await;
        Ok(Fetched::new(items, &fetch))
    }

    /// The item `qid` (`Q42`); `None` when Wikidata has no such item (or it is a redirect).
    pub async fn item(&self, qid: &str) -> Result<Fetched<Option<WikidataItem>>, Warning> {
        self.on()?;
        let qid = QId::parse(qid).ok_or_else(|| {
            warning(WarningKind::InvalidInput(
                "not a Wikidata item id (Q followed by digits)".to_owned(),
            ))
        })?;
        let key = format!("sparql/item/{qid}?lang={}", self.language);
        let q = query::sparql(query::Subject::Item(qid), &self.language);
        let (fetch, mut items) = self.query(&key, &q).await?;
        if let Some(i) = items.iter().find(|i| i.qid != qid) {
            return Err(malformed(format!("{} returned for {qid}", i.qid)));
        }
        self.sparql.fetcher.remember(&key, &fetch).await;
        Ok(Fetched::new(items.pop(), &fetch))
    }

    /// Candidates whose label or alias matches `name` (`wbsearchentities`, items only, in the
    /// configured language), in Wikidata's order. Matching them to a company is the caller's.
    pub async fn search(&self, name: &str) -> Result<Fetched<Vec<SearchHit>>, Warning> {
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
        if name.chars().any(char::is_control) {
            return Err(warning(WarningKind::InvalidInput(
                "control characters in the name".to_owned(),
            )));
        }
        let mut url = self.api.url.clone();
        url.query_pairs_mut()
            .append_pair("action", "wbsearchentities")
            .append_pair("search", &name)
            .append_pair("language", &self.language)
            .append_pair("uselang", &self.language)
            .append_pair("type", "item")
            .append_pair("limit", &self.search_size.to_string())
            .append_pair("format", "json")
            .append_pair("maxlag", "5");
        let key = format!("search?{}", url.query().unwrap_or_default());
        let fetch = self.api.send(&key, &url, API_ACCEPT, &api_inspect).await?;
        let Body::Found(b) = &fetch.body else {
            return Err(warning(WarningKind::Status(404)));
        };
        let hits = model::search(b).map_err(malformed)?;
        self.api.fetcher.remember(&key, &fetch).await;
        Ok(Fetched::new(hits, &fetch))
    }

    async fn query(&self, key: &str, q: &str) -> Result<(Fetch, Vec<WikidataItem>), Warning> {
        let mut url = self.sparql.url.clone();
        url.query_pairs_mut().append_pair("query", q);
        let fetch = self
            .sparql
            .send(key, &url, SPARQL_ACCEPT, &|_| Inspect::Status)
            .await?;
        let Body::Found(b) = &fetch.body else {
            return Err(warning(WarningKind::Status(404)));
        };
        let items = model::items(b).map_err(malformed)?;
        Ok((fetch, items))
    }

    fn on(&self) -> Result<(), Warning> {
        if self.enabled {
            Ok(())
        } else {
            Err(warning(WarningKind::Disabled))
        }
    }
}

type Parts = (Arc<dyn Transport>, Arc<dyn Cache>, Arc<dyn Clock>);

/// An endpoint at `raw` (`http(s)`, no query), with its own bucket and gate.
fn endpoint(
    raw: &str,
    what: &str,
    config: &WikidataConfig,
    policy: FetchPolicy,
    (transport, cache, clock): Parts,
) -> Result<Endpoint, Warning> {
    let url: Url = raw
        .parse()
        .map_err(|e| warning(WarningKind::Config(format!("{what} URL: {e}"))))?;
    if !matches!(url.scheme(), "https" | "http") || url.cannot_be_a_base() || url.query().is_some()
    {
        return Err(warning(WarningKind::Config(format!(
            "{what} URL must be http(s), without a query"
        ))));
    }
    Ok(Endpoint {
        url,
        fetcher: Fetcher::new(
            Registry::Wikidata,
            transport,
            cache,
            clock,
            TokenBucket::new(config.per_minute, config.burst),
            policy,
        ),
        gate: Semaphore::new(1),
    })
}

/// `MediaWiki` API errors come with a 200: `maxlag` (the database is lagging) is retried with
/// the backoff, `ratelimited` empties the bucket first, any other code fails at once.
fn api_inspect(r: &Response) -> Inspect {
    match model::api_error(&r.body) {
        None => Inspect::Status,
        Some(code) if code == "maxlag" => Inspect::Retry(WarningKind::Wikidata(code)),
        Some(code) if code == "ratelimited" => Inspect::Busy(WarningKind::Wikidata(code)),
        Some(code) => Inspect::Fail(WarningKind::Wikidata(code)),
    }
}

fn warning(kind: WarningKind) -> Warning {
    Warning::new(Registry::Wikidata, kind)
}

fn malformed(e: String) -> Warning {
    let w = warning(WarningKind::Malformed(e));
    tracing::warn!(warning = %w, "wikidata: unexpected response");
    w
}
