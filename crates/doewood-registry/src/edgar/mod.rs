//! The SEC EDGAR client (ADR 0002 section 2), keyless: `data.sec.gov` for the submissions
//! API, `www.sec.gov` for the company tickers file and the filing archives.
//!
//! - [`Edgar::company`]: a company by CIK: name, former names with dates, tickers, SIC,
//!   state of incorporation and the recent filings (form, dates, accession number, items,
//!   primary document). [`Company::acquisitions`] gives the 8-K filings with Item 2.01
//!   (completed acquisitions); [`Company::latest_annual_report`] the newest 10-K.
//! - [`Edgar::ticker`] and [`Edgar::search`]: the CIK of a ticker or a name, from the company
//!   tickers file. The file is fetched whole and searched here: the name never leaves the
//!   instance.
//! - [`Edgar::exhibit21`]: the subsidiaries listed in a filing's Exhibit 21, best-effort.
//!   [`Edgar::subsidiaries`] does it for the newest 10-K of a CIK.
//! - [`Edgar::acquisition_text`]: the text of an 8-K from "Item 2.01" on, capped, best-effort.
//!
//! Every answer is [`TrustTier::Official`] (SEC filings). A CIK, an accession number and a
//! document name are validated before anything is sent: nothing else reaches a URL.
//!
//! **Fair access.** The SEC allows at most 10 requests a second per user and asks for a
//! `User-Agent` that names the requester and a contact email. Every request of this client
//! (all hosts and documents) shares one token bucket, 5 a second by default. The contact is
//! the operator's ([`EdgarConfig::contact`], ADR 0002 section 7); with none, the generic
//! [`crate::USER_AGENT`] is sent. A 403 (an undeclared User-Agent, or the rate threshold) is
//! an [`EdgarRefusal`] warning, and the client then sends nothing for [`EdgarConfig::pause`].

mod exhibit;
mod html;
mod model;

use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use url::Url;

pub use exhibit::{Extraction, Subsidiary};
pub use model::{Accession, Cik, Company, EdgarRefusal, Filing, FormerName, TickerEntry};

use crate::cache::Cache;
use crate::clock::{Clock, SystemClock};
pub use crate::fetch::Fetched;
use crate::fetch::{Body, Fetch, FetchPolicy, Fetcher, Inspect};
use crate::http::{HttpTransport, Request, Response, Transport, USER_AGENT};
use crate::limit::TokenBucket;
pub use crate::trust::TrustTier;
use crate::warning::{Registry, Warning, WarningKind};

/// The submissions API's host.
pub const DEFAULT_DATA_URL: &str = "https://data.sec.gov/";

/// The host of the tickers file and the filing archives.
pub const DEFAULT_WWW_URL: &str = "https://www.sec.gov/";

/// EDGAR is keyless, so it is on when nothing is configured (ADR 0002 section 7).
pub const DEFAULT_ENABLED: bool = true;

/// Every EDGAR answer's tier.
pub const TIER: TrustTier = TrustTier::Official;

const JSON: &str = "application/json";
const HTML: &str = "text/html, text/plain;q=0.9, */*;q=0.1";

/// The longest name or ticker looked up.
const MAX_NAME_CHARS: usize = 256;
/// The longest contact string.
const MAX_CONTACT_CHARS: usize = 200;

const DAY: u64 = 24 * 60 * 60;

/// The EDGAR client's settings. The defaults are the production values.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EdgarConfig {
    /// Whether the client sends anything (default [`DEFAULT_ENABLED`]).
    pub enabled: bool,
    /// The operator's contact for the SEC's `User-Agent` rule: a name and an email, such as
    /// `Example Ops ops@example.invalid` (default `None`: the generic [`USER_AGENT`]). Sent
    /// to the SEC with every request; printable ASCII, at most 200 characters.
    pub contact: Option<String>,
    /// The submissions API root (default [`DEFAULT_DATA_URL`]).
    pub data_url: String,
    /// The tickers file and archives root (default [`DEFAULT_WWW_URL`]).
    pub www_url: String,
    /// The whole request, body included (default 20 s).
    pub timeout: Duration,
    /// Connecting (default 5 s).
    pub connect_timeout: Duration,
    /// Requests a minute, all hosts and documents together (default 300: 5 a second; the SEC
    /// allows 10).
    pub per_minute: u32,
    /// The bucket size (default 2: at most 7 requests in any second).
    pub burst: u32,
    /// The largest submissions JSON (default 4 MiB).
    pub max_submissions: usize,
    /// The largest company tickers file (default 4 MiB).
    pub max_tickers: usize,
    /// The largest filing index page (default 1 MiB).
    pub max_index: usize,
    /// The largest Exhibit 21 document (default 2 MiB).
    pub max_exhibit: usize,
    /// The largest 8-K primary document (default 4 MiB).
    pub max_document: usize,
    /// Retries after a timeout, a transport error, a 5xx or a 429 (default 2).
    pub max_retries: u32,
    /// The first backoff, doubling each retry (default 1 s).
    pub backoff: Duration,
    /// The longest `Retry-After` honoured (default 60 s); a longer one gives up.
    pub max_retry_after: Duration,
    /// How long after a 403 nothing is sent (default 10 minutes).
    pub pause: Duration,
    /// How long a company's submissions stay fresh, found or not (default 1 day; ADR 0002
    /// refreshes EDGAR "when new filings appear", which needs the RSS feeds: not yet).
    pub ttl_submissions: Duration,
    /// How long the tickers file stays fresh (default 1 day).
    pub ttl_tickers: Duration,
    /// How long a filing's index and documents stay fresh (default 30 days: a filing under
    /// an accession number does not change).
    pub ttl_archive: Duration,
    /// Candidates a name search returns (default 10, at most 100).
    pub search_size: u32,
    /// The most subsidiary rows read from an Exhibit 21 (default 5,000).
    pub max_subsidiaries: usize,
    /// The most characters of the "Item 2.01" text (default 2,000).
    pub item_chars: usize,
}

impl Default for EdgarConfig {
    fn default() -> Self {
        Self {
            enabled: DEFAULT_ENABLED,
            contact: None,
            data_url: DEFAULT_DATA_URL.to_owned(),
            www_url: DEFAULT_WWW_URL.to_owned(),
            timeout: Duration::from_secs(20),
            connect_timeout: Duration::from_secs(5),
            per_minute: 300,
            burst: 2,
            max_submissions: 4 * 1024 * 1024,
            max_tickers: 4 * 1024 * 1024,
            max_index: 1024 * 1024,
            max_exhibit: 2 * 1024 * 1024,
            max_document: 4 * 1024 * 1024,
            max_retries: 2,
            backoff: Duration::from_secs(1),
            max_retry_after: Duration::from_mins(1),
            pause: Duration::from_mins(10),
            ttl_submissions: Duration::from_secs(DAY),
            ttl_tickers: Duration::from_secs(DAY),
            ttl_archive: Duration::from_secs(30 * DAY),
            search_size: 10,
            max_subsidiaries: 5000,
            item_chars: 2000,
        }
    }
}

/// The `User-Agent` sent to the SEC: the configured contact followed by the generic
/// [`USER_AGENT`], or the generic one alone when no contact is set (or it is blank).
pub fn user_agent(contact: Option<&str>) -> Result<String, Warning> {
    let Some(c) = contact.map(str::trim).filter(|c| !c.is_empty()) else {
        return Ok(USER_AGENT.to_owned());
    };
    if c.chars().count() > MAX_CONTACT_CHARS || !c.bytes().all(|b| (0x20..0x7f).contains(&b)) {
        return Err(warning(WarningKind::Config(format!(
            "the EDGAR contact must be printable ASCII, at most {MAX_CONTACT_CHARS} characters"
        ))));
    }
    Ok(format!("{c} {USER_AGENT}"))
}

/// The subsidiaries listed in a filing's Exhibit 21. **Best-effort**: the rows are a
/// heuristic reading of a free-form document (see [`Extraction`]).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Exhibit21 {
    /// The filer.
    pub cik: Cik,
    /// The filing.
    pub accession: Accession,
    /// The filing date, when known (from the submissions, in [`Edgar::subsidiaries`]).
    pub filing_date: Option<time::Date>,
    /// The exhibit's document type (`EX-21`, `EX-21.1`).
    pub exhibit_type: String,
    /// The exhibit's file name in the filing.
    pub document: String,
    /// The rows read, in the document's order.
    pub subsidiaries: Vec<Subsidiary>,
    /// How they were read.
    pub extraction: Extraction,
    /// Whether there were more rows than [`EdgarConfig::max_subsidiaries`].
    pub truncated: bool,
    /// Always [`TrustTier::Official`].
    pub tier: TrustTier,
}

/// The EDGAR client. Cheap to share (`Arc`); its limits are shared by every caller.
#[derive(Debug)]
pub struct Edgar {
    enabled: bool,
    data: Url,
    www: Url,
    search_size: usize,
    max_subsidiaries: usize,
    item_chars: usize,
    pause: Duration,
    clock: Arc<dyn Clock>,
    /// Nothing is sent before this monotonic instant (after a 403).
    paused_until: Mutex<Option<Duration>>,
    submissions: Fetcher,
    tickers: Fetcher,
    index: Fetcher,
    exhibits: Fetcher,
    documents: Fetcher,
}

impl Edgar {
    /// A client on the real network and clock, caching in `cache`, sending
    /// [`user_agent`]`(config.contact)`.
    pub fn new(config: &EdgarConfig, cache: Arc<dyn Cache>) -> Result<Self, Warning> {
        let agent = user_agent(config.contact.as_deref())?;
        let transport =
            HttpTransport::with_user_agent(config.timeout, config.connect_timeout, &agent)
                .map_err(|e| warning(WarningKind::Config(e)))?;
        Self::with_parts(
            config,
            Arc::new(transport),
            cache,
            Arc::new(SystemClock::default()),
        )
    }

    /// A client on the given transport (which sends its own `User-Agent`), cache and clock.
    pub fn with_parts(
        config: &EdgarConfig,
        transport: Arc<dyn Transport>,
        cache: Arc<dyn Cache>,
        clock: Arc<dyn Clock>,
    ) -> Result<Self, Warning> {
        user_agent(config.contact.as_deref())?;
        let data = root(&config.data_url, "data URL")?;
        let www = root(&config.www_url, "www URL")?;
        let bucket = Arc::new(TokenBucket::new(config.per_minute, config.burst));
        let policy = |max_body, ttl_found, ttl_missing| FetchPolicy {
            max_body,
            max_retries: config.max_retries,
            backoff: config.backoff,
            max_retry_after: config.max_retry_after,
            ttl_found,
            ttl_missing,
        };
        let parts = (transport, cache);
        let fetcher = |p: FetchPolicy| {
            Fetcher::sharing(
                Registry::Edgar,
                parts.0.clone(),
                parts.1.clone(),
                clock.clone(),
                bucket.clone(),
                p,
            )
        };
        let day = Duration::from_secs(DAY);
        Ok(Self {
            enabled: config.enabled,
            data,
            www,
            search_size: config.search_size.clamp(1, 100) as usize,
            max_subsidiaries: config.max_subsidiaries,
            item_chars: config.item_chars,
            pause: config.pause,
            paused_until: Mutex::new(None),
            submissions: fetcher(policy(
                config.max_submissions,
                config.ttl_submissions,
                config.ttl_submissions,
            )),
            tickers: fetcher(policy(
                config.max_tickers,
                config.ttl_tickers,
                Duration::ZERO,
            )),
            // A missing filing index is a Status(404) warning, not cached.
            index: fetcher(policy(config.max_index, config.ttl_archive, Duration::ZERO)),
            exhibits: fetcher(policy(
                config.max_exhibit,
                config.ttl_archive,
                Duration::ZERO,
            )),
            documents: fetcher(policy(config.max_document, config.ttl_archive, day)),
            clock,
        })
    }

    /// The company with CIK `cik` (`42`, `0000000042`, `CIK0000000042`); `None` when
    /// EDGAR has none. An invalid CIK is [`WarningKind::InvalidInput`], and nothing is sent.
    pub async fn company(&self, cik: &str) -> Result<Fetched<Option<Company>>, Warning> {
        self.on()?;
        let cik = parse_cik(cik)?;
        self.company_of(cik).await
    }

    async fn company_of(&self, cik: Cik) -> Result<Fetched<Option<Company>>, Warning> {
        let key = format!("submissions/CIK{cik}");
        let url = join(&self.data, &format!("submissions/CIK{cik}.json"))?;
        let fetch = self.send(&self.submissions, &key, &url, JSON).await?;
        let value = match &fetch.body {
            Body::NotFound => None,
            Body::Found(b) => {
                let c = model::company(b).map_err(malformed)?;
                if c.cik != cik {
                    return Err(malformed(format!("CIK {} returned for {cik}", c.cik)));
                }
                Some(c)
            }
        };
        self.submissions.remember(&key, &fetch).await;
        Ok(Fetched::new(value, &fetch))
    }

    /// The tickers file's entry for `ticker` (case-insensitive); `None` when it lists none.
    pub async fn ticker(&self, ticker: &str) -> Result<Fetched<Option<TickerEntry>>, Warning> {
        self.on()?;
        let t = ticker.trim();
        if t.is_empty()
            || t.len() > 16
            || !t
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'-'))
        {
            return Err(warning(WarningKind::InvalidInput(
                "not a ticker symbol".to_owned(),
            )));
        }
        let (fetch, entries) = self.ticker_file().await?;
        let hit = entries
            .into_iter()
            .find(|e| e.ticker.eq_ignore_ascii_case(t));
        Ok(Fetched::new(hit, &fetch))
    }

    /// The tickers file's entries whose name has every word of `name` (case and punctuation
    /// ignored), exact names first, then by CIK; at most `search_size`, one per CIK. Only
    /// companies with a listed ticker are in the file. Matching them to a company is the
    /// caller's (doewood-enrich's fuzzy score). The name is never sent.
    pub async fn search(&self, name: &str) -> Result<Fetched<Vec<TickerEntry>>, Warning> {
        self.on()?;
        if name.chars().count() > MAX_NAME_CHARS {
            return Err(warning(WarningKind::InvalidInput(format!(
                "name longer than {MAX_NAME_CHARS} characters"
            ))));
        }
        let want = model::words(name);
        if want.is_empty() {
            return Err(warning(WarningKind::InvalidInput("empty name".to_owned())));
        }
        let (fetch, entries) = self.ticker_file().await?;
        let mut hits: Vec<(bool, TickerEntry)> = entries
            .into_iter()
            .filter_map(|e| {
                let have = model::words(&e.title);
                want.iter()
                    .all(|w| have.contains(w))
                    .then(|| (have != want, e))
            })
            .collect();
        hits.sort_by(|a, b| (a.0, a.1.cik, &a.1.ticker).cmp(&(b.0, b.1.cik, &b.1.ticker)));
        let mut seen = std::collections::HashSet::new();
        let hits = hits
            .into_iter()
            .map(|(_, e)| e)
            .filter(|e| seen.insert(e.cik))
            .take(self.search_size)
            .collect();
        Ok(Fetched::new(hits, &fetch))
    }

    async fn ticker_file(&self) -> Result<(Fetch, Vec<TickerEntry>), Warning> {
        let key = "files/company_tickers.json";
        let url = join(&self.www, key)?;
        let fetch = self.send(&self.tickers, key, &url, JSON).await?;
        let Body::Found(b) = &fetch.body else {
            return Err(warning(WarningKind::Status(404)));
        };
        let entries = model::tickers(b).map_err(malformed)?;
        self.tickers.remember(key, &fetch).await;
        Ok((fetch, entries))
    }

    /// The subsidiaries in the Exhibit 21 of the newest 10-K of `cik`. `None` when there is
    /// no such company, no 10-K among the recent filings, or no Exhibit 21 in it.
    pub async fn subsidiaries(&self, cik: &str) -> Result<Fetched<Option<Exhibit21>>, Warning> {
        self.on()?;
        let cik = parse_cik(cik)?;
        let company = self.company_of(cik).await?;
        let Some(filing) = company
            .value
            .as_ref()
            .and_then(Company::latest_annual_report)
        else {
            return Ok(Fetched {
                value: None,
                fetched_at: company.fetched_at,
            });
        };
        let mut ex = self.exhibit21(cik, &filing.accession).await?;
        if let Some(e) = &mut ex.value {
            e.filing_date = filing.filing_date;
        }
        Ok(ex)
    }

    /// The subsidiaries in the Exhibit 21 (`EX-21`, `EX-21.1`, ...) of filing `accession` of
    /// `cik`, read from the filing index (`<accession>-index.htm`). `None` when the index
    /// lists no Exhibit 21. A missing index or exhibit is a `Status(404)` warning.
    pub async fn exhibit21(
        &self,
        cik: Cik,
        accession: &Accession,
    ) -> Result<Fetched<Option<Exhibit21>>, Warning> {
        self.on()?;
        let folder = format!("Archives/edgar/data/{}/{}", cik.value(), accession.plain());
        let key = format!("{folder}/{accession}-index.htm");
        let url = join(&self.www, &key)?;
        let fetch = self.send(&self.index, &key, &url, HTML).await?;
        let Body::Found(b) = &fetch.body else {
            return Err(warning(WarningKind::Status(404)));
        };
        let docs = exhibit::index_documents(b);
        self.index.remember(&key, &fetch).await;
        let Some((kind, file)) = docs.into_iter().find(|(k, _)| exhibit::is_ex21(k)) else {
            return Ok(Fetched::new(None, &fetch));
        };
        let key = format!("{folder}/{file}");
        let url = join(&self.www, &key)?;
        let fetch = self.send(&self.exhibits, &key, &url, HTML).await?;
        let Body::Found(b) = &fetch.body else {
            return Err(warning(WarningKind::Status(404)));
        };
        let (subsidiaries, extraction, truncated) = exhibit::subsidiaries(b, self.max_subsidiaries);
        self.exhibits.remember(&key, &fetch).await;
        Ok(Fetched::new(
            Some(Exhibit21 {
                cik,
                accession: accession.clone(),
                filing_date: None,
                exhibit_type: kind,
                document: file,
                subsidiaries,
                extraction,
                truncated,
                tier: TIER,
            }),
            &fetch,
        ))
    }

    /// The text of `filing`'s primary document from "Item 2.01" on, at most
    /// [`EdgarConfig::item_chars`] characters, cut before the next item. Best-effort. `None`
    /// when the filing has no primary document, the document is missing, or it has no
    /// "Item 2.01".
    pub async fn acquisition_text(
        &self,
        cik: Cik,
        filing: &Filing,
    ) -> Result<Fetched<Option<String>>, Warning> {
        self.on()?;
        let Some(file) = filing
            .primary_document
            .as_deref()
            .and_then(model::safe_file)
        else {
            return Ok(Fetched {
                value: None,
                fetched_at: self.clock.now_utc(),
            });
        };
        let key = format!(
            "Archives/edgar/data/{}/{}/{file}",
            cik.value(),
            filing.accession.plain()
        );
        let url = join(&self.www, &key)?;
        let fetch = self.send(&self.documents, &key, &url, HTML).await?;
        let value = match &fetch.body {
            Body::NotFound => None,
            Body::Found(b) => exhibit::item_201(b, self.item_chars),
        };
        self.documents.remember(&key, &fetch).await;
        Ok(Fetched::new(value, &fetch))
    }

    /// The cached answer, else (unless paused after a 403) the request.
    async fn send(
        &self,
        fetcher: &Fetcher,
        key: &str,
        url: &Url,
        accept: &str,
    ) -> Result<Fetch, Warning> {
        if let Some(hit) = fetcher.cached(key).await {
            return Ok(hit);
        }
        let now = self.clock.monotonic();
        let until = *self
            .paused_until
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if let Some(until) = until.filter(|u| *u > now) {
            return Err(warning(WarningKind::RateLimited {
                retry_after: Some(until.saturating_sub(now)),
            }));
        }
        let result = fetcher
            .send(key, &Request::get(url, accept), &inspect)
            .await;
        if let Err(Warning {
            kind: WarningKind::Edgar(_),
            ..
        }) = &result
        {
            let until = self.clock.monotonic() + self.pause;
            *self
                .paused_until
                .lock()
                .unwrap_or_else(PoisonError::into_inner) = Some(until);
        }
        result
    }

    fn on(&self) -> Result<(), Warning> {
        if self.enabled {
            Ok(())
        } else {
            Err(warning(WarningKind::Disabled))
        }
    }
}

/// A 403 is the SEC refusing the client (an undeclared User-Agent or the rate threshold):
/// retrying makes it worse, so it fails at once and the client pauses.
fn inspect(r: &Response) -> Inspect {
    if r.status == 403 {
        Inspect::Fail(WarningKind::Edgar(EdgarRefusal::from_body(&r.body)))
    } else {
        Inspect::Status
    }
}

fn parse_cik(cik: &str) -> Result<Cik, Warning> {
    Cik::parse(cik).ok_or_else(|| {
        warning(WarningKind::InvalidInput(
            "not a CIK (1 to 10 digits, not zero)".to_owned(),
        ))
    })
}

/// A root URL (`http(s)`, no query), ending in `/`.
fn root(raw: &str, what: &str) -> Result<Url, Warning> {
    let mut url: Url = raw
        .parse()
        .map_err(|e| warning(WarningKind::Config(format!("{what}: {e}"))))?;
    if !matches!(url.scheme(), "https" | "http") || url.cannot_be_a_base() || url.query().is_some()
    {
        return Err(warning(WarningKind::Config(format!(
            "{what} must be http(s), without a query"
        ))));
    }
    if !url.path().ends_with('/') {
        let path = format!("{}/", url.path());
        url.set_path(&path);
    }
    Ok(url)
}

/// `rel` (built from validated parts only) under `base`.
fn join(base: &Url, rel: &str) -> Result<Url, Warning> {
    base.join(rel)
        .map_err(|e| warning(WarningKind::Config(format!("URL: {e}"))))
}

fn warning(kind: WarningKind) -> Warning {
    Warning::new(Registry::Edgar, kind)
}

fn malformed(e: String) -> Warning {
    let w = warning(WarningKind::Malformed(e));
    tracing::warn!(warning = %w, "edgar: unexpected response");
    w
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn agents() {
        assert_eq!(user_agent(None).unwrap(), USER_AGENT);
        assert_eq!(user_agent(Some("  ")).unwrap(), USER_AGENT);
        let ua = user_agent(Some(" Example Ops ops@example.invalid ")).unwrap();
        assert_eq!(ua, format!("Example Ops ops@example.invalid {USER_AGENT}"));
        assert!(user_agent(Some("Ops\r\nX-Evil: 1")).is_err());
        assert!(user_agent(Some("Opé ops@example.invalid")).is_err());
        assert!(user_agent(Some(&"x".repeat(201))).is_err());
    }
}
