//! The typed warning every registry failure becomes.

use std::fmt;
use std::time::Duration;

use crate::edgar::EdgarRefusal;
use crate::vies::ViesError;

/// A registry client.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Registry {
    /// GLEIF, `api.gleif.org`.
    Gleif,
    /// The EU's VAT Information Exchange System (VIES), `ec.europa.eu`.
    Vies,
    /// Wikidata: the query service (`query.wikidata.org`) and the `wbsearchentities` API.
    Wikidata,
    /// SEC EDGAR: `data.sec.gov` (submissions) and `www.sec.gov` (the tickers file, the
    /// filing archives).
    Edgar,
    /// The Serbian Business Registers Agency (APR): its open data, `openapi.apr.gov.rs`.
    Apr,
    /// UK Companies House: the public data API, `api.company-information.service.gov.uk`.
    CompaniesHouse,
}

impl Registry {
    /// The short name in logs and cache keys.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Gleif => "gleif",
            Self::Vies => "vies",
            Self::Wikidata => "wikidata",
            Self::Edgar => "edgar",
            Self::Apr => "apr",
            Self::CompaniesHouse => "companies_house",
        }
    }
}

impl fmt::Display for Registry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Why a registry request gave no answer.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum WarningKind {
    /// The registry is switched off in the configuration; nothing was sent.
    #[error("disabled in the configuration")]
    Disabled,
    /// The registry's source cannot answer this kind of question (APR's open data has no
    /// PIB); nothing was sent.
    #[error("unsupported: {0}")]
    Unsupported(String),
    /// The registry needs an API key and none is configured (Companies House); nothing was
    /// sent.
    #[error("no API key configured")]
    NotConfigured,
    /// The registry refused the configured API key (401). The client then pauses; the key is
    /// never part of the warning.
    #[error("the API key was refused (HTTP 401)")]
    KeyRefused,
    /// The client configuration is unusable (for example the base URL).
    #[error("configuration: {0}")]
    Config(String),
    /// The input was refused before any request (an LEI with bad check digits, an empty name).
    #[error("invalid input: {0}")]
    InvalidInput(String),
    /// The request took longer than the timeout.
    #[error("timed out")]
    Timeout,
    /// Connection or protocol failure.
    #[error("transport: {0}")]
    Transport(String),
    /// The response body was larger than the cap; it was not parsed.
    #[error("response larger than {limit} bytes")]
    TooLarge {
        /// The cap, in bytes.
        limit: usize,
    },
    /// The registry answered 429 and the wait it asked for was too long, or retries ran out.
    #[error("rate limited (retry after {retry_after:?})")]
    RateLimited {
        /// The `Retry-After` the registry sent, when it sent seconds.
        retry_after: Option<Duration>,
    },
    /// An HTTP status other than success, 404 and 429.
    #[error("HTTP status {0}")]
    Status(u16),
    /// The body was not the JSON the client expects.
    #[error("malformed response: {0}")]
    Malformed(String),
    /// VIES answered with one of its error codes: it could not say whether the number is
    /// valid. Never an "invalid" answer, and never cached.
    #[error("VIES error {}", .0.code())]
    Vies(ViesError),
    /// The Wikidata API answered with one of its error codes (`maxlag`, `ratelimited`, ...),
    /// sanitized. Never cached.
    #[error("Wikidata API error {0}")]
    Wikidata(String),
    /// The SEC refused the request (403): an undeclared User-Agent, the request rate
    /// threshold, or another refusal. The client then pauses (see `EdgarConfig::pause`).
    #[error("SEC refused the request: {0}")]
    Edgar(EdgarRefusal),
}

/// A registry failure: which registry and why. Never fatal; the caller logs it and goes on.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error("{registry}: {kind}")]
pub struct Warning {
    /// Which registry.
    pub registry: Registry,
    /// Why.
    pub kind: WarningKind,
}

impl Warning {
    /// A warning for `registry`.
    pub fn new(registry: Registry, kind: WarningKind) -> Self {
        Self { registry, kind }
    }
}
