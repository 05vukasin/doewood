//! doewood's public registry clients (ADR 0002 section 2).
//!
//! Every client here is **failure-soft**: a failure is a typed [`Warning`], never a panic or an
//! error that stops the caller. Every response is **length-capped** before it is parsed, and
//! parsed into types. Each client has its own **token-bucket rate limiter** and **backoff**
//! (429 with `Retry-After` is honoured), and every request has a **timeout**. Answers are
//! **cached** with their fetch date behind the [`Cache`] trait.
//!
//! - [`gleif`]: the GLEIF API (`api.gleif.org`): LEI records, name search, and Level 2 (direct
//!   and ultimate parents, reporting exceptions, successor entities with dates).
//! - [`vies`]: the EU's VIES (`ec.europa.eu`): whether a VAT number is valid, with the
//!   registered name and address.
//! - [`wikidata`]: Wikidata's query service (SPARQL) by LEI or Q-id: parent organization,
//!   owned by, replaced by, followed by, official website and logo, with dates; and the
//!   `wbsearchentities` name search. The lowest trust tier ([`TrustTier::Community`]).
//! - [`edgar`]: SEC EDGAR by CIK: name, former names with dates, tickers, SIC, state of
//!   incorporation and recent filings; the name and ticker lookup in the company tickers file;
//!   Exhibit 21 subsidiaries (best-effort); 8-K Item 2.01 (completed acquisitions).
//! - [`apr`]: the Serbian Business Registers Agency's open data (`openapi.apr.gov.rs`): a
//!   company by MB (name, status, legal form, founding date, municipality, activity), and a
//!   name search over the set, which is downloaded monthly and searched here.
//! - [`companies_house`]: UK Companies House (`api.company-information.service.gov.uk`), with
//!   the operator's API key: a company by number (status, previous names), its officers and
//!   people with significant control (personal data kept minimal), the name changes and
//!   merger filings of its filing history, and the name search. Skipped with no key.
//! - [`favicon`]: the favicon resolver behind `GET /icons/{domain}` (ADR 0002 section 5),
//!   with its SSRF guard and content checks.
//! - [`fetch`]: the shared request loop (limiter, retries, cap, cache) over a [`Transport`].
//! - [`limit`], [`clock`], [`cache`], [`http`]: its parts, each behind a small trait so the
//!   decision logic is tested without a network.
//!
//! Only identifiers and names are ever sent, never document content (ADR 0002, the privacy
//! rule). Identifiers are validated with [`doewood_enrich::check`] before a request is made.

pub mod apr;
pub mod cache;
pub mod clock;
pub mod companies_house;
pub mod edgar;
pub mod favicon;
pub mod fetch;
pub mod gleif;
pub mod http;
pub mod limit;
mod parse;
mod trust;
pub mod vies;
mod warning;
pub mod wikidata;

pub use cache::{Cache, CacheEntry, Cached, MemoryCache, NoCache};
pub use clock::{Clock, ManualClock, SystemClock};
pub use fetch::{Body, Fetch, FetchPolicy, Fetched, Fetcher, Inspect};
pub use http::{ApiKey, HttpTransport, Request, Response, Transport, TransportError, USER_AGENT};
pub use limit::TokenBucket;
pub use trust::TrustTier;
pub use warning::{Registry, Warning, WarningKind};
