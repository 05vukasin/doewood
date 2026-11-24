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
//! - [`fetch`]: the shared request loop (limiter, retries, cap, cache) over a [`Transport`].
//! - [`limit`], [`clock`], [`cache`], [`http`]: its parts, each behind a small trait so the
//!   decision logic is tested without a network.
//!
//! Only identifiers and names are ever sent, never document content (ADR 0002, the privacy
//! rule). Identifiers are validated with [`doewood_enrich::check`] before a request is made.

pub mod cache;
pub mod clock;
pub mod fetch;
pub mod gleif;
pub mod http;
pub mod limit;
mod parse;
pub mod vies;
mod warning;

pub use cache::{Cache, CacheEntry, Cached, MemoryCache, NoCache};
pub use clock::{Clock, ManualClock, SystemClock};
pub use fetch::{Body, Fetch, FetchPolicy, Fetched, Fetcher, Inspect};
pub use http::{HttpTransport, Request, Response, Transport, TransportError};
pub use limit::TokenBucket;
pub use warning::{Registry, Warning, WarningKind};
