//! The VIES client (ADR 0002 section 2): the EU's VAT Information Exchange System, keyless.
//!
//! [`Vies::check`] asks VIES whether a VAT number is valid, and gets the registered name and
//! address when the member state shares them. It uses the documented REST endpoint
//! `POST check-vat-number` with only `countryCode` and `vatNumber`: no requester VAT number is
//! sent, so VIES gives no consultation number.
//!
//! - The number is validated with `doewood_enrich` (the country's format and check digits)
//!   before it is sent; a malformed one is a [`WarningKind::InvalidInput`] and nothing is sent.
//! - Greece is `EL` in VIES: `GR` is accepted and sent as `EL`. Northern Ireland is `XI`;
//!   Great Britain (`GB`) left VIES in 2021 and is refused.
//! - A VIES error code ([`ViesError`]: member state unavailable, timeout, too many requests,
//!   ...) is a [`WarningKind::Vies`] warning, never an "invalid" answer, and is never cached.
//!   "Too many requests" codes empty the rate limiter and are retried with the backoff, as are
//!   `SERVICE_UNAVAILABLE` and `TIMEOUT`.
//! - A valid or invalid answer is cached for 90 days (the ADR's refresh).

mod model;

use std::sync::Arc;
use std::time::Duration;

use doewood_enrich::{IdKind, Identifier};
use url::Url;

pub use model::{VatRecord, ViesError};

use crate::cache::Cache;
use crate::clock::{Clock, SystemClock};
pub use crate::fetch::Fetched;
use crate::fetch::{Body, FetchPolicy, Fetcher, Inspect};
use crate::http::{HttpTransport, Request, Response, Transport};
use crate::limit::TokenBucket;
use crate::warning::{Registry, Warning, WarningKind};

/// The VIES REST API.
pub const DEFAULT_BASE_URL: &str = "https://ec.europa.eu/taxation_customs/vies/rest-api/";

/// VIES is a keyless registry, so it is on when nothing is configured (ADR 0002 section 7).
pub const DEFAULT_ENABLED: bool = true;

/// The VIES country codes: the 27 member states (Greece as `EL`) and Northern Ireland (`XI`).
pub const COUNTRIES: [&str; 28] = [
    "AT", "BE", "BG", "CY", "CZ", "DE", "DK", "EE", "EL", "ES", "FI", "FR", "HR", "HU", "IE", "IT",
    "LT", "LU", "LV", "MT", "NL", "PL", "PT", "RO", "SE", "SI", "SK", "XI",
];

const ACCEPT: &str = "application/json";

/// The VIES client's settings. The defaults are the production values.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ViesConfig {
    /// Whether the client sends anything (default [`DEFAULT_ENABLED`]).
    pub enabled: bool,
    /// The API root, ending in `/` (default [`DEFAULT_BASE_URL`]).
    pub base_url: String,
    /// The whole request, body included (default 20 s: VIES waits on the member state).
    pub timeout: Duration,
    /// Connecting (default 5 s).
    pub connect_timeout: Duration,
    /// Requests a minute (default 20). VIES publishes no number; it refuses with
    /// `MS_MAX_CONCURRENT_REQ` or `GLOBAL_MAX_CONCURRENT_REQ` when busy, and blocks addresses
    /// that poll hard.
    pub per_minute: u32,
    /// The bucket size (default 2).
    pub burst: u32,
    /// The largest body read (default 64 KiB; an answer is well under 2 KiB).
    pub max_body: usize,
    /// Retries after a timeout, a transport error, a 5xx, a 429, or a VIES "busy",
    /// `SERVICE_UNAVAILABLE` or `TIMEOUT` (default 2).
    pub max_retries: u32,
    /// The first backoff, doubling each retry (default 2 s).
    pub backoff: Duration,
    /// The longest `Retry-After` honoured (default 60 s); a longer one gives up.
    pub max_retry_after: Duration,
    /// How long a valid or invalid answer stays fresh (default 90 days, ADR 0002 section 2).
    pub ttl: Duration,
}

impl Default for ViesConfig {
    fn default() -> Self {
        const DAY: u64 = 24 * 60 * 60;
        Self {
            enabled: DEFAULT_ENABLED,
            base_url: DEFAULT_BASE_URL.to_owned(),
            timeout: Duration::from_secs(20),
            connect_timeout: Duration::from_secs(5),
            per_minute: 20,
            burst: 2,
            max_body: 64 * 1024,
            max_retries: 2,
            backoff: Duration::from_secs(2),
            max_retry_after: Duration::from_mins(1),
            ttl: Duration::from_secs(90 * DAY),
        }
    }
}

/// The VIES client. Cheap to share (`Arc`); its rate limiter is shared by every caller.
#[derive(Debug)]
pub struct Vies {
    enabled: bool,
    url: Url,
    fetcher: Fetcher,
}

impl Vies {
    /// A client on the real network and clock, caching in `cache`.
    pub fn new(config: &ViesConfig, cache: Arc<dyn Cache>) -> Result<Self, Warning> {
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
        config: &ViesConfig,
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
        let url = base
            .join("check-vat-number")
            .map_err(|e| warning(WarningKind::Config(format!("URL: {e}"))))?;
        let policy = FetchPolicy {
            max_body: config.max_body,
            max_retries: config.max_retries,
            backoff: config.backoff,
            max_retry_after: config.max_retry_after,
            ttl_found: config.ttl,
            // A 404 is not an answer about the number and is never cached.
            ttl_missing: Duration::ZERO,
        };
        Ok(Self {
            enabled: config.enabled,
            url,
            fetcher: Fetcher::new(
                Registry::Vies,
                transport,
                cache,
                clock,
                TokenBucket::new(config.per_minute, config.burst),
                policy,
            ),
        })
    }

    /// Whether `number` is a valid VAT number of `country` (`GR` or `EL` for Greece, `XI` for
    /// Northern Ireland), with the registered name and address when VIES gives them.
    /// `number` may carry the country prefix and spaces, dots or dashes.
    ///
    /// `Ok` is VIES's answer, valid or invalid (cached for 90 days). `Err` means there is no
    /// answer: the input was refused before sending, or VIES or the member state could not
    /// answer ([`WarningKind::Vies`] and the transport warnings); it says nothing about the
    /// number and is never cached.
    pub async fn check(&self, country: &str, number: &str) -> Result<Fetched<VatRecord>, Warning> {
        if !self.enabled {
            return Err(warning(WarningKind::Disabled));
        }
        let vat = vies_vat(country, number).map_err(|e| warning(WarningKind::InvalidInput(e)))?;
        let (prefix, body) = vat.value.split_at(2);
        let payload = serde_json::json!({"countryCode": prefix, "vatNumber": body});
        let payload = serde_json::to_vec(&payload)
            .map_err(|e| warning(WarningKind::Config(e.to_string())))?;
        let key = format!("check-vat-number/{}", vat.value);
        let request = Request::post_json(&self.url, ACCEPT, &payload);
        let fetch = self.fetcher.send(&key, &request, &inspect).await?;
        let Body::Found(b) = &fetch.body else {
            return Err(self.failed(WarningKind::Status(404)));
        };
        let record = model::check(b, &vat).map_err(|k| self.failed(k))?;
        self.fetcher.remember(&key, &fetch).await;
        Ok(Fetched::new(record, &fetch))
    }

    fn failed(&self, kind: WarningKind) -> Warning {
        let w = self.fetcher.warning(kind);
        tracing::warn!(warning = %w, "vies: unexpected response");
        w
    }
}

/// The VIES country code of `country`: a member state or `XI`, with `GR` read as `EL`.
pub fn vies_country(country: &str) -> Option<&'static str> {
    let c = country.trim().to_ascii_uppercase();
    let c = if c == "GR" { "EL" } else { c.as_str() };
    COUNTRIES.iter().copied().find(|x| *x == c)
}

/// `number` of `country` as a validated VAT identifier with its VIES prefix (`EL...`), or why
/// it is refused.
fn vies_vat(country: &str, number: &str) -> Result<Identifier, String> {
    let Some(prefix) = vies_country(country) else {
        return Err(match country.trim().to_ascii_uppercase().as_str() {
            "GB" => "GB is not in VIES since 2021 (Northern Ireland is XI)".to_owned(),
            c => format!(
                "not a VIES country: {:?}",
                c.chars().take(8).collect::<String>()
            ),
        });
    };
    let compact: String = number
        .chars()
        .filter(char::is_ascii_alphanumeric)
        .map(|c| c.to_ascii_uppercase())
        .collect();
    let parse = |body: &str| Identifier::parse(IdKind::Vat, &format!("{prefix}{body}"));
    // The prefix as written (`GR` too, for Greece), dropped when what is left is a number.
    let prefixes: &[&str] = if prefix == "EL" {
        &["EL", "GR"]
    } else {
        &[prefix]
    };
    prefixes
        .iter()
        .filter_map(|p| compact.strip_prefix(p))
        .find_map(&parse)
        .or_else(|| parse(&compact))
        .ok_or_else(|| format!("not a valid {prefix} VAT number"))
}

/// VIES errors come in the body, with a 4xx, a 5xx or even a 200.
fn inspect(r: &Response) -> Inspect {
    match model::error(&r.body) {
        None => Inspect::Status,
        Some(e) if e.is_busy() => Inspect::Busy(WarningKind::Vies(e)),
        Some(e @ (ViesError::ServiceUnavailable | ViesError::Timeout)) => {
            Inspect::Retry(WarningKind::Vies(e))
        }
        Some(e) => Inspect::Fail(WarningKind::Vies(e)),
    }
}

fn warning(kind: WarningKind) -> Warning {
    Warning::new(Registry::Vies, kind)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn countries() {
        assert_eq!(vies_country("GR"), Some("EL"));
        assert_eq!(vies_country(" el "), Some("EL"));
        assert_eq!(vies_country("xi"), Some("XI"));
        assert_eq!(vies_country("DE"), Some("DE"));
        for c in ["GB", "RS", "CH", "NO", "", "DEU", "E"] {
            assert_eq!(vies_country(c), None, "{c}");
        }
    }

    #[test]
    fn numbers_with_and_without_prefix() {
        let id = |c, n| vies_vat(c, n).map(|i| i.value);
        assert_eq!(id("GR", "094571238"), Ok("EL094571238".to_owned()));
        assert_eq!(id("EL", "EL 094 571 238"), Ok("EL094571238".to_owned()));
        assert_eq!(id("GR", "gr094571238"), Ok("EL094571238".to_owned()));
        assert!(id("GB", "094571238").unwrap_err().contains("2021"));
        assert!(id("EL", "09457123").is_err());
        assert!(id("EL", "").is_err());
        // A placeholder is refused like a typo.
        assert!(id("EL", "123456789").is_err());
    }
}
