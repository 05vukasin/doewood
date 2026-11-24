//! The favicon resolver behind `GET /icons/{domain}` (ADR 0002 section 5).
//!
//! doewood fetches and serves company icons itself, so third parties never learn which
//! companies a user looks at: the browser only ever talks to doewood.
//!
//! 1. **The site first:** `https://{domain}/favicon.ico`, then the homepage (at most
//!    [`MAX_PAGE_BYTES`] read) for at most [`MAX_LINKS`] `<link rel~=icon>` hrefs ([`html`]).
//! 2. **An optional fallback,** a public favicon service ([`Fallback`]), off unless the caller
//!    passes one. Its placeholder (the "globe" it answers for unknown sites) is learned by
//!    asking it for an RFC 2606 `.invalid` domain ([`GLOBE_PROBE_DOMAIN`]); an answer with the
//!    same bytes is a miss.
//! 3. Every request, the fallback's included, goes through the SSRF guard ([`guard`]).
//! 4. Every icon passes the content checks ([`content`]).
//! 5. Answers are cached ([`Cache`]): a hit 30 days, a miss 6 hours. The time budgets are 6 s
//!    for the site, 5 s for the fallback, 12 s in total.

pub mod addr;
pub mod content;
pub mod domain;
pub mod guard;
pub mod html;

use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use time::OffsetDateTime;
use tokio::sync::Semaphore;
use tracing::debug;
use url::Url;

pub use addr::{AddressPolicy, PublicOnly, is_public};
pub use content::{IconFormat, MAX_ICON_BYTES, Rejection};
pub use domain::{Domain, InvalidDomain};
pub use guard::{
    DnsResolver, Fetched, GuardError, GuardOptions, GuardedClient, MAX_REDIRECTS, Oversize,
    Refusal, SystemDns,
};

use crate::{Cache, CacheEntry, Cached, Clock};

/// The most homepage bytes read (200 KB); the rest is not read.
pub const MAX_PAGE_BYTES: usize = 200 * 1024;
/// The most `<link rel~=icon>` hrefs followed.
pub const MAX_LINKS: usize = 2;
/// The domain the fallback service is asked for to learn its placeholder (RFC 2606: it
/// never exists).
pub const GLOBE_PROBE_DOMAIN: &str = "doewood-globe-check.invalid";

const ICON_ACCEPT: &str = "image/avif,image/webp,image/png,image/x-icon,image/*;q=0.8";
const PAGE_ACCEPT: &str = "text/html,application/xhtml+xml;q=0.9,*/*;q=0.5";

/// A public favicon service: a URL template with `{domain}` in it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Fallback {
    template: String,
}

impl Fallback {
    /// `DuckDuckGo`'s icon service, the one doewood uses when the fallback is on.
    pub const DEFAULT_TEMPLATE: &str = "https://icons.duckduckgo.com/ip3/{domain}.ico";

    /// A service from `template`, which must contain `{domain}` and give an `http` or
    /// `https` URL.
    pub fn new(template: &str) -> Result<Self, String> {
        if !template.contains("{domain}") {
            return Err("the fallback template must contain {domain}".to_owned());
        }
        let f = Self {
            template: template.to_owned(),
        };
        match f.url(GLOBE_PROBE_DOMAIN) {
            Some(u) if matches!(u.scheme(), "http" | "https") => Ok(f),
            _ => Err("the fallback template must give an http(s) URL".to_owned()),
        }
    }

    /// [`Fallback::DEFAULT_TEMPLATE`].
    pub fn default_service() -> Self {
        Self {
            template: Self::DEFAULT_TEMPLATE.to_owned(),
        }
    }

    /// The service's URL for `domain` (already validated: only letters, digits, `-`, `.`).
    fn url(&self, domain: &str) -> Option<Url> {
        Url::parse(&self.template.replace("{domain}", domain)).ok()
    }
}

/// Where the site of a domain is reached. Production: `https` on the default port.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SiteOrigin {
    /// `https` (production) or `http`.
    pub scheme: &'static str,
    /// A port other than the scheme's default (tests only: the policy must allow it).
    pub port: Option<u16>,
}

impl Default for SiteOrigin {
    fn default() -> Self {
        Self {
            scheme: "https",
            port: None,
        }
    }
}

/// The resolver's tuning; the defaults are ADR 0002's.
#[derive(Clone, Debug)]
pub struct FaviconOptions {
    /// Time for the site (favicon.ico, the homepage and its links): 6 s.
    pub site_budget: Duration,
    /// Time for the fallback service: 5 s.
    pub fallback_budget: Duration,
    /// Time for one lookup in all: 12 s.
    pub total_budget: Duration,
    /// How long a hit is kept: 30 days.
    pub hit_ttl: Duration,
    /// How long a miss is kept: 6 hours.
    pub miss_ttl: Duration,
    /// How long the fallback's placeholder is kept before it is learned again: 1 day.
    pub globe_ttl: Duration,
    /// Lookups fetching at the same time; more are refused ([`ResolveError::Busy`]): 16.
    pub max_in_flight: usize,
    /// Where sites are reached.
    pub origin: SiteOrigin,
}

impl Default for FaviconOptions {
    fn default() -> Self {
        Self {
            site_budget: Duration::from_secs(6),
            fallback_budget: Duration::from_secs(5),
            total_budget: Duration::from_secs(12),
            hit_ttl: Duration::from_hours(30 * 24),
            miss_ttl: Duration::from_hours(6),
            globe_ttl: Duration::from_hours(24),
            max_in_flight: 16,
            origin: SiteOrigin::default(),
        }
    }
}

/// An icon that passed the checks.
#[derive(Clone, PartialEq, Eq)]
pub struct Icon {
    /// Its format, from the magic bytes.
    pub format: IconFormat,
    /// The bytes, as fetched.
    pub bytes: Vec<u8>,
}

impl fmt::Debug for Icon {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Icon")
            .field("format", &self.format)
            .field("len", &self.bytes.len())
            .finish()
    }
}

/// Why a lookup gave no answer at all (a missing icon is [`None`], not an error).
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum ResolveError {
    /// The domain is not valid.
    #[error(transparent)]
    InvalidDomain(#[from] InvalidDomain),
    /// Too many lookups are fetching already.
    #[error("too many icon lookups at once")]
    Busy,
}

/// Resolves, checks and caches favicons.
pub struct FaviconResolver {
    client: GuardedClient,
    cache: Arc<dyn Cache>,
    clock: Arc<dyn Clock>,
    options: FaviconOptions,
    in_flight: Semaphore,
    /// Each fallback template's placeholder, with when it was learned.
    globes: Mutex<HashMap<String, (OffsetDateTime, Vec<u8>)>>,
}

impl fmt::Debug for FaviconResolver {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FaviconResolver")
            .field("options", &self.options)
            .finish_non_exhaustive()
    }
}

impl FaviconResolver {
    /// A resolver fetching through `client`, caching in `cache`.
    pub fn new(
        client: GuardedClient,
        cache: Arc<dyn Cache>,
        clock: Arc<dyn Clock>,
        options: FaviconOptions,
    ) -> Self {
        let in_flight = Semaphore::new(options.max_in_flight.max(1));
        Self {
            client,
            cache,
            clock,
            options,
            in_flight,
            globes: Mutex::new(HashMap::new()),
        }
    }

    /// The icon of `domain`, from the cache or fetched now; `None` if there is none. With
    /// `fallback`, the service is asked when the site has no usable icon.
    pub async fn resolve(
        &self,
        domain: &str,
        fallback: Option<&Fallback>,
    ) -> Result<Option<Icon>, ResolveError> {
        let domain = Domain::parse(domain)?;
        let key = format!("favicon:{domain}");
        if let Some(cached) = self.cached(&key).await {
            return Ok(cached);
        }
        let Ok(_permit) = self.in_flight.try_acquire() else {
            return Err(ResolveError::Busy);
        };
        let lookup = async {
            let site = tokio::time::timeout(self.options.site_budget, self.site_icon(&domain))
                .await
                .unwrap_or_else(|_| {
                    debug!(%domain, "favicon: the site's time budget ran out");
                    None
                });
            if site.is_some() {
                return site;
            }
            let fallback = fallback?;
            tokio::time::timeout(
                self.options.fallback_budget,
                self.fallback_icon(fallback, &domain),
            )
            .await
            .unwrap_or_else(|_| {
                debug!(%domain, "favicon: the fallback's time budget ran out");
                None
            })
        };
        let icon = tokio::time::timeout(self.options.total_budget, lookup)
            .await
            .ok()
            .flatten();
        let value = match &icon {
            Some(i) => Cached::Found(i.bytes.clone()),
            None => Cached::NotFound,
        };
        self.cache
            .put(
                &key,
                CacheEntry {
                    value,
                    fetched_at: self.clock.now_utc(),
                },
            )
            .await;
        Ok(icon)
    }

    /// A fresh cached answer: `Some(Some(icon))` a hit, `Some(None)` a miss, `None` nothing
    /// fresh.
    async fn cached(&self, key: &str) -> Option<Option<Icon>> {
        let entry = self.cache.get(key).await?;
        let age = self.clock.now_utc() - entry.fetched_at;
        match entry.value {
            Cached::Found(bytes) if age < self.options.hit_ttl => {
                // The bytes passed the checks when they were stored; the type is sniffed again.
                let format = content::sniff(&bytes)?;
                Some(Some(Icon { format, bytes }))
            }
            Cached::NotFound if age < self.options.miss_ttl => Some(None),
            _ => None,
        }
    }

    fn site_url(&self, domain: &Domain, path: &str) -> Option<Url> {
        let o = self.options.origin;
        let port = o.port.map(|p| format!(":{p}")).unwrap_or_default();
        Url::parse(&format!("{}://{domain}{port}{path}", o.scheme)).ok()
    }

    async fn site_icon(&self, domain: &Domain) -> Option<Icon> {
        if let Some(icon) = self
            .fetch_icon(self.site_url(domain, "/favicon.ico")?)
            .await
        {
            return Some(icon);
        }
        let page = match self
            .client
            .get(
                self.site_url(domain, "/")?,
                PAGE_ACCEPT,
                MAX_PAGE_BYTES,
                Oversize::Truncate,
            )
            .await
        {
            Ok(p) if p.status == 200 => p,
            Ok(p) => {
                debug!(%domain, status = p.status, "favicon: no homepage");
                return None;
            }
            Err(e) => {
                debug!(%domain, error = %e, "favicon: no homepage");
                return None;
            }
        };
        for link in html::icon_links(&page.body, &page.url, MAX_LINKS) {
            if let Some(icon) = self.fetch_icon(link).await {
                return Some(icon);
            }
        }
        None
    }

    /// `GET url` as an icon: a 200 whose bytes pass the content checks.
    async fn fetch_icon(&self, url: Url) -> Option<Icon> {
        match self
            .client
            .get(url, ICON_ACCEPT, MAX_ICON_BYTES, Oversize::Refuse)
            .await
        {
            Ok(f) if f.status == 200 => match content::check(&f.body, f.content_type.as_deref()) {
                Ok(format) => Some(Icon {
                    format,
                    bytes: f.body,
                }),
                Err(r) => {
                    debug!(url = %f.url, reason = %r, "favicon: icon refused");
                    None
                }
            },
            Ok(f) => {
                debug!(url = %f.url, status = f.status, "favicon: no icon");
                None
            }
            Err(e) => {
                debug!(error = %e, "favicon: icon not fetched");
                None
            }
        }
    }

    async fn fallback_icon(&self, fallback: &Fallback, domain: &Domain) -> Option<Icon> {
        let globe = self.globe(fallback).await?;
        let icon = self.fetch_icon(fallback.url(domain.as_str())?).await?;
        if icon.bytes == globe {
            debug!(%domain, "favicon: the fallback answered its placeholder");
            return None;
        }
        Some(icon)
    }

    /// The fallback's placeholder: what it answers for [`GLOBE_PROBE_DOMAIN`] (any status).
    /// `None` when it cannot be learned; the fallback is then not used, since its answers
    /// cannot be told from the placeholder.
    async fn globe(&self, fallback: &Fallback) -> Option<Vec<u8>> {
        let now = self.clock.now_utc();
        if let Some((at, bytes)) = self
            .globes
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&fallback.template)
            && now - *at < self.options.globe_ttl
        {
            return Some(bytes.clone());
        }
        let probe = fallback.url(GLOBE_PROBE_DOMAIN)?;
        match self
            .client
            .get(probe, ICON_ACCEPT, MAX_ICON_BYTES, Oversize::Refuse)
            .await
        {
            Ok(f) => {
                self.globes
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .insert(fallback.template.clone(), (now, f.body.clone()));
                Some(f.body)
            }
            Err(e) => {
                debug!(error = %e, "favicon: the fallback's placeholder could not be learned");
                None
            }
        }
    }
}
