//! The registry clients and the web client of one enrichment pass, built from the runtime
//! settings (ADR 0002 section 7) over fixed templates (the production endpoints, or a test's
//! mock servers).
//!
//! Settings are read on every pass; the clients are rebuilt only when what they are built
//! from changes, so their rate limiters, pauses and APR's downloaded set survive between
//! passes. A registry that is switched off, or Companies House without a key, has no client
//! and is never asked.

use std::sync::Arc;

use doewood_registry::apr::{Apr, AprConfig};
use doewood_registry::companies_house::{CompaniesHouse, CompaniesHouseConfig};
use doewood_registry::edgar::{Edgar, EdgarConfig};
use doewood_registry::favicon::{
    FaviconOptions, FaviconResolver, GuardedClient, SiteOrigin, StoreIconCache,
};
use doewood_registry::gleif::{Gleif, GleifConfig};
use doewood_registry::vies::{Vies, ViesConfig};
use doewood_registry::wikidata::{Wikidata, WikidataConfig};
use doewood_registry::{ApiKey, Cache, Registry, StoreCache, SystemClock, Warning};
use doewood_store::EnrichmentConfig;
use url::Url;

use super::CacheSource;

/// Builds the guarded HTTP client for websites and favicons, for an egress proxy (or none).
/// Production: the system resolver and the public-only policy; tests inject their own.
pub type WebClientFactory =
    Arc<dyn Fn(Option<&Url>) -> Result<GuardedClient, String> + Send + Sync>;

/// What the clients are built from, apart from the runtime settings: the endpoints, rate
/// limits and timeouts. The defaults are the production values.
#[derive(Clone, Debug, Default)]
pub struct Templates {
    /// GLEIF.
    pub gleif: GleifConfig,
    /// VIES.
    pub vies: ViesConfig,
    /// Wikidata.
    pub wikidata: WikidataConfig,
    /// SEC EDGAR.
    pub edgar: EdgarConfig,
    /// APR.
    pub apr: AprConfig,
    /// Companies House (its key comes from the settings).
    pub companies_house: CompaniesHouseConfig,
    /// Where websites are reached (`https` on the default port in production).
    pub origin: SiteOrigin,
    /// The favicon resolver's tuning (its `origin` is set from [`Templates::origin`]).
    pub favicon: FaviconOptions,
}

/// The client configurations one set of settings gives.
#[derive(Clone, Debug, PartialEq, Eq)]
struct Built {
    gleif: Option<GleifConfig>,
    vies: Option<ViesConfig>,
    wikidata: Option<WikidataConfig>,
    edgar: Option<EdgarConfig>,
    apr: Option<AprConfig>,
    companies_house: Option<CompaniesHouseConfig>,
    proxy: Option<String>,
}

/// The clients of a pass. `None`: switched off (or not configured); never asked.
#[derive(Clone, Default)]
pub struct Clients {
    /// GLEIF.
    pub gleif: Option<Arc<Gleif>>,
    /// VIES.
    pub vies: Option<Arc<Vies>>,
    /// Wikidata.
    pub wikidata: Option<Arc<Wikidata>>,
    /// SEC EDGAR.
    pub edgar: Option<Arc<Edgar>>,
    /// APR.
    pub apr: Option<Arc<Apr>>,
    /// Companies House.
    pub companies_house: Option<Arc<CompaniesHouse>>,
    /// The guarded client for websites; `None` when it cannot be built (a refused proxy):
    /// then no website is fetched, rather than going out directly.
    pub web: Option<GuardedClient>,
    /// The favicon resolver over the same client.
    pub favicons: Option<Arc<FaviconResolver>>,
    /// Ask the public favicon fallback service.
    pub favicon_fallback: bool,
    /// EDGAR's daily index feed is on: new filings make a company due, so EDGAR's own
    /// refresh is the 30 days of the other registries instead of a day.
    pub edgar_fed: bool,
    /// Where websites are reached.
    pub origin: SiteOrigin,
}

impl std::fmt::Debug for Clients {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Clients")
            .field("on", &self.enabled())
            .field("web", &self.web.is_some())
            .finish_non_exhaustive()
    }
}

impl Clients {
    /// The registries that have a client.
    pub fn enabled(&self) -> Vec<Registry> {
        let mut out = Vec::new();
        for (r, on) in [
            (Registry::Apr, self.apr.is_some()),
            (Registry::CompaniesHouse, self.companies_house.is_some()),
            (Registry::Edgar, self.edgar.is_some()),
            (Registry::Vies, self.vies.is_some()),
            (Registry::Gleif, self.gleif.is_some()),
            (Registry::Wikidata, self.wikidata.is_some()),
        ] {
            if on {
                out.push(r);
            }
        }
        out
    }

    /// Whether `registry` has a client.
    pub fn has(&self, registry: Registry) -> bool {
        self.enabled().contains(&registry)
    }
}

/// Keeps the clients of the last settings, and rebuilds them when the settings change.
pub(crate) struct ClientSet {
    templates: Templates,
    web: WebClientFactory,
    registry_cache: Arc<dyn Cache>,
    icon_cache: Arc<dyn Cache>,
    current: Option<(Built, Clients)>,
}

impl ClientSet {
    pub(crate) fn new(templates: Templates, web: WebClientFactory, caches: &CacheSource) -> Self {
        let source = Arc::clone(caches);
        let icons = Arc::clone(caches);
        Self {
            templates,
            web,
            registry_cache: Arc::new(StoreCache::new(Arc::new(move || source()))),
            icon_cache: Arc::new(StoreIconCache::new(Arc::new(move || icons()))),
            current: None,
        }
    }

    /// The clients for `settings`, and the warnings of clients that could not be built.
    pub(crate) fn clients(&mut self, settings: &EnrichmentConfig) -> (Clients, Vec<Warning>) {
        let built = self.configs(settings);
        if let Some((b, c)) = &self.current
            && *b == built
        {
            let mut c = c.clone();
            c.favicon_fallback = settings.favicon.fallback;
            c.edgar_fed = settings.feeds.edgar && settings.edgar;
            return (c, Vec::new());
        }
        let mut warnings = Vec::new();
        let cache = &self.registry_cache;
        let gleif = build(&mut warnings, built.gleif.as_ref(), |c| {
            Gleif::new(c, Arc::clone(cache))
        });
        let vies = build(&mut warnings, built.vies.as_ref(), |c| {
            Vies::new(c, Arc::clone(cache))
        });
        let wikidata = build(&mut warnings, built.wikidata.as_ref(), |c| {
            Wikidata::new(c, Arc::clone(cache))
        });
        let edgar = build(&mut warnings, built.edgar.as_ref(), |c| {
            Edgar::new(c, Arc::clone(cache))
        });
        let apr = build(&mut warnings, built.apr.as_ref(), |c| {
            Apr::new(c, Arc::clone(cache))
        });
        let companies_house = build(&mut warnings, built.companies_house.as_ref(), |c| {
            CompaniesHouse::new(c, Arc::clone(cache))
        });
        let proxy = built.proxy.as_deref().map(Url::parse).transpose();
        let web = if let Ok(p) = proxy {
            (self.web)(p.as_ref())
                .map_err(|e| {
                    tracing::warn!(error = %e, "web client not built; no website or icon is fetched");
                })
                .ok()
        } else {
            tracing::warn!("favicon.egress_proxy is not a URL; no website or icon is fetched");
            None
        };
        let favicons = web.as_ref().map(|client| {
            Arc::new(FaviconResolver::new(
                client.clone(),
                Arc::clone(&self.icon_cache),
                Arc::new(SystemClock::default()),
                FaviconOptions {
                    origin: self.templates.origin,
                    ..self.templates.favicon.clone()
                },
            ))
        });
        let clients = Clients {
            gleif,
            vies,
            wikidata,
            edgar,
            apr,
            companies_house,
            web,
            favicons,
            favicon_fallback: settings.favicon.fallback,
            edgar_fed: settings.feeds.edgar && settings.edgar,
            origin: self.templates.origin,
        };
        self.current = Some((built, clients.clone()));
        (clients, warnings)
    }

    fn configs(&self, s: &EnrichmentConfig) -> Built {
        let t = &self.templates;
        let companies_house = match (&s.companies_house_key, s.companies_house) {
            (Some(key), true) => ApiKey::new(key.expose())
                .ok()
                .map(|k| CompaniesHouseConfig {
                    enabled: true,
                    api_key: Some(k),
                    ..t.companies_house.clone()
                }),
            _ => None,
        };
        Built {
            gleif: s.gleif.then(|| GleifConfig {
                enabled: true,
                ..t.gleif.clone()
            }),
            vies: s.vies.then(|| ViesConfig {
                enabled: true,
                ..t.vies.clone()
            }),
            wikidata: s.wikidata.then(|| WikidataConfig {
                enabled: true,
                ..t.wikidata.clone()
            }),
            edgar: s.edgar.then(|| EdgarConfig {
                enabled: true,
                contact: s.edgar_contact.clone(),
                ..t.edgar.clone()
            }),
            apr: s.apr.then(|| AprConfig {
                enabled: true,
                extra_roots_pem: s
                    .apr_extra_roots
                    .as_ref()
                    .map(|r| r.expose().to_owned())
                    .or_else(|| t.apr.extra_roots_pem.clone()),
                ..t.apr.clone()
            }),
            companies_house,
            proxy: s.favicon.egress_proxy.clone(),
        }
    }
}

fn build<C, T>(
    warnings: &mut Vec<Warning>,
    config: Option<&C>,
    make: impl FnOnce(&C) -> Result<T, Warning>,
) -> Option<Arc<T>> {
    match make(config?) {
        Ok(c) => Some(Arc::new(c)),
        Err(w) => {
            warnings.push(w);
            None
        }
    }
}
