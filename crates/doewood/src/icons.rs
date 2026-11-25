//! `GET /icons/{domain}` for the API: `doewood-registry`'s favicon resolver with the production
//! SSRF policy (public addresses and default ports only), the system resolver, and the
//! backend's `favicons` table as the cache (ADR 0002 section 5): the shared Postgres, so every
//! instance fetches an icon once, or the instance's SQLite file standalone. While the shared
//! store is not connected yet, icons are cached in memory.
//!
//! The runtime settings (`favicon.fallback`, off by default, and `favicon.egress_proxy`, set
//! with `ConfigService.SetEnrichmentSettings`) are read from the store for every lookup, so a
//! change applies without a restart. A new proxy
//! builds a new client; the cache is kept. A proxy that the guard refuses fails closed: no
//! icon is fetched, rather than going out directly.

use std::sync::{Arc, Mutex, PoisonError};

use doewood_api::{BoxFuture, IconAnswer, Icons};
use doewood_model::{Backend, CacheStore};
use doewood_registry::favicon::{
    Fallback, FaviconOptions, FaviconResolver, GuardOptions, GuardedClient, PublicOnly,
    ResolveError, StoreIconCache, SystemDns,
};
use doewood_registry::{Cache, SystemClock};
use doewood_store::{FaviconSettings, Store};
use tokio::sync::watch;
use tracing::warn;
use url::Url;

/// The icons of the API, resolved with the production policy.
pub struct FaviconIcons {
    store: Arc<Store>,
    cache: Arc<dyn Cache>,
    /// The resolver for the proxy setting it was built with.
    current: Mutex<Option<(Option<String>, Arc<FaviconResolver>)>>,
}

impl FaviconIcons {
    /// Icons cached in the backend `backend` holds (in memory while it holds none), with the
    /// settings of `store`.
    pub fn new(store: Arc<Store>, backend: watch::Receiver<Option<Arc<dyn Backend>>>) -> Self {
        let source = move || {
            backend
                .borrow()
                .as_ref()
                .map(|b| Arc::clone(b) as Arc<dyn CacheStore>)
        };
        Self {
            store,
            cache: Arc::new(StoreIconCache::new(Arc::new(source))),
            current: Mutex::new(None),
        }
    }

    fn resolver(&self, proxy: Option<String>) -> Result<Arc<FaviconResolver>, String> {
        let mut current = self.current.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some((p, r)) = current.as_ref()
            && *p == proxy
        {
            return Ok(Arc::clone(r));
        }
        let options = GuardOptions {
            proxy: proxy
                .as_deref()
                .map(Url::parse)
                .transpose()
                .map_err(|_| "favicon.egress_proxy is not a valid URL".to_owned())?,
            ..GuardOptions::default()
        };
        let client = GuardedClient::new(&options, Arc::new(SystemDns), Arc::new(PublicOnly))?;
        let resolver = Arc::new(FaviconResolver::new(
            client,
            Arc::clone(&self.cache),
            Arc::new(SystemClock::default()),
            FaviconOptions::default(),
        ));
        *current = Some((proxy, Arc::clone(&resolver)));
        Ok(resolver)
    }
}

impl Icons for FaviconIcons {
    fn icon(&self, domain: String) -> BoxFuture<'_, IconAnswer> {
        Box::pin(async move {
            let store = Arc::clone(&self.store);
            let settings = match tokio::task::spawn_blocking(move || store.favicon_settings()).await
            {
                Ok(Ok(s)) => s,
                Ok(Err(e)) => {
                    warn!(error = %e, "favicon settings unreadable; using the defaults");
                    FaviconSettings::default()
                }
                Err(e) => {
                    warn!(error = %e, "favicon settings task failed; using the defaults");
                    FaviconSettings::default()
                }
            };
            let resolver = match self.resolver(settings.egress_proxy) {
                Ok(r) => r,
                Err(e) => {
                    warn!(error = %e, "favicon client not built; no icon is fetched");
                    return IconAnswer::Missing;
                }
            };
            let fallback = settings.fallback.then(Fallback::default_service);
            match resolver.resolve(&domain, fallback.as_ref()).await {
                Ok(Some(icon)) => IconAnswer::Found {
                    content_type: icon.format.content_type(),
                    bytes: icon.bytes,
                },
                Ok(None) => IconAnswer::Missing,
                Err(ResolveError::InvalidDomain(e)) => IconAnswer::Invalid(e.to_string()),
                Err(ResolveError::Busy) => IconAnswer::Busy,
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use doewood_core::MasterKey;

    use super::*;

    fn icons() -> FaviconIcons {
        let store = Store::open_in_memory(&MasterKey::from_bytes([7; 32])).unwrap();
        let (_tx, rx) = watch::channel(None);
        FaviconIcons::new(Arc::new(store), rx)
    }

    fn proxy_in_use(icons: &FaviconIcons) -> Option<String> {
        icons
            .current
            .lock()
            .unwrap()
            .as_ref()
            .and_then(|(p, _)| p.clone())
    }

    #[tokio::test]
    async fn a_settings_change_applies_on_the_next_lookup() {
        let icons = icons();
        // `printer.local` is refused before any DNS lookup, after the client is chosen.
        assert_eq!(
            icons.icon("printer.local".to_owned()).await,
            IconAnswer::Missing
        );
        assert_eq!(proxy_in_use(&icons), None);
        // What SetEnrichmentSettings writes.
        icons
            .store
            .set_enrichment_settings(&doewood_store::EnrichmentSettingsPatch {
                favicon_fallback: Some(true),
                favicon_egress_proxy: Some(Some("http://egress.svc:3128".to_owned())),
                ..doewood_store::EnrichmentSettingsPatch::default()
            })
            .unwrap();
        assert_eq!(
            icons.icon("printer.local".to_owned()).await,
            IconAnswer::Missing
        );
        assert_eq!(
            proxy_in_use(&icons).as_deref(),
            Some("http://egress.svc:3128")
        );
        icons
            .store
            .set_enrichment_settings(&doewood_store::EnrichmentSettingsPatch {
                favicon_egress_proxy: Some(None),
                ..doewood_store::EnrichmentSettingsPatch::default()
            })
            .unwrap();
        icons.icon("printer.local".to_owned()).await;
        assert_eq!(proxy_in_use(&icons), None);
    }

    #[tokio::test]
    async fn invalid_and_special_domains_fetch_nothing() {
        let icons = icons();
        assert!(matches!(
            icons.icon("127.0.0.1".to_owned()).await,
            IconAnswer::Invalid(_)
        ));
        assert!(matches!(
            icons.icon("acme.com/x".to_owned()).await,
            IconAnswer::Invalid(_)
        ));
        // The production policy refuses special-use names before any DNS lookup.
        assert_eq!(
            icons.icon("printer.local".to_owned()).await,
            IconAnswer::Missing
        );
        assert_eq!(
            icons.icon("metadata.google.internal".to_owned()).await,
            IconAnswer::Missing
        );
    }

    #[test]
    fn a_new_proxy_builds_a_new_client() {
        let icons = icons();
        let direct = icons.resolver(None).unwrap();
        assert!(Arc::ptr_eq(&direct, &icons.resolver(None).unwrap()));
        let proxied = icons
            .resolver(Some("http://egress.svc:3128".to_owned()))
            .unwrap();
        assert!(!Arc::ptr_eq(&direct, &proxied));
        assert!(
            icons
                .resolver(Some("socks5://egress.svc:1080".to_owned()))
                .is_err()
        );
    }
}
