//! The enrichment job in the service (ADR 0002 section 6): built from the store's runtime
//! settings (read on every pass), the production registry endpoints, and the favicon guard's
//! production policy (public addresses and default ports only, the system resolver, the
//! configured egress proxy). Its counters feed `WatchProgress`.

use std::sync::Arc;

use doewood_api::{EnrichmentCounts, Progress};
use doewood_core::BootstrapConfig;
use doewood_ingest::{
    Enrichment, EnrichmentOptions, EnrichmentParts, EnrichmentStats, Templates, WebClientFactory,
};
use doewood_model::Backend;
use doewood_registry::favicon::{GuardOptions, GuardedClient, PublicOnly, SystemDns};
use doewood_store::Store;
use tokio::sync::watch;
use tokio::task::JoinHandle;

/// The web client of the production policy, through `proxy` when one is set.
pub fn production_web() -> WebClientFactory {
    Arc::new(|proxy| {
        let options = GuardOptions {
            proxy: proxy.cloned(),
            ..GuardOptions::default()
        };
        GuardedClient::new(&options, Arc::new(SystemDns), Arc::new(PublicOnly))
    })
}

fn counts(s: EnrichmentStats) -> EnrichmentCounts {
    EnrichmentCounts {
        enriched: s.enriched,
        merged: s.merged,
        proposed: s.proposed,
        failed: s.failed,
        registry_warnings: s.registry_warnings,
        edges: s.edges,
        redirected: s.redirected,
        websites: s.websites,
        passes: s.passes,
        last_pass_completed_at: s.last_pass_completed_at,
        running: s.running,
    }
}

/// Starts the job; it waits for the backend while the shared Postgres is not connected.
pub fn start(
    config: &BootstrapConfig,
    store: &Arc<Store>,
    backend: watch::Receiver<Option<Arc<dyn Backend>>>,
    progress: &Arc<Progress>,
    shutdown: watch::Receiver<bool>,
) -> JoinHandle<()> {
    let progress = Arc::clone(progress);
    progress.set_enrichment(EnrichmentCounts::default());
    let job = Enrichment::new(EnrichmentParts {
        instance: config.instance_id.clone(),
        backend,
        settings: EnrichmentParts::store_settings(Arc::clone(store)),
        templates: Templates::default(),
        web: production_web(),
        options: EnrichmentOptions::default(),
        on_change: Arc::new(move |s| progress.set_enrichment(counts(s))),
    });
    job.start(shutdown)
}
