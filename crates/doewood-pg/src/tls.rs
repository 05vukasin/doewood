//! TLS for Postgres connections: rustls with the ring provider.

use std::sync::{Arc, OnceLock};

use doewood_model::{Error, Result};
use rustls::RootCertStore;
use tokio_postgres_rustls::MakeRustlsConnect;
use tracing::{debug, warn};

/// The trusted roots: the system store (where a cluster's own CA is installed) plus the
/// Mozilla roots, so a minimal image without CA certificates still reaches managed databases.
fn roots() -> &'static RootCertStore {
    static ROOTS: OnceLock<RootCertStore> = OnceLock::new();
    ROOTS.get_or_init(|| {
        let mut roots = RootCertStore::empty();
        roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
        let native = rustls_native_certs::load_native_certs();
        for e in &native.errors {
            warn!(error = %e, "could not load a system CA certificate");
        }
        let (added, ignored) = roots.add_parsable_certificates(native.certs);
        debug!(added, ignored, "system CA certificates loaded");
        roots
    })
}

pub(crate) fn connector() -> Result<MakeRustlsConnect> {
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let config = rustls::ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .map_err(|e| Error::Invalid(format!("TLS configuration: {e}")))?
        .with_root_certificates(roots().clone())
        .with_no_client_auth();
    Ok(MakeRustlsConnect::new(config))
}
