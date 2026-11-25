//! `ConfigService.GetEnrichmentSettings` and `SetEnrichmentSettings`: company enrichment's
//! runtime settings (ADR 0002 section 7). The inputs are checked with the same code the
//! registry clients and the favicon guard use, so a value accepted here is one they accept.
//! Secrets (the API keys, APR's extra roots) are write-only: no response, log line or error
//! carries one.

use std::sync::Arc;
use std::time::Duration;

use doewood_registry::favicon::{GuardOptions, GuardedClient, PublicOnly, SystemDns};
use doewood_registry::{HttpTransport, edgar};
use doewood_store::{
    ApiKey, EnrichmentSettings, EnrichmentSettingsPatch, KeyChange, RegistrySwitchesPatch,
    RootsChange, RootsPem, valid_edgar_contact, valid_egress_proxy,
};
use tonic::Status;
use url::Url;

use crate::pb;
use crate::validate::Violations;

/// A write-only API key field and its `clear_` companion.
fn key_change(v: &mut Violations, field: &str, key: &str, clear: bool) -> KeyChange {
    if clear && !key.is_empty() {
        v.add(
            format!("clear_{field}"),
            format!("cannot be combined with {field}"),
        );
        return KeyChange::Keep;
    }
    if clear {
        return KeyChange::Clear;
    }
    if key.is_empty() {
        return KeyChange::Keep;
    }
    // The registry clients' own format check; its error never contains the key.
    if let Err(why) = doewood_registry::ApiKey::new(key) {
        v.add(field, why);
        return KeyChange::Keep;
    }
    if let Ok(k) = ApiKey::new(key.trim()) {
        KeyChange::Set(k)
    } else {
        v.add(field, "not a valid API key");
        KeyChange::Keep
    }
}

fn roots_change(v: &mut Violations, pem: &str, clear: bool) -> RootsChange {
    const FIELD: &str = "apr_extra_roots_pem";
    if clear && !pem.is_empty() {
        v.add(
            "clear_apr_extra_roots_pem",
            "cannot be combined with apr_extra_roots_pem",
        );
        return RootsChange::Keep;
    }
    if clear {
        return RootsChange::Clear;
    }
    if pem.is_empty() {
        return RootsChange::Keep;
    }
    let Ok(roots) = RootsPem::new(pem) else {
        v.add(
            FIELD,
            "must be a PEM bundle of certificates, at most 64 KiB",
        );
        return RootsChange::Keep;
    };
    // The client APR's download is built with: the certificates must parse. The error text
    // is not passed on, so no part of the PEM is echoed.
    let timeout = Duration::from_secs(5);
    if HttpTransport::with_extra_roots(timeout, timeout, roots.expose()).is_err() {
        v.add(FIELD, "holds no certificate that parses");
        return RootsChange::Keep;
    }
    RootsChange::Set(roots)
}

// `None` keeps, `Some(None)` removes, `Some(Some(_))` sets: the patch's shape.
#[allow(clippy::option_option)]
fn edgar_contact(v: &mut Violations, contact: Option<&str>) -> Option<Option<String>> {
    let c = contact?.trim();
    if c.is_empty() {
        return Some(None);
    }
    // The EDGAR client's own check (EdgarConfig::contact).
    if edgar::user_agent(Some(c)).is_err() || !valid_edgar_contact(c) {
        v.add(
            "edgar_contact",
            "must be at most 200 printable ASCII characters",
        );
        return None;
    }
    Some(Some(c.to_owned()))
}

#[allow(clippy::option_option)]
fn egress_proxy(v: &mut Violations, proxy: Option<&str>) -> Option<Option<String>> {
    const FIELD: &str = "favicon_egress_proxy";
    let p = proxy?.trim();
    if p.is_empty() {
        return Some(None);
    }
    let url = match Url::parse(p) {
        Ok(u) if valid_egress_proxy(p) => u,
        _ => {
            v.add(
                FIELD,
                "must be an http:// or https:// URL with a host and without credentials",
            );
            return None;
        }
    };
    // The favicon guard's own checks, by building the client it would use.
    let options = GuardOptions {
        proxy: Some(url),
        ..GuardOptions::default()
    };
    if let Err(why) = GuardedClient::new(&options, Arc::new(SystemDns), Arc::new(PublicOnly)) {
        v.add(FIELD, why);
        return None;
    }
    Some(Some(p.to_owned()))
}

/// Checks a `SetEnrichmentSettings` request, reporting every problem at once.
pub(crate) fn check(
    req: &pb::SetEnrichmentSettingsRequest,
) -> Result<EnrichmentSettingsPatch, Status> {
    let mut v = Violations::default();
    let patch = EnrichmentSettingsPatch {
        registries: RegistrySwitchesPatch {
            gleif: req.gleif_enabled,
            vies: req.vies_enabled,
            wikidata: req.wikidata_enabled,
            edgar: req.edgar_enabled,
            apr: req.apr_enabled,
            companies_house: req.companies_house_enabled,
        },
        companies_house_key: key_change(
            &mut v,
            "companies_house_api_key",
            &req.companies_house_api_key,
            req.clear_companies_house_api_key,
        ),
        opencorporates_key: key_change(
            &mut v,
            "opencorporates_api_key",
            &req.opencorporates_api_key,
            req.clear_opencorporates_api_key,
        ),
        edgar_contact: edgar_contact(&mut v, req.edgar_contact.as_deref()),
        apr_extra_roots: roots_change(
            &mut v,
            &req.apr_extra_roots_pem,
            req.clear_apr_extra_roots_pem,
        ),
        favicon_fallback: req.favicon_fallback,
        favicon_egress_proxy: egress_proxy(&mut v, req.favicon_egress_proxy.as_deref()),
    };
    v.check()?;
    Ok(patch)
}

/// The settings as the API shows them.
pub(crate) fn settings_pb(s: &EnrichmentSettings) -> pb::EnrichmentSettings {
    pb::EnrichmentSettings {
        gleif_enabled: s.registries.gleif,
        vies_enabled: s.registries.vies,
        wikidata_enabled: s.registries.wikidata,
        edgar_enabled: s.registries.edgar,
        apr_enabled: s.registries.apr,
        companies_house_enabled: s.registries.companies_house,
        companies_house_api_key_set: s.companies_house_key_set,
        companies_house_active: s.companies_house_active(),
        opencorporates_api_key_set: s.opencorporates_key_set,
        edgar_contact: s.edgar_contact.clone().unwrap_or_default(),
        apr_extra_roots_set: s.apr_extra_roots_set,
        favicon_fallback: s.favicon.fallback,
        favicon_egress_proxy: s.favicon.egress_proxy.clone().unwrap_or_default(),
    }
}
