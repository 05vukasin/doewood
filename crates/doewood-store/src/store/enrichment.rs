//! Company enrichment's runtime settings (ADR 0002 section 7, ADR 0001 section 3): set
//! through the API, kept in the `settings` and `secrets` tables, and read by the enrichment
//! job on every pass (and by the favicon proxy on every lookup), so a change needs no restart.
//!
//! - `registry.<name>.enabled` (`gleif`, `vies`, `wikidata`, `edgar`, `apr`,
//!   `companies_house`): the registry switches. All on by default; Companies House is still
//!   asked only once its key is set.
//! - `feed.<name>.enabled` (`gleif`, `edgar`, `apr`, `companies_house`): the registry delta
//!   feed switches (ADR 0002 section 6, issue #49). GLEIF, EDGAR and APR on by default,
//!   Companies House streaming off (it needs a stream key).
//! - `registry.edgar.contact`: the operator's contact for the SEC's `User-Agent` rule. Unset:
//!   the generic `User-Agent`.
//! - The secrets `registry/companies_house/api_key`, `registry/companies_house/stream_key` (the
//!   streaming API's own key), `registry/opencorporates/api_key` (no
//!   client yet: only stored) and `registry/apr/extra_roots_pem` (the certificates APR's
//!   download trusts next to the system's). Write-only, like every secret.
//! - The favicon settings are the ones of [`super::favicon`] (`favicon.fallback`,
//!   `favicon.egress_proxy`).
//!
//! [`Store::enrichment_settings`] is the API's view (whether each secret is set, never the
//! secret). [`Store::enrichment_config`] is the job's: it decrypts the secrets, and the API
//! never calls it.

use std::fmt;

use rusqlite::{Connection, params};
use zeroize::Zeroizing;

use super::ai::{ApiKey, KeyChange};
use super::favicon::{FAVICON_EGRESS_PROXY, FAVICON_FALLBACK, FaviconSettings, valid_egress_proxy};
use super::{Store, now, write_sealed_tx};
use crate::StoreError;

/// Settings key of the GLEIF switch.
pub const GLEIF_ENABLED: &str = "registry.gleif.enabled";
/// Settings key of the VIES switch.
pub const VIES_ENABLED: &str = "registry.vies.enabled";
/// Settings key of the Wikidata switch.
pub const WIKIDATA_ENABLED: &str = "registry.wikidata.enabled";
/// Settings key of the SEC EDGAR switch.
pub const EDGAR_ENABLED: &str = "registry.edgar.enabled";
/// Settings key of the Serbian APR switch.
pub const APR_ENABLED: &str = "registry.apr.enabled";
/// Settings key of the UK Companies House switch.
pub const COMPANIES_HOUSE_ENABLED: &str = "registry.companies_house.enabled";
/// Settings key of the GLEIF delta feed switch (the golden copy's delta files).
pub const GLEIF_FEED_ENABLED: &str = "feed.gleif.enabled";
/// Settings key of the EDGAR feed switch (the daily index).
pub const EDGAR_FEED_ENABLED: &str = "feed.edgar.enabled";
/// Settings key of the APR feed switch (the monthly set, swapped in).
pub const APR_FEED_ENABLED: &str = "feed.apr.enabled";
/// Settings key of the Companies House streaming switch.
pub const COMPANIES_HOUSE_FEED_ENABLED: &str = "feed.companies_house.enabled";
/// Settings key of the EDGAR contact.
pub const EDGAR_CONTACT: &str = "registry.edgar.contact";

/// Secret holding the Companies House API key.
pub const COMPANIES_HOUSE_KEY_SECRET: &str = "registry/companies_house/api_key";
/// Secret holding the Companies House streaming API key (a key of its own: Companies House
/// issues streaming keys apart from REST keys).
pub const COMPANIES_HOUSE_STREAM_KEY_SECRET: &str = "registry/companies_house/stream_key";
/// Secret holding the `OpenCorporates` API key.
pub const OPENCORPORATES_KEY_SECRET: &str = "registry/opencorporates/api_key";
/// Secret holding APR's extra root certificates.
pub const APR_EXTRA_ROOTS_SECRET: &str = "registry/apr/extra_roots_pem";

/// The longest EDGAR contact, in characters (as `doewood-registry`'s EDGAR client).
pub const EDGAR_CONTACT_MAX_CHARS: usize = 200;
/// The largest PEM bundle accepted, in bytes.
pub const PEM_MAX_LEN: usize = 64 * 1024;

/// Whether `contact` is a usable EDGAR contact: 1 to 200 printable ASCII characters, as the
/// EDGAR client accepts. (Blank means "none" and is not stored.)
pub fn valid_edgar_contact(contact: &str) -> bool {
    !contact.trim().is_empty()
        && contact.chars().count() <= EDGAR_CONTACT_MAX_CHARS
        && contact.bytes().all(|b| (0x20..0x7f).contains(&b))
}

/// The registry switches (ADR 0002 section 7). The default: all on.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)]
pub struct RegistrySwitches {
    /// GLEIF (LEI records and relationships).
    pub gleif: bool,
    /// VIES (EU VAT numbers).
    pub vies: bool,
    /// Wikidata (the lowest tier).
    pub wikidata: bool,
    /// SEC EDGAR.
    pub edgar: bool,
    /// The Serbian APR open data set.
    pub apr: bool,
    /// UK Companies House: asked only when this is on **and** a key is set.
    pub companies_house: bool,
}

impl Default for RegistrySwitches {
    fn default() -> Self {
        Self {
            gleif: true,
            vies: true,
            wikidata: true,
            edgar: true,
            apr: true,
            companies_house: true,
        }
    }
}

/// The registry delta feed switches (ADR 0002 section 6, issue #49). A feed runs only while
/// its registry is asked too. The default: GLEIF, EDGAR and APR on, Companies House off.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)]
pub struct FeedSwitches {
    /// GLEIF's golden copy delta files (LEI-CDF and RR-CDF).
    pub gleif: bool,
    /// EDGAR's daily index.
    pub edgar: bool,
    /// APR's monthly set, swapped in.
    pub apr: bool,
    /// Companies House's streaming API: runs only with a stream key too.
    pub companies_house: bool,
}

impl Default for FeedSwitches {
    fn default() -> Self {
        Self {
            gleif: true,
            edgar: true,
            apr: true,
            companies_house: false,
        }
    }
}

/// A change to [`FeedSwitches`]: `None` keeps the stored value.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct FeedSwitchesPatch {
    /// GLEIF.
    pub gleif: Option<bool>,
    /// EDGAR.
    pub edgar: Option<bool>,
    /// APR.
    pub apr: Option<bool>,
    /// Companies House.
    pub companies_house: Option<bool>,
}

/// A change to [`RegistrySwitches`]: `None` keeps the stored value.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RegistrySwitchesPatch {
    /// GLEIF.
    pub gleif: Option<bool>,
    /// VIES.
    pub vies: Option<bool>,
    /// Wikidata.
    pub wikidata: Option<bool>,
    /// SEC EDGAR.
    pub edgar: Option<bool>,
    /// APR.
    pub apr: Option<bool>,
    /// Companies House.
    pub companies_house: Option<bool>,
}

/// A PEM bundle of extra root certificates. Zeroized on drop; `Debug` does not print it.
#[derive(Clone, PartialEq, Eq)]
pub struct RootsPem(Zeroizing<String>);

impl fmt::Debug for RootsPem {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("RootsPem(***)")
    }
}

impl RootsPem {
    /// At most 64 KiB of text holding at least one `BEGIN CERTIFICATE` block. (Whether the
    /// certificates parse is checked by the API, with the TLS stack APR's client uses.)
    pub fn new(pem: impl Into<String>) -> Result<Self, StoreError> {
        let pem = Zeroizing::new(pem.into());
        if pem.len() > PEM_MAX_LEN
            || !pem.contains("-----BEGIN CERTIFICATE-----")
            || !pem.contains("-----END CERTIFICATE-----")
        {
            return Err(StoreError::InvalidSetting(format!(
                "the extra roots must be a PEM bundle of certificates, at most {PEM_MAX_LEN} bytes"
            )));
        }
        Ok(Self(pem))
    }

    /// The PEM text.
    pub fn expose(&self) -> &str {
        &self.0
    }
}

/// What to do with the stored extra roots.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RootsChange {
    /// Keep them, if any.
    Keep,
    /// Replace them.
    Set(RootsPem),
    /// Delete them.
    Clear,
}

/// Enrichment's settings as the API shows them: never a secret, only whether one is set.
#[derive(Clone, Debug, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)]
pub struct EnrichmentSettings {
    /// The registry switches, as stored.
    pub registries: RegistrySwitches,
    /// Whether a Companies House key is stored.
    pub companies_house_key_set: bool,
    /// The delta feed switches, as stored.
    pub feeds: FeedSwitches,
    /// Whether a Companies House streaming key is stored.
    pub companies_house_stream_key_set: bool,
    /// Whether an `OpenCorporates` key is stored.
    pub opencorporates_key_set: bool,
    /// The EDGAR contact; `None`: the generic `User-Agent`.
    pub edgar_contact: Option<String>,
    /// Whether extra root certificates for APR are stored.
    pub apr_extra_roots_set: bool,
    /// The favicon proxy's settings.
    pub favicon: FaviconSettings,
}

impl EnrichmentSettings {
    /// Whether Companies House is asked: switched on and a key is set.
    pub fn companies_house_active(&self) -> bool {
        self.registries.companies_house && self.companies_house_key_set
    }

    /// Whether the Companies House stream is read: its switch on, a stream key set, and
    /// Companies House asked.
    pub fn companies_house_feed_active(&self) -> bool {
        self.feeds.companies_house
            && self.companies_house_stream_key_set
            && self.companies_house_active()
    }
}

/// Enrichment's settings for the job, secrets decrypted. Read it on every pass; every secret
/// in it has a redacted `Debug`.
#[derive(Clone, Debug, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)]
pub struct EnrichmentConfig {
    /// Ask GLEIF.
    pub gleif: bool,
    /// Ask VIES.
    pub vies: bool,
    /// Ask Wikidata.
    pub wikidata: bool,
    /// Ask EDGAR.
    pub edgar: bool,
    /// Ask APR.
    pub apr: bool,
    /// Ask Companies House: its switch is on **and** a key is set (so it is `false` until
    /// one is, and [`EnrichmentConfig::companies_house_key`] is then `Some`).
    pub companies_house: bool,
    /// The Companies House key.
    pub companies_house_key: Option<ApiKey>,
    /// The delta feed switches, as stored (each feed also needs its registry asked; the
    /// Companies House stream its key).
    pub feeds: FeedSwitches,
    /// The Companies House streaming key.
    pub companies_house_stream_key: Option<ApiKey>,
    /// The `OpenCorporates` key (no client uses it yet).
    pub opencorporates_key: Option<ApiKey>,
    /// The EDGAR contact (`EdgarConfig::contact`); `None`: the generic `User-Agent`.
    pub edgar_contact: Option<String>,
    /// APR's extra root certificates (`AprConfig::extra_roots_pem`).
    pub apr_extra_roots: Option<RootsPem>,
    /// The favicon proxy's settings.
    pub favicon: FaviconSettings,
}

/// A change to the enrichment settings. Every `None` (and [`KeyChange::Keep`],
/// [`RootsChange::Keep`]) keeps the stored value.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EnrichmentSettingsPatch {
    /// The registry switches.
    pub registries: RegistrySwitchesPatch,
    /// The Companies House key.
    pub companies_house_key: KeyChange,
    /// The delta feed switches.
    pub feeds: FeedSwitchesPatch,
    /// The Companies House streaming key.
    pub companies_house_stream_key: KeyChange,
    /// The `OpenCorporates` key.
    pub opencorporates_key: KeyChange,
    /// `Some(None)` removes the contact; `Some(Some(c))` sets it (see
    /// [`valid_edgar_contact`]).
    pub edgar_contact: Option<Option<String>>,
    /// APR's extra roots.
    pub apr_extra_roots: RootsChange,
    /// The favicon fallback switch.
    pub favicon_fallback: Option<bool>,
    /// `Some(None)` removes the egress proxy; `Some(Some(url))` sets it.
    pub favicon_egress_proxy: Option<Option<String>>,
}

impl Default for EnrichmentSettingsPatch {
    fn default() -> Self {
        Self {
            registries: RegistrySwitchesPatch::default(),
            companies_house_key: KeyChange::Keep,
            feeds: FeedSwitchesPatch::default(),
            companies_house_stream_key: KeyChange::Keep,
            opencorporates_key: KeyChange::Keep,
            edgar_contact: None,
            apr_extra_roots: RootsChange::Keep,
            favicon_fallback: None,
            favicon_egress_proxy: None,
        }
    }
}

fn put_setting(
    conn: &Connection,
    key: &str,
    value: &serde_json::Value,
    now: i64,
) -> Result<(), StoreError> {
    conn.execute(
        "INSERT INTO settings (key, value_json, updated_at) VALUES (?1, ?2, ?3) \
         ON CONFLICT (key) DO UPDATE SET value_json = excluded.value_json, \
         updated_at = excluded.updated_at",
        params![key, value.to_string(), now],
    )?;
    Ok(())
}

fn text_or_null(v: Option<&String>) -> serde_json::Value {
    v.map_or(serde_json::Value::Null, |s| {
        serde_json::Value::String(s.clone())
    })
}

impl Store {
    fn switch(&self, key: &str) -> Result<bool, StoreError> {
        self.switch_or(key, true)
    }

    fn switch_or(&self, key: &str, default: bool) -> Result<bool, StoreError> {
        // A stored value of the wrong type (only possible by editing the file by hand) reads
        // as the default.
        Ok(self
            .setting(key)?
            .and_then(|v| v.as_bool())
            .unwrap_or(default))
    }

    fn feed_switches(&self) -> Result<FeedSwitches, StoreError> {
        let d = FeedSwitches::default();
        Ok(FeedSwitches {
            gleif: self.switch_or(GLEIF_FEED_ENABLED, d.gleif)?,
            edgar: self.switch_or(EDGAR_FEED_ENABLED, d.edgar)?,
            apr: self.switch_or(APR_FEED_ENABLED, d.apr)?,
            companies_house: self.switch_or(COMPANIES_HOUSE_FEED_ENABLED, d.companies_house)?,
        })
    }

    fn registry_switches(&self) -> Result<RegistrySwitches, StoreError> {
        Ok(RegistrySwitches {
            gleif: self.switch(GLEIF_ENABLED)?,
            vies: self.switch(VIES_ENABLED)?,
            wikidata: self.switch(WIKIDATA_ENABLED)?,
            edgar: self.switch(EDGAR_ENABLED)?,
            apr: self.switch(APR_ENABLED)?,
            companies_house: self.switch(COMPANIES_HOUSE_ENABLED)?,
        })
    }

    fn edgar_contact(&self) -> Result<Option<String>, StoreError> {
        Ok(self
            .setting(EDGAR_CONTACT)?
            .and_then(|v| v.as_str().map(str::to_owned))
            .filter(|c| valid_edgar_contact(c)))
    }

    /// Enrichment's settings, defaults filled in, secrets only as "set or not".
    pub fn enrichment_settings(&self) -> Result<EnrichmentSettings, StoreError> {
        Ok(EnrichmentSettings {
            registries: self.registry_switches()?,
            companies_house_key_set: self.secret_exists(COMPANIES_HOUSE_KEY_SECRET)?,
            feeds: self.feed_switches()?,
            companies_house_stream_key_set: self
                .secret_exists(COMPANIES_HOUSE_STREAM_KEY_SECRET)?,
            opencorporates_key_set: self.secret_exists(OPENCORPORATES_KEY_SECRET)?,
            edgar_contact: self.edgar_contact()?,
            apr_extra_roots_set: self.secret_exists(APR_EXTRA_ROOTS_SECRET)?,
            favicon: self.favicon_settings()?,
        })
    }

    /// Enrichment's settings for the enrichment job, with the secrets decrypted: the job's
    /// access path to them. The API must never call it: secrets are write-only (ADR 0001,
    /// section 3).
    pub fn enrichment_config(&self) -> Result<EnrichmentConfig, StoreError> {
        let switches = self.registry_switches()?;
        let key = |name: &str| -> Result<Option<ApiKey>, StoreError> {
            let Some(plain) = self.reveal(name)? else {
                return Ok(None);
            };
            let text = String::from_utf8(plain.to_vec())
                .map_err(|_| StoreError::BadSetting(name.to_owned()))?;
            ApiKey::new(text).map(Some)
        };
        let companies_house_key = key(COMPANIES_HOUSE_KEY_SECRET)?;
        let companies_house_stream_key = key(COMPANIES_HOUSE_STREAM_KEY_SECRET)?;
        let opencorporates_key = key(OPENCORPORATES_KEY_SECRET)?;
        let apr_extra_roots = match self.reveal(APR_EXTRA_ROOTS_SECRET)? {
            None => None,
            Some(plain) => {
                let text = String::from_utf8(plain.to_vec())
                    .map_err(|_| StoreError::BadSetting(APR_EXTRA_ROOTS_SECRET.to_owned()))?;
                Some(RootsPem::new(text)?)
            }
        };
        Ok(EnrichmentConfig {
            gleif: switches.gleif,
            vies: switches.vies,
            wikidata: switches.wikidata,
            edgar: switches.edgar,
            apr: switches.apr,
            companies_house: switches.companies_house && companies_house_key.is_some(),
            companies_house_key,
            feeds: self.feed_switches()?,
            companies_house_stream_key,
            opencorporates_key,
            edgar_contact: self.edgar_contact()?,
            apr_extra_roots,
            favicon: self.favicon_settings()?,
        })
    }

    /// Applies `patch` in one transaction and returns the settings now in effect. An invalid
    /// value changes nothing; no error names a secret's value.
    pub fn set_enrichment_settings(
        &self,
        patch: &EnrichmentSettingsPatch,
    ) -> Result<EnrichmentSettings, StoreError> {
        if let Some(Some(c)) = &patch.edgar_contact
            && !valid_edgar_contact(c)
        {
            return Err(StoreError::InvalidSetting(format!(
                "{EDGAR_CONTACT} must be 1 to {EDGAR_CONTACT_MAX_CHARS} printable ASCII characters"
            )));
        }
        if let Some(Some(url)) = &patch.favicon_egress_proxy
            && !valid_egress_proxy(url)
        {
            return Err(StoreError::InvalidSetting(format!(
                "{FAVICON_EGRESS_PROXY} must be an http:// or https:// URL without credentials"
            )));
        }
        let seal_key = |name: &str, change: &KeyChange| match change {
            KeyChange::Set(k) => self.cipher.seal(name, k.expose().as_bytes()).map(Some),
            KeyChange::Keep | KeyChange::Clear => Ok(None),
        };
        let ch_sealed = seal_key(COMPANIES_HOUSE_KEY_SECRET, &patch.companies_house_key)?;
        let stream_sealed = seal_key(
            COMPANIES_HOUSE_STREAM_KEY_SECRET,
            &patch.companies_house_stream_key,
        )?;
        let oc_sealed = seal_key(OPENCORPORATES_KEY_SECRET, &patch.opencorporates_key)?;
        let roots_sealed = match &patch.apr_extra_roots {
            RootsChange::Set(p) => Some(
                self.cipher
                    .seal(APR_EXTRA_ROOTS_SECRET, p.expose().as_bytes())?,
            ),
            RootsChange::Keep | RootsChange::Clear => None,
        };
        let now = now();
        let mut conn = self.conn();
        let tx = conn.transaction()?;
        let r = &patch.registries;
        for (key, value) in [
            (GLEIF_ENABLED, r.gleif),
            (VIES_ENABLED, r.vies),
            (WIKIDATA_ENABLED, r.wikidata),
            (EDGAR_ENABLED, r.edgar),
            (APR_ENABLED, r.apr),
            (COMPANIES_HOUSE_ENABLED, r.companies_house),
            (FAVICON_FALLBACK, patch.favicon_fallback),
            (GLEIF_FEED_ENABLED, patch.feeds.gleif),
            (EDGAR_FEED_ENABLED, patch.feeds.edgar),
            (APR_FEED_ENABLED, patch.feeds.apr),
            (COMPANIES_HOUSE_FEED_ENABLED, patch.feeds.companies_house),
        ] {
            if let Some(on) = value {
                put_setting(&tx, key, &serde_json::json!(on), now)?;
            }
        }
        if let Some(c) = &patch.edgar_contact {
            put_setting(&tx, EDGAR_CONTACT, &text_or_null(c.as_ref()), now)?;
        }
        if let Some(p) = &patch.favicon_egress_proxy {
            put_setting(&tx, FAVICON_EGRESS_PROXY, &text_or_null(p.as_ref()), now)?;
        }
        let clear = |name: &str| tx.execute("DELETE FROM secrets WHERE name = ?1", [name]);
        for (name, cleared, sealed) in [
            (
                COMPANIES_HOUSE_KEY_SECRET,
                patch.companies_house_key == KeyChange::Clear,
                &ch_sealed,
            ),
            (
                COMPANIES_HOUSE_STREAM_KEY_SECRET,
                patch.companies_house_stream_key == KeyChange::Clear,
                &stream_sealed,
            ),
            (
                OPENCORPORATES_KEY_SECRET,
                patch.opencorporates_key == KeyChange::Clear,
                &oc_sealed,
            ),
            (
                APR_EXTRA_ROOTS_SECRET,
                patch.apr_extra_roots == RootsChange::Clear,
                &roots_sealed,
            ),
        ] {
            if let Some((nonce, ct)) = sealed {
                write_sealed_tx(&tx, name, nonce, ct, now)?;
            } else if cleared {
                clear(name)?;
            }
        }
        tx.commit()?;
        drop(conn);
        self.enrichment_settings()
    }
}

#[cfg(test)]
mod tests {
    use doewood_core::MasterKey;

    use super::*;

    const CH_KEY: &str = "test-ch-key-not-real-0001";
    const OC_KEY: &str = "test-oc-key-not-real-0002";
    const PEM: &str = "-----BEGIN CERTIFICATE-----\nMIIB\n-----END CERTIFICATE-----\n";

    fn store() -> Store {
        Store::open_in_memory(&MasterKey::from_bytes([6; 32])).unwrap()
    }

    #[test]
    fn defaults_keyless_on_companies_house_waits_for_a_key_favicon_fallback_off() {
        let s = store();
        let settings = s.enrichment_settings().unwrap();
        assert_eq!(settings.registries, RegistrySwitches::default());
        assert!(settings.registries.companies_house);
        assert!(!settings.companies_house_active());
        assert!(!settings.companies_house_key_set && !settings.opencorporates_key_set);
        assert!(!settings.apr_extra_roots_set);
        assert_eq!(settings.edgar_contact, None);
        assert_eq!(settings.favicon, FaviconSettings::default());
        let config = s.enrichment_config().unwrap();
        assert!(config.gleif && config.vies && config.wikidata && config.edgar && config.apr);
        assert!(!config.companies_house);
        assert_eq!(config.companies_house_key, None);
        assert!(!config.favicon.fallback);
        // An empty patch writes nothing.
        assert_eq!(
            s.set_enrichment_settings(&EnrichmentSettingsPatch::default())
                .unwrap(),
            settings
        );
        assert_eq!(s.setting(GLEIF_ENABLED).unwrap(), None);
    }

    #[test]
    fn round_trip_with_write_only_secrets() {
        let s = store();
        let set = s
            .set_enrichment_settings(&EnrichmentSettingsPatch {
                registries: RegistrySwitchesPatch {
                    wikidata: Some(false),
                    apr: Some(false),
                    ..RegistrySwitchesPatch::default()
                },
                companies_house_key: KeyChange::Set(ApiKey::new(CH_KEY).unwrap()),
                opencorporates_key: KeyChange::Set(ApiKey::new(OC_KEY).unwrap()),
                edgar_contact: Some(Some("Example Ops ops@example.invalid".to_owned())),
                apr_extra_roots: RootsChange::Set(RootsPem::new(PEM).unwrap()),
                favicon_fallback: Some(true),
                favicon_egress_proxy: Some(Some("http://egress.svc:3128".to_owned())),
                ..EnrichmentSettingsPatch::default()
            })
            .unwrap();
        assert!(!set.registries.wikidata && !set.registries.apr && set.registries.gleif);
        assert!(set.companies_house_key_set && set.opencorporates_key_set);
        assert!(set.companies_house_active() && set.apr_extra_roots_set);
        assert_eq!(
            set.edgar_contact.as_deref(),
            Some("Example Ops ops@example.invalid")
        );
        assert!(set.favicon.fallback);
        assert_eq!(s.enrichment_settings().unwrap(), set);
        // The favicon proxy reads the same rows.
        assert_eq!(s.favicon_settings().unwrap(), set.favicon);
        // The settings view never holds a secret.
        let shown = format!("{set:?}");
        for secret in [CH_KEY, OC_KEY, "MIIB"] {
            assert!(!shown.contains(secret), "{shown}");
        }

        // The job's view decrypts them, and its Debug is redacted.
        let config = s.enrichment_config().unwrap();
        assert!(config.companies_house && !config.wikidata && !config.apr);
        assert_eq!(
            config.companies_house_key.as_ref().unwrap().expose(),
            CH_KEY
        );
        assert_eq!(config.opencorporates_key.as_ref().unwrap().expose(), OC_KEY);
        assert_eq!(config.apr_extra_roots.as_ref().unwrap().expose(), PEM);
        let shown = format!("{config:?}");
        for secret in [CH_KEY, OC_KEY, "MIIB"] {
            assert!(!shown.contains(secret), "{shown}");
        }
        // Sealed at rest.
        let cts: Vec<Vec<u8>> = {
            let conn = s.raw();
            let mut stmt = conn
                .prepare("SELECT ciphertext FROM secrets WHERE name LIKE 'registry/%'")
                .unwrap();
            stmt.query_map([], |r| r.get(0))
                .unwrap()
                .collect::<Result<_, _>>()
                .unwrap()
        };
        assert_eq!(cts.len(), 3);
        for ct in &cts {
            assert!(!ct.windows(CH_KEY.len()).any(|w| w == CH_KEY.as_bytes()));
            assert!(!ct.windows(OC_KEY.len()).any(|w| w == OC_KEY.as_bytes()));
        }

        // Keep leaves everything; Clear and `Some(None)` remove.
        assert_eq!(
            s.set_enrichment_settings(&EnrichmentSettingsPatch::default())
                .unwrap(),
            set
        );
        let cleared = s
            .set_enrichment_settings(&EnrichmentSettingsPatch {
                companies_house_key: KeyChange::Clear,
                opencorporates_key: KeyChange::Clear,
                edgar_contact: Some(None),
                apr_extra_roots: RootsChange::Clear,
                favicon_fallback: Some(false),
                favicon_egress_proxy: Some(None),
                ..EnrichmentSettingsPatch::default()
            })
            .unwrap();
        assert!(!cleared.companies_house_key_set && !cleared.opencorporates_key_set);
        assert!(!cleared.apr_extra_roots_set && !cleared.companies_house_active());
        assert_eq!(cleared.edgar_contact, None);
        assert_eq!(cleared.favicon, FaviconSettings::default());
        let config = s.enrichment_config().unwrap();
        assert!(!config.companies_house && config.companies_house_key.is_none());
    }

    #[test]
    fn companies_house_needs_its_switch_and_a_key() {
        let s = store();
        let off = s
            .set_enrichment_settings(&EnrichmentSettingsPatch {
                registries: RegistrySwitchesPatch {
                    companies_house: Some(false),
                    ..RegistrySwitchesPatch::default()
                },
                companies_house_key: KeyChange::Set(ApiKey::new(CH_KEY).unwrap()),
                ..EnrichmentSettingsPatch::default()
            })
            .unwrap();
        assert!(off.companies_house_key_set && !off.companies_house_active());
        assert!(!s.enrichment_config().unwrap().companies_house);
        let on = s
            .set_enrichment_settings(&EnrichmentSettingsPatch {
                registries: RegistrySwitchesPatch {
                    companies_house: Some(true),
                    ..RegistrySwitchesPatch::default()
                },
                ..EnrichmentSettingsPatch::default()
            })
            .unwrap();
        assert!(on.companies_house_active());
        assert!(s.enrichment_config().unwrap().companies_house);
    }

    #[test]
    fn invalid_values_are_refused_and_change_nothing() {
        let s = store();
        for patch in [
            EnrichmentSettingsPatch {
                edgar_contact: Some(Some("Ops\r\nX-Evil: 1".to_owned())),
                favicon_fallback: Some(true),
                ..EnrichmentSettingsPatch::default()
            },
            EnrichmentSettingsPatch {
                edgar_contact: Some(Some("x".repeat(201))),
                ..EnrichmentSettingsPatch::default()
            },
            EnrichmentSettingsPatch {
                edgar_contact: Some(Some("   ".to_owned())),
                ..EnrichmentSettingsPatch::default()
            },
            EnrichmentSettingsPatch {
                favicon_egress_proxy: Some(Some("socks5://egress:1080".to_owned())),
                companies_house_key: KeyChange::Set(ApiKey::new(CH_KEY).unwrap()),
                ..EnrichmentSettingsPatch::default()
            },
        ] {
            let err = s.set_enrichment_settings(&patch).unwrap_err();
            assert!(matches!(err, StoreError::InvalidSetting(_)), "{err}");
            assert!(!err.to_string().contains(CH_KEY));
        }
        assert!(RootsPem::new("not a certificate").is_err());
        assert!(RootsPem::new("x".repeat(PEM_MAX_LEN + 1)).is_err());
        assert_eq!(
            format!("{:?}", RootsPem::new(PEM).unwrap()),
            "RootsPem(***)"
        );
        let unchanged = s.enrichment_settings().unwrap();
        assert!(!unchanged.favicon.fallback && !unchanged.companies_house_key_set);
    }

    #[test]
    fn feed_switches_default_and_the_stream_key_is_write_only() {
        const STREAM_KEY: &str = "test-stream-key-not-real-03";
        let s = store();
        let settings = s.enrichment_settings().unwrap();
        assert_eq!(settings.feeds, FeedSwitches::default());
        assert!(settings.feeds.gleif && settings.feeds.edgar && settings.feeds.apr);
        assert!(!settings.feeds.companies_house);
        assert!(!settings.companies_house_stream_key_set);
        assert!(!settings.companies_house_feed_active());
        let set = s
            .set_enrichment_settings(&EnrichmentSettingsPatch {
                feeds: FeedSwitchesPatch {
                    gleif: Some(false),
                    companies_house: Some(true),
                    ..FeedSwitchesPatch::default()
                },
                companies_house_stream_key: KeyChange::Set(ApiKey::new(STREAM_KEY).unwrap()),
                ..EnrichmentSettingsPatch::default()
            })
            .unwrap();
        assert!(!set.feeds.gleif && set.feeds.edgar && set.feeds.companies_house);
        assert!(set.companies_house_stream_key_set);
        // The stream also needs Companies House asked (its REST key).
        assert!(!set.companies_house_feed_active());
        let set = s
            .set_enrichment_settings(&EnrichmentSettingsPatch {
                companies_house_key: KeyChange::Set(ApiKey::new(CH_KEY).unwrap()),
                ..EnrichmentSettingsPatch::default()
            })
            .unwrap();
        assert!(set.companies_house_feed_active());
        assert!(!format!("{set:?}").contains(STREAM_KEY));
        let config = s.enrichment_config().unwrap();
        assert_eq!(config.feeds, set.feeds);
        assert_eq!(
            config.companies_house_stream_key.as_ref().unwrap().expose(),
            STREAM_KEY
        );
        assert!(!format!("{config:?}").contains(STREAM_KEY));
        let cleared = s
            .set_enrichment_settings(&EnrichmentSettingsPatch {
                companies_house_stream_key: KeyChange::Clear,
                ..EnrichmentSettingsPatch::default()
            })
            .unwrap();
        assert!(!cleared.companies_house_stream_key_set && cleared.companies_house_key_set);
        assert!(!cleared.companies_house_feed_active());
    }

    #[test]
    fn bad_stored_values_read_as_the_default() {
        let s = store();
        s.set_setting(GLEIF_ENABLED, &serde_json::json!("no"))
            .unwrap();
        s.set_setting(EDGAR_CONTACT, &serde_json::json!("bad\u{7f}"))
            .unwrap();
        s.set_setting(COMPANIES_HOUSE_FEED_ENABLED, &serde_json::json!(1))
            .unwrap();
        let settings = s.enrichment_settings().unwrap();
        assert!(settings.registries.gleif);
        assert!(!settings.feeds.companies_house);
        assert_eq!(settings.edgar_contact, None);
    }
}
