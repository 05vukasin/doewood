//! The favicon proxy's runtime settings (ADR 0002 sections 5 and 7), kept in the `settings`
//! table and read for every icon lookup, so a change applies without a restart.
//!
//! - `favicon.fallback`: whether the public fallback service is asked when a site has no
//!   usable icon. Off unless set to `true`: asking it tells that service which domain is
//!   looked up.
//! - `favicon.egress_proxy`: an `http://` or `https://` proxy URL (no credentials) all favicon
//!   requests go through. Unset: direct.
//!
//! `ConfigService.SetEnrichmentSettings` sets them (through `Store::set_enrichment_settings`,
//! which writes the same rows).

use super::Store;
use crate::StoreError;

/// Settings key of the fallback switch.
pub const FAVICON_FALLBACK: &str = "favicon.fallback";
/// Settings key of the egress proxy.
pub const FAVICON_EGRESS_PROXY: &str = "favicon.egress_proxy";

/// The favicon proxy's runtime settings, as stored (defaults filled in).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct FaviconSettings {
    /// Ask the public fallback service. Default: off.
    pub fallback: bool,
    /// The egress proxy URL. Default: none.
    pub egress_proxy: Option<String>,
}

/// A change to [`FaviconSettings`]: `None` keeps the stored value.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct FaviconSettingsPatch {
    /// Turn the fallback on or off.
    pub fallback: Option<bool>,
    /// `Some(None)` removes the proxy; `Some(Some(url))` sets it.
    pub egress_proxy: Option<Option<String>>,
}

/// Whether `url` looks like a usable proxy URL: `http://` or `https://`, a host, no
/// credentials, no white space. (The favicon guard checks it fully.)
pub fn valid_egress_proxy(url: &str) -> bool {
    let rest = url
        .strip_prefix("http://")
        .or_else(|| url.strip_prefix("https://"));
    rest.is_some_and(|r| {
        !r.is_empty()
            && !r.starts_with('/')
            && !r.contains('@')
            && !r.chars().any(|c| c.is_whitespace() || c.is_control())
    }) && url.len() <= 2048
}

impl Store {
    /// The favicon settings. A missing row gives the default; so does a stored value of the
    /// wrong type (only possible by editing the file by hand).
    pub fn favicon_settings(&self) -> Result<FaviconSettings, StoreError> {
        let fallback = self
            .setting(FAVICON_FALLBACK)?
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        let egress_proxy = self
            .setting(FAVICON_EGRESS_PROXY)?
            .and_then(|v| v.as_str().map(str::to_owned))
            .filter(|u| valid_egress_proxy(u));
        Ok(FaviconSettings {
            fallback,
            egress_proxy,
        })
    }

    /// Applies `patch` and returns the settings now in effect. An invalid value changes
    /// nothing.
    pub fn set_favicon_settings(
        &self,
        patch: &FaviconSettingsPatch,
    ) -> Result<FaviconSettings, StoreError> {
        if let Some(Some(url)) = &patch.egress_proxy
            && !valid_egress_proxy(url)
        {
            return Err(StoreError::InvalidSetting(format!(
                "{FAVICON_EGRESS_PROXY} must be an http:// or https:// URL without credentials"
            )));
        }
        if let Some(on) = patch.fallback {
            self.set_setting(FAVICON_FALLBACK, &serde_json::json!(on))?;
        }
        if let Some(proxy) = &patch.egress_proxy {
            let value = proxy.as_ref().map_or(serde_json::Value::Null, |u| {
                serde_json::Value::String(u.clone())
            });
            self.set_setting(FAVICON_EGRESS_PROXY, &value)?;
        }
        self.favicon_settings()
    }
}

#[cfg(test)]
mod tests {
    use doewood_core::MasterKey;

    use super::*;

    fn store() -> Store {
        Store::open_in_memory(&MasterKey::from_bytes([5; 32])).unwrap()
    }

    #[test]
    fn the_fallback_is_off_by_default() {
        let s = store();
        assert_eq!(s.favicon_settings().unwrap(), FaviconSettings::default());
        assert!(!s.favicon_settings().unwrap().fallback);
        assert_eq!(s.favicon_settings().unwrap().egress_proxy, None);
    }

    #[test]
    fn set_and_read_back() {
        let s = store();
        let set = s
            .set_favicon_settings(&FaviconSettingsPatch {
                fallback: Some(true),
                egress_proxy: Some(Some("http://egress.svc:3128".to_owned())),
            })
            .unwrap();
        assert!(set.fallback);
        assert_eq!(set.egress_proxy.as_deref(), Some("http://egress.svc:3128"));
        assert_eq!(s.favicon_settings().unwrap(), set);
        // An empty patch keeps both.
        assert_eq!(
            s.set_favicon_settings(&FaviconSettingsPatch::default())
                .unwrap(),
            set
        );
        let cleared = s
            .set_favicon_settings(&FaviconSettingsPatch {
                fallback: Some(false),
                egress_proxy: Some(None),
            })
            .unwrap();
        assert_eq!(cleared, FaviconSettings::default());
    }

    #[test]
    fn invalid_proxies_are_refused_and_change_nothing() {
        let s = store();
        for bad in [
            "",
            "egress:3128",
            "socks5://egress:1080",
            "http://user:pw@egress:3128",
            "http://",
            "http:///x",
            "http://egress :3128",
        ] {
            let err = s
                .set_favicon_settings(&FaviconSettingsPatch {
                    fallback: Some(true),
                    egress_proxy: Some(Some(bad.to_owned())),
                })
                .unwrap_err();
            assert!(matches!(err, StoreError::InvalidSetting(_)), "{bad}: {err}");
        }
        assert_eq!(s.favicon_settings().unwrap(), FaviconSettings::default());
    }

    #[test]
    fn bad_stored_values_read_as_the_default() {
        let s = store();
        s.set_setting(FAVICON_FALLBACK, &serde_json::json!("yes"))
            .unwrap();
        s.set_setting(FAVICON_EGRESS_PROXY, &serde_json::json!("ftp://x"))
            .unwrap();
        assert_eq!(s.favicon_settings().unwrap(), FaviconSettings::default());
    }
}
