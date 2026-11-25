//! What the enrichment job keeps in the store (ADR 0002 sections 2, 5 and 6): the registry
//! response cache, the favicon cache, and each company's freshness.
//!
//! Times are Unix seconds (UTC), like the rest of the model. Expired cache rows are never
//! returned, and [`crate::CacheStore::purge_expired`] deletes them.

use std::collections::BTreeMap;
use std::time::Duration;

use uuid::Uuid;

use crate::Error;

/// Longest registry name: `companies_house` and the like.
pub const REGISTRY_NAME_MAX_LEN: usize = 32;
/// Longest cache key, in bytes (a Postgres index entry must stay well under its 2.7 KB limit).
pub const CACHE_KEY_MAX_LEN: usize = 2048;
/// Largest cached body. The clients cap their bodies lower (EDGAR's largest is 4 MiB).
pub const CACHE_MAX_BODY: usize = 16 * 1024 * 1024;
/// Longest domain of a cached icon (DNS: 253).
pub const ICON_DOMAIN_MAX_LEN: usize = 253;
/// Largest cached icon. The favicon resolver refuses icons over 200 KB.
pub const ICON_MAX_BYTES: usize = 1024 * 1024;
/// The longest refresh interval accepted: ten years.
pub const MAX_REFRESH_INTERVAL: Duration = Duration::from_hours(10 * 365 * 24);
/// The latest time a cache row may carry: the end of year 9999, in Unix seconds.
pub const MAX_TIME: i64 = 253_402_300_799;
/// Largest page of [`crate::FreshnessStore::companies_due`].
pub const DUE_MAX_LIMIT: u32 = 1000;

/// The page size [`crate::FreshnessStore::companies_due`] uses for `limit`.
pub fn due_limit(limit: u32) -> u32 {
    match limit {
        0 => 100,
        n => n.min(DUE_MAX_LIMIT),
    }
}

/// Checks a registry name: 1 to [`REGISTRY_NAME_MAX_LEN`] of `a-z`, `0-9` and `_`.
pub fn validate_registry(name: &str) -> Result<(), Error> {
    if name.is_empty()
        || name.len() > REGISTRY_NAME_MAX_LEN
        || !name
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_')
    {
        return Err(Error::Invalid(format!(
            "a registry name is 1 to {REGISTRY_NAME_MAX_LEN} of a-z, 0-9 and _, not {name:?}"
        )));
    }
    Ok(())
}

fn validate_times(fetched_at: i64, expires_at: i64) -> Result<(), Error> {
    if !(0..=MAX_TIME).contains(&fetched_at) || !(0..=MAX_TIME).contains(&expires_at) {
        return Err(Error::Invalid(
            "cache times are between 1970 and the end of 9999".to_owned(),
        ));
    }
    Ok(())
}

/// Checks a cache key: 1 to [`CACHE_KEY_MAX_LEN`] bytes, without NUL.
pub fn validate_cache_key(key: &str) -> Result<(), Error> {
    if key.is_empty() || key.len() > CACHE_KEY_MAX_LEN || key.contains('\0') {
        return Err(Error::Invalid(format!(
            "a cache key is 1 to {CACHE_KEY_MAX_LEN} bytes, without NUL"
        )));
    }
    Ok(())
}

/// A cached registry answer (ADR 0002 section 2): the capped body as the client stores it (for
/// Companies House, the minimised fields only), or the registry's "no such record".
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CacheRow {
    /// The registry (`gleif`, `companies_house`, ...).
    pub registry: String,
    /// The request key within the registry (`lei-records/<LEI>`).
    pub key: String,
    /// The body; `None` when the registry had no such record.
    pub body: Option<Vec<u8>>,
    /// When the registry gave it.
    pub fetched_at: i64,
    /// From when it is no longer returned.
    pub expires_at: i64,
}

impl CacheRow {
    /// Checks the row before it reaches a backend.
    pub fn validate(&self) -> Result<(), Error> {
        validate_registry(&self.registry)?;
        validate_cache_key(&self.key)?;
        validate_times(self.fetched_at, self.expires_at)?;
        if self.body.as_ref().is_some_and(|b| b.len() > CACHE_MAX_BODY) {
            return Err(Error::Invalid(format!(
                "a cached body is at most {CACHE_MAX_BODY} bytes"
            )));
        }
        Ok(())
    }
}

/// A cached icon's bytes and format.
#[derive(Clone, PartialEq, Eq)]
pub struct StoredIcon {
    /// The format from the magic bytes (`ico`, `png`, `gif`, `jpeg`, `webp`, `avif`).
    pub format: String,
    /// The bytes, as fetched and checked.
    pub bytes: Vec<u8>,
}

impl std::fmt::Debug for StoredIcon {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StoredIcon")
            .field("format", &self.format)
            .field("len", &self.bytes.len())
            .finish()
    }
}

/// The icon formats a [`StoredIcon`] may have.
pub const ICON_FORMATS: &[&str] = &["ico", "png", "gif", "jpeg", "webp", "avif"];

/// A cached favicon lookup (ADR 0002 section 5): the icon, or a miss.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IconRow {
    /// The bare domain, lowercase (`acme.com`).
    pub domain: String,
    /// The icon; `None` for a miss.
    pub icon: Option<StoredIcon>,
    /// When it was looked up.
    pub fetched_at: i64,
    /// From when it is no longer returned.
    pub expires_at: i64,
}

impl IconRow {
    /// Checks the row before it reaches a backend.
    pub fn validate(&self) -> Result<(), Error> {
        let d = &self.domain;
        if d.is_empty()
            || d.len() > ICON_DOMAIN_MAX_LEN
            || !d
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-' || b == b'.')
        {
            return Err(Error::Invalid(format!(
                "an icon domain is 1 to {ICON_DOMAIN_MAX_LEN} of a-z, 0-9, - and ., not {d:?}"
            )));
        }
        validate_times(self.fetched_at, self.expires_at)?;
        if let Some(icon) = &self.icon {
            if !ICON_FORMATS.contains(&icon.format.as_str()) {
                return Err(Error::Invalid(format!(
                    "unknown icon format {:?}",
                    icon.format
                )));
            }
            if icon.bytes.is_empty() || icon.bytes.len() > ICON_MAX_BYTES {
                return Err(Error::Invalid(format!(
                    "an icon is 1 to {ICON_MAX_BYTES} bytes"
                )));
            }
        }
        Ok(())
    }
}

/// A company's freshness (ADR 0002 section 6): when it was last enriched, how many documents
/// mentioned it then, and when each registry is due again.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Enrichment {
    /// The company node.
    pub company: Uuid,
    /// When it was last enriched.
    pub enriched_at: i64,
    /// Its document count then (documents with a live `party` edge to it).
    pub documents: u64,
    /// When each registry is due again, by registry name.
    pub next_due: BTreeMap<String, i64>,
}

/// Checks a refresh schedule: valid registry names, each once, intervals up to
/// [`MAX_REFRESH_INTERVAL`].
pub fn validate_schedule(schedule: &[(String, Duration)]) -> Result<(), Error> {
    let mut seen = std::collections::BTreeSet::new();
    for (registry, every) in schedule {
        validate_registry(registry)?;
        if !seen.insert(registry.as_str()) {
            return Err(Error::Invalid(format!(
                "registry {registry:?} is in the schedule twice"
            )));
        }
        if *every > MAX_REFRESH_INTERVAL {
            return Err(Error::Invalid(
                "a refresh interval is at most ten years".to_owned(),
            ));
        }
    }
    Ok(())
}

/// Why a company is due for enrichment. The first that applies is given.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum DueReason {
    /// Never enriched.
    New,
    /// Its document count has at least doubled since it was enriched.
    Doubled,
    /// A registry's refresh time has passed.
    Scheduled,
}

/// A company due for enrichment, from [`crate::FreshnessStore::companies_due`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DueCompany {
    /// The company node.
    pub company: Uuid,
    /// Why it is due.
    pub reason: DueReason,
    /// Its document count now.
    pub documents: u64,
    /// The registries whose refresh time has passed, by name (empty for a new company).
    pub due: Vec<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names_keys_and_rows_are_checked() {
        for ok in ["gleif", "companies_house", "a1"] {
            assert!(validate_registry(ok).is_ok(), "{ok}");
        }
        for bad in ["", "GLEIF", "g-leif", "a:b", &"x".repeat(33)] {
            assert!(validate_registry(bad).is_err(), "{bad}");
        }
        assert!(validate_cache_key("lei-records/X").is_ok());
        assert!(validate_cache_key("").is_err());
        assert!(validate_cache_key("a\0b").is_err());
        assert!(validate_cache_key(&"k".repeat(CACHE_KEY_MAX_LEN + 1)).is_err());
        let icon = |format: &str, bytes: Vec<u8>| IconRow {
            domain: "acme.example".to_owned(),
            icon: Some(StoredIcon {
                format: format.to_owned(),
                bytes,
            }),
            fetched_at: 0,
            expires_at: 1,
        };
        assert!(icon("png", vec![1]).validate().is_ok());
        assert!(icon("svg", vec![1]).validate().is_err());
        assert!(icon("png", Vec::new()).validate().is_err());
        let mut upper = icon("png", vec![1]);
        upper.domain = "Acme.example".to_owned();
        assert!(upper.validate().is_err());
        let twice = [
            ("gleif".to_owned(), Duration::ZERO),
            ("gleif".to_owned(), Duration::ZERO),
        ];
        assert!(validate_schedule(&twice).is_err());
        assert!(validate_schedule(&[("vies".to_owned(), Duration::MAX)]).is_err());
        assert_eq!(
            (due_limit(0), due_limit(5), due_limit(5000)),
            (100, 5, 1000)
        );
    }
}
