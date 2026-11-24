//! Ingest's runtime settings (ADR 0001, section 3): set through the API, kept in the
//! `settings` table, read by ingest for every file, so a change applies without a restart.
//!
//! - The settings row `ingest.similar_min_jaccard` holds the least `MinHash` Jaccard estimate
//!   for a `similar_to` edge. Unset means [`DEFAULT_SIMILAR_MIN_JACCARD`].

use super::Store;
use crate::StoreError;

/// Settings key of the `similar_to` threshold.
pub const SIMILAR_MIN_JACCARD: &str = "ingest.similar_min_jaccard";

/// The `similar_to` threshold when none is set.
pub const DEFAULT_SIMILAR_MIN_JACCARD: f64 = 0.8;

/// Whether `x` is a usable `similar_to` threshold: above 0 and at most 1.
pub fn valid_min_jaccard(x: f64) -> bool {
    x > 0.0 && x <= 1.0
}

/// Ingest's runtime settings, as stored (defaults filled in).
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct IngestSettings {
    /// Least `MinHash` Jaccard estimate for a `similar_to` edge.
    pub similar_min_jaccard: f64,
}

impl Default for IngestSettings {
    fn default() -> Self {
        Self {
            similar_min_jaccard: DEFAULT_SIMILAR_MIN_JACCARD,
        }
    }
}

/// A change to [`IngestSettings`]: `None` keeps the stored value.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct IngestSettingsPatch {
    /// The new `similar_to` threshold.
    pub similar_min_jaccard: Option<f64>,
}

impl Store {
    /// Ingest's settings. A missing row gives the default; so does a stored value that is not
    /// a valid threshold (only possible by editing the file by hand).
    pub fn ingest_settings(&self) -> Result<IngestSettings, StoreError> {
        let similar_min_jaccard = self
            .setting(SIMILAR_MIN_JACCARD)?
            .and_then(|v| v.as_f64())
            .filter(|x| valid_min_jaccard(*x))
            .unwrap_or(DEFAULT_SIMILAR_MIN_JACCARD);
        Ok(IngestSettings {
            similar_min_jaccard,
        })
    }

    /// Applies `patch` and returns the settings now in effect. An invalid value changes
    /// nothing.
    pub fn set_ingest_settings(
        &self,
        patch: &IngestSettingsPatch,
    ) -> Result<IngestSettings, StoreError> {
        if let Some(x) = patch.similar_min_jaccard {
            if !valid_min_jaccard(x) {
                return Err(StoreError::InvalidSetting(format!(
                    "{SIMILAR_MIN_JACCARD} must be above 0 and at most 1"
                )));
            }
            self.set_setting(SIMILAR_MIN_JACCARD, &serde_json::json!(x))?;
        }
        self.ingest_settings()
    }
}

#[cfg(test)]
mod tests {
    use doewood_core::MasterKey;

    use super::*;

    fn store() -> Store {
        Store::open_in_memory(&MasterKey::from_bytes([4; 32])).unwrap()
    }

    #[test]
    fn default_until_set() {
        let s = store();
        assert_eq!(s.ingest_settings().unwrap(), IngestSettings::default());
        assert!(
            (s.ingest_settings().unwrap().similar_min_jaccard - 0.8).abs() < f64::EPSILON,
            "the default is the one ingest always had"
        );
        // An empty patch keeps the default and writes nothing.
        assert_eq!(
            s.set_ingest_settings(&IngestSettingsPatch::default())
                .unwrap(),
            IngestSettings::default()
        );
        assert_eq!(s.setting(SIMILAR_MIN_JACCARD).unwrap(), None);
    }

    #[test]
    fn set_and_read_back() {
        let s = store();
        let set = s
            .set_ingest_settings(&IngestSettingsPatch {
                similar_min_jaccard: Some(0.9),
            })
            .unwrap();
        assert!((set.similar_min_jaccard - 0.9).abs() < f64::EPSILON);
        assert_eq!(s.ingest_settings().unwrap(), set);
        let one = s
            .set_ingest_settings(&IngestSettingsPatch {
                similar_min_jaccard: Some(1.0),
            })
            .unwrap();
        assert!((one.similar_min_jaccard - 1.0).abs() < f64::EPSILON);
    }

    #[test]
    fn invalid_values_are_refused_and_change_nothing() {
        let s = store();
        s.set_ingest_settings(&IngestSettingsPatch {
            similar_min_jaccard: Some(0.7),
        })
        .unwrap();
        for bad in [0.0, -0.1, 1.000_001, 2.0, f64::NAN, f64::INFINITY] {
            let err = s
                .set_ingest_settings(&IngestSettingsPatch {
                    similar_min_jaccard: Some(bad),
                })
                .unwrap_err();
            assert!(matches!(err, StoreError::InvalidSetting(_)), "{bad}: {err}");
        }
        assert!((s.ingest_settings().unwrap().similar_min_jaccard - 0.7).abs() < f64::EPSILON);
    }

    #[test]
    fn a_bad_stored_value_reads_as_the_default() {
        let s = store();
        for bad in [
            serde_json::json!("0.9"),
            serde_json::json!(5),
            serde_json::json!(null),
        ] {
            s.set_setting(SIMILAR_MIN_JACCARD, &bad).unwrap();
            assert_eq!(s.ingest_settings().unwrap(), IngestSettings::default());
        }
    }
}
