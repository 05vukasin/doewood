//! Ingest's runtime settings (ADR 0001, section 3): set through the API, kept in the
//! `settings` table, read by ingest for every file, so a change applies without a restart.
//!
//! - The settings row `ingest.similar_min_jaccard` holds the least `MinHash` Jaccard estimate
//!   for a `similar_to` edge. Unset means [`DEFAULT_SIMILAR_MIN_JACCARD`].
//! - The settings row `ingest.summarize_max_concurrent` holds how many on-demand `Summarize`
//!   analyses may run at once. Unset means [`DEFAULT_SUMMARIZE_MAX_CONCURRENT`]. Read for every
//!   `Summarize` call.

use super::Store;
use crate::StoreError;

/// Settings key of the `similar_to` threshold.
pub const SIMILAR_MIN_JACCARD: &str = "ingest.similar_min_jaccard";

/// The `similar_to` threshold when none is set.
pub const DEFAULT_SIMILAR_MIN_JACCARD: f64 = 0.8;

/// Settings key of the on-demand `Summarize` cap.
pub const SUMMARIZE_MAX_CONCURRENT: &str = "ingest.summarize_max_concurrent";

/// On-demand `Summarize` analyses at once when none is set. Low on purpose: one scanned PDF
/// in OCR can take 0.6 to 1 GB per page in flight.
pub const DEFAULT_SUMMARIZE_MAX_CONCURRENT: u32 = 2;

/// The range [`SUMMARIZE_MAX_CONCURRENT`] accepts.
pub const SUMMARIZE_MAX_CONCURRENT_RANGE: std::ops::RangeInclusive<u32> = 1..=16;

/// Whether `n` is a usable `Summarize` cap: 1 to 16.
pub fn valid_summarize_max_concurrent(n: u32) -> bool {
    SUMMARIZE_MAX_CONCURRENT_RANGE.contains(&n)
}

/// Whether `x` is a usable `similar_to` threshold: above 0 and at most 1.
pub fn valid_min_jaccard(x: f64) -> bool {
    x > 0.0 && x <= 1.0
}

/// Ingest's runtime settings, as stored (defaults filled in).
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct IngestSettings {
    /// Least `MinHash` Jaccard estimate for a `similar_to` edge.
    pub similar_min_jaccard: f64,
    /// On-demand `Summarize` analyses (with the object read before them) at once.
    pub summarize_max_concurrent: u32,
}

impl Default for IngestSettings {
    fn default() -> Self {
        Self {
            similar_min_jaccard: DEFAULT_SIMILAR_MIN_JACCARD,
            summarize_max_concurrent: DEFAULT_SUMMARIZE_MAX_CONCURRENT,
        }
    }
}

/// A change to [`IngestSettings`]: `None` keeps the stored value.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct IngestSettingsPatch {
    /// The new `similar_to` threshold.
    pub similar_min_jaccard: Option<f64>,
    /// The new on-demand `Summarize` cap.
    pub summarize_max_concurrent: Option<u32>,
}

impl Store {
    /// Ingest's settings. A missing row gives the default; so does a stored value that is not
    /// valid (only possible by editing the file by hand).
    pub fn ingest_settings(&self) -> Result<IngestSettings, StoreError> {
        let similar_min_jaccard = self
            .setting(SIMILAR_MIN_JACCARD)?
            .and_then(|v| v.as_f64())
            .filter(|x| valid_min_jaccard(*x))
            .unwrap_or(DEFAULT_SIMILAR_MIN_JACCARD);
        let summarize_max_concurrent = self
            .setting(SUMMARIZE_MAX_CONCURRENT)?
            .and_then(|v| v.as_u64())
            .and_then(|n| u32::try_from(n).ok())
            .filter(|n| valid_summarize_max_concurrent(*n))
            .unwrap_or(DEFAULT_SUMMARIZE_MAX_CONCURRENT);
        Ok(IngestSettings {
            similar_min_jaccard,
            summarize_max_concurrent,
        })
    }

    /// Applies `patch` and returns the settings now in effect. An invalid value changes
    /// nothing (no field of the patch is written).
    pub fn set_ingest_settings(
        &self,
        patch: &IngestSettingsPatch,
    ) -> Result<IngestSettings, StoreError> {
        if let Some(x) = patch.similar_min_jaccard
            && !valid_min_jaccard(x)
        {
            return Err(StoreError::InvalidSetting(format!(
                "{SIMILAR_MIN_JACCARD} must be above 0 and at most 1"
            )));
        }
        if let Some(n) = patch.summarize_max_concurrent
            && !valid_summarize_max_concurrent(n)
        {
            return Err(StoreError::InvalidSetting(format!(
                "{SUMMARIZE_MAX_CONCURRENT} must be {} to {}",
                SUMMARIZE_MAX_CONCURRENT_RANGE.start(),
                SUMMARIZE_MAX_CONCURRENT_RANGE.end()
            )));
        }
        if let Some(x) = patch.similar_min_jaccard {
            self.set_setting(SIMILAR_MIN_JACCARD, &serde_json::json!(x))?;
        }
        if let Some(n) = patch.summarize_max_concurrent {
            self.set_setting(SUMMARIZE_MAX_CONCURRENT, &serde_json::json!(n))?;
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
        assert_eq!(s.setting(SUMMARIZE_MAX_CONCURRENT).unwrap(), None);
        assert_eq!(s.ingest_settings().unwrap().summarize_max_concurrent, 2);
    }

    #[test]
    fn summarize_cap_set_validated_and_kept_apart() {
        let s = store();
        let set = s
            .set_ingest_settings(&IngestSettingsPatch {
                summarize_max_concurrent: Some(5),
                ..IngestSettingsPatch::default()
            })
            .unwrap();
        assert_eq!(set.summarize_max_concurrent, 5);
        assert!((set.similar_min_jaccard - DEFAULT_SIMILAR_MIN_JACCARD).abs() < f64::EPSILON);
        for n in [1, 16] {
            let set = s
                .set_ingest_settings(&IngestSettingsPatch {
                    summarize_max_concurrent: Some(n),
                    ..IngestSettingsPatch::default()
                })
                .unwrap();
            assert_eq!(set.summarize_max_concurrent, n);
        }
        // Out of range: refused, and nothing in the patch is written.
        for bad in [0, 17, u32::MAX] {
            let err = s
                .set_ingest_settings(&IngestSettingsPatch {
                    similar_min_jaccard: Some(0.5),
                    summarize_max_concurrent: Some(bad),
                })
                .unwrap_err();
            assert!(matches!(err, StoreError::InvalidSetting(_)), "{bad}: {err}");
        }
        let now = s.ingest_settings().unwrap();
        assert_eq!(now.summarize_max_concurrent, 16);
        assert!((now.similar_min_jaccard - DEFAULT_SIMILAR_MIN_JACCARD).abs() < f64::EPSILON);
        // A bad stored value reads as the default.
        for bad in [
            serde_json::json!(0),
            serde_json::json!(99),
            serde_json::json!("3"),
        ] {
            s.set_setting(SUMMARIZE_MAX_CONCURRENT, &bad).unwrap();
            assert_eq!(s.ingest_settings().unwrap().summarize_max_concurrent, 2);
        }
    }

    #[test]
    fn set_and_read_back() {
        let s = store();
        let set = s
            .set_ingest_settings(&IngestSettingsPatch {
                similar_min_jaccard: Some(0.9),
                ..IngestSettingsPatch::default()
            })
            .unwrap();
        assert!((set.similar_min_jaccard - 0.9).abs() < f64::EPSILON);
        assert_eq!(s.ingest_settings().unwrap(), set);
        let one = s
            .set_ingest_settings(&IngestSettingsPatch {
                similar_min_jaccard: Some(1.0),
                ..IngestSettingsPatch::default()
            })
            .unwrap();
        assert!((one.similar_min_jaccard - 1.0).abs() < f64::EPSILON);
    }

    #[test]
    fn invalid_values_are_refused_and_change_nothing() {
        let s = store();
        s.set_ingest_settings(&IngestSettingsPatch {
            similar_min_jaccard: Some(0.7),
            ..IngestSettingsPatch::default()
        })
        .unwrap();
        for bad in [0.0, -0.1, 1.000_001, 2.0, f64::NAN, f64::INFINITY] {
            let err = s
                .set_ingest_settings(&IngestSettingsPatch {
                    similar_min_jaccard: Some(bad),
                    ..IngestSettingsPatch::default()
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
