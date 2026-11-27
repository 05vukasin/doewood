//! Ingest's runtime settings (ADR 0001, section 3): set through the API, kept in the
//! `settings` table, read by ingest for every file, so a change applies without a restart.
//!
//! - The settings row `ingest.similar_min_jaccard` holds the least `MinHash` Jaccard estimate
//!   for a `similar_to` edge: the "related version" threshold (issue #169). Unset means
//!   [`DEFAULT_SIMILAR_MIN_JACCARD`].
//! - The rows of the other relations' thresholds (issue #169):
//!   `ingest.similar_near_copy_min_jaccard`, `ingest.similar_template_min_masked` and
//!   `ingest.similar_identical_min_chars`, and the crowd size `ingest.similar_crowd_size`.
//!   Unset means their defaults.
//! - The settings row `ingest.similar_max_bucket` holds the largest LSH band bucket the
//!   `similar_to` candidate query reads (issue #148): a band key more documents share is
//!   skipped. Unset means [`DEFAULT_SIMILAR_MAX_BUCKET`].
//! - The settings row `ingest.summarize_max_concurrent` holds how many on-demand `Summarize`
//!   analyses may run at once. Unset means [`DEFAULT_SUMMARIZE_MAX_CONCURRENT`]. Read for every
//!   `Summarize` call.

use super::{Snapshot, Store};
use crate::StoreError;

/// Settings key of the `similar_to` threshold.
pub const SIMILAR_MIN_JACCARD: &str = "ingest.similar_min_jaccard";

/// The `similar_to` threshold when none is set: the related-version bar of issue #169 (0.8
/// before it).
pub const DEFAULT_SIMILAR_MIN_JACCARD: f64 = 0.6;

/// Settings key of the near-copy threshold (issue #169).
pub const SIMILAR_NEAR_COPY_MIN_JACCARD: &str = "ingest.similar_near_copy_min_jaccard";

/// The least raw `MinHash` estimate of a near copy when none is set.
pub const DEFAULT_SIMILAR_NEAR_COPY_MIN_JACCARD: f64 = 0.9;

/// Settings key of the same-template threshold (issue #169).
pub const SIMILAR_TEMPLATE_MIN_MASKED: &str = "ingest.similar_template_min_masked";

/// The least masked similarity of one template when none is set.
pub const DEFAULT_SIMILAR_TEMPLATE_MIN_MASKED: f64 = 0.6;

/// Settings key of the shortest text two identical documents need (issue #169).
pub const SIMILAR_IDENTICAL_MIN_CHARS: &str = "ingest.similar_identical_min_chars";

/// The shortest identical text, in characters (`text.chars`, whitespace excluded), when none
/// is set. quail emits no text key below 400.
pub const DEFAULT_SIMILAR_IDENTICAL_MIN_CHARS: u32 = 1000;

/// The range [`SIMILAR_IDENTICAL_MIN_CHARS`] accepts.
pub const SIMILAR_IDENTICAL_MIN_CHARS_RANGE: std::ops::RangeInclusive<u32> = 0..=10_000_000;

/// Whether `n` is a usable shortest identical text: 0 to 10,000,000.
pub fn valid_identical_min_chars(n: u32) -> bool {
    SIMILAR_IDENTICAL_MIN_CHARS_RANGE.contains(&n)
}

/// Settings key of the crowd size (issue #169).
pub const SIMILAR_CROWD_SIZE: &str = "ingest.similar_crowd_size";

/// Matches in one relation above which a document is in a crowd, when none is set.
pub const DEFAULT_SIMILAR_CROWD_SIZE: u32 = 50;

/// The range [`SIMILAR_CROWD_SIZE`] accepts.
pub const SIMILAR_CROWD_SIZE_RANGE: std::ops::RangeInclusive<u32> = 1..=1000;

/// Whether `n` is a usable crowd size: 1 to 1000.
pub fn valid_crowd_size(n: u32) -> bool {
    SIMILAR_CROWD_SIZE_RANGE.contains(&n)
}

/// Settings key of the largest band bucket the `similar_to` candidate query reads.
pub const SIMILAR_MAX_BUCKET: &str = "ingest.similar_max_bucket";

/// The largest band bucket read when none is set: a band key shared by more documents is a
/// template's boilerplate, not similarity (issue #148).
pub const DEFAULT_SIMILAR_MAX_BUCKET: u32 = 1000;

/// The range [`SIMILAR_MAX_BUCKET`] accepts.
pub const SIMILAR_MAX_BUCKET_RANGE: std::ops::RangeInclusive<u32> = 10..=1_000_000;

/// Whether `n` is a usable largest band bucket: 10 to 1,000,000.
pub fn valid_similar_max_bucket(n: u32) -> bool {
    SIMILAR_MAX_BUCKET_RANGE.contains(&n)
}

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

/// Whether `x` is a usable `similar_to` threshold: above 0 and at most 1. The same range
/// holds for the near-copy and same-template thresholds.
pub fn valid_min_jaccard(x: f64) -> bool {
    x > 0.0 && x <= 1.0
}

/// Ingest's runtime settings, as stored (defaults filled in).
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct IngestSettings {
    /// Least `MinHash` Jaccard estimate for a `similar_to` edge (a related version).
    pub similar_min_jaccard: f64,
    /// Least `MinHash` Jaccard estimate of a near copy.
    pub similar_near_copy_min_jaccard: f64,
    /// Least masked similarity of one template.
    pub similar_template_min_masked: f64,
    /// Shortest text two identical documents need, in characters.
    pub similar_identical_min_chars: u32,
    /// Matches in one relation above which a document is in a crowd.
    pub similar_crowd_size: u32,
    /// Largest LSH band bucket the `similar_to` candidate query reads.
    pub similar_max_bucket: u32,
    /// On-demand `Summarize` analyses (with the object read before them) at once.
    pub summarize_max_concurrent: u32,
}

impl Default for IngestSettings {
    fn default() -> Self {
        Self {
            similar_min_jaccard: DEFAULT_SIMILAR_MIN_JACCARD,
            similar_near_copy_min_jaccard: DEFAULT_SIMILAR_NEAR_COPY_MIN_JACCARD,
            similar_template_min_masked: DEFAULT_SIMILAR_TEMPLATE_MIN_MASKED,
            similar_identical_min_chars: DEFAULT_SIMILAR_IDENTICAL_MIN_CHARS,
            similar_crowd_size: DEFAULT_SIMILAR_CROWD_SIZE,
            similar_max_bucket: DEFAULT_SIMILAR_MAX_BUCKET,
            summarize_max_concurrent: DEFAULT_SUMMARIZE_MAX_CONCURRENT,
        }
    }
}

/// A change to [`IngestSettings`]: `None` keeps the stored value.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct IngestSettingsPatch {
    /// The new `similar_to` threshold.
    pub similar_min_jaccard: Option<f64>,
    /// The new near-copy threshold.
    pub similar_near_copy_min_jaccard: Option<f64>,
    /// The new same-template threshold.
    pub similar_template_min_masked: Option<f64>,
    /// The new shortest identical text.
    pub similar_identical_min_chars: Option<u32>,
    /// The new crowd size.
    pub similar_crowd_size: Option<u32>,
    /// The new largest band bucket.
    pub similar_max_bucket: Option<u32>,
    /// The new on-demand `Summarize` cap.
    pub summarize_max_concurrent: Option<u32>,
}

impl Store {
    /// Ingest's settings. A missing row gives the default; so does a stored value that is not
    /// valid (only possible by editing the file by hand).
    pub fn ingest_settings(&self) -> Result<IngestSettings, StoreError> {
        // `Snapshot::ingest_settings` itself is not general enough over the snapshot's lifetime.
        #[allow(clippy::redundant_closure_for_method_calls)]
        self.snapshot(|s| s.ingest_settings())
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
        for (key, x) in [
            (
                SIMILAR_NEAR_COPY_MIN_JACCARD,
                patch.similar_near_copy_min_jaccard,
            ),
            (
                SIMILAR_TEMPLATE_MIN_MASKED,
                patch.similar_template_min_masked,
            ),
        ] {
            if let Some(x) = x
                && !valid_min_jaccard(x)
            {
                return Err(StoreError::InvalidSetting(format!(
                    "{key} must be above 0 and at most 1"
                )));
            }
        }
        if let Some(n) = patch.similar_identical_min_chars
            && !valid_identical_min_chars(n)
        {
            return Err(StoreError::InvalidSetting(format!(
                "{SIMILAR_IDENTICAL_MIN_CHARS} must be {} to {}",
                SIMILAR_IDENTICAL_MIN_CHARS_RANGE.start(),
                SIMILAR_IDENTICAL_MIN_CHARS_RANGE.end()
            )));
        }
        if let Some(n) = patch.similar_crowd_size
            && !valid_crowd_size(n)
        {
            return Err(StoreError::InvalidSetting(format!(
                "{SIMILAR_CROWD_SIZE} must be {} to {}",
                SIMILAR_CROWD_SIZE_RANGE.start(),
                SIMILAR_CROWD_SIZE_RANGE.end()
            )));
        }
        if let Some(n) = patch.similar_max_bucket
            && !valid_similar_max_bucket(n)
        {
            return Err(StoreError::InvalidSetting(format!(
                "{SIMILAR_MAX_BUCKET} must be {} to {}",
                SIMILAR_MAX_BUCKET_RANGE.start(),
                SIMILAR_MAX_BUCKET_RANGE.end()
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
        if let Some(x) = patch.similar_near_copy_min_jaccard {
            self.set_setting(SIMILAR_NEAR_COPY_MIN_JACCARD, &serde_json::json!(x))?;
        }
        if let Some(x) = patch.similar_template_min_masked {
            self.set_setting(SIMILAR_TEMPLATE_MIN_MASKED, &serde_json::json!(x))?;
        }
        if let Some(n) = patch.similar_identical_min_chars {
            self.set_setting(SIMILAR_IDENTICAL_MIN_CHARS, &serde_json::json!(n))?;
        }
        if let Some(n) = patch.similar_crowd_size {
            self.set_setting(SIMILAR_CROWD_SIZE, &serde_json::json!(n))?;
        }
        if let Some(n) = patch.similar_max_bucket {
            self.set_setting(SIMILAR_MAX_BUCKET, &serde_json::json!(n))?;
        }
        if let Some(n) = patch.summarize_max_concurrent {
            self.set_setting(SUMMARIZE_MAX_CONCURRENT, &serde_json::json!(n))?;
        }
        self.ingest_settings()
    }
}

impl Snapshot<'_> {
    /// Ingest's settings ([`Store::ingest_settings`]).
    pub fn ingest_settings(&self) -> Result<IngestSettings, StoreError> {
        let similar_min_jaccard = self
            .setting(SIMILAR_MIN_JACCARD)?
            .and_then(|v| v.as_f64())
            .filter(|x| valid_min_jaccard(*x))
            .unwrap_or(DEFAULT_SIMILAR_MIN_JACCARD);
        let share = |key: &str, default: f64| -> Result<f64, StoreError> {
            Ok(self
                .setting(key)?
                .and_then(|v| v.as_f64())
                .filter(|x| valid_min_jaccard(*x))
                .unwrap_or(default))
        };
        let similar_near_copy_min_jaccard = share(
            SIMILAR_NEAR_COPY_MIN_JACCARD,
            DEFAULT_SIMILAR_NEAR_COPY_MIN_JACCARD,
        )?;
        let similar_template_min_masked = share(
            SIMILAR_TEMPLATE_MIN_MASKED,
            DEFAULT_SIMILAR_TEMPLATE_MIN_MASKED,
        )?;
        let count = |key: &str, ok: fn(u32) -> bool, default: u32| -> Result<u32, StoreError> {
            Ok(self
                .setting(key)?
                .and_then(|v| v.as_u64())
                .and_then(|n| u32::try_from(n).ok())
                .filter(|n| ok(*n))
                .unwrap_or(default))
        };
        let similar_identical_min_chars = count(
            SIMILAR_IDENTICAL_MIN_CHARS,
            valid_identical_min_chars,
            DEFAULT_SIMILAR_IDENTICAL_MIN_CHARS,
        )?;
        let similar_crowd_size = count(
            SIMILAR_CROWD_SIZE,
            valid_crowd_size,
            DEFAULT_SIMILAR_CROWD_SIZE,
        )?;
        let similar_max_bucket = self
            .setting(SIMILAR_MAX_BUCKET)?
            .and_then(|v| v.as_u64())
            .and_then(|n| u32::try_from(n).ok())
            .filter(|n| valid_similar_max_bucket(*n))
            .unwrap_or(DEFAULT_SIMILAR_MAX_BUCKET);
        let summarize_max_concurrent = self
            .setting(SUMMARIZE_MAX_CONCURRENT)?
            .and_then(|v| v.as_u64())
            .and_then(|n| u32::try_from(n).ok())
            .filter(|n| valid_summarize_max_concurrent(*n))
            .unwrap_or(DEFAULT_SUMMARIZE_MAX_CONCURRENT);
        Ok(IngestSettings {
            similar_min_jaccard,
            similar_near_copy_min_jaccard,
            similar_template_min_masked,
            similar_identical_min_chars,
            similar_crowd_size,
            similar_max_bucket,
            summarize_max_concurrent,
        })
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
        let d = s.ingest_settings().unwrap();
        assert!(
            (d.similar_min_jaccard - 0.6).abs() < f64::EPSILON,
            "the related-version bar of issue #169"
        );
        assert!((d.similar_near_copy_min_jaccard - 0.9).abs() < f64::EPSILON);
        assert!((d.similar_template_min_masked - 0.6).abs() < f64::EPSILON);
        assert_eq!(d.similar_identical_min_chars, 1000);
        assert_eq!(d.similar_crowd_size, 50);
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
                    ..IngestSettingsPatch::default()
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
    fn max_bucket_set_validated_and_kept_apart() {
        let s = store();
        assert_eq!(s.ingest_settings().unwrap().similar_max_bucket, 1000);
        for n in [10, 5000, 1_000_000] {
            let set = s
                .set_ingest_settings(&IngestSettingsPatch {
                    similar_max_bucket: Some(n),
                    ..IngestSettingsPatch::default()
                })
                .unwrap();
            assert_eq!(set.similar_max_bucket, n);
            assert_eq!(
                set.summarize_max_concurrent,
                DEFAULT_SUMMARIZE_MAX_CONCURRENT
            );
        }
        // Out of range: refused, and nothing in the patch is written.
        for bad in [0, 9, 1_000_001, u32::MAX] {
            let err = s
                .set_ingest_settings(&IngestSettingsPatch {
                    similar_min_jaccard: Some(0.5),
                    similar_max_bucket: Some(bad),
                    ..IngestSettingsPatch::default()
                })
                .unwrap_err();
            assert!(matches!(err, StoreError::InvalidSetting(_)), "{bad}: {err}");
        }
        let now = s.ingest_settings().unwrap();
        assert_eq!(now.similar_max_bucket, 1_000_000);
        assert!((now.similar_min_jaccard - DEFAULT_SIMILAR_MIN_JACCARD).abs() < f64::EPSILON);
        // A bad stored value reads as the default.
        for bad in [serde_json::json!(5), serde_json::json!("2000")] {
            s.set_setting(SIMILAR_MAX_BUCKET, &bad).unwrap();
            assert_eq!(
                s.ingest_settings().unwrap().similar_max_bucket,
                DEFAULT_SIMILAR_MAX_BUCKET
            );
        }
    }

    #[test]
    fn relation_thresholds_set_validated_and_kept_apart() {
        let s = store();
        let set = s
            .set_ingest_settings(&IngestSettingsPatch {
                similar_near_copy_min_jaccard: Some(0.95),
                similar_template_min_masked: Some(0.7),
                similar_identical_min_chars: Some(0),
                similar_crowd_size: Some(1000),
                ..IngestSettingsPatch::default()
            })
            .unwrap();
        assert!((set.similar_near_copy_min_jaccard - 0.95).abs() < f64::EPSILON);
        assert!((set.similar_template_min_masked - 0.7).abs() < f64::EPSILON);
        assert_eq!(set.similar_identical_min_chars, 0);
        assert_eq!(set.similar_crowd_size, 1000);
        assert!((set.similar_min_jaccard - DEFAULT_SIMILAR_MIN_JACCARD).abs() < f64::EPSILON);
        assert_eq!(s.ingest_settings().unwrap(), set);
        // Out of range: refused, and nothing in the patch is written.
        let bad = [
            IngestSettingsPatch {
                similar_near_copy_min_jaccard: Some(0.0),
                ..IngestSettingsPatch::default()
            },
            IngestSettingsPatch {
                similar_template_min_masked: Some(f64::NAN),
                ..IngestSettingsPatch::default()
            },
            IngestSettingsPatch {
                similar_identical_min_chars: Some(10_000_001),
                ..IngestSettingsPatch::default()
            },
            IngestSettingsPatch {
                similar_crowd_size: Some(0),
                ..IngestSettingsPatch::default()
            },
            IngestSettingsPatch {
                similar_crowd_size: Some(1001),
                ..IngestSettingsPatch::default()
            },
        ];
        for mut patch in bad {
            patch.similar_min_jaccard = Some(0.5);
            let err = s.set_ingest_settings(&patch).unwrap_err();
            assert!(
                matches!(err, StoreError::InvalidSetting(_)),
                "{patch:?}: {err}"
            );
        }
        assert_eq!(s.ingest_settings().unwrap(), set);
        // A bad stored value reads as the default.
        s.set_setting(SIMILAR_CROWD_SIZE, &serde_json::json!(0))
            .unwrap();
        s.set_setting(SIMILAR_NEAR_COPY_MIN_JACCARD, &serde_json::json!(2))
            .unwrap();
        let now = s.ingest_settings().unwrap();
        assert_eq!(now.similar_crowd_size, DEFAULT_SIMILAR_CROWD_SIZE);
        assert!(
            (now.similar_near_copy_min_jaccard - DEFAULT_SIMILAR_NEAR_COPY_MIN_JACCARD).abs()
                < f64::EPSILON
        );
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
