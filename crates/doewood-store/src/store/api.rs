//! The API server's runtime limits (ADR 0001, section 3): set through the API, kept in the
//! `settings` table, applied to the next call without a restart.
//!
//! - `api.max_calls_in_flight`: calls the gRPC API runs at the same time before it sheds new
//!   ones with UNAVAILABLE. Unset means [`DEFAULT_MAX_CALLS_IN_FLIGHT`].
//! - `api.deadline_seconds`: the server's deadline for a call (a stream: for its start).
//!   Unset means [`DEFAULT_DEADLINE_SECONDS`].
//!
//! The connection-level limits (HTTP/2 streams per connection, keepalive, connection age)
//! are fixed when the listener starts, so they are constants of `doewood-api`, not settings.

use super::Store;
use crate::StoreError;

/// Settings key of the in-flight cap.
pub const API_MAX_CALLS_IN_FLIGHT: &str = "api.max_calls_in_flight";
/// Settings key of the server deadline.
pub const API_DEADLINE_SECONDS: &str = "api.deadline_seconds";

/// The in-flight cap when none is set.
pub const DEFAULT_MAX_CALLS_IN_FLIGHT: u32 = 256;
/// The server deadline when none is set, in seconds.
pub const DEFAULT_DEADLINE_SECONDS: u32 = 60;
/// Largest in-flight cap accepted.
pub const MAX_CALLS_IN_FLIGHT_LIMIT: u32 = 65_536;
/// Longest server deadline accepted, in seconds (one hour).
pub const DEADLINE_SECONDS_LIMIT: u32 = 3600;

/// Whether `n` is a usable in-flight cap: 1 to [`MAX_CALLS_IN_FLIGHT_LIMIT`].
pub fn valid_max_calls_in_flight(n: u32) -> bool {
    (1..=MAX_CALLS_IN_FLIGHT_LIMIT).contains(&n)
}

/// Whether `s` is a usable server deadline: 1 to [`DEADLINE_SECONDS_LIMIT`] seconds.
pub fn valid_deadline_seconds(s: u32) -> bool {
    (1..=DEADLINE_SECONDS_LIMIT).contains(&s)
}

/// The API server's runtime limits, as stored (defaults filled in).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ApiSettings {
    /// Calls run at the same time before new ones are shed.
    pub max_calls_in_flight: u32,
    /// The server's deadline for a call, in seconds.
    pub deadline_seconds: u32,
}

impl Default for ApiSettings {
    fn default() -> Self {
        Self {
            max_calls_in_flight: DEFAULT_MAX_CALLS_IN_FLIGHT,
            deadline_seconds: DEFAULT_DEADLINE_SECONDS,
        }
    }
}

/// A change to [`ApiSettings`]: `None` keeps the stored value.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ApiSettingsPatch {
    /// The new in-flight cap.
    pub max_calls_in_flight: Option<u32>,
    /// The new server deadline, in seconds.
    pub deadline_seconds: Option<u32>,
}

impl Store {
    /// The API server's limits. A missing row gives the default; so does a stored value out
    /// of range (only possible by editing the file by hand).
    pub fn api_settings(&self) -> Result<ApiSettings, StoreError> {
        let read = |key: &str, valid: fn(u32) -> bool, default: u32| -> Result<u32, StoreError> {
            Ok(self
                .setting(key)?
                .and_then(|v| v.as_u64())
                .and_then(|n| u32::try_from(n).ok())
                .filter(|n| valid(*n))
                .unwrap_or(default))
        };
        Ok(ApiSettings {
            max_calls_in_flight: read(
                API_MAX_CALLS_IN_FLIGHT,
                valid_max_calls_in_flight,
                DEFAULT_MAX_CALLS_IN_FLIGHT,
            )?,
            deadline_seconds: read(
                API_DEADLINE_SECONDS,
                valid_deadline_seconds,
                DEFAULT_DEADLINE_SECONDS,
            )?,
        })
    }

    /// Applies `patch` and returns the settings now in effect. An invalid value changes
    /// nothing, not even the other field.
    pub fn set_api_settings(&self, patch: &ApiSettingsPatch) -> Result<ApiSettings, StoreError> {
        if let Some(n) = patch.max_calls_in_flight
            && !valid_max_calls_in_flight(n)
        {
            return Err(StoreError::InvalidSetting(format!(
                "{API_MAX_CALLS_IN_FLIGHT} must be 1 to {MAX_CALLS_IN_FLIGHT_LIMIT}"
            )));
        }
        if let Some(s) = patch.deadline_seconds
            && !valid_deadline_seconds(s)
        {
            return Err(StoreError::InvalidSetting(format!(
                "{API_DEADLINE_SECONDS} must be 1 to {DEADLINE_SECONDS_LIMIT}"
            )));
        }
        if let Some(n) = patch.max_calls_in_flight {
            self.set_setting(API_MAX_CALLS_IN_FLIGHT, &serde_json::json!(n))?;
        }
        if let Some(s) = patch.deadline_seconds {
            self.set_setting(API_DEADLINE_SECONDS, &serde_json::json!(s))?;
        }
        self.api_settings()
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
    fn default_until_set() {
        let s = store();
        let d = s.api_settings().unwrap();
        assert_eq!(d, ApiSettings::default());
        assert_eq!(d.max_calls_in_flight, 256);
        assert_eq!(d.deadline_seconds, 60);
        assert_eq!(
            s.set_api_settings(&ApiSettingsPatch::default()).unwrap(),
            ApiSettings::default()
        );
        assert_eq!(s.setting(API_MAX_CALLS_IN_FLIGHT).unwrap(), None);
        assert_eq!(s.setting(API_DEADLINE_SECONDS).unwrap(), None);
    }

    #[test]
    fn set_one_keeps_the_other() {
        let s = store();
        let set = s
            .set_api_settings(&ApiSettingsPatch {
                max_calls_in_flight: Some(8),
                deadline_seconds: None,
            })
            .unwrap();
        assert_eq!(set.max_calls_in_flight, 8);
        assert_eq!(set.deadline_seconds, DEFAULT_DEADLINE_SECONDS);
        let set = s
            .set_api_settings(&ApiSettingsPatch {
                max_calls_in_flight: None,
                deadline_seconds: Some(3600),
            })
            .unwrap();
        assert_eq!(
            set,
            ApiSettings {
                max_calls_in_flight: 8,
                deadline_seconds: 3600,
            }
        );
        assert_eq!(s.api_settings().unwrap(), set);
    }

    #[test]
    fn invalid_values_are_refused_and_change_nothing() {
        let s = store();
        for patch in [
            ApiSettingsPatch {
                max_calls_in_flight: Some(0),
                deadline_seconds: Some(5),
            },
            ApiSettingsPatch {
                max_calls_in_flight: Some(MAX_CALLS_IN_FLIGHT_LIMIT + 1),
                deadline_seconds: None,
            },
            ApiSettingsPatch {
                max_calls_in_flight: Some(5),
                deadline_seconds: Some(0),
            },
            ApiSettingsPatch {
                max_calls_in_flight: None,
                deadline_seconds: Some(DEADLINE_SECONDS_LIMIT + 1),
            },
        ] {
            let err = s.set_api_settings(&patch).unwrap_err();
            assert!(matches!(err, StoreError::InvalidSetting(_)), "{patch:?}");
        }
        assert_eq!(s.api_settings().unwrap(), ApiSettings::default());
    }

    #[test]
    fn a_bad_stored_value_reads_as_the_default() {
        let s = store();
        for bad in [
            serde_json::json!("9"),
            serde_json::json!(0),
            serde_json::json!(-1),
            serde_json::json!(1.5),
            serde_json::json!(u64::MAX),
            serde_json::json!(null),
        ] {
            s.set_setting(API_MAX_CALLS_IN_FLIGHT, &bad).unwrap();
            s.set_setting(API_DEADLINE_SECONDS, &bad).unwrap();
            assert_eq!(s.api_settings().unwrap(), ApiSettings::default(), "{bad}");
        }
    }
}
