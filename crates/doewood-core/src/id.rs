//! Typed ids: the instance, sources and jobs.

use std::fmt;
use std::str::FromStr;

use uuid::Uuid;

use crate::Error;

/// Maximum length of an instance id: a DNS label, like a `StatefulSet` pod name.
pub const INSTANCE_ID_MAX_LEN: usize = 63;

/// The name of this instance. It owns source claims in the shared database, so it must be
/// stable across restarts (the `StatefulSet` pod name is).
///
/// Format: 1 to 63 ASCII letters, digits, `-`, `_` or `.`, starting and ending with a letter or
/// digit.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct InstanceId(String);

impl InstanceId {
    /// Validates an instance id.
    pub fn new(value: impl Into<String>) -> Result<Self, Error> {
        let value = value.into();
        let invalid = |reason: &str| Error::InvalidId {
            kind: "instance id",
            reason: reason.to_owned(),
        };
        if value.is_empty() {
            return Err(invalid("it is empty"));
        }
        if value.len() > INSTANCE_ID_MAX_LEN {
            return Err(invalid("it is longer than 63 characters"));
        }
        if !value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.'))
        {
            return Err(invalid(
                "only ASCII letters, digits, '-', '_' and '.' are allowed",
            ));
        }
        let edges_ok = value
            .bytes()
            .next()
            .is_some_and(|b| b.is_ascii_alphanumeric())
            && value
                .bytes()
                .last()
                .is_some_and(|b| b.is_ascii_alphanumeric());
        if !edges_ok {
            return Err(invalid("it must start and end with a letter or digit"));
        }
        Ok(Self(value))
    }

    /// [`crate::config::FALLBACK_INSTANCE_ID`], used when the hostname is not a valid id.
    pub(crate) fn fallback() -> Self {
        Self(crate::config::FALLBACK_INSTANCE_ID.to_owned())
    }

    /// The id as text.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for InstanceId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl FromStr for InstanceId {
    type Err = Error;
    fn from_str(s: &str) -> Result<Self, Error> {
        Self::new(s)
    }
}

macro_rules! uuid_id {
    ($(#[$doc:meta])* $name:ident, $kind:literal) => {
        $(#[$doc])*
        ///
        /// A UUIDv7, so ids sort by creation time.
        #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
        pub struct $name(Uuid);

        impl $name {
            /// A new id for the current time.
            pub fn generate() -> Self {
                Self(Uuid::now_v7())
            }

            /// The underlying UUID.
            pub fn as_uuid(&self) -> Uuid {
                self.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                self.0.hyphenated().fmt(f)
            }
        }

        impl FromStr for $name {
            type Err = Error;
            fn from_str(s: &str) -> Result<Self, Error> {
                Uuid::parse_str(s).map(Self).map_err(|e| Error::InvalidId {
                    kind: $kind,
                    reason: e.to_string(),
                })
            }
        }
    };
}

uuid_id!(
    /// The id of a source: one S3-compatible endpoint, bucket and prefix.
    SourceId,
    "source id"
);
uuid_id!(
    /// The id of a job (a source's first pass and watch, or a one-shot reprocess).
    JobId,
    "job id"
);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn instance_id_accepts_pod_names() {
        for ok in ["doewood-0", "a", "node1.example", "x_y", &"a".repeat(63)] {
            assert!(InstanceId::new(ok).is_ok(), "{ok}");
        }
    }

    #[test]
    fn instance_id_refuses_bad_values() {
        for bad in ["", "-a", "a-", "has space", "ünï", "a/b", &"a".repeat(64)] {
            assert!(InstanceId::new(bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn uuid_ids_round_trip_and_sort_by_time() {
        let a = SourceId::generate();
        let b = SourceId::generate();
        assert!(a < b);
        assert_eq!(a.to_string().parse::<SourceId>().unwrap(), a);
        assert!("nope".parse::<JobId>().is_err());
    }
}
