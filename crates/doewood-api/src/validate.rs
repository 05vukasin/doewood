//! Input validation. Every request is checked before it reaches a store, and every problem is
//! reported at once: INVALID_ARGUMENT with the field paths in the message and in a
//! `google.rpc.BadRequest` detail.
//!
//! Messages never echo a value that may hold a secret (credentials, endpoint and base URLs,
//! which could carry `user:password@`).

use std::time::Duration;

use doewood_core::SourceId;
use doewood_model::Blake3;
use doewood_store::{LlmMode, LlmPolicy, MAX_LLM_CONCURRENCY, MAX_LLM_KINDS, S3Credentials};
use doewood_store::{UrlStyle, valid_kind};
use time::{Date, Month};
use tonic::{Code, Status};
use tonic_types::{ErrorDetails, FieldViolation, StatusExt as _};
use uuid::Uuid;

use crate::pb;

/// Longest endpoint or base URL accepted.
const URL_MAX_LEN: usize = 2048;
/// Longest scan interval accepted: a week.
pub(crate) const MAX_SCAN_INTERVAL_SECS: u64 = 7 * 24 * 3600;
/// Largest object size cap: S3's largest object, 5 TiB.
pub(crate) const MAX_OBJECT_BYTES: u64 = 5 * 1024 * 1024 * 1024 * 1024;

/// The problems found in one request.
#[derive(Debug, Default)]
pub(crate) struct Violations(Vec<FieldViolation>);

impl Violations {
    pub(crate) fn add(&mut self, field: impl Into<String>, why: impl Into<String>) {
        self.0.push(FieldViolation::new(field, why));
    }

    /// `Ok` when nothing was found, else INVALID_ARGUMENT listing everything.
    pub(crate) fn check(self) -> Result<(), Status> {
        if self.0.is_empty() {
            return Ok(());
        }
        let message = self
            .0
            .iter()
            .map(|v| format!("{}: {}", v.field, v.description))
            .collect::<Vec<_>>()
            .join("; ");
        Err(Status::with_error_details(
            Code::InvalidArgument,
            format!("invalid argument: {message}"),
            ErrorDetails::with_bad_request(self.0),
        ))
    }
}

/// A URL of an S3 endpoint or an API: `http(s)://host[:port][/path]`, no credentials, query
/// or fragment. Returns it normalized (scheme and host lowercase, default port dropped, no
/// trailing `/`).
pub(crate) fn http_url(v: &mut Violations, field: &str, value: &str) -> Option<String> {
    let mut bad = |why: &str| {
        v.add(field, why);
        None
    };
    let value = value.trim();
    if value.is_empty() {
        return bad("required");
    }
    if value.len() > URL_MAX_LEN {
        return bad("longer than 2048 bytes");
    }
    let Ok(url) = url::Url::parse(value) else {
        return bad("not a valid URL");
    };
    if !matches!(url.scheme(), "http" | "https") {
        return bad("must be an http:// or https:// URL");
    }
    if !url.username().is_empty() || url.password().is_some() {
        // Credentials belong in the write-only secret fields, never in a URL that gets logged.
        return bad("must not contain credentials (user:password@)");
    }
    if url.host_str().is_none_or(str::is_empty) {
        return bad("has no host");
    }
    if url.query().is_some() || url.fragment().is_some() {
        return bad("must not have a query or a fragment");
    }
    Some(url.as_str().trim_end_matches('/').to_owned())
}

/// A bucket name. Virtual-host style needs a DNS-compatible name (3 to 63 lowercase letters,
/// digits, `.` and `-`, starting and ending with a letter or digit, not an IP address);
/// path style also accepts the legacy names some stores allow (up to 255 characters, with
/// uppercase letters and `_`).
pub(crate) fn bucket(v: &mut Violations, field: &str, value: &str, style: UrlStyle) {
    let b = value.as_bytes();
    let edge_ok = |c: Option<&u8>| c.is_some_and(u8::is_ascii_alphanumeric);
    let strict = (3..=63).contains(&b.len())
        && b.iter()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || matches!(c, b'.' | b'-'))
        && edge_ok(b.first())
        && edge_ok(b.last())
        && !value.contains("..")
        && value.parse::<std::net::Ipv4Addr>().is_err();
    if strict {
        return;
    }
    if style == UrlStyle::VirtualHost {
        v.add(
            field,
            "virtual-host style needs 3 to 63 lowercase letters, digits, '.' or '-', starting \
             and ending with a letter or digit, and not an IP address",
        );
        return;
    }
    let legacy = (3..=255).contains(&b.len())
        && b.iter()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, b'.' | b'-' | b'_'))
        && edge_ok(b.first());
    if !legacy {
        v.add(
            field,
            "must be 3 to 255 ASCII letters, digits, '.', '-' or '_', starting with a letter or \
             digit",
        );
    }
}

/// A key prefix: at most 1024 bytes, no control characters.
pub(crate) fn prefix(v: &mut Violations, field: &str, value: &str) {
    if value.len() > 1024 {
        v.add(field, "longer than 1024 bytes");
    } else if value.contains(char::is_control) {
        v.add(field, "must not contain control characters");
    }
}

/// A signing region: empty for the default, else 1 to 64 ASCII letters, digits, `-` or `_`.
pub(crate) fn region(v: &mut Violations, field: &str, value: &str) -> Option<String> {
    let value = value.trim();
    if value.is_empty() {
        return None;
    }
    if value.len() > 64
        || !value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_'))
    {
        v.add(
            field,
            "must be 1 to 64 ASCII letters, digits, '-' or '_' (empty for us-east-1)",
        );
        return None;
    }
    Some(value.to_owned())
}

/// A URL style enum value.
pub(crate) fn url_style(v: &mut Violations, field: &str, value: i32) -> UrlStyle {
    match pb::UrlStyle::try_from(value) {
        Ok(pb::UrlStyle::Unspecified | pb::UrlStyle::Path) => UrlStyle::Path,
        Ok(pb::UrlStyle::VirtualHost) => UrlStyle::VirtualHost,
        Err(_) => {
            v.add(field, format!("unknown value {value}"));
            UrlStyle::Path
        }
    }
}

/// A scan interval in seconds: 0 is the default.
pub(crate) fn scan_interval(v: &mut Violations, field: &str, secs: u64) -> Duration {
    if secs > MAX_SCAN_INTERVAL_SECS {
        v.add(field, "at most 604800 (a week)");
    }
    match secs {
        0 => doewood_store::DEFAULT_SCAN_INTERVAL,
        s => Duration::from_secs(s.min(MAX_SCAN_INTERVAL_SECS)),
    }
}

/// An object size cap: 0 is the default.
pub(crate) fn max_object_bytes(v: &mut Violations, field: &str, bytes: u64) -> Option<u64> {
    if bytes > MAX_OBJECT_BYTES {
        v.add(field, "at most 5 TiB");
        return None;
    }
    (bytes > 0).then_some(bytes)
}

/// S3 credentials. Never echoes a value.
pub(crate) fn credentials(
    v: &mut Violations,
    field: &str,
    c: &pb::S3Credentials,
) -> Option<S3Credentials> {
    let mut ok = true;
    for (name, value, required, max) in [
        ("access_key_id", c.access_key_id.as_str(), true, 1024),
        (
            "secret_access_key",
            c.secret_access_key.as_str(),
            true,
            4096,
        ),
        ("session_token", c.session_token.as_str(), false, 8192),
    ] {
        let path = format!("{field}.{name}");
        if required && value.is_empty() {
            v.add(path, "required");
            ok = false;
        } else if value.len() > max {
            v.add(path, format!("longer than {max} bytes"));
            ok = false;
        } else if value.contains(char::is_control) {
            v.add(path, "must not contain control characters or line breaks");
            ok = false;
        }
    }
    if !ok {
        return None;
    }
    let token = (!c.session_token.is_empty()).then(|| c.session_token.clone());
    let creds = S3Credentials::new(c.access_key_id.clone(), c.secret_access_key.clone(), token);
    if creds.is_err() {
        v.add(field, "invalid credentials");
    }
    creds.ok()
}

/// An LLM policy; unset is the default (off).
pub(crate) fn llm_policy(v: &mut Violations, field: &str, p: Option<&pb::LlmPolicy>) -> LlmPolicy {
    let Some(p) = p else {
        return LlmPolicy::default();
    };
    let mode = match pb::LlmMode::try_from(p.mode) {
        Ok(pb::LlmMode::Unspecified | pb::LlmMode::Off) => LlmMode::Off,
        Ok(pb::LlmMode::All) => LlmMode::All,
        Ok(pb::LlmMode::OnDemand) => LlmMode::OnDemand,
        Ok(pb::LlmMode::Kinds) => LlmMode::Kinds(p.kinds.clone()),
        Err(_) => {
            v.add(format!("{field}.mode"), format!("unknown value {}", p.mode));
            LlmMode::Off
        }
    };
    match &mode {
        LlmMode::Kinds(kinds) => {
            if kinds.is_empty() || kinds.len() > MAX_LLM_KINDS {
                v.add(
                    format!("{field}.kinds"),
                    format!("LLM_MODE_KINDS needs 1 to {MAX_LLM_KINDS} kinds"),
                );
            }
            for (i, k) in kinds.iter().enumerate() {
                if !valid_kind(k) {
                    v.add(
                        format!("{field}.kinds[{i}]"),
                        "must be 1 to 64 ASCII letters, digits, '_', '-' or '.'",
                    );
                }
            }
        }
        _ if !p.kinds.is_empty() => {
            v.add(format!("{field}.kinds"), "only with LLM_MODE_KINDS");
        }
        _ => {}
    }
    if p.concurrency > MAX_LLM_CONCURRENCY {
        v.add(
            format!("{field}.concurrency"),
            format!("at most {MAX_LLM_CONCURRENCY}"),
        );
    }
    LlmPolicy {
        mode,
        concurrency: match p.concurrency {
            0 => doewood_store::DEFAULT_LLM_CONCURRENCY,
            n => n.min(MAX_LLM_CONCURRENCY),
        },
        daily_token_budget: (p.daily_token_budget > 0).then_some(p.daily_token_budget),
    }
}

/// A source id (a UUID).
pub(crate) fn source_id(v: &mut Violations, field: &str, value: &str) -> Option<SourceId> {
    if value.is_empty() {
        v.add(field, "required");
        return None;
    }
    let parsed = value.parse().ok();
    if parsed.is_none() {
        v.add(field, "not a source id (a UUID)");
    }
    parsed
}

/// A node id (a UUID).
pub(crate) fn uuid(v: &mut Violations, field: &str, value: &str) -> Option<Uuid> {
    if value.is_empty() {
        v.add(field, "required");
        return None;
    }
    let parsed = Uuid::parse_str(value).ok();
    if parsed.is_none() {
        v.add(field, "not a UUID");
    }
    parsed
}

/// A BLAKE3 hash in hex.
pub(crate) fn blake3(v: &mut Violations, field: &str, value: &str) -> Option<Blake3> {
    if value.is_empty() {
        v.add(field, "required");
        return None;
    }
    let parsed = value.parse().ok();
    if parsed.is_none() {
        v.add(field, "not a BLAKE3 hash (64 hex digits)");
    }
    parsed
}

/// A `YYYY-MM-DD` date; empty is none.
pub(crate) fn date(v: &mut Violations, field: &str, value: &str) -> Option<Date> {
    if value.is_empty() {
        return None;
    }
    let parsed = parse_date(value);
    if parsed.is_none() {
        v.add(field, "not a YYYY-MM-DD date");
    }
    parsed
}

fn parse_date(s: &str) -> Option<Date> {
    let b = s.as_bytes();
    if b.len() != 10 || b[4] != b'-' || b[7] != b'-' {
        return None;
    }
    let num = |r: std::ops::Range<usize>| -> Option<u32> {
        let part = s.get(r)?;
        part.bytes()
            .all(|c| c.is_ascii_digit())
            .then(|| part.parse().ok())?
    };
    let year = i32::try_from(num(0..4)?).ok()?;
    let month = Month::try_from(u8::try_from(num(5..7)?).ok()?).ok()?;
    let day = u8::try_from(num(8..10)?).ok()?;
    Date::from_calendar_date(year, month, day).ok()
}

/// A date as `YYYY-MM-DD`.
pub(crate) fn format_date(d: Date) -> String {
    format!("{:04}-{:02}-{:02}", d.year(), u8::from(d.month()), d.day())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn urls_are_normalized_and_never_echoed() {
        let mut v = Violations::default();
        assert_eq!(
            http_url(&mut v, "endpoint", " HTTP://MinIO:9000/ ").as_deref(),
            Some("http://minio:9000")
        );
        assert_eq!(
            http_url(&mut v, "endpoint", "https://gw.example/s3/").as_deref(),
            Some("https://gw.example/s3")
        );
        assert_eq!(
            http_url(&mut v, "endpoint", "http://host:80").as_deref(),
            Some("http://host")
        );
        assert!(v.0.is_empty());
        for bad in [
            "",
            "ftp://x",
            "http://user:topsecret@host",
            "http://user@host",
            "http://host/?x=1",
            "http://host/#f",
            "not a url",
        ] {
            let mut v = Violations::default();
            assert!(http_url(&mut v, "endpoint", bad).is_none(), "{bad}");
            let status = v.check().unwrap_err();
            assert_eq!(status.code(), Code::InvalidArgument);
            assert!(status.message().starts_with("invalid argument: endpoint: "));
            assert!(
                !status.message().contains("topsecret"),
                "{}",
                status.message()
            );
        }
    }

    #[test]
    fn bucket_rules() {
        let check = |b: &str, style| {
            let mut v = Violations::default();
            bucket(&mut v, "bucket", b, style);
            v.0.is_empty()
        };
        for ok in ["docs", "my-bucket.eu", "a1b"] {
            assert!(check(ok, UrlStyle::VirtualHost), "{ok}");
        }
        for bad in [
            "ab",
            "Docs",
            "a..b",
            "-ab",
            "ab-",
            "1.2.3.4",
            "a_b",
            &"a".repeat(64),
        ] {
            assert!(!check(bad, UrlStyle::VirtualHost), "{bad}");
        }
        // Path style also accepts legacy names.
        assert!(check("Legacy_Bucket", UrlStyle::Path));
        assert!(!check("has space", UrlStyle::Path));
        assert!(!check("", UrlStyle::Path));
    }

    #[test]
    fn violations_are_all_reported() {
        let mut v = Violations::default();
        let creds = pb::S3Credentials {
            access_key_id: String::new(),
            secret_access_key: "sec\nret".into(),
            session_token: String::new(),
        };
        assert!(credentials(&mut v, "credentials", &creds).is_none());
        region(&mut v, "region", "bad region!");
        url_style(&mut v, "url_style", 9);
        let status = v.check().unwrap_err();
        let msg = status.message();
        assert!(msg.contains("credentials.access_key_id: required"), "{msg}");
        assert!(msg.contains("credentials.secret_access_key: "), "{msg}");
        assert!(msg.contains("region: "), "{msg}");
        assert!(msg.contains("url_style: unknown value 9"), "{msg}");
        assert!(!msg.contains("sec\nret"), "{msg}");
        let details = status.get_error_details();
        assert_eq!(details.bad_request().unwrap().field_violations.len(), 4);
    }

    #[test]
    fn policies_and_dates() {
        let mut v = Violations::default();
        let p = llm_policy(
            &mut v,
            "llm_policy",
            Some(&pb::LlmPolicy {
                mode: pb::LlmMode::Kinds as i32,
                kinds: vec!["invoice".into(), "bad kind".into()],
                concurrency: 0,
                daily_token_budget: 0,
            }),
        );
        assert_eq!(p.concurrency, 2);
        assert_eq!(p.daily_token_budget, None);
        let msg = v.check().unwrap_err().message().to_owned();
        assert!(msg.contains("llm_policy.kinds[1]"), "{msg}");
        let mut v = Violations::default();
        llm_policy(
            &mut v,
            "p",
            Some(&pb::LlmPolicy {
                mode: pb::LlmMode::All as i32,
                kinds: vec!["x".into()],
                concurrency: 65,
                daily_token_budget: 0,
            }),
        );
        let msg = v.check().unwrap_err().message().to_owned();
        assert!(
            msg.contains("p.kinds: only with") && msg.contains("p.concurrency"),
            "{msg}"
        );

        assert_eq!(
            parse_date("2026-02-28").map(format_date).as_deref(),
            Some("2026-02-28")
        );
        for bad in [
            "2026-02-30",
            "2026-2-28",
            "26-02-28x",
            "2026-13-01",
            "abcd-ef-gh",
        ] {
            assert!(parse_date(bad).is_none(), "{bad}");
        }
    }
}
