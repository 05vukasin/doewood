//! VIES answers, parsed into types. The JSON of `check-vat-number` is read leniently (every
//! field optional, unknown fields ignored), then checked: it must say whether the number is
//! valid, and it must be about the number asked.

use doewood_enrich::Identifier;
use serde::Deserialize;
use time::Date;

use crate::parse::{date, text};
use crate::warning::WarningKind;

/// A VIES check of one VAT number.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VatRecord {
    /// The VAT number as VIES knows it: the VIES prefix (`EL` for Greece, `XI` for Northern
    /// Ireland) and the number, validated by `doewood_enrich` before it was sent.
    pub vat: Identifier,
    /// Whether the member state says the number is registered and active for intra-EU trade.
    pub valid: bool,
    /// The registered name, when the member state gives it (some never do; VIES then sends
    /// `---`, read as `None`). Always `None` for an invalid number.
    pub name: Option<String>,
    /// The registered address, lines separated by `\n`, under the same rule as `name`.
    pub address: Option<String>,
    /// The day VIES says it made the check (`requestDate`).
    pub request_date: Option<Date>,
    /// VIES's consultation number (`requestIdentifier`). VIES gives one only to a requester
    /// that sends its own VAT number, which doewood does not, so it is normally `None`.
    pub request_identifier: Option<String>,
}

impl VatRecord {
    /// The VIES country prefix (`EL`, `XI`, `DE`, ...).
    pub fn country(&self) -> &str {
        &self.vat.value[..2]
    }

    /// The number without its prefix, as sent to VIES.
    pub fn number(&self) -> &str {
        &self.vat.value[2..]
    }
}

/// A VIES error code: VIES could not say whether the number is valid.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum ViesError {
    /// `INVALID_INPUT`: VIES refused the country code or the number.
    InvalidInput,
    /// `INVALID_REQUESTER_INFO`: VIES refused the requester fields (doewood sends none).
    InvalidRequesterInfo,
    /// `SERVICE_UNAVAILABLE`: VIES itself is down.
    ServiceUnavailable,
    /// `MS_UNAVAILABLE`: the member state's database is down (often for hours, or a scheduled
    /// maintenance window).
    MsUnavailable,
    /// `TIMEOUT`: the member state did not answer VIES in time.
    Timeout,
    /// `VAT_BLOCKED`: the number may not be checked through VIES.
    VatBlocked,
    /// `IP_BLOCKED`: VIES refuses this instance's address.
    IpBlocked,
    /// `GLOBAL_MAX_CONCURRENT_REQ`: too many requests to VIES overall.
    GlobalMaxConcurrentReq,
    /// `GLOBAL_MAX_CONCURRENT_REQ_TIME`: too many requests to VIES overall in a time window.
    GlobalMaxConcurrentReqTime,
    /// `MS_MAX_CONCURRENT_REQ`: too many requests to this member state.
    MsMaxConcurrentReq,
    /// `MS_MAX_CONCURRENT_REQ_TIME`: too many requests to this member state in a time window.
    MsMaxConcurrentReqTime,
    /// Any other code, as sent (at most 64 characters).
    Other(String),
}

impl ViesError {
    /// The error of a VIES code.
    pub fn from_code(code: &str) -> Self {
        match code.trim() {
            "INVALID_INPUT" => Self::InvalidInput,
            "INVALID_REQUESTER_INFO" => Self::InvalidRequesterInfo,
            "SERVICE_UNAVAILABLE" => Self::ServiceUnavailable,
            "MS_UNAVAILABLE" => Self::MsUnavailable,
            "TIMEOUT" => Self::Timeout,
            "VAT_BLOCKED" => Self::VatBlocked,
            "IP_BLOCKED" => Self::IpBlocked,
            "GLOBAL_MAX_CONCURRENT_REQ" => Self::GlobalMaxConcurrentReq,
            "GLOBAL_MAX_CONCURRENT_REQ_TIME" => Self::GlobalMaxConcurrentReqTime,
            "MS_MAX_CONCURRENT_REQ" => Self::MsMaxConcurrentReq,
            "MS_MAX_CONCURRENT_REQ_TIME" => Self::MsMaxConcurrentReqTime,
            other => Self::Other(other.chars().take(64).collect()),
        }
    }

    /// The VIES code.
    pub fn code(&self) -> &str {
        match self {
            Self::InvalidInput => "INVALID_INPUT",
            Self::InvalidRequesterInfo => "INVALID_REQUESTER_INFO",
            Self::ServiceUnavailable => "SERVICE_UNAVAILABLE",
            Self::MsUnavailable => "MS_UNAVAILABLE",
            Self::Timeout => "TIMEOUT",
            Self::VatBlocked => "VAT_BLOCKED",
            Self::IpBlocked => "IP_BLOCKED",
            Self::GlobalMaxConcurrentReq => "GLOBAL_MAX_CONCURRENT_REQ",
            Self::GlobalMaxConcurrentReqTime => "GLOBAL_MAX_CONCURRENT_REQ_TIME",
            Self::MsMaxConcurrentReq => "MS_MAX_CONCURRENT_REQ",
            Self::MsMaxConcurrentReqTime => "MS_MAX_CONCURRENT_REQ_TIME",
            Self::Other(code) => code,
        }
    }

    /// Too many requests, to VIES or to the member state: worth a retry after a wait, with
    /// the rate limiter emptied.
    pub fn is_busy(&self) -> bool {
        matches!(
            self,
            Self::GlobalMaxConcurrentReq
                | Self::GlobalMaxConcurrentReqTime
                | Self::MsMaxConcurrentReq
                | Self::MsMaxConcurrentReqTime
        )
    }

    /// Whether asking again later may give an answer: everything but a refusal of the input,
    /// of the number or of this instance.
    pub fn is_transient(&self) -> bool {
        !matches!(
            self,
            Self::InvalidInput | Self::InvalidRequesterInfo | Self::VatBlocked | Self::IpBlocked
        )
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct RawError {
    action_succeed: Option<bool>,
    error_wrappers: Option<Vec<RawErrorWrapper>>,
    /// Not in the `check-vat-number` schema; VIES's other JSON answers carry their error code
    /// here. Read so that such an answer is never taken for "invalid".
    user_error: Option<String>,
}

#[derive(Deserialize)]
struct RawErrorWrapper {
    error: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct RawCheck {
    country_code: Option<String>,
    vat_number: Option<String>,
    request_date: Option<String>,
    #[serde(alias = "isValid")]
    valid: Option<bool>,
    request_identifier: Option<String>,
    name: Option<String>,
    address: Option<String>,
}

/// The VIES error an answer reports, if any: an `errorWrappers` entry, `actionSucceed: false`,
/// or a `userError` other than `VALID` and `INVALID`.
pub(crate) fn error(body: &[u8]) -> Option<ViesError> {
    let raw: RawError = serde_json::from_slice(body).ok()?;
    if let Some(first) = raw.error_wrappers.as_deref().and_then(<[_]>::first) {
        let code = first.error.as_deref().unwrap_or("");
        return Some(ViesError::from_code(code));
    }
    if raw.action_succeed == Some(false) {
        return Some(ViesError::Other(String::new()));
    }
    match raw.user_error.as_deref().map(str::trim) {
        None | Some("" | "VALID" | "INVALID") => None,
        Some(code) => Some(ViesError::from_code(code)),
    }
}

/// The check of `vat` (validated, VIES prefix first) in a `check-vat-number` answer.
pub(crate) fn check(body: &[u8], vat: &Identifier) -> Result<VatRecord, WarningKind> {
    if let Some(e) = error(body) {
        return Err(WarningKind::Vies(e));
    }
    let raw: RawCheck =
        serde_json::from_slice(body).map_err(|e| WarningKind::Malformed(e.to_string()))?;
    let (prefix, number) = vat.value.split_at(2);
    let valid = raw
        .valid
        .ok_or_else(|| WarningKind::Malformed("no `valid` field".to_owned()))?;
    if let Some(c) = text(raw.country_code) {
        let c = c.to_ascii_uppercase();
        if c != prefix && !(prefix == "EL" && c == "GR") {
            return Err(WarningKind::Malformed(format!(
                "answer for country {c} to a check of {prefix}"
            )));
        }
    }
    if let Some(n) = text(raw.vat_number) {
        let n: String = n
            .chars()
            .filter(char::is_ascii_alphanumeric)
            .map(|c| c.to_ascii_uppercase())
            .collect();
        let n = n.strip_prefix(prefix).unwrap_or(&n);
        if n != number {
            return Err(WarningKind::Malformed(
                "answer for another VAT number".to_owned(),
            ));
        }
    }
    let shown = |s: Option<String>| text(s).filter(|_| valid).filter(|s| !withheld(s));
    Ok(VatRecord {
        vat: vat.clone(),
        valid,
        name: shown(raw.name),
        address: shown(raw.address),
        request_date: date(raw.request_date.as_deref()),
        request_identifier: text(raw.request_identifier),
    })
}

/// VIES writes a name or address the member state does not share as `---`.
fn withheld(s: &str) -> bool {
    s.chars().all(|c| c == '-')
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn error_codes_round_trip() {
        for code in [
            "INVALID_INPUT",
            "INVALID_REQUESTER_INFO",
            "SERVICE_UNAVAILABLE",
            "MS_UNAVAILABLE",
            "TIMEOUT",
            "VAT_BLOCKED",
            "IP_BLOCKED",
            "GLOBAL_MAX_CONCURRENT_REQ",
            "GLOBAL_MAX_CONCURRENT_REQ_TIME",
            "MS_MAX_CONCURRENT_REQ",
            "MS_MAX_CONCURRENT_REQ_TIME",
            "SOMETHING_NEW",
        ] {
            assert_eq!(ViesError::from_code(code).code(), code);
        }
        assert_eq!(
            ViesError::from_code(&"X".repeat(100)),
            ViesError::Other("X".repeat(64))
        );
        assert!(ViesError::MsMaxConcurrentReq.is_busy());
        assert!(!ViesError::MsUnavailable.is_busy());
        assert!(ViesError::MsUnavailable.is_transient());
        assert!(!ViesError::VatBlocked.is_transient());
    }

    #[test]
    fn error_shapes() {
        let e = |s: &str| error(s.as_bytes());
        assert_eq!(
            e(r#"{"actionSucceed":false,"errorWrappers":[{"error":"TIMEOUT","message":"x"}]}"#),
            Some(ViesError::Timeout)
        );
        assert_eq!(
            e(r#"{"actionSucceed":false}"#),
            Some(ViesError::Other(String::new()))
        );
        assert_eq!(
            e(r#"{"isValid":false,"userError":"MS_UNAVAILABLE"}"#),
            Some(ViesError::MsUnavailable)
        );
        assert_eq!(e(r#"{"isValid":false,"userError":"INVALID"}"#), None);
        assert_eq!(e(r#"{"valid":true,"errorWrappers":[]}"#), None);
        assert_eq!(e("not json"), None);
        assert_eq!(e(r#"{"errorWrappers":"nope"}"#), None);
    }

    #[test]
    fn withheld_marks() {
        assert!(withheld("---"));
        assert!(withheld("-"));
        assert!(!withheld("A-B"));
    }
}
