//! EDGAR answers, parsed into types: the submissions JSON of `data.sec.gov` (read leniently:
//! every field optional, unknown fields ignored, then checked), the company tickers file, and
//! the identifiers (CIK, accession number, document file name) validated before any request.

use std::fmt;

use serde::Deserialize;
use serde_json::Value;
use time::Date;

use super::html::find_ci;
use crate::parse::{date, text};
use crate::trust::TrustTier;

/// A Central Index Key: 1 to 9,999,999,999, written zero-padded to 10 digits.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Cik(u64);

impl Cik {
    /// The CIK `n`, if it is one (1 to 9,999,999,999).
    pub fn new(n: u64) -> Option<Self> {
        (1..=9_999_999_999).contains(&n).then_some(Self(n))
    }

    /// `320193`, `0000320193` or `CIK0000320193`: 1 to 10 ASCII digits, optionally after
    /// `CIK`, surrounding whitespace ignored. Anything else is `None`.
    pub fn parse(s: &str) -> Option<Self> {
        let t = s.trim();
        let t = match t.get(..3) {
            Some(p) if p.eq_ignore_ascii_case("cik") => &t[3..],
            _ => t,
        };
        if t.is_empty() || t.len() > 10 || !t.bytes().all(|b| b.is_ascii_digit()) {
            return None;
        }
        Self::new(t.parse().ok()?)
    }

    /// The number.
    pub fn value(self) -> u64 {
        self.0
    }
}

impl fmt::Display for Cik {
    /// Zero-padded to 10 digits, as the submissions API wants it.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:010}", self.0)
    }
}

/// An accession number: `0000000000-00-000000` (the filer agent's CIK, the year, a sequence).
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Accession(String);

impl Accession {
    /// `0009990000-26-000012`, or its 18 digits without dashes.
    pub fn parse(s: &str) -> Option<Self> {
        let t = s.trim();
        let digits: String = match t.len() {
            18 => t.to_owned(),
            20 if t.as_bytes()[10] == b'-' && t.as_bytes()[13] == b'-' => {
                t.chars().filter(|c| *c != '-').collect()
            }
            _ => return None,
        };
        (digits.len() == 18 && digits.bytes().all(|b| b.is_ascii_digit())).then_some(Self(digits))
    }

    /// The 18 digits without dashes (the archive folder name).
    pub fn plain(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for Accession {
    /// With dashes: `0009990000-26-000012`.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}-{}-{}", &self.0[..10], &self.0[10..12], &self.0[12..])
    }
}

/// A document file name inside a filing folder: 1 to 128 of `A-Z a-z 0-9 . _ -`, not
/// starting with a dot. Nothing else is ever put in an archive URL.
pub(crate) fn safe_file(s: &str) -> Option<String> {
    let t = s.trim();
    let ok = !t.is_empty()
        && t.len() <= 128
        && !t.starts_with('.')
        && !t.contains("..")
        && t.bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-'));
    ok.then(|| t.to_owned())
}

/// Why the SEC refused a request (an HTTP 403).
#[derive(Clone, Debug, PartialEq, Eq, Hash, thiserror::Error)]
pub enum EdgarRefusal {
    /// The SEC's "Undeclared Automated Tool" page: the User-Agent does not name the requester
    /// and a contact. Set `EdgarConfig::contact`.
    #[error("undeclared automated tool (set the EDGAR User-Agent contact)")]
    UndeclaredUserAgent,
    /// The SEC's "Request Rate Threshold Exceeded" page: more than 10 requests a second.
    #[error("request rate threshold exceeded")]
    RateThreshold,
    /// Any other 403.
    #[error("forbidden")]
    Forbidden,
}

impl EdgarRefusal {
    /// The refusal a 403 body describes.
    pub fn from_body(body: &[u8]) -> Self {
        let head = String::from_utf8_lossy(&body[..body.len().min(64 * 1024)]);
        if find_ci(&head, "undeclared automated tool").is_some() {
            Self::UndeclaredUserAgent
        } else if find_ci(&head, "request rate threshold").is_some() {
            Self::RateThreshold
        } else {
            Self::Forbidden
        }
    }
}

/// A company's EDGAR record (the submissions API).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Company {
    /// The CIK.
    pub cik: Cik,
    /// The current name.
    pub name: String,
    /// `operating`, `other`, ... as sent.
    pub entity_type: Option<String>,
    /// The SIC code (four digits).
    pub sic: Option<String>,
    /// The SIC code's description.
    pub sic_description: Option<String>,
    /// Ticker symbols, in the SEC's order.
    pub tickers: Vec<String>,
    /// The exchanges of those tickers, in the same order (as sent; may be shorter).
    pub exchanges: Vec<String>,
    /// The state (or country) of incorporation code (`DE`, `E9`, ...).
    pub state_of_incorporation: Option<String>,
    /// Its description, when the SEC gives one.
    pub state_of_incorporation_description: Option<String>,
    /// The fiscal year end, `MMDD`.
    pub fiscal_year_end: Option<String>,
    /// Former names, with the dates they were used.
    pub former_names: Vec<FormerName>,
    /// The recent filings (at least a year, or the last 1,000), newest first as sent.
    pub filings: Vec<Filing>,
    /// The names of the files that hold the older filings (not fetched yet).
    pub older_filing_files: Vec<String>,
    /// Always [`TrustTier::Official`]: SEC filings.
    pub tier: TrustTier,
}

impl Company {
    /// The 8-K filings that report Item 2.01, "Completion of Acquisition or Disposition of
    /// Assets", newest first.
    pub fn acquisitions(&self) -> impl Iterator<Item = &Filing> {
        self.filings.iter().filter(|f| f.is_completed_acquisition())
    }

    /// The newest annual report (10-K, 10-K405, 10-KT and their amendments) by filing date.
    pub fn latest_annual_report(&self) -> Option<&Filing> {
        self.filings
            .iter()
            .filter(|f| f.is_annual_report())
            .max_by_key(|f| f.filing_date)
    }
}

/// A name the company used before, with the period the SEC gives.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FormerName {
    /// The former name.
    pub name: String,
    /// Used from (the day).
    pub from: Option<Date>,
    /// Used until (the day).
    pub to: Option<Date>,
}

/// One filing.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Filing {
    /// The accession number.
    pub accession: Accession,
    /// The form type (`10-K`, `8-K`, `8-K/A`, ...).
    pub form: String,
    /// The filing date.
    pub filing_date: Option<Date>,
    /// The period of report.
    pub report_date: Option<Date>,
    /// The 8-K items (`2.01`, `9.01`, ...), from the comma-separated `items`.
    pub items: Vec<String>,
    /// The primary document's file name, when it is a safe one.
    pub primary_document: Option<String>,
    /// The primary document's description.
    pub primary_doc_description: Option<String>,
}

impl Filing {
    /// Whether the filing reports `item` (`2.01`).
    pub fn has_item(&self, item: &str) -> bool {
        self.items.iter().any(|i| i == item)
    }

    /// An 8-K (or 8-K/A) with Item 2.01: a completed acquisition or disposition.
    pub fn is_completed_acquisition(&self) -> bool {
        matches!(self.form.as_str(), "8-K" | "8-K/A") && self.has_item("2.01")
    }

    /// A 10-K, 10-K405, 10-KT or an amendment of one.
    pub fn is_annual_report(&self) -> bool {
        matches!(
            self.form.as_str(),
            "10-K" | "10-K/A" | "10-K405" | "10-K405/A" | "10-KT" | "10-KT/A"
        )
    }
}

/// One entry of the company tickers file.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TickerEntry {
    /// The CIK.
    pub cik: Cik,
    /// The ticker symbol.
    pub ticker: String,
    /// The company name the SEC lists.
    pub title: String,
}

#[derive(Deserialize, Default)]
#[serde(default, rename_all = "camelCase")]
struct RawSubmissions {
    cik: Option<Value>,
    name: Option<String>,
    entity_type: Option<String>,
    sic: Option<Value>,
    sic_description: Option<String>,
    tickers: Option<Vec<Option<String>>>,
    exchanges: Option<Vec<Option<String>>>,
    state_of_incorporation: Option<String>,
    state_of_incorporation_description: Option<String>,
    fiscal_year_end: Option<String>,
    former_names: Option<Vec<RawFormer>>,
    filings: Option<RawFilings>,
}

#[derive(Deserialize, Default)]
#[serde(default)]
struct RawFormer {
    name: Option<String>,
    from: Option<String>,
    to: Option<String>,
}

#[derive(Deserialize, Default)]
#[serde(default)]
struct RawFilings {
    recent: Option<RawRecent>,
    files: Option<Vec<RawFile>>,
}

type Column = Option<Vec<Option<Value>>>;

#[derive(Deserialize, Default)]
#[serde(default, rename_all = "camelCase")]
struct RawRecent {
    accession_number: Column,
    filing_date: Column,
    report_date: Column,
    form: Column,
    items: Column,
    primary_document: Column,
    primary_doc_description: Column,
}

#[derive(Deserialize, Default)]
#[serde(default)]
struct RawFile {
    name: Option<String>,
}

/// A JSON string or number as text.
fn scalar(v: Option<&Value>) -> Option<String> {
    match v? {
        Value::String(s) => text(Some(s.clone())),
        Value::Number(n) => Some(n.to_string()),
        _ => None,
    }
}

fn column(c: Option<&Vec<Option<Value>>>, i: usize) -> Option<String> {
    scalar(c?.get(i)?.as_ref())
}

fn strings(v: Option<Vec<Option<String>>>) -> Vec<String> {
    v.unwrap_or_default()
        .into_iter()
        .filter_map(text)
        .take(64)
        .collect()
}

/// The submissions JSON of a company.
pub(crate) fn company(body: &[u8]) -> Result<Company, String> {
    let raw: RawSubmissions = serde_json::from_slice(body).map_err(|e| e.to_string())?;
    let cik = scalar(raw.cik.as_ref())
        .as_deref()
        .and_then(Cik::parse)
        .ok_or("no valid cik")?;
    let name = text(raw.name).ok_or("no name")?;
    let former_names = raw
        .former_names
        .unwrap_or_default()
        .into_iter()
        .filter_map(|f| {
            Some(FormerName {
                name: text(f.name)?,
                from: date(f.from.as_deref()),
                to: date(f.to.as_deref()),
            })
        })
        .collect();
    let filings_raw = raw.filings.unwrap_or_default();
    let recent = filings_raw.recent.unwrap_or_default();
    let n = recent.accession_number.as_ref().map_or(0, Vec::len);
    let mut filings = Vec::with_capacity(n);
    for i in 0..n {
        let Some(accession) =
            column(recent.accession_number.as_ref(), i).and_then(|a| Accession::parse(&a))
        else {
            continue;
        };
        let Some(form) = column(recent.form.as_ref(), i) else {
            continue;
        };
        let items = column(recent.items.as_ref(), i)
            .map(|s| {
                s.split(',')
                    .map(str::trim)
                    .filter(|i| !i.is_empty())
                    .map(str::to_owned)
                    .collect()
            })
            .unwrap_or_default();
        filings.push(Filing {
            accession,
            form,
            filing_date: date(column(recent.filing_date.as_ref(), i).as_deref()),
            report_date: date(column(recent.report_date.as_ref(), i).as_deref()),
            items,
            primary_document: column(recent.primary_document.as_ref(), i)
                .and_then(|d| safe_file(&d)),
            primary_doc_description: column(recent.primary_doc_description.as_ref(), i),
        });
    }
    let older_filing_files = filings_raw
        .files
        .unwrap_or_default()
        .into_iter()
        .filter_map(|f| safe_file(f.name.as_deref()?))
        .collect();
    Ok(Company {
        cik,
        name,
        entity_type: text(raw.entity_type),
        sic: scalar(raw.sic.as_ref()),
        sic_description: text(raw.sic_description),
        tickers: strings(raw.tickers),
        exchanges: strings(raw.exchanges),
        state_of_incorporation: text(raw.state_of_incorporation),
        state_of_incorporation_description: text(raw.state_of_incorporation_description),
        fiscal_year_end: text(raw.fiscal_year_end),
        former_names,
        filings,
        older_filing_files,
        tier: TrustTier::Official,
    })
}

/// The company tickers file: `{"0": {"cik_str": 1, "ticker": "X", "title": "..."}, ...}`.
/// Entries without a valid CIK, a ticker or a title are skipped.
pub(crate) fn tickers(body: &[u8]) -> Result<Vec<TickerEntry>, String> {
    let v: Value = serde_json::from_slice(body).map_err(|e| e.to_string())?;
    let map = v.as_object().ok_or("not a JSON object")?;
    Ok(map
        .values()
        .filter_map(|e| {
            Some(TickerEntry {
                cik: Cik::parse(&scalar(e.get("cik_str"))?)?,
                ticker: scalar(e.get("ticker"))?,
                title: scalar(e.get("title"))?,
            })
        })
        .collect())
}

/// The lowercase letter-and-digit words of `s`.
pub(crate) fn words(s: &str) -> Vec<String> {
    s.split(|c: char| !c.is_alphanumeric())
        .filter(|w| !w.is_empty())
        .map(str::to_lowercase)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ciks() {
        assert_eq!(
            Cik::parse("9990000001").map(Cik::value),
            Some(9_990_000_001)
        );
        assert_eq!(
            Cik::parse(" CIK0000000042 ")
                .map(|c| c.to_string())
                .as_deref(),
            Some("0000000042")
        );
        assert_eq!(Cik::parse("cik42").map(Cik::value), Some(42));
        for bad in [
            "",
            "0",
            "0000000000",
            "12345678901",
            "12a",
            "-1",
            "CIK",
            "1 2",
            "４２",
            "+42",
        ] {
            assert_eq!(Cik::parse(bad), None, "{bad}");
        }
    }

    #[test]
    fn accessions() {
        let a = Accession::parse("0009990000-26-000012").unwrap();
        assert_eq!(a.plain(), "000999000026000012");
        assert_eq!(a.to_string(), "0009990000-26-000012");
        assert_eq!(Accession::parse("000999000026000012"), Some(a));
        for bad in [
            "",
            "0009990000-26-00001",
            "000999000026-00-0012",
            "00099900002600001x",
            "0009990000_26_000012",
            "é0099900002600001",
        ] {
            assert_eq!(Accession::parse(bad), None, "{bad}");
        }
    }

    #[test]
    fn file_names() {
        assert_eq!(safe_file("fict-ex21.htm").as_deref(), Some("fict-ex21.htm"));
        for bad in [
            "", "../x", "a/b", ".hidden", "a b", "x?y=1", "a\\b", "%2e%2e",
        ] {
            assert_eq!(safe_file(bad), None, "{bad}");
        }
    }

    #[test]
    fn refusals() {
        assert_eq!(
            EdgarRefusal::from_body(
                b"<h1>Your Request Originates from an Undeclared Automated Tool</h1>"
            ),
            EdgarRefusal::UndeclaredUserAgent
        );
        assert_eq!(
            EdgarRefusal::from_body(b"Request Rate Threshold Exceeded"),
            EdgarRefusal::RateThreshold
        );
        assert_eq!(
            EdgarRefusal::from_body(b"\xff\xfe"),
            EdgarRefusal::Forbidden
        );
    }
}
