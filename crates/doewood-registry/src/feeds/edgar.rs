//! SEC EDGAR's daily index (ADR 0002 section 6): which CIKs filed what on a day.
//!
//! EDGAR publishes, for each business day, `Archives/edgar/daily-index/<year>/QTR<q>/
//! master.<yyyymmdd>.idx`: a few header lines, a `CIK|Company Name|Form Type|Date Filed|
//! Filename` line, a line of dashes, then one pipe-separated row per filing. The index of a
//! day is built nightly from about 10 p.m. Eastern time; a day without filings (a weekend, a
//! holiday) has no file (404).
//!
//! The file is read line by line as it arrives (never held whole) and only the rows of the
//! forms asked for are kept, as (CIK, form). Nothing about a company is sent.

use std::time::Duration;

use time::Date;
use url::Url;

use super::net::{Line, Lines, StreamClient};
use crate::edgar::{Cik, user_agent};
use crate::warning::{Registry, Warning, WarningKind};

/// The forms that make a company due (annual reports and current reports, and their
/// amendments).
pub const DEFAULT_FORMS: &[&str] = &["10-K", "10-K/A", "20-F", "20-F/A", "8-K", "8-K/A"];

/// The feed's settings. The defaults are the production values.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EdgarFeedConfig {
    /// The archives root (default `https://www.sec.gov/`, as [`crate::edgar::EdgarConfig`]).
    pub www_url: String,
    /// The operator's contact for the SEC's `User-Agent` rule (see
    /// [`crate::edgar::EdgarConfig::contact`]).
    pub contact: Option<String>,
    /// One day's file, body included (default 2 minutes).
    pub timeout: Duration,
    /// Connecting (default 10 s).
    pub connect_timeout: Duration,
    /// The largest file read (default 64 MiB; a day is a few MB).
    pub max_file: u64,
    /// The longest line (default 4 KiB).
    pub max_line: usize,
    /// The most matching rows kept from one day (default 100,000).
    pub max_rows: usize,
    /// The forms kept (default [`DEFAULT_FORMS`]).
    pub forms: Vec<String>,
}

impl Default for EdgarFeedConfig {
    fn default() -> Self {
        Self {
            www_url: crate::edgar::DEFAULT_WWW_URL.to_owned(),
            contact: None,
            timeout: Duration::from_mins(2),
            connect_timeout: Duration::from_secs(10),
            max_file: 64 * 1024 * 1024,
            max_line: 4096,
            max_rows: 100_000,
            forms: DEFAULT_FORMS.iter().map(|f| (*f).to_owned()).collect(),
        }
    }
}

/// One day's matching filings.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct DayIndex {
    /// `(CIK, form)` of each matching row, in file order (a CIK may repeat).
    pub filings: Vec<(Cik, String)>,
    /// Rows read (every form).
    pub rows: u64,
    /// Whether [`EdgarFeedConfig::max_rows`] stopped it.
    pub truncated: bool,
}

/// The EDGAR daily index client.
#[derive(Clone, Debug)]
pub struct EdgarFeed {
    root: Url,
    client: StreamClient,
    config: EdgarFeedConfig,
}

impl EdgarFeed {
    /// The client.
    pub fn new(config: &EdgarFeedConfig) -> Result<Self, Warning> {
        let root = Url::parse(&config.www_url)
            .ok()
            .filter(|u| matches!(u.scheme(), "https" | "http") && !u.cannot_be_a_base())
            .ok_or_else(|| {
                warning(WarningKind::Config(
                    "the EDGAR archives URL must be http(s)".to_owned(),
                ))
            })?;
        let ua = user_agent(config.contact.as_deref())?;
        let client = StreamClient::new(
            Registry::Edgar,
            &ua,
            Some(config.timeout),
            config.connect_timeout,
            None,
        )?;
        Ok(Self {
            root,
            client,
            config: config.clone(),
        })
    }

    /// The index URL of `day`.
    pub fn url(&self, day: Date) -> Result<Url, Warning> {
        let quarter = (u8::from(day.month()) - 1) / 3 + 1;
        let path = format!(
            "Archives/edgar/daily-index/{y}/QTR{quarter}/master.{y}{m:02}{d:02}.idx",
            y = day.year(),
            m = u8::from(day.month()),
            d = day.day()
        );
        self.root
            .join(&path)
            .map_err(|e| warning(WarningKind::Config(e.to_string())))
    }

    /// The filings of `day` of the forms asked for; `None` when the day has no index (no
    /// filings that day, or not built yet).
    pub async fn day(&self, day: Date) -> Result<Option<DayIndex>, Warning> {
        let url = self.url(day)?;
        let Some(resp) = self.client.get(&url, "text/plain").await? else {
            return Ok(None);
        };
        if resp
            .content_length()
            .is_some_and(|n| n > self.config.max_file)
        {
            return Err(self.too_large());
        }
        let mut lines = Lines::new(resp, self.config.max_line, None);
        let mut out = DayIndex::default();
        let mut table = false;
        loop {
            let line = match lines.next(&self.client).await? {
                Line::Text(t) => t,
                Line::End => break,
            };
            if lines.read > self.config.max_file {
                return Err(self.too_large());
            }
            // Company names may be in any 8-bit encoding; only the ASCII columns are used.
            let line = String::from_utf8_lossy(&line);
            if !table {
                table = line.starts_with("---");
                continue;
            }
            let Some(row) = row(&line) else { continue };
            out.rows += 1;
            if !self
                .config
                .forms
                .iter()
                .any(|f| f.eq_ignore_ascii_case(row.1))
            {
                continue;
            }
            if out.filings.len() >= self.config.max_rows {
                out.truncated = true;
                break;
            }
            out.filings.push((row.0, row.1.to_ascii_uppercase()));
        }
        if !table {
            return Err(warning(WarningKind::Malformed(
                "daily index: no table".to_owned(),
            )));
        }
        Ok(Some(out))
    }

    fn too_large(&self) -> Warning {
        warning(WarningKind::TooLarge {
            limit: usize::try_from(self.config.max_file).unwrap_or(usize::MAX),
        })
    }
}

/// `CIK|Company Name|Form Type|Date Filed|Filename`: the CIK and the form. The company name
/// may hold a `|`, so the form is the third field from the end.
fn row(line: &str) -> Option<(Cik, &str)> {
    let fields: Vec<&str> = line.split('|').collect();
    if fields.len() < 5 {
        return None;
    }
    let cik = Cik::parse(fields[0])?;
    let form = fields[fields.len() - 3].trim();
    (!form.is_empty() && form.len() <= 20).then_some((cik, form))
}

fn warning(kind: WarningKind) -> Warning {
    Warning::new(Registry::Edgar, kind)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rows() {
        let (cik, form) = row("9990000001|EXAMPLE HOLDINGS INC|10-K|20261124|edgar/data/9990000001/0009990000-26-000001.txt").unwrap();
        assert_eq!((cik.value(), form), (9_990_000_001, "10-K"));
        let (_, form) = row("42|A | B CORP|8-K|20261124|edgar/data/42/x.txt").unwrap();
        assert_eq!(form, "8-K");
        assert!(row("CIK|Company Name|Form Type|Date Filed|Filename").is_none());
        assert!(row("42|short").is_none());
    }

    #[test]
    fn urls_by_quarter() {
        let feed = EdgarFeed::new(&EdgarFeedConfig::default()).unwrap();
        let d = Date::from_calendar_date(2026, time::Month::November, 24).unwrap();
        assert_eq!(
            feed.url(d).unwrap().as_str(),
            "https://www.sec.gov/Archives/edgar/daily-index/2026/QTR4/master.20261124.idx"
        );
        let d = Date::from_calendar_date(2026, time::Month::March, 2).unwrap();
        assert!(
            feed.url(d)
                .unwrap()
                .as_str()
                .ends_with("/2026/QTR1/master.20260302.idx")
        );
    }
}
