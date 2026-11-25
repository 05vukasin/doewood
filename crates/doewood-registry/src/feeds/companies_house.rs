//! UK Companies House's streaming API (ADR 0002 section 6, optional): which companies changed.
//!
//! `https://stream.companieshouse.gov.uk/<stream>` is a long-running `GET` whose body is one
//! JSON event per line (blank lines are heartbeats): `{"resource_kind", "resource_uri",
//! "resource_id", "data", "event": {"timepoint", "published_at", "type"}}`. A client resumes
//! with `?timepoint=<n>` (the last one it processed); a timepoint too far back is a 416.
//! Authentication is HTTP basic with a **streaming key** (Companies House issues those apart
//! from REST keys) as the user name. At most two connections per account; frequent reconnects
//! are rate limited (429: wait a minute).
//!
//! Only the company number (from `resource_uri`, `/company/<number>/...`) and the timepoint
//! are read from an event. Its `data` (officers and PSCs are mostly natural persons) is
//! skipped unparsed, never kept and never logged.

use std::time::Duration;

use serde::Deserialize;
use serde::de::IgnoredAny;
use url::Url;

use super::net::{Line, Lines, StreamClient};
use crate::companies_house::CompanyNumber;
use crate::http::{ApiKey, USER_AGENT};
use crate::warning::{Registry, Warning, WarningKind};

/// The streaming API's host.
pub const DEFAULT_URL: &str = "https://stream.companieshouse.gov.uk/";

/// A stream.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum StreamKind {
    /// Company profiles (`companies`).
    Companies,
    /// Officer appointments (`officers`).
    Officers,
    /// People with significant control (`persons-with-significant-control`).
    Pscs,
}

impl StreamKind {
    /// Every stream the feed reads.
    pub const ALL: [Self; 3] = [Self::Companies, Self::Officers, Self::Pscs];

    /// The stream's path.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Companies => "companies",
            Self::Officers => "officers",
            Self::Pscs => "persons-with-significant-control",
        }
    }

    /// The stream of path `s`.
    pub fn parse(s: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|k| k.as_str() == s)
    }
}

/// The client's settings. The defaults are the production values.
#[derive(Clone, Debug)]
pub struct ChStreamConfig {
    /// The streaming host (default [`DEFAULT_URL`]).
    pub url: String,
    /// The streaming key (required).
    pub key: Option<ApiKey>,
    /// Connecting (default 10 s).
    pub connect_timeout: Duration,
    /// The longest wait for the next line; heartbeats keep a healthy stream within it
    /// (default 2 minutes).
    pub idle: Duration,
    /// The longest event line (default 1 MiB).
    pub max_line: usize,
}

impl Default for ChStreamConfig {
    fn default() -> Self {
        Self {
            url: DEFAULT_URL.to_owned(),
            key: None,
            connect_timeout: Duration::from_secs(10),
            idle: Duration::from_mins(2),
            max_line: 1024 * 1024,
        }
    }
}

/// One event, as far as the feed reads it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StreamEvent {
    /// Its position in the stream.
    pub timepoint: u64,
    /// The company it is about, when `resource_uri` names one.
    pub company: Option<CompanyNumber>,
}

#[derive(Deserialize)]
struct RawEvent {
    resource_uri: Option<String>,
    resource_id: Option<String>,
    resource_kind: Option<String>,
    // Read past, never kept: it may describe a natural person.
    #[allow(dead_code)]
    data: Option<IgnoredAny>,
    event: Option<RawMeta>,
}

#[derive(Deserialize)]
struct RawMeta {
    timepoint: Option<u64>,
}

/// The streaming client.
#[derive(Clone, Debug)]
pub struct ChStream {
    root: Url,
    client: StreamClient,
    config: ChStreamConfig,
}

/// An open stream.
pub struct Connection {
    lines: Lines,
    client: StreamClient,
    /// Lines that were not an event (malformed or too long), skipped.
    pub skipped: u64,
}

impl ChStream {
    /// The client; without a key it is [`WarningKind::NotConfigured`].
    pub fn new(config: &ChStreamConfig) -> Result<Self, Warning> {
        let key = config
            .key
            .clone()
            .ok_or_else(|| warning(WarningKind::NotConfigured))?;
        let root = Url::parse(&config.url)
            .ok()
            .filter(|u| matches!(u.scheme(), "https" | "http") && !u.cannot_be_a_base())
            .ok_or_else(|| {
                warning(WarningKind::Config(
                    "the streaming URL must be http(s)".to_owned(),
                ))
            })?;
        let client = StreamClient::new(
            Registry::CompaniesHouse,
            USER_AGENT,
            None,
            config.connect_timeout,
            Some(key),
        )?;
        Ok(Self {
            root,
            client,
            config: config.clone(),
        })
    }

    /// Opens `stream`, from just after `timepoint` (or from now). A 416 (the timepoint is too
    /// old) is `Status(416)`; a refused key `KeyRefused`; a 429 `RateLimited`.
    pub async fn open(
        &self,
        stream: StreamKind,
        timepoint: Option<u64>,
    ) -> Result<Connection, Warning> {
        let mut url = self
            .root
            .join(stream.as_str())
            .map_err(|e| warning(WarningKind::Config(e.to_string())))?;
        if let Some(t) = timepoint {
            url.query_pairs_mut()
                .append_pair("timepoint", &t.to_string());
        }
        let resp = self
            .client
            .get(&url, "application/json")
            .await?
            .ok_or_else(|| self.client.warning(WarningKind::Status(404)))?;
        Ok(Connection {
            lines: Lines::new(resp, self.config.max_line, Some(self.config.idle)),
            client: self.client.clone(),
            skipped: 0,
        })
    }
}

impl Connection {
    /// The next event; `None` when the server closed the stream. Heartbeats and lines that
    /// are not events are skipped. No line goes far: an idle stream is a `Timeout`.
    pub async fn next(&mut self) -> Result<Option<StreamEvent>, Warning> {
        loop {
            let line = match self.lines.next(&self.client).await? {
                Line::Text(t) => t,
                Line::End => return Ok(None),
            };
            if line.iter().all(u8::is_ascii_whitespace) {
                continue;
            }
            match event(&line) {
                Some(e) => return Ok(Some(e)),
                None => self.skipped += 1,
            }
        }
    }

    /// Lines skipped for their length.
    pub fn overlong(&self) -> u64 {
        self.lines.overlong
    }
}

/// The timepoint and company of one event line.
fn event(line: &[u8]) -> Option<StreamEvent> {
    let raw: RawEvent = serde_json::from_slice(line).ok()?;
    let timepoint = raw.event?.timepoint?;
    let company = raw
        .resource_uri
        .as_deref()
        .and_then(|u| u.strip_prefix("/company/"))
        .and_then(|rest| rest.split('/').next())
        .and_then(CompanyNumber::parse)
        .or_else(|| {
            // A company profile's id is its number.
            (raw.resource_kind.as_deref() == Some("company-profile"))
                .then_some(raw.resource_id.as_deref())
                .flatten()
                .and_then(CompanyNumber::parse)
        });
    Some(StreamEvent { timepoint, company })
}

fn warning(kind: WarningKind) -> Warning {
    Warning::new(Registry::CompaniesHouse, kind)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn events_give_the_company_and_the_timepoint_only() {
        let e = event(
            br#"{"resource_kind":"company-officers","resource_uri":"/company/99000001/appointments/fake-id","resource_id":"fake-id","data":{"name":"EXAMPLE, Person","date_of_birth":{"month":1,"year":1970}},"event":{"timepoint":41,"published_at":"2026-11-25T08:00:00","type":"changed"}}"#,
        )
        .unwrap();
        assert_eq!(e.timepoint, 41);
        assert_eq!(e.company.unwrap().as_str(), "99000001");
        let e = event(
            br#"{"resource_kind":"company-profile","resource_uri":"/company/SC990002","resource_id":"SC990002","data":{},"event":{"timepoint":42}}"#,
        )
        .unwrap();
        assert_eq!(e.company.unwrap().as_str(), "SC990002");
        let e = event(br#"{"resource_uri":"/disqualified-officers/x","event":{"timepoint":43}}"#)
            .unwrap();
        assert_eq!((e.timepoint, e.company), (43, None));
        assert!(event(b"not json").is_none());
        assert!(event(br#"{"resource_uri":"/company/99000001"}"#).is_none());
    }

    #[test]
    fn streams_by_path() {
        for k in StreamKind::ALL {
            assert_eq!(StreamKind::parse(k.as_str()), Some(k));
        }
        assert_eq!(StreamKind::parse("filings"), None);
    }
}
