//! GLEIF's golden copy delta files (ADR 0002 section 6): which LEIs changed.
//!
//! GLEIF publishes the golden copy three times a day (00:00, 08:00 and 16:00 UTC), each with
//! four delta files per data set, against the publication 8 hours (`IntraDay`), 24 hours
//! (`LastDay`), 7 days (`LastWeek`) or 31 days (`LastMonth`) before. The publication API
//! (`goldencopy.gleif.org/api/v2/golden-copies/publishes/latest`) names them:
//! `data.publish_date` and, per data set (`lei2`: LEI-CDF level 1; `rr`: RR-CDF level 2
//! relationships), `delta_files.<type>.csv.url`, a zipped CSV.
//!
//! The file is downloaded into a capped temporary file (never held in memory), its CSV entry
//! is read record by record, and only the LEIs are kept: the LEI-CDF `LEI` column, and the
//! RR-CDF start and end nodes (child and parent). Every LEI must pass ISO 17442. Nothing about
//! a company is sent: the files are the same for everyone.

use std::io::{BufReader, Read as _};
use std::time::Duration;

use doewood_enrich::{IdKind, Identifier};
use serde::Deserialize;
use time::OffsetDateTime;
use tokio::sync::mpsc;
use url::Url;

use super::csv::{CsvError, Reader};
use super::net::StreamClient;
use crate::http::USER_AGENT;
use crate::warning::{Registry, Warning, WarningKind};

/// The latest publication.
pub const DEFAULT_URL: &str = "https://goldencopy.gleif.org/api/v2/golden-copies/publishes/latest";

/// The delta files' spans, shortest first.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum DeltaSpan {
    /// Against the publication 8 hours before.
    IntraDay,
    /// 24 hours before.
    LastDay,
    /// 7 days before.
    LastWeek,
    /// 31 days before.
    LastMonth,
}

impl DeltaSpan {
    /// Every span, shortest first.
    pub const ALL: [Self; 4] = [
        Self::IntraDay,
        Self::LastDay,
        Self::LastWeek,
        Self::LastMonth,
    ];

    /// How far back it reaches.
    pub fn reach(self) -> Duration {
        Duration::from_hours(match self {
            Self::IntraDay => 8,
            Self::LastDay => 24,
            Self::LastWeek => 7 * 24,
            Self::LastMonth => 31 * 24,
        })
    }

    /// The API's name.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::IntraDay => "IntraDay",
            Self::LastDay => "LastDay",
            Self::LastWeek => "LastWeek",
            Self::LastMonth => "LastMonth",
        }
    }

    /// The shortest span that covers `gap` (the time since the publication last applied), or
    /// `None` when even a month does not.
    pub fn covering(gap: Duration) -> Option<Self> {
        Self::ALL.into_iter().find(|s| gap <= s.reach())
    }
}

/// A data set of the golden copy.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum DataSet {
    /// LEI-CDF: the LEI records (level 1).
    Lei2,
    /// RR-CDF: the relationship records (level 2).
    Rr,
}

/// The latest publication: its time and its delta files (zipped CSV).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Publication {
    /// `data.publish_date`, read as UTC.
    pub published_at: OffsetDateTime,
    /// The LEI-CDF delta files by span.
    pub lei2: Vec<(DeltaSpan, Url)>,
    /// The RR-CDF delta files by span.
    pub rr: Vec<(DeltaSpan, Url)>,
}

impl Publication {
    /// The file of `set` for `span`.
    pub fn file(&self, set: DataSet, span: DeltaSpan) -> Option<&Url> {
        let files = match set {
            DataSet::Lei2 => &self.lei2,
            DataSet::Rr => &self.rr,
        };
        files.iter().find(|(s, _)| *s == span).map(|(_, u)| u)
    }
}

/// The feed's settings. The defaults are the production values.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GleifFeedConfig {
    /// The latest publication (default [`DEFAULT_URL`]). Files must be on the same host.
    pub url: String,
    /// One download, body included (default 10 minutes).
    pub timeout: Duration,
    /// Connecting (default 10 s).
    pub connect_timeout: Duration,
    /// The largest publication answer (default 256 KiB).
    pub max_index: usize,
    /// The largest zipped file (default 512 MiB; a daily LEI-CDF delta is a few MB, a
    /// monthly one tens).
    pub max_zip: u64,
    /// The most bytes read out of the zip (default 4 GiB): a bound on a hostile archive.
    pub max_csv: u64,
    /// The longest CSV record (default 256 KiB).
    pub max_record: usize,
    /// The most records read from one file (default 5,000,000).
    pub max_records: u64,
    /// LEIs per batch handed to the caller (default 500).
    pub batch: usize,
}

impl Default for GleifFeedConfig {
    fn default() -> Self {
        Self {
            url: DEFAULT_URL.to_owned(),
            timeout: Duration::from_mins(10),
            connect_timeout: Duration::from_secs(10),
            max_index: 256 * 1024,
            max_zip: 512 * 1024 * 1024,
            max_csv: 4 * 1024 * 1024 * 1024,
            max_record: 256 * 1024,
            max_records: 5_000_000,
            batch: 500,
        }
    }
}

/// What reading one file gave.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct FileStats {
    /// Records read.
    pub records: u64,
    /// LEIs that failed their check (skipped).
    pub invalid: u64,
    /// Whether [`GleifFeedConfig::max_records`] stopped the read.
    pub truncated: bool,
}

/// The GLEIF delta feed client.
#[derive(Clone, Debug)]
pub struct GleifFeed {
    url: Url,
    client: StreamClient,
    config: GleifFeedConfig,
}

#[derive(Deserialize)]
struct Latest {
    data: Option<LatestData>,
}

#[derive(Deserialize)]
struct LatestData {
    publish_date: Option<String>,
    lei2: Option<SetFiles>,
    rr: Option<SetFiles>,
}

#[derive(Deserialize)]
struct SetFiles {
    delta_files: Option<std::collections::BTreeMap<String, Formats>>,
}

#[derive(Deserialize)]
struct Formats {
    csv: Option<FileRef>,
}

#[derive(Deserialize)]
struct FileRef {
    url: Option<String>,
}

impl GleifFeed {
    /// The client.
    pub fn new(config: &GleifFeedConfig) -> Result<Self, Warning> {
        let url = http_url(&config.url).ok_or_else(|| {
            warning(WarningKind::Config(
                "the GLEIF feed URL must be http(s)".to_owned(),
            ))
        })?;
        let client = StreamClient::new(
            Registry::Gleif,
            USER_AGENT,
            Some(config.timeout),
            config.connect_timeout,
            None,
        )?;
        Ok(Self {
            url,
            client,
            config: config.clone(),
        })
    }

    /// The latest publication.
    pub async fn latest(&self) -> Result<Publication, Warning> {
        let resp = self
            .client
            .get(&self.url, "application/json")
            .await?
            .ok_or_else(|| warning(WarningKind::Status(404)))?;
        let body = self.client.body(resp, self.config.max_index).await?;
        let latest: Latest =
            serde_json::from_slice(&body).map_err(|e| malformed(format!("publication: {e}")))?;
        let data = latest
            .data
            .ok_or_else(|| malformed("publication: no data".to_owned()))?;
        let published_at = data
            .publish_date
            .as_deref()
            .and_then(publish_time)
            .ok_or_else(|| malformed("publication: no publish_date".to_owned()))?;
        let files = |set: Option<SetFiles>| -> Vec<(DeltaSpan, Url)> {
            let Some(deltas) = set.and_then(|s| s.delta_files) else {
                return Vec::new();
            };
            DeltaSpan::ALL
                .into_iter()
                .filter_map(|span| {
                    let url = deltas.get(span.as_str())?.csv.as_ref()?.url.as_deref()?;
                    let url = http_url(url)?;
                    // Only files on the publication's own host are fetched.
                    (url.host_str() == self.url.host_str() && url.port() == self.url.port())
                        .then_some((span, url))
                })
                .collect()
        };
        Ok(Publication {
            published_at,
            lei2: files(data.lei2),
            rr: files(data.rr),
        })
    }

    /// Reads the zipped CSV at `url` of data set `set`, sending its LEIs to `out` in batches
    /// (LEI-CDF: each record's LEI; RR-CDF: each relationship's start and end node). Returns
    /// once the file is read or `out` is closed.
    pub async fn read(
        &self,
        url: &Url,
        set: DataSet,
        out: mpsc::Sender<Vec<Identifier>>,
    ) -> Result<FileStats, Warning> {
        let resp = self
            .client
            .get(url, "application/zip")
            .await?
            .ok_or_else(|| warning(WarningKind::Status(404)))?;
        let mut file = tempfile::tempfile()
            .map_err(|e| warning(WarningKind::Transport(format!("temporary file: {e}"))))?;
        self.client
            .spool(resp, &mut file, self.config.max_zip)
            .await?;
        let config = self.config.clone();
        tokio::task::spawn_blocking(move || read_zip(file, set, &config, &out))
            .await
            .map_err(|e| malformed(format!("reading task: {e}")))?
    }
}

fn read_zip(
    file: std::fs::File,
    set: DataSet,
    config: &GleifFeedConfig,
    out: &mpsc::Sender<Vec<Identifier>>,
) -> Result<FileStats, Warning> {
    let mut archive =
        zip::ZipArchive::new(file).map_err(|e| malformed(format!("delta file: {e}")))?;
    let index = (0..archive.len())
        .find(|&i| {
            archive.by_index(i).is_ok_and(|f| {
                f.is_file()
                    && f.name()
                        .is_ok_and(|n| n.to_ascii_lowercase().ends_with(".csv"))
            })
        })
        .ok_or_else(|| malformed("delta file: no CSV in the archive".to_owned()))?;
    let entry = archive
        .by_index(index)
        .map_err(|e| malformed(format!("delta file: {e}")))?;
    read_csv(
        BufReader::new(entry.take(config.max_csv)),
        set,
        config,
        &mut |batch| out.blocking_send(batch).is_ok(),
    )
}

/// Reads the CSV text, handing each batch of LEIs to `sink` (which says whether to go on).
pub(crate) fn read_csv(
    input: impl std::io::BufRead,
    set: DataSet,
    config: &GleifFeedConfig,
    sink: &mut dyn FnMut(Vec<Identifier>) -> bool,
) -> Result<FileStats, Warning> {
    let mut reader = Reader::new(input, config.max_record);
    let csv = |e: CsvError| match e {
        CsvError::TooLong => malformed("delta file: a record is too long".to_owned()),
        CsvError::Io(e) => malformed(format!("delta file: {e}")),
    };
    let header = reader
        .next(&[])
        .map_err(csv)?
        .ok_or_else(|| malformed("delta file: empty".to_owned()))?;
    let column = |want: &dyn Fn(&str) -> bool| {
        header
            .iter()
            .position(|h| want(h.trim().trim_start_matches('\u{feff}')))
    };
    let keep: Vec<usize> = match set {
        DataSet::Lei2 => vec![
            column(&|h| h == "LEI")
                .ok_or_else(|| malformed("delta file: no LEI column".to_owned()))?,
        ],
        DataSet::Rr => {
            let start = column(&|h| h.ends_with("StartNode.NodeID"));
            let end = column(&|h| h.ends_with("EndNode.NodeID"));
            match (start, end) {
                (Some(s), Some(e)) => vec![s, e],
                _ => {
                    return Err(malformed(
                        "delta file: no StartNode.NodeID / EndNode.NodeID columns".to_owned(),
                    ));
                }
            }
        }
    };
    let mut stats = FileStats::default();
    let mut batch = Vec::with_capacity(config.batch.max(1));
    while let Some(record) = reader.next(&keep).map_err(csv)? {
        if stats.records >= config.max_records {
            stats.truncated = true;
            break;
        }
        stats.records += 1;
        for raw in record {
            match Identifier::parse(IdKind::Lei, raw.trim()) {
                Some(lei) => batch.push(lei),
                // An RR-CDF node that is not an LEI (another id type) is skipped too.
                None => stats.invalid += 1,
            }
        }
        if batch.len() >= config.batch.max(1) && !sink(std::mem::take(&mut batch)) {
            return Ok(stats);
        }
    }
    if !batch.is_empty() {
        sink(batch);
    }
    Ok(stats)
}

/// `2026-11-25 08:00:00` or `2026-11-25T08:00:00Z`, read as UTC (offsets other than `Z` are
/// not expected and refused).
fn publish_time(s: &str) -> Option<OffsetDateTime> {
    let s = s.trim().trim_end_matches('Z');
    let date = crate::parse::date(Some(s.get(..10)?))?;
    let rest = s.get(10..)?;
    let rest = rest.strip_prefix(' ').or_else(|| rest.strip_prefix('T'))?;
    let mut hms = rest.get(..8)?.split(':').map(|p| p.parse::<u8>().ok());
    let (h, m, sec) = (hms.next()??, hms.next()??, hms.next()??);
    if !rest[8..]
        .trim_start_matches(|c: char| c == '.' || c.is_ascii_digit())
        .is_empty()
    {
        return None;
    }
    let t = time::Time::from_hms(h, m, sec).ok()?;
    Some(time::PrimitiveDateTime::new(date, t).assume_utc())
}

fn http_url(s: &str) -> Option<Url> {
    let url = Url::parse(s).ok()?;
    (matches!(url.scheme(), "https" | "http") && url.host_str().is_some()).then_some(url)
}

fn warning(kind: WarningKind) -> Warning {
    Warning::new(Registry::Gleif, kind)
}

fn malformed(e: String) -> Warning {
    warning(WarningKind::Malformed(e))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn spans_cover_the_gap() {
        let h = Duration::from_hours;
        assert_eq!(DeltaSpan::covering(h(8)), Some(DeltaSpan::IntraDay));
        assert_eq!(DeltaSpan::covering(h(16)), Some(DeltaSpan::LastDay));
        assert_eq!(DeltaSpan::covering(h(48)), Some(DeltaSpan::LastWeek));
        assert_eq!(DeltaSpan::covering(h(24 * 31)), Some(DeltaSpan::LastMonth));
        assert_eq!(DeltaSpan::covering(h(24 * 32)), None);
    }

    /// An invented LEI with valid check digits.
    fn lei(tag: &str) -> String {
        let head = format!("FAKE00{tag:0>12}");
        let mut r: u32 = 0;
        for c in format!("{head}00").chars() {
            let d = c.to_digit(36).unwrap();
            r = if d >= 10 {
                (r * 100 + d) % 97
            } else {
                (r * 10 + d) % 97
            };
        }
        format!("{head}{:02}", 98 - r)
    }

    fn zip_file(entries: &[(&str, &str)]) -> std::fs::File {
        use std::io::{Seek as _, Write as _};
        let mut file = tempfile::tempfile().unwrap();
        {
            let mut z = zip::ZipWriter::new(&mut file);
            let opts = zip::write::SimpleFileOptions::default()
                .compression_method(zip::CompressionMethod::Deflated);
            for (name, text) in entries {
                z.start_file(*name, opts).unwrap();
                z.write_all(text.as_bytes()).unwrap();
            }
            z.finish().unwrap();
        }
        file.rewind().unwrap();
        file
    }

    #[test]
    fn deflated_delta_files_are_read_in_batches() {
        let (a, b, p) = (lei("ZIPA"), lei("ZIPB"), lei("ZIPP"));
        let lei2 = format!("\u{feff}LEI,Entity.LegalName\n{a},A\nbad,B\n{b},\"B, Ltd\"\n");
        let config = GleifFeedConfig {
            batch: 1,
            ..GleifFeedConfig::default()
        };
        let (tx, mut rx) = mpsc::channel(10);
        let file = zip_file(&[("readme.txt", "x"), ("lei2.csv", &lei2)]);
        let stats = read_zip(file, DataSet::Lei2, &config, &tx).unwrap();
        drop(tx);
        assert_eq!((stats.records, stats.invalid, stats.truncated), (3, 1, false));
        let mut got = Vec::new();
        while let Ok(batch) = rx.try_recv() {
            assert_eq!(batch.len(), 1);
            got.extend(batch.into_iter().map(|i| i.value));
        }
        assert_eq!(got, [a.clone(), b.clone()]);

        let rr = format!(
            "Relationship.StartNode.NodeID,Relationship.StartNode.NodeIDType,\
             Relationship.EndNode.NodeID,Relationship.EndNode.NodeIDType\n{a},LEI,{p},LEI\n"
        );
        let (tx, mut rx) = mpsc::channel(10);
        let stats = read_zip(zip_file(&[("rr.csv", &rr)]), DataSet::Rr, &config, &tx).unwrap();
        drop(tx);
        assert_eq!(stats.records, 1);
        let mut got = Vec::new();
        while let Ok(batch) = rx.try_recv() {
            got.extend(batch.into_iter().map(|i| i.value));
        }
        assert_eq!(got, [a, p]);

        // The record cap, a missing column, an archive without CSV, and garbage.
        let capped = GleifFeedConfig {
            max_records: 1,
            ..GleifFeedConfig::default()
        };
        let (tx, _rx) = mpsc::channel(10);
        let stats = read_zip(zip_file(&[("lei2.csv", &lei2)]), DataSet::Lei2, &capped, &tx).unwrap();
        assert!(stats.truncated && stats.records == 1);
        for file in [
            zip_file(&[("lei2.csv", "Name\nx\n")]),
            zip_file(&[("readme.txt", "LEI\n")]),
        ] {
            let w = read_zip(file, DataSet::Lei2, &config, &tx).unwrap_err();
            assert!(matches!(w.kind, WarningKind::Malformed(_)), "{w}");
        }
        let mut garbage = tempfile::tempfile().unwrap();
        std::io::Write::write_all(&mut garbage, b"not a zip").unwrap();
        std::io::Seek::rewind(&mut garbage).unwrap();
        assert!(read_zip(garbage, DataSet::Lei2, &config, &tx).is_err());
    }

    #[test]
    fn publish_dates() {
        let t = publish_time("2026-11-25 08:00:00").unwrap();
        assert_eq!((t.hour(), t.offset().is_utc()), (8, true));
        assert_eq!(publish_time("2026-11-25T08:00:00Z"), Some(t));
        assert_eq!(publish_time("2026-11-25T08:00:00.000Z"), Some(t));
        assert!(publish_time("2026-11-25 08:00:00+02:00").is_none());
        assert!(publish_time("2026-11-25 25:00:00").is_none());
        assert!(publish_time("yesterday").is_none());
    }
}
