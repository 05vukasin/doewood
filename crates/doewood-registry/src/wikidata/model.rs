//! Wikidata answers, parsed into types: SPARQL 1.1 JSON results
//! (`application/sparql-results+json`) of the query in [`super::query`], and the
//! `wbsearchentities` JSON. Read leniently (unknown fields ignored), then checked: every id
//! must be a Wikidata item id, every website `http(s)`, every LEI must pass ISO 17442.

use std::collections::{BTreeMap, HashMap};

use doewood_enrich::{IdKind, Identifier};
use serde::Deserialize;
use time::Date;
use url::Url;

use super::query::{QId, ROW_LIMIT};
use crate::parse::{date, text};
use crate::trust::TrustTier;

/// A Wikidata item: its label and the statements doewood reads, deprecated ones left out.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WikidataItem {
    /// The item.
    pub qid: QId,
    /// Its label in the configured language, else its `mul` label.
    pub label: Option<String>,
    /// Its LEIs (P1278), validated; invalid ones are dropped.
    pub leis: Vec<Identifier>,
    /// Parent organization (P749).
    pub parent_organizations: Vec<Relation>,
    /// Owned by (P127). An owner may be a person ([`Relation::human`]).
    pub owned_by: Vec<Relation>,
    /// Replaced by (P1366).
    pub replaced_by: Vec<Relation>,
    /// Followed by (P156).
    pub followed_by: Vec<Relation>,
    /// Official website (P856), `http(s)` only.
    pub websites: Vec<Website>,
    /// Logo image (P154): Commons files.
    pub logos: Vec<Logo>,
    /// Always [`TrustTier::Community`]: Wikidata is the lowest tier (ADR 0002 section 2).
    pub tier: TrustTier,
}

/// A statement whose value is another item.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Relation {
    /// The other item.
    pub target: QId,
    /// Its label.
    pub label: Option<String>,
    /// Whether it is a human (P31 Q5): a person, never a company.
    pub human: bool,
    /// When it held (the P580 / P582 qualifiers).
    pub period: Period,
    /// Whether the statement has the preferred rank.
    pub preferred: bool,
}

/// An official website.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Website {
    /// The URL (`http` or `https`). Not verified: ADR 0002 section 5 verifies it before use.
    pub url: Url,
    /// When it held.
    pub period: Period,
    /// Whether the statement has the preferred rank.
    pub preferred: bool,
}

/// A logo: a file on Wikimedia Commons.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Logo {
    /// The Commons file name, spaces as `_` (`Example_logo.svg`).
    pub file: String,
    /// `https://commons.wikimedia.org/wiki/Special:FilePath/<file>`, which redirects to the
    /// file itself.
    pub url: Url,
    /// When it held.
    pub period: Period,
    /// Whether the statement has the preferred rank.
    pub preferred: bool,
}

/// The start and end time qualifiers of a statement; either may be missing.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Period {
    /// Start time (P580).
    pub start: Option<WikiDate>,
    /// End time (P582).
    pub end: Option<WikiDate>,
}

/// A Wikidata time, at day, month or year precision. A month is stored as its first day, a
/// year as January 1st; coarser times (decades, centuries) are dropped.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct WikiDate {
    /// The date (the first day of the month or year when coarser than a day).
    pub date: Date,
    /// How much of it is known.
    pub precision: Precision,
}

/// The precision of a [`WikiDate`] (Wikidata's 11, 10 and 9).
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Precision {
    /// A year (9).
    Year,
    /// A month (10).
    Month,
    /// A day (11, or finer).
    Day,
}

/// A name-search candidate (`wbsearchentities`). Matching it to a company is doewood-enrich's.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SearchHit {
    /// The item.
    pub qid: QId,
    /// Its label.
    pub label: Option<String>,
    /// Its description.
    pub description: Option<String>,
    /// The label or alias the search matched.
    pub matched: Option<String>,
    /// Always [`TrustTier::Community`].
    pub tier: TrustTier,
}

const COMMONS_FILE_PATH: &str = "https://commons.wikimedia.org/wiki/Special:FilePath/";

#[derive(Deserialize)]
struct Results {
    results: Bindings,
}

#[derive(Deserialize)]
struct Bindings {
    bindings: Vec<HashMap<String, Term>>,
}

#[derive(Deserialize)]
struct Term {
    #[serde(rename = "type")]
    kind: String,
    value: String,
}

#[derive(Default)]
struct Statement {
    prop: String,
    value: Option<Term>,
    label: Option<String>,
    human: bool,
    preferred: bool,
    deprecated: bool,
    period: Period,
}

/// The items of a SPARQL answer, sorted by id. Rows with an unknown property or value are
/// skipped; an answer that is not SPARQL JSON, or names an item that is not an item IRI, is
/// refused.
pub(crate) fn items(body: &[u8]) -> Result<Vec<WikidataItem>, String> {
    let r: Results = serde_json::from_slice(body).map_err(|e| format!("SPARQL JSON: {e}"))?;
    if r.results.bindings.len() >= ROW_LIMIT {
        tracing::warn!(
            rows = ROW_LIMIT,
            "wikidata: answer reached the row limit, truncated"
        );
    }
    // item -> (label, statement IRI -> statement)
    let mut by_item: BTreeMap<QId, (Option<String>, BTreeMap<String, Statement>)> = BTreeMap::new();
    for mut row in r.results.bindings {
        let item = row
            .get("item")
            .filter(|t| t.kind == "uri")
            .and_then(|t| QId::from_iri(&t.value))
            .ok_or("a row without a Wikidata item")?;
        let entry = by_item.entry(item).or_default();
        if entry.0.is_none() {
            entry.0 = literal(&mut row, "itemLabel");
        }
        let (Some(prop), Some(st)) = (literal(&mut row, "prop"), row.remove("st")) else {
            continue;
        };
        let s = entry.1.entry(st.value).or_insert_with(|| Statement {
            prop,
            ..Statement::default()
        });
        if s.value.is_none() {
            s.value = row.remove("value");
        }
        if s.label.is_none() {
            s.label = literal(&mut row, "valueLabel");
        }
        s.human |= row.get("human").is_some_and(|t| t.value == "true");
        if let Some(rank) = row.get("rank") {
            s.deprecated |= rank.value.ends_with("#DeprecatedRank");
            s.preferred |= rank.value.ends_with("#PreferredRank");
        }
        // Several start or end times on one statement: the earliest start, the latest end.
        if let Some(t) = time_of(&row, "start") {
            s.period.start = Some(s.period.start.map_or(t, |x| x.min(t)));
        }
        if let Some(t) = time_of(&row, "end") {
            s.period.end = Some(s.period.end.map_or(t, |x| x.max(t)));
        }
    }
    Ok(by_item
        .into_iter()
        .map(|(qid, (label, statements))| item(qid, label, statements.into_values()))
        .collect())
}

fn item(
    qid: QId,
    label: Option<String>,
    statements: impl Iterator<Item = Statement>,
) -> WikidataItem {
    let mut it = WikidataItem {
        qid,
        label,
        leis: Vec::new(),
        parent_organizations: Vec::new(),
        owned_by: Vec::new(),
        replaced_by: Vec::new(),
        followed_by: Vec::new(),
        websites: Vec::new(),
        logos: Vec::new(),
        tier: TrustTier::Community,
    };
    for s in statements {
        let Some(value) = s.value.as_ref().filter(|_| !s.deprecated) else {
            continue;
        };
        let relation = || {
            (value.kind == "uri")
                .then(|| QId::from_iri(&value.value))
                .flatten()
                .filter(|t| *t != qid)
                .map(|target| Relation {
                    target,
                    label: s.label.clone(),
                    human: s.human,
                    period: s.period,
                    preferred: s.preferred,
                })
        };
        match s.prop.as_str() {
            "P749" => it.parent_organizations.extend(relation()),
            "P127" => it.owned_by.extend(relation()),
            "P1366" => it.replaced_by.extend(relation()),
            "P156" => it.followed_by.extend(relation()),
            "P856" => it.websites.extend(website(value).map(|url| Website {
                url,
                period: s.period,
                preferred: s.preferred,
            })),
            "P154" => it.logos.extend(logo(value).map(|(file, url)| Logo {
                file,
                url,
                period: s.period,
                preferred: s.preferred,
            })),
            "P1278" if value.kind == "literal" => {
                it.leis.extend(Identifier::parse(IdKind::Lei, &value.value));
            }
            _ => {}
        }
    }
    for v in [
        &mut it.parent_organizations,
        &mut it.owned_by,
        &mut it.replaced_by,
        &mut it.followed_by,
    ] {
        v.sort_by_key(|r| (r.period.start, r.target, r.period.end));
        v.dedup();
    }
    it.websites
        .sort_by(|a, b| a.url.as_str().cmp(b.url.as_str()));
    it.websites.dedup();
    it.logos.sort_by(|a, b| a.file.cmp(&b.file));
    it.logos.dedup();
    it.leis.sort_by(|a, b| a.value.cmp(&b.value));
    it.leis.dedup();
    it
}

/// A literal's text (trimmed, non-empty).
fn literal(row: &mut HashMap<String, Term>, var: &str) -> Option<String> {
    row.remove(var)
        .filter(|t| t.kind == "literal")
        .and_then(|t| text(Some(t.value)))
}

/// The time `var` at the precision in `{var}Precision` (missing: year).
fn time_of(row: &HashMap<String, Term>, var: &str) -> Option<WikiDate> {
    let t = row.get(var).filter(|t| t.kind == "literal")?;
    let precision = match row
        .get(&format!("{var}Precision"))
        .and_then(|p| p.value.trim().parse::<u8>().ok())
    {
        Some(11..) => Precision::Day,
        Some(10) => Precision::Month,
        Some(9) | None => Precision::Year,
        Some(_) => return None,
    };
    let d = date(Some(&t.value))?;
    let date = match precision {
        Precision::Day => d,
        Precision::Month => d.replace_day(1).ok()?,
        Precision::Year => Date::from_ordinal_date(d.year(), 1).ok()?,
    };
    Some(WikiDate { date, precision })
}

fn website(value: &Term) -> Option<Url> {
    if value.kind != "uri" {
        return None;
    }
    let url = Url::parse(value.value.trim()).ok()?;
    (matches!(url.scheme(), "http" | "https") && url.host_str().is_some()).then_some(url)
}

/// The Commons file of a logo value: the `Special:FilePath` IRI the query service gives, or a
/// bare file name.
fn logo(value: &Term) -> Option<(String, Url)> {
    let raw = match value.kind.as_str() {
        "uri" => {
            let url = Url::parse(&value.value).ok()?;
            if !matches!(url.host_str(), Some("commons.wikimedia.org")) {
                return None;
            }
            let name = url.path().strip_prefix("/wiki/Special:FilePath/")?;
            percent_decode(name)?
        }
        "literal" => value.value.clone(),
        _ => return None,
    };
    let file = raw.trim().replace(' ', "_");
    if file.is_empty()
        || file.len() > 240
        || file.contains(['/', '\\', '#', '?', '|', '[', ']', '{', '}', '<', '>'])
        || file.chars().any(char::is_control)
        || !file.contains('.')
    {
        return None;
    }
    let mut url = Url::parse(COMMONS_FILE_PATH).ok()?;
    url.path_segments_mut().ok()?.pop().push(&file);
    Some((file, url))
}

fn percent_decode(s: &str) -> Option<String> {
    let b = s.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        if b[i] == b'%' {
            let hex = s.get(i + 1..i + 3)?;
            out.push(u8::from_str_radix(hex, 16).ok()?);
            i += 3;
        } else {
            out.push(b[i]);
            i += 1;
        }
    }
    String::from_utf8(out).ok()
}

#[derive(Deserialize)]
struct Search {
    #[serde(default)]
    search: Vec<Hit>,
}

#[derive(Deserialize)]
struct Hit {
    id: Option<String>,
    label: Option<String>,
    description: Option<String>,
    #[serde(rename = "match")]
    matched: Option<Match>,
}

#[derive(Deserialize)]
struct Match {
    text: Option<String>,
}

/// The candidates of a `wbsearchentities` answer; hits without an item id are skipped.
pub(crate) fn search(body: &[u8]) -> Result<Vec<SearchHit>, String> {
    let s: Search = serde_json::from_slice(body).map_err(|e| format!("search JSON: {e}"))?;
    let mut hits: Vec<SearchHit> = Vec::new();
    for h in s.search {
        let Some(qid) = h.id.as_deref().and_then(QId::parse) else {
            continue;
        };
        if hits.iter().any(|x| x.qid == qid) {
            continue;
        }
        hits.push(SearchHit {
            qid,
            label: text(h.label),
            description: text(h.description),
            matched: text(h.matched.and_then(|m| m.text)),
            tier: TrustTier::Community,
        });
    }
    Ok(hits)
}

#[derive(Deserialize)]
struct ApiError {
    error: ApiErrorBody,
}

#[derive(Deserialize)]
struct ApiErrorBody {
    code: Option<String>,
}

/// The error code of a `MediaWiki` API error answer (`{"error": {"code": ...}}`), sanitized to
/// at most 64 ASCII letters, digits, `-` and `_`.
pub(crate) fn api_error(body: &[u8]) -> Option<String> {
    let e: ApiError = serde_json::from_slice(body).ok()?;
    let code: String = e
        .error
        .code
        .unwrap_or_default()
        .chars()
        .filter(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_'))
        .take(64)
        .collect();
    Some(if code.is_empty() {
        "unknown".to_owned()
    } else {
        code
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn term(kind: &str, value: &str) -> Term {
        Term {
            kind: kind.to_owned(),
            value: value.to_owned(),
        }
    }

    #[test]
    fn logos_become_commons_urls() {
        let (file, url) = logo(&term(
            "uri",
            "http://commons.wikimedia.org/wiki/Special:FilePath/Fictive%20Pumps%20logo.svg",
        ))
        .unwrap();
        assert_eq!(file, "Fictive_Pumps_logo.svg");
        assert_eq!(
            url.as_str(),
            "https://commons.wikimedia.org/wiki/Special:FilePath/Fictive_Pumps_logo.svg"
        );
        let (file, url) = logo(&term("literal", "Ünïcode Logo.png")).unwrap();
        assert_eq!(file, "Ünïcode_Logo.png");
        assert_eq!(
            url.as_str(),
            "https://commons.wikimedia.org/wiki/Special:FilePath/%C3%9Cn%C3%AFcode_Logo.png"
        );
        for bad in [
            term("uri", "http://example.invalid/wiki/Special:FilePath/X.svg"),
            term("uri", "http://commons.wikimedia.org/wiki/File:X.svg"),
            term("literal", "../../etc/passwd"),
            term("literal", "no extension"),
            term("literal", "a\nb.png"),
            term("bnode", "b0"),
            term(
                "uri",
                "http://commons.wikimedia.org/wiki/Special:FilePath/%ZZ.svg",
            ),
        ] {
            assert_eq!(logo(&bad), None, "{}", bad.value);
        }
    }

    #[test]
    fn websites_are_http() {
        assert!(website(&term("uri", "https://fictive.example/")).is_some());
        assert!(website(&term("uri", "javascript:alert(1)")).is_none());
        assert!(website(&term("uri", "ftp://fictive.example/")).is_none());
        assert!(website(&term("literal", "https://fictive.example/")).is_none());
    }

    #[test]
    fn api_error_codes_are_sanitized() {
        assert_eq!(
            api_error(br#"{"error":{"code":"maxlag","info":"x"}}"#),
            Some("maxlag".to_owned())
        );
        assert_eq!(
            api_error(br#"{"error":{"code":"bad\ncode<script>"}}"#),
            Some("badcodescript".to_owned())
        );
        assert_eq!(api_error(br#"{"error":{}}"#), Some("unknown".to_owned()));
        assert_eq!(api_error(br#"{"search":[]}"#), None);
        assert_eq!(api_error(b"not json"), None);
    }
}
