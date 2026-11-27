//! What the graph and search need from a record, read from the record's JSON (the form both
//! stores keep), so a record analyzed now and one read back from the store go the same way.

use std::borrow::Cow;

use quail_core::{Evidence, Money, Value};
use serde::Deserialize;
use serde::de::{Deserializer as _, IgnoredAny, MapAccess, Visitor};
use serde_json::value::RawValue;
use time::{Date, Month};

use doewood_model::{SIMILAR_MAX_KEYS, SimilarKey, SimilarKeyKind, SimilarKeys};

use crate::company::{IdKind, Identifier};
use crate::keyfacts::KeyFacts;
use crate::payment::PaymentFacts;

/// Most bytes of a party mention's span apart from an identifier for it to belong to that
/// party (same page).
pub const ATTACH_WINDOW: u32 = 400;

/// Most search body kept, in bytes.
const MAX_BODY: usize = 32 * 1024;

/// A record's JSON as it is read: borrowed from the text, each fact's value left as text
/// until it is needed (issue #98). Unknown fields are skipped without being built.
#[derive(Deserialize)]
struct RawRecord<'a> {
    #[serde(default, borrow)]
    format: Cow<'a, str>,
    #[serde(default, borrow)]
    facts: Vec<RawFact<'a>>,
}

/// One fact of a [`RawRecord`].
#[derive(Deserialize)]
struct RawFact<'a> {
    #[serde(borrow)]
    key: Cow<'a, str>,
    #[serde(borrow)]
    value: &'a RawValue,
    #[serde(default)]
    conf: f32,
    #[serde(default, borrow)]
    evidence: Option<&'a RawValue>,
}

impl<'a> RawRecord<'a> {
    /// The record, or `None` if `body` is not one.
    fn read(body: &'a RawValue) -> Option<Self> {
        serde_json::from_str(body.get()).ok()
    }
}

impl RawFact<'_> {
    /// The fact's value, or `None` if it is not a quail value.
    fn value(&self) -> Option<Value> {
        serde_json::from_str(self.value.get()).ok()
    }

    /// The fact's value and evidence read, or `None` if either is not quail's.
    fn view(&self) -> Option<FactView> {
        Some(FactView {
            key: self.key.clone().into_owned(),
            value: self.value()?,
            conf: self.conf,
            evidence: match self.evidence {
                Some(e) => serde_json::from_str(e.get()).ok()?,
                None => None,
            },
        })
    }
}

/// `raw[name]`, as `Value` indexing reads it: the member's text if `raw` is an object with
/// it (the last one, if repeated), without building the object.
pub(crate) fn member<'a>(raw: &'a RawValue, name: &str) -> Option<&'a RawValue> {
    struct Find<'n>(&'n str);
    impl<'de> Visitor<'de> for Find<'_> {
        type Value = Option<&'de RawValue>;
        fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
            f.write_str("an object")
        }
        fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Self::Value, A::Error> {
            let mut found = None;
            while let Some(k) = map.next_key::<Text<'de>>()? {
                if k.0 == self.0 {
                    found = Some(map.next_value::<&'de RawValue>()?);
                } else {
                    map.next_value::<IgnoredAny>()?;
                }
            }
            Ok(found)
        }
    }
    serde_json::Deserializer::from_str(raw.get())
        .deserialize_map(Find(name))
        .ok()
        .flatten()
}

/// A JSON string, borrowed from the text when it has no escapes.
#[derive(Deserialize)]
pub(crate) struct Text<'a>(#[serde(borrow)] pub Cow<'a, str>);

/// `raw.as_str()`: the text of a JSON string, `None` for anything else.
pub(crate) fn text(raw: Option<&RawValue>) -> Option<Cow<'_, str>> {
    serde_json::from_str::<Text<'_>>(raw?.get())
        .ok()
        .map(|t| t.0)
}

/// The elements of `raw` if it is an array (`raw.as_array()`).
pub(crate) fn elements(raw: Option<&RawValue>) -> Vec<&RawValue> {
    raw.and_then(|r| serde_json::from_str(r.get()).ok())
        .unwrap_or_default()
}

struct RecordView {
    format: String,
    facts: Vec<FactView>,
}

struct FactView {
    key: String,
    value: Value,
    conf: f32,
    evidence: Option<Evidence>,
}

/// The fact keys [`DocFacts`] reads (and the `signal.*` identifiers, see [`wanted`]).
const KEYS: &[&str] = &[
    "summary.card",
    "summary.card_text",
    "summary.extractive",
    "summary.keywords",
    "ref.title",
    "ref.instrument_kind",
    "ref.doc_number",
    "ref.cites",
    "pdf.title",
    "docx.title",
    "sheet.table",
    "sheet.text",
    "signal.date_invoice",
    "signal.date_signed",
    "signal.date_effective",
    "signal.date",
    "signal.amount_total",
    "signal.amount_gross",
    "signal.amount",
    "party.candidates",
    "party.company",
    "party.alias_of",
    "sketch.minhash",
    "sketch.minhash_bands",
    // Typed similarity (issue #169).
    "sketch.text_key",
    "text.chars",
    "sketch.simhash_template",
    "sketch.headings",
    "docx.template",
    "docx.attached_template",
    "docx.rsid_root",
    "docx.w15_doc_id",
    "pdf.xmp_document_id",
    "pdf.xmp_original_document_id",
];

/// Whether [`DocFacts`] reads facts of this key: only those are read into quail values.
fn wanted(key: &str) -> bool {
    KEYS.contains(&key)
        || crate::payment::PAYMENT_KEYS.contains(&key)
        || key.starts_with("card.")
        || key.ends_with(".highlight")
        || IdKind::from_fact_key(key).is_some()
}

impl RecordView {
    /// The record with the facts `keep` lets through read; `None` if it is not a record, or
    /// one of those facts is not quail's (as when every fact was read).
    fn read(body: &RawValue, keep: impl Fn(&str) -> bool) -> Option<Self> {
        let r = RawRecord::read(body)?;
        let facts = r
            .facts
            .iter()
            .filter(|f| keep(&f.key))
            .map(RawFact::view)
            .collect::<Option<Vec<_>>>()?;
        Some(Self {
            format: r.format.into_owned(),
            facts,
        })
    }
}

/// A company a document names.
#[derive(Clone, Debug, PartialEq)]
pub struct Party {
    /// quail's key for it.
    pub quail_key: String,
    /// The name as written (the best mention).
    pub surface: String,
    /// quail's candidate score, 0 to 1.
    pub score: f32,
    /// Roles, best first.
    pub roles: Vec<String>,
    /// The canonical legal form quail read, if any.
    pub legal_form: Option<String>,
    /// ISO 3166 codes quail guessed.
    pub jurisdiction: Option<String>,
    /// Validated identifiers found next to a mention of it.
    pub ids: Vec<Identifier>,
    /// Where it is mentioned: `(page, start, end)`.
    pub mentions: Vec<(Option<u32>, u32, u32)>,
}

/// A document another one cites.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Cite {
    /// `amends`, `annex_to`, `references`, ... (quail's relation).
    pub relation: String,
    /// The cited number's key, as quail normalizes own numbers too.
    pub key: String,
    /// As written.
    pub raw: String,
    /// The cited kind, if known.
    pub kind: Option<String>,
}

/// A name statement: `current` was `other` (former, trading or d/b/a name).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NameStatement {
    /// The name in use.
    pub current: String,
    /// The other name.
    pub other: String,
    /// `former_name`, `trading_name` or `dba`.
    pub kind: String,
}

/// The facts of one record ingest uses.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct DocFacts {
    /// The record's format.
    pub format: String,
    /// The title for search.
    pub title: Option<String>,
    /// The document kind.
    pub kind: Option<String>,
    /// The document's own date.
    pub date: Option<Date>,
    /// The main amount.
    pub amount: Option<f64>,
    /// Text for search: the summary card, keywords, the extractive summary, sheet text.
    pub body: String,
    /// The companies, best first.
    pub parties: Vec<Party>,
    /// Name statements.
    pub statements: Vec<NameStatement>,
    /// The document's own number keys.
    pub numbers: Vec<String>,
    /// The documents it cites.
    pub cites: Vec<Cite>,
    /// The `MinHash` signature and its LSH band keys.
    pub minhash: Option<(Vec<u8>, Vec<u64>)>,
    /// What typed similarity reads besides the `MinHash` (issue #169).
    pub similarity: SimilarityFacts,
    /// What is paid for and how much (issue #193).
    pub payment: Option<PaymentFacts>,
    /// The key-facts card and every highlight (issue #194).
    pub key_facts: Option<KeyFacts>,
}

/// The facts behind the typed similarity relations other than the `MinHash` ones (issue
/// #169): the folded-text key and length (identical), the masked `SimHash`, template
/// properties and heading skeleton (same template), and the document identity ids (same
/// origin).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SimilarityFacts {
    /// The first 64 bits of `sketch.text_key` (big-endian).
    pub text_key: Option<u64>,
    /// `text.chars`: characters of the text, whitespace excluded.
    pub text_chars: Option<u64>,
    /// `sketch.simhash_template`: the `SimHash` of the entity-masked words.
    pub masked: Option<u64>,
    /// `sketch.headings`: the heading skeleton digest.
    pub headings: Option<u64>,
    /// Template names: `docx.template` and the file name of `docx.attached_template`,
    /// lowercased; Word's default `Normal` template is left out (nearly every document has
    /// it).
    pub templates: Vec<String>,
    /// Document identity ids: XMP `DocumentID` / `OriginalDocumentID` (one namespace, so an
    /// original's id meets its copies' `OriginalDocumentID`) and `w15:docId`, lowercased, a
    /// GUID's braces dropped.
    pub origins: Vec<(SimilarKeyKind, String)>,
    /// `docx.rsid_root`.
    pub rsid_root: Option<u64>,
}

impl SimilarityFacts {
    /// The document's similarity keys, each once, with its masked `SimHash`.
    pub fn keys(&self) -> SimilarKeys {
        let mut keys = Vec::new();
        if let Some(k) = self.text_key {
            keys.push(SimilarKey {
                kind: SimilarKeyKind::Text,
                key: k,
            });
        }
        for t in &self.templates {
            keys.push(SimilarKey {
                kind: SimilarKeyKind::TemplateProperty,
                key: key_hash(SimilarKeyKind::TemplateProperty, t),
            });
        }
        if let Some(h) = self.headings {
            keys.push(SimilarKey {
                kind: SimilarKeyKind::Headings,
                key: h,
            });
        }
        for (kind, id) in &self.origins {
            keys.push(SimilarKey {
                kind: *kind,
                key: key_hash(*kind, id),
            });
        }
        if let Some(r) = self.rsid_root {
            keys.push(SimilarKey {
                kind: SimilarKeyKind::OriginRsidRoot,
                key: r,
            });
        }
        keys.sort();
        keys.dedup();
        keys.truncate(SIMILAR_MAX_KEYS);
        SimilarKeys {
            keys,
            masked: self.masked,
        }
    }
}

/// A text key's 64 bits: the first 8 bytes of BLAKE3 over the kind's name and the text, so
/// equal texts of one kind give equal keys.
fn key_hash(kind: SimilarKeyKind, text: &str) -> u64 {
    let mut h = blake3::Hasher::new();
    h.update(kind.as_str().as_bytes());
    h.update(&[0]);
    h.update(text.as_bytes());
    let mut out = [0u8; 8];
    out.copy_from_slice(&h.finalize().as_bytes()[..8]);
    u64::from_be_bytes(out)
}

/// A template name as a key: the file name (after the last `/` or `\`), trimmed and
/// lowercased; `None` for an empty one and for Word's default `Normal` template.
fn template_name(raw: &str) -> Option<String> {
    let name = raw
        .rsplit(['/', '\\'])
        .next()
        .unwrap_or(raw)
        .trim()
        .to_lowercase();
    let stem = name
        .rsplit_once('.')
        .map_or(name.as_str(), |(stem, _)| stem);
    (!name.is_empty() && stem != "normal").then_some(name)
}

/// A document identity id as a key: trimmed and lowercased, a GUID's braces dropped;
/// `None` when empty.
fn origin_id(raw: &str) -> Option<String> {
    let id = raw
        .trim()
        .trim_start_matches('{')
        .trim_end_matches('}')
        .trim();
    (!id.is_empty()).then(|| id.to_lowercase())
}

impl DocFacts {
    /// Reads a record's JSON. A body that is not a record gives empty facts. Only the facts
    /// it uses are read into quail values; the rest are skipped as text (issue #98).
    pub fn from_record(body: &RawValue) -> Self {
        RecordView::read(body, wanted).map_or_else(Self::default, |r| Self::from_view(&r))
    }

    /// [`from_record`](Self::from_record) reading every fact, as before issue #98: for the
    /// tests that check the facts it skips change nothing.
    #[doc(hidden)]
    pub fn from_record_every_fact(body: &RawValue) -> Self {
        RecordView::read(body, |_| true).map_or_else(Self::default, |r| Self::from_view(&r))
    }

    fn from_view(r: &RecordView) -> Self {
        let mut d = Self {
            format: r.format.clone(),
            ..Self::default()
        };
        let all = |key: &'static str| r.facts.iter().filter(move |f| f.key == key);
        let best = |key: &'static str| all(key).max_by(|a, b| a.conf.total_cmp(&b.conf));
        let card = best("summary.card").map(|f| &f.value);
        d.title = best("ref.title")
            .and_then(|f| f.value.as_text())
            .or_else(|| card.and_then(|c| field_text(c, "title")))
            .or_else(|| best("pdf.title").and_then(|f| f.value.as_text()))
            .or_else(|| best("docx.title").and_then(|f| f.value.as_text()))
            .map(str::to_owned)
            .or_else(|| {
                all("sheet.table")
                    .find_map(|f| field_text(&f.value, "title"))
                    .map(str::to_owned)
            });
        d.kind = card
            .and_then(|c| field_text(c, "kind"))
            .or_else(|| best("ref.instrument_kind").and_then(|f| f.value.as_text()))
            .map(str::to_owned);
        d.date = card.and_then(card_date).or_else(|| {
            [
                "signal.date_invoice",
                "signal.date_signed",
                "signal.date_effective",
                "signal.date",
            ]
            .into_iter()
            .find_map(|k| best(k).and_then(|f| date(&f.value)))
        });
        d.amount = card.and_then(card_amount).or_else(|| {
            [
                "signal.amount_total",
                "signal.amount_gross",
                "signal.amount",
            ]
            .into_iter()
            .find_map(|k| best(k).and_then(|f| money(&f.value)))
        });
        d.body = body_text(r);
        d.parties = parties(r);
        d.statements = all("party.alias_of")
            .filter_map(|f| {
                Some(NameStatement {
                    current: field_text(&f.value, "current")?.to_owned(),
                    other: field_text(&f.value, "other")?.to_owned(),
                    kind: field_text(&f.value, "kind").unwrap_or("alias").to_owned(),
                })
            })
            .collect();
        for f in all("ref.doc_number") {
            if let Some(k) = field_text(&f.value, "key").filter(|k| !k.is_empty())
                && !d.numbers.iter().any(|n| n == k)
            {
                d.numbers.push(k.to_owned());
            }
        }
        for f in all("ref.cites") {
            let Some(key) = field_text(&f.value, "key").filter(|k| !k.is_empty()) else {
                continue;
            };
            if d.cites.iter().any(|c| c.key == key) {
                continue;
            }
            d.cites.push(Cite {
                relation: field_text(&f.value, "relation")
                    .unwrap_or("references")
                    .to_owned(),
                key: key.to_owned(),
                raw: field_text(&f.value, "raw").unwrap_or(key).to_owned(),
                kind: field_text(&f.value, "kind").map(str::to_owned),
            });
        }
        if let (Some(Value::Bytes(sig)), Some(Value::List(bands))) = (
            best("sketch.minhash").map(|f| &f.value),
            best("sketch.minhash_bands").map(|f| &f.value),
        ) {
            let bands: Vec<u64> = bands
                .iter()
                .filter_map(|b| match b {
                    Value::U64(x) => Some(*x),
                    _ => None,
                })
                .collect();
            if !sig.is_empty() && !bands.is_empty() {
                d.minhash = Some((sig.to_vec(), bands));
            }
        }
        d.similarity = similarity(&best, &all);
        d.payment = PaymentFacts::read(r.facts.iter().map(|f| (f.key.as_str(), &f.value, f.conf)));
        d.key_facts = KeyFacts::read(r.facts.iter().map(|f| (f.key.as_str(), &f.value, f.conf)));
        d
    }
}

/// [`SimilarityFacts`] from a record's facts (`best` is the most confident of a key, `all`
/// every one).
fn similarity<'a, B, A, I>(best: &B, all: &A) -> SimilarityFacts
where
    B: Fn(&'static str) -> Option<&'a FactView>,
    A: Fn(&'static str) -> I,
    I: Iterator<Item = &'a FactView>,
{
    let u64_of = |key: &'static str| match best(key).map(|f| &f.value) {
        Some(Value::U64(x)) => Some(*x),
        Some(Value::Int(x)) => u64::try_from(*x).ok(),
        _ => None,
    };
    let text_key = match best("sketch.text_key").map(|f| &f.value) {
        Some(Value::Bytes(b)) if b.len() >= 8 => {
            let mut k = [0u8; 8];
            k.copy_from_slice(&b[..8]);
            Some(u64::from_be_bytes(k))
        }
        _ => None,
    };
    let mut templates: Vec<String> = ["docx.template", "docx.attached_template"]
        .into_iter()
        .flat_map(all)
        .filter_map(|f| f.value.as_text().and_then(template_name))
        .collect();
    templates.sort();
    templates.dedup();
    let mut origins: Vec<(SimilarKeyKind, String)> = [
        ("pdf.xmp_document_id", SimilarKeyKind::OriginXmp),
        ("pdf.xmp_original_document_id", SimilarKeyKind::OriginXmp),
        ("docx.w15_doc_id", SimilarKeyKind::OriginW15),
    ]
    .into_iter()
    .flat_map(|(key, kind)| {
        all(key).filter_map(move |f| f.value.as_text().and_then(origin_id).map(|id| (kind, id)))
    })
    .collect();
    origins.sort();
    origins.dedup();
    SimilarityFacts {
        text_key,
        text_chars: u64_of("text.chars"),
        masked: u64_of("sketch.simhash_template"),
        headings: u64_of("sketch.headings"),
        templates,
        origins,
        rsid_root: u64_of("docx.rsid_root").filter(|r| *r != 0),
    }
}

fn field_text<'a>(v: &'a Value, name: &str) -> Option<&'a str> {
    v.field(name)
        .and_then(Value::as_text)
        .filter(|s| !s.trim().is_empty())
}

fn date(v: &Value) -> Option<Date> {
    match v {
        Value::Date(d) => {
            Date::from_calendar_date(i32::from(d.y), Month::try_from(d.m).ok()?, d.d).ok()
        }
        _ => None,
    }
}

fn money(v: &Value) -> Option<f64> {
    match v {
        Value::Money(m) => Some(money_value(*m)),
        _ => None,
    }
}

#[allow(clippy::cast_precision_loss)] // amounts are far below 2^53
fn money_value(m: Money) -> f64 {
    m.units as f64 / 10f64.powi(i32::from(m.scale))
}

/// The first dated entry of the card's `dates`.
fn card_date(card: &Value) -> Option<Date> {
    match card.field("dates")? {
        Value::List(l) => l.iter().find_map(|d| d.field("date").and_then(date)),
        _ => None,
    }
}

/// The first amount of the card's `amounts`.
fn card_amount(card: &Value) -> Option<f64> {
    match card.field("amounts")? {
        Value::List(l) => l.iter().find_map(|a| a.field("amount").and_then(money)),
        _ => None,
    }
}

fn body_text(r: &RecordView) -> String {
    let mut parts: Vec<String> = Vec::new();
    for key in ["summary.card_text", "summary.extractive", "sheet.text"] {
        if let Some(t) = r
            .facts
            .iter()
            .find(|f| f.key == key)
            .and_then(|f| f.value.as_text())
        {
            parts.push(t.to_owned());
        }
    }
    if let Some(Value::List(kws)) = r
        .facts
        .iter()
        .find(|f| f.key == "summary.keywords")
        .map(|f| &f.value)
    {
        let terms: Vec<&str> = kws.iter().filter_map(|k| field_text(k, "term")).collect();
        if !terms.is_empty() {
            parts.push(terms.join(" "));
        }
    }
    let mut body = parts.join("\n");
    if body.len() > MAX_BODY {
        let mut end = MAX_BODY;
        while !body.is_char_boundary(end) {
            end -= 1;
        }
        body.truncate(end);
    }
    body
}

/// `party.candidates`, joined with the `party.company` mentions of the same key (legal form,
/// jurisdiction, where they are), and the validated `signal.*` identifiers next to a mention.
fn parties(r: &RecordView) -> Vec<Party> {
    let Some(Value::List(cands)) = r
        .facts
        .iter()
        .find(|f| f.key == "party.candidates")
        .map(|f| &f.value)
    else {
        return Vec::new();
    };
    let mut out: Vec<Party> = Vec::new();
    for c in cands {
        let (Some(key), Some(surface)) = (field_text(c, "key"), field_text(c, "surface")) else {
            continue;
        };
        if out.iter().any(|p| p.quail_key == key) {
            continue;
        }
        #[allow(clippy::cast_possible_truncation)]
        let score = match c.field("score") {
            Some(Value::Float(x)) => *x as f32,
            _ => 0.5,
        };
        let roles = match c.field("roles") {
            Some(Value::List(l)) => l
                .iter()
                .filter_map(|v| v.as_text().map(str::to_owned))
                .collect(),
            _ => Vec::new(),
        };
        let mut p = Party {
            quail_key: key.to_owned(),
            surface: surface.to_owned(),
            score: score.clamp(0.0, 1.0),
            roles,
            legal_form: None,
            jurisdiction: None,
            ids: Vec::new(),
            mentions: Vec::new(),
        };
        for m in r
            .facts
            .iter()
            .filter(|f| f.key == "party.company" && field_text(&f.value, "key") == Some(key))
        {
            if p.legal_form.is_none() {
                p.legal_form = field_text(&m.value, "legal_form").map(str::to_owned);
            }
            if p.jurisdiction.is_none() {
                p.jurisdiction = field_text(&m.value, "jurisdiction").map(str::to_owned);
            }
            if let Some(ev) = &m.evidence
                && let Some(span) = ev.span
            {
                p.mentions.push((ev.page, span.start, span.end));
            }
        }
        out.push(p);
    }
    attach_identifiers(r, &mut out);
    out
}

/// Gives each identifier to the party with the nearest mention on its page, within
/// [`ATTACH_WINDOW`] bytes; a tie gives it to nobody.
fn attach_identifiers(r: &RecordView, parties: &mut [Party]) {
    for f in &r.facts {
        let Some(kind) = IdKind::from_fact_key(&f.key) else {
            continue;
        };
        let Some(id) = f.value.as_text().and_then(|v| Identifier::parse(kind, v)) else {
            continue;
        };
        let Some((page, span)) = f.evidence.as_ref().and_then(|e| Some((e.page, e.span?))) else {
            continue;
        };
        let mut best: Option<(u32, usize)> = None;
        let mut tie = false;
        for (i, p) in parties.iter().enumerate() {
            for &(mp, start, end) in &p.mentions {
                if mp != page {
                    continue;
                }
                let gap = if span.start >= end {
                    span.start - end
                } else {
                    start.saturating_sub(span.end)
                };
                if gap > ATTACH_WINDOW {
                    continue;
                }
                match best {
                    Some((g, j)) if g == gap && j != i => tie = true,
                    Some((g, _)) if g <= gap => {}
                    _ => {
                        best = Some((gap, i));
                        tie = false;
                    }
                }
            }
        }
        if let (Some((_, i)), false) = (best, tie)
            && !parties[i].ids.contains(&id)
        {
            parties[i].ids.push(id);
        }
    }
    for p in parties {
        p.ids.sort();
    }
}

#[cfg(test)]
mod tests {
    use quail_core::{AnalyzerId, Fact, FactKey};
    use serde_json::json;

    use super::*;

    fn fact(key: &'static str, value: Value, page: u32, span: (u32, u32)) -> Fact {
        Fact {
            key: FactKey::new(key),
            value,
            conf: 0.9,
            src: AnalyzerId::new("test.step", 1),
            evidence: Some(Evidence {
                page: Some(page),
                span: Some(quail_core::Span {
                    start: span.0,
                    end: span.1,
                }),
                ..Evidence::default()
            }),
        }
    }

    fn record(facts: &[Fact]) -> Box<RawValue> {
        raw(&json!({"schema": 1, "format": "pdf", "facts": facts}))
    }

    fn raw(v: &serde_json::Value) -> Box<RawValue> {
        doewood_model::canonical::to_canonical(v).unwrap()
    }

    /// [`DocFacts::from_record`], checked against reading every fact (as before #98).
    fn read(body: &RawValue) -> DocFacts {
        let d = DocFacts::from_record(body);
        assert_eq!(d, DocFacts::from_record_every_fact(body));
        d
    }

    fn candidate(key: &str, surface: &str, score: f64, role: &str) -> Value {
        Value::record([
            ("key", Value::text(key)),
            ("surface", Value::text(surface)),
            ("score", Value::Float(score)),
            ("roles", Value::List(vec![Value::text(role)])),
        ])
    }

    fn mention(key: &str, form: &str) -> Value {
        Value::record([
            ("surface", Value::text(key)),
            ("key", Value::text(key)),
            ("legal_form", Value::text(form)),
        ])
    }

    /// A PIB with its MOD 11,10 check digit.
    pub(crate) fn pib(head: &str) -> String {
        let mut a = 10;
        for d in head.bytes().map(|b| u32::from(b - b'0')) {
            a = (a + d) % 10;
            if a == 0 {
                a = 10;
            }
            a = (a * 2) % 11;
        }
        format!("{head}{}", (11 - a) % 10)
    }

    #[test]
    fn identifiers_attach_to_the_nearest_mention_on_their_page() {
        let (p1, p2) = (pib("10123456"), pib("20654321"));
        let body = record(&[
            fact(
                "party.candidates",
                Value::List(vec![
                    candidate("alpha", "Alpha d.o.o.", 0.9, "supplier"),
                    candidate("beta", "Beta a.d.", 0.7, "customer"),
                ]),
                0,
                (0, 0),
            ),
            fact("party.company", mention("alpha", "d.o.o."), 0, (100, 112)),
            fact("party.company", mention("beta", "a.d."), 0, (900, 909)),
            // Right after Alpha; right after Beta; far from both; on another page.
            fact("signal.rs_pib", Value::text(p1.as_str()), 0, (130, 139)),
            fact("signal.rs_pib", Value::text(p2.as_str()), 0, (920, 929)),
            fact("signal.rs_mb", Value::text("10000009"), 0, (5000, 5008)),
            fact(
                "signal.lei",
                Value::text("NOTCHECKED0000000000"),
                0,
                (113, 133),
            ),
            fact("signal.rs_pib", Value::text("000000000"), 0, (115, 124)),
            fact("signal.rs_mb", Value::text("10000009"), 3, (101, 109)),
        ]);
        let d = read(&body);
        assert_eq!(d.parties.len(), 2);
        let alpha = &d.parties[0];
        assert_eq!(
            (
                alpha.surface.as_str(),
                alpha.legal_form.as_deref(),
                alpha.roles[0].as_str()
            ),
            ("Alpha d.o.o.", Some("d.o.o."), "supplier")
        );
        assert!((alpha.score - 0.9).abs() < 1e-6);
        assert_eq!(alpha.ids, [Identifier::parse(IdKind::RsPib, &p1).unwrap()]);
        assert_eq!(
            d.parties[1].ids,
            [Identifier::parse(IdKind::RsPib, &p2).unwrap()]
        );
    }

    #[test]
    fn a_tie_attaches_to_nobody() {
        let p1 = pib("10123456");
        let body = record(&[
            fact(
                "party.candidates",
                Value::List(vec![
                    candidate("alpha", "Alpha d.o.o.", 0.9, "supplier"),
                    candidate("beta", "Beta a.d.", 0.7, "customer"),
                ]),
                0,
                (0, 0),
            ),
            fact("party.company", mention("alpha", "d.o.o."), 0, (100, 110)),
            fact("party.company", mention("beta", "a.d."), 0, (130, 140)),
            fact("signal.rs_pib", Value::text(p1.as_str()), 0, (115, 125)),
        ]);
        let d = read(&body);
        assert!(d.parties.iter().all(|p| p.ids.is_empty()));
    }

    #[test]
    fn titles_dates_amounts_refs_and_sketches() {
        let card = Value::record([
            ("kind", Value::text("invoice")),
            ("title", Value::text("Card title")),
            (
                "dates",
                Value::List(vec![Value::record([
                    ("role", Value::text("invoice")),
                    (
                        "date",
                        Value::Date(quail_core::Date {
                            y: 2026,
                            m: 3,
                            d: 14,
                        }),
                    ),
                ])]),
            ),
            (
                "amounts",
                Value::List(vec![Value::record([
                    ("role", Value::text("total")),
                    (
                        "amount",
                        Value::Money(Money {
                            units: 420_050,
                            scale: 2,
                            currency: Some(*b"EUR"),
                        }),
                    ),
                ])]),
            ),
        ]);
        let cite = |rel: &str, key: &str| {
            Value::record([
                ("kind", Value::text("contract")),
                ("relation", Value::text(rel)),
                ("raw", Value::text(key.to_uppercase())),
                ("key", Value::text(key)),
            ])
        };
        let body = record(&[
            fact("summary.card", card, 0, (0, 0)),
            fact("summary.card_text", Value::text("kind: invoice"), 0, (0, 0)),
            fact(
                "summary.keywords",
                Value::List(vec![Value::record([("term", Value::text("widgets"))])]),
                0,
                (0, 0),
            ),
            fact("ref.title", Value::text("Invoice 7/2026"), 0, (0, 10)),
            fact(
                "ref.doc_number",
                Value::record([("key", Value::text("inv-7-2026"))]),
                0,
                (0, 0),
            ),
            fact("ref.cites", cite("amends", "c-114-23"), 0, (0, 0)),
            fact("ref.cites", cite("references", "po-1"), 0, (0, 0)),
            fact("ref.cites", cite("references", "po-1"), 0, (0, 0)),
            fact(
                "sketch.minhash",
                Value::Bytes(vec![1, 2, 3].into()),
                0,
                (0, 0),
            ),
            fact(
                "sketch.minhash_bands",
                Value::List(vec![Value::U64(7), Value::U64(u64::MAX)]),
                0,
                (0, 0),
            ),
        ]);
        let d = read(&body);
        assert_eq!(d.title.as_deref(), Some("Invoice 7/2026"));
        assert_eq!(d.kind.as_deref(), Some("invoice"));
        assert_eq!(
            d.date,
            Some(Date::from_calendar_date(2026, Month::March, 14).unwrap())
        );
        assert_eq!(d.amount, Some(4200.5));
        assert_eq!(d.body, "kind: invoice\nwidgets");
        assert_eq!(d.numbers, ["inv-7-2026"]);
        assert_eq!(
            d.cites
                .iter()
                .map(|c| (c.relation.as_str(), c.key.as_str()))
                .collect::<Vec<_>>(),
            [("amends", "c-114-23"), ("references", "po-1")]
        );
        assert_eq!(d.minhash, Some((vec![1, 2, 3], vec![7, u64::MAX])));
    }

    #[test]
    fn similarity_facts_and_keys() {
        let mut text_key = vec![0u8; 16];
        text_key[..8].copy_from_slice(&0x0102_0304_0506_0708u64.to_be_bytes());
        let body = record(&[
            fact("sketch.text_key", Value::Bytes(text_key.into()), 0, (0, 0)),
            fact("text.chars", Value::Int(4321), 0, (0, 0)),
            fact("sketch.simhash_template", Value::U64(99), 0, (0, 0)),
            fact("sketch.headings", Value::U64(7), 0, (0, 0)),
            fact("docx.template", Value::text("Invoice.dotx"), 0, (0, 0)),
            fact(
                "docx.attached_template",
                Value::text("C:\\Users\\x\\Templates\\invoice.dotx"),
                0,
                (0, 0),
            ),
            fact("docx.rsid_root", Value::U64(0x00ab_cdef), 0, (0, 0)),
            fact("docx.w15_doc_id", Value::text("{A1B2-C3}"), 0, (0, 0)),
            fact("pdf.xmp_document_id", Value::text(" uuid:ABC "), 0, (0, 0)),
            fact(
                "pdf.xmp_original_document_id",
                Value::text("uuid:abc"),
                0,
                (0, 0),
            ),
        ]);
        let d = read(&body);
        let s = &d.similarity;
        assert_eq!(s.text_key, Some(0x0102_0304_0506_0708));
        assert_eq!(
            (s.text_chars, s.masked, s.headings, s.rsid_root),
            (Some(4321), Some(99), Some(7), Some(0x00ab_cdef))
        );
        assert_eq!(s.templates, ["invoice.dotx"], "the same file name once");
        assert_eq!(
            s.origins,
            [
                (SimilarKeyKind::OriginXmp, "uuid:abc".to_owned()),
                (SimilarKeyKind::OriginW15, "a1b2-c3".to_owned()),
            ]
        );
        let keys = s.keys();
        assert_eq!(keys.masked, Some(99));
        let kinds: Vec<_> = keys.keys.iter().map(|k| k.kind).collect();
        assert_eq!(
            kinds,
            [
                SimilarKeyKind::Text,
                SimilarKeyKind::TemplateProperty,
                SimilarKeyKind::Headings,
                SimilarKeyKind::OriginXmp,
                SimilarKeyKind::OriginW15,
                SimilarKeyKind::OriginRsidRoot,
            ]
        );
        // Equal texts of a kind are equal keys, other kinds or texts are not.
        assert_eq!(
            key_hash(SimilarKeyKind::OriginXmp, "uuid:abc"),
            key_hash(SimilarKeyKind::OriginXmp, "uuid:abc")
        );
        assert_ne!(
            key_hash(SimilarKeyKind::OriginXmp, "uuid:abc"),
            key_hash(SimilarKeyKind::OriginW15, "uuid:abc")
        );
        // Word's default template, an empty one and a zero rsid are no keys; nor is a short
        // text key.
        for t in ["Normal.dotm", "normal.dot", "/x/Normal.dotx", " ", "NORMAL"] {
            assert_eq!(template_name(t), None, "{t}");
        }
        assert_eq!(
            template_name("Normal Invoice.dotx").as_deref(),
            Some("normal invoice.dotx")
        );
        assert_eq!(origin_id("{}"), None);
        let body = record(&[
            fact(
                "sketch.text_key",
                Value::Bytes(vec![1, 2, 3].into()),
                0,
                (0, 0),
            ),
            fact("docx.rsid_root", Value::U64(0), 0, (0, 0)),
        ]);
        assert_eq!(read(&body).similarity, SimilarityFacts::default());
        assert!(SimilarityFacts::default().keys().keys.is_empty());
    }

    #[test]
    fn a_record_without_facts_is_empty() {
        let d = read(&raw(&json!({"format": "unknown", "facts": []})));
        assert_eq!(d.format, "unknown");
        assert!(d.parties.is_empty() && d.title.is_none() && d.minhash.is_none());
        assert_eq!(read(&raw(&json!(42))), DocFacts::default());
    }
}
