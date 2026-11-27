//! Key facts for AI retrieval (issue #194): every document's normalised key-facts card, the
//! highlights of its facts over its text, and the fact index that finds documents by them.
//!
//! Ingest reads quail's `card.*` facts (`card.facts`, the one canonical value per field the
//! other steps found) and every `<namespace>.highlight` fact (`payment.highlight`,
//! `contract.highlight`, `card.highlight`, ...) from a record, and stores one [`FactRow`] per
//! record in the `document_facts` index table, beside the record:
//!
//! - the [`Card`] as canonical JSON, each field with its confidence and highlights, compact
//!   enough to hand to an LLM (`GetDocumentCard`);
//! - every highlight of the record ([`BlockHighlight`]), so `GetDocumentText` returns, per
//!   block, the facts over it;
//! - the card's searchable fields as columns ([`FactColumns`]), for `FindDocuments`.
//!
//! The row is rebuilt from the record whenever the document is written to the graph, and goes
//! with its record (`ON DELETE CASCADE`, a purge, issue #168). The supplier and customer
//! companies are read through the document's `party` edges marked `side`, as for payments
//! ([`crate::payment`]), so a merge moves them.
//!
//! **Derived dates.** [`Card::ends`] is the stated end date (`card.end`), else the start (or
//! the signing date) plus the stated term; [`Card::notice_due`] is that end minus the notice
//! period: the last day notice can be given so the contract does not run on.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;
use time::{Date, Duration, Month};
use uuid::Uuid;

use crate::{Blake3, Error, Result};

/// Most bytes of a row's [`FactRow::card`]: highlights are dropped, the longest field's
/// first, until it fits.
pub const CARD_MAX_BYTES: usize = 64 << 10;
/// Most bytes of a row's [`FactRow::highlights`]: highlights past it are left out.
pub const FACT_HIGHLIGHTS_MAX_BYTES: usize = 256 << 10;
/// Most highlights kept per document.
pub const FACT_HIGHLIGHTS_MAX: usize = 1024;
/// Most highlights kept per card field.
pub const CARD_FIELD_HIGHLIGHTS_MAX: usize = 8;
/// Most bytes of a highlight's quote in the card (longer ones are cut at a character).
pub const CARD_QUOTE_MAX: usize = 512;
/// Most bytes of a text column (a name, a subject, a court).
pub const FACT_TEXT_MAX: usize = 1024;
/// Most rows of one [`crate::FactStore::find_documents`] page.
pub const FACTS_MAX_LIMIT: u32 = 1000;

/// The page size [`crate::FactStore::find_documents`] uses for `limit`.
pub fn facts_limit(limit: u32) -> u32 {
    match limit {
        0 => 50,
        n => n.min(FACTS_MAX_LIMIT),
    }
}

/// A party on the card (`card.supplier`, `card.customer`).
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct CardParty {
    /// The name as written.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    /// Serbian tax number.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pib: Option<String>,
    /// Serbian registration number.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mb: Option<String>,
    /// VAT number.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub vat: Option<String>,
    /// Another registration number.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reg: Option<String>,
    /// A bank account or IBAN.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub account: Option<String>,
    /// The address.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub address: Option<String>,
    /// Who represents it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub representative: Option<String>,
}

/// A duration (`card.term`, `card.notice`, `card.warranty`): ISO 8601, with whole months or
/// days (years as 12 months, weeks as 7 days).
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct CardDuration {
    /// ISO 8601 (`P30D`, `P12M`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub iso: Option<String>,
    /// Whole months.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub months: Option<u32>,
    /// Whole days.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub days: Option<u32>,
    /// No fixed end (`card.term` only).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub indefinite: Option<bool>,
}

/// An advance payment (`card.advance`).
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct CardAdvance {
    /// Percent of the price.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub percent: Option<f64>,
    /// An amount (decimal text).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub amount: Option<String>,
    /// The amount's ISO 4217 code.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub currency: Option<String>,
}

/// A contractual penalty (`card.penalty`).
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct CardPenalty {
    /// Percent of the value.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub percent: Option<f64>,
    /// Per `day` or `week` of delay.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub per: Option<String>,
    /// A fixed amount (decimal text).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub amount: Option<String>,
    /// The amount's ISO 4217 code.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub currency: Option<String>,
    /// The cap, percent of the value.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cap_percent: Option<f64>,
}

/// A limit of liability (`card.liability_cap`).
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct CardLiabilityCap {
    /// An amount (decimal text).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub amount: Option<String>,
    /// The amount's ISO 4217 code.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub currency: Option<String>,
    /// Percent of `basis`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub percent: Option<f64>,
    /// `contract_value` or `fees_paid`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub basis: Option<String>,
    /// The fees of how many months.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub months: Option<u32>,
}

/// Service levels (`card.sla`).
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct CardSla {
    /// Availability, percent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub availability: Option<f64>,
    /// Response time, hours.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub response_hours: Option<u32>,
    /// Resolution time, hours.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resolution_hours: Option<u32>,
}

/// Where a card field's source fact sits in the text: `block` is the `TextBlock.index` of
/// `GetDocumentText`, `start` and `end` characters of that block's text (end exclusive).
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct CardHighlight {
    /// The page index, from 0.
    pub page: u32,
    /// The block's index among the document's blocks, from 0.
    pub block: u32,
    /// First character of the passage in the block.
    pub start: u32,
    /// One past its last character.
    pub end: u32,
    /// The passage as written (at most [`CARD_QUOTE_MAX`] bytes).
    pub quote: String,
}

/// How sure a card field is, and where it was read.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct CardEvidence {
    /// The field's confidence, 0 to 1, two decimals.
    pub conf: f64,
    /// Where its source facts sit, at most [`CARD_FIELD_HIGHLIGHTS_MAX`].
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub highlights: Vec<CardHighlight>,
}

/// The normalised key-facts card of one document (quail's `card.*`), in canonical JSON:
/// field names are quail's keys without `card.`, dates `YYYY-MM-DD`, amounts exact decimal
/// text, codes lowercase (the governing law an ISO 3166 code). A field the document does not
/// state is absent. Fields of a newer quail are kept in [`Card::other`].
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[allow(missing_docs)] // every field is documented in docs/api.md, "Key facts"
pub struct Card {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kind: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub number: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub supplier: Option<CardParty>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub customer: Option<CardParty>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub signed: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub start: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub end: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub issued: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub due: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub term: Option<CardDuration>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub auto_renewal: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub notice: Option<CardDuration>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub payment_days: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub payment_method: Option<String>,
    /// The price, decimal text, in [`Card::currency`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub amount: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub amount_basis: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub currency: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub period: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub subject: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub category: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub accounts: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub advance: Option<CardAdvance>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub indexation: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub penalty: Option<CardPenalty>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub guarantees: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub liability_cap: Option<CardLiabilityCap>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub confidentiality: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sla: Option<CardSla>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub warranty: Option<CardDuration>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub governing_law: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub court: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub arbitration: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub languages: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub annexes: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub amends: Option<String>,
    /// Derived: the stated end date, else the start (or signing) date plus the term.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ends: Option<String>,
    /// Derived: [`Card::ends`] minus the notice period.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub notice_due: Option<String>,
    /// Per field (its name above), its confidence and highlights.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub evidence: BTreeMap<String, CardEvidence>,
    /// Fields of a newer quail, as read.
    #[serde(default, flatten)]
    pub other: BTreeMap<String, serde_json::Value>,
}

/// `YYYY-MM-DD`.
pub fn format_day(d: Date) -> String {
    format!("{:04}-{:02}-{:02}", d.year(), u8::from(d.month()), d.day())
}

/// A `YYYY-MM-DD` day.
pub fn parse_day(s: &str) -> Option<Date> {
    let mut parts = s.splitn(3, '-');
    let y: i32 = parts.next()?.parse().ok()?;
    let m: u8 = parts.next()?.parse().ok()?;
    let d: u8 = parts.next()?.parse().ok()?;
    if s.len() != 10 {
        return None;
    }
    Date::from_calendar_date(y, Month::try_from(m).ok()?, d).ok()
}

/// `d` moved by `months` (negative: back), the day clamped to the month's last.
fn add_months(d: Date, months: i64) -> Option<Date> {
    let index = i64::from(d.year()) * 12 + i64::from(u8::from(d.month())) - 1 + months;
    let year = i32::try_from(index.div_euclid(12)).ok()?;
    let month = Month::try_from(u8::try_from(index.rem_euclid(12) + 1).ok()?).ok()?;
    let last = month.length(year);
    Date::from_calendar_date(year, month, d.day().min(last)).ok()
}

/// `d` moved by a duration's months and days, forward (`sign` 1) or back (-1).
fn shift(d: Date, by: &CardDuration, sign: i64) -> Option<Date> {
    if by.months.is_none() && by.days.is_none() {
        return None;
    }
    let mut d = d;
    if let Some(m) = by.months {
        d = add_months(d, sign * i64::from(m))?;
    }
    if let Some(days) = by.days {
        d = d.checked_add(Duration::days(sign * i64::from(days)))?;
    }
    Some(d)
}

impl Card {
    /// Fills [`Card::ends`] and [`Card::notice_due`] from the dates, the term and the notice.
    pub fn derive(&mut self) {
        let day = |s: &Option<String>| s.as_deref().and_then(parse_day);
        let ends = day(&self.end).or_else(|| {
            let term = self.term.as_ref()?;
            if term.indefinite == Some(true) {
                return None;
            }
            shift(day(&self.start).or_else(|| day(&self.signed))?, term, 1)
        });
        let notice_due = ends.and_then(|e| shift(e, self.notice.as_ref()?, -1));
        self.ends = ends.map(format_day);
        self.notice_due = notice_due.map(format_day);
    }

    /// Canonical JSON text, highlights dropped (from the field with the most) until it fits
    /// [`CARD_MAX_BYTES`].
    pub fn to_body(&self) -> Result<Box<RawValue>> {
        let mut c = self.clone();
        loop {
            let body = crate::canonical::to_canonical(&c)
                .map_err(|e| Error::Invalid(format!("card: {e}")))?;
            if body.get().len() <= CARD_MAX_BYTES {
                return Ok(body);
            }
            let most = c
                .evidence
                .values_mut()
                .max_by_key(|e| e.highlights.len())
                .filter(|e| !e.highlights.is_empty());
            match most {
                Some(e) => {
                    e.highlights.pop();
                }
                None => {
                    return Err(Error::Invalid(
                        "card: too large without highlights".to_owned(),
                    ));
                }
            }
        }
    }

    /// A card read from its JSON text.
    pub fn from_body(body: &RawValue) -> Result<Self> {
        serde_json::from_str(body.get()).map_err(|e| Error::Invalid(format!("card: {e}")))
    }
}

/// A highlight of any fact over the document text, as `GetDocumentText` returns it per block:
/// the fact key (`card.notice`, `contract.penalty`, `payment.amount`, ...), the block's
/// `TextBlock.index`, characters `start..end` of its text, and the fact's confidence.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct BlockHighlight {
    /// The fact key it proves.
    pub key: String,
    /// The page index, from 0.
    pub page: u32,
    /// The block's index among the document's blocks, from 0.
    pub block: u32,
    /// First character of the passage in the block.
    pub start: u32,
    /// One past its last character.
    pub end: u32,
    /// The fact's confidence, two decimals.
    pub conf: f64,
}

/// The highlights as stored: ordered by block, start, end, key; duplicates dropped; at most
/// [`FACT_HIGHLIGHTS_MAX`] and [`FACT_HIGHLIGHTS_MAX_BYTES`].
pub fn highlights_body(mut hs: Vec<BlockHighlight>) -> Result<Box<RawValue>> {
    hs.sort_by(|a, b| {
        (a.block, a.start, a.end, &a.key)
            .cmp(&(b.block, b.start, b.end, &b.key))
            .then(b.conf.total_cmp(&a.conf))
    });
    hs.dedup_by(|a, b| (a.block, a.start, a.end, &a.key) == (b.block, b.start, b.end, &b.key));
    hs.truncate(FACT_HIGHLIGHTS_MAX);
    loop {
        let body = crate::canonical::to_canonical(&hs)
            .map_err(|e| Error::Invalid(format!("highlights: {e}")))?;
        if body.get().len() <= FACT_HIGHLIGHTS_MAX_BYTES {
            return Ok(body);
        }
        let keep = hs.len() * FACT_HIGHLIGHTS_MAX_BYTES / body.get().len();
        hs.truncate(keep.min(hs.len() - 1));
    }
}

/// The searchable fields of one document's card, as the index table has them.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FactColumns {
    /// The record.
    pub blake3: Blake3,
    /// `card.kind`.
    pub kind: Option<String>,
    /// `card.number`.
    pub number: Option<String>,
    /// `card.subject`.
    pub subject: Option<String>,
    /// `card.supplier`'s name.
    pub supplier_name: Option<String>,
    /// `card.customer`'s name.
    pub customer_name: Option<String>,
    /// `card.category`.
    pub category: Option<String>,
    /// `card.amount` in units of 10^-[`crate::PAYMENT_SCALE`].
    pub amount: Option<i64>,
    /// `card.currency`.
    pub currency: Option<String>,
    /// `card.period`.
    pub period: Option<String>,
    /// `card.signed`.
    pub signed: Option<Date>,
    /// [`Card::ends`].
    pub ends: Option<Date>,
    /// [`Card::notice_due`].
    pub notice_due: Option<Date>,
    /// `card.auto_renewal`.
    pub auto_renewal: Option<bool>,
    /// `card.penalty` is stated.
    pub has_penalty: bool,
    /// `card.guarantees` is not empty.
    pub has_guarantee: bool,
    /// `card.governing_law`, an ISO 3166 code in capitals.
    pub governing_law: Option<String>,
    /// `card.court`.
    pub court: Option<String>,
}

/// Text cut to [`FACT_TEXT_MAX`] bytes at a character, NULs dropped, trimmed; `None` when
/// empty.
fn column_text(s: Option<&str>) -> Option<String> {
    let mut s: String = s?.trim().chars().filter(|c| *c != '\0').collect();
    if s.len() > FACT_TEXT_MAX {
        let mut end = FACT_TEXT_MAX;
        while !s.is_char_boundary(end) {
            end -= 1;
        }
        s.truncate(end);
    }
    (!s.is_empty()).then_some(s)
}

/// A decimal text (`1200000.00`, `-5`) in units of 10^-[`crate::PAYMENT_SCALE`]: rounded half away
/// from zero past 4 decimals; `None` when it is not one or does not fit.
pub fn parse_units(text: &str) -> Option<i64> {
    let t = text.trim();
    let (neg, t) = match t.strip_prefix('-') {
        Some(r) => (true, r),
        None => (false, t),
    };
    let (int, frac) = t.split_once('.').unwrap_or((t, ""));
    if int.is_empty() || !int.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    if !frac.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let digits = format!("{int}{frac}");
    let units: i64 = digits.parse().ok()?;
    let scale = u8::try_from(frac.len()).ok()?;
    crate::scaled_units(if neg { -units } else { units }, scale)
}

/// Text folded for a contains match: lowercase, the Serbian Latin letters with diacritics as
/// their base letters (`đ` as `dj`).
pub fn fold(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars().flat_map(char::to_lowercase) {
        match c {
            'č' | 'ć' => out.push('c'),
            'š' => out.push('s'),
            'ž' => out.push('z'),
            'đ' => out.push_str("dj"),
            c => out.push(c),
        }
    }
    out
}

impl FactColumns {
    /// The columns of `card`'s record `blake3` (after [`Card::derive`]).
    pub fn of(blake3: Blake3, card: &Card) -> Self {
        let day = |s: &Option<String>| s.as_deref().and_then(parse_day);
        Self {
            blake3,
            kind: column_text(card.kind.as_deref()),
            number: column_text(card.number.as_deref()),
            subject: column_text(card.subject.as_deref()),
            supplier_name: column_text(card.supplier.as_ref().and_then(|p| p.name.as_deref())),
            customer_name: column_text(card.customer.as_ref().and_then(|p| p.name.as_deref())),
            category: column_text(card.category.as_deref()),
            amount: card.amount.as_deref().and_then(parse_units),
            currency: column_text(card.currency.as_deref()).map(|c| c.to_ascii_uppercase()),
            period: column_text(card.period.as_deref()),
            signed: day(&card.signed),
            ends: day(&card.ends),
            notice_due: day(&card.notice_due),
            auto_renewal: card.auto_renewal,
            has_penalty: card.penalty.is_some(),
            has_guarantee: !card.guarantees.is_empty(),
            governing_law: column_text(card.governing_law.as_deref())
                .map(|c| c.to_ascii_uppercase()),
            court: column_text(card.court.as_deref()),
        }
    }

    fn validate(&self) -> Result<()> {
        let texts = [
            ("kind", &self.kind),
            ("number", &self.number),
            ("subject", &self.subject),
            ("supplier_name", &self.supplier_name),
            ("customer_name", &self.customer_name),
            ("category", &self.category),
            ("currency", &self.currency),
            ("period", &self.period),
            ("governing_law", &self.governing_law),
            ("court", &self.court),
        ];
        for (name, t) in texts {
            if t.as_ref().is_some_and(|t| t.len() > FACT_TEXT_MAX) {
                return Err(Error::Invalid(format!(
                    "document facts {name}: more than {FACT_TEXT_MAX} bytes"
                )));
            }
        }
        Ok(())
    }
}

/// One record's key facts, as stored in the index table.
#[derive(Clone, Debug)]
pub struct FactRow {
    /// The searchable fields.
    pub columns: FactColumns,
    /// The [`Card`], canonical JSON text.
    pub card: Box<RawValue>,
    /// Every highlight of the record, canonical JSON text of a list of [`BlockHighlight`]
    /// ([`highlights_body`]).
    pub highlights: Box<RawValue>,
}

impl FactRow {
    /// The row of record `blake3`: `card` derived ([`Card::derive`]) and its columns read.
    pub fn new(blake3: Blake3, mut card: Card, highlights: Vec<BlockHighlight>) -> Result<Self> {
        card.derive();
        Ok(Self {
            columns: FactColumns::of(blake3, &card),
            card: card.to_body()?,
            highlights: highlights_body(highlights)?,
        })
    }

    /// Checks the row before a store writes it.
    pub fn validate(&self) -> Result<()> {
        self.columns.validate()?;
        if self.card.get().len() > CARD_MAX_BYTES {
            return Err(Error::Invalid(format!(
                "document card: more than {CARD_MAX_BYTES} bytes"
            )));
        }
        if self.highlights.get().len() > FACT_HIGHLIGHTS_MAX_BYTES {
            return Err(Error::Invalid(format!(
                "document highlights: more than {FACT_HIGHLIGHTS_MAX_BYTES} bytes"
            )));
        }
        Ok(())
    }

    /// The card, read.
    pub fn card(&self) -> Result<Card> {
        Card::from_body(&self.card)
    }

    /// The highlights, read.
    pub fn highlights(&self) -> Result<Vec<BlockHighlight>> {
        serde_json::from_str(self.highlights.get())
            .map_err(|e| Error::Invalid(format!("document highlights: {e}")))
    }
}

/// A document [`crate::FactStore::find_documents`] found: its columns, and the companies its
/// document's `party` edges name as supplier and customer now.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FoundDocument {
    /// The card's searchable fields.
    pub columns: FactColumns,
    /// The document's node, when its graph was written.
    pub document: Option<Uuid>,
    /// The supplier company (`party` edge marked `side: supplier`).
    pub supplier: Option<Uuid>,
    /// The customer company (`side: buyer`).
    pub customer: Option<Uuid>,
}

/// Kind families of [`FactFilter::kind`] (issue #199): a family name finds each kind listed
/// for it. The kinds are quail's `card.kind` values (`ref.instrument_kind`), plus `sla`,
/// `lease`, `credit_note` and `pro_forma` for other producers (quail gives a lease
/// `contract` and a pro forma `invoice`). Any other kind finds itself only.
pub const KIND_FAMILIES: &[(&str, &[&str])] = &[
    (
        "contract",
        &[
            "contract",
            "msa",
            "nda",
            "sla",
            "lease",
            "framework_agreement",
            "annex",
            "amendment",
            "amended_restated",
            "sow",
            "change_order",
            "side_letter",
        ],
    ),
    ("invoice", &["invoice", "credit_note", "pro_forma"]),
];

impl FactFilter {
    /// The kinds [`FactFilter::kind`] finds: its family, or itself alone (exact, or not a
    /// family name); empty without a kind filter.
    pub fn kinds(&self) -> Vec<String> {
        let Some(kind) = &self.kind else {
            return Vec::new();
        };
        if !self.kind_exact
            && let Some((_, kinds)) = KIND_FAMILIES.iter().find(|(f, _)| *f == kind)
        {
            return kinds.iter().map(|k| (*k).to_owned()).collect();
        }
        vec![kind.clone()]
    }
}

/// Filters of [`crate::FactStore::find_documents`]; every one set must hold. Date bounds are
/// inclusive, and a document without the date never matches one.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct FactFilter {
    /// `card.kind` (`contract`, `invoice`, ...). A family name ([`KIND_FAMILIES`]) finds
    /// every kind of its family unless [`FactFilter::kind_exact`] is set.
    pub kind: Option<String>,
    /// Match [`FactFilter::kind`] alone, not its family.
    pub kind_exact: bool,
    /// The supplier company.
    pub supplier: Option<Uuid>,
    /// The customer company.
    pub customer: Option<Uuid>,
    /// Text the supplier's name contains ([`fold`]ed on both sides).
    pub supplier_name: Option<String>,
    /// Text the customer's name contains.
    pub customer_name: Option<String>,
    /// A category code.
    pub category: Option<String>,
    /// An ISO 4217 code; an amount bound needs it.
    pub currency: Option<String>,
    /// At least this amount (units of 10^-[`crate::PAYMENT_SCALE`]).
    pub amount_min: Option<i64>,
    /// At most this amount.
    pub amount_max: Option<i64>,
    /// Ends on or before this day ([`Card::ends`]).
    pub end_before: Option<Date>,
    /// Ends on or after this day.
    pub end_after: Option<Date>,
    /// Notice due on or before this day ([`Card::notice_due`]).
    pub notice_due_before: Option<Date>,
    /// Notice due on or after this day.
    pub notice_due_after: Option<Date>,
    /// Renews by itself, or does not (stated either way).
    pub auto_renewal: Option<bool>,
    /// Has a penalty clause, or has none.
    pub has_penalty: Option<bool>,
    /// Has a guarantee, or has none.
    pub has_guarantee: Option<bool>,
    /// An ISO 3166 code (`RS`); a country finds its subdivisions too (`GB` finds `GB-EAW`).
    pub governing_law: Option<String>,
    /// Text the court's name contains ([`fold`]ed).
    pub court: Option<String>,
    /// Signed on or after this day.
    pub signed_from: Option<Date>,
    /// Signed on or before this day.
    pub signed_to: Option<Date>,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn day(y: i32, m: u8, d: u8) -> Date {
        Date::from_calendar_date(y, Month::try_from(m).unwrap(), d).unwrap()
    }

    #[test]
    fn kind_families() {
        let kinds = |kind: &str, kind_exact| {
            FactFilter {
                kind: Some(kind.into()),
                kind_exact,
                ..FactFilter::default()
            }
            .kinds()
        };
        let contract = kinds("contract", false);
        for k in [
            "contract",
            "msa",
            "nda",
            "lease",
            "framework_agreement",
            "annex",
        ] {
            assert!(contract.iter().any(|c| c == k), "{k}");
        }
        assert!(!contract.iter().any(|c| c == "invoice"));
        assert_eq!(
            kinds("invoice", false),
            ["invoice", "credit_note", "pro_forma"]
        );
        assert_eq!(kinds("contract", true), ["contract"]);
        assert_eq!(kinds("msa", false), ["msa"]);
        assert!(FactFilter::default().kinds().is_empty());
    }

    #[test]
    fn dates_are_derived() {
        let mut c = Card {
            signed: Some("2026-01-31".into()),
            term: Some(CardDuration {
                iso: Some("P1M".into()),
                months: Some(1),
                ..CardDuration::default()
            }),
            notice: Some(CardDuration {
                iso: Some("P30D".into()),
                days: Some(30),
                ..CardDuration::default()
            }),
            ..Card::default()
        };
        c.derive();
        assert_eq!(c.ends.as_deref(), Some("2026-02-28"));
        assert_eq!(c.notice_due.as_deref(), Some("2026-01-29"));
        // A stated end wins; an indefinite term has no end.
        c.end = Some("2026-12-31".into());
        c.derive();
        assert_eq!(c.ends.as_deref(), Some("2026-12-31"));
        assert_eq!(c.notice_due.as_deref(), Some("2026-12-01"));
        c.end = None;
        c.term.as_mut().unwrap().indefinite = Some(true);
        c.derive();
        assert_eq!((c.ends, c.notice_due), (None, None));
        assert_eq!(add_months(day(2024, 3, 31), -1), Some(day(2024, 2, 29)));
        assert_eq!(add_months(day(2026, 11, 15), 14), Some(day(2028, 1, 15)));
        assert_eq!(parse_day("2026-1-05"), None);
    }

    #[test]
    fn units_and_folds() {
        assert_eq!(parse_units("1200000.00"), Some(12_000_000_000));
        assert_eq!(parse_units("-0.5"), Some(-5000));
        assert_eq!(parse_units("12"), Some(120_000));
        assert_eq!(parse_units("1,200.00"), None);
        assert_eq!(parse_units(""), None);
        assert_eq!(fold("Privredni Sud u Nišu"), "privredni sud u nisu");
        assert_eq!(fold("ĐORĐE"), "djordje");
    }

    #[test]
    fn a_card_too_large_loses_highlights() {
        let h = CardHighlight {
            quote: "x".repeat(500),
            ..CardHighlight::default()
        };
        let mut c = Card::default();
        for i in 0..200 {
            c.evidence.insert(
                format!("f{i}"),
                CardEvidence {
                    conf: 0.5,
                    highlights: vec![h.clone(); 2],
                },
            );
        }
        let body = c.to_body().unwrap();
        assert!(body.get().len() <= CARD_MAX_BYTES);
        let back = Card::from_body(&body).unwrap();
        assert_eq!(back.evidence.len(), 200);
        // Unknown fields come back as read.
        let raw: Box<RawValue> =
            serde_json::from_str(r#"{"kind":"contract","new_field":{"a":1}}"#).unwrap();
        let c = Card::from_body(&raw).unwrap();
        assert_eq!(c.other["new_field"]["a"], 1);
        let body = c.to_body().unwrap();
        assert!(body.get().contains("new_field"));
    }

    #[test]
    fn highlights_are_ordered_and_unique() {
        let h = |key: &str, block, start| BlockHighlight {
            key: key.into(),
            page: 0,
            block,
            start,
            end: start + 3,
            conf: 0.5,
        };
        let body =
            highlights_body(vec![h("b", 2, 0), h("a", 1, 5), h("a", 1, 5), h("a", 1, 0)]).unwrap();
        let back: Vec<BlockHighlight> = serde_json::from_str(body.get()).unwrap();
        assert_eq!(
            back.iter()
                .map(|h| (h.block, h.start, h.key.as_str()))
                .collect::<Vec<_>>(),
            [(1, 0, "a"), (1, 5, "a"), (2, 0, "b")]
        );
        let many = (0..2000).map(|i| h("card.notice", i, 0)).collect();
        let body = highlights_body(many).unwrap();
        assert!(body.get().len() <= FACT_HIGHLIGHTS_MAX_BYTES);
    }
}
