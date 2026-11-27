//! Payments (issue #193): what each document pays for and how much, and what each supplier
//! is paid.
//!
//! Ingest reads quail's `payment.*` facts (`payment.extract`) from a record and stores one
//! [`PaymentRow`] per record in the `payments` index table, beside the record, for fast filters
//! and sums. The row is rebuilt from the record whenever the document is written to the graph,
//! and goes with its record (a purge, issue #168).
//!
//! **Supplier and buyer.** quail's party roles say which company a document names as the
//! supplier and which as the buyer. The graph writer marks the document's `party` edge to
//! the supplier company with [`SIDE_PROP`] = [`SIDE_SUPPLIER`], and the edge to the buyer
//! with [`SIDE_BUYER`], at most one edge each per document write. A payment's supplier is read
//! through that edge when it is read, so a merge, an undo or a split, which moves the edge,
//! moves the payment with it. No new edge kind: the record, the index table and the edge's
//! props hold everything.
//!
//! **Amounts** are exact decimals: [`PaymentRow::amount`] is in units of 10^-[`PAYMENT_SCALE`]
//! of its currency, and sums are taken per currency only, never across currencies. A
//! recurring fee (`period`: `monthly`, `quarterly`, `yearly`) is never multiplied; spend
//! totals keep it apart from one-off prices.

use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;
use time::Date;
use uuid::Uuid;

use crate::{Blake3, Error, Result};

/// Decimal places of [`PaymentRow::amount`]: 4, so every amount quail reads (at most 4
/// decimals in practice) is kept exactly.
pub const PAYMENT_SCALE: u8 = 4;

/// The edge prop the graph writer sets on a document's `party` edge to say which side of the
/// payment the company is on.
pub const SIDE_PROP: &str = "side";
/// [`SIDE_PROP`] of the supplier's edge.
pub const SIDE_SUPPLIER: &str = "supplier";
/// [`SIDE_PROP`] of the buyer's edge.
pub const SIDE_BUYER: &str = "buyer";

/// Most bytes of a row's [`PaymentRow::detail`]: line items, then highlights, are dropped from
/// the end until it fits.
pub const PAYMENT_DETAIL_MAX: usize = 64 << 10;
/// Most bytes of a subject, a name or a category label kept in a row.
pub const PAYMENT_TEXT_MAX: usize = 1024;
/// Most rows of one [`crate::PaymentStore::list_payments`] page.
pub const PAYMENTS_MAX_LIMIT: u32 = 1000;
/// The byte budget of a payments page: 3 MiB of row details, as records pages.
pub const PAYMENTS_PAGE_BYTES: usize = 3 << 20;
/// Most totals and most "provides" rows one [`crate::PaymentStore::supplier_spend`] returns.
pub const SPEND_MAX_ROWS: u32 = 5000;

/// The periods quail reads (`payment.period`).
pub const PERIODS: &[&str] = &["one_off", "monthly", "quarterly", "yearly"];

/// The page size [`crate::PaymentStore::list_payments`] uses for `limit`.
pub fn payments_limit(limit: u32) -> u32 {
    match limit {
        0 => 50,
        n => n.min(PAYMENTS_MAX_LIMIT),
    }
}

/// One record's payment, as stored in the index table.
#[derive(Clone, Debug)]
pub struct PaymentRow {
    /// The record.
    pub blake3: Blake3,
    /// What is paid for, as the document states it.
    pub subject: Option<String>,
    /// The category code (`it_software`, `legal`, ..., `other`).
    pub category: Option<String>,
    /// The category's label.
    pub category_label: Option<String>,
    /// The headline price in units of 10^-[`PAYMENT_SCALE`]: gross when stated, else the plain
    /// total, else net ([`PaymentRow::basis`]).
    pub amount: Option<i64>,
    /// What [`PaymentRow::amount`] is: `gross`, `net` or `unstated`.
    pub basis: Option<String>,
    /// ISO 4217 code of the price.
    pub currency: Option<String>,
    /// How often it is paid: one of [`PERIODS`].
    pub period: Option<String>,
    /// The document's own date (as search has it).
    pub date: Option<Date>,
    /// The supplier's name as the document writes it.
    pub supplier_name: Option<String>,
    /// The buyer's name as the document writes it.
    pub buyer_name: Option<String>,
    /// The confidence of the amount (of the subject when there is no amount), 0 to 1.
    pub conf: f32,
    /// The rest, as canonical JSON text of a [`PaymentDetail`].
    pub detail: Box<RawValue>,
}

impl PaymentRow {
    /// Checks the row before a store writes it.
    pub fn validate(&self) -> Result<()> {
        let texts = [
            ("subject", &self.subject),
            ("category", &self.category),
            ("category_label", &self.category_label),
            ("basis", &self.basis),
            ("currency", &self.currency),
            ("period", &self.period),
            ("supplier_name", &self.supplier_name),
            ("buyer_name", &self.buyer_name),
        ];
        for (name, t) in texts {
            if t.as_ref().is_some_and(|t| t.len() > PAYMENT_TEXT_MAX) {
                return Err(Error::Invalid(format!(
                    "payment {name}: more than {PAYMENT_TEXT_MAX} bytes"
                )));
            }
        }
        if self.detail.get().len() > PAYMENT_DETAIL_MAX {
            return Err(Error::Invalid(format!(
                "payment detail: more than {PAYMENT_DETAIL_MAX} bytes"
            )));
        }
        if !(0.0..=1.0).contains(&self.conf) {
            return Err(Error::Invalid("payment conf: not in 0..=1".to_owned()));
        }
        Ok(())
    }

    /// The row's detail, read.
    pub fn detail(&self) -> Result<PaymentDetail> {
        serde_json::from_str(self.detail.get())
            .map_err(|e| Error::Invalid(format!("payment detail: {e}")))
    }
}

/// What a row keeps besides its columns.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct PaymentDetail {
    /// Where the subject came from: `clause`, `after_heading`, `line_items` or `title`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub subject_source: Option<String>,
    /// The word of the subject that decided the category.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub category_keyword: Option<String>,
    /// The price without VAT, where stated (decimal text).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub net: Option<String>,
    /// The VAT, where stated (decimal text).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub vat: Option<String>,
    /// The price with VAT, where stated (decimal text).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub gross: Option<String>,
    /// How the facts were found: `rules` (quail's `payment.extract`).
    pub method: String,
    /// Line items, as the document lists them.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub lines: Vec<PaymentLine>,
    /// Where each fact sits in the document text.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub highlights: Vec<Highlight>,
}

impl PaymentDetail {
    /// Canonical JSON text, line items then highlights dropped from the end until it fits
    /// [`PAYMENT_DETAIL_MAX`].
    pub fn to_body(&self) -> Result<Box<RawValue>> {
        let mut d = self.clone();
        loop {
            let body = crate::canonical::to_canonical(&d)
                .map_err(|e| Error::Invalid(format!("payment detail: {e}")))?;
            if body.get().len() <= PAYMENT_DETAIL_MAX {
                return Ok(body);
            }
            if d.lines.pop().is_none() && d.highlights.pop().is_none() {
                return Err(Error::Invalid(
                    "payment detail: too large without lines or highlights".to_owned(),
                ));
            }
        }
    }
}

/// One line item. Every field is as written or as quail normalized it; absent when unknown.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct PaymentLine {
    /// What the line is for.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// How many.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub quantity: Option<String>,
    /// Of what unit.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub unit: Option<String>,
    /// The price of one (decimal text).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub unit_price: Option<String>,
    /// The line's total (decimal text).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub total: Option<String>,
    /// ISO 4217 code.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub currency: Option<String>,
    /// The VAT rate, as written.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub vat_rate: Option<String>,
}

/// Where a fact sits in the document text: quail's shared highlight shape
/// (`quail_core::highlight`). `block` is the `TextBlock.index` of `GetDocumentText` for the
/// same record; `start` and `end` are characters in that block's text.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct Highlight {
    /// The fact key it proves (`payment.amount`).
    pub key: String,
    /// The page index, from 0.
    pub page: u32,
    /// The block's index among the document's blocks, from 0.
    pub block: u32,
    /// First character of the passage in the block.
    pub start: u32,
    /// One past its last character.
    pub end: u32,
    /// The passage as written.
    pub quote: String,
    /// The fact's confidence, two decimals.
    pub conf: f64,
}

/// A stored payment as read back, with the companies its document's `party` edges name as
/// supplier and buyer now.
#[derive(Clone, Debug)]
pub struct Payment {
    /// The row.
    pub row: PaymentRow,
    /// The document's node, when its graph was written.
    pub document: Option<Uuid>,
    /// The supplier company.
    pub supplier: Option<Uuid>,
    /// The buyer company.
    pub buyer: Option<Uuid>,
}

/// What a page counts against [`PAYMENTS_PAGE_BYTES`].
pub fn payment_page_bytes(p: &Payment) -> usize {
    p.row.detail.get().len()
        + p.row.subject.as_ref().map_or(0, String::len)
        + p.row.supplier_name.as_ref().map_or(0, String::len)
        + p.row.buyer_name.as_ref().map_or(0, String::len)
        + 128
}

/// Filters of [`crate::PaymentStore::list_payments`]; every one set must hold.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PaymentFilter {
    /// The supplier company.
    pub supplier: Option<Uuid>,
    /// The buyer company.
    pub buyer: Option<Uuid>,
    /// A category code.
    pub category: Option<String>,
    /// A period (one of [`PERIODS`]).
    pub period: Option<String>,
    /// An ISO 4217 code.
    pub currency: Option<String>,
    /// Documents dated on or after this day (undated ones never match).
    pub from: Option<Date>,
    /// Documents dated on or before this day.
    pub to: Option<Date>,
}

/// How [`crate::PaymentStore::supplier_spend`] groups the totals.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum SpendGroup {
    /// By category code.
    #[default]
    Category,
    /// By the year of the document's date (`2026`).
    Year,
    /// By its month (`2026-03`).
    Month,
}

/// One total: a supplier's amounts of one currency and period, in one group.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SpendTotal {
    /// The supplier; `None` for documents without one.
    pub supplier: Option<Uuid>,
    /// The ISO 4217 code; empty when the documents do not say.
    pub currency: String,
    /// The period; empty when the documents do not say.
    pub period: String,
    /// The category code, the year or the month; empty when unknown (no category, no date).
    pub group: String,
    /// The sum in units of 10^-[`PAYMENT_SCALE`].
    pub total: i128,
    /// The documents summed.
    pub documents: u64,
}

/// What a supplier provides: one category and subject, and in how many documents.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Provided {
    /// The supplier; `None` for documents without one.
    pub supplier: Option<Uuid>,
    /// The category code; empty when unknown.
    pub category: String,
    /// The category's label.
    pub category_label: String,
    /// The subject; empty when unknown.
    pub subject: String,
    /// The documents.
    pub documents: u64,
}

/// What [`crate::PaymentStore::supplier_spend`] returns.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Spend {
    /// Ordered by supplier (none last), currency, period, group.
    pub totals: Vec<SpendTotal>,
    /// Ordered by supplier (none last), then most documents first, category, subject.
    pub provides: Vec<Provided>,
    /// More than [`SPEND_MAX_ROWS`] totals or provides rows: the rest is left out.
    pub truncated: bool,
}

/// An exact decimal (`units × 10^-scale`) in units of 10^-[`PAYMENT_SCALE`]: rounded half
/// away from zero past 4 decimals; `None` when it does not fit.
pub fn scaled_units(units: i64, scale: u8) -> Option<i64> {
    let target = u32::from(PAYMENT_SCALE);
    let scale = u32::from(scale);
    if scale <= target {
        units.checked_mul(10i64.checked_pow(target - scale)?)
    } else {
        let div = 10i128.checked_pow(scale - target)?;
        let u = i128::from(units);
        let q = u / div;
        let r = u % div;
        let q = if r.abs() * 2 >= div {
            q + u.signum()
        } else {
            q
        };
        i64::try_from(q).ok()
    }
}

/// Units of 10^-[`PAYMENT_SCALE`] as decimal text with at least 2 decimals: `1234.50`,
/// `-0.0125`.
pub fn format_units(units: i128) -> String {
    let scale = 10i128.pow(u32::from(PAYMENT_SCALE));
    let sign = if units < 0 { "-" } else { "" };
    let abs = units.unsigned_abs();
    let whole = abs / scale.unsigned_abs();
    let frac = abs % scale.unsigned_abs();
    let mut f = format!("{frac:0width$}", width = usize::from(PAYMENT_SCALE));
    while f.len() > 2 && f.ends_with('0') {
        f.pop();
    }
    format!("{sign}{whole}.{f}")
}

/// An exact decimal as text (`units × 10^-scale`), as [`format_units`] writes it.
pub fn format_decimal(units: i64, scale: u8) -> Option<String> {
    scaled_units(units, scale).map(|u| format_units(i128::from(u)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decimals_are_exact() {
        assert_eq!(scaled_units(420_050, 2), Some(42_005_000));
        assert_eq!(scaled_units(5, 0), Some(50_000));
        assert_eq!(scaled_units(123_456, 5), Some(12_346));
        assert_eq!(scaled_units(-123_455, 5), Some(-12_346));
        assert_eq!(scaled_units(i64::MAX, 0), None);
        assert_eq!(format_units(42_005_000), "4200.50");
        assert_eq!(format_units(50_000), "5.00");
        assert_eq!(format_units(-125), "-0.0125");
        assert_eq!(format_units(0), "0.00");
        assert_eq!(format_decimal(12_000_000, 2).as_deref(), Some("120000.00"));
    }

    #[test]
    fn a_detail_too_large_loses_lines_first() {
        let line = PaymentLine {
            description: Some("x".repeat(1000)),
            ..PaymentLine::default()
        };
        let d = PaymentDetail {
            method: "rules".into(),
            lines: vec![line; 200],
            highlights: vec![Highlight::default(); 3],
            ..PaymentDetail::default()
        };
        let body = d.to_body().unwrap();
        assert!(body.get().len() <= PAYMENT_DETAIL_MAX);
        let back: PaymentDetail = serde_json::from_str(body.get()).unwrap();
        assert!(back.lines.len() < 200 && !back.lines.is_empty());
        assert_eq!(back.highlights.len(), 3);
    }
}
