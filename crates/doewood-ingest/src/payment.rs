//! Payments (issue #193): quail's `payment.*` facts read from a record, the supplier and buyer
//! among its parties, and the index row ingest stores beside the record
//! (`doewood_model::payment`).

use doewood_model::{
    Blake3, Highlight, PAYMENT_TEXT_MAX, PaymentDetail, PaymentLine, PaymentRow, format_decimal,
    scaled_units,
};
use quail_core::{Money, Value};
use time::Date;

use crate::facts::Party;

/// The fact keys of quail's `payment.extract`.
pub const PAYMENT_KEYS: &[&str] = &[
    "payment.subject",
    "payment.category",
    "payment.amount",
    "payment.amount_basis",
    "payment.currency",
    "payment.net",
    "payment.vat",
    "payment.gross",
    "payment.period",
    "payment.lines",
    "payment.highlight",
];

/// Most highlights kept per document.
pub const MAX_HIGHLIGHTS: usize = 64;

/// Roles on the buying side (quail's `party.candidates` roles).
const BUYER_ROLES: &[&str] = &["customer", "bill_to", "ship_to"];

/// What a record says is paid for and how much.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct PaymentFacts {
    /// `payment.subject`: the text.
    pub subject: Option<String>,
    /// `payment.subject`: where it came from.
    pub subject_source: Option<String>,
    /// `payment.category`: code, label, keyword.
    pub category: Option<(String, String, Option<String>)>,
    /// `payment.amount`.
    pub amount: Option<Money>,
    /// Its confidence.
    pub amount_conf: f32,
    /// The subject's confidence.
    pub subject_conf: f32,
    /// `payment.amount_basis`.
    pub basis: Option<String>,
    /// `payment.currency`.
    pub currency: Option<String>,
    /// `payment.net`.
    pub net: Option<Money>,
    /// `payment.vat`.
    pub vat: Option<Money>,
    /// `payment.gross`.
    pub gross: Option<Money>,
    /// `payment.period`.
    pub period: Option<String>,
    /// `payment.lines`.
    pub lines: Vec<PaymentLine>,
    /// `payment.highlight`, in record order.
    pub highlights: Vec<Highlight>,
}

/// Text cut to [`PAYMENT_TEXT_MAX`] bytes, NULs dropped (Postgres text cannot hold them).
fn clean(s: &str) -> String {
    let mut s: String = s.chars().filter(|c| *c != '\0').collect();
    if s.len() > PAYMENT_TEXT_MAX {
        let mut end = PAYMENT_TEXT_MAX;
        while !s.is_char_boundary(end) {
            end -= 1;
        }
        s.truncate(end);
    }
    s
}

fn text_field(v: &Value, name: &str) -> Option<String> {
    v.field(name)
        .and_then(Value::as_text)
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(clean)
}

fn int_field(v: &Value, name: &str) -> Option<u32> {
    match v.field(name)? {
        Value::Int(i) => u32::try_from(*i).ok(),
        Value::U64(i) => u32::try_from(*i).ok(),
        _ => None,
    }
}

fn currency_text(m: &Money) -> Option<String> {
    m.currency
        .and_then(|c| std::str::from_utf8(&c).ok().map(str::to_owned))
}

fn money_text(m: &Money) -> Option<String> {
    format_decimal(m.units, m.scale)
}

/// Any value as text: a decimal for money, digits for numbers.
fn value_text(v: &Value) -> Option<String> {
    match v {
        Value::Text(t) => Some(clean(t.trim())).filter(|t| !t.is_empty()),
        Value::Money(m) => money_text(m),
        Value::Int(i) => Some(i.to_string()),
        Value::U64(i) => Some(i.to_string()),
        Value::Float(f) if f.is_finite() => Some(f.to_string()),
        _ => None,
    }
}

fn line(v: &Value) -> PaymentLine {
    let f = |name| v.field(name).and_then(value_text);
    let currency = f("currency").or_else(|| match v.field("total").or(v.field("unit_price")) {
        Some(Value::Money(m)) => currency_text(m),
        _ => None,
    });
    PaymentLine {
        description: f("description"),
        quantity: f("quantity"),
        unit: f("unit"),
        unit_price: f("unit_price"),
        total: f("total"),
        currency,
        vat_rate: f("vat_rate"),
    }
}

impl PaymentFacts {
    /// The `payment.*` facts among `facts` (`(key, value, conf)`); `None` when there are
    /// none, or neither a subject nor an amount. The most confident fact of a key wins.
    pub fn read<'a>(facts: impl IntoIterator<Item = (&'a str, &'a Value, f32)>) -> Option<Self> {
        let mut p = Self::default();
        let mut best: [f32; 11] = [-1.0; 11];
        for (key, value, conf) in facts {
            let Some(i) = PAYMENT_KEYS.iter().position(|k| *k == key) else {
                continue;
            };
            if key == "payment.highlight" {
                if p.highlights.len() < MAX_HIGHLIGHTS
                    && let (Some(k), Some(page), Some(block), Some(start), Some(end)) = (
                        text_field(value, "key"),
                        int_field(value, "page"),
                        int_field(value, "block"),
                        int_field(value, "start"),
                        int_field(value, "end"),
                    )
                {
                    p.highlights.push(Highlight {
                        key: k,
                        page,
                        block,
                        start,
                        end,
                        quote: text_field(value, "quote").unwrap_or_default(),
                        conf: (f64::from(conf.clamp(0.0, 1.0)) * 100.0).round() / 100.0,
                    });
                }
                continue;
            }
            if conf <= best[i] {
                continue;
            }
            let money = |v: &Value| match v {
                Value::Money(m) => Some(*m),
                _ => None,
            };
            let taken = match key {
                "payment.subject" => {
                    p.subject = text_field(value, "text");
                    p.subject_source = text_field(value, "source");
                    p.subject_conf = conf;
                    p.subject.is_some()
                }
                "payment.category" => {
                    p.category = text_field(value, "code").map(|code| {
                        (
                            code,
                            text_field(value, "label").unwrap_or_default(),
                            text_field(value, "keyword"),
                        )
                    });
                    p.category.is_some()
                }
                "payment.amount" => {
                    p.amount = money(value);
                    p.amount_conf = conf;
                    p.amount.is_some()
                }
                "payment.amount_basis" => {
                    p.basis = value_text(value);
                    p.basis.is_some()
                }
                "payment.currency" => {
                    p.currency = value_text(value);
                    p.currency.is_some()
                }
                "payment.net" => {
                    p.net = money(value);
                    p.net.is_some()
                }
                "payment.vat" => {
                    p.vat = money(value);
                    p.vat.is_some()
                }
                "payment.gross" => {
                    p.gross = money(value);
                    p.gross.is_some()
                }
                "payment.period" => {
                    p.period = value_text(value);
                    p.period.is_some()
                }
                "payment.lines" => {
                    p.lines = match value {
                        Value::List(l) => l.iter().map(line).collect(),
                        _ => Vec::new(),
                    };
                    !p.lines.is_empty()
                }
                _ => false,
            };
            if taken {
                best[i] = conf;
            }
        }
        if p.currency.is_none() {
            p.currency = p.amount.as_ref().and_then(currency_text);
        }
        (p.subject.is_some() || p.amount.is_some()).then_some(p)
    }

    /// The index row of record `blake3`, dated `date`, with the supplier's and buyer's names.
    pub fn row(
        &self,
        blake3: Blake3,
        date: Option<Date>,
        supplier: Option<&Party>,
        buyer: Option<&Party>,
    ) -> doewood_model::Result<PaymentRow> {
        let detail = PaymentDetail {
            subject_source: self.subject_source.clone(),
            category_keyword: self.category.as_ref().and_then(|c| c.2.clone()),
            net: self.net.as_ref().and_then(money_text),
            vat: self.vat.as_ref().and_then(money_text),
            gross: self.gross.as_ref().and_then(money_text),
            method: "rules".to_owned(),
            lines: self.lines.clone(),
            highlights: self.highlights.clone(),
        };
        let conf = if self.amount.is_some() {
            self.amount_conf
        } else {
            self.subject_conf
        };
        Ok(PaymentRow {
            blake3,
            subject: self.subject.clone(),
            category: self.category.as_ref().map(|c| c.0.clone()),
            category_label: self
                .category
                .as_ref()
                .map(|c| c.1.clone())
                .filter(|l| !l.is_empty()),
            amount: self
                .amount
                .as_ref()
                .and_then(|m| scaled_units(m.units, m.scale)),
            basis: self.basis.clone(),
            currency: self.currency.clone(),
            period: self.period.clone(),
            date,
            supplier_name: supplier.map(|p| clean(&p.surface)),
            buyer_name: buyer.map(|p| clean(&p.surface)),
            conf: conf.clamp(0.0, 1.0),
            detail: detail.to_body()?,
        })
    }
}

/// The supplier and the buyer among a document's parties, by index: the most likely party
/// with the role `supplier` (else the document's `issuer`, when not also on the buying side),
/// and the most likely other party on the buying side (`customer`, `bill_to`, `ship_to`).
pub fn sides(parties: &[Party]) -> (Option<usize>, Option<usize>) {
    let has = |p: &Party, roles: &[&str]| p.roles.iter().any(|r| roles.contains(&r.as_str()));
    let best = |keep: &dyn Fn(usize, &Party) -> bool| {
        parties
            .iter()
            .enumerate()
            .filter(|(i, p)| keep(*i, p))
            .fold(None::<(usize, f32)>, |acc, (i, p)| match acc {
                Some((_, s)) if s >= p.score => acc,
                _ => Some((i, p.score)),
            })
            .map(|(i, _)| i)
    };
    let supplier = best(&|_, p| has(p, &["supplier"]))
        .or_else(|| best(&|_, p| has(p, &["issuer"]) && !has(p, BUYER_ROLES)));
    let buyer = best(&|i, p| Some(i) != supplier && has(p, BUYER_ROLES));
    (supplier, buyer)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn party(key: &str, score: f32, roles: &[&str]) -> Party {
        Party {
            quail_key: key.into(),
            surface: format!("{key} d.o.o."),
            score,
            roles: roles.iter().map(|r| (*r).to_owned()).collect(),
            legal_form: None,
            jurisdiction: None,
            ids: Vec::new(),
            mentions: Vec::new(),
        }
    }

    #[test]
    fn supplier_and_buyer_come_from_the_roles() {
        let ps = [
            party("a", 0.5, &["mentioned"]),
            party("b", 0.7, &["customer"]),
            party("c", 0.6, &["supplier", "signatory"]),
            party("d", 0.9, &["supplier"]),
        ];
        assert_eq!(sides(&ps), (Some(3), Some(1)));
        // An invoice: the letterhead issues it, a bill-to block names the buyer.
        let ps = [party("x", 0.8, &["issuer"]), party("y", 0.6, &["bill_to"])];
        assert_eq!(sides(&ps), (Some(0), Some(1)));
        // An issuer on the buying side is not the supplier.
        let ps = [party("x", 0.8, &["issuer", "customer"])];
        assert_eq!(sides(&ps), (None, Some(0)));
        assert_eq!(sides(&[]), (None, None));
    }

    #[test]
    fn facts_become_a_row() {
        let money = |units, scale| {
            Value::Money(Money {
                units,
                scale,
                currency: Some(*b"RSD"),
            })
        };
        let subject = Value::record([
            ("text", Value::text("održavanje softvera")),
            ("source", Value::text("clause")),
        ]);
        let category = Value::record([
            ("code", Value::text("it_software")),
            ("label", Value::text("IT and software")),
            ("keyword", Value::text("softver")),
        ]);
        let amount = money(12_000_000, 2);
        let net = money(10_000_000, 2);
        let period = Value::text("monthly");
        let hl = Value::record([
            ("key", Value::text("payment.amount")),
            ("page", Value::Int(0)),
            ("block", Value::Int(4)),
            ("start", Value::Int(10)),
            ("end", Value::Int(20)),
            ("quote", Value::text("120.000,00")),
        ]);
        let weaker = money(1, 0);
        let facts = [
            ("payment.subject", &subject, 0.8),
            ("payment.category", &category, 0.7),
            ("payment.amount", &weaker, 0.2),
            ("payment.amount", &amount, 0.85),
            ("payment.net", &net, 0.8),
            ("payment.period", &period, 0.8),
            ("payment.highlight", &hl, 0.85),
            ("party.candidates", &period, 0.9),
        ];
        let p = PaymentFacts::read(facts).unwrap();
        assert_eq!(p.currency.as_deref(), Some("RSD"));
        let row = p
            .row(
                Blake3([1; 32]),
                None,
                Some(&party("s", 0.9, &["supplier"])),
                None,
            )
            .unwrap();
        assert_eq!(row.amount, Some(1_200_000_000));
        assert_eq!(row.category.as_deref(), Some("it_software"));
        assert_eq!(row.supplier_name.as_deref(), Some("s d.o.o."));
        assert!((row.conf - 0.85).abs() < 1e-6);
        let d = row.detail().unwrap();
        assert_eq!(d.net.as_deref(), Some("100000.00"));
        assert_eq!(d.highlights.len(), 1);
        assert_eq!(d.highlights[0].block, 4);
        assert_eq!(d.category_keyword.as_deref(), Some("softver"));
        assert!(PaymentFacts::read([("payment.period", &period, 0.9)]).is_none());
    }
}
