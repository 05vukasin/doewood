//! Key facts (issue #194): quail's key-facts card (`card.*`) and every `<namespace>.highlight`
//! read from a record, and the index row ingest stores beside the record
//! (`doewood_model::keyfacts`).

use std::collections::BTreeMap;

use doewood_model::keyfacts::{CARD_FIELD_HIGHLIGHTS_MAX, CARD_QUOTE_MAX};
use doewood_model::{
    Blake3, BlockHighlight, Card, CardEvidence, CardHighlight, FactRow, format_decimal,
};
use quail_core::{Money, Value};
use serde_json::{Map, Value as Json};

/// Most bytes of the search text (the card's text and its fields' passages).
pub const SEARCH_FACTS_MAX: usize = 16 << 10;

/// Card keys that are not fields: the text form and the highlights.
const NOT_FIELDS: &[&str] = &["text", "highlight"];
/// Card fields whose passages search does not take: their values are in the card's text.
const NOT_QUOTED: &[&str] = &["kind", "number", "supplier", "customer", "accounts"];
/// Names the model's [`Card`] derives or keeps apart; never read from a fact.
const RESERVED: &[&str] = &["ends", "notice_due", "evidence", "other"];

/// A record's key facts.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct KeyFacts {
    /// The card, each field with its confidence and highlights.
    pub card: Card,
    /// Every highlight of the record, of every namespace.
    pub highlights: Vec<BlockHighlight>,
    /// The text search weights above the body: the card's text form and the passages its
    /// fields were read from (`card.highlight`), at most [`SEARCH_FACTS_MAX`] bytes.
    pub search_text: String,
}

/// `s` cut to at most `max` bytes at a character.
fn cut(s: &str, max: usize) -> &str {
    if s.len() <= max {
        return s;
    }
    let mut end = max;
    while !s.is_char_boundary(end) {
        end -= 1;
    }
    &s[..end]
}

fn int_field(v: &Value, name: &str) -> Option<u32> {
    match v.field(name)? {
        Value::Int(i) => u32::try_from(*i).ok(),
        Value::U64(i) => u32::try_from(*i).ok(),
        _ => None,
    }
}

fn currency(m: &Money) -> Option<String> {
    m.currency
        .and_then(|c| std::str::from_utf8(&c).ok().map(str::to_owned))
}

/// A quail value as the card's JSON: dates `YYYY-MM-DD`, money as decimal text (inside a
/// record its code goes in `currency`), records as objects. `None` for what has no JSON form
/// (bytes, a float that is not finite).
fn json(v: &Value) -> Option<Json> {
    Some(match v {
        Value::Bool(b) => Json::Bool(*b),
        Value::Int(i) => Json::from(*i),
        Value::U64(u) => Json::from(*u),
        Value::Float(f) => Json::from(serde_json::Number::from_f64(*f)?),
        Value::Text(t) => Json::String(t.trim().chars().filter(|c| *c != '\0').collect()),
        Value::Date(d) => Json::String(format!("{:04}-{:02}-{:02}", d.y, d.m, d.d)),
        Value::Money(m) => Json::String(format_decimal(m.units, m.scale)?),
        Value::List(l) => Json::Array(l.iter().filter_map(json).collect()),
        Value::Record(fields) => {
            let mut out = Map::new();
            for (k, x) in fields {
                if let Some(j) = json(x) {
                    out.insert(k.to_string(), j);
                }
                if let Value::Money(m) = x
                    && let Some(c) = currency(m)
                    && !fields.iter().any(|(n, _)| &**n == "currency")
                {
                    out.insert("currency".to_owned(), Json::String(c));
                }
            }
            Json::Object(out)
        }
        Value::Bytes(_) => return None,
    })
}

fn round2(conf: f32) -> f64 {
    (f64::from(conf.clamp(0.0, 1.0)) * 100.0).round() / 100.0
}

/// One `<namespace>.highlight` fact: `(key, page, block, start, end, quote)`.
fn highlight(v: &Value) -> Option<(String, u32, u32, u32, u32, String)> {
    let key = v.field("key")?.as_text()?.trim();
    let (page, block, start, end) = (
        int_field(v, "page")?,
        int_field(v, "block")?,
        int_field(v, "start")?,
        int_field(v, "end")?,
    );
    if key.is_empty() || end < start {
        return None;
    }
    let quote = v
        .field("quote")
        .and_then(Value::as_text)
        .unwrap_or_default();
    Some((key.to_owned(), page, block, start, end, quote.to_owned()))
}

impl KeyFacts {
    /// The card and highlight facts among `facts` (`(key, value, conf)`); `None` when there
    /// are neither. The most confident fact of a card key wins.
    pub fn read<'a>(facts: impl IntoIterator<Item = (&'a str, &'a Value, f32)>) -> Option<Self> {
        let mut fields: BTreeMap<String, (f32, Json)> = BTreeMap::new();
        let mut card_text: Option<String> = None;
        let mut highlights: Vec<BlockHighlight> = Vec::new();
        let mut field_highlights: BTreeMap<String, Vec<CardHighlight>> = BTreeMap::new();
        let mut quotes: Vec<String> = Vec::new();
        for (key, value, conf) in facts {
            if let Some(ns) = key.strip_suffix(".highlight")
                && !ns.contains('.')
            {
                let Some((k, page, block, start, end, quote)) = highlight(value) else {
                    continue;
                };
                if let Some(name) = k.strip_prefix("card.") {
                    let list = field_highlights.entry(name.to_owned()).or_default();
                    let h = CardHighlight {
                        page,
                        block,
                        start,
                        end,
                        quote: cut(&quote, CARD_QUOTE_MAX).to_owned(),
                    };
                    if list.len() < CARD_FIELD_HIGHLIGHTS_MAX && !list.contains(&h) {
                        list.push(h);
                    }
                }
                // Search weights the passages the card's fields were read from: a fact that did
                // not make the card (a cue in a letter that only talks about a clause) keeps
                // the body's weight. The parties, numbers and accounts are in the card's text
                // already, and their passages are long (BM25 ranks a long column lower).
                if k.strip_prefix("card.")
                    .is_some_and(|f| !NOT_QUOTED.contains(&f))
                    && !quote.trim().is_empty()
                    && !quotes.contains(&quote)
                {
                    quotes.push(quote);
                }
                highlights.push(BlockHighlight {
                    key: k,
                    page,
                    block,
                    start,
                    end,
                    conf: round2(conf),
                });
                continue;
            }
            let Some(name) = key.strip_prefix("card.") else {
                continue;
            };
            if name == "text" {
                card_text = value.as_text().map(str::to_owned);
                continue;
            }
            if NOT_FIELDS.contains(&name) || RESERVED.contains(&name) || name.contains('.') {
                continue;
            }
            let Some(j) = json(value) else { continue };
            if fields.get(name).is_none_or(|(c, _)| conf > *c) {
                fields.insert(name.to_owned(), (conf, j));
            }
        }
        if fields.is_empty() && highlights.is_empty() {
            return None;
        }
        // Field by field, so one value of an unexpected shape loses only its own field.
        let mut map = Map::new();
        let mut evidence = BTreeMap::new();
        for (name, (conf, j)) in fields {
            let one = Json::Object(Map::from_iter([(name.clone(), j.clone())]));
            if serde_json::from_value::<Card>(one).is_err() {
                continue;
            }
            map.insert(name.clone(), j);
            evidence.insert(
                name.clone(),
                CardEvidence {
                    conf: round2(conf),
                    highlights: field_highlights.remove(&name).unwrap_or_default(),
                },
            );
        }
        let mut card: Card = serde_json::from_value(Json::Object(map)).unwrap_or_default();
        card.evidence = evidence;
        let mut search_text = card_text
            .map(|t| t.trim_end().to_owned())
            .unwrap_or_default();
        for q in &quotes {
            if search_text.len() + q.len() + 1 > SEARCH_FACTS_MAX {
                break;
            }
            if !search_text.is_empty() {
                search_text.push('\n');
            }
            search_text.push_str(q);
        }
        let search_text = cut(&search_text, SEARCH_FACTS_MAX).to_owned();
        Some(Self {
            card,
            highlights,
            search_text,
        })
    }

    /// The index row of record `blake3`.
    pub fn row(&self, blake3: Blake3) -> doewood_model::Result<FactRow> {
        FactRow::new(blake3, self.card.clone(), self.highlights.clone())
    }
}

#[cfg(test)]
mod tests {
    use quail_core::Date;

    use super::*;

    fn hl(key: &str, block: i64, start: i64, end: i64, quote: &str) -> Value {
        Value::record([
            ("key", Value::text(key)),
            ("page", Value::Int(0)),
            ("block", Value::Int(block)),
            ("start", Value::Int(start)),
            ("end", Value::Int(end)),
            ("quote", Value::text(quote)),
        ])
    }

    #[test]
    fn card_facts_become_a_card() {
        let supplier = Value::record([
            ("name", Value::text("Alfa Soft d.o.o.")),
            ("pib", Value::text("100000024")),
        ]);
        let notice = Value::record([("iso", Value::text("P30D")), ("days", Value::Int(30))]);
        let penalty = Value::record([
            ("percent", Value::Float(0.5)),
            ("per", Value::text("day")),
            (
                "amount",
                Value::Money(Money {
                    units: 500_000,
                    scale: 2,
                    currency: Some(*b"EUR"),
                }),
            ),
        ]);
        let end = Value::Date(Date {
            y: 2026,
            m: 12,
            d: 31,
        });
        let amount = Value::Money(Money {
            units: 120_000_000,
            scale: 2,
            currency: Some(*b"RSD"),
        });
        let law = Value::text("RS");
        let renew = Value::Bool(true);
        let guarantees = Value::List(vec![Value::text("bank_guarantee")]);
        let wrong = Value::text("not a number");
        let text = Value::text("notice: iso=P30D; days=30\n");
        let h1 = hl("card.notice", 7, 10, 32, "otkazni rok od 30 dana");
        let h2 = hl("contract.notice", 7, 10, 32, "otkazni rok od 30 dana");
        let h3 = hl("payment.amount", 4, 0, 10, "120.000,00");
        let weaker = Value::text("ZZ");
        let facts = [
            ("card.supplier", &supplier, 0.9),
            ("card.notice", &notice, 0.8),
            ("card.penalty", &penalty, 0.7),
            ("card.end", &end, 0.8),
            ("card.amount", &amount, 0.85),
            ("card.governing_law", &weaker, 0.2),
            ("card.governing_law", &law, 0.9),
            ("card.auto_renewal", &renew, 0.8),
            ("card.guarantees", &guarantees, 0.7),
            ("card.payment_days", &wrong, 0.7),
            ("card.text", &text, 1.0),
            ("card.highlight", &h1, 0.8),
            ("contract.highlight", &h2, 0.8),
            ("payment.highlight", &h3, 0.85),
            ("party.candidates", &law, 0.9),
        ];
        let k = KeyFacts::read(facts).unwrap();
        let c = &k.card;
        assert_eq!(
            c.supplier.as_ref().unwrap().name.as_deref(),
            Some("Alfa Soft d.o.o.")
        );
        assert_eq!(c.notice.as_ref().unwrap().days, Some(30));
        let p = c.penalty.as_ref().unwrap();
        assert_eq!(
            (p.amount.as_deref(), p.currency.as_deref()),
            (Some("5000.00"), Some("EUR"))
        );
        assert_eq!(c.amount.as_deref(), Some("1200000.00"));
        assert_eq!(c.governing_law.as_deref(), Some("RS"));
        // A value of the wrong shape loses its own field only.
        assert_eq!(c.payment_days, None);
        assert!(!c.evidence.contains_key("payment_days"));
        assert_eq!(c.evidence["notice"].highlights.len(), 1);
        assert_eq!(c.evidence["notice"].highlights[0].block, 7);
        assert!((c.evidence["notice"].conf - 0.8).abs() < 1e-9);
        assert_eq!(k.highlights.len(), 3);
        // The card's quote is in the search text once; the others are not.
        assert_eq!(
            k.search_text,
            "notice: iso=P30D; days=30\notkazni rok od 30 dana"
        );
        let row = k.row(Blake3([2; 32])).unwrap();
        let cols = &row.columns;
        assert_eq!(cols.amount, Some(12_000_000_000));
        assert_eq!(cols.governing_law.as_deref(), Some("RS"));
        assert!(cols.has_penalty && cols.has_guarantee);
        assert_eq!(cols.auto_renewal, Some(true));
        assert_eq!(cols.notice_due, doewood_model::parse_day("2026-12-01"));
        assert_eq!(row.highlights().unwrap().len(), 3);
        assert!(KeyFacts::read([("party.candidates", &law, 0.9)]).is_none());
    }
}
