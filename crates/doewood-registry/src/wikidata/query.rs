//! The SPARQL query, built only from validated values: a [`QId`] (a number), a validated LEI
//! (20 ASCII letters and digits) and a validated language tag. No caller text ever reaches the
//! query; the whole query is then percent-encoded into the URL.

use std::fmt::{self, Write as _};
use std::str::FromStr;

/// The entity IRI prefix of Wikidata items, as the query service writes it.
pub(crate) const ENTITY: &str = "http://www.wikidata.org/entity/";

/// A Wikidata item id (`Q42`). It holds a number, so its text is always `Q` and digits.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct QId(u64);

impl QId {
    /// The largest number accepted (12 digits; Wikidata's ids are far below).
    const MAX: u64 = 999_999_999_999;

    /// `Q{n}`, for `n` in 1..=999 999 999 999.
    pub fn new(n: u64) -> Option<Self> {
        (1..=Self::MAX).contains(&n).then_some(Self(n))
    }

    /// The number.
    pub fn number(self) -> u64 {
        self.0
    }

    /// `Q42` (any case, surrounding spaces ignored); nothing else, so a hostile string is
    /// refused here.
    pub fn parse(s: &str) -> Option<Self> {
        let s = s.trim();
        let digits = s.strip_prefix(['Q', 'q'])?;
        if digits.is_empty()
            || digits.len() > 12
            || digits.starts_with('0')
            || !digits.bytes().all(|b| b.is_ascii_digit())
        {
            return None;
        }
        Self::new(digits.parse().ok()?)
    }

    /// The id of an entity IRI (`http://www.wikidata.org/entity/Q42`, or `https`); `None` for
    /// anything else (a property, a lexeme, a blank node, an unknown value).
    pub(crate) fn from_iri(iri: &str) -> Option<Self> {
        let rest = iri
            .strip_prefix(ENTITY)
            .or_else(|| iri.strip_prefix("https://www.wikidata.org/entity/"))?;
        if !rest.starts_with('Q') {
            return None;
        }
        Self::parse(rest)
    }
}

impl fmt::Display for QId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Q{}", self.0)
    }
}

impl FromStr for QId {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, String> {
        Self::parse(s).ok_or_else(|| "not a Wikidata item id (Q followed by digits)".to_owned())
    }
}

/// Which item the query is about.
#[derive(Clone, Copy, Debug)]
pub(crate) enum Subject<'a> {
    /// The item with this id.
    Item(QId),
    /// The items whose LEI (P1278) is this one; already validated.
    Lei(&'a str),
}

/// The properties read, with their name in the answer.
pub(crate) const PROPS: [&str; 7] = ["P749", "P127", "P1366", "P156", "P856", "P154", "P1278"];

/// The most rows asked for. An item has far fewer statements; a full answer is logged.
pub(crate) const ROW_LIMIT: usize = 2000;

/// Whether `lang` is a safe language tag: lowercase ASCII letters, then optional `-` subtags
/// of letters and digits, at most 20 characters (`en`, `sr-el`, `zh-hant`).
pub(crate) fn valid_language(lang: &str) -> bool {
    let mut parts = lang.split('-');
    let first = parts.next().unwrap_or_default();
    lang.len() <= 20
        && (2..=3).contains(&first.len())
        && first.bytes().all(|b| b.is_ascii_lowercase())
        && parts.all(|p| {
            (1..=8).contains(&p.len())
                && p.bytes()
                    .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit())
        })
}

/// The query: the item, its label in `lang` (else the `mul` label), and every non-deprecated
/// statement of [`PROPS`] with its rank, its start (P580) and end (P582) times with their
/// precision, the value's label, and whether the value is a human (P31 Q5).
///
/// # Panics
///
/// When `lang` or the LEI are not the validated forms ([`valid_language`], 20 ASCII letters
/// and digits): the callers check both first, so this never happens; it guards the query.
pub(crate) fn sparql(subject: Subject<'_>, lang: &str) -> String {
    assert!(valid_language(lang), "language tag not validated");
    let subject = match subject {
        Subject::Item(q) => {
            format!("VALUES ?item {{ wd:{q} }}\n  ?item wikibase:statements ?count .")
        }
        Subject::Lei(lei) => {
            assert!(
                lei.len() == 20 && lei.bytes().all(|b| b.is_ascii_alphanumeric()),
                "LEI not validated"
            );
            format!("?item wdt:P1278 \"{lei}\" .")
        }
    };
    let mut values = String::new();
    for p in PROPS {
        let _ = write!(values, " (\"{p}\" p:{p} ps:{p})");
    }
    format!(
        r#"PREFIX wd: <http://www.wikidata.org/entity/>
PREFIX wdt: <http://www.wikidata.org/prop/direct/>
PREFIX p: <http://www.wikidata.org/prop/>
PREFIX ps: <http://www.wikidata.org/prop/statement/>
PREFIX pqv: <http://www.wikidata.org/prop/qualifier/value/>
PREFIX wikibase: <http://wikiba.se/ontology#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
SELECT ?item ?itemLabel ?prop ?st ?value ?valueLabel ?human ?rank ?start ?startPrecision ?end ?endPrecision WHERE {{
  {subject}
  OPTIONAL {{ ?item rdfs:label ?l1 . FILTER(LANG(?l1) = "{lang}") }}
  OPTIONAL {{ ?item rdfs:label ?l2 . FILTER(LANG(?l2) = "mul") }}
  BIND(COALESCE(?l1, ?l2) AS ?itemLabel)
  OPTIONAL {{
    VALUES (?prop ?p ?ps) {{{values} }}
    ?item ?p ?st .
    ?st ?ps ?value ; wikibase:rank ?rank .
    FILTER(?rank != wikibase:DeprecatedRank)
    OPTIONAL {{ ?st pqv:P580 ?sv . ?sv wikibase:timeValue ?start ; wikibase:timePrecision ?startPrecision . }}
    OPTIONAL {{ ?st pqv:P582 ?ev . ?ev wikibase:timeValue ?end ; wikibase:timePrecision ?endPrecision . }}
    OPTIONAL {{ ?value rdfs:label ?v1 . FILTER(LANG(?v1) = "{lang}") }}
    OPTIONAL {{ ?value rdfs:label ?v2 . FILTER(LANG(?v2) = "mul") }}
    BIND(COALESCE(?v1, ?v2) AS ?valueLabel)
    BIND(EXISTS {{ ?value wdt:P31 wd:Q5 }} AS ?human)
  }}
}}
LIMIT {ROW_LIMIT}
"#
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn qids() {
        assert_eq!(QId::parse("Q42").map(QId::number), Some(42));
        assert_eq!(
            QId::parse(" q7 ").map(|q| q.to_string()),
            Some("Q7".to_owned())
        );
        for bad in [
            "",
            "Q",
            "Q0",
            "Q01",
            "42",
            "P42",
            "L1",
            "Q-1",
            "Q1}",
            "Q1\" .",
            "Q1\nQ2",
            "Q1 } . ?s ?p ?o {",
            "Q１",
            "Q1234567890123",
            "wd:Q1",
        ] {
            assert!(QId::parse(bad).is_none(), "{bad:?}");
        }
        assert_eq!(
            QId::from_iri("http://www.wikidata.org/entity/Q99"),
            QId::new(99)
        );
        assert_eq!(
            QId::from_iri("https://www.wikidata.org/entity/Q99"),
            QId::new(99)
        );
        for iri in [
            "http://www.wikidata.org/entity/P31",
            "http://www.wikidata.org/entity/L5",
            "http://www.wikidata.org/.well-known/genid/abc",
            "http://example.invalid/entity/Q1",
            "t123",
        ] {
            assert_eq!(QId::from_iri(iri), None, "{iri}");
        }
        assert_eq!(QId::new(0), None);
    }

    #[test]
    fn languages() {
        for ok in ["en", "sr", "sr-el", "zh-hant", "de-ch", "mul"] {
            assert!(valid_language(ok), "{ok}");
        }
        for bad in [
            "",
            "e",
            "EN",
            "en_",
            "en-",
            "en\"",
            "en\") }",
            "e n",
            "english",
            "en-toolongtag",
        ] {
            assert!(!valid_language(bad), "{bad}");
        }
    }

    #[test]
    fn query_holds_only_validated_values() {
        let q = sparql(Subject::Item(QId::new(999_000_001).unwrap()), "en");
        assert!(q.contains("VALUES ?item { wd:Q999000001 }"));
        assert!(q.contains("FILTER(LANG(?l1) = \"en\")"));
        assert!(q.contains("(\"P749\" p:P749 ps:P749)"));
        let q = sparql(Subject::Lei("FAKE0000000000000101"), "sr-el");
        assert!(q.contains("?item wdt:P1278 \"FAKE0000000000000101\" ."));
        // Braces are balanced: nothing opened a block it did not close.
        let opens = q.matches('{').count();
        assert_eq!(opens, q.matches('}').count());
    }

    #[test]
    #[should_panic(expected = "LEI not validated")]
    fn unvalidated_lei_never_reaches_the_query() {
        let _ = sparql(Subject::Lei("FAKE\" } . ?s ?p ?o {"), "en");
    }

    #[test]
    #[should_panic(expected = "language tag not validated")]
    fn unvalidated_language_never_reaches_the_query() {
        let _ = sparql(Subject::Item(QId::new(1).unwrap()), "en\") }");
    }
}
