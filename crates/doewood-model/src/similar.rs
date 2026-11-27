//! Typed similarity between documents (issue #169): the relations a `similar_to` edge holds,
//! and the exact-match keys ingest finds some of them by.
//!
//! A pair of documents has at most one `similar_to` edge (from the smaller hash to the
//! larger, ADR 0001 section 7). Its props say which relations hold:
//!
//! - one text relation at most, the strongest that holds: [`SimilarRelation::Identical`] (the
//!   same folded words, `sketch.text_key`), else [`SimilarRelation::NearCopy`] (raw `MinHash`
//!   estimate at or above `similar_near_copy_min_jaccard`), else
//!   [`SimilarRelation::RelatedVersion`] (at or above `similar_min_jaccard`);
//! - [`SimilarRelation::SameTemplate`]: masked similarity at or above
//!   `similar_template_min_masked`, and a shared template property or heading skeleton;
//! - [`SimilarRelation::SameOrigin`]: a shared document identity id (XMP, `w15:docId`,
//!   `w:rsidRoot`).
//!
//! An edge written before issue #169 has no `relations`: it was a `MinHash` edge, and
//! [`edge_relations`] reads it as a near copy or a related version by its `score`.

use serde_json::{Map, Value, json};

use crate::{Blake3, Error};

/// The relations, in the order they are reported.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum SimilarRelation {
    /// The same folded text (`sketch.text_key`), at least the minimum length.
    Identical,
    /// Raw `MinHash` estimate at or above the near-copy threshold.
    NearCopy,
    /// Raw `MinHash` estimate at or above the related-version threshold, below near copy.
    RelatedVersion,
    /// Masked similarity at or above its threshold, with a shared template property or
    /// heading skeleton.
    SameTemplate,
    /// A shared document identity id.
    SameOrigin,
}

impl SimilarRelation {
    /// Every relation, in report order.
    pub const ALL: [Self; 5] = [
        Self::Identical,
        Self::NearCopy,
        Self::RelatedVersion,
        Self::SameTemplate,
        Self::SameOrigin,
    ];

    /// The name in edge props and docs.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Identical => "identical",
            Self::NearCopy => "near_copy",
            Self::RelatedVersion => "related_version",
            Self::SameTemplate => "same_template",
            Self::SameOrigin => "same_origin",
        }
    }

    /// The relation named `s`.
    pub fn parse(s: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|r| r.as_str() == s)
    }

    /// How the relation is found: `text_key`, `minhash_lsh`, `masked_simhash` or `origin_id`.
    pub fn method(self) -> &'static str {
        match self {
            Self::Identical => "text_key",
            Self::NearCopy | Self::RelatedVersion => "minhash_lsh",
            Self::SameTemplate => "masked_simhash",
            Self::SameOrigin => "origin_id",
        }
    }

    /// The bit of this relation in a set.
    pub fn bit(self) -> u8 {
        1 << (self as u8)
    }
}

/// What a similarity key is. Stored as a small integer next to the key.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum SimilarKeyKind {
    /// The first 64 bits of `sketch.text_key`: the same folded words.
    Text = 1,
    /// A template property (`docx.template`, the file name of `docx.attached_template`).
    TemplateProperty = 2,
    /// The heading skeleton digest (`sketch.headings`).
    Headings = 3,
    /// An XMP `DocumentID` or `OriginalDocumentID`.
    OriginXmp = 4,
    /// A `w15:docId`.
    OriginW15 = 5,
    /// A `w:rsidRoot`.
    OriginRsidRoot = 6,
}

impl SimilarKeyKind {
    /// Every kind.
    pub const ALL: [Self; 6] = [
        Self::Text,
        Self::TemplateProperty,
        Self::Headings,
        Self::OriginXmp,
        Self::OriginW15,
        Self::OriginRsidRoot,
    ];

    /// The stored integer.
    pub fn code(self) -> i16 {
        self as i16
    }

    /// The kind stored as `code`.
    pub fn from_code(code: i16) -> Option<Self> {
        Self::ALL.into_iter().find(|k| k.code() == code)
    }

    /// The relation a shared key of this kind is evidence for.
    pub fn relation(self) -> SimilarRelation {
        match self {
            Self::Text => SimilarRelation::Identical,
            Self::TemplateProperty | Self::Headings => SimilarRelation::SameTemplate,
            Self::OriginXmp | Self::OriginW15 | Self::OriginRsidRoot => SimilarRelation::SameOrigin,
        }
    }

    /// The name in edge props (`template` or `origin`).
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Text => "text_key",
            Self::TemplateProperty => "template_property",
            Self::Headings => "headings",
            Self::OriginXmp => "xmp",
            Self::OriginW15 => "w15",
            Self::OriginRsidRoot => "rsid_root",
        }
    }
}

/// One exact-match key of a document.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SimilarKey {
    /// What it is.
    pub kind: SimilarKeyKind,
    /// Its 64 bits.
    pub key: u64,
}

/// A document's similarity keys and its masked `SimHash` (`sketch.simhash_template`), which
/// is stored with the keys so a template match can be checked without its record.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SimilarKeys {
    /// The keys, each once.
    pub keys: Vec<SimilarKey>,
    /// The masked `SimHash`.
    pub masked: Option<u64>,
}

/// Most keys one document carries.
pub const SIMILAR_MAX_KEYS: usize = 32;

/// Checks keys before they reach a backend.
pub fn validate_keys(keys: &[SimilarKey]) -> Result<(), Error> {
    if keys.len() > SIMILAR_MAX_KEYS {
        return Err(Error::Invalid(format!(
            "a document has at most {SIMILAR_MAX_KEYS} similarity keys"
        )));
    }
    Ok(())
}

/// Another document that shares a key, as a candidate lookup returns it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct KeyMatch {
    /// The document.
    pub blake3: Blake3,
    /// The kind of the shared key.
    pub kind: SimilarKeyKind,
    /// Its masked `SimHash`, if it has one.
    pub masked: Option<u64>,
    /// Its `MinHash` signature, if it has one.
    pub signature: Option<Vec<u8>>,
}

/// The masked similarity of two `sketch.simhash_template` values: the cosine of the angle a
/// `SimHash` estimates, `cos(pi * hamming / 64)`. 1 for equal values, 0 at 32 differing bits
/// (unrelated), negative beyond. quail's "same template" bar of 8 bits is 0.92; 0.60 is 18
/// bits.
pub fn masked_similarity(a: u64, b: u64) -> f64 {
    let bits = f64::from((a ^ b).count_ones());
    (std::f64::consts::PI * bits / 64.0).cos()
}

/// The props of a `similar_to` edge (issue #169). `score` stays the raw `MinHash` estimate
/// when it is known, as before; `relations` names every relation that holds.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct SimilarEdge {
    /// The relations that hold, each once, in report order.
    pub relations: Vec<SimilarRelation>,
    /// Raw `MinHash` estimate, when both documents have a signature.
    pub raw_similarity: Option<f64>,
    /// Masked similarity, when both have a masked `SimHash` the writer knew.
    pub masked_similarity: Option<f64>,
    /// Band keys shared, when found through the band index.
    pub shared_bands: Option<u32>,
    /// The shared template key (`template_property` or `headings`).
    pub template: Option<SimilarKeyKind>,
    /// The shared origin id (`xmp`, `w15` or `rsid_root`).
    pub origin: Option<SimilarKeyKind>,
    /// The document's text length (`text.chars`) when identical.
    pub chars: Option<u64>,
}

impl SimilarEdge {
    /// The edge props.
    pub fn to_props(&self) -> Value {
        let mut p = Map::new();
        let first = self.relations.first().copied();
        p.insert(
            "method".into(),
            json!(first.map_or("minhash_lsh", SimilarRelation::method)),
        );
        p.insert(
            "relations".into(),
            json!(
                self.relations
                    .iter()
                    .map(|r| r.as_str())
                    .collect::<Vec<_>>()
            ),
        );
        if let Some(x) = self.raw_similarity {
            p.insert("score".into(), json!(x));
            p.insert("raw_similarity".into(), json!(x));
        }
        if let Some(x) = self.masked_similarity {
            p.insert("masked_similarity".into(), json!(x));
        }
        if let Some(n) = self.shared_bands {
            p.insert("shared_bands".into(), json!(n));
        }
        if let Some(k) = self.template {
            p.insert("template".into(), json!(k.as_str()));
        }
        if let Some(k) = self.origin {
            p.insert("origin".into(), json!(k.as_str()));
        }
        if let Some(n) = self.chars {
            p.insert("chars".into(), json!(n));
        }
        Value::Object(p)
    }

    /// The edge's confidence: the larger of the two similarities, 1 when neither is known
    /// (a shared origin id alone) or the texts are identical.
    #[allow(clippy::cast_possible_truncation)]
    pub fn confidence(&self) -> f32 {
        if self.relations.contains(&SimilarRelation::Identical) {
            return 1.0;
        }
        let best = match (self.raw_similarity, self.masked_similarity) {
            (Some(a), Some(b)) => a.max(b),
            (Some(a), None) | (None, Some(a)) => a,
            (None, None) => 1.0,
        };
        best.clamp(0.0, 1.0) as f32
    }
}

/// One relation of an edge, as `SimilarDocuments` reports it.
#[derive(Clone, Debug, PartialEq)]
pub struct EdgeRelation {
    /// The relation.
    pub relation: SimilarRelation,
    /// Raw `MinHash` estimate, if known.
    pub raw_similarity: Option<f64>,
    /// Masked similarity, if known.
    pub masked_similarity: Option<f64>,
    /// How it was found ([`SimilarRelation::method`]).
    pub method: &'static str,
    /// What matched, as a JSON object: `shared_bands`, `template`, `origin` or `chars`.
    pub detail: Value,
}

/// The relations a `similar_to` edge's props hold. An edge from before issue #169 (no
/// `relations`) is a `MinHash` edge: a near copy when its `score` is at or above
/// `near_copy_min`, else a related version. Unknown relation names are left out.
pub fn edge_relations(props: &Value, near_copy_min: f64) -> Vec<EdgeRelation> {
    let num = |k: &str| props.get(k).and_then(Value::as_f64);
    let raw = num("raw_similarity").or_else(|| num("score"));
    let masked = num("masked_similarity");
    let names: Vec<SimilarRelation> = match props.get("relations").and_then(Value::as_array) {
        Some(list) => list
            .iter()
            .filter_map(|v| v.as_str().and_then(SimilarRelation::parse))
            .collect(),
        None => match raw {
            Some(x) if x >= near_copy_min => vec![SimilarRelation::NearCopy],
            Some(_) => vec![SimilarRelation::RelatedVersion],
            None => Vec::new(),
        },
    };
    let mut seen = 0u8;
    names
        .into_iter()
        .filter(|r| {
            let new = seen & r.bit() == 0;
            seen |= r.bit();
            new
        })
        .map(|relation| {
            let mut detail = Map::new();
            let mut copy = |k: &str| {
                if let Some(v) = props.get(k) {
                    detail.insert(k.into(), v.clone());
                }
            };
            match relation {
                SimilarRelation::Identical => copy("chars"),
                SimilarRelation::NearCopy | SimilarRelation::RelatedVersion => {
                    copy("shared_bands");
                }
                SimilarRelation::SameTemplate => copy("template"),
                SimilarRelation::SameOrigin => copy("origin"),
            }
            EdgeRelation {
                relation,
                raw_similarity: raw,
                masked_similarity: masked,
                method: relation.method(),
                detail: Value::Object(detail),
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names_and_codes_round_trip() {
        for r in SimilarRelation::ALL {
            assert_eq!(SimilarRelation::parse(r.as_str()), Some(r));
        }
        for k in SimilarKeyKind::ALL {
            assert_eq!(SimilarKeyKind::from_code(k.code()), Some(k));
        }
        assert_eq!(SimilarKeyKind::from_code(0), None);
    }

    #[test]
    fn masked_similarity_is_the_simhash_cosine() {
        assert!((masked_similarity(7, 7) - 1.0).abs() < 1e-12);
        assert!(masked_similarity(0, u64::from(u32::MAX)).abs() < 1e-12);
        assert!((masked_similarity(0, 0xff) - 0.9239).abs() < 1e-4);
        // 18 bits is just above 0.60, 19 just below.
        assert!(masked_similarity(0, (1 << 18) - 1) > 0.60);
        assert!(masked_similarity(0, (1 << 19) - 1) < 0.60);
    }

    #[test]
    fn props_round_trip_and_old_edges_read_by_score() {
        let e = SimilarEdge {
            relations: vec![SimilarRelation::NearCopy, SimilarRelation::SameTemplate],
            raw_similarity: Some(0.93),
            masked_similarity: Some(0.97),
            shared_bands: Some(20),
            template: Some(SimilarKeyKind::Headings),
            ..SimilarEdge::default()
        };
        let props = e.to_props();
        assert_eq!(props["method"], "minhash_lsh");
        assert_eq!(props["score"], 0.93);
        let got = edge_relations(&props, 0.9);
        assert_eq!(got.len(), 2);
        assert_eq!(got[0].relation, SimilarRelation::NearCopy);
        assert_eq!(got[0].detail, json!({"shared_bands": 20}));
        assert_eq!(got[1].relation, SimilarRelation::SameTemplate);
        assert_eq!(got[1].method, "masked_simhash");
        assert_eq!(got[1].detail, json!({"template": "headings"}));
        assert_eq!(got[1].masked_similarity, Some(0.97));
        // Before issue #169: score, method and shared bands only.
        let old = json!({"score": 0.95, "method": "minhash_lsh", "shared_bands": 30});
        let got = edge_relations(&old, 0.9);
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].relation, SimilarRelation::NearCopy);
        assert_eq!(got[0].raw_similarity, Some(0.95));
        let got = edge_relations(&old, 0.97);
        assert_eq!(got[0].relation, SimilarRelation::RelatedVersion);
        assert!(edge_relations(&json!({}), 0.9).is_empty());
        // Unknown names and repeats are left out.
        let odd = json!({"relations": ["same_origin", "later", "same_origin"]});
        let got = edge_relations(&odd, 0.9);
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].raw_similarity, None);
    }

    #[test]
    fn confidence() {
        let mut e = SimilarEdge {
            relations: vec![SimilarRelation::SameOrigin],
            ..SimilarEdge::default()
        };
        assert!((e.confidence() - 1.0).abs() < f32::EPSILON);
        e.raw_similarity = Some(0.4);
        e.masked_similarity = Some(0.7);
        assert!((e.confidence() - 0.7).abs() < 1e-6);
        e.relations.insert(0, SimilarRelation::Identical);
        assert!((e.confidence() - 1.0).abs() < f32::EPSILON);
    }
}
