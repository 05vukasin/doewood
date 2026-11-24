//! Evidence fusion and the decision gate (ADR 0002 section 1.6).
//!
//! Each piece of evidence that a document's party is a known company comes from a source with
//! a reliability ([`Source::reliability`], the ADR's table). The probability that the match is
//! right is the noisy-OR over **distinct** sources:
//!
//! ```text
//! P = 1 - Π over distinct sources s of (1 - r_s)
//! ```
//!
//! where `r_s` is the strongest piece of evidence from source `s` (two hits from the same
//! source count once). The gate ([`decide`]) then merges at P ≥ 0.90, creates a new company
//! only when every candidate is below 0.50, and sends everything else to review, including two
//! conflicting high-precision hits. An LLM verdict is advisory, never decisive, so it is not a
//! source here.

use std::collections::BTreeMap;

use crate::fuzzy::FuzzyMatch;
use crate::name::LadderStep;

/// Merge automatically at or above this probability (ADR 0002 section 1.6).
pub const MERGE_AT: f64 = 0.90;
/// Create a new company only when every candidate is below this (ADR 0002 section 1.6).
pub const NEW_BELOW: f64 = 0.50;

/// An evidence source, with its reliability from ADR 0002 section 1.6.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Source {
    /// A validated registry identifier: 0.95.
    RegistryId,
    /// A parent contract: 0.95.
    ParentContract,
    /// An email domain (never a free-mail one): 0.90.
    EmailDomain,
    /// The name ladder: 0.90, scaled down on the looser and fuzzy steps.
    NameLadder,
    /// The file name: 0.85.
    Filename,
    /// The invoice layout: 0.75.
    InvoiceLayout,
    /// Semantic similarity: 0.55.
    SemanticSimilarity,
}

impl Source {
    /// Every source, in the ADR's table order.
    pub const ALL: [Self; 7] = [
        Self::RegistryId,
        Self::ParentContract,
        Self::EmailDomain,
        Self::NameLadder,
        Self::Filename,
        Self::InvoiceLayout,
        Self::SemanticSimilarity,
    ];

    /// The reliability of the source (ADR 0002 section 1.6, the table).
    pub fn reliability(self) -> f64 {
        match self {
            Self::RegistryId | Self::ParentContract => 0.95,
            Self::EmailDomain | Self::NameLadder => 0.90,
            Self::Filename => 0.85,
            Self::InvoiceLayout => 0.75,
            Self::SemanticSimilarity => 0.55,
        }
    }

    /// Lowercase name.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::RegistryId => "registry_id",
            Self::ParentContract => "parent_contract",
            Self::EmailDomain => "email_domain",
            Self::NameLadder => "name_ladder",
            Self::Filename => "filename",
            Self::InvoiceLayout => "invoice_layout",
            Self::SemanticSimilarity => "semantic_similarity",
        }
    }
}

/// One piece of evidence: a source and how much of its reliability this hit carries.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Evidence {
    /// Where it comes from.
    pub source: Source,
    /// The reliability of this hit, in [0, 1]: the source's, or less on a looser step.
    pub reliability: f64,
}

impl Evidence {
    /// A hit at the full reliability of `source`.
    pub fn new(source: Source) -> Self {
        Self {
            source,
            reliability: source.reliability(),
        }
    }

    /// A deterministic ladder step. A registry identifier is [`Source::RegistryId`]; the name
    /// steps are [`Source::NameLadder`] at the step's reliability
    /// ([`LadderStep::reliability`]: 0.90, scaled down to 0.88 squashed and 0.85
    /// legal-form-blind).
    pub fn ladder(step: LadderStep) -> Self {
        if step == LadderStep::Identifier {
            return Self::new(Source::RegistryId);
        }
        Self {
            source: Source::NameLadder,
            reliability: step.reliability(),
        }
    }

    /// A fuzzy name match (ladder steps 10 and 11): the name ladder's 0.90 scaled by the
    /// match's Jaccard score. The ADR says only "scaled down on the fuzzy steps"; scaling by
    /// the score keeps a fuzzy hit below [`MERGE_AT`] unless every token paired.
    pub fn fuzzy(m: &FuzzyMatch) -> Self {
        Self {
            source: Source::NameLadder,
            reliability: Source::NameLadder.reliability() * m.score.clamp(0.0, 1.0),
        }
    }
}

/// The noisy-OR over distinct sources: the strongest hit of each source, then
/// `1 - Π (1 - r)` (ADR 0002 section 1.6). No evidence gives 0.
pub fn fuse(evidence: &[Evidence]) -> f64 {
    let mut best: BTreeMap<Source, f64> = BTreeMap::new();
    for e in evidence {
        let r = e.reliability.clamp(0.0, 1.0);
        let slot = best.entry(e.source).or_insert(0.0);
        if r > *slot {
            *slot = r;
        }
    }
    let p = 1.0 - best.values().map(|r| 1.0 - r).product::<f64>();
    // Rounded to 12 decimals, so float noise never moves a hit across a gate threshold.
    (p * 1e12).round() / 1e12
}

/// A known company that may be the party, with its fused probability.
#[derive(Clone, Debug, PartialEq)]
pub struct Candidate<Id> {
    /// The company.
    pub id: Id,
    /// [`fuse`] of its evidence.
    pub probability: f64,
}

impl<Id> Candidate<Id> {
    /// A candidate with the noisy-OR of `evidence`.
    pub fn new(id: Id, evidence: &[Evidence]) -> Self {
        Self {
            id,
            probability: fuse(evidence),
        }
    }
}

/// What the gate decided.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Decision<Id> {
    /// Merge into this company: exactly one candidate at or above [`MERGE_AT`].
    Merge(Id),
    /// Create a new company: every candidate below [`NEW_BELOW`] (or none).
    New,
    /// A person decides.
    Review(ReviewReason),
}

/// Why the gate sent a party to review.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReviewReason {
    /// The best candidate is in [0.50, 0.90).
    Uncertain,
    /// Two or more different candidates are high-precision hits (at or above [`MERGE_AT`]).
    Conflict,
}

/// The decision gate of ADR 0002 section 1.6. Candidates are distinct companies.
pub fn decide<Id: Clone>(candidates: &[Candidate<Id>]) -> Decision<Id> {
    let mut high = candidates.iter().filter(|c| c.probability >= MERGE_AT);
    match (high.next(), high.next()) {
        (Some(_), Some(_)) => Decision::Review(ReviewReason::Conflict),
        (Some(c), None) => Decision::Merge(c.id.clone()),
        (None, _) if candidates.iter().all(|c| c.probability < NEW_BELOW) => Decision::New,
        (None, _) => Decision::Review(ReviewReason::Uncertain),
    }
}

#[cfg(test)]
#[allow(
    clippy::float_cmp,
    reason = "the gate thresholds are compared as written"
)]
mod tests {
    use super::*;
    use crate::fuzzy::{self, FuzzyVerdict};
    use crate::name::CompanyName;

    fn one(p: f64) -> Decision<&'static str> {
        decide(&[Candidate {
            id: "a",
            probability: p,
        }])
    }

    #[test]
    fn reliabilities_are_the_adr_table() {
        let table: Vec<(Source, f64)> = Source::ALL.iter().map(|s| (*s, s.reliability())).collect();
        assert_eq!(
            table,
            [
                (Source::RegistryId, 0.95),
                (Source::ParentContract, 0.95),
                (Source::EmailDomain, 0.90),
                (Source::NameLadder, 0.90),
                (Source::Filename, 0.85),
                (Source::InvoiceLayout, 0.75),
                (Source::SemanticSimilarity, 0.55),
            ]
        );
        assert_eq!(Source::InvoiceLayout.as_str(), "invoice_layout");
    }

    #[test]
    fn noisy_or_over_distinct_sources() {
        assert_eq!(fuse(&[]), 0.0);
        assert_eq!(fuse(&[Evidence::new(Source::Filename)]), 0.85);
        // 1 - 0.05 * 0.10
        assert_eq!(
            fuse(&[
                Evidence::new(Source::RegistryId),
                Evidence::new(Source::NameLadder)
            ]),
            0.995
        );
        // 1 - 0.25 * 0.45
        assert_eq!(
            fuse(&[
                Evidence::new(Source::InvoiceLayout),
                Evidence::new(Source::SemanticSimilarity)
            ]),
            0.8875
        );
        // The same source twice counts once, at its strongest hit.
        let weak = Evidence {
            source: Source::NameLadder,
            reliability: 0.5,
        };
        assert_eq!(fuse(&[weak, weak, weak]), 0.5);
        assert_eq!(fuse(&[weak, Evidence::new(Source::NameLadder), weak]), 0.90);
        // Out-of-range reliabilities are clamped.
        let silly = Evidence {
            source: Source::Filename,
            reliability: 1.7,
        };
        assert_eq!(fuse(&[silly]), 1.0);
        let negative = Evidence {
            source: Source::Filename,
            reliability: -0.3,
        };
        assert_eq!(fuse(&[negative]), 0.0);
    }

    #[test]
    fn gate_boundaries() {
        assert_eq!(one(0.4999), Decision::New);
        assert_eq!(one(0.50), Decision::Review(ReviewReason::Uncertain));
        assert_eq!(one(0.8999), Decision::Review(ReviewReason::Uncertain));
        assert_eq!(one(0.90), Decision::Merge("a"));
        assert_eq!(one(1.0), Decision::Merge("a"));
        assert_eq!(one(0.0), Decision::New);
        assert_eq!(decide::<&str>(&[]), Decision::New);
    }

    #[test]
    fn gate_over_several_candidates() {
        let c = |id, probability| Candidate { id, probability };
        // Every candidate below 0.50: a new company.
        assert_eq!(decide(&[c("a", 0.3), c("b", 0.4999)]), Decision::New);
        // One in between: review.
        assert_eq!(
            decide(&[c("a", 0.3), c("b", 0.5)]),
            Decision::Review(ReviewReason::Uncertain)
        );
        // One high hit and weaker others: merge into it.
        assert_eq!(decide(&[c("a", 0.6), c("b", 0.95)]), Decision::Merge("b"));
        // Two conflicting high-precision hits: always review.
        assert_eq!(
            decide(&[c("a", 0.95), c("b", 0.90)]),
            Decision::Review(ReviewReason::Conflict)
        );
        assert_eq!(
            decide(&[c("a", 0.99), c("b", 0.2), c("c", 0.995)]),
            Decision::Review(ReviewReason::Conflict)
        );
    }

    #[test]
    fn ladder_and_fuzzy_evidence() {
        // A deterministic name match alone merges; a registry identifier is 0.95.
        assert_eq!(fuse(&[Evidence::ladder(LadderStep::Normalized)]), 0.90);
        assert_eq!(
            decide(&[Candidate::new(
                "a",
                &[Evidence::ladder(LadderStep::Normalized)]
            )]),
            Decision::Merge("a")
        );
        assert_eq!(
            Evidence::ladder(LadderStep::Identifier).source,
            Source::RegistryId
        );
        assert_eq!(fuse(&[Evidence::ladder(LadderStep::Identifier)]), 0.95);
        // The looser steps alone go to review, with a second source they merge.
        let blind = Evidence::ladder(LadderStep::FormBlind);
        assert_eq!(fuse(&[blind]), 0.85);
        assert_eq!(
            decide(&[Candidate::new("a", &[blind])]),
            Decision::Review(ReviewReason::Uncertain)
        );
        assert_eq!(
            decide(&[Candidate::new(
                "a",
                &[blind, Evidence::new(Source::Filename)]
            )]),
            Decision::Merge("a")
        );
        // Two name steps are one source: no double counting.
        assert_eq!(fuse(&[blind, Evidence::ladder(LadderStep::Squashed)]), 0.88);

        // A fuzzy subset match: 0.90 * 2/3 = 0.6, review.
        let a = CompanyName::parse("Primer Gradnja Inženjering d.o.o.").unwrap();
        let b = CompanyName::parse("Пример Градња д.о.о.").unwrap();
        let FuzzyVerdict::Match(m) = fuzzy::score(&a, Some("RS"), &b, Some("RS")) else {
            panic!("should match");
        };
        let e = Evidence::fuzzy(&m);
        assert!((e.reliability - 0.6).abs() < 1e-12);
        assert_eq!(
            decide(&[Candidate::new("a", &[e])]),
            Decision::Review(ReviewReason::Uncertain)
        );
        // Semantic similarity alone never merges.
        assert_eq!(
            decide(&[Candidate::new(
                "a",
                &[Evidence::new(Source::SemanticSimilarity)]
            )]),
            Decision::Review(ReviewReason::Uncertain)
        );
    }
}
