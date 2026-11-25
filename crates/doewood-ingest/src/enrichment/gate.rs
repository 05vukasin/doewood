//! The decision gate of ADR 0002 section 1.6, as enrichment applies it.
//!
//! - **A name search's records** ([`pick`]): each candidate's name is scored against the
//!   company's (the same key and legal form is the name ladder's 0.90, the same key with a
//!   form missing on one side the legal-form-blind 0.85, else the fuzzy score with the
//!   jurisdiction veto; different jurisdictions are never the same company). The noisy-OR of
//!   the evidence goes through the gate: a record at 0.90 or more is adopted (its identifiers
//!   join the company), one in the review band is a proposal, below 0.50 nothing.
//! - **Other companies** ([`run`]): a company that owns an identifier the registry gave for
//!   this one (found by this company's own identifier) is a registry identifier hit (0.95); a
//!   company that owns a key of the registry's legal or former names is a name-ladder hit. A
//!   company with a different national registry number (MB, PIB, OIB, Companies House number,
//!   CIK) or in another jurisdiction is never a candidate. Fused, gated: a merge at 0.90 or
//!   more through the merge log (actor `enrichment`, with the evidence), never for a dismissed
//!   pair; the review band and conflicts become proposals on the company.
//! - Identifiers adopted from a name match never count as registry identifier hits: a company
//!   that owns one is a proposal, not a merge.

// `b` (the backend), `s` (the company) and `f` (the findings) are used throughout.
#![allow(clippy::many_single_char_names)]

use std::collections::BTreeMap;

use doewood_enrich::fusion::{Candidate, Decision, Evidence, ReviewReason, Source, decide, fuse};
use doewood_enrich::fuzzy::{self, FuzzyVerdict};
use doewood_enrich::{CompanyName, IdKind, LadderStep};
use doewood_model::{Error, MergeActor, MergeActorKind, MergeRequest, NodeKind, Result};
use doewood_registry::Registry;
use serde_json::{Value, json};
use tracing::{debug, info, warn};
use uuid::Uuid;

use super::company::{Findings, Job};
use super::subject::Subject;
use super::write::live_id;

/// A record a name search returned.
#[derive(Clone, Debug)]
pub(crate) struct NameCandidate {
    /// Its registered name.
    pub name: String,
    /// Its country, when known.
    pub country: Option<String>,
    /// Its record id at the registry.
    pub record: String,
}

/// The name evidence that `a` (in `ac`) is the company named `b` (in `bc`).
pub(crate) fn name_evidence(
    a: &CompanyName,
    ac: Option<&str>,
    b: &CompanyName,
    bc: Option<&str>,
) -> Option<Evidence> {
    if let (Some(x), Some(y)) = (ac, bc)
        && !x.eq_ignore_ascii_case(y)
    {
        return None;
    }
    if a.key == b.key {
        return match (a.legal_form, b.legal_form) {
            (x, y) if x == y => Some(Evidence::ladder(LadderStep::Normalized)),
            (None, _) | (_, None) => Some(Evidence::ladder(LadderStep::FormBlind)),
            _ => None,
        };
    }
    match fuzzy::score(a, ac, b, bc) {
        FuzzyVerdict::Match(m) => Some(Evidence::fuzzy(&m)),
        FuzzyVerdict::NoMatch | FuzzyVerdict::Veto => None,
    }
}

/// The record of a name search the company is, at the gate: `Some((index, probability))` to
/// adopt; review-band candidates become proposals in `f`.
pub(crate) fn pick(
    s: &Subject,
    candidates: &[NameCandidate],
    registry: Registry,
    f: &mut Findings,
) -> Option<(usize, f64)> {
    let name = s.name.as_ref()?;
    let mut seen = Vec::new();
    let mut scored = Vec::new();
    for (i, c) in candidates.iter().enumerate() {
        if seen.contains(&c.record) {
            continue;
        }
        seen.push(c.record.clone());
        let Some(other) = CompanyName::parse(&c.name) else {
            continue;
        };
        if let Some(e) = name_evidence(name, s.country.as_deref(), &other, c.country.as_deref()) {
            scored.push(Candidate::new(i, &[e]));
        }
    }
    match decide(&scored) {
        Decision::Merge(i) => {
            let p = scored
                .iter()
                .find(|c| c.id == i)
                .map_or(0.0, |c| c.probability);
            debug!(registry = %registry, record = %candidates[i].record, probability = p, "name search: adopted");
            Some((i, p))
        }
        Decision::Review(reason) => {
            for c in scored
                .iter()
                .filter(|c| c.probability >= doewood_enrich::fusion::NEW_BELOW)
            {
                let cand = &candidates[c.id];
                f.proposals.push(json!({
                    "registry": registry.as_str(),
                    "record": cand.record,
                    "name": cand.name,
                    "probability": c.probability,
                    "reason": reason_name(reason),
                }));
            }
            None
        }
        Decision::New => None,
    }
}

fn reason_name(r: ReviewReason) -> &'static str {
    match r {
        ReviewReason::Uncertain => "uncertain",
        ReviewReason::Conflict => "conflict",
    }
}

/// What the gate did with other companies.
#[derive(Debug, Default)]
pub(crate) struct Gated {
    /// Merges done.
    pub merged: u64,
    /// Proposals recorded.
    pub proposed: u64,
    /// The proposals, for the company's props.
    pub proposals: Vec<Value>,
}

/// Whether two companies carry different national registry numbers of one kind: then they
/// are different companies, whatever their names say. LEIs never decide this (a branch has
/// its own).
fn conflicting(a: &Subject, b: &Subject) -> bool {
    for kind in [IdKind::RsMb, IdKind::RsPib, IdKind::HrOib, IdKind::Vat] {
        let x: Vec<&str> = a
            .ids
            .iter()
            .filter(|i| i.kind == kind)
            .map(|i| i.value.as_str())
            .collect();
        let y: Vec<&str> = b
            .ids
            .iter()
            .filter(|i| i.kind == kind)
            .map(|i| i.value.as_str())
            .collect();
        if !x.is_empty() && !y.is_empty() && !x.iter().any(|v| y.contains(v)) {
            return true;
        }
    }
    matches!((&a.ch, &b.ch), (Some(x), Some(y)) if x != y)
        || matches!((a.cik, b.cik), (Some(x), Some(y)) if x != y)
}

/// Looks for other companies that are this one, and merges or proposes.
#[allow(clippy::too_many_lines)]
pub(crate) async fn run(job: &Job<'_>, s: &mut Subject, f: &Findings) -> Result<Gated> {
    let b = job.backend.as_ref();
    let me = s.id();
    let mut evidence: BTreeMap<Uuid, Vec<(Evidence, Value)>> = BTreeMap::new();
    let mut out = Gated::default();
    for found in &f.found {
        // Wikidata is the lowest tier: never a registry identifier hit.
        if found.alias.starts_with("reg:wikidata:") {
            continue;
        }
        let Some(owner) = b
            .alias(NodeKind::Company, &found.alias)
            .await?
            .and_then(|a| a.node)
        else {
            continue;
        };
        let owner = live_id(b, owner).await?;
        if owner == me {
            continue;
        }
        if let Some(p) = found.by_name {
            out.proposals.push(json!({
                "company": owner.to_string(),
                "probability": p,
                "reason": "owns an identifier of a name match",
                "alias": found.alias,
                "evidence": [found.evidence],
            }));
            out.proposed += 1;
            continue;
        }
        evidence.entry(owner).or_default().push((
            Evidence::new(Source::RegistryId),
            json!({"source": Source::RegistryId.as_str(), "reliability": Source::RegistryId.reliability(), "alias": found.alias, "record": found.evidence}),
        ));
    }
    let names = f
        .names
        .iter()
        .map(|n| (n.as_str(), false))
        .chain(f.former.iter().map(|n| (n.name.as_str(), true)));
    for (name, former) in names {
        let Some(parsed) = CompanyName::parse(name) else {
            continue;
        };
        for (step, key) in parsed.ladder_keys() {
            let Some(owner) = b.alias(NodeKind::Company, &key).await?.and_then(|a| a.node) else {
                continue;
            };
            let owner = live_id(b, owner).await?;
            if owner == me {
                continue;
            }
            let step = if former && step == LadderStep::Normalized {
                LadderStep::LearnedAlias
            } else {
                step
            };
            let e = Evidence::ladder(step);
            evidence.entry(owner).or_default().push((
                e,
                json!({"source": e.source.as_str(), "reliability": e.reliability, "step": step.as_str(), "alias": key, "name": name}),
            ));
        }
    }
    let mut candidates = Vec::new();
    for (owner, ev) in &evidence {
        let Some(other) = Subject::load(b, *owner).await? else {
            continue;
        };
        if conflicting(s, &other) {
            debug!(company = %me, other = %owner, "different registry numbers: not the same company");
            continue;
        }
        if let (Some(x), Some(y)) = (&s.country, &other.country)
            && x != y
        {
            continue;
        }
        let list: Vec<Evidence> = ev.iter().map(|(e, _)| *e).collect();
        candidates.push(Candidate {
            id: *owner,
            probability: fuse(&list),
        });
    }
    let json_of = |id: &Uuid| -> Value {
        Value::Array(
            evidence
                .get(id)
                .map(|v| v.iter().map(|(_, j)| j.clone()).collect())
                .unwrap_or_default(),
        )
    };
    match decide(&candidates) {
        Decision::Merge(other) => {
            let p = candidates
                .iter()
                .find(|c| c.id == other)
                .map_or(0.0, |c| c.probability);
            if let Some(d) = b.dismissal(me, other).await? {
                debug!(company = %me, other = %other, "a dismissed pair is never merged");
                out.proposals.push(json!({
                    "company": other.to_string(),
                    "probability": p,
                    "reason": "dismissed",
                    "dismissal": d.seq,
                    "evidence": json_of(&other),
                }));
                out.proposed += 1;
            } else if !job.lease_other(other).await? {
                debug!(company = %me, other = %other, "the other company is leased elsewhere; merged later");
            } else {
                let mine = b.company_documents(me).await?.len();
                let theirs = b.company_documents(other).await?.len();
                let (survivor, absorbed) = if theirs > mine {
                    (other, me)
                } else {
                    (me, other)
                };
                #[allow(clippy::cast_possible_truncation)]
                let p32 = p as f32;
                let req = MergeRequest {
                    survivor,
                    absorbed,
                    actor: MergeActor {
                        kind: MergeActorKind::Enrichment,
                        instance: job.owner.clone(),
                    },
                    evidence: json_of(&other),
                    confidence: p32,
                    probability: p32,
                };
                match b.merge_companies(&req).await {
                    Ok(entry) => {
                        info!(survivor = %survivor, absorbed = %absorbed, merge = %entry.id, probability = p, "companies merged");
                        out.merged += 1;
                        if survivor != me
                            && let Some(mut next) = Subject::load(b, survivor).await?
                        {
                            next.ids.extend(s.ids.iter().cloned());
                            next.ch = next.ch.take().or_else(|| s.ch.clone());
                            next.cik = next.cik.or(s.cik);
                            next.qid = next.qid.or(s.qid);
                            next.country = next.country.take().or_else(|| s.country.clone());
                            *s = next;
                        }
                    }
                    Err(Error::Precondition(why)) => {
                        warn!(company = %me, other = %other, why, "merge refused");
                    }
                    Err(e) => return Err(e),
                }
            }
        }
        Decision::Review(reason) => {
            for c in candidates
                .iter()
                .filter(|c| c.probability >= doewood_enrich::fusion::NEW_BELOW)
            {
                out.proposals.push(json!({
                    "company": c.id.to_string(),
                    "probability": c.probability,
                    "reason": reason_name(reason),
                    "evidence": json_of(&c.id),
                }));
                out.proposed += 1;
            }
        }
        Decision::New => {}
    }
    Ok(out)
}
