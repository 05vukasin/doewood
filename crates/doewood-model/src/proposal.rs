//! Enrichment proposals (ADR 0002 section 1.6, issue #52): what the decision gate leaves to
//! a person, in a table of its own instead of capped company props.
//!
//! - **Kinds:** a registry record a name search found in the review band, for the company to
//!   adopt ([`ProposalKind::RegistryRecord`], candidate `<registry>:<record>`), and another
//!   company that may be the same ([`ProposalKind::Merge`], candidate the other company's id).
//!   A merge proposal names the pair in id order (`company` the smaller id), so the two
//!   companies' passes share one proposal.
//! - **Dedup:** one open proposal per kind, company and candidate. Proposing it again updates
//!   its evidence, probability, reason and detail.
//! - **Resolution:** an open proposal is accepted or rejected once, by a person (actor
//!   `manual`); a rejection may be a dismissal. A rejected and dismissed proposal is never
//!   opened again, and a merge proposal for a dismissed pair is never opened.
//!
//! [`accept`] and [`reject`] are the rules a person's decision follows; the API calls them.

use std::time::Duration;

use serde_json::{Value, json};
use uuid::Uuid;

use crate::merge::{validate_evidence, validate_reason, validate_unit};
use crate::{
    Backend, DismissRequest, Dismissal, EVIDENCE_MAX_BYTES, Error, MergeActor, MergeEntry,
    MergeRequest, NodeKind, ProposalKind, ProposalReason, ProposalState, PropsPatch, Result,
    merged_into,
};

/// Largest page of [`crate::ProposalStore::list_proposals`].
pub const PROPOSALS_MAX_LIMIT: u32 = 1000;

/// Longest candidate, in bytes.
pub const CANDIDATE_MAX_LEN: usize = 512;

/// The page size [`crate::ProposalStore::list_proposals`] uses for `limit`.
pub fn proposals_limit(limit: u32) -> u32 {
    match limit {
        0 => 50,
        n => n.min(PROPOSALS_MAX_LIMIT),
    }
}

/// The company alias a registry record's identifier becomes, from the registry's name and
/// the record id the enrichment job gives it (`companies/<MB>` at APR, `company/<number>` at
/// Companies House, `submissions/CIK<cik>` at EDGAR, `lei-records/<LEI>` at GLEIF). `None`
/// for anything else.
pub fn registry_record_alias(registry: &str, record: &str) -> Option<String> {
    let (prefix, value) = match registry {
        "apr" => ("id:rs_mb:", record.strip_prefix("companies/")?),
        "companies_house" => ("reg:companies_house:", record.strip_prefix("company/")?),
        "edgar" => ("reg:edgar:", record.strip_prefix("submissions/CIK")?),
        "gleif" => ("id:lei:", record.strip_prefix("lei-records/")?),
        _ => return None,
    };
    let ok =
        !value.is_empty() && value.len() <= 64 && value.bytes().all(|b| b.is_ascii_alphanumeric());
    ok.then(|| format!("{prefix}{value}"))
}

/// A proposal, as the enrichment job makes it.
#[derive(Clone, Debug, PartialEq)]
pub struct ProposalRequest {
    /// What it proposes.
    pub kind: ProposalKind,
    /// The company it is about (for a merge, the smaller id of the pair).
    pub company: Uuid,
    /// The registry record (`<registry>:<record>`) or the other company's id.
    pub candidate: String,
    /// What a person needs to decide, as a JSON object (a record's registry, id and name).
    pub detail: Value,
    /// Why: a JSON array of evidence objects.
    pub evidence: Value,
    /// The fused (noisy-OR) probability, from 0 to 1.
    pub probability: f32,
    /// Why it is for a person.
    pub reason: ProposalReason,
}

impl ProposalRequest {
    /// A merge proposal between `a` and `b`, named in id order.
    pub fn merge(
        a: Uuid,
        b: Uuid,
        detail: Value,
        evidence: Value,
        probability: f32,
        reason: ProposalReason,
    ) -> Self {
        Self {
            kind: ProposalKind::Merge,
            company: a.min(b),
            candidate: a.max(b).to_string(),
            detail,
            evidence,
            probability,
            reason,
        }
    }

    /// A registry record for `company` to adopt.
    pub fn registry_record(
        company: Uuid,
        registry: &str,
        record: &str,
        detail: Value,
        evidence: Value,
        probability: f32,
        reason: ProposalReason,
    ) -> Self {
        Self {
            kind: ProposalKind::RegistryRecord,
            company,
            candidate: format!("{registry}:{record}"),
            detail,
            evidence,
            probability,
            reason,
        }
    }

    /// Checks the request before it reaches a backend.
    pub fn validate(&self) -> Result<()> {
        if self.candidate.is_empty()
            || self.candidate.len() > CANDIDATE_MAX_LEN
            || self.candidate.contains('\0')
        {
            return Err(Error::Invalid(format!(
                "a candidate is 1 to {CANDIDATE_MAX_LEN} bytes, without NUL"
            )));
        }
        match self.kind {
            ProposalKind::Merge => match Uuid::parse_str(&self.candidate) {
                Ok(other) if other == self.company => {
                    return Err(Error::Invalid(
                        "a company cannot be merged into itself".to_owned(),
                    ));
                }
                Ok(other) if other < self.company || other.to_string() != self.candidate => {
                    return Err(Error::Invalid(
                        "a merge proposal names the pair in id order, the candidate as a \
                         lowercase id (ProposalRequest::merge)"
                            .to_owned(),
                    ));
                }
                Ok(_) => {}
                Err(_) => {
                    return Err(Error::Invalid(
                        "a merge proposal's candidate is a company id".to_owned(),
                    ));
                }
            },
            ProposalKind::RegistryRecord => {
                if !self
                    .candidate
                    .split_once(':')
                    .is_some_and(|(r, id)| !r.is_empty() && !id.is_empty())
                {
                    return Err(Error::Invalid(
                        "a registry record candidate is <registry>:<record>".to_owned(),
                    ));
                }
            }
        }
        if !self.detail.is_object() {
            return Err(Error::Invalid("detail must be a JSON object".to_owned()));
        }
        if self.detail.to_string().len() > EVIDENCE_MAX_BYTES {
            return Err(Error::Invalid(format!(
                "detail is at most {EVIDENCE_MAX_BYTES} bytes of JSON"
            )));
        }
        validate_evidence(&self.evidence)?;
        validate_unit("probability", self.probability)
    }
}

/// How a proposal was closed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Resolution {
    /// When, in Unix seconds.
    pub at: i64,
    /// Who; `None` for the dismissed pairs schema 5 moved out of props, closed by no one.
    pub actor: Option<MergeActor>,
    /// A rejection that is permanent (for a merge, the pair is dismissed too).
    pub dismissed: bool,
    /// The merge an accepted merge proposal made (undo it with `UndoMerge`).
    pub merge: Option<Uuid>,
    /// A person's note.
    pub note: String,
}

/// A stored proposal.
#[derive(Clone, Debug, PartialEq)]
pub struct Proposal {
    /// Its id.
    pub id: Uuid,
    /// Its place in the list: later proposals have larger numbers.
    pub seq: i64,
    /// What it proposes.
    pub kind: ProposalKind,
    /// The company (for a merge, the smaller id of the pair).
    pub company: Uuid,
    /// The registry record or the other company's id.
    pub candidate: String,
    /// What a person needs to decide (a JSON object).
    pub detail: Value,
    /// Why (a JSON array).
    pub evidence: Value,
    /// The fused probability.
    pub probability: f32,
    /// Why it is for a person.
    pub reason: ProposalReason,
    /// Where it stands.
    pub state: ProposalState,
    /// When it was first proposed, in Unix seconds.
    pub created_at: i64,
    /// When it was last proposed, in Unix seconds.
    pub updated_at: i64,
    /// Set once accepted or rejected.
    pub resolution: Option<Resolution>,
}

impl Proposal {
    /// A merge proposal's other company.
    pub fn candidate_company(&self) -> Option<Uuid> {
        match self.kind {
            ProposalKind::Merge => Uuid::parse_str(&self.candidate).ok(),
            ProposalKind::RegistryRecord => None,
        }
    }

    /// A registry record proposal's registry and record id.
    pub fn registry_record(&self) -> Option<(&str, &str)> {
        match self.kind {
            ProposalKind::RegistryRecord => self.candidate.split_once(':'),
            ProposalKind::Merge => None,
        }
    }
}

/// A page of proposals, newest first.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ProposalQuery {
    /// Only proposals in this state.
    pub state: Option<ProposalState>,
    /// Only proposals of this kind.
    pub kind: Option<ProposalKind>,
    /// Only proposals about this company: its own, and merge proposals naming it as the
    /// candidate.
    pub company: Option<Uuid>,
    /// Only proposals with a `seq` below this (the previous page's last).
    pub before: Option<i64>,
    /// Page size; 0 means 50, at most [`PROPOSALS_MAX_LIMIT`].
    pub limit: u32,
}

/// Closes an open proposal.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ResolveRequest {
    /// The proposal.
    pub id: Uuid,
    /// Accepted or rejected.
    pub state: ProposalState,
    /// Who.
    pub actor: MergeActor,
    /// A permanent rejection.
    pub dismissed: bool,
    /// The merge an accepted merge proposal made.
    pub merge: Option<Uuid>,
    /// Why.
    pub note: String,
}

impl ResolveRequest {
    /// Checks the request before it reaches a backend.
    pub fn validate(&self) -> Result<()> {
        if self.state == ProposalState::Open {
            return Err(Error::Invalid(
                "a proposal is resolved as accepted or rejected".to_owned(),
            ));
        }
        if self.dismissed && self.state != ProposalState::Rejected {
            return Err(Error::Invalid(
                "only a rejection can be a dismissal".to_owned(),
            ));
        }
        validate_reason(&self.note)
    }
}

/// Most `merged_into` hops followed.
const MAX_HOPS: usize = 64;

/// Most identifiers one company lists per kind in its `ids` prop (as the enrichment job).
const MAX_IDS: usize = 20;

/// What [`accept`] did.
#[derive(Clone, Debug, PartialEq)]
pub struct Accepted {
    /// The proposal, accepted.
    pub proposal: Proposal,
    /// The merge it made (a merge proposal whose companies were still two).
    pub merge: Option<MergeEntry>,
    /// The identifier alias the company adopted (a registry record proposal).
    pub alias: Option<String>,
}

/// The live company `id` stands for: itself, or where its merges led.
async fn live<B: Backend + ?Sized>(b: &B, mut id: Uuid) -> Result<Uuid> {
    for _ in 0..MAX_HOPS {
        let node = b
            .node(id)
            .await?
            .ok_or_else(|| Error::NotFound(format!("company {id} does not exist")))?;
        match merged_into(&node) {
            Some(next) => id = next,
            None => return Ok(id),
        }
    }
    Err(Error::Precondition(format!(
        "company {id} has more than {MAX_HOPS} merges in a chain"
    )))
}

/// An open proposal.
async fn open<B: Backend + ?Sized>(b: &B, id: Uuid) -> Result<Proposal> {
    let p = b
        .proposal(id)
        .await?
        .ok_or_else(|| Error::NotFound(format!("proposal {id} does not exist")))?;
    if p.state != ProposalState::Open {
        return Err(Error::Precondition(format!(
            "proposal {id} is {} already",
            p.state
        )));
    }
    Ok(p)
}

fn dismissed(d: &Dismissal) -> Error {
    Error::Precondition(format!(
        "companies {} and {} are dismissed as not the same; a dismissal is permanent",
        d.a, d.b
    ))
}

/// A person accepts an open proposal (ADR 0002 sections 1.6 and 1.8).
///
/// - **A merge:** the two companies (as they are now, following earlier merges) are merged
///   through the merge log ([`crate::MergeStore::merge_companies`]) with `actor`, the
///   proposal's evidence and probability, and confidence 1, so `UndoMerge` undoes it. The
///   survivor is `survivor` when given (one of the two), else the one with more documents
///   (the proposal's company on a tie). A pair that is one company already is accepted
///   without a merge.
/// - **A registry record:** the record's identifier (see [`registry_record_alias`]) becomes
///   an alias of the company, as when the job adopts a name match, and joins its `ids` or
///   `registry_ids` prop; the record's registry is made due now, so the next enrichment pass
///   looks the company up by that identifier and writes the record's facts and edges. An
///   identifier another company owns is refused: merge the two first.
///
/// Fails with [`Error::NotFound`] for an unknown proposal and [`Error::Precondition`] for
/// one that is not open, a dismissed pair, or an identifier that cannot be adopted.
#[allow(clippy::too_many_lines, clippy::many_single_char_names)]
pub async fn accept<B: Backend + ?Sized>(
    b: &B,
    id: Uuid,
    actor: &MergeActor,
    survivor: Option<Uuid>,
    note: &str,
) -> Result<Accepted> {
    validate_reason(note)?;
    let p = open(b, id).await?;
    let resolve = |merge: Option<Uuid>, note: String| ResolveRequest {
        id,
        state: ProposalState::Accepted,
        actor: actor.clone(),
        dismissed: false,
        merge,
        note,
    };
    match p.kind {
        ProposalKind::Merge => {
            let other = p
                .candidate_company()
                .ok_or_else(|| Error::Invalid(format!("proposal {id} names no company")))?;
            let (x, y) = (live(b, p.company).await?, live(b, other).await?);
            if x == y {
                let note = if note.is_empty() {
                    "already one company".to_owned()
                } else {
                    note.to_owned()
                };
                let proposal = b.resolve_proposal(&resolve(None, note)).await?;
                return Ok(Accepted {
                    proposal,
                    merge: None,
                    alias: None,
                });
            }
            if let Some(d) = b.dismissal(x, y).await? {
                return Err(dismissed(&d));
            }
            let (keep, gone) = if let Some(chosen) = survivor {
                let chosen = live(b, chosen).await?;
                if chosen == x {
                    (x, y)
                } else if chosen == y {
                    (y, x)
                } else {
                    return Err(Error::Invalid(format!(
                        "the survivor must be one of the proposal's companies, {x} or {y}"
                    )));
                }
            } else {
                let dx = b.company_documents(x).await?.len();
                let dy = b.company_documents(y).await?.len();
                if dy > dx { (y, x) } else { (x, y) }
            };
            let entry = b
                .merge_companies(&MergeRequest {
                    survivor: keep,
                    absorbed: gone,
                    actor: actor.clone(),
                    evidence: p.evidence.clone(),
                    confidence: 1.0,
                    probability: p.probability,
                })
                .await?;
            let proposal = b
                .resolve_proposal(&resolve(Some(entry.id), note.to_owned()))
                .await?;
            Ok(Accepted {
                proposal,
                merge: Some(entry),
                alias: None,
            })
        }
        ProposalKind::RegistryRecord => {
            let (registry, record) = p
                .registry_record()
                .ok_or_else(|| Error::Invalid(format!("proposal {id} names no record")))?;
            let alias = registry_record_alias(registry, record).ok_or_else(|| {
                Error::Precondition(format!(
                    "the {registry} record {record:?} has no identifier doewood can adopt"
                ))
            })?;
            let company = live(b, p.company).await?;
            if let Some(held) = b.alias(NodeKind::Company, &alias).await? {
                match held.node {
                    None => {
                        return Err(Error::Precondition(format!(
                            "the identifier {alias} is blocked (it belongs to more than one                              company)"
                        )));
                    }
                    Some(owner) => {
                        let owner = live(b, owner).await?;
                        if owner != company {
                            return Err(Error::Precondition(format!(
                                "the identifier {alias} belongs to company {owner}; merge the                                  two companies first"
                            )));
                        }
                    }
                }
            }
            let evidence = json!({
                "step": "registry",
                "confidence": p.probability,
                "registry": {"registry": registry, "record": record},
                "by_name": true,
                "proposal": id.to_string(),
                "accepted_by": actor.kind.as_str(),
            });
            let got = b
                .claim_alias(NodeKind::Company, &alias, company, &evidence)
                .await?;
            if got.node != Some(company) {
                return Err(Error::Precondition(format!(
                    "the identifier {alias} was claimed by another company meanwhile"
                )));
            }
            let mut patch = PropsPatch::new();
            if let Some(rest) = alias.strip_prefix("id:")
                && let Some((kind, value)) = rest.split_once(':')
            {
                patch = patch.add_to_set(&["ids", kind], json!(value), MAX_IDS);
            } else if let Some(rest) = alias.strip_prefix("reg:")
                && let Some((kind, value)) = rest.split_once(':')
            {
                patch = patch.set(&["registry_ids", kind], json!(value));
            }
            b.update_props(company, &patch).await?;
            b.record_enrichment(company, &[(registry.to_owned(), Duration::ZERO)])
                .await?;
            let proposal = b.resolve_proposal(&resolve(None, note.to_owned())).await?;
            Ok(Accepted {
                proposal,
                merge: None,
                alias: Some(alias),
            })
        }
    }
}

/// A person rejects an open proposal. With `dismiss`, the rejection is permanent: the same
/// proposal is never opened again, and for a merge the two companies (as they are now) are
/// recorded as a dismissed pair ([`crate::MergeStore::dismiss_pair`]), so they are never
/// merged either. Returns the proposal and the dismissal recorded.
///
/// Fails with [`Error::NotFound`] for an unknown proposal and [`Error::Precondition`] for
/// one that is not open, or a dismissal of two companies that are one now (undo that merge
/// with `dismiss` instead).
#[allow(clippy::many_single_char_names)]
pub async fn reject<B: Backend + ?Sized>(
    b: &B,
    id: Uuid,
    actor: &MergeActor,
    dismiss: bool,
    note: &str,
) -> Result<(Proposal, Option<Dismissal>)> {
    validate_reason(note)?;
    let p = open(b, id).await?;
    let mut dismissal = None;
    if dismiss && let Some(other) = p.candidate_company() {
        let (x, y) = (live(b, p.company).await?, live(b, other).await?);
        if x == y {
            return Err(Error::Precondition(format!(
                "companies {} and {other} are one company now; undo that merge with dismiss                  instead",
                p.company
            )));
        }
        dismissal = Some(
            b.dismiss_pair(&DismissRequest {
                a: x,
                b: y,
                actor: actor.clone(),
                reason: note.to_owned(),
            })
            .await?,
        );
    }
    let proposal = b
        .resolve_proposal(&ResolveRequest {
            id,
            state: ProposalState::Rejected,
            actor: actor.clone(),
            dismissed: dismiss,
            merge: None,
            note: note.to_owned(),
        })
        .await?;
    Ok((proposal, dismissal))
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn record_aliases() {
        assert_eq!(
            registry_record_alias("apr", "companies/12345678").as_deref(),
            Some("id:rs_mb:12345678")
        );
        assert_eq!(
            registry_record_alias("companies_house", "company/01234567").as_deref(),
            Some("reg:companies_house:01234567")
        );
        assert_eq!(
            registry_record_alias("edgar", "submissions/CIK0000320193").as_deref(),
            Some("reg:edgar:0000320193")
        );
        assert_eq!(
            registry_record_alias("gleif", "lei-records/5493001KJTIIGC8Y1R12").as_deref(),
            Some("id:lei:5493001KJTIIGC8Y1R12")
        );
        assert_eq!(registry_record_alias("wikidata", "Q1"), None);
        assert_eq!(registry_record_alias("apr", "companies/"), None);
        assert_eq!(registry_record_alias("apr", "companies/1/2"), None);
        assert_eq!(registry_record_alias("gleif", "x/1"), None);
    }

    #[test]
    fn requests_are_checked() {
        let (a, b) = (Uuid::from_u128(1), Uuid::from_u128(2));
        let m = ProposalRequest::merge(b, a, json!({}), json!([]), 0.6, ProposalReason::ReviewBand);
        assert_eq!(
            (m.company, m.candidate.as_str()),
            (a, b.to_string().as_str())
        );
        m.validate().unwrap();
        let mut bad = m.clone();
        bad.candidate = a.to_string();
        assert!(bad.validate().is_err(), "itself");
        bad.company = b;
        assert!(bad.validate().is_err(), "out of order");
        bad.candidate = "nope".to_owned();
        assert!(bad.validate().is_err(), "not an id");
        let mut bad = m.clone();
        bad.probability = 1.5;
        assert!(bad.validate().is_err());
        let mut bad = m.clone();
        bad.evidence = json!({});
        assert!(bad.validate().is_err());
        let mut bad = m;
        bad.detail = json!([]);
        assert!(bad.validate().is_err());
        let r = ProposalRequest::registry_record(
            a,
            "apr",
            "companies/12345678",
            json!({}),
            json!([]),
            0.7,
            ProposalReason::Conflict,
        );
        assert_eq!(r.candidate, "apr:companies/12345678");
        r.validate().unwrap();
        let mut bad = r;
        bad.candidate = "apr".to_owned();
        assert!(bad.validate().is_err());
    }
}
