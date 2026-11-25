//! The proposals half of `GraphService` (ADR 0002 section 1.6, issue #52): list, accept and
//! reject what the enrichment gate left to a person. The rules live in
//! `doewood_model::proposal`; here the inputs are checked and the results converted. A refusal
//! the rules explain (not open, a dismissed pair, an identifier another company owns) is
//! FAILED_PRECONDITION.

use doewood_model::proposal::{accept, reject};
use doewood_model::{Proposal, ProposalKind, ProposalQuery, ProposalReason, ProposalState};
use tonic::Status;

use crate::merges::{actor, company_filter, dismissal, entry, next_token, page, reason};
use crate::pb;
use crate::validate::{self, Violations};
use crate::{Api, error};

fn kind(k: ProposalKind) -> pb::ProposalKind {
    match k {
        ProposalKind::RegistryRecord => pb::ProposalKind::RegistryRecord,
        ProposalKind::Merge => pb::ProposalKind::Merge,
    }
}

fn state(s: ProposalState) -> pb::ProposalState {
    match s {
        ProposalState::Open => pb::ProposalState::Open,
        ProposalState::Accepted => pb::ProposalState::Accepted,
        ProposalState::Rejected => pb::ProposalState::Rejected,
    }
}

fn proposal(p: &Proposal) -> pb::Proposal {
    let r = p.resolution.as_ref();
    pb::Proposal {
        id: p.id.to_string(),
        seq: p.seq,
        kind: kind(p.kind) as i32,
        company_id: p.company.to_string(),
        candidate: p.candidate.clone(),
        detail_json: p.detail.to_string(),
        evidence_json: p.evidence.to_string(),
        probability: p.probability,
        reason: match p.reason {
            ProposalReason::ReviewBand => pb::ProposalReason::ReviewBand,
            ProposalReason::Conflict => pb::ProposalReason::Conflict,
            ProposalReason::Dismissed => pb::ProposalReason::Dismissed,
        } as i32,
        state: state(p.state) as i32,
        created_at: p.created_at,
        updated_at: p.updated_at,
        resolved_at: r.map_or(0, |r| r.at),
        resolved_by: r.and_then(|r| r.actor.as_ref()).map(actor),
        dismissed: r.is_some_and(|r| r.dismissed),
        merge_id: r
            .and_then(|r| r.merge)
            .map(|m| m.to_string())
            .unwrap_or_default(),
        note: r.map(|r| r.note.clone()).unwrap_or_default(),
    }
}

impl Api {
    pub(crate) async fn list_proposals_rpc(
        &self,
        req: &pb::ListProposalsRequest,
    ) -> Result<pb::ListProposalsResponse, Status> {
        let mut v = Violations::default();
        let company = company_filter(&mut v, "company_id", &req.company_id);
        let (limit, before) = page(&mut v, req.page_size, &req.page_token);
        let state = match pb::ProposalState::try_from(req.state) {
            Ok(pb::ProposalState::Unspecified) => None,
            Ok(pb::ProposalState::Open) => Some(ProposalState::Open),
            Ok(pb::ProposalState::Accepted) => Some(ProposalState::Accepted),
            Ok(pb::ProposalState::Rejected) => Some(ProposalState::Rejected),
            Err(_) => {
                v.add("state", "not a proposal state");
                None
            }
        };
        let kind = match pb::ProposalKind::try_from(req.kind) {
            Ok(pb::ProposalKind::Unspecified) => None,
            Ok(pb::ProposalKind::RegistryRecord) => Some(ProposalKind::RegistryRecord),
            Ok(pb::ProposalKind::Merge) => Some(ProposalKind::Merge),
            Err(_) => {
                v.add("kind", "not a proposal kind");
                None
            }
        };
        v.check()?;
        let found = self
            .state
            .backend()?
            .list_proposals(&ProposalQuery {
                state,
                kind,
                company,
                before,
                limit,
            })
            .await
            .map_err(error::model)?;
        Ok(pb::ListProposalsResponse {
            next_page_token: next_token(found.len(), limit, found.last().map(|p| p.seq)),
            proposals: found.iter().map(proposal).collect(),
        })
    }

    pub(crate) async fn accept_proposal_rpc(
        &self,
        req: &pb::AcceptProposalRequest,
    ) -> Result<pb::AcceptProposalResponse, Status> {
        let mut v = Violations::default();
        let id = validate::uuid(&mut v, "proposal_id", &req.proposal_id);
        let survivor = company_filter(&mut v, "survivor_id", &req.survivor_id);
        reason(&mut v, "note", &req.note);
        v.check()?;
        let Some(id) = id else {
            return Err(Status::invalid_argument("proposal_id: required"));
        };
        let backend = self.state.backend()?;
        let done = accept(backend.as_ref(), id, &self.manual(), survivor, &req.note)
            .await
            .map_err(error::model)?;
        Ok(pb::AcceptProposalResponse {
            proposal: Some(proposal(&done.proposal)),
            merge: done.merge.as_ref().map(entry),
            alias: done.alias.unwrap_or_default(),
        })
    }

    pub(crate) async fn reject_proposal_rpc(
        &self,
        req: &pb::RejectProposalRequest,
    ) -> Result<pb::RejectProposalResponse, Status> {
        let mut v = Violations::default();
        let id = validate::uuid(&mut v, "proposal_id", &req.proposal_id);
        reason(&mut v, "note", &req.note);
        v.check()?;
        let Some(id) = id else {
            return Err(Status::invalid_argument("proposal_id: required"));
        };
        let backend = self.state.backend()?;
        let (p, d) = reject(backend.as_ref(), id, &self.manual(), req.dismiss, &req.note)
            .await
            .map_err(error::model)?;
        Ok(pb::RejectProposalResponse {
            proposal: Some(proposal(&p)),
            dismissal: d.as_ref().map(dismissal),
        })
    }
}
