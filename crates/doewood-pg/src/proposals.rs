//! Enrichment proposals over Postgres (schema 5). The rules are `doewood_model::proposal`.

use async_trait::async_trait;
use doewood_model::{
    Error, MergeStore, NodeKind, Proposal, ProposalKind, ProposalQuery, ProposalRequest,
    ProposalState, ProposalStore, Resolution, ResolveRequest, Result, proposals_limit,
};
use tokio_postgres::{GenericClient, Row};
use uuid::Uuid;

use crate::PgStore;
use crate::backend::{client, parse, pg};
use crate::merges::actor;

const COLUMNS: &str = "id, seq, kind, company_id, candidate, detail, evidence, probability, \
     reason, state, extract(epoch FROM created_at)::bigint, \
     extract(epoch FROM updated_at)::bigint, extract(epoch FROM resolved_at)::bigint, \
     resolved_actor, resolved_instance, dismissed, merge_id, note";

fn proposal_at(r: &Row) -> Result<Proposal> {
    let state: ProposalState = parse(r.try_get::<_, &str>(9).map_err(pg)?)?;
    let resolved_at: Option<i64> = r.try_get(12).map_err(pg)?;
    let who: Option<&str> = r.try_get(13).map_err(pg)?;
    let instance: Option<&str> = r.try_get(14).map_err(pg)?;
    let resolution = match resolved_at {
        Some(at) => Some(Resolution {
            at,
            actor: match (who, instance) {
                (Some(k), Some(i)) => Some(actor(k, i)?),
                _ => None,
            },
            dismissed: r.try_get(15).map_err(pg)?,
            merge: r.try_get(16).map_err(pg)?,
            note: r.try_get(17).map_err(pg)?,
        }),
        None => None,
    };
    Ok(Proposal {
        id: r.try_get(0).map_err(pg)?,
        seq: r.try_get(1).map_err(pg)?,
        kind: parse(r.try_get::<_, &str>(2).map_err(pg)?)?,
        company: r.try_get(3).map_err(pg)?,
        candidate: r.try_get(4).map_err(pg)?,
        detail: r.try_get(5).map_err(pg)?,
        evidence: r.try_get(6).map_err(pg)?,
        probability: r.try_get(7).map_err(pg)?,
        reason: parse(r.try_get::<_, &str>(8).map_err(pg)?)?,
        state,
        created_at: r.try_get(10).map_err(pg)?,
        updated_at: r.try_get(11).map_err(pg)?,
        resolution,
    })
}

async fn by_id(c: &impl GenericClient, id: Uuid) -> Result<Option<Proposal>> {
    c.query_opt(
        &format!("SELECT {COLUMNS} FROM company_proposals WHERE id = $1"),
        &[&id],
    )
    .await
    .map_err(pg)?
    .map(|r| proposal_at(&r))
    .transpose()
}

/// Fails unless `id` is a company node.
async fn company(c: &impl GenericClient, id: Uuid) -> Result<()> {
    let kind: Option<String> = c
        .query_opt("SELECT kind FROM nodes WHERE id = $1", &[&id])
        .await
        .map_err(pg)?
        .map(|r| r.try_get(0))
        .transpose()
        .map_err(pg)?;
    match kind {
        Some(k) if k == NodeKind::Company.as_str() => Ok(()),
        Some(k) => Err(Error::Invalid(format!("node {id} is a {k}, not a company"))),
        None => Err(Error::NotFound(format!("company {id} does not exist"))),
    }
}

#[async_trait]
impl ProposalStore for PgStore {
    async fn propose(&self, req: &ProposalRequest) -> Result<Option<Proposal>> {
        req.validate()?;
        let c = client(&self.pool).await?;
        company(&**c, req.company).await?;
        if req.kind == ProposalKind::Merge {
            let other = Uuid::parse_str(&req.candidate)
                .map_err(|e| Error::Invalid(format!("candidate: {e}")))?;
            company(&**c, other).await?;
            if self.dismissal(req.company, other).await?.is_some() {
                return Ok(None);
            }
        }
        let blocked = c
            .query_opt(
                "SELECT 1 FROM company_proposals WHERE kind = $1 AND company_id = $2 \
                 AND candidate = $3 AND state = 'rejected' AND dismissed LIMIT 1",
                &[&req.kind.as_str(), &req.company, &req.candidate],
            )
            .await
            .map_err(pg)?;
        if blocked.is_some() {
            return Ok(None);
        }
        let row = c
            .query_one(
                &format!(
                    "INSERT INTO company_proposals (id, kind, company_id, candidate, detail, \
                     evidence, probability, reason, state, created_at, updated_at) \
                     VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 'open', now(), now()) \
                     ON CONFLICT (kind, company_id, candidate) WHERE state = 'open' \
                     DO UPDATE SET detail = EXCLUDED.detail, evidence = EXCLUDED.evidence, \
                     probability = EXCLUDED.probability, reason = EXCLUDED.reason, \
                     updated_at = now() RETURNING {COLUMNS}"
                ),
                &[
                    &Uuid::now_v7(),
                    &req.kind.as_str(),
                    &req.company,
                    &req.candidate,
                    &req.detail,
                    &req.evidence,
                    &req.probability,
                    &req.reason.as_str(),
                ],
            )
            .await
            .map_err(pg)?;
        proposal_at(&row).map(Some)
    }

    async fn proposal(&self, id: Uuid) -> Result<Option<Proposal>> {
        let c = client(&self.pool).await?;
        by_id(&**c, id).await
    }

    async fn list_proposals(&self, q: &ProposalQuery) -> Result<Vec<Proposal>> {
        let c = client(&self.pool).await?;
        let rows = c
            .query(
                &format!(
                    "SELECT {COLUMNS} FROM company_proposals \
                     WHERE ($1::text IS NULL OR state = $1) \
                     AND ($2::text IS NULL OR kind = $2) \
                     AND ($3::uuid IS NULL OR company_id = $3 \
                          OR (kind = 'merge' AND candidate = $3::text)) \
                     AND ($4::bigint IS NULL OR seq < $4) ORDER BY seq DESC LIMIT $5"
                ),
                &[
                    &q.state.map(ProposalState::as_str),
                    &q.kind.map(ProposalKind::as_str),
                    &q.company,
                    &q.before,
                    &i64::from(proposals_limit(q.limit)),
                ],
            )
            .await
            .map_err(pg)?;
        rows.iter().map(proposal_at).collect()
    }

    async fn resolve_proposal(&self, req: &ResolveRequest) -> Result<Proposal> {
        req.validate()?;
        let c = client(&self.pool).await?;
        let row = c
            .query_opt(
                &format!(
                    "UPDATE company_proposals SET state = $2, resolved_at = now(), \
                     resolved_actor = $3, resolved_instance = $4, dismissed = $5, \
                     merge_id = $6, note = $7 WHERE id = $1 AND state = 'open' \
                     RETURNING {COLUMNS}"
                ),
                &[
                    &req.id,
                    &req.state.as_str(),
                    &req.actor.kind.as_str(),
                    &req.actor.instance.as_str(),
                    &req.dismissed,
                    &req.merge,
                    &req.note,
                ],
            )
            .await
            .map_err(pg)?;
        if let Some(r) = row {
            return proposal_at(&r);
        }
        match by_id(&**c, req.id).await? {
            Some(p) => Err(Error::Precondition(format!(
                "proposal {} is {} already",
                p.id, p.state
            ))),
            None => Err(Error::NotFound(format!(
                "proposal {} does not exist",
                req.id
            ))),
        }
    }
}
