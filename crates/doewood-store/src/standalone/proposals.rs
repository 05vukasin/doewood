//! Enrichment proposals in standalone mode (schema 8). The rules are
//! `doewood_model::proposal`. The store has one connection, so a proposal's checks and its
//! write run together.

use async_trait::async_trait;
use doewood_model::{
    Error, MergeStore, NodeKind, Proposal, ProposalKind, ProposalQuery, ProposalRequest,
    ProposalState, ProposalStore, Resolution, ResolveRequest, Result, proposals_limit,
};
use rusqlite::{Connection, OptionalExtension as _, Row, params};
use uuid::Uuid;

use super::merges::actor;
use super::{Standalone, json, parse, sql, uuid};
use crate::store::now;

const COLUMNS: &str = "id, seq, kind, company_id, candidate, detail_json, evidence_json, \
     probability, reason, state, created_at, updated_at, resolved_at, resolved_actor, \
     resolved_instance, dismissed, merge_id, note";

/// A row as stored.
struct Stored {
    id: String,
    seq: i64,
    kind: String,
    company: String,
    candidate: String,
    detail: String,
    evidence: String,
    probability: f64,
    reason: String,
    state: String,
    created_at: i64,
    updated_at: i64,
    resolved_at: Option<i64>,
    resolved_actor: Option<String>,
    resolved_instance: Option<String>,
    dismissed: bool,
    merge: Option<String>,
    note: String,
}

fn stored(r: &Row<'_>) -> rusqlite::Result<Stored> {
    Ok(Stored {
        id: r.get(0)?,
        seq: r.get(1)?,
        kind: r.get(2)?,
        company: r.get(3)?,
        candidate: r.get(4)?,
        detail: r.get(5)?,
        evidence: r.get(6)?,
        probability: r.get(7)?,
        reason: r.get(8)?,
        state: r.get(9)?,
        created_at: r.get(10)?,
        updated_at: r.get(11)?,
        resolved_at: r.get(12)?,
        resolved_actor: r.get(13)?,
        resolved_instance: r.get(14)?,
        dismissed: r.get::<_, i64>(15)? != 0,
        merge: r.get(16)?,
        note: r.get(17)?,
    })
}

#[allow(clippy::cast_possible_truncation)]
fn proposal_from(s: Stored) -> Result<Proposal> {
    let resolution = match s.resolved_at {
        Some(at) => Some(Resolution {
            at,
            actor: match (&s.resolved_actor, &s.resolved_instance) {
                (Some(k), Some(i)) => Some(actor(k, i)?),
                _ => None,
            },
            dismissed: s.dismissed,
            merge: s.merge.as_deref().map(uuid).transpose()?,
            note: s.note,
        }),
        None => None,
    };
    Ok(Proposal {
        id: uuid(&s.id)?,
        seq: s.seq,
        kind: parse(&s.kind)?,
        company: uuid(&s.company)?,
        candidate: s.candidate,
        detail: json(&s.detail)?,
        evidence: json(&s.evidence)?,
        probability: s.probability as f32,
        reason: parse(&s.reason)?,
        state: parse(&s.state)?,
        created_at: s.created_at,
        updated_at: s.updated_at,
        resolution,
    })
}

fn by_id(conn: &Connection, id: &str) -> Result<Option<Proposal>> {
    conn.query_row(
        &format!("SELECT {COLUMNS} FROM company_proposals WHERE id = ?1"),
        params![id],
        stored,
    )
    .optional()
    .map_err(sql)?
    .map(proposal_from)
    .transpose()
}

/// Fails unless `id` is a company node.
fn company(conn: &Connection, id: Uuid) -> Result<()> {
    let kind: Option<String> = conn
        .query_row(
            "SELECT kind FROM nodes WHERE id = ?1",
            params![id.to_string()],
            |r| r.get(0),
        )
        .optional()
        .map_err(sql)?;
    match kind {
        Some(k) if k == NodeKind::Company.as_str() => Ok(()),
        Some(k) => Err(Error::Invalid(format!("node {id} is a {k}, not a company"))),
        None => Err(Error::NotFound(format!("company {id} does not exist"))),
    }
}

fn upsert(conn: &Connection, req: &ProposalRequest) -> Result<Option<Proposal>> {
    company(conn, req.company)?;
    let company_id = req.company.to_string();
    let kind = req.kind.as_str();
    let blocked = conn
        .query_row(
            "SELECT 1 FROM company_proposals WHERE kind = ?1 AND company_id = ?2 \
             AND candidate = ?3 AND state = 'rejected' AND dismissed = 1 LIMIT 1",
            params![kind, company_id, req.candidate],
            |_| Ok(()),
        )
        .optional()
        .map_err(sql)?;
    if blocked.is_some() {
        return Ok(None);
    }
    let at = now();
    conn.execute(
        "INSERT INTO company_proposals (id, kind, company_id, candidate, detail_json, \
         evidence_json, probability, reason, state, created_at, updated_at) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, 'open', ?9, ?9) \
         ON CONFLICT (kind, company_id, candidate) WHERE state = 'open' \
         DO UPDATE SET detail_json = excluded.detail_json, \
         evidence_json = excluded.evidence_json, probability = excluded.probability, \
         reason = excluded.reason, updated_at = excluded.updated_at",
        params![
            Uuid::now_v7().to_string(),
            kind,
            company_id,
            req.candidate,
            req.detail.to_string(),
            req.evidence.to_string(),
            f64::from(req.probability),
            req.reason.as_str(),
            at,
        ],
    )
    .map_err(sql)?;
    conn.query_row(
        &format!(
            "SELECT {COLUMNS} FROM company_proposals WHERE kind = ?1 AND company_id = ?2 \
             AND candidate = ?3 AND state = 'open'"
        ),
        params![kind, company_id, req.candidate],
        stored,
    )
    .map_err(sql)
    .and_then(proposal_from)
    .map(Some)
}

#[async_trait]
impl ProposalStore for Standalone {
    async fn propose(&self, req: &ProposalRequest) -> Result<Option<Proposal>> {
        req.validate()?;
        if req.kind == ProposalKind::Merge {
            let other = Uuid::parse_str(&req.candidate)
                .map_err(|e| Error::Invalid(format!("candidate: {e}")))?;
            let (a, b) = (req.company, other);
            self.run(move |c| company(c, a).and_then(|()| company(c, b)))
                .await?;
            if self.dismissal(a, b).await?.is_some() {
                return Ok(None);
            }
        }
        let req = req.clone();
        self.run(move |c| upsert(c, &req)).await
    }

    async fn proposal(&self, id: Uuid) -> Result<Option<Proposal>> {
        self.run(move |c| by_id(c, &id.to_string())).await
    }

    async fn list_proposals(&self, q: &ProposalQuery) -> Result<Vec<Proposal>> {
        let q = *q;
        self.run(move |c| {
            let mut stmt = c
                .prepare_cached(&format!(
                    "SELECT {COLUMNS} FROM company_proposals \
                     WHERE (?1 IS NULL OR state = ?1) AND (?2 IS NULL OR kind = ?2) \
                     AND (?3 IS NULL OR company_id = ?3 \
                          OR (kind = 'merge' AND candidate = ?3)) \
                     AND (?4 IS NULL OR seq < ?4) ORDER BY seq DESC LIMIT ?5"
                ))
                .map_err(sql)?;
            let rows = stmt
                .query_map(
                    params![
                        q.state.map(ProposalState::as_str),
                        q.kind.map(ProposalKind::as_str),
                        q.company.map(|c| c.to_string()),
                        q.before,
                        i64::from(proposals_limit(q.limit)),
                    ],
                    stored,
                )
                .map_err(sql)?
                .collect::<rusqlite::Result<Vec<_>>>()
                .map_err(sql)?;
            rows.into_iter().map(proposal_from).collect()
        })
        .await
    }

    async fn resolve_proposal(&self, req: &ResolveRequest) -> Result<Proposal> {
        req.validate()?;
        let req = req.clone();
        self.run(move |c| {
            let id = req.id.to_string();
            let n = c
                .execute(
                    "UPDATE company_proposals SET state = ?2, resolved_at = ?3, \
                     resolved_actor = ?4, resolved_instance = ?5, dismissed = ?6, \
                     merge_id = ?7, note = ?8 WHERE id = ?1 AND state = 'open'",
                    params![
                        id,
                        req.state.as_str(),
                        now(),
                        req.actor.kind.as_str(),
                        req.actor.instance.as_str(),
                        i64::from(req.dismissed),
                        req.merge.map(|m| m.to_string()),
                        req.note,
                    ],
                )
                .map_err(sql)?;
            match (n, by_id(c, &id)?) {
                (1, Some(p)) => Ok(p),
                (_, Some(p)) => Err(Error::Precondition(format!(
                    "proposal {} is {} already",
                    p.id, p.state
                ))),
                (_, None) => Err(Error::NotFound(format!("proposal {id} does not exist"))),
            }
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use doewood_core::MasterKey;
    use doewood_model::{ProposalReason, ProposalStore as _};
    use serde_json::json;

    use super::*;
    use crate::{STORE_FILE, Store};

    /// Schema 8 moves the props lists of schema 7 into the table, once: duplicates keep the
    /// latest entry, a dismissed pair is closed, malformed entries are left out, and the
    /// lists leave the props (the other props stay).
    #[tokio::test]
    async fn schema_8_moves_proposals_out_of_props() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join(STORE_FILE);
        let (a, b, c) = (
            Uuid::from_u128(0xa),
            Uuid::from_u128(0xb),
            Uuid::from_u128(0xc),
        );
        {
            let mut conn = Connection::open(&path).unwrap();
            assert_eq!(crate::migrate::migrate_to(&mut conn, 7).unwrap(), 7);
            let props_a = json!({
                "name": "Alfa d.o.o.",
                "kept": 1,
                "merge_proposals": [
                    {"company": b.to_string(), "probability": 0.7, "reason": "uncertain",
                     "evidence": [{"source": "name_ladder"}], "at": 1_700_000_000},
                    {"company": b.to_string(), "probability": 0.8, "reason": "conflict",
                     "evidence": [{"source": "registry_id"}], "at": 1_700_000_100},
                    {"company": c.to_string(), "probability": 0.95, "reason": "dismissed",
                     "dismissal": 1, "evidence": [], "at": 1_700_000_200},
                    {"company": "not-an-id", "probability": 0.6},
                    {"company": a.to_string(), "probability": 0.6},
                    {"company": Uuid::from_u128(0xdead).to_string(), "probability": 0.6},
                    "junk"
                ],
                "registry_proposals": [
                    {"registry": "apr", "record": "companies/12345678", "name": "Alfa DOO",
                     "probability": 0.75, "reason": "uncertain", "at": 1_700_000_300},
                    {"registry": "apr"}
                ]
            });
            let props_b = json!({
                "name": "Alfa Plus d.o.o.",
                "merge_proposals": [
                    {"company": a.to_string(), "probability": 0.6, "reason": "uncertain",
                     "at": 1_700_000_050}
                ],
                "registry_proposals": []
            });
            for (id, key, props) in [
                (a, "a", props_a),
                (b, "b", props_b),
                (c, "c", json!({"name": "Gama"})),
            ] {
                conn.execute(
                    "INSERT INTO nodes (id, kind, key, props_json, created_at, updated_at) \
                     VALUES (?1, 'company', ?2, ?3, 0, 0)",
                    params![id.to_string(), key, props.to_string()],
                )
                .unwrap();
            }
        }
        let store = Arc::new(Store::open(&path, &MasterKey::from_bytes([1; 32])).unwrap());
        let s = Standalone::new(Arc::clone(&store));
        let all = s.list_proposals(&ProposalQuery::default()).await.unwrap();
        assert_eq!(all.len(), 3, "{all:#?}");
        let pair = all
            .iter()
            .find(|p| p.kind == ProposalKind::Merge && p.state == ProposalState::Open)
            .expect("the open pair");
        assert_eq!(
            (pair.company, pair.candidate.as_str()),
            (a, b.to_string().as_str())
        );
        assert!((pair.probability - 0.8).abs() < 1e-6, "the latest entry");
        assert_eq!(pair.reason, ProposalReason::Conflict);
        assert_eq!(pair.evidence, json!([{"source": "registry_id"}]));
        assert_eq!(pair.created_at, 1_700_000_100);
        assert_eq!(pair.detail["from_props"], a.to_string());
        let closed = all
            .iter()
            .find(|p| p.state == ProposalState::Rejected)
            .expect("the dismissed pair");
        assert_eq!(
            (closed.company, closed.candidate.as_str()),
            (a, c.to_string().as_str())
        );
        assert_eq!(closed.reason, ProposalReason::Dismissed);
        let r = closed.resolution.clone().expect("closed");
        assert!(r.dismissed);
        assert_eq!((r.actor, r.at), (None, 1_700_000_200));
        let rec = all
            .iter()
            .find(|p| p.kind == ProposalKind::RegistryRecord)
            .expect("the record");
        assert_eq!(rec.company, a);
        assert_eq!(rec.registry_record(), Some(("apr", "companies/12345678")));
        assert_eq!(rec.detail["name"], "Alfa DOO");
        assert_eq!(rec.reason, ProposalReason::ReviewBand);
        assert_eq!(rec.state, ProposalState::Open);
        // The lists are gone; the rest of the props stays.
        let props = |id: Uuid| -> serde_json::Value {
            let text: String = store
                .conn()
                .query_row(
                    "SELECT props_json FROM nodes WHERE id = ?1",
                    params![id.to_string()],
                    |r| r.get(0),
                )
                .unwrap();
            serde_json::from_str(&text).unwrap()
        };
        assert_eq!(props(a), json!({"name": "Alfa d.o.o.", "kept": 1}));
        assert_eq!(props(b), json!({"name": "Alfa Plus d.o.o."}));
        assert_eq!(props(c), json!({"name": "Gama"}));
        // The dismissed pair stays closed: proposing it again opens nothing.
        let again =
            ProposalRequest::merge(c, a, json!({}), json!([]), 0.95, ProposalReason::ReviewBand);
        assert_eq!(s.propose(&again).await.unwrap(), None);
        // A re-proposal of the open pair updates it.
        let mut same = again.clone();
        same.candidate = b.to_string();
        let updated = s.propose(&same).await.unwrap().expect("open");
        assert_eq!(updated.id, pair.id);
        drop(s);
        // Opening again migrates nothing twice.
        drop(store);
        let store = Store::open(&path, &MasterKey::from_bytes([1; 32])).unwrap();
        let n: i64 = store
            .conn()
            .query_row("SELECT count(*) FROM company_proposals", [], |r| r.get(0))
            .unwrap();
        assert_eq!(n, 3);
    }
}
