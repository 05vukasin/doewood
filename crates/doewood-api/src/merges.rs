//! The merge-log half of `GraphService` (ADR 0002 section 1.8): list merges, a person's merge,
//! undo, split, dismiss and list dismissals. The rules live in `doewood_model::merge`; here
//! the inputs are checked and the results converted. A refusal the store explains (a later
//! merge, a dismissed pair, an entry already undone) is FAILED_PRECONDITION.

use doewood_model::{
    DismissRequest, Dismissal, DismissalQuery, EVIDENCE_MAX_BYTES, MERGES_MAX_LIMIT, MergeActor,
    MergeActorKind, MergeEntry, MergeOp, MergeQuery, MergeRequest, NODE_KEY_MAX_LEN,
    REASON_MAX_LEN, SPLIT_MAX_ITEMS, SplitRequest, UndoRequest, merges_limit,
};
use serde_json::{Value, json};
use tonic::Status;
use uuid::Uuid;

use crate::pb;
use crate::validate::{self, Violations};
use crate::{Api, error};

/// Longest name of a split-off company, in bytes.
const NAME_MAX_LEN: usize = 1000;

fn actor_kind(k: MergeActorKind) -> pb::MergeActorKind {
    match k {
        MergeActorKind::Ingest => pb::MergeActorKind::Ingest,
        MergeActorKind::Enrichment => pb::MergeActorKind::Enrichment,
        MergeActorKind::Manual => pb::MergeActorKind::Manual,
    }
}

pub(crate) fn actor(a: &MergeActor) -> pb::MergeActor {
    pb::MergeActor {
        kind: actor_kind(a.kind) as i32,
        instance_id: a.instance.as_str().to_owned(),
    }
}

pub(crate) fn entry(e: &MergeEntry) -> pb::MergeEntry {
    pb::MergeEntry {
        id: e.id.to_string(),
        seq: e.seq,
        op: match e.op {
            MergeOp::Merge => pb::MergeOp::Merge,
            MergeOp::Split => pb::MergeOp::Split,
        } as i32,
        at: e.at,
        actor: Some(actor(&e.actor)),
        survivor_id: e.survivor.to_string(),
        absorbed_id: e.absorbed.to_string(),
        evidence_json: e.evidence.to_string(),
        confidence: e.confidence,
        probability: e.probability,
        undo_json: e.undo.to_string(),
        undone_at: e.undone.as_ref().map_or(0, |u| u.at),
        undone_by: e.undone.as_ref().map(|u| actor(&u.actor)),
    }
}

pub(crate) fn dismissal(d: &Dismissal) -> pb::Dismissal {
    pb::Dismissal {
        company_a_id: d.a.to_string(),
        company_b_id: d.b.to_string(),
        at: d.at,
        actor: Some(actor(&d.actor)),
        reason: d.reason.clone(),
        merge_id: d.merge.map(|m| m.to_string()).unwrap_or_default(),
        seq: d.seq,
    }
}

/// An optional company filter.
pub(crate) fn company_filter(v: &mut Violations, field: &str, value: &str) -> Option<Uuid> {
    if value.is_empty() {
        None
    } else {
        validate::uuid(v, field, value)
    }
}

/// A page size and the token of the previous page (the last `seq` it held).
pub(crate) fn page(v: &mut Violations, size: u32, token: &str) -> (u32, Option<i64>) {
    if size > MERGES_MAX_LIMIT {
        v.add("page_size", format!("at most {MERGES_MAX_LIMIT}"));
    }
    let before = if token.is_empty() {
        None
    } else {
        let parsed = token.parse::<i64>().ok().filter(|n| *n > 0);
        if parsed.is_none() {
            v.add("page_token", "not a token from a previous page");
        }
        parsed
    };
    (merges_limit(size), before)
}

pub(crate) fn reason(v: &mut Violations, field: &str, value: &str) {
    if value.len() > REASON_MAX_LEN || value.contains('\0') {
        v.add(
            field,
            format!("at most {REASON_MAX_LEN} bytes, without NUL"),
        );
    }
}

fn evidence(v: &mut Violations, field: &str, value: &str) -> Value {
    if value.is_empty() {
        return json!([]);
    }
    if value.len() > EVIDENCE_MAX_BYTES {
        v.add(field, format!("at most {EVIDENCE_MAX_BYTES} bytes"));
        return json!([]);
    }
    if let Ok(a @ Value::Array(_)) = serde_json::from_str::<Value>(value) {
        a
    } else {
        v.add(field, "must be a JSON array");
        json!([])
    }
}

fn uuids(v: &mut Violations, field: &str, values: &[String]) -> Vec<Uuid> {
    if values.len() > SPLIT_MAX_ITEMS {
        v.add(field, format!("at most {SPLIT_MAX_ITEMS}"));
    }
    values
        .iter()
        .enumerate()
        .filter_map(|(i, s)| validate::uuid(v, &format!("{field}[{i}]"), s))
        .collect()
}

/// The token of the next page: the last `seq`, when the page is full.
pub(crate) fn next_token(len: usize, limit: u32, last: Option<i64>) -> String {
    match last {
        Some(seq) if len == limit as usize => seq.to_string(),
        _ => String::new(),
    }
}

impl Api {
    pub(crate) fn manual(&self) -> MergeActor {
        MergeActor {
            kind: MergeActorKind::Manual,
            instance: self.state.instance().clone(),
        }
    }

    pub(crate) async fn list_merges_rpc(
        &self,
        req: &pb::ListMergesRequest,
    ) -> Result<pb::ListMergesResponse, Status> {
        let mut v = Violations::default();
        let company = company_filter(&mut v, "company_id", &req.company_id);
        let (limit, before) = page(&mut v, req.page_size, &req.page_token);
        v.check()?;
        let found = self
            .state
            .backend()?
            .list_merges(&MergeQuery {
                company,
                before,
                limit,
            })
            .await
            .map_err(error::model)?;
        Ok(pb::ListMergesResponse {
            next_page_token: next_token(found.len(), limit, found.last().map(|e| e.seq)),
            merges: found.iter().map(entry).collect(),
        })
    }

    pub(crate) async fn merge_companies_rpc(
        &self,
        req: &pb::MergeCompaniesRequest,
    ) -> Result<pb::MergeEntry, Status> {
        let mut v = Violations::default();
        let survivor = validate::uuid(&mut v, "survivor_id", &req.survivor_id);
        let absorbed = validate::uuid(&mut v, "absorbed_id", &req.absorbed_id);
        if survivor.is_some() && survivor == absorbed {
            v.add("absorbed_id", "must differ from survivor_id");
        }
        let evidence = evidence(&mut v, "evidence_json", &req.evidence_json);
        v.check()?;
        let (Some(survivor), Some(absorbed)) = (survivor, absorbed) else {
            return Err(Status::invalid_argument(
                "survivor_id, absorbed_id: required",
            ));
        };
        let e = self
            .state
            .backend()?
            .merge_companies(&MergeRequest {
                survivor,
                absorbed,
                actor: self.manual(),
                evidence,
                confidence: 1.0,
                probability: 1.0,
            })
            .await
            .map_err(error::model)?;
        Ok(entry(&e))
    }

    pub(crate) async fn undo_merge_rpc(
        &self,
        req: &pb::UndoMergeRequest,
    ) -> Result<pb::UndoMergeResponse, Status> {
        let mut v = Violations::default();
        let id = validate::uuid(&mut v, "merge_id", &req.merge_id);
        reason(&mut v, "reason", &req.reason);
        v.check()?;
        let Some(id) = id else {
            return Err(Status::invalid_argument("merge_id: required"));
        };
        let out = self
            .state
            .backend()?
            .undo_merge(&UndoRequest {
                merge: id,
                actor: self.manual(),
                cascade: req.cascade,
                dismiss: req.dismiss,
                reason: req.reason.clone(),
            })
            .await
            .map_err(error::model)?;
        Ok(pb::UndoMergeResponse {
            undone_ids: out.undone.iter().map(Uuid::to_string).collect(),
            skipped: out.skipped,
            dismissal: out.dismissal.as_ref().map(dismissal),
        })
    }

    pub(crate) async fn split_company_rpc(
        &self,
        req: &pb::SplitCompanyRequest,
    ) -> Result<pb::MergeEntry, Status> {
        let mut v = Violations::default();
        let company = validate::uuid(&mut v, "company_id", &req.company_id);
        if req.name.trim().is_empty() || req.name.len() > NAME_MAX_LEN || req.name.contains('\0') {
            v.add(
                "name",
                format!("required, at most {NAME_MAX_LEN} bytes, without NUL"),
            );
        }
        if req.aliases.len() > SPLIT_MAX_ITEMS {
            v.add("aliases", format!("at most {SPLIT_MAX_ITEMS}"));
        }
        for (i, a) in req.aliases.iter().enumerate() {
            if a.is_empty() || a.len() > NODE_KEY_MAX_LEN || a.contains('\0') {
                v.add(
                    format!("aliases[{i}]"),
                    format!("1 to {NODE_KEY_MAX_LEN} bytes, without NUL"),
                );
            }
        }
        let documents = uuids(&mut v, "document_ids", &req.document_ids);
        let edges = uuids(&mut v, "edge_ids", &req.edge_ids);
        if req.aliases.is_empty() && req.document_ids.is_empty() && req.edge_ids.is_empty() {
            v.add(
                "aliases",
                "name at least one alias, document or edge to move",
            );
        }
        reason(&mut v, "reason", &req.reason);
        v.check()?;
        let Some(company) = company else {
            return Err(Status::invalid_argument("company_id: required"));
        };
        let evidence = if req.reason.is_empty() {
            json!([])
        } else {
            json!([{"source": "manual", "reason": req.reason}])
        };
        let e = self
            .state
            .backend()?
            .split_company(&SplitRequest {
                company,
                key: None,
                props: json!({"name": req.name.trim()}),
                aliases: req.aliases.clone(),
                documents,
                edges,
                actor: self.manual(),
                evidence,
                dismiss: req.dismiss,
            })
            .await
            .map_err(error::model)?;
        Ok(entry(&e))
    }

    pub(crate) async fn dismiss_pair_rpc(
        &self,
        req: &pb::DismissPairRequest,
    ) -> Result<pb::Dismissal, Status> {
        let mut v = Violations::default();
        let a = validate::uuid(&mut v, "company_a_id", &req.company_a_id);
        let b = validate::uuid(&mut v, "company_b_id", &req.company_b_id);
        if a.is_some() && a == b {
            v.add("company_b_id", "must differ from company_a_id");
        }
        reason(&mut v, "reason", &req.reason);
        v.check()?;
        let (Some(a), Some(b)) = (a, b) else {
            return Err(Status::invalid_argument(
                "company_a_id, company_b_id: required",
            ));
        };
        let d = self
            .state
            .backend()?
            .dismiss_pair(&DismissRequest {
                a,
                b,
                actor: self.manual(),
                reason: req.reason.clone(),
            })
            .await
            .map_err(error::model)?;
        Ok(dismissal(&d))
    }

    pub(crate) async fn list_dismissals_rpc(
        &self,
        req: &pb::ListDismissalsRequest,
    ) -> Result<pb::ListDismissalsResponse, Status> {
        let mut v = Violations::default();
        let company = company_filter(&mut v, "company_id", &req.company_id);
        let (limit, before) = page(&mut v, req.page_size, &req.page_token);
        v.check()?;
        let found = self
            .state
            .backend()?
            .list_dismissals(&DismissalQuery {
                company,
                before,
                limit,
            })
            .await
            .map_err(error::model)?;
        Ok(pb::ListDismissalsResponse {
            next_page_token: next_token(found.len(), limit, found.last().map(|d| d.seq)),
            dismissals: found.iter().map(dismissal).collect(),
        })
    }
}
