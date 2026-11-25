//! Reversible company merges in standalone mode (schema 7): the merge log and dismissals. The
//! rules are `doewood_model::merge`; this is its [`MergeTx`] over one SQLite transaction
//! (`BEGIN IMMEDIATE`). The store has one connection, so merge-log writes never race.
//!
//! The rules are async (Postgres awaits each statement); here every primitive is synchronous,
//! so their futures complete on the first poll and run inside the blocking task.

use std::future::Future;
use std::task::{Context, Poll, Waker};

use async_trait::async_trait;
use doewood_core::InstanceId;
use doewood_model::merge::{self, MergeTx};
use doewood_model::{
    DismissRequest, Dismissal, DismissalQuery, Edge, EdgeState, MergeActor, MergeEntry, MergeQuery,
    MergeRequest, MergeStore, Node, NodeAlias, NodeKind, OwnedAlias, Result, SplitRequest,
    StoredEdge, UndoOutcome, UndoRequest, Undone, merges_limit,
};
use rusqlite::types::Value as Sql;
use rusqlite::{Connection, OptionalExtension as _, Row, params, params_from_iter};
use serde_json::Value;
use uuid::Uuid;

use super::{
    EDGE_COLUMNS, NODE_COLUMNS, Standalone, backend, edge_at, edge_from, json, julian, node_at,
    node_from, parse, sql, uuid,
};
use crate::store::now;

const ENTRY_COLUMNS: &str = "id, seq, op, at, actor, instance, survivor_id, absorbed_id, \
     evidence_json, confidence, probability, undo_json, undone_at, undone_actor, undone_instance";

const DISMISSAL_COLUMNS: &str = "seq, company_a, company_b, at, actor, instance, reason, merge_id";

pub(super) fn actor(kind: &str, instance: &str) -> Result<MergeActor> {
    Ok(MergeActor {
        kind: parse(kind)?,
        instance: InstanceId::new(instance)
            .map_err(|e| backend(format!("stored instance: {e}")))?,
    })
}

type EntryRow = (
    String,
    i64,
    String,
    i64,
    String,
    String,
    String,
    String,
    String,
    f64,
    f64,
    String,
    (Option<i64>, Option<String>, Option<String>),
);

fn entry_row(r: &Row<'_>) -> rusqlite::Result<EntryRow> {
    Ok((
        r.get(0)?,
        r.get(1)?,
        r.get(2)?,
        r.get(3)?,
        r.get(4)?,
        r.get(5)?,
        r.get(6)?,
        r.get(7)?,
        r.get(8)?,
        r.get(9)?,
        r.get(10)?,
        r.get(11)?,
        (r.get(12)?, r.get(13)?, r.get(14)?),
    ))
}

#[allow(clippy::cast_possible_truncation)]
fn entry_from(row: EntryRow) -> Result<MergeEntry> {
    let (
        id,
        seq,
        op,
        at,
        kind,
        instance,
        survivor,
        absorbed,
        evidence,
        confidence,
        probability,
        undo,
        undone,
    ) = row;
    let undone = match undone {
        (Some(when), Some(by), Some(on)) => Some(Undone {
            at: when,
            actor: actor(&by, &on)?,
        }),
        _ => None,
    };
    Ok(MergeEntry {
        id: uuid(&id)?,
        seq,
        op: parse(&op)?,
        at,
        actor: actor(&kind, &instance)?,
        survivor: uuid(&survivor)?,
        absorbed: uuid(&absorbed)?,
        evidence: json(&evidence)?,
        confidence: confidence as f32,
        probability: probability as f32,
        undo: json(&undo)?,
        undone,
    })
}

type DismissalRow = (
    i64,
    String,
    String,
    i64,
    String,
    String,
    String,
    Option<String>,
);

fn dismissal_row(r: &Row<'_>) -> rusqlite::Result<DismissalRow> {
    Ok((
        r.get(0)?,
        r.get(1)?,
        r.get(2)?,
        r.get(3)?,
        r.get(4)?,
        r.get(5)?,
        r.get(6)?,
        r.get(7)?,
    ))
}

fn dismissal_from(r: DismissalRow) -> Result<Dismissal> {
    let (seq, a, b, at, kind, instance, reason, merge) = r;
    Ok(Dismissal {
        seq,
        a: uuid(&a)?,
        b: uuid(&b)?,
        at,
        actor: actor(&kind, &instance)?,
        reason,
        merge: merge.as_deref().map(uuid).transpose()?,
    })
}

fn marks(n: usize) -> String {
    vec!["?"; n].join(", ")
}

fn texts(ids: &[Uuid]) -> impl Iterator<Item = Sql> + '_ {
    ids.iter().map(|i| Sql::Text(i.to_string()))
}

pub(super) fn aliases_of(conn: &Connection, node: Uuid) -> Result<Vec<OwnedAlias>> {
    let mut stmt = conn
        .prepare_cached(
            "SELECT kind, alias, props_json FROM node_aliases WHERE node_id = ?1 \
             ORDER BY kind, alias",
        )
        .map_err(sql)?;
    let rows = stmt
        .query_map([node.to_string()], |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, String>(1)?,
                r.get::<_, String>(2)?,
            ))
        })
        .map_err(sql)?;
    rows.map(|row| {
        let (kind, alias, props) = row.map_err(sql)?;
        Ok(OwnedAlias {
            kind: parse(&kind)?,
            alias,
            props: json(&props)?,
        })
    })
    .collect()
}

/// [`MergeTx`] over the connection, inside `BEGIN IMMEDIATE`.
struct Tx<'a>(&'a mut Connection);

impl Tx<'_> {
    fn entries_where(&self, filter: &str, args: Vec<Sql>) -> Result<Vec<MergeEntry>> {
        let mut stmt = self
            .0
            .prepare(&format!(
                "SELECT {ENTRY_COLUMNS} FROM company_merges WHERE {filter}"
            ))
            .map_err(sql)?;
        let rows = stmt
            .query_map(params_from_iter(args), entry_row)
            .map_err(sql)?;
        rows.map(|r| entry_from(r.map_err(sql)?)).collect()
    }

    fn dismissals_where(&self, filter: &str, args: Vec<Sql>) -> Result<Vec<Dismissal>> {
        let mut stmt = self
            .0
            .prepare(&format!(
                "SELECT {DISMISSAL_COLUMNS} FROM company_dismissals WHERE {filter}"
            ))
            .map_err(sql)?;
        let rows = stmt
            .query_map(params_from_iter(args), dismissal_row)
            .map_err(sql)?;
        rows.map(|r| dismissal_from(r.map_err(sql)?)).collect()
    }
}

#[async_trait]
impl MergeTx for Tx<'_> {
    async fn lock_nodes(&mut self, _ids: &[Uuid]) -> Result<()> {
        Ok(()) // BEGIN IMMEDIATE holds the write lock already
    }

    async fn node(&mut self, id: Uuid) -> Result<Option<Node>> {
        self.0
            .query_row(
                &format!("SELECT {NODE_COLUMNS} FROM nodes n WHERE n.id = ?1"),
                [id.to_string()],
                |r| node_at(r, 0),
            )
            .optional()
            .map_err(sql)?
            .map(node_from)
            .transpose()
    }

    async fn insert_node(
        &mut self,
        id: Uuid,
        kind: NodeKind,
        key: &str,
        props: &Value,
    ) -> Result<bool> {
        let n = self
            .0
            .execute(
                "INSERT INTO nodes (id, kind, key, props_json, created_at, updated_at) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?5) ON CONFLICT (kind, key) DO NOTHING",
                params![id.to_string(), kind.as_str(), key, props.to_string(), now()],
            )
            .map_err(sql)?;
        Ok(n == 1)
    }

    async fn set_props(&mut self, id: Uuid, props: &Value) -> Result<()> {
        self.0
            .execute(
                "UPDATE nodes SET props_json = ?2, updated_at = ?3 WHERE id = ?1",
                params![id.to_string(), props.to_string(), now()],
            )
            .map_err(sql)?;
        Ok(())
    }

    async fn delete_node(&mut self, id: Uuid) -> Result<()> {
        self.0
            .execute("DELETE FROM nodes WHERE id = ?1", [id.to_string()])
            .map_err(sql)?;
        Ok(())
    }

    async fn edges_at(&mut self, node: Uuid) -> Result<Vec<StoredEdge>> {
        let mut stmt = self
            .0
            .prepare_cached(&format!(
                "SELECT {EDGE_COLUMNS} FROM edges e WHERE e.src = ?1 OR e.dst = ?1 ORDER BY e.id"
            ))
            .map_err(sql)?;
        let rows = stmt.query_map([node.to_string()], edge_at).map_err(sql)?;
        rows.map(|r| edge_from(r.map_err(sql)?)).collect()
    }

    async fn edge(&mut self, id: Uuid) -> Result<Option<StoredEdge>> {
        self.0
            .query_row(
                &format!("SELECT {EDGE_COLUMNS} FROM edges e WHERE e.id = ?1"),
                [id.to_string()],
                edge_at,
            )
            .optional()
            .map_err(sql)?
            .map(edge_from)
            .transpose()
    }

    async fn edge_with_identity(&mut self, e: &Edge) -> Result<Option<Uuid>> {
        let id: Option<String> = self
            .0
            .query_row(
                "SELECT id FROM edges WHERE src = ?1 AND dst = ?2 AND kind = ?3 \
                 AND source = ?4 AND valid_from IS ?5",
                params![
                    e.src.to_string(),
                    e.dst.to_string(),
                    e.kind.as_str(),
                    e.source.as_str(),
                    e.valid_from.map(julian)
                ],
                |r| r.get(0),
            )
            .optional()
            .map_err(sql)?;
        id.as_deref().map(uuid).transpose()
    }

    async fn repoint_edge(&mut self, id: Uuid, src: Uuid, dst: Uuid) -> Result<()> {
        self.0
            .execute(
                "UPDATE edges SET src = ?2, dst = ?3, updated_at = ?4 WHERE id = ?1",
                params![id.to_string(), src.to_string(), dst.to_string(), now()],
            )
            .map_err(sql)?;
        Ok(())
    }

    async fn set_edge_state(&mut self, id: Uuid, state: EdgeState) -> Result<()> {
        self.0
            .execute(
                "UPDATE edges SET state = ?2, updated_at = ?3 WHERE id = ?1",
                params![id.to_string(), state.as_str(), now()],
            )
            .map_err(sql)?;
        Ok(())
    }

    async fn aliases_of(&mut self, node: Uuid) -> Result<Vec<OwnedAlias>> {
        aliases_of(self.0, node)
    }

    async fn alias(&mut self, kind: NodeKind, alias: &str) -> Result<Option<NodeAlias>> {
        let row: Option<(Option<String>, String)> = self
            .0
            .query_row(
                "SELECT node_id, props_json FROM node_aliases WHERE kind = ?1 AND alias = ?2",
                params![kind.as_str(), alias],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .optional()
            .map_err(sql)?;
        row.map(|(node, props)| {
            Ok(NodeAlias {
                node: node.as_deref().map(uuid).transpose()?,
                props: json(&props)?,
            })
        })
        .transpose()
    }

    async fn set_alias_owner(&mut self, kind: NodeKind, alias: &str, node: Uuid) -> Result<()> {
        self.0
            .execute(
                "UPDATE node_aliases SET node_id = ?3, updated_at = ?4 \
                 WHERE kind = ?1 AND alias = ?2",
                params![kind.as_str(), alias, node.to_string(), now()],
            )
            .map_err(sql)?;
        Ok(())
    }

    async fn insert_entry(&mut self, e: &MergeEntry) -> Result<i64> {
        self.0
            .query_row(
                "INSERT INTO company_merges (id, op, at, actor, instance, survivor_id, \
                 absorbed_id, evidence_json, confidence, probability, undo_json) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11) RETURNING seq",
                params![
                    e.id.to_string(),
                    e.op.as_str(),
                    e.at,
                    e.actor.kind.as_str(),
                    e.actor.instance.as_str(),
                    e.survivor.to_string(),
                    e.absorbed.to_string(),
                    e.evidence.to_string(),
                    f64::from(e.confidence),
                    f64::from(e.probability),
                    e.undo.to_string()
                ],
                |r| r.get(0),
            )
            .map_err(sql)
    }

    async fn entry(&mut self, id: Uuid) -> Result<Option<MergeEntry>> {
        Ok(self
            .entries_where("id = ?", vec![Sql::Text(id.to_string())])?
            .pop())
    }

    async fn entries(&mut self, q: &MergeQuery) -> Result<Vec<MergeEntry>> {
        let mut filter = String::from("1 = 1");
        let mut args = Vec::new();
        if let Some(c) = q.company {
            filter.push_str(" AND (survivor_id = ? OR absorbed_id = ?)");
            args.extend([Sql::Text(c.to_string()), Sql::Text(c.to_string())]);
        }
        if let Some(before) = q.before {
            filter.push_str(" AND seq < ?");
            args.push(Sql::Integer(before));
        }
        filter.push_str(" ORDER BY seq DESC LIMIT ?");
        args.push(Sql::Integer(i64::from(merges_limit(q.limit))));
        self.entries_where(&filter, args)
    }

    async fn live_entries_after(&mut self, seq: i64, nodes: &[Uuid]) -> Result<Vec<MergeEntry>> {
        if nodes.is_empty() {
            return Ok(Vec::new());
        }
        let m = marks(nodes.len());
        let mut args = vec![Sql::Integer(seq)];
        args.extend(texts(nodes));
        args.extend(texts(nodes));
        self.entries_where(
            &format!(
                "seq > ? AND undone_at IS NULL \
                 AND (survivor_id IN ({m}) OR absorbed_id IN ({m})) ORDER BY seq"
            ),
            args,
        )
    }

    async fn absorbed_into(&mut self, survivors: &[Uuid]) -> Result<Vec<Uuid>> {
        if survivors.is_empty() {
            return Ok(Vec::new());
        }
        let mut stmt = self
            .0
            .prepare(&format!(
                "SELECT absorbed_id FROM company_merges WHERE op = 'merge' \
                 AND undone_at IS NULL AND survivor_id IN ({})",
                marks(survivors.len())
            ))
            .map_err(sql)?;
        let rows = stmt
            .query_map(params_from_iter(texts(survivors)), |r| {
                r.get::<_, String>(0)
            })
            .map_err(sql)?;
        rows.map(|r| uuid(&r.map_err(sql)?)).collect()
    }

    async fn mark_undone(&mut self, id: Uuid, u: &Undone) -> Result<()> {
        self.0
            .execute(
                "UPDATE company_merges SET undone_at = ?2, undone_actor = ?3, \
                 undone_instance = ?4 WHERE id = ?1",
                params![
                    id.to_string(),
                    u.at,
                    u.actor.kind.as_str(),
                    u.actor.instance.as_str()
                ],
            )
            .map_err(sql)?;
        Ok(())
    }

    async fn dismissal_among(&mut self, a: &[Uuid], b: &[Uuid]) -> Result<Option<Dismissal>> {
        if a.is_empty() || b.is_empty() {
            return Ok(None);
        }
        let (ma, mb) = (marks(a.len()), marks(b.len()));
        let mut args: Vec<Sql> = texts(a).chain(texts(b)).collect();
        args.extend(texts(b).chain(texts(a)));
        Ok(self
            .dismissals_where(
                &format!(
                    "(company_a IN ({ma}) AND company_b IN ({mb})) \
                     OR (company_a IN ({mb}) AND company_b IN ({ma})) ORDER BY seq LIMIT 1"
                ),
                args,
            )?
            .pop())
    }

    async fn insert_dismissal(&mut self, d: &Dismissal) -> Result<Dismissal> {
        self.0
            .execute(
                "INSERT INTO company_dismissals (company_a, company_b, at, actor, instance, \
                 reason, merge_id) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7) \
                 ON CONFLICT (company_a, company_b) DO NOTHING",
                params![
                    d.a.to_string(),
                    d.b.to_string(),
                    d.at,
                    d.actor.kind.as_str(),
                    d.actor.instance.as_str(),
                    d.reason,
                    d.merge.map(|m| m.to_string())
                ],
            )
            .map_err(sql)?;
        self.dismissals_where(
            "company_a = ? AND company_b = ?",
            vec![Sql::Text(d.a.to_string()), Sql::Text(d.b.to_string())],
        )?
        .pop()
        .ok_or_else(|| backend("the dismissal just written is missing".to_owned()))
    }

    async fn dismissals(&mut self, q: &DismissalQuery) -> Result<Vec<Dismissal>> {
        let mut filter = String::from("1 = 1");
        let mut args = Vec::new();
        if let Some(c) = q.company {
            filter.push_str(" AND (company_a = ? OR company_b = ?)");
            args.extend([Sql::Text(c.to_string()), Sql::Text(c.to_string())]);
        }
        if let Some(before) = q.before {
            filter.push_str(" AND seq < ?");
            args.push(Sql::Integer(before));
        }
        filter.push_str(" ORDER BY seq DESC LIMIT ?");
        args.push(Sql::Integer(i64::from(merges_limit(q.limit))));
        self.dismissals_where(&filter, args)
    }
}

/// Runs a future whose every step is synchronous: it is ready on the first poll.
fn ready<T>(fut: impl Future<Output = Result<T>>) -> Result<T> {
    let mut fut = std::pin::pin!(fut);
    match fut.as_mut().poll(&mut Context::from_waker(Waker::noop())) {
        Poll::Ready(r) => r,
        Poll::Pending => Err(backend("a merge step did not complete".to_owned())),
    }
}

/// Runs `f` in one `BEGIN IMMEDIATE` transaction: committed when it succeeds, rolled back
/// otherwise.
fn in_tx<T>(conn: &mut Connection, f: impl FnOnce(&mut Tx<'_>) -> Result<T>) -> Result<T> {
    conn.execute_batch("BEGIN IMMEDIATE").map_err(sql)?;
    let mut tx = Tx(conn);
    let out = f(&mut tx);
    let end = if out.is_ok() { "COMMIT" } else { "ROLLBACK" };
    match tx.0.execute_batch(end) {
        Ok(()) => out,
        Err(e) => {
            let _ = tx.0.execute_batch("ROLLBACK");
            out.and(Err(sql(e)))
        }
    }
}

#[async_trait]
impl MergeStore for Standalone {
    async fn merge_companies(&self, req: &MergeRequest) -> Result<MergeEntry> {
        req.validate()?;
        let req = req.clone();
        self.run(move |c| in_tx(c, |tx| ready(merge::merge(tx, &req))))
            .await
    }

    async fn split_company(&self, req: &SplitRequest) -> Result<MergeEntry> {
        req.validate()?;
        let req = req.clone();
        self.run(move |c| in_tx(c, |tx| ready(merge::split(tx, &req))))
            .await
    }

    async fn undo_merge(&self, req: &UndoRequest) -> Result<UndoOutcome> {
        let req = req.clone();
        self.run(move |c| in_tx(c, |tx| ready(merge::undo(tx, &req))))
            .await
    }

    async fn merge_entry(&self, id: Uuid) -> Result<Option<MergeEntry>> {
        self.run(move |c| in_tx(c, |tx| ready(tx.entry(id)))).await
    }

    async fn list_merges(&self, query: &MergeQuery) -> Result<Vec<MergeEntry>> {
        let q = *query;
        self.run(move |c| in_tx(c, |tx| ready(tx.entries(&q))))
            .await
    }

    async fn dismiss_pair(&self, req: &DismissRequest) -> Result<Dismissal> {
        let req = req.clone();
        self.run(move |c| in_tx(c, |tx| ready(merge::dismiss(tx, &req))))
            .await
    }

    async fn dismissal(&self, a: Uuid, b: Uuid) -> Result<Option<Dismissal>> {
        self.run(move |c| in_tx(c, |tx| ready(merge::find_dismissal(tx, a, b))))
            .await
    }

    async fn list_dismissals(&self, query: &DismissalQuery) -> Result<Vec<Dismissal>> {
        let q = *query;
        self.run(move |c| in_tx(c, |tx| ready(tx.dismissals(&q))))
            .await
    }
}
