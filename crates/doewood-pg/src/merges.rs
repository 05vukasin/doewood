//! Reversible company merges over Postgres (schema 4): the merge log and dismissals. The rules
//! are `doewood_model::merge`; this is its [`MergeTx`] over one transaction. Every write takes
//! one advisory lock, so merges, splits, undos and dismissals of all instances run one at a
//! time, and the nodes involved are locked `FOR UPDATE`, which also holds back new edges and
//! aliases that refer to them.

use async_trait::async_trait;
use doewood_core::InstanceId;
use doewood_model::merge::{self, MergeTx};
use doewood_model::{
    DismissRequest, Dismissal, DismissalQuery, Edge, EdgeState, MergeActor, MergeEntry, MergeQuery,
    MergeRequest, MergeStore, Node, NodeAlias, NodeKind, OwnedAlias, Result, SplitRequest,
    StoredEdge, UndoOutcome, UndoRequest, Undone, merges_limit,
};
use serde_json::Value;
use tokio_postgres::{GenericClient, Row};
use uuid::Uuid;

use crate::PgStore;
use crate::backend::{EDGE_COLUMNS, NODE_COLUMNS, bad, client, edge_at, node_at, parse, pg};

/// Serializes merge-log writes between instances ("dwmerge").
const LOCK: i64 = 0x0064_776d_6572_6765;

const ENTRY_COLUMNS: &str = "id, seq, op, extract(epoch FROM at)::bigint, actor, instance, \
     survivor_id, absorbed_id, evidence, confidence, probability, undo, \
     extract(epoch FROM undone_at)::bigint, undone_actor, undone_instance";

const DISMISSAL_COLUMNS: &str = "seq, company_a, company_b, extract(epoch FROM at)::bigint, \
     actor, instance, reason, merge_id";

fn actor(kind: &str, instance: &str) -> Result<MergeActor> {
    Ok(MergeActor {
        kind: parse(kind)?,
        instance: InstanceId::new(instance).map_err(|e| bad("instance", e))?,
    })
}

fn entry_at(r: &Row) -> Result<MergeEntry> {
    let undone_at: Option<i64> = r.try_get(12).map_err(pg)?;
    let undone_actor: Option<&str> = r.try_get(13).map_err(pg)?;
    let undone_instance: Option<&str> = r.try_get(14).map_err(pg)?;
    let undone = match (undone_at, undone_actor, undone_instance) {
        (Some(at), Some(k), Some(i)) => Some(Undone {
            at,
            actor: actor(k, i)?,
        }),
        _ => None,
    };
    Ok(MergeEntry {
        id: r.try_get(0).map_err(pg)?,
        seq: r.try_get(1).map_err(pg)?,
        op: parse(r.try_get::<_, &str>(2).map_err(pg)?)?,
        at: r.try_get(3).map_err(pg)?,
        actor: actor(r.try_get(4).map_err(pg)?, r.try_get(5).map_err(pg)?)?,
        survivor: r.try_get(6).map_err(pg)?,
        absorbed: r.try_get(7).map_err(pg)?,
        evidence: r.try_get(8).map_err(pg)?,
        confidence: r.try_get(9).map_err(pg)?,
        probability: r.try_get(10).map_err(pg)?,
        undo: r.try_get(11).map_err(pg)?,
        undone,
    })
}

fn dismissal_at(r: &Row) -> Result<Dismissal> {
    Ok(Dismissal {
        seq: r.try_get(0).map_err(pg)?,
        a: r.try_get(1).map_err(pg)?,
        b: r.try_get(2).map_err(pg)?,
        at: r.try_get(3).map_err(pg)?,
        actor: actor(r.try_get(4).map_err(pg)?, r.try_get(5).map_err(pg)?)?,
        reason: r.try_get(6).map_err(pg)?,
        merge: r.try_get(7).map_err(pg)?,
    })
}

pub(crate) async fn aliases_of(c: &impl GenericClient, node: Uuid) -> Result<Vec<OwnedAlias>> {
    let rows = c
        .query(
            "SELECT kind, alias, props FROM node_aliases WHERE node_id = $1 \
             ORDER BY kind COLLATE \"C\", alias COLLATE \"C\"",
            &[&node],
        )
        .await
        .map_err(pg)?;
    rows.iter()
        .map(|r| {
            Ok(OwnedAlias {
                kind: parse(r.try_get::<_, &str>(0).map_err(pg)?)?,
                alias: r.try_get(1).map_err(pg)?,
                props: r.try_get(2).map_err(pg)?,
            })
        })
        .collect()
}

/// [`MergeTx`] over one Postgres transaction.
struct Tx<'a>(deadpool_postgres::Transaction<'a>);

#[async_trait]
impl MergeTx for Tx<'_> {
    async fn lock_nodes(&mut self, ids: &[Uuid]) -> Result<()> {
        let mut ids = ids.to_vec();
        ids.sort_unstable();
        self.0
            .execute(
                "SELECT id FROM nodes WHERE id = ANY($1) ORDER BY id FOR UPDATE",
                &[&ids],
            )
            .await
            .map_err(pg)?;
        Ok(())
    }

    async fn node(&mut self, id: Uuid) -> Result<Option<Node>> {
        self.0
            .query_opt(
                &format!("SELECT {NODE_COLUMNS} FROM nodes n WHERE n.id = $1"),
                &[&id],
            )
            .await
            .map_err(pg)?
            .map(|r| node_at(&r, 0))
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
                "INSERT INTO nodes (id, kind, key, props) VALUES ($1, $2, $3, $4) \
                 ON CONFLICT ON CONSTRAINT nodes_kind_key DO NOTHING",
                &[&id, &kind.as_str(), &key, props],
            )
            .await
            .map_err(pg)?;
        Ok(n == 1)
    }

    async fn set_props(&mut self, id: Uuid, props: &Value) -> Result<()> {
        self.0
            .execute(
                "UPDATE nodes SET props = $2, updated_at = now() WHERE id = $1",
                &[&id, props],
            )
            .await
            .map_err(pg)?;
        Ok(())
    }

    async fn delete_node(&mut self, id: Uuid) -> Result<()> {
        self.0
            .execute("DELETE FROM nodes WHERE id = $1", &[&id])
            .await
            .map_err(pg)?;
        Ok(())
    }

    async fn edges_at(&mut self, node: Uuid) -> Result<Vec<StoredEdge>> {
        let rows = self
            .0
            .query(
                &format!(
                    "SELECT {EDGE_COLUMNS} FROM edges e WHERE e.src = $1 OR e.dst = $1 \
                     ORDER BY e.id"
                ),
                &[&node],
            )
            .await
            .map_err(pg)?;
        rows.iter().map(edge_at).collect()
    }

    async fn edge(&mut self, id: Uuid) -> Result<Option<StoredEdge>> {
        self.0
            .query_opt(
                &format!("SELECT {EDGE_COLUMNS} FROM edges e WHERE e.id = $1"),
                &[&id],
            )
            .await
            .map_err(pg)?
            .map(|r| edge_at(&r))
            .transpose()
    }

    async fn edge_with_identity(&mut self, e: &Edge) -> Result<Option<Uuid>> {
        self.0
            .query_opt(
                "SELECT id FROM edges WHERE src = $1 AND dst = $2 AND kind = $3 \
                 AND source = $4 AND valid_from IS NOT DISTINCT FROM $5",
                &[
                    &e.src,
                    &e.dst,
                    &e.kind.as_str(),
                    &e.source.as_str(),
                    &e.valid_from,
                ],
            )
            .await
            .map_err(pg)?
            .map(|r| r.try_get(0).map_err(pg))
            .transpose()
    }

    async fn repoint_edge(&mut self, id: Uuid, src: Uuid, dst: Uuid) -> Result<()> {
        self.0
            .execute(
                "UPDATE edges SET src = $2, dst = $3, updated_at = now() WHERE id = $1",
                &[&id, &src, &dst],
            )
            .await
            .map_err(pg)?;
        Ok(())
    }

    async fn set_edge_state(&mut self, id: Uuid, state: EdgeState) -> Result<()> {
        self.0
            .execute(
                "UPDATE edges SET state = $2, updated_at = now() WHERE id = $1",
                &[&id, &state.as_str()],
            )
            .await
            .map_err(pg)?;
        Ok(())
    }

    async fn aliases_of(&mut self, node: Uuid) -> Result<Vec<OwnedAlias>> {
        aliases_of(&*self.0, node).await
    }

    async fn alias(&mut self, kind: NodeKind, alias: &str) -> Result<Option<NodeAlias>> {
        self.0
            .query_opt(
                "SELECT node_id, props FROM node_aliases WHERE kind = $1 AND alias = $2",
                &[&kind.as_str(), &alias],
            )
            .await
            .map_err(pg)?
            .map(|r| {
                Ok(NodeAlias {
                    node: r.try_get(0).map_err(pg)?,
                    props: r.try_get(1).map_err(pg)?,
                })
            })
            .transpose()
    }

    async fn set_alias_owner(&mut self, kind: NodeKind, alias: &str, node: Uuid) -> Result<()> {
        self.0
            .execute(
                "UPDATE node_aliases SET node_id = $3, updated_at = now() \
                 WHERE kind = $1 AND alias = $2",
                &[&kind.as_str(), &alias, &node],
            )
            .await
            .map_err(pg)?;
        Ok(())
    }

    async fn insert_entry(&mut self, e: &MergeEntry) -> Result<i64> {
        #[allow(clippy::cast_precision_loss)]
        let at = e.at as f64;
        self.0
            .query_one(
                "INSERT INTO company_merges (id, op, at, actor, instance, survivor_id, \
                 absorbed_id, evidence, confidence, probability, undo) \
                 VALUES ($1, $2, to_timestamp($3), $4, $5, $6, $7, $8, $9, $10, $11) \
                 RETURNING seq",
                &[
                    &e.id,
                    &e.op.as_str(),
                    &at,
                    &e.actor.kind.as_str(),
                    &e.actor.instance.as_str(),
                    &e.survivor,
                    &e.absorbed,
                    &e.evidence,
                    &e.confidence,
                    &e.probability,
                    &e.undo,
                ],
            )
            .await
            .map_err(pg)?
            .try_get(0)
            .map_err(pg)
    }

    async fn entry(&mut self, id: Uuid) -> Result<Option<MergeEntry>> {
        self.0
            .query_opt(
                &format!("SELECT {ENTRY_COLUMNS} FROM company_merges WHERE id = $1"),
                &[&id],
            )
            .await
            .map_err(pg)?
            .map(|r| entry_at(&r))
            .transpose()
    }

    async fn entries(&mut self, q: &MergeQuery) -> Result<Vec<MergeEntry>> {
        let rows = self
            .0
            .query(
                &format!(
                    "SELECT {ENTRY_COLUMNS} FROM company_merges \
                     WHERE ($1::uuid IS NULL OR survivor_id = $1 OR absorbed_id = $1) \
                     AND ($2::bigint IS NULL OR seq < $2) ORDER BY seq DESC LIMIT $3"
                ),
                &[&q.company, &q.before, &i64::from(merges_limit(q.limit))],
            )
            .await
            .map_err(pg)?;
        rows.iter().map(entry_at).collect()
    }

    async fn live_entries_after(&mut self, seq: i64, nodes: &[Uuid]) -> Result<Vec<MergeEntry>> {
        let rows = self
            .0
            .query(
                &format!(
                    "SELECT {ENTRY_COLUMNS} FROM company_merges \
                     WHERE seq > $1 AND undone_at IS NULL \
                     AND (survivor_id = ANY($2) OR absorbed_id = ANY($2)) ORDER BY seq"
                ),
                &[&seq, &nodes],
            )
            .await
            .map_err(pg)?;
        rows.iter().map(entry_at).collect()
    }

    async fn absorbed_into(&mut self, survivors: &[Uuid]) -> Result<Vec<Uuid>> {
        let rows = self
            .0
            .query(
                "SELECT absorbed_id FROM company_merges WHERE op = 'merge' \
                 AND undone_at IS NULL AND survivor_id = ANY($1)",
                &[&survivors],
            )
            .await
            .map_err(pg)?;
        rows.iter().map(|r| r.try_get(0).map_err(pg)).collect()
    }

    async fn mark_undone(&mut self, id: Uuid, u: &Undone) -> Result<()> {
        #[allow(clippy::cast_precision_loss)]
        let at = u.at as f64;
        self.0
            .execute(
                "UPDATE company_merges SET undone_at = to_timestamp($2), undone_actor = $3, \
                 undone_instance = $4 WHERE id = $1",
                &[&id, &at, &u.actor.kind.as_str(), &u.actor.instance.as_str()],
            )
            .await
            .map_err(pg)?;
        Ok(())
    }

    async fn dismissal_among(&mut self, a: &[Uuid], b: &[Uuid]) -> Result<Option<Dismissal>> {
        self.0
            .query_opt(
                &format!(
                    "SELECT {DISMISSAL_COLUMNS} FROM company_dismissals \
                     WHERE (company_a = ANY($1) AND company_b = ANY($2)) \
                     OR (company_a = ANY($2) AND company_b = ANY($1)) ORDER BY seq LIMIT 1"
                ),
                &[&a, &b],
            )
            .await
            .map_err(pg)?
            .map(|r| dismissal_at(&r))
            .transpose()
    }

    async fn insert_dismissal(&mut self, d: &Dismissal) -> Result<Dismissal> {
        #[allow(clippy::cast_precision_loss)]
        let at = d.at as f64;
        self.0
            .execute(
                "INSERT INTO company_dismissals (company_a, company_b, at, actor, instance, \
                 reason, merge_id) VALUES ($1, $2, to_timestamp($3), $4, $5, $6, $7) \
                 ON CONFLICT (company_a, company_b) DO NOTHING",
                &[
                    &d.a,
                    &d.b,
                    &at,
                    &d.actor.kind.as_str(),
                    &d.actor.instance.as_str(),
                    &d.reason,
                    &d.merge,
                ],
            )
            .await
            .map_err(pg)?;
        let row = self
            .0
            .query_one(
                &format!(
                    "SELECT {DISMISSAL_COLUMNS} FROM company_dismissals \
                     WHERE company_a = $1 AND company_b = $2"
                ),
                &[&d.a, &d.b],
            )
            .await
            .map_err(pg)?;
        dismissal_at(&row)
    }

    async fn dismissals(&mut self, q: &DismissalQuery) -> Result<Vec<Dismissal>> {
        let rows = self
            .0
            .query(
                &format!(
                    "SELECT {DISMISSAL_COLUMNS} FROM company_dismissals \
                     WHERE ($1::uuid IS NULL OR company_a = $1 OR company_b = $1) \
                     AND ($2::bigint IS NULL OR seq < $2) ORDER BY seq DESC LIMIT $3"
                ),
                &[&q.company, &q.before, &i64::from(merges_limit(q.limit))],
            )
            .await
            .map_err(pg)?;
        rows.iter().map(dismissal_at).collect()
    }
}

/// Runs `f` in one transaction; `write` takes the merge-log lock first.
macro_rules! in_tx {
    ($store:expr, $write:expr, |$tx:ident| $body:expr) => {{
        let mut c = client(&$store.pool).await?;
        let t = c.transaction().await.map_err(pg)?;
        let mut $tx = Tx(t);
        if $write {
            $tx.0
                .execute("SELECT pg_advisory_xact_lock($1)", &[&LOCK])
                .await
                .map_err(pg)?;
        }
        let out = $body;
        $tx.0.commit().await.map_err(pg)?;
        Ok(out)
    }};
}

#[async_trait]
impl MergeStore for PgStore {
    async fn merge_companies(&self, req: &MergeRequest) -> Result<MergeEntry> {
        req.validate()?;
        in_tx!(self, true, |tx| merge::merge(&mut tx, req).await?)
    }

    async fn split_company(&self, req: &SplitRequest) -> Result<MergeEntry> {
        req.validate()?;
        in_tx!(self, true, |tx| merge::split(&mut tx, req).await?)
    }

    async fn undo_merge(&self, req: &UndoRequest) -> Result<UndoOutcome> {
        in_tx!(self, true, |tx| merge::undo(&mut tx, req).await?)
    }

    async fn merge_entry(&self, id: Uuid) -> Result<Option<MergeEntry>> {
        in_tx!(self, false, |tx| tx.entry(id).await?)
    }

    async fn list_merges(&self, query: &MergeQuery) -> Result<Vec<MergeEntry>> {
        in_tx!(self, false, |tx| tx.entries(query).await?)
    }

    async fn dismiss_pair(&self, req: &DismissRequest) -> Result<Dismissal> {
        in_tx!(self, true, |tx| merge::dismiss(&mut tx, req).await?)
    }

    async fn dismissal(&self, a: Uuid, b: Uuid) -> Result<Option<Dismissal>> {
        in_tx!(self, false, |tx| merge::find_dismissal(&mut tx, a, b)
            .await?)
    }

    async fn list_dismissals(&self, query: &DismissalQuery) -> Result<Vec<Dismissal>> {
        in_tx!(self, false, |tx| tx.dismissals(query).await?)
    }
}
