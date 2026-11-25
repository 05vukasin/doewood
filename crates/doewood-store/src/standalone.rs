//! Standalone mode (ADR 0001, section 2): the store traits of `doewood-model` over the
//! instance's own SQLite file. Search uses FTS5; claims and leases are local to the instance.
//!
//! Every call runs on tokio's blocking pool and takes the store's single connection.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use doewood_core::InstanceId;
use doewood_model::{
    Backend, Blake3, ClaimStore, Direction, Edge, EdgeKind, EdgeSource, Error, GraphStore,
    LiveEdge, Located, LocationEnded, MERGE_CHAIN_MAX, Neighbour, NeighbourQuery, Node, NodeAlias,
    NodeKind, ObjectLocation, OwnedAlias, PropsPatch, PutOutcome, Record, RecordStore, Result,
    SKETCH_MAX_BANDS, SearchDoc, SearchHit, SearchQuery, SearchStore, Sketch, SketchStore,
    SourceClaim, SourceKey, StepVersions, StoredEdge, band_to_i64, merged_into, records_limit,
    sketch_limit, validate_alias, validate_hashes, validate_node, validate_sketch,
    versions_from_json, versions_to_json,
};
use rusqlite::types::Value as Sql;
use rusqlite::{
    Connection, OptionalExtension as _, Row, TransactionBehavior, params, params_from_iter,
};
use serde_json::Value;
use time::Date;
use uuid::Uuid;

use crate::Store;
use crate::store::now;
use enrichment::DOCUMENTS;

mod enrichment;
mod feeds;
mod merges;
mod proposals;
#[cfg(test)]
mod read_paths;

/// The candidate query for `bands` band keys, then the excluded hash and the limit. As on
/// Postgres (issue #74): the bands are counted and limited on `sketch_bands` alone (its
/// primary key), and only the survivors' signatures are read.
fn sketch_candidates_sql(bands: usize) -> String {
    let marks = vec!["?"; bands].join(", ");
    format!(
        "SELECT c.blake3, s.signature, c.shared \
         FROM (SELECT blake3, count(*) AS shared FROM sketch_bands \
         WHERE band IN ({marks}) AND blake3 <> ? \
         GROUP BY blake3 ORDER BY shared DESC, blake3 LIMIT ?) c \
         JOIN sketches s ON s.blake3 = c.blake3 ORDER BY c.shared DESC, c.blake3"
    )
}

/// [`GraphStore::company_documents`], the first `limit` of them when given.
fn company_documents(conn: &Connection, company: Uuid, limit: Option<u32>) -> Result<Vec<Node>> {
    let mut stmt = conn
        .prepare_cached(&format!(
            "SELECT DISTINCT {NODE_COLUMNS} FROM edges e JOIN nodes n ON n.id = e.src \
             WHERE e.dst = ?1 AND e.kind = 'party' AND n.kind = 'document' \
             AND e.state NOT IN ('retracted', 'superseded') ORDER BY n.key LIMIT ?2"
        ))
        .map_err(sql)?;
    // A negative limit is no limit.
    let limit = limit.map_or(-1, i64::from);
    let rows = stmt
        .query_map(params![company.to_string(), limit], |r| node_at(r, 0))
        .map_err(sql)?;
    rows.map(|row| node_from(row.map_err(sql)?)).collect()
}

/// The SQLite backend. Cheap to clone: every clone shares the one store.
#[derive(Clone, Debug)]
pub struct Standalone {
    store: Arc<Store>,
}

impl Standalone {
    /// Serves the traits from this store.
    pub fn new(store: Arc<Store>) -> Self {
        Self { store }
    }

    async fn run<T, F>(&self, f: F) -> Result<T>
    where
        T: Send + 'static,
        F: FnOnce(&mut Connection) -> Result<T> + Send + 'static,
    {
        let store = Arc::clone(&self.store);
        tokio::task::spawn_blocking(move || f(&mut store.conn()))
            .await
            .map_err(|e| backend(format!("blocking task failed: {e}")))?
    }
}

fn backend(message: String) -> Error {
    Error::Backend {
        backend: "sqlite",
        message,
    }
}

// By value, so it reads `.map_err(sql)`.
#[allow(clippy::needless_pass_by_value)]
fn sql(e: rusqlite::Error) -> Error {
    if let rusqlite::Error::SqliteFailure(f, _) = &e
        && f.extended_code == rusqlite::ffi::SQLITE_CONSTRAINT_FOREIGNKEY
    {
        return Error::NotFound("a node or record the write refers to does not exist".to_owned());
    }
    backend(e.to_string())
}

fn json(text: &str) -> Result<Value> {
    serde_json::from_str(text).map_err(|e| backend(format!("stored JSON is invalid: {e}")))
}

fn uuid(text: &str) -> Result<Uuid> {
    Uuid::parse_str(text).map_err(|e| backend(format!("stored id is invalid: {e}")))
}

fn julian(d: Date) -> i64 {
    i64::from(d.to_julian_day())
}

fn from_julian(day: Option<i64>) -> Result<Option<Date>> {
    day.map(|d| {
        i32::try_from(d)
            .ok()
            .and_then(|d| Date::from_julian_day(d).ok())
            .ok_or_else(|| backend(format!("stored date {d} is out of range")))
    })
    .transpose()
}

fn parse<T: std::str::FromStr<Err = Error>>(text: &str) -> Result<T> {
    text.parse()
        .map_err(|e: Error| backend(format!("stored value: {e}")))
}

const NODE_COLUMNS: &str = "n.id, n.kind, n.key, n.props_json";

fn node_at(row: &Row<'_>, at: usize) -> rusqlite::Result<(String, String, String, String)> {
    Ok((
        row.get(at)?,
        row.get(at + 1)?,
        row.get(at + 2)?,
        row.get(at + 3)?,
    ))
}

fn node_from((id, kind, key, props): (String, String, String, String)) -> Result<Node> {
    Ok(Node {
        id: uuid(&id)?,
        kind: parse(&kind)?,
        key,
        props: json(&props)?,
    })
}

const EDGE_COLUMNS: &str = "e.id, e.src, e.dst, e.kind, e.props_json, e.evidence_json, \
     e.confidence, e.state, e.valid_from, e.valid_to, e.source";

type EdgeRow = (
    String,
    String,
    String,
    String,
    String,
    String,
    f64,
    String,
    Option<i64>,
    Option<i64>,
    String,
);

fn edge_at(row: &Row<'_>) -> rusqlite::Result<EdgeRow> {
    Ok((
        row.get(0)?,
        row.get(1)?,
        row.get(2)?,
        row.get(3)?,
        row.get(4)?,
        row.get(5)?,
        row.get(6)?,
        row.get(7)?,
        row.get(8)?,
        row.get(9)?,
        row.get(10)?,
    ))
}

#[allow(clippy::cast_possible_truncation)]
fn edge_from(r: EdgeRow) -> Result<StoredEdge> {
    let (id, src, dst, kind, props, evidence, confidence, state, from, to, source) = r;
    Ok(StoredEdge {
        id: uuid(&id)?,
        edge: Edge {
            src: uuid(&src)?,
            dst: uuid(&dst)?,
            kind: parse(&kind)?,
            props: json(&props)?,
            evidence: json(&evidence)?,
            confidence: confidence as f32,
            state: parse(&state)?,
            valid_from: from_julian(from)?,
            valid_to: from_julian(to)?,
            source: parse(&source)?,
        },
    })
}

fn upsert_node(conn: &Connection, kind: NodeKind, key: &str, props: &Value) -> Result<Uuid> {
    let id: String = conn
        .query_row(
            "INSERT INTO nodes (id, kind, key, props_json, created_at, updated_at) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?5) \
             ON CONFLICT (kind, key) DO UPDATE SET props_json = excluded.props_json, \
             updated_at = excluded.updated_at \
             RETURNING id",
            params![
                Uuid::now_v7().to_string(),
                kind.as_str(),
                key,
                props.to_string(),
                now()
            ],
            |r| r.get(0),
        )
        .map_err(sql)?;
    uuid(&id)
}

/// Inserts the node if it is missing and leaves an existing one untouched.
fn ensure_node(conn: &Connection, kind: NodeKind, key: &str) -> Result<Uuid> {
    conn.execute(
        "INSERT INTO nodes (id, kind, key, created_at, updated_at) VALUES (?1, ?2, ?3, ?4, ?4) \
         ON CONFLICT (kind, key) DO NOTHING",
        params![Uuid::now_v7().to_string(), kind.as_str(), key, now()],
    )
    .map_err(sql)?;
    let id: String = conn
        .query_row(
            "SELECT id FROM nodes WHERE kind = ?1 AND key = ?2",
            params![kind.as_str(), key],
            |r| r.get(0),
        )
        .map_err(sql)?;
    uuid(&id)
}

fn upsert_edge(conn: &Connection, e: &Edge) -> Result<Uuid> {
    e.validate()?;
    let id: String = conn
        .query_row(
            "INSERT INTO edges (id, src, dst, kind, props_json, evidence_json, confidence, state, \
             valid_from, valid_to, source, created_at, updated_at) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?12) \
             ON CONFLICT (src, dst, kind, source, ifnull(valid_from, -2147483648)) \
             DO UPDATE SET \
             props_json = excluded.props_json, evidence_json = excluded.evidence_json, \
             confidence = excluded.confidence, state = excluded.state, \
             valid_to = excluded.valid_to, updated_at = excluded.updated_at \
             RETURNING id",
            params![
                Uuid::now_v7().to_string(),
                e.src.to_string(),
                e.dst.to_string(),
                e.kind.as_str(),
                e.props.to_string(),
                e.evidence.to_string(),
                f64::from(e.confidence),
                e.state.as_str(),
                e.valid_from.map(julian),
                e.valid_to.map(julian),
                e.source.as_str(),
                now()
            ],
            |r| r.get(0),
        )
        .map_err(sql)?;
    uuid(&id)
}

/// [`GraphStore::update_props`]; with `live`, a husk is refused
/// ([`GraphStore::update_live_props`]) in the same write transaction.
fn update_props(
    conn: &mut Connection,
    id: Uuid,
    patch: &PropsPatch,
    live: bool,
) -> Result<Option<Node>> {
    // The write lock from the start: the read and the write are one step.
    let tx = conn
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(sql)?;
    let Some(row) = tx
        .query_row(
            &format!("SELECT {NODE_COLUMNS} FROM nodes n WHERE n.id = ?1"),
            [id.to_string()],
            |r| node_at(r, 0),
        )
        .optional()
        .map_err(sql)?
    else {
        return Ok(None);
    };
    let mut node = node_from(row)?;
    if live && let Some(into) = merged_into(&node) {
        return Err(Error::MergedAway { company: id, into });
    }
    let before = std::mem::take(&mut node.props);
    let mut props = match &before {
        Value::Object(m) => m.clone(),
        _ => serde_json::Map::new(),
    };
    patch.apply(&mut props);
    node.props = Value::Object(props);
    if node.props != before {
        tx.execute(
            "UPDATE nodes SET props_json = ?2, updated_at = ?3 WHERE id = ?1",
            params![id.to_string(), node.props.to_string(), now()],
        )
        .map_err(sql)?;
    }
    tx.commit().map_err(sql)?;
    Ok(Some(node))
}

/// Refuses a husk with [`Error::MergedAway`] (issue #62). `false` when there is no node `id`.
/// Called inside a write transaction, so no merge commits before the caller's write.
pub(crate) fn check_live(conn: &Connection, id: Uuid) -> Result<bool> {
    let row: Option<Option<String>> = conn
        .query_row(
            "SELECT json_extract(props_json, '$.merged_into') FROM nodes WHERE id = ?1",
            [id.to_string()],
            |r| r.get(0),
        )
        .optional()
        .map_err(sql)?;
    match row {
        None => Ok(false),
        Some(into) => match into.and_then(|s| Uuid::parse_str(&s).ok()) {
            Some(into) => Err(Error::MergedAway { company: id, into }),
            None => Ok(true),
        },
    }
}

/// Follows `merged_into` from `id`, counting the hops in `hops` (at most
/// [`MERGE_CHAIN_MAX`]). A missing node is returned as it is: the edge write then fails.
fn follow_merges(conn: &Connection, mut id: Uuid, hops: &mut usize) -> Result<Uuid> {
    loop {
        let next: Option<String> = conn
            .query_row(
                "SELECT json_extract(props_json, '$.merged_into') FROM nodes WHERE id = ?1",
                [id.to_string()],
                |r| r.get(0),
            )
            .optional()
            .map_err(sql)?
            .flatten();
        match next.and_then(|s| Uuid::parse_str(&s).ok()) {
            Some(next) => {
                *hops += 1;
                if *hops > MERGE_CHAIN_MAX {
                    return Err(Error::Precondition(format!(
                        "node {id} has more than {MERGE_CHAIN_MAX} merges in a chain"
                    )));
                }
                id = next;
            }
            None => return Ok(id),
        }
    }
}

fn claim_row(key: &SourceKey, owner: &str, claimed_at: i64) -> Result<SourceClaim> {
    Ok(SourceClaim {
        key: key.clone(),
        owner: InstanceId::new(owner).map_err(|e| backend(format!("stored owner: {e}")))?,
        claimed_at,
    })
}

fn source_owner(conn: &Connection, key: &SourceKey) -> Result<Option<SourceClaim>> {
    conn.query_row(
        "SELECT owner, claimed_at FROM source_claims \
         WHERE endpoint = ?1 AND bucket = ?2 AND prefix = ?3",
        params![key.endpoint(), key.bucket(), key.prefix()],
        |r| Ok((r.get::<_, String>(0)?, r.get::<_, i64>(1)?)),
    )
    .optional()
    .map_err(sql)?
    .map(|(owner, at)| claim_row(key, &owner, at))
    .transpose()
}

#[async_trait]
impl RecordStore for Standalone {
    async fn put_record(
        &self,
        blake3: &Blake3,
        body: &Value,
        versions: &StepVersions,
    ) -> Result<PutOutcome> {
        let (h, body, versions) = (*blake3, body.clone(), versions_to_json(versions));
        self.run(move |conn| {
            let tx = conn.transaction().map_err(sql)?;
            let old: Option<(String, String)> = tx
                .query_row(
                    "SELECT body_json, versions_json FROM records WHERE blake3 = ?1",
                    [h.as_bytes().as_slice()],
                    |r| Ok((r.get(0)?, r.get(1)?)),
                )
                .optional()
                .map_err(sql)?;
            let outcome = match old {
                Some((b, v)) if json(&b)? == body && json(&v)? == versions => {
                    return Ok(PutOutcome::Unchanged);
                }
                Some(_) => PutOutcome::Updated,
                None => PutOutcome::Inserted,
            };
            tx.execute(
                "INSERT INTO records (blake3, body_json, versions_json, updated_at) \
                 VALUES (?1, ?2, ?3, ?4) \
                 ON CONFLICT (blake3) DO UPDATE SET body_json = excluded.body_json, \
                 versions_json = excluded.versions_json, updated_at = excluded.updated_at",
                params![
                    h.as_bytes().as_slice(),
                    body.to_string(),
                    versions.to_string(),
                    now()
                ],
            )
            .map_err(sql)?;
            tx.commit().map_err(sql)?;
            Ok(outcome)
        })
        .await
    }

    async fn get_record(&self, blake3: &Blake3) -> Result<Option<Record>> {
        let h = *blake3;
        self.run(move |conn| {
            let row: Option<(String, String, i64)> = conn
                .query_row(
                    "SELECT body_json, versions_json, updated_at FROM records WHERE blake3 = ?1",
                    [h.as_bytes().as_slice()],
                    |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
                )
                .optional()
                .map_err(sql)?;
            row.map(|(body, versions, updated_at)| {
                Ok(Record {
                    blake3: h,
                    body: json(&body)?,
                    versions: versions_from_json(&json(&versions)?)?,
                    updated_at,
                })
            })
            .transpose()
        })
        .await
    }

    async fn record_versions(&self, hashes: &[Blake3]) -> Result<BTreeMap<Blake3, StepVersions>> {
        validate_hashes(hashes)?;
        if hashes.is_empty() {
            return Ok(BTreeMap::new());
        }
        let marks = vec!["?"; hashes.len()].join(", ");
        let args: Vec<Vec<u8>> = hashes.iter().map(|h| h.as_bytes().to_vec()).collect();
        self.run(move |conn| {
            let mut stmt = conn
                .prepare(&format!(
                    "SELECT blake3, versions_json FROM records WHERE blake3 IN ({marks})"
                ))
                .map_err(sql)?;
            let rows = stmt
                .query_map(params_from_iter(args), |r| {
                    Ok((r.get::<_, Vec<u8>>(0)?, r.get::<_, String>(1)?))
                })
                .map_err(sql)?;
            rows.map(|row| {
                let (h, versions) = row.map_err(sql)?;
                Ok((
                    Blake3::from_slice(&h)?,
                    versions_from_json(&json(&versions)?)?,
                ))
            })
            .collect()
        })
        .await
    }

    async fn fact_texts(&self, hashes: &[Blake3], key: &str) -> Result<Vec<(Blake3, String)>> {
        validate_hashes(hashes)?;
        if hashes.is_empty() {
            return Ok(Vec::new());
        }
        let marks = vec!["?"; hashes.len()].join(", ");
        let mut args: Vec<Sql> = vec![Sql::Text(key.to_owned())];
        args.extend(hashes.iter().map(|h| Sql::Blob(h.as_bytes().to_vec())));
        self.run(move |conn| {
            // As on Postgres: a `facts` that is not an array has no facts; a fact that is not
            // an object, or whose key or `value.v` is not a string, is left out. The nested
            // CASE keeps `json_type` off elements that are not JSON objects.
            let mut stmt = conn
                .prepare(&format!(
                    "SELECT blake3, v FROM (SELECT r.blake3, CASE WHEN f.type = 'object' THEN \
                     (CASE WHEN json_type(f.value, '$.key') = 'text' \
                     AND json_extract(f.value, '$.key') = ?1 \
                     AND json_type(f.value, '$.value.v') = 'text' \
                     THEN json_extract(f.value, '$.value.v') END) END AS v \
                     FROM records r, json_each(CASE WHEN json_type(r.body_json, '$.facts') = \
                     'array' THEN json_extract(r.body_json, '$.facts') ELSE '[]' END) f \
                     WHERE r.blake3 IN ({marks})) WHERE v IS NOT NULL"
                ))
                .map_err(sql)?;
            let rows = stmt
                .query_map(params_from_iter(args), |r| {
                    Ok((r.get::<_, Vec<u8>>(0)?, r.get::<_, String>(1)?))
                })
                .map_err(sql)?;
            rows.map(|row| {
                let (h, v) = row.map_err(sql)?;
                Ok((Blake3::from_slice(&h)?, v))
            })
            .collect()
        })
        .await
    }

    async fn list_records(
        &self,
        source: &SourceKey,
        after: Option<&Blake3>,
        limit: u32,
    ) -> Result<Vec<Record>> {
        let (source, after) = (source.clone(), after.copied());
        let limit = i64::from(records_limit(limit));
        self.run(move |conn| {
            // `> coalesce(?4, x'')` rather than `?4 IS NULL OR ...`: the OR kept SQLite from
            // starting the scan at the page token, so every page read the records before it
            // (issue #13). Every hash is 32 bytes, so all are greater than the empty blob.
            let mut stmt = conn
                .prepare_cached(
                    "SELECT r.blake3, r.body_json, r.versions_json, r.updated_at FROM records r \
                     WHERE r.blake3 > coalesce(?4, x'') \
                     AND EXISTS (SELECT 1 FROM locations l WHERE l.blake3 = r.blake3 \
                     AND l.endpoint = ?1 AND l.bucket = ?2 \
                     AND substr(l.object_key, 1, length(?3)) = ?3) \
                     ORDER BY r.blake3 LIMIT ?5",
                )
                .map_err(sql)?;
            let rows = stmt
                .query_map(
                    params![
                        source.endpoint(),
                        source.bucket(),
                        source.prefix(),
                        after.as_ref().map(|h| h.as_bytes().as_slice()),
                        limit
                    ],
                    |r| {
                        Ok((
                            r.get::<_, Vec<u8>>(0)?,
                            r.get::<_, String>(1)?,
                            r.get::<_, String>(2)?,
                            r.get::<_, i64>(3)?,
                        ))
                    },
                )
                .map_err(sql)?;
            rows.map(|row| {
                let (h, body, versions, updated_at) = row.map_err(sql)?;
                Ok(Record {
                    blake3: Blake3::from_slice(&h)?,
                    body: json(&body)?,
                    versions: versions_from_json(&json(&versions)?)?,
                    updated_at,
                })
            })
            .collect()
        })
        .await
    }
}

#[async_trait]
impl GraphStore for Standalone {
    async fn upsert_node(&self, kind: NodeKind, key: &str, props: &Value) -> Result<Uuid> {
        validate_node(kind, key, props)?;
        let (key, props) = (key.to_owned(), props.clone());
        self.run(move |conn| upsert_node(conn, kind, &key, &props))
            .await
    }

    async fn ensure_node(&self, kind: NodeKind, key: &str) -> Result<Uuid> {
        validate_node(kind, key, &Value::Object(serde_json::Map::new()))?;
        let key = key.to_owned();
        self.run(move |conn| ensure_node(conn, kind, &key)).await
    }

    async fn update_props(&self, id: Uuid, patch: &PropsPatch) -> Result<Option<Node>> {
        patch.validate()?;
        let patch = patch.clone();
        self.run(move |conn| update_props(conn, id, &patch, false))
            .await
    }

    async fn update_live_props(&self, id: Uuid, patch: &PropsPatch) -> Result<Option<Node>> {
        patch.validate()?;
        let patch = patch.clone();
        self.run(move |conn| update_props(conn, id, &patch, true))
            .await
    }

    async fn node(&self, id: Uuid) -> Result<Option<Node>> {
        self.run(move |conn| {
            conn.query_row(
                &format!("SELECT {NODE_COLUMNS} FROM nodes n WHERE n.id = ?1"),
                [id.to_string()],
                |r| node_at(r, 0),
            )
            .optional()
            .map_err(sql)?
            .map(node_from)
            .transpose()
        })
        .await
    }

    async fn node_by_key(&self, kind: NodeKind, key: &str) -> Result<Option<Node>> {
        let key = key.to_owned();
        self.run(move |conn| {
            conn.query_row(
                &format!("SELECT {NODE_COLUMNS} FROM nodes n WHERE n.kind = ?1 AND n.key = ?2"),
                params![kind.as_str(), key],
                |r| node_at(r, 0),
            )
            .optional()
            .map_err(sql)?
            .map(node_from)
            .transpose()
        })
        .await
    }

    async fn upsert_edge(&self, edge: &Edge) -> Result<Uuid> {
        edge.validate()?;
        let edge = edge.clone();
        self.run(move |conn| upsert_edge(conn, &edge)).await
    }

    async fn upsert_live_edge(&self, edge: &Edge) -> Result<LiveEdge> {
        edge.validate()?;
        let mut e = edge.clone();
        self.run(move |conn| {
            // The write lock from the start: a merge runs wholly before or after this.
            let tx = conn
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(sql)?;
            let (mut src_hops, mut dst_hops) = (0, 0);
            e.src = follow_merges(&tx, e.src, &mut src_hops)?;
            e.dst = follow_merges(&tx, e.dst, &mut dst_hops)?;
            let id = upsert_edge(&tx, &e)?;
            tx.commit().map_err(sql)?;
            Ok(LiveEdge {
                id,
                src: e.src,
                dst: e.dst,
                redirects: u32::try_from(src_hops + dst_hops).unwrap_or(u32::MAX),
            })
        })
        .await
    }

    async fn neighbours(&self, node: Uuid, q: &NeighbourQuery) -> Result<Vec<Neighbour>> {
        let q = q.clone();
        self.run(move |conn| {
            let me = node.to_string();
            let (at_node, other) = match q.direction {
                Direction::Out => ("e.src = ?1", "e.dst"),
                Direction::In => ("e.dst = ?1", "e.src"),
                Direction::Both => (
                    "(e.src = ?1 OR e.dst = ?1)",
                    "(CASE WHEN e.src = ?1 THEN e.dst ELSE e.src END)",
                ),
            };
            let mut args = vec![Sql::Text(me)];
            let mut sql_text = format!(
                "SELECT {EDGE_COLUMNS}, {NODE_COLUMNS} FROM edges e \
                 JOIN nodes n ON n.id = {other} WHERE {at_node}"
            );
            if !q.edge_kinds.is_empty() {
                let marks = vec!["?"; q.edge_kinds.len()].join(", ");
                sql_text.push_str(" AND e.kind IN (");
                sql_text.push_str(&marks);
                sql_text.push(')');
                args.extend(
                    q.edge_kinds
                        .iter()
                        .map(|k| Sql::Text(k.as_str().to_owned())),
                );
            }
            if let Some(kind) = q.node_kind {
                sql_text.push_str(" AND n.kind = ?");
                args.push(Sql::Text(kind.as_str().to_owned()));
            }
            if let Some(at) = q.at {
                sql_text.push_str(
                    " AND (e.valid_from IS NULL OR e.valid_from <= ?) \
                     AND (e.valid_to IS NULL OR e.valid_to > ?)",
                );
                args.extend([Sql::Integer(julian(at)), Sql::Integer(julian(at))]);
            }
            if !q.include_inactive {
                sql_text.push_str(" AND e.state NOT IN ('retracted', 'superseded')");
            }
            sql_text.push_str(" ORDER BY e.kind, e.valid_from IS NOT NULL, e.valid_from, e.id");
            let mut stmt = conn.prepare(&sql_text).map_err(sql)?;
            let rows = stmt
                .query_map(params_from_iter(args), |r| {
                    Ok((edge_at(r)?, node_at(r, 11)?))
                })
                .map_err(sql)?;
            rows.map(|row| {
                let (edge, node) = row.map_err(sql)?;
                Ok(Neighbour {
                    edge: edge_from(edge)?,
                    node: node_from(node)?,
                })
            })
            .collect()
        })
        .await
    }

    async fn company_documents(&self, company: Uuid) -> Result<Vec<Node>> {
        self.run(move |conn| company_documents(conn, company, None))
            .await
    }

    async fn company_documents_first(&self, company: Uuid, limit: u32) -> Result<Vec<Node>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        self.run(move |conn| company_documents(conn, company, Some(limit)))
            .await
    }

    async fn company_document_count(&self, company: Uuid) -> Result<u64> {
        self.run(move |conn| {
            let n: i64 = conn
                .query_row(
                    &format!("SELECT {DOCUMENTS} FROM (SELECT ?1 AS id) n"),
                    [company.to_string()],
                    |r| r.get(0),
                )
                .map_err(sql)?;
            Ok(u64::try_from(n).unwrap_or(0))
        })
        .await
    }

    async fn upsert_location(&self, blake3: &Blake3, at: &ObjectLocation) -> Result<Located> {
        let (h, at) = (*blake3, at.checked()?);
        self.run(move |conn| {
            let tx = conn.transaction().map_err(sql)?;
            let document = ensure_node(&tx, NodeKind::Document, &h.to_hex())?;
            let props = serde_json::json!({
                "endpoint": at.endpoint, "bucket": at.bucket, "key": at.key,
            });
            let location = upsert_node(&tx, NodeKind::Location, &at.node_key(), &props)?;
            tx.execute(
                "INSERT INTO locations (node_id, document_id, blake3, endpoint, bucket, \
                 object_key, updated_at) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7) \
                 ON CONFLICT (node_id) DO UPDATE SET document_id = excluded.document_id, \
                 blake3 = excluded.blake3, updated_at = excluded.updated_at",
                params![
                    location.to_string(),
                    document.to_string(),
                    h.as_bytes().as_slice(),
                    at.endpoint,
                    at.bucket,
                    at.key,
                    now()
                ],
            )
            .map_err(sql)?;
            tx.execute(
                "UPDATE edges SET state = 'superseded', updated_at = ?3 \
                 WHERE dst = ?1 AND kind = 'located_at' AND src <> ?2 AND state = 'active'",
                params![location.to_string(), document.to_string(), now()],
            )
            .map_err(sql)?;
            upsert_edge(
                &tx,
                &Edge::new(document, location, EdgeKind::LocatedAt, EdgeSource::Storage),
            )?;
            tx.commit().map_err(sql)?;
            Ok(Located { document, location })
        })
        .await
    }

    async fn end_location(&self, at: &ObjectLocation, on: Date) -> Result<Option<LocationEnded>> {
        let at = at.checked()?;
        self.run(move |conn| {
            let tx = conn.transaction().map_err(sql)?;
            let row: Option<(String, String)> = tx
                .query_row(
                    "SELECT node_id, document_id FROM locations \
                     WHERE endpoint = ?1 AND bucket = ?2 AND object_key = ?3",
                    params![at.endpoint, at.bucket, at.key],
                    |r| Ok((r.get(0)?, r.get(1)?)),
                )
                .optional()
                .map_err(sql)?;
            let Some((location, document)) = row else {
                return Ok(None);
            };
            tx.execute(
                "UPDATE edges SET state = 'superseded', \
                 valid_to = CASE WHEN valid_from IS NULL OR valid_from <= ?2 THEN ?2 \
                 ELSE valid_from END, updated_at = ?3 \
                 WHERE dst = ?1 AND kind = 'located_at' \
                 AND state NOT IN ('retracted', 'superseded')",
                params![location, julian(on), now()],
            )
            .map_err(sql)?;
            tx.execute("DELETE FROM locations WHERE node_id = ?1", [&location])
                .map_err(sql)?;
            let remaining: i64 = tx
                .query_row(
                    "SELECT count(*) FROM edges WHERE src = ?1 AND kind = 'located_at' \
                     AND state NOT IN ('retracted', 'superseded')",
                    [&document],
                    |r| r.get(0),
                )
                .map_err(sql)?;
            tx.commit().map_err(sql)?;
            Ok(Some(LocationEnded {
                document: uuid(&document)?,
                location: uuid(&location)?,
                remaining: u32::try_from(remaining).unwrap_or(u32::MAX),
            }))
        })
        .await
    }

    async fn claim_alias(
        &self,
        kind: NodeKind,
        alias: &str,
        node: Uuid,
        props: &Value,
    ) -> Result<NodeAlias> {
        validate_alias(alias, props)?;
        let (alias, props) = (alias.to_owned(), props.to_string());
        self.run(move |conn| {
            let (node, props): (Option<String>, String) = conn
                .query_row(
                    "INSERT INTO node_aliases (kind, alias, node_id, props_json, created_at, \
                     updated_at) VALUES (?1, ?2, ?3, ?4, ?5, ?5) \
                     ON CONFLICT (kind, alias) DO UPDATE SET kind = node_aliases.kind \
                     RETURNING node_id, props_json",
                    params![kind.as_str(), alias, node.to_string(), props, now()],
                    |r| Ok((r.get(0)?, r.get(1)?)),
                )
                .map_err(sql)?;
            Ok(NodeAlias {
                node: node.as_deref().map(uuid).transpose()?,
                props: json(&props)?,
            })
        })
        .await
    }

    async fn claim_live_alias(
        &self,
        kind: NodeKind,
        alias: &str,
        node: Uuid,
        props: &Value,
    ) -> Result<NodeAlias> {
        validate_alias(alias, props)?;
        let (alias, props) = (alias.to_owned(), props.to_string());
        self.run(move |conn| {
            // The write lock from the start: a merge runs wholly before or after this.
            let tx = conn
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(sql)?;
            // A missing node: the insert below fails as `claim_alias` does.
            check_live(&tx, node)?;
            let (owner, props): (Option<String>, String) = tx
                .query_row(
                    "INSERT INTO node_aliases (kind, alias, node_id, props_json, created_at, \
                     updated_at) VALUES (?1, ?2, ?3, ?4, ?5, ?5) \
                     ON CONFLICT (kind, alias) DO UPDATE SET kind = node_aliases.kind \
                     RETURNING node_id, props_json",
                    params![kind.as_str(), alias, node.to_string(), props, now()],
                    |r| Ok((r.get(0)?, r.get(1)?)),
                )
                .map_err(sql)?;
            tx.commit().map_err(sql)?;
            Ok(NodeAlias {
                node: owner.as_deref().map(uuid).transpose()?,
                props: json(&props)?,
            })
        })
        .await
    }

    async fn alias(&self, kind: NodeKind, alias: &str) -> Result<Option<NodeAlias>> {
        let alias = alias.to_owned();
        self.run(move |conn| {
            let row: Option<(Option<String>, String)> = conn
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
        })
        .await
    }

    async fn block_alias(&self, kind: NodeKind, alias: &str, props: &Value) -> Result<()> {
        validate_alias(alias, props)?;
        let (alias, props) = (alias.to_owned(), props.to_string());
        self.run(move |conn| {
            conn.execute(
                "INSERT INTO node_aliases (kind, alias, node_id, props_json, created_at, \
                 updated_at) VALUES (?1, ?2, NULL, ?3, ?4, ?4) \
                 ON CONFLICT (kind, alias) DO UPDATE SET node_id = NULL, \
                 props_json = excluded.props_json, updated_at = excluded.updated_at",
                params![kind.as_str(), alias, props, now()],
            )
            .map_err(sql)?;
            Ok(())
        })
        .await
    }

    async fn aliases_of(&self, node: Uuid) -> Result<Vec<OwnedAlias>> {
        self.run(move |conn| merges::aliases_of(conn, node)).await
    }
}

#[async_trait]
impl SketchStore for Standalone {
    async fn put_sketch(&self, blake3: &Blake3, signature: &[u8], bands: &[u64]) -> Result<()> {
        validate_sketch(signature, bands)?;
        let (h, signature) = (*blake3, signature.to_vec());
        let bands: BTreeSet<i64> = bands.iter().map(|b| band_to_i64(*b)).collect();
        self.run(move |conn| {
            let tx = conn.transaction().map_err(sql)?;
            let key = h.as_bytes().as_slice();
            // Issue #74: an unchanged sketch writes nothing. The signature row is updated
            // only when it differs, and only the bands that changed are deleted or inserted.
            tx.execute(
                "INSERT INTO sketches (blake3, signature, updated_at) VALUES (?1, ?2, ?3) \
                 ON CONFLICT (blake3) DO UPDATE SET signature = excluded.signature, \
                 updated_at = excluded.updated_at WHERE signature IS NOT excluded.signature",
                params![key, signature, now()],
            )
            .map_err(sql)?;
            let stored: BTreeSet<i64> = {
                let mut stmt = tx
                    .prepare_cached("SELECT band FROM sketch_bands WHERE blake3 = ?1")
                    .map_err(sql)?;
                let rows = stmt.query_map([key], |r| r.get(0)).map_err(sql)?;
                rows.collect::<rusqlite::Result<_>>().map_err(sql)?
            };
            {
                let mut delete = tx
                    .prepare_cached("DELETE FROM sketch_bands WHERE band = ?1 AND blake3 = ?2")
                    .map_err(sql)?;
                for band in stored.difference(&bands) {
                    delete.execute(params![band, key]).map_err(sql)?;
                }
                let mut insert = tx
                    .prepare_cached("INSERT INTO sketch_bands (band, blake3) VALUES (?1, ?2)")
                    .map_err(sql)?;
                for band in bands.difference(&stored) {
                    insert.execute(params![band, key]).map_err(sql)?;
                }
            }
            tx.commit().map_err(sql)
        })
        .await
    }

    async fn sketch_candidates(
        &self,
        bands: &[u64],
        exclude: &Blake3,
        limit: u32,
    ) -> Result<Vec<Sketch>> {
        if bands.is_empty() {
            return Ok(Vec::new());
        }
        if bands.len() > SKETCH_MAX_BANDS {
            return Err(Error::Invalid(format!(
                "at most {SKETCH_MAX_BANDS} band keys per lookup"
            )));
        }
        let (exclude, limit) = (*exclude, i64::from(sketch_limit(limit)));
        let mut args: Vec<Sql> = bands
            .iter()
            .map(|b| Sql::Integer(band_to_i64(*b)))
            .collect();
        args.push(Sql::Blob(exclude.as_bytes().to_vec()));
        args.push(Sql::Integer(limit));
        let bands_len = bands.len();
        self.run(move |conn| {
            let mut stmt = conn
                .prepare(&sketch_candidates_sql(bands_len))
                .map_err(sql)?;
            let rows = stmt
                .query_map(params_from_iter(args), |r| {
                    Ok((
                        r.get::<_, Vec<u8>>(0)?,
                        r.get::<_, Vec<u8>>(1)?,
                        r.get::<_, i64>(2)?,
                    ))
                })
                .map_err(sql)?;
            rows.map(|row| {
                let (h, signature, shared) = row.map_err(sql)?;
                Ok(Sketch {
                    blake3: Blake3::from_slice(&h)?,
                    signature,
                    shared_bands: u32::try_from(shared).unwrap_or(u32::MAX),
                })
            })
            .collect()
        })
        .await
    }
}

#[async_trait]
impl ClaimStore for Standalone {
    async fn claim_source(&self, key: &SourceKey, owner: &InstanceId) -> Result<SourceClaim> {
        let (key, owner) = (key.clone(), owner.clone());
        self.run(move |conn| {
            let tx = conn.transaction().map_err(sql)?;
            if let Some(claim) = source_owner(&tx, &key)? {
                return if claim.owner == owner {
                    Ok(claim)
                } else {
                    Err(Error::SourceClaimed {
                        key,
                        owner: claim.owner,
                    })
                };
            }
            let at = now();
            tx.execute(
                "INSERT INTO source_claims (endpoint, bucket, prefix, owner, claimed_at) \
                 VALUES (?1, ?2, ?3, ?4, ?5)",
                params![
                    key.endpoint(),
                    key.bucket(),
                    key.prefix(),
                    owner.as_str(),
                    at
                ],
            )
            .map_err(sql)?;
            tx.commit().map_err(sql)?;
            claim_row(&key, owner.as_str(), at)
        })
        .await
    }

    async fn release_source(&self, key: &SourceKey, owner: &InstanceId) -> Result<bool> {
        let (key, owner) = (key.clone(), owner.clone());
        self.run(move |conn| {
            let tx = conn.transaction().map_err(sql)?;
            match source_owner(&tx, &key)? {
                None => Ok(false),
                Some(claim) if claim.owner != owner => Err(Error::SourceClaimed {
                    key,
                    owner: claim.owner,
                }),
                Some(_) => {
                    tx.execute(
                        "DELETE FROM source_claims \
                         WHERE endpoint = ?1 AND bucket = ?2 AND prefix = ?3",
                        params![key.endpoint(), key.bucket(), key.prefix()],
                    )
                    .map_err(sql)?;
                    tx.commit().map_err(sql)?;
                    Ok(true)
                }
            }
        })
        .await
    }

    async fn source_owner(&self, key: &SourceKey) -> Result<Option<SourceClaim>> {
        let key = key.clone();
        self.run(move |conn| source_owner(conn, &key)).await
    }

    async fn lease_company(
        &self,
        company: Uuid,
        owner: &InstanceId,
        ttl: Duration,
    ) -> Result<bool> {
        let owner = owner.clone();
        self.run(move |conn| {
            let at = now();
            let expires = at.saturating_add(i64::try_from(ttl.as_secs()).unwrap_or(i64::MAX));
            let won: Option<String> = conn
                .query_row(
                    "INSERT INTO company_leases (company_id, owner, leased_at, expires_at) \
                     VALUES (?1, ?2, ?3, ?4) \
                     ON CONFLICT (company_id) DO UPDATE SET owner = excluded.owner, \
                     leased_at = excluded.leased_at, expires_at = excluded.expires_at \
                     WHERE company_leases.owner = excluded.owner \
                     OR company_leases.expires_at <= excluded.leased_at \
                     RETURNING owner",
                    params![company.to_string(), owner.as_str(), at, expires],
                    |r| r.get(0),
                )
                .optional()
                .map_err(sql)?;
            Ok(won.is_some())
        })
        .await
    }

    async fn release_company(&self, company: Uuid, owner: &InstanceId) -> Result<bool> {
        let owner = owner.clone();
        self.run(move |conn| {
            let n = conn
                .execute(
                    "DELETE FROM company_leases WHERE company_id = ?1 AND owner = ?2",
                    params![company.to_string(), owner.as_str()],
                )
                .map_err(sql)?;
            Ok(n > 0)
        })
        .await
    }
}

#[async_trait]
impl SearchStore for Standalone {
    async fn index_document(&self, doc: &SearchDoc) -> Result<()> {
        let doc = doc.clone();
        self.run(move |conn| {
            conn.execute(
                "INSERT INTO search_index (blake3, title, body, kind, doc_date, amount, \
                 indexed_at) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7) \
                 ON CONFLICT (blake3) DO UPDATE SET title = excluded.title, \
                 body = excluded.body, kind = excluded.kind, doc_date = excluded.doc_date, \
                 amount = excluded.amount, indexed_at = excluded.indexed_at \
                 WHERE title IS NOT excluded.title OR body IS NOT excluded.body \
                 OR kind IS NOT excluded.kind OR doc_date IS NOT excluded.doc_date \
                 OR amount IS NOT excluded.amount",
                params![
                    doc.blake3.as_bytes().as_slice(),
                    doc.title,
                    doc.body,
                    doc.kind,
                    doc.date.map(julian),
                    doc.amount,
                    now()
                ],
            )
            .map_err(sql)?;
            Ok(())
        })
        .await
    }

    async fn search(&self, q: &SearchQuery) -> Result<Vec<SearchHit>> {
        let q = q.clone();
        self.run(move |conn| {
            let words = q.words();
            let mut args = Vec::new();
            let mut sql_text = if words.is_empty() {
                "SELECT s.blake3, s.title, s.kind, s.doc_date, s.amount, 0.0 AS score \
                 FROM search_index s WHERE 1"
                    .to_owned()
            } else {
                // Each word quoted: the words are letters and digits only, and a quoted
                // string is never parsed as FTS5 syntax. Juxtaposed strings are an AND.
                let phrase = words
                    .iter()
                    .map(|w| format!("\"{w}\""))
                    .collect::<Vec<_>>()
                    .join(" ");
                args.push(Sql::Text(phrase));
                "SELECT s.blake3, s.title, s.kind, s.doc_date, s.amount, \
                 -bm25(search_fts, 10.0, 1.0) AS score \
                 FROM search_fts JOIN search_index s ON s.rowid = search_fts.rowid \
                 WHERE search_fts MATCH ?"
                    .to_owned()
            };
            if let Some(company) = q.company {
                sql_text.push_str(
                    " AND EXISTS (SELECT 1 FROM nodes d JOIN edges e ON e.src = d.id \
                     WHERE d.kind = 'document' AND d.key = lower(hex(s.blake3)) \
                     AND e.kind = 'party' AND e.dst = ? \
                     AND e.state NOT IN ('retracted', 'superseded'))",
                );
                args.push(Sql::Text(company.to_string()));
            }
            if let Some(kind) = &q.kind {
                sql_text.push_str(" AND s.kind = ?");
                args.push(Sql::Text(kind.clone()));
            }
            if let Some(d) = q.date_from {
                sql_text.push_str(" AND s.doc_date >= ?");
                args.push(Sql::Integer(julian(d)));
            }
            if let Some(d) = q.date_to {
                sql_text.push_str(" AND s.doc_date <= ?");
                args.push(Sql::Integer(julian(d)));
            }
            if let Some(a) = q.amount_min {
                sql_text.push_str(" AND s.amount >= ?");
                args.push(Sql::Real(a));
            }
            if let Some(a) = q.amount_max {
                sql_text.push_str(" AND s.amount <= ?");
                args.push(Sql::Real(a));
            }
            if let Some(src) = &q.source {
                sql_text.push_str(
                    " AND EXISTS (SELECT 1 FROM locations l WHERE l.blake3 = s.blake3 \
                     AND l.endpoint = ? AND l.bucket = ? \
                     AND substr(l.object_key, 1, length(?)) = ?)",
                );
                args.extend([
                    Sql::Text(src.endpoint().to_owned()),
                    Sql::Text(src.bucket().to_owned()),
                    Sql::Text(src.prefix().to_owned()),
                    Sql::Text(src.prefix().to_owned()),
                ]);
            }
            sql_text.push_str(
                " ORDER BY score DESC, s.doc_date IS NULL, s.doc_date DESC, s.blake3 \
                 LIMIT ? OFFSET ?",
            );
            args.extend([
                Sql::Integer(i64::from(q.effective_limit())),
                Sql::Integer(i64::from(q.offset)),
            ]);
            let mut stmt = conn.prepare(&sql_text).map_err(sql)?;
            let rows = stmt
                .query_map(params_from_iter(args), |r| {
                    Ok((
                        r.get::<_, Vec<u8>>(0)?,
                        r.get::<_, String>(1)?,
                        r.get::<_, Option<String>>(2)?,
                        r.get::<_, Option<i64>>(3)?,
                        r.get::<_, Option<f64>>(4)?,
                        r.get::<_, f64>(5)?,
                    ))
                })
                .map_err(sql)?;
            rows.map(|row| {
                let (h, title, kind, date, amount, score) = row.map_err(sql)?;
                #[allow(clippy::cast_possible_truncation)]
                Ok(SearchHit {
                    blake3: Blake3::from_slice(&h)?,
                    title,
                    kind,
                    date: from_julian(date)?,
                    amount,
                    score: score as f32,
                })
            })
            .collect()
        })
        .await
    }
}

#[async_trait]
impl Backend for Standalone {
    fn name(&self) -> &'static str {
        "sqlite"
    }

    async fn ping(&self) -> Result<()> {
        self.run(|conn| conn.query_row("SELECT 1", [], |_| Ok(())).map_err(sql))
            .await
    }
}

#[cfg(test)]
mod tests {
    use doewood_core::MasterKey;

    use super::*;

    fn backend() -> Standalone {
        let store = Store::open_in_memory(&MasterKey::from_bytes([1; 32])).unwrap();
        Standalone::new(Arc::new(store))
    }

    #[tokio::test]
    async fn conformance() {
        let a = backend();
        // Claims are local: both "instances" share this one store.
        let b = a.clone();
        doewood_model::conformance::run(&a, &b).await;
    }

    #[tokio::test]
    async fn undated_edges_have_one_identity() {
        let store = backend();
        let branch = store
            .upsert_node(NodeKind::Company, "x", &serde_json::json!({}))
            .await
            .unwrap();
        let head = store
            .upsert_node(NodeKind::Company, "y", &serde_json::json!({}))
            .await
            .unwrap();
        let edge = Edge::new(branch, head, EdgeKind::BranchOf, EdgeSource::Manual);
        let id = store.upsert_edge(&edge).await.unwrap();
        assert_eq!(store.upsert_edge(&edge).await.unwrap(), id);
        let count: i64 = store
            .store
            .conn()
            .query_row("SELECT count(*) FROM edges", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 1);
    }

    #[tokio::test]
    async fn fts_query_syntax_in_text_is_inert() {
        let s = backend();
        for q in ["\"", "a OR", "NEAR(", "*", "col:x", "-x", "^"] {
            let query = SearchQuery {
                text: Some(q.to_owned()),
                ..SearchQuery::default()
            };
            assert!(s.search(&query).await.is_ok(), "{q}");
        }
    }
}
