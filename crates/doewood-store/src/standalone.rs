//! Standalone mode (ADR 0001, section 2): the store traits of `doewood-model` over the
//! instance's own SQLite file. Search uses FTS5; claims and leases are local to the instance.
//!
//! Every call runs on tokio's blocking pool. Writes take the store's single writer; pure reads
//! (records, graph, search, sketches, caches, the merge log) run on its read-only connections
//! ([`Store::readers`]), each in one read transaction. Every write commits before it returns,
//! so a later read sees it; a read concurrent with a write sees the state before it.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use doewood_core::InstanceId;
use doewood_model::{
    Backend, Blake3, Candidates, ClaimStore, Edge, EdgeCursor, EdgeKind, EdgeSource, Error,
    GraphStore, KeyMatch, LiveEdge, Located, LocationEnded, MERGE_CHAIN_MAX, Neighbour,
    NeighbourQuery, Node, NodeAlias, NodeKind, ObjectLocation, OwnedAlias, PartyDocument,
    PropsPatch, PutOutcome, Record, RecordStore, Result, SKETCH_MAX_BANDS, SearchDoc, SearchHit,
    SearchQuery, SearchStore, SimilarKey, SimilarKeys, Sketch, SketchStore, SourceClaim, SourceKey,
    StepVersions, StoredEdge, StoredText, band_to_i64, merged_into, records_limit, sketch_limit,
    validate_alias, validate_batch_count, validate_hashes, validate_keys, validate_node,
    validate_sketch, versions_from_json, versions_to_json,
};
use rusqlite::types::Value as Sql;
use rusqlite::{
    Connection, OptionalExtension as _, Row, TransactionBehavior, params, params_from_iter,
};
use serde_json::Value;
use serde_json::value::RawValue;
use time::Date;
use uuid::Uuid;

use crate::Store;
use crate::store::now;
use crate::store::readers::Caller;
use enrichment::DOCUMENTS;

mod enrichment;
mod explore;
#[cfg(test)]
mod explore_scale;
mod feeds;
mod merges;
mod pages;
mod proposals;
#[cfg(test)]
mod read_paths;
#[cfg(test)]
mod scale;
#[cfg(test)]
mod search_company;
#[cfg(test)]
mod search_scale;

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

    /// Runs a pure read on one of the store's read-only connections (the writer when it has
    /// none), in one transaction: it sees what was committed when it started, and does not
    /// wait for a write in progress. An API call's read gives way to the instance's own
    /// ([`crate::api_call`]).
    async fn read<T, F>(&self, f: F) -> Result<T>
    where
        T: Send + 'static,
        F: FnOnce(&mut Connection) -> Result<T> + Send + 'static,
    {
        self.read_then(f, Ok).await
    }

    /// [`Standalone::read`], then `then` on what it read, on the same blocking thread once
    /// the reader is given back: work that needs no connection does not hold one.
    async fn read_then<R, T, F, G>(&self, f: F, then: G) -> Result<T>
    where
        T: Send + 'static,
        F: FnOnce(&mut Connection) -> Result<R> + Send + 'static,
        G: FnOnce(R) -> Result<T> + Send + 'static,
    {
        let store = Arc::clone(&self.store);
        let caller = Caller::current();
        tokio::task::spawn_blocking(move || caller.run(|| store.read(sql, f)).and_then(then))
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

fn raw_json(text: String) -> Result<Box<RawValue>> {
    RawValue::from_string(text).map_err(|e| backend(format!("stored JSON is invalid: {e}")))
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

/// The one row a write's `RETURNING` gives, with the statement then stepped to its end.
/// `query_row` stops at the row and resets the statement, and a write that commits in that
/// reset (autocommit) never runs SQLite's automatic checkpoint, which `sqlite3_step` starts
/// only when a statement is done: a long run of such writes left the WAL growing without a
/// checkpoint (1.3 million frames at 200,000 `similar_to` edges, issue #151).
fn returning_row<T>(
    stmt: &mut rusqlite::CachedStatement<'_>,
    params: impl rusqlite::Params,
    f: impl FnOnce(&rusqlite::Row<'_>) -> rusqlite::Result<T>,
) -> rusqlite::Result<T> {
    let mut rows = stmt.query(params)?;
    let value = f(rows.next()?.ok_or(rusqlite::Error::QueryReturnedNoRows)?)?;
    while rows.next()?.is_some() {}
    Ok(value)
}

fn upsert_node(conn: &Connection, kind: NodeKind, key: &str, props: &Value) -> Result<Uuid> {
    let id: String = conn
        .prepare_cached(
            "INSERT INTO nodes (id, kind, key, props_json, created_at, updated_at) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?5) \
             ON CONFLICT (kind, key) DO UPDATE SET props_json = excluded.props_json, \
             updated_at = excluded.updated_at \
             RETURNING id",
        )
        .and_then(|mut cached| {
            returning_row(
                &mut cached,
                params![
                    Uuid::now_v7().to_string(),
                    kind.as_str(),
                    key,
                    props.to_string(),
                    now()
                ],
                |r| r.get(0),
            )
        })
        .map_err(sql)?;
    uuid(&id)
}

/// Inserts the node if it is missing and leaves an existing one untouched.
fn ensure_node(conn: &Connection, kind: NodeKind, key: &str) -> Result<Uuid> {
    conn.prepare_cached(
        "INSERT INTO nodes (id, kind, key, created_at, updated_at) VALUES (?1, ?2, ?3, ?4, ?4) \
         ON CONFLICT (kind, key) DO NOTHING",
    )
    .and_then(|mut cached| {
        cached.execute(params![
            Uuid::now_v7().to_string(),
            kind.as_str(),
            key,
            now()
        ])
    })
    .map_err(sql)?;
    let id: String = conn
        .prepare_cached("SELECT id FROM nodes WHERE kind = ?1 AND key = ?2")
        .and_then(|mut cached| cached.query_row(params![kind.as_str(), key], |r| r.get(0)))
        .map_err(sql)?;
    uuid(&id)
}

fn upsert_edge(conn: &Connection, e: &Edge) -> Result<Uuid> {
    e.validate()?;
    let id: String = conn
        .prepare_cached(
            "INSERT INTO edges (id, src, dst, kind, props_json, evidence_json, confidence, state, \
             valid_from, valid_to, source, created_at, updated_at) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?12) \
             ON CONFLICT (src, dst, kind, source, ifnull(valid_from, -2147483648)) \
             DO UPDATE SET \
             props_json = excluded.props_json, evidence_json = excluded.evidence_json, \
             confidence = excluded.confidence, state = excluded.state, \
             valid_to = excluded.valid_to, updated_at = excluded.updated_at \
             RETURNING id",
        )
        .and_then(|mut cached| {
            returning_row(
                &mut cached,
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
        })
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
        .prepare_cached(&format!(
            "SELECT {NODE_COLUMNS} FROM nodes n WHERE n.id = ?1"
        ))
        .and_then(|mut cached| cached.query_row([id.to_string()], |r| node_at(r, 0)))
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
        tx.prepare_cached("UPDATE nodes SET props_json = ?2, updated_at = ?3 WHERE id = ?1")
            .and_then(|mut cached| {
                cached.execute(params![id.to_string(), node.props.to_string(), now()])
            })
            .map_err(sql)?;
    }
    tx.commit().map_err(sql)?;
    Ok(Some(node))
}

/// Refuses a husk with [`Error::MergedAway`] (issue #62). `false` when there is no node `id`.
/// Called inside a write transaction, so no merge commits before the caller's write.
pub(crate) fn check_live(conn: &Connection, id: Uuid) -> Result<bool> {
    let row: Option<Option<String>> = conn
        .prepare_cached("SELECT json_extract(props_json, '$.merged_into') FROM nodes WHERE id = ?1")
        .and_then(|mut cached| cached.query_row([id.to_string()], |r| r.get(0)))
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
            .prepare_cached(
                "SELECT json_extract(props_json, '$.merged_into') FROM nodes WHERE id = ?1",
            )
            .and_then(|mut cached| cached.query_row([id.to_string()], |r| r.get(0)))
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
    conn.prepare_cached(
        "SELECT owner, claimed_at FROM source_claims \
         WHERE endpoint = ?1 AND bucket = ?2 AND prefix = ?3",
    )
    .and_then(|mut cached| {
        cached.query_row(params![key.endpoint(), key.bucket(), key.prefix()], |r| {
            Ok((r.get::<_, String>(0)?, r.get::<_, i64>(1)?))
        })
    })
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
        body: &RawValue,
        versions: &StepVersions,
    ) -> Result<PutOutcome> {
        let (h, body, versions) = (*blake3, body.get().to_owned(), versions_to_json(versions));
        self.run(move |conn| {
            let tx = conn.transaction().map_err(sql)?;
            let old: Option<(String, String)> = tx
                .prepare_cached("SELECT body_json, versions_json FROM records WHERE blake3 = ?1")
                .and_then(|mut cached| {
                    cached.query_row([h.as_bytes().as_slice()], |r| Ok((r.get(0)?, r.get(1)?)))
                })
                .optional()
                .map_err(sql)?;
            let outcome = match old {
                // Bodies are compared as text: every writer stores the canonical form (#98).
                Some((b, v)) if b == body && json(&v)? == versions => {
                    return Ok(PutOutcome::Unchanged);
                }
                Some(_) => PutOutcome::Updated,
                None => PutOutcome::Inserted,
            };
            tx.prepare_cached(
                "INSERT INTO records (blake3, body_json, versions_json, updated_at) \
                 VALUES (?1, ?2, ?3, ?4) \
                 ON CONFLICT (blake3) DO UPDATE SET body_json = excluded.body_json, \
                 versions_json = excluded.versions_json, updated_at = excluded.updated_at",
            )
            .and_then(|mut cached| {
                cached.execute(params![
                    h.as_bytes().as_slice(),
                    body,
                    versions.to_string(),
                    now()
                ])
            })
            .map_err(sql)?;
            tx.commit().map_err(sql)?;
            Ok(outcome)
        })
        .await
    }

    async fn get_record(&self, blake3: &Blake3) -> Result<Option<Record>> {
        let h = *blake3;
        self.read(move |conn| {
            let row: Option<(String, String, i64)> = conn
                .prepare_cached(
                    "SELECT body_json, versions_json, updated_at FROM records WHERE blake3 = ?1",
                )
                .and_then(|mut cached| {
                    cached.query_row([h.as_bytes().as_slice()], |r| {
                        Ok((r.get(0)?, r.get(1)?, r.get(2)?))
                    })
                })
                .optional()
                .map_err(sql)?;
            row.map(|(body, versions, updated_at)| {
                Ok(Record {
                    blake3: h,
                    body: raw_json(body)?,
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
        self.read(move |conn| {
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
        self.read(move |conn| {
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
        max_bytes: usize,
    ) -> Result<Vec<Record>> {
        let (source, after) = (source.clone(), after.copied());
        let limit = i64::from(records_limit(limit));
        self.read_then(
            move |conn| {
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
                // Rows are read one at a time; the page ends at the byte budget (issue #111),
                // counted as `doewood_model::record_page_bytes` counts it (the body's JSON text).
                let (mut page, mut bytes) = (Vec::new(), 0);
                for row in rows {
                    let row = row.map_err(sql)?;
                    bytes += row.1.len();
                    page.push(row);
                    if bytes >= max_bytes {
                        break;
                    }
                }
                Ok(page)
            },
            // The bodies are checked as JSON once the reader is given back: a page can hold
            // megabytes of them (issue #109).
            |rows| {
                rows.into_iter()
                    .map(|(h, body, versions, updated_at)| {
                        Ok(Record {
                            blake3: Blake3::from_slice(&h)?,
                            body: raw_json(body)?,
                            versions: versions_from_json(&json(&versions)?)?,
                            updated_at,
                        })
                    })
                    .collect()
            },
        )
        .await
    }

    async fn put_document_text(&self, blake3: &Blake3, body: &RawValue) -> Result<()> {
        let (h, body) = (*blake3, body.get().to_owned());
        self.run(move |conn| {
            // Unchanged text is not written again (`updated_at` stays). A missing record is a
            // foreign key error: `NotFound`.
            conn.prepare_cached(
                "INSERT INTO document_texts (blake3, body, updated_at) VALUES (?1, ?2, ?3) \
                 ON CONFLICT (blake3) DO UPDATE SET body = excluded.body, \
                 updated_at = excluded.updated_at WHERE document_texts.body <> excluded.body",
            )
            .and_then(|mut cached| cached.execute(params![h.as_bytes().as_slice(), body, now()]))
            .map_err(sql)?;
            Ok(())
        })
        .await
    }

    async fn delete_document_text(&self, blake3: &Blake3) -> Result<()> {
        let h = *blake3;
        self.run(move |conn| {
            conn.prepare_cached("DELETE FROM document_texts WHERE blake3 = ?1")
                .and_then(|mut cached| cached.execute([h.as_bytes().as_slice()]))
                .map_err(sql)?;
            Ok(())
        })
        .await
    }

    async fn get_document_text(&self, blake3: &Blake3) -> Result<Option<StoredText>> {
        let h = *blake3;
        self.read(move |conn| {
            let row: Option<(String, i64)> = conn
                .prepare_cached("SELECT body, updated_at FROM document_texts WHERE blake3 = ?1")
                .and_then(|mut cached| {
                    cached.query_row([h.as_bytes().as_slice()], |r| Ok((r.get(0)?, r.get(1)?)))
                })
                .optional()
                .map_err(sql)?;
            row.map(|(body, updated_at)| {
                Ok(StoredText {
                    blake3: h,
                    body: raw_json(body)?,
                    updated_at,
                })
            })
            .transpose()
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
        self.read(move |conn| {
            conn.prepare_cached(&format!(
                "SELECT {NODE_COLUMNS} FROM nodes n WHERE n.id = ?1"
            ))
            .and_then(|mut cached| cached.query_row([id.to_string()], |r| node_at(r, 0)))
            .optional()
            .map_err(sql)?
            .map(node_from)
            .transpose()
        })
        .await
    }

    async fn node_by_key(&self, kind: NodeKind, key: &str) -> Result<Option<Node>> {
        let key = key.to_owned();
        self.read(move |conn| {
            conn.prepare_cached(&format!(
                "SELECT {NODE_COLUMNS} FROM nodes n WHERE n.kind = ?1 AND n.key = ?2"
            ))
            .and_then(|mut cached| cached.query_row(params![kind.as_str(), key], |r| node_at(r, 0)))
            .optional()
            .map_err(sql)?
            .map(node_from)
            .transpose()
        })
        .await
    }

    async fn node_ids(&self, kind: NodeKind, keys: &[String]) -> Result<Vec<Option<Uuid>>> {
        validate_batch_count(keys.len())?;
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        let keys = keys.to_vec();
        // One read connection for all of them (issue #126).
        self.read(move |conn| {
            let mut stmt = conn
                .prepare_cached("SELECT id FROM nodes WHERE kind = ?1 AND key = ?2")
                .map_err(sql)?;
            keys.iter()
                .map(|key| {
                    stmt.query_row(params![kind.as_str(), key], |r| r.get::<_, String>(0))
                        .optional()
                        .map_err(sql)?
                        .map(|id| uuid(&id))
                        .transpose()
                })
                .collect()
        })
        .await
    }

    async fn upsert_edge(&self, edge: &Edge) -> Result<Uuid> {
        edge.validate()?;
        let edge = edge.clone();
        self.run(move |conn| upsert_edge(conn, &edge)).await
    }

    async fn upsert_edges(&self, edges: &[Edge]) -> Result<Vec<Uuid>> {
        validate_batch_count(edges.len())?;
        for e in edges {
            e.validate()?;
        }
        if edges.is_empty() {
            return Ok(Vec::new());
        }
        let edges = edges.to_vec();
        // One trip to the writer and one transaction for all of them (issue #126).
        self.run(move |conn| {
            let tx = conn.transaction().map_err(sql)?;
            let ids = edges
                .iter()
                .map(|e| upsert_edge(&tx, e))
                .collect::<Result<Vec<_>>>()?;
            tx.commit().map_err(sql)?;
            Ok(ids)
        })
        .await
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
        self.read(move |conn| pages::neighbours(conn, node, &q, None))
            .await
    }

    async fn neighbours_page(
        &self,
        node: Uuid,
        q: &NeighbourQuery,
        after: Option<&EdgeCursor>,
        limit: u32,
        max_bytes: usize,
    ) -> Result<Vec<Neighbour>> {
        let (q, after) = (q.clone(), after.copied());
        self.read(move |conn| {
            pages::neighbours(conn, node, &q, Some((after.as_ref(), limit, max_bytes)))
        })
        .await
    }

    async fn company_documents(&self, company: Uuid) -> Result<Vec<Node>> {
        self.read(move |conn| {
            Ok(
                pages::company_documents(conn, company, None, None, usize::MAX)?
                    .into_iter()
                    .map(|d| d.node)
                    .collect(),
            )
        })
        .await
    }

    async fn company_documents_page(
        &self,
        company: Uuid,
        after: Option<&EdgeCursor>,
        limit: u32,
        max_bytes: usize,
    ) -> Result<Vec<PartyDocument>> {
        let after = after.copied();
        self.read(move |conn| {
            pages::company_documents(
                conn,
                company,
                after.as_ref(),
                Some(records_limit(limit)),
                max_bytes,
            )
        })
        .await
    }

    async fn company_documents_first(&self, company: Uuid, limit: u32) -> Result<Vec<Node>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        self.read(move |conn| {
            Ok(
                pages::company_documents(conn, company, None, Some(limit), usize::MAX)?
                    .into_iter()
                    .map(|d| d.node)
                    .collect(),
            )
        })
        .await
    }

    async fn company_document_count(&self, company: Uuid) -> Result<u64> {
        self.read(move |conn| {
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
            tx.prepare_cached(
                "INSERT INTO locations (node_id, document_id, blake3, endpoint, bucket, \
                 object_key, updated_at) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7) \
                 ON CONFLICT (node_id) DO UPDATE SET document_id = excluded.document_id, \
                 blake3 = excluded.blake3, updated_at = excluded.updated_at",
            )
            .and_then(|mut cached| {
                cached.execute(params![
                    location.to_string(),
                    document.to_string(),
                    h.as_bytes().as_slice(),
                    at.endpoint,
                    at.bucket,
                    at.key,
                    now()
                ])
            })
            .map_err(sql)?;
            tx.prepare_cached(
                "UPDATE edges SET state = 'superseded', updated_at = ?3 \
                 WHERE dst = ?1 AND kind = 'located_at' AND src <> ?2 AND state = 'active'",
            )
            .and_then(|mut cached| {
                cached.execute(params![location.to_string(), document.to_string(), now()])
            })
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
                .prepare_cached(
                    "SELECT node_id, document_id FROM locations \
                     WHERE endpoint = ?1 AND bucket = ?2 AND object_key = ?3",
                )
                .and_then(|mut cached| {
                    cached.query_row(params![at.endpoint, at.bucket, at.key], |r| {
                        Ok((r.get(0)?, r.get(1)?))
                    })
                })
                .optional()
                .map_err(sql)?;
            let Some((location, document)) = row else {
                return Ok(None);
            };
            tx.prepare_cached(
                "UPDATE edges SET state = 'superseded', \
                 valid_to = CASE WHEN valid_from IS NULL OR valid_from <= ?2 THEN ?2 \
                 ELSE valid_from END, updated_at = ?3 \
                 WHERE dst = ?1 AND kind = 'located_at' \
                 AND state NOT IN ('retracted', 'superseded')",
            )
            .and_then(|mut cached| cached.execute(params![location, julian(on), now()]))
            .map_err(sql)?;
            tx.prepare_cached("DELETE FROM locations WHERE node_id = ?1")
                .and_then(|mut cached| cached.execute([&location]))
                .map_err(sql)?;
            let remaining: i64 = tx
                .prepare_cached(
                    "SELECT count(*) FROM edges WHERE src = ?1 AND kind = 'located_at' \
                     AND state NOT IN ('retracted', 'superseded')",
                )
                .and_then(|mut cached| cached.query_row([&document], |r| r.get(0)))
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
                .prepare_cached(
                    "INSERT INTO node_aliases (kind, alias, node_id, props_json, created_at, \
                     updated_at) VALUES (?1, ?2, ?3, ?4, ?5, ?5) \
                     ON CONFLICT (kind, alias) DO UPDATE SET kind = node_aliases.kind \
                     RETURNING node_id, props_json",
                )
                .and_then(|mut cached| {
                    cached.query_row(
                        params![kind.as_str(), alias, node.to_string(), props, now()],
                        |r| Ok((r.get(0)?, r.get(1)?)),
                    )
                })
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
                .prepare_cached(
                    "INSERT INTO node_aliases (kind, alias, node_id, props_json, created_at, \
                     updated_at) VALUES (?1, ?2, ?3, ?4, ?5, ?5) \
                     ON CONFLICT (kind, alias) DO UPDATE SET kind = node_aliases.kind \
                     RETURNING node_id, props_json",
                )
                .and_then(|mut cached| {
                    cached.query_row(
                        params![kind.as_str(), alias, node.to_string(), props, now()],
                        |r| Ok((r.get(0)?, r.get(1)?)),
                    )
                })
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
        self.read(move |conn| {
            let row: Option<(Option<String>, String)> = conn
                .prepare_cached(
                    "SELECT node_id, props_json FROM node_aliases WHERE kind = ?1 AND alias = ?2",
                )
                .and_then(|mut cached| {
                    cached.query_row(params![kind.as_str(), alias], |r| {
                        Ok((r.get(0)?, r.get(1)?))
                    })
                })
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
            conn.prepare_cached(
                "INSERT INTO node_aliases (kind, alias, node_id, props_json, created_at, \
                 updated_at) VALUES (?1, ?2, NULL, ?3, ?4, ?4) \
                 ON CONFLICT (kind, alias) DO UPDATE SET node_id = NULL, \
                 props_json = excluded.props_json, updated_at = excluded.updated_at",
            )
            .and_then(|mut cached| cached.execute(params![kind.as_str(), alias, props, now()]))
            .map_err(sql)?;
            Ok(())
        })
        .await
    }

    async fn aliases_of(&self, node: Uuid) -> Result<Vec<OwnedAlias>> {
        self.read(move |conn| merges::aliases_of(conn, node)).await
    }

    async fn neighbours_of_many(
        &self,
        nodes: &[Uuid],
        q: &NeighbourQuery,
        per_node: u32,
        max_bytes: usize,
    ) -> Result<Vec<(Uuid, Neighbour)>> {
        if nodes.is_empty() || per_node == 0 {
            return Ok(Vec::new());
        }
        let (nodes, q) = (nodes.to_vec(), q.clone());
        // One read for every node: the page query once a node, on one connection.
        self.read(move |conn| {
            let mut out = Vec::new();
            let mut bytes = 0usize;
            for node in nodes {
                if bytes >= max_bytes {
                    break;
                }
                for n in
                    pages::neighbours(conn, node, &q, Some((None, per_node, max_bytes - bytes)))?
                {
                    bytes += doewood_model::neighbour_page_bytes(&n);
                    out.push((node, n));
                }
            }
            Ok(out)
        })
        .await
    }

    async fn fold_company_graph(&self, max_rows: u32) -> Result<doewood_model::Folded> {
        self.run(move |conn| explore::fold(conn, max_rows)).await
    }

    async fn company_documents_total(&self) -> Result<u64> {
        self.read(|conn| explore::documents_total(conn)).await
    }

    async fn list_companies(
        &self,
        filter: &doewood_model::CompanyFilter,
        order: doewood_model::CompanyOrder,
        after: Option<&doewood_model::CompanyCursor>,
        limit: u32,
        max_bytes: usize,
    ) -> Result<Vec<doewood_model::CompanyRow>> {
        filter.validate()?;
        let (filter, after) = (filter.clone(), after.cloned());
        let limit = limit.clamp(1, doewood_model::COMPANY_GRAPH_NODES_MAX + 1);
        self.read(move |conn| {
            explore::list_companies(conn, &filter, order, after.as_ref(), limit, max_bytes)
        })
        .await
    }

    async fn company_links(
        &self,
        companies: &[Uuid],
        min_documents: u32,
        limit: u32,
    ) -> Result<Vec<doewood_model::CompanyLink>> {
        if companies.len() > doewood_model::COMPANY_GRAPH_NODES_MAX as usize {
            return Err(Error::Invalid(format!(
                "at most {} companies",
                doewood_model::COMPANY_GRAPH_NODES_MAX
            )));
        }
        if companies.len() < 2 || limit == 0 {
            return Ok(Vec::new());
        }
        let companies = companies.to_vec();
        let limit = limit.min(doewood_model::COMPANY_GRAPH_LINKS_MAX + 1);
        self.read(move |conn| explore::company_links(conn, &companies, min_documents, limit))
            .await
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
            put_sketch(&tx, &h, &signature, &bands)?;
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
        Ok(self
            .sketch_candidates_within(bands, exclude, limit, u32::MAX)
            .await?
            .sketches)
    }

    async fn sketch_candidates_within(
        &self,
        bands: &[u64],
        exclude: &Blake3,
        limit: u32,
        max_bucket: u32,
    ) -> Result<Candidates> {
        if bands.is_empty() {
            return Ok(Candidates::default());
        }
        let bands = band_keys(bands)?;
        let (exclude, limit) = (*exclude, i64::from(sketch_limit(limit)));
        self.read(move |conn| band_candidates(conn, &bands, &exclude, limit, max_bucket))
            .await
    }

    async fn put_similarity(
        &self,
        blake3: &Blake3,
        sketch: Option<(&[u8], &[u64])>,
        keys: &SimilarKeys,
    ) -> Result<()> {
        if keys.keys.is_empty() {
            return match sketch {
                Some((signature, bands)) => self.put_sketch(blake3, signature, bands).await,
                None => Ok(()),
            };
        }
        validate_keys(&keys.keys)?;
        if let Some((signature, bands)) = sketch {
            validate_sketch(signature, bands)?;
        }
        let h = *blake3;
        let sketch: Option<(Vec<u8>, BTreeSet<i64>)> = sketch.map(|(signature, bands)| {
            (
                signature.to_vec(),
                bands.iter().map(|b| band_to_i64(*b)).collect(),
            )
        });
        let keys = keys.clone();
        self.run(move |conn| {
            let tx = conn.transaction().map_err(sql)?;
            if let Some((signature, bands)) = &sketch {
                put_sketch(&tx, &h, signature, bands)?;
            }
            put_keys(&tx, &h, &keys)?;
            tx.commit().map_err(sql)
        })
        .await
    }

    async fn similarity_candidates(
        &self,
        bands: &[u64],
        keys: &[SimilarKey],
        exclude: &Blake3,
        limit: u32,
        max_bucket: u32,
    ) -> Result<Candidates> {
        if keys.is_empty() {
            return self
                .sketch_candidates_within(bands, exclude, limit, max_bucket)
                .await;
        }
        validate_keys(keys)?;
        let bands = band_keys(bands)?;
        let keys = keys.to_vec();
        let (exclude, limit) = (*exclude, i64::from(sketch_limit(limit)));
        self.read(move |conn| {
            let mut out = if bands.is_empty() {
                Candidates::default()
            } else {
                band_candidates(conn, &bands, &exclude, limit, max_bucket)?
            };
            out.keyed = key_candidates(conn, &keys, &exclude, limit)?;
            Ok(out)
        })
        .await
    }
}

/// Band keys for a lookup, as stored; at most [`SKETCH_MAX_BANDS`].
fn band_keys(bands: &[u64]) -> Result<Vec<i64>> {
    if bands.len() > SKETCH_MAX_BANDS {
        return Err(Error::Invalid(format!(
            "at most {SKETCH_MAX_BANDS} band keys per lookup"
        )));
    }
    Ok(bands.iter().map(|b| band_to_i64(*b)).collect())
}

/// Stores a sketch in `tx` ([`SketchStore::put_sketch`]).
fn put_sketch(
    tx: &rusqlite::Transaction<'_>,
    h: &Blake3,
    signature: &[u8],
    bands: &BTreeSet<i64>,
) -> Result<()> {
    let key = h.as_bytes().as_slice();
    // Issue #74: an unchanged sketch writes nothing. The signature row is updated
    // only when it differs, and only the bands that changed are deleted or inserted.
    tx.prepare_cached(
        "INSERT INTO sketches (blake3, signature, updated_at) VALUES (?1, ?2, ?3) \
         ON CONFLICT (blake3) DO UPDATE SET signature = excluded.signature, \
         updated_at = excluded.updated_at WHERE signature IS NOT excluded.signature",
    )
    .and_then(|mut cached| cached.execute(params![key, signature, now()]))
    .map_err(sql)?;
    let stored: BTreeSet<i64> = {
        let mut stmt = tx
            .prepare_cached("SELECT band FROM sketch_bands WHERE blake3 = ?1")
            .map_err(sql)?;
        let rows = stmt.query_map([key], |r| r.get(0)).map_err(sql)?;
        rows.collect::<rusqlite::Result<_>>().map_err(sql)?
    };
    let mut delete = tx
        .prepare_cached("DELETE FROM sketch_bands WHERE band = ?1 AND blake3 = ?2")
        .map_err(sql)?;
    for band in stored.difference(bands) {
        delete.execute(params![band, key]).map_err(sql)?;
    }
    let mut insert = tx
        .prepare_cached("INSERT INTO sketch_bands (band, blake3) VALUES (?1, ?2)")
        .map_err(sql)?;
    for band in bands.difference(&stored) {
        insert.execute(params![band, key]).map_err(sql)?;
    }
    Ok(())
}

/// Stores a document's similarity keys in `tx` ([`SketchStore::put_similarity`], issue
/// #169): as `put_sketch`, only the keys that changed are deleted or inserted, and the masked
/// value is updated only where it differs.
fn put_keys(tx: &rusqlite::Transaction<'_>, h: &Blake3, keys: &SimilarKeys) -> Result<()> {
    let doc = h.as_bytes().as_slice();
    let masked = keys.masked.map(band_to_i64);
    let want: BTreeSet<(i16, i64)> = keys
        .keys
        .iter()
        .map(|k| (k.kind.code(), band_to_i64(k.key)))
        .collect();
    let stored: BTreeSet<(i16, i64)> = {
        let mut stmt = tx
            .prepare_cached("SELECT kind, key FROM similar_keys WHERE blake3 = ?1")
            .map_err(sql)?;
        let rows = stmt
            .query_map([doc], |r| Ok((r.get(0)?, r.get(1)?)))
            .map_err(sql)?;
        rows.collect::<rusqlite::Result<_>>().map_err(sql)?
    };
    let mut delete = tx
        .prepare_cached("DELETE FROM similar_keys WHERE kind = ?1 AND key = ?2 AND blake3 = ?3")
        .map_err(sql)?;
    for (kind, key) in stored.difference(&want) {
        delete.execute(params![kind, key, doc]).map_err(sql)?;
    }
    tx.prepare_cached("UPDATE similar_keys SET masked = ?2 WHERE blake3 = ?1 AND masked IS NOT ?2")
        .and_then(|mut cached| cached.execute(params![doc, masked]))
        .map_err(sql)?;
    let mut insert = tx
        .prepare_cached(
            "INSERT INTO similar_keys (kind, key, blake3, masked) VALUES (?1, ?2, ?3, ?4)",
        )
        .map_err(sql)?;
    for (kind, key) in want.difference(&stored) {
        insert
            .execute(params![kind, key, doc, masked])
            .map_err(sql)?;
    }
    Ok(())
}

/// The documents other than `exclude` sharing one of `keys`, at most `limit` per key in hash
/// order (a range of the primary key each), with their masked `SimHash` and signature.
fn key_candidates(
    conn: &Connection,
    keys: &[SimilarKey],
    exclude: &Blake3,
    limit: i64,
) -> Result<Vec<KeyMatch>> {
    let mut stmt = conn
        .prepare_cached(
            "SELECT k.blake3, k.masked, s.signature FROM similar_keys k \
             LEFT JOIN sketches s ON s.blake3 = k.blake3 \
             WHERE k.kind = ?1 AND k.key = ?2 AND k.blake3 <> ?3 ORDER BY k.blake3 LIMIT ?4",
        )
        .map_err(sql)?;
    let mut out = Vec::new();
    let distinct: BTreeSet<SimilarKey> = keys.iter().copied().collect();
    for k in distinct {
        let rows = stmt
            .query_map(
                params![
                    k.kind.code(),
                    band_to_i64(k.key),
                    exclude.as_bytes().as_slice(),
                    limit
                ],
                |r| {
                    Ok((
                        r.get::<_, Vec<u8>>(0)?,
                        r.get::<_, Option<i64>>(1)?,
                        r.get::<_, Option<Vec<u8>>>(2)?,
                    ))
                },
            )
            .map_err(sql)?;
        for row in rows {
            let (h, masked, signature) = row.map_err(sql)?;
            #[allow(clippy::cast_sign_loss)]
            out.push(KeyMatch {
                blake3: Blake3::from_slice(&h)?,
                kind: k.kind,
                masked: masked.map(|m| m as u64),
                signature,
            });
        }
    }
    out.sort_by_key(|a| (a.kind, a.blake3));
    Ok(out)
}

/// [`SketchStore::sketch_candidates_within`] on `conn`, for band keys already converted.
fn band_candidates(
    conn: &Connection,
    bands: &[i64],
    exclude: &Blake3,
    limit: i64,
    max_bucket: u32,
) -> Result<Candidates> {
    // Issue #148: the bands whose bucket is too large, one `sketch_band_sizes` row
    // each; `u32::MAX` (no limit) skips the lookup.
    let big: BTreeSet<i64> = if max_bucket == u32::MAX {
        BTreeSet::new()
    } else {
        let marks = vec!["?"; bands.len()].join(", ");
        let mut stmt = conn
            .prepare(&format!(
                "SELECT band FROM sketch_band_sizes WHERE band IN ({marks}) AND docs > ?"
            ))
            .map_err(sql)?;
        let args = bands
            .iter()
            .map(|b| Sql::Integer(*b))
            .chain([Sql::Integer(i64::from(max_bucket))]);
        let rows = stmt
            .query_map(params_from_iter(args), |r| r.get(0))
            .map_err(sql)?;
        rows.collect::<rusqlite::Result<_>>().map_err(sql)?
    };
    let kept: Vec<i64> = bands.iter().copied().filter(|b| !big.contains(b)).collect();
    let skipped_bands = u32::try_from(big.len()).unwrap_or(u32::MAX);
    if kept.is_empty() {
        return Ok(Candidates {
            skipped_bands,
            ..Candidates::default()
        });
    }
    let mut args: Vec<Sql> = kept.iter().map(|b| Sql::Integer(*b)).collect();
    args.push(Sql::Blob(exclude.as_bytes().to_vec()));
    args.push(Sql::Integer(limit));
    let mut stmt = conn
        .prepare(&sketch_candidates_sql(kept.len()))
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
    let sketches = rows
        .map(|row| {
            let (h, signature, shared) = row.map_err(sql)?;
            Ok(Sketch {
                blake3: Blake3::from_slice(&h)?,
                signature,
                shared_bands: u32::try_from(shared).unwrap_or(u32::MAX),
            })
        })
        .collect::<Result<_>>()?;
    Ok(Candidates {
        sketches,
        skipped_bands,
        ..Candidates::default()
    })
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
            tx.prepare_cached(
                "INSERT INTO source_claims (endpoint, bucket, prefix, owner, claimed_at) \
                 VALUES (?1, ?2, ?3, ?4, ?5)",
            )
            .and_then(|mut cached| {
                cached.execute(params![
                    key.endpoint(),
                    key.bucket(),
                    key.prefix(),
                    owner.as_str(),
                    at
                ])
            })
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
                    tx.prepare_cached(
                        "DELETE FROM source_claims \
                         WHERE endpoint = ?1 AND bucket = ?2 AND prefix = ?3",
                    )
                    .and_then(|mut cached| {
                        cached.execute(params![key.endpoint(), key.bucket(), key.prefix()])
                    })
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
        self.read(move |conn| source_owner(conn, &key)).await
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
                .prepare_cached(
                    "INSERT INTO company_leases (company_id, owner, leased_at, expires_at) \
                     VALUES (?1, ?2, ?3, ?4) \
                     ON CONFLICT (company_id) DO UPDATE SET owner = excluded.owner, \
                     leased_at = excluded.leased_at, expires_at = excluded.expires_at \
                     WHERE company_leases.owner = excluded.owner \
                     OR company_leases.expires_at <= excluded.leased_at \
                     RETURNING owner",
                )
                .and_then(|mut cached| {
                    cached.query_row(
                        params![company.to_string(), owner.as_str(), at, expires],
                        |r| r.get(0),
                    )
                })
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
                .prepare_cached("DELETE FROM company_leases WHERE company_id = ?1 AND owner = ?2")
                .and_then(|mut cached| cached.execute(params![company.to_string(), owner.as_str()]))
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
            conn.prepare_cached(
                "INSERT INTO search_index (blake3, title, body, kind, doc_date, amount, \
                 indexed_at) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7) \
                 ON CONFLICT (blake3) DO UPDATE SET title = excluded.title, \
                 body = excluded.body, kind = excluded.kind, doc_date = excluded.doc_date, \
                 amount = excluded.amount, indexed_at = excluded.indexed_at \
                 WHERE title IS NOT excluded.title OR body IS NOT excluded.body \
                 OR kind IS NOT excluded.kind OR doc_date IS NOT excluded.doc_date \
                 OR amount IS NOT excluded.amount",
            )
            .and_then(|mut cached| {
                cached.execute(params![
                    doc.blake3.as_bytes().as_slice(),
                    doc.title,
                    doc.body,
                    doc.kind,
                    doc.date.map(julian),
                    doc.amount,
                    now()
                ])
            })
            .map_err(sql)?;
            Ok(())
        })
        .await
    }

    async fn search(&self, q: &SearchQuery) -> Result<Vec<SearchHit>> {
        let q = q.clone();
        self.read(move |conn| {
            let words = q.words();
            if !words.is_empty() && only_text(&q) {
                return text_search(conn, &words, &q);
            }
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
                // Driven by the company's party edges (edges_by_dst), each document's hash
                // looked up by primary key, rather than a check per indexed document (issue
                // #111). A document key is its hash in lowercase hex or a `ref:` placeholder
                // (`validate_node`); unhex gives NULL for the placeholders.
                sql_text.push_str(
                    " AND s.blake3 IN (SELECT unhex(d.key) FROM edges e \
                     JOIN nodes d ON d.id = e.src \
                     WHERE e.dst = ? AND e.kind = 'party' \
                     AND e.state NOT IN ('retracted', 'superseded') AND d.kind = 'document')",
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
            search_hits(&mut stmt, params_from_iter(args))
        })
        .await
    }
}

/// Whether `q` filters on nothing but its words.
fn only_text(q: &SearchQuery) -> bool {
    q.company.is_none()
        && q.kind.is_none()
        && q.date_from.is_none()
        && q.date_to.is_none()
        && q.amount_min.is_none()
        && q.amount_max.is_none()
        && q.source.is_none()
}

/// A search on words alone (issue #140). Ranked as before, by score, then date (newest
/// first, undated last), then hash; but only the matches that can be on the page are read
/// from `search_index`. The matches are scored on the FTS index alone, the score of the
/// last hit the page can reach (`limit + offset`-th best) is the cut, and only the matches
/// scoring at least the cut are joined and sorted in full: every match below it has at
/// least `limit + offset` matches ahead of it, so the page is the same. Before, every match
/// was joined to its row and sorted, which cost a row read per match.
fn text_search(conn: &Connection, words: &[String], q: &SearchQuery) -> Result<Vec<SearchHit>> {
    // Each word quoted: the words are letters and digits only, and a quoted string is never
    // parsed as FTS5 syntax. Juxtaposed strings are an AND.
    let phrase = words
        .iter()
        .map(|w| format!("\"{w}\""))
        .collect::<Vec<_>>()
        .join(" ");
    let (limit, offset) = (i64::from(q.effective_limit()), i64::from(q.offset));
    let mut stmt = conn.prepare_cached(TEXT_SEARCH).map_err(sql)?;
    search_hits(&mut stmt, params![phrase, limit, offset])
}

/// [`text_search`]'s query: the words, the limit, the offset. With fewer matches than the
/// page reaches the cut is minus infinity (`-9e999`), which lets every match through.
/// Materialized, each match is scored once, and the cut compares the very same values.
const TEXT_SEARCH: &str = "WITH m AS MATERIALIZED (SELECT rowid AS id, \
     -bm25(search_fts, 10.0, 1.0) AS score FROM search_fts WHERE search_fts MATCH ?1) \
     SELECT s.blake3, s.title, s.kind, s.doc_date, s.amount, m.score \
     FROM m JOIN search_index s ON s.rowid = m.id \
     WHERE m.score >= coalesce((SELECT score FROM m ORDER BY score DESC \
     LIMIT 1 OFFSET ?2 + ?3 - 1), -9e999) \
     ORDER BY m.score DESC, s.doc_date IS NULL, s.doc_date DESC, s.blake3 \
     LIMIT ?2 OFFSET ?3";

/// The hits of a search statement: hash, title, kind, date, amount and score.
fn search_hits(
    stmt: &mut rusqlite::Statement<'_>,
    args: impl rusqlite::Params,
) -> Result<Vec<SearchHit>> {
    let rows = stmt
        .query_map(args, |r| {
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
}

#[async_trait]
impl Backend for Standalone {
    fn name(&self) -> &'static str {
        "sqlite"
    }

    async fn ping(&self) -> Result<()> {
        self.run(|conn| {
            conn.prepare_cached("SELECT 1")
                .and_then(|mut cached| cached.query_row([], |_| Ok(())))
                .map_err(sql)
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    use doewood_core::MasterKey;

    use super::*;

    /// Single writes that return their row (`upsert_node`, `upsert_edge`) run SQLite's
    /// automatic checkpoint (issue #151): 3,000 of each, with no checkpoint of doewood's own,
    /// leave a WAL far under the 130 MiB and 33,000 frames they left before.
    #[tokio::test(flavor = "multi_thread")]
    async fn returning_writes_let_sqlite_checkpoint() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(crate::STORE_FILE);
        let mut store =
            Store::open_with_readers(&path, &MasterKey::from_bytes([1; 32]), 2).unwrap();
        store.wal = None;
        let store = Arc::new(store);
        let b = Standalone::new(Arc::clone(&store));
        let mut nodes = Vec::new();
        for i in 0..3000 {
            let props = serde_json::json!({"n": i});
            nodes.push(
                b.upsert_node(NodeKind::Company, &format!("c{i}"), &props)
                    .await
                    .unwrap(),
            );
        }
        for w in nodes.windows(2) {
            let edge = Edge::new(w[0], w[1], EdgeKind::SimilarTo, EdgeSource::Document);
            b.upsert_edge(&edge).await.unwrap();
        }
        let wal = std::fs::metadata(format!("{}-wal", path.display()))
            .unwrap()
            .len();
        assert!(wal < 16 << 20, "WAL of {} MiB", wal >> 20);
        // Their answers are still the rows written.
        let again = b
            .upsert_node(NodeKind::Company, "c7", &serde_json::json!({"n": 7}))
            .await
            .unwrap();
        assert_eq!(again, nodes[7]);
    }

    fn backend() -> Standalone {
        let store = Store::open_in_memory(&MasterKey::from_bytes([1; 32])).unwrap();
        Standalone::new(Arc::new(store))
    }

    /// Ingest's statements on a warm connection are statement cache hits: each is prepared
    /// once, then reused. A statement's run counter (`SQLITE_STMTSTATUS_RUN`) starts at 0
    /// when it is prepared and counts every reuse, so a run that starts at 0 is a prepare.
    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn hot_statements_are_prepared_once() {
        use std::collections::BTreeMap;
        use std::sync::Mutex;

        use rusqlite::StatementStatus;
        use rusqlite::trace::{TraceEvent, TraceEventCodes};

        use crate::{IngestOutcome, NewSource, ObjectChange, SeenObject};

        let store = Arc::new(Store::open_in_memory(&MasterKey::from_bytes([1; 32])).unwrap());
        let backend = Standalone::new(Arc::clone(&store));
        let source = store
            .add_source(&NewSource::new("http://objects.test:9000", "docs"), None)
            .unwrap();
        // SQL text -> (runs, prepares).
        let seen: Arc<Mutex<BTreeMap<String, (u32, u32)>>> = Arc::default();
        {
            let seen = Arc::clone(&seen);
            store
                .raw()
                .trace_v2(
                    TraceEventCodes::SQLITE_TRACE_STMT,
                    Some(move |e: TraceEvent<'_>| {
                        if let TraceEvent::Stmt(stmt, text) = e {
                            // Trigger programs report themselves as `-- TRIGGER name`.
                            if text.starts_with("--") {
                                return;
                            }
                            let mut seen = seen.lock().unwrap();
                            let entry = seen.entry(stmt.sql().into_owned()).or_default();
                            entry.0 += 1;
                            if stmt.get_status(StatementStatus::Run) == 0 {
                                entry.1 += 1;
                            }
                        }
                    }),
                )
                .unwrap();
        }
        let rounds = 20u8;
        for i in 0..rounds {
            let key = format!("docs/{i}.pdf");
            let hash = Blake3([i; 32]);
            store
                .record_objects(
                    source,
                    &[ObjectChange::Upsert(SeenObject {
                        key: key.clone(),
                        etag: Some("e".into()),
                        size: 1,
                        last_modified: None,
                    })],
                    None,
                )
                .unwrap();
            store.pending_objects(source, None, 64).unwrap();
            store.source(source).unwrap();
            store.ingest_status(source, &key).unwrap();
            backend
                .put_record(
                    &hash,
                    &RawValue::from_string(format!("{{\"i\":{i}}}")).unwrap(),
                    &BTreeMap::new(),
                )
                .await
                .unwrap();
            let doc = backend
                .upsert_node(NodeKind::Document, &hash.to_hex(), &serde_json::json!({}))
                .await
                .unwrap();
            let at = ObjectLocation {
                endpoint: "http://objects.test:9000".into(),
                bucket: "docs".into(),
                key: key.clone(),
            };
            backend.upsert_location(&hash, &at).await.unwrap();
            let company = backend
                .ensure_node(NodeKind::Company, &format!("pib:{i}"))
                .await
                .unwrap();
            backend
                .claim_alias(
                    NodeKind::Company,
                    &format!("pib:{i}"),
                    company,
                    &serde_json::json!({}),
                )
                .await
                .unwrap();
            backend
                .alias(NodeKind::Company, &format!("pib:{i}"))
                .await
                .unwrap();
            backend
                .upsert_edge(&Edge::new(
                    doc,
                    company,
                    EdgeKind::ParentOf,
                    EdgeSource::Registry,
                ))
                .await
                .unwrap();
            backend.node(doc).await.unwrap();
            backend
                .node_by_key(NodeKind::Document, &hash.to_hex())
                .await
                .unwrap();
            backend
                .index_document(&SearchDoc {
                    blake3: hash,
                    title: format!("t{i}"),
                    body: "b".into(),
                    kind: None,
                    date: None,
                    amount: None,
                })
                .await
                .unwrap();
            assert!(
                store
                    .finish_object(
                        source,
                        &key,
                        Some("e"),
                        &IngestOutcome::Done { blake3: hash.0 }
                    )
                    .unwrap()
            );
            // What a progress snapshot reads.
            store.sources().unwrap();
            store.object_counts(source).unwrap();
            store.scan_checkpoint(source).unwrap();
            store.llm_counts(source).unwrap();
            store.llm_usage(source, 0).unwrap();
        }
        let seen = seen.lock().unwrap();
        let (runs, prepares) = seen
            .iter()
            .filter(|(sql, _)| !sql.starts_with("BEGIN") && sql.as_str() != "COMMIT")
            .fold((0, 0), |(r, p), (_, (runs, prep))| (r + runs, p + prep));
        println!(
            "{} statements, {runs} runs, {prepares} prepares: {:.1}% cache hits \
             (BEGIN and COMMIT left out)",
            seen.len(),
            f64::from(runs - prepares) * 100.0 / f64::from(runs)
        );
        // Transactions begin and end through `execute_batch`, which has no cache.
        let again: Vec<_> = seen
            .iter()
            .filter(|(sql, (runs, prepares))| {
                *runs >= u32::from(rounds)
                    && *prepares > 1
                    && !matches!(
                        sql.as_str(),
                        "BEGIN DEFERRED" | "BEGIN IMMEDIATE" | "COMMIT"
                    )
            })
            .collect();
        assert!(again.is_empty(), "prepared more than once: {again:#?}");
        assert!(
            seen.len() <= crate::store::STATEMENT_CACHE,
            "fits the cache"
        );
    }

    #[tokio::test]
    async fn conformance() {
        let a = backend();
        // Claims are local: both "instances" share this one store.
        let b = a.clone();
        doewood_model::conformance::run(&a, &b).await;
    }

    /// The suite again on an on-disk store, where the reads run on read-only connections.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn conformance_with_readers() {
        let tmp = tempfile::tempdir().unwrap();
        let store = Store::open_with_readers(
            &tmp.path().join("s.sqlite"),
            &MasterKey::from_bytes([1; 32]),
            2,
        )
        .unwrap();
        assert_eq!(store.readers(), 2);
        let a = Standalone::new(Arc::new(store));
        let b = a.clone();
        doewood_model::conformance::run(&a, &b).await;
    }

    /// A read does not wait for a write transaction on the writer, and sees the state
    /// committed before it.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn reads_go_on_while_a_write_holds_the_writer() {
        let tmp = tempfile::tempdir().unwrap();
        let store = Arc::new(
            Store::open_with_readers(
                &tmp.path().join("s.sqlite"),
                &MasterKey::from_bytes([1; 32]),
                2,
            )
            .unwrap(),
        );
        let s = Standalone::new(Arc::clone(&store));
        let id = s
            .upsert_node(NodeKind::Company, "a", &serde_json::json!({"v": 1}))
            .await
            .unwrap();
        let (held, release) = std::sync::mpsc::channel::<()>();
        let (started, wait) = std::sync::mpsc::channel::<()>();
        let writer = {
            let store = Arc::clone(&store);
            std::thread::spawn(move || {
                let conn = store.conn();
                conn.execute_batch("BEGIN IMMEDIATE; UPDATE nodes SET props_json = '{\"v\":2}';")
                    .unwrap();
                started.send(()).unwrap();
                release.recv().unwrap();
                conn.execute_batch("COMMIT").unwrap();
            })
        };
        wait.recv().unwrap();
        let t = std::time::Instant::now();
        let node = tokio::time::timeout(Duration::from_secs(2), s.node(id))
            .await
            .expect("the read waited for the writer")
            .unwrap()
            .unwrap();
        assert_eq!(node.props, serde_json::json!({"v": 1}));
        let found = tokio::time::timeout(
            Duration::from_secs(2),
            s.node_by_key(NodeKind::Company, "a"),
        )
        .await
        .expect("the read waited for the writer")
        .unwrap();
        assert!(found.is_some());
        let took = t.elapsed();
        held.send(()).unwrap();
        writer.join().unwrap();
        assert!(took < Duration::from_secs(2), "{took:?}");
        // Committed now: the next read sees it.
        let node = s.node(id).await.unwrap().unwrap();
        assert_eq!(node.props, serde_json::json!({"v": 2}));
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
