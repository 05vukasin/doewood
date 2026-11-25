//! The store traits over Postgres. Every write is an `INSERT ... ON CONFLICT` upsert.

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::time::Duration;

use async_trait::async_trait;
use deadpool_postgres::{GenericClient, Object, Pool, PoolError};
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
use serde_json::Value;
use serde_json::value::RawValue;
use time::Date;
use tokio_postgres::Row;
use tokio_postgres::error::SqlState;
use tokio_postgres::types::{Json, ToSql};
use uuid::Uuid;

use crate::PgStore;
use crate::cached::Cached;
use crate::enrichment::DOCUMENTS;

/// The documents sharing the most bands of `$1`, other than `$2`, at most `$3` (issue #74).
/// The bands are counted and limited on `sketch_bands` alone (an index-only scan of its
/// primary key); only the survivors' signatures are read. `count(*)` is the number of shared
/// bands because `(band, blake3)` is the primary key, and every band row has its sketch (a
/// foreign key), so the join drops nothing.
pub(crate) const SKETCH_CANDIDATES: &str = "SELECT c.blake3, s.signature, c.shared \
     FROM (SELECT blake3, count(*) AS shared FROM sketch_bands \
     WHERE band = ANY($1) AND blake3 <> $2 \
     GROUP BY blake3 ORDER BY shared DESC, blake3 LIMIT $3) c \
     JOIN sketches s ON s.blake3 = c.blake3 ORDER BY c.shared DESC, c.blake3";

/// The `value.v` strings of the facts named `$2` in the records `$1` (`RecordStore::fact_texts`).
/// A `facts` that is not an array has no facts; a fact that is not an object, or whose key or
/// `value.v` is not a string, is left out.
const FACT_TEXTS: &str = "SELECT r.blake3, f->'value'->>'v' FROM records r \
     CROSS JOIN LATERAL jsonb_array_elements(CASE WHEN jsonb_typeof(r.record->'facts') = 'array' \
     THEN r.record->'facts' ELSE '[]'::jsonb END) f \
     WHERE r.blake3 = ANY($1) AND f->'key' = to_jsonb($2::text) \
     AND jsonb_typeof(f->'value'->'v') = 'string'";

/// One page of a source's records: `$1` endpoint, `$2` bucket, `$3` prefix, `$4` the hash the
/// page starts after (or NULL), `$5` the page size. The `after` bound is repeated inside the
/// EXISTS: Postgres plans this as a merge semi join over `records_pkey` and
/// `locations_by_blake3` and does not carry the bound across, so without it the locations side
/// reads every row before `after` and walking all pages is quadratic (issue #13, docs/api.md).
/// The prefix is matched in the "C" collation, so a selective prefix becomes a range scan of
/// `locations_by_key_c` (issue #27); `starts_with` gives the same answer in any collation.
/// Never cached: a generic plan would lose both bounds (see `cached.rs`).
pub(crate) const LIST_RECORDS: &str = "SELECT r.blake3, r.record, r.versions, extract(epoch FROM r.updated_at)::bigint \
     FROM records r WHERE ($4::bytea IS NULL OR r.blake3 > $4) \
     AND EXISTS (SELECT 1 FROM locations l WHERE l.blake3 = r.blake3 \
     AND l.endpoint = $1 AND l.bucket = $2 AND starts_with(l.object_key COLLATE \"C\", $3) \
     AND ($4::bytea IS NULL OR l.blake3 > $4)) \
     ORDER BY r.blake3 LIMIT $5";

/// Claim attempts when the owner releases between our insert and our read.
const CLAIM_ATTEMPTS: usize = 3;

// By value, so it reads `.map_err(pg)`.
#[allow(clippy::needless_pass_by_value)]
pub(crate) fn pg(e: tokio_postgres::Error) -> Error {
    if e.code() == Some(&SqlState::FOREIGN_KEY_VIOLATION) {
        return Error::NotFound("a node or record the write refers to does not exist".to_owned());
    }
    // Connection-level trouble: closed or failed sockets, and the server's own "come back
    // later" classes (08 connection exception, 53 insufficient resources, 57P operator
    // intervention such as a shutdown).
    let io =
        std::error::Error::source(&e).is_some_and(<dyn std::error::Error>::is::<std::io::Error>);
    let transient = e.code().is_some_and(|c| {
        let c = c.code();
        c.starts_with("08") || c.starts_with("53") || c.starts_with("57P")
    });
    if e.is_closed() || io || transient {
        return Error::Unavailable(e.to_string());
    }
    // The server's message, not tokio-postgres' bare "db error". Not the DETAIL, which can
    // quote row values.
    let message = e.as_db_error().map_or_else(
        || e.to_string(),
        |db| format!("{}: {}", db.code().code(), db.message()),
    );
    Error::Backend {
        backend: "postgres",
        message,
    }
}

pub(crate) async fn client(pool: &Pool) -> Result<Object> {
    pool.get().await.map_err(|e| match e {
        PoolError::Backend(e) => Error::Unavailable(e.to_string()),
        other => Error::Unavailable(other.to_string()),
    })
}

pub(crate) fn bad(what: &str, e: impl std::fmt::Display) -> Error {
    Error::Backend {
        backend: "postgres",
        message: format!("stored {what}: {e}"),
    }
}

pub(crate) fn parse<T: std::str::FromStr<Err = Error>>(text: &str) -> Result<T> {
    text.parse().map_err(|e: Error| bad("value", e))
}

/// Positional parameters for SQL built at run time.
#[derive(Default)]
struct Args(Vec<Box<dyn ToSql + Sync + Send>>);

impl Args {
    /// Adds a value and returns its placeholder.
    fn push(&mut self, value: impl ToSql + Sync + Send + 'static) -> String {
        self.0.push(Box::new(value));
        format!("${}", self.0.len())
    }

    fn refs(&self) -> Vec<&(dyn ToSql + Sync)> {
        self.0
            .iter()
            .map(|b| b.as_ref() as &(dyn ToSql + Sync))
            .collect()
    }
}

pub(crate) const NODE_COLUMNS: &str = "n.id, n.kind, n.key, n.props";

pub(crate) fn node_at(row: &Row, at: usize) -> Result<Node> {
    Ok(Node {
        id: row.try_get(at).map_err(pg)?,
        kind: parse(row.try_get::<_, &str>(at + 1).map_err(pg)?)?,
        key: row.try_get(at + 2).map_err(pg)?,
        props: row.try_get(at + 3).map_err(pg)?,
    })
}

pub(crate) const EDGE_COLUMNS: &str = "e.id, e.src, e.dst, e.kind, e.props, e.evidence, e.confidence, \
     e.state, e.valid_from, e.valid_to, e.source";

pub(crate) fn edge_at(row: &Row) -> Result<StoredEdge> {
    let get_text = |i| row.try_get::<_, &str>(i).map_err(pg);
    Ok(StoredEdge {
        id: row.try_get(0).map_err(pg)?,
        edge: Edge {
            src: row.try_get(1).map_err(pg)?,
            dst: row.try_get(2).map_err(pg)?,
            kind: parse(get_text(3)?)?,
            props: row.try_get(4).map_err(pg)?,
            evidence: row.try_get(5).map_err(pg)?,
            confidence: row.try_get(6).map_err(pg)?,
            state: parse(get_text(7)?)?,
            valid_from: row.try_get(8).map_err(pg)?,
            valid_to: row.try_get(9).map_err(pg)?,
            source: parse(get_text(10)?)?,
        },
    })
}

async fn upsert_node(
    c: &impl GenericClient,
    kind: NodeKind,
    key: &str,
    props: &Value,
) -> Result<Uuid> {
    let row = c
        .one(
            "INSERT INTO nodes (id, kind, key, props) VALUES ($1, $2, $3, $4) \
             ON CONFLICT ON CONSTRAINT nodes_kind_key \
             DO UPDATE SET props = EXCLUDED.props, updated_at = now() \
             RETURNING id",
            &[&Uuid::now_v7(), &kind.as_str(), &key, props],
        )
        .await?;
    row.try_get(0).map_err(pg)
}

/// Inserts the node if it is missing and leaves an existing one's props untouched.
async fn ensure_node(c: &impl GenericClient, kind: NodeKind, key: &str) -> Result<Uuid> {
    let row = c
        .one(
            "INSERT INTO nodes (id, kind, key) VALUES ($1, $2, $3) \
             ON CONFLICT ON CONSTRAINT nodes_kind_key DO UPDATE SET kind = EXCLUDED.kind \
             RETURNING id",
            &[&Uuid::now_v7(), &kind.as_str(), &key],
        )
        .await?;
    row.try_get(0).map_err(pg)
}

async fn upsert_edge(c: &impl GenericClient, e: &Edge) -> Result<Uuid> {
    e.validate()?;
    let row = c
        .one(
            "INSERT INTO edges (id, src, dst, kind, props, evidence, confidence, state, \
             valid_from, valid_to, source) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11) \
             ON CONFLICT ON CONSTRAINT edges_identity DO UPDATE SET \
             props = EXCLUDED.props, evidence = EXCLUDED.evidence, \
             confidence = EXCLUDED.confidence, state = EXCLUDED.state, \
             valid_to = EXCLUDED.valid_to, updated_at = now() \
             RETURNING id",
            &[
                &Uuid::now_v7(),
                &e.src,
                &e.dst,
                &e.kind.as_str(),
                &e.props,
                &e.evidence,
                &e.confidence,
                &e.state.as_str(),
                &e.valid_from,
                &e.valid_to,
                &e.source.as_str(),
            ],
        )
        .await?;
    row.try_get(0).map_err(pg)
}

/// Follows `merged_into` from `id` without locks, counting the hops in `hops` (at most
/// [`MERGE_CHAIN_MAX`]). A missing node is returned as it is: the edge write then fails.
async fn follow_merges(c: &impl GenericClient, mut id: Uuid, hops: &mut usize) -> Result<Uuid> {
    loop {
        let row = c
            .opt(
                "SELECT props->>'merged_into' FROM nodes WHERE id = $1",
                &[&id],
            )
            .await?;
        match row.and_then(|r| merged_into_column(&r)) {
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

/// Column 0 of `r`, `props->>'merged_into'`, as an id.
fn merged_into_column(r: &Row) -> Option<Uuid> {
    r.try_get::<_, Option<&str>>(0)
        .ok()
        .flatten()
        .and_then(|s| Uuid::parse_str(s).ok())
}

/// [`GraphStore::upsert_live_edge`]: find the live ends without locks, then, in one
/// transaction, lock them `FOR SHARE` and check they are still live before writing. A merge
/// locks its two companies `FOR UPDATE`, so the share lock waits for a merge in flight and
/// then reads what it committed; while the lock is held no merge of those companies can
/// start, so the edge is in place before any later merge moves the edges. When an end was
/// merged away in between, the transaction is rolled back (no lock is held while waiting for
/// another) and the walk goes on from there.
async fn upsert_live_edge(c: &mut Object, edge: &Edge) -> Result<LiveEdge> {
    let mut e = edge.clone();
    let (mut src_hops, mut dst_hops) = (0, 0);
    loop {
        e.src = follow_merges(&*c, e.src, &mut src_hops).await?;
        e.dst = follow_merges(&*c, e.dst, &mut dst_hops).await?;
        let tx = c.transaction().await.map_err(pg)?;
        let mut ends = vec![e.src, e.dst];
        ends.sort_unstable();
        ends.dedup();
        let rows = tx
            .rows(
                "SELECT props->>'merged_into' FROM nodes WHERE id = ANY($1) \
                 ORDER BY id FOR SHARE",
                &[&ends],
            )
            .await?;
        if rows.iter().all(|r| merged_into_column(r).is_none()) {
            let id = upsert_edge(&tx, &e).await?;
            tx.commit().await.map_err(pg)?;
            let redirects = u32::try_from(src_hops + dst_hops).unwrap_or(u32::MAX);
            return Ok(LiveEdge {
                id,
                src: e.src,
                dst: e.dst,
                redirects,
            });
        }
        // A merge committed between the walk and the lock: roll back, walk on.
        tx.rollback().await.map_err(pg)?;
    }
}

/// [`GraphStore::update_props`]; with `live`, a husk is refused
/// ([`GraphStore::update_live_props`]), checked under the same row lock as the write.
async fn update_props(
    pool: &Pool,
    id: Uuid,
    patch: &PropsPatch,
    live: bool,
) -> Result<Option<Node>> {
    patch.validate()?;
    let mut c = client(pool).await?;
    let tx = c.transaction().await.map_err(pg)?;
    // The row lock orders this after any other writer of the node (another patch, an
    // upsert, a merge) and lets it read what that writer committed.
    let Some(row) = tx
        .opt(
            &format!("SELECT {NODE_COLUMNS} FROM nodes n WHERE n.id = $1 FOR UPDATE"),
            &[&id],
        )
        .await?
    else {
        return Ok(None);
    };
    let mut node = node_at(&row, 0)?;
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
        tx.exec(
            "UPDATE nodes SET props = $2, updated_at = now() WHERE id = $1",
            &[&id, &node.props],
        )
        .await?;
    }
    tx.commit().await.map_err(pg)?;
    Ok(Some(node))
}

/// Locks node `id` `FOR SHARE` in the transaction `c` and refuses a husk with
/// [`Error::MergedAway`] (issue #62). A merge locks its companies `FOR UPDATE`, so this waits
/// for a merge in flight and reads what it committed, and no merge of the node starts until
/// the transaction ends. `false` when there is no node `id`.
pub(crate) async fn lock_live(c: &impl GenericClient, id: Uuid) -> Result<bool> {
    let row = c
        .opt(
            "SELECT props->>'merged_into' FROM nodes WHERE id = $1 FOR SHARE",
            &[&id],
        )
        .await?;
    match row {
        None => Ok(false),
        Some(r) => match merged_into_column(&r) {
            Some(into) => Err(Error::MergedAway { company: id, into }),
            None => Ok(true),
        },
    }
}

fn claim_from(key: &SourceKey, row: &Row) -> Result<SourceClaim> {
    let owner: &str = row.try_get(0).map_err(pg)?;
    Ok(SourceClaim {
        key: key.clone(),
        owner: InstanceId::new(owner).map_err(|e| bad("owner", e))?,
        claimed_at: row.try_get(1).map_err(pg)?,
    })
}

async fn source_owner(c: &impl GenericClient, key: &SourceKey) -> Result<Option<SourceClaim>> {
    c.opt(
        "SELECT owner, extract(epoch FROM claimed_at)::bigint FROM source_claims \
         WHERE endpoint = $1 AND bucket = $2 AND prefix = $3",
        &[&key.endpoint(), &key.bucket(), &key.prefix()],
    )
    .await?
    .map(|row| claim_from(key, &row))
    .transpose()
}

/// A stored record as the canonical text the stores return (#98). jsonb's own text has other
/// spacing and key order, so it goes through a `Value`, as the record did before.
fn record_text(record: &Value) -> Result<Box<RawValue>> {
    doewood_model::canonical::to_canonical(record).map_err(|e| Error::Backend {
        backend: "postgres",
        message: format!("stored record: {e}"),
    })
}

#[async_trait]
impl RecordStore for PgStore {
    async fn put_record(
        &self,
        blake3: &Blake3,
        body: &RawValue,
        versions: &StepVersions,
    ) -> Result<PutOutcome> {
        let c = client(&self.pool).await?;
        let row = c
            .opt(
                "INSERT INTO records (blake3, record, versions) VALUES ($1, $2, $3) \
                 ON CONFLICT (blake3) DO UPDATE SET record = EXCLUDED.record, \
                 versions = EXCLUDED.versions, updated_at = now() \
                 WHERE records.record IS DISTINCT FROM EXCLUDED.record \
                 OR records.versions IS DISTINCT FROM EXCLUDED.versions \
                 RETURNING (xmax = 0) AS inserted",
                &[
                    &blake3.as_bytes().as_slice(),
                    // The text as it is: jsonb parses it on the server.
                    &Json(body),
                    &versions_to_json(versions),
                ],
            )
            .await?;
        Ok(match row {
            None => PutOutcome::Unchanged,
            Some(r) if r.try_get::<_, bool>(0).map_err(pg)? => PutOutcome::Inserted,
            Some(_) => PutOutcome::Updated,
        })
    }

    async fn get_record(&self, blake3: &Blake3) -> Result<Option<Record>> {
        let c = client(&self.pool).await?;
        c.opt(
            "SELECT record, versions, extract(epoch FROM updated_at)::bigint FROM records \
             WHERE blake3 = $1",
            &[&blake3.as_bytes().as_slice()],
        )
        .await?
        .map(|r| {
            Ok(Record {
                blake3: *blake3,
                body: record_text(&r.try_get(0).map_err(pg)?)?,
                versions: versions_from_json(&r.try_get::<_, Value>(1).map_err(pg)?)?,
                updated_at: r.try_get(2).map_err(pg)?,
            })
        })
        .transpose()
    }

    async fn record_versions(&self, hashes: &[Blake3]) -> Result<BTreeMap<Blake3, StepVersions>> {
        validate_hashes(hashes)?;
        if hashes.is_empty() {
            return Ok(BTreeMap::new());
        }
        let keys: Vec<&[u8]> = hashes.iter().map(|h| h.as_bytes().as_slice()).collect();
        let c = client(&self.pool).await?;
        c.rows(
            "SELECT blake3, versions FROM records WHERE blake3 = ANY($1)",
            &[&keys],
        )
        .await?
        .iter()
        .map(|r| {
            Ok((
                Blake3::from_slice(r.try_get::<_, &[u8]>(0).map_err(pg)?)?,
                versions_from_json(&r.try_get::<_, Value>(1).map_err(pg)?)?,
            ))
        })
        .collect()
    }

    async fn fact_texts(&self, hashes: &[Blake3], key: &str) -> Result<Vec<(Blake3, String)>> {
        validate_hashes(hashes)?;
        if hashes.is_empty() {
            return Ok(Vec::new());
        }
        let keys: Vec<&[u8]> = hashes.iter().map(|h| h.as_bytes().as_slice()).collect();
        let c = client(&self.pool).await?;
        c.rows(FACT_TEXTS, &[&keys, &key])
            .await?
            .iter()
            .map(|r| {
                Ok((
                    Blake3::from_slice(r.try_get::<_, &[u8]>(0).map_err(pg)?)?,
                    r.try_get(1).map_err(pg)?,
                ))
            })
            .collect()
    }

    async fn list_records(
        &self,
        source: &SourceKey,
        after: Option<&Blake3>,
        limit: u32,
    ) -> Result<Vec<Record>> {
        let c = client(&self.pool).await?;
        let after = after.map(|h| h.as_bytes().to_vec());
        let rows = c
            .rows_uncached(
                LIST_RECORDS,
                &[
                    &source.endpoint(),
                    &source.bucket(),
                    &source.prefix(),
                    &after,
                    &i64::from(records_limit(limit)),
                ],
            )
            .await?;
        rows.iter()
            .map(|r| {
                Ok(Record {
                    blake3: Blake3::from_slice(r.try_get::<_, &[u8]>(0).map_err(pg)?)?,
                    body: record_text(&r.try_get(1).map_err(pg)?)?,
                    versions: versions_from_json(&r.try_get::<_, Value>(2).map_err(pg)?)?,
                    updated_at: r.try_get(3).map_err(pg)?,
                })
            })
            .collect()
    }
}

#[async_trait]
impl GraphStore for PgStore {
    async fn upsert_node(&self, kind: NodeKind, key: &str, props: &Value) -> Result<Uuid> {
        validate_node(kind, key, props)?;
        upsert_node(&client(&self.pool).await?, kind, key, props).await
    }

    async fn ensure_node(&self, kind: NodeKind, key: &str) -> Result<Uuid> {
        validate_node(kind, key, &Value::Object(serde_json::Map::new()))?;
        ensure_node(&client(&self.pool).await?, kind, key).await
    }

    async fn update_props(&self, id: Uuid, patch: &PropsPatch) -> Result<Option<Node>> {
        update_props(&self.pool, id, patch, false).await
    }

    async fn update_live_props(&self, id: Uuid, patch: &PropsPatch) -> Result<Option<Node>> {
        update_props(&self.pool, id, patch, true).await
    }

    async fn node(&self, id: Uuid) -> Result<Option<Node>> {
        let c = client(&self.pool).await?;
        c.opt(
            &format!("SELECT {NODE_COLUMNS} FROM nodes n WHERE n.id = $1"),
            &[&id],
        )
        .await?
        .map(|r| node_at(&r, 0))
        .transpose()
    }

    async fn node_by_key(&self, kind: NodeKind, key: &str) -> Result<Option<Node>> {
        let c = client(&self.pool).await?;
        c.opt(
            &format!("SELECT {NODE_COLUMNS} FROM nodes n WHERE n.kind = $1 AND n.key = $2"),
            &[&kind.as_str(), &key],
        )
        .await?
        .map(|r| node_at(&r, 0))
        .transpose()
    }

    async fn upsert_edge(&self, edge: &Edge) -> Result<Uuid> {
        edge.validate()?;
        upsert_edge(&client(&self.pool).await?, edge).await
    }

    async fn upsert_live_edge(&self, edge: &Edge) -> Result<LiveEdge> {
        edge.validate()?;
        upsert_live_edge(&mut client(&self.pool).await?, edge).await
    }

    async fn neighbours(&self, node: Uuid, q: &NeighbourQuery) -> Result<Vec<Neighbour>> {
        let (at_node, other) = match q.direction {
            Direction::Out => ("e.src = $1", "e.dst"),
            Direction::In => ("e.dst = $1", "e.src"),
            Direction::Both => (
                "(e.src = $1 OR e.dst = $1)",
                "(CASE WHEN e.src = $1 THEN e.dst ELSE e.src END)",
            ),
        };
        let mut args = Args::default();
        args.push(node);
        let mut sql = format!(
            "SELECT {EDGE_COLUMNS}, {NODE_COLUMNS} FROM edges e \
             JOIN nodes n ON n.id = {other} WHERE {at_node}"
        );
        if !q.edge_kinds.is_empty() {
            let kinds: Vec<String> = q.edge_kinds.iter().map(|k| k.as_str().to_owned()).collect();
            sql.push_str(" AND e.kind = ANY(");
            sql.push_str(&args.push(kinds));
            sql.push(')');
        }
        if let Some(kind) = q.node_kind {
            sql.push_str(" AND n.kind = ");
            sql.push_str(&args.push(kind.as_str()));
        }
        if let Some(at) = q.at {
            let p = args.push(at);
            sql.push_str(" AND (e.valid_from IS NULL OR e.valid_from <= ");
            sql.push_str(&p);
            sql.push_str(") AND (e.valid_to IS NULL OR e.valid_to > ");
            sql.push_str(&p);
            sql.push(')');
        }
        if !q.include_inactive {
            sql.push_str(" AND e.state NOT IN ('retracted', 'superseded')");
        }
        sql.push_str(" ORDER BY e.kind COLLATE \"C\", e.valid_from NULLS FIRST, e.id");
        let c = client(&self.pool).await?;
        // Cached: at most 48 shapes (direction and the four optional filters).
        let rows = c.rows(&sql, &args.refs()).await?;
        rows.iter()
            .map(|r| {
                Ok(Neighbour {
                    edge: edge_at(r)?,
                    node: node_at(r, 11)?,
                })
            })
            .collect()
    }

    async fn company_documents(&self, company: Uuid) -> Result<Vec<Node>> {
        let c = client(&self.pool).await?;
        let rows = c
            .rows(
                &format!(
                    "SELECT DISTINCT {NODE_COLUMNS}, n.key COLLATE \"C\" AS sort_key \
                     FROM edges e JOIN nodes n ON n.id = e.src \
                     WHERE e.dst = $1 AND e.kind = 'party' AND n.kind = 'document' \
                     AND e.state NOT IN ('retracted', 'superseded') ORDER BY sort_key"
                ),
                &[&company],
            )
            .await?;
        rows.iter().map(|r| node_at(r, 0)).collect()
    }

    async fn company_documents_first(&self, company: Uuid, limit: u32) -> Result<Vec<Node>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let c = client(&self.pool).await?;
        let rows = c
            .rows(
                &format!(
                    "SELECT {NODE_COLUMNS} FROM (SELECT DISTINCT n.id, n.key COLLATE \"C\" AS sort_key \
                     FROM edges e JOIN nodes n ON n.id = e.src \
                     WHERE e.dst = $1 AND e.kind = 'party' AND n.kind = 'document' \
                     AND e.state NOT IN ('retracted', 'superseded') \
                     ORDER BY sort_key LIMIT $2) t JOIN nodes n ON n.id = t.id ORDER BY t.sort_key"
                ),
                &[&company, &i64::from(limit)],
            )
            .await?;
        rows.iter().map(|r| node_at(r, 0)).collect()
    }

    async fn company_document_count(&self, company: Uuid) -> Result<u64> {
        let c = client(&self.pool).await?;
        let n: i64 = c
            .one(
                &format!("SELECT {DOCUMENTS} FROM (SELECT $1::uuid AS id) n"),
                &[&company],
            )
            .await?
            .try_get(0)
            .map_err(pg)?;
        Ok(u64::try_from(n).unwrap_or(0))
    }

    async fn upsert_location(&self, blake3: &Blake3, at: &ObjectLocation) -> Result<Located> {
        let at = &at.checked()?;
        let mut c = client(&self.pool).await?;
        let tx = c.transaction().await.map_err(pg)?;
        let document = ensure_node(&tx, NodeKind::Document, &blake3.to_hex()).await?;
        let props = serde_json::json!({
            "endpoint": at.endpoint, "bucket": at.bucket, "key": at.key,
        });
        let location = upsert_node(&tx, NodeKind::Location, &at.node_key(), &props).await?;
        tx.exec(
            "INSERT INTO locations (node_id, document_id, blake3, endpoint, bucket, object_key) \
             VALUES ($1, $2, $3, $4, $5, $6) \
             ON CONFLICT (node_id) DO UPDATE SET document_id = EXCLUDED.document_id, \
             blake3 = EXCLUDED.blake3, updated_at = now()",
            &[
                &location,
                &document,
                &blake3.as_bytes().as_slice(),
                &at.endpoint,
                &at.bucket,
                &at.key,
            ],
        )
        .await?;
        tx.exec(
            "UPDATE edges SET state = 'superseded', updated_at = now() \
             WHERE dst = $1 AND kind = 'located_at' AND src <> $2 AND state = 'active'",
            &[&location, &document],
        )
        .await?;
        upsert_edge(
            &tx,
            &Edge::new(document, location, EdgeKind::LocatedAt, EdgeSource::Storage),
        )
        .await?;
        tx.commit().await.map_err(pg)?;
        Ok(Located { document, location })
    }

    async fn end_location(&self, at: &ObjectLocation, on: Date) -> Result<Option<LocationEnded>> {
        let at = &at.checked()?;
        let mut c = client(&self.pool).await?;
        let tx = c.transaction().await.map_err(pg)?;
        let Some(row) = tx
            .opt(
                "DELETE FROM locations WHERE endpoint = $1 AND bucket = $2 AND object_key = $3 \
                 RETURNING node_id, document_id",
                &[&at.endpoint, &at.bucket, &at.key],
            )
            .await?
        else {
            return Ok(None);
        };
        let (location, document): (Uuid, Uuid) =
            (row.try_get(0).map_err(pg)?, row.try_get(1).map_err(pg)?);
        tx.exec(
            "UPDATE edges SET state = 'superseded', \
             valid_to = greatest(coalesce(valid_from, $2), $2), updated_at = now() \
             WHERE dst = $1 AND kind = 'located_at' \
             AND state NOT IN ('retracted', 'superseded')",
            &[&location, &on],
        )
        .await?;
        let remaining: i64 = tx
            .one(
                "SELECT count(*) FROM edges WHERE src = $1 AND kind = 'located_at' \
                 AND state NOT IN ('retracted', 'superseded')",
                &[&document],
            )
            .await?
            .try_get(0)
            .map_err(pg)?;
        tx.commit().await.map_err(pg)?;
        Ok(Some(LocationEnded {
            document,
            location,
            remaining: u32::try_from(remaining).unwrap_or(u32::MAX),
        }))
    }

    async fn claim_alias(
        &self,
        kind: NodeKind,
        alias: &str,
        node: Uuid,
        props: &Value,
    ) -> Result<NodeAlias> {
        validate_alias(alias, props)?;
        let c = client(&self.pool).await?;
        // A no-op update on conflict, so the stored row always comes back.
        let row = c
            .one(
                "INSERT INTO node_aliases (kind, alias, node_id, props) VALUES ($1, $2, $3, $4) \
                 ON CONFLICT (kind, alias) DO UPDATE SET kind = node_aliases.kind \
                 RETURNING node_id, props",
                &[&kind.as_str(), &alias, &node, props],
            )
            .await?;
        Ok(NodeAlias {
            node: row.try_get(0).map_err(pg)?,
            props: row.try_get(1).map_err(pg)?,
        })
    }

    async fn claim_live_alias(
        &self,
        kind: NodeKind,
        alias: &str,
        node: Uuid,
        props: &Value,
    ) -> Result<NodeAlias> {
        validate_alias(alias, props)?;
        let mut c = client(&self.pool).await?;
        let tx = c.transaction().await.map_err(pg)?;
        // A missing node: the insert below fails as `claim_alias` does.
        lock_live(&tx, node).await?;
        let row = tx
            .one(
                "INSERT INTO node_aliases (kind, alias, node_id, props) VALUES ($1, $2, $3, $4) \
                 ON CONFLICT (kind, alias) DO UPDATE SET kind = node_aliases.kind \
                 RETURNING node_id, props",
                &[&kind.as_str(), &alias, &node, props],
            )
            .await?;
        let got = NodeAlias {
            node: row.try_get(0).map_err(pg)?,
            props: row.try_get(1).map_err(pg)?,
        };
        tx.commit().await.map_err(pg)?;
        Ok(got)
    }

    async fn alias(&self, kind: NodeKind, alias: &str) -> Result<Option<NodeAlias>> {
        let c = client(&self.pool).await?;
        c.opt(
            "SELECT node_id, props FROM node_aliases WHERE kind = $1 AND alias = $2",
            &[&kind.as_str(), &alias],
        )
        .await?
        .map(|r| {
            Ok(NodeAlias {
                node: r.try_get(0).map_err(pg)?,
                props: r.try_get(1).map_err(pg)?,
            })
        })
        .transpose()
    }

    async fn block_alias(&self, kind: NodeKind, alias: &str, props: &Value) -> Result<()> {
        validate_alias(alias, props)?;
        let c = client(&self.pool).await?;
        c.exec(
            "INSERT INTO node_aliases (kind, alias, node_id, props) VALUES ($1, $2, NULL, $3) \
             ON CONFLICT (kind, alias) DO UPDATE SET node_id = NULL, props = EXCLUDED.props, \
             updated_at = now()",
            &[&kind.as_str(), &alias, props],
        )
        .await?;
        Ok(())
    }

    async fn aliases_of(&self, node: Uuid) -> Result<Vec<OwnedAlias>> {
        crate::merges::aliases_of(&client(&self.pool).await?, node).await
    }
}

#[async_trait]
impl SketchStore for PgStore {
    async fn put_sketch(&self, blake3: &Blake3, signature: &[u8], bands: &[u64]) -> Result<()> {
        validate_sketch(signature, bands)?;
        let key = blake3.as_bytes().as_slice();
        let bands: Vec<i64> = bands.iter().map(|b| band_to_i64(*b)).collect();
        let mut c = client(&self.pool).await?;
        let tx = c.transaction().await.map_err(pg)?;
        // Issue #74: an unchanged sketch writes nothing. The signature row is updated only
        // when it differs, and only the bands that changed are deleted or inserted.
        tx.exec(
            "INSERT INTO sketches (blake3, signature) VALUES ($1, $2) \
             ON CONFLICT (blake3) DO UPDATE SET signature = EXCLUDED.signature, \
             updated_at = now() WHERE sketches.signature IS DISTINCT FROM EXCLUDED.signature",
            &[&key, &signature],
        )
        .await?;
        tx.exec(
            "DELETE FROM sketch_bands WHERE blake3 = $1 AND band <> ALL($2::bigint[])",
            &[&key, &bands],
        )
        .await?;
        tx.exec(
            "INSERT INTO sketch_bands (band, blake3) \
             SELECT DISTINCT b, $2::bytea FROM unnest($1::bigint[]) AS b \
             ON CONFLICT DO NOTHING",
            &[&bands, &key],
        )
        .await?;
        tx.commit().await.map_err(pg)
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
        let bands: Vec<i64> = bands.iter().map(|b| band_to_i64(*b)).collect();
        let c = client(&self.pool).await?;
        let rows = c
            .rows(
                SKETCH_CANDIDATES,
                &[
                    &bands,
                    &exclude.as_bytes().as_slice(),
                    &i64::from(sketch_limit(limit)),
                ],
            )
            .await?;
        rows.iter()
            .map(|r| {
                Ok(Sketch {
                    blake3: Blake3::from_slice(r.try_get::<_, &[u8]>(0).map_err(pg)?)?,
                    signature: r.try_get(1).map_err(pg)?,
                    shared_bands: u32::try_from(r.try_get::<_, i64>(2).map_err(pg)?)
                        .unwrap_or(u32::MAX),
                })
            })
            .collect()
    }
}

#[async_trait]
impl ClaimStore for PgStore {
    async fn claim_source(&self, key: &SourceKey, owner: &InstanceId) -> Result<SourceClaim> {
        let c = client(&self.pool).await?;
        for _ in 0..CLAIM_ATTEMPTS {
            // A no-op update on our own claim, so that it is returned; none on another's.
            let mine = c
                .opt(
                    "INSERT INTO source_claims (endpoint, bucket, prefix, owner) \
                     VALUES ($1, $2, $3, $4) \
                     ON CONFLICT ON CONSTRAINT source_claims_key \
                     DO UPDATE SET owner = source_claims.owner \
                     WHERE source_claims.owner = EXCLUDED.owner \
                     RETURNING owner, extract(epoch FROM claimed_at)::bigint",
                    &[
                        &key.endpoint(),
                        &key.bucket(),
                        &key.prefix(),
                        &owner.as_str(),
                    ],
                )
                .await?;
            if let Some(row) = mine {
                return claim_from(key, &row);
            }
            if let Some(claim) = source_owner(&c, key).await? {
                return Err(Error::SourceClaimed {
                    key: key.clone(),
                    owner: claim.owner,
                });
            }
            // Released in between: try again.
        }
        Err(Error::Unavailable(format!(
            "source {key} changed owner {CLAIM_ATTEMPTS} times while claiming it"
        )))
    }

    async fn release_source(&self, key: &SourceKey, owner: &InstanceId) -> Result<bool> {
        let c = client(&self.pool).await?;
        let n = c
            .exec(
                "DELETE FROM source_claims \
                 WHERE endpoint = $1 AND bucket = $2 AND prefix = $3 AND owner = $4",
                &[
                    &key.endpoint(),
                    &key.bucket(),
                    &key.prefix(),
                    &owner.as_str(),
                ],
            )
            .await?;
        if n > 0 {
            return Ok(true);
        }
        match source_owner(&c, key).await? {
            Some(claim) => Err(Error::SourceClaimed {
                key: key.clone(),
                owner: claim.owner,
            }),
            None => Ok(false),
        }
    }

    async fn source_owner(&self, key: &SourceKey) -> Result<Option<SourceClaim>> {
        source_owner(&client(&self.pool).await?, key).await
    }

    async fn lease_company(
        &self,
        company: Uuid,
        owner: &InstanceId,
        ttl: Duration,
    ) -> Result<bool> {
        let c = client(&self.pool).await?;
        let won = c
            .opt(
                "INSERT INTO company_leases (company_id, owner, leased_at, expires_at) \
                 VALUES ($1, $2, now(), now() + make_interval(secs => $3)) \
                 ON CONFLICT (company_id) DO UPDATE SET owner = EXCLUDED.owner, \
                 leased_at = EXCLUDED.leased_at, expires_at = EXCLUDED.expires_at \
                 WHERE company_leases.owner = EXCLUDED.owner \
                 OR company_leases.expires_at <= now() \
                 RETURNING owner",
                &[&company, &owner.as_str(), &ttl.as_secs_f64()],
            )
            .await?;
        Ok(won.is_some())
    }

    async fn release_company(&self, company: Uuid, owner: &InstanceId) -> Result<bool> {
        let c = client(&self.pool).await?;
        let n = c
            .exec(
                "DELETE FROM company_leases WHERE company_id = $1 AND owner = $2",
                &[&company, &owner.as_str()],
            )
            .await?;
        Ok(n > 0)
    }
}

#[async_trait]
impl SearchStore for PgStore {
    async fn index_document(&self, doc: &SearchDoc) -> Result<()> {
        let c = client(&self.pool).await?;
        c.exec(
            "INSERT INTO search_index (blake3, title, body, kind, doc_date, amount) \
             VALUES ($1, $2, $3, $4, $5, $6) \
             ON CONFLICT (blake3) DO UPDATE SET title = EXCLUDED.title, body = EXCLUDED.body, \
             kind = EXCLUDED.kind, doc_date = EXCLUDED.doc_date, amount = EXCLUDED.amount, \
             indexed_at = now() \
             WHERE (search_index.title, search_index.body, search_index.kind, \
             search_index.doc_date, search_index.amount) IS DISTINCT FROM \
             (EXCLUDED.title, EXCLUDED.body, EXCLUDED.kind, EXCLUDED.doc_date, EXCLUDED.amount)",
            &[
                &doc.blake3.as_bytes().as_slice(),
                &doc.title,
                &doc.body,
                &doc.kind,
                &doc.date,
                &doc.amount,
            ],
        )
        .await?;
        Ok(())
    }

    async fn search(&self, q: &SearchQuery) -> Result<Vec<SearchHit>> {
        let words = q.words();
        let mut args = Args::default();
        let mut sql = if words.is_empty() {
            "SELECT s.blake3, s.title, s.kind, s.doc_date, s.amount, 0::real AS score \
             FROM search_index s WHERE true"
                .to_owned()
        } else {
            // plainto_tsquery: the words are ANDed, and no query syntax is interpreted.
            let p = args.push(words.join(" "));
            format!(
                "SELECT s.blake3, s.title, s.kind, s.doc_date, s.amount, \
                 ts_rank(s.tsv, plainto_tsquery('simple', {p})) AS score \
                 FROM search_index s WHERE s.tsv @@ plainto_tsquery('simple', {p})"
            )
        };
        if let Some(company) = q.company {
            let p = args.push(company);
            let _ = write!(
                sql,
                " AND EXISTS (SELECT 1 FROM nodes d JOIN edges e ON e.src = d.id \
                 WHERE d.kind = 'document' AND d.key = encode(s.blake3, 'hex') \
                 AND e.kind = 'party' AND e.dst = {p} \
                 AND e.state NOT IN ('retracted', 'superseded'))"
            );
        }
        let mut filter = |cond: &str, value: Box<dyn ToSql + Sync + Send>| {
            args.0.push(value);
            let _ = write!(sql, " AND {cond} ${}", args.0.len());
        };
        if let Some(kind) = &q.kind {
            filter("s.kind =", Box::new(kind.clone()));
        }
        if let Some(d) = q.date_from {
            filter("s.doc_date >=", Box::new(d));
        }
        if let Some(d) = q.date_to {
            filter("s.doc_date <=", Box::new(d));
        }
        if let Some(a) = q.amount_min {
            filter("s.amount >=", Box::new(a));
        }
        if let Some(a) = q.amount_max {
            filter("s.amount <=", Box::new(a));
        }
        if let Some(src) = &q.source {
            let (e, b, p) = (
                args.push(src.endpoint().to_owned()),
                args.push(src.bucket().to_owned()),
                args.push(src.prefix().to_owned()),
            );
            let _ = write!(
                sql,
                " AND EXISTS (SELECT 1 FROM locations l WHERE l.blake3 = s.blake3 \
                 AND l.endpoint = {e} AND l.bucket = {b} \
                 AND starts_with(l.object_key COLLATE \"C\", {p}))"
            );
        }
        let (limit, offset) = (
            args.push(i64::from(q.effective_limit())),
            args.push(i64::from(q.offset)),
        );
        let _ = write!(
            sql,
            " ORDER BY score DESC, s.doc_date DESC NULLS LAST, s.blake3 \
             LIMIT {limit} OFFSET {offset}"
        );
        let c = client(&self.pool).await?;
        // Not cached: up to 256 shapes (one per combination of filters), user-driven and rare
        // next to the ingest statements, and a generic plan could not use the source prefix.
        let rows = c.query(&sql, &args.refs()).await.map_err(pg)?;
        rows.iter()
            .map(|r| {
                Ok(SearchHit {
                    blake3: Blake3::from_slice(r.try_get::<_, &[u8]>(0).map_err(pg)?)?,
                    title: r.try_get(1).map_err(pg)?,
                    kind: r.try_get(2).map_err(pg)?,
                    date: r.try_get(3).map_err(pg)?,
                    amount: r.try_get(4).map_err(pg)?,
                    score: r.try_get(5).map_err(pg)?,
                })
            })
            .collect()
    }
}

#[async_trait]
impl Backend for PgStore {
    fn name(&self) -> &'static str {
        "postgres"
    }

    async fn ping(&self) -> Result<()> {
        let c = client(&self.pool).await?;
        c.simple_query("SELECT 1").await.map_err(pg)?;
        Ok(())
    }
}
