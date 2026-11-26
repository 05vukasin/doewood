//! The store traits over Postgres. Every write is an `INSERT ... ON CONFLICT` upsert.

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::time::Duration;

use async_trait::async_trait;
use deadpool_postgres::{GenericClient, Object, Pool, PoolError};
use doewood_core::InstanceId;
use doewood_model::{
    Backend, Blake3, Candidates, ClaimStore, Direction, Edge, EdgeKind, EdgeSource, Error,
    GraphStore, LiveEdge, Located, LocationEnded, MERGE_CHAIN_MAX, Neighbour, NeighbourQuery, Node,
    NodeAlias, NodeKind, ObjectLocation, OwnedAlias, PropsPatch, PutOutcome, Record, RecordStore,
    Result, SKETCH_MAX_BANDS, SearchDoc, SearchHit, SearchQuery, SearchStore, Sketch, SketchStore,
    SourceClaim, SourceKey, StepVersions, StoredEdge, band_to_i64, merged_into, record_page_bytes,
    records_limit, sketch_limit, validate_alias, validate_alias_count, validate_batch_count,
    validate_hashes, validate_node, validate_sketch, versions_from_json, versions_to_json,
};
use futures_util::future::try_join_all;
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

/// [`SKETCH_CANDIDATES`] over the bands of `$1` whose bucket holds at most `$4` documents
/// (issue #148): `big` reads one `sketch_band_sizes` row per band, and the bands left are
/// counted as before. The first column is the number of skipped bands, on every row; with no
/// candidate there is one row, the others NULL.
pub(crate) const SKETCH_CANDIDATES_WITHIN: &str = "WITH big AS MATERIALIZED (\
     SELECT band FROM sketch_band_sizes WHERE band = ANY($1) AND docs > $4::bigint), \
     c AS (SELECT blake3, count(*) AS shared FROM sketch_bands \
     WHERE band = ANY(ARRAY(SELECT unnest($1::bigint[]) EXCEPT SELECT band FROM big)) \
     AND blake3 <> $2 \
     GROUP BY blake3 ORDER BY shared DESC, blake3 LIMIT $3) \
     SELECT (SELECT count(*) FROM big), c.blake3, s.signature, c.shared \
     FROM (SELECT 1) one LEFT JOIN (c JOIN sketches s ON s.blake3 = c.blake3) ON true \
     ORDER BY c.shared DESC, c.blake3";

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

/// Whether company `$1` has fewer `party` edges than a quarter of the indexed documents (at
/// least 1000), reading at most that many index entries. Search by such a company starts from
/// its edges and looks each document up; for a bigger one, checking the indexed documents in
/// rank order until the page is full is faster (issue #111, docs/api.md).
pub(crate) const FEW_PARTIES: &str = "WITH lim AS (SELECT greatest(1000, \
     (SELECT reltuples::bigint FROM pg_class WHERE oid = 'search_index'::regclass) / 4) AS n) \
     SELECT (SELECT count(*) FROM (SELECT 1 FROM edges WHERE dst = $1 AND kind = 'party' \
     LIMIT (SELECT n FROM lim)) x) < (SELECT n FROM lim)";

/// The rows of the first batch [`LIST_RECORDS`] reads from its portal.
const LIST_RECORDS_BATCH: usize = 8;

/// [`GraphStore::upsert_location`] in one statement (issue #103; before, a transaction of five):
/// the Document node (`$1`, `$2`) if missing, the Location node (`$3` to `$5`) with its props,
/// its `locations` row (`$6` to `$9`), the `located_at` edges from other documents to it
/// superseded (the object was overwritten), and the `located_at` edge (`$10` to `$16`). The
/// parts touch different rows, and the foreign keys are checked at the end of the statement,
/// when the new nodes are there. Followed by [`EDGE_ON_CONFLICT`] and the final select.
const UPSERT_LOCATION: &str = "WITH doc AS (INSERT INTO nodes (id, kind, key) \
     VALUES ($1, 'document', $2) \
     ON CONFLICT ON CONSTRAINT nodes_kind_key DO UPDATE SET kind = EXCLUDED.kind RETURNING id), \
     loc AS (INSERT INTO nodes (id, kind, key, props) VALUES ($3, 'location', $4, $5) \
     ON CONFLICT ON CONSTRAINT nodes_kind_key \
     DO UPDATE SET props = EXCLUDED.props, updated_at = now() RETURNING id), \
     row AS (INSERT INTO locations (node_id, document_id, blake3, endpoint, bucket, object_key) \
     SELECT loc.id, doc.id, $6::bytea, $7::text, $8::text, $9::text FROM loc, doc \
     ON CONFLICT (node_id) DO UPDATE SET document_id = EXCLUDED.document_id, \
     blake3 = EXCLUDED.blake3, updated_at = now()), \
     moved AS (UPDATE edges SET state = 'superseded', updated_at = now() \
     WHERE dst = (SELECT id FROM loc) AND kind = 'located_at' \
     AND src <> (SELECT id FROM doc) AND state = 'active'), \
     edge AS (INSERT INTO edges (id, src, dst, kind, props, evidence, confidence, state, \
     valid_from, valid_to, source) \
     SELECT $10::uuid, doc.id, loc.id, 'located_at', $11::jsonb, $12::jsonb, $13::real, \
     $14::text, $15::date, $16::date, 'storage' FROM doc, loc ";

/// [`SketchStore::put_sketch`] in one statement (issue #103; before, a transaction of three):
/// the signature row, updated only when it differs (issue #74), the bands no longer in `$3`
/// deleted and the new ones inserted. The deleted and inserted bands are different rows, and
/// the bands' foreign key is checked at the end of the statement, when the sketch is there.
const PUT_SKETCH: &str = "WITH sketch AS (INSERT INTO sketches (blake3, signature) VALUES ($1, $2) \
     ON CONFLICT (blake3) DO UPDATE SET signature = EXCLUDED.signature, \
     updated_at = now() WHERE sketches.signature IS DISTINCT FROM EXCLUDED.signature), \
     gone AS (DELETE FROM sketch_bands WHERE blake3 = $1 AND band <> ALL($3::bigint[])) \
     INSERT INTO sketch_bands (band, blake3) \
     SELECT DISTINCT b, $1::bytea FROM unnest($3::bigint[]) AS b ON CONFLICT DO NOTHING";

/// One [`GraphStore::claim_alias`]: a no-op update on conflict, so the stored row always comes
/// back.
const CLAIM_ALIAS: &str = "INSERT INTO node_aliases (kind, alias, node_id, props) \
     VALUES ($1, $2, $3, $4) \
     ON CONFLICT (kind, alias) DO UPDATE SET kind = node_aliases.kind \
     RETURNING node_id, props";

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

/// A node alias from columns `at` (`node_id`) and `at + 1` (`props`).
fn alias_at(row: &Row, at: usize) -> Result<NodeAlias> {
    Ok(NodeAlias {
        node: row.try_get(at).map_err(pg)?,
        props: row.try_get(at + 1).map_err(pg)?,
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

/// What an edge upsert updates on conflict with the edge of the same identity.
const EDGE_ON_CONFLICT: &str = "ON CONFLICT ON CONSTRAINT edges_identity DO UPDATE SET \
     props = EXCLUDED.props, evidence = EXCLUDED.evidence, \
     confidence = EXCLUDED.confidence, state = EXCLUDED.state, \
     valid_to = EXCLUDED.valid_to, updated_at = now() \
     RETURNING id";

/// The edge upsert, `$1` the id for a new edge, `$2` to `$11` the edge.
fn upsert_edge_sql() -> String {
    format!(
        "INSERT INTO edges (id, src, dst, kind, props, evidence, confidence, state, \
         valid_from, valid_to, source) \
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11) {EDGE_ON_CONFLICT}"
    )
}

/// What [`edge_params`] takes besides the edge: the id for a new edge and the edge's kind,
/// state and source as text.
type EdgeExtra = (Uuid, &'static str, &'static str, &'static str);

fn edge_extra(e: &Edge) -> EdgeExtra {
    (
        Uuid::now_v7(),
        e.kind.as_str(),
        e.state.as_str(),
        e.source.as_str(),
    )
}

/// The parameters of [`upsert_edge_sql`] for `e`.
fn edge_params<'a>(e: &'a Edge, x: &'a EdgeExtra) -> [&'a (dyn ToSql + Sync); 11] {
    [
        &x.0,
        &e.src,
        &e.dst,
        &x.1,
        &e.props,
        &e.evidence,
        &e.confidence,
        &x.2,
        &e.valid_from,
        &e.valid_to,
        &x.3,
    ]
}

/// [`GraphStore::upsert_edges`] in one statement: `$1` the ids for new edges, `$2` to `$11`
/// the edges' columns as arrays. Returns each edge's id with its identity.
fn upsert_edges_sql() -> String {
    format!(
        "INSERT INTO edges (id, src, dst, kind, props, evidence, confidence, state, \
         valid_from, valid_to, source) \
         SELECT * FROM unnest($1::uuid[], $2::uuid[], $3::uuid[], $4::text[], $5::jsonb[], \
         $6::jsonb[], $7::real[], $8::text[], $9::date[], $10::date[], $11::text[]) \
         {EDGE_ON_CONFLICT}, src, dst, kind, source, valid_from"
    )
}

/// The columns of the `edges_identity` constraint.
fn edge_identity(e: &Edge) -> (Uuid, Uuid, &'static str, &'static str, Option<Date>) {
    (
        e.src,
        e.dst,
        e.kind.as_str(),
        e.source.as_str(),
        e.valid_from,
    )
}

async fn upsert_edge(c: &impl GenericClient, e: &Edge) -> Result<Uuid> {
    e.validate()?;
    let x = edge_extra(e);
    let row = c.one(&upsert_edge_sql(), &edge_params(e, &x)).await?;
    row.try_get(0).map_err(pg)
}

/// Column `at` of `r`, a `props->>'merged_into'`, as an id.
fn merged_into_column(r: &Row, at: usize) -> Option<Uuid> {
    r.try_get::<_, Option<&str>>(at)
        .ok()
        .flatten()
        .and_then(|s| Uuid::parse_str(s).ok())
}

/// One attempt of [`GraphStore::upsert_live_edge`], in one statement (issue #103): `ends`
/// locks the edge's ends (`$12`, sorted) `FOR SHARE`, and the edge is written only when
/// neither is a husk. Returns the edge's id (NULL when it was not written) and each end's
/// `merged_into`. A merge locks its companies `FOR UPDATE`, so the share lock waits for a
/// merge in flight and then reads the row as it committed.
const LIVE_EDGE: &str = "WITH ends AS MATERIALIZED (\
     SELECT id, props->>'merged_into' AS m FROM nodes WHERE id = ANY($12::uuid[]) \
     ORDER BY id FOR SHARE), \
     ins AS (INSERT INTO edges (id, src, dst, kind, props, evidence, confidence, state, \
     valid_from, valid_to, source) \
     SELECT $1::uuid, $2::uuid, $3::uuid, $4::text, $5::jsonb, $6::jsonb, $7::real, $8::text, \
     $9::date, $10::date, $11::text \
     WHERE NOT EXISTS (SELECT 1 FROM ends WHERE m IS NOT NULL) ";

/// [`GraphStore::upsert_live_edge`]: lock the ends `FOR SHARE` and write the edge when both
/// are live, in one statement ([`LIVE_EDGE`]); while the lock is held no merge of those
/// companies can start, so the edge is in place before any later merge moves the edges. When
/// an end is a husk, nothing is written (no lock is held while waiting for another) and the
/// attempt is repeated one `merged_into` hop on: the walk and the check are one round trip
/// per hop, and a write to live ends (nearly every write) is one round trip in all.
async fn upsert_live_edge(c: &impl GenericClient, edge: &Edge) -> Result<LiveEdge> {
    let mut e = edge.clone();
    let (mut src_hops, mut dst_hops) = (0, 0);
    let sql = format!(
        "{LIVE_EDGE}{EDGE_ON_CONFLICT}) SELECT (SELECT id FROM ins), \
         (SELECT m FROM ends WHERE id = $2), (SELECT m FROM ends WHERE id = $3)"
    );
    loop {
        let mut ends = vec![e.src, e.dst];
        ends.sort_unstable();
        ends.dedup();
        let row = c
            .one(
                &sql,
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
                    &ends,
                ],
            )
            .await?;
        if let Some(id) = row.try_get::<_, Option<Uuid>>(0).map_err(pg)? {
            let redirects = u32::try_from(src_hops + dst_hops).unwrap_or(u32::MAX);
            return Ok(LiveEdge {
                id,
                src: e.src,
                dst: e.dst,
                redirects,
            });
        }
        // An end is a husk: walk on from it.
        let mut moved = false;
        for (at, end, hops) in [
            (1, &mut e.src, &mut src_hops),
            (2, &mut e.dst, &mut dst_hops),
        ] {
            if let Some(next) = merged_into_column(&row, at) {
                *hops += 1;
                if *hops > MERGE_CHAIN_MAX {
                    return Err(Error::Precondition(format!(
                        "node {end} has more than {MERGE_CHAIN_MAX} merges in a chain"
                    )));
                }
                *end = next;
                moved = true;
            }
        }
        if !moved {
            return Err(bad("merged_into", "not a node id"));
        }
    }
}

/// [`GraphStore::update_props`]; with `live`, a husk is refused
/// ([`GraphStore::update_live_props`]).
///
/// A compare-and-set, two round trips (issue #103): read the node with its row version
/// (`xmin`, which every write of the row changes), apply the patch, and write the result only
/// if the row is still that version. When another writer (another patch, an upsert, a merge)
/// wrote the row in between, the update waits for its row lock, finds another version and
/// writes nothing, and the patch is applied again on what that writer committed. So no write is lost, and a merge that marks the node a husk is seen
/// before anything is written to it. A patch that changes nothing writes nothing.
async fn update_props(
    pool: &Pool,
    id: Uuid,
    patch: &PropsPatch,
    live: bool,
) -> Result<Option<Node>> {
    patch.validate()?;
    let c = client(pool).await?;
    loop {
        let Some(row) = c
            .opt(
                &format!("SELECT {NODE_COLUMNS}, n.xmin::text FROM nodes n WHERE n.id = $1"),
                &[&id],
            )
            .await?
        else {
            return Ok(None);
        };
        let mut node = node_at(&row, 0)?;
        let version: &str = row.try_get(4).map_err(pg)?;
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
        if node.props == before {
            return Ok(Some(node));
        }
        let written = c
            .exec(
                "UPDATE nodes SET props = $3, updated_at = now() \
                 WHERE id = $1 AND xmin::text = $2",
                &[&id, &version, &node.props],
            )
            .await?;
        if written == 1 {
            return Ok(Some(node));
        }
        // Written (or deleted) since the read: read again.
    }
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
        Some(r) => match merged_into_column(&r, 0) {
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
        max_bytes: usize,
    ) -> Result<Vec<Record>> {
        let limit = records_limit(limit) as usize;
        let mut c = client(&self.pool).await?;
        let after = after.map(|h| h.as_bytes().to_vec());
        // Read through a portal a few rows at a time, so that a page stops at the byte budget
        // without the server sending, or this process holding, the rest (issue #111). Never
        // cached (see `LIST_RECORDS`): a fresh statement gets a custom plan.
        let tx = c
            .build_transaction()
            .read_only(true)
            .start()
            .await
            .map_err(pg)?;
        let st = tx.prepare(LIST_RECORDS).await.map_err(pg)?;
        let portal = tx
            .bind(
                &st,
                &[
                    &source.endpoint(),
                    &source.bucket(),
                    &source.prefix(),
                    &after,
                    &i64::try_from(limit).unwrap_or(i64::MAX),
                ],
            )
            .await
            .map_err(pg)?;
        let (mut page, mut bytes) = (Vec::new(), 0usize);
        // The first batch; later ones are sized from the mean record so far to about what
        // still fits.
        let mut batch = LIST_RECORDS_BATCH.min(limit);
        'page: loop {
            let rows = tx
                .query_portal(&portal, i32::try_from(batch).unwrap_or(i32::MAX))
                .await
                .map_err(pg)?;
            let done = rows.len() < batch;
            for r in &rows {
                let record = Record {
                    blake3: Blake3::from_slice(r.try_get::<_, &[u8]>(0).map_err(pg)?)?,
                    body: record_text(&r.try_get(1).map_err(pg)?)?,
                    versions: versions_from_json(&r.try_get::<_, Value>(2).map_err(pg)?)?,
                    updated_at: r.try_get(3).map_err(pg)?,
                };
                bytes += record_page_bytes(&record);
                page.push(record);
                if bytes >= max_bytes {
                    break 'page;
                }
            }
            if done || page.len() >= limit {
                break;
            }
            let mean = (bytes / page.len()).max(1);
            batch = ((max_bytes - bytes) / mean + 1).clamp(1, limit - page.len());
        }
        drop(portal);
        tx.commit().await.map_err(pg)?;
        Ok(page)
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

    async fn node_ids(&self, kind: NodeKind, keys: &[String]) -> Result<Vec<Option<Uuid>>> {
        validate_batch_count(keys.len())?;
        let mut out = vec![None; keys.len()];
        if keys.is_empty() {
            return Ok(out);
        }
        let c = client(&self.pool).await?;
        let rows = c
            .rows(
                "SELECT a.ord, n.id FROM unnest($2::text[]) WITH ORDINALITY AS a(key, ord) \
                 JOIN nodes n ON n.kind = $1 AND n.key = a.key",
                &[&kind.as_str(), &keys],
            )
            .await?;
        for r in &rows {
            let ord: i64 = r.try_get(0).map_err(pg)?;
            let at = usize::try_from(ord - 1).map_err(|e| bad("ordinal", e))?;
            *out.get_mut(at).ok_or_else(|| bad("ordinal", ord))? = Some(r.try_get(1).map_err(pg)?);
        }
        Ok(out)
    }

    async fn upsert_edge(&self, edge: &Edge) -> Result<Uuid> {
        edge.validate()?;
        upsert_edge(&client(&self.pool).await?, edge).await
    }

    async fn upsert_edges(&self, edges: &[Edge]) -> Result<Vec<Uuid>> {
        validate_batch_count(edges.len())?;
        for e in edges {
            e.validate()?;
        }
        if edges.is_empty() {
            return Ok(Vec::new());
        }
        // One statement for all of them (issues #126 and #148): one round trip and one
        // transaction however the client's writes reach the server. Two edges of one
        // identity cannot be upserted by one statement, so such a batch goes one by one, in
        // one transaction.
        let mut seen = std::collections::HashSet::with_capacity(edges.len());
        if !edges.iter().all(|e| seen.insert(edge_identity(e))) {
            let mut c = client(&self.pool).await?;
            let tx = c.transaction().await.map_err(pg)?;
            let mut ids = Vec::with_capacity(edges.len());
            for e in edges {
                ids.push(upsert_edge(&tx, e).await?);
            }
            tx.commit().await.map_err(pg)?;
            return Ok(ids);
        }
        let ids: Vec<Uuid> = edges.iter().map(|_| Uuid::now_v7()).collect();
        let src: Vec<Uuid> = edges.iter().map(|e| e.src).collect();
        let dst: Vec<Uuid> = edges.iter().map(|e| e.dst).collect();
        let kind: Vec<&str> = edges.iter().map(|e| e.kind.as_str()).collect();
        let props: Vec<&Value> = edges.iter().map(|e| &e.props).collect();
        let evidence: Vec<&Value> = edges.iter().map(|e| &e.evidence).collect();
        let confidence: Vec<f32> = edges.iter().map(|e| e.confidence).collect();
        let state: Vec<&str> = edges.iter().map(|e| e.state.as_str()).collect();
        let valid_from: Vec<Option<Date>> = edges.iter().map(|e| e.valid_from).collect();
        let valid_to: Vec<Option<Date>> = edges.iter().map(|e| e.valid_to).collect();
        let source: Vec<&str> = edges.iter().map(|e| e.source.as_str()).collect();
        let c = client(&self.pool).await?;
        let rows = c
            .rows(
                &upsert_edges_sql(),
                &[
                    &ids,
                    &src,
                    &dst,
                    &kind,
                    &props,
                    &evidence,
                    &confidence,
                    &state,
                    &valid_from,
                    &valid_to,
                    &source,
                ],
            )
            .await?;
        let mut by_identity = std::collections::HashMap::with_capacity(rows.len());
        for r in &rows {
            let identity = (
                r.try_get::<_, Uuid>(1).map_err(pg)?,
                r.try_get::<_, Uuid>(2).map_err(pg)?,
                r.try_get::<_, String>(3).map_err(pg)?,
                r.try_get::<_, String>(4).map_err(pg)?,
                r.try_get::<_, Option<Date>>(5).map_err(pg)?,
            );
            by_identity.insert(identity, r.try_get::<_, Uuid>(0).map_err(pg)?);
        }
        edges
            .iter()
            .map(|e| {
                let (src, dst, kind, source, from) = edge_identity(e);
                by_identity
                    .get(&(src, dst, kind.to_owned(), source.to_owned(), from))
                    .copied()
                    .ok_or_else(|| bad("upserted edge", "missing from RETURNING"))
            })
            .collect()
    }

    async fn upsert_live_edge(&self, edge: &Edge) -> Result<LiveEdge> {
        edge.validate()?;
        upsert_live_edge(&client(&self.pool).await?, edge).await
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
        let props = serde_json::json!({
            "endpoint": at.endpoint, "bucket": at.bucket, "key": at.key,
        });
        // The ends are filled in by the statement; the rest is the edge as written.
        let edge = Edge::new(
            Uuid::nil(),
            Uuid::nil(),
            EdgeKind::LocatedAt,
            EdgeSource::Storage,
        );
        edge.validate()?;
        let c = client(&self.pool).await?;
        let row = c
            .one(
                &format!(
                    "{UPSERT_LOCATION}{EDGE_ON_CONFLICT}) SELECT doc.id, loc.id FROM doc, loc"
                ),
                &[
                    &Uuid::now_v7(),
                    &blake3.to_hex(),
                    &Uuid::now_v7(),
                    &at.node_key(),
                    &props,
                    &blake3.as_bytes().as_slice(),
                    &at.endpoint,
                    &at.bucket,
                    &at.key,
                    &Uuid::now_v7(),
                    &edge.props,
                    &edge.evidence,
                    &edge.confidence,
                    &edge.state.as_str(),
                    &edge.valid_from,
                    &edge.valid_to,
                ],
            )
            .await?;
        Ok(Located {
            document: row.try_get(0).map_err(pg)?,
            location: row.try_get(1).map_err(pg)?,
        })
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
        alias_at(
            &c.one(CLAIM_ALIAS, &[&kind.as_str(), &alias, &node, props])
                .await?,
            0,
        )
    }

    async fn claim_aliases(
        &self,
        kind: NodeKind,
        node: Uuid,
        claims: &[(String, Value)],
    ) -> Result<Vec<NodeAlias>> {
        validate_alias_count(claims.len())?;
        for (alias, props) in claims {
            validate_alias(alias, props)?;
        }
        if claims.is_empty() {
            return Ok(Vec::new());
        }
        let c = client(&self.pool).await?;
        let st = c.prepare_cached(CLAIM_ALIAS).await.map_err(pg)?;
        let kind = kind.as_str();
        // Pipelined (issue #103): the claims go out together, in order, on this connection,
        // and the server runs each as its own statement and transaction, one after the other,
        // so a claim sees the ones before it. One round trip for all of them.
        let params: Vec<[&(dyn ToSql + Sync); 4]> = claims
            .iter()
            .map(|(alias, props)| [&kind as &(dyn ToSql + Sync), alias, &node, props])
            .collect();
        let rows = try_join_all(params.iter().map(|p| c.query_one(&st, p)))
            .await
            .map_err(pg)?;
        rows.iter().map(|r| alias_at(r, 0)).collect()
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
        let got = alias_at(
            &tx.one(CLAIM_ALIAS, &[&kind.as_str(), &alias, &node, props])
                .await?,
            0,
        )?;
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
        .map(|r| alias_at(&r, 0))
        .transpose()
    }

    async fn aliases(&self, kind: NodeKind, aliases: &[String]) -> Result<Vec<Option<NodeAlias>>> {
        validate_alias_count(aliases.len())?;
        let mut out = vec![None; aliases.len()];
        if aliases.is_empty() {
            return Ok(out);
        }
        let c = client(&self.pool).await?;
        let rows = c
            .rows(
                "SELECT a.ord, n.node_id, n.props \
                 FROM unnest($2::text[]) WITH ORDINALITY AS a(alias, ord) \
                 JOIN node_aliases n ON n.kind = $1 AND n.alias = a.alias",
                &[&kind.as_str(), &aliases],
            )
            .await?;
        for r in &rows {
            let ord: i64 = r.try_get(0).map_err(pg)?;
            let at = usize::try_from(ord - 1).map_err(|e| bad("ordinal", e))?;
            *out.get_mut(at).ok_or_else(|| bad("ordinal", ord))? = Some(alias_at(r, 1)?);
        }
        Ok(out)
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
        let c = client(&self.pool).await?;
        c.exec(PUT_SKETCH, &[&key, &signature, &bands]).await?;
        Ok(())
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
        if bands.len() > SKETCH_MAX_BANDS {
            return Err(Error::Invalid(format!(
                "at most {SKETCH_MAX_BANDS} band keys per lookup"
            )));
        }
        let bands: Vec<i64> = bands.iter().map(|b| band_to_i64(*b)).collect();
        let c = client(&self.pool).await?;
        let rows = c
            .rows(
                SKETCH_CANDIDATES_WITHIN,
                &[
                    &bands,
                    &exclude.as_bytes().as_slice(),
                    &i64::from(sketch_limit(limit)),
                    &i64::from(max_bucket),
                ],
            )
            .await?;
        let mut out = Candidates::default();
        for r in &rows {
            out.skipped_bands =
                u32::try_from(r.try_get::<_, i64>(0).map_err(pg)?).unwrap_or(u32::MAX);
            let Some(h) = r.try_get::<_, Option<&[u8]>>(1).map_err(pg)? else {
                continue;
            };
            out.sketches.push(Sketch {
                blake3: Blake3::from_slice(h)?,
                signature: r.try_get(2).map_err(pg)?,
                shared_bands: u32::try_from(r.try_get::<_, i64>(3).map_err(pg)?)
                    .unwrap_or(u32::MAX),
            });
        }
        Ok(out)
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
        let db = client(&self.pool).await?;
        if let Some(company) = q.company {
            let few = db
                .one(FEW_PARTIES, &[&company])
                .await?
                .try_get(0)
                .map_err(pg)?;
            let p = args.push(company);
            let _ = if few {
                // Driven by the company's party edges (edges_by_dst), each document's hash
                // looked up by primary key (issue #111). A document key is its hash in
                // lowercase hex or a `ref:` placeholder (`validate_node`); decode would fail on
                // the placeholders.
                write!(
                    sql,
                    " AND s.blake3 IN (SELECT CASE WHEN starts_with(d.key, 'ref:') THEN NULL \
                     ELSE decode(d.key, 'hex') END FROM edges e JOIN nodes d ON d.id = e.src \
                     WHERE e.dst = {p} AND e.kind = 'party' \
                     AND e.state NOT IN ('retracted', 'superseded') AND d.kind = 'document')"
                )
            } else {
                // A check per indexed document, in rank order, which stops at the page.
                write!(
                    sql,
                    " AND EXISTS (SELECT 1 FROM nodes d JOIN edges e ON e.src = d.id \
                     WHERE d.kind = 'document' AND d.key = encode(s.blake3, 'hex') \
                     AND e.kind = 'party' AND e.dst = {p} \
                     AND e.state NOT IN ('retracted', 'superseded'))"
                )
            };
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
        // Not cached: up to 256 shapes (one per combination of filters), user-driven and rare
        // next to the ingest statements, and a generic plan could not use the source prefix.
        let rows = db.query(&sql, &args.refs()).await.map_err(pg)?;
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
