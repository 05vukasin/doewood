//! `neighbours` and `company_documents`, whole or a keyset page at a time (issue #112), as on
//! SQLite (`doewood-store`, `standalone/pages.rs`).
//!
//! Incoming edges are walked in `edges_by_dst` order, as schema 10 laid it out:
//! `(dst, kind, coalesce(valid_from, '-infinity'), id)`, so that a page starts at its token
//! and stops at its size. An undated edge sorts as `-infinity`, before every date. Outgoing
//! edges use `edges_by_src (src, kind)` and are sorted: a node's outgoing edges are few
//! (ingest caps `similar_to` at 100 a document). Edge kinds sort in the database's collation,
//! which orders the 14 kind names as `"C"` does in every collation that orders lowercase
//! letters alphabetically.

use deadpool_postgres::Object;
use std::fmt::Write as _;

use doewood_model::{
    Direction, EdgeCursor, EdgeKind, Neighbour, NeighbourQuery, Node, PartyDocument, Result,
    neighbour_page_bytes, node_page_bytes, records_limit,
};
use time::Date;
use tokio_postgres::Row;
use tokio_postgres::types::ToSql;
use uuid::Uuid;

use crate::backend::{Args, EDGE_COLUMNS, NODE_COLUMNS, edge_at, node_at, pg};
use crate::cached::Cached as _;

/// The sort key of an edge's `valid_from`, exactly as the index spells it.
pub(crate) const VF_KEY: &str = "coalesce(e.valid_from, '-infinity'::date)";

/// Rows read from a page's portal at a time.
const PAGE_BATCH: usize = 64;

/// The rows of `sql` decoded by `decode`: all of them when `page` is `None`, else through a
/// portal a batch at a time, stopping at `page`'s limit or at the row that reaches its byte
/// budget (counted by `bytes`), so that the server never sends the rest.
async fn paged<T>(
    c: &mut Object,
    sql: &str,
    params: &[&(dyn ToSql + Sync)],
    page: Option<(usize, usize)>,
    decode: impl Fn(&Row) -> Result<T>,
    bytes: impl Fn(&T) -> usize,
) -> Result<Vec<T>> {
    let Some((limit, max_bytes)) = page else {
        return c.rows(sql, params).await?.iter().map(decode).collect();
    };
    let tx = c
        .build_transaction()
        .read_only(true)
        .start()
        .await
        .map_err(pg)?;
    let st = tx.prepare_cached(sql).await.map_err(pg)?;
    let portal = tx.bind(&st, params).await.map_err(pg)?;
    let (mut out, mut total) = (Vec::new(), 0usize);
    'page: loop {
        let batch = PAGE_BATCH.min(limit - out.len());
        let rows = tx
            .query_portal(&portal, i32::try_from(batch).unwrap_or(i32::MAX))
            .await
            .map_err(pg)?;
        let done = rows.len() < batch;
        for r in &rows {
            let item = decode(r)?;
            total += bytes(&item);
            out.push(item);
            if total >= max_bytes {
                break 'page;
            }
        }
        if done || out.len() >= limit {
            break;
        }
    }
    drop(portal);
    tx.commit().await.map_err(pg)?;
    Ok(out)
}

/// The kind condition of one range of a neighbour query, built per arm (each placeholder is
/// used once).
enum Range {
    All,
    Kinds(Vec<String>),
    RestOf(EdgeCursor),
    After(EdgeKind),
}

/// The neighbour query: `UNION ALL` of index ranges, one per direction and, after a cursor,
/// one for the rest of the cursor's kind and one for the kinds after it, each limited to the
/// page size. Columns: the edge (0 to 10), the node (11 to 14), the sort key (15). `node` is
/// the node's expression (`$1`, or a column of a lateral join). `None` when no range can
/// match.
pub(crate) fn neighbours_sql(
    node: &str,
    q: &NeighbourQuery,
    after: Option<&EdgeCursor>,
    limit: Option<i64>,
    args: &mut Args,
) -> Option<String> {
    // (at, other end, leave out self-loops: the outgoing arm returns them)
    let arms: &[(&str, &str, bool)] = match q.direction {
        Direction::Out => &[("e.src", "e.dst", false)],
        Direction::In => &[("e.dst", "e.src", false)],
        Direction::Both => &[("e.src", "e.dst", false), ("e.dst", "e.src", true)],
    };
    let names = |kinds: &[EdgeKind]| -> Vec<String> {
        kinds.iter().map(|k| k.as_str().to_owned()).collect()
    };
    let mut ranges = Vec::new();
    match after {
        None if q.edge_kinds.is_empty() => ranges.push(Range::All),
        None => ranges.push(Range::Kinds(names(&q.edge_kinds))),
        Some(c) => {
            if q.edge_kinds.is_empty() || q.edge_kinds.contains(&c.kind) {
                ranges.push(Range::RestOf(*c));
            }
            if q.edge_kinds.is_empty() {
                ranges.push(Range::After(c.kind));
            } else {
                let later: Vec<EdgeKind> = q
                    .edge_kinds
                    .iter()
                    .copied()
                    .filter(|k| k.as_str() > c.kind.as_str())
                    .collect();
                if !later.is_empty() {
                    ranges.push(Range::Kinds(names(&later)));
                }
            }
        }
    }
    if ranges.is_empty() {
        return None;
    }
    let mut parts = Vec::new();
    for &(at, other, not_self) in arms {
        for range in &ranges {
            let mut s = format!(
                "SELECT {EDGE_COLUMNS}, {NODE_COLUMNS}, {VF_KEY} AS vf_key FROM edges e \
                 JOIN nodes n ON n.id = {other} WHERE {at} = {node}"
            );
            if not_self {
                let _ = write!(s, " AND e.src <> {node}");
            }
            match range {
                Range::All => {}
                Range::Kinds(kinds) => {
                    s.push_str(" AND e.kind = ANY(");
                    s.push_str(&args.push(kinds.clone()));
                    s.push(')');
                }
                Range::RestOf(c) => {
                    s.push_str(" AND e.kind = ");
                    s.push_str(&args.push(c.kind.as_str()));
                    let _ = write!(s, " AND ({VF_KEY}, e.id) > (coalesce(");
                    s.push_str(&args.push(c.valid_from));
                    s.push_str("::date, '-infinity'::date), ");
                    s.push_str(&args.push(c.id));
                    s.push(')');
                }
                Range::After(k) => {
                    s.push_str(" AND e.kind > ");
                    s.push_str(&args.push(k.as_str()));
                }
            }
            if let Some(kind) = q.node_kind {
                s.push_str(" AND n.kind = ");
                s.push_str(&args.push(kind.as_str()));
            }
            if let Some(day) = q.at {
                let p = args.push(day);
                let _ = write!(
                    s,
                    " AND (e.valid_from IS NULL OR e.valid_from <= {p}) \
                     AND (e.valid_to IS NULL OR e.valid_to > {p})"
                );
            }
            if !q.include_inactive {
                s.push_str(" AND e.state NOT IN ('retracted', 'superseded')");
            }
            if let Some(n) = limit {
                s = format!(
                    "SELECT * FROM ({s} ORDER BY e.kind, vf_key, e.id LIMIT {}) r{}",
                    args.push(n),
                    parts.len()
                );
            }
            parts.push(s);
        }
    }
    let mut s = parts.join(" UNION ALL ");
    s.push_str(" ORDER BY 4, 16, 1");
    if let Some(n) = limit {
        s.push_str(" LIMIT ");
        s.push_str(&args.push(n));
    }
    Some(s)
}

/// [`doewood_model::GraphStore::neighbours`], or a page of it when `page` is
/// `(after, limit, max_bytes)`.
pub(crate) async fn neighbours(
    c: &mut Object,
    node: Uuid,
    q: &NeighbourQuery,
    page: Option<(Option<&EdgeCursor>, u32, usize)>,
) -> Result<Vec<Neighbour>> {
    let limit = page.map(|(_, l, _)| records_limit(l));
    let mut args = Args::default();
    args.push(node);
    let Some(sql) = neighbours_sql(
        "$1",
        q,
        page.and_then(|p| p.0),
        limit.map(i64::from),
        &mut args,
    ) else {
        return Ok(Vec::new());
    };
    // Cached: the shapes are few (direction, filters, a cursor or not, its kind's place).
    paged(
        c,
        &sql,
        &args.refs(),
        page.map(|(_, l, b)| (records_limit(l) as usize, b)),
        |r| {
            Ok(Neighbour {
                edge: edge_at(r)?,
                node: node_at(r, 11)?,
            })
        },
        neighbour_page_bytes,
    )
    .await
}

/// The company-documents query: the company's live party edges in `edges_by_dst` order,
/// each kept only when it is its document's first live party edge to the company (looked up
/// in `edges_identity`). `$1` the company; then the cursor's `valid_from` and id; then the
/// limit.
pub(crate) fn company_documents_sql(after: bool, limit: bool) -> String {
    let mut s = format!(
        "SELECT {NODE_COLUMNS}, e.valid_from, e.id FROM edges e JOIN nodes n ON n.id = e.src \
         WHERE e.dst = $1 AND e.kind = 'party' AND e.state NOT IN ('retracted', 'superseded') \
         AND n.kind = 'document'"
    );
    if after {
        let _ = write!(
            s,
            " AND ({VF_KEY}, e.id) > (coalesce($2::date, '-infinity'::date), $3)"
        );
    }
    let _ = write!(
        s,
        " AND NOT EXISTS (SELECT 1 FROM edges f WHERE f.src = e.src AND f.dst = e.dst \
         AND f.kind = 'party' AND f.state NOT IN ('retracted', 'superseded') \
         AND (coalesce(f.valid_from, '-infinity'::date), f.id) < ({VF_KEY}, e.id)) \
         ORDER BY {VF_KEY}, e.id"
    );
    if limit {
        s.push_str(if after { " LIMIT $4" } else { " LIMIT $2" });
    }
    s
}

/// [`doewood_model::GraphStore::company_documents_page`]; every document when `page` is
/// `None`, else `(after, limit, max_bytes)`.
pub(crate) async fn company_documents(
    c: &mut Object,
    company: Uuid,
    after: Option<&EdgeCursor>,
    page: Option<(u32, usize)>,
) -> Result<Vec<PartyDocument>> {
    let sql = company_documents_sql(after.is_some(), page.is_some());
    let vf: Option<Date> = after.and_then(|a| a.valid_from);
    let id: Option<Uuid> = after.map(|a| a.id);
    let limit = page.map(|(l, _)| i64::from(records_limit(l)));
    let mut params: Vec<&(dyn ToSql + Sync)> = vec![&company];
    if after.is_some() {
        params.push(&vf);
        params.push(&id);
    }
    if let Some(n) = &limit {
        params.push(n);
    }
    paged(
        c,
        &sql,
        &params,
        page.map(|(l, b)| (records_limit(l) as usize, b)),
        |r| {
            let node: Node = node_at(r, 0)?;
            Ok(PartyDocument {
                node,
                edge: EdgeCursor {
                    kind: EdgeKind::Party,
                    valid_from: r.try_get(4).map_err(pg)?,
                    id: r.try_get(5).map_err(pg)?,
                },
            })
        },
        |d| node_page_bytes(&d.node),
    )
    .await
}

/// [`doewood_model::GraphStore::neighbours_of_many`] in one statement (issue #172): the page
/// query of [`neighbours_sql`] for each node of `$1` in a lateral join, in the nodes' order,
/// read through a portal up to `max_bytes`. Column 16 is the node's place in `$1`.
pub(crate) async fn neighbours_of_many(
    c: &mut Object,
    nodes: &[Uuid],
    q: &NeighbourQuery,
    per_node: u32,
    max_bytes: usize,
) -> Result<Vec<(Uuid, Neighbour)>> {
    let per_node = records_limit(per_node);
    let mut args = Args::default();
    args.push(nodes.to_vec());
    let Some(inner) = neighbours_sql("f.id", q, None, Some(i64::from(per_node)), &mut args) else {
        return Ok(Vec::new());
    };
    let total = nodes.len().saturating_mul(per_node as usize);
    let sql = format!(
        "SELECT x.*, f.ord FROM unnest($1::uuid[]) WITH ORDINALITY f (id, ord) \
         CROSS JOIN LATERAL ({inner}) x ORDER BY f.ord, 4, 16, 1"
    );
    let ords = paged(
        c,
        &sql,
        &args.refs(),
        Some((total.max(1), max_bytes)),
        |r| {
            let ord: i64 = r.try_get(16).map_err(pg)?;
            Ok((
                ord,
                Neighbour {
                    edge: edge_at(r)?,
                    node: node_at(r, 11)?,
                },
            ))
        },
        |(_, n)| neighbour_page_bytes(n),
    )
    .await?;
    Ok(ords
        .into_iter()
        .filter_map(|(ord, n)| {
            let at = usize::try_from(ord).ok()?.checked_sub(1)?;
            Some((*nodes.get(at)?, n))
        })
        .collect())
}
