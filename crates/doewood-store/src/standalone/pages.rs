//! `neighbours` and `company_documents`, whole or a keyset page at a time (issue #112).
//!
//! Incoming edges are walked in `edges_by_dst` order, as schema 12 laid it out:
//! `(dst, kind, ifnull(valid_from, VF_MIN), id)`, so that a page starts at its token and stops
//! at its size. An undated edge sorts as [`VF_MIN`], before every date: "undated first".
//! Outgoing edges use `edges_by_src (src, kind)` and are sorted per page: a node's outgoing
//! edges are few (ingest caps `similar_to` at 100 a document).

use doewood_model::{
    Direction, EdgeCursor, EdgeKind, Neighbour, NeighbourQuery, Node, PartyDocument, Result,
    neighbour_page_bytes, node_page_bytes, records_limit,
};
use rusqlite::types::Value as Sql;
use rusqlite::{Connection, params_from_iter};
use uuid::Uuid;

use super::{
    EDGE_COLUMNS, NODE_COLUMNS, edge_at, edge_from, from_julian, julian, node_at, node_from, sql,
    uuid,
};

/// An undated edge's `valid_from` in the edge indexes: below every Julian day.
pub(crate) const VF_MIN: i64 = -2_147_483_648;

/// The sort key of an edge's `valid_from`, exactly as the indexes spell it.
pub(crate) const VF_KEY: &str = "ifnull(e.valid_from, -2147483648)";

fn vf_of(c: &EdgeCursor) -> i64 {
    c.valid_from.map_or(VF_MIN, julian)
}

/// One side of a neighbour query: the edges with `at` = the node, the node at `other`.
struct Arm {
    at: &'static str,
    other: &'static str,
    /// Leaves out self-loops, which the other arm returns already.
    not_self: bool,
}

/// The neighbour query: `UNION ALL` of index ranges, one per direction and, after a cursor,
/// two for the rest of the cursor's kind (its day, then later days) and one for the kinds
/// after it. Each range is read in the order of its index, so with a `limit` each stops after
/// `limit` rows and the merge sorts at most a page per range. Columns: the edge (0 to 10), the
/// node (11 to 14), the sort key (15). `None` when no range can match.
#[allow(clippy::too_many_lines)]
pub(crate) fn neighbours_sql(
    q: &NeighbourQuery,
    after: Option<&EdgeCursor>,
    limit: Option<i64>,
) -> Option<(String, Vec<Sql>)> {
    let out = Arm {
        at: "e.src",
        other: "e.dst",
        not_self: false,
    };
    let inn = Arm {
        at: "e.dst",
        other: "e.src",
        not_self: q.direction == Direction::Both,
    };
    let arms: Vec<Arm> = match q.direction {
        Direction::Out => vec![out],
        Direction::In => vec![inn],
        Direction::Both => vec![out, inn],
    };
    // (kind condition, its arguments, its order) for each range of one arm. The order is
    // spelled as the index has it, so each range is read in index order and stops at the limit.
    let all_order = format!("e.kind, {VF_KEY}, e.id");
    let mut ranges: Vec<(String, Vec<Sql>, String)> = Vec::new();
    let text = |k: EdgeKind| Sql::Text(k.as_str().to_owned());
    let kinds_in = |kinds: &[EdgeKind]| {
        (
            format!(" AND e.kind IN ({})", vec!["?"; kinds.len()].join(", ")),
            kinds.iter().map(|k| text(*k)).collect::<Vec<_>>(),
            all_order.clone(),
        )
    };
    match after {
        None if q.edge_kinds.is_empty() => {
            ranges.push((String::new(), Vec::new(), all_order.clone()));
        }
        None => ranges.push(kinds_in(&q.edge_kinds)),
        Some(c) => {
            if q.edge_kinds.is_empty() || q.edge_kinds.contains(&c.kind) {
                // The rest of the cursor's kind as two ranges: SQLite does not start an index
                // scan at a row value that holds an expression.
                ranges.push((
                    format!(" AND e.kind = ? AND {VF_KEY} = ? AND e.id > ?"),
                    vec![
                        text(c.kind),
                        Sql::Integer(vf_of(c)),
                        Sql::Text(c.id.to_string()),
                    ],
                    "e.id".to_owned(),
                ));
                ranges.push((
                    format!(" AND e.kind = ? AND {VF_KEY} > ?"),
                    vec![text(c.kind), Sql::Integer(vf_of(c))],
                    format!("{VF_KEY}, e.id"),
                ));
            }
            if q.edge_kinds.is_empty() {
                ranges.push((
                    " AND e.kind > ?".to_owned(),
                    vec![text(c.kind)],
                    all_order.clone(),
                ));
            } else {
                let later: Vec<EdgeKind> = q
                    .edge_kinds
                    .iter()
                    .copied()
                    .filter(|k| k.as_str() > c.kind.as_str())
                    .collect();
                if !later.is_empty() {
                    ranges.push(kinds_in(&later));
                }
            }
        }
    }
    if ranges.is_empty() {
        return None;
    }
    let mut parts = Vec::new();
    let mut args = vec![Sql::Text(String::new())];
    for arm in &arms {
        for (kinds, kind_args, order) in &ranges {
            let mut s = format!(
                "SELECT {EDGE_COLUMNS}, {NODE_COLUMNS}, {VF_KEY} AS vf_key FROM edges e \
                 JOIN nodes n ON n.id = {} WHERE {} = ?1",
                arm.other, arm.at
            );
            if arm.not_self {
                s.push_str(" AND e.src <> ?1");
            }
            s.push_str(kinds);
            args.extend(kind_args.iter().cloned());
            if let Some(kind) = q.node_kind {
                s.push_str(" AND n.kind = ?");
                args.push(Sql::Text(kind.as_str().to_owned()));
            }
            if let Some(at) = q.at {
                s.push_str(
                    " AND (e.valid_from IS NULL OR e.valid_from <= ?) \
                     AND (e.valid_to IS NULL OR e.valid_to > ?)",
                );
                args.extend([Sql::Integer(julian(at)), Sql::Integer(julian(at))]);
            }
            if !q.include_inactive {
                s.push_str(" AND e.state NOT IN ('retracted', 'superseded')");
            }
            if let Some(n) = limit {
                s = format!("SELECT * FROM ({s} ORDER BY {order} LIMIT ?)");
                args.push(Sql::Integer(n));
            }
            parts.push(s);
        }
    }
    let mut s = parts.join(" UNION ALL ");
    s.push_str(" ORDER BY 4, 16, 1");
    if let Some(n) = limit {
        s.push_str(" LIMIT ?");
        args.push(Sql::Integer(n));
    }
    Some((s, args))
}

/// [`doewood_model::GraphStore::neighbours`], or a page of it when `page` is
/// `(after, limit, max_bytes)`.
pub(crate) fn neighbours(
    conn: &Connection,
    node: Uuid,
    q: &NeighbourQuery,
    page: Option<(Option<&EdgeCursor>, u32, usize)>,
) -> Result<Vec<Neighbour>> {
    let limit = page.map(|(_, l, _)| i64::from(records_limit(l)));
    let Some((text, mut args)) = neighbours_sql(q, page.and_then(|p| p.0), limit) else {
        return Ok(Vec::new());
    };
    args[0] = Sql::Text(node.to_string());
    let max_bytes = page.map_or(usize::MAX, |p| p.2);
    // Cached: the shapes are few (direction, filters, a cursor or not, its kind's place).
    let mut stmt = conn.prepare_cached(&text).map_err(sql)?;
    let rows = stmt
        .query_map(params_from_iter(args), |r| {
            Ok((edge_at(r)?, node_at(r, 11)?))
        })
        .map_err(sql)?;
    let (mut out, mut bytes) = (Vec::new(), 0usize);
    for row in rows {
        let (edge, node) = row.map_err(sql)?;
        let n = Neighbour {
            edge: edge_from(edge)?,
            node: node_from(node)?,
        };
        if max_bytes != usize::MAX {
            bytes += neighbour_page_bytes(&n);
        }
        out.push(n);
        if bytes >= max_bytes {
            break;
        }
    }
    Ok(out)
}

/// The company-documents query: the company's live party edges in `edges_by_dst` order,
/// each kept only when it is its document's first live party edge to the company (looked up
/// in `edges_identity`). `?1` the company; then the cursor's sort key and id; then the limit.
pub(crate) fn company_documents_sql(after: bool, limit: bool) -> String {
    let base = |bound: &str, order: &str| {
        let mut s = format!(
            "SELECT {NODE_COLUMNS}, e.valid_from, e.id, {VF_KEY} AS vf_key FROM edges e \
             JOIN nodes n ON n.id = e.src \
             WHERE e.dst = ?1 AND e.kind = 'party' AND e.state NOT IN ('retracted', 'superseded') \
             AND n.kind = 'document'{bound} \
             AND NOT EXISTS (SELECT 1 FROM edges f WHERE f.src = e.src AND f.dst = e.dst \
             AND f.kind = 'party' AND f.state NOT IN ('retracted', 'superseded') \
             AND (ifnull(f.valid_from, -2147483648), f.id) < ({VF_KEY}, e.id))"
        );
        if limit {
            let n = if after { "?4" } else { "?2" };
            s = format!("SELECT * FROM ({s} ORDER BY {order} LIMIT {n})");
        }
        s
    };
    let mut s = if after {
        // Two ranges, as for the neighbours: the rest of the cursor's day, then later days.
        format!(
            "{} UNION ALL {}",
            base(&format!(" AND {VF_KEY} = ?2 AND e.id > ?3"), "e.id"),
            base(&format!(" AND {VF_KEY} > ?2"), &format!("{VF_KEY}, e.id"))
        )
    } else {
        base("", &format!("{VF_KEY}, e.id"))
    };
    s.push_str(" ORDER BY 7, 6");
    if limit {
        s.push_str(if after { " LIMIT ?4" } else { " LIMIT ?2" });
    }
    s
}

/// [`doewood_model::GraphStore::company_documents_page`]; every document when `limit` is
/// `None`.
pub(crate) fn company_documents(
    conn: &Connection,
    company: Uuid,
    after: Option<&EdgeCursor>,
    limit: Option<u32>,
    max_bytes: usize,
) -> Result<Vec<PartyDocument>> {
    let mut stmt = conn
        .prepare_cached(&company_documents_sql(after.is_some(), limit.is_some()))
        .map_err(sql)?;
    let mut args = vec![Sql::Text(company.to_string())];
    if let Some(c) = after {
        args.extend([Sql::Integer(vf_of(c)), Sql::Text(c.id.to_string())]);
    }
    if let Some(n) = limit {
        args.push(Sql::Integer(i64::from(n)));
    }
    let rows = stmt
        .query_map(params_from_iter(args), |r| {
            Ok((
                node_at(r, 0)?,
                r.get::<_, Option<i64>>(4)?,
                r.get::<_, String>(5)?,
            ))
        })
        .map_err(sql)?;
    let (mut out, mut bytes) = (Vec::new(), 0usize);
    for row in rows {
        let (node, vf, id) = row.map_err(sql)?;
        let node: Node = node_from(node)?;
        if max_bytes != usize::MAX {
            bytes += node_page_bytes(&node);
        }
        out.push(PartyDocument {
            node,
            edge: EdgeCursor {
                kind: EdgeKind::Party,
                valid_from: from_julian(vf)?,
                id: uuid(&id)?,
            },
        });
        if bytes >= max_bytes {
            break;
        }
    }
    Ok(out)
}
