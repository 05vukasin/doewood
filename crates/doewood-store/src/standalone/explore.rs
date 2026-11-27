//! The explorer's reads on SQLite (issue #172): the company graph's fold, the company
//! listing and the links between companies. See `doewood_model::explore` for the rules and
//! schema 14 for the tables.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;

use doewood_model::explore::{
    COMPANY_IDENTIFIERS_MAX, FoldDelta, LinkState, LogEntry, Member, fold_deltas, plan_fold,
};
use doewood_model::{
    CompanyCursor, CompanyFilter, CompanyLink, CompanyOrder, CompanyRow, Folded, Result,
    company_row_bytes,
};
use rusqlite::types::Value as Sql;
use rusqlite::{Connection, OptionalExtension as _, TransactionBehavior, params, params_from_iter};
use uuid::Uuid;

use super::{NODE_COLUMNS, node_at, node_from, sql, uuid};

/// The edge kinds the log follows, as SQL.
const COUNTED_KINDS: &str = "('party', 'parent_of', 'subsidiary_of', 'acquired', \
     'merged_into', 'renamed_to', 'branch_of', 'trading_name_of', 'former_parent_of', \
     'liquidated')";

/// A party edge's role as the triggers log it.
const TAG: &str = "CASE WHEN e.kind = 'party' \
     THEN substr(coalesce(json_extract(e.props_json, '$.role'), ''), 1, 64) ELSE e.kind END";

/// The name key, exactly as `nodes_company_name` spells it.
const NAME_KEY: &str = "lower(coalesce(json_extract(n.props_json, '$.name'), ''))";

/// One fold: the nodes of the first `max_rows` log rows, all their log rows and their live
/// counted edges, in the writer's transaction (one snapshot), then the counts moved.
pub(super) fn fold(conn: &mut Connection, max_rows: u32) -> Result<Folded> {
    let tx = conn
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(sql)?;
    let nodes: Vec<String> = {
        let mut st = tx
            .prepare_cached(
                "SELECT DISTINCT node FROM \
                 (SELECT node FROM company_graph_log ORDER BY id LIMIT ?1)",
            )
            .map_err(sql)?;
        st.query_map([i64::from(max_rows.max(1))], |r| r.get(0))
            .map_err(sql)?
            .collect::<rusqlite::Result<_>>()
            .map_err(sql)?
    };
    if nodes.is_empty() {
        return Ok(Folded::default());
    }
    let nodes_json = serde_json::Value::from(nodes).to_string();
    let mut ids = Vec::new();
    let mut log = Vec::new();
    {
        let mut st = tx
            .prepare_cached(
                "SELECT id, node, other, party, tag, delta FROM company_graph_log \
                 WHERE node IN (SELECT value FROM json_each(?1))",
            )
            .map_err(sql)?;
        let rows = st
            .query_map([&nodes_json], |r| {
                Ok((
                    r.get::<_, i64>(0)?,
                    r.get::<_, String>(1)?,
                    r.get::<_, String>(2)?,
                    r.get::<_, bool>(3)?,
                    r.get::<_, String>(4)?,
                    r.get::<_, i32>(5)?,
                ))
            })
            .map_err(sql)?;
        for row in rows {
            let (id, node, other, party, tag, delta) = row.map_err(sql)?;
            ids.push(id);
            log.push(LogEntry {
                node: uuid(&node)?,
                other: uuid(&other)?,
                party,
                tag,
                delta,
            });
        }
    }
    let mut current = Vec::new();
    {
        let mut st = tx
            .prepare_cached(&format!(
                "SELECT e.src, e.dst, e.kind = 'party', {TAG} FROM edges e \
                 WHERE e.src IN (SELECT value FROM json_each(?1)) AND e.kind IN {COUNTED_KINDS} \
                 AND e.state NOT IN ('retracted', 'superseded')"
            ))
            .map_err(sql)?;
        let rows = st
            .query_map([&nodes_json], |r| {
                Ok((
                    r.get::<_, String>(0)?,
                    r.get::<_, String>(1)?,
                    r.get::<_, bool>(2)?,
                    r.get::<_, String>(3)?,
                ))
            })
            .map_err(sql)?;
        for row in rows {
            let (node, other, party, tag) = row.map_err(sql)?;
            current.push(Member {
                node: uuid(&node)?,
                other: uuid(&other)?,
                party,
                tag,
            });
        }
    }
    apply(&tx, &fold_deltas(&log, &current))?;
    {
        let mut st = tx
            .prepare_cached("DELETE FROM company_graph_log WHERE id = ?1")
            .map_err(sql)?;
        for id in &ids {
            st.execute([id]).map_err(sql)?;
        }
    }
    let more = tx
        .query_row("SELECT 1 FROM company_graph_log LIMIT 1", [], |_| Ok(()))
        .optional()
        .map_err(sql)?
        .is_some();
    tx.commit().map_err(sql)?;
    Ok(Folded {
        rows: ids.len() as u64,
        more,
    })
}

fn apply(conn: &Connection, delta: &FoldDelta) -> Result<()> {
    let mut existing = BTreeMap::new();
    {
        let mut st = conn
            .prepare_cached("SELECT docs, events, roles FROM company_links WHERE a = ?1 AND b = ?2")
            .map_err(sql)?;
        for &(a, b) in delta.links.keys() {
            let row = st
                .query_row([a.to_string(), b.to_string()], |r| {
                    Ok((
                        r.get::<_, i64>(0)?,
                        r.get::<_, i64>(1)?,
                        r.get::<_, String>(2)?,
                    ))
                })
                .optional()
                .map_err(sql)?;
            if let Some((documents, events, roles)) = row {
                existing.insert(
                    (a, b),
                    LinkState {
                        documents,
                        events,
                        roles: LinkState::parse_roles(&roles),
                    },
                );
            }
        }
    }
    let plan = plan_fold(delta, &existing);
    let mut put = conn
        .prepare_cached(
            "INSERT INTO company_links (a, b, docs, events, roles) VALUES (?1, ?2, ?3, ?4, ?5) \
             ON CONFLICT (a, b) DO UPDATE SET docs = excluded.docs, events = excluded.events, \
             roles = excluded.roles",
        )
        .map_err(sql)?;
    let mut del = conn
        .prepare_cached("DELETE FROM company_links WHERE a = ?1 AND b = ?2")
        .map_err(sql)?;
    for ((a, b), state) in &plan.links {
        match state {
            Some(s) => {
                put.execute(params![
                    a.to_string(),
                    b.to_string(),
                    s.documents,
                    s.events,
                    s.roles_json()
                ])
                .map_err(sql)?;
            }
            None => {
                del.execute([a.to_string(), b.to_string()]).map_err(sql)?;
            }
        }
    }
    let mut stats = conn
        .prepare_cached(
            "UPDATE company_stats SET documents = max(0, documents + ?2), \
             degree = max(0, degree + ?3) WHERE company_id = ?1",
        )
        .map_err(sql)?;
    for (company, (documents, degree)) in &plan.stats {
        stats
            .execute(params![company.to_string(), documents, degree])
            .map_err(sql)?;
    }
    Ok(())
}

/// The company listing's query, its arguments and the column of the name key.
fn list_sql(
    filter: &CompanyFilter,
    order: CompanyOrder,
    after: Option<&CompanyCursor>,
    limit: u32,
) -> (String, Vec<Sql>) {
    let mut args: Vec<Sql> = Vec::new();
    let mut s = format!(
        "SELECT {NODE_COLUMNS}, s.documents, s.degree, \
         EXISTS (SELECT 1 FROM company_enrichment f WHERE f.company_id = n.id), \
         (SELECT json_group_array(alias) FROM (SELECT a.alias FROM node_aliases a \
          WHERE a.node_id = n.id AND a.kind = 'company' AND a.alias >= 'id:' AND a.alias < 'id;' \
          ORDER BY a.alias LIMIT {COMPANY_IDENTIFIERS_MAX})), \
         {NAME_KEY} "
    );
    match order {
        CompanyOrder::Name => s.push_str(
            "FROM nodes n INDEXED BY nodes_company_name CROSS JOIN company_stats s \
             ON s.company_id = n.id WHERE n.kind = 'company'",
        ),
        _ => s.push_str(
            // CROSS JOIN keeps `company_stats` outside, read in its index order: left to
            // the planner, it read every company by `kind` and sorted them.
            "FROM company_stats s CROSS JOIN nodes n ON n.id = s.company_id \
             WHERE n.kind = 'company'",
        ),
    }
    s.push_str(" AND json_extract(n.props_json, '$.merged_into') IS NULL");
    if let Some(p) = filter.name_prefix.as_deref().filter(|p| !p.is_empty()) {
        args.push(Sql::Text(p.to_owned()));
        let i = args.len();
        let _ = write!(
            s,
            " AND {NAME_KEY} >= lower(?{i}) AND {NAME_KEY} < lower(?{i}) || char(1114111)"
        );
    }
    if let Some(j) = filter.jurisdiction_key() {
        args.push(Sql::Text(j));
        let _ = write!(
            s,
            " AND upper(json_extract(n.props_json, '$.jurisdiction')) = ?{}",
            args.len()
        );
    }
    if let Some(has) = filter.has_identifier {
        let _ = write!(
            s,
            " AND {}EXISTS (SELECT 1 FROM node_aliases a WHERE a.node_id = n.id \
             AND a.kind = 'company' AND a.alias >= 'id:' AND a.alias < 'id;')",
            if has { "" } else { "NOT " }
        );
    }
    if let Some(yes) = filter.enriched {
        let _ = write!(
            s,
            " AND {}EXISTS (SELECT 1 FROM company_enrichment f WHERE f.company_id = n.id)",
            if yes { "" } else { "NOT " }
        );
    }
    let column = match order {
        CompanyOrder::Documents => "s.documents",
        CompanyOrder::Degree => "s.degree",
        CompanyOrder::Name => NAME_KEY,
    };
    if let Some(c) = after {
        args.push(match order {
            CompanyOrder::Name => Sql::Text(c.name.clone()),
            _ => Sql::Integer(i64::try_from(c.count).unwrap_or(i64::MAX)),
        });
        args.push(Sql::Text(c.id.to_string()));
        let (k, i) = (args.len() - 1, args.len());
        match order {
            CompanyOrder::Name => {
                let _ = write!(s, " AND ({NAME_KEY}, n.id) > (?{k}, ?{i})");
            }
            _ => {
                let _ = write!(s, " AND ({column}, s.company_id) < (?{k}, ?{i})");
            }
        }
    }
    match order {
        CompanyOrder::Name => {
            let _ = write!(s, " ORDER BY {NAME_KEY}, n.id");
        }
        _ => {
            let _ = write!(s, " ORDER BY {column} DESC, s.company_id DESC");
        }
    }
    args.push(Sql::Integer(i64::from(limit)));
    let _ = write!(s, " LIMIT ?{}", args.len());
    (s, args)
}

pub(super) fn list_companies(
    conn: &Connection,
    filter: &CompanyFilter,
    order: CompanyOrder,
    after: Option<&CompanyCursor>,
    limit: u32,
    max_bytes: usize,
) -> Result<Vec<CompanyRow>> {
    let (text, args) = list_sql(filter, order, after, limit);
    // Uncached: the shapes are many (filters), and each is cheap to prepare.
    let mut st = conn.prepare(&text).map_err(sql)?;
    let rows = st
        .query_map(params_from_iter(args), |r| {
            Ok((
                node_at(r, 0)?,
                r.get::<_, i64>(4)?,
                r.get::<_, i64>(5)?,
                r.get::<_, bool>(6)?,
                r.get::<_, Option<String>>(7)?,
                r.get::<_, String>(8)?,
            ))
        })
        .map_err(sql)?;
    let (mut out, mut bytes) = (Vec::new(), 0usize);
    for row in rows {
        let (node, documents, degree, enriched, ids, name_key) = row.map_err(sql)?;
        let identifiers: Vec<String> = ids
            .as_deref()
            .and_then(|t| serde_json::from_str(t).ok())
            .unwrap_or_default();
        let row = CompanyRow {
            node: node_from(node)?,
            documents: u64::try_from(documents).unwrap_or(0),
            degree: u64::try_from(degree).unwrap_or(0),
            enriched,
            identifiers,
            name_key,
        };
        bytes += company_row_bytes(&row);
        out.push(row);
        if bytes >= max_bytes {
            break;
        }
    }
    Ok(out)
}

/// The links among `companies`: for each company `a`, its pairs `(a, b)` with `b` among the
/// later companies, read as a range of the primary key when it holds fewer rows than there
/// are later companies, else probed pair by pair; so a call costs at most the smaller of
/// the two per company, and hubs cost no more than the probes. The strongest `limit` are kept
/// in a heap as they come.
pub(super) fn company_links(
    conn: &Connection,
    companies: &[Uuid],
    min_documents: u32,
    limit: u32,
) -> Result<Vec<CompanyLink>> {
    use std::cmp::Reverse;
    use std::collections::BinaryHeap;

    // The strongest first: (documents, events, then the smaller pair) as a min-heap of the
    // `limit` best.
    type Key = (i64, i64, Reverse<(String, String)>);

    let set: Vec<String> = companies
        .iter()
        .copied()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .map(|u| u.to_string())
        .collect();
    let min = i64::from(min_documents);
    let mut range = conn
        .prepare_cached(
            "SELECT b, docs, events, roles FROM company_links WHERE a = ?1 ORDER BY b LIMIT ?2",
        )
        .map_err(sql)?;
    let mut probe = conn
        .prepare_cached("SELECT b, docs, events, roles FROM company_links WHERE a = ?1 AND b = ?2")
        .map_err(sql)?;
    let mut heap: BinaryHeap<Reverse<(Key, String)>> = BinaryHeap::new();
    let row = |r: &rusqlite::Row<'_>| -> rusqlite::Result<(String, i64, i64, String)> {
        Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?))
    };
    let mut keep = |a: &str, (b, docs, events, roles): (String, i64, i64, String)| {
        if docs < min && events == 0 {
            return;
        }
        let key = (docs, events, Reverse((a.to_owned(), b)));
        heap.push(Reverse((key, roles)));
        if heap.len() > limit as usize {
            heap.pop();
        }
    };
    for (i, a) in set.iter().enumerate() {
        let later = &set[i + 1..];
        if later.is_empty() {
            break;
        }
        let k = i64::try_from(later.len()).unwrap_or(i64::MAX);
        let found: Vec<(String, i64, i64, String)> = range
            .query_map(params![a, k + 1], row)
            .map_err(sql)?
            .collect::<rusqlite::Result<_>>()
            .map_err(sql)?;
        if found.len() as u64 <= later.len() as u64 {
            for f in found {
                if later.binary_search(&f.0).is_ok() {
                    keep(a, f);
                }
            }
        } else {
            for b in later {
                if let Some(f) = probe
                    .query_row(params![a, b], row)
                    .optional()
                    .map_err(sql)?
                {
                    keep(a, f);
                }
            }
        }
    }
    let mut out = Vec::with_capacity(heap.len());
    for Reverse(((documents, events, Reverse((a, b))), roles)) in heap.into_sorted_vec() {
        out.push(
            LinkState {
                documents,
                events,
                roles: LinkState::parse_roles(&roles),
            }
            .link(uuid(&a)?, uuid(&b)?),
        );
    }
    Ok(out)
}
