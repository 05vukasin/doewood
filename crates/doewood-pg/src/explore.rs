//! The explorer's reads on Postgres (issue #172): the company graph's fold, the company
//! listing and the links between companies, as on SQLite (`doewood-store`,
//! `standalone/explore.rs`). See `doewood_model::explore` for the rules and schema 12 for
//! the tables and triggers.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;

use deadpool_postgres::Pool;
use doewood_model::explore::{
    COMPANY_IDENTIFIERS_MAX, LinkState, LogEntry, Member, fold_deltas, plan_fold,
};
use doewood_model::{
    CompanyCursor, CompanyFilter, CompanyLink, CompanyOrder, CompanyRow, Folded, Result,
    company_row_bytes,
};
use serde_json::Value;
use uuid::Uuid;

use crate::backend::{Args, NODE_COLUMNS, client, node_at, pg};
use crate::cached::Cached as _;

/// The advisory lock only folds take (two-key form: class "dwce", key 1), for their
/// transaction.
const FOLD_LOCK: &str = "SELECT pg_advisory_xact_lock(1685546853, 1)";

/// A batch in one statement, so in one snapshot: the nodes of the first `$1` log rows, every
/// log row of those nodes, and their live counted edges now (`log` false).
const FOLD_READ: &str = "WITH batch AS MATERIALIZED (\
     SELECT DISTINCT node FROM (SELECT node FROM company_graph_log ORDER BY id LIMIT $1) b) \
     SELECT true, l.id, l.node, l.other, l.party, l.tag, l.delta FROM company_graph_log l \
     WHERE l.node IN (SELECT node FROM batch) \
     UNION ALL \
     SELECT false, NULL, e.src, e.dst, e.kind = 'party', company_graph_tag(e.kind, e.props), \
     NULL FROM edges e WHERE e.src IN (SELECT node FROM batch) \
     AND company_graph_counted(e.kind, e.state)";

/// The name key, exactly as `nodes_company_name` spells it.
const NAME_KEY: &str = "(lower(coalesce(n.props->>'name', '')) COLLATE \"C\")";

/// One fold: [`FOLD_READ`] under [`FOLD_LOCK`], then the counts moved and the rows deleted,
/// in one transaction.
#[allow(clippy::too_many_lines)]
pub(crate) async fn fold(pool: &Pool, max_rows: u32) -> Result<Folded> {
    let mut c = client(pool).await?;
    let tx = c.transaction().await.map_err(pg)?;
    tx.exec(FOLD_LOCK, &[]).await?;
    let rows = tx.rows(FOLD_READ, &[&i64::from(max_rows.max(1))]).await?;
    if rows.is_empty() {
        tx.commit().await.map_err(pg)?;
        return Ok(Folded::default());
    }
    let (mut ids, mut log, mut current) = (Vec::new(), Vec::new(), Vec::new());
    for r in &rows {
        let is_log: bool = r.try_get(0).map_err(pg)?;
        let (node, other, party, tag): (Uuid, Uuid, bool, String) = (
            r.try_get(2).map_err(pg)?,
            r.try_get(3).map_err(pg)?,
            r.try_get(4).map_err(pg)?,
            r.try_get(5).map_err(pg)?,
        );
        if is_log {
            ids.push(r.try_get::<_, i64>(1).map_err(pg)?);
            log.push(LogEntry {
                node,
                other,
                party,
                tag,
                delta: i32::from(r.try_get::<_, i16>(6).map_err(pg)?),
            });
        } else {
            current.push(Member {
                node,
                other,
                party,
                tag,
            });
        }
    }
    let delta = fold_deltas(&log, &current);
    let (ka, kb): (Vec<Uuid>, Vec<Uuid>) = delta.links.keys().copied().unzip();
    let mut existing = BTreeMap::new();
    if !ka.is_empty() {
        for r in tx
            .rows(
                "SELECT l.a, l.b, l.docs, l.events, l.roles::text FROM company_links l \
                 JOIN unnest($1::uuid[], $2::uuid[]) k (a, b) ON l.a = k.a AND l.b = k.b",
                &[&ka, &kb],
            )
            .await?
        {
            existing.insert(
                (r.try_get(0).map_err(pg)?, r.try_get(1).map_err(pg)?),
                LinkState {
                    documents: r.try_get(2).map_err(pg)?,
                    events: r.try_get(3).map_err(pg)?,
                    roles: LinkState::parse_roles(r.try_get(4).map_err(pg)?),
                },
            );
        }
    }
    let plan = plan_fold(&delta, &existing);
    let (mut pa, mut pb, mut pd, mut pe, mut pr) = (vec![], vec![], vec![], vec![], vec![]);
    let (mut da, mut db) = (vec![], vec![]);
    for ((a, b), state) in &plan.links {
        if let Some(s) = state {
            pa.push(*a);
            pb.push(*b);
            pd.push(s.documents);
            pe.push(s.events);
            pr.push(serde_json::from_str::<Value>(&s.roles_json()).unwrap_or_default());
        } else {
            da.push(*a);
            db.push(*b);
        }
    }
    if !pa.is_empty() {
        tx.exec(
            "INSERT INTO company_links (a, b, docs, events, roles) \
             SELECT * FROM unnest($1::uuid[], $2::uuid[], $3::bigint[], $4::bigint[], \
             $5::jsonb[]) ON CONFLICT (a, b) DO UPDATE SET docs = EXCLUDED.docs, \
             events = EXCLUDED.events, roles = EXCLUDED.roles",
            &[&pa, &pb, &pd, &pe, &pr],
        )
        .await?;
    }
    if !da.is_empty() {
        tx.exec(
            "DELETE FROM company_links l USING unnest($1::uuid[], $2::uuid[]) k (a, b) \
             WHERE l.a = k.a AND l.b = k.b",
            &[&da, &db],
        )
        .await?;
    }
    if !plan.stats.is_empty() {
        let (mut sc, mut sd, mut sg) = (vec![], vec![], vec![]);
        for (company, (d, g)) in &plan.stats {
            sc.push(*company);
            sd.push(*d);
            sg.push(*g);
        }
        tx.exec(
            "UPDATE company_stats s SET documents = greatest(0, s.documents + d.dd), \
             degree = greatest(0, s.degree + d.dg) \
             FROM unnest($1::uuid[], $2::bigint[], $3::bigint[]) d (id, dd, dg) \
             WHERE s.company_id = d.id",
            &[&sc, &sd, &sg],
        )
        .await?;
    }
    tx.exec("DELETE FROM company_graph_log WHERE id = ANY($1)", &[&ids])
        .await?;
    let more: bool = tx
        .one("SELECT EXISTS (SELECT 1 FROM company_graph_log)", &[])
        .await?
        .try_get(0)
        .map_err(pg)?;
    tx.commit().await.map_err(pg)?;
    Ok(Folded {
        rows: ids.len() as u64,
        more,
    })
}

/// The company listing's query and its arguments. The inner query finds the page's companies
/// in the order's index from the cursor, checking the filters on each (for the count orders
/// through a lateral fence, so that Postgres walks `company_stats` in index order instead of
/// joining every company: it cannot estimate the filters on props); the outer one reads only
/// the page's rows, identifiers and enrichment.
fn list_sql(
    filter: &CompanyFilter,
    order: CompanyOrder,
    after: Option<&CompanyCursor>,
    limit: u32,
) -> (String, Args) {
    let mut args = Args::default();
    let mut w = String::from(" AND n.props->>'merged_into' IS NULL");
    if let Some(p) = filter.name_prefix.as_deref().filter(|p| !p.is_empty()) {
        let p = args.push(p.to_owned());
        let _ = write!(
            w,
            " AND {NAME_KEY} >= (lower({p}::text) COLLATE \"C\") \
             AND {NAME_KEY} < ((lower({p}::text) || chr(1114111)) COLLATE \"C\")"
        );
    }
    if let Some(j) = filter.jurisdiction_key() {
        let j = args.push(j);
        let _ = write!(w, " AND upper(n.props->>'jurisdiction') = {j}");
    }
    if let Some(has) = filter.has_identifier {
        let _ = write!(
            w,
            " AND {}EXISTS (SELECT 1 FROM node_aliases a WHERE a.node_id = n.id \
             AND a.kind = 'company' AND starts_with(a.alias, 'id:'))",
            if has { "" } else { "NOT " }
        );
    }
    if let Some(yes) = filter.enriched {
        let _ = write!(
            w,
            " AND {}EXISTS (SELECT 1 FROM company_enrichment f WHERE f.company_id = n.id)",
            if yes { "" } else { "NOT " }
        );
    }
    let (inner, outer_order) = if order == CompanyOrder::Name {
        // The name index is walked as `o`; the filters are checked on `n` behind the fence.
        let ok = NAME_KEY.replace("n.props", "o.props");
        let mut range = String::new();
        if let Some(p) = filter.name_prefix.as_deref().filter(|p| !p.is_empty()) {
            let p = args.push(p.to_owned());
            let _ = write!(
                range,
                " AND {ok} >= (lower({p}::text) COLLATE \"C\") \
                 AND {ok} < ((lower({p}::text) || chr(1114111)) COLLATE \"C\")"
            );
        }
        if let Some(c) = after {
            let k = args.push(c.name.clone());
            let i = args.push(c.id);
            let _ = write!(
                range,
                " AND ({ok}, o.id) > (({k}::text COLLATE \"C\"), {i}::uuid)"
            );
        }
        let l = args.push(i64::from(limit));
        (
            format!(
                "SELECT o.id AS company_id FROM nodes o CROSS JOIN LATERAL (SELECT 1 \
                 FROM nodes n WHERE n.id = o.id{w} OFFSET 0) ok \
                 WHERE o.kind = 'company'{range} ORDER BY {ok}, o.id LIMIT {l}"
            ),
            format!("{NAME_KEY}, n.id"),
        )
    } else {
        let column = if order == CompanyOrder::Documents {
            "documents"
        } else {
            "degree"
        };
        let mut cursor = String::new();
        if let Some(c) = after {
            let k = args.push(i64::try_from(c.count).unwrap_or(i64::MAX));
            let i = args.push(c.id);
            let _ = write!(
                cursor,
                " WHERE (s.{column}, s.company_id) < ({k}::bigint, {i}::uuid)"
            );
        }
        let l = args.push(i64::from(limit));
        (
            format!(
                "SELECT s.company_id FROM company_stats s CROSS JOIN LATERAL (SELECT 1 \
                 FROM nodes n WHERE n.id = s.company_id AND n.kind = 'company'{w} OFFSET 0) ok\
                 {cursor} ORDER BY s.{column} DESC, s.company_id DESC LIMIT {l}"
            ),
            format!("st.{column} DESC, n.id DESC"),
        )
    };
    let s = format!(
        "SELECT {NODE_COLUMNS}, st.documents, st.degree, \
         EXISTS (SELECT 1 FROM company_enrichment f WHERE f.company_id = n.id), \
         ARRAY(SELECT a.alias FROM node_aliases a WHERE a.node_id = n.id \
         AND a.kind = 'company' AND starts_with(a.alias, 'id:') \
         ORDER BY a.alias COLLATE \"C\" LIMIT {COMPANY_IDENTIFIERS_MAX}), \
         {NAME_KEY}::text \
         FROM ({inner}) k JOIN nodes n ON n.id = k.company_id \
         JOIN company_stats st ON st.company_id = n.id ORDER BY {outer_order}"
    );
    (s, args)
}

pub(crate) async fn list_companies(
    pool: &Pool,
    filter: &CompanyFilter,
    order: CompanyOrder,
    after: Option<&CompanyCursor>,
    limit: u32,
    max_bytes: usize,
) -> Result<Vec<CompanyRow>> {
    let (sql, args) = list_sql(filter, order, after, limit);
    let c = client(pool).await?;
    // Uncached: its plan depends on the filters and the cursor.
    let rows = c.rows_uncached(&sql, &args.refs()).await?;
    let (mut out, mut bytes) = (Vec::new(), 0usize);
    for r in &rows {
        let row = CompanyRow {
            node: node_at(r, 0)?,
            documents: u64::try_from(r.try_get::<_, i64>(4).map_err(pg)?).unwrap_or(0),
            degree: u64::try_from(r.try_get::<_, i64>(5).map_err(pg)?).unwrap_or(0),
            enriched: r.try_get(6).map_err(pg)?,
            identifiers: r.try_get(7).map_err(pg)?,
            name_key: r.try_get(8).map_err(pg)?,
        };
        bytes += company_row_bytes(&row);
        out.push(row);
        if bytes >= max_bytes {
            break;
        }
    }
    Ok(out)
}

pub(crate) async fn company_links(
    pool: &Pool,
    companies: &[Uuid],
    min_documents: u32,
    limit: u32,
) -> Result<Vec<CompanyLink>> {
    let set: Vec<Uuid> = companies
        .iter()
        .copied()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    let c = client(pool).await?;
    let rows = c
        .rows(
            "SELECT a, b, docs, events, roles::text FROM company_links \
             WHERE a = ANY($1) AND b = ANY($1) AND (docs >= $2 OR events > 0) \
             ORDER BY docs DESC, events DESC, a, b LIMIT $3",
            &[&set, &i64::from(min_documents), &i64::from(limit)],
        )
        .await?;
    rows.iter()
        .map(|r| {
            Ok(LinkState {
                documents: r.try_get(2).map_err(pg)?,
                events: r.try_get(3).map_err(pg)?,
                roles: LinkState::parse_roles(r.try_get(4).map_err(pg)?),
            }
            .link(r.try_get(0).map_err(pg)?, r.try_get(1).map_err(pg)?))
        })
        .collect()
}
