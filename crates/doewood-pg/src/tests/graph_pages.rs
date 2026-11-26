//! Issue #112 on generated data: walking the pages of `neighbours` and `company_documents`
//! returns what the queries before the pages returned, once each and in the documented order,
//! and a page starts at its token in `edges_by_dst`.

use std::collections::BTreeSet;

use doewood_model::{
    Direction, EdgeCursor, EdgeKind, GraphStore, NeighbourQuery, NodeKind, page_full,
};
use tokio_postgres::types::ToSql;
use uuid::Uuid;

use super::{Scratch, test_url};
use crate::backend::Args;
use crate::pages::{company_documents_sql, neighbours_sql};

/// `n` documents, `companies` companies; every document a party of company 1 and of two
/// others, a third of the edges dated, one in eleven retracted, and a few company links.
fn seed(n: u32, companies: u32) -> String {
    format!(
        "CREATE TEMP TABLE seed_docs AS
           SELECT i, gen_random_uuid() AS id FROM generate_series(1, {n}) i;
         INSERT INTO nodes (id, kind, key, props)
           SELECT id, 'document', md5(i::text) || md5((-i)::text),
                  jsonb_build_object('n', i) FROM seed_docs;
         CREATE TEMP TABLE seed_co AS
           SELECT c, gen_random_uuid() AS id FROM generate_series(1, {companies}) c;
         INSERT INTO nodes (id, kind, key) SELECT id, 'company', 'co-' || c FROM seed_co;
         INSERT INTO edges (id, src, dst, kind, props, confidence, state, source, valid_from)
           SELECT gen_random_uuid(), d.id, co.id, 'party', jsonb_build_object('role', r.role),
                  0.9, CASE WHEN (d.i + r.c) % 11 = 0 THEN 'retracted' ELSE 'active' END,
                  'document',
                  CASE WHEN (d.i + r.c) % 3 = 0 THEN date '2024-01-01' + (d.i % 7) END
           FROM seed_docs d
           CROSS JOIN LATERAL (VALUES (1, 'owner'),
               (2 + d.i % ({companies} - 1), 'buyer'),
               (2 + (7 * d.i) % ({companies} - 1), 'supplier')) r (c, role)
           JOIN seed_co co ON co.c = r.c
           ON CONFLICT DO NOTHING;
         INSERT INTO edges (id, src, dst, kind, confidence, state, source)
           SELECT gen_random_uuid(), a.id, b.id, l.kind, 0.9, 'active', 'registry'
           FROM (VALUES (1, 2, 'parent_of'), (3, 1, 'parent_of'), (1, 4, 'subsidiary_of')) l (a, b, kind)
           JOIN seed_co a ON a.c = l.a JOIN seed_co b ON b.c = l.b;
         DROP TABLE seed_docs, seed_co;
         ANALYZE nodes, edges;"
    )
}

/// The neighbour query before the pages, kept as the reference: the edge ids it returned.
async fn neighbours_before(
    c: &tokio_postgres::Client,
    node: Uuid,
    q: &NeighbourQuery,
) -> Vec<Uuid> {
    let (at_node, other) = match q.direction {
        Direction::Out => ("e.src = $1", "e.dst"),
        Direction::In => ("e.dst = $1", "e.src"),
        Direction::Both => (
            "(e.src = $1 OR e.dst = $1)",
            "(CASE WHEN e.src = $1 THEN e.dst ELSE e.src END)",
        ),
    };
    let kinds: Vec<String> = q.edge_kinds.iter().map(|k| k.as_str().to_owned()).collect();
    let node_kind = q.node_kind.map(NodeKind::as_str);
    let mut sql =
        format!("SELECT e.id FROM edges e JOIN nodes n ON n.id = {other} WHERE {at_node}");
    sql.push_str(" AND (cardinality($2::text[]) = 0 OR e.kind = ANY($2))");
    sql.push_str(" AND ($3::text IS NULL OR n.kind = $3)");
    if !q.include_inactive {
        sql.push_str(" AND e.state NOT IN ('retracted', 'superseded')");
    }
    sql.push_str(" ORDER BY e.kind COLLATE \"C\", e.valid_from NULLS FIRST, e.id");
    c.query(&sql, &[&node, &kinds, &node_kind])
        .await
        .unwrap()
        .iter()
        .map(|r| r.get(0))
        .collect()
}

async fn company(c: &tokio_postgres::Client, n: u32) -> Uuid {
    c.query_one(
        "SELECT id FROM nodes WHERE kind = 'company' AND key = $1",
        &[&format!("co-{n}")],
    )
    .await
    .unwrap()
    .get(0)
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn graph_pages_match_the_queries_before() {
    let Some(url) = test_url() else { return };
    let db = Scratch::new(&url).await;
    let s = db.store().await;
    let c = db.client().await;
    c.batch_execute(&seed(600, 20)).await.unwrap();
    let doc: Uuid = c
        .query_one(
            "SELECT id FROM nodes WHERE kind = 'document' ORDER BY key LIMIT 1",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    let kinds: [&[EdgeKind]; 3] = [
        &[],
        &[EdgeKind::Party],
        &[EdgeKind::ParentOf, EdgeKind::Party],
    ];
    for node in [company(&c, 1).await, company(&c, 2).await, doc] {
        for direction in [Direction::Out, Direction::In, Direction::Both] {
            for edge_kinds in kinds {
                for (node_kind, include_inactive) in [
                    (None, false),
                    (Some(NodeKind::Document), false),
                    (None, true),
                ] {
                    let q = NeighbourQuery {
                        direction,
                        edge_kinds: edge_kinds.to_vec(),
                        node_kind,
                        at: None,
                        include_inactive,
                    };
                    let before = neighbours_before(&c, node, &q).await;
                    let whole: Vec<Uuid> = s
                        .neighbours(node, &q)
                        .await
                        .unwrap()
                        .iter()
                        .map(|n| n.edge.id)
                        .collect();
                    assert_eq!(whole, before, "{q:?}");
                    for limit in [7, 1000] {
                        let (mut walked, mut after) = (Vec::new(), None::<EdgeCursor>);
                        loop {
                            let page = s
                                .neighbours_page(node, &q, after.as_ref(), limit, usize::MAX)
                                .await
                                .unwrap();
                            let full = page_full(&page, limit, usize::MAX, |_| 0);
                            after = page.last().map(|n| EdgeCursor::of(&n.edge));
                            walked.extend(page.iter().map(|n| n.edge.id));
                            if !full {
                                break;
                            }
                        }
                        assert_eq!(walked, before, "{q:?}, pages of {limit}");
                    }
                }
            }
        }
    }
    for n in [1, 2, 19] {
        let id = company(&c, n).await;
        let before: BTreeSet<Uuid> = c
            .query(
                "SELECT DISTINCT n.id FROM edges e JOIN nodes n ON n.id = e.src \
                 WHERE e.dst = $1 AND e.kind = 'party' AND n.kind = 'document' \
                 AND e.state NOT IN ('retracted', 'superseded')",
                &[&id],
            )
            .await
            .unwrap()
            .iter()
            .map(|r| r.get(0))
            .collect();
        let whole: Vec<Uuid> = s
            .company_documents(id)
            .await
            .unwrap()
            .iter()
            .map(|n| n.id)
            .collect();
        assert_eq!(whole.iter().copied().collect::<BTreeSet<_>>(), before);
        assert_eq!(whole.len(), before.len(), "each once");
        let (mut walked, mut after) = (Vec::new(), None::<EdgeCursor>);
        loop {
            let page = s
                .company_documents_page(id, after.as_ref(), 13, usize::MAX)
                .await
                .unwrap();
            let full = page.len() >= 13;
            after = page.last().map(|d| d.edge);
            walked.extend(page.iter().map(|d| d.node.id));
            if !full {
                break;
            }
        }
        assert_eq!(walked, whole, "company {n}");
    }
    db.drop_schema().await;
}

#[tokio::test]
async fn graph_pages_start_at_their_token() {
    let Some(url) = test_url() else { return };
    let db = Scratch::new(&url).await;
    drop(db.store().await);
    let c = db.client().await;
    c.batch_execute(&seed(3000, 20)).await.unwrap();
    let id = company(&c, 1).await;
    let cursor = EdgeCursor {
        kind: EdgeKind::Party,
        valid_from: None,
        id: Uuid::nil(),
    };
    let incoming = NeighbourQuery {
        direction: Direction::In,
        edge_kinds: vec![EdgeKind::Party],
        ..NeighbourQuery::default()
    };
    let explain = async |sql: &str, params: &[&(dyn ToSql + Sync)]| -> String {
        c.query(&format!("EXPLAIN (COSTS OFF) {sql}"), params)
            .await
            .unwrap()
            .iter()
            .map(|r| r.get::<_, String>(0))
            .collect::<Vec<_>>()
            .join("\n")
    };
    for after in [None, Some(&cursor)] {
        let mut args = Args::default();
        args.push(id);
        let sql = neighbours_sql(&incoming, after, Some(50), &mut args).unwrap();
        let p = explain(&sql, &args.refs()).await;
        assert!(p.contains("Index Scan using edges_by_dst"), "{p}");
        assert!(!p.contains("Sort"), "{p}");
        if after.is_some() {
            assert!(
                p.contains("ROW(COALESCE(valid_from, '-infinity'::date), id) > ROW("),
                "{p}"
            );
        }

        let sql = company_documents_sql(after.is_some(), true);
        let (vf, nil, limit) = (None::<time::Date>, Uuid::nil(), 50_i64);
        let mut params: Vec<&(dyn ToSql + Sync)> = vec![&id];
        if after.is_some() {
            params.extend([&vf as &(dyn ToSql + Sync), &nil]);
        }
        params.push(&limit);
        let p = explain(&sql, &params).await;
        assert!(p.contains("Index Scan using edges_by_dst"), "{p}");
        // Each edge's earlier party edges of its document: an index probe per row.
        assert!(p.contains("Nested Loop Anti Join"), "{p}");
        assert!(p.contains("Index Cond: ((src = e.src)"), "{p}");
        assert!(!p.contains("Sort"), "{p}");
        if after.is_some() {
            assert!(
                p.contains("ROW(COALESCE(e.valid_from, '-infinity'::date), e.id) > ROW(")
                    || p.contains("ROW(COALESCE(valid_from, '-infinity'::date), id) > ROW("),
                "{p}"
            );
        }
    }
    db.drop_schema().await;
}
