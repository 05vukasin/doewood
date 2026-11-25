//! `companies_due` on generated companies: the query returns what the query before issue #71
//! returned, for both owners and every page size, and an ignored test times both
//! (docs/enrichment.md, "`companies_due` cost").

use std::time::Instant;

use doewood_core::InstanceId;
use doewood_model::{DueCompany, FreshnessStore};
use uuid::Uuid;

use super::{Scratch, test_url};
use crate::enrichment::COMPANIES_DUE;

/// The query before issue #71, kept as the reference the current one must match.
const BEFORE: &str = "SELECT n.id, c.docs, f.documents, \
     ARRAY(SELECT r.registry FROM company_registry_due r \
     WHERE r.company_id = n.id AND r.next_due_at <= now() \
     ORDER BY r.registry COLLATE \"C\") \
     FROM nodes n CROSS JOIN LATERAL (SELECT (SELECT count(DISTINCT e.src) FROM edges e \
     JOIN nodes d ON d.id = e.src WHERE e.dst = n.id AND e.kind = 'party' \
     AND d.kind = 'document' AND e.state NOT IN ('retracted', 'superseded')) AS docs) c \
     LEFT JOIN company_enrichment f ON f.company_id = n.id \
     WHERE n.kind = 'company' AND ($2::uuid IS NULL OR n.id > $2) \
     AND c.docs > 0 \
     AND NOT EXISTS (SELECT 1 FROM company_leases l WHERE l.company_id = n.id \
     AND l.owner <> $1 AND l.expires_at > now()) \
     AND (f.company_id IS NULL OR c.docs >= 2 * greatest(f.documents, 1) \
     OR EXISTS (SELECT 1 FROM company_registry_due r \
     WHERE r.company_id = n.id AND r.next_due_at <= now())) \
     ORDER BY n.id LIMIT $3";

/// `n` generated companies with ten `party` edges each, from documents of their own.
///
/// With `mixed`, every case the query tells apart: company `i` is never enriched when
/// `i % 5 == 0`; enriched with fewer documents than now (doubled) when `i % 7 == 0`, with none
/// when `i % 19 == 0`; has a registry due when `i % 6 == 0`; has no edges when `i % 13 == 0`;
/// two retracted and one superseded edge when `i % 3 == 0`; one edge from a company instead of
/// a document when `i % 17 == 0`; a second edge from the same document (another source) when
/// `i % 11 == 0`, and twelve more retracted ones when `i % 4 == 0`; another instance's lease
/// when `i % 23 == 0`, the caller's own when `i % 29 == 0`, an expired one when `i % 31 == 0`.
///
/// Without `mixed`, the steady state: every company enriched with its current count and no
/// registry due, so nothing is due.
fn seed(n: u32, mixed: bool) -> String {
    let m = i32::from(mixed);
    format!(
        "CREATE TEMP TABLE seed_co AS
           SELECT i, gen_random_uuid() AS id FROM generate_series(1, {n}) i;
         INSERT INTO nodes (id, kind, key) SELECT id, 'company', 'co-' || i FROM seed_co;
         CREATE TEMP TABLE seed_doc AS
           SELECT c.i, c.id AS co, j, gen_random_uuid() AS id
           FROM seed_co c CROSS JOIN generate_series(0, 9) j
           WHERE {m} = 0 OR c.i % 13 <> 0;
         INSERT INTO nodes (id, kind, key)
           SELECT id, 'document', 'doc-' || i || '-' || j FROM seed_doc;
         INSERT INTO edges (id, src, dst, kind, confidence, state, source)
           SELECT gen_random_uuid(),
                  CASE WHEN {m} = 1 AND i % 17 = 0 AND j = 0
                       THEN (SELECT id FROM seed_co WHERE seed_co.i = 1) ELSE id END,
                  co, 'party', 0.9,
                  CASE WHEN {m} = 1 AND i % 3 = 0 AND j IN (8, 9) THEN 'retracted'
                       WHEN {m} = 1 AND i % 3 = 0 AND j = 7 THEN 'superseded'
                       WHEN {m} = 1 AND i % 2 = 0 AND j = 6 THEN 'proposed'
                       ELSE 'active' END,
                  'document'
           FROM seed_doc;
         INSERT INTO edges (id, src, dst, kind, confidence, state, source)
           SELECT gen_random_uuid(), id, co, 'party', 0.9, 'active', 'manual'
           FROM seed_doc WHERE {m} = 1 AND i % 11 = 0 AND j = 1;
         INSERT INTO edges (id, src, dst, kind, confidence, state, source, valid_from)
           SELECT gen_random_uuid(), d.id, d.co, 'party', 0.9, 'retracted', 'document',
                  date '2020-01-01' + k
           FROM seed_doc d CROSS JOIN generate_series(1, 4) k
           WHERE {m} = 1 AND d.i % 4 = 0 AND d.j IN (2, 3, 4);
         INSERT INTO company_enrichment (company_id, enriched_at, documents)
           SELECT c.id, now() - interval '1 day',
                  CASE WHEN {m} = 1 AND c.i % 19 = 0 THEN 0
                       WHEN {m} = 1 AND c.i % 7 = 0 THEN 4
                       ELSE (SELECT count(DISTINCT e.src) FROM edges e JOIN nodes d ON d.id = e.src
                             WHERE e.dst = c.id AND e.kind = 'party' AND d.kind = 'document'
                             AND e.state NOT IN ('retracted', 'superseded')) END
           FROM seed_co c WHERE {m} = 0 OR c.i % 5 <> 0;
         INSERT INTO company_registry_due (company_id, registry, next_due_at)
           SELECT f.company_id, 'gleif', now() + interval '30 days' FROM company_enrichment f;
         INSERT INTO company_registry_due (company_id, registry, next_due_at)
           SELECT c.id, r, now() - interval '1 hour'
           FROM seed_co c JOIN company_enrichment f ON f.company_id = c.id
           CROSS JOIN (VALUES ('vies'), ('edgar')) v (r)
           WHERE {m} = 1 AND c.i % 6 = 0;
         INSERT INTO company_leases (company_id, owner, leased_at, expires_at)
           SELECT id, CASE WHEN i % 23 = 0 THEN 'other' ELSE 'me' END, now(),
                  now() + interval '1 hour'
           FROM seed_co WHERE {m} = 1 AND (i % 23 = 0 OR i % 29 = 0);
         INSERT INTO company_leases (company_id, owner, leased_at, expires_at)
           SELECT id, 'other', now() - interval '2 hours', now() - interval '1 hour'
           FROM seed_co WHERE {m} = 1 AND i % 31 = 0 AND i % 23 <> 0 AND i % 29 <> 0;
         DROP TABLE seed_co, seed_doc;"
    )
}

/// [`seed`], then `VACUUM ANALYZE` (the visibility map as autovacuum would leave it).
async fn seeded(c: &tokio_postgres::Client, n: u32, mixed: bool) {
    c.batch_execute(&seed(n, mixed)).await.unwrap();
    for t in [
        "nodes",
        "edges",
        "company_enrichment",
        "company_registry_due",
        "company_leases",
    ] {
        c.batch_execute(&format!("VACUUM ANALYZE {t}"))
            .await
            .unwrap();
    }
}

type Row = (Uuid, i64, Option<i64>, Vec<String>);

async fn page(
    c: &tokio_postgres::Client,
    sql: &str,
    owner: &str,
    after: Option<Uuid>,
    limit: i64,
) -> Vec<Row> {
    c.query(sql, &[&owner, &after, &limit])
        .await
        .unwrap()
        .iter()
        .map(|r| (r.get(0), r.get(1), r.get(2), r.get(3)))
        .collect()
}

/// Every page, in order.
async fn walk(c: &tokio_postgres::Client, sql: &str, owner: &str, limit: i64) -> Vec<Vec<Row>> {
    let mut pages = Vec::new();
    let mut after = None;
    loop {
        let p = page(c, sql, owner, after, limit).await;
        let Some(last) = p.last() else { break };
        after = Some(last.0);
        pages.push(p);
    }
    pages
}

#[tokio::test]
async fn companies_due_matches_the_query_before_issue_71() {
    let Some(url) = test_url() else { return };
    let db = Scratch::new(&url).await;
    let store = db.store().await;
    let c = db.client().await;
    // 1000 companies cover every combination of the cases in `seed` that matter.
    seeded(&c, 1000, true).await;
    for owner in ["me", "other", "third"] {
        let mut sizes = Vec::new();
        for limit in [1, 7, 100, 500] {
            let before = walk(&c, BEFORE, owner, limit).await;
            let now = walk(&c, COMPANIES_DUE, owner, limit).await;
            assert_eq!(before, now, "owner {owner}, limit {limit}");
            sizes.push(now.iter().map(Vec::len).sum::<usize>());
            // The store method pages the same way and gives the same reasons.
            let instance = InstanceId::new(owner).unwrap();
            let mut after = None;
            for p in &now {
                let got: Vec<DueCompany> = store
                    .companies_due(&instance, after, u32::try_from(limit).unwrap())
                    .await
                    .unwrap();
                let got: Vec<Row> = got
                    .iter()
                    .map(|d| {
                        (
                            d.company,
                            i64::try_from(d.documents).unwrap(),
                            None,
                            d.due.clone(),
                        )
                    })
                    .collect();
                let want: Vec<Row> = p.iter().map(|r| (r.0, r.1, None, r.3.clone())).collect();
                assert_eq!(got, want, "owner {owner}, limit {limit}");
                after = Some(p.last().unwrap().0);
            }
        }
        // Something was compared, the same set at every page size.
        assert!(sizes[0] > 100, "{sizes:?}");
        assert!(sizes.windows(2).all(|w| w[0] == w[1]), "{sizes:?}");
    }
    // Every reason shows up.
    let reasons: Vec<_> = store
        .companies_due(&InstanceId::new("me").unwrap(), None, 1000)
        .await
        .unwrap()
        .into_iter()
        .map(|d| d.reason)
        .collect();
    for r in [
        doewood_model::DueReason::New,
        doewood_model::DueReason::Doubled,
        doewood_model::DueReason::Scheduled,
    ] {
        assert!(reasons.contains(&r), "{r:?}");
    }
    db.drop_schema().await;
}

/// The median of `runs` timings of `f`, in milliseconds.
async fn median_ms<F: AsyncFnMut()>(runs: usize, mut f: F) -> f64 {
    f().await;
    let mut ms = Vec::with_capacity(runs);
    for _ in 0..runs {
        let t = Instant::now();
        f().await;
        ms.push(t.elapsed().as_secs_f64() * 1000.0);
    }
    ms.sort_by(f64::total_cmp);
    ms[runs / 2]
}

/// Times the query before and after issue #71 on `DOEWOOD_BENCH_COMPANIES` generated
/// companies (default 10,000) with ten party edges each, in the steady state (nothing due) and
/// mixed, and prints the plans. See docs/enrichment.md for the command and the numbers.
#[tokio::test]
#[ignore = "a measurement; run by hand"]
async fn companies_due_timings() {
    let Some(url) = test_url() else { return };
    let n: u32 = std::env::var("DOEWOOD_BENCH_COMPANIES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(10_000);
    for mixed in [false, true] {
        let db = Scratch::new(&url).await;
        drop(db.store().await);
        let c = db.client().await;
        let t = Instant::now();
        seeded(&c, n, mixed).await;
        let edges: i64 = c
            .query_one("SELECT count(*) FROM edges", &[])
            .await
            .unwrap()
            .get(0);
        let state = if mixed { "mixed" } else { "steady state" };
        println!(
            "{n} companies, {edges} party edges, {state} (seeded in {:.1} s); median of 7 runs",
            t.elapsed().as_secs_f64()
        );
        let mut ms = [0.0; 2];
        for (i, sql) in [BEFORE, COMPANIES_DUE].into_iter().enumerate() {
            ms[i] = median_ms(7, async || drop(page(&c, sql, "me", None, 100).await)).await;
        }
        println!(
            "first page of 100: before {:>9.2} ms  after {:>9.2} ms",
            ms[0], ms[1]
        );
        for (label, sql) in [("before", BEFORE), ("after", COMPANIES_DUE)] {
            let plan = c
                .query(
                    &format!("EXPLAIN (ANALYZE, BUFFERS) {sql}"),
                    &[&"me", &None::<Uuid>, &100_i64],
                )
                .await
                .unwrap();
            println!("--- {label}, {state}");
            for row in plan {
                println!("{}", row.get::<_, String>(0));
            }
        }
        db.drop_schema().await;
    }
}
