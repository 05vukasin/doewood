//! `companies_due` on generated companies: the query returns what the query before issue #71
//! returned, for every owner and page size, and an ignored test times both and how long they
//! hold the store's connection (docs/enrichment.md, "`companies_due` cost").

use std::sync::Arc;
use std::time::Instant;

use doewood_core::{InstanceId, MasterKey};
use doewood_model::{DueReason, FreshnessStore};
use rusqlite::{Connection, params};

use super::COMPANIES_DUE;
use crate::Store;
use crate::standalone::Standalone;
use crate::store::now;

/// The query before issue #71, kept as the reference the current one must match.
const BEFORE: &str = "SELECT id, docs, documents FROM ( \
     SELECT n.id AS id, (SELECT count(DISTINCT e.src) FROM edges e JOIN nodes d ON d.id = e.src \
     WHERE e.dst = n.id AND e.kind = 'party' AND d.kind = 'document' \
     AND e.state NOT IN ('retracted', 'superseded')) AS docs, f.documents AS documents, \
     f.company_id AS enriched \
     FROM nodes n LEFT JOIN company_enrichment f ON f.company_id = n.id \
     WHERE n.kind = 'company' AND n.id > coalesce(?2, '') \
     AND NOT EXISTS (SELECT 1 FROM company_leases l \
     WHERE l.company_id = n.id AND l.owner <> ?1 AND l.expires_at > ?4)) \
     WHERE docs > 0 AND (enriched IS NULL OR docs >= 2 * max(documents, 1) \
     OR EXISTS (SELECT 1 FROM company_registry_due r \
     WHERE r.company_id = id AND r.next_due_at <= ?4)) \
     ORDER BY id LIMIT ?3";

/// `n` generated companies with ten `party` edges each; the cases as in the Postgres test
/// (`doewood-pg`, `tests/companies_due.rs`). Without `mixed`, the steady state: every company
/// enriched with its current count and no registry due.
fn seed(conn: &Connection, n: u32, mixed: bool) {
    let m = i32::from(mixed);
    let t = now();
    conn.execute_batch(&format!(
        "CREATE TEMP TABLE seed_co AS
           WITH RECURSIVE s (i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM s WHERE i < {n})
           SELECT i, printf('%08x-0000-4000-8000-%012x', (i * 2654435761) % 4294967296, i) AS id
           FROM s;
         INSERT INTO nodes (id, kind, key, created_at, updated_at)
           SELECT id, 'company', 'co-' || i, {t}, {t} FROM seed_co;
         CREATE TEMP TABLE seed_doc AS
           WITH RECURSIVE j (j) AS (SELECT 0 UNION ALL SELECT j + 1 FROM j WHERE j < 9)
           SELECT c.i, c.id AS co, j.j,
                  printf('%08x-%04x-4000-9000-%012x', (c.i * 2654435761) % 4294967296, j.j, c.i)
                  AS id
           FROM seed_co c CROSS JOIN j WHERE {m} = 0 OR c.i % 13 <> 0;
         INSERT INTO nodes (id, kind, key, created_at, updated_at)
           SELECT id, 'document', 'doc-' || i || '-' || j, {t}, {t} FROM seed_doc;
         INSERT INTO edges (id, src, dst, kind, confidence, state, source, created_at, updated_at)
           SELECT 'e-' || id,
                  CASE WHEN {m} = 1 AND i % 17 = 0 AND j = 0
                       THEN (SELECT id FROM seed_co WHERE seed_co.i = 1) ELSE id END,
                  co, 'party', 0.9,
                  CASE WHEN {m} = 1 AND i % 3 = 0 AND j IN (8, 9) THEN 'retracted'
                       WHEN {m} = 1 AND i % 3 = 0 AND j = 7 THEN 'superseded'
                       WHEN {m} = 1 AND i % 2 = 0 AND j = 6 THEN 'proposed'
                       ELSE 'active' END,
                  'document', {t}, {t}
           FROM seed_doc;
         INSERT INTO edges (id, src, dst, kind, confidence, state, source, created_at, updated_at)
           SELECT 'm-' || id, id, co, 'party', 0.9, 'active', 'manual', {t}, {t}
           FROM seed_doc WHERE {m} = 1 AND i % 11 = 0 AND j = 1;
         INSERT INTO edges (id, src, dst, kind, confidence, state, source, valid_from,
                            created_at, updated_at)
           SELECT 'r' || k || '-' || d.id, d.id, d.co, 'party', 0.9, 'retracted', 'document',
                  2458850 + k, {t}, {t}
           FROM seed_doc d CROSS JOIN (SELECT 1 AS k UNION ALL SELECT 2 UNION ALL SELECT 3
                                       UNION ALL SELECT 4)
           WHERE {m} = 1 AND d.i % 4 = 0 AND d.j IN (2, 3, 4);
         INSERT INTO company_enrichment (company_id, enriched_at, documents)
           SELECT c.id, {t} - 86400,
                  CASE WHEN {m} = 1 AND c.i % 19 = 0 THEN 0
                       WHEN {m} = 1 AND c.i % 7 = 0 THEN 4
                       ELSE (SELECT count(DISTINCT e.src) FROM edges e JOIN nodes d ON d.id = e.src
                             WHERE e.dst = c.id AND e.kind = 'party' AND d.kind = 'document'
                             AND e.state NOT IN ('retracted', 'superseded')) END
           FROM seed_co c WHERE {m} = 0 OR c.i % 5 <> 0;
         INSERT INTO company_registry_due (company_id, registry, next_due_at)
           SELECT company_id, 'gleif', {t} + 30 * 86400 FROM company_enrichment;
         INSERT INTO company_registry_due (company_id, registry, next_due_at)
           SELECT c.id, r.r, {t} - 3600
           FROM seed_co c JOIN company_enrichment f ON f.company_id = c.id
           CROSS JOIN (SELECT 'vies' AS r UNION ALL SELECT 'edgar') r
           WHERE {m} = 1 AND c.i % 6 = 0;
         INSERT INTO company_leases (company_id, owner, leased_at, expires_at)
           SELECT id, CASE WHEN i % 23 = 0 THEN 'other' ELSE 'me' END, {t}, {t} + 3600
           FROM seed_co WHERE {m} = 1 AND (i % 23 = 0 OR i % 29 = 0);
         INSERT INTO company_leases (company_id, owner, leased_at, expires_at)
           SELECT id, 'other', {t} - 7200, {t} - 3600
           FROM seed_co WHERE {m} = 1 AND i % 31 = 0 AND i % 23 <> 0 AND i % 29 <> 0;
         DROP TABLE seed_co;
         DROP TABLE seed_doc;
         ANALYZE;"
    ))
    .unwrap();
}

type Row = (String, i64, Option<i64>);

fn page(conn: &Connection, sql: &str, owner: &str, after: Option<&str>, limit: i64) -> Vec<Row> {
    let mut stmt = conn.prepare_cached(sql).unwrap();
    stmt.query_map(params![owner, after, limit, now()], |r| {
        Ok((r.get(0)?, r.get(1)?, r.get(2)?))
    })
    .unwrap()
    .collect::<rusqlite::Result<_>>()
    .unwrap()
}

fn walk(conn: &Connection, sql: &str, owner: &str, limit: i64) -> Vec<Vec<Row>> {
    let mut pages = Vec::new();
    let mut after: Option<String> = None;
    loop {
        let p = page(conn, sql, owner, after.as_deref(), limit);
        let Some(last) = p.last() else { break };
        after = Some(last.0.clone());
        pages.push(p);
    }
    pages
}

/// The query before issue #139 (no `INDEXED BY`): the same rows, but SQLite sorted every
/// company for each page.
fn before_139() -> String {
    COMPANIES_DUE.replace(" INDEXED BY nodes_companies", "")
}

fn plan(conn: &Connection, sql: &str) -> String {
    let mut stmt = conn.prepare(&format!("EXPLAIN QUERY PLAN {sql}")).unwrap();
    stmt.query_map(params!["me", None::<String>, 100, now()], |r| {
        r.get::<_, String>(3)
    })
    .unwrap()
    .collect::<rusqlite::Result<Vec<_>>>()
    .unwrap()
    .join("\n")
}

#[tokio::test]
async fn companies_due_matches_the_query_before_issue_71() {
    // With the planner statistics `seed` writes, then without them (a store ingest grew and
    // nothing analysed yet).
    for stats in [true, false] {
        companies_due_matches(stats).await;
    }
}

async fn companies_due_matches(stats: bool) {
    let store = Arc::new(Store::open_in_memory(&MasterKey::from_bytes([1; 32])).unwrap());
    seed(&store.conn(), 1000, true);
    if !stats {
        store
            .conn()
            .execute_batch("DELETE FROM sqlite_stat1; ANALYZE sqlite_schema;")
            .unwrap();
    }
    // Issue #139: the companies in id order from the page token, no sort of them all.
    let p = plan(&store.conn(), COMPANIES_DUE);
    assert!(
        p.contains("USING COVERING INDEX nodes_companies (id>?)"),
        "{p}"
    );
    assert!(!p.contains("TEMP B-TREE FOR ORDER BY"), "{p}");
    let before_139 = before_139();
    if !stats {
        // What the query did without `INDEXED BY`, as on the store of issue #139.
        let p = plan(&store.conn(), &before_139);
        assert!(p.contains("TEMP B-TREE FOR ORDER BY"), "{p}");
    }
    let s = Standalone::new(Arc::clone(&store));
    for owner in ["me", "other", "third"] {
        let mut sizes = Vec::new();
        for limit in [1, 7, 100, 500] {
            let (before, before_139, after) = {
                let conn = store.conn();
                (
                    walk(&conn, BEFORE, owner, limit),
                    walk(&conn, &before_139, owner, limit),
                    walk(&conn, COMPANIES_DUE, owner, limit),
                )
            };
            assert_eq!(before, after, "owner {owner}, limit {limit}, stats {stats}");
            assert_eq!(
                before_139, after,
                "owner {owner}, limit {limit}, stats {stats}"
            );
            sizes.push(after.iter().map(Vec::len).sum::<usize>());
            let instance = InstanceId::new(owner).unwrap();
            let mut token = None;
            for p in &after {
                let got: Vec<(String, i64)> = s
                    .companies_due(&instance, token, u32::try_from(limit).unwrap())
                    .await
                    .unwrap()
                    .iter()
                    .map(|d| (d.company.to_string(), i64::try_from(d.documents).unwrap()))
                    .collect();
                let want: Vec<(String, i64)> = p.iter().map(|r| (r.0.clone(), r.1)).collect();
                assert_eq!(got, want, "owner {owner}, limit {limit}");
                token = Some(p.last().unwrap().0.parse().unwrap());
            }
        }
        assert!(sizes[0] > 100, "{sizes:?}");
        assert!(sizes.windows(2).all(|w| w[0] == w[1]), "{sizes:?}");
    }
    let reasons: Vec<DueReason> = s
        .companies_due(&InstanceId::new("me").unwrap(), None, 1000)
        .await
        .unwrap()
        .into_iter()
        .map(|d| d.reason)
        .collect();
    for r in [DueReason::New, DueReason::Doubled, DueReason::Scheduled] {
        assert!(reasons.contains(&r), "{r:?}");
    }
}

/// The median of `runs` timings of `f`, in milliseconds.
fn median_ms(runs: usize, mut f: impl FnMut()) -> f64 {
    f();
    let mut ms = Vec::with_capacity(runs);
    for _ in 0..runs {
        let t = Instant::now();
        f();
        ms.push(t.elapsed().as_secs_f64() * 1000.0);
    }
    ms.sort_by(f64::total_cmp);
    ms[runs / 2]
}

/// Times one page of 100 with the query before and after issue #71 on
/// `DOEWOOD_BENCH_COMPANIES` generated companies (default 10,000), in a store on disk. The
/// query runs on the store's only connection, so this is also how long it holds it. See
/// docs/enrichment.md for the command and the numbers.
#[test]
#[ignore = "a measurement; run by hand"]
fn companies_due_timings() {
    let n: u32 = std::env::var("DOEWOOD_BENCH_COMPANIES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(10_000);
    for mixed in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(&dir.path().join("s.db"), &MasterKey::from_bytes([1; 32])).unwrap();
        let conn = store.conn();
        let t = Instant::now();
        seed(&conn, n, mixed);
        let edges: i64 = conn
            .query_row("SELECT count(*) FROM edges", [], |r| r.get(0))
            .unwrap();
        let state = if mixed { "mixed" } else { "steady state" };
        println!(
            "{n} companies, {edges} party edges, {state} (seeded in {:.1} s); median of 7 runs",
            t.elapsed().as_secs_f64()
        );
        let mut ms = [0.0; 2];
        for (i, sql) in [BEFORE, COMPANIES_DUE].into_iter().enumerate() {
            ms[i] = median_ms(7, || drop(page(&conn, sql, "me", None, 100)));
        }
        println!(
            "first page of 100: before {:>9.2} ms  after {:>9.2} ms",
            ms[0], ms[1]
        );
        let before_139 = before_139();
        for (label, sql) in [
            ("before #139", before_139.as_str()),
            ("after", COMPANIES_DUE),
        ] {
            let t = Instant::now();
            let pages = walk(&conn, sql, "me", 100);
            println!(
                "a pass in pages of 100, {label}: {:.1} ms, {} pages",
                t.elapsed().as_secs_f64() * 1000.0,
                pages.len()
            );
        }
        for (label, sql) in [
            ("before", BEFORE),
            ("before #139", before_139.as_str()),
            ("after", COMPANIES_DUE),
        ] {
            println!("--- {label}, {state}\n{}", plan(&conn, sql));
        }
    }
}
