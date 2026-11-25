//! Search with a company filter on generated data (issue #111), as in the Postgres test
//! (`doewood-pg`, `tests/search_company.rs`): the query driven by the company's party edges
//! returns what the correlated `EXISTS` before it returned, and an ignored test shows both
//! plans and times (docs/api.md, "Search by company").

use std::fmt::Write as _;
use std::sync::Arc;
use std::time::Instant;

use doewood_core::MasterKey;
use doewood_model::{SearchQuery, SearchStore};
use rusqlite::types::Value as Sql;
use rusqlite::{Connection, params_from_iter};
use time::{Date, Month};
use uuid::Uuid;

use super::{Standalone, julian};
use crate::Store;

/// The company filter before issue #111, kept as the reference.
const BEFORE: &str = " AND EXISTS (SELECT 1 FROM nodes d JOIN edges e ON e.src = d.id \
     WHERE d.kind = 'document' AND d.key = lower(hex(s.blake3)) \
     AND e.kind = 'party' AND e.dst = ?1 \
     AND e.state NOT IN ('retracted', 'superseded'))";

/// The company filter after issue #111, as `Standalone::search` writes it.
const AFTER: &str = " AND s.blake3 IN (SELECT unhex(d.key) FROM edges e \
     JOIN nodes d ON d.id = e.src \
     WHERE e.dst = ?1 AND e.kind = 'party' \
     AND e.state NOT IN ('retracted', 'superseded') AND d.kind = 'document')";

/// Company `c` of [`seed`].
fn company(c: u32) -> Uuid {
    format!("00000000-0000-4000-8000-{c:012}").parse().unwrap()
}

/// `n` generated documents and `companies` companies, as in the Postgres test: words, kinds,
/// dates and party edges (states, a second manual edge, `cites` edges, the odd document nodes
/// of company 1) follow the same rules.
fn seed(conn: &Connection, n: u32, companies: u32) {
    conn.execute_batch(&format!(
        "CREATE TEMP TABLE seed_docs AS
           WITH RECURSIVE s (i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM s WHERE i < {n})
           SELECT i, randomblob(32) AS h, printf('d-%d', i) AS id FROM s;
         INSERT INTO records (blake3, body_json, versions_json, updated_at)
           SELECT h, json_object('n', i), '{{\"quail\":\"1\"}}', 0 FROM seed_docs;
         INSERT INTO search_index (blake3, title, body, kind, doc_date, amount, indexed_at)
           SELECT h, 'Document ' || i || CASE WHEN i % 3 = 0 THEN ' alpha' ELSE '' END,
                  lower(hex(randomblob(16))) || CASE WHEN i % 5 = 0 THEN ' beta' ELSE '' END
                    || ' w' || (i % 100),
                  CASE i % 3 WHEN 0 THEN 'invoice' WHEN 1 THEN 'contract' END,
                  CASE WHEN i % 7 = 0 THEN NULL ELSE 2458850 + i % 1500 END,
                  i % 1000, 0
           FROM seed_docs WHERE i % 17 <> 0;
         INSERT INTO nodes (id, kind, key, created_at, updated_at)
           SELECT id, 'document', lower(hex(h)), 0, 0 FROM seed_docs;
         CREATE TEMP TABLE seed_co AS
           WITH RECURSIVE s (c) AS (SELECT 0 UNION ALL SELECT c + 1 FROM s WHERE c < {companies})
           SELECT c, printf('00000000-0000-4000-8000-%012d', c) AS id FROM s;
         INSERT INTO nodes (id, kind, key, created_at, updated_at)
           SELECT id, 'company', 'co-' || c, 0, 0 FROM seed_co;
         INSERT INTO edges (id, src, dst, kind, confidence, state, source, created_at, updated_at)
           SELECT 'p-' || d.i, d.id, co.id, 'party', 0.9,
                  CASE WHEN d.i % 11 = 0 THEN 'retracted' WHEN d.i % 13 = 0 THEN 'superseded'
                       WHEN d.i % 4 = 0 THEN 'proposed' ELSE 'active' END, 'document', 0, 0
           FROM seed_docs d JOIN seed_co co ON co.c = 1 + d.i % {companies};
         INSERT INTO edges (id, src, dst, kind, confidence, state, source, created_at, updated_at)
           SELECT 'm-' || d.i, d.id, co.id, 'party', 0.9, 'active', 'manual', 0, 0
           FROM seed_docs d JOIN seed_co co ON co.c = 1 + d.i % {companies} WHERE d.i % 9 = 0;
         INSERT INTO edges (id, src, dst, kind, confidence, state, source, created_at, updated_at)
           SELECT 'z-' || d.i, d.id, co.id, 'party', 0.9, 'active', 'document', 0, 0
           FROM seed_docs d JOIN seed_co co ON co.c = 0 WHERE d.i % 2 = 0;
         INSERT INTO edges (id, src, dst, kind, confidence, state, source, created_at, updated_at)
           SELECT 'c-' || d.i, d.id, co.id, 'cites', 0.9, 'active', 'document', 0, 0
           FROM seed_docs d JOIN seed_co co ON co.c = 1 WHERE d.i % 19 = 0;
         CREATE TEMP TABLE seed_odd AS
           SELECT 'r-' || i AS id, 'ref:number:' || i AS key FROM seed_docs WHERE i % 50 = 0;
         INSERT INTO nodes (id, kind, key, created_at, updated_at)
           SELECT id, 'document', key, 0, 0 FROM seed_odd;
         INSERT INTO edges (id, src, dst, kind, confidence, state, source, created_at, updated_at)
           SELECT 'o-' || o.id, o.id, co.id, 'party', 0.9, 'active', 'document', 0, 0
           FROM seed_odd o JOIN seed_co co ON co.c = 1;
         INSERT INTO edges (id, src, dst, kind, confidence, state, source, created_at, updated_at)
           SELECT 'k-2', a.id, b.id, 'party', 0.9, 'active', 'registry', 0, 0
           FROM seed_co a JOIN seed_co b ON a.c = 2 AND b.c = 1;
         DROP TABLE seed_docs;
         DROP TABLE seed_co;
         DROP TABLE seed_odd;
         ANALYZE;"
    ))
    .unwrap();
}

/// The search SQL as `Standalone::search` builds it, for the filters the cases use, with the
/// company filter `filter` (`?1` the company).
fn search_sql(q: &SearchQuery, filter: &str) -> (String, Vec<Sql>) {
    let mut args = vec![Sql::Text(q.company.unwrap().to_string())];
    let words = q.words();
    let mut sql = if words.is_empty() {
        "SELECT s.blake3, 0.0 AS score FROM search_index s WHERE 1".to_owned()
    } else {
        let phrase = words
            .iter()
            .map(|w| format!("\"{w}\""))
            .collect::<Vec<_>>()
            .join(" ");
        args.push(Sql::Text(phrase));
        "SELECT s.blake3, -bm25(search_fts, 10.0, 1.0) AS score \
         FROM search_fts JOIN search_index s ON s.rowid = search_fts.rowid \
         WHERE search_fts MATCH ?2"
            .to_owned()
    };
    sql.push_str(filter);
    if let Some(kind) = &q.kind {
        args.push(Sql::Text(kind.clone()));
        let _ = write!(sql, " AND s.kind = ?{}", args.len());
    }
    if let Some(d) = q.date_from {
        args.push(Sql::Integer(julian(d)));
        let _ = write!(sql, " AND s.doc_date >= ?{}", args.len());
    }
    args.push(Sql::Integer(i64::from(q.effective_limit())));
    args.push(Sql::Integer(i64::from(q.offset)));
    let _ = write!(
        sql,
        " ORDER BY score DESC, s.doc_date IS NULL, s.doc_date DESC, s.blake3 \
         LIMIT ?{} OFFSET ?{}",
        args.len() - 1,
        args.len()
    );
    (sql, args)
}

fn run(conn: &Connection, q: &SearchQuery, filter: &str) -> Vec<(Vec<u8>, f32)> {
    let (sql, args) = search_sql(q, filter);
    let mut stmt = conn.prepare(&sql).unwrap();
    #[allow(clippy::cast_possible_truncation)]
    stmt.query_map(params_from_iter(args), |r| {
        Ok((r.get(0)?, r.get::<_, f64>(1)? as f32))
    })
    .unwrap()
    .collect::<rusqlite::Result<_>>()
    .unwrap()
}

fn query(company: Uuid, text: &str, kind: Option<&str>, from: Option<Date>) -> SearchQuery {
    SearchQuery {
        text: Some(text.to_owned()),
        company: Some(company),
        kind: kind.map(str::to_owned),
        date_from: from,
        ..SearchQuery::default()
    }
}

#[tokio::test]
async fn search_by_company_matches_the_query_before() {
    let store = Arc::new(Store::open_in_memory(&MasterKey::from_bytes([1; 32])).unwrap());
    seed(&store.conn(), 3000, 40);
    let s = Standalone::new(Arc::clone(&store));
    let from = Date::from_calendar_date(2022, Month::January, 1).unwrap();
    let mut total = 0;
    for co in [
        company(0),
        company(1),
        company(2),
        company(7),
        Uuid::from_u128(7),
    ] {
        for text in ["", "alpha", "alpha beta", "w7", "nothing"] {
            for kind in [None, Some("invoice")] {
                for date in [None, Some(from)] {
                    for (limit, offset) in [(20, 0), (100, 40), (500, 0), (1000, 900)] {
                        let mut q = query(co, text, kind, date);
                        (q.limit, q.offset) = (limit, offset);
                        let (before, after) = {
                            let conn = store.conn();
                            (run(&conn, &q, BEFORE), run(&conn, &q, AFTER))
                        };
                        assert_eq!(before, after, "{q:?}");
                        let got: Vec<(Vec<u8>, f32)> = s
                            .search(&q)
                            .await
                            .unwrap()
                            .into_iter()
                            .map(|h| (h.blake3.as_bytes().to_vec(), h.score))
                            .collect();
                        assert_eq!(got, after, "{q:?}");
                        total += after.len();
                    }
                }
            }
        }
    }
    assert!(total > 3_000, "{total}");
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

/// Plans and times search by company before and after issue #111 on `DOEWOOD_BENCH_DOCS`
/// generated documents (default 50,000) in a store on disk, one company per 10 documents and
/// company 0 on half of them. See docs/api.md for the command and the numbers.
#[test]
#[ignore = "a measurement; run by hand"]
fn search_by_company_timings() {
    let n: u32 = std::env::var("DOEWOOD_BENCH_DOCS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(50_000);
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(&dir.path().join("s.db"), &MasterKey::from_bytes([1; 32])).unwrap();
    let conn = store.conn();
    seed(&conn, n, n / 10);
    let cases = [
        ("10 documents, no words", query(company(7), "", None, None)),
        (
            "10 documents, a word",
            query(company(7), "alpha", None, None),
        ),
        ("half of all, no words", query(company(0), "", None, None)),
        (
            "half of all, a word",
            query(company(0), "alpha", None, None),
        ),
    ];
    for (name, q) in &cases {
        for (label, filter) in [("before", BEFORE), ("after", AFTER)] {
            let (sql, args) = search_sql(q, filter);
            let mut stmt = conn.prepare(&format!("EXPLAIN QUERY PLAN {sql}")).unwrap();
            let plan: Vec<String> = stmt
                .query_map(params_from_iter(args.clone()), |r| r.get(3))
                .unwrap()
                .collect::<rusqlite::Result<_>>()
                .unwrap();
            println!("--- {name}, {label}:\n{}", plan.join("\n"));
            let ms = median_ms(9, || {
                run(&conn, q, filter);
            });
            println!("{name}, {label}: {ms:.2} ms");
        }
    }
}
