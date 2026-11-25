//! Search with a company filter on generated data (issue #111): the query driven by the
//! company's party edges returns what the correlated `EXISTS` before it returned, for every
//! combination of words, filters and pages, and an ignored test shows both plans and times
//! (docs/api.md, "Search by company").

use std::fmt::Write as _;
use std::time::Instant;

use doewood_model::{SearchQuery, SearchStore};
use time::{Date, Month};
use tokio_postgres::types::ToSql;
use uuid::Uuid;

use super::{Scratch, test_url};

/// The company filter before issue #111, kept as the reference.
const BEFORE: &str = " AND EXISTS (SELECT 1 FROM nodes d JOIN edges e ON e.src = d.id \
     WHERE d.kind = 'document' AND d.key = encode(s.blake3, 'hex') \
     AND e.kind = 'party' AND e.dst = $1 \
     AND e.state NOT IN ('retracted', 'superseded'))";

/// The company filter after issue #111, as `PgStore::search` writes it.
const AFTER: &str = " AND s.blake3 IN (SELECT decode(CASE WHEN d.key ~ '^[0-9a-f]{64}$' \
     THEN d.key END, 'hex') FROM edges e JOIN nodes d ON d.id = e.src \
     WHERE e.dst = $1 AND e.kind = 'party' \
     AND e.state NOT IN ('retracted', 'superseded') AND d.kind = 'document')";

/// `n` generated documents: records, search rows (every 17th has none), document nodes and
/// `companies` companies.
///
/// - Words: every 3rd title has `alpha`, every 5th body `beta`, and every body `w<i % 100>`.
///   Kinds cycle invoice, contract, none; every 7th has no date.
/// - Party edges: document `i` is a party of company `1 + i % companies`, in state retracted
///   (every 11th), superseded (13th), proposed (4th) or active; every 9th has a second, manual
///   edge to it. Every even document is also a party of company 0. Every 19th has a `cites`
///   edge to company 1, which is not a party.
/// - Odd document nodes, all parties of company 1: a `ref:` placeholder per 50 documents, the
///   uppercase-hex key of every 53rd hash, and company 2 itself.
fn seed(n: u32, companies: u32) -> String {
    format!(
        "INSERT INTO records (blake3, record, versions)
         SELECT sha256(int4send(i)), jsonb_build_object('n', i), '{{\"quail\":\"1\"}}'
         FROM generate_series(1, {n}) i;
         INSERT INTO search_index (blake3, title, body, kind, doc_date, amount)
         SELECT sha256(int4send(i)),
                'Document ' || i || CASE WHEN i % 3 = 0 THEN ' alpha' ELSE '' END,
                repeat(md5(i::text) || ' ', 20) || CASE WHEN i % 5 = 0 THEN ' beta' ELSE '' END
                  || ' w' || (i % 100),
                (ARRAY['invoice', 'contract', NULL])[1 + i % 3],
                CASE WHEN i % 7 = 0 THEN NULL ELSE date '2020-01-01' + (i % 1500) END,
                i % 1000
         FROM generate_series(1, {n}) i WHERE i % 17 <> 0;
         CREATE TEMP TABLE seed_docs AS
           SELECT i, gen_random_uuid() AS id FROM generate_series(1, {n}) i;
         INSERT INTO nodes (id, kind, key)
           SELECT id, 'document', encode(sha256(int4send(i)), 'hex') FROM seed_docs;
         CREATE TEMP TABLE seed_co AS
           SELECT c, ('00000000-0000-4000-8000-' || lpad(c::text, 12, '0'))::uuid AS id
           FROM generate_series(0, {companies}) c;
         INSERT INTO nodes (id, kind, key) SELECT id, 'company', 'co-' || c FROM seed_co;
         INSERT INTO edges (id, src, dst, kind, confidence, state, source)
           SELECT gen_random_uuid(), d.id, co.id, 'party', 0.9,
                  CASE WHEN d.i % 11 = 0 THEN 'retracted' WHEN d.i % 13 = 0 THEN 'superseded'
                       WHEN d.i % 4 = 0 THEN 'proposed' ELSE 'active' END, 'document'
           FROM seed_docs d JOIN seed_co co ON co.c = 1 + d.i % {companies};
         INSERT INTO edges (id, src, dst, kind, confidence, state, source)
           SELECT gen_random_uuid(), d.id, co.id, 'party', 0.9, 'active', 'manual'
           FROM seed_docs d JOIN seed_co co ON co.c = 1 + d.i % {companies} WHERE d.i % 9 = 0;
         INSERT INTO edges (id, src, dst, kind, confidence, state, source)
           SELECT gen_random_uuid(), d.id, co.id, 'party', 0.9, 'active', 'document'
           FROM seed_docs d JOIN seed_co co ON co.c = 0 WHERE d.i % 2 = 0;
         INSERT INTO edges (id, src, dst, kind, confidence, state, source)
           SELECT gen_random_uuid(), d.id, co.id, 'cites', 0.9, 'active', 'document'
           FROM seed_docs d JOIN seed_co co ON co.c = 1 WHERE d.i % 19 = 0;
         CREATE TEMP TABLE seed_odd AS
           SELECT gen_random_uuid() AS id, 'ref:number:' || i AS key
           FROM generate_series(1, {n}) i WHERE i % 50 = 0
           UNION ALL
           SELECT gen_random_uuid(), upper(encode(sha256(int4send(i)), 'hex'))
           FROM generate_series(1, {n}) i WHERE i % 53 = 0;
         INSERT INTO nodes (id, kind, key) SELECT id, 'document', key FROM seed_odd;
         INSERT INTO edges (id, src, dst, kind, confidence, state, source)
           SELECT gen_random_uuid(), o.id, co.id, 'party', 0.9, 'active', 'document'
           FROM seed_odd o JOIN seed_co co ON co.c = 1;
         INSERT INTO edges (id, src, dst, kind, confidence, state, source)
           SELECT gen_random_uuid(), a.id, b.id, 'party', 0.9, 'active', 'registry'
           FROM seed_co a JOIN seed_co b ON a.c = 2 AND b.c = 1;
         DROP TABLE seed_docs, seed_co, seed_odd;
         ANALYZE records, search_index, nodes, edges;"
    )
}

/// Company `c` of [`seed`].
fn company(c: u32) -> Uuid {
    format!("00000000-0000-4000-8000-{c:012}").parse().unwrap()
}

/// The search SQL as `PgStore::search` builds it, for the filters the cases use, with the
/// company filter `filter` (`$1` the company).
fn search_sql(q: &SearchQuery, filter: &str) -> (String, Vec<Box<dyn ToSql + Sync + Send>>) {
    let mut args: Vec<Box<dyn ToSql + Sync + Send>> = vec![Box::new(q.company.unwrap())];
    let words = q.words();
    let mut sql = if words.is_empty() {
        "SELECT s.blake3, s.title, s.kind, s.doc_date, s.amount, 0::real AS score \
         FROM search_index s WHERE true"
            .to_owned()
    } else {
        args.push(Box::new(words.join(" ")));
        "SELECT s.blake3, s.title, s.kind, s.doc_date, s.amount, \
         ts_rank(s.tsv, plainto_tsquery('simple', $2)) AS score \
         FROM search_index s WHERE s.tsv @@ plainto_tsquery('simple', $2)"
            .to_owned()
    };
    sql.push_str(filter);
    if let Some(kind) = &q.kind {
        args.push(Box::new(kind.clone()));
        let _ = write!(sql, " AND s.kind = ${}", args.len());
    }
    if let Some(d) = q.date_from {
        args.push(Box::new(d));
        let _ = write!(sql, " AND s.doc_date >= ${}", args.len());
    }
    args.push(Box::new(i64::from(q.effective_limit())));
    args.push(Box::new(i64::from(q.offset)));
    let _ = write!(
        sql,
        " ORDER BY score DESC, s.doc_date DESC NULLS LAST, s.blake3 LIMIT ${} OFFSET ${}",
        args.len() - 1,
        args.len()
    );
    (sql, args)
}

async fn run(c: &tokio_postgres::Client, q: &SearchQuery, filter: &str) -> Vec<(Vec<u8>, f32)> {
    let (sql, args) = search_sql(q, filter);
    let refs: Vec<&(dyn ToSql + Sync)> = args
        .iter()
        .map(|b| b.as_ref() as &(dyn ToSql + Sync))
        .collect();
    c.query(&sql, &refs)
        .await
        .unwrap()
        .iter()
        .map(|r| (r.get(0), r.get(5)))
        .collect()
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
    let Some(url) = test_url() else { return };
    let db = Scratch::new(&url).await;
    let store = db.store().await;
    let c = db.client().await;
    c.batch_execute(&seed(3000, 40)).await.unwrap();
    let from = Date::from_calendar_date(2022, Month::January, 1).unwrap();
    let missing = Uuid::from_u128(7);
    let mut total = 0;
    for co in [company(0), company(1), company(2), company(7), missing] {
        for text in ["", "alpha", "alpha beta", "w7", "nothing"] {
            for kind in [None, Some("invoice")] {
                for date in [None, Some(from)] {
                    for (limit, offset) in [(20, 0), (100, 40), (500, 0), (1000, 900)] {
                        let mut q = query(co, text, kind, date);
                        (q.limit, q.offset) = (limit, offset);
                        let before = run(&c, &q, BEFORE).await;
                        let after = run(&c, &q, AFTER).await;
                        assert_eq!(before, after, "{q:?}");
                        let got: Vec<(Vec<u8>, f32)> = store
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
    // Company 1's odd parties (placeholders, an uppercase key, a company) find nothing more
    // than its documents, and company 2 is not a document of company 1.
    let q = SearchQuery {
        company: Some(company(1)),
        limit: 1000,
        ..SearchQuery::default()
    };
    let n = store.search(&q).await.unwrap().len();
    assert_eq!(n, run(&c, &q, BEFORE).await.len());
    assert!(n > 10, "{n}");
    db.drop_schema().await;
}

/// The median of `runs` timings, in milliseconds.
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

/// Plans and times search by company before and after issue #111 on
/// `DOEWOOD_BENCH_DOCS` generated documents (default 50,000), one company per 10 documents
/// and company 0 on half of them. See docs/api.md for the command and the numbers.
#[tokio::test]
#[ignore = "a measurement; run by hand"]
async fn search_by_company_timings() {
    let Some(url) = test_url() else { return };
    let n: u32 = std::env::var("DOEWOOD_BENCH_DOCS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(50_000);
    let db = Scratch::new(&url).await;
    drop(db.store().await);
    let c = db.client().await;
    c.batch_execute(&seed(n, n / 10)).await.unwrap();
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
            let refs: Vec<&(dyn ToSql + Sync)> = args
                .iter()
                .map(|b| b.as_ref() as &(dyn ToSql + Sync))
                .collect();
            let plan: Vec<String> = c
                .query(&format!("EXPLAIN (ANALYZE, BUFFERS) {sql}"), &refs)
                .await
                .unwrap()
                .iter()
                .map(|r| r.get(0))
                .collect();
            println!("--- {name}, {label}:\n{}", plan.join("\n"));
            let ms = median_ms(9, async || {
                c.query(&sql, &refs).await.unwrap();
            })
            .await;
            println!("{name}, {label}: {ms:.2} ms");
        }
    }
    db.drop_schema().await;
}
