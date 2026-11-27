//! Search with the key facts (issue #194) on generated data: an ignored test times `Search`
//! before and after schema 18 put the key facts in the full-text vector, and the migration
//! itself (docs/api.md, "Search ranking of key facts").

use std::time::Instant;

use doewood_model::{SearchQuery, SearchStore};

use super::{Scratch, test_url};

/// `n` documents with records and search rows: 20 of 24 words and 20 of 5,000 rare terms
/// each, titled with a word and the number, every 4th with `quarter`, as `api_load` and the
/// SQLite measurement (`doewood-store`, `search_scale.rs`) seed them.
fn seed(n: u32) -> String {
    format!(
        "CREATE TEMP TABLE seed_w (k int PRIMARY KEY, w text);
         INSERT INTO seed_w SELECT k - 1, w FROM unnest(ARRAY['invoice', 'contract', 'annex',
           'delivery', 'payment', 'supplier', 'buyer', 'steel', 'timber', 'cement',
           'transport', 'service', 'warranty', 'penalty', 'schedule', 'price', 'discount',
           'quantity', 'order', 'receipt', 'tender', 'lease', 'insurance', 'audit'])
           WITH ORDINALITY AS t (w, k);
         INSERT INTO records (blake3, record, versions)
           SELECT sha256(int4send(i)), jsonb_build_object('n', i), '{{\"quail\":\"1\"}}'
           FROM generate_series(0, {n} - 1) i;
         INSERT INTO search_index (blake3, title, body, kind, doc_date, amount)
           SELECT sha256(int4send(i)), (SELECT w FROM seed_w WHERE k = i % 24) || ' ' || i,
                  (SELECT string_agg(w, ' ') FROM generate_series(0, 19) j
                     JOIN seed_w ON k = (i * 7 + j * 13) % 24)
                  || ' ' || (SELECT string_agg('term' || ((i * 20 + j)::bigint * 2654435761 % 5000), ' ')
                     FROM generate_series(0, 19) j)
                  || CASE WHEN i % 4 = 0 THEN ' quarter' ELSE '' END,
                  'invoice', NULL, 100.0
           FROM generate_series(0, {n} - 1) i;
         DROP TABLE seed_w;"
    )
}

/// The key facts every 2nd document gets: a card's text form and two highlighted passages,
/// about 250 bytes, with `otkazni rok` in every 10th (as in the SQLite measurement).
const SEED_FACTS: &str = "UPDATE search_index s SET facts = 'kind: contract' || chr(10) \
     || 'notice: iso=P30D; days=30' || chr(10) || 'penalty: percent=0.5; per=day' || chr(10) \
     || 'amount: ' || (r.n * 37 % 100000) || '.00 RSD' || chr(10) || 'governing_law: RS' \
     || chr(10) || CASE WHEN r.n % 10 = 0 \
        THEN 'Svaka strana može otkazati ugovor uz otkazni rok od 30 dana.' \
        ELSE 'Ugovor se zaključuje na period od 12 meseci.' END || chr(10) \
     || 'Ugovorna kazna iznosi 0,5% za svaki dan kašnjenja, term' || (r.n * 11 % 5000) \
     FROM (SELECT blake3, (record->>'n')::int AS n FROM records) r \
     WHERE r.blake3 = s.blake3 AND r.n % 2 = 0";

/// Back to schema 17's search (title above body, no facts): schema 19 left that vector, so
/// only its table and flag go. An empty stand-in for `search_facts` stays, so `PgStore::search`
/// finds no facts and runs schema 17's (then 18's) query on `search_index` alone; the lookup
/// is about 0.02 ms.
const SCHEMA_17: &str = "DROP TABLE search_facts; \
     ALTER TABLE search_index DROP COLUMN has_facts; \
     CREATE TABLE search_facts (ftsv tsvector); \
     ANALYZE search_index";

/// Schema 18's change to `search_index`, as its migration runs it.
fn schema_18() -> &'static str {
    let sql = crate::migrate::MIGRATIONS[17];
    &sql[sql
        .find("ALTER TABLE search_index")
        .expect("the search part")..]
}

/// `search_index` and `search_facts` (once there), with their indexes.
async fn sizes(c: &tokio_postgres::Client) -> (String, String) {
    let r = c
        .query_one(
            "SELECT pg_size_pretty(pg_total_relation_size('search_index')), \
             pg_size_pretty(coalesce(pg_total_relation_size(to_regclass('search_facts')), 0))",
            &[],
        )
        .await
        .unwrap();
    (r.get(0), r.get(1))
}

/// The median and the 99th percentile of 3 timed runs of `q` 20 times (after one untimed
/// run), in milliseconds, and the hits.
async fn timings(store: &crate::PgStore, q: &SearchQuery) -> (f64, f64, usize) {
    let hits = store.search(q).await.unwrap().len();
    let mut ms = Vec::new();
    for _ in 0..60 {
        let t = Instant::now();
        store.search(q).await.unwrap();
        ms.push(t.elapsed().as_secs_f64() * 1000.0);
    }
    ms.sort_by(f64::total_cmp);
    (ms[ms.len() / 2], ms[ms.len() * 99 / 100], hits)
}

/// `DOEWOOD_TEST_POSTGRES_URL=... DOEWOOD_BENCH_DOCS=50000 cargo test --release -p doewood-pg
/// --lib fact_search_timings -- --ignored --nocapture`
#[tokio::test]
#[ignore = "a measurement; run by hand"]
async fn fact_search_timings() {
    let Some(url) = test_url() else { return };
    let n: u32 = std::env::var("DOEWOOD_BENCH_DOCS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(50_000);
    let db = Scratch::new(&url).await;
    let store = db.store().await;
    let c = db.client().await;
    let t = Instant::now();
    c.batch_execute(&seed(n)).await.unwrap();
    println!(
        "facts: seeded {n} documents in {:.1}s",
        t.elapsed().as_secs_f64()
    );
    c.batch_execute(SCHEMA_17).await.unwrap();
    let words = [
        ("a rare term", "term125"),
        ("a word and a rare term", "invoice term250"),
        ("a word in a quarter of the documents", "quarter"),
        ("a word in most documents", "annex"),
        ("a word in most documents and the facts", "penalty"),
        ("two words of the facts", "otkazni rok"),
    ];
    let run = |label: &'static str| {
        let store = &store;
        async move {
            for (name, w) in words {
                for kind in [false, true] {
                    let q = SearchQuery {
                        text: Some(w.to_owned()),
                        kind: kind.then(|| "invoice".to_owned()),
                        limit: 20,
                        ..SearchQuery::default()
                    };
                    let (p50, p99, hits) = timings(store, &q).await;
                    println!(
                        "facts: {label}, {name}{}: p50 {p50:.2} ms, p99 {p99:.2} ms ({hits} hits)",
                        if kind { ", kind filter" } else { "" }
                    );
                }
            }
        }
    };
    run("schema 17").await;
    let t = Instant::now();
    c.batch_execute(&format!(
        "BEGIN; {} COMMIT; ANALYZE search_index",
        schema_18()
    ))
    .await
    .unwrap();
    println!(
        "facts: schema 18's search change ({n} documents): {:.2} s",
        t.elapsed().as_secs_f64()
    );
    run("schema 18, no facts").await;
    let t = Instant::now();
    c.batch_execute(SEED_FACTS).await.unwrap();
    c.batch_execute("VACUUM ANALYZE search_index")
        .await
        .unwrap();
    println!("facts: facts written in {:.2} s", t.elapsed().as_secs_f64());
    run("schema 18, facts on every 2nd").await;
    println!("facts: schema 18, with indexes: {:?}", sizes(&c).await);
    // Schema 19 (issue #198): the facts in their own table.
    let t = Instant::now();
    c.batch_execute(&format!(
        "BEGIN; DROP TABLE search_facts; {} COMMIT; ANALYZE search_index; ANALYZE search_facts",
        crate::migrate::MIGRATIONS[18]
    ))
    .await
    .unwrap();
    println!(
        "facts: schema 19's migration ({n} documents): {:.2} s",
        t.elapsed().as_secs_f64()
    );
    println!(
        "facts: schema 19, search_index and search_facts with indexes: {:?}",
        sizes(&c).await
    );
    run("schema 19").await;
    // Reanalysis writes the facts again: `search_index` rows stay as they are.
    c.batch_execute("UPDATE search_facts SET title = title")
        .await
        .unwrap();
    c.batch_execute("VACUUM ANALYZE search_facts")
        .await
        .unwrap();
    run("schema 19, facts written again").await;
    db.drop_schema().await;
}
