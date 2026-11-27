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

/// Schema 17's search vector, title above body, without the facts.
const SCHEMA_17: &str = "DROP INDEX search_tsv; \
     ALTER TABLE search_index DROP COLUMN facts CASCADE; \
     ALTER TABLE search_index ADD COLUMN tsv tsvector GENERATED ALWAYS AS \
     (setweight(to_tsvector('simple', title), 'A') \
     || setweight(to_tsvector('simple', body), 'B')) STORED; \
     CREATE INDEX search_tsv ON search_index USING gin (tsv) WITH (gin_pending_list_limit = 256); \
     ANALYZE search_index";

/// Schema 18's change to `search_index`, as its migration runs it.
fn schema_18() -> &'static str {
    let sql = crate::migrate::MIGRATIONS[17];
    &sql[sql
        .find("ALTER TABLE search_index")
        .expect("the search part")..]
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
    let size: String = c
        .query_one(
            "SELECT pg_size_pretty(pg_total_relation_size('search_index'))",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    println!("facts: search_index with its indexes: {size}");
    db.drop_schema().await;
}
