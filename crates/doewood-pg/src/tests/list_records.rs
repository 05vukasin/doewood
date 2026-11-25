//! `list_records` on generated data: the query returns what the issue #13 query returned, for
//! every filter and page size, and an ignored test times both (docs/api.md, "`ListRecords`
//! on Postgres").

use std::time::Instant;

use doewood_model::{Blake3, RecordStore, SourceKey};

use super::{Scratch, test_url};
use crate::backend::LIST_RECORDS;

/// The query before issue #13, kept as the reference the current one must match.
const BEFORE: &str = "SELECT r.blake3, r.record, r.versions, extract(epoch FROM r.updated_at)::bigint \
     FROM records r WHERE ($4::bytea IS NULL OR r.blake3 > $4) \
     AND EXISTS (SELECT 1 FROM locations l WHERE l.blake3 = r.blake3 \
     AND l.endpoint = $1 AND l.bucket = $2 AND starts_with(l.object_key, $3)) \
     ORDER BY r.blake3 LIMIT $5";

const ENDPOINT: &str = "http://s3.example:9000";

/// A timed case: its name, the (bucket, prefix) filter, the page token and the page size.
type Case<'a> = (&'a str, (&'a str, &'a str), Option<&'a [u8]>, i64);

/// `n` generated records (about 1 KB of JSON each), a document node each, and their locations:
/// record `i` is at `b{i%4}/dept{i%10}/2026/file-{i}.pdf`; every 5th also at
/// `b4/archive/file-{i}.pdf`; every 7th has a second key in its own bucket (two locations in
/// one source); every 13th is on another endpoint instead; every 11th has no location.
fn seed(n: u32) -> String {
    format!(
        "INSERT INTO records (blake3, record, versions)
         SELECT sha256(int4send(i)),
                jsonb_build_object('title', 'Document ' || i, 'text', repeat(md5(i::text), 30)),
                '{{\"quail\":\"1\"}}'
         FROM generate_series(1, {n}) i;
         CREATE TEMP TABLE seed_docs AS
           SELECT i, gen_random_uuid() AS id FROM generate_series(1, {n}) i;
         INSERT INTO nodes (id, kind, key)
           SELECT id, 'document', encode(sha256(int4send(i)), 'hex') FROM seed_docs;
         CREATE TEMP TABLE seed_locs AS
           SELECT d.id AS document_id, d.i,
                  CASE WHEN d.i % 13 = 0 THEN 'http://other:9000' ELSE '{ENDPOINT}' END AS endpoint,
                  'b' || (d.i % 4) AS bucket,
                  'dept' || (d.i % 10) || '/2026/file-' || d.i || '.pdf' AS object_key
           FROM seed_docs d WHERE d.i % 11 <> 0
           UNION ALL
           SELECT d.id, d.i, '{ENDPOINT}', 'b4', 'archive/file-' || d.i || '.pdf'
           FROM seed_docs d WHERE d.i % 5 = 0
           UNION ALL
           SELECT d.id, d.i, '{ENDPOINT}', 'b' || (d.i % 4), 'dept' || (d.i % 10) || '/copy-' || d.i
           FROM seed_docs d WHERE d.i % 7 = 0;
         ALTER TABLE seed_locs ADD COLUMN node_id uuid;
         UPDATE seed_locs SET node_id = gen_random_uuid();
         INSERT INTO nodes (id, kind, key)
           SELECT node_id, 'location', endpoint || '/' || bucket || '/' || object_key FROM seed_locs;
         INSERT INTO locations (node_id, document_id, blake3, endpoint, bucket, object_key)
           SELECT node_id, document_id, sha256(int4send(i)), endpoint, bucket, object_key
           FROM seed_locs;
         DROP TABLE seed_docs, seed_locs;
         ANALYZE records, nodes, locations;"
    )
}

/// The (bucket, prefix) filters on [`ENDPOINT`]: whole buckets, a 5 % prefix, a rare prefix,
/// one that matches nothing, and a bucket that does not exist.
const FILTERS: &[(&str, &str)] = &[
    ("b0", ""),
    ("b4", ""),
    ("b0", "dept4/"),
    ("b0", "dept0/2026/file-40"),
    ("b0", "nope/"),
    ("missing", ""),
];

async fn page(
    c: &tokio_postgres::Client,
    sql: &str,
    (bucket, prefix): (&str, &str),
    after: Option<&[u8]>,
    limit: i64,
) -> Vec<(Vec<u8>, serde_json::Value)> {
    c.query(sql, &[&ENDPOINT, &bucket, &prefix, &after, &limit])
        .await
        .unwrap()
        .iter()
        .map(|r| (r.get(0), r.get(1)))
        .collect()
}

/// Every page of a filter, in order.
async fn walk(
    c: &tokio_postgres::Client,
    sql: &str,
    filter: (&str, &str),
    limit: i64,
) -> Vec<Vec<(Vec<u8>, serde_json::Value)>> {
    let mut pages = Vec::new();
    let mut after: Option<Vec<u8>> = None;
    loop {
        let p = page(c, sql, filter, after.as_deref(), limit).await;
        let Some(last) = p.last() else { break };
        after = Some(last.0.clone());
        pages.push(p);
    }
    pages
}

#[tokio::test]
async fn list_records_matches_the_query_before_issue_13() {
    let Some(url) = test_url() else { return };
    let db = Scratch::new(&url).await;
    let store = db.store().await;
    let c = db.client().await;
    c.batch_execute(&seed(700)).await.unwrap();
    for &filter in FILTERS {
        let expected: Vec<Vec<u8>> = c
            .query(
                "SELECT DISTINCT blake3 FROM locations WHERE endpoint = $1 AND bucket = $2 \
                 AND starts_with(object_key, $3) ORDER BY 1",
                &[&ENDPOINT, &filter.0, &filter.1],
            )
            .await
            .unwrap()
            .iter()
            .map(|r| r.get(0))
            .collect();
        for limit in [1, 7, 50, 1000] {
            let before = walk(&c, BEFORE, filter, limit).await;
            let now = walk(&c, LIST_RECORDS, filter, limit).await;
            assert_eq!(before, now, "{filter:?}, limit {limit}");
            let flat: Vec<Vec<u8>> = now.iter().flatten().map(|(h, _)| h.clone()).collect();
            assert_eq!(flat, expected, "{filter:?}, limit {limit}");
            // The store method pages the same way.
            let key = SourceKey::new(ENDPOINT, filter.0, filter.1).unwrap();
            let mut after: Option<Blake3> = None;
            for p in &now {
                let got = store
                    .list_records(
                        &key,
                        after.as_ref(),
                        u32::try_from(limit).unwrap(),
                        usize::MAX,
                    )
                    .await
                    .unwrap();
                let got: Vec<(Vec<u8>, serde_json::Value)> = got
                    .iter()
                    .map(|r| (r.blake3.as_bytes().to_vec(), r.body_value().unwrap()))
                    .collect();
                assert_eq!(&got, p, "{filter:?}, limit {limit}");
                after = Some(Blake3::from_slice(&p.last().unwrap().0).unwrap());
            }
        }
    }
    // The walks above compared something: a bucket has records, a prefix matching nothing none.
    assert!(!walk(&c, LIST_RECORDS, ("b0", ""), 1000).await.is_empty());
    assert!(walk(&c, LIST_RECORDS, ("b0", "nope/"), 50).await.is_empty());
    db.drop_schema().await;
}

/// Schema 3's `(endpoint, bucket, object_key COLLATE "C")` index turns the prefix into a range
/// (issue #27). Sequential scans are off so that the small table does not hide it.
#[tokio::test]
async fn the_prefix_is_a_range_on_the_c_index() {
    let Some(url) = test_url() else { return };
    let db = Scratch::new(&url).await;
    drop(db.store().await);
    let c = db.client().await;
    c.batch_execute(&seed(700)).await.unwrap();
    c.batch_execute("SET enable_seqscan = off").await.unwrap();
    let plan: Vec<String> = c
        .query(
            &format!("EXPLAIN (COSTS OFF) {LIST_RECORDS}"),
            &[
                &ENDPOINT,
                &"b0",
                &"dept0/2026/file-40",
                &None::<&[u8]>,
                &50_i64,
            ],
        )
        .await
        .unwrap()
        .iter()
        .map(|r| r.get(0))
        .collect();
    let plan = plan.join("\n");
    assert!(plan.contains("locations_by_key_c"), "{plan}");
    assert!(
        plan.contains("object_key)::text >= 'dept0/2026/file-40'::text")
            && plan.contains("object_key)::text < 'dept0/2026/file-41'::text"),
        "{plan}"
    );
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

/// Times the query before and after issue #13 on `DOEWOOD_BENCH_RECORDS` generated records
/// (default 5000) and prints the plans. See docs/api.md for the command and the numbers.
#[tokio::test]
#[ignore = "a measurement; run by hand"]
async fn list_records_timings() {
    let Some(url) = test_url() else { return };
    let n: u32 = std::env::var("DOEWOOD_BENCH_RECORDS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(5000);
    let db = Scratch::new(&url).await;
    drop(db.store().await);
    let c = db.client().await;
    c.batch_execute(&seed(n)).await.unwrap();
    // The page that starts halfway through the source's records.
    let mid: Vec<u8> = c
        .query_one(
            "SELECT blake3 FROM records ORDER BY blake3 OFFSET (SELECT count(*) / 2 FROM records) \
             LIMIT 1",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    let cases: &[Case] = &[
        ("bucket, first page", ("b0", ""), None, 50),
        ("bucket, page halfway", ("b0", ""), Some(mid.as_slice()), 50),
        ("bucket, 1000 per page", ("b0", ""), None, 1000),
        ("prefix 5 %, first page", ("b0", "dept4/"), None, 50),
        ("rare prefix", ("b0", "dept0/2026/file-40"), None, 50),
        ("prefix matching nothing", ("b0", "nope/"), None, 50),
    ];
    println!("{n} records; median of 15 runs, ms");
    for &(name, filter, after, limit) in cases {
        let mut t = [0.0; 2];
        for (i, sql) in [BEFORE, LIST_RECORDS].into_iter().enumerate() {
            t[i] = median_ms(15, async || drop(page(&c, sql, filter, after, limit).await)).await;
        }
        println!("{name:<26} before {:>8.2}  after {:>8.2}", t[0], t[1]);
    }
    for (name, filter, limit) in [
        ("walk bucket, 50 per page", ("b0", ""), 50),
        ("walk bucket, 1000 per page", ("b0", ""), 1000),
        ("walk prefix 5 %, 50/page", ("b0", "dept4/"), 50),
    ] {
        let mut t = [0.0; 2];
        for (i, sql) in [BEFORE, LIST_RECORDS].into_iter().enumerate() {
            t[i] = median_ms(5, async || drop(walk(&c, sql, filter, limit).await)).await;
        }
        println!("{name:<26} before {:>8.2}  after {:>8.2}", t[0], t[1]);
    }
    for (label, sql) in [("before", BEFORE), ("after", LIST_RECORDS)] {
        for &(name, filter, after, limit) in cases {
            let plan = c
                .query(
                    &format!("EXPLAIN (ANALYZE, BUFFERS, COSTS OFF) {sql}"),
                    &[&ENDPOINT, &filter.0, &filter.1, &after, &limit],
                )
                .await
                .unwrap();
            println!("--- {label}: {name}");
            for row in plan {
                println!("{}", row.get::<_, String>(0));
            }
        }
    }
    db.drop_schema().await;
}
