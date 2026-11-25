//! Issues #74 and #75 on generated data: the `sketch_candidates` query returns what the query
//! before #74 returned, unchanged sketches and search rows are not written again, and an
//! ignored test times each changed path before and after (docs/ingest.md, "Read paths").

use std::time::Instant;

use doewood_model::{
    Blake3, GraphStore, RecordStore, SearchDoc, SearchStore, SketchStore, band_to_i64,
};
use uuid::Uuid;

use super::{Scratch, test_url};
use crate::PgStore;
use crate::backend::SKETCH_CANDIDATES;

/// The candidate query before issue #74, kept as the reference the current one must match.
const BEFORE: &str = "SELECT b.blake3, s.signature, count(DISTINCT b.band) AS shared \
     FROM sketch_bands b JOIN sketches s ON s.blake3 = b.blake3 \
     WHERE b.band = ANY($1) AND b.blake3 <> $2 \
     GROUP BY b.blake3, s.signature ORDER BY shared DESC, b.blake3 LIMIT $3";

/// Bands per document.
const BANDS: i32 = 32;

/// `n` generated documents with records (about 4 KB of JSON each, with 20 facts, one of them a
/// `signal.email_domain`), document nodes and sketches, and `companies` companies.
///
/// - Sketches: documents 1..=2000 are near-identical (one template); the others fall into 48
///   templates by `i % 48`. Band `j` of document `i` is its template's band `j`, except one in
///   eight (`(i + j) % 8 = 0`), which is the document's own. A signature is 512 bytes.
/// - Party edges: every document is a party of company 1 (the instance's owner, on every
///   document), of company `2 + i % (companies - 1)` as buyer and of
///   `2 + (7 * i) % (companies - 1)` as supplier.
fn seed(n: u32, companies: u32) -> String {
    format!(
        "INSERT INTO records (blake3, record, versions)
         SELECT sha256(int4send(i)),
                jsonb_build_object('title', 'Document ' || i, 'text', repeat(md5(i::text), 60),
                  'facts', (SELECT jsonb_agg(CASE WHEN k = 3 THEN
                      jsonb_build_object('key', 'signal.email_domain',
                        'value', jsonb_build_object('t', 'text', 'v', 'd' || (i % 300) || '.example'),
                        'conf', 0.9)
                    ELSE jsonb_build_object('key', 'fact.' || k,
                        'value', jsonb_build_object('t', 'text', 'v', md5((i * k)::text)),
                        'conf', 0.5) END) FROM generate_series(1, 20) k)),
                '{{\"doewood.ingest\":\"1\",\"quail\":\"abc\"}}'
         FROM generate_series(1, {n}) i;
         INSERT INTO sketches (blake3, signature)
           SELECT sha256(int4send(i)),
                  decode(repeat(md5(CASE WHEN i <= 2000 THEN 't' ELSE 't' || (i % 48) END), 32), 'hex')
           FROM generate_series(1, {n}) i;
         INSERT INTO sketch_bands (band, blake3)
           SELECT ('x' || substr(md5(CASE WHEN (i + j) % 8 = 0 THEN 'd' || i
                       WHEN i <= 2000 THEN 't' ELSE 't' || (i % 48) END || ':' || j), 1, 16))
                    ::bit(64)::bigint,
                  sha256(int4send(i))
           FROM generate_series(1, {n}) i CROSS JOIN generate_series(1, {BANDS}) j;
         CREATE TEMP TABLE seed_docs AS
           SELECT i, gen_random_uuid() AS id FROM generate_series(1, {n}) i;
         INSERT INTO nodes (id, kind, key)
           SELECT id, 'document', encode(sha256(int4send(i)), 'hex') FROM seed_docs;
         CREATE TEMP TABLE seed_co AS
           SELECT c, gen_random_uuid() AS id FROM generate_series(1, {companies}) c;
         INSERT INTO nodes (id, kind, key) SELECT id, 'company', 'co-' || c FROM seed_co;
         INSERT INTO edges (id, src, dst, kind, props, confidence, state, source)
           SELECT gen_random_uuid(), d.id, co.id, 'party', jsonb_build_object('role', r.role),
                  0.9, 'active', 'document'
           FROM seed_docs d
           CROSS JOIN LATERAL (VALUES (1, 'owner'),
               (2 + d.i % ({companies} - 1), 'buyer'),
               (2 + (7 * d.i) % ({companies} - 1), 'supplier')) r (c, role)
           JOIN seed_co co ON co.c = r.c
           ON CONFLICT DO NOTHING;
         DROP TABLE seed_docs, seed_co;
         ANALYZE records, sketches, sketch_bands, nodes, edges;"
    )
}

/// The band keys of generated document `i`, as `seed` made them.
async fn bands_of(c: &tokio_postgres::Client, i: i32) -> Vec<i64> {
    c.query_one(
        "SELECT array_agg(band ORDER BY band) FROM sketch_bands WHERE blake3 = sha256(int4send($1))",
        &[&i],
    )
    .await
    .unwrap()
    .get(0)
}

async fn candidates(
    c: &tokio_postgres::Client,
    sql: &str,
    bands: &[i64],
    exclude: &[u8],
    limit: i64,
) -> Vec<(Vec<u8>, Vec<u8>, i64)> {
    c.query(sql, &[&bands, &exclude, &limit])
        .await
        .unwrap()
        .iter()
        .map(|r| (r.get(0), r.get(1), r.get(2)))
        .collect()
}

/// The lookups compared and timed: (name, generated document whose bands are looked up).
const LOOKUPS: &[(&str, i32)] = &[
    ("one of 2000 near-identical", 17),
    ("template of ~1000", 2500),
];

#[tokio::test]
async fn sketch_candidates_match_the_query_before() {
    let Some(url) = test_url() else { return };
    let db = Scratch::new(&url).await;
    drop(db.store().await);
    let c = db.client().await;
    c.batch_execute(&seed(4000, 50)).await.unwrap();
    let mut cases: Vec<(Vec<i64>, Vec<u8>)> = Vec::new();
    for &(_, i) in LOOKUPS {
        let exclude: Vec<u8> = c
            .query_one("SELECT sha256(int4send($1))", &[&i])
            .await
            .unwrap()
            .get(0);
        cases.push((bands_of(&c, i).await, exclude.clone()));
        // Half of its bands; a band nobody has; its bands and one more, the document
        // itself included.
        let half: Vec<i64> = bands_of(&c, i).await.into_iter().step_by(2).collect();
        cases.push((half, exclude.clone()));
        cases.push((vec![42], exclude.clone()));
        let mut more = bands_of(&c, i).await;
        more.push(42);
        cases.push((more, vec![0; 32]));
    }
    for (bands, exclude) in &cases {
        for limit in [1, 10, 100, 1000] {
            let before = candidates(&c, BEFORE, bands, exclude, limit).await;
            let after = candidates(&c, SKETCH_CANDIDATES, bands, exclude, limit).await;
            assert_eq!(before, after, "limit {limit}");
        }
    }
    db.drop_schema().await;
}

/// Issue #74: writing the same sketch or search row again leaves the rows as they were (no
/// new row version), and a change is still written.
#[tokio::test]
async fn unchanged_writes_write_nothing() {
    let Some(url) = test_url() else { return };
    let db = Scratch::new(&url).await;
    let s = db.store().await;
    let c = db.client().await;
    let h = Blake3([7; 32]);
    s.put_record(&h, &serde_json::json!({}), &Default::default())
        .await
        .unwrap();
    s.put_sketch(&h, &[1, 2, 3], &[5, 6, u64::MAX]).await.unwrap();
    let doc = SearchDoc {
        blake3: h,
        title: "Invoice".to_owned(),
        body: "Consulting".to_owned(),
        kind: Some("invoice".to_owned()),
        date: None,
        amount: Some(10.0),
    };
    s.index_document(&doc).await.unwrap();
    let versions = async || -> Vec<String> {
        let q = "SELECT xmin::text FROM sketches UNION ALL \
                 (SELECT xmin::text FROM sketch_bands ORDER BY band) UNION ALL \
                 SELECT xmin::text FROM search_index";
        c.query(q, &[])
            .await
            .unwrap()
            .iter()
            .map(|r| r.get(0))
            .collect()
    };
    let before = versions().await;
    assert_eq!(before.len(), 5);
    s.put_sketch(&h, &[1, 2, 3], &[u64::MAX, 6, 5, 5]).await.unwrap();
    s.index_document(&doc).await.unwrap();
    assert_eq!(versions().await, before);
    // A change is written.
    s.put_sketch(&h, &[1, 2, 4], &[5, 7]).await.unwrap();
    s.index_document(&SearchDoc {
        amount: None,
        ..doc.clone()
    })
    .await
    .unwrap();
    let after = versions().await;
    assert_eq!(after.len(), 4);
    assert_ne!(after[0], before[0], "signature");
    assert_eq!(after[1], before[2], "band 5 kept");
    assert_ne!(after[3], before[4], "search row");
    let bands: Vec<i64> = c
        .query("SELECT band FROM sketch_bands ORDER BY band", &[])
        .await
        .unwrap()
        .iter()
        .map(|r| r.get(0))
        .collect();
    assert_eq!(bands, [band_to_i64(5), band_to_i64(7)]);
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

/// The email domains as `website.rs` read them before issue #75: every document node, the
/// first 50, one full record each.
async fn email_domains_before(s: &PgStore, company: Uuid) -> Vec<(Blake3, String)> {
    let mut out = Vec::new();
    for doc in s.company_documents(company).await.unwrap().into_iter().take(50) {
        let hash: Blake3 = doc.key.parse().unwrap();
        let record = s.get_record(&hash).await.unwrap().unwrap();
        for fact in record.body["facts"].as_array().unwrap() {
            if fact["key"] == "signal.email_domain"
                && let Some(d) = fact["value"]["v"].as_str()
            {
                out.push((hash, d.to_owned()));
            }
        }
    }
    out.sort();
    out
}

async fn email_domains_after(s: &PgStore, company: Uuid) -> Vec<(Blake3, String)> {
    let hashes: Vec<Blake3> = s
        .company_documents_first(company, 50)
        .await
        .unwrap()
        .iter()
        .map(|d| d.key.parse().unwrap())
        .collect();
    let mut out = s
        .fact_texts(&hashes, "signal.email_domain")
        .await
        .unwrap();
    out.sort();
    out
}

/// Times every path issues #74 and #75 changed, before and after, on
/// `DOEWOOD_BENCH_DOCS` generated documents (default 50,000) and `DOEWOOD_BENCH_COMPANIES`
/// companies (default 5,000). See docs/ingest.md for the command and the numbers.
#[tokio::test]
#[ignore = "a measurement; run by hand"]
#[allow(clippy::too_many_lines)]
async fn read_path_timings() {
    let Some(url) = test_url() else { return };
    let var = |name: &str, default: u32| {
        std::env::var(name)
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(default)
    };
    let (n, companies) = (
        var("DOEWOOD_BENCH_DOCS", 50_000),
        var("DOEWOOD_BENCH_COMPANIES", 5_000),
    );
    let db = Scratch::new(&url).await;
    let s = db.store().await;
    let c = db.client().await;
    let t = Instant::now();
    c.batch_execute(&seed(n, companies)).await.unwrap();
    c.batch_execute("VACUUM ANALYZE").await.unwrap();
    println!(
        "{n} documents, {companies} companies (seeded in {:.1} s); median of 9 runs, ms",
        t.elapsed().as_secs_f64()
    );

    // #74: candidates.
    for &(name, i) in LOOKUPS {
        let bands = bands_of(&c, i).await;
        let exclude: Vec<u8> = c
            .query_one("SELECT sha256(int4send($1))", &[&i])
            .await
            .unwrap()
            .get(0);
        let mut ms = [0.0; 2];
        for (k, sql) in [BEFORE, SKETCH_CANDIDATES].into_iter().enumerate() {
            ms[k] = median_ms(9, async || {
                drop(candidates(&c, sql, &bands, &exclude, 100).await);
            })
            .await;
        }
        println!(
            "sketch_candidates, {name:<28} before {:>8.2}  after {:>8.2}",
            ms[0], ms[1]
        );
    }

    // #74: rewriting 200 unchanged sketches and search rows.
    let docs: Vec<(Blake3, Vec<u8>, Vec<u64>)> = c
        .query(
            "SELECT s.blake3, s.signature, array_agg(b.band) FROM sketches s \
             JOIN sketch_bands b USING (blake3) GROUP BY s.blake3, s.signature \
             ORDER BY s.blake3 LIMIT 200",
            &[],
        )
        .await
        .unwrap()
        .iter()
        .map(|r| {
            let bands: Vec<i64> = r.get(2);
            (
                Blake3::from_slice(r.get::<_, &[u8]>(0)).unwrap(),
                r.get(1),
                bands.into_iter().map(i64::cast_unsigned).collect(),
            )
        })
        .collect();
    let search: Vec<SearchDoc> = docs
        .iter()
        .map(|(h, _, _)| SearchDoc {
            blake3: *h,
            title: "Invoice 42 for consulting".to_owned(),
            body: "Consulting services for ACME, March 2024. ".repeat(40),
            kind: Some("invoice".to_owned()),
            date: None,
            amount: Some(1200.0),
        })
        .collect();
    for doc in &search {
        s.index_document(doc).await.unwrap();
    }
    let put_before = async |h: &Blake3, sig: &[u8], bands: &[u64]| {
        let bands: Vec<i64> = bands.iter().map(|b| band_to_i64(*b)).collect();
        let key = h.as_bytes().as_slice();
        c.batch_execute("BEGIN").await.unwrap();
        c.execute(
            "INSERT INTO sketches (blake3, signature) VALUES ($1, $2) \
             ON CONFLICT (blake3) DO UPDATE SET signature = EXCLUDED.signature, \
             updated_at = now()",
            &[&key, &sig],
        )
        .await
        .unwrap();
        c.execute("DELETE FROM sketch_bands WHERE blake3 = $1", &[&key])
            .await
            .unwrap();
        c.execute(
            "INSERT INTO sketch_bands (band, blake3) \
             SELECT DISTINCT b, $2::bytea FROM unnest($1::bigint[]) AS b",
            &[&bands, &key],
        )
        .await
        .unwrap();
        c.batch_execute("COMMIT").await.unwrap();
    };
    let index_before = async |d: &SearchDoc| {
        c.execute(
            "INSERT INTO search_index (blake3, title, body, kind, doc_date, amount) \
             VALUES ($1, $2, $3, $4, $5, $6) \
             ON CONFLICT (blake3) DO UPDATE SET title = EXCLUDED.title, body = EXCLUDED.body, \
             kind = EXCLUDED.kind, doc_date = EXCLUDED.doc_date, amount = EXCLUDED.amount, \
             indexed_at = now()",
            &[
                &d.blake3.as_bytes().as_slice(),
                &d.title,
                &d.body,
                &d.kind,
                &d.date,
                &d.amount,
            ],
        )
        .await
        .unwrap();
    };
    let wal = async || -> i64 {
        c.query_one("SELECT (pg_current_wal_lsn() - '0/0')::bigint", &[])
            .await
            .unwrap()
            .get(0)
    };
    let w0 = wal().await;
    let sketch_before = median_ms(3, async || {
        for (h, sig, bands) in &docs {
            put_before(h, sig, bands).await;
        }
    })
    .await;
    let w1 = wal().await;
    let sketch_after = median_ms(3, async || {
        for (h, sig, bands) in &docs {
            s.put_sketch(h, sig, bands).await.unwrap();
        }
    })
    .await;
    let w2 = wal().await;
    println!(
        "put_sketch x200 unchanged          before {sketch_before:>8.2}  after {sketch_after:>8.2}  \
         (WAL {} KB vs {} KB for 4 rounds)",
        (w1 - w0) / 1024,
        (w2 - w1) / 1024
    );
    let w0 = wal().await;
    let index_b = median_ms(3, async || {
        for d in &search {
            index_before(d).await;
        }
    })
    .await;
    let w1 = wal().await;
    let index_a = median_ms(3, async || {
        for d in &search {
            s.index_document(d).await.unwrap();
        }
    })
    .await;
    let w2 = wal().await;
    println!(
        "index_document x200 unchanged      before {index_b:>8.2}  after {index_a:>8.2}  \
         (WAL {} KB vs {} KB for 4 rounds)",
        (w1 - w0) / 1024,
        (w2 - w1) / 1024
    );

    // #75: one reprocess page of 500.
    let page: Vec<Blake3> = c
        .query("SELECT blake3 FROM records ORDER BY blake3 LIMIT 500", &[])
        .await
        .unwrap()
        .iter()
        .map(|r| Blake3::from_slice(r.get::<_, &[u8]>(0)).unwrap())
        .collect();
    let before = median_ms(9, async || {
        for h in &page {
            drop(s.get_record(h).await.unwrap());
        }
    })
    .await;
    let after = median_ms(9, async || drop(s.record_versions(&page).await.unwrap())).await;
    println!("reprocess page of 500 (versions)   before {before:>8.2}  after {after:>8.2}");

    // #75: the merge gate's counts and the email domains, for the owner (on every document)
    // and a typical company.
    let company = async |key: &str| -> Uuid {
        c.query_one("SELECT id FROM nodes WHERE kind = 'company' AND key = $1", &[&key])
            .await
            .unwrap()
            .get(0)
    };
    for key in ["co-1", "co-2"] {
        let id = company(key).await;
        let docs = s.company_document_count(id).await.unwrap();
        assert_eq!(docs, s.company_documents(id).await.unwrap().len() as u64);
        let before = median_ms(9, async || {
            let _ = s.company_documents(id).await.unwrap().len();
        })
        .await;
        let after = median_ms(9, async || {
            let _ = s.company_document_count(id).await.unwrap();
        })
        .await;
        println!(
            "gate count, {docs:>6} documents       before {before:>8.2}  after {after:>8.2}"
        );
        assert_eq!(
            email_domains_before(&s, id).await,
            email_domains_after(&s, id).await
        );
        let before = median_ms(9, async || drop(email_domains_before(&s, id).await)).await;
        let after = median_ms(9, async || drop(email_domains_after(&s, id).await)).await;
        println!(
            "email domains, {docs:>6} documents    before {before:>8.2}  after {after:>8.2}"
        );
    }
    db.drop_schema().await;
}
