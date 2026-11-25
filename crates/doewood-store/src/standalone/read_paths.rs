//! Issues #74 and #75 on generated data, as in the Postgres test (`doewood-pg`,
//! `tests/read_paths.rs`): the `sketch_candidates` query returns what the query before #74
//! returned, unchanged sketches and search rows are not written again, and an ignored test
//! times each changed path before and after (docs/ingest.md, "Read paths").

use std::sync::Arc;
use std::time::Instant;

use doewood_core::MasterKey;
use doewood_model::{
    Blake3, GraphStore, RecordStore, SearchDoc, SearchStore, SketchStore, band_to_i64,
};
use rusqlite::types::Value as Sql;
use rusqlite::{Connection, params, params_from_iter};
use serde_json::json;
use uuid::Uuid;

use super::{Standalone, sketch_candidates_sql};
use crate::Store;
use crate::store::now;

/// The candidate query before issue #74, kept as the reference the current one must match.
fn before_sql(bands: usize) -> String {
    let marks = vec!["?"; bands].join(", ");
    format!(
        "SELECT b.blake3, s.signature, count(DISTINCT b.band) AS shared \
         FROM sketch_bands b JOIN sketches s ON s.blake3 = b.blake3 \
         WHERE b.band IN ({marks}) AND b.blake3 <> ? \
         GROUP BY b.blake3 ORDER BY shared DESC, b.blake3 LIMIT ?"
    )
}

/// Bands per document.
const BANDS: u64 = 32;

fn mix(mut z: u64) -> u64 {
    z = z.wrapping_add(0x9e37_79b9_7f4a_7c15);
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    z ^ (z >> 31)
}

/// Generated document `i`'s hash.
fn doc_hash(i: u64) -> Blake3 {
    let mut b = [0u8; 32];
    for (k, chunk) in b.chunks_mut(8).enumerate() {
        chunk.copy_from_slice(&mix(i * 4 + k as u64).to_be_bytes());
    }
    Blake3(b)
}

/// Generated document `i`'s template: documents 1..=2000 are near-identical, the others fall
/// into 48 templates.
fn template(i: u64) -> u64 {
    if i <= 2000 { 0 } else { 1 + i % 48 }
}

/// Generated document `i`'s band keys: its template's, except one in eight, its own.
fn bands(i: u64) -> Vec<u64> {
    (1..=BANDS)
        .map(|j| {
            if (i + j).is_multiple_of(8) {
                mix((1 << 40) | (i << 6) | j)
            } else {
                mix((template(i) << 6) | j)
            }
        })
        .collect()
}

fn uuid_of(tag: u64, i: u64) -> String {
    Uuid::from_u64_pair(mix(tag), mix(i)).to_string()
}

/// As in the Postgres test: `n` documents with records (about 4 KB of JSON each, 20 facts, one
/// a `signal.email_domain`), document nodes and sketches (512-byte signatures), and
/// `companies` companies; every document is a party of company 1, and of two others.
fn seed(conn: &mut Connection, n: u64, companies: u64) {
    let t = now();
    let tx = conn.transaction().unwrap();
    {
        let mut record = tx
            .prepare(
                "INSERT INTO records (blake3, body_json, versions_json, updated_at) \
                 VALUES (?1, ?2, '{\"doewood.ingest\":\"1\",\"quail\":\"abc\"}', ?3)",
            )
            .unwrap();
        let mut sketch = tx
            .prepare("INSERT INTO sketches (blake3, signature, updated_at) VALUES (?1, ?2, ?3)")
            .unwrap();
        let mut band = tx
            .prepare("INSERT OR IGNORE INTO sketch_bands (band, blake3) VALUES (?1, ?2)")
            .unwrap();
        let mut node = tx
            .prepare(
                "INSERT INTO nodes (id, kind, key, created_at, updated_at) \
                 VALUES (?1, ?2, ?3, ?4, ?4)",
            )
            .unwrap();
        let mut edge = tx
            .prepare(
                "INSERT OR IGNORE INTO edges (id, src, dst, kind, props_json, confidence, state, \
                 source, created_at, updated_at) \
                 VALUES (?1, ?2, ?3, 'party', ?4, 0.9, 'active', 'document', ?5, ?5)",
            )
            .unwrap();
        for c in 1..=companies {
            node.execute(params![uuid_of(1, c), "company", format!("co-{c}"), t])
                .unwrap();
        }
        for i in 1..=n {
            let h = doc_hash(i);
            let key = h.as_bytes().as_slice();
            let facts: Vec<_> = (1..=20u64)
                .map(|k| {
                    if k == 3 {
                        json!({"key": "signal.email_domain",
                               "value": {"t": "text", "v": format!("d{}.example", i % 300)},
                               "conf": 0.9})
                    } else {
                        json!({"key": format!("fact.{k}"),
                               "value": {"t": "text", "v": format!("{:032x}", mix(i * k))},
                               "conf": 0.5})
                    }
                })
                .collect();
            let body = json!({"title": format!("Document {i}"),
                              "text": format!("{:032x}", mix(i)).repeat(60), "facts": facts});
            record.execute(params![key, body.to_string(), t]).unwrap();
            let sig: Vec<u8> = (0..64)
                .flat_map(|k| mix(template(i) * 64 + k).to_be_bytes())
                .collect();
            sketch.execute(params![key, sig, t]).unwrap();
            for b in bands(i) {
                band.execute(params![band_to_i64(b), key]).unwrap();
            }
            let doc = uuid_of(2, i);
            node.execute(params![doc, "document", h.to_hex(), t])
                .unwrap();
            for (c, role) in [
                (1, "owner"),
                (2 + i % (companies - 1), "buyer"),
                (2 + (7 * i) % (companies - 1), "supplier"),
            ] {
                edge.execute(params![
                    Uuid::now_v7().to_string(),
                    doc,
                    uuid_of(1, c),
                    json!({"role": role}).to_string(),
                    t
                ])
                .unwrap();
            }
        }
    }
    tx.commit().unwrap();
    conn.execute_batch("ANALYZE").unwrap();
}

fn candidates(
    conn: &Connection,
    sql: &str,
    bands: &[u64],
    exclude: &Blake3,
    limit: i64,
) -> Vec<(Vec<u8>, Vec<u8>, i64)> {
    let mut args: Vec<Sql> = bands
        .iter()
        .map(|b| Sql::Integer(band_to_i64(*b)))
        .collect();
    args.push(Sql::Blob(exclude.as_bytes().to_vec()));
    args.push(Sql::Integer(limit));
    let mut stmt = conn.prepare_cached(sql).unwrap();
    stmt.query_map(params_from_iter(args), |r| {
        Ok((r.get(0)?, r.get(1)?, r.get(2)?))
    })
    .unwrap()
    .collect::<rusqlite::Result<_>>()
    .unwrap()
}

/// The lookups compared and timed: (name, generated document whose bands are looked up).
const LOOKUPS: &[(&str, u64)] = &[
    ("one of 2000 near-identical", 17),
    ("template of ~1000", 2500),
];

#[test]
fn sketch_candidates_match_the_query_before() {
    let store = Store::open_in_memory(&MasterKey::from_bytes([1; 32])).unwrap();
    let mut conn = store.conn();
    seed(&mut conn, 4000, 50);
    let mut cases: Vec<(Vec<u64>, Blake3)> = Vec::new();
    for &(_, i) in LOOKUPS {
        cases.push((bands(i), doc_hash(i)));
        cases.push((bands(i).into_iter().step_by(2).collect(), doc_hash(i)));
        cases.push((vec![42], doc_hash(i)));
        let mut more = bands(i);
        more.push(42);
        cases.push((more, Blake3([0; 32])));
    }
    for (bands, exclude) in &cases {
        for limit in [1, 10, 100, 1000] {
            let before = candidates(&conn, &before_sql(bands.len()), bands, exclude, limit);
            let after = candidates(
                &conn,
                &sketch_candidates_sql(bands.len()),
                bands,
                exclude,
                limit,
            );
            assert!(!before.is_empty() || bands == &[42]);
            assert_eq!(before, after, "limit {limit}");
        }
    }
}

/// Issue #74: writing the same sketch or search row again does not write it (the times stay),
/// and a change is still written.
#[tokio::test]
async fn unchanged_writes_write_nothing() {
    let store = Arc::new(Store::open_in_memory(&MasterKey::from_bytes([1; 32])).unwrap());
    let s = Standalone::new(Arc::clone(&store));
    let h = Blake3([7; 32]);
    s.put_record(&h, &json!({}), &doewood_model::StepVersions::new())
        .await
        .unwrap();
    s.put_sketch(&h, &[1, 2, 3], &[5, 6, u64::MAX])
        .await
        .unwrap();
    let doc = SearchDoc {
        blake3: h,
        title: "Invoice".to_owned(),
        body: "Consulting".to_owned(),
        kind: Some("invoice".to_owned()),
        date: None,
        amount: Some(10.0),
    };
    s.index_document(&doc).await.unwrap();
    // Times in the past: a write would set them to now.
    store
        .conn()
        .execute_batch("UPDATE sketches SET updated_at = 1; UPDATE search_index SET indexed_at = 1")
        .unwrap();
    let times = || -> (i64, i64) {
        store
            .conn()
            .query_row(
                "SELECT (SELECT updated_at FROM sketches), (SELECT indexed_at FROM search_index)",
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap()
    };
    let bands = || -> Vec<i64> {
        let conn = store.conn();
        let mut stmt = conn
            .prepare("SELECT band FROM sketch_bands ORDER BY band")
            .unwrap();
        stmt.query_map([], |r| r.get(0))
            .unwrap()
            .collect::<rusqlite::Result<_>>()
            .unwrap()
    };
    s.put_sketch(&h, &[1, 2, 3], &[u64::MAX, 6, 5, 5])
        .await
        .unwrap();
    s.index_document(&doc).await.unwrap();
    assert_eq!(times(), (1, 1));
    assert_eq!(bands(), [-1, 5, 6]);
    s.put_sketch(&h, &[1, 2, 4], &[5, 7]).await.unwrap();
    s.index_document(&SearchDoc {
        amount: None,
        ..doc.clone()
    })
    .await
    .unwrap();
    let (sketched, indexed) = times();
    assert!(sketched > 1 && indexed > 1, "{sketched} {indexed}");
    assert_eq!(bands(), [5, 7]);
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

/// The email domains as `website.rs` read them before issue #75.
async fn email_domains_before(s: &Standalone, company: Uuid) -> Vec<(Blake3, String)> {
    let mut out = Vec::new();
    for doc in s
        .company_documents(company)
        .await
        .unwrap()
        .into_iter()
        .take(50)
    {
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

async fn email_domains_after(s: &Standalone, company: Uuid) -> Vec<(Blake3, String)> {
    let hashes: Vec<Blake3> = s
        .company_documents_first(company, 50)
        .await
        .unwrap()
        .iter()
        .map(|d| d.key.parse().unwrap())
        .collect();
    let mut out = s.fact_texts(&hashes, "signal.email_domain").await.unwrap();
    out.sort();
    out
}

/// Times every path issues #74 and #75 changed, before and after, on `DOEWOOD_BENCH_DOCS`
/// generated documents (default 50,000) and `DOEWOOD_BENCH_COMPANIES` companies (default
/// 5,000), in a store on disk. See docs/ingest.md for the command and the numbers.
#[tokio::test]
#[ignore = "a measurement; run by hand"]
#[allow(clippy::too_many_lines)]
async fn read_path_timings() {
    let var = |name: &str, default: u64| {
        std::env::var(name)
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(default)
    };
    let (n, companies) = (
        var("DOEWOOD_BENCH_DOCS", 50_000),
        var("DOEWOOD_BENCH_COMPANIES", 5_000),
    );
    let dir = tempfile::tempdir().unwrap();
    let store =
        Arc::new(Store::open(&dir.path().join("s.db"), &MasterKey::from_bytes([1; 32])).unwrap());
    let s = Standalone::new(Arc::clone(&store));
    let t = Instant::now();
    seed(&mut store.conn(), n, companies);
    println!(
        "{n} documents, {companies} companies (seeded in {:.1} s); median of 9 runs, ms",
        t.elapsed().as_secs_f64()
    );

    for &(name, i) in LOOKUPS {
        let b = bands(i);
        let mut ms = [0.0; 2];
        for (k, sql) in [before_sql(b.len()), sketch_candidates_sql(b.len())]
            .iter()
            .enumerate()
        {
            ms[k] = median_ms(9, async || {
                drop(candidates(&store.conn(), sql, &b, &doc_hash(i), 100));
            })
            .await;
        }
        println!(
            "sketch_candidates, {name:<28} before {:>8.2}  after {:>8.2}",
            ms[0], ms[1]
        );
    }

    // Rewriting 200 unchanged sketches and search rows.
    let docs: Vec<u64> = (1..=200).collect();
    let search = |i: u64| SearchDoc {
        blake3: doc_hash(i),
        title: "Invoice 42 for consulting".to_owned(),
        body: "Consulting services for ACME, March 2024. ".repeat(40),
        kind: Some("invoice".to_owned()),
        date: None,
        amount: Some(1200.0),
    };
    for &i in &docs {
        s.index_document(&search(i)).await.unwrap();
    }
    let sig = |i: u64| -> Vec<u8> {
        (0..64)
            .flat_map(|k| mix(template(i) * 64 + k).to_be_bytes())
            .collect()
    };
    // The writes before issue #74, run as the store ran them: on its blocking pool.
    let put_before = async |i: u64| {
        let (sig, bands) = (sig(i), bands(i));
        s.run(move |conn| {
            let tx = conn.transaction().unwrap();
            let key = doc_hash(i);
            let key = key.as_bytes().as_slice();
            tx.execute(
                "INSERT INTO sketches (blake3, signature, updated_at) VALUES (?1, ?2, ?3) \
                 ON CONFLICT (blake3) DO UPDATE SET signature = excluded.signature, \
                 updated_at = excluded.updated_at",
                params![key, sig, now()],
            )
            .unwrap();
            tx.execute("DELETE FROM sketch_bands WHERE blake3 = ?1", [key])
                .unwrap();
            {
                let mut insert = tx
                    .prepare_cached(
                        "INSERT INTO sketch_bands (band, blake3) VALUES (?1, ?2) \
                         ON CONFLICT DO NOTHING",
                    )
                    .unwrap();
                for b in bands {
                    insert.execute(params![band_to_i64(b), key]).unwrap();
                }
            }
            tx.commit().unwrap();
            Ok(())
        })
        .await
        .unwrap();
    };
    let index_before = async |d: SearchDoc| {
        s.run(move |conn| {
            conn.execute(
                "INSERT INTO search_index (blake3, title, body, kind, doc_date, amount, \
                 indexed_at) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7) \
                 ON CONFLICT (blake3) DO UPDATE SET title = excluded.title, \
                 body = excluded.body, kind = excluded.kind, doc_date = excluded.doc_date, \
                 amount = excluded.amount, indexed_at = excluded.indexed_at",
                params![
                    d.blake3.as_bytes().as_slice(),
                    d.title,
                    d.body,
                    d.kind,
                    None::<f64>,
                    d.amount,
                    now()
                ],
            )
            .unwrap();
            Ok(())
        })
        .await
        .unwrap();
    };
    let before = median_ms(3, async || {
        for &i in &docs {
            put_before(i).await;
        }
    })
    .await;
    let after = median_ms(3, async || {
        for &i in &docs {
            s.put_sketch(&doc_hash(i), &sig(i), &bands(i))
                .await
                .unwrap();
        }
    })
    .await;
    println!("put_sketch x200 unchanged          before {before:>8.2}  after {after:>8.2}");
    let before = median_ms(3, async || {
        for &i in &docs {
            index_before(search(i)).await;
        }
    })
    .await;
    let after = median_ms(3, async || {
        for &i in &docs {
            s.index_document(&search(i)).await.unwrap();
        }
    })
    .await;
    println!("index_document x200 unchanged      before {before:>8.2}  after {after:>8.2}");

    // One reprocess page of 500.
    let page: Vec<Blake3> = (1..=500).map(doc_hash).collect();
    let before = median_ms(9, async || {
        for h in &page {
            drop(s.get_record(h).await.unwrap());
        }
    })
    .await;
    let after = median_ms(9, async || drop(s.record_versions(&page).await.unwrap())).await;
    println!("reprocess page of 500 (versions)   before {before:>8.2}  after {after:>8.2}");

    // The merge gate's counts and the email domains, for the owner and a typical company.
    for c in [1, 2] {
        let id: Uuid = uuid_of(1, c).parse().unwrap();
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
        println!("gate count, {docs:>6} documents       before {before:>8.2}  after {after:>8.2}");
        assert_eq!(
            email_domains_before(&s, id).await,
            email_domains_after(&s, id).await
        );
        let before = median_ms(9, async || drop(email_domains_before(&s, id).await)).await;
        let after = median_ms(9, async || drop(email_domains_after(&s, id).await)).await;
        println!("email domains, {docs:>6} documents    before {before:>8.2}  after {after:>8.2}");
    }
}
