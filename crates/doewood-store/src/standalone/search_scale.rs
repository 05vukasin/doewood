//! Search on words alone (issue #140): the query that scores the matches on the FTS index and
//! joins only those that can reach the page returns what the query before it returned, which
//! joined and sorted every match; an ignored test times both on a big generated store (see
//! docs/deploy.md, "Search on a big corpus").

use std::sync::Arc;
use std::time::Instant;

use doewood_core::MasterKey;
use doewood_model::{SearchHit, SearchQuery, SearchStore};
use rusqlite::{Connection, params};

use super::{Standalone, from_julian};
use crate::Store;

/// The search on words alone before issue #140, kept as the reference: the phrase, the limit,
/// the offset.
const BEFORE: &str = "SELECT s.blake3, s.title, s.kind, s.doc_date, s.amount, \
     -bm25(search_fts, 10.0, 1.0) AS score \
     FROM search_fts JOIN search_index s ON s.rowid = search_fts.rowid \
     WHERE search_fts MATCH ?1 \
     ORDER BY score DESC, s.doc_date IS NULL, s.doc_date DESC, s.blake3 LIMIT ?2 OFFSET ?3";

/// Words of the documents, as in `api_load` (`doewood-api`).
const WORDS: [&str; 24] = [
    "invoice",
    "contract",
    "annex",
    "delivery",
    "payment",
    "supplier",
    "buyer",
    "steel",
    "timber",
    "cement",
    "transport",
    "service",
    "warranty",
    "penalty",
    "schedule",
    "price",
    "discount",
    "quantity",
    "order",
    "receipt",
    "tender",
    "lease",
    "insurance",
    "audit",
];

/// A small deterministic generator (xorshift), so the corpus is the same on every run.
struct Rng(u64);

impl Rng {
    fn below(&mut self, n: u64) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0 % n
    }
}

/// `n` documents of varied length, with repeated words, words in titles, dates (some missing,
/// many shared), kinds and amounts; every 5th document a copy of the text of an earlier one,
/// so many scores tie and the date and the hash decide.
fn seed_varied(conn: &Connection, n: u32) {
    let mut rng = Rng(0x9e37_79b9_7f4a_7c15);
    let tx = conn.unchecked_transaction().unwrap();
    let mut texts: Vec<(String, String)> = Vec::new();
    for i in 0..n {
        let (title, body) = if i % 5 == 4 && !texts.is_empty() {
            texts[usize::try_from(rng.below(texts.len() as u64)).unwrap()].clone()
        } else {
            let mut body = Vec::new();
            for _ in 0..(3 + rng.below(120)) {
                body.push(match rng.below(4) {
                    0 => WORDS[usize::try_from(rng.below(24)).unwrap()].to_owned(),
                    1 => WORDS[usize::try_from(rng.below(4)).unwrap()].to_owned(),
                    _ => format!("term{}", rng.below(300)),
                });
            }
            let title = format!(
                "{} {}",
                WORDS[usize::try_from(rng.below(24)).unwrap()],
                if rng.below(3) == 0 { "steel" } else { "doc" }
            );
            let t = (title, body.join(" "));
            texts.push(t.clone());
            t
        };
        let mut h = [0u8; 32];
        h[..8].copy_from_slice(&rng.below(u64::MAX).to_le_bytes());
        h[8..12].copy_from_slice(&i.to_le_bytes());
        tx.execute(
            "INSERT INTO records (blake3, body_json, versions_json, updated_at) \
             VALUES (?1, '{}', '{\"quail\":\"1\"}', 0)",
            [h.as_slice()],
        )
        .unwrap();
        let date = (rng.below(7) != 0).then(|| 2_458_850 + rng.below(40).cast_signed());
        let kind = ["invoice", "contract", "offer"][usize::try_from(rng.below(3)).unwrap()];
        tx.execute(
            "INSERT INTO search_index (blake3, title, body, kind, doc_date, amount, indexed_at) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, 0)",
            params![
                h.as_slice(),
                title,
                body,
                kind,
                date,
                rng.below(1000).cast_signed()
            ],
        )
        .unwrap();
    }
    tx.commit().unwrap();
}

/// `n` documents as `api_load` seeds them (`doewood-api`): 20 of the 24 words and 20 of 5,000
/// rare terms each, titled with a word and the number; and every 4th document also has
/// `quarter` and `i % 13` more words, so a word in a quarter of the documents scores by
/// length, as in a real corpus. Each document has a record of about 1 KiB.
fn seed_api_load(conn: &Connection, n: u32) {
    let words = WORDS
        .iter()
        .enumerate()
        .map(|(k, w)| format!("({k}, '{w}')"))
        .collect::<Vec<_>>()
        .join(", ");
    conn.execute_batch(&format!(
        "CREATE TEMP TABLE seed_w (k INTEGER PRIMARY KEY, w TEXT);
         INSERT INTO seed_w VALUES {words};
         CREATE TEMP TABLE seed_k AS
           WITH RECURSIVE s (k) AS (SELECT 0 UNION ALL SELECT k + 1 FROM s WHERE k < 19)
           SELECT k FROM s;
         CREATE TEMP TABLE seed_docs AS
           WITH RECURSIVE s (i) AS (SELECT 0 UNION ALL SELECT i + 1 FROM s WHERE i < {n} - 1)
           SELECT i, unhex(printf('%016x%048x', i, 0)) AS h FROM s;
         INSERT INTO records (blake3, body_json, versions_json, updated_at)
           SELECT h, json_object('n', i, 'pad', hex(randomblob(480))), '{{\"quail\":\"1\"}}', 0
           FROM seed_docs;
         INSERT INTO search_index (blake3, title, body, kind, doc_date, amount, indexed_at)
           SELECT d.h, (SELECT w FROM seed_w WHERE k = d.i % 24) || ' ' || d.i,
                  (SELECT group_concat(w, ' ') FROM seed_k JOIN seed_w
                     ON seed_w.k = (d.i * 7 + seed_k.k * 13) % 24)
                  || ' ' || (SELECT group_concat('term' || ((d.i * 20 + k) * 2654435761 % 5000),
                     ' ') FROM seed_k)
                  || CASE WHEN d.i % 4 = 0 THEN ' quarter' || coalesce((SELECT group_concat(' w', '')
                     FROM seed_k WHERE k < d.i % 13), '') ELSE '' END,
                  'invoice', NULL, 100.0, 0
           FROM seed_docs d;
         DROP TABLE seed_w; DROP TABLE seed_k; DROP TABLE seed_docs;"
    ))
    .unwrap();
}

fn before(conn: &Connection, q: &SearchQuery) -> Vec<SearchHit> {
    let phrase = q
        .words()
        .iter()
        .map(|w| format!("\"{w}\""))
        .collect::<Vec<_>>()
        .join(" ");
    let mut stmt = conn.prepare_cached(BEFORE).unwrap();
    #[allow(clippy::cast_possible_truncation)]
    stmt.query_map(params![phrase, q.effective_limit(), q.offset], |r| {
        Ok(SearchHit {
            blake3: doewood_model::Blake3::from_slice(&r.get::<_, Vec<u8>>(0)?).unwrap(),
            title: r.get(1)?,
            kind: r.get(2)?,
            date: from_julian(r.get(3)?).unwrap(),
            amount: r.get(4)?,
            score: r.get::<_, f64>(5)? as f32,
        })
    })
    .unwrap()
    .collect::<rusqlite::Result<_>>()
    .unwrap()
}

fn text(text: &str, limit: u32, offset: u32) -> SearchQuery {
    SearchQuery {
        text: Some(text.to_owned()),
        limit,
        offset,
        ..SearchQuery::default()
    }
}

#[tokio::test]
async fn text_search_matches_the_query_before() {
    for (corpus, n) in [("varied", 6000), ("api_load", 3000)] {
        let store = Arc::new(Store::open_in_memory(&MasterKey::from_bytes([1; 32])).unwrap());
        if corpus == "varied" {
            seed_varied(&store.conn(), n);
        } else {
            seed_api_load(&store.conn(), n);
        }
        let s = Standalone::new(Arc::clone(&store));
        let mut total = 0;
        let mut ties = 0;
        for words in [
            "invoice",
            "steel",
            "term7",
            "invoice term7",
            "Steel, ANNEX",
            "contract annex delivery",
            "quarter",
            "term4999",
            "nothing",
        ] {
            for (limit, offset) in [
                (0, 0),
                (1, 0),
                (20, 0),
                (20, 1),
                (100, 40),
                (1000, 0),
                (1000, 900),
                (7, 2500),
                (50, 100_000),
            ] {
                let q = text(words, limit, offset);
                let want = before(&store.conn(), &q);
                let got = s.search(&q).await.unwrap();
                assert_eq!(got, want, "{corpus}: {q:?}");
                total += want.len();
                ties += want
                    .windows(2)
                    .filter(|w| w[0].score.to_bits() == w[1].score.to_bits())
                    .count();
            }
        }
        // Both corpora have pages to compare, and pages that ties decide.
        assert!(total > 5_000, "{corpus}: {total}");
        assert!(ties > 1_000, "{corpus}: {ties}");
    }
}

/// The median and the 99th percentile of 3 timed runs of `f` over `queries` (after one
/// untimed run), in milliseconds.
fn timings(queries: &[SearchQuery], mut f: impl FnMut(&SearchQuery)) -> (f64, f64) {
    for q in queries {
        f(q);
    }
    let mut ms = Vec::new();
    for _ in 0..3 {
        for q in queries {
            let t = Instant::now();
            f(q);
            ms.push(t.elapsed().as_secs_f64() * 1000.0);
        }
    }
    ms.sort_by(f64::total_cmp);
    (ms[ms.len() / 2], ms[ms.len() * 99 / 100])
}

/// Times search on words alone before and after issue #140 on `DOEWOOD_BENCH_DOCS` documents
/// (default 50,000) shaped as `api_load` seeds them, in a store on disk, on one of its
/// readers. See docs/deploy.md, "Search on a big corpus", for the command and the numbers.
#[test]
#[ignore = "a measurement; run by hand"]
fn text_search_timings() {
    let n: u32 = std::env::var("DOEWOOD_BENCH_DOCS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(50_000);
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(&dir.path().join("s.db"), &MasterKey::from_bytes([1; 32])).unwrap();
    let t = Instant::now();
    seed_api_load(&store.conn(), n);
    println!(
        "search: seeded {n} documents in {:.1}s",
        t.elapsed().as_secs_f64()
    );
    let rare: Vec<_> = (0..40)
        .map(|k| text(&format!("term{}", k * 125), 20, 0))
        .collect();
    let mixed: Vec<_> = (0..40)
        .map(|k| text(&format!("{} term{}", WORDS[k % 24], k * 125), 20, 0))
        .collect();
    let quarter = vec![text("quarter", 20, 0); 5];
    let common = vec![text("annex", 20, 0); 5];
    let conn = store.reader();
    for (name, queries) in [
        ("a rare term (api_load)", &rare),
        ("a word and a rare term (api_load)", &mixed),
        ("a word in a quarter of the documents", &quarter),
        ("a word in most documents", &common),
    ] {
        let phrase = queries[0]
            .words()
            .iter()
            .map(|w| format!("\"{w}\""))
            .collect::<Vec<_>>()
            .join(" ");
        let matches: i64 = conn
            .query_row(
                "SELECT count(*) FROM search_fts WHERE search_fts MATCH ?1",
                [phrase],
                |r| r.get(0),
            )
            .unwrap();
        let (b50, b99) = timings(queries, |q| {
            before(&conn, q);
        });
        let (a50, a99) = timings(queries, |q| {
            super::text_search(&conn, &q.words(), q).unwrap();
        });
        println!(
            "search: {name} ({matches} matches for the first query): before p50 {b50:.2} ms, p99 \
             {b99:.2} ms; after p50 {a50:.2} ms, p99 {a99:.2} ms"
        );
    }
}

/// The key facts every 2nd document of [`seed_api_load`]'s gets (issue #194): a card's text
/// form and two highlighted passages, about 250 bytes, with `otkazni rok` in every 10th.
const SEED_FACTS: &str = "UPDATE search_index SET facts = \
     'kind: contract' || char(10) || 'notice: iso=P30D; days=30' || char(10) \
     || 'penalty: percent=0.5; per=day' || char(10) || 'amount: ' || (rowid * 37 % 100000) \
     || '.00 RSD' || char(10) || 'governing_law: RS' || char(10) \
     || CASE WHEN rowid % 10 = 0 THEN 'Svaka strana može otkazati ugovor uz otkazni rok od 30 dana.' \
        ELSE 'Ugovor se zaključuje na period od 12 meseci.' END || char(10) \
     || 'Ugovorna kazna iznosi 0,5% za svaki dan kašnjenja, term' || (rowid * 11 % 5000) \
     WHERE rowid % 2 = 0";

/// Issue #194: search before and after the key facts joined the FTS index (schema 19 against
/// 20), on `DOEWOOD_BENCH_DOCS` documents (default 50,000) shaped as `api_load` seeds them, in
/// a store on disk: words alone and with a kind filter, before the migration, after it with
/// no facts yet, and after every 2nd document got about 250 bytes of facts; and the
/// migration's time. See docs/api.md, "Search ranking of key facts".
#[tokio::test]
#[ignore = "a measurement; run by hand"]
#[allow(clippy::too_many_lines)]
async fn fact_search_timings() {
    let n: u32 = std::env::var("DOEWOOD_BENCH_DOCS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(50_000);
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join(crate::STORE_FILE);
    let mut conn = Connection::open(&path).unwrap();
    assert_eq!(crate::migrate::migrate_to(&mut conn, 19).unwrap(), 19);
    seed_api_load(&conn, n);
    let words = [
        ("a rare term", "term125"),
        ("a word and a rare term", "invoice term250"),
        ("a word in a quarter of the documents", "quarter"),
        ("a word in most documents", "annex"),
        ("two words of the facts", "otkazni rok"),
    ];
    let queries = |w: &str, kind: bool| {
        let mut q = text(w, 20, 0);
        if kind {
            q.kind = Some("invoice".to_owned());
        }
        vec![q; 20]
    };
    let schema_19 = TEXT_SEARCH_19.as_str();
    let phrase = |q: &SearchQuery| {
        q.words()
            .iter()
            .map(|w| format!("\"{w}\""))
            .collect::<Vec<_>>()
            .join(" ")
    };
    let old = |conn: &Connection, q: &SearchQuery| {
        let mut stmt = conn.prepare_cached(schema_19).unwrap();
        stmt.query_map(params![phrase(q), q.effective_limit(), q.offset], |r| {
            r.get::<_, Vec<u8>>(0)
        })
        .unwrap()
        .count()
    };
    for (name, w) in words {
        let (p50, p99) = timings(&queries(w, false), |q| {
            old(&conn, q);
        });
        println!("facts: schema 19, {name}: p50 {p50:.2} ms, p99 {p99:.2} ms");
    }
    let t = Instant::now();
    assert_eq!(crate::migrate::migrate_to(&mut conn, 20).unwrap(), 20);
    println!(
        "facts: migration to schema 20 ({n} documents): {:.2} s",
        t.elapsed().as_secs_f64()
    );
    drop(conn);
    let store = Arc::new(Store::open(&path, &MasterKey::from_bytes([1; 32])).unwrap());
    let s = Standalone::new(Arc::clone(&store));
    let run = |label: &'static str| {
        let s = &s;
        async move {
            for (name, w) in words {
                for kind in [false, true] {
                    let qs = queries(w, kind);
                    for q in &qs {
                        s.search(q).await.unwrap();
                    }
                    let mut ms = Vec::new();
                    for _ in 0..3 {
                        for q in &qs {
                            let t = Instant::now();
                            s.search(q).await.unwrap();
                            ms.push(t.elapsed().as_secs_f64() * 1000.0);
                        }
                    }
                    ms.sort_by(f64::total_cmp);
                    let hits = s.search(&qs[0]).await.unwrap().len();
                    println!(
                        "facts: {label}, {name}{}: p50 {:.2} ms, p99 {:.2} ms ({hits} hits)",
                        if kind { ", kind filter" } else { "" },
                        ms[ms.len() / 2],
                        ms[ms.len() * 99 / 100]
                    );
                }
            }
        }
    };
    run("schema 20, no facts").await;
    let t = Instant::now();
    let changed = store.conn().execute(SEED_FACTS, []).unwrap();
    println!(
        "facts: {changed} documents got facts in {:.2} s",
        t.elapsed().as_secs_f64()
    );
    run("schema 20, facts on every 2nd").await;
}

/// [`super::TEXT_SEARCH`] as schema 19 ran it: two FTS columns, weights 10 and 1.
static TEXT_SEARCH_19: std::sync::LazyLock<String> =
    std::sync::LazyLock::new(|| super::TEXT_SEARCH.replace("10.0, 1.0, 20.0", "10.0, 1.0"));
