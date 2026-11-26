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
