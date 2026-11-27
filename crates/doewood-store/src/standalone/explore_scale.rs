//! Issue #172 on generated data: the company graph's counts, folded from the log, match a
//! count from scratch over the edges (and the document total of issue #181, also across the
//! upgrade to schema 15, and after purges, issue #168); and an ignored test times the
//! explorer's reads on
//! `DOEWOOD_BENCH_DOCS` documents (default 200,000) and `DOEWOOD_BENCH_COMPANIES` companies
//! (default 20,000). See docs/api.md, "The explorer's reads".

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Instant;

use doewood_core::MasterKey;
use doewood_model::explore::LINK_PARTIES_MAX;
use doewood_model::{GraphStore, PurgeStore, Purged};
use rusqlite::{Connection, params};
use serde_json::json;
use uuid::Uuid;

use super::Standalone;
use crate::Store;
use crate::store::now;

fn mix(mut z: u64) -> u64 {
    z = z.wrapping_add(0x9e37_79b9_7f4a_7c15);
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    z ^ (z >> 31)
}

fn uuid_of(tag: u64, i: u64) -> Uuid {
    Uuid::from_u64_pair(mix(tag), mix(i))
}

/// Company `c` of `companies` (1-based) for a uniform draw `u`: squared, so low numbers are
/// hubs (company 1 is in about one document in 70 at 20,000 companies).
#[allow(
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss
)]
fn skewed(u: u64, companies: u64) -> u64 {
    let x = (u >> 11) as f64 / (1u64 << 53) as f64;
    1 + ((x * x) * companies as f64) as u64 % companies
}

/// `n` documents with three `party` edges each (buyer and supplier skewed to the hubs,
/// signatory uniform), `companies` named companies, and a corporate edge from each of the
/// first tenth to another; with `churn`, one party edge in 13 retracted afterwards and one
/// in 29 deleted. Written in SQL with the triggers on, as ingest's writes would log them.
pub(super) fn seed(conn: &mut Connection, n: u64, companies: u64, churn: bool) {
    let t = now();
    let tx = conn.transaction().unwrap();
    {
        let mut node = tx
            .prepare(
                "INSERT INTO nodes (id, kind, key, props_json, created_at, updated_at) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?5)",
            )
            .unwrap();
        let mut edge = tx
            .prepare(
                "INSERT OR IGNORE INTO edges (id, src, dst, kind, props_json, confidence, \
                 state, source, created_at, updated_at) \
                 VALUES (?1, ?2, ?3, ?4, ?5, 0.9, 'active', ?6, ?7, ?7)",
            )
            .unwrap();
        for c in 1..=companies {
            node.execute(params![
                uuid_of(1, c).to_string(),
                "company",
                format!("co-{c}"),
                json!({"name": format!("Company {:05}", mix(c) % 100_000)}).to_string(),
                t
            ])
            .unwrap();
        }
        for i in 1..=n {
            let doc = uuid_of(2, i).to_string();
            node.execute(params![doc, "document", format!("{:064x}", i), "{}", t])
                .unwrap();
            for (k, role) in [(1, "buyer"), (2, "supplier"), (3, "signatory")] {
                let c = if k == 3 {
                    1 + mix(i * 4 + k) % companies
                } else {
                    skewed(mix(i * 4 + k), companies)
                };
                edge.execute(params![
                    uuid_of(3, i * 4 + k).to_string(),
                    doc,
                    uuid_of(1, c).to_string(),
                    "party",
                    json!({"role": role}).to_string(),
                    "document",
                    t
                ])
                .unwrap();
            }
        }
        for c in 1..=companies / 10 {
            edge.execute(params![
                uuid_of(4, c).to_string(),
                uuid_of(1, c).to_string(),
                uuid_of(1, 1 + mix(c) % companies).to_string(),
                "parent_of",
                "{}",
                "registry",
                t
            ])
            .unwrap();
        }
    }
    if churn {
        tx.execute_batch(
            "UPDATE edges SET state = 'retracted' WHERE kind = 'party' \
             AND (unicode(substr(id, 36)) * 7 + unicode(substr(id, 35))) % 13 = 0;
             DELETE FROM edges WHERE kind = 'party' \
             AND (unicode(substr(id, 36)) * 5 + unicode(substr(id, 34))) % 29 = 0;",
        )
        .unwrap();
    }
    tx.commit().unwrap();
    conn.execute_batch("ANALYZE").unwrap();
}

/// Each company's documents and degree; each pair's shared documents and events.
type Counts = (
    BTreeMap<String, (i64, i64)>,
    BTreeMap<(String, String), (i64, i64)>,
);

/// The counts from scratch: each company's documents and degree, and each pair's shared
/// documents and events.
fn from_scratch(conn: &Connection) -> Counts {
    let live = "e.state NOT IN ('retracted', 'superseded')";
    let mut pairs: BTreeMap<(String, String), (i64, i64)> = BTreeMap::new();
    let mut st = conn
        .prepare(&format!(
            "WITH p AS (SELECT DISTINCT e.src, e.dst FROM edges e WHERE e.kind = 'party' AND {live}), \
             n AS (SELECT src, count(*) AS k FROM p GROUP BY src) \
             SELECT x.dst, y.dst, count(*) FROM p x JOIN p y ON y.src = x.src AND x.dst < y.dst \
             JOIN n ON n.src = x.src WHERE n.k <= {LINK_PARTIES_MAX} GROUP BY 1, 2"
        ))
        .unwrap();
    for r in st
        .query_map([], |r| {
            Ok((r.get::<_, String>(0)?, r.get(1)?, r.get::<_, i64>(2)?))
        })
        .unwrap()
    {
        let (a, b, n) = r.unwrap();
        pairs.entry((a, b)).or_default().0 = n;
    }
    let mut st = conn
        .prepare(&format!(
            "SELECT min(e.src, e.dst), max(e.src, e.dst), count(*) FROM edges e \
             WHERE e.kind NOT IN ('party', 'located_at', 'amends', 'cites', 'similar_to') \
             AND {live} AND e.src <> e.dst GROUP BY 1, 2"
        ))
        .unwrap();
    for r in st
        .query_map([], |r| {
            Ok((r.get::<_, String>(0)?, r.get(1)?, r.get::<_, i64>(2)?))
        })
        .unwrap()
    {
        let (a, b, n) = r.unwrap();
        pairs.entry((a, b)).or_default().1 = n;
    }
    let mut stats: BTreeMap<String, (i64, i64)> = BTreeMap::new();
    let mut st = conn
        .prepare(&format!(
            "SELECT e.dst, count(DISTINCT e.src) FROM edges e WHERE e.kind = 'party' \
             AND {live} GROUP BY 1"
        ))
        .unwrap();
    for r in st
        .query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, i64>(1)?)))
        .unwrap()
    {
        let (c, n) = r.unwrap();
        stats.entry(c).or_default().0 = n;
    }
    for (a, b) in pairs.keys() {
        stats.entry(a.clone()).or_default().1 += 1;
        stats.entry(b.clone()).or_default().1 += 1;
    }
    (stats, pairs)
}

fn stored(conn: &Connection) -> Counts {
    let mut stats = BTreeMap::new();
    let mut st = conn
        .prepare("SELECT company_id, documents, degree FROM company_stats WHERE documents > 0 OR degree > 0")
        .unwrap();
    for r in st
        .query_map([], |r| Ok((r.get::<_, String>(0)?, r.get(1)?, r.get(2)?)))
        .unwrap()
    {
        let (c, d, g) = r.unwrap();
        stats.insert(c, (d, g));
    }
    let mut pairs = BTreeMap::new();
    let mut st = conn
        .prepare("SELECT a, b, docs, events FROM company_links")
        .unwrap();
    for r in st
        .query_map([], |r| {
            Ok((r.get::<_, String>(0)?, r.get(1)?, r.get(2)?, r.get(3)?))
        })
        .unwrap()
    {
        let (a, b, d, e) = r.unwrap();
        pairs.insert((a, b), (d, e));
    }
    (stats, pairs)
}

/// The churn after the first fold: party edges retracted, deleted and moved to `co-1`, and
/// every party edge of one document in 16 retracted (so those leave the document total).
const CHURN: &str = "UPDATE edges SET state = 'retracted' WHERE kind = 'party' \
     AND (unicode(substr(id, 36)) * 7 + unicode(substr(id, 35))) % 13 = 0;
     DELETE FROM edges WHERE kind = 'party' \
     AND (unicode(substr(id, 36)) * 5 + unicode(substr(id, 34))) % 29 = 0;
     UPDATE OR IGNORE edges SET dst = (SELECT id FROM nodes WHERE key = 'co-1') \
     WHERE kind = 'party' AND unicode(substr(id, 36)) % 31 = 0;
     UPDATE edges SET state = 'retracted' WHERE kind = 'party' AND src IN \
     (SELECT id FROM nodes WHERE kind = 'document' AND key LIKE '%0');";

/// The documents with a live party edge, from scratch; and as folded.
fn totals(conn: &Connection) -> (i64, i64) {
    let want = conn
        .query_row(
            "SELECT count(DISTINCT src) FROM edges WHERE kind = 'party' \
             AND state NOT IN ('retracted', 'superseded')",
            [],
            |r| r.get(0),
        )
        .unwrap();
    let got = conn
        .query_row("SELECT documents FROM company_graph_totals", [], |r| {
            r.get(0)
        })
        .unwrap();
    (want, got)
}

#[tokio::test]
async fn folded_counts_match_a_count_from_scratch() {
    let store = Arc::new(Store::open_in_memory(&MasterKey::from_bytes([1; 32])).unwrap());
    let s = Standalone::new(Arc::clone(&store));
    seed(&mut store.conn(), 3000, 300, false);
    // Half folded before the churn, the rest after: both paths of the log.
    s.fold_company_graph(2000).await.unwrap();
    {
        let mut conn = store.conn();
        let tx = conn.transaction().unwrap();
        tx.execute_batch(CHURN).unwrap();
        tx.commit().unwrap();
    }
    while s.fold_company_graph(500).await.unwrap().more {}
    let conn = store.conn();
    let (want_stats, want_pairs) = from_scratch(&conn);
    let (stats, pairs) = stored(&conn);
    assert!(want_pairs.len() > 1000, "{}", want_pairs.len());
    assert_eq!(pairs, want_pairs);
    assert_eq!(stats, want_stats);
    let (want, got) = totals(&conn);
    assert_eq!(got, want);
    assert!(got < 3000 - 150, "retractions lower the total: {got}");
}

/// Records for every seeded document, and a live location for one in ten (`i % 10 == 0`):
/// the rest are orphans for a purge. Returns how many have a location.
fn locate_some(conn: &mut Connection, n: u64) -> i64 {
    let t = now();
    let tx = conn.transaction().unwrap();
    let mut kept = 0;
    for i in 1..=n {
        let key = format!("{i:064x}");
        let mut h = [0u8; 32];
        h[24..].copy_from_slice(&i.to_be_bytes());
        tx.execute(
            "INSERT INTO records (blake3, body_json, versions_json, updated_at) \
             VALUES (?1, '{}', '{}', ?2)",
            params![&h[..], t],
        )
        .unwrap();
        if i % 10 == 0 {
            let loc = uuid_of(5, i).to_string();
            tx.execute(
                "INSERT INTO nodes (id, kind, key, props_json, created_at, updated_at) \
                 VALUES (?1, 'location', ?2, '{}', ?3, ?3)",
                params![loc, json!(["http://s", "b", key]).to_string(), t],
            )
            .unwrap();
            tx.execute(
                "INSERT INTO locations (node_id, document_id, blake3, endpoint, bucket, \
                 object_key, updated_at) VALUES (?1, ?2, ?3, 'http://s', 'b', ?4, ?5)",
                params![loc, uuid_of(2, i).to_string(), &h[..], key, t],
            )
            .unwrap();
            kept += 1;
        }
    }
    tx.commit().unwrap();
    kept
}

/// Issue #168: purges delete documents (and the companies only they named, with their
/// corporate edges) through the edges' delete triggers, so the folded counts still match a
/// count from scratch.
#[tokio::test]
async fn purged_counts_match_a_count_from_scratch() {
    let store = Arc::new(Store::open_in_memory(&MasterKey::from_bytes([1; 32])).unwrap());
    let s = Standalone::new(Arc::clone(&store));
    seed(&mut store.conn(), 3000, 300, false);
    let kept = locate_some(&mut store.conn(), 3000);
    s.fold_company_graph(2000).await.unwrap();
    {
        let mut conn = store.conn();
        let tx = conn.transaction().unwrap();
        tx.execute_batch(CHURN).unwrap();
        tx.commit().unwrap();
    }
    let (mut purged, mut after) = (Purged::default(), None::<String>);
    loop {
        let p = s.purge_orphan_records(after.as_deref(), 97).await.unwrap();
        purged.add(&p);
        // A fold between batches, as the background fold runs.
        s.fold_company_graph(50).await.unwrap();
        match p.next {
            Some(n) => after = Some(n),
            None => break,
        }
    }
    assert_eq!(i64::try_from(purged.records).unwrap(), 3000 - kept);
    assert!(purged.companies > 0, "{purged:?}");
    while s.fold_company_graph(500).await.unwrap().more {}
    let conn = store.conn();
    let left: i64 = conn
        .query_row("SELECT count(*) FROM records", [], |r| r.get(0))
        .unwrap();
    assert_eq!(left, kept);
    let (want_stats, want_pairs) = from_scratch(&conn);
    let (stats, pairs) = stored(&conn);
    assert!(!want_pairs.is_empty());
    assert_eq!(pairs, want_pairs);
    assert_eq!(stats, want_stats);
    let (want, got) = totals(&conn);
    assert_eq!(got, want);
    let stale: i64 = conn
        .query_row(
            "SELECT count(*) FROM company_stats s WHERE NOT EXISTS \
             (SELECT 1 FROM nodes n WHERE n.id = s.company_id)",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(stale, 0);
}

/// Issue #181: the upgrade to schema 15 counts the total as folded so far, so the folds after
/// it keep it exact, though the log still holds rows of documents folded before (and churned
/// since) and of documents never folded.
#[tokio::test]
async fn schema_15_counts_the_total_with_log_rows_pending() {
    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().join(crate::STORE_FILE);
    {
        let mut conn = Connection::open(&path).unwrap();
        assert_eq!(crate::migrate::migrate_to(&mut conn, 15).unwrap(), 15);
        seed(&mut conn, 3000, 300, false);
        // Fold a part as schema 14 would (the total's row aside), then go back to schema 14.
        super::explore::fold(&mut conn, 2000).unwrap();
        conn.execute_batch("DROP TABLE company_graph_totals; PRAGMA user_version = 14;")
            .unwrap();
        let tx = conn.transaction().unwrap();
        tx.execute_batch(CHURN).unwrap();
        tx.commit().unwrap();
        assert!(
            conn.query_row("SELECT count(*) FROM company_graph_log", [], |r| r
                .get::<_, i64>(0))
                .unwrap()
                > 1000
        );
    }
    let store = Arc::new(Store::open(&path, &MasterKey::from_bytes([1; 32])).unwrap());
    let s = Standalone::new(Arc::clone(&store));
    while s.fold_company_graph(500).await.unwrap().more {}
    let total = s.company_documents_total().await.unwrap();
    let conn = store.conn();
    let (want, got) = totals(&conn);
    assert_eq!(got, want);
    assert_eq!(total, u64::try_from(want).unwrap());
    let (want_stats, want_pairs) = from_scratch(&conn);
    assert_eq!(stored(&conn), (want_stats, want_pairs));
}

/// `cargo test -p doewood-store --lib explorer_timings -- --ignored --nocapture`
#[tokio::test]
#[ignore = "a measurement (issue #172); run by hand"]
async fn explorer_timings() {
    let var = |name: &str, default: u64| {
        std::env::var(name)
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(default)
    };
    let (n, companies) = (
        var("DOEWOOD_BENCH_DOCS", 200_000),
        var("DOEWOOD_BENCH_COMPANIES", 20_000),
    );
    let dir = tempfile::tempdir().unwrap();
    let store =
        Arc::new(Store::open(&dir.path().join("s.db"), &MasterKey::from_bytes([1; 32])).unwrap());
    let s = Standalone::new(Arc::clone(&store));
    let t = Instant::now();
    seed(&mut store.conn(), n, companies, true);
    println!(
        "SQLite: {n} documents, {companies} companies (seeded in {:.1} s); median of 9 runs",
        t.elapsed().as_secs_f64()
    );
    doewood_model::conformance::explorer_timings(
        &s,
        uuid_of(1, 1),
        uuid_of(1, companies - 1),
        uuid_of(1, companies - 2),
        9,
    )
    .await;
}
