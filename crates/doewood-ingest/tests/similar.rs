//! `similar_to` edges on a generated corpus with near-duplicates (issue #126): the graph writer
//! writes the same edges as the per-candidate loop it had before (a node lookup, maybe a node
//! insert, and an edge upsert for each candidate, with no stop band). No band bucket of this
//! corpus is over the default `similar_max_bucket` (1000), so the stop band of issue #148
//! skips nothing and the edges stay the same. On SQLite and, when
//! `DOEWOOD_TEST_POSTGRES_URL` is set, on Postgres in a schema of its own.

mod common;

use std::sync::Arc;

use doewood_core::MasterKey;
use doewood_ingest::facts::DocFacts;
use doewood_ingest::{GraphWriter, SimilarityConfig};
use doewood_model::{
    Backend, Blake3, Edge, EdgeKind, EdgeSource, NodeKind, ObjectLocation, StepVersions,
};
use doewood_store::{Standalone, Store};
use serde_json::json;

const PG_VAR: &str = "DOEWOOD_TEST_POSTGRES_URL";
const DOCS: u64 = 400;
const MIN_JACCARD: f64 = 0.8;

fn hash(i: u64) -> Blake3 {
    Blake3::from_slice(blake3::hash(format!("similar/{i}").as_bytes()).as_bytes()).unwrap()
}

fn facts(i: u64) -> DocFacts {
    DocFacts {
        format: "txt".into(),
        minhash: common::sketch(&common::near_duplicate_text(i, 3)),
        ..DocFacts::default()
    }
}

fn at(i: u64) -> ObjectLocation {
    ObjectLocation {
        endpoint: "http://objects.test:9000".into(),
        bucket: "docs".into(),
        key: format!("similar/{i}.txt"),
    }
}

/// Whether document `i` has only its record and sketch, no Document node: its node is
/// created when another document finds it as a candidate.
fn sketch_only(i: u64) -> bool {
    i % 7 == 3
}

/// The record of document `i`; for a sketch-only one, its sketch too.
async fn record(b: &dyn Backend, i: u64) {
    let body = doewood_model::canonical::to_canonical(&json!({})).unwrap();
    b.put_record(&hash(i), &body, &StepVersions::new())
        .await
        .unwrap();
    if sketch_only(i) {
        let (sig, bands) = facts(i).minhash.unwrap();
        b.put_sketch(&hash(i), &sig, &bands).await.unwrap();
    }
}

/// The corpus through the graph writer.
async fn write_new(b: &Arc<dyn Backend>) {
    let writer = GraphWriter::new(Arc::clone(b), SimilarityConfig::default());
    for i in 0..DOCS {
        record(&**b, i).await;
        if !sketch_only(i) {
            let stats = writer
                .write(&hash(i), &at(i), &facts(i), "x", MIN_JACCARD)
                .await
                .unwrap();
            assert_eq!(stats.similar_buckets_skipped, 0, "no bucket over the limit");
        }
    }
}

/// The corpus through the `similar_to` loop as it was before issue #126, one candidate after
/// the other.
async fn write_old(b: &dyn Backend) {
    for i in 0..DOCS {
        record(b, i).await;
        if sketch_only(i) {
            continue;
        }
        let blake3 = hash(i);
        let doc = b.upsert_location(&blake3, &at(i)).await.unwrap().document;
        let (sig, bands) = facts(i).minhash.unwrap();
        b.put_sketch(&blake3, &sig, &bands).await.unwrap();
        for c in b
            .sketch_candidates(&bands, &blake3, SimilarityConfig::default().max_candidates)
            .await
            .unwrap()
        {
            let score = quail_sketch::jaccard_estimate(&sig, &c.signature);
            if score < MIN_JACCARD {
                continue;
            }
            let hex = c.blake3.to_hex();
            let other = match b.node_by_key(NodeKind::Document, &hex).await.unwrap() {
                Some(n) => n.id,
                None => b.ensure_node(NodeKind::Document, &hex).await.unwrap(),
            };
            let (src, dst) = if blake3 < c.blake3 {
                (doc, other)
            } else {
                (other, doc)
            };
            #[allow(clippy::cast_possible_truncation)]
            let edge = Edge::new(src, dst, EdgeKind::SimilarTo, EdgeSource::Document)
                .with_props(json!({"score": score, "method": "minhash_lsh", "shared_bands": c.shared_bands}))
                .with_confidence(score.clamp(0.0, 1.0) as f32);
            b.upsert_edge(&edge).await.unwrap();
        }
    }
}

/// Writes the corpus both ways and compares the edges; there must be many, through
/// sketch-only documents too.
async fn compare(new: &Arc<dyn Backend>, old: &dyn Backend) {
    write_new(new).await;
    write_old(old).await;
    let hashes: Vec<Blake3> = (0..DOCS).map(hash).collect();
    let got = common::similar_edges(&**new, &hashes).await;
    let want = common::similar_edges(old, &hashes).await;
    assert!(got.len() > 5_000, "{} edges", got.len());
    let via_sketch_only = (0..DOCS)
        .filter(|i| sketch_only(*i))
        .map(|i| hash(i).to_hex())
        .filter(|h| got.iter().any(|e| &e.0 == h || &e.1 == h))
        .count();
    assert!(via_sketch_only > 10, "{via_sketch_only}");
    assert_eq!(got.len(), want.len());
    assert!(got == want, "the edges differ");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn similar_edges_match_the_loop_before_on_sqlite() {
    let store = |n: u8| {
        let s = Store::open_in_memory(&MasterKey::from_bytes([n; 32])).unwrap();
        Arc::new(Standalone::new(Arc::new(s))) as Arc<dyn Backend>
    };
    let (new, old) = (store(1), store(2));
    compare(&new, &*old).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn similar_edges_match_the_loop_before_on_postgres() {
    let Some(url) = std::env::var(PG_VAR).ok().filter(|v| !v.trim().is_empty()) else {
        eprintln!("skipping: {PG_VAR} is not set; the Postgres variant did not run");
        return;
    };
    let (admin, conn) = tokio_postgres::connect(&url, tokio_postgres::NoTls)
        .await
        .expect("connect to the test database");
    tokio::spawn(conn);
    let sep = if url.contains('?') { '&' } else { '?' };
    let mut stores = Vec::new();
    let mut schemas = Vec::new();
    for _ in 0..2 {
        let schema = format!("dw_similar_{}", uuid::Uuid::now_v7().simple());
        admin
            .batch_execute(&format!("CREATE SCHEMA {schema}"))
            .await
            .unwrap();
        let scoped = format!("{url}{sep}options=-c%20search_path%3D{schema}");
        let pg = doewood_pg::PgStore::connect(&doewood_core::PostgresUrl::parse(&scoped).unwrap())
            .await
            .unwrap();
        stores.push(Arc::new(pg) as Arc<dyn Backend>);
        schemas.push(schema);
    }
    compare(&stores[0], &*stores[1]).await;
    for schema in schemas {
        admin
            .batch_execute(&format!("DROP SCHEMA {schema} CASCADE"))
            .await
            .unwrap();
    }
}
