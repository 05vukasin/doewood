//! Query time of similar documents (issue #169), ignored: `Neighbours` over `similar_to` (how a
//! client read them before issue #169) against `SimilarDocuments`, through a real API server,
//! on SQLite and, with `DOEWOOD_TEST_POSTGRES_URL`, Postgres.
//!
//! One document per size in `DOEWOOD_BENCH_SIMILAR` (default `20,100,1000,5000` edges), its
//! `similar_to` edges both ways: with the props of issue #169 (a near copy, a related version
//! and a template in turn, about a fifth the same template), and the same count with the props
//! from before it (`score`, `method`, `shared_bands`). Each call is timed 50 times after 5 warm
//! ups; the median and p95 are printed, for a first page of 50 and of 1000.
//!
//! `cargo test --release -p doewood-api --test similar_bench -- --ignored --nocapture`

#![allow(clippy::cast_precision_loss, clippy::too_many_lines)]

mod common;

use std::time::{Duration, Instant};

use common::{Instance, Shared};
use doewood_api::pb;
use doewood_api::pb::graph_service_client::GraphServiceClient;
use doewood_model::{
    Blake3, Edge, EdgeKind, EdgeSource, NodeKind, SimilarEdge, SimilarKeyKind, SimilarRelation,
};
use serde_json::json;
use tonic::transport::Channel;

/// A made-up hash, distinct per tag and number.
fn hash(tag: &str, n: usize) -> Blake3 {
    let mut b = [0u8; 32];
    for (i, c) in tag.bytes().take(16).enumerate() {
        b[i] = c;
    }
    b[24..].copy_from_slice(&(n as u64).to_be_bytes());
    Blake3(b)
}

fn new_props(i: usize) -> serde_json::Value {
    let raw = 0.6 + (i % 40) as f64 / 100.0;
    let mut e = SimilarEdge {
        raw_similarity: Some(raw),
        shared_bands: Some(u32::try_from(i % 32).unwrap()),
        ..SimilarEdge::default()
    };
    e.relations.push(if raw >= 0.9 {
        SimilarRelation::NearCopy
    } else {
        SimilarRelation::RelatedVersion
    });
    if i % 5 == 0 {
        e.relations.push(SimilarRelation::SameTemplate);
        e.masked_similarity = Some(0.9);
        e.template = Some(SimilarKeyKind::Headings);
    }
    e.to_props()
}

fn old_props(i: usize) -> serde_json::Value {
    let raw = 0.8 + (i % 20) as f64 / 100.0;
    json!({"score": raw, "method": "minhash_lsh", "shared_bands": i % 32})
}

/// `edges` documents linked to a new one with `props`, half of the edges pointing at it.
async fn seed(
    b: &dyn doewood_model::Backend,
    tag: &str,
    edges: usize,
    props: fn(usize) -> serde_json::Value,
) -> Blake3 {
    let me = hash(tag, usize::MAX);
    let id = b
        .upsert_node(NodeKind::Document, &me.to_hex(), &json!({"title": "probe"}))
        .await
        .unwrap();
    let mut batch = Vec::new();
    for i in 0..edges {
        let other = b
            .upsert_node(
                NodeKind::Document,
                &hash(tag, i).to_hex(),
                &json!({"title": format!("document {i}"), "format": "pdf", "kind": "invoice"}),
            )
            .await
            .unwrap();
        let (src, dst) = if i % 2 == 0 { (id, other) } else { (other, id) };
        batch.push(
            Edge::new(src, dst, EdgeKind::SimilarTo, EdgeSource::Document)
                .with_props(props(i))
                .with_confidence(0.8),
        );
        if batch.len() == 500 {
            b.upsert_edges(&batch).await.unwrap();
            batch.clear();
        }
    }
    b.upsert_edges(&batch).await.unwrap();
    me
}

/// Median and p95 of `f`, in ms.
async fn time<F, Fut>(mut f: F) -> (f64, f64)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = ()>,
{
    for _ in 0..5 {
        f().await;
    }
    let mut t: Vec<Duration> = Vec::new();
    for _ in 0..50 {
        let s = Instant::now();
        f().await;
        t.push(s.elapsed());
    }
    t.sort();
    let ms = |d: Duration| d.as_secs_f64() * 1000.0;
    (ms(t[25]), ms(t[47]))
}

async fn neighbours(ch: &Channel, node: &str, page: u32) {
    GraphServiceClient::new(ch.clone())
        .neighbours(pb::NeighboursRequest {
            node_id: node.to_owned(),
            edge_kinds: vec![pb::EdgeKind::SimilarTo as i32],
            page_size: page,
            ..pb::NeighboursRequest::default()
        })
        .await
        .unwrap();
}

async fn similar(ch: &Channel, blake3: &Blake3, limit: u32) {
    GraphServiceClient::new(ch.clone())
        .similar_documents(pb::SimilarDocumentsRequest {
            blake3: blake3.to_hex(),
            limit,
            include_crowded: true,
            ..pb::SimilarDocumentsRequest::default()
        })
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "a measurement (issue #169)"]
async fn similar_documents_query_time() {
    let sizes: Vec<usize> = std::env::var("DOEWOOD_BENCH_SIMILAR")
        .ok()
        .map(|v| v.split(',').filter_map(|n| n.trim().parse().ok()).collect())
        .unwrap_or_else(|| vec![20, 100, 1000, 5000]);
    println!("backend | edges | page | Neighbours, old props | SimilarDocuments, old props | SimilarDocuments, new props (median / p95 ms)");
    for shared in Shared::all().await {
        let b = &shared.backend;
        let mut seeded = Vec::new();
        for &n in &sizes {
            let old = seed(&**b, &format!("old{n}"), n, old_props).await;
            let new = seed(&**b, &format!("new{n}"), n, new_props).await;
            seeded.push((n, old, new));
        }
        shared.analyze().await;
        let a = Instance::start("inst-bench", Some(b.clone())).await;
        let ch = a.channel().await;
        for (n, old, new) in &seeded {
            let old_id = b
                .node_by_key(NodeKind::Document, &old.to_hex())
                .await
                .unwrap()
                .unwrap()
                .id
                .to_string();
            for page in [50u32, 1000] {
                let nb = time(|| neighbours(&ch, &old_id, page)).await;
                let so = time(|| similar(&ch, old, page)).await;
                let sn = time(|| similar(&ch, new, page)).await;
                println!(
                    "{} | {n} | {page} | {:.2} / {:.2} | {:.2} / {:.2} | {:.2} / {:.2}",
                    shared.name, nb.0, nb.1, so.0, so.1, sn.0, sn.1
                );
            }
        }
        a.stop().await;
        shared.drop_schema().await;
    }
}
