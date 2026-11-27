//! Typed similarity (issue #169) through the graph writer: generated documents that hit each
//! relation (identical, near copy, related version, same template by heading skeleton and by
//! template property, same origin by `w:rsidRoot` and by XMP id), and pairs that hit none (a
//! shared heading skeleton with a distant masked `SimHash`, an identical text below the
//! minimum length). On SQLite and, when `DOEWOOD_TEST_POSTGRES_URL` is set, on Postgres in a
//! schema of its own.

mod common;

use std::sync::Arc;

use doewood_core::MasterKey;
use doewood_ingest::facts::{DocFacts, SimilarityFacts};
use doewood_ingest::{GraphWriter, Similar, SimilarityConfig};
use doewood_model::similar::edge_relations;
use doewood_model::{
    Backend, Blake3, Direction, EdgeKind, NeighbourQuery, NodeKind, ObjectLocation, SimilarKeyKind,
    SimilarRelation as R, StepVersions,
};
use doewood_store::{Standalone, Store};
use serde_json::{Value, json};

const PG_VAR: &str = "DOEWOOD_TEST_POSTGRES_URL";
const MASKED: u64 = 0x0f0f_1234_5678_9abc;
const HEADINGS: u64 = 0xabcd;
const RSID: u64 = 0x00a1_b2c3;

fn hash(i: u64) -> Blake3 {
    Blake3::from_slice(blake3::hash(format!("typed/{i}").as_bytes()).as_bytes()).unwrap()
}

fn at(i: u64) -> ObjectLocation {
    ObjectLocation {
        endpoint: "http://objects.test:9000".into(),
        bucket: "docs".into(),
        key: format!("typed/{i}.txt"),
    }
}

/// `base` with every `every`-th word replaced.
fn variant(base: &str, every: usize) -> String {
    base.split(' ')
        .enumerate()
        .map(|(i, w)| {
            if i % every == every - 1 {
                format!("changed{i}")
            } else {
                w.to_owned()
            }
        })
        .collect::<Vec<_>>()
        .join(" ")
}

fn jaccard(a: &str, b: &str) -> f64 {
    quail_sketch::jaccard_estimate(&common::sketch(a).unwrap().0, &common::sketch(b).unwrap().0)
}

/// The first variant of `base` whose Jaccard estimate with it is in `range`.
fn variant_in(base: &str, range: std::ops::Range<f64>) -> String {
    (2..200)
        .rev()
        .map(|every| variant(base, every))
        .find(|v| range.contains(&jaccard(base, v)))
        .expect("a variant in range")
}

fn doc(text: Option<&str>, similarity: SimilarityFacts) -> DocFacts {
    DocFacts {
        format: "txt".into(),
        minhash: text.and_then(common::sketch),
        similarity,
        ..DocFacts::default()
    }
}

/// The corpus: document `i` is `docs[i - 1]`.
fn corpus() -> Vec<DocFacts> {
    let base = common::prose(42, 400);
    let near = variant_in(&base, 0.9..0.99);
    let related = variant_in(&base, 0.6..0.85);
    let short = common::prose(55, 200);
    let long = |key| SimilarityFacts {
        text_key: Some(key),
        text_chars: Some(3000),
        ..SimilarityFacts::default()
    };
    let shaped = |masked, headings| SimilarityFacts {
        masked: Some(masked),
        headings,
        ..SimilarityFacts::default()
    };
    vec![
        // 1: the original: text key 1, masked value, heading skeleton, rsid root.
        doc(
            Some(&base),
            SimilarityFacts {
                masked: Some(MASKED),
                headings: Some(HEADINGS),
                rsid_root: Some(RSID),
                ..long(1)
            },
        ),
        // 2: identical to 1 (same text, layout and template; another rsid root).
        doc(
            Some(&base),
            SimilarityFacts {
                masked: Some(MASKED),
                headings: Some(HEADINGS),
                ..long(1)
            },
        ),
        // 3: a near copy of 1; 4: a related version.
        doc(Some(&near), long(3)),
        doc(Some(&related), long(4)),
        // 5: another text from the same template (3 masked bits apart, same headings).
        doc(
            Some(&common::prose(77, 400)),
            shaped(MASKED ^ 0b111, Some(HEADINGS)),
        ),
        // 6: another text from the same original file.
        doc(
            Some(&common::prose(88, 400)),
            SimilarityFacts {
                rsid_root: Some(RSID),
                ..SimilarityFacts::default()
            },
        ),
        // 7: the same heading skeleton, but a distant masked value: no relation.
        doc(
            Some(&common::prose(99, 400)),
            shaped(MASKED ^ 0xffff_ffff, Some(HEADINGS)),
        ),
        // 8 and 9: the same short text: too short to be identical, a near copy.
        doc(
            Some(&short),
            SimilarityFacts {
                text_key: Some(8),
                text_chars: Some(500),
                ..SimilarityFacts::default()
            },
        ),
        doc(
            Some(&short),
            SimilarityFacts {
                text_key: Some(8),
                text_chars: Some(500),
                ..SimilarityFacts::default()
            },
        ),
        // 10 and 11: no text, one XMP id (an original's DocumentID, its copy's
        // OriginalDocumentID).
        doc(
            None,
            SimilarityFacts {
                origins: vec![(SimilarKeyKind::OriginXmp, "uuid:5d1c".into())],
                ..SimilarityFacts::default()
            },
        ),
        doc(
            None,
            SimilarityFacts {
                origins: vec![
                    (SimilarKeyKind::OriginXmp, "uuid:5d1c".into()),
                    (SimilarKeyKind::OriginXmp, "xmp.did:77".into()),
                ],
                ..SimilarityFacts::default()
            },
        ),
        // 12 and 13: two texts made from one Word template.
        doc(
            Some(&common::prose(120, 400)),
            SimilarityFacts {
                templates: vec!["invoice.dotx".into()],
                ..shaped(MASKED, None)
            },
        ),
        doc(
            Some(&common::prose(130, 400)),
            SimilarityFacts {
                templates: vec!["invoice.dotx".into()],
                ..shaped(MASKED ^ 1, None)
            },
        ),
    ]
}

async fn write_all(b: &Arc<dyn Backend>, docs: &[DocFacts]) -> usize {
    let writer = GraphWriter::new(Arc::clone(b), SimilarityConfig::default());
    let settings = Similar::from(doewood_store::IngestSettings::default());
    let mut edges = 0;
    for (i, facts) in (1..).zip(docs) {
        let body = doewood_model::canonical::to_canonical(&json!({})).unwrap();
        b.put_record(&hash(i), &body, &StepVersions::new())
            .await
            .unwrap();
        let stats = writer
            .write(&hash(i), &at(i), facts, "x", settings)
            .await
            .unwrap();
        edges += stats.similar;
    }
    edges
}

/// The props of the live `similar_to` edge between `x` and `y`, if any.
async fn props(b: &dyn Backend, x: u64, y: u64) -> Option<Value> {
    let node = b
        .node_by_key(NodeKind::Document, &hash(x).to_hex())
        .await
        .unwrap()?;
    let other = hash(y).to_hex();
    let q = NeighbourQuery {
        direction: Direction::Both,
        edge_kinds: vec![EdgeKind::SimilarTo],
        ..NeighbourQuery::default()
    };
    b.neighbours(node.id, &q)
        .await
        .unwrap()
        .into_iter()
        .find(|n| n.node.key == other)
        .map(|n| n.edge.edge.props)
}

async fn relations(b: &dyn Backend, x: u64, y: u64) -> Vec<R> {
    match props(b, x, y).await {
        Some(p) => edge_relations(&p, 0.9)
            .into_iter()
            .map(|r| r.relation)
            .collect(),
        None => Vec::new(),
    }
}

async fn check(b: &Arc<dyn Backend>) {
    let written = write_all(b, &corpus()).await;
    let b = &**b;
    assert_eq!(
        relations(b, 1, 2).await,
        [R::Identical, R::SameTemplate],
        "{}",
        b.name()
    );
    assert_eq!(relations(b, 1, 3).await, [R::NearCopy]);
    assert_eq!(relations(b, 1, 4).await, [R::RelatedVersion]);
    assert_eq!(relations(b, 2, 3).await, [R::NearCopy]);
    assert_eq!(relations(b, 1, 5).await, [R::SameTemplate]);
    assert_eq!(relations(b, 2, 5).await, [R::SameTemplate]);
    assert_eq!(relations(b, 1, 6).await, [R::SameOrigin]);
    assert_eq!(relations(b, 8, 9).await, [R::NearCopy], "too short");
    assert_eq!(relations(b, 10, 11).await, [R::SameOrigin]);
    assert_eq!(relations(b, 12, 13).await, [R::SameTemplate]);
    for (x, y) in [(1, 7), (5, 7), (2, 6), (1, 12), (5, 12), (6, 10)] {
        assert!(relations(b, x, y).await.is_empty(), "{x} and {y}");
    }
    // What the props say about each relation.
    let p = props(b, 1, 2).await.unwrap();
    assert_eq!(
        (&p["method"], &p["chars"], &p["template"]),
        (&json!("text_key"), &json!(3000), &json!("headings"))
    );
    assert_eq!(p["raw_similarity"], 1.0);
    assert_eq!(p["masked_similarity"], 1.0);
    let p = props(b, 1, 5).await.unwrap();
    assert_eq!(p["method"], "masked_simhash");
    assert!(p["masked_similarity"].as_f64().unwrap() > 0.98);
    assert!(p["raw_similarity"].as_f64().unwrap() < 0.6);
    let p = props(b, 1, 6).await.unwrap();
    assert_eq!(
        (&p["method"], &p["origin"]),
        (&json!("origin_id"), &json!("rsid_root"))
    );
    let p = props(b, 10, 11).await.unwrap();
    assert_eq!(p["origin"], "xmp");
    assert!(p.get("score").is_none(), "no signatures, no score");
    assert_eq!(
        props(b, 12, 13).await.unwrap()["template"],
        "template_property"
    );
    let p = props(b, 1, 3).await.unwrap();
    assert_eq!(p["method"], "minhash_lsh");
    assert_eq!(p["score"], p["raw_similarity"]);
    assert!(p["shared_bands"].as_u64().unwrap() > 0);
    assert!(written >= 10, "{written} edges written");
}

async fn check_twice(b: &Arc<dyn Backend>) {
    check(b).await;
    // Written again (a reprocess): the same edges.
    let hashes: Vec<Blake3> = (1..=13).map(hash).collect();
    let before = common::similar_edges(&**b, &hashes).await;
    write_all(b, &corpus()).await;
    assert_eq!(common::similar_edges(&**b, &hashes).await, before);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn each_relation_on_sqlite() {
    let s = Store::open_in_memory(&MasterKey::from_bytes([3; 32])).unwrap();
    let b = Arc::new(Standalone::new(Arc::new(s))) as Arc<dyn Backend>;
    check_twice(&b).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn each_relation_on_postgres() {
    let Some(url) = std::env::var(PG_VAR).ok().filter(|v| !v.trim().is_empty()) else {
        eprintln!("skipping: {PG_VAR} is not set; the Postgres variant did not run");
        return;
    };
    let (admin, conn) = tokio_postgres::connect(&url, tokio_postgres::NoTls)
        .await
        .expect("connect to the test database");
    tokio::spawn(conn);
    let schema = format!("dw_typed_{}", uuid::Uuid::now_v7().simple());
    admin
        .batch_execute(&format!("CREATE SCHEMA {schema}"))
        .await
        .unwrap();
    let sep = if url.contains('?') { '&' } else { '?' };
    let scoped = format!("{url}{sep}options=-c%20search_path%3D{schema}");
    let pg = doewood_pg::PgStore::connect(&doewood_core::PostgresUrl::parse(&scoped).unwrap())
        .await
        .unwrap();
    check_twice(&(Arc::new(pg) as Arc<dyn Backend>)).await;
    admin
        .batch_execute(&format!("DROP SCHEMA {schema} CASCADE"))
        .await
        .unwrap();
}
