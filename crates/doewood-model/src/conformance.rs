//! The conformance suite: one generic test every backend runs, so the shared and standalone
//! modes behave the same.
//!
//! Call [`run`] from a backend's tests with two handles to the **same, empty** store. They
//! stand for two instances: claims and leases taken through one are seen by the other.

use std::collections::BTreeSet;
use std::future::Future;
use std::task::Poll;
use std::time::Duration;

use doewood_core::InstanceId;
use serde_json::json;
use serde_json::value::RawValue;
use time::{Date, Month};
use uuid::Uuid;

use crate::{
    Backend, Blake3, CACHE_KEY_MAX_LEN, CACHE_MAX_BODY, CacheRow, Direction, DismissRequest,
    DismissalQuery, DueCompany, DueReason, Edge, EdgeCursor, EdgeKind, EdgeSource, EdgeState,
    Error, IconRow, MergeActor, MergeActorKind, MergeOp, MergeQuery, MergeRequest, Neighbour,
    NeighbourQuery, Node, NodeKind, ObjectLocation, OwnedAlias, ProposalKind, ProposalQuery,
    ProposalReason, ProposalRequest, ProposalState, PropsPatch, PutOutcome, ResolveRequest,
    SearchDoc, SearchQuery, SourceClaim, SourceKey, SplitRequest, StepVersions, StoredEdge,
    StoredIcon, UndoRequest, merged_into,
};
use crate::{BlockKind, TEXT_MAX_BYTES, TextBlock, TextBuilder, TextOrigin, TextZone};

/// Runs every check.
///
/// # Panics
///
/// When the backend does not behave as the traits say.
pub async fn run<B: Backend + ?Sized>(a: &B, b: &B) {
    records(a).await;
    graph(a).await;
    graph_pages(a).await;
    props_patches(a, b).await;
    dated_edges(a).await;
    locations(a).await;
    ended_locations(a).await;
    records_by_source(a).await;
    sketches(a).await;
    stop_bands(a).await;
    aliases(a).await;
    alias_batches(a).await;
    graph_batches(a).await;
    claims(a, b).await;
    leases(a, b).await;
    search(a).await;
    caches(a, b).await;
    freshness(a, b).await;
    feeds(a, b).await;
    merges(a).await;
    proposals(a, b).await;
    decisions(a).await;
    live_edges(a, b).await;
    live_writes(a, b).await;
    explorer(a).await;
    purge_overlaps(a, b).await;
    purge_races(a, b).await;
    // Last: it purges every orphan the cases before left.
    purge(a).await;
    a.ping().await.expect("ping");
}

fn day(y: i32, m: u8, d: u8) -> Date {
    Date::from_calendar_date(y, Month::try_from(m).expect("month"), d).expect("date")
}

fn hash(n: u8) -> Blake3 {
    Blake3([n; 32])
}

fn instance(name: &str) -> InstanceId {
    InstanceId::new(name).expect("instance id")
}

/// A record body as the stores take it.
fn raw(v: &serde_json::Value) -> Box<RawValue> {
    crate::canonical::to_canonical(v).expect("JSON")
}

fn versions(quail: &str) -> StepVersions {
    StepVersions::from([("quail".to_owned(), quail.to_owned())])
}

/// Records: insert, unchanged repeat, update, read back.
///
/// # Panics
///
/// When the backend misbehaves.
pub async fn records<B: Backend + ?Sized>(s: &B) {
    let h = hash(1);
    let body = json!({"kind": "invoice", "facts": {"amount": 12.5, "parties": ["ACME"]}});
    assert_eq!(s.get_record(&h).await.unwrap(), None);
    assert_eq!(
        s.put_record(&h, &raw(&body), &versions("1.0"))
            .await
            .unwrap(),
        PutOutcome::Inserted
    );
    assert_eq!(
        s.put_record(&h, &raw(&body), &versions("1.0"))
            .await
            .unwrap(),
        PutOutcome::Unchanged
    );
    let got = s.get_record(&h).await.unwrap().expect("stored");
    assert_eq!(got.blake3, h);
    assert_eq!(got.body_value().unwrap(), body);
    // Stored as written: the compact, key-sorted text.
    assert_eq!(got.body.get(), body.to_string());
    assert_eq!(got.versions, versions("1.0"));
    assert!(got.updated_at > 1_700_000_000, "{}", got.updated_at);
    // A new step version alone is a change.
    assert_eq!(
        s.put_record(&h, &raw(&body), &versions("1.1"))
            .await
            .unwrap(),
        PutOutcome::Updated
    );
    let body2 = json!({"kind": "invoice", "facts": {}});
    assert_eq!(
        s.put_record(&h, &raw(&body2), &versions("1.1"))
            .await
            .unwrap(),
        PutOutcome::Updated
    );
    let got = s.get_record(&h).await.unwrap().expect("stored");
    assert_eq!(
        (got.body_value().unwrap(), got.versions),
        (body2, versions("1.1"))
    );
    record_versions(s).await;
    fact_texts(s).await;
    document_texts(s).await;
}

/// Document texts (issue #166): stored beside a record, returned byte for byte, replaced,
/// removed; a text without its record is refused.
async fn document_texts<B: Backend + ?Sized>(s: &B) {
    let h = hash(166);
    assert!(s.get_document_text(&h).await.unwrap().is_none());
    let mut b = TextBuilder::new("docx.extract", "2", TEXT_MAX_BYTES);
    assert!(b.page(0, None, TextOrigin::Native));
    for i in 0..2000 {
        let block = TextBlock {
            kind: if i % 50 == 0 {
                BlockKind::Heading
            } else {
                BlockKind::Paragraph
            },
            text: format!("Clause {i}: \"Ünïcødé\" text, tab\there, 日本語 😀, line\u{2028}end"),
            zone: Some(TextZone::Body),
            level: (i % 50 == 0).then_some(1),
            table: None,
            row: None,
        };
        assert!(b.block(block));
    }
    let text = b.finish();
    let body = text.to_body().unwrap();
    assert!(matches!(
        s.put_document_text(&h, &body).await,
        Err(Error::NotFound(_))
    ));
    s.put_record(&h, &raw(&json!({"kind": "contract"})), &versions("1"))
        .await
        .unwrap();
    s.put_document_text(&h, &body).await.unwrap();
    let got = s.get_document_text(&h).await.unwrap().expect("stored");
    assert_eq!((got.blake3, got.body.get()), (h, body.get()));
    assert!(got.updated_at > 1_700_000_000, "{}", got.updated_at);
    assert_eq!(crate::DocumentText::from_body(&got.body).unwrap(), text);
    // The same text again changes nothing.
    s.put_document_text(&h, &body).await.unwrap();
    let again = s.get_document_text(&h).await.unwrap().expect("stored");
    assert_eq!(
        (again.body.get(), again.updated_at),
        (body.get(), got.updated_at)
    );
    let mut other = TextBuilder::new("docx.extract", "3", TEXT_MAX_BYTES);
    other.page(0, None, TextOrigin::Native);
    other.block(TextBlock {
        kind: BlockKind::Paragraph,
        text: "short".to_owned(),
        zone: Some(TextZone::Body),
        level: None,
        table: None,
        row: None,
    });
    let other = other.finish().to_body().unwrap();
    s.put_document_text(&h, &other).await.unwrap();
    let got = s.get_document_text(&h).await.unwrap().expect("stored");
    assert_eq!(got.body.get(), other.get());
    s.delete_document_text(&h).await.unwrap();
    assert!(s.get_document_text(&h).await.unwrap().is_none());
    s.delete_document_text(&h).await.unwrap();
}

/// Versions of many records in one call (issue #75): missing hashes are left out.
async fn record_versions<B: Backend + ?Sized>(s: &B) {
    let (a, b, missing) = (hash(2), hash(3), hash(4));
    let two = StepVersions::from([
        ("quail".to_owned(), "2.0".to_owned()),
        ("hedgehog".to_owned(), "0.3".to_owned()),
    ]);
    s.put_record(&a, &raw(&json!({"n": 2})), &versions("1.0"))
        .await
        .unwrap();
    s.put_record(&b, &raw(&json!({"n": 3})), &two)
        .await
        .unwrap();
    let got = s.record_versions(&[a, missing, b, a]).await.unwrap();
    assert_eq!(
        got.into_iter().collect::<Vec<_>>(),
        [(a, versions("1.0")), (b, two)]
    );
    assert!(s.record_versions(&[]).await.unwrap().is_empty());
    assert!(s.record_versions(&[missing]).await.unwrap().is_empty());
    let too_many = vec![a; crate::RECORDS_MAX_LIMIT as usize + 1];
    assert!(matches!(
        s.record_versions(&too_many).await,
        Err(Error::Invalid(_))
    ));
}

/// The text values of one fact key in many records (issue #75), the same values a reader of
/// the full bodies finds: only string values of matching facts in a `facts` array.
async fn fact_texts<B: Backend + ?Sized>(s: &B) {
    let key = "signal.email_domain";
    let (many, one, flat, blob, missing) = (hash(5), hash(6), hash(7), hash(8), hash(9));
    let fact = |k: &str, v: serde_json::Value| json!({"key": k, "value": v, "conf": 0.9});
    s.put_record(
        &many,
        &raw(&json!({"facts": [
            fact(key, json!({"t": "text", "v": "Acme.example"})),
            fact(key, json!({"t": "text", "v": "acme.example"})),
            fact("signal.phone", json!({"t": "text", "v": "+381"})),
            fact(key, json!({"t": "number", "v": 12})),
            fact(key, json!("bare")),
            "not an object",
            {"value": {"v": "no key"}},
            {"key": 7, "value": {"v": "number key"}},
        ]})),
        &versions("1"),
    )
    .await
    .unwrap();
    s.put_record(
        &one,
        &raw(&json!({"facts": [fact(key, json!({"t": "text", "v": " b.example "}))]})),
        &versions("1"),
    )
    .await
    .unwrap();
    // `facts` that is not an array has no facts.
    s.put_record(
        &flat,
        &raw(&json!({"facts": fact(key, json!({"v": "c.example"}))})),
        &versions("1"),
    )
    .await
    .unwrap();
    s.put_record(&blob, &raw(&json!({"kind": "blob"})), &versions("1"))
        .await
        .unwrap();
    let mut got = s
        .fact_texts(&[many, one, flat, blob, missing], key)
        .await
        .unwrap();
    got.sort();
    assert_eq!(
        got,
        [
            (many, "Acme.example".to_owned()),
            (many, "acme.example".to_owned()),
            (one, " b.example ".to_owned()),
        ]
    );
    assert_eq!(
        s.fact_texts(&[many], "signal.phone").await.unwrap(),
        [(many, "+381".to_owned())]
    );
    assert!(s.fact_texts(&[], key).await.unwrap().is_empty());
    assert!(s.fact_texts(&[many], "signal").await.unwrap().is_empty());
    assert!(matches!(
        s.fact_texts(&vec![many; crate::RECORDS_MAX_LIMIT as usize + 1], key)
            .await,
        Err(Error::Invalid(_))
    ));
}

/// Nodes and edges: idempotent upserts, lookups, party edges and company → documents.
///
/// # Panics
///
/// When the backend misbehaves.
#[allow(clippy::too_many_lines)]
pub async fn graph<B: Backend + ?Sized>(s: &B) {
    let acme = s
        .upsert_node(NodeKind::Company, "acme", &json!({"name": "ACME d.o.o."}))
        .await
        .unwrap();
    let again = s
        .upsert_node(
            NodeKind::Company,
            "acme",
            &json!({"name": "ACME", "vat": "RS1"}),
        )
        .await
        .unwrap();
    assert_eq!(acme, again, "the id is stable");
    let node = s.node(acme).await.unwrap().expect("node");
    assert_eq!(node.kind, NodeKind::Company);
    assert_eq!(node.key, "acme");
    assert_eq!(
        node.props,
        json!({"name": "ACME", "vat": "RS1"}),
        "props replaced"
    );
    assert_eq!(
        s.node_by_key(NodeKind::Company, "acme").await.unwrap(),
        Some(node)
    );
    // Keys are unique per kind, not globally.
    let loc = s
        .upsert_node(NodeKind::Location, "acme", &json!({}))
        .await
        .unwrap();
    assert_ne!(loc, acme);
    assert_eq!(s.node(Uuid::now_v7()).await.unwrap(), None);
    assert!(matches!(
        s.upsert_node(NodeKind::Document, "not-a-hash", &json!({}))
            .await,
        Err(Error::Invalid(_))
    ));

    let d1 = s
        .upsert_node(NodeKind::Document, &hash(11).to_hex(), &json!({}))
        .await
        .unwrap();
    let d2 = s
        .upsert_node(NodeKind::Document, &hash(12).to_hex(), &json!({}))
        .await
        .unwrap();
    let buyer = Edge::new(d1, acme, EdgeKind::Party, EdgeSource::Document)
        .with_props(json!({"role": "buyer"}))
        .with_evidence(json!([{"page": 1, "span": [10, 14]}]))
        .with_confidence(0.7);
    let e1 = s.upsert_edge(&buyer).await.unwrap();
    let e1b = s
        .upsert_edge(&buyer.clone().with_confidence(0.9))
        .await
        .unwrap();
    assert_eq!(e1, e1b, "same identity, same edge");
    let supplier = Edge::new(d2, acme, EdgeKind::Party, EdgeSource::Document)
        .with_props(json!({"role": "supplier"}));
    let e2 = s.upsert_edge(&supplier).await.unwrap();
    assert_ne!(e1, e2);

    let out = s
        .neighbours(
            d1,
            &NeighbourQuery {
                direction: Direction::Out,
                node_kind: Some(NodeKind::Company),
                ..NeighbourQuery::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0].node.id, acme);
    assert_eq!(out[0].edge.id, e1);
    assert!((out[0].edge.edge.confidence - 0.9).abs() < 1e-6, "updated");
    assert_eq!(out[0].edge.edge.props, json!({"role": "buyer"}));
    assert_eq!(
        out[0].edge.edge.evidence,
        json!([{"page": 1, "span": [10, 14]}])
    );
    // The node-kind filter applies to the other end.
    let no_docs = s
        .neighbours(
            d1,
            &NeighbourQuery {
                node_kind: Some(NodeKind::Document),
                ..NeighbourQuery::default()
            },
        )
        .await
        .unwrap();
    assert!(no_docs.is_empty());
    let inbound = s
        .neighbours(
            acme,
            &NeighbourQuery {
                direction: Direction::In,
                edge_kinds: vec![EdgeKind::Party],
                ..NeighbourQuery::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(
        inbound.iter().map(|n| n.node.id).collect::<BTreeSet<_>>(),
        BTreeSet::from([d1, d2])
    );

    let docs = s.company_documents(acme).await.unwrap();
    // In the order of their party edges: both undated, `e1` written first (ids are UUIDv7).
    assert!(e1 < e2);
    let expected = vec![(d1, hash(11).to_hex()), (d2, hash(12).to_hex())];
    assert_eq!(
        docs.iter()
            .map(|n| (n.id, n.key.clone()))
            .collect::<Vec<_>>(),
        expected
    );
    // Two party edges from one document count it once (issue #75).
    s.upsert_edge(
        &Edge::new(d1, acme, EdgeKind::Party, EdgeSource::Document)
            .with_props(json!({"role": "payer"}))
            .valid(Some(day(2024, 1, 1)), None),
    )
    .await
    .unwrap();
    assert_eq!(s.company_documents(acme).await.unwrap(), docs);
    assert_eq!(s.company_document_count(acme).await.unwrap(), 2);
    assert_eq!(s.company_documents_first(acme, 5).await.unwrap(), docs);
    assert_eq!(s.company_documents_first(acme, 1).await.unwrap(), docs[..1]);
    assert!(s.company_documents_first(acme, 0).await.unwrap().is_empty());
    assert_eq!(s.company_document_count(d1).await.unwrap(), 0);
    assert_eq!(s.company_document_count(Uuid::now_v7()).await.unwrap(), 0);
    // A retracted party no longer counts.
    s.upsert_edge(&supplier.clone().with_state(EdgeState::Retracted))
        .await
        .unwrap();
    let docs = s.company_documents(acme).await.unwrap();
    assert_eq!(docs.iter().map(|n| n.id).collect::<Vec<_>>(), [d1]);
    assert_eq!(s.company_document_count(acme).await.unwrap(), 1);
    assert_eq!(s.company_documents_first(acme, 5).await.unwrap(), docs);

    // Edges need both nodes, and valid values.
    assert!(
        s.upsert_edge(&Edge::new(
            d1,
            Uuid::now_v7(),
            EdgeKind::Cites,
            EdgeSource::Document
        ))
        .await
        .is_err()
    );
    assert!(matches!(
        s.upsert_edge(&buyer.clone().with_confidence(2.0)).await,
        Err(Error::Invalid(_))
    ));
}

/// Runs two futures at once, each polled in turn, and waits for both: their store calls
/// are in flight together.
async fn join2<X: Future<Output = ()>, Y: Future<Output = ()>>(x: X, y: Y) {
    let (mut x, mut y) = (Box::pin(x), Box::pin(y));
    let (mut x_done, mut y_done) = (false, false);
    std::future::poll_fn(|cx| {
        if !x_done {
            x_done = x.as_mut().poll(cx).is_ready();
        }
        if !y_done {
            y_done = y.as_mut().poll(cx).is_ready();
        }
        if x_done && y_done {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    })
    .await;
}

/// Rounds each writer of [`props_patches`] runs.
const PATCH_ROUNDS: usize = 40;

/// One of the two writers of [`props_patches`]: its own keys, plus shared lists.
async fn race_writer<B: Backend + ?Sized>(s: &B, node: Uuid, who: &str, rounds: usize) {
    for i in 0..rounds {
        let patch = PropsPatch::new()
            .set(&[&format!("{who}{i}")], json!(i))
            .set(&["last", who], json!(i))
            .add_to_set(&["log"], json!(format!("{who}{i}")), 1000)
            .push_capped(&["seen"], json!({"who": who, "i": i}), &["who"], 10);
        s.update_props(node, &patch).await.unwrap().expect("node");
    }
}

/// The documented order of [`crate::GraphStore::neighbours`]: kind name, `valid_from`
/// (undated first), id.
fn neighbour_order(n: &Neighbour) -> (&'static str, Option<Date>, Uuid) {
    (n.edge.edge.kind.as_str(), n.edge.edge.valid_from, n.edge.id)
}

/// Walks [`crate::GraphStore::neighbours_page`] to its end, `limit` and `max_bytes` a page,
/// through the text of each page's last position (the API's page token).
async fn walk_neighbours<B: Backend + ?Sized>(
    s: &B,
    node: Uuid,
    q: &NeighbourQuery,
    limit: u32,
    max_bytes: usize,
) -> Vec<Neighbour> {
    let (mut all, mut after): (Vec<Neighbour>, Option<EdgeCursor>) = (Vec::new(), None);
    loop {
        let page = s
            .neighbours_page(node, q, after.as_ref(), limit, max_bytes)
            .await
            .unwrap();
        assert!(page.len() <= crate::records_limit(limit) as usize);
        let full = crate::page_full(&page, limit, max_bytes, crate::neighbour_page_bytes);
        all.extend(page);
        if !full {
            return all;
        }
        let text = EdgeCursor::of(&all.last().unwrap().edge).to_string();
        after = Some(text.parse().unwrap());
    }
}

/// Issue #112: pages of `neighbours` and `company_documents`, walked to the end, return the
/// whole list once each, in the documented order, at every page size and byte budget, for
/// every direction and filter.
///
/// # Panics
///
/// When the backend misbehaves.
#[allow(clippy::too_many_lines)]
pub async fn graph_pages<B: Backend + ?Sized>(s: &B) {
    let co = s
        .upsert_node(NodeKind::Company, "pages-co", &json!({"name": "Pages"}))
        .await
        .unwrap();
    let parent = s
        .upsert_node(NodeKind::Company, "pages-parent", &json!({}))
        .await
        .unwrap();
    let sub = s
        .upsert_node(NodeKind::Company, "pages-sub", &json!({}))
        .await
        .unwrap();
    let mut docs = Vec::new();
    for i in 0..23u32 {
        let d = s
            .upsert_node(
                NodeKind::Document,
                &format!("{:064x}", 0x1120_0000 + i),
                &json!({"n": i, "pad": "x".repeat(usize::try_from(i).unwrap() * 10)}),
            )
            .await
            .unwrap();
        // Undated, or dated on one of three days, so that the order mixes both.
        let from = match i % 4 {
            0 => None,
            k => Some(day(2020 + i32::try_from(k).unwrap(), 1, 1)),
        };
        s.upsert_edge(
            &Edge::new(d, co, EdgeKind::Party, EdgeSource::Document)
                .with_props(json!({"role": "buyer"}))
                .valid(from, None),
        )
        .await
        .unwrap();
        // Some documents have a second party edge, dated earlier or later.
        if i % 3 == 0 {
            s.upsert_edge(
                &Edge::new(d, co, EdgeKind::Party, EdgeSource::Registry)
                    .with_props(json!({"role": "supplier"}))
                    .valid(Some(day(2019 + i32::try_from(i % 5).unwrap(), 6, 1)), None),
            )
            .await
            .unwrap();
        }
        // One retracted: it does not count, its document still does through its other edge.
        if i == 9 {
            s.upsert_edge(
                &Edge::new(d, co, EdgeKind::Party, EdgeSource::Document)
                    .with_props(json!({"role": "buyer"}))
                    .valid(from, None)
                    .with_state(EdgeState::Retracted),
            )
            .await
            .unwrap();
        }
        docs.push(d);
    }
    // Edges of other kinds, both ways, and a document that cites another.
    for (src, dst, kind) in [
        (parent, co, EdgeKind::ParentOf),
        (co, sub, EdgeKind::ParentOf),
        (sub, co, EdgeKind::SubsidiaryOf),
        (co, parent, EdgeKind::SubsidiaryOf),
        (docs[1], docs[0], EdgeKind::Cites),
    ] {
        s.upsert_edge(&Edge::new(src, dst, kind, EdgeSource::Registry))
            .await
            .unwrap();
    }

    let kinds_sets: [&[EdgeKind]; 4] = [
        &[],
        &[EdgeKind::Party],
        &[EdgeKind::ParentOf, EdgeKind::Party],
        &[EdgeKind::SubsidiaryOf, EdgeKind::Cites],
    ];
    for node in [co, docs[0], docs[1], parent] {
        for direction in [Direction::Out, Direction::In, Direction::Both] {
            for kinds in kinds_sets {
                for (node_kind, include_inactive) in [
                    (None, false),
                    (Some(NodeKind::Document), false),
                    (None, true),
                ] {
                    let q = NeighbourQuery {
                        direction,
                        edge_kinds: kinds.to_vec(),
                        node_kind,
                        at: None,
                        include_inactive,
                    };
                    let whole = s.neighbours(node, &q).await.unwrap();
                    let mut sorted = whole.clone();
                    sorted.sort_by(|x, y| neighbour_order(x).cmp(&neighbour_order(y)));
                    assert_eq!(whole, sorted, "documented order, {q:?}");
                    let ids: BTreeSet<Uuid> = whole.iter().map(|n| n.edge.id).collect();
                    assert_eq!(ids.len(), whole.len(), "each edge once, {q:?}");
                    for (limit, max_bytes) in [
                        (0, usize::MAX),
                        (1, usize::MAX),
                        (4, usize::MAX),
                        (1000, usize::MAX),
                        (1000, 1),
                    ] {
                        let walked = walk_neighbours(s, node, &q, limit, max_bytes).await;
                        assert_eq!(walked, whole, "{q:?}, limit {limit}, bytes {max_bytes}");
                    }
                }
            }
        }
    }
    let q = NeighbourQuery {
        direction: Direction::In,
        edge_kinds: vec![EdgeKind::Party],
        ..NeighbourQuery::default()
    };
    // 23 documents, 8 with a second party edge, one edge retracted.
    assert_eq!(s.neighbours(co, &q).await.unwrap().len(), 23 + 8 - 1);
    // A byte budget ends a page with the neighbour that reaches it.
    let page = s.neighbours_page(co, &q, None, 1000, 300).await.unwrap();
    let bytes: Vec<usize> = page.iter().map(crate::neighbour_page_bytes).collect();
    assert!(bytes.iter().sum::<usize>() >= 300);
    assert!(bytes[..bytes.len() - 1].iter().sum::<usize>() < 300);
    // A position past every edge, or of a kind not asked for, gives an empty page.
    let last = EdgeCursor {
        kind: EdgeKind::TradingNameOf,
        valid_from: None,
        id: Uuid::max(),
    };
    assert!(
        s.neighbours_page(co, &NeighbourQuery::default(), Some(&last), 10, usize::MAX)
            .await
            .unwrap()
            .is_empty()
    );

    // Company documents: each once, in the order of its first live party edge.
    let whole = s.company_documents(co).await.unwrap();
    let mut first: Vec<(Option<Date>, Uuid, Uuid)> = Vec::new();
    for n in s.neighbours(co, &q).await.unwrap() {
        if !first.iter().any(|f| f.2 == n.node.id) {
            first.push((n.edge.edge.valid_from, n.edge.id, n.node.id));
        }
    }
    assert_eq!(
        whole.iter().map(|n| n.id).collect::<Vec<_>>(),
        first.iter().map(|f| f.2).collect::<Vec<_>>()
    );
    assert_eq!(whole.len(), docs.len());
    assert_eq!(
        s.company_documents_first(co, 7).await.unwrap(),
        whole[..7].to_vec()
    );
    for (limit, max_bytes) in [
        (0, usize::MAX),
        (1, usize::MAX),
        (5, usize::MAX),
        (1000, usize::MAX),
        (1000, 1),
        (1000, 400),
    ] {
        let (mut walked, mut after): (Vec<Node>, Option<EdgeCursor>) = (Vec::new(), None);
        loop {
            let page = s
                .company_documents_page(co, after.as_ref(), limit, max_bytes)
                .await
                .unwrap();
            for d in &page {
                let f = first.iter().find(|f| f.2 == d.node.id).unwrap();
                assert_eq!((d.edge.valid_from, d.edge.id), (f.0, f.1), "its first edge");
                assert_eq!(d.edge.kind, EdgeKind::Party);
            }
            let full =
                crate::page_full(&page, limit, max_bytes, |d| crate::node_page_bytes(&d.node));
            if let Some(last) = page.last() {
                after = Some(last.edge.to_string().parse().unwrap());
            }
            walked.extend(page.into_iter().map(|d| d.node));
            if !full {
                break;
            }
        }
        assert_eq!(walked, whole, "limit {limit}, bytes {max_bytes}");
    }
    assert!(
        s.company_documents_page(Uuid::now_v7(), None, 10, usize::MAX)
            .await
            .unwrap()
            .is_empty()
    );
}

/// Partial prop updates (issue #51): `ensure_node` never replaces props, a patch changes only
/// the keys it names, and two writers patching different keys of one node at the same time,
/// through two handles, both keep every write.
///
/// # Panics
///
/// When the backend misbehaves.
#[allow(clippy::too_many_lines)]
pub async fn props_patches<B: Backend + ?Sized>(a: &B, b: &B) {
    let id = a.ensure_node(NodeKind::Company, "patched").await.unwrap();
    assert_eq!(
        a.node(id).await.unwrap().map(|n| n.props),
        Some(json!({})),
        "created with empty props"
    );
    let patch = PropsPatch::new()
        .set(&["name"], json!("ACME"))
        .set(&["ids", "lei"], json!(["L1"]))
        .set_if_absent(&["legal_form"], json!("doo"))
        .add_to_set(&["names"], json!("ACME"), 3)
        .push_capped(
            &["matches"],
            json!({"document": "d1", "v": 1}),
            &["document"],
            2,
        );
    let got = a.update_props(id, &patch).await.unwrap().expect("node");
    let want = json!({
        "name": "ACME",
        "ids": {"lei": ["L1"]},
        "legal_form": "doo",
        "names": ["ACME"],
        "matches": [{"document": "d1", "v": 1}],
    });
    assert_eq!(got.props, want);
    assert_eq!(
        (got.id, got.kind, got.key.as_str()),
        (id, NodeKind::Company, "patched")
    );
    assert_eq!(a.node(id).await.unwrap(), Some(got));
    // `ensure_node` again: same id, props untouched.
    assert_eq!(
        b.ensure_node(NodeKind::Company, "patched").await.unwrap(),
        id
    );
    assert_eq!(a.node(id).await.unwrap().map(|n| n.props), Some(want));
    // Only the named keys change.
    let patch = PropsPatch::new()
        .set_if_absent(&["legal_form"], json!("ad"))
        .remove(&["name"])
        .remove(&["ids", "lei"])
        .add_to_set(&["names"], json!("ACME"), 3)
        .add_to_set(&["names"], json!("ACME Trading"), 3)
        .push_capped(&["matches"], json!({"document": "d2"}), &["document"], 2)
        .push_capped(
            &["matches"],
            json!({"document": "d1", "v": 2}),
            &["document"],
            2,
        )
        .push_capped(&["matches"], json!({"document": "d3"}), &["document"], 2);
    let got = b.update_props(id, &patch).await.unwrap().expect("node");
    assert_eq!(
        got.props,
        json!({
            "ids": {},
            "legal_form": "doo",
            "names": ["ACME", "ACME Trading"],
            "matches": [{"document": "d1", "v": 2}, {"document": "d3"}],
        })
    );
    // An empty patch changes nothing; a missing node is `None`; a bad patch is refused.
    assert_eq!(
        a.update_props(id, &PropsPatch::new()).await.unwrap(),
        Some(got)
    );
    assert_eq!(
        a.update_props(Uuid::now_v7(), &PropsPatch::new().set(&["x"], json!(1)))
            .await
            .unwrap(),
        None
    );
    assert!(matches!(
        a.update_props(id, &PropsPatch::new().set(&[], json!(1)))
            .await,
        Err(Error::Invalid(_))
    ));
    assert!(matches!(
        a.ensure_node(NodeKind::Document, "not-a-hash").await,
        Err(Error::Invalid(_))
    ));

    // Two writers, two handles, the same node, many rounds.
    let race = a.ensure_node(NodeKind::Company, "raced").await.unwrap();
    join2(
        race_writer(a, race, "a", PATCH_ROUNDS),
        race_writer(b, race, "b", PATCH_ROUNDS),
    )
    .await;
    let props = a.node(race).await.unwrap().expect("node").props;
    for who in ["a", "b"] {
        for i in 0..PATCH_ROUNDS {
            assert_eq!(props[format!("{who}{i}")], json!(i), "{who}{i} kept");
        }
        assert_eq!(props["last"][who], json!(PATCH_ROUNDS - 1));
        assert!(
            props["seen"]
                .as_array()
                .unwrap()
                .contains(&json!({"who": who, "i": PATCH_ROUNDS - 1}))
        );
    }
    assert_eq!(props["log"].as_array().unwrap().len(), 2 * PATCH_ROUNDS);
    assert_eq!(props["seen"].as_array().unwrap().len(), 2);
}

/// Dated corporate events: "who owned X on a day" is a query.
///
/// # Panics
///
/// When the backend misbehaves.
#[allow(clippy::too_many_lines)]
pub async fn dated_edges<B: Backend + ?Sized>(s: &B) {
    let company = |k: &'static str| async move {
        s.upsert_node(NodeKind::Company, k, &json!({}))
            .await
            .unwrap()
    };
    let sub = company("sub").await;
    let old_parent = company("old-parent").await;
    let new_parent = company("new-parent").await;
    let gleif = json!([{"registry": "gleif", "record": "rr-1"}]);
    let first = Edge::new(old_parent, sub, EdgeKind::ParentOf, EdgeSource::Registry)
        .with_evidence(gleif.clone())
        .valid(Some(day(2015, 1, 1)), Some(day(2021, 6, 1)));
    let e_old = s.upsert_edge(&first).await.unwrap();
    let e_new = s
        .upsert_edge(
            &Edge::new(new_parent, sub, EdgeKind::ParentOf, EdgeSource::Registry)
                .with_evidence(gleif.clone())
                .valid(Some(day(2021, 6, 1)), None),
        )
        .await
        .unwrap();
    // The same pair again, in a later period, is a second edge.
    let e_back = s
        .upsert_edge(
            &Edge::new(old_parent, sub, EdgeKind::ParentOf, EdgeSource::Registry)
                .valid(Some(day(2024, 1, 1)), None),
        )
        .await
        .unwrap();
    assert_ne!(e_old, e_back);
    // Closing a period updates the edge in place.
    assert_eq!(
        s.upsert_edge(
            &first
                .clone()
                .valid(Some(day(2015, 1, 1)), Some(day(2021, 6, 1)))
        )
        .await
        .unwrap(),
        e_old
    );

    let parents_on = |at: Option<Date>| async move {
        s.neighbours(
            sub,
            &NeighbourQuery {
                direction: Direction::In,
                edge_kinds: vec![EdgeKind::ParentOf],
                at,
                ..NeighbourQuery::default()
            },
        )
        .await
        .unwrap()
        .into_iter()
        .map(|n| (n.edge.id, n.node.id))
        .collect::<Vec<_>>()
    };
    assert_eq!(
        parents_on(Some(day(2020, 5, 1))).await,
        [(e_old, old_parent)]
    );
    assert_eq!(
        parents_on(Some(day(2021, 6, 1))).await,
        [(e_new, new_parent)]
    );
    assert_eq!(
        parents_on(Some(day(2022, 5, 1))).await,
        [(e_new, new_parent)]
    );
    assert_eq!(
        parents_on(Some(day(2024, 2, 1))).await,
        [(e_new, new_parent), (e_back, old_parent)]
    );
    assert!(parents_on(Some(day(2014, 1, 1))).await.is_empty());
    // Undated: every period, in valid_from order.
    assert_eq!(
        parents_on(None).await,
        [
            (e_old, old_parent),
            (e_new, new_parent),
            (e_back, old_parent)
        ]
    );
    let first_stored = s
        .neighbours(
            old_parent,
            &NeighbourQuery {
                direction: Direction::Out,
                at: Some(day(2016, 1, 1)),
                ..NeighbourQuery::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(first_stored.len(), 1);
    let e = &first_stored[0].edge.edge;
    assert_eq!(
        (e.valid_from, e.valid_to, e.source, e.state),
        (
            Some(day(2015, 1, 1)),
            Some(day(2021, 6, 1)),
            EdgeSource::Registry,
            EdgeState::Active
        )
    );
    assert_eq!(e.evidence, gleif);
}

/// Locations: idempotent, and an overwritten object supersedes the old document's edge.
///
/// # Panics
///
/// When the backend misbehaves.
pub async fn locations<B: Backend + ?Sized>(s: &B) {
    let at = ObjectLocation {
        endpoint: "http://minio:9000".to_owned(),
        bucket: "docs".to_owned(),
        key: "inbox/a.pdf".to_owned(),
    };
    let first = s.upsert_location(&hash(21), &at).await.unwrap();
    assert!(!first.record, "no record stored");
    assert_eq!(s.upsert_location(&hash(21), &at).await.unwrap(), first);
    let doc = s.node(first.document).await.unwrap().expect("document");
    assert_eq!(
        (doc.kind, doc.key.as_str()),
        (NodeKind::Document, hash(21).to_hex().as_str())
    );
    let loc = s.node(first.location).await.unwrap().expect("location");
    assert_eq!((loc.kind, loc.key), (NodeKind::Location, at.node_key()));

    // The object is overwritten with other content.
    let second = s.upsert_location(&hash(22), &at).await.unwrap();
    assert_eq!(second.location, first.location);
    assert_ne!(second.document, first.document);
    let q = |include_inactive| NeighbourQuery {
        direction: Direction::In,
        edge_kinds: vec![EdgeKind::LocatedAt],
        include_inactive,
        ..NeighbourQuery::default()
    };
    let live = s.neighbours(first.location, &q(false)).await.unwrap();
    assert_eq!(
        live.iter().map(|n| n.node.id).collect::<Vec<_>>(),
        [second.document]
    );
    let all = s.neighbours(first.location, &q(true)).await.unwrap();
    assert_eq!(all.len(), 2);
    let old = all
        .iter()
        .find(|n| n.node.id == first.document)
        .expect("old edge kept");
    assert_eq!(old.edge.edge.state, EdgeState::Superseded);
    assert_eq!(old.edge.edge.source, EdgeSource::Storage);
}

/// Ended locations: the edge is superseded and dated, the document and its other copies stay,
/// and source listings stop at the gone copy.
///
/// # Panics
///
/// When the backend misbehaves.
pub async fn ended_locations<B: Backend + ?Sized>(s: &B) {
    let at = |key: &str| ObjectLocation {
        endpoint: "http://end.test:9000".to_owned(),
        bucket: "docs".to_owned(),
        key: key.to_owned(),
    };
    let doc = hash(41);
    s.put_record(&doc, &raw(&json!({"n": 41})), &versions("1"))
        .await
        .unwrap();
    let first = s.upsert_location(&doc, &at("in/a.pdf")).await.unwrap();
    assert!(first.record);
    let second = s.upsert_location(&doc, &at("in/b.pdf")).await.unwrap();
    assert_eq!(first.document, second.document);
    assert_eq!(
        s.end_location(&at("in/never.pdf"), day(2026, 1, 1))
            .await
            .unwrap(),
        None
    );
    let ended = s
        .end_location(&at("in/a.pdf"), day(2026, 3, 1))
        .await
        .unwrap()
        .expect("recorded");
    assert_eq!(
        (ended.document, ended.location),
        (first.document, first.location)
    );
    assert_eq!(ended.remaining, 1);
    let query = NeighbourQuery {
        direction: Direction::Out,
        edge_kinds: vec![EdgeKind::LocatedAt],
        include_inactive: true,
        ..NeighbourQuery::default()
    };
    let edges = s.neighbours(first.document, &query).await.unwrap();
    let gone = edges
        .iter()
        .find(|n| n.node.id == first.location)
        .expect("edge kept");
    assert_eq!(gone.edge.edge.state, EdgeState::Superseded);
    assert_eq!(gone.edge.edge.valid_to, Some(day(2026, 3, 1)));
    assert!(s.node(first.document).await.unwrap().is_some());
    let src = SourceKey::new("http://end.test:9000", "docs", "in/a").unwrap();
    assert!(
        s.list_records(&src, None, 0, usize::MAX)
            .await
            .unwrap()
            .is_empty()
    );
    let src = SourceKey::new("http://end.test:9000", "docs", "in/").unwrap();
    assert_eq!(
        s.list_records(&src, None, 0, usize::MAX)
            .await
            .unwrap()
            .len(),
        1
    );
    // Again: nothing left to end there.
    let again = s
        .end_location(&at("in/a.pdf"), day(2026, 3, 2))
        .await
        .unwrap();
    assert_eq!(again, None);
    let last = s
        .end_location(&at("in/b.pdf"), day(2026, 3, 1))
        .await
        .unwrap()
        .expect("recorded");
    assert_eq!(last.remaining, 0);
    assert!(s.get_record(&doc).await.unwrap().is_some(), "record kept");
    // The object comes back: the edge is live again.
    let back = s.upsert_location(&doc, &at("in/a.pdf")).await.unwrap();
    assert_eq!(back, first);
    let query = NeighbourQuery {
        direction: Direction::Out,
        edge_kinds: vec![EdgeKind::LocatedAt],
        ..NeighbourQuery::default()
    };
    let live = s.neighbours(first.document, &query).await.unwrap();
    assert_eq!(live.len(), 1);
    assert_eq!(live[0].edge.edge.valid_to, None);
}

/// Aliases: the first claim wins, a blocked alias resolves to nobody, a missing node fails.
///
/// # Panics
///
/// When the backend misbehaves.
pub async fn aliases<B: Backend + ?Sized>(s: &B) {
    let one = s
        .upsert_node(NodeKind::Company, "alias-one", &json!({}))
        .await
        .unwrap();
    let two = s
        .upsert_node(NodeKind::Company, "alias-two", &json!({}))
        .await
        .unwrap();
    let kind = NodeKind::Company;
    assert_eq!(s.alias(kind, "id:rs_pib:101").await.unwrap(), None);
    let got = s
        .claim_alias(kind, "id:rs_pib:101", one, &json!({"conf": 0.95}))
        .await
        .unwrap();
    assert_eq!(got.node, Some(one));
    assert_eq!(got.props, json!({"conf": 0.95}));
    // A second claim finds the first owner and changes nothing.
    let got = s
        .claim_alias(kind, "id:rs_pib:101", two, &json!({"conf": 0.5}))
        .await
        .unwrap();
    assert_eq!((got.node, got.props), (Some(one), json!({"conf": 0.95})));
    assert_eq!(
        s.alias(kind, "id:rs_pib:101").await.unwrap().unwrap().node,
        Some(one)
    );
    // The same alias under another kind is another alias.
    assert_eq!(
        s.alias(NodeKind::Document, "id:rs_pib:101").await.unwrap(),
        None
    );
    s.block_alias(kind, "name:acme", &json!({"why": "two companies"}))
        .await
        .unwrap();
    let blocked = s.alias(kind, "name:acme").await.unwrap().unwrap();
    assert_eq!(blocked.node, None);
    let got = s
        .claim_alias(kind, "name:acme", two, &json!({}))
        .await
        .unwrap();
    assert_eq!(got.node, None, "a blocked alias stays blocked");
    s.block_alias(kind, "id:rs_pib:101", &json!({}))
        .await
        .unwrap();
    assert_eq!(
        s.alias(kind, "id:rs_pib:101").await.unwrap().unwrap().node,
        None
    );
    assert!(
        s.claim_alias(kind, "x", Uuid::now_v7(), &json!({}))
            .await
            .is_err()
    );
    assert!(matches!(
        s.claim_alias(kind, "", one, &json!({})).await,
        Err(Error::Invalid(_))
    ));
}

/// [`crate::GraphStore::claim_aliases`] and [`crate::GraphStore::aliases`] (issue #103): in
/// order, as one call after the other; an alias given twice is claimed by its first entry.
///
/// # Panics
///
/// When the backend misbehaves.
pub async fn alias_batches<B: Backend + ?Sized>(s: &B) {
    let kind = NodeKind::Company;
    let one = s.upsert_node(kind, "batch-one", &json!({})).await.unwrap();
    let two = s.upsert_node(kind, "batch-two", &json!({})).await.unwrap();
    s.block_alias(kind, "name:batch-blocked", &json!({"why": "two companies"}))
        .await
        .unwrap();
    let claims = [
        ("name:batch-b".to_owned(), json!({"n": 1})),
        ("name:batch-a".to_owned(), json!({"n": 2})),
        ("name:batch-blocked".to_owned(), json!({"n": 3})),
        ("name:batch-b".to_owned(), json!({"n": 4})),
    ];
    let got = s.claim_aliases(kind, two, &claims).await.unwrap();
    let got: Vec<_> = got.into_iter().map(|a| (a.node, a.props)).collect();
    assert_eq!(
        got,
        [
            (Some(two), json!({"n": 1})),
            (Some(two), json!({"n": 2})),
            (None, json!({"why": "two companies"})),
            (Some(two), json!({"n": 1})),
        ]
    );
    let got = s
        .claim_aliases(kind, one, &[("name:batch-a".to_owned(), json!({}))])
        .await
        .unwrap();
    assert_eq!(
        got[0].node,
        Some(two),
        "the first claim wins in a batch too"
    );
    assert!(s.claim_aliases(kind, one, &[]).await.unwrap().is_empty());
    let keys: Vec<String> = [
        "name:batch-a",
        "name:none",
        "name:batch-blocked",
        "name:batch-a",
    ]
    .map(str::to_owned)
    .to_vec();
    let found = s.aliases(kind, &keys).await.unwrap();
    assert_eq!(
        found
            .iter()
            .map(|a| a.as_ref().map(|a| a.node))
            .collect::<Vec<_>>(),
        [Some(Some(two)), None, Some(None), Some(Some(two))]
    );
    assert_eq!(found[0].as_ref().unwrap().props, json!({"n": 2}));
    assert!(
        s.aliases(NodeKind::Document, &keys)
            .await
            .unwrap()
            .iter()
            .all(Option::is_none)
    );
    assert!(s.aliases(kind, &[]).await.unwrap().is_empty());
    let many = vec!["name:x".to_owned(); crate::ALIASES_MAX + 1];
    assert!(matches!(
        s.aliases(kind, &many).await,
        Err(Error::Invalid(_))
    ));
    assert!(
        s.claim_aliases(kind, Uuid::now_v7(), &[("name:y".to_owned(), json!({}))])
            .await
            .is_err()
    );
    assert!(matches!(
        s.claim_aliases(kind, one, &[(String::new(), json!({}))])
            .await,
        Err(Error::Invalid(_))
    ));
}

/// [`crate::GraphStore::node_ids`] and [`crate::GraphStore::upsert_edges`] (issue #126): in
/// order, as one call after the other; an invalid edge fails the batch before any write.
///
/// # Panics
///
/// When the backend misbehaves.
pub async fn graph_batches<B: Backend + ?Sized>(s: &B) {
    let kind = NodeKind::Document;
    let keys: Vec<String> = [61, 62, 63, 64].map(|n| hash(n).to_hex()).to_vec();
    let a = s.ensure_node(kind, &keys[0]).await.unwrap();
    let b = s.ensure_node(kind, &keys[1]).await.unwrap();
    let c = s.ensure_node(kind, &keys[2]).await.unwrap();
    let asked = [&keys[0], &keys[3], &keys[1], &keys[0]].map(String::clone);
    assert_eq!(
        s.node_ids(kind, &asked).await.unwrap(),
        [Some(a), None, Some(b), Some(a)]
    );
    assert!(
        s.node_ids(NodeKind::Company, &asked)
            .await
            .unwrap()
            .iter()
            .all(Option::is_none)
    );
    assert!(s.node_ids(kind, &[]).await.unwrap().is_empty());
    let many = vec![keys[0].clone(); crate::GRAPH_BATCH_MAX + 1];
    assert!(matches!(
        s.node_ids(kind, &many).await,
        Err(Error::Invalid(_))
    ));

    let edge = |src, dst, n: i32| {
        Edge::new(src, dst, EdgeKind::SimilarTo, EdgeSource::Document)
            .with_props(json!({"n": n}))
            .with_confidence(0.9)
    };
    let ids = s
        .upsert_edges(&[edge(a, b, 1), edge(b, c, 2), edge(a, b, 3)])
        .await
        .unwrap();
    assert_eq!(ids.len(), 3);
    assert_ne!(ids[0], ids[1]);
    assert_eq!(ids[0], ids[2], "same identity, same edge");
    let out = |node| async move {
        s.neighbours(
            node,
            &NeighbourQuery {
                direction: Direction::Out,
                edge_kinds: vec![EdgeKind::SimilarTo],
                ..NeighbourQuery::default()
            },
        )
        .await
        .unwrap()
        .into_iter()
        .map(|n| (n.edge.id, n.node.id, n.edge.edge.props))
        .collect::<Vec<_>>()
    };
    assert_eq!(
        out(a).await,
        [(ids[0], b, json!({"n": 3}))],
        "the last one wins"
    );
    assert_eq!(out(b).await, [(ids[1], c, json!({"n": 2}))]);
    assert_eq!(
        s.upsert_edges(&[edge(a, b, 3)]).await.unwrap(),
        [ids[0]],
        "an upsert keeps the id"
    );
    assert!(s.upsert_edges(&[]).await.unwrap().is_empty());
    // An invalid edge anywhere: nothing is written.
    let bad = edge(c, a, 4).with_confidence(2.0);
    assert!(matches!(
        s.upsert_edges(&[edge(c, b, 5), bad]).await,
        Err(Error::Invalid(_))
    ));
    assert!(out(c).await.is_empty());
    let many = vec![edge(a, b, 3); crate::GRAPH_BATCH_MAX + 1];
    assert!(matches!(
        s.upsert_edges(&many).await,
        Err(Error::Invalid(_))
    ));
    // A missing end fails the call.
    assert!(s.upsert_edges(&[edge(a, Uuid::now_v7(), 6)]).await.is_err());
}

/// [`crate::SketchStore::sketch_candidates_within`] (issue #148): a band whose bucket holds
/// more than `max_bucket` documents is skipped and counted; the bucket sizes follow
/// replaced sketches.
///
/// # Panics
///
/// When the backend misbehaves.
pub async fn stop_bands<B: Backend + ?Sized>(s: &B) {
    let (wide, pair, own, gone) = (5_000, 5_001, u64::MAX - 5_002, 5_003);
    for n in 81..=86 {
        s.put_record(&hash(n), &raw(&json!({"n": n})), &versions("1"))
            .await
            .unwrap();
    }
    for n in 81..=85 {
        let bands: &[u64] = if n <= 82 { &[wide, pair] } else { &[wide] };
        s.put_sketch(&hash(n), &[n], bands).await.unwrap();
    }
    s.put_sketch(&hash(86), &[86], &[own]).await.unwrap();
    let ask = [wide, pair, own, 7_777];
    let keys = |c: &crate::Candidates| {
        c.sketches
            .iter()
            .map(|k| (k.blake3, k.shared_bands))
            .collect::<Vec<_>>()
    };
    // `wide` has 5 documents: over a limit of 4, skipped.
    let got = s
        .sketch_candidates_within(&ask, &hash(86), 0, 4)
        .await
        .unwrap();
    assert_eq!(got.skipped_bands, 1);
    assert_eq!(keys(&got), [(hash(81), 1), (hash(82), 1)]);
    assert_eq!(got.sketches[0].signature, [81]);
    // At the limit nothing is skipped, and the candidates are those of `sketch_candidates`.
    let all = s
        .sketch_candidates_within(&ask, &hash(86), 0, 5)
        .await
        .unwrap();
    assert_eq!(all.skipped_bands, 0);
    assert_eq!(
        all.sketches,
        s.sketch_candidates(&ask, &hash(86), 0).await.unwrap()
    );
    assert_eq!(all.sketches.len(), 5);
    // A band given twice is one skipped band; only skipped bands: no candidate.
    let got = s
        .sketch_candidates_within(&[wide, wide], &hash(86), 0, 4)
        .await
        .unwrap();
    assert_eq!((got.skipped_bands, got.sketches.len()), (1, 0));
    // A replaced sketch leaves the bucket: `wide` is back to 4.
    s.put_sketch(&hash(85), &[85], &[gone]).await.unwrap();
    let got = s
        .sketch_candidates_within(&ask, &hash(86), 0, 4)
        .await
        .unwrap();
    assert_eq!(got.skipped_bands, 0);
    assert_eq!(got.sketches.len(), 4);
    assert_eq!(
        s.sketch_candidates_within(&[], &hash(86), 0, 4)
            .await
            .unwrap(),
        crate::Candidates::default()
    );
}

/// The LSH band index: candidates by shared bands, replacement, the excluded document.
///
/// # Panics
///
/// When the backend misbehaves.
pub async fn sketches<B: Backend + ?Sized>(s: &B) {
    for n in 51..=54 {
        s.put_record(&hash(n), &raw(&json!({"n": n})), &versions("1"))
            .await
            .unwrap();
    }
    let big = u64::MAX - 7;
    s.put_sketch(&hash(51), &[1, 2, 3], &[10, 11, big])
        .await
        .unwrap();
    s.put_sketch(&hash(52), &[4, 5], &[10, 99]).await.unwrap();
    s.put_sketch(&hash(53), &[6], &[77]).await.unwrap();
    let got = s
        .sketch_candidates(&[10, 11, big], &hash(54), 0)
        .await
        .unwrap();
    assert_eq!(
        got.iter()
            .map(|c| (c.blake3, c.shared_bands))
            .collect::<Vec<_>>(),
        [(hash(51), 3), (hash(52), 1)]
    );
    assert_eq!(got[0].signature, [1, 2, 3]);
    // The document itself is never its own candidate.
    let got = s.sketch_candidates(&[10], &hash(51), 0).await.unwrap();
    assert_eq!(got.iter().map(|c| c.blake3).collect::<Vec<_>>(), [hash(52)]);
    assert_eq!(
        s.sketch_candidates(&[10], &hash(54), 1)
            .await
            .unwrap()
            .len(),
        1
    );
    assert!(
        s.sketch_candidates(&[], &hash(54), 0)
            .await
            .unwrap()
            .is_empty()
    );
    // Writing the same sketch again changes nothing (issue #74).
    let before = s
        .sketch_candidates(&[10, 11, 99, big], &hash(54), 0)
        .await
        .unwrap();
    s.put_sketch(&hash(51), &[1, 2, 3], &[big, 10, 11, 10])
        .await
        .unwrap();
    assert_eq!(
        s.sketch_candidates(&[10, 11, 99, big], &hash(54), 0)
            .await
            .unwrap(),
        before
    );
    // The same signature with other bands still replaces the bands.
    s.put_sketch(&hash(52), &[4, 5], &[99, 98]).await.unwrap();
    let got = s.sketch_candidates(&[10, 98], &hash(54), 0).await.unwrap();
    assert_eq!(
        got.iter()
            .map(|c| (c.blake3, c.shared_bands))
            .collect::<Vec<_>>(),
        [(hash(51), 1), (hash(52), 1)]
    );
    s.put_sketch(&hash(52), &[4, 5], &[10, 99]).await.unwrap();
    // Replacing a sketch replaces its bands.
    s.put_sketch(&hash(51), &[9], &[77]).await.unwrap();
    let got = s.sketch_candidates(&[11], &hash(54), 0).await.unwrap();
    assert!(got.is_empty());
    let got = s.sketch_candidates(&[77], &hash(54), 0).await.unwrap();
    assert_eq!(
        got.iter().map(|c| c.blake3).collect::<Vec<_>>(),
        [hash(51), hash(53)]
    );
    assert_eq!(got[0].signature, [9]);
    // A sketch without a record, and an empty signature, are refused.
    assert!(s.put_sketch(&hash(55), &[1], &[1]).await.is_err());
    assert!(matches!(
        s.put_sketch(&hash(52), &[], &[1]).await,
        Err(Error::Invalid(_))
    ));
}

/// Records listed by source: the bucket and prefix filter, hash order and paging.
///
/// # Panics
///
/// When the backend misbehaves.
pub async fn records_by_source<B: Backend + ?Sized>(s: &B) {
    let endpoint = "http://list.test:9000";
    let at = |bucket: &str, key: &str| ObjectLocation {
        endpoint: endpoint.to_owned(),
        bucket: bucket.to_owned(),
        key: key.to_owned(),
    };
    for (n, locations) in [
        (33, vec![at("docs", "in/c.pdf")]),
        (
            31,
            vec![at("docs", "in/a.pdf"), at("docs", "in/a-copy.pdf")],
        ),
        (
            32,
            vec![at("docs", "out/b.pdf"), at("docs", "in/sub/b.pdf")],
        ),
        (34, vec![at("other", "in/d.pdf")]),
    ] {
        s.put_record(&hash(n), &raw(&json!({"n": n})), &versions("1"))
            .await
            .unwrap();
        for l in &locations {
            s.upsert_location(&hash(n), l).await.unwrap();
        }
    }
    // A location without a record is not listed.
    s.upsert_location(&hash(35), &at("docs", "in/e.pdf"))
        .await
        .unwrap();
    let list = |prefix: &'static str, after: Option<Blake3>, limit| async move {
        let src = SourceKey::new(endpoint, "docs", prefix).unwrap();
        s.list_records(&src, after.as_ref(), limit, usize::MAX)
            .await
            .unwrap()
            .into_iter()
            .map(|r| r.blake3)
            .collect::<Vec<_>>()
    };
    assert_eq!(list("in/", None, 0).await, [hash(31), hash(32), hash(33)]);
    assert_eq!(list("", None, 0).await, [hash(31), hash(32), hash(33)]);
    assert_eq!(list("out/", None, 0).await, [hash(32)]);
    assert_eq!(list("in/", None, 2).await, [hash(31), hash(32)]);
    assert_eq!(list("in/", Some(hash(32)), 2).await, [hash(33)]);
    assert!(list("in/", Some(hash(33)), 2).await.is_empty());
    assert!(list("nope/", None, 0).await.is_empty());
    let src = SourceKey::new(endpoint, "docs", "in/").unwrap();
    let page = s.list_records(&src, None, 1, usize::MAX).await.unwrap();
    assert_eq!(page[0].body_value().unwrap(), json!({"n": 31}));
    assert_eq!(page[0].versions, versions("1"));

    // The byte budget (issue #111): a page ends with the record that reaches it, and walking
    // the pages returns every record once. Records 160..180 under `sized/`, of varied sizes.
    for n in 160..180u8 {
        let pad = "x".repeat(usize::from(n % 7) * 300);
        s.put_record(&hash(n), &raw(&json!({"n": n, "pad": pad})), &versions("1"))
            .await
            .unwrap();
        s.upsert_location(&hash(n), &at("docs", &format!("sized/{n}.pdf")))
            .await
            .unwrap();
    }
    let src = SourceKey::new(endpoint, "docs", "sized/").unwrap();
    let all = s.list_records(&src, None, 0, usize::MAX).await.unwrap();
    assert_eq!(all.len(), 20);
    for (limit, budget) in [(0, 1), (0, 700), (0, 2000), (3, 1000), (0, usize::MAX)] {
        let (mut seen, mut after, mut pages) = (Vec::new(), None::<Blake3>, 0);
        loop {
            let page = s
                .list_records(&src, after.as_ref(), limit, budget)
                .await
                .unwrap();
            pages += 1;
            // Every record but the last stays under the budget.
            let sizes: Vec<usize> = page.iter().map(crate::record_page_bytes).collect();
            if let Some((_, before_last)) = sizes.split_last() {
                assert!(before_last.iter().sum::<usize>() < budget, "{sizes:?}");
            }
            assert!(page.len() <= crate::records_limit(limit) as usize);
            seen.extend(page.iter().map(|r| r.blake3));
            if !crate::records_page_full(&page, limit, budget) {
                break;
            }
            after = page.last().map(|r| r.blake3);
            assert!(pages <= 21, "limit {limit}, budget {budget}");
        }
        assert_eq!(
            seen,
            all.iter().map(|r| r.blake3).collect::<Vec<_>>(),
            "limit {limit}, budget {budget}"
        );
        if budget == 1 {
            assert_eq!(pages, 21);
        }
    }
}

/// Source claims across two instances.
///
/// # Panics
///
/// When the backend misbehaves.
pub async fn claims<B: Backend + ?Sized>(a: &B, b: &B) {
    let (ia, ib) = (instance("inst-a"), instance("inst-b"));
    let src = SourceKey::new("http://minio:9000/", "docs", "inbox/").unwrap();
    assert_eq!(a.source_owner(&src).await.unwrap(), None);
    let claim = a.claim_source(&src, &ia).await.unwrap();
    assert_eq!((&claim.key, &claim.owner), (&src, &ia));
    // Claiming again is a no-op that keeps the original time.
    assert_eq!(a.claim_source(&src, &ia).await.unwrap(), claim);

    let err = b.claim_source(&src, &ib).await.unwrap_err();
    assert!(
        matches!(&err, Error::SourceClaimed { owner, key } if owner == &ia && key == &src),
        "{err:?}"
    );
    assert!(err.to_string().contains("\"inst-a\""), "{err}");
    assert!(matches!(
        b.release_source(&src, &ib).await,
        Err(Error::SourceClaimed { .. })
    ));
    assert_eq!(b.source_owner(&src).await.unwrap(), Some(claim));

    // Another prefix of the same bucket is another source.
    let other = SourceKey::new("http://minio:9000", "docs", "archive/").unwrap();
    assert_eq!(b.claim_source(&other, &ib).await.unwrap().owner, ib);

    assert!(a.release_source(&src, &ia).await.unwrap());
    assert!(!a.release_source(&src, &ia).await.unwrap());
    assert_eq!(b.claim_source(&src, &ib).await.unwrap().owner, ib);
    assert!(matches!(
        a.claim_source(&src, &ia).await,
        Err(Error::SourceClaimed { owner, .. }) if owner == ib
    ));
}

/// Company enrichment leases across two instances.
///
/// # Panics
///
/// When the backend misbehaves.
pub async fn leases<B: Backend + ?Sized>(a: &B, b: &B) {
    let (ia, ib) = (instance("inst-a"), instance("inst-b"));
    let co = a
        .upsert_node(NodeKind::Company, "leased-co", &json!({}))
        .await
        .unwrap();
    let minute = Duration::from_mins(1);
    assert!(a.lease_company(co, &ia, minute).await.unwrap());
    assert!(!b.lease_company(co, &ib, minute).await.unwrap());
    assert!(a.lease_company(co, &ia, minute).await.unwrap(), "renewal");
    assert!(!b.release_company(co, &ib).await.unwrap());
    assert!(a.release_company(co, &ia).await.unwrap());
    assert!(b.lease_company(co, &ib, minute).await.unwrap());
    assert!(b.release_company(co, &ib).await.unwrap());
    // An expired lease can be taken over.
    assert!(a.lease_company(co, &ia, Duration::ZERO).await.unwrap());
    assert!(b.lease_company(co, &ib, minute).await.unwrap());
    assert!(!a.lease_company(co, &ia, minute).await.unwrap());
}

/// Registry delta feeds (issue #49): the feed lease across two instances, the cursor written
/// only under it, the alias lookups, cache invalidation by prefix, and marking companies due.
///
/// # Panics
///
/// When the backend misbehaves.
#[allow(clippy::too_many_lines)]
pub async fn feeds<B: Backend + ?Sized>(a: &B, b: &B) {
    let (ia, ib) = (instance("inst-a"), instance("inst-b"));
    let minute = Duration::from_mins(1);
    assert_eq!(a.feed_state("gleif").await.unwrap(), None);
    assert!(a.lease_feed("Bad-Name", &ia, minute).await.is_err());

    // One instance at a time; renewal; release; take-over of an expired lease.
    assert!(a.lease_feed("gleif", &ia, minute).await.unwrap());
    assert!(!b.lease_feed("gleif", &ib, minute).await.unwrap());
    assert!(a.lease_feed("gleif", &ia, minute).await.unwrap(), "renewal");
    let state = b.feed_state("gleif").await.unwrap().unwrap();
    assert_eq!(
        (state.cursor.as_str(), state.next_run_at, state.updated_at),
        ("", 0, 0)
    );
    assert_eq!(state.owner.as_deref(), Some("inst-a"));
    // Only the holder writes the cursor.
    assert!(!b.save_feed("gleif", &ib, "nope", 5).await.unwrap());
    let before = unix_now();
    assert!(
        a.save_feed("gleif", &ia, "2026-11-25T08:00:00Z", before + 3600)
            .await
            .unwrap()
    );
    assert!(a.save_feed("gleif", &ia, "bad\ncursor", 0).await.is_err());
    let state = b.feed_state("gleif").await.unwrap().unwrap();
    assert_eq!(state.cursor, "2026-11-25T08:00:00Z");
    assert_eq!(state.next_run_at, before + 3600);
    assert!(state.updated_at >= before - 1, "{state:?}");
    assert!(!b.release_feed("gleif", &ib).await.unwrap());
    assert!(a.release_feed("gleif", &ia).await.unwrap());
    let state = b.feed_state("gleif").await.unwrap().unwrap();
    assert_eq!(state.owner, None);
    assert_eq!(state.cursor, "2026-11-25T08:00:00Z", "the cursor stays");
    assert!(
        !a.save_feed("gleif", &ia, "x", 0).await.unwrap(),
        "no lease"
    );
    assert!(b.lease_feed("gleif", &ib, minute).await.unwrap());
    assert!(!a.lease_feed("gleif", &ia, minute).await.unwrap());
    assert!(b.lease_feed("gleif", &ib, Duration::ZERO).await.unwrap());
    assert_eq!(a.feed_state("gleif").await.unwrap().unwrap().owner, None);
    assert!(a.lease_feed("gleif", &ia, minute).await.unwrap(), "expired");
    assert!(!b.save_feed("gleif", &ib, "late", 0).await.unwrap());
    // Feeds are independent.
    assert!(b.lease_feed("edgar", &ib, minute).await.unwrap());
    assert!(a.release_feed("gleif", &ia).await.unwrap());
    assert!(b.release_feed("edgar", &ib).await.unwrap());

    // Alias lookups: owned company aliases only, by exact alias or by prefix in byte order.
    let x = a
        .upsert_node(NodeKind::Company, "feed-x", &json!({}))
        .await
        .unwrap();
    let y = a
        .upsert_node(NodeKind::Company, "feed-y", &json!({}))
        .await
        .unwrap();
    let props = json!({"step": "test"});
    for (alias, node) in [
        ("id:rs_mb:99000011", x),
        ("id:rs_mb:99000029", y),
        ("id:lei:FEEDTEST0000000000X1", x),
        ("reg:edgar:9990000001", y),
    ] {
        a.claim_alias(NodeKind::Company, alias, node, &props)
            .await
            .unwrap();
    }
    a.block_alias(NodeKind::Company, "id:rs_mb:99000037", &json!({}))
        .await
        .unwrap();
    let want = |v: &[&str]| v.iter().map(|s| (*s).to_owned()).collect::<Vec<_>>();
    assert_eq!(
        b.companies_by_alias(&want(&[
            "reg:edgar:9990000001",
            "id:lei:FEEDTEST0000000000X1",
            "id:rs_mb:99000037",
            "id:lei:NOBODY",
        ]))
        .await
        .unwrap(),
        [
            ("id:lei:FEEDTEST0000000000X1".to_owned(), x),
            ("reg:edgar:9990000001".to_owned(), y),
        ]
    );
    assert_eq!(b.companies_by_alias(&[]).await.unwrap(), []);
    let too_many = vec!["id:lei:X".to_owned(); crate::FEED_BATCH_MAX + 1];
    assert!(b.companies_by_alias(&too_many).await.is_err());
    assert_eq!(
        b.company_aliases("id:rs_mb:", None, 0).await.unwrap(),
        [
            ("id:rs_mb:99000011".to_owned(), x),
            ("id:rs_mb:99000029".to_owned(), y),
        ]
    );
    assert_eq!(
        b.company_aliases("id:rs_mb:", Some("id:rs_mb:99000011"), 1)
            .await
            .unwrap(),
        [("id:rs_mb:99000029".to_owned(), y)]
    );
    assert_eq!(
        b.company_aliases("id:rs_mb:", Some("id:rs_mb:99000029"), 10)
            .await
            .unwrap(),
        []
    );

    // Cache invalidation by key prefix, one registry only.
    let now = unix_now();
    for (registry, key) in [
        ("gleif", "lei-records/FEEDTEST0000000000X1"),
        (
            "gleif",
            "lei-records/FEEDTEST0000000000X1/direct-parent-relationship",
        ),
        ("gleif", "lei-records/FEEDTEST0000000000Y2"),
        ("wikidata", "lei-records/FEEDTEST0000000000X1"),
    ] {
        a.cache_put(&CacheRow {
            registry: registry.to_owned(),
            key: key.to_owned(),
            body: Some(b"{}".to_vec()),
            fetched_at: now,
            expires_at: now + 3600,
        })
        .await
        .unwrap();
    }
    assert_eq!(
        b.cache_invalidate("gleif", "lei-records/FEEDTEST0000000000X1")
            .await
            .unwrap(),
        2
    );
    assert!(
        a.cache_get(
            "gleif",
            "lei-records/FEEDTEST0000000000X1/direct-parent-relationship"
        )
        .await
        .unwrap()
        .is_none()
    );
    assert!(
        a.cache_get("gleif", "lei-records/FEEDTEST0000000000Y2")
            .await
            .unwrap()
            .is_some()
    );
    assert!(
        a.cache_get("wikidata", "lei-records/FEEDTEST0000000000X1")
            .await
            .unwrap()
            .is_some()
    );
    assert!(a.cache_invalidate("gleif", "").await.is_err());

    // Marking due: only companies enriched before; the other registries keep their time.
    let doc = a
        .upsert_node(NodeKind::Document, &hash(250).to_hex(), &json!({}))
        .await
        .unwrap();
    for co in [x, y] {
        a.upsert_edge(&Edge::new(doc, co, EdgeKind::Party, EdgeSource::Document))
            .await
            .unwrap();
    }
    let day = Duration::from_hours(24);
    a.record_enrichment(
        x,
        &[
            ("gleif".to_owned(), day * 30),
            ("vies".to_owned(), day * 90),
        ],
    )
    .await
    .unwrap();
    let due_of = |due: Vec<DueCompany>, co: Uuid| due.into_iter().find(|d| d.company == co);
    assert_eq!(
        due_of(a.companies_due(&ia, None, 0).await.unwrap(), x),
        None
    );
    assert_eq!(
        b.mark_many_due(&[x, y, Uuid::now_v7()], "gleif")
            .await
            .unwrap(),
        1,
        "y was never enriched (it is new anyway)"
    );
    let due = due_of(a.companies_due(&ia, None, 0).await.unwrap(), x).unwrap();
    assert_eq!(
        (due.reason, due.due),
        (DueReason::Scheduled, vec!["gleif".to_owned()])
    );
    let after = a.enrichment(x).await.unwrap().unwrap();
    // Postgres rounds its times to the second (up, at times).
    assert!(after.next_due["gleif"] <= unix_now() + 1);
    assert!(after.next_due["vies"] > unix_now() + 80 * 86_400);
    // A registry not in the schedule yet is added; an earlier due time is kept.
    assert_eq!(b.mark_many_due(&[x], "edgar").await.unwrap(), 1);
    assert_eq!(b.mark_many_due(&[], "edgar").await.unwrap(), 0);
    assert!(b.mark_many_due(&[x], "Bad").await.is_err());
    let due = due_of(a.companies_due(&ia, None, 0).await.unwrap(), x).unwrap();
    assert_eq!(due.due, ["edgar", "gleif"]);
    // Re-enriching sets them again.
    a.record_enrichment(
        x,
        &[("gleif".to_owned(), day * 30), ("edgar".to_owned(), day)],
    )
    .await
    .unwrap();
    assert_eq!(
        due_of(a.companies_due(&ia, None, 0).await.unwrap(), x),
        None
    );
}

/// Search: text, every filter, paging and re-indexing.
///
/// # Panics
///
/// When the backend misbehaves.
#[allow(clippy::too_many_lines)]
pub async fn search<B: Backend + ?Sized>(s: &B) {
    let docs = [
        (
            hash(31),
            "Invoice 42",
            "Consulting services for ACME",
            "invoice",
            day(2024, 3, 1),
            1200.0,
        ),
        (
            hash(32),
            "Framework contract",
            "Supply agreement between ACME and Globex",
            "contract",
            day(2023, 1, 15),
            50_000.0,
        ),
        (
            hash(33),
            "Invoice 43",
            "Hardware for Globex",
            "invoice",
            day(2024, 7, 1),
            300.0,
        ),
    ];
    let missing = SearchDoc {
        blake3: hash(39),
        title: "x".to_owned(),
        body: String::new(),
        kind: None,
        date: None,
        amount: None,
    };
    assert!(
        s.index_document(&missing).await.is_err(),
        "the record must exist"
    );
    for (h, title, body, kind, date, amount) in &docs {
        s.put_record(h, &raw(&json!({"kind": kind})), &versions("1"))
            .await
            .unwrap();
        let doc = SearchDoc {
            blake3: *h,
            title: (*title).to_owned(),
            body: (*body).to_owned(),
            kind: Some((*kind).to_owned()),
            date: Some(*date),
            amount: Some(*amount),
        };
        s.index_document(&doc).await.unwrap();
        s.index_document(&doc).await.unwrap();
    }
    let [d1, d2, d3] = [docs[0].0, docs[1].0, docs[2].0];
    let find = |q: SearchQuery| async move {
        s.search(&q)
            .await
            .unwrap()
            .into_iter()
            .map(|h| h.blake3)
            .collect::<Vec<_>>()
    };
    let set = |v: Vec<Blake3>| v.into_iter().collect::<BTreeSet<_>>();
    let text = |t: &str| SearchQuery {
        text: Some(t.to_owned()),
        ..SearchQuery::default()
    };

    // No filter: every document, newest first.
    assert_eq!(find(SearchQuery::default()).await, [d3, d1, d2]);
    assert_eq!(set(find(text("invoice")).await), set(vec![d1, d3]));
    assert_eq!(set(find(text("ACME")).await), set(vec![d1, d2]));
    assert_eq!(
        find(text("acme, invoice!")).await,
        [d1],
        "every word must match"
    );
    assert!(find(text("nothing-like-this")).await.is_empty());
    assert_eq!(
        find(text("  ...  ")).await.len(),
        3,
        "no words: no text filter"
    );
    let hits = s.search(&text("globex")).await.unwrap();
    assert!(hits.iter().all(|h| h.score > 0.0), "{hits:?}");
    let hit = s
        .search(&text("hardware"))
        .await
        .unwrap()
        .pop()
        .expect("hit");
    assert_eq!(
        (
            hit.blake3,
            hit.title.as_str(),
            hit.kind.as_deref(),
            hit.date,
            hit.amount
        ),
        (
            d3,
            "Invoice 43",
            Some("invoice"),
            Some(day(2024, 7, 1)),
            Some(300.0)
        )
    );

    let kind = SearchQuery {
        kind: Some("contract".to_owned()),
        ..SearchQuery::default()
    };
    assert_eq!(find(kind).await, [d2]);
    let dates = SearchQuery {
        date_from: Some(day(2024, 1, 1)),
        date_to: Some(day(2024, 7, 1)),
        ..SearchQuery::default()
    };
    assert_eq!(find(dates).await, [d3, d1], "both ends inclusive");
    let amounts = SearchQuery {
        amount_min: Some(1000.0),
        amount_max: Some(50_000.0),
        ..SearchQuery::default()
    };
    assert_eq!(find(amounts).await, [d1, d2]);
    let page = SearchQuery {
        limit: 1,
        offset: 1,
        ..SearchQuery::default()
    };
    assert_eq!(find(page).await, [d1]);

    // Company: documents with a live party edge.
    let co = s
        .upsert_node(NodeKind::Company, "search-acme", &json!({}))
        .await
        .unwrap();
    for h in [d1, d2] {
        let doc = s
            .upsert_node(NodeKind::Document, &h.to_hex(), &json!({}))
            .await
            .unwrap();
        s.upsert_edge(&Edge::new(doc, co, EdgeKind::Party, EdgeSource::Document))
            .await
            .unwrap();
    }
    let by_company = |t: Option<&str>| SearchQuery {
        text: t.map(str::to_owned),
        company: Some(co),
        ..SearchQuery::default()
    };
    assert_eq!(find(by_company(None)).await, [d1, d2]);
    assert_eq!(find(by_company(Some("globex"))).await, [d2]);

    // Source: documents with a location under the prefix.
    s.upsert_location(
        &d3,
        &ObjectLocation {
            // Normalized like a source key, so the source filter below matches.
            endpoint: "http://minio:9000/".to_owned(),
            bucket: "search".to_owned(),
            key: "inbox/2024/x.pdf".to_owned(),
        },
    )
    .await
    .unwrap();
    let by_source = |prefix: &str| SearchQuery {
        source: Some(SourceKey::new("http://minio:9000", "search", prefix).unwrap()),
        ..SearchQuery::default()
    };
    assert_eq!(find(by_source("inbox/")).await, [d3]);
    assert_eq!(find(by_source("")).await, [d3]);
    assert!(find(by_source("archive/")).await.is_empty());
    // A prefix is literal: no wildcards.
    assert!(find(by_source("inbox/%")).await.is_empty());
    assert!(find(by_source("inbox_")).await.is_empty());

    // Re-indexing replaces the text.
    s.index_document(&SearchDoc {
        blake3: d1,
        title: "Credit note 42".to_owned(),
        body: "Consulting services for ACME".to_owned(),
        kind: Some("credit_note".to_owned()),
        date: Some(day(2024, 3, 1)),
        amount: None,
    })
    .await
    .unwrap();
    assert_eq!(find(text("invoice")).await, [d3]);
    assert_eq!(find(text("credit")).await, [d1]);
    // The same document again changes nothing (issue #74), and a change of one field alone,
    // from no value to a value, is written.
    let credit = SearchDoc {
        blake3: d1,
        title: "Credit note 42".to_owned(),
        body: "Consulting services for ACME".to_owned(),
        kind: Some("credit_note".to_owned()),
        date: Some(day(2024, 3, 1)),
        amount: None,
    };
    s.index_document(&credit).await.unwrap();
    assert_eq!(find(text("credit")).await, [d1]);
    s.index_document(&SearchDoc {
        amount: Some(-5.0),
        ..credit.clone()
    })
    .await
    .unwrap();
    let negative = SearchQuery {
        amount_max: Some(0.0),
        ..SearchQuery::default()
    };
    assert_eq!(find(negative).await, [d1]);
    s.index_document(&credit).await.unwrap();
    let amounts = SearchQuery {
        amount_min: Some(0.0),
        ..SearchQuery::default()
    };
    assert_eq!(
        find(amounts).await,
        [d3, d2],
        "no amount never matches a range"
    );
}

/// Now, in Unix seconds.
fn unix_now() -> i64 {
    let secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock after 1970")
        .as_secs();
    i64::try_from(secs).expect("seconds fit")
}

/// The registry and favicon caches, across two instances: round trips, replacement, expiry on
/// read, the purge and the input checks.
///
/// # Panics
///
/// When the backend misbehaves.
#[allow(clippy::too_many_lines)]
pub async fn caches<B: Backend + ?Sized>(a: &B, b: &B) {
    let now = unix_now();
    let row = |registry: &str, key: &str, body: Option<&[u8]>, expires_at: i64| CacheRow {
        registry: registry.to_owned(),
        key: key.to_owned(),
        body: body.map(<[u8]>::to_vec),
        fetched_at: now - 10,
        expires_at,
    };
    assert_eq!(a.cache_get("gleif", "lei-records/X").await.unwrap(), None);
    let found = row("gleif", "lei-records/X", Some(b"{\"data\":1}"), now + 3600);
    a.cache_put(&found).await.unwrap();
    // The other instance sees it.
    assert_eq!(
        b.cache_get("gleif", "lei-records/X").await.unwrap(),
        Some(found.clone())
    );
    // Another registry, or a key differing only in case, is another entry.
    assert_eq!(b.cache_get("vies", "lei-records/X").await.unwrap(), None);
    assert_eq!(b.cache_get("gleif", "lei-records/x").await.unwrap(), None);
    // Replaced, here by "no such record".
    let missing = row("gleif", "lei-records/X", None, now + 60);
    b.cache_put(&missing).await.unwrap();
    assert_eq!(
        a.cache_get("gleif", "lei-records/X").await.unwrap(),
        Some(missing)
    );
    // An empty body is a body.
    let empty = row("vies", "check/Ä", Some(b""), now + 60);
    a.cache_put(&empty).await.unwrap();
    assert_eq!(a.cache_get("vies", "check/Ä").await.unwrap(), Some(empty));
    // Expired rows are not returned, and the purge deletes them.
    a.cache_put(&row("edgar", "old", Some(b"x"), now - 1))
        .await
        .unwrap();
    a.cache_put(&row("edgar", "older", None, now - 3600))
        .await
        .unwrap();
    assert_eq!(a.cache_get("edgar", "old").await.unwrap(), None);
    assert_eq!(b.cache_get("edgar", "older").await.unwrap(), None);
    // Every cached body as stored, at the size limit too.
    let big = row(
        "companies_house",
        &"k".repeat(CACHE_KEY_MAX_LEN),
        Some(&vec![7; CACHE_MAX_BODY]),
        now + 60,
    );
    a.cache_put(&big).await.unwrap();
    assert_eq!(
        b.cache_get("companies_house", &big.key).await.unwrap(),
        Some(big.clone())
    );

    // Icons.
    let icon = |domain: &str, format: Option<&str>, expires_at: i64| IconRow {
        domain: domain.to_owned(),
        icon: format.map(|f| StoredIcon {
            format: f.to_owned(),
            bytes: vec![0x89, b'P', b'N', b'G', 1, 2, 3],
        }),
        fetched_at: now - 5,
        expires_at,
    };
    assert_eq!(a.icon_get("acme.example").await.unwrap(), None);
    let hit = icon("acme.example", Some("png"), now + 86_400);
    a.icon_put(&hit).await.unwrap();
    assert_eq!(b.icon_get("acme.example").await.unwrap(), Some(hit));
    let miss = icon("acme.example", None, now + 3600);
    b.icon_put(&miss).await.unwrap();
    assert_eq!(a.icon_get("acme.example").await.unwrap(), Some(miss));
    a.icon_put(&icon("gone.example", Some("ico"), now - 1))
        .await
        .unwrap();
    assert_eq!(a.icon_get("gone.example").await.unwrap(), None);

    // Three expired rows (two answers, one icon), and nothing else.
    assert_eq!(b.purge_expired().await.unwrap(), 3);
    assert_eq!(a.purge_expired().await.unwrap(), 0);
    assert!(a.cache_get("vies", "check/Ä").await.unwrap().is_some());
    assert!(a.icon_get("acme.example").await.unwrap().is_some());

    // Input checks.
    for bad in [
        row("GLEIF", "k", None, now),
        row("gleif", "", None, now),
        row("gleif", "a\0b", None, now),
        row("gleif", &"k".repeat(CACHE_KEY_MAX_LEN + 1), None, now),
        row("gleif", "k", Some(&vec![0; CACHE_MAX_BODY + 1]), now),
    ] {
        assert!(
            matches!(a.cache_put(&bad).await, Err(Error::Invalid(_))),
            "{:?}",
            (&bad.registry, bad.key.len())
        );
    }
    for bad in [
        icon("Acme.example", None, now),
        icon("acme.example/x", None, now),
        icon("acme.example", Some("svg"), now),
    ] {
        assert!(
            matches!(a.icon_put(&bad).await, Err(Error::Invalid(_))),
            "{bad:?}"
        );
    }
}

/// Company freshness and the "due now" query, across two instances: new companies, the
/// schedule per registry, a doubled document count, leases and paging.
///
/// # Panics
///
/// When the backend misbehaves.
#[allow(clippy::too_many_lines)]
pub async fn freshness<B: Backend + ?Sized>(a: &B, b: &B) {
    let (ia, ib) = (instance("inst-a"), instance("inst-b"));
    let day = Duration::from_hours(24);
    let company = |key: &'static str| async move {
        a.upsert_node(NodeKind::Company, key, &json!({}))
            .await
            .unwrap()
    };
    // Documents 200.. mention companies through `party` edges.
    let mut next_doc = 200u8;
    let mut mention = async |co: Uuid, state: EdgeState| {
        let doc = a
            .upsert_node(NodeKind::Document, &hash(next_doc).to_hex(), &json!({}))
            .await
            .unwrap();
        next_doc += 1;
        a.upsert_edge(&Edge::new(doc, co, EdgeKind::Party, EdgeSource::Document).with_state(state))
            .await
            .unwrap();
    };
    let ours = |due: Vec<DueCompany>, mine: &[Uuid]| -> Vec<DueCompany> {
        due.into_iter()
            .filter(|d| mine.contains(&d.company))
            .collect()
    };

    let quiet = company("fresh-no-docs").await;
    let xco = company("fresh-xco").await;
    mention(xco, EdgeState::Active).await;
    let mine = [quiet, xco];
    // New: due at once, from either instance; a company without documents never is.
    for s in [a, b] {
        assert_eq!(
            ours(s.companies_due(&ia, None, 0).await.unwrap(), &mine),
            [DueCompany {
                company: xco,
                reason: DueReason::New,
                documents: 1,
                due: Vec::new(),
            }]
        );
    }
    assert_eq!(a.enrichment(xco).await.unwrap(), None);

    // Enriched: GLEIF due in 30 days, VIES at once.
    let before = unix_now();
    let first = a
        .record_enrichment(
            xco,
            &[
                ("gleif".to_owned(), day * 30),
                ("vies".to_owned(), Duration::ZERO),
            ],
        )
        .await
        .unwrap();
    let after = unix_now();
    assert_eq!((first.company, first.documents), (xco, 1));
    assert!(
        (before - 1..=after + 1).contains(&first.enriched_at),
        "{first:?}"
    );
    assert_eq!(
        first.next_due.keys().collect::<Vec<_>>(),
        ["gleif", "vies"],
        "{first:?}"
    );
    assert!(
        (first.next_due["gleif"] - first.enriched_at - 30 * 86_400).abs() <= 1,
        "{first:?}"
    );
    assert!(
        (first.next_due["vies"] - first.enriched_at).abs() <= 1,
        "{first:?}"
    );
    assert_eq!(b.enrichment(xco).await.unwrap(), Some(first.clone()));
    assert_eq!(
        ours(b.companies_due(&ib, None, 0).await.unwrap(), &mine),
        [DueCompany {
            company: xco,
            reason: DueReason::Scheduled,
            documents: 1,
            due: vec!["vies".to_owned()],
        }]
    );

    // VIES refreshed for 90 days; GLEIF keeps its time. Nothing is due.
    let e2 = b
        .record_enrichment(xco, &[("vies".to_owned(), day * 90)])
        .await
        .unwrap();
    assert_eq!(e2.next_due["gleif"], first.next_due["gleif"]);
    assert!(e2.next_due["vies"] > first.next_due["vies"]);
    assert!(ours(a.companies_due(&ia, None, 0).await.unwrap(), &mine).is_empty());

    // A retracted mention does not count; a second live one doubles the count.
    mention(xco, EdgeState::Retracted).await;
    assert!(ours(a.companies_due(&ia, None, 0).await.unwrap(), &mine).is_empty());
    mention(xco, EdgeState::Active).await;
    assert_eq!(
        ours(a.companies_due(&ia, None, 0).await.unwrap(), &mine),
        [DueCompany {
            company: xco,
            reason: DueReason::Doubled,
            documents: 2,
            due: Vec::new(),
        }]
    );
    let e3 = a.record_enrichment(xco, &[]).await.unwrap();
    assert_eq!(e3.documents, 2);
    assert_eq!(e3.next_due, e2.next_due);
    // Three is not twice two.
    mention(xco, EdgeState::Active).await;
    assert!(ours(a.companies_due(&ia, None, 0).await.unwrap(), &mine).is_empty());

    // Another instance's lease hides a company until it ends; one's own lease does not.
    let yco = company("fresh-yco").await;
    mention(yco, EdgeState::Active).await;
    let mine = [quiet, xco, yco];
    assert!(
        a.lease_company(yco, &ia, Duration::from_mins(1))
            .await
            .unwrap()
    );
    assert!(ours(b.companies_due(&ib, None, 0).await.unwrap(), &mine).is_empty());
    assert_eq!(
        ours(b.companies_due(&ia, None, 0).await.unwrap(), &mine).len(),
        1
    );
    assert!(a.lease_company(yco, &ia, Duration::ZERO).await.unwrap());
    assert_eq!(
        ours(b.companies_due(&ib, None, 0).await.unwrap(), &mine).len(),
        1
    );

    // Paging: by id, after the token, at most `limit`.
    let mut all = vec![yco];
    for key in ["fresh-p1", "fresh-p2", "fresh-p3", "fresh-p4"] {
        let co = company(key).await;
        mention(co, EdgeState::Active).await;
        all.push(co);
    }
    all.sort();
    let full: Vec<Uuid> = ours(a.companies_due(&ia, None, 1000).await.unwrap(), &all)
        .iter()
        .map(|d| d.company)
        .collect();
    assert_eq!(full, all);
    let mut walked = Vec::new();
    let mut token = None;
    loop {
        let page = a.companies_due(&ia, token, 2).await.unwrap();
        assert!(page.len() <= 2);
        assert!(page.windows(2).all(|w| w[0].company < w[1].company));
        let Some(last) = page.last() else { break };
        token = Some(last.company);
        walked.extend(page.iter().map(|d| d.company).filter(|c| all.contains(c)));
    }
    assert_eq!(walked, all);

    // Errors: no such company, a node of another kind, a bad schedule.
    let doc = a
        .upsert_node(NodeKind::Document, &hash(199).to_hex(), &json!({}))
        .await
        .unwrap();
    for id in [Uuid::now_v7(), doc] {
        assert!(
            matches!(a.record_enrichment(id, &[]).await, Err(Error::NotFound(_))),
            "{id}"
        );
    }
    assert!(matches!(
        a.record_enrichment(xco, &[("Bad".to_owned(), day)]).await,
        Err(Error::Invalid(_))
    ));
    assert!(matches!(
        a.record_enrichment(xco, &[("gleif".to_owned(), Duration::MAX)])
            .await,
        Err(Error::Invalid(_))
    ));
    assert_eq!(a.enrichment(doc).await.unwrap(), None);
}

/// The graph state around some nodes: the nodes, every edge at them (any state), and the
/// aliases they own. What an undo must restore exactly.
type Snapshot = (Vec<Option<Node>>, Vec<StoredEdge>, Vec<Vec<OwnedAlias>>);

async fn snapshot<B: Backend + ?Sized>(s: &B, nodes: &[Uuid]) -> Snapshot {
    let mut out_nodes = Vec::new();
    let mut edges = std::collections::BTreeMap::new();
    let mut aliases = Vec::new();
    let all = NeighbourQuery {
        include_inactive: true,
        ..NeighbourQuery::default()
    };
    for n in nodes {
        out_nodes.push(s.node(*n).await.unwrap());
        for nb in s.neighbours(*n, &all).await.unwrap() {
            edges.insert(nb.edge.id, nb.edge);
        }
        aliases.push(s.aliases_of(*n).await.unwrap());
    }
    (out_nodes, edges.into_values().collect(), aliases)
}

fn actor(kind: MergeActorKind) -> MergeActor {
    MergeActor {
        kind,
        instance: instance("inst-a"),
    }
}

fn merge_req(survivor: Uuid, absorbed: Uuid, kind: MergeActorKind) -> MergeRequest {
    MergeRequest {
        survivor,
        absorbed,
        actor: actor(kind),
        evidence: json!([{"source": "registry_id", "alias": "id:rs_pib:100000001"}]),
        confidence: 0.9,
        probability: 0.995,
    }
}

fn undo_req(merge: Uuid, cascade: bool, dismiss: bool) -> UndoRequest {
    UndoRequest {
        merge,
        actor: actor(MergeActorKind::Manual),
        cascade,
        dismiss,
        reason: "not the same company".to_owned(),
    }
}

fn precondition<T: std::fmt::Debug>(r: crate::Result<T>, says: &str) {
    match r {
        Err(Error::Precondition(m)) => assert!(m.contains(says), "{m:?} lacks {says:?}"),
        other => panic!("expected a precondition failure ({says}), got {other:?}"),
    }
}

/// Merges, the log, undo (exact), later merges, cascade, split and its undo, dismissals.
///
/// # Panics
///
/// When the backend misbehaves.
#[allow(clippy::too_many_lines, clippy::many_single_char_names)]
pub async fn merges<B: Backend + ?Sized>(s: &B) {
    let company = |key: &'static str, props: serde_json::Value| async move {
        s.upsert_node(NodeKind::Company, key, &props).await.unwrap()
    };
    let survivor = company("m-survivor", json!({"name": "ACME", "names": ["ACME"]})).await;
    let absorbed = company(
        "m-absorbed",
        json!({"name": "Acme d.o.o.", "names": ["Acme d.o.o."], "legal_form": "doo",
               "ids": {"rs_pib": ["100000001"]}}),
    )
    .await;
    let parent = company("m-parent", json!({"name": "Holding"})).await;
    let doc = |n: u8| async move {
        s.upsert_node(NodeKind::Document, &hash(n).to_hex(), &json!({}))
            .await
            .unwrap()
    };
    let (d1, d2, d3) = (doc(201).await, doc(202).await, doc(203).await);
    let party = |d: Uuid, c: Uuid, role: &str| {
        Edge::new(d, c, EdgeKind::Party, EdgeSource::Document)
            .with_props(json!({"role": role}))
            .with_confidence(0.8)
    };
    s.upsert_edge(&party(d1, absorbed, "buyer")).await.unwrap();
    s.upsert_edge(&party(d2, survivor, "supplier"))
        .await
        .unwrap();
    // The same document at both: after the merge this one is superseded, not moved.
    let twin = s.upsert_edge(&party(d2, absorbed, "buyer")).await.unwrap();
    s.upsert_edge(&party(d3, absorbed, "buyer").with_state(EdgeState::Retracted))
        .await
        .unwrap();
    s.upsert_edge(
        &Edge::new(parent, absorbed, EdgeKind::ParentOf, EdgeSource::Registry)
            .valid(Some(day(2020, 1, 1)), None)
            .with_evidence(json!([{"registry": "apr", "record": "1"}])),
    )
    .await
    .unwrap();
    // Between the two: stays where it is.
    let between = s
        .upsert_edge(&Edge::new(
            absorbed,
            survivor,
            EdgeKind::TradingNameOf,
            EdgeSource::Document,
        ))
        .await
        .unwrap();
    for (alias, node) in [
        ("id:rs_pib:100000001", absorbed),
        ("name:acme|doo", absorbed),
        ("id:lei:5493001KJTIIGC8Y1R12", survivor),
    ] {
        s.claim_alias(NodeKind::Company, alias, node, &json!({"step": "test"}))
            .await
            .unwrap();
    }
    let nodes = [survivor, absorbed, parent, d1, d2, d3];
    let before = snapshot(s, &nodes).await;

    // Bad requests.
    let r = s
        .merge_companies(&merge_req(survivor, survivor, MergeActorKind::Manual))
        .await;
    assert!(matches!(r, Err(Error::Invalid(_))), "{r:?}");
    let r = s
        .merge_companies(&merge_req(survivor, d1, MergeActorKind::Manual))
        .await;
    assert!(matches!(r, Err(Error::Invalid(_))), "{r:?}");
    let r = s
        .merge_companies(&merge_req(survivor, Uuid::now_v7(), MergeActorKind::Manual))
        .await;
    assert!(matches!(r, Err(Error::NotFound(_))), "{r:?}");

    // Merge: logged, aliases and edges moved, the absorbed node a husk.
    let m = s
        .merge_companies(&merge_req(survivor, absorbed, MergeActorKind::Ingest))
        .await
        .unwrap();
    assert_eq!(m.op, MergeOp::Merge);
    assert_eq!((m.survivor, m.absorbed), (survivor, absorbed));
    assert_eq!(m.actor, actor(MergeActorKind::Ingest));
    assert!((m.probability - 0.995).abs() < 1e-6 && (m.confidence - 0.9).abs() < 1e-6);
    assert_eq!(m.evidence[0]["source"], "registry_id");
    assert!(m.undone.is_none() && m.at > 1_700_000_000);
    assert_eq!(s.merge_entry(m.id).await.unwrap().as_ref(), Some(&m));
    let listed = s
        .list_merges(&MergeQuery {
            company: Some(absorbed),
            ..MergeQuery::default()
        })
        .await
        .unwrap();
    assert_eq!(listed, std::slice::from_ref(&m));
    assert_eq!(
        s.alias(NodeKind::Company, "id:rs_pib:100000001")
            .await
            .unwrap()
            .unwrap()
            .node,
        Some(survivor)
    );
    assert!(s.aliases_of(absorbed).await.unwrap().is_empty());
    assert_eq!(s.aliases_of(survivor).await.unwrap().len(), 3);
    let docs: Vec<Uuid> = s
        .company_documents(survivor)
        .await
        .unwrap()
        .iter()
        .map(|n| n.id)
        .collect();
    assert_eq!(docs.len(), 2);
    assert!(docs.contains(&d1) && docs.contains(&d2));
    assert!(s.company_documents(absorbed).await.unwrap().is_empty());
    let husk = s.node(absorbed).await.unwrap().unwrap();
    assert_eq!(merged_into(&husk), Some(survivor));
    let folded = s.node(survivor).await.unwrap().unwrap().props;
    assert_eq!(folded["names"], json!(["ACME", "Acme d.o.o."]));
    assert_eq!(folded["ids"]["rs_pib"], json!(["100000001"]));
    assert_eq!(folded["legal_form"], "doo");
    assert_eq!(folded["name"], "ACME");
    let at_absorbed = s
        .neighbours(
            absorbed,
            &NeighbourQuery {
                include_inactive: true,
                ..NeighbourQuery::default()
            },
        )
        .await
        .unwrap();
    let ids: BTreeSet<Uuid> = at_absorbed.iter().map(|n| n.edge.id).collect();
    assert_eq!(ids, BTreeSet::from([twin, between]));
    assert!(
        at_absorbed
            .iter()
            .any(|n| n.edge.id == twin && n.edge.edge.state == EdgeState::Superseded)
    );
    precondition(
        s.merge_companies(&merge_req(survivor, absorbed, MergeActorKind::Manual))
            .await,
        "merged into",
    );

    // Undo restores the graph exactly.
    let out = s.undo_merge(&undo_req(m.id, false, false)).await.unwrap();
    assert_eq!(out.undone, [m.id]);
    assert!(out.skipped.is_empty(), "{:?}", out.skipped);
    assert!(out.dismissal.is_none());
    assert_eq!(snapshot(s, &nodes).await, before);
    let undone = s.merge_entry(m.id).await.unwrap().unwrap();
    assert_eq!(
        undone.undone.map(|u| u.actor),
        Some(actor(MergeActorKind::Manual))
    );
    precondition(
        s.undo_merge(&undo_req(m.id, false, false)).await,
        "already undone",
    );
    let r = s.undo_merge(&undo_req(Uuid::now_v7(), false, false)).await;
    assert!(matches!(r, Err(Error::NotFound(_))), "{r:?}");

    // A later merge involving the same companies: refused, or cascaded newest first.
    let top = company("m-top", json!({"name": "Top"})).await;
    let mut nodes2 = nodes.to_vec();
    nodes2.push(top);
    let before2 = snapshot(s, &nodes2).await;
    let m1 = s
        .merge_companies(&merge_req(survivor, absorbed, MergeActorKind::Enrichment))
        .await
        .unwrap();
    let m2 = s
        .merge_companies(&merge_req(top, survivor, MergeActorKind::Manual))
        .await
        .unwrap();
    assert!(m2.seq > m1.seq);
    assert_eq!(
        s.alias(NodeKind::Company, "name:acme|doo")
            .await
            .unwrap()
            .unwrap()
            .node,
        Some(top)
    );
    precondition(
        s.undo_merge(&undo_req(m1.id, false, false)).await,
        &m2.id.to_string(),
    );
    // The refusal changed nothing.
    assert!(
        s.merge_entry(m2.id)
            .await
            .unwrap()
            .unwrap()
            .undone
            .is_none()
    );
    let out = s.undo_merge(&undo_req(m1.id, true, false)).await.unwrap();
    assert_eq!(out.undone, [m2.id, m1.id]);
    assert_eq!(snapshot(s, &nodes2).await, before2);
    // Newest first, with paging.
    let page = s
        .list_merges(&MergeQuery {
            company: Some(survivor),
            before: None,
            limit: 2,
        })
        .await
        .unwrap();
    assert_eq!(
        page.iter().map(|e| e.id).collect::<Vec<_>>(),
        [m2.id, m1.id]
    );
    assert!(page.iter().all(|e| e.undone.is_some()));
    let rest = s
        .list_merges(&MergeQuery {
            company: Some(survivor),
            before: Some(page[1].seq),
            limit: 2,
        })
        .await
        .unwrap();
    assert_eq!(rest.iter().map(|e| e.id).collect::<Vec<_>>(), [m.id]);

    // Split: a new company takes an alias and a document; its undo restores the company.
    let before_split = snapshot(s, &nodes).await;
    let r = s
        .split_company(&SplitRequest {
            company: survivor,
            key: None,
            props: json!({}),
            aliases: vec!["name:acme|doo".to_owned()],
            documents: vec![],
            edges: vec![],
            actor: actor(MergeActorKind::Manual),
            evidence: json!([]),
            dismiss: false,
        })
        .await;
    assert!(
        matches!(r, Err(Error::Invalid(_))),
        "not survivor's alias: {r:?}"
    );
    let r = s
        .split_company(&SplitRequest {
            company: survivor,
            key: None,
            props: json!({}),
            aliases: vec![],
            documents: vec![d1],
            edges: vec![],
            actor: actor(MergeActorKind::Manual),
            evidence: json!([]),
            dismiss: false,
        })
        .await;
    assert!(matches!(r, Err(Error::Invalid(_))), "no party edge: {r:?}");
    assert_eq!(
        snapshot(s, &nodes).await,
        before_split,
        "a refused split is a no-op"
    );
    let sp = s
        .split_company(&SplitRequest {
            company: survivor,
            key: Some("m-split-off".to_owned()),
            props: json!({"name": "ACME Trading"}),
            aliases: vec!["id:lei:5493001KJTIIGC8Y1R12".to_owned()],
            documents: vec![d2],
            edges: vec![],
            actor: actor(MergeActorKind::Manual),
            evidence: json!([{"note": "two companies share a name"}]),
            dismiss: false,
        })
        .await
        .unwrap();
    assert_eq!(sp.op, MergeOp::Split);
    let new = sp.absorbed;
    let node = s.node(new).await.unwrap().unwrap();
    assert_eq!(node.key, "m-split-off");
    assert_eq!(node.props["name"], "ACME Trading");
    assert_eq!(node.props["split_from"], survivor.to_string());
    assert_eq!(
        s.company_documents(new)
            .await
            .unwrap()
            .iter()
            .map(|n| n.id)
            .collect::<Vec<_>>(),
        [d2]
    );
    assert!(s.company_documents(survivor).await.unwrap().is_empty());
    assert_eq!(
        s.alias(NodeKind::Company, "id:lei:5493001KJTIIGC8Y1R12")
            .await
            .unwrap()
            .unwrap()
            .node,
        Some(new)
    );
    let out = s.undo_merge(&undo_req(sp.id, false, false)).await.unwrap();
    assert_eq!(out.undone, [sp.id]);
    assert_eq!(snapshot(s, &nodes).await, before_split);
    assert!(
        s.node(new).await.unwrap().is_none(),
        "the empty split-off is gone"
    );

    // Dismissals: both directions, permanent, and they block merges between the groups.
    let (p, q, r) = (
        company("m-p", json!({})).await,
        company("m-q", json!({})).await,
        company("m-r", json!({})).await,
    );
    let dis = |a: Uuid, b: Uuid| DismissRequest {
        a,
        b,
        actor: actor(MergeActorKind::Manual),
        reason: "different companies".to_owned(),
    };
    let d = s.dismiss_pair(&dis(q, p)).await.unwrap();
    assert_eq!((d.a, d.b), (p.min(q), p.max(q)));
    assert_eq!(d.reason, "different companies");
    assert_eq!(
        s.dismiss_pair(&dis(p, q)).await.unwrap(),
        d,
        "first one kept"
    );
    assert_eq!(s.dismissal(p, q).await.unwrap().as_ref(), Some(&d));
    assert_eq!(s.dismissal(q, p).await.unwrap().as_ref(), Some(&d));
    assert_eq!(s.dismissal(p, r).await.unwrap(), None);
    precondition(
        s.merge_companies(&merge_req(p, q, MergeActorKind::Ingest))
            .await,
        "dismissed",
    );
    precondition(
        s.merge_companies(&merge_req(q, p, MergeActorKind::Enrichment))
            .await,
        "dismissed",
    );
    // Through a group: q merged into r keeps p away from r.
    let qr = s
        .merge_companies(&merge_req(r, q, MergeActorKind::Manual))
        .await
        .unwrap();
    assert_eq!(s.dismissal(p, r).await.unwrap().as_ref(), Some(&d));
    precondition(
        s.merge_companies(&merge_req(r, p, MergeActorKind::Ingest))
            .await,
        "dismissed",
    );
    precondition(s.dismiss_pair(&dis(q, r)).await, "merged now");
    let r_self = s.dismiss_pair(&dis(p, p)).await;
    assert!(matches!(r_self, Err(Error::Invalid(_))), "{r_self:?}");
    // Undo with dismiss: the pair never merges again.
    let out = s.undo_merge(&undo_req(qr.id, false, true)).await.unwrap();
    let rd = out.dismissal.expect("dismissal recorded");
    assert_eq!(rd.merge, Some(qr.id));
    assert_eq!((rd.a, rd.b), (q.min(r), q.max(r)));
    precondition(
        s.merge_companies(&merge_req(r, q, MergeActorKind::Ingest))
            .await,
        "dismissed",
    );
    let listed = s
        .list_dismissals(&DismissalQuery {
            company: Some(q),
            ..DismissalQuery::default()
        })
        .await
        .unwrap();
    assert_eq!(listed, [rd.clone(), d.clone()], "newest first");
    let page = s
        .list_dismissals(&DismissalQuery {
            company: Some(q),
            before: Some(rd.seq),
            limit: 1,
        })
        .await
        .unwrap();
    assert_eq!(page, [d]);
    // A split that dismisses: its undo is refused, the dismissal is permanent.
    let lone = s
        .upsert_node(NodeKind::Company, "m-lone", &json!({}))
        .await
        .unwrap();
    s.claim_alias(NodeKind::Company, "m-lone-alias", lone, &json!({}))
        .await
        .unwrap();
    let sp2 = s
        .split_company(&SplitRequest {
            company: lone,
            key: None,
            props: json!({}),
            aliases: vec!["m-lone-alias".to_owned()],
            documents: vec![],
            edges: vec![],
            actor: actor(MergeActorKind::Manual),
            evidence: json!([]),
            dismiss: true,
        })
        .await
        .unwrap();
    assert!(s.dismissal(lone, sp2.absorbed).await.unwrap().is_some());
    assert!(
        s.node(sp2.absorbed)
            .await
            .unwrap()
            .unwrap()
            .key
            .starts_with("split:")
    );
    precondition(
        s.undo_merge(&undo_req(sp2.id, false, false)).await,
        "dismissed",
    );
}

fn resolve_req(id: Uuid, state: ProposalState, dismissed: bool) -> ResolveRequest {
    ResolveRequest {
        id,
        state,
        actor: actor(MergeActorKind::Manual),
        dismissed,
        merge: None,
        note: "checked".to_owned(),
    }
}

/// Enrichment proposals (issue #52): dedup of open proposals, filters and pages, resolution
/// once, dismissed rejections and dismissed pairs never opened again.
///
/// # Panics
///
/// When the backend misbehaves.
#[allow(clippy::too_many_lines, clippy::many_single_char_names)]
pub async fn proposals<B: Backend + ?Sized>(s: &B, other: &B) {
    let company = |key: &'static str| async move {
        s.upsert_node(NodeKind::Company, key, &json!({"name": key}))
            .await
            .unwrap()
    };
    let (x, y, z) = (
        company("p-x").await,
        company("p-y").await,
        company("p-z").await,
    );
    let merge = |p: f32, reason: ProposalReason| {
        ProposalRequest::merge(
            y,
            x,
            json!({"why": "name"}),
            json!([{"source": "name_ladder", "step": "normalized"}]),
            p,
            reason,
        )
    };
    let record = ProposalRequest::registry_record(
        x,
        "apr",
        "companies/12345678",
        json!({"name": "P-X d.o.o."}),
        json!([{"source": "fuzzy_name", "registry": "apr"}]),
        0.75,
        ProposalReason::ReviewBand,
    );
    // New, then the same again: one open proposal, updated.
    let first = s
        .propose(&merge(0.6, ProposalReason::ReviewBand))
        .await
        .unwrap()
        .expect("opened");
    assert_eq!(first.kind, ProposalKind::Merge);
    assert_eq!(first.company, x.min(y));
    assert_eq!(first.candidate, x.max(y).to_string());
    assert_eq!(first.candidate_company(), Some(x.max(y)));
    assert_eq!(first.state, ProposalState::Open);
    assert_eq!(first.resolution, None);
    assert!(first.created_at > 1_700_000_000);
    let again = other
        .propose(&merge(0.7, ProposalReason::Conflict))
        .await
        .unwrap()
        .expect("updated");
    assert_eq!((again.id, again.seq), (first.id, first.seq), "deduplicated");
    assert!((again.probability - 0.7).abs() < 1e-6);
    assert_eq!(again.reason, ProposalReason::Conflict);
    assert_eq!(again.created_at, first.created_at);
    assert_eq!(s.proposal(first.id).await.unwrap(), Some(again.clone()));
    let rec = s.propose(&record).await.unwrap().expect("opened");
    assert_eq!(rec.registry_record(), Some(("apr", "companies/12345678")));
    assert_eq!(rec.detail, json!({"name": "P-X d.o.o."}));
    assert!(rec.seq > again.seq);
    // Filters and pages, newest first.
    let list = |q: ProposalQuery| async move { s.list_proposals(&q).await.unwrap() };
    let ids = |v: Vec<crate::Proposal>| v.into_iter().map(|p| p.id).collect::<Vec<_>>();
    assert_eq!(
        ids(list(ProposalQuery::default()).await),
        [rec.id, first.id]
    );
    let merges_only = ProposalQuery {
        kind: Some(ProposalKind::Merge),
        ..ProposalQuery::default()
    };
    assert_eq!(ids(list(merges_only).await), [first.id]);
    for (c, want) in [
        (x, vec![rec.id, first.id]),
        (y, vec![first.id]),
        (z, vec![]),
    ] {
        let q = ProposalQuery {
            company: Some(c),
            ..ProposalQuery::default()
        };
        assert_eq!(
            ids(list(q).await),
            want,
            "company filter, either side of a merge"
        );
    }
    let accepted = ProposalQuery {
        state: Some(ProposalState::Accepted),
        ..ProposalQuery::default()
    };
    assert!(list(accepted).await.is_empty());
    let page = list(ProposalQuery {
        limit: 1,
        ..ProposalQuery::default()
    })
    .await;
    assert_eq!(ids(page.clone()), [rec.id]);
    let next = ProposalQuery {
        limit: 1,
        before: Some(page[0].seq),
        ..ProposalQuery::default()
    };
    assert_eq!(ids(list(next).await), [first.id]);
    // Resolved once.
    let mut req = resolve_req(first.id, ProposalState::Accepted, false);
    req.merge = Some(Uuid::now_v7());
    let done = s.resolve_proposal(&req).await.unwrap();
    assert_eq!(done.state, ProposalState::Accepted);
    let r = done.resolution.clone().expect("resolved");
    assert_eq!(r.actor, Some(actor(MergeActorKind::Manual)));
    assert_eq!(
        (r.merge, r.note.as_str(), r.dismissed),
        (req.merge, "checked", false)
    );
    assert!(r.at >= done.created_at);
    precondition(other.resolve_proposal(&req).await, "accepted");
    assert!(matches!(
        s.resolve_proposal(&resolve_req(Uuid::now_v7(), ProposalState::Rejected, false))
            .await,
        Err(Error::NotFound(_))
    ));
    assert!(matches!(
        s.resolve_proposal(&resolve_req(rec.id, ProposalState::Open, false))
            .await,
        Err(Error::Invalid(_))
    ));
    assert!(matches!(
        s.resolve_proposal(&resolve_req(rec.id, ProposalState::Accepted, true))
            .await,
        Err(Error::Invalid(_))
    ));
    assert_eq!(ids(list(accepted).await), [first.id]);
    // A resolved proposal is history: proposing the pair again opens a new one.
    let reopened = s
        .propose(&merge(0.65, ProposalReason::ReviewBand))
        .await
        .unwrap()
        .expect("opened");
    assert_ne!(reopened.id, first.id);
    // A plain rejection: proposed again later, it opens again.
    s.resolve_proposal(&resolve_req(reopened.id, ProposalState::Rejected, false))
        .await
        .unwrap();
    let third = s
        .propose(&merge(0.66, ProposalReason::ReviewBand))
        .await
        .unwrap()
        .expect("opened again");
    assert_ne!(third.id, reopened.id);
    // A dismissed rejection is never opened again.
    let rejected = s
        .resolve_proposal(&resolve_req(rec.id, ProposalState::Rejected, true))
        .await
        .unwrap();
    assert_eq!(rejected.state, ProposalState::Rejected);
    assert!(rejected.resolution.expect("resolved").dismissed);
    assert_eq!(other.propose(&record).await.unwrap(), None);
    // Nor is a merge proposal for a dismissed pair.
    s.dismiss_pair(&DismissRequest {
        a: z,
        b: x,
        actor: actor(MergeActorKind::Manual),
        reason: "different".to_owned(),
    })
    .await
    .unwrap();
    let dismissed =
        ProposalRequest::merge(x, z, json!({}), json!([]), 0.95, ProposalReason::Dismissed);
    assert_eq!(s.propose(&dismissed).await.unwrap(), None);
    // Unknown companies and bad requests.
    let ghost = ProposalRequest::merge(
        x,
        Uuid::now_v7(),
        json!({}),
        json!([]),
        0.6,
        ProposalReason::ReviewBand,
    );
    assert!(matches!(s.propose(&ghost).await, Err(Error::NotFound(_))));
    let mut ghost = record.clone();
    ghost.company = Uuid::now_v7();
    assert!(matches!(s.propose(&ghost).await, Err(Error::NotFound(_))));
    let mut bad = record;
    bad.probability = 2.0;
    assert!(matches!(s.propose(&bad).await, Err(Error::Invalid(_))));
}

/// A person's decisions on proposals (`proposal::accept` and `proposal::reject`): an
/// accepted merge goes through the merge log and is undone; an accepted record adopts its
/// identifier and makes its registry due; a dismissed rejection blocks the merge and the
/// proposal; accepting a dismissed pair fails.
///
/// # Panics
///
/// When the backend misbehaves.
#[allow(clippy::too_many_lines, clippy::many_single_char_names)]
pub async fn decisions<B: Backend + ?Sized>(s: &B) {
    use crate::proposal::{accept, reject};
    let company = |key: &'static str| async move {
        s.upsert_node(NodeKind::Company, key, &json!({"name": key}))
            .await
            .unwrap()
    };
    let person = actor(MergeActorKind::Manual);
    let (a, b, c) = (
        company("d-a").await,
        company("d-b").await,
        company("d-c").await,
    );
    let doc = s
        .upsert_node(NodeKind::Document, &"d".repeat(64), &json!({}))
        .await
        .unwrap();
    s.upsert_edge(&Edge::new(doc, b, EdgeKind::Party, EdgeSource::Document))
        .await
        .unwrap();
    let evidence = json!([{"source": "name_ladder", "step": "form_blind"}]);
    let pair = |x: Uuid, y: Uuid| {
        ProposalRequest::merge(
            x,
            y,
            json!({}),
            evidence.clone(),
            0.85,
            ProposalReason::ReviewBand,
        )
    };
    // Accept a merge: the one with more documents survives; the log has the evidence.
    let p = s.propose(&pair(a, b)).await.unwrap().expect("opened");
    let done = accept(s, p.id, &person, None, "same").await.unwrap();
    let entry = done.merge.expect("merged");
    assert_eq!((entry.survivor, entry.absorbed), (b, a));
    assert_eq!(entry.actor, person);
    assert_eq!(entry.evidence, evidence);
    assert!((entry.probability - 0.85).abs() < 1e-6);
    assert_eq!(done.proposal.state, ProposalState::Accepted);
    let r = done.proposal.resolution.expect("resolved");
    assert_eq!((r.merge, r.note.as_str()), (Some(entry.id), "same"));
    precondition(accept(s, p.id, &person, None, "").await, "accepted");
    // ... and undo it.
    let undone = s
        .undo_merge(&UndoRequest {
            merge: entry.id,
            actor: person.clone(),
            cascade: false,
            dismiss: false,
            reason: String::new(),
        })
        .await
        .unwrap();
    assert_eq!(undone.undone, [entry.id]);
    assert_eq!(merged_into(&s.node(a).await.unwrap().unwrap()), None);
    // A chosen survivor; one that is neither company is refused.
    let p = s.propose(&pair(a, b)).await.unwrap().expect("opened");
    assert!(matches!(
        accept(s, p.id, &person, Some(c), "").await,
        Err(Error::Invalid(_))
    ));
    let done = accept(s, p.id, &person, Some(a), "").await.unwrap();
    let entry = done.merge.expect("merged");
    assert_eq!((entry.survivor, entry.absorbed), (a, b));
    // A pair that is one company already is accepted without a merge.
    let p = s.propose(&pair(a, b)).await.unwrap().expect("opened");
    let done = accept(s, p.id, &person, None, "").await.unwrap();
    assert_eq!(done.merge, None);
    assert_eq!(
        done.proposal.resolution.expect("resolved").note,
        "already one company"
    );
    // Rejected as dismissed: the pair is dismissed, never merged, never proposed again.
    let p = s.propose(&pair(a, c)).await.unwrap().expect("opened");
    let (rejected, d) = reject(s, p.id, &person, true, "different companies")
        .await
        .unwrap();
    assert_eq!(rejected.state, ProposalState::Rejected);
    assert!(rejected.resolution.expect("resolved").dismissed);
    let d = d.expect("dismissed");
    assert_eq!((d.a, d.b), (a.min(c), a.max(c)));
    assert_eq!(d.reason, "different companies");
    assert!(s.dismissal(a, c).await.unwrap().is_some());
    precondition(
        s.merge_companies(&merge_req(a, c, MergeActorKind::Enrichment))
            .await,
        "dismissed",
    );
    assert_eq!(s.propose(&pair(c, a)).await.unwrap(), None);
    // A plain rejection records no dismissal.
    let e = company("d-e").await;
    let p = s.propose(&pair(c, e)).await.unwrap().expect("opened");
    let (_, d) = reject(s, p.id, &person, false, "").await.unwrap();
    assert_eq!(d, None);
    assert!(s.dismissal(c, e).await.unwrap().is_none());
    // A proposal opened before its pair was dismissed cannot be accepted.
    let p = s.propose(&pair(c, e)).await.unwrap().expect("opened");
    s.dismiss_pair(&DismissRequest {
        a: c,
        b: e,
        actor: person.clone(),
        reason: String::new(),
    })
    .await
    .unwrap();
    precondition(accept(s, p.id, &person, None, "").await, "dismissed");
    assert_eq!(
        s.proposal(p.id).await.unwrap().expect("kept").state,
        ProposalState::Open
    );
    // Accept a registry record: its identifier joins the company, its registry is due now.
    let record = |company: Uuid, record: &str| {
        ProposalRequest::registry_record(
            company,
            "apr",
            record,
            json!({"name": "D-C d.o.o."}),
            json!([]),
            0.7,
            ProposalReason::ReviewBand,
        )
    };
    let p = s
        .propose(&record(c, "companies/87654321"))
        .await
        .unwrap()
        .expect("opened");
    let done = accept(s, p.id, &person, None, "").await.unwrap();
    assert_eq!(done.alias.as_deref(), Some("id:rs_mb:87654321"));
    let held = s
        .alias(NodeKind::Company, "id:rs_mb:87654321")
        .await
        .unwrap()
        .expect("claimed");
    assert_eq!(held.node, Some(c));
    assert_eq!(held.props["proposal"], p.id.to_string());
    assert_eq!(
        s.node(c).await.unwrap().unwrap().props["ids"]["rs_mb"],
        json!(["87654321"])
    );
    let fresh = s.enrichment(c).await.unwrap().expect("recorded");
    assert!(
        fresh.next_due["apr"] <= fresh.enriched_at,
        "due now: {fresh:?}"
    );
    // A record whose identifier another company owns is refused; one with no identifier too.
    let p = s
        .propose(&record(e, "companies/87654321"))
        .await
        .unwrap()
        .expect("opened");
    precondition(
        accept(s, p.id, &person, None, "").await,
        "belongs to company",
    );
    let p = s
        .propose(&ProposalRequest::registry_record(
            e,
            "wikidata",
            "Q1",
            json!({}),
            json!([]),
            0.6,
            ProposalReason::ReviewBand,
        ))
        .await
        .unwrap()
        .expect("opened");
    precondition(accept(s, p.id, &person, None, "").await, "no identifier");
    // Unknown proposals.
    assert!(matches!(
        accept(s, Uuid::now_v7(), &person, None, "").await,
        Err(Error::NotFound(_))
    ));
    assert!(matches!(
        reject(s, Uuid::now_v7(), &person, false, "").await,
        Err(Error::NotFound(_))
    ));
}

/// Edges onto live companies (issue #48): a writer resolved an alias to a company, then a
/// merge committed (through another instance), then the writer writes its edge. It lands on
/// the survivor, following chains, at either end; the husk gets nothing.
///
/// # Panics
///
/// When the backend misbehaves.
pub async fn live_edges<B: Backend + ?Sized>(a: &B, b: &B) {
    let company = |key: &'static str| async move {
        a.upsert_node(NodeKind::Company, key, &json!({"name": key}))
            .await
            .unwrap()
    };
    let (absorbed, middle, survivor, holding) = (
        company("live-x").await,
        company("live-y").await,
        company("live-z").await,
        company("live-w").await,
    );
    let doc = |n: u8| async move {
        a.upsert_node(NodeKind::Document, &hash(n).to_hex(), &json!({}))
            .await
            .unwrap()
    };
    let (d1, d2, d3) = (doc(241).await, doc(242).await, doc(243).await);
    let party = |d: Uuid, c: Uuid| {
        Edge::new(d, c, EdgeKind::Party, EdgeSource::Document)
            .with_props(json!({"role": "buyer"}))
            .with_confidence(0.8)
    };
    let parties_at = |c: Uuid| async move {
        let query = NeighbourQuery {
            direction: Direction::In,
            edge_kinds: vec![EdgeKind::Party],
            include_inactive: true,
            ..NeighbourQuery::default()
        };
        let mut docs: Vec<Uuid> = a
            .neighbours(c, &query)
            .await
            .unwrap()
            .iter()
            .map(|n| n.node.id)
            .collect();
        docs.sort_unstable();
        docs
    };

    // Both ends live: written as given.
    let live = a.upsert_live_edge(&party(d1, absorbed)).await.unwrap();
    assert_eq!((live.src, live.dst, live.redirects), (d1, absorbed, 0));
    let again = a.upsert_live_edge(&party(d1, absorbed)).await.unwrap();
    assert_eq!(again, live, "an upsert: same edge, same id");

    // The alias said absorbed; absorbed is merged into middle before the write.
    b.merge_companies(&merge_req(middle, absorbed, MergeActorKind::Enrichment))
        .await
        .unwrap();
    let moved = a.upsert_live_edge(&party(d2, absorbed)).await.unwrap();
    assert_eq!((moved.src, moved.dst, moved.redirects), (d2, middle, 1));
    assert!(parties_at(absorbed).await.is_empty(), "nothing on the husk");

    // A chain: middle into survivor, then a write to absorbed.
    b.merge_companies(&merge_req(survivor, middle, MergeActorKind::Manual))
        .await
        .unwrap();
    let chained = a.upsert_live_edge(&party(d3, absorbed)).await.unwrap();
    assert_eq!((chained.dst, chained.redirects), (survivor, 2));
    assert!(parties_at(absorbed).await.is_empty() && parties_at(middle).await.is_empty());
    let mut all = vec![d1, d2, d3];
    all.sort_unstable();
    assert_eq!(
        parties_at(survivor).await,
        all,
        "the merges moved d1 and d2"
    );

    // Either end of an edge between companies.
    let from_husk = a
        .upsert_live_edge(&Edge::new(
            absorbed,
            holding,
            EdgeKind::ParentOf,
            EdgeSource::Registry,
        ))
        .await
        .unwrap();
    assert_eq!(
        (from_husk.src, from_husk.dst, from_husk.redirects),
        (survivor, holding, 2)
    );
    let to_husk = a
        .upsert_live_edge(&Edge::new(
            holding,
            middle,
            EdgeKind::TradingNameOf,
            EdgeSource::Registry,
        ))
        .await
        .unwrap();
    assert_eq!(
        (to_husk.src, to_husk.dst, to_husk.redirects),
        (holding, survivor, 1)
    );

    // Bad edges and missing nodes fail as `upsert_edge` does.
    let bad = a
        .upsert_live_edge(&party(d1, absorbed).with_confidence(2.0))
        .await;
    assert!(matches!(bad, Err(Error::Invalid(_))), "{bad:?}");
    let missing = a.upsert_live_edge(&party(d1, Uuid::now_v7())).await;
    assert!(matches!(missing, Err(Error::NotFound(_))), "{missing:?}");
}

/// Aliases, props and freshness onto live companies only (issue #62): after a merge committed
/// through another instance, `claim_live_alias`, `update_live_props`, `record_enrichment` and
/// `mark_due` refuse the husk with [`Error::MergedAway`] and write nothing; `mark_due` makes
/// registries due without recording an enrichment.
///
/// # Panics
///
/// When the backend misbehaves.
#[allow(clippy::too_many_lines)]
pub async fn live_writes<B: Backend + ?Sized>(a: &B, b: &B) {
    let company = |key: &'static str| async move {
        a.upsert_node(NodeKind::Company, key, &json!({"name": key}))
            .await
            .unwrap()
    };
    let (husk, survivor, fresh) = (
        company("husk-c").await,
        company("husk-w").await,
        company("husk-n").await,
    );
    let hour = Duration::from_hours(1);
    let schedule = |names: &[&str]| -> Vec<(String, Duration)> {
        names.iter().map(|n| ((*n).to_owned(), hour)).collect()
    };
    let refused = |r: Result<(), Error>, what: &str| match r {
        Err(Error::MergedAway { company, into }) => {
            assert_eq!((company, into), (husk, survivor), "{what}");
        }
        other => panic!("{what}: {other:?}"),
    };
    let evidence = json!({"step": "registry", "confidence": 0.95});

    // Live: the same as the plain calls.
    let got = a
        .claim_live_alias(
            NodeKind::Company,
            "id:lei:HUSKC0000000000000A1",
            husk,
            &evidence,
        )
        .await
        .unwrap();
    assert_eq!(got.node, Some(husk));
    let n = a
        .update_live_props(
            husk,
            &PropsPatch::new().set(&["legal_name"], json!("C d.o.o.")),
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(n.props["legal_name"], json!("C d.o.o."));
    a.record_enrichment(husk, &schedule(&["gleif"]))
        .await
        .unwrap();
    let w0 = a
        .record_enrichment(survivor, &schedule(&["gleif", "wikidata"]))
        .await
        .unwrap();
    assert!(
        a.update_live_props(Uuid::now_v7(), &PropsPatch::new())
            .await
            .unwrap()
            .is_none(),
        "no node: None, as update_props"
    );

    // A person merges C into W on another instance.
    b.merge_companies(&merge_req(survivor, husk, MergeActorKind::Manual))
        .await
        .unwrap();
    let husk_props = a.node(husk).await.unwrap().unwrap().props;
    let husk_fresh = a.enrichment(husk).await.unwrap();
    assert_eq!(
        a.alias(NodeKind::Company, "id:lei:HUSKC0000000000000A1")
            .await
            .unwrap()
            .and_then(|x| x.node),
        Some(survivor),
        "a claim made before the merge was moved by it"
    );

    // Nothing more lands on the husk.
    refused(
        a.claim_live_alias(NodeKind::Company, "name:husk c late|", husk, &evidence)
            .await
            .map(drop),
        "claim_live_alias",
    );
    assert!(
        a.alias(NodeKind::Company, "name:husk c late|")
            .await
            .unwrap()
            .is_none(),
        "not claimed"
    );
    refused(
        a.update_live_props(husk, &PropsPatch::new().set(&["legal_name"], json!("late")))
            .await
            .map(drop),
        "update_live_props",
    );
    refused(
        a.record_enrichment(husk, &schedule(&["gleif", "vies"]))
            .await
            .map(drop),
        "record_enrichment",
    );
    refused(a.mark_due(husk, &["gleif".to_owned()]).await, "mark_due");
    assert_eq!(a.node(husk).await.unwrap().unwrap().props, husk_props);
    assert_eq!(a.enrichment(husk).await.unwrap(), husk_fresh);

    // The survivor: due now for the registries named, its enrichment as it was.
    a.mark_due(survivor, &["gleif".to_owned(), "vies".to_owned()])
        .await
        .unwrap();
    let w1 = a.enrichment(survivor).await.unwrap().unwrap();
    assert_eq!(
        (w1.enriched_at, w1.documents),
        (w0.enriched_at, w0.documents)
    );
    let now = i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs(),
    )
    .unwrap();
    for r in ["gleif", "vies"] {
        assert!(w1.next_due[r] <= now + 1, "{r} due now: {w1:?}");
    }
    assert_eq!(
        w1.next_due["wikidata"], w0.next_due["wikidata"],
        "not named: kept"
    );
    // An earlier due time stays.
    a.mark_due(survivor, &["gleif".to_owned()]).await.unwrap();
    assert!(
        a.enrichment(survivor).await.unwrap().unwrap().next_due["gleif"] <= w1.next_due["gleif"]
    );
    let live = a
        .claim_live_alias(NodeKind::Company, "name:husk w late|", survivor, &evidence)
        .await
        .unwrap();
    assert_eq!(live.node, Some(survivor));

    // Never enriched: due already, nothing recorded.
    a.mark_due(fresh, &["gleif".to_owned()]).await.unwrap();
    assert!(a.enrichment(fresh).await.unwrap().is_none());

    // Bad input and missing companies.
    let bad = a.mark_due(survivor, &["Not A Registry".to_owned()]).await;
    assert!(matches!(bad, Err(Error::Invalid(_))), "{bad:?}");
    let missing = a.mark_due(Uuid::now_v7(), &["gleif".to_owned()]).await;
    assert!(matches!(missing, Err(Error::NotFound(_))), "{missing:?}");
    let doc = a
        .upsert_node(NodeKind::Document, &hash(244).to_hex(), &json!({}))
        .await
        .unwrap();
    let not_company = a.mark_due(doc, &["gleif".to_owned()]).await;
    assert!(
        matches!(not_company, Err(Error::NotFound(_))),
        "{not_company:?}"
    );
}

/// Folds the company graph's whole log, a few rows at a time.
async fn fold_all<B: Backend + ?Sized>(s: &B) {
    for _ in 0..100_000 {
        if !s.fold_company_graph(7).await.expect("fold").more {
            return;
        }
    }
    panic!("the log never emptied");
}

/// Every company of a listing, walked a page of `size` at a time.
async fn walk_companies<B: Backend + ?Sized>(
    s: &B,
    filter: &crate::CompanyFilter,
    order: crate::CompanyOrder,
    size: u32,
) -> Vec<crate::CompanyRow> {
    let (mut out, mut after) = (Vec::new(), None::<crate::CompanyCursor>);
    loop {
        let page = s
            .list_companies(filter, order, after.as_ref(), size, usize::MAX)
            .await
            .expect("list companies");
        let full = page.len() == size as usize;
        after = page.last().map(|r| crate::CompanyCursor::of(order, r));
        // A cursor survives its text form, as a page token does.
        after = after.map(|c| c.to_string().parse().expect("cursor"));
        out.extend(page);
        if !full {
            return out;
        }
    }
}

/// The explorer's reads (issue #172): the company graph's counts through edge writes,
/// retractions and a merge; the listing's orders, filters and pages; the links; and the
/// bounded walks (`Subgraph`, `Path`) with their caps, depth limits and `as_of`.
///
/// # Panics
///
/// When the backend misbehaves.
#[allow(clippy::too_many_lines, clippy::many_single_char_names)]
pub async fn explorer<B: Backend + ?Sized>(s: &B) {
    use crate::explore::{self, PathQuery, SubgraphQuery};
    use crate::{CompanyFilter, CompanyOrder};

    let company = async |key: &str, props: serde_json::Value| {
        s.upsert_node(NodeKind::Company, key, &props).await.unwrap()
    };
    // The document total is the whole graph's: earlier cases wrote documents too.
    fold_all(s).await;
    let total0 = s.company_documents_total().await.unwrap();
    let a = company(
        "explore-a",
        json!({"name": "Explore Alpha", "names": ["Explore Alpha", "EA Ltd"],
               "jurisdiction": "gb", "website": {"url": "https://alpha.example/"},
               "icon": {"domain": "alpha.example"}}),
    )
    .await;
    let b = company("explore-b", json!({"name": "Explore Beta"})).await;
    let c = company("explore-c", json!({"name": "explore gamma"})).await;
    let d = company("explore-d", json!({"name": "Explore Delta"})).await;
    let e = company("explore-e", json!({"name": "Explore Echo"})).await;
    s.claim_alias(
        NodeKind::Company,
        "id:lei:EXPLOREALPHA0000001",
        a,
        &json!({}),
    )
    .await
    .unwrap();
    s.record_enrichment(a, &[]).await.unwrap();
    let mut docs = Vec::new();
    for i in 0..6u32 {
        docs.push(
            s.upsert_node(
                NodeKind::Document,
                &format!("{:064x}", 0x0172_0000 + i),
                &json!({"n": i}),
            )
            .await
            .unwrap(),
        );
    }
    let party = |doc: Uuid, co: Uuid, role: &str| {
        Edge::new(doc, co, EdgeKind::Party, EdgeSource::Document).with_props(json!({"role": role}))
    };
    // d0: A buyer, B supplier; d1: A, B, C; d2: A, C; d3: B alone; d4: D, retracted;
    // d5: E and A (E is merged into C below).
    let edges = [
        party(docs[0], a, "buyer"),
        party(docs[0], b, "supplier"),
        party(docs[1], a, "buyer"),
        party(docs[1], b, "supplier"),
        party(docs[1], c, "signatory"),
        party(docs[2], a, "buyer"),
        party(docs[2], c, "supplier"),
        party(docs[3], b, "buyer"),
        party(docs[4], d, "buyer").with_state(EdgeState::Retracted),
        party(docs[5], e, "buyer"),
        party(docs[5], a, "supplier"),
        // A second edge from d1 to A: A still counts d1 once.
        Edge::new(docs[1], a, EdgeKind::Party, EdgeSource::Registry)
            .with_props(json!({"role": "owner"})),
    ];
    s.upsert_edges(&edges).await.unwrap();
    // Corporate events: A was C's parent in 2020 and 2021; D acquired B from mid-2023.
    let parent = s
        .upsert_edge(
            &Edge::new(a, c, EdgeKind::ParentOf, EdgeSource::Registry)
                .with_evidence(json!([{"registry": "gleif"}]))
                .valid(Some(day(2020, 1, 1)), Some(day(2022, 1, 1))),
        )
        .await
        .unwrap();
    s.upsert_edge(
        &Edge::new(d, b, EdgeKind::Acquired, EdgeSource::Registry)
            .valid(Some(day(2023, 6, 1)), None),
    )
    .await
    .unwrap();
    // Writing an edge again as it was changes nothing.
    s.upsert_edge(&party(docs[0], a, "buyer")).await.unwrap();
    s.merge_companies(&MergeRequest {
        survivor: c,
        absorbed: e,
        actor: MergeActor {
            kind: MergeActorKind::Manual,
            instance: instance("explore"),
        },
        evidence: json!([]),
        confidence: 1.0,
        probability: 1.0,
    })
    .await
    .unwrap();
    fold_all(s).await;
    assert!(!s.fold_company_graph(100).await.unwrap().more);
    // d0, d1, d2, d3 and d5 once each, though d1 names three companies (and A twice); d4's
    // only edge is retracted.
    assert_eq!(s.company_documents_total().await.unwrap(), total0 + 5);

    let mine = CompanyFilter {
        name_prefix: Some("EXPLORE ".to_owned()),
        ..CompanyFilter::default()
    };
    let counts = |rows: &[crate::CompanyRow]| -> Vec<(Uuid, u64, u64)> {
        rows.iter()
            .map(|r| (r.node.id, r.documents, r.degree))
            .collect()
    };
    // Documents: A d0 d1 d2 d5 = 4; B d0 d1 d3 = 3; C d1 d2 d5 (from E) = 3; D none.
    // Links: A-B (d0, d1), A-C (d1, d2, d5; and the event), B-C (d1), B-D (the event).
    let mut want = vec![(a, 4, 2), (b, 3, 3), (c, 3, 2), (d, 0, 1)];
    let by_docs = walk_companies(s, &mine, CompanyOrder::Documents, 1000).await;
    want.sort_by_key(|x| std::cmp::Reverse((x.1, x.0)));
    assert_eq!(
        counts(&by_docs),
        want,
        "documents order; the husk E is not listed"
    );
    for size in [1, 2, 3] {
        assert_eq!(
            counts(&walk_companies(s, &mine, CompanyOrder::Documents, size).await),
            want,
            "pages of {size}"
        );
    }
    want.sort_by_key(|x| std::cmp::Reverse((x.2, x.0)));
    for size in [1, 1000] {
        assert_eq!(
            counts(&walk_companies(s, &mine, CompanyOrder::Degree, size).await),
            want,
            "degree order, pages of {size}"
        );
    }
    for size in [1, 3, 1000] {
        let names: Vec<Uuid> = walk_companies(s, &mine, CompanyOrder::Name, size)
            .await
            .iter()
            .map(|r| r.node.id)
            .collect();
        assert_eq!(
            names,
            [a, b, d, c],
            "name order, ignoring case, pages of {size}"
        );
    }
    let alpha = &by_docs.iter().find(|r| r.node.id == a).unwrap();
    assert!(alpha.enriched);
    assert_eq!(alpha.identifiers, ["id:lei:EXPLOREALPHA0000001"]);
    let only = async |f: CompanyFilter| -> BTreeSet<Uuid> {
        walk_companies(s, &f, CompanyOrder::Name, 1000)
            .await
            .iter()
            .map(|r| r.node.id)
            .collect()
    };
    assert_eq!(
        only(CompanyFilter {
            jurisdiction: Some("GB".to_owned()),
            ..mine.clone()
        })
        .await,
        BTreeSet::from([a])
    );
    assert_eq!(
        only(CompanyFilter {
            has_identifier: Some(true),
            ..mine.clone()
        })
        .await,
        BTreeSet::from([a])
    );
    assert_eq!(
        only(CompanyFilter {
            has_identifier: Some(false),
            enriched: Some(false),
            ..mine.clone()
        })
        .await,
        BTreeSet::from([b, c, d])
    );
    assert_eq!(
        only(CompanyFilter {
            name_prefix: Some("explore g".to_owned()),
            ..CompanyFilter::default()
        })
        .await,
        BTreeSet::from([c])
    );
    assert!(matches!(
        s.list_companies(
            &CompanyFilter {
                name_prefix: Some("x".repeat(explore::NAME_PREFIX_MAX_LEN + 1)),
                ..CompanyFilter::default()
            },
            CompanyOrder::Name,
            None,
            10,
            usize::MAX
        )
        .await,
        Err(Error::Invalid(_))
    ));

    // Links: strongest first; a pair with an event is kept below the weight.
    let pair = |x: Uuid, y: Uuid| if x < y { (x, y) } else { (y, x) };
    let links = s.company_links(&[a, b, c, d], 2, 100).await.unwrap();
    let got: Vec<((Uuid, Uuid), u64, u64)> = links
        .iter()
        .map(|l| ((l.a, l.b), l.documents, l.events))
        .collect();
    assert_eq!(
        got,
        [(pair(a, c), 3, 1), (pair(a, b), 2, 0), (pair(b, d), 0, 1)]
    );
    let ab = &links[1];
    let role = |co: Uuid, r: &str| {
        ab.roles
            .iter()
            .find(|x| x.company == co && x.role == r)
            .map(|x| x.documents)
    };
    assert_eq!(role(a, "buyer"), Some(2));
    assert_eq!(role(a, "owner"), Some(1));
    assert_eq!(role(b, "supplier"), Some(2));
    assert_eq!(s.company_links(&[a, b, c, d], 1, 2).await.unwrap().len(), 2);
    assert_eq!(s.company_links(&[a, d], 1, 100).await.unwrap().len(), 0);

    // The company graph: caps say `truncated`.
    let g = explore::company_graph(s, &mine, 1, 10, 100).await.unwrap();
    assert!(!g.truncated && !g.stale);
    assert_eq!(g.companies.len(), 4);
    assert_eq!(g.links.len(), 4);
    let events = &g.events[&pair(a, c)];
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].id, parent);
    let g = explore::company_graph(s, &mine, 1, 3, 100).await.unwrap();
    assert!(g.truncated, "four companies match");
    assert_eq!(g.companies.len(), 3);
    assert!(g.links.iter().all(|l| l.a != d && l.b != d));
    let g = explore::company_graph(s, &mine, 1, 10, 2).await.unwrap();
    assert!(g.truncated, "four links");
    assert_eq!(g.links.len(), 2);

    // Counts follow retractions: d3's only edge to B goes; then comes back.
    let d3b = party(docs[3], b, "buyer");
    s.upsert_edge(&d3b.clone().with_state(EdgeState::Retracted))
        .await
        .unwrap();
    fold_all(s).await;
    let count_of_b = async || {
        walk_companies(s, &mine, CompanyOrder::Documents, 1000)
            .await
            .iter()
            .find(|r| r.node.id == b)
            .unwrap()
            .documents
    };
    assert_eq!(count_of_b().await, 2);
    assert_eq!(
        s.company_documents_total().await.unwrap(),
        total0 + 4,
        "d3 lost its last party edge"
    );
    let g = explore::company_graph(s, &mine, 1, 10, 100).await.unwrap();
    assert_eq!(g.documents_total, total0 + 4);
    s.upsert_edge(&d3b).await.unwrap();
    fold_all(s).await;
    assert_eq!(count_of_b().await, 3);
    assert_eq!(s.company_documents_total().await.unwrap(), total0 + 5);

    // Subgraph: A's neighbours (four documents and C by the event), then the caps.
    let sub = |seeds: Vec<Uuid>, depth: u32| SubgraphQuery {
        seeds,
        depth,
        node_kinds: Vec::new(),
        edge_kinds: Vec::new(),
        max_nodes: 100,
        max_edges: 100,
        as_of: None,
    };
    let g = explore::subgraph(s, &sub(vec![a], 1)).await.unwrap();
    assert!(!g.truncated);
    let ids: BTreeSet<Uuid> = g.nodes.iter().map(|r| r.node.id).collect();
    assert_eq!(
        ids,
        BTreeSet::from([a, c, docs[0], docs[1], docs[2], docs[5]])
    );
    assert_eq!(g.nodes[0].node.id, a);
    assert_eq!(g.nodes[0].depth, 0);
    assert!(g.nodes[1..].iter().all(|r| r.depth == 1));
    // Two edges from d1 to A, and the event.
    assert_eq!(g.edges.len(), 6);
    let g = explore::subgraph(s, &sub(vec![a], 0)).await.unwrap();
    assert_eq!((g.nodes.len(), g.edges.len()), (1, 0));
    let g = explore::subgraph(
        s,
        &SubgraphQuery {
            max_nodes: 3,
            ..sub(vec![a], 2)
        },
    )
    .await
    .unwrap();
    assert!(g.truncated);
    assert_eq!(g.nodes.len(), 3);
    let g = explore::subgraph(
        s,
        &SubgraphQuery {
            max_edges: 2,
            ..sub(vec![a], 1)
        },
    )
    .await
    .unwrap();
    assert!(g.truncated);
    assert!(g.edges.len() <= 2);
    // Depth 2 over companies only: A reaches C by the event, and no further.
    let g = explore::subgraph(
        s,
        &SubgraphQuery {
            node_kinds: vec![NodeKind::Company],
            ..sub(vec![a], 2)
        },
    )
    .await
    .unwrap();
    let ids: BTreeSet<Uuid> = g.nodes.iter().map(|r| r.node.id).collect();
    assert_eq!(ids, BTreeSet::from([a, c]));
    // As of a day: the parent edge held in 2021, not in 2023.
    for (on, want) in [(day(2021, 6, 1), 2), (day(2023, 1, 1), 1)] {
        let g = explore::subgraph(
            s,
            &SubgraphQuery {
                edge_kinds: vec![EdgeKind::ParentOf],
                as_of: Some(on),
                ..sub(vec![a], 1)
            },
        )
        .await
        .unwrap();
        assert_eq!(g.nodes.len(), want, "as of {on}");
    }
    assert!(matches!(
        explore::subgraph(s, &sub(vec![Uuid::from_u128(0x172)], 1)).await,
        Err(Error::NotFound(_))
    ));

    // Paths: A to D is three edges, through d0 or d1 to B, then the acquisition: three paths,
    // as d1 has two edges to A.
    let path = |from: Uuid, to: Uuid| PathQuery {
        from,
        to,
        max_depth: 6,
        edge_kinds: Vec::new(),
        as_of: None,
        max_paths: 10,
    };
    let found = explore::shortest_paths(s, &path(a, d)).await.unwrap();
    assert!(!found.truncated);
    assert_eq!(found.paths.len(), 3, "{found:?}");
    let mut middles = BTreeSet::new();
    for p in &found.paths {
        assert_eq!(p.edges.len(), 3);
        let ids: Vec<Uuid> = p.nodes.iter().map(|n| n.id).collect();
        assert_eq!((ids[0], ids[2], ids[3]), (a, b, d));
        middles.insert(ids[1]);
        for (i, e) in p.edges.iter().enumerate() {
            let ends = BTreeSet::from([e.edge.src, e.edge.dst]);
            assert_eq!(ends, BTreeSet::from([ids[i], ids[i + 1]]));
        }
    }
    assert_eq!(middles, BTreeSet::from([docs[0], docs[1]]));
    let one = explore::shortest_paths(
        s,
        &PathQuery {
            max_paths: 1,
            ..path(d, a)
        },
    )
    .await
    .unwrap();
    assert_eq!(one.paths.len(), 1);
    assert_eq!(one.paths[0].nodes[0].id, d);
    // Not within two edges; not over party edges alone; not before the acquisition.
    for q in [
        PathQuery {
            max_depth: 2,
            ..path(a, d)
        },
        PathQuery {
            edge_kinds: vec![EdgeKind::Party],
            ..path(a, d)
        },
        PathQuery {
            as_of: Some(day(2023, 1, 1)),
            ..path(a, d)
        },
    ] {
        let found = explore::shortest_paths(s, &q).await.unwrap();
        assert!(found.paths.is_empty() && !found.truncated, "{q:?}");
    }
    // After the acquisition it holds.
    let found = explore::shortest_paths(
        s,
        &PathQuery {
            as_of: Some(day(2024, 1, 1)),
            ..path(a, d)
        },
    )
    .await
    .unwrap();
    assert_eq!(found.paths.len(), 3);
    // The retracted edge is not followed: D and d4 are not connected.
    let found = explore::shortest_paths(s, &path(d, docs[4])).await.unwrap();
    assert!(found.paths.is_empty());
    let found = explore::shortest_paths(s, &path(a, a)).await.unwrap();
    assert_eq!(found.paths.len(), 1);
    assert!(found.paths[0].edges.is_empty());
    assert!(matches!(
        explore::shortest_paths(s, &path(a, Uuid::from_u128(0x172))).await,
        Err(Error::NotFound(_))
    ));
}

/// Times the explorer's reads on a seeded store (issue #172; a measurement, not a check):
/// the first fold of the whole log, `CompanyGraph` at its defaults, the first `ListCompanies`
/// page of each order and a page deep in the document order, a two-level `Subgraph` around
/// `hub` and a `Path` between `from` and `to`. Prints one line a read: the median of `runs`.
///
/// # Panics
///
/// When a read fails.
#[allow(clippy::too_many_lines)]
pub async fn explorer_timings<B: Backend + ?Sized>(
    s: &B,
    hub: Uuid,
    from: Uuid,
    to: Uuid,
    runs: usize,
) {
    use std::time::Instant;

    use crate::explore::{self, PathQuery, SubgraphQuery};
    use crate::{CompanyFilter, CompanyOrder};

    let t = Instant::now();
    let mut rows = 0;
    loop {
        let f = s
            .fold_company_graph(explore::FOLD_BATCH)
            .await
            .expect("fold");
        rows += f.rows;
        if !f.more {
            break;
        }
    }
    println!(
        "fold of the whole log: {rows} rows in {:.1} s ({:.0} rows/s)",
        t.elapsed().as_secs_f64(),
        f64::from(u32::try_from(rows).unwrap_or(u32::MAX)) / t.elapsed().as_secs_f64()
    );
    // The median of `runs` runs of an async block (after one warm-up), and its last answer.
    macro_rules! median {
        ($body:expr) => {{
            let mut ms = Vec::new();
            let mut what = String::new();
            for _ in 0..=runs {
                let t = Instant::now();
                what = $body.await;
                ms.push(t.elapsed().as_secs_f64() * 1000.0);
            }
            ms.remove(0);
            ms.sort_by(f64::total_cmp);
            (ms[ms.len() / 2], what)
        }};
    }
    let all = CompanyFilter::default();
    let (ms, what) = median!(async {
        let g = explore::company_graph(
            s,
            &all,
            1,
            explore::COMPANY_GRAPH_NODES_DEFAULT,
            explore::COMPANY_GRAPH_LINKS_DEFAULT,
        )
        .await
        .expect("company graph");
        format!(
            "{} companies, {} links, truncated {}",
            g.companies.len(),
            g.links.len(),
            g.truncated
        )
    });
    println!("CompanyGraph (500 companies, 5000 links) | {ms:.1} ms | {what}");
    let (ms, what) = median!(async {
        let g = explore::company_graph(
            s,
            &all,
            1,
            explore::COMPANY_GRAPH_NODES_MAX,
            explore::COMPANY_GRAPH_LINKS_MAX,
        )
        .await
        .expect("company graph");
        format!(
            "{} companies, {} links, truncated {}",
            g.companies.len(),
            g.links.len(),
            g.truncated
        )
    });
    println!("CompanyGraph (2000 companies, 20000 links) | {ms:.1} ms | {what}");
    for order in [
        CompanyOrder::Documents,
        CompanyOrder::Degree,
        CompanyOrder::Name,
    ] {
        let (ms, what) = median!(async {
            let page = s
                .list_companies(&all, order, None, 50, crate::RECORDS_PAGE_BYTES)
                .await
                .expect("list");
            format!("{} companies", page.len())
        });
        println!("ListCompanies {order:?}, first page of 50 | {ms:.1} ms | {what}");
    }
    // A page deep in the document order: the cursor of the 10,000th company.
    let mut after = None;
    for _ in 0..10 {
        let page = s
            .list_companies(
                &all,
                CompanyOrder::Documents,
                after.as_ref(),
                1000,
                usize::MAX,
            )
            .await
            .expect("list");
        after = page
            .last()
            .map(|r| crate::CompanyCursor::of(CompanyOrder::Documents, r));
    }
    let (ms, what) = median!(async {
        let page = s
            .list_companies(
                &all,
                CompanyOrder::Documents,
                after.as_ref(),
                50,
                usize::MAX,
            )
            .await
            .expect("list");
        format!("{} companies", page.len())
    });
    println!("ListCompanies Documents, page 50 after 10,000 | {ms:.1} ms | {what}");
    let (ms, what) = median!(async {
        let g = explore::subgraph(
            s,
            &SubgraphQuery {
                seeds: vec![hub],
                depth: 2,
                node_kinds: Vec::new(),
                edge_kinds: Vec::new(),
                max_nodes: explore::SUBGRAPH_NODES_DEFAULT,
                max_edges: explore::SUBGRAPH_EDGES_DEFAULT,
                as_of: None,
            },
        )
        .await
        .expect("subgraph");
        format!(
            "{} nodes, {} edges, truncated {}",
            g.nodes.len(),
            g.edges.len(),
            g.truncated
        )
    });
    println!("Subgraph (hub, depth 2, defaults) | {ms:.1} ms | {what}");
    let (ms, what) = median!(async {
        let p = explore::shortest_paths(
            s,
            &PathQuery {
                from,
                to,
                max_depth: explore::PATH_DEPTH_DEFAULT,
                edge_kinds: Vec::new(),
                as_of: None,
                max_paths: 1,
            },
        )
        .await
        .expect("path");
        format!(
            "{} paths of {} edges, truncated {}",
            p.paths.len(),
            p.paths.first().map_or(0, |p| p.edges.len()),
            p.truncated
        )
    });
    println!("Path (two small companies, depth 4) | {ms:.1} ms | {what}");
}

/// A hash for the purge cases: `tag` then `n`, so a case's hashes sort together.
fn purge_hash(tag: u8, n: u8) -> Blake3 {
    let mut h = [tag; 32];
    h[31] = n;
    Blake3(h)
}

fn purge_at(key: &str) -> ObjectLocation {
    ObjectLocation {
        endpoint: "http://purge.test:9000".to_owned(),
        bucket: "docs".to_owned(),
        key: key.to_owned(),
    }
}

/// Every batch of a source purge, as the job runs them: the live locations, then the ended
/// ones under the source. Returns the counts.
async fn purge_source_all<B: Backend + ?Sized>(
    s: &B,
    key: &SourceKey,
    limit: u32,
) -> crate::Purged {
    let mut total = crate::Purged::default();
    loop {
        let p = s.purge_source_locations(key, limit).await.unwrap();
        total.add(&p);
        if p.next.is_none() {
            break;
        }
    }
    let mut after = None;
    loop {
        let p = s
            .purge_ended_locations(Some(key), after.as_deref(), limit)
            .await
            .unwrap();
        total.add(&p);
        match p.next {
            Some(n) => after = Some(n),
            None => break,
        }
    }
    total
}

/// Every batch of an orphan purge: the records, then the ended locations.
async fn purge_orphans_all<B: Backend + ?Sized>(s: &B, limit: u32) -> crate::Purged {
    let mut total = crate::Purged::default();
    let mut after = None;
    loop {
        let p = s
            .purge_orphan_records(after.as_deref(), limit)
            .await
            .unwrap();
        total.add(&p);
        total.hashes.extend(p.hashes);
        match p.next {
            Some(n) => after = Some(n),
            None => break,
        }
    }
    let mut after = None;
    loop {
        let p = s
            .purge_ended_locations(None, after.as_deref(), limit)
            .await
            .unwrap();
        total.add(&p);
        match p.next {
            Some(n) => after = Some(n),
            None => break,
        }
    }
    total
}

/// Purges (issue #168): a source purge deletes what only its locations held (records, search
/// rows, sketches, Document and Location nodes, the companies only its documents named and
/// their husks, pending references left alone) and keeps what another location still holds;
/// a live document's reference to a purged one goes back to a pending node; a registry's
/// company stays; an orphan purge deletes the records and ended locations left; both are
/// idempotent; the LLM writer cannot bring a purged record back.
///
/// # Panics
///
/// When the backend misbehaves.
#[allow(clippy::too_many_lines)]
pub async fn purge<B: Backend + ?Sized>(s: &B) {
    let h = |n| purge_hash(0xd1, n);
    let body = |n: u8, title: &str| raw(&json!({"n": n, "title": title}));
    // 1: only under the purged prefix; 2: there and elsewhere; 3: outside it, citing 1;
    // 4: under it, its object already deleted; 5: a record without a location.
    for n in 1..=5u8 {
        s.put_record(&h(n), &body(n, "purgeable"), &versions("1"))
            .await
            .unwrap();
    }
    let d1 = s
        .upsert_location(&h(1), &purge_at("pa/1.pdf"))
        .await
        .unwrap();
    assert!(d1.record);
    let d2 = s
        .upsert_location(&h(2), &purge_at("pa/2.pdf"))
        .await
        .unwrap();
    s.upsert_location(&h(2), &purge_at("other/2.pdf"))
        .await
        .unwrap();
    let d3 = s
        .upsert_location(&h(3), &purge_at("pb/3.pdf"))
        .await
        .unwrap();
    // 3's older copy, gone from its bucket: an ended location of a live document.
    let old3 = s
        .upsert_location(&h(3), &purge_at("pc/3.pdf"))
        .await
        .unwrap();
    s.end_location(&purge_at("pc/3.pdf"), day(2026, 1, 2))
        .await
        .unwrap();
    s.upsert_location(&h(4), &purge_at("pa/4.pdf"))
        .await
        .unwrap();
    s.end_location(&purge_at("pa/4.pdf"), day(2026, 1, 2))
        .await
        .unwrap();
    for n in 1..=3 {
        s.index_document(&SearchDoc {
            blake3: h(n),
            title: format!("Purgeable {n}"),
            body: "zebrafinch".to_owned(),
            kind: None,
            date: None,
            amount: None,
        })
        .await
        .unwrap();
    }
    // Their text (issue #166): it goes with the record.
    for n in [1, 2, 4] {
        let mut b = TextBuilder::new("docx.extract", "2", TEXT_MAX_BYTES);
        assert!(b.page(0, None, TextOrigin::Native));
        assert!(b.block(TextBlock {
            kind: BlockKind::Paragraph,
            text: format!("purgeable text {n}"),
            zone: None,
            level: None,
            table: None,
            row: None,
        }));
        s.put_document_text(&h(n), &b.finish().to_body().unwrap())
            .await
            .unwrap();
    }
    s.put_sketch(&h(1), &[1, 2, 3], &[0xd1_0001, 0xd1_0002])
        .await
        .unwrap();
    s.put_sketch(&h(2), &[1, 2, 4], &[0xd1_0001, 0xd1_0003])
        .await
        .unwrap();
    let company = |key: &'static str, props: serde_json::Value| async move {
        s.upsert_node(NodeKind::Company, key, &props).await.unwrap()
    };
    let only1 = company("purge:only1", json!({"name": "Only One"})).await;
    let both = company("purge:both", json!({"name": "Both"})).await;
    let registry = company(
        "purge:reg",
        json!({"name": "Registry", "origin": "registry"}),
    )
    .await;
    let husk = company("purge:husk", json!({"name": "Husk"})).await;
    s.claim_alias(NodeKind::Company, "name:purge only one", only1, &json!({}))
        .await
        .unwrap();
    let party = |doc: Uuid, c: Uuid, role: &str| {
        Edge::new(doc, c, EdgeKind::Party, EdgeSource::Document).with_props(json!({"role": role}))
    };
    for e in [
        party(d1.document, only1, "buyer"),
        party(d1.document, both, "supplier"),
        party(d3.document, both, "buyer"),
        party(d1.document, registry, "signatory"),
    ] {
        s.upsert_edge(&e).await.unwrap();
    }
    let merge = s
        .merge_companies(&merge_req(only1, husk, MergeActorKind::Manual))
        .await
        .unwrap();
    // 1 has a number; 3 cites it; 1 cites a number nobody has.
    s.claim_alias(
        NodeKind::Document,
        "number:PRG-1",
        d1.document,
        &json!({"document": h(1).to_hex()}),
    )
    .await
    .unwrap();
    let cite = Edge::new(
        d3.document,
        d1.document,
        EdgeKind::Cites,
        EdgeSource::Document,
    )
    .with_props(json!({"relation": "cites", "raw": "PRG-1", "key": "PRG-1", "kind": "doc"}));
    s.upsert_edge(&cite).await.unwrap();
    let ghost = s
        .upsert_node(
            NodeKind::Document,
            "ref:PRG-404",
            &json!({"pending": true, "key": "PRG-404"}),
        )
        .await
        .unwrap();
    s.upsert_edge(
        &Edge::new(d1.document, ghost, EdgeKind::Cites, EdgeSource::Document)
            .with_props(json!({"key": "PRG-404", "pending": true})),
    )
    .await
    .unwrap();
    s.upsert_edge(&Edge::new(
        d1.document,
        d2.document,
        EdgeKind::SimilarTo,
        EdgeSource::Document,
    ))
    .await
    .unwrap();

    // The source "pa/": batches of one, to cross every batch boundary.
    let src = SourceKey::new("http://purge.test:9000/", "docs", "pa/").unwrap();
    let done = purge_source_all(s, &src, 1).await;
    assert_eq!(done.records, 2, "{done:?}");
    assert!(done.locations >= 3, "{done:?}");
    assert_eq!(done.companies, 2, "only1 and its husk: {done:?}");
    assert!(done.other_documents >= 1, "the pending reference: {done:?}");
    for n in [1, 4] {
        assert_eq!(s.get_record(&h(n)).await.unwrap(), None, "record {n}");
        assert!(
            s.get_document_text(&h(n)).await.unwrap().is_none(),
            "text {n}"
        );
        assert!(
            s.node_by_key(NodeKind::Document, &h(n).to_hex())
                .await
                .unwrap()
                .is_none(),
            "document {n}"
        );
    }
    for key in ["pa/1.pdf", "pa/2.pdf", "pa/4.pdf"] {
        assert!(
            s.node_by_key(NodeKind::Location, &purge_at(key).node_key())
                .await
                .unwrap()
                .is_none(),
            "location {key}"
        );
    }
    // 2 stays, held elsewhere, with its text; its edge to 1 is gone with 1.
    assert!(s.get_record(&h(2)).await.unwrap().is_some());
    assert!(s.get_document_text(&h(2)).await.unwrap().is_some());
    let out = s
        .neighbours(
            d2.document,
            &NeighbourQuery {
                direction: Direction::Both,
                include_inactive: true,
                ..NeighbourQuery::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(
        out.iter().map(|n| n.edge.edge.kind).collect::<Vec<_>>(),
        [EdgeKind::LocatedAt],
        "{out:?}"
    );
    let hits = s
        .search(&SearchQuery {
            text: Some("zebrafinch".to_owned()),
            ..SearchQuery::default()
        })
        .await
        .unwrap();
    let mut found: Vec<_> = hits.iter().map(|x| x.blake3).collect();
    found.sort();
    assert_eq!(found, [h(2), h(3)]);
    let similar = s
        .sketch_candidates(&[0xd1_0001, 0xd1_0002], &h(9), 10)
        .await
        .unwrap();
    assert_eq!(similar.iter().map(|c| c.blake3).collect::<Vec<_>>(), [h(2)]);
    // Companies: the one only 1 named goes with its husk, and its aliases; the others stay.
    assert!(s.node(only1).await.unwrap().is_none());
    assert!(s.node(husk).await.unwrap().is_none());
    assert!(
        s.alias(NodeKind::Company, "name:purge only one")
            .await
            .unwrap()
            .is_none()
    );
    assert!(s.node(both).await.unwrap().is_some());
    assert!(
        s.node(registry).await.unwrap().is_some(),
        "a registry's company"
    );
    // The merge log keeps the entry; undoing it brings neither company back.
    let _ = s.undo_merge(&undo_req(merge.id, false, false)).await;
    assert!(s.node(only1).await.unwrap().is_none());
    assert!(s.node(husk).await.unwrap().is_none());
    // 3's reference to 1 waits on the number again; the pending node 1 alone cited is gone.
    let cites = s
        .neighbours(
            d3.document,
            &NeighbourQuery {
                direction: Direction::Out,
                edge_kinds: vec![EdgeKind::Cites],
                ..NeighbourQuery::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(cites.len(), 1, "{cites:?}");
    assert_eq!(cites[0].node.key, "ref:PRG-1");
    assert_eq!(cites[0].edge.edge.props["pending"], json!(true));
    assert_eq!(cites[0].edge.edge.props["raw"], json!("PRG-1"));
    assert!(s.node(ghost).await.unwrap().is_none());
    assert!(
        s.alias(NodeKind::Document, "number:PRG-1")
            .await
            .unwrap()
            .is_none_or(|a| a.node.is_none())
    );
    // 3's ended copy is outside the source: kept by the source purge.
    assert!(s.node(old3.location).await.unwrap().is_some());

    // Again: nothing left.
    let again = purge_source_all(s, &src, 1).await;
    assert_eq!(
        (
            again.records,
            again.locations,
            again.companies,
            again.other_documents
        ),
        (0, 0, 0, 0),
        "{again:?}"
    );

    // The LLM writer edits a record and never brings back a purged one.
    assert!(
        !s.update_record(&h(1), &body(1, "summary"), &versions("1"))
            .await
            .unwrap()
    );
    assert_eq!(s.get_record(&h(1)).await.unwrap(), None);
    assert!(
        s.update_record(&h(2), &body(2, "summary"), &versions("1"))
            .await
            .unwrap()
    );
    assert_eq!(
        s.get_record(&h(2))
            .await
            .unwrap()
            .unwrap()
            .body_value()
            .unwrap()["title"],
        json!("summary")
    );

    // The orphans: 5's record, 3's ended copy, and what other cases left.
    let orphans = purge_orphans_all(s, 3).await;
    assert!(orphans.records >= 1, "{orphans:?}");
    assert!(orphans.hashes.contains(&h(5)));
    assert_eq!(s.get_record(&h(5)).await.unwrap(), None);
    assert!(s.node(old3.location).await.unwrap().is_none());
    for n in [2, 3] {
        assert!(s.get_record(&h(n)).await.unwrap().is_some(), "record {n}");
    }
    assert!(s.node(d3.location).await.unwrap().is_some());
    assert!(s.node(both).await.unwrap().is_some());
    let again = purge_orphans_all(s, 3).await;
    assert_eq!(
        (
            again.records,
            again.locations,
            again.companies,
            again.other_documents
        ),
        (0, 0, 0, 0),
        "{again:?}"
    );
    // Purged content can come back: a new copy is ingested as new.
    s.put_record(&h(1), &body(1, "back"), &versions("1"))
        .await
        .unwrap();
    let back = s
        .upsert_location(&h(1), &purge_at("pa/1.pdf"))
        .await
        .unwrap();
    assert!(back.record);
    assert_ne!(back.document, d1.document);
}

/// A source purge never removes another source's locations: the claims whose objects overlap
/// a source's are found, whichever instance holds them.
///
/// # Panics
///
/// When the backend misbehaves.
pub async fn purge_overlaps<B: Backend + ?Sized>(a: &B, b: &B) {
    let key = |p: &str| SourceKey::new("http://overlap.test", "docs", p.to_owned()).unwrap();
    a.claim_source(&key("ov/a/"), &instance("inst-a"))
        .await
        .unwrap();
    b.claim_source(&key("ov/b/"), &instance("inst-b"))
        .await
        .unwrap();
    let prefixes = |claims: Vec<SourceClaim>| {
        claims
            .into_iter()
            .map(|c| (c.key.prefix().to_owned(), c.owner.as_str().to_owned()))
            .collect::<Vec<_>>()
    };
    assert_eq!(
        prefixes(b.overlapping_claims(&key("ov/")).await.unwrap()),
        [
            ("ov/a/".to_owned(), "inst-a".to_owned()),
            ("ov/b/".to_owned(), "inst-b".to_owned())
        ]
    );
    assert_eq!(
        prefixes(a.overlapping_claims(&key("ov/a/x/")).await.unwrap()),
        [("ov/a/".to_owned(), "inst-a".to_owned())]
    );
    // Its own claim is not an overlap, nor a sibling prefix.
    assert!(
        a.overlapping_claims(&key("ov/a/"))
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        a.overlapping_claims(&key("ov/c/"))
            .await
            .unwrap()
            .is_empty()
    );
    let other = SourceKey::new("http://overlap.test", "other", "ov/").unwrap();
    assert!(a.overlapping_claims(&other).await.unwrap().is_empty());
}

/// A purge racing ingest of the same content (issue #168): the content's location is written
/// through one handle while the other purges the orphan record. Either the purge sees the
/// location and keeps the record, or it deleted the record first and the location says so
/// (`Located::record` false: ingest stores the record again). A live location never ends up
/// with its record gone unnoticed.
///
/// # Panics
///
/// When the backend misbehaves.
pub async fn purge_races<B: Backend + ?Sized>(a: &B, b: &B) {
    let (mut kept, mut lost) = (0, 0);
    for n in 1..=40u8 {
        let h = purge_hash(0xe7, n);
        let before = purge_hash(0xe7, n - 1);
        a.put_record(&h, &raw(&json!({"race": n})), &versions("1"))
            .await
            .unwrap();
        let at = purge_at(&format!("race/{n}.pdf"));
        let after = before.to_hex();
        let (located, purged) = futures_join(
            b.upsert_location(&h, &at),
            a.purge_orphan_records(Some(&after), 1),
        )
        .await;
        let located = located.unwrap();
        let purged = purged.unwrap();
        let record = a.get_record(&h).await.unwrap();
        if located.record {
            assert!(
                record.is_some(),
                "round {n}: a live location lost its record"
            );
            assert!(purged.hashes.is_empty(), "round {n}: {purged:?}");
            kept += 1;
        } else {
            assert!(record.is_none(), "round {n}");
            assert_eq!(purged.hashes, [h], "round {n}");
            lost += 1;
            // Ingest's retry: the record again, and the location now holds it.
            a.put_record(&h, &raw(&json!({"race": n})), &versions("1"))
                .await
                .unwrap();
            assert!(b.upsert_location(&h, &at).await.unwrap().record);
            let p = a.purge_orphan_records(Some(&after), 1).await.unwrap();
            assert!(p.hashes.is_empty(), "round {n}: {p:?}");
            assert!(a.get_record(&h).await.unwrap().is_some());
        }
    }
    assert_eq!(kept + lost, 40);
}

/// Both futures to completion, polled together.
async fn futures_join<X: Future, Y: Future>(x: X, y: Y) -> (X::Output, Y::Output) {
    let (mut x, mut y) = (Box::pin(x), Box::pin(y));
    let (mut xo, mut yo) = (None, None);
    std::future::poll_fn(|cx| {
        if xo.is_none()
            && let Poll::Ready(v) = x.as_mut().poll(cx)
        {
            xo = Some(v);
        }
        if yo.is_none()
            && let Poll::Ready(v) = y.as_mut().poll(cx)
        {
            yo = Some(v);
        }
        if xo.is_some() && yo.is_some() {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    })
    .await;
    (xo.expect("polled"), yo.expect("polled"))
}
