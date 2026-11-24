//! The conformance suite: one generic test every backend runs, so the shared and standalone
//! modes behave the same.
//!
//! Call [`run`] from a backend's tests with two handles to the **same, empty** store. They
//! stand for two instances: claims and leases taken through one are seen by the other.

use std::collections::BTreeSet;
use std::time::Duration;

use doewood_core::InstanceId;
use serde_json::json;
use time::{Date, Month};
use uuid::Uuid;

use crate::{
    Backend, Blake3, Direction, Edge, EdgeKind, EdgeSource, EdgeState, Error, NeighbourQuery,
    NodeKind, ObjectLocation, PutOutcome, SearchDoc, SearchQuery, SourceKey, StepVersions,
};

/// Runs every check.
///
/// # Panics
///
/// When the backend does not behave as the traits say.
pub async fn run<B: Backend + ?Sized>(a: &B, b: &B) {
    records(a).await;
    graph(a).await;
    dated_edges(a).await;
    locations(a).await;
    records_by_source(a).await;
    claims(a, b).await;
    leases(a, b).await;
    search(a).await;
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
        s.put_record(&h, &body, &versions("1.0")).await.unwrap(),
        PutOutcome::Inserted
    );
    assert_eq!(
        s.put_record(&h, &body, &versions("1.0")).await.unwrap(),
        PutOutcome::Unchanged
    );
    let got = s.get_record(&h).await.unwrap().expect("stored");
    assert_eq!(got.blake3, h);
    assert_eq!(got.body, body);
    assert_eq!(got.versions, versions("1.0"));
    assert!(got.updated_at > 1_700_000_000, "{}", got.updated_at);
    // A new step version alone is a change.
    assert_eq!(
        s.put_record(&h, &body, &versions("1.1")).await.unwrap(),
        PutOutcome::Updated
    );
    let body2 = json!({"kind": "invoice", "facts": {}});
    assert_eq!(
        s.put_record(&h, &body2, &versions("1.1")).await.unwrap(),
        PutOutcome::Updated
    );
    let got = s.get_record(&h).await.unwrap().expect("stored");
    assert_eq!((got.body, got.versions), (body2, versions("1.1")));
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
    let mut expected = vec![(d1, hash(11).to_hex()), (d2, hash(12).to_hex())];
    expected.sort_by(|x, y| x.1.cmp(&y.1));
    assert_eq!(
        docs.iter()
            .map(|n| (n.id, n.key.clone()))
            .collect::<Vec<_>>(),
        expected
    );
    // A retracted party no longer counts.
    s.upsert_edge(&supplier.clone().with_state(EdgeState::Retracted))
        .await
        .unwrap();
    let docs = s.company_documents(acme).await.unwrap();
    assert_eq!(docs.iter().map(|n| n.id).collect::<Vec<_>>(), [d1]);

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
        s.put_record(&hash(n), &json!({"n": n}), &versions("1"))
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
        s.list_records(&src, after.as_ref(), limit)
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
    let page = s.list_records(&src, None, 1).await.unwrap();
    assert_eq!(page[0].body, json!({"n": 31}));
    assert_eq!(page[0].versions, versions("1"));
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
        s.put_record(h, &json!({"kind": kind}), &versions("1"))
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
