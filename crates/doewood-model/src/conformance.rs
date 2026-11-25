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
use time::{Date, Month};
use uuid::Uuid;

use crate::{
    Backend, Blake3, CACHE_KEY_MAX_LEN, CACHE_MAX_BODY, CacheRow, Direction, DismissRequest,
    DismissalQuery, DueCompany, DueReason, Edge, EdgeKind, EdgeSource, EdgeState, Error, IconRow,
    MergeActor, MergeActorKind, MergeOp, MergeQuery, MergeRequest, NeighbourQuery, Node, NodeKind,
    ObjectLocation, OwnedAlias, ProposalKind, ProposalQuery, ProposalReason, ProposalRequest,
    ProposalState, PropsPatch, PutOutcome, ResolveRequest, SearchDoc, SearchQuery, SourceKey,
    SplitRequest, StepVersions, StoredEdge, StoredIcon, UndoRequest, merged_into,
};

/// Runs every check.
///
/// # Panics
///
/// When the backend does not behave as the traits say.
pub async fn run<B: Backend + ?Sized>(a: &B, b: &B) {
    records(a).await;
    graph(a).await;
    props_patches(a, b).await;
    dated_edges(a).await;
    locations(a).await;
    ended_locations(a).await;
    records_by_source(a).await;
    sketches(a).await;
    aliases(a).await;
    claims(a, b).await;
    leases(a, b).await;
    search(a).await;
    caches(a, b).await;
    freshness(a, b).await;
    merges(a).await;
    proposals(a, b).await;
    decisions(a).await;
    live_edges(a, b).await;
    live_writes(a, b).await;
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
    s.put_record(&doc, &json!({"n": 41}), &versions("1"))
        .await
        .unwrap();
    let first = s.upsert_location(&doc, &at("in/a.pdf")).await.unwrap();
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
    assert!(s.list_records(&src, None, 0).await.unwrap().is_empty());
    let src = SourceKey::new("http://end.test:9000", "docs", "in/").unwrap();
    assert_eq!(s.list_records(&src, None, 0).await.unwrap().len(), 1);
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

/// The LSH band index: candidates by shared bands, replacement, the excluded document.
///
/// # Panics
///
/// When the backend misbehaves.
pub async fn sketches<B: Backend + ?Sized>(s: &B) {
    for n in 51..=54 {
        s.put_record(&hash(n), &json!({"n": n}), &versions("1"))
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
