//! Several writers of one instance resolving overlapping parties at once (issue #78): the
//! instance's party locks are per alias key, so parties that share no key run at the same
//! time, and the rules of ADR 0002 section 1 (first claim wins, form-blind blocking,
//! dismissals) must come out as with one writer.
//!
//! - `overlapping_parties_give_the_graph_of_one_worker`: parties whose result does not depend
//!   on the order the documents come in (the same identifier under several names; the same
//!   name with and without its legal form; one name in two legal forms, which blocks the
//!   form-blind key; a learned alias; a document split off a company with a dismissal,
//!   written again). Written by one worker, then, many times, by several at once in a
//!   shuffled order: every run gives the same graph.
//! - `racing_parties_keep_the_ladder_rules`: parties whose result depends on the order (a
//!   form-blind match on a company without a legal form while another party gives it one; a
//!   learned alias claimed while a party with that name is matched). No single answer, so
//!   the rules are checked instead: a form-blind match never lands on a company of another
//!   legal form, the same party always lands on one company, and a company created for a
//!   party owns the key it was created for.
//!
//! SQLite always; Postgres too when `DOEWOOD_TEST_POSTGRES_URL` is set (a schema of its own
//! per run). `DOEWOOD_TEST_PARTY_ROUNDS` runs per test and store (default 12).

mod common;

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use doewood_core::MasterKey;
use doewood_ingest::company::{CompanyName, IdKind, Identifier};
use doewood_ingest::facts::{DocFacts, Party};
use doewood_ingest::{GraphWriter, SimilarityConfig};
use doewood_model::{
    Backend, Blake3, Direction, EdgeKind, MergeActor, MergeActorKind, NeighbourQuery, NodeKind,
    ObjectLocation, SplitRequest, StepVersions,
};
use doewood_store::{Standalone, Store};
use serde_json::{Value, json};
use uuid::Uuid;

const PG_VAR: &str = "DOEWOOD_TEST_POSTGRES_URL";
/// Writers at once.
const WORKERS: usize = 4;

/// One test at a time: their schemas are created and dropped many times, and migrating one
/// while another is dropped can fail on Postgres ("referenced type was concurrently dropped").
static ONE_AT_A_TIME: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

fn rounds() -> usize {
    std::env::var("DOEWOOD_TEST_PARTY_ROUNDS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(12)
}

/// A store under test.
struct Target {
    backend: Arc<dyn Backend>,
    pg: Option<(tokio_postgres::Client, String)>,
}

impl Target {
    fn sqlite() -> Self {
        let store = Store::open_in_memory(&MasterKey::from_bytes([31; 32])).unwrap();
        Self {
            backend: Arc::new(Standalone::new(Arc::new(store))),
            pg: None,
        }
    }

    async fn postgres(url: &str) -> Self {
        let (admin, conn) = tokio_postgres::connect(url, tokio_postgres::NoTls)
            .await
            .expect("connect to the test database");
        tokio::spawn(conn);
        let schema = format!("dw_party_{}", Uuid::now_v7().simple());
        admin
            .batch_execute(&format!("CREATE SCHEMA {schema}"))
            .await
            .unwrap();
        let sep = if url.contains('?') { '&' } else { '?' };
        let scoped = format!("{url}{sep}options=-c%20search_path%3D{schema}");
        let pg = doewood_pg::PgStore::connect(&doewood_core::PostgresUrl::parse(&scoped).unwrap())
            .await
            .unwrap();
        Self {
            backend: Arc::new(pg),
            pg: Some((admin, schema)),
        }
    }

    /// A fresh store of the same kind.
    async fn fresh(kind: &str) -> Self {
        match kind {
            "sqlite" => Self::sqlite(),
            _ => Self::postgres(&std::env::var(PG_VAR).unwrap()).await,
        }
    }

    async fn finish(self) {
        drop(self.backend);
        if let Some((admin, schema)) = self.pg {
            admin
                .batch_execute(&format!("DROP SCHEMA {schema} CASCADE"))
                .await
                .unwrap();
        }
    }
}

/// The stores to run on: SQLite, and Postgres when set.
fn kinds() -> Vec<&'static str> {
    if std::env::var(PG_VAR).is_ok_and(|v| !v.trim().is_empty()) {
        vec!["sqlite", "postgres"]
    } else {
        eprintln!("skipping: {PG_VAR} is not set; the Postgres variant did not run");
        vec!["sqlite"]
    }
}

fn pib(n: usize) -> Identifier {
    Identifier::parse(IdKind::RsPib, &common::pib(&format!("{n:08}"))).unwrap()
}

fn party(surface: &str, ids: &[usize]) -> Party {
    Party {
        quail_key: "party".into(),
        surface: surface.into(),
        score: 0.9,
        roles: vec!["party".into()],
        legal_form: None,
        jurisdiction: None,
        ids: ids.iter().map(|&n| pib(n)).collect(),
        mentions: vec![],
    }
}

/// One document: its tag (the same tag again is the same document written again) and its
/// parties.
#[derive(Clone)]
struct Doc {
    tag: &'static str,
    parties: Vec<Party>,
}

fn doc(tag: &'static str, parties: &[(&str, &[usize])]) -> Doc {
    Doc {
        tag,
        parties: parties.iter().map(|(s, ids)| party(s, ids)).collect(),
    }
}

fn hash(tag: &str) -> Blake3 {
    Blake3::from_slice(blake3::hash(tag.as_bytes()).as_bytes()).unwrap()
}

fn at(tag: &str) -> ObjectLocation {
    ObjectLocation {
        endpoint: "http://objects.test:9000".into(),
        bucket: "docs".into(),
        key: format!("parties/{tag}.pdf"),
    }
}

fn facts(d: &Doc) -> DocFacts {
    DocFacts {
        format: "pdf".into(),
        title: Some(d.tag.into()),
        body: format!("document {}", d.tag),
        parties: d.parties.clone(),
        ..DocFacts::default()
    }
}

/// Stores the records of `docs` (once per tag).
async fn records(b: &dyn Backend, docs: &[Doc]) {
    let record = doewood_model::canonical::to_canonical(&json!({})).unwrap();
    let tags: BTreeSet<&str> = docs.iter().map(|d| d.tag).collect();
    for tag in tags {
        b.put_record(&hash(tag), &record, &StepVersions::new())
            .await
            .unwrap();
    }
}

/// Writes `docs` with `workers` writers of one `GraphWriter` at once, in this order.
async fn write_all(writer: &Arc<GraphWriter>, docs: &[Doc], workers: usize) {
    let docs = Arc::new(docs.to_vec());
    let next = Arc::new(AtomicUsize::new(0));
    let tasks: Vec<_> = (0..workers)
        .map(|_| {
            let (writer, docs, next) = (Arc::clone(writer), Arc::clone(&docs), Arc::clone(&next));
            tokio::spawn(async move {
                loop {
                    let i = next.fetch_add(1, Ordering::Relaxed);
                    let Some(d) = docs.get(i) else { break };
                    writer
                        .write(&hash(d.tag), &at(d.tag), &facts(d), "x.pdf", 0.8)
                        .await
                        .unwrap();
                }
            })
        })
        .collect();
    for t in tasks {
        t.await.unwrap();
    }
}

/// `docs` in an order of their own for each `seed` (a small LCG; Fisher-Yates).
fn shuffled(docs: &[Doc], seed: u64) -> Vec<Doc> {
    let mut out = docs.to_vec();
    let mut x = seed.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
    for i in (1..out.len()).rev() {
        x = x
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        let j = usize::try_from((x >> 33) % (i as u64 + 1)).unwrap();
        out.swap(i, j);
    }
    out
}

async fn doc_id(b: &dyn Backend, tag: &str) -> Uuid {
    b.node_by_key(NodeKind::Document, &hash(tag).to_hex())
        .await
        .unwrap()
        .expect("the document")
        .id
}

/// A document's live `party` edges: the company and the edge's props.
async fn parties_of(b: &dyn Backend, tag: &str) -> Vec<(Uuid, Value)> {
    let query = NeighbourQuery {
        direction: Direction::Out,
        edge_kinds: vec![EdgeKind::Party],
        node_kind: Some(NodeKind::Company),
        ..NeighbourQuery::default()
    };
    b.neighbours(doc_id(b, tag).await, &query)
        .await
        .unwrap()
        .into_iter()
        .map(|n| (n.node.id, n.edge.edge.props))
        .collect()
}

/// The aliases a company owns.
async fn owned(b: &dyn Backend, company: Uuid) -> Vec<String> {
    let mut out: Vec<String> = b
        .aliases_of(company)
        .await
        .unwrap()
        .into_iter()
        .map(|a| a.alias)
        .collect();
    out.sort();
    out
}

/// A company as the order of writes cannot change it: the aliases it owns, its legal form,
/// its names and identifiers as sets. (Its id, node key and first name depend on which
/// document came first.)
async fn describe(b: &dyn Backend, company: Uuid) -> Value {
    let props = b.node(company).await.unwrap().expect("the company").props;
    let sorted = |v: &Value| {
        let mut s: Vec<String> = v
            .as_array()
            .map(|a| a.iter().map(ToString::to_string).collect())
            .unwrap_or_default();
        s.sort();
        s
    };
    let ids: BTreeMap<String, Vec<String>> = props["ids"]
        .as_object()
        .map(|o| o.iter().map(|(k, v)| (k.clone(), sorted(v))).collect())
        .unwrap_or_default();
    json!({
        "aliases": owned(b, company).await,
        "legal_form": props["legal_form"],
        "names": sorted(&props["names"]),
        "ids": ids,
    })
}

/// Every key the parties of `docs` can read or write: identifiers and ladder keys.
fn keys_of(docs: &[Doc]) -> Vec<String> {
    let mut keys = BTreeSet::new();
    for p in docs.iter().flat_map(|d| &d.parties) {
        keys.extend(p.ids.iter().map(Identifier::alias));
        if let Some(n) = CompanyName::parse(&p.surface) {
            keys.extend(n.ladder_keys().into_iter().map(|(_, k)| k));
        }
    }
    keys.into_iter().collect()
}

/// The graph of `docs` as the order of writes cannot change it: per document its companies
/// (described) and how they matched where that is order-blind (the surface, and the company
/// a dismissal sent it away from), every company, and the blocked keys.
async fn snapshot(b: &dyn Backend, docs: &[Doc]) -> Value {
    let tags: BTreeSet<&str> = docs.iter().map(|d| d.tag).collect();
    let keys = keys_of(docs);
    let mut companies: BTreeSet<Uuid> = BTreeSet::new();
    let mut blocked = Vec::new();
    for (key, alias) in keys
        .iter()
        .zip(b.aliases(NodeKind::Company, &keys).await.unwrap())
    {
        match alias.map(|a| a.node) {
            Some(Some(owner)) => {
                companies.insert(owner);
            }
            Some(None) => blocked.push(key.clone()),
            None => {}
        }
    }
    let mut per_doc = BTreeMap::new();
    for tag in tags {
        let mut edges = Vec::new();
        for (company, props) in parties_of(b, tag).await {
            companies.insert(company);
            let dismissed = match props["match"]["dismissed"].as_str() {
                Some(d) => describe(b, d.parse().unwrap()).await,
                None => Value::Null,
            };
            edges.push(json!({
                "company": describe(b, company).await,
                "surface": props["surface"],
                "dismissed": dismissed,
            }));
        }
        edges.sort_by_key(ToString::to_string);
        per_doc.insert(tag, edges);
    }
    let mut all = Vec::new();
    for c in companies {
        all.push(describe(b, c).await);
    }
    all.sort_by_key(ToString::to_string);
    json!({"documents": per_doc, "companies": all, "blocked": blocked})
}

/// The document split off "Epsilon d.o.o." with a dismissal.
const SPLIT: &str = "e-split";

/// Written before the race, one at a time: a document of "Epsilon d.o.o.", split off its
/// company with a dismissal (as a person would).
async fn seed_dismissal(b: &Arc<dyn Backend>, writer: &GraphWriter) {
    let d = doc(SPLIT, &[("Epsilon d.o.o.", &[])]);
    records(b.as_ref(), std::slice::from_ref(&d)).await;
    writer
        .write(&hash(SPLIT), &at(SPLIT), &facts(&d), "x.pdf", 0.8)
        .await
        .unwrap();
    let company = parties_of(b.as_ref(), SPLIT).await[0].0;
    b.split_company(&SplitRequest {
        company,
        key: None,
        props: json!({"name": "the other one"}),
        aliases: vec![],
        documents: vec![doc_id(b.as_ref(), SPLIT).await],
        edges: vec![],
        actor: MergeActor {
            kind: MergeActorKind::Manual,
            instance: doewood_core::InstanceId::new("inst-a").unwrap(),
        },
        evidence: json!([]),
        dismiss: true,
    })
    .await
    .unwrap();
}

/// Overlapping parties whose graph does not depend on the order of the documents.
fn order_blind_corpus() -> Vec<Doc> {
    const P1: usize = 11_000_001;
    vec![
        // One identifier, several names.
        doc("a1", &[("Alfa d.o.o.", &[P1])]),
        doc("a2", &[("ALFA DOO", &[P1])]),
        doc("a3", &[("Alfa Promet d.o.o.", &[P1])]),
        doc("a4", &[("Alfa", &[P1]), ("Beta", &[])]),
        // One name with and without its legal form: one company, whichever comes first.
        doc("b1", &[("Beta d.o.o.", &[])]),
        doc("b2", &[("Beta", &[])]),
        doc("b3", &[("BETA DOO", &[])]),
        doc("b4", &[("Beta d.o.o.", &[])]),
        // One name in two legal forms: two companies, and the form-blind key blocked.
        doc("g1", &[("Gama d.o.o.", &[])]),
        doc("g2", &[("Gama a.d.", &[])]),
        doc("g3", &[("Gama a.d.", &[]), ("Epsilon d.o.o.", &[])]),
        doc("g4", &[("Gama d.o.o.", &[])]),
        // The split-off document again (it stays apart), and others of its old company.
        doc(SPLIT, &[("Epsilon d.o.o.", &[])]),
        doc("e1", &[("Epsilon d.o.o.", &[])]),
        doc(SPLIT, &[("Epsilon d.o.o.", &[])]),
        doc("e2", &[("EPSILON DOO", &[])]),
        doc(SPLIT, &[("Epsilon d.o.o.", &[])]),
        // A learned alias.
        doc(
            "z1",
            &[("Zeta Trade d.o.o. (formerly Omega Trade d.o.o.)", &[])],
        ),
        doc("z2", &[("Zeta Trade d.o.o.", &[])]),
        doc("z3", &[("Zeta Trade d.o.o.", &[]), ("Beta d.o.o.", &[])]),
    ]
}

/// One run: a fresh store, the dismissal seeded, then `docs` by `workers` writers.
async fn run_once(kind: &str, docs: &[Doc], workers: usize) -> Value {
    let t = Target::fresh(kind).await;
    let writer = Arc::new(GraphWriter::new(
        Arc::clone(&t.backend),
        SimilarityConfig::default(),
    ));
    seed_dismissal(&t.backend, &writer).await;
    records(t.backend.as_ref(), docs).await;
    write_all(&writer, docs, workers).await;
    let mut all = docs.to_vec();
    all.push(doc(SPLIT, &[]));
    let snap = snapshot(t.backend.as_ref(), &all).await;
    drop(writer);
    t.finish().await;
    snap
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn overlapping_parties_give_the_graph_of_one_worker() {
    let _one = ONE_AT_A_TIME.lock().await;
    let docs = order_blind_corpus();
    for kind in kinds() {
        let one = run_once(kind, &docs, 1).await;
        // The rules came out as intended, not just the same each time.
        let split = &one["documents"][SPLIT];
        assert_eq!(split.as_array().unwrap().len(), 1, "{kind}: {split}");
        assert!(!split[0]["dismissed"].is_null(), "{kind}: {split}");
        assert_eq!(
            one["blocked"],
            json!(["blind:epsilon", "blind:gama"]),
            "{kind}"
        );
        for round in 0..rounds() {
            let order = shuffled(&docs, round as u64);
            let many = run_once(kind, &order, WORKERS).await;
            assert_eq!(
                many, one,
                "{kind}, round {round}: {WORKERS} writers gave another graph than one"
            );
        }
    }
}

/// Parties whose result depends on the order: "Delta" with an identifier is written first
/// (a company without a legal form); then "Delta d.o.o." (form-blind onto it) races
/// "Omicron a.d." with the same identifier (which gives it the form `a.d.`), and a learned
/// alias races a party of that name.
fn racing_corpus() -> Vec<Doc> {
    const P4: usize = 44_000_004;
    let mut docs = Vec::new();
    for (d, o, k, l) in [
        ("d1", "o1", "k1", "l1"),
        ("d2", "o2", "k2", "l2"),
        ("d3", "o3", "k3", "l3"),
        ("d4", "o4", "k4", "l4"),
    ] {
        docs.push(doc(d, &[("Delta d.o.o.", &[])]));
        docs.push(doc(o, &[("Omicron a.d.", &[P4])]));
        docs.push(doc(
            k,
            &[("Kapa Trade d.o.o. (formerly Lambda Trade d.o.o.)", &[])],
        ));
        docs.push(doc(l, &[("Lambda Trade d.o.o.", &[])]));
    }
    docs
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn racing_parties_keep_the_ladder_rules() {
    let _one = ONE_AT_A_TIME.lock().await;
    let docs = racing_corpus();
    let form_blind = doewood_ingest::company::LadderStep::FormBlind.as_str();
    for kind in kinds() {
        for round in 0..rounds() {
            let t = Target::fresh(kind).await;
            let b = t.backend.as_ref();
            let writer = Arc::new(GraphWriter::new(
                Arc::clone(&t.backend),
                SimilarityConfig::default(),
            ));
            let seed = doc("d0", &[("Delta", &[44_000_004])]);
            records(b, std::slice::from_ref(&seed)).await;
            write_all(&writer, std::slice::from_ref(&seed), 1).await;
            records(b, &docs).await;
            write_all(&writer, &shuffled(&docs, round as u64), WORKERS).await;
            let mut on: BTreeMap<String, BTreeSet<Uuid>> = BTreeMap::new();
            for d in &docs {
                let edges = parties_of(b, d.tag).await;
                assert_eq!(edges.len(), 1, "{kind}, round {round}: {}", d.tag);
                let (company, props) = &edges[0];
                let surface = props["surface"].as_str().unwrap().to_owned();
                let company_props = b.node(*company).await.unwrap().unwrap().props;
                let step = props["match"]["step"].as_str().unwrap();
                let form = CompanyName::parse(&surface).and_then(|n| n.legal_form);
                if step == form_blind
                    && let Some(form) = form
                {
                    assert_eq!(
                        company_props["legal_form"].as_str(),
                        Some(form),
                        "{kind}, round {round}: {} matched form-blind onto a company of \
                         another legal form: {company_props}",
                        d.tag
                    );
                }
                if step == "created" {
                    let alias = props["match"]["alias"].as_str().unwrap();
                    assert!(
                        owned(b, *company).await.iter().any(|a| a == alias),
                        "{kind}, round {round}: {} created a company that does not own \
                         its key {alias}",
                        d.tag
                    );
                }
                on.entry(surface).or_default().insert(*company);
            }
            for (surface, companies) in &on {
                assert_eq!(
                    companies.len(),
                    1,
                    "{kind}, round {round}: {surface} landed on {companies:?}"
                );
            }
            drop(writer);
            t.finish().await;
        }
    }
}
