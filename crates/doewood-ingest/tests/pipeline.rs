//! The whole pipeline over generated PDF, DOCX, TXT and XLSX files: events and the pending
//! sweep, records, the graph (documents, locations, companies, references, `similar_to`),
//! search, retries, failures, deletes and reprocessing. Always against the SQLite backend, and
//! against Postgres too when `DOEWOOD_TEST_POSTGRES_URL` is set (in a schema of its own).

mod common;

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use common::MemFetch;
use doewood_core::{MasterKey, SourceId};
use doewood_ingest::facts::{DocFacts, Party};
use doewood_ingest::{
    Analyzer, AnalyzerConfig, GraphWriter, Ingest, IngestConfig, IngestParts, ReprocessState,
    SimilarityConfig,
};
use doewood_model::{
    Backend, Blake3, Direction, EdgeKind, EdgeState, MergeActor, MergeActorKind, NeighbourQuery,
    NodeKind, ObjectLocation, SearchQuery, SourceKey, SplitRequest,
};
use doewood_s3::{EventKind, EventOrigin, ObjectEvent, ObjectVersion};
use doewood_store::{NewSource, ObjectChange, SeenObject, Standalone, Store};
use tokio::sync::{mpsc, watch};

const PG_VAR: &str = "DOEWOOD_TEST_POSTGRES_URL";
const ENDPOINT: &str = "http://objects.test:9000";
const BUCKET: &str = "docs";

struct Shared {
    name: &'static str,
    backend: Arc<dyn Backend>,
    cleanup: Option<(tokio_postgres::Client, String)>,
}

impl Shared {
    async fn all(store: &Arc<Store>) -> Vec<Self> {
        let mut out = vec![Self {
            name: "sqlite",
            backend: Arc::new(Standalone::new(Arc::clone(store))),
            cleanup: None,
        }];
        let Some(url) = std::env::var(PG_VAR).ok().filter(|v| !v.trim().is_empty()) else {
            eprintln!("skipping: {PG_VAR} is not set; the Postgres variant did not run");
            return out;
        };
        let (admin, conn) = tokio_postgres::connect(&url, tokio_postgres::NoTls)
            .await
            .expect("connect to the test database");
        tokio::spawn(conn);
        let schema = format!("dw_ingest_{}", uuid::Uuid::now_v7().simple());
        admin
            .batch_execute(&format!("CREATE SCHEMA {schema}"))
            .await
            .unwrap();
        let sep = if url.contains('?') { '&' } else { '?' };
        let scoped = format!("{url}{sep}options=-c%20search_path%3D{schema}");
        let pg = doewood_pg::PgStore::connect(&doewood_core::PostgresUrl::parse(&scoped).unwrap())
            .await
            .unwrap();
        out.push(Self {
            name: "postgres",
            backend: Arc::new(pg),
            cleanup: Some((admin, schema)),
        });
        out
    }

    async fn drop_schema(self) {
        if let Some((admin, schema)) = self.cleanup {
            admin
                .batch_execute(&format!("DROP SCHEMA {schema} CASCADE"))
                .await
                .unwrap();
        }
    }
}

struct Harness {
    store: Arc<Store>,
    backend: Arc<dyn Backend>,
    fetch: Arc<MemFetch>,
    source: SourceId,
    events: mpsc::Sender<ObjectEvent>,
    ingest: Ingest,
    shutdown: watch::Sender<bool>,
    handle: Option<doewood_ingest::IngestHandle>,
}

fn analyzer(jobs: usize) -> Arc<Analyzer> {
    Arc::new(
        Analyzer::new(&AnalyzerConfig {
            workers: jobs,
            // No models here: OCR (when built) reports itself unavailable and never fails.
            ocr_models: PathBuf::from("/nonexistent/doewood-ocr-models"),
            ..AnalyzerConfig::default()
        })
        .unwrap(),
    )
}

impl Harness {
    fn start(store: Arc<Store>, backend: Arc<dyn Backend>, jobs: usize, retry: Duration) -> Self {
        let source = store
            .add_source(&NewSource::new(ENDPOINT, BUCKET), None)
            .unwrap();
        let fetch = Arc::new(MemFetch::default());
        let mut config = IngestConfig::new(Some(jobs), &PathBuf::from("/nonexistent"));
        config.retry_base = retry;
        config.sweep_interval = Duration::from_millis(200);
        config.reprocess_poll = Duration::from_millis(20);
        let (_tx, backend_rx) = watch::channel(Some(Arc::clone(&backend)));
        let ingest = Ingest::new(IngestParts {
            store: Arc::clone(&store),
            backend: backend_rx,
            fetch: Arc::clone(&fetch) as Arc<dyn doewood_ingest::Fetch>,
            analyzer: analyzer(jobs),
            config,
            on_change: Arc::new(|_| {}),
            on_llm_queued: Arc::new(|_| {}),
        });
        let (events, events_rx) = mpsc::channel(16);
        let (shutdown, shutdown_rx) = watch::channel(false);
        let handle = ingest.start(events_rx, shutdown_rx);
        Self {
            store,
            backend,
            fetch,
            source,
            events,
            ingest,
            shutdown,
            handle: Some(handle),
        }
    }

    /// Puts objects in the bucket, records them as discovery does, and sends New events for
    /// those `with_event` (the others only the sweep finds).
    async fn put(&self, objects: &[(&str, Vec<u8>, bool)]) {
        let mut changes = Vec::new();
        for (key, bytes, _) in objects {
            self.fetch.put(key, bytes.clone());
            changes.push(ObjectChange::Upsert(SeenObject {
                key: (*key).to_owned(),
                etag: Some(format!("{:x}", bytes.len())),
                size: bytes.len() as u64,
                last_modified: Some(1),
            }));
        }
        self.store
            .record_objects(self.source, &changes, None)
            .unwrap();
        for (key, bytes, with_event) in objects {
            if *with_event {
                let v = ObjectVersion {
                    etag: Some(format!("{:x}", bytes.len())),
                    size: bytes.len() as u64,
                    last_modified: Some(1),
                };
                self.events
                    .send(ObjectEvent {
                        source: self.source,
                        key: (*key).to_owned(),
                        kind: EventKind::New(v),
                        origin: EventOrigin::Scan,
                    })
                    .await
                    .unwrap();
            }
        }
    }

    async fn delete(&self, key: &str) {
        self.store
            .record_objects(
                self.source,
                &[ObjectChange::Delete {
                    key: key.to_owned(),
                }],
                None,
            )
            .unwrap();
        self.events
            .send(ObjectEvent {
                source: self.source,
                key: key.to_owned(),
                kind: EventKind::Deleted,
                origin: EventOrigin::Webhook,
            })
            .await
            .unwrap();
    }

    /// Waits until nothing of the source is pending.
    async fn settle(&self) {
        let deadline = tokio::time::Instant::now() + Duration::from_mins(3);
        loop {
            let c = self.store.object_counts(self.source).unwrap();
            if c.pending == 0 {
                // In-flight deletes have no pending row: give them a moment.
                tokio::time::sleep(Duration::from_millis(100)).await;
                return;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "ingest did not settle: {c:?}"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    fn status(&self, key: &str) -> doewood_store::IngestStatus {
        self.store.ingest_status(self.source, key).unwrap().unwrap()
    }

    fn hash(&self, key: &str) -> Blake3 {
        Blake3(self.status(key).blake3.expect("ingested"))
    }

    async fn doc(&self, key: &str) -> uuid::Uuid {
        self.backend
            .node_by_key(NodeKind::Document, &self.hash(key).to_hex())
            .await
            .unwrap()
            .expect("document node")
            .id
    }

    async fn out_edges(
        &self,
        doc: uuid::Uuid,
        kind: EdgeKind,
        all: bool,
    ) -> Vec<doewood_model::Neighbour> {
        self.backend
            .neighbours(
                doc,
                &NeighbourQuery {
                    direction: Direction::Out,
                    edge_kinds: vec![kind],
                    include_inactive: all,
                    ..NeighbourQuery::default()
                },
            )
            .await
            .unwrap()
    }

    async fn stop(mut self) {
        self.shutdown.send_replace(true);
        if let Some(h) = self.handle.take() {
            h.join().await;
        }
    }
}

fn contract(p1: &str) -> Vec<u8> {
    common::pdf(
        &[
            "UGOVOR O ISPORUCI br. 114/23",
            &format!("Dobavljac: ACME Promet d.o.o. Beograd, PIB {p1}"),
            "Narucilac: Beta Gradnja a.d. Novi Sad",
            "Ugovor stupa na snagu 01.03.2024. Ukupno: 120.000,00 RSD.",
        ],
        "Ugovor o isporuci",
    )
}

fn amendment() -> Vec<u8> {
    common::docx(
        "Aneks br. 2",
        &[
            "ANEKS br. 2 uz Ugovor o isporuci br. 114/23",
            "Dobavljac: АЦМЕ Промет д.о.о. Београд",
            "Narucilac: Beta Gradnja a.d.",
        ],
    )
}

fn invoice(p1: &str) -> Vec<u8> {
    format!(
        "INVOICE No. INV-7/2026\nCustomer: ACME Promet a.d.\n\n{}\n\nSupplier: ACME Grupa d.o.o.\nPIB {p1}\nTotal: EUR 4,200.50\n",
        "Thank you for your order. ".repeat(20)
    )
    .into_bytes()
}

/// The same prose with a handful of words changed: Jaccard well above 0.8.
fn near_copy(seed: u64) -> Vec<u8> {
    let text = common::prose(seed, 600);
    let mut words: Vec<&str> = text.split(' ').collect();
    for i in [100, 300, 500] {
        words[i] = "changed";
    }
    words.join(" ").into_bytes()
}

// One story, phase by phase: splitting it would scatter the state each phase builds on.
#[allow(
    clippy::too_many_lines,
    clippy::similar_names,
    clippy::many_single_char_names
)]
async fn scenario(store: Arc<Store>, shared: &Shared) {
    let name = shared.name;
    let h = Harness::start(
        Arc::clone(&store),
        Arc::clone(&shared.backend),
        4,
        Duration::from_millis(10),
    );
    let p1 = common::pib("10123456");
    let blob: Vec<u8> = (0..4096u32)
        .map(|i| u8::try_from((i * 7919) % 251).unwrap())
        .collect();

    // Phase 1: everything but the contract. The amendment cites it before it is known.
    h.fetch
        .flaky
        .lock()
        .unwrap()
        .insert("in/flaky.txt".into(), 2);
    h.put(&[
        ("in/aneks.docx", amendment(), true),
        (
            "in/prices.xlsx",
            common::xlsx(
                "Prices",
                &[
                    &["Item", "Qty", "Price"],
                    &["Bolts", "10", "2.5"],
                    &["Nuts", "20", "1.25"],
                ],
            ),
            true,
        ),
        ("in/notes-a.txt", common::prose(7, 600).into_bytes(), true),
        ("in/notes-b.txt", near_copy(7), true),
        ("in/notes-c.txt", common::prose(99, 600).into_bytes(), true),
        // Found by the sweep only.
        ("in/blob.bin", blob.clone(), false),
        (
            "in/flaky.txt",
            b"Retried until it works: a plain note.".to_vec(),
            true,
        ),
    ])
    .await;
    // Listed, but gone from the bucket before ingest reads it.
    h.store
        .record_objects(
            h.source,
            &[ObjectChange::Upsert(SeenObject {
                key: "in/missing.txt".into(),
                etag: Some("e".into()),
                size: 3,
                last_modified: None,
            })],
            None,
        )
        .unwrap();
    h.settle().await;

    for key in ["in/aneks.docx", "in/prices.xlsx", "in/notes-a.txt"] {
        assert_eq!(h.status(key).status, "done", "{name} {key}");
    }
    assert_eq!(h.status("in/blob.bin").status, "skipped", "{name}");
    let flaky = h.status("in/flaky.txt");
    assert_eq!(
        (flaky.status.as_str(), flaky.attempts),
        ("done", 3),
        "{name} retried twice"
    );
    let missing = h.status("in/missing.txt");
    assert_eq!(missing.status, "failed", "{name}");
    assert_eq!(missing.attempts, 1, "{name}: a 404 is not retried");
    assert!(missing.last_error.unwrap().contains("404"), "{name}");

    // Records: quail's for documents, hedgehog-derived for the workbook, identity only for
    // the blob. Same top-level shape; step versions stored.
    let b = &h.backend;
    let xl = b
        .get_record(&h.hash("in/prices.xlsx"))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(xl.body["format"], "xlsx", "{name}");
    let keys: Vec<&str> = xl.body["facts"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|f| f["key"].as_str())
        .collect();
    for k in [
        "hash.blake3",
        "engine.format",
        "sheet.count",
        "sheet.table",
        "sheet.text",
    ] {
        assert!(keys.contains(&k), "{name}: {k} in {keys:?}");
    }
    assert_eq!(xl.versions["hedgehog"], "c418c2b");
    assert_eq!(xl.versions["doewood.ingest"], "1");
    let blob_rec = b.get_record(&h.hash("in/blob.bin")).await.unwrap().unwrap();
    assert_eq!(blob_rec.body["format"], "unknown", "{name}");
    let blob_keys: Vec<&str> = blob_rec.body["facts"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|f| f["key"].as_str())
        .collect();
    assert!(
        blob_keys
            .iter()
            .all(|k| k.starts_with("hash.") || k.starts_with("engine.")),
        "{name}: {blob_keys:?}"
    );
    let docx_rec = b
        .get_record(&h.hash("in/aneks.docx"))
        .await
        .unwrap()
        .unwrap();
    assert!(docx_rec.versions.contains_key("docx.extract"), "{name}");
    assert!(
        docx_rec.versions["quail.plan"].starts_with("standard:"),
        "{name}"
    );

    // The pending reference: the amendment amends "114/23", not known yet.
    let aneks = h.doc("in/aneks.docx").await;
    let amends = h.out_edges(aneks, EdgeKind::Amends, false).await;
    assert_eq!(amends.len(), 1, "{name}");
    assert_eq!(amends[0].node.key, "ref:11423", "{name}");
    assert_eq!(amends[0].edge.edge.props["pending"], true, "{name}");

    // similar_to: a and b are near copies, c is not.
    let (a, bb, c) = (
        h.doc("in/notes-a.txt").await,
        h.doc("in/notes-b.txt").await,
        h.doc("in/notes-c.txt").await,
    );
    let similar = |n: uuid::Uuid| {
        let b = Arc::clone(b);
        async move {
            b.neighbours(
                n,
                &NeighbourQuery {
                    edge_kinds: vec![EdgeKind::SimilarTo],
                    ..NeighbourQuery::default()
                },
            )
            .await
            .unwrap()
        }
    };
    let sa = similar(a).await;
    assert_eq!(
        sa.iter().map(|n| n.node.id).collect::<Vec<_>>(),
        [bb],
        "{name}"
    );
    let score = sa[0].edge.edge.props["score"].as_f64().unwrap();
    assert!(score >= 0.8, "{name}: {score}");
    assert!(similar(c).await.is_empty(), "{name}");

    // Phase 2: the contract arrives, twice (two keys, the same bytes). Its Latin "ACME Promet
    // d.o.o." matches the amendment's Cyrillic one by name, and brings the PIB.
    h.put(&[
        ("in/contract.pdf", contract(&p1), true),
        ("in/copy/contract.pdf", contract(&p1), true),
    ])
    .await;
    h.settle().await;
    // Phase 3: the invoice names the supplier differently, with the same PIB.
    h.put(&[("in/invoice.txt", invoice(&p1), true)]).await;
    h.settle().await;
    assert_eq!(h.status("in/invoice.txt").status, "done", "{name}");
    assert_eq!(h.hash("in/contract.pdf"), h.hash("in/copy/contract.pdf"));
    let contract_doc = h.doc("in/contract.pdf").await;
    let located = h.out_edges(contract_doc, EdgeKind::LocatedAt, false).await;
    assert_eq!(located.len(), 2, "{name}: one document, two locations");
    // The pending edge moved to the contract.
    let amends = h.out_edges(aneks, EdgeKind::Amends, false).await;
    assert_eq!(
        amends.iter().map(|n| n.node.id).collect::<Vec<_>>(),
        [contract_doc],
        "{name}"
    );
    assert_eq!(amends[0].edge.edge.props.get("pending"), None, "{name}");
    let all = h.out_edges(aneks, EdgeKind::Amends, true).await;
    assert!(
        all.iter()
            .any(|n| n.node.key == "ref:11423" && n.edge.edge.state == EdgeState::Superseded),
        "{name}"
    );

    // Companies: the PIB is the identity; Latin and Cyrillic spellings share a key; another
    // legal form is another company.
    let parties = |doc| {
        let b = Arc::clone(b);
        async move {
            b.neighbours(
                doc,
                &NeighbourQuery {
                    direction: Direction::Out,
                    edge_kinds: vec![EdgeKind::Party],
                    ..NeighbourQuery::default()
                },
            )
            .await
            .unwrap()
        }
    };
    let in_contract = parties(contract_doc).await;
    let acme = in_contract
        .iter()
        .find(|n| {
            n.edge.edge.props["surface"]
                .as_str()
                .unwrap()
                .starts_with("ACME Promet")
        })
        .expect("ACME in the contract");
    assert_eq!(
        acme.node.key, "name:acme promet|d.o.o.",
        "{name}: created by the amendment"
    );
    // By name the first time; the copy, ingested at the same time, may already see the PIB.
    let step = acme.edge.edge.props["match"]["step"].as_str().unwrap();
    assert!(
        ["normalized", "identifier"].contains(&step),
        "{name}: {step}"
    );
    assert_eq!(
        b.alias(NodeKind::Company, &format!("id:rs_pib:{p1}"))
            .await
            .unwrap()
            .unwrap()
            .node,
        Some(acme.node.id),
        "{name}: the PIB now points at it"
    );
    assert_eq!(acme.edge.edge.props["role"], "supplier", "{name}");
    assert!((acme.edge.edge.confidence - 0.97).abs() < 0.01, "{name}");
    let in_aneks = parties(aneks).await;
    let cyr = in_aneks
        .iter()
        .find(|n| {
            n.edge.edge.props["surface"]
                .as_str()
                .unwrap()
                .starts_with("АЦМЕ")
        })
        .expect("the Cyrillic ACME");
    assert_eq!(
        cyr.node.id, acme.node.id,
        "{name}: Cyrillic and Latin are one company"
    );
    let beta_c = in_contract
        .iter()
        .find(|n| n.node.props["key"] == "beta gradnja")
        .unwrap();
    let beta_a = in_aneks
        .iter()
        .find(|n| n.node.props["key"] == "beta gradnja")
        .unwrap();
    assert_eq!(beta_c.node.id, beta_a.node.id, "{name}: same name and form");
    let in_invoice = parties(h.doc("in/invoice.txt").await).await;
    let grupa = in_invoice
        .iter()
        .find(|n| {
            n.edge.edge.props["surface"]
                .as_str()
                .unwrap()
                .contains("Grupa")
        })
        .expect("ACME Grupa");
    assert_eq!(
        grupa.node.id, acme.node.id,
        "{name}: the PIB matches before the name"
    );
    assert_eq!(
        grupa.edge.edge.props["match"]["step"], "identifier",
        "{name}"
    );
    let ad = in_invoice
        .iter()
        .find(|n| n.edge.edge.props["surface"] == "ACME Promet a.d.")
        .expect("ACME Promet a.d.");
    assert_ne!(
        ad.node.id, acme.node.id,
        "{name}: another legal form, another company"
    );
    let blind = b
        .alias(NodeKind::Company, "blind:acme promet")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        blind.node, None,
        "{name}: two companies own the form-blind key"
    );
    let acme_node = b.node(acme.node.id).await.unwrap().unwrap();
    assert_eq!(acme_node.props["ids"]["rs_pib"][0], p1.as_str(), "{name}");
    assert!(
        acme_node.props["matches"].as_array().unwrap().len() >= 3,
        "{name}"
    );
    let docs = b.company_documents(acme.node.id).await.unwrap();
    assert_eq!(docs.len(), 3, "{name}: contract, amendment, invoice");

    // Search: title, kind, body, company filter, source filter.
    let search = |q: SearchQuery| {
        let b = Arc::clone(b);
        async move { b.search(&q).await.unwrap() }
    };
    let hits = search(SearchQuery {
        text: Some("Bolts".into()),
        ..SearchQuery::default()
    })
    .await;
    assert_eq!(
        hits.iter().map(|x| x.blake3).collect::<Vec<_>>(),
        [h.hash("in/prices.xlsx")],
        "{name}"
    );
    assert_eq!(
        hits[0].title, "prices.xlsx",
        "{name}: the file name is the last title"
    );
    let hits = search(SearchQuery {
        kind: Some("contract".into()),
        ..SearchQuery::default()
    })
    .await;
    assert_eq!(hits.len(), 1, "{name}");
    assert_eq!(hits[0].title, "UGOVOR O ISPORUCI br. 114/23", "{name}");
    assert_eq!(hits[0].amount, Some(120_000.0), "{name}");
    assert_eq!(
        hits[0].date.map(|d| d.to_string()).as_deref(),
        Some("2024-03-01"),
        "{name}"
    );
    let hits = search(SearchQuery {
        company: Some(acme.node.id),
        ..SearchQuery::default()
    })
    .await;
    assert_eq!(hits.len(), 3, "{name}");
    let src = SourceKey::new(ENDPOINT, BUCKET, "in/copy/").unwrap();
    assert_eq!(
        b.list_records(&src, None, 0).await.unwrap().len(),
        1,
        "{name}"
    );

    // Phase 4: a delete ends one location; the document keeps the other.
    h.delete("in/copy/contract.pdf").await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    let located = h.out_edges(contract_doc, EdgeKind::LocatedAt, false).await;
    assert_eq!(located.len(), 1, "{name}");
    let all = h.out_edges(contract_doc, EdgeKind::LocatedAt, true).await;
    let ended = all
        .iter()
        .find(|n| n.edge.edge.state == EdgeState::Superseded)
        .expect("ended");
    assert!(ended.edge.edge.valid_to.is_some(), "{name}");
    assert!(
        b.list_records(&src, None, 0).await.unwrap().is_empty(),
        "{name}"
    );
    assert!(
        b.get_record(&h.hash("in/contract.pdf"))
            .await
            .unwrap()
            .is_some(),
        "{name}"
    );

    // Phase 5: reprocessing. Nothing is stale; with force, every live file goes again.
    let mut rx = h.ingest.reprocess(h.source, false);
    let done = rx
        .wait_for(|s| matches!(s.state, ReprocessState::Done | ReprocessState::Failed))
        .await
        .unwrap()
        .clone();
    assert_eq!(done.state, ReprocessState::Done, "{name}: {done:?}");
    assert_eq!(done.selected, 0, "{name}: {done:?}");
    assert!(done.examined >= 9, "{name}: {done:?}");
    let mut rx = h.ingest.reprocess(h.source, true);
    let job = rx.borrow().job;
    assert!(h.ingest.watch_job(job).is_some());
    let done = tokio::time::timeout(
        Duration::from_mins(3),
        rx.wait_for(|s| matches!(s.state, ReprocessState::Done | ReprocessState::Failed)),
    )
    .await
    .expect("reprocess finished")
    .unwrap()
    .clone();
    assert_eq!(done.state, ReprocessState::Done, "{name}: {done:?}");
    assert_eq!(done.selected, 10, "{name}: every live object: {done:?}");
    assert_eq!(done.pending, 0, "{name}");
    assert_eq!(
        h.status("in/missing.txt").status,
        "failed",
        "{name}: still gone"
    );
    let s = h.ingest.stats();
    assert!(s.analyzed >= 9, "{name}: {s:?}");
    h.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pipeline_end_to_end() {
    for kind in ["sqlite", "postgres"] {
        // A fresh instance store each time (its sources and objects_seen).
        let store = Arc::new(Store::open_in_memory(&MasterKey::from_bytes([5; 32])).unwrap());
        let mut all = Shared::all(&store).await;
        let Some(i) = all.iter().position(|s| s.name == kind) else {
            continue;
        };
        let shared = all.swap_remove(i);
        scenario(Arc::clone(&store), &shared).await;
        shared.drop_schema().await;
        for s in all {
            s.drop_schema().await;
        }
    }
}

/// ADR 0002 section 1.8: a document a person split off a company, recording the pair as not
/// the same, is not attached back to that company when it is written again (a reprocess).
/// Without the dismissal the ladder attaches it again, by the name key the company kept.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_dismissal_keeps_a_split_off_document_apart() {
    let store = Arc::new(Store::open_in_memory(&MasterKey::from_bytes([7; 32])).unwrap());
    for shared in Shared::all(&store).await {
        let (name, b) = (shared.name, &shared.backend);
        let writer = GraphWriter::new(Arc::clone(b), SimilarityConfig::default());
        for (n, dismiss) in [(71u8, true), (72, false)] {
            let h = Blake3([n; 32]);
            b.put_record(&h, &serde_json::json!({}), &Default::default())
                .await
                .unwrap();
            let at = ObjectLocation {
                endpoint: ENDPOINT.into(),
                bucket: BUCKET.into(),
                key: format!("split/{n}.pdf"),
            };
            let facts = DocFacts {
                parties: vec![Party {
                    quail_key: "party.1".into(),
                    surface: format!("Dismissal Test {n} d.o.o."),
                    score: 0.9,
                    roles: vec!["buyer".into()],
                    legal_form: None,
                    jurisdiction: None,
                    ids: vec![],
                    mentions: vec![],
                }],
                ..DocFacts::default()
            };
            writer.write(&h, &at, &facts, "x.pdf", 0.8).await.unwrap();
            let doc = b
                .node_by_key(NodeKind::Document, &h.to_hex())
                .await
                .unwrap()
                .unwrap()
                .id;
            let parties = || async {
                let query = NeighbourQuery {
                    direction: Direction::Out,
                    edge_kinds: vec![EdgeKind::Party],
                    ..NeighbourQuery::default()
                };
                b.neighbours(doc, &query).await.unwrap()
            };
            let first = parties().await;
            assert_eq!(first.len(), 1, "{name}");
            let company = first[0].node.id;
            let split = b
                .split_company(&SplitRequest {
                    company,
                    key: None,
                    props: serde_json::json!({"name": "the other one"}),
                    aliases: vec![],
                    documents: vec![doc],
                    edges: vec![],
                    actor: MergeActor {
                        kind: MergeActorKind::Manual,
                        instance: doewood_core::InstanceId::new("inst-a").unwrap(),
                    },
                    evidence: serde_json::json!([]),
                    dismiss,
                })
                .await
                .unwrap();
            // Reprocess: the same facts again.
            writer.write(&h, &at, &facts, "x.pdf", 0.8).await.unwrap();
            let after = parties().await;
            let ids: Vec<_> = after.iter().map(|nb| nb.node.id).collect();
            if dismiss {
                assert_eq!(ids, [split.absorbed], "{name}: stays split off");
                assert_eq!(
                    after[0].edge.edge.props["match"]["dismissed"],
                    company.to_string(),
                    "{name}"
                );
            } else {
                assert_eq!(ids.len(), 2, "{name}: attached again without a dismissal");
                assert!(ids.contains(&company) && ids.contains(&split.absorbed));
            }
        }
        shared.drop_schema().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shutdown_leaves_unfinished_work_pending() {
    let store = Arc::new(Store::open_in_memory(&MasterKey::from_bytes([6; 32])).unwrap());
    let backend: Arc<dyn Backend> = Arc::new(Standalone::new(Arc::clone(&store)));
    let h = Harness::start(Arc::clone(&store), backend, 1, Duration::from_secs(5));
    // Fails transiently for a long time: the worker sits in its backoff when shutdown comes.
    h.fetch
        .flaky
        .lock()
        .unwrap()
        .insert("in/slow.txt".into(), 100);
    h.put(&[("in/slow.txt", b"slow".to_vec(), true)]).await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    let source = h.source;
    let store2 = Arc::clone(&h.store);
    tokio::time::timeout(Duration::from_secs(10), h.stop())
        .await
        .expect("shutdown is prompt");
    let s = store2
        .ingest_status(source, "in/slow.txt")
        .unwrap()
        .unwrap();
    assert_eq!(s.status, "pending");
    assert!(s.attempts >= 1);
}

/// The `similar_to` threshold is the runtime setting in the instance store, read for every
/// file: a change applies to the next file, without restarting ingest.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn similarity_uses_the_stored_threshold() {
    use doewood_store::IngestSettingsPatch;

    let store = Arc::new(Store::open_in_memory(&MasterKey::from_bytes([7; 32])).unwrap());
    let backend: Arc<dyn Backend> = Arc::new(Standalone::new(Arc::clone(&store)));
    let h = Harness::start(
        Arc::clone(&store),
        Arc::clone(&backend),
        2,
        Duration::from_millis(10),
    );
    let similar = |n: uuid::Uuid| {
        let b = Arc::clone(&backend);
        async move {
            b.neighbours(
                n,
                &NeighbourQuery {
                    edge_kinds: vec![EdgeKind::SimilarTo],
                    ..NeighbourQuery::default()
                },
            )
            .await
            .unwrap()
        }
    };
    let set = |x: f64| {
        store
            .set_ingest_settings(&IngestSettingsPatch {
                similar_min_jaccard: Some(x),
            })
            .unwrap();
    };

    // Only identical sketches pass 1.0: the near copy gets no edge.
    set(1.0);
    h.put(&[("in/a.txt", common::prose(7, 600).into_bytes(), true)])
        .await;
    h.settle().await;
    h.put(&[("in/b.txt", near_copy(7), true)]).await;
    h.settle().await;
    let (a, b) = (h.doc("in/a.txt").await, h.doc("in/b.txt").await);
    assert!(similar(a).await.is_empty());
    assert!(similar(b).await.is_empty());

    // Lowered at run time: the next near copy links to both.
    set(0.8);
    let text = common::prose(7, 600);
    let mut words: Vec<&str> = text.split(' ').collect();
    for i in [50, 250, 450] {
        words[i] = "altered";
    }
    h.put(&[("in/c.txt", words.join(" ").into_bytes(), true)])
        .await;
    h.settle().await;
    let c = h.doc("in/c.txt").await;
    let mut linked: Vec<_> = similar(c).await.iter().map(|n| n.node.id).collect();
    linked.sort();
    let mut want = vec![a, b];
    want.sort();
    assert_eq!(linked, want);
    h.stop().await;
}

/// Files per second through the whole pipeline (fetch from memory, analyze, SQLite records,
/// graph and search) on a generated mix of PDF, DOCX, TXT and XLSX, every file distinct.
/// `cargo test -p doewood-ingest --test pipeline -- --ignored --nocapture throughput`
/// (`DOEWOOD_BENCH_FILES` sets the count, default 400).
#[tokio::test(flavor = "multi_thread")]
#[ignore = "a measurement, not a check"]
async fn throughput() {
    let n: usize = std::env::var("DOEWOOD_BENCH_FILES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(400);
    let jobs = doewood_ingest::default_jobs();
    let store = Arc::new(Store::open_in_memory(&MasterKey::from_bytes([8; 32])).unwrap());
    let backend: Arc<dyn Backend> = Arc::new(Standalone::new(Arc::clone(&store)));
    let h = Harness::start(Arc::clone(&store), backend, jobs, Duration::from_millis(10));
    let mut files = Vec::with_capacity(n);
    let mut bytes = 0usize;
    for i in 0..n {
        let pib = common::pib(&format!("{:08}", 10_000_000 + i));
        let body = common::prose(i as u64, 300);
        let (key, data) = match i % 4 {
            0 => (
                format!("bench/{i}.pdf"),
                common::pdf(
                    &[
                        &format!("UGOVOR br. {i}/26"),
                        &format!("Dobavljac: Firma {i} d.o.o. Beograd, PIB {pib}"),
                        "Narucilac: Kupac Grupa a.d. Novi Sad",
                        &body[..90],
                    ],
                    "Ugovor",
                ),
            ),
            1 => (
                format!("bench/{i}.docx"),
                common::docx(
                    &format!("Aneks {i}"),
                    &[&format!("ANEKS uz Ugovor br. {}/26", i - 1), &body],
                ),
            ),
            2 => (
                format!("bench/{i}.txt"),
                format!(
                    "INVOICE No. INV-{i}\nSupplier: Vendor {i} LLC\n{body}\nTotal: EUR {i}.50\n"
                )
                .into_bytes(),
            ),
            _ => (
                format!("bench/{i}.xlsx"),
                common::xlsx(
                    "Data",
                    &[
                        &["Item", "Qty"],
                        &[&format!("Part {i}"), "3"],
                        &["Bolts", "10"],
                    ],
                ),
            ),
        };
        bytes += data.len();
        files.push((key, data));
    }
    let started = std::time::Instant::now();
    for chunk in files.chunks(64) {
        let batch: Vec<(&str, Vec<u8>, bool)> = chunk
            .iter()
            .map(|(k, d)| (k.as_str(), d.clone(), true))
            .collect();
        h.put(&batch).await;
    }
    h.settle().await;
    let secs = started.elapsed().as_secs_f64();
    let c = store.object_counts(h.source).unwrap();
    #[allow(clippy::cast_precision_loss)]
    let (rate, mib) = (n as f64 / secs, bytes as f64 / f64::from(1 << 20));
    println!(
        "throughput: {n} files ({mib:.1} MiB) with {jobs} workers in {secs:.2}s = {rate:.0} \
         files/s (done {}, skipped {}, failed {})",
        c.done, c.skipped, c.failed
    );
    assert_eq!(c.done, n as u64);
    h.stop().await;
}
