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
        out.extend(Self::postgres().await);
        out
    }

    /// A fresh schema on the test database, when `DOEWOOD_TEST_POSTGRES_URL` is set.
    async fn postgres() -> Option<Self> {
        let Some(url) = std::env::var(PG_VAR).ok().filter(|v| !v.trim().is_empty()) else {
            eprintln!("skipping: {PG_VAR} is not set; the Postgres variant did not run");
            return None;
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
        // `DOEWOOD_BENCH_POOL`: the pool's size, for measurements (default as `serve`).
        let pool = std::env::var("DOEWOOD_BENCH_POOL")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or_else(doewood_pg::default_pool_size);
        let url = doewood_core::PostgresUrl::parse(&scoped).unwrap();
        let pg = doewood_pg::PgStore::connect_with_pool(&url, pool)
            .await
            .unwrap();
        Some(Self {
            name: "postgres",
            backend: Arc::new(pg),
            cleanup: Some((admin, schema)),
        })
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

fn analyzer(jobs: usize, file_timeout: Option<Duration>) -> Arc<Analyzer> {
    let mut config = AnalyzerConfig {
        workers: jobs,
        // No models here: OCR (when built) reports itself unavailable and never fails.
        ocr_models: PathBuf::from("/nonexistent/doewood-ocr-models"),
        ..AnalyzerConfig::default()
    };
    if let Some(t) = file_timeout {
        config.file_timeout = t;
    }
    Arc::new(Analyzer::new(&config).unwrap())
}

impl Harness {
    fn start(store: Arc<Store>, backend: Arc<dyn Backend>, jobs: usize, retry: Duration) -> Self {
        Self::start_with(store, backend, jobs, retry, None, Arc::new(|_| {}))
    }

    /// [`Harness::start`], with a file timeout, and ingest's change callback (the binary
    /// wakes the progress watchers through it).
    fn start_with(
        store: Arc<Store>,
        backend: Arc<dyn Backend>,
        jobs: usize,
        retry: Duration,
        file_timeout: Option<Duration>,
        on_change: Arc<dyn Fn(SourceId) + Send + Sync>,
    ) -> Self {
        Self::start_tuned(store, backend, jobs, retry, file_timeout, on_change, |_| {})
    }

    fn start_tuned(
        store: Arc<Store>,
        backend: Arc<dyn Backend>,
        jobs: usize,
        retry: Duration,
        file_timeout: Option<Duration>,
        on_change: Arc<dyn Fn(SourceId) + Send + Sync>,
        tune: impl FnOnce(&mut IngestConfig),
    ) -> Self {
        let source = store
            .add_source(&NewSource::new(ENDPOINT, BUCKET), None)
            .unwrap();
        let fetch = Arc::new(MemFetch::default());
        let mut config = IngestConfig::new(Some(jobs), &PathBuf::from("/nonexistent"));
        config.retry_base = retry;
        config.sweep_interval = Duration::from_millis(200);
        config.reprocess_poll = Duration::from_millis(20);
        tune(&mut config);
        let (_tx, backend_rx) = watch::channel(Some(Arc::clone(&backend)));
        let ingest = Ingest::new(IngestParts {
            store: Arc::clone(&store),
            backend: backend_rx,
            fetch: Arc::clone(&fetch) as Arc<dyn doewood_ingest::Fetch>,
            analyzer: analyzer(jobs, file_timeout),
            config,
            on_change,
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
    let xl_body = xl.body_value().unwrap();
    assert_eq!(xl_body["format"], "xlsx", "{name}");
    let keys: Vec<&str> = xl_body["facts"]
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
    assert_eq!(xl.versions["hedgehog"], "fac6eba");
    assert_eq!(xl.versions["doewood.ingest"], "1");
    let blob_rec = b.get_record(&h.hash("in/blob.bin")).await.unwrap().unwrap();
    let blob_body = blob_rec.body_value().unwrap();
    assert_eq!(blob_body["format"], "unknown", "{name}");
    let blob_keys: Vec<&str> = blob_body["facts"]
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
            b.put_record(
                &h,
                &doewood_model::canonical::to_canonical(&serde_json::json!({})).unwrap(),
                &doewood_model::StepVersions::new(),
            )
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
                ..IngestSettingsPatch::default()
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

/// Files per second through the whole pipeline (fetch from memory, analyze, records, graph
/// and search) on a generated mix of PDF, DOCX, TXT and XLSX, every file distinct. The shared
/// store is the instance's SQLite store, or Postgres (a schema of its own) when
/// `DOEWOOD_TEST_POSTGRES_URL` is set.
/// `cargo test -p doewood-ingest --test pipeline -- --ignored --nocapture --exact throughput`
/// (`DOEWOOD_BENCH_FILES` sets the count, default 400; `DOEWOOD_BENCH_JOBS` the workers,
/// default from the CPUs; `DOEWOOD_BENCH_WATCHERS` open `WatchProgress` streams, default 0).
/// `DOEWOOD_BENCH_READERS=n` puts the instance store in a file with `n` read-only connections
/// (0: on disk, every read on the writer) and times a `ListRecords` page every millisecond
/// while ingest runs (from `DOEWOOD_BENCH_READ_CLIENTS` clients at once, default 1;
/// `DOEWOOD_BENCH_POOL` sets the Postgres pool's size). Besides files/s it prints where the workers' time went
/// ([`doewood_ingest::StageTimes`]: the company lock's wait and hold among them) and, on
/// Postgres with `pg_stat_statements` loaded, the statements per file and the costliest
/// statements.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "a measurement, not a check"]
#[allow(clippy::cast_precision_loss, clippy::too_many_lines)]
async fn throughput() {
    let var = |name: &str| std::env::var(name).ok().and_then(|v| v.parse().ok());
    let n: usize = var("DOEWOOD_BENCH_FILES").unwrap_or(400);
    let watchers: usize = var("DOEWOOD_BENCH_WATCHERS").unwrap_or(0);
    let readers: Option<usize> = var("DOEWOOD_BENCH_READERS");
    let jobs: usize = var("DOEWOOD_BENCH_JOBS").unwrap_or_else(doewood_ingest::default_jobs);
    let dir = std::env::temp_dir().join(format!("doewood-throughput-{}", std::process::id()));
    let key = MasterKey::from_bytes([8; 32]);
    let store = Arc::new(match readers {
        None => Store::open_in_memory(&key).unwrap(),
        Some(n) => {
            std::fs::create_dir_all(&dir).unwrap();
            Store::open_with_readers(&dir.join(doewood_store::STORE_FILE), &key, n).unwrap()
        }
    });
    let pg = Shared::postgres().await;
    let backend: Arc<dyn Backend> = match &pg {
        Some(pg) => Arc::clone(&pg.backend),
        None => Arc::new(Standalone::new(Arc::clone(&store))),
    };
    let statements = bench::Statements::start(pg.as_ref()).await;
    let progress = Arc::new(doewood_api::Progress::new());
    let h = {
        let progress = Arc::clone(&progress);
        Harness::start_with(
            Arc::clone(&store),
            Arc::clone(&backend),
            jobs,
            Duration::from_millis(10),
            None,
            Arc::new(move |_| progress.touch()),
        )
    };
    let api = bench::Api::start(&store, &backend, &progress, watchers).await;
    let probe = bench::Probe::start(&store);
    let read_clients: usize = var("DOEWOOD_BENCH_READ_CLIENTS").unwrap_or(1);
    let reads = readers.map(|_| bench::ReadProbe::start(api.addr, h.source, read_clients));
    let cpu = bench::cpu_seconds();
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
    let cpu = bench::cpu_seconds() - cpu;
    let wait = probe.stop();
    let reads = match reads {
        Some(r) => r.stop().await,
        None => "-".into(),
    };
    let messages = api.stop().await;
    let c = store.object_counts(h.source).unwrap();
    let time = h.ingest.stats().time;
    let statements = bench::Statements::stop(statements, n).await;
    let (rate, mib) = (n as f64 / secs, bytes as f64 / f64::from(1 << 20));
    let backend_name = if pg.is_some() { "postgres" } else { "sqlite" };
    println!(
        "throughput: {backend_name}, {n} files ({mib:.1} MiB) with {jobs} workers in \
         {secs:.2}s = {rate:.0} files/s (done {}, skipped {}, failed {}); {watchers} progress \
         watchers ({messages} messages); CPU {cpu:.2}s; store lock wait {wait}; readers {}: \
         ListRecords from {read_clients} clients {reads}",
        c.done,
        c.skipped,
        c.failed,
        readers.map_or_else(|| "in memory".to_owned(), |n| n.to_string()),
    );
    let worker_secs = secs * jobs as f64;
    let share = |d: Duration| {
        format!(
            "{:.1}s ({:.0}%, {:.2} ms/file)",
            d.as_secs_f64(),
            100.0 * d.as_secs_f64() / worker_secs,
            d.as_secs_f64() * 1e3 / n as f64
        )
    };
    println!(
        "throughput: worker time {worker_secs:.1}s: store {}, fetch {}, analyze {}, record {}, \
         graph {} (company lock: wait {}, held {})",
        share(time.store),
        share(time.fetch),
        share(time.analyze),
        share(time.record),
        share(time.graph),
        share(time.company_lock_wait),
        share(time.company_lock_hold),
    );
    if let Some(statements) = statements {
        println!("throughput: {statements}");
    }
    assert_eq!(c.done, n as u64);
    h.stop().await;
    if let Some(pg) = pg {
        pg.drop_schema().await;
    }
    let _ = std::fs::remove_dir_all(&dir);
}

/// The `throughput` measurement's helpers: `WatchProgress` streams on a real API server, and
/// how long a trivial store call waits for the store's lock.
mod bench {
    use std::fmt::Write as _;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
    use std::time::{Duration, Instant};

    use doewood_api::pb::jobs_service_client::JobsServiceClient;
    use doewood_api::pb::records_service_client::RecordsServiceClient;
    use doewood_api::{ApiOptions, ApiParts, ApiState, Progress, pb};
    use doewood_core::InstanceId;
    use doewood_model::Backend;
    use doewood_store::Store;
    use tokio::sync::watch;

    /// An API server with `watchers` open `WatchProgress` streams of every source.
    pub struct Api {
        pub addr: std::net::SocketAddr,
        shutdown: watch::Sender<bool>,
        server: tokio::task::JoinHandle<Result<(), doewood_api::ServeError>>,
        readers: Vec<tokio::task::JoinHandle<()>>,
        messages: Arc<AtomicU64>,
    }

    impl Api {
        pub async fn start(
            store: &Arc<Store>,
            backend: &Arc<dyn Backend>,
            progress: &Arc<Progress>,
            watchers: usize,
        ) -> Self {
            let (shutdown, shutdown_rx) = watch::channel(false);
            let (_backend_tx, backend_rx) = watch::channel(Some(Arc::clone(backend)));
            let state = ApiState::new(ApiParts {
                store: Arc::clone(store),
                backend: backend_rx,
                instance: InstanceId::new("bench").unwrap(),
                discovery: None,
                progress: Arc::clone(progress),
                reprocess: None,
                summarize: None,
                icons: None,
                options: ApiOptions::default(),
                shutdown: shutdown_rx.clone(),
            });
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            let mut stop = shutdown_rx;
            let server = tokio::spawn(doewood_api::serve(listener, state, async move {
                let _ = stop.wait_for(|s| *s).await;
            }));
            let messages = Arc::new(AtomicU64::new(0));
            let mut readers = Vec::new();
            for _ in 0..watchers {
                let channel = tonic::transport::Endpoint::from_shared(format!("http://{addr}"))
                    .unwrap()
                    .connect()
                    .await
                    .unwrap();
                let mut stream = JobsServiceClient::new(channel)
                    .watch_progress(pb::WatchProgressRequest { source_ids: vec![] })
                    .await
                    .unwrap()
                    .into_inner();
                let messages = Arc::clone(&messages);
                readers.push(tokio::spawn(async move {
                    while let Ok(Some(_)) = stream.message().await {
                        messages.fetch_add(1, Ordering::Relaxed);
                    }
                }));
            }
            Self {
                addr,
                shutdown,
                server,
                readers,
                messages,
            }
        }

        /// Stops the server; returns how many progress messages the watchers received.
        pub async fn stop(self) -> u64 {
            self.shutdown.send_replace(true);
            for r in self.readers {
                r.await.unwrap();
            }
            self.server.await.unwrap().unwrap();
            self.messages.load(Ordering::Relaxed)
        }
    }

    /// Times a trivial store call every millisecond: what it waits for the lock.
    pub struct Probe {
        stop: Arc<AtomicBool>,
        thread: std::thread::JoinHandle<Vec<Duration>>,
    }

    impl Probe {
        pub fn start(store: &Arc<Store>) -> Self {
            let stop = Arc::new(AtomicBool::new(false));
            let thread = {
                let (store, stop) = (Arc::clone(store), Arc::clone(&stop));
                std::thread::spawn(move || {
                    let mut waits = Vec::new();
                    while !stop.load(Ordering::Relaxed) {
                        let t = Instant::now();
                        store.schema_version().unwrap();
                        waits.push(t.elapsed());
                        std::thread::sleep(Duration::from_millis(1));
                    }
                    waits
                })
            };
            Self { stop, thread }
        }

        /// Mean, 99th percentile and maximum.
        pub fn stop(self) -> String {
            self.stop.store(true, Ordering::Relaxed);
            stats(self.thread.join().unwrap())
        }
    }

    /// Times a `ListRecords` page of 100 of a source every millisecond, through the API.
    pub struct ReadProbe {
        stop: Arc<AtomicBool>,
        tasks: Vec<tokio::task::JoinHandle<Vec<Duration>>>,
    }

    impl ReadProbe {
        /// `clients` loops, each on its own connection.
        pub fn start(
            addr: std::net::SocketAddr,
            source: doewood_core::SourceId,
            clients: usize,
        ) -> Self {
            let stop = Arc::new(AtomicBool::new(false));
            let tasks = (0..clients.max(1))
                .map(|_| {
                    let stop = Arc::clone(&stop);
                    tokio::spawn(async move {
                        let channel =
                            tonic::transport::Endpoint::from_shared(format!("http://{addr}"))
                                .unwrap()
                                .connect()
                                .await
                                .unwrap();
                        let mut client = RecordsServiceClient::new(channel);
                        let mut waits = Vec::new();
                        while !stop.load(Ordering::Relaxed) {
                            let t = Instant::now();
                            client
                                .list_records(pb::ListRecordsRequest {
                                    source_id: source.to_string(),
                                    page_size: 100,
                                    page_token: String::new(),
                                })
                                .await
                                .unwrap();
                            waits.push(t.elapsed());
                            tokio::time::sleep(Duration::from_millis(1)).await;
                        }
                        waits
                    })
                })
                .collect();
            Self { stop, tasks }
        }

        /// Mean, 99th percentile and maximum.
        pub async fn stop(self) -> String {
            self.stop.store(true, Ordering::Relaxed);
            let mut all = Vec::new();
            for t in self.tasks {
                all.extend(t.await.unwrap());
            }
            stats(all)
        }
    }

    /// Mean, 99th percentile and maximum of `w`.
    fn stats(mut w: Vec<Duration>) -> String {
        w.sort();
        if w.is_empty() {
            return "-".into();
        }
        let total: Duration = w.iter().sum();
        let mean = total / u32::try_from(w.len()).unwrap();
        let p99 = w[w.len() * 99 / 100];
        format!(
            "mean {:.0} us, p99 {:.0} us, max {:.1} ms ({} probes)",
            mean.as_secs_f64() * 1e6,
            p99.as_secs_f64() * 1e6,
            w[w.len() - 1].as_secs_f64() * 1e3,
            w.len()
        )
    }

    /// Statements the database ran during the measurement, from `pg_stat_statements` (when
    /// the server loads it: `shared_preload_libraries=pg_stat_statements`).
    pub struct Statements<'a> {
        admin: &'a tokio_postgres::Client,
    }

    impl<'a> Statements<'a> {
        /// Resets the counters; `None` without Postgres or without the extension.
        pub async fn start(pg: Option<&'a super::Shared>) -> Option<Self> {
            let admin = &pg?.cleanup.as_ref()?.0;
            let ready = admin
                .batch_execute(
                    "CREATE EXTENSION IF NOT EXISTS pg_stat_statements SCHEMA public; \
                     SELECT public.pg_stat_statements_reset();",
                )
                .await;
            if let Err(e) = ready {
                eprintln!("pg_stat_statements is not available ({e}); no statement counts");
                return None;
            }
            Some(Self { admin })
        }

        /// Statements per file, time in the database, and the five costliest statements.
        pub async fn stop(this: Option<Self>, files: usize) -> Option<String> {
            let admin = this?.admin;
            let rows = admin
                .query(
                    "SELECT calls, total_exec_time, left(regexp_replace(query, '\\s+', ' ', 'g'), 90) \
                     FROM public.pg_stat_statements \
                     WHERE query NOT ILIKE '%pg_stat_statements%' \
                     ORDER BY total_exec_time DESC",
                    &[],
                )
                .await
                .ok()?;
            let calls: i64 = rows.iter().map(|r| r.get::<_, i64>(0)).sum();
            let ms: f64 = rows.iter().map(|r| r.get::<_, f64>(1)).sum();
            #[allow(clippy::cast_precision_loss)]
            let mut out = format!(
                "{calls} statements ({:.1} per file), {:.1}s executing in Postgres; costliest:",
                calls as f64 / files as f64,
                ms / 1e3
            );
            for r in rows.iter().take(5) {
                let (c, t, q): (i64, f64, String) = (r.get(0), r.get(1), r.get(2));
                let _ = write!(out, "\n  {c:>7} calls {t:>7.0} ms  {q}");
            }
            Some(out)
        }
    }

    /// User and system CPU time of this process (Linux), in seconds; 0 elsewhere.
    pub fn cpu_seconds() -> f64 {
        let Ok(stat) = std::fs::read_to_string("/proc/self/stat") else {
            return 0.0;
        };
        // Fields after the command name (which may hold spaces): utime is the 12th, stime the
        // 13th, in clock ticks (100 per second on Linux).
        let rest = stat.rsplit_once(')').map_or("", |(_, r)| r);
        let f: Vec<f64> = rest
            .split_whitespace()
            .skip(11)
            .take(2)
            .filter_map(|v| v.parse().ok())
            .collect();
        f.iter().sum::<f64>() / 100.0
    }
}

/// Peak and current RSS while a few spreadsheets too big for their file timeout go through:
/// `cargo test -p doewood-ingest --test pipeline -- --ignored --nocapture timed_out_spreadsheets`
/// (`DOEWOOD_BENCH_ROWS` rows of 26 numbers each, default 200,000; `DOEWOOD_BENCH_TIMEOUT_MS`
/// file timeout, default 300).
#[tokio::test(flavor = "multi_thread")]
#[ignore = "a measurement, not a check"]
async fn timed_out_spreadsheets() {
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

    fn rss_kib() -> u64 {
        let status = std::fs::read_to_string("/proc/self/status").unwrap_or_default();
        status
            .lines()
            .find(|l| l.starts_with("VmRSS:"))
            .and_then(|l| l.split_whitespace().nth(1)?.parse().ok())
            .unwrap_or(0)
    }

    let rows: usize = std::env::var("DOEWOOD_BENCH_ROWS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(200_000);
    let timeout: u64 = std::env::var("DOEWOOD_BENCH_TIMEOUT_MS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(300);
    let files: Vec<(String, Vec<u8>)> = (0..3u64)
        .map(|i| (format!("big/{i}.xlsx"), common::big_xlsx(rows, 26, i)))
        .collect();
    let size: usize = files.iter().map(|(_, d)| d.len()).sum();
    let store = Arc::new(Store::open_in_memory(&MasterKey::from_bytes([10; 32])).unwrap());
    let backend: Arc<dyn Backend> = Arc::new(Standalone::new(Arc::clone(&store)));
    let h = Harness::start_with(
        Arc::clone(&store),
        backend,
        2,
        Duration::from_millis(10),
        Some(Duration::from_millis(timeout)),
        Arc::new(|_| {}),
    );
    let base = rss_kib();
    let (peak, stop) = (
        Arc::new(AtomicU64::new(base)),
        Arc::new(AtomicBool::new(false)),
    );
    let sampler = {
        let (peak, stop) = (Arc::clone(&peak), Arc::clone(&stop));
        std::thread::spawn(move || {
            while !stop.load(Ordering::Relaxed) {
                peak.fetch_max(rss_kib(), Ordering::Relaxed);
                std::thread::sleep(Duration::from_millis(10));
            }
        })
    };
    let started = std::time::Instant::now();
    let batch: Vec<(&str, Vec<u8>, bool)> = files
        .iter()
        .map(|(k, d)| (k.as_str(), d.clone(), true))
        .collect();
    h.put(&batch).await;
    drop(batch);
    h.settle().await;
    let secs = started.elapsed().as_secs_f64();
    stop.store(true, Ordering::Relaxed);
    sampler.join().unwrap();
    let c = store.object_counts(h.source).unwrap();
    let mut errors = Vec::new();
    for i in 0..3 {
        let s = h.status(&format!("big/{i}.xlsx"));
        errors.push(format!("{} {:?}", s.status, s.last_error));
    }
    #[allow(clippy::cast_precision_loss)]
    let (mib, total) = (
        |kib: u64| kib as f64 / 1024.0,
        size as f64 / f64::from(1 << 20),
    );
    println!(
        "timed_out_spreadsheets: 3 x {rows} rows ({:.1} MiB in all), {timeout} ms timeout, \
         2 workers, {secs:.2}s; \
         RSS before {:.0} MiB, peak {:.0} MiB, after {:.0} MiB; done {} failed {}; {errors:?}",
        total,
        mib(base),
        mib(peak.load(Ordering::Relaxed)),
        mib(rss_kib()),
        c.done,
        c.failed,
    );
    h.stop().await;
}

/// Peak RSS while a few big spreadsheets go through at once, with the normal file timeout:
/// `cargo test -p doewood-ingest --test pipeline -- --ignored --nocapture big_spreadsheets`.
/// `DOEWOOD_BENCH_ROWS` rows (default 100,000) of `DOEWOOD_BENCH_COLS` cells (default 26, at
/// most 26), every other one text with `DOEWOOD_BENCH_TEXT=1`; `DOEWOOD_BENCH_FILES` files
/// (default 3) on `DOEWOOD_BENCH_JOBS` workers (default 3), under a memory budget of
/// `DOEWOOD_BENCH_BUDGET_MIB` (default: from the workers and memory).
#[tokio::test(flavor = "multi_thread")]
#[ignore = "a measurement, not a check"]
async fn big_spreadsheets() {
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

    fn rss_kib() -> u64 {
        let status = std::fs::read_to_string("/proc/self/status").unwrap_or_default();
        status
            .lines()
            .find(|l| l.starts_with("VmRSS:"))
            .and_then(|l| l.split_whitespace().nth(1)?.parse().ok())
            .unwrap_or(0)
    }
    fn env(var: &str, default: usize) -> usize {
        std::env::var(var)
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(default)
    }

    let rows = env("DOEWOOD_BENCH_ROWS", 100_000);
    let cols = env("DOEWOOD_BENCH_COLS", 26).min(26);
    let n = env("DOEWOOD_BENCH_FILES", 3);
    let jobs = env("DOEWOOD_BENCH_JOBS", 3);
    let text = env("DOEWOOD_BENCH_TEXT", 0) == 1;
    let files: Vec<(String, Vec<u8>)> = (0..n as u64)
        .map(|i| {
            let data = common::big_xlsx_with(rows, cols, 7000 + i, text);
            (format!("big/{i}.xlsx"), data)
        })
        .collect();
    let size: usize = files.iter().map(|(_, d)| d.len()).sum();
    let store = Arc::new(Store::open_in_memory(&MasterKey::from_bytes([11; 32])).unwrap());
    let backend: Arc<dyn Backend> = Arc::new(Standalone::new(Arc::clone(&store)));
    let budget = env("DOEWOOD_BENCH_BUDGET_MIB", 0) as u64;
    let h = Harness::start_tuned(
        Arc::clone(&store),
        backend,
        jobs,
        Duration::from_millis(10),
        None,
        Arc::new(|_| {}),
        |c| {
            if budget > 0 {
                c.memory_budget = budget << 20;
            }
        },
    );
    let budget = h.ingest.config().memory_budget >> 20;
    let base = rss_kib();
    let (peak, stop) = (
        Arc::new(AtomicU64::new(base)),
        Arc::new(AtomicBool::new(false)),
    );
    let sampler = {
        let (peak, stop) = (Arc::clone(&peak), Arc::clone(&stop));
        std::thread::spawn(move || {
            while !stop.load(Ordering::Relaxed) {
                peak.fetch_max(rss_kib(), Ordering::Relaxed);
                std::thread::sleep(Duration::from_millis(10));
            }
        })
    };
    let started = std::time::Instant::now();
    let batch: Vec<(&str, Vec<u8>, bool)> = files
        .iter()
        .map(|(k, d)| (k.as_str(), d.clone(), true))
        .collect();
    h.put(&batch).await;
    drop(batch);
    h.settle().await;
    let secs = started.elapsed().as_secs_f64();
    stop.store(true, Ordering::Relaxed);
    sampler.join().unwrap();
    let c = store.object_counts(h.source).unwrap();
    let errors: Vec<String> = (0..n)
        .map(|i| {
            let s = h.status(&format!("big/{i}.xlsx"));
            format!("{} {:?}", s.status, s.last_error)
        })
        .collect();
    #[allow(clippy::cast_precision_loss)]
    let (mib, total) = (
        |kib: u64| kib as f64 / 1024.0,
        size as f64 / f64::from(1 << 20),
    );
    let peak = peak.load(Ordering::Relaxed);
    #[allow(clippy::cast_precision_loss)]
    let per_cell = (peak.saturating_sub(base) * 1024) as f64 / (n * rows * cols) as f64;
    println!(
        "big_spreadsheets: {n} x {rows} x {cols} cells (text {text}, {total:.1} MiB in all), \
         {jobs} workers, budget {budget} MiB, {secs:.2}s; RSS before {:.0} MiB, peak {:.0} MiB (+{:.0} MiB, \
         {per_cell:.0} B a cell), after {:.0} MiB; done {} failed {}; {errors:?}",
        mib(base),
        mib(peak),
        mib(peak.saturating_sub(base)),
        mib(rss_kib()),
        c.done,
        c.failed,
    );
    h.stop().await;
}
