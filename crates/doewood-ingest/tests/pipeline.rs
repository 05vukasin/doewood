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
use doewood_s3::{
    EventKind, EventOrigin, ListPage, ListRequest, ListedObject, Lister, ObjectEvent,
    ObjectVersion, S3Error, ScanOptions, Scanner,
};
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
        Self::start_on(
            store,
            backend,
            jobs,
            retry,
            file_timeout,
            on_change,
            tune,
            source,
        )
    }

    /// [`Harness::start_tuned`] on a source the store already has.
    #[allow(clippy::too_many_arguments)]
    fn start_on(
        store: Arc<Store>,
        backend: Arc<dyn Backend>,
        jobs: usize,
        retry: Duration,
        file_timeout: Option<Duration>,
        on_change: Arc<dyn Fn(SourceId) + Send + Sync>,
        tune: impl FnOnce(&mut IngestConfig),
        source: SourceId,
    ) -> Self {
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
        b.list_records(&src, None, 0, usize::MAX)
            .await
            .unwrap()
            .len(),
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
        b.list_records(&src, None, 0, usize::MAX)
            .await
            .unwrap()
            .is_empty(),
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

/// A bucket in memory, listed like S3 (the token is the last key of the previous page).
#[derive(Default)]
struct MemBucket {
    objects: Vec<ListedObject>,
}

impl Lister for MemBucket {
    async fn list(&self, req: ListRequest<'_>) -> Result<ListPage, S3Error> {
        let after = req.continuation_token.or(req.start_after);
        let rest: Vec<_> = self
            .objects
            .iter()
            .filter(|o| o.key.starts_with(req.prefix) && after.is_none_or(|a| o.key.as_str() > a))
            .collect();
        let objects: Vec<_> = rest
            .iter()
            .take(req.max_keys)
            .map(|o| (*o).clone())
            .collect();
        let next_continuation_token =
            (rest.len() > objects.len()).then(|| objects.last().unwrap().key.clone());
        Ok(ListPage {
            objects,
            next_continuation_token,
        })
    }
}

/// The real scanner's first pass, on a bucket in memory: each object is fetched and ingested
/// once, and every finish matches its pending row. Issue #125: the scanner sent a chunk's
/// events before it committed the chunk's rows, so a worker that finished first matched no
/// row, and the sweep ingested the object a second time.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_scan_pass_ingests_each_object_once() {
    let n = 300;
    let store = Arc::new(Store::open_in_memory(&MasterKey::from_bytes([12; 32])).unwrap());
    let backend: Arc<dyn Backend> = Arc::new(Standalone::new(Arc::clone(&store)));
    let h = Harness::start(Arc::clone(&store), backend, 4, Duration::from_millis(10));
    let mut bucket = MemBucket::default();
    for i in 0..n {
        let key = format!("scan/{i:04}.txt");
        let bytes = format!("Dopis {i}: {}", common::prose(i as u64, 40)).into_bytes();
        bucket.objects.push(ListedObject {
            key: key.clone(),
            etag: Some(format!("e{i}")),
            size: bytes.len() as u64,
            last_modified: Some(1),
        });
        h.fetch.put(&key, bytes);
    }
    let source = store.source(h.source).unwrap().unwrap();
    let scanner = Scanner::new(
        Arc::clone(&store),
        &source,
        h.events.clone(),
        ScanOptions::default(),
    );
    let (_stop_tx, mut stop) = watch::channel(false);
    let started = std::time::Instant::now();
    let pass = scanner.pass(&bucket, &mut stop).await.unwrap();
    assert_eq!(pass.new, n as u64);
    h.settle().await;
    let secs = started.elapsed().as_secs_f64();
    // A few sweeps: anything left to ingest again would be picked up now.
    tokio::time::sleep(Duration::from_millis(600)).await;
    h.settle().await;
    let stats = h.ingest.stats();
    let reads = h.fetch.reads.lock().unwrap().clone();
    let twice = reads.values().filter(|&&r| r > 1).count();
    println!(
        "scan pass: {n} objects in {secs:.2}s; finishes unmatched {}, objects read more than \
         once {twice}, done {}, skipped {}",
        stats.finish_unmatched, stats.done, stats.skipped
    );
    assert_eq!(reads.len(), n);
    assert_eq!((stats.finish_unmatched, twice), (0, 0));
    assert_eq!(stats.done + stats.skipped, n as u64);
    h.stop().await;
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

/// File `i` of `throughput`'s mix: a contract PDF (its supplier distinct, its buyer the same
/// company in every one), an amendment DOCX citing the contract before it, an invoice TXT
/// and a small XLSX, in turn.
fn bench_file(i: usize) -> (String, Vec<u8>) {
    let pib = common::pib(&format!("{:08}", 10_000_000 + i));
    let body = common::prose(i as u64, 300);
    match i % 4 {
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
    }
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
/// `DOEWOOD_BENCH_POOL` sets the Postgres pool's size; it prints the pages' size and the
/// largest the WAL grew). Besides files/s it prints where the workers' time went
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
    let wal = readers.map(|_| dir.join(format!("{}-wal", doewood_store::STORE_FILE)));
    let probe = bench::Probe::start(&store, wal);
    let read_clients: usize = var("DOEWOOD_BENCH_READ_CLIENTS").unwrap_or(1);
    let reads = readers.map(|_| bench::ReadProbe::start(api.addr, h.source, read_clients));
    let cpu = bench::cpu_seconds();
    let mut files = Vec::with_capacity(n);
    let mut bytes = 0usize;
    for i in 0..n {
        let (key, data) = bench_file(i);
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

/// File `i` of the `scale` corpus: [`bench_file`]'s mix, except that every 20th file is a
/// near-duplicate TXT, three words changed in one of 50 template texts (Jaccard well above
/// the 0.8 of `similar_to`).
fn scale_file(i: usize) -> (String, Vec<u8>) {
    if i % 20 != 19 {
        return bench_file(i);
    }
    let text = common::prose(1_000_000 + (i as u64 / 20) % 50, 600);
    let mut words: Vec<String> = text.split(' ').map(str::to_owned).collect();
    for (k, step) in [7, 13, 29].into_iter().enumerate() {
        let at = (i * step + k * 200) % words.len();
        words[at] = format!("w{i}x{k}");
    }
    (format!("near/{i}.txt"), words.join(" ").into_bytes())
}

/// Issue #127: the system at 50,000 to 200,000 documents. The corpus is [`scale_file`]'s, put
/// as `throughput` puts it; the instance store is a file in `DOEWOOD_SCALE_DIR` (with one
/// read-only connection per CPU, as `serve` opens it) and stays there between runs, and so
/// does the shared store: SQLite (that same file), or Postgres in the schema
/// `DOEWOOD_SCALE_PG_SCHEMA` (default `dw_scale`) when `DOEWOOD_TEST_POSTGRES_URL` is set.
/// `DOEWOOD_SCALE_PHASE` picks what a run does, so a big store is built in runs of a few
/// minutes and then measured:
///
/// - `ingest` (the default): adds files until the corpus has `DOEWOOD_SCALE_TO` (default
///   50,000), at most `DOEWOOD_SCALE_RUN` (default 25,000) in this run, and prints files/s
///   and the graph stage per 5,000 files, then the writer lock wait (a trivial call every
///   millisecond) and the WAL's largest size and resets. The keys and sizes go to `keys.tsv`
///   in the directory, so the next run continues where this one stopped.
/// - `sweep`: the ingest sweep's pending query over the whole source, and `object_counts`
///   (each progress snapshot reads it), median of 11.
/// - `scan`: two scanner passes over a bucket listing every key unchanged, with the writer
///   lock wait.
/// - `due`: `companies_due` (no registries are called: only the query), the first page and a
///   whole pass in pages of 100, as the enrichment job reads it.
/// - `api`: one client through the API: `ListRecords` (the first page of 100), `Search` (a
///   word in every fourth document, and the biggest company's name), `GetNode`, `Neighbours`
///   (`similar_to` of a near-duplicate) and `CompanyDocuments` of the biggest company (the
///   buyer of every contract), median of 11 (`api_load` measures many clients on a store of
///   its own).
///
/// `cargo test --release -p doewood-ingest --test pipeline -- --ignored --nocapture --exact scale`
#[tokio::test(flavor = "multi_thread")]
#[ignore = "a measurement (issue #127); see docs/deploy.md"]
#[allow(clippy::too_many_lines, clippy::cast_precision_loss)]
async fn scale() {
    let var = |name: &str| std::env::var(name).ok().and_then(|v| v.parse().ok());
    let dir = PathBuf::from(std::env::var("DOEWOOD_SCALE_DIR").expect("DOEWOOD_SCALE_DIR"));
    std::fs::create_dir_all(&dir).unwrap();
    let phase = std::env::var("DOEWOOD_SCALE_PHASE").unwrap_or_else(|_| "ingest".into());
    let file = dir.join(doewood_store::STORE_FILE);
    let store = Arc::new(Store::open(&file, &MasterKey::from_bytes([8; 32])).unwrap());
    let (backend, admin): (Arc<dyn Backend>, _) = match std::env::var(PG_VAR) {
        Ok(url) if !url.trim().is_empty() => {
            let schema =
                std::env::var("DOEWOOD_SCALE_PG_SCHEMA").unwrap_or_else(|_| "dw_scale".into());
            let (admin, conn) = tokio_postgres::connect(&url, tokio_postgres::NoTls)
                .await
                .unwrap();
            tokio::spawn(conn);
            admin
                .batch_execute(&format!("CREATE SCHEMA IF NOT EXISTS {schema}"))
                .await
                .unwrap();
            let sep = if url.contains('?') { '&' } else { '?' };
            let scoped = format!("{url}{sep}options=-c%20search_path%3D{schema}");
            let url = doewood_core::PostgresUrl::parse(&scoped).unwrap();
            let pg = doewood_pg::PgStore::connect_with_pool(&url, doewood_pg::default_pool_size())
                .await
                .unwrap();
            (Arc::new(pg), Some(admin))
        }
        _ => (Arc::new(Standalone::new(Arc::clone(&store))), None),
    };
    let source = match store.sources().unwrap().first() {
        Some(s) => s.id,
        None => store
            .add_source(&NewSource::new(ENDPOINT, BUCKET), None)
            .unwrap(),
    };
    let keys_file = dir.join("keys.tsv");
    let keys: Vec<(String, u64)> = std::fs::read_to_string(&keys_file)
        .unwrap_or_default()
        .lines()
        .filter_map(|l| {
            let (k, n) = l.split_once('\t')?;
            Some((k.to_owned(), n.parse().ok()?))
        })
        .collect();
    let wal = dir.join(format!("{}-wal", doewood_store::STORE_FILE));
    println!(
        "scale: {phase}, {} backend, {} files so far, store readers {}",
        if admin.is_some() { "postgres" } else { "sqlite" },
        keys.len(),
        store.readers()
    );
    let median = |mut v: Vec<f64>| {
        v.sort_by(f64::total_cmp);
        v[v.len() / 2]
    };
    match phase.as_str() {
        "ingest" => {
            let to: usize = var("DOEWOOD_SCALE_TO").unwrap_or(50_000);
            let run: usize = var("DOEWOOD_SCALE_RUN").unwrap_or(25_000);
            let jobs = doewood_ingest::default_jobs();
            let h = Harness::start_on(
                Arc::clone(&store),
                Arc::clone(&backend),
                jobs,
                Duration::from_millis(10),
                None,
                Arc::new(|_| {}),
                |_| {},
                source,
            );
            let probe = bench::Probe::start(&store, Some(wal));
            let (from, end) = (keys.len(), to.min(keys.len() + run));
            let mut out = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(&keys_file)
                .unwrap();
            let started = std::time::Instant::now();
            let mut at = from;
            while at < end {
                let next = (at + 5000).min(end);
                let files: Vec<(String, Vec<u8>)> = (at..next).map(scale_file).collect();
                let before = h.ingest.stats().time;
                let t = std::time::Instant::now();
                for chunk in files.chunks(64) {
                    let batch: Vec<(&str, Vec<u8>, bool)> = chunk
                        .iter()
                        .map(|(k, d)| (k.as_str(), d.clone(), true))
                        .collect();
                    h.put(&batch).await;
                }
                h.settle().await;
                let secs = t.elapsed().as_secs_f64();
                let time = h.ingest.stats().time;
                let ms = |a: Duration, b: Duration| {
                    (a - b).as_secs_f64() * 1e3 / (next - at) as f64
                };
                println!(
                    "scale: files {at}..{next}: {:.0} files/s; per file: store {:.2} ms, record \
                     {:.2} ms, graph {:.2} ms (company lock wait {:.2}, held {:.2})",
                    (next - at) as f64 / secs,
                    ms(time.store, before.store),
                    ms(time.record, before.record),
                    ms(time.graph, before.graph),
                    ms(time.company_lock_wait, before.company_lock_wait),
                    ms(time.company_lock_hold, before.company_lock_hold),
                );
                use std::io::Write as _;
                for (k, d) in &files {
                    writeln!(out, "{k}\t{}", d.len()).unwrap();
                }
                out.flush().unwrap();
                h.fetch.objects.lock().unwrap().clear();
                at = next;
            }
            let c = store.object_counts(source).unwrap();
            println!(
                "scale: {} files in {:.1}s; store lock wait {}; done {}, skipped {}, failed {}",
                end - from,
                started.elapsed().as_secs_f64(),
                probe.stop(),
                c.done,
                c.skipped,
                c.failed
            );
            h.stop().await;
        }
        "sweep" => {
            let mut sweep = Vec::new();
            let mut counts = Vec::new();
            for _ in 0..11 {
                let t = std::time::Instant::now();
                let mut after: Option<String> = None;
                loop {
                    let page = store.pending_objects(source, after.as_deref(), 256).unwrap();
                    after = page.last().map(|o| o.key.clone());
                    if page.len() < 256 {
                        break;
                    }
                }
                sweep.push(t.elapsed().as_secs_f64() * 1e3);
                let t = std::time::Instant::now();
                store.object_counts(source).unwrap();
                counts.push(t.elapsed().as_secs_f64() * 1e3);
            }
            println!(
                "scale: sweep (nothing pending) {:.3} ms; object_counts {:.3} ms",
                median(sweep),
                median(counts)
            );
        }
        "scan" => {
            let mut bucket = MemBucket::default();
            for (k, n) in &keys {
                bucket.objects.push(ListedObject {
                    key: k.clone(),
                    etag: Some(format!("{n:x}")),
                    size: *n,
                    last_modified: Some(1),
                });
            }
            bucket.objects.sort_by(|a, b| a.key.cmp(&b.key));
            let (tx, mut rx) = mpsc::channel(1024);
            let events = tokio::spawn(async move {
                let mut n = 0u64;
                while rx.recv().await.is_some() {
                    n += 1;
                }
                n
            });
            let src = store.source(source).unwrap().unwrap();
            let scanner = Scanner::new(Arc::clone(&store), &src, tx, ScanOptions::default());
            let (_stop_tx, mut stop) = watch::channel(false);
            for _ in 0..2 {
                let probe = bench::Probe::start(&store, Some(wal.clone()));
                let cpu = bench::cpu_seconds();
                let t = std::time::Instant::now();
                let pass = scanner.pass(&bucket, &mut stop).await.unwrap();
                println!(
                    "scale: scan pass {} over {} keys: {:.2}s, CPU {:.2}s, new {} changed {} \
                     deleted {}; store lock wait {}",
                    pass.pass,
                    pass.listed,
                    t.elapsed().as_secs_f64(),
                    bench::cpu_seconds() - cpu,
                    pass.new,
                    pass.changed,
                    pass.deleted,
                    probe.stop()
                );
            }
            drop(scanner);
            println!("scale: events sent {}", events.await.unwrap());
        }
        "due" => {
            let owner = doewood_core::InstanceId::new("scale").unwrap();
            let mut first = Vec::new();
            for _ in 0..5 {
                let t = std::time::Instant::now();
                backend.companies_due(&owner, None, 100).await.unwrap();
                first.push(t.elapsed().as_secs_f64() * 1e3);
            }
            let t = std::time::Instant::now();
            let (mut after, mut pages, mut companies, mut slowest) = (None, 0, 0, 0f64);
            loop {
                let p = std::time::Instant::now();
                let page = backend.companies_due(&owner, after, 100).await.unwrap();
                slowest = slowest.max(p.elapsed().as_secs_f64() * 1e3);
                let Some(last) = page.last() else { break };
                after = Some(last.company);
                pages += 1;
                companies += page.len();
            }
            println!(
                "scale: companies_due first page {:.2} ms; a pass: {companies} companies in \
                 {pages} pages, {:.2}s, slowest page {slowest:.2} ms",
                median(first),
                t.elapsed().as_secs_f64()
            );
        }
        "api" => scale_api(&store, &backend, source, &keys).await,
        other => panic!("unknown DOEWOOD_SCALE_PHASE {other}"),
    }
}

/// The `api` phase of [`scale`].
#[allow(clippy::cast_precision_loss)]
async fn scale_api(
    store: &Arc<Store>,
    backend: &Arc<dyn Backend>,
    source: SourceId,
    keys: &[(String, u64)],
) {
    use doewood_api::pb;
    use doewood_api::pb::graph_service_client::GraphServiceClient;
    use doewood_api::pb::records_service_client::RecordsServiceClient;
    use doewood_api::pb::search_service_client::SearchServiceClient;

    let doc = |key: &str| {
        let hash = store
            .ingest_status(source, key)
            .unwrap()
            .and_then(|s| s.blake3)
            .map(Blake3)
            .expect("an ingested key");
        let backend = Arc::clone(backend);
        async move {
            backend
                .node_by_key(NodeKind::Document, &hash.to_hex())
                .await
                .unwrap()
                .expect("its document")
                .id
        }
    };
    let middle = doc(&keys[keys.len() / 2].0).await;
    let near = keys
        .iter()
        .rev()
        .find(|(k, _)| k.starts_with("near/"))
        .expect("a near-duplicate");
    let near = doc(&near.0).await;
    // The biggest company, from the due list (every company is due: nothing was enriched).
    let owner = doewood_core::InstanceId::new("scale").unwrap();
    let (mut after, mut big) = (None, (uuid::Uuid::nil(), 0));
    loop {
        let page = backend
            .companies_due(&owner, after, doewood_model::DUE_MAX_LIMIT)
            .await
            .unwrap();
        let Some(last) = page.last() else { break };
        after = Some(last.company);
        for d in page {
            if d.documents > big.1 {
                big = (d.company, d.documents);
            }
        }
    }
    let progress = Arc::new(doewood_api::Progress::new());
    let api = bench::Api::start(store, backend, &progress, 0).await;
    let channel = tonic::transport::Endpoint::from_shared(format!("http://{}", api.addr))
        .unwrap()
        .connect()
        .await
        .unwrap();
    let mut records = RecordsServiceClient::new(channel.clone());
    let mut graph = GraphServiceClient::new(channel.clone());
    let mut search = SearchServiceClient::new(channel);
    let mut report = Vec::new();
    macro_rules! time {
        ($name:expr, $call:expr, $items:expr) => {{
            let mut ms = Vec::new();
            let mut items = 0;
            let mut error = None;
            for _ in 0..11 {
                let t = std::time::Instant::now();
                match $call.await {
                    Ok(r) => items = $items(r.into_inner()),
                    Err(e) => error = Some(e.code()),
                }
                ms.push(t.elapsed().as_secs_f64() * 1e3);
            }
            ms.sort_by(f64::total_cmp);
            report.push(format!(
                "{} {:.2} ms (max {:.2}), {items} items{}",
                $name,
                ms[5],
                ms[10],
                error.map_or_else(String::new, |c| format!(", error {c:?}"))
            ));
        }};
    }
    time!(
        "ListRecords",
        records.list_records(pb::ListRecordsRequest {
            source_id: source.to_string(),
            page_size: 100,
            page_token: String::new(),
        }),
        |r: pb::ListRecordsResponse| r.records.len()
    );
    for text in ["Ugovor", "Kupac Grupa"] {
        time!(
            format!("Search {text:?}"),
            search.search(pb::SearchRequest {
                text: text.into(),
                limit: 20,
                ..pb::SearchRequest::default()
            }),
            |r: pb::SearchResponse| r.hits.len()
        );
    }
    time!(
        "GetNode",
        graph.get_node(pb::GetNodeRequest {
            lookup: Some(pb::get_node_request::Lookup::Id(middle.to_string())),
        }),
        |_| 1
    );
    time!(
        "Neighbours",
        graph.neighbours(pb::NeighboursRequest {
            node_id: near.to_string(),
            direction: pb::Direction::Unspecified as i32,
            edge_kinds: vec![pb::EdgeKind::SimilarTo as i32],
            node_kind: pb::NodeKind::Document as i32,
            at: String::new(),
            include_inactive: false,
        }),
        |r: pb::NeighboursResponse| r.neighbours.len()
    );
    time!(
        format!("CompanyDocuments ({} documents)", big.1),
        graph.company_documents(pb::CompanyDocumentsRequest {
            company_id: big.0.to_string(),
        }),
        |r: pb::CompanyDocumentsResponse| r.documents.len()
    );
    println!("scale: api, one client, median of 11:");
    for line in report {
        println!("  {line}");
    }
    api.stop().await;
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

    /// Times a trivial store call every millisecond: what it waits for the lock. With the
    /// store on disk, also the largest size its WAL file reached, and how many times a
    /// checkpoint reset it (the checkpoint sequence number in the WAL header, bytes 12 to 15).
    pub struct Probe {
        stop: Arc<AtomicBool>,
        thread: std::thread::JoinHandle<(Vec<Duration>, u64, u32)>,
    }

    /// The WAL header's checkpoint sequence number, when the file has a header.
    fn wal_sequence(wal: &std::path::Path) -> Option<u32> {
        use std::io::Read as _;
        let mut head = [0u8; 16];
        std::fs::File::open(wal).ok()?.read_exact(&mut head).ok()?;
        Some(u32::from_be_bytes([head[12], head[13], head[14], head[15]]))
    }

    impl Probe {
        pub fn start(store: &Arc<Store>, wal: Option<std::path::PathBuf>) -> Self {
            let stop = Arc::new(AtomicBool::new(false));
            let thread = {
                let (store, stop) = (Arc::clone(store), Arc::clone(&stop));
                std::thread::spawn(move || {
                    let (mut waits, mut wal_max, mut resets) = (Vec::new(), 0, 0);
                    let mut seq = None;
                    while !stop.load(Ordering::Relaxed) {
                        let t = Instant::now();
                        store.schema_version().unwrap();
                        waits.push(t.elapsed());
                        if let Some(w) = wal.as_ref() {
                            if let Ok(m) = std::fs::metadata(w) {
                                wal_max = wal_max.max(m.len());
                            }
                            if let Some(now) = wal_sequence(w) {
                                if let Some(before) = seq {
                                    resets += now.wrapping_sub(before);
                                }
                                seq = Some(now);
                            }
                        }
                        std::thread::sleep(Duration::from_millis(1));
                    }
                    (waits, wal_max, resets)
                })
            };
            Self { stop, thread }
        }

        /// Mean, 99th percentile and maximum; the WAL's largest size and its resets.
        pub fn stop(self) -> String {
            self.stop.store(true, Ordering::Relaxed);
            let (waits, wal_max, resets) = self.thread.join().unwrap();
            format!(
                "{}, WAL at most {} MiB, reset {resets} times",
                stats(waits),
                wal_max >> 20
            )
        }
    }

    /// Times a `ListRecords` page of 100 of a source every millisecond, through the API.
    pub struct ReadProbe {
        stop: Arc<AtomicBool>,
        tasks: Vec<tokio::task::JoinHandle<Vec<Duration>>>,
        bytes: Arc<AtomicU64>,
    }

    impl ReadProbe {
        /// `clients` loops, each on its own connection.
        pub fn start(
            addr: std::net::SocketAddr,
            source: doewood_core::SourceId,
            clients: usize,
        ) -> Self {
            let stop = Arc::new(AtomicBool::new(false));
            let bytes = Arc::new(AtomicU64::new(0));
            let tasks = (0..clients.max(1))
                .map(|_| {
                    let (stop, bytes) = (Arc::clone(&stop), Arc::clone(&bytes));
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
                            let page = client
                                .list_records(pb::ListRecordsRequest {
                                    source_id: source.to_string(),
                                    page_size: 100,
                                    page_token: String::new(),
                                })
                                .await
                                .unwrap()
                                .into_inner();
                            waits.push(t.elapsed());
                            let body: usize = page.records.iter().map(|r| r.body_json.len()).sum();
                            bytes.fetch_add(body as u64, Ordering::Relaxed);
                            tokio::time::sleep(Duration::from_millis(1)).await;
                        }
                        waits
                    })
                })
                .collect();
            Self { stop, tasks, bytes }
        }

        /// Mean, 99th percentile and maximum, and the pages' mean size (record bodies).
        pub async fn stop(self) -> String {
            self.stop.store(true, Ordering::Relaxed);
            let mut all = Vec::new();
            for t in self.tasks {
                all.extend(t.await.unwrap());
            }
            let pages = all.len().max(1) as u64;
            let kib = self.bytes.load(Ordering::Relaxed) / pages / 1024;
            format!("{}, {kib} KiB of bodies a page", stats(all))
        }
    }

    /// Mean, 99th percentile and maximum of `w`.
    pub fn stats(mut w: Vec<Duration>) -> String {
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
