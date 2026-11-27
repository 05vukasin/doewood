//! Push sources (ADR 0004): generated PDF, DOCX, XLSX and TXT files handed to ingest as
//! `IngestService.PutObject` hands them, on the SQLite backend and on Postgres when
//! `DOEWOOD_TEST_POSTGRES_URL` is set (in a schema of its own). The record, the location, the
//! graph and search; idempotence; a stale record; move and remove; the checks; and shedding
//! when the worker slots or the memory budget are taken.

mod common;

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use common::MemFetch;
use doewood_core::{MasterKey, SourceId};
use doewood_ingest::{
    Analyzer, AnalyzerConfig, Ingest, IngestConfig, IngestParts, PushError, PushRequest, PushStatus,
};
use doewood_model::{
    Backend, Blake3, Direction, EdgeKind, EdgeState, NeighbourQuery, NodeKind, SearchQuery,
    SourceKey, StepVersions,
};
use doewood_store::{NewSource, SourceState, Standalone, Store};
use tokio::sync::{mpsc, watch};

const PG_VAR: &str = "DOEWOOD_TEST_POSTGRES_URL";

/// The SQLite backend, and Postgres in a fresh schema when the variable is set.
async fn backends(store: &Arc<Store>) -> Vec<(&'static str, Arc<dyn Backend>, Option<Cleanup>)> {
    let mut out: Vec<(&'static str, Arc<dyn Backend>, Option<Cleanup>)> =
        vec![("sqlite", Arc::new(Standalone::new(Arc::clone(store))), None)];
    let Some(url) = std::env::var(PG_VAR).ok().filter(|v| !v.trim().is_empty()) else {
        eprintln!("skipping: {PG_VAR} is not set; the Postgres variant did not run");
        return out;
    };
    let (admin, conn) = tokio_postgres::connect(&url, tokio_postgres::NoTls)
        .await
        .expect("connect to the test database");
    tokio::spawn(conn);
    let schema = format!("dw_push_{}", uuid::Uuid::now_v7().simple());
    admin
        .batch_execute(&format!("CREATE SCHEMA {schema}"))
        .await
        .unwrap();
    let sep = if url.contains('?') { '&' } else { '?' };
    let scoped = format!("{url}{sep}options=-c%20search_path%3D{schema}");
    let url = doewood_core::PostgresUrl::parse(&scoped).unwrap();
    let pg = doewood_pg::PgStore::connect_with_pool(&url, 4)
        .await
        .unwrap();
    out.push(("postgres", Arc::new(pg), Some(Cleanup(admin, schema))));
    out
}

struct Cleanup(tokio_postgres::Client, String);

impl Cleanup {
    async fn run(self) {
        self.0
            .batch_execute(&format!("DROP SCHEMA {} CASCADE", self.1))
            .await
            .unwrap();
    }
}

fn store() -> Arc<Store> {
    Arc::new(Store::open_in_memory(&MasterKey::from_bytes([3; 32])).unwrap())
}

fn analyzer(jobs: usize) -> Arc<Analyzer> {
    let config = AnalyzerConfig {
        workers: jobs,
        ocr_models: PathBuf::from("/nonexistent/doewood-ocr-models"),
        ..AnalyzerConfig::default()
    };
    Arc::new(Analyzer::new(&config).unwrap())
}

/// Ingest with `jobs` workers (started, with no events), its push source, and the shutdown.
fn ingest(
    store: &Arc<Store>,
    backend: &Arc<dyn Backend>,
    jobs: usize,
    tune: impl FnOnce(&mut IngestConfig),
) -> (Ingest, SourceId, watch::Sender<bool>) {
    let id = SourceId::generate();
    store
        .add_source_with_id(id, &NewSource::push(id), None)
        .unwrap();
    let mut config = IngestConfig::new(Some(jobs), &PathBuf::from("/nonexistent"));
    tune(&mut config);
    let (_tx, backend_rx) = watch::channel(Some(Arc::clone(backend)));
    let ingest = Ingest::new(IngestParts {
        store: Arc::clone(store),
        backend: backend_rx,
        fetch: Arc::new(MemFetch::default()),
        analyzer: analyzer(jobs),
        config,
        on_change: Arc::new(|_| {}),
        on_llm_queued: Arc::new(|_| {}),
    });
    let (_events, events_rx) = mpsc::channel(4);
    let (shutdown, shutdown_rx) = watch::channel(false);
    drop(ingest.start(events_rx, shutdown_rx));
    (ingest, id, shutdown)
}

async fn put(
    ingest: &Ingest,
    source: SourceId,
    key: &str,
    bytes: &[u8],
) -> Result<doewood_ingest::Pushed, PushError> {
    let ticket = ingest
        .admit_push(PushRequest {
            source,
            key: key.to_owned(),
            name: String::new(),
            size: bytes.len() as u64,
        })
        .await?;
    ingest.push(ticket, bytes.to_vec()).await
}

fn request(source: SourceId, key: &str, size: u64) -> PushRequest {
    PushRequest {
        source,
        key: key.to_owned(),
        name: String::new(),
        size,
    }
}

async fn located_at(backend: &Arc<dyn Backend>, blake3: Blake3) -> Vec<(String, EdgeState)> {
    let doc = backend
        .node_by_key(NodeKind::Document, &blake3.to_hex())
        .await
        .unwrap()
        .expect("document node");
    let mut out: Vec<(String, EdgeState)> = backend
        .neighbours(
            doc.id,
            &NeighbourQuery {
                direction: Direction::Out,
                edge_kinds: vec![EdgeKind::LocatedAt],
                include_inactive: true,
                ..NeighbourQuery::default()
            },
        )
        .await
        .unwrap()
        .into_iter()
        .map(|n| (n.node.key, n.edge.edge.state))
        .collect();
    out.sort_by(|a, b| a.0.cmp(&b.0));
    out
}

fn contract() -> Vec<u8> {
    common::pdf(
        &[
            "UGOVOR O ISPORUCI br. 7/26",
            "Dobavljac: ACME Promet d.o.o. Beograd",
            "Narucilac: Beta Gradnja a.d. Novi Sad",
            "Ugovor stupa na snagu 01.03.2026. Ukupno: 50.000,00 RSD.",
        ],
        "Ugovor",
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pushed_files_are_analyzed_and_stored_like_s3_objects() {
    let store = store();
    for (name, backend, cleanup) in backends(&store).await {
        let (ingest, id, shutdown) = ingest(&store, &backend, 2, |_| {});
        let files: Vec<(&str, Vec<u8>)> = vec![
            ("drive/contract.pdf", contract()),
            (
                "drive/memo.docx",
                common::docx("Memorandum", &["Pushed straight from the application."]),
            ),
            (
                "drive/prices.xlsx",
                common::xlsx("Prices", &[&["Item", "Price"], &["Bolts", "12"]]),
            ),
            (
                "drive/notes.txt",
                b"Meeting notes: the push source works without a bucket.\n".to_vec(),
            ),
        ];
        let mut hashes = Vec::new();
        for (key, bytes) in &files {
            let p = put(&ingest, id, key, bytes)
                .await
                .unwrap_or_else(|e| panic!("{name} {key}: {e}"));
            assert_eq!(p.status, PushStatus::Analyzed, "{name} {key}");
            assert!(!p.skipped, "{name} {key}");
            assert_eq!(
                p.record.blake3,
                Blake3(*blake3::hash(bytes).as_bytes()),
                "{name} {key}"
            );
            // The record as GetRecord returns it.
            let stored = backend.get_record(&p.record.blake3).await.unwrap().unwrap();
            assert_eq!(stored.body.get(), p.record.body.get(), "{name} {key}");
            assert_eq!(stored.versions, p.record.versions, "{name} {key}");
            assert!(!p.record.versions.is_empty(), "{name} {key}");
            // The location, `["push:<id>", key]`, and the instance store's row.
            let loc = format!("[\"push:{id}\",\"{key}\"]");
            assert_eq!(
                located_at(&backend, p.record.blake3).await,
                [(loc, EdgeState::Active)],
                "{name} {key}"
            );
            let row = store.ingest_status(id, key).unwrap().unwrap();
            assert_eq!(row.status, "done", "{name} {key}");
            assert_eq!(row.blake3, Some(p.record.blake3.0), "{name} {key}");
            hashes.push(p.record.blake3);
        }
        let contract_put = put(&ingest, id, "drive/contract-again.pdf", &files[0].1)
            .await
            .unwrap();
        assert_eq!(contract_put.kind.as_deref(), Some("contract"), "{name}");
        // ListRecords and the search source filter see the push source's records.
        let mut listed: Vec<Blake3> = backend
            .list_records(&SourceKey::push(id), None, 100, usize::MAX)
            .await
            .unwrap()
            .into_iter()
            .map(|r| r.blake3)
            .collect();
        listed.sort();
        hashes.sort();
        assert_eq!(listed, hashes, "{name}");
        let hits = backend
            .search(&SearchQuery {
                text: Some("Bolts".into()),
                source: Some(SourceKey::push(id)),
                ..SearchQuery::default()
            })
            .await
            .unwrap();
        assert_eq!(hits.len(), 1, "{name}");
        assert_eq!(store.object_counts(id).unwrap().done, 5, "{name}");
        shutdown.send_replace(true);
        if let Some(c) = cleanup {
            c.run().await;
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_repeat_put_writes_nothing_and_a_stale_record_is_analyzed_again() {
    let store = store();
    for (name, backend, cleanup) in backends(&store).await {
        let (ingest, id, shutdown) = ingest(&store, &backend, 2, |_| {});
        let bytes = contract();
        let first = put(&ingest, id, "a/contract.pdf", &bytes).await.unwrap();
        assert_eq!(first.status, PushStatus::Analyzed, "{name}");
        let analyzed = ingest.stats().analyzed;
        let again = put(&ingest, id, "a/contract.pdf", &bytes).await.unwrap();
        assert_eq!(again.status, PushStatus::Unchanged, "{name}");
        assert_eq!(again.record.updated_at, first.record.updated_at, "{name}");
        assert_eq!(again.record.body.get(), first.record.body.get(), "{name}");
        // The same content at another key: no analysis, a second location.
        let copy = put(&ingest, id, "b/contract.pdf", &bytes).await.unwrap();
        assert_eq!(copy.status, PushStatus::Reused, "{name}");
        assert_eq!(ingest.stats().analyzed, analyzed, "{name}");
        assert_eq!(
            located_at(&backend, first.record.blake3).await.len(),
            2,
            "{name}"
        );
        // Other content at the same key: analyzed, and the old document's edge superseded.
        let other = b"An amended contract, as plain text.".to_vec();
        let amended = put(&ingest, id, "a/contract.pdf", &other).await.unwrap();
        assert_eq!(amended.status, PushStatus::Analyzed, "{name}");
        let old = located_at(&backend, first.record.blake3).await;
        assert!(
            old.contains(&(
                format!("[\"push:{id}\",\"a/contract.pdf\"]"),
                EdgeState::Superseded
            )),
            "{name}: {old:?}"
        );
        // A record from older step versions is stale: the same put analyzes it again.
        let mut versions: StepVersions = amended.record.versions.clone();
        versions.insert(
            doewood_ingest::analyze::VERSION_INGEST.into(),
            "0-old".into(),
        );
        backend
            .put_record(&amended.record.blake3, &amended.record.body, &versions)
            .await
            .unwrap();
        let redone = put(&ingest, id, "a/contract.pdf", &other).await.unwrap();
        assert_eq!(redone.status, PushStatus::Analyzed, "{name}");
        assert_eq!(redone.record.versions, amended.record.versions, "{name}");
        assert_eq!(ingest.stats().analyzed, analyzed + 2, "{name}");
        shutdown.send_replace(true);
        if let Some(c) = cleanup {
            c.run().await;
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn moves_and_removes_end_the_old_location() {
    let store = store();
    for (name, backend, cleanup) in backends(&store).await {
        let (ingest, id, shutdown) = ingest(&store, &backend, 2, |_| {});
        let bytes = common::docx("Offer", &["An offer, moved by the application."]);
        let p = put(&ingest, id, "inbox/offer.docx", &bytes).await.unwrap();
        let analyzed = ingest.stats().analyzed;
        let moved = ingest
            .move_pushed(id, "inbox/offer.docx", "archive/offer.docx")
            .await
            .unwrap();
        assert_eq!(moved.record.blake3, p.record.blake3, "{name}");
        assert_eq!(
            ingest.stats().analyzed,
            analyzed,
            "{name}: nothing analyzed"
        );
        let at = |k: &str| format!("[\"push:{id}\",\"{k}\"]");
        assert_eq!(
            located_at(&backend, p.record.blake3).await,
            [
                (at("archive/offer.docx"), EdgeState::Active),
                (at("inbox/offer.docx"), EdgeState::Superseded),
            ],
            "{name}"
        );
        assert_eq!(
            store
                .ingest_status(id, "inbox/offer.docx")
                .unwrap()
                .unwrap()
                .status,
            "deleted"
        );
        assert_eq!(
            store
                .ingest_status(id, "archive/offer.docx")
                .unwrap()
                .unwrap()
                .status,
            "done"
        );
        // Moving a key that holds nothing.
        assert!(matches!(
            ingest.move_pushed(id, "inbox/offer.docx", "x.docx").await,
            Err(PushError::NotFound(_))
        ));
        // Remove: the edge ends, the document and its record stay; a second remove is a no-op.
        assert!(
            ingest
                .remove_pushed(id, "archive/offer.docx")
                .await
                .unwrap()
        );
        assert!(
            located_at(&backend, p.record.blake3)
                .await
                .iter()
                .all(|(_, s)| *s == EdgeState::Superseded),
            "{name}"
        );
        assert!(
            backend
                .get_record(&p.record.blake3)
                .await
                .unwrap()
                .is_some()
        );
        assert!(
            !ingest
                .remove_pushed(id, "archive/offer.docx")
                .await
                .unwrap()
        );
        assert!(!ingest.remove_pushed(id, "never/put.docx").await.unwrap());
        let listed = backend
            .list_records(&SourceKey::push(id), None, 100, usize::MAX)
            .await
            .unwrap();
        assert!(listed.is_empty(), "{name}: {} listed", listed.len());
        // Put back after a remove: the record is current, so it is reused.
        let back = put(&ingest, id, "archive/offer.docx", &bytes)
            .await
            .unwrap();
        assert_eq!(back.status, PushStatus::Reused, "{name}");
        shutdown.send_replace(true);
        if let Some(c) = cleanup {
            c.run().await;
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn puts_are_checked_before_their_bytes_are_read() {
    let store = store();
    let backend: Arc<dyn Backend> = Arc::new(Standalone::new(Arc::clone(&store)));
    let (ingest, id, shutdown) = ingest(&store, &backend, 1, |_| {});
    // An S3 source takes no pushes.
    let s3 = store
        .add_source(&NewSource::new("http://objects.test:9000", "docs"), None)
        .unwrap();
    assert!(matches!(
        ingest.admit_push(request(s3, "k", 10)).await,
        Err(PushError::FailedPrecondition(_))
    ));
    // Nor does an unknown source.
    assert!(matches!(
        ingest
            .admit_push(request(SourceId::generate(), "k", 10))
            .await,
        Err(PushError::NotFound(_))
    ));
    // Larger than the source's cap.
    let max = store.source(id).unwrap().unwrap().max_object_bytes();
    assert!(matches!(
        ingest.admit_push(request(id, "k", max + 1)).await,
        Err(PushError::FailedPrecondition(_))
    ));
    // Bytes that do not match the header.
    let ticket = ingest.admit_push(request(id, "k", 10)).await.unwrap();
    assert!(matches!(
        ingest.push(ticket, vec![1; 9]).await,
        Err(PushError::InvalidArgument(_))
    ));
    // One call per key at a time.
    let held = ingest.admit_push(request(id, "same", 10)).await.unwrap();
    assert!(matches!(
        ingest.admit_push(request(id, "same", 10)).await,
        Err(PushError::Unavailable(_))
    ));
    drop(held);
    // Paused: refused until resumed.
    store
        .set_source_state(id, SourceState::Paused, None)
        .unwrap();
    assert!(matches!(
        ingest.admit_push(request(id, "k", 10)).await,
        Err(PushError::FailedPrecondition(_))
    ));
    store
        .set_source_state(id, SourceState::Active, None)
        .unwrap();
    let p = put(&ingest, id, "k.txt", b"plain text again")
        .await
        .unwrap();
    assert_eq!(p.status, PushStatus::Analyzed);
    shutdown.send_replace(true);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_burst_is_shed_when_the_slots_or_the_budget_are_taken() {
    let store = store();
    let backend: Arc<dyn Backend> = Arc::new(Standalone::new(Arc::clone(&store)));
    let (ingest, id, shutdown) = ingest(&store, &backend, 1, |c| {
        c.small_file = 1024;
        c.memory_budget = 4096;
    });
    // The one worker slot is taken; one more push waits for it, a third is shed at once.
    let first = ingest.admit_push(request(id, "a.txt", 10)).await.unwrap();
    let waiting = {
        let ingest = ingest.clone();
        tokio::spawn(async move { ingest.admit_push(request(id, "b.txt", 10)).await })
    };
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(!waiting.is_finished(), "the second push waits for the slot");
    assert!(matches!(
        ingest.admit_push(request(id, "c.txt", 10)).await,
        Err(PushError::ResourceExhausted(_))
    ));
    // Freed: the waiting push gets the slot and runs.
    ingest.push(first, b"first file".to_vec()).await.unwrap();
    let second = tokio::time::timeout(Duration::from_secs(10), waiting)
        .await
        .expect("the waiting push gets the slot")
        .unwrap()
        .unwrap();
    ingest.push(second, b"other file".to_vec()).await.unwrap();
    // A big file takes its share of the budget at once; a second one finds no room.
    let big = ingest
        .admit_push(request(id, "big1.txt", 3000))
        .await
        .unwrap();
    assert!(matches!(
        ingest.admit_push(request(id, "big2.txt", 3000)).await,
        Err(PushError::ResourceExhausted(_))
    ));
    drop(big);
    let big2 = ingest
        .admit_push(request(id, "big2.txt", 3000))
        .await
        .unwrap();
    ingest.push(big2, vec![b'x'; 3000]).await.unwrap();
    shutdown.send_replace(true);
}
