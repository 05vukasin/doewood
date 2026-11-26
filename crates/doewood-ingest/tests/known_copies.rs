//! Known copies (issue #131): a new key whose single-part MD5 ETag and size match content
//! already hashed is recorded against that content without a GetObject, only when its source
//! opted in, and only when HEAD confirms the ETag and size and reports no SSE-KMS or SSE-C.
//! The end state is the same as after a full ingest of the same bytes.

mod common;

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use common::MemFetch;
use doewood_core::{MasterKey, SourceId};
use doewood_ingest::{Analyzer, AnalyzerConfig, Ingest, IngestConfig, IngestParts, Stats};
use doewood_model::{Backend, Blake3, Direction, NeighbourQuery, NodeKind};
use doewood_s3::Encryption;
use doewood_store::{NewSource, ObjectChange, SeenObject, Standalone, Store};
use tokio::sync::{mpsc, watch};

const ENDPOINT: &str = "http://objects.test:9000";
const MD5: &str = "0123456789abcdef0123456789abcdef";
const OTHER_MD5: &str = "fedcba9876543210fedcba9876543210";
const MULTIPART: &str = "0123456789abcdef0123456789abcdef-3";

struct Run {
    store: Arc<Store>,
    backend: Arc<dyn Backend>,
    fetch: Arc<MemFetch>,
    ingest: Ingest,
    _events: mpsc::Sender<doewood_s3::ObjectEvent>,
    shutdown: watch::Sender<bool>,
    handle: Option<doewood_ingest::IngestHandle>,
}

impl Run {
    fn start() -> Self {
        let store = Arc::new(Store::open_in_memory(&MasterKey::from_bytes([31; 32])).unwrap());
        let backend: Arc<dyn Backend> = Arc::new(Standalone::new(Arc::clone(&store)));
        let fetch = Arc::new(MemFetch::default());
        let mut config = IngestConfig::new(Some(1), &PathBuf::from("/nonexistent"));
        config.retry_base = Duration::from_millis(10);
        config.sweep_interval = Duration::from_millis(50);
        let (_tx, backend_rx) = watch::channel(Some(Arc::clone(&backend)));
        let analyzer = Arc::new(
            Analyzer::new(&AnalyzerConfig {
                workers: 1,
                ocr_models: PathBuf::from("/nonexistent/doewood-ocr-models"),
                ..AnalyzerConfig::default()
            })
            .unwrap(),
        );
        let ingest = Ingest::new(IngestParts {
            store: Arc::clone(&store),
            backend: backend_rx,
            fetch: Arc::clone(&fetch) as Arc<dyn doewood_ingest::Fetch>,
            analyzer,
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
            ingest,
            _events: events,
            shutdown,
            handle: Some(handle),
        }
    }

    fn source(&self, bucket: &str, skip_known_copies: bool) -> SourceId {
        let mut s = NewSource::new(ENDPOINT, bucket);
        s.skip_known_copies = skip_known_copies;
        self.store.add_source(&s, None).unwrap()
    }

    /// Puts `key` in the bucket with the ETag its listing and responses report, records it
    /// as discovery does, and waits until it is ingested.
    async fn ingest(
        &self,
        source: SourceId,
        key: &str,
        bytes: &[u8],
        etag: &str,
        encryption: Encryption,
    ) {
        self.fetch.put_with(key, bytes.to_vec(), etag, encryption);
        self.store
            .record_objects(
                source,
                &[ObjectChange::Upsert(SeenObject {
                    key: key.to_owned(),
                    etag: Some(etag.to_owned()),
                    size: bytes.len() as u64,
                    last_modified: Some(1),
                })],
                None,
            )
            .unwrap();
        let deadline = tokio::time::Instant::now() + Duration::from_mins(2);
        while self.store.object_counts(source).unwrap().pending > 0 {
            assert!(tokio::time::Instant::now() < deadline, "{key} not ingested");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    /// What a key ended as, and its document's `located_at` locations and other edges, without
    /// ids or times: equal for a known copy and a full ingest of the same bytes.
    async fn state(&self, source: SourceId, key: &str) -> (String, Blake3, Vec<String>) {
        let st = self.store.ingest_status(source, key).unwrap().unwrap();
        let blake3 = Blake3(st.blake3.expect("hashed"));
        let doc = self
            .backend
            .node_by_key(NodeKind::Document, &blake3.to_hex())
            .await
            .unwrap()
            .expect("document node");
        let mut edges: Vec<String> = self
            .backend
            .neighbours(
                doc.id,
                &NeighbourQuery {
                    direction: Direction::Out,
                    include_inactive: true,
                    ..NeighbourQuery::default()
                },
            )
            .await
            .unwrap()
            .into_iter()
            .map(|n| {
                format!(
                    "{:?} {:?} {} {:?}",
                    n.edge.edge.kind, n.node.kind, n.node.key, n.edge.edge.state
                )
            })
            .collect();
        edges.sort();
        (st.status, blake3, edges)
    }

    fn stats(&self) -> Stats {
        self.ingest.stats()
    }

    async fn stop(mut self) {
        self.shutdown.send_replace(true);
        if let Some(h) = self.handle.take() {
            h.join().await;
        }
    }
}

fn contract() -> Vec<u8> {
    let p1 = common::pib("10123456");
    common::pdf(
        &[
            "UGOVOR O ISPORUCI br. 114/23",
            &format!("Dobavljac: ACME Promet d.o.o. Beograd, PIB {p1}"),
            "Narucilac: Beta Gradnja a.d. Novi Sad",
        ],
        "Ugovor",
    )
}

/// The copy under another key and source: with the option on it is not fetched, and the end
/// state (status, hash, the document's locations, parties and other edges) equals that of a
/// full ingest of the same bytes, which is what the option off gives.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_known_copy_is_located_without_a_fetch() {
    let bytes = contract();
    let mut ends = Vec::new();
    for skip in [false, true] {
        let run = Run::start();
        let a = run.source("originals", false);
        let b = run.source("copies", skip);
        run.ingest(a, "a/contract.pdf", &bytes, MD5, Encryption::None)
            .await;
        run.ingest(b, "b/copy.pdf", &bytes, MD5, Encryption::S3)
            .await;
        let stats = run.stats();
        let (reads, heads) = (
            run.fetch.reads_of("b/copy.pdf"),
            run.fetch.heads_of("b/copy.pdf"),
        );
        if skip {
            assert_eq!((reads, heads), (0, 1), "the copy is HEAD only");
            assert_eq!(stats.known_copies, 1);
            assert_eq!(stats.known_copy_bytes, bytes.len() as u64);
            assert_eq!(stats.bytes, bytes.len() as u64, "only the original fetched");
        } else {
            assert_eq!((reads, heads), (1, 0), "off: fetched, no HEAD");
            assert_eq!((stats.known_copies, stats.known_copy_bytes), (0, 0));
            assert_eq!(stats.bytes, 2 * bytes.len() as u64);
        }
        assert_eq!(stats.analyzed, 1, "the copy reuses the record either way");
        assert_eq!(stats.done, 2);
        let end = run.state(b, "b/copy.pdf").await;
        assert_eq!(end, run.state(a, "a/contract.pdf").await);
        assert_eq!(end.0, "done");
        let located = end.2.iter().filter(|e| e.starts_with("LocatedAt")).count();
        assert_eq!(located, 2, "{:?}", end.2);
        ends.push(end);
        run.stop().await;
    }
    assert_eq!(ends[0], ends[1], "a known copy ends as a full ingest does");
}

/// Every case that must fetch: the option off, a multipart ETag, SSE-KMS or SSE-C on the copy
/// or on the original, and another size.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn copies_that_cannot_be_trusted_are_fetched() {
    let bytes = b"Dopis: plain text that is not a known format but has its identity.".to_vec();
    let run = Run::start();
    let a = run.source("originals", false);
    let off = run.source("off", false);
    let on = run.source("on", true);
    run.ingest(a, "a/plain.txt", &bytes, MD5, Encryption::None)
        .await;
    // The original's ETag is known (single-part, no KMS): a trusted copy is not fetched.
    run.ingest(on, "on/trusted.txt", &bytes, MD5, Encryption::None)
        .await;
    assert_eq!(run.fetch.reads_of("on/trusted.txt"), 0);

    run.ingest(off, "off/copy.txt", &bytes, MD5, Encryption::None)
        .await;
    for (key, encryption) in [
        ("on/kms.txt", Encryption::Kms),
        ("on/customer-key.txt", Encryption::CustomerKey),
        ("on/other-sse.txt", Encryption::Other),
    ] {
        run.ingest(on, key, &bytes, MD5, encryption).await;
    }
    // Same ETag, other bytes: the listed size misses.
    let mut longer = bytes.clone();
    longer.extend_from_slice(b" More.");
    run.ingest(on, "on/longer.txt", &longer, MD5, Encryption::None)
        .await;
    // Multipart: never recorded and never trusted.
    let parts = b"Dopis: uploaded in parts, so its ETag is not an MD5 of the content.".to_vec();
    run.ingest(a, "a/parts.txt", &parts, MULTIPART, Encryption::None)
        .await;
    run.ingest(on, "on/parts.txt", &parts, MULTIPART, Encryption::None)
        .await;
    // An original under SSE-KMS is not recorded either, whatever ETag it shows.
    let sealed = b"Dopis: stored under SSE-KMS, its ETag is not the MD5.".to_vec();
    run.ingest(a, "a/sealed.txt", &sealed, OTHER_MD5, Encryption::Kms)
        .await;
    run.ingest(on, "on/sealed.txt", &sealed, OTHER_MD5, Encryption::None)
        .await;
    for key in [
        "off/copy.txt",
        "on/kms.txt",
        "on/customer-key.txt",
        "on/other-sse.txt",
        "on/longer.txt",
        "on/parts.txt",
        "on/sealed.txt",
    ] {
        assert_eq!(run.fetch.reads_of(key), 1, "{key} is fetched");
    }
    assert_eq!(
        run.fetch.heads_of("off/copy.txt"),
        0,
        "off: not even a HEAD"
    );
    assert_eq!(
        run.fetch.heads_of("on/parts.txt"),
        0,
        "multipart: no lookup"
    );
    assert_eq!(
        run.fetch.heads_of("on/longer.txt"),
        0,
        "another size: no match"
    );
    assert_eq!(
        run.fetch.heads_of("on/sealed.txt"),
        0,
        "a KMS original is unknown"
    );
    assert_eq!(run.fetch.heads_of("on/kms.txt"), 1, "HEAD shows the KMS");
    assert_eq!(run.stats().known_copies, 1);
    // Every copy still ends as its content does.
    for (src, key, original) in [
        (off, "off/copy.txt", "a/plain.txt"),
        (on, "on/kms.txt", "a/plain.txt"),
        (on, "on/parts.txt", "a/parts.txt"),
    ] {
        assert_eq!(
            run.state(src, key).await.1,
            run.state(a, original).await.1,
            "{key}"
        );
    }
    run.stop().await;
}

/// HEAD reporting another ETag than the listing (the object changed meanwhile) fetches it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_head_with_another_etag_fetches() {
    let bytes = b"Dopis: a copy that changed between the listing and the HEAD.".to_vec();
    let run = Run::start();
    let a = run.source("originals", false);
    let on = run.source("on", true);
    run.ingest(a, "a/plain.txt", &bytes, MD5, Encryption::None)
        .await;
    // Listed with the known ETag; the object's responses already report the next version.
    run.fetch
        .put_with("on/changed.txt", bytes.clone(), OTHER_MD5, Encryption::None);
    run.store
        .record_objects(
            on,
            &[ObjectChange::Upsert(SeenObject {
                key: "on/changed.txt".into(),
                etag: Some(MD5.into()),
                size: bytes.len() as u64,
                last_modified: Some(1),
            })],
            None,
        )
        .unwrap();
    let deadline = tokio::time::Instant::now() + Duration::from_mins(1);
    while run.store.object_counts(on).unwrap().pending > 0 {
        assert!(tokio::time::Instant::now() < deadline, "not ingested");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(run.fetch.heads_of("on/changed.txt"), 1);
    assert_eq!(run.fetch.reads_of("on/changed.txt"), 1);
    assert_eq!(run.stats().known_copies, 0);
    run.stop().await;
}
