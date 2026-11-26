//! End to end through real servers: an S3 put, the scanner, ingest, and the record, graph and
//! search in Postgres; then an S3 delete ends the location. Runs only when both
//! `DOEWOOD_TEST_S3_ENDPOINT` and `DOEWOOD_TEST_POSTGRES_URL` are set (CI's `ingest-e2e` job;
//! locally, MinIO and pgvector containers as in `docs/sources.md`). The S3 keys default to
//! MinIO's development login (`DOEWOOD_TEST_S3_ACCESS_KEY`, `DOEWOOD_TEST_S3_SECRET_KEY`).

mod common;

use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use doewood_core::MasterKey;
use doewood_ingest::{Analyzer, AnalyzerConfig, Ingest, IngestConfig, IngestParts, S3Fetch};
use doewood_model::{Backend, NodeKind, SearchQuery, SourceKey};
use doewood_s3::{ClientOptions, Discovery, DiscoveryOptions, http_client};
use doewood_store::{NewSource, S3Credentials, Store};
use rusty_s3::{Bucket, Credentials, S3Action as _};
use tokio::sync::{mpsc, watch};

const TTL: Duration = Duration::from_mins(10);

struct Env {
    endpoint: String,
    access_key: String,
    secret_key: String,
    pg: String,
}

fn env() -> Option<Env> {
    let get = |v: &str| std::env::var(v).ok().filter(|s| !s.trim().is_empty());
    let (Some(endpoint), Some(pg)) = (
        get("DOEWOOD_TEST_S3_ENDPOINT"),
        get("DOEWOOD_TEST_POSTGRES_URL"),
    ) else {
        eprintln!(
            "DOEWOOD_TEST_S3_ENDPOINT and DOEWOOD_TEST_POSTGRES_URL are not both set: skipped"
        );
        return None;
    };
    Some(Env {
        endpoint,
        access_key: get("DOEWOOD_TEST_S3_ACCESS_KEY").unwrap_or_else(|| "minioadmin".into()),
        secret_key: get("DOEWOOD_TEST_S3_SECRET_KEY").unwrap_or_else(|| "minioadmin".into()),
        pg,
    })
}

/// The test's bucket, written through raw presigned requests (the client under test only reads).
struct TestBucket {
    http: reqwest::Client,
    bucket: Bucket,
    creds: Credentials,
}

impl TestBucket {
    async fn put(&self, key: &str, body: Vec<u8>) {
        let url = self.bucket.put_object(Some(&self.creds), key).sign(TTL);
        let resp = self.http.put(url).body(body).send().await.unwrap();
        assert!(resp.status().is_success(), "put {key}: {}", resp.status());
    }

    async fn delete(&self, key: &str) {
        let url = self.bucket.delete_object(Some(&self.creds), key).sign(TTL);
        let resp = self.http.delete(url).send().await.unwrap();
        assert!(
            resp.status().is_success(),
            "delete {key}: {}",
            resp.status()
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
// One story from the put to the delete; its steps share the servers it sets up.
#[allow(clippy::too_many_lines)]
async fn s3_put_to_postgres_record() {
    let Some(env) = env() else { return };

    // A bucket of its own.
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let name = format!("doewood-ingest-{nanos}");
    let bucket = Bucket::new(
        env.endpoint.parse().unwrap(),
        rusty_s3::UrlStyle::Path,
        name.clone(),
        "us-east-1",
    )
    .unwrap();
    let creds = Credentials::new(&env.access_key, &env.secret_key);
    let http = http_client(&ClientOptions::default()).unwrap();
    let resp = http
        .put(bucket.create_bucket(&creds).sign(TTL))
        .send()
        .await
        .unwrap();
    assert!(
        resp.status().is_success(),
        "create bucket: {}",
        resp.status()
    );
    let tb = TestBucket {
        http,
        bucket,
        creds,
    };
    let p1 = common::pib("10123456");
    tb.put(
        "in/contract.pdf",
        common::pdf(
            &[
                "UGOVOR O ISPORUCI br. 114/23",
                &format!("Dobavljac: ACME Promet d.o.o. Beograd, PIB {p1}"),
                "Narucilac: Beta Gradnja a.d. Novi Sad",
            ],
            "Ugovor",
        ),
    )
    .await;
    tb.put(
        "in/prices.xlsx",
        common::xlsx("Prices", &[&["Item", "Qty"], &["Bolts", "10"]]),
    )
    .await;
    tb.put("other/ignored.txt", b"outside the prefix".to_vec())
        .await;

    // Postgres in a schema of its own.
    let (admin, conn) = tokio_postgres::connect(&env.pg, tokio_postgres::NoTls)
        .await
        .unwrap();
    tokio::spawn(conn);
    let schema = format!("dw_e2e_{}", uuid::Uuid::now_v7().simple());
    admin
        .batch_execute(&format!("CREATE SCHEMA {schema}"))
        .await
        .unwrap();
    let sep = if env.pg.contains('?') { '&' } else { '?' };
    let scoped = format!("{}{sep}options=-c%20search_path%3D{schema}", env.pg);
    let pg: Arc<dyn Backend> = Arc::new(
        doewood_pg::PgStore::connect(&doewood_core::PostgresUrl::parse(&scoped).unwrap())
            .await
            .unwrap(),
    );

    // The instance: its store, a source, discovery and ingest.
    let store = Arc::new(Store::open_in_memory(&MasterKey::from_bytes([4; 32])).unwrap());
    let mut source = NewSource::new(&env.endpoint, &name);
    source.prefix = "in/".into();
    source.scan_interval = Duration::from_secs(1);
    let id = store
        .add_source(
            &source,
            Some(&S3Credentials::new(&env.access_key, &env.secret_key, None).unwrap()),
        )
        .unwrap();
    let stored = store.source(id).unwrap().unwrap();
    let (shutdown, shutdown_rx) = watch::channel(false);
    let (events_tx, events_rx) = mpsc::channel(64);
    let discovery = Discovery::start(
        Arc::clone(&store),
        events_tx,
        shutdown_rx.clone(),
        DiscoveryOptions::default(),
    )
    .await
    .unwrap();
    let (_backend_tx, backend_rx) = watch::channel(Some(Arc::clone(&pg)));
    let mut config = IngestConfig::new(Some(2), &PathBuf::from("/nonexistent"));
    config.sweep_interval = Duration::from_millis(500);
    let ingest = Ingest::new(IngestParts {
        store: Arc::clone(&store),
        backend: backend_rx,
        fetch: Arc::new(S3Fetch::new(Arc::clone(&store), ClientOptions::default())),
        analyzer: Arc::new(
            Analyzer::new(&AnalyzerConfig {
                workers: 2,
                ocr_models: PathBuf::from("/nonexistent"),
                ..AnalyzerConfig::default()
            })
            .unwrap(),
        ),
        config,
        on_change: Arc::new(|_| {}),
        on_llm_queued: Arc::new(|_| {}),
    });
    let handle = ingest.start(events_rx, shutdown_rx);

    let key = SourceKey::new(&stored.endpoint, &stored.bucket, "in/").unwrap();
    let wait_for = |n: usize| {
        let pg = Arc::clone(&pg);
        let key = key.clone();
        async move {
            let deadline = tokio::time::Instant::now() + Duration::from_mins(2);
            loop {
                let got = pg.list_records(&key, None, 0, usize::MAX).await.unwrap();
                if got.len() == n {
                    return got;
                }
                assert!(
                    tokio::time::Instant::now() < deadline,
                    "{} records, not {n}",
                    got.len()
                );
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
    };
    let records = wait_for(2).await;
    let mut formats: Vec<String> = records
        .iter()
        .map(|r| {
            r.body_value().unwrap()["format"]
                .as_str()
                .unwrap()
                .to_owned()
        })
        .collect();
    formats.sort();
    assert_eq!(formats, ["pdf", "xlsx"]);
    let company = pg
        .alias(NodeKind::Company, &format!("id:rs_pib:{p1}"))
        .await
        .unwrap()
        .and_then(|a| a.node)
        .expect("the supplier, by its PIB");
    assert_eq!(pg.company_documents(company).await.unwrap().len(), 1);
    let hits = pg
        .search(&SearchQuery {
            text: Some("Bolts".into()),
            source: Some(key.clone()),
            ..SearchQuery::default()
        })
        .await
        .unwrap();
    assert_eq!(hits.len(), 1);
    let counts = store.object_counts(id).unwrap();
    assert_eq!((counts.done, counts.pending, counts.failed), (2, 0, 0));

    // A delete in the bucket: the next pass ends the location.
    tb.delete("in/prices.xlsx").await;
    wait_for(1).await;

    shutdown.send_replace(true);
    discovery.join().await;
    handle.join().await;
    for k in ["in/contract.pdf", "other/ignored.txt"] {
        tb.delete(k).await;
    }
    let resp = tb
        .http
        .delete(tb.bucket.delete_bucket(&tb.creds).sign(TTL))
        .send()
        .await
        .unwrap();
    assert!(
        resp.status().is_success(),
        "delete bucket: {}",
        resp.status()
    );
    admin
        .batch_execute(&format!("DROP SCHEMA {schema} CASCADE"))
        .await
        .unwrap();
}

/// Bytes no extractor reads (ingest ends them `skipped`, with their identity), from `seed`.
fn opaque(seed: u64, len: usize) -> Vec<u8> {
    let mut x = seed | 1;
    let mut out = Vec::with_capacity(len + 8);
    out.extend_from_slice(b"\x00DWBLOB\x00");
    while out.len() < len {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        out.extend_from_slice(&x.to_le_bytes());
    }
    out.truncate(len);
    out
}

/// Known copies (issue #131) against MinIO: copies of one stored object under new keys of
/// another source are located without a GetObject when that source opts in, and fetched
/// when it does not, with the same records and locations. MinIO's single-part ETag is the
/// MD5 of the content and it sends no SSE header for a plain bucket, so HEAD confirms them.
/// Prints the GetObject bytes and time of the copies' phase for both (a measurement with
/// `DOEWOOD_KNOWN_COPIES` copies of `DOEWOOD_KNOWN_COPIES_MIB` MiB; default 3 of 2 MiB).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[allow(clippy::too_many_lines)]
async fn known_copies_skip_the_get_object() {
    let Some(env) = env() else { return };
    let var = |name: &str, default: usize| {
        std::env::var(name)
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(default)
    };
    let copies = var("DOEWOOD_KNOWN_COPIES", 3);
    let size = var("DOEWOOD_KNOWN_COPIES_MIB", 2) << 20;

    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let name = format!("doewood-copies-{nanos}");
    let bucket = Bucket::new(
        env.endpoint.parse().unwrap(),
        rusty_s3::UrlStyle::Path,
        name.clone(),
        "us-east-1",
    )
    .unwrap();
    let creds = Credentials::new(&env.access_key, &env.secret_key);
    let http = http_client(&ClientOptions::default()).unwrap();
    let resp = http
        .put(bucket.create_bucket(&creds).sign(TTL))
        .send()
        .await
        .unwrap();
    assert!(
        resp.status().is_success(),
        "create bucket: {}",
        resp.status()
    );
    let tb = TestBucket {
        http,
        bucket,
        creds,
    };
    let bytes = opaque(nanos as u64, size);
    tb.put("a/original.bin", bytes.clone()).await;
    for i in 0..copies {
        tb.put(&format!("b/copy-{i:03}.bin"), bytes.clone()).await;
    }
    let (admin, conn) = tokio_postgres::connect(&env.pg, tokio_postgres::NoTls)
        .await
        .unwrap();
    tokio::spawn(conn);
    let s3_creds = S3Credentials::new(&env.access_key, &env.secret_key, None).unwrap();

    let mut ends = Vec::new();
    for skip in [false, true] {
        let schema = format!("dw_copies_{}", uuid::Uuid::now_v7().simple());
        admin
            .batch_execute(&format!("CREATE SCHEMA {schema}"))
            .await
            .unwrap();
        let sep = if env.pg.contains('?') { '&' } else { '?' };
        let scoped = format!("{}{sep}options=-c%20search_path%3D{schema}", env.pg);
        let pg: Arc<dyn Backend> = Arc::new(
            doewood_pg::PgStore::connect(&doewood_core::PostgresUrl::parse(&scoped).unwrap())
                .await
                .unwrap(),
        );
        let store = Arc::new(Store::open_in_memory(&MasterKey::from_bytes([5; 32])).unwrap());
        let add = |prefix: &str, skip_known_copies: bool| {
            let mut s = NewSource::new(&env.endpoint, &name);
            s.prefix = prefix.into();
            s.scan_interval = Duration::from_secs(3600);
            s.skip_known_copies = skip_known_copies;
            store.add_source(&s, Some(&s3_creds)).unwrap()
        };
        let a = add("a/", false);
        let (shutdown, shutdown_rx) = watch::channel(false);
        let (events_tx, events_rx) = mpsc::channel(64);
        let discovery = Discovery::start(
            Arc::clone(&store),
            events_tx,
            shutdown_rx.clone(),
            DiscoveryOptions::default(),
        )
        .await
        .unwrap();
        let (_backend_tx, backend_rx) = watch::channel(Some(Arc::clone(&pg)));
        let mut config = IngestConfig::new(Some(2), &PathBuf::from("/nonexistent"));
        config.sweep_interval = Duration::from_millis(200);
        let ingest = Ingest::new(IngestParts {
            store: Arc::clone(&store),
            backend: backend_rx,
            fetch: Arc::new(S3Fetch::new(Arc::clone(&store), ClientOptions::default())),
            analyzer: Arc::new(
                Analyzer::new(&AnalyzerConfig {
                    workers: 2,
                    ocr_models: PathBuf::from("/nonexistent"),
                    ..AnalyzerConfig::default()
                })
                .unwrap(),
            ),
            config,
            on_change: Arc::new(|_| {}),
            on_llm_queued: Arc::new(|_| {}),
        });
        let handle = ingest.start(events_rx, shutdown_rx);
        let through = |id, n: u64| {
            let store = Arc::clone(&store);
            async move {
                let deadline = tokio::time::Instant::now() + Duration::from_mins(5);
                loop {
                    let c = store.object_counts(id).unwrap();
                    if c.done + c.skipped + c.failed == n && c.pending == 0 {
                        return c;
                    }
                    assert!(tokio::time::Instant::now() < deadline, "{c:?}");
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
            }
        };
        through(a, 1).await;
        let before = ingest.stats();
        // The copies' source only now, so the original is known first.
        let b = add("b/", skip);
        let started = std::time::Instant::now();
        assert!(discovery.start_source(&store.source(b).unwrap().unwrap()));
        let counts = through(b, copies as u64).await;
        let secs = started.elapsed().as_secs_f64();
        let after = ingest.stats();
        let fetched = after.bytes - before.bytes;
        println!(
            "known copies {}: {copies} copies of {} MiB: GetObject {:.1} MiB, {secs:.2} s \
             (scan, HEAD/GET, graph), known copies {}",
            if skip { "on" } else { "off" },
            size >> 20,
            fetched as f64 / f64::from(1 << 20),
            after.known_copies,
        );
        assert_eq!(counts.failed, 0);
        if skip {
            assert_eq!(fetched, 0, "no copy fetched");
            assert_eq!(after.known_copies, copies as u64);
            assert_eq!(after.known_copy_bytes, (copies * size) as u64);
        } else {
            assert_eq!(fetched, (copies * size) as u64);
            assert_eq!(after.known_copies, 0);
        }
        // Every copy is the original's content, with a location of its own.
        let original = store.ingest_status(a, "a/original.bin").unwrap().unwrap();
        let mut statuses = Vec::new();
        for i in 0..copies {
            let st = store
                .ingest_status(b, &format!("b/copy-{i:03}.bin"))
                .unwrap()
                .unwrap();
            assert_eq!(st.blake3, original.blake3);
            statuses.push(st.status);
        }
        let hash = doewood_model::Blake3(original.blake3.unwrap());
        let doc = pg
            .node_by_key(NodeKind::Document, &hash.to_hex())
            .await
            .unwrap()
            .expect("document");
        let mut located: Vec<String> = pg
            .neighbours(
                doc.id,
                &doewood_model::NeighbourQuery {
                    direction: doewood_model::Direction::Out,
                    edge_kinds: vec![doewood_model::EdgeKind::LocatedAt],
                    ..doewood_model::NeighbourQuery::default()
                },
            )
            .await
            .unwrap()
            .into_iter()
            .map(|n| n.node.key)
            .collect();
        located.sort();
        assert_eq!(located.len(), copies + 1, "{located:?}");
        ends.push((original.status, statuses, located));

        shutdown.send_replace(true);
        discovery.join().await;
        handle.join().await;
        drop(pg);
        admin
            .batch_execute(&format!("DROP SCHEMA {schema} CASCADE"))
            .await
            .unwrap();
    }
    assert_eq!(ends[0], ends[1], "known copies end as full ingests do");

    tb.delete("a/original.bin").await;
    for i in 0..copies {
        tb.delete(&format!("b/copy-{i:03}.bin")).await;
    }
    let resp = tb
        .http
        .delete(tb.bucket.delete_bucket(&tb.creds).sign(TTL))
        .send()
        .await
        .unwrap();
    assert!(
        resp.status().is_success(),
        "delete bucket: {}",
        resp.status()
    );
}
