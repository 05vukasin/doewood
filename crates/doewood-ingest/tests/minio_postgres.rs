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
                let got = pg.list_records(&key, None, 0).await.unwrap();
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
        .map(|r| r.body["format"].as_str().unwrap().to_owned())
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
