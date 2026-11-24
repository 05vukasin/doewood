//! Integration tests against a real S3-compatible server. They run only when
//! `DOEWOOD_TEST_S3_ENDPOINT` is set (CI's `minio` job; locally
//! `docker run --rm -d -p 59000:9000 minio/minio server /data` and
//! `DOEWOOD_TEST_S3_ENDPOINT=http://127.0.0.1:59000`). The keys default to MinIO's
//! out-of-the-box development login; override them with `DOEWOOD_TEST_S3_ACCESS_KEY` and
//! `DOEWOOD_TEST_S3_SECRET_KEY`. Each test works in a bucket of its own and deletes it.

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use doewood_core::MasterKey;
use doewood_s3::{
    ClientOptions, EventKind, ListRequest, RetryPolicy, S3Client, S3Config, S3Error, ScanOptions,
    Scanner, http_client,
};
use doewood_store::{NewSource, S3Credentials, Store, UrlStyle};
use rusty_s3::{Bucket, Credentials, S3Action as _};
use tokio::sync::{mpsc, watch};

struct Server {
    endpoint: String,
    access_key: String,
    secret_key: String,
}

fn server() -> Option<Server> {
    let endpoint = std::env::var("DOEWOOD_TEST_S3_ENDPOINT")
        .ok()
        .filter(|v| !v.is_empty())?;
    let var = |name: &str, default: &str| std::env::var(name).unwrap_or_else(|_| default.into());
    Some(Server {
        endpoint,
        access_key: var("DOEWOOD_TEST_S3_ACCESS_KEY", "minioadmin"),
        secret_key: var("DOEWOOD_TEST_S3_SECRET_KEY", "minioadmin"),
    })
}

macro_rules! require_server {
    () => {
        match server() {
            Some(s) => s,
            None => {
                eprintln!("DOEWOOD_TEST_S3_ENDPOINT is not set: skipped");
                return;
            }
        }
    };
}

/// A bucket of the test's own, written through raw presigned requests (the client under test
/// only reads).
struct TestBucket {
    http: reqwest::Client,
    bucket: Bucket,
    creds: Credentials,
}

const TTL: Duration = Duration::from_mins(10);

impl TestBucket {
    async fn create(server: &Server, tag: &str) -> Self {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let name = format!("doewood-{tag}-{nanos}");
        let bucket = Bucket::new(
            server.endpoint.parse().unwrap(),
            rusty_s3::UrlStyle::Path,
            name,
            "us-east-1",
        )
        .unwrap();
        let creds = Credentials::new(&server.access_key, &server.secret_key);
        let http = http_client(&ClientOptions::default()).unwrap();
        let url = bucket.create_bucket(&creds).sign(TTL);
        let resp = http.put(url).send().await.unwrap();
        assert!(
            resp.status().is_success(),
            "create bucket: {}",
            resp.status()
        );
        Self {
            http,
            bucket,
            creds,
        }
    }

    fn name(&self) -> String {
        self.bucket.name().to_owned()
    }

    async fn put(&self, key: &str, body: impl Into<Vec<u8>>) {
        let url = self.bucket.put_object(Some(&self.creds), key).sign(TTL);
        let resp = self.http.put(url).body(body.into()).send().await.unwrap();
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

    /// Deletes every object, then the bucket.
    async fn destroy(self, client: &S3Client) {
        let mut token = None;
        loop {
            let page = client
                .list_objects(&ListRequest {
                    continuation_token: token.as_deref(),
                    max_keys: 1000,
                    ..ListRequest::default()
                })
                .await
                .unwrap();
            for o in &page.objects {
                self.delete(&o.key).await;
            }
            match page.next_continuation_token {
                Some(t) => token = Some(t),
                None => break,
            }
        }
        let url = self.bucket.delete_bucket(&self.creds).sign(TTL);
        let resp = self.http.delete(url).send().await.unwrap();
        assert!(
            resp.status().is_success(),
            "delete bucket: {}",
            resp.status()
        );
    }
}

fn config(server: &Server, bucket: &str) -> S3Config {
    S3Config {
        endpoint: server.endpoint.clone(),
        bucket: bucket.to_owned(),
        region: "us-east-1".into(),
        url_style: UrlStyle::Path,
        credentials: Some(
            S3Credentials::new(&server.access_key, &server.secret_key, None).unwrap(),
        ),
    }
}

#[tokio::test]
async fn list_head_and_get_with_a_cap() {
    let server = require_server!();
    let tb = TestBucket::create(&server, "client").await;
    let client = S3Client::new(&config(&server, &tb.name()), ClientOptions::default()).unwrap();
    for i in 0..25 {
        tb.put(&format!("docs/k{i:03}.txt"), format!("body {i}"))
            .await;
    }
    tb.put("docs/a key with spaces+plus é.txt", "x").await;
    tb.put("other/x.txt", "x").await;

    // Pages of 10 under the prefix, followed by continuation tokens, in key order.
    let mut keys = Vec::new();
    let mut token = None;
    let mut pages = 0;
    loop {
        let page = client
            .list_objects(&ListRequest {
                prefix: "docs/",
                continuation_token: token.as_deref(),
                max_keys: 10,
                ..ListRequest::default()
            })
            .await
            .unwrap();
        pages += 1;
        keys.extend(page.objects.into_iter().map(|o| o.key));
        match page.next_continuation_token {
            Some(t) => token = Some(t),
            None => break,
        }
    }
    assert_eq!(pages, 3);
    assert_eq!(keys.len(), 26);
    assert_eq!(keys[0], "docs/a key with spaces+plus é.txt");
    let mut sorted = keys.clone();
    sorted.sort();
    assert_eq!(keys, sorted);

    // StartAfter resumes a listing.
    let page = client
        .list_objects(&ListRequest {
            prefix: "docs/",
            start_after: Some("docs/k022.txt"),
            max_keys: 1000,
            ..ListRequest::default()
        })
        .await
        .unwrap();
    let rest: Vec<_> = page.objects.iter().map(|o| o.key.as_str()).collect();
    assert_eq!(rest, ["docs/k023.txt", "docs/k024.txt"]);

    let head = client.head_object("docs/k007.txt").await.unwrap().unwrap();
    assert_eq!(head.size, 6);
    assert!(head.last_modified.is_some());
    let listed = client
        .list_objects(&ListRequest {
            start_after: Some("docs/k006.txt"),
            max_keys: 1,
            ..ListRequest::default()
        })
        .await
        .unwrap();
    assert_eq!(listed.objects[0].key, "docs/k007.txt");
    // Unquoted, and the same in HEAD and in the listing.
    assert!(head.etag.as_deref().is_some_and(|e| !e.contains('"')));
    assert_eq!(head.etag, listed.objects[0].etag);
    assert!(client.head_object("docs/missing").await.unwrap().is_none());

    let body = client
        .get_object("docs/a key with spaces+plus é.txt", 1024)
        .await
        .unwrap();
    assert_eq!(body.meta().size, 1);
    assert_eq!(body.bytes().await.unwrap(), b"x");
    assert!(matches!(
        client.get_object("docs/k010.txt", 3).await,
        Err(S3Error::TooLarge { limit: 3 })
    ));
    let missing = client.get_object("docs/missing", 10).await.unwrap_err();
    assert_eq!(missing.status(), Some(404));

    tb.destroy(&client).await;
}

#[tokio::test]
async fn bad_credentials_fail_fast_without_leaking() {
    let server = require_server!();
    let tb = TestBucket::create(&server, "creds").await;
    let mut bad = config(&server, &tb.name());
    bad.credentials = Some(S3Credentials::new("not-a-user", "wrong-secret-123", None).unwrap());
    let options = ClientOptions {
        retry: RetryPolicy {
            max_attempts: 3,
            ..RetryPolicy::default()
        },
        ..ClientOptions::default()
    };
    let client = S3Client::new(&bad, options).unwrap();
    let started = std::time::Instant::now();
    let err = client
        .list_objects(&ListRequest::default())
        .await
        .unwrap_err();
    assert_eq!(err.status(), Some(403), "{err}");
    // 403 is not retried.
    assert!(started.elapsed() < Duration::from_secs(5));
    let text = err.to_string();
    assert!(
        !text.contains("wrong-secret") && !text.contains("X-Amz"),
        "{text}"
    );

    // An endpoint nobody listens on is a transport error, after the retries.
    let mut down = config(&server, &tb.name());
    down.endpoint = "http://127.0.0.1:9".into();
    let fast = ClientOptions {
        connect_timeout: Duration::from_secs(1),
        retry: RetryPolicy {
            max_attempts: 2,
            base_delay: Duration::from_millis(10),
            max_delay: Duration::from_millis(20),
        },
        ..ClientOptions::default()
    };
    let err = S3Client::new(&down, fast)
        .unwrap()
        .list_objects(&ListRequest::default())
        .await
        .unwrap_err();
    assert!(matches!(err, S3Error::Transport { .. }), "{err}");
    assert!(!err.to_string().contains("X-Amz"), "{err}");

    let good = S3Client::new(&config(&server, &tb.name()), ClientOptions::default()).unwrap();
    tb.destroy(&good).await;
}

#[tokio::test]
async fn scanner_finds_new_changed_and_deleted_objects() {
    let server = require_server!();
    let tb = TestBucket::create(&server, "scan").await;
    for i in 0..30 {
        tb.put(&format!("in/{i:03}.pdf"), vec![b'a'; i]).await;
    }
    tb.put("out/ignored.pdf", "x").await;

    let store = Arc::new(Store::open_in_memory(&MasterKey::from_bytes([1; 32])).unwrap());
    let mut new = NewSource::new(&server.endpoint, tb.name());
    new.prefix = "in/".into();
    let creds = S3Credentials::new(&server.access_key, &server.secret_key, None).unwrap();
    let id = store.add_source(&new, Some(&creds)).unwrap();
    let source = store.source(id).unwrap().unwrap();
    // The client is built the way discovery builds it: credentials from the store.
    let client = S3Client::new(
        &S3Config::for_source(&source, store.source_credentials(id).unwrap()),
        ClientOptions::default(),
    )
    .unwrap();
    let options = ScanOptions {
        page_size: 7,
        chunk_size: 5,
        error_retry: None,
    };

    let run = async |store: &Arc<Store>| {
        let (tx, mut rx) = mpsc::channel(1000);
        let scanner = Scanner::new(Arc::clone(store), &source, tx, options);
        let (_stop_tx, mut stop) = watch::channel(false);
        let stats = scanner.pass(&client, &mut stop).await.unwrap();
        drop(scanner);
        let mut events = Vec::new();
        while let Some(e) = rx.recv().await {
            events.push((e.key, e.kind));
        }
        (stats, events)
    };

    let (stats, events) = run(&store).await;
    assert_eq!((stats.pass, stats.listed, stats.new), (1, 30, 30));
    assert!(
        events
            .iter()
            .all(|(k, e)| k.starts_with("in/") && matches!(e, EventKind::New(_)))
    );

    let (stats, events) = run(&store).await;
    assert_eq!(stats.pass, 2);
    assert!(events.is_empty(), "{events:?}");

    tb.put("in/005.pdf", "rewritten").await;
    tb.delete("in/000.pdf").await;
    tb.delete("in/029.pdf").await;
    tb.put("in/014b.pdf", "new").await;
    let (stats, mut events) = run(&store).await;
    assert_eq!((stats.new, stats.changed, stats.deleted), (1, 1, 2));
    events.sort_by(|a, b| a.0.cmp(&b.0));
    let names: Vec<_> = events.iter().map(|(k, e)| (k.as_str(), e.name())).collect();
    assert_eq!(
        names,
        [
            ("in/000.pdf", "deleted"),
            ("in/005.pdf", "changed"),
            ("in/014b.pdf", "new"),
            ("in/029.pdf", "deleted"),
        ]
    );
    let EventKind::Changed(v) = &events[1].1 else {
        unreachable!()
    };
    assert_eq!(v.size, 9);
    assert_eq!(store.live_object_count(id).unwrap(), 29);

    tb.destroy(&client).await;
}
