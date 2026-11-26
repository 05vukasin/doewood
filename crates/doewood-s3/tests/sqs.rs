//! The SQS inlet against a mock SQS on 127.0.0.1 that speaks the AWS JSON protocol
//! (`POST /`, `Content-Type: application/x-amz-json-1.0`, `X-Amz-Target: AmazonSQS.<Action>`,
//! the shapes of the SQS API reference). Nothing here reaches AWS. Queue URLs, buckets, keys
//! and credentials are invented.

use std::collections::{HashMap, VecDeque};
use std::io::Write as _;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::{Duration, Instant};

use doewood_core::MasterKey;
use doewood_s3::inlet::sqs::{SqsClient, SqsConfig, SqsInlet, SqsOptions};
use doewood_s3::inlet::{EventInlet as _, InletStats, InletStatsSnapshot};
use doewood_s3::{
    ClientOptions, Discovery, DiscoveryOptions, EventOrigin, ObjectEvent, RetryPolicy, http_client,
};
use doewood_store::{CredentialsChange, NewSource, S3Credentials, SqsInletInput, Store};
use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt as _, AsyncReadExt as _, AsyncWriteExt as _, BufReader};
use tokio::net::TcpListener;
use tokio::sync::{Notify, mpsc, watch};

const BUCKET: &str = "doewood-test-bucket";
const ACCESS_KEY: &str = "AKIDTESTONLY0000";
const SECRET_KEY: &str = "test-sqs-secret-do-not-use";
const SESSION_TOKEN: &str = "test-sqs-session-token-do-not-use";

// ---------------------------------------------------------------------------------------------
// The mock SQS.

struct Message {
    id: String,
    body: String,
    receipt: Option<String>,
    receives: u32,
    deleted: bool,
}

#[derive(Default)]
struct MockState {
    queue: Vec<Message>,
    /// Answers for the next receives: status and JSON body.
    receive_errors: VecDeque<(u16, Value)>,
    /// Every request: when, the target, the body.
    requests: Vec<(Instant, String, Value)>,
    /// What did not match the protocol.
    violations: Vec<String>,
    /// Every header value seen, to check that no secret travels in the clear.
    header_values: Vec<String>,
    /// The signing this mock expects: (access key id, region), or unsigned.
    expect_signed: Option<(String, String)>,
    queue_url: String,
}

#[derive(Clone)]
struct MockSqs {
    state: Arc<Mutex<MockState>>,
    arrived: Arc<Notify>,
}

impl MockSqs {
    async fn start(expect_signed: Option<(&str, &str)>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let state = Arc::new(Mutex::new(MockState {
            expect_signed: expect_signed.map(|(k, r)| (k.to_owned(), r.to_owned())),
            queue_url: format!("http://{addr}/123456789012/doewood-test-events"),
            ..MockState::default()
        }));
        let mock = Self {
            state,
            arrived: Arc::new(Notify::new()),
        };
        let server = mock.clone();
        tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    return;
                };
                let server = server.clone();
                tokio::spawn(async move { server.serve(stream).await });
            }
        });
        mock
    }

    fn state(&self) -> MutexGuard<'_, MockState> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn queue_url(&self) -> String {
        self.state().queue_url.clone()
    }

    fn push(&self, id: &str, body: impl Into<String>) {
        self.state().queue.push(Message {
            id: id.to_owned(),
            body: body.into(),
            receipt: None,
            receives: 0,
            deleted: false,
        });
        self.arrived.notify_waiters();
    }

    fn undeleted(&self) -> Vec<String> {
        self.state()
            .queue
            .iter()
            .filter(|m| !m.deleted)
            .map(|m| m.id.clone())
            .collect()
    }

    fn count(&self, target: &str) -> usize {
        self.state()
            .requests
            .iter()
            .filter(|(_, t, _)| t == target)
            .count()
    }

    async fn serve(&self, stream: tokio::net::TcpStream) {
        let (read, mut write) = stream.into_split();
        let mut read = BufReader::new(read);
        loop {
            let mut line = String::new();
            if read.read_line(&mut line).await.unwrap_or(0) == 0 {
                return;
            }
            let request_line = line.trim_end().to_owned();
            let mut headers = HashMap::new();
            loop {
                let mut h = String::new();
                if read.read_line(&mut h).await.unwrap_or(0) == 0 {
                    return;
                }
                let h = h.trim_end();
                if h.is_empty() {
                    break;
                }
                if let Some((k, v)) = h.split_once(':') {
                    headers.insert(k.trim().to_ascii_lowercase(), v.trim().to_owned());
                }
            }
            let len: usize = headers
                .get("content-length")
                .and_then(|l| l.parse().ok())
                .unwrap_or(0);
            let mut body = vec![0; len];
            if read.read_exact(&mut body).await.is_err() {
                return;
            }
            let (status, answer) = self.answer(&request_line, &headers, &body).await;
            let answer = answer.to_string();
            let mut out = Vec::new();
            let _ = write!(
                out,
                "HTTP/1.1 {status} X\r\nContent-Type: application/x-amz-json-1.0\r\n\
                 x-amzn-RequestId: test\r\nContent-Length: {}\r\n\r\n{answer}",
                answer.len()
            );
            if write.write_all(&out).await.is_err() {
                return;
            }
        }
    }

    /// Checks the request against the JSON protocol and answers it.
    async fn answer(
        &self,
        request_line: &str,
        headers: &HashMap<String, String>,
        body: &[u8],
    ) -> (u16, Value) {
        let target = headers.get("x-amz-target").cloned().unwrap_or_default();
        let parsed: Value = serde_json::from_slice(body).unwrap_or(Value::Null);
        {
            let mut s = self.state();
            s.requests
                .push((Instant::now(), target.clone(), parsed.clone()));
            s.header_values.extend(headers.values().cloned());
            let mut bad = Vec::new();
            if request_line != "POST / HTTP/1.1" {
                bad.push(format!("request line {request_line:?}"));
            }
            if headers.get("content-type").map(String::as_str) != Some("application/x-amz-json-1.0")
            {
                bad.push("content type".to_owned());
            }
            if parsed["QueueUrl"] != Value::String(s.queue_url.clone()) {
                bad.push("QueueUrl".to_owned());
            }
            match &s.expect_signed {
                Some((key, region)) => {
                    let auth = headers.get("authorization").cloned().unwrap_or_default();
                    let date = headers.get("x-amz-date").cloned().unwrap_or_default();
                    let scope = format!(
                        "AWS4-HMAC-SHA256 Credential={key}/{}/{region}/sqs/aws4_request, ",
                        date.get(..8).unwrap_or("")
                    );
                    if !auth.starts_with(&scope) || !auth.contains(", Signature=") {
                        bad.push(format!("authorization {auth:?}"));
                    }
                    for signed in ["content-type", "host", "x-amz-date", "x-amz-target"] {
                        if !auth.contains(signed) {
                            bad.push(format!("{signed} not signed"));
                        }
                    }
                }
                None => {
                    if headers.contains_key("authorization") {
                        bad.push("signed without credentials".to_owned());
                    }
                }
            }
            if !bad.is_empty() {
                s.violations.extend(bad);
                return (
                    400,
                    json!({"__type": "com.amazonaws.sqs#InvalidParameterValue", "message": "mock"}),
                );
            }
        }
        match target.as_str() {
            "AmazonSQS.ReceiveMessage" => self.receive(&parsed).await,
            "AmazonSQS.DeleteMessageBatch" => self.delete(&parsed),
            other => {
                self.state().violations.push(format!("target {other:?}"));
                (
                    400,
                    json!({"__type": "com.amazonaws.sqs#InvalidAction", "message": "mock"}),
                )
            }
        }
    }

    async fn receive(&self, req: &Value) -> (u16, Value) {
        let max = req["MaxNumberOfMessages"].as_u64().unwrap_or(1);
        let wait = req["WaitTimeSeconds"].as_u64().unwrap_or(0);
        if !(1..=10).contains(&max) || wait > 20 || !req["VisibilityTimeout"].is_u64() {
            self.state()
                .violations
                .push(format!("receive parameters {req}"));
        }
        if let Some((status, body)) = self.state().receive_errors.pop_front() {
            return (status, body);
        }
        let deadline = tokio::time::Instant::now() + Duration::from_secs(wait);
        loop {
            let arrived = self.arrived.notified();
            {
                let mut s = self.state();
                // Every message not deleted is visible again: as if each visibility timeout had
                // already passed.
                let out: Vec<Value> = s
                    .queue
                    .iter_mut()
                    .filter(|m| !m.deleted)
                    .take(usize::try_from(max).unwrap())
                    .map(|m| {
                        m.receives += 1;
                        let receipt = format!("rh-{}-{}", m.id, m.receives);
                        m.receipt = Some(receipt.clone());
                        json!({
                            "MessageId": m.id,
                            "ReceiptHandle": receipt,
                            "MD5OfBody": "00000000000000000000000000000000",
                            "Body": m.body,
                            "Attributes": {"ApproximateReceiveCount": m.receives.to_string()},
                        })
                    })
                    .collect();
                if !out.is_empty() {
                    return (200, json!({"Messages": out}));
                }
            }
            if tokio::time::timeout_at(deadline, arrived).await.is_err() {
                return (200, json!({}));
            }
        }
    }

    fn delete(&self, req: &Value) -> (u16, Value) {
        let mut s = self.state();
        let entries = req["Entries"].as_array().cloned().unwrap_or_default();
        if entries.is_empty() || entries.len() > 10 {
            s.violations
                .push(format!("{} delete entries", entries.len()));
        }
        let (mut ok, mut failed) = (Vec::new(), Vec::new());
        for e in entries {
            let id = e["Id"].as_str().unwrap_or_default().to_owned();
            let handle = e["ReceiptHandle"].as_str().unwrap_or_default();
            match s
                .queue
                .iter_mut()
                .find(|m| m.receipt.as_deref() == Some(handle))
            {
                Some(m) => {
                    m.deleted = true;
                    ok.push(json!({"Id": id}));
                }
                None => failed
                    .push(json!({"Id": id, "Code": "ReceiptHandleIsInvalid", "SenderFault": true})),
            }
        }
        (200, json!({"Successful": ok, "Failed": failed}))
    }
}

// ---------------------------------------------------------------------------------------------
// Helpers.

fn s3_record(bucket: &str, key: &str, event: &str) -> String {
    json!({"Records": [{
        "eventVersion": "2.1", "eventSource": "aws:s3", "awsRegion": "eu-central-1",
        "eventTime": "2026-01-02T03:04:05.000Z", "eventName": event,
        "s3": {"s3SchemaVersion": "1.0", "configurationId": "doewood-test",
               "bucket": {"name": bucket, "arn": format!("arn:aws:s3:::{bucket}")},
               "object": {"key": key, "size": 42, "eTag": "0123abcd", "sequencer": "0A1B"}}
    }]})
    .to_string()
}

fn sns(message: &str) -> String {
    json!({
        "Type": "Notification",
        "MessageId": "00000000-0000-4000-8000-000000000001",
        "TopicArn": "arn:aws:sns:eu-central-1:123456789012:doewood-test-topic",
        "Subject": "Amazon S3 Notification",
        "Message": message,
        "Timestamp": "2026-01-02T03:04:05.000Z",
        "SignatureVersion": "1",
        "Signature": "TEST",
    })
    .to_string()
}

const TEST_EVENT: &str = r#"{"Service":"Amazon S3","Event":"s3:TestEvent","Time":"2026-01-02T03:04:05.000Z","Bucket":"doewood-test-bucket","RequestId":"TEST","HostId":"TEST"}"#;

fn store() -> Arc<Store> {
    Arc::new(Store::open_in_memory(&MasterKey::from_bytes([5; 32])).unwrap())
}

fn add_source(store: &Store) -> doewood_core::SourceId {
    let mut new = NewSource::new("http://127.0.0.1:1", BUCKET);
    new.prefix = "in/".into();
    store.add_source(&new, None).unwrap()
}

fn creds(token: bool) -> S3Credentials {
    S3Credentials::new(
        ACCESS_KEY,
        SECRET_KEY,
        token.then(|| SESSION_TOKEN.to_owned()),
    )
    .unwrap()
}

fn fast() -> SqsOptions {
    SqsOptions {
        backoff_base: Duration::from_millis(20),
        backoff_max: Duration::from_millis(100),
        delete_retry: RetryPolicy {
            max_attempts: 2,
            base_delay: Duration::from_millis(5),
            max_delay: Duration::from_millis(10),
        },
        ..SqsOptions::default()
    }
}

struct Running {
    task: tokio::task::JoinHandle<()>,
    stop: watch::Sender<bool>,
    stats: Arc<InletStats>,
}

impl Running {
    async fn stop(self) -> InletStatsSnapshot {
        self.stop.send(true).unwrap();
        tokio::time::timeout(Duration::from_secs(3), self.task)
            .await
            .expect("the inlet stops at once on shutdown, even in a long poll")
            .unwrap();
        self.stats.snapshot()
    }
}

fn run(
    store: &Arc<Store>,
    source: doewood_core::SourceId,
    mock: &MockSqs,
    credentials: Option<S3Credentials>,
    region: &str,
    options: SqsOptions,
    events: mpsc::Sender<ObjectEvent>,
) -> Running {
    let config = SqsConfig {
        queue_url: mock.queue_url(),
        region: region.to_owned(),
        credentials,
    };
    let client = SqsClient::new(&config, http_client(&ClientOptions::default()).unwrap()).unwrap();
    let stats = Arc::new(InletStats::default());
    let inlet = SqsInlet::new(
        Arc::clone(store),
        source,
        client,
        options,
        Arc::clone(&stats),
    );
    let (stop, shutdown) = watch::channel(false);
    let task = tokio::spawn(async move { inlet.run(events, shutdown).await.unwrap() });
    Running { task, stop, stats }
}

async fn until(what: &str, mut f: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while !f() {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

// ---------------------------------------------------------------------------------------------
// Tests.

#[tokio::test]
async fn messages_are_handed_off_then_deleted() {
    logs();
    let store = store();
    let source = add_source(&store);
    let mock = MockSqs::start(Some((ACCESS_KEY, "eu-central-1"))).await;
    mock.push("raw", s3_record(BUCKET, "in/a+b.pdf", "ObjectCreated:Put"));
    mock.push(
        "sns",
        sns(&s3_record(
            BUCKET,
            "in/c.xlsx",
            "ObjectCreated:CompleteMultipartUpload",
        )),
    );
    mock.push("test", TEST_EVENT);
    mock.push("test-sns", sns(TEST_EVENT));
    mock.push(
        "foreign",
        s3_record("doewood-other-bucket", "in/x.pdf", "ObjectCreated:Put"),
    );
    mock.push(
        "outside",
        s3_record(BUCKET, "out/y.pdf", "ObjectCreated:Put"),
    );
    mock.push("malformed", "this is not JSON");
    mock.push(
        "eventbridge",
        r#"{"version":"0","detail-type":"Object Created"}"#,
    );
    let (tx, mut rx) = mpsc::channel(16);
    let running = run(
        &store,
        source,
        &mock,
        Some(creds(false)),
        "eu-central-1",
        fast(),
        tx,
    );

    let mut got = Vec::new();
    for _ in 0..2 {
        let e = tokio::time::timeout(Duration::from_secs(5), rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(e.origin, EventOrigin::Sqs);
        assert_eq!(e.source, source);
        got.push((e.key, e.kind.name()));
    }
    got.sort();
    assert_eq!(
        got,
        [("in/a b.pdf".into(), "new"), ("in/c.xlsx".into(), "new")]
    );
    assert!(store.seen_object(source, "in/a b.pdf").unwrap().is_some());
    until("every message deleted", || mock.undeleted().is_empty()).await;

    // The same notification again is a no-op, and is deleted too.
    mock.push(
        "again",
        s3_record(BUCKET, "in/a+b.pdf", "ObjectCreated:Put"),
    );
    until("the repeat deleted", || mock.undeleted().is_empty()).await;
    // A removal of a seen key is a deletion.
    mock.push(
        "removed",
        sns(&s3_record(BUCKET, "in/c.xlsx", "ObjectRemoved:Delete")),
    );
    let e = tokio::time::timeout(Duration::from_secs(5), rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!((e.key.as_str(), e.kind.name()), ("in/c.xlsx", "deleted"));
    until("the removal deleted", || mock.undeleted().is_empty()).await;

    // Shutdown interrupts the long poll (the mock holds an empty receive for 20 s).
    let stats = running.stop().await;
    assert_eq!(
        stats,
        InletStatsSnapshot {
            received: 10,
            deleted: 10,
            emitted: 3,
            ignored: 3,
            test_events: 2,
            malformed: 2,
            errors: 0,
            last_error: None,
        }
    );
    let s = mock.state();
    assert!(s.violations.is_empty(), "{:?}", s.violations);
    let (_, _, first) = s
        .requests
        .iter()
        .find(|(_, t, _)| t == "AmazonSQS.ReceiveMessage")
        .unwrap();
    assert_eq!(first["WaitTimeSeconds"], 20);
    assert_eq!(first["MaxNumberOfMessages"], 10);
    assert_eq!(first["VisibilityTimeout"], 300);
    // The first batch went in one DeleteMessageBatch.
    let (_, _, delete) = s
        .requests
        .iter()
        .find(|(_, t, _)| t == "AmazonSQS.DeleteMessageBatch")
        .unwrap();
    assert_eq!(delete["Entries"].as_array().unwrap().len(), 8);
}

#[tokio::test]
async fn throttling_and_errors_back_off() {
    logs();
    let store = store();
    let source = add_source(&store);
    let mock = MockSqs::start(None).await;
    {
        let mut s = mock.state();
        s.receive_errors.push_back((
            400,
            json!({"__type": "com.amazonaws.sqs#RequestThrottled", "message": "Rate exceeded"}),
        ));
        s.receive_errors.push_back((
            503,
            json!({"__type": "com.amazonaws.sqs#ServiceUnavailable", "message": "try later"}),
        ));
        s.receive_errors.push_back((
            400,
            json!({"__type": "com.amazonaws.sqs#QueueDoesNotExist", "message": "The specified queue does not exist."}),
        ));
    }
    mock.push("m", s3_record(BUCKET, "in/a.pdf", "ObjectCreated:Put"));
    let (tx, mut rx) = mpsc::channel(4);
    let options = SqsOptions {
        backoff_base: Duration::from_millis(40),
        backoff_max: Duration::from_millis(400),
        ..fast()
    };
    let running = run(&store, source, &mock, None, "us-east-1", options, tx);
    let e = tokio::time::timeout(Duration::from_secs(5), rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(e.key, "in/a.pdf");
    until("deleted", || mock.undeleted().is_empty()).await;
    let stats = running.stop().await;
    assert_eq!((stats.errors, stats.emitted, stats.deleted), (3, 1, 1));
    let (_, last) = stats.last_error.unwrap();
    assert!(last.contains("QueueDoesNotExist"), "{last}");
    let s = mock.state();
    assert!(s.violations.is_empty(), "{:?}", s.violations);
    let times: Vec<Instant> = s
        .requests
        .iter()
        .filter(|(_, t, _)| t == "AmazonSQS.ReceiveMessage")
        .map(|(at, _, _)| *at)
        .collect();
    assert!(times.len() >= 4, "{}", times.len());
    // Throttling waits at least half the doubling bound: 20 ms, then 40 ms; an error that
    // retrying soon does not fix waits at least half the longest backoff, 200 ms.
    assert!(times[1] - times[0] >= Duration::from_millis(20));
    assert!(times[2] - times[1] >= Duration::from_millis(40));
    assert!(times[3] - times[2] >= Duration::from_millis(200));
}

#[tokio::test]
async fn a_failed_hand_off_leaves_the_message_for_redelivery() {
    logs();
    let store = store();
    let source = add_source(&store);
    let mock = MockSqs::start(None).await;
    mock.push("m", s3_record(BUCKET, "in/a.pdf", "ObjectCreated:Put"));

    // The event channel is closed: nothing is handed off, nothing deleted, the inlet stops.
    let (tx, rx) = mpsc::channel(4);
    drop(rx);
    let running = run(&store, source, &mock, None, "us-east-1", fast(), tx);
    tokio::time::timeout(Duration::from_secs(5), running.task)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(mock.undeleted(), ["m"]);
    assert_eq!(mock.count("AmazonSQS.DeleteMessageBatch"), 0);
    // The row is committed before the event is sent (issue #125): it is pending, and ingest's
    // sweep feeds it although its event was not sent.
    let seen = store.seen_object(source, "in/a.pdf").unwrap().unwrap();
    assert_eq!(
        store
            .ingest_status(source, "in/a.pdf")
            .unwrap()
            .unwrap()
            .status,
        "pending"
    );

    // SQS delivers it again: already seen, so nothing is sent again, and it is deleted.
    let (tx, mut rx) = mpsc::channel(4);
    let running = run(&store, source, &mock, None, "us-east-1", fast(), tx);
    until("deleted", || mock.undeleted().is_empty()).await;
    assert_eq!(mock.state().queue[0].receives, 2);
    running.stop().await;
    assert!(rx.try_recv().is_err());
    assert_eq!(store.seen_object(source, "in/a.pdf").unwrap(), Some(seen));

    // A source removed meanwhile: the message stays, and the inlet stops.
    mock.push("n", s3_record(BUCKET, "in/b.pdf", "ObjectCreated:Put"));
    assert!(store.delete_source(source).unwrap());
    let (tx, _rx) = mpsc::channel(4);
    let running = run(&store, source, &mock, None, "us-east-1", fast(), tx);
    tokio::time::timeout(Duration::from_secs(5), running.task)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(mock.undeleted(), ["n"]);
    assert!(mock.state().violations.is_empty());
}

#[tokio::test]
async fn credentials_stay_out_of_debug_errors_and_the_wire() {
    logs();
    let mock = MockSqs::start(Some((ACCESS_KEY, "eu-west-1"))).await;
    let store = store();
    let source = add_source(&store);
    let config = SqsConfig {
        queue_url: mock.queue_url(),
        region: "eu-west-1".into(),
        credentials: Some(creds(true)),
    };
    let client = SqsClient::new(&config, http_client(&ClientOptions::default()).unwrap()).unwrap();
    let inlet = SqsInlet::new(
        Arc::clone(&store),
        source,
        client.clone(),
        SqsOptions::default(),
        Arc::default(),
    );
    for debug in [
        format!("{config:?}"),
        format!("{client:?}"),
        format!("{inlet:?}"),
    ] {
        assert!(
            !debug.contains(SECRET_KEY) && !debug.contains(SESSION_TOKEN),
            "{debug}"
        );
    }
    // An error answer: the error text carries the code and message, nothing of the request.
    mock.state().receive_errors.push_back((
        403,
        json!({"__type": "com.amazonaws.sqs#InvalidClientTokenId", "message": "The security token included in the request is invalid."}),
    ));
    let err = client.receive(&SqsOptions::default()).await.unwrap_err();
    let text = err.to_string();
    assert!(text.contains("InvalidClientTokenId"), "{text}");
    assert!(
        !text.contains(SECRET_KEY) && !text.contains(SESSION_TOKEN) && !text.contains(ACCESS_KEY)
    );
    // The secret key never travels; the session token only in its own header.
    mock.push("m", TEST_EVENT);
    let got = client.receive(&SqsOptions::default()).await.unwrap();
    assert_eq!(got.len(), 1);
    let s = mock.state();
    assert!(s.violations.is_empty(), "{:?}", s.violations);
    assert!(!s.header_values.iter().any(|v| v.contains(SECRET_KEY)));
    assert_eq!(
        s.header_values
            .iter()
            .filter(|v| v.contains(SESSION_TOKEN))
            .count(),
        2,
        "one x-amz-security-token header per request"
    );
}

#[tokio::test]
async fn discovery_runs_the_inlet_with_the_scanner() {
    logs();
    let store = store();
    let source = add_source(&store);
    let mock = MockSqs::start(Some((ACCESS_KEY, "eu-central-1"))).await;
    let input = SqsInletInput {
        queue_url: mock.queue_url(),
        region: Some("eu-central-1".into()),
        enabled: true,
    };
    store
        .set_sqs_inlet(source, &input, &CredentialsChange::Set(creds(false)))
        .unwrap();
    mock.push("m", s3_record(BUCKET, "in/a.pdf", "ObjectCreated:Put"));
    let (tx, mut rx) = mpsc::channel(16);
    let (stop, shutdown) = watch::channel(false);
    let options = DiscoveryOptions {
        sqs: fast(),
        ..DiscoveryOptions::default()
    };
    let discovery = Discovery::start(Arc::clone(&store), tx, shutdown, options)
        .await
        .unwrap();
    assert!(discovery.is_running(source) && discovery.is_inlet_running(source));
    let e = loop {
        // The scanner's events (it cannot reach its endpoint) never come; the inlet's do.
        let e = tokio::time::timeout(Duration::from_secs(5), rx.recv())
            .await
            .unwrap()
            .unwrap();
        if e.origin == EventOrigin::Sqs {
            break e;
        }
    };
    assert_eq!(e.key, "in/a.pdf");
    until("deleted", || mock.undeleted().is_empty()).await;
    assert_eq!(discovery.inlet_stats(source).unwrap().emitted, 1);

    // Turned off: the inlet stops, the scanner keeps running.
    let off = SqsInletInput {
        enabled: false,
        ..input
    };
    store
        .set_sqs_inlet(source, &off, &CredentialsChange::Keep)
        .unwrap();
    let src = store.source(source).unwrap().unwrap();
    assert!(!discovery.restart_inlet(&src));
    assert!(discovery.is_running(source) && !discovery.is_inlet_running(source));
    // Pausing the source stops both.
    store
        .set_sqs_inlet(
            source,
            &SqsInletInput {
                enabled: true,
                ..off
            },
            &CredentialsChange::Keep,
        )
        .unwrap();
    let src = store.source(source).unwrap().unwrap();
    assert!(discovery.restart_inlet(&src));
    discovery.stop_source(source);
    assert!(!discovery.is_running(source) && !discovery.is_inlet_running(source));
    stop.send(true).unwrap();
    tokio::time::timeout(Duration::from_secs(5), discovery.join())
        .await
        .unwrap();
    assert!(mock.state().violations.is_empty());
}

#[derive(Clone, Default)]
struct Logs(Arc<Mutex<Vec<u8>>>);

/// Captures every log line of this test binary, at every level. A global subscriber: a scoped
/// one misses the events of callsites that tests on other threads registered first.
fn logs() -> &'static Logs {
    static LOGS: std::sync::OnceLock<Logs> = std::sync::OnceLock::new();
    LOGS.get_or_init(|| {
        let logs = Logs::default();
        tracing_subscriber::fmt()
            .with_writer(logs.clone())
            .with_max_level(tracing::Level::TRACE)
            .init();
        logs
    })
}

impl std::io::Write for Logs {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .extend_from_slice(buf);
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for Logs {
    type Writer = Self;
    fn make_writer(&'a self) -> Self {
        self.clone()
    }
}

#[tokio::test]
async fn nothing_secret_is_logged() {
    let logs = logs();
    let store = store();
    let source = add_source(&store);
    let mock = MockSqs::start(Some((ACCESS_KEY, "eu-west-1"))).await;
    mock.state().receive_errors.push_back((
        403,
        json!({"__type": "com.amazonaws.sqs#AccessDenied", "message": "Access to the resource is denied."}),
    ));
    mock.push("bad", "not an event");
    mock.push("ok", s3_record(BUCKET, "in/a.pdf", "ObjectCreated:Put"));
    let (tx, mut rx) = mpsc::channel(4);
    let running = run(
        &store,
        source,
        &mock,
        Some(creds(true)),
        "eu-west-1",
        fast(),
        tx,
    );
    tokio::time::timeout(Duration::from_secs(5), rx.recv())
        .await
        .unwrap()
        .unwrap();
    until("deleted", || mock.undeleted().is_empty()).await;
    let stats = running.stop().await;
    assert_eq!((stats.errors, stats.malformed, stats.emitted), (1, 1, 1));
    // Every test's logs so far, this one's included: none may hold a secret.
    let text = String::from_utf8(logs.0.lock().unwrap().clone()).unwrap();
    assert!(text.contains("SQS inlet polling"), "{text}");
    assert!(text.contains("AccessDenied"), "{text}");
    assert!(text.contains("not an S3 event notification"), "{text}");
    for secret in [SECRET_KEY, SESSION_TOKEN, "rh-ok-", "Signature="] {
        assert!(!text.contains(secret), "{secret} logged:\n{text}");
    }
}
