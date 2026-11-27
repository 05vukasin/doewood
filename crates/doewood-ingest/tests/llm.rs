//! The LLM queue and on-demand summaries against a local mock HTTP server (a `std`
//! `TcpListener` on 127.0.0.1, an ephemeral port): the policy matrix, the concurrency cap, the
//! daily budget and its reset, soft failures, and `Summarize` storing nothing. No request
//! leaves the machine, and every document is built in code.

#![allow(clippy::too_many_lines)] // each test is one scenario, in order

mod common;

use std::io::{BufRead as _, BufReader, Read as _, Write as _};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use common::{MemFetch, prose};
use doewood_core::{MasterKey, SourceId};
use doewood_ingest::facts::DocFacts;
use doewood_ingest::{
    Analyzer, AnalyzerConfig, Clock, Ingest, IngestConfig, IngestParts, QueueConfig, Summaries,
    SummariesParts, SummarizeError, SummarizeInput,
};
use doewood_model::{Backend, Blake3};
use doewood_s3::{EventKind, EventOrigin, ObjectEvent, ObjectVersion};
use doewood_store::{
    AiProviderInput, ApiKey, IngestSettingsPatch, KeyChange, LlmJobStatus, LlmMode, LlmPolicy,
    NewSource, ObjectChange, SeenObject, SourcePatch, Standalone, Store, utc_day,
};
use quail_summary::ApiConfig;
use quail_summary::api::RetryPolicy;
use serde_json::Value as Json;
use tokio::sync::{mpsc, watch};

const ENDPOINT: &str = "http://objects.test:9000";
/// Tokens each mock reply reports: 100 prompt plus 20 completion.
const TOKENS_PER_CALL: u64 = 120;
/// A grounded model output: every name and number is in the documents below.
const GOOD: &str = r#"{"summary": "A maintenance agreement between Northwind Traders and Contoso Ltd for 24 months at 4,200.00 EUR per year.", "subject": "printer maintenance", "title_en": "Maintenance Agreement"}"#;

// ---------------------------------------------------------------------------------------------
// The mock provider.

#[derive(Clone)]
struct Reply {
    status: u16,
    body: String,
    delay: Duration,
}

fn good() -> Reply {
    Reply {
        status: 200,
        body: serde_json::json!({
            "choices": [{"message": {"role": "assistant", "content": GOOD}, "finish_reason": "stop"}],
            "usage": {"prompt_tokens": 100, "completion_tokens": 20}
        })
        .to_string(),
        delay: Duration::ZERO,
    }
}

#[derive(Clone, Debug)]
struct Seen {
    line: String,
    authorization: Option<String>,
    body: String,
}

/// An OpenAI-compatible server on 127.0.0.1 that answers every request with `reply`, one
/// thread per connection, counting the requests in flight.
struct Mock {
    addr: SocketAddr,
    reply: Arc<Mutex<Reply>>,
    seen: Arc<Mutex<Vec<Seen>>>,
    in_flight: Arc<AtomicUsize>,
    max_in_flight: Arc<AtomicUsize>,
    stop: Arc<AtomicBool>,
}

impl Mock {
    fn start(reply: Reply) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let mock = Self {
            addr,
            reply: Arc::new(Mutex::new(reply)),
            seen: Arc::new(Mutex::new(Vec::new())),
            in_flight: Arc::new(AtomicUsize::new(0)),
            max_in_flight: Arc::new(AtomicUsize::new(0)),
            stop: Arc::new(AtomicBool::new(false)),
        };
        let (reply, seen, in_flight, max, stop) = (
            Arc::clone(&mock.reply),
            Arc::clone(&mock.seen),
            Arc::clone(&mock.in_flight),
            Arc::clone(&mock.max_in_flight),
            Arc::clone(&mock.stop),
        );
        std::thread::spawn(move || {
            for stream in listener.incoming() {
                if stop.load(Ordering::SeqCst) {
                    return;
                }
                let Ok(stream) = stream else { continue };
                let (reply, seen, in_flight, max) = (
                    Arc::clone(&reply),
                    Arc::clone(&seen),
                    Arc::clone(&in_flight),
                    Arc::clone(&max),
                );
                std::thread::spawn(move || {
                    let now = in_flight.fetch_add(1, Ordering::SeqCst) + 1;
                    max.fetch_max(now, Ordering::SeqCst);
                    let r = reply.lock().unwrap().clone();
                    handle(stream, &r, &seen);
                    in_flight.fetch_sub(1, Ordering::SeqCst);
                });
            }
        });
        mock
    }

    fn url(&self) -> String {
        format!("http://{}/v1", self.addr)
    }

    fn set(&self, reply: Reply) {
        *self.reply.lock().unwrap() = reply;
    }

    fn requests(&self) -> Vec<Seen> {
        self.seen.lock().unwrap().clone()
    }

    fn count(&self) -> usize {
        self.seen.lock().unwrap().len()
    }
}

impl Drop for Mock {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        let _ = TcpStream::connect(self.addr);
    }
}

fn handle(stream: TcpStream, r: &Reply, seen: &Mutex<Vec<Seen>>) {
    let mut reader = BufReader::new(stream.try_clone().unwrap());
    let mut line = String::new();
    if reader.read_line(&mut line).is_err() || line.is_empty() {
        return;
    }
    let mut len = 0usize;
    let mut authorization = None;
    loop {
        let mut h = String::new();
        if reader.read_line(&mut h).is_err() {
            return;
        }
        let h = h.trim_end();
        if h.is_empty() {
            break;
        }
        if let Some((k, v)) = h.split_once(':') {
            if k.eq_ignore_ascii_case("content-length") {
                len = v.trim().parse().unwrap_or(0);
            } else if k.eq_ignore_ascii_case("authorization") {
                authorization = Some(v.trim().to_owned());
            }
        }
    }
    let mut body = vec![0u8; len];
    if reader.read_exact(&mut body).is_err() {
        return;
    }
    seen.lock().unwrap().push(Seen {
        line: line.trim_end().to_owned(),
        authorization,
        body: String::from_utf8_lossy(&body).into_owned(),
    });
    std::thread::sleep(r.delay);
    let mut out = stream;
    let _ = write!(
        out,
        "HTTP/1.1 {} X\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
        r.status,
        r.body.len(),
        r.body
    );
    let _ = out.flush();
}

// ---------------------------------------------------------------------------------------------
// Documents, built in code.

/// A maintenance agreement (quail reads it as a contract), unique per `n`.
fn agreement(n: u64) -> Vec<u8> {
    format!(
        "MAINTENANCE AGREEMENT\n\nThis agreement is made between Northwind Traders and Contoso \
         Ltd. Northwind Traders maintains the office printers of Contoso Ltd for 24 months from \
         1 March 2024 for a fee of 4,200.00 EUR per year.\n\n{}\n",
        prose(n, 160)
    )
    .into_bytes()
}

/// Plain notes, of another kind (or none), unique per `n`.
fn notes(n: u64) -> Vec<u8> {
    format!(
        "Notes from the warehouse walk\n\n{}\n\n{}\n",
        prose(n + 1000, 120),
        prose(n + 2000, 120)
    )
    .into_bytes()
}

// ---------------------------------------------------------------------------------------------
// The harness: the store, the SQLite backend, ingest and the queue.

struct TestClock(AtomicI64);

impl Clock for TestClock {
    fn unix_now(&self) -> i64 {
        self.0.load(Ordering::SeqCst)
    }
}

/// 2026-01-01 12:00 UTC.
const NOON: i64 = 1_767_268_800;

fn analyzer() -> Arc<Analyzer> {
    Arc::new(
        Analyzer::new(&AnalyzerConfig {
            workers: 2,
            ocr_models: PathBuf::from("/nonexistent/doewood-ocr-models"),
            ..AnalyzerConfig::default()
        })
        .unwrap(),
    )
}

fn queue_config(call_timeout: Duration) -> QueueConfig {
    QueueConfig {
        sweep_interval: Duration::from_millis(50),
        api: ApiConfig {
            call_timeout,
            timeout: call_timeout,
            retry: RetryPolicy {
                max_retries: 0,
                ..RetryPolicy::default()
            },
            ..ApiConfig::default()
        },
        ..QueueConfig::default()
    }
}

struct Harness {
    store: Arc<Store>,
    backend: Arc<dyn Backend>,
    fetch: Arc<MemFetch>,
    events: mpsc::Sender<ObjectEvent>,
    summaries: Summaries,
    clock: Arc<TestClock>,
    shutdown: watch::Sender<bool>,
    ingest: Option<doewood_ingest::IngestHandle>,
    queue: Option<tokio::task::JoinHandle<()>>,
}

impl Harness {
    /// With a provider (`openai_compatible` at `url`, key `test-key`) when `url` is set.
    fn start(url: Option<&str>, call_timeout: Duration) -> Self {
        let store = Arc::new(Store::open_in_memory(&MasterKey::from_bytes([9; 32])).unwrap());
        if let Some(url) = url {
            store
                .set_ai_provider(
                    &AiProviderInput {
                        name: "mock".into(),
                        preset: "openai_compatible".into(),
                        model: "test-model".into(),
                        base_url: Some(url.to_owned()),
                    },
                    &KeyChange::Set(ApiKey::new("test-key").unwrap()),
                    true,
                )
                .unwrap();
        }
        let backend: Arc<dyn Backend> = Arc::new(Standalone::new(Arc::clone(&store)));
        let fetch = Arc::new(MemFetch::default());
        let (_tx, backend_rx) = watch::channel(Some(Arc::clone(&backend)));
        let analyzer = analyzer();
        let clock = Arc::new(TestClock(AtomicI64::new(NOON)));
        let summaries = Summaries::new(SummariesParts {
            store: Arc::clone(&store),
            backend: backend_rx.clone(),
            fetch: Arc::clone(&fetch) as Arc<dyn doewood_ingest::Fetch>,
            analyzer: Arc::clone(&analyzer),
            config: queue_config(call_timeout),
            clock: Arc::clone(&clock) as Arc<dyn Clock>,
            on_change: Arc::new(|_| {}),
        });
        let mut config = IngestConfig::new(Some(2), &PathBuf::from("/nonexistent"));
        config.sweep_interval = Duration::from_millis(200);
        let queue = summaries.clone();
        let ingest = Ingest::new(IngestParts {
            store: Arc::clone(&store),
            backend: backend_rx,
            fetch: Arc::clone(&fetch) as Arc<dyn doewood_ingest::Fetch>,
            analyzer,
            config,
            on_change: Arc::new(|_| {}),
            on_llm_queued: Arc::new(move |_| queue.notify()),
        });
        let (events, events_rx) = mpsc::channel(64);
        let (shutdown, shutdown_rx) = watch::channel(false);
        let ingest_handle = ingest.start(events_rx, shutdown_rx.clone());
        let queue = summaries.start(shutdown_rx);
        Self {
            store,
            backend,
            fetch,
            events,
            summaries,
            clock,
            shutdown,
            ingest: Some(ingest_handle),
            queue: Some(queue),
        }
    }

    fn source(
        &self,
        bucket: &str,
        mode: LlmMode,
        concurrency: u32,
        budget: Option<u64>,
    ) -> SourceId {
        let mut new = NewSource::new(ENDPOINT, bucket);
        new.llm_policy = LlmPolicy {
            mode,
            concurrency,
            daily_token_budget: budget,
        };
        self.store.add_source(&new, None).unwrap()
    }

    /// Puts objects in the source's bucket and sends their events, as discovery does.
    async fn put(&self, source: SourceId, objects: &[(&str, Vec<u8>)]) {
        let mut changes = Vec::new();
        for (key, bytes) in objects {
            self.fetch.put(key, bytes.clone());
            changes.push(ObjectChange::Upsert(SeenObject {
                key: (*key).to_owned(),
                etag: Some(format!("{:x}", bytes.len())),
                size: bytes.len() as u64,
                last_modified: Some(1),
            }));
        }
        self.store.record_objects(source, &changes, None).unwrap();
        for (key, bytes) in objects {
            self.events
                .send(ObjectEvent {
                    source,
                    key: (*key).to_owned(),
                    kind: EventKind::New(ObjectVersion {
                        etag: Some(format!("{:x}", bytes.len())),
                        size: bytes.len() as u64,
                        last_modified: Some(1),
                    }),
                    origin: EventOrigin::Scan,
                })
                .await
                .unwrap();
        }
    }

    /// Waits until ingest has nothing pending in these sources.
    async fn ingested(&self, sources: &[SourceId]) {
        let deadline = tokio::time::Instant::now() + Duration::from_mins(2);
        loop {
            let pending: u64 = sources
                .iter()
                .map(|s| self.store.object_counts(*s).unwrap().pending)
                .sum();
            if pending == 0 {
                return;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "ingest did not settle"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    /// Waits until `f` holds.
    async fn until(&self, what: &str, f: impl Fn(&Self) -> bool) {
        let deadline = tokio::time::Instant::now() + Duration::from_mins(1);
        while !f(self) {
            assert!(tokio::time::Instant::now() < deadline, "timed out: {what}");
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    fn blake3(&self, source: SourceId, key: &str) -> [u8; 32] {
        self.store
            .ingest_status(source, key)
            .unwrap()
            .unwrap()
            .blake3
            .expect("ingested")
    }

    async fn record(&self, source: SourceId, key: &str) -> Json {
        self.backend
            .get_record(&Blake3(self.blake3(source, key)))
            .await
            .unwrap()
            .expect("record stored")
            .body_value()
            .unwrap()
    }

    async fn stop(mut self) {
        let _ = self.shutdown.send(true);
        if let Some(h) = self.ingest.take() {
            h.join().await;
        }
        if let Some(q) = self.queue.take() {
            q.await.unwrap();
        }
    }
}

fn summary_of(body: &Json) -> Option<&Json> {
    body["facts"]
        .as_array()
        .unwrap()
        .iter()
        .find(|f| f["key"] == "summary.llm")
        .map(|f| &f["value"]["v"])
}

// ---------------------------------------------------------------------------------------------
// Tests.

/// `off` and `on_demand` send nothing; `all` summarizes every document, `kinds` only its
/// kinds. The key comes from the store; the summary goes into the record.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_policy_decides_what_is_summarized() {
    let mock = Mock::start(good());
    let h = Harness::start(Some(&mock.url()), Duration::from_secs(10));
    // The kinds quail gives the two documents.
    let kind_of = |bytes: Vec<u8>, name: &str| {
        DocFacts::from_record(&analyzer().analyze(bytes, name).body).kind
    };
    let contract_kind = kind_of(agreement(0), "a.txt").expect("the agreement has a kind");
    assert_ne!(
        kind_of(notes(0), "n.txt").as_deref(),
        Some(contract_kind.as_str())
    );

    let off = h.source("off", LlmMode::Off, 2, None);
    let on_demand = h.source("on-demand", LlmMode::OnDemand, 2, None);
    let all = h.source("all", LlmMode::All, 2, None);
    let kinds = h.source("kinds", LlmMode::Kinds(vec![contract_kind]), 2, None);
    let sources = [off, on_demand, all, kinds];
    for (i, s) in sources.iter().enumerate() {
        let n = i as u64 * 10;
        h.put(
            *s,
            &[
                (&format!("{i}/agreement.txt"), agreement(n + 1)),
                (&format!("{i}/notes.txt"), notes(n + 2)),
            ],
        )
        .await;
    }
    h.ingested(&sources).await;
    h.until("the queue drained", |h| {
        sources
            .iter()
            .all(|s| h.store.llm_counts(*s).unwrap().pending == 0)
            && h.summaries.in_flight() == 0
    })
    .await;
    // Give a stray call time to show up.
    tokio::time::sleep(Duration::from_millis(300)).await;

    let counts = |s| h.store.llm_counts(s).unwrap();
    assert_eq!(
        counts(off),
        doewood_store::LlmCounts::default(),
        "off queues nothing"
    );
    assert_eq!(counts(on_demand), doewood_store::LlmCounts::default());
    assert_eq!(counts(all).done, 2);
    assert_eq!((counts(kinds).done, counts(kinds).failed), (1, 0));
    assert_eq!(mock.count(), 3, "two for all, one for kinds");
    assert_eq!(h.summaries.calls(), 3);

    for (s, key, summarized) in [
        (off, "0/agreement.txt", false),
        (off, "0/notes.txt", false),
        (on_demand, "1/agreement.txt", false),
        (all, "2/agreement.txt", true),
        (all, "2/notes.txt", true),
        (kinds, "3/agreement.txt", true),
        (kinds, "3/notes.txt", false),
    ] {
        let body = h.record(s, key).await;
        assert_eq!(summary_of(&body).is_some(), summarized, "{key}");
    }
    let body = h.record(all, "2/agreement.txt").await;
    let s = summary_of(&body).unwrap();
    assert_eq!(s["method"]["v"], "llm", "{s}");
    assert!(
        s["text"]["v"]
            .as_str()
            .unwrap()
            .contains("Northwind Traders"),
        "{s}"
    );
    assert_eq!(s["model"]["v"], "openai_compatible:test-model");
    assert_eq!(s["prompt_tokens"]["v"], 100);
    assert!(
        body["steps"]
            .as_array()
            .unwrap()
            .iter()
            .any(|st| st["name"] == "summary.llm")
    );
    // Today's spend is counted per source.
    let day = utc_day(NOON);
    assert_eq!(
        h.store.llm_usage(all, day).unwrap().tokens,
        2 * TOKENS_PER_CALL
    );
    assert_eq!(h.store.llm_usage(off, day).unwrap().calls, 0);

    // What went out: the chat endpoint, the key from the store, the digest of the document.
    for r in mock.requests() {
        assert_eq!(r.line, "POST /v1/chat/completions HTTP/1.1");
        assert_eq!(r.authorization.as_deref(), Some("Bearer test-key"));
        let body: Json = serde_json::from_str(&r.body).unwrap();
        assert_eq!(body["model"], "test-model");
        let user = body["messages"][1]["content"].as_str().unwrap();
        assert!(user.contains("FACTS:") && user.contains("TEXT"), "{user}");
    }

    // Switching a policy to off skips its waiting documents.
    h.store
        .update_source(
            all,
            &SourcePatch {
                llm_policy: Some(LlmPolicy::default()),
                ..SourcePatch::default()
            },
        )
        .unwrap();
    h.store.queue_llm_job(all, &[7; 32], None).unwrap();
    h.summaries.notify();
    h.until("the job was skipped", |h| {
        h.store.llm_job(all, &[7; 32]).unwrap().unwrap().status == LlmJobStatus::Skipped
    })
    .await;
    assert_eq!(mock.count(), 3);
    h.stop().await;
}

/// No more than `concurrency` summaries of a source are in flight at once.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_concurrency_cap_holds() {
    let mock = Mock::start(Reply {
        delay: Duration::from_millis(300),
        ..good()
    });
    let h = Harness::start(Some(&mock.url()), Duration::from_secs(10));
    let s = h.source("capped", LlmMode::All, 2, None);
    let docs: Vec<(String, Vec<u8>)> = (0..6)
        .map(|i| (format!("doc-{i}.txt"), agreement(100 + i)))
        .collect();
    let refs: Vec<(&str, Vec<u8>)> = docs.iter().map(|(k, b)| (k.as_str(), b.clone())).collect();
    h.put(s, &refs).await;
    h.ingested(&[s]).await;
    h.until("all summarized", |h| {
        h.store.llm_counts(s).unwrap().done == 6
    })
    .await;
    assert_eq!(mock.count(), 6);
    let max = mock.max_in_flight.load(Ordering::SeqCst);
    assert_eq!(max, 2, "at most the cap, and the cap is used");
    h.stop().await;
}

/// No summary starts once the day's tokens reach the budget; the next UTC day goes on.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_daily_budget_stops_calls_and_resets() {
    let mock = Mock::start(good());
    let h = Harness::start(Some(&mock.url()), Duration::from_secs(10));
    // 120 tokens a call: calls start at 0, 120 and 240 spent, and none at 360.
    let s = h.source("budget", LlmMode::All, 1, Some(250));
    let docs: Vec<(String, Vec<u8>)> = (0..5)
        .map(|i| (format!("doc-{i}.txt"), agreement(200 + i)))
        .collect();
    let refs: Vec<(&str, Vec<u8>)> = docs.iter().map(|(k, b)| (k.as_str(), b.clone())).collect();
    h.put(s, &refs).await;
    h.ingested(&[s]).await;
    h.until("three summarized", |h| {
        h.store.llm_counts(s).unwrap().done == 3
    })
    .await;
    // Several passes later, nothing else went out.
    tokio::time::sleep(Duration::from_millis(500)).await;
    let c = h.store.llm_counts(s).unwrap();
    assert_eq!((c.done, c.pending), (3, 2));
    assert_eq!(mock.count(), 3);
    let day = utc_day(NOON);
    assert_eq!(
        h.store.llm_usage(s, day).unwrap().tokens,
        3 * TOKENS_PER_CALL
    );

    // Midnight UTC: a new budget.
    h.clock.0.store(NOON + 12 * 3600, Ordering::SeqCst);
    assert_eq!(utc_day(NOON + 12 * 3600), day + 1);
    h.summaries.notify();
    h.until("the rest summarized", |h| {
        h.store.llm_counts(s).unwrap().done == 5
    })
    .await;
    assert_eq!(mock.count(), 5);
    assert_eq!(
        h.store.llm_usage(s, day + 1).unwrap().tokens,
        2 * TOKENS_PER_CALL
    );
    h.stop().await;
}

/// A failing or slow provider never fails or blocks ingest: the record is stored without a
/// summary and the error is reported on the job and the source's counts.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn provider_failures_are_soft() {
    let mock = Mock::start(Reply {
        status: 500,
        body: r#"{"error": {"message": "upstream exploded"}}"#.into(),
        delay: Duration::ZERO,
    });
    let h = Harness::start(Some(&mock.url()), Duration::from_secs(1));
    let s = h.source("failing", LlmMode::All, 2, None);
    h.put(s, &[("a.txt", agreement(300))]).await;
    h.ingested(&[s]).await;
    assert_eq!(
        h.store.ingest_status(s, "a.txt").unwrap().unwrap().status,
        "done"
    );
    h.until("the summary failed", |h| {
        h.store.llm_counts(s).unwrap().failed == 1
    })
    .await;
    let body = h.record(s, "a.txt").await;
    assert!(
        summary_of(&body).is_none(),
        "the record stays without a summary"
    );
    let job = h.store.llm_job(s, &h.blake3(s, "a.txt")).unwrap().unwrap();
    assert_eq!(job.attempts, 1);
    let error = job.last_error.unwrap();
    assert!(
        error.contains("500") && error.contains("upstream exploded"),
        "{error}"
    );
    assert!(!error.contains("test-key"), "{error}");
    assert_eq!(
        h.store.llm_counts(s).unwrap().last_error.as_deref(),
        Some(error.as_str())
    );

    // A provider slower than the call timeout: ingest is done long before the call ends.
    mock.set(Reply {
        delay: Duration::from_secs(4),
        ..good()
    });
    h.put(s, &[("b.txt", agreement(301))]).await;
    h.ingested(&[s]).await;
    assert_eq!(
        h.store.ingest_status(s, "b.txt").unwrap().unwrap().status,
        "done"
    );
    assert!(summary_of(&h.record(s, "b.txt").await).is_none());
    h.until("the slow summary failed", |h| {
        h.store.llm_counts(s).unwrap().failed == 2
    })
    .await;
    let job = h.store.llm_job(s, &h.blake3(s, "b.txt")).unwrap().unwrap();
    assert!(
        job.last_error.as_deref().unwrap().contains("time"),
        "{:?}",
        job.last_error
    );
    assert!(summary_of(&h.record(s, "b.txt").await).is_none());
    h.stop().await;
}

/// Without a provider, jobs wait (nothing is sent) and go once one is configured.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn without_a_provider_jobs_wait() {
    let mock = Mock::start(good());
    let h = Harness::start(None, Duration::from_secs(10));
    let s = h.source("waiting", LlmMode::All, 2, None);
    h.put(s, &[("a.txt", agreement(400))]).await;
    h.ingested(&[s]).await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(h.store.llm_counts(s).unwrap().pending, 1);
    assert_eq!(h.summaries.calls(), 0);
    h.store
        .set_ai_provider(
            &AiProviderInput {
                name: "mock".into(),
                preset: "openai_compatible".into(),
                model: "test-model".into(),
                base_url: Some(mock.url()),
            },
            &KeyChange::Set(ApiKey::new("test-key").unwrap()),
            true,
        )
        .unwrap();
    h.summaries.notify();
    h.until("summarized", |h| h.store.llm_counts(s).unwrap().done == 1)
        .await;
    assert_eq!(mock.count(), 1);
    h.stop().await;
}

/// Issue #170: after the default provider is removed there is no default: queued jobs wait
/// (nothing is sent), `Summarize` says no provider is configured, and the next provider set
/// becomes the default and lets the jobs go.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_removed_default_provider_leaves_jobs_waiting() {
    let mock = Mock::start(good());
    let h = Harness::start(Some(&mock.url()), Duration::from_secs(10));
    let gone = h.store.remove_ai_provider("mock").unwrap().unwrap();
    assert!(gone.is_default);
    h.summaries.notify();
    let s = h.source("removed", LlmMode::All, 2, None);
    h.put(s, &[("a.txt", agreement(410))]).await;
    h.ingested(&[s]).await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(h.store.llm_counts(s).unwrap().pending, 1);
    assert_eq!(h.summaries.calls(), 0);
    let content = || SummarizeInput::Content {
        bytes: agreement(411),
        name: "a.txt".into(),
    };
    let err = h.summaries.summarize(content(), None).await.unwrap_err();
    assert!(matches!(err, SummarizeError::NoProvider(_)), "{err:?}");
    let err = h
        .summaries
        .summarize(content(), Some("mock"))
        .await
        .unwrap_err();
    assert!(matches!(err, SummarizeError::NotFound(_)), "{err:?}");
    assert_eq!(mock.count(), 0);

    // Set again without make_default: the only provider becomes the default.
    let back = h
        .store
        .set_ai_provider(
            &AiProviderInput {
                name: "mock".into(),
                preset: "openai_compatible".into(),
                model: "test-model".into(),
                base_url: Some(mock.url()),
            },
            &KeyChange::Set(ApiKey::new("test-key").unwrap()),
            false,
        )
        .unwrap();
    assert!(back.is_default);
    h.summaries.notify();
    h.until("summarized", |h| h.store.llm_counts(s).unwrap().done == 1)
        .await;
    assert_eq!(mock.count(), 1);
    h.stop().await;
}

/// `Summarize` skips the queue and stores nothing; it needs a provider, and a source whose
/// policy is not `off`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn summarize_stores_nothing() {
    let mock = Mock::start(good());

    // No provider: a clear error, and no request.
    let h = Harness::start(None, Duration::from_secs(10));
    let err = h
        .summaries
        .summarize(
            SummarizeInput::Content {
                bytes: agreement(500),
                name: "a.txt".into(),
            },
            None,
        )
        .await
        .unwrap_err();
    assert!(
        matches!(&err, SummarizeError::NoProvider(m) if m.contains("SetAiProvider")),
        "{err:?}"
    );
    let err = h
        .summaries
        .summarize(
            SummarizeInput::Content {
                bytes: agreement(500),
                name: "a.txt".into(),
            },
            Some("nope"),
        )
        .await
        .unwrap_err();
    assert!(matches!(err, SummarizeError::NotFound(_)), "{err:?}");
    assert_eq!(mock.count(), 0);
    h.stop().await;

    let h = Harness::start(Some(&mock.url()), Duration::from_secs(10));
    let bytes = agreement(501);
    let blake3 = Blake3(*blake3::hash(&bytes).as_bytes());
    let r = h
        .summaries
        .summarize(
            SummarizeInput::Content {
                bytes: bytes.clone(),
                name: "a.txt".into(),
            },
            None,
        )
        .await
        .unwrap();
    assert!(!r.extractive, "{r:?}");
    assert!(r.digest.contains("Northwind Traders"), "{r:?}");
    assert_eq!(
        (r.provider.as_str(), r.model.as_str()),
        ("mock", "openai_compatible:test-model")
    );
    assert_eq!(r.blake3, blake3.to_hex());
    assert!(
        r.checked > 0 && r.supported == r.checked && r.unsupported.is_empty(),
        "{r:?}"
    );
    assert_eq!(mock.count(), 1);

    // An object of a source: refused while its policy is off, then read and summarized.
    let s = h.source("objects", LlmMode::Off, 2, None);
    h.fetch.put("in/a.txt", bytes.clone());
    let location = || SummarizeInput::Location {
        source: s,
        key: "in/a.txt".into(),
    };
    let err = h.summaries.summarize(location(), None).await.unwrap_err();
    assert!(
        matches!(&err, SummarizeError::FailedPrecondition(m) if m.contains("off")),
        "{err:?}"
    );
    assert_eq!(mock.count(), 1);
    h.store
        .update_source(
            s,
            &SourcePatch {
                llm_policy: Some(LlmPolicy {
                    mode: LlmMode::OnDemand,
                    ..LlmPolicy::default()
                }),
                ..SourcePatch::default()
            },
        )
        .unwrap();
    let r = h
        .summaries
        .summarize(location(), Some("mock"))
        .await
        .unwrap();
    assert!(!r.extractive);
    assert_eq!(mock.count(), 2);

    // A failing provider: the extractive summary, and why.
    mock.set(Reply {
        status: 500,
        body: "{}".into(),
        delay: Duration::ZERO,
    });
    let r = h.summaries.summarize(location(), None).await.unwrap();
    assert!(r.extractive && r.provider.is_empty(), "{r:?}");
    assert!(!r.digest.is_empty());
    assert!(r.warnings.iter().any(|w| w.contains("500")), "{r:?}");

    // Nothing was stored: no record, no job, no spend.
    assert!(h.backend.get_record(&blake3).await.unwrap().is_none());
    assert_eq!(
        h.store.llm_counts(s).unwrap(),
        doewood_store::LlmCounts::default()
    );
    assert_eq!(
        h.store.llm_usage(s, utc_day(NOON)).unwrap(),
        doewood_store::LlmUsage::default()
    );
    assert!(h.store.pending_llm_jobs(10).unwrap().is_empty());
    h.stop().await;
}

/// Issue #167: `Summarize` by BLAKE3 summarizes the stored record's digest as it is, with no
/// fetch and no analysis, and stores nothing; NOT_FOUND for an unknown hash, and refused while
/// no source with a policy other than `off` holds the object.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn summarize_by_blake3_reads_the_stored_record() {
    let mock = Mock::start(good());
    let h = Harness::start(Some(&mock.url()), Duration::from_secs(10));
    let s = h.source("stored", LlmMode::OnDemand, 2, None);
    h.put(s, &[("in/a.txt", agreement(700))]).await;
    h.ingested(&[s]).await;
    let blake3 = Blake3(h.blake3(s, "in/a.txt"));
    let before = h.record(s, "in/a.txt").await;
    // The bytes are gone from the bucket: only the stored record can serve the call.
    let reads = h.fetch.reads_of("in/a.txt");
    h.fetch.objects.lock().unwrap().remove("in/a.txt");
    let stored = || SummarizeInput::Stored { blake3 };

    let r = h.summaries.summarize(stored(), None).await.unwrap();
    assert!(!r.extractive, "{r:?}");
    assert!(r.digest.contains("Northwind Traders"), "{r:?}");
    assert_eq!(r.blake3, blake3.to_hex());
    assert_eq!(r.provider, "mock");
    assert!(r.checked > 0 && r.supported == r.checked, "{r:?}");
    assert_eq!(mock.count(), 1);
    assert_eq!(h.fetch.reads_of("in/a.txt"), reads);
    // The model read the stored digest.
    let sent = &mock.requests()[0].body;
    assert!(sent.contains("Northwind Traders"), "{sent}");
    assert_eq!(h.summaries.summarize_in_flight(), 0);

    // A failing provider: the record's extractive summary, and why.
    mock.set(Reply {
        status: 500,
        body: "{}".into(),
        delay: Duration::ZERO,
    });
    let r = h.summaries.summarize(stored(), None).await.unwrap();
    assert!(
        r.extractive && r.provider.is_empty() && !r.digest.is_empty(),
        "{r:?}"
    );
    assert!(r.warnings.iter().any(|w| w.contains("500")), "{r:?}");
    mock.set(good());

    // Nothing was stored: the record is unchanged, and no job or spend.
    assert_eq!(h.record(s, "in/a.txt").await, before);
    assert!(summary_of(&before).is_none());
    assert_eq!(
        h.store.llm_counts(s).unwrap(),
        doewood_store::LlmCounts::default()
    );
    assert_eq!(
        h.store.llm_usage(s, utc_day(NOON)).unwrap(),
        doewood_store::LlmUsage::default()
    );

    // An unknown hash.
    let err = h
        .summaries
        .summarize(
            SummarizeInput::Stored {
                blake3: Blake3([0xab; 32]),
            },
            None,
        )
        .await
        .unwrap_err();
    assert!(
        matches!(&err, SummarizeError::NotFound(m) if m.contains(&"ab".repeat(32))),
        "{err:?}"
    );

    // The only source holding it has its policy off: refused, nothing sent.
    h.store
        .update_source(
            s,
            &SourcePatch {
                llm_policy: Some(LlmPolicy::default()),
                ..SourcePatch::default()
            },
        )
        .unwrap();
    let calls = mock.count();
    let err = h.summaries.summarize(stored(), None).await.unwrap_err();
    assert!(
        matches!(&err, SummarizeError::FailedPrecondition(m) if m.contains("off")),
        "{err:?}"
    );
    assert_eq!(mock.count(), calls);
    assert_eq!(h.summaries.summarize_in_flight(), 0);
    h.stop().await;
}

/// The `Summarize` cap: calls beyond it are refused at once with `RESOURCE_EXHAUSTED`, the
/// setting applies to the next call without a restart, and a refused, failed or cancelled call
/// leaves no slot taken.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn summarize_cap_refuses_extra_calls_and_frees_slots() {
    let mock = Mock::start(good());
    let h = Harness::start(Some(&mock.url()), Duration::from_secs(10));
    let s = h.source("objects", LlmMode::OnDemand, 2, None);
    h.fetch.put("in/held.txt", agreement(600));
    let gate = h.fetch.hold("in/held.txt");
    let location = |key: &str| SummarizeInput::Location {
        source: s,
        key: key.to_owned(),
    };
    let spawn = |key: &str| {
        let (summaries, input) = (h.summaries.clone(), location(key));
        tokio::spawn(async move { summaries.summarize(input, None).await })
    };
    let busy = |err: &SummarizeError| matches!(err, SummarizeError::ResourceExhausted(m) if m.contains("retry later") && m.contains("summarize_max_concurrent"));
    let set_cap = |n: u32| {
        h.store
            .set_ingest_settings(&IngestSettingsPatch {
                summarize_max_concurrent: Some(n),
                ..IngestSettingsPatch::default()
            })
            .unwrap();
    };

    // The default cap is 2: two calls held while they read the object.
    let held = [spawn("in/held.txt"), spawn("in/held.txt")];
    h.until("two calls hold a slot", |h| {
        h.summaries.summarize_in_flight() == 2
    })
    .await;
    // A third is refused at once, with content, a location or a BLAKE3, and takes no slot.
    let err = h
        .summaries
        .summarize(
            SummarizeInput::Content {
                bytes: agreement(601),
                name: "a.txt".into(),
            },
            None,
        )
        .await
        .unwrap_err();
    assert!(busy(&err), "{err:?}");
    let err = h
        .summaries
        .summarize(location("in/held.txt"), None)
        .await
        .unwrap_err();
    assert!(busy(&err), "{err:?}");
    // By BLAKE3 too (issue #167): reading a stored record takes a slot as well.
    let err = h
        .summaries
        .summarize(
            SummarizeInput::Stored {
                blake3: Blake3([1; 32]),
            },
            None,
        )
        .await
        .unwrap_err();
    assert!(busy(&err), "{err:?}");
    assert_eq!(h.summaries.summarize_rejected(), 3);
    assert_eq!(h.summaries.summarize_in_flight(), 2);
    assert_eq!(mock.count(), 0);

    // Raising the cap applies to the next call, without a restart.
    set_cap(3);
    let third = spawn("in/held.txt");
    h.until("a third call holds a slot", |h| {
        h.summaries.summarize_in_flight() == 3
    })
    .await;
    // Lowering it keeps the calls in flight and refuses new ones.
    set_cap(1);
    let err = h
        .summaries
        .summarize(location("in/held.txt"), None)
        .await
        .unwrap_err();
    assert!(busy(&err), "{err:?}");
    assert_eq!(h.summaries.summarize_in_flight(), 3);

    // A cancelled call (the client went away) frees its slot.
    third.abort();
    assert!(third.await.unwrap_err().is_cancelled());
    h.until("the cancelled call freed its slot", |h| {
        h.summaries.summarize_in_flight() == 2
    })
    .await;

    // Released, the held calls finish with the model's summary and free their slots.
    gate.add_permits(2);
    for j in held {
        let r = j.await.unwrap().unwrap();
        assert!(!r.extractive, "{r:?}");
    }
    assert_eq!(h.summaries.summarize_in_flight(), 0);
    assert_eq!(mock.count(), 2);

    // A failed call frees its slot too: with a cap of 1, a missing object, then a summary.
    let err = h
        .summaries
        .summarize(location("in/missing.txt"), None)
        .await
        .unwrap_err();
    assert!(matches!(err, SummarizeError::Unavailable(_)), "{err:?}");
    assert_eq!(h.summaries.summarize_in_flight(), 0);
    let r = h
        .summaries
        .summarize(
            SummarizeInput::Content {
                bytes: agreement(602),
                name: "a.txt".into(),
            },
            None,
        )
        .await
        .unwrap();
    assert!(!r.extractive, "{r:?}");
    assert_eq!(h.summaries.summarize_in_flight(), 0);
    assert_eq!(h.summaries.summarize_rejected(), 4);
    h.stop().await;
}
