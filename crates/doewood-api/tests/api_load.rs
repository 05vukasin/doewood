//! The API under many concurrent clients (issue #80): an in-process server on a seeded store,
//! driven by tonic clients over loopback.
//!
//! `cargo test --release -p doewood-api --test api_load -- --ignored --nocapture`
//!
//! The shared store is the instance's SQLite store (standalone), or Postgres in a schema of its
//! own when `DOEWOOD_TEST_POSTGRES_URL` is set. The instance store is a file with read-only
//! connections for API reads, as `serve` opens it (`DOEWOOD_BENCH_READERS` sets their number;
//! 0 runs every read on the writer). For each client count every client is a closed
//! loop (no think time) of unary calls in a fixed mix: `ListSources` 10%, `ListRecords` (page
//! 100) 20%, `GetNode` 15%, `Neighbours` (`similar_to`) 15%, `CompanyDocuments` 15%, `Search`
//! 25%. A share of the clients hold `WatchProgress` streams instead, woken every 10 ms as
//! ingest does after each file. A call shed with `UNAVAILABLE` is retried as docs/api.md
//! advises (100 ms doubling to 10 s, with jitter), or after a fixed
//! `DOEWOOD_BENCH_BACKOFF_MS`; with `DOEWOOD_BENCH_PUSHBACK=1` a client waits the server's
//! `grpc-retry-pushback-ms` instead when the refusal carries one (issue #104). Seeding is
//! followed by `ANALYZE` (Postgres), a checkpoint of the instance store's WAL (issue #140)
//! and a 2 s warm-up that is not reported. At the end a burst of idle connections measures
//! memory per connection.
//!
//! Settings: `DOEWOOD_BENCH_CLIENTS` (comma-separated, default `10,100,500,1000`),
//! `DOEWOOD_BENCH_PER_CONN` (clients per connection, default 1), `DOEWOOD_BENCH_WATCH_PCT`
//! (watchers as a percentage of the clients, on connections of their own, default 10),
//! `DOEWOOD_BENCH_SECS` (seconds per step, default 10), `DOEWOOD_BENCH_DOCS` (documents
//! seeded, default 10000), `DOEWOOD_BENCH_COMPANIES` (companies the documents are spread over,
//! default one per 20 documents), `DOEWOOD_BENCH_IDLE` (idle connections, default 2000),
//! `DOEWOOD_BENCH_ONLY` (one method by name, for example `Search`, instead of the mix).
//!
//! `DOEWOOD_BENCH_POOL` sets the Postgres pool's size (default as `serve`).
//! `DOEWOOD_BENCH_DB_CPU_STAT` may name a cgroup's `cpu.stat` (the database container's) to
//! report its CPU per step too, and its anonymous memory (`memory.stat` next to it) at the end. RSS, threads and CPU are the whole process: the clients run
//! in it too, so they are upper bounds for the server.

mod common;

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use common::{Instance, Options, Shared};
use doewood_api::pb::config_service_client::ConfigServiceClient;
use doewood_api::pb::graph_service_client::GraphServiceClient;
use doewood_api::pb::jobs_service_client::JobsServiceClient;
use doewood_api::pb::records_service_client::RecordsServiceClient;
use doewood_api::pb::search_service_client::SearchServiceClient;
use doewood_api::pb::{self};
use doewood_core::MasterKey;
use doewood_model::{
    Backend, Blake3, Edge, EdgeKind, EdgeSource, NodeKind, ObjectLocation, SearchDoc,
};
use doewood_store::{NewSource, Standalone, Store};
use prometheus::{Encoder as _, Registry, TextEncoder};
use serde_json::json;
use tonic::Code;
use tonic::transport::Channel;
use uuid::Uuid;

const ENDPOINT: &str = "http://objects.test:9000";
/// Words the documents are made of, and the searches ask for.
const WORDS: [&str; 24] = [
    "invoice",
    "contract",
    "annex",
    "delivery",
    "payment",
    "supplier",
    "buyer",
    "steel",
    "timber",
    "cement",
    "transport",
    "service",
    "warranty",
    "penalty",
    "schedule",
    "price",
    "discount",
    "quantity",
    "order",
    "receipt",
    "tender",
    "lease",
    "insurance",
    "audit",
];
/// Distinct rare terms.
const RARE: usize = 5000;

/// The instance store's WAL every 5 s, from its file and the `-shm` wal-index (issue #151):
/// size, frames, and frames a checkpoint copied into the main file (`mxFrame`, `nBackfill`).
struct WalWatch {
    stop: Arc<AtomicBool>,
    thread: std::thread::JoinHandle<String>,
}

impl WalWatch {
    #[allow(clippy::cast_precision_loss)]
    fn start(wal: std::path::PathBuf) -> Self {
        let sample = move |out: &mut String, secs: f64| {
            use std::io::Read as _;
            let Ok(m) = std::fs::metadata(&wal) else {
                return;
            };
            let Some(stem) = wal.to_str().and_then(|w| w.strip_suffix("wal")) else {
                return;
            };
            let shm = format!("{stem}shm");
            let mut head = [0u8; 104];
            if std::fs::File::open(shm)
                .and_then(|mut f| f.read_exact(&mut head))
                .is_err()
            {
                return;
            }
            let at =
                |o: usize| u32::from_ne_bytes([head[o], head[o + 1], head[o + 2], head[o + 3]]);
            let _ = write!(
                out,
                "\n  {secs:>6.0}s WAL {:>7.1} MiB, {:>8} frames, {:>8} in the main file",
                m.len() as f64 / f64::from(1 << 20),
                at(16),
                at(96)
            );
        };
        let stop = Arc::new(AtomicBool::new(false));
        let thread = {
            let stop = Arc::clone(&stop);
            std::thread::spawn(move || {
                let (started, mut out) = (Instant::now(), String::new());
                let mut next = Duration::ZERO;
                while !stop.load(Ordering::Relaxed) {
                    if started.elapsed() >= next {
                        next += Duration::from_secs(5);
                        sample(&mut out, started.elapsed().as_secs_f64());
                    }
                    std::thread::sleep(Duration::from_millis(50));
                }
                sample(&mut out, started.elapsed().as_secs_f64());
                out
            })
        };
        Self { stop, thread }
    }

    fn stop(self) -> String {
        self.stop.store(true, Ordering::Relaxed);
        self.thread.join().unwrap()
    }
}

/// A rare term, scattered by `n`.
fn rare(n: usize) -> String {
    format!("term{}", n.wrapping_mul(2_654_435_761) % RARE)
}

const METHODS: [&str; 6] = [
    "ListSources",
    "ListRecords",
    "GetNode",
    "Neighbours",
    "CompanyDocuments",
    "Search",
];

fn env(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

/// `VmRSS` in MiB and `Threads` of this process (Linux); zeros elsewhere.
fn rss_threads() -> (f64, u64) {
    let status = std::fs::read_to_string("/proc/self/status").unwrap_or_default();
    let field = |name: &str| -> u64 {
        status
            .lines()
            .find_map(|l| l.strip_prefix(name))
            .and_then(|v| v.split_whitespace().next())
            .and_then(|v| v.parse().ok())
            .unwrap_or(0)
    };
    #[allow(clippy::cast_precision_loss)]
    let rss = field("VmRSS:") as f64 / 1024.0;
    (rss, field("Threads:"))
}

/// User and system CPU seconds of this process (Linux); 0 elsewhere.
fn cpu_seconds() -> f64 {
    let Ok(stat) = std::fs::read_to_string("/proc/self/stat") else {
        return 0.0;
    };
    // After the command name: utime is the 12th field, stime the 13th, in 1/100 s.
    let rest = stat.rsplit_once(')').map_or("", |(_, r)| r);
    let f: Vec<f64> = rest
        .split_whitespace()
        .skip(11)
        .take(2)
        .filter_map(|v| v.parse().ok())
        .collect();
    f.iter().sum::<f64>() / 100.0
}

/// CPU seconds of the cgroup whose `cpu.stat` is at `DOEWOOD_BENCH_DB_CPU_STAT` (the database
/// container's, for example), if set.
fn db_cpu_seconds() -> Option<f64> {
    let path = std::env::var("DOEWOOD_BENCH_DB_CPU_STAT").ok()?;
    let stat = std::fs::read_to_string(path).ok()?;
    let usec: f64 = stat
        .lines()
        .find_map(|l| l.strip_prefix("usage_usec "))?
        .trim()
        .parse()
        .ok()?;
    Some(usec / 1e6)
}

/// Anonymous memory in MiB (backends' private memory, not shared buffers or page cache) of the
/// cgroup whose `cpu.stat` is at `DOEWOOD_BENCH_DB_CPU_STAT`, from its `memory.stat`.
fn db_anon_mib() -> Option<f64> {
    let path = std::env::var("DOEWOOD_BENCH_DB_CPU_STAT").ok()?;
    let stat =
        std::fs::read_to_string(std::path::Path::new(&path).with_file_name("memory.stat")).ok()?;
    let bytes: f64 = stat
        .lines()
        .find_map(|l| l.strip_prefix("anon "))?
        .trim()
        .parse()
        .ok()?;
    Some(bytes / f64::from(1 << 20))
}

/// What the load needs to address: the seeded source, documents and companies.
struct Seeded {
    /// `DOEWOOD_BENCH_ONLY`: one method of [`METHODS`] (by name) instead of the mix.
    only: Option<usize>,
    retry: Retry,
    /// `DOEWOOD_BENCH_PUSHBACK`: wait the server's retry pushback when it sends one.
    pushback: bool,
    source: String,
    docs: Vec<Uuid>,
    companies: Vec<Uuid>,
}

fn hash(i: usize) -> Blake3 {
    let mut b = [0u8; 32];
    b[..8].copy_from_slice(&(i as u64).to_le_bytes());
    b[8] = 0xd0;
    Blake3(b)
}

/// `docs` documents under `in/` of one source, each with a record (about 1 KiB), a location, a
/// search row, a `party` edge to one of `docs / 20` companies (`DOEWOOD_BENCH_COMPANIES`)
/// and a `similar_to` edge to the one before it; and 19 more sources, for `ListSources`.
#[allow(clippy::too_many_lines)]
async fn seed(a: &Instance, backend: &Arc<dyn Backend>, docs: usize) -> Seeded {
    let started = Instant::now();
    let mut source = None;
    for i in 0..20 {
        let mut s = NewSource::new(ENDPOINT, format!("bucket{i}"));
        s.prefix = "in/".into();
        let id = a.store.add_source(&s, None).unwrap();
        source.get_or_insert(id);
    }
    let source = source.unwrap();
    // `DOEWOOD_BENCH_COMPANIES`: fewer companies make each one bigger (issue #127).
    let ncompanies = env("DOEWOOD_BENCH_COMPANIES", docs / 20).clamp(1, docs);
    let mut companies = Vec::with_capacity(ncompanies);
    for c in 0..ncompanies {
        let id = backend
            .upsert_node(
                NodeKind::Company,
                &format!("company-{c}"),
                &json!({"name": format!("Company {c} d.o.o.")}),
            )
            .await
            .unwrap();
        companies.push(id);
    }
    let companies = Arc::new(companies);
    let bucket = "bucket0".to_owned();
    let mut tasks = tokio::task::JoinSet::new();
    let per = docs.div_ceil(16);
    for t in 0..16 {
        let (backend, companies, bucket) =
            (Arc::clone(backend), Arc::clone(&companies), bucket.clone());
        tasks.spawn(async move {
            let mut out = Vec::new();
            for i in (t * per)..((t + 1) * per).min(docs) {
                // 20 common words and 20 of `RARE` terms (each in about 40 documents of
                // 10 000), so a search matches a realistic few.
                let words: Vec<String> = (0..20)
                    .map(|k| WORDS[(i * 7 + k * 13) % WORDS.len()].to_owned())
                    .chain((0..20).map(|k| rare(i * 20 + k)))
                    .collect();
                let title = format!("{} {i}", WORDS[i % WORDS.len()]);
                let body = json!({"title": title, "text": words.join(" "), "n": i,
                    "parties": [{"name": format!("Company {} d.o.o.", i % companies.len())}]});
                backend
                    .put_record(
                        &hash(i),
                        &doewood_model::canonical::to_canonical(&body).unwrap(),
                        &[("quail".to_owned(), "1".to_owned())].into(),
                    )
                    .await
                    .unwrap();
                let doc = backend
                    .upsert_location(
                        &hash(i),
                        &ObjectLocation {
                            endpoint: ENDPOINT.into(),
                            bucket: bucket.clone(),
                            key: format!("in/{i:08}.pdf"),
                        },
                    )
                    .await
                    .unwrap()
                    .document;
                backend
                    .index_document(&SearchDoc {
                        blake3: hash(i),
                        title,
                        body: words.join(" "),
                        kind: Some("invoice".into()),
                        date: None,
                        amount: Some(100.0),
                    })
                    .await
                    .unwrap();
                backend
                    .upsert_edge(&Edge::new(
                        doc,
                        companies[i % companies.len()],
                        EdgeKind::Party,
                        EdgeSource::Document,
                    ))
                    .await
                    .unwrap();
                out.push((i, doc));
            }
            out
        });
    }
    let mut all = Vec::with_capacity(docs);
    while let Some(r) = tasks.join_next().await {
        all.extend(r.unwrap());
    }
    all.sort_unstable();
    let docs: Vec<Uuid> = all.into_iter().map(|(_, d)| d).collect();
    for w in docs.windows(2) {
        backend
            .upsert_edge(
                &Edge::new(w[0], w[1], EdgeKind::SimilarTo, EdgeSource::Document)
                    .with_props(json!({"score": 0.9}))
                    .with_confidence(0.9),
            )
            .await
            .unwrap();
    }
    println!(
        "api_load: seeded {} documents, {} companies in {:.1}s",
        docs.len(),
        companies.len(),
        started.elapsed().as_secs_f64()
    );
    let only = std::env::var("DOEWOOD_BENCH_ONLY")
        .ok()
        .map(|m| METHODS.iter().position(|x| *x == m).expect("a method name"));
    Seeded {
        only,
        retry: Retry::from_env(),
        pushback: std::env::var("DOEWOOD_BENCH_PUSHBACK").is_ok_and(|v| v == "1"),
        source: source.to_string(),
        docs,
        companies: Arc::try_unwrap(companies).unwrap(),
    }
}

/// How a client retries a call shed with `UNAVAILABLE`.
#[derive(Clone, Copy)]
enum Retry {
    /// As docs/api.md advises: 100 ms doubling to 10 s, with jitter.
    Exponential,
    /// A fixed wait (`DOEWOOD_BENCH_BACKOFF_MS`): an impatient client.
    Fixed(Duration),
}

impl Retry {
    fn from_env() -> Self {
        std::env::var("DOEWOOD_BENCH_BACKOFF_MS")
            .ok()
            .and_then(|v| v.parse().ok())
            .map_or(Self::Exponential, |ms| {
                Self::Fixed(Duration::from_millis(ms))
            })
    }

    /// The wait after `last` (zero after a success).
    fn next(self, last: Duration) -> Duration {
        match self {
            Self::Fixed(d) => d,
            Self::Exponential if last.is_zero() => Duration::from_millis(100),
            Self::Exponential => (last * 2).min(Duration::from_secs(10)),
        }
    }
}

/// One client's calls: method index, latency of each successful call, errors by code.
#[derive(Default)]
struct Tally {
    ok: Vec<Vec<Duration>>,
    errors: BTreeMap<String, u64>,
}

/// One unary call of the mix, by `n` (the client's call number).
async fn call(ch: &Channel, seeded: &Seeded, n: usize) -> (usize, Result<(), tonic::Status>) {
    // The first slot of each method in the mix, for `DOEWOOD_BENCH_ONLY`.
    const SLOTS: [usize; 6] = [0, 2, 6, 9, 12, 15];
    let doc = seeded.docs[n.wrapping_mul(7919) % seeded.docs.len()];
    let company = seeded.companies[n.wrapping_mul(104_729) % seeded.companies.len()];
    match seeded.only.map_or(n % 20, |m| SLOTS[m]) {
        0 | 1 => (
            0,
            ConfigServiceClient::new(ch.clone())
                .list_sources(pb::ListSourcesRequest {})
                .await
                .map(drop),
        ),
        2..=5 => (
            1,
            RecordsServiceClient::new(ch.clone())
                .list_records(pb::ListRecordsRequest {
                    source_id: seeded.source.clone(),
                    page_size: 100,
                    page_token: String::new(),
                })
                .await
                .map(drop),
        ),
        6..=8 => (
            2,
            GraphServiceClient::new(ch.clone())
                .get_node(pb::GetNodeRequest {
                    lookup: Some(pb::get_node_request::Lookup::Id(doc.to_string())),
                })
                .await
                .map(drop),
        ),
        9..=11 => (
            3,
            GraphServiceClient::new(ch.clone())
                .neighbours(pb::NeighboursRequest {
                    node_id: doc.to_string(),
                    direction: pb::Direction::Unspecified as i32,
                    edge_kinds: vec![pb::EdgeKind::SimilarTo as i32],
                    node_kind: pb::NodeKind::Document as i32,
                    at: String::new(),
                    include_inactive: false,
                    ..Default::default()
                })
                .await
                .map(drop),
        ),
        12..=14 => (
            4,
            GraphServiceClient::new(ch.clone())
                .company_documents(pb::CompanyDocumentsRequest {
                    company_id: company.to_string(),
                    ..Default::default()
                })
                .await
                .map(drop),
        ),
        _ => (
            5,
            SearchServiceClient::new(ch.clone())
                .search(pb::SearchRequest {
                    text: if n.is_multiple_of(2) {
                        rare(n)
                    } else {
                        format!("{} {}", WORDS[n % WORDS.len()], rare(n))
                    },
                    limit: 20,
                    ..pb::SearchRequest::default()
                })
                .await
                .map(drop),
        ),
    }
}

#[allow(clippy::cast_precision_loss)]
async fn client(ch: Channel, seeded: Arc<Seeded>, seed: usize, stop: Arc<AtomicBool>) -> Tally {
    let mut t = Tally {
        ok: vec![Vec::new(); METHODS.len()],
        ..Tally::default()
    };
    let mut n = seed;
    let mut backoff = Duration::ZERO;
    while !stop.load(Ordering::Relaxed) {
        let started = Instant::now();
        let (method, r) = call(&ch, &seeded, n).await;
        n += 1;
        match r {
            Ok(()) => {
                t.ok[method].push(started.elapsed());
                backoff = Duration::ZERO;
            }
            Err(e) => {
                *t.errors.entry(format!("{:?}", e.code())).or_default() += 1;
                if e.code() == Code::Unavailable {
                    backoff = seeded.retry.next(backoff);
                    // Jitter: 50% to 100% of the backoff, scattered by the call number.
                    let wait = backoff
                        .mul_f64(0.5 + (n.wrapping_mul(2_654_435_761) % 1000) as f64 / 2000.0);
                    // As a gRPC retry policy does (gRFC A6): the server's pushback wins.
                    let pushback = e
                        .metadata()
                        .get("grpc-retry-pushback-ms")
                        .and_then(|v| v.to_str().ok()?.parse().ok())
                        .filter(|_| seeded.pushback);
                    let wait = pushback.map_or(wait, Duration::from_millis);
                    let until = Instant::now() + wait;
                    while !stop.load(Ordering::Relaxed) && Instant::now() < until {
                        tokio::time::sleep(
                            until
                                .saturating_duration_since(Instant::now())
                                .min(Duration::from_millis(50)),
                        )
                        .await;
                    }
                }
            }
        }
    }
    t
}

async fn connect(a: &Instance) -> Channel {
    tonic::transport::Endpoint::from_shared(format!("http://{}", a.addr))
        .unwrap()
        .connect_timeout(Duration::from_secs(10))
        .connect()
        .await
        .unwrap()
}

fn pct(sorted: &[Duration], p: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    #[allow(
        clippy::cast_precision_loss,
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss
    )]
    let i = ((sorted.len() as f64 * p) as usize).min(sorted.len() - 1);
    sorted[i].as_secs_f64() * 1e3
}

/// The server's own counters (docs/api.md, "Metrics"), summed over methods.
#[derive(Default)]
struct Server {
    /// Unary calls answered (shed ones included) and the seconds they took in the server.
    calls: u64,
    seconds: f64,
    shed: u64,
    connections: u64,
}

impl Server {
    fn read(registry: &Registry) -> Self {
        let mut out = Vec::new();
        TextEncoder::new()
            .encode(&registry.gather(), &mut out)
            .unwrap();
        let text = String::from_utf8(out).unwrap();
        let mut s = Self::default();
        for line in text.lines() {
            let Some((name, value)) = line.rsplit_once(' ') else {
                continue;
            };
            // Streams count their whole lifetime: unary calls only.
            if name.contains("WatchProgress") {
                continue;
            }
            let v: f64 = value.parse().unwrap_or(0.0);
            #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
            let n = v as u64;
            if name.starts_with("doewood_grpc_call_duration_seconds_sum") {
                s.seconds += v;
            } else if name.starts_with("doewood_grpc_call_duration_seconds_count") {
                s.calls += n;
            } else if name == "doewood_grpc_shed_total" {
                s.shed = n;
            } else if name == "doewood_grpc_connections_open" {
                s.connections = n;
            }
        }
        s
    }

    fn since(&self, before: &Self) -> Self {
        Self {
            calls: self.calls - before.calls,
            seconds: self.seconds - before.seconds,
            shed: self.shed - before.shed,
            connections: self.connections,
        }
    }
}

/// One step: `clients` loops (and their watchers) for `secs`; prints a table row.
#[allow(clippy::too_many_lines, clippy::cast_precision_loss)]
async fn step(a: &Instance, registry: &Registry, seeded: &Arc<Seeded>, clients: usize, secs: u64) {
    let per_conn = env("DOEWOOD_BENCH_PER_CONN", 1).max(1);
    let watchers = clients * env("DOEWOOD_BENCH_WATCH_PCT", 10) / 100;
    let stop = Arc::new(AtomicBool::new(false));

    // Watchers first, on connections of their own.
    let messages = Arc::new(AtomicU64::new(0));
    let mut readers = Vec::new();
    for _ in 0..watchers {
        let mut stream = JobsServiceClient::new(connect(a).await)
            .watch_progress(pb::WatchProgressRequest { source_ids: vec![] })
            .await
            .unwrap()
            .into_inner();
        let (messages, stop) = (Arc::clone(&messages), Arc::clone(&stop));
        readers.push(tokio::spawn(async move {
            while !stop.load(Ordering::Relaxed) {
                match tokio::time::timeout(Duration::from_millis(200), stream.message()).await {
                    Ok(Ok(Some(_))) => {
                        messages.fetch_add(1, Ordering::Relaxed);
                    }
                    Ok(_) => return false,
                    Err(_) => {}
                }
            }
            true
        }));
    }
    // Ingest-like wake-ups of the watchers.
    let toucher = {
        let (progress, stop) = (Arc::clone(&a.progress), Arc::clone(&stop));
        tokio::spawn(async move {
            while !stop.load(Ordering::Relaxed) {
                progress.touch();
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
    };
    // A trivial instance-store call every millisecond: how long it waits for the lock.
    let probe = {
        let (store, stop) = (Arc::clone(&a.store), Arc::clone(&stop));
        std::thread::spawn(move || {
            let mut waits = Vec::new();
            while !stop.load(Ordering::Relaxed) {
                let t = Instant::now();
                store.schema_version().unwrap();
                waits.push(t.elapsed());
                std::thread::sleep(Duration::from_millis(1));
            }
            waits.sort();
            waits
        })
    };
    // Peak RSS and threads while the step runs.
    let sampler = {
        let stop = Arc::clone(&stop);
        tokio::spawn(async move {
            let (mut rss, mut threads) = (0f64, 0u64);
            while !stop.load(Ordering::Relaxed) {
                let (r, t) = rss_threads();
                rss = rss.max(r);
                threads = threads.max(t);
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            (rss, threads)
        })
    };

    let mut loops = Vec::with_capacity(clients);
    let mut ch = connect(a).await;
    for i in 0..clients {
        if i > 0 && i % per_conn == 0 {
            ch = connect(a).await;
        }
        loops.push(tokio::spawn(client(
            ch.clone(),
            Arc::clone(seeded),
            i * 31,
            Arc::clone(&stop),
        )));
    }
    let started = Instant::now();
    let (cpu, db_cpu) = (cpu_seconds(), db_cpu_seconds());
    let server = Server::read(registry);
    tokio::time::sleep(Duration::from_secs(secs)).await;
    let open = Server::read(registry).connections;
    let cpu = (cpu_seconds() - cpu) / secs as f64;
    let db_cpu = db_cpu
        .zip(db_cpu_seconds())
        .map_or_else(String::new, |(a, b)| {
            format!(", database {:.2} cores", (b - a) / secs as f64)
        });
    stop.store(true, Ordering::Relaxed);
    let mut by_method: Vec<Vec<Duration>> = vec![Vec::new(); METHODS.len()];
    let mut errors: BTreeMap<String, u64> = BTreeMap::new();
    for l in loops {
        let t = l.await.unwrap();
        for (m, v) in t.ok.into_iter().enumerate() {
            by_method[m].extend(v);
        }
        for (k, v) in t.errors {
            *errors.entry(k).or_default() += v;
        }
    }
    let elapsed = started.elapsed().as_secs_f64();
    let server = Server::read(registry).since(&server);
    toucher.await.unwrap();
    let mut broken = 0;
    for r in readers {
        if !r.await.unwrap() {
            broken += 1;
        }
    }
    let waits = probe.join().unwrap();
    let (rss, threads) = sampler.await.unwrap();

    let mut all: Vec<Duration> = by_method.iter().flatten().copied().collect();
    all.sort_unstable();
    let failed: u64 = errors.values().sum();
    let shed = errors.get("Unavailable").copied().unwrap_or(0);
    println!(
        "api_load: {clients:>5} clients ({} conns, {watchers} watchers): {:>7.0} calls/s, p50 \
         {:.2} ms, p99 {:.2} ms, p99.9 {:.2} ms; {failed} errors ({shed} shed) {errors:?}; \
         CPU {cpu:.2} cores{db_cpu}; peak RSS {rss:.0} MiB, {threads} threads; store probe \
         p50 {:.0} us, p99 {:.0} us; {} progress messages, {broken} streams ended early",
        clients.div_ceil(per_conn),
        all.len() as f64 / elapsed,
        pct(&all, 0.5),
        pct(&all, 0.99),
        pct(&all, 0.999),
        pct(&waits, 0.5) * 1e3,
        pct(&waits, 0.99) * 1e3,
        messages.load(Ordering::Relaxed),
    );
    println!(
        "api_load:        server metrics: {} calls answered, mean {:.2} ms in the server; \
         {} shed; {open} connections open",
        server.calls,
        server.seconds * 1e3 / server.calls.max(1) as f64,
        server.shed
    );
    let mut line = String::from("api_load:        per method p50/p99 ms:");
    for (m, v) in by_method.iter_mut().enumerate() {
        v.sort_unstable();
        let _ = write!(
            line,
            " {} {:.1}/{:.1} ({:.0}/s)",
            METHODS[m],
            pct(v, 0.5),
            pct(v, 0.99),
            v.len() as f64 / elapsed
        );
    }
    println!("{line}");
}

/// 16 clients for 2 s, results dropped.
async fn warm_up(a: &Instance, seeded: &Arc<Seeded>) {
    let stop = Arc::new(AtomicBool::new(false));
    let ch = connect(a).await;
    let loops: Vec<_> = (0..16)
        .map(|i| {
            tokio::spawn(client(
                ch.clone(),
                Arc::clone(seeded),
                i * 17,
                Arc::clone(&stop),
            ))
        })
        .collect();
    tokio::time::sleep(Duration::from_secs(2)).await;
    stop.store(true, Ordering::Relaxed);
    for l in loops {
        l.await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "a measurement, not a check"]
#[allow(clippy::cast_precision_loss)]
async fn api_load() {
    let steps: Vec<usize> = std::env::var("DOEWOOD_BENCH_CLIENTS")
        .unwrap_or_else(|_| "10,100,500,1000".into())
        .split(',')
        .filter_map(|v| v.trim().parse().ok())
        .collect();
    let secs = env("DOEWOOD_BENCH_SECS", 10) as u64;
    let docs = env("DOEWOOD_BENCH_DOCS", 10_000).max(2);
    let idle = env("DOEWOOD_BENCH_IDLE", 2000);

    let pg = Shared::postgres().await;
    // As `serve`: the instance store in a file, with read-only connections for API reads
    // (`DOEWOOD_BENCH_READERS`, default as `Store::open`; 0 reads on the writer).
    let dir = std::env::temp_dir().join(format!("doewood-api-load-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let file = dir.join(doewood_store::STORE_FILE);
    let key = MasterKey::from_bytes([7; 32]);
    let readers = std::env::var("DOEWOOD_BENCH_READERS")
        .ok()
        .and_then(|v| v.parse().ok());
    let store = Arc::new(match readers {
        Some(n) => Store::open_with_readers(&file, &key, n).unwrap(),
        None => Store::open(&file, &key).unwrap(),
    });
    let a = Instance::start_with(
        "bench",
        None,
        Options {
            // As `serve`.
            progress_interval: Duration::from_secs(1),
            store: Some(store),
            ..Options::default()
        },
    )
    .await;
    let backend: Arc<dyn Backend> = match &pg {
        Some(pg) => Arc::clone(&pg.backend),
        // Standalone: records, graph and search live in the instance store, as in `serve`.
        None => Arc::new(Standalone::new(Arc::clone(&a.store))),
    };
    a.backend.send_replace(Some(Arc::clone(&backend)));
    println!(
        "api_load: backend {}, store readers {}, pool {}, {} CPUs, {secs}s per step",
        if pg.is_some() { "postgres" } else { "sqlite" },
        a.store.readers(),
        std::env::var("DOEWOOD_BENCH_POOL").unwrap_or_else(|_| "default".into()),
        std::thread::available_parallelism().map_or(0, std::num::NonZero::get)
    );
    let registry = Registry::new();
    a.state.register_metrics(&registry).unwrap();
    let wal = WalWatch::start(dir.join(format!("{}-wal", doewood_store::STORE_FILE)));
    let seeded = Arc::new(seed(&a, &backend, docs).await);
    println!(
        "api_load: the instance store's WAL while seeding:{}",
        wal.stop()
    );
    if let Some(pg) = &pg {
        pg.analyze().await;
    }
    // The seeding's WAL into the main file (issue #140), so every step starts from the same
    // state. Before issue #151 seeding left a WAL that was never reset (1.3 million frames,
    // 5.4 GB, at 200,000 documents: its last phase, the `similar_to` edges one at a time,
    // never ran SQLite's automatic checkpoint), and the steps measured readers looking their
    // pages up in it (Search 31 against 9.5 ms at 10 clients). Now the store's own checkpoints
    // keep it small, and this finds little left.
    let t = Instant::now();
    let (frames, done) = a.store.checkpoint().unwrap();
    println!(
        "api_load: instance store checkpoint: {frames} WAL frames ({done} already in the main \
         file) in {:.1}s",
        t.elapsed().as_secs_f64()
    );
    // Warm up (caches, the pool's connections and statements); not reported.
    warm_up(&a, &seeded).await;
    for clients in steps {
        step(&a, &registry, &seeded, clients, secs).await;
    }
    if let Some(anon) = db_anon_mib() {
        println!("api_load: database anon memory after the steps {anon:.1} MiB");
    }

    // Memory per idle connection: open them, let the handshakes settle, compare RSS.
    let (before, _) = rss_threads();
    let mut conns = Vec::with_capacity(idle);
    for _ in 0..idle {
        conns.push(connect(&a).await);
    }
    tokio::time::sleep(Duration::from_secs(1)).await;
    let (after, threads) = rss_threads();
    println!(
        "api_load: {idle} idle connections: RSS {before:.0} -> {after:.0} MiB ({:.1} KiB per \
         connection, client side included), {threads} threads",
        (after - before) * 1024.0 / idle.max(1) as f64
    );
    drop(conns);
    a.stop().await;
    if let Some(pg) = pg {
        pg.drop_schema().await;
    }
    let _ = std::fs::remove_dir_all(&dir);
}
