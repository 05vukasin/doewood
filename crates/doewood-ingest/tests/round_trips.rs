//! Round trips to Postgres per graph write (issue #103), counted by a proxy between the store
//! and the database that reads the wire protocol: a statement is a `Bind` or a simple `Query`
//! the client sends (prepares are not counted), and a round trip is a send that starts while
//! the connection has no answer outstanding (statements sent together, pipelined, are one).
//! Runs when `DOEWOOD_TEST_POSTGRES_URL` is set, in a schema of its own.
//!
//! `near_duplicate_corpus` (ignored) is the measurement of issue #126 on either store.

mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use doewood_ingest::company::{IdKind, Identifier};
use doewood_ingest::facts::{Cite, DocFacts, Party};
use doewood_ingest::{GraphWriter, SimilarityConfig};
use doewood_model::{
    Backend, Blake3, Edge, EdgeKind, EdgeSource, NodeKind, ObjectLocation, PropsPatch, SearchDoc,
    StepVersions,
};
use serde_json::json;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

const PG_VAR: &str = "DOEWOOD_TEST_POSTGRES_URL";

/// What the proxy saw, over all connections.
#[derive(Default)]
struct Counts {
    statements: AtomicU64,
    round_trips: AtomicU64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Seen {
    statements: u64,
    round_trips: u64,
}

impl Counts {
    fn now(&self) -> Seen {
        Seen {
            statements: self.statements.load(Ordering::SeqCst),
            round_trips: self.round_trips.load(Ordering::SeqCst),
        }
    }

    fn since(&self, before: Seen) -> Seen {
        let now = self.now();
        Seen {
            statements: now.statements - before.statements,
            round_trips: now.round_trips - before.round_trips,
        }
    }
}

/// Complete messages at the front of `buf` (`[type][length][payload]`, or `[length][payload]`
/// for the untyped startup message), drained; their type bytes (0 for the startup message).
fn messages(buf: &mut Vec<u8>, typed: &mut bool) -> Vec<u8> {
    let mut out = Vec::new();
    loop {
        let head = usize::from(*typed);
        let Some(len) = buf.get(head..head + 4) else {
            return out;
        };
        let len = u32::from_be_bytes(len.try_into().unwrap()) as usize;
        if buf.len() < head + len {
            return out;
        }
        out.push(if *typed { buf[0] } else { 0 });
        buf.drain(..head + len);
        *typed = true;
    }
}

/// A proxy to `upstream` on a local port; every connection is counted into the result.
async fn proxy(upstream: std::net::SocketAddr) -> (std::net::SocketAddr, Arc<Counts>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let counts = Arc::new(Counts::default());
    let all = Arc::clone(&counts);
    tokio::spawn(async move {
        while let Ok((client, _)) = listener.accept().await {
            let server = TcpStream::connect(upstream).await.unwrap();
            tokio::spawn(pump(client, server, Arc::clone(&all)));
        }
    });
    (addr, counts)
}

/// Forwards one connection both ways. `outstanding` is the client's requests that end in a
/// `ReadyForQuery` (the startup message, `Sync`, `Query`) minus the server's
/// `ReadyForQuery` messages.
async fn pump(client: TcpStream, server: TcpStream, counts: Arc<Counts>) {
    let outstanding = Arc::new(std::sync::Mutex::new(0i64));
    let (mut cr, mut cw) = client.into_split();
    let (mut sr, mut sw) = server.into_split();
    let up = {
        let outstanding = Arc::clone(&outstanding);
        async move {
            let (mut buf, mut typed, mut chunk) = (Vec::new(), false, vec![0u8; 64 * 1024]);
            loop {
                let n = match cr.read(&mut chunk).await {
                    Ok(0) | Err(_) => break,
                    Ok(n) => n,
                };
                {
                    let mut out = outstanding.lock().unwrap();
                    if *out == 0 && buf.is_empty() {
                        counts.round_trips.fetch_add(1, Ordering::SeqCst);
                    }
                    buf.extend_from_slice(&chunk[..n]);
                    for t in messages(&mut buf, &mut typed) {
                        match t {
                            0 | b'S' => *out += 1,
                            b'Q' => {
                                *out += 1;
                                counts.statements.fetch_add(1, Ordering::SeqCst);
                            }
                            b'B' => {
                                counts.statements.fetch_add(1, Ordering::SeqCst);
                            }
                            _ => {}
                        }
                    }
                }
                if sw.write_all(&chunk[..n]).await.is_err() {
                    break;
                }
            }
            let _ = sw.shutdown().await;
        }
    };
    let down = async move {
        let (mut buf, mut typed, mut chunk) = (Vec::new(), true, vec![0u8; 64 * 1024]);
        loop {
            let n = match sr.read(&mut chunk).await {
                Ok(0) | Err(_) => break,
                Ok(n) => n,
            };
            buf.extend_from_slice(&chunk[..n]);
            let ready = messages(&mut buf, &mut typed)
                .into_iter()
                .filter(|t| *t == b'Z')
                .count();
            *outstanding.lock().unwrap() -= i64::try_from(ready).unwrap();
            if cw.write_all(&chunk[..n]).await.is_err() {
                break;
            }
        }
        let _ = cw.shutdown().await;
    };
    tokio::join!(up, down);
}

/// A Postgres store in a fresh schema, connected through a counting proxy; with the admin
/// connection and the schema, to drop it at the end.
async fn store() -> Option<(
    Arc<dyn Backend>,
    Arc<Counts>,
    tokio_postgres::Client,
    String,
)> {
    store_via(true).await
}

/// [`store`], through the proxy only when `proxied` (nothing is counted otherwise: the proxy
/// itself adds time to every message).
async fn store_via(
    proxied: bool,
) -> Option<(
    Arc<dyn Backend>,
    Arc<Counts>,
    tokio_postgres::Client,
    String,
)> {
    let Some(url) = std::env::var(PG_VAR).ok().filter(|v| !v.trim().is_empty()) else {
        eprintln!("skipping: {PG_VAR} is not set; the Postgres variant did not run");
        return None;
    };
    let (admin, conn) = tokio_postgres::connect(&url, tokio_postgres::NoTls)
        .await
        .expect("connect to the test database");
    tokio::spawn(conn);
    let schema = format!("dw_trips_{}", uuid::Uuid::now_v7().simple());
    admin
        .batch_execute(&format!("CREATE SCHEMA {schema}"))
        .await
        .unwrap();
    let mut target = url::Url::parse(&url).unwrap();
    let host = target.host_str().unwrap().to_owned();
    let port = target.port().unwrap_or(5432);
    let upstream = tokio::net::lookup_host((host.as_str(), port))
        .await
        .unwrap()
        .next()
        .unwrap();
    let (addr, counts) = proxy(upstream).await;
    if proxied {
        target.set_host(Some("127.0.0.1")).unwrap();
        target.set_port(Some(addr.port())).unwrap();
    }
    let sep = if target.query().is_some() { '&' } else { '?' };
    let scoped = format!("{target}{sep}sslmode=disable&options=-c%20search_path%3D{schema}");
    let pg = doewood_pg::PgStore::connect(&doewood_core::PostgresUrl::parse(&scoped).unwrap())
        .await
        .unwrap();
    Some((Arc::new(pg), counts, admin, schema))
}

/// An empty record body, in the canonical form the store takes.
fn empty_record() -> Box<serde_json::value::RawValue> {
    doewood_model::canonical::to_canonical(&json!({})).unwrap()
}

fn at(key: &str) -> ObjectLocation {
    ObjectLocation {
        endpoint: "http://objects.test:9000".into(),
        bucket: "docs".into(),
        key: key.into(),
    }
}

/// Each store call the graph writer makes per document is one round trip (two for a props
/// patch: read, then compare-and-set), however many statements it sends.
#[tokio::test]
async fn store_calls_are_one_round_trip() {
    let Some((b, counts, admin, schema)) = store().await else {
        return;
    };
    for n in 1..=4u8 {
        b.put_record(&Blake3([n; 32]), &empty_record(), &StepVersions::new())
            .await
            .unwrap();
    }
    let company = b.ensure_node(NodeKind::Company, "trips").await.unwrap();
    // Each call twice: the first prepares its statements on the connection.
    let mut checks = Vec::new();
    for n in [1u8, 2] {
        let h = Blake3([n; 32]);
        let before = counts.now();
        let doc = b
            .upsert_location(&h, &at(&format!("trips/{n}.pdf")))
            .await
            .unwrap()
            .document;
        checks.push(("upsert_location", counts.since(before), 1, 1));
        let before = counts.now();
        b.update_props(doc, &PropsPatch::new().set(&["title"], json!(n)))
            .await
            .unwrap();
        checks.push(("update_props", counts.since(before), 2, 2));
        let before = counts.now();
        b.put_sketch(&h, &[n; 16], &[u64::from(n), 100 + u64::from(n)])
            .await
            .unwrap();
        checks.push(("put_sketch", counts.since(before), 1, 1));
        let before = counts.now();
        b.index_document(&SearchDoc {
            blake3: h,
            title: format!("doc {n}"),
            body: String::new(),
            kind: None,
            date: None,
            amount: None,
        })
        .await
        .unwrap();
        checks.push(("index_document", counts.since(before), 1, 1));
        let claims: Vec<(String, serde_json::Value)> = (0..4)
            .map(|i| (format!("name:trips-{n}-{i}"), json!({"i": i})))
            .collect();
        let before = counts.now();
        b.claim_aliases(NodeKind::Company, company, &claims)
            .await
            .unwrap();
        checks.push(("claim_aliases (4)", counts.since(before), 4, 1));
        let keys: Vec<String> = claims.iter().map(|(k, _)| k.clone()).collect();
        let before = counts.now();
        b.aliases(NodeKind::Company, &keys).await.unwrap();
        checks.push(("aliases (4)", counts.since(before), 1, 1));
        let before = counts.now();
        b.upsert_live_edge(&Edge::new(
            doc,
            company,
            EdgeKind::Party,
            EdgeSource::Document,
        ))
        .await
        .unwrap();
        checks.push(("upsert_live_edge", counts.since(before), 1, 1));
    }
    // The second round, with every statement prepared.
    for (name, seen, statements, round_trips) in checks.split_off(checks.len() / 2) {
        assert_eq!(
            seen,
            Seen {
                statements,
                round_trips
            },
            "{name}"
        );
    }
    admin
        .batch_execute(&format!("DROP SCHEMA {schema} CASCADE"))
        .await
        .unwrap();
}

fn party(surface: &str, pib: Option<&str>) -> Party {
    Party {
        quail_key: format!("party.{surface}"),
        surface: surface.into(),
        score: 0.9,
        roles: vec!["supplier".into()],
        legal_form: None,
        jurisdiction: None,
        ids: pib
            .map(|p| Identifier::parse(IdKind::RsPib, &common::pib(p)).unwrap())
            .into_iter()
            .collect(),
        mentions: vec![(Some(1), 0, 10)],
    }
}

/// The facts of the `throughput` mix: a contract with two parties (one with a PIB) and its
/// own number, an annex that amends it, an invoice with one party, and a sheet with nothing
/// but a sketch.
fn mix(i: u32) -> [DocFacts; 4] {
    let sketch = |n: u32| {
        Some((
            vec![7u8; 32],
            (0..4).map(|b| u64::from(n * 8 + b)).collect(),
        ))
    };
    let pib = format!("{:08}", 20_000_000 + i);
    [
        DocFacts {
            format: "pdf".into(),
            title: Some(format!("Ugovor {i}")),
            parties: vec![
                party(&format!("Firma {i} d.o.o."), Some(&pib)),
                party("Kupac Grupa a.d.", None),
            ],
            numbers: vec![format!("{i}/26")],
            minhash: sketch(4 * i),
            ..DocFacts::default()
        },
        DocFacts {
            format: "docx".into(),
            cites: vec![Cite {
                relation: "amends".into(),
                key: format!("{i}/26"),
                raw: format!("Ugovor br. {i}/26"),
                kind: Some("contract".into()),
            }],
            minhash: sketch(4 * i + 1),
            ..DocFacts::default()
        },
        DocFacts {
            format: "txt".into(),
            parties: vec![party(&format!("Vendor {i} LLC"), None)],
            numbers: vec![format!("inv-{i}")],
            ..DocFacts::default()
        },
        DocFacts {
            format: "xlsx".into(),
            minhash: sketch(4 * i + 3),
            ..DocFacts::default()
        },
    ]
}

/// Statements and round trips per document for the `throughput` mix, under the targets of
/// issue #103. Before it this mix took 32.25 statements per document, each its own round trip.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn graph_writes_stay_under_the_round_trip_target() {
    const STATEMENTS_PER_DOC: f64 = 15.0;
    const ROUND_TRIPS_PER_DOC: f64 = 14.5;
    let Some((b, counts, admin, schema)) = store().await else {
        return;
    };
    let writer = GraphWriter::new(Arc::clone(&b), SimilarityConfig::default());
    let mut seen = Vec::new();
    for i in 0..8u32 {
        let before = counts.now();
        for (j, facts) in mix(i).iter().enumerate() {
            let h =
                Blake3::from_slice(blake3::hash(format!("{i}/{j}").as_bytes()).as_bytes()).unwrap();
            b.put_record(&h, &empty_record(), &StepVersions::new())
                .await
                .unwrap();
            writer
                .write(&h, &at(&format!("mix/{i}/{j}")), facts, "x", 0.8)
                .await
                .unwrap();
        }
        let s = counts.since(before);
        // Less the four record writes.
        seen.push((s.statements - 4, s.round_trips - 4));
    }
    // The first rounds open the pool's connections and prepare their statements.
    let last = &seen[4..];
    #[allow(clippy::cast_precision_loss)]
    let per_doc = |f: fn(&(u64, u64)) -> u64| last.iter().map(f).sum::<u64>() as f64 / 16.0;
    let (statements, round_trips) = (per_doc(|s| s.0), per_doc(|s| s.1));
    println!("per document: {statements:.2} statements, {round_trips:.2} round trips ({seen:?})");
    assert!(statements <= STATEMENTS_PER_DOC, "{statements} statements");
    assert!(
        round_trips <= ROUND_TRIPS_PER_DOC,
        "{round_trips} round trips"
    );
    admin
        .batch_execute(&format!("DROP SCHEMA {schema} CASCADE"))
        .await
        .unwrap();
}

/// A number from the environment variable `var`, else `default`.
fn env_or(var: &str, default: u64) -> u64 {
    std::env::var(var)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

/// The measurement of issue #126: `DOEWOOD_NEAR_DUP_DOCS` documents (default 2000) of
/// [`common::near_duplicate_text`] with 4 templates, written one after the other through the
/// graph writer, on Postgres (`DOEWOOD_TEST_POSTGRES_URL`) or, when `DOEWOOD_NEAR_DUP_SQLITE`
/// names a directory, on a SQLite store file in it. Per tenth of the corpus: the graph write
/// per document, statements and round trips per document (Postgres with
/// `DOEWOOD_NEAR_DUP_COUNT` set, through the counting proxy), `similar_to` edges per
/// document, and the candidate query alone for the last template document (best of 5), as
/// its plan on Postgres. Last, a digest of every `similar_to` edge, to compare two builds.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "a measurement (issue #126); see docs/deploy.md"]
#[allow(clippy::too_many_lines, clippy::cast_precision_loss)]
async fn near_duplicate_corpus() {
    let docs = env_or("DOEWOOD_NEAR_DUP_DOCS", 2000);
    let (b, counts, pg): (Arc<dyn Backend>, Option<Arc<Counts>>, _) =
        if let Ok(dir) = std::env::var("DOEWOOD_NEAR_DUP_SQLITE") {
            let path = std::path::Path::new(&dir)
                .join(format!("near-dup-{}.db", uuid::Uuid::now_v7().simple()));
            let key = doewood_core::MasterKey::from_bytes([9; 32]);
            let store = doewood_store::Store::open(&path, &key).unwrap();
            println!("store: SQLite {}", path.display());
            (
                Arc::new(doewood_store::Standalone::new(Arc::new(store))),
                None,
                None,
            )
        } else {
            let count = std::env::var_os("DOEWOOD_NEAR_DUP_COUNT").is_some();
            let (b, counts, admin, schema) = store_via(count).await.expect("a store to measure");
            println!("store: Postgres, round trips counted: {count}");
            (b, count.then_some(counts), Some((admin, schema)))
        };
    let writer = GraphWriter::new(Arc::clone(&b), SimilarityConfig::default());
    let hash = |i: u64| {
        Blake3::from_slice(blake3::hash(format!("near/{i}").as_bytes()).as_bytes()).unwrap()
    };
    let window = (docs / 10).max(1);
    let (mut spent, mut similar, mut statements, mut trips) = (Duration::ZERO, 0usize, 0u64, 0u64);
    let started = Instant::now();
    println!(
        "docs | graph write ms/doc | statements/doc | round trips/doc | similar_to/doc | candidate query ms (candidates)"
    );
    for i in 0..docs {
        let h = hash(i);
        let facts = DocFacts {
            format: "txt".into(),
            minhash: common::sketch(&common::near_duplicate_text(i, 4)),
            ..DocFacts::default()
        };
        b.put_record(&h, &empty_record(), &StepVersions::new())
            .await
            .unwrap();
        let before = counts.as_ref().map(|c| c.now());
        let t = Instant::now();
        let stats = writer
            .write(&h, &at(&format!("near/{i}.txt")), &facts, "x", 0.8)
            .await
            .unwrap();
        spent += t.elapsed();
        similar += stats.similar;
        if let (Some(c), Some(before)) = (&counts, before) {
            let s = c.since(before);
            statements += s.statements;
            trips += s.round_trips;
        }
        if (i + 1) % window == 0 {
            // A template document, the last one written (every fifth is a distinct one).
            let probe = (0..=i).rev().find(|j| j % 5 != 4).unwrap();
            let (_, bands) = common::sketch(&common::near_duplicate_text(probe, 4)).unwrap();
            let mut best = Duration::MAX;
            let mut found = 0;
            for _ in 0..5 {
                let t = Instant::now();
                found = b
                    .sketch_candidates(&bands, &hash(probe), 100)
                    .await
                    .unwrap()
                    .len();
                best = best.min(t.elapsed());
            }
            let per = |n: f64| n / window as f64;
            println!(
                "{} | {:.2} | {:.1} | {:.1} | {:.1} | {:.2} ({found})",
                i + 1,
                per(spent.as_secs_f64() * 1000.0),
                per(statements as f64),
                per(trips as f64),
                per(similar as f64),
                best.as_secs_f64() * 1000.0,
            );
            (spent, similar, statements, trips) = (Duration::ZERO, 0, 0, 0);
        }
    }
    println!("total: {:.1} s", started.elapsed().as_secs_f64());
    if let Some((admin, schema)) = &pg {
        let probe = (0..docs).rev().find(|j| j % 5 != 4).unwrap();
        let (_, bands) = common::sketch(&common::near_duplicate_text(probe, 4)).unwrap();
        let bands: Vec<i64> = bands
            .iter()
            .map(|b| doewood_model::band_to_i64(*b))
            .collect();
        admin
            .batch_execute(&format!("SET search_path = {schema}"))
            .await
            .unwrap();
        let plan = admin
            .query(
                "EXPLAIN (ANALYZE, BUFFERS) SELECT c.blake3, s.signature, c.shared \
                 FROM (SELECT blake3, count(*) AS shared FROM sketch_bands \
                 WHERE band = ANY($1) AND blake3 <> $2 \
                 GROUP BY blake3 ORDER BY shared DESC, blake3 LIMIT 100) c \
                 JOIN sketches s ON s.blake3 = c.blake3 ORDER BY c.shared DESC, c.blake3",
                &[&bands, &hash(probe).as_bytes().as_slice()],
            )
            .await
            .unwrap();
        for r in plan {
            println!("{}", r.get::<_, String>(0));
        }
    }
    let hashes: Vec<Blake3> = (0..docs).map(hash).collect();
    let edges = common::similar_edges(&*b, &hashes).await;
    let digest = blake3::hash(format!("{edges:?}").as_bytes());
    println!("similar_to edges: {}, digest {digest}", edges.len());
    if let Some((admin, schema)) = pg {
        admin
            .batch_execute(&format!("DROP SCHEMA {schema} CASCADE"))
            .await
            .unwrap();
    }
}

/// A document with many similar ones (issue #126) costs the same round trips as one with a
/// few: its candidates' nodes are read in one statement and its `similar_to` edges written
/// pipelined, where before each candidate took two round trips (a node read, an edge write).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn similar_documents_take_a_fixed_number_of_round_trips() {
    const ROUND_TRIPS_PER_DOC: u64 = 8;
    let Some((b, counts, admin, schema)) = store().await else {
        return;
    };
    let writer = GraphWriter::new(Arc::clone(&b), SimilarityConfig::default());
    let mut seen = Vec::new();
    // Template documents only (every fifth of `near_duplicate_text` is a distinct one).
    for i in (0..60u64).filter(|i| i % 5 != 4) {
        let h = Blake3::from_slice(blake3::hash(format!("trips/near/{i}").as_bytes()).as_bytes())
            .unwrap();
        let facts = DocFacts {
            format: "txt".into(),
            minhash: common::sketch(&common::near_duplicate_text(i, 1)),
            ..DocFacts::default()
        };
        b.put_record(&h, &empty_record(), &StepVersions::new())
            .await
            .unwrap();
        let before = counts.now();
        let stats = writer
            .write(&h, &at(&format!("near/{i}.txt")), &facts, "x", 0.8)
            .await
            .unwrap();
        seen.push((stats.similar, counts.since(before)));
    }
    println!("similar_to edges, then what the write took: {seen:?}");
    // Past the first documents, which open the pool's connections and prepare statements.
    for (similar, s) in &seen[10..] {
        assert!(*similar >= 10, "{similar} similar documents");
        assert!(
            s.round_trips <= ROUND_TRIPS_PER_DOC,
            "{s:?} for {similar} edges"
        );
    }
    admin
        .batch_execute(&format!("DROP SCHEMA {schema} CASCADE"))
        .await
        .unwrap();
}
