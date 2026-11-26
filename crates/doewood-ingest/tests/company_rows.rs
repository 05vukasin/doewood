//! What every document's company props patch costs (issue #129). Each party of each document
//! patches its company's props (`GraphWriter::update_company`). Until issue #129 the patch
//! always changed the row, because a `matches` list got the document's entry, so a supplier on
//! most documents was one row rewritten for every document; now the match is on the `party`
//! edge only, and a known company's patch usually writes nothing.
//!
//! `company_rows` (ignored; a measurement) writes a corpus through `GraphWriter` straight into
//! the store, without files or analysis: `hot`, where one supplier is a party of every
//! document and 20 buyers share the rest, and `spread`, where every company is on at most
//! two documents. It prints, per store and corpus: the graph stage and the company lock per
//! file over the run, the busiest company's props size, and on Postgres the WAL, the `nodes`
//! row versions and dead tuples and the compare-and-set retries (from `pg_stat_statements`,
//! when loaded); on SQLite the bytes the process wrote (`wchar` in `/proc/self/io`). Then it
//! times the company patch alone on the busiest and on a small company, with and without a
//! `matches` entry (as before issue #129, and as now), and on Postgres what reading the last 20
//! `party` edges of the busiest company costs (deriving `matches` on read).
//!
//! `a_stored_matches_list_goes_with_the_next_document` (not ignored) checks that a list an
//! earlier version stored is removed.
//!
//! SQLite always (a store file under the temp dir); Postgres when `DOEWOOD_TEST_POSTGRES_URL`
//! is set (a schema of its own). `DOEWOOD_BENCH_DOCS` documents per corpus (default 10,000),
//! `DOEWOOD_BENCH_JOBS` writers per instance (default 4), `DOEWOOD_BENCH_INSTANCES`
//! `GraphWriter`s, each with its own company lock, as instances sharing one store have
//! (default 1), `DOEWOOD_BENCH_CORPUS` `hot`, `spread` or both (default), and
//! `DOEWOOD_BENCH_STORE` `sqlite`, `postgres` or both (default).
#![allow(
    clippy::cast_precision_loss,
    clippy::cast_possible_wrap,
    clippy::too_many_lines
)]

mod common;

use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use doewood_core::MasterKey;
use doewood_ingest::company::{IdKind, Identifier};
use doewood_ingest::facts::{DocFacts, Party};
use doewood_ingest::{GraphWriter, SimilarityConfig, WriteStats};
use doewood_model::{Backend, Blake3, NodeKind, ObjectLocation, PropsPatch, StepVersions};
use doewood_store::{Standalone, Store};
use serde_json::json;
use uuid::Uuid;

const PG_VAR: &str = "DOEWOOD_TEST_POSTGRES_URL";
/// Patches per company in the patch-alone timing.
const PATCHES: usize = 1000;

fn var(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|v| !v.trim().is_empty())
}

fn num(name: &str, default: usize) -> usize {
    var(name).and_then(|v| v.parse().ok()).unwrap_or(default)
}

fn pib(n: usize) -> Identifier {
    Identifier::parse(IdKind::RsPib, &common::pib(&format!("{n:08}"))).unwrap()
}

fn party(surface: String, id: usize, slot: usize, role: &str) -> Party {
    Party {
        quail_key: format!("c{slot}"),
        surface,
        score: 0.9,
        roles: vec![role.into()],
        legal_form: None,
        jurisdiction: Some("RS".into()),
        ids: vec![pib(id)],
        mentions: vec![(Some(1), 10, 40), (Some(2), 5, 35)],
    }
}

/// The busiest company of the `hot` corpus.
const BUSY: usize = 10_000_000;

/// Document `i` of a corpus: two parties and an own number.
fn facts(hot: bool, i: usize) -> DocFacts {
    let parties = if hot {
        vec![
            party("Glavni Dobavljac d.o.o.".into(), BUSY, 0, "supplier"),
            party(
                format!("Kupac {} d.o.o.", i % 20),
                20_000_000 + i % 20,
                1,
                "buyer",
            ),
        ]
    } else {
        vec![
            party(format!("Firma {i} d.o.o."), 30_000_000 + i, 0, "supplier"),
            party(
                format!("Kupac {} d.o.o.", i / 2),
                40_000_000 + i / 2,
                1,
                "buyer",
            ),
        ]
    };
    DocFacts {
        format: "pdf".into(),
        title: Some(format!("Faktura {i}")),
        kind: Some("invoice".into()),
        body: format!("faktura {i} isporuka robe"),
        parties,
        numbers: vec![format!("{i}/26")],
        ..DocFacts::default()
    }
}

fn hash(corpus: &str, i: usize) -> Blake3 {
    Blake3::from_slice(blake3::hash(format!("{corpus}/{i}").as_bytes()).as_bytes()).unwrap()
}

fn at(corpus: &str, i: usize) -> ObjectLocation {
    ObjectLocation {
        endpoint: "http://objects.test:9000".into(),
        bucket: "docs".into(),
        key: format!("{corpus}/{i}.pdf"),
    }
}

/// Bytes this process passed to `write` so far (`wchar`), on Linux.
fn written() -> Option<u64> {
    let io = std::fs::read_to_string("/proc/self/io").ok()?;
    io.lines()
        .find_map(|l| l.strip_prefix("wchar: "))
        .and_then(|v| v.trim().parse().ok())
}

/// A store under test: SQLite in a file, or Postgres in a fresh schema.
struct Target {
    name: &'static str,
    backend: Arc<dyn Backend>,
    pg: Option<(tokio_postgres::Client, String)>,
    dir: Option<PathBuf>,
}

impl Target {
    fn sqlite() -> Self {
        let dir = std::env::temp_dir().join(format!("doewood-company-rows-{}", Uuid::now_v7()));
        std::fs::create_dir_all(&dir).unwrap();
        let store = Store::open(
            &dir.join(doewood_store::STORE_FILE),
            &MasterKey::from_bytes([29; 32]),
        )
        .unwrap();
        Self {
            name: "sqlite",
            backend: Arc::new(Standalone::new(Arc::new(store))),
            pg: None,
            dir: Some(dir),
        }
    }

    async fn postgres(url: &str) -> Self {
        let (admin, conn) = tokio_postgres::connect(url, tokio_postgres::NoTls)
            .await
            .expect("connect to the test database");
        tokio::spawn(conn);
        let schema = format!("dw_rows_{}", Uuid::now_v7().simple());
        admin
            .batch_execute(&format!("CREATE SCHEMA {schema}"))
            .await
            .unwrap();
        // Optional: the retries are counted only where the extension is loaded.
        let _ = admin
            .batch_execute("CREATE EXTENSION IF NOT EXISTS pg_stat_statements")
            .await;
        let sep = if url.contains('?') { '&' } else { '?' };
        let scoped = format!("{url}{sep}options=-c%20search_path%3D{schema}");
        let pg = doewood_pg::PgStore::connect(&doewood_core::PostgresUrl::parse(&scoped).unwrap())
            .await
            .unwrap();
        Self {
            name: "postgres",
            backend: Arc::new(pg),
            pg: Some((admin, schema)),
            dir: None,
        }
    }

    async fn finish(self) {
        drop(self.backend);
        if let Some((admin, schema)) = self.pg {
            admin
                .batch_execute(&format!("DROP SCHEMA {schema} CASCADE"))
                .await
                .unwrap();
        }
        if let Some(dir) = self.dir {
            // Best effort: on Windows the store's files can still be open (a clone of the
            // backend or the store's close running on another thread), and an open file
            // cannot be removed there. The directory is a temp dir either way.
            let _ = std::fs::remove_dir_all(dir);
        }
    }
}

/// Postgres counters at one moment.
#[derive(Clone, Copy, Debug, Default)]
struct PgSnap {
    wal: i64,
    fpi: i64,
    upd: i64,
    hot_upd: i64,
    dead: i64,
    autovacuums: i64,
    toast_dead: i64,
    cas_calls: i64,
    cas_rows: i64,
}

async fn pg_snap(admin: &tokio_postgres::Client, schema: &str) -> PgSnap {
    // Backends report their table counters when idle, at most once a second.
    tokio::time::sleep(Duration::from_millis(1500)).await;
    let wal = admin
        .query_one(
            "SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), '0/0')::bigint, wal_fpi FROM pg_stat_wal",
            &[],
        )
        .await
        .unwrap();
    let t = admin
        .query_one(
            "SELECT n_tup_upd, n_tup_hot_upd, n_dead_tup, autovacuum_count, \
                 (SELECT coalesce(max(s.n_dead_tup), 0) FROM pg_stat_all_tables s \
                   WHERE s.relid = c.reltoastrelid) \
             FROM pg_stat_user_tables u JOIN pg_class c ON c.oid = u.relid \
             WHERE u.schemaname = $1 AND u.relname = 'nodes'",
            &[&schema],
        )
        .await
        .unwrap();
    let cas = admin
        .query_opt(
            "SELECT coalesce(sum(calls), 0)::bigint, coalesce(sum(rows), 0)::bigint \
             FROM pg_stat_statements WHERE query LIKE 'UPDATE nodes SET props = $3%'",
            &[],
        )
        .await
        .ok()
        .flatten();
    PgSnap {
        wal: wal.get(0),
        fpi: wal.get(1),
        upd: t.get(0),
        hot_upd: t.get(1),
        dead: t.get(2),
        autovacuums: t.get(3),
        toast_dead: t.get(4),
        cas_calls: cas.as_ref().map_or(-1, |r| r.get(0)),
        cas_rows: cas.as_ref().map_or(-1, |r| r.get(1)),
    }
}

/// Size of a company's props: stored (`pg_column_size`, compressed) and as text.
async fn props_size(t: &Target, id: Uuid) -> (Option<i32>, usize, usize) {
    let node = t.backend.node(id).await.unwrap().unwrap();
    let matches = node.props["matches"].as_array().map_or(0, Vec::len);
    let stored = match &t.pg {
        Some((admin, schema)) => Some(
            admin
                .query_one(
                    &format!("SELECT pg_column_size(props) FROM {schema}.nodes WHERE id = $1"),
                    &[&id],
                )
                .await
                .unwrap()
                .get(0),
        ),
        None => None,
    };
    (stored, node.props.to_string().len(), matches)
}

async fn company(b: &dyn Backend, id: usize) -> Uuid {
    b.node_by_key(NodeKind::Company, &pib(id).alias())
        .await
        .unwrap()
        .expect("the company")
        .id
}

/// The corpus written by `instances` writers of `jobs` tasks each; per document, in order:
/// the graph stage and its company lock times.
async fn write_corpus(
    t: &Target,
    corpus: &'static str,
    docs: usize,
    instances: usize,
    jobs: usize,
) -> Vec<(Duration, WriteStats)> {
    let hot = corpus == "hot";
    let writers: Vec<Arc<GraphWriter>> = (0..instances)
        .map(|_| {
            Arc::new(GraphWriter::new(
                Arc::clone(&t.backend),
                SimilarityConfig::default(),
            ))
        })
        .collect();
    let lanes = instances * jobs;
    let mut tasks = Vec::new();
    for lane in 0..lanes {
        let writer = Arc::clone(&writers[lane % instances]);
        let b = Arc::clone(&t.backend);
        tasks.push(tokio::spawn(async move {
            let mut out = Vec::new();
            let record = doewood_model::canonical::to_canonical(&json!({})).unwrap();
            for i in (lane..docs).step_by(lanes) {
                let h = hash(corpus, i);
                b.put_record(&h, &record, &StepVersions::new())
                    .await
                    .unwrap();
                let f = facts(hot, i);
                let start = Instant::now();
                let stats = writer
                    .write(&h, &at(corpus, i), &f, "x.pdf", 0.8)
                    .await
                    .unwrap();
                out.push((i, start.elapsed(), stats));
            }
            out
        }));
    }
    let mut all = Vec::new();
    for task in tasks {
        all.extend(task.await.unwrap());
    }
    all.sort_by_key(|(i, ..)| *i);
    all.into_iter().map(|(_, d, s)| (d, s)).collect()
}

fn ms(d: Duration) -> f64 {
    d.as_secs_f64() * 1000.0
}

/// The patch `update_company` sends for a party of document `doc`, with or without its
/// `matches` entry (the only key that changes for a company already seen).
fn company_patch(name: &str, id: usize, doc: &str, with_matches: bool) -> PropsPatch {
    let name_key = name.to_lowercase();
    let mut patch = PropsPatch::new()
        .add_to_set(&["names"], json!(name), 20)
        .set_if_absent(&["name"], json!(name))
        .set_if_absent(&["key"], json!(name_key))
        .set_if_absent(&["legal_form"], json!("d.o.o."))
        .set(&["jurisdiction"], json!("RS"))
        .add_to_set(&["quail_keys"], json!("c0"), 20)
        .add_to_set(&["ids", "rs_pib"], json!(pib(id).value), 10);
    if with_matches {
        patch = patch.push_capped(
            &["matches"],
            json!({
                "step": "identifier",
                "alias": pib(id).alias(),
                "confidence": 0.99,
                "document": doc,
                "conflicts": [],
            }),
            &["document"],
            20,
        );
    }
    patch
}

/// `PATCHES` company patches one after another: time per patch and bytes written per patch
/// (Postgres WAL, or what this process wrote for SQLite).
async fn patch_alone(t: &Target, id: Uuid, name: &str, pib_n: usize, with_matches: bool) -> String {
    let wal = async || match &t.pg {
        Some((admin, _)) => admin
            .query_one(
                "SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), '0/0')::bigint",
                &[],
            )
            .await
            .unwrap()
            .get::<_, i64>(0)
            .unsigned_abs(),
        None => written().unwrap_or(0),
    };
    let before = wal().await;
    let start = Instant::now();
    for k in 0..PATCHES {
        let doc = blake3::hash(format!("alone/{name}/{with_matches}/{k}").as_bytes())
            .to_hex()
            .to_string();
        t.backend
            .update_props(id, &company_patch(name, pib_n, &doc, with_matches))
            .await
            .unwrap();
    }
    let per = start.elapsed() / u32::try_from(PATCHES).unwrap();
    let bytes = (wal().await - before) / PATCHES as u64;
    format!(
        "{:.3} ms, {bytes} bytes {} per patch",
        ms(per),
        if t.pg.is_some() { "WAL" } else { "written" }
    )
}

/// What deriving a company's `matches` from its `party` edges on read would cost: the last 20
/// live party edges (newest id first; ids are UUIDv7), timed over 200 reads with the indexes as
/// they are, then with an index on `(dst, kind, id)`, which is dropped again.
async fn last_matches(admin: &tokio_postgres::Client, schema: &str, company: Uuid) -> String {
    let query = format!(
        "SELECT e.props, e.evidence FROM {schema}.edges e \
         WHERE e.dst = $1 AND e.kind = 'party' AND e.state NOT IN ('retracted', 'superseded') \
         ORDER BY e.id DESC LIMIT 20"
    );
    let time = async || {
        let stmt = admin.prepare(&query).await.unwrap();
        admin.query(&stmt, &[&company]).await.unwrap();
        let start = Instant::now();
        for _ in 0..200 {
            admin.query(&stmt, &[&company]).await.unwrap();
        }
        ms(start.elapsed() / 200)
    };
    let now = time().await;
    admin
        .batch_execute(&format!(
            "CREATE INDEX edges_by_dst_id ON {schema}.edges (dst, kind, id); ANALYZE {schema}.edges"
        ))
        .await
        .unwrap();
    let indexed = time().await;
    admin
        .batch_execute(&format!("DROP INDEX {schema}.edges_by_dst_id"))
        .await
        .unwrap();
    format!("{now:.3} ms (indexes as they are), {indexed:.3} ms with (dst, kind, id)")
}

async fn measure(t: &Target, corpus: &'static str, docs: usize, instances: usize, jobs: usize) {
    let pg_before = match &t.pg {
        Some((admin, schema)) => {
            let _ = admin
                .batch_execute("SELECT pg_stat_statements_reset()")
                .await;
            Some(pg_snap(admin, schema).await)
        }
        None => None,
    };
    let io_before = written();
    let start = Instant::now();
    let per_doc = write_corpus(t, corpus, docs, instances, jobs).await;
    let wall = start.elapsed();
    let io_after = written();
    println!(
        "\n== {} / {corpus}: {docs} documents, {instances} instance(s) x {jobs} writers: {:.0} docs/s",
        t.name,
        docs as f64 / wall.as_secs_f64()
    );
    println!("tenth | graph stage ms/doc | lock wait ms/doc | lock held ms/doc");
    let tenth = docs.div_ceil(10);
    for (k, part) in per_doc.chunks(tenth).enumerate() {
        let n = part.len() as f64;
        let sum = |f: fn(&(Duration, WriteStats)) -> Duration| {
            part.iter().map(f).sum::<Duration>().as_secs_f64() * 1000.0 / n
        };
        println!(
            "{:>5} | {:.3} | {:.3} | {:.3}",
            k + 1,
            sum(|p| p.0),
            sum(|p| p.1.company_lock_wait),
            sum(|p| p.1.company_lock_hold)
        );
    }
    let busy = if corpus == "hot" {
        company(t.backend.as_ref(), BUSY).await
    } else {
        company(t.backend.as_ref(), 40_000_000).await
    };
    let (stored, text, matches) = props_size(t, busy).await;
    println!(
        "busiest company props: {text} bytes as text, {} stored, {matches} matches",
        stored.map_or_else(|| "-".into(), |s| s.to_string())
    );
    if let (Some(b), Some(a)) = (io_before, io_after)
        && t.pg.is_none()
    {
        println!("process wrote {} bytes per document", (a - b) / docs as u64);
    }
    if let (Some(before), Some((admin, schema))) = (pg_before, &t.pg) {
        let after = pg_snap(admin, schema).await;
        let d = |f: fn(&PgSnap) -> i64| f(&after) - f(&before);
        println!(
            "WAL {} bytes per document ({} full-page images); nodes: {} row versions \
             ({} HOT), {:.2} per document; {} dead tuples at the end ({} in TOAST), \
             {} autovacuums",
            d(|s| s.wal) / docs as i64,
            d(|s| s.fpi),
            d(|s| s.upd),
            d(|s| s.hot_upd),
            d(|s| s.upd) as f64 / docs as f64,
            after.dead,
            after.toast_dead,
            d(|s| s.autovacuums)
        );
        if after.cas_calls >= 0 {
            println!(
                "props compare-and-set: {} writes, {} retried ({} written)",
                after.cas_calls,
                after.cas_calls - after.cas_rows,
                after.cas_rows
            );
        }
        let sizes = admin
            .query_one(
                &format!(
                    "SELECT pg_relation_size('{schema}.nodes'), \
                     coalesce(pg_relation_size(reltoastrelid), 0), \
                     pg_total_relation_size('{schema}.nodes') \
                     FROM pg_class WHERE oid = '{schema}.nodes'::regclass"
                ),
                &[],
            )
            .await
            .unwrap();
        println!(
            "nodes: heap {} KiB, TOAST {} KiB, with indexes {} KiB",
            sizes.get::<_, i64>(0) / 1024,
            sizes.get::<_, i64>(1) / 1024,
            sizes.get::<_, i64>(2) / 1024
        );
        println!(
            "the busiest company's last 20 party edges, read: {}",
            last_matches(admin, schema, busy).await
        );
    }
    let (name, pib_n) = if corpus == "hot" {
        ("Glavni Dobavljac d.o.o.", BUSY)
    } else {
        ("Kupac 0 d.o.o.", 40_000_000)
    };
    for with_matches in [true, false] {
        println!(
            "patch alone, {}: {}",
            if with_matches {
                "with a `matches` entry (before #129)"
            } else {
                "without (now)"
            },
            patch_alone(t, busy, name, pib_n, with_matches).await
        );
    }
}

/// Measures the company props patches of a busy supplier against many small ones. Prints only.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "a measurement (issue #129); run with --ignored --nocapture"]
async fn company_rows() {
    let docs = num("DOEWOOD_BENCH_DOCS", 10_000);
    let jobs = num("DOEWOOD_BENCH_JOBS", 4);
    let instances = num("DOEWOOD_BENCH_INSTANCES", 1);
    let corpora: Vec<&'static str> = match var("DOEWOOD_BENCH_CORPUS").as_deref() {
        Some("hot") => vec!["hot"],
        Some("spread") => vec!["spread"],
        _ => vec!["hot", "spread"],
    };
    let which = var("DOEWOOD_BENCH_STORE");
    for corpus in corpora {
        if which.as_deref() != Some("postgres") {
            let t = Target::sqlite();
            measure(&t, corpus, docs, instances, jobs).await;
            t.finish().await;
        }
        if which.as_deref() != Some("sqlite") {
            match var(PG_VAR) {
                Some(url) => {
                    let t = Target::postgres(&url).await;
                    measure(&t, corpus, docs, instances, jobs).await;
                    t.finish().await;
                }
                None => println!("{PG_VAR} is not set: Postgres not measured"),
            }
        }
    }
}

/// A `matches` list an earlier version stored on a company goes with the company's next
/// document, and none is written again (issue #129). SQLite, and Postgres when
/// `DOEWOOD_TEST_POSTGRES_URL` is set.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_stored_matches_list_goes_with_the_next_document() {
    let mut targets = vec![Target::sqlite()];
    if let Some(url) = var(PG_VAR) {
        targets.push(Target::postgres(&url).await);
    }
    for t in targets {
        let b = &t.backend;
        let writer = GraphWriter::new(Arc::clone(b), SimilarityConfig::default());
        let record = doewood_model::canonical::to_canonical(&json!({})).unwrap();
        for i in 0..2 {
            let h = hash("strip", i);
            b.put_record(&h, &record, &StepVersions::new())
                .await
                .unwrap();
            writer
                .write(&h, &at("strip", i), &facts(true, i), "x.pdf", 0.8)
                .await
                .unwrap();
            let busy = company(b.as_ref(), BUSY).await;
            let props = b.node(busy).await.unwrap().unwrap().props;
            assert!(props.get("matches").is_none(), "{}: {props}", t.name);
            assert_eq!(props["ids"]["rs_pib"][0], pib(BUSY).value, "{}", t.name);
            if i == 0 {
                // As an earlier version left it.
                b.update_props(
                    busy,
                    &PropsPatch::new().set(&["matches"], json!([{"document": "old"}])),
                )
                .await
                .unwrap();
            }
        }
        t.finish().await;
    }
}
