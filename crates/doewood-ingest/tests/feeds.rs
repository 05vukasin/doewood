//! The registry delta feeds end to end (ADR 0002 section 6, issue #49), against mock
//! registries on 127.0.0.1 only: never a real registry, never the internet. Every company,
//! identifier, file and event here is invented; LEIs and MBs carry valid check digits only.
//!
//! Always against the SQLite backend, and against Postgres too when
//! `DOEWOOD_TEST_POSTGRES_URL` is set (in a schema of its own).

#![allow(clippy::too_many_lines)]

use std::collections::BTreeMap;
use std::io::Write as _;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use doewood_core::{InstanceId, MasterKey};
use doewood_enrich::{CompanyName, IdKind, Identifier};
use doewood_ingest::{
    Enrichment, EnrichmentOptions, EnrichmentParts, Feed, FeedOptions, FeedTemplates, Templates,
};
use doewood_model::{Backend, Blake3, Edge, EdgeKind, EdgeSource, NodeKind, StepVersions};
use doewood_registry::favicon::{GuardOptions, GuardedClient, PublicOnly, SystemDns};
use doewood_registry::feeds::companies_house::ChStreamConfig;
use doewood_registry::feeds::gleif::GleifFeedConfig;
use doewood_store::{
    ApiKey, EnrichmentSettingsPatch, FeedSwitchesPatch, KeyChange, RegistrySwitchesPatch,
    Standalone, Store,
};
use serde_json::{Map, Value, json};
use time::{Date, OffsetDateTime};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::watch;
use uuid::Uuid;

const PG_VAR: &str = "DOEWOOD_TEST_POSTGRES_URL";

// ---------------------------------------------------------------------------------------------
// Identifiers with valid check digits

/// An invented LEI: `FAKE00`, `tag` padded to 12, and the ISO 17442 check digits.
fn lei(tag: &str) -> String {
    let head = format!("FAKE00{tag:0>12}");
    let mut r: u32 = 0;
    for c in format!("{head}00").chars() {
        let d = c.to_digit(36).unwrap();
        r = if d >= 10 {
            (r * 100 + d) % 97
        } else {
            (r * 10 + d) % 97
        };
    }
    format!("{head}{:02}", 98 - r)
}

/// An invented MB: seven digits and the MOD 11 check digit.
fn mb(head: &str) -> String {
    let sum: u32 = head
        .bytes()
        .zip([2, 7, 6, 5, 4, 3, 2])
        .map(|(b, w)| u32::from(b - b'0') * w)
        .sum();
    let k = match 11 - sum % 11 {
        10 | 11 => 0,
        k => k,
    };
    format!("{head}{k}")
}

// ---------------------------------------------------------------------------------------------
// The mock server

#[derive(Clone, Debug)]
struct Reply {
    status: u16,
    content_type: &'static str,
    body: Vec<u8>,
}

impl Reply {
    fn json(v: &Value) -> Self {
        Self::bytes("application/json", serde_json::to_vec(v).unwrap())
    }

    fn bytes(content_type: &'static str, body: Vec<u8>) -> Self {
        Self {
            status: 200,
            content_type,
            body,
        }
    }

    fn status(status: u16) -> Self {
        Self {
            status,
            content_type: "application/json",
            body: br#"{"errors":[{"status":"404"}]}"#.to_vec(),
        }
    }
}

type Handler = dyn Fn(&str) -> Reply + Send + Sync;

/// An HTTP/1.1 server on 127.0.0.1 answering `handler(target)`, recording every target and
/// whether it carried an `Authorization` header.
struct Mock {
    port: u16,
    seen: Arc<Mutex<Vec<(String, bool)>>>,
}

impl Mock {
    async fn start(handler: impl Fn(&str) -> Reply + Send + Sync + 'static) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let seen = Arc::new(Mutex::new(Vec::new()));
        let handler: Arc<Handler> = Arc::new(handler);
        let log = Arc::clone(&seen);
        tokio::spawn(async move {
            loop {
                let Ok((mut sock, _)) = listener.accept().await else {
                    return;
                };
                let handler = Arc::clone(&handler);
                let log = Arc::clone(&log);
                tokio::spawn(async move {
                    let mut buf = Vec::new();
                    let mut chunk = [0u8; 4096];
                    while !buf.windows(4).any(|w| w == b"\r\n\r\n") {
                        match sock.read(&mut chunk).await {
                            Ok(0) | Err(_) => return,
                            Ok(n) => buf.extend_from_slice(&chunk[..n]),
                        }
                    }
                    let head = String::from_utf8_lossy(&buf).into_owned();
                    let target = head
                        .lines()
                        .next()
                        .and_then(|l| l.split(' ').nth(1))
                        .unwrap_or("")
                        .to_owned();
                    let auth = head
                        .lines()
                        .any(|l| l.to_ascii_lowercase().starts_with("authorization:"));
                    let reply = handler(&target);
                    log.lock().unwrap().push((target, auth));
                    let out = format!(
                        "HTTP/1.1 {} Mock\r\nConnection: close\r\nContent-Type: {}\r\n\
                         Content-Length: {}\r\n\r\n",
                        reply.status,
                        reply.content_type,
                        reply.body.len()
                    );
                    let _ = sock.write_all(out.as_bytes()).await;
                    let _ = sock.write_all(&reply.body).await;
                    let _ = sock.shutdown().await;
                });
            }
        });
        Self { port, seen }
    }

    fn base(&self) -> String {
        format!("http://127.0.0.1:{}", self.port)
    }

    fn seen(&self) -> Vec<String> {
        self.seen
            .lock()
            .unwrap()
            .iter()
            .map(|(t, _)| t.clone())
            .collect()
    }

    fn count(&self, part: &str) -> usize {
        self.seen().iter().filter(|s| s.contains(part)).count()
    }

    fn authorized(&self, part: &str) -> bool {
        self.seen
            .lock()
            .unwrap()
            .iter()
            .filter(|(t, _)| t.contains(part))
            .all(|(_, a)| *a)
    }
}

// ---------------------------------------------------------------------------------------------
// The job, settings and backends

fn templates(mock: &Mock) -> (Templates, FeedTemplates) {
    let base = mock.base();
    let mut t = Templates::default();
    t.gleif.base_url = format!("{base}/gleif/");
    t.edgar.data_url = format!("{base}/edgar/data/");
    t.edgar.www_url = format!("{base}/edgar/www/");
    t.apr.url = format!("{base}/apr/companies");
    t.companies_house.base_url = format!("{base}/ch/");
    for (per_minute, burst, retries) in [
        (
            &mut t.gleif.per_minute,
            &mut t.gleif.burst,
            &mut t.gleif.max_retries,
        ),
        (
            &mut t.edgar.per_minute,
            &mut t.edgar.burst,
            &mut t.edgar.max_retries,
        ),
        (
            &mut t.apr.per_minute,
            &mut t.apr.burst,
            &mut t.apr.max_retries,
        ),
        (
            &mut t.companies_house.per_minute,
            &mut t.companies_house.burst,
            &mut t.companies_house.max_retries,
        ),
    ] {
        *per_minute = 60_000;
        *burst = 1000;
        *retries = 0;
    }
    let feeds = FeedTemplates {
        gleif: GleifFeedConfig {
            url: format!("{base}/goldencopy/latest"),
            ..GleifFeedConfig::default()
        },
        companies_house: ChStreamConfig {
            url: format!("{base}/stream/"),
            idle: Duration::from_secs(5),
            ..ChStreamConfig::default()
        },
        ..FeedTemplates::default()
    };
    (t, feeds)
}

fn job(backend: &Arc<dyn Backend>, store: &Arc<Store>, mock: &Mock, instance: &str) -> Enrichment {
    let (_tx, rx) = watch::channel(Some(Arc::clone(backend)));
    let (templates, feed_templates) = templates(mock);
    Enrichment::new(EnrichmentParts {
        instance: InstanceId::new(instance).unwrap(),
        backend: rx,
        settings: EnrichmentParts::store_settings(Arc::clone(store)),
        templates,
        feed_templates,
        // No website is ever fetched here: no company has a domain.
        web: Arc::new(|_| {
            GuardedClient::new(
                &GuardOptions::default(),
                Arc::new(SystemDns),
                Arc::new(PublicOnly),
            )
        }),
        options: EnrichmentOptions {
            concurrency: 2,
            interval: Duration::from_hours(1),
            lease: Duration::from_mins(1),
            drain: Duration::from_secs(1),
            feeds: FeedOptions {
                gleif_every: Duration::ZERO,
                edgar_every: Duration::ZERO,
                apr_every: Duration::ZERO,
                apr_overdue_every: Duration::ZERO,
                retry: Duration::from_hours(1),
                stream_window: Duration::from_secs(3),
                stream_backoff: Duration::from_millis(10),
                ..FeedOptions::default()
            },
            ..EnrichmentOptions::default()
        },
        on_change: Arc::new(|_| {}),
    })
}

async fn pass(job: &Enrichment, backend: &Arc<dyn Backend>) -> u64 {
    let (_tx, rx) = watch::channel(false);
    job.pass(backend, &rx).await
}

/// Only `registry` is asked (and its feed on, the default); Companies House with keys.
fn only(store: &Store, registry: &str) {
    let on = |r: &str| Some(r == registry);
    let ch = registry == "companies_house";
    store
        .set_enrichment_settings(&EnrichmentSettingsPatch {
            registries: RegistrySwitchesPatch {
                gleif: on("gleif"),
                vies: Some(false),
                wikidata: Some(false),
                edgar: on("edgar"),
                apr: on("apr"),
                companies_house: on("companies_house"),
            },
            feeds: FeedSwitchesPatch {
                companies_house: Some(ch),
                ..FeedSwitchesPatch::default()
            },
            companies_house_key: if ch {
                KeyChange::Set(ApiKey::new("test-key").unwrap())
            } else {
                KeyChange::Keep
            },
            companies_house_stream_key: if ch {
                KeyChange::Set(ApiKey::new("test-stream-key").unwrap())
            } else {
                KeyChange::Keep
            },
            ..EnrichmentSettingsPatch::default()
        })
        .unwrap();
}

struct Backends {
    list: Vec<(&'static str, Arc<dyn Backend>, Arc<Store>)>,
    cleanup: Option<(tokio_postgres::Client, String)>,
}

fn store() -> Arc<Store> {
    Arc::new(Store::open_in_memory(&MasterKey::from_bytes([4; 32])).unwrap())
}

impl Backends {
    async fn all() -> Self {
        let sqlite = store();
        let mut list: Vec<(&'static str, Arc<dyn Backend>, Arc<Store>)> = vec![(
            "sqlite",
            Arc::new(Standalone::new(Arc::clone(&sqlite))),
            sqlite,
        )];
        let mut cleanup = None;
        if let Some((pg, admin, schema)) = postgres().await {
            list.push(("postgres", pg, store()));
            cleanup = Some((admin, schema));
        }
        Self { list, cleanup }
    }

    async fn drop_schema(self) {
        if let Some((admin, schema)) = self.cleanup {
            admin
                .batch_execute(&format!("DROP SCHEMA {schema} CASCADE"))
                .await
                .unwrap();
        }
    }
}

async fn postgres() -> Option<(Arc<dyn Backend>, tokio_postgres::Client, String)> {
    let Some(url) = std::env::var(PG_VAR).ok().filter(|v| !v.trim().is_empty()) else {
        eprintln!("skipping: {PG_VAR} is not set; the Postgres variant did not run");
        return None;
    };
    let (admin, conn) = tokio_postgres::connect(&url, tokio_postgres::NoTls)
        .await
        .expect("connect to the test database");
    tokio::spawn(conn);
    let schema = format!("dw_feeds_{}", Uuid::now_v7().simple());
    admin
        .batch_execute(&format!("CREATE SCHEMA {schema}"))
        .await
        .unwrap();
    admin
        .batch_execute(&format!(
            "BEGIN; SELECT pg_advisory_xact_lock({}); \
             DO $$ BEGIN \
                 IF EXISTS (SELECT 1 FROM pg_available_extensions WHERE name = 'vector') \
                 THEN CREATE EXTENSION IF NOT EXISTS vector SCHEMA public; END IF; \
             END $$; COMMIT",
            doewood_pg::MIGRATION_LOCK
        ))
        .await
        .unwrap();
    let sep = if url.contains('?') { '&' } else { '?' };
    let scoped = format!("{url}{sep}options=-c%20search_path%3D{schema}");
    let url = doewood_core::PostgresUrl::parse(&scoped).unwrap();
    let pg = doewood_pg::PgStore::connect(&url)
        .await
        .unwrap_or_else(|e| panic!("connect {schema}: {e}"));
    Some((Arc::new(pg), admin, schema))
}

static DOCS: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(1);

/// A company as ingest leaves it, with `aliases` and two documents.
async fn company(b: &Arc<dyn Backend>, name: &str, country: &str, aliases: &[String]) -> Uuid {
    let parsed = CompanyName::parse(name).unwrap();
    let id = b
        .upsert_node(
            NodeKind::Company,
            &aliases[0],
            &json!({"name": name, "key": parsed.key, "legal_form": parsed.legal_form,
                "jurisdiction": country}),
        )
        .await
        .unwrap();
    let props = json!({"step": "test"});
    for a in aliases {
        b.claim_alias(NodeKind::Company, a, id, &props)
            .await
            .unwrap();
    }
    for _ in 0..2 {
        let n = DOCS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let mut bytes = [0u8; 32];
        bytes[..4].copy_from_slice(&n.to_be_bytes());
        bytes[31] = 0xfe;
        let hash = Blake3(bytes);
        b.put_record(
            &hash,
            &doewood_model::canonical::to_canonical(&json!({"format": "pdf"})).unwrap(),
            &StepVersions::new(),
        )
        .await
        .unwrap();
        let doc = b
            .upsert_node(NodeKind::Document, &hash.to_hex(), &json!({}))
            .await
            .unwrap();
        b.upsert_edge(&Edge::new(doc, id, EdgeKind::Party, EdgeSource::Document))
            .await
            .unwrap();
    }
    id
}

async fn props(b: &Arc<dyn Backend>, id: Uuid) -> Value {
    b.node(id).await.unwrap().unwrap().props
}

/// Whether `registry` is due for `company` now.
async fn due(b: &Arc<dyn Backend>, company: Uuid, registry: &str) -> bool {
    let e = b.enrichment(company).await.unwrap().unwrap();
    e.next_due
        .get(registry)
        // Postgres rounds its times to the second (up, at times).
        .is_some_and(|t| *t <= OffsetDateTime::now_utc().unix_timestamp() + 1)
}

/// Seeds a feed's cursor, as an earlier run of another instance would have left it.
async fn seed(b: &Arc<dyn Backend>, feed: &str, cursor: &str) {
    let seeder = InstanceId::new("seeder").unwrap();
    assert!(
        b.lease_feed(feed, &seeder, Duration::from_mins(1))
            .await
            .unwrap()
    );
    assert!(b.save_feed(feed, &seeder, cursor, 0).await.unwrap());
    assert!(b.release_feed(feed, &seeder).await.unwrap());
}

/// A zip holding one CSV entry.
fn zipped(name: &str, csv: &str) -> Vec<u8> {
    let mut z = zip::ZipWriter::new(std::io::Cursor::new(Vec::new()));
    let opts =
        zip::write::SimpleFileOptions::default().compression_method(zip::CompressionMethod::Stored);
    z.start_file(name, opts).unwrap();
    z.write_all(csv.as_bytes()).unwrap();
    z.finish().unwrap().into_inner()
}

/// A GLEIF LEI record answer.
fn gleif_record(lei: &str, name: &str) -> Value {
    json!({"data": {
        "type": "lei-records",
        "id": lei,
        "attributes": {
            "lei": lei,
            "entity": {
                "legalName": {"name": name, "language": "en"},
                "legalAddress": {"country": "GB", "city": "Exampletown"},
                "jurisdiction": "GB",
                "category": "GENERAL",
                "legalForm": {"id": "XXXX"},
                "status": "ACTIVE"
            },
            "registration": {"status": "ISSUED"}
        }
    }})
}

// ---------------------------------------------------------------------------------------------
// GLEIF

#[derive(Default)]
struct GleifState {
    published: String,
    names: BTreeMap<String, String>,
    down: bool,
}

/// The golden copy publication and its delta files, and the LEI records.
fn gleif_mock(
    state: &Arc<Mutex<GleifState>>,
    base: &str,
    target: &str,
    files: &(Vec<u8>, Vec<u8>),
) -> Reply {
    let s = state.lock().unwrap();
    if target == "/goldencopy/latest" {
        if s.down {
            return Reply::status(503);
        }
        let set = |name: &str| {
            let mut deltas = Map::new();
            for span in ["IntraDay", "LastDay", "LastWeek", "LastMonth"] {
                deltas.insert(
                    span.to_owned(),
                    json!({"csv": {"type": name, "format": "csv", "delta_type": span,
                        "url": format!("{base}/goldencopy/files/{name}-{span}.csv.zip")},
                        "json": {"url": "https://elsewhere.invalid/never.zip"}}),
                );
            }
            json!({"type": name, "publish_date": s.published, "delta_files": deltas})
        };
        return Reply::json(&json!({"data": {
            "publish_date": s.published, "lei2": set("lei2"), "rr": set("rr")}}));
    }
    if let Some(f) = target.strip_prefix("/goldencopy/files/") {
        return Reply::bytes(
            "application/zip",
            if f.starts_with("lei2-") {
                files.0.clone()
            } else {
                files.1.clone()
            },
        );
    }
    if let Some(rest) = target.strip_prefix("/gleif/lei-records/") {
        if rest.contains('/') {
            return Reply::status(404);
        }
        return s.names.get(rest).map_or_else(
            || Reply::status(404),
            |n| Reply::json(&gleif_record(rest, n)),
        );
    }
    Reply::status(404)
}

/// A delta publication names one company's LEI (LEI-CDF) and another's as a child (RR-CDF):
/// both are made due for GLEIF with their cached answers dropped, the next pass asks GLEIF
/// again and writes what it says now. LEIs the graph does not have, and invalid ones, are
/// left alone. The cursor is the publication: nothing is applied twice.
#[tokio::test(flavor = "multi_thread")]
async fn gleif_deltas_make_known_companies_due() {
    let (la, lb, lp, lx) = (
        lei("FEEDALPHA"),
        lei("FEEDBETA"),
        lei("FEEDPARENT"),
        lei("FEEDNOBODY"),
    );
    let lei2 = format!(
        "\"LEI\",\"Entity.LegalName\"\n\"{la}\",\"Feed Alpha, \"\"Renamed\"\" Ltd\"\n\
         \"{lx}\",\"Nobody\"\n\"NOT-AN-LEI\",\"Broken\"\n"
    );
    let rr = format!(
        "\"Relationship.StartNode.NodeID\",\"Relationship.StartNode.NodeIDType\",\
         \"Relationship.EndNode.NodeID\",\"Relationship.EndNode.NodeIDType\",\
         \"Relationship.RelationshipType\"\n\
         \"{lb}\",\"LEI\",\"{lp}\",\"LEI\",\"IS_DIRECTLY_CONSOLIDATED_BY\"\n"
    );
    let files = (
        zipped("20261125-0800-gleif-goldencopy-lei2-intra-day.csv", &lei2),
        zipped("20261125-0800-gleif-goldencopy-rr-intra-day.csv", &rr),
    );
    let state = Arc::new(Mutex::new(GleifState::default()));
    let st = Arc::clone(&state);
    let base = Arc::new(Mutex::new(String::new()));
    let b2 = Arc::clone(&base);
    let mock = Mock::start(move |t| gleif_mock(&st, &b2.lock().unwrap(), t, &files)).await;
    *base.lock().unwrap() = mock.base();
    let backends = Backends::all().await;
    for (name, b, store) in &backends.list {
        {
            let mut s = state.lock().unwrap();
            s.published = "2026-11-25 00:00:00".to_owned();
            s.names = [
                (la.clone(), "FEED ALPHA LTD".to_owned()),
                (lb.clone(), "FEED BETA LTD".to_owned()),
            ]
            .into();
            s.down = false;
        }
        only(store, "gleif");
        let a = company(b, "Feed Alpha Ltd", "GB", &[format!("id:lei:{la}")]).await;
        let c = company(b, "Feed Beta Ltd", "GB", &[format!("id:lei:{lb}")]).await;
        let job = job(b, store, &mock, "feeds-gleif");
        assert_eq!(pass(&job, b).await, 2, "{name}");
        assert_eq!(props(b, a).await["legal_name"], "FEED ALPHA LTD", "{name}");
        let asked = mock.count(&format!("/gleif/lei-records/{la}"));

        // The first run starts from the latest publication: nothing to apply.
        let files_before = mock.count("/goldencopy/files/");
        assert!(!job.run_feed(Feed::Gleif, b).await, "{name}: released");
        let s = job.feed_stats(Feed::Gleif);
        assert_eq!(
            (s.enabled, s.runs, s.companies_due, s.failures),
            (true, 1, 0, 0),
            "{name} {s:?}"
        );
        let state0 = b.feed_state("gleif").await.unwrap().unwrap();
        assert_eq!(state0.cursor, "1795564800", "{name}");
        assert_eq!(state0.owner, None, "{name}: released");
        assert_eq!(mock.count("/goldencopy/files/"), files_before, "{name}");
        assert_eq!(pass(&job, b).await, 0, "{name}: nothing due");

        // Eight hours later: the intra-day delta files.
        {
            let mut s = state.lock().unwrap();
            s.published = "2026-11-25 08:00:00".to_owned();
            s.names
                .insert(la.clone(), "FEED ALPHA RENAMED LTD".to_owned());
        }
        job.run_feed(Feed::Gleif, b).await;
        let s = job.feed_stats(Feed::Gleif);
        assert_eq!(
            (s.runs, s.companies_due, s.failures),
            (2, 2, 0),
            "{name} {s:?}"
        );
        assert_eq!(
            s.records, 4,
            "{name}: three LEI records and one relationship"
        );
        assert_eq!(s.position, 1_795_593_600, "{name}");
        assert_eq!(mock.count("lei2-IntraDay"), 1 + files_before / 2, "{name}");
        assert_eq!(
            mock.count("never.zip"),
            0,
            "{name}: CSV files only, same host only"
        );
        assert!(
            due(b, a, "gleif").await && due(b, c, "gleif").await,
            "{name}"
        );
        assert!(
            !due(b, a, "edgar").await,
            "{name}: other registries untouched"
        );
        assert_eq!(
            b.cache_get("gleif", &format!("lei-records/{la}"))
                .await
                .unwrap(),
            None,
            "{name}: the cached answer is dropped"
        );
        assert!(
            b.companies_by_alias(&[format!("id:lei:{lx}")])
                .await
                .unwrap()
                .is_empty(),
            "{name}: unknown LEIs create nothing"
        );
        assert_eq!(
            b.feed_state("gleif").await.unwrap().unwrap().cursor,
            "1795593600",
            "{name}"
        );

        // The next pass asks GLEIF again and writes what it says now.
        assert_eq!(pass(&job, b).await, 2, "{name}");
        assert!(
            mock.count(&format!("/gleif/lei-records/{la}")) > asked,
            "{name}"
        );
        assert_eq!(
            props(b, a).await["legal_name"],
            "FEED ALPHA RENAMED LTD",
            "{name}"
        );
        assert!(!due(b, a, "gleif").await, "{name}");

        // The same publication again: nothing new.
        let files_now = mock.count("/goldencopy/files/");
        job.run_feed(Feed::Gleif, b).await;
        assert_eq!(mock.count("/goldencopy/files/"), files_now, "{name}");
        assert_eq!(job.feed_stats(Feed::Gleif).companies_due, 2, "{name}");
    }
    backends.drop_schema().await;
}

/// One instance at a time reads a feed (another's lease keeps it out); a failed run is a
/// warning, keeps the cursor and is tried again after the retry interval; a feed switched
/// off is never read.
#[tokio::test(flavor = "multi_thread")]
async fn a_feed_is_read_by_one_instance_and_fails_soft() {
    let state = Arc::new(Mutex::new(GleifState {
        published: "2026-11-25 16:00:00".to_owned(),
        ..GleifState::default()
    }));
    let st = Arc::clone(&state);
    let files = (Vec::new(), Vec::new());
    let mock = Mock::start(move |t| gleif_mock(&st, "", t, &files)).await;
    let backends = Backends::all().await;
    for (name, b, store) in &backends.list {
        state.lock().unwrap().down = false;
        only(store, "gleif");
        let one = job(b, store, &mock, "feeds-one");
        let two = job(b, store, &mock, "feeds-two");
        let before = mock.count("/goldencopy/latest");

        // Another instance holds the lease: this one does not read.
        let other = InstanceId::new("feeds-other").unwrap();
        assert!(
            b.lease_feed("gleif", &other, Duration::from_mins(1))
                .await
                .unwrap()
        );
        assert!(!one.run_feed(Feed::Gleif, b).await, "{name}");
        assert_eq!(mock.count("/goldencopy/latest"), before, "{name}");
        assert_eq!(one.feed_stats(Feed::Gleif).runs, 0, "{name}");
        assert!(b.release_feed("gleif", &other).await.unwrap());

        // A failed run: counted, the cursor kept, tried again after the retry interval (an
        // hour here) by whichever instance looks then.
        seed(b, "gleif", "1795593600").await;
        state.lock().unwrap().down = true;
        one.run_feed(Feed::Gleif, b).await;
        let s = one.feed_stats(Feed::Gleif);
        assert_eq!((s.runs, s.failures), (0, 1), "{name} {s:?}");
        let st = b.feed_state("gleif").await.unwrap().unwrap();
        assert_eq!(st.cursor, "1795593600", "{name}");
        assert!(
            st.next_run_at > OffsetDateTime::now_utc().unix_timestamp() + 3000,
            "{name} {st:?}"
        );
        let asked = mock.count("/goldencopy/latest");
        two.run_feed(Feed::Gleif, b).await;
        assert_eq!(
            mock.count("/goldencopy/latest"),
            asked,
            "{name}: not before the retry"
        );
        assert_eq!(two.feed_stats(Feed::Gleif).runs, 0, "{name}");

        // Switched off: never read, and shown off.
        store
            .set_enrichment_settings(&EnrichmentSettingsPatch {
                feeds: FeedSwitchesPatch {
                    gleif: Some(false),
                    ..FeedSwitchesPatch::default()
                },
                ..EnrichmentSettingsPatch::default()
            })
            .unwrap();
        seed(b, "gleif", "1795593600").await;
        assert!(!two.run_feed(Feed::Gleif, b).await, "{name}");
        assert_eq!(mock.count("/goldencopy/latest"), asked, "{name}");
        assert!(!two.feed_stats(Feed::Gleif).enabled, "{name}");
        assert!(
            two.stats()
                .feeds
                .iter()
                .any(|f| f.feed == "gleif" && !f.enabled)
        );
    }
    backends.drop_schema().await;
}

// ---------------------------------------------------------------------------------------------
// EDGAR

fn ymd(d: Date) -> String {
    format!("{:04}{:02}{:02}", d.year(), u8::from(d.month()), d.day())
}

fn index_path(d: Date) -> String {
    let q = (u8::from(d.month()) - 1) / 3 + 1;
    format!(
        "/edgar/www/Archives/edgar/daily-index/{}/QTR{q}/master.{}.idx",
        d.year(),
        ymd(d)
    )
}

/// A known CIK's 10-K in the daily index makes the company due for EDGAR, its submissions
/// dropped from the cache; other CIKs and forms are left alone. With the feed on, EDGAR's own
/// refresh is 30 days.
#[tokio::test(flavor = "multi_thread")]
async fn edgar_daily_index_makes_filers_due() {
    let today = OffsetDateTime::now_utc().date();
    let day = today - time::Duration::days(2);
    let index = format!(
        "Description:           Master Index of EDGAR Dissemination Feed\n\
         Last Data Received:    Example\n\n\
         CIK|Company Name|Form Type|Date Filed|Filename\n\
         --------------------------------------------------------------------------------\n\
         9990000001|FICTIVE FEED HOLDINGS INC|10-K|{d}|edgar/data/9990000001/0009990000-26-000001.txt\n\
         9990000001|FICTIVE FEED HOLDINGS INC|4|{d}|edgar/data/9990000001/0009990000-26-000002.txt\n\
         9990000002|FICTIVE NOBODY CORP|8-K|{d}|edgar/data/9990000002/0009990000-26-000003.txt\n\
         9990000003|FICTIVE QUARTERLY CORP|10-Q|{d}|edgar/data/9990000003/0009990000-26-000004.txt\n",
        d = ymd(day)
    );
    let day_path = index_path(day);
    let mock = Mock::start(move |t| {
        if t == day_path {
            return Reply::bytes("text/plain", index.as_bytes().to_vec());
        }
        if t.starts_with("/edgar/data/submissions/CIK999000000") {
            let cik = &t["/edgar/data/submissions/CIK".len()..][..10];
            return Reply::json(&json!({
                "cik": cik, "name": "Fictive Feed Holdings Inc.",
                "filings": {"recent": {"accessionNumber": [], "filingDate": [], "form": []}}
            }));
        }
        Reply::status(404)
    })
    .await;
    let backends = Backends::all().await;
    for (name, b, store) in &backends.list {
        only(store, "edgar");
        let filer = company(
            b,
            "Fictive Feed Holdings Inc",
            "US",
            &["reg:edgar:9990000001".to_owned()],
        )
        .await;
        let quarterly = company(
            b,
            "Fictive Quarterly Corp",
            "US",
            &["reg:edgar:9990000003".to_owned()],
        )
        .await;
        let job = job(b, store, &mock, "feeds-edgar");
        assert_eq!(pass(&job, b).await, 2, "{name}");
        let e = b.enrichment(filer).await.unwrap().unwrap();
        assert!(
            e.next_due["edgar"] > OffsetDateTime::now_utc().unix_timestamp() + 20 * 86_400,
            "{name}: with the feed on, EDGAR is refreshed on the 30-day schedule {e:?}"
        );
        let asked = mock.count("CIK9990000001");

        seed(b, "edgar", &format!("{}", today - time::Duration::days(3))).await;
        job.run_feed(Feed::Edgar, b).await;
        let s = job.feed_stats(Feed::Edgar);
        assert_eq!(
            (s.runs, s.companies_due, s.failures),
            (1, 1, 0),
            "{name} {s:?}"
        );
        assert_eq!(s.records, 4, "{name}");
        assert!(due(b, filer, "edgar").await, "{name}");
        assert!(
            !due(b, quarterly, "edgar").await,
            "{name}: a 10-Q is not a trigger"
        );
        let cursor = b.feed_state("edgar").await.unwrap().unwrap().cursor;
        let yesterday = today - time::Duration::days(1);
        assert!(
            cursor == day.to_string() || cursor == yesterday.to_string(),
            "{name}: {cursor} (yesterday's index is not out yet, unless it was a weekend)"
        );
        assert_eq!(pass(&job, b).await, 1, "{name}");
        assert!(mock.count("CIK9990000001") > asked, "{name}: asked again");
        // The day is not read again.
        let reads = mock.count(&ymd(day));
        job.run_feed(Feed::Edgar, b).await;
        assert_eq!(mock.count(&ymd(day)), reads, "{name}");
    }
    backends.drop_schema().await;
}

// ---------------------------------------------------------------------------------------------
// APR

fn apr_set(as_of: &str, companies: &[(&str, &str, &str)]) -> Value {
    let mut data = Map::new();
    for (m, name, status) in companies {
        data.insert(
            (*m).to_owned(),
            json!({
                "PoslovnoIme": name,
                "SifraOpstine": "99001",
                "NazivOpstine": "Измишљеново",
                "NazivStatus": status,
                "DatumOsnivanja": "2004-05-06",
                "NazivPravneForme": "Друштво са ограниченом одговорношћу",
                "SifraDelatnosti": "4120"
            }),
        );
    }
    json!({"DatumPreseka": as_of, "Podaci": data})
}

/// A new monthly set is swapped in: an MB whose record changed (its status) is made due,
/// with the new record already in the cache; an unchanged one is not. The same set again
/// changes nothing.
#[tokio::test(flavor = "multi_thread")]
async fn apr_monthly_set_is_swapped_in() {
    let (m1, m2) = (mb("9930001"), mb("9930002"));
    let set = Arc::new(Mutex::new(Value::Null));
    let s2 = Arc::clone(&set);
    let mock = Mock::start(move |t| {
        if t == "/apr/companies" {
            return Reply::json(&s2.lock().unwrap());
        }
        Reply::status(404)
    })
    .await;
    let backends = Backends::all().await;
    for (name, b, store) in &backends.list {
        let v1 = [
            (m1.as_str(), "Фиктивна Фид д.о.о.", "Активан"),
            (m2.as_str(), "Fiktivni Mirni d.o.o.", "Активан"),
        ];
        *set.lock().unwrap() = apr_set("2026-10-01", &v1);
        only(store, "apr");
        let c1 = company(b, "Fiktivna Fid d.o.o.", "RS", &[format!("id:rs_mb:{m1}")]).await;
        let c2 = company(
            b,
            "Fiktivni Mirni d.o.o.",
            "RS",
            &[format!("id:rs_mb:{m2}")],
        )
        .await;
        let job = job(b, store, &mock, "feeds-apr");
        assert_eq!(pass(&job, b).await, 2, "{name}");
        assert_eq!(
            props(b, c1).await["registry"]["apr"]["status"],
            "active",
            "{name}"
        );

        *set.lock().unwrap() = apr_set(
            "2026-11-01",
            &[
                (m1.as_str(), "Фиктивна Фид д.о.о.", "У ликвидацији"),
                (m2.as_str(), "Fiktivni Mirni d.o.o.", "Активан"),
            ],
        );
        let downloads = mock.count("/apr/companies");
        job.run_feed(Feed::Apr, b).await;
        let s = job.feed_stats(Feed::Apr);
        assert_eq!(
            (s.runs, s.companies_due, s.records, s.failures),
            (1, 1, 2, 0),
            "{name} {s:?}"
        );
        assert_eq!(mock.count("/apr/companies"), downloads + 1, "{name}");
        assert!(
            due(b, c1, "apr").await && !due(b, c2, "apr").await,
            "{name}"
        );
        assert_eq!(
            b.feed_state("apr").await.unwrap().unwrap().cursor,
            "2026-11-01"
        );

        // The next pass reads the new record from the cache: no download.
        assert_eq!(pass(&job, b).await, 1, "{name}");
        assert_eq!(mock.count("/apr/companies"), downloads + 1, "{name}");
        assert_eq!(
            props(b, c1).await["registry"]["apr"]["status"],
            "in_liquidation",
            "{name}"
        );

        // The same set again: downloaded, nothing compared.
        job.run_feed(Feed::Apr, b).await;
        let s = job.feed_stats(Feed::Apr);
        assert_eq!(
            (s.runs, s.companies_due, s.records),
            (2, 1, 2),
            "{name} {s:?}"
        );
    }
    backends.drop_schema().await;
}

// ---------------------------------------------------------------------------------------------
// Companies House

/// The stream's events for a known company number make it due for Companies House (its
/// cached answers dropped); the timepoints are kept per stream and the next connection
/// resumes from them; a timepoint too old (416) starts that stream from now. Only the
/// streaming key is sent, with basic auth.
#[tokio::test(flavor = "multi_thread")]
async fn companies_house_stream_resumes_from_its_timepoints() {
    let mock = Mock::start(move |t| {
        let (path, query) = t.split_once('?').unwrap_or((t, ""));
        match path {
            "/stream/companies" => Reply::bytes(
                "application/json",
                b"\n{\"resource_kind\":\"company-profile\",\"resource_uri\":\"/company/99000021\",\
                  \"resource_id\":\"99000021\",\"data\":{\"company_name\":\"FICTIVE STREAM LIMITED\"},\
                  \"event\":{\"timepoint\":101,\"type\":\"changed\"}}\n\
                  \n{\"resource_kind\":\"company-profile\",\"resource_uri\":\"/company/99000099\",\
                  \"resource_id\":\"99000099\",\"data\":{},\"event\":{\"timepoint\":102}}\n\
                  not an event\n"
                    .to_vec(),
            ),
            "/stream/officers" if query.contains("timepoint") => Reply::status(416),
            "/stream/officers" => Reply::bytes("application/json", b"\n\n".to_vec()),
            "/stream/persons-with-significant-control" => Reply::bytes(
                "application/json",
                b"{\"resource_kind\":\"company-psc-individual\",\
                  \"resource_uri\":\"/company/99000021/persons-with-significant-control/individual/fake\",\
                  \"data\":{\"name\":\"Mx Invented Person\"},\"event\":{\"timepoint\":7}}\n"
                    .to_vec(),
            ),
            "/ch/company/99000021" => Reply::json(&json!({
                "company_number": "99000021",
                "company_name": "FICTIVE STREAM LIMITED",
                "company_status": "active",
                "type": "ltd",
                "date_of_creation": "2001-03-04"
            })),
            _ => Reply::status(404),
        }
    })
    .await;
    let backends = Backends::all().await;
    for (name, b, store) in &backends.list {
        only(store, "companies_house");
        let c = company(
            b,
            "Fictive Stream Limited",
            "GB",
            &["reg:companies_house:99000021".to_owned()],
        )
        .await;
        let job = job(b, store, &mock, "feeds-ch");
        assert_eq!(pass(&job, b).await, 1, "{name}");
        let asked = mock.count("/ch/company/99000021");

        seed(b, "companies_house", "officers=5").await;
        assert!(
            job.run_feed(Feed::CompaniesHouse, b).await,
            "{name}: the stream keeps its lease"
        );
        let s = job.feed_stats(Feed::CompaniesHouse);
        assert_eq!((s.runs, s.failures), (1, 0), "{name} {s:?}");
        assert!(s.companies_due >= 1, "{name} {s:?}");
        assert!(due(b, c, "companies_house").await, "{name}");
        let st = b.feed_state("companies_house").await.unwrap().unwrap();
        assert_eq!(
            st.cursor, "companies=102,persons-with-significant-control=7",
            "{name}: the officers' timepoint was too old"
        );
        assert_eq!(st.owner.as_deref(), Some("feeds-ch"), "{name}");
        assert!(mock.count("/stream/officers?timepoint=5") >= 1, "{name}");
        assert!(mock.authorized("/stream/"), "{name}");

        // The next connection resumes.
        job.run_feed(Feed::CompaniesHouse, b).await;
        assert!(mock.count("/stream/companies?timepoint=102") >= 1, "{name}");
        assert!(
            mock.count("/stream/persons-with-significant-control?timepoint=7") >= 1,
            "{name}"
        );

        assert!(pass(&job, b).await >= 1, "{name}");
        assert!(
            mock.count("/ch/company/99000021") > asked,
            "{name}: asked again"
        );
        // Another instance cannot take the stream while this one holds it.
        let other = job_other(b, store, &mock);
        assert!(!other.run_feed(Feed::CompaniesHouse, b).await, "{name}");
        assert_eq!(other.feed_stats(Feed::CompaniesHouse).runs, 0, "{name}");
        assert!(
            b.release_feed("companies_house", &InstanceId::new("feeds-ch").unwrap())
                .await
                .unwrap()
        );
    }
    backends.drop_schema().await;
}

fn job_other(b: &Arc<dyn Backend>, store: &Arc<Store>, mock: &Mock) -> Enrichment {
    job(b, store, mock, "feeds-ch-other")
}

/// The identifiers the feeds look up are the job's own aliases.
#[test]
fn aliases_are_the_jobs() {
    let l = lei("FEEDALIAS");
    assert_eq!(
        Identifier::parse(IdKind::Lei, &l).unwrap().alias(),
        format!("id:lei:{l}")
    );
}
