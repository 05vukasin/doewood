//! The enrichment job end to end (ADR 0002 sections 1 to 3, 5 and 6), against mock
//! registries and mock websites on 127.0.0.1 only: never a real registry, never the internet.
//! Every company, identifier, relationship, domain and page here is invented; identifiers
//! only carry valid check digits. Website host names are `*.test` names that a fake resolver
//! points at the mock server, reached under the test-only address policy (exactly that server,
//! as the favicon tests of doewood-registry do); the production policy is never changed.
//!
//! Always against the SQLite backend, and against Postgres too when
//! `DOEWOOD_TEST_POSTGRES_URL` is set (in a schema of its own).

// Companies are named by letter in each test, as in its doc comment.
#![allow(
    clippy::many_single_char_names,
    clippy::case_sensitive_file_extension_comparisons,
    clippy::too_many_lines
)]

use std::collections::BTreeMap;
use std::io::Cursor;
use std::net::IpAddr;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use doewood_core::{InstanceId, MasterKey};
use doewood_enrich::{CompanyName, IdKind, Identifier};
use doewood_ingest::{Enrichment, EnrichmentOptions, EnrichmentParts, Templates, WebClientFactory};
use doewood_model::{
    Backend, Blake3, Direction, DismissRequest, Edge, EdgeKind, EdgeSource, EdgeState, MergeActor,
    MergeActorKind, MergeQuery, NeighbourQuery, NodeKind, StepVersions, StoredEdge,
};
use doewood_registry::favicon::{
    AddressPolicy, DnsResolver, GuardOptions, GuardedClient, SiteOrigin,
};
use doewood_store::{
    ApiKey, EnrichmentSettingsPatch, KeyChange, RegistrySwitchesPatch, Standalone, Store,
};
use image::{DynamicImage, ImageFormat, Rgba, RgbaImage};
use serde_json::{Map, Value, json};
use time::{Date, Month};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::watch;
use uuid::Uuid;

const PG_VAR: &str = "DOEWOOD_TEST_POSTGRES_URL";
const WD: &str = "http://www.wikidata.org/entity/";

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

/// Eight digits and the ISO 7064 MOD 11,10 check digit (a PIB, or a German VAT body).
fn mod_11_10(head: &str) -> String {
    let mut a = 10;
    for d in head.bytes().map(|b| u32::from(b - b'0')) {
        a = (a + d) % 10;
        if a == 0 {
            a = 10;
        }
        a = (a * 2) % 11;
    }
    format!("{head}{}", (11 - a) % 10)
}

fn day(y: i32, m: Month, d: u8) -> Date {
    Date::from_calendar_date(y, m, d).unwrap()
}

// ---------------------------------------------------------------------------------------------
// The mock server: registries by path, websites by `Host`

#[derive(Clone, Debug)]
struct Reply {
    status: u16,
    content_type: &'static str,
    body: Vec<u8>,
}

impl Reply {
    fn json(v: &Value) -> Self {
        Self {
            status: 200,
            content_type: "application/json",
            body: serde_json::to_vec(v).unwrap(),
        }
    }

    fn html(s: &str) -> Self {
        Self {
            status: 200,
            content_type: "text/html; charset=utf-8",
            body: s.as_bytes().to_vec(),
        }
    }

    fn png(body: Vec<u8>) -> Self {
        Self {
            status: 200,
            content_type: "image/png",
            body,
        }
    }

    fn status(status: u16) -> Self {
        Self {
            status,
            content_type: "application/json",
            body: br#"{"errors":[{"status":"404","title":"Not Found"}]}"#.to_vec(),
        }
    }
}

type Handler = dyn Fn(&str, &str) -> Reply + Send + Sync;

/// An HTTP/1.1 server on 127.0.0.1 answering `handler(host, target)`, recording
/// `host` + `target` of every request.
struct Mock {
    port: u16,
    seen: Arc<Mutex<Vec<String>>>,
}

impl Mock {
    async fn start(handler: impl Fn(&str, &str) -> Reply + Send + Sync + 'static) -> Self {
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
                    let host = head
                        .lines()
                        .filter_map(|l| l.split_once(':'))
                        .find(|(k, _)| k.trim().eq_ignore_ascii_case("host"))
                        .map(|(_, v)| v.trim().to_owned())
                        .unwrap_or_default();
                    let bare = host.rsplit_once(':').map_or(host.as_str(), |(h, _)| h);
                    let reply = handler(bare, &target);
                    log.lock().unwrap().push(format!("{bare}{target}"));
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
        self.seen.lock().unwrap().clone()
    }

    /// Requests whose `host` + target contains `part`.
    fn count(&self, part: &str) -> usize {
        self.seen().iter().filter(|s| s.contains(part)).count()
    }
}

/// Resolves every `*.test` name to 127.0.0.1; nothing else.
#[derive(Debug)]
struct FakeDns;

#[async_trait]
impl DnsResolver for FakeDns {
    async fn lookup(&self, host: &str) -> Result<Vec<IpAddr>, String> {
        if host.ends_with(".test") {
            Ok(vec![IpAddr::from([127, 0, 0, 1])])
        } else {
            Err(format!("{host}: no such host"))
        }
    }
}

/// Test-only: allows exactly the mock server, 127.0.0.1 on its port, under `*.test` names.
#[derive(Debug)]
struct MockOnly {
    port: u16,
}

impl AddressPolicy for MockOnly {
    fn allows_host(&self, host: &str) -> bool {
        host.ends_with(".test")
    }

    fn allows_port(&self, scheme: &str, port: u16) -> bool {
        scheme == "http" && port == self.port
    }

    fn allows_ip(&self, ip: IpAddr) -> bool {
        ip == IpAddr::from([127, 0, 0, 1])
    }
}

fn templates(mock: &Mock) -> Templates {
    let base = mock.base();
    let mut t = Templates::default();
    t.gleif.base_url = format!("{base}/gleif/");
    t.vies.base_url = format!("{base}/vies/");
    t.wikidata.sparql_url = format!("{base}/wikidata/sparql");
    t.wikidata.api_url = format!("{base}/wikidata/api");
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
            &mut t.vies.per_minute,
            &mut t.vies.burst,
            &mut t.vies.max_retries,
        ),
        (
            &mut t.wikidata.per_minute,
            &mut t.wikidata.burst,
            &mut t.wikidata.max_retries,
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
    t.origin = SiteOrigin {
        scheme: "http",
        port: Some(mock.port),
    };
    t
}

fn web(port: u16) -> WebClientFactory {
    Arc::new(move |_proxy| {
        GuardedClient::new(
            &GuardOptions::default(),
            Arc::new(FakeDns),
            Arc::new(MockOnly { port }),
        )
    })
}

fn job(backend: &Arc<dyn Backend>, store: &Arc<Store>, mock: &Mock, instance: &str) -> Enrichment {
    let (_tx, rx) = watch::channel(Some(Arc::clone(backend)));
    Enrichment::new(EnrichmentParts {
        instance: InstanceId::new(instance).unwrap(),
        backend: rx,
        settings: EnrichmentParts::store_settings(Arc::clone(store)),
        templates: templates(mock),
        web: web(mock.port),
        options: EnrichmentOptions {
            concurrency: 2,
            interval: Duration::from_hours(1),
            lease: Duration::from_mins(1),
            drain: Duration::from_secs(1),
            ..EnrichmentOptions::default()
        },
        on_change: Arc::new(|_| {}),
    })
}

async fn pass(job: &Enrichment, backend: &Arc<dyn Backend>) -> u64 {
    let (_tx, rx) = watch::channel(false);
    job.pass(backend, &rx).await
}

/// Switches registries on and off (`None`: as stored), and sets the Companies House key.
fn settings(store: &Store, on: &[(&str, bool)], ch_key: Option<&str>) {
    let get = |n: &str| on.iter().find(|(k, _)| *k == n).map(|(_, v)| *v);
    store
        .set_enrichment_settings(&EnrichmentSettingsPatch {
            registries: RegistrySwitchesPatch {
                gleif: get("gleif"),
                vies: get("vies"),
                wikidata: get("wikidata"),
                edgar: get("edgar"),
                apr: get("apr"),
                companies_house: get("companies_house"),
            },
            companies_house_key: ch_key
                .map_or(KeyChange::Keep, |k| KeyChange::Set(ApiKey::new(k).unwrap())),
            ..EnrichmentSettingsPatch::default()
        })
        .unwrap();
}

// ---------------------------------------------------------------------------------------------
// Backends and the graph as ingest leaves it

struct Backends {
    list: Vec<(&'static str, Arc<dyn Backend>, Arc<Store>)>,
    cleanup: Option<(tokio_postgres::Client, String)>,
}

fn store() -> Arc<Store> {
    Arc::new(Store::open_in_memory(&MasterKey::from_bytes([3; 32])).unwrap())
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
    let schema = format!("dw_enrich_{}", Uuid::now_v7().simple());
    admin
        .batch_execute(&format!("CREATE SCHEMA {schema}"))
        .await
        .unwrap();
    let sep = if url.contains('?') { '&' } else { '?' };
    let scoped = format!("{url}{sep}options=-c%20search_path%3D{schema}");
    let url = doewood_core::PostgresUrl::parse(&scoped).unwrap();
    // Tests run in parallel, each migrating its own schema: the first `CREATE EXTENSION` of
    // the database can race; a second try finds it.
    let mut tries = 0;
    let pg = loop {
        match doewood_pg::PgStore::connect(&url).await {
            Ok(pg) => break pg,
            Err(e) if tries < 5 => {
                tries += 1;
                eprintln!("connect {schema}: {e}; trying again");
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
            Err(e) => panic!("connect {schema}: {e}"),
        }
    };
    Some((Arc::new(pg), admin, schema))
}

static DOCS: AtomicU32 = AtomicU32::new(1);

/// A company as ingest leaves it: keyed by its first identifier (else its name key), with
/// its identifier and name aliases, and `docs` documents (each with `email` as its
/// `signal.email_domain`).
async fn company(
    b: &Arc<dyn Backend>,
    name: &str,
    country: &str,
    ids: &[(IdKind, &str)],
    aliases: &[String],
    docs: usize,
    email: Option<&str>,
) -> Uuid {
    let parsed = CompanyName::parse(name).unwrap();
    let ids: Vec<Identifier> = ids
        .iter()
        .map(|(k, v)| Identifier::parse(*k, v).unwrap_or_else(|| panic!("{k:?} {v}")))
        .collect();
    let key = ids
        .first()
        .map_or_else(|| parsed.ladder_keys()[0].1.clone(), Identifier::alias);
    let mut by_kind: Map<String, Value> = Map::new();
    for i in &ids {
        by_kind.insert(i.kind.as_str().to_owned(), json!([i.value]));
    }
    let id = b
        .upsert_node(
            NodeKind::Company,
            &key,
            &json!({"name": name, "key": parsed.key, "legal_form": parsed.legal_form,
                "jurisdiction": country, "ids": by_kind}),
        )
        .await
        .unwrap();
    let props = json!({"step": "test"});
    for i in &ids {
        b.claim_alias(NodeKind::Company, &i.alias(), id, &props)
            .await
            .unwrap();
    }
    for (_, k) in parsed.ladder_keys() {
        b.claim_alias(NodeKind::Company, &k, id, &props)
            .await
            .unwrap();
    }
    for a in aliases {
        b.claim_alias(NodeKind::Company, a, id, &props)
            .await
            .unwrap();
    }
    for _ in 0..docs {
        let n = DOCS.fetch_add(1, Ordering::Relaxed);
        let mut bytes = [0u8; 32];
        bytes[..4].copy_from_slice(&n.to_be_bytes());
        bytes[31] = 0xd0;
        let hash = Blake3(bytes);
        let facts: Vec<Value> = email
            .iter()
            .map(|d| json!({"key": "signal.email_domain", "value": {"t": "text", "v": d}, "conf": 0.9}))
            .collect();
        b.put_record(
            &hash,
            &json!({"format": "pdf", "facts": facts}),
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

async fn edges(b: &Arc<dyn Backend>, node: Uuid, kind: EdgeKind) -> Vec<(Uuid, StoredEdge)> {
    let q = NeighbourQuery {
        direction: Direction::Both,
        edge_kinds: vec![kind],
        include_inactive: true,
        ..NeighbourQuery::default()
    };
    b.neighbours(node, &q)
        .await
        .unwrap()
        .into_iter()
        .map(|n| (n.node.id, n.edge))
        .collect()
}

async fn props(b: &Arc<dyn Backend>, id: Uuid) -> Value {
    b.node(id).await.unwrap().unwrap().props
}

async fn owner(b: &Arc<dyn Backend>, alias: &str) -> Option<Uuid> {
    b.alias(NodeKind::Company, alias)
        .await
        .unwrap()
        .and_then(|a| a.node)
}

// ---------------------------------------------------------------------------------------------
// Registry fixtures

/// A GLEIF LEI record resource.
fn gleif_record(lei: &str, name: &str, country: &str, registered_as: &str) -> Value {
    json!({
        "type": "lei-records",
        "id": lei,
        "attributes": {
            "lei": lei,
            "entity": {
                "legalName": {"name": name, "language": "en"},
                "otherNames": [],
                "legalAddress": {"language": "en", "addressLines": ["1 Example Street"],
                    "city": "Exampletown", "country": country, "postalCode": "00000"},
                "registeredAt": {"id": "RA999999", "other": null},
                "registeredAs": registered_as,
                "jurisdiction": country,
                "category": "GENERAL",
                "legalForm": {"id": "XXXX", "other": null},
                "status": "ACTIVE",
                "expiration": {"date": null, "reason": null},
                "successorEntity": {"lei": null, "name": null},
                "successorEntities": [],
                "eventGroups": []
            },
            "registration": {"status": "ISSUED", "lastUpdateDate": "2026-06-01T08:00:00Z"}
        }
    })
}

fn one(v: &Value) -> Value {
    json!({"data": v})
}

/// A GLEIF relationship record: `child` consolidated by `parent` from `start`.
fn relationship(child: &str, parent: &str, start: &str) -> Value {
    json!({"data": {
        "type": "relationship-records",
        "id": format!("{child}-{parent}"),
        "attributes": {
            "relationship": {
                "startNode": {"id": child, "type": "LEI"},
                "endNode": {"id": parent, "type": "LEI"},
                "type": "IS_DIRECTLY_CONSOLIDATED_BY",
                "status": "ACTIVE",
                "periods": [{"startDate": start, "endDate": null, "type": "RELATIONSHIP_PERIOD"}]
            },
            "registration": {"status": "PUBLISHED"}
        }
    }})
}

/// GLEIF's answers for `records` (`lei -> (record, direct parent)`), 404 for the rest.
fn gleif(target: &str, records: &BTreeMap<String, (Value, Option<Value>)>) -> Option<Reply> {
    let rest = target.strip_prefix("/gleif/")?;
    if rest.starts_with("lei-records?") {
        return Some(Reply::json(&json!({"data": []})));
    }
    let rest = rest.strip_prefix("lei-records/")?;
    let (lei, tail) = rest.split_once('/').unwrap_or((rest, ""));
    let Some((record, parent)) = records.get(lei) else {
        return Some(Reply::status(404));
    };
    Some(match tail {
        "" => Reply::json(&one(record)),
        "direct-parent-relationship" => parent
            .as_ref()
            .map_or_else(|| Reply::status(404), Reply::json),
        _ => Reply::status(404),
    })
}

/// A SPARQL results document: `item` with its LEI, a parent organization from `start`, and
/// an official website.
fn wikidata_item(item: u64, lei: &str, parent: u64, start: &str, website: &str) -> Value {
    let row = |prop: &str, st: &str, value: Value, extra: &[(&str, Value)]| {
        let mut r = json!({
            "item": {"type": "uri", "value": format!("{WD}Q{item}")},
            "itemLabel": {"type": "literal", "xml:lang": "en", "value": "Fiktivna Gradnja"},
            "prop": {"type": "literal", "value": prop},
            "st": {"type": "uri", "value": format!("http://www.wikidata.org/entity/statement/Q{item}-{st}")},
            "rank": {"type": "uri", "value": "http://wikiba.se/ontology#NormalRank"},
            "human": {"type": "literal", "datatype": "http://www.w3.org/2001/XMLSchema#boolean", "value": "false"},
        });
        r["value"] = value;
        for (k, v) in extra {
            r[*k] = v.clone();
        }
        r
    };
    json!({
        "head": {"vars": ["item", "itemLabel", "prop", "st", "value", "valueLabel", "human",
            "rank", "start", "startPrecision", "end", "endPrecision"]},
        "results": {"bindings": [
            row("P1278", "lei", json!({"type": "literal", "value": lei}), &[]),
            row("P749", "p1", json!({"type": "uri", "value": format!("{WD}Q{parent}")}), &[
                ("valueLabel", json!({"type": "literal", "value": "Wiki Parent Example"})),
                ("start", json!({"type": "literal",
                    "datatype": "http://www.w3.org/2001/XMLSchema#dateTime", "value": start})),
                ("startPrecision", json!({"type": "literal",
                    "datatype": "http://www.w3.org/2001/XMLSchema#integer", "value": "11"})),
            ]),
            row("P856", "w1", json!({"type": "uri", "value": website}), &[]),
        ]}
    })
}

fn empty_sparql() -> Value {
    json!({"head": {"vars": ["item"]}, "results": {"bindings": []}})
}

/// The APR open-data set.
fn apr_set(companies: &[(&str, &str, &str)]) -> Value {
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
    json!({"DatumPreseka": "2026-11-01", "Podaci": data})
}

/// A 16 px icon: a stripe on light blue (not blank).
fn icon() -> Vec<u8> {
    let img = DynamicImage::ImageRgba8(RgbaImage::from_fn(16, 16, |x, y| {
        if x.abs_diff(y) < 3 {
            Rgba([200, 30, 120, 255])
        } else {
            Rgba([180, 210, 240, 255])
        }
    }));
    let mut out = Cursor::new(Vec::new());
    img.write_to(&mut out, ImageFormat::Png).unwrap();
    out.into_inner()
}

fn site(target: &str, homepage: &str, notice: Option<&str>) -> Reply {
    match target {
        "/" => Reply::html(homepage),
        "/favicon.ico" => Reply::png(icon()),
        "/impressum" => notice.map_or_else(|| Reply::status(404), Reply::html),
        _ => Reply::status(404),
    }
}

// ---------------------------------------------------------------------------------------------
// The tests

/// Identifiers drive APR, GLEIF and Wikidata; their facts become identifiers, props and dated
/// edges; the website from Wikidata is verified and its favicon cached; the company is not
/// due again until its registries' refresh.
#[tokio::test(flavor = "multi_thread")]
async fn identifiers_drive_the_registries_end_to_end() {
    let lei_a = lei("GRADNJA");
    let lei_p = lei("HOLDING");
    let mb_a = mb("9910001");
    let mut records = BTreeMap::new();
    records.insert(
        lei_a.clone(),
        (
            gleif_record(&lei_a, "FIKTIVNA GRADNJA DOO", "RS", &mb_a),
            Some(relationship(&lei_a, &lei_p, "2020-01-01T00:00:00Z")),
        ),
    );
    records.insert(
        lei_p.clone(),
        (
            gleif_record(&lei_p, "Fiktivni Holding AG", "DE", "HRB 0001"),
            None,
        ),
    );
    let set = apr_set(&[(&mb_a, "Фиктивна Градња д.о.о.", "У ликвидацији")]);
    let lei_w = lei_a.clone();
    let mock = Mock::start(move |host, target| {
        if host == "fiktivna-gradnja.test" {
            return site(
                target,
                "<html><head><title>Fiktivna Gradnja</title></head><body>Dobrodošli</body></html>",
                None,
            );
        }
        if let Some(r) = gleif(target, &records) {
            return r;
        }
        if target.starts_with("/apr/companies") {
            return Reply::json(&set);
        }
        if target.starts_with("/wikidata/sparql") {
            return Reply::json(&if target.contains(&lei_w) {
                wikidata_item(
                    999_000_101,
                    &lei_w,
                    999_000_102,
                    "2023-05-01T00:00:00Z",
                    "http://www.fiktivna-gradnja.test/",
                )
            } else {
                empty_sparql()
            });
        }
        Reply::status(404)
    })
    .await;
    let backends = Backends::all().await;
    for (name, b, store) in &backends.list {
        let a = company(
            b,
            "Fiktivna Gradnja d.o.o.",
            "RS",
            &[(IdKind::Lei, &lei_a), (IdKind::RsMb, &mb_a)],
            &[],
            2,
            None,
        )
        .await;
        let w = company(
            b,
            "Wiki Parent Example GmbH",
            "DE",
            &[],
            &["reg:wikidata:Q999000102".to_owned()],
            1,
            None,
        )
        .await;
        let job = job(b, store, &mock, "enrich-a");
        assert!(pass(&job, b).await >= 1, "{name}");
        let stats = job.stats();
        assert_eq!(
            (stats.failed, stats.registry_warnings),
            (0, 0),
            "{name} {stats:?}"
        );

        // Nothing that was not asked for: Companies House has no key, EDGAR no CIK, VIES no
        // VAT number.
        for part in ["/ch/", "/edgar/", "/vies/"] {
            assert_eq!(mock.count(part), 0, "{name} {part}: {:?}", mock.seen());
        }

        // Identifiers and props.
        assert_eq!(
            owner(b, &format!("id:lei:{lei_a}")).await,
            Some(a),
            "{name}"
        );
        assert_eq!(owner(b, "reg:wikidata:Q999000101").await, Some(a), "{name}");
        let p = props(b, a).await;
        assert_eq!(
            p["registry"]["apr"]["status"], "in_liquidation",
            "{name} {p}"
        );
        assert_eq!(p["registry"]["gleif"]["found"], true, "{name}");
        assert_eq!(p["registry"]["wikidata"]["tier"], "community", "{name}");
        assert_eq!(p["legal_name"], "Фиктивна Градња д.о.о.", "{name}");

        // GLEIF's direct parent: created by its LEI, an active registry edge pair from 2020.
        let parent = owner(b, &format!("id:lei:{lei_p}")).await.expect("parent");
        assert_eq!(props(b, parent).await["name"], "Fiktivni Holding AG");
        let parents = edges(b, a, EdgeKind::ParentOf).await;
        let gl = parents
            .iter()
            .find(|(n, _)| *n == parent)
            .expect("parent_of");
        assert_eq!(gl.1.edge.src, parent);
        assert_eq!(gl.1.edge.state, EdgeState::Active, "{name}");
        assert_eq!(gl.1.edge.source, EdgeSource::Registry);
        assert_eq!(gl.1.edge.valid_from, Some(day(2020, Month::January, 1)));
        assert_eq!(gl.1.edge.evidence[0]["registry"], "gleif");
        assert!(
            gl.1.edge.evidence[0]["record"]
                .as_str()
                .unwrap()
                .ends_with("direct-parent-relationship")
        );
        let subs = edges(b, a, EdgeKind::SubsidiaryOf).await;
        assert!(subs.iter().any(|(n, e)| *n == parent && e.edge.src == a));

        // Wikidata's parent: the lowest tier, proposed only, and never the current parent.
        let wk = parents
            .iter()
            .find(|(n, _)| *n == w)
            .expect("wikidata parent");
        assert_eq!(wk.1.edge.state, EdgeState::Proposed, "{name}");
        assert_eq!(wk.1.edge.source, EdgeSource::Registry);
        assert_eq!(wk.1.edge.props["tier"], "community");
        assert!(edges(b, a, EdgeKind::FormerParentOf).await.is_empty());

        // APR: in liquidation.
        let liq = edges(b, a, EdgeKind::Liquidated).await;
        assert_eq!(liq.len(), 1, "{name}");
        assert_eq!(liq[0].1.edge.evidence[0]["registry"], "apr");

        // The website from Wikidata, verified, and its favicon in the icon cache.
        assert_eq!(
            p["website"]["domain"], "fiktivna-gradnja.test",
            "{name} {p}"
        );
        assert_eq!(p["website"]["source"], "wikidata");
        assert_eq!(p["website"]["matched"], "name");
        assert_eq!(p["icon"]["format"], "png");
        assert_eq!(owner(b, "domain:fiktivna-gradnja.test").await, Some(a));
        let cached = b.icon_get("fiktivna-gradnja.test").await.unwrap().unwrap();
        assert_eq!(cached.icon.unwrap().format, "png");

        // Freshness: each registry asked, due again at its refresh.
        let e = b.enrichment(a).await.unwrap().expect("recorded");
        let in_days = |r: &str| (e.next_due[r] - e.enriched_at) / 86_400;
        assert_eq!(
            (in_days("apr"), in_days("gleif"), in_days("wikidata")),
            (30, 30, 30),
            "{name} {e:?}"
        );
        assert!(!e.next_due.contains_key("companies_house"));
        let before = mock.seen().len();
        pass(&job, b).await;
        let requests: Vec<String> = mock.seen()[before..].to_vec();
        assert!(
            requests.iter().all(|r| !r.contains(&lei_a)),
            "{name}: not due, not asked again: {requests:?}"
        );
    }
    backends.drop_schema().await;
}

/// The gate: a company that owns an identifier the registry gave is merged (actor
/// `enrichment`, with the evidence); a dismissed pair is never merged; a name search adopts
/// an exact match, proposes the review band and leaves the rest.
#[tokio::test(flavor = "multi_thread")]
async fn the_gate_merges_proposes_and_honours_dismissals() {
    let (lei_x, lei_u) = (lei("ALFA"), lei("UPSILON"));
    let (mb_y, mb_v, mb_n, mb_r) = (mb("9920001"), mb("9920002"), mb("9920003"), mb("9920004"));
    let mut records = BTreeMap::new();
    records.insert(
        lei_x.clone(),
        (gleif_record(&lei_x, "ALFA PRIMER DOO", "RS", &mb_y), None),
    );
    records.insert(
        lei_u.clone(),
        (
            gleif_record(&lei_u, "UPSILON PRIMER DOO", "RS", &mb_v),
            None,
        ),
    );
    let set = apr_set(&[
        (&mb_n, "Gama Fiktiv d.o.o.", "Активан"),
        (&mb_r, "Beta Fiktiv Plus d.o.o.", "Активан"),
    ]);
    let mock = Mock::start(move |_, target| {
        if let Some(r) = gleif(target, &records) {
            return r;
        }
        if target.starts_with("/apr/companies") {
            return Reply::json(&set);
        }
        if target.starts_with("/wikidata/sparql") {
            return Reply::json(&empty_sparql());
        }
        Reply::status(404)
    })
    .await;
    let backends = Backends::all().await;
    for (name, b, store) in &backends.list {
        settings(store, &[("wikidata", false)], None);
        let x = company(
            b,
            "Alfa Primer d.o.o.",
            "RS",
            &[(IdKind::Lei, &lei_x)],
            &[],
            3,
            None,
        )
        .await;
        let y = company(
            b,
            "Alfa Primer Trgovina d.o.o.",
            "RS",
            &[(IdKind::RsMb, &mb_y)],
            &[],
            1,
            None,
        )
        .await;
        let u = company(
            b,
            "Upsilon Primer d.o.o.",
            "RS",
            &[(IdKind::Lei, &lei_u)],
            &[],
            1,
            None,
        )
        .await;
        let v = company(
            b,
            "Upsilon Drugi d.o.o.",
            "RS",
            &[(IdKind::RsMb, &mb_v)],
            &[],
            1,
            None,
        )
        .await;
        let actor = MergeActor {
            kind: MergeActorKind::Manual,
            instance: InstanceId::new("person").unwrap(),
        };
        b.dismiss_pair(&DismissRequest {
            a: u,
            b: v,
            actor,
            reason: "not the same".to_owned(),
        })
        .await
        .unwrap();
        let g = company(b, "Gama Fiktiv d.o.o.", "RS", &[], &[], 1, None).await;
        let r = company(b, "Beta Fiktiv d.o.o.", "RS", &[], &[], 1, None).await;
        let z = company(b, "Zeta Nepoznata d.o.o.", "RS", &[], &[], 1, None).await;

        let job = job(b, store, &mock, "enrich-gate");
        pass(&job, b).await;

        // X absorbed Y: the registry identifier hit (0.95), logged with the evidence.
        let log = b
            .list_merges(&MergeQuery {
                company: Some(x),
                ..MergeQuery::default()
            })
            .await
            .unwrap();
        assert_eq!(log.len(), 1, "{name} {log:?}");
        assert_eq!((log[0].survivor, log[0].absorbed), (x, y), "{name}");
        assert_eq!(log[0].actor.kind, MergeActorKind::Enrichment);
        assert!(log[0].probability >= 0.9);
        assert_eq!(
            log[0].evidence[0]["source"], "registry_id",
            "{}",
            log[0].evidence
        );
        assert_eq!(owner(b, &format!("id:rs_mb:{mb_y}")).await, Some(x));

        // U and V were dismissed: never merged, a proposal says why.
        assert!(
            b.list_merges(&MergeQuery {
                company: Some(u),
                ..MergeQuery::default()
            })
            .await
            .unwrap()
            .is_empty(),
            "{name}"
        );
        let pu = props(b, u).await;
        assert_eq!(
            pu["merge_proposals"][0]["reason"], "dismissed",
            "{name} {pu}"
        );
        assert_eq!(pu["merge_proposals"][0]["company"], v.to_string());
        assert_eq!(owner(b, &format!("id:rs_mb:{mb_v}")).await, Some(v));

        // Name search: an exact APR name is adopted (its MB joins the company) ...
        assert_eq!(
            owner(b, &format!("id:rs_mb:{mb_n}")).await,
            Some(g),
            "{name}"
        );
        // ... the review band is a proposal, not a match ...
        let pr = props(b, r).await;
        assert_eq!(owner(b, &format!("id:rs_mb:{mb_r}")).await, None, "{name}");
        assert_eq!(
            pr["registry_proposals"][0]["registry"], "apr",
            "{name} {pr}"
        );
        assert_eq!(pr["registry_proposals"][0]["reason"], "uncertain");
        // ... and nothing else is nothing.
        let pz = props(b, z).await;
        assert!(
            pz["registry_proposals"].as_array().unwrap().is_empty(),
            "{name}"
        );
        assert!(b.enrichment(z).await.unwrap().is_some());
        assert!(
            job.stats().merged >= 1 && job.stats().proposed >= 2,
            "{:?}",
            job.stats()
        );
    }
    backends.drop_schema().await;
}

/// Companies House and GLEIF both name a current parent: the later dated one stays current,
/// the earlier one is superseded on that day and becomes the former parent. A dated rename is
/// a `renamed_to` edge. Settings: Companies House is asked once a key is set.
#[tokio::test(flavor = "multi_thread")]
async fn a_later_dated_parent_makes_the_other_the_former_parent() {
    let (lei_c, lei_n) = (lei("CHILDUK"), lei("NEWPARENT"));
    let mut records = BTreeMap::new();
    records.insert(
        lei_c.clone(),
        (
            gleif_record(&lei_c, "FICTIVE CHILD EXAMPLE LIMITED", "GB", "99000011"),
            Some(relationship(&lei_c, &lei_n, "2022-03-01T00:00:00Z")),
        ),
    );
    records.insert(
        lei_n.clone(),
        (
            gleif_record(&lei_n, "FICTIVE NEW PARENT PLC", "GB", "99000013"),
            None,
        ),
    );
    let mock = Mock::start(move |_, target| {
        if let Some(r) = gleif(target, &records) {
            return r;
        }
        let path = target.split('?').next().unwrap_or("");
        match path {
            "/ch/company/99000011" => Reply::json(&json!({
                "company_number": "99000011",
                "company_name": "FICTIVE CHILD EXAMPLE LIMITED",
                "company_status": "active",
                "type": "ltd",
                "jurisdiction": "england-wales",
                "date_of_creation": "2001-03-04",
                "previous_company_names": [{"name": "FICTIVE OLD NAME LIMITED",
                    "effective_from": "2001-03-04", "ceased_on": "2015-06-30"}]
            })),
            "/ch/company/99000011/persons-with-significant-control" => Reply::json(&json!({
                "items": [
                    {"kind": "individual-person-with-significant-control",
                        "name": "Mx Invented Person", "notified_on": "2016-04-06",
                        "natures_of_control": ["ownership-of-shares-75-to-100-percent"]},
                    {"kind": "corporate-entity-person-with-significant-control",
                        "name": "FICTIVE OLD PARENT LIMITED", "notified_on": "2018-01-02",
                        "natures_of_control": ["ownership-of-shares-75-to-100-percent"],
                        "identification": {"identification_type": "uk-limited-company",
                            "country_registered": "England",
                            "registration_number": "99000012"}}
                ],
                "total_results": 2
            })),
            p if p.starts_with("/wikidata/") => Reply::json(&empty_sparql()),
            _ => Reply::status(404),
        }
    })
    .await;
    let backends = Backends::all().await;
    for (name, b, store) in &backends.list {
        settings(store, &[], Some("test-key"));
        let c = company(
            b,
            "Fictive Child Example Limited",
            "GB",
            &[(IdKind::Lei, &lei_c)],
            &["reg:companies_house:99000011".to_owned()],
            2,
            None,
        )
        .await;
        let job = job(b, store, &mock, "enrich-parents");
        pass(&job, b).await;
        assert!(
            mock.count("/ch/company/99000011") >= 2,
            "{name}: {:?}",
            mock.seen()
        );

        let old = owner(b, "reg:companies_house:99000012")
            .await
            .expect("old parent");
        let new = owner(b, &format!("id:lei:{lei_n}"))
            .await
            .expect("new parent");
        let parents = edges(b, c, EdgeKind::ParentOf).await;
        let of = |id: Uuid| {
            parents
                .iter()
                .find(|(n, _)| *n == id)
                .map(|(_, e)| e.edge.clone())
                .unwrap()
        };
        let (o, n) = (of(old), of(new));
        assert_eq!(n.state, EdgeState::Active, "{name}");
        assert_eq!(n.valid_to, None);
        assert_eq!(o.state, EdgeState::Superseded, "{name}");
        assert_eq!(o.valid_from, Some(day(2018, Month::January, 2)));
        assert_eq!(o.valid_to, Some(day(2022, Month::March, 1)));
        assert_eq!(o.evidence[0]["registry"], "companies_house");
        let twin = edges(b, c, EdgeKind::SubsidiaryOf).await;
        let t = twin.iter().find(|(id, _)| *id == old).unwrap();
        assert_eq!(t.1.edge.state, EdgeState::Superseded);
        let former = edges(b, c, EdgeKind::FormerParentOf).await;
        assert_eq!(former.len(), 1, "{name}");
        let f = &former[0].1.edge;
        assert_eq!((f.src, f.dst, f.state), (old, c, EdgeState::Active));
        assert_eq!(
            (f.valid_from, f.valid_to),
            (
                Some(day(2018, Month::January, 2)),
                Some(day(2022, Month::March, 1))
            )
        );
        // A natural person is never a parent.
        assert_eq!(parents.len(), 2, "{name}");

        let renames = edges(b, c, EdgeKind::RenamedTo).await;
        assert_eq!(renames.len(), 1, "{name}");
        assert_eq!(
            renames[0].1.edge.valid_from,
            Some(day(2015, Month::June, 30))
        );
        assert_eq!(renames[0].1.edge.props["from"], "FICTIVE OLD NAME LIMITED");
        let e = b.enrichment(c).await.unwrap().unwrap();
        assert!(e.next_due.contains_key("companies_house"), "{e:?}");
    }
    backends.drop_schema().await;
}

/// Switched-off registries and Companies House without a key are never asked; a registry
/// that is down is a warning, the others still answer, and it is asked again sooner.
#[tokio::test(flavor = "multi_thread")]
async fn settings_are_honoured_and_failures_are_soft() {
    let lei_s = lei("SOFT");
    let vat = format!("DE{}", mod_11_10("31745208"));
    let mock = Mock::start(move |_, target| {
        if target.starts_with("/vies/") {
            return Reply::status(503);
        }
        if target.starts_with("/wikidata/sparql") {
            return Reply::json(&empty_sparql());
        }
        Reply::status(404)
    })
    .await;
    let backends = Backends::all().await;
    for (name, b, store) in &backends.list {
        settings(store, &[("gleif", false)], None);
        let s = company(
            b,
            "Fiktiv Soft GmbH",
            "DE",
            &[(IdKind::Lei, &lei_s), (IdKind::Vat, &vat)],
            &["reg:companies_house:99000021".to_owned()],
            1,
            None,
        )
        .await;
        let job = job(b, store, &mock, "enrich-soft");
        pass(&job, b).await;
        assert_eq!(mock.count("/gleif/"), 0, "{name}: switched off");
        assert_eq!(mock.count("/ch/"), 0, "{name}: no key");
        assert!(mock.count("/vies/") >= 1, "{name}");
        assert!(
            mock.count("/wikidata/") >= 1,
            "{name}: the others still answer"
        );
        let stats = job.stats();
        assert!(stats.registry_warnings >= 1, "{name} {stats:?}");
        assert_eq!(stats.failed, 0);
        let e = b
            .enrichment(s)
            .await
            .unwrap()
            .expect("enriched all the same");
        assert_eq!(
            (e.next_due["vies"] - e.enriched_at) / 3600,
            6,
            "{name}: retried sooner"
        );
        assert_eq!((e.next_due["wikidata"] - e.enriched_at) / 86_400, 30);
        assert!(!e.next_due.contains_key("gleif"));
    }
    backends.drop_schema().await;
}

/// Websites from the documents' email domains: verified by the name on the homepage or an
/// identifier on its legal notice; refused when the page names neither, when another company
/// owns the domain, or when it is free mail (never fetched).
#[tokio::test(flavor = "multi_thread")]
async fn websites_are_verified_before_they_are_stored() {
    let pib = mod_11_10("10999001");
    let pib_page = format!(
        "<html><body><h1>Impressum</h1><p>PIB: {} {} {}</p></body></html>",
        &pib[..3],
        &pib[3..6],
        &pib[6..]
    );
    let mock = Mock::start(move |host, target| match host {
        "acme-fiktiv.test" => site(
            target,
            "<html><head><title>Acme Fiktiv d.o.o. | Početna</title></head></html>",
            None,
        ),
        "wrong-fiktiv.test" => site(target, "<html><title>Something else</title></html>", None),
        "legal-fiktiv.test" => site(
            target,
            "<html><title>Welcome</title><a href=\"/impressum\">Impressum</a></html>",
            Some(&pib_page),
        ),
        _ => Reply::status(404),
    })
    .await;
    let backends = Backends::all().await;
    for (name, b, store) in &backends.list {
        let off = [
            ("gleif", false),
            ("vies", false),
            ("wikidata", false),
            ("edgar", false),
            ("apr", false),
        ];
        settings(store, &off, None);
        let a = company(
            b,
            "Acme Fiktiv d.o.o.",
            "RS",
            &[],
            &[],
            2,
            Some("acme-fiktiv.test"),
        )
        .await;
        let w = company(
            b,
            "Omega Fiktiv d.o.o.",
            "RS",
            &[],
            &[],
            1,
            Some("wrong-fiktiv.test"),
        )
        .await;
        let g = company(
            b,
            "Gmail Fiktiv d.o.o.",
            "RS",
            &[],
            &[],
            1,
            Some("gmail.com"),
        )
        .await;
        let l = company(
            b,
            "Lambda Fiktiv d.o.o.",
            "RS",
            &[(IdKind::RsPib, &pib)],
            &[],
            1,
            Some("legal-fiktiv.test"),
        )
        .await;
        let job = job(b, store, &mock, "enrich-web");
        pass(&job, b).await;
        let pa = props(b, a).await;
        assert_eq!(pa["website"]["domain"], "acme-fiktiv.test", "{name} {pa}");
        assert_eq!(pa["website"]["source"], "email_domain");
        assert_eq!(pa["icon"]["path"], "/icons/acme-fiktiv.test");
        assert!(
            props(b, w).await["website"].is_null(),
            "{name}: the page names neither"
        );
        assert!(props(b, g).await["website"].is_null(), "{name}");
        assert_eq!(
            mock.count("gmail.com"),
            0,
            "{name}: free mail is never fetched"
        );
        let pl = props(b, l).await;
        assert_eq!(pl["website"]["matched"], "identifier", "{name} {pl}");
        assert!(
            pl["website"]["page"]
                .as_str()
                .unwrap()
                .ends_with("/impressum")
        );

        // A domain another company owns is not this one's.
        let d = company(
            b,
            "Delta Fiktiv d.o.o.",
            "RS",
            &[],
            &[],
            1,
            Some("acme-fiktiv.test"),
        )
        .await;
        pass(&job, b).await;
        assert!(props(b, d).await["website"].is_null(), "{name}");
        assert_eq!(owner(b, "domain:acme-fiktiv.test").await, Some(a));
        assert!(job.stats().websites >= 2);
    }
    backends.drop_schema().await;
}

/// EDGAR by CIK: its LEI joins the company; Exhibit 21 rows that name companies the graph
/// knows are proposed subsidiaries (best-effort), and an 8-K Item 2.01 naming one of them a
/// proposed acquisition.
#[tokio::test(flavor = "multi_thread")]
async fn edgar_subsidiaries_are_only_proposed() {
    let lei_e = lei("EDGARCO");
    let lei_w = lei_e.clone();
    let archive = "/edgar/www/Archives/edgar/data/9990000001/";
    let mock = Mock::start(move |_, target| {
        let t = target;
        if t == "/edgar/data/submissions/CIK9990000001.json" {
            return Reply::json(&json!({
                "cik": "9990000001",
                "name": "Fictive Holdings Example Inc.",
                "lei": lei_w,
                "formerNames": [{"name": "Fictive Widgets Example Corp",
                    "from": "2001-03-01T00:00:00.000Z", "to": "2019-08-05T00:00:00.000Z"}],
                "filings": {"recent": {
                    "accessionNumber": ["0009990000-26-000012", "0009990000-26-000010"],
                    "filingDate": ["2026-02-20", "2026-01-15"],
                    "reportDate": ["2025-12-31", "2026-01-12"],
                    "form": ["10-K", "8-K"],
                    "items": ["", "2.01,9.01"],
                    "primaryDocument": ["fict-10k.htm", "fict-8k.htm"],
                    "primaryDocDescription": ["10-K", "8-K"]
                }}
            }));
        }
        if t == format!("{archive}000999000026000012/0009990000-26-000012-index.htm") {
            return Reply::html(
                "<table><tr><th>Seq</th><th>Description</th><th>Document</th><th>Type</th></tr>\
                 <tr><td>1</td><td>10-K</td><td><a href=\"x\">fict-10k.htm</a></td><td>10-K</td></tr>\
                 <tr><td>2</td><td>EX</td><td><a href=\"x\">fict-ex21.htm</a></td><td>EX-21.1</td></tr>\
                 </table>",
            );
        }
        if t == format!("{archive}000999000026000012/fict-ex21.htm") {
            return Reply::html(
                "<table><tr><td>Name of Subsidiary</td><td>Jurisdiction</td></tr>\
                 <tr><td>Omega Invented Widgets LLC</td><td>Delaware</td></tr>\
                 <tr><td>Unknown Invented Sub LLC</td><td>Delaware</td></tr></table>",
            );
        }
        if t == format!("{archive}000999000026000010/fict-8k.htm") {
            return Reply::html(
                "<p><b>Item 2.01 Completion of Acquisition or Disposition of Assets.</b></p>\
                 <p>On January 12, 2026, Fictive Holdings Example Inc. completed its acquisition \
                 of Omega Invented Widgets LLC.</p><p>Item 9.01 Financial Statements.</p>",
            );
        }
        if t.starts_with("/wikidata/") {
            return Reply::json(&empty_sparql());
        }
        Reply::status(404)
    })
    .await;
    let backends = Backends::all().await;
    for (name, b, store) in &backends.list {
        settings(store, &[("gleif", false)], None);
        let e = company(
            b,
            "Fictive Holdings Example Inc.",
            "US",
            &[],
            &["reg:edgar:9990000001".to_owned()],
            2,
            None,
        )
        .await;
        let omega = company(b, "Omega Invented Widgets LLC", "US", &[], &[], 1, None).await;
        let job = job(b, store, &mock, "enrich-edgar");
        pass(&job, b).await;
        assert_eq!(
            owner(b, &format!("id:lei:{lei_e}")).await,
            Some(e),
            "{name}"
        );
        let subs = edges(b, e, EdgeKind::ParentOf).await;
        assert_eq!(
            subs.len(),
            1,
            "{name}: only the row the graph knows {subs:?}"
        );
        let s = &subs[0].1.edge;
        assert_eq!(
            (s.src, s.dst, s.state),
            (e, omega, EdgeState::Proposed),
            "{name}"
        );
        assert_eq!(s.valid_from, Some(day(2026, Month::February, 20)));
        let acq = edges(b, e, EdgeKind::Acquired).await;
        assert_eq!(acq.len(), 1, "{name}");
        assert_eq!(acq[0].1.edge.state, EdgeState::Proposed);
        assert_eq!(
            acq[0].1.edge.valid_from,
            Some(day(2026, Month::January, 15))
        );
        let renames = edges(b, e, EdgeKind::RenamedTo).await;
        assert_eq!(
            renames[0].1.edge.valid_from,
            Some(day(2019, Month::August, 5))
        );
    }
    backends.drop_schema().await;
}

/// Two instances on one shared store: the company another instance holds the lease on is
/// left alone, and enriched once the lease is gone.
#[tokio::test(flavor = "multi_thread")]
async fn a_lease_keeps_two_instances_off_one_company() {
    let lei_l = lei("LEASED");
    let mut records = BTreeMap::new();
    records.insert(
        lei_l.clone(),
        (
            gleif_record(&lei_l, "LEASED FIKTIV DOO", "RS", "nothing"),
            None,
        ),
    );
    let mock = Mock::start(move |_, target| {
        if let Some(r) = gleif(target, &records) {
            return r;
        }
        if target.starts_with("/wikidata/") {
            return Reply::json(&empty_sparql());
        }
        Reply::status(404)
    })
    .await;
    let Some((b, admin, schema)) = postgres().await else {
        return;
    };
    let (store_a, store_b) = (store(), store());
    let c = company(
        &b,
        "Leased Fiktiv d.o.o.",
        "RS",
        &[(IdKind::Lei, &lei_l)],
        &[],
        1,
        None,
    )
    .await;
    let other = InstanceId::new("enrich-b").unwrap();
    assert!(
        b.lease_company(c, &other, Duration::from_mins(5))
            .await
            .unwrap()
    );
    let a = job(&b, &store_a, &mock, "enrich-a");
    assert_eq!(pass(&a, &b).await, 0);
    assert_eq!(mock.count(&lei_l), 0, "{:?}", mock.seen());
    assert!(b.enrichment(c).await.unwrap().is_none());

    // B's own pass works on it (it holds the lease); A then finds nothing due.
    let job_b = job(&b, &store_b, &mock, "enrich-b");
    assert_eq!(pass(&job_b, &b).await, 1);
    assert!(b.enrichment(c).await.unwrap().is_some());
    let asked = mock.count(&format!("lei-records/{lei_l}"));
    assert!(asked >= 1);
    assert_eq!(pass(&a, &b).await, 0);
    assert_eq!(mock.count(&format!("lei-records/{lei_l}")), asked);
    // The lease was given back.
    assert!(
        b.lease_company(
            c,
            &InstanceId::new("enrich-a").unwrap(),
            Duration::from_mins(1)
        )
        .await
        .unwrap()
    );
    admin
        .batch_execute(&format!("DROP SCHEMA {schema} CASCADE"))
        .await
        .unwrap();
}
