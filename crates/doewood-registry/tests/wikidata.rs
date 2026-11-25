//! The Wikidata client against a mock server on 127.0.0.1, with hand-written fixtures. Nothing
//! here reaches Wikidata: every item (Q999000001 and up), name, website and logo is invented,
//! and the LEIs only carry valid ISO 17442 check digits.

mod support;

use std::sync::Arc;
use std::time::{Duration, Instant};

use doewood_registry::wikidata::{
    Period, Precision, QId, TrustTier, WikiDate, Wikidata, WikidataConfig,
};
use doewood_registry::{HttpTransport, ManualClock, MemoryCache, USER_AGENT, WarningKind};
use serde_json::{Value, json};
use support::{MockServer, Reply, lei};
use time::{Date, Month, OffsetDateTime};
use url::Url;

const DAY: Duration = Duration::from_hours(24);
const E: &str = "http://www.wikidata.org/entity/";
const WB: &str = "http://wikiba.se/ontology#";

struct Setup {
    wd: Wikidata,
    clock: Arc<ManualClock>,
    cache: Arc<MemoryCache>,
}

fn setup(server: &MockServer, tweak: impl FnOnce(&mut WikidataConfig)) -> Setup {
    let mut config = WikidataConfig {
        sparql_url: format!("{}sparql", server.base),
        api_url: format!("{}w/api.php", server.base),
        ..WikidataConfig::default()
    };
    tweak(&mut config);
    let clock = Arc::new(ManualClock::new(
        OffsetDateTime::from_unix_timestamp(1_790_000_000).unwrap(),
    ));
    let cache = Arc::new(MemoryCache::default());
    let transport = HttpTransport::new(config.timeout, config.connect_timeout).unwrap();
    let wd = Wikidata::with_parts(&config, Arc::new(transport), cache.clone(), clock.clone())
        .expect("client");
    Setup { wd, clock, cache }
}

fn q(n: u64) -> QId {
    QId::new(n).unwrap()
}

fn uri(v: &str) -> Value {
    json!({"type": "uri", "value": v})
}

fn lit(v: &str) -> Value {
    json!({"type": "literal", "value": v})
}

fn time(v: &str) -> Value {
    json!({"type": "literal", "datatype": "http://www.w3.org/2001/XMLSchema#dateTime",
        "value": v})
}

fn int(v: u8) -> Value {
    json!({"type": "literal", "datatype": "http://www.w3.org/2001/XMLSchema#integer",
        "value": v.to_string()})
}

fn boolean(v: bool) -> Value {
    json!({"type": "literal", "datatype": "http://www.w3.org/2001/XMLSchema#boolean",
        "value": v.to_string()})
}

/// One result row: `item` has statement `st` of `prop` with `value`, plus `extra` bindings.
fn row(item: u64, prop: &str, st: &str, value: Value, extra: &[(&str, Value)]) -> Value {
    let mut r = json!({
        "item": uri(&format!("{E}Q{item}")),
        "itemLabel": json!({"type": "literal", "xml:lang": "en", "value": "Fictive Holdings Example"}),
        "prop": lit(prop),
        "st": uri(&format!("http://www.wikidata.org/entity/statement/Q{item}-{st}")),
        "rank": uri(&format!("{WB}NormalRank")),
        "human": boolean(false),
    });
    r["value"] = value;
    for (k, v) in extra {
        r[*k] = v.clone();
    }
    r
}

/// A SPARQL 1.1 JSON results document.
fn results(rows: &[Value]) -> Value {
    json!({
        "head": {"vars": ["item", "itemLabel", "prop", "st", "value", "valueLabel", "human",
            "rank", "start", "startPrecision", "end", "endPrecision"]},
        "results": {"bindings": rows}
    })
}

fn sparql_reply(v: &Value) -> Reply {
    let mut r = Reply::raw(200, serde_json::to_vec(v).unwrap());
    r.headers = vec![(
        "Content-Type".to_owned(),
        "application/sparql-results+json".to_owned(),
    )];
    r
}

/// The invented item Q999000001 with the LEI `lei_value`: two parents (one former, one
/// current with two start-time rows), a human owner, an unknown-value owner, a deprecated
/// parent, replaced by and followed by without qualifiers, a website and a logo.
fn fixture(lei_value: &str) -> Value {
    let label = |s: &str| ("valueLabel", lit(s));
    results(&[
        row(999_000_001, "P1278", "lei", lit(lei_value), &[]),
        row(
            999_000_001,
            "P749",
            "p1",
            uri(&format!("{E}Q999000002")),
            &[
                label("Fictive Parent Group"),
                ("start", time("2019-03-01T00:00:00Z")),
                ("startPrecision", int(11)),
                ("end", time("2022-06-30T00:00:00Z")),
                ("endPrecision", int(11)),
            ],
        ),
        row(
            999_000_001,
            "P749",
            "p2",
            uri(&format!("{E}Q999000003")),
            &[
                label("Fictive New Parent"),
                ("rank", uri(&format!("{WB}PreferredRank"))),
                ("start", time("2022-07-15T00:00:00Z")),
                ("startPrecision", int(9)),
            ],
        ),
        // The same statement again (a second start-time qualifier): one relation.
        row(
            999_000_001,
            "P749",
            "p2",
            uri(&format!("{E}Q999000003")),
            &[
                label("Fictive New Parent"),
                ("rank", uri(&format!("{WB}PreferredRank"))),
                ("start", time("2022-08-01T00:00:00Z")),
                ("startPrecision", int(10)),
            ],
        ),
        row(
            999_000_001,
            "P749",
            "p3",
            uri(&format!("{E}Q999000009")),
            &[("rank", uri(&format!("{WB}DeprecatedRank")))],
        ),
        row(
            999_000_001,
            "P127",
            "o1",
            uri(&format!("{E}Q999000004")),
            &[
                label("Invented Person Example"),
                ("human", boolean(true)),
                ("start", time("2010-05-00T00:00:00Z")),
                ("startPrecision", int(10)),
            ],
        ),
        row(
            999_000_001,
            "P127",
            "o2",
            uri("http://www.wikidata.org/.well-known/genid/0123abcd"),
            &[],
        ),
        row(
            999_000_001,
            "P1366",
            "r1",
            uri(&format!("{E}Q999000005")),
            &[label("Fictive Successor Example")],
        ),
        row(
            999_000_001,
            "P156",
            "f1",
            uri(&format!("{E}Q999000005")),
            &[],
        ),
        row(
            999_000_001,
            "P856",
            "w1",
            uri("https://fictive-holdings.example/"),
            &[],
        ),
        row(999_000_001, "P856", "w2", uri("javascript:alert(1)"), &[]),
        row(
            999_000_001,
            "P154",
            "l1",
            uri("http://commons.wikimedia.org/wiki/Special:FilePath/Fictive%20Holdings%20logo.svg"),
            &[
                ("start", time("2021-01-01T00:00:00Z")),
                ("startPrecision", int(7)),
            ],
        ),
    ])
}

fn sparql_of(target: &str) -> Option<String> {
    let url = Url::parse(&format!("http://mock{target}")).unwrap();
    url.query_pairs()
        .find(|(k, _)| k == "query")
        .map(|(_, v)| v.into_owned())
}

fn day(y: i32, m: Month, d: u8) -> Date {
    Date::from_calendar_date(y, m, d).unwrap()
}

#[tokio::test]
async fn lei_lookup_with_relations_and_dates() {
    let l = lei("WIKI01");
    let body = fixture(&l);
    let server = MockServer::start(move |target, _| {
        assert!(target.starts_with("/api/v1/sparql?query="), "{target}");
        sparql_reply(&body)
    })
    .await;
    let s = setup(&server, |_| {});
    let got = s.wd.by_lei(&l.to_lowercase()).await.unwrap();
    assert_eq!(
        got.fetched_at,
        OffsetDateTime::from_unix_timestamp(1_790_000_000).unwrap()
    );
    let [it] = got.value.as_slice() else {
        panic!("{:?}", got.value)
    };
    assert_eq!(it.qid, q(999_000_001));
    assert_eq!(it.label.as_deref(), Some("Fictive Holdings Example"));
    assert_eq!(it.tier, TrustTier::Community);
    assert_eq!(it.leis.len(), 1);
    assert_eq!(it.leis[0].value, l);

    // Two parents, sorted by start; the deprecated one is left out.
    let parents: Vec<_> = it.parent_organizations.iter().map(|r| r.target).collect();
    assert_eq!(parents, [q(999_000_002), q(999_000_003)]);
    let former = &it.parent_organizations[0];
    assert_eq!(former.label.as_deref(), Some("Fictive Parent Group"));
    assert_eq!(
        former.period.start,
        Some(WikiDate {
            date: day(2019, Month::March, 1),
            precision: Precision::Day
        })
    );
    assert_eq!(
        former.period.end,
        Some(WikiDate {
            date: day(2022, Month::June, 30),
            precision: Precision::Day
        })
    );
    assert!(!former.preferred && !former.human);
    let current = &it.parent_organizations[1];
    // Two start times (2022 at year precision, August 2022): the earliest.
    assert_eq!(
        current.period.start,
        Some(WikiDate {
            date: day(2022, Month::January, 1),
            precision: Precision::Year
        })
    );
    assert_eq!(current.period.end, None);
    assert!(current.preferred);

    // The human owner is kept and marked; the unknown value is skipped.
    let [owner] = it.owned_by.as_slice() else {
        panic!("{:?}", it.owned_by)
    };
    assert!(owner.human);
    assert_eq!(owner.target, q(999_000_004));
    // `2010-05-00` (month precision, day 0) is not a calendar date: no start.
    assert_eq!(owner.period.start, None);

    // Missing qualifiers: no dates.
    assert_eq!(it.replaced_by.len(), 1);
    assert_eq!(it.replaced_by[0].target, q(999_000_005));
    assert_eq!(it.replaced_by[0].period, Period::default());
    assert_eq!(
        it.replaced_by[0].label.as_deref(),
        Some("Fictive Successor Example")
    );
    assert_eq!(it.followed_by[0].target, q(999_000_005));
    assert_eq!(it.followed_by[0].label, None);

    // Only the http(s) website.
    let sites: Vec<_> = it.websites.iter().map(|w| w.url.as_str()).collect();
    assert_eq!(sites, ["https://fictive-holdings.example/"]);
    // The logo as a Commons URL; a start of century precision is dropped.
    let [logo] = it.logos.as_slice() else {
        panic!("{:?}", it.logos)
    };
    assert_eq!(logo.file, "Fictive_Holdings_logo.svg");
    assert_eq!(
        logo.url.as_str(),
        "https://commons.wikimedia.org/wiki/Special:FilePath/Fictive_Holdings_logo.svg"
    );
    assert_eq!(logo.period.start, None);

    // The query asked by the LEI, as a literal of the validated, upper-case value.
    let sent = sparql_of(&server.requests()[0]).unwrap();
    assert!(
        sent.contains(&format!("?leiSt ps:P1278 \"{l}\" ;")),
        "{sent}"
    );
    assert_eq!(
        server.header_values("accept"),
        [Some("application/sparql-results+json".to_owned())]
    );

    // Cached for 30 days, then asked again.
    s.clock.advance(29 * DAY);
    let again = s.wd.by_lei(&l).await.unwrap();
    assert_eq!(again.value, got.value);
    assert_eq!(again.fetched_at, got.fetched_at);
    assert_eq!(server.requests().len(), 1);
    s.clock.advance(2 * DAY);
    s.wd.by_lei(&l).await.unwrap();
    assert_eq!(server.requests().len(), 2);
}

#[tokio::test]
async fn qid_lookup() {
    let l = lei("WIKI02");
    let body = fixture(&l);
    let server = MockServer::start(move |target, _| {
        let sent = sparql_of(target).unwrap();
        if sent.contains("wd:Q999000001 }") {
            sparql_reply(&body)
        } else {
            sparql_reply(&results(&[]))
        }
    })
    .await;
    let s = setup(&server, |_| {});
    let it = s.wd.item(" q999000001 ").await.unwrap().value.unwrap();
    assert_eq!(it.qid, q(999_000_001));
    assert_eq!(it.leis[0].value, l);
    assert_eq!(it.parent_organizations.len(), 2);
    let sent = sparql_of(&server.requests()[0]).unwrap();
    assert!(sent.contains("VALUES ?item { wd:Q999000001 }"), "{sent}");
    assert!(!sent.contains("P1278 \""), "{sent}");

    // No such item: no rows, `None`, cached like an answer.
    assert_eq!(s.wd.item("Q999000077").await.unwrap().value, None);
    assert_eq!(s.wd.item("Q999000077").await.unwrap().value, None);
    assert_eq!(server.requests().len(), 2);
    assert_eq!(s.cache.len(), 2);
}

#[tokio::test]
async fn item_with_only_a_label_and_no_qualifiers() {
    let body = results(&[
        json!({"item": uri(&format!("{E}Q999000010")), "itemLabel": lit("Fictive Bare Item")}),
        row(
            999_000_010,
            "P749",
            "a",
            uri(&format!("{E}Q999000011")),
            &[],
        ),
        row(
            999_000_010,
            "P749",
            "b",
            uri(&format!("{E}Q999000012")),
            &[],
        ),
        row(
            999_000_010,
            "P749",
            "c",
            uri(&format!("{E}Q999000013")),
            &[],
        ),
        // A statement pointing at the item itself is dropped.
        row(
            999_000_010,
            "P127",
            "d",
            uri(&format!("{E}Q999000010")),
            &[],
        ),
        // An unknown property is ignored.
        row(
            999_000_010,
            "P9999",
            "e",
            uri(&format!("{E}Q999000014")),
            &[],
        ),
    ]);
    let server = MockServer::start(move |_, _| sparql_reply(&body)).await;
    let s = setup(&server, |_| {});
    let it = s.wd.item("Q999000010").await.unwrap().value.unwrap();
    assert_eq!(it.label.as_deref(), Some("Fictive Bare Item"));
    let parents: Vec<_> = it.parent_organizations.iter().map(|r| r.target).collect();
    assert_eq!(parents, [q(999_000_011), q(999_000_012), q(999_000_013)]);
    assert!(
        it.parent_organizations
            .iter()
            .all(|r| r.period.start.is_none() && r.period.end.is_none())
    );
    assert!(it.owned_by.is_empty() && it.websites.is_empty() && it.logos.is_empty());
    assert!(it.leis.is_empty());
}

#[tokio::test]
async fn empty_results_are_an_answer() {
    let server = MockServer::start(|_, _| sparql_reply(&results(&[]))).await;
    let s = setup(&server, |_| {});
    let l = lei("NOITEM");
    assert!(s.wd.by_lei(&l).await.unwrap().value.is_empty());
    assert!(s.wd.by_lei(&l).await.unwrap().value.is_empty());
    assert_eq!(server.requests().len(), 1);
}

#[tokio::test]
async fn answer_about_another_entity_is_malformed() {
    let asked = lei("ASKED1");
    let other = lei("OTHER1");
    let body = fixture(&other);
    let server = MockServer::start(move |_, _| sparql_reply(&body)).await;
    let s = setup(&server, |_| {});
    let w = s.wd.by_lei(&asked).await.unwrap_err();
    assert!(matches!(w.kind, WarningKind::Malformed(_)), "{w}");
    let w = s.wd.item("Q999000002").await.unwrap_err();
    assert!(matches!(w.kind, WarningKind::Malformed(_)), "{w}");
    assert!(s.cache.is_empty());
}

#[tokio::test]
async fn malformed_json_is_a_warning_and_not_cached() {
    for body in [
        b"<html>not json</html>".to_vec(),
        b"{\"head\":{}}".to_vec(),
        b"{\"results\":{\"bindings\":[{\"item\":{\"type\":\"literal\",\"value\":\"Q1\"}}]}}"
            .to_vec(),
        b"{\"results\":{\"bindings\":[{\"item\":{\"type\":\"uri\",\"value\":\"http://example.invalid/Q1\"}}]}}"
            .to_vec(),
        b"{\"results\":{\"bindings\":[".to_vec(),
    ] {
        let b = body.clone();
        let server = MockServer::start(move |_, _| Reply::raw(200, b.clone())).await;
        let s = setup(&server, |_| {});
        let w = s.wd.item("Q999000001").await.unwrap_err();
        assert!(
            matches!(w.kind, WarningKind::Malformed(_)),
            "{}: {w}",
            String::from_utf8_lossy(&body)
        );
        assert!(s.cache.is_empty());
        s.wd.item("Q999000001").await.unwrap_err();
        assert_eq!(server.requests().len(), 2);
    }
}

#[tokio::test]
async fn oversize_body_is_capped_before_parsing() {
    let big = serde_json::to_vec(&fixture(&lei("BIG1"))).unwrap();
    assert!(big.len() > 2048);
    for no_length in [false, true] {
        let body = big.clone();
        let server = MockServer::start(move |_, _| {
            let mut r = Reply::raw(200, body.clone());
            r.no_length = no_length;
            r
        })
        .await;
        let s = setup(&server, |c| c.max_body = 2048);
        let w = s.wd.item("Q999000001").await.unwrap_err();
        assert_eq!(w.kind, WarningKind::TooLarge { limit: 2048 }, "{no_length}");
        assert_eq!(server.requests().len(), 1);
        assert!(s.cache.is_empty());
    }
}

#[tokio::test]
async fn rate_limited_429_honours_retry_after() {
    let body = fixture(&lei("BUSY1"));
    let server = MockServer::start(move |_, n| {
        if n == 0 {
            Reply::raw(429, b"Too Many Requests".to_vec()).header("Retry-After", "7")
        } else {
            sparql_reply(&body)
        }
    })
    .await;
    let s = setup(&server, |c| c.per_minute = 60);
    assert!(s.wd.item("Q999000001").await.unwrap().value.is_some());
    let sleeps = s.clock.sleeps();
    assert_eq!(sleeps, [Duration::from_secs(7)], "{sleeps:?}");

    let server =
        MockServer::start(|_, _| Reply::raw(429, Vec::new()).header("Retry-After", "3600")).await;
    let s = setup(&server, |_| {});
    let w = s.wd.item("Q999000001").await.unwrap_err();
    assert_eq!(
        w.kind,
        WarningKind::RateLimited {
            retry_after: Some(Duration::from_hours(1))
        }
    );
    assert_eq!(server.requests().len(), 1);
    assert!(s.cache.is_empty());
}

#[tokio::test]
async fn query_timeout_5xx_is_retried_then_a_warning() {
    let server = MockServer::start(|_, _| {
        Reply::raw(500, b"java.util.concurrent.TimeoutException".to_vec())
    })
    .await;
    let s = setup(&server, |_| {});
    let w = s.wd.item("Q999000001").await.unwrap_err();
    assert_eq!(w.kind, WarningKind::Status(500));
    assert_eq!(server.requests().len(), 3);
    assert_eq!(s.clock.sleeps()[0], Duration::from_secs(2));
}

#[tokio::test]
async fn slow_answer_times_out() {
    let server = MockServer::start(|_, _| {
        let mut r = sparql_reply(&results(&[]));
        r.delay = Duration::from_secs(3);
        r
    })
    .await;
    let s = setup(&server, |c| {
        c.timeout = Duration::from_millis(200);
        c.max_retries = 0;
    });
    let started = Instant::now();
    let w = s.wd.item("Q999000001").await.unwrap_err();
    assert_eq!(w.kind, WarningKind::Timeout);
    assert!(
        started.elapsed() < Duration::from_secs(2),
        "{:?}",
        started.elapsed()
    );
    assert!(s.cache.is_empty());
}

#[tokio::test]
async fn hostile_ids_are_refused_before_sending() {
    let server = MockServer::start(|_, _| Reply::raw(500, Vec::new())).await;
    let s = setup(&server, |_| {});
    for id in [
        "",
        "Q",
        "Q0",
        "P31",
        "Q1\"",
        "Q1 } . ?s ?p ?o . {",
        "Q1\nQ2",
        "Q1\" . } SELECT * WHERE { ?s ?p ?o",
        "wd:Q1",
        "Q1#",
        "<http://www.wikidata.org/entity/Q1>",
        "Q99999999999999999999",
    ] {
        let w = s.wd.item(id).await.unwrap_err();
        assert!(
            matches!(w.kind, WarningKind::InvalidInput(_)),
            "{id:?}: {w}"
        );
    }
    for l in [
        String::new(),
        "not an LEI".to_owned(),
        // Valid characters but a wrong check digit.
        support::bad_lei("WIKI03"),
        format!("{}\" . ?s ?p ?o", lei("WIKI03")),
        format!("{} }} UNION {{ ?s ?p ?o }}", &lei("WIKI03")[..18]),
    ] {
        let w = s.wd.by_lei(&l).await.unwrap_err();
        assert!(matches!(w.kind, WarningKind::InvalidInput(_)), "{l:?}: {w}");
    }
    assert!(server.requests().is_empty(), "{:?}", server.requests());
}

#[tokio::test]
async fn lei_with_punctuation_is_compacted_never_passed_through() {
    // An LEI written with spaces, dashes and quotes around it is compacted to its 20
    // characters: none of the punctuation reaches the query.
    let l = lei("WIKI04");
    let server = MockServer::start(|_, _| sparql_reply(&results(&[]))).await;
    let s = setup(&server, |_| {});
    let messy = format!("\"{} {}\"\n{{}}", &l[..10], &l[10..]);
    s.wd.by_lei(&messy).await.unwrap();
    let sent = sparql_of(&server.requests()[0]).unwrap();
    assert!(
        sent.contains(&format!("?leiSt ps:P1278 \"{l}\" ;")),
        "{sent}"
    );
    assert!(!sent.contains(&messy));
    assert_eq!(sent.matches('"').count() % 2, 0);
}

fn search_answer() -> Value {
    json!({
        "searchinfo": {"search": "fictive"},
        "search": [
            {"id": "Q999000001", "title": "Q999000001", "pageid": 1,
                "label": "Fictive Holdings Example", "description": "invented company",
                "match": {"type": "label", "language": "en", "text": "Fictive Holdings Example"}},
            {"id": "Q999000020", "label": "Fictive Holdings (band)",
                "match": {"type": "alias", "language": "en", "text": "Fictive"}},
            {"id": "P999", "label": "a property"},
            {"label": "no id"},
            {"id": "Q999000001", "label": "duplicate"}
        ],
        "search-continue": 2,
        "success": 1
    })
}

#[tokio::test]
async fn search_by_name_returns_candidates() {
    let body = search_answer();
    let server = MockServer::start(move |target, _| {
        assert!(target.starts_with("/api/v1/w/api.php?"), "{target}");
        Reply::json_status(200, &body)
    })
    .await;
    let s = setup(&server, |_| {});
    let hits = s.wd.search("  Fictive   Holdings ").await.unwrap().value;
    let ids: Vec<_> = hits.iter().map(|h| h.qid).collect();
    assert_eq!(ids, [q(999_000_001), q(999_000_020)]);
    assert_eq!(hits[0].description.as_deref(), Some("invented company"));
    assert_eq!(hits[1].matched.as_deref(), Some("Fictive"));
    assert!(hits.iter().all(|h| h.tier == TrustTier::Community));
    let url = Url::parse(&format!("http://mock{}", server.requests()[0])).unwrap();
    let pairs: Vec<(String, String)> = url.query_pairs().into_owned().collect();
    let get = |k: &str| pairs.iter().find(|(x, _)| x == k).map(|(_, v)| v.as_str());
    assert_eq!(get("action"), Some("wbsearchentities"));
    assert_eq!(get("search"), Some("Fictive Holdings"));
    assert_eq!(get("type"), Some("item"));
    assert_eq!(get("language"), Some("en"));
    assert_eq!(get("limit"), Some("10"));
    assert_eq!(get("maxlag"), Some("5"));
    // Cached.
    s.wd.search("Fictive Holdings").await.unwrap();
    assert_eq!(server.requests().len(), 1);
}

#[tokio::test]
async fn hostile_names_are_url_encoded_and_never_reach_sparql() {
    let server = MockServer::start(|target, _| {
        assert!(!target.contains("/sparql"), "{target}");
        Reply::json_status(200, &json!({"search": []}))
    })
    .await;
    let s = setup(&server, |_| {});
    let hostile = [
        ("Fictive \"Quotes\" Ltd", "Fictive \"Quotes\" Ltd"),
        ("Fictive } . ?s ?p ?o { Ltd", "Fictive } . ?s ?p ?o { Ltd"),
        ("Fictive\nLtd\r\n\tGmbH", "Fictive Ltd GmbH"),
        ("a&action=wbeditentity&b=1", "a&action=wbeditentity&b=1"),
        ("Fictive'); DROP --", "Fictive'); DROP --"),
        ("100% #fictive?", "100% #fictive?"),
    ];
    for (name, _) in hostile {
        assert!(
            s.wd.search(name).await.unwrap().value.is_empty(),
            "{name:?}"
        );
    }
    let requests = server.requests();
    assert_eq!(requests.len(), hostile.len());
    for (target, (_, expected)) in requests.iter().zip(hostile) {
        let url = Url::parse(&format!("http://mock{target}")).unwrap();
        let pairs: Vec<(String, String)> = url.query_pairs().into_owned().collect();
        let searched: Vec<_> = pairs.iter().filter(|(k, _)| k == "search").collect();
        assert_eq!(searched.len(), 1, "{target}");
        assert_eq!(searched[0].1, expected, "{target}");
        assert_eq!(
            pairs.iter().filter(|(k, _)| k == "action").count(),
            1,
            "{target}"
        );
        assert!(!target.contains('"') && !target.contains(' ') && !target.contains('\n'));
    }

    for name in [
        "",
        "   \n\t ",
        "Fictive\u{0}Ltd",
        "Fictive\u{7}",
        &"x".repeat(257),
    ] {
        let w = s.wd.search(name).await.unwrap_err();
        assert!(
            matches!(w.kind, WarningKind::InvalidInput(_)),
            "{name:?}: {w}"
        );
    }
    assert_eq!(server.requests().len(), hostile.len());
}

#[tokio::test]
async fn api_errors_are_typed_and_maxlag_is_retried() {
    let body = search_answer();
    let server = MockServer::start(move |_, n| {
        if n == 0 {
            Reply::json_status(200, &json!({"error": {"code": "maxlag", "info": "lagged"}}))
                .header("Retry-After", "5")
        } else {
            Reply::json_status(200, &body)
        }
    })
    .await;
    let s = setup(&server, |c| c.per_minute = 60);
    assert_eq!(s.wd.search("Fictive").await.unwrap().value.len(), 2);
    assert_eq!(server.requests().len(), 2);

    let server = MockServer::start(|_, _| {
        Reply::json_status(200, &json!({"error": {"code": "badvalue", "info": "x"}}))
    })
    .await;
    let s = setup(&server, |_| {});
    let w = s.wd.search("Fictive").await.unwrap_err();
    assert_eq!(w.kind, WarningKind::Wikidata("badvalue".to_owned()));
    assert_eq!(server.requests().len(), 1);
    assert!(s.cache.is_empty());

    let server = MockServer::start(|_, _| {
        Reply::json_status(200, &json!({"error": {"code": "ratelimited"}}))
    })
    .await;
    let s = setup(&server, |_| {});
    let w = s.wd.search("Fictive").await.unwrap_err();
    assert_eq!(w.kind, WarningKind::Wikidata("ratelimited".to_owned()));
    assert_eq!(server.requests().len(), 3);
}

#[tokio::test]
async fn user_agent_is_sent_and_generic() {
    let body = fixture(&lei("UA1"));
    let server = MockServer::start(move |target, _| {
        if target.contains("/sparql") {
            sparql_reply(&body)
        } else {
            Reply::json_status(200, &json!({"search": []}))
        }
    })
    .await;
    let s = setup(&server, |_| {});
    s.wd.item("Q999000001").await.unwrap();
    s.wd.search("Fictive").await.unwrap();
    let agents = server.header_values("user-agent");
    assert_eq!(agents.len(), 2);
    for a in agents {
        let a = a.expect("a User-Agent");
        assert_eq!(a, USER_AGENT);
        assert!(
            a.starts_with(&format!("doewood/{} (", env!("CARGO_PKG_VERSION"))),
            "{a}"
        );
        assert!(a.contains("https://github.com/05vukasin/doewood"), "{a}");
        // No email, nothing personal: the project's URL is the contact.
        assert!(!a.contains('@'), "{a}");
    }
}

#[tokio::test]
async fn one_request_at_a_time() {
    let server = MockServer::start(|_, _| {
        let mut r = sparql_reply(&results(&[]));
        r.delay = Duration::from_millis(150);
        r
    })
    .await;
    let s = Arc::new(setup(&server, |c| {
        c.per_minute = 6000;
        c.burst = 10;
    }));
    let started = Instant::now();
    let calls: Vec<_> = (0..3)
        .map(|i| {
            let s = s.clone();
            tokio::spawn(async move { s.wd.item(&format!("Q99900010{i}")).await })
        })
        .collect();
    for c in calls {
        c.await.unwrap().unwrap();
    }
    // Three answers of 150 ms each, one after the other.
    assert!(
        started.elapsed() >= Duration::from_millis(450),
        "{:?}",
        started.elapsed()
    );
    assert_eq!(server.requests().len(), 3);
}

#[tokio::test]
async fn rate_limiter_spaces_requests() {
    let server = MockServer::start(|_, _| sparql_reply(&results(&[]))).await;
    let s = setup(&server, |_| {});
    for i in 1..=3 {
        s.wd.item(&format!("Q99900020{i}")).await.unwrap();
    }
    // The default: no burst, one every 3 s (20 a minute).
    let sleeps = s.clock.sleeps();
    assert_eq!(
        sleeps,
        [Duration::from_secs(3), Duration::from_secs(3)],
        "{sleeps:?}"
    );
}

#[tokio::test]
async fn disabled_sends_nothing() {
    let server = MockServer::start(|_, _| Reply::raw(500, Vec::new())).await;
    let s = setup(&server, |c| c.enabled = false);
    assert_eq!(
        s.wd.item("Q1").await.unwrap_err().kind,
        WarningKind::Disabled
    );
    assert_eq!(
        s.wd.by_lei(&lei("OFF1")).await.unwrap_err().kind,
        WarningKind::Disabled
    );
    assert_eq!(
        s.wd.search("Fictive").await.unwrap_err().kind,
        WarningKind::Disabled
    );
    assert!(server.requests().is_empty());
    assert!(WikidataConfig::default().enabled);
}

#[test]
fn bad_configuration_is_a_config_warning() {
    let bad: [fn(&mut WikidataConfig); 6] = [
        |c| c.sparql_url = "not a url".to_owned(),
        |c| c.sparql_url = "ftp://example.invalid/sparql".to_owned(),
        |c| c.api_url = "https://example.invalid/w/api.php?action=x".to_owned(),
        |c| c.language = "EN".to_owned(),
        |c| c.language = "en\") }".to_owned(),
        |c| c.language = String::new(),
    ];
    for tweak in bad {
        let mut config = WikidataConfig::default();
        tweak(&mut config);
        let w = Wikidata::new(&config, Arc::new(MemoryCache::default())).unwrap_err();
        assert!(matches!(w.kind, WarningKind::Config(_)), "{config:?}: {w}");
    }
    assert!(Wikidata::new(&WikidataConfig::default(), Arc::new(MemoryCache::default())).is_ok());
}
