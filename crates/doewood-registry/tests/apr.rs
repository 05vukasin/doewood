//! The APR client against a mock server on 127.0.0.1, with a hand-written open-data set.
//! Nothing here reaches APR or data.gov.rs: every company, name, municipality and date is
//! invented, and the MBs and PIBs only carry valid (or deliberately broken) check digits.

mod support;

use std::sync::Arc;
use std::time::Duration;

use doewood_enrich::IdKind;
use doewood_registry::apr::{Apr, AprConfig, LegalForm, Script, Status, TIER};
use doewood_registry::{
    Clock, HttpTransport, ManualClock, MemoryCache, Registry, TrustTier, USER_AGENT, WarningKind,
};
use serde_json::{Map, Value, json};
use support::{MockServer, Reply};
use time::{Date, Month, OffsetDateTime};

const DAY: Duration = Duration::from_hours(24);
const PATH: &str = "/api/v1/opendata/companies";

/// An invented MB: seven digits and the MOD 11 check digit (weights 2 7 6 5 4 3 2).
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

/// `mb` with a wrong check digit.
fn bad_mb(head: &str) -> String {
    let m = mb(head);
    let d = u32::from(m.as_bytes()[7] - b'0');
    format!("{head}{}", (d + 1) % 10)
}

/// An invented PIB: eight digits and the ISO 7064 MOD 11,10 check digit.
fn pib(head: &str) -> String {
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

/// One invented company of the set.
fn company(name: &str, status: &str, form: &str) -> Value {
    json!({
        "PoslovnoIme": name,
        "SifraOpstine": "99001",
        "NazivOpstine": "Измишљеново",
        "NazivStatus": status,
        "DatumOsnivanja": "2004-05-06",
        "NazivPravneForme": form,
        "SifraDelatnosti": "1071"
    })
}

/// The set: `{"DatumPreseka", "Podaci": {MB: company}}`.
fn set(as_of: &str, companies: &[(&str, Value)]) -> Vec<u8> {
    let mut data = Map::new();
    for (m, c) in companies {
        data.insert((*m).to_owned(), c.clone());
    }
    serde_json::to_vec(&json!({"DatumPreseka": as_of, "Podaci": data})).unwrap()
}

const DOO: &str = "Друштво са ограниченом одговорношћу";

/// The default set: one company of each status.
fn default_set() -> Vec<u8> {
    set(
        "2026-11-01",
        &[
            (
                &mb("9900001"),
                company("Пекара Измишљени Клас д.о.о. Измишљеново", "Активан", DOO),
            ),
            (
                &mb("9900002"),
                company("Izmišljena Tkanina doo", "У ликвидацији", DOO),
            ),
            (
                &mb("9900003"),
                company("Примерни Градитељ а.д.", "У стечају", "Акционарско друштво"),
            ),
            (
                &mb("9900004"),
                company(
                    "Измишљена Задруга Поље",
                    "У принудној ликвидацији",
                    "Задруга",
                ),
            ),
            (&mb("9900005"), company("Primer Ugašeni doo", "Брисан", DOO)),
            (
                &mb("9900006"),
                company("Измишљено Јавно Предузеће", "Мировање", "Јавно предузеће"),
            ),
            (
                &mb("9900007"),
                company("Пекара Клас Друга д.о.о.", "Активан", DOO),
            ),
        ],
    )
}

struct Setup {
    apr: Apr,
    clock: Arc<ManualClock>,
    cache: Arc<MemoryCache>,
}

fn setup(server: &MockServer, tweak: impl FnOnce(&mut AprConfig)) -> Setup {
    setup_with_cache(server, Arc::new(MemoryCache::default()), tweak)
}

fn setup_with_cache(
    server: &MockServer,
    cache: Arc<MemoryCache>,
    tweak: impl FnOnce(&mut AprConfig),
) -> Setup {
    let mut config = AprConfig {
        url: format!("{}opendata/companies", server.base),
        ..AprConfig::default()
    };
    tweak(&mut config);
    let clock = Arc::new(ManualClock::new(
        OffsetDateTime::from_unix_timestamp(1_790_000_000).unwrap(),
    ));
    let transport = HttpTransport::new(config.timeout, config.connect_timeout).unwrap();
    let apr = Apr::with_parts(&config, Arc::new(transport), cache.clone(), clock.clone())
        .expect("client");
    Setup { apr, clock, cache }
}

fn date(y: i32, m: Month, d: u8) -> Date {
    Date::from_calendar_date(y, m, d).unwrap()
}

fn kind(e: &doewood_registry::Warning) -> &WarningKind {
    assert_eq!(e.registry, Registry::Apr);
    &e.kind
}

#[tokio::test]
async fn mb_lookup_downloads_the_set_once() {
    let body = default_set();
    let server = MockServer::start(move |_, _| Reply::raw(200, body.clone())).await;
    let s = setup(&server, |_| {});

    let m = mb("9900001");
    let spaced = format!("{} {}-{}", &m[..2], &m[2..5], &m[5..]);
    let got = s.apr.company(&spaced).await.unwrap();
    assert_eq!(server.requests(), [PATH]);
    assert_eq!(got.fetched_at, s.clock.now_utc());
    let c = got.value.expect("found");
    assert_eq!(c.mb.value, m);
    assert_eq!(c.name, "Пекара Измишљени Клас д.о.о. Измишљеново");
    assert_eq!(c.script, Script::Cyrillic);
    assert_eq!(c.status, Status::Active);
    assert_eq!(c.legal_form, LegalForm::LimitedLiability);
    assert_eq!(c.legal_form.canonical(), Some("d.o.o."));
    assert_eq!(c.founded, Some(date(2004, Month::May, 6)));
    assert_eq!(c.as_of, Some(date(2026, Month::November, 1)));
    let muni = c.municipality.expect("municipality");
    assert_eq!(muni.code.as_deref(), Some("99001"));
    assert_eq!(muni.name.as_deref(), Some("Измишљеново"));
    assert_eq!(c.activity.as_deref(), Some("1071"));

    // Another MB and the same one again: no new download.
    s.clock.advance(DAY);
    let other = s.apr.company(&mb("9900002")).await.unwrap();
    assert_eq!(other.value.unwrap().script, Script::Latin);
    let again = s.apr.lookup(IdKind::RsMb, &m).await.unwrap();
    assert_eq!(again.value.unwrap().mb.value, m);
    assert_eq!(server.requests(), [PATH]);
    // Both are dated by the download, and cached by MB.
    assert_eq!(again.fetched_at, got.fetched_at);
    assert_eq!(s.cache.len(), 2);
    assert_eq!(
        server.header_values("user-agent"),
        [Some(USER_AGENT.to_owned())]
    );
    assert_eq!(TIER, TrustTier::Official);
    assert_eq!(Registry::Apr.tier(), TrustTier::Official);
}

#[tokio::test]
async fn each_status_and_legal_form() {
    let body = default_set();
    let server = MockServer::start(move |_, _| Reply::raw(200, body.clone())).await;
    let s = setup(&server, |_| {});
    for (head, status, form) in [
        ("9900001", Status::Active, LegalForm::LimitedLiability),
        (
            "9900002",
            Status::InLiquidation,
            LegalForm::LimitedLiability,
        ),
        ("9900003", Status::InBankruptcy, LegalForm::JointStock),
        (
            "9900004",
            Status::InForcedLiquidation,
            LegalForm::Cooperative,
        ),
        ("9900005", Status::Deleted, LegalForm::LimitedLiability),
        (
            "9900006",
            Status::Other("Мировање".to_owned()),
            LegalForm::PublicEnterprise,
        ),
    ] {
        let c = s.apr.company(&mb(head)).await.unwrap().value.unwrap();
        assert_eq!(c.status, status, "{head}");
        assert_eq!(c.legal_form, form, "{head}");
    }
    assert_eq!(server.requests().len(), 1);
}

#[tokio::test]
async fn pib_is_validated_then_unsupported() {
    let server = MockServer::start(|_, _| Reply::raw(200, default_set())).await;
    let s = setup(&server, |_| {});
    let e = s
        .apr
        .lookup(IdKind::RsPib, &pib("10873452"))
        .await
        .unwrap_err();
    assert!(matches!(kind(&e), WarningKind::Unsupported(m)
        if m.contains("PIB") && m.contains("docs/registries.md")));
    let p = pib("10873452");
    let broken = format!(
        "{}{}",
        &p[..8],
        (u32::from(p.as_bytes()[8] - b'0') + 1) % 10
    );
    for bad in [broken.as_str(), "000000000", "123456789", "PIB", ""] {
        let e = s.apr.lookup(IdKind::RsPib, bad).await.unwrap_err();
        assert!(matches!(kind(&e), WarningKind::InvalidInput(_)), "{bad}");
    }
    let e = s.apr.lookup(IdKind::Lei, &mb("9900001")).await.unwrap_err();
    assert!(matches!(kind(&e), WarningKind::InvalidInput(m) if m.contains("Serbian")));
    assert!(server.requests().is_empty());
}

#[tokio::test]
async fn malformed_input_is_never_sent() {
    let server = MockServer::start(|_, _| Reply::raw(200, default_set())).await;
    let s = setup(&server, |_| {});
    let long = "ж".repeat(257);
    for bad in [
        bad_mb("9900001"),
        "12345678".to_owned(),
        "00000000".to_owned(),
        "990000".to_owned(),
        "9900001x".to_owned(),
        String::new(),
        "MB".to_owned(),
    ] {
        let e = s.apr.company(&bad).await.unwrap_err();
        assert!(matches!(kind(&e), WarningKind::InvalidInput(_)), "{bad}");
    }
    for bad in ["", " .,- ", long.as_str()] {
        let e = s.apr.search(bad).await.unwrap_err();
        assert!(matches!(kind(&e), WarningKind::InvalidInput(_)), "{bad}");
    }
    assert!(server.requests().is_empty());

    let off = setup(&server, |c| c.enabled = false);
    let e = off.apr.company(&mb("9900001")).await.unwrap_err();
    assert_eq!(kind(&e), &WarningKind::Disabled);
    assert!(server.requests().is_empty());
}

#[tokio::test]
async fn not_found_is_none_and_cached() {
    let server = MockServer::start(|_, _| Reply::raw(200, default_set())).await;
    let s = setup(&server, |_| {});
    let got = s.apr.company(&mb("9912345")).await.unwrap();
    assert_eq!(got.value, None);
    assert_eq!(s.cache.len(), 1);
    assert!(s.apr.company(&mb("9912345")).await.unwrap().value.is_none());
    assert_eq!(server.requests().len(), 1);
}

#[tokio::test]
async fn name_search_is_script_blind_and_local() {
    let server = MockServer::start(|_, _| Reply::raw(200, default_set())).await;
    let s = setup(&server, |c| c.search_size = 5);

    // Latin words find the Cyrillic names, the exact name first.
    let got = s.apr.search("PEKARA KLAS DRUGA d.o.o.").await.unwrap();
    let names: Vec<_> = got.value.iter().map(|c| c.name.as_str()).collect();
    assert_eq!(names, ["Пекара Клас Друга д.о.о."]);
    let got = s.apr.search("pekara klas").await.unwrap();
    let names: Vec<_> = got.value.iter().map(|c| c.mb.value.clone()).collect();
    assert_eq!(names, [mb("9900001"), mb("9900007")]);
    // And Cyrillic words the Latin ones.
    let got = s.apr.search("Измишљена ткаНИНА").await.unwrap();
    assert_eq!(got.value.len(), 1);
    assert_eq!(got.value[0].mb.value, mb("9900002"));
    assert!(
        s.apr
            .search("Nepostojeće Ime")
            .await
            .unwrap()
            .value
            .is_empty()
    );
    // The size cap.
    let s1 = setup(&server, |c| c.search_size = 1);
    assert_eq!(s1.apr.search("doo").await.unwrap().value.len(), 1);
    // The names were never sent: only the set was fetched (once per client).
    assert_eq!(server.requests(), [PATH, PATH]);
}

#[tokio::test]
async fn refreshed_after_thirty_days() {
    let m = mb("9900002");
    let m2 = m.clone();
    let server = MockServer::start(move |_, n| {
        let status = if n == 0 {
            "Активан"
        } else {
            "У ликвидацији"
        };
        Reply::raw(
            200,
            set(
                "2026-11-01",
                &[(&m2, company("Izmišljena Tkanina doo", status, DOO))],
            ),
        )
    })
    .await;
    let s = setup(&server, |_| {});
    assert_eq!(
        s.apr.company(&m).await.unwrap().value.unwrap().status,
        Status::Active
    );
    s.clock.advance(29 * DAY);
    assert_eq!(
        s.apr.company(&m).await.unwrap().value.unwrap().status,
        Status::Active
    );
    assert_eq!(server.requests().len(), 1);
    s.clock.advance(2 * DAY);
    let got = s.apr.company(&m).await.unwrap();
    assert_eq!(got.value.unwrap().status, Status::InLiquidation);
    assert_eq!(got.fetched_at, s.clock.now_utc());
    assert_eq!(server.requests().len(), 2);
}

#[tokio::test]
async fn a_cached_company_needs_no_download() {
    let server = MockServer::start(|_, _| Reply::raw(200, default_set())).await;
    let cache = Arc::new(MemoryCache::default());
    let first = setup_with_cache(&server, cache.clone(), |_| {});
    first.apr.company(&mb("9900003")).await.unwrap();
    // A second instance sharing the cache.
    let second = setup_with_cache(&server, cache, |_| {});
    let c = second
        .apr
        .company(&mb("9900003"))
        .await
        .unwrap()
        .value
        .unwrap();
    assert_eq!(c.status, Status::InBankruptcy);
    assert_eq!(server.requests().len(), 1);
}

#[tokio::test]
async fn malformed_sets_are_refused_not_cached_and_paused() {
    let server = MockServer::start(|_, n| match n {
        0 => Reply::raw(200, b"<html>odrzavanje</html>".to_vec()),
        _ => Reply::raw(200, default_set()),
    })
    .await;
    let s = setup(&server, |_| {});
    let e = s.apr.company(&mb("9900001")).await.unwrap_err();
    assert!(matches!(kind(&e), WarningKind::Malformed(_)));
    assert!(s.cache.is_empty());
    // Within the pause: the same warning, nothing sent.
    s.clock.advance(Duration::from_mins(10));
    let e = s.apr.company(&mb("9900001")).await.unwrap_err();
    assert!(matches!(kind(&e), WarningKind::Malformed(_)));
    assert_eq!(server.requests().len(), 1);
    // After it: downloaded again.
    s.clock.advance(Duration::from_mins(6));
    assert!(s.apr.company(&mb("9900001")).await.unwrap().value.is_some());
    assert_eq!(server.requests().len(), 2);
}

#[tokio::test]
async fn malformed_shapes() {
    let one = company("Primer doo", "Активан", DOO);
    let cases: Vec<Vec<u8>> = vec![
        b"[]".to_vec(),
        b"{}".to_vec(),
        br#"{"DatumPreseka":"2026-11-01","Podaci":{}}"#.to_vec(),
        br#"{"DatumPreseka":"2026-11-01","Podaci":[]}"#.to_vec(),
        // Truncated.
        set("2026-11-01", &[(&mb("9900001"), one.clone())])[..40].to_vec(),
        // Too many companies for the cap below.
        set(
            "2026-11-01",
            &[(&mb("9900001"), one.clone()), (&mb("9900002"), one)],
        ),
    ];
    for body in cases {
        let b = body.clone();
        let server = MockServer::start(move |_, _| Reply::raw(200, b.clone())).await;
        let s = setup(&server, |c| c.max_companies = 1);
        let e = s.apr.company(&mb("9900001")).await.unwrap_err();
        assert!(
            matches!(kind(&e), WarningKind::Malformed(_)),
            "{}: {e}",
            String::from_utf8_lossy(&body)
        );
        assert!(s.cache.is_empty());
    }
}

#[tokio::test]
async fn oversize_sets_are_not_read() {
    for no_length in [false, true] {
        let server = MockServer::start(move |_, _| {
            let mut r = Reply::raw(200, default_set());
            r.no_length = no_length;
            r
        })
        .await;
        let s = setup(&server, |c| c.max_body = 512);
        let e = s.apr.company(&mb("9900001")).await.unwrap_err();
        assert_eq!(kind(&e), &WarningKind::TooLarge { limit: 512 });
        assert!(s.cache.is_empty());
    }
}

#[tokio::test]
async fn missing_set_is_a_warning() {
    let server = MockServer::start(|_, _| Reply::raw(404, b"{}".to_vec())).await;
    let s = setup(&server, |_| {});
    let e = s.apr.company(&mb("9900001")).await.unwrap_err();
    assert_eq!(kind(&e), &WarningKind::Status(404));
    assert!(s.cache.is_empty());
}

#[tokio::test]
async fn rate_limited_then_served() {
    let server = MockServer::start(|_, n| match n {
        0 => Reply::raw(429, Vec::new()).header("Retry-After", "3"),
        1 => Reply::raw(503, Vec::new()),
        _ => Reply::raw(200, default_set()),
    })
    .await;
    let s = setup(&server, |_| {});
    let got = s.apr.company(&mb("9900001")).await.unwrap();
    assert!(got.value.is_some());
    assert_eq!(server.requests().len(), 3);
    let sleeps = s.clock.sleeps();
    assert!(sleeps.contains(&Duration::from_secs(3)), "{sleeps:?}");
    // The 503 got the backoff (10 s, doubled on the second retry).
    assert!(sleeps.contains(&Duration::from_secs(20)), "{sleeps:?}");

    // A Retry-After past the limit gives up at once.
    let server =
        MockServer::start(|_, _| Reply::raw(429, Vec::new()).header("Retry-After", "3600")).await;
    let s = setup(&server, |_| {});
    let e = s.apr.company(&mb("9900001")).await.unwrap_err();
    assert_eq!(
        kind(&e),
        &WarningKind::RateLimited {
            retry_after: Some(Duration::from_hours(1))
        }
    );
    assert_eq!(server.requests().len(), 1);
}

#[tokio::test]
async fn encodings() {
    // UTF-8 with a byte order mark is read.
    let mut bom = b"\xEF\xBB\xBF".to_vec();
    bom.extend(default_set());
    let server = MockServer::start(move |_, _| Reply::raw(200, bom.clone())).await;
    let s = setup(&server, |_| {});
    let c = s.apr.company(&mb("9900001")).await.unwrap().value.unwrap();
    assert_eq!(c.name, "Пекара Измишљени Клас д.о.о. Измишљеново");

    // The set is UTF-8 JSON; Windows-1251 Cyrillic ("Пекара" as 0xCF 0xE5 ...) is not, and
    // is refused rather than misread.
    let mut cp1251 = format!(
        r#"{{"DatumPreseka":"2026-11-01","Podaci":{{"{}":{{"PoslovnoIme":""#,
        mb("9900001")
    )
    .into_bytes();
    cp1251.extend([0xCF, 0xE5, 0xEA, 0xE0, 0xF0, 0xE0]);
    cp1251.extend(br#""}}}"#);
    let server = MockServer::start(move |_, _| Reply::raw(200, cp1251.clone())).await;
    let s = setup(&server, |_| {});
    let e = s.apr.company(&mb("9900001")).await.unwrap_err();
    assert!(matches!(kind(&e), WarningKind::Malformed(_)), "{e}");
}

#[tokio::test]
async fn lenient_fields() {
    let m = mb("9900001");
    let mut data = Map::new();
    data.insert(
        m.clone(),
        json!({"PoslovnoIme": "  Primer Bez Podataka  ", "SifraOpstine": 99001,
            "NazivStatus": null, "DatumOsnivanja": "2004-13-40", "SifraDelatnosti": "10.71",
            "NovoPolje": {"a": [1, 2]}}),
    );
    data.insert("nije-mb".to_owned(), json!({"PoslovnoIme": "Preskočen"}));
    let body = serde_json::to_vec(&json!({"DatumPreseka": "nije datum", "Podaci": data})).unwrap();
    let server = MockServer::start(move |_, _| Reply::raw(200, body.clone())).await;
    let s = setup(&server, |_| {});
    let c = s.apr.company(&m).await.unwrap().value.unwrap();
    assert_eq!(c.name, "Primer Bez Podataka");
    assert_eq!(c.status, Status::Other(String::new()));
    assert_eq!(c.legal_form, LegalForm::Other(String::new()));
    assert_eq!(c.founded, None);
    assert_eq!(c.as_of, None);
    assert_eq!(c.activity, None);
    assert_eq!(c.municipality.unwrap().code.as_deref(), Some("99001"));
}

#[tokio::test]
async fn one_download_for_concurrent_lookups() {
    let server = MockServer::start(|_, _| {
        let mut r = Reply::raw(200, default_set());
        r.delay = Duration::from_millis(100);
        r
    })
    .await;
    let s = Arc::new(setup(&server, |_| {}));
    let mut tasks = Vec::new();
    for head in ["9900001", "9900002", "9900003", "9900004"] {
        let s = s.clone();
        tasks.push(tokio::spawn(async move {
            s.apr.company(&mb(head)).await.unwrap().value.is_some()
        }));
    }
    for t in tasks {
        assert!(t.await.unwrap());
    }
    assert_eq!(server.requests().len(), 1);
}

#[test]
fn extra_roots_must_be_certificates() {
    let config = AprConfig {
        extra_roots_pem: Some("not a certificate".to_owned()),
        ..AprConfig::default()
    };
    let e = Apr::new(&config, Arc::new(MemoryCache::default())).unwrap_err();
    assert!(
        matches!(kind(&e), WarningKind::Config(m) if m.contains("extra root")),
        "{e}"
    );
    let bad_url = AprConfig {
        url: "ftp://example.invalid/set".to_owned(),
        ..AprConfig::default()
    };
    let e = Apr::new(&bad_url, Arc::new(MemoryCache::default())).unwrap_err();
    assert!(matches!(kind(&e), WarningKind::Config(_)));
    assert!(Apr::new(&AprConfig::default(), Arc::new(MemoryCache::default())).is_ok());
}
