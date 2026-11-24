//! The VIES client against a mock server on 127.0.0.1, with hand-written fixtures. Nothing here
//! reaches the real service: every company, address and VAT number is invented (the numbers
//! only carry valid check digits or formats).

mod support;

use std::sync::Arc;
use std::time::Duration;

use doewood_registry::vies::{Vies, ViesConfig, ViesError};
use doewood_registry::{Clock, HttpTransport, ManualClock, MemoryCache, WarningKind};
use serde_json::{Value, json};
use support::{MockServer, Reply};
use time::{Date, Month, OffsetDateTime};

const DAY: Duration = Duration::from_hours(24);

/// An invented Greek VAT number (Greece is checked for format only: nine digits).
const EL: &str = "094571238";

struct Setup {
    vies: Vies,
    clock: Arc<ManualClock>,
    cache: Arc<MemoryCache>,
}

fn setup(server: &MockServer, tweak: impl FnOnce(&mut ViesConfig)) -> Setup {
    let mut config = ViesConfig {
        base_url: server.base.clone(),
        ..ViesConfig::default()
    };
    tweak(&mut config);
    let clock = Arc::new(ManualClock::new(
        OffsetDateTime::from_unix_timestamp(1_790_000_000).unwrap(),
    ));
    let cache = Arc::new(MemoryCache::default());
    let transport = HttpTransport::new(config.timeout, config.connect_timeout).unwrap();
    let vies = Vies::with_parts(&config, Arc::new(transport), cache.clone(), clock.clone())
        .expect("client");
    Setup { vies, clock, cache }
}

/// A German VAT number body: `head` (eight digits, not starting with 0) and its ISO 7064
/// MOD 11,10 check digit.
fn de(head: &str) -> String {
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

/// A Northern Ireland VAT number body: `head` (seven digits) and the two check digits that
/// make the weighted sum (8 7 6 5 4 3 2 10 1) a multiple of 97.
fn xi(head: &str) -> String {
    let s: u32 = head
        .bytes()
        .zip([8, 7, 6, 5, 4, 3, 2])
        .map(|(b, w)| u32::from(b - b'0') * w)
        .sum();
    let cd = (97 - s % 97) % 97;
    format!("{head}{}{}", cd / 10, cd % 10)
}

/// A `check-vat-number` answer.
fn answer(country: &str, number: &str, valid: bool, name: &str, address: &str) -> Value {
    json!({
        "countryCode": country,
        "vatNumber": number,
        "requestDate": "2026-11-24T09:15:02.123Z",
        "valid": valid,
        "requestIdentifier": "",
        "name": name,
        "address": address,
        "traderName": "---",
        "traderStreet": "---",
        "traderPostalCode": "---",
        "traderCity": "---",
        "traderCompanyType": "---",
        "traderNameMatch": "NOT_PROCESSED",
        "traderStreetMatch": "NOT_PROCESSED",
        "traderPostalCodeMatch": "NOT_PROCESSED",
        "traderCityMatch": "NOT_PROCESSED",
        "traderCompanyTypeMatch": "NOT_PROCESSED"
    })
}

/// A VIES error answer (`CommonResponse`).
fn error(code: &str) -> Value {
    json!({
        "actionSucceed": false,
        "errorWrappers": [{"error": code, "message": "invented message"}]
    })
}

fn sent(server: &MockServer) -> Vec<Value> {
    server
        .bodies()
        .iter()
        .map(|b| serde_json::from_slice(b).unwrap())
        .collect()
}

fn close(d: Duration, secs: f64) -> bool {
    (d.as_secs_f64() - secs).abs() < 1e-6
}

#[tokio::test]
async fn valid_with_name_and_address() {
    let number = de("31745208");
    let body = answer(
        "DE",
        &number,
        true,
        "Fictive Pumps GmbH",
        "Beispielweg 1\n12345 Musterstadt",
    );
    let server = MockServer::start(move |_, _| Reply::json_status(200, &body)).await;
    let s = setup(&server, |_| {});

    let got = s.vies.check("de", &format!("DE {number}")).await.unwrap();
    assert_eq!(server.requests(), ["/api/v1/check-vat-number"]);
    assert_eq!(
        sent(&server),
        [json!({"countryCode": "DE", "vatNumber": number})]
    );
    assert_eq!(got.fetched_at, s.clock.now_utc());
    let r = got.value;
    assert!(r.valid);
    assert_eq!(r.vat.value, format!("DE{number}"));
    assert_eq!(r.country(), "DE");
    assert_eq!(r.number(), number);
    assert_eq!(r.name.as_deref(), Some("Fictive Pumps GmbH"));
    assert_eq!(
        r.address.as_deref(),
        Some("Beispielweg 1\n12345 Musterstadt")
    );
    assert_eq!(
        r.request_date,
        Some(Date::from_calendar_date(2026, Month::November, 24).unwrap())
    );
    // No requester number is sent, so no consultation number comes back.
    assert_eq!(r.request_identifier, None);
}

#[tokio::test]
async fn request_identifier_is_kept_when_given() {
    let number = de("52817364");
    let mut body = answer("DE", &number, true, "Sample Werke AG", "Probeweg 2");
    body["requestIdentifier"] = json!("WAPIAAAAZFAKE001");
    let server = MockServer::start(move |_, _| Reply::json_status(200, &body)).await;
    let s = setup(&server, |_| {});
    let r = s.vies.check("DE", &number).await.unwrap().value;
    assert_eq!(r.request_identifier.as_deref(), Some("WAPIAAAAZFAKE001"));
}

#[tokio::test]
async fn valid_with_name_and_address_withheld() {
    let number = de("41827365");
    let body = answer("DE", &number, true, "---", "---");
    let server = MockServer::start(move |_, _| Reply::json_status(200, &body)).await;
    let s = setup(&server, |_| {});
    let r = s.vies.check("DE", &number).await.unwrap().value;
    assert!(r.valid);
    assert_eq!(r.name, None);
    assert_eq!(r.address, None);
}

#[tokio::test]
async fn invalid_is_an_answer_cached_for_90_days() {
    let number = de("61827354");
    let body = answer("DE", &number, false, "---", "---");
    let server = MockServer::start(move |_, _| Reply::json_status(200, &body)).await;
    let s = setup(&server, |_| {});
    let first = s.vies.check("DE", &number).await.unwrap();
    assert!(!first.value.valid);
    assert_eq!(first.value.name, None);
    s.clock.advance(89 * DAY);
    let second = s.vies.check("DE", &number).await.unwrap();
    assert_eq!(second, first);
    assert_eq!(server.requests().len(), 1);
    assert_eq!(s.cache.len(), 1);
    s.clock.advance(2 * DAY);
    let third = s.vies.check("DE", &number).await.unwrap();
    assert_eq!(server.requests().len(), 2);
    assert!(third.fetched_at > first.fetched_at);
}

#[tokio::test]
async fn invalid_answer_never_shows_a_name() {
    let number = de("71829354");
    let body = answer("DE", &number, false, "Leftover Name GmbH", "Somewhere 3");
    let server = MockServer::start(move |_, _| Reply::json_status(200, &body)).await;
    let s = setup(&server, |_| {});
    let r = s.vies.check("DE", &number).await.unwrap().value;
    assert!(!r.valid);
    assert_eq!((r.name, r.address), (None, None));
}

#[tokio::test]
async fn service_errors_are_typed_warnings_never_cached() {
    // (code, retried)
    let cases = [
        ("MS_UNAVAILABLE", false),
        ("SERVICE_UNAVAILABLE", true),
        ("TIMEOUT", true),
        ("MS_MAX_CONCURRENT_REQ", true),
        ("MS_MAX_CONCURRENT_REQ_TIME", true),
        ("GLOBAL_MAX_CONCURRENT_REQ", true),
        ("GLOBAL_MAX_CONCURRENT_REQ_TIME", true),
        ("INVALID_INPUT", false),
        ("INVALID_REQUESTER_INFO", false),
        ("VAT_BLOCKED", false),
        ("IP_BLOCKED", false),
        ("A_FUTURE_CODE", false),
    ];
    for status in [200, 400, 500] {
        for (code, retried) in cases {
            let body = error(code);
            let server = MockServer::start(move |_, _| Reply::json_status(status, &body)).await;
            let s = setup(&server, |c| c.max_retries = 2);
            let w = s.vies.check("EL", EL).await.unwrap_err();
            assert_eq!(
                w.kind,
                WarningKind::Vies(ViesError::from_code(code)),
                "{status} {code}"
            );
            let expected = if retried { 3 } else { 1 };
            assert_eq!(server.requests().len(), expected, "{status} {code}");
            assert!(s.cache.is_empty(), "{status} {code}");
        }
    }
}

#[tokio::test]
async fn unavailable_is_never_taken_for_invalid() {
    // First the member state is down, then it answers: the outage was not cached.
    let body = answer("EL", EL, true, "Fiktiki Antlies AE", "Odos Paradeigmatos 1");
    let server = MockServer::start(move |_, n| {
        if n == 0 {
            Reply::json_status(200, &error("MS_UNAVAILABLE"))
        } else if n == 1 {
            // VIES's other answers carry the code in `userError`, next to `isValid: false`.
            Reply::json_status(
                200,
                &json!({"isValid": false, "userError": "MS_UNAVAILABLE"}),
            )
        } else {
            Reply::json_status(200, &body)
        }
    })
    .await;
    let s = setup(&server, |_| {});
    for _ in 0..2 {
        let w = s.vies.check("EL", EL).await.unwrap_err();
        assert_eq!(w.kind, WarningKind::Vies(ViesError::MsUnavailable));
        assert!(w.kind.to_string().contains("MS_UNAVAILABLE"), "{w}");
    }
    assert!(s.cache.is_empty());
    let r = s.vies.check("EL", EL).await.unwrap().value;
    assert!(r.valid);
    assert_eq!(server.requests().len(), 3);
}

#[tokio::test]
async fn busy_member_state_backs_off_and_empties_the_bucket() {
    let number = de("81726354");
    let body = answer("DE", &number, true, "Fictive Busy GmbH", "Wartestrasse 4");
    let server = MockServer::start(move |_, n| {
        if n == 0 {
            Reply::json_status(500, &error("MS_MAX_CONCURRENT_REQ"))
        } else {
            Reply::json_status(200, &body)
        }
    })
    .await;
    let s = setup(&server, |c| {
        c.per_minute = 20;
        c.backoff = Duration::from_secs(2);
    });
    let r = s.vies.check("DE", &number).await.unwrap().value;
    assert!(r.valid);
    assert_eq!(server.requests().len(), 2);
    // The 2 s backoff, then the rest of the emptied bucket's 3 s interval.
    let sleeps = s.clock.sleeps();
    assert!(close(sleeps[0], 2.0), "{sleeps:?}");
    let total: f64 = sleeps.iter().map(Duration::as_secs_f64).sum();
    assert!((total - 3.0).abs() < 1e-6, "{sleeps:?}");
}

#[tokio::test]
async fn rate_limited_429_honours_retry_after() {
    let number = de("91726354");
    let body = answer("DE", &number, true, "Fictive Queue GmbH", "Reihe 5");
    let server = MockServer::start(move |_, n| {
        if n == 0 {
            Reply::raw(429, Vec::new()).header("Retry-After", "9")
        } else {
            Reply::json_status(200, &body)
        }
    })
    .await;
    let s = setup(&server, |c| c.per_minute = 60);
    assert!(s.vies.check("DE", &number).await.unwrap().value.valid);
    let sleeps = s.clock.sleeps();
    assert!(close(sleeps[0], 9.0), "{sleeps:?}");
    assert_eq!(sleeps.len(), 1, "{sleeps:?}");

    let server =
        MockServer::start(|_, _| Reply::raw(429, Vec::new()).header("Retry-After", "600")).await;
    let s = setup(&server, |_| {});
    let w = s.vies.check("DE", &number).await.unwrap_err();
    assert_eq!(
        w.kind,
        WarningKind::RateLimited {
            retry_after: Some(Duration::from_mins(10))
        }
    );
    assert_eq!(server.requests().len(), 1);
}

#[tokio::test]
async fn plain_5xx_and_404_are_status_warnings() {
    let server = MockServer::start(|target, _| {
        if target.contains("check-vat-number") {
            Reply::raw(503, b"<html>maintenance</html>".to_vec())
        } else {
            Reply::raw(404, Vec::new())
        }
    })
    .await;
    let s = setup(&server, |c| c.max_retries = 1);
    let w = s.vies.check("EL", EL).await.unwrap_err();
    assert_eq!(w.kind, WarningKind::Status(503));
    assert_eq!(server.requests().len(), 2);

    let server = MockServer::start(|_, _| Reply::raw(404, Vec::new())).await;
    let s = setup(&server, |_| {});
    for _ in 0..2 {
        let w = s.vies.check("EL", EL).await.unwrap_err();
        assert_eq!(w.kind, WarningKind::Status(404));
    }
    // A 404 is not an answer about the number: not cached.
    assert_eq!(server.requests().len(), 2);
    assert!(s.cache.is_empty());
}

#[tokio::test]
async fn malformed_bodies_are_warnings_and_not_cached() {
    let number = de("21837465");
    let mut no_valid = answer("DE", &number, true, "x", "y");
    no_valid.as_object_mut().unwrap().remove("valid");
    let mut wrong_type = answer("DE", &number, true, "x", "y");
    wrong_type["valid"] = json!("yes");
    let other_country = answer("AT", &number, true, "x", "y");
    let other_number = answer("DE", &de("21837466"), true, "x", "y");
    let bodies: Vec<Vec<u8>> = vec![
        b"<html>maintenance</html>".to_vec(),
        br#"{"countryCode":"DE","vatNumber":"21837"#.to_vec(),
        b"[]".to_vec(),
        serde_json::to_vec(&no_valid).unwrap(),
        serde_json::to_vec(&wrong_type).unwrap(),
        serde_json::to_vec(&other_country).unwrap(),
        serde_json::to_vec(&other_number).unwrap(),
    ];
    for body in bodies {
        let shown = String::from_utf8_lossy(&body).into_owned();
        let server = MockServer::start(move |_, _| Reply::raw(200, body.clone())).await;
        let s = setup(&server, |_| {});
        for _ in 0..2 {
            let w = s.vies.check("DE", &number).await.unwrap_err();
            assert!(matches!(w.kind, WarningKind::Malformed(_)), "{shown}: {w}");
        }
        assert_eq!(server.requests().len(), 2, "{shown}");
        assert!(s.cache.is_empty(), "{shown}");
    }
}

#[tokio::test]
async fn oversize_body_is_capped_before_parsing() {
    let number = de("31827465");
    let big = serde_json::to_vec(&answer("DE", &number, true, &"N".repeat(4000), "x")).unwrap();
    for no_length in [false, true] {
        let body = big.clone();
        let server = MockServer::start(move |_, _| {
            let mut r = Reply::raw(200, body.clone());
            r.no_length = no_length;
            r
        })
        .await;
        let s = setup(&server, |c| c.max_body = 1024);
        let w = s.vies.check("DE", &number).await.unwrap_err();
        assert_eq!(w.kind, WarningKind::TooLarge { limit: 1024 }, "{no_length}");
        assert_eq!(server.requests().len(), 1);
        assert!(s.cache.is_empty());
    }
}

#[tokio::test]
async fn malformed_number_is_never_sent() {
    let server = MockServer::start(|_, _| Reply::raw(500, Vec::new())).await;
    let s = setup(&server, |_| {});
    let number = de("31745208");
    let broken = {
        let (head, last) = number.split_at(8);
        let d: u32 = last.parse().unwrap();
        format!("{head}{}", (d + 1) % 10)
    };
    for (country, n) in [
        ("DE", broken.as_str()),
        ("DE", &number[..8]),
        ("DE", ""),
        ("DE", "not a number"),
        // A placeholder is not a number.
        ("EL", "123456789"),
        ("EL", "00000000"),
        ("EL", &EL[..8]),
        // Not in VIES: Great Britain since 2021, Serbia, Switzerland; no country.
        ("GB", &xi("6283917")),
        ("RS", "108734521"),
        ("CH", "E123456788"),
        ("", EL),
        // A number of one country under another.
        ("AT", &number),
    ] {
        let w = s.vies.check(country, n).await.unwrap_err();
        assert!(
            matches!(w.kind, WarningKind::InvalidInput(_)),
            "{country} {n}: {w}"
        );
    }
    assert!(server.requests().is_empty(), "{:?}", server.requests());
}

#[tokio::test]
async fn greece_is_el_in_vies() {
    let body = answer("EL", EL, true, "Fiktiki Antlies AE", "Odos Paradeigmatos 1");
    let server = MockServer::start(move |_, _| Reply::json_status(200, &body)).await;
    let s = setup(&server, |_| {});
    let r = s.vies.check("GR", EL).await.unwrap().value;
    assert_eq!(r.vat.value, format!("EL{EL}"));
    assert_eq!(r.country(), "EL");
    assert_eq!(
        sent(&server),
        [json!({"countryCode": "EL", "vatNumber": EL})]
    );
    // The same number written with either prefix is the same check (one cache entry).
    for (country, n) in [("EL", format!("EL{EL}")), ("gr", format!("GR-{EL}"))] {
        let again = s.vies.check(country, &n).await.unwrap().value;
        assert_eq!(again, r);
    }
    assert_eq!(server.requests().len(), 1);
}

#[tokio::test]
async fn northern_ireland_is_xi() {
    let number = xi("6283917");
    let body = answer(
        "XI",
        &number,
        true,
        "Sample Linen Ltd",
        "1 Example Road\nBELFAST",
    );
    let server = MockServer::start(move |_, _| Reply::json_status(200, &body)).await;
    let s = setup(&server, |_| {});
    let r = s
        .vies
        .check("XI", &format!("XI {number}"))
        .await
        .unwrap()
        .value;
    assert!(r.valid);
    assert_eq!(r.country(), "XI");
    assert_eq!(
        sent(&server),
        [json!({"countryCode": "XI", "vatNumber": number})]
    );
}

#[tokio::test]
async fn disabled_sends_nothing() {
    let server = MockServer::start(|_, _| Reply::raw(500, Vec::new())).await;
    let s = setup(&server, |c| c.enabled = false);
    let w = s.vies.check("EL", EL).await.unwrap_err();
    assert_eq!(w.kind, WarningKind::Disabled);
    assert!(server.requests().is_empty());
    assert!(ViesConfig::default().enabled);
}

#[tokio::test]
async fn rate_limiter_spaces_requests() {
    let server = MockServer::start_with_body(|_, body, _| {
        let asked: Value = serde_json::from_slice(body).unwrap();
        let n = asked["vatNumber"].as_str().unwrap().to_owned();
        Reply::json_status(200, &answer("EE", &n, false, "---", "---"))
    })
    .await;
    let s = setup(&server, |_| {});
    for n in ["102948371", "102948372", "102948373", "102948374"] {
        assert!(!s.vies.check("EE", n).await.unwrap().value.valid);
    }
    // The default: a burst of 2, then one every 3 s (20 a minute).
    let sleeps = s.clock.sleeps();
    assert_eq!(sleeps.len(), 2, "{sleeps:?}");
    assert!(sleeps.iter().all(|d| close(*d, 3.0)), "{sleeps:?}");
}

#[test]
fn bad_base_url_is_a_config_warning() {
    for base in [
        "not a url",
        "ftp://example.invalid/",
        "mailto:x@example.invalid",
    ] {
        let config = ViesConfig {
            base_url: base.to_owned(),
            ..ViesConfig::default()
        };
        let w = Vies::new(&config, Arc::new(MemoryCache::default())).unwrap_err();
        assert!(matches!(w.kind, WarningKind::Config(_)), "{base}: {w}");
    }
}
