//! The GLEIF client against a mock server on 127.0.0.1, with hand-written fixtures. Nothing
//! here reaches the real API.

mod support;

use std::sync::Arc;
use std::time::{Duration, Instant};

use doewood_registry::gleif::{
    EntityStatus, EventKind, ExceptionReason, Gleif, GleifConfig, Parent, ParentKind,
    RelationshipStatus,
};
use doewood_registry::{Clock, HttpTransport, ManualClock, MemoryCache, WarningKind};
use serde_json::json;
use support::{MockServer, Reply, bad_lei, exception, lei, record, relationship};
use time::{Date, Month, OffsetDateTime};

const DAY: Duration = Duration::from_hours(24);

struct Setup {
    gleif: Gleif,
    clock: Arc<ManualClock>,
    cache: Arc<MemoryCache>,
}

fn setup(server: &MockServer, tweak: impl FnOnce(&mut GleifConfig)) -> Setup {
    let mut config = GleifConfig {
        base_url: server.base.clone(),
        ..GleifConfig::default()
    };
    tweak(&mut config);
    let clock = Arc::new(ManualClock::new(
        OffsetDateTime::from_unix_timestamp(1_790_000_000).unwrap(),
    ));
    let cache = Arc::new(MemoryCache::default());
    let transport = HttpTransport::new(config.timeout, config.connect_timeout).unwrap();
    let gleif = Gleif::with_parts(&config, Arc::new(transport), cache.clone(), clock.clone())
        .expect("client");
    Setup {
        gleif,
        clock,
        cache,
    }
}

fn date(y: i32, m: Month, d: u8) -> Date {
    Date::from_calendar_date(y, m, d).unwrap()
}

fn close(d: Duration, secs: f64) -> bool {
    (d.as_secs_f64() - secs).abs() < 1e-6
}

#[tokio::test]
async fn lookup_parses_a_record_with_successor_and_events() {
    let target = lei("PUMPS");
    let successor = lei("HOLDINGS");
    let mut r = record(&target, "Fictive Pumps d.o.o.", "rs");
    let entity = &mut r["attributes"]["entity"];
    entity["otherNames"] = json!([
        {"name": "Fiktivne Pumpe d.o.o.", "language": "sr", "type": "PREVIOUS_LEGAL_NAME"},
        {"name": "  ", "language": "sr", "type": "TRADING_OR_OPERATING_NAME"}
    ]);
    entity["status"] = json!("INACTIVE");
    entity["expiration"] = json!({"date": "2026-04-30T00:00:00Z", "reason": "CORPORATE_ACTION"});
    entity["successorEntity"] = json!({"lei": successor, "name": null});
    entity["successorEntities"] = json!([
        {"lei": successor, "name": null},
        {"lei": null, "name": "Invented Holdings AG"}
    ]);
    entity["eventGroups"] = json!([{
        "groupType": "STANDALONE",
        "events": [
            {"type": "CHANGE_LEGAL_NAME", "status": "COMPLETED",
                "effectiveDate": "2019-02-01T00:00:00Z", "recordedDate": "2019-02-03T00:00:00Z"},
            {"type": "MERGERS_AND_ACQUISITIONS", "status": "COMPLETED",
                "effectiveDate": "2026-03-31T00:00:00+02:00",
                "recordedDate": "2026-04-02T00:00:00Z"},
            {"type": "MERGERS_AND_ACQUISITIONS", "status": "WITHDRAWN_CANCELLED",
                "effectiveDate": "2026-06-01T00:00:00Z", "recordedDate": null},
            {"type": "CHANGE_LEGAL_NAME", "status": "IN_PROGRESS",
                "effectiveDate": "2026-07-01T00:00:00Z", "recordedDate": null},
            {"type": "CHANGE_LEGAL_NAME", "status": "WITHDRAWN_CANCELLED",
                "effectiveDate": "2025-07-01T00:00:00Z", "recordedDate": null}
        ]
    }]);
    let body = json!({"data": r});
    let server = MockServer::start(move |_, _| Reply::json(&body)).await;
    let s = setup(&server, |_| {});

    let got = s.gleif.lookup(&target).await.expect("lookup");
    assert_eq!(server.requests(), [format!("/api/v1/lei-records/{target}")]);
    assert_eq!(got.fetched_at, s.clock.now_utc());
    let rec = got.value.expect("a record");
    assert_eq!(rec.lei.value, target);
    assert_eq!(rec.legal_name, "Fictive Pumps d.o.o.");
    assert_eq!(rec.legal_country.as_deref(), Some("RS"));
    assert_eq!(rec.legal_city.as_deref(), Some("Exampletown"));
    assert_eq!(rec.registered_at.as_deref(), Some("RA999999"));
    assert_eq!(rec.registered_as.as_deref(), Some("TEST-0001"));
    assert_eq!(rec.legal_form.as_deref(), Some("XXXX"));
    assert_eq!(rec.entity_status, EntityStatus::Inactive);
    assert_eq!(rec.registration_status.as_deref(), Some("ISSUED"));
    assert_eq!(rec.last_update, Some(date(2026, Month::June, 1)));
    assert_eq!(rec.other_names.len(), 1);
    assert_eq!(
        rec.other_names[0].kind.as_deref(),
        Some("PREVIOUS_LEGAL_NAME")
    );
    // The same successor twice is one; a name-only successor is kept.
    assert_eq!(rec.successors.len(), 2, "{:?}", rec.successors);
    assert_eq!(rec.events.len(), 5);
    assert!(
        rec.events
            .iter()
            .all(|e| e.group_type.as_deref() == Some("STANDALONE"))
    );

    // The succession date is the completed merger's effective date, not the cancelled one's.
    let succ = rec.successions();
    assert_eq!(succ.len(), 2);
    assert_eq!(
        succ[0].successor.lei.as_ref().map(|l| l.value.as_str()),
        Some(successor.as_str())
    );
    assert_eq!(succ[0].event, Some(EventKind::MergerOrAcquisition));
    assert_eq!(succ[0].date, Some(date(2026, Month::March, 31)));
    assert_eq!(
        succ[1].successor.name.as_deref(),
        Some("Invented Holdings AG")
    );

    // Only the completed rename: the in-progress and the withdrawn ones are not renames.
    let renames: Vec<_> = rec.renames().collect();
    assert_eq!(renames.len(), 1);
    assert_eq!(renames[0].effective, Some(date(2019, Month::February, 1)));
}

#[tokio::test]
async fn documented_codes_placeholders_and_transliterations() {
    let target = lei("SPUN");
    let mut r = record(&target, "Fictive Spun Holdings plc", "GB");
    let entity = &mut r["attributes"]["entity"];
    // LEI-CDF 3.1: `9999` / `8888` legal forms carry the real form in `other`.
    entity["legalForm"] = json!({"id": "9999", "other": "LIMITED LIABILITY COMPANY"});
    entity["registeredAt"] = json!({"id": "RA888888", "other": "RA-INVENTED-01"});
    entity["category"] = json!("INTERNATIONAL_ORGANIZATION");
    entity["transliteratedOtherNames"] = json!([
        {"name": "FIKTIVNE PUMPE DOO", "language": "sr-Latn",
            "type": "PREFERRED_ASCII_TRANSLITERATED_LEGAL_NAME"},
        {"name": "FIKTIVNE PUMPE D.O.O.", "language": "sr-Latn",
            "type": "AUTO_ASCII_TRANSLITERATED_LEGAL_NAME"}
    ]);
    entity["status"] = json!("INACTIVE");
    entity["expiration"] = json!({"date": "2026-01-31T00:00:00Z", "reason": "CORPORATE_ACTION"});
    entity["successorEntity"] = json!({"lei": lei("TAKER"), "name": null});
    entity["eventGroups"] = json!([
        {"groupType": "STANDALONE", "events": [
            {"type": "SPINOFF", "status": "COMPLETED",
                "effectiveDate": "2024-03-01T00:00:00Z", "recordedDate": null},
            {"type": "VOLUNTARY_ARRANGEMENT", "status": "COMPLETED",
                "effectiveDate": "2024-06-01T00:00:00Z", "recordedDate": null}
        ]},
        {"groupType": "REVERSE_TAKEOVER", "events": [
            {"type": "ACQUISITION_BRANCH", "status": "COMPLETED",
                "effectiveDate": "2026-01-31T00:00:00Z", "recordedDate": null}
        ]}
    ]);
    r["attributes"]["registration"]["status"] = json!("RETIRED");
    let body = json!({"data": r});
    let server = MockServer::start(move |_, _| Reply::json(&body)).await;
    let s = setup(&server, |_| {});
    let rec = s.gleif.lookup(&target).await.unwrap().value.unwrap();
    assert_eq!(rec.legal_form.as_deref(), Some("LIMITED LIABILITY COMPANY"));
    assert_eq!(rec.registered_at.as_deref(), Some("RA888888"));
    assert_eq!(rec.registered_at_other.as_deref(), Some("RA-INVENTED-01"));
    assert_eq!(rec.category.as_deref(), Some("INTERNATIONAL_ORGANIZATION"));
    let kinds: Vec<_> = rec.other_names.iter().map(|n| n.kind.as_deref()).collect();
    assert_eq!(
        kinds,
        [
            Some("PREFERRED_ASCII_TRANSLITERATED_LEGAL_NAME"),
            Some("AUTO_ASCII_TRANSLITERATED_LEGAL_NAME")
        ]
    );
    let events: Vec<_> = rec.events.iter().map(|e| e.kind.clone()).collect();
    assert_eq!(
        events,
        [
            EventKind::SpinOff,
            EventKind::VoluntaryArrangement,
            EventKind::BranchAcquisition
        ]
    );
    assert!(!rec.events[0].is_reverse_takeover());
    assert!(rec.events[2].is_reverse_takeover());
    // RETIRED with CORPORATE_ACTION is the LEI-CDF 3.1 form of the deprecated MERGED.
    assert!(rec.is_merged());
    let succ = rec.successions();
    assert_eq!(succ.len(), 1);
    assert_eq!(succ[0].event, Some(EventKind::BranchAcquisition));
    assert_eq!(succ[0].date, Some(date(2026, Month::January, 31)));
}

#[tokio::test]
async fn succession_date_falls_back_to_the_expiration() {
    let target = lei("ABSORBED");
    let mut r = record(&target, "Sample Absorbed GmbH", "DE");
    r["attributes"]["entity"]["expiration"] =
        json!({"date": "2025-09-30T00:00:00Z", "reason": "CORPORATE_ACTION"});
    r["attributes"]["entity"]["successorEntity"] =
        json!({"lei": lei("ABSORBER"), "name": "Sample Absorber GmbH"});
    // An invalid successor LEI is dropped, its name kept.
    r["attributes"]["entity"]["successorEntities"] =
        json!([{"lei": bad_lei("OTHER"), "name": "Sample Other GmbH"}]);
    let body = json!({"data": r});
    let server = MockServer::start(move |_, _| Reply::json(&body)).await;
    let s = setup(&server, |_| {});
    let rec = s.gleif.lookup(&target).await.unwrap().value.unwrap();
    // ISSUED, not retired: not merged even with a corporate-action expiration.
    assert!(!rec.is_merged());
    let succ = rec.successions();
    assert_eq!(succ.len(), 2);
    assert!(
        succ.iter()
            .all(|x| x.date == Some(date(2025, Month::September, 30)))
    );
    assert!(succ.iter().all(|x| x.event.is_none()));
    assert!(succ[0].successor.lei.is_none());
    assert_eq!(succ[0].successor.name.as_deref(), Some("Sample Other GmbH"));
}

#[tokio::test]
async fn found_records_are_cached_for_30_days() {
    let target = lei("CACHED");
    let body = json!({"data": record(&target, "Fictive Cache Ltd", "GB")});
    let server = MockServer::start(move |_, _| Reply::json(&body)).await;
    let s = setup(&server, |_| {});
    let first = s.gleif.lookup(&target).await.unwrap();
    s.clock.advance(29 * DAY);
    let second = s.gleif.lookup(&target).await.unwrap();
    assert_eq!(server.requests().len(), 1);
    // The cached answer keeps its original fetch date.
    assert_eq!(second.fetched_at, first.fetched_at);
    assert_eq!(s.cache.len(), 1);
    s.clock.advance(2 * DAY);
    let third = s.gleif.lookup(&target).await.unwrap();
    assert_eq!(server.requests().len(), 2);
    assert!(third.fetched_at > first.fetched_at);
}

#[tokio::test]
async fn unknown_lei_is_none_and_the_miss_is_cached_for_a_day() {
    let server = MockServer::start(|_, _| Reply::not_found()).await;
    let s = setup(&server, |_| {});
    let target = lei("NOBODY");
    assert_eq!(s.gleif.lookup(&target).await.unwrap().value, None);
    assert_eq!(s.gleif.lookup(&target).await.unwrap().value, None);
    assert_eq!(server.requests().len(), 1);
    s.clock.advance(DAY + Duration::from_secs(1));
    assert_eq!(s.gleif.lookup(&target).await.unwrap().value, None);
    assert_eq!(server.requests().len(), 2);
}

#[tokio::test]
async fn invalid_lei_is_never_sent() {
    let server = MockServer::start(|_, _| Reply::not_found()).await;
    let s = setup(&server, |_| {});
    for bad in [
        bad_lei("PUMPS"),
        String::new(),
        "not an lei".to_owned(),
        lei("PUMPS")[..19].to_owned(),
        "00000000000000000000".to_owned(),
    ] {
        let w = s.gleif.lookup(&bad).await.unwrap_err();
        assert!(matches!(w.kind, WarningKind::InvalidInput(_)), "{bad}: {w}");
        let w = s.gleif.parent(&bad, ParentKind::Direct).await.unwrap_err();
        assert!(matches!(w.kind, WarningKind::InvalidInput(_)), "{bad}: {w}");
    }
    assert!(server.requests().is_empty(), "{:?}", server.requests());
    // A valid LEI in another spelling is compacted and sent uppercase.
    let spaced = lei("PUMPS").to_lowercase().replace("fake", "fake ");
    s.gleif.lookup(&spaced).await.unwrap();
    assert_eq!(
        server.requests(),
        [format!("/api/v1/lei-records/{}", lei("PUMPS"))]
    );
}

#[tokio::test]
async fn disabled_sends_nothing() {
    let server = MockServer::start(|_, _| Reply::not_found()).await;
    let s = setup(&server, |c| c.enabled = false);
    let w = s.gleif.lookup(&lei("PUMPS")).await.unwrap_err();
    assert_eq!(w.kind, WarningKind::Disabled);
    let w = s.gleif.search("Fictive Pumps", None).await.unwrap_err();
    assert_eq!(w.kind, WarningKind::Disabled);
    let w = s
        .gleif
        .parent(&lei("PUMPS"), ParentKind::Ultimate)
        .await
        .unwrap_err();
    assert_eq!(w.kind, WarningKind::Disabled);
    assert!(server.requests().is_empty());
    assert!(GleifConfig::default().enabled);
}

#[tokio::test]
async fn search_returns_candidates_and_skips_invalid_records() {
    let a = lei("SEARCHA");
    let b = lei("SEARCHB");
    let mut nameless = record(&lei("SEARCHC"), "x", "RS");
    nameless["attributes"]["entity"]["legalName"] = json!({"name": null});
    let body = json!({
        "meta": {"goldenCopy": {"publishDate": "2026-11-01T00:00:00Z"},
            "pagination": {"currentPage": 1, "perPage": 10, "from": 1, "to": 4,
                "total": 4, "lastPage": 1}},
        "links": {},
        "data": [
            record(&a, "Fictive Pumps d.o.o.", "RS"),
            record(&bad_lei("SEARCHX"), "Fictive Pumps Beograd d.o.o.", "RS"),
            nameless,
            record(&b, "Fictive Pumps Servis d.o.o.", "RS"),
        ]
    });
    let server = MockServer::start(move |target, _| {
        if target.contains("Nothing") {
            Reply::json(&json!({"meta": {}, "data": []}))
        } else {
            Reply::json(&body)
        }
    })
    .await;
    let s = setup(&server, |c| c.search_size = 5);
    let got = s
        .gleif
        .search("  Fictive   Pumps d.o.o. ", Some("rs"))
        .await
        .unwrap()
        .value;
    let leis: Vec<_> = got.iter().map(|r| r.lei.value.clone()).collect();
    assert_eq!(leis, [a, b]);
    let reqs = server.requests();
    assert_eq!(reqs.len(), 1);
    assert_eq!(
        reqs[0],
        "/api/v1/lei-records?filter%5Bentity.legalName%5D=Fictive+Pumps+d.o.o.\
         &filter%5Bentity.legalAddress.country%5D=RS&page%5Bsize%5D=5&page%5Bnumber%5D=1"
    );
    // Without a country, no country filter.
    let none = s.gleif.search("Nothing Here", None).await.unwrap().value;
    assert!(none.is_empty());
    assert!(!server.requests()[1].contains("country"));

    // Refused before sending.
    for (name, country) in [
        ("   ", None),
        ("Fictive", Some("SRB")),
        ("Fictive", Some("1A")),
    ] {
        let w = s.gleif.search(name, country).await.unwrap_err();
        assert!(matches!(w.kind, WarningKind::InvalidInput(_)), "{w}");
    }
    let w = s.gleif.search(&"x".repeat(300), None).await.unwrap_err();
    assert!(matches!(w.kind, WarningKind::InvalidInput(_)));
    assert_eq!(server.requests().len(), 2);
}

#[tokio::test]
async fn direct_parent_is_a_dated_relationship() {
    let child = lei("CHILD");
    let parent = lei("PARENT");
    let body = relationship(&child, &parent, "IS_DIRECTLY_CONSOLIDATED_BY", "2021-07-01");
    let server = MockServer::start(move |target, _| {
        if target.ends_with("/direct-parent-relationship") {
            Reply::json(&body)
        } else {
            Reply::not_found()
        }
    })
    .await;
    let s = setup(&server, |_| {});
    let got = s.gleif.parent(&child, ParentKind::Direct).await.unwrap();
    let Parent::Reported(r) = got.value else {
        panic!("{:?}", got.value)
    };
    assert_eq!(r.kind, ParentKind::Direct);
    assert_eq!(r.child.value, child);
    assert_eq!(r.parent.value, parent);
    assert_eq!(r.status, RelationshipStatus::Active);
    // The relationship period, not the accounting period.
    assert_eq!(r.valid_from, Some(date(2021, Month::July, 1)));
    assert_eq!(r.valid_to, None);
    assert_eq!(r.registration_status.as_deref(), Some("PUBLISHED"));
    assert_eq!(
        server.requests(),
        [format!(
            "/api/v1/lei-records/{child}/direct-parent-relationship"
        )]
    );
    // Cached.
    s.gleif.parent(&child, ParentKind::Direct).await.unwrap();
    assert_eq!(server.requests().len(), 1);
}

#[tokio::test]
async fn ultimate_parent_reporting_exception() {
    let child = lei("NATURAL");
    let body = exception(
        &child,
        "ULTIMATE_ACCOUNTING_CONSOLIDATION_PARENT",
        "NATURAL_PERSONS",
    );
    let server = MockServer::start(move |target, _| {
        if target.ends_with("/ultimate-parent-reporting-exception") {
            Reply::json(&body)
        } else {
            Reply::not_found()
        }
    })
    .await;
    let s = setup(&server, |_| {});
    let got = s.gleif.parent(&child, ParentKind::Ultimate).await.unwrap();
    let Parent::Exception(e) = got.value else {
        panic!("{:?}", got.value)
    };
    assert_eq!(e.kind, ParentKind::Ultimate);
    assert_eq!(e.lei.value, child);
    assert_eq!(e.reason, ExceptionReason::NaturalPersons);
    assert_eq!(e.code, "NATURAL_PERSONS");
    assert_eq!(
        server.requests(),
        [
            format!("/api/v1/lei-records/{child}/ultimate-parent-relationship"),
            format!("/api/v1/lei-records/{child}/ultimate-parent-reporting-exception"),
        ]
    );
}

#[tokio::test]
async fn non_public_and_non_consolidating_exceptions() {
    for (code, reason) in [
        ("NON_CONSOLIDATING", ExceptionReason::NonConsolidating),
        ("NO_KNOWN_PERSON", ExceptionReason::NoKnownPerson),
        ("NO_LEI", ExceptionReason::NoLei),
        ("NON_PUBLIC", ExceptionReason::NonPublic),
        ("LEGAL_OBSTACLES", ExceptionReason::NonPublic),
        ("A_FUTURE_CODE", ExceptionReason::Other),
    ] {
        let child = lei("EXC");
        let body = exception(&child, "DIRECT_ACCOUNTING_CONSOLIDATION_PARENT", code);
        let server = MockServer::start(move |target, _| {
            if target.ends_with("-reporting-exception") {
                Reply::json(&body)
            } else {
                Reply::not_found()
            }
        })
        .await;
        let s = setup(&server, |_| {});
        let got = s.gleif.parent(&child, ParentKind::Direct).await.unwrap();
        let Parent::Exception(e) = got.value else {
            panic!("{code}: {:?}", got.value)
        };
        assert_eq!(e.reason, reason, "{code}");
    }
}

#[tokio::test]
async fn no_lei_exception_means_a_parent_exists() {
    let child = lei("HIDDEN");
    let body = exception(&child, "DIRECT_ACCOUNTING_CONSOLIDATION_PARENT", "NO_LEI");
    let server = MockServer::start(move |target, _| {
        if target.ends_with("-reporting-exception") {
            Reply::json(&body)
        } else {
            Reply::not_found()
        }
    })
    .await;
    let s = setup(&server, |_| {});
    let got = s.gleif.parent(&child, ParentKind::Direct).await.unwrap();
    let Parent::Exception(e) = got.value else {
        panic!("{:?}", got.value)
    };
    assert_eq!(e.reason, ExceptionReason::NoLei);
    assert_eq!(e.reason.parent_exists(), Some(true));
}

#[tokio::test]
async fn exception_of_the_other_parent_kind_is_malformed() {
    let child = lei("WRONGCAT");
    let body = exception(
        &child,
        "ULTIMATE_ACCOUNTING_CONSOLIDATION_PARENT",
        "NATURAL_PERSONS",
    );
    let server = MockServer::start(move |target, _| {
        if target.ends_with("-reporting-exception") {
            Reply::json(&body)
        } else {
            Reply::not_found()
        }
    })
    .await;
    let s = setup(&server, |_| {});
    let w = s
        .gleif
        .parent(&child, ParentKind::Direct)
        .await
        .unwrap_err();
    assert!(matches!(w.kind, WarningKind::Malformed(_)), "{w}");
}

#[tokio::test]
async fn no_parent_and_no_exception_is_not_reported() {
    let server = MockServer::start(|_, _| Reply::not_found()).await;
    let s = setup(&server, |_| {});
    let got = s.gleif.parent(&lei("ORPHAN"), ParentKind::Direct).await;
    assert_eq!(got.unwrap().value, Parent::NotReported);
    assert_eq!(server.requests().len(), 2);
}

#[tokio::test]
async fn mismatched_answers_are_malformed() {
    let child = lei("ASKED");
    let other = lei("SOMEONE");
    let parent = lei("PARENT");
    let wrong_child = relationship(&other, &parent, "IS_DIRECTLY_CONSOLIDATED_BY", "2020-01-01");
    let wrong_type = relationship(
        &child,
        &parent,
        "IS_ULTIMATELY_CONSOLIDATED_BY",
        "2020-01-01",
    );
    let own_parent = relationship(&child, &child, "IS_DIRECTLY_CONSOLIDATED_BY", "2020-01-01");
    let other_record = json!({"data": record(&other, "Fictive Other Ltd", "GB")});
    for body in [wrong_child, wrong_type, own_parent] {
        let server = MockServer::start(move |_, _| Reply::json(&body)).await;
        let s = setup(&server, |_| {});
        let w = s
            .gleif
            .parent(&child, ParentKind::Direct)
            .await
            .unwrap_err();
        assert!(matches!(w.kind, WarningKind::Malformed(_)), "{w}");
    }
    let server = MockServer::start(move |_, _| Reply::json(&other_record)).await;
    let s = setup(&server, |_| {});
    let w = s.gleif.lookup(&child).await.unwrap_err();
    assert!(matches!(w.kind, WarningKind::Malformed(_)), "{w}");
}

#[tokio::test]
async fn malformed_json_is_a_warning_and_not_cached() {
    let server = MockServer::start(|target, _| {
        if target.ends_with("TRUNCATED") {
            Reply::raw(200, br#"{"data": {"type": "lei-records", "attri"#.to_vec())
        } else {
            Reply::raw(200, b"<html>maintenance</html>".to_vec())
        }
    })
    .await;
    let s = setup(&server, |_| {});
    let target = lei("BROKEN");
    for _ in 0..2 {
        let w = s.gleif.lookup(&target).await.unwrap_err();
        assert!(matches!(w.kind, WarningKind::Malformed(_)), "{w}");
    }
    // Not cached: both calls asked again.
    assert_eq!(server.requests().len(), 2);
    assert!(s.cache.is_empty());
    let w = s.gleif.search("Fictive", None).await.unwrap_err();
    assert!(matches!(w.kind, WarningKind::Malformed(_)), "{w}");
}

#[tokio::test]
async fn oversize_body_is_capped_before_parsing() {
    let big = {
        let mut r = record(&lei("BIG"), "Fictive Big Ltd", "GB");
        r["attributes"]["entity"]["otherNames"] =
            json!(vec![json!({"name": "y".repeat(100)}); 100]);
        serde_json::to_vec(&json!({"data": r})).unwrap()
    };
    assert!(big.len() > 8 * 1024);
    for no_length in [false, true] {
        let body = big.clone();
        let server = MockServer::start(move |_, _| {
            let mut r = Reply::raw(200, body.clone());
            r.no_length = no_length;
            r
        })
        .await;
        let s = setup(&server, |c| c.max_body = 4096);
        let w = s.gleif.lookup(&lei("BIG")).await.unwrap_err();
        assert_eq!(w.kind, WarningKind::TooLarge { limit: 4096 }, "{no_length}");
        // Not retried, not cached.
        assert_eq!(server.requests().len(), 1);
        assert!(s.cache.is_empty());
    }
}

#[tokio::test]
async fn rate_limited_429_honours_retry_after() {
    let target = lei("BUSY");
    let body = json!({"data": record(&target, "Fictive Busy Ltd", "IE")});
    let server = MockServer::start(move |_, n| {
        if n == 0 {
            Reply::raw(429, br#"{"errors":[{"status":"429"}]}"#.to_vec()).header("Retry-After", "7")
        } else {
            Reply::json(&body)
        }
    })
    .await;
    let s = setup(&server, |c| c.per_minute = 60);
    let rec = s.gleif.lookup(&target).await.unwrap().value.unwrap();
    assert_eq!(rec.legal_name, "Fictive Busy Ltd");
    assert_eq!(server.requests().len(), 2);
    // Waited the 7 s asked for; the drained bucket then had its token back.
    let sleeps = s.clock.sleeps();
    assert!(close(sleeps[0], 7.0), "{sleeps:?}");
    assert_eq!(sleeps.len(), 1, "{sleeps:?}");
}

#[tokio::test]
async fn too_long_retry_after_gives_up_at_once() {
    let server =
        MockServer::start(|_, _| Reply::raw(429, Vec::new()).header("Retry-After", "3600")).await;
    let s = setup(&server, |_| {});
    let w = s.gleif.lookup(&lei("BUSY")).await.unwrap_err();
    assert_eq!(
        w.kind,
        WarningKind::RateLimited {
            retry_after: Some(Duration::from_hours(1))
        }
    );
    assert_eq!(server.requests().len(), 1);
    assert!(s.clock.sleeps().is_empty());
}

#[tokio::test]
async fn retries_429_without_retry_after_and_5xx_with_backoff() {
    let busy = lei("BUSY");
    let server = MockServer::start(move |target, _| {
        if target.ends_with(&busy) {
            Reply::raw(429, Vec::new())
        } else {
            Reply::raw(503, b"unavailable".to_vec())
        }
    })
    .await;
    let s = setup(&server, |c| {
        c.max_retries = 2;
        c.per_minute = 6000;
        c.burst = 100;
    });
    let w = s.gleif.lookup(&lei("DOWN")).await.unwrap_err();
    assert_eq!(w.kind, WarningKind::Status(503));
    assert_eq!(server.requests().len(), 3);
    let sleeps = s.clock.sleeps();
    assert!(close(sleeps[0], 1.0) && close(sleeps[1], 2.0), "{sleeps:?}");

    let w = s.gleif.lookup(&lei("BUSY")).await.unwrap_err();
    assert_eq!(w.kind, WarningKind::RateLimited { retry_after: None });
    assert_eq!(server.requests().len(), 6);
}

#[tokio::test]
async fn other_statuses_are_warnings_without_retry() {
    let server = MockServer::start(|_, _| Reply::raw(400, b"{}".to_vec())).await;
    let s = setup(&server, |_| {});
    let w = s.gleif.lookup(&lei("BADREQ")).await.unwrap_err();
    assert_eq!(w.kind, WarningKind::Status(400));
    assert_eq!(server.requests().len(), 1);
}

#[tokio::test]
async fn slow_answer_times_out() {
    let server = MockServer::start(|_, _| {
        let mut r = Reply::not_found();
        r.delay = Duration::from_secs(3);
        r
    })
    .await;
    let s = setup(&server, |c| {
        c.timeout = Duration::from_millis(200);
        c.max_retries = 0;
    });
    let started = Instant::now();
    let w = s.gleif.lookup(&lei("SLOW")).await.unwrap_err();
    assert_eq!(w.kind, WarningKind::Timeout);
    assert!(
        started.elapsed() < Duration::from_secs(2),
        "{:?}",
        started.elapsed()
    );
}

#[tokio::test]
async fn connection_refused_is_a_transport_warning() {
    // A port nothing listens on: bind, note it, close.
    let addr = {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        l.local_addr().unwrap()
    };
    let config = GleifConfig {
        base_url: format!("http://{addr}/api/v1"),
        max_retries: 1,
        ..GleifConfig::default()
    };
    let clock = Arc::new(ManualClock::new(OffsetDateTime::UNIX_EPOCH));
    let transport = HttpTransport::new(config.timeout, config.connect_timeout).unwrap();
    let gleif = Gleif::with_parts(
        &config,
        Arc::new(transport),
        Arc::new(MemoryCache::default()),
        clock.clone(),
    )
    .unwrap();
    let w = gleif.lookup(&lei("NOWHERE")).await.unwrap_err();
    assert!(matches!(w.kind, WarningKind::Transport(_)), "{w}");
    assert_eq!(clock.sleeps().len(), 1);
}

#[tokio::test]
async fn rate_limiter_spaces_requests() {
    let server = MockServer::start(|_, _| Reply::not_found()).await;
    let s = setup(&server, |c| {
        c.per_minute = 30;
        c.burst = 2;
    });
    for tag in ["R1", "R2", "R3", "R4", "R5"] {
        s.gleif.lookup(&lei(tag)).await.unwrap();
    }
    assert_eq!(server.requests().len(), 5);
    // Two from the bucket, then one every 2 s (30 a minute).
    let sleeps = s.clock.sleeps();
    assert_eq!(sleeps.len(), 3, "{sleeps:?}");
    assert!(sleeps.iter().all(|d| close(*d, 2.0)), "{sleeps:?}");
    // Cache hits take no token.
    s.gleif.lookup(&lei("R1")).await.unwrap();
    assert_eq!(s.clock.sleeps().len(), 3);
}

#[test]
fn bad_base_url_is_a_config_warning() {
    for base in [
        "not a url",
        "ftp://example.invalid/",
        "mailto:x@example.invalid",
    ] {
        let config = GleifConfig {
            base_url: base.to_owned(),
            ..GleifConfig::default()
        };
        let w = Gleif::new(&config, Arc::new(MemoryCache::default())).unwrap_err();
        assert!(matches!(w.kind, WarningKind::Config(_)), "{base}: {w}");
    }
}
