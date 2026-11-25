//! The Companies House client against a mock server on 127.0.0.1, with hand-written fixtures.
//! Nothing here reaches Companies House: every company (numbers from 99000001 up), officer,
//! PSC and filing is invented, the people have obviously fake names, and the key is
//! `test-key`.

mod support;

use std::sync::Arc;
use std::time::Duration;

use doewood_registry::companies_house::{
    ApiKey, CompaniesHouse, CompaniesHouseConfig, DEFAULT_FILING_CATEGORIES, FilingEvent, PscKind,
    StatementKind, Status, TrustTier,
};
use doewood_registry::{
    Cache, Cached, Clock, HttpTransport, ManualClock, MemoryCache, Registry, USER_AGENT,
    WarningKind,
};
use serde_json::{Value, json};
use support::{MockServer, Reply};
use time::{Date, Month, OffsetDateTime};

const KEY: &str = "test-key";
/// `base64("test-key:")`: the key as the user name, an empty password.
const AUTH: &str = "Basic dGVzdC1rZXk6";
const P: &str = "/api/v1/";

struct Setup {
    ch: CompaniesHouse,
    clock: Arc<ManualClock>,
    cache: Arc<MemoryCache>,
}

fn setup(server: &MockServer, tweak: impl FnOnce(&mut CompaniesHouseConfig)) -> Setup {
    let mut config = CompaniesHouseConfig {
        api_key: Some(ApiKey::new(KEY).unwrap()),
        base_url: server.base.clone(),
        timeout: Duration::from_secs(5),
        ..CompaniesHouseConfig::default()
    };
    tweak(&mut config);
    let clock = Arc::new(ManualClock::new(
        OffsetDateTime::from_unix_timestamp(1_790_000_000).unwrap(),
    ));
    let cache = Arc::new(MemoryCache::default());
    let transport = HttpTransport::new(config.timeout, config.connect_timeout).unwrap();
    let ch = CompaniesHouse::with_parts(&config, Arc::new(transport), cache.clone(), clock.clone())
        .expect("client");
    Setup { ch, clock, cache }
}

fn day(y: i32, m: Month, d: u8) -> Option<Date> {
    Date::from_calendar_date(y, m, d).ok()
}

fn profile(number: &str, status: &str) -> Value {
    json!({
        "company_number": number,
        "company_name": "FICTIVE WIDGETS EXAMPLE LIMITED",
        "company_status": status,
        "company_status_detail": "active-proposal-to-strike-off",
        "type": "ltd",
        "jurisdiction": "england-wales",
        "date_of_creation": "2001-03-04",
        "previous_company_names": [
            {"name": "FICTIVE GADGETS EXAMPLE LIMITED", "effective_from": "2001-03-04",
                "ceased_on": "2015-06-30"},
            {"name": "", "effective_from": "2000-01-01", "ceased_on": "2000-02-01"}
        ],
        "sic_codes": ["99999", 12],
        "registered_office_address": {"premises": "Unit 9", "address_line_1": "1 Example Street",
            "locality": "Exampletown", "region": "Exampleshire", "postal_code": "ZZ99 9ZZ",
            "country": "England"},
        "has_insolvency_history": false,
        "has_charges": true,
        "links": {"self": format!("/company/{number}")}
    })
}

/// An invented natural person's officer record, with the fields that must be dropped.
fn person_officer(name: &str, role: &str, appointed: &str, resigned: Option<&str>) -> Value {
    json!({
        "name": name,
        "officer_role": role,
        "appointed_on": appointed,
        "resigned_on": resigned,
        "nationality": "Fictionian",
        "country_of_residence": "Exampleland",
        "occupation": "Placeholder Occupation",
        "date_of_birth": {"month": 7, "year": 1911},
        "address": {"premises": "77", "address_line_1": "Secret Example Lane",
            "locality": "Hiddenville", "postal_code": "ZZ1 1ZZ"},
        "name_elements": {"forename": "Testina", "surname": "EXAMPLETON"},
        "links": {"officer": {"appointments": "/officers/FAKEOFFICERID000001/appointments"}},
        "identification": {"identification_type": "eea", "registration_number": "NOTKEPT1"}
    })
}

fn officers_page(start: usize, items: &[Value]) -> Value {
    json!({
        "items": items,
        "items_per_page": 2,
        "start_index": start,
        "total_results": 3,
        "active_count": 2,
        "resigned_count": 1,
        "kind": "officer-list"
    })
}

fn pscs() -> Value {
    json!({
        "items": [
            {
                "kind": "individual-person-with-significant-control",
                "name": "Mx Testina Fictiva EXAMPLETON",
                "name_elements": {"forename": "Testina", "surname": "EXAMPLETON"},
                "notified_on": "2016-04-06",
                "nationality": "Fictionian",
                "country_of_residence": "Exampleland",
                "natures_of_control": ["ownership-of-shares-25-to-50-percent",
                    "voting-rights-25-to-50-percent"],
                "date_of_birth": {"month": 7, "year": 1911},
                "address": {"address_line_1": "Secret Example Lane", "locality": "Hiddenville"},
                "identification": {"registration_number": "NOTKEPT2"},
                "etag": "fakeetag", "links": {"self": "/x"}
            },
            {
                "kind": "corporate-entity-person-with-significant-control",
                "name": "FICTIVE PARENT EXAMPLE PLC",
                "notified_on": "2018-01-02",
                "ceased_on": "2024-05-06",
                "natures_of_control": ["ownership-of-shares-75-to-100-percent"],
                "address": {"address_line_1": "2 Example Street", "locality": "Exampletown"},
                "identification": {
                    "identification_type": "uk-limited-company",
                    "legal_authority": "Companies Act 2006",
                    "legal_form": "Public Limited Company",
                    "place_registered": "Register Of Companies For England And Wales",
                    "country_registered": "England",
                    "registration_number": "99000002"
                }
            },
            {
                "kind": "legal-person-person-with-significant-control",
                "name": "The Example Authority",
                "notified_on": "2019-09-09",
                "natures_of_control": ["right-to-appoint-and-remove-directors"],
                "identification": {"legal_authority": "Example Act", "legal_form": "Authority"}
            },
            {
                "kind": "super-secure-person-with-significant-control",
                "description": "super-secure-persons-with-significant-control",
                "name": "should not be kept",
                "ceased": false
            },
            {"kind": "individual-beneficial-owner", "name": "Mx Placeholder SAMPLEWORTH",
                "natures_of_control": ["voting-rights-more-than-25-percent-registered-overseas-entity"]},
            {"name": "no kind"},
            "not an object"
        ],
        "total_results": 7,
        "active_count": 5,
        "ceased_count": 1
    })
}

fn filings(start: usize) -> Value {
    let items = if start == 0 {
        json!([
            {"category": "miscellaneous", "type": "OC138", "description": "scheme-of-arrangement",
                "date": "2025-09-01", "transaction_id": "FAKETX0001"},
            {"category": "officers", "type": "AP01",
                "description": "appoint-person-director-company-with-name-date",
                "description_values": {"officer_name": "Mx Testina Fictiva EXAMPLETON",
                    "appointment_date": "2024-01-01"},
                "date": "2024-01-05", "transaction_id": "FAKETX0002"}
        ])
    } else {
        json!([
            {"category": "change-of-name", "type": "CERTNM",
                "description": "certificate-change-of-name-company",
                "description_values": {"officer_name": "not kept"},
                "date": "2015-06-30", "action_date": "2015-06-29", "transaction_id": "FAKETX0003"},
            {"category": "miscellaneous", "type": "CB01", "description": "legacy",
                "description_values": {"description": "Notice of cross border merger"},
                "date": "2012-02-03", "transaction_id": "FAKETX0004"}
        ])
    };
    // The filing history sends `total_count`, not `total_results`.
    json!({"items": items, "items_per_page": 2, "start_index": start, "total_count": 4,
        "filing_history_status": "filing-history-available"})
}

fn statements() -> Value {
    json!({
        "items": [
            {"kind": "persons-with-significant-control-statement",
                "statement": "psc-exists-but-not-identified",
                "notified_on": "2017-03-01", "etag": "fakeetag",
                "links": {"self": "/company/99000001/persons-with-significant-control-statements/FAKE1"}},
            {"kind": "persons-with-significant-control-statement",
                "statement": "psc-has-failed-to-confirm-changed-details",
                "linked_psc_name": "Mx Testina Fictiva EXAMPLETON",
                "notified_on": "2018-04-05", "ceased_on": "2019-01-02", "etag": "fakeetag"},
            {"kind": "persons-with-significant-control-statement",
                "statement": "no-individual-or-entity-with-signficant-control",
                "notified_on": "2016-04-06", "ceased_on": "2017-03-01"},
            {"kind": "persons-with-significant-control-statement", "statement": ""}
        ],
        "items_per_page": 25, "start_index": 0, "total_results": 4,
        "active_count": 1, "ceased_count": 2,
        "links": {"self": "/company/99000001/persons-with-significant-control-statements"}
    })
}

fn search_hits() -> Value {
    json!({
        "items": [
            {"kind": "searchresults#company", "company_number": "99000001",
                "title": "FICTIVE WIDGETS EXAMPLE LIMITED", "company_status": "active",
                "company_type": "ltd", "date_of_creation": "2001-03-04",
                "address_snippet": "1 Example Street, Exampletown, ZZ99 9ZZ"},
            {"company_number": "SC990003", "title": "FICTIVE WIDGETS EXAMPLE (SCOTLAND) LTD",
                "company_status": "dissolved", "date_of_cessation": "2020-01-01"},
            {"company_number": "not-a-number", "title": "BROKEN EXAMPLE"},
            {"company_number": "99000004"}
        ],
        "total_results": 4, "items_per_page": 10, "start_index": 0,
        "kind": "search#companies"
    })
}

async fn registry() -> MockServer {
    MockServer::start(|target, _| {
        let path = target.split('?').next().unwrap_or("");
        let start: usize = target
            .split("start_index=")
            .nth(1)
            .and_then(|s| s.split('&').next())
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);
        match path.strip_prefix(P).unwrap_or("") {
            "company/99000001" => Reply::json(&profile("99000001", "active")),
            "company/99000001/officers" if start == 0 => Reply::json(&officers_page(
                0,
                &[
                    person_officer(
                        "EXAMPLETON, Testina Fictiva",
                        "director",
                        "2016-01-01",
                        None,
                    ),
                    json!({
                        "name": "FICTIVE SECRETARIES EXAMPLE LIMITED",
                        "officer_role": "corporate-secretary",
                        "appointed_on": "2017-02-03",
                        "address": {"address_line_1": "3 Example Street"},
                        "identification": {"identification_type": "uk-limited-company",
                            "registration_number": "99000005",
                            "place_registered": "Companies House"}
                    }),
                ],
            )),
            "company/99000001/officers" if start == 2 => Reply::json(&officers_page(
                2,
                &[{
                    // A pre-1992 appointment: `appointed_before`, no `appointed_on`.
                    let mut o = person_officer(
                        "SAMPLEWORTH, Placeholder",
                        "secretary",
                        "1991-01-01",
                        Some("2016-01-01"),
                    );
                    o["appointed_on"] = Value::Null;
                    o["appointed_before"] = json!("1991-12-31");
                    o["is_pre_1992_appointment"] = json!(true);
                    o
                }],
            )),
            "company/99000001/persons-with-significant-control" => Reply::json(&pscs()),
            "company/99000001/persons-with-significant-control-statements" => {
                Reply::json(&statements())
            }
            "company/99000001/filing-history" => Reply::json(&filings(start)),
            "search/companies" => Reply::json(&search_hits()),
            _ => Reply::raw(
                404,
                br#"{"errors":[{"error":"company-profile-not-found"}]}"#.to_vec(),
            ),
        }
    })
    .await
}

/// Every cached body, as text.
async fn cached_text(cache: &MemoryCache, keys: &[String]) -> String {
    let mut all = String::new();
    for k in keys {
        if let Some(e) = cache.get(&format!("companies_house:{k}")).await
            && let Cached::Found(b) = e.value
        {
            all.push_str(&String::from_utf8_lossy(&b));
        }
    }
    all
}

fn assert_no_personal_extras(text: &str) {
    for banned in [
        "date_of_birth",
        "1911",
        "Secret Example Lane",
        "Hiddenville",
        "address",
        "occupation",
        "Placeholder Occupation",
        "name_elements",
        "FAKEOFFICERID",
        "NOTKEPT",
        "should not be kept",
    ] {
        assert!(!text.contains(banned), "{banned:?} kept in {text}");
    }
}

#[tokio::test]
async fn company_profile() {
    let server = registry().await;
    let s = setup(&server, |_| {});
    let got = s.ch.company(" 9900 0001 ").await.unwrap();
    let c = got.value.unwrap();
    assert_eq!(c.number.as_str(), "99000001");
    assert_eq!(c.name, "FICTIVE WIDGETS EXAMPLE LIMITED");
    assert_eq!(c.status, Status::Active);
    assert!(c.status.is_live());
    assert_eq!(
        c.status_detail.as_deref(),
        Some("active-proposal-to-strike-off")
    );
    assert_eq!(c.company_type.as_deref(), Some("ltd"));
    assert_eq!(c.jurisdiction.as_deref(), Some("england-wales"));
    assert_eq!(c.created, day(2001, Month::March, 4));
    assert_eq!(c.ceased, None);
    assert_eq!(c.previous_names.len(), 1);
    assert_eq!(c.previous_names[0].name, "FICTIVE GADGETS EXAMPLE LIMITED");
    assert_eq!(c.previous_names[0].to, day(2015, Month::June, 30));
    assert_eq!(c.sic_codes, ["99999"]);
    assert!(!c.is_conversion());
    let office = c.registered_office.unwrap();
    // The registered office keeps its premises and region (a company's address only).
    assert_eq!(office.lines, ["Unit 9", "1 Example Street"]);
    assert_eq!(office.region.as_deref(), Some("Exampleshire"));
    assert_eq!(office.postal_code.as_deref(), Some("ZZ99 9ZZ"));
    assert_eq!(c.has_insolvency_history, Some(false));
    assert_eq!(c.tier, TrustTier::Official);
    assert_eq!(Registry::CompaniesHouse.tier(), TrustTier::Official);
    assert_eq!(got.fetched_at, s.clock.now_utc());

    assert_eq!(server.requests(), [format!("{P}company/99000001")]);
    // The key is the basic-auth user name, the password empty; the generic User-Agent.
    assert_eq!(
        server.header_values("authorization"),
        [Some(AUTH.to_owned())]
    );
    assert_eq!(
        server.header_values("user-agent"),
        [Some(USER_AGENT.to_owned())]
    );

    // Cached for 30 days.
    s.clock.advance(Duration::from_hours(29 * 24));
    assert!(s.ch.company("99000001").await.unwrap().value.is_some());
    assert_eq!(server.requests().len(), 1);
    s.clock.advance(Duration::from_hours(2 * 24));
    assert!(s.ch.company("99000001").await.unwrap().value.is_some());
    assert_eq!(server.requests().len(), 2);
}

#[tokio::test]
async fn statuses() {
    let server = MockServer::start(|target, _| {
        let number = target.rsplit('/').next().unwrap().to_owned();
        let status = match &number[number.len() - 2..] {
            "01" => "active",
            "02" => "dissolved",
            "03" => "liquidation",
            "04" => "receivership",
            "05" => "administration",
            "06" => "voluntary-arrangement",
            "07" => "converted-closed",
            "08" => "insolvency-proceedings",
            "09" => "registered",
            "10" => "removed",
            "11" => "open",
            "12" => "closed",
            "13" => "something-new",
            _ => "",
        };
        Reply::json(&profile(&number, status))
    })
    .await;
    let s = setup(&server, |_| {});
    let want = [
        Status::Active,
        Status::Dissolved,
        Status::Liquidation,
        Status::Receivership,
        Status::Administration,
        Status::VoluntaryArrangement,
        Status::ConvertedClosed,
        Status::InsolvencyProceedings,
        Status::Registered,
        Status::Removed,
        Status::Open,
        Status::Closed,
        Status::Other("something-new".to_owned()),
        Status::Unknown,
    ];
    for (i, status) in want.into_iter().enumerate() {
        let number = format!("990000{:02}", i + 1);
        let c = s.ch.company(&number).await.unwrap().value.unwrap();
        assert_eq!(c.status, status, "{number}");
    }
    assert!(!Status::Dissolved.is_live());
    assert!(Status::Liquidation.is_live());
}

#[tokio::test]
async fn unknown_company_and_answers_about_another() {
    let server = MockServer::start(|target, _| {
        if target.ends_with("99000002") {
            Reply::json(&profile("99000003", "active"))
        } else if target.ends_with("99000004") {
            Reply::raw(200, b"<html>not json</html>".to_vec())
        } else if target.ends_with("99000005") {
            Reply::json(&json!({"company_number": "99000005"}))
        } else {
            Reply::raw(
                404,
                br#"{"errors":[{"error":"company-profile-not-found"}]}"#.to_vec(),
            )
        }
    })
    .await;
    let s = setup(&server, |_| {});
    assert_eq!(s.ch.company("99000009").await.unwrap().value, None);
    // A 404 is cached for a day.
    assert_eq!(s.ch.company("99000009").await.unwrap().value, None);
    assert_eq!(server.requests().len(), 1);
    s.clock.advance(Duration::from_hours(25));
    assert_eq!(s.ch.company("99000009").await.unwrap().value, None);
    assert_eq!(server.requests().len(), 2);

    for n in ["99000002", "99000004", "99000005"] {
        let w = s.ch.company(n).await.unwrap_err();
        assert!(matches!(w.kind, WarningKind::Malformed(_)), "{n}: {w}");
        // Never cached: asked again.
        let before = server.requests().len();
        s.ch.company(n).await.unwrap_err();
        assert_eq!(server.requests().len(), before + 1);
    }
}

#[tokio::test]
async fn officers_every_page_without_personal_extras() {
    let server = registry().await;
    let s = setup(&server, |c| c.page_size = 2);
    let got = s.ch.officers("99000001").await.unwrap();
    let list = got.value;
    assert_eq!(list.total, Some(3));
    assert!(!list.truncated);
    assert_eq!(list.items.len(), 3);
    assert_eq!(
        server.requests(),
        [
            format!("{P}company/99000001/officers?items_per_page=2&start_index=0"),
            format!("{P}company/99000001/officers?items_per_page=2&start_index=2"),
        ]
    );
    assert!(
        server
            .header_values("authorization")
            .iter()
            .all(|a| a.as_deref() == Some(AUTH))
    );

    let d = &list.items[0];
    assert_eq!(d.name, "EXAMPLETON, Testina Fictiva");
    assert_eq!(d.role.as_deref(), Some("director"));
    assert!(!d.corporate);
    assert_eq!(d.appointed, day(2016, Month::January, 1));
    assert!(!d.appointed_before);
    assert_eq!(d.resigned, None);
    assert_eq!(d.nationality.as_deref(), Some("Fictionian"));
    assert_eq!(d.country_of_residence.as_deref(), Some("Exampleland"));
    assert_eq!(d.registration, None);

    let corp = &list.items[1];
    assert!(corp.corporate);
    let reg = corp.registration.as_ref().unwrap();
    assert_eq!(reg.number.as_deref(), Some("99000005"));
    assert_eq!(reg.kind.as_deref(), Some("uk-limited-company"));

    let resigned = &list.items[2];
    assert_eq!(resigned.resigned, day(2016, Month::January, 1));
    assert_eq!(resigned.appointed, day(1991, Month::December, 31));
    assert!(resigned.appointed_before);

    // Dropped when read, and never cached.
    assert_no_personal_extras(&format!("{list:?}"));
    let keys = [
        "company/99000001/officers?items_per_page=2&start_index=0".to_owned(),
        "company/99000001/officers?items_per_page=2&start_index=2".to_owned(),
    ];
    let cached = cached_text(&s.cache, &keys).await;
    assert!(cached.contains("EXAMPLETON"), "{cached}");
    assert_no_personal_extras(&cached);

    // From the cache: the same list, no request.
    let again = s.ch.officers("99000001").await.unwrap();
    assert_eq!(again.value, list);
    assert_eq!(again.fetched_at, got.fetched_at);
    assert_eq!(server.requests().len(), 2);
}

#[tokio::test]
async fn officers_cap_truncates() {
    let server = registry().await;
    let s = setup(&server, |c| {
        c.page_size = 2;
        c.max_items = 2;
    });
    let list = s.ch.officers("99000001").await.unwrap().value;
    assert_eq!(list.items.len(), 2);
    assert!(list.truncated);
    assert_eq!(server.requests().len(), 1);

    // A cap below one page cuts the page too.
    let s = setup(&server, |c| {
        c.page_size = 2;
        c.max_items = 1;
    });
    let list = s.ch.officers("99000001").await.unwrap().value;
    assert_eq!(list.items.len(), 1);
    assert!(list.truncated);
}

#[tokio::test]
async fn psc_kinds() {
    let server = registry().await;
    let s = setup(&server, |_| {});
    let list = s.ch.significant_control("99000001").await.unwrap().value;
    assert_eq!(
        server.requests(),
        [format!(
            "{P}company/99000001/persons-with-significant-control?items_per_page=100&start_index=0"
        )]
    );
    assert_eq!(list.items.len(), 5, "{list:?}");
    assert_eq!(list.total, Some(7));
    // Seven raw items were read; the total is reached.
    assert!(!list.truncated);

    let person = &list.items[0];
    assert_eq!(person.kind, PscKind::Individual);
    assert!(person.kind.is_natural_person());
    assert_eq!(
        person.name.as_deref(),
        Some("Mx Testina Fictiva EXAMPLETON")
    );
    assert_eq!(person.notified, day(2016, Month::April, 6));
    assert_eq!(person.nationality.as_deref(), Some("Fictionian"));
    assert_eq!(person.country_of_residence.as_deref(), Some("Exampleland"));
    assert_eq!(
        person.natures_of_control,
        [
            "ownership-of-shares-25-to-50-percent",
            "voting-rights-25-to-50-percent"
        ]
    );
    assert_eq!(person.registration, None);

    let corp = &list.items[1];
    assert_eq!(corp.kind, PscKind::CorporateEntity);
    assert!(!corp.kind.is_natural_person());
    assert_eq!(corp.ceased, day(2024, Month::May, 6));
    let reg = corp.registration.as_ref().unwrap();
    assert_eq!(reg.number.as_deref(), Some("99000002"));
    assert_eq!(reg.country.as_deref(), Some("England"));
    assert_eq!(reg.legal_form.as_deref(), Some("Public Limited Company"));
    assert_eq!(
        reg.place_registered.as_deref(),
        Some("Register Of Companies For England And Wales")
    );

    let legal = &list.items[2];
    assert_eq!(legal.kind, PscKind::LegalPerson);
    assert_eq!(
        legal
            .registration
            .as_ref()
            .unwrap()
            .legal_authority
            .as_deref(),
        Some("Example Act")
    );

    let secure = &list.items[3];
    assert_eq!(secure.kind, PscKind::SuperSecure);
    assert_eq!(secure.name, None);

    let owner = &list.items[4];
    assert_eq!(owner.kind, PscKind::Individual);
    assert!(owner.beneficial_owner);

    assert_no_personal_extras(&format!("{list:?}"));
    let cached = cached_text(
        &s.cache,
        &[
            "company/99000001/persons-with-significant-control?items_per_page=100&start_index=0"
                .to_owned(),
        ],
    )
    .await;
    assert!(cached.contains("99000002"), "{cached}");
    assert_no_personal_extras(&cached);
    let again = s.ch.significant_control("99000001").await.unwrap().value;
    assert_eq!(again, list);
    assert_eq!(server.requests().len(), 1);
}

#[tokio::test]
async fn filing_history_name_changes_and_mergers() {
    let server = registry().await;
    let s = setup(&server, |c| c.page_size = 2);
    let list = s.ch.filing_events("99000001").await.unwrap().value;
    // `total_count` is read: the second page ends the list, no third (empty) request.
    let filter = "category=change-of-name%2Cincorporation%2Cresolution%2Cmiscellaneous";
    assert_eq!(
        server.requests(),
        [
            format!("{P}company/99000001/filing-history?{filter}&items_per_page=2&start_index=0"),
            format!("{P}company/99000001/filing-history?{filter}&items_per_page=2&start_index=2"),
        ]
    );
    assert_eq!(list.total, Some(4));
    assert_eq!(list.items.len(), 3, "{list:?}");
    let scheme = &list.items[0];
    assert_eq!(scheme.event, FilingEvent::Merger);
    assert_eq!(scheme.description.as_deref(), Some("scheme-of-arrangement"));
    let rename = &list.items[1];
    assert_eq!(rename.event, FilingEvent::NameChange);
    assert_eq!(rename.form.as_deref(), Some("CERTNM"));
    assert_eq!(rename.date, day(2015, Month::June, 30));
    assert_eq!(rename.action_date, day(2015, Month::June, 29));
    assert_eq!(rename.text, None);
    let merger = &list.items[2];
    assert_eq!(merger.event, FilingEvent::Merger);
    assert_eq!(merger.form.as_deref(), Some("CB01"));
    assert_eq!(merger.date, day(2012, Month::February, 3));
    assert_eq!(
        merger.text.as_deref(),
        Some("Notice of cross border merger")
    );

    // Officer names in other filings are neither returned nor cached.
    let keys = [
        format!("company/99000001/filing-history?{filter}&items_per_page=2&start_index=0"),
        format!("company/99000001/filing-history?{filter}&items_per_page=2&start_index=2"),
    ];
    let cached = cached_text(&s.cache, &keys).await;
    assert!(cached.contains("CERTNM"), "{cached}");
    for banned in ["EXAMPLETON", "not kept", "officer_name", "AP01"] {
        assert!(!cached.contains(banned), "{banned} in {cached}");
    }
    let again = s.ch.filing_events("99000001").await.unwrap().value;
    assert_eq!(again, list);
    assert_eq!(server.requests().len(), 2);
}

#[tokio::test]
async fn filing_history_without_a_category_filter() {
    let server = registry().await;
    let s = setup(&server, |c| {
        c.page_size = 2;
        c.filing_categories = Vec::new();
    });
    let list = s.ch.filing_events("99000001").await.unwrap().value;
    assert_eq!(list.items.len(), 3);
    assert!(
        server.requests().iter().all(|r| !r.contains("category")),
        "{:?}",
        server.requests()
    );
    assert_eq!(
        CompaniesHouseConfig::default().filing_categories,
        DEFAULT_FILING_CATEGORIES
    );
}

#[tokio::test]
async fn psc_statements_tell_no_psc_from_unidentified() {
    let server = registry().await;
    let s = setup(&server, |_| {});
    let list = s.ch.psc_statements("99000001").await.unwrap().value;
    assert_eq!(
        server.requests(),
        [format!(
            "{P}company/99000001/persons-with-significant-control-statements\
             ?items_per_page=100&start_index=0"
        )]
    );
    assert_eq!(list.total, Some(4));
    // The statement without a text is skipped.
    assert_eq!(list.items.len(), 3, "{list:?}");
    assert_eq!(list.items[0].kind, StatementKind::Unidentified);
    assert_eq!(list.items[0].statement, "psc-exists-but-not-identified");
    assert_eq!(list.items[0].notified, day(2017, Month::March, 1));
    assert_eq!(list.items[0].ceased, None);
    assert_eq!(list.items[1].kind, StatementKind::Unidentified);
    assert_eq!(list.items[2].kind, StatementKind::NoPsc);
    assert_eq!(list.items[2].ceased, day(2017, Month::March, 1));

    // The linked PSC's name is neither returned nor cached.
    assert!(!format!("{list:?}").contains("EXAMPLETON"));
    let keys = [
        "company/99000001/persons-with-significant-control-statements\
         ?items_per_page=100&start_index=0"
            .to_owned(),
    ];
    let cached = cached_text(&s.cache, &keys).await;
    assert!(cached.contains("psc-exists-but-not-identified"), "{cached}");
    for banned in ["EXAMPLETON", "linked_psc_name", "etag", "links"] {
        assert!(!cached.contains(banned), "{banned} in {cached}");
    }
    let again = s.ch.psc_statements("99000001").await.unwrap().value;
    assert_eq!(again, list);
    assert_eq!(server.requests().len(), 1);
}

#[tokio::test]
async fn lists_of_an_unknown_company_are_empty() {
    let server = registry().await;
    let s = setup(&server, |_| {});
    let o = s.ch.officers("99000009").await.unwrap().value;
    assert!(o.items.is_empty() && !o.truncated);
    let p = s.ch.significant_control("99000009").await.unwrap().value;
    assert!(p.items.is_empty());
    let f = s.ch.filing_events("99000009").await.unwrap().value;
    assert!(f.items.is_empty());
    let st = s.ch.psc_statements("99000009").await.unwrap().value;
    assert!(st.items.is_empty());
    assert_eq!(server.requests().len(), 4);
}

#[tokio::test]
async fn name_search_gives_candidates() {
    let server = registry().await;
    let s = setup(&server, |_| {});
    let hits =
        s.ch.search("  Fictive   Widgets & Co ")
            .await
            .unwrap()
            .value;
    assert_eq!(
        server.requests(),
        [format!(
            "{P}search/companies?q=Fictive+Widgets+%26+Co&items_per_page=10&start_index=0"
        )]
    );
    assert_eq!(hits.len(), 2);
    assert_eq!(hits[0].number.as_str(), "99000001");
    assert_eq!(hits[0].status, Status::Active);
    assert_eq!(
        hits[0].address.as_deref(),
        Some("1 Example Street, Exampletown, ZZ99 9ZZ")
    );
    assert_eq!(hits[1].number.as_str(), "SC990003");
    assert_eq!(hits[1].status, Status::Dissolved);
    assert_eq!(hits[1].ceased, day(2020, Month::January, 1));
    assert_eq!(hits[1].tier, TrustTier::Official);
    // Cached.
    s.ch.search("Fictive Widgets & Co").await.unwrap();
    assert_eq!(server.requests().len(), 1);

    for bad in [
        String::new(),
        "   ".to_owned(),
        "x".repeat(257),
        "a\u{7}b".to_owned(),
    ] {
        let w = s.ch.search(&bad).await.unwrap_err();
        assert!(matches!(w.kind, WarningKind::InvalidInput(_)), "{w}");
    }
    assert_eq!(server.requests().len(), 1);
}

#[tokio::test]
async fn without_a_key_nothing_is_sent() {
    let server = registry().await;
    let s = setup(&server, |c| c.api_key = None);
    assert_eq!(
        s.ch.company("99000001").await.unwrap_err().kind,
        WarningKind::NotConfigured
    );
    assert_eq!(
        s.ch.officers("99000001").await.unwrap_err().kind,
        WarningKind::NotConfigured
    );
    assert_eq!(
        s.ch.significant_control("99000001").await.unwrap_err().kind,
        WarningKind::NotConfigured
    );
    assert_eq!(
        s.ch.filing_events("99000001").await.unwrap_err().kind,
        WarningKind::NotConfigured
    );
    assert_eq!(
        s.ch.search("Fictive").await.unwrap_err().kind,
        WarningKind::NotConfigured
    );
    assert!(CompaniesHouseConfig::default().api_key.is_none());

    let off = setup(&server, |c| c.enabled = false);
    assert_eq!(
        off.ch.company("99000001").await.unwrap_err().kind,
        WarningKind::Disabled
    );
    assert!(server.requests().is_empty());
}

#[tokio::test]
async fn invalid_numbers_are_never_sent() {
    let server = registry().await;
    let s = setup(&server, |_| {});
    for bad in [
        "",
        "00000000",
        "990000011",
        "../search",
        "99000001/officers",
        "9900?001",
        "S9900001",
        "99 00 00 01 1",
    ] {
        let w = s.ch.company(bad).await.unwrap_err();
        assert!(matches!(w.kind, WarningKind::InvalidInput(_)), "{bad}: {w}");
        let w = s.ch.officers(bad).await.unwrap_err();
        assert!(matches!(w.kind, WarningKind::InvalidInput(_)), "{bad}: {w}");
        let w = s.ch.significant_control(bad).await.unwrap_err();
        assert!(matches!(w.kind, WarningKind::InvalidInput(_)), "{bad}: {w}");
        let w = s.ch.filing_events(bad).await.unwrap_err();
        assert!(matches!(w.kind, WarningKind::InvalidInput(_)), "{bad}: {w}");
    }
    assert!(server.requests().is_empty());
}

#[tokio::test]
async fn a_refused_key_pauses_and_is_redacted() {
    let server = MockServer::start(|_, n| {
        if n == 0 {
            Reply::json_status(
                401,
                &json!({"error": "Invalid Authorization", "key": "test-key"}),
            )
        } else {
            Reply::json(&profile("99000001", "active"))
        }
    })
    .await;
    let s = setup(&server, |_| {});
    let w = s.ch.company("99000001").await.unwrap_err();
    assert_eq!(w.kind, WarningKind::KeyRefused);
    assert_eq!(w.registry, Registry::CompaniesHouse);
    for text in [w.to_string(), format!("{w:?}")] {
        assert!(!text.contains(KEY), "{text}");
    }
    // Not retried, and paused: nothing is sent for 30 minutes.
    assert_eq!(server.requests().len(), 1);
    s.clock.advance(Duration::from_mins(29));
    let w = s.ch.officers("99000001").await.unwrap_err();
    assert_eq!(w.kind, WarningKind::KeyRefused);
    assert_eq!(server.requests().len(), 1);
    s.clock.advance(Duration::from_mins(2));
    assert!(s.ch.company("99000001").await.unwrap().value.is_some());
    assert_eq!(server.requests().len(), 2);
}

#[test]
fn the_key_never_shows() {
    let key = ApiKey::new(" test-key ").unwrap();
    assert_eq!(format!("{key:?}"), "ApiKey(<redacted>)");
    assert_eq!(key, ApiKey::new("test-key").unwrap());
    assert_eq!(
        key.redact("x test-key y test-key"),
        "x <redacted> y <redacted>"
    );
    let config = CompaniesHouseConfig {
        api_key: Some(key),
        ..CompaniesHouseConfig::default()
    };
    let debug = format!("{config:?}");
    assert!(!debug.contains(KEY), "{debug}");
    assert!(debug.contains("ApiKey(<redacted>)"), "{debug}");
    for bad in [
        "short",
        "test key with spaces",
        "test-key:password",
        "tëst-key-1",
    ] {
        let e = ApiKey::new(bad).unwrap_err();
        assert!(!e.contains(bad), "{e}");
    }
}

#[tokio::test]
async fn too_many_requests_backs_off() {
    let server = MockServer::start(|_, n| match n {
        0 => Reply::raw(429, Vec::new()).header("Retry-After", "4"),
        1 => Reply::raw(503, Vec::new()),
        _ => Reply::json(&profile("99000001", "active")),
    })
    .await;
    let s = setup(&server, |_| {});
    assert!(s.ch.company("99000001").await.unwrap().value.is_some());
    assert_eq!(server.requests().len(), 3);
    let sleeps = s.clock.sleeps();
    assert!(sleeps.contains(&Duration::from_secs(4)), "{sleeps:?}");
    assert!(sleeps.contains(&Duration::from_secs(10)), "{sleeps:?}");

    // A 429 that stays: RateLimited, never cached.
    let busy = MockServer::start(|_, _| Reply::raw(429, Vec::new())).await;
    let s = setup(&busy, |_| {});
    let w = s.ch.company("99000001").await.unwrap_err();
    assert_eq!(w.kind, WarningKind::RateLimited { retry_after: None });
    assert_eq!(busy.requests().len(), 3);
}

#[tokio::test]
async fn oversize_and_malformed_pages() {
    let server = MockServer::start(|target, _| {
        if target.contains("/officers") {
            let mut r = Reply::raw(200, vec![b' '; 70_000]);
            r.no_length = true;
            r
        } else if target.contains("/persons-with-significant-control") {
            Reply::raw(200, b"[1, 2, 3]".to_vec())
        } else {
            Reply::raw(200, vec![b' '; 70_000])
        }
    })
    .await;
    let s = setup(&server, |c| c.max_body = 64 * 1024);
    let w = s.ch.officers("99000001").await.unwrap_err();
    assert_eq!(w.kind, WarningKind::TooLarge { limit: 64 * 1024 });
    let w = s.ch.company("99000001").await.unwrap_err();
    assert_eq!(w.kind, WarningKind::TooLarge { limit: 64 * 1024 });
    let w = s.ch.significant_control("99000001").await.unwrap_err();
    assert!(matches!(w.kind, WarningKind::Malformed(_)), "{w}");
    // Nothing was cached.
    let before = server.requests().len();
    s.ch.significant_control("99000001").await.unwrap_err();
    assert_eq!(server.requests().len(), before + 1);
}

#[tokio::test]
async fn the_rate_limit_stays_under_600_in_5_minutes() {
    let d = CompaniesHouseConfig::default();
    assert!(d.per_minute * 5 + d.burst < 600);
    let server = registry().await;
    let s = setup(&server, |c| {
        c.per_minute = 60;
        c.burst = 1;
    });
    s.ch.company("99000001").await.unwrap();
    s.ch.significant_control("99000001").await.unwrap();
    s.ch.search("Fictive").await.unwrap();
    // One bucket for every endpoint: the second and third wait a second each.
    let sleeps = s.clock.sleeps();
    assert_eq!(sleeps.len(), 2, "{sleeps:?}");
    assert!(sleeps.iter().all(|d| (d.as_secs_f64() - 1.0).abs() < 1e-6));
}

#[tokio::test]
async fn configuration_errors() {
    let server = registry().await;
    for bad in [
        "ftp://example.invalid/",
        "not a url",
        "https://example.invalid/?x=1",
        "https://user:pass@example.invalid/",
    ] {
        let config = CompaniesHouseConfig {
            base_url: bad.to_owned(),
            ..CompaniesHouseConfig::default()
        };
        let err = CompaniesHouse::with_parts(
            &config,
            Arc::new(HttpTransport::new(Duration::from_secs(1), Duration::from_secs(1)).unwrap()),
            Arc::new(MemoryCache::default()),
            Arc::new(ManualClock::new(OffsetDateTime::UNIX_EPOCH)),
        )
        .unwrap_err();
        assert!(matches!(err.kind, WarningKind::Config(_)), "{bad}");
    }
    for bad in ["", "Change-Of-Name", "accounts&x=1", "a,b"] {
        let config = CompaniesHouseConfig {
            filing_categories: vec![bad.to_owned()],
            ..CompaniesHouseConfig::default()
        };
        let err = CompaniesHouse::with_parts(
            &config,
            Arc::new(HttpTransport::new(Duration::from_secs(1), Duration::from_secs(1)).unwrap()),
            Arc::new(MemoryCache::default()),
            Arc::new(ManualClock::new(OffsetDateTime::UNIX_EPOCH)),
        )
        .unwrap_err();
        assert!(matches!(err.kind, WarningKind::Config(_)), "{bad:?}");
    }
    drop(server);
}
