//! The EDGAR client against a mock server on 127.0.0.1, with hand-written fixtures. Nothing
//! here reaches the SEC: every company, CIK (9990000001 and up), ticker, filing, accession
//! number and Exhibit 21 is invented, and the contacts are `example.invalid` addresses.

mod support;

use std::fmt::Write as _;
use std::sync::Arc;
use std::time::Duration;

use doewood_registry::edgar::{
    Accession, Cik, Edgar, EdgarConfig, EdgarRefusal, Extraction, Subsidiary, TrustTier, user_agent,
};
use doewood_registry::{Clock, HttpTransport, ManualClock, MemoryCache, USER_AGENT, WarningKind};
use serde_json::{Value, json};
use support::{MockServer, Reply, bad_lei, lei};
use time::{Date, Month, OffsetDateTime};

const CIK: &str = "9990000001";
const ARCHIVE: &str = "/api/v1/www/Archives/edgar/data/9990000001/";
const CONTACT: &str = "Example Ops ops@example.invalid";

struct Setup {
    edgar: Edgar,
    clock: Arc<ManualClock>,
}

fn config(server: &MockServer, tweak: impl FnOnce(&mut EdgarConfig)) -> EdgarConfig {
    let mut config = EdgarConfig {
        data_url: format!("{}data/", server.base),
        www_url: format!("{}www/", server.base),
        ..EdgarConfig::default()
    };
    tweak(&mut config);
    config
}

fn setup(server: &MockServer, tweak: impl FnOnce(&mut EdgarConfig)) -> Setup {
    let config = config(server, tweak);
    let clock = Arc::new(ManualClock::new(
        OffsetDateTime::from_unix_timestamp(1_790_000_000).unwrap(),
    ));
    let agent = user_agent(config.contact.as_deref()).unwrap();
    let transport =
        HttpTransport::with_user_agent(config.timeout, config.connect_timeout, &agent).unwrap();
    let edgar = Edgar::with_parts(
        &config,
        Arc::new(transport),
        Arc::new(MemoryCache::default()),
        clock.clone(),
    )
    .expect("client");
    Setup { edgar, clock }
}

fn day(y: i32, m: Month, d: u8) -> Option<Date> {
    Date::from_calendar_date(y, m, d).ok()
}

/// The submissions JSON of the invented CIK 9990000001.
fn submissions() -> Value {
    json!({
        "cik": "9990000001",
        "entityType": "operating",
        "sic": "9999",
        "sicDescription": "Example Industry",
        "name": "Fictive Holdings Example Inc.",
        "tickers": ["FHEX", null, "FHEX-P"],
        "exchanges": ["Example Exchange"],
        "ein": "000000000",
        "stateOfIncorporation": "DE",
        "stateOfIncorporationDescription": "DE",
        "fiscalYearEnd": "1231",
        "formerNames": [
            {"name": "Fictive Widgets Example Corp", "from": "2001-03-01T00:00:00.000Z",
                "to": "2019-08-05T00:00:00.000Z"},
            {"name": "Fictive Example Co", "from": null, "to": "2001-02-28"},
            {"name": "", "from": "2000-01-01", "to": "2000-12-31"}
        ],
        "filings": {
            "recent": {
                "accessionNumber": ["0009990000-26-000012", "0009990000-26-000010",
                    "garbage", "0009990000-25-000030", "0009990000-25-000020",
                    "0009990000-25-000011"],
                "filingDate": ["2026-02-20", "2026-01-15", "2026-01-01", "2025-10-01",
                    "2025-02-21", "2025-01-10"],
                "reportDate": ["2025-12-31", "2026-01-12", "", "2025-09-30", "2024-12-31", ""],
                "form": ["10-K", "8-K", "8-K", "8-K", "10-K", "8-K/A"],
                "items": ["", "2.01,9.01", "2.01", "5.02", "", "2.01"],
                "primaryDocument": ["fict-10k.htm", "fict-8k.htm", "x.htm", "fict-8k2.htm",
                    "fict-10k-2024.htm", "../../etc/passwd"],
                "primaryDocDescription": ["10-K", "8-K", "", "8-K", "10-K", "8-K/A"]
            },
            "files": [{"name": "CIK9990000001-submissions-001.json", "filingCount": 3,
                "filingFrom": "1999-01-01", "filingTo": "2010-12-31"}]
        }
    })
}

fn tickers() -> Value {
    json!({
        "0": {"cik_str": 9_990_000_001_u64, "ticker": "FHEX", "title": "Fictive Holdings Example Inc."},
        "1": {"cik_str": 9_990_000_001_u64, "ticker": "FHEX-P", "title": "Fictive Holdings Example Inc."},
        "2": {"cik_str": "9990000002", "ticker": "FHXG", "title": "Fictive Holdings Example Group plc"},
        "3": {"cik_str": 9_990_000_003_u64, "ticker": "OTHR", "title": "Other Invented Widgets LLC"},
        "4": {"cik_str": 0, "ticker": "ZERO", "title": "Fictive Holdings Example Zero"},
        "5": {"ticker": "NOCIK", "title": "Fictive Holdings Example Missing"}
    })
}

/// A filing index page with the EDGAR columns, listing `rows` as `(type, file)`.
fn index(rows: &[(&str, &str)]) -> Vec<u8> {
    let mut h = String::from(
        "<html><body><div id=\"formName\"><strong>Form 10-K</strong></div>\
         <table class=\"tableFile\" summary=\"Document Format Files\">\
         <tr><th scope=\"col\">Seq</th><th scope=\"col\">Description</th>\
         <th scope=\"col\">Document</th><th scope=\"col\">Type</th><th scope=\"col\">Size</th></tr>",
    );
    for (i, (kind, file)) in rows.iter().enumerate() {
        let _ = write!(
            h,
            "<tr><td>{}</td><td>DOC</td><td><a href=\"/Archives/edgar/data/9990000001/x/{file}\">\
             {file}</a> iXBRL</td><td>{kind}</td><td>1234</td></tr>",
            i + 1
        );
    }
    h.push_str("</table></body></html>");
    h.into_bytes()
}

const EX21_TABLE: &str = "<html><body><p style=\"text-align:center\">EXHIBIT 21.1</p>\
    <p>SUBSIDIARIES OF FICTIVE HOLDINGS EXAMPLE INC.</p>\
    <table><tr><td><b>Name of Subsidiary</b></td><td>&nbsp;</td>\
    <td><b>Jurisdiction of Incorporation or Organization</b></td><td>% Owned</td></tr>\
    <tr><td>Alpha Fictive LLC</td><td>&nbsp;</td><td>Delaware</td><td>100%</td></tr>\
    <tr><td>Beta Fictive Organization Ltd.</td><td></td><td>United&#160;Kingdom</td><td>100</td></tr>\
    <tr><td>Gamma Fictive GmbH &amp; Co. KG</td><td></td><td>Germany</td><td></td></tr>\
    <tr><td>&nbsp;</td><td></td><td></td></tr>\
    <tr><td>Delta Fictive Trading (Shanghai) Co.</td></tr>\
    </table></body></html>";

fn table_rows() -> Vec<Subsidiary> {
    let s = |n: &str, j: Option<&str>| Subsidiary {
        name: n.to_owned(),
        jurisdiction: j.map(str::to_owned),
    };
    vec![
        s("Alpha Fictive LLC", Some("Delaware")),
        s("Beta Fictive Organization Ltd.", Some("United Kingdom")),
        s("Gamma Fictive GmbH & Co. KG", Some("Germany")),
        s("Delta Fictive Trading (Shanghai) Co.", None),
    ]
}

const EIGHT_K: &str = "<html><body><p>UNITED STATES (INVENTED FIXTURE)</p><p>FORM 8-K</p>\
    <p><b>Item&nbsp;2.01</b> <b>Completion of Acquisition or Disposition of Assets.</b></p>\
    <p>On January 12, 2026, Fictive Holdings Example Inc. completed its acquisition of \
    Omega Invented Widgets LLC, a Delaware limited liability company.</p>\
    <p><b>Item 9.01 Financial Statements and Exhibits.</b></p><p>(d) Exhibits.</p>\
    </body></html>";

/// A server for the whole fixture: submissions, tickers, two filing indexes (one with an
/// EX-21.1), the exhibit and an 8-K.
async fn registry() -> MockServer {
    MockServer::start(|target, _| match target {
        "/api/v1/data/submissions/CIK9990000001.json" => Reply::json(&submissions()),
        "/api/v1/www/files/company_tickers.json" => Reply::json(&tickers()),
        t if t == format!("{ARCHIVE}000999000026000012/0009990000-26-000012-index.htm") => {
            Reply::raw(
                200,
                index(&[
                    ("10-K", "fict-10k.htm"),
                    ("EX-21.1", "fict-ex21.htm"),
                    ("EX-23.1", "fict-ex23.htm"),
                    ("GRAPHIC", "../evil.jpg"),
                ]),
            )
        }
        t if t == format!("{ARCHIVE}000999000025000020/0009990000-25-000020-index.htm") => {
            Reply::raw(
                200,
                index(&[("10-K", "fict-10k-2024.htm"), ("EX-31.1", "fict-ex31.htm")]),
            )
        }
        t if t == format!("{ARCHIVE}000999000026000012/fict-ex21.htm") => {
            Reply::raw(200, EX21_TABLE.as_bytes().to_vec())
        }
        t if t == format!("{ARCHIVE}000999000026000010/fict-8k.htm") => {
            Reply::raw(200, EIGHT_K.as_bytes().to_vec())
        }
        _ => Reply::raw(404, b"Not Found".to_vec()),
    })
    .await
}

#[tokio::test]
async fn company_by_cik() {
    let server = registry().await;
    let s = setup(&server, |_| {});
    for cik in ["9990000001", "CIK9990000001", " 9990000001 "] {
        let c = s
            .edgar
            .company(cik)
            .await
            .expect("answer")
            .value
            .expect("company");
        assert_eq!(c.cik, Cik::new(9_990_000_001).unwrap());
        assert_eq!(c.cik.to_string(), "9990000001");
        assert_eq!(c.name, "Fictive Holdings Example Inc.");
        assert_eq!(c.tickers, ["FHEX", "FHEX-P"]);
        assert_eq!(c.sic.as_deref(), Some("9999"));
        assert_eq!(c.state_of_incorporation.as_deref(), Some("DE"));
        assert_eq!(c.tier, TrustTier::Official);
        assert_eq!(c.former_names.len(), 2);
        assert_eq!(c.former_names[0].name, "Fictive Widgets Example Corp");
        assert_eq!(c.former_names[0].from, day(2001, Month::March, 1));
        assert_eq!(c.former_names[0].to, day(2019, Month::August, 5));
        assert_eq!(c.former_names[1].from, None);
        assert_eq!(c.former_names[1].to, day(2001, Month::February, 28));
        // The "garbage" accession is skipped; an unsafe primary document is dropped.
        assert_eq!(c.filings.len(), 5);
        assert_eq!(c.filings[4].primary_document, None);
        assert_eq!(c.older_filing_files, ["CIK9990000001-submissions-001.json"]);
        let acq: Vec<_> = c.acquisitions().map(|f| f.accession.to_string()).collect();
        assert_eq!(acq, ["0009990000-26-000010", "0009990000-25-000011"]);
        let k = c.latest_annual_report().unwrap();
        assert_eq!(k.accession.to_string(), "0009990000-26-000012");
        assert_eq!(k.filing_date, day(2026, Month::February, 20));
        assert_eq!(c.filings[1].items, ["2.01", "9.01"]);
        assert_eq!(c.filings[1].report_date, day(2026, Month::January, 12));
        // An all-zero EIN and a missing LEI are none.
        assert_eq!(c.ein, None);
        assert_eq!(c.lei, None);
    }
    // Cached: one request for the three lookups.
    assert_eq!(
        server.requests(),
        ["/api/v1/data/submissions/CIK9990000001.json"]
    );
    assert_eq!(
        server.header_values("accept"),
        [Some("application/json".to_owned())]
    );
}

#[tokio::test]
async fn lei_and_ein_of_the_filer() {
    let filer = lei("EDGARFILER");
    for (raw_lei, ein, want_lei, want_ein) in [
        (
            json!(filer),
            json!("999000001"),
            Some(filer.clone()),
            Some("999000001"),
        ),
        (json!(bad_lei("EDGARFILER")), json!("99-900001"), None, None),
        (Value::Null, json!(999_000_002), None, Some("999000002")),
    ] {
        let mut body = submissions();
        body["lei"] = raw_lei;
        body["ein"] = ein;
        let server = MockServer::start(move |_, _| Reply::json(&body)).await;
        let s = setup(&server, |_| {});
        let c = s.edgar.company(CIK).await.unwrap().value.unwrap();
        assert_eq!(c.lei.map(|l| l.value), want_lei);
        assert_eq!(c.ein.as_deref(), want_ein);
    }
}

#[tokio::test]
async fn company_cache_expires_after_a_day() {
    let server = registry().await;
    let s = setup(&server, |_| {});
    s.edgar.company(CIK).await.unwrap();
    s.clock.advance(Duration::from_hours(23));
    s.edgar.company(CIK).await.unwrap();
    assert_eq!(server.requests().len(), 1);
    s.clock.advance(Duration::from_hours(2));
    let again = s.edgar.company(CIK).await.unwrap();
    assert_eq!(server.requests().len(), 2);
    assert_eq!(again.fetched_at, s.clock.now_utc());
}

#[tokio::test]
async fn unknown_cik_and_mismatched_answers() {
    let server = MockServer::start(|target, _| {
        if target.ends_with("CIK9990000002.json") {
            Reply::json(&submissions())
        } else if target.ends_with("CIK9990000003.json") {
            Reply::json(&json!({"cik": "9990000003"}))
        } else if target.ends_with("CIK9990000004.json") {
            Reply::raw(200, b"{not json".to_vec())
        } else {
            Reply::raw(404, Vec::new())
        }
    })
    .await;
    let s = setup(&server, |_| {});
    assert_eq!(s.edgar.company("9990000009").await.unwrap().value, None);
    for cik in ["9990000002", "9990000003", "9990000004"] {
        let w = s.edgar.company(cik).await.unwrap_err();
        assert!(matches!(w.kind, WarningKind::Malformed(_)), "{cik}: {w}");
    }
    // The 404 is cached for a day; malformed answers are not cached.
    s.edgar.company("9990000009").await.unwrap();
    s.edgar.company("9990000004").await.unwrap_err();
    assert_eq!(server.requests().len(), 5);
}

#[tokio::test]
async fn invalid_ciks_are_never_sent() {
    let server = registry().await;
    let s = setup(&server, |_| {});
    for bad in [
        "",
        "0",
        "0000000000",
        "12345678901",
        "99900000O1",
        "-1",
        "1e5",
        "CIK",
        "9990000001/../x",
        "9990000001?a=b",
        "\u{0661}\u{0662}",
    ] {
        let w = s.edgar.company(bad).await.unwrap_err();
        assert!(matches!(w.kind, WarningKind::InvalidInput(_)), "{bad}: {w}");
        let w = s.edgar.subsidiaries(bad).await.unwrap_err();
        assert!(matches!(w.kind, WarningKind::InvalidInput(_)), "{bad}: {w}");
    }
    for bad in ["", "FH EX", "FHEX/../x", "\u{e9}"] {
        assert!(s.edgar.ticker(bad).await.is_err(), "{bad}");
    }
    assert!(s.edgar.search("  ,; ").await.is_err());
    assert!(s.edgar.search(&"x".repeat(300)).await.is_err());
    assert!(server.requests().is_empty(), "{:?}", server.requests());
}

#[tokio::test]
async fn ticker_and_name_lookup_in_the_tickers_file() {
    let server = registry().await;
    let s = setup(&server, |_| {});
    let hit = s.edgar.ticker("fhex").await.unwrap().value.unwrap();
    assert_eq!(hit.cik.value(), 9_990_000_001);
    assert_eq!(hit.title, "Fictive Holdings Example Inc.");
    assert_eq!(s.edgar.ticker("NONE").await.unwrap().value, None);
    let hits = s
        .edgar
        .search("fictive  HOLDINGS, example")
        .await
        .unwrap()
        .value;
    let ciks: Vec<u64> = hits.iter().map(|h| h.cik.value()).collect();
    // One entry per CIK; the zero and missing CIKs are skipped.
    assert_eq!(ciks, [9_990_000_001, 9_990_000_002]);
    let exact = s
        .edgar
        .search("Fictive Holdings Example Group PLC")
        .await
        .unwrap()
        .value;
    assert_eq!(exact[0].ticker, "FHXG");
    assert!(
        s.edgar
            .search("Nothing Like It")
            .await
            .unwrap()
            .value
            .is_empty()
    );
    // The file is fetched once, and no name or ticker is ever sent.
    assert_eq!(
        server.requests(),
        ["/api/v1/www/files/company_tickers.json"]
    );
}

#[tokio::test]
async fn exhibit_21_of_the_latest_10k() {
    let server = registry().await;
    let s = setup(&server, |_| {});
    let ex = s
        .edgar
        .subsidiaries(CIK)
        .await
        .unwrap()
        .value
        .expect("an EX-21");
    assert_eq!(ex.exhibit_type, "EX-21.1");
    assert_eq!(ex.document, "fict-ex21.htm");
    assert_eq!(ex.accession.to_string(), "0009990000-26-000012");
    assert_eq!(ex.filing_date, day(2026, Month::February, 20));
    assert_eq!(ex.extraction, Extraction::Table);
    assert!(!ex.truncated);
    assert_eq!(ex.tier, TrustTier::Official);
    assert_eq!(ex.subsidiaries, table_rows());
    assert_eq!(
        server.requests(),
        [
            "/api/v1/data/submissions/CIK9990000001.json".to_owned(),
            format!("{ARCHIVE}000999000026000012/0009990000-26-000012-index.htm"),
            format!("{ARCHIVE}000999000026000012/fict-ex21.htm"),
        ]
    );
    // Archives are cached for 30 days.
    s.clock.advance(Duration::from_hours(48));
    s.edgar.subsidiaries(CIK).await.unwrap();
    assert_eq!(server.requests().len(), 4, "only the submissions again");
}

#[tokio::test]
async fn filing_without_exhibit_21() {
    let server = registry().await;
    let s = setup(&server, |_| {});
    let cik = Cik::parse(CIK).unwrap();
    let acc = Accession::parse("0009990000-25-000020").unwrap();
    assert_eq!(s.edgar.exhibit21(cik, &acc).await.unwrap().value, None);
    let missing = Accession::parse("000999000024000001").unwrap();
    let w = s.edgar.exhibit21(cik, &missing).await.unwrap_err();
    assert_eq!(w.kind, WarningKind::Status(404));
}

#[tokio::test]
async fn exhibit_8_of_the_latest_20f() {
    let mut body = submissions();
    body["filings"]["recent"]["form"] = json!(["20-F", "6-K", "6-K", "6-K", "20-F", "6-K"]);
    let server = MockServer::start(move |target, _| match target {
        "/api/v1/data/submissions/CIK9990000001.json" => Reply::json(&body),
        t if t == format!("{ARCHIVE}000999000026000012/0009990000-26-000012-index.htm") => {
            Reply::raw(
                200,
                index(&[
                    ("20-F", "fict-20f.htm"),
                    ("EX-2.1", "fict-ex2.htm"),
                    ("EX-8.1", "fict-ex8.htm"),
                ]),
            )
        }
        t if t == format!("{ARCHIVE}000999000026000012/fict-ex8.htm") => {
            Reply::raw(200, EX21_TABLE.replace("21.1", "8.1").into_bytes())
        }
        _ => Reply::raw(404, b"Not Found".to_vec()),
    })
    .await;
    let s = setup(&server, |_| {});
    let ex = s
        .edgar
        .subsidiaries(CIK)
        .await
        .unwrap()
        .value
        .expect("an EX-8");
    assert_eq!(ex.exhibit_type, "EX-8.1");
    assert_eq!(ex.document, "fict-ex8.htm");
    assert_eq!(ex.filing_date, day(2026, Month::February, 20));
    assert_eq!(ex.subsidiaries, table_rows());
    // A 10-K's EX-21 reader does not take an EX-8.
    let acc = Accession::parse("0009990000-26-000012").unwrap();
    let cik = Cik::parse(CIK).unwrap();
    assert_eq!(s.edgar.exhibit21(cik, &acc).await.unwrap().value, None);
}

#[tokio::test]
async fn filing_index_falls_back_to_index_html() {
    let server = MockServer::start(|target, _| {
        if target.ends_with("-index.html") {
            Reply::raw(200, index(&[("EX-21", "ex21.htm")]))
        } else if target.ends_with("/ex21.htm") {
            Reply::raw(200, EX21_TABLE.as_bytes().to_vec())
        } else {
            Reply::raw(404, b"Not Found".to_vec())
        }
    })
    .await;
    let s = setup(&server, |_| {});
    let acc = Accession::parse("0009990000-26-000001").unwrap();
    let cik = Cik::parse(CIK).unwrap();
    let ex = s.edgar.exhibit21(cik, &acc).await.unwrap().value.unwrap();
    assert_eq!(ex.subsidiaries, table_rows());
    assert_eq!(
        server.requests(),
        [
            format!("{ARCHIVE}000999000026000001/0009990000-26-000001-index.htm"),
            format!("{ARCHIVE}000999000026000001/0009990000-26-000001-index.html"),
            format!("{ARCHIVE}000999000026000001/ex21.htm"),
        ]
    );
    // The found index is cached under its own name: no request again.
    s.edgar.exhibit21(cik, &acc).await.unwrap();
    assert_eq!(server.requests().len(), 4, "{:?}", server.requests());
}

/// A server whose only filing's EX-21 is `body`.
async fn exhibit_server(body: Vec<u8>) -> MockServer {
    MockServer::start(move |target, _| {
        if target.ends_with("-index.htm") {
            Reply::raw(200, index(&[("EX-21", "ex21.txt")]))
        } else if target.ends_with("ex21.txt") {
            Reply::raw(200, body.clone())
        } else {
            Reply::raw(404, Vec::new())
        }
    })
    .await
}

async fn read_exhibit(body: &[u8]) -> doewood_registry::edgar::Exhibit21 {
    let server = exhibit_server(body.to_vec()).await;
    let s = setup(&server, |_| {});
    let acc = Accession::parse("0009990000-26-000001").unwrap();
    s.edgar
        .exhibit21(Cik::parse(CIK).unwrap(), &acc)
        .await
        .expect("answer")
        .value
        .expect("an EX-21")
}

#[tokio::test]
async fn exhibit_21_paragraphs_and_plain_text() {
    let html = "<html><body><p>Exhibit 21</p><p>List of Subsidiaries:</p>\
        <p>Alpha Fictive LLC (Delaware)</p>\
        <p>Beta Fictive Ltd., a United Kingdom private limited company</p>\
        <div>Gamma Fictive Inc. &ndash; Ontario<br>Delta Fictive SA</div>\
        <p>1. Epsilon Fictive BV ........ Netherlands</p></body></html>";
    let ex = read_exhibit(html.as_bytes()).await;
    assert_eq!(ex.extraction, Extraction::Lines);
    let got: Vec<(&str, Option<&str>)> = ex
        .subsidiaries
        .iter()
        .map(|s| (s.name.as_str(), s.jurisdiction.as_deref()))
        .collect();
    assert_eq!(
        got,
        [
            ("Alpha Fictive LLC", Some("Delaware")),
            ("Beta Fictive Ltd.", Some("United Kingdom")),
            ("Gamma Fictive Inc.", Some("Ontario")),
            ("Delta Fictive SA", None),
            ("Epsilon Fictive BV", Some("Netherlands")),
        ]
    );
    let text = "SUBSIDIARIES OF THE REGISTRANT\r\n\r\nName                         Jurisdiction\r\n\
        Alpha Fictive LLC            Delaware\r\nZeta Fictive Pty Ltd         Australia\r\n";
    let ex = read_exhibit(text.as_bytes()).await;
    assert_eq!(ex.extraction, Extraction::Lines);
    let got: Vec<(&str, Option<&str>)> = ex
        .subsidiaries
        .iter()
        .map(|s| (s.name.as_str(), s.jurisdiction.as_deref()))
        .collect();
    assert_eq!(
        got,
        [
            ("Alpha Fictive LLC", Some("Delaware")),
            ("Zeta Fictive Pty Ltd", Some("Australia"))
        ]
    );
}

#[tokio::test]
async fn exhibit_21_garbage_never_panics() {
    let mut inputs: Vec<Vec<u8>> = vec![
        Vec::new(),
        b"\xff\xfe\x00\x00garbage\x80\x81".to_vec(),
        b"<table><tr><td>".to_vec(),
        b"<td>a</td></tr></table></td><tr><tr><td>&#xFFFFFFFF;&#0;</td>".repeat(50),
        b"<<<<>>>>&&&&;;;;".repeat(1000),
        "\u{1f600} (\u{1f600}) , a \u{1f600} ... \u{1f600}   - "
            .repeat(200)
            .into_bytes(),
        b"<script>".to_vec(),
        "x".repeat(200_000).into_bytes(),
        b"(((((((((())))))))))".to_vec(),
        b"Name\nJurisdiction\n100%\n(a)\n*\n".to_vec(),
    ];
    let mut seed = 0x9e37_79b9_u32;
    for _ in 0..20 {
        inputs.push(
            (0..4096)
                .map(|_| {
                    seed ^= seed << 13;
                    seed ^= seed >> 17;
                    seed ^= seed << 5;
                    b"<>/&;#x tdr()\t\n.,-a\xc3\xa9"[seed as usize % 21]
                })
                .collect(),
        );
    }
    for body in inputs {
        let ex = read_exhibit(&body).await;
        for s in &ex.subsidiaries {
            assert!(!s.name.is_empty() && s.name.chars().count() <= 300);
        }
    }
}

#[tokio::test]
async fn exhibit_21_rows_are_capped() {
    let mut html = String::from("<table>");
    for i in 0..50 {
        let _ = write!(
            html,
            "<tr><td>Fictive Sub {i} LLC</td><td>Delaware</td></tr>"
        );
    }
    html.push_str("</table>");
    let server = exhibit_server(html.into_bytes()).await;
    let s = setup(&server, |c| c.max_subsidiaries = 10);
    let acc = Accession::parse("0009990000-26-000001").unwrap();
    let ex = s
        .edgar
        .exhibit21(Cik::parse(CIK).unwrap(), &acc)
        .await
        .unwrap()
        .value
        .unwrap();
    assert_eq!(ex.subsidiaries.len(), 10);
    assert!(ex.truncated);
}

#[tokio::test]
async fn item_2_01_text() {
    let server = registry().await;
    let s = setup(&server, |_| {});
    let company = s.edgar.company(CIK).await.unwrap().value.unwrap();
    let filing = company.acquisitions().next().unwrap();
    let text = s
        .edgar
        .acquisition_text(company.cik, filing)
        .await
        .unwrap()
        .value
        .expect("an Item 2.01");
    assert!(
        text.starts_with("Item 2.01 Completion of Acquisition"),
        "{text}"
    );
    assert!(text.contains("Omega Invented Widgets LLC"), "{text}");
    assert!(!text.contains("9.01"), "{text}");
    // Capped.
    let short = setup(&server, |c| c.item_chars = 20);
    let t = short
        .edgar
        .acquisition_text(company.cik, filing)
        .await
        .unwrap()
        .value
        .unwrap();
    assert_eq!(t.chars().count(), 20);
    // A filing whose primary document was unsafe is never fetched.
    let before = server.requests().len();
    let unsafe_doc = company.filings.iter().find(|f| f.form == "8-K/A").unwrap();
    assert_eq!(
        s.edgar
            .acquisition_text(company.cik, unsafe_doc)
            .await
            .unwrap()
            .value,
        None
    );
    assert_eq!(server.requests().len(), before);
}

#[tokio::test]
async fn undeclared_user_agent_is_refused_and_pauses() {
    // A server that refuses like the SEC does an undeclared tool, and one that answers.
    let server = MockServer::start(|_, _| Reply::json(&submissions())).await;
    let refusing = MockServer::start(|_, _| {
        Reply::raw(
            403,
            b"<html><h1>Your Request Originates from an Undeclared Automated Tool</h1></html>"
                .to_vec(),
        )
    })
    .await;
    // The default: the generic User-Agent, via Edgar::new.
    let generic = Edgar::new(&config(&refusing, |_| {}), Arc::new(MemoryCache::default())).unwrap();
    let w = generic.company(CIK).await.unwrap_err();
    assert_eq!(
        w.kind,
        WarningKind::Edgar(EdgarRefusal::UndeclaredUserAgent)
    );
    // Not retried, and paused: the next lookup sends nothing.
    let w = generic.company("9990000002").await.unwrap_err();
    assert!(
        matches!(
            w.kind,
            WarningKind::RateLimited {
                retry_after: Some(_)
            }
        ),
        "{w}"
    );
    assert_eq!(refusing.requests().len(), 1);
    assert_eq!(
        refusing.header_values("user-agent"),
        [Some(USER_AGENT.to_owned())]
    );

    // With the operator's contact, via Edgar::new.
    let declared = Edgar::new(
        &config(&server, |c| c.contact = Some(CONTACT.to_owned())),
        Arc::new(MemoryCache::default()),
    )
    .unwrap();
    assert!(declared.company(CIK).await.unwrap().value.is_some());
    let agents = server.header_values("user-agent");
    assert_eq!(agents, [Some(format!("{CONTACT} {USER_AGENT}"))]);
    assert!(
        agents[0]
            .as_deref()
            .unwrap()
            .contains("ops@example.invalid")
    );

    // A contact that could inject a header is a configuration error.
    let bad = config(&server, |c| c.contact = Some("Ops\r\nX: y".to_owned()));
    let w = Edgar::new(&bad, Arc::new(MemoryCache::default())).unwrap_err();
    assert!(matches!(w.kind, WarningKind::Config(_)), "{w}");
}

#[tokio::test]
async fn rate_threshold_pause_ends() {
    let server = MockServer::start(|_, n| {
        if n == 0 {
            Reply::raw(403, b"<h1>Request Rate Threshold Exceeded</h1>".to_vec())
        } else {
            Reply::json(&submissions())
        }
    })
    .await;
    let s = setup(&server, |_| {});
    let w = s.edgar.company(CIK).await.unwrap_err();
    assert_eq!(w.kind, WarningKind::Edgar(EdgarRefusal::RateThreshold));
    s.clock.advance(Duration::from_mins(9));
    let w = s.edgar.company(CIK).await.unwrap_err();
    assert_eq!(
        w.kind,
        WarningKind::RateLimited {
            retry_after: Some(Duration::from_mins(1))
        }
    );
    s.clock.advance(Duration::from_mins(1));
    assert!(s.edgar.company(CIK).await.unwrap().value.is_some());
    assert_eq!(server.requests().len(), 2);
}

#[tokio::test]
async fn too_many_requests_backs_off() {
    let server = MockServer::start(|_, n| match n {
        0 => Reply::raw(429, Vec::new()).header("Retry-After", "4"),
        1 => Reply::raw(503, Vec::new()),
        _ => Reply::json(&submissions()),
    })
    .await;
    let s = setup(&server, |_| {});
    assert!(s.edgar.company(CIK).await.unwrap().value.is_some());
    assert_eq!(server.requests().len(), 3);
    let sleeps = s.clock.sleeps();
    assert!(sleeps.contains(&Duration::from_secs(4)), "{sleeps:?}");
    assert!(sleeps.contains(&Duration::from_secs(2)), "{sleeps:?}");
}

#[tokio::test]
async fn oversize_documents_are_capped() {
    let big = vec![b' '; 70_000];
    let server = MockServer::start(move |target, _| {
        if target.ends_with("-index.htm") {
            Reply::raw(200, index(&[("EX-21", "ex21.htm")]))
        } else {
            let mut r = Reply::raw(200, big.clone());
            r.no_length = target.ends_with("ex21.htm");
            r
        }
    })
    .await;
    let s = setup(&server, |c| {
        c.max_submissions = 64 * 1024;
        c.max_exhibit = 64 * 1024;
    });
    let w = s.edgar.company(CIK).await.unwrap_err();
    assert_eq!(w.kind, WarningKind::TooLarge { limit: 64 * 1024 });
    let acc = Accession::parse("0009990000-26-000001").unwrap();
    let w = s
        .edgar
        .exhibit21(Cik::parse(CIK).unwrap(), &acc)
        .await
        .unwrap_err();
    assert_eq!(w.kind, WarningKind::TooLarge { limit: 64 * 1024 });
}

#[tokio::test]
async fn one_rate_limit_over_every_document() {
    let server = registry().await;
    let s = setup(&server, |c| {
        c.per_minute = 300;
        c.burst = 2;
    });
    // Submissions, index, exhibit, tickers, 8-K: five requests through one bucket.
    s.edgar.subsidiaries(CIK).await.unwrap();
    s.edgar.ticker("FHEX").await.unwrap();
    let company = s.edgar.company(CIK).await.unwrap().value.unwrap();
    s.edgar
        .acquisition_text(company.cik, company.acquisitions().next().unwrap())
        .await
        .unwrap();
    assert_eq!(server.requests().len(), 5);
    let sleeps = s.clock.sleeps();
    assert_eq!(sleeps.len(), 3, "{sleeps:?}");
    for d in sleeps {
        assert!((d.as_secs_f64() - 0.2).abs() < 1e-6, "{d:?}");
    }
}

#[tokio::test]
async fn disabled_sends_nothing() {
    let server = registry().await;
    let s = setup(&server, |c| c.enabled = false);
    assert_eq!(
        s.edgar.company(CIK).await.unwrap_err().kind,
        WarningKind::Disabled
    );
    assert_eq!(
        s.edgar.search("Fictive").await.unwrap_err().kind,
        WarningKind::Disabled
    );
    assert!(server.requests().is_empty());
}
