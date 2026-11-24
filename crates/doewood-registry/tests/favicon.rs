//! The favicon resolver end to end against a mock server on 127.0.0.1 (ADR 0002 section 5).
//!
//! No test reaches the internet. Invented `*.test` host names resolve, through a fake resolver,
//! to the mock server, and a test-only policy allows exactly that server (127.0.0.1 on its
//! port). The production policy is proven separately to refuse it, and every other special
//! address (see also the unit tests of `favicon::addr`).

#![allow(clippy::case_sensitive_file_extension_comparisons)]

use std::collections::HashMap;
use std::fmt::Write as _;
use std::io::Cursor;
use std::net::IpAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use doewood_registry::favicon::{
    AddressPolicy, DnsResolver, Fallback, FaviconOptions, FaviconResolver, GLOBE_PROBE_DOMAIN,
    GuardError, GuardOptions, GuardedClient, IconFormat, InvalidDomain, MAX_ICON_BYTES,
    MAX_PAGE_BYTES, Oversize, PublicOnly, Refusal, ResolveError, SiteOrigin,
};
use doewood_registry::{Cache, ManualClock, MemoryCache};
use image::{DynamicImage, ImageFormat, Rgba, RgbaImage};
use time::OffsetDateTime;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use url::Url;

// ---------------------------------------------------------------------------------------------
// The mock server

#[derive(Clone, Debug)]
struct Reply {
    status: u16,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
    delay: Duration,
    no_length: bool,
}

impl Reply {
    fn new(status: u16, content_type: &str, body: Vec<u8>) -> Self {
        Self {
            status,
            headers: vec![("Content-Type".to_owned(), content_type.to_owned())],
            body,
            delay: Duration::ZERO,
            no_length: false,
        }
    }

    fn png(body: Vec<u8>) -> Self {
        Self::new(200, "image/png", body)
    }

    fn html(body: &str) -> Self {
        Self::new(200, "text/html; charset=utf-8", body.as_bytes().to_vec())
    }

    fn not_found() -> Self {
        Self::new(404, "text/html", b"<h1>Not found</h1>".to_vec())
    }

    fn redirect(location: &str) -> Self {
        let mut r = Self::new(302, "text/plain", Vec::new());
        r.headers.push(("Location".to_owned(), location.to_owned()));
        r
    }
}

/// One request the mock server saw.
#[derive(Clone, Debug)]
struct Seen {
    host: String,
    target: String,
    head: String,
}

type Handler = dyn Fn(&str, &str) -> Reply + Send + Sync;

/// An HTTP/1.1 server on 127.0.0.1 answering from `handler(host, target)`, where `host` is the
/// `Host` header without the port.
struct Mock {
    port: u16,
    seen: Arc<Mutex<Vec<Seen>>>,
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
                    let host = header(&head, "host").unwrap_or_default();
                    let bare = host.rsplit_once(':').map_or(host.as_str(), |(h, _)| h);
                    let reply = handler(bare, &target);
                    log.lock().unwrap().push(Seen {
                        host: host.clone(),
                        target,
                        head: head.clone(),
                    });
                    tokio::time::sleep(reply.delay).await;
                    let mut out =
                        format!("HTTP/1.1 {} Mock\r\nConnection: close\r\n", reply.status);
                    for (k, v) in &reply.headers {
                        let _ = write!(out, "{k}: {v}\r\n");
                    }
                    if !reply.no_length {
                        let _ = write!(out, "Content-Length: {}\r\n", reply.body.len());
                    }
                    out.push_str("\r\n");
                    let _ = sock.write_all(out.as_bytes()).await;
                    let _ = sock.write_all(&reply.body).await;
                    let _ = sock.shutdown().await;
                });
            }
        });
        Self { port, seen }
    }

    fn seen(&self) -> Vec<Seen> {
        self.seen.lock().unwrap().clone()
    }

    /// `host` + target of every request, such as `acme.test/favicon.ico`.
    fn requests(&self) -> Vec<String> {
        self.seen()
            .into_iter()
            .map(|s| {
                let bare = s
                    .host
                    .rsplit_once(':')
                    .map_or(s.host.clone(), |(h, _)| h.to_owned());
                format!("{bare}{}", s.target)
            })
            .collect()
    }

    fn url(&self, host: &str, path: &str) -> Url {
        Url::parse(&format!("http://{host}:{}{path}", self.port)).unwrap()
    }
}

fn header(head: &str, name: &str) -> Option<String> {
    head.lines()
        .filter_map(|l| l.split_once(':'))
        .find(|(k, _)| k.trim().eq_ignore_ascii_case(name))
        .map(|(_, v)| v.trim().to_owned())
}

// ---------------------------------------------------------------------------------------------
// The test resolver and policy

/// Resolves `*.test` names: each name to its list of answers (one per lookup, the last one
/// repeated), or to 127.0.0.1. Records every lookup.
#[derive(Debug, Default)]
struct FakeDns {
    answers: Mutex<HashMap<String, Vec<Vec<IpAddr>>>>,
    lookups: Mutex<Vec<String>>,
}

impl FakeDns {
    fn answer(&self, host: &str, answers: &[&[&str]]) {
        let answers = answers
            .iter()
            .map(|a| a.iter().map(|ip| ip.parse().unwrap()).collect())
            .collect();
        self.answers
            .lock()
            .unwrap()
            .insert(host.to_owned(), answers);
    }

    fn lookups(&self, host: &str) -> usize {
        self.lookups
            .lock()
            .unwrap()
            .iter()
            .filter(|h| *h == host)
            .count()
    }
}

#[async_trait]
impl DnsResolver for FakeDns {
    async fn lookup(&self, host: &str) -> Result<Vec<IpAddr>, String> {
        let n = {
            let mut l = self.lookups.lock().unwrap();
            l.push(host.to_owned());
            l.iter().filter(|h| *h == host).count() - 1
        };
        match self.answers.lock().unwrap().get(host) {
            Some(list) => Ok(list[n.min(list.len() - 1)].clone()),
            None if host.ends_with(".test") => Ok(vec!["127.0.0.1".parse().unwrap()]),
            None => Err(format!("{host}: no such host")),
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

// ---------------------------------------------------------------------------------------------
// Fixtures: icons built in code

fn encode(format: ImageFormat, f: impl Fn(u32, u32) -> [u8; 4]) -> Vec<u8> {
    let img = DynamicImage::ImageRgba8(RgbaImage::from_fn(16, 16, |x, y| Rgba(f(x, y))));
    let mut out = Cursor::new(Vec::new());
    img.write_to(&mut out, format).unwrap();
    out.into_inner()
}

/// A 16 px icon: a stripe of `shade` on light blue.
fn icon(format: ImageFormat, shade: u8) -> Vec<u8> {
    encode(format, |x, y| {
        if x.abs_diff(y) < 3 {
            [shade, 30, 120, 255]
        } else {
            [180, 210, 250, 255]
        }
    })
}

fn png(shade: u8) -> Vec<u8> {
    icon(ImageFormat::Png, shade)
}

// ---------------------------------------------------------------------------------------------
// The harness

struct Harness {
    mock: Mock,
    dns: Arc<FakeDns>,
    clock: Arc<ManualClock>,
    cache: Arc<MemoryCache>,
    resolver: FaviconResolver,
    client: GuardedClient,
}

fn options(port: u16) -> FaviconOptions {
    FaviconOptions {
        site_budget: Duration::from_secs(5),
        fallback_budget: Duration::from_secs(5),
        total_budget: Duration::from_secs(10),
        origin: SiteOrigin {
            scheme: "http",
            port: Some(port),
        },
        ..FaviconOptions::default()
    }
}

async fn harness(handler: impl Fn(&str, &str) -> Reply + Send + Sync + 'static) -> Harness {
    harness_with(handler, options).await
}

async fn harness_with(
    handler: impl Fn(&str, &str) -> Reply + Send + Sync + 'static,
    opts: impl FnOnce(u16) -> FaviconOptions,
) -> Harness {
    let mock = Mock::start(handler).await;
    let dns = Arc::new(FakeDns::default());
    let policy: Arc<dyn AddressPolicy> = Arc::new(MockOnly { port: mock.port });
    let guard = GuardOptions {
        connect_timeout: Duration::from_secs(2),
        request_timeout: Duration::from_secs(3),
        ..GuardOptions::default()
    };
    let client = GuardedClient::new(&guard, dns.clone(), policy).unwrap();
    let clock = Arc::new(ManualClock::new(
        OffsetDateTime::from_unix_timestamp(1_795_000_000).unwrap(),
    ));
    let cache = Arc::new(MemoryCache::default());
    let resolver = FaviconResolver::new(
        client.clone(),
        cache.clone(),
        clock.clone(),
        opts(mock.port),
    );
    Harness {
        mock,
        dns,
        clock,
        cache,
        resolver,
        client,
    }
}

impl Harness {
    async fn icon(&self, domain: &str) -> Option<(IconFormat, Vec<u8>)> {
        self.resolver
            .resolve(domain, None)
            .await
            .unwrap()
            .map(|i| (i.format, i.bytes))
    }

    async fn get(
        &self,
        host: &str,
        path: &str,
    ) -> Result<doewood_registry::favicon::Fetched, GuardError> {
        self.client
            .get(self.mock.url(host, path), "*/*", 1024, Oversize::Refuse)
            .await
    }
}

// ---------------------------------------------------------------------------------------------
// The site: favicon.ico, then the homepage's links

#[tokio::test]
async fn favicon_ico_is_found_first() {
    let h = harness(|host, target| match (host, target) {
        ("acme.test", "/favicon.ico") => {
            Reply::new(200, "image/x-icon", icon(ImageFormat::Ico, 20))
        }
        _ => Reply::not_found(),
    })
    .await;
    let (format, bytes) = h.icon("acme.test").await.unwrap();
    assert_eq!(format, IconFormat::Ico);
    assert_eq!(bytes, icon(ImageFormat::Ico, 20));
    assert_eq!(h.mock.requests(), ["acme.test/favicon.ico"]);
    // The original host name is sent, and doewood's generic User-Agent.
    let seen = &h.mock.seen()[0];
    assert_eq!(seen.host, format!("acme.test:{}", h.mock.port));
    assert!(
        header(&seen.head, "user-agent")
            .unwrap()
            .starts_with("doewood/")
    );
    assert_eq!(header(&seen.head, "referer"), None);
}

#[tokio::test]
async fn homepage_link_with_a_relative_href() {
    let h = harness(|host, target| match (host, target) {
        ("acme.test", "/") => Reply::html(
            r#"<html><head><link rel="shortcut icon" href="static/i.png"></head></html>"#,
        ),
        ("acme.test", "/static/i.png") => Reply::png(png(40)),
        _ => Reply::not_found(),
    })
    .await;
    assert_eq!(h.icon("acme.test").await.unwrap().1, png(40));
    assert_eq!(
        h.mock.requests(),
        [
            "acme.test/favicon.ico",
            "acme.test/",
            "acme.test/static/i.png"
        ]
    );
}

#[tokio::test]
async fn homepage_link_with_an_absolute_href_on_another_host() {
    let port = Arc::new(Mutex::new(0u16));
    let p = Arc::clone(&port);
    let h = harness(move |host, target| match (host, target) {
        ("acme.test", "/") => Reply::html(&format!(
            r#"<link rel=icon href="http://cdn.test:{}/acme/icon.png">"#,
            *p.lock().unwrap()
        )),
        ("cdn.test", "/acme/icon.png") => Reply::png(png(60)),
        _ => Reply::not_found(),
    })
    .await;
    *port.lock().unwrap() = h.mock.port;
    assert_eq!(h.icon("acme.test").await.unwrap().1, png(60));
    assert_eq!(h.dns.lookups("cdn.test"), 1);
}

#[tokio::test]
async fn the_homepage_link_is_resolved_against_the_redirected_page() {
    let h = harness(|host, target| match (host, target) {
        ("acme.test", "/") => Reply::redirect("/en/home"),
        ("acme.test", "/en/home") => Reply::html(r#"<link rel="icon" href="i.png">"#),
        ("acme.test", "/en/i.png") => Reply::png(png(70)),
        _ => Reply::not_found(),
    })
    .await;
    assert_eq!(h.icon("acme.test").await.unwrap().1, png(70));
}

#[tokio::test]
async fn at_most_two_links_are_followed() {
    let h = harness(|host, target| match (host, target) {
        ("acme.test", "/") => Reply::html(
            r#"<link rel="icon" href="/1.png"><link rel="icon" href="/2.png">
               <link rel="icon" href="/3.png">"#,
        ),
        ("acme.test", "/3.png") => Reply::png(png(80)),
        // The first two are blank.
        ("acme.test", "/1.png" | "/2.png") => Reply::png(encode(ImageFormat::Png, |_, _| [0; 4])),
        _ => Reply::not_found(),
    })
    .await;
    assert_eq!(h.icon("acme.test").await, None);
    assert_eq!(
        h.mock.requests(),
        [
            "acme.test/favicon.ico",
            "acme.test/",
            "acme.test/1.png",
            "acme.test/2.png"
        ]
    );
}

#[tokio::test]
async fn data_uris_are_skipped() {
    let h = harness(|host, target| match (host, target) {
        ("acme.test", "/") => Reply::html(
            r#"<link rel="icon" href="data:image/png;base64,iVBORw0KGgo=">
               <link rel="icon" href="/real.png">"#,
        ),
        ("acme.test", "/real.png") => Reply::png(png(90)),
        _ => Reply::not_found(),
    })
    .await;
    assert_eq!(h.icon("acme.test").await.unwrap().1, png(90));
}

#[tokio::test]
async fn only_the_first_200_kb_of_the_homepage_are_read() {
    let mut page = String::from("<html><head>");
    page.push_str(&" ".repeat(MAX_PAGE_BYTES));
    page.push_str(r#"<link rel="icon" href="/late.png"></head></html>"#);
    let h = harness(move |host, target| match (host, target) {
        ("acme.test", "/") => Reply::html(&page),
        ("acme.test", "/late.png") => Reply::png(png(100)),
        _ => Reply::not_found(),
    })
    .await;
    assert_eq!(h.icon("acme.test").await, None);
    assert!(!h.mock.requests().contains(&"acme.test/late.png".to_owned()));
}

// ---------------------------------------------------------------------------------------------
// Content checks

#[tokio::test]
async fn svg_is_refused_by_type_and_by_bytes() {
    let svg = br#"<svg xmlns="http://www.w3.org/2000/svg"><rect width="16" height="16"/></svg>"#;
    let h = harness(move |host, target| match (host, target) {
        // A real PNG, but declared SVG.
        ("bytype.test", "/favicon.ico") => Reply::new(200, "image/svg+xml", png(10)),
        // SVG bytes declared PNG.
        ("bybytes.test", "/favicon.ico") => Reply::png(svg.to_vec()),
        ("bybytes.test", "/") => {
            Reply::html(r#"<link rel="icon" href="/i.svg" type="image/svg+xml">"#)
        }
        _ => Reply::not_found(),
    })
    .await;
    assert_eq!(h.icon("bytype.test").await, None);
    assert_eq!(h.icon("bybytes.test").await, None);
    // The SVG-typed link is not even fetched.
    assert!(!h.mock.requests().contains(&"bybytes.test/i.svg".to_owned()));
}

#[tokio::test]
async fn html_served_as_png_is_refused() {
    let h = harness(|_, target| match target {
        "/favicon.ico" => Reply::new(
            200,
            "image/png",
            b"<!doctype html><title>Home</title>".to_vec(),
        ),
        "/" => Reply::html(r#"<link rel="icon" href="/x.png">"#),
        "/x.png" => Reply::new(200, "image/png", b"Not found".to_vec()),
        _ => Reply::not_found(),
    })
    .await;
    assert_eq!(h.icon("acme.test").await, None);
}

#[tokio::test]
async fn oversize_icons_are_refused() {
    let mut big = png(10);
    big.resize(MAX_ICON_BYTES + 1, 0);
    let h = harness(move |host, _| {
        let mut r = Reply::png(big.clone());
        // One server says how long the body is, the other does not.
        r.no_length = host == "nolength.test";
        r
    })
    .await;
    assert_eq!(h.icon("withlength.test").await, None);
    assert_eq!(h.icon("nolength.test").await, None);
    let err = h
        .client
        .get(
            h.mock.url("nolength.test", "/favicon.ico"),
            "*/*",
            MAX_ICON_BYTES,
            Oversize::Refuse,
        )
        .await
        .unwrap_err();
    assert_eq!(err, GuardError::TooLarge);
}

// ---------------------------------------------------------------------------------------------
// Redirects

/// A server where `/hop/n` redirects to `/hop/n+1` up to `/hop/last`, which answers 200.
async fn chain(last: usize) -> Harness {
    harness(move |_, target| {
        let n: usize = target
            .strip_prefix("/hop/")
            .and_then(|n| n.parse().ok())
            .unwrap_or(0);
        if n < last {
            Reply::redirect(&format!("/hop/{}", n + 1))
        } else {
            Reply::new(200, "text/plain", b"end".to_vec())
        }
    })
    .await
}

#[tokio::test]
async fn a_chain_of_five_redirects_is_followed() {
    let h = chain(5).await;
    let f = h.get("acme.test", "/hop/0").await.unwrap();
    assert_eq!(
        (f.status, f.redirects, f.body.as_slice()),
        (200, 5, b"end".as_slice())
    );
    assert_eq!(f.url.path(), "/hop/5");
    // Every hop opened its own checked connection: one lookup per hop.
    assert_eq!(h.dns.lookups("acme.test"), 6);
}

#[tokio::test]
async fn a_sixth_redirect_is_refused() {
    let h = chain(6).await;
    assert_eq!(
        h.get("acme.test", "/hop/0").await.unwrap_err(),
        GuardError::Refused(Refusal::TooManyRedirects)
    );
    assert_eq!(h.mock.requests().len(), 6);
}

#[tokio::test]
async fn a_redirect_to_a_private_address_is_refused() {
    let port = Arc::new(Mutex::new(0u16));
    let p = Arc::clone(&port);
    let h = harness(move |_, target| {
        let port = *p.lock().unwrap();
        match target {
            "/to-private" => Reply::redirect(&format!("http://internal.test:{port}/")),
            "/to-ip" => Reply::redirect(&format!("http://127.0.0.1:{port}/")),
            "/to-metadata" => Reply::redirect("http://169.254.169.254/latest/meta-data/"),
            "/to-file" => Reply::redirect("file:///etc/passwd"),
            "/to-userinfo" => Reply::redirect(&format!("http://user:pw@acme.test:{port}/")),
            "/to-port" => Reply::redirect("http://acme.test:22/"),
            "/to-name" => Reply::redirect(&format!("http://localhost:{port}/")),
            "/bad" => {
                let mut r = Reply::new(302, "text/plain", Vec::new());
                r.headers
                    .push(("Location".to_owned(), "http://[::1".to_owned()));
                r
            }
            _ => Reply::new(200, "text/plain", b"should not be reached".to_vec()),
        }
    })
    .await;
    *port.lock().unwrap() = h.mock.port;
    h.dns.answer("internal.test", &[&["10.0.0.7"]]);
    let refused = |r| GuardError::Refused(r);
    assert_eq!(
        h.get("acme.test", "/to-private").await.unwrap_err(),
        refused(Refusal::Address("internal.test".into()))
    );
    assert_eq!(
        h.get("acme.test", "/to-ip").await.unwrap_err(),
        refused(Refusal::Host)
    );
    assert_eq!(
        h.get("acme.test", "/to-metadata").await.unwrap_err(),
        refused(Refusal::Host)
    );
    assert_eq!(
        h.get("acme.test", "/to-file").await.unwrap_err(),
        refused(Refusal::Scheme("file".into()))
    );
    assert_eq!(
        h.get("acme.test", "/to-userinfo").await.unwrap_err(),
        refused(Refusal::UserInfo)
    );
    assert_eq!(
        h.get("acme.test", "/to-port").await.unwrap_err(),
        refused(Refusal::Port(22))
    );
    assert_eq!(
        h.get("acme.test", "/to-name").await.unwrap_err(),
        refused(Refusal::Host)
    );
    assert_eq!(
        h.get("acme.test", "/bad").await.unwrap_err(),
        refused(Refusal::BadRedirect)
    );
    // Only the first hop of each ever reached the server.
    assert!(
        h.mock.requests().iter().all(|r| !r.ends_with("acme.test/")),
        "{:?}",
        h.mock.requests()
    );
}

// ---------------------------------------------------------------------------------------------
// Resolution: every address checked, connect pinned to the checked address

#[tokio::test]
async fn one_private_address_refuses_the_whole_host() {
    let h = harness(|_, _| Reply::png(png(10))).await;
    h.dns.answer("mixed.test", &[&["127.0.0.1", "10.0.0.1"]]);
    h.dns
        .answer("mixed6.test", &[&["127.0.0.1", "fd00:ec2::254"]]);
    h.dns.answer("none.test", &[&[]]);
    assert_eq!(
        h.get("mixed.test", "/").await.unwrap_err(),
        GuardError::Refused(Refusal::Address("mixed.test".into()))
    );
    assert_eq!(
        h.get("mixed6.test", "/").await.unwrap_err(),
        GuardError::Refused(Refusal::Address("mixed6.test".into()))
    );
    assert_eq!(
        h.get("none.test", "/").await.unwrap_err(),
        GuardError::Refused(Refusal::NoAddress("none.test".into()))
    );
    assert!(h.mock.requests().is_empty());
    assert_eq!(h.icon("mixed.test").await, None);
}

#[tokio::test]
async fn dns_rebinding_cannot_swap_the_address() {
    // The first answer is the allowed address, every later one a private address.
    let h = harness(|_, target| match target {
        "/favicon.ico" => Reply::png(png(30)),
        "/moved" => Reply::redirect("/favicon.ico"),
        _ => Reply::not_found(),
    })
    .await;
    h.dns
        .answer("rebind.test", &[&["127.0.0.1"], &["10.0.0.1"]]);
    // The connection goes to the address that was checked: one lookup, no second one between
    // the check and the connect.
    let f = h.get("rebind.test", "/favicon.ico").await;
    assert!(f.is_ok(), "{f:?}");
    assert_eq!(h.dns.lookups("rebind.test"), 1);
    // The next connection resolves again, and the private answer is refused.
    assert_eq!(
        h.get("rebind.test", "/favicon.ico").await.unwrap_err(),
        GuardError::Refused(Refusal::Address("rebind.test".into()))
    );
    // So is a redirect hop that would reconnect after a rebind.
    h.dns
        .answer("rebind2.test", &[&["127.0.0.1"], &["10.0.0.1"]]);
    assert_eq!(
        h.get("rebind2.test", "/moved").await.unwrap_err(),
        GuardError::Refused(Refusal::Address("rebind2.test".into()))
    );
    assert_eq!(
        h.mock.requests(),
        ["rebind.test/favicon.ico", "rebind2.test/moved"]
    );
}

#[tokio::test]
async fn the_production_policy_refuses_the_loopback_mock() {
    let mock = Mock::start(|_, _| Reply::png(png(10))).await;
    let dns = Arc::new(FakeDns::default());
    for (host, ip) in [
        ("acme.com", "127.0.0.1"),
        ("meta.com", "169.254.169.254"),
        ("lan.com", "192.168.1.1"),
        ("cgnat.com", "100.64.0.1"),
        ("mapped.com", "::ffff:127.0.0.1"),
        ("nat64.com", "64:ff9b::7f00:1"),
        ("v6local.com", "::1"),
        ("ula.com", "fd00:ec2::254"),
    ] {
        dns.answer(host, &[&[ip]]);
    }
    let client =
        GuardedClient::new(&GuardOptions::default(), dns.clone(), Arc::new(PublicOnly)).unwrap();
    for host in [
        "acme.com",
        "meta.com",
        "lan.com",
        "cgnat.com",
        "mapped.com",
        "nat64.com",
        "v6local.com",
        "ula.com",
    ] {
        let url = Url::parse(&format!("http://{host}/favicon.ico")).unwrap();
        assert_eq!(
            client
                .get(url, "*/*", 1024, Oversize::Refuse)
                .await
                .unwrap_err(),
            GuardError::Refused(Refusal::Address(host.into())),
            "{host}"
        );
    }
    // The mock's own port is not a default port, and `.test` is a special-use name.
    let url = Url::parse(&format!("http://acme.com:{}/", mock.port)).unwrap();
    assert_eq!(
        client
            .get(url, "*/*", 1024, Oversize::Refuse)
            .await
            .unwrap_err(),
        GuardError::Refused(Refusal::Port(mock.port))
    );
    let url = Url::parse("https://acme.test/").unwrap();
    assert_eq!(
        client
            .get(url, "*/*", 1024, Oversize::Refuse)
            .await
            .unwrap_err(),
        GuardError::Refused(Refusal::Name("acme.test".into()))
    );
    assert!(mock.seen().is_empty());
}

// ---------------------------------------------------------------------------------------------
// Domains

#[tokio::test]
async fn invalid_domains_fetch_nothing() {
    let h = harness(|_, _| Reply::png(png(10))).await;
    for bad in [
        "",
        "127.0.0.1",
        "acme.test.",
        "a/b.test",
        "user@acme.test",
        "acme.test:80",
        "[::1]",
    ] {
        assert!(
            matches!(
                h.resolver.resolve(bad, None).await,
                Err(ResolveError::InvalidDomain(InvalidDomain(_)))
            ),
            "{bad:?}"
        );
    }
    assert!(h.mock.requests().is_empty());
    // Upper case and international names are normalized.
    assert!(h.icon("ACME.test").await.is_some());
    assert!(h.icon("bücher.test").await.is_some());
    assert_eq!(
        h.mock.requests(),
        ["acme.test/favicon.ico", "xn--bcher-kva.test/favicon.ico"]
    );
}

// ---------------------------------------------------------------------------------------------
// The fallback service and its placeholder

#[tokio::test]
async fn the_fallback_placeholder_is_detected() {
    let globe = png(200);
    let g = globe.clone();
    let h = harness(move |host, target| match host {
        "fallback.test" => match target {
            "/ip3/known.test.ico" => Reply::png(png(50)),
            "/ip3/globe.test.ico" => Reply::png(g.clone()),
            // The probe of the `.invalid` domain: the placeholder, with a 404.
            t if t == format!("/ip3/{GLOBE_PROBE_DOMAIN}.ico") => {
                Reply::new(404, "image/png", g.clone())
            }
            _ => Reply::not_found(),
        },
        _ => Reply::not_found(),
    })
    .await;
    let fallback = Fallback::new(&format!(
        "http://fallback.test:{}/ip3/{{domain}}.ico",
        h.mock.port
    ))
    .unwrap();
    let known = h
        .resolver
        .resolve("known.test", Some(&fallback))
        .await
        .unwrap();
    assert_eq!(known.unwrap().bytes, png(50));
    let placeholder = h
        .resolver
        .resolve("globe.test", Some(&fallback))
        .await
        .unwrap();
    assert_eq!(placeholder, None);
    // The placeholder was learned once.
    let probes = h
        .mock
        .requests()
        .iter()
        .filter(|r| r.contains(GLOBE_PROBE_DOMAIN))
        .count();
    assert_eq!(probes, 1);
    // Without a fallback, the service is never asked.
    let before = h.mock.requests().len();
    assert_eq!(h.icon("other.test").await, None);
    assert!(
        h.mock.requests()[before..]
            .iter()
            .all(|r| !r.starts_with("fallback.test"))
    );
}

#[tokio::test]
async fn the_fallback_goes_through_the_guard() {
    let h = harness(|_, _| Reply::png(png(50))).await;
    h.dns.answer("fallback.test", &[&["192.168.0.10"]]);
    // A site with no icon.
    h.dns.answer("site.test", &[&["10.1.1.1"]]);
    let fallback = Fallback::new(&format!(
        "http://fallback.test:{}/ip3/{{domain}}.ico",
        h.mock.port
    ))
    .unwrap();
    assert_eq!(
        h.resolver
            .resolve("site.test", Some(&fallback))
            .await
            .unwrap(),
        None
    );
    assert!(h.mock.requests().is_empty());
    assert!(Fallback::new("https://icons.example.com/").is_err());
    assert!(Fallback::new("ftp://icons.example.com/{domain}").is_err());
    assert!(Fallback::new(Fallback::DEFAULT_TEMPLATE).is_ok());
}

// ---------------------------------------------------------------------------------------------
// Cache, budgets, concurrency

#[tokio::test]
async fn hits_and_misses_are_cached() {
    let h = harness(|host, target| match (host, target) {
        ("hit.test", "/favicon.ico") => Reply::png(png(10)),
        _ => Reply::not_found(),
    })
    .await;
    assert!(h.icon("hit.test").await.is_some());
    assert_eq!(h.icon("miss.test").await, None);
    let fetched = h.mock.requests().len();
    // Served from the cache.
    assert!(h.icon("hit.test").await.is_some());
    assert_eq!(h.icon("miss.test").await, None);
    assert_eq!(h.mock.requests().len(), fetched);
    assert!(h.cache.get("favicon:hit.test").await.is_some());
    // A miss is kept 6 hours, a hit 30 days.
    h.clock.advance(Duration::from_secs(6 * 3600 + 1));
    assert!(h.icon("hit.test").await.is_some());
    assert_eq!(h.icon("miss.test").await, None);
    assert_eq!(
        h.mock.requests().len(),
        fetched + 2,
        "{:?}",
        h.mock.requests()
    );
    h.clock.advance(Duration::from_hours(30 * 24));
    assert!(h.icon("hit.test").await.is_some());
    assert_eq!(h.mock.requests().len(), fetched + 3);
}

#[tokio::test]
async fn the_site_budget_bounds_a_slow_site() {
    let h = harness_with(
        |_, _| {
            let mut r = Reply::png(png(10));
            r.delay = Duration::from_secs(30);
            r
        },
        |port| FaviconOptions {
            site_budget: Duration::from_millis(300),
            ..options(port)
        },
    )
    .await;
    let start = std::time::Instant::now();
    assert_eq!(h.icon("slow.test").await, None);
    assert!(start.elapsed() < Duration::from_secs(3));
}

#[tokio::test]
async fn too_many_lookups_at_once_are_refused() {
    let h = Arc::new(
        harness_with(
            |_, _| {
                let mut r = Reply::png(png(10));
                r.delay = Duration::from_millis(500);
                r
            },
            |port| FaviconOptions {
                max_in_flight: 1,
                ..options(port)
            },
        )
        .await,
    );
    let first = {
        let h = Arc::clone(&h);
        tokio::spawn(async move { h.resolver.resolve("one.test", None).await })
    };
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(
        h.resolver.resolve("two.test", None).await,
        Err(ResolveError::Busy)
    );
    assert!(first.await.unwrap().unwrap().is_some());
}
