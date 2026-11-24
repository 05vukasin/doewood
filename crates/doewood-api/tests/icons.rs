//! `GET /icons/{domain}` on the API port: the answers and their headers, with a fake resolver.

mod common;

use std::sync::{Arc, Mutex};
use std::time::Duration;

use common::{Instance, Options};
use doewood_api::{BoxFuture, CSP, IconAnswer, Icons};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// Answers by domain, recording each one asked.
#[derive(Default)]
struct Fake {
    asked: Mutex<Vec<String>>,
}

const PNG: &[u8] = b"\x89PNG\r\n\x1a\n-fake-";

impl Icons for Fake {
    fn icon(&self, domain: String) -> BoxFuture<'_, IconAnswer> {
        self.asked.lock().unwrap().push(domain.clone());
        Box::pin(async move {
            match domain.as_str() {
                "acme.test" => IconAnswer::Found {
                    content_type: "image/png",
                    bytes: PNG.to_vec(),
                },
                "svg.test" => IconAnswer::Found {
                    content_type: "image/svg+xml",
                    bytes: b"<svg/>".to_vec(),
                },
                "busy.test" => IconAnswer::Busy,
                d if d.contains('/') || d.contains('@') => {
                    IconAnswer::Invalid("invalid domain: only a host name".to_owned())
                }
                _ => IconAnswer::Missing,
            }
        })
    }
}

struct Answer {
    status: u16,
    head: String,
    body: Vec<u8>,
}

impl Answer {
    fn header(&self, name: &str) -> Option<String> {
        self.head
            .lines()
            .filter_map(|l| l.split_once(':'))
            .find(|(k, _)| k.trim().eq_ignore_ascii_case(name))
            .map(|(_, v)| v.trim().to_owned())
    }
}

async fn get(a: &Instance, path: &str) -> Answer {
    let mut s = tokio::net::TcpStream::connect(a.addr).await.unwrap();
    let req = format!(
        "GET {path} HTTP/1.1\r\nHost: {}\r\nAccept: image/*\r\nConnection: close\r\n\r\n",
        a.addr
    );
    s.write_all(req.as_bytes()).await.unwrap();
    let mut resp = Vec::new();
    tokio::time::timeout(Duration::from_secs(10), s.read_to_end(&mut resp))
        .await
        .unwrap()
        .unwrap();
    let end = resp.windows(4).position(|w| w == b"\r\n\r\n").unwrap() + 4;
    let head = String::from_utf8_lossy(&resp[..end]).into_owned();
    let status = head.split(' ').nth(1).unwrap().parse().unwrap();
    Answer {
        status,
        head,
        body: resp[end..].to_vec(),
    }
}

fn assert_safe_headers(a: &Answer) {
    assert_eq!(
        a.header("x-content-type-options").as_deref(),
        Some("nosniff")
    );
    assert_eq!(a.header("content-security-policy").as_deref(), Some(CSP));
    assert_eq!(
        a.header("cross-origin-resource-policy").as_deref(),
        Some("same-origin")
    );
    assert_eq!(a.header("referrer-policy").as_deref(), Some("no-referrer"));
    // Always proxied, never a redirect to the site.
    assert_eq!(a.header("location"), None);
    assert_eq!(a.header("access-control-allow-origin"), None);
}

async fn instance(icons: Option<Arc<dyn Icons>>) -> Instance {
    Instance::start_with(
        "inst-a",
        None,
        Options {
            icons,
            ..Options::default()
        },
    )
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_icon_is_served_with_safe_headers() {
    let fake = Arc::new(Fake::default());
    let a = instance(Some(fake.clone())).await;
    let r = get(&a, "/icons/acme.test").await;
    assert_eq!(r.status, 200, "{}", r.head);
    assert_eq!(r.body, PNG);
    assert_eq!(r.header("content-type").as_deref(), Some("image/png"));
    assert_eq!(
        r.header("cache-control").as_deref(),
        Some("private, max-age=86400")
    );
    assert_safe_headers(&r);
    assert_eq!(fake.asked.lock().unwrap().as_slice(), ["acme.test"]);
    a.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn misses_invalid_domains_and_busy() {
    let fake = Arc::new(Fake::default());
    let a = instance(Some(fake.clone())).await;

    let miss = get(&a, "/icons/nothing.test").await;
    assert_eq!(miss.status, 404);
    assert!(miss.body.is_empty());
    assert_eq!(
        miss.header("cache-control").as_deref(),
        Some("private, max-age=21600")
    );
    assert_safe_headers(&miss);

    // The path segment is percent-decoded before the resolver sees it.
    let bad = get(&a, "/icons/a%2Fb.test").await;
    assert_eq!(bad.status, 400);
    assert_eq!(bad.header("cache-control").as_deref(), Some("no-store"));
    assert_eq!(
        bad.header("content-type").as_deref(),
        Some("text/plain; charset=utf-8")
    );
    assert_safe_headers(&bad);
    assert!(fake.asked.lock().unwrap().contains(&"a/b.test".to_owned()));

    let busy = get(&a, "/icons/busy.test").await;
    assert_eq!(busy.status, 503);
    assert_eq!(busy.header("retry-after").as_deref(), Some("5"));
    assert_safe_headers(&busy);

    // A content type outside the image list is never served.
    let svg = get(&a, "/icons/svg.test").await;
    assert_eq!(svg.status, 404);
    assert!(svg.body.is_empty());
    assert_ne!(svg.header("content-type").as_deref(), Some("image/svg+xml"));

    // Only the one route: deeper paths are not icons.
    let deeper = get(&a, "/icons/acme.test/evil").await;
    assert_ne!(deeper.status, 200);
    a.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn without_a_resolver_every_icon_is_a_miss() {
    let a = instance(None).await;
    let r = get(&a, "/icons/acme.test").await;
    assert_eq!(r.status, 404);
    assert_safe_headers(&r);
    // The gRPC API on the same port still works.
    let mut client =
        doewood_api::pb::config_service_client::ConfigServiceClient::new(a.channel().await);
    client
        .list_ai_providers(doewood_api::pb::ListAiProvidersRequest {})
        .await
        .unwrap();
    a.stop().await;
}
