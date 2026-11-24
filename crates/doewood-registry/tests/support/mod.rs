//! A mock registry server on 127.0.0.1 and hand-written GLEIF fixtures. Every company, LEI and
//! relationship here is invented; the LEIs only carry valid ISO 17442 check digits. The VIES
//! fixtures are in `vies.rs`.

#![allow(dead_code)]

use std::fmt::Write as _;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde_json::{Value, json};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

/// One canned answer.
#[derive(Clone, Debug)]
pub struct Reply {
    pub status: u16,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
    /// Wait this long before answering.
    pub delay: Duration,
    /// Send no `Content-Length` (the body ends when the connection closes).
    pub no_length: bool,
}

impl Reply {
    pub fn json(v: &Value) -> Self {
        Self::raw(200, serde_json::to_vec(v).unwrap())
    }

    pub fn raw(status: u16, body: Vec<u8>) -> Self {
        Self {
            status,
            headers: vec![(
                "Content-Type".to_owned(),
                "application/vnd.api+json".to_owned(),
            )],
            body,
            delay: Duration::ZERO,
            no_length: false,
        }
    }

    /// GLEIF's JSON:API 404.
    pub fn not_found() -> Self {
        Self::raw(
            404,
            br#"{"errors":[{"status":"404","title":"Not Found"}]}"#.to_vec(),
        )
    }

    pub fn json_status(status: u16, v: &Value) -> Self {
        let mut r = Self::raw(status, serde_json::to_vec(v).unwrap());
        r.headers = vec![("Content-Type".to_owned(), "application/json".to_owned())];
        r
    }

    pub fn header(mut self, k: &str, v: &str) -> Self {
        self.headers.push((k.to_owned(), v.to_owned()));
        self
    }
}

type Handler = dyn Fn(&str, &[u8], usize) -> Reply + Send + Sync;

/// An HTTP/1.1 server answering every request from a handler, recording each request target
/// and body.
pub struct MockServer {
    pub base: String,
    requests: Arc<Mutex<Vec<String>>>,
    bodies: Arc<Mutex<Vec<Vec<u8>>>>,
}

impl MockServer {
    /// A server whose handler is `(path and query, how many requests came before) -> Reply`.
    pub async fn start(handler: impl Fn(&str, usize) -> Reply + Send + Sync + 'static) -> Self {
        Self::start_with_body(move |target, _, n| handler(target, n)).await
    }

    /// A server whose handler also gets the request body.
    pub async fn start_with_body(
        handler: impl Fn(&str, &[u8], usize) -> Reply + Send + Sync + 'static,
    ) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let requests = Arc::new(Mutex::new(Vec::new()));
        let bodies = Arc::new(Mutex::new(Vec::new()));
        let handler: Arc<Handler> = Arc::new(handler);
        let seen = requests.clone();
        let seen_bodies = bodies.clone();
        tokio::spawn(async move {
            loop {
                let Ok((mut sock, _)) = listener.accept().await else {
                    return;
                };
                let handler = handler.clone();
                let seen = seen.clone();
                let seen_bodies = seen_bodies.clone();
                tokio::spawn(async move {
                    let mut buf = Vec::new();
                    let mut chunk = [0u8; 4096];
                    while !buf.windows(4).any(|w| w == b"\r\n\r\n") {
                        match sock.read(&mut chunk).await {
                            Ok(0) | Err(_) => return,
                            Ok(n) => buf.extend_from_slice(&chunk[..n]),
                        }
                    }
                    let end = buf.windows(4).position(|w| w == b"\r\n\r\n").unwrap() + 4;
                    let head = String::from_utf8_lossy(&buf[..end]).into_owned();
                    let length = head
                        .lines()
                        .filter_map(|l| l.split_once(':'))
                        .find(|(k, _)| k.trim().eq_ignore_ascii_case("content-length"))
                        .and_then(|(_, v)| v.trim().parse::<usize>().ok())
                        .unwrap_or(0);
                    let mut body = buf[end..].to_vec();
                    while body.len() < length {
                        match sock.read(&mut chunk).await {
                            Ok(0) | Err(_) => return,
                            Ok(n) => body.extend_from_slice(&chunk[..n]),
                        }
                    }
                    let target = head
                        .lines()
                        .next()
                        .and_then(|l| l.split(' ').nth(1))
                        .unwrap_or("")
                        .to_owned();
                    let n = {
                        let mut s = seen.lock().unwrap();
                        s.push(target.clone());
                        seen_bodies.lock().unwrap().push(body.clone());
                        s.len() - 1
                    };
                    let reply = handler(&target, &body, n);
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
        Self {
            base: format!("http://{addr}/api/v1/"),
            requests,
            bodies,
        }
    }

    /// Every request body so far, in order.
    pub fn bodies(&self) -> Vec<Vec<u8>> {
        self.bodies.lock().unwrap().clone()
    }

    /// Every request target so far (path and query).
    pub fn requests(&self) -> Vec<String> {
        self.requests.lock().unwrap().clone()
    }
}

/// An invented LEI: `FAKE00`, `tag` padded to 12 with zeros, and the two ISO 17442 check
/// digits (MOD 97-10).
pub fn lei(tag: &str) -> String {
    assert!(tag.len() <= 12);
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

/// `lei` with a wrong check digit.
pub fn bad_lei(tag: &str) -> String {
    let l = lei(tag);
    let (head, last) = l.split_at(19);
    let d = last.parse::<u32>().unwrap();
    format!("{head}{}", (d + 1) % 10)
}

/// A JSON:API LEI record resource.
pub fn record(lei: &str, name: &str, country: &str) -> Value {
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
                "registeredAs": "TEST-0001",
                "jurisdiction": country,
                "category": "GENERAL",
                "legalForm": {"id": "XXXX", "other": null},
                "status": "ACTIVE",
                "expiration": {"date": null, "reason": null},
                "successorEntity": {"lei": null, "name": null},
                "successorEntities": [],
                "eventGroups": []
            },
            "registration": {
                "initialRegistrationDate": "2015-01-01T00:00:00Z",
                "lastUpdateDate": "2026-06-01T08:00:00Z",
                "status": "ISSUED",
                "nextRenewalDate": "2027-01-01T00:00:00Z",
                "corroborationLevel": "FULLY_CORROBORATED"
            },
            "bic": null
        },
        "relationships": {},
        "links": {"self": format!("https://example.invalid/lei-records/{lei}")}
    })
}

/// A JSON:API relationship record: `child` consolidated by `parent`.
pub fn relationship(child: &str, parent: &str, kind: &str, start: &str) -> Value {
    json!({
        "data": {
            "type": "relationship-records",
            "id": format!("{child}-{parent}"),
            "attributes": {
                "relationship": {
                    "startNode": {"id": child, "type": "LEI"},
                    "endNode": {"id": parent, "type": "LEI"},
                    "type": kind,
                    "status": "ACTIVE",
                    "periods": [
                        {"startDate": "2025-01-01T00:00:00Z", "endDate": "2025-12-31T00:00:00Z",
                            "type": "ACCOUNTING_PERIOD"},
                        {"startDate": start, "endDate": null, "type": "RELATIONSHIP_PERIOD"}
                    ],
                    "qualifiers": [],
                    "quantifiers": []
                },
                "registration": {"status": "PUBLISHED",
                    "lastUpdateDate": "2026-05-01T00:00:00Z"}
            }
        }
    })
}

/// A JSON:API reporting exception.
pub fn exception(lei: &str, category: &str, reason: &str) -> Value {
    json!({
        "data": {
            "type": "reporting-exceptions",
            "id": format!("{lei}-exception"),
            "attributes": {"lei": lei, "category": category, "reason": reason,
                "reference": null}
        }
    })
}
