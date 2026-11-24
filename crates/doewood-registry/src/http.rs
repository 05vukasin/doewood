//! The HTTP side: a `GET` or a JSON `POST` that reads at most a capped body within a timeout,
//! behind the [`Transport`] trait.

use std::fmt;
use std::sync::Once;
use std::time::Duration;

use async_trait::async_trait;
use reqwest::header::{ACCEPT, CONTENT_TYPE, RETRY_AFTER};
use url::Url;

/// The `User-Agent` of every registry request: generic, with the project's URL as the contact
/// (the Wikimedia User-Agent policy's `<client>/<version> (<contact>)`). Never a person's name
/// or email.
pub const USER_AGENT: &str = concat!(
    "doewood/",
    env!("CARGO_PKG_VERSION"),
    " (+https://github.com/05vukasin/doewood)"
);

/// An HTTP answer, its body read in full and within the cap.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Response {
    /// The status code.
    pub status: u16,
    /// The `Retry-After` header, as sent.
    pub retry_after: Option<String>,
    /// The body.
    pub body: Vec<u8>,
}

/// One request: a `GET`, or a `POST` of a JSON body.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Request<'a> {
    /// The URL.
    pub url: &'a Url,
    /// The `Accept` header.
    pub accept: &'a str,
    /// `None`: a `GET`. `Some(body)`: a `POST` of `body` as `application/json`.
    pub json: Option<&'a [u8]>,
}

impl<'a> Request<'a> {
    /// `GET url`, asking for `accept`.
    pub fn get(url: &'a Url, accept: &'a str) -> Self {
        Self {
            url,
            accept,
            json: None,
        }
    }

    /// `POST url` with the JSON `body`, asking for `accept`.
    pub fn post_json(url: &'a Url, accept: &'a str, body: &'a [u8]) -> Self {
        Self {
            url,
            accept,
            json: Some(body),
        }
    }
}

/// Why a request gave no [`Response`].
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum TransportError {
    /// The request or reading the body took longer than the timeout.
    #[error("timed out")]
    Timeout,
    /// The body (or its declared `Content-Length`) exceeds the cap; reading stopped there.
    #[error("body larger than the cap")]
    TooLarge,
    /// Connection or protocol failure.
    #[error("{0}")]
    Other(String),
}

/// Sends a request and reads at most `max_body` bytes of the answer.
#[async_trait]
pub trait Transport: Send + Sync + fmt::Debug {
    /// Sends `request`.
    async fn send(
        &self,
        request: &Request<'_>,
        max_body: usize,
    ) -> Result<Response, TransportError>;
}

/// The real transport: `reqwest` (HTTP/1.1, rustls with `ring`).
#[derive(Clone, Debug)]
pub struct HttpTransport {
    client: reqwest::Client,
    timeout: Duration,
}

impl HttpTransport {
    /// A transport whose every request (connect, send, read the whole body) ends within
    /// `timeout`, connecting within `connect_timeout`.
    pub fn new(timeout: Duration, connect_timeout: Duration) -> Result<Self, String> {
        Self::with_user_agent(timeout, connect_timeout, USER_AGENT)
    }

    /// As [`HttpTransport::new`], sending `user_agent` instead of [`USER_AGENT`] (EDGAR's,
    /// with the operator's contact).
    pub fn with_user_agent(
        timeout: Duration,
        connect_timeout: Duration,
        user_agent: &str,
    ) -> Result<Self, String> {
        static PROVIDER: Once = Once::new();
        PROVIDER.call_once(|| {
            // Another component may have installed one already; either is fine.
            let _ = rustls::crypto::ring::default_provider().install_default();
        });
        let client = reqwest::Client::builder()
            .connect_timeout(connect_timeout)
            .timeout(timeout)
            .user_agent(user_agent)
            .build()
            .map_err(|e| e.to_string())?;
        Ok(Self { client, timeout })
    }
}

#[async_trait]
impl Transport for HttpTransport {
    async fn send(
        &self,
        request: &Request<'_>,
        max_body: usize,
    ) -> Result<Response, TransportError> {
        let builder = match request.json {
            None => self.client.get(request.url.clone()),
            Some(body) => self
                .client
                .post(request.url.clone())
                .header(CONTENT_TYPE, "application/json")
                .body(body.to_vec()),
        };
        let exchange = async {
            let mut resp = builder
                .header(ACCEPT, request.accept)
                .send()
                .await
                .map_err(transport)?;
            let status = resp.status().as_u16();
            let retry_after = resp
                .headers()
                .get(RETRY_AFTER)
                .and_then(|v| v.to_str().ok())
                .map(|v| v.trim().to_owned());
            let limit = u64::try_from(max_body).unwrap_or(u64::MAX);
            if resp.content_length().is_some_and(|n| n > limit) {
                return Err(TransportError::TooLarge);
            }
            let mut body = Vec::new();
            while let Some(chunk) = resp.chunk().await.map_err(transport)? {
                if body.len() + chunk.len() > max_body {
                    return Err(TransportError::TooLarge);
                }
                body.extend_from_slice(&chunk);
            }
            Ok(Response {
                status,
                retry_after,
                body,
            })
        };
        // reqwest's own timeout covers the body too; this one is the guarantee.
        tokio::time::timeout(self.timeout, exchange)
            .await
            .unwrap_or(Err(TransportError::Timeout))
    }
}

fn transport(e: reqwest::Error) -> TransportError {
    if e.is_timeout() {
        TransportError::Timeout
    } else {
        let e = e.without_url();
        let mut msg = e.to_string();
        let mut source = std::error::Error::source(&e);
        while let Some(s) = source {
            msg.push_str(": ");
            msg.push_str(&s.to_string());
            source = s.source();
        }
        TransportError::Other(msg)
    }
}
