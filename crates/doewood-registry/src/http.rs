//! The HTTP side: a `GET` that reads at most a capped body within a timeout, behind the
//! [`Transport`] trait.

use std::fmt;
use std::sync::Once;
use std::time::Duration;

use async_trait::async_trait;
use reqwest::header::{ACCEPT, RETRY_AFTER};
use url::Url;

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

/// Why a `GET` gave no [`Response`].
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

/// Sends a `GET` and reads at most `max_body` bytes of the answer.
#[async_trait]
pub trait Transport: Send + Sync + fmt::Debug {
    /// `GET url`, asking for `accept`.
    async fn get(
        &self,
        url: &Url,
        accept: &str,
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
        static PROVIDER: Once = Once::new();
        PROVIDER.call_once(|| {
            // Another component may have installed one already; either is fine.
            let _ = rustls::crypto::ring::default_provider().install_default();
        });
        let client = reqwest::Client::builder()
            .connect_timeout(connect_timeout)
            .timeout(timeout)
            .user_agent(concat!("doewood/", env!("CARGO_PKG_VERSION")))
            .build()
            .map_err(|e| e.to_string())?;
        Ok(Self { client, timeout })
    }
}

#[async_trait]
impl Transport for HttpTransport {
    async fn get(
        &self,
        url: &Url,
        accept: &str,
        max_body: usize,
    ) -> Result<Response, TransportError> {
        let request = async {
            let mut resp = self
                .client
                .get(url.clone())
                .header(ACCEPT, accept)
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
        tokio::time::timeout(self.timeout, request)
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
