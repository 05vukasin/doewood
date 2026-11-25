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

/// An API key, sent as the user name of HTTP basic auth (Companies House). Its `Debug` never
/// shows it, it has no `Display`, and nothing in this crate puts it in a warning or a log.
#[derive(Clone, PartialEq, Eq)]
pub struct ApiKey(String);

impl ApiKey {
    /// The shortest key accepted (so that redacting it from a text is meaningful).
    pub const MIN_CHARS: usize = 8;
    /// The longest key accepted.
    pub const MAX_CHARS: usize = 256;

    /// `key`, trimmed: printable ASCII without spaces or `:`, 8 to 256 characters. The error
    /// never contains the key.
    pub fn new(key: &str) -> Result<Self, String> {
        let k = key.trim();
        if !(Self::MIN_CHARS..=Self::MAX_CHARS).contains(&k.len())
            || !k.bytes().all(|b| b.is_ascii_graphic() && b != b':')
        {
            return Err(format!(
                "an API key is {} to {} printable ASCII characters, without spaces or ':'",
                Self::MIN_CHARS,
                Self::MAX_CHARS
            ));
        }
        Ok(Self(k.to_owned()))
    }

    /// `text` with every occurrence of the key replaced by `<redacted>`.
    pub fn redact(&self, text: &str) -> String {
        text.replace(&self.0, "<redacted>")
    }

    pub(crate) fn expose(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for ApiKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("ApiKey(<redacted>)")
    }
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
    /// HTTP basic auth with this key as the user name and an empty password.
    pub basic_auth: Option<&'a ApiKey>,
}

impl<'a> Request<'a> {
    /// `GET url`, asking for `accept`.
    pub fn get(url: &'a Url, accept: &'a str) -> Self {
        Self {
            url,
            accept,
            json: None,
            basic_auth: None,
        }
    }

    /// `POST url` with the JSON `body`, asking for `accept`.
    pub fn post_json(url: &'a Url, accept: &'a str, body: &'a [u8]) -> Self {
        Self {
            url,
            accept,
            json: Some(body),
            basic_auth: None,
        }
    }

    /// The same request with HTTP basic auth: `key` as the user name, no password.
    #[must_use]
    pub fn with_basic_auth(self, key: &'a ApiKey) -> Self {
        Self {
            basic_auth: Some(key),
            ..self
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
        Self::build(timeout, connect_timeout, user_agent, None)
    }

    /// As [`HttpTransport::new`], also trusting the certificates of the PEM bundle
    /// `extra_roots` next to the system's roots (APR's server sends no intermediate
    /// certificate; the operator may supply it). The system's roots are kept.
    pub fn with_extra_roots(
        timeout: Duration,
        connect_timeout: Duration,
        extra_roots: &str,
    ) -> Result<Self, String> {
        Self::build(timeout, connect_timeout, USER_AGENT, Some(extra_roots))
    }

    fn build(
        timeout: Duration,
        connect_timeout: Duration,
        user_agent: &str,
        extra_roots: Option<&str>,
    ) -> Result<Self, String> {
        static PROVIDER: Once = Once::new();
        PROVIDER.call_once(|| {
            // Another component may have installed one already; either is fine.
            let _ = rustls::crypto::ring::default_provider().install_default();
        });
        let mut builder = reqwest::Client::builder()
            .connect_timeout(connect_timeout)
            .timeout(timeout)
            .user_agent(user_agent);
        if let Some(pem) = extra_roots {
            let certs = reqwest::Certificate::from_pem_bundle(pem.as_bytes())
                .map_err(|e| format!("extra root certificates: {e}"))?;
            if certs.is_empty() {
                return Err("extra root certificates: no certificate in the PEM".to_owned());
            }
            builder = builder.tls_certs_merge(certs);
        }
        let client = builder.build().map_err(|e| e.to_string())?;
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
        let mut builder = match request.json {
            None => self.client.get(request.url.clone()),
            Some(body) => self
                .client
                .post(request.url.clone())
                .header(CONTENT_TYPE, "application/json")
                .body(body.to_vec()),
        };
        if let Some(key) = request.basic_auth {
            // reqwest marks the header sensitive, so it is not in its `Debug` either.
            builder = builder.basic_auth(key.expose(), None::<&str>);
        }
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
