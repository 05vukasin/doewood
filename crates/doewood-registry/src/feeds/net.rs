//! The streaming side of the feeds: a `GET` whose body is read chunk by chunk (into a capped
//! temporary file, or line by line with a capped line length), never held whole.

use std::io::Write as _;
use std::sync::Once;
use std::time::Duration;

use reqwest::header::{ACCEPT, RETRY_AFTER};
use url::Url;

use crate::http::ApiKey;
use crate::warning::{Registry, Warning, WarningKind};

/// A streaming HTTP client for one feed.
#[derive(Clone, Debug)]
pub(crate) struct StreamClient {
    registry: Registry,
    client: reqwest::Client,
    key: Option<ApiKey>,
}

impl StreamClient {
    /// A client whose requests send `user_agent`. `timeout`: the whole exchange (`None` for a
    /// long-lived stream, which [`Lines`] bounds by an idle timeout instead).
    pub(crate) fn new(
        registry: Registry,
        user_agent: &str,
        timeout: Option<Duration>,
        connect_timeout: Duration,
        key: Option<ApiKey>,
    ) -> Result<Self, Warning> {
        static PROVIDER: Once = Once::new();
        PROVIDER.call_once(|| {
            let _ = rustls::crypto::ring::default_provider().install_default();
        });
        let mut builder = reqwest::Client::builder()
            .connect_timeout(connect_timeout)
            .user_agent(user_agent);
        if let Some(t) = timeout {
            builder = builder.timeout(t);
        }
        let client = builder
            .build()
            .map_err(|e| Warning::new(registry, WarningKind::Config(e.to_string())))?;
        Ok(Self {
            registry,
            client,
            key,
        })
    }

    pub(crate) fn warning(&self, kind: WarningKind) -> Warning {
        let kind = match (&self.key, kind) {
            (Some(k), WarningKind::Transport(t)) => WarningKind::Transport(k.redact(&t)),
            (Some(k), WarningKind::Malformed(t)) => WarningKind::Malformed(k.redact(&t)),
            (_, kind) => kind,
        };
        Warning::new(self.registry, kind)
    }

    /// `GET url`: the response once its status is 2xx; `Ok(None)` for a 404. A 401 is
    /// [`WarningKind::KeyRefused`], a 429 [`WarningKind::RateLimited`] (with `Retry-After` in
    /// seconds), any other status [`WarningKind::Status`]. Nothing is retried here: a feed
    /// tries again on its next run.
    pub(crate) async fn get(
        &self,
        url: &Url,
        accept: &str,
    ) -> Result<Option<reqwest::Response>, Warning> {
        let mut req = self.client.get(url.clone()).header(ACCEPT, accept);
        if let Some(k) = &self.key {
            req = req.basic_auth(k.expose(), None::<&str>);
        }
        let resp = req.send().await.map_err(|e| self.warning(transport(e)))?;
        let status = resp.status().as_u16();
        match status {
            200..=299 => Ok(Some(resp)),
            404 => Ok(None),
            401 => Err(self.warning(WarningKind::KeyRefused)),
            429 => {
                let after = resp
                    .headers()
                    .get(RETRY_AFTER)
                    .and_then(|v| v.to_str().ok())
                    .and_then(|v| v.trim().parse::<u64>().ok())
                    .map(Duration::from_secs);
                Err(self.warning(WarningKind::RateLimited { retry_after: after }))
            }
            s => Err(self.warning(WarningKind::Status(s))),
        }
    }

    /// The body of `resp`, at most `max` bytes (a larger one is [`WarningKind::TooLarge`]).
    pub(crate) async fn body(
        &self,
        mut resp: reqwest::Response,
        max: usize,
    ) -> Result<Vec<u8>, Warning> {
        let too_large = || self.warning(WarningKind::TooLarge { limit: max });
        if resp
            .content_length()
            .is_some_and(|n| n > u64::try_from(max).unwrap_or(u64::MAX))
        {
            return Err(too_large());
        }
        let mut body = Vec::new();
        while let Some(chunk) = resp.chunk().await.map_err(|e| self.warning(transport(e)))? {
            if body.len() + chunk.len() > max {
                return Err(too_large());
            }
            body.extend_from_slice(&chunk);
        }
        Ok(body)
    }

    /// The body of `resp` into `file`, at most `max` bytes (a larger one is
    /// [`WarningKind::TooLarge`]). Returns the bytes written.
    pub(crate) async fn spool(
        &self,
        mut resp: reqwest::Response,
        file: &mut std::fs::File,
        max: u64,
    ) -> Result<u64, Warning> {
        let too_large = || {
            self.warning(WarningKind::TooLarge {
                limit: usize::try_from(max).unwrap_or(usize::MAX),
            })
        };
        if resp.content_length().is_some_and(|n| n > max) {
            return Err(too_large());
        }
        let mut written = 0u64;
        while let Some(chunk) = resp.chunk().await.map_err(|e| self.warning(transport(e)))? {
            written += chunk.len() as u64;
            if written > max {
                return Err(too_large());
            }
            file.write_all(&chunk)
                .map_err(|e| self.warning(WarningKind::Transport(format!("spool: {e}"))))?;
        }
        file.flush()
            .map_err(|e| self.warning(WarningKind::Transport(format!("spool: {e}"))))?;
        Ok(written)
    }
}

/// The lines of a response body, read as it arrives. A line longer than the cap is skipped
/// (counted in [`Lines::overlong`]), never buffered whole.
pub(crate) struct Lines {
    resp: reqwest::Response,
    buf: Vec<u8>,
    max_line: usize,
    idle: Option<Duration>,
    /// Skipping the rest of an overlong line.
    skipping: bool,
    done: bool,
    /// Bytes read so far.
    pub read: u64,
    /// Lines skipped for their length.
    pub overlong: u64,
}

/// What [`Lines::next`] gives.
pub(crate) enum Line {
    /// A line, without its `\n` (and `\r`).
    Text(Vec<u8>),
    /// The body ended.
    End,
}

impl Lines {
    /// Lines of `resp`, each at most `max_line` bytes. `idle`: the longest wait for the next
    /// chunk (a stream's heartbeats keep it alive); `None` relies on the client's timeout.
    pub(crate) fn new(resp: reqwest::Response, max_line: usize, idle: Option<Duration>) -> Self {
        Self {
            resp,
            buf: Vec::new(),
            max_line,
            idle,
            skipping: false,
            done: false,
            read: 0,
            overlong: 0,
        }
    }

    /// The next line. A transport failure or an idle timeout is the warning of `client`.
    pub(crate) async fn next(&mut self, client: &StreamClient) -> Result<Line, Warning> {
        loop {
            if let Some(i) = self.buf.iter().position(|b| *b == b'\n') {
                let mut line: Vec<u8> = self.buf.drain(..=i).collect();
                line.pop();
                if line.last() == Some(&b'\r') {
                    line.pop();
                }
                if self.skipping {
                    self.skipping = false;
                    continue;
                }
                return Ok(Line::Text(line));
            }
            if self.buf.len() > self.max_line {
                if !self.skipping {
                    self.overlong += 1;
                }
                self.skipping = true;
                self.buf.clear();
            }
            if self.done {
                if self.buf.is_empty() || self.skipping {
                    return Ok(Line::End);
                }
                return Ok(Line::Text(std::mem::take(&mut self.buf)));
            }
            let chunk = match self.idle {
                Some(idle) => match tokio::time::timeout(idle, self.resp.chunk()).await {
                    Ok(c) => c,
                    Err(_) => return Err(client.warning(WarningKind::Timeout)),
                },
                None => self.resp.chunk().await,
            };
            match chunk.map_err(|e| client.warning(transport(e)))? {
                Some(c) => {
                    self.read += c.len() as u64;
                    self.buf.extend_from_slice(&c);
                }
                None => self.done = true,
            }
        }
    }
}

fn transport(e: reqwest::Error) -> WarningKind {
    if e.is_timeout() {
        return WarningKind::Timeout;
    }
    let e = e.without_url();
    let mut msg = e.to_string();
    let mut source = std::error::Error::source(&e);
    while let Some(s) = source {
        msg.push_str(": ");
        msg.push_str(&s.to_string());
        source = s.source();
    }
    WarningKind::Transport(msg)
}
