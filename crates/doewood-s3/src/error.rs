use doewood_store::StoreError;

/// What can go wrong talking to S3 or scanning a source.
///
/// No variant carries a request URL: presigned URLs hold the access key id, the signature and
/// any session token, so they are never logged or put in an error.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum S3Error {
    /// The source's configuration cannot be used (endpoint, bucket name, URL style).
    #[error("invalid S3 configuration: {0}")]
    Config(String),
    /// The server answered with an error status, after any retries.
    #[error("S3 {op} failed: HTTP {status}{}{}", code.as_deref().map(|c| format!(" {c}")).unwrap_or_default(), message.as_deref().map(|m| format!(": {m}")).unwrap_or_default())]
    Status {
        /// The operation.
        op: &'static str,
        /// The HTTP status.
        status: u16,
        /// The S3 error code (`AccessDenied`, `NoSuchBucket`, `SlowDown`, …).
        code: Option<String>,
        /// The S3 error message.
        message: Option<String>,
    },
    /// The request did not complete: connection, TLS or timeout, after any retries.
    #[error("S3 {op} failed: {reason}")]
    Transport {
        /// The operation.
        op: &'static str,
        /// What happened, without the URL.
        reason: String,
    },
    /// A response that does not parse.
    #[error("S3 {op} returned an unreadable response: {reason}")]
    Response {
        /// The operation.
        op: &'static str,
        /// Why.
        reason: String,
    },
    /// An object larger than the source's size cap.
    #[error("object is larger than the {limit}-byte cap")]
    TooLarge {
        /// The cap, in bytes.
        limit: u64,
    },
    /// The listing is not in key order, so it cannot be compared with what is stored.
    #[error("the listing is not in key order ({0:?} came after a later key)")]
    Unordered(String),
    /// The store failed.
    #[error("store: {0}")]
    Store(#[from] StoreError),
    /// The event channel is closed: its consumer is gone.
    #[error("the event channel is closed")]
    ChannelClosed,
    /// The source was paused or removed during a pass.
    #[error("the source is not active")]
    Paused,
    /// Shutdown was requested.
    #[error("stopped")]
    Stopped,
}

impl S3Error {
    /// The HTTP status of a [`S3Error::Status`].
    pub fn status(&self) -> Option<u16> {
        match self {
            Self::Status { status, .. } => Some(*status),
            _ => None,
        }
    }
}

/// A reqwest error as text, without the URL.
pub(crate) fn transport(op: &'static str, e: reqwest::Error) -> S3Error {
    let kind = if e.is_timeout() {
        "timed out"
    } else if e.is_connect() {
        "cannot connect"
    } else {
        "request failed"
    };
    let e = e.without_url();
    // The source chain says what failed (DNS, TLS, reset), and carries no URL.
    let mut reason = kind.to_owned();
    let mut source = std::error::Error::source(&e);
    while let Some(s) = source {
        reason.push_str(": ");
        reason.push_str(&s.to_string());
        source = s.source();
    }
    S3Error::Transport { op, reason }
}
