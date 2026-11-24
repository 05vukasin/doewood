//! The typed warning every registry failure becomes.

use std::fmt;
use std::time::Duration;

use crate::vies::ViesError;

/// A registry client.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Registry {
    /// GLEIF, `api.gleif.org`.
    Gleif,
    /// The EU's VAT Information Exchange System (VIES), `ec.europa.eu`.
    Vies,
}

impl Registry {
    /// The short name in logs and cache keys.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Gleif => "gleif",
            Self::Vies => "vies",
        }
    }
}

impl fmt::Display for Registry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Why a registry request gave no answer.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum WarningKind {
    /// The registry is switched off in the configuration; nothing was sent.
    #[error("disabled in the configuration")]
    Disabled,
    /// The client configuration is unusable (for example the base URL).
    #[error("configuration: {0}")]
    Config(String),
    /// The input was refused before any request (an LEI with bad check digits, an empty name).
    #[error("invalid input: {0}")]
    InvalidInput(String),
    /// The request took longer than the timeout.
    #[error("timed out")]
    Timeout,
    /// Connection or protocol failure.
    #[error("transport: {0}")]
    Transport(String),
    /// The response body was larger than the cap; it was not parsed.
    #[error("response larger than {limit} bytes")]
    TooLarge {
        /// The cap, in bytes.
        limit: usize,
    },
    /// The registry answered 429 and the wait it asked for was too long, or retries ran out.
    #[error("rate limited (retry after {retry_after:?})")]
    RateLimited {
        /// The `Retry-After` the registry sent, when it sent seconds.
        retry_after: Option<Duration>,
    },
    /// An HTTP status other than success, 404 and 429.
    #[error("HTTP status {0}")]
    Status(u16),
    /// The body was not the JSON the client expects.
    #[error("malformed response: {0}")]
    Malformed(String),
    /// VIES answered with one of its error codes: it could not say whether the number is
    /// valid. Never an "invalid" answer, and never cached.
    #[error("VIES error {}", .0.code())]
    Vies(ViesError),
}

/// A registry failure: which registry and why. Never fatal; the caller logs it and goes on.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error("{registry}: {kind}")]
pub struct Warning {
    /// Which registry.
    pub registry: Registry,
    /// Why.
    pub kind: WarningKind,
}

impl Warning {
    /// A warning for `registry`.
    pub fn new(registry: Registry, kind: WarningKind) -> Self {
        Self { registry, kind }
    }
}
