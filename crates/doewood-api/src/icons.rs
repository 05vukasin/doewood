//! `GET /icons/{domain}`: company icons, served from doewood (ADR 0002 section 5).
//!
//! The browser only ever talks to doewood, so third parties never learn which companies a user
//! looks at: the icon is always proxied, never a redirect to the site. It is served on the API
//! port, next to gRPC-Web, so a frontend on the same origin can use it in an `<img>`.
//!
//! The resolver (fetching, the SSRF guard, the content checks, the cache) is behind the
//! [`Icons`] trait; the binary implements it with `doewood-registry`'s favicon resolver.
//!
//! Every answer carries `X-Content-Type-Options: nosniff`, a `Content-Security-Policy` that
//! allows nothing (`default-src 'none'; sandbox`), `Cross-Origin-Resource-Policy: same-origin`
//! and `Referrer-Policy: no-referrer`. An icon's `Content-Type` is the one derived from its
//! magic bytes, never the upstream one.

use std::sync::Arc;

use axum::Router;
use axum::extract::{Path, State};
use axum::http::{HeaderName, HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::get;

use crate::summarize::BoxFuture;

/// The path of the route.
pub const ICONS_PATH: &str = "/icons/{domain}";

/// The `Content-Type`s an icon may be served with.
pub const ICON_CONTENT_TYPES: &[&str] = &[
    "image/x-icon",
    "image/png",
    "image/gif",
    "image/jpeg",
    "image/webp",
    "image/avif",
];

/// `Cache-Control` of an icon: one day in the browser only.
pub const CACHE_HIT: &str = "private, max-age=86400";
/// `Cache-Control` of a miss: six hours (the server keeps a miss as long).
pub const CACHE_MISS: &str = "private, max-age=21600";
/// `Content-Security-Policy` of every answer.
pub const CSP: &str = "default-src 'none'; sandbox";

/// The answer for one domain.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum IconAnswer {
    /// An icon that passed the checks: `200` with these bytes.
    Found {
        /// From the magic bytes; one of [`ICON_CONTENT_TYPES`].
        content_type: &'static str,
        /// The icon.
        bytes: Vec<u8>,
    },
    /// No usable icon: `404`.
    Missing,
    /// The domain is not valid: `400` with the reason.
    Invalid(String),
    /// Too many lookups at once: `503` with `Retry-After`.
    Busy,
}

/// Finds the icon of a domain (the binary implements it).
pub trait Icons: Send + Sync {
    /// The icon of `domain`, as given in the path.
    fn icon(&self, domain: String) -> BoxFuture<'_, IconAnswer>;
}

/// The router of `GET /icons/{domain}`. Without `icons` every domain is a miss.
pub(crate) fn router(icons: Option<Arc<dyn Icons>>) -> Router {
    Router::new().route(ICONS_PATH, get(icon)).with_state(icons)
}

async fn icon(State(icons): State<Option<Arc<dyn Icons>>>, Path(domain): Path<String>) -> Response {
    let answer = match icons {
        Some(icons) => icons.icon(domain).await,
        None => IconAnswer::Missing,
    };
    respond(answer)
}

/// The HTTP answer for `answer`.
pub fn respond(answer: IconAnswer) -> Response {
    let (status, content_type, cache, body, retry) = match answer {
        IconAnswer::Found {
            content_type,
            bytes,
        } if ICON_CONTENT_TYPES.contains(&content_type) => {
            (StatusCode::OK, content_type, CACHE_HIT, bytes, false)
        }
        IconAnswer::Found { .. } | IconAnswer::Missing => (
            StatusCode::NOT_FOUND,
            "text/plain; charset=utf-8",
            CACHE_MISS,
            Vec::new(),
            false,
        ),
        IconAnswer::Invalid(reason) => (
            StatusCode::BAD_REQUEST,
            "text/plain; charset=utf-8",
            "no-store",
            reason.into_bytes(),
            false,
        ),
        IconAnswer::Busy => (
            StatusCode::SERVICE_UNAVAILABLE,
            "text/plain; charset=utf-8",
            "no-store",
            Vec::new(),
            true,
        ),
    };
    let mut resp = (status, body).into_response();
    let h = resp.headers_mut();
    h.insert(header::CONTENT_TYPE, HeaderValue::from_static(content_type));
    h.insert(header::CACHE_CONTROL, HeaderValue::from_static(cache));
    h.insert(
        header::X_CONTENT_TYPE_OPTIONS,
        HeaderValue::from_static("nosniff"),
    );
    h.insert(
        header::CONTENT_SECURITY_POLICY,
        HeaderValue::from_static(CSP),
    );
    h.insert(
        HeaderName::from_static("cross-origin-resource-policy"),
        HeaderValue::from_static("same-origin"),
    );
    h.insert(
        header::REFERRER_POLICY,
        HeaderValue::from_static("no-referrer"),
    );
    if retry {
        h.insert(header::RETRY_AFTER, HeaderValue::from_static("5"));
    }
    resp
}
