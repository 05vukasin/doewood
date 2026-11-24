//! The SSRF guard (ADR 0002 section 5.3): an HTTP client that only reaches public sites.
//!
//! - **URLs:** `http` or `https` only, no user info, a domain name as the host (never an IP
//!   literal), a port the [`AddressPolicy`] allows (production: the scheme's default port),
//!   and a host name the policy allows (no special-use names such as `localhost`).
//! - **DNS is resolved here,** by the injected [`DnsResolver`], inside reqwest's resolver hook.
//!   Every address of the host must pass the policy, or the whole host is refused. reqwest then
//!   connects to exactly the addresses that were checked: there is no second lookup between
//!   the check and the connect, so DNS rebinding cannot swap in another address. The request
//!   keeps the original host name for `Host` and TLS SNI. Connections are not pooled, so every
//!   connection goes through the check.
//! - **Redirects are followed by hand,** at most [`MAX_REDIRECTS`]. Every hop is checked again
//!   (scheme, user info, host, port, resolution, policy), and `https` to `http` is refused.
//! - **An optional egress proxy:** every request then goes through it, and only its host name
//!   is resolved here. The URL checks still apply, but resolving the target (and so checking
//!   its addresses) is the proxy's job: deploy one that refuses private networks.
//! - **Timeouts** for the connect and the whole request, and a **body cap**.

use std::fmt;
use std::net::{IpAddr, SocketAddr};
use std::sync::{Arc, Once};
use std::time::Duration;

use async_trait::async_trait;
use reqwest::dns::{Addrs, Name, Resolve, Resolving};
use reqwest::header::{ACCEPT, CONTENT_TYPE, LOCATION};
use url::{Host, Url};

use super::addr::AddressPolicy;
use super::domain::check_ascii_domain;

/// The most redirects followed for one request.
pub const MAX_REDIRECTS: usize = 5;

/// Resolves a host name to its addresses.
#[async_trait]
pub trait DnsResolver: Send + Sync + fmt::Debug {
    /// Every address of `host`.
    async fn lookup(&self, host: &str) -> Result<Vec<IpAddr>, String>;
}

/// The system resolver (`getaddrinfo`, through tokio).
#[derive(Clone, Copy, Debug, Default)]
pub struct SystemDns;

#[async_trait]
impl DnsResolver for SystemDns {
    async fn lookup(&self, host: &str) -> Result<Vec<IpAddr>, String> {
        tokio::net::lookup_host((host, 0))
            .await
            .map(|addrs| addrs.map(|a| a.ip()).collect())
            .map_err(|e| e.to_string())
    }
}

/// Why the guard refused a URL or a host.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum Refusal {
    /// Not `http` or `https`.
    #[error("scheme {0:?} is not allowed")]
    Scheme(String),
    /// The URL has a user name or password.
    #[error("user info in a URL is not allowed")]
    UserInfo,
    /// The host is an IP literal, or not a valid domain name.
    #[error("the host is not a valid domain name")]
    Host,
    /// The policy refuses the host name (a special-use name).
    #[error("host {0} is not allowed")]
    Name(String),
    /// The policy refuses the port.
    #[error("port {0} is not allowed")]
    Port(u16),
    /// A redirect from `https` to `http`.
    #[error("redirect from https to http")]
    Downgrade,
    /// More than [`MAX_REDIRECTS`] redirects.
    #[error("more than {MAX_REDIRECTS} redirects")]
    TooManyRedirects,
    /// A redirect without a usable `Location`.
    #[error("redirect without a valid Location")]
    BadRedirect,
    /// One of the host's addresses is not allowed.
    #[error("{0} resolves to an address that is not public")]
    Address(String),
    /// The host has no address.
    #[error("{0} has no address")]
    NoAddress(String),
    /// A name that is not the proxy was to be resolved in proxy mode.
    #[error("unexpected lookup of {0} through the egress proxy")]
    Unexpected(String),
}

/// Why a guarded request gave no answer.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum GuardError {
    /// The guard refused the request.
    #[error("refused: {0}")]
    Refused(Refusal),
    /// The request took longer than its timeout.
    #[error("timed out")]
    Timeout,
    /// The body is larger than the cap (with [`Oversize::Refuse`]).
    #[error("body larger than the cap")]
    TooLarge,
    /// DNS, connection or protocol failure.
    #[error("{0}")]
    Other(String),
}

impl From<Refusal> for GuardError {
    fn from(r: Refusal) -> Self {
        Self::Refused(r)
    }
}

/// What to do with a body over the cap.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Oversize {
    /// Fail with [`GuardError::TooLarge`] (icons).
    Refuse,
    /// Keep the first `max_body` bytes (a homepage: the `<head>` comes first).
    Truncate,
}

/// The final answer of a guarded request, after the redirects.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Fetched {
    /// The URL that answered (the last hop).
    pub url: Url,
    /// Its status (never a followed redirect).
    pub status: u16,
    /// Its `Content-Type`, as sent.
    pub content_type: Option<String>,
    /// The body, within the cap.
    pub body: Vec<u8>,
    /// How many redirects were followed.
    pub redirects: usize,
}

/// How the guarded client is built.
#[derive(Clone, Debug)]
pub struct GuardOptions {
    /// Connecting (DNS, TCP and TLS) ends within this.
    pub connect_timeout: Duration,
    /// Each hop (connect, send, read the whole body) ends within this.
    pub request_timeout: Duration,
    /// The egress proxy (`http://` or `https://`), if any.
    pub proxy: Option<Url>,
    /// The `User-Agent`.
    pub user_agent: String,
}

impl Default for GuardOptions {
    fn default() -> Self {
        Self {
            connect_timeout: Duration::from_secs(3),
            request_timeout: Duration::from_secs(5),
            proxy: None,
            user_agent: crate::USER_AGENT.to_owned(),
        }
    }
}

/// The error the resolver hook hands to reqwest, found again in its error chain.
#[derive(Debug)]
struct Refused(Refusal);

impl fmt::Display for Refused {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl std::error::Error for Refused {}

/// reqwest's resolver hook: resolves with the injected resolver and checks every address.
#[derive(Debug)]
struct Vetting {
    dns: Arc<dyn DnsResolver>,
    policy: Arc<dyn AddressPolicy>,
    /// In proxy mode, the proxy's host: the only name resolved, and not checked.
    proxy_host: Option<String>,
}

impl Resolve for Vetting {
    fn resolve(&self, name: Name) -> Resolving {
        let host = name.as_str().to_ascii_lowercase();
        let dns = Arc::clone(&self.dns);
        let policy = Arc::clone(&self.policy);
        let proxy_host = self.proxy_host.clone();
        Box::pin(async move {
            let checked = proxy_host.is_none();
            if let Some(p) = &proxy_host
                && *p != host
            {
                return Err(Refused(Refusal::Unexpected(host)).into());
            }
            let ips = dns.lookup(&host).await.map_err(|e| format!("DNS: {e}"))?;
            if ips.is_empty() {
                return Err(Refused(Refusal::NoAddress(host)).into());
            }
            if checked && ips.iter().any(|ip| !policy.allows_ip(*ip)) {
                return Err(Refused(Refusal::Address(host)).into());
            }
            let addrs: Addrs = Box::new(ips.into_iter().map(|ip| {
                let ip = match ip {
                    IpAddr::V6(v6) => v6.to_ipv4_mapped().map_or(IpAddr::V6(v6), IpAddr::V4),
                    v4 @ IpAddr::V4(_) => v4,
                };
                SocketAddr::new(ip, 0)
            }));
            Ok(addrs)
        })
    }
}

/// An HTTP client that only reaches what the [`AddressPolicy`] allows.
#[derive(Clone, Debug)]
pub struct GuardedClient {
    client: reqwest::Client,
    policy: Arc<dyn AddressPolicy>,
    timeout: Duration,
}

impl GuardedClient {
    /// A client resolving with `dns` and connecting only where `policy` allows.
    pub fn new(
        options: &GuardOptions,
        dns: Arc<dyn DnsResolver>,
        policy: Arc<dyn AddressPolicy>,
    ) -> Result<Self, String> {
        static PROVIDER: Once = Once::new();
        PROVIDER.call_once(|| {
            // Another component may have installed one already; either is fine.
            let _ = rustls::crypto::ring::default_provider().install_default();
        });
        let proxy_host = match &options.proxy {
            None => None,
            Some(p) => {
                if !matches!(p.scheme(), "http" | "https") {
                    return Err("the egress proxy must be an http:// or https:// URL".to_owned());
                }
                if !p.username().is_empty() || p.password().is_some() {
                    return Err("the egress proxy URL must not contain credentials".to_owned());
                }
                match p.host() {
                    Some(Host::Domain(d)) => Some(d.to_ascii_lowercase()),
                    Some(Host::Ipv4(_) | Host::Ipv6(_)) => Some(String::new()),
                    None => return Err("the egress proxy URL has no host".to_owned()),
                }
            }
        };
        let mut builder = reqwest::Client::builder()
            .connect_timeout(options.connect_timeout)
            .timeout(options.request_timeout)
            .user_agent(options.user_agent.as_str())
            .redirect(reqwest::redirect::Policy::none())
            .pool_max_idle_per_host(0)
            .referer(false)
            .dns_resolver(Arc::new(Vetting {
                dns,
                policy: Arc::clone(&policy),
                proxy_host,
            }))
            .no_proxy();
        if let Some(p) = &options.proxy {
            builder = builder.proxy(reqwest::Proxy::all(p.as_str()).map_err(|e| e.to_string())?);
        }
        let client = builder.build().map_err(|e| e.to_string())?;
        Ok(Self {
            client,
            policy,
            timeout: options.request_timeout,
        })
    }

    /// Checks `url` before it is requested: scheme, user info, host, port; and, for a
    /// redirect `from` another URL, no `https` to `http` downgrade.
    pub fn check_url(&self, url: &Url, from: Option<&Url>) -> Result<(), Refusal> {
        check_url(self.policy.as_ref(), url, from)
    }

    /// `GET url`, following redirects by hand (each hop checked), reading at most
    /// `max_body` bytes of the final answer.
    pub async fn get(
        &self,
        url: Url,
        accept: &str,
        max_body: usize,
        oversize: Oversize,
    ) -> Result<Fetched, GuardError> {
        let mut url = url;
        let mut from: Option<Url> = None;
        let mut redirects = 0;
        loop {
            self.check_url(&url, from.as_ref())?;
            let (status, location, content_type, body) =
                self.hop(&url, accept, max_body, oversize).await?;
            if matches!(status, 301 | 302 | 303 | 307 | 308) {
                if redirects == MAX_REDIRECTS {
                    return Err(Refusal::TooManyRedirects.into());
                }
                let next = location
                    .and_then(|l| url.join(&l).ok())
                    .ok_or(Refusal::BadRedirect)?;
                redirects += 1;
                from = Some(std::mem::replace(&mut url, next));
                continue;
            }
            return Ok(Fetched {
                url,
                status,
                content_type,
                body,
                redirects,
            });
        }
    }

    /// One request, no redirect followed: status, `Location`, `Content-Type`, body.
    async fn hop(
        &self,
        url: &Url,
        accept: &str,
        max_body: usize,
        oversize: Oversize,
    ) -> Result<(u16, Option<String>, Option<String>, Vec<u8>), GuardError> {
        let exchange = async {
            let mut resp = self
                .client
                .get(url.clone())
                .header(ACCEPT, accept)
                .send()
                .await
                .map_err(error)?;
            let status = resp.status().as_u16();
            let header = |name| {
                resp.headers()
                    .get(name)
                    .and_then(|v: &reqwest::header::HeaderValue| v.to_str().ok())
                    .map(|v| v.trim().to_owned())
            };
            let location = header(LOCATION);
            let content_type = header(CONTENT_TYPE);
            if (300..400).contains(&status) {
                return Ok((status, location, content_type, Vec::new()));
            }
            let limit = u64::try_from(max_body).unwrap_or(u64::MAX);
            if oversize == Oversize::Refuse && resp.content_length().is_some_and(|n| n > limit) {
                return Err(GuardError::TooLarge);
            }
            let mut body = Vec::new();
            while let Some(chunk) = resp.chunk().await.map_err(error)? {
                if body.len() + chunk.len() > max_body {
                    if oversize == Oversize::Refuse {
                        return Err(GuardError::TooLarge);
                    }
                    let room = max_body - body.len();
                    body.extend_from_slice(&chunk[..room]);
                    break;
                }
                body.extend_from_slice(&chunk);
            }
            Ok((status, location, content_type, body))
        };
        tokio::time::timeout(self.timeout, exchange)
            .await
            .unwrap_or(Err(GuardError::Timeout))
    }
}

/// The URL checks of [`GuardedClient::check_url`].
pub fn check_url(policy: &dyn AddressPolicy, url: &Url, from: Option<&Url>) -> Result<(), Refusal> {
    let scheme = url.scheme();
    if !matches!(scheme, "http" | "https") {
        return Err(Refusal::Scheme(scheme.to_owned()));
    }
    if from.is_some_and(|f| f.scheme() == "https") && scheme == "http" {
        return Err(Refusal::Downgrade);
    }
    if !url.username().is_empty() || url.password().is_some() {
        return Err(Refusal::UserInfo);
    }
    let Some(Host::Domain(host)) = url.host() else {
        return Err(Refusal::Host);
    };
    if check_ascii_domain(host).is_err() {
        return Err(Refusal::Host);
    }
    if !policy.allows_host(host) {
        return Err(Refusal::Name(host.to_owned()));
    }
    let port = url.port_or_known_default().ok_or(Refusal::Host)?;
    if !policy.allows_port(scheme, port) {
        return Err(Refusal::Port(port));
    }
    Ok(())
}

/// A reqwest error as a [`GuardError`]: the resolver hook's refusal is found again in the
/// error chain.
fn error(e: reqwest::Error) -> GuardError {
    if e.is_timeout() {
        return GuardError::Timeout;
    }
    let e = e.without_url();
    let mut source = std::error::Error::source(&e);
    let mut msg = e.to_string();
    while let Some(s) = source {
        if let Some(Refused(r)) = s.downcast_ref::<Refused>() {
            return GuardError::Refused(r.clone());
        }
        msg.push_str(": ");
        msg.push_str(&s.to_string());
        source = s.source();
    }
    GuardError::Other(msg)
}

#[cfg(test)]
mod tests {
    use super::super::addr::PublicOnly;
    use super::*;

    fn u(s: &str) -> Url {
        Url::parse(s).unwrap()
    }

    #[test]
    fn production_url_checks() {
        let p = PublicOnly;
        assert_eq!(
            check_url(&p, &u("https://acme.com/favicon.ico"), None),
            Ok(())
        );
        assert_eq!(check_url(&p, &u("http://acme.com/"), None), Ok(()));
        assert_eq!(check_url(&p, &u("https://acme.com:443/"), None), Ok(()));
        assert_eq!(
            check_url(&p, &u("ftp://acme.com/"), None),
            Err(Refusal::Scheme("ftp".into()))
        );
        assert_eq!(
            check_url(&p, &u("file:///etc/passwd"), None),
            Err(Refusal::Scheme("file".into()))
        );
        assert_eq!(
            check_url(&p, &u("gopher://acme.com/"), None),
            Err(Refusal::Scheme("gopher".into()))
        );
        assert_eq!(
            check_url(&p, &u("https://user:pw@acme.com/"), None),
            Err(Refusal::UserInfo)
        );
        assert_eq!(
            check_url(&p, &u("https://user@acme.com/"), None),
            Err(Refusal::UserInfo)
        );
        for ip in [
            "https://127.0.0.1/",
            "https://[::1]/",
            "https://2130706433/",
            "https://0x7f.1/",
            "https://169.254.169.254/latest/meta-data/",
            "https://[fd00:ec2::254]/",
        ] {
            assert_eq!(check_url(&p, &u(ip), None), Err(Refusal::Host), "{ip}");
        }
        assert_eq!(
            check_url(&p, &u("https://localhost/"), None),
            Err(Refusal::Host)
        );
        assert_eq!(
            check_url(&p, &u("https://metadata.google.internal/"), None),
            Err(Refusal::Name("metadata.google.internal".into()))
        );
        assert_eq!(
            check_url(&p, &u("https://acme.com./"), None),
            Err(Refusal::Host)
        );
        assert_eq!(
            check_url(&p, &u("https://acme.com:8443/"), None),
            Err(Refusal::Port(8443))
        );
        assert_eq!(
            check_url(&p, &u("http://acme.com:443/"), None),
            Err(Refusal::Port(443))
        );
        assert_eq!(
            check_url(&p, &u("https://acme.com:22/"), None),
            Err(Refusal::Port(22))
        );
    }

    #[test]
    fn https_to_http_is_a_downgrade() {
        let p = PublicOnly;
        let https = u("https://acme.com/");
        let http = u("http://acme.com/");
        assert_eq!(
            check_url(&p, &u("http://www.acme.com/"), Some(&https)),
            Err(Refusal::Downgrade)
        );
        assert_eq!(
            check_url(&p, &u("https://www.acme.com/"), Some(&https)),
            Ok(())
        );
        assert_eq!(check_url(&p, &u("https://acme.com/"), Some(&http)), Ok(()));
        assert_eq!(
            check_url(&p, &u("http://cdn.acme.com/"), Some(&http)),
            Ok(())
        );
    }

    #[test]
    fn proxy_urls_are_checked() {
        let dns: Arc<dyn DnsResolver> = Arc::new(SystemDns);
        let policy: Arc<dyn AddressPolicy> = Arc::new(PublicOnly);
        for bad in [
            "socks5://proxy.acme.com:1080",
            "http://user:pw@proxy.acme.com:3128",
        ] {
            let options = GuardOptions {
                proxy: Some(u(bad)),
                ..GuardOptions::default()
            };
            assert!(
                GuardedClient::new(&options, Arc::clone(&dns), Arc::clone(&policy)).is_err(),
                "{bad}"
            );
        }
        let options = GuardOptions {
            proxy: Some(u("http://egress.acme.com:3128")),
            ..GuardOptions::default()
        };
        assert!(GuardedClient::new(&options, dns, policy).is_ok());
    }
}
