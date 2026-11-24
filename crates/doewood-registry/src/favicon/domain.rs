//! The domain of `GET /icons/{domain}`, validated before anything is fetched.

use std::fmt;

use url::{Host, Url};

/// The longest domain accepted, in ASCII (after IDNA), without a trailing dot.
pub const MAX_DOMAIN_LEN: usize = 253;
const MAX_LABEL_LEN: usize = 63;

/// A validated domain: lowercase ASCII (international names in punycode), at least two
/// labels of letters, digits and inner hyphens, at most 253 characters. Never an IP address.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Domain(String);

/// Why a domain was refused.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error("invalid domain: {0}")]
pub struct InvalidDomain(pub &'static str);

impl Domain {
    /// Validates `input`: a bare host name, such as `acme.com` or `bücher.de`. No scheme,
    /// port, path, user info or trailing dot; IP addresses in any notation are refused.
    pub fn parse(input: &str) -> Result<Self, InvalidDomain> {
        if input.is_empty() {
            return Err(InvalidDomain("empty"));
        }
        // Far beyond any real name even before IDNA; keeps the parser's input small.
        if input.len() > 4 * MAX_DOMAIN_LEN {
            return Err(InvalidDomain("too long"));
        }
        if input
            .chars()
            .any(|c| c.is_whitespace() || c.is_control() || "/\\?#@:%[]<>\"'`^{}|".contains(c))
        {
            return Err(InvalidDomain(
                "only a host name, without scheme, port, path or user info",
            ));
        }
        if input.ends_with('.') || input.ends_with('\u{3002}') {
            return Err(InvalidDomain("a trailing dot is not allowed"));
        }
        let url = Url::parse(&format!("https://{input}/"))
            .map_err(|_| InvalidDomain("not a valid host name"))?;
        match url.host() {
            Some(Host::Domain(d)) => check_ascii_domain(d).map(|()| Self(d.to_owned())),
            Some(Host::Ipv4(_) | Host::Ipv6(_)) => {
                Err(InvalidDomain("IP addresses are not allowed"))
            }
            None => Err(InvalidDomain("not a valid host name")),
        }
    }

    /// The domain, lowercase ASCII.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for Domain {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// Checks a host as the URL parser gave it (lowercase ASCII, IDNA applied): length, labels of
/// `[a-z0-9-]` not starting or ending with `-`, at least two labels, a top-level label that is
/// not all digits.
pub(crate) fn check_ascii_domain(d: &str) -> Result<(), InvalidDomain> {
    if d.is_empty() || d.len() > MAX_DOMAIN_LEN {
        return Err(InvalidDomain("longer than 253 characters"));
    }
    if d.ends_with('.') {
        return Err(InvalidDomain("a trailing dot is not allowed"));
    }
    let labels: Vec<&str> = d.split('.').collect();
    if labels.len() < 2 {
        return Err(InvalidDomain("needs at least two labels"));
    }
    for label in &labels {
        if label.is_empty() || label.len() > MAX_LABEL_LEN {
            return Err(InvalidDomain("each label is 1 to 63 characters"));
        }
        if !label
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
            || label.starts_with('-')
            || label.ends_with('-')
        {
            return Err(InvalidDomain(
                "labels are letters, digits and inner hyphens",
            ));
        }
    }
    if labels
        .last()
        .is_some_and(|tld| tld.bytes().all(|b| b.is_ascii_digit()))
    {
        return Err(InvalidDomain("IP addresses are not allowed"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn valid_domains_are_normalized() {
        assert_eq!(Domain::parse("acme.com").unwrap().as_str(), "acme.com");
        assert_eq!(Domain::parse("ACME.Com").unwrap().as_str(), "acme.com");
        assert_eq!(
            Domain::parse("bücher.de").unwrap().as_str(),
            "xn--bcher-kva.de"
        );
        assert_eq!(
            Domain::parse("xn--bcher-kva.de").unwrap().as_str(),
            "xn--bcher-kva.de"
        );
        assert_eq!(
            Domain::parse("www.sub-domain.acme.co.uk").unwrap().as_str(),
            "www.sub-domain.acme.co.uk"
        );
        // A full-width dot is a label separator under IDNA.
        assert_eq!(
            Domain::parse("acme\u{ff0e}com").unwrap().as_str(),
            "acme.com"
        );
        // 253 characters is the most.
        let longest = format!("{}com", "abcdefghi.".repeat(25));
        assert_eq!(Domain::parse(&longest).unwrap().as_str().len(), 253);
    }

    #[test]
    fn invalid_domains_are_refused() {
        let long_label = format!("{}.com", "a".repeat(64));
        let long_name = format!("{}com", "abcdefghi.".repeat(26));
        for bad in [
            "",
            "localhostonly",
            "acme.com.",
            "acme.com\u{3002}",
            ".acme.com",
            "acme..com",
            "https://acme.com",
            "acme.com/path",
            "acme.com:443",
            "acme.com:8080",
            "user@acme.com",
            "user:pw@acme.com",
            "acme.com?x=1",
            "acme.com#x",
            "acme com",
            "acme.com\n",
            "acme%2ecom",
            "-acme.com",
            "acme-.com",
            "ac_me.com",
            "acme.com\\evil",
            long_label.as_str(),
            long_name.as_str(),
            // IP literals in every notation the URL parser understands
            "127.0.0.1",
            "10.0.0.1",
            "2130706433",
            "0x7f.0.0.1",
            "0177.0.0.1",
            "127.1",
            "[::1]",
            "::1",
            "169.254.169.254",
            "1.2.3.4.5",
        ] {
            assert!(Domain::parse(bad).is_err(), "{bad:?} should be refused");
        }
    }
}
