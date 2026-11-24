//! Which addresses, ports and host names the favicon fetcher may connect to (ADR 0002 section
//! 5.3: every resolved address must be public).
//!
//! [`is_public`] is an allow-list for IPv6 (only global unicast `2000::/3`, minus the special
//! blocks inside it) and a deny-list for IPv4 (every special-purpose block of the IANA
//! registry). IPv4-mapped IPv6 addresses are judged as the IPv4 address they carry; every other
//! IPv6 form that embeds an IPv4 address (IPv4-compatible, NAT64, 6to4, Teredo) is refused
//! whatever it embeds.
//!
//! [`AddressPolicy`] is the decision behind a trait so tests can allow exactly one loopback mock
//! server; production uses [`PublicOnly`].

use std::fmt;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

/// Whether `ip` is a public unicast address that the fetcher may connect to.
pub fn is_public(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => is_public_v4(v4),
        IpAddr::V6(v6) => match v6.to_ipv4_mapped() {
            Some(v4) => is_public_v4(v4),
            None => is_public_v6(v6),
        },
    }
}

/// IPv4 blocks that are not public unicast (the IANA IPv4 special-purpose registry, plus
/// multicast and the reserved 240/4, which includes the broadcast address).
const V4_SPECIAL: &[(Ipv4Addr, u8)] = &[
    (Ipv4Addr::UNSPECIFIED, 8),           // "this network", 0.0.0.0
    (Ipv4Addr::new(10, 0, 0, 0), 8),      // private
    (Ipv4Addr::new(100, 64, 0, 0), 10),   // shared address space (CGNAT)
    (Ipv4Addr::new(127, 0, 0, 0), 8),     // loopback
    (Ipv4Addr::new(169, 254, 0, 0), 16),  // link-local, cloud metadata 169.254.169.254
    (Ipv4Addr::new(172, 16, 0, 0), 12),   // private
    (Ipv4Addr::new(192, 0, 0, 0), 24),    // IETF protocol assignments
    (Ipv4Addr::new(192, 0, 2, 0), 24),    // documentation TEST-NET-1
    (Ipv4Addr::new(192, 31, 196, 0), 24), // AS112-v4
    (Ipv4Addr::new(192, 52, 193, 0), 24), // AMT
    (Ipv4Addr::new(192, 88, 99, 0), 24),  // deprecated 6to4 relay anycast
    (Ipv4Addr::new(192, 168, 0, 0), 16),  // private
    (Ipv4Addr::new(192, 175, 48, 0), 24), // direct delegation AS112
    (Ipv4Addr::new(198, 18, 0, 0), 15),   // benchmarking
    (Ipv4Addr::new(198, 51, 100, 0), 24), // documentation TEST-NET-2
    (Ipv4Addr::new(203, 0, 113, 0), 24),  // documentation TEST-NET-3
    (Ipv4Addr::new(224, 0, 0, 0), 4),     // multicast
    (Ipv4Addr::new(240, 0, 0, 0), 4),     // reserved, and 255.255.255.255 broadcast
];

fn in_v4(ip: Ipv4Addr, net: Ipv4Addr, len: u8) -> bool {
    let mask = if len == 0 {
        0
    } else {
        u32::MAX << (32 - u32::from(len))
    };
    u32::from(ip) & mask == u32::from(net) & mask
}

fn is_public_v4(ip: Ipv4Addr) -> bool {
    !V4_SPECIAL.iter().any(|(net, len)| in_v4(ip, *net, *len))
}

/// Blocks inside `2000::/3` that are not public unicast.
const V6_SPECIAL: &[(Ipv6Addr, u8)] = &[
    // IETF protocol assignments: Teredo 2001::/32, benchmarking 2001:2::/48, ORCHID, ...
    (Ipv6Addr::new(0x2001, 0, 0, 0, 0, 0, 0, 0), 23),
    (Ipv6Addr::new(0x2001, 0xdb8, 0, 0, 0, 0, 0, 0), 32), // documentation
    (Ipv6Addr::new(0x2002, 0, 0, 0, 0, 0, 0, 0), 16),     // 6to4 (embeds an IPv4 address)
    (Ipv6Addr::new(0x3fff, 0, 0, 0, 0, 0, 0, 0), 20),     // documentation (RFC 9637)
];

fn in_v6(ip: Ipv6Addr, net: Ipv6Addr, len: u8) -> bool {
    let mask = if len == 0 {
        0
    } else {
        u128::MAX << (128 - u32::from(len))
    };
    u128::from(ip) & mask == u128::from(net) & mask
}

fn is_public_v6(ip: Ipv6Addr) -> bool {
    // Only global unicast. This leaves out ::, ::1, IPv4-compatible ::/96, NAT64 64:ff9b::/96
    // and 64:ff9b:1::/48, discard-only 100::/64, unique-local fc00::/7 (fd00:ec2::254 among
    // them), link-local fe80::/10, site-local fec0::/10 and multicast ff00::/8.
    in_v6(ip, Ipv6Addr::new(0x2000, 0, 0, 0, 0, 0, 0, 0), 3)
        && !V6_SPECIAL.iter().any(|(net, len)| in_v6(ip, *net, *len))
}

/// Host names that never name a public site: special-use names (RFC 6761, RFC 6762, RFC 8375,
/// RFC 9476) and common internal suffixes. Compared as the last label(s) of the host.
const SPECIAL_SUFFIXES: &[&str] = &[
    "localhost",
    "local",
    "localdomain",
    "internal",
    "intranet",
    "lan",
    "home",
    "corp",
    "private",
    "home.arpa",
    "arpa",
    "test",
    "example",
    "invalid",
    "onion",
    "alt",
];

/// Whether `host` (a validated, lowercase ASCII domain) ends in a special-use name.
pub fn is_special_name(host: &str) -> bool {
    SPECIAL_SUFFIXES
        .iter()
        .any(|s| host == *s || host.strip_suffix(s).is_some_and(|h| h.ends_with('.')))
}

/// What the fetcher may connect to. Every resolved address of a host must pass
/// [`AddressPolicy::allows_ip`], or the whole host is refused.
pub trait AddressPolicy: Send + Sync + fmt::Debug {
    /// Whether a host name may be fetched at all (before any DNS lookup).
    fn allows_host(&self, host: &str) -> bool;
    /// Whether `port` may be used with `scheme` (`http` or `https`).
    fn allows_port(&self, scheme: &str, port: u16) -> bool;
    /// Whether a resolved address may be connected to.
    fn allows_ip(&self, ip: IpAddr) -> bool;
}

/// The production policy: public addresses only ([`is_public`]), the default ports only (80
/// for `http`, 443 for `https`), and no special-use host names ([`is_special_name`]).
#[derive(Clone, Copy, Debug, Default)]
pub struct PublicOnly;

impl AddressPolicy for PublicOnly {
    fn allows_host(&self, host: &str) -> bool {
        !is_special_name(host)
    }

    fn allows_port(&self, scheme: &str, port: u16) -> bool {
        matches!((scheme, port), ("http", 80) | ("https", 443))
    }

    fn allows_ip(&self, ip: IpAddr) -> bool {
        is_public(ip)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ip(s: &str) -> IpAddr {
        s.parse().unwrap()
    }

    #[test]
    fn public_addresses_pass() {
        for s in [
            "1.1.1.1",
            "8.8.8.8",
            "93.184.215.14",
            "100.63.255.255",
            "100.128.0.0",
            "172.15.255.255",
            "172.32.0.0",
            "192.167.255.255",
            "192.169.0.0",
            "198.17.255.255",
            "198.20.0.0",
            "223.255.255.255",
            "2606:4700:4700::1111",
            "2a00:1450:4001::200e",
            "2001:4860:4860::8888",
            "::ffff:8.8.8.8",
        ] {
            assert!(is_public(ip(s)), "{s} should be public");
        }
    }

    #[test]
    fn every_special_range_is_refused() {
        for s in [
            // loopback
            "127.0.0.1",
            "127.255.255.254",
            "::1",
            // private
            "10.0.0.1",
            "10.255.255.255",
            "172.16.0.1",
            "172.31.255.255",
            "192.168.0.1",
            "192.168.255.255",
            // CGNAT
            "100.64.0.1",
            "100.127.255.255",
            // link-local and cloud metadata
            "169.254.0.1",
            "169.254.169.254",
            "fe80::1",
            "febf:ffff::1",
            "fd00:ec2::254",
            // unique-local
            "fc00::1",
            "fdff:ffff::1",
            // multicast
            "224.0.0.1",
            "239.255.255.250",
            "ff02::1",
            "ff0e::1",
            // broadcast and reserved
            "255.255.255.255",
            "240.0.0.1",
            "250.1.2.3",
            // unspecified and "this network"
            "0.0.0.0",
            "0.1.2.3",
            "::",
            // documentation and benchmarking
            "192.0.2.1",
            "198.51.100.1",
            "203.0.113.1",
            "198.18.0.1",
            "198.19.255.255",
            "2001:db8::1",
            "3fff::1",
            "2001:2::1",
            // IETF protocol assignments and relays
            "192.0.0.8",
            "192.88.99.1",
            // site-local, discard-only
            "fec0::1",
            "100::1",
        ] {
            assert!(!is_public(ip(s)), "{s} should be refused");
            assert!(
                !PublicOnly.allows_ip(ip(s)),
                "{s} should be refused by policy"
            );
        }
    }

    #[test]
    fn embedded_ipv4_forms_are_refused() {
        for s in [
            // IPv4-mapped forms of special addresses
            "::ffff:127.0.0.1",
            "::ffff:10.0.0.1",
            "::ffff:169.254.169.254",
            "::ffff:192.168.1.1",
            "::ffff:0.0.0.0",
            // IPv4-compatible (deprecated): refused even with a public IPv4 inside
            "::127.0.0.1",
            "::8.8.8.8",
            // NAT64, well-known and local-use prefixes
            "64:ff9b::7f00:1",
            "64:ff9b::a9fe:a9fe",
            "64:ff9b::808:808",
            "64:ff9b:1::a00:1",
            // 6to4
            "2002:7f00:1::1",
            "2002:a9fe:a9fe::1",
            "2002:808:808::1",
            // Teredo
            "2001:0:4136:e378:8000:63bf:3fff:fdd2",
            "2001::1",
        ] {
            assert!(!is_public(ip(s)), "{s} should be refused");
        }
    }

    #[test]
    fn only_default_ports() {
        assert!(PublicOnly.allows_port("https", 443));
        assert!(PublicOnly.allows_port("http", 80));
        assert!(!PublicOnly.allows_port("https", 80));
        assert!(!PublicOnly.allows_port("http", 443));
        assert!(!PublicOnly.allows_port("https", 8443));
        assert!(!PublicOnly.allows_port("http", 22));
        assert!(!PublicOnly.allows_port("ftp", 21));
    }

    #[test]
    fn special_names_are_refused() {
        for h in [
            "localhost",
            "app.localhost",
            "printer.local",
            "metadata.google.internal",
            "router.home.arpa",
            "1.0.0.127.in-addr.arpa",
            "acme.test",
            "acme.example",
            "acme.invalid",
            "hidden.onion",
        ] {
            assert!(is_special_name(h), "{h}");
            assert!(!PublicOnly.allows_host(h), "{h}");
        }
        for h in [
            "acme.com",
            "example.com",
            "testing.org",
            "mylocal.de",
            "arpa.io",
        ] {
            assert!(!is_special_name(h), "{h}");
            assert!(PublicOnly.allows_host(h), "{h}");
        }
    }
}
