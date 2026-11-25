# Company icons

doewood serves company icons itself, at `GET /icons/{domain}` on the API port
([ADR 0002](adr/0002-company-enrichment.md), section 5). The browser only ever talks to doewood,
so no third party learns which companies a user looks at: the icon is always fetched by doewood
and proxied, never a redirect to the site. The HTTP side (answers, headers) is in
[api.md](api.md#company-icons); this page is about how an icon is found and checked.

The code is `crates/doewood-registry/src/favicon/`: the resolver (`mod.rs`), the SSRF guard
(`guard.rs`, `addr.rs`, `domain.rs`), the homepage scanner (`html.rs`) and the content checks
(`content.rs`). The binary wires it up in `crates/doewood/src/icons.rs`; the route is
`crates/doewood-api/src/icons.rs`.

The enrichment job resolves the icon of each company's verified website ahead of time, through
the same resolver, guard and cache (see [enrichment.md](enrichment.md#website-and-favicon)), so
the route usually answers from the cache.

## Finding an icon

1. **The domain is validated** before anything is fetched: a bare host name, lowercased,
   international names in punycode (IDNA), at most 253 characters, labels of letters, digits
   and inner hyphens, at least two labels. A scheme, port, path, query, user info, white space,
   a trailing dot, or an IP address in any notation (`127.1`, `2130706433`, `0x7f.0.0.1`,
   `[::1]`) is refused with `400`.
2. **The site:** `https://{domain}/favicon.ico`; then the homepage `https://{domain}/` (only the
   first 200 KB are read), and at most **2** of its `<link rel~=icon>` hrefs, in document order.
   `rel` must contain the token `icon` (`icon`, `shortcut icon`; not `apple-touch-icon`).
   `data:` hrefs and links typed as SVG are skipped, comments are ignored, and relative hrefs
   are resolved against the page's final URL (after redirects).
3. **The fallback** (off by default, see [Settings](#settings)): a public favicon service,
   `https://icons.duckduckgo.com/ip3/{domain}.ico`. It answers a placeholder (a "globe") for
   sites it does not know. To recognize it, doewood asks the same service for
   `doewood-globe-check.invalid` (RFC 2606: it never exists) and keeps those bytes for a day.
   An answer with identical bytes is a miss. If the placeholder cannot be learned, the
   fallback is not used. The fallback goes through the same guard and checks as the site.
4. **Time budgets:** 6 s for the site, 5 s for the fallback, 12 s in all. Each request has a
   3 s connect and a 5 s total timeout.
5. **Cache:** a hit is kept 30 days, a miss 6 hours, in the backend's `favicons` table: the
   icon's bytes and format (or the miss), the lookup date and the expiry. With a shared
   Postgres every instance uses the same table, so an icon is fetched once; standalone it is
   the instance's SQLite file. While the shared store is not connected yet, icons are cached
   in memory (up to 10,000 domains; the oldest go first). Expired rows are never served and are
   purged every hour. Anything that finds no usable icon is a miss, including a site that does
   not answer or that the guard refuses. At most 16 lookups fetch at the same time; more
   answer `503`.

## The SSRF guard

The domain comes from a request, so the fetcher is built so that it can only reach public web
sites.

| rule | how | tests |
|---|---|---|
| `http` and `https` only | every URL and every redirect hop is checked before it is requested | `guard::production_url_checks`, `favicon::a_redirect_to_a_private_address_is_refused` (`file:`) |
| default ports only (80, 443) | the production policy (`PublicOnly`) allows no other port | `addr::only_default_ports`, `guard::production_url_checks`, `favicon::the_production_policy_refuses_the_loopback_mock` |
| a domain name as the host | IP literals, user info, single labels and trailing dots are refused, in the path and in every hop | `domain::invalid_domains_are_refused`, `guard::production_url_checks`, `favicon::invalid_domains_fetch_nothing` |
| no special-use names | `localhost`, `.local`, `.internal`, `.home.arpa`, `.arpa`, `.test`, `.example`, `.invalid`, `.onion` and similar are refused before any lookup | `addr::special_names_are_refused` |
| DNS is resolved by doewood, and **every** address must be public | the lookup runs in reqwest's resolver hook; if any address is not public, the whole host is refused | `addr::every_special_range_is_refused`, `addr::embedded_ipv4_forms_are_refused`, `favicon::one_private_address_refuses_the_whole_host` |
| no DNS rebinding | the hook hands reqwest exactly the checked addresses, so the connection goes to a checked address with no second lookup in between; `Host` and TLS SNI keep the original name; connections are not pooled, so every connection is checked | `favicon::dns_rebinding_cannot_swap_the_address`, `favicon::favicon_ico_is_found_first` (the `Host` header) |
| redirects by hand, at most 5 | reqwest follows none; every hop is checked again (scheme, user info, host, port, resolution, addresses) | `favicon::a_chain_of_five_redirects_is_followed`, `favicon::a_sixth_redirect_is_refused`, `favicon::a_redirect_to_a_private_address_is_refused` |
| no `https` to `http` downgrade | a hop from `https` to `http` is refused | `guard::https_to_http_is_a_downgrade` |
| body caps | an icon over 200 KB is refused (by `Content-Length` or while reading); a homepage is cut at 200 KB | `favicon::oversize_icons_are_refused`, `favicon::only_the_first_200_kb_of_the_homepage_are_read` |
| timeouts | connect 3 s, each request 5 s, and the budgets above | `favicon::the_site_budget_bounds_a_slow_site` |
| at most 2 links, no `data:` | see above | `html::*`, `favicon::at_most_two_links_are_followed`, `favicon::data_uris_are_skipped` |
| optional egress proxy | see below | `guard::proxy_urls_are_checked`, `icons::a_new_proxy_builds_a_new_client` |

"Public" means: for IPv6, only global unicast (`2000::/3`) outside its special blocks
(`2001::/23` with Teredo, `2001:db8::/32`, 6to4 `2002::/16`, `3fff::/20`); so `::`, `::1`,
IPv4-compatible, NAT64 (`64:ff9b::/96`, `64:ff9b:1::/48`), unique-local `fc00::/7` (with
`fd00:ec2::254`), link-local, site-local and multicast are all refused. An IPv4-mapped address
(`::ffff:a.b.c.d`) is judged as its IPv4 address. For IPv4, every block of the IANA
special-purpose registry is refused: `0/8`, `10/8`, `100.64/10`, `127/8`, `169.254/16` (with
`169.254.169.254`), `172.16/12`, `192.0.0/24`, `192.0.2/24`, `192.31.196/24`, `192.52.193/24`,
`192.88.99/24`, `192.168/16`, `192.175.48/24`, `198.18/15`, `198.51.100/24`, `203.0.113/24`,
multicast `224/4` and `240/4` (with `255.255.255.255`).

**With an egress proxy,** every request goes through it, and doewood resolves only the proxy's
own name. The URL checks still apply (scheme, port, host name, redirects, downgrade), but the
target's addresses are resolved, and so must be checked, by the proxy: use one that refuses
private and link-local networks. The proxy URL is `http://` or `https://` without credentials.
If the guard refuses the configured proxy, no icon is fetched (it does not fall back to going
out directly).

## Content checks

- The format comes from the **magic bytes** only: ICO, PNG, GIF, JPEG, WEBP or AVIF. The
  upstream `Content-Type` is never echoed: doewood serves the type of the detected format.
- **Never SVG:** an answer declared `image/svg+xml`, and anything whose bytes start like markup
  (`<` after an optional byte-order mark and white space, or a UTF-16/32 byte-order mark), is
  refused whatever it is declared as. So is HTML served as `image/png`.
- At most **200 KB**.
- **Blank icons are refused:** under 8 px wide or high, fully transparent, one flat colour, or
  near-white (every visible pixel at least 240 on red, green and blue). ICO, PNG, GIF, JPEG and
  WEBP are decoded for this (ICO's largest image, GIF's first frame; at most 2048 px a side and
  64 MiB, and a decoder panic is a refusal). The build has no AVIF decoder: an AVIF's size is
  read from its `ispe` property (under 8 px is refused), and the pixel checks are skipped.

## Settings

Runtime settings (ADR 0002 section 7), kept in the instance store and read for every lookup,
so a change applies without a restart:

| setting | default | meaning |
|---|---|---|
| `favicon.fallback` | off | ask the public fallback service when the site has no usable icon. That service learns the domain, which is why it is off unless turned on. |
| `favicon.egress_proxy` | none | an `http://` or `https://` proxy URL, without credentials, that every favicon request goes through |

Set them with `ConfigService.SetEnrichmentSettings` (`favicon_fallback`,
`favicon_egress_proxy`; an empty proxy removes it) and read them with `GetEnrichmentSettings`
(see [api.md](api.md#enrichment-settings)). The proxy URL is checked by the guard's own code
before it is stored. The next icon lookup uses the new values.

## Tests

`cargo test -p doewood-registry favicon` and `cargo test -p doewood-api --test icons`. No test
reaches the internet. Invented `*.test` names resolve, through a fake resolver, to a mock HTTP
server on 127.0.0.1, and a test-only policy allows exactly that server (127.0.0.1 on its
port). The production policy is never relaxed for a test: separate tests show that it refuses
the mock (by address, port and name) and every special range, IPv4-mapped, IPv4-compatible,
NAT64, 6to4 and Teredo forms included. HTTPS cannot be mocked without a certificate, so the
`https` to `http` rule is tested on the hop check itself. Icons are built in code.
