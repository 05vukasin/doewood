//! The company's website and favicon (ADR 0002 section 5).
//!
//! 1. **Candidates:** the official website from Wikidata, then the email domains seen in the
//!    company's documents (quail's `signal.email_domain`, most frequent first; free-mail
//!    hosts are never one). No domain is ever guessed from the name, and no search is made.
//! 2. **Verified** before it is stored: the domain is not owned by another company (alias
//!    `domain:<domain>`, first claim wins), it resolves through the SSRF guard (every address
//!    public, redirects re-checked), and the homepage (or the one legal-notice page it links
//!    to on the same site) contains the company's normalized name or one of its identifiers.
//! 3. **The favicon** of a verified domain is then resolved through the same guard into the
//!    icon cache, which `GET /icons/{domain}` serves from.
//!
//! A website verified less than 30 days ago is not checked again.

// `b` (the backend), `s` (the company) and `f` (the findings) are used throughout.
#![allow(clippy::many_single_char_names)]

use std::collections::BTreeMap;

use doewood_enrich::{CompanyName, normalize};
use doewood_model::{Backend, Blake3, NodeKind, PropsPatch, Result};
use doewood_registry::favicon::{
    Domain, Fallback, GuardedClient, MAX_PAGE_BYTES, Oversize, SiteOrigin,
};
use serde_json::{Value, json};
use tracing::{debug, info};
use url::Url;

use super::company::{Findings, Job};
use super::subject::{Subject, domain_alias};
use super::write::live_id;

/// How long a verified website stays verified.
const REVERIFY_SECS: i64 = 30 * 24 * 60 * 60;
/// Most candidates checked per company, and most documents read for email domains.
const MAX_CANDIDATES: usize = 4;
const MAX_DOCUMENTS: u32 = 50;
/// The shortest name key that counts as found on a page.
const MIN_KEY_CHARS: usize = 4;
/// The shortest identifier that counts as found on a page.
const MIN_ID_CHARS: usize = 8;

const PAGE_ACCEPT: &str = "text/html,application/xhtml+xml;q=0.9,*/*;q=0.5";

/// Words of a link to a legal notice.
const LEGAL_WORDS: &[&str] = &[
    "impressum",
    "imprint",
    "legal",
    "pravn",
    "kontakt",
    "contact",
    "about",
    "o-nama",
    "o nama",
];

/// Finds, verifies and stores the website, and resolves its favicon. Returns whether a
/// website was stored.
pub(crate) async fn run(job: &Job<'_>, s: &Subject, f: &Findings) -> Result<bool> {
    let Some(web) = &job.clients.web else {
        return Ok(false);
    };
    let b = job.backend.as_ref();
    let me = s.id();
    let Some(node) = b.node(me).await? else {
        return Ok(false);
    };
    let now = super::unix_now();
    if node.props["website"]["verified_at"]
        .as_i64()
        .is_some_and(|t| now - t < REVERIFY_SECS)
    {
        return Ok(false);
    }
    let mut candidates: Vec<(String, &'static str)> =
        f.websites.iter().map(|w| (w.clone(), "wikidata")).collect();
    for d in email_domains(b, me).await? {
        if !candidates.iter().any(|(c, _)| *c == d) {
            candidates.push((d, "email_domain"));
        }
    }
    let keys = name_keys(s, f);
    let ids = identifiers(s);
    for (raw, source) in candidates.into_iter().take(MAX_CANDIDATES) {
        let Ok(domain) = Domain::parse(&raw) else {
            continue;
        };
        let domain = domain.as_str().to_owned();
        if quail_signals::GENERIC_EMAIL_DOMAINS.contains(&domain.as_str()) {
            continue;
        }
        let alias = domain_alias(&domain);
        if let Some(owner) = b
            .alias(NodeKind::Company, &alias)
            .await?
            .and_then(|a| a.node)
            && live_id(b, owner).await? != me
        {
            debug!(company = %me, %domain, "website owned by another company; not used");
            continue;
        }
        let Some((matched, page)) = verify(web, job.clients.origin, &domain, &keys, &ids).await
        else {
            debug!(company = %me, %domain, "website not verified");
            continue;
        };
        let evidence = json!({"source": source, "page": page.as_str(), "matched": matched});
        // Onto the live company only: a merge since the job started ends it (issue #62).
        let got = b
            .claim_live_alias(NodeKind::Company, &alias, me, &evidence)
            .await?;
        if got.node != Some(me) {
            continue;
        }
        let icon = match &job.clients.favicons {
            Some(r) => {
                let fallback = job.clients.favicon_fallback.then(Fallback::default_service);
                match r.resolve(&domain, fallback.as_ref()).await {
                    Ok(Some(icon)) => json!({
                        "domain": domain,
                        "format": icon.format.as_str(),
                        "path": format!("/icons/{domain}"),
                    }),
                    Ok(None) => Value::Null,
                    Err(e) => {
                        debug!(%domain, error = %e, "favicon not resolved now");
                        Value::Null
                    }
                }
            }
            None => Value::Null,
        };
        // Only the job's two keys, atomically (issue #51).
        let patch = PropsPatch::new()
            .set(
                &["website"],
                json!({
                    "domain": domain,
                    "url": format!("https://{domain}/"),
                    "source": source,
                    "matched": matched,
                    "page": page.as_str(),
                    "verified_at": super::unix_now(),
                }),
            )
            .set(&["icon"], icon);
        if b.update_live_props(me, &patch).await?.is_none() {
            return Ok(false);
        }
        info!(company = %me, %domain, source, matched, "website verified");
        return Ok(true);
    }
    Ok(false)
}

/// The email domains of the company's documents, most frequent first. The first
/// [`MAX_DOCUMENTS`] documents are read, and only their `signal.email_domain` facts (issue #75).
async fn email_domains(b: &dyn Backend, company: uuid::Uuid) -> Result<Vec<String>> {
    let hashes: Vec<Blake3> = b
        .company_documents_first(company, MAX_DOCUMENTS)
        .await?
        .iter()
        .filter_map(|doc| doc.key.parse().ok())
        .collect();
    let mut counts: BTreeMap<String, usize> = BTreeMap::new();
    for (_, d) in b.fact_texts(&hashes, "signal.email_domain").await? {
        *counts.entry(d.trim().to_ascii_lowercase()).or_default() += 1;
    }
    let mut list: Vec<(String, usize)> = counts.into_iter().collect();
    list.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
    Ok(list.into_iter().map(|(d, _)| d).collect())
}

/// The name keys a page may show: the company's own and its registered names'.
fn name_keys(s: &Subject, f: &Findings) -> Vec<String> {
    let mut keys: Vec<String> = s.name.iter().map(|n| n.key.clone()).collect();
    for n in &f.names {
        if let Some(p) = CompanyName::parse(n)
            && !keys.contains(&p.key)
        {
            keys.push(p.key);
        }
    }
    keys.retain(|k| k.chars().count() >= MIN_KEY_CHARS);
    keys
}

/// The identifiers a page may show (compact, uppercase).
fn identifiers(s: &Subject) -> Vec<String> {
    let mut ids: Vec<String> = s.ids.iter().map(|i| i.value.clone()).collect();
    if let Some(n) = &s.ch {
        ids.push(n.as_str().to_owned());
    }
    ids.retain(|i| i.len() >= MIN_ID_CHARS);
    ids
}

/// Fetches the homepage (and one legal-notice page it links to) and looks for a name key or
/// an identifier. Returns what matched and the page.
async fn verify(
    web: &GuardedClient,
    origin: SiteOrigin,
    domain: &str,
    keys: &[String],
    ids: &[String],
) -> Option<(&'static str, Url)> {
    let port = origin.port.map(|p| format!(":{p}")).unwrap_or_default();
    let home = Url::parse(&format!("{}://{domain}{port}/", origin.scheme)).ok()?;
    let page = web
        .get(home, PAGE_ACCEPT, MAX_PAGE_BYTES, Oversize::Truncate)
        .await
        .map_err(|e| debug!(domain, error = %e, "homepage not fetched"))
        .ok()?;
    if page.status != 200 || !same_site(&page.url, domain) {
        return None;
    }
    let html = String::from_utf8_lossy(&page.body);
    if let Some(m) = matches(&html, keys, ids) {
        return Some((m, page.url));
    }
    let link = legal_link(&html, &page.url)?;
    let notice = web
        .get(link, PAGE_ACCEPT, MAX_PAGE_BYTES, Oversize::Truncate)
        .await
        .ok()?;
    if notice.status != 200 || !same_site(&notice.url, domain) {
        return None;
    }
    let html = String::from_utf8_lossy(&notice.body);
    matches(&html, keys, ids).map(|m| (m, notice.url))
}

/// Whether `url` is on `domain` (or its `www.`).
fn same_site(url: &Url, domain: &str) -> bool {
    url.host_str().is_some_and(|h| {
        let h = h.to_ascii_lowercase();
        h == domain || h.strip_prefix("www.") == Some(domain)
    })
}

/// `name` when a key is on the page as whole words, `identifier` when an identifier is.
fn matches(html: &str, keys: &[String], ids: &[String]) -> Option<&'static str> {
    let text = visible_text(html);
    let norm = format!(" {} ", normalize(&text));
    if keys.iter().any(|k| norm.contains(&format!(" {k} "))) {
        return Some("name");
    }
    // Identifiers as written: runs of letters and digits, also across spaces, dots and
    // dashes (`101 234 567`, `RS-101234567`), at most four pieces long.
    let pieces: Vec<String> = text
        .split(|c: char| !(c.is_ascii_alphanumeric() || matches!(c, ' ' | '.' | '-' | '/')))
        .flat_map(|run| {
            let parts: Vec<String> = run
                .split([' ', '.', '-', '/'])
                .filter(|p| !p.is_empty())
                .map(str::to_ascii_uppercase)
                .collect();
            let mut out = Vec::new();
            for i in 0..parts.len() {
                let mut joined = String::new();
                for p in parts.iter().skip(i).take(4) {
                    joined.push_str(p);
                    out.push(joined.clone());
                }
            }
            out
        })
        .collect();
    ids.iter()
        .any(|id| {
            pieces
                .iter()
                .any(|p| p == id || p.strip_prefix("RS") == Some(id))
        })
        .then_some("identifier")
}

/// The page's text: tags, scripts and styles out, a few entities decoded.
fn visible_text(html: &str) -> String {
    let mut out = String::with_capacity(html.len() / 2);
    let lower = html.to_ascii_lowercase();
    let mut i = 0;
    let bytes = html.as_bytes();
    while i < bytes.len() {
        if bytes[i] == b'<' {
            for block in ["script", "style"] {
                if lower[i + 1..].starts_with(block) {
                    let end = format!("</{block}");
                    if let Some(j) = lower[i..].find(&end) {
                        i += j;
                    }
                }
            }
            match html[i..].find('>') {
                Some(j) => i += j + 1,
                None => break,
            }
            out.push(' ');
            continue;
        }
        let next = html[i..].find('<').map_or(html.len(), |j| i + j);
        out.push_str(&html[i..next]);
        i = next;
    }
    out.replace("&amp;", "&")
        .replace("&nbsp;", " ")
        .replace("&#160;", " ")
        .replace("&quot;", "\"")
}

/// The first link on the page to a legal notice on the same site.
fn legal_link(html: &str, base: &Url) -> Option<Url> {
    let lower = html.to_ascii_lowercase();
    let mut from = 0;
    while let Some(i) = lower[from..].find("<a ") {
        let start = from + i;
        let end = lower[start..]
            .find("</a")
            .map_or(lower.len(), |j| start + j);
        from = end.max(start + 3);
        let tag = &html[start..end];
        let tag_lower = &lower[start..end];
        let Some(h) = tag_lower.find("href=") else {
            continue;
        };
        let rest = &tag[h + 5..];
        let quote = rest.chars().next()?;
        let href: String = if quote == '"' || quote == '\'' {
            rest[1..].chars().take_while(|c| *c != quote).collect()
        } else {
            rest.chars()
                .take_while(|c| !c.is_whitespace() && *c != '>')
                .collect()
        };
        if !LEGAL_WORDS.iter().any(|w| tag_lower.contains(w)) {
            continue;
        }
        if let Ok(url) = base.join(&href)
            && url.host_str() == base.host_str()
            && url.port() == base.port()
            && matches!(url.scheme(), "http" | "https")
        {
            return Some(url);
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names_and_identifiers_on_a_page() {
        let html = "<html><head><title>Primer Gradnja</title><script>var x='ACME DOO'</script>\
            </head><body><p>Primer Gradnja d.o.o. &amp; partneri</p><p>PIB: 101 234 567</p>\
            </body></html>";
        assert_eq!(matches(html, &["primer gradnja".into()], &[]), Some("name"));
        assert_eq!(
            matches(html, &["acme".into()], &[]),
            None,
            "scripts are not text"
        );
        assert_eq!(
            matches(html, &["nothing here".into()], &["101234567".into()]),
            Some("identifier")
        );
        assert_eq!(
            matches(html, &["gradnj".into()], &["999999999".into()]),
            None
        );
    }

    #[test]
    fn legal_links_stay_on_the_site() {
        let base = Url::parse("https://acme.test/").unwrap();
        let html = "<a href=\"https://elsewhere.test/impressum\">Impressum</a>\
            <a href='/blog'>Blog</a><a href=\"/o-nama\">O nama</a>";
        assert_eq!(
            legal_link(html, &base).unwrap().as_str(),
            "https://acme.test/o-nama"
        );
        assert_eq!(legal_link("<a href=\"/blog\">Blog</a>", &base), None);
    }
}
