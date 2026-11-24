//! The `<link rel~=icon>` hrefs of a homepage: a small tolerant scanner, not an HTML parser.
//!
//! Comments are skipped; `rel` must contain the token `icon` (so `icon` and `shortcut icon`,
//! not `apple-touch-icon`); `data:` hrefs and links typed as SVG are skipped; hrefs are
//! resolved against the page's URL; at most `limit` are returned, in document order, without
//! duplicates.

use url::Url;

/// The icon hrefs of `html`, resolved against `base`, at most `limit`.
pub fn icon_links(html: &[u8], base: &Url, limit: usize) -> Vec<Url> {
    let text = String::from_utf8_lossy(html);
    let lower = text.to_ascii_lowercase();
    let mut out: Vec<Url> = Vec::new();
    let mut at = 0;
    while out.len() < limit {
        let Some(open) = lower[at..].find('<').map(|i| at + i) else {
            break;
        };
        if lower[open..].starts_with("<!--") {
            match lower[open + 4..].find("-->") {
                Some(end) => {
                    at = open + 4 + end + 3;
                    continue;
                }
                None => break,
            }
        }
        let Some(close) = lower[open..].find('>').map(|i| open + i) else {
            break;
        };
        at = close + 1;
        let tag = &text[open + 1..close];
        let Some(rest) = tag
            .get(..4)
            .filter(|t| t.eq_ignore_ascii_case("link"))
            .map(|_| &tag[4..])
        else {
            continue;
        };
        if !rest.starts_with(|c: char| c.is_ascii_whitespace() || c == '/') {
            continue;
        }
        let attrs = attributes(rest);
        let get = |name: &str| {
            attrs
                .iter()
                .find(|(k, _)| k.eq_ignore_ascii_case(name))
                .map(|(_, v)| v.as_str())
        };
        let is_icon = get("rel").is_some_and(|rel| {
            rel.split_ascii_whitespace()
                .any(|t| t.eq_ignore_ascii_case("icon"))
        });
        if !is_icon || get("type").is_some_and(|t| t.to_ascii_lowercase().contains("svg")) {
            continue;
        }
        let Some(href) = get("href").map(|h| decode_entities(h.trim())) else {
            continue;
        };
        if href.is_empty()
            || href
                .get(..5)
                .is_some_and(|s| s.eq_ignore_ascii_case("data:"))
        {
            continue;
        }
        if let Ok(url) = base.join(&href)
            && !out.contains(&url)
        {
            out.push(url);
        }
    }
    out
}

/// The attributes of a tag's inside: `name`, `name=value`, `name="value"`, `name='value'`.
fn attributes(s: &str) -> Vec<(String, String)> {
    let b = s.as_bytes();
    let mut out = Vec::new();
    let mut i = 0;
    while i < b.len() {
        while i < b.len() && (b[i].is_ascii_whitespace() || b[i] == b'/') {
            i += 1;
        }
        let start = i;
        while i < b.len() && !b[i].is_ascii_whitespace() && b[i] != b'=' && b[i] != b'/' {
            i += 1;
        }
        if start == i {
            break;
        }
        let name = s[start..i].to_owned();
        while i < b.len() && b[i].is_ascii_whitespace() {
            i += 1;
        }
        let mut value = String::new();
        if i < b.len() && b[i] == b'=' {
            i += 1;
            while i < b.len() && b[i].is_ascii_whitespace() {
                i += 1;
            }
            if i < b.len() && (b[i] == b'"' || b[i] == b'\'') {
                let quote = b[i];
                i += 1;
                let v = i;
                while i < b.len() && b[i] != quote {
                    i += 1;
                }
                s[v..i].clone_into(&mut value);
                i += 1;
            } else {
                let v = i;
                while i < b.len() && !b[i].is_ascii_whitespace() {
                    i += 1;
                }
                s[v..i].clone_into(&mut value);
            }
        }
        out.push((name, value));
    }
    out
}

/// The few entities that appear in URLs.
fn decode_entities(s: &str) -> String {
    s.replace("&amp;", "&")
        .replace("&#38;", "&")
        .replace("&quot;", "\"")
        .replace("&#39;", "'")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn base() -> Url {
        Url::parse("https://www.acme.com/en/index.html").unwrap()
    }

    fn links(html: &str) -> Vec<String> {
        icon_links(html.as_bytes(), &base(), 2)
            .into_iter()
            .map(String::from)
            .collect()
    }

    #[test]
    fn relative_and_absolute_hrefs() {
        assert_eq!(
            links(r#"<link rel="icon" href="/static/icon.png">"#),
            ["https://www.acme.com/static/icon.png"]
        );
        assert_eq!(
            links(r"<LINK REL=icon HREF=img/i.ico>"),
            ["https://www.acme.com/en/img/i.ico"]
        );
        assert_eq!(
            links(r"<link href='//cdn.acme.com/i.png' rel='shortcut icon' />"),
            ["https://cdn.acme.com/i.png"]
        );
        assert_eq!(
            links(r#"<link rel="icon" href="https://img.acme.net/a.png?v=1&amp;s=2">"#),
            ["https://img.acme.net/a.png?v=1&s=2"]
        );
    }

    #[test]
    fn at_most_the_limit_in_document_order() {
        let html = r#"<head>
            <link rel="stylesheet" href="/a.css">
            <link rel="icon" href="/1.png">
            <link rel="icon" href="/1.png">
            <link rel="icon" href="/2.png">
            <link rel="icon" href="/3.png">
        </head>"#;
        assert_eq!(
            links(html),
            ["https://www.acme.com/1.png", "https://www.acme.com/2.png"]
        );
    }

    #[test]
    fn data_svg_and_other_rels_are_skipped() {
        let html = r#"
            <!-- <link rel="icon" href="/commented.png"> -->
            <link rel="apple-touch-icon" href="/touch.png">
            <link rel="icon" href="data:image/png;base64,AAAA">
            <link rel="icon" href=" DATA:image/png,xx">
            <link rel="icon" type="image/svg+xml" href="/i.svg">
            <link rel="icon">
            <linked rel="icon" href="/no.png">
            <link rel="icon" href="/ok.png">
        "#;
        assert_eq!(links(html), ["https://www.acme.com/ok.png"]);
    }

    #[test]
    fn garbage_does_not_panic() {
        for html in [
            "",
            "<",
            "<link",
            "<link rel=\"icon\" href=\"/x",
            "<!--",
            "<link rel=icon href=>",
            "\u{fffd}<link rel=icon href=/é.png>",
        ] {
            let _ = links(html);
        }
        assert_eq!(
            icon_links(b"\xff\xfe<link rel=icon href=/a.png>", &base(), 2).len(),
            1
        );
    }
}
