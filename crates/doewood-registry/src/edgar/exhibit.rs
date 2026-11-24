//! Reading filing documents, best-effort: the filing index (which document is Exhibit 21),
//! the subsidiary rows of an Exhibit 21, and the text around "Item 2.01" in an 8-K. Exhibit
//! 21 formats vary wildly (tables, paragraphs, indented lists, plain text); every rule here is
//! a heuristic, and odd input gives fewer rows, never a panic.

use super::html::{self, find_ci, find_ci_from};
use super::model::{safe_file, words};

/// A subsidiary row of an Exhibit 21.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Subsidiary {
    /// The subsidiary's name, as written.
    pub name: String,
    /// The jurisdiction of incorporation or organization, as written (`Delaware`,
    /// `United Kingdom`), when the row has one.
    pub jurisdiction: Option<String>,
}

/// How the rows of an Exhibit 21 were read. Either way the result is **best-effort**: a
/// heuristic reading of a free-form document, to be corroborated before it becomes an edge.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Extraction {
    /// From the rows of HTML tables (name in the first cell, jurisdiction in the last).
    Table,
    /// From lines of text (`Name (Delaware)`, `Name, a Delaware corporation`, `Name - Delaware`,
    /// tab, dot-leader or wide-space separated, or a name alone).
    Lines,
}

/// The subsidiary rows of an Exhibit 21 document, at most `max` of them, and whether there
/// were more. Table rows win when there are any.
pub(crate) fn subsidiaries(body: &[u8], max: usize) -> (Vec<Subsidiary>, Extraction, bool) {
    let src = String::from_utf8_lossy(body);
    let doc = html::read(&src);
    let mut out = Vec::new();
    let mut more = false;
    let mut push = |s: Subsidiary, out: &mut Vec<Subsidiary>| {
        if out.len() < max {
            out.push(s);
        } else {
            more = true;
        }
    };
    for row in &doc.rows {
        if let Some(s) = table_row(row) {
            push(s, &mut out);
        }
    }
    if !out.is_empty() {
        return (out, Extraction::Table, more);
    }
    for line in doc.text.lines() {
        if let Some(s) = line_row(line) {
            push(s, &mut out);
        }
    }
    (out, Extraction::Lines, more)
}

fn table_row(row: &[String]) -> Option<Subsidiary> {
    let mut cells: Vec<&str> = row
        .iter()
        .map(|c| c.trim())
        .filter(|c| !c.is_empty())
        .collect();
    // A leading numbering column (`1`, `1.`, `(a)`, `*`).
    while cells.len() > 1 && is_marker(cells[0]) {
        cells.remove(0);
    }
    if cells.is_empty() || cells.iter().all(|c| is_header(c)) {
        return None;
    }
    if cells.len() == 1 {
        return line_row(cells[0]);
    }
    let name = clean_name(cells[0])?;
    let jurisdiction = cells[1..]
        .iter()
        .rev()
        .find(|c| !is_share(c) && !is_marker(c))
        .and_then(|c| clean_jurisdiction(c));
    Some(Subsidiary { name, jurisdiction })
}

fn line_row(line: &str) -> Option<Subsidiary> {
    let t = strip_marker(line.trim());
    if t.is_empty() || is_heading(t) || is_header(t) {
        return None;
    }
    // Tab-separated (a table read as text, or a .txt exhibit).
    let parts: Vec<&str> = t
        .split('\t')
        .map(str::trim)
        .filter(|p| !p.is_empty())
        .collect();
    if parts.len() > 1 {
        return table_row(&parts.iter().map(|p| (*p).to_owned()).collect::<Vec<_>>());
    }
    // Dot leaders: `Name ........ Delaware`.
    if let Some(i) = t.find("...") {
        let (name, rest) = (&t[..i], t[i..].trim_start_matches(['.', ' ']));
        return split(name, Some(rest));
    }
    // Wide spaces (plain-text exhibits): `Name      Delaware`.
    if let Some(i) = t.find("   ") {
        let rest = t[i..].trim();
        let last = rest.rsplit("   ").next().unwrap_or(rest).trim();
        return split(&t[..i], Some(last));
    }
    // `Name (Delaware)`.
    if let Some(inner) = t.strip_suffix(')')
        && let Some(i) = inner.rfind('(')
    {
        let j = &inner[i + 1..];
        if !is_share(j) && !j.is_empty() {
            return split(&inner[..i], Some(j));
        }
        return split(&inner[..i], None);
    }
    // `Name, a Delaware corporation` / `Name, an Ohio limited liability company`.
    for sep in [", a ", ", an ", ", A ", ", An "] {
        if let Some(i) = t.find(sep) {
            let words: Vec<&str> = t[i + sep.len()..]
                .split_whitespace()
                .take_while(|w| !is_entity_word(w))
                .collect();
            let jur = words.join(" ");
            return split(&t[..i], (!jur.is_empty()).then_some(jur.as_str()));
        }
    }
    // `Name - Delaware` / `Name – Delaware` / `Name — Delaware`.
    for sep in [" \u{2014} ", " \u{2013} ", " - "] {
        if let Some(i) = t.rfind(sep) {
            let jur = t[i + sep.len()..].trim();
            if jur.split_whitespace().count() <= 5 {
                return split(&t[..i], Some(jur));
            }
        }
    }
    split(t, None)
}

fn split(name: &str, jurisdiction: Option<&str>) -> Option<Subsidiary> {
    Some(Subsidiary {
        name: clean_name(name)?,
        jurisdiction: jurisdiction.and_then(clean_jurisdiction),
    })
}

fn clean_name(s: &str) -> Option<String> {
    let t = s.trim().trim_end_matches([',', ';', ':', '*']).trim();
    let n = t.chars().count();
    let ok = (2..=300).contains(&n)
        && t.chars().any(char::is_alphabetic)
        && !is_header(t)
        && !is_heading(t)
        && !is_share(t);
    ok.then(|| t.to_owned())
}

fn clean_jurisdiction(s: &str) -> Option<String> {
    let t = s
        .trim()
        .trim_matches(|c: char| matches!(c, ',' | ';' | ':' | '.' | '*' | '(' | ')'));
    let t = t.trim();
    let n = t.chars().count();
    let ok = (2..=100).contains(&n) && t.chars().any(char::is_alphabetic) && !is_header(t);
    ok.then(|| t.to_owned())
}

/// `100%`, `51.5 %`, `100`: an ownership share, not a jurisdiction.
fn is_share(s: &str) -> bool {
    let t = s.trim().trim_end_matches('%').trim();
    !t.is_empty() && t.chars().all(|c| c.is_ascii_digit() || c == '.')
}

/// A numbering or footnote marker: `1`, `1.`, `(a)`, `a)`, `*`, `•`.
fn is_marker(s: &str) -> bool {
    let t = s.trim();
    let core = t.trim_matches(['(', ')', '.', ' ']);
    t.chars().count() <= 5
        && ((core.is_empty() && !t.is_empty())
            || core.chars().all(|c| c.is_ascii_digit())
            || (core.chars().count() == 1 && core.chars().all(|c| c.is_ascii_lowercase()))
            || core
                .chars()
                .all(|c| matches!(c, '*' | '\u{2022}' | '\u{b7}' | '-' | '#')))
}

fn strip_marker(s: &str) -> &str {
    let mut t = s.trim_start_matches(['\u{2022}', '\u{b7}', '*', '-', '\u{2013}', ' ']);
    if let Some((head, rest)) = t.split_once(' ')
        && is_marker(head)
        && (head.ends_with('.') || head.ends_with(')'))
    {
        t = rest.trim_start();
    }
    t
}

/// A table header cell or line: a few words naming a column (`Name of Subsidiary`,
/// `Jurisdiction of Incorporation`, `% Owned`), with no company-form word in it, so a
/// subsidiary named `Fictive Organization Inc.` is not one.
fn is_header(s: &str) -> bool {
    let l = s.to_lowercase();
    let words = words(&l);
    if words.is_empty() || words.len() > 12 || words.iter().any(|w| is_company_form(w)) {
        return false;
    }
    let l = l.trim();
    [
        "subsidiar",
        "jurisdiction",
        "incorporat",
        "organiz",
        "ownership",
        "percent",
        "state or",
        "country of",
        "place of",
        "domicile",
        "% owned",
    ]
    .iter()
    .any(|w| l.contains(w))
        || matches!(
            l,
            "name" | "names" | "entity" | "entity name" | "company" | "state" | "country"
        )
}

/// A word that names a company form (`Inc`, `LLC`, `GmbH`, ...).
fn is_company_form(w: &str) -> bool {
    matches!(
        w,
        "inc"
            | "llc"
            | "ltd"
            | "corp"
            | "corporation"
            | "gmbh"
            | "sa"
            | "bv"
            | "plc"
            | "lp"
            | "llp"
            | "ag"
            | "nv"
            | "kk"
            | "srl"
            | "spa"
            | "limited"
            | "pty"
            | "holdings"
            | "co"
            | "sarl"
            | "ab"
            | "as"
            | "oy"
            | "doo"
            | "trust"
            | "partners"
            | "group"
    )
}

/// A title or heading line of the exhibit.
fn is_heading(s: &str) -> bool {
    let l = s.to_lowercase();
    let l = l.trim();
    l.starts_with("exhibit")
        || l.starts_with("ex-21")
        || l.contains("subsidiaries of")
        || l.starts_with("list of")
        || l.starts_with("page ")
        || l.ends_with(':')
        || l.starts_with("the following")
        || l.starts_with("omitted")
        || (l.split_whitespace().count() > 25)
}

fn is_entity_word(w: &str) -> bool {
    let l = w.to_lowercase();
    let l = l.trim_matches(|c: char| !c.is_alphanumeric());
    matches!(
        l,
        "corporation"
            | "company"
            | "limited"
            | "partnership"
            | "llc"
            | "l"
            | "lp"
            | "trust"
            | "corp"
            | "inc"
            | "gmbh"
            | "ltd"
            | "entity"
            | "bank"
            | "association"
            | "business"
            | "statutory"
            | "general"
            | "private"
            | "public"
            | "société"
            | "societe"
    )
}

/// The documents of a filing index (`<accession>-index.htm`): `(type, file name)`, for each
/// row of a table whose header names a `Type` and a `Document` column (else the EDGAR column
/// order: Seq, Description, Document, Type, Size). Unsafe file names are skipped.
pub(crate) fn index_documents(body: &[u8]) -> Vec<(String, String)> {
    let src = String::from_utf8_lossy(body);
    let doc = html::parse(&src);
    let mut doc_col = 2;
    let mut type_col = 3;
    let mut out = Vec::new();
    for row in &doc.rows {
        let lower: Vec<String> = row.iter().map(|c| c.trim().to_lowercase()).collect();
        if let (Some(d), Some(t)) = (
            lower.iter().position(|c| c == "document"),
            lower.iter().position(|c| c == "type"),
        ) {
            doc_col = d;
            type_col = t;
            continue;
        }
        let (Some(file), Some(kind)) = (row.get(doc_col), row.get(type_col)) else {
            continue;
        };
        // `fict-10k.htm iXBRL`: the file name is the first word.
        let Some(file) = file.split_whitespace().next().and_then(safe_file) else {
            continue;
        };
        let kind = kind.trim().to_ascii_uppercase();
        if !kind.is_empty() && kind.len() <= 32 {
            out.push((kind, file));
        }
    }
    out
}

/// Whether a document type is Exhibit 21 (`EX-21`, `EX-21.1`, `EX-21.01`).
pub(crate) fn is_ex21(kind: &str) -> bool {
    kind == "EX-21" || kind.strip_prefix("EX-21.").is_some_and(|r| !r.is_empty())
}

/// The text from "Item 2.01" on, at most `max_chars` characters, cut before the next
/// "Item N.NN" heading. The first "Item 2.01" followed closely by "Completion" is preferred;
/// else the first one. `None` when there is no "Item 2.01".
pub(crate) fn item_201(body: &[u8], max_chars: usize) -> Option<String> {
    let src = String::from_utf8_lossy(body);
    let doc = html::read(&src);
    let text = doc.text.split_whitespace().collect::<Vec<_>>().join(" ");
    let mut first = None;
    let mut best = None;
    let mut from = 0;
    while let Some(i) = find_ci_from(&text, "item 2.01", from) {
        first.get_or_insert(i);
        let near = text.get(i..).map_or("", |s| {
            let end = s.char_indices().nth(60).map_or(s.len(), |(j, _)| j);
            &s[..end]
        });
        if find_ci(near, "completion").is_some() {
            best = Some(i);
            break;
        }
        from = i + 1;
    }
    let start = best.or(first)?;
    let window = &text[start..];
    let mut end = window.len();
    let mut from = "item 2.01".len();
    while let Some(j) = find_ci_from(window, "item ", from) {
        let after = &window.as_bytes()[j + 5..];
        if after.len() >= 4
            && after[0].is_ascii_digit()
            && after[1] == b'.'
            && after[2].is_ascii_digit()
        {
            end = j;
            break;
        }
        from = j + 1;
    }
    let cut: String = window[..end].trim().chars().take(max_chars).collect();
    Some(cut)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(name: &str, jur: Option<&str>) -> Subsidiary {
        Subsidiary {
            name: name.to_owned(),
            jurisdiction: jur.map(str::to_owned),
        }
    }

    #[test]
    fn line_forms() {
        let cases = [
            (
                "Alpha Fictive LLC (Delaware)",
                row("Alpha Fictive LLC", Some("Delaware")),
            ),
            (
                "Beta Fictive Ltd., a United Kingdom private limited company",
                row("Beta Fictive Ltd.", Some("United Kingdom")),
            ),
            (
                "Gamma Fictive Inc. - Ontario",
                row("Gamma Fictive Inc.", Some("Ontario")),
            ),
            (
                "Delta Fictive GmbH ........ Germany",
                row("Delta Fictive GmbH", Some("Germany")),
            ),
            (
                "Epsilon Fictive SA      Luxembourg",
                row("Epsilon Fictive SA", Some("Luxembourg")),
            ),
            (
                "1. Zeta Fictive BV\tNetherlands\t100%",
                row("Zeta Fictive BV", Some("Netherlands")),
            ),
            (
                "\u{2022} Eta Fictive Pty Ltd",
                row("Eta Fictive Pty Ltd", None),
            ),
            ("Theta Fictive KK (100%)", row("Theta Fictive KK", None)),
        ];
        for (line, want) in cases {
            assert_eq!(line_row(line), Some(want), "{line}");
        }
        for skip in [
            "Exhibit 21.1",
            "SUBSIDIARIES OF FICTIVE CORP",
            "Name",
            "12",
            "Page 1",
            "",
            "The following are subsidiaries:",
        ] {
            assert_eq!(line_row(skip), None, "{skip}");
        }
    }

    #[test]
    fn items() {
        assert!(is_ex21("EX-21") && is_ex21("EX-21.1") && is_ex21("EX-21.01"));
        assert!(!is_ex21("EX-21.") && !is_ex21("EX-211") && !is_ex21("EX-2.1"));
    }
}
