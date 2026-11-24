//! A small, bounded HTML reader for EDGAR documents: the text (tags stripped, entities
//! decoded, block tags as line breaks, cells separated by tabs) and the table rows. It never
//! panics on any input: it only slices at ASCII `<`, `>` and needle positions, and it caps
//! the rows, the cells and the cell text it keeps.

/// The most table rows kept.
const MAX_ROWS: usize = 20_000;
/// The most cells kept in a row.
const MAX_CELLS: usize = 64;
/// The most characters kept in a cell.
const MAX_CELL_CHARS: usize = 1000;

/// A read document.
#[derive(Debug, Default)]
pub(crate) struct Doc {
    /// The text: one line per block, table cells separated by `\t`.
    pub text: String,
    /// Every table row, as its cells' text (whitespace collapsed, trimmed).
    pub rows: Vec<Vec<String>>,
}

/// Whether `s` looks like HTML (or SGML-wrapped HTML) rather than plain text.
pub(crate) fn looks_html(s: &str) -> bool {
    [
        "<html", "<body", "<table", "<p>", "<p ", "<div", "<br", "<font", "<td",
    ]
    .iter()
    .any(|t| find_ci(s, t).is_some())
}

/// `src` as text: HTML is parsed, plain text is kept as it is (with any stray tags).
pub(crate) fn read(src: &str) -> Doc {
    if looks_html(src) {
        parse(src)
    } else {
        Doc {
            text: src.replace("\r\n", "\n"),
            rows: Vec::new(),
        }
    }
}

/// Parses `src` as HTML.
pub(crate) fn parse(src: &str) -> Doc {
    let mut b = Builder::default();
    let mut rest = src;
    while !rest.is_empty() {
        let Some(i) = rest.find('<') else {
            b.text(rest);
            break;
        };
        b.text(&rest[..i]);
        let after = &rest[i + 1..];
        if let Some(c) = after.strip_prefix("!--") {
            rest = c.find("-->").map_or("", |j| &c[j + 3..]);
            continue;
        }
        let starts_tag = after
            .chars()
            .next()
            .is_some_and(|c| c.is_ascii_alphabetic() || matches!(c, '/' | '!' | '?'));
        if !starts_tag {
            b.text("<");
            rest = after;
            continue;
        }
        let Some(j) = tag_end(after) else {
            // An unterminated tag: the rest is markup, not text.
            break;
        };
        let (closing, name) = tag_name(&after[..j]);
        rest = &after[j + 1..];
        if !closing && matches!(name.as_str(), "script" | "style") {
            rest = skip_element(rest, &name);
            continue;
        }
        b.tag(&name, closing);
    }
    b.finish()
}

/// The byte index of the `>` that ends the tag starting `s` (quotes respected).
fn tag_end(s: &str) -> Option<usize> {
    let mut quote = None;
    for (i, c) in s.bytes().enumerate() {
        match (quote, c) {
            (None, b'>') => return Some(i),
            (None, b'"' | b'\'') => quote = Some(c),
            (Some(q), c) if c == q => quote = None,
            _ => {}
        }
    }
    None
}

/// Whether the tag is a closing one, and its lowercase name.
fn tag_name(tag: &str) -> (bool, String) {
    let (closing, t) = match tag.strip_prefix('/') {
        Some(t) => (true, t),
        None => (false, tag),
    };
    let name = t
        .chars()
        .take_while(char::is_ascii_alphanumeric)
        .take(16)
        .map(|c| c.to_ascii_lowercase())
        .collect();
    (closing, name)
}

/// What follows the end tag of `name` (all of `rest` is skipped when there is none).
fn skip_element<'a>(rest: &'a str, name: &str) -> &'a str {
    let Some(i) = find_ci(rest, &format!("</{name}")) else {
        return "";
    };
    let after = &rest[i..];
    after.find('>').map_or("", |j| &after[j + 1..])
}

/// The byte index of the first ASCII-case-insensitive match of the ASCII `needle`.
pub(crate) fn find_ci(hay: &str, needle: &str) -> Option<usize> {
    find_ci_from(hay, needle, 0)
}

/// As [`find_ci`], from byte `from` on (a match is always at a character boundary when the
/// needle starts with an ASCII character).
pub(crate) fn find_ci_from(hay: &str, needle: &str, from: usize) -> Option<usize> {
    let h = hay.as_bytes();
    let n = needle.as_bytes();
    if n.is_empty() || n.len() > h.len() {
        return None;
    }
    (from..=h.len() - n.len()).find(|&i| h[i..i + n.len()].eq_ignore_ascii_case(n))
}

#[derive(Debug, Default)]
struct Builder {
    text: String,
    rows: Vec<Vec<String>>,
    row: Option<Vec<String>>,
    cell: Option<String>,
    pre: bool,
}

impl Builder {
    fn text(&mut self, raw: &str) {
        let decoded = decode(raw);
        for c in decoded.chars() {
            let ws = c.is_whitespace() || c == '\u{a0}';
            if self.pre && c == '\n' {
                self.newline();
            } else if ws {
                if !self.text.ends_with([' ', '\n', '\t']) && !self.text.is_empty() {
                    self.text.push(' ');
                }
            } else {
                self.text.push(c);
            }
            if let Some(cell) = &mut self.cell
                && cell.len() < MAX_CELL_CHARS * 4
            {
                if ws {
                    if !cell.ends_with(' ') && !cell.is_empty() {
                        cell.push(' ');
                    }
                } else {
                    cell.push(c);
                }
            }
        }
    }

    fn newline(&mut self) {
        while self.text.ends_with(' ') || self.text.ends_with('\t') {
            self.text.pop();
        }
        if !self.text.is_empty() && !self.text.ends_with('\n') {
            self.text.push('\n');
        }
    }

    fn tag(&mut self, name: &str, closing: bool) {
        match name {
            "td" | "th" => {
                self.end_cell();
                if !closing {
                    if self.row.is_none() {
                        self.row = Some(Vec::new());
                    }
                    if !self.text.ends_with(['\n', '\t']) && !self.text.is_empty() {
                        while self.text.ends_with(' ') {
                            self.text.pop();
                        }
                        self.text.push('\t');
                    }
                    self.cell = Some(String::new());
                }
            }
            "tr" | "table" | "thead" | "tbody" => {
                self.end_row();
                self.newline();
            }
            "pre" => {
                self.pre = !closing;
                self.newline();
            }
            "br" | "p" | "div" | "li" | "ul" | "ol" | "h1" | "h2" | "h3" | "h4" | "h5" | "h6"
            | "center" | "page" | "hr" => {
                if let Some(cell) = &mut self.cell
                    && !cell.ends_with(' ')
                    && !cell.is_empty()
                {
                    cell.push(' ');
                }
                if self.cell.is_none() {
                    self.newline();
                } else if !self.text.ends_with([' ', '\n', '\t']) {
                    self.text.push(' ');
                }
            }
            _ => {}
        }
    }

    fn end_cell(&mut self) {
        if let Some(cell) = self.cell.take()
            && let Some(row) = &mut self.row
            && row.len() < MAX_CELLS
        {
            row.push(cell.trim().chars().take(MAX_CELL_CHARS).collect());
        }
    }

    fn end_row(&mut self) {
        self.end_cell();
        if let Some(row) = self.row.take()
            && row.iter().any(|c| !c.is_empty())
            && self.rows.len() < MAX_ROWS
        {
            self.rows.push(row);
        }
    }

    fn finish(mut self) -> Doc {
        self.end_row();
        let text = self
            .text
            .lines()
            .map(str::trim)
            .filter(|l| !l.is_empty())
            .collect::<Vec<_>>()
            .join("\n");
        Doc {
            text,
            rows: self.rows,
        }
    }
}

/// `s` with its character references decoded (the common named ones, and numeric ones);
/// anything else is kept as written.
fn decode(s: &str) -> String {
    if !s.contains('&') {
        return s.to_owned();
    }
    let mut out = String::with_capacity(s.len());
    let mut rest = s;
    while let Some(i) = rest.find('&') {
        out.push_str(&rest[..i]);
        let after = &rest[i + 1..];
        let end = after
            .char_indices()
            .take(12)
            .find(|&(_, c)| c == ';')
            .map(|(j, _)| j);
        let decoded = end.and_then(|j| entity(&after[..j]).map(|c| (c, j)));
        if let Some((c, j)) = decoded {
            out.push(c);
            rest = &after[j + 1..];
        } else {
            out.push('&');
            rest = after;
        }
    }
    out.push_str(rest);
    out
}

fn entity(name: &str) -> Option<char> {
    if let Some(num) = name.strip_prefix('#') {
        let n = match num.strip_prefix(['x', 'X']) {
            Some(hex) => u32::from_str_radix(hex, 16).ok()?,
            None => num.parse().ok()?,
        };
        return char::from_u32(n).filter(|c| *c != '\0');
    }
    Some(match name {
        "amp" => '&',
        "lt" => '<',
        "gt" => '>',
        "quot" => '"',
        "apos" => '\'',
        "nbsp" => '\u{a0}',
        "ndash" => '\u{2013}',
        "mdash" => '\u{2014}',
        "lsquo" => '\u{2018}',
        "rsquo" => '\u{2019}',
        "ldquo" => '\u{201c}',
        "rdquo" => '\u{201d}',
        "bull" => '\u{2022}',
        "middot" => '\u{b7}',
        "eacute" => '\u{e9}',
        "uuml" => '\u{fc}',
        "ouml" => '\u{f6}',
        "auml" => '\u{e4}',
        _ => return None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn text_and_rows() {
        let d = parse(
            "<html><head><style>p{x:1}</style><script>if(a<b){}</script></head><body>\
             <p>Fictive&nbsp;Corp &amp; Co &#8212; &#x41;&bogus; &amp</p><!-- <td>no</td> -->\
             <table><tr><th>Name</th><th>Where</th></tr>\
             <tr><td>Alpha <b>One</b> LLC</td><td>Delaware</td></tr>\
             <tr><td>Beta<br>Two</td><td></td></tr></table>a < b</body></html>",
        );
        assert_eq!(
            d.rows,
            [
                vec!["Name", "Where"],
                vec!["Alpha One LLC", "Delaware"],
                vec!["Beta Two", ""]
            ]
        );
        assert!(
            d.text
                .starts_with("Fictive Corp & Co \u{2014} A&bogus; &amp\nName\tWhere\n"),
            "{}",
            d.text
        );
        assert!(d.text.ends_with("a < b"), "{}", d.text);
        assert!(!d.text.contains("no"));
    }

    #[test]
    fn garbage_never_panics() {
        let inputs = [
            "",
            "<",
            ">",
            "<<<>>>",
            "<a",
            "<a href='>",
            "&",
            "&#",
            "&#x;",
            "&#99999999999;",
            "<td>",
            "</tr></td>",
            "<!--",
            "<script>",
            "<SCRIPT>x</SCRIPT",
            "<\u{e9}>é<é",
            "&\u{1f600};",
            "<td>\u{1f600}</td>",
            "<pre>\n\n</pre>",
            "\0\0<\0>",
        ];
        for s in inputs {
            let _ = parse(s);
            let _ = read(s);
        }
        let mut seed = 0x1234_5678_u32;
        let alphabet = [
            '<', '>', '/', 't', 'd', 'r', '&', ';', '#', 'x', ' ', '"', 'é', '!', '-',
        ];
        for _ in 0..2000 {
            let s: String = (0..64)
                .map(|_| {
                    seed ^= seed << 13;
                    seed ^= seed >> 17;
                    seed ^= seed << 5;
                    alphabet[seed as usize % alphabet.len()]
                })
                .collect();
            let _ = parse(&s);
        }
    }

    #[test]
    fn case_insensitive_find() {
        assert_eq!(find_ci("abc ITEM 2.01", "item 2.01"), Some(4));
        assert_eq!(find_ci("é item", "ITEM"), Some(3));
        assert_eq!(find_ci("ab", "abc"), None);
        assert_eq!(find_ci_from("item item", "item", 1), Some(5));
        assert_eq!(find_ci_from("item", "item", 9), None);
    }
}
