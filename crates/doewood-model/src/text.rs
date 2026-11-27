//! A document's text with block locations (issue #166): what quail extracted (pages and their
//! blocks, with kinds and zones) or hedgehog read (one block per spreadsheet row), kept beside
//! the record so an application can chunk, quote or compare the text itself.
//!
//! It is stored as canonical JSON text, like a record (ADR 0003): one row per BLAKE3, written
//! with the record, compressed by the store as record bodies are. Ingest builds it with
//! [`TextBuilder`], which bounds it: once the blocks reach the builder's byte cap, the rest is
//! left out and [`DocumentText::truncated`] is set. The same bytes analyzed by the same step
//! versions give the same text.
//!
//! Heading paths are not stored: [`DocumentText::for_each_block`] derives them from the heading
//! blocks on every read.

use std::ops::ControlFlow;

use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;

use crate::{Blake3, Error};

/// The default cap of a stored text, in bytes of its JSON ([`TextBuilder`]): 2 MiB. A
/// 300-page contract has well under 1 MiB of text.
pub const TEXT_MAX_BYTES: usize = 2 << 20;

/// Most headings in a block's heading path: deeper ones replace the deepest.
pub const HEADING_PATH_MAX: usize = 16;

/// Most bytes of one heading's text in a heading path (cut at a character boundary).
pub const HEADING_TEXT_MAX: usize = 256;

/// What a block of text is.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BlockKind {
    /// A paragraph or a group of lines.
    Paragraph,
    /// A heading, with its [`TextBlock::level`] (1 is the highest).
    Heading,
    /// An item of a numbered or bulleted list.
    ListItem,
    /// One row of a table in a document: its cells, separated by tabs.
    TableRow,
    /// One row of a spreadsheet: its cells, separated by tabs, from the sheet's first used
    /// column.
    SheetRow,
    /// A footnote or endnote.
    Note,
    /// A comment or tracked change.
    Annotation,
    /// A kind a newer doewood wrote.
    #[serde(other)]
    Other,
}

impl BlockKind {
    /// The name, as stored and as the API returns it.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Paragraph => "paragraph",
            Self::Heading => "heading",
            Self::ListItem => "list_item",
            Self::TableRow => "table_row",
            Self::SheetRow => "sheet_row",
            Self::Note => "note",
            Self::Annotation => "annotation",
            Self::Other => "other",
        }
    }
}

/// Where on its page a block sits (quail's zone).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TextZone {
    /// Not known.
    Unknown,
    /// A running header or letterhead.
    Header,
    /// A running footer.
    Footer,
    /// The main text.
    Body,
    /// A title or heading.
    Title,
    /// Inside a table.
    Table,
    /// A signature block.
    Signature,
    /// A zone a newer doewood wrote.
    #[serde(other)]
    Other,
}

impl TextZone {
    /// The name, as stored and as the API returns it.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Unknown => "unknown",
            Self::Header => "header",
            Self::Footer => "footer",
            Self::Body => "body",
            Self::Title => "title",
            Self::Table => "table",
            Self::Signature => "signature",
            Self::Other => "other",
        }
    }
}

/// Where a page's text came from.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TextOrigin {
    /// The file's own text (a PDF text layer, DOCX XML, cells).
    Native,
    /// Optical character recognition.
    Ocr,
    /// No text: an image page that was not OCR'd.
    None,
    /// An origin a newer doewood wrote.
    #[serde(other)]
    Other,
}

impl TextOrigin {
    /// The name, as stored and as the API returns it.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Native => "native",
            Self::Ocr => "ocr",
            Self::None => "none",
            Self::Other => "other",
        }
    }
}

/// One block: a run of text of one kind.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TextBlock {
    /// What it is.
    pub kind: BlockKind,
    /// Its text. A table or sheet row's cells are separated by tabs.
    pub text: String,
    /// Where on the page it sits; `None` for a spreadsheet row.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub zone: Option<TextZone>,
    /// A heading's level, 1 the highest.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub level: Option<u8>,
    /// A table row's table, counted on its page from 0.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub table: Option<u32>,
    /// A table row's row in its table, or a sheet row's row in its sheet, from 0.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub row: Option<u32>,
}

/// One page: a printed page, a logical section (DOCX), or a sheet.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TextPage {
    /// The page's index in the document (quail's), or the sheet's in the workbook, from 0.
    pub index: u32,
    /// The sheet's name, for a spreadsheet.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sheet: Option<String>,
    /// Where its text came from.
    pub origin: TextOrigin,
    /// Its blocks, in reading order. Blocks without text are left out.
    pub blocks: Vec<TextBlock>,
}

/// A document's text: its pages and their blocks.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DocumentText {
    /// The step that extracted it (`pdf.extract`, `docx.extract`, `hedgehog`, ...).
    pub extractor: String,
    /// That step's version (hedgehog's revision for a spreadsheet).
    pub extractor_version: String,
    /// The byte cap was reached: the blocks after the last one kept are missing.
    pub truncated: bool,
    /// The pages, in order. Pages without blocks are kept, so indices stay in order.
    pub pages: Vec<TextPage>,
}

/// One block as a reader sees it: with its page, its index in the document and its heading
/// path.
#[derive(Clone, Copy, Debug)]
pub struct BlockView<'a> {
    /// Its index among all the document's blocks, from 0.
    pub index: usize,
    /// Its page.
    pub page: &'a TextPage,
    /// The block.
    pub block: &'a TextBlock,
    /// The texts of the headings above it, the highest first. A heading's path holds the
    /// headings above it, not itself.
    pub heading_path: &'a [&'a str],
}

impl DocumentText {
    /// The text as stored: canonical JSON text.
    ///
    /// # Errors
    ///
    /// [`Error::Invalid`] if it cannot be written (never for a text [`TextBuilder`] made).
    pub fn to_body(&self) -> Result<Box<RawValue>, Error> {
        crate::canonical::to_canonical(self)
            .map_err(|e| Error::Invalid(format!("document text: {e}")))
    }

    /// The text from its stored body.
    ///
    /// # Errors
    ///
    /// [`Error::Invalid`] if the body is not a document text.
    pub fn from_body(body: &RawValue) -> Result<Self, Error> {
        serde_json::from_str(body.get())
            .map_err(|e| Error::Invalid(format!("stored document text: {e}")))
    }

    /// Every block's count.
    pub fn block_count(&self) -> usize {
        self.pages.iter().map(|p| p.blocks.len()).sum()
    }

    /// Calls `f` with every block in order, with its heading path, until it breaks.
    ///
    /// A heading (outside a header or footer zone) of level `n` ends the headings of level `n`
    /// and below before it, then joins the path of the blocks after it. At most
    /// [`HEADING_PATH_MAX`] headings are kept, each cut to [`HEADING_TEXT_MAX`] bytes.
    pub fn for_each_block<'a>(&'a self, mut f: impl FnMut(BlockView<'_>) -> ControlFlow<()>) {
        let mut levels: Vec<u8> = Vec::new();
        let mut path: Vec<&'a str> = Vec::new();
        let mut index = 0;
        for page in &self.pages {
            for block in &page.blocks {
                let heading = block.kind == BlockKind::Heading
                    && !matches!(block.zone, Some(TextZone::Header | TextZone::Footer));
                let level = block.level.unwrap_or(1).max(1);
                if heading {
                    while levels.last().is_some_and(|&l| l >= level) {
                        levels.pop();
                        path.pop();
                    }
                }
                let view = BlockView {
                    index,
                    page,
                    block,
                    heading_path: &path,
                };
                if f(view).is_break() {
                    return;
                }
                if heading {
                    if path.len() == HEADING_PATH_MAX {
                        levels.pop();
                        path.pop();
                    }
                    levels.push(level);
                    path.push(cut(block.text.trim(), HEADING_TEXT_MAX));
                }
                index += 1;
            }
        }
    }
}

/// `s` cut to at most `max` bytes, at a character boundary.
fn cut(s: &str, max: usize) -> &str {
    if s.len() <= max {
        return s;
    }
    let mut end = max;
    while !s.is_char_boundary(end) {
        end -= 1;
    }
    &s[..end]
}

/// A stored text, as read back.
#[derive(Clone, Debug)]
pub struct StoredText {
    /// The file's hash.
    pub blake3: Blake3,
    /// The [`DocumentText`] as canonical JSON text.
    pub body: Box<RawValue>,
    /// When it was last written, in Unix seconds.
    pub updated_at: i64,
}

/// Bytes a block's JSON takes beyond its text, at most: its keys, kind, zone and numbers.
const BLOCK_OVERHEAD: usize = 112;
/// Bytes a page's JSON takes beyond its blocks and sheet name, at most.
const PAGE_OVERHEAD: usize = 64;

/// Builds a [`DocumentText`] within a byte cap.
///
/// Every block and page is counted at most the JSON it takes, so the stored body stays under
/// the cap plus the extractor's name and version. A `\0` in a text becomes U+FFFD: a stored
/// text, like a record, is JSON every backend can hold (ADR 0003, decision 4).
#[derive(Debug)]
pub struct TextBuilder {
    text: DocumentText,
    used: usize,
    max: usize,
}

impl TextBuilder {
    /// A builder for an extractor's text, at most `max` bytes.
    pub fn new(extractor: &str, extractor_version: &str, max: usize) -> Self {
        Self {
            text: DocumentText {
                extractor: extractor.to_owned(),
                extractor_version: extractor_version.to_owned(),
                truncated: false,
                pages: Vec::new(),
            },
            used: 0,
            max,
        }
    }

    /// Whether the cap was reached: nothing more is added.
    pub fn is_full(&self) -> bool {
        self.text.truncated
    }

    /// Starts a page; the blocks pushed next go on it. False (and nothing added) once full.
    pub fn page(&mut self, index: u32, sheet: Option<&str>, origin: TextOrigin) -> bool {
        let sheet = sheet.map(clean);
        let cost = PAGE_OVERHEAD + sheet.as_deref().map_or(0, json_len);
        if !self.take(cost) {
            return false;
        }
        self.text.pages.push(TextPage {
            index,
            sheet,
            origin,
            blocks: Vec::new(),
        });
        true
    }

    /// Adds a block to the current page. A block whose text is blank is skipped (true). False
    /// (and nothing added) once full, or before the first page.
    pub fn block(&mut self, mut block: TextBlock) -> bool {
        if self.text.truncated || self.text.pages.is_empty() {
            return false;
        }
        if block.text.trim().is_empty() {
            return true;
        }
        if block.text.contains('\0') {
            block.text = clean(&block.text);
        }
        if !self.take(BLOCK_OVERHEAD + json_len(&block.text)) {
            return false;
        }
        if let Some(page) = self.text.pages.last_mut() {
            page.blocks.push(block);
        }
        true
    }

    /// The text built.
    pub fn finish(self) -> DocumentText {
        self.text
    }

    fn take(&mut self, cost: usize) -> bool {
        if self.text.truncated {
            return false;
        }
        if self.used + cost > self.max {
            self.text.truncated = true;
            return false;
        }
        self.used += cost;
        true
    }
}

/// `s` with every `\0` replaced by U+FFFD.
fn clean(s: &str) -> String {
    if s.contains('\0') {
        s.replace('\0', "\u{fffd}")
    } else {
        s.to_owned()
    }
}

/// The bytes of `s` as a JSON string, quotes included.
fn json_len(s: &str) -> usize {
    2 + s
        .chars()
        .map(|c| match c {
            '"' | '\\' | '\n' | '\r' | '\t' | '\u{8}' | '\u{c}' => 2,
            c if (c as u32) < 0x20 => 6,
            c => c.len_utf8(),
        })
        .sum::<usize>()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn block(kind: BlockKind, text: &str, level: Option<u8>) -> TextBlock {
        TextBlock {
            kind,
            text: text.to_owned(),
            zone: Some(TextZone::Body),
            level,
            table: None,
            row: None,
        }
    }

    #[test]
    fn json_len_is_what_serde_writes() {
        for s in [
            "",
            "plain",
            "quote \" back \\ tab \t nl \n",
            "\u{1} \u{1f} é 日本 😀",
        ] {
            assert_eq!(
                json_len(s),
                serde_json::to_string(s).unwrap().len(),
                "{s:?}"
            );
        }
    }

    #[test]
    fn heading_paths_follow_levels() {
        let mut b = TextBuilder::new("x", "1", TEXT_MAX_BYTES);
        assert!(b.page(0, None, TextOrigin::Native));
        b.block(block(BlockKind::Heading, "1 Terms", Some(1)));
        b.block(block(BlockKind::Paragraph, "a", None));
        b.block(block(BlockKind::Heading, "1.1 Price", Some(2)));
        b.block(block(BlockKind::Paragraph, "b", None));
        assert!(b.page(1, None, TextOrigin::Native));
        b.block(TextBlock {
            zone: Some(TextZone::Header),
            ..block(BlockKind::Heading, "Running head", Some(1))
        });
        b.block(block(BlockKind::Heading, "2 Delivery", Some(1)));
        b.block(block(BlockKind::Paragraph, "c", None));
        let text = b.finish();
        let mut seen = Vec::new();
        text.for_each_block(|v| {
            seen.push((v.index, v.block.text.clone(), v.heading_path.join(" > ")));
            ControlFlow::Continue(())
        });
        let want = [
            (0, "1 Terms", ""),
            (1, "a", "1 Terms"),
            (2, "1.1 Price", "1 Terms"),
            (3, "b", "1 Terms > 1.1 Price"),
            (4, "Running head", "1 Terms > 1.1 Price"),
            (5, "2 Delivery", ""),
            (6, "c", "2 Delivery"),
        ];
        let want: Vec<_> = want
            .iter()
            .map(|(i, t, p)| (*i, (*t).to_owned(), (*p).to_owned()))
            .collect();
        assert_eq!(seen, want);
    }

    #[test]
    fn the_cap_truncates_and_bounds_the_body() {
        let max = 4096;
        let mut b = TextBuilder::new("pdf.extract", "3", max);
        let mut pushed = 0;
        'pages: for p in 0..100 {
            if !b.page(p, None, TextOrigin::Native) {
                break;
            }
            for i in 0..10 {
                let t = format!("paragraph {p}.{i} \"quoted\" \0 with a NUL");
                if !b.block(block(BlockKind::Paragraph, &t, None)) {
                    break 'pages;
                }
                pushed += 1;
            }
        }
        assert!(b.is_full());
        let text = b.finish();
        assert!(text.truncated);
        assert_eq!(text.block_count(), pushed);
        let body = text.to_body().unwrap();
        assert!(body.get().len() <= max + 128, "{}", body.get().len());
        assert!(!body.get().contains("\\u0000"));
        assert_eq!(DocumentText::from_body(&body).unwrap(), text);
    }

    #[test]
    fn blank_blocks_are_skipped_and_unknown_kinds_read() {
        let mut b = TextBuilder::new("x", "1", TEXT_MAX_BYTES);
        assert!(!b.block(block(BlockKind::Paragraph, "before a page", None)));
        b.page(0, Some("Sheet1"), TextOrigin::Native);
        assert!(b.block(block(BlockKind::Paragraph, "  \n", None)));
        assert_eq!(b.finish().block_count(), 0);
        let raw = r#"{"extractor":"x","extractor_version":"1","pages":[{"blocks":[{"kind":"figure","text":"t","zone":"margin"}],"index":0,"origin":"braille"}],"truncated":false}"#;
        let t: DocumentText = serde_json::from_str(raw).unwrap();
        let b = &t.pages[0].blocks[0];
        assert_eq!(
            (b.kind, b.zone, t.pages[0].origin),
            (BlockKind::Other, Some(TextZone::Other), TextOrigin::Other)
        );
    }
}
