//! A file's text with block locations (issue #166), built during its analysis and stored
//! beside its record ([`doewood_model::DocumentText`]).
//!
//! - **Documents:** the pages and blocks quail's extractor made (after OCR, the revised pages),
//!   with their kinds and zones. A table's cells become one `table_row` block per row.
//! - **Spreadsheets:** one `sheet_row` block per row of each sheet, from hedgehog's workbook.
//!
//! Both stop at the analyzer's byte cap and set `truncated`. The output depends only on the
//! bytes and the step versions.

use doewood_model::{BlockKind, DocumentText, TextBlock, TextBuilder, TextOrigin, TextZone};
use hh_core::{Value as Cell, Workbook};
use quail_core::{AnalyzerId, Artifacts, Document, RevisedDocument, TextSource, Zone};

/// The `extractor` of a spreadsheet's text; its version is hedgehog's revision.
pub const SHEET_EXTRACTOR: &str = "hedgehog";

/// The text of a document quail extracted with `extractor`: the revised document (OCR) when
/// there is one, else the extracted one. `None` when quail extracted nothing.
pub fn from_artifacts(
    artifacts: &Artifacts,
    extractor: AnalyzerId,
    max: usize,
) -> Option<DocumentText> {
    let doc = artifacts
        .get::<RevisedDocument>()
        .map(|r| &r.0)
        .or_else(|| artifacts.get::<Document>())?;
    Some(from_document(doc, extractor, max))
}

/// The text of `doc`.
pub fn from_document(doc: &Document, extractor: AnalyzerId, max: usize) -> DocumentText {
    let mut b = TextBuilder::new(extractor.name, &extractor.version.to_string(), max);
    'pages: for page in &doc.pages {
        if !b.page(page.index, None, origin(page.text_source)) {
            break;
        }
        let mut row: Option<TableRow> = None;
        for block in &page.blocks {
            let text = block.span.slice(&page.text);
            if let quail_core::BlockKind::TableCell { table, row: r, col } = block.kind {
                match &mut row {
                    Some(open) if open.table == table && open.row == r => {
                        open.push(col, text);
                    }
                    _ => {
                        if let Some(done) = row.take()
                            && !b.block(done.into_block())
                        {
                            break 'pages;
                        }
                        let mut open = TableRow::new(table, r, zone(block.zone));
                        open.push(col, text);
                        row = Some(open);
                    }
                }
                continue;
            }
            if let Some(done) = row.take()
                && !b.block(done.into_block())
            {
                break 'pages;
            }
            let (kind, level) = match block.kind {
                quail_core::BlockKind::Paragraph => (BlockKind::Paragraph, None),
                quail_core::BlockKind::Heading { level } => (BlockKind::Heading, Some(level)),
                quail_core::BlockKind::ListItem => (BlockKind::ListItem, None),
                quail_core::BlockKind::Note => (BlockKind::Note, None),
                quail_core::BlockKind::Annotation => (BlockKind::Annotation, None),
                quail_core::BlockKind::TableCell { .. } => unreachable!("handled above"),
            };
            let block = TextBlock {
                kind,
                text: text.to_owned(),
                zone: Some(zone(block.zone)),
                level,
                table: None,
                row: None,
            };
            if !b.block(block) {
                break 'pages;
            }
        }
        if let Some(done) = row.take()
            && !b.block(done.into_block())
        {
            break;
        }
    }
    b.finish()
}

/// The cells of one table row, collected until a block of another row comes.
struct TableRow {
    table: u16,
    row: u16,
    zone: TextZone,
    text: String,
    /// The column after the last cell pushed; `None` before the first.
    next_col: Option<u16>,
}

impl TableRow {
    fn new(table: u16, row: u16, zone: TextZone) -> Self {
        Self {
            table,
            row,
            zone,
            text: String::new(),
            next_col: None,
        }
    }

    /// Adds a cell, after a tab per column since the row's first column (so columns keep
    /// their places); a cell out of column order still gets one tab.
    fn push(&mut self, col: u16, text: &str) {
        let tabs = match self.next_col {
            None => col,
            Some(next) => col.saturating_sub(next) + 1,
        };
        for _ in 0..tabs {
            self.text.push('\t');
        }
        self.text.push_str(text.trim());
        let next = col.saturating_add(1);
        self.next_col = Some(self.next_col.map_or(next, |n| n.max(next)));
    }

    fn into_block(self) -> TextBlock {
        TextBlock {
            kind: BlockKind::TableRow,
            text: self.text,
            zone: Some(self.zone),
            level: None,
            table: Some(u32::from(self.table)),
            row: Some(u32::from(self.row)),
        }
    }
}

/// The text of a workbook: a page per sheet, a block per row with cells.
pub fn from_workbook(wb: &Workbook, rev: &str, max: usize) -> DocumentText {
    let mut b = TextBuilder::new(SHEET_EXTRACTOR, rev, max);
    'sheets: for (i, sheet) in wb.sheets.iter().enumerate() {
        let index = u32::try_from(i).unwrap_or(u32::MAX);
        if !b.page(index, Some(&sheet.name), TextOrigin::Native) {
            break;
        }
        let left = sheet.used_range().map_or(0, |u| u.left);
        let mut cells = sheet.cells.iter().peekable();
        while let Some(first) = cells.next() {
            let row = first.row;
            let mut text = String::new();
            let mut col = left;
            let mut cell = Some(first);
            while let Some(c) = cell {
                for _ in col..c.col {
                    text.push('\t');
                }
                col = c.col.max(col);
                if let Some(v) = &c.value {
                    push_value(&mut text, v);
                }
                cell = cells.next_if(|n| n.row == row);
            }
            let block = TextBlock {
                kind: BlockKind::SheetRow,
                text,
                zone: None,
                level: None,
                table: None,
                row: Some(row),
            };
            if !b.block(block) {
                break 'sheets;
            }
        }
    }
    b.finish()
}

/// A cell's value as text: numbers as Rust writes them, dates as ISO 8601.
fn push_value(out: &mut String, v: &Cell) {
    use std::fmt::Write as _;
    match v {
        Cell::Text(t) => out.push_str(t.trim()),
        Cell::Number(n) => {
            let _ = write!(out, "{n}");
        }
        Cell::Bool(true) => out.push_str("TRUE"),
        Cell::Bool(false) => out.push_str("FALSE"),
        Cell::DateTime(serial) => match excel_date(*serial) {
            Some(d) => out.push_str(&d),
            None => {
                let _ = write!(out, "{serial}");
            }
        },
        Cell::Error(e) => out.push_str(e),
    }
}

/// An Excel serial date (days since 1899-12-30) as `YYYY-MM-DD`, with `THH:MM:SS` when it has
/// a time of day. `None` outside years 1 to 9999.
fn excel_date(serial: f64) -> Option<String> {
    // Year 9999 is about 2,958,000 days after the epoch.
    if !serial.is_finite() || serial.abs() > 3_000_000.0 {
        return None;
    }
    let days = serial.floor();
    // Whole seconds, rounded; a time that rounds up to midnight is the next day.
    #[allow(clippy::cast_possible_truncation)]
    let mut secs = ((serial - days) * 86_400.0).round() as i64;
    #[allow(clippy::cast_possible_truncation)]
    let mut days = days as i64;
    if secs >= 86_400 {
        secs -= 86_400;
        days += 1;
    }
    let epoch = time::Date::from_calendar_date(1899, time::Month::December, 30).ok()?;
    let date = epoch.checked_add(time::Duration::days(days))?;
    if date.year() < 1 {
        return None;
    }
    let mut s = format!(
        "{:04}-{:02}-{:02}",
        date.year(),
        u8::from(date.month()),
        date.day()
    );
    if secs > 0 {
        use std::fmt::Write as _;
        let _ = write!(
            s,
            "T{:02}:{:02}:{:02}",
            secs / 3600,
            secs / 60 % 60,
            secs % 60
        );
    }
    Some(s)
}

fn origin(s: TextSource) -> TextOrigin {
    match s {
        TextSource::Native => TextOrigin::Native,
        TextSource::Ocr => TextOrigin::Ocr,
        TextSource::None => TextOrigin::None,
    }
}

fn zone(z: Zone) -> TextZone {
    match z {
        Zone::Unknown => TextZone::Unknown,
        Zone::Header => TextZone::Header,
        Zone::Footer => TextZone::Footer,
        Zone::Body => TextZone::Body,
        Zone::Title => TextZone::Title,
        Zone::Table => TextZone::Table,
        Zone::Signature => TextZone::Signature,
    }
}

#[cfg(test)]
mod tests {
    use doewood_model::TEXT_MAX_BYTES;
    use quail_core::{Block, Page, PageKind, Span};

    use super::*;

    const ID: AnalyzerId = AnalyzerId::new("docx.extract", 4);

    fn page(index: u32, parts: &[(&str, quail_core::BlockKind, Zone)]) -> Page {
        let mut text = String::new();
        let mut blocks = Vec::new();
        for (t, kind, zone) in parts {
            if !text.is_empty() {
                text.push('\n');
            }
            let start = u32::try_from(text.len()).unwrap();
            text.push_str(t);
            let end = u32::try_from(text.len()).unwrap();
            blocks.push(Block::new(Span { start, end }, *kind, *zone, None));
        }
        Page {
            text,
            blocks,
            text_source: TextSource::Native,
            ..Page::new(index, PageKind::Logical)
        }
    }

    fn cell(table: u16, row: u16, col: u16) -> quail_core::BlockKind {
        quail_core::BlockKind::TableCell { table, row, col }
    }

    #[test]
    fn blocks_keep_kinds_zones_and_table_rows() {
        use quail_core::BlockKind as K;
        let doc = Document {
            pages: vec![
                page(
                    0,
                    &[
                        ("ACME Ltd", K::Paragraph, Zone::Header),
                        ("1 Terms", K::Heading { level: 1 }, Zone::Title),
                        ("The buyer pays.", K::Paragraph, Zone::Body),
                        ("Item", cell(0, 0, 0), Zone::Table),
                        ("Qty", cell(0, 0, 1), Zone::Table),
                        ("Bolts", cell(0, 1, 0), Zone::Table),
                        ("10", cell(0, 1, 2), Zone::Table),
                        ("first", K::ListItem, Zone::Body),
                    ],
                ),
                page(1, &[("Page 2", K::Paragraph, Zone::Footer)]),
            ],
        };
        let text = from_document(&doc, ID, TEXT_MAX_BYTES);
        assert_eq!(
            (text.extractor.as_str(), text.extractor_version.as_str()),
            ("docx.extract", "4")
        );
        assert!(!text.truncated);
        let got: Vec<_> = text.pages[0]
            .blocks
            .iter()
            .map(|b| (b.kind, b.text.as_str(), b.zone, b.level, b.row))
            .collect();
        assert_eq!(
            got,
            [
                (
                    BlockKind::Paragraph,
                    "ACME Ltd",
                    Some(TextZone::Header),
                    None,
                    None
                ),
                (
                    BlockKind::Heading,
                    "1 Terms",
                    Some(TextZone::Title),
                    Some(1),
                    None
                ),
                (
                    BlockKind::Paragraph,
                    "The buyer pays.",
                    Some(TextZone::Body),
                    None,
                    None
                ),
                (
                    BlockKind::TableRow,
                    "Item\tQty",
                    Some(TextZone::Table),
                    None,
                    Some(0)
                ),
                (
                    BlockKind::TableRow,
                    "Bolts\t\t10",
                    Some(TextZone::Table),
                    None,
                    Some(1)
                ),
                (
                    BlockKind::ListItem,
                    "first",
                    Some(TextZone::Body),
                    None,
                    None
                ),
            ]
        );
        assert_eq!(text.pages[1].blocks[0].zone, Some(TextZone::Footer));
        // The same document gives the same text.
        assert_eq!(
            from_document(&doc, ID, TEXT_MAX_BYTES)
                .to_body()
                .unwrap()
                .get(),
            text.to_body().unwrap().get()
        );
    }

    #[test]
    fn a_small_cap_truncates() {
        let parts: Vec<(String, quail_core::BlockKind, Zone)> = (0..500)
            .map(|i| {
                (
                    format!("paragraph number {i} of the document"),
                    quail_core::BlockKind::Paragraph,
                    Zone::Body,
                )
            })
            .collect();
        let refs: Vec<(&str, quail_core::BlockKind, Zone)> =
            parts.iter().map(|(t, k, z)| (t.as_str(), *k, *z)).collect();
        let doc = Document {
            pages: vec![page(0, &refs)],
        };
        let text = from_document(&doc, ID, 8192);
        assert!(text.truncated);
        let n = text.block_count();
        assert!(n > 10 && n < 500, "{n}");
        assert!(text.to_body().unwrap().get().len() <= 8192 + 64);
    }

    #[test]
    fn excel_dates() {
        assert_eq!(excel_date(45_000.0).as_deref(), Some("2023-03-15"));
        assert_eq!(excel_date(45_000.5).as_deref(), Some("2023-03-15T12:00:00"));
        assert_eq!(excel_date(1.0).as_deref(), Some("1899-12-31"));
        assert_eq!(excel_date(f64::NAN), None);
        assert_eq!(excel_date(1e12), None);
        assert_eq!(excel_date(-1e300), None);
    }
}
