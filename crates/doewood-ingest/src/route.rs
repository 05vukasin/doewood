//! Which analyzer a file goes to: quail for documents, hedgehog for spreadsheets.
//!
//! The decision comes from the bytes: quail's sniff (`engine.sniff`) first, then a bounded
//! look for the spreadsheet containers quail calls a plain ZIP or OLE file (XLSB, ODS, XLS).
//! The object key's extension only decides between a spreadsheet and an unknown container
//! when the bytes do not tell. CSV is text, and quail reads text.

use quail_core::Format;
use quail_engine::Sniff;

/// A spreadsheet format hedgehog reads.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SheetFormat {
    /// Office Open XML workbook (`.xlsx`, `.xlsm`).
    Xlsx,
    /// Binary workbook (`.xlsb`).
    Xlsb,
    /// Excel 97-2003 (`.xls`, an OLE container).
    Xls,
    /// `OpenDocument` spreadsheet (`.ods`).
    Ods,
}

impl SheetFormat {
    /// The `format` written in the record.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Xlsx => "xlsx",
            Self::Xlsb => "xlsb",
            Self::Xls => "xls",
            Self::Ods => "ods",
        }
    }
}

/// Where a file goes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Route {
    /// quail: PDF, DOCX, PPTX, HTML, text (CSV included), images, and everything it only identifies.
    Quail(Format),
    /// hedgehog, after quail's identity layer.
    Sheet(SheetFormat),
}

const XLSX_MIME: &str = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet";
const OLE_MIME: &str = "application/x-ole-storage";

/// The route for a file with these bytes and this object key.
pub fn route(sniff: Sniff, bytes: &[u8], key: &str) -> Route {
    let ext = extension(key);
    match sniff.format {
        Format::Zip if sniff.mime == XLSX_MIME => Route::Sheet(SheetFormat::Xlsx),
        Format::Zip => {
            if is_ods(bytes) {
                Route::Sheet(SheetFormat::Ods)
            } else if has_entry(bytes, b"xl/workbook.bin") {
                Route::Sheet(SheetFormat::Xlsb)
            } else if has_entry(bytes, b"xl/workbook.xml") {
                Route::Sheet(SheetFormat::Xlsx)
            } else {
                match ext.as_deref() {
                    Some("xlsx" | "xlsm" | "xlam") => Route::Sheet(SheetFormat::Xlsx),
                    Some("xlsb") => Route::Sheet(SheetFormat::Xlsb),
                    Some("ods") => Route::Sheet(SheetFormat::Ods),
                    _ => Route::Quail(Format::Zip),
                }
            }
        }
        // OLE holds .xls and .doc/.msg alike: the workbook stream names it, else the
        // extension. Anything else stays an identified unknown.
        Format::Unknown if sniff.mime == OLE_MIME => {
            if contains(bytes, &utf16(b"Workbook"))
                || contains(bytes, &utf16(b"Book"))
                || matches!(ext.as_deref(), Some("xls" | "xla"))
            {
                Route::Sheet(SheetFormat::Xls)
            } else {
                Route::Quail(Format::Unknown)
            }
        }
        other => Route::Quail(other),
    }
}

/// The lowercase extension of an object key.
fn extension(key: &str) -> Option<String> {
    let name = key.rsplit('/').next()?;
    let (stem, ext) = name.rsplit_once('.')?;
    (!stem.is_empty() && !ext.is_empty()).then(|| ext.to_ascii_lowercase())
}

/// An ODF package: its first entry is a stored `mimetype` naming a spreadsheet.
fn is_ods(b: &[u8]) -> bool {
    b.get(30..38) == Some(b"mimetype")
        && b.get(38..120)
            .is_some_and(|w| contains(w, b"application/vnd.oasis.opendocument.spreadsheet"))
}

/// The ZIP's central directory (in the last 1 MiB) names `entry`.
fn has_entry(b: &[u8], entry: &[u8]) -> bool {
    let tail = &b[b.len().saturating_sub(1 << 20)..];
    contains(tail, entry)
}

fn utf16(ascii: &[u8]) -> Vec<u8> {
    ascii.iter().flat_map(|c| [*c, 0]).collect()
}

fn contains(hay: &[u8], needle: &[u8]) -> bool {
    !needle.is_empty() && hay.windows(needle.len()).any(|w| w == needle)
}

#[cfg(test)]
mod tests {
    use quail_engine::detect;

    use super::*;

    fn r(bytes: &[u8], key: &str) -> Route {
        route(detect(bytes), bytes, key)
    }

    /// A stored ZIP with these entries (names only, empty contents).
    pub(crate) fn zip(entries: &[(&str, &[u8])]) -> Vec<u8> {
        use std::io::Write as _;
        let mut z = zip::ZipWriter::new(std::io::Cursor::new(Vec::new()));
        let opts = zip::write::SimpleFileOptions::default()
            .compression_method(zip::CompressionMethod::Stored);
        for (name, data) in entries {
            z.start_file(*name, opts).unwrap();
            z.write_all(data).unwrap();
        }
        z.finish().unwrap().into_inner()
    }

    #[test]
    fn documents_go_to_quail() {
        assert_eq!(r(b"%PDF-1.7\n...", "a.pdf"), Route::Quail(Format::Pdf));
        assert_eq!(
            r(b"Plain text, a few words of it.\n", "notes.txt"),
            Route::Quail(Format::Text)
        );
        assert_eq!(r(b"a;b;c\n1;2;3\n", "t.csv"), Route::Quail(Format::Text));
        let docx = zip(&[("[Content_Types].xml", b""), ("word/document.xml", b"")]);
        assert_eq!(r(&docx, "x.bin"), Route::Quail(Format::Docx));
        assert_eq!(
            r(b"\x89PNG\r\n\x1a\n....", "p.png"),
            Route::Quail(Format::Image)
        );
        assert_eq!(r(b"", "empty"), Route::Quail(Format::Unknown));
        assert_eq!(
            r(&[0u8, 1, 2, 3, 255, 254], "blob"),
            Route::Quail(Format::Unknown)
        );
    }

    #[test]
    fn spreadsheets_go_to_hedgehog_by_content() {
        let xlsx = zip(&[("[Content_Types].xml", b""), ("xl/workbook.xml", b"")]);
        assert_eq!(
            r(&xlsx, "data/no-extension"),
            Route::Sheet(SheetFormat::Xlsx)
        );
        let binary = zip(&[("[Content_Types].xml", b""), ("xl/workbook.bin", b"")]);
        assert_eq!(r(&binary, "q3.zip"), Route::Sheet(SheetFormat::Xlsb));
        let ods = zip(&[(
            "mimetype",
            b"application/vnd.oasis.opendocument.spreadsheet".as_slice(),
        )]);
        assert_eq!(r(&ods, "budget"), Route::Sheet(SheetFormat::Ods));
        let odt = zip(&[(
            "mimetype",
            b"application/vnd.oasis.opendocument.text".as_slice(),
        )]);
        assert_eq!(r(&odt, "letter.odt"), Route::Quail(Format::Zip));
        // A plain ZIP is a spreadsheet only when its name says so.
        let plain = zip(&[("readme.txt", b"hi")]);
        assert_eq!(r(&plain, "archive.zip"), Route::Quail(Format::Zip));
        assert_eq!(r(&plain, "broken.XLSX"), Route::Sheet(SheetFormat::Xlsx));
    }

    #[test]
    fn ole_is_a_workbook_only_when_it_says_so() {
        let mut ole = vec![0xD0, 0xCF, 0x11, 0xE0, 0xA1, 0xB1, 0x1A, 0xE1];
        ole.extend_from_slice(&[0; 64]);
        assert_eq!(r(&ole, "old.doc"), Route::Quail(Format::Unknown));
        assert_eq!(r(&ole, "old.xls"), Route::Sheet(SheetFormat::Xls));
        ole.extend_from_slice(&utf16(b"Workbook"));
        assert_eq!(r(&ole, "noext"), Route::Sheet(SheetFormat::Xls));
    }

    #[test]
    fn extensions() {
        assert_eq!(extension("a/b/c.XLSX").as_deref(), Some("xlsx"));
        assert_eq!(extension("a.b/c"), None);
        assert_eq!(extension(".hidden"), None);
        assert_eq!(extension("x."), None);
    }
}
