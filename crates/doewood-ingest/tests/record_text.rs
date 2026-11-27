//! Issue #98: a record is written as JSON text straight from quail's record, without the
//! `serde_json::Value` tree, and read back without one. The text must be byte for byte what
//! the tree wrote (`to_value(record)`, `format` set for a spreadsheet, `to_string()`), for every
//! kind of file, and what ingest reads from it must not change.

mod common;

use std::io::{Cursor, Write as _};
use std::path::PathBuf;
use std::sync::Arc;

use doewood_core::MasterKey;
use doewood_ingest::analyze::RecordParts;
use doewood_ingest::facts::DocFacts;
use doewood_ingest::{Analyzer, AnalyzerConfig};
use doewood_model::{PutOutcome, RecordStore};
use doewood_store::{Standalone, Store};
use image::{DynamicImage, ImageFormat, Rgba, RgbaImage};
use serde_json::Value as Json;
use uuid::Uuid;

/// The text before #98: the record as a `Value` tree, then the tree as text.
fn tree_text(parts: &RecordParts) -> String {
    let mut v = serde_json::to_value(&parts.record).unwrap_or(Json::Null);
    if let Some(f) = parts.format {
        v["format"] = Json::String(f.to_owned());
    }
    v.to_string()
}

fn analyzer() -> Analyzer {
    Analyzer::new(&AnalyzerConfig {
        workers: 1,
        ocr_models: PathBuf::from("/nonexistent/doewood-ocr-models"),
        ..AnalyzerConfig::default()
    })
    .unwrap()
}

fn zip(parts: &[(&str, &[u8])]) -> Vec<u8> {
    let mut z = zip::ZipWriter::new(Cursor::new(Vec::new()));
    let opts =
        zip::write::SimpleFileOptions::default().compression_method(zip::CompressionMethod::Stored);
    for (name, data) in parts {
        z.start_file(*name, opts).unwrap();
        z.write_all(data).unwrap();
    }
    z.finish().unwrap().into_inner()
}

fn png() -> Vec<u8> {
    let img = RgbaImage::from_fn(40, 30, |x, y| {
        Rgba([
            u8::try_from(x * 6).unwrap(),
            u8::try_from(y * 8).unwrap(),
            90,
            255,
        ])
    });
    let mut out = Cursor::new(Vec::new());
    DynamicImage::ImageRgba8(img)
        .write_to(&mut out, ImageFormat::Png)
        .unwrap();
    out.into_inner()
}

/// Generated files of every kind ingest routes: documents with parties, amounts, dates and
/// numbers, spreadsheets, text, images, containers, and files that fail or are unknown.
fn corpus() -> Vec<(&'static str, Vec<u8>)> {
    let seller = common::pib("10000000");
    let invoice_lines = [
        "INVOICE No. 7/2026".to_owned(),
        "Date of issue: 14.03.2026".to_owned(),
        format!("Seller: Alpha Trade d.o.o., Beograd, PIB {seller}"),
        "Buyer: Beta Logistics a.d., Novi Sad".to_owned(),
        "Delivery of 40 pallets under contract 114/23.".to_owned(),
        "Total amount: 4.200,50 RSD".to_owned(),
        "Payment due within 15 days to account 160-0000000000000-00.".to_owned(),
        "Contact: billing@alpha-trade.example".to_owned(),
    ];
    let invoice: Vec<&str> = invoice_lines.iter().map(String::as_str).collect();
    let prose = common::prose(3, 1200);
    let words: Vec<&str> = prose.split(' ').collect();
    let lines: Vec<String> = words.chunks(12).map(|c| c.join(" ")).collect();
    let pages: Vec<Vec<&str>> = lines
        .chunks(30)
        .map(|c| c.iter().map(String::as_str).collect())
        .collect();
    let page_refs: Vec<&[&str]> = pages.iter().map(Vec::as_slice).collect();
    let paragraphs: Vec<&str> = lines.iter().map(String::as_str).collect();
    let mut docx_body = invoice.clone();
    docx_body.extend(&paragraphs);
    let table: Vec<Vec<String>> = (0..40)
        .map(|r| {
            vec![
                format!("Item {r}"),
                format!("{}", r * 3),
                format!("{}.25", r + 1),
            ]
        })
        .collect();
    let mut rows: Vec<Vec<&str>> = vec![vec!["Item", "Qty", "Price"]];
    rows.extend(table.iter().map(|r| r.iter().map(String::as_str).collect()));
    let row_refs: Vec<&[&str]> = rows.iter().map(Vec::as_slice).collect();
    let binary: Vec<u8> = (0..4096u32)
        .map(|i| u8::try_from(i * 7 % 251).unwrap())
        .collect();
    vec![
        ("invoice.pdf", common::pdf(&invoice, "Invoice 7/2026")),
        (
            "report.pdf",
            common::pdf_pages(&page_refs, "Quarterly report"),
        ),
        ("contract.docx", common::docx("Supply contract", &docx_body)),
        ("prices.xlsx", common::xlsx("Prices", &row_refs)),
        (
            "deck.pptx",
            common::pptx(&[
                ("Supply plan", &["40 pallets in March"]),
                ("Prices", &paragraphs[..3]),
            ]),
        ),
        ("terms.html", common::html("Delivery terms", &invoice)),
        ("big.xlsx", common::big_xlsx_with(3000, 8, 11, true)),
        ("notes.txt", prose.clone().into_bytes()),
        (
            "escapes.txt",
            format!(
                "Ünïcødé \"quoted\" back\\slash\ttab \u{2028} ½ € 日本語 \u{1F600}\n{}",
                common::prose(5, 300)
            )
            .into_bytes(),
        ),
        (
            "table.csv",
            b"item,qty,price\nbolts,10,2.5\nnuts,20,1.25\n".to_vec(),
        ),
        ("empty.bin", Vec::new()),
        ("blob.bin", binary),
        (
            "archive.zip",
            zip(&[("a.txt", b"hello"), ("b/c.txt", prose.as_bytes())]),
        ),
        ("scan.png", png()),
        (
            "broken.pdf",
            b"%PDF-1.7\n1 0 obj << /Type /Catalog >>\ngarbage".to_vec(),
        ),
        (
            "broken.xlsx",
            zip(&[
                ("[Content_Types].xml", b"<Types/>"),
                ("xl/workbook.xml", b"<x"),
            ]),
        ),
    ]
}

/// Every record's text is the tree's text, byte for byte; what ingest reads from it is the
/// same as reading every fact; SQLite stores and returns that text, and a second write of it
/// is unchanged.
#[tokio::test]
async fn record_text_is_what_the_value_tree_wrote() {
    let a = analyzer();
    let store = Standalone::new(Arc::new(
        Store::open_in_memory(&MasterKey::from_bytes([1; 32])).unwrap(),
    ));
    let pg = postgres().await;
    for (name, bytes) in corpus() {
        let blake3 = doewood_ingest::analyze::content_hash(&bytes);
        let analysis = a.analyze(bytes.clone(), name);
        let parts = a.analyze_record(bytes, name);
        let body = parts.to_body();
        let tree = tree_text(&parts);
        assert_eq!(body.get(), tree, "{name}");
        println!(
            "{name}: {} bytes, {} facts, format {:?}, outcome {:?}",
            tree.len(),
            parts.record.facts.len(),
            parts.format,
            analysis.outcome
        );
        let facts = DocFacts::from_record(&body);
        assert_eq!(facts, DocFacts::from_record_every_fact(&body), "{name}");

        let versions = analysis.versions;
        assert_eq!(
            store.put_record(&blake3, &body, &versions).await.unwrap(),
            PutOutcome::Inserted,
            "{name}"
        );
        assert_eq!(
            store.put_record(&blake3, &body, &versions).await.unwrap(),
            PutOutcome::Unchanged,
            "{name}"
        );
        let back = store.get_record(&blake3).await.unwrap().unwrap();
        assert_eq!(back.body.get(), tree, "{name}");
        assert_eq!(DocFacts::from_record(&back.body), facts, "{name}");

        if let Some((pg, admin, schema)) = &pg {
            assert_eq!(
                pg.put_record(&blake3, &body, &versions).await.unwrap(),
                PutOutcome::Inserted,
                "{name}"
            );
            assert_eq!(
                pg.put_record(&blake3, &body, &versions).await.unwrap(),
                PutOutcome::Unchanged,
                "{name}"
            );
            // Stored as text since schema 8 (issue #121): read back, it is the tree's text.
            let back = pg.get_record(&blake3).await.unwrap().unwrap();
            assert_eq!(back.body.get(), tree, "{name}");
            assert_eq!(DocFacts::from_record(&back.body), facts, "{name}");
            // A row as schema 7 wrote it (jsonb, which keeps the value, not the text) reads
            // as the same bytes, before and after its conversion to text.
            let mut old = *blake3.as_bytes();
            old[0] ^= 0xff;
            admin
                .execute(
                    &format!(
                        "INSERT INTO {schema}.records (blake3, record, versions) \
                         VALUES ($1, $2::text::jsonb, '{{}}')"
                    ),
                    &[&old.as_slice(), &tree],
                )
                .await
                .unwrap();
            let old = doewood_model::Blake3::from_slice(&old).unwrap();
            let back = pg.get_record(&old).await.unwrap().unwrap();
            assert_eq!(back.body.get(), tree, "{name}, jsonb");
            pg.convert_records().await.unwrap();
            let back = pg.get_record(&old).await.unwrap().unwrap();
            assert_eq!(back.body.get(), tree, "{name}, converted");
        }
    }
    if let Some((_, admin, schema)) = pg {
        admin
            .batch_execute(&format!("DROP SCHEMA {schema} CASCADE"))
            .await
            .unwrap();
    }
}

const PG_VAR: &str = "DOEWOOD_TEST_POSTGRES_URL";

/// A Postgres backend in a schema of its own, if `DOEWOOD_TEST_POSTGRES_URL` is set.
async fn postgres() -> Option<(doewood_pg::PgStore, tokio_postgres::Client, String)> {
    let Some(url) = std::env::var(PG_VAR).ok().filter(|v| !v.trim().is_empty()) else {
        eprintln!("skipping: {PG_VAR} is not set; the Postgres variant did not run");
        return None;
    };
    let (admin, conn) = tokio_postgres::connect(&url, tokio_postgres::NoTls)
        .await
        .expect("connect to the test database");
    tokio::spawn(conn);
    let schema = format!("dw_records_{}", Uuid::now_v7().simple());
    admin
        .batch_execute(&format!("CREATE SCHEMA {schema}"))
        .await
        .unwrap();
    admin
        .batch_execute(&format!(
            "BEGIN; SELECT pg_advisory_xact_lock({}); \
             DO $$ BEGIN \
                 IF EXISTS (SELECT 1 FROM pg_available_extensions WHERE name = 'vector') \
                 THEN CREATE EXTENSION IF NOT EXISTS vector SCHEMA public; END IF; \
             END $$; COMMIT",
            doewood_pg::MIGRATION_LOCK
        ))
        .await
        .unwrap();
    let sep = if url.contains('?') { '&' } else { '?' };
    let scoped = format!("{url}{sep}options=-c%20search_path%3D{schema}");
    let url = doewood_core::PostgresUrl::parse(&scoped).unwrap();
    let pg = doewood_pg::PgStore::connect(&url)
        .await
        .unwrap_or_else(|e| panic!("connect {schema}: {e}"));
    Some((pg, admin, schema))
}

/// Issue #166: the text with block locations of every kind of file. Documents get quail's
/// pages and blocks, spreadsheets a block per row, the rest none; the same bytes always give
/// the same text, and a small cap truncates it.
#[test]
fn document_text_of_every_kind() {
    let a = analyzer();
    for (name, bytes) in corpus() {
        let first = a.analyze(bytes.clone(), name);
        let again = a.analyze(bytes.clone(), name);
        assert_eq!(
            first.text.as_ref().map(|t| t.get().to_owned()),
            again.text.as_ref().map(|t| t.get().to_owned()),
            "{name}: the same bytes give the same text"
        );
        let text = first
            .text
            .as_ref()
            .map(|t| doewood_model::DocumentText::from_body(t).unwrap());
        let words = |t: &doewood_model::DocumentText| -> String {
            t.pages
                .iter()
                .flat_map(|p| &p.blocks)
                .map(|b| b.text.as_str())
                .collect::<Vec<_>>()
                .join("\n")
        };
        match name {
            "invoice.pdf" | "report.pdf" => {
                let t = text.expect(name);
                assert_eq!(t.extractor, "pdf.extract", "{name}");
                assert!(!t.truncated, "{name}");
                if name == "invoice.pdf" {
                    assert!(words(&t).contains("INVOICE No. 7/2026"), "{name}");
                } else {
                    assert!(t.pages.len() > 1, "{name}: {} pages", t.pages.len());
                }
            }
            "contract.docx" => {
                let t = text.expect(name);
                assert_eq!(t.extractor, "docx.extract", "{name}");
                assert!(words(&t).contains("Delivery of 40 pallets"), "{name}");
            }
            "notes.txt" | "escapes.txt" | "table.csv" => {
                let t = text.expect(name);
                assert_eq!(t.extractor, "text.extract", "{name}");
                assert!(t.block_count() > 0, "{name}");
            }
            "deck.pptx" | "terms.html" => {
                let t = text.expect(name);
                let want = if name == "deck.pptx" {
                    ("pptx.extract", "40 pallets in March")
                } else {
                    ("html.extract", "INVOICE No. 7/2026")
                };
                assert_eq!(t.extractor, want.0, "{name}");
                assert!(words(&t).contains(want.1), "{name}");
            }
            "prices.xlsx" => {
                let t = text.expect(name);
                assert_eq!(t.extractor, "hedgehog", "{name}");
                let rows: Vec<&str> = t.pages[0].blocks.iter().map(|b| b.text.as_str()).collect();
                assert_eq!(rows[0], "Item\tQty\tPrice", "{name}");
                assert_eq!(rows.len(), 41, "{name}");
            }
            "big.xlsx" => {
                let t = text.expect(name);
                assert!(t.block_count() >= 3000, "{name}: {}", t.block_count());
            }
            "empty.bin" | "blob.bin" | "archive.zip" | "scan.png" | "broken.xlsx" => {
                assert!(text.is_none(), "{name}");
            }
            _ => {}
        }
        if let Some(t) = &first.text {
            assert!(
                t.get().len() <= doewood_model::TEXT_MAX_BYTES + 256,
                "{name}"
            );
        }
    }
    let small = Analyzer::new(&AnalyzerConfig {
        workers: 1,
        ocr_models: PathBuf::from("/nonexistent/doewood-ocr-models"),
        text_max_bytes: 4096,
        ..AnalyzerConfig::default()
    })
    .unwrap();
    let big = common::big_xlsx_with(3000, 8, 11, true);
    let t = small.analyze(big, "big.xlsx").text.unwrap();
    let t = doewood_model::DocumentText::from_body(&t).unwrap();
    assert!(t.truncated);
    let off = Analyzer::new(&AnalyzerConfig {
        workers: 1,
        ocr_models: PathBuf::from("/nonexistent/doewood-ocr-models"),
        text_max_bytes: 0,
        ..AnalyzerConfig::default()
    })
    .unwrap();
    assert!(
        off.analyze(common::docx("x", &["one paragraph"]), "x.docx")
            .text
            .is_none()
    );
}
