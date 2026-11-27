//! Inputs built in code (no real documents), the way quail's and hedgehog's tests build theirs,
//! and an in-memory object store.
#![allow(
    dead_code,
    clippy::cast_precision_loss,
    clippy::similar_names,
    clippy::format_push_string,
    clippy::format_collect
)]

use std::collections::HashMap;
use std::io::{Cursor, Write};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use doewood_ingest::{Fetch, Fetched, IngestError};
use doewood_s3::{Encryption, ObjectMeta};
use doewood_store::Source;
use lopdf::content::{Content, Operation};
use lopdf::{Document, Object, Stream, dictionary};
use zip::write::SimpleFileOptions;
use zip::{CompressionMethod, ZipWriter};

/// Objects by key, shared by every source.
#[derive(Default)]
pub struct MemFetch {
    pub objects: Mutex<HashMap<String, Vec<u8>>>,
    /// Keys that fail with a transient error this many more times.
    pub flaky: Mutex<HashMap<String, u32>>,
    /// Keys whose reads wait for a permit of their gate (one permit per read).
    pub held: Mutex<HashMap<String, Arc<tokio::sync::Semaphore>>>,
    /// Reads per key.
    pub reads: Mutex<HashMap<String, u32>>,
    /// The ETag and encryption responses report per key; a key without has no metadata.
    pub meta: Mutex<HashMap<String, (String, Encryption)>>,
    /// HEADs per key.
    pub heads: Mutex<HashMap<String, u32>>,
}

impl MemFetch {
    pub fn put(&self, key: &str, bytes: Vec<u8>) {
        self.objects.lock().unwrap().insert(key.to_owned(), bytes);
    }

    /// Puts `key` with the ETag and encryption its responses report.
    pub fn put_with(&self, key: &str, bytes: Vec<u8>, etag: &str, encryption: Encryption) {
        self.put(key, bytes);
        self.meta
            .lock()
            .unwrap()
            .insert(key.to_owned(), (etag.to_owned(), encryption));
    }

    fn meta_of(&self, key: &str) -> Option<ObjectMeta> {
        let (etag, encryption) = self.meta.lock().unwrap().get(key).cloned()?;
        let size = self.objects.lock().unwrap().get(key)?.len() as u64;
        Some(ObjectMeta {
            size,
            etag: Some(etag),
            last_modified: None,
            content_type: None,
            encryption,
        })
    }

    pub fn reads_of(&self, key: &str) -> u32 {
        self.reads.lock().unwrap().get(key).copied().unwrap_or(0)
    }

    pub fn heads_of(&self, key: &str) -> u32 {
        self.heads.lock().unwrap().get(key).copied().unwrap_or(0)
    }

    /// Holds every read of `key` until the returned gate gets a permit (one per read).
    pub fn hold(&self, key: &str) -> Arc<tokio::sync::Semaphore> {
        let gate = Arc::new(tokio::sync::Semaphore::new(0));
        self.held
            .lock()
            .unwrap()
            .insert(key.to_owned(), Arc::clone(&gate));
        gate
    }
}

#[async_trait]
impl Fetch for MemFetch {
    async fn fetch(&self, _source: &Source, key: &str) -> Result<Vec<u8>, IngestError> {
        *self
            .reads
            .lock()
            .unwrap()
            .entry(key.to_owned())
            .or_default() += 1;
        let gate = self.held.lock().unwrap().get(key).cloned();
        if let Some(gate) = gate {
            gate.acquire().await.unwrap().forget();
        }
        if let Some(n) = self.flaky.lock().unwrap().get_mut(key)
            && *n > 0
        {
            *n -= 1;
            return Err(IngestError {
                message: "S3 GetObject failed: HTTP 503 SlowDown".into(),
                transient: true,
            });
        }
        self.objects
            .lock()
            .unwrap()
            .get(key)
            .cloned()
            .ok_or_else(|| IngestError {
                message: "S3 GetObject failed: HTTP 404 NoSuchKey".into(),
                transient: false,
            })
    }

    async fn fetch_with_meta(&self, source: &Source, key: &str) -> Result<Fetched, IngestError> {
        let bytes = self.fetch(source, key).await?;
        Ok(Fetched {
            bytes,
            meta: self.meta_of(key),
        })
    }

    async fn head(&self, _source: &Source, key: &str) -> Result<Option<ObjectMeta>, IngestError> {
        *self
            .heads
            .lock()
            .unwrap()
            .entry(key.to_owned())
            .or_default() += 1;
        Ok(self.meta_of(key))
    }
}

fn zip_of<N: AsRef<str>>(parts: &[(N, String)]) -> Vec<u8> {
    let mut z = ZipWriter::new(Cursor::new(Vec::new()));
    let opts = SimpleFileOptions::default().compression_method(CompressionMethod::Stored);
    for (name, data) in parts {
        z.start_file(name.as_ref(), opts).unwrap();
        z.write_all(data.as_bytes()).unwrap();
    }
    z.finish().unwrap().into_inner()
}

fn xml_escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

/// A minimal DOCX: a title paragraph and one paragraph per body line.
pub fn docx(title: &str, body: &[&str]) -> Vec<u8> {
    const W: &str = "http://schemas.openxmlformats.org/wordprocessingml/2006/main";
    let paras: String = body
        .iter()
        .map(|l| {
            format!(
                "<w:p><w:r><w:t xml:space=\"preserve\">{}</w:t></w:r></w:p>",
                xml_escape(l)
            )
        })
        .collect();
    let title = xml_escape(title);
    zip_of(&[
        (
            "[Content_Types].xml",
            r#"<?xml version="1.0" encoding="UTF-8"?><Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types"><Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/><Default Extension="xml" ContentType="application/xml"/><Override PartName="/word/document.xml" ContentType="application/vnd.openxmlformats-officedocument.wordprocessingml.document.main+xml"/></Types>"#.to_owned(),
        ),
        (
            "_rels/.rels",
            r#"<?xml version="1.0" encoding="UTF-8"?><Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships"><Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="word/document.xml"/></Relationships>"#.to_owned(),
        ),
        (
            "word/document.xml",
            format!(
                r#"<?xml version="1.0" encoding="UTF-8"?><w:document xmlns:w="{W}"><w:body><w:p><w:pPr><w:pStyle w:val="Title"/></w:pPr><w:r><w:t>{title}</w:t></w:r></w:p>{paras}</w:body></w:document>"#
            ),
        ),
    ])
}

/// A minimal PPTX: one slide per `(title, body lines)`, with a layout and a master.
pub fn pptx(slides: &[(&str, &[&str])]) -> Vec<u8> {
    const NS: &str = r#"xmlns:a="http://schemas.openxmlformats.org/drawingml/2006/main" xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships" xmlns:p="http://schemas.openxmlformats.org/presentationml/2006/main""#;
    const REL: &str = "http://schemas.openxmlformats.org/officeDocument/2006/relationships";
    const RELS: &str = "http://schemas.openxmlformats.org/package/2006/relationships";
    let sp = |id: u32, ph: &str, body: &str| {
        format!(
            r#"<p:sp><p:nvSpPr><p:cNvPr id="{id}" name="Shape {id}"/><p:cNvSpPr/><p:nvPr>{ph}</p:nvPr></p:nvSpPr><p:spPr/><p:txBody><a:bodyPr/><a:lstStyle/>{body}</p:txBody></p:sp>"#
        )
    };
    let para = |t: &str| format!("<a:p><a:r><a:t>{}</a:t></a:r></a:p>", xml_escape(t));
    let part = |root: &str, shapes: &str| {
        format!(
            r#"<?xml version="1.0" encoding="UTF-8" standalone="yes"?><p:{root} {NS}><p:cSld><p:spTree><p:nvGrpSpPr><p:cNvPr id="1" name=""/><p:cNvGrpSpPr/><p:nvPr/></p:nvGrpSpPr><p:grpSpPr/>{shapes}</p:spTree></p:cSld></p:{root}>"#
        )
    };
    let mut ct = String::from(
        r#"<?xml version="1.0" encoding="UTF-8"?><Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types"><Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/><Default Extension="xml" ContentType="application/xml"/><Override PartName="/ppt/presentation.xml" ContentType="application/vnd.openxmlformats-officedocument.presentationml.presentation.main+xml"/>"#,
    );
    let mut ids = String::new();
    let mut prels = format!(
        r#"<?xml version="1.0" encoding="UTF-8"?><Relationships xmlns="{RELS}"><Relationship Id="rIdM" Type="{REL}/slideMaster" Target="slideMasters/slideMaster1.xml"/>"#
    );
    let mut files: Vec<(String, String)> = Vec::new();
    for (k, (title, body)) in slides.iter().enumerate() {
        let i = k + 1;
        ct.push_str(&format!(r#"<Override PartName="/ppt/slides/slide{i}.xml" ContentType="application/vnd.openxmlformats-officedocument.presentationml.slide+xml"/>"#));
        ids.push_str(&format!(r#"<p:sldId id="{}" r:id="rIdS{i}"/>"#, 256 + k));
        prels.push_str(&format!(
            r#"<Relationship Id="rIdS{i}" Type="{REL}/slide" Target="slides/slide{i}.xml"/>"#
        ));
        let lines: String = body.iter().map(|l| para(l)).collect();
        let shapes =
            sp(2, r#"<p:ph type="title"/>"#, &para(title)) + &sp(3, r#"<p:ph idx="1"/>"#, &lines);
        files.push((format!("ppt/slides/slide{i}.xml"), part("sld", &shapes)));
        files.push((
            format!("ppt/slides/_rels/slide{i}.xml.rels"),
            format!(r#"<?xml version="1.0" encoding="UTF-8"?><Relationships xmlns="{RELS}"><Relationship Id="rId1" Type="{REL}/slideLayout" Target="../slideLayouts/slideLayout1.xml"/></Relationships>"#),
        ));
    }
    ct.push_str("</Types>");
    prels.push_str("</Relationships>");
    let placeholders =
        sp(2, r#"<p:ph type="title"/>"#, "") + &sp(3, r#"<p:ph type="body" idx="1"/>"#, "");
    files.splice(0..0, [("[Content_Types].xml".to_owned(), ct)]);
    files.extend([
        (
            "_rels/.rels".to_owned(),
            format!(r#"<?xml version="1.0" encoding="UTF-8"?><Relationships xmlns="{RELS}"><Relationship Id="rId1" Type="{REL}/officeDocument" Target="ppt/presentation.xml"/></Relationships>"#),
        ),
        (
            "ppt/presentation.xml".to_owned(),
            format!(r#"<?xml version="1.0" encoding="UTF-8"?><p:presentation {NS}><p:sldMasterIdLst><p:sldMasterId id="2147483648" r:id="rIdM"/></p:sldMasterIdLst><p:sldIdLst>{ids}</p:sldIdLst><p:sldSz cx="9144000" cy="6858000"/></p:presentation>"#),
        ),
        ("ppt/_rels/presentation.xml.rels".to_owned(), prels),
        (
            "ppt/slideLayouts/slideLayout1.xml".to_owned(),
            part("sldLayout", &placeholders),
        ),
        (
            "ppt/slideLayouts/_rels/slideLayout1.xml.rels".to_owned(),
            format!(r#"<?xml version="1.0" encoding="UTF-8"?><Relationships xmlns="{RELS}"><Relationship Id="rId1" Type="{REL}/slideMaster" Target="../slideMasters/slideMaster1.xml"/></Relationships>"#),
        ),
        (
            "ppt/slideMasters/slideMaster1.xml".to_owned(),
            part("sldMaster", &placeholders),
        ),
    ]);
    zip_of(&files)
}

/// A minimal HTML page: a title, an `h1` and one paragraph per line.
pub fn html(title: &str, paras: &[&str]) -> Vec<u8> {
    let body: String = paras
        .iter()
        .map(|p| format!("<p>{}</p>", xml_escape(p)))
        .collect();
    let title = xml_escape(title);
    format!(
        "<!DOCTYPE html>\n<html lang=\"en\"><head><meta charset=\"utf-8\"><title>{title}</title></head>\
         <body><h1>{title}</h1>{body}</body></html>\n"
    )
    .into_bytes()
}

/// A minimal XLSX: one sheet, inline strings and numbers, the first row a header.
pub fn xlsx(sheet: &str, rows: &[&[&str]]) -> Vec<u8> {
    let col = |i: usize| char::from(b'A' + u8::try_from(i).unwrap());
    let mut data = String::new();
    for (r, row) in rows.iter().enumerate() {
        data.push_str(&format!("<row r=\"{}\">", r + 1));
        for (c, v) in row.iter().enumerate() {
            let at = format!("{}{}", col(c), r + 1);
            if v.parse::<f64>().is_ok() {
                data.push_str(&format!("<c r=\"{at}\"><v>{v}</v></c>"));
            } else {
                data.push_str(&format!(
                    "<c r=\"{at}\" t=\"inlineStr\"><is><t>{}</t></is></c>",
                    xml_escape(v)
                ));
            }
        }
        data.push_str("</row>");
    }
    let ns = "http://schemas.openxmlformats.org/spreadsheetml/2006/main";
    let rel = "http://schemas.openxmlformats.org/officeDocument/2006/relationships";
    zip_of(&[
        (
            "[Content_Types].xml",
            r#"<?xml version="1.0" encoding="UTF-8"?><Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types"><Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/><Default Extension="xml" ContentType="application/xml"/><Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/><Override PartName="/xl/worksheets/sheet1.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/></Types>"#.to_owned(),
        ),
        (
            "_rels/.rels",
            format!(r#"<?xml version="1.0" encoding="UTF-8"?><Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships"><Relationship Id="rId1" Type="{rel}/officeDocument" Target="xl/workbook.xml"/></Relationships>"#),
        ),
        (
            "xl/workbook.xml",
            format!(r#"<?xml version="1.0" encoding="UTF-8"?><workbook xmlns="{ns}" xmlns:r="{rel}"><sheets><sheet name="{}" sheetId="1" r:id="rId1"/></sheets></workbook>"#, xml_escape(sheet)),
        ),
        (
            "xl/_rels/workbook.xml.rels",
            format!(r#"<?xml version="1.0" encoding="UTF-8"?><Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships"><Relationship Id="rId1" Type="{rel}/worksheet" Target="worksheets/sheet1.xml"/></Relationships>"#),
        ),
        (
            "xl/worksheets/sheet1.xml",
            format!(r#"<?xml version="1.0" encoding="UTF-8"?><worksheet xmlns="{ns}"><sheetData>{data}</sheetData></worksheet>"#),
        ),
    ])
}

/// A large XLSX of `rows` x `cols` numbers (`cols` at most 26), for the time limits.
pub fn big_xlsx(rows: usize, cols: usize, seed: u64) -> Vec<u8> {
    big_xlsx_with(rows, cols, seed, false)
}

/// [`big_xlsx`], with every other column inline text (`text`) instead of numbers.
pub fn big_xlsx_with(rows: usize, cols: usize, seed: u64, text: bool) -> Vec<u8> {
    big_xlsx_sheets(1, rows, cols, seed, text)
}

/// [`big_xlsx_with`] with `sheets` sheets (`Data`, then `Data2`, ...), each `rows` x `cols`;
/// sheet `i` is seeded with `seed + i`.
pub fn big_xlsx_sheets(sheets: usize, rows: usize, cols: usize, seed: u64, text: bool) -> Vec<u8> {
    let sheet_xml = |seed: u64| {
        let mut data = String::with_capacity(rows * cols * 24);
        for r in 1..=rows {
            data.push_str(&format!("<row r=\"{r}\">"));
            for c in 0..cols {
                let col = char::from(b'A' + u8::try_from(c).unwrap());
                let v = (r as u64 * 31 + c as u64) ^ seed;
                if text && c % 2 == 1 {
                    data.push_str(&format!(
                        "<c r=\"{col}{r}\" t=\"inlineStr\"><is><t>item {v} kom</t></is></c>"
                    ));
                } else {
                    data.push_str(&format!("<c r=\"{col}{r}\"><v>{v}</v></c>"));
                }
            }
            data.push_str("</row>");
        }
        data
    };
    let ns = "http://schemas.openxmlformats.org/spreadsheetml/2006/main";
    let rel = "http://schemas.openxmlformats.org/officeDocument/2006/relationships";
    let (mut overrides, mut entries, mut rels) = (String::new(), String::new(), String::new());
    for i in 1..=sheets {
        let name = if i == 1 {
            "Data".to_owned()
        } else {
            format!("Data{i}")
        };
        overrides.push_str(&format!(r#"<Override PartName="/xl/worksheets/sheet{i}.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>"#));
        entries.push_str(&format!(
            r#"<sheet name="{name}" sheetId="{i}" r:id="rId{i}"/>"#
        ));
        rels.push_str(&format!(
            r#"<Relationship Id="rId{i}" Type="{rel}/worksheet" Target="worksheets/sheet{i}.xml"/>"#
        ));
    }
    let mut parts = vec![
        (
            "[Content_Types].xml".to_owned(),
            format!(
                r#"<?xml version="1.0" encoding="UTF-8"?><Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types"><Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/><Default Extension="xml" ContentType="application/xml"/><Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/>{overrides}</Types>"#
            ),
        ),
        (
            "_rels/.rels".to_owned(),
            format!(
                r#"<?xml version="1.0" encoding="UTF-8"?><Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships"><Relationship Id="rId1" Type="{rel}/officeDocument" Target="xl/workbook.xml"/></Relationships>"#
            ),
        ),
        (
            "xl/workbook.xml".to_owned(),
            format!(
                r#"<?xml version="1.0" encoding="UTF-8"?><workbook xmlns="{ns}" xmlns:r="{rel}"><sheets>{entries}</sheets></workbook>"#
            ),
        ),
        (
            "xl/_rels/workbook.xml.rels".to_owned(),
            format!(
                r#"<?xml version="1.0" encoding="UTF-8"?><Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">{rels}</Relationships>"#
            ),
        ),
    ];
    for i in 1..=sheets {
        parts.push((
            format!("xl/worksheets/sheet{i}.xml"),
            format!(
                r#"<?xml version="1.0" encoding="UTF-8"?><worksheet xmlns="{ns}"><sheetData>{}</sheetData></worksheet>"#,
                sheet_xml(seed + i as u64 - 1)
            ),
        ));
    }
    zip_of(&parts)
}

/// US Letter, points.
const W: f32 = 612.0;
const H: f32 = 792.0;

/// A one-page PDF with these lines of Helvetica text and an Info `Title`.
pub fn pdf(lines: &[&str], title: &str) -> Vec<u8> {
    pdf_pages(&[lines], title)
}

/// A PDF with one page per entry of `pages`, each with its lines of Helvetica text.
pub fn pdf_pages(pages: &[&[&str]], title: &str) -> Vec<u8> {
    let mut doc = Document::with_version("1.7");
    let pages_id = doc.new_object_id();
    let font_id = doc.add_object(dictionary! {
        "Type" => "Font",
        "Subtype" => "Type1",
        "BaseFont" => "Helvetica",
        "Encoding" => "WinAnsiEncoding",
    });
    let mut kids: Vec<Object> = Vec::new();
    for lines in pages {
        let mut ops = Vec::new();
        for (i, l) in lines.iter().enumerate() {
            let top = 72.0 + 16.0 * i as f32;
            ops.push(Operation::new("BT", vec![]));
            ops.push(Operation::new("Tf", vec!["F1".into(), 11.into()]));
            ops.push(Operation::new("Td", vec![72.into(), (H - top).into()]));
            ops.push(Operation::new(
                "Tj",
                vec![Object::string_literal(l.as_bytes().to_vec())],
            ));
            ops.push(Operation::new("ET", vec![]));
        }
        let content = Content { operations: ops };
        let content_id = doc.add_object(Stream::new(dictionary! {}, content.encode().unwrap()));
        let page_id = doc.add_object(dictionary! {
            "Type" => "Page",
            "Parent" => pages_id,
            "Contents" => content_id,
        });
        kids.push(page_id.into());
    }
    let count = i64::try_from(kids.len()).unwrap();
    doc.objects.insert(
        pages_id,
        Object::Dictionary(dictionary! {
            "Type" => "Pages",
            "Kids" => kids,
            "Count" => count,
            "Resources" => dictionary! { "Font" => dictionary! { "F1" => font_id } },
            "MediaBox" => vec![0.into(), 0.into(), W.into(), H.into()],
        }),
    );
    let catalog_id = doc.add_object(dictionary! { "Type" => "Catalog", "Pages" => pages_id });
    let info_id = doc.add_object(dictionary! {
        "Title" => Object::string_literal(title),
        "Producer" => Object::string_literal("doewood test generator"),
    });
    doc.trailer.set("Root", catalog_id);
    doc.trailer.set("Info", info_id);
    let mut out = Vec::new();
    doc.save_to(&mut out).unwrap();
    out
}

/// A PIB (Serbian tax number) with its ISO 7064 MOD 11,10 check digit.
pub fn pib(head: &str) -> String {
    let mut a = 10;
    for d in head.bytes().map(|b| u32::from(b - b'0')) {
        a = (a + d) % 10;
        if a == 0 {
            a = 10;
        }
        a = (a * 2) % 11;
    }
    format!("{head}{}", (11 - a) % 10)
}

/// A long, varied English paragraph made from `seed`: documents from the same seed share
/// almost every word 3-shingle, documents from different seeds almost none.
pub fn prose(seed: u64, words: usize) -> String {
    const VOCAB: &[&str] = &[
        "delivery",
        "warehouse",
        "invoice",
        "pallet",
        "contract",
        "schedule",
        "carrier",
        "freight",
        "inspection",
        "payment",
        "quantity",
        "supplier",
        "customer",
        "order",
        "transport",
        "shipment",
        "storage",
        "insurance",
        "liability",
        "notice",
        "service",
        "maintenance",
        "equipment",
        "material",
        "quality",
        "standard",
        "delay",
        "penalty",
        "warranty",
        "audit",
        "report",
        "budget",
        "estimate",
        "approval",
        "review",
        "signature",
        "amendment",
        "annex",
        "clause",
        "term",
        "renewal",
        "termination",
        "obligation",
        "account",
        "transfer",
        "balance",
        "currency",
        "price",
        "discount",
        "tax",
    ];
    let mut x = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
    let mut out = Vec::with_capacity(words);
    for _ in 0..words {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        out.push(VOCAB[usize::try_from(x % VOCAB.len() as u64).unwrap()]);
    }
    out.join(" ")
}

/// Text of document `i` of a generated corpus with near-duplicates (issue #126), like the
/// invoices of a few suppliers: four in five come from one of `templates` templates (a
/// 300-word body that all of its documents share, under a header of their own: number, date,
/// amount, customer), the fifth is distinct. Two documents of one template have a Jaccard
/// estimate of about 0.9 and share most of their 32 LSH bands.
pub fn near_duplicate_text(i: u64, templates: u64) -> String {
    if i % 5 == 4 {
        return prose(1_000_000 + i, 300);
    }
    format!(
        "invoice number {i} dated 2026 {} {} amount {} customer {} {}",
        i % 12 + 1,
        i % 28 + 1,
        i * 37 % 100_000,
        i % 97,
        prose(i % templates, 300)
    )
}

/// The `MinHash` signature and LSH bands of `text`, as quail emits them.
pub fn sketch(text: &str) -> Option<(Vec<u8>, Vec<u64>)> {
    let m = quail_sketch::minhash::text_minhash(text)?;
    Some((m.to_bytes(), m.bands().to_vec()))
}

/// The `similar_to` edges out of the documents `hashes`: the keys of both ends, the props
/// and the confidence, sorted. Node and edge ids are left out: they differ between stores.
pub async fn similar_edges(
    b: &dyn doewood_model::Backend,
    hashes: &[doewood_model::Blake3],
) -> Vec<(String, String, String, u32)> {
    use doewood_model::{Direction, EdgeKind, NeighbourQuery, NodeKind};
    let q = NeighbourQuery {
        direction: Direction::Out,
        edge_kinds: vec![EdgeKind::SimilarTo],
        ..NeighbourQuery::default()
    };
    let mut out = Vec::new();
    for h in hashes {
        let hex = h.to_hex();
        let Some(node) = b.node_by_key(NodeKind::Document, &hex).await.unwrap() else {
            continue;
        };
        for nb in b.neighbours(node.id, &q).await.unwrap() {
            let e = nb.edge.edge;
            out.push((
                hex.clone(),
                nb.node.key,
                e.props.to_string(),
                e.confidence.to_bits(),
            ));
        }
    }
    out.sort();
    out
}
