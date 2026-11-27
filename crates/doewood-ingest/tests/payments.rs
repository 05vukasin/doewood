//! Payments (issue #193) through ingest: generated contracts and invoices (PDF, DOCX, text)
//! go through the pipeline, and each document's payment row, its supplier and buyer
//! companies, the highlights against the stored document text, and the spend per supplier
//! are checked. On SQLite and, when `DOEWOOD_TEST_POSTGRES_URL` is set, on Postgres in a
//! schema of its own. A scanned contract (an image-only PDF) is read through OCR when the
//! models are there (`DOEWOOD_OCR_MODELS`; feature `ocr`).

mod common;

use std::ops::ControlFlow;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use common::MemFetch;
use doewood_core::{MasterKey, SourceId};
use doewood_ingest::{Analyzer, AnalyzerConfig, Ingest, IngestConfig, IngestParts};
use doewood_model::{
    Backend, Blake3, DocumentText, NodeKind, Payment, PaymentFilter, SpendGroup, format_units,
};
use doewood_s3::{EventKind, EventOrigin, ObjectEvent, ObjectVersion};
use doewood_store::{NewSource, ObjectChange, SeenObject, Standalone, Store};
use tokio::sync::{mpsc, watch};
use uuid::Uuid;

const PG_VAR: &str = "DOEWOOD_TEST_POSTGRES_URL";
const ENDPOINT: &str = "http://objects.test:9000";
const BUCKET: &str = "docs";

/// The backend, and the Postgres schema to drop after.
struct Shared {
    name: &'static str,
    backend: Arc<dyn Backend>,
    cleanup: Option<(tokio_postgres::Client, String)>,
}

impl Shared {
    async fn all(store: &Arc<Store>) -> Vec<Self> {
        let mut out = vec![Self {
            name: "sqlite",
            backend: Arc::new(Standalone::new(Arc::clone(store))),
            cleanup: None,
        }];
        out.extend(Self::postgres().await);
        out
    }

    async fn postgres() -> Option<Self> {
        let Some(url) = std::env::var(PG_VAR).ok().filter(|v| !v.trim().is_empty()) else {
            eprintln!("skipping: {PG_VAR} is not set; the Postgres variant did not run");
            return None;
        };
        let (admin, conn) = tokio_postgres::connect(&url, tokio_postgres::NoTls)
            .await
            .expect("connect to the test database");
        tokio::spawn(conn);
        let schema = format!("dw_pay_{}", Uuid::now_v7().simple());
        admin
            .batch_execute(&format!("CREATE SCHEMA {schema}"))
            .await
            .unwrap();
        let sep = if url.contains('?') { '&' } else { '?' };
        let scoped = format!("{url}{sep}options=-c%20search_path%3D{schema}");
        let url = doewood_core::PostgresUrl::parse(&scoped).unwrap();
        let pg = doewood_pg::PgStore::connect(&url).await.unwrap();
        Some(Self {
            name: "postgres",
            backend: Arc::new(pg),
            cleanup: Some((admin, schema)),
        })
    }

    async fn drop_schema(self) {
        if let Some((admin, schema)) = self.cleanup {
            admin
                .batch_execute(&format!("DROP SCHEMA {schema} CASCADE"))
                .await
                .unwrap();
        }
    }
}

/// One instance's ingest over an in-memory bucket.
struct Harness {
    store: Arc<Store>,
    backend: Arc<dyn Backend>,
    fetch: Arc<MemFetch>,
    source: SourceId,
    events: mpsc::Sender<ObjectEvent>,
    shutdown: watch::Sender<bool>,
    handle: Option<doewood_ingest::IngestHandle>,
}

impl Harness {
    fn start(store: Arc<Store>, backend: Arc<dyn Backend>, ocr_models: &Path) -> Self {
        let source = store
            .add_source(&NewSource::new(ENDPOINT, BUCKET), None)
            .unwrap();
        let fetch = Arc::new(MemFetch::default());
        let mut config = IngestConfig::new(Some(2), ocr_models);
        config.retry_base = Duration::from_millis(10);
        config.sweep_interval = Duration::from_millis(200);
        let analyzer = Arc::new(
            Analyzer::new(&AnalyzerConfig {
                workers: 2,
                ocr_models: ocr_models.to_path_buf(),
                file_timeout: Duration::from_mins(5),
                ..AnalyzerConfig::default()
            })
            .unwrap(),
        );
        let (_tx, backend_rx) = watch::channel(Some(Arc::clone(&backend)));
        let ingest = Ingest::new(IngestParts {
            store: Arc::clone(&store),
            backend: backend_rx,
            fetch: Arc::clone(&fetch) as Arc<dyn doewood_ingest::Fetch>,
            analyzer,
            config,
            on_change: Arc::new(|_| {}),
            on_llm_queued: Arc::new(|_| {}),
        });
        let (events, events_rx) = mpsc::channel(16);
        let (shutdown, shutdown_rx) = watch::channel(false);
        let handle = ingest.start(events_rx, shutdown_rx);
        Self {
            store,
            backend,
            fetch,
            source,
            events,
            shutdown,
            handle: Some(handle),
        }
    }

    async fn put(&self, objects: &[(&str, Vec<u8>)]) {
        let mut changes = Vec::new();
        for (key, bytes) in objects {
            self.fetch.put(key, bytes.clone());
            changes.push(ObjectChange::Upsert(SeenObject {
                key: (*key).to_owned(),
                etag: Some(format!("{:x}", bytes.len())),
                size: bytes.len() as u64,
                last_modified: Some(1),
            }));
        }
        self.store
            .record_objects(self.source, &changes, None)
            .unwrap();
        for (key, bytes) in objects {
            self.events
                .send(ObjectEvent {
                    source: self.source,
                    key: (*key).to_owned(),
                    kind: EventKind::New(ObjectVersion {
                        etag: Some(format!("{:x}", bytes.len())),
                        size: bytes.len() as u64,
                        last_modified: Some(1),
                    }),
                    origin: EventOrigin::Scan,
                })
                .await
                .unwrap();
        }
    }

    async fn settle(&self) {
        let deadline = tokio::time::Instant::now() + Duration::from_mins(10);
        loop {
            let c = self.store.object_counts(self.source).unwrap();
            if c.pending == 0 {
                tokio::time::sleep(Duration::from_millis(100)).await;
                return;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "ingest did not settle: {c:?}"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    fn hash(&self, key: &str) -> Blake3 {
        let st = self.store.ingest_status(self.source, key).unwrap().unwrap();
        assert_eq!(st.status, "done", "{key}: {st:?}");
        Blake3(st.blake3.expect("ingested"))
    }

    async fn payment(&self, key: &str) -> Option<Payment> {
        let h = self.hash(key);
        let all = self
            .backend
            .list_payments(&PaymentFilter::default(), None, 1000, usize::MAX)
            .await
            .unwrap();
        all.into_iter().find(|p| p.row.blake3 == h)
    }

    /// The company node named `name` (its `name` prop).
    async fn company(&self, id: Uuid) -> String {
        let n = self.backend.node(id).await.unwrap().expect("company node");
        assert_eq!(n.kind, NodeKind::Company);
        n.props["name"].as_str().unwrap_or_default().to_owned()
    }

    async fn stop(mut self) {
        self.shutdown.send_replace(true);
        if let Some(h) = self.handle.take() {
            h.join().await;
        }
    }
}

/// Every highlight of the payment quotes its block of the stored document text, at its
/// characters: the block numbering of `GetDocumentText`.
async fn check_highlights(b: &Arc<dyn Backend>, p: &Payment, what: &str) {
    let d = p.row.detail().unwrap();
    assert!(!d.highlights.is_empty(), "{what}: no highlights");
    let stored = b
        .get_document_text(&p.row.blake3)
        .await
        .unwrap()
        .unwrap_or_else(|| panic!("{what}: no document text"));
    let text = DocumentText::from_body(&stored.body).unwrap();
    let mut blocks: Vec<String> = Vec::new();
    text.for_each_block(|v| {
        blocks.push(v.block.text.clone());
        ControlFlow::Continue(())
    });
    for h in &d.highlights {
        let block = blocks
            .get(h.block as usize)
            .unwrap_or_else(|| panic!("{what}: no block {} for {h:?}", h.block));
        let quoted: String = block
            .chars()
            .skip(h.start as usize)
            .take((h.end - h.start) as usize)
            .collect();
        assert_eq!(quoted, h.quote, "{what}: {h:?} in {block:?}");
    }
}

fn contract(pib: &str) -> Vec<String> {
    vec![
        "UGOVOR O PRUZANJU USLUGA ODRZAVANJA SOFTVERA br. 21/2026".into(),
        format!("Izvrsilac: Alfa Soft d.o.o. Beograd, PIB {pib}"),
        "Narucilac: Beta Trgovina d.o.o. Novi Sad".into(),
        "Ugovor je zakljucen dana 14.03.2026. godine.".into(),
        "Clan 1. Predmet ugovora".into(),
        "Predmet ovog ugovora je pruzanje usluga odrzavanja poslovnog softvera Narucioca.".into(),
        "Clan 2. Cena".into(),
        "Ukupna vrednost ugovora iznosi 100.000,00 RSD bez PDV-a.".into(),
        "PDV (20%) iznosi 20.000,00 RSD.".into(),
        "Ukupna vrednost ugovora sa PDV-om iznosi 120.000,00 RSD.".into(),
        "Ugovorna kazna iznosi 50.000,00 RSD.".into(),
    ]
}

fn refs(lines: &[String]) -> Vec<&str> {
    lines.iter().map(String::as_str).collect()
}

/// The corpus: a Serbian contract (PDF), a Serbian monthly licence of the same supplier
/// (DOCX), an English invoice of another supplier (PDF), and minutes without a price (text).
fn corpus() -> Vec<(&'static str, Vec<u8>)> {
    let p1 = common::pib("10000001");
    let licence = [
        "UGOVOR O LICENCI br. 22/2026".to_owned(),
        format!("Dobavljac: Alfa Soft d.o.o. Beograd, PIB {p1}"),
        "Korisnik: Beta Trgovina d.o.o. Novi Sad".to_owned(),
        "Ugovor je zakljucen dana 01.04.2026. godine.".to_owned(),
        "Predmet ugovora je licenca za koriscenje softvera za knjigovodstvo.".to_owned(),
        "Mesecna naknada iznosi 10.000,00 RSD bez PDV-a.".to_owned(),
    ];
    let invoice = [
        "INVOICE No. 2026-0042",
        "Seller: Gamma Consulting LLC",
        "Bill to: Beta Trgovina d.o.o.",
        "Date of issue: 10.05.2026",
        "Description Qty Unit Unit price Amount",
        "Consulting services 10 h 150.00 1,500.00",
        "Travel expenses 1 pcs 300.00 300.00",
        "Subtotal: $1,800.00",
        "Tax (8%): $144.00",
        "Total: $1,944.00",
    ];
    let minutes = "ZAPISNIK SA SASTANKA\n\nPrisutni su razmotrili plan rada za naredni \
                   kvartal i dogovorili sledeci sastanak.\n";
    vec![
        (
            "pay/ugovor.pdf",
            common::pdf(&refs(&contract(&p1)), "Ugovor 21/2026"),
        ),
        (
            "pay/licenca.docx",
            common::docx("Ugovor o licenci", &refs(&licence)),
        ),
        (
            "pay/invoice.pdf",
            common::pdf(&invoice, "Invoice 2026-0042"),
        ),
        ("pay/zapisnik.txt", minutes.as_bytes().to_vec()),
    ]
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[allow(clippy::too_many_lines, clippy::many_single_char_names)]
async fn payments_through_ingest() {
    for kind in ["sqlite", "postgres"] {
        let store = Arc::new(Store::open_in_memory(&MasterKey::from_bytes([3; 32])).unwrap());
        let mut all = Shared::all(&store).await;
        let Some(i) = all.iter().position(|s| s.name == kind) else {
            continue;
        };
        let shared = all.swap_remove(i);
        for s in all {
            s.drop_schema().await;
        }
        let h = Harness::start(
            Arc::clone(&store),
            Arc::clone(&shared.backend),
            &PathBuf::from("/nonexistent/doewood-ocr-models"),
        );
        let files = corpus();
        h.put(&files).await;
        h.settle().await;
        let b = &shared.backend;

        // The contract: gross 120,000.00 RSD with net and VAT, once; the penalty is no price.
        let c = h.payment("pay/ugovor.pdf").await.expect("contract payment");
        let r = &c.row;
        assert_eq!(
            r.amount.map(i128::from).map(format_units).as_deref(),
            Some("120000.00"),
            "{kind}: {r:?}"
        );
        assert_eq!(r.currency.as_deref(), Some("RSD"), "{kind}");
        assert_eq!(r.basis.as_deref(), Some("gross"), "{kind}");
        // No recurring cue: one-off (quail states it on invoices, leaves it out here).
        assert!(
            matches!(r.period.as_deref(), None | Some("one_off")),
            "{kind}: {r:?}"
        );
        assert_eq!(r.category.as_deref(), Some("it_software"), "{kind}");
        assert!(
            r.subject
                .as_deref()
                .is_some_and(|s| s.contains("odrzavanja poslovnog softvera")),
            "{kind}: {r:?}"
        );
        let d = r.detail().unwrap();
        assert_eq!(d.net.as_deref(), Some("100000.00"), "{kind}");
        assert_eq!(d.vat.as_deref(), Some("20000.00"), "{kind}");
        assert_eq!(d.gross.as_deref(), Some("120000.00"), "{kind}");
        assert_eq!(d.method, "rules");
        check_highlights(b, &c, &format!("{kind} contract")).await;
        // Supplier and buyer from the party roles, linked to their companies.
        let alfa = c.supplier.expect("the contract's supplier");
        let beta = c.buyer.expect("the contract's buyer");
        assert!(h.company(alfa).await.starts_with("Alfa Soft"), "{kind}");
        assert!(h.company(beta).await.starts_with("Beta Trgovina"), "{kind}");
        assert!(
            r.supplier_name
                .as_deref()
                .is_some_and(|n| n.starts_with("Alfa Soft")),
            "{kind}"
        );

        // The licence: a monthly fee of the same supplier, never multiplied.
        let l = h
            .payment("pay/licenca.docx")
            .await
            .expect("licence payment");
        assert_eq!(
            l.row.period.as_deref(),
            Some("monthly"),
            "{kind}: {:?}",
            l.row
        );
        assert_eq!(l.row.amount, Some(100_000_000), "{kind}");
        assert_eq!(
            l.supplier,
            Some(alfa),
            "{kind}: one company for both documents"
        );
        check_highlights(b, &l, &format!("{kind} licence")).await;

        // The invoice: line items, another supplier, in USD.
        let inv = h.payment("pay/invoice.pdf").await.expect("invoice payment");
        assert_eq!(inv.row.amount, Some(19_440_000), "{kind}: {:?}", inv.row);
        assert_eq!(inv.row.currency.as_deref(), Some("USD"), "{kind}");
        let lines = inv.row.detail().unwrap().lines;
        assert_eq!(lines.len(), 2, "{kind}: {lines:?}");
        assert_eq!(
            lines[0].description.as_deref(),
            Some("Consulting services"),
            "{kind}"
        );
        assert_eq!(lines[0].total.as_deref(), Some("1500.00"), "{kind}");
        check_highlights(b, &inv, &format!("{kind} invoice")).await;
        let gamma = inv.supplier.expect("the invoice's supplier");
        assert_ne!(gamma, alfa);
        assert!(
            h.company(gamma).await.starts_with("Gamma Consulting"),
            "{kind}"
        );
        assert_eq!(inv.buyer, Some(beta), "{kind}");

        // Minutes without a price: no amount.
        let m = h.payment("pay/zapisnik.txt").await;
        assert!(
            m.as_ref().is_none_or(|m| m.row.amount.is_none()),
            "{kind}: {m:?}"
        );

        // Filters.
        let by = |filter: PaymentFilter| async move {
            b.list_payments(&filter, None, 0, usize::MAX)
                .await
                .unwrap()
                .into_iter()
                .map(|p| p.row.blake3)
                .collect::<Vec<_>>()
        };
        let mut alfa_docs = vec![h.hash("pay/ugovor.pdf"), h.hash("pay/licenca.docx")];
        alfa_docs.sort();
        assert_eq!(
            by(PaymentFilter {
                supplier: Some(alfa),
                ..PaymentFilter::default()
            })
            .await,
            alfa_docs,
            "{kind}"
        );
        assert_eq!(
            by(PaymentFilter {
                currency: Some("USD".into()),
                ..PaymentFilter::default()
            })
            .await,
            [h.hash("pay/invoice.pdf")],
            "{kind}"
        );

        // Spend: the contract's price (no period stated) and the monthly fee apart, per
        // currency.
        let spend = b
            .supplier_spend(Some(alfa), SpendGroup::Category)
            .await
            .unwrap();
        let totals: Vec<_> = spend
            .totals
            .iter()
            .map(|t| {
                (
                    t.currency.as_str(),
                    t.period.as_str(),
                    t.group.as_str(),
                    format_units(t.total),
                )
            })
            .collect();
        assert_eq!(
            totals,
            [
                ("RSD", "", "it_software", "120000.00".to_owned()),
                ("RSD", "monthly", "it_software", "10000.00".to_owned()),
            ],
            "{kind}"
        );
        assert_eq!(spend.provides.len(), 2, "{kind}: {:?}", spend.provides);
        let all = b.supplier_spend(None, SpendGroup::Year).await.unwrap();
        assert!(
            all.totals
                .iter()
                .any(|t| t.supplier == Some(gamma) && t.currency == "USD" && t.total == 19_440_000),
            "{kind}: {:?}",
            all.totals
        );
        assert!(
            all.totals
                .iter()
                .all(|t| t.supplier == Some(gamma) || t.currency != "USD"),
            "{kind}: never mixed"
        );

        h.stop().await;
        shared.drop_schema().await;
    }
}

/// A scanned contract: the contract's page rendered to an image-only PDF, read through OCR.
/// Needs the OCR models (`DOEWOOD_OCR_MODELS`); skipped without them.
#[cfg(feature = "ocr")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_scanned_contract_is_labelled_too() {
    let Some(models) = std::env::var_os("DOEWOOD_OCR_MODELS").map(PathBuf::from) else {
        eprintln!("skipping: DOEWOOD_OCR_MODELS is not set");
        return;
    };
    if !quail_ocr::models::missing(&models).is_empty() {
        eprintln!("skipping: OCR models missing in {}", models.display());
        return;
    }
    let p1 = common::pib("10000001");
    let text_pdf = common::pdf(&refs(&contract(&p1)), "scan source");
    let scan = scan::image_pdf(&text_pdf);
    for kind in ["sqlite", "postgres"] {
        let store = Arc::new(Store::open_in_memory(&MasterKey::from_bytes([4; 32])).unwrap());
        let mut all = Shared::all(&store).await;
        let Some(i) = all.iter().position(|s| s.name == kind) else {
            continue;
        };
        let shared = all.swap_remove(i);
        for s in all {
            s.drop_schema().await;
        }
        let backend = Arc::clone(&shared.backend);
        let h = Harness::start(Arc::clone(&store), Arc::clone(&backend), &models);
        h.put(&[("scan/ugovor.pdf", scan.clone())]).await;
        h.settle().await;
        let p = h
            .payment("scan/ugovor.pdf")
            .await
            .expect("payment of the scan");
        let r = &p.row;
        assert_eq!(r.amount, Some(1_200_000_000), "{kind}: {r:?}");
        assert_eq!(r.currency.as_deref(), Some("RSD"), "{kind}");
        assert_eq!(r.category.as_deref(), Some("it_software"), "{kind}");
        assert!(p.supplier.is_some(), "{kind}: {p:?}");
        // The text came from OCR, and the highlights quote it.
        let stored = backend.get_document_text(&r.blake3).await.unwrap().unwrap();
        let text = DocumentText::from_body(&stored.body).unwrap();
        assert_eq!(
            text.pages[0].origin,
            doewood_model::TextOrigin::Ocr,
            "{kind}"
        );
        check_highlights(&backend, &p, &format!("{kind} scan")).await;
        h.stop().await;
        shared.drop_schema().await;
    }
}

/// Renders the first page of a text PDF into an image-only PDF (8-bit gray, 200 DPI).
#[cfg(feature = "ocr")]
mod scan {
    use lopdf::content::{Content, Operation};
    use lopdf::{Document, Object, Stream, dictionary};
    use quail_core::{Blob, Limits, PageRaster};
    use quail_pdf::PdfRaster;

    const W: f32 = 612.0;
    const H: f32 = 792.0;

    pub fn image_pdf(text_pdf: &[u8]) -> Vec<u8> {
        let img = PdfRaster::new(Blob::from(text_pdf.to_vec()))
            .render(0, 200.0, &Limits::default())
            .expect("render the text page");
        assert_eq!(img.channels, 1);
        let mut doc = Document::with_version("1.7");
        let pages_id = doc.new_object_id();
        let mut stream = Stream::new(
            dictionary! {
                "Type" => "XObject",
                "Subtype" => "Image",
                "Width" => i64::from(img.width),
                "Height" => i64::from(img.height),
                "ColorSpace" => "DeviceGray",
                "BitsPerComponent" => 8,
            },
            img.data,
        );
        stream.compress().expect("deflate the scan");
        let image_id = doc.add_object(stream);
        let ops = vec![
            Operation::new("q", vec![]),
            Operation::new(
                "cm",
                vec![W.into(), 0.into(), 0.into(), H.into(), 0.into(), 0.into()],
            ),
            Operation::new("Do", vec!["Im1".into()]),
            Operation::new("Q", vec![]),
        ];
        let content = Content { operations: ops };
        let content_id = doc.add_object(Stream::new(dictionary! {}, content.encode().unwrap()));
        let leaf = doc.add_object(dictionary! {
            "Type" => "Page",
            "Parent" => pages_id,
            "Contents" => content_id,
            "Resources" => dictionary! { "XObject" => dictionary! { "Im1" => image_id } },
        });
        doc.objects.insert(
            pages_id,
            Object::Dictionary(dictionary! {
                "Type" => "Pages",
                "Kids" => vec![leaf.into()],
                "Count" => 1,
                "MediaBox" => vec![0.into(), 0.into(), W.into(), H.into()],
            }),
        );
        let catalog_id = doc.add_object(dictionary! { "Type" => "Catalog", "Pages" => pages_id });
        doc.trailer.set("Root", catalog_id);
        let mut out = Vec::new();
        doc.save_to(&mut out).unwrap();
        out
    }
}
