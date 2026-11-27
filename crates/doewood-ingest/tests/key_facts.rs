//! Key facts (issue #194) through ingest: generated Serbian and English contracts go through
//! the pipeline, and each document's key-facts card, its highlights against the stored
//! document text, the fact search (`find_documents`, every filter) and the search ranking of
//! clause words are checked. On SQLite and, when `DOEWOOD_TEST_POSTGRES_URL` is set, on
//! Postgres in a schema of its own.

mod common;

use std::ops::ControlFlow;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use common::MemFetch;
use doewood_core::{MasterKey, SourceId};
use doewood_ingest::{Analyzer, AnalyzerConfig, Ingest, IngestConfig, IngestParts};
use doewood_model::{
    Backend, Blake3, Card, DocumentText, FactFilter, FactRow, NodeKind, SearchQuery, parse_day,
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
        let schema = format!("dw_kf_{}", Uuid::now_v7().simple());
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

/// The blocks of the stored document text of `h`, by `TextBlock.index`.
async fn blocks(b: &Arc<dyn Backend>, h: &Blake3, what: &str) -> Vec<String> {
    let stored = b
        .get_document_text(h)
        .await
        .unwrap()
        .unwrap_or_else(|| panic!("{what}: no document text"));
    let text = DocumentText::from_body(&stored.body).unwrap();
    let mut out = Vec::new();
    text.for_each_block(|v| {
        out.push(v.block.text.clone());
        ControlFlow::Continue(())
    });
    out
}

fn chars(s: &str, start: u32, end: u32) -> String {
    s.chars()
        .skip(start as usize)
        .take((end - start) as usize)
        .collect()
}

/// Every highlight of the row lies in its block of the stored text, and every card field's
/// highlight quotes that block at its characters.
async fn check_highlights(b: &Arc<dyn Backend>, row: &FactRow, what: &str) -> Card {
    let blocks = blocks(b, &row.columns.blake3, what).await;
    let hs = row.highlights().unwrap();
    assert!(!hs.is_empty(), "{what}: no highlights");
    for h in &hs {
        let block = blocks
            .get(h.block as usize)
            .unwrap_or_else(|| panic!("{what}: no block {} for {h:?}", h.block));
        assert!(
            h.start < h.end && h.end as usize <= block.chars().count(),
            "{what}: {h:?} in {block:?}"
        );
    }
    let card = row.card().unwrap();
    let mut quoted = 0;
    for (field, ev) in &card.evidence {
        for h in &ev.highlights {
            let block = &blocks[h.block as usize];
            assert_eq!(
                chars(block, h.start, h.end),
                h.quote,
                "{what} {field}: {h:?}"
            );
            assert!(
                hs.iter().any(|x| x.key == format!("card.{field}")
                    && (x.block, x.start, x.end) == (h.block, h.start, h.end)),
                "{what} {field}: {h:?} not among the block highlights"
            );
            quoted += 1;
        }
    }
    assert!(quoted > 3, "{what}: {quoted} card highlights");
    card
}

/// A Serbian (Latin) service contract with the facts quail reads (as quail's tests write
/// them), and an end date.
const SR_CONTRACT: &[&str] = &[
    "Broj ugovora: 114/2026",
    "Zaključen dana 15.01.2026. godine između:",
    "1. Alfa Soft d.o.o., sa sedištem u Beogradu, ul. Knez Mihailova 10, PIB 100000024, MB 07000012, tekući račun 160-0000000123456-78, koga zastupa direktor Petar Petrović (u daljem tekstu: Izvođač)",
    "i",
    "2. Beta Trgovina d.o.o., sa sedištem u Novom Sadu, Bulevar oslobođenja 5, PIB 100000032, MB 07000020, koga zastupa generalni direktor Marko Marković (u daljem tekstu: Naručilac)",
    "Član 1. Predmet ugovora",
    "Predmet ovog ugovora je održavanje poslovnog softvera Naručioca.",
    "Član 2. Cena i plaćanje",
    "Ukupna vrednost ugovora iznosi 1.200.000,00 RSD bez PDV-a.",
    "Naručilac se obavezuje da plati fakturu u roku od 15 dana od dana prijema fakture, virmanski na tekući račun Izvođača.",
    "Član 3. Trajanje",
    "Ugovor važi do 31.12.2026. godine i automatski se produžava za isti period.",
    "Svaka strana može otkazati ugovor uz otkazni rok od 30 dana.",
    "Član 4. Obaveze",
    "Ugovorna kazna iznosi 0,5% za svaki dan kašnjenja, a najviše 10% ugovorene vrednosti.",
    "Izvođač dostavlja bankarsku garanciju za dobro izvršenje posla u visini od 10% vrednosti ugovora.",
    "Član 5. Završne odredbe",
    "Na ovaj ugovor primenjuje se pravo Republike Srbije.",
    "Za sve sporove nadležan je Privredni sud u Beogradu.",
    "Za Izvođača",
    "Petar Petrović, direktor",
    "Za Naručioca",
    "Marko Marković",
];

/// An English agreement (as quail's tests write it), without a penalty or a stated end.
const EN_AGREEMENT: &[&str] = &[
    "Agreement No. MSA-2026-07",
    "This Agreement is made on March 1, 2026 between Alpha Widgets LLC, with its registered office at 1 Main Street, Dover, Delaware (the \"Supplier\"), represented by John Smith, Chief Executive Officer, and Beta Components GmbH, Registration No. HRB 12345, VAT No. DE136695976 (the \"Customer\").",
    "1. Term",
    "This Agreement shall remain in force for a period of two (2) years from the Effective Date.",
    "Either party may terminate this Agreement for convenience upon ninety (90) days' prior written notice.",
    "2. Payment",
    "The Customer shall pay each invoice within thirty (30) days of receipt of the invoice by bank transfer to IBAN DE89 3704 0044 0532 0130 00.",
    "The total price is EUR 50,000.00 excluding VAT for consulting services.",
    "3. Governing Law",
    "This Agreement is governed by the laws of England and Wales. The courts of London shall have exclusive jurisdiction over any dispute.",
    "For the Supplier",
    "Name: John Smith",
    "For the Customer",
    "Anna Weber, Managing Director",
];

/// A Serbian letter that talks about notice periods and penalties without stating any.
const LETTER: &str = "DOPIS\n\nPoštovani, pitali ste koliki je otkazni rok i da li postoji \
                      penalty. Otkazni rok i penalty nisu predmet ovog dopisa, a otkazni rok \
                      ćemo razmotriti na sledećem sastanku.\n";

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[allow(clippy::too_many_lines, clippy::many_single_char_names)]
async fn key_facts_through_ingest() {
    for kind in ["sqlite", "postgres"] {
        let store = Arc::new(Store::open_in_memory(&MasterKey::from_bytes([5; 32])).unwrap());
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
        h.put(&[
            (
                "kf/ugovor.docx",
                common::docx("UGOVOR O ODRŽAVANJU SOFTVERA", SR_CONTRACT),
            ),
            (
                "kf/agreement.docx",
                common::docx("MASTER SERVICES AGREEMENT", EN_AGREEMENT),
            ),
            ("kf/dopis.txt", LETTER.as_bytes().to_vec()),
        ])
        .await;
        // Other documents, so the clause words are as rare as in a real corpus (BM25 gives
        // a word in most documents no weight).
        let memos: Vec<(String, Vec<u8>)> = (0..6)
            .map(|i| {
                (
                    format!("kf/memo{i}.txt"),
                    format!("BELESKA {i}\n\nSastanak tima broj {i} o planu rada za kvartal.\n")
                        .into_bytes(),
                )
            })
            .collect();
        h.put(
            &memos
                .iter()
                .map(|(k, v)| (k.as_str(), v.clone()))
                .collect::<Vec<_>>(),
        )
        .await;
        h.settle().await;
        let b = &shared.backend;
        let facts = |key: &'static str| {
            let hash = h.hash(key);
            async move {
                b.get_document_facts(&hash)
                    .await
                    .unwrap()
                    .unwrap_or_else(|| panic!("{kind}: no key facts for {key}"))
            }
        };

        // The Serbian contract: parties, dates, renewal and notice, price, penalty, guarantee,
        // law and court, each highlighted where the text states it.
        let sr = facts("kf/ugovor.docx").await;
        let c = check_highlights(b, &sr, &format!("{kind} ugovor")).await;
        eprintln!("{kind} ugovor card: {}", sr.card.get());
        assert_eq!(c.kind.as_deref(), Some("contract"), "{kind}");
        assert_eq!(c.number.as_deref(), Some("114/2026"), "{kind}");
        let sup = c.supplier.as_ref().expect("supplier");
        assert_eq!(sup.name.as_deref(), Some("Alfa Soft d.o.o."), "{kind}");
        assert_eq!(sup.pib.as_deref(), Some("100000024"), "{kind}");
        assert_eq!(
            c.customer.as_ref().and_then(|p| p.name.as_deref()),
            Some("Beta Trgovina d.o.o."),
            "{kind}"
        );
        assert_eq!(c.signed.as_deref(), Some("2026-01-15"), "{kind}");
        assert_eq!(c.end.as_deref(), Some("2026-12-31"), "{kind}");
        assert_eq!(c.auto_renewal, Some(true), "{kind}");
        assert_eq!(c.notice.as_ref().and_then(|n| n.days), Some(30), "{kind}");
        assert_eq!(c.notice_due.as_deref(), Some("2026-12-01"), "{kind}");
        assert_eq!(c.payment_days, Some(15), "{kind}");
        assert_eq!(
            (c.amount.as_deref(), c.currency.as_deref()),
            (Some("1200000.00"), Some("RSD")),
            "{kind}"
        );
        assert_eq!(c.category.as_deref(), Some("it_software"), "{kind}");
        assert_eq!(
            c.penalty.as_ref().and_then(|p| p.per.as_deref()),
            Some("day"),
            "{kind}"
        );
        assert_eq!(c.guarantees, ["bank_guarantee"], "{kind}");
        assert_eq!(c.governing_law.as_deref(), Some("RS"), "{kind}");
        assert_eq!(
            c.court.as_deref(),
            Some("Privredni sud u Beogradu"),
            "{kind}"
        );
        // The notice clause, highlighted for the card and for the contract fact.
        let notice = &c.evidence["notice"].highlights[0];
        assert!(notice.quote.contains("30 dana"), "{kind}: {notice:?}");
        let hs = sr.highlights().unwrap();
        for key in ["card.notice", "contract.notice", "payment.amount"] {
            assert!(
                hs.iter().any(|x| x.key == key),
                "{kind}: no {key} in {hs:?}"
            );
        }

        // The English agreement: an end derived from the signing date and the term.
        let en = facts("kf/agreement.docx").await;
        let e = check_highlights(b, &en, &format!("{kind} agreement")).await;
        eprintln!("{kind} agreement card: {}", en.card.get());
        assert_eq!(
            e.supplier.as_ref().and_then(|p| p.name.as_deref()),
            Some("Alpha Widgets LLC"),
            "{kind}"
        );
        assert_eq!(e.signed.as_deref(), Some("2026-03-01"), "{kind}");
        assert_eq!(e.term.as_ref().and_then(|t| t.months), Some(24), "{kind}");
        assert_eq!(e.ends.as_deref(), Some("2028-03-01"), "{kind}");
        assert_eq!(e.notice.as_ref().and_then(|n| n.days), Some(90), "{kind}");
        assert_eq!(e.notice_due.as_deref(), Some("2027-12-02"), "{kind}");
        // England and Wales: ISO 3166-2, which the `GB` filter finds.
        assert_eq!(e.governing_law.as_deref(), Some("GB-EAW"), "{kind}");
        assert_eq!(e.kind.as_deref(), Some("msa"), "{kind}");
        assert_eq!(e.penalty, None, "{kind}");
        assert!(e.guarantees.is_empty(), "{kind}");

        // Fact search: every filter, over the three documents.
        let (ugovor, agreement, dopis) = (
            h.hash("kf/ugovor.docx"),
            h.hash("kf/agreement.docx"),
            h.hash("kf/dopis.txt"),
        );
        let alfa = b
            .find_documents(&FactFilter::default(), None, 0)
            .await
            .unwrap()
            .into_iter()
            .find(|d| d.columns.blake3 == ugovor)
            .and_then(|d| d.supplier)
            .expect("the contract's supplier company");
        assert!(h.company(alfa).await.starts_with("Alfa Soft"), "{kind}");
        let day = |s: &str| parse_day(s);
        let f = FactFilter::default;
        let cases: Vec<(FactFilter, Vec<Blake3>)> = vec![
            (
                FactFilter {
                    kind: Some("contract".into()),
                    ..f()
                },
                vec![ugovor],
            ),
            (
                FactFilter {
                    kind: Some("msa".into()),
                    ..f()
                },
                vec![agreement],
            ),
            (
                FactFilter {
                    supplier: Some(alfa),
                    ..f()
                },
                vec![ugovor],
            ),
            (
                FactFilter {
                    supplier_name: Some("alpha widgets".into()),
                    ..f()
                },
                vec![agreement],
            ),
            (
                FactFilter {
                    customer_name: Some("beta".into()),
                    ..f()
                },
                vec![ugovor, agreement],
            ),
            (
                FactFilter {
                    category: Some("it_software".into()),
                    ..f()
                },
                vec![ugovor],
            ),
            (
                FactFilter {
                    currency: Some("RSD".into()),
                    amount_min: Some(1_000_000 * 10_000),
                    amount_max: Some(2_000_000 * 10_000),
                    ..f()
                },
                vec![ugovor],
            ),
            (
                FactFilter {
                    end_before: day("2027-01-01"),
                    ..f()
                },
                vec![ugovor],
            ),
            (
                FactFilter {
                    end_after: day("2027-01-01"),
                    ..f()
                },
                vec![agreement],
            ),
            (
                FactFilter {
                    notice_due_before: day("2026-12-31"),
                    ..f()
                },
                vec![ugovor],
            ),
            (
                FactFilter {
                    notice_due_after: day("2027-06-01"),
                    ..f()
                },
                vec![agreement],
            ),
            (
                FactFilter {
                    auto_renewal: Some(true),
                    ..f()
                },
                vec![ugovor],
            ),
            (
                FactFilter {
                    has_penalty: Some(true),
                    ..f()
                },
                vec![ugovor],
            ),
            (
                FactFilter {
                    has_guarantee: Some(true),
                    ..f()
                },
                vec![ugovor],
            ),
            (
                FactFilter {
                    governing_law: Some("GB".into()),
                    ..f()
                },
                vec![agreement],
            ),
            (
                FactFilter {
                    court: Some("sud u beogradu".into()),
                    ..f()
                },
                vec![ugovor],
            ),
            (
                FactFilter {
                    signed_from: day("2026-02-01"),
                    signed_to: day("2026-12-31"),
                    ..f()
                },
                vec![agreement],
            ),
        ];
        for (filter, want) in cases {
            let mut want = want;
            want.sort();
            let got: Vec<Blake3> = b
                .find_documents(&filter, None, 0)
                .await
                .unwrap()
                .into_iter()
                .map(|d| d.columns.blake3)
                .collect();
            assert_eq!(got, want, "{kind}: {filter:?}");
        }

        // Ranking: the clause words find the contracts first, the letter that only talks
        // about them after.
        for words in ["otkazni rok", "penalty"] {
            let hits: Vec<Blake3> = b
                .search(&SearchQuery {
                    text: Some(words.to_owned()),
                    ..SearchQuery::default()
                })
                .await
                .unwrap()
                .into_iter()
                .map(|hit| hit.blake3)
                .collect();
            assert_eq!(hits.first(), Some(&ugovor), "{kind} {words:?}: {hits:?}");
            if let Some(at) = hits.iter().position(|x| *x == dopis) {
                assert!(at > 0, "{kind} {words:?}: {hits:?}");
            }
        }

        h.stop().await;
        shared.drop_schema().await;
    }
}
