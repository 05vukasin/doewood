//! hedgehog under the file's deadline.

mod common;

use std::time::{Duration, Instant};

use doewood_ingest::sheet;

fn workbook() -> Vec<u8> {
    common::xlsx(
        "Prices",
        &[&["Item", "Qty"], &["Bolts", "10"], &["Nuts", "20"]],
    )
}

/// A read that starts past the deadline fails the workbook with hedgehog's time limit, and
/// does not panic.
#[test]
fn a_read_past_the_deadline_fails_the_workbook() {
    let out = sheet::analyze(&workbook(), "p.xlsx", "xlsx", Some(Instant::now()));
    let failed = out.failed.expect("the read fails");
    assert!(
        failed.starts_with(&format!("{}: ", sheet::READ_ID)),
        "{failed}"
    );
    assert!(failed.contains("time limit"), "{failed}");
    assert!(out.facts.is_empty());
}

/// Within the deadline nothing changes.
#[test]
fn a_read_within_the_deadline_reads() {
    let deadline = Instant::now().checked_add(Duration::from_mins(1));
    let out = sheet::analyze(&workbook(), "p.xlsx", "xlsx", deadline);
    assert_eq!(out.failed, None);
    assert!(out.steps.contains(&sheet::READ_ID));
    assert!(out.steps.contains(&sheet::TABLES_ID));
}

/// The smallest cell cap is what one worker's plan holds at the measured bytes a cell; the cap
/// grows with the memory per worker, up to hedgehog's default.
#[test]
fn the_cell_cap_fits_a_worker() {
    assert_eq!(sheet::MIN_AUTO_CELLS, 1_600_000);
    const {
        assert!(
            sheet::MIN_AUTO_CELLS * sheet::CELL_BYTES <= doewood_ingest::pipeline::WORKER_BYTES
        );
    };
    assert_eq!(sheet::cells_for(512 << 20), 1_600_000);
    assert_eq!(sheet::cells_for(2 << 30), 6_700_000);
    assert_eq!(sheet::cells_for(0), 1_600_000);
    assert_eq!(sheet::cells_for(64 << 30), 10_000_000);
    assert_eq!(sheet::MAX_AUTO_CELLS, 10_000_000);
}

/// A workbook over the cap fails with hedgehog's limit error, without a panic; one at the cap
/// reads.
#[test]
fn a_workbook_over_the_cell_cap_fails() {
    let deadline = Instant::now().checked_add(Duration::from_mins(1));
    // 6 cells.
    let out = sheet::analyze_with(&workbook(), "p.xlsx", "xlsx", deadline, 5);
    let failed = out.failed.expect("the read fails");
    assert_eq!(
        failed,
        format!(
            "{}: file refused: the workbook has more than 5 cells (limit 5); \
             DOEWOOD_INGEST_MAX_CELLS raises the cap (it follows the memory per ingest worker)",
            sheet::READ_ID
        )
    );
    assert!(out.facts.is_empty());
    let out = sheet::analyze_with(&workbook(), "p.xlsx", "xlsx", deadline, 6);
    assert_eq!(out.failed, None);
}

/// Through the analyzer, with the default cap: the file fails like any refused file, and its
/// record keeps quail's identity facts.
#[test]
fn the_analyzer_refuses_a_workbook_over_the_default_cap() {
    use doewood_ingest::{Analyzer, AnalyzerConfig, Outcome};

    let cols = 26;
    let rows = usize::try_from(sheet::MIN_AUTO_CELLS).unwrap() / cols + 1;
    let analyzer = Analyzer::new(&AnalyzerConfig {
        ocr_models: "/nonexistent/doewood-ocr-models".into(),
        ..AnalyzerConfig::default()
    })
    .unwrap();
    let a = analyzer.analyze(common::big_xlsx(rows, cols, 67), "big.xlsx");
    let Outcome::Failed(error) = &a.outcome else {
        panic!("{:?}", a.outcome);
    };
    assert_eq!(
        error,
        "sheet.read@1: file refused: the workbook has more than 1,600,000 cells (limit \
         1,600,000); DOEWOOD_INGEST_MAX_CELLS raises the cap (it follows the memory per ingest \
         worker)"
    );
    let body: serde_json::Value = serde_json::from_str(a.body.get()).unwrap();
    assert_eq!(body["status"]["state"], "failed");
    assert_eq!(body["format"], "xlsx");
    let facts = body["facts"].as_array().unwrap();
    assert!(facts.iter().any(|f| f["key"] == "hash.blake3"), "{facts:?}");
    assert!(!facts.iter().any(|f| f["key"] == "sheet.cells"));
}

/// A large generated workbook (260,000 cells, every other column text, so hedgehog's inner
/// fan-outs start) gives the same facts on one thread and on lent ones, and hedgehog's own
/// result is equal too. The loan is given back.
#[test]
fn lent_threads_give_the_same_tables() {
    use doewood_ingest::cpus::{CpuSlots, MAX_SHEET_THREADS};

    let bytes = common::big_xlsx_with(10_000, 26, 101, true);
    let deadline = Instant::now().checked_add(Duration::from_mins(5));
    let one = sheet::analyze_with(&bytes, "big.xlsx", "xlsx", deadline, sheet::MIN_AUTO_CELLS);
    let cpus = CpuSlots::new(MAX_SHEET_THREADS);
    let own = cpus.enter();
    let lent = sheet::analyze_on(
        &bytes,
        "big.xlsx",
        "xlsx",
        deadline,
        sheet::MIN_AUTO_CELLS,
        Some(&cpus),
    );
    assert_eq!(cpus.in_use(), 1, "the loan is back");
    drop(own);
    assert_eq!((one.threads, lent.threads), (1, MAX_SHEET_THREADS));
    assert_eq!(one.failed, None);
    assert_eq!(one.problems, lent.problems);
    assert_eq!(one.steps, lent.steps);
    assert_eq!(one.facts, lent.facts);
    assert!(
        one.facts
            .iter()
            .any(|f| f.key == sheet::keys::TABLE && f.src == sheet::TABLES_ID)
    );

    let wb = hh_core::reader::open_bytes_with(
        &bytes,
        Some("big.xlsx"),
        hh_core::reader::ReadOptions::default(),
    )
    .unwrap();
    let norm = |threads| {
        let opts = hh_core::normalize::Options {
            threads: Some(threads),
            ..hh_core::normalize::Options::default()
        };
        hh_core::try_normalize_workbook(&wb, &opts).unwrap()
    };
    assert_eq!(norm(1), norm(MAX_SHEET_THREADS));
}

/// An analysis gives its slot (and any loan) back however it ends: read, refused, timed out.
#[test]
fn the_analyzer_gives_its_slots_back() {
    use doewood_ingest::{Analyzer, AnalyzerConfig};

    for timeout in [Duration::from_mins(1), Duration::from_nanos(1)] {
        let analyzer = Analyzer::new(&AnalyzerConfig {
            cpus: 4,
            file_timeout: timeout,
            ocr_models: "/nonexistent/doewood-ocr-models".into(),
            ..AnalyzerConfig::default()
        })
        .unwrap();
        assert_eq!(analyzer.cpus().total(), 4);
        for bytes in [
            workbook(),
            common::big_xlsx(2_000, 26, 5),
            b"PK\x03\x04x".to_vec(),
        ] {
            analyzer.analyze(bytes, "x.xlsx");
            assert_eq!(analyzer.cpus().in_use(), 0, "timeout {timeout:?}");
        }
    }
}

/// `cargo test -p doewood-ingest --test sheet -- --ignored --nocapture lent_threads_measure`.
/// `DOEWOOD_BENCH_FILES` generated workbooks (default 1) of `DOEWOOD_BENCH_ROWS` x 26 numbers
/// (default 38,462: 1,000,012 cells; every other one text with `DOEWOOD_BENCH_TEXT=1`), analyzed at once by as many threads, each holding a slot
/// of `DOEWOOD_BENCH_CPUS` (default: the CPUs; 1 is the old single-threaded normalizer). Peak
/// RSS and OS threads are sampled every millisecond.
#[test]
#[ignore = "a measurement, not a check"]
fn lent_threads_measure() {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

    use doewood_ingest::cpus::{self, CpuSlots};

    fn status(field: &str) -> u64 {
        let status = std::fs::read_to_string("/proc/self/status").unwrap_or_default();
        status
            .lines()
            .find(|l| l.starts_with(field))
            .and_then(|l| l.split_whitespace().nth(1)?.parse().ok())
            .unwrap_or(0)
    }
    fn env(var: &str, default: usize) -> usize {
        std::env::var(var)
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(default)
    }

    let n = env("DOEWOOD_BENCH_FILES", 1);
    let rows = env("DOEWOOD_BENCH_ROWS", 38_462);
    let text = env("DOEWOOD_BENCH_TEXT", 0) == 1;
    let slots = CpuSlots::new(env("DOEWOOD_BENCH_CPUS", cpus::available()));
    let files: Vec<Vec<u8>> = (0..n as u64)
        .map(|i| common::big_xlsx_with(rows, 26, 9000 + i, text))
        .collect();
    let base = status("VmRSS:");
    let (peak, threads, stop) = (
        Arc::new(AtomicU64::new(base)),
        Arc::new(AtomicU64::new(0)),
        Arc::new(AtomicBool::new(false)),
    );
    let sampler = {
        let (peak, threads, stop) = (Arc::clone(&peak), Arc::clone(&threads), Arc::clone(&stop));
        std::thread::spawn(move || {
            while !stop.load(Ordering::Relaxed) {
                peak.fetch_max(status("VmRSS:"), Ordering::Relaxed);
                threads.fetch_max(status("Threads:"), Ordering::Relaxed);
                std::thread::sleep(Duration::from_millis(1));
            }
        })
    };
    let start = Instant::now();
    let used: Vec<usize> = std::thread::scope(|s| {
        let handles: Vec<_> = files
            .iter()
            .map(|bytes| {
                let slots = &slots;
                s.spawn(move || {
                    let _own = slots.enter();
                    let deadline = Instant::now().checked_add(Duration::from_mins(5));
                    let out = sheet::analyze_on(
                        bytes,
                        "big.xlsx",
                        "xlsx",
                        deadline,
                        sheet::MIN_AUTO_CELLS,
                        Some(slots),
                    );
                    assert_eq!(out.failed, None);
                    assert!(out.problems.is_empty(), "{:?}", out.problems);
                    out.threads
                })
            })
            .collect();
        handles.into_iter().map(|h| h.join().unwrap()).collect()
    });
    let secs = start.elapsed().as_secs_f64();
    stop.store(true, Ordering::Relaxed);
    sampler.join().unwrap();
    #[allow(clippy::cast_precision_loss)]
    let mib = |kib: u64| kib as f64 / 1024.0;
    let peak = peak.load(Ordering::Relaxed);
    eprintln!(
        "lent_threads_measure: {n} x {} cells (text {text}), {} slots, threads per file {used:?}, {secs:.2}s; \
         peak OS threads {}; RSS before {:.0} MiB, peak {:.0} MiB (+{:.0} MiB)",
        rows * 26,
        slots.total(),
        threads.load(Ordering::Relaxed),
        mib(base),
        mib(peak),
        mib(peak.saturating_sub(base)),
    );
    assert_eq!(slots.in_use(), 0);
}
