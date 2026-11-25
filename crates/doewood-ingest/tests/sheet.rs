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

/// The cell cap is what one worker's plan holds at the measured bytes a cell.
#[test]
fn the_cell_cap_fits_a_worker() {
    assert_eq!(sheet::MAX_CELLS, 1_600_000);
    const { assert!(sheet::MAX_CELLS * sheet::CELL_BYTES <= doewood_ingest::pipeline::WORKER_BYTES) };
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
            "{}: file refused: the workbook has more than 5 cells (limit 5)",
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
    let rows = usize::try_from(sheet::MAX_CELLS).unwrap() / cols + 1;
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
         1,600,000)"
    );
    assert_eq!(a.body["status"]["state"], "failed");
    assert_eq!(a.body["format"], "xlsx");
    let facts = a.body["facts"].as_array().unwrap();
    assert!(facts.iter().any(|f| f["key"] == "hash.blake3"), "{facts:?}");
    assert!(!facts.iter().any(|f| f["key"] == "sheet.cells"));
}
