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
