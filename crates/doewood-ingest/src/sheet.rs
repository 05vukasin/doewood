//! Spreadsheets with hedgehog: the workbook read from memory (`hh_core::open_bytes_with`),
//! its tables found (`hh_core::try_normalize_workbook`), and both turned into quail-shaped
//! facts in the `sheet.*` namespace, so a spreadsheet's record has the same top-level shape
//! as a document's.
//!
//! Every hedgehog call runs under [`std::panic::catch_unwind`]: calamine's XLSB reader has
//! known panics on hostile files, and a panic must end only that file.

use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Arc;
use std::time::Instant;

use hh_core::normalize::Options as NormalizeOptions;
use hh_core::reader::ReadOptions;
use hh_core::{Value as Cell, Workbook};
use quail_core::{AnalyzerId, Fact, FactKey, Value};

use crate::cpus::{CpuSlots, MAX_SHEET_THREADS};

/// The step that reads the workbook. Its version follows doewood's conversion; hedgehog's own
/// revision is in [`HEDGEHOG_REV`].
pub const READ_ID: AnalyzerId = AnalyzerId::new("sheet.read", 1);
/// The step that finds the tables.
pub const TABLES_ID: AnalyzerId = AnalyzerId::new("sheet.tables", 1);
/// The hedgehog revision doewood is built with (a step version for reprocessing).
pub const HEDGEHOG_REV: &str = "1a9811f";

/// Fact keys of the `sheet.*` namespace.
pub mod keys {
    use quail_core::FactKey;

    /// The spreadsheet format: `xlsx`, `xlsb`, `xls`, `ods` (Text).
    pub const FORMAT: FactKey = FactKey::new("sheet.format");
    /// Sheets in the workbook (Int).
    pub const SHEETS: FactKey = FactKey::new("sheet.count");
    /// One sheet (Record: `name`, `range`?, `cells`, `formulas`, `merges`).
    pub const SHEET: FactKey = FactKey::new("sheet.sheet");
    /// Non-empty cells in the workbook (Int).
    pub const CELLS: FactKey = FactKey::new("sheet.cells");
    /// Formula cells in the workbook (Int).
    pub const FORMULAS: FactKey = FactKey::new("sheet.formulas");
    /// Tables found (Int).
    pub const TABLES: FactKey = FactKey::new("sheet.tables");
    /// One table (Record: `id`, `sheet`, `title`?, `columns` (List of Text), `rows`, `conf`).
    pub const TABLE: FactKey = FactKey::new("sheet.table");
    /// The first text cells, for search (Text, bounded).
    pub const TEXT: FactKey = FactKey::new("sheet.text");
}

/// Most text kept in [`keys::TEXT`], in bytes.
const MAX_TEXT: usize = 16 * 1024;
/// Most tables described one by one.
const MAX_TABLES: usize = 50;
/// Most columns named per table.
const MAX_COLUMNS: usize = 64;
/// Most sheets described one by one.
const MAX_SHEETS: usize = 200;

/// Peak memory of one workbook in ingest, per cell, rounded up: the file, hedgehog's
/// workbook and its normalizer. Measured with `big_spreadsheets` (`tests/pipeline.rs`): 200 to
/// 300 bytes a cell for numbers and text, file included; 380 MiB for 960k cells in #63.
pub const CELL_BYTES: u64 = 320;
/// The smallest automatic cell cap: what one worker's plan
/// ([`WORKER_BYTES`](crate::pipeline::WORKER_BYTES), 512 MiB) holds, by [`cells_for`]:
/// 1,600,000.
pub const MIN_AUTO_CELLS: u64 =
    crate::pipeline::WORKER_BYTES / CELL_BYTES / CELLS_STEP * CELLS_STEP;
/// The largest automatic cell cap: hedgehog's own default (10,000,000). Only
/// `DOEWOOD_INGEST_MAX_CELLS` goes above it.
pub const MAX_AUTO_CELLS: u64 = hh_core::reader::DEFAULT_MAX_CELLS;
/// Automatic cell caps are whole multiples of this.
const CELLS_STEP: u64 = 100_000;
/// The bootstrap variable that sets the cell cap, named in the refusal.
const MAX_CELLS_VAR: &str = doewood_core::config::vars::INGEST_MAX_CELLS;

/// The cell cap (hedgehog's `max_cells`, and `max_shared_strings`) for `worker_bytes` of
/// memory per worker: what they hold at [`CELL_BYTES`] a cell, rounded down to 100,000, from
/// [`MIN_AUTO_CELLS`] to [`MAX_AUTO_CELLS`]. 512 MiB: 1,600,000; 2 GiB: 6,700,000.
pub const fn cells_for(worker_bytes: u64) -> u64 {
    let cells = worker_bytes / CELL_BYTES / CELLS_STEP * CELLS_STEP;
    if cells < MIN_AUTO_CELLS {
        MIN_AUTO_CELLS
    } else if cells > MAX_AUTO_CELLS {
        MAX_AUTO_CELLS
    } else {
        cells
    }
}

/// Whether hedgehog refused a workbook over the cell cap (its cells or its shared strings).
fn over_cell_cap(error: &str) -> bool {
    error.contains(" cells (limit ") || error.contains(" shared strings (limit ")
}

/// What hedgehog made of a workbook.
#[derive(Debug, Default)]
pub struct SheetOutcome {
    /// The facts, in production order.
    pub facts: Vec<Fact>,
    /// Steps that ran.
    pub steps: Vec<AnalyzerId>,
    /// Problems (a step failed or panicked); the facts present are still valid.
    pub problems: Vec<String>,
    /// The workbook could not be read at all.
    pub failed: Option<String>,
    /// The threads hedgehog's normalizer ran with (0: it did not run).
    pub threads: usize,
}

/// Reads and analyzes a workbook, stopping at `deadline` (the file's): hedgehog's read checks
/// it per sheet and every 65,536 cells, its normalizer per sheet. A read past it fails the
/// workbook (`sheet.read@1: file refused: ...time limit`); tables past it are a problem
/// (`sheet.tables@1: ...`). At most [`MIN_AUTO_CELLS`] cells are read.
pub fn analyze(bytes: &[u8], name: &str, format: &str, deadline: Option<Instant>) -> SheetOutcome {
    analyze_with(bytes, name, format, deadline, MIN_AUTO_CELLS)
}

/// [`analyze`] with another cell cap: a workbook of more than `max_cells` cells (or shared
/// strings) fails like any other refused file, with its identity facts kept
/// (`sheet.read@1: file refused: the workbook has more than ... cells (limit ...); ...`, which
/// says how to raise the cap). The reader and the normalizer run on one thread.
pub fn analyze_with(
    bytes: &[u8],
    name: &str,
    format: &str,
    deadline: Option<Instant>,
    max_cells: u64,
) -> SheetOutcome {
    analyze_on(bytes, name, format, deadline, max_cells, None)
}

/// [`analyze_with`], with the reader and then the normalizer each borrowing the free slots of
/// `cpus` (at most [`MAX_SHEET_THREADS`] threads in all) for the length of its call. The caller
/// holds its own slot ([`CpuSlots::enter`]). The output does not depend on the threads
/// (hedgehog's determinism guarantee).
pub fn analyze_on(
    bytes: &[u8],
    name: &str,
    format: &str,
    deadline: Option<Instant>,
    max_cells: u64,
    cpus: Option<&Arc<CpuSlots>>,
) -> SheetOutcome {
    let mut out = SheetOutcome::default();
    // The read borrows the free slots too, for its call only (one thread without a loan).
    let lent = cpus.map(|c| c.lend(MAX_SHEET_THREADS - 1));
    // Decompressed bytes keep hedgehog's limit (2 GiB): parts are read in chunks, not held.
    let options = ReadOptions {
        limits: hh_core::reader::Limits {
            deadline,
            max_cells,
            max_shared_strings: max_cells,
            ..hh_core::reader::Limits::default()
        },
        threads: Some(lent.as_ref().map_or(1, crate::cpus::Lent::threads)),
        ..ReadOptions::default()
    };
    let read = guarded(|| hh_core::reader::open_bytes_with(bytes, Some(name), options));
    drop(lent);
    let wb = match read {
        Ok(Ok(wb)) => wb,
        Ok(Err(e)) => {
            let e = e.to_string();
            out.failed = Some(if over_cell_cap(&e) {
                format!(
                    "{READ_ID}: {e}; {MAX_CELLS_VAR} raises the cap (it follows the memory per \
                     ingest worker)"
                )
            } else {
                format!("{READ_ID}: {e}")
            });
            return out;
        }
        Err(panic) => {
            out.failed = Some(format!("{READ_ID}: hedgehog panicked: {panic}"));
            return out;
        }
    };
    out.steps.push(READ_ID);
    let fact = |key: FactKey, value: Value| Fact {
        key,
        value,
        conf: 1.0,
        src: READ_ID,
        evidence: None,
    };
    out.facts.push(fact(keys::FORMAT, Value::text(format)));
    out.facts.push(fact(keys::SHEETS, int(wb.sheets.len())));
    let (mut cells, mut formulas) = (0usize, 0usize);
    for s in &wb.sheets {
        cells += s.cells.len();
        formulas += s.formula_count();
    }
    for s in wb.sheets.iter().take(MAX_SHEETS) {
        let mut r = vec![("name", Value::text(s.name.as_str()))];
        if let Some(u) = s.used_range() {
            r.push((
                "range",
                Value::text(format!(
                    "{}:{}",
                    hh_core::a1(u.top, u.left),
                    hh_core::a1(u.bottom, u.right)
                )),
            ));
        }
        r.push(("cells", int(s.cells.len())));
        r.push(("formulas", int(s.formula_count())));
        r.push(("merges", int(s.merges.len())));
        out.facts.push(fact(keys::SHEET, Value::record(r)));
    }
    out.facts.push(fact(keys::CELLS, int(cells)));
    out.facts.push(fact(keys::FORMULAS, int(formulas)));
    let text = sample_text(&wb);
    if !text.is_empty() {
        out.facts.push(fact(keys::TEXT, Value::text(text)));
    }
    tables(&wb, deadline, cpus, &mut out);
    out
}

/// The tables, through hedgehog's normalizer (rules plus its learned row model), on the
/// caller's thread plus what `cpus` lends; the loan is returned when this returns (a panic
/// included).
fn tables(
    wb: &Workbook,
    deadline: Option<Instant>,
    cpus: Option<&Arc<CpuSlots>>,
    out: &mut SheetOutcome,
) {
    let lent = cpus.map(|c| c.lend(MAX_SHEET_THREADS - 1));
    let threads = lent.as_ref().map_or(1, crate::cpus::Lent::threads);
    out.threads = threads;
    // Only the tables' shapes are read below, never their values: hedgehog drops each sheet's
    // values once its tables are done, so the values of all sheets are never held at once.
    let opts = NormalizeOptions {
        deadline,
        threads: Some(threads),
        shapes_only: true,
        ..NormalizeOptions::default()
    };
    let norm = guarded(|| hh_core::try_normalize_workbook(wb, &opts));
    drop(lent);
    let norm = match norm {
        Ok(Ok(n)) => n,
        Ok(Err(e)) => {
            out.problems.push(format!("{TABLES_ID}: {e}"));
            return;
        }
        Err(panic) => {
            out.problems
                .push(format!("{TABLES_ID}: hedgehog panicked: {panic}"));
            return;
        }
    };
    out.steps.push(TABLES_ID);
    let fact = |key: FactKey, value: Value, conf: f32| Fact {
        key,
        value,
        conf,
        src: TABLES_ID,
        evidence: None,
    };
    out.facts
        .push(fact(keys::TABLES, int(norm.tables.len()), 1.0));
    for t in norm.tables.iter().take(MAX_TABLES) {
        let mut r = vec![
            ("id", Value::text(format!("T{}", t.n))),
            ("sheet", Value::text(t.source_sheet.as_str())),
        ];
        if let Some((title, _)) = &t.title {
            r.push(("title", Value::text(title.as_str())));
        }
        let columns = t
            .columns
            .iter()
            .filter(|c| c.source_col.is_some())
            .take(MAX_COLUMNS)
            .map(|c| Value::text(c.name.as_str()))
            .collect();
        r.push(("columns", Value::List(columns)));
        r.push(("rows", int(t.src_rows.len())));
        r.push(("conf", Value::Float(f64::from(t.confidence))));
        out.facts.push(fact(
            keys::TABLE,
            Value::record(r),
            t.confidence.clamp(0.0, 1.0),
        ));
    }
}

/// The text cells in sheet order, up to [`MAX_TEXT`] bytes.
fn sample_text(wb: &Workbook) -> String {
    let mut text = String::new();
    'sheets: for s in &wb.sheets {
        for c in &s.cells {
            if let Some(Cell::Text(t)) = &c.value {
                let t = t.trim();
                if t.is_empty() {
                    continue;
                }
                if text.len() + t.len() + 1 > MAX_TEXT {
                    break 'sheets;
                }
                if !text.is_empty() {
                    text.push(' ');
                }
                text.push_str(t);
            }
        }
    }
    text
}

fn int(n: usize) -> Value {
    Value::Int(i64::try_from(n).unwrap_or(i64::MAX))
}

/// Runs `f`, turning a panic into its message.
fn guarded<T>(f: impl FnOnce() -> T) -> Result<T, String> {
    catch_unwind(AssertUnwindSafe(f)).map_err(|p| {
        p.downcast_ref::<&str>()
            .map(|s| (*s).to_owned())
            .or_else(|| p.downcast_ref::<String>().cloned())
            .unwrap_or_else(|| "a panic without a message".to_owned())
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_panic_becomes_an_error() {
        let r = guarded(|| -> u8 { panic!("boom") });
        assert_eq!(r, Err("boom".to_owned()));
        let r = guarded(|| -> u8 { panic!("{}", String::from("formatted")) });
        assert_eq!(r, Err("formatted".to_owned()));
        assert_eq!(guarded(|| 7), Ok(7));
    }

    #[test]
    fn garbage_fails_without_a_panic() {
        let out = analyze(
            b"PK\x03\x04garbage",
            "x.xlsx",
            "xlsx",
            Instant::now().checked_add(std::time::Duration::from_secs(5)),
        );
        assert!(out.failed.is_some());
        assert!(out.facts.is_empty());
    }
}
