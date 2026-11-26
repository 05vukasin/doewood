//! Issue #127: where a big store's bytes are. Reads the store file that the `scale`
//! measurement of `doewood-ingest` (`tests/pipeline.rs`) built in `DOEWOOD_SCALE_DIR`, and
//! prints each table's and index's size (from `dbstat`), grouped by table, with rows and bytes
//! per document. See docs/deploy.md, "Sizing".

use std::collections::BTreeMap;

use rusqlite::{Connection, OpenFlags};

/// `cargo test -p doewood-store --lib scale_store_sizes -- --ignored --nocapture`
#[test]
#[ignore = "a measurement (issue #127); run by hand"]
#[allow(clippy::cast_precision_loss)]
fn scale_store_sizes() {
    let dir = std::env::var("DOEWOOD_SCALE_DIR").expect("DOEWOOD_SCALE_DIR");
    let path = std::path::Path::new(&dir).join(crate::STORE_FILE);
    let conn = Connection::open_with_flags(&path, OpenFlags::SQLITE_OPEN_READ_ONLY).unwrap();
    let docs: i64 = conn
        .query_row("SELECT count(*) FROM records", [], |r| r.get(0))
        .unwrap();
    let per = |b: i64| b as f64 / docs.max(1) as f64;
    // Each b-tree with the table it belongs to (an index's table; FTS shadow tables by name).
    let mut stmt = conn
        .prepare(
            "SELECT d.name, coalesce(m.tbl_name, d.name), m.type, sum(d.pgsize) \
             FROM dbstat d LEFT JOIN sqlite_master m ON m.name = d.name \
             GROUP BY d.name ORDER BY 4 DESC",
        )
        .unwrap();
    let rows: Vec<(String, String, Option<String>, i64)> = stmt
        .query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)))
        .unwrap()
        .map(Result::unwrap)
        .collect();
    let total: i64 = rows.iter().map(|r| r.3).sum();
    let mut by_table: BTreeMap<String, (i64, i64)> = BTreeMap::new();
    for (name, table, kind, bytes) in &rows {
        let e = by_table.entry(table.clone()).or_default();
        if kind.as_deref() == Some("index") || name.starts_with("sqlite_autoindex") {
            e.1 += bytes;
        } else {
            e.0 += bytes;
        }
    }
    println!(
        "scale: SQLite store {}: {docs} records, {:.1} MiB in b-trees ({:.1} KiB a record), \
         file {:.1} MiB",
        path.display(),
        total as f64 / f64::from(1 << 20),
        per(total) / 1024.0,
        std::fs::metadata(&path).map_or(0.0, |m| m.len() as f64 / f64::from(1 << 20))
    );
    let mut tables: Vec<_> = by_table.into_iter().collect();
    tables.sort_by_key(|(_, (t, i))| -(t + i));
    println!("table | rows | table MiB | indexes MiB | bytes a record");
    for (table, (t, i)) in tables {
        let count: Option<i64> = conn
            .query_row(&format!("SELECT count(*) FROM \"{table}\""), [], |r| {
                r.get(0)
            })
            .ok();
        println!(
            "{table} | {} | {:.1} | {:.1} | {:.0}",
            count.map_or_else(|| "-".into(), |c| c.to_string()),
            t as f64 / f64::from(1 << 20),
            i as f64 / f64::from(1 << 20),
            per(t + i)
        );
    }
    println!("b-trees over 1 MiB:");
    for (name, table, _, bytes) in rows.iter().filter(|r| r.3 > 1 << 20) {
        println!(
            "  {name} ({table}): {:.1} MiB",
            *bytes as f64 / f64::from(1 << 20)
        );
    }
}
