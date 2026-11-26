//! Schema migrations, embedded in the binary. The version is SQLite's `user_version`.

use rusqlite::Connection;

use crate::StoreError;

/// The migrations, in order. Migration `i` takes the schema from version `i` to `i + 1`.
/// Never edit a released migration: add a new one.
const MIGRATIONS: &[&str] = &[
    include_str!("migrations/0001_init.sql"),
    include_str!("migrations/0002_standalone_model.sql"),
    include_str!("migrations/0003_sources.sql"),
    include_str!("migrations/0004_ingest.sql"),
    include_str!("migrations/0005_llm.sql"),
    include_str!("migrations/0006_enrichment.sql"),
    include_str!("migrations/0007_merges.sql"),
    include_str!("migrations/0008_proposals.sql"),
    include_str!("migrations/0009_feeds.sql"),
    include_str!("migrations/0010_status_counts.sql"),
    include_str!("migrations/0011_sketch_band_sizes.sql"),
    include_str!("migrations/0012_graph_pages.sql"),
    include_str!("migrations/0013_known_copies.sql"),
];

/// The schema version this binary writes.
#[allow(clippy::cast_possible_wrap)]
pub const SCHEMA_VERSION: i64 = MIGRATIONS.len() as i64;

/// Brings the schema up to [`SCHEMA_VERSION`], one transaction per migration.
///
/// Foreign keys are off while migrations run, so that a migration can rebuild a table that
/// others reference (SQLite's create-copy-drop-rename procedure; the pragma has no effect
/// inside a transaction). Each migration checks them before it commits; the caller turns them
/// back on.
pub(crate) fn migrate(conn: &mut Connection) -> Result<i64, StoreError> {
    migrate_to(conn, SCHEMA_VERSION)
}

/// Brings the schema up to `target` (at most [`SCHEMA_VERSION`]): [`migrate`], or a test's
/// older schema to migrate data from.
pub(crate) fn migrate_to(conn: &mut Connection, target: i64) -> Result<i64, StoreError> {
    conn.pragma_update(None, "foreign_keys", false)?;
    let current: i64 = conn.pragma_query_value(None, "user_version", |r| r.get(0))?;
    if current > SCHEMA_VERSION {
        return Err(StoreError::SchemaTooNew {
            found: current,
            supported: SCHEMA_VERSION,
        });
    }
    for (version, sql) in (1_i64..)
        .zip(MIGRATIONS)
        .skip(usize::try_from(current).unwrap_or(0))
        .take_while(|(v, _)| *v <= target)
    {
        let tx = conn.transaction()?;
        tx.execute_batch(sql)?;
        let violations: i64 =
            tx.query_row("SELECT count(*) FROM pragma_foreign_key_check", [], |r| {
                r.get(0)
            })?;
        if violations > 0 {
            // Dropping `tx` rolls the migration back.
            return Err(StoreError::Migration {
                version,
                reason: format!("{violations} foreign key violation(s)"),
            });
        }
        tx.pragma_update(None, "user_version", version)?;
        tx.commit()?;
    }
    Ok(target.min(SCHEMA_VERSION))
}

#[cfg(test)]
mod tests {
    use std::time::Instant;

    use rusqlite::Connection;

    use super::{SCHEMA_VERSION, migrate_to};

    /// Times each pending migration of the store file `DOEWOOD_MIGRATE_DB` (issue #112: the
    /// `edges_by_dst` rebuild of schema 12 on a big store): first its SQL and the foreign key
    /// check apart, rolled back, then as [`super::migrate`] runs it. Migrates the file: run it
    /// on a copy.
    /// `DOEWOOD_MIGRATE_DB=<file> cargo test --release -p doewood-store --lib migration_timings
    /// -- --ignored --nocapture`.
    #[test]
    #[ignore = "a measurement (issue #112); see docs/configuration.md"]
    fn migration_timings() {
        let path = std::env::var("DOEWOOD_MIGRATE_DB").expect("DOEWOOD_MIGRATE_DB");
        let mut conn = Connection::open(path).unwrap();
        let from: i64 = conn
            .pragma_query_value(None, "user_version", |r| r.get(0))
            .unwrap();
        let edges: i64 = conn
            .query_row("SELECT count(*) FROM edges", [], |r| r.get(0))
            .unwrap();
        println!("schema {from}, {edges} edges");
        // The two parts of each migration, timed apart and rolled back: its SQL, then the
        // foreign key check the runner makes before it commits.
        for target in from + 1..=SCHEMA_VERSION {
            let tx = conn.transaction().unwrap();
            let t = Instant::now();
            tx.execute_batch(super::MIGRATIONS[usize::try_from(target - 1).unwrap()])
                .unwrap();
            let sql = t.elapsed().as_secs_f64();
            let t = Instant::now();
            let _: i64 = tx
                .query_row("SELECT count(*) FROM pragma_foreign_key_check", [], |r| {
                    r.get(0)
                })
                .unwrap();
            println!(
                "migration {target}, rolled back: SQL {sql:.2} s, foreign key check {:.2} s",
                t.elapsed().as_secs_f64()
            );
        }
        for target in from + 1..=SCHEMA_VERSION {
            let t = Instant::now();
            migrate_to(&mut conn, target).unwrap();
            println!("migration {target}: {:.2} s", t.elapsed().as_secs_f64());
        }
        conn.pragma_update(None, "foreign_keys", true).unwrap();
    }
}
