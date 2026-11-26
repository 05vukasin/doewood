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
