//! Schema migrations, embedded in the binary. The version is SQLite's `user_version`.

use rusqlite::Connection;

use crate::StoreError;

/// The migrations, in order. Migration `i` takes the schema from version `i` to `i + 1`.
/// Never edit a released migration: add a new one.
const MIGRATIONS: &[&str] = &[
    include_str!("migrations/0001_init.sql"),
    include_str!("migrations/0002_standalone_model.sql"),
];

/// The schema version this binary writes.
#[allow(clippy::cast_possible_wrap)]
pub const SCHEMA_VERSION: i64 = MIGRATIONS.len() as i64;

/// Brings the schema up to [`SCHEMA_VERSION`], one transaction per migration.
pub(crate) fn migrate(conn: &mut Connection) -> Result<i64, StoreError> {
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
    {
        let tx = conn.transaction()?;
        tx.execute_batch(sql)?;
        tx.pragma_update(None, "user_version", version)?;
        tx.commit()?;
    }
    Ok(SCHEMA_VERSION)
}
