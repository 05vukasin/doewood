//! Schema migrations, embedded in the binary, and the optional pgvector column.

use doewood_model::{Error, Result};
use tracing::{info, warn};

use crate::backend::pg;

/// The migrations, in order. Migration `i` takes the schema from version `i` to `i + 1`.
/// Never edit a released migration: add a new one.
const MIGRATIONS: &[&str] = &[include_str!("migrations/0001_init.sql")];

/// The schema version this binary writes.
#[allow(clippy::cast_possible_wrap)]
pub const SCHEMA_VERSION: i64 = MIGRATIONS.len() as i64;

/// Serializes schema changes between instances starting at the same time ("doewood").
const LOCK: i64 = 0x0064_6f65_776f_6f64;

/// Brings the schema up to [`SCHEMA_VERSION`] in one transaction, under the advisory lock.
pub(crate) async fn migrate(client: &mut tokio_postgres::Client) -> Result<i64> {
    let tx = client.transaction().await.map_err(pg)?;
    tx.execute("SELECT pg_advisory_xact_lock($1)", &[&LOCK])
        .await
        .map_err(pg)?;
    tx.batch_execute(
        "CREATE TABLE IF NOT EXISTS doewood_schema (
             version    integer PRIMARY KEY,
             applied_at timestamptz NOT NULL DEFAULT now()
         )",
    )
    .await
    .map_err(pg)?;
    let current: i64 = tx
        .query_one(
            "SELECT coalesce(max(version), 0)::bigint FROM doewood_schema",
            &[],
        )
        .await
        .map_err(pg)?
        .get(0);
    if current > SCHEMA_VERSION {
        return Err(Error::SchemaTooNew {
            found: current,
            supported: SCHEMA_VERSION,
        });
    }
    for (version, sql) in (1_i32..)
        .zip(MIGRATIONS)
        .skip(usize::try_from(current).unwrap_or(0))
    {
        tx.batch_execute(sql).await.map_err(pg)?;
        tx.execute(
            "INSERT INTO doewood_schema (version) VALUES ($1)",
            &[&version],
        )
        .await
        .map_err(pg)?;
        info!(version, "shared schema migrated");
    }
    tx.commit().await.map_err(pg)?;
    Ok(SCHEMA_VERSION)
}

/// Creates the pgvector extension when the server offers it, and adds
/// `search_index.embedding`. Returns whether the column exists. A missing extension or a
/// refused `CREATE EXTENSION` (no privilege) is logged and skipped.
pub(crate) async fn ensure_vector(client: &mut tokio_postgres::Client) -> Result<bool> {
    let tx = client.transaction().await.map_err(pg)?;
    tx.execute("SELECT pg_advisory_xact_lock($1)", &[&LOCK])
        .await
        .map_err(pg)?;
    let mut schema = vector_schema(&tx).await?;
    if schema.is_none() {
        let available: bool = tx
            .query_one(
                "SELECT EXISTS (SELECT 1 FROM pg_available_extensions WHERE name = 'vector')",
                &[],
            )
            .await
            .map_err(pg)?
            .get(0);
        if !available {
            info!("pgvector is not available on this server; search runs without embeddings");
            return Ok(false);
        }
        if let Err(e) = tx
            .batch_execute("CREATE EXTENSION IF NOT EXISTS vector")
            .await
        {
            warn!(error = %e, "cannot create the pgvector extension; search runs without embeddings");
            return Ok(false);
        }
        schema = vector_schema(&tx).await?;
    }
    let Some(schema) = schema else {
        return Ok(false);
    };
    let ident = format!("\"{}\"", schema.replace('"', "\"\""));
    tx.batch_execute(&format!(
        "ALTER TABLE search_index ADD COLUMN IF NOT EXISTS embedding {ident}.vector"
    ))
    .await
    .map_err(pg)?;
    tx.commit().await.map_err(pg)?;
    Ok(true)
}

/// The schema pgvector is installed in, if it is.
async fn vector_schema(tx: &tokio_postgres::Transaction<'_>) -> Result<Option<String>> {
    Ok(tx
        .query_opt(
            "SELECT n.nspname::text FROM pg_extension e \
             JOIN pg_namespace n ON n.oid = e.extnamespace WHERE e.extname = 'vector'",
            &[],
        )
        .await
        .map_err(pg)?
        .map(|r| r.get(0)))
}
