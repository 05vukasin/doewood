//! Schema migrations, embedded in the binary, and the optional pgvector column.

use std::time::{Duration, Instant};

use doewood_model::{Error, Result};
use tokio_postgres::error::SqlState;
use tracing::{info, warn};

use crate::backend::pg;

/// The migrations, in order. Migration `i` takes the schema from version `i` to `i + 1`.
/// Never edit a released migration: add a new one.
pub(crate) const MIGRATIONS: &[&str] = &[
    include_str!("migrations/0001_init.sql"),
    include_str!("migrations/0002_ingest.sql"),
    include_str!("migrations/0003_enrichment.sql"),
    include_str!("migrations/0004_merges.sql"),
    include_str!("migrations/0005_proposals.sql"),
    include_str!("migrations/0006_feeds.sql"),
    include_str!("migrations/0007_search_pending.sql"),
];

/// The schema version this binary writes.
#[allow(clippy::cast_possible_wrap)]
pub const SCHEMA_VERSION: i64 = MIGRATIONS.len() as i64;

/// The advisory lock key ("doewood" in ASCII) that serializes schema setup between instances
/// starting at the same time. Taken with `pg_try_advisory_xact_lock`, so it is held for one
/// transaction and released when it ends, also when a start fails or is cancelled. The key
/// never changes: instances of different versions must serialize on the same lock.
pub const MIGRATION_LOCK: i64 = 0x0064_6f65_776f_6f64;

/// How long a start waits for another instance to finish setting up the schema before it
/// gives up with [`Error::Unavailable`] (the caller retries with backoff).
const LOCK_WAIT: Duration = Duration::from_mins(5);

/// How often a waiting start tries the lock again.
const LOCK_POLL: Duration = Duration::from_millis(200);

/// Brings the schema up to [`SCHEMA_VERSION`] and sets up pgvector when the server offers
/// it, in one transaction under [`MIGRATION_LOCK`]. Instances starting together run this one
/// at a time: the first migrates, the others wait, then find the schema migrated. Returns
/// the schema version and whether `search_index.embedding` exists.
pub(crate) async fn migrate(client: &mut tokio_postgres::Client) -> Result<(i64, bool)> {
    let mut tx = client.transaction().await.map_err(pg)?;
    lock(&tx).await?;
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
    let vector = ensure_vector(&mut tx).await?;
    tx.commit().await.map_err(pg)?;
    Ok((SCHEMA_VERSION, vector))
}

/// Takes [`MIGRATION_LOCK`] for `tx`, waiting at most [`LOCK_WAIT`] for another instance.
async fn lock(tx: &tokio_postgres::Transaction<'_>) -> Result<()> {
    let started = Instant::now();
    let mut waiting = false;
    loop {
        let locked: bool = tx
            .query_one("SELECT pg_try_advisory_xact_lock($1)", &[&MIGRATION_LOCK])
            .await
            .map_err(pg)?
            .get(0);
        if locked {
            if waiting {
                info!(
                    waited_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
                    "schema lock acquired"
                );
            }
            return Ok(());
        }
        if started.elapsed() >= LOCK_WAIT {
            return Err(Error::Unavailable(format!(
                "another instance has held the schema lock (advisory lock {MIGRATION_LOCK}) \
                 for over {}s",
                LOCK_WAIT.as_secs()
            )));
        }
        if !waiting {
            info!(
                lock = MIGRATION_LOCK,
                max_wait_s = LOCK_WAIT.as_secs(),
                "another instance is setting up the shared schema; waiting for it"
            );
            waiting = true;
        }
        tokio::time::sleep(LOCK_POLL).await;
    }
}

/// Creates the pgvector extension when the server offers it, and adds
/// `search_index.embedding`. Returns whether the column exists. A missing extension or a
/// refused `CREATE EXTENSION` (no privilege) is logged and skipped. An extension created at
/// the same moment by someone not holding the lock (`unique_violation`, `duplicate_object`)
/// counts as created.
async fn ensure_vector(tx: &mut tokio_postgres::Transaction<'_>) -> Result<bool> {
    let mut schema = vector_schema(tx).await?;
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
        // A savepoint: a refused CREATE EXTENSION must not abort the migrations with it.
        let sp = tx.savepoint("doewood_vector").await.map_err(pg)?;
        match sp
            .batch_execute("CREATE EXTENSION IF NOT EXISTS vector")
            .await
        {
            Ok(()) => sp.commit().await.map_err(pg)?,
            Err(e) => {
                sp.rollback().await.map_err(pg)?;
                let raced = e.code().is_some_and(|c| {
                    *c == SqlState::UNIQUE_VIOLATION || *c == SqlState::DUPLICATE_OBJECT
                });
                if !raced {
                    warn!(
                        error = %pg(e),
                        "cannot create the pgvector extension; search runs without embeddings"
                    );
                    return Ok(false);
                }
            }
        }
        schema = vector_schema(tx).await?;
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
