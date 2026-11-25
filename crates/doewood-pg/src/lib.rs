//! doewood's shared store (ADR 0001, section 2): one Postgres database that every instance
//! uses for records, the graph, source claims, company leases, search, and the enrichment
//! job's registry and favicon caches and company freshness (ADR 0002 sections 2, 5 and 6), and
//! the merge log and dismissals (section 1.8). It implements the `doewood-model` store traits.
//!
//! - **No database at build time:** plain `tokio-postgres` with run-time SQL, no macros.
//! - **TLS** through rustls (ring), trusting the system roots plus the Mozilla roots. The URL's
//!   `sslmode` applies: `disable`, `prefer` (the default) or `require`. Certificates are always
//!   verified when TLS is used.
//! - **A pool** (`deadpool-postgres`) with bounded waits, so a dead database fails calls quickly
//!   instead of hanging them.
//! - **Prepared statements** are cached per pooled connection (`deadpool-postgres`' statement
//!   cache), so a statement is one round trip instead of two. The pool recycles with
//!   `RecyclingMethod::Fast`, which keeps them.
//! - **Migrations** are embedded and applied on connect, together with the pgvector setup, in
//!   one transaction under an advisory lock ([`MIGRATION_LOCK`]), so instances starting
//!   together migrate once: the others wait (at most 5 minutes) and find the schema migrated.
//!   Versions are kept in `doewood_schema`.
//! - **pgvector** is optional: when the extension is available it is created and
//!   `search_index.embedding` is added; otherwise this is logged and skipped, never fatal.

mod backend;
mod cached;
mod enrichment;
mod feeds;
mod merges;
mod migrate;
mod proposals;
#[cfg(test)]
mod tests;
mod tls;

use std::time::Duration;

use deadpool_postgres::{Manager, ManagerConfig, Pool, RecyclingMethod, Runtime};
use doewood_core::PostgresUrl;
use doewood_model::{Error, Result};
use tracing::info;

pub use migrate::{MIGRATION_LOCK, SCHEMA_VERSION};

/// Connections in the pool.
const POOL_SIZE: usize = 16;
/// Connect timeout when the URL sets none.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
/// How long a call waits for a free connection, or for a new one to open.
const POOL_WAIT: Duration = Duration::from_secs(10);

/// The Postgres backend. Cheap to clone: clones share the pool.
#[derive(Clone)]
pub struct PgStore {
    pool: Pool,
    vector: bool,
}

impl std::fmt::Debug for PgStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PgStore")
            .field("vector", &self.vector)
            .finish_non_exhaustive()
    }
}

impl PgStore {
    /// Connects, migrates the schema and sets up pgvector when it is available.
    ///
    /// Fails with [`Error::Unavailable`] when the database cannot be reached, so the caller
    /// can retry; error messages never contain the password.
    pub async fn connect(url: &PostgresUrl) -> Result<Self> {
        let config: tokio_postgres::Config = url.expose().parse().map_err(|e| {
            Error::Invalid(format!(
                "DOEWOOD_POSTGRES_URL is not a valid Postgres URL: {e}"
            ))
        })?;
        Self::connect_config(config).await
    }

    pub(crate) async fn connect_config(mut config: tokio_postgres::Config) -> Result<Self> {
        if config.get_connect_timeout().is_none() {
            config.connect_timeout(CONNECT_TIMEOUT);
        }
        if config.get_application_name().is_none() {
            config.application_name("doewood");
        }
        let manager = Manager::from_config(
            config,
            tls::connector()?,
            ManagerConfig {
                // Runs nothing on recycle, so the cached prepared statements stay valid; a
                // custom recycle with `DISCARD ALL` would drop them on the server while the
                // pool still holds them.
                recycling_method: RecyclingMethod::Fast,
            },
        );
        let pool = Pool::builder(manager)
            .max_size(POOL_SIZE)
            .runtime(Runtime::Tokio1)
            .wait_timeout(Some(POOL_WAIT))
            .create_timeout(Some(POOL_WAIT))
            .recycle_timeout(Some(POOL_WAIT))
            .build()
            .map_err(|e| Error::Invalid(format!("connection pool: {e}")))?;
        let mut client = backend::client(&pool).await?;
        let (version, vector) = migrate::migrate(&mut client).await?;
        info!(
            schema = version,
            pgvector = vector,
            "shared Postgres store ready"
        );
        Ok(Self { pool, vector })
    }

    /// Whether pgvector is installed and `search_index.embedding` exists.
    pub fn has_vector(&self) -> bool {
        self.vector
    }
}
