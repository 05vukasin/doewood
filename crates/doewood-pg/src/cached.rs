//! Statements through each connection's statement cache (issue #70).
//!
//! `tokio-postgres` prepares a `&str` before every run: one round trip and a server parse
//! for the prepare, a second for the run. [`Cached`] prepares a SQL text once per pooled
//! connection (`deadpool-postgres`' `prepare_cached`) and runs the stored statement after
//! that, so a statement is one round trip and the server can reuse its plan.
//!
//! - The cache belongs to the connection and lives as long as it does. The pool recycles with
//!   `RecyclingMethod::Fast`, which runs nothing on the connection, so cached statements stay
//!   valid; a recycling method that runs `DISCARD ALL` would drop them on the server.
//! - Migrations run before the pool hands out a connection for anything else, on a plain
//!   `tokio-postgres` client, so no statement is cached against an older schema.
//! - Only fixed SQL texts (or texts with a small, bounded number of shapes) go through here:
//!   each text keeps one prepared statement per connection for the connection's life.

use async_trait::async_trait;
use deadpool_postgres::GenericClient;
use doewood_model::Result;
use tokio_postgres::Row;
use tokio_postgres::types::ToSql;

use crate::backend::pg;

/// Runs SQL through the connection's statement cache; errors are mapped with [`pg`].
#[async_trait]
pub(crate) trait Cached {
    /// Exactly one row.
    async fn one(&self, sql: &str, params: &[&(dyn ToSql + Sync)]) -> Result<Row>;
    /// At most one row.
    async fn opt(&self, sql: &str, params: &[&(dyn ToSql + Sync)]) -> Result<Option<Row>>;
    /// All rows.
    async fn rows(&self, sql: &str, params: &[&(dyn ToSql + Sync)]) -> Result<Vec<Row>>;
    /// The number of rows changed.
    async fn exec(&self, sql: &str, params: &[&(dyn ToSql + Sync)]) -> Result<u64>;
}

#[async_trait]
impl<C: GenericClient> Cached for C {
    async fn one(&self, sql: &str, params: &[&(dyn ToSql + Sync)]) -> Result<Row> {
        let st = self.prepare_cached(sql).await.map_err(pg)?;
        self.query_one(&st, params).await.map_err(pg)
    }

    async fn opt(&self, sql: &str, params: &[&(dyn ToSql + Sync)]) -> Result<Option<Row>> {
        let st = self.prepare_cached(sql).await.map_err(pg)?;
        self.query_opt(&st, params).await.map_err(pg)
    }

    async fn rows(&self, sql: &str, params: &[&(dyn ToSql + Sync)]) -> Result<Vec<Row>> {
        let st = self.prepare_cached(sql).await.map_err(pg)?;
        self.query(&st, params).await.map_err(pg)
    }

    async fn exec(&self, sql: &str, params: &[&(dyn ToSql + Sync)]) -> Result<u64> {
        let st = self.prepare_cached(sql).await.map_err(pg)?;
        self.execute(&st, params).await.map_err(pg)
    }
}
