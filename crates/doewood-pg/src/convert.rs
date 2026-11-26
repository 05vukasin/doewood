//! Converts records written before schema 8 (jsonb in `records.record`) to their canonical
//! text in `records.body` (issue #121), in the background after a start.
//!
//! Batched and resumable: each batch is one short transaction over at most
//! [`CONVERT_BATCH`] rows still to convert (the partial index `records_body_pending` lists
//! them), so a stop loses nothing and the next start goes on where it was. The rows are taken
//! with `FOR UPDATE SKIP LOCKED`: instances starting together share the work instead of
//! repeating it, and a write to a row waits only for its batch. The text is the one reads gave
//! before (`record_text`), so the API's output is the same before, during and after; reads take
//! either column meanwhile. `updated_at` is not touched.

use std::time::Duration;

use deadpool_postgres::Pool;
use doewood_model::{Error, Result};
use serde_json::Value;
use tracing::{info, warn};

use crate::backend::{client, pg, record_text};

/// Rows converted per transaction (records are about 2 to 400 KB of JSON).
const CONVERT_BATCH: i64 = 100;

/// The pause between batches, so the conversion leaves the database to the instances' work.
const CONVERT_PAUSE: Duration = Duration::from_millis(20);

/// How long to wait before trying again when the database is unavailable.
const RETRY: Duration = Duration::from_secs(30);

/// Whether rows are left to convert.
pub(crate) async fn pending(pool: &Pool) -> Result<bool> {
    Ok(client(pool)
        .await?
        .query_one(
            "SELECT EXISTS (SELECT 1 FROM records WHERE body IS NULL)",
            &[],
        )
        .await
        .map_err(pg)?
        .get(0))
}

/// Converts one batch; returns how many rows it converted (0: none left, or all that are left
/// are being converted by another instance).
pub(crate) async fn batch(pool: &Pool) -> Result<u64> {
    let mut c = client(pool).await?;
    let tx = c.transaction().await.map_err(pg)?;
    let rows = tx
        .query(
            "SELECT blake3, record FROM records WHERE body IS NULL \
             ORDER BY blake3 LIMIT $1 FOR UPDATE SKIP LOCKED",
            &[&CONVERT_BATCH],
        )
        .await
        .map_err(pg)?;
    if rows.is_empty() {
        return Ok(0);
    }
    let mut keys = Vec::with_capacity(rows.len());
    let mut texts = Vec::with_capacity(rows.len());
    for r in &rows {
        let record: Option<Value> = r.try_get(1).map_err(pg)?;
        let record = record.ok_or_else(|| Error::Backend {
            backend: "postgres",
            message: "stored record: neither body nor record is set".to_owned(),
        })?;
        keys.push(r.try_get::<_, Vec<u8>>(0).map_err(pg)?);
        texts.push(record_text(&record)?.get().to_owned());
    }
    let n = tx
        .execute(
            "UPDATE records r SET body = v.body, record = NULL \
             FROM unnest($1::bytea[], $2::text[]) AS v (blake3, body) \
             WHERE r.blake3 = v.blake3 AND r.body IS NULL",
            &[&keys, &texts],
        )
        .await
        .map_err(pg)?;
    tx.commit().await.map_err(pg)?;
    Ok(n)
}

/// Converts every row left, batch by batch; returns how many it converted. Stops at the first
/// error other than the database being unavailable (logged; the next start goes on).
pub(crate) async fn run(pool: Pool) -> u64 {
    let mut total = 0u64;
    info!("converting stored records to text (schema 8) in the background");
    loop {
        match batch(&pool).await {
            Ok(0) => break,
            Ok(n) => {
                // A log line about every 10,000 rows.
                if total / 10_000 != (total + n) / 10_000 {
                    info!(converted = total + n, "converting stored records to text");
                }
                total += n;
                tokio::time::sleep(CONVERT_PAUSE).await;
            }
            Err(Error::Unavailable(e)) => {
                warn!(error = %e, retry_s = RETRY.as_secs(), "record conversion paused");
                tokio::time::sleep(RETRY).await;
            }
            Err(e) => {
                warn!(error = %e, converted = total, "record conversion stopped; the next start goes on");
                return total;
            }
        }
    }
    info!(converted = total, "stored records converted to text");
    total
}
