//! Registry delta feeds over Postgres (schema 6, issue #49): each feed's lease and cursor,
//! shared by every instance, and the lookups the feeds make.

use std::time::Duration;

use async_trait::async_trait;
use doewood_core::InstanceId;
use doewood_model::{
    FEED_BATCH_MAX, FeedState, FeedStore, Result, due_limit, validate_batch, validate_cache_key,
    validate_cursor, validate_feed,
};
use uuid::Uuid;

use crate::PgStore;
use crate::backend::{client, pg};
use crate::cached::Cached;

#[async_trait]
impl FeedStore for PgStore {
    async fn lease_feed(&self, feed: &str, owner: &InstanceId, ttl: Duration) -> Result<bool> {
        validate_feed(feed)?;
        let c = client(&self.pool).await?;
        let won = c
            .opt(
                "INSERT INTO registry_feeds (feed, owner, lease_expires_at) \
                 VALUES ($1, $2, now() + make_interval(secs => $3)) \
                 ON CONFLICT (feed) DO UPDATE SET owner = EXCLUDED.owner, \
                 lease_expires_at = EXCLUDED.lease_expires_at \
                 WHERE registry_feeds.owner IS NULL OR registry_feeds.owner = EXCLUDED.owner \
                 OR registry_feeds.lease_expires_at <= now() \
                 RETURNING owner",
                &[&feed, &owner.as_str(), &ttl.as_secs_f64()],
            )
            .await?;
        Ok(won.is_some())
    }

    async fn release_feed(&self, feed: &str, owner: &InstanceId) -> Result<bool> {
        validate_feed(feed)?;
        let c = client(&self.pool).await?;
        let n = c
            .exec(
                "UPDATE registry_feeds SET owner = NULL, lease_expires_at = NULL \
                 WHERE feed = $1 AND owner = $2",
                &[&feed, &owner.as_str()],
            )
            .await?;
        Ok(n > 0)
    }

    async fn feed_state(&self, feed: &str) -> Result<Option<FeedState>> {
        validate_feed(feed)?;
        let c = client(&self.pool).await?;
        c.opt(
            "SELECT cursor, extract(epoch FROM next_run_at)::bigint, \
             coalesce(extract(epoch FROM updated_at)::bigint, 0), \
             CASE WHEN lease_expires_at > now() THEN owner END \
             FROM registry_feeds WHERE feed = $1",
            &[&feed],
        )
        .await?
        .map(|r| {
            Ok(FeedState {
                feed: feed.to_owned(),
                cursor: r.try_get(0).map_err(pg)?,
                next_run_at: r.try_get(1).map_err(pg)?,
                updated_at: r.try_get(2).map_err(pg)?,
                owner: r.try_get(3).map_err(pg)?,
            })
        })
        .transpose()
    }

    async fn save_feed(
        &self,
        feed: &str,
        owner: &InstanceId,
        cursor: &str,
        next_run_at: i64,
    ) -> Result<bool> {
        validate_feed(feed)?;
        validate_cursor(cursor)?;
        let c = client(&self.pool).await?;
        let n = c
            .exec(
                "UPDATE registry_feeds SET cursor = $3, \
                 next_run_at = to_timestamp(greatest(0, least($4::bigint, 253402300799))), \
                 updated_at = now() \
                 WHERE feed = $1 AND owner = $2 AND lease_expires_at > now()",
                &[&feed, &owner.as_str(), &cursor, &next_run_at],
            )
            .await?;
        Ok(n > 0)
    }

    async fn companies_by_alias(&self, aliases: &[String]) -> Result<Vec<(String, Uuid)>> {
        validate_batch(aliases.len())?;
        if aliases.is_empty() {
            return Ok(Vec::new());
        }
        let c = client(&self.pool).await?;
        let rows = c
            .rows(
                "SELECT alias, node_id FROM node_aliases \
                 WHERE kind = 'company' AND node_id IS NOT NULL AND alias = ANY($1) \
                 ORDER BY alias COLLATE \"C\"",
                &[&aliases],
            )
            .await?;
        rows.iter()
            .map(|r| Ok((r.try_get(0).map_err(pg)?, r.try_get(1).map_err(pg)?)))
            .collect()
    }

    async fn company_aliases(
        &self,
        prefix: &str,
        after: Option<&str>,
        limit: u32,
    ) -> Result<Vec<(String, Uuid)>> {
        validate_cache_key(prefix)?;
        let limit = i64::from(due_limit(limit).min(u32::try_from(FEED_BATCH_MAX).unwrap_or(1000)));
        let c = client(&self.pool).await?;
        let rows = c
            .rows_uncached(
                "SELECT alias, node_id FROM node_aliases \
                 WHERE kind = 'company' AND node_id IS NOT NULL \
                 AND alias COLLATE \"C\" >= $1::text COLLATE \"C\" AND starts_with(alias, $1) \
                 AND ($2::text IS NULL OR alias COLLATE \"C\" > $2::text COLLATE \"C\") \
                 ORDER BY alias COLLATE \"C\" LIMIT $3",
                &[&prefix, &after, &limit],
            )
            .await?;
        rows.iter()
            .map(|r| Ok((r.try_get(0).map_err(pg)?, r.try_get(1).map_err(pg)?)))
            .collect()
    }
}
