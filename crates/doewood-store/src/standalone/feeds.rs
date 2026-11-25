//! Registry delta feeds in standalone mode (schema 9, issue #49): each feed's lease and cursor
//! in the instance's own file, and the lookups the feeds make.

use std::time::Duration;

use async_trait::async_trait;
use doewood_core::InstanceId;
use doewood_model::{
    FEED_BATCH_MAX, FeedState, FeedStore, Result, due_limit, validate_batch, validate_cache_key,
    validate_cursor, validate_feed,
};
use rusqlite::{OptionalExtension as _, params, params_from_iter};
use uuid::Uuid;

use super::{Standalone, sql, uuid};
use crate::store::now;

/// The latest time stored: the end of year 9999.
const MAX_TIME: i64 = 253_402_300_799;

#[async_trait]
impl FeedStore for Standalone {
    async fn lease_feed(&self, feed: &str, owner: &InstanceId, ttl: Duration) -> Result<bool> {
        validate_feed(feed)?;
        let (feed, owner) = (feed.to_owned(), owner.clone());
        self.run(move |conn| {
            let at = now();
            let expires = at.saturating_add(i64::try_from(ttl.as_secs()).unwrap_or(i64::MAX));
            let won: Option<String> = conn
                .query_row(
                    "INSERT INTO registry_feeds (feed, owner, lease_expires_at) \
                     VALUES (?1, ?2, ?3) \
                     ON CONFLICT (feed) DO UPDATE SET owner = excluded.owner, \
                     lease_expires_at = excluded.lease_expires_at \
                     WHERE registry_feeds.owner IS NULL \
                     OR registry_feeds.owner = excluded.owner \
                     OR registry_feeds.lease_expires_at <= ?4 \
                     RETURNING owner",
                    params![feed, owner.as_str(), expires, at],
                    |r| r.get(0),
                )
                .optional()
                .map_err(sql)?;
            Ok(won.is_some())
        })
        .await
    }

    async fn release_feed(&self, feed: &str, owner: &InstanceId) -> Result<bool> {
        validate_feed(feed)?;
        let (feed, owner) = (feed.to_owned(), owner.clone());
        self.run(move |conn| {
            let n = conn
                .execute(
                    "UPDATE registry_feeds SET owner = NULL, lease_expires_at = NULL \
                     WHERE feed = ?1 AND owner = ?2",
                    params![feed, owner.as_str()],
                )
                .map_err(sql)?;
            Ok(n > 0)
        })
        .await
    }

    async fn feed_state(&self, feed: &str) -> Result<Option<FeedState>> {
        validate_feed(feed)?;
        let feed = feed.to_owned();
        self.read(move |conn| {
            conn.query_row(
                "SELECT cursor, next_run_at, coalesce(updated_at, 0), \
                 CASE WHEN lease_expires_at > ?2 THEN owner END \
                 FROM registry_feeds WHERE feed = ?1",
                params![feed, now()],
                |r| {
                    Ok(FeedState {
                        feed: feed.clone(),
                        cursor: r.get(0)?,
                        next_run_at: r.get(1)?,
                        updated_at: r.get(2)?,
                        owner: r.get(3)?,
                    })
                },
            )
            .optional()
            .map_err(sql)
        })
        .await
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
        let (feed, owner, cursor) = (feed.to_owned(), owner.clone(), cursor.to_owned());
        self.run(move |conn| {
            let at = now();
            let n = conn
                .execute(
                    "UPDATE registry_feeds SET cursor = ?3, next_run_at = ?4, updated_at = ?5 \
                     WHERE feed = ?1 AND owner = ?2 AND lease_expires_at > ?5",
                    params![
                        feed,
                        owner.as_str(),
                        cursor,
                        next_run_at.clamp(0, MAX_TIME),
                        at
                    ],
                )
                .map_err(sql)?;
            Ok(n > 0)
        })
        .await
    }

    async fn companies_by_alias(&self, aliases: &[String]) -> Result<Vec<(String, Uuid)>> {
        validate_batch(aliases.len())?;
        if aliases.is_empty() {
            return Ok(Vec::new());
        }
        let aliases = aliases.to_vec();
        self.read(move |conn| {
            let marks = vec!["?"; aliases.len()].join(", ");
            let mut stmt = conn
                .prepare(&format!(
                    "SELECT alias, node_id FROM node_aliases \
                     WHERE kind = 'company' AND node_id IS NOT NULL AND alias IN ({marks}) \
                     ORDER BY alias"
                ))
                .map_err(sql)?;
            let rows = stmt
                .query_map(params_from_iter(aliases.iter()), |r| {
                    Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?))
                })
                .map_err(sql)?
                .collect::<rusqlite::Result<Vec<_>>>()
                .map_err(sql)?;
            rows.into_iter()
                .map(|(alias, node)| Ok((alias, uuid(&node)?)))
                .collect()
        })
        .await
    }

    async fn company_aliases(
        &self,
        prefix: &str,
        after: Option<&str>,
        limit: u32,
    ) -> Result<Vec<(String, Uuid)>> {
        validate_cache_key(prefix)?;
        let limit = i64::from(due_limit(limit).min(u32::try_from(FEED_BATCH_MAX).unwrap_or(1000)));
        let (prefix, after) = (prefix.to_owned(), after.map(str::to_owned));
        self.read(move |conn| {
            let mut stmt = conn
                .prepare_cached(
                    "SELECT alias, node_id FROM node_aliases \
                     WHERE kind = 'company' AND node_id IS NOT NULL \
                     AND alias >= ?1 AND substr(alias, 1, length(?1)) = ?1 \
                     AND (?2 IS NULL OR alias > ?2) ORDER BY alias LIMIT ?3",
                )
                .map_err(sql)?;
            let rows = stmt
                .query_map(params![prefix, after, limit], |r| {
                    Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?))
                })
                .map_err(sql)?
                .collect::<rusqlite::Result<Vec<_>>>()
                .map_err(sql)?;
            rows.into_iter()
                .map(|(alias, node)| Ok((alias, uuid(&node)?)))
                .collect()
        })
        .await
    }
}
