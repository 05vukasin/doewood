//! The LLM queue (ADR 0001, section 6): the documents a source's policy queued for a summary
//! (`llm_jobs`), and what each source spent per UTC day (`llm_usage`).
//!
//! Ingest stores the record first and then queues the document here; the queue in
//! `doewood-ingest` reads the pending rows, asks the provider and ends each row. A row is
//! durable, so a restart picks up where the queue was.

use std::str::FromStr;

use doewood_core::SourceId;
use rusqlite::params;

use super::objects::truncate;
use super::{Store, now};
use crate::StoreError;

/// Seconds in a UTC day: the budget's period.
pub const SECONDS_PER_DAY: i64 = 86_400;

/// Longest `last_error` kept, in bytes.
const MAX_ERROR_LEN: usize = 1000;

/// The UTC day (days since the Unix epoch) of a Unix time.
pub fn utc_day(unix_seconds: i64) -> i64 {
    unix_seconds.div_euclid(SECONDS_PER_DAY)
}

/// Where a queued summary is.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LlmJobStatus {
    /// Waiting for the queue.
    Pending,
    /// The summary is in the record.
    Done,
    /// The provider failed; the record stays without a summary.
    Failed,
    /// Nothing to do: no text to summarize, or the policy no longer asks for it.
    Skipped,
}

impl LlmJobStatus {
    /// `pending`, `done`, `failed` or `skipped`.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Done => "done",
            Self::Failed => "failed",
            Self::Skipped => "skipped",
        }
    }
}

impl FromStr for LlmJobStatus {
    type Err = StoreError;

    fn from_str(s: &str) -> Result<Self, StoreError> {
        Ok(match s {
            "pending" => Self::Pending,
            "done" => Self::Done,
            "failed" => Self::Failed,
            "skipped" => Self::Skipped,
            other => return Err(StoreError::BadSetting(format!("llm_jobs.status {other:?}"))),
        })
    }
}

/// One queued summary.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LlmJob {
    /// The source whose policy queued it.
    pub source: SourceId,
    /// The document (its record's key).
    pub blake3: [u8; 32],
    /// The document's kind when it was queued.
    pub kind: Option<String>,
    /// Where it is.
    pub status: LlmJobStatus,
    /// Provider calls tried for it.
    pub attempts: u32,
    /// The last failure.
    pub last_error: Option<String>,
}

/// A source's queue and spend.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct LlmCounts {
    /// Waiting.
    pub pending: u64,
    /// Summarized.
    pub done: u64,
    /// The provider failed.
    pub failed: u64,
    /// Nothing to summarize.
    pub skipped: u64,
    /// The most recent failure, if any row has one.
    pub last_error: Option<String>,
}

/// What a source spent on one UTC day.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct LlmUsage {
    /// Tokens (prompt plus completion).
    pub tokens: u64,
    /// Provider calls.
    pub calls: u64,
}

fn parse_source(s: &str) -> Result<SourceId, StoreError> {
    s.parse()
        .map_err(|_| StoreError::BadSetting(format!("llm_jobs.source_id {s:?}")))
}

fn to_i64(n: u64) -> i64 {
    i64::try_from(n).unwrap_or(i64::MAX)
}

impl Store {
    /// Queues a document of a source for a summary (again: an ended row becomes pending).
    pub fn queue_llm_job(
        &self,
        source: SourceId,
        blake3: &[u8; 32],
        kind: Option<&str>,
    ) -> Result<(), StoreError> {
        let now = now();
        self.conn()
            .prepare_cached(
                "INSERT INTO llm_jobs (source_id, blake3, kind, status, attempts, queued_at, \
             updated_at) VALUES (?1, ?2, ?3, 'pending', 0, ?4, ?4) \
             ON CONFLICT (source_id, blake3) DO UPDATE SET kind = excluded.kind, \
             status = 'pending', attempts = 0, last_error = NULL, queued_at = excluded.queued_at, \
             updated_at = excluded.updated_at WHERE llm_jobs.status != 'pending'",
            )
            .and_then(|mut cached| {
                cached.execute(params![source.to_string(), &blake3[..], kind, now])
            })?;
        Ok(())
    }

    /// Up to `limit` pending jobs, oldest first.
    pub fn pending_llm_jobs(&self, limit: u32) -> Result<Vec<LlmJob>, StoreError> {
        let conn = self.conn();
        let mut stmt = conn.prepare_cached(
            "SELECT source_id, blake3, kind, status, attempts, last_error FROM llm_jobs \
             WHERE status = 'pending' ORDER BY queued_at, source_id, blake3 LIMIT ?1",
        )?;
        let rows = stmt.query_map([limit], |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, Vec<u8>>(1)?,
                r.get::<_, Option<String>>(2)?,
                r.get::<_, String>(3)?,
                r.get::<_, i64>(4)?,
                r.get::<_, Option<String>>(5)?,
            ))
        })?;
        rows.map(|row| {
            let (source, blake3, kind, status, attempts, last_error) = row?;
            Ok(LlmJob {
                source: parse_source(&source)?,
                blake3: blake3
                    .try_into()
                    .map_err(|_| StoreError::BadSetting("llm_jobs.blake3".to_owned()))?,
                kind,
                status: status.parse()?,
                attempts: u32::try_from(attempts).unwrap_or(u32::MAX),
                last_error,
            })
        })
        .collect()
    }

    /// One job.
    pub fn llm_job(
        &self,
        source: SourceId,
        blake3: &[u8; 32],
    ) -> Result<Option<LlmJob>, StoreError> {
        use rusqlite::OptionalExtension as _;
        let row = self
            .conn()
            .prepare_cached(
                "SELECT kind, status, attempts, last_error FROM llm_jobs \
                 WHERE source_id = ?1 AND blake3 = ?2",
            )
            .and_then(|mut cached| {
                cached.query_row(params![source.to_string(), &blake3[..]], |r| {
                    Ok((
                        r.get::<_, Option<String>>(0)?,
                        r.get::<_, String>(1)?,
                        r.get::<_, i64>(2)?,
                        r.get::<_, Option<String>>(3)?,
                    ))
                })
            })
            .optional()?;
        row.map(|(kind, status, attempts, last_error)| {
            Ok(LlmJob {
                source,
                blake3: *blake3,
                kind,
                status: status.parse()?,
                attempts: u32::try_from(attempts).unwrap_or(u32::MAX),
                last_error,
            })
        })
        .transpose()
    }

    /// Ends a pending job: `done`, `failed` (with the error) or `skipped` (with the reason).
    /// `called` counts a provider call in its attempts. Returns whether a pending row changed.
    pub fn finish_llm_job(
        &self,
        source: SourceId,
        blake3: &[u8; 32],
        status: LlmJobStatus,
        error: Option<&str>,
        called: bool,
    ) -> Result<bool, StoreError> {
        let n = self
            .conn()
            .prepare_cached(
                "UPDATE llm_jobs SET status = ?3, last_error = ?4, attempts = attempts + ?5, \
             updated_at = ?6 WHERE source_id = ?1 AND blake3 = ?2 AND status = 'pending'",
            )
            .and_then(|mut cached| {
                cached.execute(params![
                    source.to_string(),
                    &blake3[..],
                    status.as_str(),
                    error.map(|e| truncate(e, MAX_ERROR_LEN)),
                    i64::from(called),
                    now()
                ])
            })?;
        Ok(n > 0)
    }

    /// How many of a source's jobs are in each status, and its latest failure.
    pub fn llm_counts(&self, source: SourceId) -> Result<LlmCounts, StoreError> {
        use rusqlite::OptionalExtension as _;
        let conn = self.conn();
        let mut counts = LlmCounts::default();
        {
            let mut stmt =
                conn.prepare_cached("SELECT status, n FROM llm_job_counts WHERE source_id = ?1")?;
            let rows = stmt.query_map([source.to_string()], |r| {
                Ok((r.get::<_, String>(0)?, r.get::<_, i64>(1)?))
            })?;
            for row in rows {
                let (status, n) = row?;
                let n = u64::try_from(n).unwrap_or(0);
                match status.parse()? {
                    LlmJobStatus::Pending => counts.pending = n,
                    LlmJobStatus::Done => counts.done = n,
                    LlmJobStatus::Failed => counts.failed = n,
                    LlmJobStatus::Skipped => counts.skipped = n,
                }
            }
        }
        counts.last_error = conn
            .prepare_cached(
                "SELECT last_error FROM llm_jobs WHERE source_id = ?1 AND status = 'failed' \
                 AND last_error IS NOT NULL ORDER BY updated_at DESC LIMIT 1",
            )?
            .query_row([source.to_string()], |r| r.get(0))
            .optional()?;
        Ok(counts)
    }

    /// What a source spent on UTC day `day` ([`utc_day`]).
    pub fn llm_usage(&self, source: SourceId, day: i64) -> Result<LlmUsage, StoreError> {
        use rusqlite::OptionalExtension as _;
        let row: Option<(i64, i64)> = self
            .conn()
            .prepare_cached("SELECT tokens, calls FROM llm_usage WHERE source_id = ?1 AND day = ?2")
            .and_then(|mut cached| {
                cached.query_row(params![source.to_string(), day], |r| {
                    Ok((r.get(0)?, r.get(1)?))
                })
            })
            .optional()?;
        Ok(row.map_or_else(LlmUsage::default, |(t, c)| LlmUsage {
            tokens: u64::try_from(t).unwrap_or(0),
            calls: u64::try_from(c).unwrap_or(0),
        }))
    }

    /// Adds one call and its tokens to a source's spend on UTC day `day`; returns the day's
    /// total. Days before the previous one are dropped: only today's spend counts.
    pub fn add_llm_usage(
        &self,
        source: SourceId,
        day: i64,
        tokens: u64,
    ) -> Result<LlmUsage, StoreError> {
        let mut conn = self.conn();
        let tx = conn.transaction()?;
        tx.prepare_cached(
            "INSERT INTO llm_usage (source_id, day, tokens, calls) VALUES (?1, ?2, ?3, 1) \
             ON CONFLICT (source_id, day) DO UPDATE SET \
             tokens = min(llm_usage.tokens + excluded.tokens, 9223372036854775807), \
             calls = llm_usage.calls + 1",
        )
        .and_then(|mut cached| cached.execute(params![source.to_string(), day, to_i64(tokens)]))?;
        tx.prepare_cached("DELETE FROM llm_usage WHERE source_id = ?1 AND day < ?2 - 1")
            .and_then(|mut cached| cached.execute(params![source.to_string(), day]))?;
        let (t, c): (i64, i64) = tx
            .prepare_cached("SELECT tokens, calls FROM llm_usage WHERE source_id = ?1 AND day = ?2")
            .and_then(|mut cached| {
                cached.query_row(params![source.to_string(), day], |r| {
                    Ok((r.get(0)?, r.get(1)?))
                })
            })?;
        tx.commit()?;
        Ok(LlmUsage {
            tokens: u64::try_from(t).unwrap_or(0),
            calls: u64::try_from(c).unwrap_or(0),
        })
    }
}

#[cfg(test)]
mod tests {
    use doewood_core::MasterKey;

    use super::*;
    use crate::NewSource;

    fn setup() -> (Store, SourceId) {
        let store = Store::open_in_memory(&MasterKey::from_bytes([3; 32])).unwrap();
        let id = store
            .add_source(&NewSource::new("http://127.0.0.1:9000", "docs"), None)
            .unwrap();
        (store, id)
    }

    #[test]
    fn jobs_queue_end_and_queue_again() {
        let (store, id) = setup();
        let (a, b) = ([1; 32], [2; 32]);
        store.queue_llm_job(id, &a, Some("contract")).unwrap();
        store.queue_llm_job(id, &b, None).unwrap();
        // Queuing a pending job again changes nothing.
        store.queue_llm_job(id, &a, Some("contract")).unwrap();
        let pending = store.pending_llm_jobs(10).unwrap();
        assert_eq!(pending.len(), 2);
        assert_eq!(pending[0].kind.as_deref(), Some("contract"));
        assert_eq!(store.pending_llm_jobs(1).unwrap().len(), 1);

        assert!(
            store
                .finish_llm_job(id, &a, LlmJobStatus::Failed, Some("HTTP 500"), true)
                .unwrap()
        );
        // Only a pending row ends.
        assert!(
            !store
                .finish_llm_job(id, &a, LlmJobStatus::Done, None, true)
                .unwrap()
        );
        store
            .finish_llm_job(id, &b, LlmJobStatus::Skipped, Some("no text"), false)
            .unwrap();
        let job = store.llm_job(id, &a).unwrap().unwrap();
        assert_eq!(
            (job.status, job.attempts, job.last_error.as_deref()),
            (LlmJobStatus::Failed, 1, Some("HTTP 500"))
        );
        let counts = store.llm_counts(id).unwrap();
        assert_eq!(
            (counts.pending, counts.failed, counts.skipped, counts.done),
            (0, 1, 1, 0)
        );
        assert_eq!(counts.last_error.as_deref(), Some("HTTP 500"));

        // A new ingest of the document queues it again, from scratch.
        store.queue_llm_job(id, &a, Some("contract")).unwrap();
        let job = store.llm_job(id, &a).unwrap().unwrap();
        assert_eq!((job.status, job.attempts), (LlmJobStatus::Pending, 0));
        assert!(job.last_error.is_none());

        // Removing the source removes its jobs.
        store.delete_source(id).unwrap();
        assert!(store.pending_llm_jobs(10).unwrap().is_empty());
    }

    #[test]
    fn policies_queue_by_mode_and_kind() {
        use crate::{LlmMode, LlmPolicy};
        let p = |mode| LlmPolicy {
            mode,
            ..LlmPolicy::default()
        };
        let kinds = p(LlmMode::Kinds(vec!["contract".into(), "invoice".into()]));
        for (policy, kind, queued, on_demand) in [
            (p(LlmMode::Off), Some("contract"), false, false),
            (p(LlmMode::Off), None, false, false),
            (p(LlmMode::All), Some("contract"), true, true),
            (p(LlmMode::All), None, true, true),
            (kinds.clone(), Some("invoice"), true, true),
            (kinds.clone(), Some("letter"), false, true),
            (kinds.clone(), Some("Invoice"), false, true),
            (kinds, None, false, true),
            (p(LlmMode::OnDemand), Some("contract"), false, true),
            (p(LlmMode::OnDemand), None, false, true),
        ] {
            assert_eq!(policy.queues(kind), queued, "{policy:?} {kind:?}");
            assert_eq!(policy.allows_on_demand(), on_demand, "{policy:?}");
        }
        assert!(
            !LlmPolicy::default().queues(Some("contract")),
            "off by default"
        );
    }

    #[test]
    fn usage_is_per_utc_day() {
        let (store, id) = setup();
        let day = utc_day(1_700_000_000);
        assert_eq!(day, 19_675);
        assert_eq!(utc_day(-1), -1);
        assert_eq!(store.llm_usage(id, day).unwrap(), LlmUsage::default());
        store.add_llm_usage(id, day, 100).unwrap();
        let u = store.add_llm_usage(id, day, 50).unwrap();
        assert_eq!((u.tokens, u.calls), (150, 2));
        // The next day starts at zero; the day before it is kept, older ones go.
        assert_eq!(store.llm_usage(id, day + 1).unwrap().tokens, 0);
        store.add_llm_usage(id, day + 1, 7).unwrap();
        assert_eq!(store.llm_usage(id, day).unwrap().tokens, 150);
        store.add_llm_usage(id, day + 3, 1).unwrap();
        assert_eq!(store.llm_usage(id, day).unwrap().tokens, 0);
        assert_eq!(store.llm_usage(id, day + 3).unwrap().tokens, 1);
    }
}
