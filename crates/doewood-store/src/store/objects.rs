//! What discovery has seen per source (`objects_seen`) and where each scan is
//! (`scan_checkpoints`).
//!
//! Keys compare as bytes (SQLite's `BINARY` collation on UTF-8 text), the same order in which
//! `ListObjectsV2` returns them, so a scan can walk a listing page and the stored rows of the
//! same key range side by side.

use doewood_core::SourceId;
use rusqlite::{OptionalExtension as _, params};

use super::Snapshot;
use super::{Store, now};
use crate::StoreError;

// The key-range reads below have one SQL text per combination of bounds. A bound written as
// `(?2 IS NULL OR key > ?2)` kept SQLite from using it as a range on the primary key (or the
// queue index): every page read the source's keys from the first one and filtered, so a
// pass over all keys was quadratic in their number (issue #138). Each text numbers its
// parameters the same way, so the same arguments bind to every variant (an unused number
// stays unbound).

/// `seen_objects` without bounds.
pub(crate) const SEEN_OBJECTS: &str = "SELECT key, etag, size, last_modified FROM objects_seen \
     WHERE source_id = ?1 AND status <> 'deleted' ORDER BY key LIMIT ?4";
/// `seen_objects` with `after` (`?2`).
pub(crate) const SEEN_OBJECTS_AFTER: &str = "SELECT key, etag, size, last_modified \
     FROM objects_seen WHERE source_id = ?1 AND status <> 'deleted' AND key > ?2 \
     ORDER BY key LIMIT ?4";
/// `seen_objects` with `upto` (`?3`).
pub(crate) const SEEN_OBJECTS_UPTO: &str = "SELECT key, etag, size, last_modified \
     FROM objects_seen WHERE source_id = ?1 AND status <> 'deleted' AND key <= ?3 \
     ORDER BY key LIMIT ?4";
/// `seen_objects` with both bounds.
pub(crate) const SEEN_OBJECTS_BETWEEN: &str = "SELECT key, etag, size, last_modified \
     FROM objects_seen WHERE source_id = ?1 AND status <> 'deleted' \
     AND key > ?2 AND key <= ?3 ORDER BY key LIMIT ?4";

/// `pending_objects` from the first key.
pub(crate) const PENDING_OBJECTS: &str = "SELECT key, etag, size, last_modified \
     FROM objects_seen INDEXED BY objects_seen_pending \
     WHERE source_id = ?1 AND status = 'pending' ORDER BY key LIMIT ?3";
/// `pending_objects` after `?2`.
pub(crate) const PENDING_OBJECTS_AFTER: &str = "SELECT key, etag, size, last_modified \
     FROM objects_seen INDEXED BY objects_seen_pending \
     WHERE source_id = ?1 AND status = 'pending' AND key > ?2 ORDER BY key LIMIT ?3";

/// `ingested_objects` from the first key.
pub(crate) const INGESTED_OBJECTS: &str = "SELECT key, blake3 FROM objects_seen \
     WHERE source_id = ?1 AND status IN ('done', 'skipped') AND blake3 IS NOT NULL \
     ORDER BY key LIMIT ?3";
/// `ingested_objects` after `?2`.
pub(crate) const INGESTED_OBJECTS_AFTER: &str = "SELECT key, blake3 FROM objects_seen \
     WHERE source_id = ?1 AND status IN ('done', 'skipped') AND blake3 IS NOT NULL \
     AND key > ?2 ORDER BY key LIMIT ?3";

/// The `seen_objects` text for the bounds given.
pub(crate) fn seen_objects_sql(after: bool, upto: bool) -> &'static str {
    match (after, upto) {
        (false, false) => SEEN_OBJECTS,
        (true, false) => SEEN_OBJECTS_AFTER,
        (false, true) => SEEN_OBJECTS_UPTO,
        (true, true) => SEEN_OBJECTS_BETWEEN,
    }
}

/// An object discovery has seen and that was not deleted since.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SeenObject {
    /// Its key.
    pub key: String,
    /// Its ETag, without quotes.
    pub etag: Option<String>,
    /// Its size in bytes.
    pub size: u64,
    /// Its last-modified time (Unix seconds).
    pub last_modified: Option<i64>,
}

/// A change discovery records.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ObjectChange {
    /// A new or changed object: stored with status `pending`, for ingest to pick up.
    Upsert(SeenObject),
    /// An object that is gone: its row gets status `deleted`.
    Delete {
        /// Its key.
        key: String,
    },
}

/// How ingest finished with one object.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum IngestOutcome {
    /// Analyzed and stored: status `done`.
    Done {
        /// The content's hash.
        blake3: [u8; 32],
    },
    /// Not analyzable (an unknown format, stored with its identity only): status `skipped`.
    Skipped {
        /// The content's hash.
        blake3: [u8; 32],
    },
    /// Gave up: status `failed`.
    Failed {
        /// Why, one line.
        error: String,
        /// Attempts made in total.
        attempts: u32,
    },
}

/// An object's ingest status (`objects_seen`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IngestStatus {
    /// `pending`, `done`, `failed`, `skipped` or `deleted`.
    pub status: String,
    /// The ETag ingest worked on.
    pub etag: Option<String>,
    /// The content's hash, once ingested (kept after a delete).
    pub blake3: Option<[u8; 32]>,
    /// Attempts so far.
    pub attempts: u32,
    /// The last failure.
    pub last_error: Option<String>,
}

/// Content ingest has hashed, by the MD5 ETag and size of the GetObject response that
/// brought it (`known_content`, issue #131).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct KnownContent {
    /// The single-part ETag (the content's MD5), lowercase hex.
    pub etag: String,
    /// The size in bytes.
    pub size: u64,
    /// The content's hash.
    pub blake3: [u8; 32],
}

/// Longest `last_error` kept, in bytes.
const MAX_ERROR_LEN: usize = 1000;

/// Where a source's scan is.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ScanCheckpoint {
    /// The number of the current (or last) pass; 0 before the first.
    pub pass: u64,
    /// Whether a pass is under way.
    pub in_progress: bool,
    /// The last key whose comparison is committed in the pass under way.
    pub start_after: Option<String>,
    /// When the current (or last) pass started (Unix seconds).
    pub pass_started_at: Option<i64>,
    /// When the last pass completed (Unix seconds).
    pub last_completed_at: Option<i64>,
}

impl Store {
    /// Up to `limit` live objects of a source with `after < key <= upto`, in key order. `None`
    /// leaves that end open.
    pub fn seen_objects(
        &self,
        source: SourceId,
        after: Option<&str>,
        upto: Option<&str>,
        limit: usize,
    ) -> Result<Vec<SeenObject>, StoreError> {
        self.snapshot(|s| s.seen_objects(source, after, upto, limit))
    }

    /// One live object of a source.
    pub fn seen_object(
        &self,
        source: SourceId,
        key: &str,
    ) -> Result<Option<SeenObject>, StoreError> {
        self.snapshot(|s| s.seen_object(source, key))
    }

    /// The number of live objects of a source.
    pub fn live_object_count(&self, source: SourceId) -> Result<u64, StoreError> {
        self.snapshot(|s| s.live_object_count(source))
    }

    /// Records changes and, if given, the scan checkpoint, in one transaction: a crash keeps
    /// both or neither.
    pub fn record_objects(
        &self,
        source: SourceId,
        changes: &[ObjectChange],
        checkpoint: Option<&ScanCheckpoint>,
    ) -> Result<(), StoreError> {
        let id = source.to_string();
        let now = now();
        let mut conn = self.conn();
        let tx = conn.transaction()?;
        {
            let mut upsert = tx.prepare_cached(
                "INSERT INTO objects_seen (source_id, key, etag, size, last_modified, status, \
                 attempts, updated_at) VALUES (?1, ?2, ?3, ?4, ?5, 'pending', 0, ?6) \
                 ON CONFLICT (source_id, key) DO UPDATE SET etag = excluded.etag, \
                 size = excluded.size, last_modified = excluded.last_modified, \
                 status = 'pending', attempts = 0, last_error = NULL, blake3 = NULL, \
                 updated_at = excluded.updated_at",
            )?;
            let mut delete = tx.prepare_cached(
                "UPDATE objects_seen SET status = 'deleted', updated_at = ?3 \
                 WHERE source_id = ?1 AND key = ?2",
            )?;
            for change in changes {
                match change {
                    ObjectChange::Upsert(o) => {
                        let size = i64::try_from(o.size).map_err(|_| {
                            StoreError::InvalidSource(format!("object size {} too large", o.size))
                        })?;
                        upsert.execute(params![id, o.key, o.etag, size, o.last_modified, now])?;
                    }
                    ObjectChange::Delete { key } => {
                        delete.execute(params![id, key, now])?;
                    }
                }
            }
        }
        if let Some(cp) = checkpoint {
            write_checkpoint(&tx, &id, cp, now)?;
        }
        tx.commit()?;
        Ok(())
    }

    /// Up to `limit` objects of a source waiting for ingest (status `pending`) with a key after
    /// `after`, in key order: the durable ingest queue. Events only make ingest look sooner.
    pub fn pending_objects(
        &self,
        source: SourceId,
        after: Option<&str>,
        limit: usize,
    ) -> Result<Vec<SeenObject>, StoreError> {
        self.snapshot(|s| s.pending_objects(source, after, limit))
    }

    /// Records how ingest finished with the version `etag` of an object. Only a `pending` row
    /// with that ETag changes: when the object changed (or was deleted) meanwhile, the newer
    /// state wins and this returns `false`.
    pub fn finish_object(
        &self,
        source: SourceId,
        key: &str,
        etag: Option<&str>,
        outcome: &IngestOutcome,
    ) -> Result<bool, StoreError> {
        self.finish_object_known(source, key, etag, outcome, None)
    }

    /// [`finish_object`](Self::finish_object), and in the same transaction records `known`:
    /// the content's MD5 ETag and size (whether or not a pending row matched; the pair
    /// describes the bytes ingest hashed either way).
    pub fn finish_object_known(
        &self,
        source: SourceId,
        key: &str,
        etag: Option<&str>,
        outcome: &IngestOutcome,
        known: Option<&KnownContent>,
    ) -> Result<bool, StoreError> {
        let (status, blake3, error, attempts): (&str, Option<&[u8]>, Option<String>, Option<i64>) =
            match outcome {
                IngestOutcome::Done { blake3 } => ("done", Some(blake3), None, None),
                IngestOutcome::Skipped { blake3 } => ("skipped", Some(blake3), None, None),
                IngestOutcome::Failed { error, attempts } => (
                    "failed",
                    None,
                    Some(truncate(error, MAX_ERROR_LEN)),
                    Some(i64::from(*attempts)),
                ),
            };
        let now = now();
        let mut conn = self.conn();
        let tx = conn.transaction()?;
        let n = tx
            .prepare_cached(
                "UPDATE objects_seen SET status = ?4, blake3 = coalesce(?5, blake3), \
             last_error = ?6, attempts = coalesce(?7, attempts + 1), updated_at = ?8 \
             WHERE source_id = ?1 AND key = ?2 AND etag IS ?3 AND status = 'pending'",
            )
            .and_then(|mut cached| {
                cached.execute(params![
                    source.to_string(),
                    key,
                    etag,
                    status,
                    blake3,
                    error,
                    attempts,
                    now
                ])
            })?;
        if let Some(k) = known {
            let size = i64::try_from(k.size).map_err(|_| {
                StoreError::InvalidSource(format!("object size {} too large", k.size))
            })?;
            tx.prepare_cached(
                "INSERT INTO known_content (etag, size, blake3, updated_at) \
                 VALUES (?1, ?2, ?3, ?4) ON CONFLICT (etag, size) DO UPDATE \
                 SET blake3 = excluded.blake3, updated_at = excluded.updated_at",
            )
            .and_then(|mut cached| {
                cached.execute(params![k.etag, size, k.blake3.as_slice(), now])
            })?;
        }
        tx.commit()?;
        Ok(n > 0)
    }

    /// Notes a failed attempt that will be retried: the row stays `pending`.
    pub fn note_attempt(
        &self,
        source: SourceId,
        key: &str,
        etag: Option<&str>,
        attempts: u32,
        error: &str,
    ) -> Result<bool, StoreError> {
        let n = self
            .conn()
            .prepare_cached(
                "UPDATE objects_seen SET attempts = ?4, last_error = ?5, updated_at = ?6 \
             WHERE source_id = ?1 AND key = ?2 AND etag IS ?3 AND status = 'pending'",
            )
            .and_then(|mut cached| {
                cached.execute(params![
                    source.to_string(),
                    key,
                    etag,
                    i64::from(attempts),
                    truncate(error, MAX_ERROR_LEN),
                    now()
                ])
            })?;
        Ok(n > 0)
    }

    /// Drops the version `etag` of an object from ingest's queue without ingesting it: its
    /// `pending` row is deleted, so a later scan sees the object as new (issue #25: another
    /// instance owns the source). Returns whether there was such a row.
    pub fn drop_pending_object(
        &self,
        source: SourceId,
        key: &str,
        etag: Option<&str>,
    ) -> Result<bool, StoreError> {
        let n = self
            .conn()
            .prepare_cached(
                "DELETE FROM objects_seen \
                 WHERE source_id = ?1 AND key = ?2 AND etag IS ?3 AND status = 'pending'",
            )
            .and_then(|mut cached| cached.execute(params![source.to_string(), key, etag]))?;
        Ok(n > 0)
    }

    /// [`drop_pending_object`](Self::drop_pending_object) for every pending object of a source,
    /// in one statement. Returns how many.
    pub fn drop_pending_objects(&self, source: SourceId) -> Result<u64, StoreError> {
        let n = self
            .conn()
            .prepare_cached("DELETE FROM objects_seen WHERE source_id = ?1 AND status = 'pending'")
            .and_then(|mut cached| cached.execute(params![source.to_string()]))?;
        Ok(u64::try_from(n).unwrap_or(u64::MAX))
    }

    /// An object's ingest status, deleted objects included.
    pub fn ingest_status(
        &self,
        source: SourceId,
        key: &str,
    ) -> Result<Option<IngestStatus>, StoreError> {
        self.snapshot(|s| s.ingest_status(source, key))
    }

    /// The hash of the content recorded with this MD5 ETag (lowercase hex) and size.
    pub fn known_content(&self, etag: &str, size: u64) -> Result<Option<[u8; 32]>, StoreError> {
        self.snapshot(|s| s.known_content(etag, size))
    }

    /// Up to `limit` ingested objects (`done` or `skipped`, with their hash) with a key after
    /// `after`, in key order: what reprocessing compares with the running step versions.
    pub fn ingested_objects(
        &self,
        source: SourceId,
        after: Option<&str>,
        limit: usize,
    ) -> Result<Vec<(String, [u8; 32])>, StoreError> {
        self.snapshot(|s| s.ingested_objects(source, after, limit))
    }

    /// Puts objects back in the ingest queue (status `pending`, attempts reset). `None`
    /// requeues every live object of the source. Returns how many rows changed.
    pub fn requeue_objects(
        &self,
        source: SourceId,
        keys: Option<&[String]>,
    ) -> Result<u64, StoreError> {
        let id = source.to_string();
        let now = now();
        let mut conn = self.conn();
        let tx = conn.transaction()?;
        let n = match keys {
            None => tx
                .prepare_cached(
                    "UPDATE objects_seen SET status = 'pending', attempts = 0, last_error = NULL, \
                 updated_at = ?2 WHERE source_id = ?1 AND status <> 'deleted'",
                )
                .and_then(|mut cached| cached.execute(params![id, now]))?,
            Some(keys) => {
                let mut stmt = tx.prepare_cached(
                    "UPDATE objects_seen SET status = 'pending', attempts = 0, \
                     last_error = NULL, updated_at = ?3 \
                     WHERE source_id = ?1 AND key = ?2 AND status <> 'deleted'",
                )?;
                let mut n = 0;
                for k in keys {
                    n += stmt.execute(params![id, k, now])?;
                }
                n
            }
        };
        tx.commit()?;
        Ok(n as u64)
    }

    /// A source's scan checkpoint; `None` before its first pass.
    pub fn scan_checkpoint(&self, source: SourceId) -> Result<Option<ScanCheckpoint>, StoreError> {
        self.snapshot(|s| s.scan_checkpoint(source))
    }

    /// Saves a source's scan checkpoint.
    pub fn save_scan_checkpoint(
        &self,
        source: SourceId,
        checkpoint: &ScanCheckpoint,
    ) -> Result<(), StoreError> {
        write_checkpoint(&self.conn(), &source.to_string(), checkpoint, now())
    }
}

/// At most `max` bytes of `s`, cut at a character boundary.
pub(crate) fn truncate(s: &str, max: usize) -> String {
    if s.len() <= max {
        return s.to_owned();
    }
    let mut end = max;
    while !s.is_char_boundary(end) {
        end -= 1;
    }
    s[..end].to_owned()
}

fn write_checkpoint(
    conn: &rusqlite::Connection,
    id: &str,
    cp: &ScanCheckpoint,
    now: i64,
) -> Result<(), StoreError> {
    conn.prepare_cached(
        "INSERT INTO scan_checkpoints (source_id, pass, in_progress, start_after, \
         pass_started_at, last_completed_at, updated_at) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7) \
         ON CONFLICT (source_id) DO UPDATE SET pass = excluded.pass, \
         in_progress = excluded.in_progress, start_after = excluded.start_after, \
         pass_started_at = excluded.pass_started_at, \
         last_completed_at = excluded.last_completed_at, updated_at = excluded.updated_at",
    )?
    .execute(params![
        id,
        i64::try_from(cp.pass).unwrap_or(i64::MAX),
        cp.in_progress,
        cp.start_after,
        cp.pass_started_at,
        cp.last_completed_at,
        now
    ])?;
    Ok(())
}

impl Snapshot<'_> {
    /// Up to `limit` live objects of a source with `after < key <= upto`, in key order. `None`
    /// leaves that end open.
    pub fn seen_objects(
        &self,
        source: SourceId,
        after: Option<&str>,
        upto: Option<&str>,
        limit: usize,
    ) -> Result<Vec<SeenObject>, StoreError> {
        let conn = self.0;
        let mut stmt = conn.prepare_cached(seen_objects_sql(after.is_some(), upto.is_some()))?;
        let rows = stmt
            .query_map(
                params![
                    source.to_string(),
                    after,
                    upto,
                    i64::try_from(limit).unwrap_or(i64::MAX)
                ],
                |r| {
                    Ok(SeenObject {
                        key: r.get(0)?,
                        etag: r.get(1)?,
                        size: u64::try_from(r.get::<_, i64>(2)?).unwrap_or(0),
                        last_modified: r.get(3)?,
                    })
                },
            )?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    /// One live object of a source.
    pub fn seen_object(
        &self,
        source: SourceId,
        key: &str,
    ) -> Result<Option<SeenObject>, StoreError> {
        Ok(self
            .0
            .prepare_cached(
                "SELECT key, etag, size, last_modified FROM objects_seen \
                 WHERE source_id = ?1 AND key = ?2 AND status <> 'deleted'",
            )
            .and_then(|mut cached| {
                cached.query_row(params![source.to_string(), key], |r| {
                    Ok(SeenObject {
                        key: r.get(0)?,
                        etag: r.get(1)?,
                        size: u64::try_from(r.get::<_, i64>(2)?).unwrap_or(0),
                        last_modified: r.get(3)?,
                    })
                })
            })
            .optional()?)
    }

    /// The number of live objects of a source.
    pub fn live_object_count(&self, source: SourceId) -> Result<u64, StoreError> {
        let n: i64 = self
            .0
            .prepare_cached(
                "SELECT coalesce(sum(n), 0) FROM object_counts \
                 WHERE source_id = ?1 AND status <> 'deleted'",
            )?
            .query_row([source.to_string()], |r| r.get(0))?;
        Ok(u64::try_from(n).unwrap_or(0))
    }

    /// Up to `limit` objects of a source waiting for ingest (status `pending`) with a key after
    /// `after`, in key order: the durable ingest queue. Events only make ingest look sooner.
    pub fn pending_objects(
        &self,
        source: SourceId,
        after: Option<&str>,
        limit: usize,
    ) -> Result<Vec<SeenObject>, StoreError> {
        let conn = self.0;
        // The planner's statistics are averages over the statuses, so it reads the primary
        // key in key order instead, which walks every ingested row of a mostly ingested
        // source. The queue index reads only the pending rows, already in key order.
        let mut stmt = conn.prepare_cached(if after.is_some() {
            PENDING_OBJECTS_AFTER
        } else {
            PENDING_OBJECTS
        })?;
        let rows = stmt
            .query_map(
                params![
                    source.to_string(),
                    after,
                    i64::try_from(limit).unwrap_or(i64::MAX)
                ],
                |r| {
                    Ok(SeenObject {
                        key: r.get(0)?,
                        etag: r.get(1)?,
                        size: u64::try_from(r.get::<_, i64>(2)?).unwrap_or(0),
                        last_modified: r.get(3)?,
                    })
                },
            )?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    /// The hash of the content recorded with this MD5 ETag (lowercase hex) and size.
    pub fn known_content(&self, etag: &str, size: u64) -> Result<Option<[u8; 32]>, StoreError> {
        let Ok(size) = i64::try_from(size) else {
            return Ok(None);
        };
        let blake3: Option<Vec<u8>> = self
            .0
            .prepare_cached("SELECT blake3 FROM known_content WHERE etag = ?1 AND size = ?2")
            .and_then(|mut cached| cached.query_row(params![etag, size], |r| r.get(0)))
            .optional()?;
        Ok(blake3.and_then(|b| b.try_into().ok()))
    }

    /// An object's ingest status, deleted objects included.
    pub fn ingest_status(
        &self,
        source: SourceId,
        key: &str,
    ) -> Result<Option<IngestStatus>, StoreError> {
        Ok(self
            .0
            .prepare_cached(
                "SELECT status, etag, blake3, attempts, last_error FROM objects_seen \
                 WHERE source_id = ?1 AND key = ?2",
            )
            .and_then(|mut cached| {
                cached.query_row(params![source.to_string(), key], |r| {
                    let blake3: Option<Vec<u8>> = r.get(2)?;
                    Ok(IngestStatus {
                        status: r.get(0)?,
                        etag: r.get(1)?,
                        blake3: blake3.and_then(|b| b.try_into().ok()),
                        attempts: u32::try_from(r.get::<_, i64>(3)?).unwrap_or(0),
                        last_error: r.get(4)?,
                    })
                })
            })
            .optional()?)
    }

    /// Up to `limit` ingested objects (`done` or `skipped`, with their hash) with a key after
    /// `after`, in key order: what reprocessing compares with the running step versions.
    pub fn ingested_objects(
        &self,
        source: SourceId,
        after: Option<&str>,
        limit: usize,
    ) -> Result<Vec<(String, [u8; 32])>, StoreError> {
        let conn = self.0;
        let mut stmt = conn.prepare_cached(if after.is_some() {
            INGESTED_OBJECTS_AFTER
        } else {
            INGESTED_OBJECTS
        })?;
        let rows = stmt
            .query_map(
                params![
                    source.to_string(),
                    after,
                    i64::try_from(limit).unwrap_or(i64::MAX)
                ],
                |r| Ok((r.get::<_, String>(0)?, r.get::<_, Vec<u8>>(1)?)),
            )?
            .filter_map(|row| match row {
                Ok((k, b)) => b.try_into().ok().map(|b| Ok((k, b))),
                Err(e) => Some(Err(e)),
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    /// A source's scan checkpoint; `None` before its first pass.
    pub fn scan_checkpoint(&self, source: SourceId) -> Result<Option<ScanCheckpoint>, StoreError> {
        Ok(self
            .0
            .prepare_cached(
                "SELECT pass, in_progress, start_after, pass_started_at, last_completed_at \
                 FROM scan_checkpoints WHERE source_id = ?1",
            )
            .and_then(|mut cached| {
                cached.query_row([source.to_string()], |r| {
                    Ok(ScanCheckpoint {
                        pass: u64::try_from(r.get::<_, i64>(0)?).unwrap_or(0),
                        in_progress: r.get(1)?,
                        start_after: r.get(2)?,
                        pass_started_at: r.get(3)?,
                        last_completed_at: r.get(4)?,
                    })
                })
            })
            .optional()?)
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
            .add_source(&NewSource::new("http://x", "b"), None)
            .unwrap();
        (store, id)
    }

    fn obj(key: &str, size: u64) -> SeenObject {
        SeenObject {
            key: key.into(),
            etag: Some(format!("e-{key}")),
            size,
            last_modified: Some(1),
        }
    }

    #[test]
    fn known_content_is_recorded_with_the_finish() {
        let (store, id) = setup();
        store
            .record_objects(id, &[ObjectChange::Upsert(obj("a", 3))], None)
            .unwrap();
        let md5 = "900150983cd24fb0d6963f7d28e17f72";
        assert_eq!(store.known_content(md5, 3).unwrap(), None);
        let known = KnownContent {
            etag: md5.into(),
            size: 3,
            blake3: [7; 32],
        };
        let done = IngestOutcome::Done { blake3: [7; 32] };
        assert!(
            store
                .finish_object_known(id, "a", Some("e-a"), &done, Some(&known))
                .unwrap()
        );
        assert_eq!(
            store.ingest_status(id, "a").unwrap().unwrap().status,
            "done"
        );
        assert_eq!(store.known_content(md5, 3).unwrap(), Some([7; 32]));
        // Keyed by size too.
        assert_eq!(store.known_content(md5, 4).unwrap(), None);
        // Recorded even when no pending row matched (the object changed meanwhile), and the
        // newer hash of the pair wins.
        let newer = KnownContent {
            blake3: [8; 32],
            ..known
        };
        assert!(
            !store
                .finish_object_known(id, "a", Some("e-a"), &done, Some(&newer))
                .unwrap()
        );
        assert_eq!(store.known_content(md5, 3).unwrap(), Some([8; 32]));
    }

    #[test]
    fn ranges_follow_byte_order_and_skip_deleted() {
        let (store, id) = setup();
        let keys = ["a", "a/b", "b", "B", "é", "z"];
        let changes: Vec<_> = keys
            .iter()
            .map(|k| ObjectChange::Upsert(obj(k, 1)))
            .collect();
        store.record_objects(id, &changes, None).unwrap();
        let all: Vec<_> = store
            .seen_objects(id, None, None, 100)
            .unwrap()
            .into_iter()
            .map(|o| o.key)
            .collect();
        // Byte order: uppercase before lowercase, multi-byte UTF-8 last.
        assert_eq!(all, ["B", "a", "a/b", "b", "z", "é"]);
        let mid: Vec<_> = store
            .seen_objects(id, Some("a"), Some("b"), 100)
            .unwrap()
            .into_iter()
            .map(|o| o.key)
            .collect();
        assert_eq!(mid, ["a/b", "b"]);
        assert_eq!(store.seen_objects(id, None, None, 2).unwrap().len(), 2);
        store
            .record_objects(id, &[ObjectChange::Delete { key: "b".into() }], None)
            .unwrap();
        assert!(store.seen_object(id, "b").unwrap().is_none());
        assert_eq!(store.live_object_count(id).unwrap(), 5);
        // An upsert brings a deleted key back as pending.
        store
            .record_objects(id, &[ObjectChange::Upsert(obj("b", 9))], None)
            .unwrap();
        assert_eq!(store.seen_object(id, "b").unwrap().unwrap().size, 9);
    }

    // The key-range reads as they were before issue #138, one text for every bound.
    const OLD_SEEN: &str = "SELECT key, etag, size, last_modified FROM objects_seen \
         WHERE source_id = ?1 AND status <> 'deleted' \
         AND (?2 IS NULL OR key > ?2) AND (?3 IS NULL OR key <= ?3) ORDER BY key LIMIT ?4";
    const OLD_PENDING: &str = "SELECT key, etag, size, last_modified FROM objects_seen \
         INDEXED BY objects_seen_pending \
         WHERE source_id = ?1 AND status = 'pending' AND (?2 IS NULL OR key > ?2) \
         ORDER BY key LIMIT ?3";
    const OLD_INGESTED: &str = "SELECT key, blake3 FROM objects_seen WHERE source_id = ?1 \
         AND status IN ('done', 'skipped') AND blake3 IS NOT NULL \
         AND (?2 IS NULL OR key > ?2) ORDER BY key LIMIT ?3";

    /// Rows of `sql` with `args`, as (key, etag or hash) pairs.
    fn rows(store: &Store, sql: &str, args: &[&dyn rusqlite::ToSql]) -> Vec<(String, Vec<u8>)> {
        let conn = store.raw();
        let mut stmt = conn.prepare(sql).unwrap();
        stmt.query_map(args, |r| {
            let second = match r.get_ref(1)? {
                rusqlite::types::ValueRef::Text(t) | rusqlite::types::ValueRef::Blob(t) => {
                    t.to_vec()
                }
                _ => Vec::new(),
            };
            Ok((r.get(0)?, second))
        })
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap()
    }

    /// `n` rows in two sources, in every status, keys in a mixed order (some with a shared
    /// prefix, some multi-byte), with a pseudo-random status each. Returns the first source.
    fn generated(store: &Store, n: usize) -> SourceId {
        let id = store
            .add_source(&NewSource::new("http://g", "b"), None)
            .unwrap();
        let other = store
            .add_source(&NewSource::new("http://g", "c"), None)
            .unwrap();
        let statuses = ["pending", "done", "failed", "skipped", "deleted"];
        let mut seed: u64 = 0x2545_f491_4f6c_dd1d;
        let conn = store.raw();
        let mut insert = conn
            .prepare(
                "INSERT INTO objects_seen (source_id, key, etag, size, status, blake3, \
                 updated_at) VALUES (?1, ?2, ?3, 1, ?4, ?5, 1)",
            )
            .unwrap();
        for i in 0..n {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            let key = match i % 4 {
                0 => format!("a/{i:05}"),
                1 => format!("a/{}", seed % 10_000),
                2 => format!("Z{}é", i * 7),
                _ => format!("{seed:x}"),
            };
            let status = statuses[usize::try_from(seed % 5).unwrap()];
            // Some ingested rows without a hash, which `ingested_objects` leaves out.
            let blake3 =
                (!seed.is_multiple_of(7)).then(|| vec![u8::try_from(i % 256).unwrap(); 32]);
            for source in [id, other] {
                insert
                    .execute(params![
                        source.to_string(),
                        key,
                        format!("e{i}"),
                        status,
                        blake3
                    ])
                    .ok();
            }
        }
        id
    }

    #[test]
    fn range_reads_match_the_former_queries() {
        let (store, _) = setup();
        let id = generated(&store, 2000);
        let src = id.to_string();
        let keys: Vec<String> = rows(
            &store,
            OLD_SEEN,
            params![src, None::<String>, None::<String>, -1],
        )
        .into_iter()
        .map(|(k, _)| k)
        .collect();
        assert!(keys.len() > 1000, "{}", keys.len());
        // Bounds: open, below every key, stored keys, keys between stored ones, past the end.
        let mut bounds: Vec<Option<String>> = vec![None, Some(String::new()), Some("~".into())];
        for k in keys.iter().step_by(97) {
            bounds.push(Some(k.clone()));
            bounds.push(Some(format!("{k}0")));
        }
        for after in &bounds {
            for upto in &bounds {
                for limit in [1_usize, 7, 1000] {
                    let n = i64::try_from(limit).unwrap();
                    let old = rows(&store, OLD_SEEN, params![src, after, upto, n]);
                    let new: Vec<_> = store
                        .seen_objects(id, after.as_deref(), upto.as_deref(), limit)
                        .unwrap()
                        .into_iter()
                        .map(|o| (o.key, o.etag.unwrap().into_bytes()))
                        .collect();
                    assert_eq!(old, new, "seen {after:?} {upto:?} {limit}");
                }
            }
            for limit in [1_usize, 7, 1000] {
                let n = i64::try_from(limit).unwrap();
                let old = rows(&store, OLD_PENDING, params![src, after, n]);
                let new: Vec<_> = store
                    .pending_objects(id, after.as_deref(), limit)
                    .unwrap()
                    .into_iter()
                    .map(|o| (o.key, o.etag.unwrap().into_bytes()))
                    .collect();
                assert_eq!(old, new, "pending {after:?} {limit}");
                let old = rows(&store, OLD_INGESTED, params![src, after, n]);
                let new: Vec<_> = store
                    .ingested_objects(id, after.as_deref(), limit)
                    .unwrap()
                    .into_iter()
                    .map(|(k, b)| (k, b.to_vec()))
                    .collect();
                assert_eq!(old, new, "ingested {after:?} {limit}");
            }
        }
        // A walk page by page sees every live key once, in order.
        let mut walked = Vec::new();
        let mut after: Option<String> = None;
        loop {
            let page = store.seen_objects(id, after.as_deref(), None, 64).unwrap();
            let Some(last) = page.last() else { break };
            after = Some(last.key.clone());
            walked.extend(page.into_iter().map(|o| o.key));
        }
        assert_eq!(walked, keys);
    }

    /// The plan of `sql`, one detail per line; every parameter bound to a text.
    fn plan(store: &Store, sql: &str) -> String {
        let conn = store.raw();
        let mut stmt = conn.prepare(&format!("EXPLAIN QUERY PLAN {sql}")).unwrap();
        let args = ["s", "k", "m", "10"];
        let args = &args[..stmt.parameter_count()];
        stmt.query_map(rusqlite::params_from_iter(args), |r| r.get::<_, String>(3))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap()
            .join("\n")
    }

    #[test]
    fn range_reads_start_at_their_bound() {
        let (store, _) = setup();
        generated(&store, 500);
        // Also with sampled statistics.
        for analyzed in [false, true] {
            if analyzed {
                assert!(store.optimize_all().unwrap().tables > 0);
            }
            for (sql, range) in [
                (SEEN_OBJECTS, "PRIMARY KEY (source_id=?)"),
                (SEEN_OBJECTS_AFTER, "PRIMARY KEY (source_id=? AND key>?)"),
                (SEEN_OBJECTS_UPTO, "PRIMARY KEY (source_id=? AND key<?)"),
                (
                    SEEN_OBJECTS_BETWEEN,
                    "PRIMARY KEY (source_id=? AND key>? AND key<?)",
                ),
                (
                    PENDING_OBJECTS,
                    "INDEX objects_seen_pending (source_id=? AND status=?)",
                ),
                (
                    PENDING_OBJECTS_AFTER,
                    "INDEX objects_seen_pending (source_id=? AND status=? AND key>?)",
                ),
                (INGESTED_OBJECTS, "PRIMARY KEY (source_id=?)"),
                (
                    INGESTED_OBJECTS_AFTER,
                    "PRIMARY KEY (source_id=? AND key>?)",
                ),
            ] {
                let p = plan(&store, sql);
                assert!(p.contains(&format!("USING {range}")), "{sql}:\n{p}");
                assert!(!p.contains("TEMP B-TREE"), "{sql}:\n{p}");
            }
        }
    }

    /// Times the pages of an unchanged scan pass (each page of 1000 listed keys compared with
    /// the stored rows up to its last key, `seen_objects` as the scanner calls it) and of a
    /// sweep over a queue of pending keys, old queries against new. `DOEWOOD_KEYSET_KEYS` sets
    /// the number of keys (200,000). Run with
    /// `cargo test --release -p doewood-store keyset_pass -- --ignored --nocapture`.
    #[test]
    #[ignore = "measurement"]
    fn keyset_pass() {
        use std::time::Instant;

        let n: usize = std::env::var("DOEWOOD_KEYSET_KEYS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(200_000);
        let tmp = tempfile::tempdir().unwrap();
        let key = MasterKey::from_bytes([3; 32]);
        let store = Store::open_with_readers(&tmp.path().join(crate::STORE_FILE), &key, 2).unwrap();
        let id = store
            .add_source(&NewSource::new("http://x", "b"), None)
            .unwrap();
        let keys: Vec<String> = (0..n)
            .map(|i| format!("docs/{:03}/{i:07}.pdf", i % 997))
            .collect();
        for chunk in keys.chunks(10_000) {
            let changes: Vec<_> = chunk
                .iter()
                .map(|k| ObjectChange::Upsert(obj(k, 1)))
                .collect();
            store.record_objects(id, &changes, None).unwrap();
        }
        let mut sorted = keys.clone();
        sorted.sort_unstable();
        let src = id.to_string();
        let ms = |t: Instant| t.elapsed().as_secs_f64() * 1000.0;
        for round in 0..2 {
            // The scan: (previous page's last key, this page's last key].
            let t = Instant::now();
            let mut seen = 0;
            for (i, page) in sorted.chunks(1000).enumerate() {
                let after = (i > 0).then(|| sorted[i * 1000 - 1].clone());
                let upto = page.last().cloned();
                seen += rows(&store, OLD_SEEN, params![src, after, upto, 1000]).len();
            }
            let old = ms(t);
            assert_eq!(seen, n);
            let t = Instant::now();
            let mut seen = 0;
            for (i, page) in sorted.chunks(1000).enumerate() {
                let after = (i > 0).then(|| sorted[i * 1000 - 1].as_str());
                seen += store
                    .seen_objects(id, after, page.last().map(String::as_str), 1000)
                    .unwrap()
                    .len();
            }
            let new = ms(t);
            assert_eq!(seen, n);
            println!(
                "round {round}, {n} keys, unchanged scan pages:  old {old:>9.1} ms  new {new:>9.1} ms"
            );
            // The sweep over a queue of pending keys (every key pending, after a first scan).
            let t = Instant::now();
            let (mut after, mut seen) = (None::<String>, 0);
            loop {
                let page = rows(&store, OLD_PENDING, params![src, after, 1000]);
                let Some(last) = page.last() else { break };
                after = Some(last.0.clone());
                seen += page.len();
            }
            let old = ms(t);
            assert_eq!(seen, n);
            let t = Instant::now();
            let (mut after, mut seen) = (None::<String>, 0);
            loop {
                let page = store.pending_objects(id, after.as_deref(), 1000).unwrap();
                let Some(last) = page.last() else { break };
                after = Some(last.key.clone());
                seen += page.len();
            }
            let new = ms(t);
            assert_eq!(seen, n);
            println!(
                "round {round}, {n} keys, pending sweep pages:   old {old:>9.1} ms  new {new:>9.1} ms"
            );
        }
    }

    #[test]
    fn ingest_status_follows_the_etag() {
        let (store, id) = setup();
        let changes: Vec<_> = ["a", "b", "c"]
            .iter()
            .map(|k| ObjectChange::Upsert(obj(k, 1)))
            .collect();
        store.record_objects(id, &changes, None).unwrap();
        let keys = |v: Vec<SeenObject>| v.into_iter().map(|o| o.key).collect::<Vec<_>>();
        assert_eq!(
            keys(store.pending_objects(id, None, 10).unwrap()),
            ["a", "b", "c"]
        );
        assert_eq!(
            keys(store.pending_objects(id, Some("a"), 1).unwrap()),
            ["b"]
        );

        let done = IngestOutcome::Done { blake3: [7; 32] };
        // A stale ETag changes nothing.
        assert!(!store.finish_object(id, "a", Some("old"), &done).unwrap());
        assert!(store.finish_object(id, "a", Some("e-a"), &done).unwrap());
        // Only pending rows change.
        assert!(!store.finish_object(id, "a", Some("e-a"), &done).unwrap());
        let s = store.ingest_status(id, "a").unwrap().unwrap();
        assert_eq!(
            (s.status.as_str(), s.blake3, s.attempts),
            ("done", Some([7; 32]), 1)
        );

        assert!(
            store
                .note_attempt(id, "b", Some("e-b"), 1, "timeout")
                .unwrap()
        );
        let failed = IngestOutcome::Failed {
            error: "x".repeat(5000),
            attempts: 3,
        };
        assert!(store.finish_object(id, "b", Some("e-b"), &failed).unwrap());
        let s = store.ingest_status(id, "b").unwrap().unwrap();
        assert_eq!((s.status.as_str(), s.attempts), ("failed", 3));
        assert_eq!(s.last_error.unwrap().len(), MAX_ERROR_LEN);
        let skipped = IngestOutcome::Skipped { blake3: [8; 32] };
        assert!(store.finish_object(id, "c", Some("e-c"), &skipped).unwrap());
        let counts = store.object_counts(id).unwrap();
        assert_eq!(
            (counts.done, counts.failed, counts.skipped, counts.pending),
            (1, 1, 1, 0)
        );

        assert_eq!(
            store.ingested_objects(id, None, 10).unwrap(),
            [("a".to_owned(), [7; 32]), ("c".to_owned(), [8; 32])]
        );
        // A delete keeps the hash.
        store
            .record_objects(id, &[ObjectChange::Delete { key: "a".into() }], None)
            .unwrap();
        let s = store.ingest_status(id, "a").unwrap().unwrap();
        assert_eq!((s.status.as_str(), s.blake3), ("deleted", Some([7; 32])));

        assert_eq!(
            store.requeue_objects(id, Some(&["c".to_owned()])).unwrap(),
            1
        );
        assert_eq!(keys(store.pending_objects(id, None, 10).unwrap()), ["c"]);
        // Everything live, not the deleted one.
        assert_eq!(store.requeue_objects(id, None).unwrap(), 2);
        assert_eq!(
            keys(store.pending_objects(id, None, 10).unwrap()),
            ["b", "c"]
        );
        assert_eq!(store.ingest_status(id, "b").unwrap().unwrap().attempts, 0);
    }

    #[test]
    fn status_reads_do_not_wait_for_the_writer() {
        use std::time::Duration;

        let tmp = tempfile::tempdir().unwrap();
        let key = MasterKey::from_bytes([3; 32]);
        let store = Store::open_with_readers(&tmp.path().join(crate::STORE_FILE), &key, 2).unwrap();
        let id = store
            .add_source(&NewSource::new("http://x", "b"), None)
            .unwrap();
        let done = IngestOutcome::Done { blake3: [7; 32] };
        let changes = [
            ObjectChange::Upsert(obj("a", 1)),
            ObjectChange::Upsert(obj("b", 1)),
        ];
        store.record_objects(id, &changes, None).unwrap();
        assert!(store.finish_object(id, "b", Some("e-b"), &done).unwrap());
        // A commit in progress holds the writer: a worker's `finish_object`, the scanner's
        // `record_objects`.
        let writer = store.conn();
        writer
            .execute_batch(
                "BEGIN IMMEDIATE; UPDATE objects_seen SET status = 'done' WHERE key = 'a';",
            )
            .unwrap();
        let (tx, rx) = std::sync::mpsc::channel();
        std::thread::scope(|s| {
            s.spawn(|| {
                let reads = (
                    store.ingest_status(id, "a").unwrap().unwrap().status,
                    store.seen_object(id, "a").unwrap().is_some(),
                    store.seen_objects(id, None, None, 10).unwrap().len(),
                    store.live_object_count(id).unwrap(),
                    store.pending_objects(id, None, 10).unwrap().len(),
                    store.ingested_objects(id, None, 10).unwrap().len(),
                    store.ingest_settings().unwrap(),
                );
                tx.send(reads).unwrap();
            });
            let reads = rx.recv_timeout(Duration::from_secs(10));
            // Let go of the writer before failing, or the reads never end.
            writer.execute_batch("COMMIT").unwrap();
            drop(writer);
            // They saw the last committed state.
            assert_eq!(
                reads.expect("the reads waited for the writer"),
                (
                    "pending".into(),
                    true,
                    2,
                    2,
                    1,
                    1,
                    crate::IngestSettings::default()
                )
            );
        });
        assert_eq!(
            store.ingest_status(id, "a").unwrap().unwrap().status,
            "done"
        );
    }

    #[test]
    fn checkpoints_commit_with_the_changes() {
        let (store, id) = setup();
        assert_eq!(store.scan_checkpoint(id).unwrap(), None);
        let cp = ScanCheckpoint {
            pass: 1,
            in_progress: true,
            start_after: Some("k1".into()),
            pass_started_at: Some(10),
            last_completed_at: None,
        };
        store
            .record_objects(id, &[ObjectChange::Upsert(obj("k1", 1))], Some(&cp))
            .unwrap();
        assert_eq!(store.scan_checkpoint(id).unwrap(), Some(cp.clone()));
        let done = ScanCheckpoint {
            in_progress: false,
            start_after: None,
            last_completed_at: Some(20),
            ..cp
        };
        store.save_scan_checkpoint(id, &done).unwrap();
        assert_eq!(store.scan_checkpoint(id).unwrap(), Some(done));
        // A failing change rolls the checkpoint back too.
        let bad = ObjectChange::Upsert(SeenObject {
            size: u64::MAX,
            ..obj("k2", 0)
        });
        let next = ScanCheckpoint {
            pass: 2,
            ..ScanCheckpoint::default()
        };
        assert!(store.record_objects(id, &[bad], Some(&next)).is_err());
        assert_eq!(store.scan_checkpoint(id).unwrap().unwrap().pass, 1);
    }
}
