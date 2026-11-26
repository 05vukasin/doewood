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
        let n = self
            .conn()
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
                    now()
                ])
            })?;
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

    /// An object's ingest status, deleted objects included.
    pub fn ingest_status(
        &self,
        source: SourceId,
        key: &str,
    ) -> Result<Option<IngestStatus>, StoreError> {
        self.snapshot(|s| s.ingest_status(source, key))
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
        let mut stmt = conn.prepare_cached(
            "SELECT key, etag, size, last_modified FROM objects_seen \
             WHERE source_id = ?1 AND status <> 'deleted' \
             AND (?2 IS NULL OR key > ?2) AND (?3 IS NULL OR key <= ?3) \
             ORDER BY key LIMIT ?4",
        )?;
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
        let mut stmt = conn.prepare_cached(
            "SELECT key, etag, size, last_modified FROM objects_seen \
             INDEXED BY objects_seen_pending \
             WHERE source_id = ?1 AND status = 'pending' AND (?2 IS NULL OR key > ?2) \
             ORDER BY key LIMIT ?3",
        )?;
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
        let mut stmt = conn.prepare_cached(
            "SELECT key, blake3 FROM objects_seen WHERE source_id = ?1 \
             AND status IN ('done', 'skipped') AND blake3 IS NOT NULL \
             AND (?2 IS NULL OR key > ?2) ORDER BY key LIMIT ?3",
        )?;
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
