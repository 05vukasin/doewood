//! What discovery has seen per source (`objects_seen`) and where each scan is
//! (`scan_checkpoints`).
//!
//! Keys compare as bytes (SQLite's `BINARY` collation on UTF-8 text), the same order in which
//! `ListObjectsV2` returns them, so a scan can walk a listing page and the stored rows of the
//! same key range side by side.

use doewood_core::SourceId;
use rusqlite::{OptionalExtension as _, params};

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
        let conn = self.conn();
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
            .conn()
            .query_row(
                "SELECT key, etag, size, last_modified FROM objects_seen \
                 WHERE source_id = ?1 AND key = ?2 AND status <> 'deleted'",
                params![source.to_string(), key],
                |r| {
                    Ok(SeenObject {
                        key: r.get(0)?,
                        etag: r.get(1)?,
                        size: u64::try_from(r.get::<_, i64>(2)?).unwrap_or(0),
                        last_modified: r.get(3)?,
                    })
                },
            )
            .optional()?)
    }

    /// The number of live objects of a source.
    pub fn live_object_count(&self, source: SourceId) -> Result<u64, StoreError> {
        let n: i64 = self.conn().query_row(
            "SELECT count(*) FROM objects_seen WHERE source_id = ?1 AND status <> 'deleted'",
            [source.to_string()],
            |r| r.get(0),
        )?;
        Ok(u64::try_from(n).unwrap_or(0))
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

    /// A source's scan checkpoint; `None` before its first pass.
    pub fn scan_checkpoint(&self, source: SourceId) -> Result<Option<ScanCheckpoint>, StoreError> {
        Ok(self
            .conn()
            .query_row(
                "SELECT pass, in_progress, start_after, pass_started_at, last_completed_at \
                 FROM scan_checkpoints WHERE source_id = ?1",
                [source.to_string()],
                |r| {
                    Ok(ScanCheckpoint {
                        pass: u64::try_from(r.get::<_, i64>(0)?).unwrap_or(0),
                        in_progress: r.get(1)?,
                        start_after: r.get(2)?,
                        pass_started_at: r.get(3)?,
                        last_completed_at: r.get(4)?,
                    })
                },
            )
            .optional()?)
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
