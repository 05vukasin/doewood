//! Keeps the WAL of an on-disk store bounded (issue #151).
//!
//! SQLite checkpoints on its own after a commit that leaves 1,000 pages or more in the WAL,
//! but only PASSIVE: it copies the frames no reader still needs and never waits. The WAL starts
//! over from its beginning (and `journal_size_limit` shrinks the file) only once every frame
//! is copied and no reader reads from it. With the read-only connections busy without a pause
//! (concurrent writers and reads, or a steady API load), that moment may not come: the WAL
//! then grows without bound and every read looks its pages up in it (checkpoint starvation).
//! A WAL of 5.4 GB made reads 7 times as slow (issue #140; that one came from writes that
//! skipped SQLite's checkpoint, see `returning_row` in the standalone backend).
//!
//! So an on-disk store keeps one more connection, for checkpoints only, and a thread that
//! every [`INTERVAL`] makes one pass ([`Store::checkpoint_wal`]):
//!
//! 1. a PASSIVE checkpoint: copies what it can, without waiting and without the writer (it
//!    takes SQLite's checkpoint lock only, so writes and reads go on);
//! 2. a TRUNCATE checkpoint when the WAL file is over [`TRUNCATE_ABOVE`], or when nothing was
//!    written since the last pass and the WAL is not empty (idle): it waits for the reads on
//!    an older snapshot to end (they are one short transaction each), copies the frames left
//!    and empties the file. It waits [`MAX_WAIT`] at most for SQLite's locks, in total, and
//!    gives up (busy; the next pass tries again), so a write waits for it about that long
//!    plus the copy of what step 1 left.

use std::cell::Cell;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, PoisonError};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use rusqlite::{Connection, OpenFlags};

use super::{JOURNAL_SIZE_LIMIT, Store};
use crate::StoreError;

/// Between two passes of the checkpoint thread.
pub const INTERVAL: Duration = Duration::from_secs(5);

/// A WAL file larger than this (64 MiB, the `journal_size_limit`) is emptied at the next pass.
#[allow(clippy::cast_sign_loss)]
pub const TRUNCATE_ABOVE: u64 = JOURNAL_SIZE_LIMIT as u64;

/// The longest a TRUNCATE checkpoint waits for SQLite's locks (the writer's lock, and the
/// readers on an older snapshot), in total.
pub const MAX_WAIT: Duration = Duration::from_millis(50);

/// A checkpoint mode `PRAGMA wal_checkpoint` takes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CheckpointMode {
    /// Copies what no reader needs; never waits.
    Passive,
    /// Copies everything and empties the WAL file, waiting for the locks ([`MAX_WAIT`]).
    Truncate,
}

/// What one checkpoint did (the row of `PRAGMA wal_checkpoint`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Checkpoint {
    /// The mode.
    pub mode: CheckpointMode,
    /// It could not finish: a lock was held longer than it waits (a TRUNCATE), or another
    /// checkpoint was running.
    pub busy: bool,
    /// Frames in the WAL (0 once a TRUNCATE emptied it).
    pub frames: i64,
    /// Of those, frames now in the main file.
    pub copied: i64,
    /// How long it took.
    pub took: Duration,
}

/// One pass of the checkpoint thread ([`Store::checkpoint_wal`]).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct WalPass {
    /// The WAL file's size before the pass, in bytes.
    pub bytes_before: u64,
    /// The PASSIVE checkpoint.
    pub passive: Checkpoint,
    /// The TRUNCATE checkpoint, when the pass made one.
    pub truncate: Option<Checkpoint>,
    /// The WAL file's size after the pass.
    pub bytes_after: u64,
}

/// The WAL's size and what the checkpoint thread did so far ([`Store::wal_stats`]).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct WalStats {
    /// The WAL file's size now, in bytes.
    pub bytes: u64,
    /// TRUNCATE checkpoints that emptied the WAL.
    pub truncated: u64,
    /// TRUNCATE checkpoints that gave up (busy).
    pub busy: u64,
    /// The longest TRUNCATE checkpoint: about the longest a write waited for one.
    pub longest_truncate: Duration,
}

thread_local! {
    /// Until when a checkpoint on this thread may wait for a lock ([`busy`]).
    static DEADLINE: Cell<Option<Instant>> = const { Cell::new(None) };
}

/// The checkpoint connection's busy handler: waits a millisecond at a time until
/// [`DEADLINE`], so that every lock a checkpoint waits for shares one budget.
fn busy(_attempts: i32) -> bool {
    match DEADLINE.get() {
        Some(deadline) if Instant::now() < deadline => {
            std::thread::sleep(Duration::from_millis(1));
            true
        }
        _ => false,
    }
}

/// The checkpoint connection and what it did.
pub(crate) struct Wal {
    /// The connection, and the WAL's frames after the last pass (to tell an idle store).
    conn: Mutex<(Connection, Option<i64>)>,
    file: PathBuf,
    truncated: AtomicU64,
    busy: AtomicU64,
    longest_us: AtomicU64,
}

impl Wal {
    /// A checkpoint connection to the store at `path` (read-write: a checkpoint writes the
    /// main file; it writes no rows).
    fn open(path: &Path) -> Result<Self, StoreError> {
        let mut conn = Connection::open_with_flags(
            path,
            OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )?;
        conn.busy_handler(Some(busy))?;
        let mut file = path.as_os_str().to_owned();
        file.push("-wal");
        Ok(Self {
            conn: Mutex::new((conn, None)),
            file: file.into(),
            truncated: AtomicU64::new(0),
            busy: AtomicU64::new(0),
            longest_us: AtomicU64::new(0),
        })
    }

    fn bytes(&self) -> u64 {
        std::fs::metadata(&self.file).map_or(0, |m| m.len())
    }

    /// One pass (see the module's documentation).
    fn pass(&self) -> Result<WalPass, StoreError> {
        let mut guard = self.conn.lock().unwrap_or_else(PoisonError::into_inner);
        let (conn, last) = &mut *guard;
        let bytes_before = self.bytes();
        let passive = checkpoint(conn, CheckpointMode::Passive)?;
        let idle = passive.frames > 0 && *last == Some(passive.frames);
        let truncate = if bytes_before > TRUNCATE_ABOVE || idle {
            let done = checkpoint(conn, CheckpointMode::Truncate)?;
            let counter = if done.busy {
                &self.busy
            } else {
                &self.truncated
            };
            counter.fetch_add(1, Ordering::Relaxed);
            let us = u64::try_from(done.took.as_micros()).unwrap_or(u64::MAX);
            self.longest_us.fetch_max(us, Ordering::Relaxed);
            Some(done)
        } else {
            None
        };
        *last = Some(truncate.map_or(passive.frames, |t| t.frames));
        Ok(WalPass {
            bytes_before,
            passive,
            truncate,
            bytes_after: self.bytes(),
        })
    }

    fn stats(&self) -> WalStats {
        WalStats {
            bytes: self.bytes(),
            truncated: self.truncated.load(Ordering::Relaxed),
            busy: self.busy.load(Ordering::Relaxed),
            longest_truncate: Duration::from_micros(self.longest_us.load(Ordering::Relaxed)),
        }
    }
}

/// Runs `PRAGMA wal_checkpoint(mode)` on `conn`; a TRUNCATE waits [`MAX_WAIT`] at most.
fn checkpoint(conn: &Connection, mode: CheckpointMode) -> Result<Checkpoint, StoreError> {
    struct Clear;
    impl Drop for Clear {
        fn drop(&mut self) {
            DEADLINE.set(None);
        }
    }
    let started = Instant::now();
    let (sql, _clear) = match mode {
        CheckpointMode::Passive => ("PRAGMA wal_checkpoint(PASSIVE)", None),
        CheckpointMode::Truncate => {
            DEADLINE.set(Some(started + MAX_WAIT));
            ("PRAGMA wal_checkpoint(TRUNCATE)", Some(Clear))
        }
    };
    let (busy, frames, copied) = conn.query_row(sql, [], |r| {
        Ok((r.get::<_, i64>(0)? != 0, r.get(1)?, r.get(2)?))
    })?;
    Ok(Checkpoint {
        mode,
        busy,
        frames,
        copied,
        took: started.elapsed(),
    })
}

/// The checkpoint thread: a pass every [`INTERVAL`] until the store closes.
pub(crate) struct Keeper {
    wal: Arc<Wal>,
    stop: Arc<(Mutex<bool>, Condvar)>,
    thread: Option<JoinHandle<()>>,
}

impl Keeper {
    /// Opens the checkpoint connection to the store at `path` and starts the thread.
    pub(crate) fn start(path: &Path) -> Result<Self, StoreError> {
        let wal = Arc::new(Wal::open(path)?);
        let stop = Arc::new((Mutex::new(false), Condvar::new()));
        let thread = {
            let (wal, stop) = (Arc::clone(&wal), Arc::clone(&stop));
            std::thread::Builder::new()
                .name("doewood-wal".into())
                .spawn(move || run(&wal, &stop))
                .map_err(StoreError::Thread)?
        };
        Ok(Self {
            wal,
            stop,
            thread: Some(thread),
        })
    }
}

impl Drop for Keeper {
    fn drop(&mut self) {
        let (stopped, wake) = &*self.stop;
        *stopped.lock().unwrap_or_else(PoisonError::into_inner) = true;
        wake.notify_all();
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

fn run(wal: &Wal, stop: &(Mutex<bool>, Condvar)) {
    let (stopped, wake) = stop;
    loop {
        {
            let guard = stopped.lock().unwrap_or_else(PoisonError::into_inner);
            let (guard, _) = wake
                .wait_timeout_while(guard, INTERVAL, |stopped| !*stopped)
                .unwrap_or_else(PoisonError::into_inner);
            if *guard {
                return;
            }
        }
        match wal.pass() {
            Ok(pass) => log(&pass),
            Err(e) => tracing::warn!(error = %e, "WAL checkpoint failed; retried at the next pass"),
        }
    }
}

fn log(pass: &WalPass) {
    let ms = |d: Duration| u64::try_from(d.as_millis()).unwrap_or(u64::MAX);
    let Some(t) = pass.truncate else {
        tracing::trace!(
            wal_bytes = pass.bytes_after,
            frames = pass.passive.frames,
            copied = pass.passive.copied,
            busy = pass.passive.busy,
            "WAL checkpoint (passive)"
        );
        return;
    };
    if t.busy {
        tracing::info!(
            wal_bytes = pass.bytes_after,
            frames = t.frames,
            copied = t.copied,
            took_ms = ms(t.took),
            "WAL truncate checkpoint busy (a read or write held a lock); retried at the next pass"
        );
    } else if pass.bytes_before > TRUNCATE_ABOVE {
        tracing::info!(
            wal_bytes_before = pass.bytes_before,
            frames = pass.passive.frames,
            took_ms = ms(t.took),
            "WAL emptied (over the journal size limit)"
        );
    } else {
        tracing::debug!(
            wal_bytes_before = pass.bytes_before,
            frames = pass.passive.frames,
            took_ms = ms(t.took),
            "WAL emptied (idle)"
        );
    }
}

impl Store {
    /// One pass of the checkpoint thread, now (see `INTERVAL`): a PASSIVE checkpoint, then a
    /// TRUNCATE when the WAL is over [`TRUNCATE_ABOVE`] or nothing was written since the last
    /// pass. `None` for an in-memory store.
    pub fn checkpoint_wal(&self) -> Result<Option<WalPass>, StoreError> {
        self.wal.as_ref().map(|k| k.wal.pass()).transpose()
    }

    /// The WAL file's size and the checkpoint thread's work (all 0 for an in-memory store).
    pub fn wal_stats(&self) -> WalStats {
        self.wal
            .as_ref()
            .map_or_else(WalStats::default, |k| k.wal.stats())
    }
}

#[cfg(test)]
mod tests {
    use doewood_core::MasterKey;

    use super::*;
    use crate::STORE_FILE;

    impl Keeper {
        /// The checkpoint connection without its thread: passes run only when a test asks.
        fn manual(path: &Path) -> Self {
            Self {
                wal: Arc::new(Wal::open(path).unwrap()),
                stop: Arc::new((Mutex::new(false), Condvar::new())),
                thread: None,
            }
        }
    }

    fn store(dir: &tempfile::TempDir) -> Store {
        let path = dir.path().join(STORE_FILE);
        let mut store =
            Store::open_with_readers(&path, &MasterKey::from_bytes([3; 32]), 2).unwrap();
        store.wal = Some(Keeper::manual(&path));
        store
    }

    fn write(store: &Store, n: usize) {
        for i in 0..n {
            store
                .set_setting(&format!("k{i}"), &serde_json::json!("x".repeat(2000)))
                .unwrap();
        }
    }

    /// Nothing written since the last pass: the WAL is emptied.
    #[test]
    fn an_idle_store_empties_its_wal() {
        let dir = tempfile::tempdir().unwrap();
        let store = store(&dir);
        write(&store, 50);
        let first = store.checkpoint_wal().unwrap().unwrap();
        assert!(first.passive.frames > 0, "{first:?}");
        assert_eq!(first.passive.copied, first.passive.frames, "{first:?}");
        assert_eq!(first.truncate, None, "written since the open: not idle");
        assert!(first.bytes_after > 0);
        let second = store.checkpoint_wal().unwrap().unwrap();
        let t = second.truncate.expect("idle: truncated");
        assert!(!t.busy, "{second:?}");
        assert_eq!((t.frames, second.bytes_after), (0, 0), "{second:?}");
        // An empty WAL is left alone.
        let third = store.checkpoint_wal().unwrap().unwrap();
        assert_eq!(
            (third.passive.frames, third.truncate),
            (0, None),
            "{third:?}"
        );
        let stats = store.wal_stats();
        assert_eq!((stats.bytes, stats.truncated, stats.busy), (0, 1, 0));
        // Writes after it go on as before.
        write(&store, 5);
        assert_eq!(
            store.setting("k4").unwrap(),
            Some(serde_json::json!("x".repeat(2000)))
        );
    }

    /// A read on an older snapshot keeps the frames after it from the main file (what starves
    /// SQLite's own checkpoints); the TRUNCATE waits for it [`MAX_WAIT`] at most, and empties
    /// the WAL at the next pass once it ended.
    #[test]
    fn a_long_read_holds_the_truncate_back_for_the_wait_only() {
        let dir = tempfile::tempdir().unwrap();
        let store = store(&dir);
        // The reader's snapshot starts with frames in the WAL (on a WAL all copied it would
        // read the main file only, and hold nothing back).
        write(&store, 20);
        let reader = store.reader();
        reader.execute_batch("BEGIN").unwrap();
        let _: i64 = reader
            .query_row("SELECT count(*) FROM settings", [], |r| r.get(0))
            .unwrap();
        write(&store, 40);
        let first = store.checkpoint_wal().unwrap().unwrap();
        assert!(
            first.passive.copied < first.passive.frames,
            "the reader's snapshot holds the newer frames back: {first:?}"
        );
        let held = store.checkpoint_wal().unwrap().unwrap();
        let t = held.truncate.expect("idle: tried");
        assert!(t.busy, "{held:?}");
        assert!(t.took < MAX_WAIT * 10, "{held:?}");
        assert!(held.bytes_after > 0);
        // The writer is not held while the checkpoint waits.
        write(&store, 1);
        drop(reader);
        // Idle at the next pass (or the one after, when that write added frames).
        let done = (0..2).find_map(|_| store.checkpoint_wal().unwrap().unwrap().truncate);
        assert!(done.is_some_and(|t| !t.busy), "{done:?}");
        let stats = store.wal_stats();
        assert_eq!(
            (stats.bytes, stats.truncated, stats.busy),
            (0, 1, 1),
            "{stats:?}"
        );
    }

    /// A WAL over the journal size limit is emptied at once, even with writes coming in.
    #[test]
    fn a_large_wal_is_emptied_while_writes_go_on() {
        let dir = tempfile::tempdir().unwrap();
        let store = store(&dir);
        // A reader on an old snapshot lets the WAL grow past the limit: SQLite cannot reset it.
        let reader = store.reader();
        reader.execute_batch("BEGIN").unwrap();
        let _: i64 = reader
            .query_row("SELECT count(*) FROM settings", [], |r| r.get(0))
            .unwrap();
        let blob = vec![7_u8; 1 << 20];
        let mut i = 0;
        while store.wal_stats().bytes <= TRUNCATE_ABOVE {
            store
                .conn()
                .execute(
                    "INSERT OR REPLACE INTO favicons (domain, icon, format, fetched_at, \
                     expires_at) VALUES (?1, ?2, 'png', 0, 0)",
                    rusqlite::params![format!("d{}", i % 8), blob],
                )
                .unwrap();
            i += 1;
        }
        drop(reader);
        let pass = store.checkpoint_wal().unwrap().unwrap();
        assert!(pass.bytes_before > TRUNCATE_ABOVE, "{pass:?}");
        let t = pass.truncate.expect("over the limit: truncated");
        assert!(!t.busy, "{pass:?}");
        assert_eq!(pass.bytes_after, 0, "{pass:?}");
    }

    #[test]
    fn an_in_memory_store_has_no_wal() {
        let store = Store::open_in_memory(&MasterKey::from_bytes([3; 32])).unwrap();
        assert_eq!(store.checkpoint_wal().unwrap(), None);
        assert_eq!(store.wal_stats(), WalStats::default());
    }

    /// The thread makes its passes on its own, and stops with the store.
    #[test]
    fn the_thread_stops_when_the_store_closes() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(STORE_FILE);
        let store = Store::open(&path, &MasterKey::from_bytes([3; 32])).unwrap();
        assert!(store.wal.as_ref().unwrap().thread.is_some());
        let t = Instant::now();
        store.close().unwrap();
        assert!(
            t.elapsed() < INTERVAL,
            "the close waited for the thread's interval"
        );
    }
}
