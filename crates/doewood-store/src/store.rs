use std::num::NonZeroUsize;
use std::ops::{Deref, DerefMut};
use std::path::{Path, PathBuf};
use std::sync::{Mutex, MutexGuard, PoisonError};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use doewood_core::MasterKey;
use rusqlite::{Connection, OpenFlags, OptionalExtension as _, params};
use zeroize::Zeroizing;

use crate::crypto::Cipher;
use crate::{StoreError, migrate};
use readers::{Caller, Readers};

pub(crate) mod ai;
pub(crate) mod api;
pub(crate) mod claims;
pub(crate) mod enrichment;
pub(crate) mod favicon;
pub(crate) mod ingest;
pub(crate) mod llm;
pub(crate) mod objects;
pub(crate) mod readers;
pub(crate) mod sources;
pub(crate) mod sqs;
pub(crate) mod wal;

/// Name of the store file inside the data directory.
pub const STORE_FILE: &str = "doewood.sqlite";

/// Largest secret value accepted, in bytes.
pub const SECRET_MAX_LEN: usize = 64 * 1024;

/// How long a statement waits for a lock held by another connection (a backup tool,
/// Litestream, the `sqlite3` shell) before it fails.
const BUSY_TIMEOUT: Duration = Duration::from_secs(5);

/// The WAL file's size after a checkpoint, at most (64 MiB).
const JOURNAL_SIZE_LIMIT: i64 = 64 * 1024 * 1024;

/// Prepared statements kept per connection.
pub(crate) const STATEMENT_CACHE: usize = 128;

/// Rows `ANALYZE` samples per index (`PRAGMA analysis_limit`; SQLite advises 100 to 1000):
/// measured for issue #118, the same plans for the hot queries as full statistics.
pub const ANALYSIS_LIMIT: i64 = 400;

/// Read-only connections of an on-disk store when none are asked for: one per CPU, at most
/// this many (see [`plan_readers`]).
pub const DEFAULT_READERS: usize = 8;

/// Most read-only connections a store opens.
pub const MAX_READERS: usize = 16;

/// Reserved secret that proves the master key is the one the store was created with.
const KEY_CHECK: &str = "_doewood.key_check";
const KEY_CHECK_VALUE: &[u8] = b"doewood key check v1";

/// Read-only connections for this machine: [`plan_readers`] for its CPUs (as the process sees
/// them, a cgroup CPU quota included).
pub fn default_readers() -> usize {
    plan_readers(std::thread::available_parallelism().map_or(1, NonZeroUsize::get))
}

/// Read-only connections for `cpus` CPUs: one per CPU, between 1 and [`DEFAULT_READERS`].
pub fn plan_readers(cpus: usize) -> usize {
    cpus.clamp(1, DEFAULT_READERS)
}

/// The instance-local store. Cheap to share behind an `Arc`.
///
/// Every write goes through the one read-write connection behind a mutex, so there is exactly
/// one writer. An on-disk store also opens a few read-only connections ([`DEFAULT_READERS`]):
/// the pure reads the API and the progress snapshot make run there, in WAL mode next to the
/// writer, so they no longer wait for ingest's writes. A read sees what was committed when it
/// started; each read call is one transaction on one connection. An in-memory store has no
/// readers: its reads use the writer, as before.
pub struct Store {
    conn: Mutex<Connection>,
    readers: Readers,
    cipher: Cipher,
    /// An on-disk store's checkpoint connection and thread ([`wal`], issue #151).
    pub(crate) wal: Option<wal::Keeper>,
    /// Which sources another instance owns, in memory ([`claims`], issue #25).
    claims: claims::ClaimView,
}

/// The connection a read runs on: a read-only one, or the writer when the store has none.
pub(crate) enum ReadConn<'a> {
    Writer(MutexGuard<'a, Connection>),
    Reader {
        readers: &'a Readers,
        conn: Option<Connection>,
        caller: Caller,
    },
}

impl Deref for ReadConn<'_> {
    type Target = Connection;

    fn deref(&self) -> &Connection {
        match self {
            Self::Writer(conn) => conn,
            Self::Reader { conn, .. } => conn.as_ref().expect("held until dropped"),
        }
    }
}

impl DerefMut for ReadConn<'_> {
    fn deref_mut(&mut self) -> &mut Connection {
        match self {
            Self::Writer(conn) => conn,
            Self::Reader { conn, .. } => conn.as_mut().expect("held until dropped"),
        }
    }
}

impl Drop for ReadConn<'_> {
    fn drop(&mut self) {
        // A read that panicked or failed to commit leaves its transaction open: end it, or the
        // connection's next read would fail to begin.
        if !self.is_autocommit() {
            let _ = self.execute_batch("ROLLBACK");
        }
        if let Self::Reader {
            readers,
            conn,
            caller,
        } = self
            && let Some(conn) = conn.take()
        {
            readers.give_back(conn, *caller);
        }
    }
}

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    // A panic while holding the lock leaves SQLite consistent (statements are atomic).
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Runs `f` in one read transaction on `conn`, so every statement it runs sees the same
/// committed state. `map` turns SQLite's errors into the caller's.
pub(crate) fn read_tx<T, E>(
    conn: &mut Connection,
    map: impl Fn(rusqlite::Error) -> E,
    f: impl FnOnce(&mut Connection) -> Result<T, E>,
) -> Result<T, E> {
    fn step(conn: &Connection, sql: &str) -> rusqlite::Result<()> {
        conn.prepare_cached(sql)
            .and_then(|mut cached| cached.execute([]))
            .map(drop)
    }
    step(conn, "BEGIN").map_err(&map)?;
    let out = f(conn);
    // A read changed nothing, so COMMIT and ROLLBACK are the same; a failed one is rolled
    // back when the connection is given back.
    let end = step(conn, "COMMIT");
    match (out, end) {
        (Ok(v), Ok(())) => Ok(v),
        (Err(e), _) => Err(e),
        (Ok(_), Err(e)) => Err(map(e)),
    }
}

/// A consistent read of the store: every call through it sees the same committed state
/// ([`Store::snapshot`]).
pub struct Snapshot<'a>(pub(crate) &'a Connection);

impl Snapshot<'_> {
    /// Reads a runtime setting.
    pub fn setting(&self, key: &str) -> Result<Option<serde_json::Value>, StoreError> {
        let text: Option<String> = self
            .0
            .prepare_cached("SELECT value_json FROM settings WHERE key = ?1")?
            .query_row([key], |r| r.get(0))
            .optional()?;
        text.map(|t| serde_json::from_str(&t).map_err(|_| StoreError::BadSetting(key.to_owned())))
            .transpose()
    }
}

impl std::fmt::Debug for Store {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Store").finish_non_exhaustive()
    }
}

impl Store {
    /// Opens (creating if needed) the store at `path`, applies the pragmas, migrates the schema
    /// and checks the master key; then opens the read-only connections: [`default_readers`].
    pub fn open(path: &Path, key: &MasterKey) -> Result<Self, StoreError> {
        Self::open_with_readers(path, key, default_readers())
    }

    /// [`Store::open`] with `readers` read-only connections (at most [`MAX_READERS`]); 0 runs
    /// every read on the writer.
    pub fn open_with_readers(
        path: &Path,
        key: &MasterKey,
        readers: usize,
    ) -> Result<Self, StoreError> {
        let conn = Connection::open_with_flags(
            path,
            OpenFlags::SQLITE_OPEN_READ_WRITE
                | OpenFlags::SQLITE_OPEN_CREATE
                | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )?;
        // The writer migrates and checks the key first: the readers open the finished schema.
        let mut store = Self::init(conn, key)?;
        let idle = (0..readers.min(MAX_READERS))
            .map(|_| open_reader(path))
            .collect::<Result<Vec<_>, _>>()?;
        store.readers = Readers::new(idle);
        store.wal = Some(wal::Keeper::start(path)?);
        Ok(store)
    }

    /// An in-memory store, for tests.
    pub fn open_in_memory(key: &MasterKey) -> Result<Self, StoreError> {
        Self::init(Connection::open_in_memory()?, key)
    }

    fn init(mut conn: Connection, key: &MasterKey) -> Result<Self, StoreError> {
        conn.busy_timeout(BUSY_TIMEOUT)?;
        // `journal_mode` returns the resulting mode; in-memory databases stay `memory`.
        let _mode: String =
            conn.pragma_update_and_check(None, "journal_mode", "WAL", |r| r.get(0))?;
        conn.pragma_update(None, "synchronous", "NORMAL")?;
        // After a large transaction the WAL file shrinks back to this at the next checkpoint.
        conn.pragma_update(None, "journal_size_limit", JOURNAL_SIZE_LIMIT)?;
        // Every hot statement stays prepared: the store has more than rusqlite's default 16.
        conn.set_prepared_statement_cache_capacity(STATEMENT_CACHE);
        // Migrations run with foreign keys off (see `migrate`); they are on from here.
        migrate::migrate(&mut conn)?;
        conn.pragma_update(None, "foreign_keys", true)?;
        // ANALYZE samples at most this many rows an index (SQLite's advice for
        // `PRAGMA optimize`): a fifth of the time at 50,000 documents, the same plans (issue
        // #118). The statistics are refreshed after the open, not here ([`Store::optimize_all`]).
        conn.pragma_update(None, "analysis_limit", ANALYSIS_LIMIT)?;
        let store = Self {
            conn: Mutex::new(conn),
            readers: Readers::default(),
            cipher: Cipher::new(key),
            wal: None,
            claims: claims::ClaimView::default(),
        };
        store.check_key()?;
        Ok(store)
    }

    /// Which of this instance's sources another instance owns ([`claims::ClaimView`]).
    pub fn claims(&self) -> &claims::ClaimView {
        &self.claims
    }

    /// The writer. Every write, and every read that must see the caller's own uncommitted
    /// writes, runs here.
    pub(crate) fn conn(&self) -> MutexGuard<'_, Connection> {
        lock(&self.conn)
    }

    /// How many read-only connections the store has (0: reads use the writer).
    pub fn readers(&self) -> usize {
        self.readers.count()
    }

    /// A connection for a read: an idle reader (waiting for one when all are busy), or the
    /// writer when the store has none. Never the writer while it has readers, so reads do not
    /// wait for writes, and a flood of reads cannot hold up a write. When the instance's own
    /// reads and the API's both wait, the instance's go first ([`readers`]).
    pub(crate) fn reader(&self) -> ReadConn<'_> {
        if self.readers.count() == 0 {
            return ReadConn::Writer(self.conn());
        }
        let caller = Caller::current();
        ReadConn::Reader {
            readers: &self.readers,
            conn: Some(self.readers.take(caller)),
            caller,
        }
    }

    /// Runs the read `f` in one transaction on a reader ([`Store::reader`]).
    pub(crate) fn read<T, E>(
        &self,
        map: impl Fn(rusqlite::Error) -> E,
        f: impl FnOnce(&mut Connection) -> Result<T, E>,
    ) -> Result<T, E> {
        read_tx(&mut self.reader(), map, f)
    }

    /// Runs `f` on one consistent read of the store, on a reader: what several calls read
    /// together (every source's progress, say) comes from the same committed state.
    pub fn snapshot<T>(
        &self,
        f: impl FnOnce(&Snapshot<'_>) -> Result<T, StoreError>,
    ) -> Result<T, StoreError> {
        self.read(StoreError::from, |conn| f(&Snapshot(conn)))
    }

    fn check_key(&self) -> Result<(), StoreError> {
        match self.read_sealed(KEY_CHECK) {
            Ok(Some(plain)) if plain.as_slice() == KEY_CHECK_VALUE => Ok(()),
            Ok(Some(_)) | Err(StoreError::Decrypt(_)) => Err(StoreError::WrongMasterKey),
            Ok(None) => self.write_sealed(KEY_CHECK, KEY_CHECK_VALUE),
            Err(e) => Err(e),
        }
    }

    /// The schema version of the open file.
    pub fn schema_version(&self) -> Result<i64, StoreError> {
        Ok(self
            .conn()
            .pragma_query_value(None, "user_version", |r| r.get(0))?)
    }

    /// The tables of the schema, sorted.
    pub fn tables(&self) -> Result<Vec<String>, StoreError> {
        let conn = self.conn();
        let mut stmt = conn.prepare(
            "SELECT name FROM sqlite_schema WHERE type = 'table' AND name NOT LIKE 'sqlite_%' \
             ORDER BY name",
        )?;
        let names = stmt
            .query_map([], |r| r.get(0))?
            .collect::<Result<Vec<String>, _>>()?;
        Ok(names)
    }

    /// Reads a runtime setting.
    pub fn setting(&self, key: &str) -> Result<Option<serde_json::Value>, StoreError> {
        self.snapshot(|s| s.setting(key))
    }

    /// Writes a runtime setting. Secrets never go here: use [`Store::set_secret`].
    pub fn set_setting(&self, key: &str, value: &serde_json::Value) -> Result<(), StoreError> {
        self.conn().execute(
            "INSERT INTO settings (key, value_json, updated_at) VALUES (?1, ?2, ?3) \
             ON CONFLICT (key) DO UPDATE SET value_json = excluded.value_json, \
             updated_at = excluded.updated_at",
            params![key, value.to_string(), now()],
        )?;
        Ok(())
    }

    /// Seals and stores a secret, replacing any previous value. There is no way to read it
    /// back through the public API.
    pub fn set_secret(&self, name: &str, value: &[u8]) -> Result<(), StoreError> {
        validate_secret_name(name)?;
        if value.len() > SECRET_MAX_LEN {
            return Err(StoreError::SecretTooLarge(value.len()));
        }
        self.write_sealed(name, value)
    }

    /// Deletes a secret. Returns whether it existed.
    pub fn delete_secret(&self, name: &str) -> Result<bool, StoreError> {
        validate_secret_name(name)?;
        let n = self
            .conn()
            .execute("DELETE FROM secrets WHERE name = ?1", [name])?;
        Ok(n > 0)
    }

    /// Whether a secret with this name is stored.
    pub fn secret_exists(&self, name: &str) -> Result<bool, StoreError> {
        validate_secret_name(name)?;
        self.read(StoreError::from, |conn| {
            Ok(conn
                .prepare_cached("SELECT 1 FROM secrets WHERE name = ?1")?
                .query_row([name], |_| Ok(()))
                .optional()?
                .is_some())
        })
    }

    /// The names of the stored secrets, sorted. Names only, never values.
    pub fn secret_names(&self) -> Result<Vec<String>, StoreError> {
        self.read(StoreError::from, |conn| {
            let mut stmt = conn.prepare_cached(
                "SELECT name FROM secrets WHERE name NOT LIKE '\\_%' ESCAPE '\\' ORDER BY name",
            )?;
            let names = stmt
                .query_map([], |r| r.get(0))?
                .collect::<Result<Vec<String>, _>>()?;
            Ok(names)
        })
    }

    /// Decrypts a secret. Crate-internal: the connectors (S3 credentials, AI provider keys)
    /// reach secrets through this, and the API never does.
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn reveal(&self, name: &str) -> Result<Option<Zeroizing<Vec<u8>>>, StoreError> {
        validate_secret_name(name)?;
        self.read_sealed(name)
    }

    fn write_sealed(&self, name: &str, value: &[u8]) -> Result<(), StoreError> {
        let (nonce, ciphertext) = self.cipher.seal(name, value)?;
        let now = now();
        self.conn().execute(
            "INSERT INTO secrets (name, nonce, ciphertext, created_at, updated_at) \
             VALUES (?1, ?2, ?3, ?4, ?4) \
             ON CONFLICT (name) DO UPDATE SET nonce = excluded.nonce, \
             ciphertext = excluded.ciphertext, updated_at = excluded.updated_at",
            params![name, nonce.as_slice(), ciphertext, now],
        )?;
        Ok(())
    }

    /// Reads and opens a sealed secret, on a reader: the cipher is the store's, whichever
    /// connection read the row. (At open, before the readers exist, the key check reads on the
    /// writer.)
    fn read_sealed(&self, name: &str) -> Result<Option<Zeroizing<Vec<u8>>>, StoreError> {
        let row: Option<(Vec<u8>, Vec<u8>)> = self.read(StoreError::from, |conn| {
            Ok(conn
                .prepare_cached("SELECT nonce, ciphertext FROM secrets WHERE name = ?1")?
                .query_row([name], |r| Ok((r.get(0)?, r.get(1)?)))
                .optional()?)
        })?;
        row.map(|(nonce, ct)| self.cipher.open(name, &nonce, &ct))
            .transpose()
    }

    /// Refreshes the query planner's statistics of every table that needs it, also those no
    /// query used yet on this connection: what `PRAGMA optimize = 0x10002` does (SQLite's
    /// advice for a long-lived connection after it opens, then [`Store::optimize`] every few
    /// hours), one table at a time.
    ///
    /// On a store closed cleanly (the close optimizes) there is nothing to do. After a crash or
    /// a bulk load the tables are analyzed: sampled ([`ANALYSIS_LIMIT`]), but every page of
    /// their indexes is still read, which takes seconds when the file is not in the page cache
    /// (17 s at 50,000 documents). So each table's pages are read first on a connection of its
    /// own, without the writer, and the writer is held for one table's `ANALYZE` at a time,
    /// over pages then cached (issue #118). The binary runs it once right after the start, in
    /// the background, so the instance is ready without waiting for it.
    pub fn optimize_all(&self) -> Result<Optimized, StoreError> {
        let started = Instant::now();
        // Mask 0x10003: what 0x10002 would run, one `ANALYZE` per line, without running it.
        let (statements, path) = {
            let conn = self.conn();
            let statements = conn
                .prepare("PRAGMA optimize = 0x10003")?
                .query_map([], |r| r.get::<_, String>(0))?
                .collect::<Result<Vec<_>, _>>()?;
            (statements, conn.path().map(PathBuf::from))
        };
        let mut done = Optimized::default();
        // An in-memory store has nothing to read ahead.
        let warm = match &path {
            Some(p) if !p.as_os_str().is_empty() => Some(open_reader(p)?),
            _ => None,
        };
        for statement in statements {
            let Some(table) = analyzed_table(&statement) else {
                continue;
            };
            let t = Instant::now();
            if let Some(warm) = &warm {
                read_ahead(warm, table);
            }
            let ahead = t.elapsed();
            let t = Instant::now();
            self.conn().execute_batch(&statement)?;
            done.steps.push((table.to_owned(), ahead, t.elapsed()));
            if t.elapsed() >= done.longest_hold {
                done.longest_hold = t.elapsed();
                done.slowest = Some(table.to_owned());
            }
            done.tables += 1;
        }
        done.took = started.elapsed();
        Ok(done)
    }

    /// Refreshes the query planner's statistics where they are stale (`PRAGMA optimize`);
    /// the binary calls it every hour.
    pub fn optimize(&self) -> Result<(), StoreError> {
        self.conn().execute_batch("PRAGMA optimize;")?;
        Ok(())
    }

    /// Copies the WAL into the main file and empties it (`PRAGMA wal_checkpoint(TRUNCATE)`),
    /// waiting for readers as any write does. Returns the WAL's frames and how many of them
    /// were already in the main file before.
    pub fn checkpoint(&self) -> Result<(i64, i64), StoreError> {
        let conn = self.conn();
        let before = conn.query_row("PRAGMA wal_checkpoint(PASSIVE)", [], |r| {
            Ok((r.get(1)?, r.get(2)?))
        })?;
        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")?;
        Ok(before)
    }

    /// Closes the readers, then checkpoints the WAL into the main file and closes the writer.
    pub fn close(self) -> Result<(), StoreError> {
        // The checkpoint thread first: the close's own checkpoint does not wait for it.
        drop(self.wal);
        for reader in self.readers.into_idle() {
            reader.close().map_err(|(_, e)| StoreError::Sqlite(e))?;
        }
        let conn = self
            .conn
            .into_inner()
            .unwrap_or_else(PoisonError::into_inner);
        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE); PRAGMA optimize;")?;
        conn.close().map_err(|(_, e)| StoreError::Sqlite(e))
    }

    #[cfg(test)]
    pub(crate) fn raw(&self) -> MutexGuard<'_, Connection> {
        self.conn()
    }
}

/// What [`Store::optimize_all`] did.
#[derive(Clone, Debug, Default)]
pub struct Optimized {
    /// Tables analyzed.
    pub tables: usize,
    /// The longest the writer was held, for one table.
    pub longest_hold: Duration,
    /// That table.
    pub slowest: Option<String>,
    /// Each table analyzed: its read ahead (without the writer) and its `ANALYZE`.
    pub steps: Vec<(String, Duration, Duration)>,
    /// The whole refresh.
    pub took: Duration,
}

/// The table of a statement `PRAGMA optimize` lists: `ANALYZE "main"."records"`.
fn analyzed_table(statement: &str) -> Option<&str> {
    statement
        .strip_prefix("ANALYZE \"main\".\"")?
        .strip_suffix('"')
        .filter(|t| !t.contains('"'))
}

/// Reads every page of the b-trees `ANALYZE` reads for `table` on `conn` (through `dbstat`),
/// so they are in the page cache when the writer analyzes it: each index, and the table's own
/// b-tree when it has no index or is a WITHOUT ROWID table (its primary key). A plain
/// `count(*)` would not do: SQLite counts on the smallest b-tree only. Only a read ahead: a
/// failure costs nothing but time.
fn read_ahead(conn: &Connection, table: &str) {
    let without_rowid: bool = conn
        .query_row(
            "SELECT wr FROM pragma_table_list WHERE schema = 'main' AND name = ?1",
            [table],
            |r| r.get(0),
        )
        .unwrap_or(false);
    // A WITHOUT ROWID table's primary key is the table's own b-tree.
    let mut btrees: Vec<String> = conn
        .prepare("SELECT name FROM pragma_index_list(?1) WHERE NOT (?2 AND origin = 'pk')")
        .and_then(|mut stmt| {
            stmt.query_map(params![table, without_rowid], |r| r.get(0))?
                .collect::<Result<Vec<_>, _>>()
        })
        .unwrap_or_default();
    if without_rowid || btrees.is_empty() {
        btrees.push(table.to_owned());
    }
    for name in btrees {
        let _ = conn.query_row(
            "SELECT count(*) FROM dbstat WHERE name = ?1",
            [&name],
            |r| r.get::<_, i64>(0),
        );
    }
}

/// A read-only connection to the store at `path`, with the writer's statement cache and busy
/// timeout. `SQLITE_OPEN_READ_ONLY` and `query_only` both refuse writes.
fn open_reader(path: &Path) -> Result<Connection, StoreError> {
    let conn = Connection::open_with_flags(
        path,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )?;
    conn.busy_timeout(BUSY_TIMEOUT)?;
    conn.pragma_update(None, "query_only", true)?;
    conn.set_prepared_statement_cache_capacity(STATEMENT_CACHE);
    Ok(conn)
}

/// Secret names: 1 to 128 ASCII letters, digits, `.`, `_`, `-` or `/`; a leading `_` is
/// reserved for doewood itself.
fn validate_secret_name(name: &str) -> Result<(), StoreError> {
    let bad = |reason| StoreError::InvalidSecretName {
        name: name.to_owned(),
        reason,
    };
    if name.is_empty() || name.len() > 128 {
        return Err(bad("must be 1 to 128 characters"));
    }
    if name.starts_with('_') {
        return Err(bad("names starting with '_' are reserved"));
    }
    if !name
        .bytes()
        .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-' | b'/'))
    {
        return Err(bad(
            "only ASCII letters, digits, '.', '_', '-' and '/' are allowed",
        ));
    }
    Ok(())
}

/// Stores an already sealed secret inside a caller's transaction.
pub(crate) fn write_sealed_tx(
    conn: &Connection,
    name: &str,
    nonce: &[u8],
    ciphertext: &[u8],
    now: i64,
) -> Result<(), StoreError> {
    conn.execute(
        "INSERT INTO secrets (name, nonce, ciphertext, created_at, updated_at) \
         VALUES (?1, ?2, ?3, ?4, ?4) \
         ON CONFLICT (name) DO UPDATE SET nonce = excluded.nonce, \
         ciphertext = excluded.ciphertext, updated_at = excluded.updated_at",
        params![name, nonce, ciphertext, now],
    )?;
    Ok(())
}

pub(crate) fn now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| i64::try_from(d.as_secs()).unwrap_or(i64::MAX))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::SCHEMA_VERSION;
    use crate::api_call_blocking;

    fn key(b: u8) -> MasterKey {
        MasterKey::from_bytes([b; 32])
    }

    /// The open leaves stale planner statistics alone (issue #118: seconds on a big store);
    /// `optimize_all` refreshes them, a table at a time, sampled.
    #[test]
    fn statistics_are_refreshed_after_the_open_not_during_it() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join(STORE_FILE);
        let stats = |s: &Store| -> i64 {
            let conn = s.conn();
            let exists: i64 = conn
                .query_row(
                    "SELECT count(*) FROM sqlite_master WHERE name = 'sqlite_stat1'",
                    [],
                    |r| r.get(0),
                )
                .unwrap();
            if exists == 0 {
                return 0;
            }
            conn.query_row(
                "SELECT count(*) FROM sqlite_stat1 WHERE tbl = 'records'",
                [],
                |r| r.get(0),
            )
            .unwrap()
        };
        {
            let store = Store::open(&path, &key(1)).unwrap();
            let conn = store.conn();
            for i in 0..2000_u32 {
                let mut hash = [0_u8; 32];
                hash[..4].copy_from_slice(&i.to_be_bytes());
                conn.execute(
                    "INSERT INTO records (blake3, body_json, updated_at) VALUES (?1, '{}', 1)",
                    [hash.as_slice()],
                )
                .unwrap();
            }
            // Dropped without `close` (which optimizes), as after a crash.
        }
        let store = Store::open(&path, &key(1)).unwrap();
        assert_eq!(stats(&store), 0, "the open analyzed");
        let limit: i64 = store
            .conn()
            .query_row("PRAGMA analysis_limit", [], |r| r.get(0))
            .unwrap();
        assert_eq!(limit, ANALYSIS_LIMIT);
        let done = store.optimize_all().unwrap();
        assert!(stats(&store) > 0, "not analyzed");
        assert!(done.tables > 0, "{done:?}");
        // The analyzed tables are not analyzed again.
        let again = store.optimize_all().unwrap();
        assert!(again.tables < done.tables, "{again:?} after {done:?}");
        assert!(
            again.steps.iter().all(|(t, _, _)| t != "records"),
            "{again:?}"
        );
    }

    #[test]
    fn migration_creates_the_schema() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join(STORE_FILE);
        let store = Store::open(&path, &key(1)).unwrap();
        assert_eq!(store.schema_version().unwrap(), SCHEMA_VERSION);
        assert_eq!(
            store.tables().unwrap(),
            [
                "company_dismissals",
                "company_enrichment",
                "company_leases",
                "company_merges",
                "company_proposals",
                "company_registry_due",
                "edges",
                "favicons",
                "jobs",
                "known_content",
                "llm_job_counts",
                "llm_jobs",
                "llm_usage",
                "locations",
                "node_aliases",
                "nodes",
                "object_counts",
                "objects_seen",
                "records",
                "registry_cache",
                "registry_feeds",
                "scan_checkpoints",
                "search_fts",
                "search_fts_config",
                "search_fts_data",
                "search_fts_docsize",
                "search_fts_idx",
                "search_index",
                "secrets",
                "settings",
                "sketch_band_sizes",
                "sketch_bands",
                "sketches",
                "source_claims",
                "sources"
            ]
        );
        let mode: String = store
            .raw()
            .pragma_query_value(None, "journal_mode", |r| r.get(0))
            .unwrap();
        assert_eq!(mode, "wal");
        store.close().unwrap();
        // Re-opening is a no-op migration and keeps the data.
        let store = Store::open(&path, &key(1)).unwrap();
        assert_eq!(store.schema_version().unwrap(), SCHEMA_VERSION);
        store.close().unwrap();
    }

    #[test]
    fn schema_columns_and_constraints() {
        let store = Store::open_in_memory(&key(1)).unwrap();
        let conn = store.raw();
        let cols = |table: &str| -> Vec<String> {
            let mut s = conn
                .prepare(&format!("PRAGMA table_info({table})"))
                .unwrap();
            s.query_map([], |r| r.get(1))
                .unwrap()
                .map(Result::unwrap)
                .collect()
        };
        assert_eq!(cols("settings"), ["key", "value_json", "updated_at"]);
        assert_eq!(
            cols("secrets"),
            ["name", "nonce", "ciphertext", "created_at", "updated_at"]
        );
        assert_eq!(
            cols("sources"),
            [
                "id",
                "endpoint",
                "bucket",
                "prefix",
                "region",
                "url_style",
                "credentials_secret",
                "max_object_bytes",
                "policy_json",
                "state",
                "state_reason",
                "scan_interval_s",
                "created_at",
                "updated_at",
                "skip_known_copies"
            ]
        );
        assert_eq!(
            cols("objects_seen"),
            [
                "source_id",
                "key",
                "etag",
                "size",
                "last_modified",
                "blake3",
                "status",
                "attempts",
                "last_error",
                "updated_at"
            ]
        );
        conn.execute(
            "INSERT INTO sources (id, endpoint, bucket, created_at) VALUES ('s1', 'http://minio', 'b', 0)",
            [],
        )
        .unwrap();
        // The same endpoint, bucket and prefix cannot be added twice.
        assert!(
            conn.execute(
                "INSERT INTO sources (id, endpoint, bucket, created_at) VALUES ('s2', 'http://minio', 'b', 0)",
                [],
            )
            .is_err()
        );
        conn.execute(
            "INSERT INTO objects_seen (source_id, key, size, updated_at) VALUES ('s1', 'a.pdf', 10, 0)",
            [],
        )
        .unwrap();
        // Foreign keys are on: progress of an unknown source is refused.
        assert!(
            conn.execute(
                "INSERT INTO objects_seen (source_id, key, size, updated_at) VALUES ('nope', 'a', 1, 0)",
                [],
            )
            .is_err()
        );
        // Removing a source removes its progress.
        conn.execute("DELETE FROM sources WHERE id = 's1'", [])
            .unwrap();
        let n: i64 = conn
            .query_row("SELECT count(*) FROM objects_seen", [], |r| r.get(0))
            .unwrap();
        assert_eq!(n, 0);
    }

    #[test]
    fn a_newer_schema_is_refused() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join(STORE_FILE);
        Store::open(&path, &key(1)).unwrap().close().unwrap();
        let conn = Connection::open(&path).unwrap();
        conn.pragma_update(None, "user_version", SCHEMA_VERSION + 1)
            .unwrap();
        drop(conn);
        assert!(matches!(
            Store::open(&path, &key(1)),
            Err(StoreError::SchemaTooNew { .. })
        ));
    }

    #[test]
    fn settings_round_trip() {
        let store = Store::open_in_memory(&key(1)).unwrap();
        assert_eq!(store.setting("llm.policy").unwrap(), None);
        let v = serde_json::json!({"mode": "off"});
        store.set_setting("llm.policy", &v).unwrap();
        assert_eq!(store.setting("llm.policy").unwrap(), Some(v));
        store
            .set_setting("llm.policy", &serde_json::json!(1))
            .unwrap();
        assert_eq!(
            store.setting("llm.policy").unwrap(),
            Some(serde_json::json!(1))
        );
    }

    #[test]
    fn secrets_are_write_only_and_encrypted_at_rest() {
        let store = Store::open_in_memory(&key(1)).unwrap();
        assert!(!store.secret_exists("s3/main").unwrap());
        store.set_secret("s3/main", b"very-secret-value").unwrap();
        assert!(store.secret_exists("s3/main").unwrap());
        assert_eq!(store.secret_names().unwrap(), ["s3/main"]);
        assert_eq!(
            store.reveal("s3/main").unwrap().unwrap().as_slice(),
            b"very-secret-value"
        );
        // The plaintext is not in the table.
        let ct: Vec<u8> = store
            .raw()
            .query_row(
                "SELECT ciphertext FROM secrets WHERE name = 's3/main'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert!(!ct.windows(17).any(|w| w == b"very-secret-value"));
        // Overwrite, then delete.
        store.set_secret("s3/main", b"v2").unwrap();
        assert_eq!(store.reveal("s3/main").unwrap().unwrap().as_slice(), b"v2");
        assert!(store.delete_secret("s3/main").unwrap());
        assert!(!store.delete_secret("s3/main").unwrap());
        assert_eq!(store.reveal("s3/main").unwrap(), None);
    }

    #[test]
    fn tampered_secret_rows_do_not_open() {
        let store = Store::open_in_memory(&key(1)).unwrap();
        store.set_secret("a", b"alpha").unwrap();
        store.set_secret("b", b"beta").unwrap();
        // A ciphertext moved to another row does not open (the name is bound).
        store
            .raw()
            .execute(
                "UPDATE secrets SET ciphertext = (SELECT ciphertext FROM secrets WHERE name = 'b') \
                 WHERE name = 'a'",
                [],
            )
            .unwrap();
        assert!(matches!(store.reveal("a"), Err(StoreError::Decrypt(_))));
        let mut ct: Vec<u8> = store
            .raw()
            .query_row("SELECT ciphertext FROM secrets WHERE name = 'b'", [], |r| {
                r.get(0)
            })
            .unwrap();
        ct[0] ^= 1;
        store
            .raw()
            .execute("UPDATE secrets SET ciphertext = ?1 WHERE name = 'b'", [ct])
            .unwrap();
        assert!(matches!(store.reveal("b"), Err(StoreError::Decrypt(_))));
    }

    #[test]
    fn secret_names_and_sizes_are_checked() {
        let store = Store::open_in_memory(&key(1)).unwrap();
        for bad in ["", "_doewood.key_check", "has space", &"a".repeat(129)] {
            assert!(store.set_secret(bad, b"x").is_err(), "{bad:?}");
        }
        assert!(store.secret_exists("_doewood.key_check").is_err());
        assert!(matches!(
            store.set_secret("big", &vec![0; SECRET_MAX_LEN + 1]),
            Err(StoreError::SecretTooLarge(_))
        ));
        // The reserved key check is never listed.
        assert!(store.secret_names().unwrap().is_empty());
    }

    #[test]
    fn a_different_master_key_is_refused() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join(STORE_FILE);
        Store::open(&path, &key(1)).unwrap().close().unwrap();
        assert!(matches!(
            Store::open(&path, &key(2)),
            Err(StoreError::WrongMasterKey)
        ));
        Store::open(&path, &key(1)).unwrap().close().unwrap();
    }

    fn on_disk(readers: usize) -> (tempfile::TempDir, Store) {
        let tmp = tempfile::tempdir().unwrap();
        let store =
            Store::open_with_readers(&tmp.path().join(STORE_FILE), &key(1), readers).unwrap();
        (tmp, store)
    }

    #[test]
    fn readers_are_for_on_disk_stores_only() {
        let tmp = tempfile::tempdir().unwrap();
        let store = Store::open(&tmp.path().join(STORE_FILE), &key(1)).unwrap();
        assert_eq!(store.readers(), default_readers());
        store.close().unwrap();
        assert_eq!(Store::open_in_memory(&key(1)).unwrap().readers(), 0);
        assert_eq!(on_disk(0).1.readers(), 0);
        assert_eq!(on_disk(MAX_READERS + 5).1.readers(), MAX_READERS);
        // With no readers, reads use the writer.
        let (_tmp, store) = on_disk(0);
        store.set_setting("k", &serde_json::json!(1)).unwrap();
        assert_eq!(store.setting("k").unwrap(), Some(serde_json::json!(1)));
    }

    #[test]
    fn readers_follow_the_cpus() {
        for (cpus, readers) in [(0, 1), (1, 1), (2, 2), (4, 4), (16, 8), (64, 8)] {
            assert_eq!(plan_readers(cpus), readers, "{cpus} CPUs");
        }
        let cpus = std::thread::available_parallelism().map_or(1, NonZeroUsize::get);
        assert_eq!(default_readers(), plan_readers(cpus));
        const { assert!(DEFAULT_READERS <= MAX_READERS) };
    }

    #[test]
    fn readers_refuse_writes() {
        let (_tmp, store) = on_disk(2);
        let reader = store.reader();
        assert!(matches!(&reader, ReadConn::Reader { .. }));
        let query_only: bool = reader
            .pragma_query_value(None, "query_only", |r| r.get(0))
            .unwrap();
        assert!(query_only);
        let err = reader
            .execute(
                "INSERT INTO settings (key, value_json, updated_at) VALUES ('x', '1', 0)",
                [],
            )
            .unwrap_err();
        assert!(
            matches!(err.sqlite_error_code(), Some(rusqlite::ErrorCode::ReadOnly)),
            "{err}"
        );
        drop(reader);
        assert_eq!(store.setting("x").unwrap(), None);
    }

    #[test]
    fn readers_see_committed_writes_and_do_not_wait_for_the_writer() {
        let (_tmp, store) = on_disk(2);
        store.set_setting("a", &serde_json::json!("one")).unwrap();
        assert_eq!(store.setting("a").unwrap(), Some(serde_json::json!("one")));
        // A long write transaction holds the writer.
        let writer = store.conn();
        writer
            .execute_batch(
                "BEGIN IMMEDIATE; UPDATE settings SET value_json = '\"two\"' WHERE key = 'a';",
            )
            .unwrap();
        // Reads on other threads go on at once and see the last committed state.
        std::thread::scope(|s| {
            let reads: Vec<_> = (0..4)
                .map(|_| {
                    s.spawn(|| {
                        let t = std::time::Instant::now();
                        let v = store.setting("a").unwrap();
                        (v, t.elapsed())
                    })
                })
                .collect();
            for r in reads {
                let (v, took) = r.join().unwrap();
                assert_eq!(v, Some(serde_json::json!("one")));
                assert!(took < Duration::from_secs(2), "a read waited {took:?}");
            }
        });
        writer.execute_batch("COMMIT").unwrap();
        drop(writer);
        assert_eq!(store.setting("a").unwrap(), Some(serde_json::json!("two")));
    }

    #[test]
    fn a_snapshot_reads_one_committed_state() {
        let (_tmp, store) = on_disk(2);
        store.set_setting("n", &serde_json::json!(1)).unwrap();
        let (before, after) = store
            .read(StoreError::from, |conn| {
                let get = |conn: &Connection| -> rusqlite::Result<String> {
                    conn.query_row("SELECT value_json FROM settings WHERE key = 'n'", [], |r| {
                        r.get(0)
                    })
                };
                let before = get(conn)?;
                // A write commits on the writer meanwhile.
                store.set_setting("n", &serde_json::json!(2))?;
                Ok((before, get(conn)?))
            })
            .unwrap();
        assert_eq!((before.as_str(), after.as_str()), ("1", "1"));
        assert_eq!(store.setting("n").unwrap(), Some(serde_json::json!(2)));
    }

    #[test]
    fn a_failed_or_panicking_read_gives_its_reader_back_clean() {
        let (_tmp, store) = on_disk(2);
        for _ in 0..3 {
            let out: Result<(), StoreError> = store.read(StoreError::from, |_| {
                Err(StoreError::BadSetting("x".into()))
            });
            assert!(out.is_err());
            let panicked = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                let _: Result<(), StoreError> = store.read(StoreError::from, |_| panic!("boom"));
            }));
            assert!(panicked.is_err());
        }
        // Every reader is idle, outside a transaction, and serves reads.
        assert_eq!(store.readers.state().0, 2);
        assert!(store.readers.each_idle(Connection::is_autocommit));
        store.set_setting("k", &serde_json::json!(true)).unwrap();
        for _ in 0..4 {
            assert_eq!(store.setting("k").unwrap(), Some(serde_json::json!(true)));
        }
    }

    #[test]
    fn a_read_waits_for_a_free_reader_not_for_the_writer() {
        let (_tmp, store) = on_disk(1);
        let held = store.reader();
        std::thread::scope(|s| {
            let read = s.spawn(|| store.setting("k").unwrap());
            std::thread::sleep(Duration::from_millis(100));
            assert!(!read.is_finished(), "the only reader is taken");
            drop(held);
            assert_eq!(read.join().unwrap(), None);
        });
    }

    /// Waits until the readers' state is `want`: (idle, instance reads waiting, API reads
    /// waiting).
    fn until(store: &Store, want: (usize, usize, usize)) {
        let start = std::time::Instant::now();
        while store.readers.state() != want {
            assert!(
                start.elapsed() < Duration::from_secs(10),
                "readers {:?}, want {want:?}",
                store.readers.state()
            );
            std::thread::sleep(Duration::from_millis(1));
        }
    }

    /// A read that notes `name` in `order` while it holds its connection.
    fn noted(store: &Store, order: &Mutex<Vec<&'static str>>, name: &'static str) {
        store
            .read(StoreError::from, |_| {
                lock(order).push(name);
                Ok(())
            })
            .unwrap();
    }

    #[test]
    fn an_instance_read_goes_before_waiting_api_reads() {
        let (_tmp, store) = on_disk(2);
        let order = Mutex::new(Vec::new());
        // The API holds both readers, and more of its reads wait.
        let mut held: Vec<_> = (0..2)
            .map(|_| api_call_blocking(|| store.reader()))
            .collect();
        std::thread::scope(|s| {
            for _ in 0..3 {
                s.spawn(|| api_call_blocking(|| noted(&store, &order, "api")));
            }
            until(&store, (0, 0, 3));
            let instance = s.spawn(|| noted(&store, &order, "instance"));
            until(&store, (0, 1, 3));
            // The reader given back goes to the instance's read, not to the API's.
            held.pop();
            instance.join().unwrap();
            held.pop();
        });
        assert_eq!(lock(&order).as_slice(), ["instance", "api", "api", "api"]);
    }

    #[test]
    fn an_api_read_holding_no_reader_gets_the_last_free_one() {
        let (_tmp, store) = on_disk(2);
        let order = Mutex::new(Vec::new());
        // Instance reads hold both readers, and more of them wait.
        let mut held: Vec<_> = (0..2).map(|_| store.reader()).collect();
        std::thread::scope(|s| {
            s.spawn(|| api_call_blocking(|| noted(&store, &order, "api")));
            until(&store, (0, 0, 1));
            for _ in 0..2 {
                s.spawn(|| noted(&store, &order, "instance"));
            }
            until(&store, (0, 2, 1));
            // The API holds none: the one reader given back goes to it first.
            held.pop();
        });
        drop(held);
        assert_eq!(lock(&order).as_slice(), ["api", "instance", "instance"]);
    }

    #[test]
    fn under_a_flood_of_reads_of_both_kinds_both_progress() {
        use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
        let (_tmp, store) = on_disk(2);
        store.set_setting("k", &serde_json::json!(1)).unwrap();
        let stop = AtomicBool::new(false);
        let (instance, api) = (AtomicU64::new(0), AtomicU64::new(0));
        let slow_read = |done: &AtomicU64| {
            store
                .read(StoreError::from, |conn| {
                    let _: i64 = conn.query_row("SELECT 1", [], |r| r.get(0))?;
                    std::thread::sleep(Duration::from_millis(1));
                    Ok(())
                })
                .unwrap();
            done.fetch_add(1, Ordering::Relaxed);
        };
        std::thread::scope(|s| {
            for _ in 0..3 {
                s.spawn(|| {
                    while !stop.load(Ordering::Relaxed) {
                        slow_read(&instance);
                    }
                });
            }
            for _ in 0..8 {
                s.spawn(|| {
                    api_call_blocking(|| {
                        while !stop.load(Ordering::Relaxed) {
                            slow_read(&api);
                        }
                    });
                });
            }
            // Each kind gets on: 50 reads of each, though the other floods the readers.
            let start = std::time::Instant::now();
            while instance.load(Ordering::Relaxed) < 50 || api.load(Ordering::Relaxed) < 50 {
                assert!(
                    start.elapsed() < Duration::from_secs(30),
                    "instance reads {instance:?}, API reads {api:?}"
                );
                std::thread::sleep(Duration::from_millis(5));
            }
            stop.store(true, Ordering::Relaxed);
        });
        // Every reader is back.
        assert_eq!(store.readers.state(), (2, 0, 0));
    }

    #[test]
    fn api_calls_mark_their_reads() {
        assert_eq!(Caller::current(), Caller::Instance);
        api_call_blocking(|| {
            assert_eq!(Caller::current(), Caller::Api);
            // Nested, and restored on the way out.
            api_call_blocking(|| assert_eq!(Caller::current(), Caller::Api));
            assert_eq!(Caller::current(), Caller::Api);
        });
        assert_eq!(Caller::current(), Caller::Instance);
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        rt.block_on(async {
            assert_eq!(Caller::current(), Caller::Instance);
            crate::api_call(async { assert_eq!(Caller::current(), Caller::Api) }).await;
            assert_eq!(Caller::current(), Caller::Instance);
        });
    }

    #[test]
    fn secrets_open_on_readers() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join(STORE_FILE);
        let store = Store::open_with_readers(&path, &key(1), 2).unwrap();
        store.set_secret("s3/main", b"very-secret-value").unwrap();
        assert!(matches!(store.reader(), ReadConn::Reader { .. }));
        assert!(store.secret_exists("s3/main").unwrap());
        assert_eq!(store.secret_names().unwrap(), ["s3/main"]);
        assert_eq!(
            store.reveal("s3/main").unwrap().unwrap().as_slice(),
            b"very-secret-value"
        );
        store.close().unwrap();
        // The key check still runs (on the writer, before the readers open).
        assert!(matches!(
            Store::open_with_readers(&path, &key(2), 2),
            Err(StoreError::WrongMasterKey)
        ));
        let store = Store::open_with_readers(&path, &key(1), 2).unwrap();
        assert_eq!(
            store.reveal("s3/main").unwrap().unwrap().as_slice(),
            b"very-secret-value"
        );
        store.close().unwrap();
    }

    /// What each reader adds to the process's memory once its page cache is full: a store of
    /// about 40 MiB is read in full on every connection at once, then the resident set is
    /// compared with the same reads on the writer alone.
    /// `cargo test -p doewood-store --lib -- --ignored --nocapture reader_memory`
    /// (`DOEWOOD_BENCH_READERS` sets the readers, default 2).
    #[test]
    #[ignore = "a measurement, not a check"]
    fn reader_memory() {
        fn rss_kib() -> u64 {
            let statm = std::fs::read_to_string("/proc/self/statm").unwrap_or_default();
            let pages: u64 = statm
                .split_whitespace()
                .nth(1)
                .and_then(|v| v.parse().ok())
                .unwrap_or(0);
            pages * 4
        }
        let n: usize = std::env::var("DOEWOOD_BENCH_READERS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(2);
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join(STORE_FILE);
        {
            let store = Store::open_with_readers(&path, &key(1), 0).unwrap();
            let mut conn = store.conn();
            let tx = conn.transaction().unwrap();
            let body = "x".repeat(4000);
            for i in 0..10_000 {
                tx.execute(
                    "INSERT INTO settings (key, value_json, updated_at) VALUES (?1, ?2, 0)",
                    params![format!("k{i:06}"), serde_json::json!(body).to_string()],
                )
                .unwrap();
            }
            tx.commit().unwrap();
            drop(conn);
            store.close().unwrap();
        }
        let scan = |conn: &Connection| {
            let mut stmt = conn
                .prepare_cached("SELECT sum(length(value_json)) FROM settings")
                .unwrap();
            let _: i64 = stmt.query_row([], |r| r.get(0)).unwrap();
        };
        let store = Store::open_with_readers(&path, &key(1), n).unwrap();
        let base = rss_kib();
        scan(&store.conn());
        let writer = rss_kib();
        // Every reader at once, so each fills its own cache.
        let held: Vec<ReadConn<'_>> = (0..n).map(|_| store.reader()).collect();
        for conn in &held {
            scan(conn);
        }
        let all = rss_kib();
        drop(held);
        #[allow(clippy::cast_precision_loss)]
        let per = if n == 0 {
            0.0
        } else {
            (all.saturating_sub(writer)) as f64 / n as f64 / 1024.0
        };
        println!(
            "reader_memory: {n} readers; RSS after open {base} KiB, writer warm {writer} KiB \
             (+{} KiB), all warm {all} KiB: {per:.2} MiB per reader",
            writer.saturating_sub(base)
        );
        store.close().unwrap();
    }
}
