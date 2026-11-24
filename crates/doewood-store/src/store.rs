use std::path::Path;
use std::sync::{Mutex, MutexGuard, PoisonError};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use doewood_core::MasterKey;
use rusqlite::{Connection, OpenFlags, OptionalExtension as _, params};
use zeroize::Zeroizing;

use crate::crypto::Cipher;
use crate::{StoreError, migrate};

pub(crate) mod ai;
pub(crate) mod objects;
pub(crate) mod sources;

/// Name of the store file inside the data directory.
pub const STORE_FILE: &str = "doewood.sqlite";

/// Largest secret value accepted, in bytes.
pub const SECRET_MAX_LEN: usize = 64 * 1024;

/// How long a statement waits for a lock held by another connection (a backup tool,
/// Litestream, the `sqlite3` shell) before it fails.
const BUSY_TIMEOUT: Duration = Duration::from_secs(5);

/// Reserved secret that proves the master key is the one the store was created with.
const KEY_CHECK: &str = "_doewood.key_check";
const KEY_CHECK_VALUE: &[u8] = b"doewood key check v1";

/// The instance-local store. Cheap to share behind an `Arc`; every call takes the single
/// connection's lock, so there is exactly one writer.
pub struct Store {
    conn: Mutex<Connection>,
    cipher: Cipher,
}

impl std::fmt::Debug for Store {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Store").finish_non_exhaustive()
    }
}

impl Store {
    /// Opens (creating if needed) the store at `path`, applies the pragmas, migrates the schema
    /// and checks the master key.
    pub fn open(path: &Path, key: &MasterKey) -> Result<Self, StoreError> {
        let conn = Connection::open_with_flags(
            path,
            OpenFlags::SQLITE_OPEN_READ_WRITE
                | OpenFlags::SQLITE_OPEN_CREATE
                | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )?;
        Self::init(conn, key)
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
        // Migrations run with foreign keys off (see `migrate`); they are on from here.
        migrate::migrate(&mut conn)?;
        conn.pragma_update(None, "foreign_keys", true)?;
        let store = Self {
            conn: Mutex::new(conn),
            cipher: Cipher::new(key),
        };
        store.check_key()?;
        Ok(store)
    }

    pub(crate) fn conn(&self) -> MutexGuard<'_, Connection> {
        // A panic while holding the lock leaves SQLite consistent (statements are atomic).
        self.conn.lock().unwrap_or_else(PoisonError::into_inner)
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
        let text: Option<String> = self
            .conn()
            .query_row(
                "SELECT value_json FROM settings WHERE key = ?1",
                [key],
                |r| r.get(0),
            )
            .optional()?;
        text.map(|t| serde_json::from_str(&t).map_err(|_| StoreError::BadSetting(key.to_owned())))
            .transpose()
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
        Ok(self
            .conn()
            .query_row("SELECT 1 FROM secrets WHERE name = ?1", [name], |_| Ok(()))
            .optional()?
            .is_some())
    }

    /// The names of the stored secrets, sorted. Names only, never values.
    pub fn secret_names(&self) -> Result<Vec<String>, StoreError> {
        let conn = self.conn();
        let mut stmt = conn.prepare(
            "SELECT name FROM secrets WHERE name NOT LIKE '\\_%' ESCAPE '\\' ORDER BY name",
        )?;
        let names = stmt
            .query_map([], |r| r.get(0))?
            .collect::<Result<Vec<String>, _>>()?;
        Ok(names)
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

    fn read_sealed(&self, name: &str) -> Result<Option<Zeroizing<Vec<u8>>>, StoreError> {
        let row: Option<(Vec<u8>, Vec<u8>)> = self
            .conn()
            .query_row(
                "SELECT nonce, ciphertext FROM secrets WHERE name = ?1",
                [name],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .optional()?;
        row.map(|(nonce, ct)| self.cipher.open(name, &nonce, &ct))
            .transpose()
    }

    /// Checkpoints the WAL into the main file and closes the connection.
    pub fn close(self) -> Result<(), StoreError> {
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

    fn key(b: u8) -> MasterKey {
        MasterKey::from_bytes([b; 32])
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
                "company_leases",
                "edges",
                "jobs",
                "locations",
                "nodes",
                "objects_seen",
                "records",
                "scan_checkpoints",
                "search_fts",
                "search_fts_config",
                "search_fts_data",
                "search_fts_docsize",
                "search_fts_idx",
                "search_index",
                "secrets",
                "settings",
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
                "updated_at"
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
}
