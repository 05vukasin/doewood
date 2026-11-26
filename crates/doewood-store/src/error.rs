use std::path::PathBuf;

/// What can go wrong in the store.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum StoreError {
    /// An SQLite error.
    #[error("sqlite: {0}")]
    Sqlite(#[from] rusqlite::Error),
    /// The WAL checkpoint thread could not be started.
    #[error("the WAL checkpoint thread did not start: {0}")]
    Thread(std::io::Error),
    /// The file was written by a newer doewood with a schema this binary does not know.
    #[error(
        "store schema version {found} is newer than this binary's {supported}; upgrade doewood"
    )]
    SchemaTooNew {
        /// Version in the file.
        found: i64,
        /// Newest version this binary knows.
        supported: i64,
    },
    /// A migration left the schema inconsistent; nothing of it was committed.
    #[error("migration to schema {version} failed: {reason}")]
    Migration {
        /// The version the migration leads to.
        version: i64,
        /// Why.
        reason: String,
    },
    /// A source that does not exist.
    #[error("source {0} does not exist")]
    NoSuchSource(String),
    /// A source value that does not follow the format.
    #[error("invalid source: {0}")]
    InvalidSource(String),
    /// The master key is not the one the store was created with.
    #[error("the master key does not match the one this store was created with")]
    WrongMasterKey,
    /// A secret did not decrypt: wrong key, or the row was tampered with.
    #[error("secret {0:?} failed to decrypt (wrong master key or tampered data)")]
    Decrypt(String),
    /// A secret name that does not follow the format.
    #[error("invalid secret name {name:?}: {reason}")]
    InvalidSecretName {
        /// The refused name.
        name: String,
        /// Why.
        reason: &'static str,
    },
    /// A secret value larger than the limit.
    #[error("secret value is {0} bytes; the limit is {max}", max = crate::SECRET_MAX_LEN)]
    SecretTooLarge(usize),
    /// A runtime setting value outside its range.
    #[error("invalid setting: {0}")]
    InvalidSetting(String),
    /// A settings value that is not valid JSON in the database.
    #[error("setting {0:?} holds invalid JSON")]
    BadSetting(String),
    /// The key file could not be read or written.
    #[error("key file {}: {source}", path.display())]
    KeyFile {
        /// The key file.
        path: PathBuf,
        /// The I/O error.
        source: std::io::Error,
    },
    /// The key file exists but does not hold a valid key. It is never overwritten, because
    /// that would make every stored secret unreadable.
    #[error("key file {} does not hold a valid key: {reason}", path.display())]
    BadKeyFile {
        /// The key file.
        path: PathBuf,
        /// Why.
        reason: String,
    },
    /// The operating system's random number generator failed.
    #[error("random number generator failed")]
    Random,
}
