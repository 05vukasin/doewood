//! The error type every store backend returns.

use doewood_core::InstanceId;

use crate::SourceKey;

/// What a store operation can fail with, whatever the backend.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// The source is owned by another instance. The message names the owner, so an operator
    /// knows which instance to look at.
    #[error("source {key} is already claimed by instance {owner:?}")]
    SourceClaimed {
        /// The source that was asked for.
        key: SourceKey,
        /// The instance that owns it.
        owner: InstanceId,
    },
    /// An input that breaks the model's rules (an empty key, a confidence above 1, ...).
    #[error("invalid input: {0}")]
    Invalid(String),
    /// Something the operation refers to does not exist (an edge to an unknown node, ...).
    #[error("not found: {0}")]
    NotFound(String),
    /// The store's state does not allow the operation: a dismissed pair, a merge that later
    /// merges depend on, an entry already undone. The message says what to do instead.
    #[error("{0}")]
    Precondition(String),
    /// The backend cannot be reached right now; the operation may succeed later.
    #[error("store unavailable: {0}")]
    Unavailable(String),
    /// The database was migrated by a newer doewood.
    #[error("schema version {found} is newer than this binary's {supported}; upgrade doewood")]
    SchemaTooNew {
        /// Version in the database.
        found: i64,
        /// Newest version this binary knows.
        supported: i64,
    },
    /// Any other backend failure.
    #[error("{backend}: {message}")]
    Backend {
        /// `postgres` or `sqlite`.
        backend: &'static str,
        /// What went wrong.
        message: String,
    },
}

/// The result type of the store traits.
pub type Result<T, E = Error> = std::result::Result<T, E>;
