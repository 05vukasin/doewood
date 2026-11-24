//! The error type of the core crate.

use std::path::PathBuf;

/// What can go wrong in the core: an unusable data directory, or a value that does not parse.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// The data directory cannot be created, or is not writable. This is the only fatal
    /// bootstrap problem.
    #[error("data directory {} is unusable: {source}", path.display())]
    DataDir {
        /// The directory that was tried.
        path: PathBuf,
        /// The underlying I/O error.
        source: std::io::Error,
    },
    /// An id that does not follow its format.
    #[error("invalid {kind}: {reason}")]
    InvalidId {
        /// Which id (`instance id`, `source id`, ...).
        kind: &'static str,
        /// Why it was refused.
        reason: String,
    },
    /// A master key that is not base64 of exactly 32 bytes.
    #[error("invalid master key: {0}")]
    InvalidMasterKey(&'static str),
}
