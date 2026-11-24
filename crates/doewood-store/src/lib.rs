//! The instance-local SQLite store (ADR 0001, section 1): runtime settings, encrypted secrets,
//! sources, jobs and per-object progress.
//!
//! - One SQLite file per instance, opened in WAL mode with a busy timeout.
//! - A single writer: every statement goes through one connection behind a mutex.
//! - The schema is migrated on open from SQL embedded in the binary.
//! - Secrets are sealed with XChaCha20-Poly1305 under the master key and are **write-only**
//!   through the public API: they can be set, deleted and tested for, never read back. Only
//!   code inside this crate (the connectors' access path) can reveal them.

mod crypto;
mod error;
pub mod keyfile;
mod migrate;
mod store;

pub use error::StoreError;
pub use migrate::SCHEMA_VERSION;
pub use store::{SECRET_MAX_LEN, STORE_FILE, Store};
