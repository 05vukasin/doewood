//! The instance-local SQLite store (ADR 0001, section 1): runtime settings, encrypted secrets,
//! sources, jobs and per-object progress.
//!
//! In standalone mode (no shared Postgres) the same file also holds the records, the graph,
//! source claims and search: [`Standalone`] implements the `doewood-model` store traits over it.
//!
//! - One SQLite file per instance, opened in WAL mode with a busy timeout.
//! - A single writer: every statement goes through one connection behind a mutex.
//! - The schema is migrated on open from SQL embedded in the binary.
//! - Secrets are sealed with XChaCha20-Poly1305 under the master key and are **write-only**
//!   through the public API: they can be set, deleted and tested for, never read back. The one
//!   exception is the connectors' access path, [`Store::source_credentials`], which decrypts only
//!   the secret a source names, for the S3 client; the API never calls it.

mod crypto;
mod error;
pub mod keyfile;
mod migrate;
mod standalone;
mod store;

pub use error::StoreError;
pub use migrate::SCHEMA_VERSION;
pub use standalone::Standalone;
pub use store::ai::{
    AI_PRESETS, AiProvider, AiProviderInput, ApiKey, DEFAULT_AI_PROVIDER, KeyChange,
    valid_provider_name,
};
pub use store::ingest::{
    DEFAULT_SIMILAR_MIN_JACCARD, IngestSettings, IngestSettingsPatch, SIMILAR_MIN_JACCARD,
    valid_min_jaccard,
};
pub use store::llm::{LlmCounts, LlmJob, LlmJobStatus, LlmUsage, SECONDS_PER_DAY, utc_day};
pub use store::objects::{IngestOutcome, IngestStatus, ObjectChange, ScanCheckpoint, SeenObject};
pub use store::sources::{
    DEFAULT_LLM_CONCURRENCY, DEFAULT_MAX_OBJECT_BYTES, DEFAULT_SCAN_INTERVAL, LlmMode, LlmPolicy,
    MAX_LLM_CONCURRENCY, MAX_LLM_KINDS, NewSource, ObjectCounts, S3Credentials, Source,
    SourcePatch, SourceState, UrlStyle, valid_kind,
};
pub use store::{SECRET_MAX_LEN, STORE_FILE, Store};
