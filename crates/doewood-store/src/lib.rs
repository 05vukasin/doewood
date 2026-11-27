//! The instance-local SQLite store (ADR 0001, section 1): runtime settings, encrypted secrets,
//! sources, jobs and per-object progress.
//!
//! In standalone mode (no shared Postgres) the same file also holds the records, the graph,
//! source claims and search: [`Standalone`] implements the `doewood-model` store traits over it.
//!
//! - One SQLite file per instance, opened in WAL mode with a busy timeout.
//! - A single writer: every write goes through one connection behind a mutex. An on-disk store
//!   also has a few read-only connections for pure reads (WAL lets them run next to the
//!   writer); an in-memory one reads on the writer.
//! - The schema is migrated on open from SQL embedded in the binary.
//! - Secrets are sealed with XChaCha20-Poly1305 under the master key and are **write-only**
//!   through the public API: they can be set, deleted and tested for, never read back. The
//!   exceptions are the connectors' access paths ([`Store::source_credentials`] for the S3
//!   client, [`Store::sqs_credentials`] for the SQS inlet, [`Store::ai_api_key`] and
//!   [`Store::ai_aws_credentials`] for the LLM, [`Store::enrichment_config`] for the
//!   enrichment job), which the API never calls.

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
pub use store::api::{
    API_DEADLINE_SECONDS, API_MAX_CALLS_IN_FLIGHT, ApiSettings, ApiSettingsPatch,
    DEADLINE_SECONDS_LIMIT, DEFAULT_DEADLINE_SECONDS, DEFAULT_MAX_CALLS_IN_FLIGHT,
    MAX_CALLS_IN_FLIGHT_LIMIT, valid_deadline_seconds, valid_max_calls_in_flight,
};
pub use store::claims::{CLAIMED_BY, ClaimDrops, ClaimView};
pub use store::enrichment::{
    APR_ENABLED, APR_EXTRA_ROOTS_SECRET, APR_FEED_ENABLED, COMPANIES_HOUSE_ENABLED,
    COMPANIES_HOUSE_FEED_ENABLED, COMPANIES_HOUSE_KEY_SECRET, COMPANIES_HOUSE_STREAM_KEY_SECRET,
    EDGAR_CONTACT, EDGAR_CONTACT_MAX_CHARS, EDGAR_ENABLED, EDGAR_FEED_ENABLED, EnrichmentConfig,
    EnrichmentSettings, EnrichmentSettingsPatch, FeedSwitches, FeedSwitchesPatch, GLEIF_ENABLED,
    GLEIF_FEED_ENABLED, OPENCORPORATES_KEY_SECRET, PEM_MAX_LEN, RegistrySwitches,
    RegistrySwitchesPatch, RootsChange, RootsPem, VIES_ENABLED, WIKIDATA_ENABLED,
    valid_edgar_contact,
};
pub use store::favicon::{
    FAVICON_EGRESS_PROXY, FAVICON_FALLBACK, FaviconSettings, FaviconSettingsPatch,
    valid_egress_proxy,
};
pub use store::ingest::{
    DEFAULT_SIMILAR_CROWD_SIZE, DEFAULT_SIMILAR_IDENTICAL_MIN_CHARS, DEFAULT_SIMILAR_MAX_BUCKET,
    DEFAULT_SIMILAR_MIN_JACCARD, DEFAULT_SIMILAR_NEAR_COPY_MIN_JACCARD,
    DEFAULT_SIMILAR_TEMPLATE_MIN_MASKED, DEFAULT_SUMMARIZE_MAX_CONCURRENT, IngestSettings,
    IngestSettingsPatch, SIMILAR_CROWD_SIZE, SIMILAR_CROWD_SIZE_RANGE, SIMILAR_IDENTICAL_MIN_CHARS,
    SIMILAR_IDENTICAL_MIN_CHARS_RANGE, SIMILAR_MAX_BUCKET, SIMILAR_MAX_BUCKET_RANGE,
    SIMILAR_MIN_JACCARD, SIMILAR_NEAR_COPY_MIN_JACCARD, SIMILAR_TEMPLATE_MIN_MASKED,
    SUMMARIZE_MAX_CONCURRENT, SUMMARIZE_MAX_CONCURRENT_RANGE, valid_crowd_size,
    valid_identical_min_chars, valid_min_jaccard, valid_similar_max_bucket,
    valid_summarize_max_concurrent,
};
pub use store::llm::{LlmCounts, LlmJob, LlmJobStatus, LlmUsage, SECONDS_PER_DAY, utc_day};
pub use store::objects::{
    IngestOutcome, IngestStatus, KnownContent, ObjectChange, ScanCheckpoint, SeenObject,
};
pub use store::readers::{api_call, api_call_blocking};
pub use store::sources::{
    DEFAULT_LLM_CONCURRENCY, DEFAULT_MAX_OBJECT_BYTES, DEFAULT_SCAN_INTERVAL, LlmMode, LlmPolicy,
    MAX_LLM_CONCURRENCY, MAX_LLM_KINDS, NewSource, ObjectCounts, S3Credentials, Source,
    SourcePatch, SourceState, UrlStyle, valid_kind,
};
pub use store::sqs::{
    CredentialsChange, QUEUE_URL_MAX_LEN, SQS_DEFAULT_REGION, SqsInlet, SqsInletInput,
    queue_url_region, valid_queue_url,
};
pub use store::wal::{Checkpoint, CheckpointMode, WalPass, WalStats};
pub use store::{
    ANALYSIS_LIMIT, Applied, DEFAULT_READERS, MAX_READERS, Optimized, SECRET_MAX_LEN, STORE_FILE,
    Snapshot, Store, default_readers, plan_readers,
};
