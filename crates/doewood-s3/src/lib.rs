//! doewood's S3 sources and discovery (ADR 0001, section 5).
//!
//! - [`S3Client`]: a light client for any S3-compatible store (AWS, MinIO, Ceph, R2, Wasabi):
//!   `rusty-s3` signs (SigV4, presigned), `reqwest` sends (HTTP/1.1, rustls). `ListObjectsV2`,
//!   `HeadObject` and a streamed GetObject under a size cap, with timeouts and retries on 5xx,
//!   429 and `SlowDown`.
//! - [`Scanner`]: the incremental scanner. It walks each listing page alongside `objects_seen`,
//!   emits [`ObjectEvent`]s (`New`, `Changed`, `Deleted`) to a bounded channel and commits a
//!   checkpoint with every step, so that a restart resumes and memory stays bounded.
//! - [`Discovery`]: one scanner task per active source, stopped on shutdown.
//! - [`inlet`]: optional push notifications: the MinIO / S3 webhook parser, and the SQS inlet
//!   that long-polls a queue of S3 event notifications (one task per source, run by
//!   [`Discovery`] next to the scanner).
//!
//! Credentials come from the store's encrypted secrets through
//! [`doewood_store::Store::source_credentials`] and never appear in logs, errors or `Debug`.

mod client;
mod discovery;
mod error;
mod event;
pub mod inlet;
mod retry;
mod scanner;
#[cfg(feature = "sqs")]
mod sigv4;

pub use client::{
    ClientOptions, ListPage, ListRequest, ListedObject, ObjectBody, ObjectMeta, S3Client, S3Config,
    http_client,
};
pub use discovery::{Discovery, DiscoveryOptions, EVENT_CHANNEL_CAPACITY};
pub use error::S3Error;
pub use event::{EventKind, EventOrigin, ObjectEvent, ObjectVersion};
pub use retry::{RetryPolicy, retryable_status};
pub use scanner::{Connect, Lister, PassStats, ScanOptions, Scanner};
