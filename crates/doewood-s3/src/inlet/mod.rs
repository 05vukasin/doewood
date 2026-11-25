//! Event inlets: push notifications that pick objects up within seconds (ADR 0001, section 5).
//! They are optional: the periodic scan catches anything they miss, and nothing runs unless an
//! inlet is configured.
//!
//! - [`webhook`] (feature `webhook`, on by default): parses MinIO / S3 event notification JSON
//!   into the scanner's events. The API step mounts [`webhook::WebhookInlet::handle`] on a URL
//!   per source.
//! - [`sqs`] (feature `sqs`, on by default): [`sqs::SqsInlet`] long-polls an SQS queue that
//!   S3 sends its event notifications to (directly or through SNS). [`crate::Discovery`] runs
//!   one per source that has an enabled inlet, next to its scanner.

use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, PoisonError};

use tokio::sync::{mpsc, watch};

use crate::{ObjectEvent, S3Error};

#[cfg(feature = "sqs")]
pub mod sqs;
#[cfg(feature = "webhook")]
pub mod webhook;

/// An inlet that runs as a task of its own and pulls notifications from somewhere (a queue),
/// sending the same [`ObjectEvent`]s as the scanner until shutdown.
///
/// The webhook inlet does not implement it: it is pushed to through the API instead.
pub trait EventInlet: Send + 'static {
    /// A short name for logs (`sqs`).
    fn name(&self) -> &'static str;

    /// Runs until `shutdown` turns true or the channel closes.
    fn run(
        self,
        events: mpsc::Sender<ObjectEvent>,
        shutdown: watch::Receiver<bool>,
    ) -> impl Future<Output = Result<(), S3Error>> + Send;
}

/// What a running inlet did since it started. Shared between the inlet task and the API.
#[derive(Debug, Default)]
pub struct InletStats {
    received: AtomicU64,
    deleted: AtomicU64,
    emitted: AtomicU64,
    ignored: AtomicU64,
    test_events: AtomicU64,
    malformed: AtomicU64,
    errors: AtomicU64,
    last_error: Mutex<Option<(i64, String)>>,
}

/// A copy of [`InletStats`].
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct InletStatsSnapshot {
    /// Messages received from the queue.
    pub received: u64,
    /// Messages deleted from the queue (handed off, ignored, test events or malformed).
    pub deleted: u64,
    /// Events sent.
    pub emitted: u64,
    /// Notification records ignored: another bucket, outside the prefix, already seen, or
    /// the source is not scanned.
    pub ignored: u64,
    /// `s3:TestEvent` messages.
    pub test_events: u64,
    /// Messages that are not S3 event notifications, deleted after a warning.
    pub malformed: u64,
    /// Failed queue requests and hand-offs.
    pub errors: u64,
    /// The last failure (Unix seconds and what happened), if any. Never holds a credential.
    pub last_error: Option<(i64, String)>,
}

impl InletStats {
    /// A copy of the counters.
    pub fn snapshot(&self) -> InletStatsSnapshot {
        let get = |c: &AtomicU64| c.load(Ordering::Relaxed);
        InletStatsSnapshot {
            received: get(&self.received),
            deleted: get(&self.deleted),
            emitted: get(&self.emitted),
            ignored: get(&self.ignored),
            test_events: get(&self.test_events),
            malformed: get(&self.malformed),
            errors: get(&self.errors),
            last_error: self
                .last_error
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .clone(),
        }
    }

    #[cfg_attr(not(feature = "sqs"), allow(dead_code))]
    pub(crate) fn add(counter: &AtomicU64, n: usize) {
        counter.fetch_add(u64::try_from(n).unwrap_or(u64::MAX), Ordering::Relaxed);
    }

    #[cfg_attr(not(feature = "sqs"), allow(dead_code))]
    pub(crate) fn error(&self, what: String) {
        self.errors.fetch_add(1, Ordering::Relaxed);
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| i64::try_from(d.as_secs()).unwrap_or(i64::MAX));
        *self
            .last_error
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = Some((now, what));
    }
}
