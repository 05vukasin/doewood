//! Event inlets: push notifications that pick objects up within seconds (ADR 0001, section 5).
//! They are optional: the periodic scan catches anything they miss, and nothing runs unless an
//! inlet is configured.
//!
//! - [`webhook`] (feature `webhook`, on by default): parses MinIO / S3 event notification JSON
//!   into the scanner's events. The API step mounts [`webhook::WebhookInlet::handle`] on a URL
//!   per source.
//! - An SQS poller is not built yet; it will implement [`EventInlet`].

use std::future::Future;

use tokio::sync::{mpsc, watch};

use crate::{ObjectEvent, S3Error};

#[cfg(feature = "webhook")]
pub mod webhook;

/// An inlet that runs as a task of its own and pulls notifications from somewhere (a queue),
/// sending the same [`ObjectEvent`]s as the scanner until shutdown.
///
/// The webhook inlet does not implement it: it is pushed to through the API instead.
///
/// TODO(sqs): an SQS poller (long polling `ReceiveMessage`, `DeleteMessage` after the events
/// are recorded) that parses S3 event notifications with [`webhook::parse_notifications`].
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
