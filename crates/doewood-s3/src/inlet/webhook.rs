//! MinIO / S3 event notifications over a webhook.
//!
//! Both MinIO's webhook target and S3's notification JSON (as delivered by SNS, SQS or
//! `EventBridge` pipes) carry a `Records` array:
//!
//! ```json
//! {"Records": [{"eventName": "s3:ObjectCreated:Put",
//!               "s3": {"bucket": {"name": "docs"},
//!                      "object": {"key": "in/a+b.pdf", "size": 42, "eTag": "0123"}}}]}
//! ```
//!
//! Keys are URL-encoded (a space is `+`). `ObjectCreated:*` becomes `New` or `Changed` after a
//! comparison with `objects_seen`, `ObjectRemoved:*` and `LifecycleExpiration:*` become
//! `Deleted`, and anything else (test events, restores, replication) is ignored.

use std::sync::Arc;

use doewood_core::SourceId;
use doewood_store::{ObjectChange, Store};
use percent_encoding::percent_decode_str;
use serde::Deserialize;
use tokio::sync::mpsc;

use crate::S3Error;
use crate::client::unquote;
use crate::event::{Diff, EventKind, EventOrigin, ObjectEvent, ObjectVersion, classify};

/// Largest notification body accepted.
pub const MAX_BODY: usize = 1024 * 1024;

/// What a notification says happened.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum NotificationKind {
    /// An object was written.
    Created(ObjectVersion),
    /// An object was removed (or expired).
    Removed,
}

/// One parsed notification record.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Notification {
    /// The bucket.
    pub bucket: String,
    /// The key, decoded.
    pub key: String,
    /// What happened.
    pub kind: NotificationKind,
}

/// Why a notification body was refused.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum WebhookError {
    /// The body is not notification JSON.
    #[error("not an S3 event notification: {0}")]
    Parse(String),
    /// The body is larger than [`MAX_BODY`].
    #[error("notification body larger than {MAX_BODY} bytes")]
    TooLarge,
    /// The source does not exist.
    #[error("source {0} does not exist")]
    UnknownSource(SourceId),
    /// The store or the event channel failed.
    #[error(transparent)]
    S3(#[from] S3Error),
}

#[derive(Deserialize)]
struct Body {
    #[serde(rename = "Records", default)]
    records: Vec<Record>,
}

#[derive(Deserialize)]
struct Record {
    #[serde(rename = "eventName")]
    event_name: String,
    s3: Entity,
}

#[derive(Deserialize)]
struct Entity {
    bucket: BucketEntity,
    object: ObjectEntity,
}

#[derive(Deserialize)]
struct BucketEntity {
    name: String,
}

#[derive(Deserialize)]
struct ObjectEntity {
    key: String,
    size: Option<u64>,
    #[serde(rename = "eTag")]
    etag: Option<String>,
}

/// Parses a notification body. Records of other event types are left out; a body without
/// `Records` (an S3 test event) gives none.
pub fn parse_notifications(body: &[u8]) -> Result<Vec<Notification>, WebhookError> {
    if body.len() > MAX_BODY {
        return Err(WebhookError::TooLarge);
    }
    let body: Body =
        serde_json::from_slice(body).map_err(|e| WebhookError::Parse(e.to_string()))?;
    Ok(body
        .records
        .into_iter()
        .filter_map(|r| {
            let name = r.event_name.strip_prefix("s3:").unwrap_or(&r.event_name);
            let kind = if name.starts_with("ObjectCreated:") {
                // Without a size the event cannot be compared; the scan will see the object.
                NotificationKind::Created(ObjectVersion {
                    etag: r.s3.object.etag.as_deref().map(unquote),
                    size: r.s3.object.size?,
                    last_modified: None,
                })
            } else if name.starts_with("ObjectRemoved:") || name.starts_with("LifecycleExpiration:")
            {
                NotificationKind::Removed
            } else {
                return None;
            };
            Some(Notification {
                bucket: r.s3.bucket.name,
                key: decode_key(&r.s3.object.key),
                kind,
            })
        })
        .collect())
}

/// Decodes a notification key like a form value: `+` is a space, then percent-escapes.
fn decode_key(key: &str) -> String {
    let plus = key.replace('+', " ");
    percent_decode_str(&plus)
        .decode_utf8()
        .map_or(plus.clone(), std::borrow::Cow::into_owned)
}

/// What a webhook call did.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct WebhookOutcome {
    /// Events sent.
    pub emitted: usize,
    /// Records ignored: another bucket, outside the prefix, already seen, or the source is
    /// not active.
    pub ignored: usize,
}

/// The webhook inlet: turns notification bodies into events for one source at a time.
#[derive(Clone, Debug)]
pub struct WebhookInlet {
    store: Arc<Store>,
    events: mpsc::Sender<ObjectEvent>,
}

impl WebhookInlet {
    /// An inlet sending to the scanners' channel.
    pub fn new(store: Arc<Store>, events: mpsc::Sender<ObjectEvent>) -> Self {
        Self { store, events }
    }

    /// Handles one notification body for `source`: compares each record with `objects_seen`,
    /// sends the events, then records them, like a scan does.
    pub async fn handle(
        &self,
        source: SourceId,
        body: &[u8],
    ) -> Result<WebhookOutcome, WebhookError> {
        let notifications = parse_notifications(body)?;
        deliver(
            &self.store,
            &self.events,
            source,
            notifications,
            EventOrigin::Webhook,
        )
        .await
    }
}

/// Hands parsed notifications for `source` off as events: compares each record with
/// `objects_seen`, records it and sends the event, like a scan does: a new or changed object
/// is recorded (`pending`) before its event is sent, so the worker finds its row; a deletion
/// is sent first, then recorded. Records of another bucket, outside the prefix, already seen,
/// or of a source that is not scanned are ignored. A failure part-way leaves the rest undone;
/// handing the same notifications off again is safe, since what was recorded is then seen (a
/// pending row whose event was not sent is fed by ingest's sweep).
pub(crate) async fn deliver(
    store: &Arc<Store>,
    events: &mpsc::Sender<ObjectEvent>,
    source: SourceId,
    notifications: Vec<Notification>,
    origin: EventOrigin,
) -> Result<WebhookOutcome, WebhookError> {
    let s = Arc::clone(store);
    let src = blocking(move || s.source(source))
        .await?
        .ok_or(WebhookError::UnknownSource(source))?;
    let mut outcome = WebhookOutcome::default();
    if !src.state.is_scanned() {
        outcome.ignored = notifications.len();
        return Ok(outcome);
    }
    for n in notifications {
        if n.bucket != src.bucket || !n.key.starts_with(&src.prefix) {
            outcome.ignored += 1;
            continue;
        }
        let s = Arc::clone(store);
        let key = n.key.clone();
        let seen = blocking(move || s.seen_object(source, &key)).await?;
        let kind = match n.kind {
            NotificationKind::Created(version) => classify(seen.as_ref(), version),
            NotificationKind::Removed => seen.is_some().then_some(EventKind::Deleted),
        };
        let Some(kind) = kind else {
            outcome.ignored += 1;
            continue;
        };
        let diff = Diff { key: n.key, kind };
        let change: ObjectChange = diff.change();
        let event = ObjectEvent {
            source,
            key: diff.key,
            kind: diff.kind,
            origin,
        };
        let s = Arc::clone(store);
        let record = blocking(move || s.record_objects(source, &[change], None));
        // As in a scan: a new or changed object's row is committed before its event is sent
        // (issue #125), a deletion's after.
        if matches!(event.kind, EventKind::Deleted) {
            send(events, event).await?;
            record.await?;
        } else {
            record.await?;
            send(events, event).await?;
        }
        outcome.emitted += 1;
    }
    Ok(outcome)
}

async fn send(events: &mpsc::Sender<ObjectEvent>, event: ObjectEvent) -> Result<(), S3Error> {
    events.send(event).await.map_err(|_| S3Error::ChannelClosed)
}

async fn blocking<T: Send + 'static>(
    f: impl FnOnce() -> Result<T, doewood_store::StoreError> + Send + 'static,
) -> Result<T, S3Error> {
    tokio::task::spawn_blocking(f)
        .await
        .map_err(|e| S3Error::Transport {
            op: "store",
            reason: e.to_string(),
        })?
        .map_err(S3Error::from)
}

#[cfg(test)]
mod tests {
    use doewood_core::MasterKey;
    use doewood_store::{NewSource, SourceState};

    use super::*;

    const MINIO_PUT: &str = r#"{"EventName":"s3:ObjectCreated:Put","Key":"docs/in/a b.pdf",
      "Records":[{"eventVersion":"2.0","eventSource":"minio:s3","awsRegion":"",
      "eventTime":"2026-01-02T03:04:05.000Z","eventName":"s3:ObjectCreated:Put",
      "userIdentity":{"principalId":"test-user"},
      "s3":{"s3SchemaVersion":"1.0","configurationId":"Config",
        "bucket":{"name":"docs","ownerIdentity":{"principalId":"test-user"},"arn":"arn:aws:s3:::docs"},
        "object":{"key":"in%2Fa+b.pdf","size":42,"eTag":"0123abcd","contentType":"application/pdf",
          "sequencer":"17A0"}},
      "source":{"host":"127.0.0.1","port":"","userAgent":"test"}}]}"#;

    const AWS_EVENTS: &str = r#"{"Records":[
      {"eventVersion":"2.1","eventSource":"aws:s3","eventName":"ObjectCreated:CompleteMultipartUpload",
       "s3":{"bucket":{"name":"docs"},"object":{"key":"in/%C3%A9t%C3%A9.xlsx","size":7,"eTag":"\"ff-2\""}}},
      {"eventVersion":"2.1","eventSource":"aws:s3","eventName":"ObjectRemoved:DeleteMarkerCreated",
       "s3":{"bucket":{"name":"docs"},"object":{"key":"in/old.pdf"}}},
      {"eventVersion":"2.1","eventSource":"aws:s3","eventName":"LifecycleExpiration:Delete",
       "s3":{"bucket":{"name":"docs"},"object":{"key":"in/expired.pdf"}}},
      {"eventVersion":"2.1","eventSource":"aws:s3","eventName":"ObjectRestore:Completed",
       "s3":{"bucket":{"name":"docs"},"object":{"key":"in/cold.pdf","size":1}}}
    ]}"#;

    #[test]
    fn parses_minio_notifications() {
        let n = parse_notifications(MINIO_PUT.as_bytes()).unwrap();
        assert_eq!(
            n,
            [Notification {
                bucket: "docs".into(),
                key: "in/a b.pdf".into(),
                kind: NotificationKind::Created(ObjectVersion {
                    etag: Some("0123abcd".into()),
                    size: 42,
                    last_modified: None,
                }),
            }]
        );
    }

    #[test]
    fn parses_aws_notifications_and_skips_other_events() {
        let n = parse_notifications(AWS_EVENTS.as_bytes()).unwrap();
        assert_eq!(n.len(), 3);
        assert_eq!(n[0].key, "in/été.xlsx");
        assert!(
            matches!(&n[0].kind, NotificationKind::Created(v) if v.etag.as_deref() == Some("ff-2"))
        );
        assert_eq!(n[1].kind, NotificationKind::Removed);
        assert_eq!(n[2].key, "in/expired.pdf");
        // The S3 test event has no records.
        let test = r#"{"Service":"Amazon S3","Event":"s3:TestEvent","Bucket":"docs"}"#;
        assert!(parse_notifications(test.as_bytes()).unwrap().is_empty());
        assert!(matches!(
            parse_notifications(b"not json"),
            Err(WebhookError::Parse(_))
        ));
        assert!(matches!(
            parse_notifications(&vec![b' '; MAX_BODY + 1]),
            Err(WebhookError::TooLarge)
        ));
    }

    #[test]
    fn keys_decode_like_form_values() {
        assert_eq!(decode_key("a+b%2Bc"), "a b+c");
        assert_eq!(decode_key("%E2%82%AC"), "€");
        // Invalid UTF-8 escapes are kept as they are.
        assert_eq!(decode_key("%FF"), "%FF");
    }

    #[tokio::test]
    async fn handle_compares_records_and_emits_events() {
        let store = Arc::new(Store::open_in_memory(&MasterKey::from_bytes([9; 32])).unwrap());
        let mut new = NewSource::new("http://minio", "docs");
        new.prefix = "in/".into();
        let id = store.add_source(&new, None).unwrap();
        let (tx, mut rx) = mpsc::channel(10);
        let inlet = WebhookInlet::new(Arc::clone(&store), tx);

        let out = inlet.handle(id, MINIO_PUT.as_bytes()).await.unwrap();
        assert_eq!(
            out,
            WebhookOutcome {
                emitted: 1,
                ignored: 0
            }
        );
        let e = rx.recv().await.unwrap();
        assert_eq!(
            (e.key.as_str(), e.kind.name(), e.origin),
            ("in/a b.pdf", "new", EventOrigin::Webhook)
        );
        assert!(store.seen_object(id, "in/a b.pdf").unwrap().is_some());
        // The same notification again changes nothing.
        let out = inlet.handle(id, MINIO_PUT.as_bytes()).await.unwrap();
        assert_eq!(
            out,
            WebhookOutcome {
                emitted: 0,
                ignored: 1
            }
        );
        // A different ETag is a change.
        let changed = MINIO_PUT.replace("0123abcd", "4567");
        inlet.handle(id, changed.as_bytes()).await.unwrap();
        assert_eq!(rx.recv().await.unwrap().kind.name(), "changed");
        // A removal of a seen key is a deletion; of an unseen one, nothing.
        let removed = MINIO_PUT.replace("s3:ObjectCreated:Put", "s3:ObjectRemoved:Delete");
        assert_eq!(
            inlet.handle(id, removed.as_bytes()).await.unwrap().emitted,
            1
        );
        assert_eq!(rx.recv().await.unwrap().kind.name(), "deleted");
        assert_eq!(
            inlet.handle(id, removed.as_bytes()).await.unwrap().emitted,
            0
        );
        // Another bucket, or outside the prefix, is ignored.
        let other = MINIO_PUT.replace(r#""name":"docs""#, r#""name":"other""#);
        assert_eq!(inlet.handle(id, other.as_bytes()).await.unwrap().ignored, 1);
        let outside = MINIO_PUT.replace("in%2Fa", "out%2Fa");
        assert_eq!(
            inlet.handle(id, outside.as_bytes()).await.unwrap().ignored,
            1
        );
        // A paused source ignores everything.
        store
            .set_source_state(id, SourceState::Paused, None)
            .unwrap();
        let out = inlet.handle(id, MINIO_PUT.as_bytes()).await.unwrap();
        assert_eq!(
            out,
            WebhookOutcome {
                emitted: 0,
                ignored: 1
            }
        );
        assert!(matches!(
            inlet
                .handle(SourceId::generate(), MINIO_PUT.as_bytes())
                .await,
            Err(WebhookError::UnknownSource(_))
        ));
    }

    /// A new object's row is committed before its event is sent (issue #125): the consumer
    /// finds it at once.
    #[tokio::test]
    async fn a_row_commits_before_its_event() {
        let store = Arc::new(Store::open_in_memory(&MasterKey::from_bytes([9; 32])).unwrap());
        let id = store
            .add_source(&NewSource::new("http://minio", "docs"), None)
            .unwrap();
        // The channel is full, so the hand-off waits until the consumer reads.
        let (tx, mut rx) = mpsc::channel(1);
        tx.send(ObjectEvent {
            source: id,
            key: "earlier".into(),
            kind: EventKind::Deleted,
            origin: EventOrigin::Webhook,
        })
        .await
        .unwrap();
        let inlet = WebhookInlet::new(Arc::clone(&store), tx);
        let handoff = tokio::spawn(async move { inlet.handle(id, MINIO_PUT.as_bytes()).await });
        assert_eq!(rx.recv().await.unwrap().key, "earlier");
        let e = rx.recv().await.unwrap();
        assert_eq!(e.key, "in/a b.pdf");
        let row = store.seen_object(id, &e.key).unwrap();
        assert_eq!(row.and_then(|r| r.etag).as_deref(), Some("0123abcd"));
        assert_eq!(handoff.await.unwrap().unwrap().emitted, 1);
    }
}
