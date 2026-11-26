//! The SQS inlet: long-polls a queue that S3 sends its event notifications to, and hands them
//! off as the same events the webhook produces (ADR 0001, section 5).
//!
//! **Protocol.** The AWS JSON protocol (`awsJson1_0`), as the AWS SDKs speak it: `POST /` on
//! the queue's host with `Content-Type: application/x-amz-json-1.0`,
//! `X-Amz-Target: AmazonSQS.<Action>` and the queue URL in the JSON body, signed with SigV4 for
//! the service `sqs` (see [`crate::sigv4`]). Two actions are used: `ReceiveMessage`
//! (`WaitTimeSeconds` 20, `MaxNumberOfMessages` 10, a `VisibilityTimeout` that covers the
//! hand-off) and `DeleteMessageBatch` (up to 10 receipt handles). Nothing else: no
//! `GetQueueAttributes`.
//!
//! **Bodies.** A message body is an S3 event notification (`Records`), the same wrapped in an
//! SNS envelope (`"Type": "Notification"`, the notification in `Message`), or S3's
//! `s3:TestEvent`. Records go through the webhook's parser and comparison
//! ([`super::webhook::parse_notifications`]).
//!
//! **Delivery.** A message is deleted only once it is handed off: its events are recorded and
//! sent, or every record was ignored (another bucket, outside the prefix, already seen).
//! A hand-off that fails (the store, the event channel) leaves the message, and the ones after
//! it in the batch, in the queue: SQS delivers them again after the visibility timeout, and the
//! comparison with `objects_seen` makes that harmless (a row recorded whose event was not sent
//! stays `pending`, and ingest's sweep feeds it). A body that is not a notification is
//! logged and deleted, so that it cannot come back forever; the periodic scan catches whatever
//! it was about. (A redrive policy on the queue never sees it, since it is deleted on its first
//! receive.)
//!
//! **Failures** back off exponentially (1 s doubling to 60 s, with jitter); throttling and
//! server errors start at 1 s, other errors (access denied, no such queue) wait the longest
//! at once. Shutdown interrupts a long poll at once; a batch being handed off is finished
//! first.

use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use doewood_core::SourceId;
use doewood_store::{S3Credentials, Store, valid_queue_url};
use jiff::Timestamp;
use serde::Deserialize;
use tokio::sync::{mpsc, watch};
use tracing::{debug, info, warn};
use url::Url;

use super::webhook::{self, Notification, WebhookError};
use super::{EventInlet, InletStats};
use crate::error::transport;
use crate::retry::{RetryPolicy, jitter};
use crate::{EventOrigin, ObjectEvent, S3Error, sigv4};

/// The content type of the AWS JSON 1.0 protocol.
pub const CONTENT_TYPE: &str = "application/x-amz-json-1.0";

/// Most messages `ReceiveMessage` returns at once, and most entries of a batch delete.
pub const MAX_MESSAGES: u8 = 10;

/// Largest `ReceiveMessage` response read: ten messages of SQS's largest size (1 MiB) and
/// their attributes.
const RESPONSE_MAX: usize = 12 * 1024 * 1024;

/// Largest error body read to find the error code.
const ERROR_BODY_MAX: usize = 16 * 1024;

/// How the inlet polls.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SqsOptions {
    /// Long-poll wait of a `ReceiveMessage` (SQS allows 0 to 20 s).
    pub wait_time: Duration,
    /// Messages per `ReceiveMessage`, 1 to 10.
    pub max_messages: u8,
    /// How long received messages stay hidden from other receivers: the time a batch has to
    /// be handed off and deleted before SQS delivers it again.
    pub visibility_timeout: Duration,
    /// Time a request may take beyond the long-poll wait.
    pub request_margin: Duration,
    /// Backoff after the first failure; it doubles with each failure in a row.
    pub backoff_base: Duration,
    /// Longest backoff.
    pub backoff_max: Duration,
    /// Retries of a `DeleteMessageBatch` on throttling, server and transport errors.
    pub delete_retry: RetryPolicy,
}

impl Default for SqsOptions {
    fn default() -> Self {
        Self {
            wait_time: Duration::from_secs(20),
            max_messages: MAX_MESSAGES,
            visibility_timeout: Duration::from_mins(5),
            request_margin: Duration::from_secs(10),
            backoff_base: Duration::from_secs(1),
            backoff_max: Duration::from_mins(1),
            delete_retry: RetryPolicy::default(),
        }
    }
}

/// Where the queue is and how to sign for it.
#[derive(Clone, Debug)]
pub struct SqsConfig {
    /// The queue URL, `https://sqs.<region>.amazonaws.com/<account>/<queue>`.
    pub queue_url: String,
    /// The signing region.
    pub region: String,
    /// Credentials; `None` sends unsigned requests (local test queues only).
    pub credentials: Option<S3Credentials>,
}

/// A message as received.
#[derive(Clone, Debug, Deserialize)]
pub struct SqsMessage {
    /// Its id.
    #[serde(rename = "MessageId", default)]
    pub id: String,
    /// The handle that deletes it. Not logged.
    #[serde(rename = "ReceiptHandle")]
    pub receipt_handle: String,
    /// The body.
    #[serde(rename = "Body", default)]
    pub body: String,
    #[serde(rename = "Attributes", default)]
    attributes: std::collections::HashMap<String, String>,
}

impl SqsMessage {
    /// How many times SQS delivered it (`ApproximateReceiveCount`), if it said.
    pub fn receive_count(&self) -> Option<u32> {
        self.attributes
            .get("ApproximateReceiveCount")
            .and_then(|c| c.parse().ok())
    }
}

#[derive(Deserialize)]
struct ReceiveResponse {
    #[serde(rename = "Messages", default)]
    messages: Vec<SqsMessage>,
}

#[derive(Deserialize)]
struct DeleteResponse {
    #[serde(rename = "Failed", default)]
    failed: Vec<BatchFailure>,
}

/// An entry of a batch delete that SQS refused.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
pub struct BatchFailure {
    /// The entry id (its index in the batch).
    #[serde(rename = "Id")]
    pub id: String,
    /// The error code.
    #[serde(rename = "Code", default)]
    pub code: String,
    /// Whether the request was at fault.
    #[serde(rename = "SenderFault", default)]
    pub sender_fault: bool,
}

#[derive(Deserialize)]
struct ErrorBody {
    #[serde(rename = "__type")]
    kind: Option<String>,
    code: Option<String>,
    message: Option<String>,
    #[serde(rename = "Message")]
    message_upper: Option<String>,
}

/// A small SQS client for one queue: `ReceiveMessage` and `DeleteMessageBatch` over the AWS
/// JSON protocol. `Debug` shows no credential.
#[derive(Clone)]
pub struct SqsClient {
    http: reqwest::Client,
    endpoint: Url,
    queue_url: String,
    region: String,
    credentials: Option<S3Credentials>,
}

impl fmt::Debug for SqsClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SqsClient")
            .field("host", &self.endpoint.host_str())
            .field("region", &self.region)
            .field("signed", &self.credentials.is_some())
            .finish_non_exhaustive()
    }
}

impl SqsClient {
    /// A client on a shared HTTP client (see [`crate::http_client`]). Requests go to the queue
    /// URL's scheme, host and port.
    pub fn new(config: &SqsConfig, http: reqwest::Client) -> Result<Self, S3Error> {
        valid_queue_url(&config.queue_url)
            .map_err(|why| S3Error::Config(format!("the queue URL {why}")))?;
        let mut endpoint: Url = config
            .queue_url
            .parse()
            .map_err(|e| S3Error::Config(format!("the queue URL is not a URL: {e}")))?;
        endpoint.set_path("/");
        Ok(Self {
            http,
            endpoint,
            queue_url: config.queue_url.clone(),
            region: config.region.clone(),
            credentials: config.credentials.clone(),
        })
    }

    /// One long poll: up to `max_messages` messages, or none after `wait_time`.
    pub async fn receive(&self, options: &SqsOptions) -> Result<Vec<SqsMessage>, S3Error> {
        const OP: &str = "ReceiveMessage";
        let body = serde_json::json!({
            "QueueUrl": self.queue_url,
            "MaxNumberOfMessages": options.max_messages.clamp(1, MAX_MESSAGES),
            "WaitTimeSeconds": options.wait_time.as_secs().min(20),
            "VisibilityTimeout": options.visibility_timeout.as_secs(),
            "MessageSystemAttributeNames": ["ApproximateReceiveCount"],
        });
        let bytes = self
            .call(OP, &body, options.wait_time + options.request_margin)
            .await?;
        if bytes.iter().all(u8::is_ascii_whitespace) {
            return Ok(Vec::new());
        }
        let parsed: ReceiveResponse =
            serde_json::from_slice(&bytes).map_err(|e| S3Error::Response {
                op: OP,
                reason: e.to_string(),
            })?;
        Ok(parsed.messages)
    }

    /// Deletes up to 10 messages by receipt handle. Returns the entries SQS refused, by their
    /// index in `receipt_handles`. Retries the whole call on throttling, server and transport
    /// errors.
    pub async fn delete_batch(
        &self,
        receipt_handles: &[&str],
        retry: RetryPolicy,
    ) -> Result<Vec<BatchFailure>, S3Error> {
        const OP: &str = "DeleteMessageBatch";
        let entries: Vec<_> = receipt_handles
            .iter()
            .take(usize::from(MAX_MESSAGES))
            .enumerate()
            .map(|(i, h)| serde_json::json!({"Id": i.to_string(), "ReceiptHandle": h}))
            .collect();
        let body = serde_json::json!({"QueueUrl": self.queue_url, "Entries": entries});
        let mut attempt = 1_u32;
        loop {
            match self.call(OP, &body, Duration::from_secs(30)).await {
                Ok(bytes) => {
                    if bytes.iter().all(u8::is_ascii_whitespace) {
                        return Ok(Vec::new());
                    }
                    let parsed: DeleteResponse =
                        serde_json::from_slice(&bytes).map_err(|e| S3Error::Response {
                            op: OP,
                            reason: e.to_string(),
                        })?;
                    return Ok(parsed.failed);
                }
                Err(e) if retryable(&e) && attempt < retry.max_attempts => {
                    let delay = retry.delay(attempt, None);
                    debug!(op = OP, attempt, delay_ms = delay.as_millis(), error = %e, "retrying");
                    tokio::time::sleep(delay).await;
                    attempt += 1;
                }
                Err(e) => return Err(e),
            }
        }
    }

    /// Sends one action and returns the response body of a success.
    async fn call(
        &self,
        op: &'static str,
        body: &serde_json::Value,
        timeout: Duration,
    ) -> Result<Bytes, S3Error> {
        let payload = serde_json::to_vec(body).map_err(|e| S3Error::Config(e.to_string()))?;
        let target = format!("AmazonSQS.{op}");
        let mut request = self
            .http
            .post(self.endpoint.clone())
            .timeout(timeout)
            .header(reqwest::header::CONTENT_TYPE, CONTENT_TYPE)
            .header("x-amz-target", &target);
        if let Some(c) = &self.credentials {
            for (name, value) in sigv4::sign(
                "POST",
                &self.endpoint,
                &[("content-type", CONTENT_TYPE), ("x-amz-target", &target)],
                &payload,
                c,
                &self.region,
                "sqs",
                Timestamp::now(),
            ) {
                request = request.header(name, value);
            }
        }
        let mut response = request
            .body(payload)
            .send()
            .await
            .map_err(|e| transport(op, e))?;
        let status = response.status();
        let limit = if status.is_success() {
            RESPONSE_MAX
        } else {
            ERROR_BODY_MAX
        };
        let query_error = response
            .headers()
            .get("x-amzn-query-error")
            .and_then(|v| v.to_str().ok())
            .map(str::to_owned);
        let mut bytes = Vec::new();
        while let Some(chunk) = response.chunk().await.map_err(|e| transport(op, e))? {
            bytes.extend_from_slice(&chunk);
            if bytes.len() > limit {
                if status.is_success() {
                    return Err(S3Error::Response {
                        op,
                        reason: format!("response larger than {RESPONSE_MAX} bytes"),
                    });
                }
                bytes.truncate(limit);
                break;
            }
        }
        if status.is_success() {
            return Ok(Bytes::from(bytes));
        }
        let (code, message) = parse_error(query_error.as_deref(), &bytes);
        Err(S3Error::Queue {
            op,
            status: status.as_u16(),
            code,
            message,
        })
    }
}

/// The error code and message of a JSON protocol error: the code from `__type`
/// (`com.amazonaws.sqs#QueueDoesNotExist`), else `code`, else the `x-amzn-query-error` header
/// (`AWS.SimpleQueueService.NonExistentQueue;Sender`). Nothing else of the body is kept.
fn parse_error(query_error: Option<&str>, body: &[u8]) -> (Option<String>, Option<String>) {
    let cap = |s: &str| s.trim().chars().take(300).collect::<String>();
    let parsed: Option<ErrorBody> = serde_json::from_slice(body).ok();
    let (kind, code, message) = parsed.map_or((None, None, None), |e| {
        (e.kind, e.code, e.message.or(e.message_upper))
    });
    let code = kind
        .map(|k| {
            let k = k.rsplit('#').next().unwrap_or(&k);
            k.split(':').next().unwrap_or(k).to_owned()
        })
        .or(code)
        .or_else(|| {
            query_error
                .and_then(|q| q.split(';').next())
                .map(str::to_owned)
        })
        .filter(|c| !c.is_empty())
        .map(|c| cap(&c));
    (code, message.map(|m| cap(&m)).filter(|m| !m.is_empty()))
}

/// Whether a failure is worth retrying soon: throttling, server errors, transport failures
/// and unreadable responses.
pub fn retryable(e: &S3Error) -> bool {
    match e {
        S3Error::Queue { status, code, .. } => {
            matches!(status, 429 | 500 | 502 | 503 | 504)
                || matches!(
                    code.as_deref(),
                    Some(
                        "RequestThrottled"
                            | "ThrottlingException"
                            | "Throttling"
                            | "KmsThrottled"
                            | "OverLimit"
                            | "ServiceUnavailable"
                            | "InternalFailure"
                            | "InternalError"
                            | "RequestTimeout"
                    )
                )
        }
        S3Error::Transport { .. } | S3Error::Response { .. } => true,
        _ => false,
    }
}

/// What a message body is.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SqsBody {
    /// An S3 event notification, raw or from SNS: its records (other event types left out).
    Notifications(Vec<Notification>),
    /// S3's `s3:TestEvent`, sent when a notification configuration is saved.
    TestEvent,
    /// Anything else, and why (never the body itself).
    Malformed(String),
}

/// Reads a message body: an S3 event notification, the same in an SNS envelope (unless the
/// subscription uses raw message delivery, which gives the notification itself), or
/// `s3:TestEvent`.
pub fn parse_body(body: &str) -> SqsBody {
    let value: serde_json::Value = match serde_json::from_str(body) {
        Ok(v) => v,
        Err(e) => return SqsBody::Malformed(format!("not JSON: {e}")),
    };
    let is_sns = value.get("Type").and_then(serde_json::Value::as_str) == Some("Notification");
    let (inner, text) = match value.get("Message").and_then(serde_json::Value::as_str) {
        Some(message) if is_sns => match serde_json::from_str::<serde_json::Value>(message) {
            Ok(v) => (v, message),
            Err(e) => {
                return SqsBody::Malformed(format!("the SNS message is not JSON: {e}"));
            }
        },
        _ if is_sns => {
            return SqsBody::Malformed("an SNS notification without a message".to_owned());
        }
        _ => (value, body),
    };
    if inner.get("Event").and_then(serde_json::Value::as_str) == Some("s3:TestEvent") {
        return SqsBody::TestEvent;
    }
    if inner.get("Records").is_none_or(|r| !r.is_array()) {
        return SqsBody::Malformed("not an S3 event notification (no Records)".to_owned());
    }
    match webhook::parse_notifications(text.as_bytes()) {
        Ok(n) => SqsBody::Notifications(n),
        Err(e) => SqsBody::Malformed(e.to_string()),
    }
}

/// The SQS inlet of one source.
#[derive(Debug)]
pub struct SqsInlet {
    store: Arc<Store>,
    source: SourceId,
    client: SqsClient,
    options: SqsOptions,
    stats: Arc<InletStats>,
}

impl SqsInlet {
    /// An inlet for `source` reading from `client`'s queue, counting into `stats`.
    pub fn new(
        store: Arc<Store>,
        source: SourceId,
        client: SqsClient,
        options: SqsOptions,
        stats: Arc<InletStats>,
    ) -> Self {
        Self {
            store,
            source,
            client,
            options,
            stats,
        }
    }

    /// The delay after `failures` failures in a row: exponential, at least half the bound
    /// (so a persistent error never spins), with jitter. A failure that retrying soon does
    /// not fix waits the longest at once.
    fn backoff(&self, failures: u32, soon: bool) -> Duration {
        let n = if soon { failures } else { 32 };
        let bound = self
            .options
            .backoff_base
            .saturating_mul(1_u32.checked_shl(n.saturating_sub(1)).unwrap_or(u32::MAX))
            .min(self.options.backoff_max);
        bound / 2 + bound.mul_f64(jitter() / 2.0)
    }

    /// Hands a batch off, in order. Returns the receipt handles (with message ids) to delete
    /// and the failure that stopped the batch, if any: that message and the ones after it
    /// stay in the queue.
    async fn hand_off<'m>(
        &self,
        events: &mpsc::Sender<ObjectEvent>,
        messages: &'m [SqsMessage],
    ) -> (Vec<&'m SqsMessage>, Option<WebhookError>) {
        let mut done = Vec::with_capacity(messages.len());
        for m in messages {
            match parse_body(&m.body) {
                SqsBody::TestEvent => {
                    InletStats::add(&self.stats.test_events, 1);
                    info!(source = %self.source, message_id = %m.id, "SQS test event received");
                }
                SqsBody::Malformed(why) => {
                    InletStats::add(&self.stats.malformed, 1);
                    warn!(
                        source = %self.source,
                        message_id = %m.id,
                        reason = %why,
                        "SQS message is not an S3 event notification; deleting it"
                    );
                }
                SqsBody::Notifications(n) => {
                    match webhook::deliver(&self.store, events, self.source, n, EventOrigin::Sqs)
                        .await
                    {
                        Ok(out) => {
                            InletStats::add(&self.stats.emitted, out.emitted);
                            InletStats::add(&self.stats.ignored, out.ignored);
                            debug!(
                                source = %self.source,
                                message_id = %m.id,
                                emitted = out.emitted,
                                ignored = out.ignored,
                                "SQS notification handed off"
                            );
                        }
                        Err(e) => {
                            if !matches!(e, WebhookError::S3(S3Error::ChannelClosed)) {
                                warn!(
                                    source = %self.source,
                                    message_id = %m.id,
                                    receive_count = m.receive_count(),
                                    error = %e,
                                    "SQS notification not handed off; it stays in the queue"
                                );
                            }
                            return (done, Some(e));
                        }
                    }
                }
            }
            done.push(m);
        }
        (done, None)
    }

    /// Deletes handed-off messages, ten at a time. A message that is not deleted comes back
    /// after the visibility timeout, which is harmless.
    async fn delete(&self, done: &[&SqsMessage]) {
        for chunk in done.chunks(usize::from(MAX_MESSAGES)) {
            let handles: Vec<&str> = chunk.iter().map(|m| m.receipt_handle.as_str()).collect();
            match self
                .client
                .delete_batch(&handles, self.options.delete_retry)
                .await
            {
                Ok(failed) => {
                    InletStats::add(
                        &self.stats.deleted,
                        chunk.len() - failed.len().min(chunk.len()),
                    );
                    for f in &failed {
                        let id =
                            f.id.parse::<usize>()
                                .ok()
                                .and_then(|i| chunk.get(i))
                                .map_or("?", |m| m.id.as_str());
                        warn!(
                            source = %self.source,
                            message_id = %id,
                            code = %f.code,
                            "SQS did not delete a message; it will be received again"
                        );
                    }
                    if let Some(f) = failed.first() {
                        self.stats
                            .error(format!("DeleteMessageBatch: entry failed with {}", f.code));
                    }
                }
                Err(e) => {
                    warn!(source = %self.source, error = %e, "SQS DeleteMessageBatch failed; the messages will be received again");
                    self.stats.error(e.to_string());
                }
            }
        }
    }
}

/// Waits for `delay` or shutdown; true on shutdown.
async fn sleep_or_stop(shutdown: &mut watch::Receiver<bool>, delay: Duration) -> bool {
    tokio::select! {
        () = stopped(shutdown) => true,
        () = tokio::time::sleep(delay) => false,
    }
}

async fn stopped(shutdown: &mut watch::Receiver<bool>) {
    let _ = shutdown.wait_for(|stop| *stop).await;
}

impl EventInlet for SqsInlet {
    fn name(&self) -> &'static str {
        "sqs"
    }

    /// Polls until shutdown, the event channel closes, or the source is gone.
    async fn run(
        self,
        events: mpsc::Sender<ObjectEvent>,
        mut shutdown: watch::Receiver<bool>,
    ) -> Result<(), S3Error> {
        info!(
            source = %self.source,
            region = %self.client.region,
            signed = self.client.credentials.is_some(),
            "SQS inlet polling"
        );
        let mut failures = 0_u32;
        loop {
            if *shutdown.borrow() {
                break;
            }
            let received = tokio::select! {
                biased;
                () = stopped(&mut shutdown) => break,
                r = self.client.receive(&self.options) => r,
            };
            let messages = match received {
                Ok(m) => m,
                Err(e) => {
                    failures = failures.saturating_add(1);
                    let delay = self.backoff(failures, retryable(&e));
                    warn!(
                        source = %self.source,
                        error = %e,
                        retry_in_ms = delay.as_millis(),
                        "SQS ReceiveMessage failed"
                    );
                    self.stats.error(e.to_string());
                    if sleep_or_stop(&mut shutdown, delay).await {
                        break;
                    }
                    continue;
                }
            };
            InletStats::add(&self.stats.received, messages.len());
            let (done, failed) = self.hand_off(&events, &messages).await;
            if !done.is_empty() {
                self.delete(&done).await;
            }
            match failed {
                None => failures = 0,
                Some(WebhookError::S3(S3Error::ChannelClosed)) => {
                    info!(source = %self.source, "event channel closed; SQS inlet stops");
                    break;
                }
                Some(WebhookError::UnknownSource(_)) => {
                    info!(source = %self.source, "source removed; SQS inlet stops");
                    break;
                }
                Some(e) => {
                    failures = failures.saturating_add(1);
                    self.stats.error(format!("hand-off: {e}"));
                    let delay = self.backoff(failures, true);
                    if sleep_or_stop(&mut shutdown, delay).await {
                        break;
                    }
                }
            }
        }
        info!(source = %self.source, "SQS inlet stopped");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const RECORD: &str = r#"{"Records":[{"eventVersion":"2.1","eventSource":"aws:s3",
      "eventName":"ObjectCreated:Put","s3":{"bucket":{"name":"doewood-test-bucket"},
      "object":{"key":"in/a.pdf","size":3,"eTag":"abc"}}}]}"#;

    #[test]
    fn bodies_raw_sns_and_test_events() {
        let SqsBody::Notifications(n) = parse_body(RECORD) else {
            panic!("raw");
        };
        assert_eq!((n.len(), n[0].key.as_str()), (1, "in/a.pdf"));
        let sns = serde_json::json!({
            "Type": "Notification",
            "MessageId": "00000000-0000-0000-0000-000000000001",
            "TopicArn": "arn:aws:sns:eu-central-1:123456789012:doewood-test-topic",
            "Subject": "Amazon S3 Notification",
            "Message": RECORD,
        })
        .to_string();
        assert_eq!(parse_body(&sns), parse_body(RECORD));
        let test = r#"{"Service":"Amazon S3","Event":"s3:TestEvent","Time":"2026-01-02T03:04:05.000Z",
          "Bucket":"doewood-test-bucket","RequestId":"TEST","HostId":"TEST"}"#;
        assert_eq!(parse_body(test), SqsBody::TestEvent);
        let sns_test = serde_json::json!({"Type": "Notification", "Message": test}).to_string();
        assert_eq!(parse_body(&sns_test), SqsBody::TestEvent);
        for bad in [
            "not json",
            "{}",
            r#"{"Records":{}}"#,
            r#"{"Records":[{"eventName":"ObjectCreated:Put"}]}"#,
            r#"{"Type":"Notification","Message":"not json"}"#,
            r#"{"Type":"Notification"}"#,
            r#"{"version":"0","detail-type":"Object Created","source":"aws.s3"}"#,
        ] {
            assert!(matches!(parse_body(bad), SqsBody::Malformed(_)), "{bad}");
        }
    }

    #[test]
    fn json_errors_and_what_is_retried() {
        let body = br#"{"__type":"com.amazonaws.sqs#QueueDoesNotExist","message":"The specified queue does not exist."}"#;
        assert_eq!(
            parse_error(None, body),
            (
                Some("QueueDoesNotExist".into()),
                Some("The specified queue does not exist.".into())
            )
        );
        assert_eq!(
            parse_error(Some("AWS.SimpleQueueService.NonExistentQueue;Sender"), b"").0,
            Some("AWS.SimpleQueueService.NonExistentQueue".into())
        );
        assert_eq!(
            parse_error(
                None,
                br#"{"__type":"RequestThrottled:http://internal","Message":"slow"}"#
            ),
            (Some("RequestThrottled".into()), Some("slow".into()))
        );
        let status = |status: u16, code: &str| S3Error::Queue {
            op: "ReceiveMessage",
            status,
            code: Some(code.into()),
            message: None,
        };
        assert!(retryable(&status(400, "RequestThrottled")));
        assert!(retryable(&status(400, "ThrottlingException")));
        assert!(retryable(&status(503, "ServiceUnavailable")));
        assert!(!retryable(&status(400, "QueueDoesNotExist")));
        assert!(!retryable(&status(403, "InvalidClientTokenId")));
    }
}
