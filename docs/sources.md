# Sources and discovery

A **source** is an S3-compatible endpoint, a bucket and an optional key prefix
([ADR 0001](adr/0001-architecture.md), section 5), or a [push source](#push-sources) whose
bytes an application hands in ([ADR 0004](adr/0004-push-sources.md)). Sources are runtime settings: the
[gRPC API](api.md) adds, changes, pauses and removes them without a restart; they live in the
instance's SQLite store, and their credentials are sealed in its encrypted secrets.

Before a source is stored, the instance **claims** it (its endpoint, bucket and prefix) in the
shared store. A source another instance owns is refused with `ALREADY_EXISTS`, naming that
instance; removing the source releases the claim.

## Push sources

A **push source** ([ADR 0004](adr/0004-push-sources.md)) is the second kind of source: an
application hands doewood the bytes of each file through `IngestService.PutObject` and gets
the analysis back in the same call ([api.md](api.md#push-sources-ingestservice),
[ingest.md](ingest.md#push-sources)). It suits an application that already reads its files
from its own connectors (local folders, Drive, SharePoint, Dropbox, Box): nothing is copied to
S3 first.

- Add it with `AddSource` and `kind: SOURCE_KIND_PUSH`. It takes `max_object_bytes` (the largest
  file `PutObject` accepts, 512 MiB by default), `llm_policy` and `paused`, nothing else.
- It has **no scanner, no bucket and no event inlets**. The application owns discovery, moves
  (`MoveLocation`) and deletions (`RemoveLocation`); doewood never looks for changes.
- Its endpoint is `push:<id>`, with an empty bucket and prefix, and its documents' locations
  are `["push:<id>", key]`. `ListRecords` and the `Search` source filter work on it as on an S3
  source.
- It is **claimed** like any source, by `(push:<id>, "", "")`: `AddSource` claims it, `serve`
  re-claims it at start with a shared store, and `ResumeSource` claims it again. While another
  instance owns it, every `IngestService` call to it is `FAILED_PRECONDITION`.
  `ReleaseSourceClaim` takes `endpoint: "push:<id>"` with an empty bucket and prefix.
- Paused, it refuses `PutObject`, `MoveLocation` and `RemoveLocation` with
  `FAILED_PRECONDITION` until it is resumed.
- doewood keeps no bytes of a pushed file, so `ReprocessJob` and `Summarize` by location are
  refused. To reanalyze after an upgrade, put the files again: a stale record is analyzed again,
  a current one answers `UNCHANGED`.
- `UpdateSource` changes only `max_object_bytes`; `SetLlmPolicy` works as for any source.
  `SetSqsInlet` is refused, and event notifications naming it are ignored.

## Claims

A claim is a row in the shared store naming the owning instance (`DOEWOOD_INSTANCE_ID`) and when
it first claimed the source ([ADR 0001](adr/0001-architecture.md), section 2). It has no lease
and does not expire: it stays until its owner removes the source, or an operator releases it
with `ReleaseSourceClaim` after the owner is gone for good ([api.md](api.md#sources),
[deploy.md](deploy.md#a-dead-instance)).

With a shared Postgres, `serve` re-claims its sources at start, once the database answers
(before that, no scanner runs):

| the source in the shared store | result |
|---|---|
| claimed by nobody (for example added while standalone) | claimed for this instance, then scanned (a paused source stays paused) |
| already claimed by this instance | claim kept, with its first time; scanned |
| claimed by another instance | not taken over, not scanned, and the start goes on. An active or failing source turns `error` with `state_reason` `claimed by instance "<owner>" since <UTC time>; this instance does not scan it`; a paused one stays paused. A WARN names the owner. |
| the database fails during the pass | the whole pass is retried with backoff (1 s doubling to 60 s) |

`ListSources`, `GetSource` and `WatchProgress` show the state and the reason. Resuming such a
source claims it first: while the other instance holds the claim, `ResumeSource` fails with
`ALREADY_EXISTS` naming it; once that instance removes the source, `ResumeSource` claims and
scans it. Standalone, nothing of this runs: claims live in the instance's own file.

A source another instance owns gets no work done here (issue #25); the owner scans the bucket
itself:

- **Ingest's local queue.** Objects already pending for it (for example from before the claim
  went to the other instance) are dropped, not ingested: their pending rows are deleted, so a
  scan here sees them as new once this instance holds the claim again. The sweep drops them in
  one statement; an event in flight is dropped by the worker that takes it. A deletion event
  is left to the owner.
- **Event notifications.** The webhook answers `200` with the records counted as `ignored`, as
  for a paused source, and queues nothing.
- Both are counted in `doewood_ingest_claimed_elsewhere_dropped_total` (`path` `queue` or
  `webhook`, [api.md](api.md#metrics)) and logged at DEBUG (a sweep's drop at INFO, with the
  count).

The instance knows which sources another instance owns from the start-up claim pass and from
`ResumeSource`, and keeps that in memory: the check costs no database call per object. Until
the start-up pass has run, ingest holds its work. A paused source claimed elsewhere keeps its
state, and its queue is dropped too.

Known limits: an instance learns that another one owns a source only at start and when the
source is resumed. If a claim is released (`ReleaseSourceClaim`) while its owner still runs,
that owner keeps scanning until it restarts. Changing `DOEWOOD_INSTANCE_ID` makes the old name
the owner of every source: keep it stable (on Kubernetes it is the pod name), or release the
old name's claims as in [deploy.md](deploy.md#a-dead-instance).

## Any S3-compatible store

The client (`crates/doewood-s3`) is small on purpose: `rusty-s3` presigns each request with
SigV4 and `reqwest` sends it over HTTP/1.1 with rustls. It works with AWS, MinIO, Ceph, R2 and
Wasabi.

| setting | default | notes |
|---|---|---|
| endpoint | (required) | `http://` or `https://`, without credentials in the URL. A path (`https://gw/s3`) is kept as a prefix. |
| bucket, prefix | (bucket required) | An empty prefix watches the whole bucket. |
| region | `us-east-1` | What MinIO and Ceph accept by default; `auto` for R2. |
| URL style | path | `https://endpoint/bucket/key`. Virtual-host style (`https://bucket.endpoint/key`) for AWS, R2, Wasabi; it needs a DNS name, not an IP. |
| credentials | none (unsigned) | Access key id, secret key and an optional session token. Write-only. |
| scan interval | 300 s | Time from the end of one pass to the start of the next. |
| object size cap | 512 MiB | GetObject refuses larger objects (by `Content-Length`, and while streaming). |
| skip known copies | off | Record a new key whose single-part ETag and size match content already stored without fetching it. Trusts the ETag as the content's MD5: see [Known copies](#known-copies). |

Requests time out (10 s to connect, 60 s for a listing or HEAD, 60 s without data on a GetObject
body) and are retried with exponential backoff and full jitter on 429, 5xx, `SlowDown`,
`RequestTimeout` and transport errors (6 attempts, `Retry-After` honoured). Presigned URLs carry
the signature and any session token, so they are never logged and never part of an error.

## Known copies

`skip_known_copies` (`AddSource`, `UpdateSource`; `--skip-known-copies` for
`doewood dev add-source`) is off by default. With it on, a copy of content doewood already
stored (the same file under another key, in this source or another one) is not downloaded
again (issue #131):

- Every GetObject response of a single-part object without SSE-KMS or SSE-C records its ETag
  and size against the content's BLAKE3 hash (the instance store's `known_content` table,
  schema 13), whatever the source's setting.
- When a source has the setting on, a pending object whose listed ETag is 32 hex digits (no
  `-<parts>` suffix, so not a multipart upload) and whose listed size match such a row gets a
  HEAD. Only when the HEAD reports the same ETag and size, and no SSE-KMS
  (`x-amz-server-side-encryption: aws:kms` or `aws:kms:dsse`) and no SSE-C
  (`x-amz-server-side-encryption-customer-algorithm`), and the content's record is current,
  is the location recorded against that content, with the graph and search a full ingest of
  the same bytes would write. There is no GetObject and no hash. SSE-S3 (`AES256`) keeps the
  MD5 ETag and is allowed.
- Anything else fetches the object as before: no row, another size, a multipart ETag, SSE-KMS
  or SSE-C, another SSE value, a failed HEAD (an SSE-C object's HEAD without its key fails),
  an ETag that changed since the listing, or a record that is gone or older than the running
  step versions.
- The SSE type comes from the HEAD and GetObject headers. Listings do not carry it, so the
  HEAD is required. A store that sends no SSE header is taken as unencrypted. A store that
  does not report the SSE type is unknown, so its objects are never skipped.

**The trust assumption.** This takes the ETag for the MD5 of the content, as S3 defines it for
single-part uploads without SSE-KMS or SSE-C. MD5 is broken for collisions: **anyone who can
write to the bucket can make two different files with the same MD5 and size**, upload one where
doewood ingests it, then upload the other as a "copy". The copy is then recorded as the first
file's content, with its record, parties and search text. Turn the setting on only for buckets
whose writers you trust. doewood does not check that a store really computes MD5 ETags. A store
that computes them another way never matches, unless its ETags collide with real MD5s.

Each skipped fetch is counted in `doewood_ingest_known_copies_total`, and its listed bytes in
`doewood_ingest_known_copy_bytes_total` ([api.md](api.md#metrics)). The existing check that
skips a key whose ETag has not changed works with the setting off too.

## States

| state | scanned | meaning |
|---|---|---|
| `active` | yes | Scanned on its schedule. |
| `error` | yes | The last pass failed; the reason is kept. The pass is retried after the interval, and success makes the source `active` again. |
| `paused` | no | Its scanner stops, during a pass at the next page (the pass stays resumable). |
| `removed` | no | Kept until its rows are cleaned up. |

## The incremental scan

`serve` starts one scanner task per `active` or `error` source; with no sources it starts
nothing. A source added or resumed through the API starts its scanner at once; pausing or
removing one stops it, and an update restarts it with the new options. Each pass lists the source with `ListObjectsV2` and compares every key's ETag and size
with the `objects_seen` table:

- a key not seen before is **New**, a key whose ETag or size differs is **Changed**, and a seen
  key the listing no longer has is **Deleted**;
- the events go to a bounded channel (1024 events) that [ingest](ingest.md) consumes; a scanner
  waits while the channel is full, so a busy ingest slows the scan down instead of queueing.

The first pass is full: every key is new. Later passes emit only the differences.

**Memory is bounded.** S3 lists keys in byte order, which is also the order of `objects_seen`'s
primary key, so a pass walks one listing page (up to 1000 keys) alongside one chunk of stored
rows (up to 1000) at a time. A bucket with millions of keys never sits in memory.

**Checkpoints.** After each step, the scanner commits the row changes and the last key compared
(`scan_checkpoints`) in one transaction. A restart or a failed pass resumes from that key with
`StartAfter`, in the same pass. Continuation tokens are used between pages but not persisted,
because servers expire them. Delivery is at least once. A new or changed object's `pending` row
is committed before its event is sent, so the worker that gets the event finds the row (issue
#125); a crash or a stop in between leaves the row pending, and ingest's sweep picks it up. A
deleted object leaves no pending row, so its event is sent before the commit: a crash in
between sends it again. The webhook and SQS inlets use the same order for each notification.

A listing that is not in key order is refused (the source goes to `error`), since it cannot be
compared with the stored rows. Every general-purpose S3 implementation lists in order.

## Event inlets

Push notifications pick changes up within seconds; the scan catches anything they miss.

- **Webhook** (cargo feature `webhook`, on by default): `doewood_s3::inlet::webhook` parses MinIO
  and S3 event notification JSON (`ObjectCreated:*`, `ObjectRemoved:*`,
  `LifecycleExpiration:*`) and `WebhookInlet::handle(source, body)` turns it into the same events
  after comparing with `objects_seen`. The ops server mounts it at
  `POST /events/s3/{source_id}` (see [api.md](api.md#event-webhook)).
- **SQS** (cargo feature `sqs`, on by default): a poller per source, set through the API. See
  [SQS](#sqs).

## SQS

The SQS inlet long-polls a queue that S3 sends the bucket's event notifications to, directly or
through an SNS topic, and turns them into the same events as the webhook (origin `sqs`). It is
set per source with `ConfigService.SetSqsInlet` (see [api.md](api.md#sources)) and runs while
the source's scanner runs: it starts and stops with it (pause, resume, remove, the claim). The
periodic scan keeps running and catches anything the queue misses.

| setting | default | notes |
|---|---|---|
| queue URL | (required) | `https://sqs.<region>.amazonaws.com/<account>/<queue>`, as the SQS console shows it. Any `http(s)://host[:port]/<account>/<queue>` for an SQS-compatible server. No credentials, query or fragment. |
| region | from the URL | The region in an AWS queue URL, else `us-east-1`. |
| on/off | on | `disabled: true` keeps the settings without polling. |
| credentials | the source's | Credentials for the queue alone (an access key id, a secret key, an optional session token), write-only and sealed like the source's. Without them the inlet signs with the source's credentials; a source without credentials sends unsigned requests (local test queues only). |

**Protocol.** The AWS JSON protocol: `POST /` on the queue's host with
`Content-Type: application/x-amz-json-1.0`, `X-Amz-Target: AmazonSQS.ReceiveMessage` or
`AmazonSQS.DeleteMessageBatch`, the queue URL in the body, signed with SigV4 for the service
`sqs`. `ReceiveMessage` waits 20 s for up to 10 messages and hides them for 300 s (the
`VisibilityTimeout`), which is the time the batch has to be handed off and deleted.

**Messages.** A body is an S3 event notification (`Records`), the same in an SNS envelope (or
with SNS raw message delivery), or S3's `s3:TestEvent`, which is counted and deleted. Records
are parsed and compared with `objects_seen` like the webhook's: `ObjectCreated:*` becomes `new`
or `changed`, `ObjectRemoved:*` and `LifecycleExpiration:*` become `deleted`; records of another
bucket, outside the prefix or already seen are ignored (and counted).

**Delete and redelivery.** A message is deleted (`DeleteMessageBatch`, up to 10 at once) only
after its events are sent and recorded, or when all its records were ignored. If the hand-off
fails (the store, or the event channel at shutdown), that message and the rest of its batch stay
in the queue and SQS delivers them again after the visibility timeout; the comparison with
`objects_seen` makes a repeat harmless. A failed delete has the same effect.

**Malformed messages** (not JSON, no `Records`, an EventBridge event, an SNS envelope without a
message) are logged with their message id (never the body) and **deleted** on first receipt, so
they cannot loop. A redrive policy on the queue therefore never sees them; it only matters for
messages whose hand-off keeps failing. If you set one, give it a generous `maxReceiveCount`
(10 or more): what lands in the dead-letter queue is picked up by the next scan anyway.

**Failures** back off: 1 s doubling to 60 s with jitter for throttling (`RequestThrottled`,
`ThrottlingException`, `KmsThrottled`, `OverLimit`), 5xx and transport errors; 60 s at once for
other errors (`AccessDenied`, `QueueDoesNotExist`, bad credentials), which a retry does not fix
until the settings change. The source's state is not touched: the scan works on its own.
`Source.sqs_inlet` shows whether the inlet runs, its counters and the last error. On shutdown a
long poll is dropped at once; a batch being handed off is finished first.

Receipt handles, signatures, session tokens and keys are never logged or returned.

### Setting it up on AWS

1. Create a **standard** queue (S3 notifications cannot go to a FIFO queue). Allow S3 to send to
   it with the queue's access policy (the bucket and account below are examples):

   ```json
   {"Version": "2012-10-17",
    "Statement": [{"Effect": "Allow", "Principal": {"Service": "s3.amazonaws.com"},
      "Action": "sqs:SendMessage",
      "Resource": "arn:aws:sqs:eu-central-1:123456789012:doewood-events",
      "Condition": {"ArnEquals": {"aws:SourceArn": "arn:aws:s3:::example-docs"},
                    "StringEquals": {"aws:SourceAccount": "123456789012"}}}]}
   ```

   Only S3 (or your SNS topic) should be able to send: a message in the queue makes doewood
   compare the keys it names (a forged removal marks a key deleted until the next scan finds it
   again). SNS signatures are not checked.
2. On the bucket, add an event notification for `s3:ObjectCreated:*` and `s3:ObjectRemoved:*`
   (and `s3:LifecycleExpiration:*` if objects expire) with the source's prefix as the filter,
   and the queue (or an SNS topic the queue subscribes to) as the destination. S3 then sends a
   test event, which shows up as `test_events: 1`.
3. Give the credentials doewood signs with these permissions on the queue:

   ```json
   {"Version": "2012-10-17",
    "Statement": [{"Effect": "Allow",
      "Action": ["sqs:ReceiveMessage", "sqs:DeleteMessage"],
      "Resource": "arn:aws:sqs:eu-central-1:123456789012:doewood-events"}]}
   ```

   `sqs:DeleteMessage` covers `DeleteMessageBatch`. `sqs:GetQueueAttributes` is not used. A
   queue encrypted with a customer-managed KMS key also needs `kms:Decrypt` on that key (and the
   key policy must let S3 use it to send).
4. Call `SetSqsInlet` with the queue URL, and the queue's own credentials if the source's (for
   example MinIO's) are not AWS credentials.

## Trying it locally

Add a source through the API (see [api.md](api.md#examples-with-grpcurl)), or, for scripts and
without a running server, with the hidden `dev add-source` command. It goes through the same
code as `ConfigService.AddSource` (the same validation and claim):

```sh
docker run --rm -d --name minio -p 59000:9000 minio/minio server /data   # a local image
# (Docker Hub no longer serves minio/minio; CI uses bitnamilegacy/minio:2025.7.23 with
#  MINIO_ROOT_USER / MINIO_ROOT_PASSWORD set to the same development login)
# create a bucket and upload a few files (for example with `mc`), then:
export MY_ACCESS_KEY=minioadmin MY_SECRET_KEY=minioadmin   # MinIO's development login
doewood dev add-source --endpoint http://127.0.0.1:59000 --bucket docs --prefix in/ \
    --interval-secs 30 --access-key-env MY_ACCESS_KEY --secret-key-env MY_SECRET_KEY
RUST_LOG=info,doewood=debug doewood serve
docker stop minio
```

The keys are read from the variables you name, so they stay out of the shell history. Other
options: `--region`, `--virtual-host`, `--max-object-mib`, `--session-token-env`, `--paused`.

## Tests

`cargo test` runs the unit tests: SigV4 against AWS's published examples (the presigned S3
URLs and the header-signed SQS requests, with AWS's SigV4 test suite), the diff, checkpoints
and resume, the schedule and states, and notification parsing. `crates/doewood-s3/tests/sqs.rs`
runs the SQS inlet against a mock SQS on 127.0.0.1 that speaks the JSON protocol: receive and
delete, SNS envelopes, test events, foreign buckets, malformed bodies, throttling and backoff,
redelivery after a failed hand-off, shutdown during a long poll, and credentials kept out of
`Debug`, errors, logs and the wire. Nothing reaches AWS. The integration tests in
`crates/doewood-s3/tests/minio.rs` run against a real server only when
`DOEWOOD_TEST_S3_ENDPOINT` is set (CI's `minio` job does):

```sh
DOEWOOD_TEST_S3_ENDPOINT=http://127.0.0.1:59000 cargo test -p doewood-s3 --test minio
```
