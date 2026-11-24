# Sources and discovery

A **source** is an S3-compatible endpoint, a bucket and an optional key prefix
([ADR 0001](adr/0001-architecture.md), section 5). Sources are runtime settings: the
[gRPC API](api.md) adds, changes, pauses and removes them without a restart; they live in the
instance's SQLite store, and their credentials are sealed in its encrypted secrets.

Before a source is stored, the instance **claims** it (its endpoint, bucket and prefix) in the
shared store. A source another instance owns is refused with `ALREADY_EXISTS`, naming that
instance; removing the source releases the claim.

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

Requests time out (10 s to connect, 60 s for a listing or HEAD, 60 s without data on a GetObject
body) and are retried with exponential backoff and full jitter on 429, 5xx, `SlowDown`,
`RequestTimeout` and transport errors (6 attempts, `Retry-After` honoured). Presigned URLs carry
the signature and any session token, so they are never logged and never part of an error.

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
because servers expire them. Events are sent before they are committed, so delivery is at least
once: a crash in between sends them again.

A listing that is not in key order is refused (the source goes to `error`), since it cannot be
compared with the stored rows. Every general-purpose S3 implementation lists in order.

## Event inlets

Push notifications pick changes up within seconds; the scan catches anything they miss.

- **Webhook** (cargo feature `webhook`, on by default): `doewood_s3::inlet::webhook` parses MinIO
  and S3 event notification JSON (`ObjectCreated:*`, `ObjectRemoved:*`,
  `LifecycleExpiration:*`) and `WebhookInlet::handle(source, body)` turns it into the same events
  after comparing with `objects_seen`. The ops server mounts it at
  `POST /events/s3/{source_id}` (see [api.md](api.md#event-webhook)).
- **SQS**: not built yet; it will implement the `EventInlet` trait.

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

`cargo test` runs the unit tests: SigV4 against AWS's published example, the diff, checkpoints
and resume, the schedule and states, and notification parsing. The integration tests in
`crates/doewood-s3/tests/minio.rs` run against a real server only when
`DOEWOOD_TEST_S3_ENDPOINT` is set (CI's `minio` job does):

```sh
DOEWOOD_TEST_S3_ENDPOINT=http://127.0.0.1:59000 cargo test -p doewood-s3 --test minio
```
