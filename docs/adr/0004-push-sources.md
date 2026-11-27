# ADR 0004: push sources

Status: proposed, 2026-11-27. Amends ADR 0001, section 5 ("Sources and jobs").

## Context

ADR 0001 has one kind of source: an S3-compatible bucket that doewood scans, with event inlets
to see changes sooner. Ingest fetches every object itself and the results arrive later, through
the API.

An application that embeds doewood already holds the bytes of every file, from its own
connectors: local folders, Drive, SharePoint, Dropbox, Box and so on. It needs each file
analyzed **synchronously**, inside its own processing step. Landing every file in S3 first would
store all the bytes twice, and the results would only come back later (issue #165).

## Decisions

### 1. A second source kind: `push`

- A source is either `s3` (ADR 0001 section 5, unchanged) or `push`. It is created with
  `ConfigService.AddSource` and `kind: SOURCE_KIND_PUSH`.
- A push source has **no scanner**, no bucket, prefix, region, URL style or credentials, and
  no event inlets. The application owns discovery, moves and deletions.
- It keeps what every source has: its state (`active` or `paused`), its LLM policy (ADR 0001
  section 6), its size cap (`max_object_bytes`, the largest file it takes) and its progress
  counters.
- **Identity.** Its endpoint is `push:<source id>`, with an empty bucket and prefix. No
  `http(s)://` endpoint can look like that, so the kind is read from it and the instance store
  needs no new column.
- **Claims.** It is claimed like any other source (ADR 0001 section 2), by the key
  `(push:<source id>, "", "")`: when `AddSource` adds it, at every start of `serve` with a
  shared store, and by `ResumeSource`. `ReleaseSourceClaim` takes that key too. A push source
  that another instance owns refuses every call.
- **Locations.** A pushed file's Location (ADR 0001 section 7) has the endpoint
  `push:<source id>`, an empty bucket and the application's key; its node key is
  `["push:<source id>", key]`. `ListRecords`, `Search` and the source filters select it the
  same way as an S3 object.

### 2. `IngestService.PutObject` (client-streamed)

- The request is a header `{source_id, key, name, mime_type, size}`, then the bytes in chunks.
  Each chunk is one gRPC message, at most `DOEWOOD_API_MAX_MESSAGE_MIB`; the whole file is at
  most the source's `max_object_bytes`. So a file is not limited to one message.
- It runs **the same analysis as S3 ingest**, in the same process and code: the hash, quail or
  hedgehog, and the record with its step versions. It stores the record, the Location, the
  graph and search, and queues a summary when the source's policy asks for one.
- **Idempotent per (BLAKE3, key).** If the key already holds the same content and its record is
  current, nothing is written (`UNCHANGED`). If the content is stored under another key with
  the running step versions, it is not analyzed again: only the location (with the graph and
  search for it) is written (`REUSED`). Otherwise it is analyzed (`ANALYZED`).
- **Response:** the record (as `GetRecord` returns it), its `blake3`, `kind` and `status`, and
  whether the format is unsupported (`skipped`). The document text (`want_text`) comes with
  issue #166.
- **Errors:**
  - `UNAVAILABLE` or `RESOURCE_EXHAUSTED` for transient failures: the caller retries later;
  - `INVALID_ARGUMENT` for a malformed request, and `FAILED_PRECONDITION` for a file that
    cannot be read or is too large, and for a source that is not a push source, is paused or
    belongs to another instance;
  - `NOT_FOUND` for an unknown source.
- **Back-pressure.** A push uses the same worker slots and memory budget as S3 ingest.
  - The call counts against the API's calls-in-flight cap. Beyond it, the call is shed with
    `UNAVAILABLE` and `grpc-retry-pushback-ms`, like every other call.
  - Before a byte is read, the file takes its share of the memory budget at once, and a
    worker slot. It waits for the slot only briefly, behind at most as many other pushes as
    there are workers.
  - Anything else is refused with `RESOURCE_EXHAUSTED` and a retry pushback. A burst is
    shed, and never queued in memory or allowed to run the instance out of memory.
  - `PutObject` has no server deadline, as `Summarize` has none. Each chunk must arrive
    within 60 s, and the analysis has its own time limits.

### 3. Location RPCs

- `MoveLocation(source_id, old_key, new_key)`: the new key gets the stored content, with the
  graph and search for it, and nothing is analyzed. The old key's `located_at` edge is ended
  (superseded), exactly as for an S3 delete.
- `RemoveLocation(source_id, key)`: the key's `located_at` edge is ended (superseded), as for an
  S3 delete. The document, its record and its other locations stay. It is idempotent.

### 4. What a push source does not do

- `ReprocessJob` is refused with `FAILED_PRECONDITION`: doewood keeps no bytes of a pushed file,
  so the application puts its files again to reanalyze them.
- `Summarize` by location is refused for the same reason. `Summarize` with the content works.
- The S3 options, SQS inlets and the event webhook do not apply: `UpdateSource` takes only
  `max_object_bytes`, `SetSqsInlet` is refused, and notifications naming a push source are
  ignored.

## Consequences

- An application gets doewood's analysis in its own processing step, without a copy of every
  file in S3. doewood stores what it stores for an S3 object: the record, the graph and
  search, never the bytes.
- doewood cannot re-read a pushed file. A new step version therefore marks its records stale,
  but nothing reanalyzes them until the application puts them again (the put then analyzes,
  since the stored record is stale).
- The application is the source of truth for what exists. A file it forgets to remove stays
  located in doewood.
- Pushes and S3 ingest share one instance's capacity. Under load, a push is refused rather
  than queued, so the application must retry with backoff (`grpc-retry-pushback-ms` gives the
  least delay).
- The shared Postgres schema does not change: a push location and a push claim are ordinary
  rows with an empty bucket.
