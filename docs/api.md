# The gRPC API

doewood serves its API with gRPC on `DOEWOOD_LISTEN` (default `127.0.0.1:7443`), as described in
[ADR 0001](adr/0001-architecture.md), section 4. The protobuf files are in
[`proto/doewood/v1/`](../proto/doewood/v1), in the versioned package `doewood.v1`. Breaking
changes go to a new package (`doewood.v2`), never into v1.

- **gRPC and gRPC-Web** on the same port. HTTP/1.1 is accepted, so a browser can call the API
  with gRPC-Web (`application/grpc-web+proto`, or the base64 `application/grpc-web-text`).
  No CORS headers are sent: serve the frontend from the same origin as the API (one ingress).
  With no authentication in v1, a CORS policy would let any page a user opens call the API.
- **Health** (`grpc.health.v1.Health`) and **reflection** (`grpc.reflection.v1` and
  `v1alpha`), so `grpcurl` and Kubernetes gRPC probes work without the `.proto` files.
- **No authentication in v1.** The API binds to localhost by default; on Kubernetes, network
  isolation protects it. Every doewood service runs behind one (empty) interceptor, where
  tokens or OIDC go later without changing the API. Health and reflection stay open.
- **Message size:** up to `DOEWOOD_API_MAX_MESSAGE_MIB` (default 64 MiB) each way, room for a
  file sent to `Summarize`. Larger messages are refused (`OUT_OF_RANGE`).
- **Secrets are write-only.** S3 credentials and AI API keys are sealed with the master key;
  no response and no error carries one, only `credentials_set` or `api_key_set`. Error
  messages never echo a value that may hold a secret (an endpoint with `user:password@`, a
  key), nor a presigned URL.

The S3 event webhook is not part of the gRPC API: it is on the ops server (see
[Event webhook](#event-webhook)).

## Services

| service | RPCs |
|---|---|
| `ConfigService` | `AddSource`, `UpdateSource`, `PauseSource`, `ResumeSource`, `RemoveSource`, `ListSources`, `GetSource`, `SetLlmPolicy`, `SetAiProvider`, `ListAiProviders`, `GetIngestSettings`, `SetIngestSettings` |
| `JobsService` | `WatchProgress` (server stream), `ReprocessJob`, `WatchReprocess` (server stream) |
| `RecordsService` | `GetRecord`, `ListRecords` |
| `GraphService` | `GetNode`, `Neighbours`, `CompanyDocuments` |
| `SearchService` | `Search` |
| `SummarizeService` | `Summarize` |

The comments in the `.proto` files are the reference for every field. In short:

### Sources

- **AddSource** first validates the request, then **claims** the source (its endpoint, bucket
  and prefix) in the shared store. When another instance owns it, the call fails with
  `ALREADY_EXISTS`, and the message names the owner, for example
  `source http://minio:9000/docs/in/ is already claimed by instance "doewood-1"`. The source is
  then stored with its credentials sealed, and an active source starts scanning at once, without
  a restart. `paused: true` adds it without scanning.
- The endpoint is normalized before the claim (scheme and host lowercased, the default port
  and a trailing `/` dropped), so `HTTP://MinIO:9000/` and `http://minio:9000` are one source.
- **UpdateSource** changes the region, URL style, scan interval, size cap or credentials
  (`clear_credentials` deletes them). The endpoint, bucket and prefix are the source's identity
  and do not change. A running scanner restarts, so new options apply at once (with a shared
  Postgres, a source that is not scanned, for example one claimed by another instance, is not
  started).
- **PauseSource** stops the scanner (a pass under way keeps its checkpoint); **ResumeSource**
  starts it again, and retries a failing source right away. With a shared Postgres it claims the
  source first: when another instance owns it, the source turns `error` (the owner in
  `state_reason`) and the call fails with `ALREADY_EXISTS`. Before the start-up claim pass has
  run, it only stores the state, and the pass starts the scanner (see
  [sources](sources.md#claims)).
- **RemoveSource** releases the claim, stops the scanner, and deletes the source with its
  progress and credentials. If the shared store is unreachable it fails with `UNAVAILABLE` and
  removes nothing, so no claim is left behind.
- While the shared Postgres is not connected yet, `AddSource`, `RemoveSource` and the records,
  graph and search RPCs answer `UNAVAILABLE`; the rest works. Once the start-up claim pass has
  run, `ResumeSource` needs the database too (it claims the source).

### LLM policy and AI providers

- **SetLlmPolicy** sets which documents of a source get an LLM summary: `LLM_MODE_OFF` (the
  default), `LLM_MODE_ALL`, `LLM_MODE_KINDS` with `kinds`, or `LLM_MODE_ON_DEMAND`; with a
  concurrency (1 to 64, default 2) and an optional daily token budget (tokens per UTC day). It
  applies to documents ingested from now on and to those waiting in the queue. See
  [llm.md](llm.md).
- **SetAiProvider** configures a provider with a preset, a model, an optional base URL and a
  write-only API key. The presets are those of quail's `ApiConfig` and hedgehog: `openai`,
  `openai_compatible` (needs `base_url`), `anthropic`, `bedrock`, `openrouter`, `groq`, `vllm`
  and `ollama`. The name defaults to the preset, so several providers of one preset can live
  side by side under other names. An empty `api_key` keeps the stored key; `clear_api_key`
  deletes it. The first provider set becomes the default; `make_default` moves the default.
  The LLM queue uses the default provider. For `bedrock`, `base_url` is required
  (`https://bedrock-runtime.<region>.amazonaws.com`): the region is read from it.
- **ListAiProviders** lists them with `api_key_set`, never the key, plus every preset name.

### Ingest settings

- **GetIngestSettings** returns ingest's runtime settings, defaults filled in.
- **SetIngestSettings** changes them; an unset field keeps its value. `similar_min_jaccard` is
  the least MinHash Jaccard estimate for a `similar_to` edge: above 0 and at most 1 (default
  0.8), anything else is `INVALID_ARGUMENT`. Ingest reads it for every file, so it applies to
  the next file without a restart. Edges already written are kept; `ReprocessJob` with
  `force: true` compares a source's documents again (a lower value adds edges, a higher one
  removes none). It replaces the old `DOEWOOD_SIMILAR_MIN_JACCARD` variable (see
  [configuration.md](configuration.md)).

### Progress

**WatchProgress** streams one `KIND_SNAPSHOT` update per source, then an update whenever a
source changes: `KIND_COUNTERS`, `KIND_STATE_CHANGED` (with `previous_state`) or `KIND_REMOVED`.
It ends when the client cancels or the server shuts down. `source_ids` narrows it; empty watches
every source, including ones added later.

| counter | meaning |
|---|---|
| `seen` | objects under the prefix that discovery knows about |
| `new`, `changed`, `deleted` | discovery events since this instance started (scans and the webhook) |
| `processed`, `failed`, `pending` | objects ingested (`done`), failed (after their retries), and not ingested yet |
| `queued_llm` | documents waiting in the LLM queue |
| `summarized`, `llm_failed` | documents the LLM queue summarized, and those whose summary failed (`llm_last_error` on the source's progress has the latest error) |
| `llm_tokens_today` | tokens the source's summaries spent on the current UTC day (what its daily budget is checked against) |

### Records, graph and search

- **GetRecord** takes a BLAKE3 in hex; the record's JSON is in `body_json`, exactly as stored.
- **ListRecords** pages through the records of a source's files, ordered by hash: pass
  `next_page_token` back as `page_token` until it comes back empty.
- **GetNode** finds a node by id or by kind and key; **Neighbours** follows its edges with the
  filters of the model's `NeighbourQuery` (direction, edge kinds, neighbour kind, a day the edge
  must hold on, inactive edges); **CompanyDocuments** lists the documents with a live `party`
  edge to a company.
- **Search** matches every word of `text` and every filter that is set (company, kind, date and
  amount ranges, source), with `limit` (up to 1000) and `offset`.

Dates are `YYYY-MM-DD` strings; JSON props and evidence are strings holding JSON.

### Reprocessing

- **ReprocessJob** starts a one-shot job for a source and answers at once with its `job_id`. It
  puts back in the ingest queue every ingested file whose stored step versions differ from the
  running engine's (`doewood.ingest`, the quail plan fingerprint, and the hedgehog revision for
  spreadsheets); with `force: true`, every live file of the source. Without ingest on the
  instance it answers `UNAVAILABLE`.
- **WatchReprocess** streams the job's progress: `STATE_SELECTING` (with `examined` and
  `selected` growing), `STATE_RUNNING` (the source's `processed`, `failed` and `pending`), and
  last `STATE_DONE` (nothing of the source pending) or `STATE_FAILED` with `error`; then the
  stream ends. Jobs live in the instance that started them, until it restarts (`NOT_FOUND`
  after that).

```sh
JOB=$(grpcurl -plaintext -d "{\"source_id\": \"$ID\"}" 127.0.0.1:7443 \
    doewood.v1.JobsService/ReprocessJob | jq -r .jobId)
grpcurl -plaintext -d "{\"job_id\": \"$JOB\"}" 127.0.0.1:7443 doewood.v1.JobsService/WatchReprocess
```

### Summarize

**Summarize** summarizes one file now, outside the LLM queue, and stores nothing (no record, no
summary, no spend). Send the bytes (`content`, with `name`) or an object of a source
(`location`); a source whose LLM policy is `LLM_MODE_OFF` is refused. `provider` names an AI
provider; empty uses the default. The answer has the summary (`digest`), the fact check of the
model's output, the provider and model, the file's BLAKE3 and kind. When the provider fails or
its output fails the fact check, the answer is quail's extractive summary with `extractive`
set and the reason in `warnings`. Without a configured provider it answers
`FAILED_PRECONDITION`. See [llm.md](llm.md).

## Errors

| code | when |
|---|---|
| `INVALID_ARGUMENT` | a field is wrong. Every problem is reported at once: the message lists `field: reason` pairs, and a `google.rpc.BadRequest` detail carries the same field paths (`credentials.access_key_id`, `llm_policy.kinds[2]`, ...). |
| `NOT_FOUND` | the source, record, node or provider does not exist |
| `ALREADY_EXISTS` | the source exists on this instance, or another instance has claimed it (named in the message) |
| `UNAVAILABLE` | the shared Postgres is not connected (yet), or a call to it failed; retry. `ReprocessJob` and `Summarize` on an instance without ingest; `Summarize` of an object that cannot be read |
| `FAILED_PRECONDITION` | the shared database was migrated by a newer doewood. `Summarize` without a configured AI provider (or with one missing its key or base URL), of a source whose LLM policy is off, or of a file without text |
| `OUT_OF_RANGE` | a message larger than the limit |
| `INTERNAL` | anything else; the details are in the server log, not in the message |

## Examples with grpcurl

Reflection is on, so `grpcurl` needs no `.proto` files. `-plaintext`: the API speaks
cleartext HTTP/2 (put TLS in the ingress).

```sh
grpcurl -plaintext 127.0.0.1:7443 list
grpcurl -plaintext 127.0.0.1:7443 describe doewood.v1.ConfigService
grpcurl -plaintext 127.0.0.1:7443 grpc.health.v1.Health/Check
```

Add a source (MinIO's development login here; pass real keys from a file or a variable, not
the command line):

```sh
grpcurl -plaintext -d @ 127.0.0.1:7443 doewood.v1.ConfigService/AddSource <<'EOF'
{"endpoint": "http://127.0.0.1:9000", "bucket": "docs", "prefix": "in/",
 "credentials": {"access_key_id": "minioadmin", "secret_access_key": "minioadmin"},
 "scan_interval_seconds": 60}
EOF
```

The answer has the source's `id`, `"credentialsSet": true` and no credentials. Then:

```sh
ID=...   # the id from AddSource
grpcurl -plaintext 127.0.0.1:7443 doewood.v1.ConfigService/ListSources
grpcurl -plaintext -d "{\"source_id\": \"$ID\"}" 127.0.0.1:7443 doewood.v1.ConfigService/PauseSource
grpcurl -plaintext -d "{\"source_id\": \"$ID\"}" 127.0.0.1:7443 doewood.v1.ConfigService/ResumeSource
grpcurl -plaintext -d "{\"source_id\": \"$ID\", \"scan_interval_seconds\": 600}" \
    127.0.0.1:7443 doewood.v1.ConfigService/UpdateSource
grpcurl -plaintext -d "{\"source_id\": \"$ID\", \"policy\": {\"mode\": \"LLM_MODE_KINDS\",
    \"kinds\": [\"invoice\", \"contract\"], \"concurrency\": 2}}" \
    127.0.0.1:7443 doewood.v1.ConfigService/SetLlmPolicy
grpcurl -plaintext -d "{\"source_id\": \"$ID\"}" 127.0.0.1:7443 doewood.v1.ConfigService/RemoveSource
```

An AI provider (the key is read from a variable, and never comes back):

```sh
grpcurl -plaintext -d @ 127.0.0.1:7443 doewood.v1.ConfigService/SetAiProvider <<EOF
{"preset": "anthropic", "model": "claude-sonnet-4-5", "api_key": "$ANTHROPIC_API_KEY"}
EOF
grpcurl -plaintext -d @ 127.0.0.1:7443 doewood.v1.ConfigService/SetAiProvider <<'EOF'
{"name": "local", "preset": "openai_compatible", "model": "qwen2.5-7b-instruct",
 "base_url": "http://vllm:8000/v1", "make_default": true}
EOF
grpcurl -plaintext 127.0.0.1:7443 doewood.v1.ConfigService/ListAiProviders
```

Ingest settings:

```sh
grpcurl -plaintext -d '{"similar_min_jaccard": 0.85}' \
    127.0.0.1:7443 doewood.v1.ConfigService/SetIngestSettings
grpcurl -plaintext 127.0.0.1:7443 doewood.v1.ConfigService/GetIngestSettings
```

Progress, records, the graph and search:

```sh
grpcurl -plaintext -d '{}' 127.0.0.1:7443 doewood.v1.JobsService/WatchProgress   # Ctrl-C to stop
grpcurl -plaintext -d "{\"source_id\": \"$ID\", \"page_size\": 100}" \
    127.0.0.1:7443 doewood.v1.RecordsService/ListRecords
grpcurl -plaintext -d '{"blake3": "<64 hex digits>"}' 127.0.0.1:7443 doewood.v1.RecordsService/GetRecord
grpcurl -plaintext -d '{"key": {"kind": "NODE_KIND_COMPANY", "key": "acme"}}' \
    127.0.0.1:7443 doewood.v1.GraphService/GetNode
grpcurl -plaintext -d '{"node_id": "<uuid>", "direction": "DIRECTION_IN", "edge_kinds": ["EDGE_KIND_PARTY"]}' \
    127.0.0.1:7443 doewood.v1.GraphService/Neighbours
grpcurl -plaintext -d '{"company_id": "<uuid>"}' 127.0.0.1:7443 doewood.v1.GraphService/CompanyDocuments
grpcurl -plaintext -d '{"text": "invoice", "kind": "invoice", "date_from": "2026-01-01", "amount_min": 100, "limit": 20}' \
    127.0.0.1:7443 doewood.v1.SearchService/Search
```

A validation error lists every field:

```console
$ grpcurl -plaintext -d '{"endpoint": "http://user:pw@minio:9000", "bucket": "B!"}' \
    127.0.0.1:7443 doewood.v1.ConfigService/AddSource
ERROR:
  Code: InvalidArgument
  Message: invalid argument: endpoint: must not contain credentials (user:password@); bucket: must be 3 to 255 ASCII letters, digits, '.', '-' or '_', starting with a letter or digit
```

## gRPC-Web

A browser uses a gRPC-Web client (`@connectrpc/connect-web`, `grpc-web`, or generated code from
the `.proto` files) against the same address. On the wire it is a plain HTTP/1.1 POST:

```sh
# An empty ListAiProvidersRequest: one frame, flag 0, length 0.
printf '\x00\x00\x00\x00\x00' | curl -s --http1.1 -X POST \
    -H 'Content-Type: application/grpc-web+proto' -H 'X-Grpc-Web: 1' --data-binary @- \
    http://127.0.0.1:7443/doewood.v1.ConfigService/ListAiProviders | xxd | head
```

The body holds the message frame and then a trailer frame with `grpc-status:0`.

## Event webhook

MinIO and S3 event notifications go to the **ops** server (`DOEWOOD_OPS_LISTEN`, default
`127.0.0.1:7444`), one URL per source:

```
POST /events/s3/{source_id}
```

The body is the notification JSON (at most 1 MiB). Each record is compared with what the source
has seen, and new, changed or removed objects become the same events a scan emits; the scan
still catches anything a notification misses. The answer is `200 {"emitted": n, "ignored": m}`
(ignored: another bucket, outside the prefix, already seen, or the source is paused), `400` for a
body that is not a notification, `404` for an unknown source, `413` for a body over 1 MiB, and
`503` before the store is open and during shutdown. Like the API, it has no authentication in
v1: keep the ops port inside the cluster.

With MinIO (`mc`), for example:

```sh
mc admin config set local notify_webhook:doewood endpoint="http://doewood:7444/events/s3/$ID"
mc admin service restart local
mc event add local/docs arn:minio:sqs::doewood:webhook --event put,delete --prefix in/
```

## Scripts without a server

`doewood dev add-source` (hidden) adds a source through the same code as `AddSource` (the same
validation and claim), without a running server; `serve` then scans it. See
[sources.md](sources.md#trying-it-locally).

## ListRecords on Postgres

`ListRecords` is one query: the records with a location in the source (`EXISTS` over
`locations`), after the page token, ordered by hash. Postgres runs it as a merge semi join over
`records_pkey` and `locations_by_blake3`. It does not carry the `after` bound over to the
`locations` side, so before issue #13 each page read every location before the page token, and
walking all pages of a source was quadratic. The bound is now in the `EXISTS` as well. The
results are the same (`list_records_matches_the_query_before_issue_13` in
`crates/doewood-pg/src/tests/list_records.rs` walks both queries over six filters and four page
sizes).

```
DOEWOOD_TEST_POSTGRES_URL=postgres://postgres:pw@127.0.0.1:5432/postgres DOEWOOD_BENCH_RECORDS=50000 \
  cargo test -p doewood-pg list_records_timings -- --ignored --nocapture
```

The data is generated: each record has about 1 KB of JSON and one location, in one of four
buckets (`b0` to `b3`). Every 5th record is also in `b4`, every 7th has two keys in its
bucket, every 13th is on another endpoint and every 11th has no location. The query runs
against bucket `b0`. Times are medians of 15 runs (5 for full walks), in ms, measured from the
client. The setup: `pgvector/pgvector:pg17` (17.12) in Docker on the 4-core server, test
profile, commit `fa9f72f` plus this change.

| case | 5,000 before | 5,000 after | 50,000 before | 50,000 after |
|---|---|---|---|---|
| bucket, first page (50) | 2.06 | 1.95 | 1.02 | 0.98 |
| bucket, page halfway (50) | 2.38 | 1.24 | 13.30 | 1.17 |
| bucket, 1000 per page | 9.61 | 10.07 | 15.72 | 13.67 |
| prefix with 5 %, first page | 1.51 | 1.72 | 1.72 | 1.61 |
| rare prefix (49 records) | 2.03 | 2.22 | 12.53 | 13.07 |
| prefix matching nothing | 1.37 | 0.89 | 9.80 | 9.44 |
| all pages of the bucket, 50 each | 44.0 | 31.5 | 2,793 | 341 |
| all pages of the bucket, 1000 each | 10.3 | 11.2 | 241 | 184 |
| all pages of the 5 % prefix, 50 each | 9.8 | 9.6 | 628 | 203 |

With 50,000 records, the page halfway through read 32,024 buffers before and 485 after: the
`locations` index scan starts at the page token now. A prefix that matches few or no keys
gets a sequential scan of `locations`, which grows with the table: about 10 ms at 62,597
locations. The default collation does not let `starts_with` use the
`(endpoint, bucket, object_key)` index. In a trial with a similar 60,000 locations, an index on
`(endpoint, bucket, object_key COLLATE "C")` brought these cases from 7 to 11 ms to under
1.2 ms. It needs a schema migration, though, and a
database migrated by a newer doewood is refused by older instances
([configuration.md](configuration.md#the-shared-postgres)). So it is left for a release that
upgrades every instance together.

Standalone SQLite probes `locations` once per record, but `?4 IS NULL OR r.blake3 > ?4` kept
it from starting at the page token, so it too read every record before the page. It now
compares with `coalesce(?4, x'')` and searches the primary key from the token. In a trial with
Python's sqlite3 (3.46.1, in memory, 50,000 records, the same query), walking a bucket 50 per
page took 471 ms before and 69 ms after, with the same results.

## Tests

`cargo test -p doewood-api` starts in-process servers and calls every RPC with a tonic client:
the happy paths, validation errors, two instances sharing one backend (the claim conflict),
secrets never coming back, the progress stream, a gRPC-Web request over HTTP/1.1, health,
reflection and the message limit. With `DOEWOOD_TEST_POSTGRES_URL` set, the claim, records,
graph and search tests also run against Postgres.
