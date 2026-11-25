# Configuration

doewood has two kinds of settings, and each setting has exactly one source
([ADR 0001](adr/0001-architecture.md), section 3):

- **Bootstrap values** come from environment variables, listed below. Every one has a default,
  so `doewood serve` starts with no environment at all.
- **Runtime settings** (S3 sources and credentials, AI providers, policies, schedules, ingest
  tuning, company enrichment and icons) are set through the [gRPC API](api.md) and kept in the instance's SQLite store.
  Secrets are encrypted with the master key and are write-only: the API never returns them.

## Runtime settings

Set through the API, kept in `doewood.sqlite`, applied without a restart:

| setting | set with | stored as |
|---|---|---|
| S3 sources and their options | `AddSource`, `UpdateSource` | `sources` rows |
| S3 credentials | `AddSource`, `UpdateSource` | secret `s3/<source id>` (sealed, write-only) |
| A source's SQS inlet: queue URL, region, on/off (see [sources.md](sources.md#sqs)) | `SetSqsInlet`, `RemoveSqsInlet` | setting `sqs_inlet/<source id>` |
| The SQS inlet's own credentials (unset: the source's) | `SetSqsInlet` | secret `sqs/<source id>` (sealed, write-only) |
| A source's LLM policy: mode (`off` (default), `all`, `kinds`, `on_demand`), kinds, concurrency (1 to 64, default 2), daily token budget (tokens per UTC day, unset: no limit) | `SetLlmPolicy`, `AddSource` | `sources.policy_json` |
| AI providers: preset, model, base URL | `SetAiProvider` | setting `ai.provider.<name>` |
| An AI provider's API key | `SetAiProvider` | secret `ai/<name>/api_key` (sealed, write-only) |
| The default AI provider (the LLM queue's) | `SetAiProvider` (`make_default`; the first one set) | setting `ai.default_provider` |
| Ingest: least MinHash Jaccard estimate for a `similar_to` edge (above 0, at most 1, default `0.8`). Read for every file, so a change applies to the next file ingested. Edges already written are kept; `ReprocessJob` with `force` compares a source's documents again (a lower value adds edges, a higher one removes none). | `SetIngestSettings`, read with `GetIngestSettings` | setting `ingest.similar_min_jaccard` |
| Ingest: `Summarize` calls that read and analyze a file at once (1 to 16, default `2`; OCR of a scanned page can take 0.6 to 1 GB). Read for every call, so a change applies to the next one. A call beyond it gets `RESOURCE_EXHAUSTED` at once. | `SetIngestSettings`, read with `GetIngestSettings` | setting `ingest.summarize_max_concurrent` |
| Enrichment: the registry switches for GLEIF, VIES, Wikidata, EDGAR, APR and Companies House (all default on; Companies House is asked only once its key is set) | `SetEnrichmentSettings`, read with `GetEnrichmentSettings` | settings `registry.<gleif\|vies\|wikidata\|edgar\|apr\|companies_house>.enabled` |
| Enrichment: the EDGAR contact for the SEC's `User-Agent` (printable ASCII, at most 200 characters; default none: the generic `User-Agent`) | `SetEnrichmentSettings` | setting `registry.edgar.contact` |
| Enrichment: the Companies House API key | `SetEnrichmentSettings` | secret `registry/companies_house/api_key` (sealed, write-only) |
| Enrichment: the registry delta feed switches: GLEIF's delta files, EDGAR's daily index and APR's monthly set (default on), Companies House streaming (default off; it needs a streaming key). A feed runs only while its registry is on. | `SetEnrichmentSettings` | settings `feed.<gleif\|edgar\|apr\|companies_house>.enabled` |
| Enrichment: the Companies House streaming key (its own key, not the REST key) | `SetEnrichmentSettings` | secret `registry/companies_house/stream_key` (sealed, write-only) |
| Enrichment: the OpenCorporates API key (no client uses it yet) | `SetEnrichmentSettings` | secret `registry/opencorporates/api_key` (sealed, write-only) |
| Enrichment: extra root certificates for APR's download (PEM, at most 64 KiB; default none) | `SetEnrichmentSettings` | secret `registry/apr/extra_roots_pem` (sealed, write-only) |
| Company icons: ask the public fallback service (default off). Read for every icon lookup. | `SetEnrichmentSettings` (see [favicons.md](favicons.md#settings)) | setting `favicon.fallback` |
| Company icons: the egress proxy (`http://` or `https://`, no credentials; default none). Read for every icon lookup. | `SetEnrichmentSettings` | setting `favicon.egress_proxy` |

The LLM queue's state is in the store too: `llm_jobs` (queued documents) and `llm_usage` (each
source's spend per UTC day). The provider variables quail and hedgehog read (`AI_PROVIDER`,
`OPENAI_API_KEY`, `AI_MAX_CONCURRENT_LLM`, ...) are **not** read by doewood: providers come only
from the store. See [llm.md](llm.md).

The enrichment job reads its settings on every pass, so a change applies to the next pass;
see [registries.md](registries.md#configuring-the-registries).

## Bootstrap variables

| variable | default | meaning |
|---|---|---|
| `DOEWOOD_DATA_DIR` | `./doewood-data` | Directory of the store (`doewood.sqlite`) and, in development, the key file. Created if missing. |
| `DOEWOOD_LISTEN` | `127.0.0.1:7443` | `IP:port` of the gRPC API. |
| `DOEWOOD_OPS_LISTEN` | `127.0.0.1:7444` | `IP:port` of the ops endpoints: `/healthz`, `/readyz`, `/metrics`. |
| `DOEWOOD_POSTGRES_URL` | unset: standalone | `postgres://` or `postgresql://` URL of the shared Postgres + pgvector database. |
| `DOEWOOD_MASTER_KEY` | the key file `<data>/master.key` | Base64 of 32 random bytes. Encrypts the secrets in the store. |
| `DOEWOOD_INSTANCE_ID` | the hostname | Name of this instance: 1 to 63 ASCII letters, digits, `-`, `_`, `.`. On Kubernetes, the `StatefulSet` pod name. It is the owner named on the sources this instance claims. |
| `DOEWOOD_API_MAX_MESSAGE_MIB` | `64` | Largest gRPC message the API decodes or encodes, in MiB (1 to 1024). Leaves room for a file sent to `Summarize`. |
| `DOEWOOD_INGEST_JOBS` | from the CPUs and memory | Files ingest analyzes at the same time (1 to 256). The default is one per CPU, at most what half the available memory holds at 512 MiB each (a cgroup limit counts), at most 32. See [ingest.md](ingest.md). |
| `DOEWOOD_OCR_MODELS` | `<data>/ocr-models` | Directory of quail's OCR model files (`quail models fetch` downloads them). Missing models are a `WARN` at start, never an error: scanned pages are then flagged `needs_ocr`. |

`RUST_LOG` sets the log filter (default `info`), for example `RUST_LOG=doewood=debug,info`.

### Why the ingest variables stay bootstrap values

Tuning values that change what ingest writes are runtime settings. Two ingest variables are
not, because the API cannot set them:

- `DOEWOOD_INGEST_JOBS` sizes the process: the worker pool and the engine's threads are built
  once at start, before the API is up, and must fit the CPU and memory limits the deployment
  gives the container. It belongs with those limits, in the deployment.
- `DOEWOOD_OCR_MODELS` is part of the image layout: a path to model files that the image (or a
  volume mounted into it) provides. It is fixed by how the container is built, not by an
  operator at run time, and the analyzer checks the models once at start.

`DOEWOOD_SIMILAR_MIN_JACCARD`, read by earlier versions, is the runtime setting
`similar_min_jaccard` now. If it is still set, doewood ignores it and logs a `WARN` at start
(also shown by `doewood config check`) saying to use `ConfigService.SetIngestSettings`.

### Rules

- An empty value counts as unset.
- An invalid value is never fatal: doewood logs a `WARN` naming the variable, uses the default,
  and keeps going. Values that may hold a secret (`DOEWOOD_POSTGRES_URL`,
  `DOEWOOD_MASTER_KEY`) are never echoed, not even in the warning.
- An invalid instance id falls back to the hostname; a hostname that is not a valid id falls
  back to `doewood`.
- **Fatal at start:** a data directory that cannot be created or written, a key file that exists
  but does not hold a valid key (it is never overwritten, since that would lose every secret), a
  master key that differs from the one the store was created with, and an ops or gRPC address
  that cannot be bound.

### The shared Postgres

With `DOEWOOD_POSTGRES_URL` set, records, the graph, source claims and search live in that
database (PostgreSQL 15 or newer); without it, in the instance's SQLite file.

- **Never fatal.** If the database cannot be reached, the instance still starts, runs degraded
  and retries with backoff (1 s doubling to 60 s, with jitter). `/readyz` answers 503 with the
  reason, for example `not ready: shared Postgres unreachable: ... (retrying in 4s)`. A connected
  store is pinged every 15 s; while it does not answer, `/readyz` is 503 again.
- **Migrations** run on connect, together with the pgvector setup, in one transaction under a
  Postgres advisory lock (key `28269979257237348`, "doewood" in ASCII; a transaction-level
  lock, so it works through a transaction-mode pooler and never outlives a failed start).
  Instances starting together against an empty database are safe: one migrates, the others
  log `another instance is setting up the shared schema; waiting for it`, wait for it (at most
  5 minutes, then they retry with backoff) and find the schema migrated. The version is in
  the `doewood_schema` table. A database migrated by a newer doewood is
  refused (not ready) until this one is upgraded.
- **Schema 6** (the registry delta feeds of issue #49: the new table `registry_feeds`, one
  row per feed with its lease, cursor and next run, and an index on company aliases in byte
  order) is the current version. It only adds; as with every schema, older instances refuse
  it, so all instances are upgraded together (below). The instance's own SQLite file goes to
  schema 9 with the same table. **Schema 5** (the enrichment proposals of issue #52: the new
  table `company_proposals`, and the `merge_proposals` and `registry_proposals` lists of
  company props moved into it and removed from the props) was the one before. Its move is part of the migration: it
  runs once, in the same transaction that records schema 5, so it is never repeated or half done.
  A malformed entry is left out; a pair kept as `dismissed` becomes a rejected, dismissed
  proposal. Schema 4 added the merge log and dismissals of ADR 0002 section 1.8
  (`company_merges` and `company_dismissals`), schema 3 the enrichment storage (the registry
  and favicon caches, company freshness, and the prefix index on `locations`). An instance
  from before a schema refuses a database migrated to it, so **upgrade every instance that
  shares the database together**: once one new instance has started, the older ones stay not
  ready until they are upgraded. With schema 5 the instance's own SQLite file went to schema 8
  (the same table and the same move).
- **Cache purge.** Every hour (first a minute after the start), each instance deletes the
  expired registry answers and icons. Every instance does it; the delete is idempotent.
- **pgvector** is used when the server has it: doewood creates the extension (this needs a role
  allowed to) and adds `search_index.embedding`. Without it, this is logged once and search runs
  on full text only.
- **TLS** follows the URL's `sslmode` (`disable`, `prefer` (the default) or `require`), via
  rustls. The server certificate is always verified, against the system roots plus the Mozilla
  roots; put a private CA into the system store.
- Unset parameters get defaults: `connect_timeout=5`, `application_name=doewood`.
- **Source claims at start.** No scanner starts before the database answers. Then the instance
  claims each source of its SQLite store for `DOEWOOD_INSTANCE_ID` (a source added while
  standalone gets its claim now; one it already owns keeps its claim and its first time) and
  scans those. A source **another instance** owns is not taken over and does not fail the
  start: it is not scanned, an active source turns `error` with the reason
  `claimed by instance "doewood-1" since 2026-11-24T10:00:00Z; this instance does not scan it`
  (a paused one stays paused), and a WARN `source claimed by another instance` names the owner.
  The log line `source claims checked in the shared store` gives the counts. See
  [sources](sources.md#claims).

### The master key

In production, set `DOEWOOD_MASTER_KEY` from a secret store (a Kubernetes `Secret`):

```sh
openssl rand -base64 32
```

When it is unset (or invalid), doewood uses `<data>/master.key` and generates it on the first
start, with mode 0600 on Unix. This is for development only, and doewood says so with a loud
`WARN` on every start: anyone who can read the data directory can decrypt the secrets, and losing
the file loses them all.

The store keeps a sealed check value, so starting with a different key fails at once, instead
of running with secrets it cannot decrypt.

## Checking the configuration

```sh
doewood config check
```

prints the effective value and origin (`env`, `default`, or `default (env value invalid)`) of
every variable, with secrets redacted, and every warning. It creates nothing.

## Ops endpoints

| path | answers |
|---|---|
| `/healthz` | `200 ok` while the process runs |
| `/readyz` | `200 ready` once the store is open; `503` before that and during shutdown |
| `/metrics` | Prometheus text: `doewood_build_info`, `doewood_ready`, `doewood_start_time_seconds`, `doewood_ops_requests_total` |
| `POST /events/s3/{source_id}` | MinIO / S3 event notifications for a source (see [api.md](api.md#event-webhook)) |

On SIGTERM or SIGINT, doewood reports not ready, stops its servers (the gRPC API gets up to 10 s
to finish the calls in flight; its progress streams end), stops the scanners, checkpoints and
closes the store, and exits 0.
