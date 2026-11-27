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
| Ingest: the largest LSH band bucket the `similar_to` candidate query reads (10 to 1000000, default `1000`): a band key shared by more documents is skipped as template boilerplate, which changes which edges exist on template-heavy corpora (see [ingest.md](ingest.md)). Read for every file. | `SetIngestSettings`, read with `GetIngestSettings` | setting `ingest.similar_max_bucket` |
| Ingest: `Summarize` calls that read and analyze a file at once (1 to 16, default `2`; OCR of a scanned page can take 0.6 to 1 GB). Read for every call, so a change applies to the next one. A call beyond it gets `RESOURCE_EXHAUSTED` at once. | `SetIngestSettings`, read with `GetIngestSettings` | setting `ingest.summarize_max_concurrent` |
| API: calls in flight before new ones are shed with `UNAVAILABLE` (1 to 65536, default `256`). Applies to the next call. See [api.md](api.md#limits). | `SetApiSettings`, read with `GetApiSettings` | setting `api.max_calls_in_flight` |
| API: the server's deadline for a call, in seconds (1 to 3600, default `60`; `Summarize` and open streams are not cut by it). Applies to the next call. | `SetApiSettings` | setting `api.deadline_seconds` |
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
| `DOEWOOD_INGEST_BUDGET_MIB` | from the workers and memory | Ingest's memory budget, in MiB (16 to 1048576): the listed sizes of the files over 16 MiB held at once, from their fetch to the end of their analysis. The default is half the workers' plan (workers x 512 MiB, at most half the available memory), at least 256 MiB: 1 GiB with 4 workers, 768 MiB under a 4 GiB limit with 3. A bigger file waits; one bigger than the budget runs with no other big file. See [ingest.md](ingest.md#memory). |
| `DOEWOOD_INGEST_MAX_CELLS` | from the workers and memory | Most cells (and shared strings) read from one spreadsheet (10000 to 100000000); a bigger workbook fails. The default is what each worker's share of half the available memory holds at 320 bytes a cell, rounded down to 100,000, from 1,600,000 to 10,000,000 (hedgehog's own default): 1,600,000 for a 512 MiB worker, 6,700,000 for a 2 GiB one. Above 10,000,000 only when set here. See [ingest.md](ingest.md#memory). |
| `DOEWOOD_OCR_MODELS` | `<data>/ocr-models` | Directory of quail's OCR model files (`quail models fetch` downloads them). Missing models are a `WARN` at start, never an error: scanned pages are then flagged `needs_ocr`. |
| `DOEWOOD_OCR_JOBS` | from the memory | Scanned pages OCR'd at the same time (1 to 64, at most the ingest workers), each by its own engine with `CPUs / jobs` threads (at most 4). The default is quail's plan: one engine per GiB of 20 % of the total memory (a cgroup limit counts), so one below 10 GiB. More jobs read more pages a second on the same cores and need about 0.5 GiB more each; the text is the same. See [deploy.md](deploy.md#ocr-per-page). |

`RUST_LOG` sets the log filter (default `info`), for example `RUST_LOG=doewood=debug,info`.

### Why the ingest variables stay bootstrap values

Tuning values that change what ingest writes are runtime settings. Five ingest variables
are not, because the API cannot set them:

- `DOEWOOD_INGEST_JOBS` sizes the process: the worker pool and the engine's threads are built
  once at start, before the API is up, and must fit the CPU and memory limits the deployment
  gives the container. It belongs with those limits, in the deployment.
- `DOEWOOD_INGEST_BUDGET_MIB` sizes the process the same way: it is the share of the
  container's memory limit that files in flight may take, set once at start next to the
  workers. It changes when ingest runs a file, never what it writes.
- `DOEWOOD_INGEST_MAX_CELLS` sizes the process too: it is how much of a worker's share of the
  memory limit one workbook may take, so it belongs with that limit and the workers in the
  deployment, and is read once at start like them. Unlike the budget it can change an
  outcome (a workbook over it fails instead of being read), which is why its default follows
  the memory the deployment gives each worker rather than being fixed.
- `DOEWOOD_OCR_JOBS` sizes the process like the workers: each job is an OCR engine that holds
  about 0.5 GiB from its first page on, and together they must fit the container's memory
  limit. It changes how fast scans are read, never what is read.
- `DOEWOOD_OCR_MODELS` is part of the image layout: a path to model files that the image (or a
  volume mounted into it) provides. It is fixed by how the container is built, not by an
  operator at run time, and the analyzer checks the models once at start.


`DOEWOOD_SIMILAR_MIN_JACCARD`, read by earlier versions, is the runtime setting
`similar_min_jaccard` now. If it is still set, doewood ignores it and logs a `WARN` at start
(also shown by `doewood config check`) saying to use `ConfigService.SetIngestSettings`.

### API limits

The two per-call limits of the API (calls in flight, the server deadline) are runtime
settings, above. The connection limits are neither settings nor variables but fixed values,
because hyper and the listener take them once, when the server starts, before the API could
set anything, and a new bootstrap variable would extend ADR 0001's table (issue #21):

| limit | value |
|---|---|
| HTTP/2 streams per connection | 256 |
| HTTP/2 keepalive | PING after 30 s without traffic, closed after 20 s without an answer |
| TCP keepalive | probes after 60 s idle (the kernel's interval and count) |
| Connection age | GOAWAY at 30 min ± 10% (drawn per connection), closed 5 min later |

They are `doewood_api::MAX_CONCURRENT_STREAMS` and the constants next to it. See
[api.md](api.md#limits) for what clients see, and [deploy.md](deploy.md#load-balancing) for
why connections are aged.

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
- **Schema 11** (issue #141: the triggers behind `sketch_band_sizes` keep a row only for a
  band bucket of two or more documents, which cut the WAL of ingest by 12 to 29%; new
  functions only, no table or data changed, migrates in seconds, see
  [deploy.md](deploy.md#postgres-wal-on-a-big-corpus-issue-141)) is the current version. As
  with every schema, **all instances are upgraded together**.
- **Schema 10** (issue #112: `edges_by_dst` is rebuilt as `(dst, kind, coalesce(valid_from,
  '-infinity'), id)`, the order `Neighbours` and `CompanyDocuments` page in; see
  [api.md](api.md#pages-of-neighbours-and-companydocuments)) was the one before. The
  migration drops and builds one index on `edges`; no data changes. On the issue #127
  `scale` store at 200,000 documents (997,834 edges, `edges` 223 MB) the build took 0.8 to
  1.0 s. For that long the migration's transaction holds `edges` exclusively: reads and
  writes of edges by any instance wait, other starting instances wait on the migration lock,
  and the migrating instance is not ready until it commits. Short enough that the index is
  not built `CONCURRENTLY` (which cannot run in the migration's transaction). **Down path:** `DROP INDEX edges_by_dst;
  CREATE INDEX edges_by_dst ON edges (dst, kind); DELETE FROM doewood_schema WHERE version
  = 10;`. The instance's own SQLite file went to schema 12 with the same index (`ifnull(valid_from,
  -2147483648)`). On the same store in SQLite (a 5.0 GiB file) the index takes about 8 s. The
  migrations run when the store opens at start, before the instance is ready; nothing else
  uses the file meanwhile. Before issue #159 the runner also checked every foreign key in the
  file before each migration committed, 70 to 375 s each (mostly disk reads; it depends on how
  much of the file the OS has cached): from schema 10 to 12 the instance was ready 6.3
  minutes after the start (another run was still migrating when the 10-minute limit stopped it). Since #159 each
  migration checks only what it can break (see [The instance store](#the-instance-store)), and
  schema 10 to 12 takes 3 to 26 s on that store. `migration_timings` in
  `crates/doewood-store/src/migrate.rs` (ignored) times them on a copy of a store. Schema 9 (issue #148: the `similar_to` stop band, `sketch_band_sizes`) came
  before it.
- **Schema 8** (issue #121: records are stored as their canonical JSON text, in the new
  column `records.body`, lz4-compressed where the server has lz4) came before that. The
  migration only adds the column, makes `records.record` (jsonb) nullable and adds a small
  partial index, so it is quick on a big table. Rows written before keep their jsonb until a
  started instance converts them in the background, in batches of 100 rows, each its own short
  transaction (resumable: a stop loses nothing and the next start goes on; instances starting
  together share the work); reads return the same bytes from either form meanwhile. The log
  shows `converting stored records to text` and then `stored records converted to text`. As
  with every schema, older instances refuse it and cannot read the converted rows, so **all
  instances are upgraded together** (below). The instance's own SQLite file is unchanged. See
  [deploy.md](deploy.md#records-as-text-on-postgres-issue-121) and
  [ADR 0003](adr/0003-records-as-canonical-text.md). **Down path** (by hand, with every
  instance stopped): `UPDATE records SET record = body::jsonb WHERE record IS NULL` (in
  batches of a few thousand rows by `blake3` on a big table), then
  `DROP INDEX records_body_pending; ALTER TABLE records DROP COLUMN body, ALTER COLUMN record
  SET NOT NULL; DELETE FROM doewood_schema WHERE version = 8;`, then start the older binary.
  An older binary that meets schema 8 refuses it and stays not ready. If the down path is
  skipped and the schema row is deleted anyway, the older binary fails to read every converted
  row, because its `record` is NULL. **Schema 7** (issue #102: the
  search index `search_tsv` gets `gin_pending_list_limit = 256` kB, and its pending list is
  merged once; no table or data changed, see
  [deploy.md](deploy.md#search-and-the-gin-pending-list-issue-102)) was the one before.
  **Schema 6** (the registry
  delta feeds of issue #49: the new table `registry_feeds`, one row per feed with its lease,
  cursor and next run, and an index on company aliases in byte order) came before that. The
  instance's own SQLite file went to schema 9 with the same table. **Schema 5** (the enrichment proposals of issue #52: the new
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
- **Connection pool (issue #108).** Each instance opens at most **4 connections per CPU,
  between 16 and 64** (16 up to 4 CPUs, 32 on 8, 64 from 16), counting the CPUs the process
  may use (a container's CPU limit included). Fixed, not a setting; the log line
  `shared Postgres store ready` shows it (`pool`). Plan the server's `max_connections` for
  every instance at once: the sum of the instances' pools (for example 3 replicas on 4 CPUs:
  48), plus a rolling deploy's surge replicas, plus your own tools and superuser slots
  (Postgres's default `max_connections` is 100). Behind PgBouncer the pools count against
  its client limit instead. See [deploy.md](deploy.md#api-read-capacity-issue-108).
- **Prepared statements.** Each pooled connection prepares a fixed
  statement once and reuses it for the connection's life, so a statement is one round trip
  (paged list queries and search are still planned per call). Behind PgBouncer in
  transaction mode this needs PgBouncer 1.21 or newer with `max_prepared_statements` set (for
  example 200); older PgBouncer, or transaction mode without that setting, breaks named
  prepared statements. Session mode and a direct connection need nothing.
- **Cache purge.** Every hour (first a minute after the start), each instance deletes the
  expired registry answers and icons. Every instance does it; the delete is idempotent.

### The instance store

**Migrations** run in one transaction each when the file opens, with foreign keys off (so a
migration can rebuild a table others reference). Before a migration commits it checks the
foreign keys it could break, as the runner declares per migration (issue #159): every table
for one that rebuilds a table, deletes rows or moves data (schemas 1 to 10); only the tables
it fills for one that writes into new tables (schema 11); none for one that writes no
rows and rebuilds nothing (schema 12, an index; schema 13, a column with a default and a new
table). A violation rolls the migration back and the instance does not start.
A test holds each migration to its declaration.

`doewood.sqlite` runs in WAL mode with `synchronous = NORMAL`, and a `journal_size_limit` of
64 MiB: after a large transaction the `-wal` file shrinks back to that at the next checkpoint.
Its writer connection keeps up to 128 prepared statements and samples at most 400 rows an
index in `ANALYZE` (`analysis_limit`; same plans as full statistics, issue #118). Right after
the start (once the instance is ready, not before) it refreshes the planner statistics of every
table that needs it, as `PRAGMA optimize = 0x10002` would, one table at a time: each table's
pages are read first on a connection of its own, then the writer is held for that table's
`ANALYZE` only (at most about 70 ms at 50,000 documents, even with the file not in the page
cache). The hourly cache purge task runs `PRAGMA optimize`, so the planner statistics follow
tables that grow from empty to millions of rows. On a store closed cleanly (the close
optimizes too) there is nothing to do. Schema 10 (issues
#72 and #73) keeps per-source status counters of `objects_seen` and `llm_jobs`
(`object_counts`, `llm_job_counts`, updated by triggers in the same transaction), adds an
index for a source's newest failed summary, and drops `objects_seen_by_status`, which held the
same entries as `objects_seen_pending`. Schema 13 (issue #131) adds the per-source
`skip_known_copies` option and the `known_content` table (MD5 ETag and size to BLAKE3, one row
per distinct content, written with each object's final status); see
[sources.md](sources.md#known-copies).

**Read-only connections (issue #83).** Every write goes through the one writer connection,
behind a mutex: there is exactly one writer (ADR 0001). Next to it, an on-disk store opens
**one read-only connection per CPU, between 1 and 8** (issue #108; counting the CPUs the process
may use, a container's CPU limit included; fixed, not a setting; the `store open` log line shows
it as `readers`), opened with
`SQLITE_OPEN_READ_ONLY` and `query_only = ON`, with the writer's 128-statement cache and 5 s
busy timeout. In WAL mode they read next to the writer, so these reads no longer wait for
ingest's write transactions:

- standalone: records (`GetRecord`, `ListRecords`, versions, facts), the graph (nodes, aliases,
  neighbours, a company's documents), search, similar documents (sketch candidates), source
  claims, proposals, the merge log and dismissals, the registry and icon caches, enrichment
  state and due companies, feed state and company aliases;
- the instance store: sources, the `WatchProgress` snapshot (every source's counters,
  checkpoint and LLM spend, read in one transaction), the objects discovery has seen and
  their ingest status (each ingest attempt reads the status, the source and the settings in
  one transaction, issue #128), runtime settings and AI providers,
  whether a secret is set, and opening a secret for a connector (decrypted with the same
  master key; the key check at open runs on the writer before the readers open).

Each read call is one transaction on one connection: it sees what was committed when it
started, never a write still in progress. Every write commits before it returns, so a read made
after a write returned sees it (a call that writes and then reads, such as `AddSource`, still
gets what it wrote). Writes, and the LLM queue's reads that decide its next write, stay on
the writer. When every reader
is busy a read waits for one, never for the writer. An in-memory store (tests) has no
readers; its reads use the writer, as before.

**The WAL is kept bounded (issue #151).** SQLite's own checkpoints (after a commit that leaves
1,000 pages in the WAL) are passive: they copy only the frames no read still needs, and the
WAL starts over only once every frame is copied and no read is using it. With the readers busy
without a pause, that moment may not come, and the WAL then grows without bound (checkpoint
starvation); a large WAL makes every read slower (7 times at 5.4 GB, issue #140). So an on-disk
store has one more connection, for checkpoints only (it writes no rows; the one writer stays
the only writer), and a thread `doewood-wal` that every 5 s:

1. runs `PRAGMA wal_checkpoint(PASSIVE)`: copies what it can, without waiting and without the
   writer;
2. when the `-wal` file is over 64 MiB, or nothing was written since the last pass and the WAL
   is not empty, runs `PRAGMA wal_checkpoint(TRUNCATE)`: it waits for the reads on an older
   snapshot to end (each read call is one short transaction; no read keeps one open across an
   `await`), copies what is left and empties the file. It waits 50 ms at most in total for
   SQLite's locks, then gives up until the next pass, so a write waits for it about that long
   plus the copy of what the passive step left.

Writes that return their row (`INSERT ... RETURNING`, such as a node's or an edge's upsert) are
stepped to their end: SQLite runs its automatic checkpoint only when a statement is done, and
reading the one row and resetting the statement skipped it, so a long run of single node or
edge upserts never checkpointed (this, not the readers, left the 5.4 GB). An emptied WAL is
logged at INFO when it was over 64 MiB (DEBUG when idle), a truncate that gave
up at INFO. The WAL's size and the truncations are metrics (`doewood_store_wal_bytes`,
[api.md](api.md#metrics)). The close still checkpoints and empties the WAL.

**The instance's reads go first (issue #109).** When reads wait for a reader, the instance's
own (ingest, enrichment, the scanners) get the next free one before the API's (its calls and
the `WatchProgress` snapshot), so a flood of API reads cannot slow ingest down by holding every reader. The API
is not starved: while none of its reads holds a reader, the instance's reads leave it the last
free one, so at least one API read is always going. Neither side waits for more than the reads
already running.
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
  The log line `source claims checked in the shared store` gives the counts. Until it, ingest
  holds its work; after it, the queued objects and webhook notifications of a source another
  instance owns are dropped, not ingested. An owner that is gone for good keeps its claims
  until `ReleaseSourceClaim` releases them ([deploy](deploy.md#a-dead-instance)). See
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
| `/metrics` | Prometheus text: `doewood_build_info`, `doewood_ready`, `doewood_start_time_seconds`, `doewood_ops_requests_total`, `doewood_ingest_claimed_elsewhere_dropped_total`, `doewood_ingest_finish_unmatched_total`, `doewood_ingest_known_copies_total`, `doewood_ingest_known_copy_bytes_total` |
| `POST /events/s3/{source_id}` | MinIO / S3 event notifications for a source (see [api.md](api.md#event-webhook)) |

On SIGTERM or SIGINT, doewood reports not ready, stops its servers (the gRPC API gets up to 10 s
to finish the calls in flight; its progress streams end), stops the scanners, checkpoints and
closes the store, and exits 0.
