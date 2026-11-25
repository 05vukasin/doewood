# Deploy

One image holds the binary, built with OCR, and quail's OCR models
([ADR 0001](adr/0001-architecture.md), section 1). On Kubernetes each instance is a
`StatefulSet` pod with its own volume.

## The image

```sh
docker build -t doewood:dev .
```

- Multi-stage: `rust:1.97.1-slim-trixie` (the version of `rust-toolchain.toml`; the build fails
  if they differ) builds `cargo build --release --locked -p doewood` (default features, so
  `ocr`). The runtime is `debian:trixie-slim` with CA certificates and `libstdc++6`.
- `--build-arg CARGO_BUILD_JOBS=N` (default 4) caps the parallel jobs. The release profile uses
  fat LTO: lower it if the build runs out of memory.
- OCR is quail's PP-OCRv6 on ONNX Runtime, not Tesseract. ONNX Runtime is linked statically (the
  `ort` build script downloads it). The 7 model files (about 45 MiB, Apache-2.0) are downloaded at
  build time from the URLs of `quail_ocr::models::MANIFEST`, each checked against its SHA-256
  (`ADD --checksum`), into `/opt/doewood/ocr-models`. When the quail revision changes, update them
  together.
- Runs as `doewood` (UID/GID 10001), not root. The root filesystem can be read-only.
- Volume `/data` holds the SQLite store (and, without `DOEWOOD_MASTER_KEY`, the development key
  file).

The image sets these [bootstrap variables](configuration.md#bootstrap-variables):

| variable | value in the image |
|---|---|
| `DOEWOOD_DATA_DIR` | `/data` |
| `DOEWOOD_LISTEN` | `0.0.0.0:7443` |
| `DOEWOOD_OPS_LISTEN` | `0.0.0.0:7444` |
| `DOEWOOD_OCR_MODELS` | `/opt/doewood/ocr-models` |

It also sets `GLIBC_TUNABLES=glibc.malloc.arena_max=2:glibc.malloc.mmap_threshold=131072`:
glibc then returns the memory of big files and OCR to the system when it is freed, instead of
keeping it in per-thread arenas (a third less memory after OCR, see [Allocator](#allocator)). A
binary run outside the image does not get it; set the variable yourself there.

```sh
docker run --rm -p 7443:7443 -p 7444:7444 -v doewood-data:/data \
  -e DOEWOOD_MASTER_KEY="$(openssl rand -base64 32)" doewood:dev
curl -s 127.0.0.1:7444/readyz        # ready
docker run --rm doewood:dev version
docker run --rm doewood:dev config check
```

Keep the master key: the store refuses to start with a different one.

CI (`image` job) builds the image, runs this smoke test, and lints and renders the chart. It
pushes nothing; publishing the image to a registry is not set up yet.

## Helm

The chart in `charts/doewood` runs a `StatefulSet` (pods start in parallel; each gets its own
`PersistentVolumeClaim` from `volumeClaimTemplates`), a headless Service naming each pod, and a
Service for the gRPC (7443) and ops (7444) ports. The liveness probe is `/healthz`, the readiness
probe `/readyz`. `DOEWOOD_INSTANCE_ID` is the pod name. Parallel starts against a new, empty
database are safe: one pod migrates the shared schema (and creates pgvector), the others
wait for it and stay not ready until then (see the Migrations bullet in
[configuration.md](configuration.md#the-shared-postgres)). Behind PgBouncer in transaction
mode, use PgBouncer 1.21 or newer with `max_prepared_statements` set: doewood keeps prepared
statements per connection (see the Prepared statements bullet there).

Secrets come only from existing `Secret`s, never from values:

```sh
kubectl create secret generic doewood \
  --from-literal=master-key="$(openssl rand -base64 32)" \
  --from-literal=postgres-url='postgres://doewood:...@postgres:5432/doewood?sslmode=require'

helm install doewood charts/doewood \
  --set image.repository=registry.example.com/doewood --set image.tag=0.1.0 \
  --set masterKey.existingSecret=doewood \
  --set postgres.existingSecret=doewood --set postgres.key=postgres-url
```

Instances are independent: each has its own settings and sources, set through the API. Each
source is claimed in the shared Postgres by one instance; a restarted pod re-claims its sources
under the same name, and one another instance owns is reported (state `error`, the owner in the
reason) instead of scanned (see [sources](sources.md#claims)). Talk to
one instance through the headless Service, for example
`doewood-0.doewood-headless.<namespace>.svc:7443`; the regular Service spreads calls over all of
them, which suits only reads of the shared store (see [Load balancing](#load-balancing)).

| value | default | meaning |
|---|---|---|
| `replicas` | `1` | instances |
| `image.repository`, `image.tag`, `image.pullPolicy` | `doewood`, the chart's `appVersion`, `IfNotPresent` | the image |
| `imagePullSecrets` | `[]` | pull secrets |
| `postgres.existingSecret`, `postgres.key` | `""`, `url` | Secret and key of `DOEWOOD_POSTGRES_URL`; unset: standalone |
| `masterKey.existingSecret`, `masterKey.key` | `""`, `master-key` | Secret and key of `DOEWOOD_MASTER_KEY`; unset: a development key file per volume |
| `extraEnv` | `[]` | more variables, for example `RUST_LOG`, `DOEWOOD_INGEST_JOBS` |
| `service.type`, `service.grpcPort`, `service.opsPort` | `ClusterIP`, `7443`, `7444` | the Service |
| `persistence.size`, `persistence.storageClass`, `persistence.accessModes` | `1Gi`, default class, `[ReadWriteOnce]` | each pod's volume |
| `resources` | requests 500m CPU and 1 GiB, limit 4 GiB | measured: about 1.1 GiB at most with OCR, 0.4 GiB for a 120k-row XLSX (see [Sizing](#sizing)); the ingest jobs follow the memory limit |
| `podSecurityContext`, `securityContext` | non-root 10001, `fsGroup` 10001, seccomp `RuntimeDefault`, read-only root, no privilege escalation, all capabilities dropped | security |
| `terminationGracePeriodSeconds` | `30` | shutdown time |
| `podAnnotations`, `podLabels`, `nodeSelector`, `tolerations`, `affinity` | empty | scheduling and metadata |

`/metrics` is on the ops port; add scrape annotations through `podAnnotations` if your
Prometheus uses them. See [Metrics and alerts](#metrics-and-alerts).

## Load balancing

A Kubernetes `ClusterIP` Service balances **TCP connections**, not calls. A gRPC client keeps
one HTTP/2 connection and sends every call over it, so without help all of a client's calls
stay on the pod it first reached, and replicas added later get no existing clients. doewood
therefore ages its connections: at 30 minutes (each connection draws its own age within
±10%, so clients that connected together, for example after a deploy, do not all reconnect at
the same moment) it sends GOAWAY, the client opens a new
connection for new calls (which the Service may send to another pod), and the old one is
closed 5 minutes later, open streams included (see [api.md](api.md#limits)). Load spreads
again within about half an hour after a scale-up or a restart.

Which Service to use:

- **Calls that need one instance** (its sources, settings, `WatchProgress`,
  `ReprocessJob`): the headless Service, by pod name,
  `doewood-0.doewood-headless.<namespace>.svc:7443`. Each instance has its own settings and
  sources; never send these through the regular Service.
- **Reads of the shared store** (records, graph, search; Postgres mode, several replicas):
  either the regular Service, and rely on the connection age to spread clients, or
  client-side balancing for an even spread per call: resolve the headless Service
  (`dns:///doewood-headless.<namespace>.svc:7443`) and use the `round_robin` policy (in
  grpc-go `grpc.WithDefaultServiceConfig({"loadBalancingConfig":[{"round_robin":{}}]})`).
  The headless Service publishes pods that are not ready too (so each pod has a name during
  start-up), so a client balancing over it should retry `UNAVAILABLE` on another pod. A
  service mesh or an L7 proxy that understands HTTP/2 (Envoy, Linkerd, an ingress with gRPC
  support) balances per call as well.

Clients:

- Set a deadline on every unary call, and retry `UNAVAILABLE` with exponential backoff and
  jitter (for example 100 ms doubling to 10 s, a few attempts). A call shed because the
  instance is overloaded did not run, so retrying it is safe; with client-side balancing the
  retry may reach another replica.
- Watch again when a stream ends with an error; the first message is a full snapshot.
- HTTP/2 keepalive: the server PINGs idle connections every 30 s and accepts PINGs from
  clients; a client that wants its own keepalive should not PING more often than every 30 s.

## Metrics and alerts

`/metrics` on the ops port lists the gRPC metrics (calls by method and code, duration, calls
in flight, shed calls, connections; see [api.md](api.md#metrics)) next to `doewood_ready`.
Alerts worth setting (tune the thresholds to your traffic):

- **Shedding:** `rate(doewood_grpc_shed_total[5m]) > 0` for 10 minutes. The instance refuses
  calls because `max_calls_in_flight` is reached: add replicas, or raise the cap if CPU and
  memory have room.
- **Errors:** the share of `INTERNAL`, `UNKNOWN` and `DATA_LOSS` among
  `doewood_grpc_calls_total`, for example above 1% over 10 minutes. `UNAVAILABLE` also rises
  while the shared Postgres is not connected (see `doewood_ready`).
- **Latency:** p99 of unary calls,
  `histogram_quantile(0.99, sum by (le, method) (rate(doewood_grpc_call_duration_seconds_bucket{method!~".*/(Watch.*|ServerReflectionInfo)"}[5m])))`,
  above a few seconds, or near `deadline_seconds`. Leave the streams (`WatchProgress`,
  `WatchReprocess`, health `Watch`, reflection) out: their duration is their lifetime.
- **Load:** `sum(doewood_grpc_calls_in_flight)` near `max_calls_in_flight` (minus the open
  streams, which do not count against it), and `doewood_grpc_connections_open` per pod, to
  see whether the clients spread over the replicas.
- **Not ready:** `doewood_ready == 0` for more than a few minutes.

## Sizing

Measured for issue #63 on generated, invented data: the image built from `main` at `bf47753`
(quail `62108d4`), Postgres 17.12 with pgvector 0.8.7 (`pgvector/pgvector:pg17`) and MinIO
(`bitnamilegacy/minio:2025.7.23`), each in its own container on one private Docker network.
Machine: AMD EPYC-Genoa, 4 vCPU, 7.6 GiB, Linux 7.0.0-30, Docker 29.9, no container limits
unless stated. Every run starts with a new Postgres and a new doewood volume, adds one source
with `doewood dev add-source` and runs `serve` until every object has its location in Postgres.
Memory is the container's cgroup v2 `memory.peak` (anon + page cache) and the highest `anon`
sampled every 0.5 s. Times are from the start of `serve`. Medians of 2 or 3 runs; single runs
are marked.

The scans are pages of invented invoice text rendered with DejaVu Sans at 300 DPI (US Letter,
2550 x 3300, 8-bit gray, Flate) and wrapped as image-only PDFs, about 300 KiB a page. OCR read
every page (`ocr.page` facts on every page, about 1500 characters a page, mean confidence 0.99).

### Ingest

| case | `DOEWOOD_INGEST_JOBS` | time | throughput | doewood `memory.peak` (anon) | Postgres peak |
|---|---|---|---|---|---|
| 200 scanned PDFs, 1 page each | 1 | 474 s | 0.42 pages/s, 2.4 s a page | 932 MiB (796) | 139 MiB |
| same | 2 (2 runs) | 468 s | 0.43 pages/s | 906 MiB (825) | 134 MiB |
| same | default (4 here) | 472 s | 0.42 pages/s | 986 MiB (919) | 135 MiB |
| 20 scanned PDFs, 20 pages each (400 pages) | 1 (2 runs) | 918 s | 0.44 pages/s | 888 MiB (807) | 136 MiB |
| same | 2 (2 runs) | 925 s | 0.43 pages/s | 936 MiB (885) | 131 MiB |
| same | default and 4 (3 runs) | 608 s | 0.66 pages/s | 1094 MiB (984) | 131 MiB |
| same, container limit 4 GiB (as the chart) | default (3 under this limit; 1 run) | 814 s | 0.49 pages/s | 1120 MiB (1026) | 140 MiB |
| text PDF, 150 pages (490 KiB, 344k characters) | default | 0.7 s | | 37 MiB (35) | 91 MiB |
| XLSX, 120,000 rows x 8 columns (7.6 MiB, 960k cells) | default | 7.5 s | | 380 MiB (297) | 82 MiB |
| DOCX, 20 MiB (1.3M characters, 7 embedded images) | default | 0.8 s | | 68 MiB (65) | 82 MiB |
| 50,000 small text documents | | not measured | | | |
| enrichment pass with mocked registries | | not measured | | | |

The earlier measurement without OCR (2000 small text PDF, DOCX, TXT and XLSX files): doewood
peaked at 159 MiB, Postgres at 208 MiB, MinIO at 369 MiB.

What the numbers say:

- **OCR is one page at a time here, whatever the jobs.** quail sizes the OCR engines from the
  memory: one engine per 1 GiB of 20 % of the total memory (and 40 % of the available), at least
  1, at most 4, each with `cpus / engines` threads (at most 4). On 7.6 GiB, and under any limit
  below 10 GiB, that is 1 engine with 4 threads (`ocr: Ready { jobs: 1, threads: 4 }` in the
  `ingest started` log line). It uses about 2.7 of the 4 cores (6.3 CPU seconds a page).
  `DOEWOOD_INGEST_JOBS` sets the file workers, not the engines.
- **Throughput:** about 0.42 pages/s (1500 pages an hour) on 4 vCPU. More workers did not help
  single-page scans; for 20-page scans 4 workers gave 0.66 pages/s (and a third less CPU time),
  3 workers (the default under a 4 GiB limit) 0.49, and 1 or 2 workers 0.43. This held in all
  three runs with 4 workers; the cause is not known yet.
- **Memory with OCR** is about 0.9 GiB with 1 worker and 1.0 to 1.1 GiB with 4, almost all
  anonymous memory of the OCR engine (ONNX Runtime), and it stays allocated after the queue is
  empty (750 to 950 MiB anon when the runs ended). Without OCR a worker needs tens of MiB for
  ordinary files; a large spreadsheet is the most expensive file seen (380 MiB for 960k cells).
- **Postgres** stayed below 160 MiB in every run (20 to 40 MiB anon, the rest page cache). A
  scanned single-page record adds about 40 KiB to the database (200 records: 9 MiB empty to
  16.5 MiB), a 20-page one about 100 KiB. Growth to 50,000 records was not measured.
- **MinIO** used 600 to 1300 MiB, most of it page cache; its anonymous memory was 250 to 490
  MiB.

### Big spreadsheets at once (issue #67)

`big_spreadsheets` in `crates/doewood-ingest/tests/pipeline.rs`: generated XLSX (stored, not
deflated) of 26 columns, numbers or every other column text, all put at once, 3 workers, the
normal file timeout, peak RSS of the test process sampled every 10 ms. In process, test
profile, the server above, single runs. "Peak" is above the RSS before the run (which holds
the test's own copies of the files).

| files | before (hedgehog's 10 M cells, no budget) | after (1.6 M cells, budget) |
|---|---|---|
| 1 x 2.6 M cells, numbers (80 MiB) | +605 MiB, `done` | +287 MiB, `failed` (over the cell cap) |
| 1 x 2.6 M cells, half text (120 MiB) | +736 MiB, `done` | +386 MiB, `failed` |
| 3 x 2.6 M cells, numbers (241 MiB) | +1701 MiB, `done` | +775 MiB, `failed` |
| 3 x 2.6 M cells, half text (360 MiB) | +2210 MiB, `done` | +1044 MiB, `failed` |
| 3 x 5.2 M cells, numbers (492 MiB) | +3004 MiB, `done` | +1204 MiB, `failed` |
| 3 x 1.56 M cells, numbers (143 MiB), budget 4 GiB / 768 MiB (the default for 3 workers) / 64 MiB | +989 / +976 / +551 MiB, all `done` | |
| 3 x 1.56 M cells, half text (214 MiB), budget 4 GiB / 64 MiB | +1294 / +729 MiB, all `done` (4.2 s / 7.8 s) | |

- The peak per cell was 200 to 300 bytes (file included) before the cap; that is
  `CELL_BYTES` (320, rounded up), and the cap was what 512 MiB holds at that rate (1.6 M).
  Since #94 the cap follows the memory per worker (at least 1.6 M, at most 10 M; see
  [ingest.md](ingest.md#memory)), so on the server these runs had (7 GB, 3 workers, up to
  about 1 GiB per worker) it would be around 3 M, depending on the memory free at start, and
  the 2.6 M-cell files would be read. A refused
  workbook still costs its file and the cells read up to the cap.
- Under the cap the default budget does not serialize these files (71 MiB each, 3 fit in
  768 MiB): the budget bounds the bytes of files in flight, the cell cap the workbook's
  working set. A small budget runs big files one at a time (64 MiB: 40 % lower peak, twice
  the wall time).
- glibc kept some of the freed memory (RSS after the runs: 0.5 to 1.2 GiB above the start). This was before the image set
  `GLIBC_TUNABLES` (see [Allocator](#allocator)).

### Allocator

Issues #68 and #77, generated data, the same machine and setup as above (one private Docker
network created with `--internal`, every registry, feed and the favicon fallback switched off in
the instance settings before `serve`). Single runs. "After" is the container's `anon` when the
last object had its location, then again after the idle time; it did not change between 60 s
and the end of the idle time with glibc (glibc does not give memory back on its own), so one
number is shown. jemalloc and mimalloc were loaded with `LD_PRELOAD` from Debian's
`libjemalloc2` and `libmimalloc3`, not linked in.

OCR burst: 20 scanned PDFs of one page, default workers (4), image from `f8c6e3a`, 2 min idle:

| allocator | time | CPU | `memory.peak` | `anon` peak | `anon` after (RSS) |
|---|---|---|---|---|---|
| glibc, defaults | 48.9 s | 129 s | 958 MiB | 858 MiB | 810 MiB (896) |
| `MALLOC_ARENA_MAX=2` | 48.4 s | 129 s | 805 MiB | 653 MiB | 597 MiB |
| `mmap_threshold=131072` | 47.1 s | 124 s | 621 MiB | 587 MiB | 530 MiB |
| both (the image now) | 49.7 s | 129 s | 663 MiB | 584 MiB | 527 MiB (613) |
| jemalloc | 49.7 s | 130 s | 784 MiB | 643 MiB | 573, 547 after 60 s |
| mimalloc | 44.3 s | 118 s | 783 MiB | 730 MiB | 730, 602 after 60 s |

Big files: 200 small documents (PDF, DOCX, TXT, XLSX) plus a 120k-row XLSX, a 190k-row XLSX
(1.52 M cells), a 150-page text PDF and a 22 MiB DOCX, default workers (4), 3 min idle:

| image | allocator | time | `memory.peak` | `anon` peak | `anon` after (RSS) | threads at most |
|---|---|---|---|---|---|---|
| `f8c6e3a` (hedgehog `c418c2b`) | glibc, defaults | 17.0 s | 791 MiB | 695 MiB | 266 MiB (341) | 19 |
| same | `MALLOC_ARENA_MAX=2` | 17.7 s | 774 MiB | 567 MiB | 154 MiB | 20 |
| same | both tunables | 17.7 s | 613 MiB | 579 MiB | 122 MiB (198) | 19 |
| same | jemalloc | 16.9 s | 850 MiB | 664 MiB | 214, 138 after 60 s | 19 |
| same | mimalloc | 16.4 s | 685 MiB | 644 MiB | 331, 106 after 60 s | 17 |
| `84ad79f` (hedgehog `fac6eba`) | glibc, defaults | 15.7 s | 586 MiB | 515 MiB | 145 MiB (224) | 13 |
| same | both tunables (the image) | 16.2 s | 586 MiB | 504 MiB | 122 MiB (202) | 13 |

- **The OCR engine is kept** after the queue is empty (quail's pool keeps one idle engine per
  OCR job, for the life of the process), so memory after OCR never falls to the start (about
  20 MiB). What the allocator changes is how much of the freed page buffers it keeps: with
  glibc's defaults about 280 MiB more than with the tunables.
- **The image now sets** `GLIBC_TUNABLES=glibc.malloc.arena_max=2:glibc.malloc.mmap_threshold=131072`:
  a third less memory after OCR (810 to 527 MiB) and a 30 % lower peak, at the same speed. The
  fixed mmap threshold does most of it (buffers of 128 KiB and more are mapped and unmapped
  instead of staying in the heap); two arenas add a little for mixed files. Clear the variable
  to get glibc's defaults back.
- jemalloc and mimalloc give back memory a few seconds to a minute later and end close to the
  tunables; neither beat them on memory, so no allocator is linked in. mimalloc was 10 % faster
  in the OCR run (one run). doewood cannot call `malloc_trim` itself (`unsafe` is denied); the
  tunables do that job, and hedgehog trims after each big sheet.
- **Big-sheet threads:** with hedgehog `fac6eba` (which honours `threads: 1` in every inner
  fan-out) the process peaked at 13 threads with 4 workers on these files, against 19 to 20
  before, and its memory after the run fell from 266 to 145 MiB with glibc's defaults.
- Not measured: a 30 to 60 minute soak, quail's pool with no idle engine (`max_idle = 0`),
  3 M-cell workbooks under load and a bytes-per-cell sweep (see the #67 table above for that).

### Records

The same big-file run (204 records, `f8c6e3a`, Postgres 17). Sizes are the compact JSON a
record is stored as (SQLite stores this text as it is, in `records.body_json`; a separate SQLite
run was not made) and what Postgres stores in `records.record` (`pg_column_size`, TOASTed).

| record | JSON, median | JSON, largest | Postgres stored, median |
|---|---|---|---|
| one-page text PDF | 30 KB | 390 KB (150-page PDF) | 12 KB |
| small DOCX | 28.6 KB | 110 KB (22 MiB DOCX) | 12 KB |
| small TXT | 27 KB | 27 KB | 12 KB |
| XLSX of 20 rows | 4.7 KB | 65 KB (190k rows) | 2.4 KB |
| scanned page with OCR (other run, 20 records) | 33 KB | 34 KB | 14 KB |

- **Where the bytes go:** the fact envelope (`key`, `src`, `conf` and the value's type tag) is
  a third of every record. The largest facts: `signal.amount` 12 %, `summary.keywords` 9 %,
  `summary.digest` 7 %, `sketch.compat_minhash` 6 %, `party.company` 6 %, `summary.card` 5 %,
  `timings` 4 %, the two `*_minhash_bands` 4 % each, `hash.cdc` 4 %.
- **`hash.cdc`** (one entry per 8 KiB chunk, never read by doewood) is small on small files but
  76 % of the 22 MiB DOCX's record (110 KB, 26 KB without it; 2291 entries) and 71 % of the
  190k-row XLSX's.
- **In memory** a record was a `serde_json::Value` tree on its way to the store and back,
  about 16 times the JSON text: serde_json's maps are B-trees, and every small object (each
  fact, its `src`, its value) allocated a whole node. Since #98 the record is written as text
  straight from quail's record (the same bytes: compact, keys sorted) and read without a tree;
  `DocFacts` reads only the facts it uses. Per record, counted with a counting allocator on
  generated files (`2927bfa`, one run each; times are medians of 15 to 101 runs, workspace
  built at `opt-level=3`):

  | record (JSON text) | write: peak / held, before | after | read: peak / held, before | after | write / read time, before → after |
  |---|---|---|---|---|---|
  | small DOCX (20 KB) | 337 / 337 KB | 65 / 20 KB | 325 / 306 KB | 36 / 21 KB | 227 → 107 µs / 182 → 70 µs |
  | 150-page PDF (273 KB) | 4.9 / 4.9 MB | 0.61 / 0.27 MB | 4.5 / 4.4 MB | 0.43 / 0.28 MB | 3.7 → 1.6 ms / 3.0 → 1.1 ms |
  | 22 MiB DOCX (102 KB) | 1.8 / 1.8 MB | 0.30 / 0.10 MB | 1.9 / 1.8 MB | 0.12 / 0.10 MB | 1.0 → 0.41 ms / 0.94 → 0.25 ms |
  | 190k-row XLSX (227 KB) | 4.1 / 4.1 MB | 0.71 / 0.23 MB | 4.2 / 4.0 MB | 0.28 / 0.24 MB | 2.3 → 0.87 ms / 2.0 → 0.49 ms |

  *Write* is building the stored text (before: the tree, then its text); *held* is what stays
  while the graph and search are written (before: the tree). *Read* is a SQLite record read and
  `DocFacts` (before: the tree, and every fact read into a quail value). The write peak is a
  scratch buffer per nesting level for objects whose keys do not come sorted. Postgres still
  reads jsonb into a `Value` and writes the canonical text from it (the tree is dropped before
  the record is returned).
- **TOAST:** jsonb is 1.28 times the JSON text before compression; `pglz` (the default) stores
  37 % of the text (2.08 MB for these 204 records, 12.7 KB a record with the index). `lz4`
  stored 9 % more (2.27 MB) but wrote the records 45 % faster (98 against 179 ms) and read them
  13 % faster. Not worth a change for space; the default stays.

### The instance store's read-only connections (issue #83)

An on-disk instance store opens 2 read-only SQLite connections next to its writer (see
[configuration.md](configuration.md#the-instance-store)). Each keeps SQLite's default page
cache (up to 2000 KiB) and up to 128 prepared statements, and there is no `mmap`. Measured with
`reader_memory` (`cargo test -p doewood-store --lib -- --ignored --nocapture reader_memory`,
a 40 MiB store read in full on every connection): **2.1 MiB of RSS per reader** once its cache
is full, so about 4 MiB for the default 2 (2, 4 readers: 2.08 MiB each). They also hold
two more file handles to the store file and share its `-wal` and `-shm` files.

Effect, measured with `throughput` on an on-disk store (`DOEWOOD_BENCH_FILES=2000
DOEWOOD_BENCH_READERS=0|2`, 4 ingest workers, a `ListRecords` page of 100 every millisecond
through the API, test profile, 4 cores, 3 runs each, medians):

| watchers | readers | files/s | writer lock wait mean / p99 | `ListRecords` mean / p99 |
|---|---|---|---|---|
| 0 | 0 (before) | 105 | 2.03 ms / 59.2 ms | 85 ms / 160 ms |
| 0 | 2 | 239 | 0.51 ms / 3.7 ms | 57 ms / 162 ms |
| 10 | 0 (before) | 101 | 2.08 ms / 59.9 ms | 86 ms / 168 ms |
| 10 | 2 | 236 | 0.50 ms / 3.7 ms | 58 ms / 173 ms |

The reads no longer take the writer's lock, so ingest runs more than twice as fast while the
API reads, and the writer's lock wait falls by an order of magnitude. A `ListRecords` page
waits less on average, but its tail is the query itself (the test profile builds SQLite
without optimisation), not the lock.

### Requests and limits

Per doewood instance (one pod):

| workload | CPU request | memory request | memory limit |
|---|---|---|---|
| typical: text documents, a few scans | 500m | 512 MiB | 2 GiB |
| OCR-heavy: mostly scans | 2 to 4 (each OCR engine uses up to 4 threads) | 1.5 GiB | 4 GiB |

- The chart's defaults (500m, 1 GiB, limit 4 GiB) fit both, so they are unchanged: the request
  covers the OCR engine with one worker, the limit covers 4 workers with OCR plus a large
  spreadsheet in flight. With a lower limit the default worker count drops (half the room under
  the limit, 512 MiB a worker), and so does the memory budget (half the workers' plan: 768 MiB
  with 3 workers under 4 GiB). Files over 16 MiB beyond the budget wait. A workbook over the
  cell cap fails: each worker's share of half the room under the limit at 320 bytes a cell,
  from 1,600,000 to 10,000,000 (2,100,000 with 3 workers under 4 GiB, 6,700,000 for a 2 GiB
  worker; see [ingest.md](ingest.md#memory)). Set `DOEWOOD_INGEST_BUDGET_MIB` lower if big
  PDFs or DOCX files come several at a time on a small limit. For bigger workbooks, raise the
  memory limit or lower `DOEWOOD_INGEST_JOBS` (the cap grows with the memory per worker), or
  set `DOEWOOD_INGEST_MAX_CELLS` with about 320 bytes a cell of room for each worker.
- OCR throughput follows the CPU: with a 500m request an OCR-heavy pod gets only what the node
  has spare. Plan about 1500 scanned pages an hour per instance on 4 cores, and more instances
  (each with its own sources) rather than more workers to go faster. A second OCR engine needs a
  memory limit of at least 10 GiB.
- Postgres for a few instances: 256 MiB to 512 MiB of memory is enough for ingest; size
  `shared_buffers` and the disk for the records (about 40 KiB a scanned page).
