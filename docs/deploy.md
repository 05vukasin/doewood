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

CI (`image` job) builds the image, runs this smoke test, and lints and renders the chart, on every
pull request and every push to `staging` and `main`. It pushes nothing.

### The published image

`.github/workflows/image.yml` publishes the image to GitHub Container Registry as
`ghcr.io/05vukasin/doewood`, only from the release branch `main` and from `vX.Y.Z` tags (never
from `staging` or a pull request). Each platform (linux/amd64, linux/arm64) is built on its own
runner, smoke-tested as above, and pushed; the two are joined into one multi-platform image.

| pushed from | tags |
|---|---|
| a push to `main` | `edge`, `sha-<short commit>` |
| a tag `vX.Y.Z` | `X.Y.Z`, `X.Y`, `latest` |

```sh
docker pull ghcr.io/05vukasin/doewood:edge
docker run --rm ghcr.io/05vukasin/doewood:edge version
```

Pin a digest (`ghcr.io/05vukasin/doewood@sha256:...`) or a version tag in production; `edge`
moves with every push to `main`.

Every published image carries an SBOM and maximal build provenance (BuildKit attestations), a
GitHub build-provenance attestation, and a keyless cosign signature. The certificate is issued by
GitHub's OIDC provider to this workflow:

```sh
cosign verify ghcr.io/05vukasin/doewood:edge \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com \
  --certificate-identity-regexp '^https://github.com/05vukasin/doewood/\.github/workflows/image\.yml@refs/(heads/main|tags/v.+)$'
gh attestation verify oci://ghcr.io/05vukasin/doewood:edge --repo 05vukasin/doewood
docker buildx imagetools inspect ghcr.io/05vukasin/doewood:edge --format '{{ json .SBOM }}'
```

In the Helm chart, set `image.repository: ghcr.io/05vukasin/doewood` and `image.tag` to a
released version.

## Helm

The chart in `charts/doewood` runs a `StatefulSet` (pods start in parallel; each gets its own
`PersistentVolumeClaim` from `volumeClaimTemplates`), a headless Service naming each pod, and a
Service for the gRPC (7443) and ops (7444) ports. The liveness probe is `/healthz` (every 10 s,
6 failures), the readiness probe `/readyz` (every 2 s, 8 failures; see
[Time to ready](#time-to-ready-issue-118) for why). `DOEWOOD_INSTANCE_ID` is the pod name. Parallel starts against a new, empty
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
| `ocrJobs` | `2` | `DOEWOOD_OCR_JOBS`: OCR engines at once; empty for the binary's default (one below 10 GiB). 2 read 35 to 45 % more scanned pages a second than 1, same text, about 0.55 GiB more (see [OCR per page](#ocr-per-page)) |
| `resources` | requests 500m CPU and 1638 MiB, limit 4710 MiB | one OCR engine needs about 1 GiB / 4 GiB; the second engine of `ocrJobs: 2` adds 0.6 GiB to both (see [Sizing](#sizing)); the ingest jobs follow the memory limit |
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
- **Ingest results dropped:** `doewood_ingest_finish_unmatched_total` growing much faster than
  objects are changed or deleted while being ingested. Each count is a result that was not
  recorded; when its row is still pending, the next sweep ingests the object again (issue #125).
- **Instance store WAL:** `doewood_store_wal_bytes` above a few hundred MiB for more than a few
  minutes, or `doewood_store_wal_truncations_busy_total` growing on every try (one every 5 s).
  The store empties its WAL when it is over 64 MiB or idle (issue #151); a WAL that stays
  large means a read or a write holds a lock longer than the 50 ms the checkpoint waits for,
  for example a backup tool or an `sqlite3` shell with a transaction open on the file.
- **Template-heavy sources** (not an alert): `doewood_ingest_similar_buckets_skipped_total`
  counts band keys the `similar_to` query skipped because more than `similar_max_bucket`
  documents share them (issue #148). It grows when one template (one supplier's invoices)
  passes the limit; those documents then link only through their less common bands.

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
| 50,000 and 200,000 small documents | default (4) | see [below](#50000-to-200000-documents-issue-127) | 144 and 101 files/s (SQLite), 329 and 256 (Postgres) | | |
| enrichment pass with mocked registries | | not measured | | | |

The earlier measurement without OCR (2000 small text PDF, DOCX, TXT and XLSX files): doewood
peaked at 159 MiB, Postgres at 208 MiB, MinIO at 369 MiB.

What the numbers say:

- **OCR is one page at a time here, whatever the jobs.** quail sizes the OCR engines from the
  memory: one engine per 1 GiB of 20 % of the total memory (and 40 % of the available), at least
  1, at most 4, each with `cpus / engines` threads (at most 4). On 7.6 GiB, and under any limit
  below 10 GiB, that is 1 engine with 4 threads (`ocr: Ready { jobs: 1, threads: 4 }` in the
  `ingest started` log line). It uses about 2.7 of the 4 cores (6.3 CPU seconds a page).
  `DOEWOOD_INGEST_JOBS` sets the file workers, not the engines; `DOEWOOD_OCR_JOBS` sets the
  engines (see [OCR per page](#ocr-per-page)).
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
  16.5 MiB), a 20-page one about 100 KiB. Small text documents take about 18 KiB each at
  50,000 and 200,000 documents (see [below](#50000-to-200000-documents-issue-127)).
- **MinIO** used 600 to 1300 MiB, most of it page cache; its anonymous memory was 250 to 490
  MiB.

### OCR per page

Issue #116: `crates/doewood-ingest/tests/ocr_profile.rs` (ignored; how to run it is in its
header). In process, release build, quail `62108d4`, the server above, single runs. The pages
are made as in #63 (invented invoice text, Helvetica 11 pt, about 1550 characters, 300 DPI
8-bit gray, Flate, image-only, about 240 KiB a page), so OCR reads the embedded scan: no render
and no 300 DPI retry. 12 pages a row unless stated. CPU is the process's user + system time;
"cores busy" is its mean over the run, sampled every 250 ms.

Where one page's time goes (one engine, 4 ONNX Runtime threads, warm):

| stage | per page |
|---|---|
| parse the PDF and inflate the scan | 6 ms |
| quail's OCR engine | 1.80 s, 5.2 CPU s (2.9 cores) |
| of which page orientation | under 0.1 s (within the noise) |
| of which text detection (and the image copies) | 0.50 s (a blank page of the same size) |
| of which line orientation | under 0.1 s (within the noise) |
| of which recognition (one line at a time) and the rest | about 1.3 s |
| every other step of the record (`item.lines`, `sketch.*`, `text.*`, ...) | about 12 ms |

A new engine loads in 0.25 s; its first page takes 2.0 s and grows the RSS by about 420 MiB
(ONNX Runtime's arenas), which then stays.

Engines and threads (`DOEWOOD_OCR_JOBS`; threads are 4 / jobs):

| run | OCR jobs x threads | pages/s | CPU s a page | cores busy | peak RSS |
|---|---|---|---|---|---|
| engines alone, warm | 1 x 4 (the default here) | 0.53 | 5.45 | 2.90 | 757 MiB |
| same | 2 x 2 | 0.77 | 4.49 | 3.47 | 1300 MiB |
| same | 4 x 1 | 1.07 | 3.62 | 3.85 | 2379 MiB |
| analyzer, 1 worker | 1 x 4 (default) | 0.52 | 5.43 | 2.83 | 745 MiB |
| analyzer, 4 workers | 1 x 4 (default) | 0.53 | 5.44 | 2.90 | 886 MiB |
| same | 2 x 2 (`DOEWOOD_OCR_JOBS=2`) | 0.71 | 4.87 | 3.43 | 1401 MiB |
| same | 4 x 1 (`DOEWOOD_OCR_JOBS=4`) | 0.98 | 3.82 | 3.73 | 2376 MiB |

The analyzer rows include each engine's cold first page.

- **OCR is the whole cost of a scan:** 99.3 % of the record's step time is `ocr.pages`, and
  recognition is about three quarters of that. Page and line orientation cost almost nothing.
- **One engine leaves cores idle.** Recognition runs one short text line at a time, and 4
  intra-op threads keep only 2.9 cores busy. With 4 workers the other workers wait for the one
  OCR permit (`ocr.pages` then counts the wait: 6.5 s a page against 1.9 s with 1 worker), so
  more workers do not help.
- **More engines with fewer threads are faster on the same cores:** 2 x 2 reads 1.3 to 1.45
  times as many pages a second, 4 x 1 1.85 to 2 times as many, with a third less CPU a page. The
  cost is memory: about 0.55 GiB more per engine. The text is byte-identical (`same_output`:
  every engine output for 1, 2 and 4 threads, and the records without timings for 1 and 4
  workers and 1, 2 and 4 jobs).
- **What to set:** the default stays quail's plan (one engine per GiB of 20 % of the total
  memory, so one below 10 GiB). Set `DOEWOOD_OCR_JOBS=2` where a pod has room for about 1.5
  GiB of OCR (the chart does since 2026-11-26: `ocrJobs: 2`, with 0.6 GiB more request and limit), and 4 only with about 2.5 GiB for OCR and 4 cores;
  jobs above the workers are not used.
- The single-page and 20-page runs of #63 above all ran with one engine; their 0.42 to 0.66
  pages/s include the store, S3 and the pipeline, which this in-process test leaves out.

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
run was not made) and what Postgres stored in `records.record` (`pg_column_size`, TOASTed;
jsonb until schema 8, text since, see [below](#records-as-text-on-postgres-issue-121)).

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
  scratch buffer per nesting level for objects whose keys do not come sorted. Postgres read
  jsonb into a `Value` and wrote the canonical text from it until schema 8; it now stores
  and returns the text as SQLite does (issue #121).
- **TOAST:** jsonb is 1.28 times the JSON text before compression; `pglz` (the default) stores
  37 % of the text (2.08 MB for these 204 records, 12.7 KB a record with the index). `lz4`
  stored 9 % more (2.27 MB) but wrote the records 45 % faster (98 against 179 ms) and read them
  13 % faster. Not worth a change for space; the default stayed for jsonb. Since schema 8 the
  record is text, and its column uses lz4 (below).

### Records as text on Postgres (issue #121)

Until schema 8 Postgres kept a record as jsonb. Every read turned it back into text on the
server (jsonb's binary wire format is that text too, so reading it binary or as `::text`
costs the same), and doewood parsed that text into a `Value` and wrote the canonical text
again. Under heavy API reads this was 92-95 % of the database's time. Since schema 8 the
canonical text that ingest writes is stored as it is (`records.body`) and sent back
unchanged; only `fact_texts` (the enrichment's e-mail domains) looks inside a record, and it
parses `body` as jsonb on the fly, as SQLite parses `body_json` (0.16 ms a record).

Measured on the ~1,840 records of a `throughput` run (about 15.5 KB of canonical JSON each),
Postgres 17 on an idle server, a page of 100 by key (median of 3 rounds of 17 pages):

| stored as | database time per page | stored per record (column / table with index) | server time to store a record |
|---|---|---|---|
| jsonb (before) | 12.5 ms | 6,730 / 8,481 bytes | 0.48 ms (parse and pglz) |
| jsonb, read as `record::text` | 13.5 ms | same | same |
| text, pglz | 3.6 ms | 5,486 / 6,473 bytes | 0.24 ms |
| **text, lz4 (chosen)** | **2.4 ms** | 5,996 / 7,065 bytes | 0.05 ms |
| jsonb and text side by side | as text | 13,752 bytes (table) | both |

- **Text, not jsonb next to it:** no query needs jsonb's operators often enough to store the
  record twice; lz4 is chosen for its reads and writes, at 9 % more space than pglz and still
  17 % less than the jsonb was. A server built without lz4 keeps pglz (the migration notes it).
- **Same bytes out:** the stored text is the canonical text, so the API returns what it did.
  Two values where jsonb did not give the text back now come back as written, as on SQLite:
  `-0.0` (jsonb's numeric has no negative zero, so it read as `0.0`) and numbers near
  `f64::MAX` (jsonb wrote them as 309 digits, which doewood could not read back). A record
  with `\u0000` is still refused, as jsonb refused it, so every stored record parses as jsonb.
- **Under load** (`throughput`, release, 2000 files, 4 workers, 4 readers, 8 clients asking
  for a `ListRecords` page of 100 every millisecond; `main` `056e4b2` once, this change twice):

  | | files/s | `ListRecords`: pages / DB time per page / mean | all DB time | record upsert, DB |
  |---|---|---|---|---|
  | before (jsonb) | 60 | 2,725 / 25.3 ms / 95 ms | 73.3 s (94 % `ListRecords`) | 0.66 ms |
  | after (text, lz4) | 110, 114 | 3,889-3,927 / 5.3-7.5 ms / 33-35 ms | 23.6-32.6 s | 0.18-0.19 ms |

  Ingest runs 1.9 times as fast while the API reads 43 % more pages, each in a third of the
  time. What is left of a page's database time is reading and decompressing 1.5 MB.
- **Upgrade:** the migration adds `body`, makes `record` nullable and adds the partial
  index `records_body_pending` (the rows still to convert), so it takes seconds even on a big
  table, though the index build reads the table once under the migration's lock. **All
  instances are upgraded together**: an older instance refuses schema 8, and it could not read
  a converted row. After a start, an instance converts the rows from before in the background:
  100 rows per transaction, taken with `FOR UPDATE SKIP LOCKED` (instances share the work), a
  20 ms pause between batches, the text the reads gave before, `updated_at` untouched. It is
  resumable: a stop or a failure loses nothing, and the next start goes on. Until a row is
  converted it reads from its jsonb, with the same bytes; a changed record is converted when it
  is written. The log shows `converting stored records to text` (about every 10,000 rows) and
  `stored records converted to text`. Space of the old jsonb is reused after autovacuum (a
  `VACUUM FULL records` gives it back to the disk, but locks the table). The decision is
  [ADR 0003](adr/0003-records-as-canonical-text.md); the down path is in
  [configuration.md](configuration.md#the-shared-postgres).

### The instance store's read-only connections (issue #83)

An on-disk instance store opens read-only SQLite connections next to its writer (2 when this
was measured; one per CPU, at most 8, since #108; see
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

### Ingest on Postgres (issue #69)

`throughput` in `crates/doewood-ingest/tests/pipeline.rs` (ignored; see
[ingest.md](ingest.md#throughput)): 2000 generated small files (PDF, DOCX, TXT, XLSX, 4.7
MiB), fetched from memory, no OCR, in process, test profile. The shared store is the
instance's SQLite store, or Postgres 17.12 (`pgvector/pgvector:pg17`, default settings plus
`pg_stat_statements`) in a container on an `--internal` Docker network on the same machine.
`main` at `f8c6e3a` with this branch's counters; medians of 3 runs; `taskset` pins only the
doewood process.

| store | workers | files/s | runs | company lock: wait / held per file | statements per file |
|---|---|---|---|---|---|
| SQLite | 1 | 111 | 111, 110, 115 | 0 / 0.67 ms | |
| SQLite | 2 | 253 | 272, 250, 253 | 0.01 / 0.63 ms | |
| SQLite | 4 | 324 | 324, 333, 302 | 0.47 / 1.21 ms | |
| SQLite, `taskset -c 0,1` | 2 | 222 | 221, 222, 253 | 0.04 / 0.84 ms | |
| Postgres | 1 | 56 | 53, 56, 57 | 0 / 1.80 ms | 47 |
| Postgres | 2 | 136 | 136, 138, 133 | 0.08 / 1.54 ms | 47 |
| Postgres | 4 | 246 | 246, 243, 248 | 1.08 / 1.55 ms | 47 |
| Postgres, `taskset -c 0,1` | 2 | 122 | 122, 115, 124 | 0.11 / 1.72 ms | 47 |

Where the workers' time goes (4 workers, share of workers x wall time; the test prints it from
`Stats::time`, which the ingest log line at shutdown also carries):

| stage | SQLite | Postgres |
|---|---|---|
| instance store (status, source, settings) | 10% (1.2 ms/file) | 4% (0.6 ms) |
| analyze (hash, quail, hedgehog) | 30% (3.7 ms) | 21% (3.5 ms) |
| record (read, write) | 15% (1.8 ms) | 12% (2.0 ms) |
| graph and search | 40% (4.9 ms) | 58% (9.5 ms) |
| of which waiting for the company lock | 4% (0.5 ms) | 7% (1.1 ms) |
| of which holding it | 10% (1.2 ms) | 9% (1.6 ms) |

- **Postgres ingest is about 75% of SQLite at 4 workers (246 vs 324 files/s) and half at 1 or
  2.** The difference is round trips: 47 statements per file (from `pg_stat_statements`),
  of which Postgres spends 1.4 ms per file executing (2.8 s for 2000 files at 4 workers; the
  costliest are the record upsert, the sketch bands and the search row), while the graph
  stage alone takes 9.5 ms per file. Each statement waits for its answer before the next is
  sent. See #103.
- **The company lock is not the bottleneck at 4 workers or fewer.** Workers wait for it 0 to
  7% of their time; it is held 1.5 to 1.8 ms per file on Postgres (round trips under the
  lock), so one instance cannot pass about 600 files/s through it, 2.5 times what 4 workers
  reach here. It becomes the limit with more workers or cheaper files; fewer round trips
  (below) also shorten the time it is held. Issue #78 replaced it with locks per alias key
  (see [below](#party-locks-per-alias-key-issue-78)).
- More workers help both stores (1 to 4 workers: 2.9x SQLite, 4.4x Postgres, where workers
  mostly wait on the network). One SQLite worker is slower per file than two (every stage
  takes 20 to 30% longer; not explained yet).
- The instance store's lock wait (a trivial call every millisecond) stayed under 0.4 ms mean
  in every run.

### Fewer round trips per file on Postgres (issue #103)

The same `throughput` run (2000 files, test profile, Postgres 17 in a container bound to
127.0.0.1 on the same machine, `pg_stat_statements` loaded), one run each, `main` at
`574b4cc` against this change, back to back:

| workers | files/s before | files/s after | graph stage per file before / after | statements per file before / after |
|---|---|---|---|---|
| 1 | 44 | 61 | 13.4 / 6.9 ms | 24.1 / 11.3 |
| 2 | 150 | 194 | 8.1 / 4.6 ms | 24.1 / 11.3 |
| 4 | 206 | 277 | 12.2 / 6.6 ms | 24.1 / 11.3 |

(Statements as `pg_stat_statements` counts them, `BEGIN` and `COMMIT` included. The machine was
shared with other builds, so absolute files/s vary between sessions; the before and after
runs are next to each other.)

What changed, with the same rows written:

- **One statement instead of a transaction** for `upsert_location` (5 statements plus
  `BEGIN`/`COMMIT`), `put_sketch` (3 plus `BEGIN`/`COMMIT`) and `upsert_live_edge` (two
  `merged_into` reads, then a locked check and the write in a transaction): data-modifying
  CTEs, whose parts touch different rows and whose foreign keys are checked at the end of the
  statement. The live edge locks its ends `FOR SHARE` and writes only when both are live, in
  that one statement; a husk end costs one more round trip per `merged_into` hop.
- **`update_props` is a compare-and-set** (read the node and its row version `xmin`, write
  only if the row is still that version, else apply the patch again): two round trips
  instead of four, and still no lost write and no write onto a node a merge marked as a husk.
- **Batches**: `GraphStore::aliases` reads all the keys of a party's match ladder in one
  statement, and `GraphStore::claim_aliases` sends a party's claims (and learned aliases,
  and a document's numbers) pipelined on one connection: still one statement and one
  transaction per claim, run in order, in one round trip. Both have sequential defaults, so
  SQLite is unchanged.
- **Independent steps overlap** in `GraphWriter::write`: the document's props, its search
  row, its references (own numbers, then cites), its parties (under the company lock, as
  before) and its similar documents run together, each in its own order. A document without
  parties no longer takes the company lock.

`crates/doewood-ingest/tests/round_trips.rs` counts statements and round trips through a
proxy that reads the wire protocol, and fails above the targets (each store call above is
one round trip; the `throughput` mix is at most 15 statements and 14.5 round trips per
document, 32.25 of each before).

### Party locks per alias key (issue #78)

The one instance-wide company lock (`GraphWriter::companies`) is now a lock per alias key: a
document's parties take a striped lock for each identifier, ladder key and learned alias they
use, in order, plus a shared `RwLock`, for the same span as before. A form-blind match onto a
company without a legal form is made again with that `RwLock` held exclusively (the
reasoning is on `PartyLocks` in `crates/doewood-ingest/src/graph.rs`).

Postgres 17 (`pgvector/pgvector:pg17`, `pg_stat_statements` loaded) in a container on an
`--internal` Docker network, test profile, `staging` at `d659ebf` (before) against this change
(after). The two test binaries were built once and run in turns (before, after, before, ...)
through `serial`, so a load change on the shared machine hits both. Medians of 3:

| benchmark | workers | before | after | runs before / after |
|---|---|---|---|---|
| `throughput`, 2000 files, files/s | 1 | 65 | 64 | 62, 65, 66 / 64, 64, 69 |
| `throughput`, 2000 files, files/s | 2 | 210 | 215 | 210, 212, 158 / 215, 223, 170 |
| `throughput`, 2000 files, files/s | 4 | 303 | 308 | 233, 309, 303 / 244, 308, 326 |
| `company_rows` `spread`, 3000 documents, docs/s | 4 | 72 | 217 | 61, 72, 72 / 217, 211, 226 |
| `company_rows` `hot`, 3000 documents, docs/s | 4 | 104 | 105 | 104, 104, 104 / 113, 105, 99 |

- **No measurable change in `throughput`.** Its workers waited 0.01 to 0.02 ms per file for
  the lock before (it overlaps the other graph steps since #103), so nothing was waiting on it.
- **3x on the graph alone when companies differ** (`company_rows` `spread`: every company on
  at most two documents, written straight through `GraphWriter` by 4 writers). Waiting for
  the lock went from 31 to 46 ms per document to 1.2 to 2.1 ms.
- **No change when one company is on every document** (`hot`): every document shares that
  supplier's keys, so they still run one after the other, as they must.
- Several workers resolving overlapping parties at once give the graph of one worker
  (`crates/doewood-ingest/tests/party_locks.rs`, SQLite and Postgres; 1000 shuffled rounds of
  4 writers on each store passed). Each protection, removed on purpose (the form-blind
  fallback, the learned aliases in the lock set, the form-blind key in it), made the tests
  fail within 100 rounds.

### Similar documents on near-duplicate corpora (issue #126)

`near_duplicate_corpus` in `crates/doewood-ingest/tests/round_trips.rs` (ignored) writes a
generated corpus through the graph writer, one document after the other: four in five
documents come from one of 4 templates (a shared 300-word body under a header of their own,
like the invoices of one supplier; Jaccard about 0.9, most of the 32 LSH bands shared), the
fifth is distinct. So a template document has 100 candidates (`max_candidates`) and about 80
`similar_to` edges on average over the corpus. Test profile, Postgres 17 in a container on
an internal Docker network on the same machine, a SQLite store file; `main` at `baf53ad`
against this change, back to back on a shared machine:

| store | documents | graph write per document before / after | round trips per document before / after |
|---|---|---|---|
| Postgres | 1,200 to 3,600 | 114 to 120 / 20 to 25 ms | 166 / 7.6 |
| SQLite | 1,200 to 4,000 | 35 to 42 / 23 to 34 ms | (one writer hop per edge / one per batch) |

(Round trips counted through the proxy at 2,000 documents; the times are with a direct
connection, since the proxy adds to every message.) The edges are the same: the digest of
every `similar_to` edge (ends by key, props, confidence) is equal before and after on both
stores, and `tests/similar.rs` compares the edges with the per-candidate loop of before on
both stores in CI.

What changed: `GraphStore::node_ids` reads the candidates' Document nodes in one statement
(the few missing ones are still created one by one), and `GraphStore::upsert_edges` writes
the edges in one transaction: one statement on Postgres (an `INSERT ... SELECT FROM unnest`
upsert; since #148, before that the upserts were pipelined between `BEGIN` and `COMMIT`,
which the client could split into more round trips on a busy machine), one writer call on
SQLite. A document costs 8 round trips on Postgres
however many similar documents it has (`similar_documents_take_a_fixed_number_of_round_trips`),
against 8 plus 2 per similar document before.

**The candidate query still grows with the bucket.** It counts every `sketch_bands` row
that shares a band, then keeps the best 100. At 10,000 documents (2,000 per template) it
reads 53,543 index entries for one template document (`EXPLAIN (ANALYZE, BUFFERS)`: 26 ms;
13.5 ms best of 5 from the client), against 10,400 entries at 2,000 documents: linear in the
number of near-duplicates, so quadratic over a corpus of one template. The graph write per
document rose from 25 ms at 2,000 documents to 31 ms at 10,000 on Postgres. Bounded since
issue #148, below.

### Stop bands (issue #148)

The same `near_duplicate_corpus` on Postgres, on this change, with the default
`similar_max_bucket` of 1000 against 1,000,000 (no bound in practice), one run each:

| documents | graph write per document, no bound / 1000 | candidate query (best of 5), no bound / 1000 | `similar_to` per document, no bound / 1000 | bands skipped per document (1000) |
|---|---|---|---|---|
| 3,000 | 18.1 / 17.5 ms | 9.6 / 8.0 ms | 80 / 80 | 0 |
| 6,000 | 21.9 / 20.5 ms | 9.2 / 2.2 ms | 80 / 80 | 6 (the buckets pass 1000) |
| 9,000 | 25.7 / 11.0 ms | 27.1 / 1.1 ms | 80 / 57 | 22 |
| 10,000 (10,500) | 30.7 / 11.1 ms | 15.8 / 0.4 ms | 80 / 53 | 22 |
| 15,000 | not run / 11.4 ms | not run / 1.5 ms | 52 | 22.5 |

(Per window of a tenth of the corpus; the candidate query is timed for the last template
document of the window. The machine was shared with other builds.)

On SQLite (a store file, 10,000 documents, the bound at 1000) the graph write per document
rose to 38.8 ms at 5,000 documents and came back to 20.7 ms at 10,000 (candidate query 8.1 ms
at 5,000, 0.2 to 0.5 ms from 8,000 on); the same 22 bands per document are skipped. (The
measurement of #126 on SQLite went from 35 to 42 ms per document between 1,200 and 4,000
documents, still rising.)

With the bound the graph write stays flat once the 4 templates pass 1000 documents each
(about 5,000 documents): at 15,000 documents the candidate query reads 32 `sketch_band_sizes`
rows and 128 `sketch_bands` entries (`EXPLAIN ANALYZE` 3.0 ms), against 53,543 entries and
31 ms at 10,000 without it. The cost is fewer edges for template documents: they link only
through the bands their template does not share with more than 1000 others (52 instead of
80 edges per document on average over the corpus, 22 of 32 bands skipped). With the bound
off, the edges are the same as before: the digest of every `similar_to` edge at 10,000
documents is the one of #126 (`1cbe59b1…`).

### API under many clients (issue #80)

`api_load` in `crates/doewood-api/tests/api_load.rs` (ignored): an in-process API server on a
seeded store (10,000 documents with records, locations, search rows, a `party` edge each to one
of 500 companies and a `similar_to` chain; 20 sources), driven over loopback by tonic clients in
the same process. Each client is its own connection and a closed loop (no think time) of
`ListSources` 10%, `ListRecords` (page of 100) 20%, `GetNode` 15%, `Neighbours` (`similar_to`)
15%, `CompanyDocuments` 15% and `Search` 25%; one client in ten holds a `WatchProgress` stream
instead (on a connection of its own), woken every 10 ms as ingest does. A shed call
(`UNAVAILABLE`) is retried as [api.md](api.md#limits) advises (100 ms doubling to 10 s, jitter).
Default limits (`max_calls_in_flight` 256, 256 streams per connection, the Postgres pool of 16;
the instance store in a file with its 2 read-only connections, as `serve` opens it). Release
build, 10 s per step after a 2 s warm-up, 3 runs; the table is the median run. SQLite rows on
`main` at `93376bb` (with #83), Postgres rows at `f8c6e3a`. Postgres 17.12
(`pgvector/pgvector:pg17`, default settings) in a container on an `--internal` Docker network,
on the same 4 vCPU; nothing calls out (no enrichment, no registries, no icons).

```
cargo test --release -p doewood-api --test api_load -- --ignored --nocapture
DOEWOOD_TEST_POSTGRES_URL=postgres://... cargo test --release -p doewood-api --test api_load -- --ignored --nocapture
```

(`DOEWOOD_BENCH_CLIENTS=10,100,500,1000`, `DOEWOOD_BENCH_SECS`, `DOEWOOD_BENCH_DOCS`,
`DOEWOOD_BENCH_COMPANIES`,
`DOEWOOD_BENCH_PER_CONN`, `DOEWOOD_BENCH_WATCH_PCT`, `DOEWOOD_BENCH_IDLE`,
`DOEWOOD_BENCH_ONLY=<method>`, `DOEWOOD_BENCH_BACKOFF_MS`, `DOEWOOD_BENCH_DB_CPU_STAT`: see the
file's header.)

| store | clients | calls/s | p50 | p99 | shed / 10 s | process CPU | RSS | threads |
|---|---|---|---|---|---|---|---|---|
| SQLite | 10 | 4,943 | 1.9 ms | 5.4 ms | 0 | 2.9 cores | 45 MiB | 33 |
| SQLite | 100 | 4,505 | 17 ms | 75 ms | 0 | 2.9 | 70 MiB | 123 |
| SQLite | 500 | 4,743 | 40 ms | 197 ms | 15,354 | 3.0 | 139 MiB | 280 |
| SQLite | 1000 | 4,358 | 44 ms | 216 ms | 30,396 | 3.0 | 209 MiB | 284 |
| SQLite, no readers | 10 / 100 / 1000 | 3,586 / 3,320 / 3,031 | 2.5 / 23 / 66 ms | 7.5 / 89 / 255 ms | 0 / 0 / 22,773 | 2.1 to 2.3 | | |
| Postgres | 10 | 5,109 | 1.6 ms | 6.0 ms | 0 | 1.8 (+1.8 database) | 21 MiB | 15 |
| Postgres | 100 | 5,903 | 17 ms | 33 ms | 0 | 2.1 (+1.7) | 35 MiB | 17 |
| Postgres | 500 | 5,645 | 44 ms | 93 ms | 19,722 | 2.2 (+1.6) | 93 MiB | 17 |
| Postgres | 1000 | 5,380 | 48 ms | 110 ms | 37,827 | 2.2 (+1.5) | 159 MiB | 17 |

Calls/s are successful calls; process CPU, RSS and threads are the whole test process, the
clients included. Runs: SQLite 3,978 to 4,943 calls/s in every step of the three runs
(`DOEWOOD_BENCH_READERS=0`, every read on the writer as before #83: one run); Postgres 4,570
to 6,005 in two runs, and 2,671 to 3,008 in the third (see below).

What the numbers say:

- **SQLite saturates at 10 clients** (about 4,500 calls/s; 3,300 with every read on the
  writer): the API's reads share the store's 2 read-only connections, each behind its own
  lock, and the process stays below 3 of the 4 cores. The read-only connections (#83) added
  about a third and freed the writer: a trivial write-side call now waits 7 us (p50) at any
  load, against 1.7 ms at 10 clients and 57 ms at 500 without them. More clients only add
  latency, and blocking-pool threads parked on the connections' locks (one per call in
  flight: 284 threads at the cap). One call alone (one client) takes 0.2 ms (`GetNode`) to
  1.7 ms (`ListRecords`, 100 records); `Neighbours` and `CompanyDocuments` made two store
  calls and waited twice (one since #124: see
  [below](#one-store-call-for-neighbours-and-companydocuments-issue-124)). More readers were
  the next step: see [#108](#api-read-capacity-issue-108).
- **Postgres saturates at about 100 clients** (about 6,000 calls/s), on the pool of 16
  connections: `GetNode` is 0.8 ms at 10 clients and 13 ms at 100, nearly all of it waiting
  for a pooled connection, and `Neighbours` and `CompanyDocuments` (two checkouts each, one
  since #124) twice that. Neither the database (1.5 to 1.8 cores) nor the process (2.2 cores) is at the
  machine's 4 cores. `ListSources` (the instance store) stays at 1 to 3 ms.
- **Beyond the cap, shedding works**: with 500 and 1000 clients the server refuses the calls
  over 256 in flight at once (`doewood_grpc_shed_total` agreed with the clients' count within
  5%), the calls it takes keep their latency (p99 under 230 ms on SQLite, under 110 ms on
  Postgres), and throughput stays within 10% of the peak. No call failed otherwise, no stream ended
  early, and nothing hit the 60 s deadline. With impatient clients (a fixed 20 ms retry,
  `DOEWOOD_BENCH_BACKOFF_MS=20`, one run) a 1000-client step sheds 10 times as many calls
  (about 25,000 a second) and Postgres throughput falls to 2,252 calls/s (SQLite: 3,428): the
  refusals cost CPU (the process at 3 to 3.8 cores) that the calls then lack. Clients must
  back off as documented, and since #104 the server tells them how long (see
  [Retries and shed calls](#retries-and-shed-calls-issue-104)).
- **Memory:** 13 to 13.5 KiB per idle connection (2000 opened at once: +26 MiB, client side
  included), about 70 KiB per busy client (1000 clients with 100 streams: 160 to 210 MiB).
- **Postgres `Search` is unstable between runs:** in one run of three `Search` took 6.9 ms
  alone instead of 1.8 ms, the database used 2.8 cores, and throughput was half (2,700 to
  3,000 calls/s) in every step; in a fourth run the slow mode ended by itself in the last
  step (2,624 to 4,402 calls/s, the database from 2.8 to 1.8 cores). The likely cause is the GIN
  index's pending list (new search rows wait there until a vacuum, and every search scans it): a
  rare-term lookup on 10,000 rows took 0.54 ms with the list full and 0.065 ms after `VACUUM`.
  Fixed in #102: see [Search and the GIN pending list](#search-and-the-gin-pending-list-issue-102).
- The network layer is not the limit (as the io_uring research expected): the store's
  connections (SQLite) and the pool (Postgres) are, well below the HTTP/2 stack's 60,000+ requests/s on
  this machine.

### Search and the GIN pending list (issue #102)

`search_index.tsv` has a GIN index (`search_tsv`). With Postgres's default (`fastupdate` on,
`gin_pending_list_limit` 4 MB) a new row's index entries first go to the index's pending
list, which is merged into the index only by a vacuum or when it passes the limit, and every
search scans the whole list. So `Search` was fast or slow depending on when autovacuum last
ran. Since shared schema 7 the index has `gin_pending_list_limit = 256` kB: the list never
holds more than about 32 pages, and an insert that fills it merges it.

Measured with `api_load` (issue #80; `DOEWOOD_BENCH_ONLY=Search DOEWOOD_BENCH_CLIENTS=10`,
10,000 seeded documents, release build), Postgres 17.12 (`pgvector/pgvector:pg17`) on the
machine above, on a Docker network created with `--internal`; ranges of 3 runs:

| `search_tsv` | pending pages at the start | `Search` calls/s | p50 | p99 |
|---|---|---|---|---|
| default (before), as seeded | 282 to 297 | 2,749 to 2,765 | 3.34 to 3.36 ms | 8.1 to 8.4 ms |
| default, `VACUUM` after seeding | 0 | 6,898 to 7,143 | 1.32 to 1.38 ms | 2.8 to 3.0 ms |
| `gin_pending_list_limit = 256` (now) | 0 to 30 | 5,857 to 6,934 | 1.34 to 1.61 ms | 3.3 to 3.6 ms |
| `fastupdate = off` | 0 | 6,840 to 7,347 | 1.29 to 1.38 ms | 2.7 to 3.1 ms |

Writing cost. `throughput` (`DOEWOOD_BENCH_JOBS=4 DOEWOOD_BENCH_FILES=2000`, test profile,
medians of 3, before the fewer round trips of #103): 246 files/s before (238 to 249), 242
with 256 kB (240 to 250), 240 with `fastupdate = off` (238 to 250): within the noise,
because the graph stage's round trips dominated there (see
[Ingest on Postgres](#ingest-on-postgres-issue-69)). Inserts
alone, `pgbench` (20 s, one run each) with 4 clients each inserting one search row per
statement into a copy of the table (generated text, the tsvector built in the insert):

| text per row | default | 256 kB | `fastupdate = off` |
|---|---|---|---|
| 300 words (about 2 KiB, like the small files) | 2,571 rows/s | 2,369 (-8%) | 1,463 (-43%) |
| 5,000 words (about the 32 KiB search body cap) | 233 rows/s | 210 (-10%) | 139 (-40%) |

- **The pending list was the cause**: the same data with the list merged (`VACUUM`) answers
  `Search` 2.5x faster, and a full list made every run slow. With 256 kB the list holds at
  most about 32 pages, so a search stays within about 20% of the merged index whatever
  autovacuum did.
- **Why not `fastupdate = off`:** it gives the same search latency but makes every insert
  update the GIN entries directly, about 40% fewer search rows a second with concurrent
  writers. The 256 kB list keeps most of the batching (8 to 10% fewer rows a second, in the
  writer that fills the list).
- Nothing needs tuning: the limit is on the index (it overrides the server's
  `gin_pending_list_limit`), and autovacuum still merges the list as before.

### Retries and shed calls (issue #104)

A call over `max_calls_in_flight` is refused at once with `UNAVAILABLE` and a
`grpc-retry-pushback-ms` trailer of 100 to 199 ms (see [api.md](api.md#limits)). Refusing is
cheap per call but not free, so clients that retry fast take CPU from the calls that run.
Measured with `api_load` (issue #80; 1000 clients, `max_calls_in_flight` 256, Postgres, the
mix of calls, release build, one 10 s run each):

| client retry | calls/s served | shed / 10 s | p50 / p99 | process CPU |
|---|---|---|---|---|
| as docs/api.md advises: 100 ms doubling to 10 s, jitter | 5,410 | 38,722 | 47 / 100 ms | 2.2 cores |
| a fixed 20 ms (`DOEWOOD_BENCH_BACKOFF_MS=20`) | 2,125 | 247,843 | 129 / 289 ms | 3.1 cores |
| a fixed 20 ms, but the pushback when the server sends one (`DOEWOOD_BENCH_PUSHBACK=1`) | 5,185 | 49,439 | 50 / 106 ms | 2.3 cores |

A client that honours the pushback (as a gRPC client with a retry policy does) keeps
throughput within 5% of the documented backoff, even if its own backoff is too short.

### API read capacity (issue #108)

#105 (above) found the API saturating on the store's read-only connections (SQLite) and the
pool (Postgres). Both are now sized from the CPUs the process may use
(`available_parallelism`, which counts a cgroup CPU quota): **SQLite readers: one per CPU,
between 1 and 8** (was 2); **Postgres pool: 4 per CPU, between 16 and 64** (was 16). Neither
is a setting (ADR 0001); `serve` logs both (`store open` `readers`, `shared Postgres store
ready` `pool`).

| CPUs | 1 | 2 | 4 | 8 | 16 | 64 |
|---|---|---|---|---|---|---|
| SQLite readers | 1 | 2 | 4 | 8 | 8 | 8 |
| Postgres pool | 16 | 16 | 16 | 32 | 64 | 64 |

Measured with `api_load` (as above: release build, 10 s per step, 10,000 documents, 10% of the
clients watching progress; `DOEWOOD_BENCH_CLIENTS=10,100,500 DOEWOOD_BENCH_READERS=2|4
DOEWOOD_BENCH_POOL=16|32`) on the same 4 vCPU, `main` at `012a1e6` plus this branch, Postgres
17 (`pgvector/pgvector:pg17`, default settings but `max_connections = 200` and
`pg_stat_statements`) in a container on an `--internal` Docker network. SQLite: 2 runs each (the
second 2-reader run stopped before its 500-client step), Postgres: 1 run each.

| store | clients | calls/s | p50 | p99 | process CPU |
|---|---|---|---|---|---|
| SQLite, 2 readers (before) | 10 / 100 / 500 | 4,957 to 5,043 / 4,404 to 4,455 / 4,414 | 1.8 / 17 / 43 ms | 5.3 / 76 / 209 ms | 2.9 cores |
| SQLite, 4 readers (now, 4 CPUs) | 10 / 100 / 500 | 5,216 to 5,218 / 5,209 to 5,282 / 4,877 to 5,105 | 1.8 / 15 / 37 to 39 ms | 4.9 / 69 / 197 to 207 ms | 3.2 to 3.4 |
| Postgres, pool 16 (before and now on 4 CPUs) | 10 / 100 / 500 | 4,151 / 4,431 / 5,083 | 2.2 / 22 / 47 ms | 6.8 / 43 / 106 ms | 1.5 to 2.0 (+1.8 to 2.2 database) |
| Postgres, pool 32 | 10 / 100 / 500 | 4,082 / 4,607 / 5,274 | 2.2 / 22 / 50 ms | 6.9 / 45 / 107 ms | 1.5 to 2.1 (+1.7 to 2.3) |

- **SQLite: one reader per CPU is worth 3 to 5% at 10 clients and 10 to 19% at 100 and 500**
  on 4 CPUs, and the per-method latencies fall by about the same share. The process then uses about
  3.4 of the 4 cores, clients included: it is CPU-bound, so readers beyond the CPUs would only
  queue on the cores instead of on the connections. Each reader costs about 2.1 MiB (above),
  so 8 cost about 17 MiB; 8 is the cap because more than 8 was not measured.
- **Postgres: 32 connections are no faster than 16 on 4 CPUs** (within 4%, run to run noise):
  the process and the database share the 4 cores and use 3.7 to 3.9 of them, so the pool is
  no longer the limit #105 saw once both sides are busy (this run's `Search` was in the slow
  mode #102 describes: 3.1 ms at 10 clients, so throughput is below #105's). 4 connections per
  CPU keeps the pool full on 4 CPUs and grows it with the CPUs that can drive more calls.
  The database itself may run on more cores than the instance; its own capacity is the next
  limit.
- **Memory per connection.** A pooled connection costs the instance about 0.1 MiB (the
  process RSS rose 0 to 2 MiB from 16 to 32 connections, clients included); a Postgres backend
  about **2.1 MiB of private memory** once it has run the mix (the database container's `anon`
  memory after the steps: 56.0 MiB with 16, 89.5 MiB with 32), besides `work_mem` while a
  query sorts.
- **`max_connections`.** Each instance opens up to its pool size; the database needs the sum
  over every instance that may run at once, a rolling deploy's surge included, plus its
  superuser slots and your tools: 3 replicas on 4 CPUs need 48, 3 on 16 CPUs 192. Postgres's
  default is 100. A replica that cannot open a connection waits up to 10 s for one, then the
  call fails as unavailable.

Ingest under API load, measured with `throughput` (release build, 2000 files, 4 workers, the
instance store on disk; `DOEWOOD_BENCH_READ_CLIENTS` clients each asking for a `ListRecords`
page of 100 every millisecond; 1 run each):

| store | API clients | readers / pool | files/s | worker time per file: store / record / graph | `ListRecords` mean |
|---|---|---|---|---|---|
| SQLite | 1 | 2 readers | 284 | 1.2 / 1.9 / 6.0 ms | 46 ms |
| SQLite | 1 | 4 readers | 285 | 1.2 / 1.8 / 6.0 ms | 40 ms |
| SQLite | 8 | 2 readers | 18 | 41 / 32 / 128 ms | 95 ms |
| SQLite | 8 | 4 readers | 31 | 18 / 16 / 65 ms | 92 ms |
| Postgres | 8 | pool 16, 4 readers | 21 | 6.4 / 25 / 143 ms | 99 ms |
| Postgres | 8 | pool 32, 4 readers | 20 | 6.8 / 25 / 150 ms | 98 ms |

- **No regression:** with a light API load ingest runs as before with 4 readers (285 against
  284 files/s), and the writer's lock wait is unchanged (0.48 against 0.46 ms mean).
- **Under a heavy API load ingest slows a lot, on both stores**, and more readers help: on
  SQLite ingest's own reads (the graph, search candidates, records, settings) share the
  read-only connections with the API, so they queue behind the API's pages; with 4 readers
  they wait half as long and ingest runs 1.7 times as fast. On Postgres the bigger pool does
  not help: the database (172 s executing for 2000 files, 165 s of it the API's `ListRecords` pages) and the
  4 cores are the limit. With 32 such clients and 2 readers, 2000 files did not finish in
  10 minutes. 8 clients asking for 100 records every millisecond is far more than
  the API usually sees; #109 (below) gives ingest's reads priority.

### One store call for `Neighbours` and `CompanyDocuments` (issue #124)

Both calls read the node and then ran their query: two store calls, so two waits for a
reader (SQLite) or a pooled connection (Postgres). Now the query runs first and the node is
read only when it finds nothing, to tell a missing node (`NOT_FOUND`) or, for
`CompanyDocuments`, a non-company (`INVALID_ARGUMENT`) from an empty list. Its documents come
from `party` edges, which always end at a company (ADR 0001 section 7), so a non-empty result
needs no kind check. A call that finds something makes 1 store call instead of 2; one that
finds nothing still makes 2. In the `api_load` mix (every seeded node has neighbours and
every company documents) that is 100 store calls per 100 API calls instead of 130.

Measured with `api_load` (release build, 10 s per step, 10,000 documents,
`DOEWOOD_BENCH_CLIENTS=10,100 DOEWOOD_BENCH_IDLE=0`, SQLite with 4 readers and Postgres 17 on
an `--internal` Docker network; `main` at `baf53ad` against this change, the two binaries
run alternately; ranges over the runs):

| | runs | 10 clients: calls/s | p99 | 100 clients: calls/s | p99 |
|---|---|---|---|---|---|
| SQLite, `Neighbours` only, before | 2 | 6,794 to 7,029 | 3.1 to 3.2 ms | 5,630 to 5,779 | 49 to 50 ms |
| SQLite, `Neighbours` only, after | 2 | 7,275 to 9,103 | 2.9 to 3.3 ms | 5,946 to 6,407 | 52 to 56 ms |
| SQLite, `CompanyDocuments` only, before | 2 | 7,252 to 7,785 | 3.0 to 3.1 ms | 7,325 to 7,533 | 43 to 44 ms |
| SQLite, `CompanyDocuments` only, after | 2 | 7,879 to 8,061 | 3.2 ms | 7,559 to 7,611 | 55 to 56 ms |
| SQLite, mix, before | 3 | 5,515 to 6,276 | 4.3 to 4.7 ms | 5,440 to 6,099 | 60 to 66 ms |
| SQLite, mix, after | 3 | 6,157 to 6,523 | 4.3 to 4.5 ms | 5,566 to 5,751 | 67 to 69 ms |
| Postgres, `Neighbours` only, before | 1 | 8,306 | 2.6 ms | 8,860 | 14.5 ms |
| Postgres, `Neighbours` only, after | 1 | 9,964 | 2.4 ms | 10,435 | 13.0 ms |
| Postgres, `CompanyDocuments` only, before | 1 | 10,541 | 1.8 ms | 9,938 | 13.3 ms |
| Postgres, `CompanyDocuments` only, after | 1 | 12,926 | 1.7 ms | 12,839 | 10.5 ms |
| Postgres, mix, before | 2 | 4,933 to 5,128 | 6.6 to 6.9 ms | 5,449 to 5,487 | 39 to 42 ms |
| Postgres, mix, after | 2 | 5,194 to 5,340 | 6.5 to 6.8 ms | 5,514 to 5,664 | 30 to 32 ms |

In the mix the two calls themselves got faster on both stores: p50 1.3 to 1.8 ms before,
0.9 to 1.3 ms after at 10 clients; at 100 clients 19 to 21 ms before, 13 ms after on SQLite
and 22 to 23 ms before, 17.5 ms after on Postgres (p99 41 to 45 ms before, 24 to 27 ms
after).

- **Postgres gains most:** 18 to 29% more calls/s for the two calls alone, 4 to 5% for the
  mix at 10 clients, and the mix's p99 at 100 clients falls by a quarter; the pool's
  checkouts were the wait.
- **SQLite at 100 clients is unchanged** within the noise of these runs: the 4 readers are
  saturated either way, so the time the two calls save goes to the other calls' queue
  (`GetNode` p50 9 to 10 ms before, 12 to 12.5 ms after), and the p99 is a little higher. At
  10 clients the mix is 4 to 12% faster.

### Company rows rewritten by every document (issue #129)

Every party of every document patches its company's props (`GraphWriter::update_company`):
`names`, `quail_keys` and `ids` are capped sets that stop changing once a company is known,
but `matches` gets the document's entry, so the patch always changes the row and the
compare-and-set writes it. `company_rows` in `crates/doewood-ingest/tests/company_rows.rs`
(ignored) writes 10,000 documents of two parties each straight through `GraphWriter` (no
files, no analysis), 4 writers on one instance, release build: `hot`, where one supplier is
on every document and 20 buyers share the rest, against `spread`, where no company is on
more than two documents. SQLite in a file; Postgres 17.12 (`pgvector/pgvector:pg17`,
`pg_stat_statements` loaded) on an `--internal` Docker network on the same machine, which
other measurements shared; one run each, `main` at `e47c815`.

| | SQLite hot | SQLite spread | Postgres hot | Postgres spread |
|---|---|---|---|---|
| documents/s | 209 | 201 | 89 | 72 |
| graph stage per document | 16 to 21 ms | 16 to 21 ms | 42 to 45 ms | 47 to 86 ms |
| company lock held per document | 4.1 to 5.2 ms | 4.1 to 5.4 ms | 10.7 to 11.5 ms | 11.8 to 21.7 ms |
| busiest company's props (text / stored) | 3,720 B | 508 B | 3,720 / 1,818 B | 508 / 620 B |
| written per document | 313 KB (process) | 401 KB | 13.9 KB WAL | 14.6 KB WAL |
| `nodes` row versions per document (HOT) | | | 2.98 (92%) | 2.98 (80%) |
| `nodes` dead tuples at the end, autovacuums | | | 145, 2 | 1,338, 2 |
| props compare-and-set writes, retried | | | 30,000, 0 | 30,000, 0 |

(Ranges are over the tenths of the run; the lock and graph stage did not grow from the first
tenth to the last. The SQLite figure is `wchar` of the process: every write of the WAL and the
checkpoints, in pages.)

The company patch alone, 1,000 times on the busiest company (`matches` full at 20), with
the document's `matches` entry as now, and without it (the same patch for a known company,
which changes nothing and so writes nothing):

| | with `matches` | without |
|---|---|---|
| SQLite | 0.23 ms, 4.7 KB written | 0.14 ms, 0 |
| Postgres | 1.56 ms, 2.0 KB WAL | 0.30 ms, 0 |

Two instances on one Postgres (2 writers each, each instance its own company lock), `hot`:
241 documents/s, and 585 of the 20,000 company patches (2.9%) found the row changed by the
other instance and were applied again (one more read and write each).

- **The row does not grow without bound.** `matches` is capped at 20 entries (and `names`
  and `quail_keys` at 20, `ids` at 10 per kind), so the busiest company's props stop at
  about 3.7 KB of JSON, 1.8 KB stored: inline, compressed, never TOASTed. The lock hold and
  the graph stage per document are flat over 10,000 documents.
- **A busy supplier costs no more than many small ones.** Each party is one row rewrite in
  both corpora (2 per document, plus the document's own props): about 2 KB of WAL each on
  Postgres, mostly HOT updates whose dead versions are pruned in the page. The spread corpus
  writes slightly more, because its companies are new rows.
- **The rewrite is still a real share.** On Postgres the two company writes are about 4 KB of
  the 13.9 KB of WAL per document (29%), 2 of the 3 `nodes` row versions, and about 2.5 ms of
  the 11 ms the company lock is held (the write is 1.3 ms of the patch's 1.6 ms). In this
  graph-only run the lock is busy all the time (89 documents/s x 11.2 ms), so that is the
  throughput a single instance loses. On SQLite it is about 3% of the bytes written and about
  4% of the lock hold. In a real ingest the graph stage is 40 to 58% of a worker's time (see
  [Ingest on Postgres](#ingest-on-postgres-issue-69)), so the gain there is smaller.
- **Since then the company keeps no `matches` list** (the owner's decision on issue #129):
  how each document matched is on its `party` edge only (see below).

#### Without `matches` (issue #129)

The match step, alias, confidence and conflicts of each document were already on its `party`
edge (`props.match`, the document in `evidence`), so the company's list was a copy that
nothing read. Ingest no longer writes it, and removes a list an earlier version stored the
next time it patches that company (a remove that changes nothing on a company without one).
No migration: companies that get no new document keep their old list until they do. While
older and newer instances run side by side, the older ones add the list back and the newer
ones remove it: the rows are written as often as before, nothing is lost.

Deriving the list on read instead (the last 20 live `party` edges of the company, for
`GetNode`) was measured too: 4.4 to 7.8 ms per read for the busiest company (10,000 edges)
with today's indexes, which find all its party edges and sort them, against about 0.3 ms
for reading the node. An index on `edges (dst, kind, id)` makes it 0.2 to 0.35 ms, but needs
a new shared schema (every instance upgraded together) and one more store call per
`GetNode`; `Neighbours` (in, `party`) already lists the same edges. So the list was dropped,
not derived ([api.md](api.md#records-graph-and-search)).

`company_rows`, `hot` corpus, the same machine and settings as above, `staging` at `c87a4f4`
before and this change after, back to back, one run each (lock and graph stage are means
over the run):

| | documents/s | graph stage per document | lock held per document | written per document | `nodes` row versions per document | props compare-and-set writes, retried |
|---|---|---|---|---|---|---|
| SQLite, before | 231 | 17.1 ms | 4.31 ms | 314 KB (process) | | |
| SQLite, after | 275 | 14.4 ms | 3.61 ms | 300 KB | | |
| Postgres, before | 94 | 41.7 ms | 10.60 ms | 13.3 KB WAL | 2.99 | 30,000, 0 |
| Postgres, after | 114 | 34.2 ms | 8.73 ms | 9.2 KB WAL | 0.99 | 10,021, 0 |
| Postgres, 2 instances, before | 226 | 17.0 ms | 8.79 ms | 13.2 KB WAL | 2.99 | 30,555, 555 |
| Postgres, 2 instances, after | 271 | 14.0 ms | 7.34 ms | 9.1 KB WAL | 0.98 | 10,021, 0 |

- **Each known company's patch now writes nothing**: 10,021 props writes for 10,000
  documents instead of 30,000 (the documents' own props, and the 21 companies' first
  ones), one `nodes` row version per document instead of three, and 30 to 31% less WAL. The
  busiest company's props are 188 bytes instead of 3.7 KB.
- **The company lock is held 16 to 18% less** (the patch is a read only, 0.3 ms instead of
  1.3 ms on Postgres), and in this graph-only run, where the lock is the limit, 19 to 21%
  more documents a second on both stores. A real ingest gains less (the graph stage is 40 to
  58% of a worker's time).
- **Two instances no longer retry** a busy company's patch: 555 retries before, 0 after.
- The `spread` corpus was not run again: its companies are mostly new, and their first patch
  writes as before.

### Ingest's reads before the API's (issue #109)

**Why a `ListRecords` page is slow under write load.** Not the query: `EXPLAIN QUERY PLAN`
on SQLite is a primary-key range scan of `records` with one `locations_by_blake3` lookup per
record, and the page's statement runs in 1 to 2 ms; on Postgres `EXPLAIN (ANALYZE, BUFFERS)`
is a merge semi join over the two indexes in 0.2 ms (201 buffers, all hits). The time is the
record bodies: `throughput`'s records are about 15 KiB of JSON (6.6 KiB compressed in
Postgres's TOAST), so a page of 100 carries 1.3 to 1.4 MiB. On SQLite that is copying,
checking as JSON, encoding and sending 1.4 MiB per page (10 ms with one client, 22 ms with 8
on 4 busy cores); `api_load`'s records are about 1 KiB, hence its 2.5 ms. On Postgres it is
mostly the database turning 1.9 MB of `jsonb` back into text per page (19 ms on an idle
server, `SELECT record::text` of the first 100): 22 to 26 ms of database time per page under
this load, 92 to 95% of everything the database executed; the instance then parses each body
into its canonical text. The WAL is not the cause (it stayed under 80 MiB). #111's byte budget (3 MiB a
page) bounds a page; it does not shorten these.

**What changed.**
- The read-only connections serve the instance's reads first. When reads wait for a reader,
  ingest's (and enrichment's, the scanners') get the next free one before the API's (its
  calls and the `WatchProgress` snapshot), so they no longer queue behind a backlog of pages.
  The API is not starved: while none of its reads holds a reader, the instance's reads leave
  it the last free one. API calls are told apart by a task-local the call limiter sets, and a
  thread-local on the blocking threads that run their store work.
- Standalone `ListRecords` checks the bodies as JSON after giving its reader back, so the
  reader is held only for the SQLite statement (same rows and byte budget as before).

Measured with `throughput` and `api_load` as above (release build, 4 vCPU, `main` at
`0f62b6a` against this branch, the two test binaries run alternately, 2 runs each; Postgres
17 on an `--internal` Docker network):

| store | API clients | readers | files/s before / after | store / graph per file before / after | WAL at most before / after |
|---|---|---|---|---|---|
| SQLite | 8 | 2 | 74, 76 / 110, 110 | 12.1 / 23.3 ms, 5.7 / 15.4 ms | 59 to 79 / 7 to 9 MiB |
| SQLite | 8 | 4 | 102, 103 / 101, 103 | 6.5 / 16.8 ms, 5.8 / 17.0 ms | 35 to 66 / 6 to 9 MiB |
| SQLite | 1 | 4 | 339, 356 / 336, 344 | 1.4 / 4.9 ms, 1.4 / 5.0 ms | 4 / 4 MiB |
| Postgres | 8 | pool 16 | 64 / 63 (1 run each) | | |

| `api_load`, SQLite, 4 readers | 10 clients | 100 clients | 500 clients |
|---|---|---|---|
| before (calls/s) | 5,849 to 6,117 | 5,588 to 6,345 (mean 5,838, 5 runs) | 5,326 to 5,393 |
| after (calls/s) | 5,728 to 5,871 | 5,505 to 6,642 (mean 5,861, 5 runs) | 5,476 to 5,502 |

- **With 2 readers ingest under 8 heavy API clients is 1.45 times as fast** (6 times the
  18 files/s #108 measured, which predates #106's records as text), its store reads wait half
  as long, and the WAL no longer grows to tens of MiB: the readers are free often enough for
  checkpoints to complete.
- **With one reader per CPU (4) it is unchanged at about 100 files/s**: the readers are not
  the limit there, the 4 cores are (3.6 cores busy; the 8 clients run in the same process and
  decode 300 pages of 1.4 MiB a second). Holding the API to fewer readers would trade its
  throughput for ingest's; it is not done.
- **No regression:** with one API client ingest runs as before, and `api_load` is within its
  run to run noise. (A first version woke the waiting reads while still holding the pool's
  lock: 7% fewer calls/s at 100 clients. They are now woken after it is released.)
- **Postgres is unchanged** (the readers are SQLite's). Its limit is the database's time on
  the API's pages, the `jsonb` turned back into text. Since schema 8 records are stored as
  text and the same load runs at 110 to 114 files/s
  ([records as text](#records-as-text-on-postgres-issue-121)).

### Time to ready (issue #118)

How long a new instance takes from `exec` to `/readyz` answering 200, that is to serving the
gRPC API. Each start logs one `startup step` line per step (`step`, `took_ms`, `at_ms` since
the process started) and a `ready` line with `startup_ms` and the resident memory
`rss_mib`; the shared Postgres's `shared Postgres store ready` line adds `connect_ms` and
`migrate_ms`, and `analyzer ready` adds `ocr_ms` (checking the OCR models' SHA-256) and
`engine_ms` (building the quail engine).

Measured on the machine above with release builds of `36b041c` (after) and `e943d5a` (before:
`main` plus the timings), the OCR models present, in a network namespace with only loopback,
Postgres 17 in a container on an internal network (no TLS), nothing to ingest. Times in ms,
the median of 3 runs unless marked; the store file in the page cache unless marked. "50k" is
a store with 50,000 generated documents (records with 20 facts, search rows, sketches, nodes
and 3 party edges each, 5,000 companies; 630 MiB of SQLite):

| store | before | after | critical path after |
|---|---|---|---|
| SQLite, empty, restart | 145 | 138 | analyzer 126 (OCR check 57, engine 70) |
| SQLite, 50k, restart | 144 | 138 | analyzer 127; store open 5 |
| SQLite, 50k, statistics stale (1 run) | 12,896 | 109 | analyzer 102; statistics refreshed after ready (below) |
| SQLite, 50k, statistics stale, file not cached (1 run) | 18,610 | 497 | analyzer 453; statistics refreshed after ready (below) |
| Postgres, empty database, first start (1 run) | 145 | 481 | schema migration 424 (129 before: run to run noise on a busy machine) |
| Postgres, 50k, restart | 145 | 132 | analyzer 122; connect + migrations 20, in parallel |

- **What was cut.** `Store::open` ran `PRAGMA optimize = 0x10002`; with statistics missing or
  stale (after a crash, an OOM kill or a bulk load, before the close-time optimize ran) it
  analyzed whole tables: 13 s on 50k documents, 18 s when the file is not in the page cache
  (a pod on a new node). It now runs right after the instance is ready, in the cleanup task,
  sampled (`analysis_limit` 400) and one table at a time, with each table's pages read ahead
  (through `dbstat`) on a connection of its own, so the writer is held only for one table's
  `ANALYZE` over cached pages. The `instance store statistics refreshed` log line shows
  `tables`, `took_ms`, `longest_writer_hold_ms` and the `slowest` table. The analyzer and the
  shared Postgres's connect and migrations need nothing from the instance store and now start
  before it opens; the readers, the prepared statements (per pooled connection, on first
  use) and the OCR engines (on the first scanned page) were already lazy.

  The refresh on the 50k store with its statistics dropped, measured with
  `cargo test -p doewood-store --lib stale_statistics_refresh -- --ignored --nocapture`
  (`DOEWOOD_BENCH_UNCACHED=1` drops the file from the page cache first; 1 run each; the test
  also checks that the hot queries' plans match those of full statistics):

  | refresh | cached: total / writer held | not cached: total / writer held |
  |---|---|---|
  | full statistics, one step (before) | 564 / 564 ms | 17.0 s / 17.0 s |
  | `analysis_limit` 1000, one step | 100 / 100 ms | 13.3 s / 13.3 s |
  | `analysis_limit` 400, one step | 100 / 100 ms | 16.9 s / 16.9 s |
  | `analysis_limit` 400, a table at a time, read ahead (now) | 265 / 50 ms | 17.9 s / 68 ms |

  Sampling cuts the work on cached pages, but not reading them: `ANALYZE` still reads every
  page of every index. Reading them ahead, without the writer, is what keeps writes moving
  (the slowest table, `sketch_bands`, takes 10 s to read uncached and 68 ms to analyze then).
  Every hot query (pending objects, sketch candidates, record by hash, company documents,
  neighbours, search by company and by date, object counts) keeps its plan with the sampled
  statistics.
- **Readiness is stricter.** Before, standalone instances reported ready as soon as the store
  was open, about 130 ms before the gRPC API served (and with Postgres, as soon as it
  connected). Ready now means the API serves and the backend answers.
- **What is left:** the analyzer, about 125 ms: the SHA-256 of the 47 MB of OCR models
  (30 to 60 ms) and building the quail engine (about 70 ms), both in quail. Without the OCR
  models only the engine remains. Postgres's first start on an empty database migrates the
  schema (0.1 to 0.4 s, once per database; other instances wait for it).
- `/healthz` answers about 15 ms after `exec` (100 ms with the binary not in the page cache).
- RSS at ready: 81 to 83 MiB in every case.

Probes, from these numbers: no `initialDelaySeconds` for either probe, and no startup probe;
an instance is ready well within a second, so the readiness period decides when a new pod
takes traffic (on average half a period after ready). The chart's readiness probe runs every
2 s (it ran every 5 s before) with 8 failures, about 16 s before a pod that lost Postgres is
taken out, as before. The liveness probe stays at every 10 s with 6 failures: the process
answers `/healthz` from the start, and a slow start never needs more.

### The instance store per file (issue #128)

Each file made three blocking calls to the instance store: its ingest status, its source
and the runtime settings, and the finish. The status was read on the writer, so it waited
behind the scanner's `record_objects` and the other workers' `finish_object` commits. Now
each attempt reads the status, the source and the settings in one read transaction on a
read-only connection (an instance read, so it keeps its priority over the API's, #109); the
settings are still read per file, and the finish stays on the writer. The other read-only
`objects_seen` reads (`seen_objects`, `seen_object`, `live_object_count`,
`pending_objects`, `ingested_objects`) moved to the readers too. None needs to see its own
uncommitted writes: every write commits before it returns, and `inflight` holds a key until
its attempt is recorded, so the status a worker reads is the committed one.

Measured with `throughput` (release build, `DOEWOOD_BENCH_FILES=2000 DOEWOOD_BENCH_JOBS=4
DOEWOOD_BENCH_READERS=4`: the store on disk with 4 readers, one API client asking for a
`ListRecords` page of 100 every millisecond; `main` at `af345f7` against this change, the
two test binaries run alternately, 3 runs each):

| | files/s | store stage per file | writer lock wait mean / p99 | `ListRecords` mean |
|---|---|---|---|---|
| before | 341, 338, 341 | 1.42, 1.45, 1.46 ms (12%) | 646 to 668 us / 3.8 to 4.2 ms | 10.4 to 10.6 ms |
| after | 354, 354, 345 | 1.00, 0.98, 1.00 ms (9%) | 685 to 710 us / 3.8 to 4.2 ms | 10.3 to 10.7 ms |

- **4% more files/s** (median 354 against 341), and the store stage takes 30% less per file.
- **The writer's lock wait is unchanged** within noise (a trivial writer call every
  millisecond; slightly higher mean, as more files a second commit more finishes). The
  writer now serves only writes on this path.

### 50,000 to 200,000 documents (issue #127)

`scale` in `crates/doewood-ingest/tests/pipeline.rs` (ignored; its header lists the phases
and knobs) grows one store in runs of a few minutes: `throughput`'s mix of PDF, DOCX, TXT and
XLSX, except that every 20th file is a near-duplicate of one of 50 template texts and only
every 16th file names the one big buyer (the other contracts name a buyer of their own). At
200,000 files that is 39,372 companies, one with 12,504 documents, and 997,834 edges
(about 0.75 million of them `similar_to` between near-duplicates). The instance store is a
file with 4 readers, as `serve` opens it; the shared store is that file (SQLite) or Postgres
17.12 (`pgvector/pgvector:pg17`, default settings, `pg_stat_statements`) on an `--internal`
Docker network; ingest calls nothing outside the process. Release build of `main` at
`e47c815` plus the measurement code, the 4 vCPU server above, one run per row (each
ingest row is the 5,000 files ending at that size). The 10k column is the same run (ingest)
or the same session (`api_load`).

```
DOEWOOD_SCALE_DIR=<dir> DOEWOOD_SCALE_TO=50000 cargo test --release -p doewood-ingest \
  --test pipeline -- --ignored --nocapture --exact scale          # repeat until it has them
DOEWOOD_SCALE_DIR=<dir> DOEWOOD_SCALE_PHASE=analyze|sweep|scan|due|api ...   # then measure
DOEWOOD_SCALE_DIR=<dir> cargo test -p doewood-store --lib scale_store_sizes -- --ignored --nocapture
```

| | SQLite 10k | 50k | 200k | Postgres 10k | 50k | 200k |
|---|---|---|---|---|---|---|
| ingest, files/s (4 workers) | 187 | 144 | 101 | 379 | 329 | 256 |
| same, without the near-duplicates (at 200k) | | | 169 | | | 385 |
| graph and search per file | 9.7 ms | 15.2 ms | 24.0 ms | 5.6 ms | 7.6 ms | 10.9 ms |
| record per file | 4.4 ms | 4.8 ms | 6.8 ms | 1.9 ms | 1.7 ms | 1.7 ms |
| store size (b-trees / schema) | | 1,206 MiB | 5,085 MiB | | 812 MiB | 3,440 MiB |
| per document | | 24.7 KiB | 26.0 KiB | | 16.6 KiB | 17.6 KiB |
| instance store writer: lock wait during ingest, mean / p99 | 2.1 / 33 ms | 2.2 / 35 ms | 2.5 / 35 ms | 0.03 / 0.2 ms | 0.03 / 0.2 ms | 0.03 / 0.2 ms |
| SQLite WAL during ingest: at most / resets per 1,000 files | 4 MiB / 83 | 5 MiB / 93 | 6 MiB / 113 | | | |
| Postgres WAL per document (the 25,000 files up to that size) | | | | 26 KB (25k) | 30 KB | 109 KB |
| ingest sweep, nothing pending / `object_counts` | | 0.03 / 0.007 ms | 0.01 / 0.003 ms | | 0.03 / 0.007 ms | 0.02 / 0.004 ms |
| unchanged scan pass over every key (before #138; after: 0.10 to 0.16 s and 0.37 to 0.67 s) | | (see below) | 7.6 to 8.3 s | | 0.46 to 0.50 s | 6.7 to 6.9 s |
| writer lock wait during the scan, mean | | | 18 ms | | 3.6 ms | 16 ms |
| `companies_due`, first page of 100 (SQLite before #139) | | 14 ms (51) | 55 ms (235) | | 10 ms | 86 ms |
| an enrichment pass of `companies_due` pages (registries off; SQLite before #139) | | 0.12 s (2.5 s), 94 pages | 0.35 to 0.4 s (41.6 s), 375 pages | | 0.28 s | 1.1 s |
| `ListRecords`, one client (page of 100) | | 12.8 ms | 9.7 ms | | 66 ms | 69 ms |
| `Search`, a word in a quarter of the documents | | 17 ms | 71 ms | | 10 ms | 112 ms |
| `Search`, the big company's name | | 8.8 ms | 34 ms | | 6.1 ms | 20 ms |
| `GetNode` / `Neighbours` (`similar_to`) | | 0.36 / 1.1 ms | 0.34 / 2.1 ms | | 0.37 / 1.4 ms | 0.59 / 2.7 ms |
| `CompanyDocuments`, the big company | | 29 ms (3,129) | 150 ms (12,504) | | 24 ms (3,129) | 153 ms (12,504) |
| `api_load` 10 clients: calls/s, p99 | 5,898, 4.6 ms | 3,140, 10.5 ms | 508, 72 ms | 5,060, 7.2 ms | 4,606, 7.6 ms | 3,525, 9.5 ms |
| `api_load` 100 clients: calls/s, p99 | 5,645, 68 ms | 2,955, 129 ms | 472, 714 ms | 5,277, 34 ms | 5,017, 33 ms | 3,196, 53 ms |
| `api_load` 10 clients, `Search` p50 / `GetNode` p50 | 2.0 / 0.7 ms | 5.0 / 1.1 ms | 38.7 / 4.1 ms | 1.6 / 0.8 ms | 2.0 / 0.8 ms | 3.7 / 0.8 ms |
| `serve` on the store (210k): RSS at ready / after its first pass / after 3 min idle | | | 80 / 129 / 117 MiB | | | 80 / 103 / 103 MiB |

The single-client API rows are medians of 11 calls through the API on the `scale` store
(`DOEWOOD_SCALE_PHASE=api`), on SQLite without planner statistics (ingest never refreshed
them; after `analyze`, as `serve` does once it is ready, the 210k store answered within 30%
of these, and `companies_due` before #139 was as slow); `api_load` seeds its own store (10, 50 or 200 thousand documents
of about 1 KiB, one company per 20, `DOEWOOD_BENCH_CLIENTS=10,100 DOEWOOD_BENCH_IDLE=0`).
`serve` ran the release binary on the 210k store with every registry, feed and the favicon
fallback switched off (`DOEWOOD_SCALE_PHASE=offline`), in a network namespace with only
loopback (SQLite) or the database container's (Postgres); its first enrichment pass marked
the 39,372 companies enriched with nothing found (90 s on SQLite, 55 s on Postgres). The
Postgres server itself used 213 MiB at 50k and 349 MiB at 200k (`docker stats`, page cache
included).

Where the bytes are, per document (50k, then 200k):

| | SQLite | Postgres |
|---|---|---|
| records (Postgres: TOAST included) | 18.6 / 18.6 KiB | 8.7 / 8.7 KiB |
| `sketch_bands` (17.6 bands a document) and `sketches` | 2.2 / 2.2 KiB | 3.4 / 3.4 KiB |
| `edges` | 1.3 / 2.7 KiB | 1.1 / 2.1 KiB |
| `nodes` and `node_aliases` | 1.3 / 1.3 KiB | 1.5 / 1.5 KiB |
| search (`search_index` and the FTS tables / the GIN index) | 0.8 / 0.8 KiB | 1.4 / 1.4 KiB |
| `locations` and `objects_seen` | 0.5 / 0.5 KiB | 0.5 / 0.5 KiB (`objects_seen` is in the instance store) |

What the numbers say:

- **Store size is linear** except `edges`: the near-duplicates link to every earlier copy of
  their template until `similar_to`'s candidate cap (100), so their edges grow with the
  square of a template's copies up to the cap (from 49 to 100 a near-duplicate here). Plan
  about 26 KiB a small document on SQLite and 18 KiB on Postgres, plus the disk the WAL needs.
- **Ingest slows as the store grows, mostly because of the near-duplicates.** Without them
  Postgres ingests at 200k as fast as at the start (385 against 390 files/s), SQLite at 169
  against 228. Each near-duplicate compares itself with and writes an edge to each of
  up to 100 candidates; before #126 that cost 2 round trips per candidate. With #126 (merged
  during this measurement) Postgres at 210k ran 328 files/s with them and 401 without (graph
  stage 7.0 against 5.2 ms a file), against 248 and 385 before. The cost is bounded by the
  candidate cap, so it stops growing once a template has 100 copies (about 100k files
  here). SQLite's record and graph stages also grow by about 40% from 5k to 200k without
  them (deeper b-trees, a 5 GiB file over a 2 MiB page cache per connection).
- **The ingest sweep and the progress counts do not grow**: the sweep reads only pending rows
  and `object_counts` is kept by triggers.
- **An unchanged scan pass grew with the square of the keys** (0.5 s at 50k, 7 to 8 s at
  200k, CPU-bound): each chunk's `seen_objects` scanned the source's keys from the first one.
  (The SQLite 50k pass, 1.0 to 1.5 s, was measured with a test bucket that was itself
  quadratic; the Postgres run's 50k pass uses the same instance store code.) Fixed in #138:
  the pass is now linear, see [below](#key-ranges-of-objects_seen-issue-138).
- **SQLite's `companies_due` sorted every company for each page**, so an enrichment pass was
  quadratic in the companies (41.6 s for 37,497); Postgres stays linear (1.1 s). Since #139
  it reads the companies through `nodes_companies` from the page token: a pass is linear,
  0.12 s at 50k and 0.35 to 0.4 s at 200k (the first page, 55 ms, holds the big company's
  12,504 documents). With the store's pages out of the OS page cache (other jobs on the
  server) the 200k pass took 4 to 8.5 s, nearly all of it disk reads (about 1 s of CPU);
  the query before took 39 to 42 s either way. Measured on a store grown again to 200k
  with the #127 code (`b1718f6`) plus the change, the old and new query on the same store
  (docs/enrichment.md).
- **SQLite `Search` slows faster than its matches grow** under load: `api_load` at 200k
  serves 508 calls/s, a tenth of 10k, and every call waits behind the searches. One search
  costs its matches on both stores (every match is ranked before the page is taken); on
  Postgres the common-word search went from 10 to 112 ms when `search_index` (206 MiB)
  outgrew the default 128 MB of shared buffers. Issue #140: see
  [Search on a big corpus](#search-on-a-big-corpus-issue-140).
- **Postgres's WAL per document grows** from 26 KB to 109 KB (full-page images after each
  checkpoint over indexes keyed by hashes and random ids; half the checkpoints were
  requested by `max_wal_size` 1 GB). Issue #141: see
  [Postgres WAL on a big corpus](#postgres-wal-on-a-big-corpus-issue-141).
- **`CompanyDocuments` returns every document of the company** (12,504 nodes, 150 ms on
  either store): linear, but unbounded. Since #112 it pages (50 by default, at most 1000):
  the first page of a 12,000-document company takes 0.4 ms on SQLite and 1.2 ms on Postgres
  (docs/api.md, [Pages of Neighbours and CompanyDocuments](api.md#pages-of-neighbours-and-companydocuments)).
- **`ListRecords`, `GetNode` and `Neighbours` do not grow** beyond their result size; Postgres
  `ListRecords` is its `jsonb` round trip (#121).
- **Memory does not follow the store**: an idle instance on the 210k store holds 100 to 120
  MiB (SQLite reads through its 2 MiB page cache per connection and the OS page cache; the
  writer and readers do not map the file), about 20 to 40 MiB more than at ready, after one
  enrichment pass over every company.
- **SQLite's WAL stays small** (4 to 6 MiB at most, a checkpoint resetting it about every
  10 files) at every size; the writer's lock wait is flat at about 2 ms mean and 35 ms p99.

### Postgres WAL on a big corpus (issue #141)

The `scale` corpus of [#127](#50000-to-200000-documents-issue-127) on Postgres 17.12
(`pgvector/pgvector:pg17`, `--internal` Docker network, `pg_stat_statements`), release
build, the 4 vCPU server above. The scale run prints `pg_stat_wal` and the checkpoints per
5,000 files; the WAL was split by relation with `pg_walinspect`
(`pg_get_wal_block_info` over the run's LSN range). `staging` at `01e09a9` (after #154)
against this change; the 175k store was copied once and every 175k row below starts from
that copy (25k rows: from one copy at 25k). Measured before #112 rebuilt `edges_by_dst` as
(dst, kind, valid_from, id): still keyed by the edge's end, so it takes its entries on the
same random pages, with somewhat larger entries.

Where the WAL of one document goes, default settings (`max_wal_size` 1 GB,
`wal_compression` off), before this change:

| | 25k to 50k | 175k to 200k |
|---|---|---|
| WAL per document | 38.4 KB | 147.0 KB |
| of it full-page images | 31% | 76% |
| checkpoints (all requested by `max_wal_size`) | 2 | 6 |
| `sketch_bands` and its two indexes | 294 MB (30%) | 1,399 MB (38%) |
| `sketch_band_sizes` and its index | 133 MB (14%) | 538 MB (15%) |
| `nodes` and `nodes_kind_key` | 87 MB | 486 MB |
| `edges` and its indexes | 86 MB | about 570 MB |
| the record (TOAST) | 159 MB | 159 MB |
| `search_index` and `search_tsv` | 77 MB | 167 MB |

- **The growth is full-page images.** After a checkpoint the first change to a page writes
  the whole page. A document's 32 LSH bands go to 32 random leaves of `sketch_bands_pkey`
  (band, hash), and at 200k that index has 37,600 pages: every checkpoint rewrites most of
  them (200,000 images, 1.16 GB, in 25,000 documents). The same holds, smaller, for the
  other indexes on hashes and random ids (`edges_identity`, `edges_by_src`/`_by_dst` for the
  `similar_to` edges to older documents, `nodes_kind_key`, `records_pkey`,
  `search_index_pkey`, `locations_by_blake3`, the GIN `search_tsv`) and for `nodes`, whose
  rows each new edge's foreign key locks (`FOR KEY SHARE` writes to the row's page). The
  more WAL, the more checkpoints, and the more images again.
- **Without images a document is about 25 KB** at every size (between two checkpoints:
  26.6 KB before, 24.6 KB after this change), a third of it the record itself.
- **What doewood changed (schema 11):** `sketch_band_sizes` (issue #148) had a row for every
  band key, and 92% to 99.9% of the keys belong to one document: each new document with a
  sketch inserted about 29 rows and index entries on random pages, 15% of the WAL. Since
  schema 11 a bucket gets its row when its second document arrives, with the exact count
  (the triggers take a transaction advisory lock per band, in order, so documents adding the
  same new band at once are counted exactly; a statement of more than 64 bands, such as a
  delete of many records, takes one lock for all bands instead); a bucket of one has none, which the candidate
  query reads as small (`similar_max_bucket` is at least 10). The bands, the edges and every
  row the API reads are the same. The rows of single documents an older schema wrote stay,
  with their exact count (at 200k: 2.0 million rows, 220 MB; a store started on schema 11
  does not make them).
- **What doewood did not change:** the random-keyed indexes are what the lookups need
  (candidates by band, documents by hash), and the stored data stays the same; no no-op
  rewrite was left on the ingest path (`pg_stat_user_tables`: about 0.4 `node_aliases` and
  1.2 `nodes` row versions per document, over 90% HOT).

Before and after, default settings:

| | before | after |
|---|---|---|
| 25k to 50k: WAL per document / checkpoints / files/s | 38.4 KB / 2 / 371 | 33.7 KB / 2 / 387 |
| 175k to 200k: WAL per document / checkpoints / files/s | 147.0 KB / 6 / 329 | 104.8 KB / 4 / 340 |
| WAL records per document | 200 to 227 | 157 to 179 |

(The 175k row is the before store upgraded to schema 11: its old size rows stay. Grown with
schema 11 from 125k on, the same window wrote 106.3 KB a document.)

**Server settings for a big corpus.** Most of what is left is full-page images, so the
server's checkpoint settings decide the WAL. With this change, from the same 175k store,
50,000 documents (and the first checkpoint cycle after it for the last row):

| server settings | WAL per document | full-page images per document | checkpoints | files/s |
|---|---|---|---|---|
| defaults (`max_wal_size` 1 GB, `wal_compression` off) | 115.7 KB | 14.6 | 10 requested | 345 |
| `wal_compression = lz4` | 71.4 KB | 10.2 | 5 requested, 1 timed | 346 |
| `lz4`, `max_wal_size = 4GB`, `checkpoint_timeout = 15min` | 35.0 KB (one cycle, 230k to 295k) | 1.5 | 1 timed, 1 requested in 125,000 files | 350 |

- **Set `wal_compression = lz4`** (Postgres 15 and newer; `pglz` where the server has no
  lz4): it compresses the images, not the data, and cost no ingest speed here. 38% less WAL.
- **Give checkpoints room: `max_wal_size` 4 GB or more and `checkpoint_timeout` 15 min or
  more** for a store of 100,000 documents and up. A checkpoint rewrites the hot index pages
  once whatever happened between, so its cost per document falls with the documents between
  two checkpoints: with 4 GB and lz4 a checkpoint came every 65,000 documents (2.3 GB of
  WAL) and the WAL was 35 KB a document, less than a third of the default's. The disk must hold
  `max_wal_size` of WAL (plus `wal_keep_size` and what replication slots hold), and a crash
  recovery replays up to that much (tens of seconds per GB). With replicas or WAL archiving,
  less WAL is less to ship and store.
- `checkpoint_completion_target` (default 0.9) can stay. `full_page_writes` must stay on
  (it protects against torn pages).
- Nothing in doewood reads these settings: they are the database's, set in
  `postgresql.conf` or with `ALTER SYSTEM` by whoever runs the server.

### Search on a big corpus (issue #140)

Issue #127 found SQLite's `Search` under `api_load` 7.7 times slower at 200,000 documents than
at 50,000, and the other calls queueing behind it on the 4 readers; and on Postgres a
common-word search went from 10 to 112 ms once `search_index` outgrew the 128 MB of shared
buffers. Three causes, measured with `api_load` (as in #127: release build, 10 s a step,
`DOEWOOD_BENCH_CLIENTS=10,100 DOEWOOD_BENCH_IDLE=0`, one company per 20 documents) and
`text_search_timings` (one search at a time on one reader of a store on disk, seeded by SQL
with `api_load`'s documents, where every 4th also has `quarter` and up to 12 more words, so a
word in a quarter of the documents scores by length; medians and p99 of 3 runs of 40 queries,
5 for the common words), on the 4 vCPU server above; "before" is `main` at `b1718f6`,
"after" this change, Postgres 17.12
(`pgvector/pgvector:pg17`) on an `--internal` Docker network:

```
DOEWOOD_BENCH_DOCS=200000 cargo test --release -p doewood-store --lib text_search_timings \
  -- --ignored --nocapture
```

**1. `api_load`'s own seeding (SQLite).** Seeding 200,000 documents left a WAL of 1.3 million
frames (5.4 GB; 330,000 frames at 50,000) that was never reset, and as `api_load` then only
reads, no automatic checkpoint ever ended it: every page the readers read was looked up in
it. `api_load` now checkpoints the instance store after seeding (`Store::checkpoint`), as
`serve`'s next write would. That alone is most of the 7.7x:

| SQLite `api_load` | 10 clients: calls/s | `Search` p50 / p99 | `GetNode` p50 | 100 clients: calls/s | p99 |
|---|---|---|---|---|---|
| 50,000, before | 3,047 | 5.1 / 12.4 ms | 1.3 ms | 2,881 | 131 ms |
| 50,000, after | 3,875 | 4.0 / 9.6 ms | 1.0 ms | 3,604 | 105 ms |
| 200,000, before | 505 | 36.7 / 71.8 ms | 6.9 ms | 456 | 732 ms |
| 200,000, the new query only (no checkpoint) | 582 | 31.3 / 67.9 ms | 6.3 ms | 531 | 598 ms |
| 200,000, after | 1,753 | 9.5 / 23.9 ms | 2.1 ms | 1,811 | 200 ms |

From 50,000 to 200,000 documents `Search` now takes 2.4 times as long, less than its matches
grow (4 times).

**2. Every match was read and sorted (SQLite).** A search scored each match with `bm25`,
joined it to its `search_index` row and sorted all of them by score, date and hash before the
page was taken. A search on words alone now joins only the matches that can reach the page
(see [api.md](api.md#search-on-words-alone)); the results are the same, ties included.
One search at a time:

| SQLite, one search, p50 / p99 | 50,000: before | after | 200,000: before | after |
|---|---|---|---|---|
| a rare term (`api_load`; 200 / 800 matches) | 0.51 / 0.71 ms | 0.46 / 0.71 ms | 2.47 / 2.85 ms | 2.37 / 3.46 ms |
| a word and a rare term (`api_load`; 168 / 668) | 1.80 / 2.44 ms | 1.76 / 2.29 ms | 7.23 / 7.69 ms | 7.02 / 10.47 ms |
| a word in a quarter of the documents (12,500 / 50,000) | 14.7 / 22.8 ms | 8.4 / 8.9 ms | 61.6 / 74.3 ms | 36.0 / 43.5 ms |
| a word in 5 of 6 documents (41,667 / 166,667) | 29.4 / 33.4 ms | 27.0 / 42.1 ms | 113.5 / 117.0 ms | 111.0 / 118.5 ms |

The gain is the rows not read; where most matches tie on the score (`api_load`'s documents
are all the same length, and a word in most documents adds almost nothing to the score), the
ties are read as before. What stays is linear in the matches, and
cannot go without changing the ranking:

- `bm25` scores every match (its column sizes and positions) before the best can be known.
- `bm25`'s IDF counts every document containing each word, once per search, whatever the
  page: about 5 ms per search for a word in 5 of 6 of 200,000 documents. This is most of
  "a word and a rare term".

**3. Postgres ranks every match too.** `ts_rank` reads each match's `tsv` from its heap row,
so a word in many documents reads most of `search_index`: a word in a quarter of the rows or
more is a parallel sequential scan of the whole table (28,572 blocks at 200,000
`api_load` documents: 223 MB of heap, 44 MB of indexes, 36 MB of them `search_tsv`); a rare
term is a bitmap scan of its own rows. The page, ties included, needs every match ranked, so
Postgres runs as before. `api_load` on Postgres (default settings, 128 MB of shared buffers):

| Postgres `api_load` | 10 clients: calls/s | `Search` p50 / p99 | 100 clients: calls/s | p99 |
|---|---|---|---|---|
| 50,000 | 4,630 | 2.0 / 4.6 ms | 4,833 | 34 ms |
| 200,000 | 3,537 | 3.7 / 9.3 ms | 3,318 | 52 ms |

One search at a time on the same documents in a table of their own (`EXPLAIN (ANALYZE,
BUFFERS)` of `PgStore::search`'s query, after a restart, 2 to 3 sessions of 3 runs; first
run, then the others):

| Postgres, 200,000 documents | 128 MB shared buffers | 1 GB |
|---|---|---|
| a rare term (800 matches) | 8 to 12, then 0.7 to 1.3 ms (804 blocks, hits once read) | 2.7, then 0.6 to 1.4 ms |
| a word in a quarter of the documents | 441 to 500, then 32 to 49 ms (28,476 blocks read each time) | 112, then 53 to 66 ms (all hits) |
| a word in 5 of 6 documents | 35 to 42 ms (28,188 blocks read each time) | 60 to 72 ms (all hits) |

Sizing:

- A sequential scan of a table bigger than a quarter of `shared_buffers` reads through a
  small ring of buffers, so with the default 128 MB a common-word search never caches
  `search_index` in shared buffers: it reads the whole table again each time (28,000 blocks
  here, #127's 25,000 at 200,000 documents). From the OS page cache that is cheap (32 to 49
  ms here, ranking 50,000 rows included); the first search after a restart took 441 to 500 ms, and
  #127's 112 ms on a store of 3.4 GB is in line with reads the page cache did not hold.
- So plan the database host's memory, shared buffers and page cache together, for at least
  the search table and its indexes:
  `SELECT pg_size_pretty(pg_table_size('search_index') + pg_indexes_size('search_index'))`
  (about 1.4 KiB a document with small documents, #127). A container memory limit counts
  the page cache.
- Bigger shared buffers are not a fix for common words: with 1 GB (more than four times the
  table) the scan found every block in shared buffers and was still slower here (53 to 72
  against 32 to 49 ms), as the cost is ranking the matches. Rare terms are as fast either way.

What would make a common-word search cost its page rather than its matches is a change to the
ranking, for the owner to decide (see the PR of #140): for example rank only a bounded set of
candidates (the newest N matches, or the first N by an index order) and say so in the API, or
keep per-word document counts so SQLite need not count them at every search.

### Key ranges of `objects_seen` (issue #138)

`seen_objects` (the scanner's comparison of a listing page with the stored rows),
`pending_objects` (the ingest sweep) and `ingested_objects` (reprocessing) wrote an optional
bound as `(?2 IS NULL OR key > ?2)`. That form kept SQLite from using the bound as a range on
the primary key (or the queue index `objects_seen_pending`): every page read the source's
keys from the first one and filtered them, so walking a source page by page was quadratic in
its keys. Each query now has one SQL text per combination of bounds, and its plan starts at
the bound (`SEARCH objects_seen USING PRIMARY KEY (source_id=? AND key>? AND key<?)`; the
sweep `USING INDEX objects_seen_pending (source_id=? AND status=? AND key>?)`). Tests check
the plans and compare the pages with the former queries. The Postgres store has no
`objects_seen` (the instance store is SQLite with either shared store), so nothing changes
there.

Measured with `scale`'s `scan` phase (release build, the 4 vCPU server above, a `keys.tsv`
of `bench/` and `near/` keys and an instance store holding only those rows; the second pass
of each run is the unchanged one) and with `keyset_pass` in
`crates/doewood-store/src/store/objects.rs` (ignored; the store reads alone, old and new
query in the same run):

| | 50,000 keys | 200,000 keys |
|---|---|---|
| unchanged scan pass (`scale`), before | 0.41 s (CPU 0.43 s) | 6.57 s (CPU 6.86 s) |
| same, after | 0.10 to 0.16 s (CPU 0.10 to 0.12 s) | 0.37 to 0.67 s (CPU 0.34 to 0.46 s) |
| `seen_objects` pages of a pass (`keyset_pass`), old / new | 117 to 119 / 16 to 23 ms | 1,899 to 2,039 / 60 to 62 ms |
| `pending_objects` pages over a full queue, old / new | 502 to 503 / 31 to 32 ms | 8,789 to 8,838 / 128 to 132 ms |

- **The pass is linear now**: four times the keys cost about four times the time, where
  they cost 16 times before. (The first pass of a run is the slower figure: a cold page
  cache.)
- **The sweep over a long queue** (every key pending, as after a first scan or a
  reprocess) gained the most: the old form also read the queue index from its start for
  every page.

### The instance store's WAL (issue #151)

PR #150 found a WAL of 1.3 million frames (5.4 GB) after `api_load` seeded 200,000 documents,
and every read 7 times as slow. Measured on this branch's parent (`staging` at `c87a4f4`, release
build, 4 vCPU, 4 readers), with the WAL's size and frames read every 5 s from the file and its
`-shm` index (`throughput` and `api_load` print them now):

| | WAL at most | WAL at the end |
|---|---|---|
| `api_load` seeding 50,000 documents, before | 1,295 MiB, 329,610 frames | the same (never checkpointed) |
| same, after | 5.7 MiB, about 1,000 frames | 553 frames, all in the main file |
| `throughput`, 8,000 files, 16 `ListRecords` clients, then 60 s idle with the clients on, before | 21 MiB (reset 378 times) | 21.2 MiB, 707 frames, none in the main file |
| same, after | 22 MiB (reset 434 times) | 0 (emptied 5 to 10 s after the last write) |

- **Production ingest does not starve the checkpoint**, even with 16 API clients reading
  without a pause: SQLite's own checkpoint resets the WAL every few hundred frames, and the WAL
  stays near 20 MiB. Ingest's speed and the writer's lock wait are unchanged (46 and 43 files/s,
  p99 32 and 35 ms; one run each). No read holds a transaction open: each read call is one
  transaction inside a blocking closure, and the connection guard cannot be held across an
  `await`.
- **The 5.4 GB was not the readers**: it was the seed's last phase, one `similar_to` edge
  upsert at a time. SQLite runs its automatic checkpoint only when `sqlite3_step` reports a
  statement done, and a write read with `query_row` (its `RETURNING id`) is reset after its row
  instead, so 50,000 such writes in a row never checkpointed. Node and edge upserts now step
  to the end (`returning_writes_let_sqlite_checkpoint`).
- **Idle, the WAL was never emptied**: without a write, SQLite does not checkpoint, and the
  last frames stayed in the WAL. The checkpoint thread now empties it.
- **The thread's cost to writes**: its PASSIVE step takes no writer lock; a TRUNCATE holds the
  writer's lock for its wait (50 ms at most; the test's busy try took 50.3 ms) plus the copy of
  what was left (0.2 to 0.3 ms in the tests).

### Requests and limits

Per doewood instance (one pod):

| workload | CPU request | memory request | memory limit |
|---|---|---|---|
| typical: text documents, a few scans | 500m | 512 MiB | 2 GiB |
| OCR-heavy: mostly scans | 2 to 4 (each OCR engine uses up to 4 threads) | 1.5 GiB | 4 GiB |

- The chart's defaults (500m, 1638 MiB, limit 4710 MiB, `ocrJobs: 2`) fit both: the request
  covers two OCR engines with one worker, the limit covers 4 workers with OCR plus a large
  spreadsheet in flight. With `ocrJobs: ""` (one engine) 1 GiB / 4 GiB is enough. With a lower limit the default worker count drops (half the room under
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
- The API's reads are small next to ingest: a `ListRecords` page holds about twice its 3 MiB
  byte budget at its peak, whatever the page size (it held the whole page before issue #111,
  up to the 64 MiB message cap and more), and while the shared Postgres is not connected the
  in-memory registry and icon caches hold at most 16 MiB each (see
  [api.md](api.md#listrecords-page-budget)).
- Postgres for a few instances: 256 MiB to 512 MiB of memory is enough for ingest; size
  `shared_buffers` and the disk for the records (about 40 KiB a scanned page).
