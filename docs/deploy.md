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
[configuration.md](configuration.md#the-shared-postgres)).

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
them, which suits only reads of the shared store.

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
Prometheus uses them.

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

### Requests and limits

Per doewood instance (one pod):

| workload | CPU request | memory request | memory limit |
|---|---|---|---|
| typical: text documents, a few scans | 500m | 512 MiB | 2 GiB |
| OCR-heavy: mostly scans | 2 to 4 (each OCR engine uses up to 4 threads) | 1.5 GiB | 4 GiB |

- The chart's defaults (500m, 1 GiB, limit 4 GiB) fit both, so they are unchanged: the request
  covers the OCR engine with one worker, the limit covers 4 workers with OCR plus a large
  spreadsheet in flight. With a lower limit the default worker count drops (half the room under
  the limit, 512 MiB a worker).
- OCR throughput follows the CPU: with a 500m request an OCR-heavy pod gets only what the node
  has spare. Plan about 1500 scanned pages an hour per instance on 4 cores, and more instances
  (each with its own sources) rather than more workers to go faster. A second OCR engine needs a
  memory limit of at least 10 GiB.
- Postgres for a few instances: 256 MiB to 512 MiB of memory is enough for ingest; size
  `shared_buffers` and the disk for the records (about 40 KiB a scanned page).
