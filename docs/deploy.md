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
| `resources` | requests 500m CPU and 1 GiB, limit 4 GiB | OCR takes about 0.6 to 1 GB a page in flight; the ingest jobs follow the memory limit |
| `podSecurityContext`, `securityContext` | non-root 10001, `fsGroup` 10001, seccomp `RuntimeDefault`, read-only root, no privilege escalation, all capabilities dropped | security |
| `terminationGracePeriodSeconds` | `30` | shutdown time |
| `podAnnotations`, `podLabels`, `nodeSelector`, `tolerations`, `affinity` | empty | scheduling and metadata |

`/metrics` is on the ops port; add scrape annotations through `podAnnotations` if your
Prometheus uses them.
