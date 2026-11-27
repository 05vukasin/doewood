# ADR 0001: doewood architecture

Status: accepted, 2026-11-23

## Context

[quail](https://github.com/05vukasin/quail) reads business documents (PDF, DOCX, text) and writes one
record of facts per file. [hedgehog](https://github.com/05vukasin/hedgehog) does the same for
spreadsheets. doewood is the service around them: it watches S3 storage, analyzes every file, links
documents and companies in a shared graph, and serves it all to other applications.

Requirements: written in Rust, fast and light; runs on any infrastructure (Kubernetes first); many
instances work at the same time; each instance connects to a list of S3-compatible stores; AI
models are reached through APIs; and the service builds and starts with no environment at all.

## Decisions

### 1. One binary, independent instances

- doewood is a single binary in a single image. The image holds quail (with OCR), hedgehog, the
  gRPC server, the S3 client and the Postgres client.
- Each instance is **independent**: it owns a SQLite file holding its configuration, its encrypted
  secrets, its sources, its jobs and its progress. On Kubernetes it runs as a `StatefulSet`
  member with its own small volume.
- To scale, add instances and give each its own sources. There is no shared work queue.
- Optional: Litestream replicates the SQLite file to S3.

### 2. Shared store: Postgres + pgvector

All instances share **one** Postgres database. It holds:

- **records:** each quail/hedgehog record as JSON, keyed by the file's BLAKE3 (JSONB until
  shared schema 8; since then its compressed canonical JSON text: see
  [ADR 0003](0003-records-as-canonical-text.md));
- **the graph:** typed `node` and `edge` tables (see 7), behind a `GraphStore` trait so that a
  Neo4j backend can be added later if deep traversals ever matter;
- **source claims:** a unique `(endpoint, bucket, prefix)` row owned by an instance id. A second
  instance adding the same source is refused, with the owner's name in the error;
- **search:** full text plus filters in v1. The vector column exists from v1, but embeddings
  arrive in v2.

When no Postgres is configured, an instance runs **standalone**: records, graph and search live in
its SQLite file, behind the same traits.

### 3. Configuration: one source

Each setting has exactly **one** source.

- **Runtime settings** come from the gRPC API (and later the frontend), stored in SQLite and
  encrypted with the master key (XChaCha20-Poly1305): S3 endpoints and credentials, AI provider
  keys and models, connectors, the LLM policy and source schedules. Changing them needs no
  restart. Secrets are **write-only**: the API never returns them.
- **Bootstrap values** come from env, and every one has a default:

  | variable | default |
  |---|---|
  | `DOEWOOD_DATA_DIR` | `./doewood-data` |
  | `DOEWOOD_LISTEN` | `127.0.0.1:7443` |
  | `DOEWOOD_POSTGRES_URL` | none: standalone |
  | `DOEWOOD_MASTER_KEY` | generated into `<data>/master.key` (mode 0600), with a loud warning that this is for development only |
  | `DOEWOOD_INSTANCE_ID` | the hostname, which is the StatefulSet pod name |

- A missing or invalid optional value never fails the build or the start. The service starts with
  what it has and reports what is not configured.

### 4. API: gRPC

- `tonic`, plus gRPC-Web (`tonic-web`) so that a browser frontend can call it directly.
- Services: `Config` (sources, AI providers, policy), `Jobs` (start, pause, status stream),
  `Records`, `Graph`, `Search`, and `Summarize` (on demand; stores nothing).
- **No authentication in v1.** Kubernetes network isolation protects it. The defaults stay
  safe: it binds to localhost, secrets are write-only, and every request passes through an
  (empty) auth interceptor so that tokens or OIDC can be added without changing the API.
- Ops endpoints for Kubernetes: `/healthz`, `/readyz` and `/metrics` (Prometheus). Logs are
  JSON. On SIGTERM the instance finishes the files in flight, saves its progress and exits.

### 5. Sources and jobs

> Amended by [ADR 0004](0004-push-sources.md): a source is also a **push** source, whose bytes an
> application hands in through `IngestService.PutObject`. It has no scanner and no bucket; the
> application owns discovery, moves and deletions. What follows describes S3 sources.

- A source is an S3-compatible endpoint plus a bucket and an optional prefix. Every
  S3-compatible store works: AWS, MinIO, Ceph, R2, Wasabi.
- **Discovery:**
  - a periodic incremental `ListObjectsV2` scan, comparing ETag and size with what is stored,
    with an interval per source;
  - optional event inlets (a MinIO/S3 webhook, or an SQS poller) to pick files up within seconds.
    The scan catches anything the events miss.
- **Jobs:** starting a source runs a full first pass, then keeps watching until it is paused or
  removed, and its progress streams over gRPC. `Reprocess` is a one-shot job that re-analyzes
  only the files whose quail/hedgehog step versions changed.
- **Connectors** (Coupa first, later) pull data from other systems into an S3 source; doewood then
  ingests it like any other file.

### 6. AI models: API only

- No model ships inside doewood. Every LLM is reached through API presets, with the same
  provider names and variables as hedgehog: `openai`, `openai_compatible`, `anthropic`,
  `bedrock`, `openrouter`, `groq`, `vllm`, `ollama`. A self-hosted or fine-tuned model is just
  another OpenAI-compatible URL.
- Summaries use quail's `summary.llm`: the digest, the fact check and the extractive fallback.
- **Separate queue:** ingest always stores the record first. LLM summaries then go through an
  in-instance queue, with a policy per source (`off`, `all`, `kinds:[…]`, `on_demand`), a
  concurrency cap and an optional daily budget.
- `Summarize` (on demand) skips the queue and stores nothing.

### 7. Graph v1

Every node and edge comes from quail facts; no LLM is involved.

- `Document` (BLAKE3) → `Location` (endpoint/bucket/key; a document can have many locations).
- `Company` nodes, merged first on strong identifiers (VAT, registry number, LEI, IBAN), then on
  the normalized name plus legal form. Each merge stores its evidence and confidence, so a person
  can undo it.
- `Document –[party {role, conf}]→ Company` (role: buyer, supplier, signatory, …).
- `Document –[amends | cites]→ Document`, from `ref.*`.
- `Document –[similar_to {score}]→ Document`, from the MinHash LSH bands and SimHash.

### 8. Engineering

These follow quail's rules:

- Rust 2024, clippy pedantic with `-D warnings`, `unsafe` denied, and CI on Linux, macOS, Windows
  and Linux ARM;
- tests build their inputs in code;
- no real documents in the repository.

Hedgehog's patched calamine has to be patched in again here, because `[patch]` does not carry
over to dependents.

## Consequences

- One component (Postgres) is shared and must be backed up; instances are cheap to add and
  replace.
- One large bucket cannot be split across instances automatically, unless it is split by prefix
  into several sources.
- Without auth, the API must never be exposed outside the cluster until v2 adds tokens or OIDC.
