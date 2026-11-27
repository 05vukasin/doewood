# doewood

A Rust service around [quail](https://github.com/05vukasin/quail) (PDF, DOCX, PPTX, HTML and
text) and [hedgehog](https://github.com/05vukasin/hedgehog) (spreadsheets). It watches S3-compatible
storage, extracts metadata from every file, links documents and companies in a shared graph, and
serves it all over gRPC.

The name comes from Košutnjak, a Belgrade forest: *košuta* (a doe) + wood. It joins the hedgehog
and the quail.

**Status:** the skeleton (configuration, local store, ops endpoints), the shared store, S3
discovery, the gRPC API, ingest (quail and hedgehog into records, the graph and search), the
LLM queue (per-source policy, concurrency cap, daily budget, on-demand `Summarize`; see
[docs/llm.md](docs/llm.md)), the container image with a Helm chart (see
[docs/deploy.md](docs/deploy.md)), the company identity rules, the six registry clients and the
favicon proxy (see [docs/registries.md](docs/registries.md)), and the enrichment job that runs
them next to ingest (registries, the decision gate, dated edges, websites and favicons; see
[docs/enrichment.md](docs/enrichment.md)) are in. See
[ADR 0002](docs/adr/0002-company-enrichment.md) (company identity and registries),
[ADR 0003](docs/adr/0003-records-as-canonical-text.md) (records as text) and
[ADR 0001](docs/adr/0001-architecture.md).

- **One binary.** It starts with no configuration at all; everything else is set at runtime
  through the API.
- **Independent instances**, each with its own SQLite store, all sharing one Postgres + pgvector
  database for records, relations and search.
- **Any S3-compatible store**, watched continuously.
- **AI models through APIs:** OpenAI, Anthropic, Bedrock, OpenRouter, Groq, vLLM or Ollama, using
  the same presets as hedgehog.

## Quickstart

```sh
cargo run -- serve
```

With no environment at all, this:

- creates `./doewood-data/` with the SQLite store and a development master key (and warns
  loudly that the key file is for development only);
- serves the gRPC API (with gRPC-Web, health and reflection) on `127.0.0.1:7443`, and
  `/healthz`, `/readyz` and `/metrics` on `127.0.0.1:7444`;
- runs standalone, without a shared Postgres;
- logs JSON to stderr (`RUST_LOG` sets the filter) and stops cleanly on Ctrl-C or SIGTERM.

```sh
curl -s 127.0.0.1:7444/readyz     # ready
cargo run -- config check         # effective configuration, secrets redacted
cargo run -- health               # exit 0 once /readyz answers 200 (container health checks)
cargo run -- version
```

```sh
grpcurl -plaintext 127.0.0.1:7443 list            # the services, through reflection
```

The bootstrap variables are listed in [docs/configuration.md](docs/configuration.md), sources
and discovery in [docs/sources.md](docs/sources.md), ingest in [docs/ingest.md](docs/ingest.md), the enrichment job in
[docs/enrichment.md](docs/enrichment.md),
the API, with `grpcurl` examples, in [docs/api.md](docs/api.md), company icons
(`GET /icons/{domain}`) in [docs/favicons.md](docs/favicons.md), and the image and the Helm
chart in [docs/deploy.md](docs/deploy.md).

## Layout

```
crates/
  doewood-core    bootstrap config, errors, ids, the master key type
  doewood-model   records, graph, claims, search, the enrichment caches and company freshness:
                  the store traits and their conformance suite
  doewood-store   the instance-local SQLite store: settings, encrypted secrets, sources, jobs,
                  and the standalone backend of the store traits (FTS5 search)
  doewood-pg      the shared Postgres (+ pgvector) backend of the store traits
  doewood-s3      S3 sources: a light SigV4 client, the incremental scanner, event inlets
  doewood-enrich  company identity (ADR 0002 section 1), pure: names, legal forms, aliases,
                  identifier checks, the fuzzy score, evidence fusion, uuid5 group ids
  doewood-registry public registry clients (ADR 0002 section 2): failure-soft, capped,
                  rate-limited, cached; GLEIF, VIES, Wikidata, SEC EDGAR, APR and Companies
                  House; the favicon resolver with its SSRF guard (ADR 0002 section 5)
  doewood-ingest  ingest: fetch, quail and hedgehog, records, the graph (companies, references,
                  similar documents) and search; feature `ocr` (on in the binary); and the
                  enrichment job (ADR 0002 sections 1 to 3, 5 and 6)
  doewood-api     the gRPC API (tonic, gRPC-Web, health, reflection) and GET /icons/{domain};
                  protos in proto/doewood/v1
  doewood         the binary: serve, health, version, config check
charts/doewood    the Helm chart (StatefulSet, one volume per instance)
Dockerfile        the image, with OCR
```

Enrichment runs in every instance: the registry and favicon caches in the store (shared
Postgres, or the instance's SQLite file), each company's freshness and lease, the runtime
settings (on/off, keys, the EDGAR contact), and the job that uses them. See
[docs/enrichment.md](docs/enrichment.md) and [docs/registries.md](docs/registries.md).

The `.proto` files compile with `protox` (pure Rust) in `doewood-api`'s build script, so
`cargo build` needs no `protoc`.

## Development

Branches: `staging` is development (the default branch; every pull request goes there), `main` is
release (fast-forwarded from `staging`; a push to it or a `v*` tag publishes the image, see
[docs/deploy.md](docs/deploy.md#the-published-image)).

The rules follow quail's: Rust 2024 on the pinned toolchain (1.97.1), clippy pedantic with
`-D warnings`, `unsafe` denied, CI on Linux, macOS, Windows and Linux ARM, plus `cargo deny`.

```sh
make check     # fmt, clippy, tests
make deny      # licenses, advisories, bans
```

The Postgres tests skip unless `DOEWOOD_TEST_POSTGRES_URL` is set (the `postgres` CI job sets
it). Locally:

```sh
docker run --rm -d --name dw-pg -p 55432:5432 -e POSTGRES_PASSWORD=pw pgvector/pgvector:pg17
DOEWOOD_TEST_POSTGRES_URL=postgres://postgres:pw@127.0.0.1:55432/postgres make test
docker stop dw-pg
```

## License

MIT, see [LICENSE](LICENSE).
