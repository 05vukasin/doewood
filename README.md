# doewood

A Rust service around [quail](https://github.com/05vukasin/quail) (PDF, DOCX and text) and
[hedgehog](https://github.com/05vukasin/hedgehog) (spreadsheets). It watches S3-compatible
storage, extracts metadata from every file, links documents and companies in a shared graph, and
serves it all over gRPC.

The name comes from Košutnjak, a Belgrade forest: *košuta* (a doe) + wood. It joins the hedgehog
and the quail.

**Status:** step 1 (skeleton: configuration, local store, ops endpoints) in progress. See [ADR 0002](docs/adr/0002-company-enrichment.md) (company identity and registries) and
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
- serves `/healthz`, `/readyz` and `/metrics` on `127.0.0.1:7444`;
- runs standalone, without a shared Postgres;
- logs JSON to stderr (`RUST_LOG` sets the filter) and stops cleanly on Ctrl-C or SIGTERM.

```sh
curl -s 127.0.0.1:7444/readyz     # ready
cargo run -- config check         # effective configuration, secrets redacted
cargo run -- version
```

The bootstrap variables are listed in [docs/configuration.md](docs/configuration.md); sources
and discovery in [docs/sources.md](docs/sources.md).
The gRPC API (`127.0.0.1:7443`) arrives in a later step.

## Layout

```
crates/
  doewood-core    bootstrap config, errors, ids, the master key type
  doewood-model   records, graph, claims, search: the store traits and their conformance suite
  doewood-store   the instance-local SQLite store: settings, encrypted secrets, sources, jobs,
                  and the standalone backend of the store traits (FTS5 search)
  doewood-pg      the shared Postgres (+ pgvector) backend of the store traits
  doewood-s3      S3 sources: a light SigV4 client, the incremental scanner, event inlets
  doewood         the binary: serve, version, config check
```

Planned: `doewood-ingest`
(quail/hedgehog), `doewood-api` (gRPC) and `doewood-enrich` (registries, ADR 0002).

## Development

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
