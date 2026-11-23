# doewood

A Rust service around [quail](https://github.com/05vukasin/quail) (PDF, DOCX and text) and
[hedgehog](https://github.com/05vukasin/hedgehog) (spreadsheets). It watches S3-compatible
storage, extracts metadata from every file, links documents and companies in a shared graph, and
serves it all over gRPC.

The name comes from Košutnjak, a Belgrade forest: *košuta* (a doe) + wood. It joins the hedgehog
and the quail.

**Status:** design accepted, implementation starting. See [ADR 0002](docs/adr/0002-company-enrichment.md) (company identity and registries) and
[ADR 0001](docs/adr/0001-architecture.md).

- **One binary.** It starts with no configuration at all; everything else is set at runtime
  through the API.
- **Independent instances**, each with its own SQLite store, all sharing one Postgres + pgvector
  database for records, relations and search.
- **Any S3-compatible store**, watched continuously.
- **AI models through APIs:** OpenAI, Anthropic, Bedrock, OpenRouter, Groq, vLLM or Ollama, using
  the same presets as hedgehog.

## License

MIT, see [LICENSE](LICENSE).
