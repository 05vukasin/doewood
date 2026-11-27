# LLM summaries

doewood reaches language models through APIs only ([ADR 0001](adr/0001-architecture.md),
section 6). Summaries use quail's `summary.llm`: the model reads quail's digest of the document,
its output is fact-checked against what it read, and quail's extractive summary stands in when
the check fails. The code is `crates/doewood-ingest/src/llm.rs`.

**Nothing is sent anywhere until it is configured.** Every source's policy is `off` by default,
and without an AI provider nothing can be sent at all.

```
ingest: fetch → analyze → store record, graph, search ─▶ policy asks? ─▶ llm_jobs (pending)
                                                                              │
LLM queue: pending jobs ─▶ source cap and daily budget ─▶ provider ─▶ summary.llm in the record
Summarize (API): file or object ─▶ analyze (not stored) ─▶ provider ─▶ answer (nothing stored)
```

## Configuration

Everything is a runtime setting, set through the [API](api.md) and kept in the instance store;
nothing comes from the environment ([configuration.md](configuration.md)).

- **An AI provider** (`ConfigService.SetAiProvider`): a preset, a model, a base URL and an API
  key. The presets and their defaults are quail's (and hedgehog's): `openai`,
  `openai_compatible`, `anthropic`, `bedrock`, `openrouter`, `groq`, `vllm`, `ollama`. A
  self-hosted or fine-tuned model is `openai_compatible` or `vllm` with its URL. The key is
  sealed with the master key and never returned. The queue uses the **default** provider;
  `Summarize` may name another. `bedrock` needs `base_url`
  `https://bedrock-runtime.<region>.amazonaws.com`: its region is read from it. It
  authenticates with a bearer token (`api_key`, a Bedrock API key) or with IAM credentials
  (`aws_access_key_id`, `aws_secret_access_key`, optional `aws_session_token`), which sign
  each request with SigV4 (quail's `Endpoint::resolve_with_aws`). The bearer token wins when
  both are stored. IAM credentials are sealed and write-only too; `ListAiProviders` shows only
  `aws_credentials_set`. A change of key or credentials applies to the next call: the queue
  builds a new summarizer when the endpoint changes.
  `ConfigService.RemoveAiProvider` removes a provider with its sealed key and credentials. If
  it was the default there is no default until another provider is set (the next one set
  becomes it): queued jobs wait, as without any provider, and `Summarize` without a provider
  name is refused. Summaries already in flight finish.
- **A policy per source** (`ConfigService.SetLlmPolicy`, or `llm_policy` in `AddSource`):

  | mode | queued at ingest | `Summarize` of its objects |
  |---|---|---|
  | `off` (default) | nothing | refused |
  | `all` | every document | yes |
  | `kinds` | documents of the listed kinds (`contract`, `invoice`, ... as quail classifies them; exact match) | yes |
  | `on_demand` | nothing | yes |

  - `concurrency`: summaries of the source in flight at once, 1 to 64 (default 2).
  - `daily_token_budget`: tokens (prompt plus completion) the source's queued summaries may
    spend per UTC day; unset means no limit.

## The queue

- **The record comes first.** Ingest stores the record, the graph and search, then queues the
  document (`llm_jobs` in the instance store, status `pending`) if the policy asks for one. A
  summary never fails, retries or delays ingest. Only documents with a digest (text enough for
  a model) are queued; spreadsheets are not.
- **Durable.** Jobs live in the store: a restart picks them up. The queue looks at them when
  ingest queues one, when a policy or provider changes, and every 30 s.
- **From the record.** A job reads the stored record's `summary.digest` (the card, then the
  text or, for a long document, its outline, start and excerpts) and sends quail's prompt. The
  fact-checked result goes into the record as quail's `summary.llm` fact (`text`, `subject`,
  `title_en`, `method` = `llm` or `extractive_fallback`, `model`, `checked`, `prompt_tokens`,
  `output_tokens`, or `estimated_tokens` when the provider reports none); the record's step
  versions are unchanged. A record that already has a model's summary is not sent again.
- **Caps.** At most the policy's `concurrency` summaries of a source run at once, and at most 64
  provider calls in the whole instance.
- **Budget.** Before a summary starts, the source's spend on the current UTC day
  (`llm_usage`) is compared with its budget; at or over it, the job waits. Summaries already
  in flight finish, so a day can end up to `concurrency - 1` summaries over. Waiting jobs go on
  after midnight UTC. The spend is the tokens the provider reports, else an estimate of the
  prompt and the output (quail's estimator).
- **Policy changes** apply to waiting jobs too: a job the new policy does not ask for ends
  `skipped`.
- **Soft failures.** A provider error, a timeout (60 s per attempt, 120 s per summary, quail's
  retries with backoff for 429, 5xx and connection errors) or a refusal ends the job `failed`
  with the error (keys are redacted); the record stays without a summary. A new ingest of the
  object queues it again. Without a provider the jobs wait, and the log says why once.
- **Shutdown** abandons the summaries in flight; their jobs stay pending.

The source's progress (`JobsService.WatchProgress`) shows `queued_llm`, `summarized`,
`llm_failed`, `llm_tokens_today` and `llm_last_error`.

## Summarize

`SummarizeService.Summarize` makes one summary now. It skips the queue and stores nothing: no
record, no summary, no job, no spend (the daily budget governs the queue only; an explicit call
is the caller's choice).

- Input: the bytes (`content` with a `name`) or an object of a source (`location`) whose policy
  is not `off`. A `content` call needs no source and no LLM policy, only a provider: the
  policy governs what leaves a source's bucket, and these bytes come from the caller (example
  in [api.md](api.md#summarize)). An object already stored with current step versions is read from its record;
  anything else is analyzed (and not stored).
- Output: the summary, the fact check of the model's output (claims checked, supported, the
  unsupported ones), the provider and model, the file's BLAKE3 and kind. A failing provider or
  an output that fails the fact check gives the extractive summary with `extractive` set and
  the reason in `warnings`.
- Errors: `FAILED_PRECONDITION` without a provider (or one without its key, or without a base
  URL where the preset needs one), for a source whose policy is `off`, a file without text, or
  a file larger than ingest analyzes (512 MiB); `NOT_FOUND` for an unknown provider or source;
  `UNAVAILABLE` without ingest or when the object cannot be read; `RESOURCE_EXHAUSTED` over the
  cap below.

### Cap

An analysis (OCR of a scanned PDF takes 0.6 to 1 GB per page in flight) runs on the request
path, outside ingest's bounded workers, so `Summarize` has its own cap:

- At most `summarize_max_concurrent` calls read and analyze a file at once. It is an ingest
  setting (`ConfigService.SetIngestSettings`, 1 to 16, default 2), read for every call: a
  change applies to the next call without a restart; calls in flight keep running.
- The slot covers reading the object (`location`) and the analysis. It is freed when the
  analysis ends, also when the client went away or the call timed out first (the analysis
  cannot be stopped midway, so its slot stays until it does). The provider call after it holds
  no slot: it is bounded with the queue's calls (at most 64 in flight in all).
- Beyond the cap, the call fails at once with `RESOURCE_EXHAUSTED` and a message to retry
  later. There is no queue and no wait: a waiting call would hold its request (up to the
  message limit) in memory, and a client can retry with backoff instead.
- Size: `content` is bounded by `DOEWOOD_API_MAX_MESSAGE_MIB` (default 64 MiB), an object by its
  source's `max_object_bytes` (as for ingest), and both by what ingest analyzes (512 MiB).
- The log has a debug line per call started (`in_flight`, `cap`) and a warning per call
  refused (`in_flight`, `cap`, and `rejected`, the refusals since start).

## Privacy

Summaries send the card and the digest of a document (names, numbers and excerpts of its text)
to the configured provider. Keep every policy `off`, or use a self-hosted provider
(`openai_compatible`, `vllm`, `ollama`), for documents that must not leave the network. Keys
are never logged, never in errors, and never returned by the API. The tests use a local mock
server only.
