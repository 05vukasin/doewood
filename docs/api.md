# The gRPC API

doewood serves its API with gRPC on `DOEWOOD_LISTEN` (default `127.0.0.1:7443`), as described in
[ADR 0001](adr/0001-architecture.md), section 4. The protobuf files are in
[`proto/doewood/v1/`](../proto/doewood/v1), in the versioned package `doewood.v1`. Breaking
changes go to a new package (`doewood.v2`), never into v1.

- **gRPC and gRPC-Web** on the same port. HTTP/1.1 is accepted, so a browser can call the API
  with gRPC-Web (`application/grpc-web+proto`, or the base64 `application/grpc-web-text`).
  No CORS headers are sent: serve the frontend from the same origin as the API (one ingress).
  With no authentication in v1, a CORS policy would let any page a user opens call the API.
- **Health** (`grpc.health.v1.Health`) and **reflection** (`grpc.reflection.v1` and
  `v1alpha`), so `grpcurl` and Kubernetes gRPC probes work without the `.proto` files.
- **No authentication in v1.** The API binds to localhost by default; on Kubernetes, network
  isolation protects it. Every doewood service runs behind one (empty) interceptor, where
  tokens or OIDC go later without changing the API. Health and reflection stay open.
- **Message size:** up to `DOEWOOD_API_MAX_MESSAGE_MIB` (default 64 MiB) each way, room for a
  file sent to `Summarize`. Larger messages are refused (`OUT_OF_RANGE`).
- **Limits** on streams per connection, keepalive, connection age, calls in flight and a
  server deadline: see [Limits](#limits).
- **Secrets are write-only.** S3 credentials, AI API keys and Bedrock IAM credentials are
  sealed with the master key; no response and no error carries one, only `credentials_set`,
  `api_key_set` or `aws_credentials_set`. Error
  messages never echo a value that may hold a secret (an endpoint with `user:password@`, a
  key), nor a presigned URL.

The S3 event webhook is not part of the gRPC API: it is on the ops server (see
[Event webhook](#event-webhook)). One plain HTTP route shares the API port,
`GET /icons/{domain}` (see [Company icons](#company-icons)).

## Services

| service | RPCs |
|---|---|
| `ConfigService` | `AddSource`, `UpdateSource`, `PauseSource`, `ResumeSource`, `RemoveSource`, `ReleaseSourceClaim`, `ListSources`, `GetSource`, `SetLlmPolicy`, `SetSqsInlet`, `RemoveSqsInlet`, `SetAiProvider`, `RemoveAiProvider`, `ListAiProviders`, `GetIngestSettings`, `SetIngestSettings`, `GetEnrichmentSettings`, `SetEnrichmentSettings`, `GetApiSettings`, `SetApiSettings` |
| `JobsService` | `WatchProgress` (server stream), `ReprocessJob`, `WatchReprocess` (server stream) |
| `RecordsService` | `GetRecord`, `ListRecords` |
| `GraphService` | `GetNode`, `Neighbours`, `CompanyDocuments`, `ListMerges`, `MergeCompanies`, `UndoMerge`, `SplitCompany`, `DismissPair`, `ListDismissals`, `ListProposals`, `AcceptProposal`, `RejectProposal` |
| `SearchService` | `Search` |
| `SummarizeService` | `Summarize` |

The comments in the `.proto` files are the reference for every field. In short:

### Sources

- **AddSource** first validates the request, then **claims** the source (its endpoint, bucket
  and prefix) in the shared store. When another instance owns it, the call fails with
  `ALREADY_EXISTS`, and the message names the owner, for example
  `source http://minio:9000/docs/in/ is already claimed by instance "doewood-1"`. The source is
  then stored with its credentials sealed, and an active source starts scanning at once, without
  a restart. `paused: true` adds it without scanning.
- The endpoint is normalized before the claim (scheme and host lowercased, the default port
  and a trailing `/` dropped), so `HTTP://MinIO:9000/` and `http://minio:9000` are one source.
- `skip_known_copies` (off by default) records a new key whose single-part ETag and size match
  content already stored without a GetObject. It trusts the ETag as the content's MD5; see
  [sources.md](sources.md#known-copies).
- **UpdateSource** changes the region, URL style, scan interval, size cap, `skip_known_copies`
  or credentials (`clear_credentials` deletes them). The endpoint, bucket and prefix are the source's identity
  and do not change. A running scanner restarts, so new options apply at once (with a shared
  Postgres, a source that is not scanned, for example one claimed by another instance, is not
  started).
- **PauseSource** stops the scanner (a pass under way keeps its checkpoint); **ResumeSource**
  starts it again, and retries a failing source right away. With a shared Postgres it claims the
  source first: when another instance owns it, the source turns `error` (the owner in
  `state_reason`) and the call fails with `ALREADY_EXISTS`. Before the start-up claim pass has
  run, it only stores the state, and the pass starts the scanner (see
  [sources](sources.md#claims)).
- **RemoveSource** releases the claim, stops the scanner, and deletes the source with its
  progress and credentials. If the shared store is unreachable it fails with `UNAVAILABLE` and
  removes nothing, so no claim is left behind.
- **ReleaseSourceClaim** (issue #24) deletes the claim on a source (`endpoint`, `bucket`,
  `prefix`, normalized as in `AddSource`) in the shared store, whichever instance holds it, so
  that another instance can add or resume the source. It is meant for an owner that is gone
  for good ([deploy.md](deploy.md#a-dead-instance)); nothing is taken over automatically, and
  claims still have no lease ([ADR 0001](adr/0001-architecture.md), section 2). `owner` is
  required and must name the instance that holds the claim (as the `claimed by instance ...`
  reason or the `ALREADY_EXISTS` error names it): another holder is `FAILED_PRECONDITION` and
  nothing changes, so a retry never releases a claim a new owner has taken meanwhile. It is
  idempotent: with no claim, it answers OK with `released: false`. A claim this instance holds
  for one of its own sources is refused with `FAILED_PRECONDITION` (remove the source
  instead). The answer has the released `owner` and its `claimed_at`. Each call is logged with
  the key, the owner, the instance that served it (`released_by`) and the caller's address
  (`peer`); a release at WARN, `source claim released by an operator`. If the old owner still
  runs, it keeps scanning the source until it restarts: release only the claim of an instance
  that is gone.
- **SetSqsInlet** sets the source's SQS event inlet (see [sources](sources.md#sqs)): the
  `queue_url` (`https://sqs.<region>.amazonaws.com/<account>/<queue>`, without credentials,
  query or fragment), the `region` (empty: the one in an AWS queue URL, else `us-east-1`),
  `disabled` to store it without polling, and write-only `credentials` for the queue alone
  (unset keeps the stored ones; with none the inlet signs with the source's;
  `clear_credentials` deletes them). The inlet restarts at once; it polls only while the
  source's scanner runs. `Source.sqs_inlet` shows the settings, `credentials_set` (never the
  credentials), whether it runs, and counters since it started: messages received and
  deleted, events emitted, records ignored, test events, malformed messages, errors and the
  last error. **RemoveSqsInlet** deletes the inlet and its credentials; removing the source
  does too.
- While the shared Postgres is not connected yet, `AddSource`, `RemoveSource`,
  `ReleaseSourceClaim` and the records,
  graph and search RPCs answer `UNAVAILABLE`; the rest works. Once the start-up claim pass has
  run, `ResumeSource` needs the database too (it claims the source).

### LLM policy and AI providers

- **SetLlmPolicy** sets which documents of a source get an LLM summary: `LLM_MODE_OFF` (the
  default), `LLM_MODE_ALL`, `LLM_MODE_KINDS` with `kinds`, or `LLM_MODE_ON_DEMAND`; with a
  concurrency (1 to 64, default 2) and an optional daily token budget (tokens per UTC day). It
  applies to documents ingested from now on and to those waiting in the queue. The same
  policy again writes nothing and leaves the queue alone. See [llm.md](llm.md).
- **SetAiProvider** configures a provider with a preset, a model, an optional base URL and a
  write-only API key. The presets are those of quail's `ApiConfig` and hedgehog: `openai`,
  `openai_compatible` (needs `base_url`), `anthropic`, `bedrock`, `openrouter`, `groq`, `vllm`
  and `ollama`. The name defaults to the preset, so several providers of one preset can live
  side by side under other names. An empty `api_key` keeps the stored key; `clear_api_key`
  deletes it. The first provider set becomes the default; `make_default` moves the default.
  The LLM queue uses the default provider. For `bedrock`, `base_url` is required
  (`https://bedrock-runtime.<region>.amazonaws.com`): the region is read from it.
  - **Bedrock authentication:** `api_key` is a bearer token (a Bedrock API key). Instead, or
    as well, `bedrock` takes IAM credentials: `aws_access_key_id` and `aws_secret_access_key`
    (both or neither) and, for temporary credentials, `aws_session_token`. They are sealed and
    write-only like `api_key` and replaced as one unit: a new pair without a token drops the
    stored token. All three empty keep the stored credentials; `clear_aws_credentials` deletes
    them. Requests are signed with SigV4 (service `bedrock`, the base URL's region). When both
    a bearer token and IAM credentials are stored, the bearer token wins, as in quail. A
    `bedrock` provider needs one of them, and IAM fields on another preset are
    `INVALID_ARGUMENT`; a provider moved to another preset loses its IAM credentials.
  - **Idempotent:** the same preset, model, base URL, key, IAM credentials and default again
    write nothing (the `updated_at` it returns stays), requeue no job and leave the queue
    alone, so a service can make it at every boot
    ([configuration.md](configuration.md#configuring-doewood-from-another-service)). Stored
    secrets are decrypted and compared in constant time, never returned.
- **RemoveAiProvider** removes a provider by `name` with its sealed API key and IAM
  credentials, for an application that switches its AI off at boot. When it was the default
  (`was_default`) there is no default afterwards, even if other providers remain: queued LLM
  jobs wait (the log says why), `Summarize` without a provider name is
  `FAILED_PRECONDITION` and one naming the removed provider `NOT_FOUND`. Summaries already in
  flight finish. The next provider set becomes the default, as the first one does. Idempotent:
  an unknown provider is OK with `removed` false; a malformed name is `INVALID_ARGUMENT`.
- **ListAiProviders** lists them with `api_key_set` and `aws_credentials_set`, never a secret,
  plus every preset name.

### Ingest settings

- **GetIngestSettings** returns ingest's runtime settings, defaults filled in.
- **SetIngestSettings** changes them; an unset field keeps its value. `similar_min_jaccard` is
  the least MinHash Jaccard estimate for a `similar_to` edge: above 0 and at most 1 (default
  0.8), anything else is `INVALID_ARGUMENT`. Ingest reads it for every file, so it applies to
  the next file without a restart. Edges already written are kept; `ReprocessJob` with
  `force: true` compares a source's documents again (a lower value adds edges, a higher one
  removes none). It replaces the old `DOEWOOD_SIMILAR_MIN_JACCARD` variable (see
  [configuration.md](configuration.md)). `similar_max_bucket` is the largest LSH band bucket
  the `similar_to` candidate query reads: 10 to 1000000 (default 1000), anything else is
  `INVALID_ARGUMENT`; a band key shared by more documents is skipped (issue #148, see
  [ingest.md](ingest.md)). `summarize_max_concurrent` is how many `Summarize`
  calls read and analyze a file (or read a stored record) at once: 1 to 16 (default 2), anything else is
  `INVALID_ARGUMENT`. It is read for every `Summarize` call, so it applies to the next one;
  calls in flight keep running.

### API settings

- **GetApiSettings** returns the server's per-call limits, defaults filled in:
  `max_calls_in_flight` (default 256) and `deadline_seconds` (default 60).
- **SetApiSettings** changes them; an unset field keeps its value, and a refused request
  changes nothing. `max_calls_in_flight` is 1 to 65536, `deadline_seconds` 1 to 3600; anything
  else is `INVALID_ARGUMENT`. They apply to the next call, without a restart; a call already
  running keeps the deadline it started with. What they do is in [Limits](#limits).

### Enrichment settings

Company enrichment's runtime settings (ADR 0002 section 7). The enrichment job reads them on
every pass and the icon route on every lookup, so a change needs no restart.

- **GetEnrichmentSettings** returns them, defaults filled in: the registry switches
  (`gleif_enabled`, `vies_enabled`, `wikidata_enabled`, `edgar_enabled`, `apr_enabled`,
  `companies_house_enabled`, all default `true`), `edgar_contact` (default empty: the generic
  User-Agent), `favicon_fallback` (default `false`) and `favicon_egress_proxy` (default
  empty: direct), and the registry delta feed switches (`gleif_feed_enabled`,
  `edgar_feed_enabled`, `apr_feed_enabled`, default `true`; `companies_house_feed_enabled`,
  default `false`). Secrets come back only as `companies_house_api_key_set`,
  `companies_house_stream_key_set`, `opencorporates_api_key_set` and `apr_extra_roots_set`.
  `companies_house_active` is `companies_house_enabled` and a key set: Companies House is
  asked only then. `companies_house_feed_active` is `companies_house_feed_enabled`, a
  streaming key set and `companies_house_active`: the stream is read only then. A feed runs
  only while its registry is switched on too (see [enrichment.md](enrichment.md#delta-feeds)).
- **SetEnrichmentSettings** changes them, all or nothing; an unset field keeps its value. The
  write-only fields work as `SetAiProvider`'s `api_key`: empty keeps the stored value, the
  `clear_` field deletes it, and both at once is `INVALID_ARGUMENT`.
  - `companies_house_api_key`, `companies_house_stream_key`, `opencorporates_api_key`: 8 to
    256 printable ASCII characters, without spaces or `:` (surrounding white space is
    trimmed). The streaming key is a key of its own: Companies House issues streaming keys
    apart from REST keys.
  - `apr_extra_roots_pem`: a PEM bundle of at most 64 KiB whose certificates parse.
  - `edgar_contact`: at most 200 printable ASCII characters; empty goes back to the generic
    User-Agent.
  - `favicon_egress_proxy`: `http://` or `https://`, a host, no credentials (the favicon
    guard's check); empty removes it.

  A wrong value is `INVALID_ARGUMENT` with the field, and the message never echoes a key or
  the PEM. See [registries.md](registries.md#configuring-the-registries) for what each
  registry needs.

### Progress

**WatchProgress** streams one `KIND_SNAPSHOT` update per source, then an update whenever a
source changes: `KIND_COUNTERS`, `KIND_STATE_CHANGED` (with `previous_state`) or `KIND_REMOVED`.
It ends when the client cancels or the server shuts down, or when its connection reaches its
maximum age (at most 35 minutes, see [Limits](#limits)). `source_ids` narrows it; empty
watches every source, including ones added later. A client watches again when the stream
ends with an error (for example a transport error when the connection closed): the first message of the new
stream is a full snapshot, so nothing is lost. `WatchReprocess` behaves the same way. The server
takes one snapshot of every source for all open streams together (at most every 100 ms while
something changes, and every second otherwise), and each stream sends what changed for its
sources; the store's per-status counters make a snapshot cheap however many objects a source holds.

A watch of every source (empty `source_ids`) also carries the instance's enrichment job: a
`KIND_ENRICHMENT` update with the first snapshot and whenever its counters change, with
`enrichment` set (and no `progress`): `enriched`, `merged`, `proposed`, `failed`,
`registry_warnings`, `edges`, `redirected`, `websites`, `passes`, `last_pass_completed_at`,
`running`, and `feeds` (one `FeedProgress` per delta feed: `feed`, `enabled`, `leader`,
`runs`, `records`, `companies_due`, `failures`, `last_run_at`, `position`), counted since the
instance started (see [enrichment.md](enrichment.md#progress)). A client that does not know
the kind can skip it.

| counter | meaning |
|---|---|
| `seen` | objects under the prefix that discovery knows about |
| `new`, `changed`, `deleted` | discovery events since this instance started (scans and the webhook) |
| `processed`, `failed`, `pending` | objects ingested (`done`), failed (after their retries), and not ingested yet |
| `queued_llm` | documents waiting in the LLM queue |
| `summarized`, `llm_failed` | documents the LLM queue summarized, and those whose summary failed (`llm_last_error` on the source's progress has the latest error) |
| `llm_tokens_today` | tokens the source's summaries spent on the current UTC day (what its daily budget is checked against) |

A spreadsheet over ingest's cell cap (at least 1,600,000 cells, more with more memory per
worker, or `DOEWOOD_INGEST_MAX_CELLS`) counts as `failed` (its status says `sheet.read@1: file
refused: the workbook has more than 1,600,000 cells ...`, and how to raise the cap), and a file over 16 MiB may stay
`pending` a little longer while bigger files hold ingest's memory budget; see
[ingest.md](ingest.md#memory).

### Records, graph and search

- **GetRecord** takes a BLAKE3 in hex; the record's JSON is in `body_json`, exactly as stored.
- **ListRecords** pages through the records of a source's files, ordered by hash: pass
  `next_page_token` back as `page_token` until it comes back empty. A page holds up to
  `page_size` records (default 50, at most 1000) and also ends with the record that brings
  its bodies to 3 MiB (issue #111), so a page can hold fewer records than asked for and still
  have a next one; it always holds at least one record when any is left. With records up to
  about 1 MiB a page stays under the 4 MiB message size that gRPC clients accept by default.
  A page that ends exactly at the end of the source may still give a token; the next page is
  then empty. See [ListRecords page budget](#listrecords-page-budget).
- **GetNode** finds a node by id or by kind and key; **Neighbours** follows its edges with the
  filters of the model's `NeighbourQuery` (direction, edge kinds, neighbour kind, a day the edge
  must hold on, inactive edges); **CompanyDocuments** lists the documents with a live `party`
  edge to a company.
  - A company's props have no `matches` list since issue #129. How each document matched (the
    ladder step, the alias, its confidence and conflicts) is in the props of its `party` edge
    (`match`), with the document in the edge's evidence: `Neighbours` with `direction` in and
    `edge_kinds` party lists them. A company that ingest has not patched since the upgrade can
    still show the old stored list until its next document.
- **Neighbours** and **CompanyDocuments** page as ListRecords does (issue #112): `page_size`
  (default 50, at most 1000; more is `INVALID_ARGUMENT`), `page_token` and `next_page_token`.
  A page also ends with the item that brings it to 3 MiB (node keys and props, and for
  Neighbours the edges' props and evidence, as JSON text). **Since #112 a client that ignores
  `next_page_token` sees only the first page**, 50 items by default: before, both returned
  every item in one response. Pass the same filters with each `page_token`. Neighbours are
  ordered by edge kind (its name), then `valid_from` (undated first), then edge id, as before.
  CompanyDocuments lists each document once, in the order of its first live `party` edge to
  the company (that edge's `valid_from`, undated first, then its id; edge ids are UUIDv7, so
  undated documents come in the order they were linked); before #112 they were ordered by key.
  A token that is not one from a previous page is `INVALID_ARGUMENT`; an empty page still
  tells a missing node (`NOT_FOUND`) and, for CompanyDocuments, a node that is not a company
  (`INVALID_ARGUMENT`) apart. See [Pages of Neighbours and CompanyDocuments](#pages-of-neighbours-and-companydocuments).
- **Search** matches every word of `text` and every filter that is set (company, kind, date and
  amount ranges, source), with `limit` (up to 1000) and `offset`.
- **The explorer's reads** (issue #172), for a visual graph explorer. Each is bounded, by a
  page or by caps that end it with `truncated: true`; a cap above its maximum is
  `INVALID_ARGUMENT`. See [The explorer's reads](#the-explorers-reads) for how they are kept
  fast.
  - **ListCompanies** lists live companies (merged-away husks never), with id, name, other
    names, identifiers (`lei`, `vat`, ...), jurisdiction, website, favicon domain (for
    `GET /icons/{domain}`), document count, degree and whether they were enriched. Filter by
    name prefix (ignoring case), jurisdiction, having an identifier, and enriched or not;
    order by documents, degree (most first; ties by id, descending) or name. Pages as
    ListRecords: `page_size` default 50, at most 1000, a 3 MiB budget; a token only goes with
    the order it came from.
  - **CompanyGraph** returns the `limit` companies with the most documents that match the
    filter (default 500, at most 2000) and the links between them, most shared documents
    first (`max_links` default 5000, at most 20000). A link is two companies that are parties
    of the same documents (`shared_documents`, at least `min_weight`, with each side's roles)
    or that have a live corporate-event edge (`events`: the edges themselves, with dates,
    source and evidence; at most 50 read per company). A document with more than 32 distinct
    parties (a register, not a deal) links none of them, but counts for each.
    `documents_total` is the number of documents with a live party edge in the whole graph,
    each once however many companies it names (so not the sum of the companies' counts), and
    not narrowed by the filter or the caps. A link names no document ids: Subgraph from its
    two companies at depth 1 shows the documents they share (within its caps).
  - **Subgraph** walks from 1 to 100 seeds up to `depth` 3 over live edges either way, only
    through `node_kinds` and `edge_kinds` when given, and only over edges that hold on `as_of`
    when given. Caps: `limit` nodes (default 200, at most 2000), `max_edges` (default 1000, at
    most 10000), about 8 MiB of props and evidence, and per level each node's share of the
    edges left (so one hub cannot take them all). Nodes come with their depth.
  - **Path** finds the shortest paths (`max_paths`, default 1, at most 10) between two nodes
    within `max_depth` edges (default 4, at most 6), over `edge_kinds` and `as_of` as above;
    each edge carries its evidence. It is a bidirectional breadth-first search that grows the
    smaller side a level at a time, within 5000 visited nodes and 20000 edges a level.
    `truncated` says a cap cut it, so a shorter path (or one, when none came back) may exist;
    no paths and `truncated: false` means there is none within `max_depth`.
  - Document counts, the document total, degrees and weights are counted by the store, not
    on each read, and trail the edges by a few seconds: `stale: true` says a large change (a
    bulk ingest) is still being counted. A missing seed or path end is `NOT_FOUND`.

Dates are `YYYY-MM-DD` strings; JSON props and evidence are strings holding JSON.

### Merges, undo, split and dismissals

Every company merge and split is reversible (ADR 0002 section 1.8). The rules are in
[ingest.md](ingest.md#merges-undo-and-dismissals); in short:

- **ListMerges** pages through the merge log, newest first (`company_id` filters to entries
  where it survived or was absorbed). Each entry has the actor (`ingest`, `enrichment` or
  `manual`, and the instance), the evidence, the confidence, the fused probability, the undo
  payload (`undo_json`: the aliases, edges and props that moved) and, once undone,
  `undone_at` and `undone_by`.
- **MergeCompanies** is a person's merge: `absorbed_id` into `survivor_id`. The absorbed
  company's aliases and edges move to the survivor, its props are folded in, and its node
  stays with `{"merged_into": <survivor>}`.
- **UndoMerge** restores what the entry's payload lists. When later live merges or splits
  involve the same companies it answers `FAILED_PRECONDITION` naming them; with `cascade`
  they are undone first, newest first (`undone_ids` gives the order). With `dismiss` the two
  companies become a dismissed pair. `skipped` lists what could not be put back because it
  changed since (an alias blocked, an edge identity taken).
- **SplitCompany** gives a new company (`name`) the named `aliases`, the `party` edges of
  `document_ids`, and `edge_ids`; it is logged like a merge, so UndoMerge reverses it (the new
  company is deleted when nothing else attached to it). With `dismiss` the two are dismissed
  too, and that split can no longer be undone.
- **DismissPair** records a permanent "not the same" pair, in both directions: no merge
  between them, or between companies merged into them, is ever made, whatever the evidence.
  Dismissing a pair again returns the first dismissal; a pair merged now is refused (undo the
  merge with `dismiss`). **ListDismissals** pages through them, newest first.

### Enrichment proposals

What the enrichment gate leaves to a person (ADR 0002 section 1.6; see
[enrichment.md](enrichment.md#proposals)): a **registry record** a name search found in the
review band (`candidate` is `<registry>:<record>`, such as `apr:companies/12345678`), or
another company that may be the same, to **merge** with (`candidate` is its id; the pair is
named in id order, `company_id` the smaller). Each has its evidence (`evidence_json`), the
fused probability, the reason (`REVIEW_BAND`, `CONFLICT`, or `DISMISSED` for the dismissed
pairs schema 5 moved out of props), `detail_json` (a record's registry, id and name) and its
state (`OPEN`, `ACCEPTED`, `REJECTED`, with `resolved_at`, `resolved_by`, `note`, and
`dismissed` or `merge_id`). There is one open proposal per kind, company and candidate: the
job proposing it again updates its evidence and probability.

- **ListProposals** pages through them, newest first, filtered by `state`, `kind` and
  `company_id` (a company's own proposals and the merge proposals naming it).
- **AcceptProposal** (actor `manual`, this instance):
  - a **merge** goes through the merge log like MergeCompanies, with the proposal's evidence
    and probability, so **UndoMerge** of the returned `merge` undoes it. The survivor is
    `survivor_id` when given (one of the two), else the one with more documents. Companies
    merged meanwhile are followed to the company they are now; a pair that is one company
    already is accepted without a merge.
  - a **registry record**: its identifier (`alias`: the MB for APR, the LEI for GLEIF, the
    company number for Companies House, the CIK for EDGAR) becomes an alias of the company,
    as when the job adopts a name match, and the record's registry is made due, so the next
    enrichment pass (within 30 seconds) looks the company up by it and writes its facts and
    edges. To take it back, split the alias off (SplitCompany).
  - `FAILED_PRECONDITION` when the proposal is not open, the pair is dismissed, or the
    record's identifier belongs to another company (merge the two first).
- **RejectProposal** closes it. With `dismiss` it is permanent: the same proposal is never
  opened again, and for a merge the two companies are recorded as a dismissed pair
  (`dismissal`), so they are never merged or proposed either. A plain rejection is not
  permanent: the job may propose the same again on a later pass.

Page tokens are opaque: pass `next_page_token` back as `page_token` until it comes back empty.

### Reprocessing

- **ReprocessJob** starts a one-shot job for a source and answers at once with its `job_id`. It
  puts back in the ingest queue every ingested file whose stored step versions differ from the
  running engine's (`doewood.ingest`, the quail plan fingerprint, and the hedgehog revision for
  spreadsheets); with `force: true`, every live file of the source. Without ingest on the
  instance it answers `UNAVAILABLE`.
- **WatchReprocess** streams the job's progress: `STATE_SELECTING` (with `examined` and
  `selected` growing), `STATE_RUNNING` (the source's `processed`, `failed` and `pending`), and
  last `STATE_DONE` (nothing of the source pending) or `STATE_FAILED` with `error`; then the
  stream ends. Jobs live in the instance that started them, until it restarts (`NOT_FOUND`
  after that).

```sh
JOB=$(grpcurl -plaintext -d "{\"source_id\": \"$ID\"}" 127.0.0.1:7443 \
    doewood.v1.JobsService/ReprocessJob | jq -r .jobId)
grpcurl -plaintext -d "{\"job_id\": \"$JOB\"}" 127.0.0.1:7443 doewood.v1.JobsService/WatchReprocess
```

### Summarize

**Summarize** summarizes one file now, outside the LLM queue, and stores nothing (no record, no
summary, no spend). Send the bytes (`content`, with `name`), an object of a source
(`location`), or the BLAKE3 of a document doewood has stored (`blake3`, 64 hex digits); a source
whose LLM policy is `LLM_MODE_OFF` is refused. With `blake3` the stored record's digest is
summarized as it is (no fetch, no OCR, no analysis): one of this instance's sources must hold
the object with a policy other than `LLM_MODE_OFF` (else `FAILED_PRECONDITION`), and an unknown
hash is `NOT_FOUND`. `provider` names an AI
provider; empty uses the default. The answer has the summary (`digest`), the fact check of the
model's output, the provider and model, the file's BLAKE3 and kind. When the provider fails or
its output fails the fact check, the answer is quail's extractive summary with `extractive`
set and the reason in `warnings`. Without a configured provider it answers
`FAILED_PRECONDITION`. See [llm.md](llm.md).

At most `summarize_max_concurrent` calls (an ingest setting, default 2) read and analyze a file,
or read a stored record, at once. A call beyond that is answered `RESOURCE_EXHAUSTED` at once ("... Summarize analyses
are in flight ...: retry later"); nothing waits in a queue. Retry with backoff. `content` is
bounded by the message limit (`DOEWOOD_API_MAX_MESSAGE_MIB`, default 64 MiB), an object by its
source's `max_object_bytes`; a file larger than ingest analyzes (512 MiB) is
`FAILED_PRECONDITION`.

A local file, sent as `content`. It needs only a configured provider: no source and no LLM
policy. `bytes` fields are base64 in grpcurl's JSON, and `-d @` reads the request from stdin:

```sh
jq -n --arg name report.pdf --rawfile b <(base64 -w0 report.pdf) '{name:$name, content:$b}' \
  | grpcurl -plaintext -d @ 127.0.0.1:7443 doewood.v1.SummarizeService/Summarize
```

A stored document, by its BLAKE3 (from `RecordsService.ListRecords`, say):

```sh
grpcurl -plaintext -d "{\"blake3\": \"$BLAKE3\"}" 127.0.0.1:7443 doewood.v1.SummarizeService/Summarize
```

`"provider": "<name>"` in the request picks a provider other than the default. On macOS,
`base64 -w0` is `base64 -i report.pdf`.

## Errors

| code | when |
|---|---|
| `INVALID_ARGUMENT` | a field is wrong. Every problem is reported at once: the message lists `field: reason` pairs, and a `google.rpc.BadRequest` detail carries the same field paths (`credentials.access_key_id`, `llm_policy.kinds[2]`, ...). |
| `NOT_FOUND` | the source, record, node or provider does not exist (also `Summarize` by an unknown `blake3`) |
| `ALREADY_EXISTS` | the source exists on this instance, or another instance has claimed it (named in the message) |
| `UNAVAILABLE` | the shared Postgres is not connected (yet), or a call to it failed; retry. `ReprocessJob` and `Summarize` on an instance without ingest; `Summarize` of an object that cannot be read, or by `blake3` while the records store is not connected. The server is overloaded: more calls are in flight than `max_calls_in_flight` (the message says `overloaded` and the trailer `grpc-retry-pushback-ms` says how long to wait; nothing ran, retry with backoff). |
| `DEADLINE_EXCEEDED` | the call ran longer than the server's deadline (`deadline_seconds`, default 60 s), or than the client's own deadline |
| `CANCELLED` | the client's `grpc-timeout` ran out on the server side (the client usually reports its own `DEADLINE_EXCEEDED` first) |
| `FAILED_PRECONDITION` | the shared database was migrated by a newer doewood. A merge of a dismissed pair or of a merged-away company; an undo that later merges depend on (named in the message), of an entry already undone, or of a split that dismissed its pair; a dismissal of two companies merged now. `Summarize` without a configured AI provider (or with one missing its key or base URL), of a source whose LLM policy is off (by `blake3`: of a document no source with a policy other than off holds), of a file without text, or of a file larger than ingest analyzes |
| `RESOURCE_EXHAUSTED` | `Summarize` while as many calls as `summarize_max_concurrent` allows are reading or analyzing a file; retry later |
| `OUT_OF_RANGE` | a message larger than the limit |
| `INTERNAL` | anything else; the details are in the server log, not in the message |

## Limits

The server protects itself from one client and from overload. What a client sees, and what
to do:

| limit | value | what the client sees |
|---|---|---|
| Streams (calls) at the same time on one HTTP/2 connection | 256 (fixed) | the next call waits in the client until one ends (HTTP/2 `SETTINGS_MAX_CONCURRENT_STREAMS`); open another connection, or a channel per 256 watches |
| HTTP/2 keepalive | PING after 30 s without traffic, closed if no answer within 20 s (fixed) | nothing; a dead or vanished peer's connection, streams and tasks are freed |
| TCP keepalive | probes after 60 s idle (fixed) | nothing; half-open connections behind a NAT or load balancer are found |
| Connection age | GOAWAY at 30 min ± 10% (each connection draws its own age, so clients that connected together do not reconnect together), closed 5 min later (fixed) | new calls go to a new connection (which the load balancer may send to another replica); calls already running get the 5 minutes to finish; an open stream still running then ends with a transport error (the connection is closed): watch again |
| Calls in flight, per instance | `max_calls_in_flight`, default 256 (runtime setting) | `UNAVAILABLE`, at once, with `overloaded` in the message and a `grpc-retry-pushback-ms` trailer (100 to 199 ms); retry with backoff, no sooner than the pushback (another replica may answer) |
| Server deadline | `deadline_seconds`, default 60 s (runtime setting) | `DEADLINE_EXCEEDED`; a shorter client `grpc-timeout` wins |

- **Streams** (`WatchProgress`, `WatchReprocess`, reflection, the health `Watch`) count
  against the in-flight cap and the deadline only while they are set up: until the server
  has answered with the stream. An open stream is never shed or cut by the deadline; the
  streams per connection bound how many one connection holds.
- **`Summarize`** counts against the in-flight cap but has no server deadline: its analysis
  and provider call are bounded on their own and can take minutes, and its analyses have
  their own cap (`summarize_max_concurrent`, `RESOURCE_EXHAUSTED` beyond it). Give it a
  client deadline if you need one.
- **Health checks** (`grpc.health.v1.Health`) are never shed nor given the deadline, so a
  probe answers under load.
- **gRPC-Web** calls get the same limits and errors (the status in the response headers). The
  icon route `GET /icons/{domain}` is outside them: it has its own (16 lookups in flight, then
  `503`, see [Company icons](#company-icons)), and so do the ops endpoints (another port).
- The connection limits are fixed when the listener starts (they are not bootstrap variables;
  see [configuration.md](configuration.md#api-limits)). The two per-call limits are set with
  `SetApiSettings` and apply to the next call.
- **Reads and ingest (standalone, issue #83).** Read calls (records, graph, search, similar
  documents, sources, settings, merges, the `WatchProgress` snapshot) run on the instance
  store's read-only connections, so they do not queue behind ingest's writes. A read sees
  what was committed when it started: a write call's effect is visible to every call made
  after it returned, and a read running at the same time as a write sees the state before
  it. When the API's reads and ingest's both wait for a read-only connection, ingest's go
  first; at least one API read always runs (issue #109). Store calls are bounded by the
  in-flight cap above (`max_calls_in_flight`, default 256, fewer than tokio's 512 blocking
  threads), so a flood of reads is shed with `UNAVAILABLE` before it can take every blocking
  thread from ingest. See
  [configuration.md](configuration.md#the-instance-store).
- **Retries:** `UNAVAILABLE` is safe to retry for every call (a shed call did not run). Use
  exponential backoff with jitter (for example 100 ms doubling to 10 s), and a client
  deadline on every unary call. gRPC clients reconnect on GOAWAY by themselves.
- **Retry pushback (issue #104):** a shed call carries the trailer `grpc-retry-pushback-ms`
  (gRFC A6; in a trailers-only response, so a gRPC-Web client finds it in the response
  headers). Its value is drawn per call, uniformly from 100 to 199 ms, so clients shed at
  the same moment do not all come back at the same moment. A gRPC client with a retry policy
  (`retryPolicy` in its service config, `UNAVAILABLE` among `retryableStatusCodes`) waits
  exactly that long before its next attempt; a client with its own retry loop should wait at
  least that long and keep its backoff growing while the calls are still shed. Other
  `UNAVAILABLE` answers (the shared store is not connected) and every other status carry no
  pushback, and health checks are never shed. Retrying fast costs the calls that run:
  [deploy.md](deploy.md#retries-and-shed-calls-issue-104) has the numbers.
- **Load balancing** across replicas: see [deploy.md](deploy.md#load-balancing).

## Metrics

The API's and ingest's metrics are on the ops port's `/metrics` (Prometheus text format), next
to `doewood_ready` and the others. gRPC-Web calls are counted like gRPC calls.

| metric | type | labels | what |
|---|---|---|---|
| `doewood_grpc_calls_total` | counter | `method`, `code` | calls that ended, by status code (`OK`, `INVALID_ARGUMENT`, `UNAVAILABLE`, ...); a call the client dropped before its end (a closed stream, a cancelled call) is `CANCELLED` |
| `doewood_grpc_call_duration_seconds` | histogram | `method` | until the answer for a unary call, the whole lifetime for a stream; buckets from 1 ms to 30 min |
| `doewood_grpc_calls_in_flight` | gauge | `method` | calls running now, open streams included |
| `doewood_grpc_shed_total` | counter | | calls refused at once because `max_calls_in_flight` was reached (also counted as `UNAVAILABLE`) |
| `doewood_grpc_connections_open` | gauge | | connections to the API port accepted and not yet closed |
| `doewood_grpc_connections_total` | counter | | connections accepted |
| `doewood_icons_requests_total` | counter | `outcome` | `GET /icons/{domain}`: `found`, `missing`, `invalid`, `busy` |
| `doewood_ingest_claimed_elsewhere_dropped_total` | counter | `path` | work dropped because another instance owns the source in the shared store (issue #25, [sources.md](sources.md#claims)): `queue`, objects of ingest's local queue (pending objects and their events) dropped instead of ingested; `webhook`, event notification records ignored |
| `doewood_ingest_finish_unmatched_total` | counter | | ingested objects whose status matched no `pending` row of that version, so it was not recorded: the object changed or was deleted meanwhile (issue #125). Near 0 in steady state |
| `doewood_ingest_known_copies_total` | counter | | objects recorded against content already stored, by their single-part MD5 ETag and size, without a GetObject (sources with `skip_known_copies` on, issue #131; [sources.md](sources.md#known-copies)) |
| `doewood_ingest_known_copy_bytes_total` | counter | | the listed bytes of those objects, not fetched |
| `doewood_store_wal_bytes` | gauge | | size of the instance store's WAL file (`doewood.sqlite-wal`); emptied when over 64 MiB or idle (issue #151, [configuration.md](configuration.md#the-instance-store)) |
| `doewood_store_wal_truncations_total` | counter | | checkpoints that emptied the instance store's WAL |
| `doewood_store_wal_truncations_busy_total` | counter | | such checkpoints that gave up after their 50 ms wait for a read or a write; the next try is 5 s later |

- `method` is the full method name, for example `doewood.v1.RecordsService/GetRecord` or
  `grpc.health.v1.Health/Check`. It comes from the fixed list of served methods; any other
  path is `other`, so the number of series stays bounded. A method or a code appears once it
  has been called.
- A call is counted when it ends; shed calls and calls cut by the server deadline are
  included, with their codes.
- Cost: a few atomic operations and one small allocation per call.
- Alerts worth setting: see [deploy.md](deploy.md#metrics-and-alerts).

## Capacity

Measured with `tests/api_load.rs` (issue #80) on 4 vCPU, a realistic mix of reads, streams
included: one instance serves about 4,500 calls/s on SQLite, reached at 10 clients (the
store's 2 read-only connections; about 5,200 since #108 opens one per CPU), and about 6,000
calls/s on Postgres, reached at about 100 clients (the pool of 16 connections, 4 per CPU since
#108; on 4 vCPU the process and the database then use all the cores). Beyond `max_calls_in_flight` the server sheds and the
calls it takes keep their latency. An idle connection costs about 13.5 KiB. Numbers, method and
caveats: [deploy.md](deploy.md#api-under-many-clients-issue-80) and
[deploy.md](deploy.md#api-read-capacity-issue-108).

## Examples with grpcurl

Reflection is on, so `grpcurl` needs no `.proto` files. `-plaintext`: the API speaks
cleartext HTTP/2 (put TLS in the ingress).

```sh
grpcurl -plaintext 127.0.0.1:7443 list
grpcurl -plaintext 127.0.0.1:7443 describe doewood.v1.ConfigService
grpcurl -plaintext 127.0.0.1:7443 grpc.health.v1.Health/Check
```

Add a source (MinIO's development login here; pass real keys from a file or a variable, not
the command line):

```sh
grpcurl -plaintext -d @ 127.0.0.1:7443 doewood.v1.ConfigService/AddSource <<'EOF'
{"endpoint": "http://127.0.0.1:9000", "bucket": "docs", "prefix": "in/",
 "credentials": {"access_key_id": "minioadmin", "secret_access_key": "minioadmin"},
 "scan_interval_seconds": 60}
EOF
```

The answer has the source's `id`, `"credentialsSet": true` and no credentials. Then:

```sh
ID=...   # the id from AddSource
grpcurl -plaintext 127.0.0.1:7443 doewood.v1.ConfigService/ListSources
grpcurl -plaintext -d "{\"source_id\": \"$ID\"}" 127.0.0.1:7443 doewood.v1.ConfigService/PauseSource
grpcurl -plaintext -d "{\"source_id\": \"$ID\"}" 127.0.0.1:7443 doewood.v1.ConfigService/ResumeSource
grpcurl -plaintext -d "{\"source_id\": \"$ID\", \"scan_interval_seconds\": 600}" \
    127.0.0.1:7443 doewood.v1.ConfigService/UpdateSource
grpcurl -plaintext -d "{\"source_id\": \"$ID\", \"policy\": {\"mode\": \"LLM_MODE_KINDS\",
    \"kinds\": [\"invoice\", \"contract\"], \"concurrency\": 2}}" \
    127.0.0.1:7443 doewood.v1.ConfigService/SetLlmPolicy
grpcurl -plaintext -d "{\"source_id\": \"$ID\"}" 127.0.0.1:7443 doewood.v1.ConfigService/RemoveSource
grpcurl -plaintext -d '{"endpoint": "http://minio:9000", "bucket": "docs", "prefix": "in/",
    "owner": "doewood-1"}' 127.0.0.1:7443 doewood.v1.ConfigService/ReleaseSourceClaim
```

An SQS inlet with credentials of its own (read from variables; they never come back):

```sh
grpcurl -plaintext -d @ 127.0.0.1:7443 doewood.v1.ConfigService/SetSqsInlet <<EOF
{"source_id": "$ID",
 "queue_url": "https://sqs.eu-central-1.amazonaws.com/123456789012/doewood-events",
 "credentials": {"access_key_id": "$SQS_ACCESS_KEY", "secret_access_key": "$SQS_SECRET_KEY"}}
EOF
grpcurl -plaintext -d "{\"source_id\": \"$ID\"}" 127.0.0.1:7443 doewood.v1.ConfigService/RemoveSqsInlet
```

An AI provider (the key is read from a variable, and never comes back):

```sh
grpcurl -plaintext -d @ 127.0.0.1:7443 doewood.v1.ConfigService/SetAiProvider <<EOF
{"preset": "anthropic", "model": "claude-haiku-5-5", "api_key": "$ANTHROPIC_API_KEY"}
EOF
grpcurl -plaintext -d @ 127.0.0.1:7443 doewood.v1.ConfigService/SetAiProvider <<'EOF'
{"name": "local", "preset": "openai_compatible", "model": "qwen2.5-7b-instruct",
 "base_url": "http://vllm:8000/v1", "make_default": true}
EOF
grpcurl -plaintext 127.0.0.1:7443 doewood.v1.ConfigService/ListAiProviders
grpcurl -plaintext -d '{"name": "local"}' 127.0.0.1:7443 doewood.v1.ConfigService/RemoveAiProvider
```

Ingest settings:

```sh
grpcurl -plaintext -d '{"similar_min_jaccard": 0.85}' \
    127.0.0.1:7443 doewood.v1.ConfigService/SetIngestSettings
grpcurl -plaintext 127.0.0.1:7443 doewood.v1.ConfigService/GetIngestSettings
```

Enrichment settings (the key is a placeholder; a key is never returned):

```sh
grpcurl -plaintext -d '{"companies_house_api_key": "<your key>",
    "edgar_contact": "Example Ops ops@example.invalid", "favicon_fallback": true}' \
    127.0.0.1:7443 doewood.v1.ConfigService/SetEnrichmentSettings
grpcurl -plaintext -d '{"clear_companies_house_api_key": true}' \
    127.0.0.1:7443 doewood.v1.ConfigService/SetEnrichmentSettings
grpcurl -plaintext 127.0.0.1:7443 doewood.v1.ConfigService/GetEnrichmentSettings
```

Progress, records, the graph and search:

```sh
grpcurl -plaintext -d '{}' 127.0.0.1:7443 doewood.v1.JobsService/WatchProgress   # Ctrl-C to stop
grpcurl -plaintext -d "{\"source_id\": \"$ID\", \"page_size\": 100}" \
    127.0.0.1:7443 doewood.v1.RecordsService/ListRecords
grpcurl -plaintext -d '{"blake3": "<64 hex digits>"}' 127.0.0.1:7443 doewood.v1.RecordsService/GetRecord
grpcurl -plaintext -d '{"key": {"kind": "NODE_KIND_COMPANY", "key": "acme"}}' \
    127.0.0.1:7443 doewood.v1.GraphService/GetNode
grpcurl -plaintext -d '{"node_id": "<uuid>", "direction": "DIRECTION_IN", "edge_kinds": ["EDGE_KIND_PARTY"]}' \
    127.0.0.1:7443 doewood.v1.GraphService/Neighbours
grpcurl -plaintext -d '{"company_id": "<uuid>", "page_size": 1000}' 127.0.0.1:7443 doewood.v1.GraphService/CompanyDocuments
grpcurl -plaintext -d '{"company_id": "<uuid>", "page_size": 1000, "page_token": "<next_page_token>"}' \
    127.0.0.1:7443 doewood.v1.GraphService/CompanyDocuments
grpcurl -plaintext -d '{"company_id": "<uuid>", "page_size": 20}' 127.0.0.1:7443 doewood.v1.GraphService/ListMerges
grpcurl -plaintext -d '{"merge_id": "<uuid>", "dismiss": true, "reason": "not the same company"}' \
    127.0.0.1:7443 doewood.v1.GraphService/UndoMerge
grpcurl -plaintext -d '{"company_id": "<uuid>", "name": "ACME Trading", "document_ids": ["<uuid>"]}' \
    127.0.0.1:7443 doewood.v1.GraphService/SplitCompany
grpcurl -plaintext -d '{"state": "PROPOSAL_STATE_OPEN", "kind": "PROPOSAL_KIND_MERGE", "page_size": 20}' \
    127.0.0.1:7443 doewood.v1.GraphService/ListProposals
grpcurl -plaintext -d '{"proposal_id": "<uuid>", "note": "same company"}' \
    127.0.0.1:7443 doewood.v1.GraphService/AcceptProposal
grpcurl -plaintext -d '{"proposal_id": "<uuid>", "dismiss": true, "note": "not the same company"}' \
    127.0.0.1:7443 doewood.v1.GraphService/RejectProposal
grpcurl -plaintext -d '{"text": "invoice", "kind": "invoice", "date_from": "2026-01-01", "amount_min": 100, "limit": 20}' \
    127.0.0.1:7443 doewood.v1.SearchService/Search
```

The explorer's reads (issue #172):

```sh
# Companies with the most documents; then the next page.
grpcurl -plaintext -d '{"order": "COMPANY_ORDER_DOCUMENTS", "page_size": 100}' \
    127.0.0.1:7443 doewood.v1.GraphService/ListCompanies
grpcurl -plaintext -d '{"order": "COMPANY_ORDER_DOCUMENTS", "page_size": 100, "page_token": "<next_page_token>"}' \
    127.0.0.1:7443 doewood.v1.GraphService/ListCompanies
# British companies whose name starts with "acme", enriched and with an identifier, by name.
grpcurl -plaintext -d '{"filter": {"name_prefix": "acme", "jurisdiction": "GB", "has_identifier": true, "enriched": true}, "order": "COMPANY_ORDER_NAME"}' \
    127.0.0.1:7443 doewood.v1.GraphService/ListCompanies
# The company graph: the 500 companies with the most documents, links of 3 shared documents or more.
grpcurl -plaintext -d '{"min_weight": 3, "limit": 500, "max_links": 5000}' \
    127.0.0.1:7443 doewood.v1.GraphService/CompanyGraph
# Two levels around a company, companies and documents only, edges that held on 2022-05-01.
grpcurl -plaintext -d '{"seed_ids": ["<uuid>"], "depth": 2, "node_kinds": ["NODE_KIND_COMPANY", "NODE_KIND_DOCUMENT"], "as_of": "2022-05-01", "limit": 300}' \
    127.0.0.1:7443 doewood.v1.GraphService/Subgraph
# Who owned whom: the shortest ownership paths between two companies on a day.
grpcurl -plaintext -d '{"from_id": "<uuid>", "to_id": "<uuid>", "max_depth": 6, "edge_kinds": ["EDGE_KIND_PARENT_OF", "EDGE_KIND_SUBSIDIARY_OF", "EDGE_KIND_ACQUIRED"], "as_of": "2022-05-01", "max_paths": 3}' \
    127.0.0.1:7443 doewood.v1.GraphService/Path
```

An answer that a cap ended says so (`"truncated": true`); proto3 JSON leaves out a `false`.

A validation error lists every field:

```console
$ grpcurl -plaintext -d '{"endpoint": "http://user:pw@minio:9000", "bucket": "B!"}' \
    127.0.0.1:7443 doewood.v1.ConfigService/AddSource
ERROR:
  Code: InvalidArgument
  Message: invalid argument: endpoint: must not contain credentials (user:password@); bucket: must be 3 to 255 ASCII letters, digits, '.', '-' or '_', starting with a letter or digit
```

## gRPC-Web

A browser uses a gRPC-Web client (`@connectrpc/connect-web`, `grpc-web`, or generated code from
the `.proto` files) against the same address. On the wire it is a plain HTTP/1.1 POST:

```sh
# An empty ListAiProvidersRequest: one frame, flag 0, length 0.
printf '\x00\x00\x00\x00\x00' | curl -s --http1.1 -X POST \
    -H 'Content-Type: application/grpc-web+proto' -H 'X-Grpc-Web: 1' --data-binary @- \
    http://127.0.0.1:7443/doewood.v1.ConfigService/ListAiProviders | xxd | head
```

The body holds the message frame and then a trailer frame with `grpc-status:0`.

## Event webhook

MinIO and S3 event notifications go to the **ops** server (`DOEWOOD_OPS_LISTEN`, default
`127.0.0.1:7444`), one URL per source:

```
POST /events/s3/{source_id}
```

The body is the notification JSON (at most 1 MiB). Each record is compared with what the source
has seen, and new, changed or removed objects become the same events a scan emits; the scan
still catches anything a notification misses. The answer is `200 {"emitted": n, "ignored": m}`
(ignored: another bucket, outside the prefix, already seen, the source is paused, or another
instance owns it, see [sources](sources.md#claims)), `400` for a
body that is not a notification, `404` for an unknown source, `413` for a body over 1 MiB, and
`503` before the store is open and during shutdown. Like the API, it has no authentication in
v1: keep the ops port inside the cluster.

With MinIO (`mc`), for example:

```sh
mc admin config set local notify_webhook:doewood endpoint="http://doewood:7444/events/s3/$ID"
mc admin service restart local
mc event add local/docs arn:minio:sqs::doewood:webhook --event put,delete --prefix in/
```

## Company icons

```
GET /icons/{domain}
```

The icon of a company's web site, fetched and checked by doewood and served from the API port,
so a frontend on the same origin uses it as `<img src="/icons/acme.com">` and no third party
learns which companies a user looks at. doewood always proxies the icon; it never redirects
the browser to the site or to a favicon service. How the icon is found, the SSRF guard and the
content checks are in [favicons.md](favicons.md). Like the API, it has no authentication in v1.

`{domain}` is a bare host name (`acme.com`, `bücher.de`, percent-encoded or not), without a
scheme, port, path or user info; an IP address is refused.

| status | when | body | `Content-Type` | `Cache-Control` |
|---|---|---|---|---|
| `200` | an icon that passed the checks | the icon | from its magic bytes: `image/x-icon`, `image/png`, `image/gif`, `image/jpeg`, `image/webp` or `image/avif` (never the upstream one, never SVG) | `private, max-age=86400` |
| `404` | no usable icon (also cached by doewood for 6 hours) | empty | `text/plain; charset=utf-8` | `private, max-age=21600` |
| `400` | the domain is not valid | the reason | `text/plain; charset=utf-8` | `no-store` |
| `503` | too many lookups at once (16); `Retry-After: 5` | empty | `text/plain; charset=utf-8` | `no-store` |

Every answer also has:

```
X-Content-Type-Options: nosniff
Content-Security-Policy: default-src 'none'; sandbox
Cross-Origin-Resource-Policy: same-origin
Referrer-Policy: no-referrer
```

A first lookup can take up to 12 s (the site, then the fallback when it is on); later ones
come from the cache.

```sh
curl -si http://127.0.0.1:7443/icons/acme.com -o icon.bin
```

## Scripts without a server

`doewood dev add-source` (hidden) adds a source through the same code as `AddSource` (the same
validation and claim), without a running server; `serve` then scans it. See
[sources.md](sources.md#trying-it-locally).

## ListRecords on Postgres

`ListRecords` is one query: the records with a location in the source (`EXISTS` over
`locations`), after the page token, ordered by hash. Postgres runs it as a merge semi join over
`records_pkey` and `locations_by_blake3`. It does not carry the `after` bound over to the
`locations` side, so before issue #13 each page read every location before the page token, and
walking all pages of a source was quadratic. The bound is now in the `EXISTS` as well. The
results are the same (`list_records_matches_the_query_before_issue_13` in
`crates/doewood-pg/src/tests/list_records.rs` walks both queries over six filters and four page
sizes).

```
DOEWOOD_TEST_POSTGRES_URL=postgres://postgres:pw@127.0.0.1:5432/postgres DOEWOOD_BENCH_RECORDS=50000 \
  cargo test -p doewood-pg list_records_timings -- --ignored --nocapture
```

The data is generated: each record has about 1 KB of JSON and one location, in one of four
buckets (`b0` to `b3`). Every 5th record is also in `b4`, every 7th has two keys in its
bucket, every 13th is on another endpoint and every 11th has no location. The query runs
against bucket `b0`. Times are medians of 15 runs (5 for full walks), in ms, measured from the
client. The setup: `pgvector/pgvector:pg17` (17.12) in Docker on the 4-core server, test
profile, commit `fa9f72f` plus this change.

| case | 5,000 before | 5,000 after | 50,000 before | 50,000 after |
|---|---|---|---|---|
| bucket, first page (50) | 2.06 | 1.95 | 1.02 | 0.98 |
| bucket, page halfway (50) | 2.38 | 1.24 | 13.30 | 1.17 |
| bucket, 1000 per page | 9.61 | 10.07 | 15.72 | 13.67 |
| prefix with 5 %, first page | 1.51 | 1.72 | 1.72 | 1.61 |
| rare prefix (49 records) | 2.03 | 2.22 | 12.53 | 13.07 |
| prefix matching nothing | 1.37 | 0.89 | 9.80 | 9.44 |
| all pages of the bucket, 50 each | 44.0 | 31.5 | 2,793 | 341 |
| all pages of the bucket, 1000 each | 10.3 | 11.2 | 241 | 184 |
| all pages of the 5 % prefix, 50 each | 9.8 | 9.6 | 628 | 203 |

With 50,000 records, the page halfway through read 32,024 buffers before and 485 after: the
`locations` index scan starts at the page token now.

A prefix that matches few or no keys used to get a sequential scan of `locations`, which grows
with the table (about 10 ms at 62,597 locations): the default collation does not let
`starts_with` use the `(endpoint, bucket, object_key)` index. Shared schema 3 (issue #27) adds
`locations_by_key_c` on `(endpoint, bucket, object_key COLLATE "C")`, and the query matches the
prefix as `starts_with(l.object_key COLLATE "C", $3)`. Postgres (15 and newer) turns that into
a range on the index (`object_key >= 'dept0/2026/file-40' AND object_key < 'dept0/2026/file-41'`)
and keeps `starts_with` as the filter, so the results are the same; the comparison test above
still passes. The same index also serves the old query and the source filter of search.

The same test at 50,000 records, run on commit `fb0db75` (schema 2) and on this change
(schema 3), same setup. "before #13" and "now" are the two queries the test times; the bold
rows are the ones the index is for.

| case | 50,000, schema 2, before #13 | 50,000, schema 2, now | 50,000, schema 3, before #13 | 50,000, schema 3, now |
|---|---|---|---|---|
| bucket, first page (50) | 1.08 | 1.14 | 1.19 | 1.14 |
| bucket, page halfway (50) | 15.11 | 1.23 | 11.53 | 1.12 |
| bucket, 1000 per page | 16.06 | 11.75 | 12.55 | 9.59 |
| prefix with 5 %, first page | 1.52 | 1.54 | 1.58 | 1.83 |
| **rare prefix (49 records)** | 13.20 | **13.35** | 1.48 | **0.96** |
| **prefix matching nothing** | 8.29 | **8.88** | 0.54 | **0.52** |
| all pages of the bucket, 50 each | 2,628 | 339 | 2,548 | 360 |
| all pages of the bucket, 1000 each | 256 | 181 | 271 | 192 |
| all pages of the 5 % prefix, 50 each | 527 | 164 | 351 | 157 |

The rare prefix now reads 227 buffers instead of 1,393. At 5,000 records the prefix cases were
already 1 to 2 ms and stay there. Every shared instance must run schema 3 together
([configuration.md](configuration.md#the-shared-postgres)).

Standalone SQLite probes `locations` once per record, but `?4 IS NULL OR r.blake3 > ?4` kept
it from starting at the page token, so it too read every record before the page. It now
compares with `coalesce(?4, x'')` and searches the primary key from the token. In a trial with
Python's sqlite3 (3.46.1, in memory, 50,000 records, the same query), walking a bucket 50 per
page took 471 ms before and 69 ms after, with the same results.

## ListRecords page budget

Before issue #111 a `ListRecords` page was only bounded by its size (up to 1000 records), so a
page of big records was read into memory in full, several times over (the rows, the records,
the reply), and only then met the 64 MiB message cap. Now `RecordStore::list_records` takes a
byte budget and stops reading once the bodies reach it: SQLite reads row by row, Postgres reads
through a portal in batches (8 rows, then about what still fits), so neither the server nor
doewood holds the rest. The API's budget is `RECORDS_PAGE_BYTES`, 3 MiB of record bodies (not
configurable). The conformance test (`records_by_source`) and
`list_records_pages_end_at_the_byte_budget` in `crates/doewood-api/tests/api.rs` walk pages of
varied sizes and budgets on both backends and see every record exactly once.

```
DOEWOOD_TEST_POSTGRES_URL=... MALLOC_MMAP_THRESHOLD_=65536 \
  cargo test -p doewood-api --test records_memory -- --ignored --nocapture
```

The test (`tests/records_memory.rs`) asks for `page_size` 1000 once, then walks every page,
and prints the peak RSS above the RSS before each (server and client in one process; glibc
told to map every allocation above 64 KiB, so freed buffers leave the RSS). Generated records
in one source; `pgvector/pgvector:pg17` in Docker on the 4-core server, test profile, one run
each, `origin/main` at `77524d9` before and this change after.

| case | before: one page | after: one page | before: all pages | after: all pages |
|---|---|---|---|---|
| SQLite, 150 records of 256 KiB | 150 records, +75.9 MiB, 217 ms | 12 records, +6.3 MiB, 25 ms | 1 page, +75.8 MiB, 211 ms | 13 pages, +6.3 MiB, 330 ms |
| Postgres, 150 records of 256 KiB | 150 records, +101.1 MiB, 1,651 ms | 12 records, +5.8 MiB, 180 ms | 1 page, +101.5 MiB, 1,622 ms | 13 pages, +7.0 MiB, 1,992 ms |
| SQLite, 20,000 records of 1 KiB | 1000 records, +2.4 MiB, 32 ms | 1000 records, +3.5 MiB, 22 ms | 21 pages, +4.9 MiB, 687 ms | 21 pages, +4.1 MiB, 518 ms |
| Postgres, 20,000 records of 1 KiB | 1000 records, +2.9 MiB, 80 ms | 1000 records, +1.1 MiB, 71 ms | 21 pages, +4.4 MiB, 1,450 ms | 21 pages, +8.3 MiB, 1,388 ms |

The peak of a page of big records drops from two to 2.7 times the whole page's 37.5 MiB of
bodies to about twice the 3 MiB budget. Walking big records takes more pages, so more
round trips: 20 to 60 % longer in all. Small records page as before.

## Pages of Neighbours and CompanyDocuments

Before issue #112 `Neighbours` and `CompanyDocuments` returned every item in one response: for
a company with 12,504 documents, 150 ms and every node in memory at once (docs/deploy.md,
issue #127). They now page with ListRecords' size, maximum and byte budget
(`RECORDS_PAGE_BYTES`, 3 MiB), through `GraphStore::neighbours_page` and
`GraphStore::company_documents_page`.

The token is a place in the order, not an offset: `<edge kind>.<Julian day of valid_from,
empty when undated>.<edge id>` of the page's last edge (opaque to clients; a CompanyDocuments
token is always a `party` edge). Schema 12 (SQLite) and 10 (Postgres) rebuild `edges_by_dst`
as `(dst, kind, valid_from with undated lowest, id)`, the order of a node's incoming edges, so
a page reads the index from its token and stops at its size:

- A page is a `UNION ALL` of index ranges, each limited to the page size: per direction, and
  after a token one range for the rest of the token's kind (on SQLite two: the rest of its
  day, then later days, since SQLite starts no scan at a row value holding an expression)
  and one for the kinds after it. The merge sorts at most a page per range. Outgoing edges use
  `edges_by_src (src, kind)` as before and are sorted per page: a node's outgoing edges are
  few (ingest caps `similar_to` at 100 a document), so that index was left narrow.
- CompanyDocuments reads the company's party edges in that order and keeps an edge only when
  it is its document's first live party edge to the company (an index probe per edge, no
  `DISTINCT` over the company), which is why it is ordered by that edge rather than by key.
- Postgres reads a page through a portal 64 rows at a time, so it stops at the byte budget
  as ListRecords does; SQLite reads row by row. Edge kinds sort in the database's collation on
  Postgres (before, `COLLATE "C"`), which orders the 14 kind names the same in any collation
  that orders lowercase letters alphabetically.
- The plan tests (`graph_pages_start_at_their_token` in `doewood-store` and `doewood-pg`)
  assert the ranges and that nothing sorts more than a page; `graph_pages_match_the_queries_before`
  walks pages of 7 and 1000 against the queries before #112 (the same neighbours in the same
  order; the same documents, once each); the conformance case `graph_pages` walks every
  direction and filter at several sizes and byte budgets on both backends.

Measured with `api_load` (release build, 10 s a step, `DOEWOOD_BENCH_DOCS=12000
DOEWOOD_BENCH_COMPANIES=1 DOEWOOD_BENCH_ONLY=CompanyDocuments DOEWOOD_BENCH_IDLE=0
DOEWOOD_BENCH_WATCH_PCT=0`, one company with all 12,000 documents, the #127 shape;
`pgvector/pgvector:pg17` on an internal Docker network; 4-core server, other agents' builds
queued behind the same lock). Before: `staging` at `72b459c`, every document in each
response; after: the default first page of 50. RSS is the whole test process (server and
clients), its peak during the step.

| | p50 / p99, 1 client | peak RSS | p50 / p99, 10 clients | calls/s, 10 clients | peak RSS |
|---|---|---|---|---|---|
| SQLite, before | 57.7 / 81.0 ms | 98 MiB | 170 / 238 ms | 58 | 140 MiB |
| SQLite, after | 0.42 / 1.17 ms | 38 MiB | 1.8 / 5.1 ms | 5,116 | 40 MiB |
| Postgres, before | 48.5 / 78.6 ms | 68 MiB | 111 / 178 ms | 90 | 68 MiB |
| Postgres, after | 1.24 / 1.98 ms | 18 MiB | 1.6 / 3.3 ms | 5,862 | 20 MiB |

A client that needs every document now walks 12 pages of 1000 (or 240 of 50), and then the
empty page that a full last page's token leads to; each page costs its own size, so the walk
is linear.

## The explorer's reads

Issue #172: `ListCompanies`, `CompanyGraph`, `Subgraph` and `Path`, for a visual explorer
(`doewood_model::explore`, the stores' `explore.rs`).

**Counts kept, not computed per read.** Ordering 20,000 companies by documents, or the pairs
of companies by shared documents, would aggregate every `party` edge on each call: on 200,000
documents (600,000 party edges) 0.5 s for the counts and 0.8 s for the pairs on Postgres even
with covering indexes. Instead the counts are kept in `company_stats` (a company's documents
and degree; a row per company, made with it) and `company_links` (each pair `a < b` with
shared documents, corporate events and each side's roles), shared schema 12 and instance
schema 14:

- **Writers do not touch them.** A trigger on `edges` (Postgres: statement triggers, one
  `INSERT` per statement) appends each change to a live `party` or corporate-event edge to
  `company_graph_log`: +1 when an edge is written live or becomes live, -1 when it stops or
  is deleted (a cascade from a deleted node too), nothing for an upsert that rewrites an edge
  as it was. The log has no unique key that two writers could wait on, so ingest's
  concurrency is unchanged.
- **A fold moves the log into the counts** (`GraphStore::fold_company_graph`): the nodes of
  the first 1000 log rows, all their log rows, and their live edges now, read in one snapshot
  (one statement on Postgres, the writer's transaction on SQLite). What was counted for a node
  is its edges now minus its logged changes, so the counts move by the difference; then the
  rows are deleted. Folds take a transaction-scoped advisory lock (Postgres) that writers
  never take, so two instances never fold the same rows. Each instance folds every 5 s in the
  background (`cleanup::start_fold`), and `ListCompanies` and `CompanyGraph` fold first (at
  most 20 batches; past that they answer with `stale: true`). After a bulk load the first
  fold counts everything; the upgrade to these schemas logs every live edge, so the first fold
  after it builds the counts.
- **The document total** (issue #181; `company_graph_totals`, one row, shared schema 13 and
  instance schema 15) is kept by the same fold: a batch's documents that gained their first
  live party edge add one, those that lost their last take one away. The upgrade counts it
  once from the edges less the log rows not yet folded (on Postgres under the folds' lock),
  so the folds after it keep it exact.
- A document with more than 32 distinct parties links none of them (it still counts for
  each), which also bounds a fold's work per document to 496 pairs. A pair keeps at most 16
  role counts.
- The tests compare the folded counts with a count from scratch on generated data with
  retractions, deletions and moved edges (`folded_counts_match_a_count_from_scratch`, both
  backends; on Postgres also with two writers and two folders racing; the document total
  too, also when the upgrade to it finds log rows not yet folded), and the conformance case
  `explorer` checks them through a merge.

**Reads.** `ListCompanies` reads `company_stats` in `(documents, id)` or `(degree, id)` index
order, or `nodes_company_name` (the lowercased name, partial on companies) for the name
order, from its token, and checks the filters on the companies in order. `CompanyGraph` is
that listing's first `limit` companies, then one indexed lookup of the links among them
(strongest first, cut at `max_links`) and one statement for their corporate events.
`Subgraph` and `Path` read a level of the walk in one statement (`neighbours_of_many`:
Postgres runs the `Neighbours` page query for each node of the level in a lateral join;
SQLite runs it per node on one read connection), each node capped at its share of the
level's edge budget.

Shared schema 12 and instance schema 14 add the tables, triggers and the name index; older
instances refuse the shared schema, so every instance is upgraded together. The upgrade logs
every live party and corporate edge (at 200,000 documents about 680,000 rows), and the
background fold counts them in under half a minute (below).

**Measured** (issue #172; `explorer_timings` in `doewood-store` and `doewood-pg`, release
build, `cargo test --release -p doewood-store --lib explorer_timings -- --ignored
--nocapture`, and the same in `doewood-pg` with `DOEWOOD_TEST_POSTGRES_URL`): 200,000
documents with three `party` edges each (buyer and supplier skewed to a few hubs, signatory
uniform), 20,000 companies, 2,000 `parent_of` edges, then one party edge in 13 retracted,
one in 29 deleted (and on Postgres one in 31 moved); `pgvector/pgvector:pg17` in a container
on an internal Docker network; 4-core server, median of 9 runs after a warm-up.

| read | SQLite | Postgres |
|---|---|---|
| first fold of the whole log (666k / 681k rows) | 17.2 s | 24.3 s |
| CompanyGraph, defaults (500 companies, 5,000 links) | 33.3 ms | 51.1 ms |
| CompanyGraph, maximum (2,000 companies, 20,000 links) | 99.1 ms | 117.9 ms |
| ListCompanies, first page of 50, by documents / degree / name | 0.3 / 0.3 / 0.3 ms | 1.0 / 1.0 / 1.1 ms |
| ListCompanies, page of 50 after 10,000 companies | 0.3 ms | 1.1 ms |
| Subgraph from the biggest hub, depth 2, defaults (truncated at 200 nodes) | 8.6 ms | 8.8 ms |
| Path between two small companies, depth 4 (found, 4 edges) | 4.3 ms | 3.5 ms |

**Cost to ingest** (the triggers are on the write path). `throughput` (`crates/doewood-ingest/tests/pipeline.rs`,
release, `DOEWOOD_BENCH_FILES=2000`), `staging` at `f38f6b6` (before) against this change
(after), runs interleaved before/after, through `serial`, Postgres in a `--rm` container on an
internal network; files/s, median of 3 (the runs in brackets):

| store | workers | before | after |
|---|---|---|---|
| Postgres | 1 | 98 (97, 98, 102) | 99 (99, 103, 96) |
| Postgres | 4 | 440 (450, 440, 433) | 430 (430, 425, 438) |
| SQLite | 1 | 160 (167, 148, 160) | 167 (178, 157, 167) |
| SQLite | 4 | 582 (582, 543, 601) | 577 (548, 577, 604) |

The differences are within run-to-run noise (-2.3 % to +4 %). After a run the log holds 500
rows (0.16 to 0.21 MiB on Postgres with its index, 0.08 MiB on SQLite), folded in 0.01 s.
That mix has few parties (a quarter of a `party` edge a file), so the triggers alone were also
timed on Postgres with party-heavy writes: one `INSERT ... ON CONFLICT` of a document's three
party edges per transaction, triggers off and on, interleaved, 5,000 documents a run, median
of 6: new edges 683 vs 729 µs a document (+46 µs, three log rows of about 125 bytes each
with the index), rewrites of the same edges (nothing logged) 643 vs 671 µs. At three parties
a document that is about 0.5 % of a file's worker time in `throughput` on Postgres. The
background fold keeps up with far more than ingest writes: 28,000 log rows a second on
Postgres and 39,000 on SQLite (the first-fold row above).

Before the counts were kept, the same aggregation per read took 540 ms for the document
counts and 650 to 770 ms for the pairs on Postgres alone. Two query plans needed care: on
SQLite the listing joins `company_stats` first (`CROSS JOIN`; left to the planner it read
every company by kind and sorted them: 13 ms a page), and on Postgres the filters on props
sit behind a lateral `OFFSET 0` fence (the planner cannot estimate them and hash-joined
every company: 33 ms a page). SQLite reads a CompanyGraph's links per company as a key range
or as point probes, whichever is fewer (as one `IN` probe of every pair it took 1.0 s at
2,000 companies).


## Search by company

`Search` with a company checked each candidate document with a correlated `EXISTS` (its node by
`encode(blake3, 'hex')`, then its party edges). Without words every indexed document is a
candidate, so the cost grew with the index, not with the company. Since issue #111 a company
with few documents drives the query instead: its party edges (`edges_by_dst`), each document's
hash looked up by the `search_index` primary key. The same rows come back in the same order
(`search_by_company_matches_the_query_before` in `crates/doewood-pg/src/tests/search_company.rs`
and `crates/doewood-store/src/standalone/search_company.rs`: 5 companies, 5 texts, kind and
date filters and 4 pages, against the query before, and through `search` itself).

For a company on a large share of the documents (the instance's owner is on every one) the
check per document is faster on Postgres: it runs in rank order and stops when the page is
full, while the edges are all read. So Postgres first counts the company's party edges, up to
a quarter of the indexed documents (`pg_class.reltuples`, at least 1000), and keeps the check
per document when the count reaches that. SQLite drives from the edges always: its check per
document was slower in every case measured.

```
DOEWOOD_TEST_POSTGRES_URL=... DOEWOOD_BENCH_DOCS=50000 \
  cargo test -p doewood-pg search_by_company_timings -- --ignored --nocapture
DOEWOOD_BENCH_DOCS=50000 \
  cargo test -p doewood-store --lib search_by_company_timings -- --ignored --nocapture
```

50,000 generated documents (47,059 indexed), 5,000 companies of about 10 documents each, and
company 0 on half of the documents; "a word" is in a third of the titles. Medians of 9 runs of
the query, in ms; `EXPLAIN (ANALYZE, BUFFERS)` from the same test. Same setup as above; SQLite
in a file on disk.

| case | Postgres before | Postgres after (from the edges) | `PgStore::search` | SQLite before | SQLite after |
|---|---|---|---|---|---|
| 10 documents, no words | 9.30 (5,927 buffers) | 0.63 (80 buffers) | 1.71 (from the edges) | 93.86 | 0.06 |
| 10 documents, a word | 13.54 (5,927 buffers) | 0.63 (80 buffers) | 0.86 (from the edges) | 42.40 | 4.74 |
| half of all, no words | 28.27 | 48.64 | 27.64 (per document) | 107.96 | 67.06 |
| half of all, a word | 20.23 | 45.90 | 25.96 (per document) | 51.20 | 45.83 |

Before, Postgres sorted the whole index (in parallel) and probed each document's edges; after,
for the small company, it reads its 10 edges and 10 rows. The first attempt guarded the key
with a regular expression (`~ '^[0-9a-f]{64}$'`), which alone cost 150 ms on 25,000 keys; the
guard is now `starts_with(d.key, 'ref:')`, since document keys are validated (lowercase hex or
a `ref:` placeholder).

## Search on words alone

A search with words and no other filter (issue #140) used to join every match to its
`search_index` row and sort them all by score, date and hash before taking the page, so a
word in a quarter of the documents cost a row read per match. SQLite now scores the matches
on the FTS index alone, takes the score of the last hit the page can reach (the
`limit + offset`-th best) as a cut, and joins and sorts only the matches scoring at least
that: every match below the cut has at least `limit + offset` matches ahead of it, so the page
is the same, ties included (`text_search_matches_the_query_before` in
`crates/doewood-store/src/standalone/search_scale.rs` compares every page of 9 texts and 9
pages on two generated corpora with the query before). Searches with a filter, and Postgres
(whose rank and sort keys are in the same heap row), run as before. Every match is still
scored: the ranking (`bm25` on SQLite, `ts_rank` on Postgres) needs all of them for an exact
page. See [deploy.md](deploy.md#search-on-a-big-corpus-issue-140) for the measurements.

## Tests

`cargo test -p doewood-api` starts in-process servers and calls every RPC with a tonic client:
the happy paths, validation errors, two instances sharing one backend (the claim conflict),
secrets never coming back (SQS inlet credentials included), the progress stream, a gRPC-Web
request over HTTP/1.1, health, reflection and the message limit. `tests/icons.rs` checks the icon route's answers and
headers with a fake resolver. `tests/api_load.rs` (ignored) is the load benchmark (see
[Capacity](#capacity)). With `DOEWOOD_TEST_POSTGRES_URL` set, the claim, records,
graph, merge-log and search tests also run against Postgres.
