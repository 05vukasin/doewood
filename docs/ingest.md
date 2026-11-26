# Ingest

Ingest (`crates/doewood-ingest`, step 4 of [ADR 0001](adr/0001-architecture.md)) turns every
object discovery finds into a record, graph nodes and edges, and a search entry:

```
scanner / webhook events ─┐
                          ├─▶ dispatcher ─▶ bounded queue ─▶ workers: GetObject ─▶ analyze ─▶ store
pending sweep (objects_seen)┘                                         (cap)        quail      record
                                                                                   hedgehog   graph
                                                                                              search
```

## The queue

- **Durable.** The work is the `objects_seen` rows with status `pending`. Events only make ingest
  look sooner. A sweep feeds every pending object at start, every 30 s, and after a reprocess
  request, so a restart, a row whose event was never sent, or a source added while ingest was
  busy are all picked up.
- **Event order.** The scanner (per chunk) and the webhook and SQS inlets (per notification)
  commit a new or changed object's `pending` row before they send its event, so a worker always
  finds the row it finishes. Deletes are sent before their commit (they leave no pending row
  for the sweep). Until issue #125 every event went first: a worker that finished before the
  commit matched no row, its result was dropped, and the next sweep ingested the file again
  (272 of 300 files in the `a_scan_pass_ingests_each_object_once` test, with a 16-event
  channel; 0 of 3000 with the service's 1024-event channel and in-memory fetches). A finish
  that matches no pending row of its version (the object changed or was deleted meanwhile) is
  counted in `doewood_ingest_finish_unmatched_total`.
- **Bounded, with backpressure.** The dispatcher hands jobs to the workers through a queue of
  twice the workers and waits while it is full. It then stops reading the scanners' event channel
  (1024 events), and the scanners wait in turn.
- **Workers:** `DOEWOOD_INGEST_JOBS`, else one per CPU, at most what half the available memory
  holds at 512 MiB a worker (a cgroup v2 limit counts), at most 32. Every analysis runs on a
  blocking thread; the quail engine is shared. Big files also wait for the
  [memory budget](#memory). A spreadsheet may borrow idle workers' CPUs
  ([Memory](#memory), Threads).
- **Shutdown:** each worker finishes the file in hand; the rest stays pending for the next start.

## Fetch

`GetObject` with the source's credentials, streamed under its size cap (512 MiB by default). An
object over the cap fails without a retry.

**Known copies** (issue #131, off by default, per source: [sources.md](sources.md#known-copies)).
A GetObject response with a single-part ETag and no SSE-KMS or SSE-C records `(ETag, size) ->
BLAKE3` with the object's final status, in the same transaction. On a source with
`skip_known_copies` on, the status read before the fetch also looks up the listed ETag and size.
When they match, a HEAD confirms the ETag, the size and the SSE type, and the stored record is
read and checked against the running step versions. The worker then writes the location, graph
and search as for any reused record, and the status and outcome are the record's. No GetObject,
no hash and no analysis. Anything that does not match falls back to the normal fetch. The file's
share of the [memory budget](#memory) is still taken by its listed size before the worker
starts it, and freed when it is through. A HEAD is a short wait, not a download.

## Analyze

- **One engine per instance**, built like the `quail` CLI builds its own
  (`quail_cli::registry::builder`: `quail-cli` is a library too, so no quail change was needed).
  The file timeout is quail's (2 min); see [Time limits](#time-limits).
- **Routing** comes from the bytes: quail's sniff (`engine.sniff`), then a bounded look for the
  spreadsheet containers quail calls a plain ZIP or OLE file. XLSX/XLSM, XLSB, ODS and XLS go to
  hedgehog; PDF, DOCX and text (CSV included: quail reads text) to quail. An object key's
  extension only decides when the bytes do not.
- **Spreadsheets:** quail's identity layer (hashes, sniffed format), then hedgehog
  (`hh_core::reader::open_bytes_with` and its normalizer) as `sheet.*` facts: sheets, cells,
  formulas, tables with their columns, and the first text cells for search. The record has
  quail's top-level shape, with the spreadsheet format as `format`. Every hedgehog call runs under
  `catch_unwind` (calamine's XLSB reader has known panics), and the release profile unwinds.
- **Everything else** (images, archives, unknown files) keeps quail's generic record, identity and
  format only, and ends `skipped`.
- **OCR** (cargo feature `ocr`, on in the binary): with the model files in `DOEWOOD_OCR_MODELS`
  (default `<data>/ocr-models`), the engine runs under quail's `deep` profile and OCRs the pages
  that need it, with quail's budgets (pages read at once from the memory, or
  `DOEWOOD_OCR_JOBS`; 45 s a page, the file timeout extended for them). Without the models it
  logs one `WARN`, runs `standard`, and scanned pages are flagged `needs_ocr`.
  `--no-default-features` builds without ONNX Runtime.
- **Content already stored** with the current step versions is not analyzed again: a second
  copy only adds its location.

## Time limits

- **Hashed once.** Each fetched file is hashed (BLAKE3) once, on a blocking thread, before the
  record lookup; the analyzer takes that hash (`Analyzer::analyze_hashed`). quail's
  `hash.identity` still hashes for its own record.
- **One deadline per file**, the file timeout from when the analysis starts. quail's engine
  starts its clock there (it does not start a step past it, and its steps check it as they
  go); hedgehog's read (`ReadOptions.limits.deadline`, checked per sheet and every 65,536 cells)
  and its normalizer (`NormalizeOptions.deadline`) get the same instant. A spreadsheet takes at
  most the file timeout, not quail's plus hedgehog's. A read past it fails the file
  (`sheet.read@1: file refused: reading took longer than the time limit`); tables past it are
  a problem of a partial record. The cell cap is in [Memory](#memory); decompressed bytes keep
  hedgehog's limit (2 GiB, read in chunks, not held). The fetch is not under it: the
  S3 read timeout bounds a stalled transfer, and a large object takes as long as it takes.
- **The watchdog.** A step that does not stop on time (a native call that never checks) is
  backed up by a watchdog of the file's time cap (the timeout, or more with OCR) plus 1 minute
  (`IngestConfig::watchdog_grace`). Past it the file ends `failed` with `analysis did not finish
  within <n>s`, without a retry, as before, and the worker takes the next file.
- **What a timed-out analysis still holds.** Nothing can stop a thread from outside, so the
  analysis runs on until its step returns; then its file's bytes are freed, and one `WARN`
  says how long it ran (its result is discarded: the file stays `failed`). A worker leaves at
  most one such analysis behind (`MAX_OVERRUNS`); after a second it waits for the older one to
  end before it takes another file. So stuck analyses hold at most two files per worker instead
  of piling up, and a step that never returns costs one worker at most two files' memory.
  `Stats::overruns` counts them.

## Memory

A worker is planned at 512 MiB (`WORKER_BYTES`): the file, the engine's working set and the
extracted text. Two bounds keep one file, and several big files at once, inside that plan
(or, for spreadsheets, inside each worker's share of half the available memory when that is
more), so none of them can abort the instance with an out-of-memory error.

- **The memory budget** (`DOEWOOD_INGEST_BUDGET_MIB`, else half the workers' plan: workers x
  512 MiB, at most half the available memory, halved; at least 256 MiB. 1 GiB with 4 workers,
  768 MiB under the chart's 4 GiB limit with 3). A file over 16 MiB (`SMALL_FILE_BYTES`) is
  charged its listed size (from the scan or the event) before it is fetched, and keeps that
  share until its analysis really ends: through retries, and for an analysis the watchdog gave
  up on, until it returns. A file bigger than the whole budget takes all of it, so it runs
  with no other big file (it is still refused by the source's size cap, before its body is
  read, and quail still gives a file over `max_bytes` its identity only).
- **Fairness.** Files up to 16 MiB are not charged and never wait (at most 16 MiB a worker
  beside the budget). A big file that does not fit is put aside in a list of its own and its
  worker takes the next job, so small files keep flowing on every worker. The list is first in,
  first out: a big file is not overtaken by a later big file, even a smaller one, so none
  waits forever. Each worker takes a waiting big file first once its share is free. Only when
  more big files wait than there are workers do the workers stop taking new jobs and wait for
  the budget (the queue then waits too, as for a full queue).
- **The cell cap** (`DOEWOOD_INGEST_MAX_CELLS`, else from the memory per worker). hedgehog
  reads at most this many cells (and shared strings) per workbook. The default is what one
  worker's share of the memory holds at `CELL_BYTES` = 320 bytes a cell (the peak per cell
  measured with `big_spreadsheets`, 200 to 300 bytes for numbers and text, the file included,
  rounded up), rounded down to 100,000:

  ```text
  per worker = available memory (a cgroup limit counts) / 2 / workers   (512 MiB if unknown)
  cell cap   = per worker / 320, rounded down to 100,000, from 1,600,000 to 10,000,000
  ```

  The floor, 1,600,000, is what a 512 MiB worker plan (`WORKER_BYTES`) holds; the ceiling,
  10,000,000, is hedgehog's own default (about 2.5 to 3 GiB), which only
  `DOEWOOD_INGEST_MAX_CELLS` goes above. The inputs are the same as the budget's: the
  workers (`DOEWOOD_INGEST_JOBS`, else from the CPUs and memory) and the available memory,
  read once at start; `DOEWOOD_INGEST_BUDGET_MIB` does not change the cap. For example:

  | memory available | workers | per worker | cell cap |
  |---|---|---|---|
  | unknown (not Linux) | any | 512 MiB | 1,600,000 |
  | 4 GiB limit (about 3.9 GiB free) | 3 | 666 MiB | 2,100,000 |
  | 8 GiB | 4 | 1 GiB | 3,300,000 |
  | 16 GiB | 4 | 2 GiB | 6,700,000 |
  | 2 GiB | 4 | 256 MiB | 1,600,000 (the floor) |
  | 128 GiB | 2 | 32 GiB | 10,000,000 (the ceiling) |

  A bigger workbook fails without a retry, like any other refused file, and its record keeps
  quail's identity facts: `sheet.read@1: file refused: the workbook has more than 1,600,000
  cells (limit 1,600,000); DOEWOOD_INGEST_MAX_CELLS raises the cap (it follows the memory per
  ingest worker)`. The cap in use is logged at start (`sheet_max_cells` in `ingest started`).
  Raise it only with the memory for it: about 320 bytes a cell for each worker that may read
  such a workbook at once.
- **Threads: one per analysis, more for a spreadsheet when the instance is idle** (issue
  #101). The instance has CPU slots: its CPUs (`available_parallelism`, which follows a cgroup
  CPU quota), at most the workers. Each running analysis holds one slot until it returns (an
  analysis the watchdog gave up on too). When a spreadsheet's hedgehog normalizer starts, it
  borrows the slots nobody holds, so it runs with `threads: Some(1 + free slots)`, at most 4
  threads in all (`MAX_SHEET_THREADS`; hedgehog's row model streams on at most 4). It gives
  them back when the call returns, fails or panics. Since hedgehog `fac6eba` every inner
  fan-out honours `threads`, so under load each analysis is single-threaded and the workers
  decide how many CPUs ingest uses.
  - A loan only takes slots that are free at that moment, so lending never takes the count
    over the CPUs. An analysis that starts while slots are lent does not wait: it runs beside
    them, one thread, until that normalize returns, and the next normalize borrows nothing.
  - The output does not depend on the threads (hedgehog's determinism guarantee; checked by
    `lent_threads_give_the_same_tables` in `tests/sheet.rs`).
  - Memory: the lent threads did not raise the peak measurably (below). A lent slot is an idle
    worker's, so its 512 MiB plan is not in use meanwhile; with fewer workers than CPUs
    (a memory-sized instance) only the workers' slots are lent.

  `lent_threads_measure` in `tests/sheet.rs` (generated XLSX, 38,462 rows x 26 cells, every
  other one text: 1,000,012 cells; read and tables; 4 vCPU server, test profile; RSS is the
  peak above the start):

  | | slots | threads per workbook | wall, run 1 / 2 | peak RSS, run 1 / 2 |
  |---|---|---|---|---|
  | 1 workbook, before | 1 | 1 | 1.91 / 1.90 s | +210 / +209 MiB |
  | 1 workbook, after | 4 | 4 | 1.33 / 1.30 s | +201 / +203 MiB |
  | 4 workbooks at once, before | 1 | 1 each | 2.59 / 2.51 s | +696 / +717 MiB |
  | 4 workbooks at once, after | 4 | 1 each | 2.53 / 2.46 s | +667 / +666 MiB |

  The read (calamine) stays on one thread; only the tables step is lent threads. With numbers
  only, one workbook took 0.76 s before and 0.60 s after.

  ```
  DOEWOOD_BENCH_TEXT=1 DOEWOOD_BENCH_FILES=1 cargo test -p doewood-ingest --test sheet -- --ignored --nocapture lent_threads_measure
  ```
- **quail's limits** stay its defaults: 256 MiB decompressed and 64 MiB of text per file, 40
  million pixels a rendered page, which already fit a worker. OCR has its own budget (quail's
  OCR permits, about 1 GiB an engine; see [deploy.md](deploy.md#sizing)).
- **Not covered:** `Summarize` reads and analyzes files beside ingest, under its own limit
  (`summarize_max_concurrent`), not under this budget.

## Records and step versions

The record is the quail record JSON, keyed by BLAKE3, with step versions: one per quail step
(`pdf.extract: 3`, ...), plus the ones `ReprocessJob` compares:

| key | value |
|---|---|
| `doewood.ingest` | doewood's own conversion |
| `quail.plan` | `<profile>:<digest of every planned step and its version>` |
| `hedgehog` | the hedgehog revision (spreadsheets) |

The record is written as JSON text straight from quail's record (`doewood_model::canonical`),
with no `serde_json::Value` tree in between: compact, object keys sorted, byte for byte the text
the tree wrote before issue #98. Both stores take and return that text (`Record::body` is a
`RawValue`; both keep it as it is: SQLite in `body_json`, Postgres in `records.body` since
shared schema 8, issue #121), and
readers deserialize only what they use: `DocFacts` reads the facts it needs into quail values
and skips the rest as text, the outcome of a stored record reads `format` and `status` only.
Numbers are in [Sizing, Records](deploy.md#records).

## The graph

- **Document** (BLAKE3) and **Location** through `upsert_location`.
- **Companies** from `party.candidates`, by the deterministic part of ADR 0002 section 1. The
  rules are in `doewood-enrich` (see [Company identity](#company-identity) below); ingest applies
  them and does the store lookups:
  - Normalization: Cyrillic → Latin (Serbian, Macedonian, Bulgarian, Russian, Ukrainian), NFKD
    without marks, casefold, punctuation deleted, spaces collapsed; legal forms (Balkan forms
    `d.o.o.`, `a.d.`, `d.d.`, `j.p.`, `PR`, `s.p.`, `dooel`, ... and the long spellings) taken
    off the key; seat cities after the form and noise words left out; contract role terms
    (`Supplier`, `Naručilac`) are never a company; `formerly`, `f/k/a`, `n/k/a`, `d/b/a`,
    `trading as`, `u daljem tekstu` brackets become aliases and leave the key.
  - Identifiers next to a party's mention (same page, within 400 bytes, the nearest party; a tie
    gives it to nobody): VAT, PIB, MB, OIB, LEI, DUNS, IBAN, checked before use (quail's check
    digits), placeholders refused.
  - The ladder: identifiers first, then the normalized name with its legal form, the squashed
    name, and the legal-form-blind name while exactly one company owns it (it is blocked once a
    second does). Each key is a node alias: the first claim wins, and an identifier another
    company owns is refused and noted as a conflict, never moved. Fuzzy steps are not taken here;
    nothing merges on a fuzzy match.
  - The `party` edge has quail's role and confidence, the match step, its reliability and the
    evidence (mentions, identifiers); the company keeps its names and identifiers. How each
    document matched is on its `party` edge only (issue #129): the company props no longer
    keep a `matches` list, which changed with every document and so rewrote the company's row
    for every party. Ingest removes a `matches` list stored by an earlier version the next
    time it patches that company.
  - Ingest never merges two company nodes. When a party's identifier and its name point at
    different companies, the identifier's company gets the party and nothing merges: two
    conflicting high-precision hits are for review (ADR 0002 section 1.6), never an automatic
    merge. Merges come from the enrichment job and from people, through the merge log below.
- **`amends` / `cites`** from `ref.cites`. A cited number resolves to the document that has it as
  its own number (`ref.doc_number`); until that document is known, the edge goes to a pending
  Document node `ref:<number>`, and moves to the real document when it arrives.
- **`similar_to`** from quail's MinHash LSH bands: candidates come from the band index (a lookup
  by band key), each confirmed by its Jaccard estimate (the runtime setting
  `similar_min_jaccard`, default 0.8, set with `ConfigService.SetIngestSettings`; it is read
  for every file, so a change applies to the next file without a restart). A document's
  candidates cost the same round trips however many there are (issue #126): their Document
  nodes are read in one statement (`GraphStore::node_ids`) and the edges written in one batch
  (`GraphStore::upsert_edges`: one transaction, one statement on Postgres).
- **Stop bands (issue #148).** A band key shared by more than `similar_max_bucket` documents
  (runtime setting, 10 to 1,000,000, default 1000, `SetIngestSettings`) is skipped by the
  candidate query: a bucket that large is a template's boilerplate, not similarity, and
  reading it costs a row per document in it (quadratic over a corpus of one template). The
  bucket sizes are kept in `sketch_band_sizes` by triggers on `sketch_bands` (both stores), so
  the check is one row per band (on Postgres since schema 11 only for buckets of two or more
  documents, issue #141: a missing row is a bucket of one). **This changes which `similar_to` edges exist on
  template-heavy corpora:** once a template has more than the limit of documents, a new one
  of it is compared only through its less common bands, so it may link to fewer (or other)
  near-duplicates than the exact top 100 by shared bands; documents that share only
  boilerplate bands get no edge. Below the limit nothing changes. Skipped bands are counted
  per document (`similar_buckets_skipped` in the `ingested` debug log) and in the metric
  `doewood_ingest_similar_buckets_skipped_total`. The setting applies to the next file;
  edges already written are kept (`ReprocessJob` with `force` compares again).
- **Search:** title (`ref.title`, else the card's, the file's own, the file name), body (the
  summary card, the extractive summary, keywords, sheet text), kind, date and amount.

## Company props

Ingest and the enrichment job write the same company nodes, each only its own keys (ingest:
`name`, `key`, `legal_form`, `jurisdiction`, `names`, `quail_keys`, `ids.<kind>`; the
document's `format`, `title`, `kind` and `numbers`). A writer never reads,
changes and writes back the whole props: it sends a `PropsPatch` (set, set if missing, remove,
add to a capped set, push to a capped list replacing the same entry) to
`GraphStore::update_props`, which applies it to the props as stored, with the node row locked,
in one transaction (Postgres `SELECT ... FOR UPDATE`, SQLite `BEGIN IMMEDIATE`). Nodes that
more than one writer may create are created with `ensure_node` (empty props, never replacing
an existing node's) and then patched. The conformance suite runs two writers patching one node
at the same time through two handles and checks that every write survives (issue #51).

## Merges, undo and dismissals

Every merge and split is reversible (ADR 0002 section 1.8, ADR 0001 section 7). The rules are
in `doewood_model::merge` and run once over each backend's transaction, so both behave the
same; the API is in [api.md](api.md#merges-undo-split-and-dismissals).

- **The log.** Each merge or split is one entry in `company_merges`, written in the same
  transaction as the change: the actor (`ingest`, `enrichment` or `manual`, and the instance),
  the surviving and absorbed companies, the evidence, the confidence, the fused probability,
  and an undo payload listing exactly what moved.
- **A merge** moves the absorbed company's aliases (identifiers and name keys) and its edges
  (`party` edges from documents, dated company edges) to the survivor, keeping each edge's id.
  An edge whose moved identity the survivor already has is superseded instead (a document that
  names both); edges between the two stay as they are. Props are folded: missing keys are
  added, lists (`names`, `ids.<kind>`) get what they lack, the survivor's values win. The
  absorbed node stays, with `{"merged_into": <survivor>, "merge": <entry>}`, so its id still
  resolves; ingest follows `merged_into` and never attaches to it. The `party` edge write
  resolves `merged_into` again in the same transaction (`upsert_live_edge`; on Postgres under
  share locks that wait for a merge in flight), so a merge another instance commits between the
  match and the write sends the edge to the survivor (issue #48). Such redirects are counted in
  the ingest stats (`redirected`) and logged at debug.
- **Undo** puts back what the payload lists: the aliases still owned by the survivor, the edges
  still at the survivor, the superseded edges' states, the absorbed node's props, and it takes
  out of the survivor's props exactly what the merge added (values written since stay).
  Documents that attached to the survivor after the merge stay with it. Anything that changed
  since and cannot be put back (an alias blocked or claimed by another company, an edge
  identity taken) is skipped and reported.
- **Later merges.** An undo is refused (`FAILED_PRECONDITION`, naming them) while later live
  entries involve one of its companies, or a company of such an entry. With `cascade` those are
  undone first, newest first, in the same transaction. This is the safe choice: a later merge
  may have moved the same aliases again, so undoing out of order could not restore exactly.
- **Split.** A new company (key `split:<entry>`) takes the named aliases, the `party` edges of
  the named documents and the named edges, logged like a merge. Undoing it moves everything at
  the new company back (what attached to it since too) and deletes it when nothing is left.
- **Dismissal.** A permanent "not the same" pair in `company_dismissals`, both directions,
  never deleted. A merge is refused when any company of the survivor's group (it and what was
  merged into it) is dismissed from any company of the absorbed one's, whatever the evidence;
  the enrichment gate asks `MergeStore::dismissal` before merging. An undo can record one
  (`dismiss`), and so can a split; a split that dismissed its pair cannot be undone. Ingest
  honours it too: when the ladder finds a company a document is not attached to, while the
  document is attached to a company dismissed from that one (a person split it off), the party
  stays where it is (`match.dismissed` on the edge names the company it was kept from).
- **Concurrency.** On Postgres every merge-log write takes one advisory lock and locks the
  companies involved, so merges from several instances run one at a time and no edge or alias
  is added to a company while it moves. Standalone, the store's single connection does the
  same.

## LLM summaries

Documents also get quail's `summary.digest` in their record: the bounded text a model reads.
After the record, the graph and search are stored, a document whose source's LLM policy asks
for a summary (`all`, or `kinds` naming its kind) is queued for one; the summary comes later
and never fails or holds up the file. See [llm.md](llm.md).

## Company identity

`crates/doewood-enrich` holds the company identity rules of ADR 0002 section 1. It is pure: no
I/O, no store, no network.

- `name`: normalization, Latin/Cyrillic keys, legal forms, noise, geography and role words,
  aliases from parentheses, and the deterministic ladder keys ingest stores as node aliases.
- `id` and `check`: identifiers, validated before use. LEI (ISO 17442 MOD 97-10), IBAN
  (MOD 97-10), PIB, MB, OIB and EU VAT use quail's check-digit code; VAT also covers Serbia
  (`RS` and the PIB), Norway (`NO`, the organisasjonsnummer, `MVA`) and Switzerland (`CHE`, the
  UID, `MWST`/`TVA`/`IVA`/`TPV`); DUNS is format only (nine digits). Placeholders (`000000000`,
  `123456789`) and label text are refused.
- `fuzzy`: the fuzzy name score. Tokens pair one to one, with one Damerau-Levenshtein edit on
  tokens of 5+ characters (none on CJK); a subset may add one token and must share two;
  otherwise Jaccard ≥ 0.6; an overlap of typos only is not enough. The veto: names that share at
  most one core word (function words such as `of`, `za` do not count) and are in different
  jurisdictions are different companies.
- `fusion`: the noisy-OR over distinct evidence sources (registry identifier 0.95, parent
  contract 0.95, email domain 0.90, name ladder 0.90 and less on the looser steps, filename
  0.85, invoice layout 0.75, semantic similarity 0.55; a fuzzy name hit is 0.90 times its
  Jaccard), and the gate: merge at ≥ 0.90, a new company only when every candidate is below
  0.50, review otherwise and whenever two candidates are at ≥ 0.90.
- `group`: `group_id = uuid5(NAMESPACE, LEI)`, else `uuid5(NAMESPACE, "<kind>:<value>")` for the
  MB, PIB, OIB or VAT, the strongest kind first. IBAN and DUNS never key a group.

Ingest uses only the deterministic part so far: the fuzzy score, the fusion gate and group ids
are there for enrichment, and no document merges on a fuzzy match yet.

## Status, retries, deletes

- Each object ends `done`, `skipped` or `failed` with the reason and the attempts; the progress
  counters (`processed`, `failed`, `pending`) follow.
- Transient failures (S3 5xx and 429, transport errors, the store) are retried after 2 s and 8 s:
  three attempts in all. A file quail or hedgehog cannot read, a 404 or a too-large object is not
  retried. An object that changed meanwhile keeps its newer state.
- A deleted object's `located_at` edge becomes superseded with `valid_to`; the document, its
  record and its other locations stay.

## Tests

- Unit tests: routing, the record and fact reading, the company rules (Latin and Cyrillic, legal
  forms, aliases, identifiers), the job sizing and error classes. `doewood-enrich` has its own
  (`cargo test -p doewood-enrich`): names in both scripts, every identifier check, the fuzzy
  rules and veto, the gate boundaries and pinned group ids.
- `tests/pipeline.rs`: generated PDF, DOCX, TXT and XLSX files through the whole pipeline into
  SQLite (and Postgres with `DOEWOOD_TEST_POSTGRES_URL`), then the records, the graph (pending
  references, companies across spellings and identifiers, `similar_to`), search, retries,
  deletes and reprocessing. `throughput` (ignored) measures files per second.
- `tests/known_copies.rs`: a known copy is located with a HEAD and no GetObject only with the
  source's setting on, a single-part ETag, no SSE-KMS or SSE-C on the copy and on the stored
  original, the same size and an unchanged ETag. Its status, hash and document edges equal
  those of a full ingest of the same bytes. `tests/minio_postgres.rs` checks the same against
  MinIO and measures it ([Known copies, measured](#known-copies-measured)).
- `tests/company_rows.rs` (ignored, a measurement): what each document's company props patch
  costs, for one supplier on every document against many small ones (issue #129; numbers in
  [deploy.md](deploy.md#company-rows-rewritten-by-every-document-issue-129)).
- Unit tests in `pipeline.rs` stall an analysis past the watchdog: the file fails, the worker
  goes on, the stalled analysis keeps its bytes counted until it returns and then frees them, a
  worker leaves at most one behind, and each file is hashed once. Others hold a big file past
  the watchdog: a second big file waits for the budget until that analysis ends, small files
  flow meanwhile, and a file bigger than the budget takes all of it. `tests/sheet.rs`:
  hedgehog's read past the deadline, or over the cell cap, fails the workbook.
- `tests/record_text.rs`: generated files of every kind (PDF, DOCX, XLSX, text, CSV, image,
  ZIP, unknown, empty, broken PDF and XLSX): the record text is the `Value` tree's text byte for
  byte, `DocFacts` reads the same as reading every fact, and SQLite (and Postgres with
  `DOEWOOD_TEST_POSTGRES_URL`) store it and give that text back, a second write unchanged.
- `tests/minio_postgres.rs`: an S3 put in MinIO to a record in Postgres, with both
  `DOEWOOD_TEST_S3_ENDPOINT` and `DOEWOOD_TEST_POSTGRES_URL` set (CI's `ingest-e2e` job).

## Throughput

`throughput` in `tests/pipeline.rs`: 2000 generated files (PDF, DOCX, TXT, XLSX in equal parts,
4.7 MiB), fetched from memory, analyzed without OCR, stored in in-memory SQLite (records, graph,
search), or in Postgres (a schema of its own) when `DOEWOOD_TEST_POSTGRES_URL` is set. Test
profile (dependencies at opt-level 2). The worker count is `default_jobs()` (CPUs and memory),
or `DOEWOOD_BENCH_JOBS`; the test does not read `DOEWOOD_INGEST_JOBS`.
`DOEWOOD_BENCH_WATCHERS=n` opens n `WatchProgress` streams on a real API server during the
run, and the test also prints the process CPU time, how long a trivial store call waits for
the store's lock (sampled every millisecond), and where the workers' time went
(`Stats::time`: instance store, fetch, analyze, record, graph, and the company lock's wait and
hold). On Postgres with `pg_stat_statements` loaded (`-c
shared_preload_libraries=pg_stat_statements`) it also prints the statements per file and the
costliest ones. `crates/doewood-api/tests/progress_bench.rs` (ignored) measures the same with
n streams on a 200 000-object source while the watchers are woken every millisecond.

```
DOEWOOD_BENCH_FILES=2000 cargo test -p doewood-ingest --test pipeline -- --ignored --nocapture --exact throughput
DOEWOOD_TEST_POSTGRES_URL=postgres://... DOEWOOD_BENCH_JOBS=4 DOEWOOD_BENCH_FILES=2000 cargo test -p doewood-ingest --test pipeline -- --ignored --nocapture --exact throughput
```

| machine | cores, RAM | workers | files/s | notes |
|---|---|---|---|---|
| laptop | 16, 14 GiB | 9 | 409 | from PR #5 |
| server (AMD EPYC-Genoa) | 4, 7.6 GiB | 4 | 313 | median of 317, 313, 311 |
| server, `taskset -c 0,1` | 2 | 2 | 228 | median of 242, 226, 228 |
| server, SQLite (issue #69) | 4, 7.6 GiB | 1 / 2 / 4 | 111 / 253 / 324 | medians of 3 |
| server, Postgres 17.12 (issue #69) | 4, 7.6 GiB | 1 / 2 / 4 | 56 / 136 / 246 | medians of 3; 47 statements per file |
| server, Postgres, `taskset -c 0,1` | 2 | 2 | 122 | median of 122, 115, 124 |
| server, Postgres, fewer round trips (issue #103) | 4, 7.6 GiB | 1 / 2 / 4 | 61 / 194 / 277 | one run each; 44 / 150 / 206 for `main` in the same session; 11.3 statements per file (24.1 before) |

Server: Linux 7.0.0-30, rustc 1.97.1, commit `ba0b963` for the first rows, `f8c6e3a` (with
the stage counters) for the issue #69 rows. Throughput grows less than linearly with workers
here (2 to 4 workers: 1.3x on SQLite, 1.8x on Postgres). On Postgres the time goes to round
trips, and the company lock is not the limit at 4 workers: see
[deploy.md](deploy.md#ingest-on-postgres-issue-69) and, for the round trips cut by issue #103,
[deploy.md](deploy.md#fewer-round-trips-per-file-on-postgres-issue-103).

## Read paths

Issues #74 and #75: the store reads and writes only what a caller needs. Results are the same
as before; each backend has a test that compares the new candidate query with the old one on
4,000 generated documents (`sketch_candidates_match_the_query_before`), and the conformance
suite covers the new methods.

- **`similar_to` candidates** count the shared bands on `sketch_bands` alone, order and limit
  them, and only then read the survivors' signatures. Before, every matching band row was
  joined to its signature first.
- **Unchanged writes** write nothing: `put_sketch` updates the signature only when it differs
  and deletes or inserts only the bands that changed; `index_document` leaves a row with the
  same title, body, kind, date and amount as it is (no new `tsvector` and GIN entries on
  Postgres, no FTS5 rewrite on SQLite). `sketches.updated_at` and `search_index.indexed_at`
  no longer move on such writes (nothing reads them).
- **Reprocess** reads the step versions of a page of 500 files in one query
  (`RecordStore::record_versions`), not one full record per file.
- **The merge gate** (and accepting a merge proposal without a survivor) compares document
  counts (`GraphStore::company_document_count`) instead of loading every document node.
- **Email domains** read the first 50 documents (`GraphStore::company_documents_first`) and only
  their `signal.email_domain` values (`RecordStore::fact_texts`), not 50 full records.

Ignored test `read_path_timings` in `doewood-pg` and `doewood-store` (release build; median of 9
runs, ms; writes: 200 calls, median of 3 runs). 50,000 generated documents (records of about
4 KB with 20 facts, 32 bands each; 2,000 near-identical, the rest in 48 templates), 5,000
companies; company 1 is a party of every document, the others of about 10.
`DOEWOOD_BENCH_DOCS` and `DOEWOOD_BENCH_COMPANIES` set the sizes. Postgres 17 in Docker on the
same machine, after `VACUUM ANALYZE`; SQLite on disk.

```
DOEWOOD_TEST_POSTGRES_URL=postgres://... cargo test --release -p doewood-pg -p doewood-store \
    read_path_timings -- --ignored --nocapture
```

| path | Postgres before | after | SQLite before | after |
|---|---|---|---|---|
| `sketch_candidates`, one of 2,000 near-identical | 105 | 10.0 | 146 | 15.7 |
| `sketch_candidates`, a template of ~1,000 | 79 | 5.7 | 88 | 6.4 |
| `put_sketch` x200, unchanged | 498 | 428 | 78 | 33 |
| `index_document` x200, unchanged | 267 | 229 | 61 | 23 |
| reprocess, one page of 500 | 104 | 1.9 | 55 | 18 |
| gate count, 50,000 documents | 154 | 69 | 332 | 103 |
| gate count, 10 documents | 0.25 | 0.22 | 0.17 | 0.12 |
| email domains, 50,000 documents | 170 | 83 | 301 | 5.4 |
| email domains, 10 documents | 2.7 | 0.6 | 1.9 | 0.8 |

On Postgres the unchanged writes are bound by round trips; what they save is WAL and dead
rows: 800 unchanged `put_sketch` calls wrote 59 MB of WAL before and 76 KB after. The gate
count and the email domains of a company with 50,000 documents still walk its `party` edges
(the count must, and the first 50 by key need every key); a stored count would need a schema
change.

## Timed-out spreadsheets

`timed_out_spreadsheets` in `tests/pipeline.rs`: three generated XLSX of 200,000 rows x 26
numbers (492 MiB in all) with a 300 ms file timeout, 2 workers, peak RSS sampled every 10 ms.
Server as above, test profile, median of 2 runs.

```
DOEWOOD_BENCH_ROWS=200000 cargo test -p doewood-ingest --test pipeline -- --ignored --nocapture timed_out_spreadsheets
```

| | wall | peak RSS | outcome |
|---|---|---|---|
| before (hedgehog's read without a deadline) | 4.1 s | 2066 MiB | `done` (each read to the end) |
| after (one deadline per file) | 1.8 s | 1548 MiB | `failed`: `sheet.read@1: file refused: reading took longer than the time limit` |

The RSS before the run (574 MiB) is the test's own copies of the files.

## Known copies, measured

`known_copies_skip_the_get_object` in `tests/minio_postgres.rs` (issue #131), release build,
MinIO and Postgres in containers on an internal Docker network, 2 workers. One 256 MiB object
is ingested first, then a second source with 8 copies of it under new keys
(`DOEWOOD_KNOWN_COPIES=8 DOEWOOD_KNOWN_COPIES_MIB=256`). Times run from starting the copies'
scanner to the last copy being through (scan, HEAD or GetObject, hash, graph):

| `skip_known_copies` | GetObject bytes | time | known copies |
|---|---|---|---|
| off | 2048 MiB | 2.01 s | 0 |
| on | 0 MiB | 0.11 s | 8 |

With the setting on, each copy costs one HEAD and one record read. With it off, each copy is a
full download and hash, and its 256 MiB share of the memory budget is held for that time. Both
runs end with the same statuses, hashes and `located_at` locations.
