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
  request, so a restart, an event that raced the scanner's commit, or a source added while ingest
  was busy are all picked up.
- **Bounded, with backpressure.** The dispatcher hands jobs to the workers through a queue of
  twice the workers and waits while it is full. It then stops reading the scanners' event channel
  (1024 events), and the scanners wait in turn.
- **Workers:** `DOEWOOD_INGEST_JOBS`, else one per CPU, at most what half the available memory
  holds at 512 MiB a worker (a cgroup v2 limit counts), at most 32. Every analysis runs on a
  blocking thread; the quail engine is shared.
- **Shutdown:** each worker finishes the file in hand; the rest stays pending for the next start.

## Fetch

`GetObject` with the source's credentials, streamed under its size cap (512 MiB by default). An
object over the cap fails without a retry.

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
  that need it, with quail's budgets (pages read at once from the memory, 45 s a page, the file
  timeout extended for them). Without the models it logs one `WARN`, runs `standard`, and scanned
  pages are flagged `needs_ocr`. `--no-default-features` builds without ONNX Runtime.
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
  (`sheet.read: ... time limit`); tables past it are a problem of a partial record. The other
  hedgehog limits (cells, decompressed bytes) are its defaults. The fetch is not under it: the
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

## Records and step versions

The record is the quail record JSON, keyed by BLAKE3, with step versions: one per quail step
(`pdf.extract: 3`, ...), plus the ones `ReprocessJob` compares:

| key | value |
|---|---|
| `doewood.ingest` | doewood's own conversion |
| `quail.plan` | `<profile>:<digest of every planned step and its version>` |
| `hedgehog` | the hedgehog revision (spreadsheets) |

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
    evidence (mentions, identifiers); the company keeps its names, identifiers and matches.
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
  for every file, so a change applies to the next file without a restart).
- **Search:** title (`ref.title`, else the card's, the file's own, the file name), body (the
  summary card, the extractive summary, keywords, sheet text), kind, date and amount.

## Company props

Ingest and the enrichment job write the same company nodes, each only its own keys (ingest:
`name`, `key`, `legal_form`, `jurisdiction`, `names`, `quail_keys`, `ids.<kind>`,
`matches`; the document's `format`, `title`, `kind` and `numbers`). A writer never reads,
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
- Unit tests in `pipeline.rs` stall an analysis past the watchdog: the file fails, the worker
  goes on, the stalled analysis keeps its bytes counted until it returns and then frees them, a
  worker leaves at most one behind, and each file is hashed once. `tests/sheet.rs`: hedgehog's
  read past the deadline fails the workbook.
- `tests/minio_postgres.rs`: an S3 put in MinIO to a record in Postgres, with both
  `DOEWOOD_TEST_S3_ENDPOINT` and `DOEWOOD_TEST_POSTGRES_URL` set (CI's `ingest-e2e` job).

## Throughput

`throughput` in `tests/pipeline.rs`: 2000 generated files (PDF, DOCX, TXT, XLSX in equal parts,
4.7 MiB), fetched from memory, analyzed without OCR, stored in in-memory SQLite (records, graph,
search). Test profile (dependencies at opt-level 2). The worker count is `default_jobs()` (CPUs
and memory); the test does not read `DOEWOOD_INGEST_JOBS`.

```
DOEWOOD_BENCH_FILES=2000 cargo test -p doewood-ingest --test pipeline -- --ignored --nocapture throughput
```

| machine | cores, RAM | workers | files/s | notes |
|---|---|---|---|---|
| laptop | 16, 14 GiB | 9 | 409 | from PR #5 |
| server (AMD EPYC-Genoa) | 4, 7.6 GiB | 4 | 313 | median of 317, 313, 311 |
| server, `taskset -c 0,1` | 2 | 2 | 228 | median of 242, 226, 228 |

Server: Linux 7.0.0-30, rustc 1.97.1, commit `ba0b963`. Throughput grows less than linearly
with workers here (2 to 4 workers: 1.4x).
