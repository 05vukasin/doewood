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
  The file timeout is quail's (2 min).
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
- **`amends` / `cites`** from `ref.cites`. A cited number resolves to the document that has it as
  its own number (`ref.doc_number`); until that document is known, the edge goes to a pending
  Document node `ref:<number>`, and moves to the real document when it arrives.
- **`similar_to`** from quail's MinHash LSH bands: candidates come from the band index (a lookup
  by band key), each confirmed by its Jaccard estimate (the runtime setting
  `similar_min_jaccard`, default 0.8, set with `ConfigService.SetIngestSettings`; it is read
  for every file, so a change applies to the next file without a restart).
- **Search:** title (`ref.title`, else the card's, the file's own, the file name), body (the
  summary card, the extractive summary, keywords, sheet text), kind, date and amount.

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
