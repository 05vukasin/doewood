# ADR 0003: records stored as compressed canonical text

Status: accepted, 2026-11-26. Amends ADR 0001, section 2 ("records ... as JSONB").

## Context

ADR 0001 put every quail/hedgehog record in the shared Postgres as JSONB. Since issue #98/#106
ingest writes a record as canonical JSON text (`doewood_model::canonical`: compact, keys
sorted), and both stores return that text (`Record::body` is a `RawValue`). SQLite already kept
the text as it is.

On Postgres the text was parsed into jsonb on every write and turned back into text on every
read. jsonb's binary wire format is a version byte followed by that same text, so the
conversion happens on the server whether doewood reads binary or `::text`. doewood then parsed
the result into a `Value` and wrote the canonical text again. Issue #121, found in #120: under
heavy API reads, `ListRecords` pages spent 92 to 95% of all database time on this conversion,
and ingest slowed to about 60 files/s.

Records are read by key: by BLAKE3 (`get_record`, `record_versions`, `fact_texts`) or as pages of
a source in key order (`ListRecords`, which filters through `locations`). The fields that
queries filter or sort on are already in their own columns (below). Nothing searches inside a
record.

## Decision

1. **A record is stored as its canonical JSON text** in `records.body` (`text`), compressed by
   TOAST with **lz4** (pglz on a server built without lz4). It is sent back unchanged. This is
   shared schema 8 (`0008_record_text.sql`). The old `records.record` (jsonb) is kept, nullable,
   only for rows written before schema 8 until they are converted.
2. **Filters stay in columns, not in the record.** Every field a query filters, joins or sorts
   on is a column of its own:
   - `records`: `blake3` (primary key; get, page order and page token, `ANY($1)` lookups) and
     `body IS NULL` (the rows still to convert, partial index `records_body_pending`);
   - `locations`: `endpoint`, `bucket`, `object_key` (the source and prefix of `ListRecords`)
     and `blake3`;
   - `search_index`: `tsv` (full text from `title` and `body`), `kind`, `doc_date`, `amount`;
   - the graph: `nodes.kind`, `nodes.key`, `node_aliases`, `edges.src`, `edges.dst`,
     `edges.kind`, `edges.state`; `sketch_bands.band`; the enrichment and feed tables' own
     columns.

   Ingest fills these columns from the record when it writes it. A new filter on record content
   gets a column (or a table) of its own; it does not query `records.body`.
3. **`fact_texts` parses on the fly.** It is the one query that looks inside a record: it
   returns the `value.v` strings of one fact key (the enrichment's e-mail domains) for records
   it already selected by key, parsing `body` as jsonb, the way SQLite parses `body_json`
   (about 0.16 ms a record). It is not a filter path: it never chooses which records to read.
4. **A record Postgres's jsonb could not hold is still refused** (one with `\u0000`), so every
   stored body parses as jsonb, for `fact_texts` and for the down path below.
5. **Upgrade together, convert in the background.** The migration is additive (a column, a
   dropped `NOT NULL`, a partial index). An instance converts the rows from before after it
   starts, in resumable batches. The API returns the same bytes before, during and after the
   conversion. As with every shared schema, all instances are upgraded together
   ([configuration.md](../configuration.md#the-shared-postgres)).

## Measurements (issue #121, PR #132)

`throughput`, release, 2000 files, 4 workers, 4 readers, 8 API clients asking for a
`ListRecords` page of 100 every millisecond, Postgres 17:

| | jsonb (before) | text, lz4 |
|---|---|---|
| ingest under API load | 60 files/s | 110 to 114 files/s |
| `ListRecords` database time per page | 25.3 ms | 5.3 to 7.5 ms |
| stored per record (table with index) | 8,481 bytes | 7,065 bytes |
| record upsert, database time | 0.66 ms | 0.18 to 0.19 ms |

Text with pglz stored the least (6,473 bytes), but a page took 3.6 ms against lz4's 2.4 ms (idle
server), and writes took 0.24 ms against 0.05 ms. Keeping jsonb next to the text would have
nearly doubled the table (13,752 bytes a record) for the one `fact_texts` query. Details:
[deploy.md](../deploy.md#records-as-text-on-postgres-issue-121).

## Consequences

- The API's output is unchanged, byte for byte. Two values that jsonb did not give back as
  written now come back as written, as on SQLite: `-0.0` (read as `0.0` before) and numbers near
  `f64::MAX` (reading those failed before).
- Postgres can no longer answer ad-hoc questions about record content with jsonb operators
  without a cast (`body::jsonb`), which parses each record. Such questions belong in columns.
- **Down path.** Schema 8 can be undone by hand while no instance runs. Set
  `UPDATE records SET record = body::jsonb WHERE record IS NULL` (in batches on a big table),
  then drop `records_body_pending` and `body`, restore `record`'s `NOT NULL`, and delete the
  `doewood_schema` row for version 8. A binary from before schema 8 refuses a database at
  schema 8 (not ready). Without the down path it could not read converted rows, whose `record`
  is NULL. See [configuration.md](../configuration.md#the-shared-postgres).
- A later schema can drop `records.record` and `records_body_pending` once no database still
  has unconverted rows.
