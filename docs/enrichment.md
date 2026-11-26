# Enrichment

The enrichment job of [ADR 0002](adr/0002-company-enrichment.md) (sections 1 to 3, 5 and 6,
with the registry [delta feeds](#delta-feeds)):
it links the companies the documents mention to public registries, merges the ones that are
the same company, writes what the registries say as company props and dated edges, and finds
each company's website and favicon. It lives in `doewood-ingest` (`enrichment` module) and
runs in every instance, next to ingest, started by `doewood serve`. The registry clients are
those of [registries.md](registries.md).

No LLM is involved (the LLM tier of ADR 0002 section 4 is a later, optional step). Only
identifiers and names go to registries, and only domains to websites: documents never leave
the instance (ADR 0002, the privacy rule). APR and EDGAR's name lookups are done locally on
downloaded files, so not even the name is sent there.

## Passes

The job works in passes: one at start, then one every 30 seconds. A pass reads the runtime
settings (so a change applies to the next pass), then pages through the companies the store
says are due (`FreshnessStore::companies_due`, 100 at a time) and works on up to 4 companies
at once. Each registry client keeps its own rate limit and backoff, so a cold start of a large
corpus takes hours without slowing ingest down.

A company is due (ADR 0002 section 6):

- **new:** it has a document and was never enriched: right away;
- **documents doubled:** its document count is at least twice what it was (and at least 2);
- **scheduled:** a registry's refresh time has passed, or a [delta feed](#delta-feeds) said
  the registry changed it.

A new or doubled company is asked of every registry; a scheduled one of the due registries
and of any registry it was never asked (switched on since, say).

**Leases.** Before it works on a company, the instance takes the company's lease
(`ClaimStore::lease_company`, 15 minutes, renewed during the work) and gives it back after.
With a shared Postgres, a company another instance holds is left out of `companies_due`, and
a lease it cannot take is skipped, so one instance enriches each company and the others reuse
the result (and the registry answers, cached in the shared store). A merge also takes the
other company's lease first. When another task of the same pass works on the other company,
the company is run once more after that page's tasks are done, and the merge happens then;
when another instance holds it, every registry of the company is due again after the retry
interval (6 hours), and the merge happens on that run. A lease left behind by a crash expires.

**`companies_due` cost (issue #71).** Every pass on every instance asks for the companies
due, and in the steady state (nothing due) one call walks every company. The query tests the
cheap things first: no enrichment row or a registry due (then one live document is enough),
else a bound from the company's `party` edges in the `edges_by_dst` index alone (every party
edge, so never below the document count); only a company past that bound gets the exact
document count. The exact count is taken for the returned page only. On Postgres the
candidates are listed in id order behind a fence (`OFFSET 0`), so the tests run in that order
and stop at the page size. No schema change. Results are those of the query before: a test on
each backend walks both queries on 1,000 generated companies covering every case, for three
owners and four page sizes, and compares the pages.

One page of 100, generated companies with ten party edges each, median of 7 runs (ignored
tests `companies_due_timings` in `doewood-pg` and `doewood-store`; `DOEWOOD_BENCH_COMPANIES`
sets the count; SQLite on disk, release build; Postgres 17 in Docker, after `VACUUM ANALYZE`):

| backend | companies | state | before | after |
|---|---|---|---|---|
| Postgres | 10,000 | steady (nothing due) | 461 ms | 36 ms |
| Postgres | 10,000 | mixed | 15 ms | 7.6 ms |
| Postgres | 100,000 | steady (nothing due) | 9,795 ms | 391 ms |
| Postgres | 100,000 | mixed | 26 ms | 12.7 ms |
| SQLite | 10,000 | steady (nothing due) | 155 ms | 15 ms |
| SQLite | 10,000 | mixed | 4.1 ms | 2.1 ms |
| SQLite | 100,000 | steady (nothing due) | 1,763 ms | 160 ms |
| SQLite | 100,000 | mixed | 4.3 ms | 2.5 ms |

At 100,000 companies the Postgres steady-state plan touches 0.8 million buffers instead of
10.7 million (`EXPLAIN (ANALYZE, BUFFERS)`): no heap fetch per edge any more. The partial
covering index the issue held in reserve is not needed. On SQLite this is also how long the call holds the store's only connection. The steady state
is still linear in the companies (a few index probes each, about 10 index entries of
`edges_by_dst` per company); keeping a live document count per company at write time would
make it a range scan on due times, at the cost of a schema change.

**SQLite pages (issue #139).** On a store grown by ingest (many document and location nodes,
with or without planner statistics) SQLite took every company from the `(kind, key)` index,
ran the tests on each and sorted them all for every page, so one pass in pages of 100 grew
with the square of the companies. The query now reads the companies through the partial
index `nodes_companies` (`INDEXED BY`): in id order from the page token, stopping at the page
size, as the fence does on Postgres. A page costs its own companies and their tests, and a
pass is linear. No schema change. The comparison test also runs without statistics, checks
the plan (`nodes_companies`, no sort for the `ORDER BY`) and compares with the query before
this change. The pass on the `scale` store (docs/deploy.md, "50,000 to 200,000 documents"):

| documents | companies | first page, before / after | a pass in pages of 100, before / after |
|---|---|---|---|
| 50,000 | 9,372 | 51 / 14 ms | 2.4 s / 0.12 s (94 pages) |
| 200,000 | 37,497 | 218 / 55 ms | 39.2 to 39.7 s / 0.35 to 0.4 s (375 pages) |

The first page holds the big company (12,504 documents), whose exact count is most of that
page. At 200k with the store's pages out of the OS page cache the new pass took 4 to 8.5 s,
nearly all of it disk reads (about 1 s of CPU): the exact counts read each party edge and
its document. After `analyze` (planner statistics as `serve` refreshes them) the plan is
the same (`INDEXED BY` fixes it); the passes measured then were of the cold kind, 7.2 and
7.7 s.

**Shutdown.** The pass stops taking companies; those in flight get 10 seconds, then are
dropped (their leases expire and they are enriched again later).

## Registry order

Registries are asked in jurisdiction order (ADR 0002 section 2): the registry of the
company's country first, then the other national registries it has an identifier for, then
GLEIF, then Wikidata, the lowest tier. The country is the company's `jurisdiction` (from
quail), else what its identifiers imply (a Serbian MB or PIB, an OIB, a VAT prefix, a UK
company number).

| registry | asked by | without an identifier |
|---|---|---|
| APR (Serbia) | MB (`id:rs_mb`) | name search in the open-data set, for a Serbian company |
| Companies House (UK) | company number (`reg:companies_house`) | name search, for a UK company |
| SEC EDGAR (US) | CIK (`reg:edgar`) | name lookup in the tickers file, for a US company |
| VIES (EU) | an EU VAT number (`id:vat`) | nothing |
| GLEIF | LEI (`id:lei`) | name search (with the country, when known) |
| Wikidata | LEI, or its item (`reg:wikidata`) | nothing (no name search on the lowest tier) |

One registry's answer can unlock another in the same pass: GLEIF's `registeredAs` is the UK
company number or the Serbian MB (for a GB or RS entity, when it passes its check), and EDGAR
gives the LEI. Each registry is asked at most once per company per pass. A PIB alone is not an
APR lookup (the open data has no PIB): the company is then searched by name.

## The decision gate

Evidence is fused with noisy-OR over distinct sources (ADR 0002 section 1.6,
`doewood_enrich::fusion`) and gated: **merge at 0.90 or more, review between 0.50 and 0.90,
nothing below 0.50**; two conflicting high-precision hits are always for review.

- **A name search's records.** Each candidate's registered name is scored against the
  company's: the same key and legal form is the name ladder's 0.90, the same key with the form
  missing on one side the legal-form-blind 0.85, otherwise the fuzzy score (scaled by its
  Jaccard) with the jurisdiction veto. A record in another country than the company's is never
  a candidate. A record at 0.90 or more is adopted: its identifiers join the company. Records in
  the review band become registry record proposals, for a person (see [Proposals](#proposals)).
- **Other companies.** A company that owns an identifier the registry gave for this one is a
  registry identifier hit (0.95); a company that owns a key of the registry's legal name or a
  former name is a name-ladder hit (0.90, 0.88 squashed, 0.85 legal-form-blind). A company that
  carries a different national number of the same kind (MB, PIB, OIB, VAT, Companies House
  number, CIK) or is in another country is never a candidate. At 0.90 or more the two are
  merged through the merge log (`MergeStore::merge_companies`, actor `enrichment`, with the
  evidence, the confidence and the fused probability; the survivor is the one with more
  documents, counted without loading them), so a person can undo it. The review band and conflicts become merge proposals.
- **Dismissals are permanent.** A dismissed pair (`MergeStore::dismissal`) is never merged
  or proposed, whatever the evidence, and is no candidate at all (so it makes no conflict for
  the others).
- **No circular evidence.** An identifier adopted from a name match is never a registry
  identifier hit: a company that owns one is a proposal. Wikidata items never count as one
  either (the lowest tier).

## What it writes

**Identifiers** become aliases of the company, first claim wins: `id:lei:…`, `id:rs_mb:…`,
`reg:companies_house:…`, `reg:edgar:…` (the CIK zero-padded to 10 digits), `reg:wikidata:Q…`,
and the verified website `domain:…`. One another company owns stays its own (the gate merged
the two, or recorded a proposal). Former names become learned aliases (`name:<key>|<form>`).

**Props** of the company: `registry.<name>` (each registry's facts and fetch date: status,
legal form, registered name, events), `ids` and `registry_ids`, `legal_name`, `former_names` (at
most 20), `website`, `icon` and `enriched_at`. The proposals lists of schema 4
(`merge_proposals`, `registry_proposals`) are removed when the job writes (the migration
moved them, and an undo of an older merge can bring them back). The document name (`name`) is kept. The job writes only these keys, as one
atomic patch (`GraphStore::update_live_props`, see [ingest.md](ingest.md#company-props)), so what
ingest writes to the same company at the same moment is never lost.

**Edges** (ADR 0002 section 3), each with `source = registry`, the registry and record id
(and fetch date) as evidence, `valid_from` / `valid_to`, a confidence and a state:

| registry | edges | state |
|---|---|---|
| GLEIF Level 2 | `parent_of` (parent → company) and `subsidiary_of` (company → parent), from the relationship period; the ultimate parent too when it is another entity (`level: ultimate`); an inactive relationship is superseded | active |
| GLEIF successions | `merged_into` the successor, on the date of the latest completed merger-type event (else the expiration) | active |
| GLEIF events | `liquidated` (on the company itself) for a completed liquidation, bankruptcy, insolvency or dissolution event, or a dissolved entity | active |
| GLEIF, Companies House, EDGAR former names | `renamed_to` on the company itself, on the day the old name ended (`props.from`, `props.to`); undated ones only become aliases | active |
| Companies House PSCs | `parent_of` / `subsidiary_of` from a corporate-entity or legal-person PSC with majority control (more than 50% of shares or votes, or the right to appoint and remove directors), from `notified_on` to `ceased_on`; natural persons never | active |
| Companies House status | `liquidated` for `dissolved` (on `date_of_cessation`) and `liquidation` | active |
| APR status | `liquidated` for in liquidation, forced liquidation or bankruptcy (no date in the open data; `props.as_of`) | active |
| EDGAR Exhibit 21 | `parent_of` (company → subsidiary) and `subsidiary_of`, from the annual report's filing date, to companies the graph already has (by name) | proposed |
| EDGAR 8-K Item 2.01 | `acquired`, on the filing date, when the item's text names one of the company's Exhibit 21 subsidiaries | proposed |
| Wikidata | `parent_of` / `subsidiary_of` from parent organization (P749) and owned by (P127, never a person), `merged_into` from replaced by (P1366) and followed by (P156), with the statements' dates, to companies the graph has that item for | proposed |

Confidence is 0.95 for an official registry's edge, 0.6 for Exhibit 21 and 8-K text, 0.5 for
Wikidata. Proposed edges never become active on their own (the trust order: registry, then
documents, then web; a person or a registry makes them active).

**The other end** of an edge: the company with that LEI or Companies House number, created
(keyed by the identifier, `origin: registry`, with its registered name and name keys) when the
graph has none; or the company that has that Wikidata item or name (Exhibit 21 rows, PSCs
without a UK number), never created. An edge whose other end cannot be found is dropped.
Every edge write (the superseding and `former_parent_of` writes below too) resolves
`merged_into` again in the same transaction (`GraphStore::upsert_live_edge`, issue #60): a
merge committed since the ends were looked up (by a person, or on another instance) moves the
edge to the survivor instead of leaving it on the merged-away company.

**A company merged away during its job** (by a person, or on another instance; issue #62)
gets nothing more. The job writes the edges first (they follow the merge, above), then the
company's own aliases and props, its website and its freshness, each onto the live company
only: `GraphStore::claim_live_alias`, `GraphStore::update_live_props` and
`FreshnessStore::record_enrichment` check `merged_into` atomically with the write (under the
node's row lock on Postgres, in the write transaction on SQLite) and refuse a husk with
`Error::MergedAway`. The job then stops for that company: what it wrote before the merge was
moved or folded by the merge, nothing lands on the husk afterwards, and the company it was
merged into is made due for every enabled registry (`FreshnessStore::mark_due`, which leaves
its last enrichment as it was), so the next pass enriches the survivor with the identifiers it
has now, the husk's included. The company does not count as `enriched`. The job does not
redirect these writes to the survivor: its findings came from the husk's identifiers only, so
the survivor is enriched as itself instead.

**The later dated parent is current.** When a company has two current parents (active, no
end date, not an ultimate parent), the later dated one stays current; each earlier one ends on
that day (its `parent_of` and `subsidiary_of` become superseded, with `valid_to`), and a
`former_parent_of` edge records it, from its start to that day. So "who owned X on
2022-05-01" is a query.

## Proposals

What the gate leaves to a person (ADR 0002 section 1.6) goes to the proposals table
(`ProposalStore::propose`, issue #52), not to company props:

| kind | candidate | from |
|---|---|---|
| `registry_record` | `<registry>:<record>` (`apr:companies/<MB>`, `company/<number>`, `submissions/CIK<cik>`, `lei-records/<LEI>`) | a name search's record in the review band, or one of conflicting records |
| `merge` | the other company's id (the pair in id order) | a company in the review band, conflicting companies, or a company that owns an identifier the job adopted from a name match |

Each has its evidence (the same objects a merge logs), the fused probability, a reason
(`review_band` or `conflict`) and a detail object (a record's registry, id, name and country).
There is one open proposal per kind, company and candidate: on a later pass the job updates
its evidence, probability and reason instead of adding another. A proposal rejected with a
dismissal is never opened again, and a merge proposal is never opened for a dismissed pair.
`proposed` counts the proposals opened or updated.

A person lists, accepts and rejects them through `GraphService` (see
[api.md](api.md#enrichment-proposals)). Accepting a merge merges through the merge log (actor
`manual`, the proposal's evidence), so it can be undone. Accepting a record adopts its
identifier as an alias of the company and makes the record's registry due, so the next pass
looks the company up by that identifier, as for any company that has it, and writes the
record's facts and edges.

Schema 5 (shared) and 8 (standalone) moved the props lists of earlier versions into the table
(see [configuration.md](configuration.md)).

## Website and favicon

ADR 0002 section 5:

1. **Candidates:** the official website from Wikidata, then the email domains seen in the
   company's first 50 documents by key (quail's `signal.email_domain`, most frequent first;
   free-mail hosts are never one). No domain is guessed from the name, and no search is made.
2. **Verified** before it is stored: the domain is not owned by another company (alias
   `domain:<domain>`), it resolves through the favicon SSRF guard (public addresses only,
   redirects re-checked, the configured egress proxy), and the homepage, or the one
   legal-notice page it links to on the same site (`impressum`, `legal`, `kontakt`,
   `o-nama`, ...), contains the company's normalized name key (or a registered name's) as whole
   words, or one of its identifiers (MB, PIB, LEI, VAT, Companies House number; spaced or
   dotted forms too).
3. **The favicon** of a verified domain is resolved through the same guard into the icon
   cache (`favicons`, shared in shared mode), which `GET /icons/{domain}` serves from (see
   [favicons.md](favicons.md)). The company's `icon` prop names that path.

A website verified less than 30 days ago is not checked again. At most four candidates are
tried per company.

## Freshness

After a company, the job records its enrichment (`record_enrichment`): the document count now,
and for each registry asked the time it is due again, at the client's refresh
([registries.md](registries.md)):

| registry | refresh |
|---|---|
| GLEIF, APR, Companies House, Wikidata | 30 days |
| VIES | 90 days |
| EDGAR | 30 days while its daily index feed is on (new filings make the company due); 1 day with the feed off (the submissions' freshness, standing in for "when new filings appear") |
| any registry that failed | 6 hours |

A registry that was not asked (switched off, not configured, nothing to ask it) keeps no due
time; it is asked when the company is next due and it is switched on.

## Delta feeds

ADR 0002 section 6: registry delta feeds are applied when configured, so a known company is
refreshed when a registry changes it, not only on its schedule (issue #49). The feeds live in
the job (`enrichment::feeds`), their clients in `doewood_registry::feeds`.

| feed | source | checked | a record makes due | default |
|---|---|---|---|---|
| `gleif` | the golden copy's delta files: LEI-CDF (`lei2`) and RR-CDF (`rr`), zipped CSV, published at 00:00, 08:00 and 16:00 UTC | hourly | the company owning the LEI (`id:lei:`); for a relationship, the child's and the parent's | on |
| `edgar` | the daily index, `Archives/edgar/daily-index/<y>/QTR<q>/master.<yyyymmdd>.idx` | hourly | the company owning the CIK (`reg:edgar:`) of a `10-K`, `10-K/A`, `20-F`, `20-F/A`, `8-K` or `8-K/A` | on |
| `apr` | APR's open-data set (no deltas exist): downloaded again and swapped in for the client's index | weekly; daily once the set applied is over 31 days old | the company owning the MB (`id:rs_mb:`) whose record (name, status, legal form, seat, activity, founding date) differs from the cached one | on |
| `companies_house` | the streaming API: `companies`, `officers` and `persons-with-significant-control` | continuously, the three streams in turns of 5 minutes on one connection (an account may hold two) | the company owning the number (`reg:companies_house:`) of an event's `resource_uri` | off (needs a streaming key) |

- **Known companies only.** A feed looks the identifiers it reads up among the company
  aliases the graph already has (`FeedStore::companies_by_alias`, 500 at a time); anything else
  is dropped. It never creates a company or writes a fact: it drops the company's cached
  answers of that registry (`CacheStore::cache_invalidate`: `gleif:lei-records/<LEI>`,
  `edgar:submissions/CIK<cik>`, `companies_house:company/<number>`, each with what is under it)
  and makes that registry due now (`FreshnessStore::mark_many_due`; a company never enriched is
  due anyway). The next pass (the job is woken) asks the registry again, through the gate and
  the writer as always. APR is the exception to the cache drop: the feed writes the new set's
  record into the cache (`apr:companies/<MB>`), so the next lookup reads it on every instance
  without a download.
- **Bounded.** GLEIF's files are spooled into a temporary file (at most 512 MiB) and read
  record by record (a record at most 256 KiB, at most 5 million records, at most 4 GiB out of
  the zip); EDGAR's index and the stream are read line by line (4 KiB and 1 MiB lines). Only
  identifiers are kept. The feeds send nothing about a company: the files and streams are
  the same for everyone.
- **One instance per cluster** reads each feed: the feed's row in `registry_feeds` (shared
  schema 6, instance schema 9) holds a lease (`FeedStore::lease_feed`, 15 minutes, renewed as
  the feed goes; an abandoned one expires), the feed's **cursor** and its next run. Another
  instance carries on from the cursor after a restart or a crash. The Companies House stream
  keeps its lease between connections; the others give it back after each run. An instance
  where a feed is switched off never takes its lease.
- **Cursors.** `gleif`: the publication last applied (Unix seconds); the next run takes the
  shortest delta that reaches back to it (`IntraDay` 8 hours, `LastDay`, `LastWeek`,
  `LastMonth` 31 days). Further behind than a month, or on the first run, the latest
  publication becomes the cursor and the 30-day schedule covers the gap. `edgar`: the last
  day applied (`YYYY-MM-DD`; at most 31 days back, 14 days per run); the first run starts
  with yesterday. A day without an index (404) is skipped once a later day has one, or when
  it is a weekend or five days old; otherwise the feed waits for it (the SEC builds the index
  in the evening, Eastern time). `apr`: the date of the set last applied (`DatumPreseka`); a
  set with the same date compares nothing. `companies_house`: each stream's last timepoint
  (`companies=<n>,officers=<n>,persons-with-significant-control=<n>`), saved as it reads; a
  connection resumes after it. A timepoint too old (416) restarts that stream from now; the
  schedule covers the gap.
- **Switches** (runtime settings, read every minute): `gleif_feed_enabled`,
  `edgar_feed_enabled`, `apr_feed_enabled` (default on) and `companies_house_feed_enabled`
  (default off). A feed runs only while its registry is on too; the Companies House stream
  also needs a streaming key (`companies_house_stream_key`, write-only: Companies House issues
  streaming keys apart from REST keys) besides the REST key. See
  [api.md](api.md#enrichment-settings).
- **Failure-soft.** A failed run (the source down, refusing, a malformed file, a store error)
  is logged at `WARN` (`feed run failed`), counted in `failures`, keeps the cursor, and is
  tried again after 30 minutes. A stream connection that ends or goes quiet (no heartbeat
  for 2 minutes) keeps what it read; the next stream takes its turn. A 429 waits a minute.
- **Log lines:** `gleif feed: delta file read` / `publication applied`, `edgar feed: daily
  index applied`, `apr feed: new set swapped in`, `companies house stream: connection ended`,
  `feed run failed`, `feed lease lost ...`; each company made due at debug level.

## Settings

The registry switches, the Companies House key, the EDGAR contact, APR's extra roots and the
favicon fallback and proxy are the runtime settings of
[registries.md](registries.md#configuring-the-registries), read on every pass. A switched-off
registry, and Companies House without a key, has no client and is never asked. The clients are
rebuilt only when what they are built from changes, so their rate limiters, pauses and APR's
downloaded set carry over between passes.

## Failure-soft

A registry failure (down, refusing, rate limiting, a malformed answer) is a warning on that
company, logged at `WARN` with the registry and why, counted in `registry_warnings`; the other
registries still answer, the company is still recorded, and the failed registry is asked again
after 6 hours. A store error fails that company only (counted in `failed`, retried on the next
pass). Nothing the job does blocks ingest.

## Progress

`JobsService.WatchProgress` (a watch of every source) carries the job's counters since the
instance started, as `KIND_ENRICHMENT` updates (see [api.md](api.md)): `enriched`, `merged`,
`proposed`, `failed`, `registry_warnings`, `edges`, `redirected` (edges that landed on the
company an end was merged into while the company was worked on), `websites`, `passes`,
`last_pass_completed_at` and `running`, and per delta feed (`feeds`): `enabled` (here),
`leader` (this instance holds its lease), `runs`, `records` (records read), `companies_due`,
`failures`, `last_run_at` and `position` (the publication time, the last index day or the
set's date as Unix seconds; the Companies House timepoint). Only the instance reading a feed
counts its runs.

Log lines: `enrichment started` / `stopped`, `company enriched` (with the company, why it was
due, and what it did), `companies merged` (survivor, absorbed, merge entry, probability),
`website verified`, `registry warning`, and `enrichment pass done`.

## Limits and assumptions

- **Delta feeds mark, they do not write.** A feed makes the registry due; the facts come from
  the registry's answer on the next pass. A feed that is down for longer than its reach
  (GLEIF's monthly delta, EDGAR's 31 days, an expired stream timepoint) leaves the gap to the
  30-day schedule. Only the six EDGAR forms above trigger a refresh (`10-Q`, `S-1`, `4` and
  the rest do not).
- **APR's set on other instances.** The instance reading the APR feed swaps its own index in
  and writes the changed records to the shared cache; other instances keep their index (used
  for name searches) until its 30 days are up.
- **APR's open data** has no PIB, no dates of status changes, no mergers and no deleted
  companies, so APR gives no dated events beyond the status.
- **Merger filings** of Companies House name no counterpart; they are kept in
  `registry.companies_house.events`, not as edges. 8-K acquisitions become edges only when
  their text names a known subsidiary.
- **The in-memory caches.** While the shared store is not connected, registry answers and
  icons (`GET /icons/{domain}` is public) are cached in memory instead: two `MemoryCache`s of
  at most 10,000 entries and 16 MiB each (32 MiB together; keys, bodies and about 160 bytes
  per entry). The oldest fetch goes first, an answer bigger than the budget is not kept, and
  entries past their expiry are dropped on lookup and insert (issue #111). Neither is
  configurable; once the store is connected they are no longer used.
- **Parser assumptions** of the registry clients (field names checked against public
  documentation, and what is still assumed) are listed per registry in
  [registries.md](registries.md).

## Tests

`cargo test -p doewood-ingest --test enrichment` and `--test feeds`, against SQLite and,
with `DOEWOOD_TEST_POSTGRES_URL`, Postgres. Mock registries and mock websites on 127.0.0.1 only:
invented companies and identifiers (valid check digits only), `*.test` host names resolved to
the mock by a fake resolver, reached under a test-only address policy (the production policy is
unchanged). They cover identifiers through APR, GLEIF and Wikidata to identifiers, props and
dated edges; the gate's merge (registry identifier), dismissed pair (never merged or proposed),
adopted name match, review band (a registry record proposal in the table, accepted, and the
record's facts written on the next pass) and no match; the trust order (official active, Wikidata and Exhibit 21 proposed); a later
dated parent superseding the other into a former parent; leases between two instances
(Postgres); a registry down while the others answer; switched-off registries and Companies
House without a key never asked; website verification by name and by an identifier on the
legal notice, refused for an unrelated page, a domain another company owns and free mail; and
the favicon in the icon cache. The feeds' tests cover GLEIF delta files (invented zipped
CSVs) making the companies that own their LEIs due, their cached answers dropped and the new
answer written on the next pass, unknown and invalid LEIs left alone, the cursor (nothing
applied twice); one instance per feed under the lease, a failed run kept soft and retried
after its interval, a feed switched off never read; an EDGAR daily index making a known
CIK's 10-K due (not a 10-Q, not unknown CIKs) and EDGAR's 30-day refresh with the feed on;
APR's new set making only the changed MB due, read from the cache on the next pass; and the
Companies House stream (with basic auth) making a company due, resuming each stream from its
timepoint, and restarting a stream whose timepoint is too old (416).

`companies_due` against the query before issue #71: `cargo test -p doewood-store companies_due`
and, with `DOEWOOD_TEST_POSTGRES_URL`, `cargo test -p doewood-pg companies_due`; the timings
with `-- --ignored --nocapture companies_due_timings`.
