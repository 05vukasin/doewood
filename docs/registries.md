# Registries

`crates/doewood-registry` holds the public registry clients of
[ADR 0002](adr/0002-company-enrichment.md), section 2. `doewood-enrich` stays pure (no I/O);
this crate does the requests and parses the answers into types: GLEIF, VIES, Wikidata,
SEC EDGAR, the Serbian APR and UK Companies House.

The same crate holds the favicon resolver behind `GET /icons/{domain}` (ADR 0002 section 5),
with its SSRF guard; see [favicons.md](favicons.md).

Nothing calls the clients yet: the enrichment job (ADR 0002 section 6) that turns their answers
into company identifiers and dated edges comes later.

## What is sent

Only identifiers and names: an LEI, a VAT number, a Wikidata item id, an EDGAR CIK and
accession number, a Companies House company number, or a legal name and a country code. The APR client sends nothing about a
company at all: it downloads the whole open-data set and looks up MBs and names in it. Document
content never leaves the instance (ADR 0002, the privacy rule). An identifier is validated before
it is sent: an LEI must pass ISO 17442 (`doewood_enrich::check::lei`), a VAT number its
country's format and check digits (`doewood_enrich::check::vat`), so a typo or a placeholder is
a warning and no request is made.

## Shared behaviour

Every client is built on the same request loop (`Fetcher`):

- **Failure-soft.** Every failure is a typed `Warning { registry, kind }`, never a panic or a
  failed ingest. The kinds: `Disabled`, `Config`, `InvalidInput`, `Timeout`, `Transport`,
  `TooLarge`, `RateLimited`, `Status`, `Malformed`, `Unsupported` (the source cannot answer
  that question: APR has no PIB), `NotConfigured` (the registry needs a key and none is set),
  `KeyRefused` (the registry refused the key), `Vies` (a VIES error code), `Wikidata`
  (a Wikidata API error code) and `Edgar` (the SEC refused the request). The loop
  logs each one at `WARN`.
- **Length-capped.** The body is read chunk by chunk and dropped as soon as it passes the cap
  (a larger `Content-Length` is refused before reading). It is never parsed.
- **Typed.** JSON is read leniently (every field optional, unknown fields ignored), then checked:
  an LEI in an answer must pass its check digits, a record must have a legal name, and an answer
  about another entity than the one asked is `Malformed`.
- **Rate-limited.** Each client has its own token bucket; callers wait in turn.
- **Backoff.** A timeout, a connection error, a 5xx or a 429 is retried with a doubling backoff
  (up to 60 s). A 429 with `Retry-After` in seconds waits exactly that long and empties the
  bucket; a `Retry-After` longer than the limit gives up at once with `RateLimited`, so a lookup
  never hangs. Other statuses (400, 403, ...) are not retried. The HTTP-date form of
  `Retry-After` is not parsed; the backoff applies.
- **The registry's own errors.** A client may read each answer before the status rules
  (`Inspect`): to retry, to retry with the bucket emptied, or to fail at once with its own
  warning. VIES uses it for the error codes it puts in the body.
- **`GET` or JSON `POST`.** The transport sends either; VIES's documented endpoint is a `POST`.
  A request may carry HTTP basic auth with an `ApiKey` (Companies House).
- **API keys.** An `ApiKey` is runtime configuration supplied by the operator, never in the
  code. Its `Debug` is `ApiKey(<redacted>)`, it has no `Display`, it never goes into a URL,
  and a client that has one also scrubs it from the text of every warning it returns.
- **User-Agent.** Every request sends `doewood/<version> (+https://github.com/05vukasin/doewood)`
  (`USER_AGENT`): generic, with the project's URL as the contact, as the Wikimedia User-Agent
  policy asks. Never a person's name or email in the code. EDGAR is the one exception: the SEC
  asks for the requester's name and email, which the operator configures (see
  [EDGAR](#sec-edgar)).
- **Trust tier.** `Registry::tier()` is `TrustTier::Official` for GLEIF, VIES, EDGAR, APR
  and Companies House, and `TrustTier::Community` for Wikidata, the lowest tier of ADR
  0002 section 2 (`Community < Official`).
- **Timeouts.** One for connecting, and one for the whole request, body included.
- **Cached, with a fetch date.** Answers are cached by request under keys like
  `gleif:lei-records/<LEI>`, with the date they were fetched; every answer returns that date
  (`Fetched { value, fetched_at }`). A record is fresh for 30 days (the ADR's refresh), a "no
  such record" (404) for one day. Failures and malformed bodies are never cached. The cache
  keeps the raw (capped) body, so a better parser also applies to cached answers.

The cache is a trait (`Cache`), with three implementations:

- `StoreCache`: the store's `registry_cache` table (shared schema 3, instance schema 6), through
  `doewood-model`'s `CacheStore` trait. In shared mode that is the shared Postgres, so an answer
  one instance fetched is reused by every other (ADR 0002 sections 2 and 6); standalone it is
  the instance's SQLite file. A key `<registry>:<request>` is stored under that registry and
  request, with the body as the client hands it over (for Companies House the minimised fields
  only), the fetch date and an expiry: the fetch date plus the client's freshness for that
  answer. Times are kept to the second. The store never returns an expired entry, and the
  binary purges expired rows every hour. A store failure is a miss and is logged at debug
  level; while the shared store is not connected, the cache works in memory.
- `MemoryCache` (in-process, at most 10,000 entries, the oldest fetch evicted first).
- `NoCache`.

`doewood-registry` itself has no database dependency: the backends implement `CacheStore`.
The enrichment job (issue #40) builds its clients with a `StoreCache`.

### Company freshness

ADR 0002 section 6 refreshes a company on each registry's schedule and as soon as its document
count doubles. The store keeps, per company, the last enrichment time, its document count then
(documents with a live `party` edge) and each registry's next due time (`FreshnessStore` in
`doewood-model`; tables `company_enrichment` and `company_registry_due`):

- `record_enrichment(company, [(registry, interval)])` sets the time and the count to now and
  each listed registry's due time to now plus its interval; other registries keep theirs.
- `companies_due(owner, after, limit)` is one page (by id, at most 1000) of the companies with
  at least one document that were never enriched (`New`), whose count is at least twice the
  count then, and at least 2 (`Doubled`), or with a registry due (`Scheduled`, naming the
  registries). A company another instance holds an unexpired lease on is left out.
- The lease is the existing one (`lease_company`, table `company_leases`), unchanged.

Both backends pass the same conformance checks.

## GLEIF

`api.gleif.org/api/v1`, keyless, JSON:API. GLEIF allows 60 requests a minute.

| call | requests | gives |
|---|---|---|
| `lookup(lei)` | `GET lei-records/{lei}` | the LEI record, or `None` |
| `search(name, country)` | `GET lei-records?filter[entity.legalName]=…&filter[entity.legalAddress.country]=…&page[size]=…` | candidate records (first page) |
| `parent(lei, Direct \| Ultimate)` | `GET lei-records/{lei}/{direct,ultimate}-parent-relationship`, then on 404 `…-parent-reporting-exception` | `Reported`, `Exception` or `NotReported` |

An LEI record (`LeiRecord`) has the LEI, the legal name and its language, other names (previous
legal names, trading names), the legal address country and city, the jurisdiction, the
registration authority (`registeredAt`) and the entity's id there (`registeredAs`), the ELF
legal form code, the category, the entity status (active or inactive), the LEI registration
status (`ISSUED`, `LAPSED`, `MERGED`, `RETIRED`, ...) and its last update, the expiration (date
and reason), the successor entities, and the legal-entity events with their effective and
recorded dates.

Level 2:

- **A reported parent** (`ParentRelationship`): the child and parent LEIs, the relationship
  status, and `valid_from` / `valid_to` from the relationship period (not the accounting
  period). The relationship type must match the kind asked (`IS_DIRECTLY_CONSOLIDATED_BY` or
  `IS_ULTIMATELY_CONSOLIDATED_BY`), and an entity is never its own parent.
- **A reporting exception** (`ReportingException`): why no parent is reported.
  `NATURAL_PERSONS`, `NON_CONSOLIDATING`, `NO_KNOWN_PERSON`, `NON_PUBLIC` (and its older
  detailed codes: legal obstacles, consent not obtained, binding legal commitments, disclosure
  detrimental); any other code is kept as `Other` with the code.
- **Successors with dates:** `LeiRecord::successions()` gives each successor entity (an LEI,
  or only a name) with the effective date of the latest completed merger-type event
  (`MERGERS_AND_ACQUISITIONS`, `ABSORPTION`, `BREAKUP`, `TRANSFORMATION_*`), else the
  expiration date. `LeiRecord::renames()` gives the `CHANGE_LEGAL_NAME` events: a rename keeps
  the LEI.

Search returns candidates only. Matching a candidate to a company is doewood-enrich's job (the
fuzzy score and the jurisdiction veto); a registry identifier is then the strongest evidence.
Search results with an invalid LEI or no legal name are skipped.

### Settings

`GleifConfig`, whose defaults are the production values:

| field | default |
|---|---|
| `enabled` | `true` (keyless registries run with nothing configured, ADR 0002 section 7) |
| `base_url` | `https://api.gleif.org/api/v1/` |
| `timeout` / `connect_timeout` | 10 s / 5 s |
| `per_minute` / `burst` | 50 / 5 (at most 55 requests in any minute, under GLEIF's 60) |
| `max_body` | 2 MiB |
| `max_retries` / `backoff` / `max_retry_after` | 2 / 1 s, doubling / 60 s |
| `ttl_found` / `ttl_missing` | 30 days / 1 day |
| `search_size` | 10 (at most 200) |

The on/off switch is not yet a runtime setting: it becomes one (store and API, ADR 0001
section 3) together with the other registries' switches and keys, when the enrichment job uses
the clients.

## VIES

The EU's VAT Information Exchange System, keyless. doewood uses the documented REST endpoint
(`https://ec.europa.eu/taxation_customs/vies/rest-api/`, the `CheckVatRequest` /
`CheckVatResponse` of the EC's OpenAPI description):

| call | requests | gives |
|---|---|---|
| `check(country, number)` | `POST check-vat-number` with `{"countryCode", "vatNumber"}` | `VatRecord`: valid or not, name, address, request date, request identifier |

- **Before sending.** The country must be a VIES country: the 27 member states and `XI`
  (Northern Ireland). Greece is `EL` in VIES: `GR` is accepted and sent as `EL`. `GB` left VIES
  in 2021 and is refused, like any other country. The number may carry its prefix (`GR` too, for
  Greece) and spaces, dots or dashes; it is compacted and validated with doewood-enrich (format,
  and check digits where the country has a public algorithm). A malformed number, a bad check
  digit or a placeholder is `InvalidInput`, and nothing is sent.
- **Nothing else is sent.** No requester VAT number (`requesterMemberStateCode`,
  `requesterNumber`) and no trader details: VIES then gives no consultation number, so
  `request_identifier` is normally `None` (it is kept when VIES sends one).
- **The answer.** `valid` is required. The answer must be about the number asked (its
  `countryCode` and `vatNumber`, when present, must match), else it is `Malformed`. `name` and
  `address` are kept only for a valid number; `---` (the member state does not share them) is
  `None`. The address keeps VIES's line breaks. `requestDate` gives `request_date`.
- **VIES errors** come as `{"actionSucceed": false, "errorWrappers": [{"error": CODE}]}`,
  whatever the HTTP status (200, 4xx or 5xx); a `userError` other than `VALID` / `INVALID` is
  read the same way. Each is a `WarningKind::Vies(ViesError)`, never an "invalid" answer, and
  never cached:

  | code | handling |
  |---|---|
  | `MS_MAX_CONCURRENT_REQ`, `MS_MAX_CONCURRENT_REQ_TIME`, `GLOBAL_MAX_CONCURRENT_REQ`, `GLOBAL_MAX_CONCURRENT_REQ_TIME` | too many requests: the bucket is emptied and the request retried with the backoff |
  | `SERVICE_UNAVAILABLE`, `TIMEOUT` | retried with the backoff |
  | `MS_UNAVAILABLE` | at once (a member state's outage or maintenance lasts hours; the enrichment job asks again later) |
  | `INVALID_INPUT`, `INVALID_REQUESTER_INFO`, `VAT_BLOCKED`, `IP_BLOCKED`, any other code | at once; `ViesError::is_transient` is false for the first four |

- **Cached** under `vies:check-vat-number/<prefix><number>` for 90 days, valid or invalid (the
  ADR's refresh: on first sight, then every 90 days). A 404 is a `Status` warning and is not
  cached: it is not an answer about the number.

VIES publishes no rate limit. It refuses with the "max concurrent" codes when it or a member
state is busy, and blocks addresses that poll hard, so the default is low: 20 requests a minute
with a burst of 2, one bucket for all member states.

### Settings

`ViesConfig`, whose defaults are the production values:

| field | default |
|---|---|
| `enabled` | `true` (keyless, ADR 0002 section 7) |
| `base_url` | `https://ec.europa.eu/taxation_customs/vies/rest-api/` |
| `timeout` / `connect_timeout` | 20 s (VIES waits on the member state) / 5 s |
| `per_minute` / `burst` | 20 / 2 |
| `max_body` | 64 KiB |
| `max_retries` / `backoff` / `max_retry_after` | 2 / 2 s, doubling / 60 s |
| `ttl` | 90 days |

What the EC's OpenAPI description does not say, and is therefore handled defensively: which
HTTP status comes with which error code (all are read from the body), and the full list of error
codes (the eleven above are the faults VIES lists for its test service;
any other is kept as `ViesError::Other`).

## Wikidata

Wikidata, keyless, community-edited: **the lowest trust tier** (`TrustTier::Community`, ADR
0002 section 2). Every item and search hit carries `tier: TrustTier::Community`. How the
enrichment job weighs it: an official registry's fact always wins over a Wikidata one, and a
Wikidata hit is never on its own a "registry identifier" match in the fusion of ADR 0002
section 1.6 (that is the LEI or national id it was found by). Its relations are corroboration
and gap-filling; turning them into dated edges (ADR 0002 section 3) comes with the enrichment
job.

| call | requests | gives |
|---|---|---|
| `by_lei(lei)` | `GET https://query.wikidata.org/sparql?query=…` (items with `wdt:P1278 "<LEI>"`) | the items with that LEI, sorted by id (normally one or none) |
| `item(qid)` | the same query with `VALUES ?item { wd:<Q-id> }` | the item, or `None` |
| `search(name)` | `GET https://www.wikidata.org/w/api.php?action=wbsearchentities&search=…&type=item&language=…&limit=…&format=json&maxlag=5` | candidates: Q-id, label, description, the text matched |

A `WikidataItem` has the Q-id, the label (in the configured language, else the `mul` label),
its LEIs (P1278, validated), and each statement of:

| property | field | value |
|---|---|---|
| P749 parent organization | `parent_organizations` | `Relation` |
| P127 owned by | `owned_by` | `Relation` (an owner may be a person: `human`) |
| P1366 replaced by | `replaced_by` | `Relation` |
| P156 followed by | `followed_by` | `Relation` |
| P856 official website | `websites` | `http(s)` URL only (not verified; ADR 0002 section 5 verifies before use) |
| P154 logo image | `logos` | the Commons file name and `https://commons.wikimedia.org/wiki/Special:FilePath/<file>` |

Each statement has its period (`start` from the P580 start time qualifier, `end` from P582
end time) and whether it has the preferred rank. Deprecated statements are left out; a
statement with an unknown or no value, a value that is not an item, or an item pointing at
itself is skipped. A date keeps its precision (`Day`, `Month`, `Year`): a month is its first
day, a year January 1st, and anything coarser (decade, century) is dropped. A `Relation`'s
`human` is P31 Q5 on the target: a person is never a company. Several start or end times on
one statement give the earliest start and the latest end.

- **Injection.** No caller text reaches a SPARQL query. A Q-id is parsed into a number (`QId`:
  `Q` and 1 to 12 digits, nothing else); an LEI must pass ISO 17442 (compacted to 20 letters
  and digits, and checked again when the query is built); the language is a validated tag
  (`en`, `sr-el`) from the configuration. The query is built from those alone and then
  percent-encoded into the URL. A name only goes to `wbsearchentities`, as one
  percent-encoded URL parameter (whitespace collapsed; empty, longer than 256 characters or
  with control characters is `InvalidInput`), never into SPARQL.
- **Checks.** Every `by_lei` item must list the asked LEI, and `item` must answer about the
  asked Q-id, else `Malformed`. A SPARQL answer that is not SPARQL 1.1 JSON, or names an item
  that is not a Wikidata entity IRI, is `Malformed`. An item without rows (no such item, or a
  redirect) is `None`.
- **API errors.** `wbsearchentities` reports errors as `{"error": {"code": …}}` with a 200:
  `maxlag` is retried with the backoff, `ratelimited` empties the bucket and is retried, any
  other code is a `Wikidata(code)` warning at once (the code sanitized). Never cached.
- **Cached** under `wikidata:sparql/lei/<LEI>?lang=…`, `wikidata:sparql/item/<Q-id>?lang=…` and
  `wikidata:search?<query>` for 30 days (the ADR's refresh), found or empty. Neither endpoint
  answers 404 for "nothing found"; a 404 is a `Status` warning.

The query service's usage policy (the WDQS user manual): at most 5 parallel queries per IP and
60 seconds of query time a minute per client (User-Agent and IP), a 60 s limit per query, 429
with `Retry-After` beyond the limits (clients that keep going are banned), and a User-Agent
following the Wikimedia policy, else blocked. GET is recommended for small queries (the service
caches them). doewood stays well inside: **one request at a time per endpoint** (a gate held
through retries and waits), 20 a minute with no burst, a 20 s timeout, and the shared 429 /
`Retry-After` handling. A query timeout on the service side comes as a 5xx and is retried twice
with the backoff.

### Settings

`WikidataConfig`, whose defaults are the production values:

| field | default |
|---|---|
| `enabled` | `true` (keyless, ADR 0002 section 7) |
| `sparql_url` | `https://query.wikidata.org/sparql` |
| `api_url` | `https://www.wikidata.org/w/api.php` |
| `language` | `en` (the `mul` label is the fallback) |
| `timeout` / `connect_timeout` | 20 s / 5 s |
| `per_minute` / `burst` | 20 / 1, per endpoint, one request in flight per endpoint |
| `max_body` | 1 MiB |
| `max_retries` / `backoff` / `max_retry_after` | 2 / 2 s, doubling / 60 s |
| `ttl` | 30 days |
| `search_size` | 10 (at most 50) |

Assumed rather than confirmed from the documentation, and handled defensively: that every
item has a `wikibase:statements` count in the query service's RDF (the `item` query uses it to
tell an existing item from an unknown id), how unknown values appear (blank nodes or
`.well-known/genid` IRIs: both are skipped), and that the WDQS v1 endpoint stays where it is
(the manual mentions a v2 service; `sparql_url` is a setting).

## SEC EDGAR

SEC EDGAR, keyless, `TrustTier::Official` (SEC filings). Two hosts: `data.sec.gov` for the
submissions API, `www.sec.gov` for the company tickers file and the filing archives.

| call | requests | gives |
|---|---|---|
| `company(cik)` | `GET data.sec.gov/submissions/CIK##########.json` | `Company`, or `None` (404) |
| `ticker(symbol)` | `GET www.sec.gov/files/company_tickers.json` | the entry (CIK, ticker, name), or `None` |
| `search(name)` | the same file | entries whose name has every word of `name` |
| `exhibit21(cik, accession)` | `GET www.sec.gov/Archives/edgar/data/<cik>/<accession digits>/<accession>-index.htm`, then the exhibit | `Exhibit21`, or `None` when the filing has none |
| `subsidiaries(cik)` | `company`, then `exhibit21` of the newest 10-K | `Exhibit21` with the 10-K's filing date |
| `acquisition_text(cik, filing)` | `GET` the filing's primary document | the text from "Item 2.01" on, or `None` |

- **A company** (`Company`): the CIK, name, entity type, SIC code and description, tickers and
  exchanges, state of incorporation, fiscal year end, **former names** with their `from` / `to`
  days, and the recent filings (the SEC lists at least a year, or the last 1,000): form, filing
  and report dates, accession number, 8-K items, primary document. The older filings' file
  names are kept (`older_filing_files`) but not fetched. The answer must be about the CIK asked,
  else `Malformed`. Filings with an invalid accession number are skipped, and a primary document
  name that is not a plain file name (`A-Z a-z 0-9 . _ -`) is dropped.
- **Completed acquisitions.** `Company::acquisitions()` gives the 8-K and 8-K/A filings whose
  items include `2.01` ("Completion of Acquisition or Disposition of Assets"), with the filing
  date and accession number: the dated `acquired` events of ADR 0002 section 3. The text window
  (`acquisition_text`) is best-effort: the primary document's text from the first "Item 2.01"
  followed by "Completion" (else the first "Item 2.01"), cut before the next "Item N.NN" and at
  `item_chars` characters.
- **Exhibit 21** (subsidiaries of the registrant). The filing index page's document table is
  read (columns found by their `Document` and `Type` headers, else EDGAR's order), and the first
  document of type `EX-21` or `EX-21.<n>` is fetched. Its rows are **best-effort**
  (`Exhibit21::extraction`): HTML table rows give the name in the first cell and the
  jurisdiction in the last one that is not an ownership share; without tables, lines are read
  (`Name (Delaware)`, `Name, a Delaware corporation`, `Name - Delaware`, tab, dot-leader or
  wide-space separated, or a name alone). Headers, titles, numbering and page numbers are
  skipped. At most `max_subsidiaries` rows (`truncated` says there were more). Formats vary
  widely, so a row is a candidate to corroborate before it becomes an edge. The HTML reader is a
  small bounded routine in the crate (no new dependency); odd or garbage input gives fewer rows,
  never a panic.
- **Name and ticker lookup.** The tickers file lists only companies with a listed ticker. It is
  fetched whole (capped at 4 MiB) and searched on the instance, so the name is never sent.
- **Before sending.** A CIK is 1 to 10 digits (optionally after `CIK`), not zero, and sent
  zero-padded to 10 digits; an accession number is `0000000000-00-000000` or its 18 digits; a
  document name is a plain file name from the filing index or the submissions. Anything else is
  `InvalidInput`, and nothing is sent. No other text reaches a URL.
- **User-Agent and contact.** The SEC's fair-access policy asks automated clients to declare a
  User-Agent with the requester's name and a contact email (its example: `Sample Company Name
  AdminContact@<sample company domain>.com`). This contact is runtime configuration (ADR 0002
  section 7), `EdgarConfig::contact`, set by the operator: then every EDGAR request sends
  `<contact> doewood/<version> (+https://github.com/05vukasin/doewood)`. With none set, EDGAR
  runs with the generic `USER_AGENT` (ADR 0002 section 7). The SEC may refuse that with a 403;
  **operators should set their own contact.** The contact is printable ASCII, at most 200
  characters (else a `Config` warning). doewood ships no contact, and never a person's name or
  email.
- **Fair access.** The SEC allows at most 10 requests a second per user, across all its hosts
  and machines. Every EDGAR request of a client (submissions, tickers, indexes, documents)
  shares one token bucket: 300 a minute (5 a second) with a burst of 2, so at most 7 in any
  second. A 429 and 5xx get the shared backoff and `Retry-After` handling.
- **403.** The SEC answers 403 for an undeclared User-Agent and when the rate threshold is
  exceeded. A 403 is never retried: it is `Edgar(UndeclaredUserAgent)`, `Edgar(RateThreshold)`
  or `Edgar(Forbidden)` (from the page text), and the client then sends nothing for `pause`
  (10 minutes); requests meanwhile are `RateLimited` with the time left. Cached answers are
  still served.
- **Cached** under `edgar:submissions/CIK##########` for 1 day (found or 404),
  `edgar:files/company_tickers.json` for 1 day, and `edgar:Archives/edgar/data/...` (index
  pages, exhibits, 8-K documents) for 30 days: a filing under an accession number does not
  change. A missing index or exhibit is a `Status(404)` warning and is not cached; a missing 8-K
  document is `None`, cached for a day.
- **Refresh.** ADR 0002 refreshes EDGAR "when new filings appear (RSS)". Without the feed, the
  1-day submissions TTL bounds how late a new filing is seen. Reading EDGAR's RSS (or the
  daily index) to refresh only the companies with new filings is an open item, for the
  enrichment job (ADR 0002 section 6).

### Settings

`EdgarConfig`, whose defaults are the production values:

| field | default |
|---|---|
| `enabled` | `true` (keyless, ADR 0002 section 7) |
| `contact` | none: the generic `USER_AGENT` (set your own, such as `Example Ops ops@example.invalid`) |
| `data_url` / `www_url` | `https://data.sec.gov/` / `https://www.sec.gov/` |
| `timeout` / `connect_timeout` | 20 s / 5 s |
| `per_minute` / `burst` | 300 / 2, one bucket for every EDGAR request |
| `max_submissions` / `max_tickers` / `max_index` / `max_exhibit` / `max_document` | 4 MiB / 4 MiB / 1 MiB / 2 MiB / 4 MiB |
| `max_retries` / `backoff` / `max_retry_after` | 2 / 1 s, doubling / 60 s |
| `pause` | 10 minutes after a 403 |
| `ttl_submissions` / `ttl_tickers` / `ttl_archive` | 1 day / 1 day / 30 days |
| `search_size` | 10 (at most 100) |
| `max_subsidiaries` / `item_chars` | 5,000 / 2,000 |

Confirmed from the SEC's developer pages: the submissions URL (`CIK` and 10 digits), that it
holds the current and former names, tickers and exchanges, at least a year or 1,000 filings in
columnar arrays plus files for older ones; the 10 requests a second; the declared User-Agent
and its sample form. Assumed, and handled defensively: the JSON field names (`formerNames`
with `name` / `from` / `to`, `filings.recent.accessionNumber`, `items`, `primaryDocument`, ...;
every field optional), the tickers file's shape (`cik_str`, `ticker`, `title`), the filing index
page's table and its `Document` / `Type` columns, the 403 page texts ("Undeclared Automated
Tool", "Request Rate Threshold Exceeded"; any other 403 is `Forbidden`), and the length of the
SEC's block (the default pause is 10 minutes).

## APR

The Serbian Business Registers Agency (Агенција за привредне регистре, APR), keyless, through
its **official open data**.

**Source.** The data.gov.rs dataset "АПИ за Регистар привредних друштава"
(`data.gov.rs/sr/datasets/api-za-registar-privrednikh-drushtava/`, publisher: APR), whose
resource is `https://openapi.apr.gov.rs/api/opendata/companies`: one JSON document with the
snapshot date and every registered company that is active, in liquidation, in forced
liquidation or in bankruptcy. It is one of the EU high-value datasets (Implementing Regulation
2023/138). APR updates it **monthly** (the dataset says "једном месечно"); it was about 58 MB
for some 134,000 companies in November 2026. No delta feed is published.

**Licence.** The dataset's licence is the Serbian open data licence (data.gov.rs terms,
"Лиценца за коришћење података"): free reuse by anyone, commercial or not, including
copying, adapting and combining, with **attribution**: name the publishing body (APR), the
download date, the address the data came from, and mark any change. Every record carries the
snapshot date (`as_of`) and the download date (`fetched_at`) for that.

**Why not the search pages.** APR's website terms ("Terms and Conditions of Use") forbid web
robots for downloading data and search results, and allow copying only for personal,
non-commercial use. APR's own data service ("e-data on request") and its web service are paid
and contract-based. The client uses neither.

| call | requests | gives |
|---|---|---|
| `company(mb)` / `lookup(IdKind::RsMb, mb)` | the set (once per 30 days) | the company, or `None` |
| `lookup(IdKind::RsPib, pib)` | nothing | `Unsupported`: the set has no PIB |
| `search(name)` | the set (once per 30 days) | candidate companies |

A company (`apr::Company`) has the MB, the business name as registered (`PoslovnoIme`) and its
script (Cyrillic or Latin; the set gives one spelling only, and matching keys are
script-blind), the status (`Active`, `InLiquidation`, `InForcedLiquidation`, `InBankruptcy`,
`Deleted`, or `Other` as given), the legal form (`LimitedLiability`, `JointStock`, the older
open and closed joint-stock forms, `Partnership`, `LimitedPartnership`, `PublicEnterprise`,
`SocialEnterprise`, `Cooperative`, `CooperativeUnion`, `ForeignRepresentativeOffice`,
`ForeignBranch`, or `Other`; `canonical()` gives the `doewood_enrich::LEGAL_FORMS` spelling such
as `d.o.o.`), the founding date, the municipality of the seat (code and name), the activity
code, and the snapshot date. Statuses and legal forms are read in either script.

- **Not in the set:** the PIB, deleted companies (a deleted company is absent, so `None`;
  `Deleted` is read in case APR adds them), the date of a status change, statutory changes,
  mergers and divisions, and the street address. ADR 0002 lists these for APR; the open data
  does not have them, and APR's search pages may not be scraped (above). They stay open.
- **Before sending.** An MB must pass `doewood_enrich::check::rs_mb` (8 digits, MOD 11), a PIB
  `rs_pib` (9 digits, MOD 11,10); placeholders are refused. Anything else is `InvalidInput`. No
  identifier or name ever reaches a URL.
- **The set.** It is downloaded whole, capped (`max_body`, 128 MiB), and read with a streaming
  visitor into a compact index by MB (no JSON tree; each name kept once, the low-cardinality
  fields interned). The body is dropped once indexed; the index is a fraction of it. At most
  `max_companies` (400,000) entries; a larger set is `Malformed`. Unknown fields are ignored, a
  key that is not eight digits or a record without a name is skipped, a value that is not a
  string or a number is empty. The set is UTF-8 JSON (a byte order mark is tolerated); any
  other encoding, such as Windows-1251, is `Malformed` rather than misread. Indexing and
  searching run on Tokio's blocking pool.
- **One download.** The snapshot is shared by every caller; concurrent lookups wait for one
  download. It is fresh for `ttl` (30 days, the ADR's refresh), then downloaded again.
- **Failures.** A failed download (transport, status, oversize, malformed) is never cached.
  The client then sends nothing for `failure_pause` (15 minutes) and answers with the same
  warning meanwhile, so a broken set is not downloaded on every lookup. 429 and 5xx get the
  shared backoff and `Retry-After` handling.
- **Cached** by MB under `apr:companies/<MB>`, with the download's date: a company found is
  fresh for 30 days (until the set is), "no such MB" for one day. The cached body is the
  company's raw fields, so a better reader applies to it. With a shared cache a second instance
  answers without downloading the set.
- **Name search.** Every word of the name (after `doewood_enrich::normalize`: Cyrillic to
  Latin, marks, case and punctuation dropped) must be in the company's name; exact names first,
  then by MB, at most `search_size`. Matching a candidate to a company is doewood-enrich's.
- **TLS.** In November 2026 `openapi.apr.gov.rs` sends its certificate (`*.apr.gov.rs`, issued
  by "SSL2BUY EMEA RSA Domain Validation Secure Server CA") **without the intermediate**. The
  platform verifier does not fetch intermediates, so the download fails with
  `Transport(... UnknownIssuer)`. The operator can supply that intermediate (its URL is in the
  leaf's Authority Information Access, `crt.sectigo.com`; it chains to Sectigo's public root)
  as `extra_roots_pem`, a PEM bundle trusted next to the system's roots. doewood ships none.

### Settings

`AprConfig`, whose defaults are the production values:

| field | default |
|---|---|
| `enabled` | `true` (keyless, ADR 0002 section 7) |
| `url` | `https://openapi.apr.gov.rs/api/opendata/companies` |
| `timeout` / `connect_timeout` | 5 minutes / 10 s |
| `per_minute` / `burst` | 1 / 1 |
| `max_body` / `max_companies` | 128 MiB / 400,000 |
| `max_retries` / `backoff` / `max_retry_after` | 2 / 10 s, doubling / 60 s |
| `ttl` / `ttl_missing` | 30 days / 1 day |
| `failure_pause` | 15 minutes |
| `search_size` | 10 (at most 100) |
| `extra_roots_pem` | none (see TLS above) |

Confirmed from the data.gov.rs dataset metadata and a look at the set's structure (field names
and value vocabularies only, nothing stored): the URL, the top-level `DatumPreseka` and
`Podaci` (an object keyed by the eight-digit MB), the fields `PoslovnoIme`, `SifraOpstine`,
`NazivOpstine`, `NazivStatus`, `DatumOsnivanja`, `NazivPravneForme`, `SifraDelatnosti`, all
strings, ISO dates; the four statuses and thirteen legal-form names in Cyrillic; names in
both scripts (about 8% Cyrillic); that every MB but one passes the MOD 11 check; the licence;
the monthly update; the missing intermediate. Assumed: that the shape stays stable (the
reader is lenient), the spellings of a deleted status, and that no PIB or event data will be
added soon.

## Companies House

UK Companies House's public data API (`https://api.company-information.service.gov.uk/`),
JSON, `TrustTier::Official`. It needs a **free API key**, which the operator registers with
Companies House and configures (`CompaniesHouseConfig::api_key`; later from the encrypted
settings store, ADR 0002 section 7). The key is sent as Companies House specifies: as the user
name of HTTP basic auth, with an empty password (`Authorization: Basic base64("<key>:")`).
doewood ships no key.

| call | requests | gives |
|---|---|---|
| `company(number)` | `GET company/{number}` | `Company`, or `None` (404) |
| `officers(number)` | `GET company/{number}/officers?items_per_page=…&start_index=…`, every page | `Listing<Officer>` |
| `significant_control(number)` | `GET company/{number}/persons-with-significant-control?…`, every page | `Listing<Psc>` |
| `filing_events(number)` | `GET company/{number}/filing-history?…`, every page | `Listing<Filing>`: name changes and mergers only |
| `search(name)` | `GET search/companies?q=…&items_per_page=…&start_index=0` | candidates (`SearchHit`) |

- **A company** (`Company`): the number, the name, the status (`Active`, `Dissolved`,
  `Liquidation`, `Receivership`, `Administration`, `VoluntaryArrangement`, `ConvertedClosed`,
  `InsolvencyProceedings`, `Registered`, `Removed`, `Open`, `Closed`, or `Other` as sent) and
  its detail (`active-proposal-to-strike-off`, `converted-to-plc`, ...), the type, the
  jurisdiction, the creation and cessation dates, the **previous names** with `from` / `to`
  (the dated `renamed_to` events of ADR 0002 section 3), the SIC codes, the registered office
  and whether it has an insolvency history. It must be about the number asked and have a name,
  else `Malformed`.
- **Officers** (`Officer`): name, role (`director`, `secretary`, `llp-member`,
  `corporate-director`, ...), whether it is a company or firm (a `corporate-*` role), appointed
  and resigned dates, nationality and country of residence; for a corporate officer its
  registration (`Registration`: identification type, registration number, register, country,
  legal authority, legal form).
- **People with significant control** (`Psc`): the kind (`Individual`, `CorporateEntity`,
  `LegalPerson`, `SuperSecure`, or `Other`; an overseas entity's `*-beneficial-owner` kinds map
  to the same four, with `beneficial_owner` set), the name (none for a super-secure PSC), the
  notified and ceased dates, the natures of control as sent
  (`ownership-of-shares-75-to-100-percent`, ...), nationality and country of residence for an
  individual, and for a corporate entity or legal person its `Registration`: a corporate PSC's
  registration number and country are how ownership chains are followed (a UK company's number
  leads to its own profile and PSCs).
- **Filing history.** The whole history is read page by page (newest first, at most
  `max_filings` filings) and only the corporate events are kept, with their filing date and
  action date: **name changes** (category `change-of-name`, or a description with
  `change-of-name`; not the `change-of-name-exemption`) and **mergers** (a description with
  `merger`, such as `formation-by-merger-of-societas-europaea`, a cross-border merger form
  `CB<digits>`, or a legacy filing whose text mentions a merger, kept as `text`). The merger
  detection is best-effort.
- **Paging.** Lists are read with `items_per_page` = `page_size` (100) until `total_results`
  is reached, a page is empty, or the cap (`max_items`, 1,000; `max_filings` for the filing
  history) is passed; `Listing::truncated` says the cap stopped it. Each page is cached on its
  own; the listing's fetch date is its oldest page's. A 404 on a list is an empty list (an
  unknown company; assumed also for a company with no PSC).
- **Name search.** The name is whitespace-collapsed and sent as one percent-encoded `q`
  parameter (empty, longer than 256 characters or with control characters is
  `InvalidInput`). Hits without a valid company number or a title are skipped. Candidates
  only: matching is doewood-enrich's.
- **Before sending.** A company number is eight characters: eight digits, or two letters and
  six digits (`SC`, `NI`, `OC`, `LP`, ...; `R0` and six digits), case and spaces ignored,
  zero-padded after the prefix (`990001` is `00990001`, `sc12345` is `SC012345`); all zeros
  are refused. Anything else is `InvalidInput`, and nothing is sent. No other caller text
  reaches a path.
- **No key.** With no key configured, every call is `NotConfigured` and nothing is sent (ADR
  0002 section 7: "everything else is skipped"). `enabled: false` is `Disabled`.
- **401.** A refused key is `KeyRefused`, never retried; the client then sends nothing for
  `key_pause` (30 minutes) and answers `KeyRefused` meanwhile; cached answers are still
  served. The warning never holds the key, and a client with a key scrubs it from every
  warning text it returns.
- **Rate limit.** Companies House allows 600 requests in 5 minutes per key and answers 429
  beyond. One bucket for every endpoint: 100 a minute with a burst of 10, so at most 510 in any
  5 minutes. A 429 and 5xx get the shared backoff (5 s, doubling) and `Retry-After` handling.
- **Cached** under `companies_house:company/<number>`, `companies_house:company/<number>/
  {officers,persons-with-significant-control,filing-history}?items_per_page=…&start_index=…`
  and `companies_house:search/companies?<query>` for 30 days (the ADR's refresh); a 404 for a
  day. Failures and malformed bodies are never cached.
- **Streaming API.** ADR 0002 lists the Companies House streaming API as optional. It is not
  implemented: it is an open item for the enrichment job's refresh (ADR 0002 section 6).

### Personal data

Officers and PSCs are mostly natural persons. Their entries are public registry data (ADR
0002's privacy rule allows public registry data), but doewood keeps them minimal:

- **Kept** for a natural person: the name as registered, the role, the appointed / resigned
  (officers) or notified / ceased (PSCs) dates, the nationality and the country of residence
  when given, and a PSC's natures of control.
- **Dropped** when the answer is read: the date of birth (Companies House gives month and
  year), the residential, correspondence or service address, the occupation, the name parts
  (`name_elements`), the officer id and appointment links, and any identification block on a
  natural person. A super-secure PSC keeps only its kind, dates and natures of control.
- **Never cached either.** The cache otherwise keeps raw bodies; officer, PSC and filing-history
  pages are cached as the kept fields only (re-serialized after the read), so a dropped field
  never reaches the cache, and later the shared Postgres cache. Filing-history pages keep only
  the name-change and merger filings, without their `description_values` (which may hold an
  officer's name) unless the filing is a legacy one whose free text is the description.
- **Companies keep their registration.** A corporate officer or a corporate-entity or
  legal-person PSC keeps its identification (registration number, register, country, legal
  authority and form): that is how ownership chains are built. A company's registered office
  is kept; it is not a person's address.

### Settings

`CompaniesHouseConfig`, whose defaults are the production values:

| field | default |
|---|---|
| `enabled` | `true`, but nothing is sent without a key |
| `api_key` | none (`NotConfigured`); `ApiKey::new` takes 8 to 256 printable ASCII characters without spaces or `:` |
| `base_url` | `https://api.company-information.service.gov.uk/` |
| `timeout` / `connect_timeout` | 15 s / 5 s |
| `per_minute` / `burst` | 100 / 10, one bucket for every endpoint |
| `max_body` | 1 MiB |
| `max_retries` / `backoff` / `max_retry_after` | 2 / 5 s, doubling / 60 s |
| `key_pause` | 30 minutes after a 401 |
| `ttl` / `ttl_missing` | 30 days / 1 day |
| `page_size` | 100 (at most 100) |
| `max_items` / `max_filings` | 1,000 / 2,000 |
| `search_size` | 10 (at most 100) |

Confirmed from the Companies House developer documentation and its published enumerations
(`companieshouse/api-enumerations`): the host; basic auth with the key as the user name and no
password; the 600 requests per 5 minutes and the 429; the endpoints and their `items_per_page`
/ `start_index` paging; the profile fields (`company_number`, `company_name`,
`company_status`, `company_status_detail`, `type`, `jurisdiction`, `date_of_creation`,
`date_of_cessation`, `previous_company_names` with `name` / `effective_from` / `ceased_on`,
`sic_codes`, `registered_office_address`); the company statuses; the officer roles; the PSC
kinds and natures of control; the filing-history fields (`category`, `type`, `description`,
`description_values`, `date`, `action_date`, `transaction_id`) and the name-change
descriptions. Assumed, and handled defensively: that a 401 is the only answer to a bad key
(another 4xx is a `Status` warning), that a 404 on a list means no list, the exact form codes
of merger filings (the enumerations list only `formation-by-merger-of-societas-europaea`;
`CB<digits>` and legacy text are a best-effort reading), and that `items_per_page` above 100 is
not honoured (the default asks for 100).

## Tests

`cargo test -p doewood-registry`. The tests never reach a real registry: a mock HTTP server on
127.0.0.1 answers with fixtures written by hand in the test code (invented companies and
addresses, LEIs and VAT numbers built with valid check digits, invented relationships, and
invented Wikidata items from Q999000001 up, and invented EDGAR companies, CIKs from 9990000001
up, filings, filing indexes and Exhibit 21 documents, with `example.invalid` contacts, an
invented APR set of Cyrillic and Latin names with MBs from 9900001x up, and invented Companies
House companies from 99000001 up with obviously fake officers and PSCs, and the key
`test-key`). For Companies House they cover the profile with previous names and its 30-day
cache, every status, 404 and answers about another company, officers over two pages and the
cap, every PSC kind (a corporate PSC with its registration number), that natural persons'
dates of birth, addresses, occupations and ids are neither returned nor cached, name changes
and mergers from a paged filing history, the name search, that nothing is sent without a key
or for an invalid number, the basic-auth header, a 401 that pauses the client and never shows
the key (in warnings or the configuration's `Debug`), 429 and 5xx backoff, oversize and
malformed bodies (never cached), one rate limit over every endpoint, and configuration
errors. For APR they cover
the lookup by MB (one download for many lookups and for concurrent callers), every status and
legal form, a PIB refused as unsupported, MBs, PIBs and names that are never sent, not found,
the script-blind name search done locally, the 30-day refresh, a shared cache, malformed,
truncated and oversize sets (never cached, with the pause after a failure), too many
companies, a missing set, 429 and 5xx backoff, UTF-8 with a byte order mark and a refused
Windows-1251 body, lenient fields, and the configuration errors. For
EDGAR they cover the lookup by CIK with former names, tickers and filings, the cache's day,
404 and answers about another CIK, CIKs and tickers that are never sent, the ticker and name
lookup (the file fetched once, no name sent), the filing index with and without an EX-21,
Exhibit 21 tables, paragraphs, plain text, garbage input and the row cap, 8-K Item 2.01
detection and its capped text, the 403 for an undeclared User-Agent and the pause after it,
the rate-threshold 403, 429 and 5xx backoff, oversize documents, one rate limit over every
document, and that the configured contact is in the User-Agent while the default is the
generic one. For Wikidata they cover the LEI and Q-id lookups
with relations, dates and precisions, missing qualifiers, several parents, deprecated and
unknown values, empty results, answers about another entity, malformed JSON, oversize bodies,
429 with `Retry-After`, 5xx retries, timeouts, hostile ids, LEIs and names (quotes, braces,
newlines, `&`) refused or only ever percent-encoded into the search API, the API error codes,
one request at a time, the rate limiter, and the generic User-Agent. For
VIES they cover a valid number with and without (`---`) name and address, an invalid one and its
90-day cache, every error code under 200, 400 and 500 (retried or not, never cached), an outage
followed by an answer, the busy backoff, 429, malformed and oversize bodies, numbers that are
never sent, and the `GR`/`EL` and `XI` prefixes. For GLEIF they cover the lookup, the name search, direct
and ultimate parents with every reporting exception, successors with dates, 404 and empty
results, malformed JSON, oversize bodies (with and without `Content-Length`), 429 with
`Retry-After`, 5xx backoff, timeouts, refused connections, the rate limiter's spacing on an
injected clock, the cache's freshness, and that an invalid LEI is never sent.
