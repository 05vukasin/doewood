# Registries

`crates/doewood-registry` holds the public registry clients of
[ADR 0002](adr/0002-company-enrichment.md), section 2. `doewood-enrich` stays pure (no I/O);
this crate does the requests and parses the answers into types. GLEIF and VIES are done;
Wikidata, EDGAR, APR and Companies House follow, one at a time.

Nothing calls the clients yet: the enrichment job (ADR 0002 section 6) that turns their answers
into company identifiers and dated edges comes later.

## What is sent

Only identifiers and names: an LEI, a VAT number, or a legal name and a country code. Document
content never leaves the instance (ADR 0002, the privacy rule). An identifier is validated before
it is sent: an LEI must pass ISO 17442 (`doewood_enrich::check::lei`), a VAT number its
country's format and check digits (`doewood_enrich::check::vat`), so a typo or a placeholder is
a warning and no request is made.

## Shared behaviour

Every client is built on the same request loop (`Fetcher`):

- **Failure-soft.** Every failure is a typed `Warning { registry, kind }`, never a panic or a
  failed ingest. The kinds: `Disabled`, `Config`, `InvalidInput`, `Timeout`, `Transport`,
  `TooLarge`, `RateLimited`, `Status`, `Malformed`, and `Vies` (a VIES error code). The loop
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
- **Timeouts.** One for connecting, and one for the whole request, body included.
- **Cached, with a fetch date.** Answers are cached by request under keys like
  `gleif:lei-records/<LEI>`, with the date they were fetched; every answer returns that date
  (`Fetched { value, fetched_at }`). A record is fresh for 30 days (the ADR's refresh), a "no
  such record" (404) for one day. Failures and malformed bodies are never cached. The cache
  keeps the raw (capped) body, so a better parser also applies to cached answers.

The cache is a trait (`Cache`). This step ships `MemoryCache` (in-process, at most 10,000
entries, the oldest fetch evicted first) and `NoCache`. The shared Postgres cache of ADR 0002
section 6 is not here yet: it needs a new table, and a new shared-schema migration makes every
older instance refuse the database (see [configuration.md](configuration.md#the-shared-postgres)),
so it comes with the enrichment job.

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

## Tests

`cargo test -p doewood-registry`. The tests never reach a real registry: a mock HTTP server on
127.0.0.1 answers with fixtures written by hand in the test code (invented companies and
addresses, LEIs and VAT numbers built with valid check digits, invented relationships). For
VIES they cover a valid number with and without (`---`) name and address, an invalid one and its
90-day cache, every error code under 200, 400 and 500 (retried or not, never cached), an outage
followed by an answer, the busy backoff, 429, malformed and oversize bodies, numbers that are
never sent, and the `GR`/`EL` and `XI` prefixes. For GLEIF they cover the lookup, the name search, direct
and ultimate parents with every reporting exception, successors with dates, 404 and empty
results, malformed JSON, oversize bodies (with and without `Content-Length`), 429 with
`Retry-After`, 5xx backoff, timeouts, refused connections, the rate limiter's spacing on an
injected clock, the cache's freshness, and that an invalid LEI is never sent.
