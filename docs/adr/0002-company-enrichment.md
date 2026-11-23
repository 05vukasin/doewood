# ADR 0002: company identity, enrichment and corporate events

Status: accepted, 2026-11-23

## Context

The graph (ADR 0001, section 7) merges `Company` nodes from what documents say. Matching gets far
more accurate, and stays current, when companies are also linked to public registries. Registries
know legal names, identifiers, parents and subsidiaries, mergers, renames and liquidations, which
answers "who acquired whom, and when".

An earlier private system taught us what works:

- **For identity:** a deterministic matching ladder, and calibrated evidence fusion with an LLM
  that only advises.
- **For hierarchy:** relation extraction from web text with strict checks, and a favicon
  resolver.

Its main weakness is also the lesson: it relies on web search plus an LLM for hierarchy, uses one
registry shallowly, and has no refresh schedule. doewood re-implements the ideas, never the data.

## Decisions

### 1. Identity: deterministic first, evidence-fused, the LLM advises

1. **Normalization.** NFKD, drop combining marks, casefold, delete punctuation ("U.K." → `uk`),
   collapse spaces. Cyrillic is transliterated to Latin for the key (Serbian, Macedonian,
   Bulgarian, Russian, Ukrainian), and the original spelling is kept. Vocabularies:
   - legal forms, with spelling variants and Balkan forms (d.o.o., a.d., d.d., j.p., PR, s.p.,
     dooel);
   - noise and trade words, geography words (removed from the key, never a veto), and contract role
     terms ("Supplier", "Naručilac"; never a company).
2. **Aliases from parentheses.** "trading as", d/b/a, f/k/a, n/k/a, "formerly", "u daljem tekstu"
   become aliases. The bracketed text is removed from the base key.
3. **The match ladder,** tried in order:
   1. exact
   2. lowercase
   3. normalized
   4. annotations stripped
   5. learned alias
   6. squashed (no spaces)
   7. legal-form-blind (only when exactly one company owns the key)
   8. annotation
   9. acronym
   10. fuzzy token match
   11. fuzzy alias

   Only steps 1–7 may be saved as permanent aliases. The fuzzy steps decide for one document only.
4. **Fuzzy name score.** Tokens are paired one to one, allowing one Damerau-Levenshtein edit on
   tokens of 5+ characters; CJK tokens get no fuzziness. Then:
   - a subset match may add at most one token, and must share at least 2;
   - otherwise Jaccard ≥ 0.6 is required;
   - an overlap made only of typo matches is not enough;
   - **veto:** if the names share at most one core word and are in different jurisdictions, they
     are different companies ("Bank of America" ≠ "Bank of China").
5. **Identifiers are validated before use:**
   - checksums: PIB (mod 11), MB, the VAT format per country (with a VIES check, below), LEI
     (ISO 17442 mod 97), IBAN (mod 97), DUNS (format only);
   - write guards: reject placeholders and label text, and refuse an identifier that another
     company already owns;
   - free-mail domains never count as identity.
6. **Evidence fusion and the decision gate.**
   - Each evidence source has a reliability, and the probability that a match is right is the
     noisy-OR over **distinct** sources:

     | evidence source | reliability |
     |---|---|
     | registry identifier | 0.95 |
     | parent contract | 0.95 |
     | email domain | 0.90 |
     | name ladder (scaled down on the fuzzy steps) | 0.90 |
     | filename | 0.85 |
     | invoice layout | 0.75 |
     | semantic similarity | 0.55 |

   - Merge automatically at ≥ 0.90.
   - Create a new company only when every candidate is below 0.50.
   - Two conflicting high-precision hits always go to review.
   - An LLM verdict is advisory, never decisive.
7. **Stable ids.** `group_id = uuid5(NAMESPACE, normalized LEI)` (or APR MB, or another national
   registry id), so merges are idempotent across instances and batches. A *different* LEI never
   splits a group, because branches have their own LEIs.
8. **Every merge is reversible.** Each merge or split is logged with its evidence and an undo
   payload. A human dismissal ("these are not the same") is permanent in both directions.

### 2. Registries: structured sources first

Registry clients are pure Rust. Each one is failure-soft: a failure is a warning, never a crash.
Every response is length-capped and parsed into types, and cached in Postgres with a fetch date.
Each client has its own rate limiter (a token bucket) and backoff. No key is needed unless noted.

| registry | what it gives | refresh |
|---|---|---|
| **GLEIF** (api.gleif.org) | LEI, legal name, `registeredAs`, status. **Level 2**: `direct-parent` and `ultimate-parent` relationships, and `successorEntity` (mergers and renames), with dates | delta files daily; each company re-checked every 30 days |
| **Serbian APR** (public registry search/open data) | MB, PIB, name, status (active, in liquidation, deleted), legal form, statutory changes, status-change dates, mergers and divisions | 30 days, plus open-data deltas when available |
| **UK Companies House** (free API key) | company number, status, officers, **people with significant control**, filing history (mergers, name changes) | 30 days, plus the streaming API (optional) |
| **SEC EDGAR** (free, a User-Agent is required) | CIK, former names, **Exhibit 21 subsidiaries**, 8-K Item 2.01 (completed acquisitions) | when new filings appear (RSS) |
| **EU VIES** | checks that a VAT number is valid; registered name and address | on first sight, then 90 days |
| **Wikidata** (SPARQL) | parent organization, owned by, `replaced by`, `followed by`, official website, logo, with dates | 30 days; the lowest trust tier |
| **OpenCorporates** (optional key) | registry ids across 140+ jurisdictions | optional tier |

Registries are queried in jurisdiction order: the country of the identifier first, then global
(GLEIF), then Wikidata. A registry identifier is the strongest evidence there is.

### 3. Corporate events: dated edges

- Hierarchy and history become **dated edges** between companies:
  `parent_of`, `subsidiary_of`, `acquired {date}`, `merged_into {date}`, `renamed_to {date}`,
  `branch_of`, `trading_name_of`, `former_parent_of`, `liquidated {date}`.
- Every edge carries:
  - `source` (registry, document, web, manual);
  - `evidence` (a registry record id, or a document with page and span, or a URL);
  - `valid_from` / `valid_to`;
  - `confidence`;
  - `state` (proposed, active, contested, retracted, superseded).
- So "who owned X on 2022-05-01" is a query, not a guess.
- **Trust order:** registry, then a document statement on 2+ documents, then web, then an LLM
  suggestion.
  - Web- and LLM-derived edges are stored as **proposed** and never become active alone.
  - A group fold needs a corroborator: a registry, a document identifier, or the company master.
- When two parent claims disagree, the later dated one is the current parent and the other becomes
  the former parent.

### 4. Web and LLM relation extraction (optional, after the registries)

This tier is only for what registries do not cover. It goes through the LLM presets from ADR 0001
and an optional search API, and every extracted relation passes the same checks:

- **The quote must be in the source.** A fingerprint of the quoted sentence must appear in the
  fetched text, otherwise the relation is dropped.
- **Share trades are rejected:** "acquired a 10% stake" is not an acquisition.
- **Direction comes from grammar,** not from the model. "X was acquired by Y" means Y owns X.
- **Confidence is capped:** a parent claim keeps confidence ≥ 0.8 only if the source is a
  registry- or reference-class domain.
- **The source ranking orders the evidence:** first-party site, then registry or reference site,
  then press, then aggregator.
- **Pairs are fixed:** a partnership or joint venture vetoes merging that pair.
- **Rows are proposed only:** a person or a registry makes them active.

Search snippets are used transiently. Only the URL, the title and our own extracted quote are
stored, as most search APIs' terms require.

### 5. Website and favicon

- **Finding the website:**
  1. a registry or Wikidata `official website`;
  2. otherwise the email domains seen in the company's documents (free-mail excluded);
  3. otherwise a search result.

  A candidate is **verified** before it is stored: DNS resolves, the homepage title or legal
  notice contains the normalized name or an identifier, and the domain is not owned by another
  company. Never guess a `.com` from the name without verifying it.
- **Favicon resolver and proxy** (`GET /icons/{domain}`, served from doewood, so third parties
  never learn which companies a user looks at):
  1. **Try the site first:** `https://{domain}/favicon.ico`, then the homepage (max 200 KB) for at
     most 2 `<link rel~=icon>` hrefs. `data:` URIs are skipped and relative URLs are resolved.
  2. **Optional fallback:** a public favicon service.
     - Self-calibrating globe check: fetch the same service for an RFC 2606 `.invalid` domain. If
       the bytes are identical, it is the placeholder globe and is rejected.
  3. **SSRF guard:**
     - every resolved address must be public;
     - redirects are followed by hand (max 5), and every hop is re-checked;
     - an optional egress proxy.
  4. **Content checks:**
     - ICO, PNG, GIF, JPEG, WEBP or AVIF by magic bytes, **never SVG**, max 200 KB;
     - blank icons are rejected: fully transparent, one flat colour, near-white, or under 8 px.
  5. **Cache in Postgres:** a hit is kept 30 days, a miss 6 hours. Time budgets: 6 s for the site,
     5 s for the fallback, 12 s in total.

### 6. Freshness

- **An enrichment job per instance** works through the companies its documents mention.
  - New companies are enriched right away.
  - Known ones are refreshed on the schedule in the table above, and **as soon as their document
    count doubles**.
  - Registry delta feeds (GLEIF daily files, Companies House streaming, EDGAR RSS) are applied when
    configured.
- **Companies are shared through Postgres,** so when several instances see the same company, the
  first one enriches it (a lease on the company row) and the others reuse the result.

### 7. Configuration

All of this is runtime configuration (ADR 0001, section 3):

- registries on/off, and keys where one is needed (Companies House, OpenCorporates);
- the EDGAR User-Agent contact string;
- the search API (off by default);
- the LLM tier (off by default);
- the favicon fallback service (on/off).

With nothing configured, the keyless registries run (GLEIF, VIES, EDGAR with a generic User-Agent,
Wikidata, APR) and everything else is skipped.

## Consequences

- Most hierarchy facts come from registries with dates, so they are current and auditable. Web and
  LLM fill gaps, but only as proposals.
- Registry rate limits bound how fast a cold start can enrich. The first full enrichment of a large
  corpus takes hours, while the rest of ingest is not blocked.
- The privacy rule from the earlier system stays: only public registry data and our own extracted
  facts are stored. Documents never leave the instance except to the LLM the user configured.
