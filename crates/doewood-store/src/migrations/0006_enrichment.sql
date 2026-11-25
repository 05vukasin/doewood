-- doewood instance store, schema 6: the enrichment job's storage in standalone mode (ADR 0002
-- sections 2, 5 and 6), the same as shared schema 3. Times are Unix seconds (UTC).

-- Registry answers. `body` is what the client caches; NULL is "no such record". Rows past
-- `expires_at` are not read and are purged on a schedule.
CREATE TABLE registry_cache (
    registry   TEXT NOT NULL CHECK (length(registry) BETWEEN 1 AND 32
                   AND registry NOT GLOB '*[^a-z0-9_]*'),
    key        TEXT NOT NULL CHECK (length(CAST(key AS BLOB)) BETWEEN 1 AND 2048),
    body       BLOB,
    fetched_at INTEGER NOT NULL,
    expires_at INTEGER NOT NULL,
    PRIMARY KEY (registry, key)
) STRICT, WITHOUT ROWID;

CREATE INDEX registry_cache_by_expiry ON registry_cache (expires_at);

-- Favicon lookups; `icon` and `format` are both NULL for a miss.
CREATE TABLE favicons (
    domain     TEXT PRIMARY KEY NOT NULL CHECK (length(domain) BETWEEN 1 AND 253),
    icon       BLOB,
    format     TEXT CHECK (format IN ('ico', 'png', 'gif', 'jpeg', 'webp', 'avif')),
    fetched_at INTEGER NOT NULL,
    expires_at INTEGER NOT NULL,
    CHECK ((icon IS NULL) = (format IS NULL))
) STRICT, WITHOUT ROWID;

CREATE INDEX favicons_by_expiry ON favicons (expires_at);

-- Company freshness: the last enrichment, the document count then, and when each registry is
-- due again. The lease stays in `company_leases`.
CREATE TABLE company_enrichment (
    company_id  TEXT PRIMARY KEY NOT NULL REFERENCES nodes (id) ON DELETE CASCADE,
    enriched_at INTEGER NOT NULL,
    documents   INTEGER NOT NULL CHECK (documents >= 0)
) STRICT, WITHOUT ROWID;

CREATE TABLE company_registry_due (
    company_id  TEXT NOT NULL REFERENCES company_enrichment (company_id) ON DELETE CASCADE,
    registry    TEXT NOT NULL,
    next_due_at INTEGER NOT NULL,
    PRIMARY KEY (company_id, registry)
) STRICT, WITHOUT ROWID;

-- The "companies due" query walks the companies in id order.
CREATE INDEX nodes_companies ON nodes (id) WHERE kind = 'company';
