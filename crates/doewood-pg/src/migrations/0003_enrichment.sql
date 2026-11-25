-- doewood shared store, schema 3: the enrichment job's storage (ADR 0002 sections 2, 5 and 6)
-- and the index for selective ListRecords prefixes (issue #27). Older instances refuse this
-- schema, so every instance is upgraded together.

-- Registry answers, shared by every instance. `body` is what the client caches (Companies House
-- keeps only its minimised fields); NULL is the registry's "no such record". Rows past
-- `expires_at` are not read and are purged on a schedule.
CREATE TABLE registry_cache (
    registry   text NOT NULL CHECK (registry ~ '^[a-z0-9_]{1,32}$'),
    key        text NOT NULL CHECK (octet_length(key) BETWEEN 1 AND 2048),
    body       bytea,
    fetched_at timestamptz NOT NULL,
    expires_at timestamptz NOT NULL,
    PRIMARY KEY (registry, key)
);

CREATE INDEX registry_cache_by_expiry ON registry_cache (expires_at);

-- Favicon lookups (ADR 0002 section 5.5), so each icon is fetched once for every instance.
-- `icon` and `format` are both NULL for a miss.
CREATE TABLE favicons (
    domain     text PRIMARY KEY CHECK (octet_length(domain) BETWEEN 1 AND 253),
    icon       bytea,
    format     text CHECK (format IN ('ico', 'png', 'gif', 'jpeg', 'webp', 'avif')),
    fetched_at timestamptz NOT NULL,
    expires_at timestamptz NOT NULL,
    CHECK ((icon IS NULL) = (format IS NULL))
);

CREATE INDEX favicons_by_expiry ON favicons (expires_at);

-- Company freshness (ADR 0002 section 6): the last enrichment and the document count then,
-- and when each registry is due again. The lease stays in `company_leases`.
CREATE TABLE company_enrichment (
    company_id  uuid PRIMARY KEY REFERENCES nodes (id) ON DELETE CASCADE,
    enriched_at timestamptz NOT NULL,
    documents   bigint NOT NULL CHECK (documents >= 0)
);

CREATE TABLE company_registry_due (
    company_id  uuid NOT NULL REFERENCES company_enrichment (company_id) ON DELETE CASCADE,
    registry    text NOT NULL CHECK (registry ~ '^[a-z0-9_]{1,32}$'),
    next_due_at timestamptz NOT NULL,
    PRIMARY KEY (company_id, registry)
);

-- The "companies due" query walks the companies in id order.
CREATE INDEX nodes_companies ON nodes (id) WHERE kind = 'company';

-- Prefix filters on locations: with the default collation `starts_with(object_key, ...)` cannot
-- use the (endpoint, bucket, object_key) index; with "C" (byte order) it gets a range scan.
CREATE INDEX locations_by_key_c ON locations (endpoint, bucket, object_key COLLATE "C");
