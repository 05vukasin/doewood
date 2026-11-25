-- doewood shared store, schema 6: registry delta feeds (ADR 0002 section 6, issue #49).
-- A new table only. Older instances refuse this schema, so every instance is upgraded
-- together.

-- One row per feed (`gleif`, `edgar`, `apr`, `companies_house`): the lease that keeps one
-- instance per cluster pulling it, and its resumable cursor and next run, shared by every
-- instance. `owner` is NULL when no instance holds it.
CREATE TABLE registry_feeds (
    feed             text PRIMARY KEY CHECK (feed ~ '^[a-z0-9_]{1,32}$'),
    owner            text,
    lease_expires_at timestamptz,
    cursor           text NOT NULL DEFAULT '' CHECK (octet_length(cursor) <= 1024),
    next_run_at      timestamptz NOT NULL DEFAULT 'epoch',
    updated_at       timestamptz,
    CHECK ((owner IS NULL) = (lease_expires_at IS NULL))
);

-- The feeds find the companies they name by alias prefix (`id:rs_mb:`) in byte order.
CREATE INDEX node_aliases_company_c ON node_aliases (alias COLLATE "C")
    WHERE kind = 'company' AND node_id IS NOT NULL;
