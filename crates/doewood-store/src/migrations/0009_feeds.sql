-- doewood instance store, schema 9: registry delta feeds (ADR 0002 section 6, issue #49), in
-- standalone mode. A new table only. Times are Unix seconds.

-- One row per feed (`gleif`, `edgar`, `apr`, `companies_house`): its lease, and its resumable
-- cursor and next run. `owner` is NULL when no one holds it (standalone: only this instance).
CREATE TABLE registry_feeds (
    feed             TEXT PRIMARY KEY CHECK (length(feed) BETWEEN 1 AND 32
                     AND feed NOT GLOB '*[^a-z0-9_]*'),
    owner            TEXT,
    lease_expires_at INTEGER,
    cursor           TEXT NOT NULL DEFAULT '' CHECK (length(CAST(cursor AS BLOB)) <= 1024),
    next_run_at      INTEGER NOT NULL DEFAULT 0,
    updated_at       INTEGER,
    CHECK ((owner IS NULL) = (lease_expires_at IS NULL))
) STRICT, WITHOUT ROWID;
