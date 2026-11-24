-- doewood instance store, schema 2: source states and S3 options, scan checkpoints.

-- SQLite cannot change a CHECK constraint, so `sources` is rebuilt (the documented
-- create-copy-drop-rename procedure; migrations run with foreign keys off, and the migration
-- runner checks them before it commits). Changes:
-- - the states are `active`, `paused`, `error` (with `state_reason`) and `removed`; an old
--   `running` row becomes `active`;
-- - `url_style` (path or virtual host) and `max_object_bytes` (GetObject size cap; NULL is the
--   default, 512 MiB) are per source.
CREATE TABLE sources_v2 (
    id                 TEXT PRIMARY KEY NOT NULL,
    endpoint           TEXT NOT NULL,
    bucket             TEXT NOT NULL,
    prefix             TEXT NOT NULL DEFAULT '',
    region             TEXT,
    url_style          TEXT NOT NULL DEFAULT 'path' CHECK (url_style IN ('path', 'virtual_host')),
    credentials_secret TEXT,
    max_object_bytes   INTEGER CHECK (max_object_bytes IS NULL OR max_object_bytes > 0),
    policy_json        TEXT NOT NULL DEFAULT '{}' CHECK (json_valid(policy_json)),
    state              TEXT NOT NULL DEFAULT 'paused'
                       CHECK (state IN ('active', 'paused', 'error', 'removed')),
    state_reason       TEXT,
    scan_interval_s    INTEGER NOT NULL DEFAULT 300 CHECK (scan_interval_s > 0),
    created_at         INTEGER NOT NULL,
    updated_at         INTEGER NOT NULL DEFAULT 0,
    UNIQUE (endpoint, bucket, prefix)
) STRICT;

INSERT INTO sources_v2 (id, endpoint, bucket, prefix, region, credentials_secret, policy_json,
                        state, scan_interval_s, created_at, updated_at)
SELECT id, endpoint, bucket, prefix, region, credentials_secret, policy_json,
       CASE state WHEN 'running' THEN 'active' ELSE state END, scan_interval_s, created_at,
       created_at
FROM sources;

DROP TABLE sources;
ALTER TABLE sources_v2 RENAME TO sources;

-- Where each source's scan is. A pass lists the bucket in key order; `start_after` is the last
-- key whose comparison is committed, so a restart resumes the pass with `StartAfter` instead of
-- starting over. NULL with `in_progress = 1` means the pass started and nothing is committed.
CREATE TABLE scan_checkpoints (
    source_id         TEXT PRIMARY KEY NOT NULL REFERENCES sources (id) ON DELETE CASCADE,
    pass              INTEGER NOT NULL DEFAULT 0 CHECK (pass >= 0),
    in_progress       INTEGER NOT NULL DEFAULT 0 CHECK (in_progress IN (0, 1)),
    start_after       TEXT,
    pass_started_at   INTEGER,
    last_completed_at INTEGER,
    updated_at        INTEGER NOT NULL
) STRICT;
