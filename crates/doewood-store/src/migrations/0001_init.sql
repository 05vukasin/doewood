-- doewood instance store, schema 1.
-- Times are Unix seconds (UTC). Ids are UUIDv7 text.

-- Runtime settings set through the API (never secrets).
CREATE TABLE settings (
    key        TEXT PRIMARY KEY NOT NULL,
    value_json TEXT NOT NULL CHECK (json_valid(value_json)),
    updated_at INTEGER NOT NULL
) STRICT;

-- Secrets, sealed with XChaCha20-Poly1305 under the master key. The name is the associated
-- data, so a ciphertext moved to another row does not open.
CREATE TABLE secrets (
    name       TEXT PRIMARY KEY NOT NULL,
    nonce      BLOB NOT NULL CHECK (length(nonce) = 24),
    ciphertext BLOB NOT NULL,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
) STRICT;

-- S3-compatible sources this instance watches.
CREATE TABLE sources (
    id                 TEXT PRIMARY KEY NOT NULL,
    endpoint           TEXT NOT NULL,
    bucket             TEXT NOT NULL,
    prefix             TEXT NOT NULL DEFAULT '',
    region             TEXT,
    credentials_secret TEXT,
    policy_json        TEXT NOT NULL DEFAULT '{}' CHECK (json_valid(policy_json)),
    state              TEXT NOT NULL DEFAULT 'paused'
                       CHECK (state IN ('paused', 'running', 'error', 'removed')),
    scan_interval_s    INTEGER NOT NULL DEFAULT 300 CHECK (scan_interval_s > 0),
    created_at         INTEGER NOT NULL,
    UNIQUE (endpoint, bucket, prefix)
) STRICT;

-- Jobs: a source's first pass and watch, or a one-shot reprocess.
CREATE TABLE jobs (
    id            TEXT PRIMARY KEY NOT NULL,
    source_id     TEXT REFERENCES sources (id) ON DELETE CASCADE,
    kind          TEXT NOT NULL CHECK (kind IN ('scan', 'reprocess')),
    state         TEXT NOT NULL DEFAULT 'queued'
                  CHECK (state IN ('queued', 'running', 'paused', 'done', 'failed', 'cancelled')),
    progress_json TEXT NOT NULL DEFAULT '{}' CHECK (json_valid(progress_json)),
    last_error    TEXT,
    created_at    INTEGER NOT NULL,
    started_at    INTEGER,
    finished_at   INTEGER,
    updated_at    INTEGER NOT NULL
) STRICT;

CREATE INDEX jobs_by_source ON jobs (source_id, state);

-- What discovery has seen per source, and how far ingest got with it.
CREATE TABLE objects_seen (
    source_id     TEXT NOT NULL REFERENCES sources (id) ON DELETE CASCADE,
    key           TEXT NOT NULL,
    etag          TEXT,
    size          INTEGER NOT NULL CHECK (size >= 0),
    last_modified INTEGER,
    blake3        BLOB CHECK (blake3 IS NULL OR length(blake3) = 32),
    status        TEXT NOT NULL DEFAULT 'pending'
                  CHECK (status IN ('pending', 'done', 'failed', 'skipped', 'deleted')),
    attempts      INTEGER NOT NULL DEFAULT 0,
    last_error    TEXT,
    updated_at    INTEGER NOT NULL,
    PRIMARY KEY (source_id, key)
) STRICT, WITHOUT ROWID;

CREATE INDEX objects_seen_by_status ON objects_seen (source_id, status);
