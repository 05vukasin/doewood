-- doewood instance store, schema 5: the LLM queue (ADR 0001, section 6).

-- One row per document a source's policy queued for a summary. Ingest writes it `pending`
-- after the record is stored; the queue ends it `done` (the summary is in the record),
-- `failed` (the provider failed; the record stays without a summary) or `skipped` (no text to
-- summarize, or the policy no longer asks for it).
CREATE TABLE llm_jobs (
    source_id  TEXT NOT NULL REFERENCES sources (id) ON DELETE CASCADE,
    blake3     BLOB NOT NULL CHECK (length(blake3) = 32),
    kind       TEXT,
    status     TEXT NOT NULL DEFAULT 'pending'
               CHECK (status IN ('pending', 'done', 'failed', 'skipped')),
    attempts   INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    last_error TEXT,
    queued_at  INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    PRIMARY KEY (source_id, blake3)
) STRICT, WITHOUT ROWID;

CREATE INDEX llm_jobs_by_status ON llm_jobs (status, queued_at);

-- What each source spent on summaries per UTC day (days since the Unix epoch): the daily
-- budget is checked against `tokens`.
CREATE TABLE llm_usage (
    source_id TEXT NOT NULL REFERENCES sources (id) ON DELETE CASCADE,
    day       INTEGER NOT NULL,
    tokens    INTEGER NOT NULL DEFAULT 0 CHECK (tokens >= 0),
    calls     INTEGER NOT NULL DEFAULT 0 CHECK (calls >= 0),
    PRIMARY KEY (source_id, day)
) STRICT, WITHOUT ROWID;
