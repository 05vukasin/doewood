-- doewood instance store, schema 10: cheap progress counts (issues #72 and #73). New tables,
-- triggers and an index; one duplicate index dropped. No data changes.

-- How many objects of a source are in each ingest status, kept by the triggers below in the
-- same transaction as every change to `objects_seen`. `WatchProgress` reads these instead of
-- counting a source's objects on every update.
CREATE TABLE object_counts (
    source_id TEXT NOT NULL REFERENCES sources (id) ON DELETE CASCADE,
    status    TEXT NOT NULL,
    n         INTEGER NOT NULL,
    PRIMARY KEY (source_id, status)
) STRICT, WITHOUT ROWID;

INSERT INTO object_counts (source_id, status, n)
    SELECT source_id, status, count(*) FROM objects_seen GROUP BY source_id, status;

CREATE TRIGGER objects_seen_count_insert AFTER INSERT ON objects_seen
BEGIN
    INSERT INTO object_counts (source_id, status, n) VALUES (new.source_id, new.status, 1)
        ON CONFLICT (source_id, status) DO UPDATE SET n = n + 1;
END;

CREATE TRIGGER objects_seen_count_delete AFTER DELETE ON objects_seen
BEGIN
    UPDATE object_counts SET n = n - 1 WHERE source_id = old.source_id AND status = old.status;
END;

CREATE TRIGGER objects_seen_count_update AFTER UPDATE OF source_id, status ON objects_seen
    WHEN old.source_id IS NOT new.source_id OR old.status IS NOT new.status
BEGIN
    UPDATE object_counts SET n = n - 1 WHERE source_id = old.source_id AND status = old.status;
    INSERT INTO object_counts (source_id, status, n) VALUES (new.source_id, new.status, 1)
        ON CONFLICT (source_id, status) DO UPDATE SET n = n + 1;
END;

-- The same for the LLM queue.
CREATE TABLE llm_job_counts (
    source_id TEXT NOT NULL REFERENCES sources (id) ON DELETE CASCADE,
    status    TEXT NOT NULL,
    n         INTEGER NOT NULL,
    PRIMARY KEY (source_id, status)
) STRICT, WITHOUT ROWID;

INSERT INTO llm_job_counts (source_id, status, n)
    SELECT source_id, status, count(*) FROM llm_jobs GROUP BY source_id, status;

CREATE TRIGGER llm_jobs_count_insert AFTER INSERT ON llm_jobs
BEGIN
    INSERT INTO llm_job_counts (source_id, status, n) VALUES (new.source_id, new.status, 1)
        ON CONFLICT (source_id, status) DO UPDATE SET n = n + 1;
END;

CREATE TRIGGER llm_jobs_count_delete AFTER DELETE ON llm_jobs
BEGIN
    UPDATE llm_job_counts SET n = n - 1 WHERE source_id = old.source_id AND status = old.status;
END;

CREATE TRIGGER llm_jobs_count_update AFTER UPDATE OF source_id, status ON llm_jobs
    WHEN old.source_id IS NOT new.source_id OR old.status IS NOT new.status
BEGIN
    UPDATE llm_job_counts SET n = n - 1 WHERE source_id = old.source_id AND status = old.status;
    INSERT INTO llm_job_counts (source_id, status, n) VALUES (new.source_id, new.status, 1)
        ON CONFLICT (source_id, status) DO UPDATE SET n = n + 1;
END;

-- A source's newest failed summary (`llm_counts`) without sorting its failed jobs.
CREATE INDEX llm_jobs_by_source ON llm_jobs (source_id, status, updated_at);

-- `objects_seen` is WITHOUT ROWID with primary key (source_id, key), so this index held
-- exactly what `objects_seen_pending` (source_id, status, key) holds.
DROP INDEX objects_seen_by_status;
