-- doewood instance store, schema 7: reversible company merges in standalone mode (ADR 0002
-- section 1.8), the same as shared schema 4. Times are Unix seconds (UTC).

-- The merge log; `undo` lists exactly what moved. No foreign keys to `nodes`: the log
-- outlives a split's company that an undo deletes.
CREATE TABLE company_merges (
    seq             INTEGER PRIMARY KEY AUTOINCREMENT,
    id              TEXT NOT NULL UNIQUE,
    op              TEXT NOT NULL CHECK (op IN ('merge', 'split')),
    at              INTEGER NOT NULL,
    actor           TEXT NOT NULL CHECK (actor IN ('ingest', 'enrichment', 'manual')),
    instance        TEXT NOT NULL,
    survivor_id     TEXT NOT NULL,
    absorbed_id     TEXT NOT NULL,
    evidence_json   TEXT NOT NULL DEFAULT '[]' CHECK (json_valid(evidence_json)),
    confidence      REAL NOT NULL CHECK (confidence BETWEEN 0 AND 1),
    probability     REAL NOT NULL CHECK (probability BETWEEN 0 AND 1),
    undo_json       TEXT NOT NULL CHECK (json_valid(undo_json)),
    undone_at       INTEGER,
    undone_actor    TEXT CHECK (undone_actor IN ('ingest', 'enrichment', 'manual')),
    undone_instance TEXT,
    CHECK (survivor_id <> absorbed_id),
    CHECK ((undone_at IS NULL) = (undone_actor IS NULL)
           AND (undone_at IS NULL) = (undone_instance IS NULL))
) STRICT;

CREATE INDEX company_merges_by_survivor ON company_merges (survivor_id, seq);
CREATE INDEX company_merges_by_absorbed ON company_merges (absorbed_id, seq);

-- Permanent "not the same" pairs, both directions: the smaller id first. Never deleted.
CREATE TABLE company_dismissals (
    seq       INTEGER PRIMARY KEY AUTOINCREMENT,
    company_a TEXT NOT NULL,
    company_b TEXT NOT NULL,
    at        INTEGER NOT NULL,
    actor     TEXT NOT NULL CHECK (actor IN ('ingest', 'enrichment', 'manual')),
    instance  TEXT NOT NULL,
    reason    TEXT NOT NULL DEFAULT '',
    merge_id  TEXT,
    UNIQUE (company_a, company_b),
    CHECK (company_a < company_b)
) STRICT;

CREATE INDEX company_dismissals_by_b ON company_dismissals (company_b);
