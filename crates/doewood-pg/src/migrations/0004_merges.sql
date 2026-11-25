-- doewood shared store, schema 4: reversible company merges (ADR 0002 section 1.8, ADR 0001
-- section 7). New tables only. Older instances refuse this schema, so every instance is
-- upgraded together.

-- The merge log: one row per merge or split, written in the same transaction as the change.
-- `undo` lists exactly what moved (aliases, edges, props), so an undo restores it. No foreign
-- keys to `nodes`: the log outlives a split's company that an undo deletes.
CREATE TABLE company_merges (
    id              uuid PRIMARY KEY,
    seq             bigint GENERATED ALWAYS AS IDENTITY UNIQUE,
    op              text NOT NULL CHECK (op IN ('merge', 'split')),
    at              timestamptz NOT NULL,
    actor           text NOT NULL CHECK (actor IN ('ingest', 'enrichment', 'manual')),
    instance        text NOT NULL,
    survivor_id     uuid NOT NULL,
    absorbed_id     uuid NOT NULL,
    evidence        jsonb NOT NULL DEFAULT '[]',
    confidence      real NOT NULL CHECK (confidence BETWEEN 0 AND 1),
    probability     real NOT NULL CHECK (probability BETWEEN 0 AND 1),
    undo            jsonb NOT NULL,
    undone_at       timestamptz,
    undone_actor    text CHECK (undone_actor IN ('ingest', 'enrichment', 'manual')),
    undone_instance text,
    CHECK (survivor_id <> absorbed_id),
    CHECK ((undone_at IS NULL) = (undone_actor IS NULL)
           AND (undone_at IS NULL) = (undone_instance IS NULL))
);

CREATE INDEX company_merges_by_survivor ON company_merges (survivor_id, seq);
CREATE INDEX company_merges_by_absorbed ON company_merges (absorbed_id, seq);

-- Permanent "not the same" pairs, both directions: the smaller id first. Never deleted.
CREATE TABLE company_dismissals (
    company_a uuid NOT NULL,
    company_b uuid NOT NULL,
    seq       bigint GENERATED ALWAYS AS IDENTITY UNIQUE,
    at        timestamptz NOT NULL,
    actor     text NOT NULL CHECK (actor IN ('ingest', 'enrichment', 'manual')),
    instance  text NOT NULL,
    reason    text NOT NULL DEFAULT '',
    merge_id  uuid,
    PRIMARY KEY (company_a, company_b),
    CHECK (company_a < company_b)
);

CREATE INDEX company_dismissals_by_b ON company_dismissals (company_b);
