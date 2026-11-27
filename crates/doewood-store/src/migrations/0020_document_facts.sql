-- doewood instance store, schema 20: key facts for AI retrieval (issue #194).
--
-- 1. A new empty table, `document_facts`: one row per record with key facts (quail's
--    `card.facts` and the `<namespace>.highlight` facts): the card (canonical JSON text of
--    `doewood_model::Card`), every highlight (`doewood_model::BlockHighlight`), and the card's
--    searchable fields for `FindDocuments` (dates `YYYY-MM-DD`, amounts in units of 10^-4,
--    booleans 0/1, `_fold` columns as `doewood_model::fold` writes them). Ingest rebuilds the
--    row from the record when it writes the document's graph. The rows go with their record.
-- 2. Search weights the key facts: `search_index.facts` (the card's text and the passages its
--    fields were read from) is a third FTS5 column, ranked above the title and the body
--    (`bm25` weights 10, 1 and 20). An FTS5 table cannot gain a column, so `search_fts` is
--    created again with it and rebuilt from `search_index` once; its triggers follow.
CREATE TABLE document_facts (
    blake3        BLOB PRIMARY KEY NOT NULL REFERENCES records (blake3) ON DELETE CASCADE
        CHECK (length(blake3) = 32),
    kind          TEXT,
    number        TEXT,
    subject       TEXT,
    supplier_name TEXT,
    supplier_fold TEXT,
    customer_name TEXT,
    customer_fold TEXT,
    category      TEXT,
    amount        INTEGER,
    currency      TEXT,
    period        TEXT,
    signed        TEXT CHECK (signed IS NULL OR length(signed) = 10),
    ends          TEXT CHECK (ends IS NULL OR length(ends) = 10),
    notice_due    TEXT CHECK (notice_due IS NULL OR length(notice_due) = 10),
    auto_renewal  INTEGER CHECK (auto_renewal IS NULL OR auto_renewal IN (0, 1)),
    has_penalty   INTEGER NOT NULL CHECK (has_penalty IN (0, 1)),
    has_guarantee INTEGER NOT NULL CHECK (has_guarantee IN (0, 1)),
    governing_law TEXT,
    court         TEXT,
    court_fold    TEXT,
    card          TEXT NOT NULL,
    highlights    TEXT NOT NULL,
    updated_at    INTEGER NOT NULL
) STRICT, WITHOUT ROWID;

CREATE INDEX document_facts_by_kind ON document_facts (kind, blake3);
CREATE INDEX document_facts_by_ends ON document_facts (ends) WHERE ends IS NOT NULL;
CREATE INDEX document_facts_by_notice ON document_facts (notice_due) WHERE notice_due IS NOT NULL;

ALTER TABLE search_index ADD COLUMN facts TEXT NOT NULL DEFAULT '';

DROP TRIGGER search_index_ai;
DROP TRIGGER search_index_ad;
DROP TRIGGER search_index_au;
DROP TABLE search_fts;

CREATE VIRTUAL TABLE search_fts USING fts5 (
    title, body, facts,
    content = 'search_index',
    tokenize = 'unicode61 remove_diacritics 2'
);
INSERT INTO search_fts (search_fts) VALUES ('rebuild');

CREATE TRIGGER search_index_ai AFTER INSERT ON search_index BEGIN
    INSERT INTO search_fts (rowid, title, body, facts)
        VALUES (new.rowid, new.title, new.body, new.facts);
END;

CREATE TRIGGER search_index_ad AFTER DELETE ON search_index BEGIN
    INSERT INTO search_fts (search_fts, rowid, title, body, facts)
        VALUES ('delete', old.rowid, old.title, old.body, old.facts);
END;

CREATE TRIGGER search_index_au AFTER UPDATE ON search_index BEGIN
    INSERT INTO search_fts (search_fts, rowid, title, body, facts)
        VALUES ('delete', old.rowid, old.title, old.body, old.facts);
    INSERT INTO search_fts (rowid, title, body, facts)
        VALUES (new.rowid, new.title, new.body, new.facts);
END;
