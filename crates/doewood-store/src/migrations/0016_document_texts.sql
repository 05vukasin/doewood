-- doewood instance store, schema 16: documents' text with block locations (issue #166).
-- Additive: a new empty table; no data changes.

-- One row per record that has text: the pages and blocks quail extracted, or the rows of a
-- spreadsheet, as canonical JSON text (`doewood_model::DocumentText`, ADR 0003), stored as
-- written like `records.body_json`. Ingest bounds it (2 MiB by default) and writes it with the
-- record. A rowid table, unlike `records`: its rows run to megabytes, and SQLite keeps large
-- rows out of the primary key's b-tree only in a rowid table.
CREATE TABLE document_texts (
    blake3     BLOB NOT NULL UNIQUE REFERENCES records (blake3) ON DELETE CASCADE
        CHECK (length(blake3) = 32),
    body       TEXT NOT NULL,
    updated_at INTEGER NOT NULL
) STRICT;
