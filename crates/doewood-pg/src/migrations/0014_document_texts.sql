-- doewood shared store, schema 14: documents' text with block locations (issue #166).
-- Additive: one new empty table; no existing rows change. Older instances refuse this schema,
-- so every instance is upgraded together.
--
-- One row per record that has text: the pages and blocks quail extracted, or the rows of a
-- spreadsheet, as canonical JSON text (`doewood_model::DocumentText`), stored and sent back as
-- written, like `records.body` (ADR 0003). Ingest bounds it (2 MiB by default) and writes it
-- with the record. Read by `blake3` only; nothing filters on its content.
CREATE TABLE document_texts (
    blake3     bytea PRIMARY KEY REFERENCES records (blake3) ON DELETE CASCADE
        CHECK (octet_length(blake3) = 32),
    body       text NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT now()
);

-- lz4, as `records.body` (ADR 0003, decision 1); a server built without lz4 keeps pglz.
DO $$ BEGIN
    ALTER TABLE document_texts ALTER COLUMN body SET COMPRESSION lz4;
EXCEPTION WHEN feature_not_supported OR invalid_parameter_value THEN
    RAISE NOTICE 'lz4 is not available; document_texts.body uses pglz';
END $$;
