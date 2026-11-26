-- doewood shared store, schema 8: records are stored as their canonical JSON text (issue #121).
-- Older instances refuse this schema, so every instance is upgraded together.

-- Since #106 a record arrives as canonical JSON text, and the API returns that text. Kept as
-- jsonb, every read turned it back into text on the server (92-95% of the database's time for
-- ListRecords pages under load) and into a tree and text again in doewood. `body` keeps the
-- text as it is; reads send it unchanged. The one query that looks inside a record
-- (`fact_texts`) parses `body` as jsonb, as SQLite parses its `body_json`.
ALTER TABLE records ADD COLUMN body text;

-- lz4 instead of the default pglz for the text: measured on ~17 KB records it stores 9% more
-- than pglz (still less than the jsonb did), compresses 5 times faster and makes a page of
-- 100 records a third faster to read (docs/deploy.md, "Records"). A server built without lz4
-- keeps pglz.
DO $$ BEGIN
    ALTER TABLE records ALTER COLUMN body SET COMPRESSION lz4;
EXCEPTION WHEN feature_not_supported OR invalid_parameter_value THEN
    RAISE NOTICE 'lz4 is not available; records.body uses pglz';
END $$;

-- New rows have `body` and no `record`. Rows written before keep `record` until doewood
-- converts them in the background (batches, resumable; docs/deploy.md); reads take either.
ALTER TABLE records ALTER COLUMN record DROP NOT NULL;

-- The rows still to convert, so the conversion finds them without scanning the table. It is
-- empty once they are converted, and new rows never enter it.
CREATE INDEX records_body_pending ON records (blake3) WHERE body IS NULL;
