-- doewood instance store, schema 12: known copies (issue #131). Additive: a source option and
-- a new table; no data changes.

-- Per source, off by default: a new key whose single-part, non-SSE-KMS/C ETag and size match
-- content already stored is recorded against that content without a GetObject. It trusts the
-- ETag as the content's MD5 (docs/sources.md, "Known copies").
ALTER TABLE sources ADD COLUMN skip_known_copies INTEGER NOT NULL DEFAULT 0
    CHECK (skip_known_copies IN (0, 1));

-- Content ingest has hashed, by the MD5 ETag (lowercase hex) and size its GetObject response
-- had. Written only for single-part objects without SSE-KMS or SSE-C, whatever the source's
-- option. A row whose record is gone or stale is not used: the copy is fetched as before.
CREATE TABLE known_content (
    etag       TEXT NOT NULL CHECK (length(etag) = 32),
    size       INTEGER NOT NULL CHECK (size >= 0),
    blake3     BLOB NOT NULL CHECK (length(blake3) = 32),
    updated_at INTEGER NOT NULL,
    PRIMARY KEY (etag, size)
) STRICT, WITHOUT ROWID;
