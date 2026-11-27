-- doewood instance store, schema 18: the exact-match keys of typed similarity (issue #169).
-- One new empty table; no existing rows change.
--
-- A document's keys: the first 64 bits of its folded-text key, its template property, its
-- heading skeleton and its document identity ids (`kind`, doewood-model `SimilarKeyKind`),
-- each with the document's masked SimHash (`sketch.simhash_template`), so a template match
-- is checked without reading a record. A lookup reads one key's documents in hash order
-- from the primary key. The rows go with their record. Documents ingested before this
-- schema get keys when they are ingested again (`ReprocessJob` with `force`); their
-- `similar_to` edges keep working as they are.
CREATE TABLE similar_keys (
    kind   INTEGER NOT NULL,
    key    INTEGER NOT NULL,
    blake3 BLOB NOT NULL REFERENCES records (blake3) ON DELETE CASCADE,
    masked INTEGER,
    PRIMARY KEY (kind, key, blake3)
) STRICT, WITHOUT ROWID;

CREATE INDEX similar_keys_by_doc ON similar_keys (blake3);
