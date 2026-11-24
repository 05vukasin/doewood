-- doewood instance store, schema 4: ingest (step 4).

-- The MinHash LSH band index behind `similar_to` edges (standalone mode). Each document's
-- signature, and one row per band key: a candidate lookup reads the bands it shares, never
-- every document. Band keys are the 64 bits of quail's u64 keys as a signed integer.
CREATE TABLE sketches (
    blake3     BLOB PRIMARY KEY NOT NULL REFERENCES records (blake3) ON DELETE CASCADE,
    signature  BLOB NOT NULL CHECK (length(signature) > 0),
    updated_at INTEGER NOT NULL
) STRICT, WITHOUT ROWID;

CREATE TABLE sketch_bands (
    band   INTEGER NOT NULL,
    blake3 BLOB NOT NULL REFERENCES sketches (blake3) ON DELETE CASCADE,
    PRIMARY KEY (band, blake3)
) STRICT, WITHOUT ROWID;

CREATE INDEX sketch_bands_by_doc ON sketch_bands (blake3);

-- Ingest picks pending objects up in key order, per source.
CREATE INDEX objects_seen_pending ON objects_seen (source_id, status, key);
