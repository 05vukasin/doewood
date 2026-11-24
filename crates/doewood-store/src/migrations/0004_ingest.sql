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

-- Aliases of nodes: the identifiers and name keys a company is matched by (ADR 0002,
-- section 1). One owner per alias, the first claim wins; a blocked alias has no owner.
CREATE TABLE node_aliases (
    kind       TEXT NOT NULL CHECK (kind IN ('document', 'location', 'company')),
    alias      TEXT NOT NULL,
    node_id    TEXT REFERENCES nodes (id) ON DELETE CASCADE,
    props_json TEXT NOT NULL DEFAULT '{}' CHECK (json_valid(props_json)),
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    PRIMARY KEY (kind, alias)
) STRICT, WITHOUT ROWID;

CREATE INDEX node_aliases_by_node ON node_aliases (node_id);

-- Ingest picks pending objects up in key order, per source.
CREATE INDEX objects_seen_pending ON objects_seen (source_id, status, key);
