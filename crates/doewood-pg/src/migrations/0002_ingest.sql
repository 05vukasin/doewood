-- doewood shared store, schema 2 (ingest): the MinHash LSH band index behind `similar_to`
-- edges, and the node aliases companies are matched by.

-- Each document's signature, and one row per band key, so a candidate lookup reads the bands
-- it shares and never every document. Band keys are the 64 bits of quail's u64 keys as bigint.
CREATE TABLE sketches (
    blake3     bytea PRIMARY KEY REFERENCES records (blake3) ON DELETE CASCADE,
    signature  bytea NOT NULL CHECK (octet_length(signature) > 0),
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE sketch_bands (
    band   bigint NOT NULL,
    blake3 bytea NOT NULL REFERENCES sketches (blake3) ON DELETE CASCADE,
    PRIMARY KEY (band, blake3)
);

CREATE INDEX sketch_bands_by_doc ON sketch_bands (blake3);

-- Aliases of nodes: the identifiers and name keys a company is matched by (ADR 0002,
-- section 1). One owner per alias, the first claim wins; a blocked alias has no owner.
CREATE TABLE node_aliases (
    kind       text NOT NULL CHECK (kind IN ('document', 'location', 'company')),
    alias      text NOT NULL,
    node_id    uuid REFERENCES nodes (id) ON DELETE CASCADE,
    props      jsonb NOT NULL DEFAULT '{}',
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (kind, alias)
);

CREATE INDEX node_aliases_by_node ON node_aliases (node_id);
