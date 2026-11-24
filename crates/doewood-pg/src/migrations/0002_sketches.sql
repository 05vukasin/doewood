-- doewood shared store, schema 2: the MinHash LSH band index behind `similar_to` edges.
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
