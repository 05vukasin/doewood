-- doewood shared store, schema 1 (ADR 0001 sections 2 and 7, ADR 0002 sections 3 and 6).
-- Needs PostgreSQL 15 or newer (UNIQUE NULLS NOT DISTINCT). The pgvector column is added
-- outside the migrations, only when the extension is available.

-- quail and hedgehog records, keyed by the file's BLAKE3.
CREATE TABLE records (
    blake3     bytea PRIMARY KEY CHECK (octet_length(blake3) = 32),
    record     jsonb NOT NULL,
    versions   jsonb NOT NULL DEFAULT '{}',
    updated_at timestamptz NOT NULL DEFAULT now()
);

-- Graph nodes; the key is unique per kind.
CREATE TABLE nodes (
    id         uuid PRIMARY KEY,
    kind       text NOT NULL CHECK (kind IN ('document', 'location', 'company')),
    key        text NOT NULL,
    props      jsonb NOT NULL DEFAULT '{}',
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT nodes_kind_key UNIQUE (kind, key)
);

-- Graph edges. The identity is (src, dst, kind, source, valid_from), with one undated edge
-- per identity; the identity index serves outgoing lookups, edges_by_dst incoming ones.
CREATE TABLE edges (
    id         uuid PRIMARY KEY,
    src        uuid NOT NULL REFERENCES nodes (id) ON DELETE CASCADE,
    dst        uuid NOT NULL REFERENCES nodes (id) ON DELETE CASCADE,
    kind       text NOT NULL CHECK (kind IN ('located_at', 'party', 'amends', 'cites',
                   'similar_to', 'parent_of', 'subsidiary_of', 'acquired', 'merged_into',
                   'renamed_to', 'branch_of', 'trading_name_of', 'former_parent_of',
                   'liquidated')),
    props      jsonb NOT NULL DEFAULT '{}',
    evidence   jsonb NOT NULL DEFAULT '[]',
    confidence real NOT NULL CHECK (confidence BETWEEN 0 AND 1),
    state      text NOT NULL
               CHECK (state IN ('proposed', 'active', 'contested', 'retracted', 'superseded')),
    valid_from date,
    valid_to   date,
    source     text NOT NULL CHECK (source IN ('registry', 'document', 'web', 'manual', 'storage')),
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CHECK (valid_to IS NULL OR valid_from IS NULL OR valid_to >= valid_from),
    CONSTRAINT edges_identity UNIQUE NULLS NOT DISTINCT (src, dst, kind, source, valid_from)
);

CREATE INDEX edges_by_src ON edges (src, kind);
CREATE INDEX edges_by_dst ON edges (dst, kind);

-- Where documents live: one row per Location node, pointing at the document stored there now.
CREATE TABLE locations (
    node_id     uuid PRIMARY KEY REFERENCES nodes (id) ON DELETE CASCADE,
    document_id uuid NOT NULL REFERENCES nodes (id) ON DELETE CASCADE,
    blake3      bytea NOT NULL CHECK (octet_length(blake3) = 32),
    endpoint    text NOT NULL,
    bucket      text NOT NULL,
    object_key  text NOT NULL,
    updated_at  timestamptz NOT NULL DEFAULT now(),
    UNIQUE (endpoint, bucket, object_key)
);

CREATE INDEX locations_by_blake3 ON locations (blake3);

-- Source claims: one owner instance per (endpoint, bucket, prefix).
CREATE TABLE source_claims (
    endpoint   text NOT NULL,
    bucket     text NOT NULL,
    prefix     text NOT NULL,
    owner      text NOT NULL,
    claimed_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT source_claims_key UNIQUE (endpoint, bucket, prefix)
);

-- Enrichment leases on companies (ADR 0002, section 6): the first instance to see a company
-- enriches it, the others reuse the result.
CREATE TABLE company_leases (
    company_id uuid PRIMARY KEY REFERENCES nodes (id) ON DELETE CASCADE,
    owner      text NOT NULL,
    leased_at  timestamptz NOT NULL,
    expires_at timestamptz NOT NULL
);

-- Search: the filter fields and a weighted full-text vector (title above body). The
-- `simple` configuration does not stem, so every language is treated alike.
CREATE TABLE search_index (
    blake3     bytea PRIMARY KEY REFERENCES records (blake3) ON DELETE CASCADE,
    title      text NOT NULL,
    body       text NOT NULL,
    kind       text,
    doc_date   date,
    amount     double precision,
    indexed_at timestamptz NOT NULL DEFAULT now(),
    tsv        tsvector GENERATED ALWAYS AS (
                   setweight(to_tsvector('simple', title), 'A')
                   || setweight(to_tsvector('simple', body), 'B')
               ) STORED
);

CREATE INDEX search_tsv ON search_index USING gin (tsv);
CREATE INDEX search_by_date ON search_index (doc_date);
CREATE INDEX search_by_kind ON search_index (kind, doc_date);
