-- doewood instance store, schema 2: standalone mode (ADR 0001, section 2).
-- The records, graph, claims, leases and search that live in the shared Postgres database
-- when one is configured. Times are Unix seconds (UTC); dates are Julian day numbers.

-- quail and hedgehog records, keyed by the file's BLAKE3.
CREATE TABLE records (
    blake3        BLOB PRIMARY KEY NOT NULL CHECK (length(blake3) = 32),
    body_json     TEXT NOT NULL CHECK (json_valid(body_json)),
    versions_json TEXT NOT NULL DEFAULT '{}' CHECK (json_valid(versions_json)),
    updated_at    INTEGER NOT NULL
) STRICT, WITHOUT ROWID;

-- Graph nodes; the key is unique per kind.
CREATE TABLE nodes (
    id         TEXT PRIMARY KEY NOT NULL,
    kind       TEXT NOT NULL CHECK (kind IN ('document', 'location', 'company')),
    key        TEXT NOT NULL,
    props_json TEXT NOT NULL DEFAULT '{}' CHECK (json_valid(props_json)),
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    UNIQUE (kind, key)
) STRICT;

-- Graph edges. The identity is (src, dst, kind, source, valid_from); an undated edge is one
-- identity, hence the expression index below (SQLite treats NULLs as distinct).
CREATE TABLE edges (
    id            TEXT PRIMARY KEY NOT NULL,
    src           TEXT NOT NULL REFERENCES nodes (id) ON DELETE CASCADE,
    dst           TEXT NOT NULL REFERENCES nodes (id) ON DELETE CASCADE,
    kind          TEXT NOT NULL CHECK (kind IN ('located_at', 'party', 'amends', 'cites',
                      'similar_to', 'parent_of', 'subsidiary_of', 'acquired', 'merged_into',
                      'renamed_to', 'branch_of', 'trading_name_of', 'former_parent_of',
                      'liquidated')),
    props_json    TEXT NOT NULL DEFAULT '{}' CHECK (json_valid(props_json)),
    evidence_json TEXT NOT NULL DEFAULT '[]' CHECK (json_valid(evidence_json)),
    confidence    REAL NOT NULL CHECK (confidence BETWEEN 0 AND 1),
    state         TEXT NOT NULL
                  CHECK (state IN ('proposed', 'active', 'contested', 'retracted', 'superseded')),
    valid_from    INTEGER,
    valid_to      INTEGER,
    source        TEXT NOT NULL CHECK (source IN ('registry', 'document', 'web', 'manual', 'storage')),
    created_at    INTEGER NOT NULL,
    updated_at    INTEGER NOT NULL,
    CHECK (valid_to IS NULL OR valid_from IS NULL OR valid_to >= valid_from)
) STRICT;

CREATE UNIQUE INDEX edges_identity
    ON edges (src, dst, kind, source, ifnull(valid_from, -2147483648));
CREATE INDEX edges_by_src ON edges (src, kind);
CREATE INDEX edges_by_dst ON edges (dst, kind);

-- Where documents live: one row per Location node, pointing at the document stored there now.
CREATE TABLE locations (
    node_id     TEXT PRIMARY KEY NOT NULL REFERENCES nodes (id) ON DELETE CASCADE,
    document_id TEXT NOT NULL REFERENCES nodes (id) ON DELETE CASCADE,
    blake3      BLOB NOT NULL CHECK (length(blake3) = 32),
    endpoint    TEXT NOT NULL,
    bucket      TEXT NOT NULL,
    object_key  TEXT NOT NULL,
    updated_at  INTEGER NOT NULL,
    UNIQUE (endpoint, bucket, object_key)
) STRICT;

CREATE INDEX locations_by_blake3 ON locations (blake3);

-- Source claims. Standalone, they only guard against adding a source twice locally.
CREATE TABLE source_claims (
    endpoint   TEXT NOT NULL,
    bucket     TEXT NOT NULL,
    prefix     TEXT NOT NULL,
    owner      TEXT NOT NULL,
    claimed_at INTEGER NOT NULL,
    PRIMARY KEY (endpoint, bucket, prefix)
) STRICT, WITHOUT ROWID;

-- Enrichment leases on companies (ADR 0002, section 6).
CREATE TABLE company_leases (
    company_id TEXT PRIMARY KEY NOT NULL REFERENCES nodes (id) ON DELETE CASCADE,
    owner      TEXT NOT NULL,
    leased_at  INTEGER NOT NULL,
    expires_at INTEGER NOT NULL
) STRICT;

-- Search: the filter fields, plus an FTS5 index over title and body kept in step by triggers.
CREATE TABLE search_index (
    blake3     BLOB PRIMARY KEY NOT NULL REFERENCES records (blake3) ON DELETE CASCADE,
    title      TEXT NOT NULL,
    body       TEXT NOT NULL,
    kind       TEXT,
    doc_date   INTEGER,
    amount     REAL,
    indexed_at INTEGER NOT NULL
) STRICT;

CREATE INDEX search_by_date ON search_index (doc_date);
CREATE INDEX search_by_kind ON search_index (kind, doc_date);

CREATE VIRTUAL TABLE search_fts USING fts5 (
    title, body,
    content = 'search_index',
    tokenize = 'unicode61 remove_diacritics 2'
);

CREATE TRIGGER search_index_ai AFTER INSERT ON search_index BEGIN
    INSERT INTO search_fts (rowid, title, body) VALUES (new.rowid, new.title, new.body);
END;

CREATE TRIGGER search_index_ad AFTER DELETE ON search_index BEGIN
    INSERT INTO search_fts (search_fts, rowid, title, body)
        VALUES ('delete', old.rowid, old.title, old.body);
END;

CREATE TRIGGER search_index_au AFTER UPDATE ON search_index BEGIN
    INSERT INTO search_fts (search_fts, rowid, title, body)
        VALUES ('delete', old.rowid, old.title, old.body);
    INSERT INTO search_fts (rowid, title, body) VALUES (new.rowid, new.title, new.body);
END;
