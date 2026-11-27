-- doewood shared store, schema 18: key facts for AI retrieval (issue #194).
-- Older instances refuse this schema, so every instance is upgraded together.
--
-- 1. A new empty table, `document_facts`: one row per record with key facts (quail's
--    `card.facts` and the `<namespace>.highlight` facts). `card` holds the normalised
--    key-facts card as canonical JSON text (`doewood_model::Card`: each field with its
--    confidence and highlights), `highlights` every highlight of the record
--    (`doewood_model::BlockHighlight`: fact key, block, characters, confidence); the other
--    columns are the card's searchable fields for `FindDocuments`, amounts in units of 10^-4
--    of their currency, the `_fold` columns lowercased with the Serbian diacritics folded
--    (`doewood_model::fold`) for contains matches. Ingest rebuilds the row from the record when
--    it writes the document's graph. The rows go with their record.
-- 2. Search weights the key facts: `search_index.facts` (the card's text and the passages its
--    fields were read from) joins the full-text vector with the title's weight (A, above the
--    body's B). The vector is a generated column, so it is dropped and added again: this
--    rewrites `search_index` and rebuilds its GIN index once. Rows stored before have no facts
--    until their document is written again (the new quail steps change the analyzer plan, so
--    every record is analyzed again).
CREATE TABLE document_facts (
    blake3        bytea PRIMARY KEY REFERENCES records (blake3) ON DELETE CASCADE
        CHECK (octet_length(blake3) = 32),
    kind          text,
    number        text,
    subject       text,
    supplier_name text,
    supplier_fold text,
    customer_name text,
    customer_fold text,
    category      text,
    amount        bigint,
    currency      text,
    period        text,
    signed        date,
    ends          date,
    notice_due    date,
    auto_renewal  boolean,
    has_penalty   boolean NOT NULL,
    has_guarantee boolean NOT NULL,
    governing_law text,
    court         text,
    court_fold    text,
    card          text NOT NULL,
    highlights    text NOT NULL,
    updated_at    timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX document_facts_by_kind ON document_facts (kind, blake3);
CREATE INDEX document_facts_by_ends ON document_facts (ends) WHERE ends IS NOT NULL;
CREATE INDEX document_facts_by_notice ON document_facts (notice_due) WHERE notice_due IS NOT NULL;

ALTER TABLE search_index ADD COLUMN facts text NOT NULL DEFAULT '';
ALTER TABLE search_index DROP COLUMN tsv;
ALTER TABLE search_index ADD COLUMN tsv tsvector GENERATED ALWAYS AS (
    setweight(to_tsvector('simple', title), 'A')
    || setweight(to_tsvector('simple', facts), 'A')
    || setweight(to_tsvector('simple', body), 'B')
) STORED;
-- As schema 7 set it (issue #102).
CREATE INDEX search_tsv ON search_index USING gin (tsv) WITH (gin_pending_list_limit = 256);
