-- doewood shared store, schema 19: common-word search as fast as before the key facts
-- (issue #198). Older instances refuse this schema, so every instance is upgraded together.
--
-- Schema 18 put the key facts in every document's full-text vector, so each match of a
-- common word read a larger vector and `search_index` grew; a word in a quarter of the
-- documents went from 10 to 31 ms on 50,000 documents. Now:
-- 1. `search_index.tsv` is the title and the body again (schema 17's vector, the same for
--    every document), and `has_facts` says whether the document has key facts.
-- 2. The new table `search_facts` holds, for each document with key facts, its title and
--    filter fields, the facts text, the full vector `tsv` (title A, body B, then the facts
--    A: the body's positions are the ones of `search_index.tsv`) and the facts alone (`ftsv`).
-- A search whose words are in no document's facts reads `search_index` alone, exactly as in
-- schema 17 (ranking a document's title and body ranks it as its full vector would, since
-- none of the words are in its facts). Otherwise it reads the documents with facts from
-- `search_facts` and the others from `search_index`, as schema 18 did.
-- The migration copies the facts of schema 18's rows and rewrites `search_index` once (the
-- vector is a generated column), rebuilding its GIN index; `has_facts` is the old `facts`
-- column, converted in the same rewrite.
CREATE TABLE search_facts (
    blake3   bytea PRIMARY KEY REFERENCES records (blake3) ON DELETE CASCADE,
    title    text NOT NULL,
    kind     text,
    doc_date date,
    amount   double precision,
    facts    text NOT NULL,
    tsv      tsvector NOT NULL,
    ftsv     tsvector GENERATED ALWAYS AS (to_tsvector('simple', facts)) STORED
);

INSERT INTO search_facts (blake3, title, kind, doc_date, amount, facts, tsv)
    SELECT blake3, title, kind, doc_date, amount, facts,
           setweight(to_tsvector('simple', title), 'A')
           || setweight(to_tsvector('simple', body), 'B')
           || setweight(to_tsvector('simple', facts), 'A')
    FROM search_index WHERE facts <> '';

-- As schema 7 set it for `search_tsv` (issue #102).
CREATE INDEX search_facts_tsv ON search_facts USING gin (tsv) WITH (gin_pending_list_limit = 256);
CREATE INDEX search_facts_ftsv ON search_facts USING gin (ftsv) WITH (gin_pending_list_limit = 256);

-- One rewrite of `search_index`: `facts` becomes the flag, and the vector is added again.
ALTER TABLE search_index
    DROP COLUMN tsv,
    ALTER COLUMN facts DROP DEFAULT,
    ALTER COLUMN facts TYPE boolean USING facts <> '',
    ALTER COLUMN facts SET DEFAULT false,
    ADD COLUMN tsv tsvector GENERATED ALWAYS AS (
        setweight(to_tsvector('simple', title), 'A')
        || setweight(to_tsvector('simple', body), 'B')
    ) STORED;
ALTER TABLE search_index RENAME COLUMN facts TO has_facts;
CREATE INDEX search_tsv ON search_index USING gin (tsv) WITH (gin_pending_list_limit = 256);
