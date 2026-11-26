-- doewood shared store, schema 10: `sketch_band_sizes` keeps only the buckets of two or more
-- documents (issue #141). New trigger functions; no data changes. Older instances refuse this
-- schema, so every instance is upgraded together.

-- Most band keys belong to one document (92% of them on the #127 corpus), and their size rows
-- were about a seventh of the WAL ingest wrote: a heap row and an index entry on a random
-- page of `sketch_band_sizes_pkey` for each new band. Only a bucket above `similar_max_bucket`
-- (at least 10) is ever skipped, so a band without a row is a bucket of at most one document.
-- A row is made when its bucket gets a second document, with the bucket's exact count, and is
-- kept up to date from then on. Rows of one document written before this schema stay, with
-- their exact count.
--
-- Every change to a band's bucket takes a transaction-scoped advisory lock on the band (two-key
-- form, class 1685550946, "dwsb"; the key is folded to 31 bits, so two bands may share a lock,
-- which only makes one wait), in key order, before it reads or writes the size. Under the lock
-- no other transaction is changing that bucket, and each statement below takes a new snapshot
-- (READ COMMITTED), so the count read when a row is made sees every committed document.
CREATE FUNCTION sketch_band_lock_key(band bigint) RETURNS integer LANGUAGE sql IMMUTABLE
    PARALLEL SAFE RETURN ((band # (band >> 32)) & 2147483647)::integer;

CREATE OR REPLACE FUNCTION sketch_bands_added() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    k integer;
BEGIN
    FOR k IN SELECT DISTINCT sketch_band_lock_key(band) FROM added ORDER BY 1 LOOP
        PERFORM pg_advisory_xact_lock(1685550946, k);
    END LOOP;
    INSERT INTO sketch_band_sizes AS z (band, docs)
        SELECT a.band,
               CASE WHEN EXISTS (SELECT 1 FROM sketch_band_sizes s WHERE s.band = a.band)
                    THEN a.n
                    ELSE (SELECT count(*) FROM sketch_bands b WHERE b.band = a.band) END
        FROM (SELECT band, count(*) AS n FROM added GROUP BY band) a
        WHERE EXISTS (SELECT 1 FROM sketch_band_sizes s WHERE s.band = a.band)
           OR (SELECT count(*) FROM (SELECT 1 FROM sketch_bands b WHERE b.band = a.band LIMIT 2) x) > 1
        ORDER BY a.band
        ON CONFLICT (band) DO UPDATE SET docs = z.docs + EXCLUDED.docs;
    RETURN NULL;
END $$;

CREATE OR REPLACE FUNCTION sketch_bands_removed() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    k integer;
BEGIN
    FOR k IN SELECT DISTINCT sketch_band_lock_key(band) FROM removed ORDER BY 1 LOOP
        PERFORM pg_advisory_xact_lock(1685550946, k);
    END LOOP;
    UPDATE sketch_band_sizes z SET docs = z.docs - r.n
        FROM (SELECT band, count(*) AS n FROM removed GROUP BY band) r WHERE z.band = r.band;
    RETURN NULL;
END $$;
