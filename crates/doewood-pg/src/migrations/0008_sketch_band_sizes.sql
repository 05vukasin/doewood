-- doewood shared store, schema 8: the size of every LSH band bucket (issue #148). A new table
-- and triggers; no data changes. Older instances refuse this schema, so every instance is
-- upgraded together.

-- How many documents carry each band key, kept by the triggers below in the same transaction
-- as every change to `sketch_bands` (cascades from a deleted record included). The
-- `similar_to` candidate query skips a band whose bucket is larger than the runtime setting
-- `ingest.similar_max_bucket` (the LSH "stop band") by reading these, one row per band.
CREATE TABLE sketch_band_sizes (
    band bigint PRIMARY KEY,
    docs integer NOT NULL
);

INSERT INTO sketch_band_sizes (band, docs) SELECT band, count(*) FROM sketch_bands GROUP BY band;

-- Statement triggers: one upsert per statement for all its bands. The rows are locked in
-- band order, so two documents adding the same bands at once wait for each other instead of
-- deadlocking.
CREATE FUNCTION sketch_bands_added() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    INSERT INTO sketch_band_sizes AS z (band, docs)
        SELECT band, count(*) FROM added GROUP BY band ORDER BY band
        ON CONFLICT (band) DO UPDATE SET docs = z.docs + EXCLUDED.docs;
    RETURN NULL;
END $$;

CREATE FUNCTION sketch_bands_removed() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    PERFORM 1 FROM sketch_band_sizes WHERE band IN (SELECT band FROM removed)
        ORDER BY band FOR UPDATE;
    UPDATE sketch_band_sizes z SET docs = z.docs - r.n
        FROM (SELECT band, count(*) AS n FROM removed GROUP BY band) r WHERE z.band = r.band;
    RETURN NULL;
END $$;

CREATE TRIGGER sketch_bands_size_insert AFTER INSERT ON sketch_bands
    REFERENCING NEW TABLE AS added FOR EACH STATEMENT EXECUTE FUNCTION sketch_bands_added();

CREATE TRIGGER sketch_bands_size_delete AFTER DELETE ON sketch_bands
    REFERENCING OLD TABLE AS removed FOR EACH STATEMENT EXECUTE FUNCTION sketch_bands_removed();
