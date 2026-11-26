-- doewood instance store, schema 11: the size of every LSH band bucket (issue #148). A new
-- table and triggers; no data changes.

-- How many documents carry each band key, kept by the triggers below in the same transaction
-- as every change to `sketch_bands` (cascades from a deleted record included). The
-- `similar_to` candidate query skips a band whose bucket is larger than the runtime setting
-- `ingest.similar_max_bucket` (the LSH "stop band") by reading these, one row per band.
CREATE TABLE sketch_band_sizes (
    band INTEGER PRIMARY KEY NOT NULL,
    docs INTEGER NOT NULL
) STRICT, WITHOUT ROWID;

INSERT INTO sketch_band_sizes (band, docs) SELECT band, count(*) FROM sketch_bands GROUP BY band;

CREATE TRIGGER sketch_bands_size_insert AFTER INSERT ON sketch_bands
BEGIN
    INSERT INTO sketch_band_sizes (band, docs) VALUES (new.band, 1)
        ON CONFLICT (band) DO UPDATE SET docs = docs + 1;
END;

CREATE TRIGGER sketch_bands_size_delete AFTER DELETE ON sketch_bands
BEGIN
    UPDATE sketch_band_sizes SET docs = docs - 1 WHERE band = old.band;
END;
