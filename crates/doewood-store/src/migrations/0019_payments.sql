-- doewood instance store, schema 19: payments per document (issue #193).
-- Additive: a new empty table; no data changes.
--
-- One row per record with payment facts (quail's `payment.extract`): what is paid for, the
-- category, the headline amount in units of 10^-4 of its currency, the period and the
-- document's date (`YYYY-MM-DD`), for fast filters and per-currency sums. `detail` holds the
-- rest as canonical JSON text (`doewood_model::PaymentDetail`). Ingest rebuilds the row from
-- the record when it writes the document's graph. The supplier and buyer companies are read
-- through the document's `party` edges marked `side`. The rows go with their record.
CREATE TABLE payments (
    blake3         BLOB PRIMARY KEY NOT NULL REFERENCES records (blake3) ON DELETE CASCADE
        CHECK (length(blake3) = 32),
    subject        TEXT,
    category       TEXT,
    category_label TEXT,
    amount         INTEGER,
    basis          TEXT,
    currency       TEXT,
    period         TEXT,
    day            TEXT CHECK (day IS NULL OR length(day) = 10),
    supplier_name  TEXT,
    buyer_name     TEXT,
    conf           REAL NOT NULL CHECK (conf BETWEEN 0 AND 1),
    detail         TEXT NOT NULL,
    updated_at     INTEGER NOT NULL
) STRICT, WITHOUT ROWID;

CREATE INDEX payments_by_category ON payments (category, blake3);
