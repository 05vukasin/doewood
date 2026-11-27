-- doewood shared store, schema 17: payments per document (issue #193).
-- Additive: one new empty table; no existing rows change. Older instances refuse this schema,
-- so every instance is upgraded together.
--
-- One row per record with payment facts (quail's `payment.extract`): what is paid for, the
-- category, the headline amount in units of 10^-4 of its currency, the period and the
-- document's date, for fast filters and per-currency sums. `detail` holds the rest as
-- canonical JSON text (`doewood_model::PaymentDetail`: net, VAT, gross, line items,
-- highlights). Ingest rebuilds the row from the record when it writes the document's graph;
-- records stored before this schema get one when they are analyzed again (the new step
-- changes the plan, so they are). The supplier and buyer companies are not stored here: they
-- are read through the document's `party` edges marked `side` (merges move those). The rows
-- go with their record.
CREATE TABLE payments (
    blake3         bytea PRIMARY KEY REFERENCES records (blake3) ON DELETE CASCADE
        CHECK (octet_length(blake3) = 32),
    subject        text,
    category       text,
    category_label text,
    amount         bigint,
    basis          text,
    currency       text,
    period         text,
    day            date,
    supplier_name  text,
    buyer_name     text,
    conf           real NOT NULL CHECK (conf BETWEEN 0 AND 1),
    detail         text NOT NULL,
    updated_at     timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX payments_by_category ON payments (category, blake3);
