-- doewood instance store, schema 16: an index on `locations.document_id` (issue #168). An
-- index only; no data changes.
--
-- A purge deletes Document, Location and company nodes. Each node deleted makes SQLite look
-- for `locations` rows that refer to it through `document_id` (the foreign key's cascade),
-- which without an index reads the whole table once per node.
CREATE INDEX locations_by_document ON locations (document_id);
