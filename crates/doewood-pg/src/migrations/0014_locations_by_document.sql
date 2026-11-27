-- doewood shared store, schema 14: an index on `locations.document_id` (issue #168). An index
-- only; no data changes. Older instances refuse this schema, so every instance is upgraded
-- together.
--
-- A purge deletes Document, Location and company nodes. For each node deleted, the foreign
-- key's cascade looks for `locations` rows that refer to it through `document_id`, which
-- without an index is a scan of the whole table per node. Built in the migration's
-- transaction, so writes to `locations` wait while it builds (about a second per million
-- rows).
CREATE INDEX locations_by_document ON locations (document_id);
