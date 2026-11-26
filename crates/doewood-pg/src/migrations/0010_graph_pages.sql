-- doewood shared store, schema 10: keyset pages of Neighbours and CompanyDocuments
-- (issue #112). The incoming-edge index carries the order of a node's edges (kind, then
-- valid_from with undated first, then id), so that a page starts at its token and stops at
-- its size instead of sorting every edge of a big company. `coalesce` puts an undated edge
-- at -infinity, before every date; queries spell the expression exactly so.
DROP INDEX edges_by_dst;
CREATE INDEX edges_by_dst ON edges (dst, kind, coalesce(valid_from, '-infinity'::date), id);
