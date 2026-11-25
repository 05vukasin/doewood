-- doewood shared store, schema 7: a bounded GIN pending list for search (issue #102).
-- An index storage parameter only. Older instances refuse this schema, so every instance is
-- upgraded together.

-- With the default (`fastupdate` on, `gin_pending_list_limit` 4 MB) new rows wait in the
-- index's pending list until a vacuum or a full list merges them, and every search scans the
-- whole list: a rare-term lookup was 8x slower with a full list, so search latency depended
-- on when autovacuum last ran. 256 kB keeps the list short (a search scans at most 32 pages)
-- while inserts still batch their GIN updates, which `fastupdate = off` would not.
ALTER INDEX search_tsv SET (gin_pending_list_limit = 256);

-- Merge what is pending now; later inserts merge whenever the list passes 256 kB.
SELECT gin_clean_pending_list('search_tsv'::regclass);
