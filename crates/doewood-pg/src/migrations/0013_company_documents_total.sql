-- doewood shared store, schema 13: the company graph's document total (issue #181). One new
-- row, counted from the graph; no existing rows change. Older instances refuse this schema,
-- so every instance is upgraded together.
--
-- The documents with at least one live `party` edge, each once however many companies it
-- names. A fold (doewood-model `explore`) moves it with the other counts of schema 12,
-- under the same lock; writers never touch it.
CREATE TABLE company_graph_totals (
    id        smallint PRIMARY KEY CHECK (id = 1),
    documents bigint NOT NULL CHECK (documents >= 0)
);

-- No fold runs while this counts (an instance still on schema 12 may fold): the folds' lock.
SELECT pg_advisory_xact_lock(1685546853, 1);

-- The total as folded so far: a document's party edges now, less its changes still in the
-- log, by target and role as a fold counts them (`fold_deltas`). One statement, so the edges
-- and the log are read in one snapshot; the log's rows are counted by the next fold as usual.
INSERT INTO company_graph_totals (id, documents)
    SELECT 1, count(DISTINCT node) FROM (
        SELECT node FROM (
            SELECT src AS node, dst AS other, company_graph_tag(kind, props) AS tag, 1 AS n
            FROM edges WHERE kind = 'party' AND company_graph_counted(kind, state)
            UNION ALL
            SELECT node, other, tag, -delta FROM company_graph_log WHERE party) x
        GROUP BY node, other, tag HAVING sum(n) > 0) y;
