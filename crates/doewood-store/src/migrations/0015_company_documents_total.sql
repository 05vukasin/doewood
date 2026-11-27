-- doewood instance store, schema 15: the company graph's document total (issue #181). One new
-- row, counted from the graph; no existing rows change.
--
-- The documents with at least one live `party` edge, each once however many companies it
-- names. A fold (doewood-model `explore`) moves it with the other counts of schema 14.
CREATE TABLE company_graph_totals (
    id        INTEGER PRIMARY KEY CHECK (id = 1),
    documents INTEGER NOT NULL CHECK (documents >= 0)
) STRICT;

-- The total as folded so far: a document's party edges now, less its changes still in the
-- log, by target and role as a fold counts them (`fold_deltas`). The log's rows are
-- counted by the next fold as usual.
INSERT INTO company_graph_totals (id, documents)
    SELECT 1, count(DISTINCT node) FROM (
        SELECT node FROM (
            SELECT src AS node, dst AS other,
                   substr(coalesce(json_extract(props_json, '$.role'), ''), 1, 64) AS tag,
                   1 AS n
            FROM edges WHERE kind = 'party' AND state NOT IN ('retracted', 'superseded')
            UNION ALL
            SELECT node, other, tag, -delta FROM company_graph_log WHERE party = 1)
        GROUP BY node, other, tag HAVING sum(n) > 0);
