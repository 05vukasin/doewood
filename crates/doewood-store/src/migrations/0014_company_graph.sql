-- doewood instance store, schema 14: the company graph's counts (issue #172). New tables,
-- indexes and triggers, filled from the graph; no existing rows change.
--
-- A company's documents and degree, and the weight of each pair of companies, are kept here
-- so that ListCompanies and CompanyGraph never aggregate the `party` edges. Writers only
-- append to `company_graph_log` (the triggers below); a fold (doewood-model `explore`) moves
-- the log into the counts. The log starts with every live edge, so the first fold counts the
-- graph as it is.

-- Every company, with its counts as last folded. The row is made with the company.
CREATE TABLE company_stats (
    company_id TEXT PRIMARY KEY NOT NULL REFERENCES nodes (id) ON DELETE CASCADE,
    documents  INTEGER NOT NULL DEFAULT 0 CHECK (documents >= 0),
    degree     INTEGER NOT NULL DEFAULT 0 CHECK (degree >= 0)
) STRICT, WITHOUT ROWID;

CREATE INDEX company_stats_by_documents ON company_stats (documents, company_id);
CREATE INDEX company_stats_by_degree ON company_stats (degree, company_id);

INSERT INTO company_stats (company_id) SELECT id FROM nodes WHERE kind = 'company';

CREATE TRIGGER nodes_company_stats AFTER INSERT ON nodes WHEN NEW.kind = 'company'
BEGIN
    INSERT OR IGNORE INTO company_stats (company_id) VALUES (NEW.id);
END;

-- Pairs of companies (a < b) that share documents or a corporate-event edge, as last folded.
-- `roles` counts each side's roles in the shared documents: {"a:buyer": 3, "b:supplier": 3}.
-- A pair whose counts reach zero is deleted.
CREATE TABLE company_links (
    a      TEXT NOT NULL,
    b      TEXT NOT NULL,
    docs   INTEGER NOT NULL CHECK (docs >= 0),
    events INTEGER NOT NULL CHECK (events >= 0),
    roles  TEXT NOT NULL DEFAULT '{}' CHECK (json_valid(roles)),
    PRIMARY KEY (a, b),
    CHECK (a < b)
) STRICT, WITHOUT ROWID;

-- Changes to live `party` and corporate-event edges, not yet folded: +1 when an edge became
-- live (or was written live), -1 when it stopped (or was deleted). `node` is the edge's
-- source, `other` its target; `tag` is a party edge's role (as text, at most 64 characters)
-- or an event's kind.
CREATE TABLE company_graph_log (
    id    INTEGER PRIMARY KEY,
    node  TEXT NOT NULL,
    other TEXT NOT NULL,
    party INTEGER NOT NULL CHECK (party IN (0, 1)),
    tag   TEXT NOT NULL,
    delta INTEGER NOT NULL CHECK (delta IN (-1, 1))
) STRICT;

CREATE INDEX company_graph_log_by_node ON company_graph_log (node);

INSERT INTO company_graph_log (node, other, party, tag, delta)
    SELECT src, dst, kind = 'party',
           CASE WHEN kind = 'party'
                THEN substr(coalesce(json_extract(props_json, '$.role'), ''), 1, 64)
                ELSE kind END,
           1
    FROM edges
    -- Party and the corporate-event kinds: every kind but these four.
    WHERE kind NOT IN ('located_at', 'amends', 'cites', 'similar_to')
      AND state NOT IN ('retracted', 'superseded');

CREATE TRIGGER edges_company_graph_insert AFTER INSERT ON edges
WHEN NEW.kind IN ('party', 'parent_of', 'subsidiary_of', 'acquired', 'merged_into',
                  'renamed_to', 'branch_of', 'trading_name_of', 'former_parent_of',
                  'liquidated')
 AND NEW.state NOT IN ('retracted', 'superseded')
BEGIN
    INSERT INTO company_graph_log (node, other, party, tag, delta)
    VALUES (NEW.src, NEW.dst, NEW.kind = 'party',
            CASE WHEN NEW.kind = 'party'
                 THEN substr(coalesce(json_extract(NEW.props_json, '$.role'), ''), 1, 64)
                 ELSE NEW.kind END,
            1);
END;

CREATE TRIGGER edges_company_graph_delete AFTER DELETE ON edges
WHEN OLD.kind IN ('party', 'parent_of', 'subsidiary_of', 'acquired', 'merged_into',
                  'renamed_to', 'branch_of', 'trading_name_of', 'former_parent_of',
                  'liquidated')
 AND OLD.state NOT IN ('retracted', 'superseded')
BEGIN
    INSERT INTO company_graph_log (node, other, party, tag, delta)
    VALUES (OLD.src, OLD.dst, OLD.kind = 'party',
            CASE WHEN OLD.kind = 'party'
                 THEN substr(coalesce(json_extract(OLD.props_json, '$.role'), ''), 1, 64)
                 ELSE OLD.kind END,
            -1);
END;

-- An update that changes what is counted (an end, the kind, being live, a party's role)
-- logs the old edge out and the new one in. Rewriting an edge as it was logs nothing.
CREATE TRIGGER edges_company_graph_update AFTER UPDATE OF src, dst, kind, state, props_json
ON edges
WHEN (OLD.kind IN ('party', 'parent_of', 'subsidiary_of', 'acquired', 'merged_into',
                   'renamed_to', 'branch_of', 'trading_name_of', 'former_parent_of',
                   'liquidated')
      OR NEW.kind IN ('party', 'parent_of', 'subsidiary_of', 'acquired', 'merged_into',
                      'renamed_to', 'branch_of', 'trading_name_of', 'former_parent_of',
                      'liquidated'))
 AND (OLD.src IS NOT NEW.src OR OLD.dst IS NOT NEW.dst OR OLD.kind IS NOT NEW.kind
      OR (OLD.state IN ('retracted', 'superseded')) IS NOT (NEW.state IN ('retracted', 'superseded'))
      OR json_extract(OLD.props_json, '$.role') IS NOT json_extract(NEW.props_json, '$.role'))
BEGIN
    INSERT INTO company_graph_log (node, other, party, tag, delta)
    SELECT OLD.src, OLD.dst, OLD.kind = 'party',
           CASE WHEN OLD.kind = 'party'
                THEN substr(coalesce(json_extract(OLD.props_json, '$.role'), ''), 1, 64)
                ELSE OLD.kind END,
           -1
    WHERE OLD.kind IN ('party', 'parent_of', 'subsidiary_of', 'acquired', 'merged_into',
                       'renamed_to', 'branch_of', 'trading_name_of', 'former_parent_of',
                       'liquidated')
      AND OLD.state NOT IN ('retracted', 'superseded');
    INSERT INTO company_graph_log (node, other, party, tag, delta)
    SELECT NEW.src, NEW.dst, NEW.kind = 'party',
           CASE WHEN NEW.kind = 'party'
                THEN substr(coalesce(json_extract(NEW.props_json, '$.role'), ''), 1, 64)
                ELSE NEW.kind END,
           1
    WHERE NEW.kind IN ('party', 'parent_of', 'subsidiary_of', 'acquired', 'merged_into',
                       'renamed_to', 'branch_of', 'trading_name_of', 'former_parent_of',
                       'liquidated')
      AND NEW.state NOT IN ('retracted', 'superseded');
END;

-- ListCompanies by name: the lowercased name (byte order; ASCII letters only, as SQLite's
-- lower() folds), then id. Queries spell the expression exactly so.
CREATE INDEX nodes_company_name ON nodes
    (lower(coalesce(json_extract(props_json, '$.name'), '')), id) WHERE kind = 'company';
