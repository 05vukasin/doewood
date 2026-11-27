-- doewood shared store, schema 12: the company graph's counts (issue #172). New tables,
-- indexes and triggers, filled from the graph; no existing rows change. Older instances
-- refuse this schema, so every instance is upgraded together.
--
-- A company's documents and degree, and the weight of each pair of companies, are kept here
-- so that ListCompanies and CompanyGraph never aggregate the `party` edges. Writers only
-- append to `company_graph_log` (the statement triggers below: one INSERT per statement,
-- rows nobody updates, so writers never wait for each other or for a fold there). A fold
-- (doewood-model `explore`) moves the log into the counts under a transaction-scoped
-- advisory lock that only folds take (class 1685546853, "dwce", key 1). The log starts with
-- every live edge, so the first fold counts the graph as it is.

-- Every company, with its counts as last folded. The row is made with the company.
CREATE TABLE company_stats (
    company_id uuid PRIMARY KEY REFERENCES nodes (id) ON DELETE CASCADE,
    documents  bigint NOT NULL DEFAULT 0 CHECK (documents >= 0),
    degree     bigint NOT NULL DEFAULT 0 CHECK (degree >= 0)
);

CREATE INDEX company_stats_by_documents ON company_stats (documents, company_id);
CREATE INDEX company_stats_by_degree ON company_stats (degree, company_id);

INSERT INTO company_stats (company_id) SELECT id FROM nodes WHERE kind = 'company';

CREATE FUNCTION company_stats_made() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    INSERT INTO company_stats (company_id)
        SELECT id FROM added WHERE kind = 'company' ORDER BY id
        ON CONFLICT DO NOTHING;
    RETURN NULL;
END $$;

CREATE TRIGGER nodes_company_stats AFTER INSERT ON nodes
    REFERENCING NEW TABLE AS added FOR EACH STATEMENT EXECUTE FUNCTION company_stats_made();

-- Pairs of companies (a < b) that share documents or a corporate-event edge, as last folded.
-- `roles` counts each side's roles in the shared documents: {"a:buyer": 3, "b:supplier": 3}.
-- A pair whose counts reach zero is deleted.
CREATE TABLE company_links (
    a      uuid NOT NULL,
    b      uuid NOT NULL,
    docs   bigint NOT NULL CHECK (docs >= 0),
    events bigint NOT NULL CHECK (events >= 0),
    roles  jsonb NOT NULL DEFAULT '{}',
    PRIMARY KEY (a, b),
    CHECK (a < b)
);

-- Changes to live `party` and corporate-event edges, not yet folded: +1 when an edge became
-- live (or was written live), -1 when it stopped (or was deleted). `node` is the edge's
-- source, `other` its target; `tag` is a party edge's role (as text, at most 64 characters)
-- or an event's kind.
CREATE TABLE company_graph_log (
    id    bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    node  uuid NOT NULL,
    other uuid NOT NULL,
    party boolean NOT NULL,
    tag   text NOT NULL,
    delta smallint NOT NULL CHECK (delta IN (-1, 1))
);

CREATE INDEX company_graph_log_by_node ON company_graph_log (node);

-- What the log counts: a live party or corporate-event edge, and its tag.
CREATE FUNCTION company_graph_counted(kind text, state text) RETURNS boolean LANGUAGE sql
    IMMUTABLE PARALLEL SAFE
    RETURN kind IN ('party', 'parent_of', 'subsidiary_of', 'acquired', 'merged_into',
                    'renamed_to', 'branch_of', 'trading_name_of', 'former_parent_of',
                    'liquidated')
       AND state NOT IN ('retracted', 'superseded');

CREATE FUNCTION company_graph_tag(kind text, props jsonb) RETURNS text LANGUAGE sql
    IMMUTABLE PARALLEL SAFE
    RETURN CASE WHEN kind = 'party' THEN left(coalesce(props->>'role', ''), 64) ELSE kind END;

INSERT INTO company_graph_log (node, other, party, tag, delta)
    SELECT src, dst, kind = 'party', company_graph_tag(kind, props), 1
    FROM edges WHERE company_graph_counted(kind, state);

CREATE FUNCTION company_graph_added() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    INSERT INTO company_graph_log (node, other, party, tag, delta)
        SELECT src, dst, kind = 'party', company_graph_tag(kind, props), 1
        FROM added WHERE company_graph_counted(kind, state);
    RETURN NULL;
END $$;

CREATE FUNCTION company_graph_removed() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    INSERT INTO company_graph_log (node, other, party, tag, delta)
        SELECT src, dst, kind = 'party', company_graph_tag(kind, props), -1
        FROM removed WHERE company_graph_counted(kind, state);
    RETURN NULL;
END $$;

-- An update that changes what is counted (an end, the kind, being live, a party's role)
-- logs the old edge out and the new one in. Rewriting an edge as it was (an upsert of the
-- same edge) logs nothing.
CREATE FUNCTION company_graph_changed() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    INSERT INTO company_graph_log (node, other, party, tag, delta)
        SELECT x.node, x.other, x.party, x.tag, x.delta
        FROM old_rows o JOIN new_rows n ON n.id = o.id
        CROSS JOIN LATERAL (VALUES
            (o.src, o.dst, o.kind = 'party', company_graph_tag(o.kind, o.props), -1,
             company_graph_counted(o.kind, o.state)),
            (n.src, n.dst, n.kind = 'party', company_graph_tag(n.kind, n.props), 1,
             company_graph_counted(n.kind, n.state))) x (node, other, party, tag, delta, counted)
        WHERE x.counted
          AND (o.src, o.dst, o.kind, company_graph_counted(o.kind, o.state),
               company_graph_tag(o.kind, o.props))
              IS DISTINCT FROM
              (n.src, n.dst, n.kind, company_graph_counted(n.kind, n.state),
               company_graph_tag(n.kind, n.props));
    RETURN NULL;
END $$;

CREATE TRIGGER edges_company_graph_insert AFTER INSERT ON edges
    REFERENCING NEW TABLE AS added FOR EACH STATEMENT EXECUTE FUNCTION company_graph_added();

CREATE TRIGGER edges_company_graph_delete AFTER DELETE ON edges
    REFERENCING OLD TABLE AS removed FOR EACH STATEMENT
    EXECUTE FUNCTION company_graph_removed();

CREATE TRIGGER edges_company_graph_update AFTER UPDATE ON edges
    REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows FOR EACH STATEMENT
    EXECUTE FUNCTION company_graph_changed();

-- ListCompanies by name: the lowercased name in byte order, then id. Queries spell the
-- expression exactly so.
CREATE INDEX nodes_company_name ON nodes
    ((lower(coalesce(props->>'name', '')) COLLATE "C"), id) WHERE kind = 'company';
