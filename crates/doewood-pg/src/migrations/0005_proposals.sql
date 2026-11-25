-- doewood shared store, schema 5: enrichment proposals (ADR 0002 section 1.6, issue #52).
-- A new table, and the proposals schema 4 kept in company props moved into it. Older
-- instances refuse this schema, so every instance is upgraded together.

-- What the decision gate leaves to a person: a registry record to adopt (candidate
-- `<registry>:<record>`) or a company to merge with (candidate the other company's id; the
-- pair in id order, `company_id` the smaller). One open proposal per kind, company and
-- candidate.
CREATE TABLE company_proposals (
    id                uuid PRIMARY KEY,
    seq               bigint GENERATED ALWAYS AS IDENTITY UNIQUE,
    kind              text NOT NULL CHECK (kind IN ('registry_record', 'merge')),
    company_id        uuid NOT NULL REFERENCES nodes (id) ON DELETE CASCADE,
    candidate         text NOT NULL CHECK (length(candidate) BETWEEN 1 AND 512),
    detail            jsonb NOT NULL DEFAULT '{}',
    evidence          jsonb NOT NULL DEFAULT '[]',
    probability       real NOT NULL CHECK (probability BETWEEN 0 AND 1),
    reason            text NOT NULL CHECK (reason IN ('review_band', 'conflict', 'dismissed')),
    state             text NOT NULL DEFAULT 'open'
                      CHECK (state IN ('open', 'accepted', 'rejected')),
    created_at        timestamptz NOT NULL,
    updated_at        timestamptz NOT NULL,
    resolved_at       timestamptz,
    resolved_actor    text CHECK (resolved_actor IN ('ingest', 'enrichment', 'manual')),
    resolved_instance text,
    dismissed         boolean NOT NULL DEFAULT false,
    merge_id          uuid,
    note              text NOT NULL DEFAULT '',
    CHECK ((state = 'open') = (resolved_at IS NULL)),
    CHECK ((resolved_actor IS NULL) = (resolved_instance IS NULL)),
    CHECK (NOT dismissed OR state = 'rejected')
);

CREATE UNIQUE INDEX company_proposals_open ON company_proposals (kind, company_id, candidate)
    WHERE state = 'open';
CREATE INDEX company_proposals_by_company ON company_proposals (company_id, seq);
CREATE INDEX company_proposals_by_candidate ON company_proposals (candidate, seq)
    WHERE kind = 'merge';
CREATE INDEX company_proposals_by_state ON company_proposals (state, seq);

-- The props lists of schema 4, `merge_proposals` and `registry_proposals`, into the table,
-- once (this migration runs once, in the transaction that records schema 5). Malformed
-- entries are left out. A pair kept as `dismissed` is closed (rejected, dismissed, by no
-- one); the rest are open. Duplicates keep the latest entry.
WITH entries AS (
    SELECT n.id AS company, e.value AS p,
           CASE WHEN e.value->>'company' ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
                THEN (e.value->>'company')::uuid END AS other,
           CASE WHEN jsonb_typeof(e.value->'at') = 'number'
                THEN to_timestamp((e.value->>'at')::double precision) ELSE now() END AS at,
           CASE WHEN jsonb_typeof(e.value->'probability') = 'number'
                THEN least(1, greatest(0, (e.value->>'probability')::real)) ELSE 0 END AS probability,
           CASE e.value->>'reason' WHEN 'conflict' THEN 'conflict' WHEN 'dismissed' THEN 'dismissed'
                ELSE 'review_band' END AS reason
    FROM nodes n
    CROSS JOIN LATERAL jsonb_array_elements(
        CASE WHEN jsonb_typeof(n.props->'merge_proposals') = 'array'
             THEN n.props->'merge_proposals' ELSE '[]'::jsonb END) e
    WHERE n.kind = 'company' AND jsonb_typeof(e.value) = 'object'
), pairs AS (
    SELECT DISTINCT ON (least(company, other), greatest(company, other), reason = 'dismissed')
           least(company, other) AS a, greatest(company, other) AS b, company, p, at,
           probability, reason
    FROM entries
    WHERE other IS NOT NULL AND other <> company
      AND EXISTS (SELECT 1 FROM nodes c WHERE c.id = other AND c.kind = 'company')
    ORDER BY least(company, other), greatest(company, other), reason = 'dismissed', at DESC
)
INSERT INTO company_proposals (id, kind, company_id, candidate, detail, evidence, probability,
                               reason, state, created_at, updated_at, resolved_at, dismissed,
                               note)
SELECT gen_random_uuid(), 'merge', a, b::text,
       (p - 'company' - 'evidence' - 'probability' - 'at' - 'reason')
           || jsonb_build_object('from_props', company::text, 'props_reason', p->'reason'),
       CASE WHEN jsonb_typeof(p->'evidence') = 'array' THEN p->'evidence' ELSE '[]' END,
       probability, reason,
       CASE WHEN reason = 'dismissed' THEN 'rejected' ELSE 'open' END,
       at, at,
       CASE WHEN reason = 'dismissed' THEN at END,
       reason = 'dismissed',
       CASE WHEN reason = 'dismissed' THEN 'the pair is dismissed (moved from props, schema 5)'
            ELSE '' END
FROM pairs;

WITH entries AS (
    SELECT n.id AS company, e.value AS p,
           (e.value->>'registry') || ':' || (e.value->>'record') AS candidate,
           CASE WHEN jsonb_typeof(e.value->'at') = 'number'
                THEN to_timestamp((e.value->>'at')::double precision) ELSE now() END AS at,
           CASE WHEN jsonb_typeof(e.value->'probability') = 'number'
                THEN least(1, greatest(0, (e.value->>'probability')::real)) ELSE 0 END AS probability,
           CASE e.value->>'reason' WHEN 'conflict' THEN 'conflict' ELSE 'review_band' END AS reason
    FROM nodes n
    CROSS JOIN LATERAL jsonb_array_elements(
        CASE WHEN jsonb_typeof(n.props->'registry_proposals') = 'array'
             THEN n.props->'registry_proposals' ELSE '[]'::jsonb END) e
    WHERE n.kind = 'company' AND jsonb_typeof(e.value) = 'object'
      AND jsonb_typeof(e.value->'registry') = 'string' AND e.value->>'registry' <> ''
      AND jsonb_typeof(e.value->'record') = 'string' AND e.value->>'record' <> ''
), records AS (
    SELECT DISTINCT ON (company, candidate) company, candidate, p, at, probability, reason
    FROM entries
    WHERE length(candidate) <= 512
    ORDER BY company, candidate, at DESC
)
INSERT INTO company_proposals (id, kind, company_id, candidate, detail, evidence, probability,
                               reason, state, created_at, updated_at)
SELECT gen_random_uuid(), 'registry_record', company, candidate,
       jsonb_build_object('registry', p->'registry', 'record', p->'record', 'name', p->'name',
                          'from_props', company::text, 'props_reason', p->'reason'),
       jsonb_build_array(jsonb_build_object('registry', p->'registry', 'record', p->'record',
                                            'name', p->'name')),
       probability, reason, 'open', at, at
FROM records;

-- The lists leave the props; the job no longer writes them.
UPDATE nodes SET props = props - 'merge_proposals' - 'registry_proposals', updated_at = now()
WHERE kind = 'company' AND (props ? 'merge_proposals' OR props ? 'registry_proposals');
