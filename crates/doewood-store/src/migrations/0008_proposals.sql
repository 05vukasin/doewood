-- doewood instance store, schema 8: enrichment proposals in standalone mode (ADR 0002
-- section 1.6, issue #52), the same as shared schema 5. Times are Unix seconds (UTC).

-- What the decision gate leaves to a person: a registry record to adopt (candidate
-- `<registry>:<record>`) or a company to merge with (candidate the other company's id; the
-- pair in id order, `company_id` the smaller). One open proposal per kind, company and
-- candidate.
CREATE TABLE company_proposals (
    seq               INTEGER PRIMARY KEY AUTOINCREMENT,
    id                TEXT NOT NULL UNIQUE,
    kind              TEXT NOT NULL CHECK (kind IN ('registry_record', 'merge')),
    company_id        TEXT NOT NULL REFERENCES nodes (id) ON DELETE CASCADE,
    candidate         TEXT NOT NULL CHECK (length(candidate) BETWEEN 1 AND 512),
    detail_json       TEXT NOT NULL DEFAULT '{}' CHECK (json_valid(detail_json)),
    evidence_json     TEXT NOT NULL DEFAULT '[]' CHECK (json_valid(evidence_json)),
    probability       REAL NOT NULL CHECK (probability BETWEEN 0 AND 1),
    reason            TEXT NOT NULL CHECK (reason IN ('review_band', 'conflict', 'dismissed')),
    state             TEXT NOT NULL DEFAULT 'open'
                      CHECK (state IN ('open', 'accepted', 'rejected')),
    created_at        INTEGER NOT NULL,
    updated_at        INTEGER NOT NULL,
    resolved_at       INTEGER,
    resolved_actor    TEXT CHECK (resolved_actor IN ('ingest', 'enrichment', 'manual')),
    resolved_instance TEXT,
    dismissed         INTEGER NOT NULL DEFAULT 0 CHECK (dismissed IN (0, 1)),
    merge_id          TEXT,
    note              TEXT NOT NULL DEFAULT '',
    CHECK ((state = 'open') = (resolved_at IS NULL)),
    CHECK ((resolved_actor IS NULL) = (resolved_instance IS NULL)),
    CHECK (dismissed = 0 OR state = 'rejected')
) STRICT;

CREATE UNIQUE INDEX company_proposals_open ON company_proposals (kind, company_id, candidate)
    WHERE state = 'open';
CREATE INDEX company_proposals_by_company ON company_proposals (company_id, seq);
CREATE INDEX company_proposals_by_candidate ON company_proposals (candidate, seq)
    WHERE kind = 'merge';
CREATE INDEX company_proposals_by_state ON company_proposals (state, seq);

-- The props lists of schema 7, `merge_proposals` and `registry_proposals`, into the table,
-- once (this migration runs once, in the transaction that records schema 8). Malformed
-- entries are left out. A pair kept as `dismissed` is closed (rejected, dismissed, by no
-- one); the rest are open. Duplicates keep the latest entry. Ids are random (version 4).
INSERT INTO company_proposals (id, kind, company_id, candidate, detail_json, evidence_json,
                               probability, reason, state, created_at, updated_at,
                               resolved_at, dismissed, note)
SELECT lower(hex(randomblob(4))) || '-' || lower(hex(randomblob(2))) || '-4'
           || substr(lower(hex(randomblob(2))), 2) || '-'
           || substr('89ab', 1 + (abs(random()) % 4), 1)
           || substr(lower(hex(randomblob(2))), 2) || '-' || lower(hex(randomblob(6))),
       'merge', a, b,
       json_set(json_remove(p, '$.company', '$.evidence', '$.probability', '$.at', '$.reason'),
                '$.from_props', company, '$.props_reason', json_extract(p, '$.reason')),
       CASE WHEN json_type(p, '$.evidence') = 'array' THEN json_extract(p, '$.evidence')
            ELSE '[]' END,
       probability, reason,
       CASE WHEN reason = 'dismissed' THEN 'rejected' ELSE 'open' END,
       at, at,
       CASE WHEN reason = 'dismissed' THEN at END,
       reason = 'dismissed',
       CASE WHEN reason = 'dismissed' THEN 'the pair is dismissed (moved from props, schema 8)'
            ELSE '' END
FROM (
    SELECT min(company, other) AS a, max(company, other) AS b, company, p, at, probability,
           reason,
           row_number() OVER (PARTITION BY min(company, other), max(company, other),
                                           reason = 'dismissed'
                              ORDER BY at DESC, ord DESC) AS rn
    FROM (
        SELECT n.id AS company, e.value AS p, e.id AS ord,
               lower(json_extract(e.value, '$.company')) AS other,
               CASE WHEN json_type(e.value, '$.at') IN ('integer', 'real')
                    THEN CAST(json_extract(e.value, '$.at') AS INTEGER)
                    ELSE CAST(strftime('%s', 'now') AS INTEGER) END AS at,
               CASE WHEN json_type(e.value, '$.probability') IN ('integer', 'real')
                    THEN max(0.0, min(1.0, CAST(json_extract(e.value, '$.probability') AS REAL)))
                    ELSE 0.0 END AS probability,
               CASE json_extract(e.value, '$.reason') WHEN 'conflict' THEN 'conflict'
                    WHEN 'dismissed' THEN 'dismissed' ELSE 'review_band' END AS reason
        FROM nodes n, json_each(n.props_json, '$.merge_proposals') e
        WHERE n.kind = 'company' AND json_type(n.props_json, '$.merge_proposals') = 'array'
          AND e.type = 'object' AND json_type(e.value, '$.company') = 'text'
    )
    WHERE other GLOB '[0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f]-[0-9a-f][0-9a-f][0-9a-f][0-9a-f]-[0-9a-f][0-9a-f][0-9a-f][0-9a-f]-[0-9a-f][0-9a-f][0-9a-f][0-9a-f]-[0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f]'
      AND other <> company
      AND EXISTS (SELECT 1 FROM nodes c WHERE c.id = other AND c.kind = 'company')
)
WHERE rn = 1;

INSERT INTO company_proposals (id, kind, company_id, candidate, detail_json, evidence_json,
                               probability, reason, state, created_at, updated_at)
SELECT lower(hex(randomblob(4))) || '-' || lower(hex(randomblob(2))) || '-4'
           || substr(lower(hex(randomblob(2))), 2) || '-'
           || substr('89ab', 1 + (abs(random()) % 4), 1)
           || substr(lower(hex(randomblob(2))), 2) || '-' || lower(hex(randomblob(6))),
       'registry_record', company, candidate,
       json_object('registry', json_extract(p, '$.registry'),
                   'record', json_extract(p, '$.record'),
                   'name', json_extract(p, '$.name'),
                   'from_props', company,
                   'props_reason', json_extract(p, '$.reason')),
       json_array(json_object('registry', json_extract(p, '$.registry'),
                              'record', json_extract(p, '$.record'),
                              'name', json_extract(p, '$.name'))),
       probability, reason, 'open', at, at
FROM (
    SELECT company, candidate, p, at, probability, reason,
           row_number() OVER (PARTITION BY company, candidate ORDER BY at DESC, ord DESC) AS rn
    FROM (
        SELECT n.id AS company, e.value AS p, e.id AS ord,
               json_extract(e.value, '$.registry') || ':' || json_extract(e.value, '$.record')
                   AS candidate,
               CASE WHEN json_type(e.value, '$.at') IN ('integer', 'real')
                    THEN CAST(json_extract(e.value, '$.at') AS INTEGER)
                    ELSE CAST(strftime('%s', 'now') AS INTEGER) END AS at,
               CASE WHEN json_type(e.value, '$.probability') IN ('integer', 'real')
                    THEN max(0.0, min(1.0, CAST(json_extract(e.value, '$.probability') AS REAL)))
                    ELSE 0.0 END AS probability,
               CASE json_extract(e.value, '$.reason') WHEN 'conflict' THEN 'conflict'
                    ELSE 'review_band' END AS reason
        FROM nodes n, json_each(n.props_json, '$.registry_proposals') e
        WHERE n.kind = 'company' AND json_type(n.props_json, '$.registry_proposals') = 'array'
          AND e.type = 'object'
          AND json_type(e.value, '$.registry') = 'text' AND json_extract(e.value, '$.registry') <> ''
          AND json_type(e.value, '$.record') = 'text' AND json_extract(e.value, '$.record') <> ''
    )
    WHERE length(candidate) <= 512
)
WHERE rn = 1;

-- The lists leave the props; the job no longer writes them.
UPDATE nodes
SET props_json = json_remove(props_json, '$.merge_proposals', '$.registry_proposals'),
    updated_at = CAST(strftime('%s', 'now') AS INTEGER)
WHERE kind = 'company'
  AND (json_type(props_json, '$.merge_proposals') IS NOT NULL
       OR json_type(props_json, '$.registry_proposals') IS NOT NULL);
