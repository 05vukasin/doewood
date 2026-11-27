//! Purges on Postgres (issue #168; `doewood_model::purge`). Each batch is one transaction in
//! READ COMMITTED. Before a batch decides that a record, a Location node or a company goes,
//! it locks the rows (`FOR UPDATE`, in key order) and then checks them in a statement of its
//! own, so the check sees every write that held those rows and no later write can slip in:
//! `upsert_location` holds a share lock on the record while it writes the location row, and a
//! new edge holds one on its nodes (the foreign key) until it commits. A deadlock with such a
//! write aborts one of the two; the caller retries the batch.

use std::collections::BTreeSet;

use deadpool_postgres::{Pool, Transaction};
use doewood_model::{Blake3, Purged, Result, SourceKey, purge_limit};
use uuid::Uuid;

use crate::backend::{client, pg};
use crate::cached::Cached as _;

/// How many rounds a batch follows (documents to their ended locations, to the documents
/// those held, ...) before it leaves the rest to the next batch's examination.
const ROUNDS_MAX: usize = 8;

/// The live `cites` and `amends` edges into the purged documents `$1` from documents that
/// stay go back to the pending node of their number (`props.key`), made as ingest makes it
/// when it is missing, as ingest wrote them before the document arrived. Returns the pending
/// nodes used, which stay.
const REPEND: &str = "WITH moved AS (SELECT DISTINCT ON (e.src, e.kind, e.props->>'key', \
     e.valid_from) e.src, e.kind, e.props, e.evidence, e.confidence, e.valid_from, e.valid_to, \
     e.props->>'key' AS key FROM edges e \
     WHERE e.dst = ANY($1) AND e.kind IN ('cites', 'amends') AND e.src <> ALL($1) \
     AND e.state NOT IN ('retracted', 'superseded') AND jsonb_typeof(e.props->'key') = 'string' \
     ORDER BY e.src, e.kind, e.props->>'key', e.valid_from), \
     pend AS (INSERT INTO nodes (id, kind, key, props) \
     SELECT DISTINCT ON (key) gen_random_uuid(), 'document', 'ref:' || key, \
     jsonb_build_object('pending', true, 'key', key, 'raw', props->'raw', 'kind', props->'kind') \
     FROM moved ORDER BY key \
     ON CONFLICT ON CONSTRAINT nodes_kind_key DO UPDATE SET kind = EXCLUDED.kind \
     RETURNING id, key), \
     edge AS (INSERT INTO edges (id, src, dst, kind, props, evidence, confidence, state, \
     valid_from, valid_to, source) \
     SELECT gen_random_uuid(), m.src, p.id, m.kind, m.props || '{\"pending\": true}', \
     m.evidence, m.confidence, 'active', m.valid_from, m.valid_to, 'document' \
     FROM moved m JOIN pend p ON p.key = 'ref:' || m.key \
     ON CONFLICT ON CONSTRAINT edges_identity DO UPDATE SET props = EXCLUDED.props, \
     evidence = EXCLUDED.evidence, confidence = EXCLUDED.confidence, state = EXCLUDED.state, \
     valid_to = EXCLUDED.valid_to, updated_at = now()) \
     SELECT id FROM pend";

/// [`doewood_model::PurgeStore::purge_source_locations`].
pub(crate) async fn source_locations(
    pool: &Pool,
    source: &SourceKey,
    limit: u32,
) -> Result<Purged> {
    let limit = purge_limit(limit);
    let mut c = client(pool).await?;
    let tx = c.transaction().await.map_err(pg)?;
    // Uncached: the prefix range needs the actual prefix in the plan (see `cached.rs`).
    let rows = tx
        .rows_uncached(
            "DELETE FROM locations WHERE node_id IN (SELECT node_id FROM locations \
             WHERE endpoint = $1 AND bucket = $2 AND starts_with(object_key COLLATE \"C\", $3) \
             ORDER BY object_key COLLATE \"C\" LIMIT $4) RETURNING node_id, blake3",
            &[
                &source.endpoint(),
                &source.bucket(),
                &source.prefix(),
                &i64::from(limit),
            ],
        )
        .await?;
    let mut out = Purged {
        examined: rows.len() as u64,
        next: (rows.len() as u64 >= u64::from(limit)).then(String::new),
        ..Purged::default()
    };
    let (mut docs, mut locations) = (BTreeSet::new(), BTreeSet::new());
    for r in &rows {
        locations.insert(r.try_get::<_, Uuid>(0).map_err(pg)?);
        docs.insert(Blake3::from_slice(r.try_get::<_, &[u8]>(1).map_err(pg)?)?);
    }
    purge(&tx, docs, locations, &mut out).await?;
    tx.commit().await.map_err(pg)?;
    Ok(out)
}

/// [`doewood_model::PurgeStore::purge_ended_locations`].
pub(crate) async fn ended_locations(
    pool: &Pool,
    source: Option<&SourceKey>,
    after: Option<&str>,
    limit: u32,
) -> Result<Purged> {
    let limit = purge_limit(limit);
    let mut c = client(pool).await?;
    let tx = c.transaction().await.map_err(pg)?;
    // Location nodes in the order of `nodes_kind_key`, whatever the collation; a source's are
    // picked by their props (`upsert_location` writes them), so the scan covers every
    // location, a batch at a time.
    let rows = tx
        .rows(
            "SELECT n.id, n.key, EXISTS (SELECT 1 FROM locations l WHERE l.node_id = n.id), \
             ($3::text IS NULL OR coalesce(n.props->>'endpoint' = $3 \
             AND n.props->>'bucket' = $4 AND starts_with(n.props->>'key', $5), false)) \
             FROM nodes n WHERE n.kind = 'location' AND n.key > $1 ORDER BY n.key LIMIT $2",
            &[
                &after.unwrap_or(""),
                &i64::from(limit),
                &source.map(SourceKey::endpoint),
                &source.map(SourceKey::bucket),
                &source.map(SourceKey::prefix),
            ],
        )
        .await?;
    let mut out = Purged {
        examined: rows.len() as u64,
        ..Purged::default()
    };
    let mut ended = BTreeSet::new();
    let mut last = None;
    for r in &rows {
        let (id, key, live, mine): (Uuid, String, bool, bool) = (
            r.try_get(0).map_err(pg)?,
            r.try_get(1).map_err(pg)?,
            r.try_get(2).map_err(pg)?,
            r.try_get(3).map_err(pg)?,
        );
        if mine && !live {
            ended.insert(id);
        }
        last = Some(key);
    }
    out.next = last.filter(|_| out.examined >= u64::from(limit));
    purge(&tx, BTreeSet::new(), ended, &mut out).await?;
    tx.commit().await.map_err(pg)?;
    Ok(out)
}

/// [`doewood_model::PurgeStore::purge_orphan_records`].
pub(crate) async fn orphan_records(pool: &Pool, after: Option<&str>, limit: u32) -> Result<Purged> {
    let limit = purge_limit(limit);
    let after = after.map(str::parse::<Blake3>).transpose()?;
    let mut c = client(pool).await?;
    let tx = c.transaction().await.map_err(pg)?;
    let rows = tx
        .rows(
            "SELECT r.blake3, EXISTS (SELECT 1 FROM locations l WHERE l.blake3 = r.blake3) \
             FROM records r WHERE r.blake3 > $1 ORDER BY r.blake3 LIMIT $2",
            &[
                &after.as_ref().map_or(&[][..], |h| h.as_bytes().as_slice()),
                &i64::from(limit),
            ],
        )
        .await?;
    let mut out = Purged {
        examined: rows.len() as u64,
        ..Purged::default()
    };
    let mut docs = BTreeSet::new();
    let mut last = None;
    for r in &rows {
        let h = Blake3::from_slice(r.try_get::<_, &[u8]>(0).map_err(pg)?)?;
        if !r.try_get::<_, bool>(1).map_err(pg)? {
            docs.insert(h);
        }
        last = Some(h);
    }
    out.next = last
        .filter(|_| out.examined >= u64::from(limit))
        .map(|h| h.to_hex());
    purge(&tx, docs, BTreeSet::new(), &mut out).await?;
    tx.commit().await.map_err(pg)?;
    Ok(out)
}

/// Locks the nodes `ids` in id order.
async fn lock_nodes(tx: &Transaction<'_>, ids: &[Uuid]) -> Result<()> {
    tx.exec(
        "SELECT 1 FROM nodes WHERE id = ANY($1) ORDER BY id FOR UPDATE",
        &[&ids],
    )
    .await?;
    Ok(())
}

async fn uuids(tx: &Transaction<'_>, sql: &str, ids: &[Uuid]) -> Result<Vec<Uuid>> {
    tx.rows(sql, &[&ids])
        .await?
        .iter()
        .map(|r| r.try_get(0).map_err(pg))
        .collect()
}

/// Purges the documents `docs` that no live location holds and the Location nodes
/// `locations` without a live row, then what they leave behind.
async fn purge(
    tx: &Transaction<'_>,
    mut docs: BTreeSet<Blake3>,
    mut locations: BTreeSet<Uuid>,
    out: &mut Purged,
) -> Result<()> {
    let mut companies = BTreeSet::new();
    let mut done = BTreeSet::new();
    for _ in 0..ROUNDS_MAX {
        if docs.is_empty() && locations.is_empty() {
            break;
        }
        let ids: Vec<Uuid> = std::mem::take(&mut locations).into_iter().collect();
        let held = purge_locations(tx, &ids, out).await?;
        docs.extend(held.into_iter().filter(|h| !done.contains(h)));
        let now: Vec<Blake3> = std::mem::take(&mut docs).into_iter().collect();
        done.extend(now.iter().copied());
        let (c, l) = purge_documents(tx, &now, out).await?;
        companies.extend(c);
        locations.extend(l);
    }
    purge_companies(tx, companies.into_iter().collect(), out).await
}

/// Deletes the Location nodes among `ids` without a live row. Returns the documents that had a
/// `located_at` edge to them.
async fn purge_locations(
    tx: &Transaction<'_>,
    ids: &[Uuid],
    out: &mut Purged,
) -> Result<Vec<Blake3>> {
    if ids.is_empty() {
        return Ok(Vec::new());
    }
    lock_nodes(tx, ids).await?;
    let ended = uuids(
        tx,
        "SELECT n.id FROM nodes n WHERE n.id = ANY($1) AND n.kind = 'location' \
         AND NOT EXISTS (SELECT 1 FROM locations l WHERE l.node_id = n.id)",
        ids,
    )
    .await?;
    if ended.is_empty() {
        return Ok(Vec::new());
    }
    let mut docs = Vec::new();
    for r in tx
        .rows(
            "SELECT DISTINCT d.key FROM edges e JOIN nodes d ON d.id = e.src \
             WHERE e.dst = ANY($1) AND e.kind = 'located_at' AND d.kind = 'document'",
            &[&ended],
        )
        .await?
    {
        if let Ok(h) = r.try_get::<_, &str>(0).map_err(pg)?.parse::<Blake3>() {
            docs.push(h);
        }
    }
    out.locations += tx
        .exec("DELETE FROM nodes WHERE id = ANY($1)", &[&ended])
        .await?;
    Ok(docs)
}

/// Deletes the documents among `docs` that no live location holds: the record with its rows,
/// the Document node with its edges. Returns the companies and Location nodes they named.
async fn purge_documents(
    tx: &Transaction<'_>,
    docs: &[Blake3],
    out: &mut Purged,
) -> Result<(Vec<Uuid>, Vec<Uuid>)> {
    if docs.is_empty() {
        return Ok((Vec::new(), Vec::new()));
    }
    let hashes: Vec<&[u8]> = docs.iter().map(|h| h.as_bytes().as_slice()).collect();
    let hexes: Vec<String> = docs.iter().map(Blake3::to_hex).collect();
    // Records, then their Document nodes, each in key order; then the check, in a statement
    // of its own, so it sees every location written by a statement that held the record.
    tx.exec(
        "SELECT 1 FROM records WHERE blake3 = ANY($1) ORDER BY blake3 FOR UPDATE",
        &[&hashes],
    )
    .await?;
    tx.exec(
        "SELECT 1 FROM nodes WHERE kind = 'document' AND key = ANY($1) ORDER BY id FOR UPDATE",
        &[&hexes],
    )
    .await?;
    let gone: Vec<Vec<u8>> = tx
        .rows(
            "SELECT h FROM unnest($1::bytea[]) h \
             WHERE NOT EXISTS (SELECT 1 FROM locations l WHERE l.blake3 = h)",
            &[&hashes],
        )
        .await?
        .iter()
        .map(|r| r.try_get(0).map_err(pg))
        .collect::<Result<_>>()?;
    if gone.is_empty() {
        return Ok((Vec::new(), Vec::new()));
    }
    let gone_hex: Vec<String> = gone
        .iter()
        .map(|h| Blake3::from_slice(h).map(|h| h.to_hex()))
        .collect::<Result<_>>()?;
    let ids: Vec<Uuid> = tx
        .rows(
            "SELECT id FROM nodes WHERE kind = 'document' AND key = ANY($1)",
            &[&gone_hex],
        )
        .await?
        .iter()
        .map(|r| r.try_get(0).map_err(pg))
        .collect::<Result<_>>()?;
    let companies = uuids(
        tx,
        "SELECT DISTINCT dst FROM edges WHERE src = ANY($1) AND kind = 'party'",
        &ids,
    )
    .await?;
    let locations = uuids(
        tx,
        "SELECT DISTINCT dst FROM edges WHERE src = ANY($1) AND kind = 'located_at'",
        &ids,
    )
    .await?;
    let mut pending: BTreeSet<Uuid> = uuids(
        tx,
        "SELECT DISTINCT e.dst FROM edges e JOIN nodes p ON p.id = e.dst \
         WHERE e.src = ANY($1) AND e.kind IN ('cites', 'amends') AND p.kind = 'document' \
         AND starts_with(p.key, 'ref:')",
        &ids,
    )
    .await?
    .into_iter()
    .collect();
    for kept in uuids(tx, REPEND, &ids).await? {
        pending.remove(&kept);
    }
    tx.exec("DELETE FROM nodes WHERE id = ANY($1)", &[&ids])
        .await?;
    let deleted = tx
        .rows(
            "DELETE FROM records WHERE blake3 = ANY($1) RETURNING blake3",
            &[&gone],
        )
        .await?;
    for r in &deleted {
        out.hashes
            .push(Blake3::from_slice(r.try_get::<_, &[u8]>(0).map_err(pg)?)?);
    }
    out.records += deleted.len() as u64;
    out.other_documents += (ids.len() as u64).saturating_sub(deleted.len() as u64);
    if !pending.is_empty() {
        let pending: Vec<Uuid> = pending.into_iter().collect();
        lock_nodes(tx, &pending).await?;
        out.other_documents += tx
            .exec(
                "DELETE FROM nodes n WHERE n.id = ANY($1) AND n.kind = 'document' \
                 AND NOT EXISTS (SELECT 1 FROM edges e WHERE e.dst = n.id) \
                 AND NOT EXISTS (SELECT 1 FROM edges e WHERE e.src = n.id)",
                &[&pending],
            )
            .await?;
    }
    Ok((companies, locations))
}

/// Deletes the companies among `ids` that no document names (no `party` edge in any state)
/// and that no registry created, then the same for the companies merged into those.
async fn purge_companies(tx: &Transaction<'_>, mut ids: Vec<Uuid>, out: &mut Purged) -> Result<()> {
    let mut seen = BTreeSet::new();
    for _ in 0..doewood_model::MERGE_CHAIN_MAX {
        ids.retain(|id| seen.insert(*id));
        if ids.is_empty() {
            break;
        }
        lock_nodes(tx, &ids).await?;
        let gone = uuids(
            tx,
            "SELECT n.id FROM nodes n WHERE n.id = ANY($1) AND n.kind = 'company' \
             AND (n.props->>'origin') IS DISTINCT FROM 'registry' \
             AND NOT EXISTS (SELECT 1 FROM edges e WHERE e.dst = n.id AND e.kind = 'party')",
            &ids,
        )
        .await?;
        if gone.is_empty() {
            break;
        }
        // Their husks: the companies the merge log says were merged into them, and that
        // still say so (`merged_into` in their props).
        let husks = uuids(
            tx,
            "SELECT DISTINCT m.absorbed_id FROM company_merges m \
             JOIN nodes n ON n.id = m.absorbed_id \
             WHERE m.survivor_id = ANY($1) AND m.op = 'merge' \
             AND n.props->>'merged_into' = m.survivor_id::text",
            &gone,
        )
        .await?;
        out.companies += tx
            .exec("DELETE FROM nodes WHERE id = ANY($1)", &[&gone])
            .await?;
        // A merge proposal naming one as the candidate goes too (their own go with the node).
        let names: Vec<String> = gone.iter().map(Uuid::to_string).collect();
        tx.exec(
            "DELETE FROM company_proposals WHERE kind = 'merge' AND candidate = ANY($1)",
            &[&names],
        )
        .await?;
        ids = husks;
    }
    Ok(())
}
