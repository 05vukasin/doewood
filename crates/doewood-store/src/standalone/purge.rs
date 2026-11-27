//! Purges on SQLite (issue #168; `doewood_model::purge`). Each batch is one transaction on
//! the writer, so no ingest write runs between a batch's checks and its deletes.

use std::collections::BTreeSet;

use doewood_model::{
    Blake3, EdgeState, PENDING_REF_PREFIX, Purged, Result, SourceKey, purge_limit,
};
use rusqlite::{Connection, OptionalExtension as _, Transaction, params};
use serde_json::{Value, json};
use uuid::Uuid;

use super::sql;
use crate::store::now;

/// How many rounds a batch follows (documents to their ended locations, to the documents
/// those held, ...) before it leaves the rest to the next batch's examination.
const ROUNDS_MAX: usize = 8;

/// The smallest string above every string that starts with `prefix` (code point order is
/// SQLite's byte order for UTF-8), or `None` when there is none (an empty prefix).
pub(crate) fn prefix_end(prefix: &str) -> Option<String> {
    let mut chars: Vec<char> = prefix.chars().collect();
    while let Some(last) = chars.pop() {
        let mut next = u32::from(last) + 1;
        if (0xD800..0xE000).contains(&next) {
            next = 0xE000;
        }
        if let Some(c) = char::from_u32(next) {
            chars.push(c);
            return Some(chars.into_iter().collect());
        }
    }
    None
}

/// The start of the node key of every location under `source` (a location's node key is the
/// JSON array of endpoint, bucket and key; JSON escapes one character at a time, so a prefix
/// of the key is a prefix of the node key).
pub(crate) fn node_key_prefix(source: &SourceKey) -> String {
    let full = Value::from(vec![source.endpoint(), source.bucket(), source.prefix()]).to_string();
    full.strip_suffix("\"]").unwrap_or(&full).to_owned()
}

/// [`doewood_model::PurgeStore::purge_source_locations`].
pub(super) fn source_locations(
    conn: &mut Connection,
    source: &SourceKey,
    limit: u32,
) -> Result<Purged> {
    let limit = purge_limit(limit);
    let tx = conn.transaction().map_err(sql)?;
    let end = prefix_end(source.prefix());
    let rows: Vec<(String, Vec<u8>)> = {
        let mut stmt = tx
            .prepare_cached(
                "SELECT node_id, blake3 FROM locations WHERE endpoint = ?1 AND bucket = ?2 \
                 AND object_key >= ?3 AND (?4 IS NULL OR object_key < ?4) \
                 ORDER BY object_key LIMIT ?5",
            )
            .map_err(sql)?;
        stmt.query_map(
            params![
                source.endpoint(),
                source.bucket(),
                source.prefix(),
                end,
                limit
            ],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .map_err(sql)?
        .collect::<rusqlite::Result<_>>()
        .map_err(sql)?
    };
    let mut out = Purged {
        examined: rows.len() as u64,
        next: (rows.len() as u64 >= u64::from(limit)).then(String::new),
        ..Purged::default()
    };
    let mut docs = BTreeSet::new();
    for (_, h) in &rows {
        docs.insert(Blake3::from_slice(h)?);
    }
    let locations: BTreeSet<String> = rows.into_iter().map(|(id, _)| id).collect();
    // The rows first: the nodes are then ended locations, deleted with the rest.
    for id in &locations {
        tx.prepare_cached("DELETE FROM locations WHERE node_id = ?1")
            .and_then(|mut c| c.execute([id]))
            .map_err(sql)?;
    }
    purge(&tx, docs, locations, &mut out)?;
    tx.commit().map_err(sql)?;
    Ok(out)
}

/// [`doewood_model::PurgeStore::purge_ended_locations`].
pub(super) fn ended_locations(
    conn: &mut Connection,
    source: Option<&SourceKey>,
    after: Option<&str>,
    limit: u32,
) -> Result<Purged> {
    let limit = purge_limit(limit);
    let tx = conn.transaction().map_err(sql)?;
    let (start, end) = match source {
        Some(s) => {
            let p = node_key_prefix(s);
            let end = prefix_end(&p);
            (after.map_or(p, str::to_owned), end)
        }
        None => (after.unwrap_or("").to_owned(), None),
    };
    let rows: Vec<(String, String, bool)> = {
        let mut stmt = tx
            .prepare_cached(
                "SELECT n.id, n.key, EXISTS (SELECT 1 FROM locations l WHERE l.node_id = n.id) \
                 FROM nodes n WHERE n.kind = 'location' AND n.key > ?1 \
                 AND (?2 IS NULL OR n.key < ?2) ORDER BY n.key LIMIT ?3",
            )
            .map_err(sql)?;
        stmt.query_map(params![start, end, limit], |r| {
            Ok((r.get(0)?, r.get(1)?, r.get(2)?))
        })
        .map_err(sql)?
        .collect::<rusqlite::Result<_>>()
        .map_err(sql)?
    };
    let mut out = Purged {
        examined: rows.len() as u64,
        next: (rows.len() as u64 >= u64::from(limit))
            .then(|| rows.last().map(|r| r.1.clone()).unwrap_or_default()),
        ..Purged::default()
    };
    let ended: BTreeSet<String> = rows
        .into_iter()
        .filter(|(_, _, live)| !live)
        .map(|(id, _, _)| id)
        .collect();
    purge(&tx, BTreeSet::new(), ended, &mut out)?;
    tx.commit().map_err(sql)?;
    Ok(out)
}

/// [`doewood_model::PurgeStore::purge_orphan_records`].
pub(super) fn orphan_records(
    conn: &mut Connection,
    after: Option<&str>,
    limit: u32,
) -> Result<Purged> {
    let limit = purge_limit(limit);
    let after = after.map(str::parse::<Blake3>).transpose()?;
    let tx = conn.transaction().map_err(sql)?;
    let rows: Vec<(Vec<u8>, bool)> = {
        let mut stmt = tx
            .prepare_cached(
                "SELECT r.blake3, EXISTS (SELECT 1 FROM locations l WHERE l.blake3 = r.blake3) \
                 FROM records r WHERE r.blake3 > coalesce(?1, x'') ORDER BY r.blake3 LIMIT ?2",
            )
            .map_err(sql)?;
        stmt.query_map(
            params![after.as_ref().map(|h| h.as_bytes().as_slice()), limit],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .map_err(sql)?
        .collect::<rusqlite::Result<_>>()
        .map_err(sql)?
    };
    let mut out = Purged {
        examined: rows.len() as u64,
        ..Purged::default()
    };
    let mut docs = BTreeSet::new();
    let mut last = None;
    for (h, live) in rows {
        let h = Blake3::from_slice(&h)?;
        last = Some(h);
        if !live {
            docs.insert(h);
        }
    }
    out.next = last
        .filter(|_| out.examined >= u64::from(limit))
        .map(|h| h.to_hex());
    purge(&tx, docs, BTreeSet::new(), &mut out)?;
    tx.commit().map_err(sql)?;
    Ok(out)
}

fn strings(tx: &Transaction<'_>, sql_text: &str, arg: &str) -> Result<Vec<String>> {
    let mut stmt = tx.prepare_cached(sql_text).map_err(sql)?;
    stmt.query_map([arg], |r| r.get(0))
        .map_err(sql)?
        .collect::<rusqlite::Result<_>>()
        .map_err(sql)
}

/// Purges the documents `docs` that no live location holds and the Location nodes
/// `locations` that have no live row, then what they leave behind (see the crate docs of
/// `doewood_model::purge`).
fn purge(
    tx: &Transaction<'_>,
    mut docs: BTreeSet<Blake3>,
    mut locations: BTreeSet<String>,
    out: &mut Purged,
) -> Result<()> {
    let mut companies = BTreeSet::new();
    let mut done = BTreeSet::new();
    for _ in 0..ROUNDS_MAX {
        if docs.is_empty() && locations.is_empty() {
            break;
        }
        let ended = purge_locations(tx, &std::mem::take(&mut locations), out)?;
        docs.extend(ended.into_iter().filter(|h| !done.contains(h)));
        let docs_now = std::mem::take(&mut docs);
        done.extend(docs_now.iter().copied());
        let left = purge_documents(tx, &docs_now, out)?;
        companies.extend(left.companies);
        locations.extend(left.locations);
    }
    purge_companies(tx, companies, out)
}

/// What purged documents leave to look at.
#[derive(Default)]
struct Left {
    companies: BTreeSet<String>,
    locations: BTreeSet<String>,
}

/// Deletes the Location nodes among `ids` without a live row. Returns the documents that had a
/// `located_at` edge to them.
fn purge_locations(
    tx: &Transaction<'_>,
    ids: &BTreeSet<String>,
    out: &mut Purged,
) -> Result<BTreeSet<Blake3>> {
    let mut docs = BTreeSet::new();
    for id in ids {
        let live: bool = tx
            .prepare_cached("SELECT EXISTS (SELECT 1 FROM locations WHERE node_id = ?1)")
            .and_then(|mut c| c.query_row([id], |r| r.get(0)))
            .map_err(sql)?;
        if live {
            continue;
        }
        for key in strings(
            tx,
            "SELECT d.key FROM edges e JOIN nodes d ON d.id = e.src \
             WHERE e.dst = ?1 AND e.kind = 'located_at' AND d.kind = 'document'",
            id,
        )? {
            if let Ok(h) = key.parse::<Blake3>() {
                docs.insert(h);
            }
        }
        let n = tx
            .prepare_cached("DELETE FROM nodes WHERE id = ?1 AND kind = 'location'")
            .and_then(|mut c| c.execute([id]))
            .map_err(sql)?;
        out.locations += n as u64;
    }
    Ok(docs)
}

/// Deletes the documents among `docs` that no live location holds: the record and its rows,
/// the Document node and its edges. A live document's reference to one goes back to the
/// pending node of its number; pending nodes it cited are deleted once nothing points at them.
fn purge_documents(
    tx: &Transaction<'_>,
    docs: &BTreeSet<Blake3>,
    out: &mut Purged,
) -> Result<Left> {
    let mut left = Left::default();
    // The documents to purge first, so a reference between two of them is not moved.
    let mut gone: Vec<(Blake3, Option<String>)> = Vec::new();
    for h in docs {
        let live: bool = tx
            .prepare_cached("SELECT EXISTS (SELECT 1 FROM locations WHERE blake3 = ?1)")
            .and_then(|mut c| c.query_row([h.as_bytes().as_slice()], |r| r.get(0)))
            .map_err(sql)?;
        if live {
            continue;
        }
        let id: Option<String> = tx
            .prepare_cached("SELECT id FROM nodes WHERE kind = 'document' AND key = ?1")
            .and_then(|mut c| c.query_row([h.to_hex()], |r| r.get(0)).optional())
            .map_err(sql)?;
        gone.push((*h, id));
    }
    let ids: BTreeSet<String> = gone.iter().filter_map(|(_, id)| id.clone()).collect();
    let mut pending = BTreeSet::new();
    for id in &ids {
        left.companies.extend(strings(
            tx,
            "SELECT dst FROM edges WHERE src = ?1 AND kind = 'party'",
            id,
        )?);
        left.locations.extend(strings(
            tx,
            "SELECT dst FROM edges WHERE src = ?1 AND kind = 'located_at'",
            id,
        )?);
        pending.extend(strings(
            tx,
            "SELECT e.dst FROM edges e JOIN nodes p ON p.id = e.dst \
             WHERE e.src = ?1 AND e.kind IN ('cites', 'amends') AND p.kind = 'document' \
             AND substr(p.key, 1, 4) = 'ref:'",
            id,
        )?);
        repend(tx, id, &ids, &mut pending)?;
    }
    for (h, id) in &gone {
        if let Some(id) = id {
            tx.prepare_cached("DELETE FROM nodes WHERE id = ?1")
                .and_then(|mut c| c.execute([id]))
                .map_err(sql)?;
        }
        let n = tx
            .prepare_cached("DELETE FROM records WHERE blake3 = ?1")
            .and_then(|mut c| c.execute([h.as_bytes().as_slice()]))
            .map_err(sql)?;
        if n > 0 {
            out.records += 1;
            out.hashes.push(*h);
        } else if id.is_some() {
            out.other_documents += 1;
        }
    }
    for id in &pending {
        let n = tx
            .prepare_cached(
                "DELETE FROM nodes WHERE id = ?1 AND kind = 'document' \
                 AND NOT EXISTS (SELECT 1 FROM edges WHERE dst = ?1) \
                 AND NOT EXISTS (SELECT 1 FROM edges WHERE src = ?1)",
            )
            .and_then(|mut c| c.execute([id]))
            .map_err(sql)?;
        out.other_documents += n as u64;
    }
    Ok(left)
}

/// Moves the live `cites` and `amends` edges into document `id` from documents that stay
/// back to the pending node of the cited number (`props.key`), as ingest wrote them before the
/// document arrived. The moved edges are left for the node's delete to remove.
fn repend(
    tx: &Transaction<'_>,
    id: &str,
    gone: &BTreeSet<String>,
    pending: &mut BTreeSet<String>,
) -> Result<()> {
    type Incoming = (
        String,
        String,
        String,
        String,
        f64,
        Option<i64>,
        Option<i64>,
    );
    let rows: Vec<Incoming> = {
        let mut stmt = tx
            .prepare_cached(
                "SELECT e.src, e.kind, e.props_json, e.evidence_json, e.confidence, \
                 e.valid_from, e.valid_to FROM edges e \
                 WHERE e.dst = ?1 AND e.kind IN ('cites', 'amends') \
                 AND e.state NOT IN ('retracted', 'superseded')",
            )
            .map_err(sql)?;
        stmt.query_map([id], |r| {
            Ok((
                r.get(0)?,
                r.get(1)?,
                r.get(2)?,
                r.get(3)?,
                r.get(4)?,
                r.get(5)?,
                r.get(6)?,
            ))
        })
        .map_err(sql)?
        .collect::<rusqlite::Result<_>>()
        .map_err(sql)?
    };
    for (src, kind, props, evidence, confidence, from, to) in rows {
        if gone.contains(&src) {
            continue;
        }
        let mut props: Value = super::json(&props)?;
        let Some(key) = props.get("key").and_then(Value::as_str).map(str::to_owned) else {
            continue;
        };
        props["pending"] = Value::Bool(true);
        let node = pending_node(tx, &key, &props)?;
        pending.remove(&node);
        tx.prepare_cached(
            "INSERT INTO edges (id, src, dst, kind, props_json, evidence_json, confidence, \
             state, valid_from, valid_to, source, created_at, updated_at) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, 'document', ?11, ?11) \
             ON CONFLICT (src, dst, kind, source, ifnull(valid_from, -2147483648)) \
             DO UPDATE SET props_json = excluded.props_json, \
             evidence_json = excluded.evidence_json, confidence = excluded.confidence, \
             state = excluded.state, valid_to = excluded.valid_to, \
             updated_at = excluded.updated_at",
        )
        .and_then(|mut c| {
            c.execute(params![
                Uuid::now_v7().to_string(),
                src,
                node,
                kind,
                props.to_string(),
                evidence,
                confidence,
                EdgeState::Active.as_str(),
                from,
                to,
                now()
            ])
        })
        .map_err(sql)?;
    }
    Ok(())
}

/// The pending node of number `key`, made as ingest makes it when it is missing.
fn pending_node(tx: &Transaction<'_>, key: &str, props: &Value) -> Result<String> {
    let node_key = format!("{PENDING_REF_PREFIX}{key}");
    let made =
        json!({"pending": true, "key": key, "raw": props.get("raw"), "kind": props.get("kind")});
    tx.prepare_cached(
        "INSERT INTO nodes (id, kind, key, props_json, created_at, updated_at) \
         VALUES (?1, 'document', ?2, ?3, ?4, ?4) ON CONFLICT (kind, key) DO NOTHING",
    )
    .and_then(|mut c| {
        c.execute(params![
            Uuid::now_v7().to_string(),
            node_key,
            made.to_string(),
            now()
        ])
    })
    .map_err(sql)?;
    tx.prepare_cached("SELECT id FROM nodes WHERE kind = 'document' AND key = ?1")
        .and_then(|mut c| c.query_row([node_key], |r| r.get(0)))
        .map_err(sql)
}

/// Deletes the companies among `ids` that no document names (no `party` edge in any state)
/// and that no registry created, then the same for the companies merged into those.
fn purge_companies(
    tx: &Transaction<'_>,
    mut ids: BTreeSet<String>,
    out: &mut Purged,
) -> Result<()> {
    let mut seen = BTreeSet::new();
    for _ in 0..doewood_model::MERGE_CHAIN_MAX {
        if ids.is_empty() {
            break;
        }
        let mut husks = BTreeSet::new();
        for id in &ids {
            if !seen.insert(id.clone()) {
                continue;
            }
            let gone: bool = tx
                .prepare_cached(
                    "SELECT EXISTS (SELECT 1 FROM nodes n WHERE n.id = ?1 AND n.kind = 'company' \
                     AND json_extract(n.props_json, '$.origin') IS NOT 'registry' \
                     AND NOT EXISTS (SELECT 1 FROM edges e WHERE e.dst = n.id \
                     AND e.kind = 'party'))",
                )
                .and_then(|mut c| c.query_row([id], |r| r.get(0)))
                .map_err(sql)?;
            if !gone {
                continue;
            }
            // Its husks: the companies the merge log says were merged into it, and that
            // still say so (`merged_into` in their props).
            husks.extend(strings(
                tx,
                "SELECT DISTINCT m.absorbed_id FROM company_merges m \
                 JOIN nodes n ON n.id = m.absorbed_id \
                 WHERE m.survivor_id = ?1 AND m.op = 'merge' \
                 AND json_extract(n.props_json, '$.merged_into') = m.survivor_id",
                id,
            )?);
            tx.prepare_cached("DELETE FROM nodes WHERE id = ?1")
                .and_then(|mut c| c.execute([id]))
                .map_err(sql)?;
            // A merge proposal naming it as the candidate goes too (the company's own go
            // with the node).
            tx.prepare_cached(
                "DELETE FROM company_proposals WHERE kind = 'merge' AND candidate = ?1",
            )
            .and_then(|mut c| c.execute([id]))
            .map_err(sql)?;
            out.companies += 1;
        }
        ids = husks;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prefix_ends_bound_the_prefix() {
        assert_eq!(prefix_end(""), None);
        assert_eq!(prefix_end("a/").as_deref(), Some("a0"));
        assert_eq!(prefix_end("a\u{D7FF}").as_deref(), Some("a\u{E000}"));
        assert_eq!(prefix_end("a\u{10FFFF}").as_deref(), Some("b"));
        assert_eq!(prefix_end("\u{10FFFF}"), None);
        let key = SourceKey::new("http://s3", "b\"x", "in/\"q").unwrap();
        let p = node_key_prefix(&key);
        let full = Value::from(vec!["http://s3", "b\"x", "in/\"q/z.pdf"]).to_string();
        assert!(full.starts_with(&p), "{full} {p}");
        let other = Value::from(vec!["http://s3", "b\"x", "in/\"r.pdf"]).to_string();
        assert!(!other.starts_with(&p));
    }
}
