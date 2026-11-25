//! The enrichment job's storage in standalone mode (schema 6): the registry and favicon
//! caches and company freshness, local to the instance.

use std::collections::BTreeMap;
use std::time::Duration;

use async_trait::async_trait;
use doewood_core::InstanceId;
use doewood_model::{
    CacheRow, CacheStore, DueCompany, DueReason, Enrichment, Error, FreshnessStore, IconRow,
    Result, StoredIcon, due_limit, validate_batch, validate_cache_key, validate_registry,
    validate_schedule,
};
use rusqlite::{Connection, OptionalExtension as _, TransactionBehavior, params};
use uuid::Uuid;

use super::{Standalone, backend, check_live, sql, uuid};
use crate::store::now;

/// A company's document count: documents with a live `party` edge to `n.id`.
const DOCUMENTS: &str = "(SELECT count(DISTINCT e.src) FROM edges e JOIN nodes d ON d.id = e.src \
     WHERE e.dst = n.id AND e.kind = 'party' AND d.kind = 'document' \
     AND e.state NOT IN ('retracted', 'superseded'))";

fn count(n: i64) -> Result<u64> {
    u64::try_from(n).map_err(|e| backend(format!("stored count: {e}")))
}

fn enrichment(conn: &Connection, company: Uuid) -> Result<Option<Enrichment>> {
    let id = company.to_string();
    let Some((enriched_at, documents)) = conn
        .query_row(
            "SELECT enriched_at, documents FROM company_enrichment WHERE company_id = ?1",
            params![id],
            |r| Ok((r.get::<_, i64>(0)?, r.get::<_, i64>(1)?)),
        )
        .optional()
        .map_err(sql)?
    else {
        return Ok(None);
    };
    let mut stmt = conn
        .prepare_cached(
            "SELECT registry, next_due_at FROM company_registry_due WHERE company_id = ?1",
        )
        .map_err(sql)?;
    let next_due = stmt
        .query_map(params![id], |r| Ok((r.get(0)?, r.get(1)?)))
        .map_err(sql)?
        .collect::<rusqlite::Result<BTreeMap<String, i64>>>()
        .map_err(sql)?;
    Ok(Some(Enrichment {
        company,
        enriched_at,
        documents: count(documents)?,
        next_due,
    }))
}

#[async_trait]
impl CacheStore for Standalone {
    async fn cache_get(&self, registry: &str, key: &str) -> Result<Option<CacheRow>> {
        validate_registry(registry)?;
        validate_cache_key(key)?;
        let (registry, key) = (registry.to_owned(), key.to_owned());
        self.run(move |conn| {
            let row = conn
                .query_row(
                    "SELECT body, fetched_at, expires_at FROM registry_cache \
                     WHERE registry = ?1 AND key = ?2 AND expires_at > ?3",
                    params![registry, key, now()],
                    |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
                )
                .optional()
                .map_err(sql)?;
            Ok(row.map(|(body, fetched_at, expires_at)| CacheRow {
                registry,
                key,
                body,
                fetched_at,
                expires_at,
            }))
        })
        .await
    }

    async fn cache_put(&self, row: &CacheRow) -> Result<()> {
        row.validate()?;
        let row = row.clone();
        self.run(move |conn| {
            conn.execute(
                "INSERT INTO registry_cache (registry, key, body, fetched_at, expires_at) \
                 VALUES (?1, ?2, ?3, ?4, ?5) \
                 ON CONFLICT (registry, key) DO UPDATE SET body = excluded.body, \
                 fetched_at = excluded.fetched_at, expires_at = excluded.expires_at",
                params![
                    row.registry,
                    row.key,
                    row.body,
                    row.fetched_at,
                    row.expires_at
                ],
            )
            .map_err(sql)?;
            Ok(())
        })
        .await
    }

    async fn icon_get(&self, domain: &str) -> Result<Option<IconRow>> {
        let domain = domain.to_owned();
        self.run(move |conn| {
            let row = conn
                .query_row(
                    "SELECT icon, format, fetched_at, expires_at FROM favicons \
                     WHERE domain = ?1 AND expires_at > ?2",
                    params![domain, now()],
                    |r| {
                        Ok((
                            r.get::<_, Option<Vec<u8>>>(0)?,
                            r.get::<_, Option<String>>(1)?,
                            r.get(2)?,
                            r.get(3)?,
                        ))
                    },
                )
                .optional()
                .map_err(sql)?;
            Ok(row.map(|(icon, format, fetched_at, expires_at)| IconRow {
                domain,
                icon: icon
                    .zip(format)
                    .map(|(bytes, format)| StoredIcon { format, bytes }),
                fetched_at,
                expires_at,
            }))
        })
        .await
    }

    async fn icon_put(&self, row: &IconRow) -> Result<()> {
        row.validate()?;
        let row = row.clone();
        self.run(move |conn| {
            let (bytes, format) = match &row.icon {
                Some(i) => (Some(i.bytes.as_slice()), Some(i.format.as_str())),
                None => (None, None),
            };
            conn.execute(
                "INSERT INTO favicons (domain, icon, format, fetched_at, expires_at) \
                 VALUES (?1, ?2, ?3, ?4, ?5) \
                 ON CONFLICT (domain) DO UPDATE SET icon = excluded.icon, \
                 format = excluded.format, fetched_at = excluded.fetched_at, \
                 expires_at = excluded.expires_at",
                params![row.domain, bytes, format, row.fetched_at, row.expires_at],
            )
            .map_err(sql)?;
            Ok(())
        })
        .await
    }

    async fn purge_expired(&self) -> Result<u64> {
        self.run(|conn| {
            let tx = conn.transaction().map_err(sql)?;
            let at = now();
            let mut n = 0;
            for table in ["registry_cache", "favicons"] {
                n += tx
                    .execute(
                        &format!("DELETE FROM {table} WHERE expires_at <= ?1"),
                        params![at],
                    )
                    .map_err(sql)?;
            }
            tx.commit().map_err(sql)?;
            Ok(n as u64)
        })
        .await
    }

    async fn cache_invalidate(&self, registry: &str, key_prefix: &str) -> Result<u64> {
        validate_registry(registry)?;
        validate_cache_key(key_prefix)?;
        let (registry, prefix) = (registry.to_owned(), key_prefix.to_owned());
        self.run(move |conn| {
            let n = conn
                .execute(
                    "DELETE FROM registry_cache WHERE registry = ?1 \
                     AND key >= ?2 AND substr(key, 1, length(?2)) = ?2",
                    params![registry, prefix],
                )
                .map_err(sql)?;
            Ok(n as u64)
        })
        .await
    }
}

#[async_trait]
impl FreshnessStore for Standalone {
    async fn record_enrichment(
        &self,
        company: Uuid,
        schedule: &[(String, Duration)],
    ) -> Result<Enrichment> {
        validate_schedule(schedule)?;
        let schedule = schedule.to_vec();
        self.run(move |conn| {
            let tx = conn
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(sql)?;
            // A husk is never recorded (issue #62).
            if !check_live(&tx, company)? {
                return Err(Error::NotFound(format!("no company {company}")));
            }
            let at = now();
            let id = company.to_string();
            let written = tx
                .execute(
                    &format!(
                        "INSERT INTO company_enrichment (company_id, enriched_at, documents) \
                         SELECT n.id, ?2, {DOCUMENTS} FROM nodes n \
                         WHERE n.id = ?1 AND n.kind = 'company' \
                         ON CONFLICT (company_id) DO UPDATE SET \
                         enriched_at = excluded.enriched_at, documents = excluded.documents"
                    ),
                    params![id, at],
                )
                .map_err(sql)?;
            if written == 0 {
                return Err(Error::NotFound(format!("no company {company}")));
            }
            for (registry, every) in &schedule {
                let due = at.saturating_add(i64::try_from(every.as_secs()).unwrap_or(i64::MAX));
                tx.execute(
                    "INSERT INTO company_registry_due (company_id, registry, next_due_at) \
                     VALUES (?1, ?2, ?3) ON CONFLICT (company_id, registry) DO UPDATE SET \
                     next_due_at = excluded.next_due_at",
                    params![id, registry, due],
                )
                .map_err(sql)?;
            }
            let e = enrichment(&tx, company)?
                .ok_or_else(|| backend("enrichment missing after the write".to_owned()))?;
            tx.commit().map_err(sql)?;
            Ok(e)
        })
        .await
    }

    async fn enrichment(&self, company: Uuid) -> Result<Option<Enrichment>> {
        self.run(move |conn| enrichment(conn, company)).await
    }

    async fn mark_due(&self, company: Uuid, registries: &[String]) -> Result<()> {
        for r in registries {
            validate_registry(r)?;
        }
        let registries = registries.to_vec();
        self.run(move |conn| {
            let tx = conn
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(sql)?;
            if !check_live(&tx, company)? {
                return Err(Error::NotFound(format!("no company {company}")));
            }
            let id = company.to_string();
            let (kind, enriched): (String, bool) = tx
                .query_row(
                    "SELECT n.kind, f.company_id IS NOT NULL FROM nodes n \
                     LEFT JOIN company_enrichment f ON f.company_id = n.id WHERE n.id = ?1",
                    [&id],
                    |r| Ok((r.get(0)?, r.get(1)?)),
                )
                .map_err(sql)?;
            if kind != "company" {
                return Err(Error::NotFound(format!("no company {company}")));
            }
            // Never enriched: due already.
            if enriched {
                let at = now();
                for registry in &registries {
                    tx.execute(
                        "INSERT INTO company_registry_due (company_id, registry, next_due_at) \
                         VALUES (?1, ?2, ?3) ON CONFLICT (company_id, registry) DO UPDATE SET \
                         next_due_at = min(next_due_at, excluded.next_due_at)",
                        params![id, registry, at],
                    )
                    .map_err(sql)?;
                }
            }
            tx.commit().map_err(sql)?;
            Ok(())
        })
        .await
    }

    async fn companies_due(
        &self,
        owner: &InstanceId,
        after: Option<Uuid>,
        limit: u32,
    ) -> Result<Vec<DueCompany>> {
        let owner = owner.clone();
        self.run(move |conn| {
            let at = now();
            let rows = {
                let mut stmt = conn
                    .prepare_cached(&format!(
                        "SELECT id, docs, documents FROM ( \
                         SELECT n.id AS id, {DOCUMENTS} AS docs, f.documents AS documents, \
                         f.company_id AS enriched \
                         FROM nodes n LEFT JOIN company_enrichment f ON f.company_id = n.id \
                         WHERE n.kind = 'company' AND n.id > coalesce(?2, '') \
                         AND NOT EXISTS (SELECT 1 FROM company_leases l \
                         WHERE l.company_id = n.id AND l.owner <> ?1 AND l.expires_at > ?4)) \
                         WHERE docs > 0 AND (enriched IS NULL OR docs >= 2 * max(documents, 1) \
                         OR EXISTS (SELECT 1 FROM company_registry_due r \
                         WHERE r.company_id = id AND r.next_due_at <= ?4)) \
                         ORDER BY id LIMIT ?3"
                    ))
                    .map_err(sql)?;
                stmt.query_map(
                    params![
                        owner.as_str(),
                        after.map(|a| a.to_string()),
                        i64::from(due_limit(limit)),
                        at
                    ],
                    |r| {
                        Ok((
                            r.get::<_, String>(0)?,
                            r.get::<_, i64>(1)?,
                            r.get::<_, Option<i64>>(2)?,
                        ))
                    },
                )
                .map_err(sql)?
                .collect::<rusqlite::Result<Vec<_>>>()
                .map_err(sql)?
            };
            let mut due_of = conn
                .prepare_cached(
                    "SELECT registry FROM company_registry_due \
                     WHERE company_id = ?1 AND next_due_at <= ?2 ORDER BY registry",
                )
                .map_err(sql)?;
            rows.into_iter()
                .map(|(id, docs, then)| {
                    let documents = count(docs)?;
                    let reason = match then {
                        None => DueReason::New,
                        Some(t) if documents >= 2 * count(t)?.max(1) => DueReason::Doubled,
                        Some(_) => DueReason::Scheduled,
                    };
                    let due = due_of
                        .query_map(params![id, at], |r| r.get(0))
                        .map_err(sql)?
                        .collect::<rusqlite::Result<Vec<String>>>()
                        .map_err(sql)?;
                    Ok(DueCompany {
                        company: uuid(&id)?,
                        reason,
                        documents,
                        due,
                    })
                })
                .collect()
        })
        .await
    }

    async fn mark_many_due(&self, companies: &[Uuid], registry: &str) -> Result<u64> {
        validate_registry(registry)?;
        validate_batch(companies.len())?;
        let (companies, registry) = (companies.to_vec(), registry.to_owned());
        self.run(move |conn| {
            let tx = conn.transaction().map_err(sql)?;
            let at = now();
            let mut n = 0;
            for c in &companies {
                n += tx
                    .execute(
                        "INSERT INTO company_registry_due (company_id, registry, next_due_at) \
                         SELECT f.company_id, ?2, ?3 FROM company_enrichment f \
                         WHERE f.company_id = ?1 \
                         ON CONFLICT (company_id, registry) DO UPDATE SET \
                         next_due_at = min(company_registry_due.next_due_at, excluded.next_due_at)",
                        params![c.to_string(), registry, at],
                    )
                    .map_err(sql)?;
            }
            tx.commit().map_err(sql)?;
            Ok(n as u64)
        })
        .await
    }
}
