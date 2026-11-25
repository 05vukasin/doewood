//! The enrichment job's storage over Postgres (schema 3): the registry and favicon caches and
//! company freshness, shared by every instance.

use std::collections::BTreeMap;
use std::time::Duration;

use async_trait::async_trait;
use deadpool_postgres::GenericClient;
use doewood_core::InstanceId;
use doewood_model::{
    CacheRow, CacheStore, DueCompany, DueReason, Enrichment, Error, FreshnessStore, IconRow,
    Result, StoredIcon, due_limit, validate_batch, validate_cache_key, validate_registry,
    validate_schedule,
};
use uuid::Uuid;

use crate::PgStore;
use crate::backend::{bad, client, lock_live, pg};
use crate::cached::Cached;

/// A company's document count: documents with a live `party` edge to `n.id`.
const DOCUMENTS: &str = "(SELECT count(DISTINCT e.src) FROM edges e JOIN nodes d ON d.id = e.src \
     WHERE e.dst = n.id AND e.kind = 'party' AND d.kind = 'document' \
     AND e.state NOT IN ('retracted', 'superseded'))";

/// The companies due (issue #71: the cheap tests decide first).
///
/// The inner query lists the candidates in id order: companies after the page token without
/// another instance's lease, with their enrichment row. The fence (`OFFSET 0`) keeps the
/// tests below out of it, so that they run on the candidates in id order and stop at the page
/// size; left to the planner, they ran on every company before a sort.
///
/// A company never enriched, or with a registry due, needs one live document. Any other is
/// due only when its document count doubled: first a bound from its `party` edges in
/// `edges_by_dst` alone (an index-only count, stopped at the threshold; it counts every party
/// edge, so it is never below the document count), and only past that bound the exact count,
/// stopped at the threshold too. The scalar subqueries (not `EXISTS`) are never turned into
/// hashes of the whole table, and `CASE` fixes the order of the tests. The exact count is
/// taken for the returned rows only. `$1` is the caller, `$2` the page token, `$3` the page
/// size.
pub(crate) const COMPANIES_DUE: &str = "SELECT s.id, \
     (SELECT count(DISTINCT e.src) FROM edges e JOIN nodes d ON d.id = e.src \
     WHERE e.dst = s.id AND e.kind = 'party' AND d.kind = 'document' \
     AND e.state NOT IN ('retracted', 'superseded')), \
     s.documents, \
     ARRAY(SELECT r.registry FROM company_registry_due r \
     WHERE r.company_id = s.id AND r.next_due_at <= now() \
     ORDER BY r.registry COLLATE \"C\") \
     FROM (SELECT n.id, f.company_id AS enriched, f.documents \
     FROM nodes n LEFT JOIN company_enrichment f ON f.company_id = n.id \
     WHERE n.kind = 'company' AND ($2::uuid IS NULL OR n.id > $2) \
     AND NOT EXISTS (SELECT 1 FROM company_leases l WHERE l.company_id = n.id \
     AND l.owner <> $1 AND l.expires_at > now()) \
     ORDER BY n.id OFFSET 0) s \
     WHERE CASE WHEN s.enriched IS NULL OR (SELECT true FROM company_registry_due r \
     WHERE r.company_id = s.id AND r.next_due_at <= now() LIMIT 1) \
     THEN (SELECT true FROM edges e JOIN nodes d ON d.id = e.src \
     WHERE e.dst = s.id AND e.kind = 'party' AND d.kind = 'document' \
     AND e.state NOT IN ('retracted', 'superseded') LIMIT 1) IS NOT NULL \
     WHEN (SELECT count(*) FROM (SELECT 1 FROM edges e WHERE e.dst = s.id AND e.kind = 'party' \
     LIMIT 2 * greatest(s.documents, 1)) b) < 2 * greatest(s.documents, 1) THEN false \
     ELSE (SELECT count(*) FROM (SELECT DISTINCT e.src FROM edges e JOIN nodes d ON d.id = e.src \
     WHERE e.dst = s.id AND e.kind = 'party' AND d.kind = 'document' \
     AND e.state NOT IN ('retracted', 'superseded') LIMIT 2 * greatest(s.documents, 1)) x) \
     >= 2 * greatest(s.documents, 1) END \
     ORDER BY s.id LIMIT $3";

fn count(n: i64) -> Result<u64> {
    u64::try_from(n).map_err(|e| bad("count", e))
}

async fn enrichment(c: &impl GenericClient, company: Uuid) -> Result<Option<Enrichment>> {
    let Some(row) = c
        .opt(
            "SELECT extract(epoch FROM enriched_at)::bigint, documents FROM company_enrichment \
             WHERE company_id = $1",
            &[&company],
        )
        .await?
    else {
        return Ok(None);
    };
    let mut next_due = BTreeMap::new();
    for r in c
        .rows(
            "SELECT registry, extract(epoch FROM next_due_at)::bigint FROM company_registry_due \
             WHERE company_id = $1",
            &[&company],
        )
        .await?
    {
        next_due.insert(r.try_get(0).map_err(pg)?, r.try_get(1).map_err(pg)?);
    }
    Ok(Some(Enrichment {
        company,
        enriched_at: row.try_get(0).map_err(pg)?,
        documents: count(row.try_get(1).map_err(pg)?)?,
        next_due,
    }))
}

#[async_trait]
impl CacheStore for PgStore {
    async fn cache_get(&self, registry: &str, key: &str) -> Result<Option<CacheRow>> {
        validate_registry(registry)?;
        validate_cache_key(key)?;
        let c = client(&self.pool).await?;
        c.opt(
            "SELECT body, extract(epoch FROM fetched_at)::bigint, \
             extract(epoch FROM expires_at)::bigint FROM registry_cache \
             WHERE registry = $1 AND key = $2 AND expires_at > now()",
            &[&registry, &key],
        )
        .await?
        .map(|r| {
            Ok(CacheRow {
                registry: registry.to_owned(),
                key: key.to_owned(),
                body: r.try_get(0).map_err(pg)?,
                fetched_at: r.try_get(1).map_err(pg)?,
                expires_at: r.try_get(2).map_err(pg)?,
            })
        })
        .transpose()
    }

    async fn cache_put(&self, row: &CacheRow) -> Result<()> {
        row.validate()?;
        let c = client(&self.pool).await?;
        c.exec(
            "INSERT INTO registry_cache (registry, key, body, fetched_at, expires_at) \
             VALUES ($1, $2, $3, to_timestamp($4::bigint), to_timestamp($5::bigint)) \
             ON CONFLICT (registry, key) DO UPDATE SET body = EXCLUDED.body, \
             fetched_at = EXCLUDED.fetched_at, expires_at = EXCLUDED.expires_at",
            &[
                &row.registry,
                &row.key,
                &row.body.as_deref(),
                &row.fetched_at,
                &row.expires_at,
            ],
        )
        .await?;
        Ok(())
    }

    async fn icon_get(&self, domain: &str) -> Result<Option<IconRow>> {
        let c = client(&self.pool).await?;
        c.opt(
            "SELECT icon, format, extract(epoch FROM fetched_at)::bigint, \
             extract(epoch FROM expires_at)::bigint FROM favicons \
             WHERE domain = $1 AND expires_at > now()",
            &[&domain],
        )
        .await?
        .map(|r| {
            let icon: Option<Vec<u8>> = r.try_get(0).map_err(pg)?;
            let format: Option<String> = r.try_get(1).map_err(pg)?;
            Ok(IconRow {
                domain: domain.to_owned(),
                icon: icon
                    .zip(format)
                    .map(|(bytes, format)| StoredIcon { format, bytes }),
                fetched_at: r.try_get(2).map_err(pg)?,
                expires_at: r.try_get(3).map_err(pg)?,
            })
        })
        .transpose()
    }

    async fn icon_put(&self, row: &IconRow) -> Result<()> {
        row.validate()?;
        let c = client(&self.pool).await?;
        let (bytes, format) = match &row.icon {
            Some(i) => (Some(i.bytes.as_slice()), Some(i.format.as_str())),
            None => (None, None),
        };
        c.exec(
            "INSERT INTO favicons (domain, icon, format, fetched_at, expires_at) \
             VALUES ($1, $2, $3, to_timestamp($4::bigint), to_timestamp($5::bigint)) \
             ON CONFLICT (domain) DO UPDATE SET icon = EXCLUDED.icon, format = EXCLUDED.format, \
             fetched_at = EXCLUDED.fetched_at, expires_at = EXCLUDED.expires_at",
            &[
                &row.domain,
                &bytes,
                &format,
                &row.fetched_at,
                &row.expires_at,
            ],
        )
        .await?;
        Ok(())
    }

    async fn purge_expired(&self) -> Result<u64> {
        let c = client(&self.pool).await?;
        let n: i64 = c
            .one(
                "WITH a AS (DELETE FROM registry_cache WHERE expires_at <= now() RETURNING 1), \
                 b AS (DELETE FROM favicons WHERE expires_at <= now() RETURNING 1) \
                 SELECT (SELECT count(*) FROM a) + (SELECT count(*) FROM b)",
                &[],
            )
            .await?
            .try_get(0)
            .map_err(pg)?;
        count(n)
    }

    async fn cache_invalidate(&self, registry: &str, key_prefix: &str) -> Result<u64> {
        validate_registry(registry)?;
        validate_cache_key(key_prefix)?;
        let c = client(&self.pool).await?;
        // Not cached: a generic plan could not turn the prefix into a range.
        let n = c
            .execute(
                "DELETE FROM registry_cache WHERE registry = $1 AND starts_with(key, $2)",
                &[&registry, &key_prefix],
            )
            .await
            .map_err(pg)?;
        Ok(n)
    }
}

#[async_trait]
impl FreshnessStore for PgStore {
    async fn record_enrichment(
        &self,
        company: Uuid,
        schedule: &[(String, Duration)],
    ) -> Result<Enrichment> {
        validate_schedule(schedule)?;
        let mut c = client(&self.pool).await?;
        let tx = c.transaction().await.map_err(pg)?;
        // A husk is never recorded (issue #62); the lock holds back a merge until the commit.
        if !lock_live(&tx, company).await? {
            return Err(Error::NotFound(format!("no company {company}")));
        }
        let written = tx
            .exec(
                &format!(
                    "INSERT INTO company_enrichment (company_id, enriched_at, documents) \
                     SELECT n.id, now(), {DOCUMENTS} FROM nodes n \
                     WHERE n.id = $1 AND n.kind = 'company' \
                     ON CONFLICT (company_id) DO UPDATE SET \
                     enriched_at = EXCLUDED.enriched_at, documents = EXCLUDED.documents"
                ),
                &[&company],
            )
            .await?;
        if written == 0 {
            return Err(Error::NotFound(format!("no company {company}")));
        }
        for (registry, every) in schedule {
            tx.exec(
                "INSERT INTO company_registry_due (company_id, registry, next_due_at) \
                 VALUES ($1, $2, now() + make_interval(secs => $3)) \
                 ON CONFLICT (company_id, registry) DO UPDATE SET \
                 next_due_at = EXCLUDED.next_due_at",
                &[&company, registry, &every.as_secs_f64()],
            )
            .await?;
        }
        let e = enrichment(&tx, company)
            .await?
            .ok_or_else(|| bad("enrichment", "missing after the write"))?;
        tx.commit().await.map_err(pg)?;
        Ok(e)
    }

    async fn enrichment(&self, company: Uuid) -> Result<Option<Enrichment>> {
        enrichment(&client(&self.pool).await?, company).await
    }

    async fn mark_due(&self, company: Uuid, registries: &[String]) -> Result<()> {
        for r in registries {
            validate_registry(r)?;
        }
        let mut c = client(&self.pool).await?;
        let tx = c.transaction().await.map_err(pg)?;
        if !lock_live(&tx, company).await? {
            return Err(Error::NotFound(format!("no company {company}")));
        }
        let kind = tx
            .opt(
                "SELECT n.kind, f.company_id IS NOT NULL FROM nodes n \
                 LEFT JOIN company_enrichment f ON f.company_id = n.id WHERE n.id = $1",
                &[&company],
            )
            .await?;
        let Some(row) = kind.filter(|r| r.try_get::<_, &str>(0).is_ok_and(|k| k == "company"))
        else {
            return Err(Error::NotFound(format!("no company {company}")));
        };
        // Never enriched: due already.
        if row.try_get::<_, bool>(1).map_err(pg)? {
            for registry in registries {
                tx.exec(
                    "INSERT INTO company_registry_due (company_id, registry, next_due_at) \
                     VALUES ($1, $2, now()) ON CONFLICT (company_id, registry) DO UPDATE SET \
                     next_due_at = least(company_registry_due.next_due_at, EXCLUDED.next_due_at)",
                    &[&company, registry],
                )
                .await?;
            }
        }
        tx.commit().await.map_err(pg)?;
        Ok(())
    }

    async fn companies_due(
        &self,
        owner: &InstanceId,
        after: Option<Uuid>,
        limit: u32,
    ) -> Result<Vec<DueCompany>> {
        let c = client(&self.pool).await?;
        let rows = c
            .rows_uncached(
                COMPANIES_DUE,
                &[&owner.as_str(), &after, &i64::from(due_limit(limit))],
            )
            .await?;
        rows.iter()
            .map(|r| {
                let documents = count(r.try_get(1).map_err(pg)?)?;
                let then: Option<i64> = r.try_get(2).map_err(pg)?;
                let reason = match then {
                    None => DueReason::New,
                    Some(t) if documents >= 2 * count(t)?.max(1) => DueReason::Doubled,
                    Some(_) => DueReason::Scheduled,
                };
                Ok(DueCompany {
                    company: r.try_get(0).map_err(pg)?,
                    reason,
                    documents,
                    due: r.try_get(3).map_err(pg)?,
                })
            })
            .collect()
    }

    async fn mark_many_due(&self, companies: &[Uuid], registry: &str) -> Result<u64> {
        validate_registry(registry)?;
        validate_batch(companies.len())?;
        if companies.is_empty() {
            return Ok(0);
        }
        let c = client(&self.pool).await?;
        let n = c
            .exec(
                "INSERT INTO company_registry_due (company_id, registry, next_due_at) \
                 SELECT f.company_id, $2, now() FROM company_enrichment f \
                 WHERE f.company_id = ANY($1) \
                 ON CONFLICT (company_id, registry) DO UPDATE SET \
                 next_due_at = least(company_registry_due.next_due_at, EXCLUDED.next_due_at)",
                &[&companies, &registry],
            )
            .await?;
        Ok(n)
    }
}
