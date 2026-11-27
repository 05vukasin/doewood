//! Payments on Postgres (issue #193, see `doewood_model::payment`): the `payments` index
//! table (schema 17), and the supplier and buyer read through the document's `party` edges
//! marked `side`, as on SQLite (`doewood-store`, `standalone/payments.rs`).

use deadpool_postgres::Pool;
use doewood_model::{
    Blake3, Error, Payment, PaymentFilter, PaymentRow, Provided, Result, SIDE_BUYER, SIDE_SUPPLIER,
    SPEND_MAX_ROWS, Spend, SpendGroup, SpendTotal, payment_page_bytes, payments_limit,
};
use serde_json::value::RawValue;
use time::Date;
use tokio_postgres::Row;
use uuid::Uuid;

use crate::backend::{Args, bad, client, pg};
use crate::cached::Cached as _;

const PUT: &str = "INSERT INTO payments (blake3, subject, category, category_label, amount, \
     basis, currency, period, day, supplier_name, buyer_name, conf, detail) \
     VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13) \
     ON CONFLICT (blake3) DO UPDATE SET subject = EXCLUDED.subject, \
     category = EXCLUDED.category, category_label = EXCLUDED.category_label, \
     amount = EXCLUDED.amount, basis = EXCLUDED.basis, currency = EXCLUDED.currency, \
     period = EXCLUDED.period, day = EXCLUDED.day, supplier_name = EXCLUDED.supplier_name, \
     buyer_name = EXCLUDED.buyer_name, conf = EXCLUDED.conf, detail = EXCLUDED.detail, \
     updated_at = now() \
     WHERE (payments.subject, payments.category, payments.category_label, payments.amount, \
     payments.basis, payments.currency, payments.period, payments.day, payments.supplier_name, \
     payments.buyer_name, payments.conf, payments.detail) IS DISTINCT FROM \
     (EXCLUDED.subject, EXCLUDED.category, EXCLUDED.category_label, EXCLUDED.amount, \
     EXCLUDED.basis, EXCLUDED.currency, EXCLUDED.period, EXCLUDED.day, EXCLUDED.supplier_name, \
     EXCLUDED.buyer_name, EXCLUDED.conf, EXCLUDED.detail)";

/// The live `party` edge of document `d` marked `side`: the most confident, then the
/// smallest company id.
pub(crate) fn side(side: &str) -> String {
    format!(
        "(SELECT e.dst FROM edges e WHERE e.src = d.id AND e.kind = 'party' \
         AND e.state NOT IN ('retracted', 'superseded') AND e.props->>'side' = '{side}' \
         ORDER BY e.confidence DESC, e.dst LIMIT 1)"
    )
}

/// The payments whose document has a live `party` edge to `company` marked `side` (a
/// candidate set from `edges_by_dst`; the chosen edge is checked after).
pub(crate) fn by_company(side: &str, company: &str) -> String {
    format!(
        "p.blake3 IN (SELECT CASE WHEN g.key ~ '^[0-9a-f]{{64}}$' THEN decode(g.key, 'hex') END \
         FROM edges f JOIN nodes g ON g.id = f.src WHERE f.dst = {company} AND f.kind = 'party' \
         AND g.kind = 'document' AND f.state NOT IN ('retracted', 'superseded') \
         AND f.props->>'side' = '{side}')"
    )
}

const FROM: &str = "FROM payments p \
     LEFT JOIN nodes d ON d.kind = 'document' AND d.key = encode(p.blake3, 'hex')";

const COLUMNS: &str = "p.blake3, p.subject, p.category, p.category_label, p.amount, p.basis, \
     p.currency, p.period, p.day, p.supplier_name, p.buyer_name, p.conf, p.detail";

pub(crate) async fn put(pool: &Pool, row: &PaymentRow) -> Result<()> {
    row.validate()?;
    let c = client(pool).await?;
    c.exec(
        PUT,
        &[
            &row.blake3.as_bytes().as_slice(),
            &row.subject,
            &row.category,
            &row.category_label,
            &row.amount,
            &row.basis,
            &row.currency,
            &row.period,
            &row.date,
            &row.supplier_name,
            &row.buyer_name,
            &row.conf,
            &row.detail.get(),
        ],
    )
    .await?;
    Ok(())
}

pub(crate) async fn delete(pool: &Pool, blake3: &Blake3) -> Result<()> {
    let c = client(pool).await?;
    c.exec(
        "DELETE FROM payments WHERE blake3 = $1",
        &[&blake3.as_bytes().as_slice()],
    )
    .await?;
    Ok(())
}

fn row_at(r: &Row) -> Result<PaymentRow> {
    let hash: &[u8] = r.try_get(0).map_err(pg)?;
    let detail: String = r.try_get(12).map_err(pg)?;
    Ok(PaymentRow {
        blake3: Blake3::from_slice(hash)?,
        subject: r.try_get(1).map_err(pg)?,
        category: r.try_get(2).map_err(pg)?,
        category_label: r.try_get(3).map_err(pg)?,
        amount: r.try_get(4).map_err(pg)?,
        basis: r.try_get(5).map_err(pg)?,
        currency: r.try_get(6).map_err(pg)?,
        period: r.try_get(7).map_err(pg)?,
        date: r.try_get::<_, Option<Date>>(8).map_err(pg)?,
        supplier_name: r.try_get(9).map_err(pg)?,
        buyer_name: r.try_get(10).map_err(pg)?,
        conf: r.try_get(11).map_err(pg)?,
        detail: RawValue::from_string(detail).map_err(|e| bad("payment detail", e))?,
    })
}

pub(crate) async fn list(
    pool: &Pool,
    filter: &PaymentFilter,
    after: Option<&Blake3>,
    limit: u32,
    max_bytes: usize,
) -> Result<Vec<Payment>> {
    let limit = payments_limit(limit);
    let mut args = Args::default();
    let mut wheres: Vec<String> = Vec::new();
    if let Some(a) = after {
        wheres.push(format!("p.blake3 > {}", args.push(a.as_bytes().to_vec())));
    }
    if let Some(s) = filter.supplier {
        let at = args.push(s);
        wheres.push(by_company(SIDE_SUPPLIER, &at));
        wheres.push(format!("s.sup = {at}"));
    }
    if let Some(b) = filter.buyer {
        let at = args.push(b);
        wheres.push(by_company(SIDE_BUYER, &at));
        wheres.push(format!("s.buy = {at}"));
    }
    for (col, v) in [
        ("p.category", &filter.category),
        ("p.period", &filter.period),
        ("p.currency", &filter.currency),
    ] {
        if let Some(v) = v {
            wheres.push(format!("{col} = {}", args.push(v.clone())));
        }
    }
    if let Some(d) = filter.from {
        wheres.push(format!("p.day >= {}", args.push(d)));
    }
    if let Some(d) = filter.to {
        wheres.push(format!("p.day <= {}", args.push(d)));
    }
    let lim = args.push(i64::from(limit));
    let filter_sql = if wheres.is_empty() {
        String::new()
    } else {
        format!("WHERE {}", wheres.join(" AND "))
    };
    let sql = format!(
        "SELECT {COLUMNS}, d.id, s.sup, s.buy {FROM} \
         LEFT JOIN LATERAL (SELECT {} AS sup, {} AS buy) s ON true \
         {filter_sql} ORDER BY p.blake3 LIMIT {lim}",
        side(SIDE_SUPPLIER),
        side(SIDE_BUYER),
    );
    let c = client(pool).await?;
    let rows = c.rows(&sql, &args.refs()).await?;
    let mut out = Vec::with_capacity(rows.len());
    let mut bytes = 0usize;
    for r in &rows {
        let p = Payment {
            row: row_at(r)?,
            document: r.try_get(13).map_err(pg)?,
            supplier: r.try_get(14).map_err(pg)?,
            buyer: r.try_get(15).map_err(pg)?,
        };
        bytes += payment_page_bytes(&p);
        out.push(p);
        if bytes >= max_bytes {
            break;
        }
    }
    Ok(out)
}

pub(crate) async fn spend(pool: &Pool, supplier: Option<Uuid>, group: SpendGroup) -> Result<Spend> {
    let group_sql = match group {
        SpendGroup::Category => "coalesce(p.category, '')",
        SpendGroup::Year => "coalesce(to_char(p.day, 'YYYY'), '')",
        SpendGroup::Month => "coalesce(to_char(p.day, 'YYYY-MM'), '')",
    };
    let mut args = Args::default();
    let (pre, post) = match supplier {
        Some(s) => {
            let at = args.push(s);
            (
                format!("WHERE {}", by_company(SIDE_SUPPLIER, &at)),
                format!("WHERE sup = {at}"),
            )
        }
        None => (String::new(), String::new()),
    };
    let max = args.push(i64::from(SPEND_MAX_ROWS) + 1);
    let base = format!(
        "WITH base AS MATERIALIZED (SELECT * FROM (SELECT p.amount, \
         coalesce(p.currency, '') AS cur, coalesce(p.period, '') AS per, {group_sql} AS grp, \
         coalesce(p.category, '') AS cat, coalesce(p.category_label, '') AS label, \
         coalesce(p.subject, '') AS subj, {} AS sup {FROM} {pre}) b {post})",
        side(SIDE_SUPPLIER)
    );
    let totals_sql = format!(
        "{base} SELECT sup, cur, per, grp, sum(amount)::text, count(*) FROM base \
         WHERE amount IS NOT NULL GROUP BY sup, cur, per, grp \
         ORDER BY sup NULLS LAST, cur, per, grp LIMIT {max}"
    );
    let provides_sql = format!(
        "{base} SELECT sup, cat, max(label), subj, count(*) FROM base GROUP BY sup, cat, subj \
         ORDER BY sup NULLS LAST, count(*) DESC, cat, subj LIMIT {max}"
    );
    let c = client(pool).await?;
    let refs = args.refs();
    let (totals, provides) =
        futures_util::future::try_join(c.rows(&totals_sql, &refs), c.rows(&provides_sql, &refs))
            .await?;
    let mut out = Spend::default();
    for r in &totals {
        let sum: String = r.try_get(4).map_err(pg)?;
        let n: i64 = r.try_get(5).map_err(pg)?;
        out.totals.push(SpendTotal {
            supplier: r.try_get(0).map_err(pg)?,
            currency: r.try_get(1).map_err(pg)?,
            period: r.try_get(2).map_err(pg)?,
            group: r.try_get(3).map_err(pg)?,
            total: sum
                .parse()
                .map_err(|e| Error::Invalid(format!("payment total {sum}: {e}")))?,
            documents: u64::try_from(n).unwrap_or(0),
        });
    }
    for r in &provides {
        let n: i64 = r.try_get(4).map_err(pg)?;
        out.provides.push(Provided {
            supplier: r.try_get(0).map_err(pg)?,
            category: r.try_get(1).map_err(pg)?,
            category_label: r.try_get(2).map_err(pg)?,
            subject: r.try_get(3).map_err(pg)?,
            documents: u64::try_from(n).unwrap_or(0),
        });
    }
    let max = SPEND_MAX_ROWS as usize;
    out.truncated = out.totals.len() > max || out.provides.len() > max;
    out.totals.truncate(max);
    out.provides.truncate(max);
    Ok(out)
}
