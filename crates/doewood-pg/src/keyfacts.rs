//! Key facts on Postgres (issue #194, see `doewood_model::keyfacts`): the `document_facts`
//! index table (schema 18), and the supplier and customer read through the document's
//! `party` edges marked `side`, as payments do (`payments.rs`) and as on SQLite
//! (`doewood-store`, `standalone/keyfacts.rs`).

use deadpool_postgres::Pool;
use doewood_model::{
    Blake3, FactColumns, FactFilter, FactRow, FoundDocument, Result, SIDE_BUYER, SIDE_SUPPLIER,
    facts_limit, fold,
};
use serde_json::value::RawValue;
use tokio_postgres::Row;

use crate::backend::{Args, bad, client, pg};
use crate::cached::Cached as _;
use crate::payments::{by_company, side};

const PUT: &str = "INSERT INTO document_facts (blake3, kind, number, subject, supplier_name, \
     supplier_fold, customer_name, customer_fold, category, amount, currency, period, signed, \
     ends, notice_due, auto_renewal, has_penalty, has_guarantee, governing_law, court, \
     court_fold, card, highlights) \
     VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, \
     $19, $20, $21, $22, $23) \
     ON CONFLICT (blake3) DO UPDATE SET kind = EXCLUDED.kind, number = EXCLUDED.number, \
     subject = EXCLUDED.subject, supplier_name = EXCLUDED.supplier_name, \
     supplier_fold = EXCLUDED.supplier_fold, customer_name = EXCLUDED.customer_name, \
     customer_fold = EXCLUDED.customer_fold, category = EXCLUDED.category, \
     amount = EXCLUDED.amount, currency = EXCLUDED.currency, period = EXCLUDED.period, \
     signed = EXCLUDED.signed, ends = EXCLUDED.ends, notice_due = EXCLUDED.notice_due, \
     auto_renewal = EXCLUDED.auto_renewal, has_penalty = EXCLUDED.has_penalty, \
     has_guarantee = EXCLUDED.has_guarantee, governing_law = EXCLUDED.governing_law, \
     court = EXCLUDED.court, court_fold = EXCLUDED.court_fold, card = EXCLUDED.card, \
     highlights = EXCLUDED.highlights, updated_at = now() \
     WHERE (document_facts.card, document_facts.highlights) IS DISTINCT FROM \
     (EXCLUDED.card, EXCLUDED.highlights)";

/// The columns [`FactColumns`] reads, in its order.
const COLUMNS: &str = "p.blake3, p.kind, p.number, p.subject, p.supplier_name, \
     p.customer_name, p.category, p.amount, p.currency, p.period, p.signed, p.ends, \
     p.notice_due, p.auto_renewal, p.has_penalty, p.has_guarantee, p.governing_law, p.court";

pub(crate) async fn put(pool: &Pool, row: &FactRow) -> Result<()> {
    row.validate()?;
    let c = row.columns.clone();
    let folded = |s: &Option<String>| s.as_deref().map(fold);
    let c2 = client(pool).await?;
    c2.exec(
        PUT,
        &[
            &c.blake3.as_bytes().as_slice(),
            &c.kind,
            &c.number,
            &c.subject,
            &c.supplier_name,
            &folded(&c.supplier_name),
            &c.customer_name,
            &folded(&c.customer_name),
            &c.category,
            &c.amount,
            &c.currency,
            &c.period,
            &c.signed,
            &c.ends,
            &c.notice_due,
            &c.auto_renewal,
            &c.has_penalty,
            &c.has_guarantee,
            &c.governing_law,
            &c.court,
            &folded(&c.court),
            &row.card.get(),
            &row.highlights.get(),
        ],
    )
    .await?;
    Ok(())
}

pub(crate) async fn delete(pool: &Pool, blake3: &Blake3) -> Result<()> {
    let c = client(pool).await?;
    c.exec(
        "DELETE FROM document_facts WHERE blake3 = $1",
        &[&blake3.as_bytes().as_slice()],
    )
    .await?;
    Ok(())
}

fn columns_at(r: &Row) -> Result<FactColumns> {
    let hash: &[u8] = r.try_get(0).map_err(pg)?;
    Ok(FactColumns {
        blake3: Blake3::from_slice(hash)?,
        kind: r.try_get(1).map_err(pg)?,
        number: r.try_get(2).map_err(pg)?,
        subject: r.try_get(3).map_err(pg)?,
        supplier_name: r.try_get(4).map_err(pg)?,
        customer_name: r.try_get(5).map_err(pg)?,
        category: r.try_get(6).map_err(pg)?,
        amount: r.try_get(7).map_err(pg)?,
        currency: r.try_get(8).map_err(pg)?,
        period: r.try_get(9).map_err(pg)?,
        signed: r.try_get(10).map_err(pg)?,
        ends: r.try_get(11).map_err(pg)?,
        notice_due: r.try_get(12).map_err(pg)?,
        auto_renewal: r.try_get(13).map_err(pg)?,
        has_penalty: r.try_get(14).map_err(pg)?,
        has_guarantee: r.try_get(15).map_err(pg)?,
        governing_law: r.try_get(16).map_err(pg)?,
        court: r.try_get(17).map_err(pg)?,
    })
}

pub(crate) async fn get(pool: &Pool, blake3: &Blake3) -> Result<Option<FactRow>> {
    let c = client(pool).await?;
    let sql =
        format!("SELECT {COLUMNS}, p.card, p.highlights FROM document_facts p WHERE p.blake3 = $1");
    c.opt(&sql, &[&blake3.as_bytes().as_slice()])
        .await?
        .map(|r| {
            let card: String = r.try_get(18).map_err(pg)?;
            let highlights: String = r.try_get(19).map_err(pg)?;
            Ok(FactRow {
                columns: columns_at(&r)?,
                card: RawValue::from_string(card).map_err(|e| bad("document card", e))?,
                highlights: RawValue::from_string(highlights)
                    .map_err(|e| bad("document highlights", e))?,
            })
        })
        .transpose()
}

pub(crate) async fn find(
    pool: &Pool,
    filter: &FactFilter,
    after: Option<&Blake3>,
    limit: u32,
) -> Result<Vec<FoundDocument>> {
    let limit = facts_limit(limit);
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
    if let Some(b) = filter.customer {
        let at = args.push(b);
        wheres.push(by_company(SIDE_BUYER, &at));
        wheres.push(format!("s.buy = {at}"));
    }
    for (col, v) in [
        ("p.kind", &filter.kind),
        ("p.category", &filter.category),
        ("p.currency", &filter.currency),
    ] {
        if let Some(v) = v {
            wheres.push(format!("{col} = {}", args.push(v.clone())));
        }
    }
    // A country finds its subdivisions too: `GB` finds `GB-EAW` (England and Wales).
    if let Some(law) = &filter.governing_law {
        let at = args.push(law.clone());
        wheres.push(format!(
            "(p.governing_law = {at} OR starts_with(p.governing_law, {at} || '-'))"
        ));
    }
    for (col, v) in [
        ("p.supplier_fold", &filter.supplier_name),
        ("p.customer_fold", &filter.customer_name),
        ("p.court_fold", &filter.court),
    ] {
        if let Some(v) = v {
            wheres.push(format!("strpos({col}, {}) > 0", args.push(fold(v))));
        }
    }
    for (cond, v) in [
        ("p.amount >=", filter.amount_min),
        ("p.amount <=", filter.amount_max),
    ] {
        if let Some(v) = v {
            wheres.push(format!("{cond} {}", args.push(v)));
        }
    }
    for (cond, v) in [
        ("p.ends <=", filter.end_before),
        ("p.ends >=", filter.end_after),
        ("p.notice_due <=", filter.notice_due_before),
        ("p.notice_due >=", filter.notice_due_after),
        ("p.signed >=", filter.signed_from),
        ("p.signed <=", filter.signed_to),
    ] {
        if let Some(v) = v {
            wheres.push(format!("{cond} {}", args.push(v)));
        }
    }
    for (col, v) in [
        ("p.auto_renewal", filter.auto_renewal),
        ("p.has_penalty", filter.has_penalty),
        ("p.has_guarantee", filter.has_guarantee),
    ] {
        if let Some(v) = v {
            wheres.push(format!("{col} = {}", args.push(v)));
        }
    }
    let lim = args.push(i64::from(limit));
    let filter_sql = if wheres.is_empty() {
        String::new()
    } else {
        format!("WHERE {}", wheres.join(" AND "))
    };
    let sql = format!(
        "SELECT {COLUMNS}, d.id, s.sup, s.buy FROM document_facts p \
         LEFT JOIN nodes d ON d.kind = 'document' AND d.key = encode(p.blake3, 'hex') \
         LEFT JOIN LATERAL (SELECT {} AS sup, {} AS buy) s ON true \
         {filter_sql} ORDER BY p.blake3 LIMIT {lim}",
        side(SIDE_SUPPLIER),
        side(SIDE_BUYER),
    );
    let c = client(pool).await?;
    let rows = c.rows(&sql, &args.refs()).await?;
    rows.iter()
        .map(|r| {
            Ok(FoundDocument {
                columns: columns_at(r)?,
                document: r.try_get(18).map_err(pg)?,
                supplier: r.try_get(19).map_err(pg)?,
                customer: r.try_get(20).map_err(pg)?,
            })
        })
        .collect()
}
