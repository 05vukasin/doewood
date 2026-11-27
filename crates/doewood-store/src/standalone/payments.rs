//! Payments on SQLite (issue #193, see `doewood_model::payment`): the `payments` index table
//! (schema 19), and the supplier and buyer read through the document's `party` edges marked
//! `side`, as on Postgres (`doewood-pg`, `payments.rs`).

use doewood_model::{
    Blake3, Payment, PaymentFilter, PaymentRow, Provided, Result, SIDE_BUYER, SIDE_SUPPLIER,
    SPEND_MAX_ROWS, Spend, SpendGroup, SpendTotal, payment_page_bytes, payments_limit,
};
use rusqlite::types::Value as Sql;
use rusqlite::{Connection, Row, params_from_iter};
use time::{Date, Month};
use uuid::Uuid;

use super::{backend, raw_json, sql, uuid};
use crate::store::now;

const PUT: &str = "INSERT INTO payments (blake3, subject, category, category_label, amount, \
     basis, currency, period, day, supplier_name, buyer_name, conf, detail, updated_at) \
     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14) \
     ON CONFLICT (blake3) DO UPDATE SET subject = excluded.subject, \
     category = excluded.category, category_label = excluded.category_label, \
     amount = excluded.amount, basis = excluded.basis, currency = excluded.currency, \
     period = excluded.period, day = excluded.day, supplier_name = excluded.supplier_name, \
     buyer_name = excluded.buyer_name, conf = excluded.conf, detail = excluded.detail, \
     updated_at = excluded.updated_at \
     WHERE (payments.subject, payments.category, payments.category_label, payments.amount, \
     payments.basis, payments.currency, payments.period, payments.day, payments.supplier_name, \
     payments.buyer_name, payments.conf, payments.detail) IS NOT \
     (excluded.subject, excluded.category, excluded.category_label, excluded.amount, \
     excluded.basis, excluded.currency, excluded.period, excluded.day, excluded.supplier_name, \
     excluded.buyer_name, excluded.conf, excluded.detail)";

/// The live `party` edge of document `d` marked `side`: the most confident, then the
/// smallest company id.
fn side(side: &str) -> String {
    format!(
        "(SELECT e.dst FROM edges e WHERE e.src = d.id AND e.kind = 'party' \
         AND e.state NOT IN ('retracted', 'superseded') \
         AND json_extract(e.props_json, '$.side') = '{side}' \
         ORDER BY e.confidence DESC, e.dst LIMIT 1)"
    )
}

/// The payments (`t`: the table or subquery) whose document has a live `party` edge to the
/// company `?` marked `side` (a candidate set from `edges_by_dst`; unhex gives NULL for `ref:`
/// placeholders).
fn by_company(t: &str, side: &str) -> String {
    format!(
        "{t}.blake3 IN (SELECT unhex(g.key) FROM edges f JOIN nodes g ON g.id = f.src \
         WHERE f.dst = ? AND f.kind = 'party' AND g.kind = 'document' \
         AND f.state NOT IN ('retracted', 'superseded') \
         AND json_extract(f.props_json, '$.side') = '{side}')"
    )
}

const FROM: &str = "FROM payments p \
     LEFT JOIN nodes d ON d.kind = 'document' AND d.key = lower(hex(p.blake3))";

const COLUMNS: &str = "p.blake3, p.subject, p.category, p.category_label, p.amount, p.basis, \
     p.currency, p.period, p.day, p.supplier_name, p.buyer_name, p.conf, p.detail";

fn day_text(d: Date) -> String {
    format!("{:04}-{:02}-{:02}", d.year(), u8::from(d.month()), d.day())
}

fn day_from(text: Option<String>) -> Result<Option<Date>> {
    let Some(t) = text else { return Ok(None) };
    let bad = || backend(format!("stored payment day {t:?} is invalid"));
    let mut parts = t.splitn(3, '-');
    let (y, m, d) = (parts.next(), parts.next(), parts.next());
    let y: i32 = y.and_then(|y| y.parse().ok()).ok_or_else(bad)?;
    let m: u8 = m.and_then(|m| m.parse().ok()).ok_or_else(bad)?;
    let d: u8 = d.and_then(|d| d.parse().ok()).ok_or_else(bad)?;
    let m = Month::try_from(m).map_err(|_| bad())?;
    Date::from_calendar_date(y, m, d)
        .map(Some)
        .map_err(|_| bad())
}

fn opt_uuid(text: Option<String>) -> Result<Option<Uuid>> {
    text.as_deref().map(uuid).transpose()
}

pub(super) fn put(conn: &mut Connection, row: &PaymentRow) -> Result<()> {
    conn.prepare_cached(PUT)
        .and_then(|mut cached| {
            cached.execute(rusqlite::params![
                row.blake3.as_bytes().as_slice(),
                row.subject,
                row.category,
                row.category_label,
                row.amount,
                row.basis,
                row.currency,
                row.period,
                row.date.map(day_text),
                row.supplier_name,
                row.buyer_name,
                f64::from(row.conf),
                row.detail.get(),
                now(),
            ])
        })
        .map_err(sql)?;
    Ok(())
}

pub(super) fn delete(conn: &mut Connection, blake3: &Blake3) -> Result<()> {
    conn.prepare_cached("DELETE FROM payments WHERE blake3 = ?1")
        .and_then(|mut cached| cached.execute([blake3.as_bytes().as_slice()]))
        .map_err(sql)?;
    Ok(())
}

/// The columns of [`COLUMNS`], then the document, supplier and buyer ids, as read.
type Read = (
    Vec<u8>,
    [Option<String>; 7],
    Option<i64>,
    Option<String>,
    Option<String>,
    f64,
    String,
    [Option<String>; 3],
);

fn read_row(r: &Row<'_>) -> rusqlite::Result<Read> {
    Ok((
        r.get(0)?,
        [
            r.get(1)?,
            r.get(2)?,
            r.get(3)?,
            r.get(5)?,
            r.get(6)?,
            r.get(7)?,
            r.get(8)?,
        ],
        r.get(4)?,
        r.get(9)?,
        r.get(10)?,
        r.get(11)?,
        r.get(12)?,
        [r.get(13)?, r.get(14)?, r.get(15)?],
    ))
}

#[allow(clippy::cast_possible_truncation)] // a confidence, stored from an f32
fn payment_from(read: Read) -> Result<Payment> {
    let (hash, texts, amount, supplier_name, buyer_name, conf, detail, ids) = read;
    let [subject, category, category_label, basis, currency, period, day] = texts;
    let [document, supplier, buyer] = ids;
    Ok(Payment {
        row: PaymentRow {
            blake3: Blake3::from_slice(&hash)?,
            subject,
            category,
            category_label,
            amount,
            basis,
            currency,
            period,
            date: day_from(day)?,
            supplier_name,
            buyer_name,
            conf: conf as f32,
            detail: raw_json(detail)?,
        },
        document: opt_uuid(document)?,
        supplier: opt_uuid(supplier)?,
        buyer: opt_uuid(buyer)?,
    })
}

pub(super) fn list(
    conn: &mut Connection,
    filter: &PaymentFilter,
    after: Option<&Blake3>,
    limit: u32,
    max_bytes: usize,
) -> Result<Vec<Payment>> {
    let limit = payments_limit(limit);
    let mut args: Vec<Sql> = Vec::new();
    let mut wheres: Vec<String> = Vec::new();
    if let Some(a) = after {
        wheres.push("s.blake3 > ?".to_owned());
        args.push(Sql::Blob(a.as_bytes().to_vec()));
    }
    if let Some(s) = filter.supplier {
        wheres.push(by_company("s", SIDE_SUPPLIER));
        wheres.push("s.sup = ?".to_owned());
        args.push(Sql::Text(s.to_string()));
        args.push(Sql::Text(s.to_string()));
    }
    if let Some(b) = filter.buyer {
        wheres.push(by_company("s", SIDE_BUYER));
        wheres.push("s.buy = ?".to_owned());
        args.push(Sql::Text(b.to_string()));
        args.push(Sql::Text(b.to_string()));
    }
    for (col, v) in [
        ("s.category", &filter.category),
        ("s.period", &filter.period),
        ("s.currency", &filter.currency),
    ] {
        if let Some(v) = v {
            wheres.push(format!("{col} = ?"));
            args.push(Sql::Text(v.clone()));
        }
    }
    if let Some(d) = filter.from {
        wheres.push("s.day >= ?".to_owned());
        args.push(Sql::Text(day_text(d)));
    }
    if let Some(d) = filter.to {
        wheres.push("s.day <= ?".to_owned());
        args.push(Sql::Text(day_text(d)));
    }
    args.push(Sql::Integer(i64::from(limit)));
    let filter_sql = if wheres.is_empty() {
        String::new()
    } else {
        format!("WHERE {}", wheres.join(" AND "))
    };
    // The supplier and buyer in a subquery, so a filter on them reads each once.
    let sql_text = format!(
        "SELECT * FROM (SELECT {COLUMNS}, d.id AS doc, {} AS sup, {} AS buy {FROM}) s \
         {} ORDER BY s.blake3 LIMIT ?",
        side(SIDE_SUPPLIER),
        side(SIDE_BUYER),
        filter_sql,
    );
    let mut stmt = conn.prepare(&sql_text).map_err(sql)?;
    let rows = stmt
        .query_map(params_from_iter(args), read_row)
        .map_err(sql)?;
    let mut out = Vec::new();
    let mut bytes = 0usize;
    for r in rows {
        let p = payment_from(r.map_err(sql)?)?;
        bytes += payment_page_bytes(&p);
        out.push(p);
        if bytes >= max_bytes {
            break;
        }
    }
    Ok(out)
}

pub(super) fn spend(
    conn: &mut Connection,
    supplier: Option<Uuid>,
    group: SpendGroup,
) -> Result<Spend> {
    let group_sql = match group {
        SpendGroup::Category => "coalesce(p.category, '')",
        SpendGroup::Year => "coalesce(substr(p.day, 1, 4), '')",
        SpendGroup::Month => "coalesce(substr(p.day, 1, 7), '')",
    };
    let mut args: Vec<Sql> = Vec::new();
    let (pre, post) = match supplier {
        Some(s) => {
            args.push(Sql::Text(s.to_string()));
            args.push(Sql::Text(s.to_string()));
            (format!("WHERE {}", by_company("p", SIDE_SUPPLIER)), "WHERE sup = ?")
        }
        None => (String::new(), ""),
    };
    let max = i64::from(SPEND_MAX_ROWS) + 1;
    args.push(Sql::Integer(max));
    let base = format!(
        "WITH base AS MATERIALIZED (SELECT * FROM (SELECT p.amount, \
         coalesce(p.currency, '') AS cur, coalesce(p.period, '') AS per, {group_sql} AS grp, \
         coalesce(p.category, '') AS cat, coalesce(p.category_label, '') AS label, \
         coalesce(p.subject, '') AS subj, {} AS sup {FROM} {pre}) b {post})",
        side(SIDE_SUPPLIER)
    );
    let totals_sql = format!(
        "{base} SELECT sup, cur, per, grp, sum(amount), count(*) FROM base \
         WHERE amount IS NOT NULL GROUP BY sup, cur, per, grp \
         ORDER BY sup NULLS LAST, cur, per, grp LIMIT ?"
    );
    let provides_sql = format!(
        "{base} SELECT sup, cat, max(label), subj, count(*) FROM base GROUP BY sup, cat, subj \
         ORDER BY sup NULLS LAST, count(*) DESC, cat, subj LIMIT ?"
    );
    let mut out = Spend::default();
    {
        let mut stmt = conn.prepare(&totals_sql).map_err(sql)?;
        let rows = stmt
            .query_map(params_from_iter(args.iter()), |r| {
                Ok((
                    r.get::<_, Option<String>>(0)?,
                    r.get::<_, String>(1)?,
                    r.get::<_, String>(2)?,
                    r.get::<_, String>(3)?,
                    r.get::<_, i64>(4)?,
                    r.get::<_, i64>(5)?,
                ))
            })
            .map_err(sql)?;
        for r in rows {
            let (sup, currency, period, group, total, n) = r.map_err(sql)?;
            out.totals.push(SpendTotal {
                supplier: opt_uuid(sup)?,
                currency,
                period,
                group,
                total: i128::from(total),
                documents: u64::try_from(n).unwrap_or(0),
            });
        }
    }
    {
        let mut stmt = conn.prepare(&provides_sql).map_err(sql)?;
        let rows = stmt
            .query_map(params_from_iter(args.iter()), |r| {
                Ok((
                    r.get::<_, Option<String>>(0)?,
                    r.get::<_, String>(1)?,
                    r.get::<_, String>(2)?,
                    r.get::<_, String>(3)?,
                    r.get::<_, i64>(4)?,
                ))
            })
            .map_err(sql)?;
        for r in rows {
            let (sup, category, category_label, subject, n) = r.map_err(sql)?;
            out.provides.push(Provided {
                supplier: opt_uuid(sup)?,
                category,
                category_label,
                subject,
                documents: u64::try_from(n).unwrap_or(0),
            });
        }
    }
    let max = SPEND_MAX_ROWS as usize;
    out.truncated = out.totals.len() > max || out.provides.len() > max;
    out.totals.truncate(max);
    out.provides.truncate(max);
    Ok(out)
}
