//! Key facts on SQLite (issue #194, see `doewood_model::keyfacts`): the `document_facts`
//! index table (schema 20), and the supplier and customer read through the document's
//! `party` edges marked `side`, as payments do (`payments.rs`) and as on Postgres
//! (`doewood-pg`, `keyfacts.rs`).

use doewood_model::{
    Blake3, FactColumns, FactFilter, FactRow, FoundDocument, Result, SIDE_BUYER, SIDE_SUPPLIER,
    facts_limit, fold,
};
use rusqlite::types::Value as Sql;
use rusqlite::{Connection, OptionalExtension as _, Row, params_from_iter};

use super::payments::{by_company, day_from, day_text, side};
use super::{raw_json, sql, uuid};
use crate::store::now;

const PUT: &str = "INSERT INTO document_facts (blake3, kind, number, subject, supplier_name, \
     supplier_fold, customer_name, customer_fold, category, amount, currency, period, signed, \
     ends, notice_due, auto_renewal, has_penalty, has_guarantee, governing_law, court, \
     court_fold, card, highlights, updated_at) \
     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, \
     ?19, ?20, ?21, ?22, ?23, ?24) \
     ON CONFLICT (blake3) DO UPDATE SET kind = excluded.kind, number = excluded.number, \
     subject = excluded.subject, supplier_name = excluded.supplier_name, \
     supplier_fold = excluded.supplier_fold, customer_name = excluded.customer_name, \
     customer_fold = excluded.customer_fold, category = excluded.category, \
     amount = excluded.amount, currency = excluded.currency, period = excluded.period, \
     signed = excluded.signed, ends = excluded.ends, notice_due = excluded.notice_due, \
     auto_renewal = excluded.auto_renewal, has_penalty = excluded.has_penalty, \
     has_guarantee = excluded.has_guarantee, governing_law = excluded.governing_law, \
     court = excluded.court, court_fold = excluded.court_fold, card = excluded.card, \
     highlights = excluded.highlights, updated_at = excluded.updated_at \
     WHERE (document_facts.card, document_facts.highlights) IS NOT \
     (excluded.card, excluded.highlights)";

/// The columns [`FactColumns`] reads, in its order.
const COLUMNS: &str = "p.blake3, p.kind, p.number, p.subject, p.supplier_name, \
     p.customer_name, p.category, p.amount, p.currency, p.period, p.signed, p.ends, \
     p.notice_due, p.auto_renewal, p.has_penalty, p.has_guarantee, p.governing_law, p.court";

pub(super) fn put(conn: &mut Connection, row: &FactRow) -> Result<()> {
    let c = &row.columns;
    let folded = |s: &Option<String>| s.as_deref().map(fold);
    conn.prepare_cached(PUT)
        .and_then(|mut cached| {
            cached.execute(rusqlite::params![
                c.blake3.as_bytes().as_slice(),
                c.kind,
                c.number,
                c.subject,
                c.supplier_name,
                folded(&c.supplier_name),
                c.customer_name,
                folded(&c.customer_name),
                c.category,
                c.amount,
                c.currency,
                c.period,
                c.signed.map(day_text),
                c.ends.map(day_text),
                c.notice_due.map(day_text),
                c.auto_renewal,
                c.has_penalty,
                c.has_guarantee,
                c.governing_law,
                c.court,
                folded(&c.court),
                row.card.get(),
                row.highlights.get(),
                now(),
            ])
        })
        .map_err(sql)?;
    Ok(())
}

pub(super) fn delete(conn: &mut Connection, blake3: &Blake3) -> Result<()> {
    conn.prepare_cached("DELETE FROM document_facts WHERE blake3 = ?1")
        .and_then(|mut cached| cached.execute([blake3.as_bytes().as_slice()]))
        .map_err(sql)?;
    Ok(())
}

/// The columns of [`COLUMNS`] as read: hash, the 8 texts, the amount, the 3 days, the 3
/// flags, the 2 texts after them.
type Read = (
    Vec<u8>,
    [Option<String>; 8],
    Option<i64>,
    [Option<String>; 3],
    (Option<bool>, bool, bool),
    [Option<String>; 2],
);

fn read_columns(r: &Row<'_>) -> rusqlite::Result<Read> {
    Ok((
        r.get(0)?,
        [
            r.get(1)?,
            r.get(2)?,
            r.get(3)?,
            r.get(4)?,
            r.get(5)?,
            r.get(6)?,
            r.get(8)?,
            r.get(9)?,
        ],
        r.get(7)?,
        [r.get(10)?, r.get(11)?, r.get(12)?],
        (r.get(13)?, r.get(14)?, r.get(15)?),
        [r.get(16)?, r.get(17)?],
    ))
}

fn columns_from(read: Read) -> Result<FactColumns> {
    let (hash, texts, amount, days, flags, law) = read;
    let [
        kind,
        number,
        subject,
        supplier_name,
        customer_name,
        category,
        currency,
        period,
    ] = texts;
    let [signed, ends, notice_due] = days;
    let [governing_law, court] = law;
    Ok(FactColumns {
        blake3: Blake3::from_slice(&hash)?,
        kind,
        number,
        subject,
        supplier_name,
        customer_name,
        category,
        amount,
        currency,
        period,
        signed: day_from(signed)?,
        ends: day_from(ends)?,
        notice_due: day_from(notice_due)?,
        auto_renewal: flags.0,
        has_penalty: flags.1,
        has_guarantee: flags.2,
        governing_law,
        court,
    })
}

pub(super) fn get(conn: &mut Connection, blake3: &Blake3) -> Result<Option<FactRow>> {
    let sql_text =
        format!("SELECT {COLUMNS}, p.card, p.highlights FROM document_facts p WHERE p.blake3 = ?1");
    let row = conn
        .prepare_cached(&sql_text)
        .and_then(|mut cached| {
            cached
                .query_row([blake3.as_bytes().as_slice()], |r| {
                    Ok((
                        read_columns(r)?,
                        r.get::<_, String>(18)?,
                        r.get::<_, String>(19)?,
                    ))
                })
                .optional()
        })
        .map_err(sql)?;
    row.map(|(cols, card, highlights)| {
        Ok(FactRow {
            columns: columns_from(cols)?,
            card: raw_json(card)?,
            highlights: raw_json(highlights)?,
        })
    })
    .transpose()
}

pub(super) fn find(
    conn: &mut Connection,
    filter: &FactFilter,
    after: Option<&Blake3>,
    limit: u32,
) -> Result<Vec<FoundDocument>> {
    let limit = facts_limit(limit);
    let mut args: Vec<Sql> = Vec::new();
    let mut wheres: Vec<String> = Vec::new();
    if let Some(a) = after {
        wheres.push("s.blake3 > ?".to_owned());
        args.push(Sql::Blob(a.as_bytes().to_vec()));
    }
    if let Some(c) = filter.supplier {
        wheres.push(by_company("s", SIDE_SUPPLIER));
        wheres.push("s.sup = ?".to_owned());
        args.push(Sql::Text(c.to_string()));
        args.push(Sql::Text(c.to_string()));
    }
    if let Some(c) = filter.customer {
        wheres.push(by_company("s", SIDE_BUYER));
        wheres.push("s.buy = ?".to_owned());
        args.push(Sql::Text(c.to_string()));
        args.push(Sql::Text(c.to_string()));
    }
    for (col, v) in [
        ("s.kind", &filter.kind),
        ("s.category", &filter.category),
        ("s.currency", &filter.currency),
    ] {
        if let Some(v) = v {
            wheres.push(format!("{col} = ?"));
            args.push(Sql::Text(v.clone()));
        }
    }
    // A country finds its subdivisions too: `GB` finds `GB-EAW` (England and Wales).
    if let Some(law) = &filter.governing_law {
        wheres.push(
            "(s.governing_law = ? OR substr(s.governing_law, 1, length(?) + 1) = ? || '-')"
                .to_owned(),
        );
        args.extend([
            Sql::Text(law.clone()),
            Sql::Text(law.clone()),
            Sql::Text(law.clone()),
        ]);
    }
    for (col, v) in [
        ("s.supplier_fold", &filter.supplier_name),
        ("s.customer_fold", &filter.customer_name),
        ("s.court_fold", &filter.court),
    ] {
        if let Some(v) = v {
            wheres.push(format!("instr({col}, ?) > 0"));
            args.push(Sql::Text(fold(v)));
        }
    }
    for (cond, v) in [
        ("s.amount >=", filter.amount_min),
        ("s.amount <=", filter.amount_max),
    ] {
        if let Some(v) = v {
            wheres.push(format!("{cond} ?"));
            args.push(Sql::Integer(v));
        }
    }
    for (cond, v) in [
        ("s.ends <=", filter.end_before),
        ("s.ends >=", filter.end_after),
        ("s.notice_due <=", filter.notice_due_before),
        ("s.notice_due >=", filter.notice_due_after),
        ("s.signed >=", filter.signed_from),
        ("s.signed <=", filter.signed_to),
    ] {
        if let Some(v) = v {
            wheres.push(format!("{cond} ?"));
            args.push(Sql::Text(day_text(v)));
        }
    }
    for (col, v) in [
        ("s.auto_renewal", filter.auto_renewal),
        ("s.has_penalty", filter.has_penalty),
        ("s.has_guarantee", filter.has_guarantee),
    ] {
        if let Some(v) = v {
            wheres.push(format!("{col} = ?"));
            args.push(Sql::Integer(i64::from(v)));
        }
    }
    args.push(Sql::Integer(i64::from(limit)));
    let filter_sql = if wheres.is_empty() {
        String::new()
    } else {
        format!("WHERE {}", wheres.join(" AND "))
    };
    // The supplier and customer in a subquery, so a filter on them reads each once.
    let sql_text = format!(
        "SELECT * FROM (SELECT {COLUMNS}, p.supplier_fold, p.customer_fold, p.court_fold, \
         d.id AS doc, {} AS sup, {} AS buy FROM document_facts p \
         LEFT JOIN nodes d ON d.kind = 'document' AND d.key = lower(hex(p.blake3))) s \
         {filter_sql} ORDER BY s.blake3 LIMIT ?",
        side(SIDE_SUPPLIER),
        side(SIDE_BUYER),
    );
    let mut stmt = conn.prepare(&sql_text).map_err(sql)?;
    let rows = stmt
        .query_map(params_from_iter(args), |r| {
            Ok((
                read_columns(r)?,
                r.get::<_, Option<String>>(21)?,
                r.get::<_, Option<String>>(22)?,
                r.get::<_, Option<String>>(23)?,
            ))
        })
        .map_err(sql)?;
    rows.map(|r| {
        let (cols, doc, sup, buy) = r.map_err(sql)?;
        Ok(FoundDocument {
            columns: columns_from(cols)?,
            document: doc.as_deref().map(uuid).transpose()?,
            supplier: sup.as_deref().map(uuid).transpose()?,
            customer: buy.as_deref().map(uuid).transpose()?,
        })
    })
    .collect()
}
