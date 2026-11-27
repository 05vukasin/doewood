//! `RecordsService.ListPayments` and `SupplierSpend` (issue #193): what documents pay for and
//! how much, and what each supplier is paid. See `doewood_model::payment`.

use std::collections::BTreeMap;

use doewood_model::{
    PAYMENTS_MAX_LIMIT, PAYMENTS_PAGE_BYTES, PERIODS, Payment, PaymentFilter, SpendGroup,
    format_units, page_full, payment_page_bytes, payments_limit,
};
use tonic::{Request, Response, Status};
use uuid::Uuid;

use crate::pb::{
    self, ListPaymentsRequest, ListPaymentsResponse, SpendGroupBy, SupplierSpendRequest,
    SupplierSpendResponse,
};
use crate::validate::{self, Violations};
use crate::{Api, error};

/// Most bytes of a category, period or currency filter.
const CODE_MAX: usize = 64;

fn code(v: &mut Violations, field: &str, value: &str) -> Option<String> {
    if value.is_empty() {
        return None;
    }
    if value.len() > CODE_MAX {
        v.add(field, format!("at most {CODE_MAX} bytes"));
        return None;
    }
    Some(value.to_owned())
}

fn opt_uuid(v: &mut Violations, field: &str, value: &str) -> Option<Uuid> {
    if value.is_empty() {
        None
    } else {
        validate::uuid(v, field, value)
    }
}

fn id(u: Option<Uuid>) -> String {
    u.map(|u| u.to_string()).unwrap_or_default()
}

fn payment(p: Payment) -> pb::Payment {
    let detail = p.row.detail().unwrap_or_default();
    let r = p.row;
    pb::Payment {
        blake3: r.blake3.to_hex(),
        document_id: id(p.document),
        supplier_id: id(p.supplier),
        supplier_name: r.supplier_name.unwrap_or_default(),
        buyer_id: id(p.buyer),
        buyer_name: r.buyer_name.unwrap_or_default(),
        subject: r.subject.unwrap_or_default(),
        subject_source: detail.subject_source.unwrap_or_default(),
        category: r.category.unwrap_or_default(),
        category_label: r.category_label.unwrap_or_default(),
        amount: r
            .amount
            .map(|a| format_units(i128::from(a)))
            .unwrap_or_default(),
        amount_basis: r.basis.unwrap_or_default(),
        currency: r.currency.unwrap_or_default(),
        net: detail.net.unwrap_or_default(),
        vat: detail.vat.unwrap_or_default(),
        gross: detail.gross.unwrap_or_default(),
        period: r.period.unwrap_or_default(),
        date: r.date.map(validate::format_date).unwrap_or_default(),
        conf: r.conf,
        method: detail.method,
        lines: detail
            .lines
            .into_iter()
            .map(|l| pb::PaymentLine {
                description: l.description.unwrap_or_default(),
                quantity: l.quantity.unwrap_or_default(),
                unit: l.unit.unwrap_or_default(),
                unit_price: l.unit_price.unwrap_or_default(),
                total: l.total.unwrap_or_default(),
                currency: l.currency.unwrap_or_default(),
                vat_rate: l.vat_rate.unwrap_or_default(),
            })
            .collect(),
        highlights: detail
            .highlights
            .into_iter()
            .map(|h| pb::Highlight {
                key: h.key,
                page: h.page,
                block: h.block,
                start: h.start,
                end: h.end,
                quote: h.quote,
                #[allow(clippy::cast_possible_truncation)] // 0 to 1, two decimals
                conf: h.conf as f32,
            })
            .collect(),
    }
}

/// How a period's total reads, and its multiplier to a year.
fn period_label(period: &str) -> (&'static str, Option<i128>) {
    match period {
        "one_off" => ("one-off prices", None),
        "monthly" => ("monthly fees (per month)", Some(12)),
        "quarterly" => ("quarterly fees (per quarter)", Some(4)),
        "yearly" => ("yearly fees (per year)", Some(1)),
        _ => ("prices, period not stated", None),
    }
}

pub(crate) async fn list(
    api: &Api,
    request: Request<ListPaymentsRequest>,
) -> Result<Response<ListPaymentsResponse>, Status> {
    let req = request.get_ref();
    let mut v = Violations::default();
    let filter = PaymentFilter {
        supplier: opt_uuid(&mut v, "supplier_id", &req.supplier_id),
        buyer: opt_uuid(&mut v, "buyer_id", &req.buyer_id),
        category: code(&mut v, "category", &req.category),
        period: code(&mut v, "period", &req.period),
        currency: code(&mut v, "currency", &req.currency),
        from: validate::date(&mut v, "date_from", &req.date_from),
        to: validate::date(&mut v, "date_to", &req.date_to),
    };
    if let Some(p) = &filter.period
        && !PERIODS.contains(&p.as_str())
    {
        v.add("period", "one of one_off, monthly, quarterly, yearly");
    }
    if let (Some(a), Some(b)) = (filter.from, filter.to)
        && a > b
    {
        v.add("date_to", "before date_from");
    }
    if req.page_size > PAYMENTS_MAX_LIMIT {
        v.add("page_size", "at most 1000");
    }
    let after = if req.page_token.is_empty() {
        None
    } else {
        let parsed = req.page_token.parse().ok();
        if parsed.is_none() {
            v.add("page_token", "not a token from a previous page");
        }
        parsed
    };
    v.check()?;
    let limit = payments_limit(req.page_size);
    let page = api
        .state
        .backend()?
        .list_payments(&filter, after.as_ref(), limit, PAYMENTS_PAGE_BYTES)
        .await
        .map_err(error::model)?;
    // The token is the last hash (opaque to clients), as for ListRecords.
    let next_page_token = if page_full(&page, limit, PAYMENTS_PAGE_BYTES, payment_page_bytes) {
        page.last()
            .map(|p| p.row.blake3.to_hex())
            .unwrap_or_default()
    } else {
        String::new()
    };
    Ok(Response::new(ListPaymentsResponse {
        payments: page.into_iter().map(payment).collect(),
        next_page_token,
    }))
}

pub(crate) async fn spend(
    api: &Api,
    request: Request<SupplierSpendRequest>,
) -> Result<Response<SupplierSpendResponse>, Status> {
    let req = request.get_ref();
    let mut v = Violations::default();
    let company = opt_uuid(&mut v, "company_id", &req.company_id);
    let group = match SpendGroupBy::try_from(req.group_by) {
        Ok(SpendGroupBy::Unspecified | SpendGroupBy::Category) => SpendGroup::Category,
        Ok(SpendGroupBy::Year) => SpendGroup::Year,
        Ok(SpendGroupBy::Month) => SpendGroup::Month,
        Err(_) => {
            v.add("group_by", "not a SpendGroupBy");
            SpendGroup::Category
        }
    };
    v.check()?;
    let backend = api.state.backend()?;
    let spend = backend
        .supplier_spend(company, group)
        .await
        .map_err(error::model)?;
    // One entry per supplier, in the store's order (by id, none last).
    let mut order: Vec<Option<Uuid>> = Vec::new();
    let mut by: BTreeMap<Option<Uuid>, pb::SupplierSpend> = BTreeMap::new();
    let mut entry = |s: Option<Uuid>, order: &mut Vec<Option<Uuid>>| {
        by.entry(s).or_insert_with(|| {
            order.push(s);
            pb::SupplierSpend {
                company_id: id(s),
                ..pb::SupplierSpend::default()
            }
        });
    };
    for t in &spend.totals {
        entry(t.supplier, &mut order);
    }
    for p in &spend.provides {
        entry(p.supplier, &mut order);
    }
    for t in spend.totals {
        let (label, year) = period_label(&t.period);
        let e = by.get_mut(&t.supplier).expect("entry");
        e.totals.push(pb::SpendTotal {
            group: t.group,
            currency: t.currency,
            period: t.period,
            total: format_units(t.total),
            documents: u32::try_from(t.documents).unwrap_or(u32::MAX),
            annualized: year
                .and_then(|n| t.total.checked_mul(n))
                .map(format_units)
                .unwrap_or_default(),
            label: label.to_owned(),
        });
    }
    for p in spend.provides {
        let e = by.get_mut(&p.supplier).expect("entry");
        e.provides.push(pb::SupplierProvides {
            category: p.category,
            category_label: p.category_label,
            subject: p.subject,
            documents: u32::try_from(p.documents).unwrap_or(u32::MAX),
        });
    }
    // Ids sort before the documents without a supplier, as the store returns them.
    order.sort_by_key(|s| (s.is_none(), *s));
    let mut suppliers = Vec::with_capacity(order.len());
    for s in order {
        let mut e = by.remove(&s).expect("entry");
        if let Some(company) = s {
            e.name = backend
                .node(company)
                .await
                .map_err(error::model)?
                .and_then(|n| {
                    n.props
                        .get("name")
                        .and_then(|v| v.as_str().map(str::to_owned))
                })
                .unwrap_or_default();
        }
        suppliers.push(e);
    }
    Ok(Response::new(SupplierSpendResponse {
        suppliers,
        truncated: spend.truncated,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn periods_are_labelled() {
        assert_eq!(
            period_label("monthly"),
            ("monthly fees (per month)", Some(12))
        );
        assert_eq!(period_label("one_off").1, None);
        assert_eq!(period_label("").0, "prices, period not stated");
    }
}
