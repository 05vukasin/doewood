//! `RecordsService.GetDocumentCard` and `FindDocuments` (issue #194): a document's key-facts
//! card, and the documents whose key facts match filters. See `doewood_model::keyfacts`.

use std::collections::BTreeMap;

use doewood_model::{
    BlockHighlight, Card, CardDuration, CardParty, FACTS_MAX_LIMIT, FactFilter, FoundDocument,
    facts_limit, format_units, parse_units,
};
use tonic::{Request, Response, Status};
use uuid::Uuid;

use crate::pb::{
    self, DocumentCard, FindDocumentsRequest, FindDocumentsResponse, GetDocumentCardRequest,
};
use crate::validate::{self, Violations};
use crate::{Api, error};

/// Most bytes of a code, name or court filter.
const FILTER_MAX: usize = 256;

fn text(v: &mut Violations, field: &str, value: &str) -> Option<String> {
    let value = value.trim();
    if value.is_empty() {
        return None;
    }
    if value.len() > FILTER_MAX {
        v.add(field, format!("at most {FILTER_MAX} bytes"));
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

fn amount(v: &mut Violations, field: &str, value: &str) -> Option<i64> {
    if value.is_empty() {
        return None;
    }
    let parsed = parse_units(value);
    if parsed.is_none() {
        v.add(field, "not a decimal number (`1200.50`)");
    }
    parsed
}

fn id(u: Option<Uuid>) -> String {
    u.map(|u| u.to_string()).unwrap_or_default()
}

fn day(d: Option<time::Date>) -> String {
    d.map(validate::format_date).unwrap_or_default()
}

fn party(p: Option<CardParty>) -> Option<pb::CardParty> {
    p.map(|p| pb::CardParty {
        name: p.name.unwrap_or_default(),
        pib: p.pib.unwrap_or_default(),
        mb: p.mb.unwrap_or_default(),
        vat: p.vat.unwrap_or_default(),
        reg: p.reg.unwrap_or_default(),
        account: p.account.unwrap_or_default(),
        address: p.address.unwrap_or_default(),
        representative: p.representative.unwrap_or_default(),
    })
}

fn duration(d: Option<CardDuration>) -> Option<pb::CardDuration> {
    d.map(|d| pb::CardDuration {
        iso: d.iso.unwrap_or_default(),
        months: d.months,
        days: d.days,
        indefinite: d.indefinite.unwrap_or(false),
    })
}

/// The card as the API returns it: the typed fields, and every stated field with its
/// evidence.
#[allow(clippy::cast_possible_truncation)] // confidences, 0 to 1
pub(crate) fn card(blake3: String, card_json: String, c: Card) -> DocumentCard {
    let value = serde_json::to_value(&c).unwrap_or_default();
    let fields = value
        .as_object()
        .into_iter()
        .flatten()
        .filter(|(name, _)| !matches!(name.as_str(), "evidence" | "ends" | "notice_due"))
        .map(|(name, v)| {
            let ev = c.evidence.get(name);
            pb::CardField {
                name: name.clone(),
                value_json: v.to_string(),
                conf: ev.map_or(0.0, |e| e.conf as f32),
                highlights: ev
                    .map(|e| {
                        e.highlights
                            .iter()
                            .map(|h| pb::Highlight {
                                key: format!("card.{name}"),
                                page: h.page,
                                block: h.block,
                                start: h.start,
                                end: h.end,
                                quote: h.quote.clone(),
                                conf: e.conf as f32,
                            })
                            .collect()
                    })
                    .unwrap_or_default(),
            }
        })
        .collect();
    DocumentCard {
        blake3,
        card_json,
        kind: c.kind.unwrap_or_default(),
        number: c.number.unwrap_or_default(),
        supplier: party(c.supplier),
        customer: party(c.customer),
        signed: c.signed.unwrap_or_default(),
        start: c.start.unwrap_or_default(),
        end: c.end.unwrap_or_default(),
        issued: c.issued.unwrap_or_default(),
        due: c.due.unwrap_or_default(),
        term: duration(c.term),
        auto_renewal: c.auto_renewal,
        notice: duration(c.notice),
        payment_days: c.payment_days,
        payment_method: c.payment_method.unwrap_or_default(),
        amount: c.amount.unwrap_or_default(),
        amount_basis: c.amount_basis.unwrap_or_default(),
        currency: c.currency.unwrap_or_default(),
        period: c.period.unwrap_or_default(),
        subject: c.subject.unwrap_or_default(),
        category: c.category.unwrap_or_default(),
        accounts: c.accounts,
        advance: c.advance.map(|a| pb::CardAdvance {
            percent: a.percent,
            amount: a.amount.unwrap_or_default(),
            currency: a.currency.unwrap_or_default(),
        }),
        indexation: c.indexation.unwrap_or_default(),
        penalty: c.penalty.map(|p| pb::CardPenalty {
            percent: p.percent,
            per: p.per.unwrap_or_default(),
            amount: p.amount.unwrap_or_default(),
            currency: p.currency.unwrap_or_default(),
            cap_percent: p.cap_percent,
        }),
        guarantees: c.guarantees,
        liability_cap: c.liability_cap.map(|l| pb::CardLiabilityCap {
            amount: l.amount.unwrap_or_default(),
            currency: l.currency.unwrap_or_default(),
            percent: l.percent,
            basis: l.basis.unwrap_or_default(),
            months: l.months,
        }),
        confidentiality: c.confidentiality,
        sla: c.sla.map(|s| pb::CardSla {
            availability: s.availability,
            response_hours: s.response_hours,
            resolution_hours: s.resolution_hours,
        }),
        warranty: duration(c.warranty),
        governing_law: c.governing_law.unwrap_or_default(),
        court: c.court.unwrap_or_default(),
        arbitration: c.arbitration.unwrap_or_default(),
        languages: c.languages,
        annexes: c.annexes,
        amends: c.amends.unwrap_or_default(),
        ends: c.ends.unwrap_or_default(),
        notice_due: c.notice_due.unwrap_or_default(),
        fields,
    }
}

pub(crate) async fn get_card(
    api: &Api,
    request: Request<GetDocumentCardRequest>,
) -> Result<Response<DocumentCard>, Status> {
    let mut v = Violations::default();
    let hash = validate::blake3(&mut v, "blake3", &request.get_ref().blake3);
    v.check()?;
    let Some(hash) = hash else {
        return Err(Status::invalid_argument("blake3: required"));
    };
    let row = api
        .state
        .backend()?
        .get_document_facts(&hash)
        .await
        .map_err(error::model)?
        .ok_or_else(|| Status::not_found(format!("no key facts for {hash}")))?;
    let c = row.card().map_err(error::model)?;
    Ok(Response::new(card(
        hash.to_hex(),
        row.card.get().to_owned(),
        c,
    )))
}

/// The highlights of a stored document by block index, each block's in start, end, key
/// order.
pub(crate) fn by_block(hs: Vec<BlockHighlight>) -> BTreeMap<u32, Vec<pb::BlockHighlight>> {
    let mut hs = hs;
    hs.sort_by(|a, b| (a.block, a.start, a.end, &a.key).cmp(&(b.block, b.start, b.end, &b.key)));
    let mut out: BTreeMap<u32, Vec<pb::BlockHighlight>> = BTreeMap::new();
    for h in hs {
        #[allow(clippy::cast_possible_truncation)] // 0 to 1, two decimals
        out.entry(h.block).or_default().push(pb::BlockHighlight {
            key: h.key,
            start: h.start,
            end: h.end,
            conf: h.conf as f32,
        });
    }
    out
}

fn found(d: FoundDocument) -> pb::FoundDocument {
    let c = d.columns;
    pb::FoundDocument {
        blake3: c.blake3.to_hex(),
        document_id: id(d.document),
        kind: c.kind.unwrap_or_default(),
        number: c.number.unwrap_or_default(),
        subject: c.subject.unwrap_or_default(),
        supplier_id: id(d.supplier),
        supplier_name: c.supplier_name.unwrap_or_default(),
        customer_id: id(d.customer),
        customer_name: c.customer_name.unwrap_or_default(),
        category: c.category.unwrap_or_default(),
        amount: c
            .amount
            .map(|a| format_units(i128::from(a)))
            .unwrap_or_default(),
        currency: c.currency.unwrap_or_default(),
        period: c.period.unwrap_or_default(),
        signed: day(c.signed),
        ends: day(c.ends),
        notice_due: day(c.notice_due),
        auto_renewal: c.auto_renewal,
        has_penalty: c.has_penalty,
        has_guarantee: c.has_guarantee,
        governing_law: c.governing_law.unwrap_or_default(),
        court: c.court.unwrap_or_default(),
    }
}

pub(crate) async fn find(
    api: &Api,
    request: Request<FindDocumentsRequest>,
) -> Result<Response<FindDocumentsResponse>, Status> {
    let req = request.get_ref();
    let mut v = Violations::default();
    let filter = FactFilter {
        kind: text(&mut v, "kind", &req.kind).map(|k| k.to_ascii_lowercase()),
        kind_exact: req.kind_exact,
        supplier: opt_uuid(&mut v, "supplier_id", &req.supplier_id),
        customer: opt_uuid(&mut v, "customer_id", &req.customer_id),
        supplier_name: text(&mut v, "supplier_name", &req.supplier_name),
        customer_name: text(&mut v, "customer_name", &req.customer_name),
        category: text(&mut v, "category", &req.category),
        currency: text(&mut v, "currency", &req.currency).map(|c| c.to_ascii_uppercase()),
        amount_min: amount(&mut v, "amount_min", &req.amount_min),
        amount_max: amount(&mut v, "amount_max", &req.amount_max),
        end_before: validate::date(&mut v, "end_before", &req.end_before),
        end_after: validate::date(&mut v, "end_after", &req.end_after),
        notice_due_before: validate::date(&mut v, "notice_due_before", &req.notice_due_before),
        notice_due_after: validate::date(&mut v, "notice_due_after", &req.notice_due_after),
        auto_renewal: req.auto_renewal,
        has_penalty: req.has_penalty,
        has_guarantee: req.has_guarantee,
        governing_law: text(&mut v, "governing_law", &req.governing_law)
            .map(|c| c.to_ascii_uppercase()),
        court: text(&mut v, "court", &req.court),
        signed_from: validate::date(&mut v, "signed_from", &req.signed_from),
        signed_to: validate::date(&mut v, "signed_to", &req.signed_to),
    };
    if (filter.amount_min.is_some() || filter.amount_max.is_some()) && filter.currency.is_none() {
        v.add(
            "currency",
            "required with amount_min or amount_max (amounts are never compared across currencies)",
        );
    }
    if let (Some(a), Some(b)) = (filter.amount_min, filter.amount_max)
        && a > b
    {
        v.add("amount_max", "below amount_min");
    }
    for (lo, hi, field, other) in [
        (
            filter.end_after,
            filter.end_before,
            "end_before",
            "end_after",
        ),
        (
            filter.notice_due_after,
            filter.notice_due_before,
            "notice_due_before",
            "notice_due_after",
        ),
        (
            filter.signed_from,
            filter.signed_to,
            "signed_to",
            "signed_from",
        ),
    ] {
        if let (Some(a), Some(b)) = (lo, hi)
            && a > b
        {
            v.add(field, format!("before {other}"));
        }
    }
    if req.page_size > FACTS_MAX_LIMIT {
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
    let limit = facts_limit(req.page_size);
    let page = api
        .state
        .backend()?
        .find_documents(&filter, after.as_ref(), limit)
        .await
        .map_err(error::model)?;
    // The token is the last hash (opaque to clients), as for ListRecords.
    let next_page_token = if page.len() >= limit as usize {
        page.last()
            .map(|d| d.columns.blake3.to_hex())
            .unwrap_or_default()
    } else {
        String::new()
    };
    Ok(Response::new(FindDocumentsResponse {
        documents: page.into_iter().map(found).collect(),
        next_page_token,
    }))
}
