//! `SearchService`: full text plus filters.

use doewood_model::{SEARCH_MAX_LIMIT, SearchQuery, SourceKey};
use doewood_store::valid_kind;
use tonic::{Request, Response, Status};

use crate::pb::search_service_server::SearchService;
use crate::pb::{SearchRequest, SearchResponse};
use crate::validate::{self, Violations};
use crate::{Api, convert, error};

/// Longest search text accepted.
const TEXT_MAX_LEN: usize = 1024;
/// Largest offset accepted: deeper pages are better served by narrower filters.
const MAX_OFFSET: u32 = 100_000;

#[tonic::async_trait]
impl SearchService for Api {
    async fn search(
        &self,
        request: Request<SearchRequest>,
    ) -> Result<Response<SearchResponse>, Status> {
        let req = request.get_ref();
        let mut v = Violations::default();
        if req.text.len() > TEXT_MAX_LEN {
            v.add("text", format!("longer than {TEXT_MAX_LEN} bytes"));
        }
        let company = (!req.company_id.is_empty())
            .then(|| validate::uuid(&mut v, "company_id", &req.company_id))
            .flatten();
        if !req.kind.is_empty() && !valid_kind(&req.kind) {
            v.add(
                "kind",
                "must be 1 to 64 ASCII letters, digits, '_', '-' or '.'",
            );
        }
        let date_from = validate::date(&mut v, "date_from", &req.date_from);
        let date_to = validate::date(&mut v, "date_to", &req.date_to);
        if let (Some(from), Some(to)) = (date_from, date_to)
            && to < from
        {
            v.add("date_to", "is before date_from");
        }
        for (field, value) in [
            ("amount_min", req.amount_min),
            ("amount_max", req.amount_max),
        ] {
            if value.is_some_and(|a| !a.is_finite()) {
                v.add(field, "must be a finite number");
            }
        }
        if let (Some(min), Some(max)) = (req.amount_min, req.amount_max)
            && max < min
        {
            v.add("amount_max", "is below amount_min");
        }
        let source = (!req.source_id.is_empty())
            .then(|| validate::source_id(&mut v, "source_id", &req.source_id))
            .flatten();
        if req.limit > SEARCH_MAX_LIMIT {
            v.add("limit", format!("at most {SEARCH_MAX_LIMIT}"));
        }
        if req.offset > MAX_OFFSET {
            v.add("offset", format!("at most {MAX_OFFSET}"));
        }
        v.check()?;
        let source = match source {
            Some(id) => {
                let s = self.state.source(id).await?;
                Some(
                    SourceKey::new(&s.endpoint, &s.bucket, s.prefix.clone())
                        .map_err(error::model)?,
                )
            }
            None => None,
        };
        let query = SearchQuery {
            text: (!req.text.trim().is_empty()).then(|| req.text.clone()),
            company,
            kind: (!req.kind.is_empty()).then(|| req.kind.clone()),
            date_from,
            date_to,
            amount_min: req.amount_min,
            amount_max: req.amount_max,
            source,
            limit: req.limit,
            offset: req.offset,
        };
        let hits = self
            .state
            .backend()?
            .search(&query)
            .await
            .map_err(error::model)?;
        Ok(Response::new(SearchResponse {
            hits: hits.iter().map(convert::search_hit).collect(),
        }))
    }
}
