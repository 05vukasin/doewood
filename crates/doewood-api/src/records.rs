//! `RecordsService`: records by hash, and by source a page at a time.

use doewood_model::{RECORDS_PAGE_BYTES, SourceKey, records_limit, records_page_full};
use tonic::{Request, Response, Status};

use crate::pb::records_service_server::RecordsService;
use crate::pb::{GetRecordRequest, ListRecordsRequest, ListRecordsResponse, Record};
use crate::validate::{self, Violations};
use crate::{Api, convert, error};

#[tonic::async_trait]
impl RecordsService for Api {
    async fn get_record(
        &self,
        request: Request<GetRecordRequest>,
    ) -> Result<Response<Record>, Status> {
        let mut v = Violations::default();
        let hash = validate::blake3(&mut v, "blake3", &request.get_ref().blake3);
        v.check()?;
        let Some(hash) = hash else {
            return Err(Status::invalid_argument("blake3: required"));
        };
        let record = self
            .state
            .backend()?
            .get_record(&hash)
            .await
            .map_err(error::model)?
            .ok_or_else(|| Status::not_found(format!("no record for {hash}")))?;
        Ok(Response::new(convert::record(&record)))
    }

    async fn list_records(
        &self,
        request: Request<ListRecordsRequest>,
    ) -> Result<Response<ListRecordsResponse>, Status> {
        let req = request.get_ref();
        let mut v = Violations::default();
        let id = validate::source_id(&mut v, "source_id", &req.source_id);
        if req.page_size > doewood_model::RECORDS_MAX_LIMIT {
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
        let Some(id) = id else {
            return Err(Status::invalid_argument("source_id: required"));
        };
        let source = self.state.source(id).await?;
        let key = SourceKey::new(&source.endpoint, &source.bucket, source.prefix.clone())
            .map_err(error::model)?;
        let limit = records_limit(req.page_size);
        let records = self
            .state
            .backend()?
            .list_records(&key, after.as_ref(), limit, RECORDS_PAGE_BYTES)
            .await
            .map_err(error::model)?;
        // A page that ended at its size or byte budget may have a next one; the token is the
        // last hash (opaque to clients).
        let next_page_token = if records_page_full(&records, limit, RECORDS_PAGE_BYTES) {
            records
                .last()
                .map(|r| r.blake3.to_hex())
                .unwrap_or_default()
        } else {
            String::new()
        };
        Ok(Response::new(ListRecordsResponse {
            records: records.into_iter().map(convert::into_record).collect(),
            next_page_token,
        }))
    }
}
