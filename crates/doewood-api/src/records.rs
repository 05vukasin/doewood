//! `RecordsService`: records by hash, and by source a page at a time; a document's text with
//! block locations a page of blocks at a time (issue #166), with the highlights of the facts
//! over each block (issue #194); payments per document and spend per supplier (issue #193, in
//! [`crate::payments`]); key-facts cards and the documents found by their key facts (issue
//! #194, in [`crate::keyfacts`]).

use std::collections::BTreeMap;
use std::ops::ControlFlow;

use doewood_model::{
    DocumentText, RECORDS_PAGE_BYTES, SourceKey, TextBlock, records_limit, records_page_full,
};
use tonic::{Request, Response, Status};

use crate::pb::records_service_server::RecordsService;
use crate::pb::{
    DocumentCard, FindDocumentsRequest, FindDocumentsResponse, GetDocumentCardRequest,
    GetDocumentTextRequest, GetDocumentTextResponse, GetRecordRequest, ListPaymentsRequest,
    ListPaymentsResponse, ListRecordsRequest, ListRecordsResponse, Record, SupplierSpendRequest,
    SupplierSpendResponse,
};
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

    async fn get_document_text(
        &self,
        request: Request<GetDocumentTextRequest>,
    ) -> Result<Response<GetDocumentTextResponse>, Status> {
        let req = request.get_ref();
        let mut v = Violations::default();
        let hash = validate::blake3(&mut v, "blake3", &req.blake3);
        if req.page_size > TEXT_MAX_PAGE_SIZE {
            v.add("page_size", "at most 1000");
        }
        let token = if req.page_token.is_empty() {
            None
        } else {
            let parsed = parse_text_token(&req.page_token);
            if parsed.is_none() {
                v.add("page_token", "not a token from a previous page");
            }
            parsed
        };
        v.check()?;
        let Some(hash) = hash else {
            return Err(Status::invalid_argument("blake3: required"));
        };
        let backend = self.state.backend()?;
        let (stored, facts) = tokio::join!(backend.get_document_text(&hash), async {
            if req.skip_highlights {
                Ok(None)
            } else {
                backend.get_document_facts(&hash).await
            }
        });
        let stored = stored
            .map_err(error::model)?
            .ok_or_else(|| Status::not_found(format!("no document text for {hash}")))?;
        let mut highlights = match facts.map_err(error::model)? {
            Some(row) => crate::keyfacts::by_block(row.highlights().map_err(error::model)?),
            None => BTreeMap::new(),
        };
        let from = match token {
            None => 0,
            Some((at, _)) if at != stored.updated_at => {
                return Err(Status::failed_precondition(
                    "page_token: the text was stored again since the first page; start over",
                ));
            }
            Some((_, from)) => from,
        };
        let text = DocumentText::from_body(&stored.body).map_err(error::model)?;
        let total = text.block_count();
        if from > total {
            return Err(Status::invalid_argument(
                "page_token: not a token from a previous page",
            ));
        }
        let limit = match req.page_size {
            0 => TEXT_DEFAULT_PAGE_SIZE,
            n => n as usize,
        };
        let (pages, end) = text_page(&text, from, limit, TEXT_PAGE_BYTES, &mut highlights);
        let next_page_token = if end < total {
            format!("{}.{end}", stored.updated_at)
        } else {
            String::new()
        };
        Ok(Response::new(GetDocumentTextResponse {
            blake3: hash.to_hex(),
            extractor: text.extractor,
            extractor_version: text.extractor_version,
            truncated: text.truncated,
            pages,
            total_blocks: u32::try_from(total).unwrap_or(u32::MAX),
            next_page_token,
            updated_at: stored.updated_at,
        }))
    }

    async fn list_payments(
        &self,
        request: Request<ListPaymentsRequest>,
    ) -> Result<Response<ListPaymentsResponse>, Status> {
        crate::payments::list(self, request).await
    }

    async fn supplier_spend(
        &self,
        request: Request<SupplierSpendRequest>,
    ) -> Result<Response<SupplierSpendResponse>, Status> {
        crate::payments::spend(self, request).await
    }

    async fn get_document_card(
        &self,
        request: Request<GetDocumentCardRequest>,
    ) -> Result<Response<DocumentCard>, Status> {
        crate::keyfacts::get_card(self, request).await
    }

    async fn find_documents(
        &self,
        request: Request<FindDocumentsRequest>,
    ) -> Result<Response<FindDocumentsResponse>, Status> {
        crate::keyfacts::find(self, request).await
    }
}

/// Largest `page_size` of `GetDocumentText`, in blocks.
const TEXT_MAX_PAGE_SIZE: u32 = 1000;
/// `GetDocumentText`'s page size for `page_size` 0.
const TEXT_DEFAULT_PAGE_SIZE: usize = 200;
/// The byte budget of a `GetDocumentText` page: 1 MiB of block text, heading paths and sheet
/// names. A page ends with the block that reaches it, so with the stored text's cap (2 MiB by
/// default) a page stays well under the 4 MiB gRPC clients accept by default.
const TEXT_PAGE_BYTES: usize = 1 << 20;

/// A `GetDocumentText` page token: `<updated_at>.<first block>`, so a token from a text that
/// was stored again since is told apart.
fn parse_text_token(token: &str) -> Option<(i64, usize)> {
    let (at, from) = token.split_once('.')?;
    Some((at.parse().ok()?, from.parse().ok()?))
}

/// The blocks `from..end` of `text` (at most `limit`, ending with the block that brings their
/// bytes to `max_bytes`), grouped by page, and `end`. A page without blocks goes with the
/// response that holds the block after it, or with the last response. Each block takes its
/// highlights out of `highlights` (by block index).
fn text_page(
    text: &DocumentText,
    from: usize,
    limit: usize,
    max_bytes: usize,
    highlights: &mut BTreeMap<u32, Vec<crate::pb::BlockHighlight>>,
) -> (Vec<crate::pb::TextPage>, usize) {
    let total = text.block_count();
    // Where each page's blocks start among all the blocks.
    let mut starts = Vec::with_capacity(text.pages.len() + 1);
    let mut n = 0;
    for p in &text.pages {
        starts.push(n);
        n += p.blocks.len();
    }
    starts.push(n);
    let mut blocks: Vec<Vec<crate::pb::TextBlock>> = vec![Vec::new(); text.pages.len()];
    let (mut end, mut bytes, mut pos) = (from, 0usize, 0usize);
    text.for_each_block(|v| {
        if v.index < from {
            return ControlFlow::Continue(());
        }
        if end > from && (end - from >= limit || bytes >= max_bytes) {
            return ControlFlow::Break(());
        }
        while starts[pos + 1] <= v.index {
            pos += 1;
        }
        let mut b = text_block(v.index, v.page, v.block, v.heading_path);
        b.highlights = highlights.remove(&b.index).unwrap_or_default();
        bytes +=
            b.text.len() + b.sheet.len() + b.heading_path.iter().map(String::len).sum::<usize>();
        blocks[pos].push(b);
        end = v.index + 1;
        ControlFlow::Continue(())
    });
    let pages = text
        .pages
        .iter()
        .zip(blocks)
        .enumerate()
        .filter_map(|(i, (page, blocks))| {
            let empty_here = page.blocks.is_empty()
                && ((from..end).contains(&starts[i]) || (starts[i] == total && end == total));
            (!blocks.is_empty() || empty_here).then(|| crate::pb::TextPage {
                index: page.index,
                sheet: page.sheet.clone().unwrap_or_default(),
                origin: page.origin.as_str().to_owned(),
                blocks,
            })
        })
        .collect();
    (pages, end)
}

fn text_block(
    index: usize,
    page: &doewood_model::TextPage,
    b: &TextBlock,
    path: &[&str],
) -> crate::pb::TextBlock {
    crate::pb::TextBlock {
        index: u32::try_from(index).unwrap_or(u32::MAX),
        kind: b.kind.as_str().to_owned(),
        text: b.text.clone(),
        page: page.index,
        heading_path: path.iter().map(|&h| h.to_owned()).collect(),
        sheet: page.sheet.clone().unwrap_or_default(),
        row: b.row,
        zone: b.zone.map(|z| z.as_str().to_owned()).unwrap_or_default(),
        level: b.level.map_or(0, u32::from),
        table: b.table,
        highlights: Vec::new(),
    }
}

#[cfg(test)]
mod tests {
    use doewood_model::{BlockKind, TEXT_MAX_BYTES, TextBuilder, TextOrigin, TextZone};

    use super::*;

    fn doc() -> DocumentText {
        let mut b = TextBuilder::new("pdf.extract", "1", TEXT_MAX_BYTES);
        let block = |kind, text: &str, level| TextBlock {
            kind,
            text: text.to_owned(),
            zone: Some(TextZone::Body),
            level,
            table: None,
            row: None,
        };
        b.page(0, None, TextOrigin::None);
        b.page(1, None, TextOrigin::Native);
        b.block(block(BlockKind::Heading, "Terms", Some(1)));
        for i in 0..5 {
            b.block(block(BlockKind::Paragraph, &format!("p{i}"), None));
        }
        b.page(2, None, TextOrigin::None);
        b.page(3, None, TextOrigin::Native);
        b.block(block(BlockKind::Paragraph, "last", None));
        b.page(4, None, TextOrigin::None);
        b.finish()
    }

    /// Walking every page sees every block once, in order, and every page once.
    #[test]
    fn pages_cover_every_block_and_page_once() {
        let text = doc();
        for (limit, max_bytes) in [
            (1, usize::MAX),
            (2, usize::MAX),
            (3, 4),
            (200, 1),
            (200, 1 << 20),
        ] {
            let (mut from, mut blocks, mut pages) = (0, Vec::new(), Vec::new());
            loop {
                let (page, end) = text_page(&text, from, limit, max_bytes, &mut BTreeMap::new());
                assert!(
                    end > from || end == text.block_count(),
                    "{limit} {max_bytes}"
                );
                for p in page {
                    if pages.last() != Some(&p.index) {
                        pages.push(p.index);
                    }
                    blocks.extend(
                        p.blocks
                            .into_iter()
                            .map(|b| (b.index, b.text, b.heading_path)),
                    );
                }
                if end == text.block_count() {
                    break;
                }
                from = end;
            }
            assert_eq!(pages, [0, 1, 2, 3, 4], "{limit} {max_bytes}");
            let idx: Vec<u32> = blocks.iter().map(|b| b.0).collect();
            assert_eq!(idx, (0..7).collect::<Vec<_>>(), "{limit} {max_bytes}");
            assert_eq!(blocks[6].1, "last");
            assert_eq!(blocks[6].2, ["Terms"]);
            assert!(blocks[0].2.is_empty());
        }
    }

    #[test]
    fn an_empty_text_has_one_page_of_pages() {
        let mut b = TextBuilder::new("pdf.extract", "1", TEXT_MAX_BYTES);
        b.page(0, None, TextOrigin::None);
        b.page(1, None, TextOrigin::None);
        let (pages, end) = text_page(&b.finish(), 0, 200, 1 << 20, &mut BTreeMap::new());
        assert_eq!((pages.len(), end), (2, 0));
    }

    #[test]
    fn tokens() {
        assert_eq!(parse_text_token("1700000000.17"), Some((1_700_000_000, 17)));
        for bad in ["", "17", "a.1", "1.b", "1.-1"] {
            assert_eq!(parse_text_token(bad), None, "{bad}");
        }
    }
}
