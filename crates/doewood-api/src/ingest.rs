//! `IngestService`: push sources (ADR 0004). An application hands in a file's bytes with
//! `PutObject` and gets the analysis back in the same call; it moves and removes its objects
//! with `MoveLocation` and `RemoveLocation`. The work is ingest's (it has the analyzer and the
//! worker slots); the binary hands in a [`Pusher`], so this crate does not depend on it.
//!
//! `PutObject` is client-streamed: a header, then the bytes in chunks. Ingest admits the call
//! (the source, the size, a worker slot and the memory budget) before a byte is read, so a
//! burst is shed with `RESOURCE_EXHAUSTED` (with `grpc-retry-pushback-ms`, as for a shed call)
//! instead of filling memory. Each chunk must arrive within [`CHUNK_IDLE`].

use std::time::Duration;

use doewood_core::SourceId;
use doewood_model::Record;
use tonic::metadata::MetadataValue;
use tonic::{Request, Response, Status, Streaming};

use crate::limits::RETRY_PUSHBACK_HEADER;
use crate::pb::ingest_service_server::IngestService;
use crate::pb::{
    MoveLocationRequest, MoveLocationResponse, PutObjectHeader, PutObjectRequest,
    PutObjectResponse, PutStatus, RemoveLocationRequest, RemoveLocationResponse,
    put_object_request::Part,
};
use crate::summarize::BoxFuture;
use crate::validate::{self, Violations};
use crate::{Api, convert};

/// Longest key or file name accepted.
const KEY_MAX_LEN: usize = 1024;
/// Longest MIME type accepted.
const MIME_MAX_LEN: usize = 255;
/// How long `PutObject` waits for the header or the next chunk.
pub const CHUNK_IDLE: Duration = Duration::from_mins(1);

/// A push's header, checked.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PushHeader {
    /// The push source.
    pub source: SourceId,
    /// The application's key.
    pub key: String,
    /// The file name; empty for the key's last segment.
    pub name: String,
    /// The exact size of the bytes.
    pub size: u64,
}

/// What a push, or a move, did.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PushOutcome {
    /// Analyzed now.
    Analyzed,
    /// Stored content reused: only the location was written.
    Reused,
    /// The key held this content already: nothing was written.
    Unchanged,
}

/// A push's (or a move's) result.
#[derive(Clone, Debug)]
pub struct PushResult {
    /// The record, as `GetRecord` returns it.
    pub record: Record,
    /// The document's kind; empty when none.
    pub kind: String,
    /// What happened.
    pub outcome: PushOutcome,
    /// No extractor reads the format.
    pub skipped: bool,
}

/// Why a push, move or remove did not happen.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PushFailure {
    /// `NOT_FOUND`.
    NotFound(String),
    /// `INVALID_ARGUMENT`.
    InvalidArgument(String),
    /// `FAILED_PRECONDITION`.
    FailedPrecondition(String),
    /// `UNAVAILABLE`: retry.
    Unavailable(String),
    /// `RESOURCE_EXHAUSTED`: retry later (answered with a retry pushback).
    ResourceExhausted(String),
    /// `INTERNAL` (logged, answered without details).
    Internal(String),
}

/// An admitted push, waiting for its bytes.
pub trait PushUpload: Send {
    /// Ingests the bytes (exactly the header's size).
    fn finish(
        self: Box<Self>,
        bytes: Vec<u8>,
    ) -> BoxFuture<'static, Result<PushResult, PushFailure>>;
}

/// Push sources' ingest (the binary implements it with ingest).
pub trait Pusher: Send + Sync {
    /// Admits a push before its bytes are read: the source, the size, a worker slot and the
    /// memory budget.
    fn admit(&self, header: PushHeader) -> BoxFuture<'_, Result<Box<dyn PushUpload>, PushFailure>>;

    /// Moves a pushed object to another key.
    fn move_location(
        &self,
        source: SourceId,
        from: String,
        to: String,
    ) -> BoxFuture<'_, Result<PushResult, PushFailure>>;

    /// Removes a pushed object; whether the key held one.
    fn remove_location(
        &self,
        source: SourceId,
        key: String,
    ) -> BoxFuture<'_, Result<bool, PushFailure>>;
}

impl Api {
    fn status(&self, e: PushFailure) -> Status {
        match e {
            PushFailure::NotFound(m) => Status::not_found(m),
            PushFailure::InvalidArgument(m) => Status::invalid_argument(m),
            PushFailure::FailedPrecondition(m) => Status::failed_precondition(m),
            PushFailure::Unavailable(m) => Status::unavailable(m),
            PushFailure::ResourceExhausted(m) => {
                let mut s = Status::resource_exhausted(m);
                let ms = self.state.call_limits().pushback_ms();
                s.metadata_mut()
                    .insert(RETRY_PUSHBACK_HEADER, MetadataValue::from(ms));
                s
            }
            PushFailure::Internal(m) => crate::error::internal("ingest", &m),
        }
    }

    fn pusher(&self) -> Result<std::sync::Arc<dyn Pusher>, Status> {
        self.state.parts().push.clone().ok_or_else(|| {
            Status::unavailable("ingest is not running on this instance, so nothing can be put")
        })
    }
}

/// Checks a key: 1 to [`KEY_MAX_LEN`] bytes, no control characters.
fn check_key(v: &mut Violations, field: &str, key: &str) {
    if key.is_empty() || key.len() > KEY_MAX_LEN || key.contains(char::is_control) {
        v.add(
            field,
            format!("must be 1 to {KEY_MAX_LEN} bytes without control characters"),
        );
    }
}

fn check_header(h: &PutObjectHeader) -> Result<PushHeader, Status> {
    let mut v = Violations::default();
    let source = validate::source_id(&mut v, "header.source_id", &h.source_id);
    check_key(&mut v, "header.key", &h.key);
    if h.name.len() > KEY_MAX_LEN || h.name.contains(char::is_control) {
        v.add(
            "header.name",
            format!("must be at most {KEY_MAX_LEN} bytes without control characters"),
        );
    }
    if h.mime_type.len() > MIME_MAX_LEN || h.mime_type.contains(char::is_control) {
        v.add(
            "header.mime_type",
            format!("must be at most {MIME_MAX_LEN} bytes without control characters"),
        );
    }
    if h.size == 0 {
        v.add(
            "header.size",
            "must be at least 1: an empty file has nothing to analyze",
        );
    }
    v.check()?;
    Ok(PushHeader {
        source: source.ok_or_else(|| Status::invalid_argument("header.source_id: required"))?,
        key: h.key.clone(),
        name: h.name.clone(),
        size: h.size,
    })
}

/// The next message, within [`CHUNK_IDLE`].
async fn next(
    stream: &mut Streaming<PutObjectRequest>,
) -> Result<Option<PutObjectRequest>, Status> {
    match tokio::time::timeout(CHUNK_IDLE, stream.message()).await {
        Ok(m) => m,
        Err(_) => Err(Status::deadline_exceeded(format!(
            "no PutObject message for {}s",
            CHUNK_IDLE.as_secs()
        ))),
    }
}

fn put_status(o: PushOutcome) -> PutStatus {
    match o {
        PushOutcome::Analyzed => PutStatus::Analyzed,
        PushOutcome::Reused => PutStatus::Reused,
        PushOutcome::Unchanged => PutStatus::Unchanged,
    }
}

#[tonic::async_trait]
impl IngestService for Api {
    async fn put_object(
        &self,
        request: Request<Streaming<PutObjectRequest>>,
    ) -> Result<Response<PutObjectResponse>, Status> {
        let mut stream = request.into_inner();
        let Some(PutObjectRequest {
            part: Some(Part::Header(raw)),
        }) = next(&mut stream).await?
        else {
            return Err(Status::invalid_argument(
                "the first PutObject message must be the header",
            ));
        };
        let header = check_header(&raw)?;
        let size = header.size;
        let key = header.key.clone();
        let upload = self
            .pusher()?
            .admit(header)
            .await
            .map_err(|e| self.status(e))?;
        // Admitted: the size is within the source's cap and the memory budget holds it.
        let mut bytes = Vec::with_capacity(usize::try_from(size).unwrap_or(0));
        while let Some(msg) = next(&mut stream).await? {
            match msg.part {
                Some(Part::Chunk(chunk)) => {
                    if bytes.len() as u64 + chunk.len() as u64 > size {
                        return Err(Status::invalid_argument(format!(
                            "more bytes than header.size ({size}) were sent"
                        )));
                    }
                    bytes.extend_from_slice(&chunk);
                }
                Some(Part::Header(_)) => {
                    return Err(Status::invalid_argument("the header was sent twice"));
                }
                None => return Err(Status::invalid_argument("a message without a part")),
            }
        }
        if bytes.len() as u64 != size {
            return Err(Status::invalid_argument(format!(
                "{} bytes were sent, header.size is {size}",
                bytes.len()
            )));
        }
        let r = upload.finish(bytes).await.map_err(|e| self.status(e))?;
        tracing::debug!(
            key,
            mime_type = raw.mime_type,
            blake3 = %r.record.blake3,
            outcome = ?r.outcome,
            "object put"
        );
        Ok(Response::new(PutObjectResponse {
            blake3: r.record.blake3.to_hex(),
            kind: r.kind,
            status: put_status(r.outcome) as i32,
            skipped: r.skipped,
            record: Some(convert::record(&r.record)),
        }))
    }

    async fn move_location(
        &self,
        request: Request<MoveLocationRequest>,
    ) -> Result<Response<MoveLocationResponse>, Status> {
        let req = request.into_inner();
        let mut v = Violations::default();
        let source = validate::source_id(&mut v, "source_id", &req.source_id);
        check_key(&mut v, "old_key", &req.old_key);
        check_key(&mut v, "new_key", &req.new_key);
        if !req.old_key.is_empty() && req.old_key == req.new_key {
            v.add("new_key", "must differ from old_key");
        }
        v.check()?;
        let source = source.ok_or_else(|| Status::invalid_argument("source_id: required"))?;
        let r = self
            .pusher()?
            .move_location(source, req.old_key, req.new_key)
            .await
            .map_err(|e| self.status(e))?;
        Ok(Response::new(MoveLocationResponse {
            blake3: r.record.blake3.to_hex(),
            record: Some(convert::record(&r.record)),
        }))
    }

    async fn remove_location(
        &self,
        request: Request<RemoveLocationRequest>,
    ) -> Result<Response<RemoveLocationResponse>, Status> {
        let req = request.into_inner();
        let mut v = Violations::default();
        let source = validate::source_id(&mut v, "source_id", &req.source_id);
        check_key(&mut v, "key", &req.key);
        v.check()?;
        let source = source.ok_or_else(|| Status::invalid_argument("source_id: required"))?;
        let removed = self
            .pusher()?
            .remove_location(source, req.key)
            .await
            .map_err(|e| self.status(e))?;
        Ok(Response::new(RemoveLocationResponse { removed }))
    }
}
