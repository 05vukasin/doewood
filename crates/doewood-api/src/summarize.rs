//! `SummarizeService`: on-demand summaries (ADR 0001, section 6). They skip the LLM queue and
//! store nothing. The work is ingest's (it has the analyzers); the binary hands in an
//! [`OnDemand`] implementation, so this crate does not depend on them.

use std::future::Future;
use std::pin::Pin;

use doewood_core::SourceId;
use doewood_store::valid_provider_name;
use tonic::{Request, Response, Status};

use crate::Api;
use crate::pb::summarize_service_server::SummarizeService;
use crate::pb::{FactCheck, SummarizeRequest, SummarizeResponse, summarize_request::Input};
use crate::validate::{self, Violations};

/// Longest file name or object key accepted.
const NAME_MAX_LEN: usize = 1024;

/// What to summarize.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum OnDemandInput {
    /// The file's bytes and its name.
    Content {
        /// The bytes.
        bytes: Vec<u8>,
        /// The file name.
        name: String,
    },
    /// An object of one of this instance's sources.
    Location {
        /// The source.
        source: SourceId,
        /// The object key.
        key: String,
    },
}

/// A summary, as `Summarize` answers it.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct OnDemandResult {
    /// The summary.
    pub digest: String,
    /// Claims of the model's output checked against the document.
    pub checked: u32,
    /// Of those, the supported ones.
    pub supported: u32,
    /// The unsupported ones.
    pub unsupported: Vec<String>,
    /// The digest is the extractive summary.
    pub extractive: bool,
    /// The provider that wrote it; empty for an extractive summary.
    pub provider: String,
    /// Its model.
    pub model: String,
    /// The file's BLAKE3, hex.
    pub blake3: String,
    /// The file's kind.
    pub kind: String,
    /// Why the model's output was not used.
    pub warnings: Vec<String>,
}

/// Why no summary was made.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum OnDemandError {
    /// No provider is configured, or the one used is incomplete: FAILED_PRECONDITION.
    NoProvider(String),
    /// NOT_FOUND.
    NotFound(String),
    /// FAILED_PRECONDITION.
    FailedPrecondition(String),
    /// UNAVAILABLE.
    Unavailable(String),
    /// `RESOURCE_EXHAUSTED`: as many on-demand analyses as the instance allows are in flight.
    ResourceExhausted(String),
    /// INTERNAL (logged, answered without details).
    Internal(String),
}

/// A boxed future, so the trait stays object safe without a macro.
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// On-demand summaries and the LLM queue's wake-up (ingest implements it).
pub trait OnDemand: Send + Sync {
    /// Summarizes one file now with the named provider (`None`: the default).
    fn summarize(
        &self,
        input: OnDemandInput,
        provider: Option<String>,
    ) -> BoxFuture<'_, Result<OnDemandResult, OnDemandError>>;

    /// A policy or a provider changed: the queue looks at its waiting jobs again.
    fn settings_changed(&self);
}

fn status(e: OnDemandError) -> Status {
    match e {
        OnDemandError::NoProvider(m) | OnDemandError::FailedPrecondition(m) => {
            Status::failed_precondition(m)
        }
        OnDemandError::NotFound(m) => Status::not_found(m),
        OnDemandError::Unavailable(m) => Status::unavailable(m),
        OnDemandError::ResourceExhausted(m) => Status::resource_exhausted(m),
        OnDemandError::Internal(m) => crate::error::internal("summarize", &m),
    }
}

#[tonic::async_trait]
impl SummarizeService for Api {
    async fn summarize(
        &self,
        request: Request<SummarizeRequest>,
    ) -> Result<Response<SummarizeResponse>, Status> {
        let req = request.into_inner();
        let mut v = Violations::default();
        let mut source = None;
        match &req.input {
            None => v.add("input", "set content or location"),
            Some(Input::Content(bytes)) => {
                if bytes.is_empty() {
                    v.add("content", "empty");
                }
                if req.name.is_empty() {
                    v.add(
                        "name",
                        "required with content (its extension picks the reader)",
                    );
                }
            }
            Some(Input::Location(at)) => {
                source = validate::source_id(&mut v, "location.source_id", &at.source_id);
                if at.key.is_empty() || at.key.len() > NAME_MAX_LEN {
                    v.add("location.key", format!("must be 1 to {NAME_MAX_LEN} bytes"));
                }
            }
        }
        if req.name.len() > NAME_MAX_LEN || req.name.contains(char::is_control) {
            v.add(
                "name",
                format!("must be at most {NAME_MAX_LEN} bytes without control characters"),
            );
        }
        if !req.provider.is_empty() && !valid_provider_name(&req.provider) {
            v.add(
                "provider",
                "must be 1 to 64 lowercase letters, digits, '_' or '-'",
            );
        }
        v.check()?;
        if let Some(id) = source {
            self.state.source(id).await?;
        }
        if !req.provider.is_empty() {
            let name = req.provider.clone();
            if self
                .state
                .db(move |s| s.ai_provider(&name))
                .await?
                .is_none()
            {
                return Err(Status::not_found(format!(
                    "AI provider {:?} is not configured",
                    req.provider
                )));
            }
        }
        let Some(on_demand) = self.state.parts().summarize.clone() else {
            return Err(Status::unavailable(
                "ingest is not running on this instance, so nothing can be summarized",
            ));
        };
        let input = match (req.input, source) {
            (Some(Input::Content(bytes)), _) => OnDemandInput::Content {
                bytes,
                name: req.name,
            },
            (Some(Input::Location(at)), Some(source)) => OnDemandInput::Location {
                source,
                key: at.key,
            },
            _ => return Err(Status::invalid_argument("input: set content or location")),
        };
        let provider = (!req.provider.is_empty()).then_some(req.provider);
        let r = on_demand.summarize(input, provider).await.map_err(status)?;
        Ok(Response::new(SummarizeResponse {
            digest: r.digest,
            fact_check: Some(FactCheck {
                checked: r.checked,
                supported: r.supported,
                unsupported: r.unsupported,
            }),
            extractive: r.extractive,
            provider: r.provider,
            model: r.model,
            blake3: r.blake3,
            kind: r.kind,
            warnings: r.warnings,
        }))
    }
}
