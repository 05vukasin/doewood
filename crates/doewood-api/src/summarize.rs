//! `SummarizeService`: on-demand summaries. The messages are final; the work arrives with the
//! LLM step, so a valid request answers UNIMPLEMENTED for now.

use doewood_store::valid_provider_name;
use tonic::{Request, Response, Status};

use crate::Api;
use crate::pb::summarize_service_server::SummarizeService;
use crate::pb::{SummarizeRequest, SummarizeResponse, summarize_request::Input};
use crate::validate::{self, Violations};

/// Longest file name or object key accepted.
const NAME_MAX_LEN: usize = 1024;

#[tonic::async_trait]
impl SummarizeService for Api {
    async fn summarize(
        &self,
        request: Request<SummarizeRequest>,
    ) -> Result<Response<SummarizeResponse>, Status> {
        let req = request.get_ref();
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
        Err(Status::unimplemented(
            "Summarize is not available yet: it arrives with the LLM step (step 6); the request \
             is valid and the messages will not change",
        ))
    }
}
