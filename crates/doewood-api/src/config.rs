//! `ConfigService`: sources, LLM policies, AI providers, ingest's and enrichment's settings.
//! Every change applies without a restart; secrets are write-only.

use doewood_core::{InstanceId, SourceId};
use doewood_model::SourceKey;
use doewood_store::{
    AI_PRESETS, AiProviderInput, ApiKey, ApiSettingsPatch, CredentialsChange,
    DEADLINE_SECONDS_LIMIT, IngestSettingsPatch, KeyChange, MAX_CALLS_IN_FLIGHT_LIMIT, NewSource,
    S3Credentials, SIMILAR_MAX_BUCKET_RANGE, SUMMARIZE_MAX_CONCURRENT_RANGE, SourcePatch,
    SourceState, SqsInletInput, UrlStyle, valid_deadline_seconds, valid_max_calls_in_flight,
    valid_min_jaccard, valid_provider_name, valid_similar_max_bucket,
    valid_summarize_max_concurrent,
};
use tonic::{Request, Response, Status};
use tracing::{info, warn};

use crate::pb::config_service_server::ConfigService;
use crate::pb::{
    AddSourceRequest, AiProvider, ApiSettings, EnrichmentSettings, GetApiSettingsRequest,
    GetEnrichmentSettingsRequest, GetIngestSettingsRequest, GetSourceRequest, IngestSettings,
    ListAiProvidersRequest, ListAiProvidersResponse, ListSourcesRequest, ListSourcesResponse,
    PauseSourceRequest, ReleaseSourceClaimRequest, ReleaseSourceClaimResponse, RemoveSourceRequest,
    RemoveSourceResponse, RemoveSqsInletRequest, ResumeSourceRequest, SetAiProviderRequest,
    SetApiSettingsRequest, SetEnrichmentSettingsRequest, SetIngestSettingsRequest,
    SetLlmPolicyRequest, SetSqsInletRequest, Source, UpdateSourceRequest,
};
use crate::validate::{self, Violations};
use crate::{Api, ApiState, claims, convert, enrichment, error};

/// Longest model name accepted.
const MODEL_MAX_LEN: usize = 256;
/// Longest API key accepted.
const API_KEY_MAX_LEN: usize = 8 * 1024;

fn check_add(req: &AddSourceRequest) -> Result<(NewSource, Option<S3Credentials>), Status> {
    let mut v = Violations::default();
    let url_style = validate::url_style(&mut v, "url_style", req.url_style);
    let endpoint = validate::http_url(&mut v, "endpoint", &req.endpoint);
    validate::bucket(&mut v, "bucket", &req.bucket, url_style);
    validate::prefix(&mut v, "prefix", &req.prefix);
    let region = validate::region(&mut v, "region", &req.region);
    let credentials = req
        .credentials
        .as_ref()
        .and_then(|c| validate::credentials(&mut v, "credentials", c));
    let scan_interval =
        validate::scan_interval(&mut v, "scan_interval_seconds", req.scan_interval_seconds);
    let max_object_bytes =
        validate::max_object_bytes(&mut v, "max_object_bytes", req.max_object_bytes);
    let llm_policy = validate::llm_policy(&mut v, "llm_policy", req.llm_policy.as_ref());
    v.check()?;
    Ok((
        NewSource {
            endpoint: endpoint.unwrap_or_default(),
            bucket: req.bucket.clone(),
            prefix: req.prefix.clone(),
            region,
            url_style,
            max_object_bytes,
            scan_interval,
            state: if req.paused {
                SourceState::Paused
            } else {
                SourceState::Active
            },
            llm_policy,
            skip_known_copies: req.skip_known_copies,
        },
        credentials,
    ))
}

/// Adds a source: validates it, claims it in the shared store (failing with ALREADY_EXISTS
/// naming the owner when another instance has it), stores it with its sealed credentials, and
/// starts its scanner when it is active. `ConfigService.AddSource` and the `doewood dev
/// add-source` command both go through here.
pub async fn add_source(state: &ApiState, req: &AddSourceRequest) -> Result<Source, Status> {
    let (new, credentials) = check_add(req)?;
    let _guard = state.lock_sources().await;
    let (endpoint, bucket, prefix) = (new.endpoint.clone(), new.bucket.clone(), new.prefix.clone());
    if let Some(existing) = state
        .db(move |s| s.source_by_key(&endpoint, &bucket, &prefix))
        .await?
    {
        return Err(Status::already_exists(format!(
            "this instance already has this source ({})",
            existing.id
        )));
    }
    let backend = state.backend()?;
    let key =
        SourceKey::new(&new.endpoint, &new.bucket, new.prefix.clone()).map_err(error::model)?;
    backend
        .claim_source(&key, state.instance())
        .await
        .map_err(error::model)?;
    let added = state
        .db(move |s| {
            let id = s.add_source(&new, credentials.as_ref())?;
            s.source(id)?
                .ok_or_else(|| doewood_store::StoreError::NoSuchSource(id.to_string()))
        })
        .await;
    let source = match added {
        Ok(s) => s,
        Err(status) => {
            // Nothing owns the claim now: give it back.
            if let Err(e) = backend.release_source(&key, state.instance()).await {
                warn!(source = %key, error = %e, "could not release the claim of a source that was not added");
            }
            return Err(status);
        }
    };
    info!(
        source = %source.id,
        bucket = %source.bucket,
        prefix = %source.prefix,
        state = %source.state,
        "source added"
    );
    if let Some(d) = state.discovery() {
        d.start_source(&source);
    }
    state.progress().touch();
    Ok(state.source_pb(&source))
}

fn check_release(req: &ReleaseSourceClaimRequest) -> Result<(SourceKey, InstanceId), Status> {
    let mut v = Violations::default();
    let endpoint = validate::http_url(&mut v, "endpoint", &req.endpoint);
    // Any bucket a source could have been added with (path style accepts the most).
    validate::bucket(&mut v, "bucket", &req.bucket, UrlStyle::Path);
    validate::prefix(&mut v, "prefix", &req.prefix);
    let owner = match InstanceId::new(req.owner.trim()) {
        Ok(o) => Some(o),
        Err(_) if req.owner.trim().is_empty() => {
            v.add("owner", "required: the instance that holds the claim");
            None
        }
        Err(e) => {
            v.add("owner", e.to_string());
            None
        }
    };
    v.check()?;
    let owner = owner.ok_or_else(|| Status::invalid_argument("invalid argument: owner"))?;
    let key = SourceKey::new(
        endpoint.unwrap_or_default(),
        &req.bucket,
        req.prefix.clone(),
    )
    .map_err(error::model)?;
    Ok((key, owner))
}

fn check_id(field: &str, value: &str) -> Result<SourceId, Status> {
    let mut v = Violations::default();
    let id = validate::source_id(&mut v, field, value);
    v.check()?;
    id.ok_or_else(|| Status::invalid_argument(format!("{field}: required")))
}

fn check_update(req: &UpdateSourceRequest) -> Result<(SourceId, SourcePatch, KeyUpdate), Status> {
    let mut v = Violations::default();
    let id = validate::source_id(&mut v, "source_id", &req.source_id);
    let patch = SourcePatch {
        region: req
            .region
            .as_deref()
            .map(|r| validate::region(&mut v, "region", r)),
        url_style: req
            .url_style
            .map(|s| validate::url_style(&mut v, "url_style", s)),
        scan_interval: req
            .scan_interval_seconds
            .map(|s| validate::scan_interval(&mut v, "scan_interval_seconds", s)),
        max_object_bytes: req
            .max_object_bytes
            .map(|b| validate::max_object_bytes(&mut v, "max_object_bytes", b)),
        llm_policy: None,
        skip_known_copies: req.skip_known_copies,
    };
    let credentials = req
        .credentials
        .as_ref()
        .and_then(|c| validate::credentials(&mut v, "credentials", c));
    if req.clear_credentials && req.credentials.is_some() {
        v.add("clear_credentials", "cannot be combined with credentials");
    }
    v.check()?;
    let key = match (credentials, req.clear_credentials) {
        (Some(c), _) => KeyUpdate::Set(c),
        (None, true) => KeyUpdate::Clear,
        (None, false) => KeyUpdate::Keep,
    };
    Ok((id.unwrap_or_else(SourceId::generate), patch, key))
}

fn check_sqs(
    req: &SetSqsInletRequest,
) -> Result<(SourceId, SqsInletInput, CredentialsChange), Status> {
    let mut v = Violations::default();
    let id = validate::source_id(&mut v, "source_id", &req.source_id);
    let queue_url = validate::queue_url(&mut v, "queue_url", &req.queue_url);
    let region = validate::region(&mut v, "region", &req.region);
    let credentials = req
        .credentials
        .as_ref()
        .and_then(|c| validate::credentials(&mut v, "credentials", c));
    if req.clear_credentials && req.credentials.is_some() {
        v.add("clear_credentials", "cannot be combined with credentials");
    }
    v.check()?;
    let change = match (credentials, req.clear_credentials) {
        (Some(c), _) => CredentialsChange::Set(c),
        (None, true) => CredentialsChange::Clear,
        (None, false) => CredentialsChange::Keep,
    };
    Ok((
        id.unwrap_or_else(SourceId::generate),
        SqsInletInput {
            queue_url: queue_url.unwrap_or_default(),
            region,
            enabled: !req.disabled,
        },
        change,
    ))
}

enum KeyUpdate {
    Keep,
    Set(S3Credentials),
    Clear,
}

impl Api {
    async fn set_state(
        &self,
        id: SourceId,
        state: SourceState,
    ) -> Result<doewood_store::Source, Status> {
        let _guard = self.state.lock_sources().await;
        let current = self.state.source(id).await?;
        let previous = current.state;
        // With a shared store, a source is scanned only under this instance's claim.
        let claim_now = state.is_scanned() && self.state.shared_claims();
        if claim_now && self.state.claims_checked() {
            self.claim_for_scan(&current).await?;
        }
        if previous != state {
            self.state
                .db(move |s| s.set_source_state(id, state, None))
                .await?;
        }
        let source = self.state.source(id).await?;
        if claim_now && !self.state.claims_checked() {
            // The start-up claim pass has not run yet: it claims the source and starts it.
            info!(
                source = %id,
                state = %source.state,
                "source state set; scanning starts once its claim is checked"
            );
            self.state.progress().touch();
            return Ok(source);
        }
        if let Some(d) = self.state.discovery() {
            match (state.is_scanned(), previous) {
                // A failing source waits out its retry delay: resuming retries now.
                (true, SourceState::Error) => d.restart_source(&source),
                (true, _) => d.start_source(&source),
                (false, _) => d.stop_source(id),
            };
        }
        info!(source = %id, state = %source.state, "source state set");
        self.state.progress().touch();
        Ok(source)
    }

    /// Claims the source for this instance before it is scanned. Another owner: the source is
    /// marked (state `error`, the owner in the reason) and the call fails with ALREADY_EXISTS.
    async fn claim_for_scan(&self, source: &doewood_store::Source) -> Result<(), Status> {
        let backend = self.state.backend()?;
        let key = SourceKey::new(&source.endpoint, &source.bucket, source.prefix.clone())
            .map_err(error::model)?;
        match backend.claim_source(&key, self.state.instance()).await {
            Ok(_) => {
                self.state.store().claims().set_mine(source.id);
                Ok(())
            }
            Err(doewood_model::Error::SourceClaimed { owner, key }) => {
                // Marked as an active source would be: resuming asked for a scan.
                let mut marked = source.clone();
                marked.state = SourceState::Error;
                claims::mark_conflict(&self.state, &backend, &marked, &key, &owner)
                    .await
                    .map_err(Status::unavailable)?;
                self.state.progress().touch();
                Err(error::model(doewood_model::Error::SourceClaimed {
                    owner,
                    key,
                }))
            }
            Err(e) => Err(error::model(e)),
        }
    }
}

#[tonic::async_trait]
impl ConfigService for Api {
    async fn add_source(
        &self,
        request: Request<AddSourceRequest>,
    ) -> Result<Response<Source>, Status> {
        add_source(&self.state, request.get_ref())
            .await
            .map(Response::new)
    }

    async fn update_source(
        &self,
        request: Request<UpdateSourceRequest>,
    ) -> Result<Response<Source>, Status> {
        let (id, patch, key) = check_update(request.get_ref())?;
        let _guard = self.state.lock_sources().await;
        let current = self.state.source(id).await?;
        if patch.url_style == Some(UrlStyle::VirtualHost) {
            let mut v = Violations::default();
            validate::bucket(&mut v, "url_style", &current.bucket, UrlStyle::VirtualHost);
            v.check()?;
        }
        let source = self
            .state
            .db(move |s| {
                match &key {
                    KeyUpdate::Set(c) => s.set_source_credentials(id, c)?,
                    KeyUpdate::Clear => s.clear_source_credentials(id)?,
                    KeyUpdate::Keep => {}
                }
                s.update_source(id, &patch)
            })
            .await?;
        if let Some(d) = self.state.discovery()
            && source.state.is_scanned()
            // With shared claims, a source that is not scanned (claimed elsewhere, or waiting
            // for the start-up claim pass) is not started here.
            && (!self.state.shared_claims() || d.is_running(id))
        {
            // The new options (interval, credentials) apply now, not after the next sleep.
            d.restart_source(&source);
        }
        info!(source = %id, "source updated");
        self.state.progress().touch();
        Ok(Response::new(self.state.source_pb(&source)))
    }

    async fn pause_source(
        &self,
        request: Request<PauseSourceRequest>,
    ) -> Result<Response<Source>, Status> {
        let id = check_id("source_id", &request.get_ref().source_id)?;
        let source = self.set_state(id, SourceState::Paused).await?;
        Ok(Response::new(self.state.source_pb(&source)))
    }

    async fn resume_source(
        &self,
        request: Request<ResumeSourceRequest>,
    ) -> Result<Response<Source>, Status> {
        let id = check_id("source_id", &request.get_ref().source_id)?;
        let source = self.set_state(id, SourceState::Active).await?;
        Ok(Response::new(self.state.source_pb(&source)))
    }

    async fn remove_source(
        &self,
        request: Request<RemoveSourceRequest>,
    ) -> Result<Response<RemoveSourceResponse>, Status> {
        let id = check_id("source_id", &request.get_ref().source_id)?;
        let _guard = self.state.lock_sources().await;
        let source = self.state.source(id).await?;
        // The claim goes first: if the shared store is away, nothing is removed and the
        // call can be repeated, instead of leaving a claim nobody owns.
        let backend = self.state.backend()?;
        let key = SourceKey::new(&source.endpoint, &source.bucket, source.prefix.clone())
            .map_err(error::model)?;
        let claim_released = match backend.release_source(&key, self.state.instance()).await {
            Ok(released) => released,
            Err(doewood_model::Error::SourceClaimed { owner, .. }) => {
                warn!(source = %id, owner = %owner, "another instance owns the source; removing the local copy only");
                false
            }
            Err(e) => return Err(error::model(e)),
        };
        if let Some(d) = self.state.discovery() {
            d.stop_source(id);
        }
        self.state.db(move |s| s.delete_source(id)).await?;
        self.state.store().claims().set_mine(id);
        self.state.progress().forget(id);
        info!(source = %id, claim_released, "source removed");
        Ok(Response::new(RemoveSourceResponse { claim_released }))
    }

    async fn release_source_claim(
        &self,
        request: Request<ReleaseSourceClaimRequest>,
    ) -> Result<Response<ReleaseSourceClaimResponse>, Status> {
        let peer = request.remote_addr();
        let (key, owner) = check_release(request.get_ref())?;
        claims::release_claim(&self.state, &key, &owner, peer)
            .await
            .map(Response::new)
    }

    async fn list_sources(
        &self,
        _request: Request<ListSourcesRequest>,
    ) -> Result<Response<ListSourcesResponse>, Status> {
        let sources = self.state.db(doewood_store::Store::sources).await?;
        Ok(Response::new(ListSourcesResponse {
            sources: sources.iter().map(|s| self.state.source_pb(s)).collect(),
        }))
    }

    async fn get_source(
        &self,
        request: Request<GetSourceRequest>,
    ) -> Result<Response<Source>, Status> {
        let id = check_id("source_id", &request.get_ref().source_id)?;
        let source = self.state.source(id).await?;
        Ok(Response::new(self.state.source_pb(&source)))
    }

    async fn set_llm_policy(
        &self,
        request: Request<SetLlmPolicyRequest>,
    ) -> Result<Response<Source>, Status> {
        let req = request.get_ref();
        let mut v = Violations::default();
        let id = validate::source_id(&mut v, "source_id", &req.source_id);
        if req.policy.is_none() {
            v.add("policy", "required");
        }
        let policy = validate::llm_policy(&mut v, "policy", req.policy.as_ref());
        v.check()?;
        let id = id.unwrap_or_else(SourceId::generate);
        let patch = SourcePatch {
            llm_policy: Some(policy),
            ..SourcePatch::default()
        };
        let _guard = self.state.lock_sources().await;
        let source = self.state.db(move |s| s.update_source(id, &patch)).await?;
        info!(source = %id, mode = source.llm_policy.mode.name(), "LLM policy set");
        self.state.llm_settings_changed();
        Ok(Response::new(self.state.source_pb(&source)))
    }

    async fn set_sqs_inlet(
        &self,
        request: Request<SetSqsInletRequest>,
    ) -> Result<Response<Source>, Status> {
        let (id, input, change) = check_sqs(request.get_ref())?;
        let _guard = self.state.lock_sources().await;
        let source = self
            .state
            .db(move |s| {
                s.set_sqs_inlet(id, &input, &change)?;
                s.source(id)?
                    .ok_or_else(|| doewood_store::StoreError::NoSuchSource(id.to_string()))
            })
            .await?;
        // Only the inlet restarts, and only where the scanner runs (claimed, not paused).
        let running = self
            .state
            .discovery()
            .is_some_and(|d| d.restart_inlet(&source));
        let inlet = source.sqs.as_ref();
        // The region and on/off; never the credentials, nor the queue URL's account.
        info!(
            source = %id,
            region = inlet.map_or("", doewood_store::SqsInlet::region),
            enabled = inlet.is_some_and(|i| i.enabled),
            own_credentials = inlet.is_some_and(|i| i.credentials_set),
            running,
            "SQS inlet set"
        );
        Ok(Response::new(self.state.source_pb(&source)))
    }

    async fn remove_sqs_inlet(
        &self,
        request: Request<RemoveSqsInletRequest>,
    ) -> Result<Response<Source>, Status> {
        let id = check_id("source_id", &request.get_ref().source_id)?;
        let _guard = self.state.lock_sources().await;
        let (removed, source) = self
            .state
            .db(move |s| {
                let removed = s.remove_sqs_inlet(id)?;
                let source = s
                    .source(id)?
                    .ok_or_else(|| doewood_store::StoreError::NoSuchSource(id.to_string()))?;
                Ok((removed, source))
            })
            .await?;
        if let Some(d) = self.state.discovery() {
            d.restart_inlet(&source);
        }
        info!(source = %id, removed, "SQS inlet removed");
        Ok(Response::new(self.state.source_pb(&source)))
    }

    async fn set_ai_provider(
        &self,
        request: Request<SetAiProviderRequest>,
    ) -> Result<Response<AiProvider>, Status> {
        let req = request.into_inner();
        let mut v = Violations::default();
        if !AI_PRESETS.contains(&req.preset.as_str()) {
            v.add(
                "preset",
                format!("must be one of {}", AI_PRESETS.join(", ")),
            );
        }
        let name = if req.name.is_empty() {
            req.preset.clone()
        } else {
            req.name.clone()
        };
        if !req.name.is_empty() && !valid_provider_name(&name) {
            v.add(
                "name",
                "must be 1 to 64 lowercase letters, digits, '_' or '-'",
            );
        }
        if req.model.trim().is_empty() {
            v.add("model", "required");
        } else if req.model.len() > MODEL_MAX_LEN || req.model.contains(char::is_control) {
            v.add(
                "model",
                "must be at most 256 bytes without control characters",
            );
        }
        let base_url = if req.base_url.is_empty() {
            if req.preset == "openai_compatible" {
                v.add("base_url", "required for the openai_compatible preset");
            }
            None
        } else {
            validate::http_url(&mut v, "base_url", &req.base_url)
        };
        if req.api_key.len() > API_KEY_MAX_LEN {
            v.add("api_key", "longer than 8 KiB");
        } else if req.api_key.contains(char::is_control) {
            v.add(
                "api_key",
                "must not contain control characters or line breaks",
            );
        }
        if req.clear_api_key && !req.api_key.is_empty() {
            v.add("clear_api_key", "cannot be combined with api_key");
        }
        v.check()?;
        let key = if req.clear_api_key {
            KeyChange::Clear
        } else if req.api_key.is_empty() {
            KeyChange::Keep
        } else {
            KeyChange::Set(ApiKey::new(req.api_key).map_err(error::store)?)
        };
        let input = AiProviderInput {
            name,
            preset: req.preset,
            model: req.model.trim().to_owned(),
            base_url,
        };
        let make_default = req.make_default;
        let provider = self
            .state
            .db(move |s| s.set_ai_provider(&input, &key, make_default))
            .await?;
        info!(
            provider = %provider.name,
            preset = %provider.preset,
            api_key_set = provider.api_key_set,
            "AI provider set"
        );
        self.state.llm_settings_changed();
        Ok(Response::new(convert::ai_provider(&provider)))
    }

    async fn list_ai_providers(
        &self,
        _request: Request<ListAiProvidersRequest>,
    ) -> Result<Response<ListAiProvidersResponse>, Status> {
        let providers = self.state.db(doewood_store::Store::ai_providers).await?;
        Ok(Response::new(ListAiProvidersResponse {
            providers: providers.iter().map(convert::ai_provider).collect(),
            presets: AI_PRESETS.iter().map(|p| (*p).to_owned()).collect(),
        }))
    }

    async fn get_ingest_settings(
        &self,
        _request: Request<GetIngestSettingsRequest>,
    ) -> Result<Response<IngestSettings>, Status> {
        let settings = self.state.db(doewood_store::Store::ingest_settings).await?;
        Ok(Response::new(convert::ingest_settings(settings)))
    }

    async fn set_ingest_settings(
        &self,
        request: Request<SetIngestSettingsRequest>,
    ) -> Result<Response<IngestSettings>, Status> {
        let req = request.get_ref();
        let mut v = Violations::default();
        if let Some(x) = req.similar_min_jaccard
            && !valid_min_jaccard(x)
        {
            v.add("similar_min_jaccard", "must be above 0 and at most 1");
        }
        if let Some(n) = req.similar_max_bucket
            && !valid_similar_max_bucket(n)
        {
            v.add(
                "similar_max_bucket",
                format!(
                    "must be {} to {}",
                    SIMILAR_MAX_BUCKET_RANGE.start(),
                    SIMILAR_MAX_BUCKET_RANGE.end()
                ),
            );
        }
        if let Some(n) = req.summarize_max_concurrent
            && !valid_summarize_max_concurrent(n)
        {
            v.add(
                "summarize_max_concurrent",
                format!(
                    "must be {} to {}",
                    SUMMARIZE_MAX_CONCURRENT_RANGE.start(),
                    SUMMARIZE_MAX_CONCURRENT_RANGE.end()
                ),
            );
        }
        v.check()?;
        let patch = IngestSettingsPatch {
            similar_min_jaccard: req.similar_min_jaccard,
            similar_max_bucket: req.similar_max_bucket,
            summarize_max_concurrent: req.summarize_max_concurrent,
        };
        let settings = self
            .state
            .db(move |s| s.set_ingest_settings(&patch))
            .await?;
        info!(
            similar_min_jaccard = settings.similar_min_jaccard,
            similar_max_bucket = settings.similar_max_bucket,
            summarize_max_concurrent = settings.summarize_max_concurrent,
            "ingest settings set"
        );
        Ok(Response::new(convert::ingest_settings(settings)))
    }

    async fn get_api_settings(
        &self,
        _request: Request<GetApiSettingsRequest>,
    ) -> Result<Response<ApiSettings>, Status> {
        let settings = self.state.db(doewood_store::Store::api_settings).await?;
        Ok(Response::new(convert::api_settings(settings)))
    }

    async fn set_api_settings(
        &self,
        request: Request<SetApiSettingsRequest>,
    ) -> Result<Response<ApiSettings>, Status> {
        let req = request.get_ref();
        let mut v = Violations::default();
        if let Some(n) = req.max_calls_in_flight
            && !valid_max_calls_in_flight(n)
        {
            v.add(
                "max_calls_in_flight",
                format!("must be 1 to {MAX_CALLS_IN_FLIGHT_LIMIT}"),
            );
        }
        if let Some(n) = req.deadline_seconds
            && !valid_deadline_seconds(n)
        {
            v.add(
                "deadline_seconds",
                format!("must be 1 to {DEADLINE_SECONDS_LIMIT}"),
            );
        }
        v.check()?;
        let patch = ApiSettingsPatch {
            max_calls_in_flight: req.max_calls_in_flight,
            deadline_seconds: req.deadline_seconds,
        };
        let settings = self.state.db(move |s| s.set_api_settings(&patch)).await?;
        self.state.call_limits().set(settings);
        info!(
            max_calls_in_flight = settings.max_calls_in_flight,
            deadline_seconds = settings.deadline_seconds,
            "API limits set"
        );
        Ok(Response::new(convert::api_settings(settings)))
    }

    async fn get_enrichment_settings(
        &self,
        _request: Request<GetEnrichmentSettingsRequest>,
    ) -> Result<Response<EnrichmentSettings>, Status> {
        let settings = self
            .state
            .db(doewood_store::Store::enrichment_settings)
            .await?;
        Ok(Response::new(enrichment::settings_pb(&settings)))
    }

    async fn set_enrichment_settings(
        &self,
        request: Request<SetEnrichmentSettingsRequest>,
    ) -> Result<Response<EnrichmentSettings>, Status> {
        let patch = enrichment::check(request.get_ref())?;
        let settings = self
            .state
            .db(move |s| s.set_enrichment_settings(&patch))
            .await?;
        // Switches and "is set" only: never a secret, nor the contact (personal data).
        let r = settings.registries;
        info!(
            gleif = r.gleif,
            vies = r.vies,
            wikidata = r.wikidata,
            edgar = r.edgar,
            apr = r.apr,
            companies_house = r.companies_house,
            companies_house_active = settings.companies_house_active(),
            opencorporates_api_key_set = settings.opencorporates_key_set,
            edgar_contact_set = settings.edgar_contact.is_some(),
            apr_extra_roots_set = settings.apr_extra_roots_set,
            favicon_fallback = settings.favicon.fallback,
            favicon_egress_proxy_set = settings.favicon.egress_proxy.is_some(),
            "enrichment settings set"
        );
        Ok(Response::new(enrichment::settings_pb(&settings)))
    }
}
