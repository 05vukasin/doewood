//! Every RPC against an in-process server, through a tonic client: happy paths, validation
//! errors, the claim conflict between two instances sharing one backend, secrets never coming
//! back, the progress stream, gRPC-Web, health, reflection and the message size limit.

#![allow(clippy::many_single_char_names, clippy::too_many_lines)]

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::{Instance, Options, Shared, closed_port, eventually};
use doewood_api::pb::config_service_client::ConfigServiceClient;
use doewood_api::pb::graph_service_client::GraphServiceClient;
use doewood_api::pb::jobs_service_client::JobsServiceClient;
use doewood_api::pb::records_service_client::RecordsServiceClient;
use doewood_api::pb::search_service_client::SearchServiceClient;
use doewood_api::pb::summarize_service_client::SummarizeServiceClient;
use doewood_api::pb::{self, progress_update::Kind};
use doewood_api::{ReprocessPhase, ReprocessView, Reprocessor};
use doewood_model::{
    Blake3, Edge, EdgeKind, EdgeSource, EdgeState, NodeKind, ObjectLocation, SearchDoc,
};
use doewood_s3::{EventKind, EventOrigin, ObjectEvent, ObjectVersion};
use serde_json::json;
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tonic::Code;
use tonic::transport::Channel;
use tonic_types::StatusExt as _;

const ACCESS_KEY: &str = "test-access-key-id-1234";
const SECRET_KEY: &str = "test-secret-access-key-5678";
const API_KEY: &str = "test-api-key-value-9012";

fn creds() -> pb::S3Credentials {
    pb::S3Credentials {
        access_key_id: ACCESS_KEY.into(),
        secret_access_key: SECRET_KEY.into(),
        session_token: String::new(),
    }
}

fn add_request(endpoint: &str, bucket: &str, prefix: &str) -> pb::AddSourceRequest {
    pb::AddSourceRequest {
        endpoint: endpoint.into(),
        bucket: bucket.into(),
        prefix: prefix.into(),
        credentials: Some(creds()),
        // Long, so a failed pass waits instead of retrying during the test.
        scan_interval_seconds: 3600,
        ..pb::AddSourceRequest::default()
    }
}

/// No secret appears anywhere in a message.
fn assert_no_secret(what: &str, text: &str) {
    for secret in [ACCESS_KEY, SECRET_KEY, API_KEY] {
        assert!(!text.contains(secret), "{what} leaks a secret: {text}");
    }
}

fn config(ch: &Channel) -> ConfigServiceClient<Channel> {
    ConfigServiceClient::new(ch.clone())
}

fn invalid(status: &tonic::Status, fields: &[&str]) {
    assert_eq!(status.code(), Code::InvalidArgument, "{status:?}");
    let violations = status
        .get_error_details()
        .bad_request()
        .map(|b| {
            b.field_violations
                .iter()
                .map(|v| v.field.clone())
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    for f in fields {
        assert!(
            violations.iter().any(|v| v == f),
            "no violation for {f}: {violations:?} ({})",
            status.message()
        );
        assert!(status.message().contains(f), "{}", status.message());
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sources_lifecycle_and_secrets() {
    let shared = Shared::sqlite();
    let a = Instance::start("inst-a", Some(shared.backend.clone())).await;
    let ch = a.channel().await;
    let port = closed_port().await;
    let endpoint = format!("HTTP://127.0.0.1:{port}/");

    let added = config(&ch)
        .add_source(add_request(&endpoint, "docs", "in/"))
        .await
        .unwrap()
        .into_inner();
    assert_no_secret("AddSource", &format!("{added:?}"));
    assert!(added.credentials_set);
    assert_eq!(added.endpoint, format!("http://127.0.0.1:{port}"));
    assert_eq!(added.region, "us-east-1");
    assert_eq!(added.scan_interval_seconds, 3600);
    assert_eq!(added.max_object_bytes, 512 * 1024 * 1024);
    assert_eq!(added.owner_instance, "inst-a");
    assert_eq!(added.state, pb::SourceState::Active as i32);
    assert!(
        added.scanner_running,
        "the scanner starts without a restart"
    );
    let policy = added.llm_policy.clone().unwrap();
    assert_eq!(policy.mode, pb::LlmMode::Off as i32);
    assert_eq!(policy.concurrency, 2);
    // The credentials are sealed in the store, not returned.
    let id: doewood_core::SourceId = added.id.parse().unwrap();
    assert!(a.store.secret_exists(&format!("s3/{id}")).unwrap());

    // The scanner runs against a closed port: the source goes to error, with a reason that
    // carries no credentials.
    let get = |id: String| {
        let mut c = config(&ch);
        async move {
            c.get_source(pb::GetSourceRequest { source_id: id })
                .await
                .unwrap()
                .into_inner()
        }
    };
    eventually("the source fails its first pass", || async {
        get(added.id.clone()).await.state == pb::SourceState::Error as i32
    })
    .await;
    let failed = get(added.id.clone()).await;
    assert!(!failed.state_reason.is_empty());
    assert_no_secret("state_reason", &failed.state_reason);
    assert!(
        !failed.state_reason.contains("X-Amz-Signature"),
        "{}",
        failed.state_reason
    );

    // The same source again on this instance.
    let err = config(&ch)
        .add_source(add_request(&endpoint, "docs", "in/"))
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::AlreadyExists);
    assert!(err.message().contains(&added.id), "{}", err.message());

    // Update: options, then clear the credentials.
    let updated = config(&ch)
        .update_source(pb::UpdateSourceRequest {
            source_id: added.id.clone(),
            region: Some("eu-central-1".into()),
            url_style: Some(pb::UrlStyle::VirtualHost as i32),
            scan_interval_seconds: Some(1800),
            max_object_bytes: Some(1024),
            credentials: Some(pb::S3Credentials {
                session_token: "test-session-token".into(),
                ..creds()
            }),
            clear_credentials: false,
        })
        .await
        .unwrap()
        .into_inner();
    assert_no_secret("UpdateSource", &format!("{updated:?}"));
    assert!(!format!("{updated:?}").contains("test-session-token"));
    assert_eq!(updated.region, "eu-central-1");
    assert_eq!(updated.url_style, pb::UrlStyle::VirtualHost as i32);
    assert_eq!(updated.scan_interval_seconds, 1800);
    assert_eq!(updated.max_object_bytes, 1024);
    assert!(updated.credentials_set);
    let cleared = config(&ch)
        .update_source(pb::UpdateSourceRequest {
            source_id: added.id.clone(),
            region: Some(String::new()),
            max_object_bytes: Some(0),
            clear_credentials: true,
            ..pb::UpdateSourceRequest::default()
        })
        .await
        .unwrap()
        .into_inner();
    assert!(!cleared.credentials_set);
    assert_eq!(cleared.region, "us-east-1");
    assert_eq!(cleared.max_object_bytes, 512 * 1024 * 1024);
    assert!(!a.store.secret_exists(&format!("s3/{id}")).unwrap());

    // Pause stops the scanner; resume starts it again.
    let paused = config(&ch)
        .pause_source(pb::PauseSourceRequest {
            source_id: added.id.clone(),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(paused.state, pb::SourceState::Paused as i32);
    assert!(!paused.scanner_running);
    assert!(!a.discovery.is_running(id));
    let resumed = config(&ch)
        .resume_source(pb::ResumeSourceRequest {
            source_id: added.id.clone(),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(resumed.state, pb::SourceState::Active as i32);
    assert!(resumed.scanner_running);

    // The LLM policy.
    let with_policy = config(&ch)
        .set_llm_policy(pb::SetLlmPolicyRequest {
            source_id: added.id.clone(),
            policy: Some(pb::LlmPolicy {
                mode: pb::LlmMode::Kinds as i32,
                kinds: vec!["invoice".into(), "contract".into()],
                concurrency: 4,
                daily_token_budget: 250_000,
            }),
        })
        .await
        .unwrap()
        .into_inner();
    let p = with_policy.llm_policy.unwrap();
    assert_eq!(
        (p.mode, p.kinds.len(), p.concurrency, p.daily_token_budget),
        (pb::LlmMode::Kinds as i32, 2, 4, 250_000)
    );

    // List, then remove (the claim is released).
    let listed = config(&ch)
        .list_sources(pb::ListSourcesRequest {})
        .await
        .unwrap()
        .into_inner();
    assert_eq!(listed.sources.len(), 1);
    assert_no_secret("ListSources", &format!("{listed:?}"));
    let removed = config(&ch)
        .remove_source(pb::RemoveSourceRequest {
            source_id: added.id.clone(),
        })
        .await
        .unwrap()
        .into_inner();
    assert!(removed.claim_released);
    assert!(!a.discovery.is_running(id));
    let err = config(&ch)
        .get_source(pb::GetSourceRequest {
            source_id: added.id.clone(),
        })
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::NotFound);
    for (what, err) in [
        (
            "pause",
            config(&ch)
                .pause_source(pb::PauseSourceRequest {
                    source_id: added.id.clone(),
                })
                .await
                .unwrap_err(),
        ),
        (
            "remove",
            config(&ch)
                .remove_source(pb::RemoveSourceRequest {
                    source_id: added.id.clone(),
                })
                .await
                .unwrap_err(),
        ),
    ] {
        assert_eq!(err.code(), Code::NotFound, "{what}");
    }
    a.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sqs_inlets_are_set_through_the_api_and_keep_their_secrets() {
    const QUEUE_KEY: &str = "test-queue-access-key-3456";
    const QUEUE_SECRET: &str = "test-queue-secret-key-7890";
    let a = Instance::start("inst-a", Some(Shared::sqlite().backend)).await;
    let ch = a.channel().await;
    let s3_port = closed_port().await;
    let added = config(&ch)
        .add_source(add_request(
            &format!("http://127.0.0.1:{s3_port}"),
            "docs",
            "in/",
        ))
        .await
        .unwrap()
        .into_inner();
    assert!(added.sqs_inlet.is_none());
    let id: doewood_core::SourceId = added.id.parse().unwrap();
    // A queue on a closed local port: the inlet runs, and fails without leaking anything.
    let queue_port = closed_port().await;
    let queue_url = format!("http://127.0.0.1:{queue_port}/123456789012/doewood-test-events");
    let request = pb::SetSqsInletRequest {
        source_id: added.id.clone(),
        queue_url: queue_url.clone(),
        credentials: Some(pb::S3Credentials {
            access_key_id: QUEUE_KEY.into(),
            secret_access_key: QUEUE_SECRET.into(),
            session_token: "test-queue-session-token".into(),
        }),
        ..pb::SetSqsInletRequest::default()
    };
    let set = config(&ch)
        .set_sqs_inlet(request.clone())
        .await
        .unwrap()
        .into_inner();
    let no_secret = |what: &str, text: &str| {
        assert_no_secret(what, text);
        for secret in [QUEUE_KEY, QUEUE_SECRET, "test-queue-session-token"] {
            assert!(!text.contains(secret), "{what} leaks a secret: {text}");
        }
    };
    no_secret("SetSqsInlet", &format!("{set:?}"));
    let inlet = set.sqs_inlet.unwrap();
    assert_eq!(inlet.queue_url, queue_url);
    assert_eq!(inlet.region, "us-east-1");
    assert!(inlet.enabled && inlet.credentials_set && inlet.running);
    assert!(a.store.secret_exists(&format!("sqs/{id}")).unwrap());
    assert!(a.discovery.is_inlet_running(id));
    eventually("the inlet reports its failure", || async {
        let s = get_source(&ch, &added.id).await;
        s.sqs_inlet
            .is_some_and(|i| i.errors > 0 && !i.last_error.is_empty())
    })
    .await;
    let listed = config(&ch)
        .list_sources(pb::ListSourcesRequest {})
        .await
        .unwrap()
        .into_inner();
    no_secret("ListSources", &format!("{listed:?}"));
    let inlet = listed.sources[0].sqs_inlet.clone().unwrap();
    assert!(
        inlet.last_error.contains("ReceiveMessage"),
        "{}",
        inlet.last_error
    );
    assert!(inlet.last_error_at > 0);

    // Off: stored, not polling; the scanner is untouched.
    let off = config(&ch)
        .set_sqs_inlet(pb::SetSqsInletRequest {
            disabled: true,
            credentials: None,
            region: "eu-west-3".into(),
            ..request.clone()
        })
        .await
        .unwrap()
        .into_inner();
    let inlet = off.sqs_inlet.unwrap();
    assert!(!inlet.enabled && !inlet.running && inlet.credentials_set);
    assert_eq!(inlet.region, "eu-west-3");
    assert!(off.scanner_running && !a.discovery.is_inlet_running(id));
    // An AWS queue URL gives its region (off, so nothing reaches AWS); clearing the queue's
    // own credentials goes back to the source's.
    let aws = config(&ch)
        .set_sqs_inlet(pb::SetSqsInletRequest {
            source_id: added.id.clone(),
            queue_url: "https://sqs.eu-central-1.amazonaws.com/123456789012/doewood-test-events"
                .into(),
            disabled: true,
            clear_credentials: true,
            ..pb::SetSqsInletRequest::default()
        })
        .await
        .unwrap()
        .into_inner()
        .sqs_inlet
        .unwrap();
    assert_eq!(aws.region, "eu-central-1");
    assert!(!aws.credentials_set && !aws.running);
    assert!(!a.store.secret_exists(&format!("sqs/{id}")).unwrap());

    // Validation reports every field, without echoing a value.
    let err = config(&ch)
        .set_sqs_inlet(pb::SetSqsInletRequest {
            source_id: "nope".into(),
            queue_url: "https://user:topsecret-pw@sqs.example/1/q".into(),
            region: "eu west".into(),
            credentials: Some(pb::S3Credentials::default()),
            clear_credentials: true,
            ..pb::SetSqsInletRequest::default()
        })
        .await
        .unwrap_err();
    invalid(
        &err,
        &[
            "source_id",
            "queue_url",
            "region",
            "credentials.access_key_id",
            "clear_credentials",
        ],
    );
    assert!(!err.message().contains("topsecret-pw"), "{}", err.message());
    for bad in [
        "",
        "https://sqs.example/q",
        "ftp://sqs.example/1/q",
        "https://sqs.example/1/q?x",
    ] {
        let err = config(&ch)
            .set_sqs_inlet(pb::SetSqsInletRequest {
                queue_url: bad.into(),
                ..request.clone()
            })
            .await
            .unwrap_err();
        invalid(&err, &["queue_url"]);
    }
    let missing = config(&ch)
        .set_sqs_inlet(pb::SetSqsInletRequest {
            source_id: doewood_core::SourceId::generate().to_string(),
            ..request.clone()
        })
        .await
        .unwrap_err();
    assert_eq!(missing.code(), Code::NotFound, "{missing:?}");

    // Removed: gone with its credentials. Removing the source removes a new one too.
    config(&ch).set_sqs_inlet(request.clone()).await.unwrap();
    let removed = config(&ch)
        .remove_sqs_inlet(pb::RemoveSqsInletRequest {
            source_id: added.id.clone(),
        })
        .await
        .unwrap()
        .into_inner();
    assert!(removed.sqs_inlet.is_none() && removed.scanner_running);
    assert!(!a.discovery.is_inlet_running(id));
    assert!(!a.store.secret_exists(&format!("sqs/{id}")).unwrap());
    config(&ch).set_sqs_inlet(request.clone()).await.unwrap();
    config(&ch)
        .remove_source(pb::RemoveSourceRequest {
            source_id: added.id.clone(),
        })
        .await
        .unwrap();
    assert!(!a.discovery.is_inlet_running(id));
    assert!(!a.store.secret_exists(&format!("sqs/{id}")).unwrap());
    assert!(
        a.store
            .setting(&format!("sqs_inlet/{id}"))
            .unwrap()
            .is_none()
    );
    a.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn source_validation_reports_every_field() {
    let a = Instance::start("inst-a", Some(Shared::sqlite().backend)).await;
    let ch = a.channel().await;
    let err = config(&ch)
        .add_source(pb::AddSourceRequest {
            endpoint: "http://admin:topsecret-pw@minio:9000".into(),
            bucket: "Not A Bucket".into(),
            prefix: "a\u{1}b".into(),
            region: "eu west".into(),
            url_style: 7,
            credentials: Some(pb::S3Credentials {
                access_key_id: String::new(),
                secret_access_key: "line\nbreak".into(),
                session_token: String::new(),
            }),
            scan_interval_seconds: 10_000_000,
            max_object_bytes: u64::MAX,
            llm_policy: Some(pb::LlmPolicy {
                mode: 42,
                kinds: vec![],
                concurrency: 1000,
                daily_token_budget: 0,
            }),
            paused: false,
        })
        .await
        .unwrap_err();
    invalid(
        &err,
        &[
            "endpoint",
            "bucket",
            "prefix",
            "region",
            "url_style",
            "credentials.access_key_id",
            "credentials.secret_access_key",
            "scan_interval_seconds",
            "max_object_bytes",
            "llm_policy.mode",
            "llm_policy.concurrency",
        ],
    );
    assert!(!err.message().contains("topsecret-pw"), "{}", err.message());
    assert!(!err.message().contains("line\nbreak"), "{}", err.message());

    // A legacy bucket name works path-style, not virtual-host style.
    let mut req = add_request("http://127.0.0.1:1", "Legacy_Bucket", "");
    req.paused = true;
    let ok = config(&ch)
        .add_source(req.clone())
        .await
        .unwrap()
        .into_inner();
    assert_eq!(ok.state, pb::SourceState::Paused as i32);
    assert!(!ok.scanner_running);
    let err = config(&ch)
        .update_source(pb::UpdateSourceRequest {
            source_id: ok.id.clone(),
            url_style: Some(pb::UrlStyle::VirtualHost as i32),
            ..pb::UpdateSourceRequest::default()
        })
        .await
        .unwrap_err();
    invalid(&err, &["url_style"]);
    req.url_style = pb::UrlStyle::VirtualHost as i32;
    req.bucket = "Legacy_Bucket2".into();
    invalid(&config(&ch).add_source(req).await.unwrap_err(), &["bucket"]);

    // Ids, the policy and conflicting flags.
    for (err, fields) in [
        (
            config(&ch)
                .get_source(pb::GetSourceRequest {
                    source_id: "nope".into(),
                })
                .await
                .unwrap_err(),
            vec!["source_id"],
        ),
        (
            config(&ch)
                .update_source(pb::UpdateSourceRequest {
                    source_id: ok.id.clone(),
                    credentials: Some(creds()),
                    clear_credentials: true,
                    ..pb::UpdateSourceRequest::default()
                })
                .await
                .unwrap_err(),
            vec!["clear_credentials"],
        ),
        (
            config(&ch)
                .set_llm_policy(pb::SetLlmPolicyRequest {
                    source_id: ok.id.clone(),
                    policy: Some(pb::LlmPolicy {
                        mode: pb::LlmMode::All as i32,
                        kinds: vec!["invoice".into()],
                        ..pb::LlmPolicy::default()
                    }),
                })
                .await
                .unwrap_err(),
            vec!["policy.kinds"],
        ),
        (
            config(&ch)
                .set_llm_policy(pb::SetLlmPolicyRequest {
                    source_id: String::new(),
                    policy: None,
                })
                .await
                .unwrap_err(),
            vec!["source_id", "policy"],
        ),
        (
            config(&ch)
                .pause_source(pb::PauseSourceRequest {
                    source_id: String::new(),
                })
                .await
                .unwrap_err(),
            vec!["source_id"],
        ),
    ] {
        invalid(&err, &fields);
    }
    let err = config(&ch)
        .resume_source(pb::ResumeSourceRequest {
            source_id: doewood_core::SourceId::generate().to_string(),
        })
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::NotFound);
    a.stop().await;
}

/// Two instances share one backend; the second cannot add a source the first owns.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn claims_conflict_between_instances() {
    for shared in Shared::all().await {
        let a = Instance::start("inst-a", Some(shared.backend.clone())).await;
        let b = Instance::start("inst-b", Some(shared.backend.clone())).await;
        let (cha, chb) = (a.channel().await, b.channel().await);
        let mut req = add_request("http://minio.test:9000", "shared-docs", "in/");
        req.paused = true;
        let added = config(&cha)
            .add_source(req.clone())
            .await
            .unwrap()
            .into_inner();
        // Spelled differently, it is the same source.
        let mut same = req.clone();
        same.endpoint = "HTTP://Minio.Test:9000/".into();
        let err = config(&chb).add_source(same).await.unwrap_err();
        assert_eq!(err.code(), Code::AlreadyExists, "{}: {err:?}", shared.name);
        assert!(
            err.message().contains("\"inst-a\""),
            "{}: the owner is named: {}",
            shared.name,
            err.message()
        );
        assert_no_secret("claim error", err.message());
        assert!(
            config(&chb)
                .list_sources(pb::ListSourcesRequest {})
                .await
                .unwrap()
                .into_inner()
                .sources
                .is_empty()
        );
        // Another prefix of the same bucket is another source.
        let mut other = req.clone();
        other.prefix = "out/".into();
        config(&chb).add_source(other).await.unwrap();

        // Once A removes it, B can add it.
        let removed = config(&cha)
            .remove_source(pb::RemoveSourceRequest {
                source_id: added.id,
            })
            .await
            .unwrap()
            .into_inner();
        assert!(removed.claim_released);
        let b_added = config(&chb).add_source(req).await.unwrap().into_inner();
        assert_eq!(b_added.owner_instance, "inst-b");
        a.stop().await;
        b.stop().await;
        shared.drop_schema().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn without_a_backend_sources_wait_and_health_says_so() {
    let a = Instance::start("inst-a", None).await;
    let ch = a.channel().await;
    let health = tonic_health::pb::health_client::HealthClient::new(ch.clone());
    let status = |name: &str| {
        let mut h = health.clone();
        let name = name.to_owned();
        async move {
            h.check(tonic_health::pb::HealthCheckRequest { service: name })
                .await
                .unwrap()
                .into_inner()
                .status
        }
    };
    let serving = tonic_health::pb::health_check_response::ServingStatus::Serving as i32;
    let not_serving = tonic_health::pb::health_check_response::ServingStatus::NotServing as i32;
    eventually("health reports the missing backend", || async {
        status("").await == not_serving
    })
    .await;
    assert_eq!(status("doewood.v1.RecordsService").await, not_serving);
    assert_eq!(status("doewood.v1.ConfigService").await, serving);
    let err = config(&ch)
        .add_source(add_request("http://minio:9000", "docs", ""))
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::Unavailable);
    let err = RecordsServiceClient::new(ch.clone())
        .get_record(pb::GetRecordRequest {
            blake3: "ab".repeat(32),
        })
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::Unavailable);
    // Unknown services are NOT_FOUND.
    let err = health
        .clone()
        .check(tonic_health::pb::HealthCheckRequest {
            service: "nope.Service".into(),
        })
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::NotFound);

    // The backend arrives (Postgres connected): everything serves.
    a.backend.send_replace(Some(Shared::sqlite().backend));
    eventually("health follows the backend", || async {
        status("").await == serving
    })
    .await;
    assert_eq!(status("doewood.v1.SearchService").await, serving);
    config(&ch)
        .add_source(add_request("http://minio:9000", "docs", ""))
        .await
        .unwrap();
    a.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ai_providers_are_write_only() {
    let a = Instance::start("inst-a", Some(Shared::sqlite().backend)).await;
    let ch = a.channel().await;
    let set = config(&ch)
        .set_ai_provider(pb::SetAiProviderRequest {
            preset: "openai".into(),
            model: "gpt-test".into(),
            api_key: API_KEY.into(),
            ..pb::SetAiProviderRequest::default()
        })
        .await
        .unwrap()
        .into_inner();
    assert_no_secret("SetAiProvider", &format!("{set:?}"));
    assert_eq!(set.name, "openai");
    assert!(set.api_key_set && set.is_default);
    let local = config(&ch)
        .set_ai_provider(pb::SetAiProviderRequest {
            name: "local-vllm".into(),
            preset: "openai_compatible".into(),
            model: "qwen-test".into(),
            base_url: "http://VLLM:8000/v1/".into(),
            make_default: true,
            ..pb::SetAiProviderRequest::default()
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(local.base_url, "http://vllm:8000/v1");
    assert!(!local.api_key_set && local.is_default);
    // An empty key keeps the stored one; clear_api_key deletes it.
    let kept = config(&ch)
        .set_ai_provider(pb::SetAiProviderRequest {
            preset: "openai".into(),
            model: "gpt-test-2".into(),
            ..pb::SetAiProviderRequest::default()
        })
        .await
        .unwrap()
        .into_inner();
    assert!(kept.api_key_set && !kept.is_default);
    assert_eq!(
        a.store.ai_api_key("openai").unwrap().unwrap().expose(),
        API_KEY
    );
    let list = config(&ch)
        .list_ai_providers(pb::ListAiProvidersRequest {})
        .await
        .unwrap()
        .into_inner();
    assert_no_secret("ListAiProviders", &format!("{list:?}"));
    assert_eq!(
        list.providers
            .iter()
            .map(|p| p.name.as_str())
            .collect::<Vec<_>>(),
        ["local-vllm", "openai"]
    );
    assert_eq!(
        list.presets,
        [
            "openai",
            "openai_compatible",
            "anthropic",
            "bedrock",
            "openrouter",
            "groq",
            "vllm",
            "ollama"
        ]
    );
    let cleared = config(&ch)
        .set_ai_provider(pb::SetAiProviderRequest {
            preset: "openai".into(),
            model: "gpt-test-2".into(),
            clear_api_key: true,
            ..pb::SetAiProviderRequest::default()
        })
        .await
        .unwrap()
        .into_inner();
    assert!(!cleared.api_key_set);

    let err = config(&ch)
        .set_ai_provider(pb::SetAiProviderRequest {
            name: "Bad Name".into(),
            preset: "gpt".into(),
            model: String::new(),
            base_url: "https://user:secret-in-url@api.test/v1".into(),
            api_key: "multi\nline".into(),
            clear_api_key: true,
            make_default: false,
        })
        .await
        .unwrap_err();
    invalid(
        &err,
        &[
            "preset",
            "name",
            "model",
            "base_url",
            "api_key",
            "clear_api_key",
        ],
    );
    assert!(
        !err.message().contains("secret-in-url"),
        "{}",
        err.message()
    );
    let err = config(&ch)
        .set_ai_provider(pb::SetAiProviderRequest {
            preset: "openai_compatible".into(),
            model: "m".into(),
            ..pb::SetAiProviderRequest::default()
        })
        .await
        .unwrap_err();
    invalid(&err, &["base_url"]);
    a.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ingest_settings_round_trip_and_validation() {
    let a = Instance::start("inst-a", Some(Shared::sqlite().backend)).await;
    let ch = a.channel().await;
    let get = || async {
        config(&ch)
            .get_ingest_settings(pb::GetIngestSettingsRequest {})
            .await
            .unwrap()
            .into_inner()
    };
    let eq = |a: f64, b: f64| (a - b).abs() < f64::EPSILON;
    // The default, before anything is set.
    assert!(eq(get().await.similar_min_jaccard, 0.8));

    let set = config(&ch)
        .set_ingest_settings(pb::SetIngestSettingsRequest {
            similar_min_jaccard: Some(0.65),
            ..pb::SetIngestSettingsRequest::default()
        })
        .await
        .unwrap()
        .into_inner();
    assert!(eq(set.similar_min_jaccard, 0.65));
    assert!(eq(get().await.similar_min_jaccard, 0.65));
    // Kept in the instance store, where ingest reads it.
    assert!(eq(
        a.store.ingest_settings().unwrap().similar_min_jaccard,
        0.65
    ));
    // An unset field keeps the stored value; 1 is allowed.
    let kept = config(&ch)
        .set_ingest_settings(pb::SetIngestSettingsRequest::default())
        .await
        .unwrap()
        .into_inner();
    assert!(eq(kept.similar_min_jaccard, 0.65));
    let one = config(&ch)
        .set_ingest_settings(pb::SetIngestSettingsRequest {
            similar_min_jaccard: Some(1.0),
            ..pb::SetIngestSettingsRequest::default()
        })
        .await
        .unwrap()
        .into_inner();
    assert!(eq(one.similar_min_jaccard, 1.0));

    for bad in [0.0, -0.5, 1.01, f64::NAN, f64::INFINITY] {
        let err = config(&ch)
            .set_ingest_settings(pb::SetIngestSettingsRequest {
                similar_min_jaccard: Some(bad),
                ..pb::SetIngestSettingsRequest::default()
            })
            .await
            .unwrap_err();
        invalid(&err, &["similar_min_jaccard"]);
    }
    // A refused value changes nothing.
    assert!(eq(get().await.similar_min_jaccard, 1.0));

    // The on-demand Summarize cap: default 2, 1 to 16, kept apart from the threshold.
    assert_eq!(get().await.summarize_max_concurrent, 2);
    let set = config(&ch)
        .set_ingest_settings(pb::SetIngestSettingsRequest {
            summarize_max_concurrent: Some(4),
            ..pb::SetIngestSettingsRequest::default()
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(set.summarize_max_concurrent, 4);
    assert!(eq(set.similar_min_jaccard, 1.0));
    assert_eq!(
        a.store.ingest_settings().unwrap().summarize_max_concurrent,
        4
    );
    for bad in [0, 17] {
        let err = config(&ch)
            .set_ingest_settings(pb::SetIngestSettingsRequest {
                similar_min_jaccard: Some(0.9),
                summarize_max_concurrent: Some(bad),
            })
            .await
            .unwrap_err();
        invalid(&err, &["summarize_max_concurrent"]);
    }
    let now = get().await;
    assert_eq!(now.summarize_max_concurrent, 4);
    assert!(eq(now.similar_min_jaccard, 1.0));
    a.stop().await;
}

/// A throwaway self-signed certificate made for these tests (its key was discarded): not a
/// real CA.
const TEST_ROOT_PEM: &str = "-----BEGIN CERTIFICATE-----\n\
MIIBrzCCAVWgAwIBAgIUaYELWlw7QeVdyYce7dI/nXVWFqwwCgYIKoZIzj0EAwIw\n\
LDEqMCgGA1UEAwwhZG9ld29vZCB0ZXN0IHJvb3QgKG5vdCBhIHJlYWwgQ0EpMCAX\n\
DTI2MTEyNTA2MTAwNVoYDzIxMjYxMTAxMDYxMDA1WjAsMSowKAYDVQQDDCFkb2V3\n\
b29kIHRlc3Qgcm9vdCAobm90IGEgcmVhbCBDQSkwWTATBgcqhkjOPQIBBggqhkjO\n\
PQMBBwNCAATYqcUgrvtY3A3jxscNSCGujoppdopW6a18MYEyWotlOGE0KtzRp7B5\n\
0GToB5wGb/QziTbGR2CheugNsVR+Jhnyo1MwUTAdBgNVHQ4EFgQUBzS5SQayIVCE\n\
YjOvGTr1kuzYEt8wHwYDVR0jBBgwFoAUBzS5SQayIVCEYjOvGTr1kuzYEt8wDwYD\n\
VR0TAQH/BAUwAwEB/zAKBggqhkjOPQQDAgNIADBFAiEA8F1BYV2yWkduhFJQpQ5Q\n\
qBsrIl0ifX/gzMCRRz8fb8oCIH8fQvRNlgoozch12VIte5yxqOVzFfB0oDbceu5+\n\
3muB\n\
-----END CERTIFICATE-----\n\
";
const CH_KEY: &str = "test-ch-key-not-real-1111";
const OC_KEY: &str = "test-oc-key-not-real-2222";

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn enrichment_settings_round_trip_and_secrets_stay_write_only() {
    let a = Instance::start("inst-a", Some(Shared::sqlite().backend)).await;
    let ch = a.channel().await;
    let get = || async {
        config(&ch)
            .get_enrichment_settings(pb::GetEnrichmentSettingsRequest {})
            .await
            .unwrap()
            .into_inner()
    };
    let stream_key = "test-stream-key-not-real-9";
    let leaks = |what: &str, text: &str| {
        for secret in [CH_KEY, OC_KEY, "MIIBrzCCAVWg"] {
            assert!(!text.contains(secret), "{what} leaks a secret: {text}");
        }
    };
    // The defaults (ADR 0002 section 7): the keyless registries on, Companies House waiting
    // for a key, the favicon fallback off.
    let defaults = get().await;
    assert_eq!(
        defaults,
        pb::EnrichmentSettings {
            gleif_enabled: true,
            vies_enabled: true,
            wikidata_enabled: true,
            edgar_enabled: true,
            apr_enabled: true,
            companies_house_enabled: true,
            // The delta feeds: GLEIF, EDGAR and APR on, Companies House streaming off.
            gleif_feed_enabled: true,
            edgar_feed_enabled: true,
            apr_feed_enabled: true,
            ..pb::EnrichmentSettings::default()
        }
    );
    assert!(!defaults.companies_house_active && !defaults.favicon_fallback);
    assert!(!a.store.enrichment_config().unwrap().companies_house);

    let set = config(&ch)
        .set_enrichment_settings(pb::SetEnrichmentSettingsRequest {
            wikidata_enabled: Some(false),
            companies_house_api_key: format!(" {CH_KEY} "),
            opencorporates_api_key: OC_KEY.into(),
            edgar_contact: Some(" Example Ops ops@example.invalid ".into()),
            apr_extra_roots_pem: TEST_ROOT_PEM.into(),
            favicon_fallback: Some(true),
            favicon_egress_proxy: Some("http://egress.svc:3128".into()),
            apr_feed_enabled: Some(false),
            companies_house_feed_enabled: Some(true),
            companies_house_stream_key: stream_key.into(),
            ..pb::SetEnrichmentSettingsRequest::default()
        })
        .await
        .unwrap()
        .into_inner();
    leaks("SetEnrichmentSettings", &format!("{set:?}"));
    assert!(!format!("{set:?}").contains(stream_key));
    assert!(set.gleif_feed_enabled && !set.apr_feed_enabled);
    assert!(set.companies_house_stream_key_set && set.companies_house_feed_active);
    assert!(!set.wikidata_enabled && set.gleif_enabled);
    assert!(set.companies_house_api_key_set && set.companies_house_active);
    assert!(set.opencorporates_api_key_set && set.apr_extra_roots_set);
    assert_eq!(set.edgar_contact, "Example Ops ops@example.invalid");
    assert!(set.favicon_fallback);
    assert_eq!(set.favicon_egress_proxy, "http://egress.svc:3128");
    let got = get().await;
    leaks("GetEnrichmentSettings", &format!("{got:?}"));
    assert_eq!(got, set);
    // The job's view has the keys (trimmed); the favicon proxy reads the same rows.
    let job = a.store.enrichment_config().unwrap();
    assert!(job.companies_house && !job.wikidata);
    assert_eq!(job.companies_house_key.as_ref().unwrap().expose(), CH_KEY);
    assert_eq!(job.opencorporates_key.as_ref().unwrap().expose(), OC_KEY);
    assert_eq!(
        job.apr_extra_roots.as_ref().unwrap().expose(),
        TEST_ROOT_PEM
    );
    leaks("EnrichmentConfig", &format!("{job:?}"));
    assert!(!format!("{job:?}").contains(stream_key));
    assert_eq!(
        job.companies_house_stream_key.as_ref().unwrap().expose(),
        stream_key
    );
    assert!(job.feeds.companies_house && !job.feeds.apr);
    let favicon = a.store.favicon_settings().unwrap();
    assert!(favicon.fallback);
    assert_eq!(
        favicon.egress_proxy.as_deref(),
        Some("http://egress.svc:3128")
    );

    // An empty request keeps everything.
    let kept = config(&ch)
        .set_enrichment_settings(pb::SetEnrichmentSettingsRequest::default())
        .await
        .unwrap()
        .into_inner();
    assert_eq!(kept, set);

    // Refused values, all reported at once, without echoing a secret; nothing changes.
    let err = config(&ch)
        .set_enrichment_settings(pb::SetEnrichmentSettingsRequest {
            gleif_enabled: Some(false),
            companies_house_api_key: "short".into(),
            opencorporates_api_key: format!("{OC_KEY}:x"),
            clear_opencorporates_api_key: true,
            edgar_contact: Some("Ops\r\nX-Evil: 1".into()),
            apr_extra_roots_pem:
                "-----BEGIN CERTIFICATE-----\nMIIBrzCCAVWgnope\n-----END CERTIFICATE-----\n".into(),
            favicon_egress_proxy: Some("socks5://egress.svc:1080".into()),
            ..pb::SetEnrichmentSettingsRequest::default()
        })
        .await
        .unwrap_err();
    invalid(
        &err,
        &[
            "companies_house_api_key",
            "clear_opencorporates_api_key",
            "edgar_contact",
            "apr_extra_roots_pem",
            "favicon_egress_proxy",
        ],
    );
    leaks("the error", &format!("{err:?}"));
    for (field, bad) in [
        ("opencorporates_api_key", format!("{OC_KEY}:x")),
        ("companies_house_api_key", format!("{CH_KEY} with spaces")),
    ] {
        let mut req = pb::SetEnrichmentSettingsRequest::default();
        if field == "opencorporates_api_key" {
            req.opencorporates_api_key = bad;
        } else {
            req.companies_house_api_key = bad;
        }
        let err = config(&ch).set_enrichment_settings(req).await.unwrap_err();
        invalid(&err, &[field]);
        leaks("the error", &format!("{err:?}"));
    }
    for bad in [
        "egress.svc:3128",
        "http://user:pw@egress.svc:3128",
        "http://",
        "ftp://egress.svc",
    ] {
        let err = config(&ch)
            .set_enrichment_settings(pb::SetEnrichmentSettingsRequest {
                favicon_egress_proxy: Some(bad.into()),
                ..pb::SetEnrichmentSettingsRequest::default()
            })
            .await
            .unwrap_err();
        invalid(&err, &["favicon_egress_proxy"]);
        assert!(!err.message().contains("pw@"), "{}", err.message());
    }
    for bad in ["not a certificate", "x"] {
        let err = config(&ch)
            .set_enrichment_settings(pb::SetEnrichmentSettingsRequest {
                apr_extra_roots_pem: bad.into(),
                ..pb::SetEnrichmentSettingsRequest::default()
            })
            .await
            .unwrap_err();
        invalid(&err, &["apr_extra_roots_pem"]);
    }
    let err = config(&ch)
        .set_enrichment_settings(pb::SetEnrichmentSettingsRequest {
            edgar_contact: Some("x".repeat(201)),
            ..pb::SetEnrichmentSettingsRequest::default()
        })
        .await
        .unwrap_err();
    invalid(&err, &["edgar_contact"]);
    assert_eq!(get().await, set);

    // Clearing: the keys, the roots, the contact and the proxy go; Companies House is off
    // again until a key is set.
    let cleared = config(&ch)
        .set_enrichment_settings(pb::SetEnrichmentSettingsRequest {
            clear_companies_house_api_key: true,
            clear_opencorporates_api_key: true,
            clear_apr_extra_roots_pem: true,
            edgar_contact: Some(String::new()),
            favicon_fallback: Some(false),
            favicon_egress_proxy: Some(String::new()),
            ..pb::SetEnrichmentSettingsRequest::default()
        })
        .await
        .unwrap()
        .into_inner();
    assert!(cleared.companies_house_enabled && !cleared.companies_house_active);
    assert!(!cleared.companies_house_api_key_set && !cleared.opencorporates_api_key_set);
    assert!(!cleared.apr_extra_roots_set && !cleared.favicon_fallback);
    assert!(cleared.edgar_contact.is_empty() && cleared.favicon_egress_proxy.is_empty());
    let job = a.store.enrichment_config().unwrap();
    assert!(!job.companies_house && job.companies_house_key.is_none());
    assert_eq!(
        a.store.favicon_settings().unwrap(),
        doewood_store::FaviconSettings::default()
    );
    a.stop().await;
}

fn hash(n: u8) -> Blake3 {
    Blake3([n; 32])
}

/// `ListRecords` pages end at the byte budget (issue #111): ten records of 1 MiB come three
/// to a page (each page under the 4 MiB a default client accepts), every record exactly once.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn list_records_pages_end_at_the_byte_budget() {
    for shared in Shared::all().await {
        let a = Instance::start("inst-a", Some(shared.backend.clone())).await;
        let ch = a.channel().await;
        let mut req = add_request("http://minio.test:9000", "big", "");
        req.paused = true;
        let source = config(&ch).add_source(req).await.unwrap().into_inner();
        let be = &shared.backend;
        let pad = "x".repeat(1 << 20);
        for n in 100..110u8 {
            be.put_record(
                &hash(n),
                &doewood_model::canonical::to_canonical(&json!({"n": n, "pad": pad})).unwrap(),
                &[("quail".to_owned(), "1".to_owned())].into(),
            )
            .await
            .unwrap();
            be.upsert_location(
                &hash(n),
                &ObjectLocation {
                    endpoint: "http://minio.test:9000".into(),
                    bucket: "big".into(),
                    key: format!("f{n}.pdf"),
                },
            )
            .await
            .unwrap();
        }
        let mut records = RecordsServiceClient::new(ch.clone());
        let (mut seen, mut sizes, mut token) = (Vec::new(), Vec::new(), String::new());
        loop {
            let page = records
                .list_records(pb::ListRecordsRequest {
                    source_id: source.id.clone(),
                    page_size: 1000,
                    page_token: token,
                })
                .await
                .unwrap()
                .into_inner();
            sizes.push(page.records.len());
            seen.extend(page.records.into_iter().map(|r| r.blake3));
            if page.next_page_token.is_empty() {
                break;
            }
            token = page.next_page_token;
        }
        assert_eq!(sizes, [3, 3, 3, 1], "{}", shared.name);
        let want: Vec<String> = (100..110u8).map(|n| hash(n).to_hex()).collect();
        assert_eq!(seen, want, "{}", shared.name);
        a.stop().await;
        shared.drop_schema().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn records_graph_and_search() {
    for shared in Shared::all().await {
        let a = Instance::start("inst-a", Some(shared.backend.clone())).await;
        let ch = a.channel().await;
        let mut req = add_request("http://minio.test:9000", "docs", "in/");
        req.paused = true;
        let source = config(&ch).add_source(req).await.unwrap().into_inner();

        // What ingest will write: records, locations, a company, party edges, search rows.
        let be = &shared.backend;
        let at = |key: &str| ObjectLocation {
            endpoint: "http://minio.test:9000".into(),
            bucket: "docs".into(),
            key: key.into(),
        };
        for (n, key, title, kind, amount) in [
            (1, "in/a.pdf", "Invoice ACME 1", "invoice", 100.0),
            (2, "in/b.pdf", "Invoice ACME 2", "invoice", 250.0),
            (3, "in/c.docx", "Framework contract", "contract", 9000.0),
            (4, "out/d.pdf", "Invoice elsewhere", "invoice", 5.0),
        ] {
            be.put_record(
                &hash(n),
                &doewood_model::canonical::to_canonical(&json!({"title": title, "n": n})).unwrap(),
                &[("quail".to_owned(), "1.2.3".to_owned())].into(),
            )
            .await
            .unwrap();
            be.upsert_location(&hash(n), &at(key)).await.unwrap();
            be.index_document(&SearchDoc {
                blake3: hash(n),
                title: title.into(),
                body: format!("{title} body text"),
                kind: Some(kind.into()),
                date: Some(time::Date::from_calendar_date(2026, time::Month::March, n).unwrap()),
                amount: Some(amount),
            })
            .await
            .unwrap();
        }
        let acme = be
            .upsert_node(NodeKind::Company, "acme", &json!({"name": "ACME"}))
            .await
            .unwrap();
        for n in [1, 2] {
            let doc = be
                .node_by_key(NodeKind::Document, &hash(n).to_hex())
                .await
                .unwrap()
                .unwrap();
            be.upsert_edge(
                &Edge::new(doc.id, acme, EdgeKind::Party, EdgeSource::Document)
                    .with_props(json!({"role": "supplier"})),
            )
            .await
            .unwrap();
        }

        // Records.
        let mut records = RecordsServiceClient::new(ch.clone());
        let r = records
            .get_record(pb::GetRecordRequest {
                blake3: hash(1).to_hex().to_uppercase(),
            })
            .await
            .unwrap()
            .into_inner();
        assert_eq!(r.blake3, hash(1).to_hex());
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&r.body_json).unwrap(),
            json!({"title": "Invoice ACME 1", "n": 1})
        );
        assert_eq!(r.versions["quail"], "1.2.3");
        let err = records
            .get_record(pb::GetRecordRequest {
                blake3: hash(99).to_hex(),
            })
            .await
            .unwrap_err();
        assert_eq!(err.code(), Code::NotFound);
        invalid(
            &records
                .get_record(pb::GetRecordRequest {
                    blake3: "xyz".into(),
                })
                .await
                .unwrap_err(),
            &["blake3"],
        );
        // Paging through the source: hashes 1, 2, 3 (4 is outside the prefix).
        let page = |token: String| {
            let mut c = records.clone();
            let id = source.id.clone();
            async move {
                c.list_records(pb::ListRecordsRequest {
                    source_id: id,
                    page_size: 2,
                    page_token: token,
                })
                .await
                .unwrap()
                .into_inner()
            }
        };
        let first = page(String::new()).await;
        assert_eq!(first.records.len(), 2);
        assert!(!first.next_page_token.is_empty());
        let second = page(first.next_page_token.clone()).await;
        assert_eq!(
            second
                .records
                .iter()
                .map(|r| r.blake3.clone())
                .collect::<Vec<_>>(),
            [hash(3).to_hex()]
        );
        assert!(second.next_page_token.is_empty());
        invalid(
            &records
                .list_records(pb::ListRecordsRequest {
                    source_id: "bad".into(),
                    page_size: 5000,
                    page_token: "garbage".into(),
                })
                .await
                .unwrap_err(),
            &["source_id", "page_size", "page_token"],
        );

        // Graph.
        let mut graph = GraphServiceClient::new(ch.clone());
        let by_key = graph
            .get_node(pb::GetNodeRequest {
                lookup: Some(pb::get_node_request::Lookup::Key(pb::NodeKey {
                    kind: pb::NodeKind::Company as i32,
                    key: "acme".into(),
                })),
            })
            .await
            .unwrap()
            .into_inner();
        assert_eq!(by_key.id, acme.to_string());
        let by_id = graph
            .get_node(pb::GetNodeRequest {
                lookup: Some(pb::get_node_request::Lookup::Id(acme.to_string())),
            })
            .await
            .unwrap()
            .into_inner();
        assert_eq!(by_id, by_key);
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&by_id.props_json).unwrap(),
            json!({"name": "ACME"})
        );
        let neighbours = graph
            .neighbours(pb::NeighboursRequest {
                node_id: acme.to_string(),
                direction: pb::Direction::In as i32,
                edge_kinds: vec![pb::EdgeKind::Party as i32],
                node_kind: pb::NodeKind::Document as i32,
                at: "2026-03-01".into(),
                include_inactive: false,
            })
            .await
            .unwrap()
            .into_inner()
            .neighbours;
        assert_eq!(neighbours.len(), 2);
        let edge = neighbours[0].edge.as_ref().unwrap();
        assert_eq!(edge.kind, pb::EdgeKind::Party as i32);
        assert_eq!(edge.state, pb::EdgeState::Active as i32);
        assert_eq!(edge.source, pb::EdgeSource::Document as i32);
        assert_eq!(edge.dst, acme.to_string());
        // Only located_at edges out of the companies: none.
        let none = graph
            .neighbours(pb::NeighboursRequest {
                node_id: acme.to_string(),
                direction: pb::Direction::Out as i32,
                ..pb::NeighboursRequest::default()
            })
            .await
            .unwrap()
            .into_inner()
            .neighbours;
        assert!(none.is_empty());
        let docs = graph
            .company_documents(pb::CompanyDocumentsRequest {
                company_id: acme.to_string(),
            })
            .await
            .unwrap()
            .into_inner()
            .documents;
        assert_eq!(
            docs.iter().map(|d| d.key.clone()).collect::<Vec<_>>(),
            [hash(1).to_hex(), hash(2).to_hex()]
        );
        let doc_id = docs[0].id.clone();
        invalid(
            &graph
                .company_documents(pb::CompanyDocumentsRequest { company_id: doc_id })
                .await
                .unwrap_err(),
            &["company_id"],
        );
        assert_eq!(
            graph
                .get_node(pb::GetNodeRequest {
                    lookup: Some(pb::get_node_request::Lookup::Id(
                        uuid::Uuid::now_v7().to_string()
                    )),
                })
                .await
                .unwrap_err()
                .code(),
            Code::NotFound
        );
        invalid(
            &graph
                .get_node(pb::GetNodeRequest { lookup: None })
                .await
                .unwrap_err(),
            &["lookup"],
        );
        invalid(
            &graph
                .get_node(pb::GetNodeRequest {
                    lookup: Some(pb::get_node_request::Lookup::Key(pb::NodeKey {
                        kind: 0,
                        key: String::new(),
                    })),
                })
                .await
                .unwrap_err(),
            &["key.kind", "key.key"],
        );
        invalid(
            &graph
                .neighbours(pb::NeighboursRequest {
                    node_id: "x".into(),
                    direction: 9,
                    edge_kinds: vec![1, 99],
                    node_kind: 17,
                    at: "March 1".into(),
                    include_inactive: false,
                })
                .await
                .unwrap_err(),
            &["node_id", "direction", "edge_kinds[1]", "node_kind", "at"],
        );

        // Search.
        let mut search = SearchServiceClient::new(ch.clone());
        let hits = search
            .search(pb::SearchRequest {
                text: "invoice acme".into(),
                company_id: acme.to_string(),
                kind: "invoice".into(),
                date_from: "2026-03-01".into(),
                date_to: "2026-03-31".into(),
                amount_min: Some(50.0),
                amount_max: Some(1000.0),
                source_id: source.id.clone(),
                limit: 10,
                offset: 0,
            })
            .await
            .unwrap()
            .into_inner()
            .hits;
        let mut found: Vec<_> = hits.iter().map(|h| h.blake3.clone()).collect();
        found.sort();
        assert_eq!(
            found,
            [hash(1).to_hex(), hash(2).to_hex()],
            "{}",
            shared.name
        );
        assert_eq!(hits[0].kind, "invoice");
        assert!(hits[0].date.starts_with("2026-03-0"));
        let all = search
            .search(pb::SearchRequest::default())
            .await
            .unwrap()
            .into_inner()
            .hits;
        assert_eq!(all.len(), 4);
        invalid(
            &search
                .search(pb::SearchRequest {
                    text: "x".repeat(2000),
                    company_id: "nope".into(),
                    kind: "bad kind".into(),
                    date_from: "2026-04-01".into(),
                    date_to: "2026-03-01".into(),
                    amount_min: Some(10.0),
                    amount_max: Some(f64::NAN),
                    source_id: "nope".into(),
                    limit: 5000,
                    offset: 1_000_000,
                })
                .await
                .unwrap_err(),
            &[
                "text",
                "company_id",
                "kind",
                "date_to",
                "amount_max",
                "source_id",
                "limit",
                "offset",
            ],
        );
        a.stop().await;
        shared.drop_schema().await;
    }
}

/// `Neighbours` and `CompanyDocuments` read the node only when the query finds nothing (issue
/// #124): a missing node is still `NOT_FOUND`, a node with nothing to list an empty list, and
/// a non-company `INVALID_ARGUMENT`, whether or not it has edges.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn neighbours_and_company_documents_tell_missing_from_empty() {
    for shared in Shared::all().await {
        let name = shared.name;
        let a = Instance::start("inst-a", Some(shared.backend.clone())).await;
        let mut graph = GraphServiceClient::new(a.channel().await);
        let be = &shared.backend;
        let company = |key: &'static str| async move {
            be.upsert_node(NodeKind::Company, key, &json!({"name": key}))
                .await
                .unwrap()
        };
        let (lone, retracted, husk, survivor) = (
            company("e124-lone").await,
            company("e124-retracted").await,
            company("e124-husk").await,
            company("e124-survivor").await,
        );
        let doc = |n: u8| async move {
            be.upsert_node(NodeKind::Document, &hash(n).to_hex(), &json!({}))
                .await
                .unwrap()
        };
        let (party_doc, bare_doc, gone_doc, husk_doc) = (
            doc(124).await,
            doc(125).await,
            doc(126).await,
            doc(127).await,
        );
        be.upsert_edge(&Edge::new(
            party_doc,
            survivor,
            EdgeKind::Party,
            EdgeSource::Document,
        ))
        .await
        .unwrap();
        be.upsert_edge(
            &Edge::new(gone_doc, retracted, EdgeKind::Party, EdgeSource::Document)
                .with_state(EdgeState::Retracted),
        )
        .await
        .unwrap();
        be.upsert_edge(&Edge::new(
            husk_doc,
            husk,
            EdgeKind::Party,
            EdgeSource::Document,
        ))
        .await
        .unwrap();
        graph
            .merge_companies(pb::MergeCompaniesRequest {
                survivor_id: survivor.to_string(),
                absorbed_id: husk.to_string(),
                evidence_json: r#"[{"source": "manual", "note": "test"}]"#.into(),
            })
            .await
            .unwrap();

        let neighbours = |id: uuid::Uuid, direction: pb::Direction| {
            let mut g = graph.clone();
            async move {
                g.neighbours(pb::NeighboursRequest {
                    node_id: id.to_string(),
                    direction: direction as i32,
                    ..pb::NeighboursRequest::default()
                })
                .await
                .map(|r| r.into_inner().neighbours.len())
            }
        };
        let documents = |id: uuid::Uuid| {
            let mut g = graph.clone();
            async move {
                g.company_documents(pb::CompanyDocumentsRequest {
                    company_id: id.to_string(),
                })
                .await
                .map(|r| r.into_inner().documents.len())
            }
        };

        // A node that does not exist.
        let missing = uuid::Uuid::now_v7();
        let err = neighbours(missing, pb::Direction::Both).await.unwrap_err();
        assert_eq!(err.code(), Code::NotFound, "{name}: {err:?}");
        let err = documents(missing).await.unwrap_err();
        assert_eq!(err.code(), Code::NotFound, "{name}: {err:?}");

        // Nodes that exist with nothing to list: no edges at all, edges only the other way,
        // only an inactive edge, or a merged-away company whose edges moved to the survivor.
        for (id, direction) in [
            (lone, pb::Direction::Both),
            (bare_doc, pb::Direction::Both),
            (party_doc, pb::Direction::In),
            (retracted, pb::Direction::Both),
        ] {
            assert_eq!(
                neighbours(id, direction).await.unwrap(),
                0,
                "{name}: {id} {direction:?}"
            );
        }
        for id in [lone, retracted, husk] {
            assert_eq!(documents(id).await.unwrap(), 0, "{name}: {id}");
        }
        assert_eq!(documents(survivor).await.unwrap(), 2, "{name}");
        assert_eq!(
            neighbours(party_doc, pb::Direction::Out).await.unwrap(),
            1,
            "{name}"
        );

        // Not a company: a document with a `party` edge out, and one with no edges.
        for id in [party_doc, bare_doc] {
            invalid(&documents(id).await.unwrap_err(), &["company_id"]);
        }
        a.stop().await;
        shared.drop_schema().await;
    }
}

async fn next(s: &mut tonic::Streaming<pb::ProgressUpdate>) -> pb::ProgressUpdate {
    tokio::time::timeout(Duration::from_secs(10), s.message())
        .await
        .expect("an update within 10 s")
        .unwrap()
        .expect("the stream is open")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn watch_progress_streams_the_enrichment_counters() {
    let a = Instance::start("inst-a", Some(Shared::sqlite().backend)).await;
    let ch = a.channel().await;
    let mut jobs = JobsServiceClient::new(ch.clone());
    // No enrichment job (as in these tests): no enrichment update at all.
    let paused = {
        let mut req = add_request("http://minio.test:9000", "docs", "");
        req.paused = true;
        config(&ch).add_source(req).await.unwrap().into_inner()
    };
    let mut stream = jobs
        .watch_progress(pb::WatchProgressRequest { source_ids: vec![] })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(next(&mut stream).await.kind, Kind::Snapshot as i32);
    // The binary sets the counters once its job runs; they come as their own update.
    a.progress.set_enrichment(doewood_api::EnrichmentCounts {
        enriched: 3,
        merged: 1,
        registry_warnings: 2,
        ..doewood_api::EnrichmentCounts::default()
    });
    let u = loop {
        let u = next(&mut stream).await;
        if u.kind == Kind::Enrichment as i32 {
            break u;
        }
    };
    assert!(u.progress.is_none());
    let e = u.enrichment.unwrap();
    assert_eq!((e.enriched, e.merged, e.registry_warnings), (3, 1, 2));
    // A watch of named sources does not get it.
    let mut only = jobs
        .watch_progress(pb::WatchProgressRequest {
            source_ids: vec![paused.id.clone()],
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(next(&mut only).await.kind, Kind::Snapshot as i32);
    a.progress.set_enrichment(doewood_api::EnrichmentCounts {
        enriched: 4,
        ..doewood_api::EnrichmentCounts::default()
    });
    let quiet = tokio::time::timeout(Duration::from_millis(500), only.message()).await;
    assert!(
        quiet.is_err()
            || quiet
                .unwrap()
                .unwrap()
                .is_none_or(|u| u.kind != Kind::Enrichment as i32),
        "a filtered watch gets no enrichment update"
    );
    a.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn watch_progress_streams_counters_and_state_changes() {
    let a = Instance::start("inst-a", Some(Shared::sqlite().backend)).await;
    let ch = a.channel().await;
    let mut jobs = JobsServiceClient::new(ch.clone());

    // A paused source first: its snapshot comes at once.
    let mut req = add_request("http://minio.test:9000", "docs", "");
    req.paused = true;
    let paused = config(&ch).add_source(req).await.unwrap().into_inner();
    let mut stream = jobs
        .watch_progress(pb::WatchProgressRequest { source_ids: vec![] })
        .await
        .unwrap()
        .into_inner();
    let snap = next(&mut stream).await;
    assert_eq!(snap.kind, Kind::Snapshot as i32);
    let p = snap.progress.unwrap();
    assert_eq!(p.source_id, paused.id);
    assert_eq!(p.state, pb::SourceState::Paused as i32);
    assert!(!p.scanner_running);

    // Discovery events move the counters.
    let id: doewood_core::SourceId = paused.id.parse().unwrap();
    let v = ObjectVersion {
        etag: Some("e1".into()),
        size: 10,
        last_modified: None,
    };
    for kind in [
        EventKind::New(v.clone()),
        EventKind::New(v.clone()),
        EventKind::Changed(v),
    ] {
        a.events
            .send(ObjectEvent {
                source: id,
                key: "k".into(),
                kind,
                origin: EventOrigin::Webhook,
            })
            .await
            .unwrap();
    }
    let counters = loop {
        let u = next(&mut stream).await;
        assert_eq!(u.kind, Kind::Counters as i32);
        let c = u.progress.unwrap().counters.unwrap();
        if c.new == 2 && c.changed == 1 {
            break c;
        }
    };
    assert_eq!(
        (counters.deleted, counters.processed, counters.queued_llm),
        (0, 0, 0)
    );

    // A new active source against a closed port: it appears (SNAPSHOT), and its first pass
    // fails (the error may already show in the snapshot when the scanner is quick).
    let port = closed_port().await;
    let active = config(&ch)
        .add_source(add_request(&format!("http://127.0.0.1:{port}"), "docs", ""))
        .await
        .unwrap()
        .into_inner();
    let mut kinds = Vec::new();
    loop {
        let u = next(&mut stream).await;
        let p = u.progress.clone().unwrap();
        if p.source_id != active.id {
            continue;
        }
        kinds.push(u.kind);
        if p.state == pb::SourceState::Error as i32 {
            assert!(!p.state_reason.is_empty());
            assert_eq!(p.pass, 1);
            break;
        }
    }
    assert_eq!(kinds[0], Kind::Snapshot as i32, "{kinds:?}");
    // A state change through the API: error -> paused.
    config(&ch)
        .pause_source(pb::PauseSourceRequest {
            source_id: active.id.clone(),
        })
        .await
        .unwrap();
    let changed = loop {
        let u = next(&mut stream).await;
        if u.progress.as_ref().unwrap().source_id == active.id
            && u.kind == Kind::StateChanged as i32
        {
            break u;
        }
    };
    assert_eq!(changed.previous_state, pb::SourceState::Error as i32);
    let p = changed.progress.unwrap();
    assert_eq!(p.state, pb::SourceState::Paused as i32);
    assert!(!p.scanner_running);

    // Removing a source ends with a REMOVED update.
    config(&ch)
        .remove_source(pb::RemoveSourceRequest {
            source_id: paused.id.clone(),
        })
        .await
        .unwrap();
    loop {
        let u = next(&mut stream).await;
        if u.kind == Kind::Removed as i32 {
            assert_eq!(u.progress.unwrap().source_id, paused.id);
            assert_eq!(u.previous_state, pb::SourceState::Paused as i32);
            break;
        }
    }

    // A filtered watch sees only its source; unknown ids are refused.
    let mut only = jobs
        .watch_progress(pb::WatchProgressRequest {
            source_ids: vec![active.id.clone()],
        })
        .await
        .unwrap()
        .into_inner();
    let first = next(&mut only).await;
    assert_eq!(first.progress.unwrap().source_id, active.id);
    let err = jobs
        .watch_progress(pb::WatchProgressRequest {
            source_ids: vec![doewood_core::SourceId::generate().to_string()],
        })
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::NotFound);
    invalid(
        &jobs
            .watch_progress(pb::WatchProgressRequest {
                source_ids: vec!["x".into()],
            })
            .await
            .unwrap_err(),
        &["source_ids[0]"],
    );

    // Shutdown ends the open streams (the server does not hang on them).
    a.stop().await;
    let end = tokio::time::timeout(Duration::from_secs(5), stream.message())
        .await
        .expect("the stream ends on shutdown");
    assert!(!matches!(end, Ok(Some(_))), "{end:?}");
}

/// Reprocess jobs that walk through their states on a timer.
struct FakeReprocess {
    jobs: std::sync::Mutex<
        std::collections::HashMap<uuid::Uuid, tokio::sync::watch::Receiver<ReprocessView>>,
    >,
}

impl Reprocessor for FakeReprocess {
    fn start(
        &self,
        source: doewood_core::SourceId,
        force: bool,
    ) -> tokio::sync::watch::Receiver<ReprocessView> {
        let view = ReprocessView {
            job: uuid::Uuid::now_v7(),
            source,
            force,
            phase: ReprocessPhase::Selecting,
            examined: 0,
            selected: 0,
            processed: 0,
            failed: 0,
            pending: 0,
            error: None,
        };
        let (tx, rx) = tokio::sync::watch::channel(view.clone());
        self.jobs.lock().unwrap().insert(view.job, rx.clone());
        tokio::spawn(async move {
            for (phase, n) in [(ReprocessPhase::Running, 3), (ReprocessPhase::Done, 0)] {
                tokio::time::sleep(Duration::from_millis(30)).await;
                tx.send_modify(|v| {
                    v.phase = phase;
                    v.examined = 3;
                    v.selected = 3;
                    v.pending = n;
                    v.processed = 3 - n;
                });
            }
            // Keep the sender until every watcher saw the last state.
            tokio::time::sleep(Duration::from_secs(2)).await;
        });
        rx
    }

    fn watch(&self, job: uuid::Uuid) -> Option<tokio::sync::watch::Receiver<ReprocessView>> {
        self.jobs.lock().unwrap().get(&job).cloned()
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reprocess_jobs_stream_their_progress() {
    let fake = Arc::new(FakeReprocess {
        jobs: std::sync::Mutex::new(std::collections::HashMap::new()),
    });
    let a = Instance::start_with(
        "inst-a",
        Some(Shared::sqlite().backend),
        common::Options {
            reprocess: Some(fake),
            ..common::Options::default()
        },
    )
    .await;
    let ch = a.channel().await;
    let mut req = add_request("http://minio.test:9000", "docs", "");
    req.paused = true;
    let source = config(&ch).add_source(req).await.unwrap().into_inner();
    let mut jobs = JobsServiceClient::new(ch.clone());
    let job = jobs
        .reprocess_job(pb::ReprocessJobRequest {
            source_id: source.id.clone(),
            force: true,
        })
        .await
        .unwrap()
        .into_inner()
        .job_id;
    let mut stream = jobs
        .watch_reprocess(pb::WatchReprocessRequest {
            job_id: job.clone(),
        })
        .await
        .unwrap()
        .into_inner();
    let mut states = Vec::new();
    while let Some(p) = tokio::time::timeout(Duration::from_secs(5), stream.message())
        .await
        .expect("an update")
        .unwrap()
    {
        assert_eq!(
            (p.job_id.as_str(), p.source_id.as_str()),
            (job.as_str(), source.id.as_str())
        );
        assert!(p.force);
        states.push(p.state);
        if p.state == pb::reprocess_progress::State::Done as i32 {
            assert_eq!((p.selected, p.processed, p.pending), (3, 3, 0));
        }
    }
    assert_eq!(
        states.last().copied(),
        Some(pb::reprocess_progress::State::Done as i32),
        "the stream ends after the last state: {states:?}"
    );
    let err = jobs
        .watch_reprocess(pb::WatchReprocessRequest {
            job_id: uuid::Uuid::now_v7().to_string(),
        })
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::NotFound);
    invalid(
        &jobs
            .watch_reprocess(pb::WatchReprocessRequest {
                job_id: "nope".into(),
            })
            .await
            .unwrap_err(),
        &["job_id"],
    );
    a.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn without_ingest_jobs_and_summaries_are_unavailable_after_validation() {
    let a = Instance::start("inst-a", Some(Shared::sqlite().backend)).await;
    let ch = a.channel().await;
    let mut req = add_request("http://minio.test:9000", "docs", "");
    req.paused = true;
    let source = config(&ch).add_source(req).await.unwrap().into_inner();

    // Without ingest on the instance, reprocessing is unavailable (after validation).
    let mut jobs = JobsServiceClient::new(ch.clone());
    let err = jobs
        .reprocess_job(pb::ReprocessJobRequest {
            source_id: source.id.clone(),
            force: false,
        })
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::Unavailable);
    assert!(err.message().contains("ingest"), "{}", err.message());
    let err = jobs
        .reprocess_job(pb::ReprocessJobRequest {
            source_id: doewood_core::SourceId::generate().to_string(),
            force: false,
        })
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::NotFound);
    invalid(
        &jobs
            .reprocess_job(pb::ReprocessJobRequest::default())
            .await
            .unwrap_err(),
        &["source_id"],
    );

    let mut sum = SummarizeServiceClient::new(ch.clone());
    let err = sum
        .summarize(pb::SummarizeRequest {
            input: Some(pb::summarize_request::Input::Content(
                b"%PDF-1.7 test".to_vec(),
            )),
            name: "a.pdf".into(),
            provider: String::new(),
        })
        .await
        .unwrap_err();
    // Without ingest there is nothing to summarize with.
    assert_eq!(err.code(), Code::Unavailable);
    assert!(err.message().contains("ingest"), "{}", err.message());
    let err = sum
        .summarize(pb::SummarizeRequest {
            input: Some(pb::summarize_request::Input::Location(pb::S3Location {
                source_id: source.id.clone(),
                key: "a.pdf".into(),
            })),
            name: String::new(),
            provider: "not-configured".into(),
        })
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::NotFound);
    invalid(
        &sum.summarize(pb::SummarizeRequest::default())
            .await
            .unwrap_err(),
        &["input"],
    );
    invalid(
        &sum.summarize(pb::SummarizeRequest {
            input: Some(pb::summarize_request::Input::Content(Vec::new())),
            name: String::new(),
            provider: "Bad!".into(),
        })
        .await
        .unwrap_err(),
        &["content", "name", "provider"],
    );
    invalid(
        &sum.summarize(pb::SummarizeRequest {
            input: Some(pb::summarize_request::Input::Location(pb::S3Location {
                source_id: "x".into(),
                key: String::new(),
            })),
            ..pb::SummarizeRequest::default()
        })
        .await
        .unwrap_err(),
        &["location.source_id", "location.key"],
    );
    // A large file within the limit is decoded (and answered UNAVAILABLE: no ingest here).
    let big = vec![b'x'; 8 * 1024 * 1024];
    let err = sum
        .summarize(pb::SummarizeRequest {
            input: Some(pb::summarize_request::Input::Content(big)),
            name: "big.txt".into(),
            provider: String::new(),
        })
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::Unavailable);
    a.stop().await;
}

/// `Summarize` with ingest's part faked: what it is handed, and how its answers and errors
/// come out.
struct FakeOnDemand {
    seen: std::sync::Mutex<Vec<(doewood_api::OnDemandInput, Option<String>)>>,
    changed: std::sync::atomic::AtomicU32,
}

impl doewood_api::OnDemand for FakeOnDemand {
    fn summarize(
        &self,
        input: doewood_api::OnDemandInput,
        provider: Option<String>,
    ) -> doewood_api::BoxFuture<'_, Result<doewood_api::OnDemandResult, doewood_api::OnDemandError>>
    {
        Box::pin(async move {
            let fail = matches!(&input, doewood_api::OnDemandInput::Content { name, .. } if name == "fail.txt");
            let busy = matches!(&input, doewood_api::OnDemandInput::Content { name, .. } if name == "busy.txt");
            self.seen.lock().unwrap().push((input, provider.clone()));
            if busy {
                return Err(doewood_api::OnDemandError::ResourceExhausted(
                    "2 Summarize analyses are in flight, the most this instance runs at once: retry later".into(),
                ));
            }
            if fail {
                return Err(doewood_api::OnDemandError::NoProvider(
                    "no AI provider is configured: add one with ConfigService.SetAiProvider".into(),
                ));
            }
            Ok(doewood_api::OnDemandResult {
                digest: "A supply contract.".into(),
                checked: 3,
                supported: 2,
                unsupported: vec!["number:9".into()],
                extractive: false,
                provider: provider.unwrap_or_else(|| "local".into()),
                model: "vllm:test-model".into(),
                blake3: "ab".repeat(32),
                kind: "contract".into(),
                warnings: vec![],
            })
        })
    }

    fn settings_changed(&self) {
        self.changed
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn summarize_hands_requests_to_ingest() {
    let fake = Arc::new(FakeOnDemand {
        seen: std::sync::Mutex::new(Vec::new()),
        changed: std::sync::atomic::AtomicU32::new(0),
    });
    let a = Instance::start_with(
        "inst-a",
        Some(Shared::sqlite().backend),
        Options {
            summarize: Some(Arc::clone(&fake) as Arc<dyn doewood_api::OnDemand>),
            ..Options::default()
        },
    )
    .await;
    let ch = a.channel().await;
    let mut req = add_request("http://minio.test:9000", "docs", "");
    req.paused = true;
    let source = config(&ch).add_source(req).await.unwrap().into_inner();
    config(&ch)
        .set_ai_provider(pb::SetAiProviderRequest {
            name: "local".into(),
            preset: "vllm".into(),
            model: "test-model".into(),
            base_url: "http://127.0.0.1:9/v1".into(),
            ..pb::SetAiProviderRequest::default()
        })
        .await
        .unwrap();
    config(&ch)
        .set_llm_policy(pb::SetLlmPolicyRequest {
            source_id: source.id.clone(),
            policy: Some(pb::LlmPolicy {
                mode: pb::LlmMode::OnDemand as i32,
                ..pb::LlmPolicy::default()
            }),
        })
        .await
        .unwrap();
    // A provider and a policy change wake the queue.
    assert_eq!(fake.changed.load(std::sync::atomic::Ordering::Relaxed), 2);

    let mut sum = SummarizeServiceClient::new(ch.clone());
    let r = sum
        .summarize(pb::SummarizeRequest {
            input: Some(pb::summarize_request::Input::Location(pb::S3Location {
                source_id: source.id.clone(),
                key: "in/a.pdf".into(),
            })),
            name: String::new(),
            provider: "local".into(),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(r.digest, "A supply contract.");
    let fc = r.fact_check.unwrap();
    assert_eq!((fc.checked, fc.supported), (3, 2));
    assert_eq!(fc.unsupported, ["number:9"]);
    assert_eq!(
        (r.provider.as_str(), r.kind.as_str()),
        ("local", "contract")
    );
    let r = sum
        .summarize(pb::SummarizeRequest {
            input: Some(pb::summarize_request::Input::Content(b"text".to_vec())),
            name: "a.txt".into(),
            provider: String::new(),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(r.provider, "local");
    {
        let seen = fake.seen.lock().unwrap();
        assert_eq!(seen.len(), 2);
        assert!(matches!(
            &seen[0],
            (doewood_api::OnDemandInput::Location { key, .. }, Some(p)) if key == "in/a.pdf" && p == "local"
        ));
        assert!(matches!(
            &seen[1],
            (doewood_api::OnDemandInput::Content { name, bytes }, None) if name == "a.txt" && bytes == b"text"
        ));
    }
    // Ingest's errors are statuses with their message.
    let err = sum
        .summarize(pb::SummarizeRequest {
            input: Some(pb::summarize_request::Input::Content(b"text".to_vec())),
            name: "fail.txt".into(),
            provider: String::new(),
        })
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::FailedPrecondition);
    assert!(err.message().contains("SetAiProvider"), "{}", err.message());
    // Over the cap: RESOURCE_EXHAUSTED, with the reason.
    let err = sum
        .summarize(pb::SummarizeRequest {
            input: Some(pb::summarize_request::Input::Content(b"text".to_vec())),
            name: "busy.txt".into(),
            provider: String::new(),
        })
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::ResourceExhausted);
    assert!(err.message().contains("retry later"), "{}", err.message());
    a.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn messages_above_the_limit_are_refused() {
    let a = Instance::start_with(
        "inst-a",
        Some(Shared::sqlite().backend),
        Options {
            max_message_bytes: 64 * 1024,
            ..Options::default()
        },
    )
    .await;
    let ch = a.channel().await;
    let err = SummarizeServiceClient::new(ch)
        .summarize(pb::SummarizeRequest {
            input: Some(pb::summarize_request::Input::Content(vec![0; 128 * 1024])),
            name: "a.bin".into(),
            provider: String::new(),
        })
        .await
        .unwrap_err();
    assert!(
        matches!(err.code(), Code::OutOfRange | Code::ResourceExhausted),
        "{err:?}"
    );
    a.stop().await;
}

/// A browser's call: HTTP/1.1, `application/grpc-web+proto`, a length-prefixed message, and
/// the status in a trailer frame of the body.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn grpc_web_over_http1() {
    let a = Instance::start("inst-a", Some(Shared::sqlite().backend)).await;
    let mut s = tokio::net::TcpStream::connect(a.addr).await.unwrap();
    // ListAiProvidersRequest is empty: a frame of flag 0 and length 0.
    let body = [0u8, 0, 0, 0, 0];
    let head = format!(
        "POST /doewood.v1.ConfigService/ListAiProviders HTTP/1.1\r\nHost: {}\r\n\
         Content-Type: application/grpc-web+proto\r\nX-Grpc-Web: 1\r\nAccept: application/grpc-web+proto\r\n\
         Content-Length: {}\r\nConnection: close\r\n\r\n",
        a.addr,
        body.len()
    );
    s.write_all(head.as_bytes()).await.unwrap();
    s.write_all(&body).await.unwrap();
    let mut resp = Vec::new();
    tokio::time::timeout(Duration::from_secs(10), s.read_to_end(&mut resp))
        .await
        .unwrap()
        .unwrap();
    let text = String::from_utf8_lossy(&resp);
    assert!(text.starts_with("HTTP/1.1 200"), "{text}");
    assert!(
        text.to_ascii_lowercase()
            .contains("content-type: application/grpc-web+proto"),
        "{text}"
    );
    // The message (the preset list) and the trailers (grpc-status 0) are in the body.
    assert!(text.contains("openai_compatible"), "{text}");
    assert!(text.contains("grpc-status:0"), "{text}");
    a.stop().await;
}

/// The merge log through `GraphService` (ADR 0002 section 1.8): a person's merge, the log, undo
/// with its later-merge rule, split and its undo, dismissals, and the validation errors.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn merges_undo_split_and_dismissals() {
    for shared in Shared::all().await {
        let name = shared.name;
        let a = Instance::start("inst-a", Some(shared.backend.clone())).await;
        let mut graph = GraphServiceClient::new(a.channel().await);
        let be = &shared.backend;
        let company = |key: &'static str| async move {
            be.upsert_node(NodeKind::Company, key, &json!({"name": key}))
                .await
                .unwrap()
        };
        let (s, x, t) = (
            company("api-s").await,
            company("api-x").await,
            company("api-t").await,
        );
        let doc = be
            .upsert_node(NodeKind::Document, &hash(90).to_hex(), &json!({}))
            .await
            .unwrap();
        be.upsert_edge(&Edge::new(doc, x, EdgeKind::Party, EdgeSource::Document))
            .await
            .unwrap();
        be.claim_alias(NodeKind::Company, "id:rs_pib:100000002", x, &json!({}))
            .await
            .unwrap();
        let reader = graph.clone();
        let docs_of = |c: uuid::Uuid| {
            let mut g = reader.clone();
            async move {
                g.company_documents(pb::CompanyDocumentsRequest {
                    company_id: c.to_string(),
                })
                .await
                .unwrap()
                .into_inner()
                .documents
                .len()
            }
        };

        // Validation.
        invalid(
            &graph
                .merge_companies(pb::MergeCompaniesRequest {
                    survivor_id: s.to_string(),
                    absorbed_id: s.to_string(),
                    evidence_json: "{}".into(),
                })
                .await
                .unwrap_err(),
            &["absorbed_id", "evidence_json"],
        );
        invalid(
            &graph
                .list_merges(pb::ListMergesRequest {
                    company_id: "nope".into(),
                    page_size: 5000,
                    page_token: "x".into(),
                })
                .await
                .unwrap_err(),
            &["company_id", "page_size", "page_token"],
        );
        invalid(
            &graph
                .split_company(pb::SplitCompanyRequest {
                    company_id: s.to_string(),
                    document_ids: vec!["bad".into()],
                    ..Default::default()
                })
                .await
                .unwrap_err(),
            &["name", "document_ids[0]"],
        );
        invalid(
            &graph
                .dismiss_pair(pb::DismissPairRequest {
                    company_a_id: s.to_string(),
                    company_b_id: s.to_string(),
                    reason: "x".repeat(3000),
                })
                .await
                .unwrap_err(),
            &["company_b_id", "reason"],
        );
        let err = graph
            .merge_companies(pb::MergeCompaniesRequest {
                survivor_id: s.to_string(),
                absorbed_id: uuid::Uuid::now_v7().to_string(),
                evidence_json: String::new(),
            })
            .await
            .unwrap_err();
        assert_eq!(err.code(), Code::NotFound, "{name}: {err:?}");

        // A person's merge, logged with the instance.
        let m1 = graph
            .merge_companies(pb::MergeCompaniesRequest {
                survivor_id: s.to_string(),
                absorbed_id: x.to_string(),
                evidence_json: r#"[{"source": "manual", "note": "same PIB"}]"#.into(),
            })
            .await
            .unwrap()
            .into_inner();
        assert_eq!(m1.op, pb::MergeOp::Merge as i32);
        let who = m1.actor.clone().unwrap();
        assert_eq!(who.kind, pb::MergeActorKind::Manual as i32);
        assert_eq!(who.instance_id, "inst-a");
        assert_eq!(docs_of(s).await, 1, "{name}");
        assert_eq!(docs_of(x).await, 0, "{name}");
        let husk = graph
            .get_node(pb::GetNodeRequest {
                lookup: Some(pb::get_node_request::Lookup::Id(x.to_string())),
            })
            .await
            .unwrap()
            .into_inner();
        let props: serde_json::Value = serde_json::from_str(&husk.props_json).unwrap();
        assert_eq!(props["merged_into"], s.to_string());
        let m2 = graph
            .merge_companies(pb::MergeCompaniesRequest {
                survivor_id: t.to_string(),
                absorbed_id: s.to_string(),
                evidence_json: String::new(),
            })
            .await
            .unwrap()
            .into_inner();

        // The log, newest first, paged.
        let page = graph
            .list_merges(pb::ListMergesRequest {
                company_id: s.to_string(),
                page_size: 1,
                page_token: String::new(),
            })
            .await
            .unwrap()
            .into_inner();
        assert_eq!(page.merges.len(), 1);
        assert_eq!(page.merges[0].id, m2.id);
        let next = graph
            .list_merges(pb::ListMergesRequest {
                company_id: s.to_string(),
                page_size: 1,
                page_token: page.next_page_token,
            })
            .await
            .unwrap()
            .into_inner();
        assert_eq!(next.merges[0].id, m1.id);

        // Undo: refused while m2 depends on m1, then cascaded with a dismissal.
        let err = graph
            .undo_merge(pb::UndoMergeRequest {
                merge_id: m1.id.clone(),
                ..Default::default()
            })
            .await
            .unwrap_err();
        assert_eq!(err.code(), Code::FailedPrecondition, "{name}: {err:?}");
        assert!(err.message().contains(&m2.id), "{}", err.message());
        let out = graph
            .undo_merge(pb::UndoMergeRequest {
                merge_id: m1.id.clone(),
                cascade: true,
                dismiss: true,
                reason: "two companies".into(),
            })
            .await
            .unwrap()
            .into_inner();
        assert_eq!(out.undone_ids, [m2.id.clone(), m1.id.clone()]);
        let d = out.dismissal.unwrap();
        assert_eq!(d.merge_id, m1.id);
        assert_eq!(d.reason, "two companies");
        assert_eq!(docs_of(x).await, 1, "{name}: back where it was");
        let err = graph
            .merge_companies(pb::MergeCompaniesRequest {
                survivor_id: x.to_string(),
                absorbed_id: s.to_string(),
                evidence_json: String::new(),
            })
            .await
            .unwrap_err();
        assert_eq!(err.code(), Code::FailedPrecondition, "{name}: dismissed");
        let err = graph
            .undo_merge(pb::UndoMergeRequest {
                merge_id: m1.id.clone(),
                ..Default::default()
            })
            .await
            .unwrap_err();
        assert_eq!(
            err.code(),
            Code::FailedPrecondition,
            "{name}: already undone"
        );
        let listed = graph
            .list_merges(pb::ListMergesRequest {
                company_id: x.to_string(),
                ..Default::default()
            })
            .await
            .unwrap()
            .into_inner();
        assert!(
            listed
                .merges
                .iter()
                .all(|m| m.undone_at > 0 && m.undone_by.is_some())
        );
        assert!(listed.next_page_token.is_empty());

        // Split and its undo.
        let sp = graph
            .split_company(pb::SplitCompanyRequest {
                company_id: x.to_string(),
                name: "X Trading".into(),
                aliases: vec!["id:rs_pib:100000002".into()],
                document_ids: vec![doc.to_string()],
                reason: "a different company with the same name".into(),
                ..Default::default()
            })
            .await
            .unwrap()
            .into_inner();
        assert_eq!(sp.op, pb::MergeOp::Split as i32);
        let new: uuid::Uuid = sp.absorbed_id.parse().unwrap();
        assert_eq!(docs_of(new).await, 1, "{name}");
        assert_eq!(docs_of(x).await, 0, "{name}");
        let err = graph
            .split_company(pb::SplitCompanyRequest {
                company_id: x.to_string(),
                name: "Again".into(),
                aliases: vec!["id:rs_pib:100000002".into()],
                ..Default::default()
            })
            .await
            .unwrap_err();
        assert_eq!(
            err.code(),
            Code::InvalidArgument,
            "{name}: not x's alias now"
        );
        graph
            .undo_merge(pb::UndoMergeRequest {
                merge_id: sp.id.clone(),
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(docs_of(x).await, 1, "{name}");

        // Dismissals: a pair merged now is refused; otherwise recorded once.
        let p = company("api-p").await;
        let first = graph
            .dismiss_pair(pb::DismissPairRequest {
                company_a_id: p.to_string(),
                company_b_id: t.to_string(),
                reason: "not the same".into(),
            })
            .await
            .unwrap()
            .into_inner();
        assert_eq!(first.actor.unwrap().instance_id, "inst-a");
        let all = graph
            .list_dismissals(pb::ListDismissalsRequest {
                company_id: p.to_string(),
                ..Default::default()
            })
            .await
            .unwrap()
            .into_inner();
        assert_eq!(all.dismissals.len(), 1, "{name}");
        let page = graph
            .list_dismissals(pb::ListDismissalsRequest {
                page_size: 1,
                ..Default::default()
            })
            .await
            .unwrap()
            .into_inner();
        assert_eq!(page.dismissals.len(), 1);
        assert!(!page.next_page_token.is_empty(), "{name}: two dismissals");
        let err = graph
            .dismiss_pair(pb::DismissPairRequest {
                company_a_id: doc.to_string(),
                company_b_id: p.to_string(),
                reason: String::new(),
            })
            .await
            .unwrap_err();
        assert_eq!(err.code(), Code::InvalidArgument, "{name}: a document");
        a.stop().await;
        shared.drop_schema().await;
    }
}

/// Enrichment proposals through `GraphService` (issue #52): list with filters and pages,
/// accept a merge (through the merge log, so `UndoMerge` undoes it), accept a registry record,
/// reject with a dismissal that blocks the merge and the proposal, a dismissed pair refused,
/// and the validation errors.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn proposals_list_accept_and_reject() {
    use doewood_model::{ProposalReason, ProposalRequest};

    for shared in Shared::all().await {
        let name = shared.name;
        let a = Instance::start("inst-a", Some(shared.backend.clone())).await;
        let mut graph = GraphServiceClient::new(a.channel().await);
        let be = &shared.backend;
        let company = |key: &'static str| async move {
            be.upsert_node(NodeKind::Company, key, &json!({"name": key}))
                .await
                .unwrap()
        };
        let (p, q, r, w) = (
            company("prop-p").await,
            company("prop-q").await,
            company("prop-r").await,
            company("prop-w").await,
        );
        let doc = be
            .upsert_node(NodeKind::Document, &hash(91).to_hex(), &json!({}))
            .await
            .unwrap();
        be.upsert_edge(&Edge::new(doc, q, EdgeKind::Party, EdgeSource::Document))
            .await
            .unwrap();
        let evidence = json!([{"source": "name_ladder", "step": "form_blind"}]);
        let pair = |x: uuid::Uuid, y: uuid::Uuid| {
            ProposalRequest::merge(
                x,
                y,
                json!({"why": "the gate's review band"}),
                evidence.clone(),
                0.8,
                ProposalReason::ReviewBand,
            )
        };
        let pq = be.propose(&pair(p, q)).await.unwrap().unwrap();
        let rec = be
            .propose(&ProposalRequest::registry_record(
                r,
                "apr",
                "companies/11111111",
                json!({"registry": "apr", "record": "companies/11111111", "name": "Prop R d.o.o."}),
                json!([{"source": "fuzzy_name"}]),
                0.7,
                ProposalReason::ReviewBand,
            ))
            .await
            .unwrap()
            .unwrap();
        let rw = be.propose(&pair(r, w)).await.unwrap().unwrap();

        // Validation.
        invalid(
            &graph
                .list_proposals(pb::ListProposalsRequest {
                    state: 99,
                    kind: 99,
                    company_id: "nope".into(),
                    page_size: 5000,
                    page_token: "x".into(),
                })
                .await
                .unwrap_err(),
            &["state", "kind", "company_id", "page_size", "page_token"],
        );
        invalid(
            &graph
                .accept_proposal(pb::AcceptProposalRequest {
                    proposal_id: "x".into(),
                    survivor_id: "bad".into(),
                    note: "n".repeat(3000),
                })
                .await
                .unwrap_err(),
            &["proposal_id", "survivor_id", "note"],
        );
        invalid(
            &graph
                .reject_proposal(pb::RejectProposalRequest::default())
                .await
                .unwrap_err(),
            &["proposal_id"],
        );
        let err = graph
            .accept_proposal(pb::AcceptProposalRequest {
                proposal_id: uuid::Uuid::now_v7().to_string(),
                ..Default::default()
            })
            .await
            .unwrap_err();
        assert_eq!(err.code(), Code::NotFound, "{name}: {err:?}");

        // Listing: newest first, filters, pages.
        let lister = graph.clone();
        let list = |req: pb::ListProposalsRequest| {
            let mut g = lister.clone();
            async move { g.list_proposals(req).await.unwrap().into_inner() }
        };
        let all = list(pb::ListProposalsRequest::default()).await;
        let ids: Vec<_> = all.proposals.iter().map(|x| x.id.clone()).collect();
        assert_eq!(
            ids,
            [rw.id.to_string(), rec.id.to_string(), pq.id.to_string()],
            "{name}"
        );
        let got = &all.proposals[1];
        assert_eq!(got.kind, pb::ProposalKind::RegistryRecord as i32);
        assert_eq!(got.candidate, "apr:companies/11111111");
        assert_eq!(got.state, pb::ProposalState::Open as i32);
        assert_eq!(got.reason, pb::ProposalReason::ReviewBand as i32);
        assert!(
            got.detail_json.contains("Prop R d.o.o."),
            "{}",
            got.detail_json
        );
        assert!((got.probability - 0.7).abs() < 1e-6);
        assert_eq!((got.resolved_at, got.resolved_by.clone()), (0, None));
        let merges = list(pb::ListProposalsRequest {
            kind: pb::ProposalKind::Merge as i32,
            ..Default::default()
        })
        .await;
        assert_eq!(merges.proposals.len(), 2, "{name}");
        let of_q = list(pb::ListProposalsRequest {
            company_id: q.to_string(),
            ..Default::default()
        })
        .await;
        assert_eq!(of_q.proposals.len(), 1, "{name}: q is a merge candidate");
        assert_eq!(of_q.proposals[0].evidence_json, evidence.to_string());
        let first = list(pb::ListProposalsRequest {
            page_size: 2,
            ..Default::default()
        })
        .await;
        assert_eq!(first.proposals.len(), 2);
        assert!(!first.next_page_token.is_empty());
        let second = list(pb::ListProposalsRequest {
            page_size: 2,
            page_token: first.next_page_token,
            ..Default::default()
        })
        .await;
        assert_eq!(second.proposals.len(), 1);
        assert_eq!(second.proposals[0].id, pq.id.to_string());
        assert!(second.next_page_token.is_empty());

        // Accept a merge: logged as a person's, with the proposal's evidence; undone.
        let done = graph
            .accept_proposal(pb::AcceptProposalRequest {
                proposal_id: pq.id.to_string(),
                survivor_id: String::new(),
                note: "same company".into(),
            })
            .await
            .unwrap()
            .into_inner();
        let m = done.merge.expect("merged");
        assert_eq!(
            (m.survivor_id.as_str(), m.absorbed_id.as_str()),
            (q.to_string().as_str(), p.to_string().as_str()),
            "{name}: the one with more documents survives"
        );
        assert_eq!(
            m.actor.clone().unwrap().kind,
            pb::MergeActorKind::Manual as i32
        );
        assert_eq!(m.evidence_json, evidence.to_string());
        let accepted = done.proposal.expect("proposal");
        assert_eq!(accepted.state, pb::ProposalState::Accepted as i32);
        assert_eq!(accepted.merge_id, m.id);
        assert_eq!(accepted.note, "same company");
        assert_eq!(accepted.resolved_by.unwrap().instance_id, "inst-a");
        assert!(accepted.resolved_at > 0);
        let err = graph
            .accept_proposal(pb::AcceptProposalRequest {
                proposal_id: pq.id.to_string(),
                ..Default::default()
            })
            .await
            .unwrap_err();
        assert_eq!(err.code(), Code::FailedPrecondition, "{name}: {err:?}");
        let undone = graph
            .undo_merge(pb::UndoMergeRequest {
                merge_id: m.id.clone(),
                cascade: false,
                dismiss: false,
                reason: String::new(),
            })
            .await
            .unwrap()
            .into_inner();
        assert_eq!(undone.undone_ids, std::slice::from_ref(&m.id), "{name}");
        let accepted_only = list(pb::ListProposalsRequest {
            state: pb::ProposalState::Accepted as i32,
            ..Default::default()
        })
        .await;
        assert_eq!(accepted_only.proposals.len(), 1);

        // Accept a registry record: its identifier joins the company.
        let done = graph
            .accept_proposal(pb::AcceptProposalRequest {
                proposal_id: rec.id.to_string(),
                ..Default::default()
            })
            .await
            .unwrap()
            .into_inner();
        assert_eq!(done.alias, "id:rs_mb:11111111", "{name}");
        assert_eq!(done.merge, None);
        let held = be
            .alias(NodeKind::Company, "id:rs_mb:11111111")
            .await
            .unwrap()
            .expect("claimed");
        assert_eq!(held.node, Some(r), "{name}");

        // Reject with a dismissal: the pair is dismissed, never merged nor proposed again.
        let out = graph
            .reject_proposal(pb::RejectProposalRequest {
                proposal_id: rw.id.to_string(),
                dismiss: true,
                note: "different companies".into(),
            })
            .await
            .unwrap()
            .into_inner();
        let rejected = out.proposal.unwrap();
        assert_eq!(rejected.state, pb::ProposalState::Rejected as i32);
        assert!(rejected.dismissed);
        let d = out.dismissal.expect("dismissal");
        assert_eq!(d.reason, "different companies");
        let err = graph
            .merge_companies(pb::MergeCompaniesRequest {
                survivor_id: r.to_string(),
                absorbed_id: w.to_string(),
                evidence_json: String::new(),
            })
            .await
            .unwrap_err();
        assert_eq!(err.code(), Code::FailedPrecondition, "{name}: {err:?}");
        assert_eq!(be.propose(&pair(w, r)).await.unwrap(), None, "{name}");

        // A proposal whose pair was dismissed after it opened cannot be accepted.
        let pw = be.propose(&pair(p, w)).await.unwrap().unwrap();
        graph
            .dismiss_pair(pb::DismissPairRequest {
                company_a_id: p.to_string(),
                company_b_id: w.to_string(),
                reason: String::new(),
            })
            .await
            .unwrap();
        let err = graph
            .accept_proposal(pb::AcceptProposalRequest {
                proposal_id: pw.id.to_string(),
                ..Default::default()
            })
            .await
            .unwrap_err();
        assert_eq!(err.code(), Code::FailedPrecondition, "{name}: {err:?}");
        assert!(err.message().contains("dismissed"), "{}", err.message());
        // A plain rejection records no dismissal.
        let out = graph
            .reject_proposal(pb::RejectProposalRequest {
                proposal_id: pw.id.to_string(),
                dismiss: false,
                note: String::new(),
            })
            .await
            .unwrap()
            .into_inner();
        assert_eq!(out.dismissal, None);
        assert!(!out.proposal.unwrap().dismissed);
        a.stop().await;
        shared.drop_schema().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn health_and_reflection() {
    use tonic_reflection::pb::v1::ServerReflectionRequest;
    use tonic_reflection::pb::v1::server_reflection_client::ServerReflectionClient;
    use tonic_reflection::pb::v1::server_reflection_request::MessageRequest;
    use tonic_reflection::pb::v1::server_reflection_response::MessageResponse;

    let a = Instance::start("inst-a", Some(Shared::sqlite().backend)).await;
    let ch = a.channel().await;
    let mut health = tonic_health::pb::health_client::HealthClient::new(ch.clone());
    let serving = tonic_health::pb::health_check_response::ServingStatus::Serving as i32;
    for service in [
        "",
        "doewood.v1.ConfigService",
        "doewood.v1.JobsService",
        "doewood.v1.RecordsService",
        "doewood.v1.GraphService",
        "doewood.v1.SearchService",
        "doewood.v1.SummarizeService",
    ] {
        let status = health
            .check(tonic_health::pb::HealthCheckRequest {
                service: service.into(),
            })
            .await
            .unwrap()
            .into_inner()
            .status;
        assert_eq!(status, serving, "{service:?}");
    }

    let mut reflection = ServerReflectionClient::new(ch);
    let ask = |r: MessageRequest| ServerReflectionRequest {
        host: String::new(),
        message_request: Some(r),
    };
    let requests = tokio_stream::iter(vec![
        ask(MessageRequest::ListServices(String::new())),
        ask(MessageRequest::FileContainingSymbol(
            "doewood.v1.ConfigService".into(),
        )),
    ]);
    let mut responses = reflection
        .server_reflection_info(requests)
        .await
        .unwrap()
        .into_inner();
    let Some(MessageResponse::ListServicesResponse(list)) =
        responses.message().await.unwrap().unwrap().message_response
    else {
        panic!("not a service list");
    };
    let names: Vec<_> = list.service.iter().map(|s| s.name.as_str()).collect();
    for want in [
        "doewood.v1.ConfigService",
        "doewood.v1.JobsService",
        "doewood.v1.RecordsService",
        "doewood.v1.GraphService",
        "doewood.v1.SearchService",
        "doewood.v1.SummarizeService",
        "grpc.health.v1.Health",
    ] {
        assert!(names.contains(&want), "{want} not in {names:?}");
    }
    let Some(MessageResponse::FileDescriptorResponse(files)) =
        responses.message().await.unwrap().unwrap().message_response
    else {
        panic!("not a file descriptor");
    };
    assert!(!files.file_descriptor_proto.is_empty());
    a.stop().await;
}

/// Adds a source to the instance's own store only, as an earlier run (or a standalone one) left
/// it: no claim in the shared store.
fn local_source(
    inst: &Instance,
    endpoint: &str,
    bucket: &str,
    state: doewood_store::SourceState,
) -> String {
    let mut new = doewood_store::NewSource::new(endpoint, bucket);
    new.state = state;
    inst.store.add_source(&new, None).unwrap().to_string()
}

async fn get_source(ch: &Channel, id: &str) -> pb::Source {
    config(ch)
        .get_source(pb::GetSourceRequest {
            source_id: id.to_owned(),
        })
        .await
        .unwrap()
        .into_inner()
}

/// `serve` with a shared store re-claims its sources at start: its own are claimed and scanned,
/// one another instance owns is marked and not scanned, and the start goes on (issue #12).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_start_up_pass_reclaims_sources_and_reports_conflicts() {
    use doewood_store::SourceState as S;
    for shared in Shared::all().await {
        let endpoint = format!("http://127.0.0.1:{}", closed_port().await);
        let a = Instance::start("inst-a", Some(shared.backend.clone())).await;
        let cha = a.channel().await;
        let taken = config(&cha)
            .add_source(add_request(&endpoint, "taken", ""))
            .await
            .unwrap()
            .into_inner();
        let mut paused_req = add_request(&endpoint, "taken-paused", "");
        paused_req.paused = true;
        config(&cha).add_source(paused_req).await.unwrap();

        let b = Instance::start_with(
            "inst-b",
            Some(shared.backend.clone()),
            Options {
                shared_claims: true,
                ..Options::default()
            },
        )
        .await;
        let chb = b.channel().await;
        let mine = local_source(&b, &endpoint, "mine", S::Active);
        let conflict = local_source(&b, &endpoint, "taken", S::Active);
        let paused = local_source(&b, &endpoint, "taken-paused", S::Paused);
        assert_eq!(b.discovery.len(), 0, "nothing is scanned before the pass");

        let found = doewood_api::reclaim_sources(&b.state, b.shutdown_rx())
            .await
            .unwrap();
        assert_eq!(
            found,
            doewood_api::Reclaimed {
                claimed: 1,
                conflicts: 2
            },
            "{}",
            shared.name
        );
        let key = |bucket: &str| doewood_model::SourceKey::new(&endpoint, bucket, "").unwrap();
        let claim = shared
            .backend
            .source_owner(&key("mine"))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(claim.owner.as_str(), "inst-b", "{}", shared.name);
        assert!(get_source(&chb, &mine).await.scanner_running);

        let c = get_source(&chb, &conflict).await;
        assert_eq!(c.state, pb::SourceState::Error as i32, "{}", shared.name);
        assert!(
            c.state_reason
                .starts_with("claimed by instance \"inst-a\" since 20"),
            "{}: {}",
            shared.name,
            c.state_reason
        );
        assert!(!c.scanner_running);
        let p = get_source(&chb, &paused).await;
        assert_eq!(p.state, pb::SourceState::Paused as i32);
        assert!(!p.scanner_running);
        // The owner keeps its claim.
        let owner = shared.backend.source_owner(&key("taken")).await.unwrap();
        assert_eq!(owner.unwrap().owner.as_str(), "inst-a");

        // A restart finds the same: its own claim kept (with its first time), the conflict again.
        let again = doewood_api::reclaim_sources(&b.state, b.shutdown_rx())
            .await
            .unwrap();
        assert_eq!(again, found);
        let kept = shared.backend.source_owner(&key("mine")).await.unwrap();
        assert_eq!(kept, Some(claim));
        assert!(!get_source(&chb, &conflict).await.scanner_running);

        // Updating a source claimed elsewhere does not start it.
        config(&chb)
            .update_source(pb::UpdateSourceRequest {
                source_id: conflict.clone(),
                scan_interval_seconds: Some(120),
                ..Default::default()
            })
            .await
            .unwrap();
        assert!(!get_source(&chb, &conflict).await.scanner_running);
        // Resuming it is refused, naming the owner; so is resuming the paused one.
        for id in [&conflict, &paused] {
            let err = config(&chb)
                .resume_source(pb::ResumeSourceRequest {
                    source_id: id.clone(),
                })
                .await
                .unwrap_err();
            assert_eq!(err.code(), Code::AlreadyExists, "{}: {err:?}", shared.name);
            assert!(err.message().contains("\"inst-a\""), "{}", err.message());
            let s = get_source(&chb, id).await;
            assert_eq!(s.state, pb::SourceState::Error as i32);
            assert!(s.state_reason.starts_with("claimed by instance"));
            assert!(!s.scanner_running);
        }

        // Once A gives it up, resuming claims it and scans it.
        config(&cha)
            .remove_source(pb::RemoveSourceRequest {
                source_id: taken.id,
            })
            .await
            .unwrap();
        config(&chb)
            .resume_source(pb::ResumeSourceRequest {
                source_id: conflict.clone(),
            })
            .await
            .unwrap();
        assert!(get_source(&chb, &conflict).await.scanner_running);
        let owner = shared.backend.source_owner(&key("taken")).await.unwrap();
        assert_eq!(owner.unwrap().owner.as_str(), "inst-b");

        a.stop().await;
        b.stop().await;
        shared.drop_schema().await;
    }
}

/// Before the shared store answers, resuming a source only stores its state; the start-up pass
/// claims it and starts it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn resuming_before_the_pass_waits_for_it() {
    use doewood_store::SourceState as S;
    for shared in Shared::all().await {
        let endpoint = format!("http://127.0.0.1:{}", closed_port().await);
        let b = Instance::start_with(
            "inst-b",
            None,
            Options {
                shared_claims: true,
                ..Options::default()
            },
        )
        .await;
        let chb = b.channel().await;
        let id = local_source(&b, &endpoint, "later", S::Paused);
        let pass = tokio::spawn({
            let (state, shutdown) = (b.state.clone(), b.shutdown_rx());
            async move { doewood_api::reclaim_sources(&state, shutdown).await }
        });
        let s = config(&chb)
            .resume_source(pb::ResumeSourceRequest {
                source_id: id.clone(),
            })
            .await
            .unwrap()
            .into_inner();
        assert_eq!(s.state, pb::SourceState::Active as i32);
        assert!(!s.scanner_running, "{}", shared.name);

        b.backend.send_replace(Some(shared.backend.clone()));
        let found = pass.await.unwrap().unwrap();
        assert_eq!(found.claimed, 1, "{}", shared.name);
        assert!(get_source(&chb, &id).await.scanner_running);
        let key = doewood_model::SourceKey::new(&endpoint, "later", "").unwrap();
        let owner = shared.backend.source_owner(&key).await.unwrap().unwrap();
        assert_eq!(owner.owner.as_str(), "inst-b");
        b.stop().await;
        shared.drop_schema().await;
    }
}
