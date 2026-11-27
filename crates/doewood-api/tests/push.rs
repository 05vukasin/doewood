//! Push sources over gRPC (ADR 0004): `AddSource` with `kind: SOURCE_KIND_PUSH` (claimed, no
//! scanner), the S3-only RPCs refused for one, and `IngestService` against a fake ingest: the
//! stream's checks, the answer, and `RESOURCE_EXHAUSTED` with a retry pushback. Shared backends:
//! SQLite always, Postgres when `DOEWOOD_TEST_POSTGRES_URL` is set.

mod common;

use std::sync::{Arc, Mutex};

use common::{Instance, Options, Shared};
use doewood_api::pb::config_service_client::ConfigServiceClient;
use doewood_api::pb::ingest_service_client::IngestServiceClient;
use doewood_api::pb::jobs_service_client::JobsServiceClient;
use doewood_api::pb::{self, put_object_request::Part};
use doewood_api::{
    BoxFuture, PushFailure, PushHeader, PushOutcome, PushResult, PushUpload, Pusher,
};
use doewood_core::SourceId;
use doewood_model::{Blake3, Record, SourceKey};
use serde_json::value::RawValue;
use tonic::Code;

fn push_request() -> pb::AddSourceRequest {
    pb::AddSourceRequest {
        kind: pb::SourceKind::Push as i32,
        ..pb::AddSourceRequest::default()
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_push_source_is_claimed_and_never_scanned() {
    for shared in Shared::all().await {
        let name = shared.name;
        let a = Instance::start("inst-a", Some(Arc::clone(&shared.backend))).await;
        let mut config = ConfigServiceClient::new(a.channel().await);
        let s = config
            .add_source(push_request())
            .await
            .unwrap()
            .into_inner();
        assert_eq!(s.kind, pb::SourceKind::Push as i32, "{name}");
        assert_eq!(s.endpoint, format!("push:{}", s.id), "{name}");
        assert!(s.bucket.is_empty() && s.prefix.is_empty(), "{name}");
        assert_eq!(s.state, pb::SourceState::Active as i32, "{name}");
        // Active, and still no scanner.
        assert!(!s.scanner_running, "{name}");
        assert!(a.discovery.is_empty(), "{name}");
        let id: SourceId = s.id.parse().unwrap();
        let owner = shared
            .backend
            .source_owner(&SourceKey::push(id))
            .await
            .unwrap()
            .expect("claimed");
        assert_eq!(owner.owner.as_str(), "inst-a", "{name}");
        // Pause and resume: still no scanner.
        let req = pb::ResumeSourceRequest {
            source_id: s.id.clone(),
        };
        config
            .pause_source(pb::PauseSourceRequest {
                source_id: s.id.clone(),
            })
            .await
            .unwrap();
        let resumed = config.resume_source(req).await.unwrap().into_inner();
        assert!(!resumed.scanner_running, "{name}");
        assert!(a.discovery.is_empty(), "{name}");
        // Removing it releases the claim.
        let removed = config
            .remove_source(pb::RemoveSourceRequest {
                source_id: s.id.clone(),
            })
            .await
            .unwrap()
            .into_inner();
        assert!(removed.claim_released, "{name}");
        a.stop().await;
        shared.drop_schema().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn s3_options_and_rpcs_are_refused_for_a_push_source() {
    let shared = Shared::sqlite();
    let a = Instance::start("inst-a", Some(Arc::clone(&shared.backend))).await;
    let mut config = ConfigServiceClient::new(a.channel().await);
    let bad = config
        .add_source(pb::AddSourceRequest {
            bucket: "docs".into(),
            scan_interval_seconds: 60,
            ..push_request()
        })
        .await
        .unwrap_err();
    assert_eq!(bad.code(), Code::InvalidArgument);
    assert!(bad.message().contains("bucket"), "{}", bad.message());
    assert!(
        bad.message().contains("scan_interval_seconds"),
        "{}",
        bad.message()
    );
    let s = config
        .add_source(pb::AddSourceRequest {
            max_object_bytes: 1 << 20,
            ..push_request()
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(s.max_object_bytes, 1 << 20);
    let e = config
        .update_source(pb::UpdateSourceRequest {
            source_id: s.id.clone(),
            region: Some("eu-west-1".into()),
            ..pb::UpdateSourceRequest::default()
        })
        .await
        .unwrap_err();
    assert_eq!(e.code(), Code::FailedPrecondition);
    let updated = config
        .update_source(pb::UpdateSourceRequest {
            source_id: s.id.clone(),
            max_object_bytes: Some(2 << 20),
            ..pb::UpdateSourceRequest::default()
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(updated.max_object_bytes, 2 << 20);
    let e = config
        .set_sqs_inlet(pb::SetSqsInletRequest {
            source_id: s.id.clone(),
            queue_url: "https://sqs.eu-central-1.amazonaws.com/123456789012/q".into(),
            ..pb::SetSqsInletRequest::default()
        })
        .await
        .unwrap_err();
    assert_eq!(e.code(), Code::FailedPrecondition);
    let e = JobsServiceClient::new(a.channel().await)
        .reprocess_job(pb::ReprocessJobRequest {
            source_id: s.id.clone(),
            force: false,
        })
        .await
        .unwrap_err();
    assert_eq!(e.code(), Code::FailedPrecondition);
    // The claim of a push source can be released by its key.
    let e = config
        .release_source_claim(pb::ReleaseSourceClaimRequest {
            endpoint: format!("push:{}", s.id),
            owner: "inst-a".into(),
            ..pb::ReleaseSourceClaimRequest::default()
        })
        .await
        .unwrap_err();
    // Its own source: remove it instead.
    assert_eq!(e.code(), Code::FailedPrecondition, "{}", e.message());
    a.stop().await;
}

/// What the fake ingest saw and answers.
#[derive(Default)]
struct Fake {
    headers: Mutex<Vec<PushHeader>>,
    bytes: Arc<Mutex<Vec<Vec<u8>>>>,
    exhausted: bool,
}

struct FakeUpload(Arc<Mutex<Vec<Vec<u8>>>>);

fn record(bytes: &[u8]) -> Record {
    Record {
        // Not a real hash: the fake stands for ingest.
        blake3: Blake3([u8::try_from(bytes.len() % 256).unwrap(); 32]),
        body: RawValue::from_string(r#"{"format":"text"}"#.to_owned()).unwrap(),
        versions: [("doewood.ingest".to_owned(), "1".to_owned())].into(),
        updated_at: 1,
    }
}

impl PushUpload for FakeUpload {
    fn finish(
        self: Box<Self>,
        bytes: Vec<u8>,
    ) -> BoxFuture<'static, Result<PushResult, PushFailure>> {
        Box::pin(async move {
            let r = record(&bytes);
            self.0.lock().unwrap().push(bytes);
            Ok(PushResult {
                record: r,
                kind: "invoice".into(),
                outcome: PushOutcome::Analyzed,
                skipped: false,
            })
        })
    }
}

impl Pusher for Fake {
    fn admit(&self, header: PushHeader) -> BoxFuture<'_, Result<Box<dyn PushUpload>, PushFailure>> {
        Box::pin(async move {
            self.headers.lock().unwrap().push(header);
            if self.exhausted {
                return Err(PushFailure::ResourceExhausted("all slots busy".into()));
            }
            Ok(Box::new(FakeUpload(Arc::clone(&self.bytes))) as Box<dyn PushUpload>)
        })
    }

    fn move_location(
        &self,
        _source: SourceId,
        _from: String,
        _to: String,
    ) -> BoxFuture<'_, Result<PushResult, PushFailure>> {
        Box::pin(async { Err(PushFailure::NotFound("no such object".into())) })
    }

    fn remove_location(
        &self,
        _source: SourceId,
        _key: String,
    ) -> BoxFuture<'_, Result<bool, PushFailure>> {
        Box::pin(async { Ok(true) })
    }
}

fn header(source: &str, key: &str, size: u64) -> pb::PutObjectRequest {
    pb::PutObjectRequest {
        part: Some(Part::Header(pb::PutObjectHeader {
            source_id: source.into(),
            key: key.into(),
            name: String::new(),
            mime_type: "text/plain".into(),
            size,
        })),
    }
}

fn chunk(bytes: &[u8]) -> pb::PutObjectRequest {
    pb::PutObjectRequest {
        part: Some(Part::Chunk(bytes.to_vec())),
    }
}

async fn put(
    client: &mut IngestServiceClient<tonic::transport::Channel>,
    messages: Vec<pb::PutObjectRequest>,
) -> Result<tonic::Response<pb::PutObjectResponse>, tonic::Status> {
    client.put_object(tokio_stream::iter(messages)).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn put_object_streams_the_bytes_to_ingest() {
    let fake = Arc::new(Fake::default());
    let a = Instance::start_with(
        "inst-a",
        Some(Arc::clone(&Shared::sqlite().backend)),
        Options {
            push: Some(Arc::clone(&fake) as Arc<dyn Pusher>),
            // Small messages: the file needs several chunks.
            max_message_bytes: 1024,
            ..Options::default()
        },
    )
    .await;
    let mut client = IngestServiceClient::new(a.channel().await);
    let source = SourceId::generate().to_string();
    let file: Vec<u8> = (0..3000u32).map(|i| (i % 251) as u8).collect();
    let mut messages = vec![header(&source, "drive/a.txt", file.len() as u64)];
    messages.extend(file.chunks(700).map(chunk));
    let r = put(&mut client, messages).await.unwrap().into_inner();
    assert_eq!(r.status, pb::PutStatus::Analyzed as i32);
    assert_eq!(r.kind, "invoice");
    assert_eq!(r.blake3, record(&file).blake3.to_hex());
    assert_eq!(r.record.unwrap().body_json, r#"{"format":"text"}"#);
    assert_eq!(fake.bytes.lock().unwrap().as_slice(), std::slice::from_ref(&file));
    {
        let h = fake.headers.lock().unwrap();
        assert_eq!(h[0].key, "drive/a.txt");
        assert_eq!(h[0].size, 3000);
    }
    // No header first, a second header, too many or too few bytes, an empty file.
    for (what, messages) in [
        ("no header", vec![chunk(b"abc")]),
        (
            "two headers",
            vec![header(&source, "k", 3), header(&source, "k", 3)],
        ),
        ("too many", vec![header(&source, "k", 2), chunk(b"abc")]),
        ("too few", vec![header(&source, "k", 4), chunk(b"abc")]),
        ("empty", vec![header(&source, "k", 0)]),
        ("no key", vec![header(&source, "", 3), chunk(b"abc")]),
        ("bad source", vec![header("nope", "k", 3), chunk(b"abc")]),
    ] {
        let e = put(&mut client, messages).await.unwrap_err();
        assert_eq!(e.code(), Code::InvalidArgument, "{what}: {}", e.message());
    }
    // A chunk over the message limit is refused by the transport.
    let e = put(
        &mut client,
        vec![header(&source, "k", 2000), chunk(&[0; 2000])],
    )
    .await
    .unwrap_err();
    assert_eq!(e.code(), Code::OutOfRange, "{}", e.message());
    // Move and remove go to ingest.
    let e = client
        .move_location(pb::MoveLocationRequest {
            source_id: source.clone(),
            old_key: "a".into(),
            new_key: "a".into(),
        })
        .await
        .unwrap_err();
    assert_eq!(e.code(), Code::InvalidArgument);
    let e = client
        .move_location(pb::MoveLocationRequest {
            source_id: source.clone(),
            old_key: "a".into(),
            new_key: "b".into(),
        })
        .await
        .unwrap_err();
    assert_eq!(e.code(), Code::NotFound);
    let removed = client
        .remove_location(pb::RemoveLocationRequest {
            source_id: source.clone(),
            key: "a".into(),
        })
        .await
        .unwrap()
        .into_inner();
    assert!(removed.removed);
    a.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_shed_put_carries_a_retry_pushback() {
    let fake = Arc::new(Fake {
        exhausted: true,
        ..Fake::default()
    });
    let a = Instance::start_with(
        "inst-a",
        Some(Arc::clone(&Shared::sqlite().backend)),
        Options {
            push: Some(Arc::clone(&fake) as Arc<dyn Pusher>),
            ..Options::default()
        },
    )
    .await;
    let mut client = IngestServiceClient::new(a.channel().await);
    let source = SourceId::generate().to_string();
    let e = put(&mut client, vec![header(&source, "k", 3), chunk(b"abc")])
        .await
        .unwrap_err();
    assert_eq!(e.code(), Code::ResourceExhausted);
    let ms: u64 = e
        .metadata()
        .get("grpc-retry-pushback-ms")
        .expect("a retry pushback")
        .to_str()
        .unwrap()
        .parse()
        .unwrap();
    assert!((100..200).contains(&ms), "{ms}");
    // Nothing was read.
    assert!(fake.bytes.lock().unwrap().is_empty());
    a.stop().await;

    // Without ingest, UNAVAILABLE.
    let b = Instance::start("inst-b", Some(Arc::clone(&Shared::sqlite().backend))).await;
    let mut client = IngestServiceClient::new(b.channel().await);
    let e = put(&mut client, vec![header(&source, "k", 3), chunk(b"abc")])
        .await
        .unwrap_err();
    assert_eq!(e.code(), Code::Unavailable);
    b.stop().await;
}
