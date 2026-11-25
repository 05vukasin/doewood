//! The server's limits (docs/api.md, "Limits"): the runtime settings, shedding beyond the
//! in-flight cap (gRPC and gRPC-Web, not health checks, open streams or the icon route), the
//! deadline that leaves open streams and `Summarize` alone, and the streams per connection.

mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use common::{Instance, Options, Shared, eventually};
use doewood_api::pb::config_service_client::ConfigServiceClient;
use doewood_api::pb::jobs_service_client::JobsServiceClient;
use doewood_api::pb::summarize_service_client::SummarizeServiceClient;
use doewood_api::pb::{self, progress_update::Kind};
use doewood_api::{BoxFuture, OnDemand, OnDemandError, OnDemandInput, OnDemandResult};
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tokio::sync::Notify;
use tonic::Code;
use tonic::transport::Channel;
use tonic_health::pb::HealthCheckRequest;
use tonic_health::pb::health_client::HealthClient;

fn config(ch: &Channel) -> ConfigServiceClient<Channel> {
    ConfigServiceClient::new(ch.clone())
}

async fn set_limits(ch: &Channel, max: Option<u32>, deadline: Option<u32>) -> pb::ApiSettings {
    config(ch)
        .set_api_settings(pb::SetApiSettingsRequest {
            max_calls_in_flight: max,
            deadline_seconds: deadline,
        })
        .await
        .unwrap()
        .into_inner()
}

/// `Summarize` held until released (or for `hold`), counting the calls that entered.
struct Held {
    entered: AtomicU32,
    release: Notify,
    hold: Duration,
}

impl Held {
    fn new(hold: Duration) -> Arc<Self> {
        Arc::new(Self {
            entered: AtomicU32::new(0),
            release: Notify::new(),
            hold,
        })
    }
}

impl OnDemand for Held {
    fn summarize(
        &self,
        _input: OnDemandInput,
        _provider: Option<String>,
    ) -> BoxFuture<'_, Result<OnDemandResult, OnDemandError>> {
        self.entered.fetch_add(1, Ordering::SeqCst);
        Box::pin(async move {
            tokio::select! {
                () = self.release.notified() => {}
                () = tokio::time::sleep(self.hold) => {}
            }
            Ok(OnDemandResult {
                digest: "held".into(),
                checked: 0,
                supported: 0,
                unsupported: vec![],
                extractive: true,
                provider: "local".into(),
                model: "test".into(),
                blake3: "ab".repeat(32),
                kind: "note".into(),
                warnings: vec![],
            })
        })
    }

    fn settings_changed(&self) {}
}

fn summarize_request() -> pb::SummarizeRequest {
    pb::SummarizeRequest {
        input: Some(pb::summarize_request::Input::Content(b"text".to_vec())),
        name: "a.txt".into(),
        provider: String::new(),
    }
}

async fn start(held: &Arc<Held>) -> Instance {
    Instance::start_with(
        "inst-a",
        Some(Shared::sqlite().backend),
        Options {
            summarize: Some(Arc::clone(held) as Arc<dyn OnDemand>),
            ..Options::default()
        },
    )
    .await
}

/// A raw HTTP/1.1 exchange; returns the whole answer as text.
async fn http1(a: &Instance, request: &[u8]) -> String {
    let mut s = tokio::net::TcpStream::connect(a.addr).await.unwrap();
    s.write_all(request).await.unwrap();
    let mut resp = Vec::new();
    tokio::time::timeout(Duration::from_secs(10), s.read_to_end(&mut resp))
        .await
        .unwrap()
        .unwrap();
    String::from_utf8_lossy(&resp).into_owned()
}

/// `ListAiProviders` over gRPC-Web.
async fn grpc_web_call(a: &Instance) -> String {
    let head = format!(
        "POST /doewood.v1.ConfigService/ListAiProviders HTTP/1.1\r\nHost: {}\r\n\
         Content-Type: application/grpc-web+proto\r\nX-Grpc-Web: 1\r\n\
         Accept: application/grpc-web+proto\r\nContent-Length: 5\r\nConnection: close\r\n\r\n",
        a.addr
    );
    let mut req = head.into_bytes();
    req.extend_from_slice(&[0, 0, 0, 0, 0]);
    http1(a, &req).await.to_ascii_lowercase()
}

/// The retry pushback (issue #104) in milliseconds, if the answer has one.
fn pushback(md: &tonic::metadata::MetadataMap) -> Option<u64> {
    md.get("grpc-retry-pushback-ms")
        .map(|v| v.to_str().unwrap().parse().unwrap())
}

/// A paused source, so a watch of every source starts with a snapshot.
async fn add_paused_source(ch: &Channel) {
    config(ch)
        .add_source(pb::AddSourceRequest {
            endpoint: "http://minio.test:9000".into(),
            bucket: "docs".into(),
            credentials: Some(pb::S3Credentials {
                access_key_id: "test-access-key-id".into(),
                secret_access_key: "test-secret-access-key".into(),
                session_token: String::new(),
            }),
            scan_interval_seconds: 3600,
            paused: true,
            ..pb::AddSourceRequest::default()
        })
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn api_settings_round_trip_and_validation() {
    let a = Instance::start("inst-a", Some(Shared::sqlite().backend)).await;
    let ch = a.channel().await;
    let get = || async {
        config(&ch)
            .get_api_settings(pb::GetApiSettingsRequest {})
            .await
            .unwrap()
            .into_inner()
    };
    let d = get().await;
    assert_eq!((d.max_calls_in_flight, d.deadline_seconds), (256, 60));

    let set = set_limits(&ch, Some(512), None).await;
    assert_eq!((set.max_calls_in_flight, set.deadline_seconds), (512, 60));
    let set = set_limits(&ch, None, Some(120)).await;
    assert_eq!((set.max_calls_in_flight, set.deadline_seconds), (512, 120));
    assert_eq!(get().await, set);
    assert_eq!(
        a.store.api_settings().unwrap(),
        doewood_store::ApiSettings {
            max_calls_in_flight: 512,
            deadline_seconds: 120,
        }
    );

    for (max, deadline, field) in [
        (Some(0), None, "max_calls_in_flight"),
        (Some(65_537), Some(30), "max_calls_in_flight"),
        (None, Some(0), "deadline_seconds"),
        (Some(10), Some(3601), "deadline_seconds"),
    ] {
        let err = config(&ch)
            .set_api_settings(pb::SetApiSettingsRequest {
                max_calls_in_flight: max,
                deadline_seconds: deadline,
            })
            .await
            .unwrap_err();
        assert_eq!(err.code(), Code::InvalidArgument, "{err:?}");
        assert!(err.message().contains(field), "{}", err.message());
    }
    // A refused request changes nothing, not even its valid field.
    assert_eq!(get().await, set);
    a.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn calls_beyond_the_cap_are_shed() {
    let held = Held::new(Duration::from_mins(1));
    let a = start(&held).await;
    let ch = a.channel().await;
    add_paused_source(&ch).await;
    set_limits(&ch, Some(2), None).await;

    // Open streams do not count: three watches, more than the cap, all open.
    let mut jobs = JobsServiceClient::new(ch.clone());
    let mut streams = Vec::new();
    for _ in 0..3 {
        let mut s = jobs
            .watch_progress(pb::WatchProgressRequest { source_ids: vec![] })
            .await
            .unwrap()
            .into_inner();
        let first = tokio::time::timeout(Duration::from_secs(10), s.message())
            .await
            .expect("a snapshot within 10 s")
            .unwrap()
            .unwrap();
        assert_eq!(first.kind, Kind::Snapshot as i32);
        streams.push(s);
    }

    // Two held calls fill the cap.
    let calls: Vec<_> = (0..2)
        .map(|_| {
            let mut c = SummarizeServiceClient::new(ch.clone());
            tokio::spawn(async move { c.summarize(summarize_request()).await })
        })
        .collect();
    eventually("both calls run", || async {
        held.entered.load(Ordering::SeqCst) == 2
    })
    .await;

    // The next call is refused at once.
    let started = tokio::time::Instant::now();
    let err = config(&ch)
        .list_sources(pb::ListSourcesRequest {})
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::Unavailable, "{err:?}");
    assert!(err.message().contains("overloaded"), "{}", err.message());
    assert!(started.elapsed() < Duration::from_secs(2));
    // It tells the client how long to back off (issue #104).
    let ms = pushback(err.metadata()).expect("a retry pushback");
    assert!((100..200).contains(&ms), "{ms}");
    // So is a gRPC-Web call, with the status and the pushback in its headers.
    let web = grpc_web_call(&a).await;
    assert!(web.starts_with("http/1.1 200"), "{web}");
    assert!(web.contains("grpc-status: 14"), "{web}");
    assert!(web.contains("grpc-retry-pushback-ms: 1"), "{web}");
    // Health checks still answer, without a pushback.
    let health = HealthClient::new(ch.clone())
        .check(HealthCheckRequest {
            service: String::new(),
        })
        .await
        .unwrap();
    assert_eq!(pushback(health.metadata()), None);
    let health = health.into_inner();
    assert_eq!(
        health.status,
        tonic_health::pb::health_check_response::ServingStatus::Serving as i32
    );
    // The icon route has its own limits (no icons configured: a miss, not a gRPC error).
    let icon = http1(
        &a,
        format!(
            "GET /icons/acme.test HTTP/1.1\r\nHost: {}\r\nConnection: close\r\n\r\n",
            a.addr
        )
        .as_bytes(),
    )
    .await;
    assert!(!icon.starts_with("HTTP/1.1 200"), "{icon}");
    assert!(!icon.to_ascii_lowercase().contains("grpc-status"), "{icon}");

    // Once the held calls finish, calls go through again.
    held.release.notify_waiters();
    for c in calls {
        assert_eq!(c.await.unwrap().unwrap().into_inner().digest, "held");
    }
    config(&ch)
        .list_sources(pb::ListSourcesRequest {})
        .await
        .unwrap();
    let web = grpc_web_call(&a).await;
    assert!(web.contains("grpc-status:0"), "{web}");
    assert!(!web.contains("grpc-retry-pushback-ms"), "{web}");
    // The streams were not touched.
    a.progress.set_enrichment(doewood_api::EnrichmentCounts {
        enriched: 1,
        ..doewood_api::EnrichmentCounts::default()
    });
    for s in &mut streams {
        let u = loop {
            let u = tokio::time::timeout(Duration::from_secs(10), s.message())
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            if u.kind == Kind::Enrichment as i32 {
                break u;
            }
        };
        assert_eq!(u.enrichment.unwrap().enriched, 1);
    }
    a.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_deadline_leaves_open_streams_and_summarize_alone() {
    let held = Held::new(Duration::from_millis(1500));
    let a = start(&held).await;
    let ch = a.channel().await;
    add_paused_source(&ch).await;
    set_limits(&ch, None, Some(1)).await;

    let mut stream = JobsServiceClient::new(ch.clone())
        .watch_progress(pb::WatchProgressRequest { source_ids: vec![] })
        .await
        .unwrap()
        .into_inner();
    let first = tokio::time::timeout(Duration::from_secs(10), stream.message())
        .await
        .expect("a snapshot within 10 s")
        .unwrap()
        .unwrap();
    assert_eq!(first.kind, Kind::Snapshot as i32);

    // `Summarize` runs longer than the deadline and still answers.
    let r = SummarizeServiceClient::new(ch.clone())
        .summarize(summarize_request())
        .await
        .unwrap()
        .into_inner();
    assert_eq!(r.digest, "held");

    // The stream is still open well past the deadline.
    tokio::time::sleep(Duration::from_secs(1)).await;
    a.progress.set_enrichment(doewood_api::EnrichmentCounts {
        enriched: 7,
        ..doewood_api::EnrichmentCounts::default()
    });
    let u = loop {
        let u = tokio::time::timeout(Duration::from_secs(10), stream.message())
            .await
            .unwrap()
            .unwrap()
            .expect("the stream is open");
        if u.kind == Kind::Enrichment as i32 {
            break u;
        }
    };
    assert_eq!(u.enrichment.unwrap().enriched, 7);
    // A quick unary call is unaffected by the short deadline.
    config(&ch)
        .list_sources(pb::ListSourcesRequest {})
        .await
        .unwrap();
    a.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn one_connection_opens_at_most_256_streams() {
    let a = Instance::start("inst-a", Some(Shared::sqlite().backend)).await;
    let ch = a.channel().await;
    let watch = || {
        let mut h = HealthClient::new(ch.clone());
        async move {
            h.watch(HealthCheckRequest {
                service: String::new(),
            })
            .await
            .map(tonic::Response::into_inner)
        }
    };
    let mut open = Vec::new();
    for _ in 0..doewood_api::MAX_CONCURRENT_STREAMS {
        open.push(watch().await.unwrap());
    }
    // The next stream waits in the client until one closes.
    let next = tokio::spawn(watch());
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(!next.is_finished(), "a stream beyond the limit opened");
    drop(open.pop());
    let s = tokio::time::timeout(Duration::from_secs(10), next)
        .await
        .expect("the waiting stream opens once one closes")
        .unwrap();
    assert!(s.is_ok(), "{s:?}");
    // Health watches do not end on shutdown by themselves: close them first.
    drop((s, open));
    a.stop().await;
}
