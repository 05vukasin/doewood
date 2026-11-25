//! The request metrics (docs/api.md, "Metrics"), read back in the text format `/metrics`
//! serves: unary calls by code, a shed call, a gRPC-Web call, a stream counted while open
//! and when it ends, and the connections.

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
use prometheus::{Encoder as _, Registry, TextEncoder};
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tokio::sync::Notify;
use tonic::Code;

/// `Summarize` held until released, counting the calls that entered.
#[derive(Default)]
struct Held {
    entered: AtomicU32,
    release: Notify,
}

impl OnDemand for Held {
    fn summarize(
        &self,
        _input: OnDemandInput,
        _provider: Option<String>,
    ) -> BoxFuture<'_, Result<OnDemandResult, OnDemandError>> {
        self.entered.fetch_add(1, Ordering::SeqCst);
        Box::pin(async move {
            self.release.notified().await;
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

fn scrape(registry: &Registry) -> String {
    let mut out = Vec::new();
    TextEncoder::new()
        .encode(&registry.gather(), &mut out)
        .unwrap();
    String::from_utf8(out).unwrap()
}

/// The value of one integer series (labels in alphabetical order, as the encoder writes
/// them, then a bucket's `le`), or `None` when it is not there.
fn value(text: &str, series: &str) -> Option<u64> {
    text.lines()
        .find_map(|l| l.strip_prefix(series)?.strip_prefix(' ')?.parse().ok())
}

fn calls(text: &str, method: &str, code: &str) -> u64 {
    value(
        text,
        &format!("doewood_grpc_calls_total{{code=\"{code}\",method=\"doewood.v1.{method}\"}}"),
    )
    .unwrap_or(0)
}

fn in_flight(text: &str, method: &str) -> Option<u64> {
    value(
        text,
        &format!("doewood_grpc_calls_in_flight{{method=\"doewood.v1.{method}\"}}"),
    )
}

fn duration_count(text: &str, method: &str) -> Option<u64> {
    value(
        text,
        &format!("doewood_grpc_call_duration_seconds_count{{method=\"doewood.v1.{method}\"}}"),
    )
}

/// An instance whose `Summarize` is `held`, and the registry its metrics are on.
async fn start(held: &Arc<Held>) -> (Instance, Registry) {
    let a = Instance::start_with(
        "inst-a",
        Some(Shared::sqlite().backend),
        Options {
            summarize: Some(Arc::clone(held) as Arc<dyn OnDemand>),
            ..Options::default()
        },
    )
    .await;
    let registry = Registry::new();
    a.state.register_metrics(&registry).unwrap();
    (a, registry)
}

/// `ListAiProviders` over gRPC-Web (HTTP/1.1); returns the answer.
async fn grpc_web_call(a: &Instance) -> String {
    let head = format!(
        "POST /doewood.v1.ConfigService/ListAiProviders HTTP/1.1\r\nHost: {}\r\n\
         Content-Type: application/grpc-web+proto\r\nX-Grpc-Web: 1\r\n\
         Accept: application/grpc-web+proto\r\nContent-Length: 5\r\nConnection: close\r\n\r\n",
        a.addr
    );
    let mut req = head.into_bytes();
    req.extend_from_slice(&[0, 0, 0, 0, 0]);
    let mut s = tokio::net::TcpStream::connect(a.addr).await.unwrap();
    s.write_all(&req).await.unwrap();
    let mut resp = Vec::new();
    tokio::time::timeout(Duration::from_secs(10), s.read_to_end(&mut resp))
        .await
        .unwrap()
        .unwrap();
    String::from_utf8_lossy(&resp).to_ascii_lowercase()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unary_calls_are_counted_by_method_and_code() {
    let (a, registry) = start(&Arc::new(Held::default())).await;
    assert!(
        a.state.register_metrics(&registry).is_err(),
        "registered twice"
    );
    let text = scrape(&registry);
    assert_eq!(value(&text, "doewood_grpc_connections_open"), Some(0));
    assert_eq!(value(&text, "doewood_grpc_shed_total"), Some(0));

    let ch = a.channel().await;
    let mut config = ConfigServiceClient::new(ch.clone());
    // Two OK, one INVALID_ARGUMENT.
    for _ in 0..2 {
        config
            .list_sources(pb::ListSourcesRequest {})
            .await
            .unwrap();
    }
    let err = config
        .set_api_settings(pb::SetApiSettingsRequest {
            max_calls_in_flight: Some(0),
            deadline_seconds: None,
        })
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::InvalidArgument);
    let text = scrape(&registry);
    assert_eq!(calls(&text, "ConfigService/ListSources", "OK"), 2);
    assert_eq!(
        calls(&text, "ConfigService/SetApiSettings", "INVALID_ARGUMENT"),
        1
    );
    assert_eq!(calls(&text, "ConfigService/SetApiSettings", "OK"), 0);
    assert_eq!(duration_count(&text, "ConfigService/ListSources"), Some(2));
    assert_eq!(in_flight(&text, "ConfigService/ListSources"), Some(0));
    // Only methods that were called appear.
    assert_eq!(in_flight(&text, "GraphService/Neighbors"), None);
    assert_eq!(value(&text, "doewood_grpc_connections_open"), Some(1));
    assert_eq!(value(&text, "doewood_grpc_connections_total"), Some(1));

    // gRPC-Web is counted like gRPC (on a connection of its own).
    let web = grpc_web_call(&a).await;
    assert!(web.contains("grpc-status:0"), "{web}");
    let text = scrape(&registry);
    assert_eq!(calls(&text, "ConfigService/ListAiProviders", "OK"), 1);
    assert_eq!(value(&text, "doewood_grpc_connections_total"), Some(2));

    // An unknown method is `other`.
    let mut raw = tonic::client::Grpc::new(ch.clone());
    raw.ready().await.unwrap();
    let err = raw
        .unary::<pb::ListSourcesRequest, pb::ListSourcesResponse, _>(
            tonic::Request::new(pb::ListSourcesRequest {}),
            "/doewood.v1.NoSuchService/Nope".parse().unwrap(),
            tonic_prost::ProstCodec::default(),
        )
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::Unimplemented);
    let text = scrape(&registry);
    assert_eq!(
        value(
            &text,
            "doewood_grpc_calls_total{code=\"UNIMPLEMENTED\",method=\"other\"}"
        ),
        Some(1)
    );

    drop((config, raw, ch));
    a.stop().await;
    let text = scrape(&registry);
    assert_eq!(value(&text, "doewood_grpc_connections_open"), Some(0));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stream_is_in_flight_while_open_and_counted_when_it_ends() {
    let (a, registry) = start(&Arc::new(Held::default())).await;
    let ch = a.channel().await;
    let mut stream = JobsServiceClient::new(ch.clone())
        .watch_progress(pb::WatchProgressRequest { source_ids: vec![] })
        .await
        .unwrap()
        .into_inner();
    a.progress.set_enrichment(doewood_api::EnrichmentCounts {
        enriched: 1,
        ..doewood_api::EnrichmentCounts::default()
    });
    loop {
        let u = tokio::time::timeout(Duration::from_secs(10), stream.message())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        if u.kind == Kind::Enrichment as i32 {
            break;
        }
    }
    let text = scrape(&registry);
    assert_eq!(in_flight(&text, "JobsService/WatchProgress"), Some(1));
    assert_eq!(duration_count(&text, "JobsService/WatchProgress"), Some(0));
    // The client drops it: CANCELLED, with its lifetime as its duration.
    drop(stream);
    eventually("the stream ends", || async {
        in_flight(&scrape(&registry), "JobsService/WatchProgress") == Some(0)
    })
    .await;
    let text = scrape(&registry);
    assert_eq!(calls(&text, "JobsService/WatchProgress", "CANCELLED"), 1);
    assert_eq!(duration_count(&text, "JobsService/WatchProgress"), Some(1));
    drop(ch);
    a.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shed_calls_are_counted() {
    let held = Arc::new(Held::default());
    let (a, registry) = start(&held).await;
    let ch = a.channel().await;
    let mut config = ConfigServiceClient::new(ch.clone());
    // One held call fills a cap of 1; the next is refused, counted as UNAVAILABLE and shed.
    config
        .set_api_settings(pb::SetApiSettingsRequest {
            max_calls_in_flight: Some(1),
            deadline_seconds: None,
        })
        .await
        .unwrap();
    let mut summarize = SummarizeServiceClient::new(ch.clone());
    let running = tokio::spawn(async move {
        summarize
            .summarize(pb::SummarizeRequest {
                input: Some(pb::summarize_request::Input::Content(b"text".to_vec())),
                name: "a.txt".into(),
                provider: String::new(),
            })
            .await
    });
    eventually("the call runs", || async {
        held.entered.load(Ordering::SeqCst) == 1
    })
    .await;
    let text = scrape(&registry);
    assert_eq!(in_flight(&text, "SummarizeService/Summarize"), Some(1));
    let err = config
        .list_sources(pb::ListSourcesRequest {})
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::Unavailable);
    held.release.notify_waiters();
    assert_eq!(running.await.unwrap().unwrap().into_inner().digest, "held");
    let text = scrape(&registry);
    assert_eq!(value(&text, "doewood_grpc_shed_total"), Some(1));
    assert_eq!(calls(&text, "ConfigService/ListSources", "UNAVAILABLE"), 1);
    assert_eq!(calls(&text, "SummarizeService/Summarize", "OK"), 1);
    assert_eq!(in_flight(&text, "SummarizeService/Summarize"), Some(0));
    assert_eq!(
        value(
            &text,
            "doewood_grpc_call_duration_seconds_bucket{\
             method=\"doewood.v1.SummarizeService/Summarize\",le=\"+Inf\"}"
        ),
        Some(1)
    );
    drop((config, ch));
    a.stop().await;
}
