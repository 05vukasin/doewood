//! The peak memory of `ListRecords` over big records (issue #111): an ignored measurement, run
//! by hand (docs/api.md, "`ListRecords` page budget"). Linux only: it reads and resets the
//! process's peak RSS through `/proc/self`.

#![allow(clippy::cast_precision_loss, clippy::too_many_lines)]

mod common;

use std::time::Instant;

use common::{Instance, Shared};
use doewood_api::pb::config_service_client::ConfigServiceClient;
use doewood_api::pb::records_service_client::RecordsServiceClient;
use doewood_api::pb::{self};
use doewood_model::{Blake3, ObjectLocation};
use serde_json::json;

/// A `/proc/self/status` field in KiB.
fn status_kib(field: &str) -> u64 {
    let status = std::fs::read_to_string("/proc/self/status").unwrap();
    status
        .lines()
        .find_map(|l| l.strip_prefix(field))
        .and_then(|v| v.trim().trim_end_matches(" kB").trim().parse().ok())
        .unwrap()
}

/// Resets the peak RSS to the current RSS.
fn reset_peak() {
    std::fs::write("/proc/self/clear_refs", "5").unwrap();
}

/// `DOEWOOD_BENCH_RECORDS` records (default 150) of `DOEWOOD_BENCH_RECORD_KIB` KiB each
/// (default 256) in one source; one `ListRecords` call with `page_size` 1000, then a walk of
/// every page. Prints the peak RSS above the RSS before each, and the time.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "a measurement; run by hand"]
async fn list_records_peak_memory() {
    let env = |k: &str, d: usize| {
        std::env::var(k)
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(d)
    };
    let (n, kib) = (
        env("DOEWOOD_BENCH_RECORDS", 150),
        env("DOEWOOD_BENCH_RECORD_KIB", 256),
    );
    for shared in Shared::all().await {
        let a = Instance::start("inst-a", Some(shared.backend.clone())).await;
        let ch = a.channel().await;
        let source = ConfigServiceClient::new(ch.clone())
            .add_source(pb::AddSourceRequest {
                endpoint: "http://minio.test:9000".into(),
                bucket: "big".into(),
                prefix: String::new(),
                credentials: Some(pb::S3Credentials {
                    access_key_id: "test-access-key-id-1234".into(),
                    secret_access_key: "test-secret-access-key-5678".into(),
                    session_token: String::new(),
                }),
                paused: true,
                ..pb::AddSourceRequest::default()
            })
            .await
            .unwrap()
            .into_inner();
        let pad = "x".repeat(kib << 10);
        for i in 0..n {
            let mut h = [0u8; 32];
            h[..8].copy_from_slice(&(i as u64).to_be_bytes());
            let body = doewood_model::canonical::to_canonical(&json!({"i": i, "pad": pad}));
            shared
                .backend
                .put_record(
                    &Blake3(h),
                    &body.unwrap(),
                    &[("quail".to_owned(), "1".to_owned())].into(),
                )
                .await
                .unwrap();
            shared
                .backend
                .upsert_location(
                    &Blake3(h),
                    &ObjectLocation {
                        endpoint: "http://minio.test:9000".into(),
                        bucket: "big".into(),
                        key: format!("f{i}.pdf"),
                    },
                )
                .await
                .unwrap();
        }
        shared.analyze().await;
        let records = RecordsServiceClient::new(ch.clone()).max_decoding_message_size(64 << 20);
        let page = |token: String| {
            let mut c = records.clone();
            let id = source.id.clone();
            async move {
                c.list_records(pb::ListRecordsRequest {
                    source_id: id,
                    page_size: 1000,
                    page_token: token,
                })
                .await
                .unwrap()
                .into_inner()
            }
        };
        // Warm up.
        drop(page(String::new()).await);
        let rss = status_kib("VmRSS:");
        reset_peak();
        let t = Instant::now();
        let first = page(String::new()).await;
        let one_ms = t.elapsed().as_secs_f64() * 1000.0;
        let one_peak = status_kib("VmHWM:").saturating_sub(rss);
        let first_len = first.records.len();
        drop(first);
        let rss = status_kib("VmRSS:");
        reset_peak();
        let t = Instant::now();
        let (mut token, mut pages, mut total) = (String::new(), 0, 0);
        loop {
            let p = page(token).await;
            pages += 1;
            total += p.records.len();
            if p.next_page_token.is_empty() {
                break;
            }
            token = p.next_page_token;
        }
        let walk_ms = t.elapsed().as_secs_f64() * 1000.0;
        let walk_peak = status_kib("VmHWM:").saturating_sub(rss);
        assert_eq!(total, n);
        println!(
            "{}: {n} records of {kib} KiB: first page {first_len} records, peak +{:.1} MiB, \
             {one_ms:.0} ms; all {pages} pages: peak +{:.1} MiB, {walk_ms:.0} ms",
            shared.name,
            one_peak as f64 / 1024.0,
            walk_peak as f64 / 1024.0,
        );
        a.stop().await;
        shared.drop_schema().await;
    }
}
