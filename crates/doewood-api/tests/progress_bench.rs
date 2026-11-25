//! `WatchProgress` under load: N streams of every source while one source holds 200 000
//! objects and something wakes the watchers every millisecond (as ingest does after every
//! object). Reports how long a trivial store call waits for the store's lock, and the CPU the
//! process spends.
//! `cargo test -p doewood-api --test progress_bench -- --ignored --nocapture`
//! (`DOEWOOD_BENCH_WATCHERS` sets N, default 0; `DOEWOOD_BENCH_OBJECTS` the object count).

mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use common::Instance;
use doewood_api::pb::{self, jobs_service_client::JobsServiceClient};
use doewood_store::{NewSource, ObjectChange, SeenObject};

fn env(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

/// User and system CPU time of this process (Linux), in seconds; 0 elsewhere.
fn cpu_seconds() -> f64 {
    let Ok(stat) = std::fs::read_to_string("/proc/self/stat") else {
        return 0.0;
    };
    // After the command name: utime is the 12th field, stime the 13th, in 1/100 s.
    let rest = stat.rsplit_once(')').map_or("", |(_, r)| r);
    let f: Vec<f64> = rest
        .split_whitespace()
        .skip(11)
        .take(2)
        .filter_map(|v| v.parse().ok())
        .collect();
    f.iter().sum::<f64>() / 100.0
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "a measurement, not a check"]
async fn watchers_on_a_large_source() {
    let watchers = env("DOEWOOD_BENCH_WATCHERS", 0);
    let objects = env("DOEWOOD_BENCH_OBJECTS", 200_000);
    let a = Instance::start("bench", None).await;
    let source = a
        .store
        .add_source(&NewSource::new("http://objects.test:9000", "docs"), None)
        .unwrap();
    let keys: Vec<ObjectChange> = (0..objects)
        .map(|i| {
            ObjectChange::Upsert(SeenObject {
                key: format!("docs/{i:08}.pdf"),
                etag: Some(format!("{i:x}")),
                size: 1000,
                last_modified: Some(1),
            })
        })
        .collect();
    for chunk in keys.chunks(10_000) {
        a.store.record_objects(source, chunk, None).unwrap();
    }

    let messages = Arc::new(AtomicU64::new(0));
    let mut readers = Vec::new();
    for _ in 0..watchers {
        let mut stream = JobsServiceClient::new(a.channel().await)
            .watch_progress(pb::WatchProgressRequest { source_ids: vec![] })
            .await
            .unwrap()
            .into_inner();
        let messages = Arc::clone(&messages);
        readers.push(tokio::spawn(async move {
            while let Ok(Some(_)) = stream.message().await {
                messages.fetch_add(1, Ordering::Relaxed);
            }
        }));
    }
    // Let the first snapshots go out.
    tokio::time::sleep(Duration::from_millis(500)).await;

    let run = Duration::from_secs(5);
    let stop = Arc::new(AtomicBool::new(false));
    let probe = {
        let (store, progress, stop) = (
            Arc::clone(&a.store),
            Arc::clone(&a.progress),
            Arc::clone(&stop),
        );
        std::thread::spawn(move || {
            let mut waits = Vec::new();
            while !stop.load(Ordering::Relaxed) {
                progress.touch();
                let t = Instant::now();
                store.schema_version().unwrap();
                waits.push(t.elapsed());
                std::thread::sleep(Duration::from_millis(1));
            }
            waits
        })
    };
    let cpu = cpu_seconds();
    tokio::time::sleep(run).await;
    let cpu = cpu_seconds() - cpu;
    stop.store(true, Ordering::Relaxed);
    let mut waits = probe.join().unwrap();
    waits.sort();
    let total: Duration = waits.iter().sum();
    let mean = total / u32::try_from(waits.len()).unwrap();
    println!(
        "progress_bench: {watchers} watchers, {objects} objects, {}s: store lock wait mean \
         {:.0} us, p99 {:.0} us, max {:.1} ms ({} probes); CPU {cpu:.2}s; {} messages",
        run.as_secs(),
        mean.as_secs_f64() * 1e6,
        waits[waits.len() * 99 / 100].as_secs_f64() * 1e6,
        waits[waits.len() - 1].as_secs_f64() * 1e3,
        waits.len(),
        messages.load(Ordering::Relaxed),
    );
    a.stop().await;
    for r in readers {
        r.await.unwrap();
    }
}
