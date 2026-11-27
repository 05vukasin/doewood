//! `PurgeSource`, `PurgeOrphans` and `WatchPurge` (issue #168) against an in-process server,
//! on SQLite and (when configured) Postgres: a source purge removes the source and what only
//! it held, an orphan purge what no location holds, a purge waits for ingest to let go of the
//! source, and an interrupted purge resumes.

#![allow(clippy::too_many_lines)]

mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use common::{Instance, Options, Shared, eventually};
use doewood_api::pb::config_service_client::ConfigServiceClient;
use doewood_api::pb::jobs_service_client::JobsServiceClient;
use doewood_api::pb::records_service_client::RecordsServiceClient;
use doewood_api::pb::{self, purge_progress::State};
use doewood_api::{ReprocessView, Reprocessor};
use doewood_model::{Blake3, Edge, EdgeKind, EdgeSource, NodeKind, ObjectLocation};
use doewood_store::SourceState;
use serde_json::json;
use tonic::Code;
use tonic::transport::Channel;

const ENDPOINT: &str = "http://minio.test:9000";

fn hash(n: u8) -> Blake3 {
    Blake3([n; 32])
}

fn add_request(prefix: &str) -> pb::AddSourceRequest {
    pb::AddSourceRequest {
        endpoint: ENDPOINT.into(),
        bucket: "docs".into(),
        prefix: prefix.into(),
        paused: true,
        scan_interval_seconds: 3600,
        ..pb::AddSourceRequest::default()
    }
}

fn at(key: &str) -> ObjectLocation {
    ObjectLocation {
        endpoint: ENDPOINT.into(),
        bucket: "docs".into(),
        key: key.into(),
    }
}

/// Streams the job to its last update; returns every state seen and the last update.
async fn watch(ch: &Channel, job: &str) -> (Vec<i32>, pb::PurgeProgress) {
    let mut stream = JobsServiceClient::new(ch.clone())
        .watch_purge(pb::WatchPurgeRequest {
            job_id: job.to_owned(),
        })
        .await
        .unwrap()
        .into_inner();
    let (mut states, mut last) = (Vec::new(), None);
    while let Some(p) = tokio::time::timeout(Duration::from_secs(30), stream.message())
        .await
        .expect("an update")
        .unwrap()
    {
        assert_eq!(p.job_id, job);
        states.push(p.state);
        last = Some(p);
    }
    (states, last.expect("at least one update"))
}

async fn record(ch: &Channel, n: u8) -> Code {
    match RecordsServiceClient::new(ch.clone())
        .get_record(pb::GetRecordRequest {
            blake3: hash(n).to_hex(),
        })
        .await
    {
        Ok(_) => Code::Ok,
        Err(e) => e.code(),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_source_purge_removes_the_source_and_what_only_it_held() {
    for shared in Shared::all().await {
        let name = shared.name;
        let a = Instance::start("inst-a", Some(shared.backend.clone())).await;
        let ch = a.channel().await;
        let mut config = ConfigServiceClient::new(ch.clone());
        let mut jobs = JobsServiceClient::new(ch.clone());
        let source = config
            .add_source(add_request("in/"))
            .await
            .unwrap()
            .into_inner();
        config.add_source(add_request("out/")).await.unwrap();
        // 1: only in the source; 2: also elsewhere; 3: elsewhere only; 4: no location.
        let be = &shared.backend;
        for n in 1..=4 {
            be.put_record(
                &hash(n),
                &doewood_model::canonical::to_canonical(&json!({"n": n})).unwrap(),
                &[("quail".to_owned(), "1".to_owned())].into(),
            )
            .await
            .unwrap();
        }
        let d1 = be.upsert_location(&hash(1), &at("in/a.pdf")).await.unwrap();
        be.upsert_location(&hash(2), &at("in/b.pdf")).await.unwrap();
        be.upsert_location(&hash(2), &at("out/b.pdf"))
            .await
            .unwrap();
        be.upsert_location(&hash(3), &at("out/c.pdf"))
            .await
            .unwrap();
        let company = be
            .upsert_node(
                NodeKind::Company,
                "purge-api",
                &json!({"name": "Purge API"}),
            )
            .await
            .unwrap();
        be.upsert_edge(
            &Edge::new(d1.document, company, EdgeKind::Party, EdgeSource::Document)
                .with_props(json!({"role": "buyer"})),
        )
        .await
        .unwrap();
        let id: doewood_core::SourceId = source.id.parse().unwrap();
        a.store.queue_llm_job(id, &hash(1).0, None).unwrap();

        // A source whose objects overlap: refused, nothing changes.
        let sub = config
            .add_source(add_request("in/sub/"))
            .await
            .unwrap()
            .into_inner();
        let err = jobs
            .purge_source(pb::PurgeSourceRequest {
                source_id: source.id.clone(),
            })
            .await
            .unwrap_err();
        assert_eq!(err.code(), Code::FailedPrecondition, "{name}: {err:?}");
        assert!(err.message().contains(&sub.id), "{name}: {}", err.message());
        config
            .remove_source(pb::RemoveSourceRequest { source_id: sub.id })
            .await
            .unwrap();

        let job = jobs
            .purge_source(pb::PurgeSourceRequest {
                source_id: source.id.clone(),
            })
            .await
            .unwrap()
            .into_inner()
            .job_id;
        let (states, last) = watch(&ch, &job).await;
        assert_eq!(last.state, State::Done as i32, "{name}: {last:?}");
        assert_eq!(states.last(), Some(&(State::Done as i32)));
        assert_eq!(last.source_id, source.id);
        assert_eq!(
            (last.records, last.companies, last.llm_jobs),
            (1, 1, 1),
            "{name}: {last:?}"
        );
        assert!(last.locations >= 2, "{name}: {last:?}");
        assert!(last.batches >= 2, "{name}: {last:?}");
        // The source is gone, as RemoveSource removes it, and its claim with it.
        let err = config
            .get_source(pb::GetSourceRequest {
                source_id: source.id.clone(),
            })
            .await
            .unwrap_err();
        assert_eq!(err.code(), Code::NotFound);
        let key = doewood_model::SourceKey::new(ENDPOINT, "docs", "in/").unwrap();
        assert_eq!(be.source_owner(&key).await.unwrap(), None, "{name}");
        assert!(a.store.llm_job(id, &hash(1).0).unwrap().is_none());
        assert_eq!(record(&ch, 1).await, Code::NotFound, "{name}");
        assert_eq!(record(&ch, 2).await, Code::Ok, "{name}");
        assert_eq!(record(&ch, 3).await, Code::Ok, "{name}");
        assert_eq!(record(&ch, 4).await, Code::Ok, "{name}: not the source's");

        // The orphans: 4.
        let job = jobs
            .purge_orphans(pb::PurgeOrphansRequest {})
            .await
            .unwrap()
            .into_inner()
            .job_id;
        let (_, last) = watch(&ch, &job).await;
        assert_eq!(last.state, State::Done as i32, "{name}: {last:?}");
        assert_eq!(last.source_id, "");
        assert_eq!(last.records, 1, "{name}: {last:?}");
        assert_eq!(record(&ch, 4).await, Code::NotFound, "{name}");
        assert_eq!(record(&ch, 2).await, Code::Ok, "{name}");

        // Unknown and malformed jobs and sources.
        let err = jobs
            .watch_purge(pb::WatchPurgeRequest {
                job_id: uuid::Uuid::now_v7().to_string(),
            })
            .await
            .unwrap_err();
        assert_eq!(err.code(), Code::NotFound);
        let err = jobs
            .watch_purge(pb::WatchPurgeRequest {
                job_id: "nope".into(),
            })
            .await
            .unwrap_err();
        assert_eq!(err.code(), Code::InvalidArgument);
        let err = jobs
            .purge_source(pb::PurgeSourceRequest {
                source_id: source.id.clone(),
            })
            .await
            .unwrap_err();
        assert_eq!(err.code(), Code::NotFound);
        let err = jobs
            .purge_source(pb::PurgeSourceRequest {
                source_id: String::new(),
            })
            .await
            .unwrap_err();
        assert_eq!(err.code(), Code::InvalidArgument);
        a.stop().await;
        shared.drop_schema().await;
    }
}

/// Ingest holding files of the source: the purge waits until it lets go.
struct Holding(AtomicUsize);

impl Reprocessor for Holding {
    fn start(
        &self,
        _source: doewood_core::SourceId,
        _force: bool,
    ) -> tokio::sync::watch::Receiver<ReprocessView> {
        unreachable!("no reprocess in this test")
    }

    fn watch(&self, _job: uuid::Uuid) -> Option<tokio::sync::watch::Receiver<ReprocessView>> {
        None
    }

    fn in_flight(&self, _source: doewood_core::SourceId) -> usize {
        self.0.load(Ordering::SeqCst)
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_purge_waits_for_ingest_and_resumes_after_an_interruption() {
    let holding = Arc::new(Holding(AtomicUsize::new(2)));
    let shared = Shared::sqlite();
    let a = Instance::start_with(
        "inst-a",
        Some(shared.backend.clone()),
        Options {
            reprocess: Some(Arc::clone(&holding) as Arc<dyn Reprocessor>),
            ..Options::default()
        },
    )
    .await;
    let ch = a.channel().await;
    let mut config = ConfigServiceClient::new(ch.clone());
    let mut jobs = JobsServiceClient::new(ch.clone());
    let source = config
        .add_source(add_request("in/"))
        .await
        .unwrap()
        .into_inner();
    let be = &shared.backend;
    be.put_record(
        &hash(1),
        &doewood_model::canonical::to_canonical(&json!({"n": 1})).unwrap(),
        &[("quail".to_owned(), "1".to_owned())].into(),
    )
    .await
    .unwrap();
    be.upsert_location(&hash(1), &at("in/a.pdf")).await.unwrap();
    let job = jobs
        .purge_source(pb::PurgeSourceRequest {
            source_id: source.id.clone(),
        })
        .await
        .unwrap()
        .into_inner()
        .job_id;
    // The same job again while it runs.
    let again = jobs
        .purge_source(pb::PurgeSourceRequest {
            source_id: source.id.clone(),
        })
        .await
        .unwrap()
        .into_inner()
        .job_id;
    assert_eq!(again, job);
    // Removed while it waits: nothing else changes it.
    let got = config
        .get_source(pb::GetSourceRequest {
            source_id: source.id.clone(),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(got.state, pb::SourceState::Removed as i32);
    for err in [
        config
            .resume_source(pb::ResumeSourceRequest {
                source_id: source.id.clone(),
            })
            .await
            .unwrap_err(),
        config
            .remove_source(pb::RemoveSourceRequest {
                source_id: source.id.clone(),
            })
            .await
            .unwrap_err(),
    ] {
        assert_eq!(err.code(), Code::FailedPrecondition, "{err:?}");
    }
    // Watched from before ingest lets go: waiting first, then running, then done.
    let watching = {
        let (ch, job) = (ch.clone(), job.clone());
        tokio::spawn(async move { watch(&ch, &job).await })
    };
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(
        record(&ch, 1).await,
        Code::Ok,
        "nothing goes while ingest holds files"
    );
    holding.0.store(0, Ordering::SeqCst);
    let (states, last) = watching.await.unwrap();
    assert_eq!(last.state, State::Done as i32, "{last:?}");
    assert_eq!(states.first(), Some(&(State::Waiting as i32)), "{states:?}");
    assert_eq!(last.records, 1, "{last:?}");
    assert_eq!(record(&ch, 1).await, Code::NotFound);

    // An interrupted purge: a source left `removed`, its rows still there. The start resumes
    // it.
    let other = config
        .add_source(add_request("again/"))
        .await
        .unwrap()
        .into_inner();
    be.put_record(
        &hash(2),
        &doewood_model::canonical::to_canonical(&json!({"n": 2})).unwrap(),
        &[("quail".to_owned(), "1".to_owned())].into(),
    )
    .await
    .unwrap();
    be.upsert_location(&hash(2), &at("again/b.pdf"))
        .await
        .unwrap();
    let id: doewood_core::SourceId = other.id.parse().unwrap();
    a.store
        .set_source_state(id, SourceState::Removed, None)
        .unwrap();
    doewood_api::resume_purges(&a.state, a.shutdown_rx()).await;
    let ch2 = ch.clone();
    eventually("the resumed purge removes the source", move || {
        let (ch, id) = (ch2.clone(), other.id.clone());
        async move {
            ConfigServiceClient::new(ch)
                .get_source(pb::GetSourceRequest { source_id: id })
                .await
                .is_err_and(|e| e.code() == Code::NotFound)
        }
    })
    .await;
    assert_eq!(record(&ch, 2).await, Code::NotFound);
    a.stop().await;
}
