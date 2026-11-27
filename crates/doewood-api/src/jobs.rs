//! `JobsService`: the progress stream, reprocessing, and purges.

use std::pin::Pin;
use std::sync::Arc;

use tokio::sync::{mpsc, watch};
use tokio_stream::Stream;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};
use uuid::Uuid;

use crate::Api;
use crate::pb::jobs_service_server::JobsService;
use crate::pb::{
    ProgressUpdate, PurgeJobResponse, PurgeOrphansRequest, PurgeProgress, PurgeSourceRequest,
    ReprocessJobRequest, ReprocessJobResponse, ReprocessProgress, WatchProgressRequest,
    WatchPurgeRequest, WatchReprocessRequest, purge_progress, reprocess_progress,
};
use crate::progress::Watch;
use crate::purge::{PurgePhase, PurgeView};
use crate::reprocess::{ReprocessPhase, ReprocessView};
use crate::validate::{self, Violations};

/// Most sources one watch names.
const MAX_WATCHED: usize = 1000;

type ProgressStream = Pin<Box<dyn Stream<Item = Result<ProgressUpdate, Status>> + Send>>;
type ReprocessStream = Pin<Box<dyn Stream<Item = Result<ReprocessProgress, Status>> + Send>>;
type PurgeStream = Pin<Box<dyn Stream<Item = Result<PurgeProgress, Status>> + Send>>;

#[tonic::async_trait]
impl JobsService for Api {
    type WatchProgressStream = ProgressStream;
    type WatchReprocessStream = ReprocessStream;
    type WatchPurgeStream = PurgeStream;

    async fn watch_progress(
        &self,
        request: Request<WatchProgressRequest>,
    ) -> Result<Response<Self::WatchProgressStream>, Status> {
        let req = request.get_ref();
        let mut v = Violations::default();
        if req.source_ids.len() > MAX_WATCHED {
            v.add("source_ids", format!("at most {MAX_WATCHED}"));
        }
        let ids: Vec<_> = req
            .source_ids
            .iter()
            .enumerate()
            .filter_map(|(i, id)| validate::source_id(&mut v, &format!("source_ids[{i}]"), id))
            .collect();
        v.check()?;
        for id in &ids {
            self.state.source(*id).await?;
        }
        let watch = Watch {
            snapshots: Arc::clone(self.state.snapshots()),
            only: ids,
            shutdown: self.state.shutdown(),
        };
        let (tx, rx) = mpsc::channel(64);
        tokio::spawn(watch.run(tx));
        Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
    }

    async fn reprocess_job(
        &self,
        request: Request<ReprocessJobRequest>,
    ) -> Result<Response<ReprocessJobResponse>, Status> {
        let req = request.get_ref();
        let mut v = Violations::default();
        let id = validate::source_id(&mut v, "source_id", &req.source_id);
        v.check()?;
        let Some(id) = id else {
            return Err(Status::invalid_argument("source_id: required"));
        };
        self.state.source(id).await?;
        let Some(reprocess) = self.state.parts().reprocess.clone() else {
            return Err(Status::unavailable(
                "ingest is not running on this instance, so nothing can be reprocessed",
            ));
        };
        let job = reprocess.start(id, req.force).borrow().job;
        Ok(Response::new(ReprocessJobResponse {
            job_id: job.to_string(),
        }))
    }

    async fn watch_reprocess(
        &self,
        request: Request<WatchReprocessRequest>,
    ) -> Result<Response<Self::WatchReprocessStream>, Status> {
        let job = job_id(&request.get_ref().job_id)?;
        let rx = self
            .state
            .parts()
            .reprocess
            .as_ref()
            .and_then(|r| r.watch(job))
            .ok_or_else(|| Status::not_found(format!("reprocess job {job} is not known here")))?;
        let (tx, out) = mpsc::channel(16);
        tokio::spawn(stream_job(rx, tx, self.state.parts().shutdown.clone()));
        Ok(Response::new(Box::pin(ReceiverStream::new(out))))
    }

    async fn purge_source(
        &self,
        request: Request<PurgeSourceRequest>,
    ) -> Result<Response<PurgeJobResponse>, Status> {
        let mut v = Violations::default();
        let id = validate::source_id(&mut v, "source_id", &request.get_ref().source_id);
        v.check()?;
        let id = id.ok_or_else(|| Status::invalid_argument("source_id: required"))?;
        let job = crate::purge::start_source(&self.state, id).await?;
        Ok(Response::new(PurgeJobResponse {
            job_id: job.to_string(),
        }))
    }

    async fn purge_orphans(
        &self,
        _request: Request<PurgeOrphansRequest>,
    ) -> Result<Response<PurgeJobResponse>, Status> {
        // As the other graph calls: UNAVAILABLE until the shared store is connected.
        self.state.backend()?;
        let job = crate::purge::start_orphans(&self.state);
        Ok(Response::new(PurgeJobResponse {
            job_id: job.to_string(),
        }))
    }

    async fn watch_purge(
        &self,
        request: Request<WatchPurgeRequest>,
    ) -> Result<Response<Self::WatchPurgeStream>, Status> {
        let job = job_id(&request.get_ref().job_id)?;
        let rx = self
            .state
            .purges()
            .watch(job)
            .ok_or_else(|| Status::not_found(format!("purge job {job} is not known here")))?;
        let (tx, out) = mpsc::channel(16);
        tokio::spawn(stream_purge(rx, tx, self.state.parts().shutdown.clone()));
        Ok(Response::new(Box::pin(ReceiverStream::new(out))))
    }
}

/// A job id from a request.
fn job_id(raw: &str) -> Result<Uuid, Status> {
    let raw = raw.trim();
    let mut v = Violations::default();
    let job = match Uuid::parse_str(raw) {
        Ok(j) => Some(j),
        Err(_) if raw.is_empty() => {
            v.add("job_id", "required");
            None
        }
        Err(_) => {
            v.add("job_id", "not a job id (a UUID)");
            None
        }
    };
    v.check()?;
    job.ok_or_else(|| Status::invalid_argument("job_id: required"))
}

/// [`stream_job`] for a purge job.
async fn stream_purge(
    mut rx: watch::Receiver<PurgeView>,
    tx: mpsc::Sender<Result<PurgeProgress, Status>>,
    mut shutdown: watch::Receiver<bool>,
) {
    loop {
        let view = rx.borrow_and_update().clone();
        let last = matches!(view.phase, PurgePhase::Done | PurgePhase::Failed);
        let sent = tokio::select! {
            sent = tx.send(Ok(purge_progress(&view))) => sent.is_ok(),
            _ = shutdown.wait_for(|s| *s) => false,
        };
        if !sent || last {
            return;
        }
        tokio::select! {
            changed = rx.changed() => if changed.is_err() { return },
            () = tx.closed() => return,
            _ = shutdown.wait_for(|s| *s) => return,
        }
    }
}

fn purge_progress(v: &PurgeView) -> PurgeProgress {
    use purge_progress::State;
    let state = match v.phase {
        PurgePhase::Waiting => State::Waiting,
        PurgePhase::Running => State::Running,
        PurgePhase::Done => State::Done,
        PurgePhase::Failed => State::Failed,
    };
    PurgeProgress {
        job_id: v.job.to_string(),
        source_id: v.source.map(|s| s.to_string()).unwrap_or_default(),
        state: state as i32,
        batches: v.batches,
        examined: v.done.examined,
        locations: v.done.locations,
        records: v.done.records,
        other_documents: v.done.other_documents,
        companies: v.done.companies,
        llm_jobs: v.llm_jobs,
        error: v.error.clone().unwrap_or_default(),
        at: unix_now(),
    }
}

fn unix_now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| i64::try_from(d.as_secs()).unwrap_or(i64::MAX))
}

/// Sends the job's state now and after every change, until its last state, the client going
/// away, or shutdown.
async fn stream_job(
    mut rx: watch::Receiver<ReprocessView>,
    tx: mpsc::Sender<Result<ReprocessProgress, Status>>,
    mut shutdown: watch::Receiver<bool>,
) {
    loop {
        let view = rx.borrow_and_update().clone();
        let last = view.phase.is_final();
        let sent = tokio::select! {
            sent = tx.send(Ok(progress(&view))) => sent.is_ok(),
            _ = shutdown.wait_for(|s| *s) => false,
        };
        if !sent || last {
            return;
        }
        tokio::select! {
            changed = rx.changed() => if changed.is_err() { return },
            () = tx.closed() => return,
            _ = shutdown.wait_for(|s| *s) => return,
        }
    }
}

fn progress(v: &ReprocessView) -> ReprocessProgress {
    use reprocess_progress::State;
    let state = match v.phase {
        ReprocessPhase::Selecting => State::Selecting,
        ReprocessPhase::Running => State::Running,
        ReprocessPhase::Done => State::Done,
        ReprocessPhase::Failed => State::Failed,
    };
    ReprocessProgress {
        job_id: v.job.to_string(),
        source_id: v.source.to_string(),
        state: state as i32,
        force: v.force,
        examined: v.examined,
        selected: v.selected,
        processed: v.processed,
        failed: v.failed,
        pending: v.pending,
        error: v.error.clone().unwrap_or_default(),
        at: unix_now(),
    }
}
