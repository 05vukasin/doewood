//! `JobsService`: the progress stream, and reprocessing (not built yet).

use std::pin::Pin;
use std::sync::Arc;

use tokio::sync::mpsc;
use tokio_stream::Stream;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

use crate::Api;
use crate::pb::jobs_service_server::JobsService;
use crate::pb::{ProgressUpdate, ReprocessJobRequest, ReprocessJobResponse, WatchProgressRequest};
use crate::progress::Watch;
use crate::validate::{self, Violations};

/// Most sources one watch names.
const MAX_WATCHED: usize = 1000;

type ProgressStream = Pin<Box<dyn Stream<Item = Result<ProgressUpdate, Status>> + Send>>;

#[tonic::async_trait]
impl JobsService for Api {
    type WatchProgressStream = ProgressStream;

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
        let parts = self.state.parts();
        let discovery = parts.discovery.clone();
        let watch = Watch {
            store: Arc::clone(&parts.store),
            progress: Arc::clone(&parts.progress),
            running: Arc::new(move |id| discovery.as_ref().is_some_and(|d| d.is_running(id))),
            only: ids,
            interval: parts.options.progress_interval,
            shutdown: parts.shutdown.clone(),
        };
        let (tx, rx) = mpsc::channel(64);
        tokio::spawn(watch.run(tx));
        Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
    }

    async fn reprocess_job(
        &self,
        request: Request<ReprocessJobRequest>,
    ) -> Result<Response<ReprocessJobResponse>, Status> {
        let mut v = Violations::default();
        let id = validate::source_id(&mut v, "source_id", &request.get_ref().source_id);
        v.check()?;
        if let Some(id) = id {
            self.state.source(id).await?;
        }
        Err(Status::unimplemented(
            "ReprocessJob is not available yet: it arrives with ingest (step 4), which records \
             the quail/hedgehog step versions it compares",
        ))
    }
}
