//! Push sources (ADR 0004): an application hands in the bytes through
//! `IngestService.PutObject` and gets the analysis back in the same call. There is no scanner:
//! the application owns discovery, moves ([`Ingest::move_pushed`]) and deletions
//! ([`Ingest::remove_pushed`]).
//!
//! - **The same pipeline.** A pushed file is hashed, analyzed (unless its content is stored
//!   with the running step versions already) and written as an S3 object would be: the
//!   record, the Location `["push:<source id>", key]`, the graph, search and, if the source's
//!   policy asks, a queued summary. Its `objects_seen` row gets its final status at once.
//! - **Idempotent per (BLAKE3, key).** When the key already holds the same content and its
//!   record is current, nothing is written ([`PushStatus::Unchanged`]).
//! - **Admission first** ([`Ingest::admit_push`]), before the bytes are read: the source is
//!   checked, the file's share of the memory budget is taken at once (a big file that does not
//!   fit is refused), and a worker slot is awaited for at most [`PUSH_SLOT_WAIT`] behind at most
//!   [`IngestConfig::jobs`](super::IngestConfig::jobs) other pushes. Anything else is
//!   [`PushError::ResourceExhausted`]: a burst is shed, never queued in memory.
//! - **One put per key at a time**; a second one meanwhile is [`PushError::Unavailable`].

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use doewood_core::SourceId;
use doewood_model::{Backend, Blake3, ObjectLocation, Record};
use doewood_store::{IngestOutcome, IngestSettings, Source, SourceState, StoreError};
use tokio::sync::OwnedSemaphorePermit;
use tracing::debug;

use super::{Charge, Ingest, IngestError, Overruns, hash, lock, stored_outcome};
use crate::analyze::Outcome;
use crate::facts::DocFacts;

/// How long a push waits for a worker slot before it is refused.
pub const PUSH_SLOT_WAIT: Duration = Duration::from_secs(10);

/// Why a push, a move or a remove did not happen. The messages name no secret.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PushError {
    /// `NOT_FOUND`: no such source, or no such object.
    NotFound(String),
    /// `INVALID_ARGUMENT`: the request itself is wrong (the bytes do not match the size).
    InvalidArgument(String),
    /// `FAILED_PRECONDITION`: not a push source, paused, owned by another instance, too large
    /// for the source's cap, or a file that cannot be read.
    FailedPrecondition(String),
    /// `UNAVAILABLE`: transient (the shared store, a put of the same key in flight); retry.
    Unavailable(String),
    /// `RESOURCE_EXHAUSTED`: no worker slot or memory budget free now; retry later.
    ResourceExhausted(String),
    /// `INTERNAL`: logged, answered without details.
    Internal(String),
}

impl std::fmt::Display for PushError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotFound(m)
            | Self::InvalidArgument(m)
            | Self::FailedPrecondition(m)
            | Self::Unavailable(m)
            | Self::ResourceExhausted(m)
            | Self::Internal(m) => f.write_str(m),
        }
    }
}

impl From<IngestError> for PushError {
    fn from(e: IngestError) -> Self {
        if e.transient {
            Self::Unavailable(e.message)
        } else {
            Self::Internal(e.message)
        }
    }
}

impl From<doewood_model::Error> for PushError {
    fn from(e: doewood_model::Error) -> Self {
        IngestError::from(e).into()
    }
}

/// What a push asks for.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PushRequest {
    /// A push source of this instance.
    pub source: SourceId,
    /// The application's key (a path, an id): the location's key.
    pub key: String,
    /// The file name; its extension is a hint for spreadsheet containers. Empty: the key's
    /// last `/` segment.
    pub name: String,
    /// The exact size of the bytes that follow.
    pub size: u64,
}

/// What a push did.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PushStatus {
    /// The file was analyzed now and its record written.
    Analyzed,
    /// Its content was stored already with the running step versions: no analysis, only the
    /// location (and the graph and search for it) was written.
    Reused,
    /// The key held this content already, with a current record: nothing was written.
    Unchanged,
}

/// A push's result.
#[derive(Clone, Debug)]
pub struct Pushed {
    /// The record, as `GetRecord` returns it.
    pub record: Record,
    /// The document's kind (`invoice`, `contract`, ...), if quail found one.
    pub kind: Option<String>,
    /// What happened.
    pub status: PushStatus,
    /// No extractor reads the format: the record holds the identity and format only
    /// (`skipped` in the source's progress).
    pub skipped: bool,
}

/// A push admitted: its source, its share of the memory budget and its worker slot, held until
/// the push ends (and by its analysis, if that outlives the call). The key is marked in
/// flight until it is dropped.
pub struct PushTicket {
    ingest: Ingest,
    source: Source,
    settings: IngestSettings,
    backend: Arc<dyn Backend>,
    key: String,
    name: String,
    size: u64,
    charge: Option<Arc<Charge>>,
    slot: Option<OwnedSemaphorePermit>,
}

impl PushTicket {
    /// The size the bytes must have.
    pub fn size(&self) -> u64 {
        self.size
    }
}

impl Drop for PushTicket {
    fn drop(&mut self) {
        let key = std::mem::take(&mut self.key);
        lock(&self.ingest.inner.inflight).remove(&(self.source.id, key));
    }
}

/// The key's last `/` segment.
fn base_name(key: &str) -> &str {
    key.rsplit('/').next().unwrap_or(key)
}

impl Ingest {
    /// The push source `id`, if this instance may write to it now: it exists, is a push
    /// source, is active and is not owned by another instance.
    async fn push_source(&self, id: SourceId) -> Result<(Source, IngestSettings), PushError> {
        let (source, settings) = self
            .db(move |s| s.snapshot(|s| Ok((s.source(id)?, s.ingest_settings()?))))
            .await?;
        let source = source
            .filter(|s| s.state != SourceState::Removed)
            .ok_or_else(|| PushError::NotFound(format!("source {id} does not exist")))?;
        if !source.is_push() {
            return Err(PushError::FailedPrecondition(format!(
                "source {id} is an S3 source: its objects come from its bucket; add a push \
                 source (AddSource with kind PUSH) to hand in bytes"
            )));
        }
        let claims = self.inner.store.claims();
        if !claims.is_checked() {
            return Err(PushError::Unavailable(
                "the source claims are not checked in the shared store yet; retry".to_owned(),
            ));
        }
        if let Some(owner) = claims.owner_elsewhere(id) {
            return Err(PushError::FailedPrecondition(format!(
                "source {id} is claimed by instance \"{owner}\"; push to that instance"
            )));
        }
        match source.state {
            SourceState::Active => Ok((source, settings)),
            SourceState::Paused => Err(PushError::FailedPrecondition(format!(
                "source {id} is paused: resume it (ResumeSource) to push to it"
            ))),
            _ => Err(PushError::FailedPrecondition(format!(
                "source {id} is in the error state: {}",
                source
                    .state_reason
                    .as_deref()
                    .unwrap_or("no reason recorded")
            ))),
        }
    }

    /// The backend, or [`PushError::Unavailable`] while the shared store is not connected.
    fn push_backend(&self) -> Result<Arc<dyn Backend>, PushError> {
        self.inner.backend.borrow().clone().ok_or_else(|| {
            PushError::Unavailable("the shared store is not connected; retry".to_owned())
        })
    }

    /// Admits a push before its bytes are read (see the module documentation): checks the
    /// source and the size, marks the key in flight, takes the file's share of the memory
    /// budget and a worker slot.
    pub async fn admit_push(&self, req: PushRequest) -> Result<PushTicket, PushError> {
        let (source, settings) = self.push_source(req.source).await?;
        if req.size > source.max_object_bytes() {
            return Err(PushError::FailedPrecondition(format!(
                "the file is {} bytes, more than the {} bytes source {} accepts \
                 (UpdateSource max_object_bytes)",
                req.size,
                source.max_object_bytes(),
                source.id
            )));
        }
        let backend = self.push_backend()?;
        if !lock(&self.inner.inflight).insert((source.id, req.key.clone())) {
            return Err(PushError::Unavailable(format!(
                "a put, move or remove of key {:?} is in flight; retry",
                req.key
            )));
        }
        let name = if req.name.is_empty() {
            base_name(&req.key).to_owned()
        } else {
            req.name
        };
        // From here the ticket takes the key out of flight when it is dropped.
        let mut ticket = PushTicket {
            ingest: self.clone(),
            source,
            settings,
            backend,
            key: req.key,
            name,
            size: req.size,
            charge: None,
            slot: None,
        };
        let Ok(charge) = self.inner.budget.try_charge(req.size) else {
            return Err(PushError::ResourceExhausted(format!(
                "the ingest memory budget ({} MiB) has no room for {} bytes now; retry later",
                self.inner.config.memory_budget >> 20,
                req.size
            )));
        };
        ticket.charge = charge;
        ticket.slot = Some(self.push_slot().await?);
        Ok(ticket)
    }

    /// A worker slot for a push: at once when one is free, else after a wait of at most
    /// [`PUSH_SLOT_WAIT`] behind at most `jobs` other pushes.
    async fn push_slot(&self) -> Result<OwnedSemaphorePermit, PushError> {
        let slots = Arc::clone(&self.inner.slots);
        if let Ok(slot) = Arc::clone(&slots).try_acquire_owned() {
            return Ok(slot);
        }
        let jobs = self.inner.config.jobs.max(1);
        let busy = || {
            PushError::ResourceExhausted(format!(
                "all {jobs} ingest worker slots are busy; retry later"
            ))
        };
        let waiting = &self.inner.push_waiting;
        if waiting.fetch_add(1, Ordering::SeqCst) >= jobs {
            waiting.fetch_sub(1, Ordering::SeqCst);
            return Err(busy());
        }
        let slot = tokio::time::timeout(PUSH_SLOT_WAIT, slots.acquire_owned()).await;
        waiting.fetch_sub(1, Ordering::SeqCst);
        match slot {
            Ok(Ok(slot)) => Ok(slot),
            _ => Err(busy()),
        }
    }

    /// Ingests an admitted push's bytes (see the module documentation).
    pub async fn push(&self, mut ticket: PushTicket, bytes: Vec<u8>) -> Result<Pushed, PushError> {
        if bytes.len() as u64 != ticket.size {
            return Err(PushError::InvalidArgument(format!(
                "{} bytes were sent, the header said {}",
                bytes.len(),
                ticket.size
            )));
        }
        let t = Instant::now();
        let (id, key) = (ticket.source.id, ticket.key.clone());
        let backend = Arc::clone(&ticket.backend);
        lock(&self.inner.stats).bytes += ticket.size;
        let (bytes, blake3) = hash(bytes).await?;
        let mut t = self.stage(t, |s| &mut s.analyze);
        let k = key.clone();
        let prior = self.db(move |s| s.ingest_status(id, &k)).await?;
        let current = backend
            .get_record(&blake3)
            .await?
            .filter(|r| !self.inner.analyzer.is_stale(&r.versions));
        t = self.stage(t, |s| &mut s.record);
        let same = prior
            .as_ref()
            .is_some_and(|p| p.status != "deleted" && p.blake3 == Some(blake3.0));
        let (record, outcome, status) = match current {
            Some(r) if same => {
                let outcome = stored_outcome(&r.body);
                debug!(source = %id, key, blake3 = %blake3, "pushed content unchanged");
                return finish(r, outcome, PushStatus::Unchanged);
            }
            Some(r) => {
                let outcome = stored_outcome(&r.body);
                (r, outcome, PushStatus::Reused)
            }
            None => {
                let mut overruns = Overruns::new();
                let a = self
                    .analyze(
                        bytes,
                        blake3,
                        &ticket.name,
                        ticket.charge.take(),
                        ticket.slot.take(),
                        &mut overruns,
                    )
                    .await
                    // A watchdog timeout may be load: the caller can try again.
                    .map_err(|e| PushError::Unavailable(e.message))?;
                lock(&self.inner.stats).analyzed += 1;
                t = self.stage(t, |s| &mut s.analyze);
                backend.put_record(&a.blake3, &a.body, &a.versions).await?;
                let r = backend.get_record(&a.blake3).await?.ok_or_else(|| {
                    PushError::Unavailable("the record just written is not there; retry".into())
                })?;
                t = self.stage(t, |s| &mut s.record);
                (r, a.outcome, PushStatus::Analyzed)
            }
        };
        let location = ObjectLocation {
            endpoint: ticket.source.endpoint.clone(),
            bucket: ticket.source.bucket.clone(),
            key: key.clone(),
        };
        let done = self
            .write_graph(
                &backend,
                &ticket.source,
                &location,
                &ticket.name,
                blake3,
                record.body.clone(),
                outcome.clone(),
                ticket.settings,
                t,
            )
            .await?;
        let size = ticket.size;
        let row = done.clone();
        self.db(move |s| s.record_pushed(id, &key, size, &blake3.0, &row))
            .await?;
        {
            let mut s = lock(&self.inner.stats);
            match done {
                IngestOutcome::Done { .. } => s.done += 1,
                IngestOutcome::Skipped { .. } => s.skipped += 1,
                IngestOutcome::Failed { .. } => s.failed += 1,
            }
        }
        (self.inner.on_change)(id);
        drop(ticket);
        finish(record, outcome, status)
    }

    /// Moves a pushed object from `from` to `to` (`IngestService.MoveLocation`): the stored
    /// content gets the location `to`, with the graph and search for it, and `from`'s
    /// `located_at` edge is ended (superseded), as for an S3 delete. Nothing is analyzed.
    pub async fn move_pushed(
        &self,
        source: SourceId,
        from: &str,
        to: &str,
    ) -> Result<Pushed, PushError> {
        let (source, settings) = self.push_source(source).await?;
        let backend = self.push_backend()?;
        let _from = self.hold_key(source.id, from)?;
        let _to = self.hold_key(source.id, to)?;
        let id = source.id;
        let f = from.to_owned();
        let row = self
            .db(move |s| s.ingest_status(id, &f))
            .await?
            .filter(|r| r.status != "deleted")
            .ok_or_else(|| PushError::NotFound(format!("source {id} has no object {from:?}")))?;
        let blake3 = Blake3(row.blake3.ok_or_else(|| {
            PushError::NotFound(format!("object {from:?} of source {id} has no content"))
        })?);
        let record = backend.get_record(&blake3).await?.ok_or_else(|| {
            PushError::NotFound(format!(
                "the record of object {from:?} is gone; put it again"
            ))
        })?;
        let outcome = stored_outcome(&record.body);
        let at = |key: &str| ObjectLocation {
            endpoint: source.endpoint.clone(),
            bucket: source.bucket.clone(),
            key: key.to_owned(),
        };
        self.write_graph(
            &backend,
            &source,
            &at(to),
            base_name(to),
            blake3,
            record.body.clone(),
            outcome.clone(),
            settings,
            Instant::now(),
        )
        .await?;
        let today = time::OffsetDateTime::now_utc().date();
        if backend.end_location(&at(from), today).await?.is_some() {
            lock(&self.inner.stats).deleted += 1;
        }
        let (f, t) = (from.to_owned(), to.to_owned());
        self.db(move |s| s.move_pushed(id, &f, &t)).await?;
        (self.inner.on_change)(id);
        debug!(source = %id, from, to, blake3 = %blake3, "pushed object moved");
        finish(record, outcome, PushStatus::Reused)
    }

    /// Removes a pushed object (`IngestService.RemoveLocation`): its `located_at` edge is
    /// ended (superseded), as for an S3 delete, and its row turns `deleted`. The document, its
    /// record and its other locations stay. Returns whether the key held a live object.
    pub async fn remove_pushed(&self, source: SourceId, key: &str) -> Result<bool, PushError> {
        let (source, _) = self.push_source(source).await?;
        let backend = self.push_backend()?;
        let _key = self.hold_key(source.id, key)?;
        let at = ObjectLocation {
            endpoint: source.endpoint.clone(),
            bucket: source.bucket.clone(),
            key: key.to_owned(),
        };
        let today = time::OffsetDateTime::now_utc().date();
        let ended = backend.end_location(&at, today).await?;
        let id = source.id;
        let k = key.to_owned();
        let live = self
            .db(move |s| {
                let live = s
                    .ingest_status(id, &k)?
                    .is_some_and(|r| r.status != "deleted");
                s.record_objects(
                    id,
                    &[doewood_store::ObjectChange::Delete { key: k.clone() }],
                    None,
                )?;
                Ok::<_, StoreError>(live)
            })
            .await?;
        if ended.is_some() {
            lock(&self.inner.stats).deleted += 1;
        }
        (self.inner.on_change)(id);
        debug!(source = %id, key, live, "pushed object removed");
        Ok(live)
    }

    /// Marks a key in flight for a move or a remove, or [`PushError::Unavailable`] when a put,
    /// move or remove of it is.
    fn hold_key(&self, source: SourceId, key: &str) -> Result<KeyHold, PushError> {
        if !lock(&self.inner.inflight).insert((source, key.to_owned())) {
            return Err(PushError::Unavailable(format!(
                "a put, move or remove of key {key:?} is in flight; retry"
            )));
        }
        Ok(KeyHold {
            ingest: self.clone(),
            source,
            key: key.to_owned(),
        })
    }
}

/// A key in flight until dropped.
struct KeyHold {
    ingest: Ingest,
    source: SourceId,
    key: String,
}

impl Drop for KeyHold {
    fn drop(&mut self) {
        let key = std::mem::take(&mut self.key);
        lock(&self.ingest.inner.inflight).remove(&(self.source, key));
    }
}

/// The result for a record, or the file's read error.
fn finish(record: Record, outcome: Outcome, status: PushStatus) -> Result<Pushed, PushError> {
    match outcome {
        Outcome::Failed(error) => Err(PushError::FailedPrecondition(format!(
            "the file cannot be read: {error}"
        ))),
        outcome => Ok(Pushed {
            kind: DocFacts::from_record(&record.body).kind,
            skipped: outcome == Outcome::Unsupported,
            record,
            status,
        }),
    }
}
