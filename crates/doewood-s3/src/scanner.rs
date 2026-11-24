//! The incremental scanner (ADR 0001, section 5): one task per source lists the bucket on the
//! source's schedule and compares every key's ETag and size with `objects_seen`.
//!
//! # A pass
//!
//! `ListObjectsV2` returns keys in byte order, which is also the order of `objects_seen`'s
//! primary key. A pass walks the two side by side, one listing page (≤ 1000 keys) and one chunk
//! of stored rows (≤ 1000) at a time:
//!
//! - a key only listed is `New`, a key in both whose ETag or size differs is `Changed`, and a
//!   stored key the listing skipped over is `Deleted`;
//! - each step sends its events to the bounded channel (waiting while it is full), then commits
//!   the row changes **and** the checkpoint (the last key compared) in one transaction.
//!
//! Memory stays bounded by one page plus one chunk, however many keys the bucket holds. Events
//! are delivered at least once: a crash between sending and committing sends them again.
//!
//! # Resuming
//!
//! A restart (or a failed pass) resumes from the checkpoint with `StartAfter`, so a pass over
//! millions of keys does not start over. Continuation tokens are used between pages; they are
//! not persisted, because servers expire them.
//!
//! # Schedule
//!
//! The first pass is a full one (every key is new); each later pass emits only the differences.
//! The next pass starts `scan_interval` after the previous one completed, or right away when
//! one is unfinished. A pass that fails puts the source in the `error` state with the reason and
//! is retried after the interval; one that succeeds makes it `active` again. A paused or removed
//! source stops its scanner.

use std::future::Future;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use doewood_core::SourceId;
use doewood_store::{ObjectChange, ScanCheckpoint, Source, SourceState, Store};
use tokio::sync::{mpsc, watch};
use tracing::{info, warn};

use crate::client::{ListPage, ListRequest};
use crate::event::{Diff, EventOrigin, ObjectEvent, ObjectVersion, diff};
use crate::{S3Client, S3Error};

/// Lists a bucket: [`S3Client`], or a fake in tests.
pub trait Lister: Send + Sync {
    /// One page of `ListObjectsV2`.
    fn list(&self, req: ListRequest<'_>) -> impl Future<Output = Result<ListPage, S3Error>> + Send;
}

impl Lister for S3Client {
    async fn list(&self, req: ListRequest<'_>) -> Result<ListPage, S3Error> {
        self.list_objects(&req).await
    }
}

/// Builds a source's lister from its current row, at the start of every pass, so that changed
/// credentials or options apply without a restart. It runs on a blocking thread.
pub type Connect<L> = Arc<dyn Fn(&Store, &Source) -> Result<L, S3Error> + Send + Sync>;

/// Tuning of the scanner.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ScanOptions {
    /// Keys per listing page (S3 caps it at 1000).
    pub page_size: usize,
    /// Stored rows compared per step.
    pub chunk_size: usize,
    /// Wait before retrying a failed pass; `None` waits the source's scan interval.
    pub error_retry: Option<Duration>,
}

impl Default for ScanOptions {
    fn default() -> Self {
        Self {
            page_size: 1000,
            chunk_size: 1000,
            error_retry: None,
        }
    }
}

/// What a pass found.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PassStats {
    /// The pass number (1 for the first, full pass).
    pub pass: u64,
    /// Whether it resumed an unfinished pass.
    pub resumed: bool,
    /// Keys listed (in this run of the pass).
    pub listed: u64,
    /// `New` events.
    pub new: u64,
    /// `Changed` events.
    pub changed: u64,
    /// `Deleted` events.
    pub deleted: u64,
}

/// The scanner of one source.
pub struct Scanner {
    store: Arc<Store>,
    source: SourceId,
    prefix: String,
    events: mpsc::Sender<ObjectEvent>,
    options: ScanOptions,
}

impl Scanner {
    /// A scanner for `source`, sending to `events`.
    pub fn new(
        store: Arc<Store>,
        source: &Source,
        events: mpsc::Sender<ObjectEvent>,
        options: ScanOptions,
    ) -> Self {
        Self {
            store,
            source: source.id,
            prefix: source.prefix.clone(),
            events,
            options: ScanOptions {
                page_size: options.page_size.clamp(1, 1000),
                chunk_size: options.chunk_size.max(1),
                ..options
            },
        }
    }

    /// Runs passes on the source's schedule until shutdown, until the source is paused or
    /// removed, or until the event channel closes.
    pub async fn run<L: Lister + 'static>(
        self,
        connect: Connect<L>,
        mut stop: watch::Receiver<bool>,
    ) {
        let id = self.source;
        let mut retry_at: Option<i64> = None;
        loop {
            if *stop.borrow() {
                return;
            }
            let source = match self.db(move |s| s.source(id)).await {
                Ok(Some(s)) if s.state.is_scanned() => s,
                Ok(Some(s)) => {
                    info!(source = %id, state = %s.state, "source not active; scanner stops");
                    return;
                }
                Ok(None) => {
                    info!(source = %id, "source removed; scanner stops");
                    return;
                }
                Err(e) => {
                    warn!(source = %id, error = %e, "cannot read the source; retrying");
                    if sleep_or_stop(Duration::from_secs(30), &mut stop).await {
                        return;
                    }
                    continue;
                }
            };
            let interval = i64::try_from(source.scan_interval.as_secs()).unwrap_or(i64::MAX);
            let due = match retry_at {
                Some(t) => t,
                None => match self.db(move |s| s.scan_checkpoint(id)).await {
                    Ok(Some(cp)) if !cp.in_progress => cp
                        .last_completed_at
                        .map_or(0, |t| t.saturating_add(interval)),
                    _ => 0,
                },
            };
            let wait = due.saturating_sub(unix_now());
            if wait > 0 {
                // Sleep, then look at the source again: it may have been paused meanwhile.
                let wait = Duration::from_secs(u64::try_from(wait).unwrap_or(u64::MAX));
                if sleep_or_stop(wait, &mut stop).await {
                    return;
                }
                retry_at = None;
                continue;
            }
            let result = {
                let src = source.clone();
                let connect = Arc::clone(&connect);
                match self.db(move |s| connect(s, &src)).await {
                    Ok(lister) => self.pass(&lister, &mut stop).await,
                    Err(e) => Err(e),
                }
            };
            match result {
                Ok(stats) => {
                    info!(
                        source = %id,
                        pass = stats.pass,
                        resumed = stats.resumed,
                        listed = stats.listed,
                        new = stats.new,
                        changed = stats.changed,
                        deleted = stats.deleted,
                        "scan pass complete"
                    );
                    retry_at = None;
                    if source.state == SourceState::Error {
                        self.set_state(SourceState::Active, None).await;
                    }
                }
                Err(S3Error::Stopped) => return,
                Err(S3Error::ChannelClosed) => {
                    warn!(source = %id, "the event consumer is gone; scanner stops");
                    return;
                }
                Err(e) => {
                    let reason = e.to_string();
                    warn!(source = %id, error = %reason, "scan pass failed");
                    self.set_state(SourceState::Error, Some(reason)).await;
                    let delay = self
                        .options
                        .error_retry
                        .map_or(interval, |d| i64::try_from(d.as_secs()).unwrap_or(i64::MAX));
                    retry_at = Some(unix_now().saturating_add(delay.max(1)));
                }
            }
        }
    }

    /// Runs one pass, or resumes the unfinished one. Returns [`S3Error::Stopped`] on shutdown,
    /// with the checkpoint kept for the next start.
    pub async fn pass<L: Lister>(
        &self,
        lister: &L,
        stop: &mut watch::Receiver<bool>,
    ) -> Result<PassStats, S3Error> {
        let id = self.source;
        let mut cp = self
            .db(move |s| s.scan_checkpoint(id))
            .await?
            .unwrap_or_default();
        let mut stats = PassStats {
            resumed: cp.in_progress,
            ..PassStats::default()
        };
        if !cp.in_progress {
            cp = ScanCheckpoint {
                pass: cp.pass + 1,
                in_progress: true,
                start_after: None,
                pass_started_at: Some(unix_now()),
                last_completed_at: cp.last_completed_at,
            };
            let saved = cp.clone();
            self.db(move |s| s.save_scan_checkpoint(id, &saved)).await?;
        }
        stats.pass = cp.pass;

        let mut token: Option<String> = None;
        loop {
            let start_after = cp.start_after.clone();
            let req = ListRequest {
                prefix: &self.prefix,
                start_after: if token.is_none() {
                    start_after.as_deref()
                } else {
                    None
                },
                continuation_token: token.as_deref(),
                max_keys: self.options.page_size,
            };
            let page = or_stop(lister.list(req), stop).await??;
            check_order(start_after.as_deref(), &page)?;
            stats.listed += page.objects.len() as u64;
            let upto = match (&page.next_continuation_token, page.objects.last()) {
                (None, _) => None,
                (Some(_), Some(last)) => Some(last.key.clone()),
                // A truncated page without keys: nothing to compare yet.
                (Some(next), None) => {
                    token = Some(next.clone());
                    continue;
                }
            };
            self.merge(&mut cp, upto.as_deref(), &page, &mut stats, stop)
                .await?;
            match page.next_continuation_token {
                Some(next) => token = Some(next),
                None => break,
            }
        }

        cp.in_progress = false;
        cp.start_after = None;
        cp.last_completed_at = Some(unix_now());
        self.db(move |s| s.save_scan_checkpoint(id, &cp)).await?;
        Ok(stats)
    }

    /// Compares the page's keys with the stored rows in `(cp.start_after, upto]` (`upto: None`
    /// is open-ended: the last page also deletes every stored key after it), a chunk at a time.
    async fn merge(
        &self,
        cp: &mut ScanCheckpoint,
        upto: Option<&str>,
        page: &ListPage,
        stats: &mut PassStats,
        stop: &mut watch::Receiver<bool>,
    ) -> Result<(), S3Error> {
        let id = self.source;
        let chunk_size = self.options.chunk_size;
        let mut next = 0;
        loop {
            let (after, until) = (cp.start_after.clone(), upto.map(str::to_owned));
            let chunk = self
                .db(move |s| s.seen_objects(id, after.as_deref(), until.as_deref(), chunk_size))
                .await?;
            let full = chunk.len() == chunk_size;
            // This step covers keys up to `bound`: the chunk's last key while more rows may
            // follow, else the whole range.
            let bound = if full {
                chunk.last().map(|o| o.key.clone())
            } else {
                upto.map(str::to_owned)
            };
            let rest = &page.objects[next..];
            let end = next
                + bound.as_deref().map_or(rest.len(), |b| {
                    rest.partition_point(|o| o.key.as_str() <= b)
                });
            let listed = page.objects[next..end].iter().map(|o| {
                (
                    o.key.as_str(),
                    ObjectVersion {
                        etag: o.etag.clone(),
                        size: o.size,
                        last_modified: o.last_modified,
                    },
                )
            });
            let diffs = diff(listed, &chunk);
            let covered = bound
                .or_else(|| page.objects[..end].last().map(|o| o.key.clone()))
                .or_else(|| cp.start_after.clone());
            next = end;

            let changes: Vec<ObjectChange> = diffs.iter().map(Diff::change).collect();
            for d in diffs {
                match d.kind {
                    crate::EventKind::New(_) => stats.new += 1,
                    crate::EventKind::Changed(_) => stats.changed += 1,
                    crate::EventKind::Deleted => stats.deleted += 1,
                }
                let event = ObjectEvent {
                    source: id,
                    key: d.key,
                    kind: d.kind,
                    origin: EventOrigin::Scan,
                };
                or_stop(self.events.send(event), stop)
                    .await?
                    .map_err(|_| S3Error::ChannelClosed)?;
            }
            cp.start_after = covered;
            let saved = cp.clone();
            self.db(move |s| s.record_objects(id, &changes, Some(&saved)))
                .await?;
            if !full {
                return Ok(());
            }
        }
    }

    async fn set_state(&self, state: SourceState, reason: Option<String>) {
        let id = self.source;
        if let Err(e) = self
            .db(move |s| s.set_source_state(id, state, reason.as_deref()))
            .await
        {
            warn!(source = %id, error = %e, "cannot record the source state");
        }
    }

    /// Runs a store call on a blocking thread.
    async fn db<T: Send + 'static, E: Into<S3Error>>(
        &self,
        f: impl FnOnce(&Store) -> Result<T, E> + Send + 'static,
    ) -> Result<T, S3Error> {
        let store = Arc::clone(&self.store);
        tokio::task::spawn_blocking(move || f(&store).map_err(Into::into))
            .await
            .map_err(|e| S3Error::Transport {
                op: "store",
                reason: e.to_string(),
            })?
    }
}

/// Refuses a page whose keys are not strictly after `after` and ascending.
fn check_order(after: Option<&str>, page: &ListPage) -> Result<(), S3Error> {
    let mut prev = after;
    for o in &page.objects {
        if prev.is_some_and(|p| o.key.as_str() <= p) {
            return Err(S3Error::Unordered(o.key.clone()));
        }
        prev = Some(&o.key);
    }
    Ok(())
}

/// Awaits `fut` unless shutdown is requested first.
async fn or_stop<T>(
    fut: impl Future<Output = T>,
    stop: &mut watch::Receiver<bool>,
) -> Result<T, S3Error> {
    if *stop.borrow() {
        return Err(S3Error::Stopped);
    }
    tokio::select! {
        v = fut => Ok(v),
        _ = stop.wait_for(|s| *s) => Err(S3Error::Stopped),
    }
}

/// Sleeps; returns whether shutdown was requested.
async fn sleep_or_stop(d: Duration, stop: &mut watch::Receiver<bool>) -> bool {
    or_stop(tokio::time::sleep(d), stop).await.is_err()
}

fn unix_now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| i64::try_from(d.as_secs()).unwrap_or(i64::MAX))
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use doewood_core::MasterKey;
    use doewood_store::NewSource;

    use super::*;
    use crate::{EventKind, ListedObject};

    /// A bucket in memory that serves pages like S3 and can fail after a number of pages.
    #[derive(Default)]
    struct FakeBucket {
        objects: Mutex<Vec<ListedObject>>,
        pages_served: AtomicUsize,
        fail_after: Mutex<Option<usize>>,
        requests: Mutex<Vec<(Option<String>, Option<String>)>>,
    }

    impl FakeBucket {
        fn put(&self, key: &str, etag: &str, size: u64) {
            let mut objects = self.objects.lock().unwrap();
            objects.retain(|o| o.key != key);
            objects.push(ListedObject {
                key: key.into(),
                etag: Some(etag.into()),
                size,
                last_modified: Some(1),
            });
            objects.sort_by(|a, b| a.key.cmp(&b.key));
        }

        fn remove(&self, key: &str) {
            self.objects.lock().unwrap().retain(|o| o.key != key);
        }
    }

    impl Lister for FakeBucket {
        fn list(
            &self,
            req: ListRequest<'_>,
        ) -> impl Future<Output = Result<ListPage, S3Error>> + Send {
            self.requests.lock().unwrap().push((
                req.start_after.map(str::to_owned),
                req.continuation_token.map(str::to_owned),
            ));
            let served = self.pages_served.fetch_add(1, Ordering::SeqCst);
            let result = if self.fail_after.lock().unwrap().is_some_and(|n| served >= n) {
                Err(S3Error::Status {
                    op: "ListObjectsV2",
                    status: 503,
                    code: Some("SlowDown".into()),
                    message: None,
                })
            } else {
                // The token is the last key of the previous page.
                let after = req.continuation_token.or(req.start_after);
                let objects = self.objects.lock().unwrap();
                let page: Vec<_> = objects
                    .iter()
                    .filter(|o| o.key.starts_with(req.prefix))
                    .filter(|o| after.is_none_or(|a| o.key.as_str() > a))
                    .take(req.max_keys)
                    .cloned()
                    .collect();
                let more = objects
                    .iter()
                    .filter(|o| o.key.starts_with(req.prefix))
                    .filter(|o| page.last().is_some_and(|l| o.key > l.key))
                    .count()
                    > 0;
                let next_continuation_token = more.then(|| page.last().unwrap().key.clone());
                Ok(ListPage {
                    objects: page,
                    next_continuation_token,
                })
            };
            async move { result }
        }
    }

    struct Setup {
        store: Arc<Store>,
        source: Source,
        bucket: FakeBucket,
    }

    fn setup(prefix: &str) -> Setup {
        let store = Arc::new(Store::open_in_memory(&MasterKey::from_bytes([5; 32])).unwrap());
        let mut new = NewSource::new("http://fake", "bucket");
        new.prefix = prefix.into();
        let id = store.add_source(&new, None).unwrap();
        let source = store.source(id).unwrap().unwrap();
        Setup {
            store,
            source,
            bucket: FakeBucket::default(),
        }
    }

    fn small() -> ScanOptions {
        ScanOptions {
            page_size: 3,
            chunk_size: 2,
            error_retry: None,
        }
    }

    async fn pass(
        s: &Setup,
        options: ScanOptions,
    ) -> (Result<PassStats, S3Error>, Vec<(String, &'static str)>) {
        let (tx, mut rx) = mpsc::channel(10_000);
        let scanner = Scanner::new(Arc::clone(&s.store), &s.source, tx, options);
        let (_stop_tx, mut stop) = watch::channel(false);
        let result = scanner.pass(&s.bucket, &mut stop).await;
        drop(scanner);
        let mut events = Vec::new();
        while let Some(e) = rx.recv().await {
            assert_eq!(e.origin, EventOrigin::Scan);
            events.push((e.key, e.kind.name()));
        }
        (result, events)
    }

    #[tokio::test]
    async fn first_pass_is_full_then_only_differences() {
        let s = setup("");
        for i in 0..10 {
            s.bucket.put(&format!("k{i:02}"), "e1", 10);
        }
        let (r, events) = pass(&s, small()).await;
        let stats = r.unwrap();
        assert_eq!((stats.pass, stats.listed, stats.new), (1, 10, 10));
        assert_eq!(events.len(), 10);
        assert!(events.iter().all(|(_, k)| *k == "new"));
        assert_eq!(s.store.live_object_count(s.source.id).unwrap(), 10);

        // Nothing changed: nothing is emitted.
        let (r, events) = pass(&s, small()).await;
        assert_eq!(r.unwrap().pass, 2);
        assert!(events.is_empty(), "{events:?}");

        s.bucket.put("k03", "e2", 10); // changed etag
        s.bucket.put("k05", "e1", 11); // changed size
        s.bucket.remove("k00"); // first key
        s.bucket.remove("k06");
        s.bucket.remove("k07"); // a run of deletions
        s.bucket.remove("k09"); // last key
        s.bucket.put("k04a", "e1", 1); // new in the middle
        s.bucket.put("zz", "e1", 1); // new at the end
        let (r, mut events) = pass(&s, small()).await;
        let stats = r.unwrap();
        assert_eq!((stats.new, stats.changed, stats.deleted), (2, 2, 4));
        events.sort();
        assert_eq!(
            events,
            [
                ("k00".to_owned(), "deleted"),
                ("k03".to_owned(), "changed"),
                ("k04a".to_owned(), "new"),
                ("k05".to_owned(), "changed"),
                ("k06".to_owned(), "deleted"),
                ("k07".to_owned(), "deleted"),
                ("k09".to_owned(), "deleted"),
                ("zz".to_owned(), "new"),
            ]
        );
        assert_eq!(s.store.live_object_count(s.source.id).unwrap(), 8);
        // A key that comes back is new again.
        s.bucket.put("k00", "e1", 10);
        let (_, events) = pass(&s, small()).await;
        assert_eq!(events, [("k00".to_owned(), "new")]);
    }

    #[tokio::test]
    async fn an_emptied_bucket_deletes_everything_in_chunks() {
        let s = setup("");
        for i in 0..7 {
            s.bucket.put(&format!("k{i}"), "e", 1);
        }
        pass(&s, small()).await.0.unwrap();
        for i in 0..7 {
            s.bucket.remove(&format!("k{i}"));
        }
        let (r, events) = pass(&s, small()).await;
        assert_eq!(r.unwrap().deleted, 7);
        assert_eq!(events.len(), 7);
        assert_eq!(s.store.live_object_count(s.source.id).unwrap(), 0);
    }

    #[tokio::test]
    async fn a_failed_pass_resumes_from_its_checkpoint() {
        let s = setup("");
        for i in 0..10 {
            s.bucket.put(&format!("k{i:02}"), "e", 1);
        }
        // Pages of 3: the third request fails, after 6 keys are committed.
        *s.bucket.fail_after.lock().unwrap() = Some(2);
        let (r, events) = pass(&s, small()).await;
        assert!(matches!(r, Err(S3Error::Status { status: 503, .. })));
        assert_eq!(events.len(), 6);
        let cp = s.store.scan_checkpoint(s.source.id).unwrap().unwrap();
        assert!(cp.in_progress);
        assert_eq!(cp.start_after.as_deref(), Some("k05"));

        // The retry resumes after k05 with StartAfter, in the same pass.
        *s.bucket.fail_after.lock().unwrap() = None;
        s.bucket.requests.lock().unwrap().clear();
        let (r, events) = pass(&s, small()).await;
        let stats = r.unwrap();
        assert!(stats.resumed);
        assert_eq!(stats.pass, 1);
        assert_eq!(stats.listed, 4);
        assert_eq!(
            events.iter().map(|(k, _)| k.as_str()).collect::<Vec<_>>(),
            ["k06", "k07", "k08", "k09"]
        );
        let requests = s.bucket.requests.lock().unwrap().clone();
        assert_eq!(requests[0], (Some("k05".to_owned()), None));
        assert_eq!(
            requests[1].1.as_deref(),
            Some("k08"),
            "then continuation tokens"
        );
        let cp = s.store.scan_checkpoint(s.source.id).unwrap().unwrap();
        assert!(!cp.in_progress && cp.start_after.is_none() && cp.last_completed_at.is_some());
    }

    #[tokio::test]
    async fn a_restart_resumes_without_reemitting_committed_keys() {
        // A new Scanner (as after a restart) on the same store picks the checkpoint up.
        let s = setup("p/");
        for i in 0..5 {
            s.bucket.put(&format!("p/{i}"), "e", 1);
        }
        s.bucket.put("other/x", "e", 1); // outside the prefix
        *s.bucket.fail_after.lock().unwrap() = Some(1);
        let (r, events) = pass(&s, small()).await;
        assert!(r.is_err());
        assert_eq!(events.len(), 3);
        *s.bucket.fail_after.lock().unwrap() = None;
        let (r, events) = pass(&s, small()).await;
        assert!(r.unwrap().resumed);
        assert_eq!(
            events.iter().map(|(k, _)| k.as_str()).collect::<Vec<_>>(),
            ["p/3", "p/4"]
        );
        assert_eq!(s.store.live_object_count(s.source.id).unwrap(), 5);
    }

    #[tokio::test]
    async fn unordered_listings_are_refused() {
        struct Backwards;
        impl Lister for Backwards {
            fn list(
                &self,
                _: ListRequest<'_>,
            ) -> impl Future<Output = Result<ListPage, S3Error>> + Send {
                let o = |k: &str| ListedObject {
                    key: k.into(),
                    etag: None,
                    size: 0,
                    last_modified: None,
                };
                async move {
                    Ok(ListPage {
                        objects: vec![o("b"), o("a")],
                        next_continuation_token: None,
                    })
                }
            }
        }
        let s = setup("");
        let (tx, _rx) = mpsc::channel(10);
        let scanner = Scanner::new(Arc::clone(&s.store), &s.source, tx, small());
        let (_t, mut stop) = watch::channel(false);
        assert!(matches!(
            scanner.pass(&Backwards, &mut stop).await,
            Err(S3Error::Unordered(k)) if k == "a"
        ));
    }

    #[tokio::test]
    async fn a_full_channel_waits_and_shutdown_interrupts_it() {
        let s = setup("");
        for i in 0..5 {
            s.bucket.put(&format!("k{i}"), "e", 1);
        }
        let (tx, mut rx) = mpsc::channel(1);
        let scanner = Scanner::new(Arc::clone(&s.store), &s.source, tx, small());
        let (stop_tx, mut stop) = watch::channel(false);
        let pass = scanner.pass(&s.bucket, &mut stop);
        tokio::pin!(pass);
        // The channel holds one event; the pass blocks on the second.
        tokio::select! {
            _ = &mut pass => panic!("the pass should wait for the consumer"),
            () = tokio::time::sleep(Duration::from_millis(100)) => {}
        }
        assert_eq!(rx.recv().await.unwrap().key, "k0");
        stop_tx.send(true).unwrap();
        assert!(matches!(pass.await, Err(S3Error::Stopped)));
        // Nothing past the first step was committed, and the pass is resumable.
        let cp = s.store.scan_checkpoint(s.source.id).unwrap().unwrap();
        assert!(cp.in_progress);
    }

    #[tokio::test]
    async fn run_follows_the_schedule_and_the_source_state() {
        let s = setup("");
        s.bucket.put("a", "e", 1);
        let bucket = Arc::new(s.bucket);
        let (tx, mut rx) = mpsc::channel(100);
        let scanner = Scanner::new(
            Arc::clone(&s.store),
            &s.source,
            tx,
            ScanOptions {
                error_retry: Some(Duration::from_secs(1)),
                ..small()
            },
        );
        let fail = Arc::new(Mutex::new(true));
        let connect: Connect<Arc<FakeBucket>> = {
            let bucket = Arc::clone(&bucket);
            let fail = Arc::clone(&fail);
            Arc::new(move |_, _| {
                if *fail.lock().unwrap() {
                    Err(S3Error::Config("no route".into()))
                } else {
                    Ok(Arc::clone(&bucket))
                }
            })
        };
        let (stop_tx, stop) = watch::channel(false);
        let task = tokio::spawn(scanner.run(connect, stop));
        // The first attempt fails: the source records the error.
        let id = s.source.id;
        wait_until(|| s.store.source(id).unwrap().unwrap().state == SourceState::Error).await;
        let src = s.store.source(id).unwrap().unwrap();
        assert!(src.state_reason.unwrap().contains("no route"));
        // It recovers on the retry, emits, and becomes active again.
        *fail.lock().unwrap() = false;
        assert_eq!(rx.recv().await.unwrap().kind.name(), "new");
        wait_until(|| s.store.source(id).unwrap().unwrap().state == SourceState::Active).await;
        // The next pass waits for the interval (300 s): shutdown ends the task promptly.
        stop_tx.send(true).unwrap();
        tokio::time::timeout(Duration::from_secs(5), task)
            .await
            .unwrap()
            .unwrap();
        assert!(rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn a_paused_source_is_not_scanned() {
        let s = setup("");
        s.bucket.put("a", "e", 1);
        s.store
            .set_source_state(s.source.id, SourceState::Paused, None)
            .unwrap();
        let bucket = Arc::new(s.bucket);
        let (tx, mut rx) = mpsc::channel(10);
        let scanner = Scanner::new(Arc::clone(&s.store), &s.source, tx, small());
        let b = Arc::clone(&bucket);
        let (_stop_tx, stop) = watch::channel(false);
        tokio::time::timeout(
            Duration::from_secs(5),
            scanner.run(Arc::new(move |_, _| Ok(Arc::clone(&b))), stop),
        )
        .await
        .unwrap();
        assert!(rx.recv().await.is_none());
        assert_eq!(bucket.pages_served.load(Ordering::SeqCst), 0);
    }

    impl Lister for Arc<FakeBucket> {
        fn list(
            &self,
            req: ListRequest<'_>,
        ) -> impl Future<Output = Result<ListPage, S3Error>> + Send {
            FakeBucket::list(self, req)
        }
    }

    async fn wait_until(mut f: impl FnMut() -> bool) {
        for _ in 0..200 {
            if f() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        panic!("condition not reached");
    }

    #[test]
    fn events_kinds_are_named() {
        assert_eq!(EventKind::Deleted.name(), "deleted");
    }
}
