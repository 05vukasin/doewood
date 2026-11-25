//! The registry delta feeds (ADR 0002 section 6, issue #49): known companies are refreshed
//! when a registry changes them, not only on their schedule.
//!
//! | feed | source | what it does |
//! |---|---|---|
//! | `gleif` | the golden copy's delta files (LEI-CDF and RR-CDF), checked hourly | each LEI a company owns: its GLEIF answers dropped from the cache, GLEIF due |
//! | `edgar` | the daily index, checked hourly | each CIK a company owns that filed a 10-K, 20-F or 8-K (or an amendment): its submissions dropped from the cache, EDGAR due |
//! | `apr` | the monthly open-data set, downloaded weekly (daily once the set is a month old) and swapped in | each MB a company owns whose record changed: the new record cached, APR due |
//! | `companies_house` | the streaming API (company profiles, officers, PSCs), read continuously | each company number a company owns: its Companies House answers dropped from the cache, Companies House due |
//!
//! A feed never writes company facts: the enrichment job asks the registry again (the
//! records are not complete enough to write from, and the job's gate and writer stay the one
//! place facts come from). Only companies the graph already has are touched.
//!
//! **One instance per cluster** reads each feed: a lease on the feed's row in the store
//! ([`FeedStore::lease_feed`]), renewed while it reads. The feed's cursor (where it stopped)
//! and its next run are kept there too, so another instance carries on after a restart or a
//! crash (an abandoned lease expires). An instance where a feed is switched off never takes
//! its lease.
//!
//! **Failure-soft:** a failed run is a warning (logged, counted in `failures`); the cursor
//! stays where it was and the feed is tried again after the retry interval.

use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use std::time::Duration;

use doewood_core::InstanceId;
use doewood_enrich::{IdKind, Identifier};
use doewood_model::{Backend, FeedState};
use doewood_registry::Warning;
use doewood_registry::WarningKind;
use doewood_registry::companies_house::CompanyNumber;
use doewood_registry::feeds::companies_house::{ChStream, ChStreamConfig, StreamKind};
use doewood_registry::feeds::edgar::{EdgarFeed, EdgarFeedConfig};
use doewood_registry::feeds::gleif::{DataSet, DeltaSpan, GleifFeed, GleifFeedConfig};
use doewood_store::EnrichmentConfig;
use time::{Date, OffsetDateTime};
use tokio::sync::{mpsc, watch};
use tracing::{debug, info, warn};
use uuid::Uuid;

use super::Enrichment;
use super::subject::{ch_alias, cik_alias};

/// The feeds, in the order of `WatchProgress`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Feed {
    /// GLEIF's delta files.
    Gleif,
    /// EDGAR's daily index.
    Edgar,
    /// APR's monthly set.
    Apr,
    /// Companies House's streaming API.
    CompaniesHouse,
}

impl Feed {
    /// Every feed.
    pub const ALL: [Self; 4] = [Self::Gleif, Self::Edgar, Self::Apr, Self::CompaniesHouse];

    /// The feed's name in the store and in `WatchProgress`; also its registry's name.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Gleif => "gleif",
            Self::Edgar => "edgar",
            Self::Apr => "apr",
            Self::CompaniesHouse => "companies_house",
        }
    }

    fn index(self) -> usize {
        match self {
            Self::Gleif => 0,
            Self::Edgar => 1,
            Self::Apr => 2,
            Self::CompaniesHouse => 3,
        }
    }

    /// Whether the settings switch it on: its switch, and its registry asked (Companies
    /// House: also a streaming key).
    pub fn enabled(self, s: &EnrichmentConfig) -> bool {
        match self {
            Self::Gleif => s.feeds.gleif && s.gleif,
            Self::Edgar => s.feeds.edgar && s.edgar,
            Self::Apr => s.feeds.apr && s.apr,
            Self::CompaniesHouse => {
                s.feeds.companies_house
                    && s.companies_house
                    && s.companies_house_stream_key.is_some()
            }
        }
    }
}

/// The feed clients' endpoints and limits. The defaults are the production values (EDGAR's
/// archives root and contact come from the EDGAR client's template and the settings; the
/// streaming key from the settings).
#[derive(Clone, Debug, Default)]
pub struct FeedTemplates {
    /// GLEIF's golden copy.
    pub gleif: GleifFeedConfig,
    /// EDGAR's daily index.
    pub edgar: EdgarFeedConfig,
    /// Companies House's streaming API.
    pub companies_house: ChStreamConfig,
}

/// How the feeds run. The defaults are the production values.
#[derive(Clone, Debug)]
pub struct FeedOptions {
    /// How often each feed's loop looks at the settings and its next run.
    pub tick: Duration,
    /// The lease on a feed while it is read; renewed as it goes.
    pub lease: Duration,
    /// GLEIF: how often the latest publication is checked (GLEIF publishes every 8 hours).
    pub gleif_every: Duration,
    /// EDGAR: how often the daily index is checked.
    pub edgar_every: Duration,
    /// APR: how often the set is downloaded again while the one applied is recent.
    pub apr_every: Duration,
    /// APR: how often once the set applied is older than `apr_stale` (a new one is due).
    pub apr_overdue_every: Duration,
    /// APR: the age after which a new set is expected (APR publishes monthly).
    pub apr_stale: Duration,
    /// After a failed run.
    pub retry: Duration,
    /// Companies House: how long each stream is read before the next (the account may hold
    /// two connections, so the three streams take turns on one).
    pub stream_window: Duration,
    /// Companies House: the wait after a 429 (the API asks for a minute).
    pub stream_backoff: Duration,
    /// EDGAR: the most days caught up in one run.
    pub edgar_max_days: u32,
}

impl Default for FeedOptions {
    fn default() -> Self {
        Self {
            tick: Duration::from_mins(1),
            lease: Duration::from_mins(15),
            gleif_every: Duration::from_hours(1),
            edgar_every: Duration::from_hours(1),
            apr_every: Duration::from_hours(7 * 24),
            apr_overdue_every: Duration::from_hours(24),
            apr_stale: Duration::from_hours(31 * 24),
            retry: Duration::from_mins(30),
            stream_window: Duration::from_mins(5),
            stream_backoff: Duration::from_mins(1),
            edgar_max_days: 14,
        }
    }
}

/// One feed's counters on this instance, for `WatchProgress`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct FeedStats {
    /// `gleif`, `edgar`, `apr` or `companies_house`.
    pub feed: &'static str,
    /// Switched on here (and its registry asked).
    pub enabled: bool,
    /// This instance holds the feed's lease.
    pub leader: bool,
    /// Runs completed.
    pub runs: u64,
    /// Records read.
    pub records: u64,
    /// Companies made due.
    pub companies_due: u64,
    /// Runs that failed.
    pub failures: u64,
    /// When a run last completed, Unix seconds; 0 before the first.
    pub last_run_at: i64,
    /// Where the feed stands: a time (Unix seconds), or the Companies House timepoint.
    pub position: i64,
}

#[derive(Default)]
pub(crate) struct FeedCounters {
    enabled: AtomicBool,
    leader: AtomicBool,
    runs: AtomicU64,
    records: AtomicU64,
    companies_due: AtomicU64,
    failures: AtomicU64,
    last_run_at: AtomicI64,
    position: AtomicI64,
}

impl FeedCounters {
    pub(crate) fn snapshot(&self, feed: Feed) -> FeedStats {
        let get = |a: &AtomicU64| a.load(Ordering::Relaxed);
        FeedStats {
            feed: feed.as_str(),
            enabled: self.enabled.load(Ordering::Relaxed),
            leader: self.leader.load(Ordering::Relaxed),
            runs: get(&self.runs),
            records: get(&self.records),
            companies_due: get(&self.companies_due),
            failures: get(&self.failures),
            last_run_at: self.last_run_at.load(Ordering::Relaxed),
            position: self.position.load(Ordering::Relaxed),
        }
    }
}

/// What a run gives: the new cursor and when to run next.
struct Ran {
    cursor: String,
    next_in: Duration,
}

/// A failed run.
enum RunError {
    /// A registry answered badly.
    Registry(Warning),
    /// The store failed.
    Store(doewood_model::Error),
    /// The lease was lost (another instance took it over): stop, nothing saved.
    LeaseLost,
}

impl std::fmt::Display for RunError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Registry(w) => write!(f, "{w}"),
            Self::Store(e) => write!(f, "store: {e}"),
            Self::LeaseLost => f.write_str("the feed's lease was taken over"),
        }
    }
}

impl From<Warning> for RunError {
    fn from(w: Warning) -> Self {
        Self::Registry(w)
    }
}

impl From<doewood_model::Error> for RunError {
    fn from(e: doewood_model::Error) -> Self {
        Self::Store(e)
    }
}

/// One feed's context for a run.
struct Run<'a> {
    job: &'a Enrichment,
    feed: Feed,
    backend: Arc<dyn Backend>,
    owner: InstanceId,
    lease: Duration,
    shutdown: watch::Receiver<bool>,
}

impl Run<'_> {
    fn counters(&self) -> &FeedCounters {
        &self.job.inner.feed_counters[self.feed.index()]
    }

    async fn renew(&self) -> Result<(), RunError> {
        if self
            .backend
            .lease_feed(self.feed.as_str(), &self.owner, self.lease)
            .await?
        {
            Ok(())
        } else {
            self.counters().leader.store(false, Ordering::Relaxed);
            Err(RunError::LeaseLost)
        }
    }

    fn stopping(&self) -> bool {
        *self.shutdown.borrow()
    }

    /// The companies owning `items` (alias, cache key prefix): their cached answers of this
    /// feed's registry are dropped and the registry made due. Returns how many companies.
    async fn apply(&self, items: &[(String, Option<String>)]) -> Result<u64, RunError> {
        let registry = self.feed.as_str();
        let mut done = 0;
        for chunk in items.chunks(500) {
            let aliases: Vec<String> = chunk
                .iter()
                .map(|(a, _)| a.clone())
                .collect::<BTreeSet<_>>()
                .into_iter()
                .collect();
            let found = self.backend.companies_by_alias(&aliases).await?;
            if found.is_empty() {
                continue;
            }
            let mut companies: BTreeSet<Uuid> = BTreeSet::new();
            for (alias, company) in &found {
                if let Some((_, Some(prefix))) = chunk.iter().find(|(a, _)| a == alias) {
                    self.backend.cache_invalidate(registry, prefix).await?;
                }
                companies.insert(*company);
            }
            let companies: Vec<Uuid> = companies.into_iter().collect();
            let n = self.backend.mark_many_due(&companies, registry).await?;
            for c in &companies {
                debug!(feed = registry, company = %c, "feed: company made due");
            }
            done += n;
            self.counters()
                .companies_due
                .fetch_add(n, Ordering::Relaxed);
        }
        if done > 0 {
            self.job.wake();
        }
        Ok(done)
    }

    fn records(&self, n: u64) {
        self.counters().records.fetch_add(n, Ordering::Relaxed);
    }

    fn position(&self, p: i64) {
        self.counters().position.store(p, Ordering::Relaxed);
    }
}

impl Enrichment {
    /// Runs `feed` once now if its next run has come (and its lease is free): what the feed's
    /// loop does on every tick. Returns whether this instance holds the lease afterwards.
    pub async fn run_feed(&self, feed: Feed, backend: &Arc<dyn Backend>) -> bool {
        let Some(settings) = self.settings().await else {
            return false;
        };
        let on = feed.enabled(&settings);
        self.inner.feed_counters[feed.index()]
            .enabled
            .store(on, Ordering::Relaxed);
        if !on {
            return false;
        }
        let (_tx, rx) = watch::channel(false);
        let owner = self.inner.instance.clone();
        let holding = self.feed_once(feed, backend, &settings, &owner, &rx).await;
        self.inner.feed_counters[feed.index()]
            .leader
            .store(holding, Ordering::Relaxed);
        self.changed();
        holding
    }

    /// One feed's counters.
    pub fn feed_stats(&self, feed: Feed) -> FeedStats {
        self.inner.feed_counters[feed.index()].snapshot(feed)
    }

    /// Runs one feed's loop until shutdown.
    pub(crate) async fn feed_loop(&self, feed: Feed, mut shutdown: watch::Receiver<bool>) {
        let options = self.inner.options.feeds.clone();
        let owner = self.inner.instance.clone();
        let mut holding = false;
        loop {
            if *shutdown.borrow() {
                break;
            }
            let backend = self.inner.backend.borrow().clone();
            let settings = self.settings().await;
            let on = settings.as_ref().is_some_and(|s| feed.enabled(s));
            let counters = &self.inner.feed_counters[feed.index()];
            if counters.enabled.swap(on, Ordering::Relaxed) != on {
                self.changed();
            }
            if let (Some(backend), Some(settings)) = (backend, settings) {
                if on {
                    holding = self
                        .feed_once(feed, &backend, &settings, &owner, &shutdown)
                        .await;
                } else if holding {
                    let _ = backend.release_feed(feed.as_str(), &owner).await;
                    holding = false;
                }
                if counters.leader.swap(holding, Ordering::Relaxed) != holding {
                    self.changed();
                }
            }
            tokio::select! {
                () = tokio::time::sleep(options.tick) => {}
                _ = shutdown.wait_for(|s| *s) => break,
            }
        }
        let backend = self.inner.backend.borrow().clone();
        if holding && let Some(b) = backend {
            let _ = b.release_feed(feed.as_str(), &owner).await;
        }
    }

    /// One look at `feed`: when its next run has come, lease it, run it, store the cursor.
    /// Returns whether this instance still holds the lease (only the Companies House stream
    /// keeps it between runs).
    async fn feed_once(
        &self,
        feed: Feed,
        backend: &Arc<dyn Backend>,
        settings: &EnrichmentConfig,
        owner: &InstanceId,
        shutdown: &watch::Receiver<bool>,
    ) -> bool {
        let name = feed.as_str();
        let options = &self.inner.options.feeds;
        let now = super::unix_now();
        match backend.feed_state(name).await {
            Ok(Some(s)) if s.next_run_at > now && s.owner.as_deref() != Some(owner.as_str()) => {
                return false;
            }
            Ok(_) => {}
            Err(e) => {
                warn!(feed = name, error = %e, "feed state unreadable; tried again later");
                return false;
            }
        }
        match backend.lease_feed(name, owner, options.lease).await {
            Ok(true) => {}
            Ok(false) => {
                debug!(feed = name, "another instance reads this feed");
                return false;
            }
            Err(e) => {
                warn!(feed = name, error = %e, "feed lease failed; tried again later");
                return false;
            }
        }
        // Read again under the lease: another instance may have just run it.
        let state = match backend.feed_state(name).await {
            Ok(s) => s,
            Err(e) => {
                warn!(feed = name, error = %e, "feed state unreadable; tried again later");
                let _ = backend.release_feed(name, owner).await;
                return false;
            }
        };
        let state = state.unwrap_or(FeedState {
            feed: name.to_owned(),
            cursor: String::new(),
            next_run_at: 0,
            updated_at: 0,
            owner: Some(owner.as_str().to_owned()),
        });
        if state.next_run_at > now && feed != Feed::CompaniesHouse {
            let _ = backend.release_feed(name, owner).await;
            return false;
        }
        let run = Run {
            job: self,
            feed,
            backend: Arc::clone(backend),
            owner: owner.clone(),
            lease: options.lease,
            shutdown: shutdown.clone(),
        };
        run.counters().leader.store(true, Ordering::Relaxed);
        self.changed();
        let result = match feed {
            Feed::Gleif => run.gleif(&state.cursor).await,
            Feed::Edgar => run.edgar(&state.cursor, settings).await,
            Feed::Apr => run.apr(&state.cursor).await,
            Feed::CompaniesHouse => run.companies_house(&state.cursor, settings).await,
        };
        let c = run.counters();
        let (cursor, next_in) = match result {
            Ok(Ran { cursor, next_in }) => {
                c.runs.fetch_add(1, Ordering::Relaxed);
                c.last_run_at.store(super::unix_now(), Ordering::Relaxed);
                (cursor, next_in)
            }
            Err(RunError::LeaseLost) => {
                warn!(
                    feed = name,
                    "feed lease lost to another instance; this run's progress is dropped"
                );
                c.leader.store(false, Ordering::Relaxed);
                self.changed();
                return false;
            }
            Err(e) => {
                c.failures.fetch_add(1, Ordering::Relaxed);
                warn!(feed = name, error = %e, retry_s = options.retry.as_secs(),
                    "feed run failed; tried again later");
                (state.cursor.clone(), options.retry)
            }
        };
        self.changed();
        let next =
            super::unix_now().saturating_add(i64::try_from(next_in.as_secs()).unwrap_or(i64::MAX));
        match backend.save_feed(name, owner, &cursor, next).await {
            Ok(true) => {}
            Ok(false) => {
                warn!(feed = name, "feed lease lost before the cursor was saved");
                return false;
            }
            Err(e) => warn!(feed = name, error = %e, "feed cursor not saved; read again later"),
        }
        if feed == Feed::CompaniesHouse && !*shutdown.borrow() {
            // The stream keeps its lease (and its one connection) between runs.
            return true;
        }
        let _ = backend.release_feed(name, owner).await;
        false
    }
}

// ---------------------------------------------------------------------------------------------
// GLEIF

impl Run<'_> {
    /// The cursor is the publication last applied, Unix seconds. With none, the latest
    /// publication becomes the cursor (the companies were enriched recently anyway).
    async fn gleif(&self, cursor: &str) -> Result<Ran, RunError> {
        let options = &self.job.inner.options.feeds;
        let client = GleifFeed::new(&self.job.inner.feed_templates.gleif)?;
        let publication = client.latest().await?;
        let published = publication.published_at.unix_timestamp();
        let done = |cursor: i64| Ran {
            cursor: cursor.to_string(),
            next_in: options.gleif_every,
        };
        let Some(last) = cursor.parse::<i64>().ok() else {
            info!(
                feed = "gleif",
                published, "gleif feed: starting from the latest publication"
            );
            self.position(published);
            return Ok(done(published));
        };
        self.position(last);
        if published <= last {
            debug!(feed = "gleif", published, "gleif feed: nothing new");
            return Ok(done(last));
        }
        let gap = Duration::from_secs(u64::try_from(published - last).unwrap_or(u64::MAX));
        let Some(span) = DeltaSpan::covering(gap) else {
            warn!(
                feed = "gleif",
                gap_h = gap.as_secs() / 3600,
                "gleif feed: further behind than the monthly delta; the 30-day schedule covers the gap"
            );
            self.position(published);
            return Ok(done(published));
        };
        let mut due = 0;
        for set in [DataSet::Lei2, DataSet::Rr] {
            let Some(url) = publication.file(set, span) else {
                return Err(Warning::new(
                    doewood_registry::Registry::Gleif,
                    WarningKind::Malformed(format!("no {} {set:?} delta file", span.as_str())),
                )
                .into());
            };
            let (tx, mut rx) = mpsc::channel::<Vec<Identifier>>(4);
            let reading = client.read(url, set, tx);
            tokio::pin!(reading);
            let mut stats = None;
            loop {
                tokio::select! {
                    r = &mut reading, if stats.is_none() => stats = Some(r),
                    batch = rx.recv() => {
                        let Some(batch) = batch else { break };
                        let items: Vec<(String, Option<String>)> = batch
                            .iter()
                            .map(|lei| (lei.alias(), Some(format!("lei-records/{}", lei.value))))
                            .collect();
                        due += self.apply(&items).await?;
                        self.renew().await?;
                    }
                }
                if self.stopping() {
                    return Err(RunError::Registry(Warning::new(
                        doewood_registry::Registry::Gleif,
                        WarningKind::Transport("shutting down".to_owned()),
                    )));
                }
            }
            let stats = match stats {
                Some(s) => s?,
                None => reading.await?,
            };
            self.records(stats.records);
            info!(feed = "gleif", set = ?set, span = span.as_str(), records = stats.records,
                invalid = stats.invalid, truncated = stats.truncated, "gleif feed: delta file read");
        }
        info!(
            feed = "gleif",
            published,
            companies_due = due,
            "gleif feed: publication applied"
        );
        self.position(published);
        Ok(done(published))
    }
}

// ---------------------------------------------------------------------------------------------
// EDGAR

fn day_text(d: Date) -> String {
    format!("{:04}-{:02}-{:02}", d.year(), u8::from(d.month()), d.day())
}

fn parse_day(s: &str) -> Option<Date> {
    let b = s.as_bytes();
    if s.len() != 10 || b[4] != b'-' || b[7] != b'-' {
        return None;
    }
    Date::from_calendar_date(
        s[..4].parse().ok()?,
        time::Month::try_from(s[5..7].parse::<u8>().ok()?).ok()?,
        s[8..].parse().ok()?,
    )
    .ok()
}

fn day_unix(d: Date) -> i64 {
    d.midnight().assume_utc().unix_timestamp()
}

impl Run<'_> {
    /// The cursor is the last day whose index was applied (`YYYY-MM-DD`). With none, the run
    /// starts with yesterday. A day without an index (404) is skipped once a later day has
    /// one, or once it is a weekend or five days old; otherwise the run waits for it.
    async fn edgar(&self, cursor: &str, settings: &EnrichmentConfig) -> Result<Ran, RunError> {
        let options = &self.job.inner.options.feeds;
        let t = &self.job.inner.feed_templates.edgar;
        let config = EdgarFeedConfig {
            www_url: self.job.inner.edgar_www_url.clone(),
            contact: settings.edgar_contact.clone(),
            ..t.clone()
        };
        let client = EdgarFeed::new(&config)?;
        let today = OffsetDateTime::now_utc().date();
        let yesterday = today.previous_day().unwrap_or(today);
        let oldest = today - time::Duration::days(31);
        let mut last = parse_day(cursor)
            .unwrap_or_else(|| yesterday.previous_day().unwrap_or(yesterday))
            .max(oldest);
        let mut day = last.next_day().unwrap_or(last);
        let mut due = 0;
        let mut checked = 0;
        while day <= yesterday && checked < options.edgar_max_days {
            if self.stopping() {
                break;
            }
            checked += 1;
            if let Some(index) = client.day(day).await? {
                self.records(index.rows);
                let items: Vec<(String, Option<String>)> = index
                    .filings
                    .iter()
                    .map(|(cik, _)| (cik_alias(*cik), Some(format!("submissions/CIK{cik}"))))
                    .collect();
                let n = self.apply(&items).await?;
                due += n;
                info!(feed = "edgar", day = %day_text(day), rows = index.rows,
                        filings = index.filings.len(), companies_due = n,
                        truncated = index.truncated, "edgar feed: daily index applied");
                last = day;
                self.renew().await?;
            } else {
                let weekend = matches!(
                    day.weekday(),
                    time::Weekday::Saturday | time::Weekday::Sunday
                );
                if (weekend || today - day >= time::Duration::days(5))
                    && last.next_day() == Some(day)
                {
                    debug!(feed = "edgar", day = %day_text(day), "edgar feed: no index that day");
                    last = day;
                }
            }
            match day.next_day() {
                Some(d) => day = d,
                None => break,
            }
        }
        self.position(day_unix(last));
        if due > 0 {
            info!(feed = "edgar", companies_due = due, "edgar feed: run done");
        }
        Ok(Ran {
            cursor: day_text(last),
            next_in: options.edgar_every,
        })
    }
}

// ---------------------------------------------------------------------------------------------
// APR

impl Run<'_> {
    /// The cursor is the date of the set last applied (`YYYY-MM-DD`). Every run downloads the
    /// set again and swaps it in; when its date is new, each MB a company owns is compared
    /// with the record the cache holds, and the changed ones are made due.
    async fn apr(&self, cursor: &str) -> Result<Ran, RunError> {
        let options = &self.job.inner.options.feeds;
        let apr = {
            let settings = self.job.settings().await;
            let Some(settings) = settings else {
                return Err(Warning::new(
                    doewood_registry::Registry::Apr,
                    WarningKind::Config("settings unreadable".to_owned()),
                )
                .into());
            };
            let (clients, _) = self.job.inner.clients.lock().await.clients(&settings);
            clients.apr.ok_or_else(|| {
                Warning::new(doewood_registry::Registry::Apr, WarningKind::Disabled)
            })?
        };
        let info = apr.refresh().await?;
        let as_of = info.as_of.unwrap_or_else(|| info.fetched_at.date());
        let next_in = |as_of: Date| {
            let age = OffsetDateTime::now_utc().date() - as_of;
            if age.whole_seconds() > i64::try_from(options.apr_stale.as_secs()).unwrap_or(i64::MAX)
            {
                options.apr_overdue_every
            } else {
                options.apr_every
            }
        };
        self.position(day_unix(as_of));
        if parse_day(cursor) == Some(as_of) {
            debug!(feed = "apr", as_of = %day_text(as_of), "apr feed: the set has not changed");
            return Ok(Ran {
                cursor: day_text(as_of),
                next_in: next_in(as_of),
            });
        }
        let mut after: Option<String> = None;
        let mut compared = 0u64;
        let mut due = 0;
        loop {
            if self.stopping() {
                return Err(Warning::new(
                    doewood_registry::Registry::Apr,
                    WarningKind::Transport("shutting down".to_owned()),
                )
                .into());
            }
            let page = self
                .backend
                .company_aliases("id:rs_mb:", after.as_deref(), 500)
                .await?;
            let Some((last, _)) = page.last() else { break };
            after = Some(last.clone());
            let mut changed = Vec::new();
            for (alias, _) in &page {
                let Some(mb) = alias
                    .strip_prefix("id:rs_mb:")
                    .and_then(|v| Identifier::parse(IdKind::RsMb, v))
                else {
                    continue;
                };
                compared += 1;
                if apr.update_cached(&mb).await? {
                    changed.push((alias.clone(), None));
                }
            }
            due += self.apply(&changed).await?;
            self.renew().await?;
        }
        self.records(compared);
        info!(feed = "apr", as_of = %day_text(as_of), companies = info.companies, compared,
            companies_due = due, "apr feed: new set swapped in");
        Ok(Ran {
            cursor: day_text(as_of),
            next_in: next_in(as_of),
        })
    }
}

// ---------------------------------------------------------------------------------------------
// Companies House

/// The stream timepoints, `companies=12,officers=34,persons-with-significant-control=56`.
fn timepoints(cursor: &str) -> Vec<(StreamKind, u64)> {
    cursor
        .split(',')
        .filter_map(|p| {
            let (k, v) = p.split_once('=')?;
            Some((StreamKind::parse(k.trim())?, v.trim().parse().ok()?))
        })
        .collect()
}

fn timepoints_text(points: &[(StreamKind, u64)]) -> String {
    points
        .iter()
        .map(|(k, v)| format!("{}={v}", k.as_str()))
        .collect::<Vec<_>>()
        .join(",")
}

fn set_point(points: &mut Vec<(StreamKind, u64)>, kind: StreamKind, value: Option<u64>) {
    points.retain(|(k, _)| *k != kind);
    if let Some(v) = value {
        points.push((kind, v));
        points.sort();
    }
}

impl Run<'_> {
    /// Reads each stream in turn for the stream window, from its stored timepoint (or from
    /// now). The cursor holds the three timepoints; it is saved as the run goes.
    async fn companies_house(
        &self,
        cursor: &str,
        settings: &EnrichmentConfig,
    ) -> Result<Ran, RunError> {
        let options = &self.job.inner.options.feeds;
        let config = ChStreamConfig {
            key: settings
                .companies_house_stream_key
                .as_ref()
                .and_then(|k| doewood_registry::ApiKey::new(k.expose()).ok()),
            ..self.job.inner.feed_templates.companies_house.clone()
        };
        let client = ChStream::new(&config)?;
        let mut points = timepoints(cursor);
        for kind in StreamKind::ALL {
            if self.stopping() {
                break;
            }
            let from = points.iter().find(|(k, _)| *k == kind).map(|(_, v)| *v);
            let mut conn = match client.open(kind, from).await {
                Ok(c) => c,
                Err(w) if w.kind == WarningKind::Status(416) => {
                    warn!(
                        feed = "companies_house",
                        stream = kind.as_str(),
                        "companies house stream: the timepoint is too old; reading from now (the 30-day schedule covers the gap)"
                    );
                    set_point(&mut points, kind, None);
                    client.open(kind, None).await?
                }
                Err(w) if matches!(w.kind, WarningKind::RateLimited { .. }) => {
                    tokio::time::sleep(options.stream_backoff).await;
                    return Err(w.into());
                }
                Err(w) => return Err(w.into()),
            };
            let window = tokio::time::sleep(options.stream_window);
            tokio::pin!(window);
            let mut pending: Vec<(String, Option<String>)> = Vec::new();
            let mut last: Option<u64> = None;
            let mut events = 0u64;
            let mut flush_at = tokio::time::Instant::now() + Duration::from_secs(10);
            let mut stop = self.shutdown.clone();
            loop {
                let next = tokio::select! {
                    () = &mut window => break,
                    _ = stop.wait_for(|s| *s) => break,
                    () = tokio::time::sleep_until(flush_at) => None,
                    e = conn.next() => Some(e),
                };
                match next {
                    None => {}
                    Some(Ok(Some(event))) => {
                        events += 1;
                        last = Some(event.timepoint);
                        if let Some(n) = event.company {
                            pending.push(ch_item(&n));
                        }
                        if pending.len() < 200 {
                            continue;
                        }
                    }
                    Some(Ok(None)) => break,
                    Some(Err(w)) => {
                        // What was read up to here is kept; the next stream takes its turn.
                        warn!(feed = "companies_house", stream = kind.as_str(), warning = %w,
                            "companies house stream: connection ended");
                        break;
                    }
                }
                self.flush_stream(&mut pending, &mut points, kind, last, events)
                    .await?;
                events = 0;
                flush_at = tokio::time::Instant::now() + Duration::from_secs(10);
            }
            self.flush_stream(&mut pending, &mut points, kind, last, events)
                .await?;
            if conn.skipped > 0 || conn.overlong() > 0 {
                warn!(
                    feed = "companies_house",
                    stream = kind.as_str(),
                    skipped = conn.skipped,
                    overlong = conn.overlong(),
                    "companies house stream: lines skipped"
                );
            }
        }
        Ok(Ran {
            cursor: timepoints_text(&points),
            next_in: Duration::ZERO,
        })
    }

    async fn flush_stream(
        &self,
        pending: &mut Vec<(String, Option<String>)>,
        points: &mut Vec<(StreamKind, u64)>,
        kind: StreamKind,
        last: Option<u64>,
        events: u64,
    ) -> Result<(), RunError> {
        if !pending.is_empty() {
            let n = self.apply(pending).await?;
            if n > 0 {
                debug!(
                    feed = "companies_house",
                    stream = kind.as_str(),
                    companies_due = n,
                    "companies house stream: companies made due"
                );
            }
            pending.clear();
        }
        self.records(events);
        if let Some(t) = last {
            set_point(points, kind, Some(t));
            self.position(i64::try_from(t).unwrap_or(i64::MAX));
        }
        self.renew().await?;
        let next = super::unix_now();
        if !self
            .backend
            .save_feed(
                self.feed.as_str(),
                &self.owner,
                &timepoints_text(points),
                next,
            )
            .await?
        {
            return Err(RunError::LeaseLost);
        }
        Ok(())
    }
}

fn ch_item(n: &CompanyNumber) -> (String, Option<String>) {
    (ch_alias(n), Some(format!("company/{}", n.as_str())))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn timepoints_round_trip() {
        let mut p =
            timepoints("officers=34, companies=12,bogus=1,persons-with-significant-control=x");
        assert_eq!(p, [(StreamKind::Officers, 34), (StreamKind::Companies, 12)]);
        set_point(&mut p, StreamKind::Pscs, Some(56));
        set_point(&mut p, StreamKind::Officers, None);
        assert_eq!(
            timepoints_text(&p),
            "companies=12,persons-with-significant-control=56"
        );
        assert!(timepoints("").is_empty());
    }

    #[test]
    fn days() {
        let d = parse_day("2026-11-24").unwrap();
        assert_eq!(day_text(d), "2026-11-24");
        assert!(parse_day("2026-02-30").is_none());
        assert!(parse_day("20261124").is_none());
        assert_eq!(day_unix(d) % 86_400, 0);
    }
}
