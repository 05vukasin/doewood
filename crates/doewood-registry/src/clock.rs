//! Time for the rate limiter, the backoff and the cache, behind a trait so tests run without
//! waiting.

use std::fmt;
use std::sync::{Mutex, PoisonError};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use time::OffsetDateTime;

/// A monotonic clock, the wall clock, and sleeping.
#[async_trait]
pub trait Clock: Send + Sync + fmt::Debug {
    /// Monotonic time since an arbitrary start.
    fn monotonic(&self) -> Duration;
    /// The wall clock (UTC), for fetch dates.
    fn now_utc(&self) -> OffsetDateTime;
    /// Waits `d`.
    async fn sleep(&self, d: Duration);
}

/// The real clock: `Instant`, the system time and `tokio::time::sleep`.
#[derive(Debug)]
pub struct SystemClock {
    start: Instant,
}

impl Default for SystemClock {
    fn default() -> Self {
        Self {
            start: Instant::now(),
        }
    }
}

#[async_trait]
impl Clock for SystemClock {
    fn monotonic(&self) -> Duration {
        self.start.elapsed()
    }

    fn now_utc(&self) -> OffsetDateTime {
        OffsetDateTime::now_utc()
    }

    async fn sleep(&self, d: Duration) {
        tokio::time::sleep(d).await;
    }
}

/// A clock that only moves when told to: [`Clock::sleep`] returns at once, advances the clock
/// and records the wait. For tests (rate-limit spacing, `Retry-After`, cache expiry).
#[derive(Debug)]
pub struct ManualClock {
    state: Mutex<ManualState>,
}

#[derive(Debug)]
struct ManualState {
    monotonic: Duration,
    utc: OffsetDateTime,
    sleeps: Vec<Duration>,
}

impl ManualClock {
    /// A clock at monotonic zero and wall time `utc`.
    pub fn new(utc: OffsetDateTime) -> Self {
        Self {
            state: Mutex::new(ManualState {
                monotonic: Duration::ZERO,
                utc,
                sleeps: Vec::new(),
            }),
        }
    }

    /// Moves both clocks forward by `d` (not recorded as a sleep).
    pub fn advance(&self, d: Duration) {
        let mut s = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        s.monotonic += d;
        s.utc += d;
    }

    /// Every wait asked of [`Clock::sleep`], in order.
    pub fn sleeps(&self) -> Vec<Duration> {
        self.state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .sleeps
            .clone()
    }
}

#[async_trait]
impl Clock for ManualClock {
    fn monotonic(&self) -> Duration {
        self.state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .monotonic
    }

    fn now_utc(&self) -> OffsetDateTime {
        self.state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .utc
    }

    async fn sleep(&self, d: Duration) {
        let mut s = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        s.monotonic += d;
        s.utc += d;
        s.sleeps.push(d);
    }
}
