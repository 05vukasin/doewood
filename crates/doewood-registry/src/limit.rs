//! A token bucket: each registry client has its own (ADR 0002 section 2).

use std::time::Duration;

use tokio::sync::Mutex;

use crate::clock::Clock;

/// A token bucket of `burst` tokens, refilled at `per_minute` tokens a minute. Callers wait in
/// turn: the lock is held while one waits, so requests leave spaced, never in a burst larger
/// than the bucket.
#[derive(Debug)]
pub struct TokenBucket {
    capacity: f64,
    /// Seconds per token.
    interval: f64,
    state: Mutex<State>,
}

#[derive(Debug)]
struct State {
    tokens: f64,
    last: Option<Duration>,
}

impl TokenBucket {
    /// `per_minute` and `burst` are at least 1. The bucket starts full.
    pub fn new(per_minute: u32, burst: u32) -> Self {
        let capacity = f64::from(burst.max(1));
        Self {
            capacity,
            interval: 60.0 / f64::from(per_minute.max(1)),
            state: Mutex::new(State {
                tokens: capacity,
                last: None,
            }),
        }
    }

    /// Takes one token, waiting on `clock` until there is one.
    pub async fn acquire(&self, clock: &dyn Clock) {
        let mut s = self.state.lock().await;
        let now = clock.monotonic();
        self.refill(&mut s, now);
        if s.tokens >= 1.0 {
            s.tokens -= 1.0;
            return;
        }
        let wait = Duration::from_secs_f64((1.0 - s.tokens) * self.interval);
        clock.sleep(wait).await;
        // The token that just arrived is spent at once.
        s.tokens = 0.0;
        s.last = Some(now + wait);
    }

    /// Empties the bucket: after a 429, the next request waits a full interval too.
    pub async fn drain(&self, clock: &dyn Clock) {
        let mut s = self.state.lock().await;
        s.tokens = 0.0;
        s.last = Some(clock.monotonic());
    }

    fn refill(&self, s: &mut State, now: Duration) {
        if let Some(last) = s.last {
            let gained = now.saturating_sub(last).as_secs_f64() / self.interval;
            s.tokens = (s.tokens + gained).min(self.capacity);
        }
        s.last = Some(now);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clock::ManualClock;
    use time::OffsetDateTime;

    fn close(a: Duration, secs: f64) -> bool {
        (a.as_secs_f64() - secs).abs() < 1e-6
    }

    #[tokio::test(flavor = "current_thread")]
    async fn burst_then_spacing() {
        let clock = ManualClock::new(OffsetDateTime::UNIX_EPOCH);
        let bucket = TokenBucket::new(30, 2);
        for _ in 0..5 {
            bucket.acquire(&clock).await;
        }
        // Two from the full bucket, then one every 2 s.
        let sleeps = clock.sleeps();
        assert_eq!(sleeps.len(), 3, "{sleeps:?}");
        assert!(sleeps.iter().all(|d| close(*d, 2.0)), "{sleeps:?}");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn refills_while_idle_up_to_capacity() {
        let clock = ManualClock::new(OffsetDateTime::UNIX_EPOCH);
        let bucket = TokenBucket::new(60, 3);
        for _ in 0..3 {
            bucket.acquire(&clock).await;
        }
        assert!(clock.sleeps().is_empty());
        // A long idle time refills the bucket to 3, not more.
        clock.advance(Duration::from_mins(10));
        for _ in 0..4 {
            bucket.acquire(&clock).await;
        }
        let sleeps = clock.sleeps();
        assert_eq!(sleeps.len(), 1, "{sleeps:?}");
        assert!(close(sleeps[0], 1.0));
        // Half an interval idle: the next one waits the other half.
        clock.advance(Duration::from_millis(500));
        bucket.acquire(&clock).await;
        assert!(
            close(*clock.sleeps().last().unwrap(), 0.5),
            "{:?}",
            clock.sleeps()
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn drain_makes_the_next_request_wait() {
        let clock = ManualClock::new(OffsetDateTime::UNIX_EPOCH);
        let bucket = TokenBucket::new(60, 5);
        bucket.drain(&clock).await;
        bucket.acquire(&clock).await;
        assert_eq!(clock.sleeps().len(), 1);
        assert!(close(clock.sleeps()[0], 1.0));
    }

    #[test]
    fn zero_rates_are_clamped() {
        let b = TokenBucket::new(0, 0);
        assert!((b.capacity - 1.0).abs() < f64::EPSILON);
        assert!((b.interval - 60.0).abs() < f64::EPSILON);
    }
}
