//! Retries: exponential backoff with full jitter on throttling (429, 503 `SlowDown`), server
//! errors (5xx), S3's `RequestTimeout` and transport failures.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// How requests are retried.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RetryPolicy {
    /// Attempts in total, the first included; 1 disables retries.
    pub max_attempts: u32,
    /// Backoff before the first retry; it doubles each time.
    pub base_delay: Duration,
    /// Longest backoff, and longest `Retry-After` honoured.
    pub max_delay: Duration,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: 6,
            base_delay: Duration::from_millis(200),
            max_delay: Duration::from_secs(20),
        }
    }
}

impl RetryPolicy {
    /// The delay before retry number `retry` (1 for the first), with full jitter: a uniform
    /// random duration up to the exponential bound, so that many clients do not retry in step.
    /// A server's `Retry-After` is a floor.
    pub fn delay(&self, retry: u32, retry_after: Option<Duration>) -> Duration {
        let bound = self
            .base_delay
            .saturating_mul(
                1_u32
                    .checked_shl(retry.saturating_sub(1))
                    .unwrap_or(u32::MAX),
            )
            .min(self.max_delay);
        let jittered = bound.mul_f64(jitter());
        retry_after.map_or(jittered, |ra| ra.min(self.max_delay).max(jittered))
    }
}

/// Whether a response status (with its S3 error code, if read) is worth retrying.
pub fn retryable_status(status: u16, code: Option<&str>) -> bool {
    matches!(status, 429 | 500 | 502 | 503 | 504)
        || matches!(
            code,
            Some("SlowDown" | "RequestTimeout" | "InternalError" | "ServiceUnavailable")
        )
}

/// A value in `[0, 1)`. Backoff jitter needs no strong randomness: the clock's nanoseconds
/// through a `SplitMix64` step, so concurrent clients spread out.
fn jitter() -> f64 {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.subsec_nanos());
    let mut z = u64::from(nanos).wrapping_add(0x9E37_79B9_7F4A_7C15);
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^= z >> 31;
    // The top 53 bits as a fraction: exact in an f64.
    #[allow(clippy::cast_precision_loss)]
    let fraction = (z >> 11) as f64 / (1_u64 << 53) as f64;
    fraction
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn statuses_that_are_retried() {
        for s in [429, 500, 502, 503, 504] {
            assert!(retryable_status(s, None), "{s}");
        }
        assert!(retryable_status(503, Some("SlowDown")));
        assert!(retryable_status(400, Some("RequestTimeout")));
        for s in [400, 401, 403, 404, 409, 412] {
            assert!(!retryable_status(s, None), "{s}");
        }
        assert!(!retryable_status(403, Some("AccessDenied")));
    }

    #[test]
    fn backoff_grows_to_the_cap_and_honours_retry_after() {
        let p = RetryPolicy {
            max_attempts: 10,
            base_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(1),
        };
        for retry in 1..=10 {
            let bound = Duration::from_millis(100 * (1 << (retry - 1))).min(p.max_delay);
            for _ in 0..50 {
                assert!(p.delay(retry, None) <= bound);
            }
        }
        assert!(p.delay(1, Some(Duration::from_millis(700))) >= Duration::from_millis(700));
        // Retry-After is capped too.
        assert!(p.delay(1, Some(Duration::from_hours(1))) <= p.max_delay);
        // A huge retry number does not overflow.
        assert!(p.delay(u32::MAX, None) <= p.max_delay);
    }

    #[test]
    fn jitter_is_a_fraction() {
        for _ in 0..1000 {
            let j = jitter();
            assert!((0.0..1.0).contains(&j));
        }
    }
}
