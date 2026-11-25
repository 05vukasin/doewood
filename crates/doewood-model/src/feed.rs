//! Registry delta feeds (ADR 0002 section 6, issue #49): what the store keeps so that one
//! instance per cluster pulls each feed (a lease on the feed's row) and the next one carries on
//! where it stopped (the feed's cursor).
//!
//! A feed never writes company facts: it finds the companies it names among the ones the graph
//! already has ([`crate::FeedStore::companies_by_alias`]), drops their cached registry answers
//! ([`crate::CacheStore::cache_invalidate`]) and makes the registry due for them
//! ([`crate::FreshnessStore::mark_due`]); the enrichment job then asks the registry again.

use crate::Error;

/// Longest feed name.
pub const FEED_NAME_MAX_LEN: usize = 32;
/// Longest cursor, in bytes.
pub const FEED_CURSOR_MAX_LEN: usize = 1024;
/// Most aliases (or companies) in one call of the feed lookups.
pub const FEED_BATCH_MAX: usize = 1000;

/// Checks a feed name: 1 to [`FEED_NAME_MAX_LEN`] of `a-z`, `0-9` and `_`.
pub fn validate_feed(name: &str) -> Result<(), Error> {
    if name.is_empty()
        || name.len() > FEED_NAME_MAX_LEN
        || !name
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_')
    {
        return Err(Error::Invalid(format!(
            "a feed name is 1 to {FEED_NAME_MAX_LEN} of a-z, 0-9 and _, not {name:?}"
        )));
    }
    Ok(())
}

/// Checks a cursor: at most [`FEED_CURSOR_MAX_LEN`] bytes, without control characters.
pub fn validate_cursor(cursor: &str) -> Result<(), Error> {
    if cursor.len() > FEED_CURSOR_MAX_LEN || cursor.chars().any(char::is_control) {
        return Err(Error::Invalid(format!(
            "a feed cursor is at most {FEED_CURSOR_MAX_LEN} bytes, without control characters"
        )));
    }
    Ok(())
}

/// Checks a batch size: at most [`FEED_BATCH_MAX`].
pub fn validate_batch(n: usize) -> Result<(), Error> {
    if n > FEED_BATCH_MAX {
        return Err(Error::Invalid(format!(
            "at most {FEED_BATCH_MAX} items per feed lookup"
        )));
    }
    Ok(())
}

/// A feed's shared state.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FeedState {
    /// The feed (`gleif`, `edgar`, `apr`, `companies_house`).
    pub feed: String,
    /// Where it stopped: the feed's own resumable position (a publication time, a day, a
    /// snapshot date, a `timepoint`). Empty before the first run.
    pub cursor: String,
    /// When it is to be pulled next, Unix seconds (0: at once).
    pub next_run_at: i64,
    /// When the cursor was last written, Unix seconds (0: never).
    pub updated_at: i64,
    /// The instance holding the lease, if one holds an unexpired one.
    pub owner: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names_and_cursors_are_checked() {
        for ok in ["gleif", "companies_house", "edgar1"] {
            assert!(validate_feed(ok).is_ok(), "{ok}");
        }
        for bad in ["", "GLEIF", "a-b", &"x".repeat(33)] {
            assert!(validate_feed(bad).is_err(), "{bad}");
        }
        assert!(validate_cursor("").is_ok());
        assert!(validate_cursor("2026-11-25T08:00:00Z").is_ok());
        assert!(validate_cursor("a\nb").is_err());
        assert!(validate_cursor(&"c".repeat(FEED_CURSOR_MAX_LEN + 1)).is_err());
        assert!(validate_batch(FEED_BATCH_MAX).is_ok());
        assert!(validate_batch(FEED_BATCH_MAX + 1).is_err());
    }
}
