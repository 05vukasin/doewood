//! Lenient readers of the text fields of registry answers.

use time::{Date, Month};

/// Trimmed, non-empty, at most 1000 characters.
pub(crate) fn text(s: Option<String>) -> Option<String> {
    let s = s?;
    let t = s.trim();
    if t.is_empty() {
        return None;
    }
    Some(t.chars().take(1000).collect())
}

/// The date of an ISO 8601 date or date-time (`2021-03-04`, `2021-03-04T00:00:00Z`,
/// `2021-03-04T00:00:00.000+01:00`): its first ten characters.
pub(crate) fn date(s: Option<&str>) -> Option<Date> {
    let s = s?.trim();
    let d = s.get(..10)?;
    if s.len() > 10 && !s[10..].starts_with('T') {
        return None;
    }
    let b = d.as_bytes();
    if !d.is_ascii() || b[4] != b'-' || b[7] != b'-' {
        return None;
    }
    let year: i32 = d[..4].parse().ok()?;
    let month: u8 = d[5..7].parse().ok()?;
    let day: u8 = d[8..10].parse().ok()?;
    Date::from_calendar_date(year, Month::try_from(month).ok()?, day).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dates() {
        let d = Date::from_calendar_date(2021, Month::March, 4).unwrap();
        assert_eq!(date(Some("2021-03-04")), Some(d));
        assert_eq!(date(Some("2021-03-04T00:00:00Z")), Some(d));
        assert_eq!(date(Some(" 2021-03-04T23:00:00.000+01:00 ")), Some(d));
        assert_eq!(date(Some("2021-02-30")), None);
        assert_eq!(date(Some("2021/03/04")), None);
        assert_eq!(date(Some("2021-03-04X")), None);
        assert_eq!(date(Some("2021-3-4")), None);
        assert_eq!(date(Some("")), None);
        assert_eq!(date(Some("€€€€-€€-€€")), None);
        assert_eq!(date(Some("1€2-03-04xx")), None);
        assert_eq!(date(None), None);
    }
}
