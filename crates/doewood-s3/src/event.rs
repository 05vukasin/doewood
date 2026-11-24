//! The events discovery emits for ingest (step 4) to consume, and the comparison that decides
//! them.

use doewood_core::SourceId;
use doewood_store::{ObjectChange, SeenObject};

/// The version of an object that an event reports.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ObjectVersion {
    /// ETag, without quotes.
    pub etag: Option<String>,
    /// Size in bytes.
    pub size: u64,
    /// Last-modified time (Unix seconds).
    pub last_modified: Option<i64>,
}

/// What happened to an object.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum EventKind {
    /// A key that was not seen before (or was deleted since).
    New(ObjectVersion),
    /// A seen key whose ETag or size differs.
    Changed(ObjectVersion),
    /// A seen key that is gone.
    Deleted,
}

impl EventKind {
    /// `new`, `changed` or `deleted`.
    pub fn name(&self) -> &'static str {
        match self {
            Self::New(_) => "new",
            Self::Changed(_) => "changed",
            Self::Deleted => "deleted",
        }
    }
}

/// Where an event came from.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EventOrigin {
    /// The periodic listing.
    Scan,
    /// A MinIO / S3 event notification.
    Webhook,
}

/// An object event. Delivery is at least once: an event is sent before discovery commits it,
/// so a crash in between sends it again.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ObjectEvent {
    /// The source.
    pub source: SourceId,
    /// The object key.
    pub key: String,
    /// What happened.
    pub kind: EventKind,
    /// Where it was noticed.
    pub origin: EventOrigin,
}

/// A key and what happened to it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Diff {
    pub key: String,
    pub kind: EventKind,
}

impl Diff {
    /// The row change that records this event.
    pub fn change(&self) -> ObjectChange {
        match &self.kind {
            EventKind::New(v) | EventKind::Changed(v) => ObjectChange::Upsert(SeenObject {
                key: self.key.clone(),
                etag: v.etag.clone(),
                size: v.size,
                last_modified: v.last_modified,
            }),
            EventKind::Deleted => ObjectChange::Delete {
                key: self.key.clone(),
            },
        }
    }
}

/// Compares an object as it is now with what was seen: `New` if it was not seen, `Changed` if
/// its ETag or size differs, nothing if it is the same. The last-modified time alone is not a
/// change (a copy onto itself, a metadata update).
pub(crate) fn classify(seen: Option<&SeenObject>, now: ObjectVersion) -> Option<EventKind> {
    match seen {
        None => Some(EventKind::New(now)),
        Some(s) if s.etag != now.etag || s.size != now.size => Some(EventKind::Changed(now)),
        Some(_) => None,
    }
}

/// Merges a listing with the stored rows of the **same key range**, both in byte order, into
/// the events: keys only listed are new, keys in both are compared, keys only stored are
/// deleted.
pub(crate) fn diff<'a>(
    listed: impl IntoIterator<Item = (&'a str, ObjectVersion)>,
    seen: &[SeenObject],
) -> Vec<Diff> {
    let mut out = Vec::new();
    let mut seen = seen.iter().peekable();
    for (key, version) in listed {
        // Stored keys before this one are gone.
        while let Some(s) = seen.next_if(|s| s.key.as_str() < key) {
            out.push(Diff {
                key: s.key.clone(),
                kind: EventKind::Deleted,
            });
        }
        let same = seen.next_if(|s| s.key == key);
        if let Some(kind) = classify(same, version) {
            out.push(Diff {
                key: key.to_owned(),
                kind,
            });
        }
    }
    out.extend(seen.map(|s| Diff {
        key: s.key.clone(),
        kind: EventKind::Deleted,
    }));
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn v(etag: &str, size: u64) -> ObjectVersion {
        ObjectVersion {
            etag: Some(etag.into()),
            size,
            last_modified: Some(1),
        }
    }

    fn seen(key: &str, etag: &str, size: u64) -> SeenObject {
        SeenObject {
            key: key.into(),
            etag: Some(etag.into()),
            size,
            last_modified: Some(1),
        }
    }

    fn kinds(d: &[Diff]) -> Vec<(&str, &str)> {
        d.iter().map(|d| (d.key.as_str(), d.kind.name())).collect()
    }

    #[test]
    fn classify_compares_etag_and_size_only() {
        let s = seen("k", "e1", 10);
        assert_eq!(
            classify(None, v("e1", 10)),
            Some(EventKind::New(v("e1", 10)))
        );
        assert_eq!(classify(Some(&s), v("e1", 10)), None);
        assert_eq!(
            classify(Some(&s), v("e2", 10)),
            Some(EventKind::Changed(v("e2", 10)))
        );
        assert_eq!(
            classify(Some(&s), v("e1", 11)),
            Some(EventKind::Changed(v("e1", 11)))
        );
        let touched = ObjectVersion {
            last_modified: Some(99),
            ..v("e1", 10)
        };
        assert_eq!(classify(Some(&s), touched), None);
    }

    #[test]
    fn diff_merges_new_changed_unchanged_and_deleted() {
        let stored = [
            seen("a", "1", 1),
            seen("b", "1", 1),
            seen("c", "1", 1),
            seen("e", "1", 1),
            seen("g", "1", 1),
        ];
        let listed = [
            ("a", v("1", 1)), // unchanged
            ("b", v("2", 1)), // changed
            ("d", v("1", 1)), // new; c is deleted
            ("e", v("1", 1)), // unchanged
            ("f", v("1", 1)), // new; g (after the last listed) is deleted
        ];
        let d = diff(listed.iter().map(|(k, v)| (*k, v.clone())), &stored);
        assert_eq!(
            kinds(&d),
            [
                ("b", "changed"),
                ("c", "deleted"),
                ("d", "new"),
                ("f", "new"),
                ("g", "deleted")
            ]
        );
    }

    #[test]
    fn diff_edges() {
        assert!(diff([], &[]).is_empty());
        let all_new = diff([("x", v("1", 1)), ("y", v("1", 1))], &[]);
        assert_eq!(kinds(&all_new), [("x", "new"), ("y", "new")]);
        let all_gone = diff([], &[seen("x", "1", 1), seen("y", "1", 1)]);
        assert_eq!(kinds(&all_gone), [("x", "deleted"), ("y", "deleted")]);
        // Byte order: "B" < "a" < "é".
        let d = diff(
            [("B", v("1", 1)), ("é", v("1", 1))],
            &[seen("a", "1", 1), seen("é", "1", 1)],
        );
        assert_eq!(kinds(&d), [("B", "new"), ("a", "deleted")]);
    }

    #[test]
    fn diffs_become_row_changes() {
        let new = Diff {
            key: "k".into(),
            kind: EventKind::New(v("e", 3)),
        };
        assert_eq!(new.change(), ObjectChange::Upsert(seen("k", "e", 3)));
        let gone = Diff {
            key: "k".into(),
            kind: EventKind::Deleted,
        };
        assert_eq!(gone.change(), ObjectChange::Delete { key: "k".into() });
    }
}
