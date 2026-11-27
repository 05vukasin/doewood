//! Purges (issue #168): deleting what no live location holds any more.
//!
//! A location is live while the `locations` table has its row: ingest writes it with the
//! `located_at` edge, and an object deleted from its bucket ([`crate::GraphStore::end_location`])
//! loses it. A purge deletes, in bounded batches of one transaction each:
//!
//! - **documents** no live location holds: the record (with its search row, the pgvector
//!   column with it, its text in `document_texts`, its payment row in `payments`, and its sketch
//!   and band rows), the Document node with every edge from or
//!   to it (`located_at`, `party`, `amends`, `cites`, `similar_to`) and its aliases. A live
//!   document's `cites` or `amends` edge to a purged one goes back to the pending reference
//!   node of its number, as it was before that document arrived;
//! - **Location nodes** without a live row (an object deleted from its bucket, or the
//!   locations a source purge removes);
//! - **pending reference nodes** (`ref:<number>`) left without edges;
//! - **companies** a purged document named that no document names any more (no `party`
//!   edge in any state), unless a registry created them (`origin: registry`), and the
//!   companies merged into a purged one (their husks: the merge log's absorbed companies
//!   whose `merged_into` still names it) under the same rule. The merge log keeps its
//!   entries (it has no foreign keys); undoing one whose companies are gone brings nothing
//!   back.
//!
//! Every edge delete is a delete on `edges` (a cascade from the node), so the company graph's
//! log triggers record it and the explorer's folded counts stay exact.
//!
//! **Against ingest.** A batch locks its records (Postgres: `FOR UPDATE`, in hash order)
//! before it looks for live locations in a statement of its own, and
//! [`crate::GraphStore::upsert_location`] takes a share lock on the record in the statement that
//! writes the location row. So a location written while the record is there is either seen by
//! the batch, which keeps the document, or waits for it, and then finds the record gone
//! ([`crate::Located::record`] false): ingest then stores the record again. On SQLite one
//! writer serializes both. Location and company nodes are locked the same way before they are
//! checked and deleted.

use crate::Blake3;

/// Most rows one purge batch examines.
pub const PURGE_BATCH_MAX: u32 = 1000;

/// The batch size for `limit` (0 means 200; at most [`PURGE_BATCH_MAX`]).
pub fn purge_limit(limit: u32) -> u32 {
    match limit {
        0 => 200,
        n => n.min(PURGE_BATCH_MAX),
    }
}

/// What one purge batch did.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Purged {
    /// The rows the batch looked at: live locations removed, Location nodes or records
    /// examined.
    pub examined: u64,
    /// Location nodes deleted.
    pub locations: u64,
    /// Records deleted, with their search rows and sketches; their Document nodes went too.
    pub records: u64,
    /// Document nodes deleted that had no record (pending references, and documents whose
    /// record was missing).
    pub other_documents: u64,
    /// Company nodes deleted.
    pub companies: u64,
    /// The records deleted, for the instance's own rows about them (the LLM queue).
    pub hashes: Vec<Blake3>,
    /// Where the next batch starts (opaque); `None` when this was the last.
    pub next: Option<String>,
}

impl Purged {
    /// Adds `other`'s counts (not its hashes or cursor).
    pub fn add(&mut self, other: &Self) {
        self.examined += other.examined;
        self.locations += other.locations;
        self.records += other.records;
        self.other_documents += other.other_documents;
        self.companies += other.companies;
    }
}
