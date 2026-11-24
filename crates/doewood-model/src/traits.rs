//! The store traits. Postgres (`doewood-pg`) implements them for the shared mode, and the
//! instance's SQLite file (`doewood-store`) for standalone mode (ADR 0001, section 2).
//!
//! Every write is an idempotent upsert: repeating it leaves the store as it was.

use std::time::Duration;

use async_trait::async_trait;
use doewood_core::InstanceId;
use serde_json::Value;
use time::Date;
use uuid::Uuid;

use crate::{
    Blake3, Edge, Located, LocationEnded, Neighbour, NeighbourQuery, Node, NodeAlias, NodeKind,
    ObjectLocation, PutOutcome, Record, Result, SearchDoc, SearchHit, SearchQuery, Sketch,
    SourceClaim, SourceKey, StepVersions,
};

/// quail and hedgehog records, keyed by BLAKE3.
#[async_trait]
pub trait RecordStore: Send + Sync {
    /// Inserts or replaces the record. Writing the same body and versions again is
    /// [`PutOutcome::Unchanged`] and does not touch `updated_at`.
    async fn put_record(
        &self,
        blake3: &Blake3,
        body: &Value,
        versions: &StepVersions,
    ) -> Result<PutOutcome>;

    /// The record, if stored.
    async fn get_record(&self, blake3: &Blake3) -> Result<Option<Record>>;

    /// One page of the records that have a location in `source` (its endpoint and bucket, a
    /// key under its prefix), ordered by hash. A page starts after the hash `after`, and holds
    /// at most `limit` records (0 means 50, at most [`crate::RECORDS_MAX_LIMIT`]).
    async fn list_records(
        &self,
        source: &SourceKey,
        after: Option<&Blake3>,
        limit: u32,
    ) -> Result<Vec<Record>>;
}

/// The typed graph of ADR 0001 section 7 and ADR 0002 section 3.
#[async_trait]
pub trait GraphStore: Send + Sync {
    /// Inserts the node, or replaces the props of the node with this kind and key. Returns
    /// its id, which never changes once assigned.
    async fn upsert_node(&self, kind: NodeKind, key: &str, props: &Value) -> Result<Uuid>;

    /// A node by id.
    async fn node(&self, id: Uuid) -> Result<Option<Node>>;

    /// A node by kind and key.
    async fn node_by_key(&self, kind: NodeKind, key: &str) -> Result<Option<Node>>;

    /// Inserts the edge, or updates the edge with the same `(src, dst, kind, source,
    /// valid_from)`. Returns its id. Both nodes must exist.
    async fn upsert_edge(&self, edge: &Edge) -> Result<Uuid>;

    /// The edges at a node and the nodes at their other ends, ordered by edge kind, then
    /// `valid_from` (undated first), then edge id.
    async fn neighbours(&self, node: Uuid, query: &NeighbourQuery) -> Result<Vec<Neighbour>>;

    /// The documents with a live `party` edge to the company, ordered by key.
    async fn company_documents(&self, company: Uuid) -> Result<Vec<Node>>;

    /// Records that a document lives at a location, in one transaction: upserts the Document
    /// and Location nodes and the `located_at` edge, and supersedes any `located_at` edge from
    /// another document to that location (the object was overwritten).
    async fn upsert_location(&self, blake3: &Blake3, location: &ObjectLocation) -> Result<Located>;

    /// Records that the object at a location is gone (deleted from its bucket), in one
    /// transaction: the live `located_at` edges to it become superseded with `valid_to = on`,
    /// and the location no longer counts for source filters. The Document node, its record and
    /// its other locations stay. `None` when the location was never recorded.
    async fn end_location(
        &self,
        location: &ObjectLocation,
        on: Date,
    ) -> Result<Option<LocationEnded>>;

    /// Claims `alias` (an identifier or a name key that resolves to one node, ADR 0002
    /// section 1) for `node`, with `props` (evidence, confidence). The first claim wins: the
    /// result is the alias as stored, owned by `node` or by whoever claimed it before (or by
    /// nobody when it is blocked). The node must exist.
    async fn claim_alias(
        &self,
        kind: NodeKind,
        alias: &str,
        node: Uuid,
        props: &Value,
    ) -> Result<NodeAlias>;

    /// The alias, if claimed or blocked.
    async fn alias(&self, kind: NodeKind, alias: &str) -> Result<Option<NodeAlias>>;

    /// Blocks an alias: it resolves to no node from now on (a key that turned out to belong
    /// to more than one), with `props` saying why.
    async fn block_alias(&self, kind: NodeKind, alias: &str, props: &Value) -> Result<()>;
}

/// The `MinHash` LSH band index behind `similar_to` edges: candidates come from a lookup by
/// band key, never from a scan of every document.
#[async_trait]
pub trait SketchStore: Send + Sync {
    /// Stores (or replaces) a document's signature and band keys. Its record must exist.
    async fn put_sketch(&self, blake3: &Blake3, signature: &[u8], bands: &[u64]) -> Result<()>;

    /// Documents other than `exclude` that share at least one of `bands`, most shared bands
    /// first, then by hash; at most `limit` (0 means 100, at most 1000).
    async fn sketch_candidates(
        &self,
        bands: &[u64],
        exclude: &Blake3,
        limit: u32,
    ) -> Result<Vec<Sketch>>;
}

/// The page size [`SketchStore::sketch_candidates`] uses for `limit`.
pub fn sketch_limit(limit: u32) -> u32 {
    match limit {
        0 => 100,
        n => n.min(1000),
    }
}

/// Ownership of sources between instances, and the enrichment leases on companies
/// (ADR 0002, section 6). In standalone mode both are local to the instance.
#[async_trait]
pub trait ClaimStore: Send + Sync {
    /// Claims the source for `owner`. Claiming a source it already owns is a no-op; a source
    /// another instance owns fails with [`crate::Error::SourceClaimed`] naming the owner.
    async fn claim_source(&self, key: &SourceKey, owner: &InstanceId) -> Result<SourceClaim>;

    /// Releases `owner`'s claim. Returns whether there was one; a claim held by another
    /// instance fails with [`crate::Error::SourceClaimed`].
    async fn release_source(&self, key: &SourceKey, owner: &InstanceId) -> Result<bool>;

    /// Who owns the source, if anyone.
    async fn source_owner(&self, key: &SourceKey) -> Result<Option<SourceClaim>>;

    /// Takes or renews the enrichment lease on a company for `ttl`. Returns `false` when
    /// another instance holds an unexpired lease.
    async fn lease_company(&self, company: Uuid, owner: &InstanceId, ttl: Duration)
    -> Result<bool>;

    /// Gives the lease back. Returns whether `owner` held it.
    async fn release_company(&self, company: Uuid, owner: &InstanceId) -> Result<bool>;
}

/// Full-text search with filters.
#[async_trait]
pub trait SearchStore: Send + Sync {
    /// Indexes (or re-indexes) a document. Its record must exist.
    async fn index_document(&self, doc: &SearchDoc) -> Result<()>;

    /// Searches; see [`SearchQuery`] and [`SearchHit`] for the semantics and order.
    async fn search(&self, query: &SearchQuery) -> Result<Vec<SearchHit>>;
}

/// A complete backend: everything the service needs from its store.
#[async_trait]
pub trait Backend: RecordStore + GraphStore + ClaimStore + SearchStore + SketchStore {
    /// `postgres` or `sqlite`.
    fn name(&self) -> &'static str;

    /// Checks the backend answers.
    async fn ping(&self) -> Result<()>;
}
