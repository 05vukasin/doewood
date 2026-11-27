//! The store traits. Postgres (`doewood-pg`) implements them for the shared mode, and the
//! instance's SQLite file (`doewood-store`) for standalone mode (ADR 0001, section 2).
//!
//! Every write is an idempotent upsert: repeating it leaves the store as it was.

use std::collections::BTreeMap;
use std::time::Duration;

use async_trait::async_trait;
use doewood_core::InstanceId;
use serde_json::Value;
use serde_json::value::RawValue;
use time::Date;
use uuid::Uuid;

use crate::{
    Blake3, CacheRow, Candidates, CompanyCursor, CompanyFilter, CompanyLink, CompanyOrder,
    CompanyRow, DismissRequest, Dismissal, DismissalQuery, DueCompany, Edge, EdgeCursor,
    Enrichment, FeedState, Folded, IconRow, LiveEdge, Located, LocationEnded, MergeEntry,
    MergeQuery, MergeRequest, Neighbour, NeighbourQuery, Node, NodeAlias, NodeKind, ObjectLocation,
    OwnedAlias, PartyDocument, Proposal, ProposalQuery, ProposalRequest, PropsPatch, Purged, PutOutcome,
    Record, ResolveRequest, Result, SearchDoc, SearchHit, SearchQuery, Sketch, SourceClaim,
    SourceKey, SplitRequest, StepVersions, StoredText, UndoOutcome, UndoRequest,
};

/// quail and hedgehog records, keyed by BLAKE3.
#[async_trait]
pub trait RecordStore: Send + Sync {
    /// Inserts or replaces the record. `body` is JSON text in the form
    /// [`crate::canonical::to_canonical`] writes. Writing the same body and versions again is
    /// [`PutOutcome::Unchanged`] and does not touch `updated_at`.
    async fn put_record(
        &self,
        blake3: &Blake3,
        body: &RawValue,
        versions: &StepVersions,
    ) -> Result<PutOutcome>;

    /// The record, if stored.
    async fn get_record(&self, blake3: &Blake3) -> Result<Option<Record>>;

    /// Replaces a stored record, as [`RecordStore::put_record`] does, but never inserts one:
    /// `false` when there is no record (a purge deleted it, issue #168), and nothing is
    /// written. For writers that edit a record they read (the LLM summary).
    async fn update_record(
        &self,
        blake3: &Blake3,
        body: &RawValue,
        versions: &StepVersions,
    ) -> Result<bool>;

    /// The step versions of the stored records among `hashes`, without their bodies (issue
    /// #75: reprocess compares versions only). Hashes with no record are missing from the
    /// map. At most [`crate::RECORDS_MAX_LIMIT`] hashes per call.
    async fn record_versions(&self, hashes: &[Blake3]) -> Result<BTreeMap<Blake3, StepVersions>>;

    /// The text values of the facts named `key` in the stored records among `hashes`: one
    /// entry per fact of the record's `facts` array whose `key` is `key` and whose
    /// `value.v` is a string, in no particular order. Only those values are read, not the
    /// bodies (issue #75). At most [`crate::RECORDS_MAX_LIMIT`] hashes per call.
    async fn fact_texts(&self, hashes: &[Blake3], key: &str) -> Result<Vec<(Blake3, String)>>;

    /// One page of the records that have a location in `source` (its endpoint and bucket, a
    /// key under its prefix), ordered by hash. A page starts after the hash `after`, holds at
    /// most `limit` records (0 means 50, at most [`crate::RECORDS_MAX_LIMIT`]), and ends with
    /// the record that brings its bodies ([`crate::record_page_bytes`]) to `max_bytes` or
    /// more, so it holds at least one record when there is one. Records past that point are
    /// not read into memory. [`crate::records_page_full`] tells whether a next page may follow.
    async fn list_records(
        &self,
        source: &SourceKey,
        after: Option<&Blake3>,
        limit: u32,
        max_bytes: usize,
    ) -> Result<Vec<Record>>;

    /// Inserts or replaces the document text of the stored record `blake3` (issue #166):
    /// canonical JSON text of a [`crate::DocumentText`], as [`crate::DocumentText::to_body`]
    /// writes it. Writing the same text again does not touch `updated_at`. Removing the record
    /// removes its text.
    ///
    /// # Errors
    ///
    /// [`crate::Error::NotFound`] when no record `blake3` is stored.
    async fn put_document_text(&self, blake3: &Blake3, body: &RawValue) -> Result<()>;

    /// Removes the document text of `blake3`, if any (a file re-analyzed into no text).
    async fn delete_document_text(&self, blake3: &Blake3) -> Result<()>;

    /// The stored document text of `blake3`.
    async fn get_document_text(&self, blake3: &Blake3) -> Result<Option<StoredText>>;
}

/// The typed graph of ADR 0001 section 7 and ADR 0002 section 3.
#[async_trait]
pub trait GraphStore: Send + Sync {
    /// Inserts the node, or replaces the props of the node with this kind and key. Returns
    /// its id, which never changes once assigned.
    async fn upsert_node(&self, kind: NodeKind, key: &str, props: &Value) -> Result<Uuid>;

    /// Inserts the node with empty props if it is missing, and leaves an existing one as it
    /// is. Returns its id. Writers that share a node create it with this, then
    /// [`GraphStore::update_props`], so a create never replaces what another wrote.
    async fn ensure_node(&self, kind: NodeKind, key: &str) -> Result<Uuid>;

    /// Applies `patch` to the node's props atomically, on the props as stored (in one
    /// transaction with the node locked, or as a compare-and-set that applies it again when
    /// another writer came first). Keys the patch does not name stay as they are, so two
    /// writers changing different keys never lose each other's writes (issue #51). Returns
    /// the node as written, `None` when there is no node `id`.
    async fn update_props(&self, id: Uuid, patch: &PropsPatch) -> Result<Option<Node>>;

    /// [`GraphStore::update_props`] onto a live company only (issue #62): when the node is a
    /// husk (merged away, with `merged_into`) nothing is written and the call fails with
    /// [`crate::Error::MergedAway`]. The check and the write are atomic and ordered after any
    /// merge of the node, so a merge never commits between them.
    async fn update_live_props(&self, id: Uuid, patch: &PropsPatch) -> Result<Option<Node>>;

    /// A node by id.
    async fn node(&self, id: Uuid) -> Result<Option<Node>>;

    /// A node by kind and key.
    async fn node_by_key(&self, kind: NodeKind, key: &str) -> Result<Option<Node>>;

    /// The ids of the nodes of `kind` with `keys`, in the same order; `None` for a missing
    /// one. A backend may read them in one statement (issue #126). At most
    /// [`crate::GRAPH_BATCH_MAX`] per call.
    async fn node_ids(&self, kind: NodeKind, keys: &[String]) -> Result<Vec<Option<Uuid>>> {
        crate::validate_batch_count(keys.len())?;
        let mut out = Vec::with_capacity(keys.len());
        for key in keys {
            out.push(self.node_by_key(kind, key).await?.map(|n| n.id));
        }
        Ok(out)
    }

    /// Inserts the edge, or updates the edge with the same `(src, dst, kind, source,
    /// valid_from)`. Returns its id. Both nodes must exist.
    async fn upsert_edge(&self, edge: &Edge) -> Result<Uuid>;

    /// [`GraphStore::upsert_edge`] of each edge, in order, as if one call followed the other;
    /// the ids in the same order. An invalid edge fails the call before anything is written.
    /// A backend may write them in one transaction (Postgres: one statement, issues #126 and
    /// #148), so an error may leave none of them written. Writers that can race on the
    /// same edges keep them in one order. At most [`crate::GRAPH_BATCH_MAX`] per call.
    async fn upsert_edges(&self, edges: &[Edge]) -> Result<Vec<Uuid>> {
        crate::validate_batch_count(edges.len())?;
        for e in edges {
            e.validate()?;
        }
        let mut out = Vec::with_capacity(edges.len());
        for e in edges {
            out.push(self.upsert_edge(e).await?);
        }
        Ok(out)
    }

    /// [`GraphStore::upsert_edge`] onto live companies (issue #48): an end that is a
    /// merged-away company (a husk with `merged_into`, ADR 0002 section 1.8) is replaced by
    /// the company it was merged into, following chains of at most
    /// [`crate::MERGE_CHAIN_MAX`] hops. The ends are checked and the edge written atomically,
    /// ordered after any merge of those companies, so a merge that commits between a
    /// writer's alias lookup and this write never leaves the edge on the husk: the edge
    /// either lands on the survivor, or is written first and moved by the merge. A longer
    /// chain fails with [`crate::Error::Precondition`].
    async fn upsert_live_edge(&self, edge: &Edge) -> Result<LiveEdge>;

    /// The edges at a node and the nodes at their other ends, ordered by edge kind (its name),
    /// then `valid_from` (undated first), then edge id ([`crate::EdgeCursor`]).
    async fn neighbours(&self, node: Uuid, query: &NeighbourQuery) -> Result<Vec<Neighbour>>;

    /// A page of [`GraphStore::neighbours`] (issue #112): the neighbours after `after` in that
    /// order, at most `limit` of them (0 means 50, at most [`crate::RECORDS_MAX_LIMIT`]),
    /// ending early with the neighbour that reaches `max_bytes`
    /// ([`crate::neighbour_page_bytes`]). The page starts at `after` in the edge indexes; it
    /// never reads the neighbours before it.
    async fn neighbours_page(
        &self,
        node: Uuid,
        query: &NeighbourQuery,
        after: Option<&EdgeCursor>,
        limit: u32,
        max_bytes: usize,
    ) -> Result<Vec<Neighbour>>;

    /// The documents with a live `party` edge to the company, each once, in the order of
    /// their first such edge: by its `valid_from` (undated first), then its id
    /// ([`crate::EdgeCursor`]; issue #112).
    async fn company_documents(&self, company: Uuid) -> Result<Vec<Node>>;

    /// A page of [`GraphStore::company_documents`]: the documents whose first live `party`
    /// edge comes after `after`, at most `limit` (as for [`GraphStore::neighbours_page`]),
    /// ending early with the one that reaches `max_bytes` ([`crate::node_page_bytes`]). The
    /// page starts at `after` in the company's party edges.
    async fn company_documents_page(
        &self,
        company: Uuid,
        after: Option<&EdgeCursor>,
        limit: u32,
        max_bytes: usize,
    ) -> Result<Vec<PartyDocument>>;

    /// The first `limit` of [`GraphStore::company_documents`], in the same order (issue #75:
    /// internal callers that read a bounded sample).
    async fn company_documents_first(&self, company: Uuid, limit: u32) -> Result<Vec<Node>>;

    /// How many documents [`GraphStore::company_documents`] returns, counted without loading
    /// them (issue #75).
    async fn company_document_count(&self, company: Uuid) -> Result<u64>;

    /// Records that a document lives at a location, in one transaction: upserts the Document
    /// and Location nodes and the `located_at` edge, and supersedes any `located_at` edge from
    /// another document to that location (the object was overwritten). The location is
    /// written whether or not the record is stored; [`Located::record`] says whether it was,
    /// read under a share lock on the record so that a purge either sees the location or has
    /// deleted the record first (issue #168).
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

    /// [`GraphStore::claim_alias`] for a live company only (issue #62): when `node` is a husk
    /// (merged away, with `merged_into`) nothing is claimed and the call fails with
    /// [`crate::Error::MergedAway`]. The check and the claim are atomic and ordered after any
    /// merge of the node: a claim made before a merge is moved by it, none lands after.
    async fn claim_live_alias(
        &self,
        kind: NodeKind,
        alias: &str,
        node: Uuid,
        props: &Value,
    ) -> Result<NodeAlias>;

    /// [`GraphStore::claim_alias`] of each `(alias, props)` for `node`, in order, as if one
    /// call followed the other: the results are in the same order, and an alias given twice
    /// is claimed by its first entry. Each claim is its own write, not one transaction. A
    /// backend may send them together (issue #103: Postgres pipelines them on one
    /// connection, one round trip).
    async fn claim_aliases(
        &self,
        kind: NodeKind,
        node: Uuid,
        claims: &[(String, Value)],
    ) -> Result<Vec<NodeAlias>> {
        crate::validate_alias_count(claims.len())?;
        let mut out = Vec::with_capacity(claims.len());
        for (alias, props) in claims {
            out.push(self.claim_alias(kind, alias, node, props).await?);
        }
        Ok(out)
    }

    /// The alias, if claimed or blocked.
    async fn alias(&self, kind: NodeKind, alias: &str) -> Result<Option<NodeAlias>>;

    /// [`GraphStore::alias`] of each of `aliases`, in the same order. A backend may read them
    /// in one statement (issue #103). At most [`crate::ALIASES_MAX`] per call.
    async fn aliases(&self, kind: NodeKind, aliases: &[String]) -> Result<Vec<Option<NodeAlias>>> {
        crate::validate_alias_count(aliases.len())?;
        let mut out = Vec::with_capacity(aliases.len());
        for alias in aliases {
            out.push(self.alias(kind, alias).await?);
        }
        Ok(out)
    }

    /// Blocks an alias: it resolves to no node from now on (a key that turned out to belong
    /// to more than one), with `props` saying why.
    async fn block_alias(&self, kind: NodeKind, alias: &str, props: &Value) -> Result<()>;

    /// The aliases `node` owns, ordered by kind, then alias.
    async fn aliases_of(&self, node: Uuid) -> Result<Vec<OwnedAlias>>;

    /// [`GraphStore::neighbours_page`] of each of `nodes`, in that order: the first
    /// `per_node` neighbours of each (at most [`crate::RECORDS_MAX_LIMIT`]), each with the
    /// node it was read for, ending early with the neighbour that brings them all to
    /// `max_bytes` ([`crate::neighbour_page_bytes`]). A backend may read them in one
    /// statement (Postgres; issue #172).
    async fn neighbours_of_many(
        &self,
        nodes: &[Uuid],
        query: &NeighbourQuery,
        per_node: u32,
        max_bytes: usize,
    ) -> Result<Vec<(Uuid, Neighbour)>> {
        let mut out = Vec::new();
        let mut bytes = 0usize;
        for &node in nodes {
            if per_node == 0 || bytes >= max_bytes {
                break;
            }
            let page = self
                .neighbours_page(node, query, None, per_node, max_bytes - bytes)
                .await?;
            for n in page {
                bytes += crate::neighbour_page_bytes(&n);
                out.push((node, n));
            }
        }
        Ok(out)
    }

    /// Folds the company graph's change log into its counts (issue #172, see
    /// [`crate::explore`]): at most `max_rows` log rows and every other log row of their
    /// nodes, in one transaction, one fold at a time across instances.
    async fn fold_company_graph(&self, max_rows: u32) -> Result<Folded>;

    /// The documents with at least one live `party` edge, each once however many companies
    /// it names, as last folded (issue #181): one stored row, no scan.
    async fn company_documents_total(&self) -> Result<u64>;

    /// A page of the live companies (not merged away) that match `filter`, in `order`, after
    /// `after` (a cursor of the same order), with their counts as last folded: at most
    /// `limit` (at most [`crate::COMPANY_GRAPH_NODES_MAX`] + 1), ending early with the one
    /// that reaches `max_bytes` ([`crate::company_row_bytes`]). The page starts at `after` in
    /// the order's index; filters are checked on the companies in order.
    async fn list_companies(
        &self,
        filter: &CompanyFilter,
        order: CompanyOrder,
        after: Option<&CompanyCursor>,
        limit: u32,
        max_bytes: usize,
    ) -> Result<Vec<CompanyRow>>;

    /// The pairs among `companies` (at most [`crate::COMPANY_GRAPH_NODES_MAX`]) that share at
    /// least `min_documents` documents or have a corporate-event edge, as last folded: the
    /// most shared documents first, then the most events, then by `a` and `b`; at most
    /// `limit` (at most [`crate::COMPANY_GRAPH_LINKS_MAX`] + 1).
    async fn company_links(
        &self,
        companies: &[Uuid],
        min_documents: u32,
        limit: u32,
    ) -> Result<Vec<CompanyLink>>;
}

/// Reversible company merges (ADR 0002 section 1.8): the merge log, undo and split, and
/// permanent dismissals. Each write is one transaction; see [`crate::merge`] for the rules.
#[async_trait]
pub trait MergeStore: Send + Sync {
    /// Merges `absorbed` into `survivor` and logs it, atomically. Fails with
    /// [`crate::Error::Precondition`] when either company was merged away already or the
    /// pair (or a company merged into either) is dismissed.
    async fn merge_companies(&self, req: &MergeRequest) -> Result<MergeEntry>;

    /// Splits a company in two and logs it, atomically. The entry's `absorbed` is the new
    /// company.
    async fn split_company(&self, req: &SplitRequest) -> Result<MergeEntry>;

    /// Undoes a merge or split from its undo payload. Later live entries involving the same
    /// companies make it fail with [`crate::Error::Precondition`] listing them, unless
    /// `cascade` is set: then they are undone first, newest first.
    async fn undo_merge(&self, req: &UndoRequest) -> Result<UndoOutcome>;

    /// A log entry.
    async fn merge_entry(&self, id: Uuid) -> Result<Option<MergeEntry>>;

    /// A page of the log, newest first.
    async fn list_merges(&self, query: &MergeQuery) -> Result<Vec<MergeEntry>>;

    /// Records a permanent "not the same" pair (both directions). Dismissing a pair again
    /// returns the first dismissal. Two companies merged now are refused: undo the merge with
    /// `dismiss` instead.
    async fn dismiss_pair(&self, req: &DismissRequest) -> Result<Dismissal>;

    /// The dismissal that keeps `a` and `b` apart, if any: between them, or between companies
    /// merged into them. The enrichment gate asks this before every merge.
    async fn dismissal(&self, a: Uuid, b: Uuid) -> Result<Option<Dismissal>>;

    /// A page of dismissals, newest first.
    async fn list_dismissals(&self, query: &DismissalQuery) -> Result<Vec<Dismissal>>;
}

/// Enrichment proposals (ADR 0002 section 1.6, issue #52): what the decision gate leaves to
/// a person. See [`crate::proposal`] for the rules.
#[async_trait]
pub trait ProposalStore: Send + Sync {
    /// Records a proposal: a new open one, or the open one of the same kind, company and
    /// candidate updated (evidence, probability, reason, detail, `updated_at`). `None` when it
    /// is never to be opened: the same proposal was rejected as dismissed, or (a merge) the
    /// pair is dismissed ([`MergeStore::dismissal`]). Fails with [`crate::Error::NotFound`]
    /// when the company (or a merge's other company) does not exist.
    async fn propose(&self, req: &ProposalRequest) -> Result<Option<Proposal>>;

    /// A proposal by id.
    async fn proposal(&self, id: Uuid) -> Result<Option<Proposal>>;

    /// A page of proposals, newest first.
    async fn list_proposals(&self, query: &ProposalQuery) -> Result<Vec<Proposal>>;

    /// Accepts or rejects an open proposal. Fails with [`crate::Error::NotFound`] when there
    /// is none, and with [`crate::Error::Precondition`] when it is not open any more.
    async fn resolve_proposal(&self, req: &ResolveRequest) -> Result<Proposal>;
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

    /// [`SketchStore::sketch_candidates`] over only the bands whose bucket holds at most
    /// `max_bucket` documents (`exclude` included, once its sketch is stored): the LSH "stop
    /// band" of issue #148. A larger bucket is a template's boilerplate, not similarity, and
    /// reading it would cost a row per document in it. The bucket sizes are kept with the
    /// bands, so the check is one row per band. Also returns how many distinct band keys were
    /// skipped.
    async fn sketch_candidates_within(
        &self,
        bands: &[u64],
        exclude: &Blake3,
        limit: u32,
        max_bucket: u32,
    ) -> Result<Candidates>;
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

    /// The claims on other sources of the same endpoint and bucket whose prefix contains
    /// `key`'s or is contained in it (so their objects overlap), at most 100.
    async fn overlapping_claims(&self, key: &SourceKey) -> Result<Vec<SourceClaim>>;

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

/// The registry response cache and the favicon cache (ADR 0002 sections 2 and 5), shared by
/// every instance in shared mode. Rows past their `expires_at` are never returned.
#[async_trait]
pub trait CacheStore: Send + Sync {
    /// The unexpired answer cached under `registry` and `key`.
    async fn cache_get(&self, registry: &str, key: &str) -> Result<Option<CacheRow>>;

    /// Stores the answer, replacing any under the same registry and key.
    async fn cache_put(&self, row: &CacheRow) -> Result<()>;

    /// The unexpired icon lookup of `domain`.
    async fn icon_get(&self, domain: &str) -> Result<Option<IconRow>>;

    /// Stores the icon lookup, replacing any of the same domain.
    async fn icon_put(&self, row: &IconRow) -> Result<()>;

    /// Deletes the expired rows of both caches. Returns how many.
    async fn purge_expired(&self) -> Result<u64>;

    /// Deletes the cached answers of `registry` whose key starts with `key_prefix` (a valid,
    /// non-empty cache key), so the next lookup asks the registry again: a delta feed said the
    /// record changed (issue #49). Returns how many.
    async fn cache_invalidate(&self, registry: &str, key_prefix: &str) -> Result<u64>;
}

/// Company freshness (ADR 0002 section 6): which companies are due for enrichment. The lease
/// that keeps two instances from enriching one company at once is
/// [`ClaimStore::lease_company`].
#[async_trait]
pub trait FreshnessStore: Send + Sync {
    /// Records that `company` was enriched now: its current document count, and for each
    /// registry in `schedule` the time it is due again (now plus the interval). Registries not
    /// in `schedule` keep their due time. Fails with [`crate::Error::NotFound`] when there is
    /// no such company node, and with [`crate::Error::MergedAway`] when it was merged away (a
    /// husk is never recorded as enriched; checked atomically with the write, issue #62).
    async fn record_enrichment(
        &self,
        company: Uuid,
        schedule: &[(String, Duration)],
    ) -> Result<Enrichment>;

    /// Makes each registry in `registries` due now for `company`, so the next pass enriches
    /// it (a registry due sooner keeps its time). Its last enrichment and document count stay
    /// as they are. Fails like [`FreshnessStore::record_enrichment`]: with
    /// [`crate::Error::NotFound`] when there is no such company node, with
    /// [`crate::Error::MergedAway`] when it was merged away.
    async fn mark_due(&self, company: Uuid, registries: &[String]) -> Result<()>;

    /// The company's freshness, if it was ever enriched.
    async fn enrichment(&self, company: Uuid) -> Result<Option<Enrichment>>;

    /// One page of the companies due now, ordered by id, starting after `after`: companies
    /// with at least one document (a live `party` edge) that were never enriched, whose
    /// document count is at least twice the count when they were enriched (at least 2), or
    /// with a registry due. Companies another instance than `owner` holds an unexpired lease
    /// on are left out. At most `limit` (0 means 100, at most [`crate::DUE_MAX_LIMIT`]).
    async fn companies_due(
        &self,
        owner: &InstanceId,
        after: Option<Uuid>,
        limit: u32,
    ) -> Result<Vec<DueCompany>>;

    /// Makes `registry` due now for each of `companies` that was enriched before (one never
    /// enriched is due anyway, as new): a delta feed said its record changed (issue #49).
    /// Other registries keep their due time. At most [`crate::FEED_BATCH_MAX`] companies;
    /// unknown ids are skipped. Returns how many were made due.
    async fn mark_many_due(&self, companies: &[Uuid], registry: &str) -> Result<u64>;
}

/// Registry delta feeds (ADR 0002 section 6, issue #49): one instance per cluster pulls each
/// feed, under a lease on the feed's row, and keeps the feed's cursor there for the next run
/// (on this instance or another).
#[async_trait]
pub trait FeedStore: Send + Sync {
    /// Takes or renews the lease on `feed` for `ttl`. Returns `false` when another instance
    /// holds an unexpired lease.
    async fn lease_feed(&self, feed: &str, owner: &InstanceId, ttl: Duration) -> Result<bool>;

    /// Gives the lease back. Returns whether `owner` held it.
    async fn release_feed(&self, feed: &str, owner: &InstanceId) -> Result<bool>;

    /// The feed's cursor, next run and lease holder; `None` before it was first leased.
    async fn feed_state(&self, feed: &str) -> Result<Option<FeedState>>;

    /// Stores the feed's cursor and its next run (Unix seconds), only while `owner` holds an
    /// unexpired lease on it. Returns `false` (and writes nothing) otherwise.
    async fn save_feed(
        &self,
        feed: &str,
        owner: &InstanceId,
        cursor: &str,
        next_run_at: i64,
    ) -> Result<bool>;

    /// The companies owning `aliases` (company aliases such as `id:lei:<LEI>`), as
    /// `(alias, company)`; aliases nobody owns are left out. At most
    /// [`crate::FEED_BATCH_MAX`] aliases.
    async fn companies_by_alias(&self, aliases: &[String]) -> Result<Vec<(String, Uuid)>>;

    /// One page of the owned company aliases that start with `prefix`, in byte order after
    /// `after`, as `(alias, company)`. At most `limit` (at most [`crate::FEED_BATCH_MAX`]).
    async fn company_aliases(
        &self,
        prefix: &str,
        after: Option<&str>,
        limit: u32,
    ) -> Result<Vec<(String, Uuid)>>;
}

/// Purges (issue #168; see [`crate::purge`]): deleting the records, search rows and graph
/// nodes no live location holds. Each call is one bounded batch in one transaction, safe to
/// repeat, to stop between, and to run on several instances at once.
#[async_trait]
pub trait PurgeStore: Send + Sync {
    /// Removes up to `limit` ([`crate::purge_limit`]) live locations under `source` (the
    /// Location node, its row and its `located_at` edges), then purges the documents that no
    /// live location holds any more. `next` is `Some` while live locations may remain.
    async fn purge_source_locations(&self, source: &SourceKey, limit: u32) -> Result<Purged>;

    /// Examines up to `limit` Location nodes after `after` (a previous batch's `next`) in key
    /// order, under `source` when it is given, and deletes those without a live location,
    /// then purges the documents they leave without one.
    async fn purge_ended_locations(
        &self,
        source: Option<&SourceKey>,
        after: Option<&str>,
        limit: u32,
    ) -> Result<Purged>;

    /// Examines up to `limit` records after `after` (a previous batch's `next`) in hash
    /// order, and purges those no live location holds.
    async fn purge_orphan_records(&self, after: Option<&str>, limit: u32) -> Result<Purged>;
}

/// A complete backend: everything the service needs from its store.
#[async_trait]
pub trait Backend:
    RecordStore
    + GraphStore
    + ClaimStore
    + SearchStore
    + SketchStore
    + CacheStore
    + FreshnessStore
    + FeedStore
    + MergeStore
    + ProposalStore
    + PurgeStore
{
    /// `postgres` or `sqlite`.
    fn name(&self) -> &'static str;

    /// Checks the backend answers.
    async fn ping(&self) -> Result<()>;
}
