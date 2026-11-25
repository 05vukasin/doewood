//! doewood's shared data model (ADR 0001 sections 2 and 7, ADR 0002 sections 3 and 6): the
//! records, the typed graph, source claims, company leases, search, the enrichment job's
//! registry and favicon caches and company freshness, the merge log and dismissals (ADR 0002
//! section 1.8), and the store traits
//! that both backends implement.
//!
//! - **Shared mode:** `doewood-pg`, one Postgres database for every instance.
//! - **Standalone mode:** `doewood-store`, the instance's own SQLite file.
//!
//! The `conformance` feature exports the test suite every backend must pass.

#[cfg(feature = "conformance")]
pub mod conformance;
mod enrichment;
mod error;
pub mod merge;
mod traits;
mod types;

pub use enrichment::{
    CACHE_KEY_MAX_LEN, CACHE_MAX_BODY, CacheRow, DUE_MAX_LIMIT, DueCompany, DueReason, Enrichment,
    ICON_DOMAIN_MAX_LEN, ICON_FORMATS, ICON_MAX_BYTES, IconRow, MAX_REFRESH_INTERVAL, MAX_TIME,
    REGISTRY_NAME_MAX_LEN, StoredIcon, due_limit, validate_cache_key, validate_registry,
    validate_schedule,
};
pub use error::{Error, Result};
pub use merge::{
    DismissRequest, Dismissal, DismissalQuery, EVIDENCE_MAX_BYTES, MERGES_MAX_LIMIT, MergeActor,
    MergeEntry, MergeQuery, MergeRequest, MergeTx, OwnedAlias, REASON_MAX_LEN, SPLIT_MAX_ITEMS,
    SplitRequest, UndoOutcome, UndoRequest, Undone, merged_into, merges_limit,
};
pub use traits::{
    Backend, CacheStore, ClaimStore, FreshnessStore, GraphStore, MergeStore, RecordStore,
    SearchStore, SketchStore, sketch_limit,
};
pub use types::{
    Blake3, Direction, Edge, EdgeKind, EdgeSource, EdgeState, Located, LocationEnded,
    MergeActorKind, MergeOp, NODE_KEY_MAX_LEN, Neighbour, NeighbourQuery, Node, NodeAlias,
    NodeKind, ObjectLocation, PENDING_REF_PREFIX, PutOutcome, RECORDS_MAX_LIMIT, Record,
    SEARCH_MAX_LIMIT, SKETCH_MAX_BANDS, SKETCH_MAX_SIGNATURE, SearchDoc, SearchHit, SearchQuery,
    Sketch, SourceClaim, SourceKey, StepVersions, StoredEdge, band_to_i64, records_limit,
    validate_alias, validate_node, validate_sketch, versions_from_json, versions_to_json,
};
