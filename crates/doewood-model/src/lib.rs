//! doewood's shared data model (ADR 0001 sections 2 and 7, ADR 0002 sections 3 and 6): the
//! records, the typed graph, source claims, company leases, search, and the enrichment job's
//! registry and favicon caches and company freshness, and the store traits
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
mod traits;
mod types;

pub use enrichment::{
    CACHE_KEY_MAX_LEN, CACHE_MAX_BODY, CacheRow, DUE_MAX_LIMIT, DueCompany, DueReason, Enrichment,
    ICON_DOMAIN_MAX_LEN, ICON_FORMATS, ICON_MAX_BYTES, IconRow, MAX_REFRESH_INTERVAL, MAX_TIME,
    REGISTRY_NAME_MAX_LEN, StoredIcon, due_limit, validate_cache_key, validate_registry,
    validate_schedule,
};
pub use error::{Error, Result};
pub use traits::{
    Backend, CacheStore, ClaimStore, FreshnessStore, GraphStore, RecordStore, SearchStore,
    SketchStore, sketch_limit,
};
pub use types::{
    Blake3, Direction, Edge, EdgeKind, EdgeSource, EdgeState, Located, LocationEnded,
    NODE_KEY_MAX_LEN, Neighbour, NeighbourQuery, Node, NodeAlias, NodeKind, ObjectLocation,
    PENDING_REF_PREFIX, PutOutcome, RECORDS_MAX_LIMIT, Record, SEARCH_MAX_LIMIT, SKETCH_MAX_BANDS,
    SKETCH_MAX_SIGNATURE, SearchDoc, SearchHit, SearchQuery, Sketch, SourceClaim, SourceKey,
    StepVersions, StoredEdge, band_to_i64, records_limit, validate_alias, validate_node,
    validate_sketch, versions_from_json, versions_to_json,
};
