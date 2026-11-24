//! doewood's shared data model (ADR 0001 sections 2 and 7, ADR 0002 sections 3 and 6): the
//! records, the typed graph, source claims, company leases and search, and the store traits
//! that both backends implement.
//!
//! - **Shared mode:** `doewood-pg`, one Postgres database for every instance.
//! - **Standalone mode:** `doewood-store`, the instance's own SQLite file.
//!
//! The `conformance` feature exports the test suite every backend must pass.

#[cfg(feature = "conformance")]
pub mod conformance;
mod error;
mod traits;
mod types;

pub use error::{Error, Result};
pub use traits::{
    Backend, ClaimStore, GraphStore, RecordStore, SearchStore, SketchStore, sketch_limit,
};
pub use types::{
    Blake3, Direction, Edge, EdgeKind, EdgeSource, EdgeState, Located, LocationEnded,
    NODE_KEY_MAX_LEN, Neighbour, NeighbourQuery, Node, NodeAlias, NodeKind, ObjectLocation,
    PENDING_REF_PREFIX, PutOutcome, RECORDS_MAX_LIMIT, Record, SEARCH_MAX_LIMIT, SKETCH_MAX_BANDS,
    SKETCH_MAX_SIGNATURE, SearchDoc, SearchHit, SearchQuery, Sketch, SourceClaim, SourceKey,
    StepVersions, StoredEdge, band_to_i64, records_limit, validate_alias, validate_node,
    validate_sketch, versions_from_json, versions_to_json,
};
