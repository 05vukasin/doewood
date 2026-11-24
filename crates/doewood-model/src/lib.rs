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
pub use traits::{Backend, ClaimStore, GraphStore, RecordStore, SearchStore};
pub use types::{
    Blake3, Direction, Edge, EdgeKind, EdgeSource, EdgeState, Located, NODE_KEY_MAX_LEN, Neighbour,
    NeighbourQuery, Node, NodeKind, ObjectLocation, PutOutcome, RECORDS_MAX_LIMIT, Record,
    SEARCH_MAX_LIMIT, SearchDoc, SearchHit, SearchQuery, SourceClaim, SourceKey, StepVersions,
    StoredEdge, records_limit, validate_node, versions_from_json, versions_to_json,
};
