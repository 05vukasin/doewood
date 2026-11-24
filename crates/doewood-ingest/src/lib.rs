//! doewood's ingest (step 4 of ADR 0001): scanner events → fetch → analyze → store.
//!
//! - [`route`]: quail for documents, hedgehog for spreadsheets, from the bytes.
//! - [`analyze`]: one quail engine per instance (OCR behind the `ocr` feature), hedgehog
//!   under `catch_unwind`, the record and its step versions.
//! - [`facts`]: what the graph and search need, read from the record.
//! - [`company`]: the deterministic company identity rules of ADR 0002 section 1, from
//!   `doewood-enrich`.
//! - [`graph`]: documents, locations, companies, references, `similar_to` and search.
//! - [`llm`]: the LLM queue (per-source policy, concurrency cap, daily budget) and
//!   on-demand `Summarize` (ADR 0001, section 6).

pub mod analyze;
pub mod company;
pub mod facts;
pub mod graph;
pub mod llm;
pub mod pipeline;
pub mod route;
pub mod sheet;

pub use analyze::{Analysis, Analyzer, AnalyzerConfig, OcrState, Outcome};
pub use graph::{GraphWriter, SimilarityConfig, WriteStats};
pub use llm::{
    Clock, OnDemandSummary, QueueConfig, Summaries, SummariesParts, SummarizeError, SummarizeInput,
    SystemClock,
};
pub use pipeline::{
    Fetch, Ingest, IngestConfig, IngestError, IngestHandle, IngestParts, ReprocessState,
    ReprocessStatus, S3Fetch, Stats, default_jobs,
};
pub use route::{Route, SheetFormat};
