//! One file's bytes to one record: routing, quail, hedgehog, and the step versions.
//!
//! One [`Analyzer`] per instance holds one quail engine, built like the `quail` CLI builds
//! its own (`quail_cli::registry::builder`), shared by every ingest worker.
//!
//! - Documents (PDF, DOCX, text, CSV) get quail's record as is.
//! - Spreadsheets get quail's identity layer (hashes, sniffed format) plus hedgehog's `sheet.*`
//!   facts, with the spreadsheet format as the record's `format`: the same top-level shape.
//! - Documents also get `summary.digest` (quail's bounded summary input), which the LLM queue
//!   reads ([`crate::llm`]).
//! - Anything without an extractor (an image, an archive, an unknown file) keeps the record of
//!   quail's generic layer: identity and format only.
//!
//! With the `ocr` feature and the model files present, the engine runs under
//! `Profile::Deep`, so pages that need it are OCR'd (budgets as in quail's CLI). Without
//! either it runs under `Profile::Standard`, and scanned pages are only flagged `needs_ocr`.

use std::path::{Path, PathBuf};
use std::time::Duration;

use doewood_model::{Blake3, StepVersions};
use quail_core::{Blob, FileRecord, Format, Limits, Profile, Status};
use quail_engine::Engine;
use serde_json::Value as Json;
use tracing::info;

use crate::route::{Route, route};
use crate::sheet;

/// Version of doewood's own conversion (records, graph, search). Part of every record's step
/// versions, so changing it makes `ReprocessJob` redo every file.
pub const INGEST_VERSION: &str = "1";

/// Step-version keys that decide whether a stored record is current. The other keys (one per
/// quail step) are informative.
pub const VERSION_INGEST: &str = "doewood.ingest";
/// The quail plan: profile and every planned step with its version.
pub const VERSION_PLAN: &str = "quail.plan";
/// The hedgehog revision, on spreadsheet records.
pub const VERSION_HEDGEHOG: &str = "hedgehog";

/// How the analyzer is built.
#[derive(Clone, Debug)]
pub struct AnalyzerConfig {
    /// Wall time per file (`Limits::timeout`); OCR may extend it within its budget.
    pub file_timeout: Duration,
    /// Largest file analyzed (`Limits::max_bytes`); bigger ones get identity only.
    pub max_bytes: u64,
    /// File workers that share the engine (sizes the OCR permits).
    pub workers: usize,
    /// Where the OCR models are.
    pub ocr_models: PathBuf,
}

impl Default for AnalyzerConfig {
    fn default() -> Self {
        Self {
            file_timeout: Limits::default().timeout,
            max_bytes: Limits::default().max_bytes,
            workers: 1,
            ocr_models: PathBuf::from("ocr-models"),
        }
    }
}

/// Whether OCR runs.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum OcrState {
    /// Pages that need it are OCR'd, `jobs` at a time with `threads` each.
    Ready {
        /// Pages read at once.
        jobs: usize,
        /// ONNX Runtime threads per engine.
        threads: usize,
    },
    /// OCR is built in but cannot run (no models); the reason.
    Unavailable(String),
    /// Built without the `ocr` feature.
    NotBuilt,
}

/// What ingest made of one file.
#[derive(Clone, Debug)]
pub struct Analysis {
    /// The content's hash.
    pub blake3: Blake3,
    /// Where it went.
    pub route: Route,
    /// The record, as stored.
    pub body: Json,
    /// The step versions, as stored.
    pub versions: StepVersions,
    /// How it ended.
    pub outcome: Outcome,
}

/// How analysis ended.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Outcome {
    /// Analyzed (possibly partially).
    Analyzed,
    /// No extractor for the format: identity and format only.
    Unsupported,
    /// The extractor (or hedgehog's reader) failed; the record keeps what was found.
    Failed(String),
}

/// Builds and holds the engine.
pub struct Analyzer {
    engine: Engine,
    plan: String,
    ocr: OcrState,
    limits: Limits,
}

impl std::fmt::Debug for Analyzer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Analyzer")
            .field("plan", &self.plan)
            .field("ocr", &self.ocr)
            .finish_non_exhaustive()
    }
}

impl Analyzer {
    /// Builds the engine. Missing OCR models are a warning (logged once, here), never an error.
    pub fn new(config: &AnalyzerConfig) -> Result<Self, String> {
        let mut limits = Limits {
            max_bytes: config.max_bytes,
            timeout: config.file_timeout,
            ..Limits::default()
        };
        let ocr = Ocr::setup(&config.ocr_models, config.workers, &mut limits);
        let profile = if ocr.analyzer_ready() {
            Profile::Deep
        } else {
            Profile::Standard
        };
        // `summary.digest`: the bounded summary input, kept in the record so the LLM queue
        // (and `Summarize`) work from the record alone, with what `summary.llm` would read.
        let digest = quail_summary::SummaryDigest::with_config(
            quail_summary::OutlineConfig::default(),
            quail_summary::DigestConfig {
                max_tokens: quail_summary::LlmConfig::default().digest_tokens,
                card_tokens: quail_summary::LlmConfig::default().card_tokens,
                ..quail_summary::DigestConfig::default()
            },
            0,
        );
        let builder =
            quail_cli::registry::builder(profile, limits.clone()).analyzer(Box::new(digest));
        let (builder, ocr) = ocr.register(builder);
        let engine = builder.build().map_err(|e| format!("quail engine: {e}"))?;
        let plan = plan_fingerprint(&engine);
        info!(
            plan = %plan,
            ocr = ?ocr,
            steps = engine.plan().count(),
            "analyzer ready"
        );
        Ok(Self {
            engine,
            plan,
            ocr,
            limits,
        })
    }

    /// Whether OCR runs.
    pub fn ocr(&self) -> &OcrState {
        &self.ocr
    }

    /// The per-file limits, OCR's extension included.
    pub fn limits(&self) -> &Limits {
        &self.limits
    }

    /// The versions a record must carry to be current, for a file of this route.
    pub fn current_versions(&self, sheet: bool) -> StepVersions {
        let mut v = StepVersions::from([
            (VERSION_INGEST.to_owned(), INGEST_VERSION.to_owned()),
            (VERSION_PLAN.to_owned(), self.plan.clone()),
        ]);
        if sheet {
            v.insert(VERSION_HEDGEHOG.to_owned(), sheet::HEDGEHOG_REV.to_owned());
        }
        v
    }

    /// Whether a stored record's versions are behind this analyzer's.
    pub fn is_stale(&self, stored: &StepVersions) -> bool {
        let sheet = stored.contains_key(VERSION_HEDGEHOG);
        self.current_versions(sheet)
            .iter()
            .any(|(k, v)| stored.get(k) != Some(v))
    }

    /// Analyzes one file. CPU-bound: call it from a blocking thread. `key` is the object key
    /// (its extension is a hint for spreadsheet containers only).
    pub fn analyze(&self, bytes: Vec<u8>, key: &str) -> Analysis {
        let blake3 = Blake3(*blake3::hash(&bytes).as_bytes());
        let sniff = quail_engine::detect(&bytes);
        let route = route(sniff, &bytes, key);
        let blob = Blob::from(bytes);
        // The locator is the content's own name: records are shared by every copy, and their
        // locations live in the graph.
        let record = self
            .engine
            .process_blob(format!("blake3:{}", blake3.to_hex()), &blob);
        let (body, versions, outcome) = match route {
            Route::Sheet(format) => self.sheet_record(record, blob.as_bytes(), key, format),
            Route::Quail(format) => {
                let outcome = match &record.status {
                    Status::Failed { error } => Outcome::Failed(error.clone()),
                    _ if self.engine.extractor_for(format).is_none()
                        || format == Format::Unknown =>
                    {
                        Outcome::Unsupported
                    }
                    _ => Outcome::Analyzed,
                };
                let versions = self.versions(&record, false);
                (to_json(&record), versions, outcome)
            }
        };
        Analysis {
            blake3,
            route,
            body,
            versions,
            outcome,
        }
    }

    fn sheet_record(
        &self,
        mut record: FileRecord,
        bytes: &[u8],
        key: &str,
        format: crate::route::SheetFormat,
    ) -> (Json, StepVersions, Outcome) {
        let name = key.rsplit('/').next().unwrap_or(key);
        let out = sheet::analyze(bytes, name, format.as_str(), self.limits.timeout);
        record.facts.extend(out.facts);
        record.steps.extend(out.steps);
        let mut problems = out.problems;
        let outcome = match out.failed {
            Some(e) => {
                problems.insert(0, e.clone());
                Outcome::Failed(e)
            }
            None => Outcome::Analyzed,
        };
        if !problems.is_empty() {
            record.status = match (&outcome, record.status) {
                (Outcome::Failed(e), _) => Status::Failed { error: e.clone() },
                (_, Status::Partial { problems: mut p }) => {
                    p.extend(problems);
                    Status::Partial { problems: p }
                }
                (_, Status::Failed { error }) => Status::Failed { error },
                (_, Status::Ok) => Status::Partial { problems },
            };
        } else if matches!(record.status, Status::Failed { .. }) {
            // quail has no extractor for a ZIP or OLE container: not hedgehog's failure.
            record.status = Status::Ok;
        }
        let versions = self.versions(&record, true);
        let mut body = to_json(&record);
        body["format"] = Json::String(format.as_str().to_owned());
        (body, versions, outcome)
    }

    fn versions(&self, record: &FileRecord, sheet: bool) -> StepVersions {
        let mut v = self.current_versions(sheet);
        for s in &record.steps {
            v.entry(s.name.to_owned())
                .or_insert_with(|| s.version.to_string());
        }
        v
    }
}

fn to_json(record: &FileRecord) -> Json {
    // A FileRecord always serializes (no maps with non-string keys, no failing types).
    serde_json::to_value(record).unwrap_or(Json::Null)
}

/// `<profile>:<16 hex>`, a digest of every planned step and its version.
fn plan_fingerprint(engine: &Engine) -> String {
    let mut ids: Vec<String> = engine.plan().map(|id| id.to_string()).collect();
    ids.push(quail_engine::SNIFF_ID.to_string());
    ids.sort();
    let digest = blake3::hash(ids.join(",").as_bytes()).to_hex();
    let profile = match engine.profile() {
        Profile::Fast => "fast",
        Profile::Standard => "standard",
        Profile::Deep => "deep",
    };
    format!("{profile}:{}", &digest.as_str()[..16])
}

/// OCR, set up from doewood's model directory (not quail's environment variable).
struct Ocr {
    state: OcrState,
    #[cfg(feature = "ocr")]
    analyzer: Option<quail_ocr::OcrPages>,
}

impl Ocr {
    #[cfg(feature = "ocr")]
    fn setup(dir: &Path, workers: usize, limits: &mut Limits) -> Self {
        use std::sync::Arc;

        use quail_ocr::paddle::{PaddleConfig, PaddleFactory};
        use quail_ocr::{OcrConfig, OcrPages, OcrPermits, OcrResources};

        let missing = quail_ocr::models::missing(dir);
        if !missing.is_empty() {
            let reason = format!(
                "{} of {} OCR model files missing in {} (set DOEWOOD_OCR_MODELS, or fetch \
                 them with `quail models fetch`); scanned pages are only flagged needs_ocr",
                missing.len(),
                quail_ocr::models::MANIFEST.len(),
                dir.display()
            );
            tracing::warn!("{reason}");
            return Self {
                state: OcrState::Unavailable(reason),
                analyzer: None,
            };
        }
        let r = OcrResources::auto(workers.max(1));
        let paddle = PaddleConfig {
            intra_threads: r.threads.max(1),
            ..PaddleConfig::default()
        };
        match PaddleFactory::new(dir, paddle) {
            Ok(factory) => {
                let config = OcrConfig::default();
                // As quail's CLI: a file with pages to OCR may take its page budget after the
                // base timeout, with the other workers queued for the permits ahead of it.
                let ahead = workers.max(1).div_ceil(r.jobs.max(1)).saturating_sub(1);
                limits.max_timeout =
                    config.file_timeout(limits.timeout, u32::try_from(ahead).unwrap_or(u32::MAX));
                let analyzer = OcrPages::new(Arc::new(factory), r.jobs.max(1), config)
                    .with_permits(Arc::new(OcrPermits::new(r.jobs)));
                Self {
                    state: OcrState::Ready {
                        jobs: r.jobs,
                        threads: r.threads,
                    },
                    analyzer: Some(analyzer),
                }
            }
            Err(e) => {
                let reason = format!("OCR models in {} are unusable: {e}", dir.display());
                tracing::warn!("{reason}; scanned pages are only flagged needs_ocr");
                Self {
                    state: OcrState::Unavailable(reason),
                    analyzer: None,
                }
            }
        }
    }

    #[cfg(not(feature = "ocr"))]
    fn setup(_dir: &Path, _workers: usize, _limits: &mut Limits) -> Self {
        Self {
            state: OcrState::NotBuilt,
        }
    }

    fn analyzer_ready(&self) -> bool {
        matches!(self.state, OcrState::Ready { .. })
    }

    /// Adds the OCR step to the builder, if it can run.
    #[cfg(feature = "ocr")]
    fn register(self, b: quail_engine::EngineBuilder) -> (quail_engine::EngineBuilder, OcrState) {
        match self.analyzer {
            Some(a) => (b.analyzer(Box::new(a)), self.state),
            None => (b, self.state),
        }
    }

    /// Adds the OCR step to the builder, if it can run.
    #[cfg(not(feature = "ocr"))]
    fn register(self, b: quail_engine::EngineBuilder) -> (quail_engine::EngineBuilder, OcrState) {
        (b, self.state)
    }
}
