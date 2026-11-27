//! doewood's company identity (ADR 0002 section 1): pure rules, no I/O and no store.
//!
//! - [`name`]: normalization, Latin/Cyrillic keys, legal forms, noise and geography words,
//!   contract role terms, aliases from parentheses, and the deterministic ladder keys
//!   (sections 1.1 to 1.3).
//! - [`id`] and [`check`]: registry identifiers, validated before use (section 1.5).
//! - [`fuzzy`]: the fuzzy name score and the jurisdiction veto (section 1.4).
//! - [`fusion`]: noisy-OR evidence fusion and the decision gate (section 1.6).
//! - [`group`]: stable `uuid5` group ids (section 1.7).
//! - [`jurisdiction`]: a company's jurisdiction as one ISO 3166 code (issue #201).
//!
//! doewood-ingest applies the deterministic steps when it writes the graph; the store lookups
//! (who owns a key) stay there.

pub mod check;
pub mod fusion;
pub mod fuzzy;
pub mod group;
pub mod id;
pub mod jurisdiction;
pub mod name;
#[cfg(test)]
mod testids;

pub use fusion::{Candidate, Decision, Evidence, ReviewReason, Source, decide, fuse};
pub use fuzzy::{FuzzyMatch, FuzzyVerdict};
pub use group::{group_id, group_id_of};
pub use id::{IdKind, Identifier};
pub use name::{
    Alias, AliasKind, CompanyName, LEGAL_FORMS, LadderStep, LegalForm, is_role_term, normalize,
    split_aliases,
};
