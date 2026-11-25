//! Registry delta feeds (ADR 0002 section 6, issue #49): what changed at a registry, so the
//! enrichment job refreshes the companies it already knows when the registry changes them,
//! not only on their 30-day schedule.
//!
//! - [`gleif`]: GLEIF's golden copy delta files (LEI-CDF and RR-CDF, zipped CSV), three
//!   publications a day; the changed LEIs.
//! - [`edgar`]: SEC EDGAR's daily index; the CIKs that filed a 10-K, 20-F or 8-K (or an
//!   amendment) that day.
//! - [`companies_house`]: Companies House's streaming API (company profiles, officers and
//!   PSCs), with a streaming key; the company numbers of the events, and their timepoints.
//! - APR has no deltas: its monthly set is downloaded again and swapped in
//!   ([`crate::apr::Apr::refresh`]).
//!
//! Each feed client streams: a file is spooled into a capped temporary file or read line by
//! line, never held whole, and only identifiers are kept. They send nothing about a company:
//! the files and streams are the same for everyone. Like the clients, every failure is a
//! typed [`crate::Warning`]; the caller tries again on its next run.

pub mod companies_house;
mod csv;
pub mod edgar;
pub mod gleif;
mod net;
