//! The contract every doewood crate shares: the bootstrap configuration (ADR 0001, section 3),
//! the error type and the ids.
//!
//! Runtime settings (sources, AI providers, policies) are not here: they live in the
//! instance's store and arrive through the API.

pub mod config;
pub mod error;
pub mod id;
pub mod key;

pub use config::{
    BootstrapConfig, ConfigWarning, Entry, Loaded, MasterKeySource, Origin, PostgresUrl,
};
pub use error::Error;
pub use id::{InstanceId, JobId, SourceId};
pub use key::MasterKey;
