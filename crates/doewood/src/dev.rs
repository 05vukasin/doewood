//! `doewood dev …`: hidden tools for local testing until the gRPC API (step 5) exists. They are
//! not a stable interface and will be removed.

use std::time::Duration;

use clap::Args;
use doewood_core::BootstrapConfig;
use doewood_store::{NewSource, S3Credentials, STORE_FILE, SourceState, Store, UrlStyle};

/// FOR LOCAL TESTING ONLY, until the gRPC API exists: adds an S3 source to this instance's
/// store and seals its credentials. The keys are read from the environment variables you name,
/// so they stay out of the shell history and the process list. `serve` scans the source.
#[derive(Args, Debug)]
pub struct AddSource {
    /// Endpoint URL, such as `http://127.0.0.1:9000` or `https://s3.eu-central-1.amazonaws.com`.
    #[arg(long)]
    endpoint: String,
    /// Bucket name.
    #[arg(long)]
    bucket: String,
    /// Key prefix; the whole bucket by default.
    #[arg(long, default_value = "")]
    prefix: String,
    /// Signing region (default `us-east-1`).
    #[arg(long)]
    region: Option<String>,
    /// Virtual-host-style URLs (`https://bucket.endpoint/key`) instead of path-style.
    #[arg(long)]
    virtual_host: bool,
    /// Seconds between scan passes.
    #[arg(long, default_value_t = 300)]
    interval_secs: u64,
    /// GetObject size cap in MiB (default 512).
    #[arg(long)]
    max_object_mib: Option<u64>,
    /// Environment variable holding the access key id.
    #[arg(long, value_name = "VAR")]
    access_key_env: String,
    /// Environment variable holding the secret access key.
    #[arg(long, value_name = "VAR")]
    secret_key_env: String,
    /// Environment variable holding a session token (temporary credentials).
    #[arg(long, value_name = "VAR")]
    session_token_env: Option<String>,
    /// Add the source paused (not scanned).
    #[arg(long)]
    paused: bool,
}

impl AddSource {
    /// Writes the source; returns the line to print.
    pub fn run(self, config: &BootstrapConfig) -> Result<String, String> {
        let var = |name: &str| {
            std::env::var(name)
                .ok()
                .filter(|v| !v.is_empty())
                .ok_or_else(|| format!("environment variable {name} is not set"))
        };
        let credentials = S3Credentials::new(
            var(&self.access_key_env)?,
            var(&self.secret_key_env)?,
            self.session_token_env.as_deref().map(var).transpose()?,
        )
        .map_err(|e| e.to_string())?;
        let source = NewSource {
            endpoint: self.endpoint,
            bucket: self.bucket,
            prefix: self.prefix,
            region: self.region,
            url_style: if self.virtual_host {
                UrlStyle::VirtualHost
            } else {
                UrlStyle::Path
            },
            max_object_bytes: self.max_object_mib.map(|m| m.saturating_mul(1024 * 1024)),
            scan_interval: Duration::from_secs(self.interval_secs),
            state: if self.paused {
                SourceState::Paused
            } else {
                SourceState::Active
            },
        };
        config.prepare_data_dir().map_err(|e| e.to_string())?;
        let key = crate::serve::master_key(config.master_key.clone())?;
        let path = config.data_dir.join(STORE_FILE);
        let store = Store::open(&path, &key)
            .map_err(|e| format!("cannot open the store {}: {e}", path.display()))?;
        let id = store
            .add_source(&source, Some(&credentials))
            .map_err(|e| e.to_string())?;
        store.close().map_err(|e| e.to_string())?;
        Ok(format!(
            "added source {id} ({} bucket {:?}, prefix {:?}, {})",
            source.endpoint, source.bucket, source.prefix, source.state
        ))
    }
}
