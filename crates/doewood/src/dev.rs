//! `doewood dev …`: hidden tools for local testing and scripts. Not a stable interface.

use std::sync::Arc;

use clap::Args;
use doewood_api::pb::{AddSourceRequest, S3Credentials, SourceState, UrlStyle};
use doewood_api::{ApiOptions, ApiParts, ApiState, Progress};
use doewood_core::BootstrapConfig;
use doewood_model::Backend;
use doewood_store::{STORE_FILE, Standalone, Store};
use tokio::sync::watch;

/// Adds an S3 source to this instance without a running server, through the same code as
/// `ConfigService.AddSource`: the same validation, and the same claim in the shared store
/// (`DOEWOOD_POSTGRES_URL`, when set). The keys are read from the environment variables you
/// name, so they stay out of the shell history and the process list. `serve` scans the source.
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
    #[arg(long, default_value = "")]
    region: String,
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
        let url_style = if self.virtual_host {
            UrlStyle::VirtualHost
        } else {
            UrlStyle::Path
        };
        let request = AddSourceRequest {
            endpoint: self.endpoint,
            bucket: self.bucket,
            prefix: self.prefix,
            region: self.region,
            url_style: url_style as i32,
            credentials: Some(S3Credentials {
                access_key_id: var(&self.access_key_env)?,
                secret_access_key: var(&self.secret_key_env)?,
                session_token: self
                    .session_token_env
                    .as_deref()
                    .map(var)
                    .transpose()?
                    .unwrap_or_default(),
            }),
            scan_interval_seconds: self.interval_secs,
            max_object_bytes: self
                .max_object_mib
                .map_or(0, |m| m.saturating_mul(1024 * 1024)),
            llm_policy: None,
            paused: self.paused,
        };
        config.prepare_data_dir().map_err(|e| e.to_string())?;
        let key = crate::serve::master_key(config.master_key.clone())?;
        let path = config.data_dir.join(STORE_FILE);
        let store = Arc::new(
            Store::open(&path, &key)
                .map_err(|e| format!("cannot open the store {}: {e}", path.display()))?,
        );
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| format!("cannot start the async runtime: {e}"))?;
        let result = runtime.block_on(add(config, Arc::clone(&store), &request));
        drop(runtime);
        let store = Arc::try_unwrap(store).map_err(|_| "store still in use".to_owned())?;
        store.close().map_err(|e| e.to_string())?;
        let source = result?;
        let state = if source.state == SourceState::Paused as i32 {
            "paused"
        } else {
            "active"
        };
        Ok(format!(
            "added source {} ({} bucket {:?}, prefix {:?}, {state})",
            source.id, source.endpoint, source.bucket, source.prefix
        ))
    }
}

async fn add(
    config: &BootstrapConfig,
    store: Arc<Store>,
    request: &AddSourceRequest,
) -> Result<doewood_api::pb::Source, String> {
    let backend: Arc<dyn Backend> = match &config.postgres_url {
        None => Arc::new(Standalone::new(Arc::clone(&store))),
        Some(url) => Arc::new(
            doewood_pg::PgStore::connect(url)
                .await
                .map_err(|e| format!("shared Postgres: {e}"))?,
        ),
    };
    let (_backend_tx, backend_rx) = watch::channel(Some(backend));
    let (_shutdown_tx, shutdown_rx) = watch::channel(false);
    let state = ApiState::new(ApiParts {
        store,
        backend: backend_rx,
        instance: config.instance_id.clone(),
        // No scanner here: `serve` starts it.
        discovery: None,
        progress: Arc::new(Progress::new()),
        reprocess: None,
        options: ApiOptions::default(),
        shutdown: shutdown_rx,
    });
    doewood_api::add_source(&state, request)
        .await
        .map_err(|status| format!("{:?}: {}", status.code(), status.message()))
}
