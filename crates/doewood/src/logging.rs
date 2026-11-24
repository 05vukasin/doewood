//! JSON logs on stderr, filtered by `RUST_LOG` (default `info`).

use tracing_subscriber::EnvFilter;

pub fn init() {
    let rust_log = std::env::var("RUST_LOG")
        .ok()
        .filter(|v| !v.trim().is_empty());
    let (filter, bad) = match &rust_log {
        None => (EnvFilter::new("info"), false),
        Some(v) => match EnvFilter::try_new(v) {
            Ok(f) => (f, false),
            Err(_) => (EnvFilter::new("info"), true),
        },
    };
    tracing_subscriber::fmt()
        .json()
        .flatten_event(true)
        .with_current_span(false)
        .with_span_list(false)
        .with_env_filter(filter)
        .with_writer(std::io::stderr)
        .init();
    if bad {
        tracing::warn!(var = "RUST_LOG", "invalid filter; using \"info\"");
    }
}
