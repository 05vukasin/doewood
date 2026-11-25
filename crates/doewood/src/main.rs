//! The `doewood` binary: `serve`, `version` and `config check` (plus the hidden `dev` tools for
//! local testing).

mod cleanup;
mod dev;
mod enrichment;
mod icons;
mod ingest;
mod logging;
mod ops;
mod serve;
mod shared;

use std::process::ExitCode;

use clap::{Parser, Subcommand};
use doewood_core::{BootstrapConfig, Loaded, MasterKeySource};

/// The crate version, as printed by `doewood version`.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

#[derive(Parser)]
#[command(name = "doewood", version, about, propagate_version = true)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Run the service. Needs no configuration: every bootstrap value has a default.
    Serve,
    /// Print the version.
    Version,
    /// Inspect the configuration.
    Config {
        #[command(subcommand)]
        command: ConfigCommand,
    },
    /// Development tools for local testing only; not a stable interface.
    #[command(hide = true)]
    Dev {
        #[command(subcommand)]
        command: DevCommand,
    },
}

#[derive(Subcommand)]
enum DevCommand {
    /// Add an S3 source without a running server (same validation and claim as
    /// `ConfigService.AddSource`); the credentials are read from the variables you name.
    AddSource(dev::AddSource),
}

#[derive(Subcommand)]
enum ConfigCommand {
    /// Print the effective bootstrap configuration (secrets redacted) and any warnings.
    /// Creates nothing.
    Check,
}

fn main() -> ExitCode {
    match Cli::parse().command {
        Command::Serve => {
            logging::init();
            serve::main(BootstrapConfig::from_env())
        }
        Command::Version => {
            println!(
                "doewood {VERSION} (store schema {}, shared schema {})",
                doewood_store::SCHEMA_VERSION,
                doewood_pg::SCHEMA_VERSION
            );
            ExitCode::SUCCESS
        }
        Command::Config {
            command: ConfigCommand::Check,
        } => {
            print!("{}", config_report(&BootstrapConfig::from_env()));
            ExitCode::SUCCESS
        }
        Command::Dev {
            command: DevCommand::AddSource(args),
        } => {
            logging::init();
            match args.run(&BootstrapConfig::from_env().config) {
                Ok(line) => {
                    println!("{line}");
                    ExitCode::SUCCESS
                }
                Err(e) => {
                    eprintln!("doewood dev add-source: {e}");
                    ExitCode::FAILURE
                }
            }
        }
    }
}

/// The text of `doewood config check`.
fn config_report(loaded: &Loaded) -> String {
    use std::fmt::Write as _;
    let Loaded { config, warnings } = loaded;
    let mut out = format!("doewood {VERSION} bootstrap configuration\n");
    let entries = config.entries();
    let width = entries.iter().map(|e| e.var.len()).max().unwrap_or(0);
    for e in &entries {
        let _ = writeln!(out, "  {:width$}  {}  [{}]", e.var, e.value, e.origin);
    }
    if let MasterKeySource::File(path) = &config.master_key {
        let state = if path.exists() {
            "exists"
        } else {
            "will be generated on first start"
        };
        let _ = writeln!(out, "  key file: {state}");
    }
    let _ = writeln!(
        out,
        "  mode: {}",
        if config.is_standalone() {
            "standalone (records, graph and search in the local store)"
        } else {
            "shared Postgres"
        }
    );
    if warnings.is_empty() {
        out.push_str("no warnings\n");
    } else {
        let _ = writeln!(out, "{} warning(s):", warnings.len());
        for w in warnings {
            let _ = writeln!(out, "  {w}");
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use std::ffi::OsString;

    use super::*;

    #[test]
    fn config_report_redacts_and_lists_warnings() {
        let loaded = BootstrapConfig::from_lookup(
            |k| match k {
                "DOEWOOD_POSTGRES_URL" => Some(OsString::from("postgres://u:pw-123@db/x")),
                "DOEWOOD_LISTEN" => Some(OsString::from("nope")),
                _ => None,
            },
            || Some(OsString::from("host-a")),
        );
        let text = config_report(&loaded);
        assert!(text.contains("postgres://u:***@db/x"), "{text}");
        assert!(!text.contains("pw-123"), "{text}");
        assert!(text.contains("1 warning(s)"), "{text}");
        assert!(text.contains("DOEWOOD_MASTER_KEY"), "{text}");
        assert!(text.contains("host-a"), "{text}");
    }
}
