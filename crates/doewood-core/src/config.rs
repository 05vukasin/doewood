//! Bootstrap configuration (ADR 0001, section 3).
//!
//! Every bootstrap value comes from one environment variable and has a default. A missing or
//! invalid optional value never stops the start: an invalid one produces a [`ConfigWarning`]
//! and the default is used. Only an unusable data directory is fatal
//! ([`BootstrapConfig::prepare_data_dir`]).

use std::ffi::OsString;
use std::fmt;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};

use crate::{Error, InstanceId, MasterKey};

/// The environment variable names.
pub mod vars {
    /// Directory of the instance's SQLite store and (in development) its key file.
    pub const DATA_DIR: &str = "DOEWOOD_DATA_DIR";
    /// Address of the gRPC API.
    pub const LISTEN: &str = "DOEWOOD_LISTEN";
    /// Address of the ops endpoints: `/healthz`, `/readyz`, `/metrics`.
    pub const OPS_LISTEN: &str = "DOEWOOD_OPS_LISTEN";
    /// URL of the shared Postgres database; unset means standalone.
    pub const POSTGRES_URL: &str = "DOEWOOD_POSTGRES_URL";
    /// Base64 of the 32-byte master key that encrypts secrets.
    pub const MASTER_KEY: &str = "DOEWOOD_MASTER_KEY";
    /// The instance's name; defaults to the hostname.
    pub const INSTANCE_ID: &str = "DOEWOOD_INSTANCE_ID";

    /// Every bootstrap variable, in the order they are reported.
    pub const ALL: [&str; 6] = [
        DATA_DIR,
        LISTEN,
        OPS_LISTEN,
        POSTGRES_URL,
        MASTER_KEY,
        INSTANCE_ID,
    ];
}

/// Default of `DOEWOOD_DATA_DIR`.
pub const DEFAULT_DATA_DIR: &str = "./doewood-data";
/// Default of `DOEWOOD_LISTEN`.
pub const DEFAULT_LISTEN: SocketAddr = addr([127, 0, 0, 1], 7443);
/// Default of `DOEWOOD_OPS_LISTEN`.
pub const DEFAULT_OPS_LISTEN: SocketAddr = addr([127, 0, 0, 1], 7444);
/// Instance id used when the hostname is unknown or not a valid id.
pub const FALLBACK_INSTANCE_ID: &str = "doewood";
/// Name of the development key file inside the data directory.
pub const MASTER_KEY_FILE: &str = "master.key";

const fn addr(ip: [u8; 4], port: u16) -> SocketAddr {
    SocketAddr::V4(std::net::SocketAddrV4::new(
        std::net::Ipv4Addr::new(ip[0], ip[1], ip[2], ip[3]),
        port,
    ))
}

/// Where an effective value came from.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Origin {
    /// The variable was unset (or empty), so the default is used.
    Default,
    /// The variable's value is used.
    Env,
    /// The variable was set but invalid, so the default is used and a warning was raised.
    Fallback,
}

impl fmt::Display for Origin {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Default => "default",
            Self::Env => "env",
            Self::Fallback => "default (env value invalid)",
        })
    }
}

/// Where the master key comes from.
#[derive(Clone, Debug)]
pub enum MasterKeySource {
    /// From `DOEWOOD_MASTER_KEY`.
    Env(MasterKey),
    /// From a key file in the data directory, generated on first start. For development only.
    File(PathBuf),
}

/// A Postgres connection URL. `Display` and `Debug` hide the password.
#[derive(Clone, PartialEq, Eq)]
pub struct PostgresUrl(String);

impl PostgresUrl {
    /// Accepts `postgres://` and `postgresql://` URLs.
    pub fn parse(value: &str) -> Option<Self> {
        let lower = value.get(..13).unwrap_or(value).to_ascii_lowercase();
        let ok = (lower.starts_with("postgres://") && value.len() > "postgres://".len())
            || (lower.starts_with("postgresql://") && value.len() > "postgresql://".len());
        ok.then(|| Self(value.to_owned()))
    }

    /// The full URL, password included. Only for the database client.
    pub fn expose(&self) -> &str {
        &self.0
    }

    /// The URL with the password (in the user info or a `password=` parameter) replaced by
    /// `***`.
    pub fn redacted(&self) -> String {
        let url = &self.0;
        let Some(scheme_end) = url.find("://").map(|i| i + 3) else {
            return "***".to_owned();
        };
        let rest = &url[scheme_end..];
        let authority_end = rest.find(['/', '?', '#']).unwrap_or(rest.len());
        let (authority, tail) = rest.split_at(authority_end);
        let authority = match authority.rfind('@') {
            Some(at) => {
                let (userinfo, host) = authority.split_at(at);
                match userinfo.find(':') {
                    Some(colon) => format!("{}:***{host}", &userinfo[..colon]),
                    None => authority.to_owned(),
                }
            }
            None => authority.to_owned(),
        };
        let tail = match tail.find('?') {
            Some(q) => {
                let (path, query) = tail.split_at(q + 1);
                let (query, fragment) = query.split_at(query.find('#').unwrap_or(query.len()));
                let query: Vec<String> = query
                    .split('&')
                    .map(|pair| match pair.split_once('=') {
                        Some((k, _)) if k.to_ascii_lowercase().contains("password") => {
                            format!("{k}=***")
                        }
                        _ => pair.to_owned(),
                    })
                    .collect();
                format!("{path}{}{fragment}", query.join("&"))
            }
            None => tail.to_owned(),
        };
        format!("{}{authority}{tail}", &url[..scheme_end])
    }
}

impl fmt::Display for PostgresUrl {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.redacted())
    }
}

impl fmt::Debug for PostgresUrl {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "PostgresUrl({})", self.redacted())
    }
}

/// An optional value that was set but could not be used. Never contains a secret.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ConfigWarning {
    /// The variable (or `hostname` for the instance id default).
    pub var: &'static str,
    /// What was wrong and what is used instead.
    pub message: String,
}

impl fmt::Display for ConfigWarning {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.var, self.message)
    }
}

/// The result of loading: the effective configuration and the warnings raised on the way.
#[derive(Debug)]
pub struct Loaded {
    /// The effective configuration.
    pub config: BootstrapConfig,
    /// One warning per invalid value.
    pub warnings: Vec<ConfigWarning>,
}

/// One line of the configuration report (start log, `doewood config check`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Entry {
    /// The variable.
    pub var: &'static str,
    /// The effective value, with secrets redacted.
    pub value: String,
    /// Where it came from.
    pub origin: Origin,
}

#[derive(Clone, Copy, Debug)]
struct Origins {
    data_dir: Origin,
    listen: Origin,
    ops_listen: Origin,
    postgres_url: Origin,
    master_key: Origin,
    instance_id: Origin,
}

/// The effective bootstrap configuration.
#[derive(Clone, Debug)]
pub struct BootstrapConfig {
    /// Directory of the store and the development key file.
    pub data_dir: PathBuf,
    /// Address of the gRPC API.
    pub listen: SocketAddr,
    /// Address of the ops endpoints.
    pub ops_listen: SocketAddr,
    /// Shared Postgres database; `None` means standalone.
    pub postgres_url: Option<PostgresUrl>,
    /// Where the master key comes from.
    pub master_key: MasterKeySource,
    /// This instance's name.
    pub instance_id: InstanceId,
    origins: Origins,
}

impl BootstrapConfig {
    /// Loads from the process environment and the system hostname.
    pub fn from_env() -> Loaded {
        Self::from_lookup(
            |var| std::env::var_os(var),
            || Some(gethostname::gethostname()),
        )
    }

    /// Loads from any variable lookup, so tests never touch the process environment.
    /// An empty (or all-whitespace) value counts as unset.
    pub fn from_lookup(
        lookup: impl Fn(&str) -> Option<OsString>,
        hostname: impl FnOnce() -> Option<OsString>,
    ) -> Loaded {
        let mut w = Vec::new();
        let get = |var: &str| lookup(var).filter(|v| !v.to_string_lossy().trim().is_empty());

        let (data_dir, data_dir_origin) = match get(vars::DATA_DIR) {
            Some(v) => (PathBuf::from(v), Origin::Env),
            None => (PathBuf::from(DEFAULT_DATA_DIR), Origin::Default),
        };
        let (listen, listen_origin) =
            parse_socket(get(vars::LISTEN), vars::LISTEN, DEFAULT_LISTEN, &mut w);
        let (ops_listen, ops_listen_origin) = parse_socket(
            get(vars::OPS_LISTEN),
            vars::OPS_LISTEN,
            DEFAULT_OPS_LISTEN,
            &mut w,
        );
        let (postgres_url, postgres_origin) = parse_postgres(get(vars::POSTGRES_URL), &mut w);
        let (master_key, master_key_origin) = parse_master_key(
            get(vars::MASTER_KEY),
            data_dir.join(MASTER_KEY_FILE),
            &mut w,
        );
        let (instance_id, instance_origin) =
            parse_instance_id(get(vars::INSTANCE_ID), hostname, &mut w);

        Loaded {
            config: Self {
                data_dir,
                listen,
                ops_listen,
                postgres_url,
                master_key,
                instance_id,
                origins: Origins {
                    data_dir: data_dir_origin,
                    listen: listen_origin,
                    ops_listen: ops_listen_origin,
                    postgres_url: postgres_origin,
                    master_key: master_key_origin,
                    instance_id: instance_origin,
                },
            },
            warnings: w,
        }
    }

    /// True when no shared Postgres is configured.
    pub fn is_standalone(&self) -> bool {
        self.postgres_url.is_none()
    }

    /// Path of the development key file, whether or not it is used.
    pub fn master_key_file(&self) -> PathBuf {
        self.data_dir.join(MASTER_KEY_FILE)
    }

    /// Creates the data directory if needed and checks that it is writable. This is the only
    /// fatal bootstrap check.
    pub fn prepare_data_dir(&self) -> Result<(), Error> {
        prepare_dir(&self.data_dir)
    }

    /// The effective configuration, one entry per variable, with secrets redacted.
    pub fn entries(&self) -> Vec<Entry> {
        let o = self.origins;
        vec![
            Entry {
                var: vars::DATA_DIR,
                value: self.data_dir.display().to_string(),
                origin: o.data_dir,
            },
            Entry {
                var: vars::LISTEN,
                value: self.listen.to_string(),
                origin: o.listen,
            },
            Entry {
                var: vars::OPS_LISTEN,
                value: self.ops_listen.to_string(),
                origin: o.ops_listen,
            },
            Entry {
                var: vars::POSTGRES_URL,
                value: self.postgres_url.as_ref().map_or_else(
                    || "not configured (standalone)".to_owned(),
                    PostgresUrl::redacted,
                ),
                origin: o.postgres_url,
            },
            Entry {
                var: vars::MASTER_KEY,
                value: match &self.master_key {
                    MasterKeySource::Env(_) => "set (redacted)".to_owned(),
                    MasterKeySource::File(p) => {
                        format!(
                            "not configured: key file {} (development only)",
                            p.display()
                        )
                    }
                },
                origin: o.master_key,
            },
            Entry {
                var: vars::INSTANCE_ID,
                value: self.instance_id.to_string(),
                origin: o.instance_id,
            },
        ]
    }
}

fn warn(warnings: &mut Vec<ConfigWarning>, var: &'static str, message: String) {
    warnings.push(ConfigWarning { var, message });
}

fn parse_socket(
    value: Option<OsString>,
    var: &'static str,
    default: SocketAddr,
    w: &mut Vec<ConfigWarning>,
) -> (SocketAddr, Origin) {
    let Some(v) = value else {
        return (default, Origin::Default);
    };
    match v.to_str().map(|s| s.trim().parse::<SocketAddr>()) {
        Some(Ok(a)) => (a, Origin::Env),
        Some(Err(_)) => {
            let shown = v.to_string_lossy();
            warn(
                w,
                var,
                format!("{shown:?} is not an IP:port address; using {default}"),
            );
            (default, Origin::Fallback)
        }
        None => {
            warn(w, var, format!("not valid UTF-8; using {default}"));
            (default, Origin::Fallback)
        }
    }
}

/// Never echoes the value: it may hold a password.
fn parse_postgres(
    value: Option<OsString>,
    w: &mut Vec<ConfigWarning>,
) -> (Option<PostgresUrl>, Origin) {
    let Some(v) = value else {
        return (None, Origin::Default);
    };
    if let Some(url) = v.to_str().map(str::trim).and_then(PostgresUrl::parse) {
        (Some(url), Origin::Env)
    } else {
        warn(
            w,
            vars::POSTGRES_URL,
            "not a postgres:// or postgresql:// URL (value not shown); running standalone"
                .to_owned(),
        );
        (None, Origin::Fallback)
    }
}

/// Never echoes the value.
fn parse_master_key(
    value: Option<OsString>,
    key_file: PathBuf,
    w: &mut Vec<ConfigWarning>,
) -> (MasterKeySource, Origin) {
    let Some(v) = value else {
        return (MasterKeySource::File(key_file), Origin::Default);
    };
    let reason = match v.to_str().map(MasterKey::from_base64) {
        Some(Ok(key)) => return (MasterKeySource::Env(key), Origin::Env),
        Some(Err(e)) => format!("{e} (value not shown)"),
        None => "not valid UTF-8".to_owned(),
    };
    warn(
        w,
        vars::MASTER_KEY,
        format!("{reason}; using the key file instead"),
    );
    (MasterKeySource::File(key_file), Origin::Fallback)
}

fn parse_instance_id(
    value: Option<OsString>,
    hostname: impl FnOnce() -> Option<OsString>,
    w: &mut Vec<ConfigWarning>,
) -> (InstanceId, Origin) {
    let origin = match value {
        None => Origin::Default,
        Some(v) => {
            let reason = match v.to_str().map(|s| InstanceId::new(s.trim())) {
                Some(Ok(id)) => return (id, Origin::Env),
                Some(Err(e)) => e.to_string(),
                None => "not valid UTF-8".to_owned(),
            };
            let shown = v.to_string_lossy();
            warn(
                w,
                vars::INSTANCE_ID,
                format!("{shown:?}: {reason}; using the hostname"),
            );
            Origin::Fallback
        }
    };
    let id = match hostname().map(|h| h.to_string_lossy().trim().to_owned()) {
        Some(h) if !h.is_empty() => InstanceId::new(h.clone()).unwrap_or_else(|e| {
            warn(
                w,
                "hostname",
                format!("{h:?} is not a valid instance id ({e}); using {FALLBACK_INSTANCE_ID:?}"),
            );
            InstanceId::fallback()
        }),
        _ => {
            warn(
                w,
                "hostname",
                format!("unknown; using instance id {FALLBACK_INSTANCE_ID:?}"),
            );
            InstanceId::fallback()
        }
    };
    (id, origin)
}

fn prepare_dir(path: &Path) -> Result<(), Error> {
    let fail = |source| Error::DataDir {
        path: path.to_owned(),
        source,
    };
    std::fs::create_dir_all(path).map_err(fail)?;
    let probe = path.join(".doewood-write-probe");
    std::fs::write(&probe, b"ok").map_err(fail)?;
    std::fs::remove_file(&probe).map_err(fail)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    fn load(env: &[(&str, &str)], host: Option<&str>) -> Loaded {
        let env: HashMap<String, OsString> = env
            .iter()
            .map(|(k, v)| ((*k).to_owned(), OsString::from(v)))
            .collect();
        BootstrapConfig::from_lookup(|k| env.get(k).cloned(), || host.map(OsString::from))
    }

    #[test]
    fn defaults_with_no_env() {
        let Loaded { config, warnings } = load(&[], Some("doewood-0"));
        assert!(warnings.is_empty(), "{warnings:?}");
        assert_eq!(config.data_dir, PathBuf::from("./doewood-data"));
        assert_eq!(config.listen.to_string(), "127.0.0.1:7443");
        assert_eq!(config.ops_listen.to_string(), "127.0.0.1:7444");
        assert!(config.is_standalone());
        assert!(
            matches!(&config.master_key, MasterKeySource::File(p) if p == &PathBuf::from("./doewood-data/master.key"))
        );
        assert_eq!(config.instance_id.as_str(), "doewood-0");
        assert!(config.entries().iter().all(|e| e.origin == Origin::Default));
    }

    #[test]
    fn valid_env_values_are_used() {
        let key = MasterKey::from_bytes([3; 32]);
        let Loaded { config, warnings } = load(
            &[
                ("DOEWOOD_DATA_DIR", "/var/lib/doewood"),
                ("DOEWOOD_LISTEN", "0.0.0.0:9000"),
                ("DOEWOOD_OPS_LISTEN", "[::1]:9001"),
                (
                    "DOEWOOD_POSTGRES_URL",
                    "postgres://dw:hunter2@db:5432/doewood",
                ),
                ("DOEWOOD_MASTER_KEY", &key.to_base64()),
                ("DOEWOOD_INSTANCE_ID", "worker-7"),
            ],
            Some("ignored"),
        );
        assert!(warnings.is_empty(), "{warnings:?}");
        assert_eq!(config.data_dir, PathBuf::from("/var/lib/doewood"));
        assert_eq!(config.listen.to_string(), "0.0.0.0:9000");
        assert_eq!(config.ops_listen.to_string(), "[::1]:9001");
        assert!(!config.is_standalone());
        assert!(matches!(&config.master_key, MasterKeySource::Env(k) if k.as_bytes() == &[3; 32]));
        assert_eq!(config.instance_id.as_str(), "worker-7");
        // The data dir also moves the key file path.
        assert_eq!(
            config.master_key_file(),
            PathBuf::from("/var/lib/doewood/master.key")
        );
        assert!(config.entries().iter().all(|e| e.origin == Origin::Env));
    }

    #[test]
    fn empty_values_count_as_unset() {
        let Loaded { config, warnings } = load(
            &[("DOEWOOD_LISTEN", ""), ("DOEWOOD_POSTGRES_URL", "  ")],
            Some("h"),
        );
        assert!(warnings.is_empty());
        assert_eq!(config.listen, DEFAULT_LISTEN);
        assert!(config.is_standalone());
    }

    #[test]
    fn invalid_values_fall_back_with_a_warning() {
        let Loaded { config, warnings } = load(
            &[
                ("DOEWOOD_LISTEN", "localhost"),
                ("DOEWOOD_OPS_LISTEN", "1.2.3.4:99999"),
                ("DOEWOOD_POSTGRES_URL", "mysql://root:topsecret@db/x"),
                ("DOEWOOD_MASTER_KEY", "c2hvcnQ="),
                ("DOEWOOD_INSTANCE_ID", "bad id!"),
            ],
            Some("pod-1"),
        );
        assert_eq!(config.listen, DEFAULT_LISTEN);
        assert_eq!(config.ops_listen, DEFAULT_OPS_LISTEN);
        assert!(config.is_standalone());
        assert!(matches!(config.master_key, MasterKeySource::File(_)));
        assert_eq!(config.instance_id.as_str(), "pod-1");
        let vars: Vec<_> = warnings.iter().map(|w| w.var).collect();
        assert_eq!(
            vars,
            [
                "DOEWOOD_LISTEN",
                "DOEWOOD_OPS_LISTEN",
                "DOEWOOD_POSTGRES_URL",
                "DOEWOOD_MASTER_KEY",
                "DOEWOOD_INSTANCE_ID"
            ]
        );
        // Secret values are never echoed.
        let all = warnings.iter().map(ToString::to_string).collect::<String>();
        assert!(
            !all.contains("topsecret") && !all.contains("c2hvcnQ"),
            "{all}"
        );
        let origins: Vec<_> = config.entries().iter().map(|e| e.origin).collect();
        assert_eq!(
            origins,
            [
                Origin::Default,
                Origin::Fallback,
                Origin::Fallback,
                Origin::Fallback,
                Origin::Fallback,
                Origin::Fallback
            ]
        );
    }

    #[test]
    fn bad_or_missing_hostname_falls_back_to_a_fixed_id() {
        let Loaded { config, warnings } = load(&[], Some("my laptop"));
        assert_eq!(config.instance_id.as_str(), FALLBACK_INSTANCE_ID);
        assert_eq!(warnings.len(), 1);
        let Loaded { config, warnings } = load(&[], None);
        assert_eq!(config.instance_id.as_str(), FALLBACK_INSTANCE_ID);
        assert_eq!(warnings.len(), 1);
    }

    #[test]
    fn entries_never_show_secrets() {
        let key = MasterKey::from_bytes([9; 32]);
        let b64 = key.to_base64();
        let Loaded { config, .. } = load(
            &[
                (
                    "DOEWOOD_POSTGRES_URL",
                    "postgresql://dw:hunter2@db/doewood?sslpassword=s3cr3t&sslmode=require",
                ),
                ("DOEWOOD_MASTER_KEY", &b64),
            ],
            Some("h"),
        );
        let shown = format!("{:?} {:?}", config.entries(), config);
        assert!(!shown.contains("hunter2"), "{shown}");
        assert!(!shown.contains("s3cr3t"), "{shown}");
        assert!(!shown.contains(b64.as_str()), "{shown}");
        assert!(shown.contains("postgresql://dw:***@db/doewood?sslpassword=***&sslmode=require"));
    }

    #[test]
    fn postgres_url_redaction() {
        let r = |s: &str| PostgresUrl::parse(s).unwrap().redacted();
        assert_eq!(r("postgres://db/x"), "postgres://db/x");
        assert_eq!(r("postgres://user@db/x"), "postgres://user@db/x");
        assert_eq!(r("postgres://u:p@ss@db:5432"), "postgres://u:***@db:5432");
        assert_eq!(
            r("POSTGRES://u:p@db?password=x#f"),
            "POSTGRES://u:***@db?password=***#f"
        );
        assert!(PostgresUrl::parse("postgres://").is_none());
        assert!(PostgresUrl::parse("host=db user=x").is_none());
    }

    #[test]
    fn data_dir_is_created_and_checked() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("a/b");
        let dir_str = dir.to_str().unwrap();
        let Loaded { config, .. } = load(&[("DOEWOOD_DATA_DIR", dir_str)], Some("h"));
        config.prepare_data_dir().unwrap();
        assert!(dir.is_dir());
        // A regular file where the directory should be is unusable, and fatal.
        let file = tmp.path().join("file");
        std::fs::write(&file, b"x").unwrap();
        let Loaded { config, .. } = load(&[("DOEWOOD_DATA_DIR", file.to_str().unwrap())], None);
        assert!(matches!(
            config.prepare_data_dir(),
            Err(Error::DataDir { .. })
        ));
    }
}
