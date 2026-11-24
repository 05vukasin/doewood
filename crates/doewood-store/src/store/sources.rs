//! Sources (ADR 0001, section 5): an S3-compatible endpoint, a bucket and an optional prefix,
//! with their state, schedule and options, and the credentials sealed in `secrets`.

use std::fmt;
use std::str::FromStr;
use std::time::Duration;

use doewood_core::SourceId;
use rusqlite::{OptionalExtension as _, Row, params};
use zeroize::Zeroizing;

use super::{Store, now};
use crate::StoreError;

/// Scan interval of a source that sets none.
pub const DEFAULT_SCAN_INTERVAL: Duration = Duration::from_mins(5);

/// GetObject size cap of a source that sets none: 512 MiB.
pub const DEFAULT_MAX_OBJECT_BYTES: u64 = 512 * 1024 * 1024;

/// The state of a source.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum SourceState {
    /// Scanned on its schedule.
    Active,
    /// Not scanned until it is activated again.
    Paused,
    /// The last scan failed; [`Source::state_reason`] says why. Still scanned on its schedule,
    /// and a pass that succeeds makes it active again.
    Error,
    /// Removed; kept only until its rows are cleaned up.
    Removed,
}

impl SourceState {
    /// The state as stored.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Active => "active",
            Self::Paused => "paused",
            Self::Error => "error",
            Self::Removed => "removed",
        }
    }

    /// Whether a scanner runs for a source in this state.
    pub fn is_scanned(self) -> bool {
        matches!(self, Self::Active | Self::Error)
    }
}

impl fmt::Display for SourceState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for SourceState {
    type Err = StoreError;
    fn from_str(s: &str) -> Result<Self, StoreError> {
        Ok(match s {
            "active" => Self::Active,
            "paused" => Self::Paused,
            "error" => Self::Error,
            "removed" => Self::Removed,
            other => {
                return Err(StoreError::InvalidSource(format!(
                    "unknown state {other:?}"
                )));
            }
        })
    }
}

/// How bucket URLs are built.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub enum UrlStyle {
    /// `https://endpoint/bucket/key`: MinIO, Ceph and most self-hosted stores.
    #[default]
    Path,
    /// `https://bucket.endpoint/key`: AWS (required for new buckets), R2, Wasabi.
    VirtualHost,
}

impl UrlStyle {
    /// The style as stored.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Path => "path",
            Self::VirtualHost => "virtual_host",
        }
    }
}

impl FromStr for UrlStyle {
    type Err = StoreError;
    fn from_str(s: &str) -> Result<Self, StoreError> {
        match s {
            "path" => Ok(Self::Path),
            "virtual_host" => Ok(Self::VirtualHost),
            other => Err(StoreError::InvalidSource(format!(
                "unknown URL style {other:?}"
            ))),
        }
    }
}

/// A source to add.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NewSource {
    /// Endpoint URL, `http://` or `https://`.
    pub endpoint: String,
    /// Bucket name.
    pub bucket: String,
    /// Key prefix; empty for the whole bucket.
    pub prefix: String,
    /// Signing region; `None` signs for `us-east-1`, which MinIO and Ceph accept.
    pub region: Option<String>,
    /// Path-style or virtual-host-style URLs.
    pub url_style: UrlStyle,
    /// GetObject size cap; `None` is [`DEFAULT_MAX_OBJECT_BYTES`].
    pub max_object_bytes: Option<u64>,
    /// Time between the end of one scan pass and the start of the next.
    pub scan_interval: Duration,
    /// The initial state.
    pub state: SourceState,
}

impl NewSource {
    /// A source with the defaults: whole bucket, path style, 300 s interval, active.
    pub fn new(endpoint: impl Into<String>, bucket: impl Into<String>) -> Self {
        Self {
            endpoint: endpoint.into(),
            bucket: bucket.into(),
            prefix: String::new(),
            region: None,
            url_style: UrlStyle::Path,
            max_object_bytes: None,
            scan_interval: DEFAULT_SCAN_INTERVAL,
            state: SourceState::Active,
        }
    }

    fn validate(&self) -> Result<(), StoreError> {
        let bad = |m: &str| Err(StoreError::InvalidSource(m.to_owned()));
        let lower = self.endpoint.to_ascii_lowercase();
        if !(lower.starts_with("http://") || lower.starts_with("https://")) {
            return bad("the endpoint must be an http:// or https:// URL");
        }
        if self.bucket.is_empty() || self.bucket.len() > 255 {
            return bad("the bucket name must be 1 to 255 characters");
        }
        if self.prefix.len() > 1024 {
            return bad("the prefix is longer than 1024 bytes");
        }
        if self.scan_interval.as_secs() == 0 {
            return bad("the scan interval must be at least one second");
        }
        if self.max_object_bytes == Some(0) {
            return bad("the object size cap must be positive");
        }
        Ok(())
    }
}

/// A stored source.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Source {
    /// Its id.
    pub id: SourceId,
    /// Endpoint URL.
    pub endpoint: String,
    /// Bucket name.
    pub bucket: String,
    /// Key prefix; empty for the whole bucket.
    pub prefix: String,
    /// Signing region, if set.
    pub region: Option<String>,
    /// Path-style or virtual-host-style URLs.
    pub url_style: UrlStyle,
    /// Name of the secret holding its credentials; `None` for anonymous access.
    pub credentials_secret: Option<String>,
    /// GetObject size cap, if set.
    pub max_object_bytes: Option<u64>,
    /// Its state.
    pub state: SourceState,
    /// Why it is in the error state.
    pub state_reason: Option<String>,
    /// Time between scan passes.
    pub scan_interval: Duration,
    /// When it was added (Unix seconds).
    pub created_at: i64,
}

impl Source {
    /// The GetObject size cap, with the default applied.
    pub fn max_object_bytes(&self) -> u64 {
        self.max_object_bytes.unwrap_or(DEFAULT_MAX_OBJECT_BYTES)
    }

    /// The signing region, with the default (`us-east-1`) applied.
    pub fn region(&self) -> &str {
        self.region.as_deref().unwrap_or("us-east-1")
    }

    fn from_row(r: &Row<'_>) -> rusqlite::Result<Result<Self, StoreError>> {
        let id: String = r.get("id")?;
        let url_style: String = r.get("url_style")?;
        let state: String = r.get("state")?;
        let max: Option<i64> = r.get("max_object_bytes")?;
        let interval: i64 = r.get("scan_interval_s")?;
        let endpoint = r.get("endpoint")?;
        let bucket = r.get("bucket")?;
        let prefix = r.get("prefix")?;
        let region = r.get("region")?;
        let credentials_secret = r.get("credentials_secret")?;
        let state_reason = r.get("state_reason")?;
        let created_at = r.get("created_at")?;
        Ok((|| {
            Ok(Self {
                id: id
                    .parse()
                    .map_err(|e: doewood_core::Error| StoreError::InvalidSource(e.to_string()))?,
                endpoint,
                bucket,
                prefix,
                region,
                url_style: url_style.parse()?,
                credentials_secret,
                max_object_bytes: max.map(u64::try_from).transpose().map_err(|_| {
                    StoreError::InvalidSource("negative object size cap".to_owned())
                })?,
                state: state.parse()?,
                state_reason,
                scan_interval: Duration::from_secs(u64::try_from(interval).unwrap_or(1)),
                created_at,
            })
        })())
    }
}

/// Credentials for an S3-compatible store. Every field is zeroized on drop, and `Debug` prints
/// none of them.
#[derive(Clone, PartialEq, Eq)]
pub struct S3Credentials {
    access_key_id: Zeroizing<String>,
    secret_access_key: Zeroizing<String>,
    session_token: Option<Zeroizing<String>>,
}

impl fmt::Debug for S3Credentials {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("S3Credentials")
            .field("session_token", &self.session_token.is_some())
            .finish_non_exhaustive()
    }
}

const CREDENTIALS_FORMAT: &str = "doewood.s3.v1";

impl S3Credentials {
    /// Validates credentials: the key id and secret are non-empty, and no value holds a line
    /// break.
    pub fn new(
        access_key_id: impl Into<String>,
        secret_access_key: impl Into<String>,
        session_token: Option<String>,
    ) -> Result<Self, StoreError> {
        let creds = Self {
            access_key_id: Zeroizing::new(access_key_id.into()),
            secret_access_key: Zeroizing::new(secret_access_key.into()),
            session_token: session_token.map(Zeroizing::new),
        };
        let bad = |m: &str| Err(StoreError::InvalidSource(m.to_owned()));
        if creds.access_key_id.is_empty() || creds.secret_access_key.is_empty() {
            return bad("the access key id and the secret key must not be empty");
        }
        let values = [
            Some(creds.access_key_id.as_str()),
            Some(creds.secret_access_key.as_str()),
            creds.session_token(),
        ];
        if values
            .into_iter()
            .flatten()
            .any(|v| v.contains(['\n', '\r']))
        {
            return bad("credentials must not contain line breaks");
        }
        Ok(creds)
    }

    /// The access key id.
    pub fn access_key_id(&self) -> &str {
        &self.access_key_id
    }

    /// The secret access key.
    pub fn secret_access_key(&self) -> &str {
        &self.secret_access_key
    }

    /// The session token of temporary credentials.
    pub fn session_token(&self) -> Option<&str> {
        self.session_token.as_ref().map(|t| t.as_str())
    }

    fn encode(&self) -> Zeroizing<Vec<u8>> {
        let mut out = Zeroizing::new(Vec::new());
        for part in [
            CREDENTIALS_FORMAT,
            self.access_key_id(),
            self.secret_access_key(),
            self.session_token().unwrap_or(""),
        ] {
            out.extend_from_slice(part.as_bytes());
            out.push(b'\n');
        }
        out
    }

    fn decode(name: &str, bytes: &[u8]) -> Result<Self, StoreError> {
        let bad = || StoreError::InvalidSource(format!("secret {name:?} is not S3 credentials"));
        let text = std::str::from_utf8(bytes).map_err(|_| bad())?;
        let mut parts = text.split('\n');
        if parts.next() != Some(CREDENTIALS_FORMAT) {
            return Err(bad());
        }
        let (Some(key), Some(secret), Some(token), Some(""), None) = (
            parts.next(),
            parts.next(),
            parts.next(),
            parts.next(),
            parts.next(),
        ) else {
            return Err(bad());
        };
        let token = (!token.is_empty()).then(|| token.to_owned());
        Self::new(key, secret, token).map_err(|_| bad())
    }
}

/// The secret name of a source's credentials.
fn credentials_secret_name(id: SourceId) -> String {
    format!("s3/{id}")
}

const SOURCE_COLUMNS: &str = "id, endpoint, bucket, prefix, region, url_style, \
    credentials_secret, max_object_bytes, state, state_reason, scan_interval_s, created_at";

impl Store {
    /// Adds a source, with its credentials sealed in one transaction. A source with the same
    /// endpoint, bucket and prefix is refused.
    pub fn add_source(
        &self,
        source: &NewSource,
        credentials: Option<&S3Credentials>,
    ) -> Result<SourceId, StoreError> {
        source.validate()?;
        let id = SourceId::generate();
        let now = now();
        let max = source
            .max_object_bytes
            .map(i64::try_from)
            .transpose()
            .map_err(|_| StoreError::InvalidSource("object size cap too large".to_owned()))?;
        let interval = i64::try_from(source.scan_interval.as_secs())
            .map_err(|_| StoreError::InvalidSource("scan interval too large".to_owned()))?;
        let sealed = credentials
            .map(|c| {
                let name = credentials_secret_name(id);
                let (nonce, ct) = self.cipher.seal(&name, &c.encode())?;
                Ok::<_, StoreError>((name, nonce, ct))
            })
            .transpose()?;
        let mut conn = self.conn();
        let tx = conn.transaction()?;
        tx.execute(
            "INSERT INTO sources (id, endpoint, bucket, prefix, region, url_style, \
             credentials_secret, max_object_bytes, state, scan_interval_s, created_at, updated_at) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?11)",
            params![
                id.to_string(),
                source.endpoint,
                source.bucket,
                source.prefix,
                source.region,
                source.url_style.as_str(),
                sealed.as_ref().map(|(name, _, _)| name),
                max,
                source.state.as_str(),
                interval,
                now,
            ],
        )?;
        if let Some((name, nonce, ct)) = &sealed {
            tx.execute(
                "INSERT INTO secrets (name, nonce, ciphertext, created_at, updated_at) \
                 VALUES (?1, ?2, ?3, ?4, ?4) \
                 ON CONFLICT (name) DO UPDATE SET nonce = excluded.nonce, \
                 ciphertext = excluded.ciphertext, updated_at = excluded.updated_at",
                params![name, nonce.as_slice(), ct, now],
            )?;
        }
        tx.commit()?;
        Ok(id)
    }

    /// All sources, oldest first.
    pub fn sources(&self) -> Result<Vec<Source>, StoreError> {
        let conn = self.conn();
        let mut stmt = conn.prepare(&format!(
            "SELECT {SOURCE_COLUMNS} FROM sources ORDER BY created_at, id"
        ))?;
        let rows = stmt
            .query_map([], Source::from_row)?
            .collect::<Result<Vec<_>, _>>()?;
        rows.into_iter().collect()
    }

    /// One source.
    pub fn source(&self, id: SourceId) -> Result<Option<Source>, StoreError> {
        self.conn()
            .query_row(
                &format!("SELECT {SOURCE_COLUMNS} FROM sources WHERE id = ?1"),
                [id.to_string()],
                Source::from_row,
            )
            .optional()?
            .transpose()
    }

    /// Sets a source's state. `reason` is kept for [`SourceState::Error`] and cleared otherwise.
    pub fn set_source_state(
        &self,
        id: SourceId,
        state: SourceState,
        reason: Option<&str>,
    ) -> Result<(), StoreError> {
        let reason = if state == SourceState::Error {
            reason
        } else {
            None
        };
        let n = self.conn().execute(
            "UPDATE sources SET state = ?2, state_reason = ?3, updated_at = ?4 WHERE id = ?1",
            params![id.to_string(), state.as_str(), reason, now()],
        )?;
        if n == 0 {
            return Err(StoreError::NoSuchSource(id.to_string()));
        }
        Ok(())
    }

    /// Replaces a source's credentials (write-only, like every secret).
    pub fn set_source_credentials(
        &self,
        id: SourceId,
        credentials: &S3Credentials,
    ) -> Result<(), StoreError> {
        let name = credentials_secret_name(id);
        let (nonce, ct) = self.cipher.seal(&name, &credentials.encode())?;
        let now = now();
        let mut conn = self.conn();
        let tx = conn.transaction()?;
        let n = tx.execute(
            "UPDATE sources SET credentials_secret = ?2, updated_at = ?3 WHERE id = ?1",
            params![id.to_string(), name, now],
        )?;
        if n == 0 {
            return Err(StoreError::NoSuchSource(id.to_string()));
        }
        tx.execute(
            "INSERT INTO secrets (name, nonce, ciphertext, created_at, updated_at) \
             VALUES (?1, ?2, ?3, ?4, ?4) \
             ON CONFLICT (name) DO UPDATE SET nonce = excluded.nonce, \
             ciphertext = excluded.ciphertext, updated_at = excluded.updated_at",
            params![name, nonce.as_slice(), ct, now],
        )?;
        tx.commit()?;
        Ok(())
    }

    /// Decrypts a source's credentials: the S3 connector's access path to the secret store.
    /// `None` for a source without credentials (anonymous access).
    ///
    /// This is the only way to read a secret from outside this crate, and only for the secret a
    /// source names. The API must never call it: secrets are write-only (ADR 0001, section 3).
    pub fn source_credentials(&self, id: SourceId) -> Result<Option<S3Credentials>, StoreError> {
        let source = self
            .source(id)?
            .ok_or_else(|| StoreError::NoSuchSource(id.to_string()))?;
        let Some(name) = source.credentials_secret else {
            return Ok(None);
        };
        let plain = self
            .read_sealed(&name)?
            .ok_or_else(|| StoreError::InvalidSource(format!("secret {name:?} is missing")))?;
        S3Credentials::decode(&name, &plain).map(Some)
    }
}

#[cfg(test)]
mod tests {
    use doewood_core::MasterKey;

    use super::*;

    fn store() -> Store {
        Store::open_in_memory(&MasterKey::from_bytes([7; 32])).unwrap()
    }

    fn creds(token: Option<&str>) -> S3Credentials {
        S3Credentials::new("test-key-id", "test-secret-value", token.map(str::to_owned)).unwrap()
    }

    #[test]
    fn sources_round_trip() {
        let store = store();
        let mut new = NewSource::new("http://127.0.0.1:9000", "docs");
        new.prefix = "in/".into();
        new.url_style = UrlStyle::VirtualHost;
        new.max_object_bytes = Some(1024);
        new.scan_interval = Duration::from_mins(1);
        let id = store.add_source(&new, Some(&creds(None))).unwrap();
        let got = store.source(id).unwrap().unwrap();
        assert_eq!(got.endpoint, "http://127.0.0.1:9000");
        assert_eq!(got.prefix, "in/");
        assert_eq!(got.url_style, UrlStyle::VirtualHost);
        assert_eq!(got.max_object_bytes(), 1024);
        assert_eq!(got.region(), "us-east-1");
        assert_eq!(got.state, SourceState::Active);
        assert_eq!(got.scan_interval, Duration::from_mins(1));
        assert_eq!(
            got.credentials_secret.as_deref(),
            Some(&*format!("s3/{id}"))
        );
        assert_eq!(store.sources().unwrap(), [got]);
        // The same endpoint, bucket and prefix is refused.
        assert!(store.add_source(&new, None).is_err());
        assert!(store.source(SourceId::generate()).unwrap().is_none());
    }

    #[test]
    fn defaults_and_validation() {
        let store = store();
        let id = store
            .add_source(&NewSource::new("https://s3.example.com", "b"), None)
            .unwrap();
        let got = store.source(id).unwrap().unwrap();
        assert_eq!(got.max_object_bytes(), DEFAULT_MAX_OBJECT_BYTES);
        assert_eq!(got.scan_interval, DEFAULT_SCAN_INTERVAL);
        assert_eq!(store.source_credentials(id).unwrap(), None);
        for bad in [
            NewSource::new("ftp://x", "b"),
            NewSource::new("http://x", ""),
            NewSource {
                scan_interval: Duration::ZERO,
                ..NewSource::new("http://x", "b")
            },
            NewSource {
                max_object_bytes: Some(0),
                ..NewSource::new("http://x", "b")
            },
        ] {
            assert!(store.add_source(&bad, None).is_err(), "{bad:?}");
        }
    }

    #[test]
    fn states_carry_a_reason_only_in_error() {
        let store = store();
        let id = store
            .add_source(&NewSource::new("http://x", "b"), None)
            .unwrap();
        store
            .set_source_state(id, SourceState::Error, Some("403 AccessDenied"))
            .unwrap();
        let got = store.source(id).unwrap().unwrap();
        assert_eq!(got.state, SourceState::Error);
        assert_eq!(got.state_reason.as_deref(), Some("403 AccessDenied"));
        store
            .set_source_state(id, SourceState::Paused, Some("ignored"))
            .unwrap();
        let got = store.source(id).unwrap().unwrap();
        assert_eq!((got.state, got.state_reason), (SourceState::Paused, None));
        assert!(matches!(
            store.set_source_state(SourceId::generate(), SourceState::Active, None),
            Err(StoreError::NoSuchSource(_))
        ));
    }

    #[test]
    fn credentials_are_sealed_and_revealed_only_per_source() {
        let store = store();
        let id = store
            .add_source(&NewSource::new("http://x", "b"), Some(&creds(None)))
            .unwrap();
        let got = store.source_credentials(id).unwrap().unwrap();
        assert_eq!(got.access_key_id(), "test-key-id");
        assert_eq!(got.secret_access_key(), "test-secret-value");
        assert_eq!(got.session_token(), None);
        let debug = format!("{got:?}");
        assert!(
            !debug.contains("test-key-id") && !debug.contains("test-secret"),
            "{debug}"
        );
        // Sealed at rest.
        let ct: Vec<u8> = store
            .raw()
            .query_row(
                "SELECT ciphertext FROM secrets WHERE name = ?1",
                [format!("s3/{id}")],
                |r| r.get(0),
            )
            .unwrap();
        assert!(!ct.windows(17).any(|w| w == b"test-secret-value"));
        // Replaced, with a session token.
        store
            .set_source_credentials(id, &creds(Some("test-session-token")))
            .unwrap();
        let got = store.source_credentials(id).unwrap().unwrap();
        assert_eq!(got.session_token(), Some("test-session-token"));
        assert!(store.secret_exists(&format!("s3/{id}")).unwrap());
    }

    #[test]
    fn credentials_are_validated() {
        assert!(S3Credentials::new("", "s", None).is_err());
        assert!(S3Credentials::new("k", "", None).is_err());
        assert!(S3Credentials::new("k", "s\nx", None).is_err());
        assert!(S3Credentials::new("k", "s", Some("t\r".into())).is_err());
        let c = creds(Some("tok"));
        assert_eq!(S3Credentials::decode("n", &c.encode()).unwrap(), c);
        assert!(S3Credentials::decode("n", b"garbage").is_err());
    }

    #[test]
    fn migration_maps_running_to_active() {
        // A schema-1 file with a `running` source and its progress keeps both.
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join(crate::STORE_FILE);
        {
            let conn = rusqlite::Connection::open(&path).unwrap();
            conn.execute_batch(include_str!("../migrations/0001_init.sql"))
                .unwrap();
            conn.pragma_update(None, "user_version", 1).unwrap();
            let id = SourceId::generate().to_string();
            conn.execute(
                "INSERT INTO sources (id, endpoint, bucket, state, created_at) \
                 VALUES (?1, 'http://m', 'b', 'running', 5)",
                [&id],
            )
            .unwrap();
            conn.execute(
                "INSERT INTO objects_seen (source_id, key, size, updated_at) VALUES (?1, 'k', 1, 0)",
                [&id],
            )
            .unwrap();
        }
        let store = Store::open(&path, &MasterKey::from_bytes([7; 32])).unwrap();
        let sources = store.sources().unwrap();
        assert_eq!(sources.len(), 1);
        assert_eq!(sources[0].state, SourceState::Active);
        assert_eq!(sources[0].url_style, UrlStyle::Path);
        let n: i64 = store
            .raw()
            .query_row("SELECT count(*) FROM objects_seen", [], |r| r.get(0))
            .unwrap();
        assert_eq!(n, 1);
        // The foreign key from objects_seen still points at the rebuilt table.
        store.raw().execute("DELETE FROM sources", []).unwrap();
        let n: i64 = store
            .raw()
            .query_row("SELECT count(*) FROM objects_seen", [], |r| r.get(0))
            .unwrap();
        assert_eq!(n, 0);
    }
}
