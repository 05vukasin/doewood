//! Sources (ADR 0001, section 5): an S3-compatible endpoint, a bucket and an optional prefix,
//! with their state, schedule and options, and the credentials sealed in `secrets`.

use std::fmt;
use std::str::FromStr;
use std::time::Duration;

use doewood_core::SourceId;
use rusqlite::{OptionalExtension as _, Row, params};
use zeroize::Zeroizing;

use super::sqs::{self, SqsInlet};
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

/// Which documents of a source get an LLM summary (ADR 0001, section 6). Ingest always stores
/// the record first; summaries then go through the instance's LLM queue.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub enum LlmMode {
    /// None (the default: no LLM spend without opting in).
    #[default]
    Off,
    /// Every document.
    All,
    /// Only documents of these kinds (`invoice`, `contract`, ...).
    Kinds(Vec<String>),
    /// None automatically; only when asked through the API.
    OnDemand,
}

impl LlmMode {
    /// `off`, `all`, `kinds` or `on_demand`.
    pub fn name(&self) -> &'static str {
        match self {
            Self::Off => "off",
            Self::All => "all",
            Self::Kinds(_) => "kinds",
            Self::OnDemand => "on_demand",
        }
    }
}

/// Default of [`LlmPolicy::concurrency`].
pub const DEFAULT_LLM_CONCURRENCY: u32 = 2;
/// Largest [`LlmPolicy::concurrency`].
pub const MAX_LLM_CONCURRENCY: u32 = 64;
/// Most kinds a [`LlmMode::Kinds`] policy lists.
pub const MAX_LLM_KINDS: usize = 100;

/// A source's LLM policy.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LlmPolicy {
    /// Which documents are summarized.
    pub mode: LlmMode,
    /// Summaries of this source in flight at once (1 to [`MAX_LLM_CONCURRENCY`]).
    pub concurrency: u32,
    /// Tokens (prompt plus completion) this source may spend per UTC day; `None` is no limit.
    pub daily_token_budget: Option<u64>,
}

impl Default for LlmPolicy {
    fn default() -> Self {
        Self {
            mode: LlmMode::Off,
            concurrency: DEFAULT_LLM_CONCURRENCY,
            daily_token_budget: None,
        }
    }
}

impl LlmPolicy {
    /// Checks the policy: a concurrency from 1 to 64, a positive budget if any, and for
    /// `kinds` 1 to 100 kind names of 1 to 64 ASCII letters, digits, `_`, `-` or `.`.
    pub fn validate(&self) -> Result<(), StoreError> {
        let bad = |m: String| Err(StoreError::InvalidSource(m));
        if !(1..=MAX_LLM_CONCURRENCY).contains(&self.concurrency) {
            return bad(format!(
                "the LLM concurrency must be 1 to {MAX_LLM_CONCURRENCY}"
            ));
        }
        if self.daily_token_budget == Some(0) {
            return bad("a daily token budget must be positive (unset means no limit)".into());
        }
        if let LlmMode::Kinds(kinds) = &self.mode {
            if kinds.is_empty() || kinds.len() > MAX_LLM_KINDS {
                return bad(format!("a kinds policy lists 1 to {MAX_LLM_KINDS} kinds"));
            }
            if let Some(k) = kinds.iter().find(|k| !valid_kind(k)) {
                return bad(format!(
                    "kind {k:?} is not 1 to 64 ASCII letters, digits, '_', '-' or '.'"
                ));
            }
        }
        Ok(())
    }

    /// Whether ingest queues a summary for a document of `kind`: `all` queues every
    /// document, `kinds` the listed ones (exact match), `off` and `on_demand` none.
    pub fn queues(&self, kind: Option<&str>) -> bool {
        match &self.mode {
            LlmMode::All => true,
            LlmMode::Kinds(kinds) => kind.is_some_and(|k| kinds.iter().any(|x| x == k)),
            LlmMode::Off | LlmMode::OnDemand => false,
        }
    }

    /// Whether `Summarize` may read this source's objects: every mode but `off`.
    pub fn allows_on_demand(&self) -> bool {
        self.mode != LlmMode::Off
    }

    /// The policy as stored in `sources.policy_json`.
    pub fn to_json(&self) -> serde_json::Value {
        let mut v = serde_json::json!({
            "mode": self.mode.name(),
            "concurrency": self.concurrency,
        });
        if let LlmMode::Kinds(kinds) = &self.mode {
            v["kinds"] = serde_json::json!(kinds);
        }
        if let Some(b) = self.daily_token_budget {
            v["daily_token_budget"] = serde_json::json!(b);
        }
        v
    }

    /// Reads a stored policy; `{}` (a source added before policies existed) is the default.
    pub fn from_json(value: &serde_json::Value) -> Result<Self, StoreError> {
        let bad = || StoreError::InvalidSource("stored LLM policy is malformed".to_owned());
        let obj = value.as_object().ok_or_else(bad)?;
        let mut policy = Self::default();
        if let Some(c) = obj.get("concurrency") {
            policy.concurrency = c
                .as_u64()
                .and_then(|c| u32::try_from(c).ok())
                .ok_or_else(bad)?;
        }
        if let Some(b) = obj.get("daily_token_budget") {
            policy.daily_token_budget = Some(b.as_u64().ok_or_else(bad)?);
        }
        policy.mode = match obj.get("mode") {
            None => LlmMode::Off,
            Some(m) => match m.as_str().ok_or_else(bad)? {
                "off" => LlmMode::Off,
                "all" => LlmMode::All,
                "on_demand" => LlmMode::OnDemand,
                "kinds" => LlmMode::Kinds(
                    obj.get("kinds")
                        .and_then(serde_json::Value::as_array)
                        .ok_or_else(bad)?
                        .iter()
                        .map(|k| k.as_str().map(str::to_owned).ok_or_else(bad))
                        .collect::<Result<_, _>>()?,
                ),
                _ => return Err(bad()),
            },
        };
        Ok(policy)
    }
}

/// Whether `k` is a valid document kind name for a policy.
pub fn valid_kind(k: &str) -> bool {
    (1..=64).contains(&k.len())
        && k.bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-' | b'.'))
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
    /// The initial state: active or paused.
    pub state: SourceState,
    /// Which documents get an LLM summary.
    pub llm_policy: LlmPolicy,
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
            llm_policy: LlmPolicy::default(),
        }
    }

    fn validate(&self) -> Result<(), StoreError> {
        if !matches!(self.state, SourceState::Active | SourceState::Paused) {
            return Err(StoreError::InvalidSource(
                "a new source is active or paused".to_owned(),
            ));
        }
        self.llm_policy.validate()?;
        let bad = |m: &str| Err(StoreError::InvalidSource(m.to_owned()));
        let lower = self.endpoint.to_ascii_lowercase();
        if !(lower.starts_with("http://") || lower.starts_with("https://")) {
            return bad("the endpoint must be an http:// or https:// URL");
        }
        let authority = self.endpoint.split_once("://").map_or("", |(_, rest)| {
            rest.split(['/', '?', '#']).next().unwrap_or("")
        });
        if authority.is_empty() {
            return bad("the endpoint has no host");
        }
        if authority.contains('@') {
            // Credentials belong in the sealed secret, never in a URL that gets logged.
            return bad("the endpoint must not contain credentials");
        }
        if self.bucket.is_empty() || self.bucket.len() > 255 {
            return bad("the bucket name must be 1 to 255 characters");
        }
        if self.prefix.len() > 1024 {
            return bad("the prefix is longer than 1024 bytes");
        }
        check_options(self.scan_interval, self.max_object_bytes)
    }
}

fn check_options(scan_interval: Duration, max_object_bytes: Option<u64>) -> Result<(), StoreError> {
    if scan_interval.as_secs() == 0 {
        return Err(StoreError::InvalidSource(
            "the scan interval must be at least one second".to_owned(),
        ));
    }
    if max_object_bytes == Some(0) {
        return Err(StoreError::InvalidSource(
            "the object size cap must be positive".to_owned(),
        ));
    }
    Ok(())
}

/// Changes to a source's options. `None` leaves a field as it is. The endpoint, bucket and
/// prefix never change: they are the source's identity (and its claim).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SourcePatch {
    /// The signing region; `Some(None)` goes back to the default.
    pub region: Option<Option<String>>,
    /// The URL style.
    pub url_style: Option<UrlStyle>,
    /// The scan interval.
    pub scan_interval: Option<Duration>,
    /// The GetObject size cap; `Some(None)` goes back to the default.
    pub max_object_bytes: Option<Option<u64>>,
    /// The LLM policy.
    pub llm_policy: Option<LlmPolicy>,
}

/// How many objects of a source are in each ingest status (`objects_seen.status`).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ObjectCounts {
    /// Seen, not yet ingested.
    pub pending: u64,
    /// Ingested.
    pub done: u64,
    /// Ingest failed.
    pub failed: u64,
    /// Skipped by ingest (not a supported file).
    pub skipped: u64,
    /// Gone from the bucket.
    pub deleted: u64,
}

impl ObjectCounts {
    /// Objects still in the bucket.
    pub fn live(&self) -> u64 {
        self.pending + self.done + self.failed + self.skipped
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
    /// Which documents get an LLM summary.
    pub llm_policy: LlmPolicy,
    /// When it was added (Unix seconds).
    pub created_at: i64,
    /// When it last changed (Unix seconds).
    pub updated_at: i64,
    /// Its SQS event inlet, if one is set.
    pub sqs: Option<SqsInlet>,
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
        let updated_at = r.get("updated_at")?;
        let policy: String = r.get("policy_json")?;
        let sqs: Option<String> = r.get("sqs_json")?;
        let sqs_credentials: bool = r.get("sqs_credentials")?;
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
                llm_policy: LlmPolicy::from_json(&serde_json::from_str(&policy).map_err(
                    |_| StoreError::InvalidSource("stored LLM policy is not JSON".to_owned()),
                )?)?,
                created_at,
                updated_at,
                sqs: sqs
                    .map(|text| {
                        let value = serde_json::from_str(&text).map_err(|_| {
                            StoreError::InvalidSource("stored SQS inlet is not JSON".to_owned())
                        })?;
                        SqsInlet::from_json(&value, sqs_credentials)
                    })
                    .transpose()?,
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

    pub(crate) fn encode(&self) -> Zeroizing<Vec<u8>> {
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

    pub(crate) fn decode(name: &str, bytes: &[u8]) -> Result<Self, StoreError> {
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

/// The columns of a source, with its SQS inlet (`settings` row `sqs_inlet/<id>`, secret
/// `sqs/<id>`): every query reads `FROM sources`.
const SOURCE_COLUMNS: &str = "id, endpoint, bucket, prefix, region, url_style, \
    credentials_secret, max_object_bytes, policy_json, state, state_reason, scan_interval_s, \
    created_at, updated_at, \
    (SELECT value_json FROM settings WHERE key = 'sqs_inlet/' || sources.id) AS sqs_json, \
    EXISTS (SELECT 1 FROM secrets WHERE name = 'sqs/' || sources.id) AS sqs_credentials";

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
        tx.prepare_cached(
            "INSERT INTO sources (id, endpoint, bucket, prefix, region, url_style, \
             credentials_secret, max_object_bytes, state, scan_interval_s, created_at, updated_at, \
             policy_json) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?11, ?12)",
        )
        .and_then(|mut cached| {
            cached.execute(params![
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
                source.llm_policy.to_json().to_string(),
            ])
        })?;
        if let Some((name, nonce, ct)) = &sealed {
            tx.prepare_cached(
                "INSERT INTO secrets (name, nonce, ciphertext, created_at, updated_at) \
                 VALUES (?1, ?2, ?3, ?4, ?4) \
                 ON CONFLICT (name) DO UPDATE SET nonce = excluded.nonce, \
                 ciphertext = excluded.ciphertext, updated_at = excluded.updated_at",
            )
            .and_then(|mut cached| cached.execute(params![name, nonce.as_slice(), ct, now]))?;
        }
        tx.commit()?;
        Ok(id)
    }

    /// All sources, oldest first.
    pub fn sources(&self) -> Result<Vec<Source>, StoreError> {
        let conn = self.conn();
        let mut stmt = conn.prepare_cached(&format!(
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
            .prepare_cached(&format!(
                "SELECT {SOURCE_COLUMNS} FROM sources WHERE id = ?1"
            ))
            .and_then(|mut cached| cached.query_row([id.to_string()], Source::from_row))
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
        let n = self
            .conn()
            .prepare_cached(
                "UPDATE sources SET state = ?2, state_reason = ?3, updated_at = ?4 WHERE id = ?1",
            )
            .and_then(|mut cached| {
                cached.execute(params![id.to_string(), state.as_str(), reason, now()])
            })?;
        if n == 0 {
            return Err(StoreError::NoSuchSource(id.to_string()));
        }
        Ok(())
    }

    /// Sets the state a scanner reports ([`SourceState::Active`] or [`SourceState::Error`]),
    /// but only while the source is still scanned: a pause or removal made meanwhile (through
    /// the API, while a pass was failing) is never overwritten. Returns whether it was set.
    pub fn set_scanned_source_state(
        &self,
        id: SourceId,
        state: SourceState,
        reason: Option<&str>,
    ) -> Result<bool, StoreError> {
        debug_assert!(state.is_scanned(), "a scanner only reports active or error");
        let reason = if state == SourceState::Error {
            reason
        } else {
            None
        };
        let n = self
            .conn()
            .prepare_cached(
                "UPDATE sources SET state = ?2, state_reason = ?3, updated_at = ?4 \
             WHERE id = ?1 AND state IN ('active', 'error')",
            )
            .and_then(|mut cached| {
                cached.execute(params![id.to_string(), state.as_str(), reason, now()])
            })?;
        if n == 0 && self.source(id)?.is_none() {
            return Err(StoreError::NoSuchSource(id.to_string()));
        }
        Ok(n == 1)
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
        let n = tx
            .prepare_cached(
                "UPDATE sources SET credentials_secret = ?2, updated_at = ?3 WHERE id = ?1",
            )
            .and_then(|mut cached| cached.execute(params![id.to_string(), name, now]))?;
        if n == 0 {
            return Err(StoreError::NoSuchSource(id.to_string()));
        }
        tx.prepare_cached(
            "INSERT INTO secrets (name, nonce, ciphertext, created_at, updated_at) \
             VALUES (?1, ?2, ?3, ?4, ?4) \
             ON CONFLICT (name) DO UPDATE SET nonce = excluded.nonce, \
             ciphertext = excluded.ciphertext, updated_at = excluded.updated_at",
        )
        .and_then(|mut cached| cached.execute(params![name, nonce.as_slice(), ct, now]))?;
        tx.commit()?;
        Ok(())
    }

    /// The source with this endpoint, bucket and prefix (compared exactly as stored).
    pub fn source_by_key(
        &self,
        endpoint: &str,
        bucket: &str,
        prefix: &str,
    ) -> Result<Option<Source>, StoreError> {
        self.conn()
            .prepare_cached(&format!(
                "SELECT {SOURCE_COLUMNS} FROM sources \
                     WHERE endpoint = ?1 AND bucket = ?2 AND prefix = ?3"
            ))
            .and_then(|mut cached| {
                cached.query_row(params![endpoint, bucket, prefix], Source::from_row)
            })
            .optional()?
            .transpose()
    }

    /// Applies a patch to a source's options and returns the source as it is now.
    pub fn update_source(&self, id: SourceId, patch: &SourcePatch) -> Result<Source, StoreError> {
        let mut conn = self.conn();
        let tx = conn.transaction()?;
        let select = format!("SELECT {SOURCE_COLUMNS} FROM sources WHERE id = ?1");
        let current = tx
            .query_row(&select, [id.to_string()], Source::from_row)
            .optional()?
            .transpose()?
            .ok_or_else(|| StoreError::NoSuchSource(id.to_string()))?;
        let region = patch.region.clone().unwrap_or(current.region);
        let url_style = patch.url_style.unwrap_or(current.url_style);
        let interval = patch.scan_interval.unwrap_or(current.scan_interval);
        let max = patch.max_object_bytes.unwrap_or(current.max_object_bytes);
        let policy = patch.llm_policy.clone().unwrap_or(current.llm_policy);
        check_options(interval, max)?;
        policy.validate()?;
        let max = max
            .map(i64::try_from)
            .transpose()
            .map_err(|_| StoreError::InvalidSource("object size cap too large".to_owned()))?;
        let interval = i64::try_from(interval.as_secs())
            .map_err(|_| StoreError::InvalidSource("scan interval too large".to_owned()))?;
        tx.prepare_cached(
            "UPDATE sources SET region = ?2, url_style = ?3, scan_interval_s = ?4, \
             max_object_bytes = ?5, policy_json = ?6, updated_at = ?7 WHERE id = ?1",
        )
        .and_then(|mut cached| {
            cached.execute(params![
                id.to_string(),
                region,
                url_style.as_str(),
                interval,
                max,
                policy.to_json().to_string(),
                now()
            ])
        })?;
        let updated = tx.query_row(&select, [id.to_string()], Source::from_row)??;
        tx.commit()?;
        Ok(updated)
    }

    /// Removes a source's credentials: it is then read anonymously.
    pub fn clear_source_credentials(&self, id: SourceId) -> Result<(), StoreError> {
        let mut conn = self.conn();
        let tx = conn.transaction()?;
        let n = tx
            .prepare_cached(
                "UPDATE sources SET credentials_secret = NULL, updated_at = ?2 WHERE id = ?1",
            )
            .and_then(|mut cached| cached.execute(params![id.to_string(), now()]))?;
        if n == 0 {
            return Err(StoreError::NoSuchSource(id.to_string()));
        }
        tx.prepare_cached("DELETE FROM secrets WHERE name = ?1")
            .and_then(|mut cached| cached.execute([credentials_secret_name(id)]))?;
        tx.commit()?;
        Ok(())
    }

    /// Deletes a source with its progress, its checkpoint and its credentials, in one
    /// transaction. Returns whether it existed.
    pub fn delete_source(&self, id: SourceId) -> Result<bool, StoreError> {
        let mut conn = self.conn();
        let tx = conn.transaction()?;
        // `objects_seen`, `scan_checkpoints` and `jobs` go with it (ON DELETE CASCADE).
        let n = tx
            .prepare_cached("DELETE FROM sources WHERE id = ?1")
            .and_then(|mut cached| cached.execute([id.to_string()]))?;
        tx.prepare_cached("DELETE FROM secrets WHERE name = ?1")
            .and_then(|mut cached| cached.execute([credentials_secret_name(id)]))?;
        // The SQS inlet and its credentials go with it.
        tx.prepare_cached("DELETE FROM settings WHERE key = ?1")
            .and_then(|mut cached| cached.execute([sqs::settings_key(id)]))?;
        tx.prepare_cached("DELETE FROM secrets WHERE name = ?1")
            .and_then(|mut cached| cached.execute([sqs::secret_name(id)]))?;
        tx.commit()?;
        Ok(n > 0)
    }

    /// How many of a source's objects are in each ingest status.
    pub fn object_counts(&self, id: SourceId) -> Result<ObjectCounts, StoreError> {
        let conn = self.conn();
        let mut stmt =
            conn.prepare_cached("SELECT status, n FROM object_counts WHERE source_id = ?1")?;
        let mut counts = ObjectCounts::default();
        let rows = stmt.query_map([id.to_string()], |r| {
            Ok((r.get::<_, String>(0)?, r.get::<_, i64>(1)?))
        })?;
        for row in rows {
            let (status, n) = row?;
            let n = u64::try_from(n).unwrap_or(0);
            match status.as_str() {
                "pending" => counts.pending = n,
                "done" => counts.done = n,
                "failed" => counts.failed = n,
                "skipped" => counts.skipped = n,
                _ => counts.deleted = n,
            }
        }
        Ok(counts)
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
            NewSource::new("http://user:pw@x", "b"),
            NewSource::new("http:///b", "b"),
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
    fn a_scanner_state_never_overwrites_a_pause() {
        let store = store();
        let id = store
            .add_source(&NewSource::new("http://x", "b"), None)
            .unwrap();
        assert!(
            store
                .set_scanned_source_state(id, SourceState::Error, Some("timeout"))
                .unwrap()
        );
        assert_eq!(store.source(id).unwrap().unwrap().state, SourceState::Error);
        store
            .set_source_state(id, SourceState::Paused, None)
            .unwrap();
        // A pass that was failing while the source was paused reports late: the pause stays.
        assert!(
            !store
                .set_scanned_source_state(id, SourceState::Error, Some("timeout"))
                .unwrap()
        );
        assert!(
            !store
                .set_scanned_source_state(id, SourceState::Active, None)
                .unwrap()
        );
        let got = store.source(id).unwrap().unwrap();
        assert_eq!((got.state, got.state_reason), (SourceState::Paused, None));
        assert!(matches!(
            store.set_scanned_source_state(SourceId::generate(), SourceState::Active, None),
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
    fn updates_policies_and_deletion() {
        let store = store();
        let id = store
            .add_source(&NewSource::new("http://x", "b"), Some(&creds(None)))
            .unwrap();
        let got = store.source(id).unwrap().unwrap();
        assert_eq!(got.llm_policy, LlmPolicy::default());
        let policy = LlmPolicy {
            mode: LlmMode::Kinds(vec!["invoice".into(), "contract".into()]),
            concurrency: 4,
            daily_token_budget: Some(100_000),
        };
        let patch = SourcePatch {
            region: Some(Some("eu-1".into())),
            url_style: Some(UrlStyle::VirtualHost),
            scan_interval: Some(Duration::from_secs(30)),
            max_object_bytes: Some(Some(10)),
            llm_policy: Some(policy.clone()),
        };
        let got = store.update_source(id, &patch).unwrap();
        assert_eq!(got.region(), "eu-1");
        assert_eq!(got.url_style, UrlStyle::VirtualHost);
        assert_eq!(got.scan_interval, Duration::from_secs(30));
        assert_eq!(got.max_object_bytes(), 10);
        assert_eq!(got.llm_policy, policy);
        assert_eq!(LlmPolicy::from_json(&policy.to_json()).unwrap(), policy);
        // Back to the defaults; an empty patch changes nothing.
        let got = store
            .update_source(
                id,
                &SourcePatch {
                    region: Some(None),
                    max_object_bytes: Some(None),
                    ..SourcePatch::default()
                },
            )
            .unwrap();
        assert_eq!(
            (got.region(), got.max_object_bytes()),
            ("us-east-1", DEFAULT_MAX_OBJECT_BYTES)
        );
        let same = store.update_source(id, &SourcePatch::default()).unwrap();
        assert_eq!(same.llm_policy, policy);
        for bad in [
            SourcePatch {
                scan_interval: Some(Duration::ZERO),
                ..SourcePatch::default()
            },
            SourcePatch {
                llm_policy: Some(LlmPolicy {
                    concurrency: 0,
                    ..LlmPolicy::default()
                }),
                ..SourcePatch::default()
            },
            SourcePatch {
                llm_policy: Some(LlmPolicy {
                    mode: LlmMode::Kinds(vec![]),
                    ..LlmPolicy::default()
                }),
                ..SourcePatch::default()
            },
        ] {
            assert!(store.update_source(id, &bad).is_err(), "{bad:?}");
        }
        assert!(matches!(
            store.update_source(SourceId::generate(), &SourcePatch::default()),
            Err(StoreError::NoSuchSource(_))
        ));
        assert_eq!(
            store
                .source_by_key("http://x", "b", "")
                .unwrap()
                .map(|s| s.id),
            Some(id)
        );
        assert!(
            store
                .source_by_key("http://x", "b", "p/")
                .unwrap()
                .is_none()
        );
        // Credentials can be cleared; deleting removes the source and its secret.
        store.clear_source_credentials(id).unwrap();
        assert_eq!(store.source_credentials(id).unwrap(), None);
        store.set_source_credentials(id, &creds(None)).unwrap();
        assert!(store.delete_source(id).unwrap());
        assert!(!store.delete_source(id).unwrap());
        assert!(store.source(id).unwrap().is_none());
        assert!(!store.secret_exists(&format!("s3/{id}")).unwrap());
        assert_eq!(
            LlmPolicy::from_json(&serde_json::json!({})).unwrap(),
            LlmPolicy::default()
        );
        assert!(LlmPolicy::from_json(&serde_json::json!({"mode": "sometimes"})).is_err());
    }

    #[test]
    fn object_counts_by_status() {
        let store = store();
        let id = store
            .add_source(&NewSource::new("http://x", "b"), None)
            .unwrap();
        assert_eq!(store.object_counts(id).unwrap(), ObjectCounts::default());
        let obj = |k: &str| {
            crate::ObjectChange::Upsert(crate::SeenObject {
                key: k.into(),
                etag: None,
                size: 1,
                last_modified: None,
            })
        };
        store
            .record_objects(id, &[obj("a"), obj("b"), obj("c")], None)
            .unwrap();
        store
            .record_objects(id, &[crate::ObjectChange::Delete { key: "c".into() }], None)
            .unwrap();
        let counts = store.object_counts(id).unwrap();
        assert_eq!((counts.pending, counts.deleted, counts.live()), (2, 1, 2));
    }

    /// `(status, n)` of a source's rows of `table`, counted the slow way, without zeros.
    fn counted(store: &Store, table: &str, id: SourceId) -> Vec<(String, i64)> {
        let conn = store.raw();
        let mut stmt = conn
            .prepare(&format!(
                "SELECT status, count(*) FROM {table} WHERE source_id = ?1 \
                 GROUP BY status ORDER BY status"
            ))
            .unwrap();
        stmt.query_map([id.to_string()], |r| Ok((r.get(0)?, r.get(1)?)))
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap()
    }

    /// `(status, n)` of a source's rows of a counter table, without zeros.
    fn kept(store: &Store, table: &str, id: SourceId) -> Vec<(String, i64)> {
        let conn = store.raw();
        let mut stmt = conn
            .prepare(&format!(
                "SELECT status, n FROM {table} WHERE source_id = ?1 AND n <> 0 ORDER BY status"
            ))
            .unwrap();
        stmt.query_map([id.to_string()], |r| Ok((r.get(0)?, r.get(1)?)))
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap()
    }

    #[test]
    fn status_counters_follow_every_change() {
        use crate::{IngestOutcome, LlmJobStatus, ObjectChange, SeenObject};
        let store = store();
        let add = |b: &str| {
            store
                .add_source(&NewSource::new("http://x", b), None)
                .unwrap()
        };
        let (a, b) = (add("a"), add("b"));
        let obj = |k: &str, etag: &str| {
            ObjectChange::Upsert(SeenObject {
                key: k.into(),
                etag: Some(etag.into()),
                size: 1,
                last_modified: None,
            })
        };
        let keys = ["k1", "k2", "k3", "k4", "k5"];
        let all: Vec<_> = keys.iter().map(|k| obj(k, "e1")).collect();
        store.record_objects(a, &all, None).unwrap();
        store.record_objects(b, &all[..2], None).unwrap();
        let done = IngestOutcome::Done { blake3: [1; 32] };
        assert!(store.finish_object(a, "k1", Some("e1"), &done).unwrap());
        let failed = IngestOutcome::Failed {
            error: "boom".into(),
            attempts: 3,
        };
        assert!(store.finish_object(a, "k2", Some("e1"), &failed).unwrap());
        let skipped = IngestOutcome::Skipped { blake3: [2; 32] };
        assert!(store.finish_object(a, "k3", Some("e1"), &skipped).unwrap());
        // A retry that stays pending, and an outcome for an older version: no change.
        store.note_attempt(a, "k5", Some("e1"), 1, "again").unwrap();
        assert!(!store.finish_object(a, "k5", Some("old"), &done).unwrap());
        store
            .record_objects(a, &[ObjectChange::Delete { key: "k4".into() }], None)
            .unwrap();
        store.record_objects(a, &[obj("k1", "e2")], None).unwrap();
        assert!(store.finish_object(a, "k1", Some("e2"), &done).unwrap());
        assert!(store.finish_object(b, "k1", Some("e1"), &done).unwrap());
        let check = |id| {
            assert_eq!(
                kept(&store, "object_counts", id),
                counted(&store, "objects_seen", id)
            );
            assert_eq!(
                kept(&store, "llm_job_counts", id),
                counted(&store, "llm_jobs", id)
            );
        };
        check(a);
        check(b);
        let c = store.object_counts(a).unwrap();
        assert_eq!(
            (c.pending, c.done, c.failed, c.skipped, c.deleted),
            (1, 1, 1, 1, 1)
        );
        assert_eq!(store.live_object_count(a).unwrap(), 4);
        store.requeue_objects(a, Some(&["k1".to_owned()])).unwrap();
        check(a);
        store.requeue_objects(a, None).unwrap();
        check(a);
        assert_eq!(store.object_counts(a).unwrap().pending, 4);

        // The LLM queue.
        for h in 1..=4u8 {
            store.queue_llm_job(a, &[h; 32], None).unwrap();
        }
        store
            .finish_llm_job(a, &[1; 32], LlmJobStatus::Done, None, true)
            .unwrap();
        store
            .finish_llm_job(a, &[2; 32], LlmJobStatus::Failed, Some("first"), true)
            .unwrap();
        store
            .finish_llm_job(a, &[3; 32], LlmJobStatus::Skipped, Some("empty"), false)
            .unwrap();
        // Queued again after it ended; queued again while pending (no change).
        store.queue_llm_job(a, &[1; 32], None).unwrap();
        store.queue_llm_job(a, &[4; 32], None).unwrap();
        check(a);
        let l = store.llm_counts(a).unwrap();
        assert_eq!(
            (
                l.pending,
                l.done,
                l.failed,
                l.skipped,
                l.last_error.as_deref()
            ),
            (2, 0, 1, 1, Some("first"))
        );

        // A removed source's counters go with it.
        store.queue_llm_job(b, &[9; 32], None).unwrap();
        assert!(store.delete_source(b).unwrap());
        let left: i64 = store
            .raw()
            .query_row(
                "SELECT (SELECT count(*) FROM object_counts WHERE source_id = ?1) \
                 + (SELECT count(*) FROM llm_job_counts WHERE source_id = ?1)",
                [b.to_string()],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(left, 0);
        check(a);
    }

    /// The plan of a query, one detail per line.
    fn plan(store: &Store, sql: &str) -> String {
        let conn = store.raw();
        let mut stmt = conn.prepare(&format!("EXPLAIN QUERY PLAN {sql}")).unwrap();
        stmt.query_map(["s", "k"], |r| r.get::<_, String>(3))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap()
            .join("\n")
    }

    #[test]
    fn hot_queries_use_their_indexes() {
        let store = store();
        // `pending_objects` (as it is written there): the queue index, no sort. Without
        // `INDEXED BY`, SQLite 3.54 reads the primary key here, even after `ANALYZE` of a
        // source whose rows are spread over the five statuses.
        let p = plan(
            &store,
            "SELECT key, etag, size, last_modified FROM objects_seen \
             INDEXED BY objects_seen_pending \
             WHERE source_id = ?1 AND status = 'pending' AND (?2 IS NULL OR key > ?2) \
             ORDER BY key LIMIT 10",
        );
        assert!(p.contains("USING INDEX objects_seen_pending"), "{p}");
        assert!(!p.contains("TEMP B-TREE"), "{p}");
        let indexes: i64 = store
            .raw()
            .query_row(
                "SELECT count(*) FROM sqlite_schema WHERE name = 'objects_seen_by_status'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(indexes, 0, "the duplicate index is gone");
        // `llm_counts`: the newest failure without a sort.
        let p = plan(
            &store,
            "SELECT last_error FROM llm_jobs WHERE source_id = ?1 AND status = 'failed' \
             AND last_error IS NOT NULL AND ?2 = ?2 ORDER BY updated_at DESC LIMIT 1",
        );
        assert!(p.contains("USING INDEX llm_jobs_by_source"), "{p}");
        assert!(!p.contains("TEMP B-TREE"), "{p}");
        // The counters are one primary key range.
        let p = plan(
            &store,
            "SELECT status, n FROM object_counts WHERE source_id = ?1 AND ?2 = ?2",
        );
        assert!(p.contains("USING PRIMARY KEY"), "{p}");
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
