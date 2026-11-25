//! A source's SQS event inlet (ADR 0001, section 5): the queue its S3 event notifications go
//! to, the signing region, an on/off switch and, optionally, credentials of its own.
//!
//! No schema change: the settings are a row of `settings` (`sqs_inlet/<source id>`) and the
//! separate credentials a sealed secret (`sqs/<source id>`). Both go with the source.

use doewood_core::SourceId;
use rusqlite::{OptionalExtension as _, params};

use super::sources::S3Credentials;
use super::{Store, now, write_sealed_tx};
use crate::StoreError;

/// Signing region of a queue whose URL names none (not an AWS host) and that sets none.
pub const SQS_DEFAULT_REGION: &str = "us-east-1";

/// Longest queue URL accepted.
pub const QUEUE_URL_MAX_LEN: usize = 2048;

/// The settings key of a source's inlet.
pub(crate) fn settings_key(id: SourceId) -> String {
    format!("sqs_inlet/{id}")
}

/// The secret name of a source's inlet credentials.
pub(crate) fn secret_name(id: SourceId) -> String {
    format!("sqs/{id}")
}

/// A source's SQS inlet as stored. `Debug` is safe: no secret is part of it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SqsInlet {
    /// The queue URL, `https://sqs.<region>.amazonaws.com/<account>/<queue>`.
    pub queue_url: String,
    /// The signing region, if set.
    pub region: Option<String>,
    /// Whether the inlet polls the queue (while the source is scanned).
    pub enabled: bool,
    /// Whether the inlet has credentials of its own; without, it signs with the source's.
    pub credentials_set: bool,
}

impl SqsInlet {
    /// The signing region: the one set, else the region of an AWS queue URL
    /// (`sqs.<region>.amazonaws.com`), else [`SQS_DEFAULT_REGION`].
    pub fn region(&self) -> &str {
        self.region
            .as_deref()
            .or_else(|| queue_url_region(&self.queue_url))
            .unwrap_or(SQS_DEFAULT_REGION)
    }

    pub(crate) fn from_json(
        value: &serde_json::Value,
        credentials_set: bool,
    ) -> Result<Self, StoreError> {
        let bad = || StoreError::InvalidSource("stored SQS inlet is malformed".to_owned());
        let obj = value.as_object().ok_or_else(bad)?;
        let queue_url = obj
            .get("queue_url")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(bad)?
            .to_owned();
        let region = match obj.get("region") {
            None | Some(serde_json::Value::Null) => None,
            Some(r) => Some(r.as_str().ok_or_else(bad)?.to_owned()),
        };
        let enabled = obj
            .get("enabled")
            .and_then(serde_json::Value::as_bool)
            .ok_or_else(bad)?;
        Ok(Self {
            queue_url,
            region,
            enabled,
            credentials_set,
        })
    }
}

/// What to set for a source's inlet.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SqsInletInput {
    /// The queue URL (see [`valid_queue_url`]).
    pub queue_url: String,
    /// The signing region; `None` for the default (see [`SqsInlet::region`]).
    pub region: Option<String>,
    /// On or off.
    pub enabled: bool,
}

impl SqsInletInput {
    fn validate(&self) -> Result<(), StoreError> {
        valid_queue_url(&self.queue_url)
            .map_err(|why| StoreError::InvalidSource(format!("the queue URL {why}")))?;
        if let Some(r) = &self.region
            && (r.is_empty()
                || r.len() > 64
                || !r
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_')))
        {
            return Err(StoreError::InvalidSource(
                "the SQS region must be 1 to 64 ASCII letters, digits, '-' or '_'".to_owned(),
            ));
        }
        Ok(())
    }
}

/// What to do with an inlet's own credentials.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CredentialsChange {
    /// Keep them (or keep using the source's, if there are none).
    Keep,
    /// Replace them.
    Set(S3Credentials),
    /// Delete them: the source's credentials are used.
    Clear,
}

/// Checks a queue URL: `http(s)://host[:port]/<account>/<queue>` (an optional trailing `/`),
/// without credentials, query or fragment. The account is 1 to 64 ASCII letters or digits
/// (AWS: 12 digits); the queue name 1 to 80 ASCII letters, digits, `-` or `_`, with an optional
/// `.fifo` suffix. On error, says why without echoing the value.
pub fn valid_queue_url(url: &str) -> Result<(), &'static str> {
    if url.is_empty() {
        return Err("is required");
    }
    if url.len() > QUEUE_URL_MAX_LEN {
        return Err("is longer than 2048 bytes");
    }
    if url.contains(|c: char| c.is_whitespace() || c.is_control()) {
        return Err("must not contain spaces or control characters");
    }
    let lower = url.to_ascii_lowercase();
    let rest = if lower.starts_with("https://") {
        &url[8..]
    } else if lower.starts_with("http://") {
        &url[7..]
    } else {
        return Err("must be an http:// or https:// URL");
    };
    if url.contains(['?', '#']) {
        return Err("must not have a query or a fragment");
    }
    let (authority, path) = rest.split_once('/').unwrap_or((rest, ""));
    if authority.contains('@') {
        // Credentials belong in the sealed secret, never in a URL that gets logged.
        return Err("must not contain credentials");
    }
    let host = authority.rsplit_once(':').map_or(authority, |(h, _)| h);
    if host.is_empty() {
        return Err("has no host");
    }
    let path = path.strip_suffix('/').unwrap_or(path);
    let shape = "must be http(s)://host/<account>/<queue name>";
    let Some((account, name)) = path.split_once('/') else {
        return Err(shape);
    };
    if account.is_empty()
        || account.len() > 64
        || !account.bytes().all(|b| b.is_ascii_alphanumeric())
    {
        return Err("has an account that is not 1 to 64 ASCII letters or digits");
    }
    let base = name.strip_suffix(".fifo").unwrap_or(name);
    if base.is_empty()
        || name.len() > 80
        || !base
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_'))
    {
        return Err(
            "has a queue name that is not 1 to 80 ASCII letters, digits, '-' or '_' (optionally ending in .fifo)",
        );
    }
    Ok(())
}

/// The region of an AWS queue URL (`https://sqs.<region>.amazonaws.com[.cn]/...`, or
/// `sqs-fips`); `None` for any other host.
pub fn queue_url_region(url: &str) -> Option<&str> {
    let rest = url.split_once("://")?.1;
    let authority = rest.split('/').next()?;
    let host = authority.rsplit_once(':').map_or(authority, |(h, _)| h);
    let labels: Vec<&str> = host.split('.').collect();
    let aws = match labels.as_slice() {
        [service, region, a, c]
            if a.eq_ignore_ascii_case("amazonaws") && c.eq_ignore_ascii_case("com") =>
        {
            Some((*service, *region))
        }
        [service, region, a, c, cn]
            if a.eq_ignore_ascii_case("amazonaws")
                && c.eq_ignore_ascii_case("com")
                && cn.eq_ignore_ascii_case("cn") =>
        {
            Some((*service, *region))
        }
        _ => None,
    };
    let (service, region) = aws?;
    let ok = (service.eq_ignore_ascii_case("sqs") || service.eq_ignore_ascii_case("sqs-fips"))
        && !region.is_empty()
        && region
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-');
    ok.then_some(region)
}

impl Store {
    /// Sets a source's SQS inlet, with its credentials change, in one transaction. Returns
    /// the inlet as stored.
    pub fn set_sqs_inlet(
        &self,
        id: SourceId,
        input: &SqsInletInput,
        credentials: &CredentialsChange,
    ) -> Result<SqsInlet, StoreError> {
        input.validate()?;
        let key = settings_key(id);
        let name = secret_name(id);
        let sealed = match credentials {
            CredentialsChange::Set(c) => Some(self.cipher.seal(&name, &c.encode())?),
            CredentialsChange::Keep | CredentialsChange::Clear => None,
        };
        let value = serde_json::json!({
            "queue_url": input.queue_url,
            "region": input.region,
            "enabled": input.enabled,
        });
        let now = now();
        let mut conn = self.conn();
        let tx = conn.transaction()?;
        let exists = tx
            .query_row(
                "SELECT 1 FROM sources WHERE id = ?1",
                [id.to_string()],
                |_| Ok(()),
            )
            .optional()?
            .is_some();
        if !exists {
            return Err(StoreError::NoSuchSource(id.to_string()));
        }
        tx.execute(
            "INSERT INTO settings (key, value_json, updated_at) VALUES (?1, ?2, ?3) \
             ON CONFLICT (key) DO UPDATE SET value_json = excluded.value_json, \
             updated_at = excluded.updated_at",
            params![key, value.to_string(), now],
        )?;
        match (credentials, &sealed) {
            (CredentialsChange::Set(_), Some((nonce, ct))) => {
                write_sealed_tx(&tx, &name, nonce.as_slice(), ct, now)?;
            }
            (CredentialsChange::Clear, _) => {
                tx.execute("DELETE FROM secrets WHERE name = ?1", [&name])?;
            }
            _ => {}
        }
        tx.execute(
            "UPDATE sources SET updated_at = ?2 WHERE id = ?1",
            params![id.to_string(), now],
        )?;
        let credentials_set = tx
            .query_row("SELECT 1 FROM secrets WHERE name = ?1", [&name], |_| Ok(()))
            .optional()?
            .is_some();
        tx.commit()?;
        Ok(SqsInlet {
            queue_url: input.queue_url.clone(),
            region: input.region.clone(),
            enabled: input.enabled,
            credentials_set,
        })
    }

    /// Removes a source's SQS inlet and its credentials. Returns whether it had one.
    pub fn remove_sqs_inlet(&self, id: SourceId) -> Result<bool, StoreError> {
        let mut conn = self.conn();
        let tx = conn.transaction()?;
        let exists = tx
            .query_row(
                "SELECT 1 FROM sources WHERE id = ?1",
                [id.to_string()],
                |_| Ok(()),
            )
            .optional()?
            .is_some();
        if !exists {
            return Err(StoreError::NoSuchSource(id.to_string()));
        }
        let n = tx.execute("DELETE FROM settings WHERE key = ?1", [settings_key(id)])?;
        tx.execute("DELETE FROM secrets WHERE name = ?1", [secret_name(id)])?;
        if n > 0 {
            tx.execute(
                "UPDATE sources SET updated_at = ?2 WHERE id = ?1",
                params![id.to_string(), now()],
            )?;
        }
        tx.commit()?;
        Ok(n > 0)
    }

    /// Decrypts the credentials a source's SQS inlet signs with: its own, else the source's,
    /// else `None` (unsigned requests, for local queues). The SQS connector's access path to
    /// the secret store; the API must never call it (ADR 0001, section 3).
    pub fn sqs_credentials(&self, id: SourceId) -> Result<Option<S3Credentials>, StoreError> {
        let name = secret_name(id);
        if let Some(plain) = self.read_sealed(&name)? {
            return S3Credentials::decode(&name, &plain).map(Some);
        }
        self.source_credentials(id)
    }
}

#[cfg(test)]
mod tests {
    use doewood_core::MasterKey;

    use super::*;
    use crate::NewSource;

    const QUEUE: &str = "https://sqs.eu-central-1.amazonaws.com/123456789012/doewood-test-events";

    fn store() -> Store {
        Store::open_in_memory(&MasterKey::from_bytes([3; 32])).unwrap()
    }

    fn creds(id: &str) -> S3Credentials {
        S3Credentials::new(id, "test-sqs-secret-value", None).unwrap()
    }

    fn input(url: &str) -> SqsInletInput {
        SqsInletInput {
            queue_url: url.to_owned(),
            region: None,
            enabled: true,
        }
    }

    #[test]
    fn queue_urls_are_checked() {
        for good in [
            QUEUE,
            "https://sqs.eu-central-1.amazonaws.com/123456789012/doewood-test-events/",
            "http://127.0.0.1:9324/000000000000/q_1",
            "https://sqs.us-east-1.amazonaws.com/123456789012/events.fifo",
            "HTTPS://localhost/queue/name",
        ] {
            assert_eq!(valid_queue_url(good), Ok(()), "{good}");
        }
        for bad in [
            "",
            "ftp://sqs.example/1/q",
            "https://sqs.example/1",
            "https://sqs.example/1/q/extra",
            "https:///1/q",
            "https://user:pw@sqs.example/1/q",
            "https://sqs.example/1/q?x=1",
            "https://sqs.example/1/q#f",
            "https://sqs.example/1/q w",
            "https://sqs.example/1/q.x",
            "https://sqs.example//q",
            "https://sqs.example/1/.fifo",
            &format!("https://sqs.example/1/{}", "q".repeat(81)),
        ] {
            assert!(valid_queue_url(bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn regions_come_from_aws_hosts() {
        assert_eq!(queue_url_region(QUEUE), Some("eu-central-1"));
        assert_eq!(
            queue_url_region("https://sqs.cn-north-1.amazonaws.com.cn/1/q"),
            Some("cn-north-1")
        );
        assert_eq!(
            queue_url_region("https://sqs-fips.us-gov-west-1.amazonaws.com/1/q"),
            Some("us-gov-west-1")
        );
        assert_eq!(queue_url_region("http://127.0.0.1:9324/1/q"), None);
        assert_eq!(
            queue_url_region("https://s3.eu-west-1.amazonaws.com/1/q"),
            None
        );
        let mut inlet = SqsInlet {
            queue_url: "http://127.0.0.1:9324/1/q".into(),
            region: None,
            enabled: true,
            credentials_set: false,
        };
        assert_eq!(inlet.region(), SQS_DEFAULT_REGION);
        inlet.queue_url = QUEUE.into();
        assert_eq!(inlet.region(), "eu-central-1");
        inlet.region = Some("eu-west-3".into());
        assert_eq!(inlet.region(), "eu-west-3");
    }

    #[test]
    fn inlets_round_trip_with_the_source() {
        let store = store();
        let id = store
            .add_source(
                &NewSource::new("https://s3.example", "docs"),
                Some(&creds("test-source-key")),
            )
            .unwrap();
        assert_eq!(store.source(id).unwrap().unwrap().sqs, None);
        // Without credentials of its own, the inlet signs with the source's.
        let set = store
            .set_sqs_inlet(id, &input(QUEUE), &CredentialsChange::Keep)
            .unwrap();
        assert!(!set.credentials_set);
        assert_eq!(store.source(id).unwrap().unwrap().sqs, Some(set));
        assert_eq!(
            store.sqs_credentials(id).unwrap().unwrap().access_key_id(),
            "test-source-key"
        );
        // Its own credentials, sealed; then off.
        let set = store
            .set_sqs_inlet(
                id,
                &SqsInletInput {
                    enabled: false,
                    region: Some("eu-west-3".into()),
                    ..input(QUEUE)
                },
                &CredentialsChange::Set(creds("test-queue-key")),
            )
            .unwrap();
        assert!(set.credentials_set && !set.enabled);
        assert_eq!(store.sources().unwrap()[0].sqs.as_ref(), Some(&set));
        assert_eq!(
            store.sqs_credentials(id).unwrap().unwrap().access_key_id(),
            "test-queue-key"
        );
        let ct: Vec<u8> = store
            .raw()
            .query_row(
                "SELECT ciphertext FROM secrets WHERE name = ?1",
                [secret_name(id)],
                |r| r.get(0),
            )
            .unwrap();
        assert!(!ct.windows(14).any(|w| w == b"test-queue-key"));
        // Keep keeps them; Clear goes back to the source's.
        assert!(
            store
                .set_sqs_inlet(id, &input(QUEUE), &CredentialsChange::Keep)
                .unwrap()
                .credentials_set
        );
        assert!(
            !store
                .set_sqs_inlet(id, &input(QUEUE), &CredentialsChange::Clear)
                .unwrap()
                .credentials_set
        );
        assert_eq!(
            store.sqs_credentials(id).unwrap().unwrap().access_key_id(),
            "test-source-key"
        );
        // Invalid input and unknown sources are refused.
        assert!(
            store
                .set_sqs_inlet(id, &input("https://x/q"), &CredentialsChange::Keep)
                .is_err()
        );
        assert!(
            store
                .set_sqs_inlet(
                    id,
                    &SqsInletInput {
                        region: Some("eu west".into()),
                        ..input(QUEUE)
                    },
                    &CredentialsChange::Keep
                )
                .is_err()
        );
        assert!(matches!(
            store.set_sqs_inlet(
                SourceId::generate(),
                &input(QUEUE),
                &CredentialsChange::Keep
            ),
            Err(StoreError::NoSuchSource(_))
        ));
        // Removed; then removing the source removes the inlet and its secret.
        assert!(store.remove_sqs_inlet(id).unwrap());
        assert!(!store.remove_sqs_inlet(id).unwrap());
        assert_eq!(store.source(id).unwrap().unwrap().sqs, None);
        store
            .set_sqs_inlet(
                id,
                &input(QUEUE),
                &CredentialsChange::Set(creds("test-queue-key")),
            )
            .unwrap();
        assert!(store.delete_source(id).unwrap());
        assert!(store.setting(&settings_key(id)).unwrap().is_none());
        assert!(!store.secret_exists(&secret_name(id)).unwrap());
    }

    #[test]
    fn a_source_without_credentials_signs_nothing() {
        let store = store();
        let id = store
            .add_source(&NewSource::new("http://127.0.0.1:9000", "docs"), None)
            .unwrap();
        store
            .set_sqs_inlet(
                id,
                &input("http://127.0.0.1:9324/1/q"),
                &CredentialsChange::Keep,
            )
            .unwrap();
        assert_eq!(store.sqs_credentials(id).unwrap(), None);
    }
}
