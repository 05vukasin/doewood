//! AI providers (ADR 0001, section 6): every LLM is reached through an API preset, with the
//! same preset names as quail's `ApiConfig` and hedgehog. Each configured provider has a name
//! (by default its preset), a model, an optional base URL, and an API key sealed in `secrets`.
//!
//! - The settings row `ai.provider.<name>` holds the preset, model and base URL (no secret).
//! - The secret `ai/<name>/api_key` holds the key. It is write-only, like every secret.
//! - The settings row `ai.default_provider` names the provider used when a request names none.

use std::fmt;

use rusqlite::{OptionalExtension as _, params};
use zeroize::Zeroizing;

use super::{Applied, Store, now, write_sealed_tx};
use crate::StoreError;

/// The provider presets: the names quail's `ApiConfig` and hedgehog use.
pub const AI_PRESETS: &[&str] = &[
    "openai",
    "openai_compatible",
    "anthropic",
    "bedrock",
    "openrouter",
    "groq",
    "vllm",
    "ollama",
];

/// Settings key naming the default provider.
pub const DEFAULT_AI_PROVIDER: &str = "ai.default_provider";

const PROVIDER_PREFIX: &str = "ai.provider.";

/// Largest API key accepted, in bytes.
const API_KEY_MAX_LEN: usize = 8 * 1024;

/// A configured AI provider, as the API shows it: never the key, only whether one is set.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AiProvider {
    /// Its name: 1 to 64 lowercase ASCII letters, digits, `_` or `-`.
    pub name: String,
    /// One of [`AI_PRESETS`].
    pub preset: String,
    /// The model to ask.
    pub model: String,
    /// The API base URL; `None` uses the preset's.
    pub base_url: Option<String>,
    /// Whether an API key is stored.
    pub api_key_set: bool,
    /// Whether requests that name no provider use this one.
    pub is_default: bool,
    /// When it last changed (Unix seconds).
    pub updated_at: i64,
}

/// An API key. Zeroized on drop; `Debug` does not print it.
#[derive(Clone, PartialEq, Eq)]
pub struct ApiKey(Zeroizing<String>);

impl fmt::Debug for ApiKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("ApiKey(***)")
    }
}

impl ApiKey {
    /// A non-empty key of at most 8 KiB, without line breaks.
    pub fn new(key: impl Into<String>) -> Result<Self, StoreError> {
        let key = Zeroizing::new(key.into());
        if key.is_empty() || key.len() > API_KEY_MAX_LEN {
            return Err(StoreError::InvalidSource(format!(
                "an API key is 1 to {API_KEY_MAX_LEN} bytes"
            )));
        }
        if key.contains(['\n', '\r']) {
            return Err(StoreError::InvalidSource(
                "an API key must not contain line breaks".to_owned(),
            ));
        }
        Ok(Self(key))
    }

    /// The key.
    pub fn expose(&self) -> &str {
        &self.0
    }
}

/// What to do with a provider's stored API key.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum KeyChange {
    /// Keep the stored key, if any.
    Keep,
    /// Replace it.
    Set(ApiKey),
    /// Delete it.
    Clear,
}

/// Whether `name` is a valid provider name.
pub fn valid_provider_name(name: &str) -> bool {
    (1..=64).contains(&name.len())
        && name
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || matches!(b, b'_' | b'-'))
}

/// Whether two byte strings are equal, in time that depends only on their lengths.
fn same_bytes(a: &[u8], b: &[u8]) -> bool {
    a.len() == b.len() && a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

fn key_secret(name: &str) -> String {
    format!("ai/{name}/api_key")
}

impl Store {
    /// Adds or replaces a provider. The first provider configured becomes the default, and
    /// `make_default` makes this one the default. See [`Store::apply_ai_provider`].
    pub fn set_ai_provider(
        &self,
        provider: &AiProviderInput,
        key: &KeyChange,
        make_default: bool,
    ) -> Result<AiProvider, StoreError> {
        self.apply_ai_provider(provider, key, make_default)
            .map(|a| a.value)
    }

    /// [`Store::set_ai_provider`], and whether it changed anything.
    ///
    /// Idempotent (issue #144): a service that pushes the same settings at every boot writes
    /// nothing, so `updated_at` stays and callers wake nothing. The call is a no-op when the
    /// stored preset, model and base URL are the same, the key is the same (the stored key is
    /// decrypted and compared: it is sealed with a random nonce, so the ciphertext differs at
    /// every write), and the default is already where `make_default` would put it.
    pub fn apply_ai_provider(
        &self,
        provider: &AiProviderInput,
        key: &KeyChange,
        make_default: bool,
    ) -> Result<Applied<AiProvider>, StoreError> {
        provider.validate()?;
        let now = now();
        let value = serde_json::json!({
            "preset": provider.preset,
            "model": provider.model,
            "base_url": provider.base_url,
        });
        let setting = format!("{PROVIDER_PREFIX}{}", provider.name);
        let secret = key_secret(&provider.name);
        let mut conn = self.conn();
        let tx = conn.transaction()?;
        let stored: Option<String> = tx
            .query_row(
                "SELECT value_json FROM settings WHERE key = ?1",
                [&setting],
                |r| r.get(0),
            )
            .optional()?;
        let default: Option<String> = tx
            .query_row(
                "SELECT value_json FROM settings WHERE key = ?1",
                [DEFAULT_AI_PROVIDER],
                |r| r.get(0),
            )
            .optional()?;
        let is_default = default
            .as_deref()
            .and_then(|d| serde_json::from_str::<serde_json::Value>(d).ok())
            .is_some_and(|d| d.as_str() == Some(provider.name.as_str()));
        let same_settings = stored
            .as_deref()
            .and_then(|v| serde_json::from_str::<serde_json::Value>(v).ok())
            .is_some_and(|v| v == value);
        let sealed_row: Option<(Vec<u8>, Vec<u8>)> = tx
            .query_row(
                "SELECT nonce, ciphertext FROM secrets WHERE name = ?1",
                [&secret],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .optional()?;
        let same_key = match key {
            KeyChange::Keep => true,
            KeyChange::Clear => sealed_row.is_none(),
            // A key that cannot be opened is written again.
            KeyChange::Set(k) => sealed_row.is_some_and(|(nonce, ct)| {
                self.cipher
                    .open(&secret, &nonce, &ct)
                    .is_ok_and(|plain| same_bytes(&plain, k.expose().as_bytes()))
            }),
        };
        let same_default = default.is_some() && (!make_default || is_default);
        if same_settings && same_key && same_default {
            drop(tx);
            drop(conn);
            return self.applied_provider(&provider.name, false);
        }
        tx.execute(
            "INSERT INTO settings (key, value_json, updated_at) VALUES (?1, ?2, ?3) \
             ON CONFLICT (key) DO UPDATE SET value_json = excluded.value_json, \
             updated_at = excluded.updated_at",
            params![setting, value.to_string(), now],
        )?;
        match key {
            KeyChange::Set(k) if !same_key => {
                let (nonce, ct) = self.cipher.seal(&secret, k.expose().as_bytes())?;
                write_sealed_tx(&tx, &secret, &nonce, &ct, now)?;
            }
            KeyChange::Clear => {
                tx.execute("DELETE FROM secrets WHERE name = ?1", [&secret])?;
            }
            _ => {}
        }
        if !same_default {
            tx.execute(
                "INSERT INTO settings (key, value_json, updated_at) VALUES (?1, ?2, ?3) \
                 ON CONFLICT (key) DO UPDATE SET value_json = excluded.value_json, \
                 updated_at = excluded.updated_at",
                params![
                    DEFAULT_AI_PROVIDER,
                    serde_json::Value::from(provider.name.as_str()).to_string(),
                    now
                ],
            )?;
        }
        tx.commit()?;
        drop(conn);
        self.applied_provider(&provider.name, true)
    }

    fn applied_provider(
        &self,
        name: &str,
        changed: bool,
    ) -> Result<Applied<AiProvider>, StoreError> {
        let value = self
            .ai_provider(name)?
            .ok_or_else(|| StoreError::InvalidSource("provider vanished".to_owned()))?;
        Ok(Applied { value, changed })
    }

    /// Every configured provider, by name.
    pub fn ai_providers(&self) -> Result<Vec<AiProvider>, StoreError> {
        let default = self.default_ai_provider()?;
        let rows: Vec<(String, String, i64)> = self.read(StoreError::from, |conn| {
            let mut stmt = conn.prepare_cached(
                "SELECT key, value_json, updated_at FROM settings \
                 WHERE substr(key, 1, length(?1)) = ?1 ORDER BY key",
            )?;
            let rows = stmt
                .query_map([PROVIDER_PREFIX], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))?
                .collect::<Result<_, _>>()?;
            Ok(rows)
        })?;
        rows.into_iter()
            .map(|(key, value, updated_at)| {
                let name = key[PROVIDER_PREFIX.len()..].to_owned();
                let api_key_set = self.secret_exists(&key_secret(&name))?;
                provider_from(name, &value, updated_at, api_key_set, default.as_deref())
            })
            .collect()
    }

    /// One provider.
    pub fn ai_provider(&self, name: &str) -> Result<Option<AiProvider>, StoreError> {
        if !valid_provider_name(name) {
            return Ok(None);
        }
        let row: Option<(String, i64)> = self.read(StoreError::from, |conn| {
            Ok(conn
                .prepare_cached("SELECT value_json, updated_at FROM settings WHERE key = ?1")?
                .query_row([format!("{PROVIDER_PREFIX}{name}")], |r| {
                    Ok((r.get(0)?, r.get(1)?))
                })
                .optional()?)
        })?;
        let Some((value, updated_at)) = row else {
            return Ok(None);
        };
        let default = self.default_ai_provider()?;
        let api_key_set = self.secret_exists(&key_secret(name))?;
        provider_from(
            name.to_owned(),
            &value,
            updated_at,
            api_key_set,
            default.as_deref(),
        )
        .map(Some)
    }

    /// The name of the default provider, if any is configured.
    pub fn default_ai_provider(&self) -> Result<Option<String>, StoreError> {
        Ok(self
            .setting(DEFAULT_AI_PROVIDER)?
            .and_then(|v| v.as_str().map(str::to_owned)))
    }

    /// Decrypts a provider's API key: the LLM connector's access path to the secret store.
    /// The API must never call it: secrets are write-only (ADR 0001, section 3).
    pub fn ai_api_key(&self, name: &str) -> Result<Option<ApiKey>, StoreError> {
        let Some(plain) = self.reveal(&key_secret(name))? else {
            return Ok(None);
        };
        let text = String::from_utf8(plain.to_vec())
            .map_err(|_| StoreError::InvalidSource(format!("the key of {name:?} is not UTF-8")))?;
        ApiKey::new(text).map(Some)
    }
}

fn provider_from(
    name: String,
    value: &str,
    updated_at: i64,
    api_key_set: bool,
    default: Option<&str>,
) -> Result<AiProvider, StoreError> {
    let bad = || StoreError::BadSetting(format!("{PROVIDER_PREFIX}{name}"));
    let v: serde_json::Value = serde_json::from_str(value).map_err(|_| bad())?;
    let text = |k: &str| {
        v.get(k)
            .and_then(serde_json::Value::as_str)
            .map(str::to_owned)
    };
    Ok(AiProvider {
        is_default: default == Some(name.as_str()),
        preset: text("preset").ok_or_else(bad)?,
        model: text("model").ok_or_else(bad)?,
        base_url: text("base_url"),
        api_key_set,
        updated_at,
        name,
    })
}

/// A provider to configure.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AiProviderInput {
    /// Its name; see [`valid_provider_name`].
    pub name: String,
    /// One of [`AI_PRESETS`].
    pub preset: String,
    /// The model: 1 to 256 characters, no control characters.
    pub model: String,
    /// The API base URL (`http://` or `https://`, no credentials); `None` uses the preset's.
    /// Required for `openai_compatible`.
    pub base_url: Option<String>,
}

impl AiProviderInput {
    fn validate(&self) -> Result<(), StoreError> {
        let bad = |m: &str| Err(StoreError::InvalidSource(m.to_owned()));
        if !valid_provider_name(&self.name) {
            return bad("a provider name is 1 to 64 lowercase letters, digits, '_' or '-'");
        }
        if !AI_PRESETS.contains(&self.preset.as_str()) {
            return bad("unknown preset");
        }
        if self.model.is_empty() || self.model.len() > 256 || self.model.contains(char::is_control)
        {
            return bad("a model is 1 to 256 characters without control characters");
        }
        match &self.base_url {
            None if self.preset == "openai_compatible" => {
                bad("the openai_compatible preset needs a base URL")
            }
            Some(u) if !(u.starts_with("http://") || u.starts_with("https://")) => {
                bad("the base URL must be an http:// or https:// URL")
            }
            Some(u) if u.len() > 2048 || u.contains(char::is_control) => {
                bad("the base URL is longer than 2048 bytes or holds control characters")
            }
            Some(u) if authority(u).contains('@') => {
                bad("the base URL must not contain credentials")
            }
            _ => Ok(()),
        }
    }
}

fn authority(url: &str) -> &str {
    url.split_once("://").map_or("", |(_, rest)| {
        rest.split(['/', '?', '#']).next().unwrap_or("")
    })
}

#[cfg(test)]
mod tests {
    use doewood_core::MasterKey;

    use super::*;

    fn input(name: &str, preset: &str) -> AiProviderInput {
        AiProviderInput {
            name: name.into(),
            preset: preset.into(),
            model: "test-model".into(),
            base_url: None,
        }
    }

    #[test]
    fn providers_round_trip_with_write_only_keys() {
        let store = Store::open_in_memory(&MasterKey::from_bytes([5; 32])).unwrap();
        assert!(store.ai_providers().unwrap().is_empty());
        let key = ApiKey::new("test-api-key-value").unwrap();
        assert_eq!(format!("{key:?}"), "ApiKey(***)");
        let a = store
            .set_ai_provider(&input("openai", "openai"), &KeyChange::Set(key), false)
            .unwrap();
        assert!(a.api_key_set && a.is_default);
        assert_eq!(a.model, "test-model");
        let mut local = input("local", "ollama");
        local.base_url = Some("http://127.0.0.1:11434/v1".into());
        let b = store
            .set_ai_provider(&local, &KeyChange::Keep, false)
            .unwrap();
        assert!(!b.api_key_set && !b.is_default);
        let all = store.ai_providers().unwrap();
        assert_eq!(
            all.iter().map(|p| p.name.as_str()).collect::<Vec<_>>(),
            ["local", "openai"]
        );
        // The key is sealed, and only the connector path reads it.
        assert_eq!(
            store.ai_api_key("openai").unwrap().unwrap().expose(),
            "test-api-key-value"
        );
        let ct: Vec<u8> = store
            .raw()
            .query_row(
                "SELECT ciphertext FROM secrets WHERE name = 'ai/openai/api_key'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert!(!ct.windows(18).any(|w| w == b"test-api-key-value"));
        // Keep leaves it, Clear removes it; make_default moves the default.
        let a = store
            .set_ai_provider(&input("openai", "openai"), &KeyChange::Keep, false)
            .unwrap();
        assert!(a.api_key_set);
        let a = store
            .set_ai_provider(&input("openai", "openai"), &KeyChange::Clear, false)
            .unwrap();
        assert!(!a.api_key_set);
        store
            .set_ai_provider(&local, &KeyChange::Keep, true)
            .unwrap();
        assert_eq!(
            store.default_ai_provider().unwrap().as_deref(),
            Some("local")
        );
        assert!(store.ai_provider("nope").unwrap().is_none());
        assert!(store.ai_provider("Bad Name").unwrap().is_none());
    }

    /// Every row a provider call may touch, with its timestamps, and the LLM jobs.
    fn snapshot(
        store: &Store,
        source: doewood_core::SourceId,
    ) -> (Vec<(String, String, i64)>, usize) {
        let conn = store.raw();
        let mut stmt = conn
            .prepare(
                "SELECT key, value_json, updated_at FROM settings WHERE key LIKE 'ai.%' \
                 UNION ALL SELECT name, hex(nonce) || hex(ciphertext), updated_at FROM secrets \
                 WHERE name LIKE 'ai/%' UNION ALL SELECT hex(blake3), status || attempts \
                 || coalesce(last_error, ''), updated_at FROM llm_jobs WHERE source_id = ?1 \
                 ORDER BY 1",
            )
            .unwrap();
        let rows: Vec<(String, String, i64)> = stmt
            .query_map([source.to_string()], |r| {
                Ok((r.get(0)?, r.get(1)?, r.get(2)?))
            })
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap();
        drop(stmt);
        drop(conn);
        (rows, store.ai_providers().unwrap().len())
    }

    /// Backdates every timestamp, so a write in the same second still shows.
    fn backdate(store: &Store) {
        let conn = store.raw();
        for table in ["settings", "secrets", "llm_jobs", "sources"] {
            conn.execute(&format!("UPDATE {table} SET updated_at = 1000"), [])
                .unwrap();
        }
    }

    /// Issue #144: the same settings again (a service configuring doewood at every boot)
    /// write nothing; a changed key, model, base URL or default applies at once.
    #[test]
    fn set_ai_provider_is_idempotent() {
        use crate::{LlmJobStatus, NewSource};

        let store = Store::open_in_memory(&MasterKey::from_bytes([5; 32])).unwrap();
        let source = store
            .add_source(&NewSource::new("http://127.0.0.1:9000", "docs"), None)
            .unwrap();
        let mut mock = input("mock", "openai_compatible");
        mock.base_url = Some("http://127.0.0.1:9/v1".into());
        let key = || KeyChange::Set(ApiKey::new("test-key-1").unwrap());
        let first = store.apply_ai_provider(&mock, &key(), true).unwrap();
        assert!(first.changed && first.value.is_default && first.value.api_key_set);
        // Jobs in every state: pending, failed after a call, done.
        let (a, b, c) = ([1; 32], [2; 32], [3; 32]);
        for j in [&a, &b, &c] {
            store.queue_llm_job(source, j, None).unwrap();
        }
        store
            .finish_llm_job(source, &b, LlmJobStatus::Failed, Some("HTTP 500"), true)
            .unwrap();
        store
            .finish_llm_job(source, &c, LlmJobStatus::Done, None, true)
            .unwrap();
        backdate(&store);
        let before = snapshot(&store, source);
        assert_eq!(before.1, 1);
        // The same call, three times, with make_default and without (it is the default),
        // and with the key kept.
        for (k, make_default) in [(key(), true), (key(), false), (KeyChange::Keep, true)] {
            let again = store.apply_ai_provider(&mock, &k, make_default).unwrap();
            assert!(!again.changed, "{k:?} {make_default}");
            assert_eq!(again.value.updated_at, 1000);
            assert_eq!(snapshot(&store, source), before);
        }
        let jobs = store.pending_llm_jobs(10).unwrap();
        assert_eq!(jobs.len(), 1);
        assert_eq!(jobs[0].blake3, a);
        assert_eq!(
            store.llm_job(source, &b).unwrap().unwrap().status,
            LlmJobStatus::Failed
        );

        // A new key applies.
        let k2 = KeyChange::Set(ApiKey::new("test-key-2").unwrap());
        let changed = store.apply_ai_provider(&mock, &k2, true).unwrap();
        assert!(changed.changed);
        assert!(changed.value.updated_at > 1000);
        assert_eq!(
            store.ai_api_key("mock").unwrap().unwrap().expose(),
            "test-key-2"
        );
        assert!(!store.apply_ai_provider(&mock, &k2, true).unwrap().changed);
        // So do a new model and a new base URL.
        let edits: [fn(&mut AiProviderInput); 2] = [
            |m| m.model = "test-model-2".into(),
            |m| m.base_url = Some("http://127.0.0.1:10/v1".into()),
        ];
        for change in edits {
            change(&mut mock);
            backdate(&store);
            let changed = store
                .apply_ai_provider(&mock, &KeyChange::Keep, true)
                .unwrap();
            assert!(changed.changed);
            assert_eq!(changed.value.model, mock.model);
            assert_eq!(changed.value.base_url, mock.base_url);
            assert!(changed.value.updated_at > 1000);
        }
        // Clearing the key changes it once.
        assert!(
            store
                .apply_ai_provider(&mock, &KeyChange::Clear, true)
                .unwrap()
                .changed
        );
        assert!(
            !store
                .apply_ai_provider(&mock, &KeyChange::Clear, true)
                .unwrap()
                .changed
        );
        // The default flag: a second provider takes it only when asked, and asking again for
        // it is a no-op.
        let other = input("other", "openai");
        let set = |make_default| {
            store
                .apply_ai_provider(&other, &KeyChange::Keep, make_default)
                .unwrap()
        };
        assert!(set(false).changed && !set(false).changed);
        assert_eq!(
            store.default_ai_provider().unwrap().as_deref(),
            Some("mock")
        );
        let moved = set(true);
        assert!(moved.changed && moved.value.is_default && !set(true).changed);
        assert_eq!(store.ai_providers().unwrap().len(), 2);
        assert_eq!(
            store.pending_llm_jobs(10).unwrap().len(),
            1,
            "no job touched"
        );
    }

    /// Issue #144, for `SetLlmPolicy`: the same policy writes nothing; a new one applies.
    #[test]
    fn set_llm_policy_is_idempotent() {
        use crate::{LlmMode, LlmPolicy, NewSource};

        let store = Store::open_in_memory(&MasterKey::from_bytes([5; 32])).unwrap();
        let source = store
            .add_source(&NewSource::new("http://127.0.0.1:9000", "docs"), None)
            .unwrap();
        store.queue_llm_job(source, &[1; 32], None).unwrap();
        let policy = LlmPolicy {
            mode: LlmMode::Kinds(vec!["contract".into(), "invoice".into()]),
            concurrency: 2,
            daily_token_budget: Some(10_000),
        };
        assert!(store.set_llm_policy(source, &policy).unwrap().changed);
        backdate(&store);
        let before = snapshot(&store, source);
        let again = store.set_llm_policy(source, &policy).unwrap();
        assert!(!again.changed);
        assert_eq!(again.value.updated_at, 1000);
        assert_eq!(again.value.llm_policy, policy);
        assert_eq!(snapshot(&store, source), before);
        let budget = LlmPolicy {
            daily_token_budget: Some(20_000),
            ..policy
        };
        let changed = store.set_llm_policy(source, &budget).unwrap();
        assert!(changed.changed && changed.value.updated_at > 1000);
        assert_eq!(store.source(source).unwrap().unwrap().llm_policy, budget);
        assert!(
            store
                .set_llm_policy(doewood_core::SourceId::generate(), &budget)
                .is_err()
        );
    }

    #[test]
    fn providers_are_validated() {
        let store = Store::open_in_memory(&MasterKey::from_bytes([5; 32])).unwrap();
        let mut compatible = input("x", "openai_compatible");
        let mut with_creds = input("y", "openai");
        with_creds.base_url = Some("https://user:pw@api.example.com/v1".into());
        let mut no_model = input("z", "groq");
        no_model.model = String::new();
        for bad in [
            input("", "openai"),
            input("UPPER", "openai"),
            input("x", "gpt"),
            compatible.clone(),
            with_creds,
            no_model,
        ] {
            assert!(
                store
                    .set_ai_provider(&bad, &KeyChange::Keep, false)
                    .is_err(),
                "{bad:?}"
            );
        }
        compatible.base_url = Some("http://vllm:8000/v1".into());
        assert!(
            store
                .set_ai_provider(&compatible, &KeyChange::Keep, false)
                .is_ok()
        );
        assert!(ApiKey::new("").is_err());
        assert!(ApiKey::new("a\nb").is_err());
    }
}
