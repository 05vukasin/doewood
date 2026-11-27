//! Re-claiming this instance's sources in the shared store when `serve` starts (ADR 0001,
//! section 2).
//!
//! With a shared Postgres, `serve` starts no scanner until the database answers. It then claims
//! every source of its SQLite store for its instance id:
//!
//! - unclaimed, or already claimed by this instance: the claim is (re)written and the source is
//!   scanned as usual;
//! - claimed by another instance: nothing is taken over and the start does not fail. The source
//!   is not scanned; an active (or failing) source turns `error` with the reason
//!   `claimed by instance "<owner>" since <time>`, a paused one stays paused. Either way a WARN
//!   names the owner.
//!
//! A claim call that fails (the database went away again) retries the whole pass with backoff;
//! claims are idempotent. Until the pass has finished, resuming a source only stores the new
//! state, and the pass starts its scanner. After it, resuming a source claims it first.
//!
//! What the instance learns is kept in the store's [`doewood_store::ClaimView`] too: ingest
//! drops the queued objects of a source another instance owns, and the webhook inlet ignores
//! its notifications (issue #25). Until the pass has finished, ingest holds its work.
//!
//! [`release_claim`] is `ConfigService.ReleaseSourceClaim` (issue #24): an operator deletes a
//! claim, whoever owns it, so that another instance can take the bucket after its owner is
//! gone for good. Nothing is taken over automatically.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use doewood_core::InstanceId;
use doewood_model::{Backend, Error as ModelError, SourceKey};
use doewood_store::{CLAIMED_BY, Source, SourceState, Store};
use tokio::sync::watch;
use tonic::Status;
use tracing::{info, warn};

use crate::{ApiState, error, pb};

const MIN_RETRY: Duration = Duration::from_secs(1);
const MAX_RETRY: Duration = Duration::from_mins(1);

/// What the start-up claim pass found.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Reclaimed {
    /// Sources claimed (or confirmed) for this instance.
    pub claimed: usize,
    /// Sources another instance owns: not scanned here.
    pub conflicts: usize,
}

/// Waits for the shared store, then claims every source of this instance for it, starts the
/// scanners of those it may scan, and marks the ones another instance owns (see the module
/// documentation). Retries with backoff while claim calls fail. `None` on shutdown, or when
/// the backend never arrives.
pub async fn reclaim_sources(
    state: &ApiState,
    mut shutdown: watch::Receiver<bool>,
) -> Option<Reclaimed> {
    let mut backend_rx = state.parts().backend.clone();
    let backend = tokio::select! {
        b = backend_rx.wait_for(Option::is_some) => b.ok()?.clone()?,
        _ = shutdown.wait_for(|s| *s) => return None,
    };
    let mut wait = MIN_RETRY;
    loop {
        if *shutdown.borrow() {
            return None;
        }
        match pass(state, &backend).await {
            Ok(r) => {
                state.claims_done();
                info!(
                    claimed = r.claimed,
                    conflicts = r.conflicts,
                    "source claims checked in the shared store"
                );
                return Some(r);
            }
            Err(e) => {
                warn!(
                    error = %e,
                    retry_in_s = wait.as_secs(),
                    "cannot re-claim the sources in the shared store; retrying"
                );
                tokio::select! {
                    () = tokio::time::sleep(wait) => {}
                    _ = shutdown.wait_for(|s| *s) => return None,
                }
                wait = (wait * 2).min(MAX_RETRY);
            }
        }
    }
}

/// One pass over the sources. An error means "try the whole pass again".
async fn pass(state: &ApiState, backend: &Arc<dyn Backend>) -> Result<Reclaimed, String> {
    let _guard = state.lock_sources().await;
    let sources = state
        .db(Store::sources)
        .await
        .map_err(|s| s.message().to_owned())?;
    let mut found = Reclaimed::default();
    for source in sources
        .into_iter()
        .filter(|s| s.state != SourceState::Removed)
    {
        let key = SourceKey::new(&source.endpoint, &source.bucket, source.prefix.clone())
            .map_err(|e| e.to_string())?;
        match backend.claim_source(&key, state.instance()).await {
            Ok(_) => {
                found.claimed += 1;
                state.store().claims().set_mine(source.id);
                if let Some(d) = state.discovery() {
                    d.start_source(&source);
                }
            }
            Err(ModelError::SourceClaimed { owner, .. }) => {
                found.conflicts += 1;
                mark_conflict(state, backend, &source, &key, &owner).await?;
            }
            Err(e) => return Err(e.to_string()),
        }
    }
    state.progress().touch();
    Ok(found)
}

/// Stops and marks a source another instance owns; returns the reason given.
pub(crate) async fn mark_conflict(
    state: &ApiState,
    backend: &Arc<dyn Backend>,
    source: &Source,
    key: &SourceKey,
    owner: &InstanceId,
) -> Result<String, String> {
    // The owner's claim time, if it still holds the claim; best effort.
    let since = match backend.source_owner(key).await {
        Ok(Some(claim)) if &claim.owner == owner => Some(claim.claimed_at),
        _ => None,
    };
    let reason = conflict_reason(owner, since);
    if let Some(d) = state.discovery() {
        d.stop_source(source.id);
    }
    // Paused or not, its queued objects and notifications are dropped from now on.
    state
        .store()
        .claims()
        .set_elsewhere(source.id, owner.clone());
    if source.state == SourceState::Paused {
        warn!(
            source = %source.id,
            key = %key,
            owner = %owner,
            "source claimed by another instance; it stays paused, and resuming it is refused"
        );
    } else {
        let (id, why) = (source.id, reason.clone());
        state
            .db(move |s| s.set_source_state(id, SourceState::Error, Some(&why)))
            .await
            .map_err(|s| s.message().to_owned())?;
        warn!(
            source = %source.id,
            key = %key,
            owner = %owner,
            reason = %reason,
            "source claimed by another instance; not scanning it"
        );
    }
    Ok(reason)
}

/// `ConfigService.ReleaseSourceClaim` (issue #24): deletes `owner`'s claim on `key`, whichever
/// instance `owner` is, so that another instance can take the bucket. Idempotent: no claim is
/// `released: false`. A claim held by another instance than `owner` is FAILED_PRECONDITION and
/// stays; so is this instance's claim on one of its own sources (`RemoveSource` releases it
/// with the source). Each call is logged with the key, the owner, this instance and the
/// caller's address: a release at WARN, as it lifts the guard of ADR 0001 section 2 for
/// that bucket.
pub(crate) async fn release_claim(
    state: &ApiState,
    key: &SourceKey,
    owner: &InstanceId,
    peer: Option<SocketAddr>,
) -> Result<pb::ReleaseSourceClaimResponse, Status> {
    let _guard = state.lock_sources().await;
    let backend = state.backend()?;
    let peer = peer.map_or_else(|| "unknown".to_owned(), |p| p.to_string());
    let not_owner = |holder: &InstanceId| {
        Status::failed_precondition(format!(
            "source {key} is claimed by instance {:?}, not {:?}; nothing was released",
            holder.as_str(),
            owner.as_str()
        ))
    };
    let none = |why: &str| {
        info!(
            key = %key,
            owner = %owner,
            released_by = %state.instance(),
            peer = %peer,
            "source claim release asked for; {why}"
        );
        pb::ReleaseSourceClaimResponse::default()
    };
    let Some(claim) = backend.source_owner(key).await.map_err(error::model)? else {
        return Ok(none("nobody holds the claim"));
    };
    if &claim.owner != owner {
        return Err(not_owner(&claim.owner));
    }
    if owner == state.instance() {
        let (e, b, p) = (
            key.endpoint().to_owned(),
            key.bucket().to_owned(),
            key.prefix().to_owned(),
        );
        if let Some(local) = state.db(move |s| s.source_by_key(&e, &b, &p)).await?
            && local.state != SourceState::Removed
        {
            return Err(Status::failed_precondition(format!(
                "this instance owns source {key} as {}: remove the source instead",
                local.id
            )));
        }
    }
    match backend.release_source(key, owner).await {
        Ok(true) => {
            warn!(
                key = %key,
                owner = %owner,
                claimed_at = claim.claimed_at,
                released_by = %state.instance(),
                peer = %peer,
                "source claim released by an operator; another instance may claim the source now"
            );
            Ok(pb::ReleaseSourceClaimResponse {
                released: true,
                owner: owner.as_str().to_owned(),
                claimed_at: claim.claimed_at,
            })
        }
        // Released by someone else in between.
        Ok(false) => Ok(none("nobody holds the claim")),
        Err(ModelError::SourceClaimed { owner: holder, .. }) => Err(not_owner(&holder)),
        Err(e) => Err(error::model(e)),
    }
}

/// `claimed by instance "<owner>" since <UTC time>; this instance does not scan it`.
pub(crate) fn conflict_reason(owner: &InstanceId, since: Option<i64>) -> String {
    let since = since.map_or_else(String::new, |t| format!(" since {}", utc(t)));
    format!(
        "{CLAIMED_BY}{:?}{since}; this instance does not scan it",
        owner.as_str()
    )
}

fn utc(unix: i64) -> String {
    time::OffsetDateTime::from_unix_timestamp(unix).map_or_else(
        |_| format!("{unix} (Unix seconds)"),
        |t| {
            format!(
                "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}Z",
                t.year(),
                u8::from(t.month()),
                t.day(),
                t.hour(),
                t.minute(),
                t.second()
            )
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_reason_names_the_owner_and_the_time() {
        let owner = InstanceId::new("doewood-1").unwrap();
        assert_eq!(
            conflict_reason(&owner, Some(1_795_000_000)),
            "claimed by instance \"doewood-1\" since 2026-11-18T11:06:40Z; this instance does \
             not scan it"
        );
        assert_eq!(
            conflict_reason(&owner, None),
            "claimed by instance \"doewood-1\"; this instance does not scan it"
        );
    }
}
