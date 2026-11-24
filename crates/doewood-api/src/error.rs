//! Store and model errors as gRPC statuses.
//!
//! Messages never carry a secret or a presigned URL: no error type below holds one (S3 errors
//! drop the URL, credentials are never formatted), and failures whose text could echo stored
//! data (SQL errors, decryption) are logged here and answered with a generic message.

use doewood_store::StoreError;
use tonic::{Code, Status};
use tracing::warn;

/// A model (backend) error.
pub(crate) fn model(e: doewood_model::Error) -> Status {
    use doewood_model::Error as E;
    match e {
        E::SourceClaimed { .. } => Status::already_exists(e.to_string()),
        E::Invalid(m) => Status::invalid_argument(m),
        E::NotFound(m) => Status::not_found(m),
        E::Unavailable(_) => Status::unavailable(e.to_string()),
        E::SchemaTooNew { .. } => Status::failed_precondition(e.to_string()),
        other => internal("shared store", &other),
    }
}

/// An instance store error.
pub(crate) fn store(e: StoreError) -> Status {
    match e {
        StoreError::NoSuchSource(id) => Status::not_found(format!("source {id} does not exist")),
        StoreError::InvalidSource(m) | StoreError::InvalidSetting(m) => {
            Status::invalid_argument(m)
        }
        StoreError::InvalidSecretName { .. } | StoreError::SecretTooLarge(_) => {
            Status::invalid_argument(e.to_string())
        }
        StoreError::Sqlite(rusqlite_error) if is_constraint(&rusqlite_error) => {
            Status::already_exists("a source with this endpoint, bucket and prefix exists")
        }
        other => internal("instance store", &other),
    }
}

fn is_constraint(e: &impl std::error::Error) -> bool {
    // rusqlite is not a dependency of this crate; its constraint errors say so.
    e.to_string().contains("UNIQUE constraint failed")
}

/// A failure the client cannot act on: logged in full, answered without details.
pub(crate) fn internal(what: &str, e: &dyn std::fmt::Display) -> Status {
    warn!(error = %e, "{what} failed");
    Status::new(
        Code::Internal,
        format!("{what} error (details are in the server log)"),
    )
}

/// A blocking task that did not finish.
pub(crate) fn join(e: &tokio::task::JoinError) -> Status {
    internal("blocking task", e)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn statuses_follow_the_error() {
        let key = doewood_model::SourceKey::new("http://m", "b", "").unwrap();
        let owner = doewood_core::InstanceId::new("inst-a").unwrap();
        let s = model(doewood_model::Error::SourceClaimed { key, owner });
        assert_eq!(s.code(), Code::AlreadyExists);
        assert!(s.message().contains("\"inst-a\""), "{}", s.message());
        assert_eq!(
            model(doewood_model::Error::NotFound("x".into())).code(),
            Code::NotFound
        );
        assert_eq!(
            model(doewood_model::Error::Unavailable("down".into())).code(),
            Code::Unavailable
        );
        let s = model(doewood_model::Error::Backend {
            backend: "postgres",
            message: "duplicate key value (secret-ish)".into(),
        });
        assert_eq!(s.code(), Code::Internal);
        assert!(!s.message().contains("secret-ish"));
        assert_eq!(
            store(StoreError::NoSuchSource("s1".into())).code(),
            Code::NotFound
        );
        let s = store(StoreError::Decrypt("ai/x/api_key".into()));
        assert_eq!(s.code(), Code::Internal);
        assert!(!s.message().contains("api_key"));
    }
}
