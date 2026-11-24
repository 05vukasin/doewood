//! The authentication hook.
//!
//! v1 has **no authentication** (ADR 0001, section 4): the API binds to localhost by default,
//! secrets are write-only, and Kubernetes network isolation protects it. Every doewood
//! service still runs behind this interceptor, so that tokens or OIDC can be added in one
//! place without changing the API: read the `authorization` metadata here, answer
//! UNAUTHENTICATED (or PERMISSION_DENIED) when it does not verify, and attach the caller's
//! identity as a request extension for the services to read.
//!
//! Health and reflection are not behind it: probes and tooling must work without
//! credentials.

use tonic::service::Interceptor;
use tonic::{Request, Status};

/// The (empty) v1 interceptor: lets every request through.
#[derive(Clone, Copy, Debug, Default)]
pub struct Authorize;

impl Interceptor for Authorize {
    fn call(&mut self, request: Request<()>) -> Result<Request<()>, Status> {
        // No authentication in v1; see the module documentation.
        Ok(request)
    }
}
