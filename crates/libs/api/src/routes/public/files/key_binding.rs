use crate::routes::public::error::database_error;
use pplx_space_common_core::types::IdempotencyKey;
use pplx_space_native_files::RequestDigest;
use pplx_space_public_api::{
    RequestFingerprint,
    routes::{NativeHttpMethod, NativeOperation},
};

use super::super::error::PublicApiError;
use crate::{
    models::Sandbox,
    public_operations::{
        CreatePublicOperationIdempotency, HttpOperationMethod, OperationPathTemplate,
        bind_idempotency_key,
    },
    routes::AppState,
};

/// Bind the client's key to this request before the sandbox sees it. A key
/// reused for a request that differs gets `idempotency_conflict`, and the
/// guest is never asked. mkdir and remove, which the sandbox runs again on a
/// retry, are bound the same way, so no key is accepted and then ignored.
///
/// The key is scoped to the sandbox as the sandbox's own replay is: a key
/// reused in another sandbox keeps working as it did.
pub(super) async fn bind_request_key(
    state: &AppState,
    sandbox: &Sandbox,
    route: NativeOperation,
    key: &IdempotencyKey,
    fingerprint: &RequestFingerprint,
) -> Result<(), PublicApiError> {
    let mut scoped = RequestDigest::default();
    scoped
        .field(sandbox.id.to_string().as_bytes())
        .field(key.as_str().as_bytes());
    let scoped =
        IdempotencyKey::parse(scoped.finish()?.as_str()).map_err(|_| PublicApiError::internal())?;
    let method = match route.method() {
        NativeHttpMethod::Post => HttpOperationMethod::Post,
        NativeHttpMethod::Patch => HttpOperationMethod::Patch,
        NativeHttpMethod::Delete => HttpOperationMethod::Delete,
        NativeHttpMethod::Get | NativeHttpMethod::Put => return Err(PublicApiError::internal()),
    };
    let idempotency = CreatePublicOperationIdempotency {
        method,
        path_template: OperationPathTemplate::new(route.path()),
        key: &scoped,
        request_fingerprint: fingerprint,
    };
    if bind_idempotency_key(&state.pool, sandbox.org_id, &idempotency)
        .await
        .map_err(database_error)?
    {
        Ok(())
    } else {
        Err(PublicApiError::idempotency_conflict(
            "idempotency key was already used with a different request",
        ))
    }
}
