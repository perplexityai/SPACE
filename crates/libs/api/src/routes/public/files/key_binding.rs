use pplx_space_common_core::types::{GuestUserName, IdempotencyKey};
use pplx_space_public_api::{RequestFingerprint, routes::NativeOperation};
use sha2::{Digest as _, Sha256};

use super::super::error::PublicApiError;
use crate::{
    models::Sandbox,
    public_operations::{
        CreatePublicOperationIdempotency, HttpOperationMethod, OperationPathTemplate,
        bind_idempotency_key,
    },
    routes::AppState,
};

/// Bind the client's key to this request before the sandbox sees it. The
/// sandbox replays a keyed write or rename by key alone, so without the
/// binding a key reused for another path or other data got the first
/// request's answer while nothing was written. A reused key whose request
/// differs gets `idempotency_conflict` instead, and the guest is never
/// asked. mkdir and remove, which the sandbox runs again on a retry, are
/// bound the same way, so no key is accepted and then ignored.
///
/// The key is scoped to the sandbox as the sandbox's own replay is: a key
/// reused in another sandbox keeps working as it did.
pub(super) async fn bind_request_key(
    state: &AppState,
    sandbox: &Sandbox,
    route: NativeOperation,
    key: &IdempotencyKey,
    fingerprint: RequestDigest,
) -> Result<(), PublicApiError> {
    let mut scoped = Sha256::new();
    scoped.update(sandbox.id.to_string());
    scoped.update([0]);
    scoped.update(key.as_str());
    let scoped = IdempotencyKey::parse(hex::encode(scoped.finalize()))
        .map_err(|_| PublicApiError::internal())?;
    let fingerprint = fingerprint.finish()?;
    let idempotency = CreatePublicOperationIdempotency {
        method: HttpOperationMethod::Post,
        path_template: OperationPathTemplate::new(route.path()),
        key: &scoped,
        request_fingerprint: &fingerprint,
    };
    if bind_idempotency_key(&state.pool, sandbox.org_id, &idempotency).await? {
        Ok(())
    } else {
        Err(PublicApiError::idempotency_conflict(
            "idempotency key was already used with a different request",
        ))
    }
}

/// What a file mutation asks the sandbox to do, as the fingerprint its key is
/// bound to. Every field is framed, so no two requests share an encoding.
/// Write data is hashed from the decoded chunks the request already holds,
/// with no further copy of the payload.
#[derive(Default)]
pub(super) struct RequestDigest(Sha256);

impl RequestDigest {
    pub(super) fn field(&mut self, value: &[u8]) -> &mut Self {
        self.count(value.len());
        self.0.update(value);
        self
    }

    pub(super) fn count(&mut self, value: usize) -> &mut Self {
        self.0.update((value as u64).to_be_bytes());
        self
    }

    pub(super) fn flag(&mut self, value: bool) -> &mut Self {
        self.0.update([u8::from(value)]);
        self
    }

    pub(super) fn user(&mut self, user: Option<&GuestUserName>) -> &mut Self {
        match user {
            Some(user) => self.flag(true).field(user.as_str().as_bytes()),
            None => self.flag(false),
        }
    }

    /// One field split across write messages.
    pub(super) fn chunks(&mut self, chunks: &[Vec<u8>]) -> &mut Self {
        self.count(chunks.iter().map(Vec::len).sum());
        for chunk in chunks {
            self.0.update(chunk);
        }
        self
    }

    pub(super) fn finish(self) -> Result<RequestFingerprint, PublicApiError> {
        RequestFingerprint::parse(hex::encode(self.0.finalize()))
            .map_err(|_| PublicApiError::internal())
    }
}
