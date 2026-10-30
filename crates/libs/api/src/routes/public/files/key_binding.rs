use pplx_space_common_core::types::{GuestUserName, IdempotencyKey};
use pplx_space_public_api::{
    RequestFingerprint,
    routes::{NativeHttpMethod, NativeOperation},
};
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
    if bind_idempotency_key(&state.pool, sandbox.org_id, &idempotency).await? {
        Ok(())
    } else {
        Err(PublicApiError::idempotency_conflict(
            "idempotency key was already used with a different request",
        ))
    }
}

/// The key the sandbox sees for a request bound to `key`. The sandbox
/// replays a keyed write or rename by key alone, and nothing bounds how long
/// it holds a record whose operation is still running, so its memory of a key
/// can outlast the binding. Deriving its key from the request as well means a
/// different request never reaches another one's record, however long that
/// lives, while a retry of the same request still replays its own.
pub(super) fn guest_key(
    route: NativeOperation,
    key: &IdempotencyKey,
    fingerprint: &RequestFingerprint,
) -> Result<IdempotencyKey, PublicApiError> {
    let mut digest = RequestDigest::default();
    digest
        .field(route.path().as_bytes())
        .field(key.as_str().as_bytes())
        .field(fingerprint.as_str().as_bytes());
    IdempotencyKey::parse(format!("native-file-{}", digest.finish()?.as_str()))
        .map_err(|_| PublicApiError::internal())
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
