use super::idempotency;
use pplx_space_common_core::types::{CredentialReferenceId, IdempotencyKey, OrgId};
use pplx_space_credential_injection_core::validation::{
    ReferenceValidationError, ValidatedCredentialReference,
};
use pplx_space_credential_injection_store::references::{self as store, ReferenceState};
use pplx_space_public_api::credential_references as wire;

#[derive(Debug, thiserror::Error)]
pub(crate) enum ReferenceError {
    #[error("credential reference not found")]
    NotFound,
    #[error("credential reference remains attached to a network policy")]
    Attached,
    #[error("request_id was already used with a different request")]
    IdempotencyConflict,
    #[error(transparent)]
    Invalid(#[from] ReferenceValidationError),
    #[error(transparent)]
    Database(#[from] sqlx::Error),
}

pub(crate) fn definition(
    request: &wire::RegisterCredentialReferenceRequest,
) -> Result<ValidatedCredentialReference, ReferenceError> {
    match request {
        wire::RegisterCredentialReferenceRequest::AwsSecretsManager { secret_arn, .. } => Ok(
            ValidatedCredentialReference::aws_secrets_manager(secret_arn.clone())?,
        ),
    }
}

pub(crate) async fn register(
    pool: &sqlx::PgPool,
    organization: OrgId,
    request_id: &IdempotencyKey,
    definition: &ValidatedCredentialReference,
) -> Result<wire::CredentialReference, ReferenceError> {
    let scope = idempotency::Scope::register(organization, request_id, definition);
    let fingerprint = scope.fingerprint();
    let mut tx = pool.begin().await?;
    if let Some(reply) = idempotency::get::<wire::CredentialReference>(&mut *tx, &scope).await? {
        return if reply.fingerprint == fingerprint.as_str() {
            Ok(reply.response.0)
        } else {
            Err(ReferenceError::IdempotencyConflict)
        };
    }
    let id = store::create(&mut *tx, organization, definition).await?;
    let response = wire::CredentialReference {
        credential_reference_id: id.to_string(),
    };
    if idempotency::insert(&mut *tx, &scope, &fingerprint, &response).await? {
        tx.commit().await?;
        return Ok(response);
    }
    // A concurrent registration won the unique idempotency key. Roll back this
    // attempt's new identity before replaying that writer's committed result.
    tx.rollback().await?;
    let reply = idempotency::get::<wire::CredentialReference>(pool, &scope)
        .await?
        .ok_or(ReferenceError::IdempotencyConflict)?;
    if reply.fingerprint != fingerprint.as_str() {
        return Err(ReferenceError::IdempotencyConflict);
    }
    Ok(reply.response.0)
}

pub(crate) async fn retire(
    pool: &sqlx::PgPool,
    organization: OrgId,
    id: CredentialReferenceId,
) -> Result<(), ReferenceError> {
    let mut tx = pool.begin().await?;
    let reference = store::lock(&mut *tx, organization, &[id])
        .await?
        .pop()
        .ok_or(ReferenceError::NotFound)?;
    if reference.state == ReferenceState::Active {
        // After waiting for in-flight binds, this fresh statement sees their
        // committed policies. Detachment still needs worker reconciliation;
        // this check does not revoke material already held by a gateway.
        if store::attached(&mut *tx, organization, id).await? {
            return Err(ReferenceError::Attached);
        }
        store::retire(&mut *tx, organization, id).await?;
    }
    tx.commit().await?;
    Ok(())
}
