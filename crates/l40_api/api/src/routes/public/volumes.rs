//! Management of volumes provisioned by native sandbox creation.

use axum::{
    Extension, Json,
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
};
use pplx_space_public_api::{
    OperationKind, OperationTargetType, PublicOperationRecord, PublicVolume,
};
use pplx_space_sandbox_store::block_volume as store;
use sha2::{Digest, Sha256};
use uuid::Uuid;

use super::{
    error::{PublicApiError, database_error},
    sandboxes::idempotency_key,
};
use crate::{
    middleware::auth::{AuthOrg, AuthUser},
    public_operations::{
        ApiProduct, CreatePublicOperation, CreatePublicOperationIdempotency,
        CreatePublicOperationResult, HttpOperationMethod, OperationPathTemplate,
        PublicOperationStore, create_operation_tx, request_traceparent,
    },
    routes::AppState,
};

#[cfg(test)]
mod tests;

pub(super) async fn get(
    State(state): State<AppState>,
    Extension(AuthOrg(org)): Extension<AuthOrg>,
    Path(id): Path<Uuid>,
) -> Result<Json<PublicVolume>, PublicApiError> {
    let sandbox = store::sandbox_for_volume(&state.pool, org.id, id)
        .await
        .map_err(database_error)?
        .ok_or_else(|| PublicApiError::not_found("volume not found"))?;
    let row = store::get(&state.pool, sandbox)
        .await
        .map_err(database_error)?
        .ok_or_else(|| PublicApiError::not_found("volume not found"))?;
    Ok(Json(crate::block_volumes::public_volume(row)))
}

pub(super) async fn delete(
    State(state): State<AppState>,
    Extension(AuthOrg(org)): Extension<AuthOrg>,
    actor: Option<Extension<AuthUser>>,
    Path(id): Path<Uuid>,
    headers: HeaderMap,
) -> Result<(StatusCode, Json<PublicOperationRecord>), PublicApiError> {
    if !state.volume_management_organizations.contains(&org.id) {
        return Err(PublicApiError::forbidden(
            "volume management admission is disabled",
        ));
    }
    let key = idempotency_key(&headers)?;
    let fingerprint = pplx_space_public_api::RequestFingerprint::parse(hex::encode(
        Sha256::digest(format!("delete_volume:{id}").as_bytes()),
    ))
    .map_err(|_| PublicApiError::internal())?;
    let mut tx = state.pool.begin().await.map_err(database_error)?;
    let target = id.to_string();
    let (created, _) = create_operation_tx(
        &mut tx,
        CreatePublicOperation {
            org_id: org.id,
            actor_user_id: actor.map(|Extension(AuthUser(user))| user.id),
            api_product: ApiProduct::Native,
            kind: OperationKind::DeleteVolume,
            target_type: Some(OperationTargetType::Volume),
            target_id: Some(&target),
            payload_json: serde_json::json!({"volume_id":id}),
            idempotency: Some(CreatePublicOperationIdempotency {
                method: HttpOperationMethod::Delete,
                path_template: OperationPathTemplate::new("/v1/volumes/{volume_id}"),
                key: &key,
                request_fingerprint: &fingerprint,
            }),
            traceparent: request_traceparent(),
        },
    )
    .await?;
    let operation_id = match created {
        CreatePublicOperationResult::Created(operation_id) => {
            if !store::request_detached_delete(&mut tx, org.id, id)
                .await
                .map_err(database_error)?
            {
                return Err(PublicApiError::conflict_with_reason(
                    "volume deletion requires a stopped sandbox, no memory checkpoint and observed detach",
                    "volume_in_use",
                ));
            }
            operation_id
        }
        CreatePublicOperationResult::Existing(id) => id,
        CreatePublicOperationResult::IdempotencyConflict { .. } => {
            return Err(PublicApiError::idempotency_conflict(
                "idempotency key conflicts",
            ));
        }
        CreatePublicOperationResult::AdmissionDenied => return Err(PublicApiError::internal()),
    };
    tx.commit().await.map_err(database_error)?;
    let operation = PublicOperationStore::new(state.pool)
        .fetch_operation(org.id, operation_id)
        .await?
        .ok_or_else(PublicApiError::internal)?;
    Ok((StatusCode::ACCEPTED, Json(operation)))
}
