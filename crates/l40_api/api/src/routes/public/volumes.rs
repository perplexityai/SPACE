//! Independent storage and legacy sandbox-created volume management.

use axum::{
    Extension, Json,
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode},
};
use pplx_space_public_api::{
    CreateVolumeRequest, OperationKind, OperationTargetType, PublicOperationRecord, PublicVolume,
    ResizeVolumeRequest,
};
use pplx_space_sandbox_store::block_volume as store;
use sha2::{Digest, Sha256};
use uuid::Uuid;

use super::{
    error::{PublicApiError, database_error},
    sandboxes::idempotency_key,
};
use crate::{
    middleware::auth::{AuthFederatedOwner, AuthOrg, AuthUser},
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
    let row = store::resource::get(&state.pool, org.id, id)
        .await
        .map_err(database_error)?
        .ok_or_else(|| PublicApiError::not_found("volume not found"))?;
    Ok(Json(crate::block_volumes::public_volume(row)))
}

pub(super) async fn resize(
    State(state): State<AppState>,
    Extension(AuthOrg(org)): Extension<AuthOrg>,
    Extension(AuthUser(actor)): Extension<AuthUser>,
    owner: Option<Extension<AuthFederatedOwner>>,
    Path(id): Path<Uuid>,
    headers: HeaderMap,
    Json(request): Json<ResizeVolumeRequest>,
) -> Result<(StatusCode, Json<PublicOperationRecord>), PublicApiError> {
    if owner.is_some() {
        return Err(PublicApiError::forbidden(
            "federated independent volumes are unsupported",
        ));
    }
    let admission = crate::block_volumes::independent_admission(
        &pplx_space_env_vars::ProcessEnvironment,
        org.id,
        org.byok_enabled,
    )
    .map_err(super::sandboxes::controller_error)?;
    let target_size = i32::try_from(request.size_gib.get())
        .map_err(|_| PublicApiError::bad_request("volume size is outside the supported range"))?;
    if !matches!(target_size, 128 | 256) {
        return Err(PublicApiError::bad_request(
            "volume growth currently supports 128 to 256 GiB",
        ));
    }
    let key = idempotency_key(&headers)?;
    let payload = serde_json::to_value(&request).map_err(|_| PublicApiError::internal())?;
    let fingerprint =
        pplx_space_public_api::RequestFingerprint::parse(hex::encode(Sha256::digest(
            serde_json::to_vec(&serde_json::json!({
                "volume_id": id, "request": payload,
            }))
            .map_err(|_| PublicApiError::internal())?,
        )))
        .map_err(|_| PublicApiError::internal())?;
    let target = id.to_string();
    let mut tx = state.pool.begin().await.map_err(database_error)?;
    let (created, _) = create_operation_tx(
        &mut tx,
        CreatePublicOperation {
            org_id: org.id,
            actor_user_id: Some(actor.id),
            api_product: ApiProduct::Native,
            kind: OperationKind::ResizeVolume,
            target_type: Some(OperationTargetType::Volume),
            target_id: Some(&target),
            payload_json: payload,
            idempotency: Some(CreatePublicOperationIdempotency {
                method: HttpOperationMethod::Patch,
                path_template: OperationPathTemplate::new("/v1/volumes/{volume_id}"),
                key: &key,
                request_fingerprint: &fingerprint,
            }),
            traceparent: request_traceparent(),
        },
    )
    .await?;
    let operation = match created {
        CreatePublicOperationResult::Created(operation) => {
            let row = store::resource::get(&mut *tx, org.id, id)
                .await
                .map_err(database_error)?
                .ok_or_else(|| PublicApiError::not_found("volume not found"))?;
            if !row.retained || target_size < row.size_gib {
                return Err(PublicApiError::bad_request(
                    "only grow-only independent volume resizing is supported",
                ));
            }
            let unchanged = target_size == row.size_gib
                && row.target_size_gib.is_none()
                && !row.delete_requested
                && row.phase != store::VolumePhase::Deleted;
            if !unchanged
                && !store::resource::request_growth(
                    &mut tx,
                    org.id,
                    id,
                    target_size,
                    operation.as_uuid(),
                    &admission
                        .qualified_growth_guest_images
                        .iter()
                        .map(ToString::to_string)
                        .collect::<Vec<_>>(),
                )
                .await
                .map_err(database_error)?
            {
                return Err(PublicApiError::conflict_with_reason(
                    "growth requires a fresh unreserved volume or a running read-write owner with no checkpoint or pending mutation",
                    "volume_resize_not_ready",
                ));
            }
            operation
        }
        CreatePublicOperationResult::Existing(operation) => operation,
        CreatePublicOperationResult::IdempotencyConflict { .. } => {
            return Err(PublicApiError::idempotency_conflict(
                "idempotency key conflicts",
            ));
        }
        CreatePublicOperationResult::AdmissionDenied => return Err(PublicApiError::internal()),
    };
    tx.commit().await.map_err(database_error)?;
    let record = PublicOperationStore::new(state.pool)
        .fetch_operation(org.id, operation)
        .await?
        .ok_or_else(PublicApiError::internal)?;
    Ok((StatusCode::ACCEPTED, Json(record)))
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
            let row = store::resource::get(&mut *tx, org.id, id)
                .await
                .map_err(database_error)?
                .ok_or_else(|| PublicApiError::not_found("volume not found"))?;
            let accepted = if row.retained {
                store::resource::request_delete(&mut tx, org.id, id, operation_id.as_uuid()).await
            } else {
                store::request_detached_delete(&mut tx, org.id, id).await
            };
            if !accepted.map_err(database_error)? {
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

#[derive(serde::Deserialize)]
pub(super) struct ListQuery {
    after: Option<Uuid>,
}

pub(super) async fn list(
    State(state): State<AppState>,
    Extension(AuthOrg(org)): Extension<AuthOrg>,
    owner: Option<Extension<AuthFederatedOwner>>,
    Query(query): Query<ListQuery>,
) -> Result<Json<serde_json::Value>, PublicApiError> {
    if owner.is_some() {
        return Err(PublicApiError::forbidden(
            "federated independent volumes are unsupported",
        ));
    }
    let rows = store::resource::list(&state.pool, org.id, query.after)
        .await
        .map_err(database_error)?;
    let next_after = (rows.len() == 100)
        .then(|| rows.last().map(|row| row.id))
        .flatten();
    let volumes: Vec<_> = rows
        .into_iter()
        .map(crate::block_volumes::public_volume)
        .collect();
    Ok(Json(
        serde_json::json!({"volumes": volumes, "next_after": next_after}),
    ))
}

pub(super) async fn create(
    State(state): State<AppState>,
    Extension(AuthOrg(org)): Extension<AuthOrg>,
    Extension(AuthUser(actor)): Extension<AuthUser>,
    owner: Option<Extension<AuthFederatedOwner>>,
    headers: HeaderMap,
    Json(request): Json<CreateVolumeRequest>,
) -> Result<(StatusCode, Json<PublicOperationRecord>), PublicApiError> {
    if owner.is_some() {
        return Err(PublicApiError::forbidden(
            "federated independent volumes are unsupported",
        ));
    }
    let policy = crate::block_volumes::independent_admission(
        &pplx_space_env_vars::ProcessEnvironment,
        org.id,
        org.byok_enabled,
    )
    .map_err(super::sandboxes::controller_error)?;
    if u32::from(request.size_gib) != 128 {
        return Err(PublicApiError::bad_request(
            "independent volumes currently require 128 GiB",
        ));
    }
    let key = idempotency_key(&headers)?;
    let payload = serde_json::to_value(&request).map_err(|_| PublicApiError::internal())?;
    let fingerprint = pplx_space_public_api::RequestFingerprint::parse(hex::encode(
        Sha256::digest(serde_json::to_vec(&payload).map_err(|_| PublicApiError::internal())?),
    ))
    .map_err(|_| PublicApiError::internal())?;
    let id = Uuid::now_v7();
    let target = id.to_string();
    let mut tx = state.pool.begin().await.map_err(database_error)?;
    let (created, _) = create_operation_tx(
        &mut tx,
        CreatePublicOperation {
            org_id: org.id,
            actor_user_id: Some(actor.id),
            api_product: ApiProduct::Native,
            kind: OperationKind::CreateVolume,
            target_type: Some(OperationTargetType::Volume),
            target_id: Some(&target),
            payload_json: payload,
            idempotency: Some(CreatePublicOperationIdempotency {
                method: HttpOperationMethod::Post,
                path_template: OperationPathTemplate::new("/v1/volumes"),
                key: &key,
                request_fingerprint: &fingerprint,
            }),
            traceparent: request_traceparent(),
        },
    )
    .await?;
    let operation = match created {
        CreatePublicOperationResult::Created(operation) => {
            store::resource::reserve(
                &mut tx,
                store::resource::NewResource {
                    id,
                    org_id: org.id,
                    managed_by: state.sandbox_controller.managed_by(),
                    availability_zone: &policy.availability_zone,
                    resource_limit: policy.resource_limit,
                },
            )
            .await
            .map_err(|error| match error {
                store::VolumeStoreError::Database(error) => database_error(error),
                _ => PublicApiError::conflict_with_reason(
                    "volume resource capacity exhausted",
                    "volume_capacity",
                ),
            })?;
            operation
        }
        CreatePublicOperationResult::Existing(operation) => operation,
        CreatePublicOperationResult::IdempotencyConflict { .. } => {
            return Err(PublicApiError::idempotency_conflict(
                "idempotency key conflicts",
            ));
        }
        CreatePublicOperationResult::AdmissionDenied => return Err(PublicApiError::internal()),
    };
    tx.commit().await.map_err(database_error)?;
    let record = PublicOperationStore::new(state.pool)
        .fetch_operation(org.id, operation)
        .await?
        .ok_or_else(PublicApiError::internal)?;
    Ok((StatusCode::ACCEPTED, Json(record)))
}
