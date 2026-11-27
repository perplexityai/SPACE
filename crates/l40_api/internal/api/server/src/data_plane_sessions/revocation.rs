//! Session and org revocation state.

use super::*;
use pplx_space_data_plane_session_store as data_plane_session_store;

pub(crate) async fn revocation_state_for_session(
    pool: &sqlx::PgPool,
    request: GetDataPlaneSessionRevocationStateRequest,
) -> Result<GetDataPlaneSessionRevocationStateResponse, Status> {
    let session_id = required(&request.session_id, "session_id")?;
    let protocol_session_id = optional(request.protocol_session_id.as_deref());
    let row = data_plane_session_store::live_session_revocation(
        pool,
        &session_id,
        protocol_session_id.as_deref(),
    )
    .await
    .map_err(|error| Status::internal(format!("get data-plane revocation state: {error}")))?
    .ok_or_else(|| Status::not_found("data-plane session not found"))?;

    Ok(GetDataPlaneSessionRevocationStateResponse {
        session_id: row.session_id,
        protocol_session_id: row.protocol_session_id,
        revocation: Some(revocation_state(
            row.revocation_epoch,
            &row.revocation_status,
            row.revocation_updated_at,
            row.revocation_reason,
            row.org_revocation_epoch,
        )?),
        org_id: row.org_id.to_string(),
    })
}

pub(crate) async fn org_revocation_epoch(
    pool: &sqlx::PgPool,
    request: GetDataPlaneOrgRevocationEpochRequest,
) -> Result<GetDataPlaneOrgRevocationEpochResponse, Status> {
    let org_id = required(&request.org_id, "org_id")?
        .parse::<Uuid>()
        .map_err(|_| Status::invalid_argument("org_id must be a UUID"))?;
    let (epoch, updated_at) =
        data_plane_session_store::org_revocation_epoch(pool, OrgId::from_uuid(org_id))
            .await
            .map_err(|error| {
                Status::internal(format!("get data-plane org revocation epoch: {error}"))
            })?
            .map_or((0, None), |row| (row.epoch, Some(row.updated_at)));

    Ok(GetDataPlaneOrgRevocationEpochResponse {
        org_id: org_id.to_string(),
        epoch: i64_to_u64(epoch, "org_revocation_epoch")?,
        updated_at: updated_at.map(timestamp),
    })
}

pub(super) fn i64_to_u64(value: i64, field: &str) -> Result<u64, Status> {
    u64::try_from(value).map_err(|_| Status::internal(format!("{field} is negative")))
}

pub(super) fn revocation_state(
    epoch: i64,
    status: &str,
    updated_at: OffsetDateTime,
    reason: Option<String>,
    org_epoch: i64,
) -> Result<DataPlaneRevocationState, Status> {
    Ok(DataPlaneRevocationState {
        epoch: i64_to_u64(epoch, "revocation_epoch")?,
        status: revocation_status(status)? as i32,
        updated_at: Some(timestamp(updated_at)),
        reason,
        org_epoch: i64_to_u64(org_epoch, "org_revocation_epoch")?,
    })
}

pub(super) fn revocation_status(status: &str) -> Result<DataPlaneRevocationStatus, Status> {
    match status {
        "active" => Ok(DataPlaneRevocationStatus::Active),
        "revoking" => Ok(DataPlaneRevocationStatus::Revoking),
        "revoked" => Ok(DataPlaneRevocationStatus::Revoked),
        other => Err(Status::internal(format!(
            "stored data-plane revocation status is invalid: {other}"
        ))),
    }
}

pub(super) fn timestamp(value: OffsetDateTime) -> prost_types::Timestamp {
    prost_types::Timestamp {
        seconds: value.unix_timestamp(),
        nanos: value.nanosecond() as i32,
    }
}
