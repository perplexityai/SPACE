//! Session and org revocation state.

use super::*;

#[derive(Debug, Clone, FromRow)]
pub(super) struct RevocationRow {
    pub(super) session_id: String,
    pub(super) protocol_session_id: Option<String>,
    pub(super) org_id: OrgId,
    pub(super) revocation_epoch: i64,
    pub(super) revocation_status: String,
    pub(super) revocation_reason: Option<String>,
    pub(super) revocation_updated_at: OffsetDateTime,
    pub(super) org_revocation_epoch: i64,
}

#[derive(Debug, Clone, FromRow)]
pub(super) struct OrgRevocationEpochRow {
    pub(super) epoch: i64,
    pub(super) updated_at: Option<OffsetDateTime>,
}

pub(crate) async fn revocation_state_for_session(
    pool: &sqlx::PgPool,
    request: GetDataPlaneSessionRevocationStateRequest,
) -> Result<GetDataPlaneSessionRevocationStateResponse, Status> {
    let session_id = required(&request.session_id, "session_id")?;
    let protocol_session_id = optional(request.protocol_session_id.as_deref());
    let row = sqlx::query_as::<_, RevocationRow>(sql!(
        "SELECT d.session_id,
                d.protocol_session_id,
                d.org_id,
                d.revocation_epoch,
                d.revocation_status,
                d.revocation_reason,
                d.revocation_updated_at,
                COALESCE(o.epoch, 0) AS org_revocation_epoch
         FROM data_plane_sessions d
         LEFT JOIN data_plane_org_revocation_epochs o ON o.org_id = d.org_id
         WHERE d.session_id = $1
           AND ($2::TEXT IS NULL OR d.protocol_session_id = $2)
           AND (d.expires_at IS NULL OR d.expires_at > NOW())"
    ))
    .bind(&session_id)
    .bind(protocol_session_id.as_deref())
    .fetch_optional(pool)
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
    let row = sqlx::query_as::<_, OrgRevocationEpochRow>(sql!(
        "SELECT epoch, updated_at
         FROM data_plane_org_revocation_epochs
         WHERE org_id = $1"
    ))
    .bind(org_id)
    .fetch_optional(pool)
    .await
    .map_err(|error| Status::internal(format!("get data-plane org revocation epoch: {error}")))?
    .unwrap_or(OrgRevocationEpochRow {
        epoch: 0,
        updated_at: None,
    });

    Ok(GetDataPlaneOrgRevocationEpochResponse {
        org_id: org_id.to_string(),
        epoch: i64_to_u64(row.epoch, "org_revocation_epoch")?,
        updated_at: row.updated_at.map(timestamp),
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
