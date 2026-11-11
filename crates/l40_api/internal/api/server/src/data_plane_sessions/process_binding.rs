//! Binding a session to its process and recording reconnects.

use super::*;

#[derive(Debug, Clone, FromRow)]
pub(super) struct BoundProcessRow {
    pub(super) session_id: String,
    pub(super) protocol_session_id: Option<String>,
    pub(super) backend_process_id: String,
    pub(super) newly_bound: bool,
}

#[derive(Debug, Clone, FromRow)]
pub(super) struct ExistingProcessBindingRow {
    pub(super) backend_process_id: Option<String>,
}

#[derive(Debug, Clone, FromRow)]
pub(super) struct ReconnectRow {
    pub(super) session_id: String,
    pub(super) protocol_session_id: Option<String>,
    pub(super) reconnect_count: i64,
    pub(super) last_reconnect_reason: String,
}

pub(crate) async fn bind_session_process(
    pool: &sqlx::PgPool,
    request: BindDataPlaneSessionProcessRequest,
) -> Result<BindDataPlaneSessionProcessResponse, Status> {
    let session_id = required(&request.session_id, "session_id")?;
    let protocol_session_id = optional(request.protocol_session_id.as_deref());
    let process_id = required(&request.process_id, "process_id")?;
    let row = sqlx::query_as::<_, BoundProcessRow>(sql!(
        "WITH target AS (
             SELECT session_id,
                    protocol_session_id,
                    backend_process_id IS NULL AS newly_bound
             FROM data_plane_sessions
             WHERE session_id = $1
               AND ($2::TEXT IS NULL OR protocol_session_id = $2)
               AND kind IN ('terminal', 'process')
               AND (expires_at IS NULL OR expires_at > NOW())
               AND (backend_process_id IS NULL OR backend_process_id = $3)
             FOR UPDATE
         ),
         updated AS (
             UPDATE data_plane_sessions d
             SET backend_process_id = COALESCE(d.backend_process_id, $3),
                 backend_process_bound_at = CASE
                     WHEN d.backend_process_id IS NULL THEN NOW()
                     ELSE d.backend_process_bound_at
                 END,
                 updated_at = NOW()
             FROM target
             WHERE d.session_id = target.session_id
             RETURNING d.session_id,
                       d.protocol_session_id,
                       d.backend_process_id,
                       target.newly_bound
         )
         SELECT session_id,
                protocol_session_id,
                backend_process_id,
                newly_bound
         FROM updated"
    ))
    .bind(&session_id)
    .bind(protocol_session_id.as_deref())
    .bind(&process_id)
    .fetch_optional(pool)
    .await
    .map_err(|error| Status::internal(format!("bind data-plane session process: {error}")))?;

    if let Some(row) = row {
        return Ok(BindDataPlaneSessionProcessResponse {
            session_id: row.session_id,
            protocol_session_id: row.protocol_session_id,
            process_id: row.backend_process_id,
            newly_bound: row.newly_bound,
        });
    }

    let existing = sqlx::query_as::<_, ExistingProcessBindingRow>(sql!(
        "SELECT backend_process_id
         FROM data_plane_sessions
         WHERE session_id = $1
           AND ($2::TEXT IS NULL OR protocol_session_id = $2)
           AND kind IN ('terminal', 'process')
           AND (expires_at IS NULL OR expires_at > NOW())"
    ))
    .bind(&session_id)
    .bind(protocol_session_id.as_deref())
    .fetch_optional(pool)
    .await
    .map_err(|error| Status::internal(format!("lookup data-plane session process: {error}")))?;

    match existing.and_then(|row| row.backend_process_id) {
        Some(existing_process_id) => Err(Status::already_exists(format!(
            "data-plane session already bound to process {existing_process_id}"
        ))),
        None => Err(Status::not_found(
            "process-backed data-plane session not found",
        )),
    }
}

pub(crate) async fn record_session_reconnect(
    pool: &sqlx::PgPool,
    request: RecordDataPlaneSessionReconnectRequest,
) -> Result<RecordDataPlaneSessionReconnectResponse, Status> {
    let session_id = required(&request.session_id, "session_id")?;
    let protocol_session_id = optional(request.protocol_session_id.as_deref());
    let reason = reconnect_reason(request.reason)?;
    let row = sqlx::query_as::<_, ReconnectRow>(sql!(
        "UPDATE data_plane_sessions
         SET reconnect_count = reconnect_count + 1,
             last_reconnect_reason = $3,
             updated_at = NOW()
         WHERE session_id = $1
           AND ($2::TEXT IS NULL OR protocol_session_id = $2)
           AND (expires_at IS NULL OR expires_at > NOW())
         RETURNING session_id,
                   protocol_session_id,
                   reconnect_count,
                   last_reconnect_reason"
    ))
    .bind(&session_id)
    .bind(protocol_session_id.as_deref())
    .bind(reason.as_str())
    .fetch_optional(pool)
    .await
    .map_err(|error| Status::internal(format!("record data-plane session reconnect: {error}")))?
    .ok_or_else(|| Status::not_found("data-plane session not found"))?;

    let reconnect_count = u64::try_from(row.reconnect_count).map_err(|_| {
        Status::internal(format!(
            "stored reconnect_count is invalid: {}",
            row.reconnect_count
        ))
    })?;
    let last_reconnect_reason = reconnect_reason_from_storage(&row.last_reconnect_reason)?;
    Ok(RecordDataPlaneSessionReconnectResponse {
        session_id: row.session_id,
        protocol_session_id: row.protocol_session_id,
        reconnect_count,
        last_reconnect_reason: last_reconnect_reason as i32,
    })
}

pub(super) fn reconnect_reason(value: i32) -> Result<DataPlaneReconnectReason, Status> {
    let reason = DataPlaneReconnectReason::try_from(value)
        .map_err(|_| Status::invalid_argument("reconnect reason is invalid"))?;
    if reason == DataPlaneReconnectReason::Unspecified {
        return Err(Status::invalid_argument("reconnect reason is required"));
    }
    Ok(reason)
}

pub(super) fn reconnect_reason_from_storage(
    value: &str,
) -> Result<DataPlaneReconnectReason, Status> {
    match value {
        "backend_restart" => Ok(DataPlaneReconnectReason::BackendRestart),
        "sandbox_migration" => Ok(DataPlaneReconnectReason::SandboxMigration),
        "route_generation_changed" => Ok(DataPlaneReconnectReason::RouteGenerationChanged),
        "worker_unavailable" => Ok(DataPlaneReconnectReason::WorkerUnavailable),
        "data_plane_restart" => Ok(DataPlaneReconnectReason::DataPlaneRestart),
        other => Err(Status::internal(format!(
            "stored reconnect reason is invalid: {other}"
        ))),
    }
}

pub(super) trait DataPlaneReconnectReasonExt {
    fn as_str(&self) -> &'static str;
}

impl DataPlaneReconnectReasonExt for DataPlaneReconnectReason {
    fn as_str(&self) -> &'static str {
        match self {
            DataPlaneReconnectReason::BackendRestart => "backend_restart",
            DataPlaneReconnectReason::SandboxMigration => "sandbox_migration",
            DataPlaneReconnectReason::RouteGenerationChanged => "route_generation_changed",
            DataPlaneReconnectReason::WorkerUnavailable => "worker_unavailable",
            DataPlaneReconnectReason::DataPlaneRestart => "data_plane_restart",
            DataPlaneReconnectReason::Unspecified => "unspecified",
        }
    }
}
