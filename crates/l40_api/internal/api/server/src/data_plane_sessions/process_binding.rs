//! Binding a session to its process, recording its exit, and recording
//! reconnects.

use super::*;
use pplx_space_data_plane_session_store as data_plane_session_store;

pub(crate) async fn bind_session_process(
    pool: &sqlx::PgPool,
    request: BindDataPlaneSessionProcessRequest,
) -> Result<BindDataPlaneSessionProcessResponse, Status> {
    let session_id = required(&request.session_id, "session_id")?;
    let protocol_session_id = optional(request.protocol_session_id.as_deref());
    let process_id = required(&request.process_id, "process_id")?;
    let row = data_plane_session_store::bind_session_process(
        pool,
        &session_id,
        protocol_session_id.as_deref(),
        &process_id,
        request.records_exit,
    )
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

    let existing = data_plane_session_store::live_process_session_bound_process_id(
        pool,
        &session_id,
        protocol_session_id.as_deref(),
    )
    .await
    .map_err(|error| Status::internal(format!("lookup data-plane session process: {error}")))?;

    match existing {
        Some(existing_process_id) => Err(Status::already_exists(format!(
            "data-plane session already bound to process {existing_process_id}"
        ))),
        None => Err(Status::not_found(
            "process-backed data-plane session not found",
        )),
    }
}

/// Closes a terminal or process session whose bound process a stream saw
/// exit. Only a pending or open session bound to `process_id` closes: a
/// revoked one stays revoked, and a stale report about another process
/// changes nothing. Repeating it is a no-op that reports `closed: false`.
pub(crate) async fn record_session_process_exited(
    pool: &sqlx::PgPool,
    request: RecordDataPlaneSessionProcessExitedRequest,
) -> Result<RecordDataPlaneSessionProcessExitedResponse, Status> {
    let session_id = required(&request.session_id, "session_id")?;
    let protocol_session_id = optional(request.protocol_session_id.as_deref());
    let process_id = required(&request.process_id, "process_id")?;
    let closed = data_plane_session_store::close_exited_process_session(
        pool,
        &session_id,
        protocol_session_id.as_deref(),
        &process_id,
    )
    .await
    .map_err(|error| Status::internal(format!("record data-plane session exit: {error}")))?;
    Ok(RecordDataPlaneSessionProcessExitedResponse { closed })
}

pub(crate) async fn record_session_reconnect(
    pool: &sqlx::PgPool,
    request: RecordDataPlaneSessionReconnectRequest,
) -> Result<RecordDataPlaneSessionReconnectResponse, Status> {
    let session_id = required(&request.session_id, "session_id")?;
    let protocol_session_id = optional(request.protocol_session_id.as_deref());
    let reason = reconnect_reason(request.reason)?;
    let row = data_plane_session_store::record_reconnect(
        pool,
        &session_id,
        protocol_session_id.as_deref(),
        reason.as_str(),
    )
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
