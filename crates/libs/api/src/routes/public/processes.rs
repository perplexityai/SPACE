use pplx_space_common_core::types::OrgId;

use std::str::FromStr;

use axum::{
    Extension, Json,
    extract::{Path, Query, State},
    http::StatusCode,
};
use pplx_space_common_core::types::{ProcessId, ProcessSignal};
use pplx_space_metrics::response::{ResponseKind, ResponseOrigin};
use pplx_space_metrics::sandbox::api::SandboxApiMetricIdentity;
use pplx_space_public_api::{
    ProcessMutationResponse, ProcessSignalRequest, PublicPage, PublicProcess, PublicSandboxId,
};
use pplx_space_sbx_mgr_client::operations as spaced_ops;
use pplx_space_sbx_mgr_client::proto::{SpacedKillProcessRequest, SpacedSendSignalRequest};
use serde::Deserialize;
use serde_json::Value;

use crate::{
    controllers::ControllerError, middleware::auth::AuthOrg, models::Sandbox, routes::AppState,
};

use super::{
    cursors::{CursorKind, decode_cursor, encode_cursor},
    error::{PublicApiError, with_retry_after_hint},
    owner_scope::{AuthorizedSandbox, authorized_sandbox},
    sandboxes::parse_public_sandbox_id,
};

const DEFAULT_PROCESS_LIST_LIMIT: u16 = 100;
const MAX_PROCESS_LIST_LIMIT: u16 = 1000;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ProcessAction {
    Signal,
    Kill,
}

impl FromStr for ProcessAction {
    type Err = ();

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "signal" => Ok(Self::Signal),
            "kill" => Ok(Self::Kill),
            _ => Err(()),
        }
    }
}

pub async fn list_processes(
    State(state): State<AppState>,
    Extension(AuthOrg(org)): Extension<AuthOrg>,
    authorized: Option<Extension<AuthorizedSandbox>>,
    Path(sandbox_id): Path<String>,
    Query(query): Query<ListProcessesQuery>,
) -> Result<Json<PublicPage<PublicProcess>>, PublicApiError> {
    let cursor = query.cursor()?;
    let limit = query.limit();
    let processes = fetch_public_processes(authorized, &state, org.id, &sandbox_id).await?;

    Ok(Json(process_page(processes, cursor.as_ref(), limit)?))
}

#[derive(Debug, Deserialize)]
pub struct ListProcessesQuery {
    cursor: Option<String>,
    limit: Option<u16>,
}

impl ListProcessesQuery {
    fn cursor(&self) -> Result<Option<ProcessId>, PublicApiError> {
        self.cursor
            .as_deref()
            .map(|cursor| {
                decode_cursor(CursorKind::Process, cursor).and_then(|process_id| {
                    ProcessId::parse(process_id)
                        .map_err(|_| PublicApiError::bad_request("invalid cursor"))
                })
            })
            .transpose()
    }

    const fn limit(&self) -> u16 {
        match self.limit {
            Some(0) | None => DEFAULT_PROCESS_LIST_LIMIT,
            Some(limit) if limit > MAX_PROCESS_LIST_LIMIT => MAX_PROCESS_LIST_LIMIT,
            Some(limit) => limit,
        }
    }
}

pub async fn get_process(
    State(state): State<AppState>,
    Extension(AuthOrg(org)): Extension<AuthOrg>,
    authorized: Option<Extension<AuthorizedSandbox>>,
    Path((sandbox_id, process_id)): Path<(String, String)>,
) -> Result<Json<PublicProcess>, PublicApiError> {
    let process_id = ProcessId::parse(process_id)
        .map_err(|_| PublicApiError::bad_request("invalid process id"))?;
    let processes = fetch_public_processes(authorized, &state, org.id, &sandbox_id).await?;
    let Some(process) = processes
        .into_iter()
        .find(|process| process.process_id == process_id)
    else {
        return Err(PublicApiError::not_found("process not found"));
    };

    Ok(Json(process))
}

pub async fn apply_process_action(
    State(state): State<AppState>,
    Extension(AuthOrg(org)): Extension<AuthOrg>,
    authorized: Option<Extension<AuthorizedSandbox>>,
    Path((sandbox_id, process_id_and_action)): Path<(String, String)>,
    body: Option<Json<Value>>,
) -> Result<Json<ProcessMutationResponse>, PublicApiError> {
    let (process_id, action) = parse_process_action_segment(&process_id_and_action)?;
    match action {
        ProcessAction::Signal => {
            let request = body
                .map(|Json(value)| serde_json::from_value::<ProcessSignalRequest>(value))
                .transpose()
                .map_err(|_| PublicApiError::bad_request("invalid signal request"))?
                .ok_or_else(|| PublicApiError::bad_request("signal request body is required"))?;
            let signal = ProcessSignal::try_from_i32(request.signal)
                .map_err(|error| PublicApiError::bad_request(error.to_string()))?;
            apply_process_signal(state, authorized, org.id, sandbox_id, process_id, signal).await
        }
        ProcessAction::Kill => {
            apply_process_kill(state, authorized, org.id, sandbox_id, process_id).await
        }
    }
}

async fn apply_process_signal(
    state: AppState,
    authorized: Option<Extension<AuthorizedSandbox>>,
    org_id: OrgId,
    sandbox_id: String,
    process_id: ProcessId,
    signal: ProcessSignal,
) -> Result<Json<ProcessMutationResponse>, PublicApiError> {
    let sandbox = ensure_sandbox_owner(authorized, &state, org_id, &sandbox_id).await?;
    state
        .sandbox_controller
        .spaced_send_signal(
            &state.pool,
            &sandbox,
            SandboxApiMetricIdentity::native("process_signal"),
            SpacedSendSignalRequest {
                sandbox_id: sandbox.id.to_string(),
                operation: Some(spaced_ops::SendSignalRequest {
                    process_id: process_id.to_string(),
                    signal: signal.as_i32(),
                }),
            },
        )
        .await
        .map_err(controller_error)?;
    Ok(Json(ProcessMutationResponse { ok: true }))
}

async fn apply_process_kill(
    state: AppState,
    authorized: Option<Extension<AuthorizedSandbox>>,
    org_id: OrgId,
    sandbox_id: String,
    process_id: ProcessId,
) -> Result<Json<ProcessMutationResponse>, PublicApiError> {
    let sandbox = ensure_sandbox_owner(authorized, &state, org_id, &sandbox_id).await?;
    state
        .sandbox_controller
        .spaced_kill_process(
            &state.pool,
            &sandbox,
            SandboxApiMetricIdentity::native("process_kill"),
            SpacedKillProcessRequest {
                sandbox_id: sandbox.id.to_string(),
                operation: Some(spaced_ops::KillRequest {
                    process_id: process_id.to_string(),
                }),
            },
        )
        .await
        .map_err(controller_error)?;
    Ok(Json(ProcessMutationResponse { ok: true }))
}

fn parse_process_action_segment(value: &str) -> Result<(ProcessId, ProcessAction), PublicApiError> {
    let Some((process_id, action)) = value.rsplit_once(':') else {
        return Err(PublicApiError::bad_request(
            "process action path must use /v1/sandboxes/{sandbox_id}/processes/{process_id}:action",
        ));
    };
    if process_id.is_empty() || action.is_empty() {
        return Err(PublicApiError::bad_request("invalid process action path"));
    }
    let process_id = ProcessId::parse(process_id)
        .map_err(|_| PublicApiError::bad_request("invalid process id"))?;
    let action = action
        .parse::<ProcessAction>()
        .map_err(|_| PublicApiError::bad_request("invalid process action"))?;
    Ok((process_id, action))
}

async fn fetch_public_processes(
    authorized: Option<Extension<AuthorizedSandbox>>,
    state: &AppState,
    org_id: OrgId,
    sandbox_id: &str,
) -> Result<Vec<PublicProcess>, PublicApiError> {
    let sandbox = ensure_sandbox_owner(authorized, state, org_id, sandbox_id).await?;
    let sandbox_id = PublicSandboxId::from(sandbox.id);

    let response = state
        .sandbox_controller
        .spaced_list_processes(
            &state.pool,
            &sandbox,
            SandboxApiMetricIdentity::native("process_list"),
            sandbox_id.into(),
        )
        .await
        .map_err(controller_error)?;

    response.processes.into_iter().map(public_process).collect()
}

async fn ensure_sandbox_owner(
    authorized: Option<Extension<AuthorizedSandbox>>,
    state: &AppState,
    org_id: OrgId,
    sandbox_id: &str,
) -> Result<Sandbox, PublicApiError> {
    let sandbox_id = parse_public_sandbox_id(sandbox_id)?;
    authorized_sandbox(authorized, &state.pool, org_id, sandbox_id).await
}

pub(in crate::routes::public) fn controller_error(error: ControllerError) -> PublicApiError {
    // A lock race is this service's own database, not the sandbox fleet. The
    // status match below cannot tell it from an out-of-capacity 503, so it
    // would attribute it to the cluster or the node.
    if error.is_lock_contention() {
        return PublicApiError::lock_contention();
    }
    let retry_after = error.retry_after_header();
    match error.status() {
        StatusCode::BAD_REQUEST => PublicApiError::bad_request(error.to_string()),
        StatusCode::FORBIDDEN => PublicApiError::forbidden(error.to_string()),
        StatusCode::NOT_FOUND => PublicApiError::not_found(error.to_string()),
        StatusCode::CONFLICT => PublicApiError::conflict(error.to_string()),
        StatusCode::PRECONDITION_FAILED => PublicApiError::failed_precondition(error.to_string()),
        StatusCode::TOO_MANY_REQUESTS => with_retry_after_hint(
            PublicApiError::rate_limited(error.to_string())
                .with_classification(ResponseOrigin::Node, ResponseKind::Admission),
            retry_after,
        ),
        StatusCode::SERVICE_UNAVAILABLE => with_retry_after_hint(
            PublicApiError::service_unavailable(error.to_string())
                .with_classification(ResponseOrigin::Node, ResponseKind::BackendStatus),
            retry_after,
        ),
        status if status.is_server_error() => {
            tracing::warn!(%error, "native process operation failed");
            PublicApiError::internal()
                .with_classification(ResponseOrigin::Node, ResponseKind::BackendStatus)
        }
        _ => PublicApiError::internal(),
    }
}

fn public_process(info: spaced_ops::ProcessInfo) -> Result<PublicProcess, PublicApiError> {
    let process_id = ProcessId::parse(info.process_id).map_err(|error| {
        tracing::warn!(%error, "sandbox runtime returned an invalid process id");
        PublicApiError::internal()
            .with_classification(ResponseOrigin::Node, ResponseKind::BackendStatus)
    })?;
    Ok(PublicProcess {
        process_id,
        pid: info.pid,
        command: info.command,
        cwd: info.cwd,
        user: info.user,
        tag: info.tag,
    })
}

fn process_page(
    mut processes: Vec<PublicProcess>,
    cursor: Option<&ProcessId>,
    limit: u16,
) -> Result<PublicPage<PublicProcess>, PublicApiError> {
    processes.sort_unstable_by(|left, right| left.process_id.cmp(&right.process_id));
    if let Some(cursor) = cursor {
        processes.retain(|process| process.process_id.as_str() > cursor.as_str());
    }

    let has_more = processes.len() > usize::from(limit);
    if has_more {
        processes.truncate(usize::from(limit));
    }
    let next_cursor = if has_more {
        processes
            .last()
            .map(|process| encode_cursor(CursorKind::Process, process.process_id.to_string()))
            .transpose()?
    } else {
        None
    };

    Ok(PublicPage {
        data: processes,
        next_cursor,
    })
}

#[cfg(test)]
mod tests;
