//! `GET /v1/nodes/{node_id}/drain-v2-preview`: what a v2 drain would find on
//! one node, asked of the node and changing nothing there.

use axum::{
    Extension, Json,
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
};
use pplx_space_accounts::NodeId;
use pplx_space_common_core::types::NodeRole;
use pplx_space_postgres_pool::contention;
use pplx_space_sbx_mgr_client::proto::{DrainNodeResponse, DrainPrior};

use crate::middleware::auth::AuthUser;
use crate::routes::AppState;
use crate::routes::admin::node_store;
use crate::routes::admin::support::{api_error, read_db_error, require_managing_org_admin};

/// The longest a drain preview takes once its caller is authorized: reading
/// the node's rows, then finding its node-api, connecting to it, and the call
/// under the probe's own request timeout. The console waits longer than this
/// for a preview (`DRAIN_PREVIEW_TIMEOUT_MS` in
/// `ui/src/lib/drain-preview.ts`), so it covers the whole request.
pub(super) const DRAIN_PREVIEW_DEADLINE: std::time::Duration = std::time::Duration::from_secs(60);

/// Why a drain preview got no answer from the node.
#[derive(Clone, Debug, thiserror::Error)]
pub enum DrainPreviewError {
    /// A read of the node's rows from the admin API's database ran out of
    /// time, the preview's deadline or the database's own bounds, before the
    /// preview asked the node anything; the authorization checks ahead of the
    /// reads answer a pool timeout with 500, as on every admin endpoint.
    #[error("{0}")]
    DatabaseTimeout(String),
    /// The node's node-api couldn't be reached, or the call didn't get
    /// through to its sandbox-manager in time.
    #[error("{0}")]
    Unreachable(String),
    /// The node's node-api or sandbox-manager predates the preview.
    #[error("{0}")]
    Unsupported(String),
    /// The node refused or failed the preview otherwise.
    #[error("{0}")]
    Failed(tonic::Status),
}

impl DrainPreviewError {
    /// `status` the node answered a preview with, by what it means for the
    /// caller. The request carries only what a preview takes, so an
    /// `InvalidArgument` is a sandbox-manager that predates `preview`: it
    /// drops the field and refuses the zero epoch rather than sealing. A
    /// node-api or sandbox-manager without `DrainNode` answers
    /// `Unimplemented`. The client's own request timeout surfaces as
    /// `Cancelled` carrying [`tonic::TimeoutExpired`]; any other `Cancelled`,
    /// a handler that panicked among them, is a failure of the node's.
    pub(super) fn from_status(status: tonic::Status) -> Self {
        match status.code() {
            tonic::Code::Unimplemented | tonic::Code::InvalidArgument => {
                Self::Unsupported(format!(
                    "the node's sandbox-manager or node-api doesn't support drain previews: {status}"
                ))
            }
            tonic::Code::Unavailable | tonic::Code::DeadlineExceeded => {
                Self::Unreachable(format!("the node didn't answer: {status}"))
            }
            _ if timed_out(&status) => {
                Self::Unreachable(format!("the node didn't answer in time: {status}"))
            }
            _ => Self::Failed(status),
        }
    }

    /// The answer the admin API gives for it: the `error` message, and a
    /// `reason` a caller can tell the cases apart by.
    pub(super) fn response(&self) -> (StatusCode, Json<serde_json::Value>) {
        let (status, reason, error) = match self {
            Self::DatabaseTimeout(message) => (
                StatusCode::SERVICE_UNAVAILABLE,
                "database_timeout",
                message.clone(),
            ),
            Self::Unreachable(message) => (
                StatusCode::BAD_GATEWAY,
                "node_unreachable",
                format!("node unreachable: {message}"),
            ),
            Self::Unsupported(message) => (
                StatusCode::NOT_IMPLEMENTED,
                "node_version_unsupported",
                message.clone(),
            ),
            Self::Failed(status) => (
                StatusCode::BAD_GATEWAY,
                "node_preview_failed",
                format!("drain preview failed: {status}"),
            ),
        };
        (
            status,
            Json(serde_json::json!({ "error": error, "reason": reason })),
        )
    }
}

/// Whether `status` is the client's own request timeout: tonic reports it
/// with a [`tonic::TimeoutExpired`] in its source chain.
fn timed_out(status: &tonic::Status) -> bool {
    let mut source = std::error::Error::source(status);
    while let Some(error) = source {
        if error.is::<tonic::TimeoutExpired>() {
            return true;
        }
        source = error.source();
    }
    false
}

/// `response` with what the admin API itself would refuse a v2 drain of the
/// node for ([`super::drain_v2_node_refusals`]) ahead of its blockers, each a
/// node-level one. A drain the API refuses never starts, so a node with
/// one isn't `done`.
fn with_api_refusals(mut response: DrainNodeResponse, refusals: &[&str]) -> DrainNodeResponse {
    response.blocked.splice(
        0..0,
        refusals
            .iter()
            .map(|reason| pplx_space_sbx_mgr_client::proto::DrainBlocked {
                sandbox_id: None,
                reason: (*reason).to_owned(),
            }),
    );
    response.done &= refusals.is_empty();
    response
}

/// The preview as the admin API returns it.
pub(super) fn preview_body(node_id: NodeId, response: &DrainNodeResponse) -> serde_json::Value {
    let prior = |value: i32| match DrainPrior::try_from(value) {
        Ok(DrainPrior::Started) => "started",
        Ok(DrainPrior::Paused) => "paused",
        Ok(DrainPrior::Stopped) => "stopped",
        _ => "unknown",
    };
    serde_json::json!({
        "node_id": node_id,
        "sealed_epoch": response.sealed_epoch,
        "done": response.done,
        "evacuated": response.evacuated.iter().map(|entry| serde_json::json!({
            "sandbox_id": entry.sandbox_id,
            "prior": prior(entry.prior),
            "artifact_id": entry.artifact_id,
        })).collect::<Vec<_>>(),
        "pending": response.pending.iter().map(|entry| serde_json::json!({
            "sandbox_id": entry.sandbox_id,
            "action": entry.action,
        })).collect::<Vec<_>>(),
        "blocked": response.blocked.iter().map(|entry| serde_json::json!({
            "sandbox_id": entry.sandbox_id,
            "reason": entry.reason,
        })).collect::<Vec<_>>(),
    })
}

/// Ask the node what a v2 drain would find on it now, a `DrainNode` call
/// with `preview` set, through the client `:terminate`'s probe uses
/// ([`super::V2DrainProbe`]). The node seals nothing, starts nothing and
/// writes nothing, so this answers whether or not drain v2 is enabled. The
/// client reaches a Kubernetes node through its node-api pod and a Direct
/// node through the endpoint it registered, so either can be previewed.
/// Only a worker can: a builder's node-api serves no `DrainNode`. What
/// `:drain` would refuse a v2 drain of the node for, whatever the request
/// asks, is reported as node-level blockers beside the node's own, every
/// refusal where `:drain` answers with the first; when the node can't
/// answer, the error lists them as `refusals`. Past authorization, the
/// request is bounded by [`DRAIN_PREVIEW_DEADLINE`].
///
/// A read: it writes no node row, and being a `GET` it persists no request
/// audit event (`admin_request_should_persist_audit_event`). The admin reads
/// emit no `audit::record` line either; that is for actions.
pub(in crate::routes::admin) async fn preview_node_drain(
    State(state): State<AppState>,
    Extension(AuthUser(actor)): Extension<AuthUser>,
    Path(node_id): Path<NodeId>,
) -> axum::response::Response {
    if let Err(err) = require_managing_org_admin(&state.pool, &actor).await {
        return err.into_response();
    }
    preview_within(&state, node_id, DRAIN_PREVIEW_DEADLINE).await
}

/// [`preview_node_drain`] once its caller is authorized: the node's reads
/// and the node call, all within `deadline`. A preview that runs out says
/// which step it was in, so a slow database read doesn't read as a node
/// that can't be reached.
async fn preview_within(
    state: &AppState,
    node_id: NodeId,
    deadline: std::time::Duration,
) -> axum::response::Response {
    let until = tokio::time::Instant::now() + deadline;
    let ran_out = |step: &str| format!("the preview didn't finish within {deadline:?}: {step}");
    // The node read the other admin node actions build their target from:
    // it carries the endpoint a Direct node registered.
    let target = match read_node_rows(
        node_id,
        "the node",
        until,
        &ran_out,
        node_store::AdminNodeStore::new(state.pool.clone()).fetch(node_id),
    )
    .await
    {
        Ok(Some(node)) => crate::controllers::substrate::OwnedNodeApiTarget {
            managed_by: node.managed_by,
            role: node.role,
            direct_endpoint: node.node_api_endpoint,
            substrate_node_id: node.substrate_node_id,
        },
        Ok(None) => return api_error(StatusCode::NOT_FOUND, "Node not found").into_response(),
        Err(answer) => return *answer,
    };
    if target.role != NodeRole::Worker {
        return api_error(
            StatusCode::BAD_REQUEST,
            "a drain preview is only available for worker nodes",
        )
        .into_response();
    }
    let recorded_instance = match read_node_rows(
        node_id,
        "the node's drain columns",
        until,
        &ran_out,
        node_store::drain_v2_columns(&state.pool, node_id),
    )
    .await
    {
        Ok(Some(columns)) => columns.recorded_instance,
        Ok(None) => return api_error(StatusCode::NOT_FOUND, "Node not found").into_response(),
        Err(answer) => return *answer,
    };
    let refusals = super::drain_v2_node_refusals(target.managed_by, recorded_instance);
    let answer = tokio::time::timeout_at(until, state.v2_drain_probe.preview(state, &target))
        .await
        .unwrap_or_else(|_| {
            Err(DrainPreviewError::Unreachable(ran_out(
                "the node didn't answer the call",
            )))
        });
    match answer {
        Ok(response) => {
            let response = with_api_refusals(response, &refusals);
            Json(preview_body(node_id, &response)).into_response()
        }
        Err(error) => {
            tracing::warn!(%node_id, %error, "drain preview failed");
            failure_with_refusals(&error, &refusals).into_response()
        }
    }
}

/// One of the preview's reads of the node's rows, `read` naming it, by
/// `until`. A read that runs out of time answers 503 `database_timeout`,
/// naming the read, not an unreachable node: one the preview's deadline
/// cuts short, saying so through `ran_out`, and one the database's own
/// bounds end first ([`read_timed_out`]). Those bounds are shorter than the
/// deadline under the public defaults, so they are how a slow read usually
/// ends. Any other failure of the read is answered as a read's
/// ([`read_db_error`]).
async fn read_node_rows<T>(
    node_id: NodeId,
    read: &str,
    until: tokio::time::Instant,
    ran_out: impl Fn(&str) -> String,
    query: impl std::future::Future<Output = Result<T, sqlx::Error>>,
) -> Result<T, Box<axum::response::Response>> {
    let error = match tokio::time::timeout_at(until, query).await {
        Ok(Ok(rows)) => return Ok(rows),
        Ok(Err(err)) => match read_timed_out(&err) {
            Some(why) => DrainPreviewError::DatabaseTimeout(format!(
                "the preview's read of {read} from the database timed out: {why}"
            )),
            None => return Err(Box::new(read_db_error(err))),
        },
        Err(_) => DrainPreviewError::DatabaseTimeout(ran_out(&format!(
            "it was reading {read} from the database"
        ))),
    };
    tracing::warn!(%node_id, %error, "drain preview timed out");
    Err(Box::new(error.response().into_response()))
}

/// Why `err` is a read the database's own bounds ended, or `None` when it is
/// another failure: the pool handed out no connection within its acquire
/// timeout, the statement was canceled (`57014`, its `statement_timeout` or a
/// cancel), or it waited past `lock_timeout` for a lock (`55P03`).
fn read_timed_out(err: &sqlx::Error) -> Option<&'static str> {
    if matches!(err, sqlx::Error::PoolTimedOut) {
        Some(
            "the pool handed out no connection within its acquire timeout (all in use, or a new \
             one couldn't be opened)",
        )
    } else if contention::is_query_canceled(err) {
        Some("the statement was canceled, by its statement timeout or a cancel")
    } else if contention::is_lock_not_available(err) {
        Some("the statement waited past its lock timeout")
    } else {
        None
    }
}

/// `error`'s answer ([`DrainPreviewError::response`]) with what `:drain`
/// would refuse a v2 drain of the node for, every refusal, as `refusals`:
/// the node didn't answer, but the admin API's own refusals stand.
fn failure_with_refusals(
    error: &DrainPreviewError,
    refusals: &[&str],
) -> (StatusCode, Json<serde_json::Value>) {
    let (status, Json(mut body)) = error.response();
    if let Some(fields) = body.as_object_mut() {
        fields.insert("refusals".to_owned(), serde_json::json!(refusals));
    }
    (status, Json(body))
}

#[cfg(test)]
mod tests;
