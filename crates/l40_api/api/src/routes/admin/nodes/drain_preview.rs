//! `GET /v1/nodes/{node_id}/drain-v2-preview`: what an evacuating v2 drain
//! would find on one unsealed node now, asked of the node and changing
//! nothing there. Before its deadline a drain holds each running sandbox it
//! could suspend, `pending` with the wait `hold`, whatever this preview lists
//! for it. The node skips node-manager's orphan inventory for a preview, so
//! a drain may find more node-wide blockers than a preview does.

use axum::{Json, http::StatusCode, response::IntoResponse};
use pplx_space_accounts::NodeId;
use pplx_space_node_drain::NodeDrainEngine;
use pplx_space_node_model::NodeRole;
use pplx_space_node_registration_store::drain_v2::{
    StoredV2ArtifactKind, StoredV2Blocked, StoredV2Evacuated, StoredV2Pending, StoredV2Wait,
};
use pplx_space_postgres_pool::contention;
use pplx_space_sbx_mgr_client::proto::{ArtifactKind, DrainReport, DrainWait};

use crate::routes::AppState;
use crate::routes::admin::node_store;
use crate::routes::admin::support::{api_error, read_db_error};

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
    /// Another preview was in progress on the node (`Aborted`): retryable
    /// contention, which changed nothing.
    #[error("{0}")]
    Busy(String),
    /// The node is sealed against VM activations (`FailedPrecondition`): a
    /// preview previews only an unsealed node.
    #[error("{0}")]
    Sealed(String),
    /// The node refused or failed the preview otherwise.
    #[error("{0}")]
    Failed(tonic::Status),
}

impl DrainPreviewError {
    /// `status` the node answered a preview with, by what it means for the
    /// caller. A node-api or sandbox-manager without `PreviewDrainNode`
    /// answers `Unimplemented`. The client's own request timeout surfaces as
    /// `Cancelled` carrying [`tonic::TimeoutExpired`]; any other `Cancelled`,
    /// a handler that panicked among them, is a failure of the node's.
    pub(super) fn from_status(status: tonic::Status) -> Self {
        match status.code() {
            tonic::Code::Unimplemented => Self::Unsupported(format!(
                "the node's sandbox-manager or node-api doesn't support drain previews: {status}"
            )),
            tonic::Code::Unavailable | tonic::Code::DeadlineExceeded => {
                Self::Unreachable(format!("the node didn't answer: {status}"))
            }
            tonic::Code::Aborted => Self::Busy(format!(
                "another drain preview is in progress on the node; retry shortly: {status}"
            )),
            tonic::Code::FailedPrecondition => Self::Sealed(status.message().to_owned()),
            _ if timed_out(&status) => {
                Self::Unreachable(format!("the node didn't answer in time: {status}"))
            }
            _ => Self::Failed(status),
        }
    }

    /// How long a caller is told to wait before it asks again, for a
    /// refusal it should retry.
    pub(super) const fn retry_after(&self) -> Option<std::time::Duration> {
        match self {
            Self::Busy(_) => Some(DRAIN_PREVIEW_BUSY_RETRY_AFTER),
            _ => None,
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
            Self::Busy(message) => (
                StatusCode::SERVICE_UNAVAILABLE,
                "node_busy",
                message.clone(),
            ),
            Self::Sealed(message) => (
                StatusCode::CONFLICT,
                ORPHANED_SEAL_REASON,
                format!(
                    "the node is sealed against VM activations, but no v2 drain of it is \
                     recorded: an orphaned seal. With drain v2 enabled and no `refusals` listed, \
                     `:drain {{engine: v2, hold_seconds: N}}` then `:undrain` heals it. \
                     Otherwise, send the node a `DrainNode` cancel under the generation it is \
                     sealed under (docs/troubleshooting.md, 'An orphaned seal'). The node said: \
                     {message}"
                ),
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

/// How long a preview refused because another one was in progress is told
/// to wait before it asks again.
pub(super) const DRAIN_PREVIEW_BUSY_RETRY_AFTER: std::time::Duration =
    std::time::Duration::from_secs(5);

/// The `reason` of the 409 a preview of a node with a v2 drain gets.
pub(super) const DRAIN_IN_PROGRESS_REASON: &str = "drain_v2_in_progress";

/// The `reason` of the 409 a preview of a sealed node with no v2 drain
/// recorded gets.
pub(super) const ORPHANED_SEAL_REASON: &str = "orphaned_seal";

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

/// `report` with what the admin API itself would refuse a v2 drain of the
/// node for ([`super::drain_v2_node_refusals`]) ahead of its blockers, each a
/// node-level one. A drain the API refuses never starts, so a drain of a
/// node with one would never finish.
fn with_api_refusals(mut report: DrainReport, refusals: &[&str]) -> DrainReport {
    report.blocked.splice(
        0..0,
        refusals
            .iter()
            .map(|reason| pplx_space_sbx_mgr_client::proto::DrainBlocked {
                reason: (*reason).to_owned(),
            }),
    );
    report
}

/// The preview as the admin API returns it: the node's report, each entry
/// under the keys of the stored v2 report (`StoredV2Report`), a wait or an
/// artifact kind this build doesn't know read as `unspecified` and
/// `unknown`.
pub(super) fn preview_body(node_id: NodeId, report: &DrainReport) -> serde_json::Value {
    let artifact_kind = |value: i32| match ArtifactKind::try_from(value) {
        Ok(ArtifactKind::Full) => StoredV2ArtifactKind::Full,
        Ok(ArtifactKind::Disk) => StoredV2ArtifactKind::Disk,
        _ => StoredV2ArtifactKind::Unknown,
    };
    let wait = |value: i32| match DrainWait::try_from(value) {
        Ok(DrainWait::Hold) => StoredV2Wait::Hold,
        Ok(DrainWait::Moving) => StoredV2Wait::Moving,
        Ok(DrainWait::External) => StoredV2Wait::External,
        Ok(DrainWait::Unspecified) | Err(_) => StoredV2Wait::Unspecified,
    };
    let evacuated: Vec<StoredV2Evacuated> = report
        .evacuated
        .iter()
        .map(|entry| StoredV2Evacuated {
            sandbox_id: entry.sandbox_id.clone(),
            artifact_kind: artifact_kind(entry.artifact_kind),
            artifact_id: entry.artifact_id.clone(),
        })
        .collect();
    let pending: Vec<StoredV2Pending> = report
        .pending
        .iter()
        .map(|entry| StoredV2Pending {
            sandbox_id: entry.sandbox_id.clone(),
            wait: wait(entry.wait),
            running: entry.running,
            reason: entry.reason.clone(),
            since: entry.since.as_ref().map(|since| since.seconds),
            pool_member: entry.pool_member,
        })
        .collect();
    let blocked: Vec<StoredV2Blocked> = report
        .blocked
        .iter()
        .map(|entry| StoredV2Blocked {
            reason: entry.reason.clone(),
        })
        .collect();
    serde_json::json!({
        "node_id": node_id,
        "evacuated": evacuated,
        "pending": pending,
        "blocked": blocked,
    })
}

/// [`preview_node_drain`] once its caller is authorized: the node's reads
/// and the node call, all within `deadline`. A preview that runs out says
/// which step it was in, so a slow database read doesn't read as a node
/// that can't be reached.
///
/// A node with a v2 drain recorded, its cancel included, is sealed or about
/// to be: the preview answers 409 ([`DRAIN_IN_PROGRESS_REASON`]) without
/// asking it. A node that refuses as sealed while no v2 drain of it is
/// recorded has an orphaned seal: the preview answers 409
/// ([`ORPHANED_SEAL_REASON`]), warns and counts it
/// (`node_drain.v2.orphaned_seal_refusals`). A node busy with another
/// preview answers 503 with `Retry-After`.
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
    let columns = match read_node_rows(
        node_id,
        "the node's drain columns",
        until,
        &ran_out,
        pplx_space_node_registration_store::drain_v2::drain_v2_columns(&state.pool, node_id),
    )
    .await
    {
        Ok(Some(columns)) => columns,
        Ok(None) => return api_error(StatusCode::NOT_FOUND, "Node not found").into_response(),
        Err(answer) => return *answer,
    };
    if columns.engine == NodeDrainEngine::V2 {
        return drain_in_progress().into_response();
    }
    let recorded_instance = columns.recorded_instance;
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
            let report = with_api_refusals(response.report.unwrap_or_default(), &refusals);
            Json(preview_body(node_id, &report)).into_response()
        }
        Err(DrainPreviewError::Sealed(message)) => {
            // A drain started since the columns were read seals the node
            // legitimately; only a seal no drain records is orphaned. The
            // re-read is bounded by the same deadline as the preview's other
            // reads.
            match read_node_rows(
                node_id,
                "the node's drain columns",
                until,
                &ran_out,
                pplx_space_node_registration_store::drain_v2::drain_v2_columns(
                    &state.pool,
                    node_id,
                ),
            )
            .await
            {
                Ok(Some(columns)) if columns.engine == NodeDrainEngine::V2 => {
                    drain_in_progress().into_response()
                }
                // The node's row went while the node answered: it is gone,
                // not orphaned.
                Ok(None) => api_error(StatusCode::NOT_FOUND, "Node not found").into_response(),
                Ok(Some(_)) => {
                    tracing::warn!(
                        %node_id,
                        node_said = %message,
                        "the node refused a drain preview as sealed, but no v2 drain of it is \
                         recorded: an orphaned seal"
                    );
                    crate::metrics::node_drain::v2_orphaned_seal_refusal(&node_id.to_string());
                    failure_with_refusals(&DrainPreviewError::Sealed(message), &refusals)
                }
                Err(answer) => *answer,
            }
        }
        Err(error) => {
            tracing::warn!(%node_id, %error, "drain preview failed");
            failure_with_refusals(&error, &refusals)
        }
    }
}

/// The 409 a preview of a node with a v2 drain recorded gets.
fn drain_in_progress() -> (StatusCode, Json<serde_json::Value>) {
    (
        StatusCode::CONFLICT,
        Json(serde_json::json!({
            "error": "a v2 drain of the node is recorded, so the node is sealed or about to \
                      be; a preview previews only a node no drain holds",
            "reason": DRAIN_IN_PROGRESS_REASON,
        })),
    )
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
/// the node didn't answer, but the admin API's own refusals stand. A
/// refusal to retry carries `Retry-After`.
fn failure_with_refusals(error: &DrainPreviewError, refusals: &[&str]) -> axum::response::Response {
    let (status, Json(mut body)) = error.response();
    if let Some(fields) = body.as_object_mut() {
        fields.insert("refusals".to_owned(), serde_json::json!(refusals));
    }
    let mut response = (status, Json(body)).into_response();
    if let Some(value) = error
        .retry_after()
        .and_then(crate::controllers::retry_after_header_value)
    {
        response
            .headers_mut()
            .insert(axum::http::header::RETRY_AFTER, value);
    }
    response
}

#[cfg(test)]
mod tests;
