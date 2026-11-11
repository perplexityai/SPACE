//! Whether a sandbox's runtime may be accessed now, and the retry advice when it may not.

use super::*;

pub(super) const SANDBOX_RUNTIME_CONVERGENCE_RETRY_AFTER_MS: u64 = 5_000;

pub(super) const POST_WAKE_LISTENER_REBIND_GRACE_SECS: f64 = 150.0;

#[derive(Debug, Clone, FromRow)]
pub(super) struct SandboxRuntimeAccessRow {
    pub(super) lifecycle: String,
    pub(super) lifecycle_changed_recently: bool,
}

pub(crate) async fn sandbox_runtime_access(
    pool: &sqlx::PgPool,
    request: GetSandboxRuntimeAccessRequest,
) -> Result<GetSandboxRuntimeAccessResponse, Status> {
    let sandbox_id = required(&request.sandbox_id, "sandbox_id")?
        .parse::<SandboxId>()
        .map_err(|_| Status::invalid_argument("sandbox_id must be a UUID"))?;
    let row = fetch_sandbox_runtime_access_row(pool, sandbox_id).await?;

    Ok(GetSandboxRuntimeAccessResponse {
        access: Some(sandbox_runtime_access_from_row(row)),
    })
}

pub(super) async fn fetch_sandbox_runtime_access_row(
    pool: &sqlx::PgPool,
    sandbox_id: SandboxId,
) -> Result<SandboxRuntimeAccessRow, Status> {
    sqlx::query_as::<_, SandboxRuntimeAccessRow>(sql!(
        "SELECT lifecycle,
                lifecycle_changed_at > NOW() - make_interval(secs => $2)
                    AS lifecycle_changed_recently
         FROM sandboxes
         WHERE id = $1"
    ))
    .bind(sandbox_id)
    .bind(POST_WAKE_LISTENER_REBIND_GRACE_SECS)
    .fetch_optional(pool)
    .await
    .map_err(|error| Status::internal(format!("get sandbox runtime access: {error}")))?
    .ok_or_else(|| Status::not_found("sandbox not found"))
}

pub(super) fn sandbox_runtime_access_from_row(
    row: SandboxRuntimeAccessRow,
) -> SandboxRuntimeAccess {
    let lifecycle = row
        .lifecycle
        .trim()
        .to_ascii_lowercase()
        .parse::<SandboxLifecycle>()
        .ok();
    let decision = sandbox_runtime_access_decision(lifecycle, row.lifecycle_changed_recently);
    SandboxRuntimeAccess {
        decision: decision as i32,
        retry_after_ms: if decision == SandboxRuntimeAccessDecision::RetryableConverging {
            SANDBOX_RUNTIME_CONVERGENCE_RETRY_AFTER_MS
        } else {
            0
        },
    }
}

pub(super) fn sandbox_runtime_access_decision(
    lifecycle: Option<SandboxLifecycle>,
    lifecycle_changed_recently: bool,
) -> SandboxRuntimeAccessDecision {
    match lifecycle {
        Some(
            SandboxLifecycle::Failed | SandboxLifecycle::Quarantined | SandboxLifecycle::Stopped,
        ) => SandboxRuntimeAccessDecision::TerminalUnavailable,
        Some(
            SandboxLifecycle::Suspended
            | SandboxLifecycle::Suspending
            | SandboxLifecycle::Pausing
            | SandboxLifecycle::Paused
            | SandboxLifecycle::Restoring
            | SandboxLifecycle::Resuming
            | SandboxLifecycle::Starting,
        ) => SandboxRuntimeAccessDecision::RetryableConverging,
        Some(SandboxLifecycle::Started) if lifecycle_changed_recently => {
            SandboxRuntimeAccessDecision::RetryableConverging
        }
        Some(
            SandboxLifecycle::Created
            | SandboxLifecycle::Creating
            | SandboxLifecycle::Started
            | SandboxLifecycle::Stopping
            | SandboxLifecycle::Deleting
            | SandboxLifecycle::Checkpointing,
        ) => SandboxRuntimeAccessDecision::Ready,
        None => SandboxRuntimeAccessDecision::Unknown,
    }
}
