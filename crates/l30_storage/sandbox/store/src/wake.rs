//! Writes of `sandboxes` that finish bringing a sandbox up: completing a wake
//! or restore, including the redrive of one whose request was cancelled;
//! clearing the command-ready marker a preserved slow create leaves, once an
//! access confirms the guest's command API is up; and, on Kubernetes, renewing
//! a restore's lease while it claims the active execution and recording the
//! node a sandbox's pod was scheduled on. That placement write runs wherever a
//! pod is created or resolved, cold creates included, not only on a wake.
//!
//! The callers keep everything around them: the sandbox and execution row
//! locks (`SELECT ... FOR UPDATE`), the transactions, the execution writes
//! beside them, and the read that tells a lost completion CAS apart from one
//! a suspend superseded. Every function runs on the executor its caller
//! passes, so it composes into the caller's transaction when there is one.

use pplx_space_common_core::types::{NodeId, SandboxId, SubstrateKind};

use pplx_space_common_core::sql;
use pplx_space_sandbox_routability::routable_execution_or_non_k8s;
use uuid::Uuid;

/// Flip a woken sandbox to `started`/`on_node`. Returns whether the row
/// flipped.
///
/// Writes an **operation step**: it ends the wake. The `WHERE` refuses a row
/// in a lifecycle the wake must not resurrect (`failed`, `quarantined`,
/// `stopped`, `stopping`, `suspending`, `deleting`, `deleted`,
/// `cleanup_required`), compared case- and whitespace-insensitively. With
/// `enforce_published_suspend_fence` it also refuses `suspended`, for a
/// substrate whose restore-shaped wakes claim `restoring` before the node call
/// and leave `restoring -> started` to this write; without it a `suspended`
/// row flips, as a Direct wake that leaves its pre-wake lifecycle in place
/// expects. Nothing else is re-checked: no lease, generation or activation
/// owner. No locking read precedes it; the `UPDATE` itself locks the row, so
/// it can wait on a concurrent holder of that lock.
pub async fn complete_wake(
    executor: impl sqlx::PgExecutor<'_>,
    sandbox_id: SandboxId,
    enforce_published_suspend_fence: bool,
) -> Result<bool, sqlx::Error> {
    let completed = sqlx::query_scalar::<_, bool>(sql!(
        "UPDATE sandboxes
            SET lifecycle = 'started', placement = 'on_node'
          WHERE id = $1
            AND LOWER(TRIM(lifecycle)) NOT IN (
                'failed',
                'quarantined',
                'stopped',
                'stopping',
                'suspending',
                'deleting',
                'deleted',
                'cleanup_required'
            )
            AND (
                LOWER(TRIM(lifecycle)) <> 'suspended'
                OR $2 = FALSE
            )
        RETURNING TRUE"
    ))
    .bind(sandbox_id)
    .bind(enforce_published_suspend_fence)
    .fetch_optional(executor)
    .await?;
    Ok(completed.is_some())
}

/// The completion CAS a cancelled wake's redrive runs: same shape as
/// [`complete_wake`], hardened for running without a live attempt behind it —
/// it claims only a row still in `restoring`, and only when a routable
/// execution (or a non-k8s substrate) backs the wake target at flip time. The
/// shared routability predicate is the same fragment the transient-lifecycle
/// sweeper's converge CAS and the node published-state convergence use; `$2`
/// binds `SubstrateKind::Kubernetes` for it.
/// The restore-lease guard makes the redrive honor the same ownership
/// invariant as the sweeper's converge CAS and `begin_restore_lifecycle`:
/// an unexpired lease means a restore attempt — most likely a client retry
/// that fired after our caller hung up — is actively driving the row and
/// will run its own completion. The cancelled attempt's *own* lease also
/// blocks the CAS until it expires (the lease TTL is the node-RPC timeout),
/// which is why the redrive's budget must exceed that TTL.
fn fenced_completion_cas() -> String {
    let routable = routable_execution_or_non_k8s("sandboxes", "$2");
    format!(
        "UPDATE sandboxes
            SET lifecycle = 'started', placement = 'on_node'
          WHERE id = $1
            AND lifecycle = 'restoring'
            AND (restore_lease_expires_at IS NULL OR restore_lease_expires_at <= NOW())
            AND {routable}"
    )
}

/// Complete a wake whose request was cancelled mid-flight, once the redrive
/// found its execution routable ([`fenced_completion_cas`]). Returns whether
/// the row flipped; `false` means the row moved on, a restore lease is still
/// live, or the execution was replaced after the redrive checked it.
///
/// Writes an **operation step**: it ends the wake the cancelled request left
/// in `restoring`. Every precondition is re-checked in the `WHERE`; the caller
/// holds no lock.
pub async fn complete_redriven_wake(
    executor: impl sqlx::PgExecutor<'_>,
    sandbox_id: SandboxId,
) -> Result<bool, sqlx::Error> {
    let claimed = sqlx::query(sqlx::AssertSqlSafe(sql!(fenced_completion_cas())))
        .bind(sandbox_id)
        .bind(SubstrateKind::Kubernetes)
        .execute(executor)
        .await?;
    Ok(claimed.rows_affected() > 0)
}

/// CAS-clear the `command_ready_pending` marker once an access has confirmed
/// the guest command API is up. Returns whether this call cleared a marker
/// that was still set on a `started` sandbox.
///
/// Writes an **observed projection**: the access's readiness probe. The
/// `lifecycle = 'started'` guard makes every access path lose to the
/// command-ready reaper: if the reaper flipped the row to `failed` (and its
/// trigger cleared the marker) between the probe and this clear, the CAS
/// claims nothing. A `false` return is either that lost race or a marker
/// another access already cleared; callers re-read the lifecycle to tell them
/// apart.
pub async fn clear_command_ready_pending(
    executor: impl sqlx::PgExecutor<'_>,
    sandbox_id: SandboxId,
) -> Result<bool, sqlx::Error> {
    let result = sqlx::query(sql!(
        "UPDATE sandboxes
             SET command_ready_pending = FALSE
             WHERE id = $1 AND lifecycle = 'started' AND command_ready_pending"
    ))
    .bind(sandbox_id)
    .execute(executor)
    .await?;
    Ok(result.rows_affected() > 0)
}

/// Extend a restore's lease to `ttl_secs` from now while it claims the
/// sandbox's active Kubernetes execution. Returns whether the lease was
/// renewed; `false` means this token no longer holds an unexpired lease on
/// the row: another restore took the row over, the lease expired, or the row
/// left `restoring` and the `sandboxes_clear_restore_lease` trigger cleared
/// the token.
///
/// Writes an **operation step**: the restore's ownership lease. Guarded by
/// the lease token and its expiry only; the caller holds the sandbox row lock
/// and has re-checked the lifecycle in the same transaction.
pub async fn renew_restore_lease_for_execution_claim(
    executor: impl sqlx::PgExecutor<'_>,
    sandbox_id: SandboxId,
    restore_lease_token: Uuid,
    ttl_secs: i64,
) -> Result<bool, sqlx::Error> {
    let renewed = sqlx::query_scalar::<_, bool>(sql!(
        "UPDATE sandboxes
                    SET restore_lease_expires_at =
                        NOW() + make_interval(secs => $3)
                  WHERE id = $1
                    AND restore_lease_token = $2
                    AND restore_lease_expires_at > NOW()
                RETURNING TRUE"
    ))
    .bind(sandbox_id)
    .bind(restore_lease_token)
    .bind(ttl_secs)
    .fetch_optional(executor)
    .await?;
    Ok(renewed.is_some())
}

/// Record the node a sandbox's Kubernetes pod was scheduled on as its active
/// and target node, `on_node`, bumping `placement_changed_at`. A row already
/// placed there is left untouched.
///
/// Writes an **observed projection**: the pod's node as Kubernetes reported
/// it. No lifecycle predicate of its own; it relies on the caller's sandbox
/// row lock and on the caller having just recorded the node on a still-active
/// execution in the same transaction.
pub async fn record_pod_node_placement(
    executor: impl sqlx::PgExecutor<'_>,
    sandbox_id: SandboxId,
    node_id: NodeId,
) -> Result<(), sqlx::Error> {
    sqlx::query(sql!(
        "UPDATE sandboxes
                     SET active_node_id = $1,
                         target_node_id = $1,
                         placement = 'on_node',
                         placement_changed_at = NOW()
                     WHERE id = $2
                       AND (
                         active_node_id IS DISTINCT FROM $1
                         OR target_node_id IS DISTINCT FROM $1
                         OR placement IS DISTINCT FROM 'on_node'
                       )"
    ))
    .bind(node_id)
    .bind(sandbox_id)
    .execute(executor)
    .await?;
    Ok(())
}
