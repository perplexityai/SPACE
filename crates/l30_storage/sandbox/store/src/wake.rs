//! Writes of `sandboxes` that finish bringing a sandbox up: completing a wake
//! or restore, including the redrive of one whose request was cancelled;
//! clearing the command-ready marker a preserved slow create leaves, once an
//! access confirms the guest's command API is up; and, on Kubernetes, renewing
//! a restore's lease while it claims the active execution and recording the
//! node a sandbox's pod was scheduled on. That placement write runs wherever a
//! pod is created or resolved, cold creates included, not only on a wake, and
//! is guarded by the execution generation the pod backs.
//!
//! The callers keep everything around them: the sandbox and execution row
//! locks (`SELECT ... FOR UPDATE`), the transactions, the execution writes
//! beside them, and the read that tells a lost completion CAS apart from one
//! a suspend superseded. Every function runs on the executor its caller
//! passes, so it composes into the caller's transaction when there is one.

use pplx_space_accounts::NodeId;
use pplx_space_common_core::types::{
    ExecutionGeneration, SandboxId, SandboxLifecycle, SubstrateKind,
};

use pplx_space_common_core::sql;
use pplx_space_sandbox_routability::routable_execution_or_non_k8s;
use uuid::Uuid;

/// Flip a woken sandbox to `started`/`on_node`. Returns whether the row
/// flipped.
///
/// Writes an **operation step**: it ends the wake. The `WHERE` refuses a row
/// in a lifecycle the wake must not resurrect (`failed`, `quarantined`,
/// `stopped`, `stopping`, `suspending`, `deleting`, `deleted`), compared
/// case- and whitespace-insensitively. With
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
                'deleted'
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

/// What [`record_pod_node_placement`] found.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PodNodePlacementWrite {
    /// The row now names `on_node` as its active and target node, and
    /// `placement_changed_at` was bumped.
    Placed,
    /// The row already named `on_node`, `on_node`, `on_node`: nothing
    /// changed.
    AlreadyPlaced,
    /// The execution is the active one, but the row is in `lifecycle`, which
    /// no pod places; its placement is unchanged.
    LifecycleRefused { lifecycle: String },
    /// No sandbox row, or `generation` is not its active Kubernetes
    /// execution: nothing changed.
    NotActive,
}

/// Lifecycles in which a pod backs the sandbox, so its node is the sandbox's
/// placement: those in which `ensure_pod_exists` materializes one (`created`,
/// `creating`, `starting`, `started`, `restoring`, `pausing`, `paused`,
/// `resuming`, `suspended`, `checkpointing`, and `stopped` for a start), and
/// `suspending` and `stopping`, while the pod a suspend or a stop acts on is
/// still the sandbox's. The Kubernetes substrate's tests pin this list
/// against `ensure_pod_exists`'s own.
pub const POD_PLACEABLE_LIFECYCLES: &[SandboxLifecycle] = &[
    SandboxLifecycle::Created,
    SandboxLifecycle::Creating,
    SandboxLifecycle::Starting,
    SandboxLifecycle::Started,
    SandboxLifecycle::Restoring,
    SandboxLifecycle::Pausing,
    SandboxLifecycle::Paused,
    SandboxLifecycle::Resuming,
    SandboxLifecycle::Suspending,
    SandboxLifecycle::Suspended,
    SandboxLifecycle::Stopping,
    SandboxLifecycle::Stopped,
    SandboxLifecycle::Checkpointing,
];

/// Record the node a sandbox's Kubernetes pod was scheduled on as its active
/// and target node, `on_node`, bumping `placement_changed_at`. A row already
/// placed there is left untouched.
///
/// Writes an **observed projection**: the node of the pod that backs the
/// execution at `generation`, as Kubernetes reported it. The `WHERE` guards it
/// with what it projects:
///
/// * the execution. `generation` must still be the sandbox's active
///   Kubernetes execution. A late write for a pod whose execution a suspend,
///   an evacuation, a lost-node write-off or a replacement has since retired
///   leaves the row alone.
/// * the lifecycles a pod can back the sandbox in,
///   [`POD_PLACEABLE_LIFECYCLES`]. `deleting`, `failed`, `quarantined`, the
///   tombstones and any value outside the stored vocabulary are refused:
///   nothing places a row that is being torn down for good.
///
/// The caller holds the sandbox row lock and has just recorded the node on
/// that execution in the same transaction.
pub async fn record_pod_node_placement(
    executor: impl sqlx::PgExecutor<'_>,
    sandbox_id: SandboxId,
    node_id: NodeId,
    generation: ExecutionGeneration,
) -> Result<PodNodePlacementWrite, sqlx::Error> {
    let placeable: Vec<&str> = POD_PLACEABLE_LIFECYCLES
        .iter()
        .map(|lifecycle| lifecycle.as_str())
        .collect();
    let found = sqlx::query_as::<_, (String, bool, bool)>(sql!(
        "WITH guarded AS (
             SELECT s.id, s.lifecycle, s.lifecycle = ANY ($4) AS placeable
               FROM sandboxes s
              WHERE s.id = $2
                AND EXISTS (
                    SELECT 1
                      FROM sandbox_k8s_executions e
                     WHERE e.sandbox_id = s.id
                       AND e.active
                       AND e.generation = $3
                )
         ),
         placed AS (
             UPDATE sandboxes s
                SET active_node_id = $1,
                    target_node_id = $1,
                    placement = 'on_node',
                    placement_changed_at = NOW()
               FROM guarded
              WHERE s.id = guarded.id
                AND guarded.placeable
                AND (
                    s.active_node_id IS DISTINCT FROM $1
                    OR s.target_node_id IS DISTINCT FROM $1
                    OR s.placement IS DISTINCT FROM 'on_node'
                )
             RETURNING s.id
         )
         SELECT guarded.lifecycle, guarded.placeable, EXISTS (SELECT 1 FROM placed)
           FROM guarded"
    ))
    .bind(node_id)
    .bind(sandbox_id)
    .bind(generation.get())
    .bind(&placeable)
    .fetch_optional(executor)
    .await?;
    Ok(match found {
        None => PodNodePlacementWrite::NotActive,
        Some((lifecycle, false, _)) => PodNodePlacementWrite::LifecycleRefused { lifecycle },
        Some((_, true, true)) => PodNodePlacementWrite::Placed,
        Some((_, true, false)) => PodNodePlacementWrite::AlreadyPlaced,
    })
}
