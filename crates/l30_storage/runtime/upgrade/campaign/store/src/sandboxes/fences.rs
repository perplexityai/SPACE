//! External public/admin operation fences and Kubernetes stopped-execution
//! ownership checks for sandbox runtime upgrade campaigns.

use pplx_space_k8s_ids::PodUid;
use pplx_space_node_model::{NodeDrainMode, NodeDrainPhase, SubstrateKind};
use pplx_space_postgres_pool::{DbError, Executor};
use pplx_space_public_api::{OperationKind, OperationState, OperationTargetType, PublicSandboxId};
use pplx_space_sandbox_model::{SandboxId, SandboxLifecycle};
use pplx_space_snapshot_model::{ArtifactKind, ArtifactOrigin, SnapshotKind, SnapshotState};
use pplx_space_sql_locator::sql;

const STOPPED_EXECUTION_SUBSTRATE: SubstrateKind = SubstrateKind::Kubernetes;

/// Status of external public or admin sandbox operations matching a fence query.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExternalOperation {
    /// No matching `queued`, `running`, or post-`since` `succeeded` operation
    /// exists for the sandbox.
    None,
    /// At least one matching operation is `queued` or `running`, and none has
    /// `succeeded` at or after `since` yet.
    InFlight,
    /// At least one matching operation `succeeded` at or after `since`.
    Succeeded,
}

/// Status of external public or admin stop, kill, delete, or rollback
/// operations targeting `sandbox_id` across `(initiated_at, dispatched_at)`.
///
/// - Any matching `queued` or `running` operation yields [`ExternalOperation::InFlight`]
///   when no superseding `succeeded` operation exists yet, so the caller holds
///   without clearing stop ownership in case the user operation fails.
/// - Any matching `succeeded` operation completed at or after `dispatched_at`
///   yields [`ExternalOperation::Succeeded`]: once the campaign dispatched its
///   own stop, any stop, kill, delete, or rollback (`auto_restart: false` or
///   `true`, `already_completed: true` or absent) supersedes campaign stop
///   ownership.
/// - Before `dispatched_at` (`[initiated_at, dispatched_at)`), only a stop,
///   kill, or delete without `controller_response.already_completed = true` or
///   a rollback without `rollback.auto_restart = true` that has no newer
///   `succeeded` activation (`start`, `wake`, `restore`, or `rollback` with
///   `rollback.auto_restart = true`) yields [`ExternalOperation::Succeeded`].
///   That catches stale-`started` stops whose node hadn't published `stopped`
///   yet without misclassifying an earlier auto-restarting rollback, a no-op
///   `already_completed` stop, or an earlier stop that a subsequent wake
///   cleared without advancing `sandboxes.lifecycle_changed_at`. Takes no lock.
pub async fn external_stop_since(
    executor: impl Executor<'_>,
    sandbox_id: uuid::Uuid,
    initiated_at: time::OffsetDateTime,
    dispatched_at: time::OffsetDateTime,
) -> Result<ExternalOperation, DbError> {
    let target_id = PublicSandboxId::from(SandboxId::from_uuid(sandbox_id)).to_string();
    let stop_kinds = [
        OperationKind::StopSandbox.as_str(),
        OperationKind::KillSandbox.as_str(),
        OperationKind::DeleteSandbox.as_str(),
        OperationKind::RollbackSandbox.as_str(),
    ];
    let direct_stop_kinds = [
        OperationKind::StopSandbox.as_str(),
        OperationKind::KillSandbox.as_str(),
        OperationKind::DeleteSandbox.as_str(),
    ];
    let direct_activation_kinds = [
        OperationKind::StartSandbox.as_str(),
        OperationKind::WakeSandbox.as_str(),
        OperationKind::RestoreSandbox.as_str(),
    ];
    let active_states = [
        OperationState::Queued.as_str(),
        OperationState::Running.as_str(),
    ];
    // Explicit `IS NOT NULL` predicates keep PostgreSQL's generic prepared-plan
    // matcher eligible for partial index `idx_public_operations_target`.
    let code = sqlx::query_scalar::<_, i16>(sql!(
        "SELECT CASE
                    WHEN COALESCE(
                        BOOL_OR(
                            p.state = $6
                            AND (
                                COALESCE(p.completed_at, p.requested_at) >= $8
                                OR (
                                    COALESCE(p.completed_at, p.requested_at) >= $7
                                    AND (
                                        (
                                            p.kind = ANY($9)
                                            AND NOT COALESCE(
                                                p.result_json #> '{controller_response,already_completed}' = 'true'::jsonb,
                                                FALSE
                                            )
                                        )
                                        OR (
                                            p.kind = $10
                                            AND NOT COALESCE(
                                                p.payload_json #> '{rollback,auto_restart}' = 'true'::jsonb,
                                                FALSE
                                            )
                                        )
                                    )
                                    AND NOT EXISTS (
                                        SELECT 1
                                        FROM public_operations act
                                        WHERE act.org_id = s.org_id
                                          AND act.target_type = $2
                                          AND act.target_id = $3
                                          AND act.target_type IS NOT NULL
                                          AND act.target_id IS NOT NULL
                                          AND act.state = $6
                                          AND (
                                              act.kind = ANY($11)
                                              OR (
                                                  act.kind = $10
                                                  AND COALESCE(
                                                      act.payload_json #> '{rollback,auto_restart}' = 'true'::jsonb,
                                                      FALSE
                                                  )
                                              )
                                          )
                                          AND (
                                              COALESCE(act.completed_at, act.requested_at),
                                              act.operation_id
                                          ) > (
                                              COALESCE(p.completed_at, p.requested_at),
                                              p.operation_id
                                          )
                                    )
                                )
                            )
                        ),
                        FALSE
                    ) THEN 2::int2
                    WHEN COALESCE(BOOL_OR(p.state = ANY($5)), FALSE) THEN 1::int2
                    ELSE 0::int2
                END
         FROM sandboxes s
         JOIN public_operations p
           ON p.org_id = s.org_id
          AND p.target_type = $2
          AND p.target_id = $3
          AND p.target_type IS NOT NULL
          AND p.target_id IS NOT NULL
         WHERE s.id = $1
           AND p.kind = ANY($4)
           AND (
               p.state = ANY($5)
               OR (p.state = $6 AND COALESCE(p.completed_at, p.requested_at) >= LEAST($7, $8))
           )"
    ))
    .bind(sandbox_id)
    .bind(OperationTargetType::Sandbox.as_str())
    .bind(target_id)
    .bind(&stop_kinds[..])
    .bind(&active_states[..])
    .bind(OperationState::Succeeded.as_str())
    .bind(initiated_at)
    .bind(dispatched_at)
    .bind(&direct_stop_kinds[..])
    .bind(OperationKind::RollbackSandbox.as_str())
    .bind(&direct_activation_kinds[..])
    .fetch_one(executor.into_sqlx())
    .await?;
    Ok(match code {
        2 => ExternalOperation::Succeeded,
        1 => ExternalOperation::InFlight,
        _ => ExternalOperation::None,
    })
}

/// Status of external public or admin start, wake, restore, or rollback
/// operations targeting `sandbox_id` since `since`.
///
/// Distinguishes a campaign-owned stopped sandbox (including one verified via
/// [`stopped_execution_ownership`]) from one with a `queued`/`running`
/// (`InFlight`) or `succeeded` (`Succeeded`) activation since
/// `StopFence::stopped_at`. Takes no lock.
pub async fn external_activation_since(
    executor: impl Executor<'_>,
    sandbox_id: uuid::Uuid,
    since: time::OffsetDateTime,
) -> Result<ExternalOperation, DbError> {
    let kinds = [
        OperationKind::StartSandbox.as_str(),
        OperationKind::WakeSandbox.as_str(),
        OperationKind::RestoreSandbox.as_str(),
        OperationKind::RollbackSandbox.as_str(),
    ];
    external_operations_since(executor, sandbox_id, &kinds, since).await
}

/// Ownership of a campaign-stopped Kubernetes sandbox across pod replacement,
/// `node_drain` stopped-disk evacuation, stopped re-placement, and implicit
/// wakes (`sessions`/`exec`/`pty`/port-exposure/files).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StoppedExecutionOwnership {
    /// `sandbox_id` is still `'stopped'` from `(expected_generation,
    /// expected_pod_uid)` (or its `node_drain` stopped-disk evacuation) with no
    /// live activation or active/unplaced drain evacuation in flight.
    Owned,
    /// Stop ownership is still intact, while either `node_drain` holds an
    /// active stopped-evacuation claim on a force-draining node or has torn
    /// down the local copy and hasn't finished placing its replacement pod
    /// (`evacuation_restore_target = 'stopped'`), or the active execution
    /// currently holds an unexpired `activation_attempt_id` (an in-flight drain
    /// re-placement, implicit wake, or not-yet-released retry). The caller
    /// yields the lease without clearing stop ownership or burning retry budget.
    InFlight,
    /// The sandbox left `'stopped'`, moved to a newer stop not backed by
    /// `expected_generation`'s stop disk, or mismatched `(expected_generation,
    /// expected_pod_uid)`.
    Superseded,
}

/// Ownership of `sandbox_id`'s current `'stopped'` state relative to the
/// Kubernetes execution `(expected_generation, expected_pod_uid)` whose stop
/// settled at `stopped_at`, with `settled_stopped_at` naming the latest
/// controller-settled `'stopped'` `sandboxes.lifecycle_changed_at` for this
/// stop episode (`settled_stopped_at == stopped_at` initially, or a later
/// timestamp when rolling back an undispatched `'starting' -> 'stopped'`
/// claim).
///
/// Covers both stops still at `settled_stopped_at`
/// (`sandboxes.lifecycle_changed_at == settled_stopped_at`, including
/// pre-teardown `node_drain` claims and disk pins before or during remote
/// backup upload, `:undrain`-canceled pins whose local copy remains active on
/// `expected_generation`, preserved `node_drain` disk pins across an
/// undispatched `'starting' -> 'stopped'` rollback, same-generation pod
/// re-materializations that clear `pod_uid` or hold a live
/// `activation_attempt_id` at or after `stopped_at` without advancing
/// `sandboxes.lifecycle_changed_at`, and start/wake attempts that gave up or
/// crashed before materializing a pod or dispatching `Wake`)
/// and `node_drain` stopped evacuations/re-placements after `settled_stopped_at`
/// (`sandboxes.lifecycle_changed_at > settled_stopped_at` with
/// `expected_generation` retired as `'stopped'`/`'forgotten'` at or after
/// `stopped_at`, matching `expected_pod_uid`, and a pinned ready
/// `'disk'`/`'stop'` snapshot captured at or before
/// `expected_generation.terminated_at`). Potentially dispatched,
/// unfinished wakes retain a non-`NULL` `activation_attempt_id` past lease
/// expiry (`kubernetes/store/activation_fence.rs`), so an expired
/// `activation_attempt_id` on an active execution that already materialized a
/// pod or initialized `route_activation_id` resolves to
/// [`StoppedExecutionOwnership::Superseded`] rather than `Owned`, while a
/// `NULL` `activation_attempt_expires_at` remains live (`InFlight`).
/// Takes no lock.
pub async fn stopped_execution_ownership(
    executor: impl Executor<'_>,
    sandbox_id: uuid::Uuid,
    stopped_at: time::OffsetDateTime,
    settled_stopped_at: time::OffsetDateTime,
    expected_generation: i64,
    expected_pod_uid: &PodUid,
) -> Result<StoppedExecutionOwnership, DbError> {
    let same_stop_retired_states = [SandboxLifecycle::Stopped.as_str(), "replaced", "forgotten"];
    let drain_retired_states = [SandboxLifecycle::Stopped.as_str(), "forgotten"];
    let code = sqlx::query_scalar::<_, i16>(sql!(
        "SELECT CASE
                    WHEN (
                        s.evacuation_restore_target = $3
                        AND (
                            s.active_node_id IS NULL
                            OR EXISTS (
                                SELECT 1
                                FROM nodes n_fd
                                WHERE n_fd.id = s.active_node_id
                                  AND n_fd.drain_phase = $14
                                  AND n_fd.drain_mode = $15
                            )
                        )
                    ) OR (
                        cur.id IS NOT NULL
                        AND cur.activation_attempt_id IS NOT NULL
                        AND COALESCE(cur.activation_attempt_expires_at > NOW(), TRUE)
                    )
                    THEN 1::int2
                    ELSE 2::int2
                END
         FROM sandboxes s
         JOIN sandbox_k8s_executions halted
           ON halted.sandbox_id = s.id
          AND halted.generation = $6
         LEFT JOIN sandbox_k8s_executions cur
           ON cur.sandbox_id = s.id
          AND cur.active
         WHERE s.id = $1
           AND s.managed_by = $2
           AND s.lifecycle = $3
           AND $13 >= $8
           AND s.lifecycle_changed_at >= $13
           AND (cur.id IS NULL OR cur.generation >= halted.generation)
           AND NOT (
               cur.id IS NOT NULL
               AND cur.activation_attempt_id IS NOT NULL
               AND cur.activation_attempt_expires_at IS NOT NULL
               AND cur.activation_attempt_expires_at <= NOW()
               AND (
                   cur.pod_materialized_at IS NOT NULL
                   OR NULLIF(BTRIM(cur.pod_uid), '') IS NOT NULL
                   OR cur.pod_ip IS NOT NULL
                   OR cur.route_activation_id IS NOT NULL
               )
           )
           AND (
               (
                   s.lifecycle_changed_at = $13
                   AND (
                       (
                           halted.active
                           AND halted.terminated_at IS NULL
                           AND (
                               NULLIF(BTRIM(halted.pod_uid), '') = $7
                               OR (
                                   NULLIF(BTRIM(halted.pod_uid), '') IS NULL
                                   AND halted.pod_materialized_at IS NOT NULL
                                   AND halted.updated_at >= $8
                               )
                               OR (
                                   halted.activation_attempt_id IS NOT NULL
                                   AND COALESCE(halted.activation_attempt_expires_at > NOW(), TRUE)
                                   AND halted.pod_materialized_at IS NOT NULL
                                   AND halted.updated_at >= $8
                               )
                           )
                           AND (
                               s.current_local_artifact_id IS NULL
                               OR (
                                   s.current_local_artifact_kind = $4
                                   AND s.current_local_artifact_origin = $5
                               )
                           )
                       )
                       OR (
                           NOT halted.active
                           AND halted.terminated_at IS NOT NULL
                           AND halted.terminated_at >= $8
                           AND (
                               (
                                   s.current_local_artifact_id IS NULL
                                   AND (
                                       cur.id IS NULL
                                       OR cur.activation_attempt_id IS NOT NULL
                                   )
                                   AND halted.state = ANY($11)
                                   AND (
                                       NULLIF(BTRIM(halted.pod_uid), '') = $7
                                       OR (
                                           NULLIF(BTRIM(halted.pod_uid), '') IS NULL
                                           AND halted.pod_materialized_at IS NOT NULL
                                           AND halted.updated_at >= $8
                                       )
                                   )
                               )
                               OR (
                                   s.current_local_artifact_id IS NOT NULL
                                   AND NULLIF(BTRIM(halted.pod_uid), '') = $7
                                   AND halted.state = ANY($12)
                                   AND s.current_local_artifact_kind = $4
                                   AND s.current_local_artifact_origin = $5
                                   AND EXISTS (
                                       SELECT 1
                                       FROM snapshots sn
                                       WHERE sn.sandbox_id = s.id
                                         AND sn.local_artifact_id = s.current_local_artifact_id
                                         AND sn.type = $9
                                         AND sn.state = $10
                                         AND sn.artifact_kind = $4
                                         AND sn.artifact_origin = $5
                                         AND sn.timestamp <= halted.terminated_at
                                   )
                               )
                           )
                       )
                   )
               )
               OR (
                   s.lifecycle_changed_at > $13
                   AND NULLIF(BTRIM(halted.pod_uid), '') = $7
                   AND s.current_local_artifact_id IS NOT NULL
                   AND s.current_local_artifact_kind = $4
                   AND s.current_local_artifact_origin = $5
                   AND NOT halted.active
                   AND halted.state = ANY($12)
                   AND halted.terminated_at IS NOT NULL
                   AND halted.terminated_at >= $8
                   AND EXISTS (
                       SELECT 1
                       FROM snapshots sn
                       WHERE sn.sandbox_id = s.id
                         AND sn.local_artifact_id = s.current_local_artifact_id
                         AND sn.type = $9
                         AND sn.state = $10
                         AND sn.artifact_kind = $4
                         AND sn.artifact_origin = $5
                         AND sn.timestamp <= halted.terminated_at
                   )
               )
           )"
    ))
    .bind(sandbox_id)
    .bind(STOPPED_EXECUTION_SUBSTRATE)
    .bind(SandboxLifecycle::Stopped.as_str())
    .bind(ArtifactKind::Disk.as_str())
    .bind(ArtifactOrigin::Stop.as_str())
    .bind(expected_generation)
    .bind(expected_pod_uid.as_str())
    .bind(stopped_at)
    .bind(SnapshotKind::Disk.as_str())
    .bind(SnapshotState::Ready.as_str())
    .bind(&same_stop_retired_states[..])
    .bind(&drain_retired_states[..])
    .bind(settled_stopped_at)
    .bind(NodeDrainPhase::Draining.as_str())
    .bind(NodeDrainMode::Forced.as_str())
    .fetch_optional(executor.into_sqlx())
    .await?;
    Ok(match code {
        Some(2) => StoppedExecutionOwnership::Owned,
        Some(1) => StoppedExecutionOwnership::InFlight,
        _ => StoppedExecutionOwnership::Superseded,
    })
}

async fn external_operations_since(
    executor: impl Executor<'_>,
    sandbox_id: uuid::Uuid,
    kinds: &[&str],
    since: time::OffsetDateTime,
) -> Result<ExternalOperation, DbError> {
    let target_id = PublicSandboxId::from(SandboxId::from_uuid(sandbox_id)).to_string();
    let active_states = [
        OperationState::Queued.as_str(),
        OperationState::Running.as_str(),
    ];
    // Explicit `IS NOT NULL` predicates keep PostgreSQL's generic prepared-plan
    // matcher eligible for partial index `idx_public_operations_target`.
    let code = sqlx::query_scalar::<_, i16>(sql!(
        "SELECT CASE
                    WHEN COALESCE(
                        BOOL_OR(p.state = $6 AND COALESCE(p.completed_at, p.requested_at) >= $7),
                        FALSE
                    ) THEN 2::int2
                    WHEN COALESCE(BOOL_OR(p.state = ANY($5)), FALSE) THEN 1::int2
                    ELSE 0::int2
                END
         FROM sandboxes s
         JOIN public_operations p
           ON p.org_id = s.org_id
          AND p.target_type = $2
          AND p.target_id = $3
          AND p.target_type IS NOT NULL
          AND p.target_id IS NOT NULL
         WHERE s.id = $1
           AND p.kind = ANY($4)
           AND (p.state = ANY($5)
                OR (p.state = $6 AND COALESCE(p.completed_at, p.requested_at) >= $7))"
    ))
    .bind(sandbox_id)
    .bind(OperationTargetType::Sandbox.as_str())
    .bind(target_id)
    .bind(kinds)
    .bind(&active_states[..])
    .bind(OperationState::Succeeded.as_str())
    .bind(since)
    .fetch_one(executor.into_sqlx())
    .await?;
    Ok(match code {
        2 => ExternalOperation::Succeeded,
        1 => ExternalOperation::InFlight,
        _ => ExternalOperation::None,
    })
}
