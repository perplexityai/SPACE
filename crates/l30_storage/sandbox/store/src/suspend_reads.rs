//! Reads the Kubernetes suspend flow makes of `sandboxes` and its executions:
//! whether a suspend is claimed, committed or projected, which artifact it
//! committed, the durable snapshot a node can no longer publish, whether a
//! node-reported local suspend's upload is still `creating`, and whether a
//! suspend whose pod went missing can finish from the row's current snapshot.
//!
//! Plain reads: none takes a lock. Each runs on the executor its caller
//! passes, so the ones the suspend flow makes after its row locks run in
//! that transaction. The suspend's `sandboxes` writes are
//! [`crate::suspend`]'s. Its commit's execution writes and the pod
//! retirement's execution lock are [`crate::k8s_executions::suspend`]'s.

use std::sync::LazyLock;

use pplx_space_k8s_ids::PodUid;
use pplx_space_postgres_pool::{DbError, Executor};
use pplx_space_sandbox_model::{ExecutionGeneration, SandboxId};
use pplx_space_sandbox_portability::missing_suspending_pod_revert_eligibility;
use pplx_space_snapshot_model::{ArtifactKind, ArtifactOrigin, LocalArtifactId};
use pplx_space_sql_locator::sql;
use pplx_space_state_reconciliation::{FACET_SANDBOX_DESIRED_STATE, SANDBOX_LOCAL_SUSPEND_KEY};
use uuid::Uuid;

/// Whether `sandbox_id` is `suspending` (compared trimmed and
/// case-insensitively); `None` when the row is gone.
pub async fn lifecycle_is_suspending(
    executor: impl Executor<'_>,
    sandbox_id: SandboxId,
) -> Result<Option<bool>, DbError> {
    Ok(sqlx::query_scalar(sql!(
        "SELECT LOWER(TRIM(lifecycle)) = 'suspending' FROM sandboxes WHERE id = $1"
    ))
    .bind(sandbox_id)
    .fetch_optional(executor.into_sqlx())
    .await?)
}

/// Whether `sandbox_id` is `suspending` with an active execution at
/// `generation` tracking `pod_uid` that is `running` or `suspended`: a
/// suspend claim already held by that execution.
pub async fn claim_held(
    executor: impl Executor<'_>,
    sandbox_id: SandboxId,
    generation: ExecutionGeneration,
    pod_uid: &PodUid,
) -> Result<bool, DbError> {
    Ok(sqlx::query_scalar(sql!(
        "SELECT EXISTS (
                 SELECT 1
                   FROM sandboxes s
                   JOIN sandbox_k8s_executions e ON e.sandbox_id = s.id
                  WHERE s.id = $1
                    AND s.lifecycle = 'suspending'
                    AND e.generation = $2
                    AND e.active
                    AND e.pod_uid = $3
                    AND e.state IN ('running', 'suspended')
             )"
    ))
    .bind(sandbox_id)
    .bind(generation.get())
    .bind(pod_uid.as_str())
    .fetch_one(executor.into_sqlx())
    .await?)
}

/// The finalized suspend a projection must already have written: the
/// execution it retired and the artifact marker it left on the row.
#[derive(Clone, Copy, Debug)]
pub struct ProjectedSuspend<'a> {
    pub generation: ExecutionGeneration,
    pub pod_uid: &'a PodUid,
    pub artifact_id: LocalArtifactId,
    pub artifact_kind: ArtifactKind,
    pub artifact_origin: ArtifactOrigin,
    /// The execution state a finalized suspend leaves, in the caller's
    /// vocabulary.
    pub execution_state: &'a str,
}

/// Whether `sandbox_id` is `suspended` with exactly `projected`'s artifact
/// marker, and its execution at that generation and pod is retired in
/// `projected.execution_state` with no successor.
pub async fn projection_matches(
    executor: impl Executor<'_>,
    sandbox_id: SandboxId,
    projected: ProjectedSuspend<'_>,
) -> Result<bool, DbError> {
    Ok(sqlx::query_scalar::<_, bool>(sql!(
        "SELECT EXISTS (
                     SELECT 1
                       FROM sandboxes s
                       JOIN sandbox_k8s_executions e
                         ON e.sandbox_id = s.id
                        AND e.generation = $2
                        AND e.pod_uid = $3
                      WHERE s.id = $1
                        AND s.lifecycle = 'suspended'
                        AND s.current_local_artifact_id = $4
                        AND s.current_local_artifact_kind = $5
                        AND s.current_local_artifact_origin = $6
                        AND e.state = $7
                        AND NOT e.active
                        AND NOT EXISTS (
                            SELECT 1
                              FROM sandbox_k8s_executions successor
                             WHERE successor.sandbox_id = s.id
                               AND successor.generation > e.generation
                        )
                 )"
    ))
    .bind(sandbox_id)
    .bind(projected.generation.get())
    .bind(projected.pod_uid.as_str())
    .bind(projected.artifact_id.as_uuid())
    .bind(projected.artifact_kind.as_str())
    .bind(projected.artifact_origin.as_str())
    .bind(projected.execution_state)
    .fetch_one(executor.into_sqlx())
    .await?)
}

/// The execution state and the sandbox's artifact marker behind a committed
/// suspend, undecoded: the caller owns the state vocabulary.
#[derive(Clone, Debug, sqlx::FromRow)]
pub struct CommittedSuspendMarker {
    pub execution_state: String,
    pub artifact_id: Option<Uuid>,
    pub artifact_kind: Option<String>,
    pub artifact_origin: Option<String>,
}

/// The marker of the suspend the execution of `sandbox_id` at `generation`
/// tracking `pod_uid` owns: while the row is `suspending` and the execution
/// active, or once it is `suspended` and that execution is retired
/// `suspended` with no successor. `None` otherwise.
pub async fn committed_marker(
    executor: impl Executor<'_>,
    sandbox_id: SandboxId,
    generation: ExecutionGeneration,
    pod_uid: &PodUid,
) -> Result<Option<CommittedSuspendMarker>, DbError> {
    Ok(sqlx::query_as::<_, CommittedSuspendMarker>(sql!(
        "SELECT e.state AS execution_state,
                    s.current_local_artifact_id AS artifact_id,
                    s.current_local_artifact_kind AS artifact_kind,
                    s.current_local_artifact_origin AS artifact_origin
               FROM sandboxes s
               JOIN sandbox_k8s_executions e
                 ON e.sandbox_id = s.id
                AND e.generation = $2
                AND e.pod_uid = $3
              WHERE s.id = $1
                AND (
                    (s.lifecycle = 'suspending' AND e.active)
                    OR (
                        s.lifecycle = 'suspended'
                        AND e.state = 'suspended'
                        AND NOT e.active
                        AND NOT EXISTS (
                            SELECT 1
                              FROM sandbox_k8s_executions successor
                             WHERE successor.sandbox_id = s.id
                               AND successor.generation > e.generation
                        )
                    )
                )"
    ))
    .bind(sandbox_id)
    .bind(generation.get())
    .bind(pod_uid.as_str())
    .fetch_optional(executor.into_sqlx())
    .await?)
}

/// The artifact of a durable suspend snapshot, its kind undecoded.
#[derive(Clone, Debug, sqlx::FromRow)]
pub struct DurableSuspendArtifact {
    pub local_artifact_id: Uuid,
    pub artifact_kind: String,
}

/// The artifact of the newest full suspend snapshot of a `suspending`
/// `sandbox_id` whose active execution at `generation` tracks `pod_uid`:
/// taken by the active node since the suspend began, ready, with a live
/// storage location, and with no newer full or in-flight snapshot. `None`
/// when there is none.
pub async fn durable_suspend_artifact(
    executor: impl Executor<'_>,
    sandbox_id: SandboxId,
    generation: ExecutionGeneration,
    pod_uid: &PodUid,
) -> Result<Option<DurableSuspendArtifact>, DbError> {
    Ok(sqlx::query_as::<_, DurableSuspendArtifact>(sql!(
        "SELECT sn.local_artifact_id, sn.artifact_kind
               FROM sandboxes s
               JOIN sandbox_k8s_executions e
                 ON e.sandbox_id = s.id
                AND e.generation = $2
                AND e.pod_uid = $3
                AND e.active
               JOIN snapshots sn
                 ON sn.sandbox_id = s.id
              WHERE s.id = $1
                AND s.lifecycle = 'suspending'
                AND sn.type = 'full'
                AND sn.state = 'ready'
                AND sn.artifact_origin = 'suspend'
                AND sn.artifact_kind IS NOT NULL
                AND sn.local_artifact_id IS NOT NULL
                AND sn.source_node_id = s.active_node_id
                AND sn.timestamp >= s.lifecycle_changed_at
                AND EXISTS (
                    SELECT 1
                      FROM snapshot_storage_locations l
                     WHERE l.snapshot_id = sn.id
                       AND l.orphaned_at IS NULL
                )
                AND NOT EXISTS (
                    SELECT 1
                      FROM snapshots newer
                     WHERE newer.sandbox_id = s.id
                       AND newer.id <> sn.id
                       AND newer.timestamp >= sn.timestamp
                       AND (newer.state = 'creating' OR newer.type = 'full')
                )"
    ))
    .bind(sandbox_id)
    .bind(generation.get())
    .bind(pod_uid.as_str())
    .fetch_optional(executor.into_sqlx())
    .await?)
}

/// Whether `sandbox_id` is `started` with an active, `recovering` Kubernetes
/// execution at `generation` on `pod_uid`, and its active node's exact
/// local-suspend observation of that execution matches a `creating` full
/// suspend upload from that node, taken no earlier than the sandbox's last
/// command or access.
pub async fn local_backup_pending(
    executor: impl Executor<'_>,
    sandbox_id: SandboxId,
    generation: ExecutionGeneration,
    pod_uid: &PodUid,
) -> Result<bool, DbError> {
    Ok(sqlx::query_scalar(sql!(
        "SELECT EXISTS (
                 SELECT 1
                   FROM sandboxes s
                   JOIN sandbox_k8s_executions e ON e.sandbox_id = s.id
                   JOIN node_facet_observed_status observed
                     ON observed.node_id = s.active_node_id
                    AND observed.facet = $4
                    AND observed.element = s.id::TEXT
                   JOIN snapshots sn
                     ON sn.sandbox_id = s.id
                    AND sn.source_node_id = observed.node_id
                    AND sn.local_artifact_id::TEXT =
                        observed.observed_payload -> $5::TEXT ->> 'local_artifact_id'
                  WHERE s.id = $1
                    AND s.lifecycle = 'started'
                    AND e.generation = $2
                    AND e.pod_uid = $3
                    AND e.active
                    AND e.state = 'recovering'
                    AND observed.observed_payload -> $5::TEXT ->> 'execution_generation' = e.generation::TEXT
                    AND observed.observed_payload -> $5::TEXT ->> 'pod_uid' = e.pod_uid
                    AND sn.type = 'full'
                    AND sn.artifact_kind = 'full'
                    AND sn.artifact_origin = 'suspend'
                    AND sn.state = 'creating'
                    AND (s.last_command_at IS NULL OR sn.timestamp >= s.last_command_at)
                    AND (s.last_access_at IS NULL OR sn.timestamp >= s.last_access_at)
             )"
    ))
    .bind(sandbox_id.as_uuid())
    .bind(generation.get())
    .bind(pod_uid.as_str())
    .bind(FACET_SANDBOX_DESIRED_STATE)
    .bind(SANDBOX_LOCAL_SUSPEND_KEY)
    .fetch_one(executor.into_sqlx())
    .await?)
}

/// Whether the `suspending` `sandbox_id`, still placed, whose active execution
/// at `generation` tracking `pod_uid` is `running` or `suspended` with no
/// successor, may finish its suspend from its current snapshot
/// ([`missing_suspending_pod_revert_eligibility`]'s `eligible`). `Some` with
/// that fragment's `clear_marker` when it may, `None` when it may not.
///
/// The Kubernetes store reads it after its per-sandbox row lock, in that
/// transaction, and feeds the answer to
/// [`crate::suspend::finalize_missing_pod_from_current_snapshot`].
pub async fn missing_suspending_pod_clear_marker(
    executor: impl Executor<'_>,
    sandbox_id: SandboxId,
    generation: i64,
    pod_uid: &str,
) -> Result<Option<bool>, DbError> {
    static QUERY: LazyLock<String> = LazyLock::new(|| {
        let eligibility = missing_suspending_pod_revert_eligibility("s");
        let clear_marker = eligibility.clear_marker;
        let eligible = eligibility.eligible;
        sql!(format!(
            "SELECT {clear_marker}
               FROM sandboxes s
              WHERE s.id = $1
                AND LOWER(TRIM(s.lifecycle)) = 'suspending'
                AND s.active_node_id IS NOT NULL
                AND EXISTS (
                    SELECT 1
                      FROM sandbox_k8s_executions e
                     WHERE e.sandbox_id = s.id
                       AND e.generation = $2
                       AND e.pod_uid = $3
                       AND e.active
                       AND e.state IN ('running', 'suspended')
                )
                AND NOT EXISTS (
                    SELECT 1
                      FROM sandbox_k8s_executions successor
                     WHERE successor.sandbox_id = s.id
                       AND successor.generation > $2
                )
                AND {eligible}",
        ))
    });
    Ok(
        sqlx::query_scalar::<_, bool>(sqlx::AssertSqlSafe(QUERY.as_str()))
            .bind(sandbox_id)
            .bind(generation)
            .bind(pod_uid)
            .fetch_optional(executor.into_sqlx())
            .await?,
    )
}
