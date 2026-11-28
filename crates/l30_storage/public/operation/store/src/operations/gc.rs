//! Retention and payload-scrub operations on finished `public_operations` rows.

use pplx_space_postgres_pool::{DbError, Executor};
use pplx_space_public_api::{OperationKind, OperationState, OperationTargetType};
use pplx_space_sandbox_model::SandboxLifecycle;
use pplx_space_sql_locator::sql;
use uuid::Uuid;

const CAMPAIGN_RETENTION_LOCK_KEY: &str = "runtime_upgrade_campaigns/sandbox_force_restarts";

/// Take a transaction-scoped shared advisory lock against concurrent
/// `sandbox_force_restarts` campaign creation so [`due_for_gc`], dependent
/// `public_idempotency_records` cleanup, and [`delete_by_ids`] observe a stable
/// set of retaining campaigns until the caller's open transaction ends.
pub async fn share_lock_campaign_retention<C>(tx: &mut C) -> Result<(), DbError>
where
    for<'a> &'a mut C: Executor<'a>,
{
    sqlx::query(sql!(
        "SELECT pg_advisory_xact_lock_shared(hashtextextended($1, 0))"
    ))
    .bind(CAMPAIGN_RETENTION_LOCK_KEY)
    .execute((&mut *tx).into_sqlx())
    .await?;
    Ok(())
}

/// Ids deleted by [`delete_by_ids_with_idempotency`].
#[derive(Debug, Default, Eq, PartialEq)]
pub struct DeletedOperations {
    pub operations: Vec<Uuid>,
    pub idempotency_records: Vec<Uuid>,
}

/// Delete the operations `operation_ids` that aren't currently retained by a
/// `sandbox_force_restarts` campaign, along with their dependent
/// `public_idempotency_records` rows, returning the operation ids deleted.
pub async fn delete_by_ids(
    executor: impl Executor<'_>,
    operation_ids: &[Uuid],
) -> Result<Vec<Uuid>, DbError> {
    Ok(delete_by_ids_with_idempotency(executor, operation_ids)
        .await?
        .operations)
}

/// Delete the operations `operation_ids` that aren't currently retained by a
/// `sandbox_force_restarts` campaign and their dependent
/// `public_idempotency_records` rows in one snapshot, returning both deleted id
/// sets.
///
/// Deleting dependent idempotency rows from the same locked `doomed_operations`
/// set prevents both clearing idempotency records for operations that a raced
/// campaign just retained and violating `public_idempotency_records_payload_check`
/// via `ON DELETE SET NULL` when an in-progress idempotency row still has
/// `response_json IS NULL`.
pub async fn delete_by_ids_with_idempotency(
    executor: impl Executor<'_>,
    operation_ids: &[Uuid],
) -> Result<DeletedOperations, DbError> {
    if operation_ids.is_empty() {
        return Ok(DeletedOperations::default());
    }
    let stop_kinds = [
        OperationKind::StopSandbox.as_str(),
        OperationKind::KillSandbox.as_str(),
        OperationKind::DeleteSandbox.as_str(),
        OperationKind::RollbackSandbox.as_str(),
    ];
    let fenced_kinds = [
        OperationKind::StopSandbox.as_str(),
        OperationKind::KillSandbox.as_str(),
        OperationKind::DeleteSandbox.as_str(),
        OperationKind::RollbackSandbox.as_str(),
        OperationKind::StartSandbox.as_str(),
        OperationKind::WakeSandbox.as_str(),
        OperationKind::RestoreSandbox.as_str(),
    ];
    let scanned_lifecycles = [
        SandboxLifecycle::Started.as_str(),
        SandboxLifecycle::Paused.as_str(),
        SandboxLifecycle::Creating.as_str(),
        SandboxLifecycle::Starting.as_str(),
        SandboxLifecycle::Stopping.as_str(),
        SandboxLifecycle::Pausing.as_str(),
        SandboxLifecycle::Resuming.as_str(),
        SandboxLifecycle::Suspending.as_str(),
        SandboxLifecycle::Restoring.as_str(),
        SandboxLifecycle::Checkpointing.as_str(),
    ];
    let resumable_campaigns = ["canary", "rolling", "paused"];
    let resumable_items = ["pending", "failed"];
    let max_reopenable_attempts = 5_i64;
    let rows = sqlx::query_as::<_, (bool, Uuid)>(sql!(
        "WITH retaining_campaigns AS MATERIALIZED (
             SELECT c.id, c.phase, c.created_at, c.from_environment_ref
             FROM runtime_upgrade_campaigns c
             WHERE c.kind = $5
               AND (
                   c.phase = ANY($7)
                   OR (
                       c.phase = $8
                       AND EXISTS (
                           SELECT 1
                           FROM runtime_upgrade_items ai
                           WHERE ai.campaign_id = c.id
                             AND ai.phase = $6
                             AND ai.lease_expires_at > NOW()
                       )
                   )
               )
         ),
         doomed_operations AS (
             SELECT p.operation_id
             FROM public_operations AS p
             WHERE p.operation_id = ANY($1::UUID[])
               AND NOT EXISTS (
                   SELECT 1
                   FROM retaining_campaigns c
                   JOIN sandboxes s
                     ON s.id = CASE
                            WHEN p.target_id ~ '^sbx_[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$'
                            THEN substring(p.target_id FROM 5)::uuid
                        END
                    AND s.org_id = p.org_id
                   LEFT JOIN sandbox_observed_states o
                     ON o.sandbox_id = s.id
                   LEFT JOIN runtime_upgrade_items i
                     ON i.campaign_id = c.id
                    AND i.subject_kind = $4
                    AND i.subject_id = s.id
                   WHERE p.state = $2
                     AND p.target_type = $3
                     AND (
                         (
                             (
                                 (c.phase = $8 AND i.phase = $6 AND i.lease_expires_at > NOW())
                                 OR (
                                     c.phase = ANY($7)
                                     AND (i.phase = $6 OR i.phase = ANY($9))
                                 )
                             )
                             AND p.kind = ANY($13)
                         )
                         OR (
                             c.phase = ANY($7)
                             AND s.lifecycle = ANY($14)
                             AND (
                                 jsonb_typeof(o.execution_metadata -> 'environment') IS DISTINCT FROM 'string'
                                 OR o.execution_metadata ->> 'environment' = c.from_environment_ref
                             )
                             AND (
                                 i.subject_id IS NULL
                                 OR (i.phase = $10 AND i.attempts < $11)
                             )
                             AND (
                                 (
                                     p.kind = ANY($13)
                                     AND COALESCE(p.completed_at, p.requested_at)
                                         >= LEAST(s.lifecycle_changed_at, c.created_at)
                                 )
                                 OR (
                                     p.kind = ANY($13)
                                     AND NOT EXISTS (
                                         SELECT 1
                                         FROM public_operations p2
                                         WHERE p2.org_id = p.org_id
                                           AND p2.target_type = p.target_type
                                           AND p2.target_id = p.target_id
                                           AND p2.target_type IS NOT NULL
                                           AND p2.target_id IS NOT NULL
                                           AND p2.state = $2
                                           AND p2.kind = ANY($13)
                                           AND (
                                               COALESCE(p2.completed_at, p2.requested_at),
                                               p2.operation_id
                                           ) > (
                                               COALESCE(p.completed_at, p.requested_at),
                                               p.operation_id
                                           )
                                     )
                                     AND (
                                         p.kind = ANY($12)
                                         OR EXISTS (
                                             SELECT 1
                                             FROM public_operations p_old
                                             WHERE p_old.org_id = p.org_id
                                               AND p_old.target_type = p.target_type
                                               AND p_old.target_id = p.target_id
                                               AND p_old.target_type IS NOT NULL
                                               AND p_old.target_id IS NOT NULL
                                               AND p_old.state = $2
                                               AND p_old.kind = ANY($12)
                                               AND (
                                                   COALESCE(p_old.completed_at, p_old.requested_at),
                                                   p_old.operation_id
                                               ) < (
                                                   COALESCE(p.completed_at, p.requested_at),
                                                   p.operation_id
                                               )
                                         )
                                     )
                                 )
                             )
                         )
                     )
               )
             FOR UPDATE OF p
         ),
         deleted_idempotency AS (
             DELETE FROM public_idempotency_records AS r
             USING doomed_operations d
             WHERE r.operation_id = d.operation_id
             RETURNING r.id
         ),
         deleted_operations AS (
             DELETE FROM public_operations AS p
             USING doomed_operations d
             WHERE p.operation_id = d.operation_id
             RETURNING p.operation_id
         )
         SELECT FALSE AS is_idempotency, d.operation_id AS id
         FROM deleted_operations d
         UNION ALL
         SELECT TRUE AS is_idempotency, r.id AS id
         FROM deleted_idempotency r"
    ))
    .bind(operation_ids)
    .bind(OperationState::Succeeded.as_str())
    .bind(OperationTargetType::Sandbox.as_str())
    .bind("sandbox")
    .bind("sandbox_force_restarts")
    .bind("in_flight")
    .bind(&resumable_campaigns[..])
    .bind("aborted")
    .bind(&resumable_items[..])
    .bind("skipped")
    .bind(max_reopenable_attempts)
    .bind(&stop_kinds[..])
    .bind(&fenced_kinds[..])
    .bind(&scanned_lifecycles[..])
    .fetch_all(executor.into_sqlx())
    .await?;

    let mut deleted = DeletedOperations::default();
    for (is_idempotency, id) in rows {
        if is_idempotency {
            deleted.idempotency_records.push(id);
        } else {
            deleted.operations.push(id);
        }
    }
    Ok(deleted)
}

/// Remove the payload's `json_path` from up to `limit` finished `kind`
/// operations that still carry it, least recently updated first, skipping
/// any another transaction holds. Returns how many were scrubbed.
pub async fn scrub_finished_payload_path(
    executor: impl Executor<'_>,
    kind: OperationKind,
    json_path: &[&str],
    limit: i64,
) -> Result<u64, DbError> {
    let scrubbed = sqlx::query(sql!(
        "WITH terminal_operations AS (
             SELECT operation_id
             FROM public_operations
             WHERE kind = $2
               AND state IN ($3, $4, $5, $6)
               AND payload_json #> $7::text[] IS NOT NULL
             ORDER BY updated_at ASC, operation_id ASC
             LIMIT $1
             FOR UPDATE SKIP LOCKED
         )
         UPDATE public_operations AS operation
         SET payload_json = operation.payload_json #- $7::text[],
             updated_at = NOW()
         FROM terminal_operations
         WHERE operation.operation_id = terminal_operations.operation_id"
    ))
    .bind(limit)
    .bind(kind.as_str())
    .bind(OperationState::Succeeded.as_str())
    .bind(OperationState::Failed.as_str())
    .bind(OperationState::Canceled.as_str())
    .bind(OperationState::Expired.as_str())
    .bind(json_path)
    .execute(executor.into_sqlx())
    .await?
    .rows_affected();
    Ok(scrubbed)
}

/// Up to `limit` finished operations past their `gc_after` by the database
/// clock, earliest first.
///
/// Retains `succeeded` supersession receipts while an active, resumable, or
/// draining-aborted (`in_flight` with an unexpired lease) `sandbox_force_restarts`
/// campaign could still consult them (`in_flight`, `pending`, or `failed` items
/// on resumable campaigns across any sandbox lifecycle, unexpired `in_flight`
/// leases on `aborted` campaigns, plus active or mid-transition sandboxes a
/// resumable campaign hasn't scanned yet or may reopen from `skipped` with
/// attempts left):
/// - `sandbox.stop`, `sandbox.kill`, `sandbox.delete`, `sandbox.rollback`,
///   `sandbox.start`, `sandbox.wake`, and `sandbox.restore` unconditionally
///   while the sandbox's campaign item is `in_flight`, `pending`, or `failed`
///   (or holds an unexpired `in_flight` lease on an `aborted` campaign), so
///   stop and clearing-activation receipts at or after `initiated_at` survive
///   even after the campaign's own stop advances `sandboxes.lifecycle_changed_at`
///   past `created_at`; and
/// - on unscanned or reopenable-`skipped` active sandboxes, all seven fenced
///   kinds when completed at or after
///   `LEAST(sandboxes.lifecycle_changed_at, runtime_upgrade_campaigns.created_at)`
///   plus the newest `succeeded` fenced operation when it is a
///   stop/kill/delete/rollback receipt or still shields an older uncollected
///   stop/kill/delete/rollback receipt from becoming newest across bounded GC
///   batches.
pub async fn due_for_gc(executor: impl Executor<'_>, limit: i64) -> Result<Vec<Uuid>, DbError> {
    let stop_kinds = [
        OperationKind::StopSandbox.as_str(),
        OperationKind::KillSandbox.as_str(),
        OperationKind::DeleteSandbox.as_str(),
        OperationKind::RollbackSandbox.as_str(),
    ];
    let fenced_kinds = [
        OperationKind::StopSandbox.as_str(),
        OperationKind::KillSandbox.as_str(),
        OperationKind::DeleteSandbox.as_str(),
        OperationKind::RollbackSandbox.as_str(),
        OperationKind::StartSandbox.as_str(),
        OperationKind::WakeSandbox.as_str(),
        OperationKind::RestoreSandbox.as_str(),
    ];
    let scanned_lifecycles = [
        SandboxLifecycle::Started.as_str(),
        SandboxLifecycle::Paused.as_str(),
        SandboxLifecycle::Creating.as_str(),
        SandboxLifecycle::Starting.as_str(),
        SandboxLifecycle::Stopping.as_str(),
        SandboxLifecycle::Pausing.as_str(),
        SandboxLifecycle::Resuming.as_str(),
        SandboxLifecycle::Suspending.as_str(),
        SandboxLifecycle::Restoring.as_str(),
        SandboxLifecycle::Checkpointing.as_str(),
    ];
    // Campaign/item wire literals and the 5-attempt retry cap mirror
    // `pplx_space_runtime_upgrade_campaign_store` (`SubjectKind::Sandbox`,
    // `CampaignKind::SandboxForceRestarts`, `CampaignPhase`, `ItemPhase`) and
    // `cluster-controller`'s `MAX_ATTEMPTS`; `l30_storage` crates stay
    // independent, and end-to-end wire agreement is asserted in
    // `cluster-controller` fence tests.
    let resumable_campaigns = ["canary", "rolling", "paused"];
    let resumable_items = ["pending", "failed"];
    let max_reopenable_attempts = 5_i64;
    Ok(sqlx::query_scalar::<_, Uuid>(sql!(
        "WITH retaining_campaigns AS MATERIALIZED (
             SELECT c.id, c.phase, c.created_at, c.from_environment_ref
             FROM runtime_upgrade_campaigns c
             WHERE c.kind = $8
               AND (
                   c.phase = ANY($10)
                   OR (
                       c.phase = $11
                       AND EXISTS (
                           SELECT 1
                           FROM runtime_upgrade_items ai
                           WHERE ai.campaign_id = c.id
                             AND ai.phase = $9
                             AND ai.lease_expires_at > NOW()
                       )
                   )
               )
         )
         SELECT p.operation_id
         FROM public_operations p
         WHERE p.state IN ($2, $3, $4, $5)
           AND p.gc_after <= NOW()
           AND NOT EXISTS (
               SELECT 1
               FROM retaining_campaigns c
               JOIN sandboxes s
                 ON s.id = CASE
                        WHEN p.target_id ~ '^sbx_[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$'
                        THEN substring(p.target_id FROM 5)::uuid
                    END
                AND s.org_id = p.org_id
               LEFT JOIN sandbox_observed_states o
                 ON o.sandbox_id = s.id
               LEFT JOIN runtime_upgrade_items i
                 ON i.campaign_id = c.id
                AND i.subject_kind = $7
                AND i.subject_id = s.id
               WHERE p.state = $2
                 AND p.target_type = $6
                 AND (
                     (
                         (
                             (c.phase = $11 AND i.phase = $9 AND i.lease_expires_at > NOW())
                             OR (
                                 c.phase = ANY($10)
                                 AND (i.phase = $9 OR i.phase = ANY($12))
                             )
                         )
                         AND p.kind = ANY($16)
                     )
                     OR (
                         c.phase = ANY($10)
                         AND s.lifecycle = ANY($17)
                         AND (
                             jsonb_typeof(o.execution_metadata -> 'environment') IS DISTINCT FROM 'string'
                             OR o.execution_metadata ->> 'environment' = c.from_environment_ref
                         )
                         AND (
                             i.subject_id IS NULL
                             OR (i.phase = $13 AND i.attempts < $14)
                         )
                         AND (
                             (
                                 p.kind = ANY($16)
                                 AND COALESCE(p.completed_at, p.requested_at)
                                     >= LEAST(s.lifecycle_changed_at, c.created_at)
                             )
                             OR (
                                 p.kind = ANY($16)
                                 AND NOT EXISTS (
                                     SELECT 1
                                     FROM public_operations p2
                                     WHERE p2.org_id = p.org_id
                                       AND p2.target_type = p.target_type
                                       AND p2.target_id = p.target_id
                                       AND p2.target_type IS NOT NULL
                                       AND p2.target_id IS NOT NULL
                                       AND p2.state = $2
                                       AND p2.kind = ANY($16)
                                       AND (
                                           COALESCE(p2.completed_at, p2.requested_at),
                                           p2.operation_id
                                       ) > (
                                           COALESCE(p.completed_at, p.requested_at),
                                           p.operation_id
                                       )
                                 )
                                 AND (
                                     p.kind = ANY($15)
                                     OR EXISTS (
                                         SELECT 1
                                         FROM public_operations p_old
                                         WHERE p_old.org_id = p.org_id
                                           AND p_old.target_type = p.target_type
                                           AND p_old.target_id = p.target_id
                                           AND p_old.target_type IS NOT NULL
                                           AND p_old.target_id IS NOT NULL
                                           AND p_old.state = $2
                                           AND p_old.kind = ANY($15)
                                           AND (
                                               COALESCE(p_old.completed_at, p_old.requested_at),
                                               p_old.operation_id
                                           ) < (
                                               COALESCE(p.completed_at, p.requested_at),
                                               p.operation_id
                                           )
                                     )
                                 )
                             )
                         )
                     )
                 )
           )
         ORDER BY p.gc_after ASC, p.operation_id ASC
         LIMIT $1"
    ))
    .bind(limit)
    .bind(OperationState::Succeeded.as_str())
    .bind(OperationState::Failed.as_str())
    .bind(OperationState::Canceled.as_str())
    .bind(OperationState::Expired.as_str())
    .bind(OperationTargetType::Sandbox.as_str())
    .bind("sandbox")
    .bind("sandbox_force_restarts")
    .bind("in_flight")
    .bind(&resumable_campaigns[..])
    .bind("aborted")
    .bind(&resumable_items[..])
    .bind("skipped")
    .bind(max_reopenable_attempts)
    .bind(&stop_kinds[..])
    .bind(&fenced_kinds[..])
    .bind(&scanned_lifecycles[..])
    .fetch_all(executor.into_sqlx())
    .await?)
}
