//! A stay-stopped runtime upgrade's statements on `sandbox_k8s_executions`:
//! the upgrade a stopped sandbox's activation records on the execution it
//! claimed (`runtime_upgrade_operation_id`, its target environment and
//! authorization id), the claim's lease, and the reads a later activation or
//! a drain's stopped evacuation makes of an execution such an upgrade left.
//!
//! The upgrade claims the execution with an activation attempt targeting
//! `stopped` on a `stopped` row, then records its operation in the claim's
//! transaction with [`record_upgrade`], under the sandbox's row lock. Only
//! that upgrade's own claim keeps the record: any other activation's claim
//! clears it with [`clear_recorded_upgrade`], once the disk backup the
//! upgrade may owe is enqueued. An expired claim that still records an
//! upgrade is found with [`read_stopped_claim`] or [`read_leftover_upgrade`],
//! taken over with [`reserve_expired_upgrade`], and ended early with
//! [`expire_attempt_keeping_record`].
//!
//! Each write keeps its guard whole and returns its outcome: matching no row
//! means another writer moved the execution on, and the Kubernetes store
//! that calls these decides what that means. It keeps the transactions, the
//! sandbox row lock taken before the execution's, and every re-check.

use pplx_space_postgres_pool::{DbError, Executor};
use pplx_space_sandbox_model::{SandboxActivationAttemptId, SandboxId};
use pplx_space_sql_locator::sql;
use uuid::Uuid;

/// The active execution claimed by an attempt targeting `stopped`, as
/// [`read_stopped_claim`] reads it.
#[derive(Debug, Clone, PartialEq)]
pub struct StoppedClaim {
    pub generation: i64,
    pub activation_attempt_id: SandboxActivationAttemptId,
    pub pod_name: String,
    pub k8s_node_name: Option<String>,
    /// Whether the claim's lease still runs by the database clock; `true`
    /// for a claim with no expiry.
    pub lease_live: bool,
    pub runtime_upgrade_operation_id: Option<Uuid>,
    /// Raw `runtime_upgrade_target_environment_ref`, as stored.
    pub runtime_upgrade_target_environment_ref: Option<String>,
    pub runtime_upgrade_authorization_id: Option<String>,
}

/// The active execution of `sandbox_id` claimed by an attempt toward
/// `target_state`, whether or not it records an upgrade. `None` when the
/// active execution holds no attempt, or one toward another target.
pub async fn read_stopped_claim(
    executor: impl Executor<'_>,
    sandbox_id: SandboxId,
    target_state: &str,
) -> Result<Option<StoppedClaim>, DbError> {
    type ClaimRow = (
        i64,
        SandboxActivationAttemptId,
        String,
        Option<String>,
        bool,
        Option<Uuid>,
        Option<String>,
        Option<String>,
    );
    let row: Option<ClaimRow> = sqlx::query_as(sql!(
        "SELECT execution.generation, execution.activation_attempt_id, execution.pod_name,
                    execution.k8s_node_name,
                    COALESCE(execution.activation_attempt_expires_at > NOW(), TRUE),
                    execution.runtime_upgrade_operation_id,
                    execution.runtime_upgrade_target_environment_ref,
                    execution.runtime_upgrade_authorization_id
               FROM sandbox_k8s_executions execution
              WHERE execution.sandbox_id = $1
                AND execution.active
                AND execution.activation_attempt_id IS NOT NULL
                AND execution.activation_attempt_target_state = $2"
    ))
    .bind(sandbox_id)
    .bind(target_state)
    .fetch_optional(executor.into_sqlx())
    .await?;
    Ok(row.map(
        |(
            generation,
            activation_attempt_id,
            pod_name,
            k8s_node_name,
            lease_live,
            runtime_upgrade_operation_id,
            runtime_upgrade_target_environment_ref,
            runtime_upgrade_authorization_id,
        )| StoppedClaim {
            generation,
            activation_attempt_id,
            pod_name,
            k8s_node_name,
            lease_live,
            runtime_upgrade_operation_id,
            runtime_upgrade_target_environment_ref,
            runtime_upgrade_authorization_id,
        },
    ))
}

/// The upgrade [`record_upgrade`] records on an execution.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UpgradeRecord<'a> {
    pub operation_id: Uuid,
    /// The target environment's reference, in the `runtime_environments`
    /// format the column's CHECK accepts.
    pub target_environment_ref: &'a str,
    pub authorization_id: &'a str,
}

/// Record `record` on the active execution at `generation` that
/// `activation_attempt_id` holds, unless it records another operation.
/// Writes an **operation step** (the upgrade's record). Returns whether it
/// recorded: `false` when that generation isn't active, another attempt
/// holds it, or it records another operation, whose backup may still be
/// owed.
///
/// The caller holds its sandbox's row lock and has claimed the execution in
/// the same transaction, and passes that transaction.
pub async fn record_upgrade<C>(
    conn: &mut C,
    sandbox_id: SandboxId,
    generation: i64,
    activation_attempt_id: SandboxActivationAttemptId,
    record: UpgradeRecord<'_>,
) -> Result<bool, DbError>
where
    for<'a> &'a mut C: Executor<'a>,
{
    let recorded = sqlx::query(sql!(
        "UPDATE sandbox_k8s_executions
                SET runtime_upgrade_operation_id = $4,
                    runtime_upgrade_target_environment_ref = $5,
                    runtime_upgrade_authorization_id = $6,
                    updated_at = NOW()
              WHERE sandbox_id = $1 AND generation = $2 AND active
                AND activation_attempt_id = $3
                -- Another operation's record may still owe its disk backup:
                -- never overwrite it. Its own retry takes it over instead.
                AND (runtime_upgrade_operation_id IS NULL
                     OR runtime_upgrade_operation_id = $4)"
    ))
    .bind(sandbox_id)
    .bind(generation)
    .bind(activation_attempt_id)
    .bind(record.operation_id)
    .bind(record.target_environment_ref)
    .bind(record.authorization_id)
    .execute(conn.into_sqlx())
    .await?
    .rows_affected();
    Ok(recorded == 1)
}

/// Clear the upgrade the active execution at `generation` records, for an
/// activation that just claimed it and carries no such upgrade. Writes an
/// **operation step** (the record's release). Returns the number of rows
/// changed: `0` when that generation isn't active.
///
/// It checks no claim: the caller runs it after its own claim, in the same
/// transaction under its sandbox's row lock, once it decided the record
/// goes. The caller must pass that transaction.
pub async fn clear_recorded_upgrade<C>(
    conn: &mut C,
    sandbox_id: SandboxId,
    generation: i64,
) -> Result<u64, DbError>
where
    for<'a> &'a mut C: Executor<'a>,
{
    Ok(sqlx::query(sql!(
        "UPDATE sandbox_k8s_executions
                    SET runtime_upgrade_operation_id = NULL,
                        runtime_upgrade_target_environment_ref = NULL,
                        runtime_upgrade_authorization_id = NULL
                  WHERE sandbox_id = $1
                    AND generation = $2
                    AND active"
    ))
    .bind(sandbox_id)
    .bind(generation)
    .execute(conn.into_sqlx())
    .await?
    .rows_affected())
}

/// End now the lease `activation_attempt_id` holds on the active execution
/// at `generation`. An execution that records an upgrade keeps the attempt
/// and its target, so a retry still finds what the claim was for; one that
/// records none gives the attempt up, as
/// [`super::activation_fence::give_up_attempt`] does. Writes an **operation
/// step** (the lease's end). Returns the number of rows changed: `0` when
/// the attempt no longer holds that execution.
pub async fn expire_attempt_keeping_record(
    executor: impl Executor<'_>,
    sandbox_id: SandboxId,
    generation: i64,
    activation_attempt_id: SandboxActivationAttemptId,
) -> Result<u64, DbError> {
    Ok(sqlx::query(sql!(
        "UPDATE sandbox_k8s_executions
                    SET activation_attempt_expires_at = LEAST(activation_attempt_expires_at, NOW()),
                        activation_attempt_id = CASE
                            WHEN runtime_upgrade_operation_id IS NULL THEN NULL
                            ELSE activation_attempt_id
                        END,
                        activation_attempt_target_state = CASE
                            WHEN runtime_upgrade_operation_id IS NULL THEN NULL
                            ELSE activation_attempt_target_state
                        END,
                        updated_at = NOW()
                  WHERE sandbox_id = $1 AND generation = $2 AND active
                    AND activation_attempt_id = $3"
    ))
    .bind(sandbox_id)
    .bind(generation)
    .bind(activation_attempt_id)
    .execute(executor.into_sqlx())
    .await?
    .rows_affected())
}

/// Take over the expired claim `expired` holds toward `target_state` on the
/// active execution at `generation`, for `reserved`, leased for
/// `lease_ttl_secs` from now, keeping its target and the upgrade it records.
/// Writes an **operation step** (the claim's takeover). Returns whether it
/// took it over: `false` when the claim changed hands, targets another
/// state, its lease still runs, or the execution records no upgrade.
pub async fn reserve_expired_upgrade(
    executor: impl Executor<'_>,
    sandbox_id: SandboxId,
    generation: i64,
    expired: SandboxActivationAttemptId,
    reserved: SandboxActivationAttemptId,
    target_state: &str,
    lease_ttl_secs: i64,
) -> Result<bool, DbError> {
    let reserved_rows = sqlx::query(sql!(
        "UPDATE sandbox_k8s_executions
                SET activation_attempt_id = $4,
                    activation_attempt_expires_at = NOW() + make_interval(secs => $5),
                    updated_at = NOW()
              WHERE sandbox_id = $1
                AND active
                AND generation = $2
                AND activation_attempt_id = $3
                AND activation_attempt_target_state = $6
                AND activation_attempt_expires_at <= NOW()
                AND runtime_upgrade_operation_id IS NOT NULL"
    ))
    .bind(sandbox_id)
    .bind(generation)
    .bind(expired)
    .bind(reserved)
    .bind(lease_ttl_secs)
    .bind(target_state)
    .execute(executor.into_sqlx())
    .await?
    .rows_affected();
    Ok(reserved_rows == 1)
}

/// End now the lease `activation_attempt_id` (`None` matches an execution
/// holding none) holds on the active execution at `generation`, while it's
/// still an unmaterialized `creating` claim (no pod UID or IP) that records
/// an upgrade. Writes an **operation step** (the lease's end). Returns the
/// number of rows changed: `0` when no such claim matches, such as one that
/// records nothing, which
/// [`super::retire::abandon_unmaterialized_create_claim`] retires instead.
///
/// The caller runs it in the transaction that decided the claim's
/// abandonment, and passes it.
pub async fn end_recorded_create_claim_lease<C>(
    conn: &mut C,
    sandbox_id: SandboxId,
    generation: i64,
    activation_attempt_id: Option<SandboxActivationAttemptId>,
) -> Result<u64, DbError>
where
    for<'a> &'a mut C: Executor<'a>,
{
    Ok(sqlx::query(sql!(
        "UPDATE sandbox_k8s_executions
             SET activation_attempt_expires_at = LEAST(activation_attempt_expires_at, NOW()),
                 updated_at = NOW()
             WHERE sandbox_id = $1
               AND active
               AND generation = $2
               AND activation_attempt_id IS NOT DISTINCT FROM $3
               AND state = 'creating'
               AND pod_uid IS NULL
               AND pod_ip IS NULL
               AND runtime_upgrade_operation_id IS NOT NULL"
    ))
    .bind(sandbox_id)
    .bind(generation)
    .bind(activation_attempt_id)
    .execute(conn.into_sqlx())
    .await?
    .rows_affected())
}

/// Put the active execution at `generation` back to an unmaterialized
/// `creating` claim with its lease ended, keeping the upgrade it records,
/// after its pod was found stale and deleted. Writes an **operation step**
/// (the execution's reset). Returns whether it did: `false` when that
/// generation isn't active or records no upgrade, which the caller then
/// retires as any stale pod's execution.
pub async fn keep_recorded_upgrade_after_stale_pod(
    executor: impl Executor<'_>,
    sandbox_id: SandboxId,
    generation: i64,
) -> Result<bool, DbError> {
    let kept = sqlx::query(sql!(
        "UPDATE sandbox_k8s_executions
                SET state = 'creating',
                    pod_materialized_at = COALESCE(
                        pod_materialized_at,
                        CASE WHEN NULLIF(BTRIM(pod_uid), '') IS NOT NULL THEN NOW() END
                    ),
                    pod_uid = NULL,
                    pod_ip = NULL,
                    activation_attempt_expires_at = LEAST(activation_attempt_expires_at, NOW()),
                    updated_at = NOW()
              WHERE sandbox_id = $1
                AND active
                AND generation = $2
                AND runtime_upgrade_operation_id IS NOT NULL"
    ))
    .bind(sandbox_id)
    .bind(generation)
    .execute(executor.into_sqlx())
    .await?
    .rows_affected();
    Ok(kept > 0)
}

/// An active execution whose claim toward `stopped` records an upgrade, as
/// [`read_leftover_upgrade`] reads it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeftoverUpgrade {
    pub pod_name: String,
    /// The recorded pod UID, blanks read as `None`.
    pub pod_uid: Option<String>,
    pub operation_id: Uuid,
    /// Whether the claim's lease still runs by the database clock; `true`
    /// for a claim with no expiry.
    pub lease_live: bool,
    /// The execution's Kubernetes node, or else the registered node the
    /// sandbox's placement pins its disk to (`active_node_id`, with no other
    /// target chosen).
    pub node_name: Option<String>,
    pub generation: i64,
    pub activation_attempt_id: SandboxActivationAttemptId,
}

/// The active execution of `sandbox_id` whose claim toward `target_state`
/// records an upgrade. `None` when there's no such execution. It takes no
/// lock: a caller that relies on it under its sandbox's row lock takes that
/// lock first and passes its transaction.
pub async fn read_leftover_upgrade(
    executor: impl Executor<'_>,
    sandbox_id: SandboxId,
    target_state: &str,
) -> Result<Option<LeftoverUpgrade>, DbError> {
    type LeftoverRow = (
        String,
        Option<String>,
        Uuid,
        bool,
        Option<String>,
        i64,
        SandboxActivationAttemptId,
    );
    let row: Option<LeftoverRow> = sqlx::query_as(sql!(
        "SELECT e.pod_name,
                        NULLIF(BTRIM(e.pod_uid), ''),
                        e.runtime_upgrade_operation_id,
                        COALESCE(e.activation_attempt_expires_at > NOW(), TRUE),
                        COALESCE(
                            NULLIF(BTRIM(e.k8s_node_name), ''),
                            (SELECT nk.k8s_node_name
                               FROM sandboxes s
                               JOIN node_k8s nk ON nk.node_id = s.active_node_id
                              WHERE s.id = e.sandbox_id
                                AND (s.target_node_id IS NULL
                                     OR s.target_node_id = s.active_node_id))
                        ),
                        e.generation,
                        e.activation_attempt_id
                   FROM sandbox_k8s_executions e
                  WHERE e.sandbox_id = $1
                    AND e.active
                    AND e.activation_attempt_id IS NOT NULL
                    AND e.activation_attempt_target_state = $2
                    AND e.runtime_upgrade_operation_id IS NOT NULL"
    ))
    .bind(sandbox_id)
    .bind(target_state)
    .fetch_optional(executor.into_sqlx())
    .await?;
    Ok(row.map(
        |(
            pod_name,
            pod_uid,
            operation_id,
            lease_live,
            node_name,
            generation,
            activation_attempt_id,
        )| {
            LeftoverUpgrade {
                pod_name,
                pod_uid,
                operation_id,
                lease_live,
                node_name,
                generation,
                activation_attempt_id,
            }
        },
    ))
}

/// Whether `sandbox_id`'s row is in `lifecycle`, as stored. Read under the
/// sandbox's row lock the caller holds, in its transaction.
pub async fn sandbox_in_lifecycle<C>(
    conn: &mut C,
    sandbox_id: SandboxId,
    lifecycle: &str,
) -> Result<bool, DbError>
where
    for<'a> &'a mut C: Executor<'a>,
{
    Ok(sqlx::query_scalar::<_, bool>(sql!(
        "SELECT EXISTS (
                             SELECT 1 FROM sandboxes WHERE id = $1 AND lifecycle = $2
                         )"
    ))
    .bind(sandbox_id)
    .bind(lifecycle)
    .fetch_one(conn.into_sqlx())
    .await?)
}

/// Whether a drain's evacuation has marked `sandbox_id` for `target`
/// (`evacuation_restore_target`). Read under the sandbox's row lock the
/// caller holds, which the evacuation's claim takes too, in its transaction.
pub async fn evacuation_marked<C>(
    conn: &mut C,
    sandbox_id: SandboxId,
    target: &str,
) -> Result<bool, DbError>
where
    for<'a> &'a mut C: Executor<'a>,
{
    Ok(sqlx::query_scalar(sql!(
        "SELECT EXISTS (
                 SELECT 1 FROM sandboxes
                  WHERE id = $1 AND evacuation_restore_target = $2
             )"
    ))
    .bind(sandbox_id)
    .bind(target)
    .fetch_one(conn.into_sqlx())
    .await?)
}

#[cfg(test)]
mod tests;
