use std::collections::BTreeMap;

use pplx_space_accounts::{NodeId, OrgId, UserId};
use pplx_space_auto_lifecycle::AutoLifecyclePolicy;
use pplx_space_common_core::{
    sql,
    types::{ClientId, ExecutionGeneration, SandboxId},
};
use pplx_space_execution_spec::DiskIoLimits;
use sqlx::types::JsonValue;
use uuid::Uuid;

use crate::{SandboxMetadata, SandboxStoreError};

/// Values shared by cold creation and warm-pool adoption.
#[derive(Debug)]
pub struct SandboxCreate<'a> {
    pub id: SandboxId,
    pub name: Option<&'a str>,
    pub user_id: UserId,
    pub org_id: OrgId,
    pub managed_by: &'a str,
    pub template: &'a str,
    pub content_hash: &'a str,
    pub vcpu: Option<i32>,
    pub memory_mib: Option<i32>,
    pub disk_mib: Option<i32>,
    pub disk_io_limits: DiskIoLimits,
    pub env: &'a BTreeMap<String, String>,
    pub metadata: &'a SandboxMetadata,
    pub auto_lifecycle: &'a AutoLifecyclePolicy,
    pub network_policy_override_json: Option<&'a JsonValue>,
    pub egress_profiles: &'a [&'a str],
    /// The client whose activation creates the sandbox's first runtime: the
    /// ECW caller's id, or `native`. Written to `activating_client_id`;
    /// `None` (an activation with no client to attribute) leaves it `NULL`.
    pub activating_client_id: Option<&'a ClientId>,
}

/// Insert an unplaced sandbox. A restore token starts it in `restoring` and
/// sets the lease against the database clock; otherwise it starts in `creating`.
pub async fn insert_unplaced(
    executor: impl sqlx::PgExecutor<'_>,
    input: &SandboxCreate<'_>,
    volume_encryption: bool,
    restore_lease_token: Option<Uuid>,
    restore_lease_seconds: i64,
) -> Result<(), SandboxStoreError> {
    sqlx::query(sql!(
        "INSERT INTO sandboxes (
            id, name, user_id, org_id, managed_by, lifecycle, placement,
            template_id, content_hash, vcpu, memory_mib, disk_mib,
            disk_io_max_operations_per_sec,
            disk_io_max_bandwidth_mib_per_sec, env,
            metadata, auto_lifecycle_json, network_policy_override_json,
            network_policy_generation, volume_encryption,
            restore_lease_token, restore_lease_expires_at,
            preserve_local_artifact_on_restore_failure, egress_profiles,
            activating_client_id
        ) VALUES (
            $1, $2, $3, $4, $5,
            CASE WHEN $18::uuid IS NULL THEN 'creating' ELSE 'restoring' END,
            'unplaced', $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, 1, $17,
            $18,
            CASE WHEN $18::uuid IS NULL
                 THEN NULL
                 ELSE NOW() + make_interval(secs => $19::double precision)
            END,
            $18::uuid IS NOT NULL, $20, $21
        )"
    ))
    .bind(input.id)
    .bind(input.name)
    .bind(input.user_id)
    .bind(input.org_id)
    .bind(input.managed_by)
    .bind(input.template)
    .bind(input.content_hash)
    .bind(input.vcpu)
    .bind(input.memory_mib)
    .bind(input.disk_mib)
    .bind(i64::try_from(input.disk_io_limits.max_operations_per_sec()).unwrap_or(i64::MAX))
    .bind(i64::try_from(input.disk_io_limits.max_bandwidth_mib_per_sec()).unwrap_or(i64::MAX))
    .bind(sqlx::types::Json(input.env))
    .bind(sqlx::types::Json(input.metadata))
    .bind(sqlx::types::Json(input.auto_lifecycle))
    .bind(input.network_policy_override_json)
    .bind(volume_encryption)
    .bind(restore_lease_token)
    .bind(restore_lease_seconds)
    .bind(input.egress_profiles)
    .bind(input.activating_client_id)
    .execute(executor)
    .await?;
    Ok(())
}

/// Insert a sandbox adopted from a warm pool with its existing placement.
/// Substrates without a database node row pass no active node id.
/// The caller owns the transaction that also records the pool claim.
pub async fn insert_pooled(
    executor: impl sqlx::PgExecutor<'_>,
    input: &SandboxCreate<'_>,
    active_node_id: Option<NodeId>,
) -> Result<(), SandboxStoreError> {
    sqlx::query(sql!(
        "INSERT INTO sandboxes (
            id, name, user_id, org_id, managed_by, lifecycle, placement,
            active_node_id, target_node_id, placement_changed_at,
            template_id, content_hash, vcpu, memory_mib, disk_mib,
            disk_io_max_operations_per_sec,
            disk_io_max_bandwidth_mib_per_sec, env, metadata,
            auto_lifecycle_json, network_policy_override_json, network_policy_generation,
            egress_profiles, activating_client_id
        )
        VALUES (
            $1, $2, $3, $4, $5, 'paused', 'on_node',
            $6, $6, NOW(),
            $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, 1, $18, $19
        )"
    ))
    .bind(input.id)
    .bind(input.name)
    .bind(input.user_id)
    .bind(input.org_id)
    .bind(input.managed_by)
    .bind(active_node_id)
    .bind(input.template)
    .bind(input.content_hash)
    .bind(input.vcpu)
    .bind(input.memory_mib)
    .bind(input.disk_mib)
    .bind(i64::try_from(input.disk_io_limits.max_operations_per_sec()).unwrap_or(i64::MAX))
    .bind(i64::try_from(input.disk_io_limits.max_bandwidth_mib_per_sec()).unwrap_or(i64::MAX))
    .bind(sqlx::types::Json(input.env))
    .bind(sqlx::types::Json(input.metadata))
    .bind(sqlx::types::Json(input.auto_lifecycle))
    .bind(input.network_policy_override_json)
    .bind(input.egress_profiles)
    .bind(input.activating_client_id)
    .execute(executor)
    .await?;
    Ok(())
}

/// Finish a create (or a create that restores from a snapshot) by moving the
/// row to the `lifecycle`, `placement` and `active_node_id` the substrate
/// reported. An **operation step**: it ends the create's in-flight work.
/// Returns whether the row matched; `false` is a lost compare-and-set, not an
/// error.
///
/// The `WHERE` is the guard, and it accepts three rows:
///
/// * a plain create (`restore_operation_id` is `None`) still `creating`;
/// * a restoring create still `restoring` under that operation's
///   `restore_lease_token`, finishing as `started` or `failed`;
/// * a row already `started` at this placement and node, which a matching
///   activation-completion receipt may have converged while the create waited
///   for command readiness. The write is then idempotent.
///
/// In the third case the row already has the reported lifecycle, placement
/// and node, so a create whose row is already `started` on a different node
/// or placement loses the compare-and-set. When a restoring create matches
/// that case and reports `on_node` with a node, the write also sets
/// `placement_changed_at` to `NOW()` if it is still null; no other case
/// changes that column. `command_ready_pending` is set to `!command_ready`
/// only while the row is `creating` (a row trigger clears it again when the
/// write moves the row to a lifecycle other than `started`). The `WHERE`
/// alone guards the write: it needs no row lock from its caller. It does not
/// know which execution the create started, so a create whose execution
/// another replaced would lose it as a mismatch; the create path calls it
/// through [`finish_created`], which tells that case apart first.
pub async fn finalize_created(
    executor: impl sqlx::PgExecutor<'_>,
    sandbox_id: SandboxId,
    lifecycle: &str,
    placement: &str,
    active_node_id: Option<NodeId>,
    command_ready: bool,
    restore_operation_id: Option<Uuid>,
) -> Result<bool, sqlx::Error> {
    let result = sqlx::query(sql!(
        "UPDATE sandboxes
         SET lifecycle = CASE
                 WHEN $6::uuid IS NOT NULL
                      AND lifecycle = 'started'
                      AND placement = 'on_node'
                      AND active_node_id IS NOT NULL
                 THEN lifecycle
                 ELSE $1
             END,
             placement = CASE
                 WHEN $6::uuid IS NOT NULL
                      AND lifecycle = 'started'
                      AND placement = 'on_node'
                      AND active_node_id IS NOT NULL
                 THEN placement
                 ELSE $2
             END,
             active_node_id = CASE
                 WHEN $6::uuid IS NOT NULL
                      AND lifecycle = 'started'
                      AND placement = 'on_node'
                      AND active_node_id IS NOT NULL
                 THEN active_node_id
                 ELSE $3
             END,
             placement_changed_at = CASE
                 WHEN $6::uuid IS NOT NULL
                      AND $1 = 'started'
                      AND $2 = 'on_node'
                      AND $3 IS NOT NULL
                      AND lifecycle = 'started'
                      AND placement = 'on_node'
                      AND active_node_id IS NOT DISTINCT FROM $3
                 THEN COALESCE(placement_changed_at, NOW())
                 ELSE placement_changed_at
             END,
             command_ready_pending = CASE
                 WHEN lifecycle = 'creating' THEN $5
                 ELSE command_ready_pending
             END,
             preserve_local_artifact_on_restore_failure = CASE
                 WHEN lifecycle = 'restoring' AND $1 IN ('started', 'failed') THEN FALSE
                 ELSE preserve_local_artifact_on_restore_failure
             END
         WHERE id = $4
           AND (
               ($6::uuid IS NULL AND lifecycle = 'creating')
               OR (
                   $6::uuid IS NOT NULL
                   AND lifecycle = 'restoring'
                   AND restore_lease_token = $6
                   AND $1 IN ('started', 'failed')
               )
               OR (
                   $1 = 'started'
                   AND lifecycle = 'started'
                   AND placement = $2
                   AND active_node_id IS NOT DISTINCT FROM $3
               )
           )"
    ))
    .bind(lifecycle)
    .bind(placement)
    .bind(active_node_id)
    .bind(sandbox_id.as_uuid())
    .bind(!command_ready)
    .bind(restore_operation_id)
    .execute(executor)
    .await?;

    Ok(result.rows_affected() == 1)
}

/// Record the node a just-created sandbox landed on and stamp
/// `placement_changed_at`. An **observed projection** of the substrate's
/// create outcome. Returns whether the row matched; `false` means the sandbox
/// moved on before the create recorded it, and the write changed nothing.
///
/// The `WHERE` guards it with the create's lifecycles: the row must still be
/// `creating`, as [`insert_unplaced`] inserted it, or `started`, as an
/// activation-completion receipt may have converged it while the create
/// waited for command readiness (the third case [`finalize_created`]
/// accepts). A sandbox deleted, failed by the transient-lifecycle sweeper,
/// quarantined or suspended in the meantime is not pointed back at the node.
///
/// That is the whole guard of a Direct create, which records its node with
/// this before anything else, since its runtime client resolves the worker
/// from it: the Direct route identity is the Direct substrate's own record
/// (archcheck's `direct_tables` rule keeps it there). A Kubernetes create
/// records its node only through [`finish_created`], which first checks,
/// under the sandbox row lock, that the create's execution is still the
/// active one.
pub async fn record_created_placement(
    executor: impl sqlx::PgExecutor<'_>,
    sandbox_id: SandboxId,
    active_node_id: NodeId,
) -> Result<bool, sqlx::Error> {
    let updated = sqlx::query(sql!(
        "UPDATE sandboxes
            SET active_node_id = $1,
                placement_changed_at = NOW()
          WHERE id = $2
            AND lifecycle IN ('creating', 'started')"
    ))
    .bind(active_node_id)
    .bind(sandbox_id)
    .execute(executor)
    .await?;
    Ok(updated.rows_affected() == 1)
}

/// A create's outcome, for [`finish_created`]: what the substrate reported,
/// and which execution the create started.
#[derive(Debug, Clone, Copy)]
pub struct CreatedOutcome<'a> {
    pub sandbox_id: SandboxId,
    pub lifecycle: &'a str,
    pub placement: &'a str,
    pub active_node_id: Option<NodeId>,
    pub command_ready: bool,
    /// The restore operation of a create that restores from a snapshot, whose
    /// lease the row carries; see [`finalize_created`].
    pub restore_operation_id: Option<Uuid>,
    /// The generation of the Kubernetes execution the create started. `None`
    /// on Direct, which recorded its node already (see
    /// [`record_created_placement`]) and is guarded by lifecycle alone.
    pub execution_generation: Option<ExecutionGeneration>,
}

/// What [`finish_created`] did with a create's outcome.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CreatedFinish {
    /// The row took the outcome: [`finalize_created`] matched, after the node
    /// was recorded if the create reported one and still owned its
    /// execution.
    Finalized,
    /// The create's Kubernetes execution is no longer the sandbox's active
    /// one: something replaced it while the create was in flight, as a
    /// recovering runtime client's started recovery does once the pod is
    /// lost, on a row still `creating` or already `started`. The sandbox
    /// exists and runs, or is being rebuilt, on `active_generation` (`None`
    /// while nothing is active), and that execution's own placement writer
    /// owns the node. Nothing of the create's node was written; a `creating`
    /// row moved to the create's lifecycle with `command_ready_pending` set,
    /// since nobody has confirmed the successor's command API, and a `started`
    /// row was left as it was. As for a slow create, the activation-completion
    /// receipt that completes the active execution's attempt
    /// ([`crate::ingest::release_completed_activations`]) or the access path's
    /// confirmation clears the marker; a receipt of the create's replaced
    /// execution does not. The create succeeded and must not be rolled back:
    /// that would delete the successor.
    Superseded {
        active_generation: Option<ExecutionGeneration>,
    },
    /// The row is in no lifecycle the create owns, or `started` on another
    /// node or placement than the create's own execution reported: the
    /// compare-and-set of [`finalize_created`] lost.
    Lost,
}

/// Finish a create in one guarded step: record its node and its lifecycle
/// only while the create still owns the row and, for a Kubernetes create,
/// while the execution it started is still the sandbox's active one. An
/// **operation step**, composed of [`record_created_placement`] and
/// [`finalize_created`].
///
/// Locks the sandbox row first, as every writer that replaces the active
/// Kubernetes execution does before it touches the execution rows, then reads
/// which execution is active in a statement of its own. So a replacement
/// that committed while this waited for the lock is seen, and one that has
/// not started waits for this transaction; a read in the same statement as
/// the write would see the execution rows as they were before the lock wait.
///
/// With `execution_generation`, a `creating` or `started` row whose active
/// execution is another one ends [`CreatedFinish::Superseded`]. Otherwise the
/// node is written for a Kubernetes create that reported one (a restoring
/// create's row records its node through [`finalize_created`]), and
/// [`finalize_created`] decides between [`CreatedFinish::Finalized`] and
/// [`CreatedFinish::Lost`]. A Direct create (`None`) is guarded by lifecycle
/// alone, as before.
///
/// Runs in the caller's transaction, which must commit for anything to be
/// written; the row lock is held until then.
pub async fn finish_created(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    outcome: CreatedOutcome<'_>,
) -> Result<CreatedFinish, sqlx::Error> {
    let lifecycle: Option<String> = sqlx::query_scalar(sql!(
        "SELECT lifecycle FROM sandboxes WHERE id = $1 FOR UPDATE"
    ))
    .bind(outcome.sandbox_id)
    .fetch_optional(&mut **tx)
    .await?;
    let Some(lifecycle) = lifecycle else {
        return Ok(CreatedFinish::Lost);
    };

    if let Some(generation) = outcome.execution_generation {
        let active: Option<i64> = sqlx::query_scalar(sql!(
            "SELECT generation
               FROM sandbox_k8s_executions
              WHERE sandbox_id = $1 AND active"
        ))
        .bind(outcome.sandbox_id)
        .fetch_optional(&mut **tx)
        .await?;
        if active != Some(generation.get()) {
            let owned = match lifecycle.as_str() {
                "creating" => outcome.restore_operation_id.is_none(),
                "started" => true,
                _ => false,
            };
            if !owned {
                return Ok(CreatedFinish::Lost);
            }
            if lifecycle == "creating" {
                sqlx::query(sql!(
                    "UPDATE sandboxes
                        SET lifecycle = $2,
                            command_ready_pending = TRUE
                      WHERE id = $1
                        AND lifecycle = 'creating'"
                ))
                .bind(outcome.sandbox_id)
                .bind(outcome.lifecycle)
                .execute(&mut **tx)
                .await?;
            }
            let active_generation = active
                .map(ExecutionGeneration::new)
                .transpose()
                .map_err(|error| sqlx::Error::Decode(Box::new(error)))?;
            return Ok(CreatedFinish::Superseded { active_generation });
        }
        if outcome.restore_operation_id.is_none()
            && let Some(active_node_id) = outcome.active_node_id
        {
            record_created_placement(&mut **tx, outcome.sandbox_id, active_node_id).await?;
        }
    }

    let finalized = finalize_created(
        &mut **tx,
        outcome.sandbox_id,
        outcome.lifecycle,
        outcome.placement,
        outcome.active_node_id,
        outcome.command_ready,
        outcome.restore_operation_id,
    )
    .await?;
    Ok(if finalized {
        CreatedFinish::Finalized
    } else {
        CreatedFinish::Lost
    })
}

/// Delete the row of a create that failed or was rolled back. An
/// **operation step**: the create's compensation.
///
/// No lifecycle predicate: it deletes the row whatever its lifecycle, relying
/// on the caller to pass only the id its own create inserted. Unlike
/// [`crate::delete::delete_with_dependents`] it clears no dependents beyond
/// what cascades from the row; reconcile state, which does not cascade, is left
/// to the caller. The statement carries no `sql!` locator,
/// as at the call sites it moved from.
pub async fn delete_failed_create(
    executor: impl sqlx::PgExecutor<'_>,
    sandbox_id: SandboxId,
) -> Result<(), sqlx::Error> {
    sqlx::query("DELETE FROM sandboxes WHERE id = $1")
        .bind(sandbox_id.as_uuid())
        .execute(executor)
        .await?;
    Ok(())
}
