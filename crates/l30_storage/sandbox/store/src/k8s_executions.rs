//! `sandbox_k8s_executions`, the per-generation execution records of
//! Kubernetes-managed sandboxes.
//!
//! This module holds the table's unlocked reads. Each runs on the executor
//! its caller passes, so one issued inside a transaction stays in it, and
//! keeps its comparisons with the database clock in the SQL.
//!
//! Its submodules, each named for the transition or the path that issues
//! them, hold the execution-ownership transitions and their lock verbs, as
//! [`activation_fence`] does for the activation attempt's fence and route
//! publication. Each guarded write there keeps its guard whole, bound to the
//! caller's values, and returns its outcome; each `FOR UPDATE` read is a
//! `lock_…` verb on the caller's transaction, its lock clause verbatim (see
//! "Guarded state transitions" and "Locks the caller relies on after the
//! statement" in the `space-data-store-development` skill). Some are correct
//! only under locks their caller holds, as [`activation_fence`]'s claims are
//! after [`activation_fence::lock_attempt_owner`], so the callers keep their
//! transactions, their lock order (the sandbox row before its executions),
//! their re-checks between statements and the decision on each outcome.

use pplx_space_accounts::{ClientId, NodeId};
use pplx_space_k8s_ids::PodUid;
use pplx_space_node_model::SubstrateKind;
use pplx_space_postgres_pool::{DbError, Executor};
use pplx_space_sandbox_model::{ExecutionGeneration, SandboxActivationAttemptId, SandboxId};
use pplx_space_sql_locator::sql;
use uuid::Uuid;

use crate::SandboxStoreError;

pub mod activation_fence;
pub mod auto_lifecycle_delete;
pub mod backup_generation_fence;
pub mod claim;
pub mod materialize;
#[cfg(test)]
pub(crate) mod ownership_test_fixture;
pub mod pod_less_giveup;
pub mod pod_placement;
pub mod pod_tracking;
pub mod pool_claim;
pub mod quarantine_recovery;
pub mod quarantine_rollback;
pub mod retire;
pub mod runtime_attribution;
pub mod started_recovery;
pub mod suspend;
pub mod suspended_reaper;
pub mod transient_sweeper;
pub mod transition;

/// The sandbox's active execution, as [`active_execution`] reads it.
#[derive(Debug, Clone, PartialEq, Eq, sqlx::FromRow)]
pub struct ActiveExecutionRecord {
    pub generation: i64,
    pub pod_name: String,
    /// The row records a non-blank pod UID.
    pub pod_materialized: bool,
    /// Raw `state`; the caller parses it.
    pub state: String,
    /// `updated_at` is at least the caller's age limit old, by the database
    /// clock.
    pub create_age_expired: bool,
    pub activation_attempt_id: Option<SandboxActivationAttemptId>,
    pub route_activation_id: Option<SandboxActivationAttemptId>,
}

/// The sandbox's active execution, if it has one, with whether it was last
/// updated at least `create_age_limit_secs` ago.
pub async fn active_execution(
    executor: impl Executor<'_>,
    sandbox_id: SandboxId,
    create_age_limit_secs: i64,
) -> Result<Option<ActiveExecutionRecord>, DbError> {
    Ok(sqlx::query_as::<_, ActiveExecutionRecord>(sql!(
        "SELECT generation, pod_name,
                NULLIF(BTRIM(pod_uid), '') IS NOT NULL AS pod_materialized,
                state,
                updated_at <= NOW() - make_interval(secs => $2) AS create_age_expired,
                activation_attempt_id, route_activation_id
         FROM sandbox_k8s_executions
         WHERE sandbox_id = $1 AND active"
    ))
    .bind(sandbox_id)
    .bind(create_age_limit_secs)
    .fetch_optional(executor.into_sqlx())
    .await?)
}

/// Whether the sandbox's execution `generation` is its active one, when
/// that execution's pod was placed on one of `node_ids` (its Kubernetes node
/// is registered as one of them). `None` when there is no such execution, or
/// it ran elsewhere.
pub async fn generation_active_on_nodes(
    executor: impl Executor<'_>,
    sandbox_id: SandboxId,
    generation: ExecutionGeneration,
    node_ids: &[NodeId],
) -> Result<Option<bool>, DbError> {
    Ok(sqlx::query_scalar::<_, bool>(sql!(
        "SELECT e.active
           FROM sandbox_k8s_executions e
           JOIN node_k8s nk ON nk.k8s_node_name = e.k8s_node_name
          WHERE e.sandbox_id = $1
            AND e.generation = $2
            AND nk.node_id = ANY($3)"
    ))
    .bind(sandbox_id)
    .bind(generation.get())
    .bind(node_ids)
    .fetch_optional(executor.into_sqlx())
    .await?)
}

/// Milliseconds left, by the database clock and never negative, until the
/// newest active execution at or past `generation` is `lease_ttl_secs` past
/// its last update. `None` when there is no such execution.
pub async fn started_recovery_lease_remaining_millis(
    executor: impl Executor<'_>,
    sandbox_id: SandboxId,
    generation: i64,
    lease_ttl_secs: i64,
) -> Result<Option<i64>, DbError> {
    Ok(sqlx::query_scalar::<_, i64>(sql!(
        "SELECT GREATEST(
                    CEIL(EXTRACT(EPOCH FROM (
                        updated_at + make_interval(secs => $3) - NOW()
                    )) * 1000)::BIGINT,
                    0::BIGINT
                )
         FROM sandbox_k8s_executions
         WHERE sandbox_id = $1 AND generation >= $2 AND active
         ORDER BY generation DESC
         LIMIT 1"
    ))
    .bind(sandbox_id)
    .bind(generation)
    .bind(lease_ttl_secs)
    .fetch_optional(executor.into_sqlx())
    .await?)
}

/// A started recovery still inside its lease, as [`fresh_started_recovery`]
/// reads it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FreshStartedRecovery {
    pub generation: i64,
    /// Milliseconds left on the lease by the database clock, never negative.
    pub lease_remaining_millis: i64,
}

/// The sandbox's active execution in `recovering_state` that was last
/// updated less than `lease_ttl_secs` ago by the database clock, with the
/// time its lease has left. `None` when there is no such execution.
pub async fn fresh_started_recovery(
    executor: impl Executor<'_>,
    sandbox_id: SandboxId,
    recovering_state: &str,
    lease_ttl_secs: i64,
) -> Result<Option<FreshStartedRecovery>, DbError> {
    let row = sqlx::query_as::<_, (i64, i64)>(sql!(
        "SELECT generation,
                    GREATEST(
                        CEIL(EXTRACT(EPOCH FROM (
                            updated_at + make_interval(secs => $3) - NOW()
                        )) * 1000)::BIGINT,
                        0::BIGINT
                    ) AS lease_remaining_millis
             FROM sandbox_k8s_executions
             WHERE sandbox_id = $1 AND active AND state = $2
               AND updated_at > NOW() - make_interval(secs => $3)"
    ))
    .bind(sandbox_id.as_uuid())
    .bind(recovering_state)
    .bind(lease_ttl_secs)
    .fetch_optional(executor.into_sqlx())
    .await?;
    Ok(row.map(
        |(generation, lease_remaining_millis)| FreshStartedRecovery {
            generation,
            lease_remaining_millis,
        },
    ))
}

/// Whether a failed sandbox's active execution still holds a first activation
/// that never finished, and whether that activation's lease has expired by
/// the database clock, as [`list_failed_active_executions`] defines them.
#[derive(Debug, Clone, Copy, PartialEq, Eq, sqlx::FromRow)]
pub struct FirstActivationRecord {
    pub first_activation_unfinished: bool,
    pub first_activation_abandoned: bool,
}

/// One `failed` sandbox with an active execution, as
/// [`list_failed_active_executions`] lists it: the execution's pod fence and
/// its first activation.
#[derive(Debug, Clone, PartialEq, Eq, sqlx::FromRow)]
pub struct FailedActiveExecutionRecord {
    pub id: SandboxId,
    pub k8s_node_name: Option<String>,
    pub pod_name: String,
    pub activation_attempt_id: Option<SandboxActivationAttemptId>,
    pub generation: i64,
    /// Blank UIDs read as `None`.
    pub pod_uid: Option<PodUid>,
    pub route_activation_id: Option<SandboxActivationAttemptId>,
    pub first_activation_unfinished: bool,
    pub first_activation_abandoned: bool,
}

/// Failed-cleanup candidates for the transient lifecycle sweeper: up to
/// `limit` `failed` rows managed by `managed_by` (the sweeper passes
/// [`SubstrateKind::Kubernetes`] only) that still carry an active execution,
/// oldest attempt first. The sweeper captures each row's pod fence here,
/// before it probes the pod, and the retire after the probe fences on that
/// capture.
///
/// `first_activation_unfinished` says the execution's first activation, for
/// a cold create the create itself, never finished: the execution never
/// reached `running` (still `creating`, or `created` once its pod UID was
/// written; the wake dial can record a pod IP without moving it on), and
/// it still holds an activation attempt, the route's or a later one.
/// `first_activation_abandoned` adds that the attempt's lease has expired. It
/// is the same predicate the repair CAS
/// ([`crate::sweeper::repair_failed_active_execution`]) refuses, without the
/// expiry. A finished activation leaves the execution `running`, and a
/// started recovery `recovering`, even when the best-effort release of its
/// attempt was missed. Like the sweep's `creating` owner check, it reads
/// whichever attempt the execution holds. Unlike it, it needs no full age
/// window past the expiry: that window spares a create that may still
/// finish, and this row is already `failed`, so its create can't. An owner
/// that finds its row failed rolls the create back and deletes the sandbox.
///
/// An execution that never reached `running` and whose last owner gave up its
/// activation without dispatching it counts too: it holds no attempt, but
/// keeps the expiry `give_up_activation_attempt_best_effort` cut to when it
/// gave up, so it reads as abandoned once that passes. A start that adopts a
/// crashed create's row and fails before its wake leaves this shape when it
/// never recorded the pod as running. A finished activation clears its expiry
/// along with its attempt, so it can't take this shape, and an execution with
/// no lease at all (a non-blocking warm-pool adoption never takes one) counts
/// as finished. A start that found the pod up records it first
/// (`ensure_pod_exists` moves the execution to `running`), and a `running`
/// execution counts as finished here: the sweep still fails that row (no
/// attempt is left for a completion receipt to finish it), but if its retire
/// doesn't complete, the sweeper's repair can put it back to `started`.
pub async fn list_failed_active_executions(
    executor: impl Executor<'_>,
    managed_by: SubstrateKind,
    limit: i64,
) -> Result<Vec<FailedActiveExecutionRecord>, DbError> {
    Ok(sqlx::query_as::<_, FailedActiveExecutionRecord>(sql!(
        "SELECT s.id, e.k8s_node_name, e.pod_name, e.activation_attempt_id, e.generation,
                NULLIF(BTRIM(e.pod_uid), '') AS pod_uid, e.route_activation_id,
                COALESCE(
                    e.state IN ('creating', 'created')
                        AND (e.activation_attempt_id IS NOT NULL
                             OR e.activation_attempt_expires_at IS NOT NULL),
                    FALSE
                ) AS first_activation_unfinished,
                COALESCE(
                    e.state IN ('creating', 'created')
                        AND (e.activation_attempt_id IS NOT NULL
                             OR e.activation_attempt_expires_at IS NOT NULL)
                        AND e.activation_attempt_expires_at < NOW(),
                    FALSE
                ) AS first_activation_abandoned
         FROM sandboxes s
         JOIN sandbox_k8s_executions e ON e.sandbox_id = s.id
         WHERE s.managed_by = $1
           AND s.lifecycle = 'failed'
           AND e.active
         ORDER BY e.updated_at ASC, s.id
         LIMIT $2"
    ))
    .bind(managed_by)
    .bind(limit)
    .fetch_all(executor.into_sqlx())
    .await?)
}

/// Re-read one failed-cleanup candidate's first activation just before an
/// alive retire, so an execution that reached `running` since the listing
/// (and the probe) moves it off the retire. Its two columns are
/// [`list_failed_active_executions`]'s, spelled the same way. `None`: the
/// sandbox is no longer a `failed` row managed by `managed_by` with an active
/// execution.
pub async fn first_activation(
    executor: impl Executor<'_>,
    sandbox_id: SandboxId,
    managed_by: SubstrateKind,
) -> Result<Option<FirstActivationRecord>, DbError> {
    Ok(sqlx::query_as::<_, FirstActivationRecord>(sql!(
        "SELECT COALESCE(
                    e.state IN ('creating', 'created')
                        AND (e.activation_attempt_id IS NOT NULL
                             OR e.activation_attempt_expires_at IS NOT NULL),
                    FALSE
                ) AS first_activation_unfinished,
                COALESCE(
                    e.state IN ('creating', 'created')
                        AND (e.activation_attempt_id IS NOT NULL
                             OR e.activation_attempt_expires_at IS NOT NULL)
                        AND e.activation_attempt_expires_at < NOW(),
                    FALSE
                ) AS first_activation_abandoned
         FROM sandboxes s
         JOIN sandbox_k8s_executions e ON e.sandbox_id = s.id
         WHERE s.id = $1
           AND s.managed_by = $2
           AND s.lifecycle = 'failed'
           AND e.active"
    ))
    .bind(sandbox_id)
    .bind(managed_by)
    .fetch_optional(executor.into_sqlx())
    .await?)
}

/// Whether `sandbox_id` has an active execution row. A plain read: run it in
/// the caller's transaction to see that transaction's writes.
pub async fn exists(
    executor: impl Executor<'_>,
    sandbox_id: SandboxId,
) -> Result<bool, SandboxStoreError> {
    Ok(sqlx::query_scalar::<_, bool>(sql!(
        "SELECT EXISTS (
                     SELECT 1
                       FROM sandbox_k8s_executions
                      WHERE sandbox_id = $1
                        AND active
                 )"
    ))
    .bind(sandbox_id)
    .fetch_one(executor.into_sqlx())
    .await?)
}

/// One pod name the sandbox has had, with the namespace and generation its
/// execution row recorded for it.
#[derive(Debug, Clone, PartialEq, Eq, sqlx::FromRow)]
pub struct ExecutionPodRef {
    /// Decoded as [`ExecutionGeneration`], so a stored value that isn't one
    /// fails the database read rather than reaching the node.
    pub generation: ExecutionGeneration,
    pub pod_namespace: String,
    pub pod_name: String,
}

/// Every pod name `sandbox_id`'s execution rows recorded, newest generation
/// first: a live pod is likeliest under the newest name.
pub async fn newest_first(
    executor: impl Executor<'_>,
    sandbox_id: SandboxId,
) -> Result<Vec<ExecutionPodRef>, SandboxStoreError> {
    Ok(sqlx::query_as::<_, ExecutionPodRef>(sql!(
        "SELECT generation, pod_namespace, pod_name
           FROM sandbox_k8s_executions
          WHERE sandbox_id = $1
          ORDER BY generation DESC"
    ))
    .bind(sandbox_id)
    .fetch_all(executor.into_sqlx())
    .await?)
}

/// The Kubernetes node the newest execution of `sandbox_id` that recorded one
/// ran on; `None` when none did. The unique index on
/// `(sandbox_id, generation)` makes the pick deterministic.
pub async fn latest_k8s_node_name(
    executor: impl Executor<'_>,
    sandbox_id: SandboxId,
) -> Result<Option<String>, DbError> {
    Ok(sqlx::query_scalar::<_, String>(sql!(
        "SELECT ske.k8s_node_name
           FROM sandbox_k8s_executions ske
          WHERE ske.sandbox_id = $1
            AND ske.k8s_node_name IS NOT NULL
          ORDER BY ske.generation DESC
          LIMIT 1"
    ))
    .bind(sandbox_id)
    .fetch_optional(executor.into_sqlx())
    .await?)
}

/// The trimmed, distinct Kubernetes node names the inactive executions of
/// `sandbox_id` recorded, sorted; blank names are skipped.
pub async fn inactive_k8s_node_names(
    executor: impl Executor<'_>,
    sandbox_id: SandboxId,
) -> Result<Vec<String>, DbError> {
    Ok(sqlx::query_scalar(sql!(
        "SELECT DISTINCT BTRIM(k8s_node_name)
               FROM sandbox_k8s_executions
              WHERE sandbox_id = $1
                AND NOT active
                AND NULLIF(BTRIM(k8s_node_name), '') IS NOT NULL
              ORDER BY 1"
    ))
    .bind(sandbox_id)
    .fetch_all(executor.into_sqlx())
    .await?)
}

/// The route activation the active execution of `sandbox_id` at
/// `generation` tracking `pod_uid` has published; `None` when there is no
/// such execution or it has published none.
pub async fn active_route_activation_id(
    executor: impl Executor<'_>,
    sandbox_id: SandboxId,
    generation: i64,
    pod_uid: &str,
) -> Result<Option<Uuid>, DbError> {
    let route_activation_id = sqlx::query_scalar::<_, Option<Uuid>>(sql!(
        "SELECT route_activation_id
               FROM sandbox_k8s_executions
              WHERE sandbox_id = $1
                AND generation = $2
                AND active
                AND pod_uid = $3"
    ))
    .bind(sandbox_id)
    .bind(generation)
    .bind(pod_uid)
    .fetch_optional(executor.into_sqlx())
    .await?;
    Ok(route_activation_id.flatten())
}

/// Whether the active execution of `sandbox_id` at `generation` was created
/// no later than the sandbox's last lifecycle change, while that sandbox is
/// `stopped`: the execution its stop retired rather than one booted after.
/// Both timestamps come from the database clock.
pub async fn predates_current_stop(
    executor: impl Executor<'_>,
    sandbox_id: SandboxId,
    generation: i64,
) -> Result<bool, DbError> {
    Ok(sqlx::query_scalar(sql!(
        "SELECT EXISTS (
                SELECT 1 FROM sandbox_k8s_executions execution
                JOIN sandboxes sandbox ON sandbox.id = execution.sandbox_id
                WHERE execution.sandbox_id = $1 AND execution.generation = $2
                  AND execution.active AND sandbox.lifecycle = 'stopped'
                  AND execution.created_at <= sandbox.lifecycle_changed_at
            )"
    ))
    .bind(sandbox_id)
    .bind(generation)
    .fetch_one(executor.into_sqlx())
    .await?)
}

/// The generation a new execution claim for `sandbox_id` would take now,
/// one past the highest it has recorded (`1` for none), or `None` when an
/// active execution exists and a claim would reuse it. Taken without a lock,
/// it is a hint: a concurrent claim can take that generation first.
///
/// Mirrors the claim's generation allocation, [`claim::mint_generation`];
/// change them together.
pub async fn unclaimed_generation(
    executor: impl Executor<'_>,
    sandbox_id: SandboxId,
) -> Result<Option<i64>, DbError> {
    Ok(sqlx::query_scalar::<_, Option<i64>>(sql!(
        "SELECT CASE
                        WHEN BOOL_OR(active) THEN NULL
                        ELSE COALESCE(MAX(generation), 0) + 1
                    END
             FROM sandbox_k8s_executions
             WHERE sandbox_id = $1"
    ))
    .bind(sandbox_id)
    .fetch_one(executor.into_sqlx())
    .await?)
}

/// Whether `sandbox_id`'s active execution is the one at `generation`.
pub async fn active_is_generation(
    executor: impl Executor<'_>,
    sandbox_id: SandboxId,
    generation: i64,
) -> Result<bool, DbError> {
    Ok(sqlx::query_scalar(sql!(
        "SELECT EXISTS(
                 SELECT 1
                   FROM sandbox_k8s_executions
                  WHERE sandbox_id = $1
                    AND generation = $2
                    AND active
             )"
    ))
    .bind(sandbox_id)
    .bind(generation)
    .fetch_one(executor.into_sqlx())
    .await?)
}

/// Whether the active execution of `sandbox_id` at `generation` is in
/// `state`; `None` when that generation isn't the active execution.
pub async fn active_in_state(
    executor: impl Executor<'_>,
    sandbox_id: SandboxId,
    generation: i64,
    state: &str,
) -> Result<Option<bool>, DbError> {
    Ok(sqlx::query_scalar::<_, bool>(sql!(
        "SELECT state = $3
             FROM sandbox_k8s_executions
             WHERE sandbox_id = $1 AND generation = $2 AND active"
    ))
    .bind(sandbox_id)
    .bind(generation)
    .bind(state)
    .fetch_optional(executor.into_sqlx())
    .await?)
}

/// Whether `sandbox_id` has an execution newer than `generation`, in any
/// state.
pub async fn successor_exists(
    executor: impl Executor<'_>,
    sandbox_id: SandboxId,
    generation: i64,
) -> Result<bool, DbError> {
    Ok(sqlx::query_scalar::<_, bool>(sql!(
        "SELECT EXISTS (
                     SELECT 1
                       FROM sandbox_k8s_executions
                      WHERE sandbox_id = $1
                        AND generation > $2
                 )"
    ))
    .bind(sandbox_id)
    .bind(generation)
    .fetch_one(executor.into_sqlx())
    .await?)
}

/// Whether the execution of `sandbox_id` at `generation` that names
/// `pod_name` is retired (`active = FALSE`); `false` when there is no such
/// execution.
pub async fn generation_retired(
    executor: impl Executor<'_>,
    sandbox_id: SandboxId,
    generation: i64,
    pod_name: &str,
) -> Result<bool, DbError> {
    Ok(sqlx::query_scalar::<_, bool>(sql!(
        "SELECT EXISTS (
                 SELECT 1
                   FROM sandbox_k8s_executions
                  WHERE sandbox_id = $1
                    AND generation = $2
                    AND pod_name = $3
                    AND NOT active
             )"
    ))
    .bind(sandbox_id)
    .bind(generation)
    .bind(pod_name)
    .fetch_one(executor.into_sqlx())
    .await?)
}

/// The pod name of every execution `sandbox_id` has recorded.
pub async fn pod_names(
    executor: impl Executor<'_>,
    sandbox_id: SandboxId,
) -> Result<Vec<String>, DbError> {
    Ok(sqlx::query_scalar::<_, String>(sql!(
        "SELECT pod_name FROM sandbox_k8s_executions WHERE sandbox_id = $1"
    ))
    .bind(sandbox_id)
    .fetch_all(executor.into_sqlx())
    .await?)
}

/// The pod names of the executions of `sandbox_id` older than `generation`.
pub async fn pod_names_before_generation(
    executor: impl Executor<'_>,
    sandbox_id: SandboxId,
    generation: i64,
) -> Result<Vec<String>, DbError> {
    Ok(sqlx::query_scalar::<_, String>(sql!(
        "SELECT pod_name
               FROM sandbox_k8s_executions
              WHERE sandbox_id = $1
                AND generation < $2"
    ))
    .bind(sandbox_id)
    .bind(generation)
    .fetch_all(executor.into_sqlx())
    .await?)
}

/// The pod names of the inactive executions of `sandbox_id`.
pub async fn inactive_pod_names(
    executor: impl Executor<'_>,
    sandbox_id: SandboxId,
) -> Result<Vec<String>, DbError> {
    Ok(sqlx::query_scalar::<_, String>(sql!(
        "SELECT pod_name
               FROM sandbox_k8s_executions
              WHERE sandbox_id = $1
                AND NOT active"
    ))
    .bind(sandbox_id)
    .fetch_all(executor.into_sqlx())
    .await?)
}

/// The active execution a started-recovery follower waits on, with its
/// sandbox's lifecycle.
#[derive(Clone, Debug, sqlx::FromRow)]
pub struct RecoveringExecution {
    pub generation: i64,
    pub state: String,
    pub pod_name: String,
    /// `host(pod_ip)`; `None` before the pod has an address.
    pub pod_ip: Option<String>,
    pub lifecycle: String,
}

/// The active execution of `sandbox_id` at `generation` or newer whose state
/// is one of `live_states`, the states the caller counts as a recovery still
/// in progress or done; `None` when there is none.
pub async fn recovering_execution(
    executor: impl Executor<'_>,
    sandbox_id: SandboxId,
    generation: i64,
    live_states: [&str; 3],
) -> Result<Option<RecoveringExecution>, DbError> {
    let [first, second, third] = live_states;
    Ok(sqlx::query_as::<_, RecoveringExecution>(sql!(
        "SELECT e.generation, e.state, e.pod_name, host(e.pod_ip) AS pod_ip, s.lifecycle
             FROM sandbox_k8s_executions e
             JOIN sandboxes s ON s.id = e.sandbox_id
             WHERE e.sandbox_id = $1 AND e.generation >= $2 AND e.active
               AND e.state IN ($3, $4, $5)"
    ))
    .bind(sandbox_id)
    .bind(generation)
    .bind(first)
    .bind(second)
    .bind(third)
    .fetch_optional(executor.into_sqlx())
    .await?)
}

/// The active execution of a sandbox with the parts of its row the
/// Kubernetes substrate reads beside it. `state` is the stored text: the
/// caller owns that vocabulary.
#[derive(Clone, Debug, sqlx::FromRow)]
pub struct ActiveExecutionStatus {
    pub generation: i64,
    pub pod_name: String,
    /// `None` for a blank UID as well as a missing one.
    pub pod_uid: Option<PodUid>,
    /// `host(pod_ip)`.
    pub pod_ip: Option<String>,
    pub k8s_node_name: Option<String>,
    /// The sandbox's `active_node_id`.
    pub current_node_id: Option<NodeId>,
    pub state: String,
    pub lifecycle: String,
    /// Whether the execution row was last updated at least `create_ttl_secs`
    /// ago by the database clock.
    pub create_age_expired: bool,
    pub activation_attempt_id: Option<SandboxActivationAttemptId>,
    pub route_activation_id: Option<SandboxActivationAttemptId>,
    pub activating_client_id: Option<ClientId>,
}

/// [`ActiveExecutionStatus`] of `sandbox_id`'s active execution; `None` when
/// it has none.
pub async fn active_execution_status(
    executor: impl Executor<'_>,
    sandbox_id: SandboxId,
    create_ttl_secs: i64,
) -> Result<Option<ActiveExecutionStatus>, DbError> {
    Ok(sqlx::query_as::<_, ActiveExecutionStatus>(sql!(
        "SELECT e.generation, e.pod_name, NULLIF(BTRIM(e.pod_uid), '') AS pod_uid,
                    host(e.pod_ip) AS pod_ip, s.active_node_id AS current_node_id,
                    e.k8s_node_name, e.state, s.lifecycle,
                    e.updated_at <= NOW() - make_interval(secs => $2) AS create_age_expired,
                    e.activation_attempt_id,
                    e.route_activation_id, s.activating_client_id
             FROM sandbox_k8s_executions e
             JOIN sandboxes s ON s.id = e.sandbox_id
             WHERE e.sandbox_id = $1 AND e.active"
    ))
    .bind(sandbox_id)
    .bind(create_ttl_secs)
    .fetch_optional(executor.into_sqlx())
    .await?)
}
