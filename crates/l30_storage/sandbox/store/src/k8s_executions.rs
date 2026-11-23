//! Reads of `sandbox_k8s_executions`, the per-generation execution records of
//! Kubernetes-managed sandboxes, for callers that read them without locking
//! them.
//!
//! Writes of the table stay with the execution-ownership paths that issue
//! them, as do the generation allocation and every `FOR UPDATE` read: they
//! are correct only under the caller's locks. Each read here runs on the
//! executor its caller passes, so one issued inside a transaction stays in
//! it, and keeps its comparisons with the database clock in the SQL.

use pplx_space_k8s_ids::PodUid;
use pplx_space_node_model::SubstrateKind;
use pplx_space_sandbox_model::{SandboxActivationAttemptId, SandboxId};
use pplx_space_sql_locator::sql;
use sqlx::PgExecutor;

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
    executor: impl PgExecutor<'_>,
    sandbox_id: SandboxId,
    create_age_limit_secs: i64,
) -> Result<Option<ActiveExecutionRecord>, sqlx::Error> {
    sqlx::query_as::<_, ActiveExecutionRecord>(sql!(
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
    .fetch_optional(executor)
    .await
}

/// Milliseconds left, by the database clock and never negative, until the
/// newest active execution at or past `generation` is `lease_ttl_secs` past
/// its last update. `None` when there is no such execution.
pub async fn started_recovery_lease_remaining_millis(
    executor: impl PgExecutor<'_>,
    sandbox_id: SandboxId,
    generation: i64,
    lease_ttl_secs: i64,
) -> Result<Option<i64>, sqlx::Error> {
    sqlx::query_scalar::<_, i64>(sql!(
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
    .fetch_optional(executor)
    .await
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
    executor: impl PgExecutor<'_>,
    managed_by: SubstrateKind,
    limit: i64,
) -> Result<Vec<FailedActiveExecutionRecord>, sqlx::Error> {
    sqlx::query_as::<_, FailedActiveExecutionRecord>(sql!(
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
    .fetch_all(executor)
    .await
}

/// Re-read one failed-cleanup candidate's first activation just before an
/// alive retire, so an execution that reached `running` since the listing
/// (and the probe) moves it off the retire. Its two columns are
/// [`list_failed_active_executions`]'s, spelled the same way. `None`: the
/// sandbox is no longer a `failed` row managed by `managed_by` with an active
/// execution.
pub async fn first_activation(
    executor: impl PgExecutor<'_>,
    sandbox_id: SandboxId,
    managed_by: SubstrateKind,
) -> Result<Option<FirstActivationRecord>, sqlx::Error> {
    sqlx::query_as::<_, FirstActivationRecord>(sql!(
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
    .fetch_optional(executor)
    .await
}
