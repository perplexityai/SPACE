//! Production [`SweepSink`] backed by Postgres and the substrate.

use pplx_space_common_core::sql;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use pplx_space_api::controllers::substrate::{
    CommandReadyProbe, ExecutionRoutabilityCheck, NodeApiTarget, ObservedPodIdentity,
    PodFenceMismatch, PodIdentityFence, RecordedExecution, RetireExecutionOutcome,
    RetireExecutionRequest, RetireFence, RuntimeClient, Substrate, SubstrateAdapter,
    SubstrateError, connect_node_api_client_for_target, sandbox_states_with_client,
};
use pplx_space_common_core::types::{
    NodeManagedBy, NodeRole, PodName, PodUid, PositiveDuration, SandboxActivationAttemptId,
    SandboxId, SandboxLifecycle, SubstrateKind,
};
use pplx_space_common_svc::sql::sandbox_routability::{
    routable_execution_or_non_k8s, routable_k8s_execution_exists,
};
use pplx_space_metrics::cluster_reconciler::{HydrationOutcome, OutcomeEvent};
use pplx_space_sandbox_portability::{
    fresh_ready_snapshot_exists, inflight_suspend_upload_exists,
    recoverable_suspend_snapshot_exists, restore_retry_source_snapshot_exists,
    restore_retry_suspended_revert_snapshot_exists,
};
use sqlx::PgPool;

use super::classify::{ExecutionRoutability, ProbeObservation};
use super::probe::{ProbeNodeError, SweepCandidate};
use super::restoring_episode::{
    EpisodeDeadlineBinds, LIST_EPISODE_DEADLINE, REVERT_EPISODE_DEADLINE, restoring_episode_overdue,
};
use super::{
    CommandReadyPendingCandidate, FailGuard, FailedCleanupCandidate, FirstActivation,
    RevertedToSuspended, SweepSink, is_activation_owned,
};
use pplx_space_api::controllers::substrate::AbandonedRestoreRetirement;

/// Max preserved `command_ready_pending` rows resolved per tick. Each costs one
/// non-recovering guest probe (bounded by the configured probe timeout), so the
/// cap keeps a node-mgr-wide preserve backlog from fanning out an unbounded
/// number of RPCs in a single tick — the remainder rolls to the next tick. The
/// steady-state candidate count is tiny (only slow-create preserves no client
/// ever retried), so this rarely binds.
const COMMAND_READY_PENDING_BATCH: i64 = 50;

/// Candidate query for the command-ready-pending reaper. Bind order: `$1` =
/// `managed_by`, `$2` = age threshold seconds (`BIGINT`), `$3` = batch limit.
/// `lifecycle_changed_at` is the age clock because the create preserve path
/// sets the marker in the same write that flips the row to `started`.
///
/// Ordered by `random()`, not oldest-first: a probe outcome of `Unreachable`
/// leaves the row unchanged, so a strict oldest-first `LIMIT` would re-select
/// the same front rows every tick and let a batch of stuck/unreachable rows
/// permanently starve the ready/wedged candidates behind them. Random sampling
/// gives every candidate a fair shot each tick — healed/reaped rows leave the
/// set, and no fixed front-of-queue can monopolize the batch. The matching set
/// (aged, never-retried preserved creates) is small, so the random sort is
/// cheap.
pub(super) const LIST_COMMAND_READY_PENDING_SQL: &str = sql!(
    "SELECT
         s.id,
         EXTRACT(EPOCH FROM (NOW() - s.lifecycle_changed_at))::BIGINT AS age_secs,
         e.pod_name,
         e.activation_attempt_id,
         NULLIF(BTRIM(e.pod_uid), '') AS pod_uid,
         e.route_activation_id,
         e.generation
     FROM sandboxes s
     LEFT JOIN sandbox_k8s_executions e
       ON e.sandbox_id = s.id AND e.active
     WHERE s.managed_by = $1
       AND s.lifecycle = 'started'
       AND s.command_ready_pending = true
       AND s.lifecycle_changed_at < NOW() - make_interval(secs => $2)
     ORDER BY random()
     LIMIT $3"
);

/// Heal query: clear the marker on a probe-confirmed-ready row. Bind order:
/// `$1` = sandbox id, `$2` = `managed_by`. Lifecycle is left untouched.
pub(super) const HEAL_COMMAND_READY_PENDING_SQL: &str = sql!(
    "UPDATE sandboxes
     SET command_ready_pending = false
     WHERE id = $1
       AND managed_by = $2
       AND command_ready_pending = true"
);

/// Failed-cleanup candidates: `failed` rows that still carry an active
/// execution, oldest attempt first. Bind order: `$1` =
/// [`SubstrateKind::Kubernetes`] (see [`failed_active_execution_scope`]),
/// `$2` = batch limit.
///
/// `first_activation_unfinished` says the execution's first activation, for
/// a cold create the create itself, never finished: the execution never
/// reached `running` (still `creating`, or `created` once its pod UID was
/// written; the wake dial can record a pod IP without moving it on), and
/// it still holds an activation attempt, the route's or a later one.
/// `first_activation_abandoned` adds that the attempt's lease has expired. It
/// is the same predicate the repair CAS refuses, without the expiry. A finished
/// activation leaves the execution `running`, and a started recovery
/// `recovering`, even when the best-effort release of its attempt was missed.
/// Like the sweep's `creating` owner check, it reads whichever attempt the
/// execution holds. Unlike it, it needs no full age window past the expiry:
/// that window spares a create that may still finish, and this row is already
/// `failed`, so its create can't. An owner that finds its row failed rolls the
/// create back and deletes the sandbox.
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
/// doesn't complete, the repair below can put it back to `started`.
pub(super) const LIST_FAILED_ACTIVE_EXECUTIONS_SQL: &str = sql!(
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
);

/// Re-read one failed-cleanup candidate's first activation just before an
/// alive retire, so an execution that reached `running` since the listing
/// (and the probe) moves it off the retire. Its two columns are the listing's, spelled the same way.
/// Bind order: `$1` = sandbox id, `$2` = [`SubstrateKind::Kubernetes`] (see
/// [`failed_active_execution_scope`]). No row: the sandbox is no longer
/// `failed` with an active execution.
pub(super) const FIRST_ACTIVATION_SQL: &str = sql!(
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
);

/// Rotate a failed-cleanup candidate behind the others by bumping its active
/// execution's `updated_at`. Bind order: `$1` = sandbox id, `$2` =
/// [`SubstrateKind::Kubernetes`] (see [`failed_active_execution_scope`]).
pub(super) const NOTE_FAILED_CLEANUP_ATTEMPT_SQL: &str = sql!(
    "UPDATE sandbox_k8s_executions e
     SET updated_at = NOW()
     FROM sandboxes s
     WHERE e.sandbox_id = s.id
       AND s.id = $1
       AND s.managed_by = $2
       AND s.lifecycle = 'failed'
       AND e.active"
);

/// Repair only the observed stale-lifecycle split-brain: placement already
/// says the sandbox is on a node, and the active execution's node reported the
/// runtime as started. This path does not reconstruct placement metadata.
///
/// It never repairs a sandbox whose first activation never finished (the
/// execution never reached `running` and still holds an activation attempt,
/// or was given up by its last owner: no attempt, but a give-up expiry; see
/// [`LIST_FAILED_ACTIVE_EXECUTIONS_SQL`]). The sweep fails a create in that
/// state on purpose when it never became routable; putting it back to
/// `started` would hand out a sandbox its create request never returned, and
/// for any activation a `started` row with no pod IP to reach it at. This
/// refusal doesn't cover an execution with no lease at all, whatever its
/// state, so such a row is repaired like any other.
/// Bind order: `$1` = sandbox id, `$2` = [`SubstrateKind::Kubernetes`] (see
/// [`failed_active_execution_scope`]).
pub(super) const REPAIR_FAILED_ACTIVE_EXECUTION_SQL: &str = sql!(
    "UPDATE sandboxes s
     SET lifecycle = 'started'
   WHERE s.id = $1
     AND s.managed_by = $2
     AND s.lifecycle = 'failed'
     AND s.placement = 'on_node'
     AND EXISTS (
         SELECT 1
           FROM sandbox_k8s_executions e
          WHERE e.sandbox_id = s.id
            AND e.active
            AND NOT (
                e.state IN ('creating', 'created')
                AND (e.activation_attempt_id IS NOT NULL
                     OR e.activation_attempt_expires_at IS NOT NULL)
            )
     )
   RETURNING s.id"
);

/// Reap CAS: flip a wedged `started` + marker row to `failed`. Bind order:
/// `$1` = sandbox id, `$2` = `managed_by`. `RETURNING id` lets the caller tell
/// whether the CAS actually claimed the row (a concurrent heal / transition
/// drops it out of the predicate). The `sandboxes_lifecycle_changed_at` and
/// `sandboxes_clear_command_ready_pending` triggers fire on the resulting
/// `started` -> `failed` change, so no follow-up write is needed.
pub(super) const REAP_COMMAND_READY_PENDING_SQL: &str = sql!(
    "UPDATE sandboxes
     SET lifecycle = 'failed'
     WHERE id = $1
       AND managed_by = $2
       AND lifecycle = 'started'
       AND command_ready_pending = true
     RETURNING to_char(lifecycle_changed_at, 'YYYY-MM-DD HH24:MI:SS.US')"
);

/// The `managed_by` the failed-active-execution cleanup binds, or `None` when
/// the running substrate is not Kubernetes. That cleanup's statements scope to
/// Kubernetes-managed rows only, so on any other substrate they match nothing
/// and the caller skips them. An unparseable label is not Kubernetes.
pub(super) fn failed_active_execution_scope(managed_by: &str) -> Option<SubstrateKind> {
    match managed_by.parse::<SubstrateKind>() {
        Ok(SubstrateKind::Kubernetes) => Some(SubstrateKind::Kubernetes),
        Ok(SubstrateKind::Space) | Err(_) => None,
    }
}

/// SQL expression naming the lifecycle write a sweep just made.
///
/// A trigger bumps `lifecycle_changed_at` on every lifecycle change, so the
/// value a CAS returns identifies *that* write. A compensating CAS binds it,
/// which is what stops an undo from reaching past its own transition and
/// reverting a later, legitimate one that happens to have landed on the same
/// lifecycle. Formatted as text because this crate's sqlx has no timestamp
/// type enabled; microsecond precision is what the column holds.
const LIFECYCLE_WRITE_TOKEN: &str = "to_char(lifecycle_changed_at, 'YYYY-MM-DD HH24:MI:SS.US')";

/// Put a reverted row back, bound to the revert's own write.
///
/// Restores the `placement_changed_at` a retiring revert cleared (a clock
/// still set, by a lifecycle-only revert or a pod bind since, is kept), and
/// `preserve_local_artifact_on_restore_failure` alongside the lifecycle:
/// leaving `restoring` fired `sandboxes_clear_restore_lease`, which clears
/// it, and a fork restore that came back without its preservation
/// contract could discard the local artifact it was supposed to keep. The
/// lease itself is deliberately *not* restored — another actor may hold one by
/// now.
pub(crate) fn undo_revert_to_suspended_query() -> String {
    format!(
        "UPDATE sandboxes
            SET lifecycle = $3,
                preserve_local_artifact_on_restore_failure = $5,
                placement_changed_at = COALESCE(placement_changed_at, $6)
          WHERE id = $1
            AND managed_by = $2
            AND lifecycle = 'suspended'
            AND {LIFECYCLE_WRITE_TOKEN} = $4
          RETURNING id"
    )
}

/// Put a reaped row back, bound to the reap's own write. Restores the marker
/// explicitly: the trigger that cleared it on the way to `failed` does not put
/// it back, and without it the row leaves the reaper for good.
pub(crate) fn undo_reap_command_ready_pending_query() -> String {
    format!(
        "UPDATE sandboxes
            SET lifecycle = 'started',
                command_ready_pending = TRUE
          WHERE id = $1
            AND managed_by = $2
            AND lifecycle = 'failed'
            AND {LIFECYCLE_WRITE_TOKEN} = $3
          RETURNING id"
    )
}

/// Max aged candidates listed per lifecycle per tick. Each candidate costs at
/// most a share of one per-node probe RPC, so the cap bounds tick I/O when a
/// cluster-wide incident wedges a large population at once; the remainder
/// rolls to the next tick.
const SWEEP_CANDIDATE_BATCH: i64 = 100;

/// Shared evacuation-aware guards layered on top of the plain age predicate.
/// Used identically by [`list_aged_transient_query`] (the probe-first select)
/// and [`fail_candidate_query`] (the per-row fail CAS), so a row can never be
/// failed under a guard the select did not apply:
///
/// 1. `(evacuation_restore_target IS NULL OR lifecycle = 'suspending')` exempts
///    node-drain's in-flight evacuation **restore** — a marked `restoring` row
///    legitimately polls the rebuilt pod for up to the evacuate-timeout (the
///    same order of magnitude as this sweep's default age) and has its own
///    re-drive path (`restore_pending_evacuations_query` + `restore_in_flight`),
///    so the backstop must not race it. The clear-trigger only keeps the marker
///    on `suspending`/`suspended`/`restoring`, and `suspended` isn't sweepable,
///    so `restoring` is the only lifecycle this exemption actually spares. A
///    marked `suspending` row is deliberately *not* spared by this clause —
///    guard 2 handles the only `suspending` case worth sparing.
///
/// 2. The `suspending` arm carries one extra guard the other transient
///    lifecycles do not: an evacuation-marked row with recoverable published
///    state is owned by node-drain redrive. For new rows that is a persisted
///    `current_local_artifact_id`, even before backup enqueue; for nullable
///    rollout-skew rows it is a current-cycle `creating` or `ready` snapshot.
///    Aging either form to `failed` would clear the marker before redrive can
///    continue. A marked suspend with no recoverable state still ages out.
///
/// 3. Every activation-owned lifecycle ([`is_activation_owned`]) spares a
///    row while its owner may still be alive; see [`activation_owner_guard`].
///
/// `episode_deadline` names the binds of the restoring-episode deadline for a
/// `restoring` row's select, which waives its lease and owner guards once the
/// episode is overdue (see [`restoring_ownership_guards`]). The fail CAS
/// passes `None`: an overdue episode is reverted, never failed, under a live
/// owner.
fn evacuation_guards(
    lifecycle: SandboxLifecycle,
    episode_deadline: Option<EpisodeDeadlineBinds>,
) -> String {
    let recoverable_snapshot_guard = if lifecycle == SandboxLifecycle::Suspending {
        let recoverable = recoverable_suspend_snapshot_exists("sandboxes");
        format!(
            "\n                AND NOT (\n                    sandboxes.evacuation_restore_target IS NOT NULL\n                    AND {recoverable}\n                )"
        )
    } else {
        String::new()
    };
    let ownership_guards = if lifecycle == SandboxLifecycle::Restoring {
        restoring_ownership_guards("$3", episode_deadline)
    } else if is_activation_owned(lifecycle) {
        activation_owner_guard("$3")
    } else {
        String::new()
    };
    format!(
        "AND (sandboxes.evacuation_restore_target IS NULL OR sandboxes.lifecycle = 'suspending')
            AND sandboxes.lifecycle_changed_at < NOW() - make_interval(secs => $3::BIGINT){recoverable_snapshot_guard}{ownership_guards}"
    )
}

/// The guards that spare a `restoring` row while a restore may still own it:
/// its restore lease is live or ended less than a full age window ago, or its
/// wake's activation attempt is ([`activation_owner_guard`]). `age_param`
/// names the statement's age-threshold bind.
///
/// Legacy writers may refresh `lifecycle_changed_at` and clear the lease, so
/// the lifecycle-age guard remains authoritative for them. Newer writers
/// freeze that source boundary and retain expiry; requiring the same full
/// window after expiry means a renewal gap never makes an active retry
/// immediately reapable.
///
/// With `episode_deadline`, an overdue episode ([`restoring_episode_overdue`])
/// passes them anyway.
fn restoring_ownership_guards(
    age_param: &str,
    episode_deadline: Option<EpisodeDeadlineBinds>,
) -> String {
    let owned = restoring_unowned(age_param);
    match episode_deadline {
        None => format!("\n                AND {owned}"),
        Some(binds) => {
            let overdue = restoring_episode_overdue(binds);
            format!("\n                AND (({owned}) OR {overdue})")
        }
    }
}

/// Whether no restore may still own a `restoring` row: the condition
/// [`restoring_ownership_guards`] requires without an overdue episode.
fn restoring_unowned(age_param: &str) -> String {
    let owner_guard = activation_owner_guard(age_param);
    format!(
        "(sandboxes.restore_lease_expires_at IS NULL
                     OR sandboxes.restore_lease_expires_at <
                        NOW() - make_interval(secs => {age_param}::BIGINT)){owner_guard}"
    )
}

/// Spare a row whose active execution's activation-attempt lease is live, or
/// ended less than a full age window ago. `age_param` names the statement's
/// age-threshold bind.
///
/// That lease is the owner of every activation-owned lifecycle
/// ([`is_activation_owned`]): the create, start, resume, wake or restore
/// driving the row, or a start running on a `pausing` or `checkpointing`
/// row. Its TTL outlives any single API-to-node call but not
/// necessarily a whole activation, which makes several in a row, so expiry
/// alone doesn't prove the request is gone; a full age window after it does,
/// the way the restore lease is treated. An owner that finds its row already
/// failed rolls the create back and deletes the sandbox, and a wake that finds
/// it converged or reverted has lost its own transition. A lease with no
/// expiry counts as live, so an unexpected shape spares the row rather than
/// exposing it.
///
/// The expiry is read whether or not an attempt still holds it. An owner that
/// gives up an activation it never dispatched clears its attempt but cuts the
/// expiry to the time it gave up (`give_up_activation_attempt_best_effort`),
/// and the row it leaves behind still waits out the window from then. An
/// execution with neither an attempt nor an expiry (one whose activation
/// finished, or that never took an attempt) has nothing to wait for.
///
/// Every CAS that applies this takes the sandbox row lock first (see
/// [`LOCK_SANDBOX_ROW_SQL`]); the candidate select only lists, so it needn't.
fn activation_owner_guard(age_param: &str) -> String {
    format!(
        "
                AND NOT EXISTS (
                    SELECT 1
                      FROM sandbox_k8s_executions owner
                     WHERE owner.sandbox_id = sandboxes.id
                       AND owner.active
                       AND (owner.activation_attempt_expires_at >=
                               NOW() - make_interval(secs => {age_param}::BIGINT)
                            OR (owner.activation_attempt_id IS NOT NULL
                                AND owner.activation_attempt_expires_at IS NULL))
                )"
    )
}

/// Raw row shape of the failed-cleanup listing.
#[derive(sqlx::FromRow)]
struct FailedCleanupRow {
    id: SandboxId,
    k8s_node_name: Option<String>,
    pod_name: String,
    activation_attempt_id: Option<SandboxActivationAttemptId>,
    generation: i64,
    pod_uid: Option<PodUid>,
    route_activation_id: Option<SandboxActivationAttemptId>,
    first_activation_unfinished: bool,
    first_activation_abandoned: bool,
}

/// Raw row shape of [`LIST_COMMAND_READY_PENDING_SQL`].
#[derive(sqlx::FromRow)]
struct CommandReadyPendingRow {
    id: SandboxId,
    age_secs: i64,
    pod_name: Option<String>,
    activation_attempt_id: Option<SandboxActivationAttemptId>,
    generation: Option<i64>,
    pod_uid: Option<PodUid>,
    route_activation_id: Option<SandboxActivationAttemptId>,
}

/// Raw row shape of [`list_aged_transient_query`]. Fields map to the
/// select's output columns by name (`sqlx::FromRow`), so reordering the
/// SELECT list cannot silently shift values into the wrong slots.
#[derive(sqlx::FromRow)]
pub(crate) struct AgedTransientRow {
    pub(crate) id: SandboxId,
    pub(crate) age_secs: i64,
    pub(crate) evacuation_restore_target: Option<String>,
    pub(crate) ready_snapshot: bool,
    pub(crate) fork_restore: bool,
    pub(crate) k8s_node_name: Option<String>,
    pub(crate) routable_execution: bool,
    pub(crate) activation_abandoned: bool,
    pub(crate) episode_overdue: bool,
    pub(crate) pod_name: Option<String>,
    pub(crate) activation_attempt_id: Option<SandboxActivationAttemptId>,
    pub(crate) generation: Option<i64>,
    pub(crate) pod_uid: Option<PodUid>,
    pub(crate) route_activation_id: Option<SandboxActivationAttemptId>,
}

/// Build the retire fence for a candidate from its active execution row's
/// `(pod_name, pod_uid)`.
///
/// Every candidate gets a fence — this never yields the unfenced teardown,
/// which is the most destructive path there is (it deletes every tracked pod
/// name and releases tracking unconditionally). Three shapes, weakest last:
///
/// * **No execution row** — the row had none when it was selected, and
///   [`RetireFence::NoExecution`] makes the substrate re-prove that before
///   acting. Reading "nothing to protect" out of it would be a fail-open: a
///   row with no execution is exactly the one a concurrent create or wake is
///   most likely to give one.
/// * **A pod name and a UID** — the full fence.
/// * **A pod name alone** — `pod_uid` is nullable because a pod's UID is
///   persisted after the pod is created, so a row wedged before that write has
///   a real pod and no recorded identity. Supersession still applies, and the
///   substrate refuses to touch a live pod it cannot identify.
///
/// A parse failure is schema drift, not a state: it resolves to
/// [`RetireFence::NoExecution`], which fails closed, because an execution row
/// does exist and the substrate will refuse on that basis.
/// The retire fence for a candidate, built from its active execution row and
/// upgraded with what the substrate currently sees.
///
/// The persisted UID cannot tell a pod that *moved*: a Pending pod becoming
/// Running keeps its UID, and the sweep's evidence that the sandbox has no
/// live runtime is a node probe, which a pod coming up after it invalidates.
/// A resourceVersion observed here, before the probe and the CAS, is what the
/// substrate re-checks to notice.
///
/// The revert to `suspended` re-checks it once, before its CAS
/// (`retire_fence_mismatch`). With abandoned-restore retirement disabled (the
/// default), the cleanup then retires the still-active execution under this
/// fence and refuses a pod that came up since, and the revert is undone.
/// Enabled, the revert retires the execution in the CAS's own transaction,
/// and a restore pod that binds after that check is the abandoned
/// restore's leftover: the restore's lease has expired, its row aged past the
/// sweep's window, and the revert's snapshot proof says restore selection
/// picks a durable pre-restore source. So the cleanup deletes it, the row
/// stays `suspended`, and the next wake restores from that source. The row
/// never reached `started`, and no user request reached its VM: the API's
/// access, runtime-client, ingress and data-plane route paths never dial a
/// `restoring` sandbox's pod, so nothing it holds is newer than the source.
/// That holds only once every routing replica runs those paths, which is why
/// retirement stays disabled until then (see `AbandonedRestoreRetirement`).
///
/// The observation may only *confirm* the row's persisted UID, never replace
/// it. The row is read by one query and the pods by a later one, so a row that
/// is re-claimed in between — which clears its UID and keeps its generation,
/// pod name and activation attempt — would otherwise produce a fence built
/// from the old row's facts and the replacement's pod, a combination that
/// matches the new state exactly and authorizes tearing it down.
pub(crate) fn candidate_pod_fence_with_observation(
    sandbox_id: SandboxId,
    pod_name: Option<String>,
    activation_attempt_id: Option<SandboxActivationAttemptId>,
    generation: Option<i64>,
    recorded_pod_uid: Option<PodUid>,
    route_activation_id: Option<SandboxActivationAttemptId>,
    observed: &HashMap<PodName, ObservedPodIdentity>,
) -> RetireFence {
    let Some(pod_name) = pod_name else {
        return RetireFence::NoExecution;
    };
    let pod_name = match PodName::parse(&pod_name) {
        Ok(pod_name) => pod_name,
        Err(error) => {
            tracing::warn!(
                sandbox_id = %sandbox_id,
                %error,
                "transient_lifecycle_sweeper: unparseable pod name on the active execution \
                 row; the retire will fail closed against the execution that does exist"
            );
            return RetireFence::NoExecution;
        }
    };
    let with_identity = |fence: PodIdentityFence| {
        // Every shape of fence carries what the row said about itself at
        // select time. It is the only part of the identity that can notice the
        // row being re-claimed when no pod is left to compare, and the fence
        // is at its weakest exactly there.
        let fence = fence.with_recorded(RecordedExecution::Read {
            pod_uid: recorded_pod_uid.clone(),
            route_activation_id,
        });
        let fence = match activation_attempt_id {
            Some(attempt) => fence.with_activation_attempt(attempt),
            None => fence,
        };
        match generation {
            Some(generation) => fence.with_generation(generation),
            None => fence,
        }
    };
    match (recorded_pod_uid.as_ref(), observed.get(&pod_name)) {
        // The row and the live pod agree on which object this is, so the
        // observation is provably of the execution that was selected.
        (Some(recorded), Some(identity)) if recorded == &identity.pod_uid => {
            let mut fence = PodIdentityFence::new(pod_name).with_pod_uid(identity.pod_uid.clone());
            if let Some(resource_version) = identity.resource_version.clone() {
                fence = fence.with_resource_version(resource_version);
            }
            RetireFence::Pod(with_identity(fence))
        }
        // A pod exists under this name that the selected row does not claim —
        // either the row was re-claimed since the select, or its UID was never
        // written. Either way nothing here can prove the two describe the same
        // object, so fall through to the name-only fence, which refuses to act
        // while any pod is there. A row whose UID is simply late gets the full
        // fence on the tick after it lands.
        _ => {
            // A name-only fence would let a retire proceed against a live pod
            // it cannot prove is still the one the probe judged, because a
            // Pending pod becoming Running keeps its UID. It says exactly
            // that — act only on an absent pod.
            RetireFence::Pod(with_identity(PodIdentityFence::new(pod_name)))
        }
    }
}

/// Snapshot the substrate's current view of every pod named in the candidate
/// rows. Reflector-backed, so this is a map lookup per candidate rather than a
/// round trip; a pod the substrate cannot see is simply absent and its
/// candidate falls back to the persisted identity.
async fn observe_candidate_pods(
    substrate: &Substrate,
    pod_names: impl Iterator<Item = &str>,
) -> HashMap<PodName, ObservedPodIdentity> {
    let names: Vec<PodName> = pod_names
        .filter_map(|name| PodName::parse(name).ok())
        .collect();
    if names.is_empty() {
        return HashMap::new();
    }
    substrate.observe_pod_identities(&names).await
}

/// Snapshot fact for the candidate select and the revert CAS, chosen per
/// lifecycle to match `sandbox_portability`'s guidance:
///
/// * `restoring` — the same pre-restore source window used by restore retry
///   selection. A failed restore branch can publish a later artifact; the
///   sweeper must treat the pre-restore source as recoverable without treating
///   that late branch as a safe suspended restore source.
/// * every other lifecycle — placement-freshness
///   (`fresh_ready_snapshot_exists`). The fact is diagnostic only there: the
///   decision table never reverts a non-`restoring` row, because a backup
///   matched by the never-cleared-on-wake artifact pointer is the previous
///   suspend cycle's bytes (see `classify::decide`).
fn ready_snapshot_predicate(lifecycle: SandboxLifecycle) -> String {
    if lifecycle == SandboxLifecycle::Restoring {
        restore_retry_source_snapshot_exists("sandboxes")
    } else {
        fresh_ready_snapshot_exists("sandboxes")
    }
}

/// Build the candidate select for `lifecycle`: aged rows listed **without**
/// flipping anything. Bind order: `$1` = `managed_by`, `$2` = lifecycle
/// string, `$3` = age threshold seconds (`BIGINT`), `$4` = batch limit, `$5` =
/// `SubstrateKind::Kubernetes` (the routability fragment's substrate), `$6` =
/// the restoring-episode deadline in seconds (`BIGINT`, NULL when off).
///
/// A `restoring` row whose episode is overdue ([`restoring_episode_overdue`])
/// is listed whatever its restore lease and activation attempt say. It
/// reports `episode_overdue` when those would have spared it.
///
/// Carries everything classification needs: the row's actual age, the
/// evacuation wake target, the per-lifecycle ready-snapshot fact
/// ([`ready_snapshot_predicate`] — both branches keep the artifact-identity
/// guard for rows that carry `current_local_artifact_id`, so only an
/// exact-artifact backup counts), and the node from the row's **active
/// execution** (`sandbox_k8s_executions`), not `sandboxes.active_node_id`,
/// which is NULL for this population.
///
/// It also carries that execution's `(pod_name, pod_uid)`, which becomes the
/// retire's pod-identity fence. Capturing it here — before the probe, before
/// the act-phase CAS — is what makes it evidence: a fence read at retire time
/// would describe whatever replaced the pod, which is precisely the thing the
/// fence has to refuse.
///
/// Ordered by `random()`, not oldest-first: deferred/inconclusive candidates
/// stay in the set across ticks, so a strict oldest-first `LIMIT` would let a
/// batch of unreachable rows permanently starve the resolvable ones behind
/// them. Same rationale as the command-ready-pending list.
///
/// Extracted as a builder so the production query string is the exact one
/// exercised by the Postgres-backed tests, with no drift between them.
pub(crate) fn list_aged_transient_query(lifecycle: SandboxLifecycle) -> String {
    let guards = evacuation_guards(lifecycle, Some(LIST_EPISODE_DEADLINE));
    // Reported only when the deadline is what let the row through, so the
    // outcome it labels is a revert that overrode a live owner.
    let episode_overdue = if lifecycle == SandboxLifecycle::Restoring {
        let overdue = restoring_episode_overdue(LIST_EPISODE_DEADLINE);
        let unowned = restoring_unowned("$3");
        format!("({overdue} AND NOT ({unowned}))")
    } else {
        // Referenced either way, so every lifecycle's select takes the same
        // binds.
        "($6::BIGINT IS NULL AND FALSE)".to_owned()
    };
    let ready_snapshot = ready_snapshot_predicate(lifecycle);
    // The same routability contract the node published-state handler gates
    // its own `restoring -> started` convergence on: `started` in the K8s
    // substrate promises an active running execution with a pod IP, so the
    // converge arm needs this fact to avoid asserting it blindly.
    let routable_execution = routable_execution_or_non_k8s("sandboxes", "$5");
    format!(
        "SELECT sandboxes.id,
                EXTRACT(EPOCH FROM (NOW() - sandboxes.lifecycle_changed_at))::BIGINT AS age_secs,
                sandboxes.evacuation_restore_target,
                ({ready_snapshot}) AS ready_snapshot,
                sandboxes.preserve_local_artifact_on_restore_failure AS fork_restore,
                e.k8s_node_name,
                e.pod_name,
                e.activation_attempt_id,
                NULLIF(BTRIM(e.pod_uid), '') AS pod_uid,
                e.route_activation_id,
                e.generation,
                ({routable_execution}) AS routable_execution,
                COALESCE(
                    e.activation_attempt_expires_at <
                        NOW() - make_interval(secs => $3::BIGINT),
                    FALSE
                ) AS activation_abandoned,
                {episode_overdue} AS episode_overdue
           FROM sandboxes
           LEFT JOIN sandbox_k8s_executions e
             ON e.sandbox_id = sandboxes.id AND e.active
          WHERE sandboxes.managed_by = $1
            AND sandboxes.lifecycle = $2
            {guards}
          ORDER BY random()
          LIMIT $4"
    )
}

/// Build the per-row fail CAS for `lifecycle`. Bind order: `$1` =
/// `managed_by`, `$2` = lifecycle string, `$3` = age threshold seconds
/// (`BIGINT`), `$4` = sandbox id. `RETURNING id` reports whether this call
/// claimed the row; the predicate repeats every guard the candidate select
/// applied (see [`evacuation_guards`]) plus the age window, so a row that
/// exited the transient lifecycle between select and act is left alone.
///
/// The shared guards require a full age window after the last restore lease
/// expiry. The lifecycle clock protects legacy NULL-lease writers; retained
/// expiry is the independent activity clock for newer writers.
///
/// [`FailGuard::NoPendingReceipt`] also requires that the row's active
/// execution isn't both routable (`running` with a pod IP) and holding an
/// activation attempt, the state in which a completion receipt can still move
/// it to `started`.
pub(crate) fn fail_candidate_query(lifecycle: SandboxLifecycle, guard: FailGuard) -> String {
    let guards = evacuation_guards(lifecycle, None);
    let routable_guard = match guard {
        FailGuard::Aged => String::new(),
        FailGuard::NoPendingReceipt => format!(
            "\n            AND NOT ({} AND EXISTS (\
             \n                SELECT 1\
             \n                  FROM sandbox_k8s_executions held\
             \n                 WHERE held.sandbox_id = sandboxes.id\
             \n                   AND held.active\
             \n                   AND held.activation_attempt_id IS NOT NULL\
             \n            ))",
            routable_k8s_execution_exists("sandboxes")
        ),
    };
    format!(
        "UPDATE sandboxes
            SET lifecycle = 'failed'
          WHERE managed_by = $1
            AND lifecycle = $2
            {guards}{routable_guard}
            AND id = $4
          RETURNING {LIFECYCLE_WRITE_TOKEN}"
    )
}

/// Lock an activation-owned row before a CAS re-checks its owner. Bind order:
/// `$1` = sandbox id.
///
/// The fail, converge and revert CASes check the owner with a subquery on the
/// execution ([`activation_owner_guard`]), and every activation-attempt
/// assignment (`assign_activation_attempt_in`'s callers) locks the sandbox row
/// without changing it before it writes a new lease there. An `UPDATE` that
/// waits on that lock goes ahead on its original snapshot once the lock is
/// released, so it would act on a row a new owner had just claimed. Taking the
/// lock first, in the CAS's own transaction, makes the CAS read a snapshot
/// that includes any such claim.
pub(crate) const LOCK_SANDBOX_ROW_SQL: &str =
    sql!("SELECT id FROM sandboxes WHERE id = $1 FOR UPDATE");

/// Begin a transaction holding the sandbox row lock, for a CAS whose owner
/// check must see any activation attempt claimed while it waited for the row
/// (see [`LOCK_SANDBOX_ROW_SQL`]).
async fn begin_locked(
    pool: &PgPool,
    sandbox_id: SandboxId,
) -> Result<sqlx::Transaction<'static, sqlx::Postgres>, sqlx::Error> {
    let mut tx = pool.begin().await?;
    sqlx::query(LOCK_SANDBOX_ROW_SQL)
        .bind(sandbox_id)
        .execute(&mut *tx)
        .await?;
    Ok(tx)
}

/// Put an aged-out row back, bound to the fail CAS's own write.
pub(crate) fn undo_fail_aged_candidate_query() -> String {
    format!(
        "UPDATE sandboxes
            SET lifecycle = $3
          WHERE id = $1
            AND managed_by = $2
            AND lifecycle = 'failed'
            AND {LIFECYCLE_WRITE_TOKEN} = $4
          RETURNING id"
    )
}

/// Lease-guarded converge CAS: flip an aged, probe-verified-alive `restoring`
/// row to its wake target. Bind order: `$1` = `managed_by`, `$2` = age
/// threshold seconds (`BIGINT`), `$3` = sandbox id, `$4` = target lifecycle
/// (`'started'` or `'paused'`), `$5` = `SubstrateKind::Kubernetes` (the
/// routability fragment's substrate).
///
/// The restore-lease guard mirrors the user-restore claim's semantics. An
/// unexpired lease means a restore attempt is still actively driving the row.
/// A recently expired lease may only be a renewal gap, so the row must remain
/// quiet for the complete age threshold before convergence. The
/// `sandboxes_clear_restore_lease` and
/// `sandboxes_clear_evacuation_restore_target` triggers clear the lease and
/// marker on the resulting transition out of `restoring`.
///
/// The routability predicate is re-checked **inside** the CAS, not only in
/// the act phase, because the act-phase proof (candidate-select fact or
/// kube-verified hydration) is stale the moment a concurrent wake replaces
/// the active execution — `replace_active_execution_claim_if_generation`
/// retires the routable row and inserts a fresh `creating` generation with no
/// `pod_ip` while the sandbox row is still `restoring` (reachable once the
/// restore lease has expired, since the lease TTL is far below the sweep age
/// threshold). The wake target is a contract that a routable execution exists
/// *at flip time*; if the execution changed underneath, the CAS claims
/// nothing and the in-flight wake keeps ownership of the row.
///
/// It also repeats the select's [`activation_owner_guard`]. Materialization
/// sets the restore lease and the wake's activation attempt to the same
/// ownership window, so the two usually end together, but the restore-lease
/// clause above lets a row whose restore lease was cleared through, and the
/// attempt is still the wake's. Converging under a live wake would make its
/// own `restoring -> started` CAS lose.
pub(crate) fn converge_restoring_query() -> String {
    let routable_execution = routable_execution_or_non_k8s("sandboxes", "$5");
    let owner_guard = activation_owner_guard("$2");
    format!(
        "UPDATE sandboxes
        SET lifecycle = $4
      WHERE managed_by = $1
        AND lifecycle = 'restoring'
        AND lifecycle_changed_at < NOW() - make_interval(secs => $2::BIGINT)
        AND (restore_lease_expires_at IS NULL
             OR restore_lease_expires_at < NOW() - make_interval(secs => $2::BIGINT))
        AND {routable_execution}{owner_guard}
        AND id = $3
      RETURNING id"
    )
}

/// Build the revert-to-suspended CAS. Bind order: `$1` = `managed_by`, `$2` =
/// lifecycle string, `$3` = age threshold seconds (`BIGINT`), `$4` = sandbox
/// id, `$5` = the restoring-episode deadline in seconds (`BIGINT`, NULL when
/// off), `$6` = `SubstrateKind::Kubernetes`.
///
/// Re-checks the snapshot predicate inside the CAS (not just at select time)
/// because `suspended` is a contract: a restore path trusts that a suspended
/// row has a restorable snapshot behind it. If the snapshot disappeared
/// between select and act (retention sweep, storage delete) the CAS claims
/// nothing and the row defers to the next tick.
///
/// Three guards beyond the select's, so classification drift can never widen
/// revert eligibility at the SQL level:
///
/// * `lifecycle = 'restoring'` — hard pin (in addition to the `$2` bind).
///   Only `restoring` rows have a provably-current snapshot; for every other
///   transient lifecycle the artifact pointer (never cleared on wake) matches
///   the *previous* cycle's backup, and reverting would silently restore
///   pre-wake bytes (see `classify::decide`).
/// * the restore-lease guard, mirroring the converge CAS: an unexpired or
///   recently expired lease means a restore attempt may still own the row;
///   reclaim waits one complete age window after expiry.
/// * the suspended-revert-safe restore snapshot predicate, so a late artifact
///   published by a failed restore branch can't become the snapshot a later
///   suspended restore selects.
///
/// It also clears `placement_changed_at` (see
/// [`SweepSink::revert_to_suspended`]), and repeats the select's
/// [`activation_owner_guard`], for the same reason as
/// [`converge_restoring_query`]: a row whose restore lease was cleared may
/// still have a live wake's activation attempt. An overdue episode
/// ([`restoring_episode_overdue`]) waives the lease and owner guards, as in the
/// select; the converge CAS never does, since only a restore that reached a
/// pod IP can converge.
pub(crate) fn revert_to_suspended_query() -> String {
    let ready_snapshot = restore_retry_suspended_revert_snapshot_exists("sandboxes");
    let ownership_guards = restoring_ownership_guards("$3", Some(REVERT_EPISODE_DEADLINE));
    format!(
        "UPDATE sandboxes
            SET lifecycle = 'suspended',
                placement_changed_at = NULL
          WHERE managed_by = $1
            AND lifecycle = $2
            AND lifecycle = 'restoring'
            AND lifecycle_changed_at < NOW() - make_interval(secs => $3::BIGINT)
            AND ({ready_snapshot}){ownership_guards}
            AND id = $4
          RETURNING {LIFECYCLE_WRITE_TOKEN}"
    )
}

/// [`revert_to_suspended_query`] with abandoned-restore retirement disabled:
/// the same guards, moving only the lifecycle. The execution, placement and
/// placement clock stay, and the cleanup that follows retires the execution
/// under the candidate's fence, refusing a pod that came up since.
pub(crate) fn revert_to_suspended_lifecycle_only_query() -> String {
    let ready_snapshot = restore_retry_suspended_revert_snapshot_exists("sandboxes");
    let ownership_guards = restoring_ownership_guards("$3", Some(REVERT_EPISODE_DEADLINE));
    format!(
        "UPDATE sandboxes
            SET lifecycle = 'suspended'
          WHERE managed_by = $1
            AND lifecycle = $2
            AND lifecycle = 'restoring'
            AND lifecycle_changed_at < NOW() - make_interval(secs => $3::BIGINT)
            AND ({ready_snapshot}){ownership_guards}
            AND id = $4
          RETURNING {LIFECYCLE_WRITE_TOKEN}"
    )
}

/// Retire the reverted restore's execution as `replaced` inside the revert
/// CAS's transaction, matching the identity the candidate select recorded
/// (see [`SweepSink::revert_to_suspended`]). A crash, error or refusal
/// between a revert and a separate retire would leave a `suspended` row with
/// its same-generation execution still active, which nothing revisits.
///
/// Returns `false`, and the caller rolls the revert back, when the active
/// execution is no longer the one the candidate observed.
async fn retire_reverted_restore_execution(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    sandbox_id: SandboxId,
    fence: &PodIdentityFence,
) -> Result<bool, sqlx::Error> {
    if fence.recorded == RecordedExecution::Unread {
        return Ok(false);
    }
    let retired = sqlx::query(sql!(
        "UPDATE sandbox_k8s_executions
            SET active = FALSE,
                state = 'replaced',
                pod_ip = NULL,
                terminated_at = COALESCE(terminated_at, NOW()),
                updated_at = NOW()
          WHERE sandbox_id = $1
            AND active
            AND pod_name = $2
            AND generation IS NOT DISTINCT FROM $3
            AND activation_attempt_id IS NOT DISTINCT FROM $4
            AND NULLIF(BTRIM(pod_uid), '') IS NOT DISTINCT FROM $5::TEXT
            AND route_activation_id IS NOT DISTINCT FROM $6"
    ))
    .bind(sandbox_id)
    .bind(fence.pod_name.as_str())
    .bind(fence.generation)
    .bind(fence.activation_attempt_id)
    .bind(fence.recorded.pod_uid().map(PodUid::as_str))
    .bind(fence.recorded.route_activation_id())
    .execute(&mut **tx)
    .await?
    .rows_affected()
        == 1;
    if retired {
        // The retired execution no longer places the row. The cleanup that
        // follows unplaces it too, but it is best-effort, and a `suspended`
        // row left reading as placed is one nothing revisits.
        sqlx::query(sql!(
            "UPDATE sandboxes
                SET placement = 'unplaced',
                    active_node_id = NULL
              WHERE id = $1"
        ))
        .bind(sandbox_id)
        .execute(&mut **tx)
        .await?;
        // The node state the open exposures described died with the runtime.
        sqlx::query(sql!(
            "UPDATE sandbox_port_exposures
                SET apply_state = 'pending',
                    last_applied_node_id = NULL,
                    last_error = NULL,
                    last_bridge_status = NULL,
                    last_applied_bridge_generation = NULL,
                    last_observed_at = NULL,
                    updated_at = NOW()
              WHERE sandbox_id = $1
                AND desired_state = 'open'"
        ))
        .bind(sandbox_id)
        .execute(&mut **tx)
        .await?;
    }
    Ok(retired)
}

/// Shared predicate for the unrestorable-`restoring` sweep: a markerless
/// `restoring` row that the restore planner **provably cannot restore from
/// anywhere** — the exact wedge the aged-transient sweep above cannot reach,
/// because every doomed wake's restore-lease claim holds an active lease, so
/// the fail CAS defers while the attempt is owned. This query diagnoses the
/// missing-source shape directly instead of waiting for ordinary age-based
/// recovery after the lease expires.
///
/// The predicate mirrors `suspend_restore_plan`'s source resolution, clause
/// for clause, so a row matches iff the planner's next attempt is guaranteed
/// to fail:
///
/// * unplaced (`active_node_id`/`target_node_id` NULL) and **no execution row
///   carries a node name** — `restore_source_node_name`'s fallback scans all
///   of `sandbox_k8s_executions` (not just the active row) for a non-NULL
///   `k8s_node_name`, so the sweep only matches when that scan comes up
///   empty too;
/// * no **active** execution — a restore attempt that already created its
///   claim/pod is in flight and owns the row, even before a node is assigned;
/// * no snapshot in the restore-retry source window
///   (`restore_retry_source_snapshot_exists`, the planner's own selector);
/// * no in-flight suspend upload (`inflight_suspend_upload_exists`) — the
///   race guard `mark_sandbox_lost` uses: a `creating` snapshot may finalize
///   `ready` moments later and make the row restorable;
/// * evacuation-markerless — node-drain re-drives marked rows.
///
/// **Age clock: `placement_changed_at`, never `lifecycle_changed_at`.** Legacy
/// retries may already have reset `lifecycle_changed_at`, while
/// `placement_changed_at` was last written by the suspend that unplaced the
/// row. New restore preflight avoids claiming a source-less attempt, but this
/// phase still has to clean up rows created before that contract or by a
/// source disappearing between checks. The window only needs to outlast
/// suspend→backup-enqueue latency (seconds) — a freshly suspended row is
/// additionally shielded by the execution-history and inflight-upload clauses
/// above. `placement_changed_at IS NOT NULL` is required: a
/// `restoring` row has necessarily been placed before, so a NULL there is
/// schema drift the sweep defers on rather than acts on.
///
/// **Deliberately no restore-lease guard** (unlike every other CAS in this
/// module): legacy wedged rows can carry a lease even though no restore source
/// exists, so a lease guard would defer them forever. Omitting it is safe
/// *because of* the source clauses:
/// a lease-holding attempt that could actually make progress would have an
/// active execution or an eligible source, both of which drop the row out of
/// this predicate atomically in the fail CAS.
fn unrestorable_restoring_guards() -> String {
    let source_snapshot = restore_retry_source_snapshot_exists("sandboxes");
    let inflight_upload = inflight_suspend_upload_exists("sandboxes");
    format!(
        "AND sandboxes.lifecycle = 'restoring'
            AND sandboxes.evacuation_restore_target IS NULL
            AND sandboxes.active_node_id IS NULL
            AND sandboxes.target_node_id IS NULL
            AND NOT EXISTS (
                SELECT 1
                  FROM sandbox_k8s_executions e
                 WHERE e.sandbox_id = sandboxes.id
                   AND e.active
            )
            AND NOT EXISTS (
                SELECT 1
                  FROM sandbox_k8s_executions ske
                 WHERE ske.sandbox_id = sandboxes.id
                   AND ske.k8s_node_name IS NOT NULL
            )
            AND NOT {source_snapshot}
            AND NOT {inflight_upload}
            AND sandboxes.placement_changed_at IS NOT NULL
            AND sandboxes.placement_changed_at < NOW() - make_interval(secs => $2::BIGINT)"
    )
}

/// Candidate select for the unrestorable-`restoring` sweep. Bind order:
/// `$1` = `managed_by`, `$2` = min placement age seconds (`BIGINT`), `$3` =
/// batch limit. Lists without flipping; the reported age is time since the
/// last placement change (the stable clock — see
/// [`unrestorable_restoring_guards`]).
pub(crate) fn list_unrestorable_restoring_query() -> String {
    let guards = unrestorable_restoring_guards();
    format!(
        "SELECT sandboxes.id,
                EXTRACT(EPOCH FROM (NOW() - sandboxes.placement_changed_at))::BIGINT AS age_secs
           FROM sandboxes
          WHERE sandboxes.managed_by = $1
            {guards}
          ORDER BY random()
          LIMIT $3"
    )
}

/// Per-row fail CAS for the unrestorable-`restoring` sweep. Bind order:
/// `$1` = `managed_by`, `$2` = min placement age seconds (`BIGINT`), `$3` =
/// sandbox id. Repeats every guard the select applied — atomically, in the
/// UPDATE's own WHERE — so a source that appeared between select and act
/// (snapshot finalized `ready`, upload enqueued, restore claim created an
/// active execution, a node adopted the sandbox) makes this claim nothing.
pub(crate) fn fail_unrestorable_restoring_query() -> String {
    let guards = unrestorable_restoring_guards();
    format!(
        "UPDATE sandboxes
            SET lifecycle = 'failed'
          WHERE managed_by = $1
            {guards}
            AND id = $3
          RETURNING id"
    )
}

pub struct SubstrateSweepSink {
    pool: PgPool,
    substrate: Arc<Substrate>,
    /// Per-node node-api clients for the liveness probe, keyed by
    /// `k8s_node_name`. Mirrors the node-drain promotion client cache:
    /// bounded by cluster size, evicted on transport-shaped errors so a
    /// replaced node-api pod gets a fresh connection. Probing is sequential,
    /// so the `tokio::sync::Mutex` is uncontended.
    probe_clients: tokio::sync::Mutex<HashMap<String, CachedProbeClient>>,
    /// See [`super::ReconcilerConfig::abandoned_restore_retirement`].
    abandoned_restore_retirement: AbandonedRestoreRetirement,
    /// See [`super::ReconcilerConfig::restoring_episode_deadline`].
    restoring_episode_deadline: Option<PositiveDuration>,
}

#[derive(Clone)]
struct CachedProbeClient {
    endpoint_addr: String,
    client: RuntimeClient,
}

/// Whether a probe failure should drop the cached client for its node.
/// Transport-shaped errors mean the connection (or the node-api pod behind
/// it) may be gone; precondition-shaped errors do not.
///
/// `LockContention` is deliberately absent: a lost lock race on a
/// control-plane statement says nothing about the node's connection. It used
/// to arrive here as `Failed` and evict the client, which was never right.
fn should_evict_probe_client(error: &SubstrateError) -> bool {
    matches!(
        error,
        SubstrateError::Unavailable { .. }
            | SubstrateError::Failed { .. }
            | SubstrateError::Timeout { .. }
            | SubstrateError::Cancelled { .. }
    )
}

impl SubstrateSweepSink {
    pub fn new(pool: PgPool, substrate: Arc<Substrate>) -> Self {
        Self {
            pool,
            substrate,
            probe_clients: tokio::sync::Mutex::new(HashMap::new()),
            abandoned_restore_retirement: AbandonedRestoreRetirement::Disabled,
            restoring_episode_deadline: None,
        }
    }

    /// Whether [`SweepSink::revert_to_suspended`] retires the abandoned
    /// restore's execution. Disabled by default.
    pub fn with_abandoned_restore_retirement(
        mut self,
        retirement: AbandonedRestoreRetirement,
    ) -> Self {
        self.abandoned_restore_retirement = retirement;
        self
    }

    /// How long a `restoring` episode may go without a pod IP before the
    /// select and the revert stop waiting for its owner. Off by default.
    pub fn with_restoring_episode_deadline(mut self, deadline: Option<PositiveDuration>) -> Self {
        self.restoring_episode_deadline = deadline;
        self
    }

    async fn probe_client(
        &self,
        node_name: &str,
        probe_timeout: Duration,
    ) -> Result<CachedProbeClient, SubstrateError> {
        let mut clients = self.probe_clients.lock().await;
        if let Some(client) = clients.get(node_name) {
            return Ok(client.clone());
        }
        let managed_by = self
            .substrate
            .managed_by()
            .parse::<NodeManagedBy>()
            .map_err(|err| SubstrateError::Failed {
                message: format!("substrate reported invalid managed_by value: {err}"),
            })?;
        let target = NodeApiTarget {
            managed_by,
            role: NodeRole::Worker,
            direct_endpoint: None,
            substrate_node_id: Some(node_name),
        };
        let (endpoint_addr, client) = connect_node_api_client_for_target(
            self.substrate.as_ref(),
            &target,
            // Per-request channel timeout: the probe deadline is the RPC
            // budget too, so a wedged node-api can't hold a tick longer
            // than the outer bound in `probe_node`.
            probe_timeout,
        )
        .await?;
        let client = CachedProbeClient {
            endpoint_addr,
            client,
        };
        clients.insert(node_name.to_string(), client.clone());
        Ok(client)
    }

    async fn evict_probe_client(&self, node_name: &str) {
        self.probe_clients.lock().await.remove(node_name);
    }

    /// Saturating seconds-as-`i64` for SQL binds; any value above `i64::MAX`
    /// is "never" in human terms.
    fn age_secs_bind(age: PositiveDuration) -> i64 {
        i64::try_from(age.get().as_secs()).unwrap_or(i64::MAX)
    }

    /// The restoring-episode deadline bind: NULL when it's off.
    fn episode_deadline_bind(&self) -> Option<i64> {
        self.restoring_episode_deadline.map(Self::age_secs_bind)
    }
}

impl SweepSink for SubstrateSweepSink {
    /// List aged candidates for `lifecycle` without flipping anything.
    ///
    /// `managed_by` scope: each substrate writes its label
    /// (`SubstrateAdapter::managed_by()`) into `sandboxes.managed_by` at
    /// row creation. The sweeper scopes to its own substrate's label so a
    /// cluster-controller running with the Kubernetes substrate doesn't
    /// claim direct-substrate rows (and vice versa) — the cleanup step
    /// is substrate-specific and would fail or silently no-op against an
    /// executor it doesn't own. Mirrors the `managed_by = 'k8s'` scope on
    /// the drain reconciler's queries.
    ///
    /// `lifecycle_changed_at` is maintained by a BEFORE UPDATE trigger
    /// (`sandboxes_lifecycle_changed_at`) so the column is bumped exactly
    /// when `lifecycle` changes, regardless of which SQL call site issued
    /// the UPDATE.
    ///
    /// Evacuation-marked `restoring` rows (a non-NULL
    /// `evacuation_restore_target`) are exempt — node-drain's restore sweep
    /// owns re-driving them and they legitimately run for the evacuate-timeout
    /// window. A marked `suspending` row is exempt only while its current
    /// suspend snapshot is still recoverable (`creating` or `ready`); once
    /// that snapshot terminalizes `failed` (or never existed) the backstop
    /// lists it, because nothing else recovers a crashed/hung suspend with no
    /// durable copy. See [`list_aged_transient_query`] for the full rationale.
    async fn list_aged_transient(
        &self,
        lifecycle: SandboxLifecycle,
        age: PositiveDuration,
    ) -> Result<Vec<SweepCandidate>, sqlx::Error> {
        // `as BIGINT` for the SQL bind. `u64::MAX` is ~5.8e11 years and
        // `i64::MAX` is ~2.9e11 years; only an operator who sets
        // `SPACE_TRANSIENT_LIFECYCLE_SWEEPER_<...>_AGE_SECS` past `i64::MAX`
        // can trip the saturation. Saturate (rather than silently wrap or
        // return an error) because any value above i64::MAX is "never age
        // out" in human terms, which matches what an operator typing a
        // huge number was almost certainly trying to express. The
        // `debug_assert!` documents the intentional clamp so a future
        // reader doesn't mistake this for fail-silent error swallowing.
        let secs = age.get().as_secs();
        debug_assert!(
            i64::try_from(secs).is_ok(),
            "age threshold {secs}s exceeds i64::MAX; will saturate to i64::MAX (effectively disabling the sweep for this lifecycle)"
        );
        let managed_by = self.substrate.managed_by();
        let query = list_aged_transient_query(lifecycle);
        let rows: Vec<AgedTransientRow> = sqlx::query_as(sqlx::AssertSqlSafe(sql!(query.as_str())))
            .bind(managed_by)
            .bind(lifecycle.to_string())
            .bind(Self::age_secs_bind(age))
            .bind(SWEEP_CANDIDATE_BATCH)
            .bind(SubstrateKind::Kubernetes)
            .bind(self.episode_deadline_bind())
            .fetch_all(&self.pool)
            .await?;
        let observed = observe_candidate_pods(
            &self.substrate,
            rows.iter().filter_map(|row| row.pod_name.as_deref()),
        )
        .await;
        Ok(rows
            .into_iter()
            .map(|row| {
                let evacuation_restore_target =
                    row.evacuation_restore_target
                        .and_then(|raw| match raw.parse() {
                            Ok(target) => Some(target),
                            Err(err) => {
                                // The column CHECK pins values to
                                // 'started'/'paused'/'stopped'; reaching this
                                // means schema/enum drift. Treat as unset (the
                                // converge target then defaults to started).
                                tracing::warn!(
                                    sandbox_id = %row.id,
                                    error = %err,
                                    "transient_lifecycle_sweeper: unparseable \
                                     evacuation_restore_target; treating as unset"
                                );
                                None
                            }
                        });
                let activation_attempt_held = row.activation_attempt_id.is_some();
                let pod_fence = candidate_pod_fence_with_observation(
                    row.id,
                    row.pod_name,
                    row.activation_attempt_id,
                    row.generation,
                    row.pod_uid,
                    row.route_activation_id,
                    &observed,
                );
                SweepCandidate {
                    sandbox_id: row.id,
                    lifecycle,
                    // Negative only under clock skew; clamp defensively.
                    age: Duration::from_secs(row.age_secs.max(0).cast_unsigned()),
                    evacuation_restore_target,
                    ready_snapshot: row.ready_snapshot,
                    fork_restore: row.fork_restore,
                    node_name: row.k8s_node_name,
                    routable_execution: row.routable_execution,
                    activation_abandoned: row.activation_abandoned,
                    episode_overdue: row.episode_overdue,
                    activation_attempt_held,
                    pod_fence,
                }
            })
            .collect())
    }

    /// Batch-probe `sandbox_ids` on `node_name` via the worker node-api's
    /// `SandboxState` RPC (which answers from the on-node sbx-mgr). The
    /// endpoint resolves **by node** through the substrate's node-api pod
    /// cache — never through the sandbox pod IP, which is unreachable for
    /// exactly the dead-pod population this probe exists to detect.
    ///
    /// Latency contract: connect and RPC are each bounded by
    /// `probe_timeout` (the channel's request timeout equals it), so the
    /// worst case per node is ~2x `probe_timeout`.
    async fn probe_node(
        &self,
        node_name: &str,
        sandbox_ids: &[SandboxId],
        probe_timeout: Duration,
    ) -> Result<HashMap<SandboxId, ProbeObservation>, ProbeNodeError> {
        let cached =
            match tokio::time::timeout(probe_timeout, self.probe_client(node_name, probe_timeout))
                .await
            {
                Ok(Ok(cached)) => cached,
                Ok(Err(err)) => {
                    if should_evict_probe_client(&err) {
                        self.evict_probe_client(node_name).await;
                    }
                    return Err(ProbeNodeError::NodeApi(err));
                }
                Err(_) => return Err(ProbeNodeError::Timeout(probe_timeout)),
            };
        let mut client = cached.client.clone();
        let states = match tokio::time::timeout(
            probe_timeout,
            sandbox_states_with_client(&mut client, &cached.endpoint_addr, sandbox_ids),
        )
        .await
        {
            Ok(Ok(states)) => states,
            Ok(Err(err)) => {
                if should_evict_probe_client(&err) {
                    self.evict_probe_client(node_name).await;
                }
                return Err(ProbeNodeError::NodeApi(err));
            }
            Err(_) => {
                self.evict_probe_client(node_name).await;
                return Err(ProbeNodeError::Timeout(probe_timeout));
            }
        };
        Ok(states
            .into_iter()
            .map(|(sandbox_id, state)| (sandbox_id, ProbeObservation::from(state)))
            .collect())
    }

    /// Routability verify-and-repair for an alive `restoring` candidate the
    /// DB reported unroutable, through the substrate's
    /// [`SubstrateAdapter::ensure_routable_execution`]. The Kubernetes
    /// substrate consults the apiserver and hydrates the execution row from a
    /// live matching pod; the direct substrate has no pod-routability
    /// contract, so its rows are always routable (and in practice never reach
    /// this call — the candidate select's fact is unconditionally true for
    /// `managed_by <> 'k8s'` rows).
    async fn ensure_routable_execution(
        &self,
        sandbox_id: SandboxId,
        probe_timeout: Duration,
    ) -> Result<ExecutionRoutability, String> {
        let check = SubstrateAdapter::ensure_routable_execution(
            self.substrate.as_ref(),
            sandbox_id,
            probe_timeout,
        )
        .await
        .map_err(|err| {
            // A broken check (apiserver unreachable, RBAC, DB error) must
            // be distinguishable from "nothing to do": the affected rows
            // sit `restoring` until this path works again, so its failure
            // rate needs its own alertable signal.
            OutcomeEvent::execution_hydration(HydrationOutcome::Error)
                .sandbox_id(sandbox_id)
                .error(&err)
                .warn();
            err.to_string()
        })?;
        match check {
            // Nothing was checked, so there is no hydration outcome to report.
            ExecutionRoutabilityCheck::Unconditional => {}
            ExecutionRoutabilityCheck::Hydrated => {
                OutcomeEvent::execution_hydration(HydrationOutcome::HydratedExecutionFromLivePod)
                    .sandbox_id(sandbox_id)
                    .info()
            }
            ExecutionRoutabilityCheck::AlreadyRoutable => {
                OutcomeEvent::execution_hydration(HydrationOutcome::Skipped)
                    .reason("already_routable")
                    .sandbox_id(sandbox_id)
                    .debug()
            }
            ExecutionRoutabilityCheck::NotRoutable => {
                OutcomeEvent::execution_hydration(HydrationOutcome::Deferred)
                    .reason("no_routable_pod")
                    .sandbox_id(sandbox_id)
                    .debug()
            }
        }
        Ok(if check.is_routable() {
            ExecutionRoutability::Routable
        } else {
            ExecutionRoutability::NotRoutable
        })
    }

    async fn converge_restoring(
        &self,
        sandbox_id: SandboxId,
        target: SandboxLifecycle,
        age: PositiveDuration,
    ) -> Result<bool, sqlx::Error> {
        let managed_by = self.substrate.managed_by();
        let mut tx = begin_locked(&self.pool, sandbox_id).await?;
        let claimed = sqlx::query_scalar::<_, SandboxId>(sqlx::AssertSqlSafe(sql!(
            converge_restoring_query()
        )))
        .bind(managed_by)
        .bind(Self::age_secs_bind(age))
        .bind(sandbox_id)
        .bind(target.as_str())
        .bind(SubstrateKind::Kubernetes)
        .fetch_optional(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(claimed.is_some())
    }

    async fn undo_revert_to_suspended(
        &self,
        sandbox_id: SandboxId,
        lifecycle: SandboxLifecycle,
        reverted: &RevertedToSuspended,
        preserve_local_artifact: bool,
    ) -> Result<bool, sqlx::Error> {
        let managed_by = self.substrate.managed_by();
        // Bound to the exact write this sweep made, not merely to the row
        // reading `suspended`. A restore that started and genuinely suspended
        // again in the meantime holds a different token, and undoing *that*
        // would drag a legitimately suspended sandbox back to `restoring`.
        let restored = sqlx::query_scalar::<_, SandboxId>(sqlx::AssertSqlSafe(sql!(
            undo_revert_to_suspended_query()
        )))
        .bind(sandbox_id)
        .bind(managed_by)
        .bind(lifecycle.to_string())
        .bind(&reverted.write_token)
        .bind(preserve_local_artifact)
        .bind(reverted.prior_placement_changed_at)
        .fetch_optional(&self.pool)
        .await?;
        Ok(restored.is_some())
    }

    async fn revert_to_suspended(
        &self,
        sandbox_id: SandboxId,
        lifecycle: SandboxLifecycle,
        age: PositiveDuration,
        fence: &RetireFence,
    ) -> Result<Option<RevertedToSuspended>, sqlx::Error> {
        let managed_by = self.substrate.managed_by();
        let mut tx = self.pool.begin().await?;
        // Take the sandbox row lock that every execution claim also takes, so
        // the execution check below can't race a new claim, and read the
        // placement clock the CAS clears while holding it.
        let prior_placement_changed_at: Option<Option<time::OffsetDateTime>> =
            sqlx::query_scalar(sql!(
                "SELECT placement_changed_at
                   FROM sandboxes
                  WHERE id = $1
                    AND managed_by = $2
                    FOR UPDATE"
            ))
            .bind(sandbox_id)
            .bind(managed_by)
            .fetch_optional(&mut *tx)
            .await?;
        let Some(prior_placement_changed_at) = prior_placement_changed_at else {
            tx.rollback().await?;
            return Ok(None);
        };
        // Then the active execution's, which the pod-IP writers take, so the
        // CAS's "no pod IP" check reads a committed IP rather than racing one.
        // Without waiting: the activation-completion receipt locks the
        // execution before the sandbox, so waiting here while holding the
        // sandbox could close a cycle with it. A held execution is one whose
        // owner is writing it right now, so the row defers to the next tick.
        let execution_locked = sqlx::query(sql!(
            "SELECT id
               FROM sandbox_k8s_executions
              WHERE sandbox_id = $1
                AND active
                FOR UPDATE NOWAIT"
        ))
        .bind(sandbox_id)
        .execute(&mut *tx)
        .await;
        match execution_locked {
            Ok(_) => {}
            Err(error) if crate::node_drain::store::is_lock_timeout(&error) => {
                tx.rollback().await?;
                return Ok(None);
            }
            Err(error) => return Err(error),
        }
        let retire = self.abandoned_restore_retirement.is_enabled();
        let query = if retire {
            revert_to_suspended_query()
        } else {
            revert_to_suspended_lifecycle_only_query()
        };
        let claimed = sqlx::query_scalar::<_, String>(sqlx::AssertSqlSafe(sql!(query.as_str())))
            .bind(managed_by)
            .bind(lifecycle.to_string())
            .bind(Self::age_secs_bind(age))
            .bind(sandbox_id)
            .bind(self.episode_deadline_bind())
            .bind(SubstrateKind::Kubernetes)
            .fetch_optional(&mut *tx)
            .await?;
        let Some(write_token) = claimed else {
            tx.rollback().await?;
            return Ok(None);
        };
        if !retire {
            // The cleanup retires the execution afterwards, under the fence.
            tx.commit().await?;
            return Ok(Some(RevertedToSuspended {
                write_token,
                prior_placement_changed_at,
            }));
        }
        let execution_settled = match fence {
            RetireFence::Pod(pod_fence) => {
                retire_reverted_restore_execution(&mut tx, sandbox_id, pod_fence).await?
            }
            RetireFence::NoExecution => {
                !sqlx::query_scalar::<_, bool>(sql!(
                    "SELECT EXISTS (
                     SELECT 1
                       FROM sandbox_k8s_executions
                      WHERE sandbox_id = $1
                        AND active
                 )"
                ))
                .bind(sandbox_id)
                .fetch_one(&mut *tx)
                .await?
            }
            // The candidate select always observes one of the two above; a
            // caller that observed nothing can't name the execution to retire.
            RetireFence::Unobserved => false,
        };
        if !execution_settled {
            tx.rollback().await?;
            return Ok(None);
        }
        tx.commit().await?;
        Ok(Some(RevertedToSuspended {
            write_token,
            prior_placement_changed_at,
        }))
    }

    async fn fail_aged_candidate(
        &self,
        sandbox_id: SandboxId,
        lifecycle: SandboxLifecycle,
        age: PositiveDuration,
        guard: FailGuard,
    ) -> Result<Option<String>, sqlx::Error> {
        let managed_by = self.substrate.managed_by();
        let query = fail_candidate_query(lifecycle, guard);
        let fail = sqlx::query_scalar::<_, String>(sqlx::AssertSqlSafe(sql!(query.as_str())))
            .bind(managed_by)
            .bind(lifecycle.to_string())
            .bind(Self::age_secs_bind(age))
            .bind(sandbox_id);
        if !is_activation_owned(lifecycle) {
            return fail.fetch_optional(&self.pool).await;
        }
        let mut tx = begin_locked(&self.pool, sandbox_id).await?;
        let token = fail.fetch_optional(&mut *tx).await?;
        tx.commit().await?;
        Ok(token)
    }

    async fn undo_fail_aged_candidate(
        &self,
        sandbox_id: SandboxId,
        lifecycle: SandboxLifecycle,
        write_token: &str,
    ) -> Result<bool, sqlx::Error> {
        let managed_by = self.substrate.managed_by();
        let restored = sqlx::query_scalar::<_, SandboxId>(sqlx::AssertSqlSafe(sql!(
            undo_fail_aged_candidate_query()
        )))
        .bind(sandbox_id)
        .bind(managed_by)
        .bind(lifecycle.to_string())
        .bind(write_token)
        .fetch_optional(&self.pool)
        .await?;
        Ok(restored.is_some())
    }

    async fn list_unrestorable_restoring(
        &self,
        min_placement_age: PositiveDuration,
    ) -> Result<Vec<(SandboxId, Duration)>, sqlx::Error> {
        let managed_by = self.substrate.managed_by();
        let rows: Vec<(SandboxId, i64)> = sqlx::query_as(sqlx::AssertSqlSafe(sql!(
            list_unrestorable_restoring_query()
        )))
        .bind(managed_by)
        .bind(Self::age_secs_bind(min_placement_age))
        .bind(SWEEP_CANDIDATE_BATCH)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows
            .into_iter()
            .map(|(id, age_secs)| {
                // Negative only under clock skew; clamp defensively.
                (id, Duration::from_secs(age_secs.max(0).cast_unsigned()))
            })
            .collect())
    }

    async fn fail_unrestorable_restoring(
        &self,
        sandbox_id: SandboxId,
        min_placement_age: PositiveDuration,
    ) -> Result<bool, sqlx::Error> {
        let managed_by = self.substrate.managed_by();
        let claimed = sqlx::query_scalar::<_, SandboxId>(sqlx::AssertSqlSafe(sql!(
            fail_unrestorable_restoring_query()
        )))
        .bind(managed_by)
        .bind(Self::age_secs_bind(min_placement_age))
        .bind(sandbox_id)
        .fetch_optional(&self.pool)
        .await?;
        Ok(claimed.is_some())
    }

    async fn retire_fence_mismatch(
        &self,
        sandbox_id: SandboxId,
        fence: &RetireFence,
    ) -> Result<Option<PodFenceMismatch>, String> {
        self.substrate
            .retire_fence_mismatch(sandbox_id, fence)
            .await
            .map_err(|err| err.to_string())
    }

    async fn cleanup_after_sweep(
        &self,
        sandbox_id: SandboxId,
        fence: RetireFence,
    ) -> Result<RetireExecutionOutcome, String> {
        self.substrate
            .retire_execution(RetireExecutionRequest { sandbox_id, fence })
            .await
            .map_err(|err| err.to_string())
    }

    async fn list_failed_active_executions(
        &self,
        batch_size: i64,
    ) -> Result<Vec<FailedCleanupCandidate>, sqlx::Error> {
        let Some(managed_by) = failed_active_execution_scope(self.substrate.managed_by()) else {
            return Ok(Vec::new());
        };
        let rows: Vec<FailedCleanupRow> = sqlx::query_as(LIST_FAILED_ACTIVE_EXECUTIONS_SQL)
            .bind(managed_by)
            .bind(batch_size)
            .fetch_all(&self.pool)
            .await?;
        let observed = observe_candidate_pods(
            &self.substrate,
            rows.iter().map(|row| row.pod_name.as_str()),
        )
        .await;
        Ok(rows
            .into_iter()
            .map(|row| FailedCleanupCandidate {
                sandbox_id: row.id,
                node_name: row.k8s_node_name,
                first_activation: FirstActivation::from_columns(
                    row.first_activation_unfinished,
                    row.first_activation_abandoned,
                ),
                pod_fence: candidate_pod_fence_with_observation(
                    row.id,
                    Some(row.pod_name),
                    row.activation_attempt_id,
                    Some(row.generation),
                    row.pod_uid,
                    row.route_activation_id,
                    &observed,
                ),
            })
            .collect())
    }

    async fn first_activation(
        &self,
        sandbox_id: SandboxId,
    ) -> Result<Option<FirstActivation>, sqlx::Error> {
        let Some(managed_by) = failed_active_execution_scope(self.substrate.managed_by()) else {
            return Ok(None);
        };
        let columns: Option<(bool, bool)> = sqlx::query_as(FIRST_ACTIVATION_SQL)
            .bind(sandbox_id)
            .bind(managed_by)
            .fetch_optional(&self.pool)
            .await?;
        Ok(columns
            .map(|(unfinished, abandoned)| FirstActivation::from_columns(unfinished, abandoned)))
    }

    async fn note_failed_cleanup_attempt(&self, sandbox_id: SandboxId) -> Result<(), sqlx::Error> {
        let Some(managed_by) = failed_active_execution_scope(self.substrate.managed_by()) else {
            return Ok(());
        };
        // This bumps `updated_at`, which is also where the age of a
        // materialization attempt comes from. That overlap is safe only
        // because a `failed` sandbox cannot have a create in flight, so
        // nothing reads that age for these rows; were it read, every rotation
        // would refresh it and an abandoned create would defer forever.
        sqlx::query(NOTE_FAILED_CLEANUP_ATTEMPT_SQL)
            .bind(sandbox_id)
            .bind(managed_by)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn cleanup_failed_execution(
        &self,
        sandbox_id: SandboxId,
        fence: RetireFence,
    ) -> Result<RetireExecutionOutcome, String> {
        self.substrate
            .retire_execution(RetireExecutionRequest { sandbox_id, fence })
            .await
            .map_err(|err| err.to_string())
    }

    async fn repair_failed_active_execution(
        &self,
        sandbox_id: SandboxId,
    ) -> Result<bool, sqlx::Error> {
        let Some(managed_by) = failed_active_execution_scope(self.substrate.managed_by()) else {
            return Ok(false);
        };
        let claimed = sqlx::query_scalar::<_, SandboxId>(REPAIR_FAILED_ACTIVE_EXECUTION_SQL)
            .bind(sandbox_id)
            .bind(managed_by)
            .fetch_optional(&self.pool)
            .await?;
        Ok(claimed.is_some())
    }

    /// List preserved `started` rows whose `command_ready_pending` marker has
    /// stuck longer than `age`. `lifecycle_changed_at` is the right age clock:
    /// the create preserve path sets the marker in the same write that flips
    /// the row to `started`, so `lifecycle_changed_at` is when the row became
    /// this pending zombie. Scoped to this controller's substrate (the reap's
    /// `cleanup_after_sweep` is substrate-specific), and capped at
    /// [`COMMAND_READY_PENDING_BATCH`] so one tick can't fan out an unbounded
    /// number of per-row guest probes.
    async fn list_command_ready_pending(
        &self,
        age: PositiveDuration,
    ) -> Result<Vec<CommandReadyPendingCandidate>, sqlx::Error> {
        let age_secs_i64 = i64::try_from(age.get().as_secs()).unwrap_or(i64::MAX);
        let managed_by = self.substrate.managed_by();
        let rows: Vec<CommandReadyPendingRow> = sqlx::query_as(LIST_COMMAND_READY_PENDING_SQL)
            .bind(managed_by)
            .bind(age_secs_i64)
            .bind(COMMAND_READY_PENDING_BATCH)
            .fetch_all(&self.pool)
            .await?;
        // `age_secs` is `NOW() - lifecycle_changed_at`, always positive given
        // the WHERE clause; clamp a negative (clock skew) to zero defensively.
        let observed = observe_candidate_pods(
            &self.substrate,
            rows.iter().filter_map(|row| row.pod_name.as_deref()),
        )
        .await;
        Ok(rows
            .into_iter()
            .map(|row| CommandReadyPendingCandidate {
                sandbox_id: row.id,
                pending_age: Duration::from_secs(row.age_secs.max(0).cast_unsigned()),
                pod_fence: candidate_pod_fence_with_observation(
                    row.id,
                    row.pod_name,
                    row.activation_attempt_id,
                    row.generation,
                    row.pod_uid,
                    row.route_activation_id,
                    &observed,
                ),
            })
            .collect())
    }

    async fn probe_command_ready(
        &self,
        sandbox_id: SandboxId,
        probe_timeout: Duration,
    ) -> CommandReadyProbe {
        self.substrate
            .probe_command_ready(sandbox_id, probe_timeout)
            .await
    }

    async fn heal_command_ready_pending(&self, sandbox_id: SandboxId) -> Result<(), sqlx::Error> {
        let managed_by = self.substrate.managed_by();
        sqlx::query(HEAL_COMMAND_READY_PENDING_SQL)
            .bind(sandbox_id)
            .bind(managed_by)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn reap_command_ready_pending(
        &self,
        sandbox_id: SandboxId,
    ) -> Result<Option<String>, sqlx::Error> {
        let managed_by = self.substrate.managed_by();
        sqlx::query_scalar::<_, String>(REAP_COMMAND_READY_PENDING_SQL)
            .bind(sandbox_id)
            .bind(managed_by)
            .fetch_optional(&self.pool)
            .await
    }

    async fn undo_reap_command_ready_pending(
        &self,
        sandbox_id: SandboxId,
        write_token: &str,
    ) -> Result<bool, sqlx::Error> {
        let managed_by = self.substrate.managed_by();
        let restored = sqlx::query_scalar::<_, SandboxId>(sqlx::AssertSqlSafe(sql!(
            undo_reap_command_ready_pending_query()
        )))
        .bind(sandbox_id)
        .bind(managed_by)
        .bind(write_token)
        .fetch_optional(&self.pool)
        .await?;
        Ok(restored.is_some())
    }
}
