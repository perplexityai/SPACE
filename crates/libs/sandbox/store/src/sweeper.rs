//! Guarded writes the cluster controller's transient-lifecycle sweeper makes
//! to `sandboxes`: aging wedged rows out, converging or reverting an
//! abandoned restore, and the compensations that put a sweep's own write back.
//!
//! The sweeper keeps everything around them: the candidate selects, the
//! sandbox and execution row locks (`SELECT ... FOR UPDATE`), the
//! transactions, and the execution and port-exposure writes beside the
//! revert. Every write function runs on the executor its caller passes, so
//! it composes into the caller's transaction when there is one;
//! [`unplace_reverted_restore`] requires that transaction, since its
//! predicate is the id alone.
//!
//! A sweep write that returns a write token (`RETURNING`
//! [`LIFECYCLE_WRITE_TOKEN`]) is undone only by the compensation bound to that
//! token, so an undo never reaches past its own transition.
//!
//! The guard fragments here are also interpolated by the statements that stay
//! with the sweeper: its candidate selects and its per-lifecycle fail CAS. They
//! are public so that each guard keeps one definition while those statements
//! live elsewhere.
//!
//! [`HEAL_COMMAND_READY_PENDING_SQL`], [`REAP_COMMAND_READY_PENDING_SQL`] and
//! the [`converge_restoring_query`], [`revert_to_suspended_query`],
//! [`undo_revert_to_suspended_query`] and
//! [`fail_unrestorable_restoring_query`] builders are public only because
//! the sweeper's Postgres tests run them directly. Production code calls the
//! function here that binds each one.

use pplx_space_common_core::types::{PositiveDuration, SandboxId, SandboxLifecycle, SubstrateKind};

use pplx_space_common_core::sql;
use pplx_space_sandbox_portability::{
    inflight_suspend_upload_exists, restore_retry_source_snapshot_exists,
    restore_retry_suspended_revert_snapshot_exists,
};
use pplx_space_sandbox_routability::routable_execution_or_non_k8s;
use time::OffsetDateTime;

/// SQL expression naming the lifecycle write a sweep just made.
///
/// A trigger bumps `lifecycle_changed_at` on every lifecycle change, so the
/// value a CAS returns identifies *that* write. A compensating CAS binds it,
/// which is what stops an undo from reaching past its own transition and
/// reverting a later, legitimate one that happens to have landed on the same
/// lifecycle. Formatted as text, at the microsecond precision the column
/// holds, so callers carry it as an opaque token.
pub const LIFECYCLE_WRITE_TOKEN: &str = "to_char(lifecycle_changed_at, 'YYYY-MM-DD HH24:MI:SS.US')";

/// Saturating seconds-as-`i64` for SQL binds; any value above `i64::MAX`
/// is "never" in human terms.
fn secs_bind(age: PositiveDuration) -> i64 {
    i64::try_from(age.get().as_secs()).unwrap_or(i64::MAX)
}

/// The binds [`restoring_episode_overdue`] reads.
#[derive(Debug, Clone, Copy)]
pub struct EpisodeDeadlineBinds {
    /// A nullable `BIGINT` bind of the deadline in seconds; NULL turns the
    /// deadline off.
    pub deadline: &'static str,
    /// A bind of `SubstrateKind::Kubernetes`.
    pub kubernetes: &'static str,
}

/// The revert CASes' binds ([`revert_to_suspended_query`]).
const REVERT_EPISODE_DEADLINE: EpisodeDeadlineBinds = EpisodeDeadlineBinds {
    deadline: "$5",
    kubernetes: "$6",
};

/// Whether a `restoring` row's episode outlived the restoring-episode
/// deadline while its active execution has no pod IP, so that no retry can
/// keep it alive.
///
/// A client retry re-claims an expired restore lease, renews it, and gives
/// the wake a fresh activation attempt on the same pending pod, so a client
/// that retries faster than the lease and owner guards' windows keeps a
/// failed restore's pod, and the node slot it holds, forever. The deadline
/// keys on `lifecycle_changed_at`, which a retry can't move: the user claim
/// leaves it alone on a `restoring -> restoring` re-drive, and the lifecycle
/// trigger stamps it only when the lifecycle changes.
///
/// It stops short of a restore whose pod is up. A restore pod gets its IP
/// only once its node has loaded the snapshot and resumed the VM, so an active
/// execution with a pod IP means that load finished; what is left is the
/// wake's own completion, and only the ordinary guards may let that row go.
/// The check reads the IP the execution holds now: one whose pod was replaced
/// has none again, and its new pod starts the load over. It also requires the
/// revert's own snapshot proof
/// ([`restore_retry_suspended_revert_snapshot_exists`]) and the restore-source
/// proof the sweep's decision reverts on
/// ([`restore_retry_source_snapshot_exists`]): the deadline exists to revert,
/// and a row that can't be reverted keeps the owner guards rather than being
/// listed for a fail its owner guards then refuse every tick.
///
/// Only a Kubernetes row has a pod IP to wait for. Any other substrate's
/// `restoring` row has no Kubernetes execution at all, which would read as
/// one without a pod IP and override a live restore lease.
///
/// Reverting under a live owner is safe for the same reason as reverting a
/// gone one: the wake's own writes are `restoring`-guarded, and the revert's
/// pod fence refuses a pod that was replaced, or whose object changed, after
/// the candidate was observed. A pod that comes up between that re-check and
/// the CAS is still deleted, which wastes its load but loses nothing: the row
/// never reached `started`, so no request reached its VM, and the snapshot
/// the revert proved is the one the next restore selects.
///
/// The revert CAS's caller also takes the active execution's row lock, which
/// the pod-IP writers take, so the no-IP read here is current when it
/// commits, and it defers rather than waits when a writer holds it (the
/// sweeper's `SubstrateSweepSink::revert_to_suspended`).
///
/// It renders one parenthesized condition on the row named `sandboxes`, so
/// the calling statement must not alias that table.
pub fn restoring_episode_overdue(binds: EpisodeDeadlineBinds) -> String {
    let EpisodeDeadlineBinds {
        deadline,
        kubernetes,
    } = binds;
    let revert_safe_snapshot = restore_retry_suspended_revert_snapshot_exists("sandboxes");
    let source_snapshot = restore_retry_source_snapshot_exists("sandboxes");
    format!(
        "({deadline}::BIGINT IS NOT NULL
                     AND sandboxes.managed_by = {kubernetes}
                     AND sandboxes.lifecycle_changed_at <
                        NOW() - make_interval(secs => {deadline}::BIGINT)
                     AND NOT EXISTS (
                         SELECT 1
                           FROM sandbox_k8s_executions ip
                          WHERE ip.sandbox_id = sandboxes.id
                            AND ip.active
                            AND ip.pod_ip IS NOT NULL
                     )
                     AND {revert_safe_snapshot}
                     AND {source_snapshot})"
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
///
/// It renders a conjunct that starts with a newline and `AND`, on the row
/// named `sandboxes`, so the calling statement must not alias that table.
pub fn restoring_ownership_guards(
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
///
/// It renders an unparenthesized conjunction (the lease clause `AND` the
/// owner guard): parenthesize it before applying `NOT` or `OR`. It reads the
/// row named `sandboxes`, so the calling statement must not alias that table.
pub fn restoring_unowned(age_param: &str) -> String {
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
/// That lease is the owner of every activation-owned lifecycle (the sweeper's
/// `is_activation_owned`): the create, start, resume, wake or restore
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
/// Every CAS that applies this takes the sandbox row lock first (the
/// sweeper's `LOCK_SANDBOX_ROW_SQL`); the candidate select only lists, so it
/// needn't.
///
/// It renders a conjunct that starts with a newline and `AND`, on the row
/// named `sandboxes`, so the calling statement must not alias that table.
pub fn activation_owner_guard(age_param: &str) -> String {
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

/// Heal query: clear the marker on a probe-confirmed-ready row. Bind order:
/// `$1` = sandbox id, `$2` = `managed_by`. Lifecycle is left untouched.
pub const HEAL_COMMAND_READY_PENDING_SQL: &str = sql!(
    "UPDATE sandboxes
     SET command_ready_pending = false
     WHERE id = $1
       AND managed_by = $2
       AND command_ready_pending = true"
);

/// Clear `command_ready_pending` on a `started` row the sweeper's guest probe
/// found command-ready ([`HEAL_COMMAND_READY_PENDING_SQL`]).
///
/// Writes an **observed projection**: the probe's answer. No lifecycle
/// predicate of its own; it re-checks only that the marker is still set.
pub async fn heal_command_ready_pending(
    executor: impl sqlx::PgExecutor<'_>,
    sandbox_id: SandboxId,
    managed_by: &str,
) -> Result<(), sqlx::Error> {
    sqlx::query(HEAL_COMMAND_READY_PENDING_SQL)
        .bind(sandbox_id)
        .bind(managed_by)
        .execute(executor)
        .await?;
    Ok(())
}

/// Reap CAS: flip a wedged `started` + marker row to `failed`. Bind order:
/// `$1` = sandbox id, `$2` = `managed_by`. It returns the reap's write token:
/// the [`LIFECYCLE_WRITE_TOKEN`] expression, spelled out because a `sql!`
/// literal cannot interpolate it. Keep the two identical, or the reap's undo
/// never matches. No row means a concurrent heal or transition dropped it out
/// of the predicate. The `sandboxes_lifecycle_changed_at` and
/// `sandboxes_clear_command_ready_pending` triggers fire on the resulting
/// `started` -> `failed` change, so no follow-up write is needed.
pub const REAP_COMMAND_READY_PENDING_SQL: &str = sql!(
    "UPDATE sandboxes
     SET lifecycle = 'failed'
     WHERE id = $1
       AND managed_by = $2
       AND lifecycle = 'started'
       AND command_ready_pending = true
     RETURNING to_char(lifecycle_changed_at, 'YYYY-MM-DD HH24:MI:SS.US')"
);

/// Fail a `started` row whose `command_ready_pending` marker outlived the
/// reaper's age and whose guest probe found it wedged
/// ([`REAP_COMMAND_READY_PENDING_SQL`]). Returns the reap's write token
/// ([`LIFECYCLE_WRITE_TOKEN`]), or `None` when a heal or another transition
/// got there first.
///
/// Writes an **inference**: the sweeper concluding the preserved create will
/// never become ready. The age the caller listed it under is not re-checked
/// here; the `started` and marker predicates are the whole guard.
pub async fn reap_command_ready_pending(
    executor: impl sqlx::PgExecutor<'_>,
    sandbox_id: SandboxId,
    managed_by: &str,
) -> Result<Option<String>, sqlx::Error> {
    sqlx::query_scalar::<_, String>(REAP_COMMAND_READY_PENDING_SQL)
        .bind(sandbox_id)
        .bind(managed_by)
        .fetch_optional(executor)
        .await
}

/// Put a reaped row back, bound to the reap's own write. Restores the marker
/// explicitly: the trigger that cleared it on the way to `failed` does not put
/// it back, and without it the row leaves the reaper for good.
fn undo_reap_command_ready_pending_query() -> String {
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

/// Undo [`reap_command_ready_pending`] when the retire it was meant to enable
/// was refused, bound to the reap's `write_token`. Returns whether the row
/// went back to `started` with its marker.
///
/// Writes an **operation step**: the reap's compensation. A row that left
/// `failed`, or was failed again by another write, holds a different token
/// and is left alone.
pub async fn undo_reap_command_ready_pending(
    executor: impl sqlx::PgExecutor<'_>,
    sandbox_id: SandboxId,
    managed_by: &str,
    write_token: &str,
) -> Result<bool, sqlx::Error> {
    let restored = sqlx::query_scalar::<_, SandboxId>(sqlx::AssertSqlSafe(sql!(
        undo_reap_command_ready_pending_query()
    )))
    .bind(sandbox_id)
    .bind(managed_by)
    .bind(write_token)
    .fetch_optional(executor)
    .await?;
    Ok(restored.is_some())
}

/// Put an aged-out row back, bound to the fail CAS's own write.
fn undo_fail_aged_candidate_query() -> String {
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

/// Undo the sweeper's fail of an aged `lifecycle` row when the retire it was
/// meant to enable was refused, bound to the fail's `write_token`. Returns
/// whether the row went back to `lifecycle`.
///
/// Writes an **operation step**: the fail's compensation. Only the token
/// guards which write it undoes; the caller supplies the lifecycle the fail
/// started from.
///
/// It puts back only the lifecycle. A fail from `suspending` or `restoring`
/// fired triggers that cleared the evacuation marker and its restore backoff
/// and, from `restoring`, also the restore lease and
/// `preserve_local_artifact_on_restore_failure`; none of that comes back. The
/// sweeper therefore never undoes a fail from either lifecycle (its
/// `Reconciler::fail_candidate`) and leaves such a row `failed`.
pub async fn undo_fail_aged_candidate(
    executor: impl sqlx::PgExecutor<'_>,
    sandbox_id: SandboxId,
    managed_by: &str,
    lifecycle: SandboxLifecycle,
    write_token: &str,
) -> Result<bool, sqlx::Error> {
    let restored = sqlx::query_scalar::<_, SandboxId>(sqlx::AssertSqlSafe(sql!(
        undo_fail_aged_candidate_query()
    )))
    .bind(sandbox_id)
    .bind(managed_by)
    .bind(lifecycle.to_string())
    .bind(write_token)
    .fetch_optional(executor)
    .await?;
    Ok(restored.is_some())
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
pub fn converge_restoring_query() -> String {
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

/// Converge an aged `restoring` row whose restore the sweeper's probe found
/// alive to its wake `target` ([`converge_restoring_query`]). Returns whether
/// the row converged.
///
/// Writes an **inference**: the sweeper concluding that an abandoned restore
/// finished. The caller runs it in a transaction that already holds the
/// sandbox row lock, so the owner guard sees any attempt claimed while it
/// waited.
pub async fn converge_restoring(
    executor: impl sqlx::PgExecutor<'_>,
    sandbox_id: SandboxId,
    managed_by: &str,
    target: SandboxLifecycle,
    age: PositiveDuration,
) -> Result<bool, sqlx::Error> {
    let claimed =
        sqlx::query_scalar::<_, SandboxId>(sqlx::AssertSqlSafe(sql!(converge_restoring_query())))
            .bind(managed_by)
            .bind(secs_bind(age))
            .bind(sandbox_id)
            .bind(target.as_str())
            .bind(SubstrateKind::Kubernetes)
            .fetch_optional(executor)
            .await?;
    Ok(claimed.is_some())
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
///   pre-wake bytes (see the sweeper's `classify::decide`).
/// * the restore-lease guard, mirroring the converge CAS: an unexpired or
///   recently expired lease means a restore attempt may still own the row;
///   reclaim waits one complete age window after expiry.
/// * the suspended-revert-safe restore snapshot predicate, so a late artifact
///   published by a failed restore branch can't become the snapshot a later
///   suspended restore selects.
///
/// It also clears `placement_changed_at` (see the sweeper's
/// `SweepSink::revert_to_suspended`), and repeats the select's
/// [`activation_owner_guard`], for the same reason as
/// [`converge_restoring_query`]: a row whose restore lease was cleared may
/// still have a live wake's activation attempt. An overdue episode
/// ([`restoring_episode_overdue`]) waives the lease and owner guards, as in the
/// select; the converge CAS never does, since only a restore that reached a
/// pod IP can converge.
pub fn revert_to_suspended_query() -> String {
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
fn revert_to_suspended_lifecycle_only_query() -> String {
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

/// Revert an abandoned `restoring` row to `suspended` and clear its
/// `placement_changed_at` ([`revert_to_suspended_query`]), the form used when
/// the caller retires the restore's execution in the same transaction.
/// Returns the revert's write token ([`LIFECYCLE_WRITE_TOKEN`]), or `None`
/// when the row no longer qualifies.
///
/// Writes an **inference**: the sweeper concluding the restore is abandoned
/// and its pre-restore snapshot is what the row should rest on.
/// `restoring_episode_deadline` (`None` turns it off) lets an overdue episode
/// through the lease and owner guards ([`restoring_episode_overdue`]).
///
/// The caller holds the sandbox row lock, and the active execution's lock
/// when there is one, before this runs. In the same transaction it then
/// retires the execution its candidate observed and, once that retire
/// matches, calls [`unplace_reverted_restore`]; for a candidate that observed
/// no execution it only checks that none is active. It rolls the revert back
/// when the execution is not the one its candidate observed, or when the
/// candidate has no usable record of it.
pub async fn revert_to_suspended(
    executor: impl sqlx::PgExecutor<'_>,
    sandbox_id: SandboxId,
    managed_by: &str,
    lifecycle: SandboxLifecycle,
    age: PositiveDuration,
    restoring_episode_deadline: Option<PositiveDuration>,
) -> Result<Option<String>, sqlx::Error> {
    sqlx::query_scalar::<_, String>(sqlx::AssertSqlSafe(sql!(revert_to_suspended_query())))
        .bind(managed_by)
        .bind(lifecycle.to_string())
        .bind(secs_bind(age))
        .bind(sandbox_id)
        .bind(restoring_episode_deadline.map(secs_bind))
        .bind(SubstrateKind::Kubernetes)
        .fetch_optional(executor)
        .await
}

/// [`revert_to_suspended`] moving only the lifecycle
/// (`revert_to_suspended_lifecycle_only_query`), the form used when the
/// caller leaves the execution for its cleanup to retire under the
/// candidate's fence. The same guards and binds, the same write token.
///
/// Writes an **inference**, as [`revert_to_suspended`] does. The caller takes
/// the same locks first.
pub async fn revert_to_suspended_lifecycle_only(
    executor: impl sqlx::PgExecutor<'_>,
    sandbox_id: SandboxId,
    managed_by: &str,
    lifecycle: SandboxLifecycle,
    age: PositiveDuration,
    restoring_episode_deadline: Option<PositiveDuration>,
) -> Result<Option<String>, sqlx::Error> {
    sqlx::query_scalar::<_, String>(sqlx::AssertSqlSafe(sql!(
        revert_to_suspended_lifecycle_only_query()
    )))
    .bind(managed_by)
    .bind(lifecycle.to_string())
    .bind(secs_bind(age))
    .bind(sandbox_id)
    .bind(restoring_episode_deadline.map(secs_bind))
    .bind(SubstrateKind::Kubernetes)
    .fetch_optional(executor)
    .await
}

/// Unplace a row [`revert_to_suspended`] just reverted, once its caller has
/// retired the restore's execution: the retired execution no longer places
/// the row, and a `suspended` row left reading as placed is one nothing
/// revisits.
///
/// Writes an **inference**, the revert's consequence for placement. It has no
/// predicate beyond the id, no lifecycle guard of its own: it relies on the
/// revert CAS and the execution retire before it in the caller's transaction,
/// under the sandbox row lock the caller took first.
pub async fn unplace_reverted_restore(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    sandbox_id: SandboxId,
) -> Result<(), sqlx::Error> {
    sqlx::query(sql!(
        "UPDATE sandboxes
                SET placement = 'unplaced',
                    active_node_id = NULL
              WHERE id = $1"
    ))
    .bind(sandbox_id)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

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
pub fn undo_revert_to_suspended_query() -> String {
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

/// Undo either revert form when the cleanup it was meant to enable was
/// refused, bound to the revert's `write_token`
/// ([`undo_revert_to_suspended_query`]). Returns whether the row went back to
/// `lifecycle`.
///
/// Writes an **operation step**: the revert's compensation.
/// `preserve_local_artifact` and `prior_placement_changed_at` are what the
/// row held before the revert. A restore that started and genuinely suspended
/// again in the meantime holds a different token, and is left alone.
pub async fn undo_revert_to_suspended(
    executor: impl sqlx::PgExecutor<'_>,
    sandbox_id: SandboxId,
    managed_by: &str,
    lifecycle: SandboxLifecycle,
    write_token: &str,
    preserve_local_artifact: bool,
    prior_placement_changed_at: Option<OffsetDateTime>,
) -> Result<bool, sqlx::Error> {
    let restored = sqlx::query_scalar::<_, SandboxId>(sqlx::AssertSqlSafe(sql!(
        undo_revert_to_suspended_query()
    )))
    .bind(sandbox_id)
    .bind(managed_by)
    .bind(lifecycle.to_string())
    .bind(write_token)
    .bind(preserve_local_artifact)
    .bind(prior_placement_changed_at)
    .fetch_optional(executor)
    .await?;
    Ok(restored.is_some())
}

/// Shared predicate for the unrestorable-`restoring` sweep: a markerless
/// `restoring` row that the restore planner **provably cannot restore from
/// anywhere** — the exact wedge the aged-transient sweep cannot reach,
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
/// **Deliberately no restore-lease guard** (unlike the sweeper's other CASes
/// on a `restoring` row: the aged fail, the converge and the revert all apply
/// one, and the revert waives it only for an overdue episode): legacy wedged
/// rows can carry a lease even though no restore source exists, so a lease
/// guard would defer them forever. Omitting it is safe *because of* the
/// source clauses: a lease-holding attempt that could actually make progress
/// would have an active execution or an eligible source, both of which drop
/// the row out of this predicate atomically in the fail CAS.
///
/// It renders conjuncts that start with `AND`, on the row named `sandboxes`
/// (so the calling statement must not alias that table), and reads `$2` as
/// the minimum placement age in seconds (`BIGINT`), which the calling
/// statement must bind there.
pub fn unrestorable_restoring_guards() -> String {
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

/// Per-row fail CAS for the unrestorable-`restoring` sweep. Bind order:
/// `$1` = `managed_by`, `$2` = min placement age seconds (`BIGINT`), `$3` =
/// sandbox id. Repeats every guard the select applied — atomically, in the
/// UPDATE's own WHERE — so a source that appeared between select and act
/// (snapshot finalized `ready`, upload enqueued, restore claim created an
/// active execution, a node adopted the sandbox) makes this claim nothing.
pub fn fail_unrestorable_restoring_query() -> String {
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

/// Fail a `restoring` row no restore can ever succeed for
/// ([`fail_unrestorable_restoring_query`]). Returns whether the row failed.
///
/// Writes an **inference**: the sweeper concluding, from the absence of any
/// restore source, that the row is unrestorable. It has no restore-lease
/// guard, on purpose ([`unrestorable_restoring_guards`]).
pub async fn fail_unrestorable_restoring(
    executor: impl sqlx::PgExecutor<'_>,
    sandbox_id: SandboxId,
    managed_by: &str,
    min_placement_age: PositiveDuration,
) -> Result<bool, sqlx::Error> {
    let claimed = sqlx::query_scalar::<_, SandboxId>(sqlx::AssertSqlSafe(sql!(
        fail_unrestorable_restoring_query()
    )))
    .bind(managed_by)
    .bind(secs_bind(min_placement_age))
    .bind(sandbox_id)
    .fetch_optional(executor)
    .await?;
    Ok(claimed.is_some())
}
