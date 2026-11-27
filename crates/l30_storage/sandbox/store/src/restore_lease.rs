//! Guarded writes of the Kubernetes substrate's restore lease on `sandboxes`:
//! the user and evacuation restore claims, the markerless re-drive's lease
//! claim, the owned completion and the lease release, and the reverts of an
//! abandoned claim. Also the row locks of those paths: the revert's lock on a
//! row still `restoring` under its claim ([`lock_restore_claim`]), and the
//! lock a create-claim cleanup takes to read the lease token it compares
//! ([`lock_restore_lease_token`]). And the reads behind them: the evidence
//! the revert decides on under its lock ([`restore_revert_evidence`]), and,
//! of the restore gate, the ownership snapshot a caller reads when a claim
//! matches nothing and the batched observation of user restores in flight.
//!
//! The restore lease is the pair `restore_lease_expires_at` /
//! `restore_lease_token`. A user claim writes both; the evacuation claim is
//! tokenless and is fenced by the `evacuation_restore_target` marker
//! instead. Leaving `restoring` clears the pair through the
//! `sandboxes_clear_restore_lease` trigger.
//!
//! The Kubernetes store keeps everything around these statements: the
//! transactions the locks last for, the execution retirement beside the
//! revert, and what it decides from the locks, the evidence and the gate
//! reads. A claim, completion or guarded lock that matches no
//! row is an outcome, not an error.

use std::sync::LazyLock;

use pplx_space_node_model::SubstrateKind;
use pplx_space_postgres_pool::{DbError, Executor};
use pplx_space_sandbox_model::{SandboxActivationAttemptId, SandboxId, SandboxLifecycle};

use pplx_space_sandbox_portability::restore_retry_suspended_revert_snapshot_exists;
use pplx_space_sandbox_routability::routable_execution_or_non_k8s;
use pplx_space_sql_locator::sql;
use uuid::Uuid;

/// What a restore claim returns: the lifecycle the row had before the claim,
/// and the lease token the claim wrote (`None` for the tokenless evacuation
/// claim).
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct RestoreClaimRow {
    pub lifecycle: String,
    pub restore_lease_token: Option<Uuid>,
}

/// The evacuation restore claim: flip a row that carries the node-drain
/// `evacuation_restore_target` marker to `restoring`, atomically on that
/// marker. Writes an **operation step** (the claim). Returns the row's
/// lifecycle before the claim, or `None` when the claim lost.
///
/// Takes a `suspended` or `restoring` row with the marker still set (a
/// `restoring` one is crash recovery after the controller died between this
/// claim and finalize), or a released stopped evacuation: lifecycle
/// `stopped`, marker `'stopped'`, no `active_node_id`, and only when
/// `evacuation_target` is [`SandboxLifecycle::Stopped`]. It writes no lease:
/// the marker fences it, and the claim's `restore_lease_token` is always
/// `NULL`. `lease_ttl_secs` is bound as `$2` but not written: it only
/// filters the returned row. The `UPDATE` in the statement's `WITH` runs
/// whatever its value, so a negative value still moves the row to
/// `restoring` and returns `None`. Pass a non-negative value.
///
/// `execution_ownership_ttl_secs` is bound as `$4`, which this statement
/// doesn't read; it keeps the parameter list the statement has always been
/// prepared with.
pub async fn claim_evacuation_restore(
    executor: impl Executor<'_>,
    sandbox_id: SandboxId,
    lease_ttl_secs: i64,
    execution_ownership_ttl_secs: i64,
    evacuation_target: Option<SandboxLifecycle>,
) -> Result<Option<RestoreClaimRow>, DbError> {
    Ok(sqlx::query_as::<_, RestoreClaimRow>(sql!(
        "WITH candidate AS (
                     SELECT id, lifecycle
                       FROM sandboxes
                      WHERE id = $1
                        AND (
                          LOWER(TRIM(lifecycle)) IN ('suspended', 'restoring')
                          OR (
                            LOWER(TRIM(lifecycle)) = 'stopped'
                            AND evacuation_restore_target = 'stopped'
                            AND active_node_id IS NULL
                            AND $5::text = 'stopped'
                          )
                        )
                        AND evacuation_restore_target IS NOT NULL
                      FOR UPDATE
                 ),
                 updated AS (
                     UPDATE sandboxes s
                        SET lifecycle = 'restoring'
                       FROM candidate c
                     WHERE s.id = c.id
                      RETURNING c.lifecycle
                 )
                 SELECT u.lifecycle, args.restore_lease_token
                   FROM updated u
                   CROSS JOIN (
                     SELECT $2::bigint AS ttl_secs, $3::uuid AS restore_lease_token
                   ) args
                  WHERE args.ttl_secs >= 0"
    ))
    .bind(sandbox_id)
    .bind(lease_ttl_secs)
    .bind(None::<Uuid>)
    .bind(execution_ownership_ttl_secs)
    .bind(evacuation_target.map(SandboxLifecycle::as_str))
    .fetch_optional(executor.into_sqlx())
    .await?)
}

/// The user restore claim: flip a `suspended` row, or a markerless
/// `restoring` row, to `restoring` under a new restore lease. Writes an
/// **operation step** (the claim). Returns the row's lifecycle before the
/// claim and the lease token it wrote, or `None` when the claim lost.
///
/// In both cases the row must hold no live restore lease, and no active
/// execution in `creating`/`created`/`recovering`/`running` may have been
/// updated within `execution_ownership_ttl_secs`. A `restoring` row that
/// still carries the `evacuation_restore_target` marker is a drain restore in
/// flight and matches nothing. The claim clears that marker, so on a marked
/// `suspended` row it is a user takeover: the drain's marker-required claim
/// then loses.
///
/// The claim preserves `lifecycle_changed_at` across
/// `restoring -> restoring` re-drives: restore selection uses it as the
/// immutable boundary between the original source window and artifacts
/// published by a failed restore branch. The lifecycle trigger still sets it
/// for a fresh `suspended -> restoring` claim. The restore lease protects an
/// active re-drive from the transient-lifecycle sweeper, with one exception:
/// when the sweeper's restoring-episode deadline is configured, an overdue
/// episode (`sweeper::restoring_episode_overdue`) passes the lease and owner
/// guards, so the sweeper may revert the row under a live lease.
///
/// `evacuation_target` is bound as `$5`, which this statement doesn't read;
/// it keeps the parameter list the statement has always been prepared with.
pub async fn claim_user_restore(
    executor: impl Executor<'_>,
    sandbox_id: SandboxId,
    lease_ttl_secs: i64,
    restore_lease_token: Uuid,
    execution_ownership_ttl_secs: i64,
    evacuation_target: Option<SandboxLifecycle>,
) -> Result<Option<RestoreClaimRow>, DbError> {
    Ok(sqlx::query_as::<_, RestoreClaimRow>(sql!(
        "WITH candidate AS (
                     SELECT id, lifecycle
                       FROM sandboxes
                      WHERE id = $1
                        AND (
                          LOWER(TRIM(lifecycle)) = 'suspended'
                          OR (
                            LOWER(TRIM(lifecycle)) = 'restoring'
                            AND evacuation_restore_target IS NULL
                            AND (
                              restore_lease_expires_at IS NULL
                              OR restore_lease_expires_at <= NOW()
                            )
                          )
                        )
                        AND (
                          restore_lease_expires_at IS NULL
                          OR restore_lease_expires_at <= NOW()
                        )
                        AND NOT EXISTS (
                          SELECT 1
                            FROM sandbox_k8s_executions e
                           WHERE e.sandbox_id = sandboxes.id
                             AND e.active
                             AND e.state IN ('creating', 'created', 'recovering', 'running')
                             AND e.updated_at >
                                 NOW() - make_interval(secs => $4)
                        )
                      FOR UPDATE
                 ),
                 updated AS (
                     UPDATE sandboxes s
                        SET lifecycle = 'restoring',
                            evacuation_restore_target = NULL,
                            restore_lease_expires_at = NOW() + ($2::bigint * INTERVAL '1 second'),
                            restore_lease_token = $3
                       FROM candidate c
                      WHERE s.id = c.id
                      RETURNING c.lifecycle
                 )
                 SELECT lifecycle, $3::uuid AS restore_lease_token FROM updated"
    ))
    .bind(sandbox_id)
    .bind(lease_ttl_secs)
    .bind(Some(restore_lease_token))
    .bind(execution_ownership_ttl_secs)
    .bind(evacuation_target.map(SandboxLifecycle::as_str))
    .fetch_optional(executor.into_sqlx())
    .await?)
}

/// Take the restore lease of a markerless `restoring` row without moving its
/// lifecycle, so a retry may re-drive it. Writes an **operation step** (the
/// lease claim). Returns the claimed row, or `None` when the row isn't a
/// markerless `restoring` row, its lease is still live, or an active
/// execution did work within `execution_ownership_ttl_secs`.
pub async fn claim_markerless_restoring_lease(
    executor: impl Executor<'_>,
    sandbox_id: SandboxId,
    lease_ttl_secs: i64,
    restore_lease_token: Uuid,
    execution_ownership_ttl_secs: i64,
) -> Result<Option<RestoreClaimRow>, DbError> {
    Ok(sqlx::query_as::<_, RestoreClaimRow>(sql!(
        "WITH candidate AS (
                 SELECT id, lifecycle
                   FROM sandboxes
                  WHERE id = $1
                    AND LOWER(TRIM(lifecycle)) = 'restoring'
                    AND evacuation_restore_target IS NULL
                    AND (
                      restore_lease_expires_at IS NULL
                      OR restore_lease_expires_at <= NOW()
                    )
                    AND NOT EXISTS (
                      SELECT 1
                        FROM sandbox_k8s_executions e
                       WHERE e.sandbox_id = sandboxes.id
                         AND e.active
                         AND e.state IN ('creating', 'created', 'recovering', 'running')
                         AND e.updated_at > NOW() - make_interval(secs => $4)
                    )
                  FOR UPDATE
             ),
             updated AS (
                 UPDATE sandboxes s
                    SET restore_lease_expires_at = NOW() + ($2::bigint * INTERVAL '1 second'),
                        restore_lease_token = $3
                   FROM candidate c
                  WHERE s.id = c.id
                  RETURNING c.lifecycle
             )
             SELECT lifecycle, $3::uuid AS restore_lease_token FROM updated"
    ))
    .bind(sandbox_id)
    .bind(lease_ttl_secs)
    .bind(restore_lease_token)
    .bind(execution_ownership_ttl_secs)
    .fetch_optional(executor.into_sqlx())
    .await?)
}

/// Commit `restoring -> started` (and `on_node`) for the restore whose lease
/// `restore_lease_token` holds. Writes an **operation step** (the claim's
/// completion). Returns whether the row moved.
///
/// The row must still be `restoring` under that token, with a routable
/// active execution behind it, or a `managed_by` other than Kubernetes
/// ([`routable_execution_or_non_k8s`]).
/// A row a rollback, the sweeper, a suspend or a delete has moved on matches
/// nothing.
pub async fn complete_owned_user_restore(
    executor: impl Executor<'_>,
    sandbox_id: SandboxId,
    restore_lease_token: Uuid,
) -> Result<bool, DbError> {
    let routable = routable_execution_or_non_k8s("sandboxes", "$3");
    let query = format!(
        "UPDATE sandboxes
                SET lifecycle = 'started', placement = 'on_node'
              WHERE id = $1
                AND lifecycle = 'restoring'
                AND restore_lease_token = $2
                AND {routable}"
    );
    let completed = sqlx::query(sqlx::AssertSqlSafe(sql!(query.as_str())))
        .bind(sandbox_id)
        .bind(restore_lease_token)
        .bind(SubstrateKind::Kubernetes)
        .execute(executor.into_sqlx())
        .await?
        .rows_affected();
    Ok(completed == 1)
}

/// Expire the restore lease `restore_lease_token` holds, keeping the token
/// and setting the expiry to the database's `NOW()`. Writes an **operation
/// step** (the lease's release).
///
/// Guarded by the token alone, with no lifecycle predicate: a lease a newer
/// claim replaced, or one the lease-clear trigger dropped, matches nothing.
pub async fn release_user_restore_lease(
    executor: impl Executor<'_>,
    sandbox_id: SandboxId,
    restore_lease_token: Uuid,
) -> Result<(), DbError> {
    sqlx::query(sql!(
        "UPDATE sandboxes
                SET restore_lease_expires_at = NOW()
              WHERE id = $1
                AND restore_lease_token = $2"
    ))
    .bind(sandbox_id)
    .bind(restore_lease_token)
    .execute(executor.into_sqlx())
    .await?;
    Ok(())
}

/// Revert an abandoned restore claim to `suspended` whose execution the
/// caller retired in this transaction: unplace the row and clear
/// `placement_changed_at`. Writes an **operation step** (the claim's
/// compensation).
///
/// No lifecycle or lease predicate of its own; it relies on the caller's row
/// lock, taken on `restoring` (and, when the claim wrote a lease token, on
/// that token), and on the caller's proof that the pre-restore source is still restorable.
/// The caller must pass its open transaction.
pub async fn revert_restore_claim_retiring_placement<C>(
    tx: &mut C,
    sandbox_id: SandboxId,
) -> Result<(), DbError>
where
    for<'a> &'a mut C: Executor<'a>,
{
    sqlx::query(sql!(
        "UPDATE sandboxes
                        SET lifecycle = 'suspended',
                            placement = 'unplaced',
                            active_node_id = NULL,
                            placement_changed_at = NULL
                      WHERE id = $1"
    ))
    .bind(sandbox_id)
    .execute(tx.into_sqlx())
    .await?;
    Ok(())
}

/// Revert an abandoned restore claim with no execution at all to
/// `suspended`, clearing `placement_changed_at`. Writes an **operation step**
/// (the claim's compensation).
///
/// No lifecycle or lease predicate of its own; it relies on the caller's row
/// lock and on the caller's proof that the pre-restore source is still
/// restorable. The caller must pass its open transaction.
pub async fn revert_restore_claim_clearing_placement_clock<C>(
    tx: &mut C,
    sandbox_id: SandboxId,
) -> Result<(), DbError>
where
    for<'a> &'a mut C: Executor<'a>,
{
    sqlx::query(sql!(
        "UPDATE sandboxes
                        SET lifecycle = 'suspended',
                            placement_changed_at = NULL
                      WHERE id = $1"
    ))
    .bind(sandbox_id)
    .execute(tx.into_sqlx())
    .await?;
    Ok(())
}

/// Revert an abandoned restore claim's lifecycle to `suspended`, leaving
/// its execution, placement and placement clock as the restore left them.
/// Writes an **operation step** (the claim's compensation).
///
/// No lifecycle or lease predicate of its own; it relies on the caller's row
/// lock. The caller must pass its open transaction.
pub async fn revert_restore_claim_lifecycle<C>(
    tx: &mut C,
    sandbox_id: SandboxId,
) -> Result<(), DbError>
where
    for<'a> &'a mut C: Executor<'a>,
{
    sqlx::query(sql!(
        "UPDATE sandboxes SET lifecycle = 'suspended' WHERE id = $1"
    ))
    .bind(sandbox_id)
    .execute(tx.into_sqlx())
    .await?;
    Ok(())
}

/// Lock the sandbox row `FOR UPDATE` while it is still `restoring` under the
/// claim being reverted, until the caller's transaction ends. With
/// `restore_lease_token` the row must still carry that token; `None` skips
/// the token check. Returns whether the row matched. A row that already
/// fails the guard when the statement reads it is not locked; under READ
/// COMMITTED, one that a concurrent writer moves off the guard while the
/// statement waits for its lock can stay locked until the transaction ends.
///
/// The revert takes it in its own statement first, so its later statements
/// read the snapshots and executions as of after the lock. The caller must
/// pass its open transaction.
pub async fn lock_restore_claim<C>(
    tx: &mut C,
    sandbox_id: SandboxId,
    restore_lease_token: Option<Uuid>,
) -> Result<bool, DbError>
where
    for<'a> &'a mut C: Executor<'a>,
{
    let locked: Option<bool> = sqlx::query_scalar(sql!(
        "SELECT TRUE
               FROM sandboxes
              WHERE id = $1
                AND lifecycle = 'restoring'
                AND ($2::uuid IS NULL OR restore_lease_token = $2)
                FOR UPDATE"
    ))
    .bind(sandbox_id)
    .bind(restore_lease_token)
    .fetch_optional(tx.into_sqlx())
    .await?;
    Ok(locked.is_some())
}

/// Lock the sandbox row `FOR UPDATE` until the caller's transaction ends and
/// read its `restore_lease_token`: `None` when no row exists, `Some(None)`
/// when the row carries no token.
///
/// The caller compares the token under the lock, so a cleanup by a lapsed
/// lease owner serializes against the lease takeover, which locks the row
/// before it writes a new token. The caller must pass its open transaction.
pub async fn lock_restore_lease_token<C>(
    tx: &mut C,
    sandbox_id: SandboxId,
) -> Result<Option<Option<Uuid>>, DbError>
where
    for<'a> &'a mut C: Executor<'a>,
{
    Ok(sqlx::query_scalar::<_, Option<Uuid>>(sql!(
        "SELECT restore_lease_token FROM sandboxes WHERE id = $1 FOR UPDATE"
    ))
    .bind(sandbox_id)
    .fetch_optional(tx.into_sqlx())
    .await?)
}

/// What [`restore_revert_evidence`] reads of one row: the active execution
/// the reverted claim created, if one is identifiable (its generation and
/// pod name), whether any active execution exists, and whether the
/// pre-restore source is proven restorable once the row is `suspended`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RestoreRevertEvidence {
    pub claimed_generation: Option<i64>,
    pub claimed_pod_name: Option<String>,
    pub any_active_execution: bool,
    pub source_proven: bool,
}

/// Read the evidence a restore revert decides on, after
/// [`lock_restore_claim`] in the caller's transaction. A plain read: it takes
/// no lock, and a missing row is an error
/// ([`DbErrorKind::NotFound`](pplx_space_postgres_pool::DbErrorKind::NotFound)).
///
/// The claimed execution is an active one created at or after the row's
/// `lifecycle_changed_at` that carries `activation_attempt` (its attempt id
/// and generation) or, with no attempt, any such execution when the claim
/// wrote `restore_lease_token`. Under the caller's row lock no other claim
/// can replace the execution, and `lifecycle_changed_at` still reads this
/// claim's `restoring` transition, so that execution is the one this claim
/// created. The source proof is
/// `pplx_space_sandbox_portability::restore_retry_suspended_revert_snapshot_exists`.
/// The caller must pass its open transaction.
pub async fn restore_revert_evidence<C>(
    tx: &mut C,
    sandbox_id: SandboxId,
    restore_lease_token: Option<Uuid>,
    activation_attempt: Option<(SandboxActivationAttemptId, i64)>,
) -> Result<RestoreRevertEvidence, DbError>
where
    for<'a> &'a mut C: Executor<'a>,
{
    /// Built once: every input is a literal or a bind placeholder.
    static QUERY: LazyLock<String> = LazyLock::new(|| {
        let proof = restore_retry_suspended_revert_snapshot_exists("s");
        sql!(format!(
            "SELECT claimed.generation,
                    claimed.pod_name,
                    EXISTS (
                        SELECT 1
                          FROM sandbox_k8s_executions e
                         WHERE e.sandbox_id = s.id
                           AND e.active
                    ),
                    {proof}
               FROM sandboxes s
               LEFT JOIN LATERAL (
                   SELECT e.generation, e.pod_name
                     FROM sandbox_k8s_executions e
                    WHERE e.sandbox_id = s.id
                      AND e.active
                      AND e.created_at >= s.lifecycle_changed_at
                      AND CASE
                              WHEN $3::uuid IS NOT NULL
                                  THEN e.activation_attempt_id = $3
                                       AND e.generation = $4
                              ELSE $2::uuid IS NOT NULL
                          END
               ) claimed ON TRUE
              WHERE s.id = $1"
        ))
    });
    let (claimed_generation, claimed_pod_name, any_active_execution, source_proven): (
        Option<i64>,
        Option<String>,
        bool,
        bool,
    ) = sqlx::query_as(sqlx::AssertSqlSafe(QUERY.as_str()))
        .bind(sandbox_id)
        .bind(restore_lease_token)
        .bind(activation_attempt.map(|(attempt, _)| attempt))
        .bind(activation_attempt.map(|(_, generation)| generation))
        .fetch_one(tx.into_sqlx())
        .await?;
    Ok(RestoreRevertEvidence {
        claimed_generation,
        claimed_pod_name,
        any_active_execution,
        source_proven,
    })
}

/// What [`restore_ownership_snapshot`] reads of one row: its lifecycle and
/// evacuation marker, and the seconds left on its restore lease and on its
/// active execution's ownership window, each floored at zero.
#[derive(Debug, Clone, PartialEq)]
pub struct RestoreOwnershipSnapshot {
    pub lifecycle: String,
    pub evacuation_restore_target: Option<String>,
    pub lease_remaining_secs: f64,
    pub execution_remaining_secs: f64,
}

/// Read the restore gate of one row, as a caller does after a restore claim
/// matched nothing. `None` when no row exists.
///
/// The execution's window counts only an active execution in `creating`,
/// `created`, `recovering` or `running`, and runs
/// `execution_ownership_ttl_secs` past its `updated_at`. A plain read: it
/// takes no lock, and both remainders are against the database's `NOW()`.
pub async fn restore_ownership_snapshot(
    executor: impl Executor<'_>,
    sandbox_id: SandboxId,
    execution_ownership_ttl_secs: i64,
) -> Result<Option<RestoreOwnershipSnapshot>, DbError> {
    let row: Option<(String, Option<String>, f64, f64)> = sqlx::query_as(sql!(
        "SELECT lifecycle,
                    evacuation_restore_target,
                    GREATEST(
                        EXTRACT(EPOCH FROM (restore_lease_expires_at - NOW())),
                        0.0
                    )::float8,
                    GREATEST(
                        CASE
                          WHEN e.state IN ('creating', 'created', 'recovering', 'running')
                          THEN EXTRACT(EPOCH FROM (
                              e.updated_at + make_interval(secs => $2) - NOW()
                          ))
                          ELSE 0.0
                        END,
                        0.0
                    )::float8
               FROM sandboxes s
               LEFT JOIN sandbox_k8s_executions e
                 ON e.sandbox_id = s.id AND e.active
              WHERE s.id = $1"
    ))
    .bind(sandbox_id.as_uuid())
    .bind(execution_ownership_ttl_secs)
    .fetch_optional(executor.into_sqlx())
    .await?;
    Ok(row.map(
        |(lifecycle, evacuation_restore_target, lease_remaining_secs, execution_remaining_secs)| {
            RestoreOwnershipSnapshot {
                lifecycle,
                evacuation_restore_target,
                lease_remaining_secs,
                execution_remaining_secs,
            }
        },
    ))
}

/// What [`observe_user_restores`] reads of one row: the
/// [`RestoreOwnershipSnapshot`] fields, and the active execution's
/// generation, state, pod name and pod IP (`host(pod_ip)`), all `None`
/// without one.
#[derive(Debug, Clone, PartialEq)]
pub struct UserRestoreObservationRow {
    pub sandbox_id: SandboxId,
    pub lifecycle: String,
    pub evacuation_restore_target: Option<String>,
    pub lease_remaining_secs: f64,
    pub execution_remaining_secs: f64,
    pub generation: Option<i64>,
    pub execution_state: Option<String>,
    pub pod_name: Option<String>,
    pub pod_ip: Option<String>,
}

/// Read the restore gate and active execution of every row in `sandbox_ids`,
/// in one statement, so each row's lifecycle, lease, marker and execution
/// are read together. Rows that don't exist are missing from the result.
///
/// The remainders are as [`restore_ownership_snapshot`] computes them. A
/// plain read: it takes no lock.
pub async fn observe_user_restores(
    executor: impl Executor<'_>,
    sandbox_ids: &[SandboxId],
    execution_ownership_ttl_secs: i64,
) -> Result<Vec<UserRestoreObservationRow>, DbError> {
    type Row = (
        Uuid,
        String,
        Option<String>,
        f64,
        f64,
        Option<i64>,
        Option<String>,
        Option<String>,
        Option<String>,
    );
    let rows = sqlx::query_as::<_, Row>(sql!(
        "SELECT s.id,
                    s.lifecycle,
                    s.evacuation_restore_target,
                    GREATEST(EXTRACT(EPOCH FROM (s.restore_lease_expires_at - NOW())), 0.0)::float8,
                    GREATEST(
                        CASE
                          WHEN e.state IN ('creating', 'created', 'recovering', 'running')
                          THEN EXTRACT(EPOCH FROM (
                              e.updated_at + make_interval(secs => $2) - NOW()
                          ))
                          ELSE 0.0
                        END,
                        0.0
                    )::float8,
                    e.generation,
                    e.state,
                    e.pod_name,
                    host(e.pod_ip)
               FROM sandboxes s
               LEFT JOIN sandbox_k8s_executions e
                 ON e.sandbox_id = s.id AND e.active
              WHERE s.id = ANY($1)"
    ))
    .bind(
        sandbox_ids
            .iter()
            .map(|sandbox_id| sandbox_id.as_uuid())
            .collect::<Vec<_>>(),
    )
    .bind(execution_ownership_ttl_secs)
    .fetch_all(executor.into_sqlx())
    .await?;
    Ok(rows
        .into_iter()
        .map(
            |(
                sandbox_id,
                lifecycle,
                evacuation_restore_target,
                lease_remaining_secs,
                execution_remaining_secs,
                generation,
                execution_state,
                pod_name,
                pod_ip,
            )| UserRestoreObservationRow {
                sandbox_id: SandboxId::from_uuid(sandbox_id),
                lifecycle,
                evacuation_restore_target,
                lease_remaining_secs,
                execution_remaining_secs,
                generation,
                execution_state,
                pod_name,
                pod_ip,
            },
        )
        .collect())
}
