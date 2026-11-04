//! Guarded writes of the Kubernetes substrate's restore lease on `sandboxes`:
//! the user and evacuation restore claims, the markerless re-drive's lease
//! claim, the owned completion and the lease release, and the reverts of an
//! abandoned claim.
//!
//! The restore lease is the pair `restore_lease_expires_at` /
//! `restore_lease_token`. A user claim writes both; the evacuation claim is
//! tokenless and is fenced by the `evacuation_restore_target` marker
//! instead. Leaving `restoring` clears the pair through the
//! `sandboxes_clear_restore_lease` trigger.
//!
//! The Kubernetes store keeps everything around these statements: the
//! revert's row lock (`SELECT ... FOR UPDATE`) and its evidence read, the
//! transactions, the execution retirement beside the revert, and the
//! ownership snapshot it reads when a claim matches nothing. A claim or
//! completion that matches no row is an outcome, not an error.

use pplx_space_common_core::types::{SandboxId, SandboxLifecycle, SubstrateKind};

use pplx_space_common_core::sql;
use pplx_space_sandbox_routability::routable_execution_or_non_k8s;
use sqlx::{PgExecutor, Postgres, Transaction};
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
    executor: impl PgExecutor<'_>,
    sandbox_id: SandboxId,
    lease_ttl_secs: i64,
    execution_ownership_ttl_secs: i64,
    evacuation_target: Option<SandboxLifecycle>,
) -> Result<Option<RestoreClaimRow>, sqlx::Error> {
    sqlx::query_as::<_, RestoreClaimRow>(sql!(
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
    .fetch_optional(executor)
    .await
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
    executor: impl PgExecutor<'_>,
    sandbox_id: SandboxId,
    lease_ttl_secs: i64,
    restore_lease_token: Uuid,
    execution_ownership_ttl_secs: i64,
    evacuation_target: Option<SandboxLifecycle>,
) -> Result<Option<RestoreClaimRow>, sqlx::Error> {
    sqlx::query_as::<_, RestoreClaimRow>(sql!(
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
    .fetch_optional(executor)
    .await
}

/// Take the restore lease of a markerless `restoring` row without moving its
/// lifecycle, so a retry may re-drive it. Writes an **operation step** (the
/// lease claim). Returns the claimed row, or `None` when the row isn't a
/// markerless `restoring` row, its lease is still live, or an active
/// execution did work within `execution_ownership_ttl_secs`.
pub async fn claim_markerless_restoring_lease(
    executor: impl PgExecutor<'_>,
    sandbox_id: SandboxId,
    lease_ttl_secs: i64,
    restore_lease_token: Uuid,
    execution_ownership_ttl_secs: i64,
) -> Result<Option<RestoreClaimRow>, sqlx::Error> {
    sqlx::query_as::<_, RestoreClaimRow>(sql!(
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
    .fetch_optional(executor)
    .await
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
    executor: impl PgExecutor<'_>,
    sandbox_id: SandboxId,
    restore_lease_token: Uuid,
) -> Result<bool, sqlx::Error> {
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
        .execute(executor)
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
    executor: impl PgExecutor<'_>,
    sandbox_id: SandboxId,
    restore_lease_token: Uuid,
) -> Result<(), sqlx::Error> {
    sqlx::query(sql!(
        "UPDATE sandboxes
                SET restore_lease_expires_at = NOW()
              WHERE id = $1
                AND restore_lease_token = $2"
    ))
    .bind(sandbox_id)
    .bind(restore_lease_token)
    .execute(executor)
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
pub async fn revert_restore_claim_retiring_placement(
    tx: &mut Transaction<'_, Postgres>,
    sandbox_id: SandboxId,
) -> Result<(), sqlx::Error> {
    sqlx::query(sql!(
        "UPDATE sandboxes
                        SET lifecycle = 'suspended',
                            placement = 'unplaced',
                            active_node_id = NULL,
                            placement_changed_at = NULL
                      WHERE id = $1"
    ))
    .bind(sandbox_id)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

/// Revert an abandoned restore claim with no execution at all to
/// `suspended`, clearing `placement_changed_at`. Writes an **operation step**
/// (the claim's compensation).
///
/// No lifecycle or lease predicate of its own; it relies on the caller's row
/// lock and on the caller's proof that the pre-restore source is still
/// restorable.
pub async fn revert_restore_claim_clearing_placement_clock(
    tx: &mut Transaction<'_, Postgres>,
    sandbox_id: SandboxId,
) -> Result<(), sqlx::Error> {
    sqlx::query(sql!(
        "UPDATE sandboxes
                        SET lifecycle = 'suspended',
                            placement_changed_at = NULL
                      WHERE id = $1"
    ))
    .bind(sandbox_id)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

/// Revert an abandoned restore claim's lifecycle to `suspended`, leaving
/// its execution, placement and placement clock as the restore left them.
/// Writes an **operation step** (the claim's compensation).
///
/// No lifecycle or lease predicate of its own; it relies on the caller's row
/// lock.
pub async fn revert_restore_claim_lifecycle(
    tx: &mut Transaction<'_, Postgres>,
    sandbox_id: SandboxId,
) -> Result<(), sqlx::Error> {
    sqlx::query(sql!(
        "UPDATE sandboxes SET lifecycle = 'suspended' WHERE id = $1"
    ))
    .bind(sandbox_id)
    .execute(&mut **tx)
    .await?;
    Ok(())
}
