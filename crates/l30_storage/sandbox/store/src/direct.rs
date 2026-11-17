//! Guarded writes the Direct substrate makes to `sandboxes`: the restore
//! lease's claim, renewal, release and completion, the placement an owned
//! restore receipt records, and the adoption of a live node record by a row
//! with no active node.
//!
//! The adoption's guard also reads the Direct-only route-identity and
//! provenance tables, which `pattern_ratchet.direct_tables` confines to the
//! Direct substrate. The substrate evaluates that part and passes what it
//! decided; [`adopt_live_execution`] guards on the row's own columns.
//!
//! The substrate keeps everything around them: the row locks
//! (`SELECT ... FOR UPDATE` for restore ownership, `FOR NO KEY UPDATE` for
//! the adoption), the route-identity writes beside the receipt placement and
//! the reads the adoption decides from, the transactions, and the lease's
//! duration. Each function runs on the executor its caller passes, so it
//! composes into the caller's transaction when there is one. A guarded write
//! that matches no row returns `false` (or `None`), whether another writer
//! won or the row never met the guard; the caller decides what that means.
//!
//! Lease tokens need no lifecycle re-check of their own, because a row that
//! holds a token is `restoring`. The `sandboxes_clear_restore_lease` BEFORE
//! UPDATE trigger clears the token and its expiry on every update that leaves
//! the row outside `restoring` (compared case- and whitespace-insensitively),
//! and an insert that sets a token starts the row in `restoring`, as
//! `insert_unplaced` does.

use pplx_space_accounts::NodeId;
use pplx_space_node_model::SubstrateKind;
use pplx_space_sandbox_model::{SandboxId, SandboxLifecycle};

use pplx_space_sql_locator::sql;
use sqlx::PgExecutor;
use uuid::Uuid;

/// The restore lease [`claim_restore_lease`] took.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ClaimedRestoreLease {
    /// The token the claim wrote, as the statement returned it.
    pub token: Uuid,
    /// Whether the row was already `restoring` (an expired lease, or a row
    /// with none, taken over) rather than `suspended`.
    pub restore_retry: bool,
}

/// Operation step: claims the Direct restore lease, moving the row to
/// `restoring` with `token` and an expiry `lease_seconds` from the database
/// clock. Returns `None` when no lease was claimed.
///
/// Guarded in the statement: the row must be `suspended` or `restoring`
/// (compared case- and whitespace-insensitively) and hold no unexpired lease.
/// The candidate row is locked by the statement's own `FOR UPDATE`, so the
/// guard and the write see the same row version.
pub async fn claim_restore_lease<'e>(
    executor: impl PgExecutor<'e>,
    sandbox_id: SandboxId,
    lease_seconds: i64,
    token: Uuid,
) -> Result<Option<ClaimedRestoreLease>, sqlx::Error> {
    let claimed = sqlx::query_as::<_, (Uuid, bool)>(sql!(
        "WITH candidate AS (
                 SELECT id,
                        LOWER(TRIM(lifecycle)) = 'restoring' AS restore_retry
                   FROM sandboxes
                  WHERE id = $1
                    AND LOWER(TRIM(lifecycle)) IN ('suspended', 'restoring')
                    AND (restore_lease_expires_at IS NULL OR restore_lease_expires_at <= NOW())
                  FOR UPDATE
             )
             UPDATE sandboxes s
                SET lifecycle = 'restoring',
                    restore_lease_expires_at = NOW() + ($2::bigint * INTERVAL '1 second'),
                    restore_lease_token = $3
               FROM candidate c
              WHERE s.id = c.id
          RETURNING $3::UUID, c.restore_retry"
    ))
    .bind(sandbox_id)
    .bind(lease_seconds)
    .bind(token)
    .fetch_optional(executor)
    .await?;
    Ok(claimed.map(|(token, restore_retry)| ClaimedRestoreLease {
        token,
        restore_retry,
    }))
}

/// Operation step: ends a Direct restore lease that did not complete. Returns
/// whether the row still held the lease.
///
/// Guarded by `token` and `lifecycle = 'restoring'`. A first attempt
/// (`restore_retry` false) returns the row to `suspended`, which clears the
/// lease. A retry leaves the row, its lease and its expiry as they were: the
/// row was already `restoring` when the lease was claimed, and the lease
/// expires on its own; the write still matches and still fires the row's
/// update triggers.
pub async fn release_restore_lease<'e>(
    executor: impl PgExecutor<'e>,
    sandbox_id: SandboxId,
    token: Uuid,
    restore_retry: bool,
) -> Result<bool, sqlx::Error> {
    let released = sqlx::query(sql!(
        "UPDATE sandboxes
                SET lifecycle = CASE WHEN $3 THEN lifecycle ELSE 'suspended' END,
                    restore_lease_expires_at = CASE
                        WHEN $3 THEN restore_lease_expires_at
                        ELSE NULL
                    END,
                    restore_lease_token = CASE
                        WHEN $3 THEN restore_lease_token
                        ELSE NULL
                    END
              WHERE id = $1
                AND restore_lease_token = $2
                AND lifecycle = 'restoring'"
    ))
    .bind(sandbox_id)
    .bind(token)
    .bind(restore_retry)
    .execute(executor)
    .await?;
    Ok(released.rows_affected() == 1)
}

/// Operation step: extends a held Direct restore lease to `lease_seconds`
/// from the database clock. Returns whether the row still held the lease.
///
/// Guarded by `token` alone; a row that holds a token is `restoring` (see the
/// module docs).
pub async fn renew_restore_lease<'e>(
    executor: impl PgExecutor<'e>,
    sandbox_id: SandboxId,
    token: Uuid,
    lease_seconds: i64,
) -> Result<bool, sqlx::Error> {
    let renewed = sqlx::query(sql!(
        "UPDATE sandboxes
                SET restore_lease_expires_at = NOW() + ($3::bigint * INTERVAL '1 second')
              WHERE id = $1
                AND restore_lease_token = $2"
    ))
    .bind(sandbox_id)
    .bind(token)
    .bind(lease_seconds)
    .execute(executor)
    .await?;
    Ok(renewed.rows_affected() == 1)
}

/// Operation step: completes a Direct restore, moving the row to `started`
/// and `on_node`. Returns whether the row still held the lease.
///
/// Guarded by `token` alone. The statement does not name the lease columns:
/// leaving `restoring` clears them through the `sandboxes_clear_restore_lease`
/// trigger. It does not set the active node; the receipt placement
/// ([`place_owned_restore_receipt`]) did that earlier under the same lease.
pub async fn complete_restore_lease<'e>(
    executor: impl PgExecutor<'e>,
    sandbox_id: SandboxId,
    token: Uuid,
) -> Result<bool, sqlx::Error> {
    let completed = sqlx::query(sql!(
        "UPDATE sandboxes
                SET lifecycle = 'started',
                    placement = 'on_node'
              WHERE id = $1
                AND restore_lease_token = $2"
    ))
    .bind(sandbox_id)
    .bind(token)
    .execute(executor)
    .await?;
    Ok(completed.rows_affected() == 1)
}

/// Observed projection: places the row on the node whose restore receipt the
/// lease owner just persisted, as both its active and target node. Returns
/// whether the row still held the lease.
///
/// Guarded by `token` alone, with no lifecycle predicate: the caller holds
/// the row lock it took while re-checking the same token, and persists the
/// receipt's route identity in the same transaction.
pub async fn place_owned_restore_receipt<'e>(
    executor: impl PgExecutor<'e>,
    sandbox_id: SandboxId,
    node_id: NodeId,
    token: Uuid,
) -> Result<bool, sqlx::Error> {
    let placed = sqlx::query(sql!(
        "UPDATE sandboxes
                SET active_node_id = $2,
                    target_node_id = $2,
                    placement = 'on_node',
                    placement_changed_at = NOW()
              WHERE id = $1
                AND restore_lease_token = $3"
    ))
    .bind(sandbox_id)
    .bind(node_id)
    .bind(token)
    .execute(executor)
    .await?;
    Ok(placed.rows_affected() == 1)
}

/// Observed projection: places a Direct row with no active node back on the
/// node still running its execution, as both its active and target node.
/// Returns whether the row was placed.
///
/// The caller decides from the Direct route identity and provenance rows that
/// `node_id` runs the execution to adopt, and passes two facts derived from
/// them: `one_generation`, that the route identity, the execution and the last
/// reservation are one generation, and `started_now`, that the execution is
/// started and is the route identity's. It holds the row lock while it reads
/// them, in the transaction this runs in.
///
/// Guarded on the row's own columns: Direct's row, no active node, still in
/// `read_lifecycle` (compared case- and whitespace-insensitively), no live
/// restore lease, and either no restore-lease token or `one_generation`. The
/// row becomes `started` only when `started_now`; otherwise it keeps its
/// lifecycle, and a row that stays `restoring` keeps its token.
pub async fn adopt_live_execution<'e>(
    executor: impl PgExecutor<'e>,
    sandbox_id: SandboxId,
    node_id: NodeId,
    read_lifecycle: SandboxLifecycle,
    one_generation: bool,
    started_now: bool,
) -> Result<bool, sqlx::Error> {
    let adopted = sqlx::query(sql!(
        "UPDATE sandboxes
                SET active_node_id = $2,
                    target_node_id = $2,
                    placement = 'on_node',
                    placement_changed_at = NOW(),
                    lifecycle = CASE WHEN $5 THEN 'started' ELSE lifecycle END
              WHERE id = $1
                AND managed_by = $6
                AND active_node_id IS NULL
                AND LOWER(TRIM(lifecycle)) = $3
                AND (restore_lease_expires_at IS NULL OR restore_lease_expires_at <= NOW())
                AND (restore_lease_token IS NULL OR $4)"
    ))
    .bind(sandbox_id)
    .bind(node_id)
    .bind(read_lifecycle.as_str())
    .bind(one_generation)
    .bind(started_now)
    .bind(SubstrateKind::Space)
    .execute(executor)
    .await?;
    Ok(adopted.rows_affected() == 1)
}
