//! The Direct substrate's statements on `sandboxes`. This module holds its
//! guarded writes: the restore lease's claim, renewal, release and
//! completion, the placement an owned restore receipt records, and the
//! adoption of a live node record by a row with no active node. Beside them
//! are the lease owner's row lock ([`lock_restore_lease_owner`]) and the two
//! unlocked reads of the lease's remaining time by the database clock that a
//! restore takes before and after its claim ([`unexpired_restore_lease`],
//! [`floored_restore_lease`]). Its child modules hold this crate's other
//! statements that join the Direct-only tables, each named for the question
//! it answers, whatever their caller; `pattern_ratchet.direct_tables` lets
//! this module and its children name those tables.
//!
//! The adoption's guard also reads the Direct-only route-identity and
//! provenance tables. The substrate takes the row lock and reads them with
//! [`adoption::lock_live_adoption_candidate`], one statement, first in the
//! adoption's transaction, decides from what it read and passes that;
//! [`adopt_live_execution`] guards on the row's own columns.
//!
//! The substrate keeps everything around them: the route-identity writes
//! beside the receipt placement, the adoption's decision from the generations
//! it read, the transactions, and the lease's duration. Each function runs on
//! the executor its caller passes, so it composes into the caller's
//! transaction when there is one; the lock verbs take the caller's
//! transaction or connection, so their locks last until the caller's
//! transaction ends. The two reads run on the pool, so `NOW()` is each read's
//! own instant. A guarded write that matches no row returns `false` (or
//! `None`), whether another writer won or the row never met the guard; the
//! caller decides what that means.
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

pub mod adoption;
pub mod node_endpoints;
pub mod observed_publication;
pub mod observed_reads;
pub mod restore_candidates;
pub mod route_reads;
pub mod service_route_reset;

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

/// Returns whether the row still holds the Direct restore lease `token`,
/// and when it does, holds the row's `FOR UPDATE` lock until the caller's
/// transaction ends. The guard is inside the locking statement, so `false`
/// means the row no longer holds the token or no longer exists.
///
/// The lease owner takes it before persisting a restore receipt's route
/// identity and placement ([`place_owned_restore_receipt`]) in the same
/// transaction.
pub async fn lock_restore_lease_owner(
    connection: &mut sqlx::PgConnection,
    sandbox_id: SandboxId,
    token: Uuid,
) -> Result<bool, sqlx::Error> {
    let owns_restore = sqlx::query_scalar::<_, bool>(sql!(
        "SELECT TRUE
               FROM sandboxes
              WHERE id = $1
                AND restore_lease_token = $2
              FOR UPDATE"
    ))
    .bind(sandbox_id.as_uuid())
    .bind(token)
    .fetch_optional(connection)
    .await?;
    Ok(owns_restore.is_some())
}

/// Observed projection: places the row on the node whose restore receipt the
/// lease owner just persisted, as both its active and target node. Returns
/// whether the row still held the lease.
///
/// Guarded by `token` alone, with no lifecycle predicate: the caller holds
/// the row lock [`lock_restore_lease_owner`] took while re-checking the same
/// token, and persists the receipt's route identity in the same transaction.
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

/// A row's lifecycle and the seconds its Direct restore lease has left
/// while unexpired, as [`unexpired_restore_lease`] reads them.
#[derive(Debug, Clone, PartialEq)]
pub struct UnexpiredRestoreLease {
    /// Raw `sandboxes.lifecycle`; the caller parses it.
    pub lifecycle: String,
    /// Seconds left on the lease by the database clock; `None` once it
    /// has expired or when the row holds no lease expiry.
    pub remaining_seconds: Option<f64>,
}

/// A row's lifecycle and the seconds its Direct restore lease has left,
/// floored at zero, as [`floored_restore_lease`] reads them.
#[derive(Debug, Clone, PartialEq)]
pub struct FlooredRestoreLease {
    /// Raw `sandboxes.lifecycle`; the caller parses it.
    pub lifecycle: String,
    /// Seconds left on the lease by the database clock, `0.0` once it
    /// has expired; `None` only when the row holds no lease expiry.
    pub remaining_seconds: Option<f64>,
}

/// The row's lifecycle, and the seconds its restore lease has left by the
/// database clock while the lease is unexpired (`None` once it has expired
/// or when the row holds no lease expiry). A plain read that takes no lock;
/// `None` when no such sandbox exists.
pub async fn unexpired_restore_lease<'e>(
    executor: impl PgExecutor<'e>,
    sandbox_id: SandboxId,
) -> Result<Option<UnexpiredRestoreLease>, sqlx::Error> {
    let row = sqlx::query_as::<_, (String, Option<f64>)>(sql!(
        "SELECT lifecycle,
                    CASE
                        WHEN restore_lease_expires_at > NOW() THEN
                            EXTRACT(EPOCH FROM (restore_lease_expires_at - NOW()))::float8
                        ELSE NULL
                    END
               FROM sandboxes
              WHERE id = $1"
    ))
    .bind(sandbox_id.as_uuid())
    .fetch_optional(executor)
    .await?;
    Ok(
        row.map(|(lifecycle, remaining_seconds)| UnexpiredRestoreLease {
            lifecycle,
            remaining_seconds,
        }),
    )
}

/// The row's lifecycle, and the seconds its restore lease has left by the
/// database clock, `0.0` once it has expired and `None` only when the row
/// holds no lease expiry, as a caller reads them after
/// [`claim_restore_lease`] claimed nothing. A plain read that takes no lock;
/// `None` when no such sandbox exists.
pub async fn floored_restore_lease<'e>(
    executor: impl PgExecutor<'e>,
    sandbox_id: SandboxId,
) -> Result<Option<FlooredRestoreLease>, sqlx::Error> {
    let row = sqlx::query_as::<_, (String, Option<f64>)>(sql!(
        "SELECT lifecycle,
                    CASE
                        WHEN restore_lease_expires_at IS NULL THEN NULL
                        ELSE GREATEST(
                            EXTRACT(EPOCH FROM (restore_lease_expires_at - NOW())),
                            0.0
                        )::float8
                    END
               FROM sandboxes
              WHERE id = $1"
    ))
    .bind(sandbox_id.as_uuid())
    .fetch_optional(executor)
    .await?;
    Ok(
        row.map(|(lifecycle, remaining_seconds)| FlooredRestoreLease {
            lifecycle,
            remaining_seconds,
        }),
    )
}
