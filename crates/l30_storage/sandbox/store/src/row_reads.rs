//! Reads of one `sandboxes` row that answer a question rather than return
//! the row: who owns it, whether it has prior state, whether it is started,
//! how long its restore lease has left, and whose activation its runtime
//! records. [`owner`] also
//! reads whether the owning organization brings its own key, and
//! [`home_node_v2_drain`] whether the node the row names is under a v2
//! drain, from `nodes`, which belongs to
//! `pplx-space-node-registration-store`.
//!
//! Plain reads: none takes a lock, and each runs on the executor its caller
//! passes.

use pplx_space_accounts::{ClientId, NodeId, OrgId, UserId};
use pplx_space_postgres_pool::{DbError, Executor};
use pplx_space_sandbox_model::SandboxId;
use pplx_space_sql_locator::sql;

/// Whether `sandbox_id` exists and belongs to `org_id`. A deleted row kept
/// for retention still counts.
pub async fn org_owns(
    executor: impl Executor<'_>,
    sandbox_id: SandboxId,
    org_id: OrgId,
) -> Result<bool, DbError> {
    Ok(sqlx::query_scalar::<_, bool>(sql!(
        "SELECT EXISTS(
             SELECT 1
             FROM sandboxes
             WHERE id = $1 AND org_id = $2
         )"
    ))
    .bind(sandbox_id)
    .bind(org_id)
    .fetch_one(executor.into_sqlx())
    .await?)
}

/// Whether `sandbox_id` has prior state: a current local
/// artifact, or a full snapshot that is `creating` or `ready`.
pub async fn prior_state_exists(
    executor: impl Executor<'_>,
    sandbox_id: SandboxId,
) -> Result<bool, DbError> {
    Ok(sqlx::query_scalar(sql!(
        "SELECT EXISTS (
                 SELECT 1
                   FROM sandboxes s
                  WHERE s.id = $1
                    AND (
                      s.current_local_artifact_id IS NOT NULL
                      OR EXISTS (
                          SELECT 1
                            FROM snapshots sn
                           WHERE sn.sandbox_id = s.id
                             AND sn.type = 'full'
                             AND sn.state IN ('creating', 'ready')
                      )
                    )
             )"
    ))
    .bind(sandbox_id)
    .fetch_one(executor.into_sqlx())
    .await?)
}

/// A sandbox's lifecycle and its runtime's activation record
/// (`activating_client_id`), read together.
#[derive(Clone, Debug, sqlx::FromRow)]
pub struct ActivationRecord {
    pub lifecycle: String,
    pub activating_client_id: Option<ClientId>,
}

/// Whether `sandbox_id` is `started`; `false` when the row is gone.
pub async fn is_started(
    executor: impl Executor<'_>,
    sandbox_id: SandboxId,
) -> Result<bool, DbError> {
    Ok(sqlx::query_scalar(sql!(
        "SELECT COALESCE((SELECT lifecycle = 'started' FROM sandboxes WHERE id = $1), FALSE)"
    ))
    .bind(sandbox_id)
    .fetch_one(executor.into_sqlx())
    .await?)
}

/// [`ActivationRecord`] of `sandbox_id`; `None` when the row is gone. See
/// [`crate::attribute_runtime`] for who writes the record.
pub async fn activation_record(
    executor: impl Executor<'_>,
    sandbox_id: SandboxId,
) -> Result<Option<ActivationRecord>, DbError> {
    Ok(sqlx::query_as::<_, ActivationRecord>(sql!(
        "SELECT lifecycle, activating_client_id FROM sandboxes WHERE id = $1"
    ))
    .bind(sandbox_id)
    .fetch_optional(executor.into_sqlx())
    .await?)
}

/// Seconds left on `sandbox_id`'s restore lease by the database clock:
/// `0.0` once it has expired or when the row holds no lease expiry
/// (`GREATEST` skips the NULL). Fails with a [`DbError`] of kind `NotFound`
/// (the driver's `RowNotFound`) when the row is gone.
pub async fn restore_lease_remaining_secs(
    executor: impl Executor<'_>,
    sandbox_id: SandboxId,
) -> Result<f64, DbError> {
    Ok(sqlx::query_scalar::<_, f64>(sql!(
        "SELECT GREATEST(
                                EXTRACT(EPOCH FROM (restore_lease_expires_at - NOW())),
                                0.0
                            )::float8
                       FROM sandboxes
                      WHERE id = $1"
    ))
    .bind(sandbox_id)
    .fetch_one(executor.into_sqlx())
    .await?)
}

/// A sandbox's owning user and organization, and whether that organization
/// brings its own key.
#[derive(Clone, Copy, Debug, PartialEq, Eq, sqlx::FromRow)]
pub struct OwnerRecord {
    pub user_id: UserId,
    pub org_id: OrgId,
    /// `organizations.byok_enabled`.
    pub byok_enabled: bool,
}

/// The sandbox's [`OwnerRecord`]. `None` when no such sandbox exists.
pub async fn owner(
    executor: impl Executor<'_>,
    sandbox_id: SandboxId,
) -> Result<Option<OwnerRecord>, DbError> {
    Ok(sqlx::query_as::<_, OwnerRecord>(sql!(
        "SELECT s.user_id, s.org_id, o.byok_enabled \
         FROM sandboxes s JOIN organizations o ON o.id = s.org_id \
         WHERE s.id = $1"
    ))
    .bind(sandbox_id)
    .fetch_optional(executor.into_sqlx())
    .await?)
}

/// The node `sandbox_id`'s row names (its `active_node_id`, else its
/// `target_node_id`), and whether a v2 drain of that node is recorded
/// (`drain_engine = 'v2'`). `None` when the row is gone; `Some((None,
/// false))` when it names no node, or one whose `nodes` row is gone.
pub async fn home_node_v2_drain(
    executor: impl Executor<'_>,
    sandbox_id: SandboxId,
) -> Result<Option<(Option<NodeId>, bool)>, DbError> {
    Ok(sqlx::query_as(sql!(
        "SELECT n.id, COALESCE(n.drain_engine = 'v2', FALSE)
               FROM sandboxes s
               LEFT JOIN nodes n ON n.id = COALESCE(s.active_node_id, s.target_node_id)
              WHERE s.id = $1"
    ))
    .bind(sandbox_id)
    .fetch_optional(executor.into_sqlx())
    .await?)
}

#[cfg(test)]
mod tests;
