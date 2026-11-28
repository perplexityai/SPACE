//! `sandbox_direct_route_identities`: the route a Direct sandbox's traffic
//! takes now, one row per sandbox.
//!
//! A row is written when the substrate records a route (a node's accepted
//! receipt, an owned restore's identity, or a pooled claim's) and deleted
//! when the substrate clears the route or with its `sandboxes` row; the
//! `sandboxes` row's lifecycle and placement stay with
//! `pplx-space-sandbox-store`.

use pplx_space_postgres_pool::{DbError, Executor};
use pplx_space_sandbox_model::{SandboxId, SandboxRouteIdentity};
use pplx_space_sql_locator::sql;
use sqlx::FromRow;
use uuid::Uuid;

/// A sandbox's stored route identity, as its columns hold it. The caller
/// validates it into a [`SandboxRouteIdentity`].
#[derive(Debug, Clone, PartialEq, Eq, FromRow)]
pub struct RouteIdentityRow {
    pub route_generation: i64,
    pub current_node_id: Uuid,
    pub substrate_kind: String,
    pub execution_generation: i64,
    pub activation_attempt_id: Uuid,
    pub pod_uid: Option<String>,
}

/// The sandbox's route identity, or `None` when it has none.
pub async fn get(
    executor: impl Executor<'_>,
    sandbox_id: SandboxId,
) -> Result<Option<RouteIdentityRow>, DbError> {
    Ok(sqlx::query_as::<_, RouteIdentityRow>(sql!(
        "SELECT route_generation,
                    current_node_id,
                    substrate_kind,
                    execution_generation,
                    activation_attempt_id,
                    pod_uid
               FROM sandbox_direct_route_identities
              WHERE sandbox_id = $1"
    ))
    .bind(sandbox_id.as_uuid())
    .fetch_optional(executor.into_sqlx())
    .await?)
}

/// Guarded write: stores `identity` as the sandbox's route unless the stored
/// one is newer. Returns `false`, and changes nothing, when a stored route has
/// a higher route or execution generation.
///
/// The guard compares generations only. Whatever else the write needs, such
/// as the restore lease it is made under, the caller checks first in the same
/// transaction. The caller must pass its open transaction.
pub async fn store_unless_newer<C>(
    tx: &mut C,
    identity: &SandboxRouteIdentity,
) -> Result<bool, DbError>
where
    for<'a> &'a mut C: Executor<'a>,
{
    let persisted = sqlx::query_scalar::<_, i64>(sql!(
        "INSERT INTO sandbox_direct_route_identities (
             sandbox_id, route_generation, current_node_id, substrate_kind,
             execution_generation, activation_attempt_id, pod_uid, updated_at
         )
         VALUES ($1, $2, $3, $4, $5, $6, $7, NOW())
         ON CONFLICT (sandbox_id) DO UPDATE
         SET route_generation = EXCLUDED.route_generation,
             current_node_id = EXCLUDED.current_node_id,
             substrate_kind = EXCLUDED.substrate_kind,
             execution_generation = EXCLUDED.execution_generation,
             activation_attempt_id = EXCLUDED.activation_attempt_id,
             pod_uid = EXCLUDED.pod_uid,
             updated_at = NOW()
         WHERE sandbox_direct_route_identities.route_generation <= EXCLUDED.route_generation
           AND sandbox_direct_route_identities.execution_generation <= EXCLUDED.execution_generation
         RETURNING execution_generation"
    ))
    .bind(identity.sandbox_id().as_uuid())
    .bind(identity.route_generation())
    .bind(identity.current_node_id().as_uuid())
    .bind(identity.substrate_kind().as_str())
    .bind(identity.execution_generation())
    .bind(identity.activation_attempt_id().as_uuid())
    .bind(identity.pod_uid().map(ToString::to_string))
    .fetch_optional(tx.into_sqlx())
    .await?;
    Ok(persisted.is_some())
}

/// Guarded write: accepts a node's receipt for `identity` as the sandbox's
/// route. Returns `false`, and changes nothing, when it is refused as stale:
///
/// - on the stored route's node, when the stored route generation is higher,
///   or the stored execution is newer (a higher execution generation, or the
///   same one with a later activation attempt);
/// - on another node, unless the `sandboxes` row's `active_node_id` is
///   already that node.
///
/// A sandbox with no stored route always accepts. The guard reads the
/// `sandboxes` row; a caller that changes it first, in the same transaction,
/// holds its row lock across this write. The caller must pass its open
/// transaction.
pub async fn accept_receipt<C>(tx: &mut C, identity: &SandboxRouteIdentity) -> Result<bool, DbError>
where
    for<'a> &'a mut C: Executor<'a>,
{
    let persisted = sqlx::query_scalar::<_, i64>(sql!(
        "INSERT INTO sandbox_direct_route_identities (
             sandbox_id,
             route_generation,
             current_node_id,
             substrate_kind,
             execution_generation,
             activation_attempt_id,
             pod_uid,
             updated_at
         )
         VALUES ($1, $2, $3, $4, $5, $6, $7, NOW())
         ON CONFLICT (sandbox_id) DO UPDATE
         SET route_generation = EXCLUDED.route_generation,
             current_node_id = EXCLUDED.current_node_id,
             substrate_kind = EXCLUDED.substrate_kind,
             execution_generation = EXCLUDED.execution_generation,
             activation_attempt_id = EXCLUDED.activation_attempt_id,
             pod_uid = EXCLUDED.pod_uid,
             updated_at = NOW()
         WHERE (
             sandbox_direct_route_identities.current_node_id = EXCLUDED.current_node_id
             AND sandbox_direct_route_identities.route_generation
                 <= EXCLUDED.route_generation
             AND (
                 sandbox_direct_route_identities.execution_generation
                     < EXCLUDED.execution_generation
                 OR (
                     sandbox_direct_route_identities.execution_generation
                         = EXCLUDED.execution_generation
                     AND sandbox_direct_route_identities.activation_attempt_id
                         <= EXCLUDED.activation_attempt_id
                 )
             )
         )
         OR (
             sandbox_direct_route_identities.current_node_id <> EXCLUDED.current_node_id
             AND EXISTS (
                 SELECT 1
                 FROM sandboxes s
                 WHERE s.id = EXCLUDED.sandbox_id
                   AND s.active_node_id = EXCLUDED.current_node_id
             )
         )
         RETURNING execution_generation"
    ))
    .bind(identity.sandbox_id().as_uuid())
    .bind(identity.route_generation())
    .bind(identity.current_node_id().as_uuid())
    .bind(identity.substrate_kind().as_str())
    .bind(identity.execution_generation())
    .bind(identity.activation_attempt_id().as_uuid())
    .bind(identity.pod_uid().map(ToString::to_string))
    .fetch_optional(tx.into_sqlx())
    .await?;
    Ok(persisted.is_some())
}

/// Deletes the sandbox's route identity, if it has one. Its provenance stays.
pub async fn delete(executor: impl Executor<'_>, sandbox_id: SandboxId) -> Result<(), DbError> {
    sqlx::query(sql!(
        "DELETE FROM sandbox_direct_route_identities
         WHERE sandbox_id = $1"
    ))
    .bind(sandbox_id.as_uuid())
    .execute(executor.into_sqlx())
    .await?;
    Ok(())
}
