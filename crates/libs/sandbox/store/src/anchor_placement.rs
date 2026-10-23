//! Authoritative placement snapshot for releasing a sandbox's trust key.
use pplx_space_common_core::{sql, types::SandboxId};

#[derive(Clone, Debug, PartialEq, Eq, sqlx::FromRow)]
pub struct AnchorPlacement {
    pub node_name: String,
    pub node_uid: String,
    pub generation: Option<i64>,
}

/// An active execution, or an unclaimed warm-pool reservation. The pool path
/// never overrides an existing sandbox row, including a departed execution.
pub async fn load(
    executor: impl sqlx::PgExecutor<'_>,
    id: SandboxId,
) -> Result<Option<AnchorPlacement>, super::SandboxStoreError> {
    Ok(sqlx::query_as(sql!(
        "SELECT k.k8s_node_name AS node_name, k.k8s_node_uid AS node_uid,
                e.generation AS generation
         FROM sandboxes s
         JOIN node_k8s k ON k.node_id = s.active_node_id
         JOIN sandbox_k8s_executions e ON e.sandbox_id = s.id AND e.active
              AND e.k8s_node_name = k.k8s_node_name
         WHERE s.id = $1 AND k.k8s_node_uid IS NOT NULL"
    ))
    .bind(id)
    .fetch_optional(executor)
    .await?)
}

#[derive(Clone, Debug, PartialEq, Eq, sqlx::FromRow)]
pub struct WarmAnchorReservation {
    pub slot_id: uuid::Uuid,
    pub pod_name: String,
    pub pod_namespace: String,
    pub pod_uid: Option<String>,
}

pub async fn load_reservation(
    executor: impl sqlx::PgExecutor<'_>,
    id: SandboxId,
) -> Result<Option<WarmAnchorReservation>, super::SandboxStoreError> {
    Ok(sqlx::query_as(sql!(
        "SELECT id AS slot_id, pod_name, pod_namespace, pod_uid
         FROM kubernetes_pool_slots
         WHERE reserved_sandbox_id = $1 AND state IN ('warming', 'ready', 'claimed')
           AND NOT EXISTS (SELECT 1 FROM sandboxes s WHERE s.id = $1)"
    ))
    .bind(id)
    .fetch_optional(executor)
    .await?)
}

pub async fn node_is_current(
    executor: impl sqlx::PgExecutor<'_>,
    node_name: &str,
    node_uid: &str,
) -> Result<bool, super::SandboxStoreError> {
    Ok(sqlx::query_scalar(sql!(
        "SELECT EXISTS (SELECT 1 FROM node_k8s WHERE k8s_node_name = $1 AND k8s_node_uid = $2)"
    ))
    .bind(node_name)
    .bind(node_uid)
    .fetch_one(executor)
    .await?)
}

/// Lower bound across every non-tombstoned sandbox, including stopped,
/// suspended, quarantined and deleting identities, plus every pool reservation.
/// A deleted tombstone cannot wake under the same ID; forks use a new UUID.
/// Unknown legacy IDs or an empty inventory retain all seeds.
pub async fn oldest_identity_ms(
    executor: impl sqlx::PgExecutor<'_>,
) -> Result<Option<u64>, super::SandboxStoreError> {
    let (oldest, invalid): (Option<String>, i64) = sqlx::query_as(sql!(
        "WITH identities AS (
           SELECT id FROM sandboxes WHERE lifecycle <> 'deleted'
           UNION ALL
           SELECT reserved_sandbox_id AS id FROM kubernetes_pool_slots
             WHERE reserved_sandbox_id IS NOT NULL
         )
         SELECT MIN(id::text),
           COUNT(*) FILTER (
             WHERE SUBSTRING(id::text, 15, 1) <> '7'
                OR SUBSTRING(id::text, 20, 1) NOT IN ('8', '9', 'a', 'b')
           )
         FROM identities"
    ))
    .fetch_one(executor)
    .await?;
    if invalid != 0 {
        return Ok(None);
    }
    Ok(oldest
        .and_then(|id| uuid::Uuid::parse_str(&id).ok())
        .map(|id| {
            let b = id.as_bytes();
            u64::from_be_bytes([0, 0, b[0], b[1], b[2], b[3], b[4], b[5]])
        }))
}
