//! The Kubernetes node inventory: every registered Kubernetes node with its
//! registration and tombstone.

use pplx_space_node_model::{NodeRole, SubstrateKind};
use pplx_space_postgres_pool::{DbError, Executor};
use pplx_space_sql_locator::sql;
use time::OffsetDateTime;

/// One registered Kubernetes node as the inventory lists it.
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct KubernetesInventoryRow {
    pub id: String,
    pub name: Option<String>,
    pub address: Option<String>,
    /// Empty when the node has no registered address.
    pub node_api_endpoint: String,
    pub role: String,
    pub managed_by: String,
    pub state: Option<String>,
    pub last_heartbeat: Option<OffsetDateTime>,
    pub created_at: OffsetDateTime,
    /// When the node's Kubernetes registration last changed.
    pub updated_at: OffsetDateTime,
    /// When the node was tombstoned.
    pub deleted_at: Option<OffsetDateTime>,
    pub metadata_json: String,
}

/// Registered Kubernetes nodes, optionally of one `role`, ordered by role,
/// Kubernetes node name and id. `include_deleted` keeps `deleted` and
/// tombstoned nodes.
///
/// The node-api endpoint is `endpoint_prefix`, the registered IP and
/// `node_api_port`; a node without an address gets an empty endpoint. It is
/// never derived from a Kubernetes node name.
pub async fn list_kubernetes_nodes(
    executor: impl Executor<'_>,
    role: Option<NodeRole>,
    include_deleted: bool,
    node_api_port: &str,
    endpoint_prefix: &str,
) -> Result<Vec<KubernetesInventoryRow>, DbError> {
    Ok(sqlx::query_as::<_, KubernetesInventoryRow>(sql!(
        "SELECT n.id::TEXT AS id,
                nk.k8s_node_name AS name,
                host(n.address) AS address,
                CASE WHEN n.address IS NULL THEN ''
                     WHEN family(n.address) = 6 THEN $4::TEXT || '[' || host(n.address) || ']:' || $3::TEXT
                     ELSE $4::TEXT || host(n.address) || ':' || $3::TEXT
                END AS node_api_endpoint,
                n.role, n.managed_by, n.state, n.last_heartbeat, n.created_at,
                nk.updated_at, t.terminated_at AS deleted_at, '{}'::TEXT AS metadata_json
         FROM nodes n
         JOIN node_k8s nk ON nk.node_id = n.id
         LEFT JOIN node_k8s_tombstones t ON t.node_id = n.id
         WHERE n.managed_by = $5
           AND ($1::TEXT IS NULL OR n.role = $1)
           AND ($2::BOOL OR (n.state IS DISTINCT FROM 'deleted' AND t.node_id IS NULL))
         ORDER BY n.role, nk.k8s_node_name, n.id"
    ))
    .bind(role.map(NodeRole::as_str))
    .bind(include_deleted)
    .bind(node_api_port)
    .bind(endpoint_prefix)
    .bind(SubstrateKind::Kubernetes)
    .fetch_all(executor.into_sqlx())
    .await?)
}
