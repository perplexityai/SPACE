//! Kubernetes registrations for node inventory, not direct-node template polling.

use super::NodeRow;
use pplx_space_common_core::{
    sql,
    types::{NodeRole, SubstrateKind},
};
use pplx_space_node_api_client::DEFAULT_PORT;
use sqlx::PgExecutor;

pub(super) async fn list_kubernetes(
    executor: impl PgExecutor<'_>,
    role: Option<NodeRole>,
    include_deleted: bool,
) -> Result<Vec<NodeRow>, sqlx::Error> {
    // Node APIs use the registered IP and platform port. Missing addresses stay
    // visible as inventory but grant no transfer authority. Never derive an
    // endpoint from a caller-supplied host, port, or Kubernetes node name.
    sqlx::query_as(sql!(
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
    .bind(DEFAULT_PORT.to_string())
    .bind(pplx_space_common_grpc::internal_transport::advertised_endpoint_prefix())
    .bind(SubstrateKind::Kubernetes)
    .fetch_all(executor)
    .await
}
