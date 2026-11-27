//! Kubernetes registrations for node inventory, not direct-node template polling.

use super::NodeRow;
use pplx_space_node_api_client::DEFAULT_PORT;
use pplx_space_node_model::NodeRole;
use pplx_space_node_registration_store::inventory::{self, KubernetesInventoryRow};

pub(super) async fn list_kubernetes(
    executor: impl pplx_space_postgres_pool::Executor<'_>,
    role: Option<NodeRole>,
    include_deleted: bool,
) -> Result<Vec<NodeRow>, sqlx::Error> {
    // Node APIs use the registered IP and platform port. Missing addresses stay
    // visible as inventory but grant no transfer authority. Never derive an
    // endpoint from a caller-supplied host, port, or Kubernetes node name.
    let rows = inventory::list_kubernetes_nodes(
        executor,
        role,
        include_deleted,
        &DEFAULT_PORT.to_string(),
        pplx_space_common_grpc::internal_transport::advertised_endpoint_prefix(),
    )
    .await?;
    Ok(rows
        .into_iter()
        .map(
            |KubernetesInventoryRow {
                 id,
                 name,
                 address,
                 node_api_endpoint,
                 role,
                 managed_by,
                 state,
                 last_heartbeat,
                 created_at,
                 updated_at,
                 deleted_at,
                 metadata_json,
             }| NodeRow {
                id,
                name,
                address,
                node_api_endpoint,
                role,
                managed_by,
                state,
                last_heartbeat,
                created_at,
                updated_at,
                deleted_at,
                metadata_json,
            },
        )
        .collect())
}
