//! The Direct-node endpoint reads against Postgres.

use pplx_space_accounts::NodeId;
use pplx_space_db_test_fixtures::{TestDbResult, pg_test_db};
use pplx_space_node_model::{NodeRole, SubstrateKind};

use super::direct_seed::{direct_node, endpoint, node, snapshot, snapshot_reachable, worker};
use crate::direct::endpoints::{self, NodeEndpoint};

fn ids<I: IntoIterator<Item = NodeId>>(ids: I) -> std::collections::HashSet<NodeId> {
    ids.into_iter().collect()
}

#[tokio::test(flavor = "current_thread")]
async fn random_ready_returns_live_nodes_of_the_role_with_a_fresh_ready_snapshot()
-> TestDbResult<()> {
    let db = pg_test_db().await?;
    let pool = &db.pool;
    let ready = worker(pool, "ready").await?;
    snapshot(pool, ready, "ready", false).await?;
    let stale = worker(pool, "stale").await?;
    snapshot(pool, stale, "ready", true).await?;
    let full = worker(pool, "full").await?;
    snapshot(pool, full, "full", false).await?;
    let unreachable = worker(pool, "unreachable").await?;
    snapshot_reachable(pool, unreachable, "ready", false, false).await?;
    worker(pool, "unobserved").await?;
    let deleted = direct_node(pool, "worker", "registered", "deleted", true).await?;
    snapshot(pool, deleted, "ready", false).await?;
    let unregistered = direct_node(pool, "worker", "deleted", "unregistered", false).await?;
    snapshot(pool, unregistered, "ready", false).await?;
    let builder = direct_node(pool, "builder", "registered", "builder", false).await?;
    snapshot(pool, builder, "ready", false).await?;
    let k8s = node(pool, "worker", "k8s", Some("registered")).await?;
    snapshot(pool, k8s, "ready", false).await?;

    assert_eq!(
        endpoints::random_ready(pool, NodeRole::Worker, SubstrateKind::Space).await?,
        vec![NodeEndpoint {
            id: ready,
            node_api_endpoint: endpoint("ready"),
        }]
    );
    assert_eq!(
        ids(
            endpoints::random_ready(pool, NodeRole::Builder, SubstrateKind::Space)
                .await?
                .into_iter()
                .map(|node| node.id)
        ),
        ids([builder])
    );
    assert!(
        endpoints::random_ready(pool, NodeRole::Worker, SubstrateKind::Kubernetes)
            .await?
            .is_empty()
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn live_worker_and_builder_match_only_their_role_and_a_live_registry_row() -> TestDbResult<()>
{
    let db = pg_test_db().await?;
    let pool = &db.pool;
    // Neither reads `nodes.state` or a snapshot.
    let worker_id = direct_node(pool, "worker", "draining", "worker", false).await?;
    let builder_id = direct_node(pool, "builder", "registered", "builder", false).await?;
    let deleted_worker = direct_node(pool, "worker", "registered", "gone-w", true).await?;
    let deleted_builder = direct_node(pool, "builder", "registered", "gone-b", true).await?;

    let worker_row = NodeEndpoint {
        id: worker_id,
        node_api_endpoint: endpoint("worker"),
    };
    let builder_row = NodeEndpoint {
        id: builder_id,
        node_api_endpoint: endpoint("builder"),
    };
    assert_eq!(
        endpoints::live_worker(pool, worker_id, SubstrateKind::Space).await?,
        Some(worker_row)
    );
    assert_eq!(
        endpoints::live_builder(pool, builder_id, SubstrateKind::Space).await?,
        Some(builder_row)
    );
    assert_eq!(
        endpoints::live_worker(pool, builder_id, SubstrateKind::Space).await?,
        None
    );
    assert_eq!(
        endpoints::live_builder(pool, worker_id, SubstrateKind::Space).await?,
        None
    );
    assert_eq!(
        endpoints::live_worker(pool, deleted_worker, SubstrateKind::Space).await?,
        None
    );
    assert_eq!(
        endpoints::live_builder(pool, deleted_builder, SubstrateKind::Space).await?,
        None
    );
    assert_eq!(
        endpoints::live_worker(pool, worker_id, SubstrateKind::Kubernetes).await?,
        None
    );
    assert_eq!(
        endpoints::live_builder(pool, builder_id, SubstrateKind::Kubernetes).await?,
        None
    );
    assert_eq!(
        endpoints::live_worker(pool, NodeId::new(), SubstrateKind::Space).await?,
        None
    );
    Ok(())
}
