//! The admin inventory listing and pod placement register a Kubernetes node
//! with the EC2 instance its Node's `providerID` names, as node sync does: a
//! Node under a new UID on another instance replaces the recorded instance,
//! and a Node that names no instance leaves the recorded one alone.

use k8s_openapi::api::core::v1::{
    Node, NodeAddress as K8sNodeAddress, NodeSpec as K8sNodeSpec, NodeStatus as K8sNodeStatus, Pod,
    PodSpec as K8sPodSpec,
};
use pplx_space_common_core::sql;
use pplx_space_common_core::types::{NodeId, NodeRole, SandboxId};
use pplx_space_test_env::TestResult;
use sqlx::PgPool;

use super::*;
use crate::controllers::substrate::kubernetes::store::PlacementRetryDisposition;

const NODE_NAME: &str = "space-test-instance-node";

/// The `providerID` each case gives the Node, and the instance its row must
/// name afterwards. Only an AWS `providerID` replaces the recorded `i-0a`.
const CASES: [(Option<&str>, &str); 3] = [
    (Some("aws:///us-west-2a/i-0b"), "i-0b"),
    (None, "i-0a"),
    (Some("kind://docker/space/space-test-instance-node"), "i-0a"),
];

/// The Node as registration now sees it: UID `uid-2`, and `provider_id`.
fn node(provider_id: Option<&str>) -> Node {
    Node {
        metadata: kube::api::ObjectMeta {
            name: Some(NODE_NAME.to_string()),
            uid: Some("uid-2".to_string()),
            ..Default::default()
        },
        spec: Some(K8sNodeSpec {
            provider_id: provider_id.map(str::to_string),
            ..Default::default()
        }),
        status: Some(K8sNodeStatus {
            addresses: Some(vec![K8sNodeAddress {
                type_: "InternalIP".to_string(),
                address: "10.0.0.70".to_string(),
            }]),
            ..Default::default()
        }),
    }
}

/// The node's rows as an earlier registration left them: UID `uid-1` on
/// instance `i-0a`.
async fn seed(pool: &PgPool) -> TestResult<NodeId> {
    let node_id = space_k8s_node_id(NODE_NAME, NodeRole::Worker.as_str());
    sqlx::query(sql!(
        "INSERT INTO nodes (id, address, role, managed_by, state)
         VALUES ($1, '10.0.0.70'::INET, 'worker', 'k8s', 'ready')"
    ))
    .bind(node_id)
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO node_k8s (node_id, k8s_node_name, k8s_node_uid, instance_id)
         VALUES ($1, $2, 'uid-1', 'i-0a')"
    ))
    .bind(node_id)
    .bind(NODE_NAME)
    .execute(pool)
    .await?;
    Ok(node_id)
}

type Recorded = (Option<String>, Option<String>);

/// The row's `(k8s_node_uid, instance_id)`.
async fn recorded(pool: &PgPool, node_id: NodeId) -> TestResult<Recorded> {
    Ok(sqlx::query_as(sql!(
        "SELECT k8s_node_uid, instance_id FROM node_k8s WHERE node_id = $1"
    ))
    .bind(node_id)
    .fetch_one(pool)
    .await?)
}

fn expected(instance_id: &str) -> Recorded {
    (Some("uid-2".to_string()), Some(instance_id.to_string()))
}

#[tokio::test(flavor = "current_thread")]
async fn inventory_listing_records_the_instance_its_node_names() -> TestResult {
    for (provider_id, instance_id) in CASES {
        let db = pplx_space_db_test_fixtures::pg_test_db().await?;
        let node_id = seed(&db.pool).await?;
        let substrate = substrate_with_inventory_caches_and_pool(
            vec![node_api_pod_on_node("space-worker-instance-pod", NODE_NAME)],
            vec![node(provider_id)],
            db.pool.clone(),
        );

        let entries = substrate
            .list_inventory_nodes(Some(NodeRole::Worker.as_str()))
            .await?
            .expect("kubernetes substrate supplies inventory");

        assert_eq!(
            entries.iter().map(|entry| entry.id).collect::<Vec<_>>(),
            [node_id],
            "{provider_id:?}"
        );
        assert_eq!(
            recorded(&db.pool, node_id).await?,
            expected(instance_id),
            "{provider_id:?}"
        );
    }
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn pod_placement_records_the_instance_its_node_names() -> TestResult {
    for (provider_id, instance_id) in CASES {
        let db = pplx_space_db_test_fixtures::pg_test_db().await?;
        let node_id = seed(&db.pool).await?;
        let substrate = substrate_with_inventory_caches_and_pool(
            Vec::new(),
            vec![node(provider_id)],
            db.pool.clone(),
        );
        let pod = SandboxPodView::from(Pod {
            metadata: kube::api::ObjectMeta {
                name: Some("space-sbx-instance-g1".to_string()),
                ..Default::default()
            },
            spec: Some(K8sPodSpec {
                node_name: Some(NODE_NAME.to_string()),
                ..Default::default()
            }),
            ..Default::default()
        });

        let placed = substrate
            .remember_pod_node_placement(
                SandboxId::new(),
                &pod,
                PlacementRetryDisposition::RetryConverges,
            )
            .await?;

        assert_eq!(placed, Some(node_id), "{provider_id:?}");
        assert_eq!(
            recorded(&db.pool, node_id).await?,
            expected(instance_id),
            "{provider_id:?}"
        );
    }
    Ok(())
}
