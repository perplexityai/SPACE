use super::super::scripted_test_doubles::{
    ScriptedReply, ScriptedWorkerConfig, start_scripted_worker,
};
use super::super::tests::{node_api_pod_on_node, substrate_with_inventory_caches_and_pool};
use super::*;
use crate::controllers::substrate::{DurableSnapshotError, NodeApiTarget, SubstrateAdapter};
use pplx_space_common_core::types::{NodeManagedBy, NodeRole};
use pplx_space_public_api::OperationId;
use pplx_space_sbx_mgr_client::proto as sbx;

#[tokio::test]
async fn durable_copy_refusal_can_be_replayed_with_the_same_identity() -> TestResult {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }

    const COPY: &str = "WorkerApi/CreateSandboxSnapshot";
    let destination = "s3://snapshots/fork/destination";
    let refused = pplx_space_common_grpc::refused_before_start(
        tonic::Status::resource_exhausted("copy admission is busy"),
        Duration::from_secs(1),
    );
    let worker = start_scripted_worker(ScriptedWorkerConfig::new([(
        COPY,
        ScriptedReply::Sequence(std::sync::Arc::from([
            ScriptedReply::Status(refused),
            ScriptedReply::message(&sbx::CreateSnapshotResponse {
                published_durable_artifact: Some(sbx::PublishedDurableArtifact {
                    storage_uri: destination.to_string(),
                    bytes_published: 42,
                }),
                ..Default::default()
            }),
        ])),
    )]))
    .await?;
    let pool = sqlx::postgres::PgPoolOptions::new()
        .acquire_timeout(Duration::from_millis(1))
        .connect_lazy("postgres://postgres@127.0.0.1:1/postgres")?;
    let mut pod = node_api_pod_on_node("space-worker-node-a", WORKER_NODE);
    pod.status.as_mut().unwrap().pod_ip = Some("127.0.0.1".to_string());
    let mut substrate = substrate_with_inventory_caches_and_pool(vec![pod], Vec::new(), pool);
    substrate.k8s.node_api_port = worker.port;
    let target = NodeApiTarget {
        managed_by: NodeManagedBy::Kubernetes,
        role: NodeRole::Worker,
        direct_endpoint: None,
        substrate_node_id: Some(WORKER_NODE),
    };
    let operation_id = OperationId::new();
    let source = SandboxId::new();
    let child = SandboxId::new();
    let copy = || {
        substrate.create_snapshot_from_durable_artifact(
            &target,
            operation_id,
            source,
            child,
            "s3://snapshots/source".to_string(),
            destination.to_string(),
        )
    };

    assert!(matches!(
        copy().await,
        Err(DurableSnapshotError::Backpressure)
    ));
    assert_eq!(
        worker.received(COPY).len(),
        1,
        "RPC hops must not replay a refusal"
    );
    assert_eq!(copy().await?.storage_uri, destination);
    let requests = worker.received(COPY);
    assert_eq!(requests.len(), 2);
    assert_eq!(
        requests[0], requests[1],
        "retry must preserve the artifact and target identity"
    );
    Ok(())
}
