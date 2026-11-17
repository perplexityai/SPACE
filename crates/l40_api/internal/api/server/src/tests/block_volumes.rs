use super::*;
use crate::proto::{
    ActivateBlockVolumeRequest, ClaimBlockVolumeInitializationRequest,
    CompleteBlockVolumeInitializationRequest, PublishBlockVolumeCheckpointRequest,
    ReleaseBlockVolumeBindingRequest, ResolveBlockVolumeBindingRequest,
};
use pplx_space_execution_common::proto::BlockVolumeExecution;
use pplx_space_execution_spec::BlockVolumeMount;
use pplx_space_sandbox_store::block_volume::{self as volumes, VolumePhase};
use pplx_space_test_env::TestEnv;

const ZONE: &str = "test-region-1a";
const INSTANCE: &str = "i-00000000000000001";
const CLOUD_DISK: &str = "vol-00000000000000001";

fn node_request<T>(body: T) -> Request<T> {
    request_from_node(body, TEST_WORKER_IP)
}

#[tokio::test]
async fn block_volume_grants_authenticate_origin_and_fence_progress_but_keep_deleted_owner_cleanup()
-> TestResult {
    let Some(mut fixture) = cluster_fixture().await? else {
        return Ok(());
    };
    let pool = fixture.service.pool.clone();
    let node = NodeId::new();
    let sandbox = SandboxId::new();
    let attempt = Uuid::now_v7();
    insert_ready_worker_node(&pool, node).await?;
    seed_active_k8s_sandbox(&pool, sandbox, node).await?;
    seed_active_k8s_execution(&pool, sandbox, 1, "192.0.2.20").await?;
    sqlx::query(sql!(
        "UPDATE sandbox_k8s_executions SET activation_attempt_id = $2 WHERE sandbox_id = $1"
    ))
    .bind(sandbox)
    .bind(attempt)
    .execute(&pool)
    .await?;
    let execution = BlockVolumeExecution {
        node_id: node.to_string(),
        sandbox_id: sandbox.to_string(),
        execution_generation: 1,
        activation_attempt_id: attempt.to_string(),
    };
    let resolve = ResolveBlockVolumeBindingRequest {
        execution: Some(execution.clone()),
    };
    assert!(
        fixture
            .service
            .resolve_block_volume_binding(node_request(resolve.clone()))
            .await?
            .into_inner()
            .binding
            .is_none(),
        "diskless executions do not require an experiment host grant"
    );
    let mut env = TestEnv::default();
    env.set(
        "SPACE_EBS_NODE_BINDINGS",
        serde_json::json!({
            node.to_string(): {"instance_id": INSTANCE, "availability_zone": ZONE}
        })
        .to_string(),
    );
    let hosts = crate::block_volumes::Hosts::from_environment(&env)?;
    let mount: BlockVolumeMount = serde_json::from_value(serde_json::json!({
        "type":"ebs", "size_gib":128, "permissions":"read_write", "mount":"/data"
    }))?;
    let mut tx = pool.begin().await?;
    volumes::reserve(
        &mut tx,
        volumes::NewBlockVolume {
            sandbox_id: sandbox,
            org_id: pplx_space_accounts::OrgId::from_uuid(SEEDED_ORG_ID),
            managed_by: "k8s",
            mount_spec: &mount,
            availability_zone: ZONE,
            resource_limit: 2,
        },
    )
    .await?;
    tx.commit().await?;
    let row = volumes::get(&pool, sandbox).await?.unwrap();
    assert!(volumes::observe(&pool, &row, VolumePhase::Available, Some(CLOUD_DISK), None).await?);
    assert_eq!(
        fixture
            .service
            .resolve_block_volume_binding(node_request(resolve.clone()))
            .await
            .unwrap_err()
            .code(),
        Code::PermissionDenied,
        "a retained disk still requires an experiment host grant"
    );
    fixture.service.block_volume_hosts = hosts;
    assert_eq!(
        fixture
            .service
            .resolve_block_volume_binding(Request::new(resolve.clone()))
            .await
            .unwrap_err()
            .code(),
        Code::PermissionDenied
    );
    assert_eq!(
        fixture
            .service
            .resolve_block_volume_binding(request_from_node(resolve.clone(), "192.0.2.99".parse()?))
            .await
            .unwrap_err()
            .code(),
        Code::PermissionDenied
    );
    let mut stale = execution.clone();
    stale.execution_generation += 1;
    assert_eq!(
        fixture
            .service
            .resolve_block_volume_binding(node_request(ResolveBlockVolumeBindingRequest {
                execution: Some(stale)
            }))
            .await
            .unwrap_err()
            .code(),
        Code::FailedPrecondition
    );
    assert_eq!(
        fixture
            .service
            .resolve_block_volume_binding(node_request(resolve.clone()))
            .await
            .unwrap_err()
            .code(),
        Code::Unavailable
    );
    let attaching = volumes::get(&pool, sandbox).await?.unwrap();
    assert_eq!(attaching.phase, VolumePhase::Attaching);
    assert_eq!(attaching.node_id, Some(node));
    assert!(volumes::observe(&pool, &attaching, VolumePhase::Attached, None, None).await?);
    let binding = fixture
        .service
        .resolve_block_volume_binding(node_request(resolve))
        .await?
        .into_inner()
        .binding
        .unwrap();
    assert_eq!(binding.aws_volume_id, CLOUD_DISK);
    assert!(!binding.initialization_ready);
    let fence = binding.fence.unwrap();
    let claim = Uuid::now_v7();
    let initialization = ClaimBlockVolumeInitializationRequest {
        fence: Some(fence.clone()),
        claim_id: claim.to_string(),
    };
    assert!(
        fixture
            .service
            .claim_block_volume_initialization(node_request(initialization.clone()))
            .await?
            .into_inner()
            .granted
    );
    assert!(
        !fixture
            .service
            .claim_block_volume_initialization(node_request(initialization))
            .await?
            .into_inner()
            .granted
    );
    let mut complete = CompleteBlockVolumeInitializationRequest {
        fence: Some(fence.clone()),
        claim_id: claim.to_string(),
        filesystem_uuid: Uuid::now_v7().to_string(),
    };
    assert_eq!(
        fixture
            .service
            .complete_block_volume_initialization(node_request(complete.clone()))
            .await
            .unwrap_err()
            .code(),
        Code::FailedPrecondition
    );
    complete.filesystem_uuid = binding.filesystem_uuid;
    fixture
        .service
        .complete_block_volume_initialization(node_request(complete))
        .await?;
    let activate = ActivateBlockVolumeRequest {
        fence: Some(fence.clone()),
        activation_id: attempt.to_string(),
        checkpoint_id: None,
    };
    let active = fixture
        .service
        .activate_block_volume(node_request(activate.clone()))
        .await?
        .into_inner();
    assert_eq!(active.disk_generation, 1);
    assert_eq!(
        fixture
            .service
            .activate_block_volume(node_request(activate.clone()))
            .await?
            .into_inner()
            .disk_generation,
        1
    );
    let checkpoint = Uuid::now_v7();
    let mut publish = PublishBlockVolumeCheckpointRequest {
        fence: Some(fence.clone()),
        disk_generation: 2,
        checkpoint_id: checkpoint.to_string(),
    };
    assert_eq!(
        fixture
            .service
            .publish_block_volume_checkpoint(node_request(publish.clone()))
            .await
            .unwrap_err()
            .code(),
        Code::FailedPrecondition
    );
    publish.disk_generation = 1;
    fixture
        .service
        .publish_block_volume_checkpoint(node_request(publish.clone()))
        .await?;
    fixture
        .service
        .activate_block_volume(node_request(activate))
        .await?;
    let resumed = volumes::get(&pool, sandbox).await?.unwrap();
    assert_eq!(resumed.phase, VolumePhase::Attached);
    assert_eq!(resumed.checkpoint_id, None);
    fixture
        .service
        .publish_block_volume_checkpoint(node_request(publish))
        .await?;
    // Publishing a checkpoint does not grant cloud detach.
    assert_eq!(
        volumes::get(&pool, sandbox).await?.unwrap().released_epoch,
        None
    );
    let mut tx = pool.begin().await?;
    pplx_space_sandbox_store::delete::delete_with_dependents(&mut tx, sandbox).await?;
    tx.commit().await?;
    let mut wrong_epoch = fence.clone();
    wrong_epoch.attachment_epoch += 1;
    assert_eq!(
        fixture
            .service
            .release_block_volume_binding(node_request(ReleaseBlockVolumeBindingRequest {
                fence: Some(wrong_epoch)
            }))
            .await
            .unwrap_err()
            .code(),
        Code::FailedPrecondition
    );
    fixture
        .service
        .release_block_volume_binding(node_request(ReleaseBlockVolumeBindingRequest {
            fence: Some(fence.clone()),
        }))
        .await?;
    let released = volumes::get(&pool, sandbox).await?.unwrap();
    assert!(released.delete_requested);
    assert_eq!(released.phase, VolumePhase::Detaching);
    assert_eq!(
        released.released_epoch,
        Some(i64::try_from(fence.attachment_epoch)?)
    );
    assert_eq!(released.aws_volume_id.as_deref(), Some(CLOUD_DISK));
    Ok(())
}
