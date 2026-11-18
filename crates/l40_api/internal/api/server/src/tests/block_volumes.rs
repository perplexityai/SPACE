use super::*;
use crate::proto::{
    ActivateBlockVolumeRequest, ClaimBlockVolumeInitializationRequest,
    CompleteBlockVolumeInitializationRequest, PublishBlockVolumeCheckpointRequest,
    ReleaseBlockVolumeBindingRequest, ResolveBlockVolumeBindingRequest,
};
use pplx_space_execution_common::proto::BlockVolumeExecution;
use pplx_space_execution_spec::BlockVolumeMount;
#[cfg(feature = "kubernetes")]
use pplx_space_rpc_auth::{
    NodeApiAuthConfig, NodeApiAuthLayer, NodeRole as AuthRole,
    test_support::{
        pod_bound_service_account_status, projected_token, serve_token_reviews,
        service_account_status,
    },
};
use pplx_space_sandbox_store::block_volume::{self as volumes, VolumePhase};
use pplx_space_test_env::TestEnv;
#[cfg(feature = "kubernetes")]
use tokio_stream::wrappers::TcpListenerStream;

const ZONE: &str = "test-region-1a";
const INSTANCE: &str = "i-00000000000000001";
const CLOUD_DISK: &str = "vol-00000000000000001";

fn node_request<T>(body: T) -> Request<T> {
    request_from_node(body, TEST_WORKER_IP)
}

#[tokio::test]
#[cfg(feature = "kubernetes")]
async fn block_volume_pod_origin_requires_reviewed_live_node_incarnation() -> TestResult {
    let fixture = cluster_fixture().await?.expect("Postgres required");
    let pool = &fixture.service.pool;
    let node = NodeId::from_uuid(insert_k8s_node(pool, "192.0.2.10".parse()?, "worker").await?);
    let node_uid = Uuid::now_v7().to_string();
    sqlx::query(sql!(
        "INSERT INTO node_k8s (node_id, k8s_node_name, k8s_node_uid) VALUES ($1, $2, $3)"
    ))
    .bind(node)
    .bind("volume-worker")
    .bind(&node_uid)
    .execute(pool)
    .await?;
    let sandbox = SandboxId::new();
    let attempt = Uuid::now_v7();
    seed_active_k8s_sandbox(pool, sandbox, node).await?;
    seed_active_k8s_execution(pool, sandbox, 1, "192.0.2.20").await?;
    sqlx::query(sql!(
        "UPDATE sandbox_k8s_executions
         SET activation_attempt_id = $2, route_activation_id = $2 WHERE sandbox_id = $1"
    ))
    .bind(sandbox)
    .bind(attempt)
    .execute(pool)
    .await?;
    let tokens = ["bound", "stale", "missing", "malformed"].map(projected_token);
    let reviewed_tokens = tokens.clone();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let kube_config = kube::Config::new(format!("http://{}", listener.local_addr()?).parse()?);
    let _reviewer = tokio_util::task::AbortOnDropHandle::new(tokio::spawn(serve_token_reviews(
        listener,
        move |spec| {
            let index = reviewed_tokens
                .iter()
                .position(|token| Some(token) == spec.token.as_ref())
                .unwrap();
            let username = "system:serviceaccount:space:workers";
            if index == 2 {
                return service_account_status("space-cluster-api", username);
            }
            let uid = if index == 1 { "stale-uid" } else { &node_uid };
            let mut status = pod_bound_service_account_status(
                "space-cluster-api",
                username,
                "volume-worker",
                uid,
            );
            if index == 3 {
                status
                    .user
                    .as_mut()
                    .unwrap()
                    .extra
                    .as_mut()
                    .unwrap()
                    .insert(
                        "authentication.kubernetes.io/node-uid".into(),
                        vec![uid.into(), uid.into()],
                    );
            }
            status
        },
        tokio_util::sync::CancellationToken::new(),
    )));
    let mut env = TestEnv::default();
    for (key, value) in [
        ("SPACE_K8S_TOKEN_AUTH_ENABLED", "true"),
        ("SPACE_K8S_TOKEN_AUTH_STATIC_FALLBACK_ENABLED", "false"),
        ("SPACE_K8S_TOKEN_AUTH_NAMESPACE", "space"),
        ("KUBERNETES_SERVICE_HOST", "127.0.0.1"),
        ("SPACE_CLUSTER_API_AUTH_ALLOWED_SERVICE_ACCOUNTS", "workers"),
    ] {
        env.set(key, value);
    }
    let config = NodeApiAuthConfig::from_environment(&env, AuthRole::Cluster, String::new(), None)?;
    let auth =
        NodeApiAuthLayer::with_k8s_client_for_test(config, kube::Client::try_from(kube_config)?);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let endpoint = format!("http://{}", listener.local_addr()?);
    let _server = tokio_util::task::AbortOnDropHandle::new(tokio::spawn(async move {
        tonic::transport::Server::builder()
            .layer(auth)
            .add_service(crate::proto::cluster_api_server::ClusterApiServer::new(
                fixture.service,
            ))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
    }));
    let mut client = crate::proto::cluster_api_client::ClusterApiClient::connect(endpoint).await?;
    for (index, token) in tokens.iter().enumerate() {
        let mut request = Request::new(ResolveBlockVolumeBindingRequest {
            execution: Some(BlockVolumeExecution {
                node_id: node.to_string(),
                sandbox_id: sandbox.to_string(),
                execution_generation: 1,
                activation_attempt_id: attempt.to_string(),
            }),
        });
        request
            .metadata_mut()
            .insert("authorization", format!("Bearer {token}").parse()?);
        let response = client.resolve_block_volume_binding(request).await;
        if index == 0 {
            assert!(response?.into_inner().binding.is_none());
        } else {
            assert_eq!(response.unwrap_err().code(), Code::PermissionDenied);
        }
    }
    Ok(())
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
        "UPDATE sandbox_k8s_executions
         SET activation_attempt_id = $2, route_activation_id = $2 WHERE sandbox_id = $1"
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
    let mut fence = binding.fence.unwrap();
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
    // Normal activation completion releases the temporary claim, not the
    // resident execution's identity. Pause/resume must retain disk authority.
    sqlx::query(sql!(
        "UPDATE sandbox_k8s_executions SET activation_attempt_id = NULL WHERE sandbox_id = $1"
    ))
    .bind(sandbox)
    .execute(&pool)
    .await?;
    let mut wrong_owner = execution;
    wrong_owner.activation_attempt_id = Uuid::now_v7().to_string();
    assert_eq!(
        fixture
            .service
            .resolve_block_volume_binding(node_request(ResolveBlockVolumeBindingRequest {
                execution: Some(wrong_owner),
            }))
            .await
            .unwrap_err()
            .code(),
        Code::FailedPrecondition
    );
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

    // Stop the old execution without publishing a new checkpoint. Rebind the
    // same disk under a new control-plane execution and attachment epoch.
    fixture
        .service
        .release_block_volume_binding(node_request(ReleaseBlockVolumeBindingRequest {
            fence: Some(fence.clone()),
        }))
        .await?;
    let detached = volumes::get(&pool, sandbox).await?.unwrap();
    assert!(volumes::finish_detach(&pool, &detached).await?);
    let cold_attempt = Uuid::now_v7();
    sqlx::query(sql!(
        "UPDATE sandbox_k8s_executions
         SET generation = 2, route_activation_id = $2, activation_attempt_id = $2
         WHERE sandbox_id = $1 AND active"
    ))
    .bind(sandbox)
    .bind(cold_attempt)
    .execute(&pool)
    .await?;
    let cold_resolve = ResolveBlockVolumeBindingRequest {
        execution: Some(BlockVolumeExecution {
            node_id: node.to_string(),
            sandbox_id: sandbox.to_string(),
            execution_generation: 2,
            activation_attempt_id: cold_attempt.to_string(),
        }),
    };
    assert_eq!(
        fixture
            .service
            .resolve_block_volume_binding(node_request(cold_resolve.clone()))
            .await
            .unwrap_err()
            .code(),
        Code::Unavailable
    );
    let attaching = volumes::get(&pool, sandbox).await?.unwrap();
    assert!(volumes::observe(&pool, &attaching, VolumePhase::Attached, None, None).await?);
    let cold_binding = fixture
        .service
        .resolve_block_volume_binding(node_request(cold_resolve))
        .await?
        .into_inner()
        .binding
        .unwrap();
    assert!(cold_binding.initialization_ready);
    assert_eq!(
        cold_binding.filesystem_uuid,
        resumed.filesystem_uuid.to_string()
    );
    fence = cold_binding.fence.unwrap();
    let cold_activate = ActivateBlockVolumeRequest {
        fence: Some(fence.clone()),
        activation_id: cold_attempt.to_string(),
        checkpoint_id: None,
    };
    assert_eq!(
        fixture
            .service
            .activate_block_volume(node_request(cold_activate.clone()))
            .await
            .unwrap_err()
            .code(),
        Code::FailedPrecondition,
        "existing host bindings must not implicitly enable cold restart"
    );
    env.set(
        "SPACE_EBS_NODE_BINDINGS",
        serde_json::json!({
            node.to_string(): {
                "instance_id": INSTANCE,
                "availability_zone": ZONE,
                "retained_cold_start": true,
            }
        })
        .to_string(),
    );
    fixture.service.block_volume_hosts = crate::block_volumes::Hosts::from_environment(&env)?;
    assert_eq!(
        fixture
            .service
            .activate_block_volume(node_request(cold_activate))
            .await?
            .into_inner()
            .disk_generation,
        2
    );
    publish.fence = Some(fence.clone());
    publish.disk_generation = 2;
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
