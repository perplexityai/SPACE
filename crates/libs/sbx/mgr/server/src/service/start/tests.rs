use pplx_space_common_core::types::{LocalArtifactId, SandboxId, SnapshotId};

use super::*;
use crate::db::CleanupCompletion;
use crate::service::recovery::tests::{insert_suspended_test_sandbox, recovery_test_service};
use crate::service::suspend_snapshot_publication::LOCAL_SNAPSHOT_DIR;
use pplx_space_common_core::types::ExecutionGeneration;

// A cold boot and a snapshot load give a sandbox the same rootfs limiter,
// sized by its row's assignment, with both buckets holding the configured
// burst; builders are exempt.
#[tokio::test]
async fn rootfs_rate_limiter_uses_the_assignment_and_configured_burst() {
    let service = SandboxManagerService::new_with_config(
        crate::db::test_conn(),
        crate::service::SandboxManagerServiceOptions::for_test(),
        crate::config::SandboxConfig {
            disk_io_burst_secs: std::num::NonZeroU32::new(3),
            ..Default::default()
        },
    );
    let limits = pplx_space_common_core::types::DiskIoLimits::try_new(5_000, 200).unwrap();
    let limiter = service
        .rootfs_rate_limiter(false, limits)
        .expect("sandbox is limited");

    // Runtime-core boot adapters cover device serialization. This layer owns
    // the assignment and the snapshot-load request.
    assert_eq!(limiter.bandwidth.size, 629_145_600);
    assert_eq!(limiter.bandwidth.refill_time_ms, 3000);
    assert_eq!(limiter.operations.size, 15_000);
    let loaded = drive_rate_limit_proto(limiter);
    assert_eq!(
        loaded.operations,
        Some(pplx_space_node_mgr_client::proto::TokenBucket {
            size: 15_000,
            refill_time_ms: 3_000,
        })
    );
    assert_eq!(
        loaded.bandwidth,
        Some(pplx_space_node_mgr_client::proto::TokenBucket {
            size: 629_145_600,
            refill_time_ms: 3_000,
        })
    );

    assert_eq!(service.rootfs_rate_limiter(true, limits), None);
}
use pplx_space_common_core::types::{
    NodeId, NodeManagedBy, RouteGeneration, SandboxActivationAttemptId,
};

fn restore_artifact_as_new_request() -> StartRequest {
    let source_sandbox_id = SandboxId::new();
    let artifact_snapshot_id = SnapshotId::new();
    StartRequest {
        sandbox_id: SandboxId::new().to_string(),
        suspend_restore_storage_uri: Some(format!(
            "s3://snapshots/suspend/{source_sandbox_id}/{artifact_snapshot_id}?region=us-east-1"
        )),
        route_identity: Some(crate::proto::SpaceApiRouteIdentity {
            execution_generation: Some(7),
            ..Default::default()
        }),
        restore_artifact_as_new: Some(crate::proto::RestoreArtifactAsNewContext {
            source_sandbox_id: source_sandbox_id.to_string(),
            artifact_snapshot_id: artifact_snapshot_id.to_string(),
            target_owner: format!("user:{}", uuid::Uuid::now_v7()),
            artifact_kind: crate::proto::ArtifactKind::Disk as i32,
            disk_io_max_operations_per_sec: None,
            disk_io_max_bandwidth_mib_per_sec: None,
            volume_encryption: false,
        }),
        ..Default::default()
    }
}

#[test]
fn restore_artifact_as_new_parses_target_disk_io_assignment() {
    let mut request = restore_artifact_as_new_request();
    let restore = request.restore_artifact_as_new.as_mut().unwrap();
    restore.disk_io_max_operations_per_sec = Some(5_000);
    restore.disk_io_max_bandwidth_mib_per_sec = Some(200);

    let context = StartSandboxContext::from_request(&request).unwrap();

    assert_eq!(
        context
            .restore_artifact_as_new
            .unwrap()
            .target_disk_io_limits,
        Some(pplx_space_common_core::types::DiskIoLimits::try_new(5_000, 200).unwrap())
    );
}

#[test]
fn restore_artifact_as_new_rejects_partial_disk_io_assignment() {
    let mut request = restore_artifact_as_new_request();
    request
        .restore_artifact_as_new
        .as_mut()
        .unwrap()
        .disk_io_max_operations_per_sec = Some(5_000);

    let error = match StartSandboxContext::from_request(&request) {
        Ok(_) => panic!("partial disk I/O assignment must fail at the request boundary"),
        Err(error) => error,
    };

    assert_eq!(error.code(), tonic::Code::InvalidArgument);
    assert!(error.message().contains("must either both be present"));
}

#[test]
fn restore_artifact_as_new_rejects_invalid_snapshot_id_at_request_boundary() {
    let mut request = restore_artifact_as_new_request();
    request
        .restore_artifact_as_new
        .as_mut()
        .unwrap()
        .artifact_snapshot_id = "not-a-snapshot-id".to_string();

    let error = match StartSandboxContext::from_request(&request) {
        Ok(_) => panic!("invalid snapshot identity must fail before restore I/O"),
        Err(error) => error,
    };

    assert_eq!(error.code(), tonic::Code::InvalidArgument);
}

#[test]
fn restore_artifact_as_new_normalizes_route_generation_before_validation() {
    let mut request = restore_artifact_as_new_request();
    request.route_identity = Some(crate::proto::SpaceApiRouteIdentity {
        route_generation: Some(7),
        ..Default::default()
    });

    let context = StartSandboxContext::from_request(&request)
        .expect("route generation supplies the restore execution generation");

    assert_eq!(
        context.execution_generation.map(ExecutionGeneration::get),
        Some(7)
    );
}

#[test]
fn missing_checkpoint_restore_requires_caller_route_provenance() {
    let error = resolve_missing_checkpoint_restore_route_identity(
        SandboxId::new(),
        SpaceApiRouteIdentity::default(),
    )
    .expect_err("a missing local row cannot safely infer an earlier generation");

    assert_eq!(error.code(), tonic::Code::FailedPrecondition);
    assert!(error.message().contains("authoritative route identity"));
}

#[test]
fn missing_checkpoint_restore_preserves_caller_generation() {
    let sandbox_id = SandboxId::new();
    let identity = resolve_missing_checkpoint_restore_route_identity(
        sandbox_id,
        SpaceApiRouteIdentity {
            sandbox_id: Some(sandbox_id),
            route_generation: Some(RouteGeneration::new(8).unwrap()),
            current_node_id: Some(NodeId::new()),
            substrate_kind: Some(NodeManagedBy::Space),
            execution_generation: Some(ExecutionGeneration::new(8).unwrap()),
            activation_attempt_id: Some(SandboxActivationAttemptId::new()),
            ..Default::default()
        },
    )
    .expect("the control plane supplies missing-row provenance");

    assert_eq!(
        identity.execution_generation.map(ExecutionGeneration::get),
        Some(8)
    );
}

#[test]
fn missing_checkpoint_restore_accepts_complete_kubernetes_identity() {
    let sandbox_id = SandboxId::new();
    let identity = resolve_missing_checkpoint_restore_route_identity(
        sandbox_id,
        SpaceApiRouteIdentity {
            sandbox_id: Some(sandbox_id),
            route_generation: Some(RouteGeneration::new(8).unwrap()),
            current_node_id: Some(NodeId::new()),
            substrate_kind: Some(NodeManagedBy::Kubernetes),
            execution_generation: Some(ExecutionGeneration::new(8).unwrap()),
            activation_attempt_id: Some(SandboxActivationAttemptId::new()),
            pod_uid: Some("019f8000-0000-7000-8000-0000000000cc".parse().unwrap()),
        },
    )
    .expect("a complete Kubernetes identity is authoritative for a missing-row restore");

    assert_eq!(identity.substrate_kind, Some(NodeManagedBy::Kubernetes));
    assert!(identity.pod_uid.is_some());
}

#[test]
fn restore_artifact_as_new_rejects_invalid_owner_at_request_boundary() {
    let mut request = restore_artifact_as_new_request();
    request
        .restore_artifact_as_new
        .as_mut()
        .unwrap()
        .target_owner = String::new();

    let error = match StartSandboxContext::from_request(&request) {
        Ok(_) => panic!("invalid target owner must fail before restore I/O"),
        Err(error) => error,
    };

    assert_eq!(error.code(), tonic::Code::InvalidArgument);
}

#[test]
fn restore_artifact_as_new_rejects_uri_identity_mismatch_at_request_boundary() {
    let mut request = restore_artifact_as_new_request();
    request
        .restore_artifact_as_new
        .as_mut()
        .unwrap()
        .artifact_snapshot_id = SnapshotId::new().to_string();

    let error = match StartSandboxContext::from_request(&request) {
        Ok(_) => panic!("snapshot URI mismatch must fail before restore I/O"),
        Err(error) => error,
    };

    assert_eq!(error.code(), tonic::Code::InvalidArgument);
}

#[test]
fn restore_artifact_as_new_rejects_uri_source_mismatch_at_request_boundary() {
    let mut request = restore_artifact_as_new_request();
    let artifact_snapshot_id = request
        .restore_artifact_as_new
        .as_ref()
        .unwrap()
        .artifact_snapshot_id
        .clone();
    request.suspend_restore_storage_uri = Some(format!(
        "s3://snapshots/suspend/{}/{artifact_snapshot_id}?region=us-east-1",
        SandboxId::new()
    ));

    let error = match StartSandboxContext::from_request(&request) {
        Ok(_) => panic!("source sandbox URI mismatch must fail before restore I/O"),
        Err(error) => error,
    };

    assert_eq!(error.code(), tonic::Code::InvalidArgument);
}

#[test]
fn restoring_artifact_as_new_rematerializes_without_using_suspend_recovery() {
    assert_eq!(
        interrupted_restore_action("", SandboxState::Restoring, None, true),
        InterruptedRestoreAction::RematerializeArtifact,
    );
}
fn write_cache_file(cache_dir: &std::path::Path, sandbox_id: &str, relative_path: &str) {
    let path = cache_dir.join(sandbox_id).join(relative_path);
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(path, b"artifact").unwrap();
}

fn write_suspend_snapshot_files(
    cache_dir: &std::path::Path,
    sandbox_id: &str,
    local_dir: &str,
    artifacts: &[crate::snapshot_upload::Artifact],
) {
    for artifact in artifacts {
        write_cache_file(
            cache_dir,
            sandbox_id,
            &format!("{local_dir}/{}", artifact.local_name),
        );
    }
}

#[tokio::test]
async fn restore_rootfs_policy_failure_is_permanent_not_a_retry_response() {
    let directory = tempfile::tempdir().unwrap();
    let sandbox_id = SandboxId::new().to_string();
    let mut provenance = RestoredSnapshotProvenance::default();
    let error = stage_remote_suspend_restore_if_needed(
        &sandbox_id,
        directory.path(),
        SuspendRestorePlacement::RemoteOnly,
        Some("s3://test/selected-snapshot?region=us-east-1"),
        &mut provenance,
        |_, _| async {
            Err(crate::snapshot_restore::RestoreError::RootfsPolicy(
                pplx_space_common_core::sandbox_storage::RootfsSizeError::TooLarge {
                    requested_bytes: u64::MAX,
                    maximum_bytes: 128 * 1024 * 1024 * 1024,
                },
            ))
        },
    )
    .await
    .unwrap_err();
    assert_eq!(error.code(), tonic::Code::FailedPrecondition);
    assert!(provenance.storage_uri.is_none());
}

async fn run_selected_remote_stage_for_test(
    cache_dir: &std::path::Path,
    sandbox_id: &str,
    storage_uri: &str,
    mut restored_snapshot: RestoredSnapshotProvenance,
    stage_local_artifact_id: Option<LocalArtifactId>,
) -> (Vec<(String, Option<String>)>, RestoredSnapshotProvenance) {
    let stage_calls = Arc::new(std::sync::Mutex::new(Vec::new()));
    stage_remote_suspend_restore_if_needed(
        sandbox_id,
        cache_dir,
        SuspendRestorePlacement::RemoteOnly,
        Some(storage_uri),
        &mut restored_snapshot,
        {
            let stage_calls = Arc::clone(&stage_calls);
            move |sandbox_id, storage_uri| {
                stage_calls
                    .lock()
                    .expect("stage call lock poisoned")
                    .push((sandbox_id, storage_uri.clone()));
                let storage_uri = storage_uri.expect("selected restore URI");
                async move {
                    Ok(crate::snapshot_restore::RestorePrepared {
                        snapshot_dir: std::path::PathBuf::from("/unused/snapshot"),
                        storage_uri,
                        local_artifact_id: stage_local_artifact_id,
                        artifact_kind: stage_local_artifact_id
                            .map(|_| pplx_space_common_core::types::ArtifactKind::Full),
                    })
                }
            }
        },
    )
    .await
    .expect("selected remote restore stage decision should succeed");
    let stage_calls = Arc::try_unwrap(stage_calls)
        .expect("stage call tracker still shared")
        .into_inner()
        .expect("stage call lock poisoned");
    (stage_calls, restored_snapshot)
}

#[test]
fn local_suspend_snapshot_materialized_accepts_plaintext_snapshot() {
    let tmp = tempfile::tempdir().unwrap();
    let sandbox_id = "019d84bd-280d-7331-99c6-b63377f3a3a2";
    write_suspend_snapshot_files(
        tmp.path(),
        sandbox_id,
        LOCAL_SNAPSHOT_DIR,
        crate::snapshot_upload::COMMON_VM_ARTIFACTS,
    );

    let materialized = local_suspend_snapshot_materialized(tmp.path(), sandbox_id);

    assert!(materialized);
}

#[test]
fn local_suspend_snapshot_materialized_rejects_partial_plaintext_snapshot() {
    let tmp = tempfile::tempdir().unwrap();
    let sandbox_id = "019d84bd-280d-7331-99c6-b63377f3a3a2";
    let first_artifact = crate::snapshot_upload::COMMON_VM_ARTIFACTS
        .iter()
        .next()
        .unwrap();
    write_cache_file(
        tmp.path(),
        sandbox_id,
        &format!("{LOCAL_SNAPSHOT_DIR}/{}", first_artifact.local_name),
    );

    let materialized = local_suspend_snapshot_materialized(tmp.path(), sandbox_id);

    assert!(!materialized);
}

#[test]
fn local_suspend_snapshot_materialized_accepts_same_node_encrypted_snapshot() {
    let tmp = tempfile::tempdir().unwrap();
    let sandbox_id = "019d84bd-280d-7331-99c6-b63377f3a3a2";
    let encrypted_artifacts = crate::snapshot_upload::encrypted_vm_artifacts();
    write_suspend_snapshot_files(
        tmp.path(),
        sandbox_id,
        LOCAL_INSTANCE_DIR,
        &encrypted_artifacts,
    );

    let materialized = local_suspend_snapshot_materialized(tmp.path(), sandbox_id);

    assert!(materialized);
}

#[test]
fn local_suspend_snapshot_materialized_accepts_cross_node_encrypted_snapshot() {
    let tmp = tempfile::tempdir().unwrap();
    let sandbox_id = "019d84bd-280d-7331-99c6-b63377f3a3a2";
    let encrypted_artifacts = crate::snapshot_upload::encrypted_vm_artifacts();
    write_suspend_snapshot_files(
        tmp.path(),
        sandbox_id,
        LOCAL_SNAPSHOT_DIR,
        &encrypted_artifacts,
    );

    let materialized = local_suspend_snapshot_materialized(tmp.path(), sandbox_id);

    assert!(materialized);
}

#[tokio::test]
async fn selected_remote_stage_skips_when_cross_node_encrypted_snapshot_already_staged() {
    let tmp = tempfile::tempdir().unwrap();
    let sandbox_id = SandboxId::new().to_string();
    let storage_uri = format!("s3://space-test/suspend_snapshots/{sandbox_id}?region=us-east-1");
    let local_artifact_id = LocalArtifactId::new();
    let encrypted_artifacts = crate::snapshot_upload::encrypted_vm_artifacts();
    write_suspend_snapshot_files(
        tmp.path(),
        &sandbox_id,
        LOCAL_SNAPSHOT_DIR,
        &encrypted_artifacts,
    );
    let restored_snapshot = RestoredSnapshotProvenance {
        storage_uri: Some(storage_uri.clone()),
        local_artifact_id: Some(local_artifact_id.to_string()),
    };

    let (stage_calls, restored_snapshot) = run_selected_remote_stage_for_test(
        tmp.path(),
        &sandbox_id,
        &storage_uri,
        restored_snapshot,
        None,
    )
    .await;

    assert!(stage_calls.is_empty());
    assert_eq!(restored_snapshot.storage_uri, Some(storage_uri));
    assert_eq!(
        restored_snapshot.local_artifact_id,
        Some(local_artifact_id.to_string())
    );
}

#[test]
fn local_suspend_snapshot_materialized_rejects_partial_encrypted_snapshot() {
    let tmp = tempfile::tempdir().unwrap();
    let sandbox_id = "019d84bd-280d-7331-99c6-b63377f3a3a2";
    let encrypted_artifacts = crate::snapshot_upload::encrypted_vm_artifacts();
    let first_artifact = encrypted_artifacts.first().unwrap();
    write_cache_file(
        tmp.path(),
        sandbox_id,
        &format!("{LOCAL_SNAPSHOT_DIR}/{}", first_artifact.local_name),
    );

    let materialized = local_suspend_snapshot_materialized(tmp.path(), sandbox_id);

    assert!(!materialized);
}

#[test]
fn local_restore_keeps_local_generation_without_a_selected_remote_uri() {
    let decision = decide_suspend_restore(SuspendRestorePlacement::Legacy, false, false);

    assert_eq!(decision, SuspendRestoreDecision::UseLocal);
}

#[test]
fn selected_remote_uri_keeps_local_generation_on_the_trusted_source_node() {
    let decision = decide_suspend_restore(SuspendRestorePlacement::SourceNode, true, false);

    assert_eq!(decision, SuspendRestoreDecision::UseLocal);
}

#[test]
fn selected_remote_uri_replaces_residual_local_generation_on_another_node() {
    let decision = decide_suspend_restore(SuspendRestorePlacement::OtherNode, true, false);

    assert_eq!(
        decision,
        SuspendRestoreDecision::StageRemote(RestoreSourceSelection::RemoteUriSelected)
    );
}

#[test]
fn source_node_without_local_bytes_resolves_remote_from_cluster_api() {
    // Local suspend snapshot missing on the source node with no URI threaded
    // into the request: route to a remote stage so the latest ready backup is
    // resolved from cluster-api, instead of failing closed.
    let decision = decide_suspend_restore(SuspendRestorePlacement::SourceNode, false, true);

    assert_eq!(
        decision,
        SuspendRestoreDecision::StageRemote(RestoreSourceSelection::RemoteLocalFilesMissing)
    );
}

#[test]
fn other_node_without_remote_uri_resolves_remote_from_cluster_api() {
    let decision = decide_suspend_restore(SuspendRestorePlacement::OtherNode, false, true);

    assert_eq!(
        decision,
        SuspendRestoreDecision::StageRemote(RestoreSourceSelection::RemoteLocalFilesMissing)
    );
}

#[test]
fn legacy_missing_local_restore_falls_back_to_remote_lookup() {
    let decision = decide_suspend_restore(SuspendRestorePlacement::Legacy, false, true);

    assert_eq!(
        decision,
        SuspendRestoreDecision::StageRemote(RestoreSourceSelection::RemoteLocalFilesMissing)
    );
}

#[tokio::test]
async fn selected_remote_stage_runs_even_with_local_snapshot_files() {
    let sandbox_id = SandboxId::new().to_string();
    let storage_uri = format!("s3://space-test/suspend_snapshots/{sandbox_id}?region=us-east-1");
    let local_artifact_id = LocalArtifactId::new();
    let cache_dir = tempfile::tempdir().unwrap();
    write_suspend_snapshot_files(
        cache_dir.path(),
        &sandbox_id,
        LOCAL_SNAPSHOT_DIR,
        crate::snapshot_upload::COMMON_VM_ARTIFACTS,
    );
    let restored_snapshot = RestoredSnapshotProvenance::default();

    let (stage_calls, restored_snapshot) = run_selected_remote_stage_for_test(
        cache_dir.path(),
        &sandbox_id,
        &storage_uri,
        restored_snapshot,
        Some(local_artifact_id),
    )
    .await;

    assert_eq!(stage_calls, vec![(sandbox_id, Some(storage_uri.clone()))]);
    assert_eq!(restored_snapshot.storage_uri, Some(storage_uri));
    assert_eq!(
        restored_snapshot.local_artifact_id,
        Some(local_artifact_id.to_string())
    );
}

#[tokio::test]
async fn selected_remote_stage_runs_when_selected_uri_differs_from_staged_uri() {
    let sandbox_id = SandboxId::new().to_string();
    let selected_uri =
        format!("s3://space-test/suspend_snapshots/{sandbox_id}/selected?region=us-east-1");
    let staged_uri =
        format!("s3://space-test/suspend_snapshots/{sandbox_id}/staged?region=us-east-1");
    let local_artifact_id = LocalArtifactId::new();
    let cache_dir = tempfile::tempdir().unwrap();
    write_suspend_snapshot_files(
        cache_dir.path(),
        &sandbox_id,
        LOCAL_SNAPSHOT_DIR,
        crate::snapshot_upload::COMMON_VM_ARTIFACTS,
    );
    let restored_snapshot = RestoredSnapshotProvenance {
        storage_uri: Some(staged_uri),
        local_artifact_id: None,
    };

    let (stage_calls, restored_snapshot) = run_selected_remote_stage_for_test(
        cache_dir.path(),
        &sandbox_id,
        &selected_uri,
        restored_snapshot,
        Some(local_artifact_id),
    )
    .await;

    assert_eq!(stage_calls, vec![(sandbox_id, Some(selected_uri.clone()))]);
    assert_eq!(restored_snapshot.storage_uri, Some(selected_uri));
    assert_eq!(
        restored_snapshot.local_artifact_id,
        Some(local_artifact_id.to_string())
    );
}

#[tokio::test]
async fn selected_remote_stage_runs_when_already_staged_snapshot_is_partial() {
    let sandbox_id = SandboxId::new().to_string();
    let storage_uri = format!("s3://space-test/suspend_snapshots/{sandbox_id}?region=us-east-1");
    let local_artifact_id = LocalArtifactId::new();
    let cache_dir = tempfile::tempdir().unwrap();
    let first_artifact = crate::snapshot_upload::COMMON_VM_ARTIFACTS.first().unwrap();
    write_cache_file(
        cache_dir.path(),
        &sandbox_id,
        &format!("{LOCAL_SNAPSHOT_DIR}/{}", first_artifact.local_name),
    );
    let restored_snapshot = RestoredSnapshotProvenance {
        storage_uri: Some(storage_uri.clone()),
        local_artifact_id: None,
    };

    let (stage_calls, restored_snapshot) = run_selected_remote_stage_for_test(
        cache_dir.path(),
        &sandbox_id,
        &storage_uri,
        restored_snapshot,
        Some(local_artifact_id),
    )
    .await;

    assert_eq!(stage_calls, vec![(sandbox_id, Some(storage_uri.clone()))]);
    assert_eq!(restored_snapshot.storage_uri, Some(storage_uri));
    assert_eq!(
        restored_snapshot.local_artifact_id,
        Some(local_artifact_id.to_string())
    );
}

#[test]
fn restore_placement_accepts_local_bytes_only_on_named_source_node() {
    assert_eq!(
        suspend_restore_placement(Some("node-a"), Some("node-a"), true),
        SuspendRestorePlacement::SourceNode
    );
    assert_eq!(
        suspend_restore_placement(Some("node-a"), Some("node-b"), true),
        SuspendRestorePlacement::OtherNode
    );
}

// A Direct node has no SPACE_NODE_ID; its node-id file names it.
#[test]
fn restore_placement_recognises_the_source_node_from_the_node_id_file() {
    let dir = tempfile::tempdir().unwrap();
    let node_id_path = dir.path().join("node-id");
    let node_id = NodeId::new().to_string();
    std::fs::write(&node_id_path, format!("{node_id}\n")).unwrap();
    let request = StartRequest {
        sandbox_id: SandboxId::new().to_string(),
        checkpoint: Some(SUSPEND_CHECKPOINT.to_string()),
        suspend_restore_source_node: Some(node_id),
        ..Default::default()
    };

    let context = StartSandboxContext::from_request_on_node(
        &request,
        &NodeIdentitySource::node_id_file(node_id_path),
    )
    .unwrap();

    assert_eq!(
        context.restore_placement,
        SuspendRestorePlacement::SourceNode
    );
}

#[tokio::test]
async fn restore_releases_artifact_lock_while_waiting_for_cleanup_completion() {
    use crate::service::tests::service_tests::{insert_test_sandbox, service_for_teardown_test};
    let conn = crate::db::test_conn();
    let id = SandboxId::new().to_string();
    let cleanup_id = SandboxCleanupEpisodeId::new();
    insert_test_sandbox(&conn, &id, SandboxState::CleanupRequired);
    crate::db::set_cleanup_intent(&conn, &id, true, CleanupCompletion::MarkSuspended).unwrap();
    conn.execute("INSERT INTO sandbox_cleanup_episodes
        (sandbox_id, cleanup_id, completion, keep_filesystem, phase, activation_state, created_at, requested_at, quarantine_reason_code)
        VALUES (?1, ?2, 'mark_suspended', TRUE, 'requested', 'restoring', 0, 0, ?3)",
        crate::db::fixture::params![id, cleanup_id.to_string(), SandboxQuarantineReasonCode::RestoreActivationInterrupted.as_str()]).unwrap();
    let service = service_for_teardown_test(conn);
    *service.cleanup_executor.dispatch.lock().unwrap() =
        Some(Arc::new(|_, _| Box::pin(async { Ok(()) })));
    let request = StartRequest {
        sandbox_id: id.clone(),
        checkpoint: Some(SUSPEND_CHECKPOINT.to_owned()),
        ..Default::default()
    };
    let mut context = StartSandboxContext::from_request(&request).unwrap();
    let result = tokio::time::timeout(
        Duration::from_secs(2),
        service.restore_suspended_checkpoint_start(
            &request,
            service.lookup_sandbox(&id).unwrap(),
            &mut context,
            RestoredSnapshotProvenance::default(),
            Some(cleanup_id),
        ),
    )
    .await
    .expect("restore and cleanup must not wait on each other's artifact lock");
    result.expect_err("missing suspend evidence must prevent restore after cleanup");
    assert_eq!(
        service.lookup_sandbox(&id).unwrap().state,
        SandboxState::Quarantined
    );
    service.shutdown_token.cancel();
    service.task_tracker.close();
    service.task_tracker.wait().await;
}

// Template validation applies the effective memory budget before the runtime
// adapter sees the balloon. Runtime-core owns provider configuration rendering.
#[test]
fn template_balloon_is_validated_against_effective_memory() {
    use crate::service::validation::template_runtime_defaults;
    let parse = |metadata: &str, memory_mib: u32| {
        let template = tempfile::tempdir().unwrap();
        std::fs::write(template.path().join("metadata.json"), metadata).unwrap();
        template_runtime_defaults(template.path().to_str().unwrap())
            .balloon
            .for_memory_mib(memory_mib)
    };
    assert_eq!(
        parse(r#"{"vcpu":2,"memory_mib":8192}"#, 8192).unwrap(),
        None
    );
    for (metadata, memory_mib) in [
        (r#"{"balloon":{"mode":"deflate"}}"#, 8192),
        (r#"{"balloon":{"mode":"hint","amount_mib":1}}"#, 8192),
        (r#"{"balloon":{"mode":"inflate","amount_mib":1024}}"#, 8192),
        (
            r#"{"balloon":{"mode":"inflate","amount_mib":1024,"deflate_on_oom":true}}"#,
            1024,
        ),
    ] {
        let error = parse(metadata, memory_mib).unwrap_err();
        assert_eq!(error.code(), tonic::Code::FailedPrecondition, "{metadata}");
        assert!(error.message().contains("balloon"), "{metadata}: {error}");
    }
    let balloon = parse(
        r#"{"balloon":{"mode":"inflate","amount_mib":1024,"deflate_on_oom":true}}"#,
        8192,
    )
    .unwrap()
    .unwrap();
    assert_eq!(balloon.amount_mib(), 1024);
}

// Each refusal happens before Start persists anything or contacts
// node-manager, so the row keeps the state the caller observed. Rows that carry
// a template hash would otherwise continue to template-manager, which every
// test service fails to reach; only the hash-less row is meant to reach
// template resolution.
#[tokio::test(start_paused = true)]
async fn start_refusals_before_activation_leave_the_row_state_unchanged() {
    use crate::service::tests::service_tests::insert_test_sandbox;

    struct Case {
        name: &'static str,
        state: SandboxState,
        template_hash: bool,
        hugepages: bool,
        restrict_to_template_manager_owned: bool,
        code: tonic::Code,
        message: &'static str,
    }
    let cases = [
        Case {
            name: "template-manager may start only its own sandboxes",
            state: SandboxState::Stopped,
            template_hash: true,
            hugepages: false,
            restrict_to_template_manager_owned: true,
            code: tonic::Code::PermissionDenied,
            message: "only call Start for template-owned sandboxes",
        },
        Case {
            name: "failed row needs deletion before any start",
            state: SandboxState::Failed,
            template_hash: true,
            hugepages: false,
            restrict_to_template_manager_owned: false,
            code: tonic::Code::FailedPrecondition,
            message: "requires explicit deletion before cold start",
        },
        Case {
            name: "warm start needs file-backed memory",
            state: SandboxState::Stopped,
            template_hash: true,
            hugepages: true,
            restrict_to_template_manager_owned: false,
            code: tonic::Code::FailedPrecondition,
            message: "file-backed memory",
        },
        Case {
            name: "warm start of a started row stages nothing",
            state: SandboxState::Started,
            template_hash: true,
            hugepages: false,
            restrict_to_template_manager_owned: false,
            code: tonic::Code::FailedPrecondition,
            message: "from a warm snapshot while local state is started",
        },
        Case {
            name: "warm start of a paused row stages nothing",
            state: SandboxState::Paused,
            template_hash: true,
            hugepages: false,
            restrict_to_template_manager_owned: false,
            code: tonic::Code::FailedPrecondition,
            message: "from a warm snapshot while local state is paused",
        },
        Case {
            name: "warm start never resolves a template by name alone",
            state: SandboxState::Stopped,
            template_hash: false,
            hugepages: false,
            restrict_to_template_manager_owned: false,
            code: tonic::Code::FailedPrecondition,
            message: "without its saved template hash",
        },
    ];
    for case in cases {
        let conn = crate::db::test_conn();
        let id = SandboxId::new().to_string();
        insert_test_sandbox(&conn, &id, case.state);
        if !case.template_hash {
            conn.execute(
                "UPDATE sandboxes SET template_hash = NULL WHERE id = ?1",
                [&id],
            )
            .unwrap();
        }
        let service = SandboxManagerService::new_with_config(
            conn,
            crate::service::SandboxManagerServiceOptions::for_test(),
            crate::config::SandboxConfig {
                hugepages: case.hugepages,
                ..Default::default()
            },
        );
        let request = StartRequest {
            sandbox_id: id.clone(),
            from_warm: true,
            ..Default::default()
        };

        let error = service
            .start_sandbox(request, case.restrict_to_template_manager_owned)
            .await
            .expect_err(case.name);

        assert_eq!(error.code(), case.code, "{}: {error:?}", case.name);
        assert!(
            error.message().contains(case.message),
            "{}: {error:?}",
            case.name
        );
        assert_eq!(
            service.lookup_sandbox(&id).unwrap().state,
            case.state,
            "{}",
            case.name
        );
    }
}

// Without a local row a suspend-checkpoint Start restores from the cluster's
// latest ready backup. A node with no cluster API cannot have one, so the
// caller gets the original not-found answer rather than a retryable error, and
// no sandbox row appears. The child process pins the cluster API endpoint
// variables so the result does not depend on the host environment. The restore
// transfer scope row is deliberately not asserted: this path currently leaves
// the scope it opened behind, and that outcome is not an agreed contract.
#[test]
fn missing_row_restore_without_a_cluster_api_reports_the_sandbox_not_found() {
    use pplx_space_node_api_client::cluster::{
        CLUSTER_API_ENDPOINT_ENV, CLUSTER_API_ENDPOINT_ENV_LEGACY,
    };
    if !pplx_space_test_env::enter_test_process(&[
        (CLUSTER_API_ENDPOINT_ENV, None),
        (CLUSTER_API_ENDPOINT_ENV_LEGACY, None),
    ]) {
        return;
    }

    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            let service = crate::service::tests::service_tests::service_for_teardown_test(
                crate::db::test_conn(),
            );
            let id = SandboxId::new().to_string();
            let request = StartRequest {
                sandbox_id: id.clone(),
                checkpoint: Some(SUSPEND_CHECKPOINT.to_owned()),
                route_identity: Some(crate::proto::SpaceApiRouteIdentity {
                    sandbox_id: Some(id.clone()),
                    route_generation: Some(3),
                    current_node_id: Some(service.local_node_id.unwrap().to_string()),
                    substrate_kind: crate::proto::SandboxSubstrateKind::Direct as i32,
                    execution_generation: Some(3),
                    activation_attempt_id: Some(SandboxActivationAttemptId::new().to_string()),
                    pod_uid: None,
                }),
                ..Default::default()
            };

            let error = service
                .start_sandbox(request, false)
                .await
                .expect_err("no cluster API means no registered snapshot");

            assert_eq!(error.code(), tonic::Code::NotFound, "{error:?}");
            assert_eq!(error.message(), format!("sandbox not found: {id}"));
            assert!(
                crate::db::get_sandbox(&service.db_conn().unwrap(), &id)
                    .unwrap()
                    .is_none()
            );
        });
}

// An in-place restore first retires any previous transfer scope. When that
// scope cannot be cancelled, the restore stops and keeps the previous scope row
// as the record of work still to clean up; it opens no new scope.
//
// The cancel fails deterministically only because
// `cancel_restore_transfer_scope_and_remove_staging` compares the recorded
// staging path with the sandbox and scope ids before it connects to the fixed
// volume-manager socket. If that check moves after the connect, this test
// waits out the connect-retry budget instead.
#[tokio::test]
async fn suspend_restore_dir_keeps_a_previous_scope_it_cannot_cancel() {
    let conn = crate::db::test_conn();
    let id = SandboxId::new().to_string();
    crate::service::tests::service_tests::insert_test_sandbox(&conn, &id, SandboxState::Suspended);
    let previous = crate::db::RestoreTransferScopeRow {
        transfer_scope_id: pplx_space_common_core::types::TransferScopeId::new(),
        execution_generation: Some(ExecutionGeneration::new(2).unwrap()),
        activation_attempt_id: Some(SandboxActivationAttemptId::new()),
        staging_dir: "/synthetic/foreign/staging".to_owned(),
    };
    assert!(
        crate::db::insert_restore_transfer_scope(
            &conn,
            &id,
            previous.transfer_scope_id,
            previous.execution_generation,
            previous.activation_attempt_id,
            &previous.staging_dir,
        )
        .unwrap()
    );
    let service = SandboxManagerService::for_test(conn);

    let error = prepare_suspend_restore_dir(
        &service,
        &service.lookup_sandbox(&id).unwrap().user,
        id.clone(),
        Some("s3://snapshots/suspend/synthetic?region=us-east-1".to_owned()),
        Some(ExecutionGeneration::new(3).unwrap()),
        Some(SandboxActivationAttemptId::new()),
    )
    .await
    .expect_err("an uncancelled previous scope must stop the restore");

    assert_eq!(
        crate::db::restore_transfer_scope(&service.db_conn().unwrap(), &id).unwrap(),
        Some(previous),
        "{error:?}"
    );
    assert!(
        error
            .to_string()
            .contains("cancel previous restore transfer scope"),
        "{error:?}"
    );
}

/// The shape the cluster-controller sends to materialize an evacuated stopped
/// sandbox: its own id, a suspend checkpoint, and the exact URI of the disk
/// artifact the drain published for it.
fn materialize_stopped_request() -> StartRequest {
    materialize_stopped_request_for(&SandboxId::new().to_string())
}

pub(in crate::service) fn materialize_stopped_request_for(sandbox_id: &str) -> StartRequest {
    let artifact_snapshot_id = pplx_space_common_core::types::SnapshotId::new();
    StartRequest {
        sandbox_id: sandbox_id.to_string(),
        checkpoint: Some(SUSPEND_CHECKPOINT.to_string()),
        suspend_restore_storage_uri: Some(format!(
            "s3://snapshots/suspend/{sandbox_id}/{artifact_snapshot_id}?region=us-east-1"
        )),
        target_state: crate::proto::WakeTargetState::Stopped as i32,
        expected_local_artifact_id: Some(
            pplx_space_common_core::types::LocalArtifactId::new().to_string(),
        ),
        route_identity: Some(crate::proto::SpaceApiRouteIdentity {
            execution_generation: Some(7),
            ..Default::default()
        }),
        ..Default::default()
    }
}

#[test]
fn a_stopped_target_carries_through_the_request_boundary() {
    let context = StartSandboxContext::from_request(&materialize_stopped_request()).unwrap();

    assert_eq!(context.target_state, SandboxState::Stopped);
}

/// The property, in operator terms: asking for `stopped` can never turn into a
/// running microVM.
///
/// The stopped target is only reachable through the cross-node restore that
/// stages a disk artifact. Every other request shape that could carry it ends
/// in a boot — a fresh cold start, a checkpoint restore with no artifact to
/// stage, or a fork, which cold-boots the child by design. Refusing them at
/// the wire is the first half of "never reaches the hypervisor"; the
/// short-circuit in the start path is the second.
#[test]
fn a_stopped_target_is_refused_on_every_request_shape_that_would_boot() {
    let fresh_cold = StartRequest {
        checkpoint: None,
        suspend_restore_storage_uri: None,
        ..materialize_stopped_request()
    };
    let checkpoint_without_an_artifact = StartRequest {
        suspend_restore_storage_uri: None,
        ..materialize_stopped_request()
    };
    let uri_without_a_checkpoint = StartRequest {
        checkpoint: None,
        ..materialize_stopped_request()
    };

    for (description, request) in [
        ("a fresh cold start", fresh_cold),
        (
            "a checkpoint restore naming no artifact",
            checkpoint_without_an_artifact,
        ),
        (
            "an artifact URI with no checkpoint",
            uri_without_a_checkpoint,
        ),
    ] {
        let error = StartSandboxContext::from_request(&request)
            .err()
            .unwrap_or_else(|| panic!("{description} must not accept a stopped target"));
        assert_eq!(error.code(), tonic::Code::InvalidArgument, "{description}");
        assert!(
            error
                .message()
                .contains("stopped target state requires a suspend checkpoint"),
            "{description} produced the wrong refusal: {error}"
        );
    }
}

/// Fork and evacuation have opposite postconditions on the artifact pin — the
/// fork arm drops it so the staged disk stays an immutable source, the
/// evacuation arm keeps it because it is the sandbox's own disk. A request
/// asking for both would have to satisfy both.
///
/// It cannot be asked for, and the two refusals that prevent it both predate
/// this change: restore-as-new requires an empty checkpoint, and it requires
/// the `Started` target. The stopped materialization requires the opposite of
/// each. This asserts the property from both sides rather than adding a third
/// check, which would name itself as the mechanism and never run.
#[test]
fn a_stopped_target_and_a_fork_cannot_be_requested_together() {
    let source_sandbox_id = SandboxId::new();
    let artifact_snapshot_id = pplx_space_common_core::types::SnapshotId::new();
    let fork_context = crate::proto::RestoreArtifactAsNewContext {
        source_sandbox_id: source_sandbox_id.to_string(),
        artifact_snapshot_id: artifact_snapshot_id.to_string(),
        target_owner: format!("user:{}", uuid::Uuid::now_v7()),
        artifact_kind: crate::proto::ArtifactKind::Disk as i32,
        disk_io_max_operations_per_sec: None,
        disk_io_max_bandwidth_mib_per_sec: None,
        volume_encryption: false,
    };
    let fork_storage_uri = format!(
        "s3://snapshots/suspend/{source_sandbox_id}/{artifact_snapshot_id}?region=us-east-1"
    );

    // With the checkpoint the stopped target requires, the fork arm refuses on
    // its cold-start requirement.
    let with_checkpoint = StartRequest {
        suspend_restore_storage_uri: Some(fork_storage_uri.clone()),
        restore_artifact_as_new: Some(fork_context.clone()),
        ..materialize_stopped_request()
    };
    let error = StartSandboxContext::from_request(&with_checkpoint)
        .err()
        .expect("a fork carrying a suspend checkpoint must be refused");
    assert_eq!(error.code(), tonic::Code::InvalidArgument);
    assert!(
        error.message().contains("requires a cold start"),
        "unexpected refusal: {error}"
    );

    // Without it, the fork arm refuses on its target: restore-as-new
    // cold-boots the child, so it cannot land in `stopped`.
    let without_checkpoint = StartRequest {
        checkpoint: None,
        suspend_restore_storage_uri: Some(fork_storage_uri),
        restore_artifact_as_new: Some(fork_context),
        ..materialize_stopped_request()
    };
    let error = StartSandboxContext::from_request(&without_checkpoint)
        .err()
        .expect("a fork asking for a stopped target must be refused");
    assert_eq!(error.code(), tonic::Code::InvalidArgument);
    assert!(
        error
            .message()
            .contains("cold-boots into the started lifecycle"),
        "unexpected refusal: {error}"
    );
}

/// The property, in operator terms: "your bytes are here" must never be
/// answered for older bytes.
///
/// A sandbox that ran and stopped on a node earlier leaves the same shape an
/// evacuation does — `Stopped`, pinning a `disk`/`stop` artifact — so the
/// lifecycle cannot distinguish them. If the node reported convergence on the
/// state match alone, the cluster would be free to retire the source while
/// this node still held the older rootfs, and the user would silently get it
/// back.
///
/// The request names the artifact it expects, so the decision is a local
/// comparison. Every way of failing to be that artifact is refused: a
/// different id, no pin at all, and a pin of the wrong kind or origin — the
/// last because an id alone does not say the bytes are an evacuated disk
/// rather than a suspend checkpoint or a user's named snapshot.
#[test]
fn only_the_requested_artifact_lets_a_stopped_row_pass() {
    use pplx_space_common_core::types::{ArtifactKind, ArtifactOrigin, LocalArtifactId};
    let pinned = LocalArtifactId::new();
    let other = LocalArtifactId::new();

    // Rows are built through the real schema rather than a literal: the
    // check reads four columns and the CHECK constraints on them are part of
    // what makes a shape reachable at all.
    let conn = crate::db::test_conn();
    let row = |state: SandboxState,
               pin: Option<LocalArtifactId>,
               kind: Option<ArtifactKind>,
               origin: Option<ArtifactOrigin>| {
        let id = SandboxId::new().to_string();
        insert_suspended_test_sandbox(&conn, &id);
        conn.execute(
            "UPDATE sandboxes
             SET state = ?1,
                 current_local_artifact_id = ?2,
                 current_local_artifact_kind = ?3,
                 current_local_artifact_origin = ?4
             WHERE id = ?5",
            crate::db::fixture::params![
                state.as_str(),
                pin.map(|id| id.to_string()),
                kind.map(|k| k.as_str()),
                origin.map(|o| o.as_str()),
                &id
            ],
        )
        .unwrap();
        crate::db::get_sandbox(&conn, &id).unwrap().unwrap()
    };
    let evacuated = row(
        SandboxState::Stopped,
        Some(pinned),
        Some(ArtifactKind::Disk),
        Some(ArtifactOrigin::Stop),
    );

    verify_stopped_materialization(&evacuated, pinned)
        .expect("a row pinning exactly the requested stop-origin disk is the materialization");

    for (description, candidate, expected) in [
        (
            "a row pinning a different artifact — residue from an earlier life",
            row(
                SandboxState::Stopped,
                Some(other),
                Some(ArtifactKind::Disk),
                Some(ArtifactOrigin::Stop),
            ),
            pinned,
        ),
        (
            "a stopped row with no pin at all",
            row(SandboxState::Stopped, None, None, None),
            pinned,
        ),
        (
            "a pin of the right id but a suspend checkpoint's kind and origin",
            row(
                SandboxState::Stopped,
                Some(pinned),
                Some(ArtifactKind::Full),
                Some(ArtifactOrigin::Suspend),
            ),
            pinned,
        ),
        (
            "a row that is not stopped",
            row(
                SandboxState::Suspended,
                Some(pinned),
                Some(ArtifactKind::Disk),
                Some(ArtifactOrigin::Stop),
            ),
            pinned,
        ),
    ] {
        let error = verify_stopped_materialization(&candidate, expected)
            .expect_err(&format!("{description} must not pass as materialized"));
        assert_eq!(
            error.code(),
            tonic::Code::FailedPrecondition,
            "{description}"
        );
    }
}

/// The second stopped boundary sits after the pending cleanup.
///
/// Production reaches this point from the `CleanupRestoring` arm; the fixture
/// is the `cleanup_required` row with a `MarkSuspended` intent that arm leaves,
/// handed straight to `restore_suspended_checkpoint_start`.
/// `restore_suspended_checkpoint_start` runs that cleanup synchronously before
/// the second stopped boundary; in a unit test the cleanup fails for want of a
/// node-manager, so the error has to come from the cleanup executor rather
/// than from `verify_stopped_materialization` answering first. The boundary's
/// own refusal of the row a cleanup leaves is
/// `a_stopped_target_refuses_a_suspended_row_after_the_cleanup`.
#[tokio::test]
async fn the_second_stopped_boundary_sits_after_the_pending_cleanup() {
    let conn = crate::db::test_conn();
    let sandbox_id = SandboxId::new().to_string();
    insert_suspended_test_sandbox(&conn, &sandbox_id);
    // The shape the `CleanupRestoring` arm leaves: an intent, and a row that
    // the cleanup will move to `suspended` — not to `stopped`.
    conn.execute(
        "UPDATE sandboxes
         SET state = 'cleanup_required',
             cleanup_completion = 'mark_suspended',
             cleanup_keep_filesystem = 1
         WHERE id = ?1",
        crate::db::fixture::params![&sandbox_id],
    )
    .unwrap();
    let service = recovery_test_service(conn);
    let request = materialize_stopped_request_for(&sandbox_id);
    let mut ctx = StartSandboxContext::from_request(&request).unwrap();
    let sandbox = service.lookup_sandbox(&sandbox_id).unwrap();

    let error = service
        .restore_suspended_checkpoint_start(
            &request,
            sandbox,
            &mut ctx,
            RestoredSnapshotProvenance::default(),
            None,
        )
        .await
        .expect_err("a unit test has no node-manager to complete the retirement with");

    // The cleanup was attempted — the boundary did not answer this row. The
    // synchronous cleanup runs through the durable cleanup executor, whose
    // context names the failure; a boundary placed above the cleanup would
    // instead answer with `verify_stopped_materialization`'s refusal.
    assert!(
        error.message().contains("durable sandbox cleanup"),
        "the pending cleanup must be driven, not skipped; got: {error}"
    );
    assert!(
        !error.message().contains("cannot materialize"),
        "the stopped boundary must not answer before the cleanup; got: {error}"
    );
}

/// What a resumed materialization returns once its retry has finished.
///
/// The `ResumeRestoring` arm re-stages the disk artifact, finalizes the row,
/// and re-reads it — leaving `Stopped`, which is the state this drives the
/// function with. Everything after that point in
/// `restore_suspended_checkpoint_start` restores and launches a VM, and it
/// opens with an `expected suspended` precondition that a `Stopped` row fails.
/// So the resumed materialization has to be answered before it, and this is
/// the assertion that the answer is placed there: delete the stopped return
/// and this call comes back `FailedPrecondition … expected suspended` instead.
///
/// The sibling tests in `service::tests` cover the entry into this function;
/// they stop at the staging-lock deferral, so this is the half that reaches
/// past it without needing object storage.
#[tokio::test]
async fn a_resumed_stopped_materialization_is_answered_before_the_launch() {
    let conn = crate::db::test_conn();
    let sandbox_id = SandboxId::new().to_string();
    insert_suspended_test_sandbox(&conn, &sandbox_id);
    let pin = pplx_space_common_core::types::LocalArtifactId::new();
    conn.execute(
        "UPDATE sandboxes
         SET state = 'stopped',
             current_local_artifact_id = ?1,
             current_local_artifact_kind = 'disk',
             current_local_artifact_origin = 'stop'
         WHERE id = ?2",
        crate::db::fixture::params![pin.to_string(), &sandbox_id],
    )
    .unwrap();
    let service = recovery_test_service(conn);
    let mut request = materialize_stopped_request_for(&sandbox_id);
    // The resumed row pins what the retry staged, so the request has to expect
    // that same artifact — a mismatch here is the stale-row refusal, not this
    // test's subject.
    request.expected_local_artifact_id = Some(pin.to_string());
    let mut ctx = StartSandboxContext::from_request(&request).unwrap();
    let sandbox = service.lookup_sandbox(&sandbox_id).unwrap();

    let response = service
        .restore_suspended_checkpoint_start(
            &request,
            sandbox,
            &mut ctx,
            RestoredSnapshotProvenance::default(),
            None,
        )
        .await
        .expect("a resumed materialization must be reported, not refused for not being suspended")
        .into_inner();

    // Only the identity is asserted on the reply: `materialized_stopped_response`
    // builds it with `..Default::default()`, so `pid == 0` and
    // `route_identity.is_none()` would hold even with the behaviour they name
    // removed. What can actually fail is that the call succeeded at all, and
    // what the row looks like afterwards.
    assert_eq!(response.sandbox_id, sandbox_id);
    let row = service.lookup_sandbox(&sandbox_id).unwrap();
    assert_eq!(row.state, SandboxState::Stopped);
    assert_eq!(
        row.current_local_artifact_id,
        Some(pin),
        "answering the resume must not disturb the pin the retry just wrote"
    );
}

/// Names the branch the stopped short-circuit has to get in front of.
///
/// A `Suspended` row under a suspend checkpoint classifies as
/// `ResumeSuspendCheckpoint`, whose handler restores the VM and launches it.
/// `a_stopped_target_refuses_a_row_it_did_not_materialize` in
/// `service::tests` asserts that a stopped target refuses that same row
/// instead; this is the half that shows the alternative was a launch.
#[test]
fn a_suspended_row_under_a_checkpoint_is_a_launching_fixture() {
    assert_eq!(
        interrupted_restore_action(SUSPEND_CHECKPOINT, SandboxState::Suspended, None, false),
        InterruptedRestoreAction::ResumeSuspendCheckpoint
    );
}

/// A stopped target with no expected artifact id is refused at the wire, so no
/// boundary can be reached without the evidence it compares.
#[test]
fn a_stopped_target_requires_the_expected_artifact_id() {
    let mut request = materialize_stopped_request();
    request.expected_local_artifact_id = None;

    let error = StartSandboxContext::from_request(&request)
        .err()
        .expect("a stopped target without the expected artifact id must be refused");

    assert_eq!(error.code(), tonic::Code::InvalidArgument);
    assert!(
        error
            .message()
            .contains("requires expected_local_artifact_id"),
        "unexpected refusal: {error}"
    );
}

#[test]
fn the_stopped_target_selects_the_evacuation_finalize_arm() {
    let request = materialize_stopped_request();
    let expected: pplx_space_common_core::types::LocalArtifactId = request
        .expected_local_artifact_id
        .as_deref()
        .unwrap()
        .parse()
        .unwrap();
    let ctx = StartSandboxContext::from_request(&request).unwrap();

    assert_eq!(
        ctx.restore_target,
        CrossNodeRestoreTarget::StoppedDiskArtifact { expected },
        "the target must carry the artifact the finalize compares against"
    );

    // Every other target keeps taking the historical checkpoint arm.
    for target_state in [
        crate::proto::WakeTargetState::Started,
        crate::proto::WakeTargetState::Paused,
    ] {
        let mut other = materialize_stopped_request();
        other.target_state = target_state as i32;
        other.expected_local_artifact_id = None;
        let ctx = StartSandboxContext::from_request(&other).unwrap();
        assert_eq!(
            ctx.restore_target,
            CrossNodeRestoreTarget::SuspendedCheckpoint,
            "{target_state:?} must keep taking the historical checkpoint arm"
        );
    }
}

/// The checkpoint launch refuses a stopped target on its own, so a path that
/// reaches it without passing through a `Start` stopped boundary fails rather
/// than booting. The same row and launch with a `Started` target get past the
/// refusal, which is what makes the refusal observable here.
#[tokio::test]
async fn a_checkpoint_launch_refuses_a_stopped_target() {
    let conn = crate::db::test_conn();
    let sandbox_id = SandboxId::new().to_string();
    insert_suspended_test_sandbox(&conn, &sandbox_id);
    let service = recovery_test_service(conn);
    let sandbox = service.lookup_sandbox(&sandbox_id).unwrap();

    for target_state in [SandboxState::Stopped, SandboxState::Started] {
        let outcome = tokio::time::timeout(
            std::time::Duration::from_secs(20),
            service.start_from_checkpoint(StartFromCheckpointLaunch {
                vcpu: None,
                activation_auto_lifecycle: None,
                operation_activation_attempt_id: None,
                sandbox: &sandbox,
                blocking: false,
                netns_path: None,
                target_state,
                execution_generation: None,
                space_api_route_identity: Default::default(),
                route_identity_provenance:
                    crate::auto_lifecycle_delete_receipts::RouteIdentityProvenance::Unfenced,
                open_activation_claim_eligible: false,
                restored_snapshot: RestoredSnapshotProvenance::default(),
            }),
        )
        .await
        .expect("the launch must not hang in a unit test");
        let refused = matches!(
            &outcome,
            Err(status) if status.message().contains("a checkpoint launch never runs for it")
        );
        assert_eq!(
            refused,
            target_state == SandboxState::Stopped,
            "{target_state:?}: {:?}",
            outcome.map(|_| ())
        );
    }
}

/// `expected_local_artifact_id` is evidence only a stopped materialization
/// compares. Sent with any other target it would be silently ignored, so it is
/// refused at the wire instead; the stopped request it was built from passes.
#[test]
fn an_expected_artifact_without_the_stopped_target_is_refused() {
    let request = materialize_stopped_request();
    StartSandboxContext::from_request(&request).expect("the stopped request is valid");

    for target_state in [
        crate::proto::WakeTargetState::Started,
        crate::proto::WakeTargetState::Paused,
    ] {
        let mut other = request.clone();
        other.target_state = target_state as i32;
        let error = StartSandboxContext::from_request(&other)
            .err()
            .unwrap_or_else(|| {
                panic!("{target_state:?} with an expected artifact must be refused")
            });
        assert_eq!(
            error.code(),
            tonic::Code::InvalidArgument,
            "{target_state:?}"
        );
        assert!(
            error
                .message()
                .contains("expected_local_artifact_id requires the stopped target state"),
            "{target_state:?}: {error}"
        );

        other.expected_local_artifact_id = None;
        StartSandboxContext::from_request(&other)
            .unwrap_or_else(|error| panic!("{target_state:?} without the id stays valid: {error}"));
    }
}

/// The second stopped boundary refuses a `Suspended` row — the shape a driven
/// cleanup leaves — rather than letting the `expected suspended` path carry it
/// to the launch. The assertion is the boundary's own message, so a refusal
/// from anywhere past it would fail the test.
#[tokio::test]
async fn a_stopped_target_refuses_a_suspended_row_after_the_cleanup() {
    let conn = crate::db::test_conn();
    let sandbox_id = SandboxId::new().to_string();
    insert_suspended_test_sandbox(&conn, &sandbox_id);
    let service = recovery_test_service(conn);
    let request = materialize_stopped_request_for(&sandbox_id);
    let mut ctx = StartSandboxContext::from_request(&request).unwrap();
    let sandbox = service.lookup_sandbox(&sandbox_id).unwrap();

    let error = service
        .restore_suspended_checkpoint_start(
            &request,
            sandbox,
            &mut ctx,
            RestoredSnapshotProvenance::default(),
            None,
        )
        .await
        .expect_err("a suspended row is not the requested stopped materialization");

    assert_eq!(error.code(), tonic::Code::FailedPrecondition);
    assert!(
        error
            .message()
            .contains("as stopped while local state is suspended"),
        "the stopped boundary must refuse it, not the launch backstop: {error}"
    );
    let row = service.lookup_sandbox(&sandbox_id).unwrap();
    assert_eq!(row.state, SandboxState::Suspended);
}

/// The stopped boundary vouches for an inert disk, not just a row: a `Stopped`
/// row that pins exactly the requested artifact is refused when its rootfs is
/// missing or its Firecracker runtime is still live, and the otherwise-identical
/// row with the rootfs present and no live runtime converges. A rootfs check
/// that errors is refused too, with its own reason. Both probes assert
/// they are asked about this sandbox, so a wrong-id probe can't pass. Both `Start`
/// return sites are driven — the first boundary through `start_sandbox`, the
/// second through `restore_suspended_checkpoint_start` — so neither can vouch
/// for a live VM.
#[tokio::test]
async fn a_stopped_row_is_reported_materialized_only_when_inert_and_present() {
    // (rootfs probe answer — `None` is an I/O error, runtime live, expected refusal)
    let cases = [
        (Some(true), false, None),
        (
            Some(true),
            true,
            Some((tonic::Code::FailedPrecondition, "its runtime is still live")),
        ),
        (
            Some(false),
            false,
            Some((tonic::Code::FailedPrecondition, "its rootfs is missing")),
        ),
        (
            None,
            false,
            Some((
                tonic::Code::FailedPrecondition,
                "its rootfs could not be checked",
            )),
        ),
    ];
    for (rootfs_present, runtime_live, refusal) in cases {
        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new().to_string();
        insert_suspended_test_sandbox(&conn, &sandbox_id);
        let pin = pplx_space_common_core::types::LocalArtifactId::new();
        conn.execute(
            "UPDATE sandboxes
             SET state = 'stopped',
                 current_local_artifact_id = ?1,
                 current_local_artifact_kind = 'disk',
                 current_local_artifact_origin = 'stop'
             WHERE id = ?2",
            crate::db::fixture::params![pin.to_string(), &sandbox_id],
        )
        .unwrap();
        let service = SandboxManagerService::new_with_options(
            conn,
            crate::service::SandboxManagerServiceOptions {
                sandbox_runtime_live: std::sync::Arc::new({
                    let expected_id = sandbox_id.clone();
                    move |id| {
                        assert_eq!(
                            id, expected_id,
                            "the liveness probe asked about another sandbox"
                        );
                        runtime_live
                    }
                }),
                sandbox_rootfs_present: std::sync::Arc::new({
                    let expected_id = sandbox_id.clone();
                    move |id| {
                        assert_eq!(
                            id, expected_id,
                            "the rootfs probe asked about another sandbox"
                        );
                        rootfs_present.ok_or_else(|| {
                            std::io::Error::from(std::io::ErrorKind::PermissionDenied)
                        })
                    }
                }),
                ..crate::service::SandboxManagerServiceOptions::for_test()
            },
        );
        let mut request = materialize_stopped_request_for(&sandbox_id);
        request.expected_local_artifact_id = Some(pin.to_string());

        let first = tokio::time::timeout(
            std::time::Duration::from_secs(20),
            service.start_sandbox(request.clone(), false),
        )
        .await
        .expect("a stopped target must not wait on a launch");
        let mut ctx = StartSandboxContext::from_request(&request).unwrap();
        let sandbox = service.lookup_sandbox(&sandbox_id).unwrap();
        let second = tokio::time::timeout(
            std::time::Duration::from_secs(20),
            service.restore_suspended_checkpoint_start(
                &request,
                sandbox,
                &mut ctx,
                RestoredSnapshotProvenance::default(),
                None,
            ),
        )
        .await
        .expect("the second boundary answers from local state without waiting");

        for (site, outcome) in [("first boundary", first), ("second boundary", second)] {
            match refusal {
                None => assert_eq!(
                    outcome
                        .unwrap_or_else(|error| panic!(
                            "{site}: an inert, present row converges: {error}"
                        ))
                        .into_inner()
                        .sandbox_id,
                    sandbox_id
                ),
                Some((code, reason)) => {
                    let error = outcome.expect_err(&format!(
                        "{site}: rootfs={rootfs_present:?} live={runtime_live} must not be vouched for"
                    ));
                    assert_eq!(error.code(), code, "{site}");
                    assert!(
                        error.message().contains(reason),
                        "{site}: unexpected refusal: {error}"
                    );
                }
            }
        }
    }
}

/// The stopped boundary applies the route-identity fence before it vouches for
/// an existing row: a request aimed at another node is refused, though the row
/// pins exactly the artifact it names.
#[tokio::test]
async fn a_stopped_row_is_not_vouched_for_to_a_request_fenced_elsewhere() {
    let conn = crate::db::test_conn();
    let sandbox_id = SandboxId::new().to_string();
    insert_suspended_test_sandbox(&conn, &sandbox_id);
    let pin = pplx_space_common_core::types::LocalArtifactId::new();
    conn.execute(
        "UPDATE sandboxes
         SET state = 'stopped',
             current_local_artifact_id = ?1,
             current_local_artifact_kind = 'disk',
             current_local_artifact_origin = 'stop'
         WHERE id = ?2",
        crate::db::fixture::params![pin.to_string(), &sandbox_id],
    )
    .unwrap();
    let service = recovery_test_service(conn);
    let mut request = materialize_stopped_request_for(&sandbox_id);
    request.expected_local_artifact_id = Some(pin.to_string());
    request.route_identity = Some(crate::proto::SpaceApiRouteIdentity {
        route_generation: Some(7),
        execution_generation: Some(7),
        current_node_id: Some("019f8000-0000-7000-8000-0000000000ee".to_string()),
        ..Default::default()
    });

    let error = tokio::time::timeout(
        std::time::Duration::from_secs(20),
        service.start_sandbox(request, false),
    )
    .await
    .expect("a stopped target must not wait on a launch")
    .expect_err("a request fenced to another node must not be told the sandbox converged");

    assert_eq!(error.code(), tonic::Code::FailedPrecondition);
    assert!(
        error
            .message()
            .contains("runtime activation identity targets node"),
        "the route-identity fence must be what refuses it: {error}"
    );
}

/// The second stopped boundary applies the route-identity fence itself: a
/// `Stopped` row a concurrent materialization finalized reaches it without the
/// resume arm, so a request fenced to another node must still be refused.
#[tokio::test]
async fn the_second_stopped_boundary_fences_the_request_too() {
    let conn = crate::db::test_conn();
    let sandbox_id = SandboxId::new().to_string();
    insert_suspended_test_sandbox(&conn, &sandbox_id);
    let pin = pplx_space_common_core::types::LocalArtifactId::new();
    conn.execute(
        "UPDATE sandboxes
         SET state = 'stopped',
             current_local_artifact_id = ?1,
             current_local_artifact_kind = 'disk',
             current_local_artifact_origin = 'stop'
         WHERE id = ?2",
        crate::db::fixture::params![pin.to_string(), &sandbox_id],
    )
    .unwrap();
    let service = recovery_test_service(conn);
    let mut request = materialize_stopped_request_for(&sandbox_id);
    request.expected_local_artifact_id = Some(pin.to_string());
    request.route_identity = Some(crate::proto::SpaceApiRouteIdentity {
        route_generation: Some(7),
        execution_generation: Some(7),
        current_node_id: Some("019f8000-0000-7000-8000-0000000000ee".to_string()),
        ..Default::default()
    });
    let mut ctx = StartSandboxContext::from_request(&request).unwrap();
    let sandbox = service.lookup_sandbox(&sandbox_id).unwrap();

    let error = service
        .restore_suspended_checkpoint_start(
            &request,
            sandbox,
            &mut ctx,
            RestoredSnapshotProvenance::default(),
            None,
        )
        .await
        .expect_err("a request fenced to another node must not be told the sandbox converged");

    assert_eq!(error.code(), tonic::Code::FailedPrecondition);
    assert!(
        error
            .message()
            .contains("runtime activation identity targets node"),
        "the route-identity fence must be what refuses it: {error}"
    );
}
