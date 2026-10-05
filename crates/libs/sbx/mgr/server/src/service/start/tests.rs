use pplx_space_common_core::types::{LocalArtifactId, SandboxId, SnapshotId};

use super::*;
use crate::db::CleanupCompletion;
use crate::service::suspend_snapshot_publication::LOCAL_SNAPSHOT_DIR;
use pplx_space_common_core::types::ExecutionGeneration;

// A cold boot and a snapshot load give a sandbox the same rootfs limiter,
// sized by its row's assignment, with both buckets holding the configured
// burst; builders are exempt.
#[tokio::test]
async fn rootfs_rate_limiter_uses_the_assignment_and_configured_burst() {
    let service = SandboxManagerService::new_with_config(
        crate::db::test_conn(),
        Default::default(),
        crate::config::SandboxConfig {
            disk_io_burst_secs: std::num::NonZeroU32::new(3),
            ..Default::default()
        },
    );
    let limits = pplx_space_common_core::types::DiskIoLimits::try_new(5_000, 200).unwrap();
    let limiter = service
        .rootfs_rate_limiter(false, limits)
        .expect("sandbox is limited");

    let rendered = firecracker_rate_limiter_field("rate_limiter", Some(&limiter));
    let field: serde_json::Value =
        serde_json::from_str(&format!("{{{}}}", rendered.trim_start_matches(','))).unwrap();
    assert_eq!(
        field["rate_limiter"],
        serde_json::json!({
            "bandwidth": { "size": 629_145_600, "refill_time": 3000 },
            "ops": { "size": 15_000, "refill_time": 3000 }
        })
    );
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
    assert!(firecracker_rate_limiter_field("rate_limiter", None).is_empty());
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
        rusqlite::params![id, cleanup_id.to_string(), SandboxQuarantineReasonCode::RestoreActivationInterrupted.as_str()]).unwrap();
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
