use super::*;
#[test]
fn operation_snapshot_accepts_the_current_sandbox_source() {
    let request = crate::proto::CreateSnapshotRequest {
        source: Some(crate::proto::CreateSnapshotSource {
            source: Some(
                crate::proto::create_snapshot_source::Source::CurrentSandbox(
                    crate::proto::CurrentSandboxSnapshotSource {
                        execution_generation: 7,
                        lifecycle: crate::proto::SnapshotSourceLifecycle::Started as i32,
                    },
                ),
            ),
        }),
        ..Default::default()
    };

    assert_eq!(
        current_snapshot_precondition(&request).unwrap(),
        CurrentSnapshotPrecondition {
            execution_generation: 7,
            lifecycle: crate::proto::SnapshotSourceLifecycle::Started as i32,
        }
    );
}

#[test]
fn operation_snapshot_rejects_an_empty_source_union() {
    let request = crate::proto::CreateSnapshotRequest {
        source: Some(crate::proto::CreateSnapshotSource { source: None }),
        ..Default::default()
    };

    let error = current_snapshot_precondition(&request).unwrap_err();

    assert_eq!(error.code(), tonic::Code::InvalidArgument);
}

#[test]
fn operation_snapshot_plan_keeps_started_and_paused_sources_in_place() {
    let started = OperationSnapshotPlan::from_state(SandboxState::Started)
        .expect("started sources support disk capture");
    assert_eq!(started.source_state(), SandboxState::Started);
    assert!(started.flush_guest_filesystems());

    let paused = OperationSnapshotPlan::from_state(SandboxState::Paused)
        .expect("paused sources support disk capture");
    assert_eq!(paused.source_state(), SandboxState::Paused);
    assert!(!paused.flush_guest_filesystems());

    assert!(OperationSnapshotPlan::from_state(SandboxState::Checkpointing).is_none());
}

#[test]
fn operation_snapshot_replay_uses_the_server_generated_artifact_identity() {
    let sandbox_id = SandboxId::new().to_string();
    let request_id = Uuid::now_v7();
    let execution_generation = pplx_space_sandbox_model::ExecutionGeneration::new(7).unwrap();
    let local_artifact_id = disk_operation_snapshot_artifact_id(
        &sandbox_id,
        request_id,
        execution_generation,
        SandboxState::Started,
    );
    let snapshot_name = disk_operation_snapshot_name(request_id).expect("generated name is valid");
    let paths = disk_operation_snapshot_paths(
        pplx_space_host_layout::paths::SANDBOX_CACHE_DIR,
        &sandbox_id,
        &snapshot_name,
    );
    let artifact = crate::db::LocalArtifactRow {
        local_artifact_id,
        sandbox_id: sandbox_id.clone(),
        artifact_kind: pplx_space_snapshot_model::ArtifactKind::Disk,
        artifact_origin: pplx_space_snapshot_model::ArtifactOrigin::Snapshot,
        snapshot_name: Some(snapshot_name.clone()),
        local_path: paths.artifact.clone(),
    };

    assert!(disk_operation_snapshot_artifact_matches(
        &artifact,
        local_artifact_id,
        &sandbox_id,
        &snapshot_name,
        &paths,
    ));
    assert!(!disk_operation_snapshot_artifact_matches(
        &artifact,
        disk_operation_snapshot_artifact_id(
            &sandbox_id,
            request_id,
            pplx_space_sandbox_model::ExecutionGeneration::new(8).unwrap(),
            SandboxState::Paused,
        ),
        &sandbox_id,
        &snapshot_name,
        &paths,
    ));
    assert_eq!(
        disk_operation_snapshot_response(local_artifact_id, &snapshot_name)
            .published_artifact
            .expect("operation snapshot publishes an artifact")
            .local_artifact_id,
        local_artifact_id.to_string(),
    );
}

#[test]
fn operation_snapshot_creates_bytes_before_publishing_and_rejects_unbound_bytes() {
    assert_eq!(
        operation_snapshot_next_step(false, false),
        OperationSnapshotNextStep::CreateSnapshot,
        "a missing path must be created before its artifact row is published"
    );
    assert_eq!(
        operation_snapshot_next_step(false, true),
        OperationSnapshotNextStep::RemoveUnboundSnapshot,
        "bytes without an artifact row must be deleted rather than adopted"
    );
    assert_eq!(
        operation_snapshot_next_step(true, true),
        OperationSnapshotNextStep::Replay,
    );
    assert_eq!(
        operation_snapshot_next_step(true, false),
        OperationSnapshotNextStep::RejectMissingArtifactPath,
        "an artifact row whose immutable bytes disappeared is corrupt, not a new capture",
    );
}

#[test]
fn operation_snapshot_crash_orphan_requires_cleanup_before_recapture() {
    assert_eq!(
        operation_snapshot_next_step(false, true),
        OperationSnapshotNextStep::RemoveUnboundSnapshot,
        "an unpublished deterministic path must be removed under the artifact lock so retry can progress"
    );
}

#[test]
fn operation_snapshot_removes_an_unbound_path_after_create_failure() {
    assert_eq!(
        operation_snapshot_create_disposition(Some(tonic::Code::AlreadyExists), true),
        OperationSnapshotCreateDisposition::RemoveUnboundSnapshot,
    );
    assert_eq!(
        operation_snapshot_create_disposition(Some(tonic::Code::Internal), true),
        OperationSnapshotCreateDisposition::RemoveUnboundSnapshot,
        "node-manager currently maps an existing btrfs destination to Internal",
    );
}

#[test]
fn operation_snapshot_lost_response_requires_unbound_path_cleanup() {
    assert_eq!(
        operation_snapshot_create_disposition(Some(tonic::Code::Unavailable), true),
        OperationSnapshotCreateDisposition::RemoveUnboundSnapshot,
        "no concurrent publisher can bind the path while this handler holds the artifact lock"
    );
}

#[test]
fn operation_snapshot_removes_bytes_when_registry_publication_fails() {
    assert_eq!(
        operation_snapshot_publish_failure_disposition(false),
        OperationSnapshotPublishFailureDisposition::RemoveCreatedSnapshot,
    );
}

#[test]
fn operation_snapshot_removes_bytes_when_post_create_fence_fails() {
    assert_eq!(
        operation_snapshot_post_create_disposition(false),
        OperationSnapshotPostCreateDisposition::RemoveCreatedSnapshot,
    );
}

#[test]
fn durable_fork_copy_rejects_snapshot_destination_origin() {
    let error = durable_copy_destination_origin(crate::proto::ArtifactOrigin::Snapshot as i32)
        .expect_err("durable fork copies do not carry a destination snapshot name");

    assert_eq!(error.code(), tonic::Code::InvalidArgument);
    assert_eq!(
        durable_copy_destination_origin(crate::proto::ArtifactOrigin::Fork as i32).unwrap(),
        ArtifactOrigin::Fork
    );
}

#[test]
fn disk_operation_snapshot_uses_the_disk_prefix() {
    let request_id = Uuid::now_v7();
    assert_eq!(
        disk_operation_snapshot_name(request_id)
            .expect("disk operation snapshot name")
            .as_str(),
        format!("d-operation-{request_id}"),
    );
}

#[test]
fn operation_snapshot_paths_separate_capture_from_encoded_artifact() {
    let sandbox_id = SandboxId::new().to_string();
    let snapshot_name =
        disk_operation_snapshot_name(Uuid::now_v7()).expect("disk operation snapshot name");

    let paths = disk_operation_snapshot_paths(
        pplx_space_host_layout::paths::SANDBOX_CACHE_DIR,
        &sandbox_id,
        &snapshot_name,
    );

    assert_eq!(
        paths.capture,
        format!("/space/cache/firecracker/sandboxes/{sandbox_id}/snapshots/{snapshot_name}"),
        "the exact read-only capture remains at the node-manager named-snapshot path",
    );
    assert_eq!(
        paths.artifact,
        format!(
            "/space/cache/firecracker/sandboxes/{sandbox_id}/snapshots/{snapshot_name}-artifact"
        ),
        "the encoded stream needs a writable directory separate from the read-only capture",
    );
}

/// A retry of an operation snapshot that already completed is answered with
/// its earlier result before the write gate admits anything, so a node sealed
/// since answers it too, though the sandbox it captured has been suspended
/// and a new snapshot of it is refused.
#[tokio::test]
async fn a_completed_operation_snapshot_is_replayed_on_a_sealed_node() {
    let scratch = tempfile::tempdir().unwrap();
    let conn = crate::db::test_conn();
    let sandbox_id = pplx_space_sandbox_model::SandboxId::new().to_string();
    crate::service::tests::service_tests::insert_test_sandbox(
        &conn,
        &sandbox_id,
        SandboxState::Started,
    );
    // An encrypted volume skips the plaintext-COW probe of the host cache.
    conn.execute(
        "UPDATE sandboxes SET volume_encryption = 1 WHERE id = ?1",
        [&sandbox_id],
    )
    .unwrap();
    let service = SandboxManagerService::new_with_options(
        conn,
        crate::service::SandboxManagerServiceOptions::for_test_in(scratch.path()),
    );
    let request_id = Uuid::now_v7();
    let generation = pplx_space_sandbox_model::ExecutionGeneration::new(3).unwrap();
    let request = |request_id: Uuid| {
        crate::service::activation_support::internal_request(crate::proto::CreateSnapshotRequest {
            sandbox_id: sandbox_id.clone(),
            r#type: crate::proto::SnapshotType::Disk as i32,
            creation_identity: Some(crate::proto::SnapshotCreationIdentity {
                request_id: request_id.to_string(),
            }),
            source: Some(crate::proto::CreateSnapshotSource {
                source: Some(
                    crate::proto::create_snapshot_source::Source::CurrentSandbox(
                        crate::proto::CurrentSandboxSnapshotSource {
                            execution_generation: generation.get(),
                            lifecycle: crate::proto::SnapshotSourceLifecycle::Started as i32,
                        },
                    ),
                ),
            }),
            ..Default::default()
        })
    };
    // The snapshot the first call published, of the running sandbox.
    let name = disk_operation_snapshot_name(request_id).unwrap();
    let artifact_id = disk_operation_snapshot_artifact_id(
        &sandbox_id,
        request_id,
        generation,
        SandboxState::Started,
    );
    let paths =
        disk_operation_snapshot_paths(&service.cache_root().to_string_lossy(), &sandbox_id, &name);
    std::fs::create_dir_all(&paths.artifact).unwrap();
    crate::db::publish_named_local_artifact(
        &service.db.lock().unwrap(),
        &sandbox_id,
        artifact_id,
        pplx_space_snapshot_model::ArtifactKind::Disk,
        &name,
        &paths.artifact,
    )
    .unwrap();
    // A drain seals the node and suspends the sandbox.
    service.activation_seal.seal(&service.db, 7).unwrap();
    crate::db::test_fixtures::state_write_fixtures::write_status(
        &service.db.lock().unwrap(),
        &sandbox_id,
        SandboxState::Suspended,
    );

    let replayed = service
        .create_snapshot(request(request_id))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        replayed
            .published_artifact
            .map(|artifact| artifact.local_artifact_id),
        Some(artifact_id.to_string())
    );
    let refused = service
        .create_snapshot(request(Uuid::now_v7()))
        .await
        .unwrap_err();
    assert_eq!(
        refused.code(),
        tonic::Code::FailedPrecondition,
        "{refused:?}"
    );
    assert!(
        refused
            .message()
            .starts_with(crate::activation_seal::ACTIVATIONS_SEALED_MARKER),
        "{refused:?}"
    );
    service.activation_seal.assert_quiesced();
}
