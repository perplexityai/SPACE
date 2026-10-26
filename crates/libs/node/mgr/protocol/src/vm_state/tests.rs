use super::*;

#[test]
fn capture_and_restore_preserve_storage_and_execution_ownership() {
    let legacy = CreateSnapshotRequest {
        sandbox_id: String::new(),
        environment: Some(EnvironmentSnapshotInput {
            sandbox_id: "018e390d-4dd8-7000-8000-000000000001".into(),
            reference: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                .into(),
        }),
        snapshot_path: "vmstate".into(),
        mem_file_path: "memory".into(),
        dest_dir: Some("/snapshot/staging".into()),
        snapshot_volume_mount_dir: Some("/snapshot/encrypted".into()),
        execution_fence: Some(ExecutionFence {
            execution_id: "execution".into(),
            ..Default::default()
        }),
    };
    let generic = CaptureVmStateRequest::try_from(legacy.clone()).unwrap();
    assert_eq!(
        generic.sandbox_id,
        legacy.environment.as_ref().unwrap().sandbox_id
    );
    assert_eq!(CreateSnapshotRequest::try_from(generic).unwrap(), legacy);
    let legacy = LoadSnapshotRequest {
        sandbox_id: "018e390d-4dd8-7000-8000-000000000001".into(),
        snapshot_path: "vmstate".into(),
        mem_file_path: "memory".into(),
        source_dir: Some("/snapshot/staging".into()),
        snapshot_volume_mount_dir: Some("/snapshot/encrypted".into()),
        execution_fence: legacy.execution_fence,
        rootfs_rate_limit: Some(DriveRateLimit {
            bandwidth: Some(TokenBucket {
                size: 4096,
                refill_time_ms: 100,
            }),
            operations: None,
        }),
    };
    assert_eq!(
        LoadSnapshotRequest::try_from(RestoreVmStateRequest::from(legacy.clone())).unwrap(),
        legacy
    );
}

#[test]
fn state_profile_rejects_partial_duplicate_and_foreign_artifact_sets() {
    for names in [
        vec![],
        vec!["memory"],
        vec!["machine-state"],
        vec!["memory", "memory", "machine-state"],
        vec!["machine-state", "memory", "extra"],
    ] {
        let artifacts = names
            .into_iter()
            .map(|name| VmStateArtifact {
                name: name.into(),
                relative_path: "file".into(),
            })
            .collect();
        assert!(matches!(
            firecracker_files(artifacts),
            Err(StateContractError::Missing(_)
                | StateContractError::Duplicate(_)
                | StateContractError::Unknown(_))
        ));
    }
    let files = firecracker_files(vec![
        VmStateArtifact {
            name: "memory".into(),
            relative_path: "guest-memory".into(),
        },
        VmStateArtifact {
            name: "machine-state".into(),
            relative_path: "device-state".into(),
        },
    ])
    .unwrap();
    assert_eq!(files, ("device-state".into(), "guest-memory".into()));
}

#[test]
fn capture_conversion_rejects_dual_sandbox_identities() {
    let source = "018e390d-4dd8-7000-8000-000000000001";
    for top_level in [source, "018e390d-4dd8-7000-8000-000000000002"] {
        let request = CreateSnapshotRequest {
            sandbox_id: top_level.into(),
            environment: Some(EnvironmentSnapshotInput {
                sandbox_id: source.into(),
                reference:
                    "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".into(),
            }),
            ..Default::default()
        };
        assert!(matches!(
            CaptureVmStateRequest::try_from(request),
            Err(StateContractError::DualSandboxIdentities)
        ));
    }
}
