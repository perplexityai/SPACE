use super::*;

#[test]
fn capture_and_restore_preserve_storage_and_execution_ownership() {
    let artifacts = vec![
        VmStateArtifact {
            name: "machine-state".into(),
            relative_path: "vmstate".into(),
        },
        VmStateArtifact {
            name: "memory".into(),
            relative_path: "memory".into(),
        },
    ];
    let storage = VmStateStorage {
        directory: Some("/snapshot/staging".into()),
        encrypted_volume_mount: Some("/snapshot/encrypted".into()),
    };
    let fence = ExecutionFence {
        execution_id: "execution".into(),
        ..Default::default()
    };
    let reference = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    let id = "018e390d-4dd8-7000-8000-000000000001";
    let capture = FirecrackerCapture::try_from(CaptureVmStateRequest {
        sandbox_id: id.into(),
        environment_reference: Some(reference.into()),
        artifacts: artifacts.clone(),
        storage: Some(storage.clone()),
        execution_fence: Some(fence.clone()),
    })
    .unwrap();
    let environment = capture.environment.unwrap();
    assert_eq!(environment.sandbox_id, id);
    assert_eq!(environment.reference, reference);
    assert_eq!(capture.execution_fence, Some(fence.clone()));
    assert_eq!(capture.dest_dir, storage.directory);
    assert_eq!(
        capture.snapshot_volume_mount_dir,
        storage.encrypted_volume_mount
    );
    let limit = DriveRateLimit {
        bandwidth: Some(TokenBucket {
            size: 4096,
            refill_time_ms: 100,
        }),
        operations: None,
    };
    let restore = FirecrackerRestore::try_from(RestoreVmStateRequest {
        sandbox_id: id.into(),
        artifacts,
        storage: Some(storage.clone()),
        execution_fence: Some(fence.clone()),
        rootfs_rate_limit: Some(limit),
    })
    .unwrap();
    assert_eq!(restore.sandbox_id, id);
    assert_eq!(
        (
            restore.snapshot_path.as_str(),
            restore.mem_file_path.as_str()
        ),
        ("vmstate", "memory")
    );
    assert_eq!(restore.execution_fence, Some(fence));
    assert_eq!(restore.source_dir, storage.directory);
    assert_eq!(
        restore.snapshot_volume_mount_dir,
        storage.encrypted_volume_mount
    );
    assert_eq!(restore.rootfs_rate_limit, Some(limit));
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
