use super::*;

#[test]
fn capture_and_restore_preserve_storage_and_execution_ownership() {
    let id = "018e390d-4dd8-7000-8000-000000000001";
    let reference = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    let fence = ExecutionFence {
        execution_id: "018e390d-4dd8-7000-8000-000000000002".into(),
        ..Default::default()
    };
    let limit = DriveRateLimit {
        bandwidth: Some(TokenBucket {
            size: 4096,
            refill_time_ms: 100,
        }),
        operations: None,
    };
    for encrypted in [false, true] {
        let storage = VmStateStorage {
            directory: Some(
                if encrypted {
                    "../ignored"
                } else {
                    "/snapshot/staging"
                }
                .into(),
            ),
            encrypted_volume_mount: encrypted.then(|| "/snapshot/encrypted".into()),
        };
        let files = if encrypted {
            artifacts("ignored".into(), "ignored".into())
        } else {
            artifacts("/vmstate".into(), "/memory".into())
        };
        let capture = FirecrackerCapture::try_from(CaptureVmStateRequest {
            sandbox_id: id.into(),
            environment_reference: Some(reference.into()),
            artifacts: files.clone(),
            storage: Some(storage.clone()),
            execution_fence: Some(fence.clone()),
        })
        .unwrap();
        let restore = FirecrackerRestore::try_from(RestoreVmStateRequest {
            sandbox_id: id.into(),
            artifacts: files,
            storage: Some(storage),
            execution_fence: Some(fence.clone()),
            rootfs_rate_limit: Some(limit),
        })
        .unwrap();
        assert_eq!(
            capture.environment_reference,
            Some(reference.parse().unwrap())
        );
        assert_eq!(restore.rootfs_rate_limit, Some(limit));
        for (sandbox, parsed_fence, storage) in [
            (capture.sandbox_id, capture.execution_fence, capture.storage),
            (restore.sandbox_id, restore.execution_fence, restore.storage),
        ] {
            assert_eq!(sandbox.id(), id.parse().unwrap());
            assert_eq!(parsed_fence.current().to_string(), fence.execution_id);
            match storage {
                SnapshotStorage::Encrypted(mount) => {
                    assert!(encrypted);
                    assert_eq!(path_str(&mount).unwrap(), "/snapshot/encrypted");
                }
                SnapshotStorage::Plain { files, directory } => {
                    assert!(!encrypted);
                    assert_eq!((files.state(), files.memory()), ("vmstate", "memory"));
                    assert_eq!(
                        path_str(directory.as_ref().unwrap()).unwrap(),
                        "/snapshot/staging"
                    );
                }
            }
        }
    }
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
fn capture_validates_optional_environment_without_relocating_identity() {
    let id = "018e390d-4dd8-7000-8000-000000000001";
    let reference = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    for (sandbox_id, reference, accepted) in [
        (id, None, true),
        (id, Some(reference), true),
        (id, Some("invalid"), false),
        (id, Some(""), false),
        ("deadbeef", Some(reference), false),
        ("deadbeef", None, false),
    ] {
        let result = FirecrackerCapture::try_from(CaptureVmStateRequest {
            sandbox_id: sandbox_id.into(),
            environment_reference: reference.map(str::to_owned),
            execution_fence: Some(ExecutionFence {
                execution_id: "018e390d-4dd8-7000-8000-000000000002".into(),
                ..Default::default()
            }),
            artifacts: ["machine-state", "memory"]
                .into_iter()
                .map(|name| VmStateArtifact {
                    name: name.into(),
                    relative_path: if name == "machine-state" {
                        "vmstate"
                    } else {
                        "memory"
                    }
                    .into(),
                })
                .collect(),
            ..Default::default()
        });
        assert_eq!(result.is_ok(), accepted);
        if let Ok(capture) = result {
            assert_eq!(capture.sandbox_id.as_str(), sandbox_id);
            assert_eq!(
                capture.environment_reference,
                reference.map(|value| value.parse().unwrap())
            );
        }
    }
}

pub(crate) fn artifacts(state: String, memory: String) -> Vec<VmStateArtifact> {
    vec![
        VmStateArtifact {
            name: "machine-state".into(),
            relative_path: state,
        },
        VmStateArtifact {
            name: "memory".into(),
            relative_path: memory,
        },
    ]
}

#[test]
fn missing_execution_fences_are_refused_at_the_snapshot_boundary() {
    let id = "018e390d-4dd8-7000-8000-000000000001";
    let capture = FirecrackerCapture::try_from(CaptureVmStateRequest {
        sandbox_id: id.into(),
        artifacts: artifacts("vmstate".into(), "memory".into()),
        ..Default::default()
    });
    let restore = FirecrackerRestore::try_from(RestoreVmStateRequest {
        sandbox_id: id.into(),
        artifacts: artifacts("vmstate".into(), "memory".into()),
        ..Default::default()
    });
    for error in [capture.unwrap_err(), restore.unwrap_err()] {
        assert_eq!(error.code(), tonic::Code::FailedPrecondition);
    }
}
