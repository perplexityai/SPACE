use super::*;

#[test]
fn encrypted_rootfs_rpc_error_preserves_status_code_and_metadata() {
    let mut metadata = tonic::metadata::MetadataMap::new();
    metadata.insert("x-test-reason", "invalid-key".parse().unwrap());
    let error = EncryptedRootfsPrepareError::Rpc(tonic::Status::with_metadata(
        tonic::Code::FailedPrecondition,
        "federated service grant is required",
        metadata,
    ));

    let status = error.into_status("encrypted rootfs preparation failed".into());

    assert_eq!(status.code(), tonic::Code::FailedPrecondition);
    assert_eq!(
        status.metadata().get("x-test-reason").unwrap(),
        "invalid-key"
    );
}

/// The degrade decision is the guard against booting the bare rootfs file
/// over a live composite: only failures that prove node-manager built
/// nothing (never reached, or answered — its unwind ran) may degrade. Every
/// transport-ambiguous code must fail the start, because the lost reply may
/// hide a prepare that succeeded node-side.
#[test]
fn degrade_is_allowed_only_when_no_partial_device_state_is_possible() {
    let rpc = |code: tonic::Code| PoolRootfsPrepareError::Rpc(tonic::Status::new(code, "x"));

    for definitive in [
        tonic::Code::InvalidArgument,
        tonic::Code::FailedPrecondition,
        tonic::Code::ResourceExhausted,
        tonic::Code::Unimplemented,
        tonic::Code::PermissionDenied,
        tonic::Code::Unauthenticated,
        tonic::Code::NotFound,
    ] {
        assert!(
            rpc(definitive).degrade_to_rootfs_file_is_safe(),
            "{definitive:?} is a definitive node answer; the file is safe"
        );
    }
    // Internal is a device failure whose unwind is best-effort (and a
    // panicked task has no unwind at all), so devices may survive it.
    for ambiguous in [
        tonic::Code::Internal,
        tonic::Code::Unavailable,
        tonic::Code::DeadlineExceeded,
        tonic::Code::Cancelled,
        tonic::Code::Unknown,
        tonic::Code::Aborted,
        tonic::Code::DataLoss,
    ] {
        assert!(
            !rpc(ambiguous).degrade_to_rootfs_file_is_safe(),
            "{ambiguous:?} may hide a node-side success; must not degrade"
        );
    }
}

const DIGEST: &str = "sha256:5e0f1d2c3b4a59687766554433221100ffeeddccbbaa99887766554433221100";
const OTHER_DIGEST: &str =
    "sha256:0000000000000000000000000000000000000000000000000000000000000001";

/// The never-format guard. node-manager formats a fresh COW wherever
/// `instance/cow.luks` is absent, so a start that would hand it no COW source
/// while the checkpoint holds the guest's writes either hands it the COW of
/// the encrypted suspend checkpoint the row pins, written over the template
/// the row names, to lay, as a restore does, or, for any other checkpoint, is
/// refused before node-manager is asked. Every other shape reaches node-manager as it did: a first start, a
/// restart over its own COW, and a restore that hands the staged COW over.
#[tokio::test(flavor = "current_thread")]
async fn a_start_never_formats_a_cow_over_a_staged_checkpoints_writes() {
    use pplx_space_node_mgr_client::proto::{
        PrepareEncryptedRootfsRequest, PrepareEncryptedRootfsResponse,
    };
    let sent = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let recorded = std::sync::Arc::clone(&sent);
    let node = crate::node_manager_transport::fake::FakeNodeManager::default()
        .unary(
            "PrepareEncryptedRootfs",
            move |request: PrepareEncryptedRootfsRequest| {
                recorded.lock().unwrap().push(request.restore_cow_source);
                Ok(PrepareEncryptedRootfsResponse {
                    mapper_path: "/dev/mapper/rootfs".into(),
                })
            },
        )
        .serve();
    let fence = pplx_space_node_mgr_client::execution_fence::ExecutionFence::new(
        pplx_space_common_core::types::SandboxActivationAttemptId::new(),
        None,
    );
    #[derive(Clone, Copy, Debug, PartialEq)]
    enum Sidecar {
        None,
        OwnSuspend,
        AnotherSandbox,
        Plaintext,
        AnotherArtifact,
        AnotherTemplate,
        /// The row records no template digest to clone the origin from.
        Undigested,
        /// A stop after the suspend dropped the row's pin.
        Unpinned,
    }
    #[derive(Debug, PartialEq)]
    enum Expect {
        Refused,
        Sent(Option<&'static str>),
    }
    // (instance COW, staged checkpoint COW, sidecar, restore source, outcome)
    let cases = [
        (false, true, Sidecar::None, false, Expect::Refused),
        (false, true, Sidecar::AnotherSandbox, false, Expect::Refused),
        (false, true, Sidecar::Plaintext, false, Expect::Refused),
        (
            false,
            true,
            Sidecar::AnotherArtifact,
            false,
            Expect::Refused,
        ),
        (
            false,
            true,
            Sidecar::AnotherTemplate,
            false,
            Expect::Refused,
        ),
        (false, true, Sidecar::Undigested, false, Expect::Refused),
        (
            false,
            true,
            Sidecar::Unpinned,
            false,
            Expect::Sent(Some("snapshot")),
        ),
        (
            false,
            true,
            Sidecar::OwnSuspend,
            false,
            Expect::Sent(Some("snapshot")),
        ),
        (false, false, Sidecar::None, false, Expect::Sent(None)),
        (true, false, Sidecar::None, false, Expect::Sent(None)),
        (true, true, Sidecar::OwnSuspend, false, Expect::Sent(None)),
        (
            false,
            true,
            Sidecar::None,
            true,
            Expect::Sent(Some("snapshot")),
        ),
    ];
    for (instance, staged, sidecar, source, expect) in cases {
        let cache = tempfile::tempdir().unwrap();
        let id = pplx_space_common_core::types::SandboxId::new().to_string();
        let dir = cache.path().join(&id);
        std::fs::create_dir_all(dir.join("instance")).unwrap();
        std::fs::create_dir_all(dir.join("snapshot")).unwrap();
        if instance {
            std::fs::write(dir.join("instance/cow.luks"), b"cow").unwrap();
        }
        if staged {
            std::fs::write(dir.join("snapshot/cow.luks"), b"cow").unwrap();
        }
        let pin = pplx_space_common_core::types::LocalArtifactId::new();
        if sidecar != Sidecar::None {
            let mut meta = serde_json::json!({
                "version": 1,
                "sandbox_id": id,
                "size_bytes": 1,
                "created_at": "2026-11-09T00:00:00Z",
                "source": {"runtime": "fc", "template_id": "seed", "content_hash": DIGEST},
                "encrypted": true,
                "local_artifact_id": pin.to_string(),
                "artifact_kind": "full",
                "artifact_origin": "suspend",
            });
            match sidecar {
                Sidecar::AnotherSandbox => {
                    meta["sandbox_id"] = pplx_space_common_core::types::SandboxId::new()
                        .to_string()
                        .into();
                }
                Sidecar::Plaintext => meta["encrypted"] = false.into(),
                Sidecar::AnotherArtifact => {
                    meta["local_artifact_id"] =
                        pplx_space_common_core::types::LocalArtifactId::new()
                            .to_string()
                            .into();
                }
                Sidecar::AnotherTemplate => {
                    meta["source"]["content_hash"] = OTHER_DIGEST.into();
                }
                Sidecar::None | Sidecar::OwnSuspend | Sidecar::Undigested | Sidecar::Unpinned => {}
            }
            std::fs::write(
                dir.join("snapshot/snapshot.json"),
                serde_json::to_vec(&meta).unwrap(),
            )
            .unwrap();
        }
        let staged_cow = dir.join("snapshot/cow.luks").to_string_lossy().into_owned();
        let source = source.then(|| staged_cow.clone());
        let before = sent.lock().unwrap().len();

        let pinned = PinnedCheckpoint {
            template_hash: (sidecar != Sidecar::Undigested).then_some(DIGEST),
            local_artifact_id: (sidecar != Sidecar::Unpinned).then_some(pin),
            suspended: false,
        };
        let result = prepare_encrypted_rootfs(
            cache.path(),
            &id,
            pinned,
            fence,
            source.as_deref(),
            &node.connector(),
        )
        .await;

        let case = (instance, staged, sidecar, source.is_some());
        match expect {
            Expect::Refused => {
                let error = result.unwrap_err();
                assert!(
                    matches!(error, EncryptedRootfsPrepareError::CheckpointCowNotLaid),
                    "{case:?}: {error}"
                );
                assert_eq!(
                    error.into_status(String::new()).code(),
                    tonic::Code::FailedPrecondition
                );
                assert_eq!(
                    sent.lock().unwrap().len(),
                    before,
                    "{case:?}: node-manager was asked"
                );
            }
            Expect::Sent(expected) => {
                assert_eq!(result.unwrap(), "/dev/mapper/rootfs", "{case:?}");
                let sent = sent.lock().unwrap();
                assert_eq!(sent.len(), before + 1, "{case:?}");
                assert_eq!(
                    sent[before],
                    expected.map(|_| staged_cow.clone()),
                    "{case:?}: restore_cow_source"
                );
            }
        }
    }
}

/// A suspended sandbox's disk is its checkpoint, as a restore treats it
/// whenever the checkpoint holds a COW.
/// Its cold start, as a runtime upgrade boots it, hands
/// node-manager the checkpoint's COW to lay over whatever an earlier attempt
/// left in the instance, as a restore does. A restart or a stopped sandbox
/// opens the instance's own COW, and a checkpoint that isn't the one the row
/// pins is never laid.
#[tokio::test(flavor = "current_thread")]
async fn a_suspended_sandbox_boots_its_pinned_checkpoints_cow() {
    use pplx_space_node_mgr_client::proto::{
        PrepareEncryptedRootfsRequest, PrepareEncryptedRootfsResponse,
    };
    let sent = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let recorded = std::sync::Arc::clone(&sent);
    let node = crate::node_manager_transport::fake::FakeNodeManager::default()
        .unary(
            "PrepareEncryptedRootfs",
            move |request: PrepareEncryptedRootfsRequest| {
                recorded.lock().unwrap().push(request.restore_cow_source);
                Ok(PrepareEncryptedRootfsResponse {
                    mapper_path: "/dev/mapper/rootfs".into(),
                })
            },
        )
        .serve();
    let fence = pplx_space_node_mgr_client::execution_fence::ExecutionFence::new(
        pplx_space_common_core::types::SandboxActivationAttemptId::new(),
        None,
    );
    // (suspended, the sidecar names the pin, checkpoint COW laid)
    for (suspended, pinned_checkpoint, laid) in [
        (true, true, true),
        (false, true, false),
        (true, false, false),
    ] {
        let cache = tempfile::tempdir().unwrap();
        let id = pplx_space_common_core::types::SandboxId::new().to_string();
        let dir = cache.path().join(&id);
        std::fs::create_dir_all(dir.join("instance")).unwrap();
        std::fs::create_dir_all(dir.join("snapshot")).unwrap();
        std::fs::write(dir.join("instance/cow.luks"), b"an earlier attempt's").unwrap();
        std::fs::write(dir.join("snapshot/cow.luks"), b"sealed").unwrap();
        let pin = pplx_space_common_core::types::LocalArtifactId::new();
        let named = if pinned_checkpoint {
            pin
        } else {
            pplx_space_common_core::types::LocalArtifactId::new()
        };
        std::fs::write(
            dir.join("snapshot/snapshot.json"),
            serde_json::to_vec(&serde_json::json!({
                "version": 1,
                "sandbox_id": id,
                "size_bytes": 1,
                "created_at": "2026-11-09T00:00:00Z",
                "source": {"runtime": "fc", "template_id": "seed", "content_hash": DIGEST},
                "encrypted": true,
                "local_artifact_id": named.to_string(),
                "artifact_kind": "full",
                "artifact_origin": "suspend",
            }))
            .unwrap(),
        )
        .unwrap();
        let before = sent.lock().unwrap().len();

        prepare_encrypted_rootfs(
            cache.path(),
            &id,
            PinnedCheckpoint {
                template_hash: Some(DIGEST),
                local_artifact_id: Some(pin),
                suspended,
            },
            fence,
            None,
            &node.connector(),
        )
        .await
        .unwrap();

        let case = (suspended, pinned_checkpoint);
        let sent = sent.lock().unwrap();
        assert_eq!(sent.len(), before + 1, "{case:?}");
        assert_eq!(
            sent[before],
            laid.then(|| dir.join("snapshot/cow.luks").to_string_lossy().into_owned()),
            "{case:?}: restore_cow_source"
        );
    }
}
