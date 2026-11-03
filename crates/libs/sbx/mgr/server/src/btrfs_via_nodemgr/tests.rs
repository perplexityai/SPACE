use super::*;

const RPC: &str = "BtrfsEncodeRootfsSend";

#[test]
fn capture_response_proves_deferral_only_with_identity_size_and_no_stream() {
    let id = uuid::Uuid::now_v7();
    let id_text = id.to_string();
    let source = Some(id_text.as_str());

    assert_eq!(
        CapturedRootfs::from_response(RPC, true, 0, source, Some(42)).unwrap(),
        CapturedRootfs::Deferred {
            source_id: id,
            rootfs_bytes: 42
        }
    );
    // Node-manager may materialize despite the request; the stream wins.
    assert_eq!(
        CapturedRootfs::from_response(RPC, true, 9, source, Some(42)).unwrap(),
        CapturedRootfs::Stream {
            stream_bytes: 9,
            source_id: Some(id)
        }
    );
    // Without retention there is nothing to defer to.
    assert_eq!(
        CapturedRootfs::from_response(RPC, true, 9, None, None).unwrap(),
        CapturedRootfs::Stream {
            stream_bytes: 9,
            source_id: None
        }
    );
    // A materialized capture with a retained source needs no size: older
    // node-managers never send one, and the stream file sizes the artifact.
    assert_eq!(
        CapturedRootfs::from_response(RPC, false, 9, source, None).unwrap(),
        CapturedRootfs::Stream {
            stream_bytes: 9,
            source_id: Some(id)
        }
    );
}

#[test]
fn capture_response_without_retained_size_fails_instead_of_sizing_zero() {
    let id = uuid::Uuid::now_v7().to_string();
    let error = CapturedRootfs::from_response(RPC, true, 0, Some(&id), None).unwrap_err();
    assert!(matches!(
        error,
        BtrfsViaNodemgrError::InvalidResponse { .. }
    ));
    // Not FailedPrecondition: rootfs.ext4 is already gone, so a Full
    // fallback cannot succeed.
    assert_eq!(error.into_status().code(), tonic::Code::Internal);
    assert!(matches!(
        CapturedRootfs::from_response(RPC, true, 0, Some("not-a-uuid"), Some(1)),
        Err(BtrfsViaNodemgrError::InvalidResponse { .. })
    ));
}

#[tokio::test]
async fn capability_cache_reuses_a_fresh_positive_answer_and_reprobes_a_negative_one() {
    let cache = CapabilityCache::new();
    let probes = std::sync::atomic::AtomicUsize::new(0);
    let probe = |answer: bool| {
        let probes = &probes;
        async move {
            probes.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            answer
        }
    };
    assert!(cache.get(probe(true)).await);
    assert!(cache.get(probe(false)).await, "fresh answer reused");
    assert_eq!(probes.load(std::sync::atomic::Ordering::SeqCst), 1);
    cache.clear();
    assert!(!cache.get(probe(false)).await);
    assert!(
        !cache.get(probe(true)).await,
        "a negative answer is also reused within its shorter life"
    );
    assert_eq!(probes.load(std::sync::atomic::Ordering::SeqCst), 2);
}

#[test]
fn deferred_capture_is_on_unless_a_deployment_opts_out() {
    assert!(retain_send_sources_from(None));
    assert!(!retain_send_sources_from(Some("false")));
}

/// A restore that writes its own rootfs asks node-manager for an empty
/// `instance/` subvolume, and reads back what is there: a subvolume, a plain
/// directory node-manager kept, or nothing on a node-manager that predates the
/// RPC, where the caller may fall back to the plain directory it always made. Any other refusal is an error the restore fails on, not a silent
/// fallback to a directory nothing can snapshot later.
#[tokio::test(flavor = "current_thread")]
async fn a_sandbox_instance_is_created_as_a_subvolume_or_reported_unsupported() {
    use pplx_space_node_mgr_client::proto::{
        BtrfsCreateSandboxInstanceRequest, BtrfsCreateSandboxInstanceResponse,
    };
    let target =
        || InstanceTarget::RestoreScope(pplx_space_common_core::types::TransferScopeId::new());

    let created = crate::node_manager_transport::fake::FakeNodeManager::default()
        .unary(
            "BtrfsCreateSandboxInstance",
            |request: BtrfsCreateSandboxInstanceRequest| {
                assert_eq!(request.sandbox_id, "sbx");
                assert!(request.target.is_some(), "the target must be sent");
                Ok(BtrfsCreateSandboxInstanceResponse { subvolume: true })
            },
        )
        .serve();
    assert_eq!(
        create_sandbox_instance("sbx", target(), &created.connector())
            .await
            .unwrap(),
        PreparedInstance::Subvolume
    );
    assert_eq!(
        created.calls(),
        vec!["BtrfsCreateSandboxInstance".to_owned()]
    );

    let kept = crate::node_manager_transport::fake::FakeNodeManager::default()
        .unary(
            "BtrfsCreateSandboxInstance",
            |_: BtrfsCreateSandboxInstanceRequest| {
                Ok(BtrfsCreateSandboxInstanceResponse { subvolume: false })
            },
        )
        .serve();
    assert_eq!(
        create_sandbox_instance("sbx", target(), &kept.connector())
            .await
            .unwrap(),
        PreparedInstance::PlainDirectory,
        "a kept plain directory must not read as a subvolume"
    );

    let old = crate::node_manager_transport::fake::FakeNodeManager::default().serve();
    assert_eq!(
        create_sandbox_instance("sbx", target(), &old.connector())
            .await
            .unwrap(),
        PreparedInstance::Unsupported,
        "a node-manager without the RPC must read as unsupported"
    );

    let refusing = crate::node_manager_transport::fake::FakeNodeManager::default()
        .unary(
            "BtrfsCreateSandboxInstance",
            |_: BtrfsCreateSandboxInstanceRequest| -> Result<BtrfsCreateSandboxInstanceResponse, Status> {
                Err(Status::failed_precondition("instance exists and is not a subvolume"))
            },
        )
        .serve();
    assert!(
        create_sandbox_instance("sbx", target(), &refusing.connector())
            .await
            .is_err(),
        "a refusal must fail the restore"
    );
}
