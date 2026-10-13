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
