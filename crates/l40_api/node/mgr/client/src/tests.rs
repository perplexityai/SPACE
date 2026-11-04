use pplx_space_common_core::types::SandboxId;

use super::*;

#[test]
fn snapshot_pages_retry_admission_without_losing_the_cursor_or_treating_errors_as_eof() {
    pplx_space_tokio_runtime::build_multi_thread_runtime()
        .unwrap()
        .block_on(async {
            let mut pages = SnapshotPages::new(SandboxId::new(), false);
            let mut calls = 0;
            let page = pages
                .next_with(|request| {
                    assert!(request.page_token.is_empty());
                    calls += 1;
                    std::future::ready(if calls < 3 {
                        Err(tonic::Status::resource_exhausted("inventory busy"))
                    } else {
                        Ok(tonic::Response::new(proto::ListBtrfsSnapshotsResponse {
                            snapshots: vec![proto::BtrfsSnapshotInfo {
                                name: "first".into(),
                                ..Default::default()
                            }],
                            next_page_token: "continuation".into(),
                        }))
                    })
                })
                .await
                .unwrap()
                .unwrap();
            assert_eq!(page[0].name, "first");
            assert_eq!(calls, 3);

            for code in [
                tonic::Code::ResourceExhausted,
                tonic::Code::Aborted,
                tonic::Code::PermissionDenied,
                tonic::Code::Unavailable,
            ] {
                let mut calls = 0;
                let error = pages
                    .next_with(|request| {
                        assert_eq!(request.page_token, "continuation");
                        calls += 1;
                        std::future::ready(Err(tonic::Status::new(code, "injected")))
                    })
                    .await
                    .unwrap_err();
                assert_eq!(error.code(), code);
                assert!((1..=5).contains(&calls), "retries must be bounded");
                if code != tonic::Code::ResourceExhausted {
                    assert_eq!(calls, 1);
                }
            }
            assert!(
                pages
                    .next_with(|request| {
                        assert_eq!(request.page_token, "continuation");
                        std::future::ready(Ok(tonic::Response::new(
                            proto::ListBtrfsSnapshotsResponse::default(),
                        )))
                    })
                    .await
                    .unwrap()
                    .unwrap()
                    .is_empty()
            );
            assert!(
                pages
                    .next_with(|_| std::future::ready(Err(tonic::Status::internal(
                        "must not fetch after EOF"
                    ))))
                    .await
                    .unwrap()
                    .is_none()
            );
        });
}

fn resource_status(free_bytes: u64) -> proto::NodeResourceStatusResponse {
    proto::NodeResourceStatusResponse {
        cache_disk: Some(proto::CacheDiskStatus {
            statvfs_total_bytes: 1_000,
            statvfs_free_bytes: free_bytes,
            statvfs_available_bytes: free_bytes,
            statvfs_available_fraction: free_bytes as f64 / 1_000.0,
            admission_state: proto::CacheDiskAdmissionState::Ready as i32,
            ..Default::default()
        }),
    }
}

fn network_pool_status(ready_slots: u32) -> proto::NetworkPoolStatusResponse {
    proto::NetworkPoolStatusResponse {
        network_pool_status: proto::NetworkPoolStatus::Healthy as i32,
        network_slots_ready: ready_slots,
        network_pool_min_ready: 1,
        network_slots_total: ready_slots,
        ..Default::default()
    }
}

#[test]
fn resource_status_reader_reuses_recent_success_after_read_failure() {
    let reader = NodeResourceStatusReader::with_failure_grace(Duration::from_secs(60));
    let now = Instant::now();
    let status = resource_status(900);

    let fresh = reader.observe_success(status.clone(), now);
    assert!(fresh.fallback_error().is_none());

    let recent = reader
        .status_for_failure(
            now + Duration::from_secs(59),
            NodeResourceStatusReadError::Rpc("node-manager unavailable".to_string()),
        )
        .expect("recent status remains inside the grace window");
    assert_eq!(
        recent
            .response
            .cache_disk
            .as_ref()
            .unwrap()
            .statvfs_free_bytes,
        900
    );
    assert!(matches!(
        recent.fallback_error(),
        Some(NodeResourceStatusReadError::Rpc(_))
    ));

    assert!(
        reader
            .status_for_failure(
                now + Duration::from_secs(61),
                NodeResourceStatusReadError::Rpc("node-manager unavailable".to_string()),
            )
            .is_none()
    );
}

#[test]
fn network_pool_status_reader_reuses_recent_success_after_read_failure() {
    let reader = NetworkPoolStatusReader::with_failure_grace(Duration::from_secs(60));
    let now = Instant::now();
    let status = network_pool_status(7);

    let fresh = reader.observe_success(status, now);
    assert!(fresh.fallback_error().is_none());

    let recent = reader
        .status_for_failure(
            now + Duration::from_secs(59),
            NetworkPoolStatusReadError::Rpc("node-manager unavailable".to_string()),
        )
        .expect("recent status remains inside the grace window");
    assert_eq!(recent.response.network_slots_ready, 7);
    assert!(matches!(
        recent.fallback_error(),
        Some(NetworkPoolStatusReadError::Rpc(_))
    ));

    assert!(
        reader
            .status_for_failure(
                now + Duration::from_secs(61),
                NetworkPoolStatusReadError::Rpc("node-manager unavailable".to_string()),
            )
            .is_none()
    );
}

#[test]
fn resource_status_reader_rejects_missing_cache_disk_payload() {
    let response = proto::NodeResourceStatusResponse { cache_disk: None };

    assert_eq!(
        cache_disk_status(&response).unwrap_err(),
        NodeResourceStatusReadError::MissingCacheDisk
    );
}

#[test]
fn resource_status_reader_reuses_recent_success_after_missing_cache_disk_payload() {
    let reader = NodeResourceStatusReader::with_failure_grace(Duration::from_secs(60));
    let now = Instant::now();

    reader.observe_success(resource_status(900), now);

    let recent = reader
        .status_for_failure(
            now + Duration::from_secs(1),
            NodeResourceStatusReadError::MissingCacheDisk,
        )
        .expect("missing payload uses recent status inside the grace window");
    assert_eq!(
        recent
            .response
            .cache_disk
            .as_ref()
            .unwrap()
            .statvfs_free_bytes,
        900
    );
    assert_eq!(
        recent.fallback_error(),
        Some(&NodeResourceStatusReadError::MissingCacheDisk)
    );
}
