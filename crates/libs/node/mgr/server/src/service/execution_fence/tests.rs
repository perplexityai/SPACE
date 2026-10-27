use super::*;
use pplx_space_common_core::types::SandboxActivationAttemptId;

#[rstest::rstest]
#[case::adopted(true, false, false)]
#[case::unadopted(false, false, false)]
#[case::released(false, true, false)]
#[case::shutdown(true, false, true)]
#[tokio::test]
async fn execution_preflight_waits_for_cleanup_and_revalidates(
    #[case] adopted: bool,
    #[case] release_lease: bool,
    #[case] shutdown: bool,
) {
    let service = crate::service::tests::service_tests::service_for_unit_tests().await;
    service.pool.set_all_slots_ready_for_test();
    let id = SandboxId::new().to_string();
    let lease = service
        .pool
        .allocate(
            &id,
            None,
            Some(crate::slot_pool::NewLeaseMemoryReservation {
                memory_reservation_mib: 4096,
            }),
        )
        .await
        .unwrap();
    let fence = adopted.then(|| ExecutionFence::new(SandboxActivationAttemptId::new(), None));
    if let Some(fence) = fence {
        service.pool.bind_execution(&id, fence).unwrap();
    }
    let reservations = service.pool.active_memory_reservations().unwrap();
    let cleanup = service.claim_sandbox_mutation(&id).unwrap();

    let mut preflight = Box::pin(service.verify_execution_authority());
    assert!(futures_util::poll!(preflight.as_mut()).is_pending());
    // Finish candidate discovery before checking that preflight waits on cleanup.
    service.device_op_tasks.close();
    service.device_op_tasks.wait().await;
    service.device_op_tasks.reopen();
    assert!(futures_util::poll!(preflight.as_mut()).is_pending());
    // Recovery must still be able to take the host gate while preflight waits.
    let gate = tokio::time::timeout(Duration::from_secs(1), service.lock_sandbox_cgroups(&id))
        .await
        .expect("preflight must not hold the cgroup gate while awaiting cleanup");
    drop(gate);
    if release_lease {
        service.pool.release(&id).unwrap().unwrap();
    }
    if shutdown {
        service.pool.shutdown_token.cancel();
    } else {
        drop(cleanup);
    }
    let result = tokio::time::timeout(Duration::from_secs(1), preflight)
        .await
        .expect("preflight must wake after cleanup or shutdown");

    if shutdown {
        let error = result.unwrap_err();
        assert_eq!(error.code(), tonic::Code::Unavailable);
        assert_eq!(error.message(), "node-manager is shutting down");
    } else if adopted || release_lease {
        result.unwrap();
    } else {
        assert_eq!(result.unwrap_err().code(), tonic::Code::FailedPrecondition);
    }
    assert_eq!(service.pool.execution_fence(&id).unwrap(), fence);
    if release_lease {
        assert!(service.pool.get(&id).unwrap().is_none());
        assert!(
            service
                .pool
                .active_memory_reservations()
                .unwrap()
                .is_empty()
        );
    } else {
        let after = service.pool.get(&id).unwrap().unwrap();
        assert_eq!(
            (after.slot, after.generation),
            (lease.slot, lease.generation)
        );
        assert_eq!(
            service.pool.active_memory_reservations().unwrap(),
            reservations
        );
    }
}

#[tokio::test]
async fn execution_inspection_reads_retained_authority_without_a_lease_or_host_paths() {
    let service = crate::service::tests::service_tests::service_for_unit_tests().await;
    let id = SandboxId::new();
    let fence = ExecutionFence::new(SandboxActivationAttemptId::new(), None);
    for expected in [None, Some(fence)] {
        if let Some(fence) = expected {
            service.pool.bind_execution(&id.to_string(), fence).unwrap();
        }
        let mut request = Request::new(InspectSandboxRequest {
            sandbox_id: id.to_string(),
            execution_only: true,
            ..Default::default()
        });
        request
            .extensions_mut()
            .insert(pplx_space_common_svc::PeerIdentity::new(0, 0, None));
        let response = service.inspect_sandbox(request).await.unwrap();
        assert_eq!(
            response.into_inner().execution_fence,
            expected.map(Into::into)
        );
    }
}

#[tokio::test]
async fn execution_preflight_rejects_legacy_leases_without_releasing_them() {
    for count in [1, 2, 63, 64, 65] {
        let mut service = crate::service::tests::service_tests::service_for_unit_tests().await;
        service.pool = NetworkPool::open_in_memory_for_test(NetworkPoolConfig::new(count));
        service.pool.set_all_slots_ready_for_test();
        service.verify_execution_authority().await.unwrap();
        let mut leases = Vec::new();
        for _ in 0..count {
            let id = SandboxId::new().to_string();
            let lease = service
                .pool
                .allocate(
                    &id,
                    None,
                    Some(crate::slot_pool::NewLeaseMemoryReservation {
                        memory_reservation_mib: 4096,
                    }),
                )
                .await
                .unwrap();
            leases.push((id, lease));
        }
        leases.sort_by(|a, b| a.0.cmp(&b.0));
        let reservations = service.pool.active_memory_reservations().unwrap();
        let error = service.verify_execution_authority().await.unwrap_err();
        assert_eq!(error.code(), tonic::Code::FailedPrecondition);
        let expected_count = if count >= 64 {
            "at least 64".into()
        } else {
            count.to_string()
        };
        assert!(
            error.message().starts_with(&format!(
                "{expected_count} sandbox(s) have no execution authority;"
            )),
            "{error}"
        );
        for (index, (id, before)) in leases.iter().enumerate() {
            assert_eq!(
                error.message().contains(id),
                index < 8,
                "bounded ID sample: {error}"
            );
            assert_eq!(service.pool.execution_fence(id).unwrap(), None);
            let after = service.pool.get(id).unwrap().unwrap();
            assert_eq!(
                (before.slot, before.generation, before.pid),
                (after.slot, after.generation, after.pid)
            );
        }
        assert_eq!(
            reservations,
            service.pool.active_memory_reservations().unwrap()
        );
        let fence = ExecutionFence::new(SandboxActivationAttemptId::new(), None);
        for (id, _) in &leases {
            service.pool.bind_execution(id, fence).unwrap();
        }
        service.verify_execution_authority().await.unwrap();
        for (id, _) in &leases {
            assert_eq!(service.pool.execution_fence(id).unwrap(), Some(fence));
        }
    }
}

#[tokio::test]
async fn allocation_waits_for_previous_cleanup_before_entering_activation() {
    let service = crate::service::tests::service_tests::service_for_unit_tests().await;
    let sandbox = SandboxId::new().to_string();
    let cleanup = service.claim_sandbox_mutation(&sandbox).unwrap();
    let mut request = Request::new(AllocateRequest {
        sandbox_id: sandbox.clone(),
        execution_fence: Some(ExecutionFence::new(SandboxActivationAttemptId::new(), None).into()),
        ..Default::default()
    });
    request
        .extensions_mut()
        .insert(pplx_space_common_svc::PeerIdentity::new(0, 0, None));

    let mut allocation = Box::pin(service.allocate(request));
    assert!(futures_util::poll!(allocation.as_mut()).is_pending());
    drop(cleanup);

    // This test peer lacks the process identity required for a fenced
    // activation. Reaching that check proves the reset collision was awaited.
    assert_eq!(
        allocation.await.unwrap_err().code(),
        tonic::Code::PermissionDenied
    );
}

#[tokio::test]
async fn cancelled_allocation_keeps_cleanup_excluded_while_queued() {
    let service = crate::service::tests::service_tests::service_for_unit_tests().await;
    let sandbox = SandboxId::new().to_string();
    let gate = service.lock_sandbox_cgroups(&sandbox).await;
    let mut request = Request::new(AllocateRequest {
        sandbox_id: sandbox.clone(),
        ..Default::default()
    });
    request
        .extensions_mut()
        .insert(pplx_space_common_svc::PeerIdentity::new(0, 0, None));
    let mut allocation = Box::pin(service.allocate(request));
    assert!(futures_util::poll!(allocation.as_mut()).is_pending());
    drop(allocation);
    let Err(error) = service.claim_sandbox_mutation(&sandbox) else {
        panic!("cleanup overtook a queued allocation");
    };
    assert_eq!(error.code(), tonic::Code::Unavailable);
    drop(gate);
    service.device_op_tasks.close();
    service.device_op_tasks.wait().await;
    // Missing authority rejects this request before host work. Even that
    // failed request must retain its claim until its queued task finishes.
    service.claim_sandbox_mutation(&sandbox).unwrap();
}

#[tokio::test]
async fn execution_authority_survives_slot_release_and_rejects_stale_mutations() {
    let service = crate::service::tests::service_tests::service_for_unit_tests().await;
    let sandbox = SandboxId::new().to_string();
    let first = ExecutionFence::new(SandboxActivationAttemptId::new(), None);
    let replacement = ExecutionFence::new(SandboxActivationAttemptId::new(), Some(first.current()));
    let unrelated = ExecutionFence::new(SandboxActivationAttemptId::new(), None);

    for bound in [None, Some(first), Some(replacement)] {
        if let Some(current) = service.pool.execution_fence(&sandbox).unwrap() {
            service
                .pool
                .retire_execution(&sandbox, current.current())
                .unwrap();
        }
        if let Some(bound) = bound {
            service.pool.bind_execution(&sandbox, bound).unwrap();
        }
        // No network lease exists: ownership of retained cache must still
        // prevent a stale cleanup or process mutation from reaching it.
        assert!(service.pool.get(&sandbox).unwrap().is_none());
        for (request, ordinary, cleanup) in match bound {
            None => vec![(None, false, false), (Some(first), false, true)],
            Some(bound) if bound == first => vec![
                (None, false, false),
                (Some(first), true, true),
                (Some(replacement), false, true),
                (Some(unrelated), false, false),
            ],
            Some(_) => vec![
                (None, false, false),
                (Some(first), false, false),
                (Some(replacement), true, true),
                (Some(unrelated), false, false),
            ],
        } {
            if let Some(request) = request {
                use pplx_space_node_mgr_client::execution_fence::InstanceTarget;
                let result = service
                    .instance_target_root(&sandbox, InstanceTarget::Execution(request))
                    .await;
                assert_eq!(
                    result.is_ok(),
                    cleanup,
                    "canonical instance: bound={bound:?}, request={request:?}"
                );
                if let Ok(path) = result {
                    assert_eq!(path, PathBuf::from(CACHE_DIR).join(&sandbox));
                }
            }
            let wire = request.map(Into::into);
            let filesystem = service
                .check_filesystem_execution(&sandbox, wire.as_ref())
                .await;
            assert_eq!(
                filesystem.is_ok(),
                ordinary || (bound.is_none() && request.is_some()),
                "filesystem: bound={bound:?}, request={request:?}"
            );
            for (allow_predecessor, accepted) in [(false, ordinary), (true, cleanup)] {
                let result = service.check_execution(&sandbox, wire.as_ref(), allow_predecessor);
                assert_eq!(
                    result.is_ok(),
                    accepted,
                    "bound={bound:?}, request={request:?}, cleanup={allow_predecessor}"
                );
                if let Err(error) = result {
                    assert_eq!(error.code(), tonic::Code::FailedPrecondition);
                }
            }
        }
    }
}

#[tokio::test]
async fn stopped_unbound_snapshot_reaches_filesystem_without_allocating() {
    let service = crate::service::tests::service_tests::service_for_unit_tests().await;
    let sandbox = SandboxId::new().to_string();
    let fence = ExecutionFence::new(SandboxActivationAttemptId::new(), None);
    // Legacy stopped sandboxes have retained files but no lease or receiver
    // token. Their upgraded owner supplies a persisted, unbound staging token.
    // This fixture deliberately has no instance: authorization must let the
    // handler reach its filesystem lookup, not require a guest activation.
    let mut request = Request::new(crate::proto::CreateBtrfsSnapshotRequest {
        sandbox_id: sandbox.clone(),
        snapshot_name: "d-manual-20260924T120000Z".into(),
        writable: false,
        execution_fence: Some(fence.into()),
    });
    request
        .extensions_mut()
        .insert(pplx_space_common_svc::PeerIdentity::new(0, 0, None));
    let error = service.create_btrfs_snapshot(request).await.unwrap_err();
    assert_eq!(error.code(), tonic::Code::NotFound, "{error}");
    assert!(error.message().contains("instance dir not found"));
    assert!(service.pool.get(&sandbox).unwrap().is_none());
    assert!(service.pool.execution_fence(&sandbox).unwrap().is_none());
    service.claim_sandbox_mutation(&sandbox).unwrap();

    // Absence of a receiver token must not bypass an unadopted active lease.
    service.pool.set_all_slots_ready_for_test();
    service
        .pool
        .allocate(
            &sandbox,
            None,
            Some(crate::slot_pool::NewLeaseMemoryReservation {
                memory_reservation_mib: 4096,
            }),
        )
        .await
        .unwrap();
    let gate = service.lock_sandbox_cgroups(&sandbox).await;
    let error = service
        .check_filesystem_execution(&sandbox, Some(&fence.into()))
        .await
        .unwrap_err();
    assert_eq!(error.code(), tonic::Code::FailedPrecondition);
    assert!(service.pool.get(&sandbox).unwrap().is_some());
    assert!(service.pool.execution_fence(&sandbox).unwrap().is_none());
    drop(gate);
}

#[tokio::test]
async fn canonical_artifact_rpcs_reject_missing_or_superseded_execution_before_host_work() {
    let service = crate::service::tests::service_tests::service_for_unit_tests().await;
    let sandbox = SandboxId::new().to_string();
    let old = ExecutionFence::new(SandboxActivationAttemptId::new(), None);
    let current = ExecutionFence::new(SandboxActivationAttemptId::new(), Some(old.current()));
    service.pool.bind_execution(&sandbox, current).unwrap();
    fn request<T>(payload: T) -> Request<T> {
        let mut request = Request::new(payload);
        request
            .extensions_mut()
            .insert(pplx_space_common_svc::PeerIdentity::new(0, 0, None));
        request
    }
    for fence in [None, Some(old)] {
        let wire = fence.map(Into::into);
        let target = fence.map(|fence| {
            pplx_space_node_mgr_client::execution_fence::InstanceTarget::Execution(fence).into()
        });
        let results = [
            service
                .create_btrfs_snapshot(request(crate::proto::CreateBtrfsSnapshotRequest {
                    sandbox_id: sandbox.clone(),
                    snapshot_name: "d-1m-20260922T120000Z".into(),
                    writable: false,
                    execution_fence: wire.clone(),
                }))
                .await
                .map(|_| ()),
            service
                .rollback_btrfs_snapshot(request(crate::proto::RollbackBtrfsSnapshotRequest {
                    sandbox_id: sandbox.clone(),
                    snapshot_name: "d-1m-20260922T120000Z".into(),
                    execution_fence: wire.clone(),
                }))
                .await
                .map(|_| ()),
            service
                .restore_instance_snapshot(request(crate::proto::RestoreInstanceSnapshotRequest {
                    sandbox_id: sandbox.clone(),
                    snapshot_name: "d-operation-runtime-upgrade-test".into(),
                    execution_fence: wire.clone(),
                }))
                .await
                .map(|_| ()),
            service
                .btrfs_encode_rootfs_send(request(crate::proto::BtrfsEncodeRootfsSendRequest {
                    sandbox_id: sandbox.clone(),
                    execution_fence: wire,
                    ..Default::default()
                }))
                .await
                .map(|_| ()),
            service
                .btrfs_receive_instance(request(crate::proto::BtrfsReceiveInstanceRequest {
                    sandbox_id: sandbox.clone(),
                    target: target.clone(),
                    template_name: "test".into(),
                    content_hash: format!("sha256:{}", "a".repeat(64)),
                }))
                .await
                .map(|_| ()),
            service
                .btrfs_delete_sandbox_instance(request(
                    crate::proto::BtrfsDeleteSandboxInstanceRequest {
                        sandbox_id: sandbox.clone(),
                        target,
                    },
                ))
                .await
                .map(|_| ()),
        ];
        for result in results {
            let error = result.unwrap_err();
            assert_eq!(error.code(), tonic::Code::FailedPrecondition, "{error}");
            assert!(
                error.message().contains("identity") || error.message().contains("target"),
                "{error}"
            );
        }
        service.claim_sandbox_mutation(&sandbox).unwrap();
        assert_eq!(
            service.pool.execution_fence(&sandbox).unwrap(),
            Some(current)
        );
    }
    use pplx_space_node_mgr_client::execution_fence::InstanceTarget;
    let scope = pplx_space_common_core::types::TransferScopeId::new();
    let root = service
        .instance_target_root(&sandbox, InstanceTarget::RestoreScope(scope))
        .await
        .unwrap();
    assert_ne!(root, PathBuf::from(CACHE_DIR).join(&sandbox));
    assert!(
        root.file_name()
            .unwrap()
            .to_str()
            .unwrap()
            .contains(&scope.to_string())
    );
}
