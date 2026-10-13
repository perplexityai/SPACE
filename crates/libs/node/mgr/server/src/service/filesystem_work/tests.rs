use super::*;
use std::sync::atomic::{AtomicBool, Ordering};

#[tokio::test]
async fn artifact_mutation_cancellation_retains_target_until_blocking_work_exits() {
    for cancelled in [false, true] {
        let service = super::super::tests::service_tests::service_for_unit_tests().await;
        // A fresh id keeps a regressed artifact delete away from any real path.
        let sandbox_id = super::super::SandboxId::new().to_string();
        let id = sandbox_id.as_str();
        let published = Arc::new(AtomicBool::new(false));
        let (started, running) = tokio::sync::oneshot::channel();
        let (finish, finished) = std::sync::mpsc::channel();
        let caller = tokio::spawn({
            let service = service.clone();
            let published = published.clone();
            let id = sandbox_id.clone();
            async move {
                service
                    .run_artifact_mutation(&id, |worker| async move {
                        worker
                            .off_runtime("transfer fixture", move || {
                                started.send(()).unwrap();
                                finished
                                    .recv_timeout(std::time::Duration::from_secs(10))
                                    .unwrap();
                                Ok(())
                            })
                            .await?;
                        worker
                            .off_runtime("publish fixture", move || {
                                published.store(true, Ordering::SeqCst);
                                Ok(())
                            })
                            .await
                    })
                    .await
            }
        });
        running.await.unwrap();
        if cancelled {
            caller.abort();
        }
        // Cancellation cannot free the target, reset exclusion, or capacity
        // while an uninterruptible blocking section is still running.
        let target =
            NodeManagerService::sandbox_cgroup_family_lock(&service.sandbox_artifact_locks, id)
                .await;
        target.try_lock().unwrap_err();
        assert!(service.claim_sandbox_mutation(id).is_err());
        // A retry for this target must reject immediately, not consume the
        // remaining global capacity while waiting behind the incumbent.
        for _ in 0..4 {
            use futures_util::FutureExt;
            let retry = service.run_artifact_mutation(id, |_| async { Ok(()) });
            let rejection = retry.now_or_never().expect("busy target must not park");
            assert_eq!(rejection.unwrap_err().code(), tonic::Code::Unavailable);
        }
        service
            .run_artifact_mutation("018f03b0-1832-7000-8000-000000000002", |_| async { Ok(()) })
            .await
            .unwrap();
        let waiter = tokio::spawn({
            let service = service.clone();
            let id = sandbox_id.clone();
            async move { service.lock_sandbox_artifacts(&id).await.unwrap() }
        });
        if cancelled {
            assert!(caller.await.unwrap_err().is_cancelled());
            finish.send(()).unwrap();
        } else {
            finish.send(()).unwrap();
            caller.await.unwrap().unwrap();
        }
        drop(
            tokio::time::timeout(std::time::Duration::from_secs(10), waiter)
                .await
                .unwrap()
                .unwrap(),
        );
        assert_eq!(published.load(Ordering::SeqCst), !cancelled);
        drop(service.claim_sandbox_mutation(id).unwrap());
        // A surviving snapshot capture uses shared host-work ownership rather
        // than the transfer lock. Artifact deletion must exclude it as well.
        let capture = service.claim_sandbox_work(id).unwrap();
        fn root_request<T>(message: T) -> tonic::Request<T> {
            let mut request = tonic::Request::new(message);
            request
                .extensions_mut()
                .insert(pplx_space_common_svc::PeerIdentity::new(0, 0, None));
            request
        }
        let request = root_request(crate::proto::DeleteCacheArtifactRequest {
            local_path: format!(
                "{}/{id}/snapshots/.suspend-gc-pending-{id}",
                super::super::CACHE_DIR
            ),
        });
        let busy = |result: Result<(), tonic::Status>| {
            let error = result.unwrap_err();
            assert_eq!(error.code(), tonic::Code::Unavailable);
            assert_eq!(
                error.message(),
                "sandbox host mutation or reset is in progress; retry"
            );
        };
        busy(service.delete_cache_artifact(request).await.map(|_| ()));
        let snapshot_name = "d-1m-20260922T120000Z";
        let results = [
            service
                .create_btrfs_snapshot(root_request(crate::proto::CreateBtrfsSnapshotRequest {
                    sandbox_id: id.into(),
                    snapshot_name: snapshot_name.into(),
                    writable: false,
                    execution_fence: None,
                }))
                .await
                .map(|_| ()),
            service
                .rollback_btrfs_snapshot(root_request(crate::proto::RollbackBtrfsSnapshotRequest {
                    sandbox_id: id.into(),
                    snapshot_name: snapshot_name.into(),
                    execution_fence: None,
                }))
                .await
                .map(|_| ()),
            service
                .delete_btrfs_snapshot(root_request(crate::proto::DeleteBtrfsSnapshotRequest {
                    sandbox_id: id.into(),
                    snapshot_name: snapshot_name.into(),
                }))
                .await
                .map(|_| ()),
            service
                .btrfs_delete_sandbox_instance(root_request(
                    crate::proto::BtrfsDeleteSandboxInstanceRequest {
                        sandbox_id: id.into(),
                        target: Some(pplx_space_node_mgr_client::execution_fence::InstanceTarget::Execution(
                            pplx_space_node_mgr_client::execution_fence::ExecutionFence::new(id.parse().unwrap(), None),
                        ).into()),
                    },
                ))
                .await
                .map(|_| ()),
            service
                .btrfs_delete_sandbox_instance(root_request(
                    crate::proto::BtrfsDeleteSandboxInstanceRequest {
                        sandbox_id: id.into(),
                        target: Some(pplx_space_node_mgr_client::execution_fence::InstanceTarget::RestoreScope(id.parse().unwrap()).into()),
                    },
                ))
                .await
                .map(|_| ()),
        ];
        results.into_iter().for_each(busy);
        drop(capture);
        drop(service.claim_sandbox_mutation(id).unwrap());
        drop(target.try_lock().unwrap());
    }
}
