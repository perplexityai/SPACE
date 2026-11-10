use super::*;

#[tokio::test]
async fn readiness_cannot_publish_while_recovery_owns_termination() {
    let root = tempfile::tempdir().unwrap();
    std::fs::create_dir(root.path().join("slots")).unwrap();
    let recovery = SlotStore::open(root.path()).unwrap();
    let mut update = UpdateRuntime::new();
    update.root = root.path().into();
    update.lock_budget = Duration::ZERO;
    update.enabled.store(true, Ordering::Release);
    let shutdown = CancellationToken::new();
    let service = SpacedGuestService {
        inner: Arc::new(ServiceInner {
            update,
            execution: execution::Execution {
                admission: execution::Admission::new(shutdown.clone()),
                filesystem: None,
                processes: None,
            },
            service_generation: Uuid::now_v7().to_string(),
            shutdown,
            loopback_bridge: Forwarding,
            ready: watch::channel(false).0,
            // No SCM dependency: test the production publication path's lock.
            scm_status: None,
        }),
    };
    assert_eq!(
        service
            .publish_platform_readiness()
            .await
            .unwrap_err()
            .code(),
        tonic::Code::Unavailable
    );
    assert!(!*service.inner.ready.borrow());
    drop(recovery);
    service.publish_platform_readiness().await.unwrap();
    // Publication does not itself open the trial gate; Commit owns that edge.
    assert!(!*service.inner.ready.borrow());
}
