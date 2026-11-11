use super::*;

#[tokio::test]
async fn release_drains_the_responder_before_returning_and_is_idempotent() {
    let service = crate::service::tests::service_tests::service_for_unit_tests().await;
    let sandbox = SandboxId::new();
    let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
    let (released_tx, released_rx) = tokio::sync::oneshot::channel::<()>();
    let task = tokio::spawn(async move {
        let _resource = released_tx;
        entered_tx.send(()).unwrap();
        std::future::pending::<()>().await;
    });
    entered_rx.await.unwrap();
    service.guest_networks.entries.insert(
        sandbox,
        DhcpTask {
            execution: pplx_space_common_core::types::SandboxActivationAttemptId::new(),
            task,
        },
    );
    service.stop_guest_dhcp(&sandbox.to_string()).await.unwrap();
    released_rx.await.unwrap_err();
    assert!(service.guest_networks.entries.is_empty());
    service.stop_guest_dhcp(&sandbox.to_string()).await.unwrap();
}
