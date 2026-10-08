use super::*;

#[tokio::test]
async fn unconfigured_activation_preserves_startup_and_refuses_only_delegated_wake() {
    if !pplx_space_test_env::enter_test_process(&[("SPACE_INGRESS_ACTIVATION_ENDPOINT", None)]) {
        return;
    }
    let client = RemoteIngressActivation::from_env()
        .await
        .expect("route resolver must start without a new listener configuration");
    let error = client
        .prepare(SandboxId::new(), Duration::from_secs(1))
        .await
        .unwrap_err();
    assert_eq!(error.code(), tonic::Code::Unavailable);
    assert_eq!(
        error.message(),
        "ingress activation endpoint is not configured"
    );
}
