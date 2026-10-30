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
    // Unconfigured is unknown, never "nothing can wake it".
    let error = client
        .queue_implicit_wake(
            SandboxId::new(),
            ImplicitWakeActivity::Access,
            Duration::from_secs(1),
        )
        .await
        .unwrap_err();
    assert_eq!(error.code(), tonic::Code::Unavailable);
}

struct PrepareOnly;
#[tonic::async_trait]
impl IngressActivation for PrepareOnly {
    async fn prepare(&self, _: SandboxId, _: Duration) -> Result<(), Status> {
        Ok(())
    }
}

#[tokio::test]
async fn an_owner_that_cannot_queue_a_wake_answers_unknown() {
    let error = PrepareOnly
        .queue_implicit_wake(
            SandboxId::new(),
            ImplicitWakeActivity::Access,
            Duration::from_secs(1),
        )
        .await
        .unwrap_err();
    assert_eq!(error.code(), tonic::Code::Unimplemented);
}
