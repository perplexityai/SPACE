//! The drain promotion's backup enqueue (`dispatch_backup`, through
//! `enqueue_artifact_backup_with_node_api`) answers a node's refusal of a
//! sandbox not yet stopped or suspended as `Conflict`, whether the node says
//! `FailedPrecondition` or `Aborted`.

use pplx_space_bearer_token::{SPACE_INTERNAL_CONTROL_TOKEN_ENV, SPACE_INTERNAL_TOKEN_ENV};
use pplx_space_node_api_sim::SimWorkerHarness;
use pplx_space_test_env::TestResult;

use super::*;

#[tokio::test]
async fn a_not_yet_stopped_backup_refusal_is_a_conflict_for_the_drain() -> TestResult {
    if !pplx_space_test_env::enter_test_process(&[
        (
            "SPACE_INTERNAL_RPC_INSECURE_DEV",
            Some(std::ffi::OsStr::new("true")),
        ),
        (
            SPACE_INTERNAL_TOKEN_ENV,
            Some(std::ffi::OsStr::new("drain-test-token")),
        ),
        (
            SPACE_INTERNAL_CONTROL_TOKEN_ENV,
            Some(std::ffi::OsStr::new("drain-test-control-token")),
        ),
    ]) {
        return Ok(());
    }
    let server = SimWorkerHarness::builder("drain-backup-node")
        .start()
        .await?;
    let endpoint =
        tonic::transport::Endpoint::from_shared(format!("http://{}", server.endpoint()))?;
    let mut client = NodeApiClient::with_service_name("worker-node-api", endpoint);
    for code in [tonic::Code::FailedPrecondition, tonic::Code::Aborted] {
        let sandbox_id = SandboxId::new();
        server
            .state()
            .refuse_next_artifact_backup(sandbox_id.to_string(), code)
            .await;

        let dispatched = dispatch_backup(&mut client, server.endpoint(), sandbox_id, None).await;

        assert!(
            matches!(&dispatched, Err(SubstrateError::Conflict { message }) if message.contains("isn't stopped")),
            "{code:?}: {dispatched:?}"
        );
    }
    Ok(())
}
