use super::*;
#[tokio::test]
async fn unapproved_snapshot_environment_is_refused_before_host_io() {
    let service = crate::service::tests::service_tests::service_for_unit_tests().await;
    let reference = "sha256:959f1181ac69f0dde7125fbc81be6fcd75b64e9e06e7a393f067b96eb6fb2ac4"
        .parse()
        .unwrap();
    let error = service
        .verify_snapshot_environment(SandboxId::new().to_string(), Some(reference))
        .await
        .unwrap_err();
    assert_eq!(error.code(), tonic::Code::FailedPrecondition);
    assert!(error.message().contains("not allowed"));
}

#[test]
fn legacy_capture_cannot_bypass_non_default_environment_binding() {
    use pplx_space_common_core::types::{Arch, Digest};
    let environments =
        pplx_space_node_runtime_core::catalog::CompiledEnvironment::for_architecture(Arch::X86_64)
            .unwrap();
    let initial = environments[0].definition().reference();
    for environment in &environments {
        let actual = environment.definition().reference();
        for requested in [None]
            .into_iter()
            .chain(
                environments
                    .iter()
                    .map(|entry| Some(entry.definition().reference())),
            )
            .chain([Some(EnvironmentReference::new(Digest::sha256([7; 32])))])
        {
            let allowed = requested == Some(actual) || (requested.is_none() && actual == initial);
            assert_eq!(
                validate_binding(requested, actual, initial).is_ok(),
                allowed,
                "{actual} {requested:?}"
            );
        }
    }
}
