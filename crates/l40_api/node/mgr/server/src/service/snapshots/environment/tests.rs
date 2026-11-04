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

#[tokio::test]
async fn captures_obey_the_startup_settings_not_the_cluster_policy() {
    use pplx_space_common_core::types::Arch;
    use pplx_space_node_runtime_core::{catalog::CompiledEnvironment, selection::*};
    let catalog = CompiledEnvironment::for_architecture(Arch::X86_64).unwrap();
    let reference = |name: &str| {
        catalog
            .iter()
            .find(|entry| entry.name() == name)
            .unwrap()
            .definition()
            .reference()
    };
    let (e0, e1) = (reference("e0"), reference("fc-1.17.0-linux-6.1.155"));
    let mut service = crate::service::tests::service_tests::service_for_unit_tests().await;
    service.set_startup_execution_policy_for_test(
        ExecutionPolicy::parse(Some("e0,fc-1.17.0-linux-6.1.155,legacy"), None, &catalog).unwrap(),
    );
    // The cluster blocks E1. Capturing an E1 VM gets past the policy and
    // stops only at the missing metadata of this synthetic sandbox.
    service.narrow_execution_for_test(&ClusterExecutionPolicy {
        generation: 1,
        default: Some(ClusterDefault::Environment(e0)),
        executing: [e0].into(),
        legacy_unbound_allowed: Some(false),
    });
    let sandbox = SandboxId::new().to_string();
    let error = service
        .verify_snapshot_environment(sandbox.clone(), Some(e1))
        .await
        .unwrap_err();
    assert!(
        error
            .message()
            .contains("requires sandbox execution metadata"),
        "{error}"
    );
    // An unreadable record doesn't stop captures either.
    service.refuse_all_execution_for_test();
    service
        .verify_snapshot_environment(sandbox.clone(), None)
        .await
        .unwrap();
    let error = service
        .verify_snapshot_environment(sandbox, Some(e1))
        .await
        .unwrap_err();
    assert!(
        error
            .message()
            .contains("requires sandbox execution metadata"),
        "{error}"
    );
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
