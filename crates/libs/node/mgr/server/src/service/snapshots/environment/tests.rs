use super::*;
use crate::proto::{CreateSnapshotRequest, EnvironmentSnapshotInput};

#[test]
fn selected_snapshot_identity_rejects_conflicts_and_malformed_values() {
    let id = SandboxId::new();
    let input = EnvironmentSnapshotInput {
        sandbox_id: id.to_string(),
        reference: format!("sha256:{}", "01".repeat(32)),
    };
    for (legacy, selected, accepted) in [
        (String::new(), Some(input.clone()), true),
        (id.to_string(), Some(input.clone()), false),
        (
            String::new(),
            Some(EnvironmentSnapshotInput {
                sandbox_id: "invalid".into(),
                ..input.clone()
            }),
            false,
        ),
        (
            String::new(),
            Some(EnvironmentSnapshotInput {
                reference: "invalid".into(),
                ..input.clone()
            }),
            false,
        ),
        (id.to_string(), None, true),
    ] {
        let mut request = CreateSnapshotRequest {
            sandbox_id: legacy,
            environment: selected,
            ..Default::default()
        };
        let result = normalize(&mut request);
        assert_eq!(result.is_ok(), accepted);
        if accepted {
            assert_eq!(request.sandbox_id, id.to_string());
        }
    }
}

#[tokio::test]
async fn unapproved_snapshot_environment_is_refused_before_host_io() {
    let mut service = crate::service::tests::service_tests::service_for_unit_tests().await;
    service.production = true;
    let reference = "sha256:959f1181ac69f0dde7125fbc81be6fcd75b64e9e06e7a393f067b96eb6fb2ac4"
        .parse()
        .unwrap();
    let error = service
        .verify_snapshot_environment(SandboxId::new().to_string(), Some(reference))
        .await
        .unwrap_err();
    assert_eq!(error.code(), tonic::Code::FailedPrecondition);
    assert!(error.message().contains("production execution"));
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
