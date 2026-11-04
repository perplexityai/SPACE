use super::*;
use crate::node_manager_transport::fake::FakeNodeManager;
use crate::service::SandboxManagerServiceOptions;
use pplx_space_common_core::types::Arch;
use pplx_space_node_mgr_client::proto::ResolveExecutionEnvironmentResponse;
use pplx_space_node_runtime_core::catalog::CompiledEnvironment;

#[tokio::test]
async fn only_missing_acknowledgement_retains_the_exact_source_during_rollout() {
    if !crate::service::tests::enter_route_test_process(&[
        ("EPPO_SDK_KEY", None),
        ("SPACE_FLAGD_ENABLED", None),
    ]) {
        return;
    }
    let catalog = CompiledEnvironment::for_architecture(Arch::X86_64).unwrap();
    let source = catalog[0].definition().execution_requirements().unwrap();
    let candidate = catalog
        .iter()
        .find(|entry| {
            entry.release().release.version.as_str() == "1.17.0"
                && entry
                    .kernel()
                    .is_some_and(|kernel| kernel.version.as_str() == "6.1.155")
        })
        .unwrap();
    for (intent, acknowledgement, exact_allowed, expected) in [
        (
            ExecutionEnvironmentSelection::ColdBoot,
            0,
            true,
            Some("1.15.1"),
        ),
        (ExecutionEnvironmentSelection::ColdBoot, 0, false, None),
        (
            ExecutionEnvironmentSelection::Restore,
            0,
            false,
            Some("1.15.1"),
        ),
        (
            ExecutionEnvironmentSelection::ColdBoot,
            ExecutionEnvironmentSelection::ColdBoot as i32,
            true,
            Some("1.17.0"),
        ),
        (ExecutionEnvironmentSelection::ColdBoot, 99, true, None),
    ] {
        let response = ResolveExecutionEnvironmentResponse {
            accepted_selection: acknowledgement,
            definition: Some(candidate.definition().into()),
            execution: Some((&candidate.definition().execution_requirements().unwrap()).into()),
        };
        let exact_definition = catalog[0].definition().clone();
        let exact_source = source.clone();
        let node = FakeNodeManager::default()
            .unary(
                "ResolveExecutionEnvironment",
                move |request: ResolveExecutionEnvironmentRequest| {
                    if let Some(compatible) = request.compatible {
                        assert!(request.reference.is_none());
                        assert!(matches!(
                            (intent, compatible),
                            (
                                ExecutionEnvironmentSelection::ColdBoot,
                                Compatible::ColdBootSource(_)
                            ) | (
                                ExecutionEnvironmentSelection::Restore,
                                Compatible::RestoreSource(_)
                            )
                        ));
                        return Ok(response.clone());
                    }
                    assert_eq!(intent, ExecutionEnvironmentSelection::ColdBoot);
                    assert_eq!(
                        request.reference,
                        Some(exact_definition.reference().to_string())
                    );
                    if !exact_allowed {
                        return Err(Status::failed_precondition("exact source unavailable"));
                    }
                    Ok(ResolveExecutionEnvironmentResponse {
                        accepted_selection: 0,
                        definition: Some((&exact_definition).into()),
                        execution: Some((&exact_source).into()),
                    })
                },
            )
            .serve();
        let service = SandboxManagerService::new_with_options(
            crate::db::test_conn(),
            SandboxManagerServiceOptions {
                node_manager: node.connector(),
                ..SandboxManagerServiceOptions::for_test()
            },
        );
        let result = service.select_compatible_execution(&source, intent).await;
        if let Some(expected) = expected {
            assert_eq!(result.unwrap().runtime().version.as_str(), expected);
        } else {
            result.unwrap_err();
        }
        service.shutdown_token.cancel();
        service.task_tracker.close();
        tokio::time::timeout(
            std::time::Duration::from_secs(30),
            service.task_tracker.wait(),
        )
        .await
        .unwrap();
    }
}
