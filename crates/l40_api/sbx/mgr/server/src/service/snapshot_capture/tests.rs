use super::*;
use pplx_space_common_core::types::{Arch, SandboxId};

#[test]
fn capture_keeps_non_default_producer_binding_and_sandbox_identity() {
    let environments =
        pplx_space_node_runtime_core::catalog::CompiledEnvironment::for_architecture(Arch::X86_64)
            .unwrap();
    for execution in [None].into_iter().chain(
        environments
            .iter()
            .map(|entry| Some(entry.definition().execution_requirements().unwrap())),
    ) {
        let id = SandboxId::new();
        let request = bind(
            CaptureVmStateRequest {
                sandbox_id: id.to_string(),
                artifacts: artifacts(),
                ..Default::default()
            },
            execution.as_ref(),
        );
        assert_eq!(request.sandbox_id, id.to_string());
        let expected = execution
            .as_ref()
            .filter(|execution| super::super::validation::is_non_default_execution(execution))
            .and_then(|execution| execution.environment())
            .map(|reference| reference.to_string());
        assert_eq!(request.environment_reference, expected);
    }
}
