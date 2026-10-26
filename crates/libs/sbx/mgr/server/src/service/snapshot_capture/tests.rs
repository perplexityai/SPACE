use super::*;
use crate::node_manager_transport::fake::FakeNodeManager;
use pplx_space_common_core::types::{Arch, SandboxId};
use pplx_space_node_mgr_client::proto::CreateSnapshotResponse;
use pplx_space_node_mgr_client::runtime_control::RuntimeControl;

// The previous request schema does not know the environment input.
#[derive(Clone, PartialEq, prost::Message)]
struct LegacySnapshotRequest {
    #[prost(string, tag = "1")]
    sandbox_id: String,
}

#[tokio::test]
async fn mixed_version_capture_cannot_reach_old_nodes_with_a_bound_identity() {
    let environments =
        pplx_space_node_runtime_core::catalog::CompiledEnvironment::for_architecture(Arch::X86_64)
            .unwrap();
    for execution in [None].into_iter().chain(
        environments
            .iter()
            .map(|entry| Some(entry.definition().execution_requirements().unwrap())),
    ) {
        for encrypted in [false, true] {
            let old_node = FakeNodeManager::default()
                .unary("CreateSnapshot", |request: LegacySnapshotRequest| {
                    request.sandbox_id.parse::<SandboxId>().map_err(|_| {
                        tonic::Status::invalid_argument("sandbox_id must be a UUID")
                    })?;
                    Ok(CreateSnapshotResponse {})
                })
                .serve();
            let id = SandboxId::new();
            let request = bind(
                CaptureVmStateRequest {
                    sandbox_id: id.to_string(),
                    artifacts: artifacts(),
                    storage: Some(pplx_space_node_mgr_client::proto::VmStateStorage {
                        encrypted_volume_mount: encrypted.then(|| "/synthetic/snapshot".into()),
                        ..Default::default()
                    }),
                    ..Default::default()
                },
                execution.as_ref(),
            );
            let legacy = execution.as_ref().is_none_or(|execution| {
                !super::super::validation::is_non_default_execution(execution)
            });
            assert_eq!(request.environment_reference.is_some(), !legacy);
            let mut node = old_node.connector().connect().await.unwrap();
            assert_eq!(
                node.capture_runtime_state(runtime(execution.as_ref()), request)
                    .await
                    .is_ok(),
                legacy
            );
            assert_eq!(old_node.calls(), ["CaptureVmState", "CreateSnapshot"]);
        }
    }
}
