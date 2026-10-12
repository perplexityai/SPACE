//! Spaced bridge and readiness calls that pass their state and caller gates
//! ask node-manager for the sandbox's endpoint proof before any spaced-host
//! connection.
//!
//! Either an in-process node-manager refuses the capture, or node-manager is
//! down and the call fails retryably. Neither reaches the guest's fixed
//! spaced-host socket.

use super::node_manager_transport_tests::service_with_node_manager;
use super::service_tests::{
    complete_runtime_endpoint_identity, insert_test_sandbox_with_runtime,
    node_api_trusted_lifecycle_request, node_ctl_request,
};
use super::*;
use crate::node_manager_transport::fake::FakeNodeManager;
use pplx_space_node_mgr_client::proto::{CaptureSandboxEndpointProofRequest, SandboxEndpointProof};

#[derive(Clone, Copy, Debug)]
enum Call {
    OpenLoopbackBridge,
    CloseLoopbackBridge,
    WaitReady,
}

impl Call {
    async fn send(self, service: &SandboxManagerService, sandbox_id: &str) -> Status {
        let sandbox_id = sandbox_id.to_owned();
        match self {
            Self::OpenLoopbackBridge => service
                .spaced_open_loopback_bridge(node_ctl_request(SpacedOpenLoopbackBridgeRequest {
                    sandbox_id,
                    operation: Some(
                        pplx_space_sbx_mgr_client::operations::OpenLoopbackBridgeRequest {
                            port: 9012,
                            wait_timeout: None,
                        },
                    ),
                }))
                .await
                .map(|_| ()),
            Self::CloseLoopbackBridge => service
                .spaced_close_loopback_bridge(node_ctl_request(SpacedCloseLoopbackBridgeRequest {
                    sandbox_id,
                    operation: Some(
                        pplx_space_sbx_mgr_client::operations::CloseLoopbackBridgeRequest {
                            port: 9012,
                        },
                    ),
                }))
                .await
                .map(|_| ()),
            Self::WaitReady => service
                .spaced_wait_ready(node_api_trusted_lifecycle_request(SpacedWaitReadyRequest {
                    sandbox_id,
                    timeout: None,
                }))
                .await
                .map(|_| ()),
        }
        .expect_err("node-manager refuses the endpoint proof")
    }
}

#[tokio::test]
async fn gated_spaced_calls_capture_the_endpoint_proof_from_node_manager() {
    if !crate::service::tests::enter_route_test_process(&[
        ("EPPO_SDK_KEY", None),
        ("SPACE_FLAGD_ENABLED", None),
    ]) {
        return;
    }
    // Exposure re-apply during a wake in flight: after a suspend, the
    // cluster marks open port exposures pending and the node-ctl reconciler
    // re-arms the in-guest loopback bridge once the sandbox is back. The
    // guest is reachable as soon as the VM resumes — long before the wake's
    // readiness contract and network-policy gates finish — so the bridge
    // RPCs must not gate on `Started`. Gating on `Started` left exposed
    // ports answering 502 for the full duration of a stalled wake even
    // though the guest was already serving spaced RPCs. node-ctl needs no
    // route token, and a trusted node-api lifecycle caller needs none to
    // wait for readiness.
    for (call, state) in [
        (Call::OpenLoopbackBridge, SandboxState::Restoring),
        (Call::CloseLoopbackBridge, SandboxState::Restoring),
        (Call::WaitReady, SandboxState::Started),
    ] {
        for node_manager_up in [true, false] {
            let context =
                format!("{call:?} on a {state} sandbox, node-manager up: {node_manager_up}");
            let conn = crate::db::test_conn();
            let sandbox_id = SandboxId::new().to_string();
            insert_test_sandbox_with_runtime(&conn, &sandbox_id, state);
            complete_runtime_endpoint_identity(&conn, &sandbox_id);
            let captures = Arc::new(Mutex::new(Vec::new()));
            let node_manager = FakeNodeManager::default()
                .unary("CaptureSandboxEndpointProof", {
                    let captures = Arc::clone(&captures);
                    move |request: CaptureSandboxEndpointProofRequest| {
                        captures.lock().unwrap().push(request);
                        Err::<SandboxEndpointProof, _>(Status::failed_precondition(
                            "scripted endpoint proof refusal",
                        ))
                    }
                })
                .serve();
            let service = if node_manager_up {
                service_with_node_manager(conn, node_manager.connector())
            } else {
                // The test default: node-manager's socket is unbound.
                SandboxManagerService::for_test(conn)
            };

            let error = call.send(&service, &sandbox_id).await;

            if node_manager_up {
                // node-manager's refusal of the local binding.
                assert_eq!(
                    error.code(),
                    tonic::Code::PermissionDenied,
                    "{context}: {error}"
                );
                assert_eq!(
                    error.message(),
                    "spaced endpoint attestation failed",
                    "{context}"
                );
                assert_eq!(
                    *captures.lock().unwrap(),
                    vec![CaptureSandboxEndpointProofRequest {
                        sandbox_id: sandbox_id.clone()
                    }],
                    "{context}"
                );
                assert_eq!(
                    node_manager.calls(),
                    vec!["CaptureSandboxEndpointProof".to_owned()],
                    "{context}"
                );
            } else {
                // A down node-manager is retryable.
                assert_eq!(error.code(), tonic::Code::Unavailable, "{context}: {error}");
                assert_eq!(
                    error.message(),
                    "spaced endpoint proof is temporarily unavailable",
                    "{context}"
                );
            }
        }
    }
}
