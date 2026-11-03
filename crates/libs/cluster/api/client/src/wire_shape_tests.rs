use super::proto::*;

#[test]
fn get_node_sandbox_desired_state_response_wire_shape() {
    insta::assert_debug_snapshot!(GetNodeSandboxDesiredStateResponse {
        sandboxes: vec![SandboxDesiredState {
            sandbox_id: "019e6370-cfc9-7a43-9337-399c7b51a1d1".to_string(),
            snapshots_enabled_desired: true,
            port_exposures: vec![SandboxPortExposureDesiredState {
                protocol: SandboxPortProtocol::Tcp as i32,
                guest_port: 9012,
                desired_state: SandboxPortDesiredStateKind::SandboxPortDesiredStateOpen as i32,
            }],
            auto_lifecycle: None,
        }],
        deleted_sandbox_ids: vec![
            "019e6370-cfc9-7a43-9337-399c7b51a1d2".to_string(),
            "019e6370-cfc9-7a43-9337-399c7b51a1d3".to_string(),
        ],
    });
}

#[test]
fn unstable_resolve_sandbox_service_route_request_wire_shape() {
    insta::assert_debug_snapshot!(UnstableResolveSandboxServiceRouteRequest {
        sandbox_id: "019e6370-cfc9-7a43-9337-399c7b51a1d1".to_string(),
        port: 8080,
        route_kind: SandboxServiceRouteKind::Agent as i32,
        credential_proofs: vec![],
        transport: SandboxServiceRouteTransport::Direct as i32,
    });
}

#[test]
fn get_quarantine_recovery_plan_response_wire_shape() {
    insta::assert_debug_snapshot!(GetQuarantineRecoveryPlanResponse {
        recovery_id: "rec-001".to_string(),
        execution_generation: 7,
        reason_code: "sandbox_unreachable".to_string(),
        target_state: QuarantineRecoveryTargetState::Suspended as i32,
        suspend_snapshot: Some(QuarantineRecoverySuspendSnapshot {
            snapshot_id: "snap-001".to_string(),
            local_artifact_id: "art-abc".to_string(),
            storage_uri: "s3://bucket/key?region=us-east-1".to_string(),
            bytes_uploaded: 1024,
        }),
    });
}
