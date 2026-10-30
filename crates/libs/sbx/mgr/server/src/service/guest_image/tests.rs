use super::*;
use crate::node_manager_transport::fake::FakeNodeManager;
use pplx_space_common_core::types::guest_image::{GuestDiskLayout, GuestNetwork, GuestReadiness};
use pplx_space_common_core::types::{PolicyAuthority, SandboxOwner, SandboxRuntimeLabel};
use pplx_space_node_mgr_client::proto::{ProbeGuestServiceRequest, ProbeGuestServiceResponse};
use std::sync::atomic::{AtomicUsize, Ordering};

fn image() -> GuestImageContract {
    GuestImageContract::new(
        GuestDiskLayout::OpaqueRawDisk,
        GuestReadiness::TcpService {
            port: std::num::NonZeroU16::new(3389).unwrap(),
        },
        GuestNetwork::SlotDhcpV1,
        pplx_space_common_core::types::guest_image::GuestAgent::Absent,
    )
    .unwrap()
}

#[tokio::test]
async fn agentless_activity_and_environment_updates_refuse_without_waking_or_persisting() {
    for state in [SandboxState::Stopped, SandboxState::Started] {
        let conn = crate::db::test_conn();
        let id = SandboxId::new().to_string();
        let image = image();
        crate::db::insert_sandbox(
            &conn,
            crate::db::InsertSandboxParams {
                id: &id,
                uuid8: &SandboxRuntimeLabel::parse("01234567").unwrap(),
                state,
                template: "firmware-test",
                template_hash: None,
                user: &SandboxOwner::parse("test-owner").unwrap(),
                runtime: "ch",
                guest_image: image,
                snapshots_enabled: false,
                rootfs_size_mb: None,
                disk_io_limits: Default::default(),
                pool_template: None,
                volume_encryption: false,
                process_env: &Default::default(),
                auto_lifecycle: &AutoLifecyclePolicy::disabled(),
                network_policy_override: None,
                network_policy_generation: 0,
                network_policy_authority: PolicyAuthority::Local,
                auto_lifecycle_authority: PolicyAuthority::Local,
            },
        )
        .unwrap();
        let node = FakeNodeManager::default().serve();
        let service = SandboxManagerService::new_with_options(
            conn,
            SandboxManagerServiceOptions {
                node_manager: node.connector(),
                ..SandboxManagerServiceOptions::for_test()
            },
        );
        let previous_execution = service.lookup_sandbox(&id).unwrap().node_execution_id;
        for activity in [
            super::super::activation_support::GuestActivityKind::Access,
            super::super::activation_support::GuestActivityKind::Command,
        ] {
            let error = service
                .ensure_active_for_guest_activity_with_start(
                    None,
                    &id,
                    true,
                    Some(Duration::from_secs(1)),
                    activity,
                    super::super::activation_support::WakeStartConfig::default(),
                )
                .await
                .err()
                .expect("agentless activity must refuse");
            assert_eq!(error.code(), tonic::Code::Unimplemented);
        }
        let error = service
            .update(super::super::activation_support::internal_request(
                UpdateRequest {
                    sandbox_id: id.clone(),
                    replace_process_env: true,
                    process_env: std::collections::HashMap::from([("KEY".into(), "value".into())]),
                    ..Default::default()
                },
            ))
            .await
            .unwrap_err();
        assert_eq!(error.code(), tonic::Code::Unimplemented);
        let retained = service.lookup_sandbox(&id).unwrap();
        assert_eq!(retained.state, state);
        assert_eq!(retained.guest_image, image);
        assert!(retained.process_env.is_empty());
        assert_eq!(retained.node_execution_id, previous_execution);
        assert!(node.calls().is_empty());
    }
}

#[tokio::test(start_paused = true)]
async fn service_readiness_retries_false_but_stops_at_execution_refusal_or_deadline() {
    for (eventual_response, expected_code) in [
        (Ok(ProbeGuestServiceResponse { reachable: true }), None),
        (
            Err(Status::resource_exhausted("probe budget busy")),
            Some(tonic::Code::DeadlineExceeded),
        ),
        (
            Err(Status::unavailable("reconnecting")),
            Some(tonic::Code::DeadlineExceeded),
        ),
        (
            Err(Status::aborted("execution replaced")),
            Some(tonic::Code::Aborted),
        ),
        (
            Ok(ProbeGuestServiceResponse { reachable: false }),
            Some(tonic::Code::DeadlineExceeded),
        ),
    ] {
        let calls = Arc::new(AtomicUsize::new(0));
        let id = SandboxId::new().to_string();
        let execution = pplx_space_common_core::types::SandboxActivationAttemptId::new();
        let node = FakeNodeManager::default()
            .unary("ProbeGuestService", {
                let calls = Arc::clone(&calls);
                let id = id.clone();
                move |request: ProbeGuestServiceRequest| {
                    assert_eq!(request.sandbox_id, id);
                    assert_eq!(
                        request.execution_fence.unwrap().execution_id,
                        execution.to_string()
                    );
                    assert_eq!(request.port, 3389);
                    if calls.fetch_add(1, Ordering::SeqCst) == 0 {
                        Ok(ProbeGuestServiceResponse { reachable: false })
                    } else {
                        eventual_response.clone()
                    }
                }
            })
            .serve();
        let result = wait_service(
            &node.connector(),
            &id,
            execution,
            std::num::NonZeroU16::new(3389).unwrap(),
            Duration::from_secs(2),
        )
        .await;
        match expected_code {
            None => result.unwrap(),
            Some(code) => assert_eq!(result.unwrap_err().code(), code),
        }
        assert!(calls.load(Ordering::SeqCst) >= 2);
        assert!(
            node.calls()
                .iter()
                .all(|method| method == "ProbeGuestService")
        );
    }
}
