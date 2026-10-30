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

#[tokio::test(start_paused = true)]
async fn shutdown_inspection_requires_matching_execution_and_positive_process_evidence() {
    use pplx_space_node_mgr_client::execution_fence::ExecutionFence;
    use pplx_space_node_mgr_client::proto::{InspectSandboxRequest, InspectSandboxResponse};
    let id = SandboxId::new().to_string();
    let fence = ExecutionFence::new(
        pplx_space_common_core::types::SandboxActivationAttemptId::new(),
        None,
    );
    for case in 0..6 {
        let mut response = InspectSandboxResponse {
            sandbox_id: id.clone(),
            execution_fence: Some(fence.into()),
            runtime_pid: Some(1234),
            runtime_alive: false,
            ..Default::default()
        };
        match case {
            0 => {}
            1 => response.runtime_alive = true,
            2 => response.runtime_pid = None,
            3 => response.execution_fence = None,
            4 => {
                response.execution_fence = Some(
                    ExecutionFence::new(
                        pplx_space_common_core::types::SandboxActivationAttemptId::new(),
                        None,
                    )
                    .into(),
                )
            }
            5 => response.sandbox_id = SandboxId::new().to_string(),
            _ => unreachable!(),
        }
        let node = FakeNodeManager::default()
            .unary("InspectSandbox", move |_: InspectSandboxRequest| {
                Ok(response.clone())
            })
            .serve();
        let observed = inspect_shutdown_process(
            &node.connector(),
            &id,
            fence,
            tokio::time::Instant::now() + Duration::from_secs(5),
        )
        .await;
        match case {
            0 => assert_eq!(observed.unwrap(), Some((1234, false))),
            1 => assert_eq!(observed.unwrap(), Some((1234, true))),
            2 | 3 => assert_eq!(observed.unwrap(), None),
            4 | 5 => assert_eq!(observed.unwrap_err().code(), tonic::Code::Aborted),
            _ => unreachable!(),
        }
    }
    let node = FakeNodeManager::default()
        .unary(
            "InspectSandbox",
            |_: InspectSandboxRequest| -> Result<InspectSandboxResponse, Status> {
                Err(Status::unavailable("socket unavailable"))
            },
        )
        .serve();
    assert_eq!(
        inspect_shutdown_process(
            &node.connector(),
            &id,
            fence,
            tokio::time::Instant::now() + Duration::from_secs(5)
        )
        .await
        .unwrap_err()
        .code(),
        tonic::Code::Unavailable
    );
}

#[tokio::test(start_paused = true)]
async fn shutdown_wait_finishes_on_exit_but_not_on_missing_process_evidence() {
    use pplx_space_node_mgr_client::{
        execution_fence::ExecutionFence,
        proto::{
            GetVmStateRequest, GetVmStateResponse, InspectSandboxRequest, InspectSandboxResponse,
        },
    };
    let id = SandboxId::new().to_string();
    let fence = ExecutionFence::new(
        pplx_space_common_core::types::SandboxActivationAttemptId::new(),
        None,
    );
    for (pid, forced, expected) in [
        (Some(1234), false, "guest_exit_observed"),
        (None, false, "forced_after_deadline"),
        (None, true, "force_requested"),
        (Some(5678), false, "aborted"),
    ] {
        let response = InspectSandboxResponse {
            sandbox_id: id.clone(),
            execution_fence: Some(fence.into()),
            runtime_pid: pid,
            runtime_alive: false,
            ..Default::default()
        };
        let node = FakeNodeManager::default()
            .unary("InspectSandbox", move |_: InspectSandboxRequest| {
                Ok(response.clone())
            })
            .unary(
                "GetVmState",
                |_: GetVmStateRequest| -> Result<GetVmStateResponse, Status> {
                    Err(Status::unavailable("VMM API socket gone"))
                },
            )
            .serve();
        let start = tokio::time::Instant::now();
        let deadline = start + Duration::from_secs(2);
        let result =
            wait_firmware_shutdown(&id, &node.connector(), fence, Some(1234), deadline, || {
                Ok(forced)
            })
            .await;
        if expected == "aborted" {
            assert_eq!(result.unwrap_err().code(), tonic::Code::Aborted);
        } else {
            assert_eq!(result.unwrap().as_str(), expected);
        }
        if expected == "forced_after_deadline" {
            assert!(tokio::time::Instant::now() >= deadline);
        } else {
            assert!(tokio::time::Instant::now() < deadline);
        }
    }
}
