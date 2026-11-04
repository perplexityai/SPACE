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

fn insert_firmware_sandbox(
    conn: &crate::db::Connection,
    id: &str,
    state: SandboxState,
    image: GuestImageContract,
) {
    crate::db::insert_sandbox(
        conn,
        crate::db::InsertSandboxParams {
            id,
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
}

#[tokio::test]
async fn agentless_activity_and_environment_updates_refuse_without_waking_or_persisting() {
    for state in [SandboxState::Stopped, SandboxState::Started] {
        let conn = crate::db::test_conn();
        let id = SandboxId::new().to_string();
        let image = image();
        insert_firmware_sandbox(&conn, &id, state, image);
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

#[tokio::test]
async fn agentless_memory_lifecycle_refuses_before_changing_state_or_contacting_workers() {
    for (action, state) in [
        ("pause", SandboxState::Started),
        ("resume", SandboxState::Paused),
        ("suspend", SandboxState::Started),
        ("suspend", SandboxState::Paused),
    ] {
        let conn = crate::db::test_conn();
        let id = SandboxId::new().to_string();
        insert_firmware_sandbox(&conn, &id, state, image());
        let catalog = pplx_space_execution_catalog::catalog::CompiledEnvironment::for_architecture(
            pplx_space_common_core::types::Arch::X86_64,
        )
        .unwrap();
        let execution = catalog
            .iter()
            .find(|entry| entry.name() == "ch-54.0-windows-uefi")
            .unwrap()
            .definition()
            .execution_requirements()
            .unwrap();
        crate::db::upsert_sandbox_execution_metadata(&conn, &id, &execution).unwrap();
        let node = FakeNodeManager::default().serve();
        let service = SandboxManagerService::new_with_options(
            conn,
            SandboxManagerServiceOptions {
                node_manager: node.connector(),
                ..SandboxManagerServiceOptions::for_test()
            },
        );
        let error = if action == "pause" {
            service
                .pause(super::super::activation_support::internal_request(
                    PauseRequest {
                        sandbox_id: id.clone(),
                    },
                ))
                .await
                .unwrap_err()
        } else if action == "resume" {
            service
                .resume(super::super::activation_support::internal_request(
                    ResumeRequest {
                        sandbox_id: id.clone(),
                        ..Default::default()
                    },
                ))
                .await
                .unwrap_err()
        } else {
            service
                .suspend(super::super::activation_support::internal_request(
                    SuspendRequest {
                        sandbox_id: id.clone(),
                        ..Default::default()
                    },
                ))
                .await
                .unwrap_err()
        };
        assert_eq!(error.code(), tonic::Code::FailedPrecondition);
        assert!(pplx_space_common_grpc::GuestCapabilityUnsupported::from_status(&error).is_some());
        assert_eq!(service.lookup_sandbox(&id).unwrap().state, state);
        assert!(node.calls().is_empty());
    }
}

#[tokio::test]
async fn windows_environment_update_refuses_before_persistence_but_allows_clearing() {
    use pplx_space_common_core::types::guest_image::GuestAgent;
    let conn = crate::db::test_conn();
    let id = SandboxId::new().to_string();
    let image = GuestImageContract::new(
        image().disk(),
        GuestReadiness::Agent,
        image().network(),
        GuestAgent::Spaced,
    )
    .unwrap();
    insert_firmware_sandbox(&conn, &id, SandboxState::Started, image);
    let catalog = pplx_space_execution_catalog::catalog::CompiledEnvironment::for_architecture(
        pplx_space_common_core::types::Arch::X86_64,
    )
    .unwrap();
    let execution = catalog
        .iter()
        .find(|entry| entry.name() == "ch-54.0-windows-uefi")
        .unwrap()
        .definition()
        .execution_requirements()
        .unwrap();
    crate::db::upsert_sandbox_execution_metadata(&conn, &id, &execution).unwrap();
    let node = FakeNodeManager::default().serve();
    let service = SandboxManagerService::new_with_options(
        conn,
        SandboxManagerServiceOptions {
            node_manager: node.connector(),
            ..SandboxManagerServiceOptions::for_test()
        },
    );
    let request = |env| {
        super::super::activation_support::internal_request(UpdateRequest {
            sandbox_id: id.clone(),
            replace_process_env: true,
            process_env: env,
            ..Default::default()
        })
    };
    let error = service
        .update(request(std::collections::HashMap::from([(
            "KEY".into(),
            "value".into(),
        )])))
        .await
        .unwrap_err();
    assert_eq!(error.code(), tonic::Code::FailedPrecondition);
    assert!(service.lookup_sandbox(&id).unwrap().process_env.is_empty());
    // A stale setting must remain clearable without an unsupported guest RPC.
    crate::db::update_process_env(
        &service.db_conn().unwrap(),
        &id,
        &std::collections::BTreeMap::from([("STALE".into(), "value".into())]),
    )
    .unwrap();
    service.update(request(Default::default())).await.unwrap();
    assert!(service.lookup_sandbox(&id).unwrap().process_env.is_empty());
    assert!(node.calls().is_empty());
}

#[tokio::test]
async fn restored_windows_registration_preserves_guest_identity_and_native_app_readiness() {
    use crate::service::activation_support::internal_request;
    use pplx_space_common_core::types::Arch;
    let native = GuestImageContract::new(
        GuestDiskLayout::OpaqueRawDisk,
        GuestReadiness::Agent,
        GuestNetwork::SlotDhcpV1,
        pplx_space_common_core::types::guest_image::GuestAgent::Spaced,
    )
    .unwrap();
    let catalog =
        pplx_space_execution_catalog::catalog::CompiledEnvironment::for_architecture(Arch::X86_64)
            .unwrap();
    let execution = catalog
        .iter()
        .find(|entry| entry.name() == "ch-54.0-windows-uefi")
        .unwrap()
        .definition()
        .execution_requirements()
        .unwrap();
    let conn = crate::db::test_conn();
    let id = SandboxId::new().to_string();
    insert_firmware_sandbox(&conn, &id, SandboxState::Started, native);
    crate::db::upsert_sandbox_execution_metadata(&conn, &id, &execution).unwrap();
    crate::db::update_user(
        &conn,
        &id,
        &pplx_space_common_core::types::TEMPLATE_MANAGER_SANDBOX_OWNER,
    )
    .unwrap();
    let service = SandboxManagerService::for_test(conn);
    // Cold activation, restore and manager recovery all register from this
    // durable row after committing the current runtime fields.
    service.watchdog.rearm_sandbox_from_db(&id).await;
    let info = service.watchdog.sidecar_info_for_test(&id).await.unwrap();
    assert_eq!(info.guest_image, native);
    let row = service.lookup_sandbox(&id).unwrap();
    assert_eq!(
        profile_for_sandbox(&row).unwrap().agent_protocol(),
        pplx_space_common_core::types::guest_image::QualifiedGuestAgent::WindowsV1
    );
    let response = service
        .wait_template_apps(internal_request(crate::proto::WaitTemplateAppsRequest {
            sandbox_id: id,
        }))
        .await
        .unwrap();
    assert!(response.into_inner().warnings.is_empty());
    service.shutdown_token.cancel();
}
