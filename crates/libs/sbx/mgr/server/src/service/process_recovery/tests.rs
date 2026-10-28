use super::*;
use crate::service::cleanup_support::require_state_write_from;
use crate::service::tests::service_tests::insert_test_sandbox_with_runtime;
use pplx_space_common_core::types::SandboxId;

#[tokio::test]
async fn recovery_activation_rechecks_state_after_snapshot_exclusion() {
    let id = SandboxId::new().to_string();
    let conn = crate::db::test_conn();
    insert_test_sandbox_with_runtime(&conn, &id, SandboxState::Started);
    let service = SandboxManagerService::for_test(conn);
    let lock = service.auto_lifecycle.artifact_lock(&id);
    let capture = lock.clone().lock_owned().await;
    let mut activation = Box::pin(service.recovery_activation_scope(&id, None));
    assert!(futures_util::poll!(&mut activation).is_pending());
    {
        let conn = service.db_conn().unwrap();
        require_state_write_from(
            crate::db::update_state_from(&conn, &id, &crate::db::edges::PAUSE_BEGIN),
            "prepare paused test sandbox",
        )
        .unwrap();
        require_state_write_from(
            crate::db::update_state_from(&conn, &id, &crate::db::edges::PAUSE_COMPLETE),
            "prepare paused test sandbox",
        )
        .unwrap();
    }
    assert_eq!(
        service.lookup_sandbox(&id).unwrap().state,
        SandboxState::Paused
    );
    drop(capture);
    assert_eq!(
        activation.await.err().unwrap().code(),
        tonic::Code::FailedPrecondition
    );
    drop(
        lock.try_lock()
            .expect("refusal releases snapshot exclusion"),
    );
    service.shutdown_token.cancel();
}

#[tokio::test]
async fn recovery_activation_holds_snapshot_exclusion_through_confirmation() {
    let id = SandboxId::new().to_string();
    let conn = crate::db::test_conn();
    insert_test_sandbox_with_runtime(&conn, &id, SandboxState::Started);
    let service = SandboxManagerService::for_test(conn);
    let scope = service.recovery_activation_scope(&id, None).await.unwrap();
    let lock = service.auto_lifecycle.artifact_lock(&id);
    lock.try_lock().unwrap_err();
    assert_eq!(
        scope.sandbox.node_execution_id,
        service.lookup_sandbox(&id).unwrap().node_execution_id
    );
    drop(scope);
    drop(lock.try_lock().unwrap());
    service.shutdown_token.cancel();
}

#[tokio::test(start_paused = true)]
async fn legacy_readiness_survives_busy_or_failed_rearming() {
    use pplx_space_spaced_host_client::proto::ProcessExecutionInfo;
    let observed = WaitReadyResponse {
        guest_uptime: Some(prost_types::Duration {
            seconds: 10,
            nanos: 0,
        }),
        process_recovery_barrier_id: Some(ProcessRecoveryToken::new().to_string()),
        process_execution: Some(ProcessExecutionInfo {
            instance_id: ProcessRecoveryToken::new().to_string(),
            activation_id: Some(ProcessRecoveryToken::new().to_string()),
            capabilities: vec![
                "process_output_gap_resume".into(),
                "process_recovery_start".into(),
            ],
            ..Default::default()
        }),
        ..Default::default()
    };
    let id = SandboxId::new().to_string();
    let conn = crate::db::test_conn();
    insert_test_sandbox_with_runtime(&conn, &id, SandboxState::Started);
    let service = SandboxManagerService::for_test(conn);
    let capture = service.auto_lifecycle.artifact_lock(&id).lock_owned().await;
    for failure in [
        None,
        Some(tonic::Code::FailedPrecondition),
        Some(tonic::Code::ResourceExhausted),
    ] {
        let response =
            best_effort_recovery_ready(observed.clone(), Duration::from_millis(100), async {
                if let Some(code) = failure {
                    return Err(Status::new(code, "rearm unavailable"));
                }
                let _scope = service.recovery_activation_scope(&id, None).await?;
                Ok(observed.clone())
            })
            .await;
        assert_eq!(response.guest_uptime, observed.guest_uptime);
        let info = response.process_execution.unwrap();
        assert_eq!(
            info.instance_id,
            observed.process_execution.as_ref().unwrap().instance_id
        );
        assert_eq!(info.capabilities, ["process_output_gap_resume"]);
        assert!(info.activation_id.is_none());
    }
    drop(capture);
    service.shutdown_token.cancel();
}

#[tokio::test]
async fn recovery_adapters_refuse_mixed_or_stale_requests_before_waking() {
    use crate::service::{
        activation_support::internal_request, tests::service_tests::route_authorized_service,
    };
    use pplx_space_common_svc::space_api_route_ed25519::insert_space_api_route_ed25519_token;
    use pplx_space_spaced_common::{proto::operations as ops, recovery::RecoveryReason};
    for state in [
        SandboxState::Started,
        SandboxState::Paused,
        SandboxState::Suspended,
    ] {
        let conn = crate::db::test_conn();
        let id = SandboxId::new();
        crate::db::fixture::insert_test_sandbox(&conn, &id.to_string(), SandboxState::Started);
        let (service, token) = route_authorized_service(conn, id);
        crate::db::test_fixtures::state_write_fixtures::force_state(
            &service.db_conn().unwrap(),
            &id.to_string(),
            state,
        );
        let fence = pplx_space_spaced_common::recovery::RecoveryFence {
            instance: ProcessRecoveryToken::new(),
            activation: ProcessRecoveryToken::new(),
        };
        let handle = ops::RecoveryHandle {
            contract_version: 1,
            expected_instance_id: fence.instance.to_string(),
            expected_activation_id: fence.activation.to_string(),
            process_id: "original-process".into(),
            process_incarnation: ProcessRecoveryToken::new().to_string(),
            sequence: None,
        };
        for mixed in [false, true] {
            macro_rules! check {
                ($method:ident, $request:ident, $operation:expr) => {{
                    let mut request = internal_request($request {
                        sandbox_id: id.to_string(),
                        operation: Some($operation),
                    });
                    insert_space_api_route_ed25519_token(request.metadata_mut(), &token).unwrap();
                    let status = service
                        .$method(request)
                        .await
                        .err()
                        .expect("refused before host contact or wake");
                    if mixed {
                        assert_eq!(status.code(), tonic::Code::InvalidArgument);
                    } else {
                        let expected = if state == SandboxState::Suspended {
                            RecoveryReason::ActivationChanged
                        } else if service
                            .lookup_sandbox(&id.to_string())
                            .unwrap()
                            .node_execution_id
                            .is_none()
                        {
                            RecoveryReason::TemporarilyUnavailable
                        } else {
                            RecoveryReason::ActivationChanged
                        };
                        assert_eq!(RecoveryReason::from_status(&status), Some(expected));
                    }
                }};
            }
            let legacy = if mixed { "legacy-process" } else { "" }.to_owned();
            check!(
                spaced_start_process,
                SpacedStartProcessRequest,
                ops::StartRequest {
                    command: if mixed {
                        vec!["/bin/true".into()]
                    } else {
                        vec![]
                    },
                    idempotency_key: Some("adapter-start".into()),
                    recovery: Some(ops::RecoveryStart {
                        contract_version: 1,
                        expected_instance_id: fence.instance.to_string(),
                        expected_activation_id: fence.activation.to_string(),
                        command: vec!["/bin/true".into()]
                    }),
                    ..Default::default()
                }
            );
            check!(
                spaced_connect_process,
                SpacedConnectProcessRequest,
                ops::ConnectProcessRequest {
                    process_id: legacy.clone(),
                    recovery: Some(handle.clone()),
                    ..Default::default()
                }
            );
            check!(
                spaced_send_input,
                SpacedSendInputRequest,
                ops::SendInputRequest {
                    process_id: legacy.clone(),
                    recovery: Some(handle.clone()),
                    data: b"must not write".to_vec()
                }
            );
            check!(
                spaced_close_stdin,
                SpacedCloseStdinRequest,
                ops::CloseStdinRequest {
                    process_id: legacy.clone(),
                    recovery: Some(handle.clone())
                }
            );
            check!(
                spaced_send_signal,
                SpacedSendSignalRequest,
                ops::SendSignalRequest {
                    process_id: legacy.clone(),
                    recovery: Some(handle.clone()),
                    signal: libc::SIGKILL
                }
            );
            check!(
                spaced_resize_pty,
                SpacedResizePtyRequest,
                ops::ResizePtyRequest {
                    process_id: legacy,
                    recovery: Some(handle.clone()),
                    size: Some(ops::PtySize { rows: 24, cols: 80 })
                }
            );
            assert_eq!(
                service.lookup_sandbox(&id.to_string()).unwrap().state,
                state
            );
        }
        let mut sandbox = service.lookup_sandbox(&id.to_string()).unwrap();
        // The same external epoch remains usable for a resident pause, but never restores a snapshot.
        sandbox.node_execution_id = Some(fence.activation.to_string().parse().unwrap());
        let result = resident_recovery_fence(&sandbox, fence);
        if state == SandboxState::Suspended {
            assert_eq!(
                RecoveryReason::from_status(&result.unwrap_err()),
                Some(RecoveryReason::ActivationChanged)
            );
        } else {
            result.unwrap();
        }
        service.shutdown_token.cancel();
    }
}

#[test]
fn rearm_failures_are_temporary_without_masking_unsupported_or_stale_handles() {
    for code in [
        tonic::Code::ResourceExhausted,
        tonic::Code::Unavailable,
        tonic::Code::FailedPrecondition,
        tonic::Code::DeadlineExceeded,
    ] {
        assert_eq!(
            RecoveryReason::from_status(&rearm_status(Status::new(
                code,
                "synthetic rearm failure"
            ))),
            Some(RecoveryReason::TemporarilyUnavailable)
        );
    }
    let unsupported = pplx_space_common_grpc::guest_capability_unsupported_status(
        pplx_space_common_grpc::GuestCapability::ProcessRecoveryStart,
        "unsupported",
    );
    assert!(
        pplx_space_common_grpc::GuestCapabilityUnsupported::from_status(&rearm_status(unsupported))
            .is_some()
    );
    assert_eq!(
        RecoveryReason::from_status(&rearm_status(
            RecoveryReason::ActivationChanged.into_status()
        )),
        Some(RecoveryReason::ActivationChanged)
    );
}

#[tokio::test]
async fn recovery_wake_rechecks_a_matching_fence_after_lock_admission() {
    let conn = crate::db::test_conn();
    let id = SandboxId::new().to_string();
    insert_test_sandbox_with_runtime(&conn, &id, SandboxState::Started);
    let service = SandboxManagerService::for_test(conn);
    let sandbox = service.lookup_sandbox(&id).unwrap();
    let fence = pplx_space_spaced_common::recovery::RecoveryFence {
        instance: ProcessRecoveryToken::new(),
        activation: sandbox
            .node_execution_id
            .unwrap()
            .to_string()
            .parse()
            .unwrap(),
    };
    resident_recovery_fence(&sandbox, fence).unwrap();
    let active = service
        .ensure_active_for_guest_activity_with_start(
            None,
            &id,
            true,
            Some(Duration::from_secs(1)),
            GuestActivityKind::Access,
            WakeStartConfig {
                recovery_fence: Some(fence),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(active.node_execution_id, sandbox.node_execution_id);
    let guard = service.auto_lifecycle.wake_lock(&id).lock_owned().await;
    let mut waking = Box::pin(service.ensure_active_for_guest_activity_with_start(
        None,
        &id,
        true,
        Some(Duration::from_secs(1)),
        GuestActivityKind::Access,
        WakeStartConfig {
            recovery_fence: Some(fence),
            ..Default::default()
        },
    ));
    assert!(futures_util::poll!(&mut waking).is_pending());
    crate::db::test_fixtures::state_write_fixtures::force_state(
        &service.db_conn().unwrap(),
        &id,
        SandboxState::Suspended,
    );
    drop(guard);
    let status = waking.await.err().expect("stale activation");
    assert_eq!(
        RecoveryReason::from_status(&status),
        Some(RecoveryReason::ActivationChanged)
    );
    assert_eq!(
        service.lookup_sandbox(&id).unwrap().state,
        SandboxState::Suspended
    );
    service.shutdown_token.cancel();
}

mod transport;
