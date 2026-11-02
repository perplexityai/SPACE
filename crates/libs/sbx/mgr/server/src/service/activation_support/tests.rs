use super::super::tests::service_tests::insert_test_sandbox;
use super::*;

mod interception_recovery;

#[tokio::test]
async fn manager_checks_readiness_identity_with_legacy_status_fallback() {
    use pplx_space_spaced_host_client::proto::{GetStatusResponse, VersionInfo};
    // The shared policy owns version parsing/ranges. The manager must validate
    // the reported identity without probing again when WaitReady supplies it.
    for (legacy, ready, guest, expected) in [
        (false, true, Some(("0.1.0", "0.1")), None),
        (
            false,
            true,
            Some(("0.9.0", "0.9")),
            Some(tonic::Code::FailedPrecondition),
        ),
        (
            false,
            true,
            Some(("", "")),
            Some(tonic::Code::FailedPrecondition),
        ),
        (true, true, Some(("0.1.0", "0.1")), None),
        (
            true,
            true,
            Some(("0.9.0", "0.9")),
            Some(tonic::Code::FailedPrecondition),
        ),
        (true, true, None, Some(tonic::Code::FailedPrecondition)),
        (
            true,
            false,
            Some(("0.1.0", "0.1")),
            Some(tonic::Code::Unavailable),
        ),
    ] {
        let guest = guest.map(|(version, compatibility_family)| VersionInfo {
            version: version.to_string(),
            compatibility_family: compatibility_family.to_string(),
        });
        let legacy_status = || {
            assert!(
                legacy,
                "readiness identity must not trigger another guest probe"
            );
            std::future::ready(Ok(GetStatusResponse {
                ready,
                guest_version: guest.clone(),
                ..Default::default()
            }))
        };
        assert_eq!(
            validate_ready_guest_identity(
                if legacy { None } else { guest.as_ref() },
                legacy_status
            )
            .await
            .err()
            .map(|error| error.code()),
            expected
        );
    }
}

#[tokio::test(start_paused = true)]
async fn restore_completion_requires_gateway_readiness_only_when_starting() {
    for (final_state, pool_template, should_succeed) in [
        (SandboxState::Started, None, false),
        (SandboxState::Paused, None, true),
        (SandboxState::Started, Some("seed"), true),
    ] {
        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new().to_string();
        insert_test_sandbox(&conn, &sandbox_id, SandboxState::Restoring);
        crate::db::set_pool_template(&conn, &sandbox_id, pool_template).unwrap();
        let db = crate::db::SharedConnection::new(conn);

        // No gateway exists. A restored snapshot alone must not authorize
        // publishing Started, while a drain restore may remain Paused.
        let result = mark_restore_after_network_ready(&db, &sandbox_id, final_state, None, async {
            refresh_network_policy(&db, &sandbox_id, None, "snapshot-restore")
                .await
                .map_err(network_policy_refresh_error_to_status)
        })
        .await;
        if !should_succeed {
            let error = result.expect_err("restore succeeded without a ready gateway");
            assert!(
                matches!(
                    error.code(),
                    tonic::Code::Internal | tonic::Code::Unavailable
                ),
                "expected a gateway connection failure or policy deadline: {error}"
            );
            assert!(
                error
                    .message()
                    .starts_with("failed to refresh network policy:")
                    || error.message() == "gateway network-readiness deadline",
                "unexpected failure: {error}"
            );
        } else {
            result.unwrap();
        }
        let row = crate::db::get_sandbox(&db.lock().unwrap(), &sandbox_id)
            .unwrap()
            .unwrap();
        assert_eq!(
            row.state,
            if !should_succeed {
                SandboxState::Restoring
            } else {
                final_state
            }
        );
    }
}

#[tokio::test(start_paused = true)]
async fn restore_defers_guest_readiness_only_while_paused() {
    for final_state in [SandboxState::Started, SandboxState::Paused] {
        let conn = crate::db::test_conn();
        let id = SandboxId::new().to_string();
        insert_test_sandbox(&conn, &id, SandboxState::Restoring);
        // Exclude the network gate: even a pool restore needs guest support
        // before Started. No guest exists to answer this synthetic sandbox.
        crate::db::set_pool_template(&conn, &id, Some("seed")).unwrap();
        let db = crate::db::SharedConnection::new(conn);
        let result =
            complete_restore_and_mark_final(db.clone(), &id, &BTreeMap::new(), final_state, None)
                .await;
        if final_state == SandboxState::Started {
            let status = result.expect_err("snapshot alone cannot establish guest compatibility");
            assert!(matches!(
                status.code(),
                tonic::Code::Unavailable | tonic::Code::DeadlineExceeded
            ));
        } else {
            result.expect("paused restore must not wait on a paused guest");
        }
        let row = crate::db::get_sandbox(&db.lock().unwrap(), &id)
            .unwrap()
            .unwrap();
        assert_eq!(
            row.state,
            if final_state == SandboxState::Started {
                SandboxState::Restoring
            } else {
                SandboxState::Paused
            }
        );
    }
}

#[tokio::test]
async fn start_guest_failure_preserves_terminal_status_and_activation_ownership() {
    for (code, superseded) in [
        (tonic::Code::FailedPrecondition, false),
        (tonic::Code::DeadlineExceeded, false),
        (tonic::Code::FailedPrecondition, true),
    ] {
        let conn = crate::db::test_conn();
        let id = SandboxId::new().to_string();
        insert_test_sandbox(
            &conn,
            &id,
            if superseded {
                SandboxState::Stopped
            } else {
                SandboxState::Starting
            },
        );
        let db = crate::db::SharedConnection::new(conn);
        let status = complete_start_after_readiness(
            db.clone(),
            &id,
            &BTreeMap::new(),
            30,
            "start",
            None,
            StartReadiness {
                guest: std::future::ready(Err::<GuestReadyEvidence, _>(Status::new(
                    code,
                    "guest readiness rejected",
                ))),
                policy: None::<std::future::Ready<Result<(), Status>>>,
            },
        )
        .await
        .unwrap_err();
        assert_eq!(
            status.code(),
            if superseded {
                tonic::Code::Aborted
            } else {
                code
            }
        );
        let row = crate::db::get_sandbox(&db.lock().unwrap(), &id)
            .unwrap()
            .unwrap();
        assert_eq!(
            row.state,
            if superseded {
                SandboxState::Stopped
            } else {
                SandboxState::Starting
            }
        );
    }
}

#[tokio::test]
async fn restore_publication_waits_for_network_readiness_and_preserves_activation_fences() {
    for (failure, superseded) in [
        (None, false),
        (Some(tonic::Code::Unavailable), false),
        (Some(tonic::Code::FailedPrecondition), false),
        (None, true),
        (Some(tonic::Code::Unavailable), true),
    ] {
        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new().to_string();
        insert_test_sandbox(&conn, &sandbox_id, SandboxState::Suspended);
        let episode = crate::db::begin_activation(
            &conn,
            &sandbox_id,
            SandboxState::Suspended,
            crate::db::ActivationKind::Restore,
            None,
            None,
        )
        .unwrap()
        .unwrap();
        let db = crate::db::SharedConnection::new(conn);
        let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
        let completion = mark_restore_after_network_ready(
            &db,
            &sandbox_id,
            SandboxState::Started,
            Some(episode.id),
            async { ready_rx.await.unwrap() },
        );
        tokio::pin!(completion);
        assert!(futures_util::poll!(&mut completion).is_pending());
        {
            let conn = db.lock().unwrap();
            assert_eq!(
                crate::db::get_sandbox(&conn, &sandbox_id)
                    .unwrap()
                    .unwrap()
                    .state,
                SandboxState::Restoring,
                "pending network readiness must not publish Started"
            );
            assert_eq!(
                crate::db::cleanup_episode(&conn, &sandbox_id)
                    .unwrap()
                    .unwrap()
                    .id,
                episode.id
            );
            if superseded {
                crate::db::mark_deleting_superseding_activation(&conn, &sandbox_id).unwrap();
            }
        }
        ready_tx
            .send(failure.map_or(Ok(()), |code| Err(Status::new(code, "gateway not ready"))))
            .unwrap();
        let result = completion.await;
        let expected_error = if superseded {
            Some(tonic::Code::Aborted)
        } else {
            failure
        };
        assert_eq!(result.err().map(|error| error.code()), expected_error);
        let conn = db.lock().unwrap();
        assert_eq!(
            crate::db::get_sandbox(&conn, &sandbox_id)
                .unwrap()
                .unwrap()
                .state,
            if superseded {
                SandboxState::Deleting
            } else if failure.is_some() {
                SandboxState::Restoring
            } else {
                SandboxState::Started
            }
        );
        if !superseded {
            assert_eq!(
                crate::db::cleanup_episode(&conn, &sandbox_id)
                    .unwrap()
                    .is_some(),
                failure.is_some()
            );
        }
    }
}

#[test]
fn separate_operation_requires_a_kubernetes_route_but_not_an_api_supplied_node() {
    use pplx_space_common_core::types::{ExecutionGeneration, NodeManagedBy, RouteGeneration};
    let attempt = SandboxActivationAttemptId::new();
    let route = SpaceApiRouteIdentity {
        route_generation: Some(RouteGeneration::new(7).unwrap()),
        execution_generation: Some(ExecutionGeneration::new(7).unwrap()),
        substrate_kind: Some(NodeManagedBy::Kubernetes),
        activation_attempt_id: Some(attempt),
        pod_uid: Some("pod-7".parse().unwrap()),
        ..Default::default()
    };
    let wire = attempt.to_string();
    assert_eq!(
        operation_activation_attempt_id_from_wire(Some(&wire), &route).unwrap(),
        Some(attempt)
    );
    assert_eq!(
        operation_activation_attempt_id_from_wire(None, &SpaceApiRouteIdentity::default()).unwrap(),
        None
    );
    for (value, candidate) in [
        ("", route.clone()),
        ("malformed", route.clone()),
        (wire.as_str(), SpaceApiRouteIdentity::default()),
        (
            wire.as_str(),
            SpaceApiRouteIdentity {
                substrate_kind: Some(NodeManagedBy::Space),
                ..route.clone()
            },
        ),
    ] {
        assert_eq!(
            operation_activation_attempt_id_from_wire(Some(value), &candidate)
                .unwrap_err()
                .code(),
            tonic::Code::InvalidArgument
        );
    }
}

#[derive(Clone, Default)]
struct GuestSpecsLogCapture(Arc<Mutex<std::collections::BTreeMap<String, String>>>);

impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for GuestSpecsLogCapture {
    fn on_event(
        &self,
        event: &tracing::Event<'_>,
        _context: tracing_subscriber::layer::Context<'_, S>,
    ) {
        let mut visitor = pplx_space_observability::test_capture::RecordingVisitor::default();
        event.record(&mut visitor);
        self.0
            .lock()
            .expect("guest specs log capture lock poisoned")
            .extend(visitor.into_fields());
    }
}

#[test]
fn guest_observed_specs_log_identifies_the_sandbox_and_observed_values() {
    use tracing_subscriber::layer::SubscriberExt as _;

    let capture = GuestSpecsLogCapture::default();
    let subscriber = tracing_subscriber::registry().with(capture.clone());

    tracing::subscriber::with_default(subscriber, || {
        emit_guest_observed_specs(
            "019c3cb7-b54d-7c42-a003-bb380e243c33",
            "app-runtime",
            Some(8_589_934_592),
            Some(4),
        );
    });

    assert_eq!(
        *capture.0.lock().unwrap(),
        std::collections::BTreeMap::from([
            (
                "message".to_string(),
                "observed guest sandbox specs".to_string()
            ),
            ("mem_total_bytes".to_string(), "8589934592".to_string()),
            (
                "sandbox_id".to_string(),
                "019c3cb7-b54d-7c42-a003-bb380e243c33".to_string()
            ),
            ("template".to_string(), "app-runtime".to_string()),
            ("vcpu_count".to_string(), "4".to_string()),
        ])
    );
}

/// Every activation stage, paired with whether its failure carries a
/// node-manager status whose terminal codes must survive the hop out to the
/// shim. Exhaustive over [`ActivationFailureStage`] so a stage added later
/// shows up here as a deliberate choice rather than an accident.
const STAGE_REPORTS_NODE_MANAGER_STATUS: &[(ActivationFailureStage, bool)] = &[
    (ActivationFailureStage::ColdPrepare, true),
    (ActivationFailureStage::ColdFirecrackerLaunch, true),
    (ActivationFailureStage::RestorePrepare, true),
    (ActivationFailureStage::RestoreFirecrackerLaunch, true),
    (ActivationFailureStage::RestoreSnapshotLoad, true),
    (ActivationFailureStage::RestoreResume, true),
    (ActivationFailureStage::ColdMetadata, false),
    (ActivationFailureStage::ColdEncryptedRootfsPrepare, false),
    (ActivationFailureStage::ColdSidecarLaunch, false),
    (ActivationFailureStage::RestoreMetadata, false),
    (
        ActivationFailureStage::RestoreEncryptedDevicesPrepare,
        false,
    ),
    (ActivationFailureStage::RestoreNetGatewayLaunch, false),
    (ActivationFailureStage::RestoreSpacedHostLaunch, false),
];

/// The two codes node-manager uses for "this node cannot execute this
/// sandbox until something is installed, repaired, or the request changes."
const TERMINAL_NODE_MANAGER_CODES: &[tonic::Code] = &[
    tonic::Code::FailedPrecondition,
    tonic::Code::InvalidArgument,
    tonic::Code::PermissionDenied,
];

#[test]
fn every_stage_declares_whether_it_carries_a_node_manager_status() {
    for (stage, expected) in STAGE_REPORTS_NODE_MANAGER_STATUS {
        assert_eq!(
            stage.reports_node_manager_status(),
            *expected,
            "unexpected node-manager-status disposition for {stage:?}"
        );
    }
    assert_eq!(
        STAGE_REPORTS_NODE_MANAGER_STATUS.len(),
        13,
        "the table must name every ActivationFailureStage exactly once"
    );
}

#[test]
fn node_manager_terminal_codes_are_exactly_the_documented_three() {
    for raw in 0..=16 {
        let code = tonic::Code::from_i32(raw);
        assert_eq!(
            node_manager_failure_is_terminal(code),
            TERMINAL_NODE_MANAGER_CODES.contains(&code),
            "unexpected terminality for {code:?}"
        );
    }
    assert_eq!(
        tonic::Code::from_i32(17),
        tonic::Code::Unknown,
        "a new gRPC code must be classified in node_manager_failure_is_terminal, \
         not folded into Unknown by this loop"
    );
}

#[test]
fn node_reported_stages_keep_a_terminal_node_manager_code() {
    let stages = STAGE_REPORTS_NODE_MANAGER_STATUS
        .iter()
        .filter(|(_, reports)| *reports);
    for (stage, _) in stages {
        for code in TERMINAL_NODE_MANAGER_CODES {
            let node_error = Status::new(
                *code,
                "kernel installation /space/cache/firecracker/assets/default/kernel/\
                 vmlinux.bin is missing vmlinux.bin",
            );
            let reason = format!("prepare sandbox failed: {node_error}");

            let status = stage.failure_status(&node_error, reason.clone());

            assert_eq!(
                status.code(),
                *code,
                "{stage:?} must not report a terminal {code:?} as retryable"
            );
            assert_eq!(status.message(), reason);
        }
    }
}

#[test]
fn retryable_node_manager_codes_collapse_to_internal() {
    for raw in 0..=16 {
        let code = tonic::Code::from_i32(raw);
        if TERMINAL_NODE_MANAGER_CODES.contains(&code) {
            continue;
        }
        let node_error = Status::new(code, "node-manager lifecycle rpc failed");
        let reason = format!("prepare sandbox failed: {node_error}");

        let status =
            ActivationFailureStage::ColdPrepare.failure_status(&node_error, reason.clone());

        assert_eq!(
            status.code(),
            tonic::Code::Internal,
            "{code:?} must stay retryable"
        );
        assert_eq!(status.message(), reason);
    }
}

#[test]
fn locally_built_failures_stay_internal_even_when_terminal_shaped() {
    let stages = STAGE_REPORTS_NODE_MANAGER_STATUS
        .iter()
        .filter(|(_, reports)| !*reports);
    for (stage, _) in stages {
        let local_error = Status::failed_precondition("locally built precondition failure");
        let reason = format!("prepare metadata failed: {}", local_error.message());

        let status = stage.failure_status(&local_error, reason.clone());

        assert_eq!(
            status.code(),
            tonic::Code::Internal,
            "{stage:?} has no inbound node-manager verdict to preserve"
        );
        assert_eq!(status.message(), reason);
    }
}

#[tokio::test]
async fn restore_final_state_db_error_is_propagated() {
    let db = crate::db::SharedConnection::new(crate::db::Connection::open_in_memory());

    let result = complete_restore_and_mark_final(
        db,
        "sandbox-1",
        &BTreeMap::new(),
        SandboxState::Paused,
        None,
    )
    .await;

    let error = result.expect_err("missing sandbox schema should fail final-state persistence");
    assert_eq!(error.code(), tonic::Code::Internal);
    assert!(
        error
            .message()
            .contains("failed to persist final restore state")
    );
}

#[tokio::test]
async fn restore_final_state_requires_a_restoring_row() {
    let conn = crate::db::test_conn();
    let db = crate::db::SharedConnection::new(conn);

    let result = complete_restore_and_mark_final(
        db,
        "missing-sandbox",
        &BTreeMap::new(),
        SandboxState::Paused,
        None,
    )
    .await;

    let error = result.expect_err("missing restoring row should prevent lifecycle handoff");
    assert_eq!(error.code(), tonic::Code::Aborted);
}

#[tokio::test(start_paused = true)]
async fn restore_finalizer_retains_cleanup_until_state_persistence_succeeds() {
    for final_state in [SandboxState::Paused, SandboxState::Started] {
        let sandbox_id = SandboxId::new().to_string();
        let (conn, cleanup_id) = if final_state == SandboxState::Started {
            let conn = crate::db::test_conn();
            insert_test_sandbox(&conn, &sandbox_id, SandboxState::Suspended);
            let episode = crate::db::begin_activation(
                &conn,
                &sandbox_id,
                SandboxState::Suspended,
                crate::db::ActivationKind::Restore,
                None,
                None,
            )
            .unwrap()
            .unwrap();
            (conn, episode.id)
        } else {
            (
                crate::db::Connection::open_in_memory(),
                SandboxCleanupEpisodeId::new(),
            )
        };
        let service = SandboxManagerService::for_test(conn);
        let temp = tempfile::tempdir().unwrap();
        let snapshot_dir = temp.path().join("snapshot");
        std::fs::create_dir(&snapshot_dir).unwrap();
        let (cleanup_tx, cleanup_rx) = tokio::sync::oneshot::channel();
        let cleanup_guard = ActivationCleanupGuard::with_interruption_reason(
            SandboxRuntimeLabel::parse("runtime1").unwrap(),
            SandboxQuarantineReasonCode::Unspecified,
            "activation did not complete",
            move |_, _, _, _| {
                let _ = cleanup_tx.send(());
            },
        );

        spawn_restore_state_transition(
            &service,
            sandbox_id,
            BTreeMap::new(),
            final_state,
            Some(snapshot_dir.to_string_lossy().into_owned()),
            cleanup_guard,
            cleanup_id,
        )
        .unwrap();

        tokio::time::timeout(
            NETWORK_GATEWAY_READINESS_TIMEOUT + Duration::from_secs(10),
            cleanup_rx,
        )
        .await
        .expect("failed restore completion should schedule cleanup")
        .expect("cleanup guard should remain armed");
        assert!(
            snapshot_dir.exists(),
            "failed handoff must preserve retry bytes"
        );
    }
}

#[tokio::test]
async fn cold_finalizer_retains_cleanup_until_state_persistence_succeeds() {
    let service = SandboxManagerService::for_test(crate::db::test_conn());
    let (cleanup_tx, cleanup_rx) = tokio::sync::oneshot::channel();
    let cleanup_guard = ActivationCleanupGuard::with_interruption_reason(
        SandboxRuntimeLabel::parse("runtime1").unwrap(),
        SandboxQuarantineReasonCode::Unspecified,
        "activation did not complete",
        move |_, _, _, _| {
            let _ = cleanup_tx.send(());
        },
    );

    spawn_guarded_start_state_transition(&service, "runtime1", cleanup_guard, async {
        Err(Status::internal("failed to persist final start state"))
    })
    .unwrap();

    tokio::time::timeout(Duration::from_secs(1), cleanup_rx)
        .await
        .expect("failed final-state persistence should schedule cleanup")
        .expect("cleanup guard should remain armed");
}

#[tokio::test]
async fn cold_finalizer_disarms_cleanup_after_state_persistence_succeeds() {
    let service = SandboxManagerService::for_test(crate::db::test_conn());
    let (cleanup_tx, cleanup_rx) = tokio::sync::oneshot::channel();
    let cleanup_guard = ActivationCleanupGuard::with_interruption_reason(
        SandboxRuntimeLabel::parse("runtime1").unwrap(),
        SandboxQuarantineReasonCode::Unspecified,
        "activation did not complete",
        move |_, _, _, _| {
            let _ = cleanup_tx.send(());
        },
    );

    spawn_guarded_start_state_transition(&service, "runtime1", cleanup_guard, async { Ok(()) })
        .unwrap();

    assert!(cleanup_rx.await.is_err());
}

#[tokio::test(start_paused = true)]
async fn activation_env_failure_keeps_setup_context_for_callers_and_cleanup() {
    for (state, phase, expected_code) in [
        (Some(SandboxState::Starting), "start", tonic::Code::Internal),
        (
            Some(SandboxState::Restoring),
            "restore",
            tonic::Code::Internal,
        ),
        (
            Some(SandboxState::Resuming),
            "resume",
            tonic::Code::Internal,
        ),
        (Some(SandboxState::Started), "restore", tonic::Code::Aborted),
        (None, "restore", tonic::Code::NotFound),
    ] {
        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new().to_string();
        let env = BTreeMap::from([("MODE".to_string(), "synthetic".to_string())]);
        if let Some(state) = state {
            let activation = match state {
                SandboxState::Starting => {
                    Some((SandboxState::Created, crate::db::ActivationKind::ColdStart))
                }
                SandboxState::Restoring => {
                    Some((SandboxState::Suspended, crate::db::ActivationKind::Restore))
                }
                _ => None,
            };
            insert_test_sandbox(
                &conn,
                &sandbox_id,
                activation.map_or(state, |(prior, _)| prior),
            );
            crate::db::update_process_env(&conn, &sandbox_id, &env).unwrap();
            if let Some((prior, kind)) = activation {
                crate::db::begin_activation(&conn, &sandbox_id, prior, kind, None, None)
                    .unwrap()
                    .unwrap();
            }
        }
        let cleanup_id = crate::db::cleanup_episode(&conn, &sandbox_id)
            .unwrap()
            .map(|episode| episode.id);
        let db = crate::db::SharedConnection::new(conn);
        let (cleanup_tx, cleanup_rx) = std::sync::mpsc::channel();
        let mut cleanup = ActivationCleanupGuard::with_interruption_reason(
            SandboxRuntimeLabel::parse("runtime1").unwrap(),
            SandboxQuarantineReasonCode::Unspecified,
            "activation did not complete",
            move |_, _, _, reason| cleanup_tx.send(reason).unwrap(),
        );

        // No host exists for this synthetic sandbox. Exhausting connection
        // retries must retain setup context; lifecycle fences remain distinct.
        let status =
            configure_process_env_for_activation(&db, &sandbox_id, &env, phase, cleanup_id)
                .await
                .unwrap_err();
        let status = cleanup.failure(status);
        drop(cleanup);

        assert_eq!(status.code(), expected_code, "{state:?}");
        if expected_code == tonic::Code::Internal {
            assert!(status.message().starts_with(&format!(
                "guest activation reached {phase}, but process env injection failed:"
            )));
        } else {
            assert!(!status.message().contains("process env injection failed"));
        }
        assert_eq!(cleanup_rx.recv().unwrap(), status.message());
    }
}

#[tokio::test(start_paused = true)]
async fn activation_env_retries_only_transient_failures_within_one_budget() {
    use std::cell::Cell;
    for (code, expected_calls) in [
        (tonic::Code::Unavailable, 2),
        (tonic::Code::Unknown, 2),
        (tonic::Code::Cancelled, 2),
        (tonic::Code::PermissionDenied, 1),
        (tonic::Code::InvalidArgument, 1),
        (tonic::Code::FailedPrecondition, 1),
        (tonic::Code::DeadlineExceeded, 1),
        (tonic::Code::Aborted, 1),
    ] {
        let calls = Cell::new(0);
        let result = retry_activation_env("synthetic-sandbox", "restore", || {
            let call = calls.get();
            calls.set(call + 1);
            async move {
                if call == 0 {
                    Err(Status::new(code, "injected"))
                } else {
                    Ok(())
                }
            }
        })
        .await;
        assert_eq!(calls.get(), expected_calls, "{code:?}");
        assert_eq!(result.is_ok(), expected_calls == 2, "{code:?}");
    }
    let calls = Cell::new(0);
    let started = tokio::time::Instant::now();
    let status = retry_activation_env("synthetic-sandbox", "restore", || {
        calls.set(calls.get() + 1);
        async { Err(Status::unavailable("still disconnected")) }
    })
    .await
    .unwrap_err();
    assert_eq!(status.code(), tonic::Code::Unavailable);
    assert_eq!(calls.get(), ACTIVATION_ENV_ATTEMPTS);
    assert!(started.elapsed() < ACTIVATION_ENV_TIMEOUT);

    let started = tokio::time::Instant::now();
    let status = retry_activation_env(
        "synthetic-sandbox",
        "restore",
        std::future::pending::<Result<(), Status>>,
    )
    .await
    .unwrap_err();
    assert_eq!(status.code(), tonic::Code::DeadlineExceeded);
    assert_eq!(started.elapsed(), ACTIVATION_ENV_TIMEOUT);
}

#[tokio::test(start_paused = true)]
async fn activation_env_lost_reply_replays_the_same_map_and_cancellation_stops_retries() {
    use std::cell::{Cell, RefCell};
    let env = BTreeMap::from([("MODE".to_string(), "synthetic".to_string())]);
    let applied = RefCell::new(BTreeMap::new());
    let calls = Cell::new(0);
    retry_activation_env("synthetic-sandbox", "restore", || {
        *applied.borrow_mut() = env.clone();
        calls.set(calls.get() + 1);
        let first = calls.get() == 1;
        async move {
            if first {
                Err(Status::unavailable("reply lost"))
            } else {
                Ok(())
            }
        }
    })
    .await
    .unwrap();
    assert_eq!(*applied.borrow(), env);
    assert_eq!(calls.get(), 2);

    let calls = Cell::new(0);
    {
        let retry = retry_activation_env("synthetic-sandbox", "restore", || {
            calls.set(calls.get() + 1);
            async { Err(Status::unavailable("disconnected")) }
        });
        tokio::pin!(retry);
        assert!(futures_util::poll!(&mut retry).is_pending());
        assert_eq!(calls.get(), 1);
    }
    tokio::time::advance(ACTIVATION_ENV_TIMEOUT).await;
    assert_eq!(calls.get(), 1, "dropping the retry leaves no detached work");
}

#[tokio::test(start_paused = true)]
async fn activation_env_rejects_requested_or_unowned_cleanup() {
    for requested in [false, true] {
        let conn = crate::db::test_conn();
        let id = SandboxId::new().to_string();
        insert_test_sandbox(&conn, &id, SandboxState::Suspended);
        let env = BTreeMap::from([("MODE".to_string(), "synthetic".to_string())]);
        crate::db::update_process_env(&conn, &id, &env).unwrap();
        let episode = crate::db::begin_activation(
            &conn,
            &id,
            SandboxState::Suspended,
            crate::db::ActivationKind::Restore,
            None,
            None,
        )
        .unwrap()
        .unwrap();
        if requested {
            crate::db::request_cleanup_episode(
                &conn,
                &id,
                episode.id,
                SandboxQuarantineReasonCode::RestoreActivationInterrupted,
                "synthetic activation failure",
            )
            .unwrap();
        }
        let db = crate::db::SharedConnection::new(conn);
        for owner in [None, Some(SandboxCleanupEpisodeId::new()), Some(episode.id)] {
            if !requested && owner == Some(episode.id) {
                continue; // The setup-context test covers the current owner.
            }
            let status = configure_process_env_for_activation(&db, &id, &env, "restore", owner)
                .await
                .unwrap_err();
            assert_eq!(
                status.code(),
                tonic::Code::Aborted,
                "{requested}, {owner:?}"
            );
        }
    }
}

#[tokio::test(start_paused = true)]
async fn activation_env_retry_does_not_overwrite_a_replaced_execution_or_configuration() {
    use std::cell::Cell;
    for replace_execution in [false, true] {
        let conn = crate::db::test_conn();
        let id = SandboxId::new().to_string();
        insert_test_sandbox(&conn, &id, SandboxState::Restoring);
        let original = crate::db::get_sandbox(&conn, &id).unwrap().unwrap();
        let mut current = crate::db::get_sandbox(&conn, &id).unwrap().unwrap();
        let calls = Cell::new(0);
        let status = retry_activation_env("synthetic-sandbox", "restore", || {
            let valid = activation_env_matches(&original, &current, &original.process_env);
            if !valid {
                return std::future::ready(Err(Status::aborted("superseded")));
            }
            calls.set(calls.get() + 1);
            // A failed first dispatch is followed by replacement before retry.
            if replace_execution {
                current.node_execution_id = Some(SandboxActivationAttemptId::new());
            } else {
                current
                    .process_env
                    .insert("MODE".to_string(), "newer".to_string());
            }
            std::future::ready(Err(Status::unavailable("reply lost")))
        })
        .await
        .unwrap_err();
        assert_eq!(status.code(), tonic::Code::Aborted);
        assert_eq!(calls.get(), 1);
    }
}

async fn commit_start(service: &SandboxManagerService, id: &str) {
    complete_start_after_readiness(
        service.db.clone(),
        id,
        &BTreeMap::new(),
        30,
        "start",
        None,
        StartReadiness {
            guest: std::future::ready(Ok(
                pplx_space_spaced_host_client::proto::WaitReadyResponse::default().into(),
            )),
            policy: None::<std::future::Ready<Result<(), Status>>>,
        },
    )
    .await
    .expect("the start commits");
}

// The paused clock in these tests moves only while every task waits on a
// timer, so elapsed time shows whether a waiter noticed a commit through the
// signal (no time) or through its next 200 ms re-read of the row.

#[tokio::test(start_paused = true)]
async fn guest_activity_waiting_on_a_start_proceeds_when_the_start_commits() {
    let conn = crate::db::test_conn();
    let id = SandboxId::new().to_string();
    insert_test_sandbox(&conn, &id, SandboxState::Starting);
    let service = SandboxManagerService::for_test(conn);
    let waiter = service.ensure_active_for_guest_activity_with_start(
        None,
        &id,
        false,
        None,
        GuestActivityKind::Command,
        WakeStartConfig::default(),
    );
    tokio::pin!(waiter);
    assert!(futures_util::poll!(&mut waiter).is_pending());

    let committed_at = tokio::time::Instant::now();
    commit_start(&service, &id).await;
    let row = waiter
        .await
        .expect("guest activity proceeds once the start commits");
    assert_eq!(row.state, SandboxState::Started);
    assert_eq!(committed_at.elapsed(), Duration::ZERO);
}

#[tokio::test(start_paused = true)]
async fn a_start_that_commits_after_the_starting_read_ends_the_wait() {
    let notify = tokio::sync::Notify::new();
    let started_at = tokio::time::Instant::now();
    wait_while_starting(&notify, Duration::from_millis(200), || {
        // The start commits after the read saw `Starting` and before the
        // wait is first polled.
        notify.notify_waiters();
        Ok(true)
    })
    .await
    .expect("the wait ends");
    assert_eq!(started_at.elapsed(), Duration::ZERO);
}

#[tokio::test(start_paused = true)]
async fn another_sandboxs_start_does_not_end_a_starting_wait() {
    let conn = crate::db::test_conn();
    let waiting = SandboxId::new().to_string();
    let other = SandboxId::new().to_string();
    insert_test_sandbox(&conn, &waiting, SandboxState::Starting);
    insert_test_sandbox(&conn, &other, SandboxState::Starting);
    let service = SandboxManagerService::for_test(conn);
    let waiter = service.ensure_active_for_guest_activity_with_start(
        None,
        &waiting,
        false,
        None,
        GuestActivityKind::Command,
        WakeStartConfig::default(),
    );
    tokio::pin!(waiter);
    assert!(futures_util::poll!(&mut waiter).is_pending());

    commit_start(&service, &other).await;
    tokio::select! {
        biased;
        result = &mut waiter => panic!(
            "the other sandbox's start ended the wait: {:?}",
            result.map(|row| row.state)
        ),
        () = tokio::time::sleep(Duration::from_millis(100)) => {}
    }

    let committed_at = tokio::time::Instant::now();
    commit_start(&service, &waiting).await;
    let row = waiter
        .await
        .expect("guest activity proceeds once its own start commits");
    assert_eq!(row.state, SandboxState::Started);
    assert_eq!(committed_at.elapsed(), Duration::ZERO);
}

#[tokio::test(start_paused = true)]
async fn a_start_does_not_wake_a_waiter_on_another_transition() {
    let conn = crate::db::test_conn();
    let restoring = SandboxId::new().to_string();
    let starting = SandboxId::new().to_string();
    insert_test_sandbox(&conn, &restoring, SandboxState::Restoring);
    insert_test_sandbox(&conn, &starting, SandboxState::Starting);
    let service = SandboxManagerService::for_test(conn);
    let waiter = service.ensure_active_for_guest_activity_with_start(
        None,
        &restoring,
        false,
        None,
        GuestActivityKind::Command,
        WakeStartConfig::default(),
    );
    tokio::pin!(waiter);
    assert!(futures_util::poll!(&mut waiter).is_pending());
    let waiting_since = tokio::time::Instant::now();

    // Restore completion is not signalled, so only a wake from the start
    // below would let the waiter see `Started` before its re-read.
    assert!(
        crate::db::mark_restore_succeeded(
            &service.db.lock().unwrap(),
            &restoring,
            SandboxState::Started,
            None,
        )
        .unwrap()
    );
    commit_start(&service, &starting).await;
    let row = waiter
        .await
        .expect("guest activity proceeds once the restore commits");
    assert_eq!(row.state, SandboxState::Started);
    assert_eq!(waiting_since.elapsed(), Duration::from_millis(200));
}
