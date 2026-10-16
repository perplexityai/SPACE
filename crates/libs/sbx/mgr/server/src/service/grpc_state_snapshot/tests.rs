use super::*;
use crate::service::activation_support::internal_request;
use pplx_space_common_core::types::{
    ExecutionGeneration, NodeManagedBy, RouteGeneration, SandboxId, SandboxRuntimeLabel,
};
use tokio::sync::oneshot;

#[test]
fn live_sandbox_state_includes_network_namespace() {
    let runtime_label = SandboxRuntimeLabel::from_sandbox_id(SandboxId::new());

    assert_eq!(
        live_sandbox_namespace(true, &runtime_label),
        Some(format!("sns-{runtime_label}"))
    );
    assert_eq!(live_sandbox_namespace(false, &runtime_label), None);
}

#[test]
fn cached_runtime_state_only_refines_resident_lifecycle() {
    for state in SandboxState::all() {
        for pid_alive in [false, true] {
            let expected =
                if pid_alive && matches!(state, SandboxState::Started | SandboxState::Paused) {
                    SandboxState::Paused
                } else {
                    state
                };
            assert_eq!(
                sandbox_status_from_runtime_observation(
                    state,
                    pid_alive,
                    Some(FirecrackerVmState::Paused),
                ),
                expected,
                "{state}, pid_alive={pid_alive}"
            );
        }
    }
}

#[test]
fn retained_route_identity_provenance_wire_shape() {
    let mut sandbox = SandboxInfo {
        sandbox_id: "019e6370-cfc9-7a43-9337-399c7b51a1d9".to_string(),
        lifecycle: Some(crate::proto::SandboxLifecycleState::Suspended as i32),
        ..SandboxInfo::default()
    };
    apply_route_identity_provenance_to_state(
        &mut sandbox,
        crate::db::ActivationRouteProvenance {
            current_route_generation: None,
            current_execution_generation: None,
            current_substrate_kind: None,
            retained_route_generation: Some(RouteGeneration::new(7).unwrap()),
            retained_execution_generation: Some(ExecutionGeneration::new(9).unwrap()),
            retained_substrate_kind: Some(NodeManagedBy::Space),
        },
    );

    insta::assert_debug_snapshot!(sandbox);
}

#[test]
fn retained_kubernetes_provenance_cannot_be_reported_as_direct() {
    let mut sandbox = SandboxInfo::default();
    apply_route_identity_provenance_to_state(
        &mut sandbox,
        crate::db::ActivationRouteProvenance {
            current_route_generation: Some(RouteGeneration::new(7).unwrap()),
            current_execution_generation: Some(ExecutionGeneration::new(7).unwrap()),
            current_substrate_kind: Some(NodeManagedBy::Space),
            retained_route_generation: Some(RouteGeneration::new(9).unwrap()),
            retained_execution_generation: Some(ExecutionGeneration::new(9).unwrap()),
            retained_substrate_kind: Some(NodeManagedBy::Kubernetes),
        },
    );

    assert_eq!(
        sandbox.route_provenance_substrate_kind,
        crate::proto::SandboxSubstrateKind::Kubernetes as i32
    );
}

#[test]
fn partial_direct_provenance_publishes_a_conservative_common_floor() {
    let mut sandbox = SandboxInfo::default();
    apply_route_identity_provenance_to_state(
        &mut sandbox,
        crate::db::ActivationRouteProvenance {
            retained_route_generation: Some(RouteGeneration::new(7).unwrap()),
            retained_substrate_kind: Some(NodeManagedBy::Space),
            ..Default::default()
        },
    );

    assert_eq!(sandbox.route_provenance_generation, Some(7));
    assert_eq!(sandbox.route_provenance_execution_generation, Some(7));
    assert_eq!(
        sandbox.route_provenance_substrate_kind,
        crate::proto::SandboxSubstrateKind::Direct as i32
    );
}

#[tokio::test]
async fn full_snapshot_capture_serializes_resume_on_wake_lock() {
    let coordinator = AutoLifecycleCoordinator::default();
    let sandbox_id = "sandbox-full-snapshot-race";
    let (_wake_guard, _artifact_guard) = coordinator
        .artifact_publication_locks(
            sandbox_id,
            pplx_space_common_core::types::ArtifactKind::Full,
        )
        .await;

    let (resumed_tx, mut resumed_rx) = oneshot::channel();
    let resume_lock = coordinator.wake_lock(sandbox_id);
    let resume_task = tokio::spawn(async move {
        let _guard = resume_lock.lock().await;
        let _ = resumed_tx.send(());
    });

    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(20), &mut resumed_rx)
            .await
            .is_err(),
        "resume must remain behind full snapshot publication while memory is captured"
    );
    drop(_artifact_guard);
    drop(_wake_guard);
    tokio::time::timeout(std::time::Duration::from_secs(1), resumed_rx)
        .await
        .expect("resume must proceed after full snapshot publication releases its locks")
        .unwrap();
    resume_task.await.unwrap();
}

#[tokio::test]
async fn candidate_snapshot_admission_uses_durable_execution_before_checkpointing() {
    use crate::service::tests::service_tests::insert_test_sandbox;
    use pplx_space_common_core::types::{
        Arch, Digest, EnvironmentReference, ExecutionMetadata, RuntimeIdentity, RuntimeKind,
        SandboxOwnerKind,
    };
    let conn = crate::db::test_conn();
    let id = SandboxId::new().to_string();
    insert_test_sandbox(&conn, &id, SandboxState::Paused);
    conn.execute(
        "UPDATE sandboxes SET user = ?1 WHERE id = ?2",
        crate::db::fixture::params![
            SandboxOwnerKind::TemplateManager
                .reserved_owner_label()
                .unwrap(),
            id
        ],
    )
    .unwrap();
    let execution = ExecutionMetadata::new(
        RuntimeIdentity {
            kind: RuntimeKind::Firecracker,
            version: "1.17.0".parse().unwrap(),
        },
        ExecutionMetadata::initial_firecracker(Arch::X86_64, vec![]).guest(),
        None,
        vec![],
    )
    .unwrap()
    .with_environment(EnvironmentReference::new(Digest::sha256([8; 32])));
    crate::db::upsert_sandbox_execution_metadata(&conn, &id, &execution).unwrap();
    // A regression must stop before filesystem staging or dialing a host service.
    conn.execute_batch("CREATE TEMP TRIGGER refuse_checkpoint BEFORE UPDATE OF state ON sandboxes WHEN NEW.state = 'checkpointing' BEGIN SELECT RAISE(ABORT, 'test checkpoint fence'); END;").unwrap();
    let service = SandboxManagerService::for_test(conn);
    let sandbox = service.lookup_sandbox(&id).unwrap();
    service
        .ensure_memory_state_supported(&sandbox, "snapshot")
        .expect_err("durable candidate evidence must win when metadata.json is absent");
    let error = service
        .create_template_base_snapshot(internal_request(CreateTemplateBaseSnapshotRequest {
            sandbox_id: id.clone(),
            vcpu: 1,
            memory_mib: 128,
        }))
        .await
        .unwrap_err();
    assert_eq!(error.code(), tonic::Code::FailedPrecondition);
    assert_eq!(
        service.lookup_sandbox(&id).unwrap().state,
        SandboxState::Paused
    );
}

fn sandbox_state(conn: &crate::db::Connection, id: &str) -> SandboxState {
    crate::db::get_sandbox(conn, id).unwrap().unwrap().state
}

fn sandbox_inventory_dirty(conn: &crate::db::Connection, id: &str) -> bool {
    crate::db::list_node_dirty_refs(conn, 1000)
        .unwrap()
        .iter()
        .any(|reference| {
            reference.element_kind == "sandbox_inventory" && reference.element_value == id
        })
}

/// The template base-snapshot completion and its failure rollback hold only
/// the wake lock, which Stop does not take. A Stop that completes while the
/// snapshot is captured or hashed leaves `stopped`, and neither write may
/// revive the row as `paused`.
#[test]
fn template_base_snapshot_writes_leave_a_stop_that_completed_during_capture_stopped() {
    for completion in [true, false] {
        let conn = crate::db::test_conn();
        let id = SandboxId::new().to_string();
        crate::service::tests::service_tests::insert_test_sandbox_with_runtime(
            &conn,
            &id,
            SandboxState::Paused,
        );
        begin_template_base_snapshot_state(&conn, &id).unwrap();
        assert_eq!(sandbox_state(&conn, &id), SandboxState::Checkpointing);
        crate::db::tests::state_write_fixtures::stop(&conn, &id);
        assert_eq!(sandbox_state(&conn, &id), SandboxState::Stopped);
        let before = crate::db::tests::database_rows(&conn);
        let db = crate::db::SharedConnection::new(conn);

        if completion {
            let error = complete_template_base_snapshot_state(&db.lock().unwrap(), &id)
                .expect_err("completion must not revive a stopped row");
            assert_eq!(error.code(), tonic::Code::Aborted);
            assert_eq!(
                error.message(),
                "complete template checkpoint: sandbox state write was superseded: sandbox is stopped"
            );
        } else {
            rollback_template_base_snapshot_state(&db, &id);
        }

        assert_eq!(
            crate::db::tests::database_rows(&db.lock().unwrap()),
            before,
            "completion={completion}"
        );
    }
}

/// Both writes return only a `checkpointing` row to `paused`. Every other row
/// keeps every table unchanged; the completion reports why to its caller. Only
/// the best-effort rollback marks the inventory dirty, and only when it writes.
/// Rows carry a failure reason, a receipt and backdated timestamps, so a write
/// is visible in every column it touches.
#[test]
fn template_base_snapshot_writes_return_only_a_checkpointing_row_to_paused() {
    for state in SandboxState::all() {
        let expected_error = match state {
            SandboxState::Checkpointing => None,
            // The generic guards: a stopping or cleanup row is never revived,
            // and a deleting row owns its `delete_row` cleanup.
            SandboxState::Stopping
            | SandboxState::Deleting
            | SandboxState::CleanupRequired
            | SandboxState::CleanupFailed => Some(
                "complete template checkpoint: sandbox state write was not applied: \
                 superseded_by_cleanup_episode"
                    .to_string(),
            ),
            SandboxState::Quarantined => Some(
                "complete template checkpoint: sandbox state write was not applied: \
                 blocked_by_quarantine"
                    .to_string(),
            ),
            SandboxState::Creating
            | SandboxState::Created
            | SandboxState::Starting
            | SandboxState::Started
            | SandboxState::Stopped
            | SandboxState::Pausing
            | SandboxState::Paused
            | SandboxState::Resuming
            | SandboxState::Suspending
            | SandboxState::Suspended
            | SandboxState::Restoring
            | SandboxState::Failed => Some(format!(
                "complete template checkpoint: sandbox state write was superseded: sandbox is {state}"
            )),
        };
        for completion in [true, false] {
            let conn = crate::db::test_conn();
            let id = SandboxId::new().to_string();
            crate::service::tests::service_tests::insert_test_sandbox_with_runtime(
                &conn, &id, state,
            );
            crate::db::tests::state_write_fixtures::decorate(&conn, &id, SandboxState::Started);
            let before = crate::db::tests::database_rows(&conn);
            let db = crate::db::SharedConnection::new(conn);

            let result = if completion {
                complete_template_base_snapshot_state(&db.lock().unwrap(), &id)
            } else {
                rollback_template_base_snapshot_state(&db, &id);
                Ok(())
            };

            let conn = db.lock().unwrap();
            match &expected_error {
                None => {
                    result.unwrap();
                    assert_eq!(sandbox_state(&conn, &id), SandboxState::Paused);
                    assert_eq!(
                        sandbox_inventory_dirty(&conn, &id),
                        !completion,
                        "completion={completion}"
                    );
                }
                Some(message) => {
                    if completion {
                        let error = result.expect_err("refused completion must fail");
                        assert_eq!(error.code(), tonic::Code::Aborted, "{state}");
                        assert_eq!(error.message(), message, "{state}");
                    }
                    assert_eq!(
                        crate::db::tests::database_rows(&conn),
                        before,
                        "{state}, completion={completion}"
                    );
                }
            }
        }
    }
}

/// Template base-snapshot admission reads `paused` and writes `checkpointing`
/// under the same connection guard, so it writes only over a `paused` row.
/// Every other row keeps every table unchanged and reports why.
#[test]
fn template_base_snapshot_admission_writes_only_over_a_paused_row() {
    for state in SandboxState::all() {
        let expected_error = match state {
            SandboxState::Paused => None,
            // The generic guards: a stopping or cleanup row is never revived,
            // and a deleting row owns its `delete_row` cleanup.
            SandboxState::Stopping
            | SandboxState::Deleting
            | SandboxState::CleanupRequired
            | SandboxState::CleanupFailed => Some(
                "begin template checkpoint: sandbox state write was not applied: \
                 superseded_by_cleanup_episode"
                    .to_string(),
            ),
            SandboxState::Quarantined => Some(
                "begin template checkpoint: sandbox state write was not applied: \
                 blocked_by_quarantine"
                    .to_string(),
            ),
            SandboxState::Creating
            | SandboxState::Created
            | SandboxState::Starting
            | SandboxState::Started
            | SandboxState::Stopped
            | SandboxState::Pausing
            | SandboxState::Resuming
            | SandboxState::Checkpointing
            | SandboxState::Suspending
            | SandboxState::Suspended
            | SandboxState::Restoring
            | SandboxState::Failed => Some(format!(
                "begin template checkpoint: sandbox state write was superseded: sandbox is {state}"
            )),
        };
        let conn = crate::db::test_conn();
        let id = SandboxId::new().to_string();
        crate::service::tests::service_tests::insert_test_sandbox_with_runtime(&conn, &id, state);
        let before = crate::db::tests::database_rows(&conn);

        let result = begin_template_base_snapshot_state(&conn, &id);

        match expected_error {
            None => {
                result.unwrap();
                assert_eq!(sandbox_state(&conn, &id), SandboxState::Checkpointing);
            }
            Some(message) => {
                let error = result.expect_err("a refused admission must fail");
                assert_eq!(error.code(), tonic::Code::Aborted, "{state}");
                assert_eq!(error.message(), message, "{state}");
                assert_eq!(crate::db::tests::database_rows(&conn), before, "{state}");
            }
        }
    }
}
