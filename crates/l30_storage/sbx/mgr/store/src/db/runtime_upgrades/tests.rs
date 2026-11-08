use super::*;
use crate::db::fixture::insert_test_sandbox;
use crate::db::test_fixtures::begin_activation;

struct Environments {
    source: ExecutionMetadata,
    target: ExecutionMetadata,
    target_reference: EnvironmentReference,
}

fn environments() -> Environments {
    let compiled = pplx_space_node_runtime_core::catalog::CompiledEnvironment::for_architecture(
        pplx_space_common_core::types::Arch::X86_64,
    )
    .unwrap();
    let source = compiled[0].definition().execution_requirements().unwrap();
    let target_definition = compiled
        .iter()
        .map(|environment| environment.definition())
        .find(|definition| definition.reference() != compiled[0].definition().reference())
        .unwrap();
    let target = target_definition.execution_requirements().unwrap();
    assert_eq!(target.environment(), Some(target_definition.reference()));
    Environments {
        source,
        target_reference: target_definition.reference(),
        target,
    }
}

fn authorization(memory_loss: bool) -> RuntimeUpgradeAuthorization {
    RuntimeUpgradeAuthorization {
        id: "block-e0".to_string(),
        reason: "runtime advisory".to_string(),
        memory_loss,
    }
}

fn operation() -> RuntimeUpgradeOperationId {
    RuntimeUpgradeOperationId::from_uuid(uuid::Uuid::now_v7())
}

fn sandbox(conn: &Connection, state: SandboxState, execution: &ExecutionMetadata) -> String {
    let id = SandboxId::new().to_string();
    insert_test_sandbox(conn, &id, state);
    upsert_sandbox_execution_metadata(conn, &id, execution).unwrap();
    id
}

fn begin(
    conn: &Connection,
    operation_id: RuntimeUpgradeOperationId,
    sandbox_id: &str,
    target: EnvironmentReference,
    memory_loss: bool,
) -> BeginRuntimeUpgrade {
    let row = get_sandbox(conn, sandbox_id).unwrap();
    let observed = row.as_ref().map_or(
        ObservedSandbox {
            state: SandboxState::Stopped.into(),
            current_local_artifact_id: None,
            execution: None,
        },
        ObservedSandbox::of,
    );
    begin_runtime_upgrade(
        conn,
        NewRuntimeUpgrade {
            operation_id,
            sandbox_id,
            observed,
            target_environment: target,
            target_state: RuntimeUpgradeTargetState::Stopped,
            authorization: &authorization(memory_loss),
            checkpoint_dir_identity: Some("7:42"),
            // An unbound sandbox's disk records the source environment.
            previous_execution: row
                .as_ref()
                .and_then(|row| row.execution_metadata.as_ref())
                .unwrap_or(&environments().source),
        },
    )
    .unwrap()
}

fn fenced(outcome: BeginRuntimeUpgrade) -> RuntimeUpgradeRecord {
    match outcome {
        BeginRuntimeUpgrade::Fenced(record) => *record,
        other => panic!("expected a fenced upgrade, got {other:?}"),
    }
}

fn pin_suspend_artifact(conn: &Connection, sandbox_id: &str) -> LocalArtifactId {
    let artifact = LocalArtifactId::new();
    insert_local_artifact(
        conn,
        sandbox_id,
        artifact,
        ArtifactKind::Full,
        ArtifactOrigin::Suspend,
        None,
        "/space/cache/test/snapshot",
    )
    .unwrap();
    conn.execute(
        "UPDATE sandboxes
            SET current_local_artifact_id = ?1,
                current_local_artifact_kind = 'full',
                current_local_artifact_origin = 'suspend'
          WHERE id = ?2",
        params![artifact.to_string(), sandbox_id],
    )
    .unwrap();
    artifact
}

#[test]
fn begin_fences_a_suspended_sandbox_and_records_the_pair_it_keeps() {
    let conn = test_conn();
    let envs = environments();
    let id = sandbox(&conn, SandboxState::Suspended, &envs.source);
    let artifact = pin_suspend_artifact(&conn, &id);
    let operation_id = operation();

    let record = fenced(begin(&conn, operation_id, &id, envs.target_reference, true));

    assert_eq!(record.operation_id, operation_id);
    assert_eq!(record.sandbox_id, id);
    assert_eq!(record.phase, RuntimeUpgradePhase::Fenced);
    assert_eq!(record.source_state, SandboxState::Suspended);
    assert_eq!(record.previous_execution, envs.source);
    assert_eq!(record.checkpoint_artifact_id, Some(artifact));
    assert_eq!(record.checkpoint_dir_identity.as_deref(), Some("7:42"));
    assert_eq!(record.target_environment, envs.target_reference);
    assert_eq!(record.target_state, RuntimeUpgradeTargetState::Stopped);
    assert_eq!(record.authorization, authorization(true));
    assert_eq!(
        crate::db::read::runtime_upgrade_fence(&conn, &id).unwrap(),
        Some(operation_id)
    );
    // Fencing changes nothing on the sandbox itself.
    let row = get_sandbox(&conn, &id).unwrap().unwrap();
    assert_eq!(row.state, SandboxStatus::from(SandboxState::Suspended));
    assert_eq!(row.execution_metadata, Some(envs.source));
    assert_eq!(row.current_local_artifact_id, Some(artifact));
}

#[test]
fn begin_fences_a_stopped_sandbox_without_memory_loss_consent() {
    let conn = test_conn();
    let envs = environments();
    let id = sandbox(&conn, SandboxState::Stopped, &envs.source);

    let record = fenced(begin(&conn, operation(), &id, envs.target_reference, false));

    assert_eq!(record.source_state, SandboxState::Stopped);
    assert_eq!(record.checkpoint_artifact_id, None);
}

#[test]
fn begin_refuses_everything_that_already_owns_the_sandbox() {
    let envs = environments();
    for state in [
        SandboxState::Created,
        SandboxState::Started,
        SandboxState::Paused,
        SandboxState::Starting,
        SandboxState::Restoring,
        SandboxState::Suspending,
        SandboxState::Deleting,
        SandboxState::Quarantined,
    ] {
        let conn = test_conn();
        let id = sandbox(&conn, state, &envs.source);
        assert_eq!(
            begin(&conn, operation(), &id, envs.target_reference, true),
            BeginRuntimeUpgrade::UnsupportedState(state.into()),
            "{state}"
        );
        assert_eq!(unsettled_runtime_upgrades(&conn).unwrap(), vec![]);
    }

    let conn = test_conn();
    assert_eq!(
        begin(
            &conn,
            operation(),
            &SandboxId::new().to_string(),
            envs.target_reference,
            true
        ),
        BeginRuntimeUpgrade::SandboxMissing
    );

    // A suspended sandbox loses its memory; only an authorization that
    // accepts that may fence it.
    let suspended = sandbox(&conn, SandboxState::Suspended, &envs.source);
    assert_eq!(
        begin(&conn, operation(), &suspended, envs.target_reference, false),
        BeginRuntimeUpgrade::MemoryLossUnauthorized
    );

    let cleaning = sandbox(&conn, SandboxState::Stopped, &envs.source);
    conn.execute(
        "UPDATE sandboxes SET cleanup_completion = ?1, cleanup_keep_filesystem = 1 WHERE id = ?2",
        params![CleanupCompletion::MarkStopped.as_str(), cleaning],
    )
    .unwrap();
    assert_eq!(
        begin(&conn, operation(), &cleaning, envs.target_reference, true),
        BeginRuntimeUpgrade::CleanupActive
    );

    let owned = sandbox(&conn, SandboxState::Stopped, &envs.source);
    let first = operation();
    fenced(begin(&conn, first, &owned, envs.target_reference, true));
    assert_eq!(
        begin(&conn, operation(), &owned, envs.target_reference, true),
        BeginRuntimeUpgrade::OtherOperation(first)
    );
    assert_eq!(
        begin(&conn, first, &owned, envs.target_reference, true),
        BeginRuntimeUpgrade::OperationExists
    );
    assert_eq!(unsettled_runtime_upgrades(&conn).unwrap().len(), 1);
}

#[test]
fn the_fence_refuses_a_concurrent_resume_and_cold_start_without_touching_the_row() {
    let conn = test_conn();
    let envs = environments();
    let id = sandbox(&conn, SandboxState::Suspended, &envs.source);
    pin_suspend_artifact(&conn, &id);
    let operation_id = operation();
    fenced(begin(&conn, operation_id, &id, envs.target_reference, true));
    let before = get_sandbox(&conn, &id).unwrap().unwrap();

    for phase in [
        RuntimeUpgradePhase::Fenced,
        RuntimeUpgradePhase::Preserved,
        RuntimeUpgradePhase::Booting,
    ] {
        if phase == RuntimeUpgradePhase::Preserved {
            assert!(
                advance_runtime_upgrade(&conn, operation_id, RuntimeUpgradePhase::Fenced, phase)
                    .unwrap()
            );
        }
        if phase == RuntimeUpgradePhase::Booting {
            assert!(bind_runtime_upgrade_consumer(&conn, operation_id, &envs.target).unwrap());
        }
        for kind in [ActivationKind::Restore, ActivationKind::ColdStart] {
            let refused = begin_activation(&conn, &id, SandboxState::Suspended, kind, None, None);
            assert!(
                matches!(
                    refused,
                    Err(BeginActivationError::RuntimeUpgrade(crate::db::RuntimeUpgradeFenceError::Fenced { operation_id: owner }))
                        if owner == operation_id
                ),
                "{phase} {kind:?}: {refused:?}"
            );
        }
        let after = get_sandbox(&conn, &id).unwrap().unwrap();
        assert_eq!(after.state, SandboxStatus::from(SandboxState::Suspended));
        assert_eq!(after.cleanup_completion, None);
        assert_eq!(after.node_execution_id, before.node_execution_id);
        assert!(cleanup_episode(&conn, &id).unwrap().is_none());
    }
}

#[test]
fn only_the_fencing_upgrade_in_its_booting_phase_passes_its_own_fence() {
    let conn = test_conn();
    let envs = environments();
    let id = sandbox(&conn, SandboxState::Stopped, &envs.source);
    let operation_id = operation();
    fenced(begin(
        &conn,
        operation_id,
        &id,
        envs.target_reference,
        false,
    ));
    let claim = |claimed| {
        begin_activation_with_route_evidence(
            &conn,
            &id,
            SandboxState::Stopped,
            ActivationKind::ColdStart,
            None,
            None,
            ActivationRouteAdmission {
                runtime_upgrade: Some(claimed),
                ..ActivationRouteAdmission::default()
            },
        )
    };

    // Before the consumer is bound, not even the owner may boot.
    assert!(matches!(
        claim(operation_id),
        Err(BeginActivationError::RuntimeUpgrade(
            crate::db::RuntimeUpgradeFenceError::Fenced { .. }
        ))
    ));
    assert!(
        advance_runtime_upgrade(
            &conn,
            operation_id,
            RuntimeUpgradePhase::Fenced,
            RuntimeUpgradePhase::Preserved
        )
        .unwrap()
    );
    assert!(matches!(
        claim(operation_id),
        Err(BeginActivationError::RuntimeUpgrade(
            crate::db::RuntimeUpgradeFenceError::Fenced { .. }
        ))
    ));
    assert!(bind_runtime_upgrade_consumer(&conn, operation_id, &envs.target).unwrap());
    // Another upgrade's identity never passes.
    assert!(matches!(
        claim(operation()),
        Err(BeginActivationError::RuntimeUpgrade(
            crate::db::RuntimeUpgradeFenceError::Fenced { .. }
        ))
    ));

    let episode = claim(operation_id).unwrap().unwrap();
    assert_eq!(episode.completion, CleanupCompletion::MarkStopped);
    assert_eq!(
        get_sandbox(&conn, &id).unwrap().unwrap().state,
        SandboxStatus::from(SandboxState::Starting)
    );
}

#[test]
fn an_upgrade_activation_without_a_fence_is_refused() {
    let conn = test_conn();
    let envs = environments();
    let id = sandbox(&conn, SandboxState::Stopped, &envs.source);
    let stale = operation();
    let refused = begin_activation_with_route_evidence(
        &conn,
        &id,
        SandboxState::Stopped,
        ActivationKind::ColdStart,
        None,
        None,
        ActivationRouteAdmission {
            runtime_upgrade: Some(stale),
            ..ActivationRouteAdmission::default()
        },
    );
    assert!(matches!(
        refused,
        Err(BeginActivationError::RuntimeUpgrade(crate::db::RuntimeUpgradeFenceError::NotFencing { operation_id })) if operation_id == stale
    ));
    assert_eq!(
        get_sandbox(&conn, &id).unwrap().unwrap().state,
        SandboxStatus::from(SandboxState::Stopped)
    );
}

#[test]
fn phases_move_only_along_their_edges() {
    use RuntimeUpgradePhase::*;
    let all = [Fenced, Preserved, Booting, Verified, RolledBack, Retired];
    let forward = [
        (Fenced, Preserved),
        (Preserved, Booting),
        (Booting, Verified),
        (Verified, Retired),
    ];
    for from in all {
        for to in all {
            let conn = test_conn();
            let envs = environments();
            let id = sandbox(&conn, SandboxState::Stopped, &envs.source);
            let operation_id = operation();
            fenced(begin(
                &conn,
                operation_id,
                &id,
                envs.target_reference,
                false,
            ));
            conn.execute(
                "UPDATE sandbox_runtime_upgrades SET phase = ?1",
                params![from.as_str()],
            )
            .unwrap();
            let result = advance_runtime_upgrade(&conn, operation_id, from, to);
            if forward.contains(&(from, to)) {
                assert!(result.unwrap(), "{from} -> {to}");
                // A stale writer that observed the old phase loses.
                assert!(!advance_runtime_upgrade(&conn, operation_id, from, to).unwrap());
            } else {
                assert!(
                    matches!(result, Err(ref error) if matches!(error.raw(), rusqlite::Error::InvalidQuery)),
                    "{from} -> {to} must not be an edge"
                );
                assert_eq!(
                    runtime_upgrade(&conn, operation_id).unwrap().unwrap().phase,
                    from
                );
            }
        }
    }
}

#[test]
fn retirement_waits_for_a_verified_boot() {
    let conn = test_conn();
    let envs = environments();
    let id = sandbox(&conn, SandboxState::Suspended, &envs.source);
    pin_suspend_artifact(&conn, &id);
    let operation_id = operation();
    fenced(begin(&conn, operation_id, &id, envs.target_reference, true));
    for phase in [
        RuntimeUpgradePhase::Fenced,
        RuntimeUpgradePhase::Preserved,
        RuntimeUpgradePhase::Booting,
    ] {
        let refused =
            advance_runtime_upgrade(&conn, operation_id, phase, RuntimeUpgradePhase::Retired);
        assert!(
            matches!(refused, Err(ref error) if matches!(error.raw(), rusqlite::Error::InvalidQuery)),
            "{phase} must not retire"
        );
        match phase {
            RuntimeUpgradePhase::Fenced => assert!(
                advance_runtime_upgrade(&conn, operation_id, phase, RuntimeUpgradePhase::Preserved)
                    .unwrap()
            ),
            RuntimeUpgradePhase::Preserved => {
                assert!(bind_runtime_upgrade_consumer(&conn, operation_id, &envs.target).unwrap())
            }
            _ => {}
        }
    }
    // Retiring from the observed verified phase is refused while the row is
    // still booting.
    assert!(
        !advance_runtime_upgrade(
            &conn,
            operation_id,
            RuntimeUpgradePhase::Verified,
            RuntimeUpgradePhase::Retired
        )
        .unwrap()
    );
    assert!(
        advance_runtime_upgrade(
            &conn,
            operation_id,
            RuntimeUpgradePhase::Booting,
            RuntimeUpgradePhase::Verified
        )
        .unwrap()
    );
    // A verified boot can no longer be rolled back onto the old pair.
    assert!(!roll_back_runtime_upgrade(&conn, operation_id, "late failure").unwrap());
    assert_eq!(
        crate::db::read::runtime_upgrade_fence(&conn, &id).unwrap(),
        None,
        "a verified upgrade no longer fences activation"
    );
    assert!(
        advance_runtime_upgrade(
            &conn,
            operation_id,
            RuntimeUpgradePhase::Verified,
            RuntimeUpgradePhase::Retired
        )
        .unwrap()
    );
    assert_eq!(unsettled_runtime_upgrades(&conn).unwrap(), vec![]);
    assert_eq!(
        runtime_upgrade(&conn, operation_id).unwrap().unwrap().phase,
        RuntimeUpgradePhase::Retired
    );
}

#[test]
fn binding_the_consumer_and_booting_commit_together() {
    let conn = test_conn();
    let envs = environments();
    let id = sandbox(&conn, SandboxState::Stopped, &envs.source);
    let operation_id = operation();
    fenced(begin(
        &conn,
        operation_id,
        &id,
        envs.target_reference,
        false,
    ));

    // Not before the disk is preserved.
    assert!(!bind_runtime_upgrade_consumer(&conn, operation_id, &envs.target).unwrap());
    assert!(
        advance_runtime_upgrade(
            &conn,
            operation_id,
            RuntimeUpgradePhase::Fenced,
            RuntimeUpgradePhase::Preserved
        )
        .unwrap()
    );
    // Never a consumer other than the row's target.
    assert!(!bind_runtime_upgrade_consumer(&conn, operation_id, &envs.source).unwrap());
    assert_eq!(
        get_sandbox(&conn, &id).unwrap().unwrap().execution_metadata,
        Some(envs.source.clone())
    );
    assert_eq!(
        runtime_upgrade(&conn, operation_id).unwrap().unwrap().phase,
        RuntimeUpgradePhase::Preserved
    );

    // A refused write leaves both rows unchanged.
    conn.execute_batch(
        "CREATE TEMP TRIGGER refuse_booting BEFORE UPDATE OF phase ON sandbox_runtime_upgrades
         BEGIN SELECT RAISE(ABORT, 'scripted phase refusal'); END;",
    )
    .unwrap();
    bind_runtime_upgrade_consumer(&conn, operation_id, &envs.target).unwrap_err();
    assert_eq!(
        get_sandbox(&conn, &id).unwrap().unwrap().execution_metadata,
        Some(envs.source.clone())
    );
    conn.execute_batch("DROP TRIGGER refuse_booting").unwrap();

    assert!(bind_runtime_upgrade_consumer(&conn, operation_id, &envs.target).unwrap());
    assert_eq!(
        get_sandbox(&conn, &id).unwrap().unwrap().execution_metadata,
        Some(envs.target)
    );
    assert_eq!(
        runtime_upgrade(&conn, operation_id).unwrap().unwrap().phase,
        RuntimeUpgradePhase::Booting
    );
}

#[test]
fn a_rollback_restores_the_previous_consumer_and_releases_the_fence() {
    let conn = test_conn();
    let envs = environments();
    let id = sandbox(&conn, SandboxState::Suspended, &envs.source);
    let operation_id = operation();
    fenced(begin(&conn, operation_id, &id, envs.target_reference, true));
    assert!(
        advance_runtime_upgrade(
            &conn,
            operation_id,
            RuntimeUpgradePhase::Fenced,
            RuntimeUpgradePhase::Preserved
        )
        .unwrap()
    );
    assert!(bind_runtime_upgrade_consumer(&conn, operation_id, &envs.target).unwrap());

    assert!(roll_back_runtime_upgrade(&conn, operation_id, "guest never became ready").unwrap());

    let record = runtime_upgrade(&conn, operation_id).unwrap().unwrap();
    assert_eq!(record.phase, RuntimeUpgradePhase::RolledBack);
    assert_eq!(record.failure.as_deref(), Some("guest never became ready"));
    assert_eq!(
        get_sandbox(&conn, &id).unwrap().unwrap().execution_metadata,
        Some(envs.source)
    );
    assert_eq!(
        crate::db::read::runtime_upgrade_fence(&conn, &id).unwrap(),
        None
    );
    // The old pair is restorable again.
    assert!(
        begin_activation(
            &conn,
            &id,
            SandboxState::Suspended,
            ActivationKind::Restore,
            None,
            None
        )
        .unwrap()
        .is_some()
    );
    // The row still owes the preserved disk, then goes.
    assert_eq!(unsettled_runtime_upgrades(&conn).unwrap(), vec![record]);
    assert!(!roll_back_runtime_upgrade(&conn, operation_id, "twice").unwrap());
    assert!(finish_runtime_upgrade_rollback(&conn, operation_id).unwrap());
    assert!(!finish_runtime_upgrade_rollback(&conn, operation_id).unwrap());
    assert_eq!(runtime_upgrade(&conn, operation_id).unwrap(), None);
}

#[test]
fn a_rollback_of_a_legacy_unbound_sandbox_binds_the_execution_its_disk_recorded() {
    let conn = test_conn();
    let envs = environments();
    let id = SandboxId::new().to_string();
    insert_test_sandbox(&conn, &id, SandboxState::Stopped);
    let operation_id = operation();
    let record = fenced(begin(
        &conn,
        operation_id,
        &id,
        envs.target_reference,
        false,
    ));
    assert_eq!(record.previous_execution, envs.source);
    assert!(
        advance_runtime_upgrade(
            &conn,
            operation_id,
            RuntimeUpgradePhase::Fenced,
            RuntimeUpgradePhase::Preserved
        )
        .unwrap()
    );
    assert!(bind_runtime_upgrade_consumer(&conn, operation_id, &envs.target).unwrap());

    assert!(roll_back_runtime_upgrade(&conn, operation_id, "boot failed").unwrap());

    // Not unbound again: the boot rewrote the disk's record to the target,
    // which the next cold start would otherwise bind.
    assert_eq!(
        get_sandbox(&conn, &id).unwrap().unwrap().execution_metadata,
        Some(envs.source)
    );
}

#[test]
fn deleting_the_sandbox_deletes_its_upgrades() {
    let conn = test_conn();
    let envs = environments();
    let id = sandbox(&conn, SandboxState::Stopped, &envs.source);
    let operation_id = operation();
    fenced(begin(
        &conn,
        operation_id,
        &id,
        envs.target_reference,
        false,
    ));

    delete_sandbox(&conn, &id).unwrap();

    assert_eq!(runtime_upgrade(&conn, operation_id).unwrap(), None);
    assert_eq!(unsettled_runtime_upgrades(&conn).unwrap(), vec![]);
}

#[test]
fn begin_refuses_a_sandbox_that_changed_since_it_was_inspected() {
    let envs = environments();
    for change in ["state", "pin", "consumer"] {
        let conn = test_conn();
        let id = sandbox(&conn, SandboxState::Suspended, &envs.source);
        let inspected = get_sandbox(&conn, &id).unwrap().unwrap();
        match change {
            // Restored, then suspended again before the fence.
            "pin" => {
                pin_suspend_artifact(&conn, &id);
            }
            "state" => {
                conn.execute(
                    "UPDATE sandboxes SET state = 'stopped' WHERE id = ?1",
                    params![id],
                )
                .unwrap();
            }
            _ => upsert_sandbox_execution_metadata(&conn, &id, &envs.target).unwrap(),
        }
        let outcome = begin_runtime_upgrade(
            &conn,
            NewRuntimeUpgrade {
                operation_id: operation(),
                sandbox_id: &id,
                observed: ObservedSandbox::of(&inspected),
                target_environment: envs.target_reference,
                target_state: RuntimeUpgradeTargetState::Started,
                authorization: &authorization(true),
                checkpoint_dir_identity: None,
                previous_execution: &envs.source,
            },
        )
        .unwrap();
        assert_eq!(outcome, BeginRuntimeUpgrade::SandboxChanged, "{change}");
        assert_eq!(unsettled_runtime_upgrades(&conn).unwrap(), vec![]);
    }
}

#[test]
fn the_upgrade_boot_is_verified_in_the_transaction_that_starts_it() {
    let conn = test_conn();
    let envs = environments();
    let id = sandbox(&conn, SandboxState::Suspended, &envs.source);
    let other = sandbox(&conn, SandboxState::Stopped, &envs.source);
    let operation_id = operation();
    fenced(begin(&conn, operation_id, &id, envs.target_reference, true));
    let untouched = operation();
    fenced(begin(
        &conn,
        untouched,
        &other,
        envs.target_reference,
        false,
    ));
    assert!(
        advance_runtime_upgrade(
            &conn,
            operation_id,
            RuntimeUpgradePhase::Fenced,
            RuntimeUpgradePhase::Preserved
        )
        .unwrap()
    );
    assert!(bind_runtime_upgrade_consumer(&conn, operation_id, &envs.target).unwrap());
    let episode = begin_activation_with_route_evidence(
        &conn,
        &id,
        SandboxState::Suspended,
        ActivationKind::ColdStart,
        None,
        None,
        ActivationRouteAdmission {
            runtime_upgrade: Some(operation_id),
            ..ActivationRouteAdmission::default()
        },
    )
    .unwrap()
    .unwrap();
    assert_eq!(
        runtime_upgrade(&conn, operation_id).unwrap().unwrap().phase,
        RuntimeUpgradePhase::Booting
    );

    assert!(mark_start_succeeded(&conn, &id, Some(episode.id)).unwrap());

    assert_eq!(
        runtime_upgrade(&conn, operation_id).unwrap().unwrap().phase,
        RuntimeUpgradePhase::Verified
    );
    // A later stop or suspend can no longer make the boot look interrupted.
    assert_eq!(
        crate::db::read::runtime_upgrade_fence(&conn, &id).unwrap(),
        None
    );
    // Another sandbox's upgrade is untouched.
    assert_eq!(
        runtime_upgrade(&conn, untouched).unwrap().unwrap().phase,
        RuntimeUpgradePhase::Fenced
    );
}

#[test]
fn a_row_that_does_not_decode_is_skipped_but_keeps_its_fence() {
    let conn = test_conn();
    let envs = environments();
    let broken = sandbox(&conn, SandboxState::Stopped, &envs.source);
    let healthy = sandbox(&conn, SandboxState::Stopped, &envs.source);
    let broken_operation = operation();
    fenced(begin(
        &conn,
        broken_operation,
        &broken,
        envs.target_reference,
        false,
    ));
    let healthy_operation = operation();
    fenced(begin(
        &conn,
        healthy_operation,
        &healthy,
        envs.target_reference,
        false,
    ));
    conn.execute(
        "UPDATE sandbox_runtime_upgrades SET target_state = 'sideways' WHERE operation_id = ?1",
        params![broken_operation.to_string()],
    )
    .unwrap();

    let unsettled = unsettled_runtime_upgrades(&conn).unwrap();

    assert_eq!(
        unsettled
            .iter()
            .map(|record| record.operation_id)
            .collect::<Vec<_>>(),
        [healthy_operation]
    );
    assert_eq!(
        crate::db::read::runtime_upgrade_fence(&conn, &broken).unwrap(),
        Some(broken_operation)
    );
}

fn report(
    conn: &Connection,
    operation_id: RuntimeUpgradeOperationId,
) -> Option<RuntimeUpgradeStatus> {
    crate::db::read::runtime_upgrade_report(conn, operation_id)
        .unwrap()
        .map(|report| report.status)
}

#[test]
fn the_report_follows_an_upgrade_through_every_phase_to_upgraded() {
    let conn = test_conn();
    let envs = environments();
    let id = sandbox(&conn, SandboxState::Stopped, &envs.source);
    let operation_id = operation();
    assert_eq!(report(&conn, operation_id), None);

    fenced(begin(
        &conn,
        operation_id,
        &id,
        envs.target_reference,
        false,
    ));
    let in_progress = |phase| {
        assert_eq!(
            report(&conn, operation_id),
            Some(RuntimeUpgradeStatus::InProgress(phase))
        );
    };
    let advance = |from, to| {
        assert!(advance_runtime_upgrade(&conn, operation_id, from, to).unwrap());
    };
    in_progress(RuntimeUpgradePhase::Fenced);
    advance(RuntimeUpgradePhase::Fenced, RuntimeUpgradePhase::Preserved);
    in_progress(RuntimeUpgradePhase::Preserved);
    assert!(bind_runtime_upgrade_consumer(&conn, operation_id, &envs.target).unwrap());
    in_progress(RuntimeUpgradePhase::Booting);
    advance(RuntimeUpgradePhase::Booting, RuntimeUpgradePhase::Verified);
    in_progress(RuntimeUpgradePhase::Verified);
    advance(RuntimeUpgradePhase::Verified, RuntimeUpgradePhase::Retired);
    let upgraded = crate::db::read::runtime_upgrade_report(&conn, operation_id)
        .unwrap()
        .unwrap();
    assert_eq!(upgraded.sandbox_id, id);
    assert_eq!(upgraded.status, RuntimeUpgradeStatus::Upgraded);
}

#[test]
fn a_finished_rollback_keeps_its_failure_as_the_operations_outcome() {
    let conn = test_conn();
    let envs = environments();
    let id = sandbox(&conn, SandboxState::Stopped, &envs.source);
    let operation_id = operation();
    fenced(begin(
        &conn,
        operation_id,
        &id,
        envs.target_reference,
        false,
    ));
    assert!(roll_back_runtime_upgrade(&conn, operation_id, "guest never became ready").unwrap());
    let rolled_back = Some(RuntimeUpgradeStatus::RolledBack {
        failure: Some("guest never became ready".to_string()),
    });
    // Still owing the preserved copy, and after the row is gone.
    assert_eq!(report(&conn, operation_id), rolled_back);
    assert!(finish_runtime_upgrade_rollback(&conn, operation_id).unwrap());
    assert_eq!(runtime_upgrade(&conn, operation_id).unwrap(), None);
    assert_eq!(report(&conn, operation_id), rolled_back);

    // A retry starts over under the same operation, and its row wins.
    fenced(begin(
        &conn,
        operation_id,
        &id,
        envs.target_reference,
        false,
    ));
    assert_eq!(
        report(&conn, operation_id),
        Some(RuntimeUpgradeStatus::InProgress(
            RuntimeUpgradePhase::Fenced
        ))
    );
    assert!(roll_back_runtime_upgrade(&conn, operation_id, "second failure").unwrap());
    assert!(finish_runtime_upgrade_rollback(&conn, operation_id).unwrap());
    assert_eq!(
        report(&conn, operation_id),
        Some(RuntimeUpgradeStatus::RolledBack {
            failure: Some("second failure".to_string()),
        })
    );
}

#[test]
fn an_upgrade_that_found_its_target_reports_it_for_its_own_sandbox_only() {
    let conn = test_conn();
    let envs = environments();
    let id = sandbox(&conn, SandboxState::Stopped, &envs.source);
    let other = sandbox(&conn, SandboxState::Stopped, &envs.source);
    let operation_id = operation();

    assert!(
        record_runtime_upgrade_already_on_target(&conn, &on_target(operation_id, &id)).unwrap()
    );
    assert!(
        record_runtime_upgrade_already_on_target(&conn, &on_target(operation_id, &id)).unwrap()
    );
    // The same operation asking for another lifecycle records nothing.
    let mut started = on_target(operation_id, &id);
    started.target_state = RuntimeUpgradeTargetState::Started;
    assert!(!record_runtime_upgrade_already_on_target(&conn, &started).unwrap());
    let found = crate::db::read::runtime_upgrade_report(&conn, operation_id)
        .unwrap()
        .unwrap();
    assert_eq!(found.sandbox_id, id);
    assert_eq!(
        found.status,
        RuntimeUpgradeStatus::AlreadyOnTarget { activation: None }
    );

    // Another sandbox's outcome under the same operation id never rewrites
    // this one's.
    let rolled_back = operation();
    fenced(begin(&conn, rolled_back, &id, envs.target_reference, false));
    assert!(roll_back_runtime_upgrade(&conn, rolled_back, "boot failed").unwrap());
    assert!(finish_runtime_upgrade_rollback(&conn, rolled_back).unwrap());
    assert!(
        !record_runtime_upgrade_already_on_target(&conn, &on_target(rolled_back, &other)).unwrap()
    );
    let kept = crate::db::read::runtime_upgrade_report(&conn, rolled_back)
        .unwrap()
        .unwrap();
    assert_eq!(kept.sandbox_id, id);
    assert_eq!(
        kept.status,
        RuntimeUpgradeStatus::RolledBack {
            failure: Some("boot failed".to_string())
        }
    );
}

#[test]
fn deleting_the_sandbox_deletes_its_recorded_outcomes() {
    let conn = test_conn();
    let envs = environments();
    let id = sandbox(&conn, SandboxState::Stopped, &envs.source);
    let operation_id = operation();
    assert!(
        record_runtime_upgrade_already_on_target(&conn, &on_target(operation_id, &id)).unwrap()
    );

    delete_sandbox(&conn, &id).unwrap();

    assert_eq!(report(&conn, operation_id), None);
}

#[test]
fn an_activation_is_claimed_once_on_its_already_on_target_outcome() {
    let conn = test_conn();
    let envs = environments();
    let id = sandbox(&conn, SandboxState::Stopped, &envs.source);
    let other = sandbox(&conn, SandboxState::Stopped, &envs.source);
    let operation_id = operation();
    // Nothing to mark before the outcome exists, or for another sandbox.
    assert!(!claim_runtime_upgrade_activation(&conn, operation_id, &id, started()).unwrap());
    assert!(
        record_runtime_upgrade_already_on_target(&conn, &on_target(operation_id, &id)).unwrap()
    );
    assert!(!claim_runtime_upgrade_activation(&conn, operation_id, &other, started()).unwrap());

    assert!(claim_runtime_upgrade_activation(&conn, operation_id, &id, started()).unwrap());
    // Once.
    assert!(!claim_runtime_upgrade_activation(&conn, operation_id, &id, started()).unwrap());
    // A replay records the outcome again and keeps the activation.
    assert!(
        record_runtime_upgrade_already_on_target(&conn, &on_target(operation_id, &id)).unwrap()
    );
    assert_eq!(
        report(&conn, operation_id),
        Some(RuntimeUpgradeStatus::AlreadyOnTarget {
            activation: Some(started())
        })
    );
}

/// A claimed start of a sandbox that has never run.
fn started() -> RuntimeUpgradeActivationClaim {
    RuntimeUpgradeActivationClaim::Start {
        from_execution: None,
    }
}

#[test]
fn an_answer_with_a_running_execution_is_recorded_as_the_operations_activation() {
    let conn = test_conn();
    let envs = environments();
    let id = sandbox(&conn, SandboxState::Started, &envs.source);
    let operation_id = operation();
    assert!(
        record_runtime_upgrade_already_on_target(&conn, &on_target(operation_id, &id)).unwrap()
    );

    assert!(
        claim_runtime_upgrade_activation(
            &conn,
            operation_id,
            &id,
            RuntimeUpgradeActivationClaim::Attach
        )
        .unwrap()
    );

    // It is the activation: no start can be claimed after it.
    assert!(!claim_runtime_upgrade_activation(&conn, operation_id, &id, started()).unwrap());
    assert_eq!(
        report(&conn, operation_id),
        Some(RuntimeUpgradeStatus::AlreadyOnTarget {
            activation: Some(RuntimeUpgradeActivationClaim::Attach)
        })
    );
}

/// What an already-on-target operation of `sandbox_id` asked for.
fn on_target(
    operation_id: RuntimeUpgradeOperationId,
    sandbox_id: &str,
) -> RuntimeUpgradeRequestIdentity {
    RuntimeUpgradeRequestIdentity {
        operation_id,
        sandbox_id: sandbox_id.to_string(),
        target_environment: environments().target_reference,
        target_state: RuntimeUpgradeTargetState::Stopped,
        authorization_id: authorization(false).id,
    }
}

#[test]
fn a_recorded_outcome_keeps_what_its_operation_asked_for() {
    let conn = test_conn();
    let envs = environments();
    let id = sandbox(&conn, SandboxState::Stopped, &envs.source);
    let found = operation();
    assert_eq!(
        recorded_runtime_upgrade_request(&conn, found).unwrap(),
        None
    );
    assert!(record_runtime_upgrade_already_on_target(&conn, &on_target(found, &id)).unwrap());
    assert_eq!(
        recorded_runtime_upgrade_request(&conn, found).unwrap(),
        Some(on_target(found, &id))
    );

    // A rollback keeps its row's request, which `begin` wrote.
    let rolled_back = operation();
    let record = fenced(begin(&conn, rolled_back, &id, envs.target_reference, false));
    assert!(roll_back_runtime_upgrade(&conn, rolled_back, "boot failed").unwrap());
    assert!(finish_runtime_upgrade_rollback(&conn, rolled_back).unwrap());
    assert_eq!(
        recorded_runtime_upgrade_request(&conn, rolled_back).unwrap(),
        Some(record.identity())
    );
}

#[test]
fn an_operation_id_names_one_sandbox_across_both_tables() {
    let conn = test_conn();
    let envs = environments();
    let id = sandbox(&conn, SandboxState::Stopped, &envs.source);
    let other = sandbox(&conn, SandboxState::Stopped, &envs.source);

    // Recorded as an outcome for one sandbox, it can't fence another.
    let found = operation();
    assert!(record_runtime_upgrade_already_on_target(&conn, &on_target(found, &id)).unwrap());
    assert_eq!(
        begin(&conn, found, &other, envs.target_reference, false),
        BeginRuntimeUpgrade::OperationNamesAnother
    );

    // Fencing one sandbox, it can't be recorded for another.
    let fencing = operation();
    fenced(begin(&conn, fencing, &id, envs.target_reference, false));
    assert!(!record_runtime_upgrade_already_on_target(&conn, &on_target(fencing, &other)).unwrap());
    assert_eq!(
        recorded_runtime_upgrade_request(&conn, fencing).unwrap(),
        None
    );
}

#[test]
fn an_activation_decided_on_an_execution_refuses_a_sandbox_moved_off_it() {
    let conn = test_conn();
    let envs = environments();
    let id = sandbox(&conn, SandboxState::Stopped, &envs.source);
    let current = get_sandbox(&conn, &id).unwrap().unwrap().node_execution_id;
    let admission = |expected| ActivationRouteAdmission {
        expected_node_execution: Some(expected),
        ..ActivationRouteAdmission::default()
    };

    let moved = begin_activation_with_route_evidence(
        &conn,
        &id,
        SandboxState::Stopped,
        ActivationKind::ColdStart,
        None,
        None,
        admission(Some(SandboxActivationAttemptId::new())),
    );
    assert!(
        matches!(moved, Err(BeginActivationError::ExecutionChanged)),
        "{moved:?}"
    );
    assert_eq!(
        get_sandbox(&conn, &id).unwrap().unwrap().state,
        SandboxStatus::from(SandboxState::Stopped)
    );

    let claimed = begin_activation_with_route_evidence(
        &conn,
        &id,
        SandboxState::Stopped,
        ActivationKind::ColdStart,
        None,
        None,
        admission(current),
    );
    assert!(claimed.is_ok(), "{claimed:?}");
    assert_eq!(
        get_sandbox(&conn, &id).unwrap().unwrap().state,
        SandboxStatus::from(SandboxState::Starting)
    );
}
