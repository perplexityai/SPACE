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
    let attempt_id = new_runtime_upgrade_attempt_id();
    begin_for_attempt(
        conn,
        operation_id,
        sandbox_id,
        target,
        memory_loss,
        &attempt_id,
    )
}

/// [`begin`] as the attempt `attempt_id`.
fn begin_for_attempt(
    conn: &Connection,
    operation_id: RuntimeUpgradeOperationId,
    sandbox_id: &str,
    target: EnvironmentReference,
    memory_loss: bool,
    attempt_id: &str,
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
            attempt_id,
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

/// A stay-stopped upgrade awaits its stop from its fence, whatever the
/// sandbox's state, and once verified only while its boot's execution is the
/// sandbox's current one and runs: a stop, which keeps the execution's id but
/// retires it, ends it. An older unretired upgrade of the sandbox to another
/// target state never hides it.
#[test]
fn a_stay_stopped_upgrade_awaits_its_stop_only_while_its_boot_runs() {
    let conn = test_conn();
    let envs = environments();
    let id = sandbox(&conn, SandboxState::Stopped, &envs.source);
    let stay_stopped = fenced(begin(&conn, operation(), &id, envs.target_reference, false));
    let operation_id = stay_stopped.operation_id;
    let boot = uuid::Uuid::now_v7().to_string();
    // Created at the same instant, so the operation id breaks the tie and
    // the smallest one is the oldest.
    let older = RuntimeUpgradeOperationId::from_uuid(uuid::Uuid::from_u128(1));
    conn.execute_batch(&format!(
        "CREATE TEMP TABLE older_upgrade AS
             SELECT * FROM sandbox_runtime_upgrades WHERE operation_id = '{operation_id}';
         UPDATE older_upgrade
            SET operation_id = '{older}', target_state = 'started', phase = 'verified',
                boot_execution_id = '{boot}';
         INSERT INTO sandbox_runtime_upgrades SELECT * FROM older_upgrade;
         DROP TABLE older_upgrade;",
    ))
    .unwrap();
    let awaiting = |conn: &Connection| {
        crate::db::read::stay_stopped_runtime_upgrade_awaiting_its_stop(conn, &id).unwrap()
    };
    let set = |conn: &Connection, sql: &str| conn.execute_batch(sql).unwrap();
    assert_eq!(
        crate::db::read::unretired_runtime_upgrade(&conn, &id).unwrap(),
        Some(older),
        "the older upgrade is the sandbox's oldest unretired one"
    );

    for phase in ["fenced", "preserved", "booting"] {
        set(
            &conn,
            &format!(
                "UPDATE sandbox_runtime_upgrades SET phase = '{phase}'
                  WHERE operation_id = '{operation_id}'"
            ),
        );
        assert_eq!(
            awaiting(&conn),
            Some(operation_id),
            "{phase}: its boot may start before a suspend's capture"
        );
    }

    // The boot started and verified: its execution runs the sandbox.
    set(
        &conn,
        &format!(
            "UPDATE sandbox_runtime_upgrades SET phase = 'verified', boot_execution_id = '{boot}'
              WHERE operation_id = '{operation_id}';
             UPDATE sandboxes SET state = 'started', node_execution_id = '{boot}',
                                  node_execution_retired = 0
              WHERE id = '{id}';"
        ),
    );
    assert_eq!(awaiting(&conn), Some(operation_id));
    set(
        &conn,
        &format!("UPDATE sandboxes SET state = 'paused' WHERE id = '{id}'"),
    );
    assert_eq!(
        awaiting(&conn),
        Some(operation_id),
        "a paused boot is still the upgrade's to stop"
    );

    set(
        &conn,
        &format!(
            "UPDATE sandbox_runtime_upgrades SET phase = 'retired'
              WHERE operation_id = '{operation_id}'"
        ),
    );
    assert_eq!(awaiting(&conn), None, "a retired upgrade owes nothing");
    set(
        &conn,
        &format!(
            "UPDATE sandbox_runtime_upgrades SET phase = 'verified'
              WHERE operation_id = '{operation_id}'"
        ),
    );

    // A later execution runs the sandbox.
    set(
        &conn,
        &format!(
            "UPDATE sandboxes SET state = 'started', node_execution_id = '{later}' WHERE id = '{id}'",
            later = uuid::Uuid::now_v7(),
        ),
    );
    assert_eq!(
        awaiting(&conn),
        None,
        "a later execution is never the upgrade's to stop"
    );

    // A stop keeps the boot's execution id and retires it.
    set(
        &conn,
        &format!(
            "UPDATE sandboxes SET state = 'stopped', node_execution_id = '{boot}',
                                  node_execution_retired = 1
              WHERE id = '{id}'"
        ),
    );
    assert_eq!(awaiting(&conn), None, "a stopped boot owes no stop");
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
                attempt_id: &new_runtime_upgrade_attempt_id(),
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
        record_runtime_upgrade_already_on_target(
            &conn,
            &on_target(operation_id, &id),
            &new_runtime_upgrade_attempt_id()
        )
        .unwrap()
    );
    assert!(
        record_runtime_upgrade_already_on_target(
            &conn,
            &on_target(operation_id, &id),
            &new_runtime_upgrade_attempt_id()
        )
        .unwrap()
    );
    // The same operation asking for another lifecycle records nothing.
    let mut started = on_target(operation_id, &id);
    started.target_state = RuntimeUpgradeTargetState::Started;
    assert!(
        !record_runtime_upgrade_already_on_target(
            &conn,
            &started,
            &new_runtime_upgrade_attempt_id()
        )
        .unwrap()
    );
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
        !record_runtime_upgrade_already_on_target(
            &conn,
            &on_target(rolled_back, &other),
            &new_runtime_upgrade_attempt_id()
        )
        .unwrap()
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

fn report_with_attempt(
    conn: &Connection,
    operation_id: RuntimeUpgradeOperationId,
) -> Option<(RuntimeUpgradeStatus, Option<String>)> {
    crate::db::read::runtime_upgrade_report(conn, operation_id)
        .unwrap()
        .map(|report| (report.status, report.attempt_id))
}

/// The attempt an upgrade ran as is reported with its live row and kept with
/// its rollback; a refusal is recorded for the attempt it refused, replacing an
/// earlier attempt's rollback or refusal but never an already-on-target outcome,
/// a live row, or another request's outcome, and only while the sandbox is
/// on the node.
#[test]
fn the_report_names_the_attempt_each_outcome_was_recorded_for() {
    let conn = test_conn();
    let envs = environments();
    let id = sandbox(&conn, SandboxState::Stopped, &envs.source);
    let operation_id = operation();
    let identity = RuntimeUpgradeRequestIdentity {
        target_environment: envs.target_reference,
        ..on_target(operation_id, &id)
    };

    fenced(begin_for_attempt(
        &conn,
        operation_id,
        &id,
        envs.target_reference,
        false,
        "attempt-a",
    ));
    assert_eq!(
        report_with_attempt(&conn, operation_id),
        Some((
            RuntimeUpgradeStatus::InProgress(RuntimeUpgradePhase::Fenced),
            Some("attempt-a".to_string())
        ))
    );
    // No refusal while the live row stands.
    assert!(!record_runtime_upgrade_refusal(&conn, &identity, "attempt-b", "busy").unwrap());
    assert!(roll_back_runtime_upgrade(&conn, operation_id, "boot failed").unwrap());
    assert!(finish_runtime_upgrade_rollback(&conn, operation_id).unwrap());
    assert_eq!(
        report_with_attempt(&conn, operation_id),
        Some((
            RuntimeUpgradeStatus::RolledBack {
                failure: Some("boot failed".to_string())
            },
            Some("attempt-a".to_string())
        ))
    );

    // A later attempt's refusal replaces the earlier attempt's rollback.
    assert!(
        record_runtime_upgrade_refusal(&conn, &identity, "attempt-b", "cleanup owns it").unwrap()
    );
    let refused_for = |attempt: &str, failure: &str| {
        Some((
            RuntimeUpgradeStatus::Refused {
                failure: Some(failure.to_string()),
            },
            Some(attempt.to_string()),
        ))
    };
    assert_eq!(
        report_with_attempt(&conn, operation_id),
        refused_for("attempt-b", "cleanup owns it")
    );
    // Another request under the operation records nothing: another target
    // state, target, authorization or sandbox.
    let other_sandbox = sandbox(&conn, SandboxState::Stopped, &envs.source);
    let others = [
        RuntimeUpgradeRequestIdentity {
            target_state: RuntimeUpgradeTargetState::Started,
            ..identity.clone()
        },
        RuntimeUpgradeRequestIdentity {
            target_environment: envs.source.environment().unwrap(),
            ..identity.clone()
        },
        RuntimeUpgradeRequestIdentity {
            authorization_id: "another-authorization".to_string(),
            ..identity.clone()
        },
        RuntimeUpgradeRequestIdentity {
            sandbox_id: other_sandbox,
            ..identity.clone()
        },
    ];
    assert_ne!(others[1].target_environment, identity.target_environment);
    for other in &others {
        assert!(
            !record_runtime_upgrade_refusal(&conn, other, "attempt-c", "other").unwrap(),
            "{other:?}"
        );
        assert_eq!(
            report_with_attempt(&conn, operation_id),
            refused_for("attempt-b", "cleanup owns it"),
            "{other:?}"
        );
    }

    // A refusal never replaces an already-on-target outcome.
    let found = operation();
    let found_identity = RuntimeUpgradeRequestIdentity {
        operation_id: found,
        ..identity.clone()
    };
    assert!(record_runtime_upgrade_already_on_target(&conn, &found_identity, "attempt-a").unwrap());
    assert!(!record_runtime_upgrade_refusal(&conn, &found_identity, "attempt-b", "busy").unwrap());
    assert_eq!(
        report_with_attempt(&conn, found),
        Some((
            RuntimeUpgradeStatus::AlreadyOnTarget { activation: None },
            Some("attempt-a".to_string())
        ))
    );
    // A replay as another attempt is reported for that attempt.
    assert!(record_runtime_upgrade_already_on_target(&conn, &found_identity, "attempt-c").unwrap());
    assert_eq!(
        report_with_attempt(&conn, found),
        Some((
            RuntimeUpgradeStatus::AlreadyOnTarget { activation: None },
            Some("attempt-c".to_string())
        ))
    );

    // Nor is one recorded for a sandbox that isn't on the node.
    let gone = RuntimeUpgradeRequestIdentity {
        operation_id: operation(),
        sandbox_id: SandboxId::new().to_string(),
        ..identity
    };
    assert!(!record_runtime_upgrade_refusal(&conn, &gone, "attempt-a", "missing").unwrap());
    assert_eq!(report_with_attempt(&conn, gone.operation_id), None);
}

/// The operation's report keeps only its latest outcome, but each attempt
/// keeps its own: a later attempt's rollback or refusal never erases an
/// earlier attempt's, and an attempt's first final outcome stays. A row written
/// before attempts were recorded adds nothing. Deleting the sandbox keeps the
/// history, which the lookup reads by operation and attempt alone, so it
/// outlives the sandbox's row.
#[test]
fn each_attempts_rollback_or_refusal_is_kept_for_that_attempt() {
    let conn = test_conn();
    let envs = environments();
    let id = sandbox(&conn, SandboxState::Stopped, &envs.source);
    let operation_id = operation();
    let identity = RuntimeUpgradeRequestIdentity {
        target_environment: envs.target_reference,
        ..on_target(operation_id, &id)
    };
    let attempt_outcome = |attempt: &str| {
        crate::db::read::runtime_upgrade_attempt_outcome(&conn, operation_id, attempt).unwrap()
    };
    // `None`: a live row written before the node recorded attempts.
    let roll_back_for = |attempt: Option<&str>, failure: &str| {
        fenced(begin_for_attempt(
            &conn,
            operation_id,
            &id,
            envs.target_reference,
            false,
            attempt.unwrap_or("legacy"),
        ));
        if attempt.is_none() {
            conn.execute(
                "UPDATE sandbox_runtime_upgrades SET attempt_id = NULL WHERE operation_id = ?1",
                params![operation_id.to_string()],
            )
            .unwrap();
        }
        assert!(roll_back_runtime_upgrade(&conn, operation_id, failure).unwrap());
        assert!(finish_runtime_upgrade_rollback(&conn, operation_id).unwrap());
    };

    roll_back_for(Some("attempt-a"), "a's boot failed");
    roll_back_for(Some("attempt-b"), "b's boot failed");
    // A second rollback for an attempt keeps its first.
    roll_back_for(Some("attempt-a"), "a's second boot failed");
    roll_back_for(None, "anonymous boot failed");
    // The operation's outcome is the latest rollback's, which names no attempt.
    assert_eq!(
        report_with_attempt(&conn, operation_id),
        Some((
            RuntimeUpgradeStatus::RolledBack {
                failure: Some("anonymous boot failed".to_string())
            },
            None
        ))
    );
    assert!(record_runtime_upgrade_refusal(&conn, &identity, "attempt-c", "c refused").unwrap());
    // An attempt's first final outcome stays.
    assert!(record_runtime_upgrade_refusal(&conn, &identity, "attempt-a", "a refused").unwrap());

    let rolled_back = |failure: &str| {
        Some(RuntimeUpgradeAttemptOutcome {
            outcome: RecordedRuntimeUpgradeOutcome::RolledBack,
            failure: Some(failure.to_string()),
        })
    };
    assert_eq!(attempt_outcome("attempt-a"), rolled_back("a's boot failed"));
    assert_eq!(attempt_outcome("attempt-b"), rolled_back("b's boot failed"));
    assert_eq!(
        attempt_outcome("attempt-c"),
        Some(RuntimeUpgradeAttemptOutcome {
            outcome: RecordedRuntimeUpgradeOutcome::Refused,
            failure: Some("c refused".to_string()),
        })
    );
    assert_eq!(attempt_outcome("attempt-d"), None);
    assert_eq!(
        attempt_outcome("legacy"),
        None,
        "a legacy row adds no history"
    );
    // The report is still the operation's latest outcome.
    assert_eq!(
        report_with_attempt(&conn, operation_id),
        Some((
            RuntimeUpgradeStatus::Refused {
                failure: Some("a refused".to_string())
            },
            Some("attempt-a".to_string())
        ))
    );

    delete_sandbox(&conn, &id).unwrap();
    assert_eq!(attempt_outcome("attempt-b"), rolled_back("b's boot failed"));
}

#[test]
fn deleting_the_sandbox_deletes_its_recorded_outcomes() {
    let conn = test_conn();
    let envs = environments();
    let id = sandbox(&conn, SandboxState::Stopped, &envs.source);
    let operation_id = operation();
    assert!(
        record_runtime_upgrade_already_on_target(
            &conn,
            &on_target(operation_id, &id),
            &new_runtime_upgrade_attempt_id()
        )
        .unwrap()
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
        record_runtime_upgrade_already_on_target(
            &conn,
            &on_target(operation_id, &id),
            &new_runtime_upgrade_attempt_id()
        )
        .unwrap()
    );
    assert!(!claim_runtime_upgrade_activation(&conn, operation_id, &other, started()).unwrap());

    assert!(claim_runtime_upgrade_activation(&conn, operation_id, &id, started()).unwrap());
    // Once.
    assert!(!claim_runtime_upgrade_activation(&conn, operation_id, &id, started()).unwrap());
    // A replay records the outcome again and keeps the activation.
    assert!(
        record_runtime_upgrade_already_on_target(
            &conn,
            &on_target(operation_id, &id),
            &new_runtime_upgrade_attempt_id()
        )
        .unwrap()
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
        record_runtime_upgrade_already_on_target(
            &conn,
            &on_target(operation_id, &id),
            &new_runtime_upgrade_attempt_id()
        )
        .unwrap()
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
    assert!(
        record_runtime_upgrade_already_on_target(
            &conn,
            &on_target(found, &id),
            &new_runtime_upgrade_attempt_id()
        )
        .unwrap()
    );
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
    assert!(
        record_runtime_upgrade_already_on_target(
            &conn,
            &on_target(found, &id),
            &new_runtime_upgrade_attempt_id()
        )
        .unwrap()
    );
    assert_eq!(
        begin(&conn, found, &other, envs.target_reference, false),
        BeginRuntimeUpgrade::OperationNamesAnother
    );

    // Fencing one sandbox, it can't be recorded for another.
    let fencing = operation();
    fenced(begin(&conn, fencing, &id, envs.target_reference, false));
    assert!(
        !record_runtime_upgrade_already_on_target(
            &conn,
            &on_target(fencing, &other),
            &new_runtime_upgrade_attempt_id()
        )
        .unwrap()
    );
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

/// A suspended sandbox's eviction claim waits for every upgrade that still
/// owns its disk, fencing it or owing the old checkpoint's retirement, and
/// leaves the row unchanged while it does. A rolled-back or retired upgrade
/// owns nothing. The claim spells its phases out, so this also holds it to
/// `unretired_runtime_upgrade`'s.
#[test]
fn a_suspended_eviction_claim_waits_for_an_unretired_upgrade() {
    use RuntimeUpgradePhase::*;
    for (phase, claimable) in [
        (Fenced, false),
        (Preserved, false),
        (Booting, false),
        (Verified, false),
        (RolledBack, true),
        (Retired, true),
    ] {
        let conn = test_conn();
        let envs = environments();
        let id = sandbox(&conn, SandboxState::Suspended, &envs.source);
        let pin = pin_suspend_artifact(&conn, &id).to_string();
        fenced(begin(&conn, operation(), &id, envs.target_reference, true));
        conn.execute(
            "UPDATE sandbox_runtime_upgrades SET phase = ?1",
            params![phase.as_str()],
        )
        .unwrap();
        let before = get_sandbox(&conn, &id).unwrap().unwrap();
        assert_eq!(
            claimable,
            unretired_runtime_upgrade(&conn, &id).unwrap().is_none(),
            "{phase}: the claim and the unretired-upgrade reader disagree"
        );

        for expected_updated_at in [None, Some(before.updated_at.as_str())] {
            if claimable {
                break;
            }
            assert!(
                !claim_suspended_sandbox_for_eviction(&conn, &id, Some(&pin), expected_updated_at)
                    .unwrap(),
                "{phase}"
            );
        }
        if !claimable {
            // So does the claim of a copy a runtime upgrade replaces.
            let generations = activation_generations(&conn, &id).unwrap().unwrap();
            assert!(
                !claim_stale_upgrade_copy_for_eviction(&conn, &id, &pin, &generations).unwrap(),
                "{phase}"
            );
        }
        let after = get_sandbox(&conn, &id).unwrap().unwrap();
        assert_eq!(
            (after.state, &after.updated_at),
            (
                SandboxStatus::Unowned(SandboxState::Suspended),
                &before.updated_at
            ),
            "{phase}"
        );
        if claimable {
            assert!(
                claim_suspended_sandbox_for_eviction(&conn, &id, Some(&pin), None).unwrap(),
                "{phase}"
            );
        }
    }
}

/// An attempt's refusal is recorded only for an operation that reached its
/// target for that sandbox: retired, or already on target with no live row.
/// One still in progress, rolled back or refused, another sandbox's, or one
/// the node never saw records nothing, since the operation's own report
/// already says what happened to the attempt. The report names the latest
/// attempt refused, and deleting the sandbox keeps the record.
#[test]
fn a_refused_attempt_is_recorded_only_for_an_operation_that_reached_its_target() {
    let conn = test_conn();
    let envs = environments();
    let id = sandbox(&conn, SandboxState::Stopped, &envs.source);
    let other = sandbox(&conn, SandboxState::Stopped, &envs.source);
    let refuse = |operation_id, sandbox_id: &str, attempt_id: &str| {
        record_runtime_upgrade_refused_attempt(
            &conn,
            operation_id,
            sandbox_id,
            attempt_id,
            "refused",
        )
        .unwrap()
    };
    let refused_attempt = |operation_id| {
        crate::db::read::runtime_upgrade_report(&conn, operation_id)
            .unwrap()
            .and_then(|report| report.refused_attempt)
            .map(|refused| refused.attempt_id)
    };

    // Never seen, in progress, rolled back and refused: nothing recorded.
    let unseen = operation();
    assert!(!refuse(unseen, &id, "attempt-a"));
    let upgrading = operation();
    fenced(begin(&conn, upgrading, &id, envs.target_reference, false));
    assert!(
        advance_runtime_upgrade(
            &conn,
            upgrading,
            RuntimeUpgradePhase::Fenced,
            RuntimeUpgradePhase::Preserved
        )
        .unwrap()
    );
    assert!(bind_runtime_upgrade_consumer(&conn, upgrading, &envs.target).unwrap());
    assert!(
        advance_runtime_upgrade(
            &conn,
            upgrading,
            RuntimeUpgradePhase::Booting,
            RuntimeUpgradePhase::Verified
        )
        .unwrap()
    );
    assert!(
        !refuse(upgrading, &id, "attempt-a"),
        "verified isn't retired"
    );
    let refused = operation();
    let rolled_back = operation();
    fenced(begin(
        &conn,
        rolled_back,
        &other,
        envs.target_reference,
        false,
    ));
    assert!(roll_back_runtime_upgrade(&conn, rolled_back, "boot failed").unwrap());
    assert!(finish_runtime_upgrade_rollback(&conn, rolled_back).unwrap());
    assert!(!refuse(rolled_back, &other, "attempt-a"));
    let refused_early = on_target(refused, &other);
    assert!(record_runtime_upgrade_refusal(&conn, &refused_early, "attempt-a", "busy").unwrap());
    assert!(!refuse(refused, &other, "attempt-a"));

    // Retired: recorded, and only for its own sandbox.
    assert!(
        advance_runtime_upgrade(
            &conn,
            upgrading,
            RuntimeUpgradePhase::Verified,
            RuntimeUpgradePhase::Retired
        )
        .unwrap()
    );
    assert!(!refuse(upgrading, &other, "attempt-a"));
    assert!(refuse(upgrading, &id, "attempt-a"));
    assert_eq!(refused_attempt(upgrading).as_deref(), Some("attempt-a"));
    assert_eq!(
        crate::db::read::runtime_upgrade_refused_attempt(&conn, upgrading, "attempt-a").unwrap(),
        Some(RuntimeUpgradeRefusedAttempt {
            attempt_id: "attempt-a".to_string(),
            failure: Some("refused".to_string()),
        })
    );
    assert_eq!(
        crate::db::read::runtime_upgrade_refused_attempt(&conn, upgrading, "attempt-b").unwrap(),
        None
    );

    // Already on target: recorded, and the latest attempt is the one reported.
    let found = operation();
    assert!(
        record_runtime_upgrade_already_on_target(
            &conn,
            &on_target(found, &other),
            &new_runtime_upgrade_attempt_id()
        )
        .unwrap()
    );
    assert!(
        !refuse(found, &id, "attempt-a"),
        "the outcome is another sandbox's"
    );
    assert!(refuse(found, &other, "attempt-a"));
    assert!(refuse(found, &other, "attempt-b"));
    assert_eq!(refused_attempt(found).as_deref(), Some("attempt-b"));
    // A retry that fenced after the outcome was recorded hasn't reached the
    // target until its row retires.
    let third = sandbox(&conn, SandboxState::Stopped, &envs.source);
    let fenced_after = operation();
    assert!(
        record_runtime_upgrade_already_on_target(
            &conn,
            &on_target(fenced_after, &third),
            &new_runtime_upgrade_attempt_id()
        )
        .unwrap()
    );
    fenced(begin(
        &conn,
        fenced_after,
        &third,
        envs.target_reference,
        false,
    ));
    assert!(
        !refuse(fenced_after, &third, "attempt-a"),
        "its live row is fenced"
    );
    // An attempt's refusal is final: recording it again, in the same second
    // or later, keeps the first and leaves the later attempt the latest.
    assert!(
        !record_runtime_upgrade_refused_attempt(&conn, found, &other, "attempt-a", "again")
            .unwrap(),
        "attempt-a's refusal is already recorded"
    );
    assert_eq!(refused_attempt(found).as_deref(), Some("attempt-b"));
    assert_eq!(
        crate::db::read::runtime_upgrade_refused_attempt(&conn, found, "attempt-a")
            .unwrap()
            .and_then(|refused| refused.failure)
            .as_deref(),
        Some("refused")
    );

    // Each operation reports its own latest, whichever recorded last.
    assert_eq!(refused_attempt(upgrading).as_deref(), Some("attempt-a"));

    delete_sandbox(&conn, &other).unwrap();
    assert_eq!(
        crate::db::read::runtime_upgrade_refused_attempt(&conn, found, "attempt-a").unwrap(),
        Some(RuntimeUpgradeRefusedAttempt {
            attempt_id: "attempt-a".to_string(),
            failure: Some("refused".to_string()),
        })
    );
}

/// [`UNRETIRED_UPGRADE_PHASES_SQL`] spells out the fencing phases by hand,
/// alongside `verified`. It names exactly those, so a fencing phase added to
/// [`FENCING_PHASES`] alone can't read as retired to the queries that use it.
#[test]
fn the_unretired_phase_list_is_the_fencing_phases_and_verified() {
    let listed: std::collections::BTreeSet<&str> = UNRETIRED_UPGRADE_PHASES_SQL
        .trim_matches(|c| c == '(' || c == ')')
        .split(',')
        .map(|phase| phase.trim().trim_matches('\''))
        .collect();
    let expected: std::collections::BTreeSet<&str> = FENCING_PHASES
        .into_iter()
        .chain([RuntimeUpgradePhase::Verified])
        .map(RuntimeUpgradePhase::as_str)
        .collect();
    assert_eq!(listed, expected);
}

/// An operation abandoned before the node ever saw it (its call still on its
/// way) never fences afterwards, from any pod; abandoning it again changes
/// nothing. Another operation of the same sandbox is untouched.
#[test]
fn an_abandoned_operation_never_fences_afterwards() {
    let conn = test_conn();
    let envs = environments();
    let id = sandbox(&conn, SandboxState::Stopped, &envs.source);
    let abandoned = operation();

    assert_eq!(
        abandon_runtime_upgrade(&conn, &id, abandoned).unwrap(),
        AbandonRuntimeUpgrade::Abandoned
    );
    assert_eq!(
        abandon_runtime_upgrade(&conn, &id, abandoned).unwrap(),
        AbandonRuntimeUpgrade::Abandoned
    );
    assert!(crate::db::read::runtime_upgrade_abandoned(&conn, abandoned).unwrap());
    let other_sandbox = sandbox(&conn, SandboxState::Stopped, &envs.source);
    assert_eq!(
        abandon_runtime_upgrade(&conn, &other_sandbox, abandoned).unwrap(),
        AbandonRuntimeUpgrade::OperationNamesAnother
    );

    for attempt_id in ["late-attempt", "new-attempt", "another-attempt"] {
        assert!(matches!(
            begin_for_attempt(
                &conn,
                abandoned,
                &id,
                envs.target_reference,
                false,
                attempt_id
            ),
            BeginRuntimeUpgrade::Abandoned
        ));
    }
    assert_eq!(runtime_upgrade(&conn, abandoned).unwrap(), None);
    assert_eq!(
        crate::db::read::runtime_upgrade_fence(&conn, &id).unwrap(),
        None
    );

    let other = operation();
    assert!(!crate::db::read::runtime_upgrade_abandoned(&conn, other).unwrap());
    fenced(begin(&conn, other, &id, envs.target_reference, false));
}

/// An operation that hasn't settled isn't abandoned: one that fences the
/// sandbox may still change its disk, and a verified boot still owes its
/// stop. One rolled back since is: a new pod's retry of it would
/// otherwise fence and boot again. An id the node recorded for another
/// sandbox is refused, and a sandbox the node doesn't hold has nothing to
/// fence.
#[test]
fn only_a_settled_operation_is_abandoned() {
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

    assert_eq!(
        abandon_runtime_upgrade(&conn, &id, operation_id).unwrap(),
        AbandonRuntimeUpgrade::Unsettled
    );
    assert!(!crate::db::read::runtime_upgrade_abandoned(&conn, operation_id).unwrap());
    // A verified boot still owes its stop and retirement, which only a retry
    // of it finishes, so it isn't abandoned either.
    conn.execute(
        "UPDATE sandbox_runtime_upgrades SET phase = 'verified' WHERE operation_id = ?1",
        params![operation_id.to_string()],
    )
    .unwrap();
    assert_eq!(
        abandon_runtime_upgrade(&conn, &id, operation_id).unwrap(),
        AbandonRuntimeUpgrade::Unsettled
    );
    conn.execute(
        "UPDATE sandbox_runtime_upgrades SET phase = 'booting' WHERE operation_id = ?1",
        params![operation_id.to_string()],
    )
    .unwrap();

    let other_sandbox = sandbox(&conn, SandboxState::Stopped, &envs.source);
    assert_eq!(
        abandon_runtime_upgrade(&conn, &other_sandbox, operation_id).unwrap(),
        AbandonRuntimeUpgrade::OperationNamesAnother
    );

    assert!(roll_back_runtime_upgrade(&conn, operation_id, "boot failed").unwrap());
    assert!(finish_runtime_upgrade_rollback(&conn, operation_id).unwrap());
    // The settled operation's row is gone, but its recorded outcome still
    // names the sandbox.
    assert_eq!(
        abandon_runtime_upgrade(&conn, &other_sandbox, operation_id).unwrap(),
        AbandonRuntimeUpgrade::OperationNamesAnother
    );
    assert_eq!(
        abandon_runtime_upgrade(&conn, &id, operation_id).unwrap(),
        AbandonRuntimeUpgrade::Abandoned
    );
    assert!(matches!(
        begin_for_attempt(
            &conn,
            operation_id,
            &id,
            envs.target_reference,
            false,
            "new-attempt"
        ),
        BeginRuntimeUpgrade::Abandoned
    ));

    // A sandbox the node doesn't hold: a call carrying `stage_from` could
    // still stage it, so the operation is recorded all the same.
    let absent = operation();
    assert_eq!(
        abandon_runtime_upgrade(&conn, &SandboxId::new().to_string(), absent).unwrap(),
        AbandonRuntimeUpgrade::Abandoned
    );
    assert!(crate::db::read::runtime_upgrade_abandoned(&conn, absent).unwrap());
}

/// A call for an abandoned operation that names another sandbox takes
/// another lock than the abandon, so it can pass its own check first. The
/// writes it can still reach refuse the operation too, so it never records an
/// outcome the abandonment contradicts.
#[test]
fn an_abandoned_operation_records_no_outcome_for_another_sandbox() {
    let conn = test_conn();
    let envs = environments();
    let abandoned_for = sandbox(&conn, SandboxState::Stopped, &envs.source);
    let other = sandbox(&conn, SandboxState::Stopped, &envs.source);
    let operation_id = operation();
    abandon_runtime_upgrade(&conn, &abandoned_for, operation_id).unwrap();

    assert!(
        !record_runtime_upgrade_already_on_target(
            &conn,
            &on_target(operation_id, &other),
            "attempt-a"
        )
        .unwrap()
    );
    assert!(
        !record_runtime_upgrade_refusal(
            &conn,
            &on_target(operation_id, &other),
            "attempt-a",
            "busy"
        )
        .unwrap()
    );
    assert!(
        crate::db::read::runtime_upgrade_report(&conn, operation_id)
            .unwrap()
            .is_none()
    );
}

/// A rolled-back row still owes deleting its preserved copy, which never
/// touches the sandbox, so it's done with the sandbox and abandoned as it
/// stands; recovery deletes the copy afterwards.
#[test]
fn a_rolled_back_operation_is_abandoned_as_it_stands() {
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
    assert!(roll_back_runtime_upgrade(&conn, operation_id, "boot failed").unwrap());

    assert_eq!(
        abandon_runtime_upgrade(&conn, &id, operation_id).unwrap(),
        AbandonRuntimeUpgrade::Abandoned
    );
    assert_eq!(
        runtime_upgrade(&conn, operation_id)
            .unwrap()
            .map(|record| record.phase),
        Some(RuntimeUpgradePhase::RolledBack)
    );
}

#[test]
fn deleting_the_sandbox_keeps_its_abandoned_operations() {
    let conn = test_conn();
    let envs = environments();
    let id = sandbox(&conn, SandboxState::Stopped, &envs.source);
    let operation_id = operation();
    abandon_runtime_upgrade(&conn, &id, operation_id).unwrap();

    delete_sandbox(&conn, &id).unwrap();

    assert!(crate::db::read::runtime_upgrade_abandoned(&conn, operation_id).unwrap());
}

/// A fencing upgrade's retained execution, which the State page publishes in
/// place of the target the boot saved, is charged to the page's byte budget
/// whether or not it decodes: one larger than the whole budget fails the page
/// as an oversized row does.
#[test]
fn a_fencing_upgrades_retained_execution_is_charged_to_the_page_budget() {
    use crate::db::inventory_read::InventoryReadError;
    use crate::db::read::{StateBudgets, StatePageError, state_page};
    use pplx_space_common_core::inventory::{InventoryBudget, MAX_ITEMS, MAX_RAW_BYTES};

    let conn = test_conn();
    let envs = environments();
    let id = sandbox(&conn, SandboxState::Stopped, &envs.source);
    fenced(begin(&conn, operation(), &id, envs.target_reference, true));
    let ids = vec![id];
    let budgets = || StateBudgets {
        raw: InventoryBudget::new(MAX_ITEMS, MAX_RAW_BYTES),
        metadata: InventoryBudget::new(MAX_ITEMS, MAX_RAW_BYTES),
    };
    state_page(&conn, &ids, &mut budgets()).unwrap();

    let retained: String = conn
        .query_row(
            "SELECT previous_execution_metadata_json FROM sandbox_runtime_upgrades",
            [],
            |row| row.get(0),
        )
        .unwrap();
    // The same execution padded past the budget still decodes; the other
    // value doesn't.
    for oversized in [
        format!("{retained}{}", " ".repeat(MAX_RAW_BYTES)),
        "x".repeat(MAX_RAW_BYTES + 1),
    ] {
        conn.execute(
            "UPDATE sandbox_runtime_upgrades SET previous_execution_metadata_json = ?1",
            params![oversized],
        )
        .unwrap();

        let result = state_page(&conn, &ids, &mut budgets());
        assert!(
            matches!(
                result,
                Err(StatePageError::Row(InventoryReadError::Limit(_)))
            ),
            "{:?}",
            result.err()
        );
    }
}

/// Only one upgrade fences a sandbox, but should a second fencing row ever
/// appear, the State page publishes the earliest one's retained execution,
/// the upgrade `fencing_runtime_upgrade` names, not the saved target and not
/// whichever row SQLite returns first.
#[test]
fn the_earliest_fencing_upgrade_is_the_one_published() {
    use crate::db::read::{StateBudgets, StatePageRow, state_page};
    use pplx_space_common_core::inventory::{InventoryBudget, MAX_ITEMS, MAX_RAW_BYTES};

    let conn = test_conn();
    let envs = environments();
    let id = sandbox(&conn, SandboxState::Stopped, &envs.source);
    fenced(begin(&conn, operation(), &id, envs.target_reference, true));
    // The boot saved the target, and the row inserted first is the later
    // one: it retains the target too, while an earlier row inserted after it
    // retains the source.
    upsert_sandbox_execution_metadata(&conn, &id, &envs.target).unwrap();
    conn.execute(
        "INSERT INTO sandbox_runtime_upgrades (
             operation_id, sandbox_id, target_environment, target_state,
             authorization_id, authorization_reason, memory_loss_authorized,
             source_state, previous_execution_metadata_json, checkpoint_artifact_id,
             checkpoint_dir_identity, boot_execution_id, phase, failure, created_at,
             updated_at
         )
         SELECT ?1, sandbox_id, target_environment, target_state, authorization_id,
                authorization_reason, memory_loss_authorized, source_state,
                previous_execution_metadata_json, checkpoint_artifact_id,
                checkpoint_dir_identity, boot_execution_id, 'booting', failure,
                created_at, updated_at
           FROM sandbox_runtime_upgrades",
        params![operation().to_string()],
    )
    .unwrap();
    conn.execute(
        "UPDATE sandbox_runtime_upgrades
            SET previous_execution_metadata_json = ?1, created_at = created_at || 'z'
          WHERE phase = 'fenced'",
        params![serde_json::to_string(&envs.target).unwrap()],
    )
    .unwrap();
    let mut budgets = StateBudgets {
        raw: InventoryBudget::new(MAX_ITEMS, MAX_RAW_BYTES),
        metadata: InventoryBudget::new(MAX_ITEMS, MAX_RAW_BYTES),
    };

    let page = state_page(&conn, std::slice::from_ref(&id), &mut budgets).unwrap();

    let [StatePageRow::Decoded(row)] = page.as_slice() else {
        panic!("one decoded row");
    };
    assert_eq!(row.published_execution.as_ref(), Some(&envs.source));
}

/// The substitution starts at the fence, not at the boot: an unbound
/// sandbox, which published no execution, publishes the execution its disk
/// records, the one a rollback writes, in `fenced` and `preserved` alike.
#[test]
fn an_unbound_sandbox_publishes_its_disks_execution_from_the_fence() {
    use crate::db::read::{StateBudgets, StatePageRow, state_page};
    use pplx_space_common_core::inventory::{InventoryBudget, MAX_ITEMS, MAX_RAW_BYTES};

    let conn = test_conn();
    let envs = environments();
    let id = SandboxId::new().to_string();
    insert_test_sandbox(&conn, &id, SandboxState::Stopped);
    let published = |conn: &Connection| {
        let mut budgets = StateBudgets {
            raw: InventoryBudget::new(MAX_ITEMS, MAX_RAW_BYTES),
            metadata: InventoryBudget::new(MAX_ITEMS, MAX_RAW_BYTES),
        };
        match state_page(conn, std::slice::from_ref(&id), &mut budgets)
            .unwrap()
            .as_slice()
        {
            [StatePageRow::Decoded(row)] => row.published_execution.clone(),
            _ => panic!("one decoded row"),
        }
    };
    assert_eq!(published(&conn), None);

    let operation_id = operation();
    fenced(begin(&conn, operation_id, &id, envs.target_reference, true));
    assert_eq!(published(&conn).as_ref(), Some(&envs.source), "fenced");
    assert!(
        advance_runtime_upgrade(
            &conn,
            operation_id,
            RuntimeUpgradePhase::Fenced,
            RuntimeUpgradePhase::Preserved,
        )
        .unwrap()
    );
    assert_eq!(published(&conn).as_ref(), Some(&envs.source), "preserved");
}

/// A fencing upgrade whose retained execution doesn't decode costs only its
/// own sandbox's execution: the State page publishes that sandbox without one,
/// as it does a sandbox whose own saved execution isn't valid JSON, never
/// under the target the boot saved, and answers for the rest unchanged. That
/// holds for malformed JSON and for a value that isn't valid text at all,
/// which SQLite's non-strict column accepts.
#[test]
fn an_undecodable_retained_execution_degrades_only_its_own_sandbox() {
    use crate::db::read::{StateBudgets, StatePageRow, state_page};
    use pplx_space_common_core::inventory::{InventoryBudget, MAX_ITEMS, MAX_RAW_BYTES};

    let budgets = || StateBudgets {
        raw: InventoryBudget::new(MAX_ITEMS, MAX_RAW_BYTES),
        metadata: InventoryBudget::new(MAX_ITEMS, MAX_RAW_BYTES),
    };
    let decoded = |row: &StatePageRow| match row {
        StatePageRow::Decoded(decoded) => Some((
            decoded.row.id.clone(),
            decoded.row.state,
            decoded.published_execution.clone(),
        )),
        _ => None,
    };
    for retained in ["'not json'", "x'00ff'", "CAST(x'ff' AS TEXT)"] {
        let conn = test_conn();
        let envs = environments();
        let fenced_id = sandbox(&conn, SandboxState::Stopped, &envs.source);
        fenced(begin(
            &conn,
            operation(),
            &fenced_id,
            envs.target_reference,
            true,
        ));
        let others = [
            sandbox(&conn, SandboxState::Stopped, &envs.source),
            sandbox(&conn, SandboxState::Suspended, &envs.source),
        ];
        conn.execute(
            &format!(
                "UPDATE sandbox_runtime_upgrades SET previous_execution_metadata_json = {retained}"
            ),
            [],
        )
        .unwrap();
        let alone: Vec<_> = state_page(&conn, &others, &mut budgets())
            .unwrap()
            .iter()
            .map(|row| decoded(row).expect("a good row decodes"))
            .collect();

        let ids = vec![others[0].clone(), fenced_id.clone(), others[1].clone()];
        let page = state_page(&conn, &ids, &mut budgets()).unwrap();

        assert_eq!(page.len(), 3, "{retained}");
        assert_eq!(
            decoded(&page[1]),
            Some((
                fenced_id.clone(),
                SandboxStatus::Unowned(SandboxState::Stopped),
                None
            )),
            "{retained}: the fenced sandbox is published without an execution"
        );
        assert_eq!(
            [decoded(&page[0]), decoded(&page[2])],
            [Some(alone[0].clone()), Some(alone[1].clone())],
            "{retained}: the other sandboxes are published as without it"
        );
    }
}
