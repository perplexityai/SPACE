//! Direct tests for node-local SQLite `sandboxes.state` writers that had no or
//! thin coverage: each one's accepting case and each refusing predicate in its
//! own case, every refusal starting from a row the write would otherwise change.

use pplx_space_common_core::types::{
    LocalArtifactId, RuntimeKind, SandboxCleanupEpisodeId, SandboxId, SandboxQuarantineNextAction,
    SandboxQuarantineReasonCode, SandboxQuarantineSource, TransferScopeId,
};
use rusqlite::{Connection, params};

use super::accepted_shapes_quarantine::{adoptable, insert_claimed_adoptable};
use super::state_write_fixtures::{
    Cell, ExpectedDiff, NULL, RESTORE_STORAGE_URI, Snapshot, as_synthetic_orphan, assert_accepts,
    assert_unchanged, backdate, decorate, endpoint_proof, execution_metadata, generation,
    insert_creating, insert_row, int, quarantine_history_entered, quarantine_state_entered,
    receipt_cleared, restore_source, seed_receipt, set_failure_reason, snapshot, snapshot_path,
    text, with_sandbox_params,
};
use crate::db::{
    ActivationKind, ActivationRouteAdmission, CleanupCompletion, CleanupEpisodeSpec,
    QuarantineAdoptionOutcome, RestoreAdoptOrphanInsertError, SandboxStateWriteOutcome,
    StartupResidentSettlement, SupersedeActivationError, adopt_quarantine_cleanup_episode,
    begin_activation_with_route_evidence, begin_teardown_suspend, get_sandbox,
    immediate_transaction, insert_cleanup_episode, insert_quarantined_sandbox_in_transaction,
    insert_restore_transfer_scope, mark_local_artifact_backup_completed,
    pin_suspended_artifact_for_cleanup, request_cleanup_episode,
    restore_adopt_orphan_or_insert_sandbox, restore_checkpoint_state, settle_startup_resident,
    stage_teardown_suspend_artifact, test_conn, update_state_superseding_activation_in_transaction,
};
use crate::restore_operation::RemoteRestoreOperation;
use crate::state::SandboxState;

fn refuses<T: std::fmt::Debug>(
    case: &str,
    conn: &Connection,
    write: impl FnOnce() -> T,
    refused: impl FnOnce(&T) -> bool,
) {
    let before: Snapshot = snapshot(conn);
    let result = write();
    assert!(
        refused(&result),
        "{case}: expected a refusal, got {result:?}"
    );
    assert_unchanged(case, &before, conn);
}

#[test]
fn checkpoint_rollback_requires_a_checkpointing_row_and_a_resident_target() {
    let conn = test_conn();
    for target in SandboxState::all() {
        let id = insert_row(&conn, SandboxState::Checkpointing);
        let case = format!("checkpointing -> {target}");
        if matches!(target, SandboxState::Started | SandboxState::Paused) {
            // `accepted_shapes_single_statement` pins the accepted row diff.
            assert!(
                restore_checkpoint_state(&conn, &id, target).unwrap(),
                "{case}"
            );
            assert_eq!(
                get_sandbox(&conn, &id).unwrap().unwrap().state,
                target,
                "{case}"
            );
        } else {
            refuses(
                &case,
                &conn,
                || restore_checkpoint_state(&conn, &id, target),
                |result| matches!(result, Err(rusqlite::Error::InvalidQuery)),
            );
        }
    }
    for state in SandboxState::all() {
        if state == SandboxState::Checkpointing {
            continue;
        }
        let id = insert_row(&conn, state);
        refuses(
            state.as_str(),
            &conn,
            || restore_checkpoint_state(&conn, &id, SandboxState::Started),
            |result| matches!(result, Ok(false)),
        );
    }
    refuses(
        "missing row",
        &conn,
        || restore_checkpoint_state(&conn, &SandboxId::new().to_string(), SandboxState::Paused),
        |result| matches!(result, Ok(false)),
    );
}

/// Cleanup admission's call: the writer in the caller's transaction, which a
/// refusal abandons.
fn supersede(
    conn: &Connection,
    id: &str,
    target: SandboxState,
) -> Result<(), SupersedeActivationError> {
    let tx = immediate_transaction(conn)?;
    update_state_superseding_activation_in_transaction(&tx, id, target)?;
    tx.commit()?;
    Ok(())
}

fn superseded(target: SandboxState) -> Vec<(&'static str, Cell)> {
    vec![
        ("state", text(target.as_str())),
        ("cleanup_completion", NULL),
        ("cleanup_keep_filesystem", NULL),
        ("failure_reason", NULL),
        ("state_changed_at", Cell::NowText),
        ("updated_at", Cell::NowText),
    ]
}

/// Per-source-state outcomes are `update_state`'s, pinned by
/// `update_state_matrix`; these cases pin what supersession adds around it.
#[test]
fn activation_supersession_retires_the_episode_before_the_generic_write() {
    for target in [SandboxState::Stopping, SandboxState::Deleting] {
        let conn = test_conn();

        let starting = insert_row(&conn, SandboxState::Stopped);
        begin_activation_with_route_evidence(
            &conn,
            &starting,
            SandboxState::Stopped,
            ActivationKind::ColdStart,
            None,
            None,
            ActivationRouteAdmission::default(),
        )
        .unwrap()
        .unwrap();
        decorate(&conn, &starting, SandboxState::Started);
        let mut cells = superseded(target);
        cells.extend(receipt_cleared(SandboxState::Started));
        assert_accepts(
            &format!("armed cold start -> {target}"),
            &conn,
            || supersede(&conn, &starting, target).unwrap(),
            ExpectedDiff::new()
                .sandbox(&starting, cells)
                .deleted("sandbox_cleanup_episodes", ("sandbox_id", &starting)),
        );

        let (requested, cleanup_id) = insert_creating(&conn, CleanupCompletion::DeleteRow);
        request_cleanup_episode(
            &conn,
            &requested,
            cleanup_id,
            SandboxQuarantineReasonCode::CreateInterrupted,
            "create interrupted",
        )
        .unwrap();
        backdate(&conn, &requested);
        assert_accepts(
            &format!("requested create cleanup -> {target}"),
            &conn,
            || supersede(&conn, &requested, target).unwrap(),
            ExpectedDiff::new()
                .sandbox(&requested, superseded(target))
                .deleted("sandbox_cleanup_episodes", ("sandbox_id", &requested)),
        );

        // A create quarantined while its episode was armed: the generic write
        // refuses a stop of the quarantined row, and the episode it already
        // deleted returns with the abandoned transaction.
        let (held, _) = insert_creating(&conn, CleanupCompletion::DeleteRow);
        assert!(
            crate::db::mark_quarantined_from_state(
                &conn,
                &held,
                SandboxState::Creating,
                SandboxQuarantineSource::SbxManager,
                SandboxQuarantineReasonCode::CreateInterrupted,
                "create interrupted",
                SandboxQuarantineNextAction::RequiresOperatorAttention,
            )
            .unwrap()
        );
        backdate(&conn, &held);
        if target == SandboxState::Stopping {
            refuses(
                "quarantined create -> stopping",
                &conn,
                || supersede(&conn, &held, target),
                |result| {
                    matches!(
                        result,
                        Err(SupersedeActivationError::StateWriteNotApplied(error))
                            if error.outcome == SandboxStateWriteOutcome::BlockedByQuarantine
                    )
                },
            );
        } else {
            assert_accepts(
                "quarantined create -> deleting",
                &conn,
                || supersede(&conn, &held, target).unwrap(),
                ExpectedDiff::new()
                    .sandbox(&held, superseded(target))
                    .deleted("sandbox_cleanup_episodes", ("sandbox_id", &held)),
            );
        }

        // Automatic quarantine recovery's adopted episode yields only to a
        // deletion.
        let (adopted, _, claimed) = insert_claimed_adoptable(&conn);
        assert_eq!(
            adopt_quarantine_cleanup_episode(
                &conn,
                &claimed,
                adoptable(&claimed),
                generation(7),
                SandboxCleanupEpisodeId::new(),
                &execution_metadata(),
                &snapshot_path(&adopted),
            )
            .unwrap(),
            QuarantineAdoptionOutcome::Adopted
        );
        backdate(&conn, &adopted);
        if target == SandboxState::Stopping {
            refuses(
                "adopted recovery -> stopping",
                &conn,
                || supersede(&conn, &adopted, target),
                |result| {
                    matches!(
                        result,
                        Err(SupersedeActivationError::QuarantineRecoveryInProgress)
                    )
                },
            );
        } else {
            assert_accepts(
                "adopted recovery -> deleting",
                &conn,
                || supersede(&conn, &adopted, target).unwrap(),
                ExpectedDiff::new()
                    .sandbox(&adopted, superseded(target))
                    .deleted("sandbox_cleanup_episodes", ("sandbox_id", &adopted)),
            );
        }

        refuses(
            &format!("missing row -> {target}"),
            &conn,
            || supersede(&conn, &SandboxId::new().to_string(), target),
            |result| {
                matches!(
                    result,
                    Err(SupersedeActivationError::StateWriteNotApplied(error))
                        if error.outcome == SandboxStateWriteOutcome::Missing
                )
            },
        );
    }
}

/// A resident row as startup recovery observes it: a failure reason and an
/// activation receipt for `started`, with backdated timestamps.
fn insert_resident(conn: &Connection, state: SandboxState) -> String {
    let id = insert_row(conn, state);
    decorate(conn, &id, SandboxState::Started);
    id
}

fn settlement(target: SandboxState) -> StartupResidentSettlement<'static> {
    match target {
        SandboxState::Started => StartupResidentSettlement::Started,
        SandboxState::Paused => StartupResidentSettlement::Paused,
        _ => StartupResidentSettlement::Quarantined {
            reason_code: SandboxQuarantineReasonCode::RuntimeProbeInconsistent,
            message: "no live runtime",
        },
    }
}

#[test]
fn startup_resident_settlement_writes_the_observed_outcome() {
    let probe = SandboxQuarantineReasonCode::RuntimeProbeInconsistent;
    for observed_state in [
        SandboxState::Started,
        SandboxState::Paused,
        SandboxState::Resuming,
        SandboxState::Checkpointing,
    ] {
        for target in [
            SandboxState::Started,
            SandboxState::Paused,
            SandboxState::Quarantined,
        ] {
            let case = format!("{observed_state} settled {target}");
            let conn = test_conn();
            let id = insert_resident(&conn, observed_state);
            let observed = get_sandbox(&conn, &id).unwrap().unwrap();
            // The change time is stamped even when the state is unchanged.
            let mut cells = vec![
                ("state_changed_at", Cell::NowText),
                ("updated_at", Cell::NowText),
            ];
            if observed_state != target {
                cells.push(("state", text(target.as_str())));
            }
            if target != SandboxState::Started {
                cells.extend(receipt_cleared(SandboxState::Started));
            }
            let mut expected = ExpectedDiff::new();
            if target == SandboxState::Quarantined {
                cells.push(("failure_reason", text("no live runtime")));
                expected = expected
                    .inserted(
                        "sandbox_quarantine_state",
                        quarantine_state_entered(
                            &id,
                            observed_state.as_str(),
                            probe,
                            "no live runtime",
                        ),
                    )
                    .inserted(
                        "sandbox_quarantine_history",
                        quarantine_history_entered(&id, probe, "no live runtime"),
                    );
            } else {
                cells.push(("failure_reason", NULL));
            }
            assert_accepts(
                &case,
                &conn,
                || assert!(settle_startup_resident(&conn, &observed, settlement(target)).unwrap()),
                expected.sandbox(&id, cells),
            );
        }
    }
}

#[test]
fn startup_resident_settlement_refuses_any_change_to_the_observed_row() {
    let conn = test_conn();
    for state in SandboxState::all() {
        if matches!(
            state,
            SandboxState::Started
                | SandboxState::Paused
                | SandboxState::Resuming
                | SandboxState::Checkpointing
        ) {
            continue;
        }
        let id = insert_resident(&conn, state);
        let observed = get_sandbox(&conn, &id).unwrap().unwrap();
        refuses(
            &format!("observed {state}"),
            &conn,
            || settle_startup_resident(&conn, &observed, StartupResidentSettlement::Started),
            |result| matches!(result, Err(rusqlite::Error::InvalidQuery)),
        );
    }

    // Each change below supersedes the observation on its own.
    let perturbations: [(&str, &str); 17] = [
        (
            "version",
            "UPDATE sandboxes SET updated_at = '2' WHERE id = ?1",
        ),
        (
            "state",
            "UPDATE sandboxes SET state = 'paused' WHERE id = ?1",
        ),
        (
            "failure reason",
            "UPDATE sandboxes SET failure_reason = 'other' WHERE id = ?1",
        ),
        (
            "created_at",
            "UPDATE sandboxes SET created_at = '2' WHERE id = ?1",
        ),
        (
            "uuid8",
            "UPDATE sandboxes SET uuid8 = 'zzzzzzzz' WHERE id = ?1",
        ),
        ("uid", "UPDATE sandboxes SET uid = 2002 WHERE id = ?1"),
        ("slot", "UPDATE sandboxes SET slot = 2 WHERE id = ?1"),
        (
            "guest cid",
            "UPDATE sandboxes SET guest_cid = 4 WHERE id = ?1",
        ),
        ("pid", "UPDATE sandboxes SET pid = 4300 WHERE id = ?1"),
        (
            "spaced-host pid",
            "UPDATE sandboxes SET spaced_host_pid = 4301 WHERE id = ?1",
        ),
        (
            "gateway pid",
            "UPDATE sandboxes SET netgtw_pid = 4302 WHERE id = ?1",
        ),
        (
            "vsock path",
            "UPDATE sandboxes SET vsock_uds_path = 'other.sock' WHERE id = ?1",
        ),
        (
            "api socket",
            "UPDATE sandboxes SET api_socket_path = 'other.sock' WHERE id = ?1",
        ),
        (
            "route identity",
            "UPDATE sandboxes SET space_api_route_execution_generation = 8 WHERE id = ?1",
        ),
        (
            "process environment",
            "UPDATE sandboxes SET process_env_json = '{\"NEW\":\"1\"}' WHERE id = ?1",
        ),
        (
            "cleanup completion",
            "UPDATE sandboxes SET cleanup_completion = 'mark_stopped' WHERE id = ?1",
        ),
        (
            "cleanup keep",
            "UPDATE sandboxes SET cleanup_keep_filesystem = 1 WHERE id = ?1",
        ),
    ];
    for (case, perturbation) in perturbations {
        let id = insert_resident(&conn, SandboxState::Started);
        let observed = get_sandbox(&conn, &id).unwrap().unwrap();
        conn.execute(perturbation, params![id]).unwrap();
        refuses(
            case,
            &conn,
            || settle_startup_resident(&conn, &observed, settlement(SandboxState::Quarantined)),
            |result| matches!(result, Ok(false)),
        );
    }

    // A resident that pins an artifact, so each pin column can change alone.
    for (case, perturbation) in [
        (
            "artifact id",
            "UPDATE sandboxes SET current_local_artifact_id = '019a0000-0000-7000-8000-0000000000d3' WHERE id = ?1",
        ),
        (
            "artifact kind",
            "UPDATE sandboxes SET current_local_artifact_kind = 'disk' WHERE id = ?1",
        ),
        (
            "artifact origin",
            "UPDATE sandboxes SET current_local_artifact_origin = 'stop' WHERE id = ?1",
        ),
    ] {
        let id = insert_resident(&conn, SandboxState::Started);
        conn.execute(
            "UPDATE sandboxes
                SET current_local_artifact_id = '019a0000-0000-7000-8000-0000000000d1',
                    current_local_artifact_kind = 'full',
                    current_local_artifact_origin = 'suspend'
              WHERE id = ?1",
            params![id],
        )
        .unwrap();
        let observed = get_sandbox(&conn, &id).unwrap().unwrap();
        conn.execute(perturbation, params![id]).unwrap();
        refuses(
            case,
            &conn,
            || settle_startup_resident(&conn, &observed, settlement(SandboxState::Quarantined)),
            |result| matches!(result, Ok(false)),
        );
    }

    let id = insert_resident(&conn, SandboxState::Started);
    let observed = get_sandbox(&conn, &id).unwrap().unwrap();
    insert_cleanup_episode(
        &conn,
        &id,
        CleanupEpisodeSpec {
            id: SandboxCleanupEpisodeId::new(),
            completion: CleanupCompletion::MarkStopped,
            keep_filesystem: true,
            entered_from_state: Some(SandboxState::Stopped),
            activation_state: SandboxState::Starting,
            execution_generation: None,
            activation_attempt_id: None,
        },
    )
    .unwrap();
    // The episode insert also wrote its intent; restore the observed columns
    // so only the episode differs.
    conn.execute(
        "UPDATE sandboxes SET cleanup_completion = NULL, cleanup_keep_filesystem = NULL WHERE id = ?1",
        params![id],
    )
    .unwrap();
    refuses(
        "cleanup episode",
        &conn,
        || settle_startup_resident(&conn, &observed, settlement(SandboxState::Quarantined)),
        |result| matches!(result, Ok(false)),
    );

    let id = insert_resident(&conn, SandboxState::Started);
    let observed = get_sandbox(&conn, &id).unwrap().unwrap();
    let proof = endpoint_proof(id.parse().unwrap());
    conn.execute(
        "UPDATE sandboxes SET spaced_endpoint_proof_json = ?2 WHERE id = ?1",
        params![id, serde_json::to_string(&proof).unwrap()],
    )
    .unwrap();
    refuses(
        "endpoint proof",
        &conn,
        || settle_startup_resident(&conn, &observed, settlement(SandboxState::Quarantined)),
        |result| matches!(result, Ok(false)),
    );

    let id = insert_resident(&conn, SandboxState::Started);
    let observed = get_sandbox(&conn, &id).unwrap().unwrap();
    assert!(
        insert_restore_transfer_scope(
            &conn,
            &id,
            TransferScopeId::new(),
            None,
            None,
            &format!("/cache/restore-staging/{id}"),
        )
        .unwrap()
    );
    refuses(
        "restore transfer scope",
        &conn,
        || settle_startup_resident(&conn, &observed, settlement(SandboxState::Quarantined)),
        |result| matches!(result, Ok(false)),
    );

    let id = insert_resident(&conn, SandboxState::Started);
    let observed = get_sandbox(&conn, &id).unwrap().unwrap();
    conn.execute("DELETE FROM sandboxes WHERE id = ?1", params![id])
        .unwrap();
    refuses(
        "missing row",
        &conn,
        || settle_startup_resident(&conn, &observed, settlement(SandboxState::Started)),
        |result| matches!(result, Ok(false)),
    );
}

/// Adopt or insert `id` as the restored sandbox's own row in `state`.
fn restore_into(
    conn: &Connection,
    id: &str,
    state: SandboxState,
) -> Result<(), RestoreAdoptOrphanInsertError> {
    let source = restore_source();
    with_sandbox_params(
        id,
        state,
        |params| {
            params.runtime = RuntimeKind::Firecracker.alias();
            params.snapshots_enabled = true;
            params.rootfs_size_mb = Some(2048);
        },
        |params| {
            restore_adopt_orphan_or_insert_sandbox(
                conn,
                params,
                &RemoteRestoreOperation::metadata_staging(source),
            )
        },
    )
}

/// The synthetic quarantined row the orphan reconciler materializes, with the
/// timestamps adoption replaces backdated.
fn insert_synthetic_orphan(conn: &Connection) -> String {
    let id = SandboxId::new().to_string();
    with_sandbox_params(
        &id,
        SandboxState::Quarantined,
        as_synthetic_orphan,
        |params| {
            insert_quarantined_sandbox_in_transaction(
                conn,
                params,
                SandboxQuarantineSource::SbxManager,
                SandboxQuarantineReasonCode::OrphanedSandboxRowMissing,
                "orphaned cache",
                SandboxQuarantineNextAction::RequiresOperatorAttention,
            )
        },
    )
    .unwrap();
    backdate(conn, &id);
    conn.execute(
        "UPDATE sandboxes SET created_at = '1' WHERE id = ?1",
        params![id],
    )
    .unwrap();
    id
}

#[test]
fn orphan_restore_adoption_rewrites_only_a_synthetic_orphan() {
    let conn = test_conn();
    let id = insert_synthetic_orphan(&conn);
    // The quarantine episode stays for automatic recovery to retire, and the
    // orphan's node execution authority is kept.
    assert_accepts(
        "adopt the quarantined orphan",
        &conn,
        || restore_into(&conn, &id, SandboxState::Restoring).unwrap(),
        ExpectedDiff::new().sandbox(
            &id,
            [
                ("state", text("restoring")),
                ("template", text("seed")),
                ("template_hash", text("hash")),
                ("user", text("user")),
                ("snapshots_enabled", int(1)),
                ("rootfs_size_mb", int(2048)),
                ("failure_reason", NULL),
                ("restore_origin", text("remote")),
                ("restore_phase", text("metadata_staging")),
                ("restore_storage_uri", text(RESTORE_STORAGE_URI)),
                ("created_at", Cell::NowText),
                ("state_changed_at", Cell::NowText),
                ("updated_at", Cell::NowText),
            ],
        ),
    );

    let conn = test_conn();
    for state in SandboxState::all() {
        if state == SandboxState::Restoring {
            continue;
        }
        let id = insert_synthetic_orphan(&conn);
        refuses(
            &format!("insert as {state}"),
            &conn,
            || restore_into(&conn, &id, state),
            |result| {
                matches!(
                    result,
                    Err(RestoreAdoptOrphanInsertError::NonRestoringState { .. })
                )
            },
        );
    }

    let mut perturbations: Vec<(String, String)> = SandboxState::all()
        .into_iter()
        .filter(|state| !matches!(state, SandboxState::Quarantined | SandboxState::Restoring))
        .map(|state| {
            (
                format!("orphan in {state}"),
                format!(
                    "UPDATE sandboxes SET state = '{}' WHERE id = ?1",
                    state.as_str()
                ),
            )
        })
        .collect();
    for (case, sql) in [
        (
            "no quarantine marker",
            "DELETE FROM sandbox_quarantine_state WHERE sandbox_id = ?1",
        ),
        (
            "another quarantine source",
            "UPDATE sandbox_quarantine_state SET source = 'operator' WHERE sandbox_id = ?1",
        ),
        (
            "another quarantine reason",
            "UPDATE sandbox_quarantine_state SET reason_code = 'pause_failed' WHERE sandbox_id = ?1",
        ),
        (
            "real template",
            "UPDATE sandboxes SET template = 'seed' WHERE id = ?1",
        ),
        (
            "template hash",
            "UPDATE sandboxes SET template_hash = 'hash' WHERE id = ?1",
        ),
        (
            "real owner",
            "UPDATE sandboxes SET user = 'user' WHERE id = ?1",
        ),
        (
            "another runtime",
            "UPDATE sandboxes SET runtime = 'other' WHERE id = ?1",
        ),
        ("uid", "UPDATE sandboxes SET uid = 2001 WHERE id = ?1"),
        ("slot", "UPDATE sandboxes SET slot = 1 WHERE id = ?1"),
        (
            "guest cid",
            "UPDATE sandboxes SET guest_cid = 3 WHERE id = ?1",
        ),
        ("pid", "UPDATE sandboxes SET pid = 4242 WHERE id = ?1"),
        (
            "spaced-host pid",
            "UPDATE sandboxes SET spaced_host_pid = 4243 WHERE id = ?1",
        ),
        (
            "gateway pid",
            "UPDATE sandboxes SET netgtw_pid = 4244 WHERE id = ?1",
        ),
        (
            "vsock path",
            "UPDATE sandboxes SET vsock_uds_path = 'vsock.sock' WHERE id = ?1",
        ),
        (
            "api socket",
            "UPDATE sandboxes SET api_socket_path = 'api.sock' WHERE id = ?1",
        ),
        (
            "snapshots enabled",
            "UPDATE sandboxes SET snapshots_enabled = 1 WHERE id = ?1",
        ),
        (
            "pool template",
            "UPDATE sandboxes SET pool_template = 'pool' WHERE id = ?1",
        ),
        (
            "guest uptime",
            "UPDATE sandboxes SET guest_uptime_secs = 9 WHERE id = ?1",
        ),
        (
            "process environment",
            "UPDATE sandboxes SET process_env_json = '{\"A\":\"1\"}' WHERE id = ?1",
        ),
        (
            "auto-lifecycle policy",
            "UPDATE sandboxes SET auto_lifecycle_json = '{\"wake\":{\"on\":[]}}' WHERE id = ?1",
        ),
        (
            "cleanup completion",
            "UPDATE sandboxes SET cleanup_completion = 'delete_row' WHERE id = ?1",
        ),
        (
            "cleanup keep",
            "UPDATE sandboxes SET cleanup_keep_filesystem = 0 WHERE id = ?1",
        ),
        (
            "applied network policy generation",
            "UPDATE sandboxes SET applied_network_policy_generation = 1 WHERE id = ?1",
        ),
        (
            "applied network policy source",
            "UPDATE sandboxes SET applied_network_policy_source = 'cluster' WHERE id = ?1",
        ),
        (
            "local network policy authority",
            "UPDATE sandboxes SET network_policy_authority = 'local' WHERE id = ?1",
        ),
        (
            "local auto-lifecycle authority",
            "UPDATE sandboxes SET auto_lifecycle_authority = 'local' WHERE id = ?1",
        ),
    ] {
        perturbations.push((case.to_owned(), sql.to_owned()));
    }
    for (case, sql) in perturbations {
        let id = insert_synthetic_orphan(&conn);
        conn.execute(&sql, params![id]).unwrap();
        refuses(
            &case,
            &conn,
            || restore_into(&conn, &id, SandboxState::Restoring),
            |result| {
                matches!(
                    result,
                    Err(RestoreAdoptOrphanInsertError::ExistingRowNotSyntheticOrphan { .. })
                )
            },
        );
    }
}

fn adopt(
    conn: &Connection,
    claimed: &crate::db::ClaimedQuarantineRecoveryCandidate,
    id: &str,
) -> QuarantineAdoptionOutcome {
    adopt_quarantine_cleanup_episode(
        conn,
        claimed,
        adoptable(claimed),
        generation(7),
        SandboxCleanupEpisodeId::new(),
        &execution_metadata(),
        &snapshot_path(id),
    )
    .unwrap()
}

#[test]
fn quarantine_adoption_binds_the_quarantined_full_suspend_pin_and_generation() {
    let message = "automatic recovery adopted validated suspend evidence";
    for generation_seeded in [true, false] {
        let conn = test_conn();
        let (id, artifact, claimed) = insert_claimed_adoptable(&conn);
        if !generation_seeded {
            conn.execute(
                "UPDATE sandboxes SET space_api_route_execution_generation = NULL WHERE id = ?1",
                params![id],
            )
            .unwrap();
        }
        backdate(&conn, &id);
        conn.execute(
            "UPDATE local_artifacts SET last_touched_at = '1' WHERE sandbox_id = ?1",
            params![id],
        )
        .unwrap();
        conn.execute(
            "UPDATE sandbox_quarantine_state SET updated_at = '1' WHERE sandbox_id = ?1",
            params![id],
        )
        .unwrap();
        let mut cells = vec![
            ("state", text("cleanup_required")),
            ("cleanup_completion", text("mark_suspended")),
            ("cleanup_keep_filesystem", int(1)),
            ("failure_reason", text(message)),
            ("execution_metadata_json", Cell::Fresh),
            ("state_changed_at", Cell::NowText),
            ("updated_at", Cell::NowText),
        ];
        if !generation_seeded {
            cells.push(("space_api_route_execution_generation", int(7)));
        }
        let cleanup_id = SandboxCleanupEpisodeId::new();
        let reason = SandboxQuarantineReasonCode::RestoreFirecrackerLaunchFailed;
        assert_accepts(
            &format!("generation seeded {generation_seeded}"),
            &conn,
            || {
                assert_eq!(
                    adopt_quarantine_cleanup_episode(
                        &conn,
                        &claimed,
                        adoptable(&claimed),
                        generation(7),
                        cleanup_id,
                        &execution_metadata(),
                        &snapshot_path(&id),
                    )
                    .unwrap(),
                    QuarantineAdoptionOutcome::Adopted
                )
            },
            ExpectedDiff::new()
                .sandbox(&id, cells)
                .updated(
                    "local_artifacts",
                    ("local_artifact_id", &artifact.to_string()),
                    [("last_touched_at", Cell::NowText)],
                )
                .updated(
                    "sandbox_quarantine_state",
                    ("sandbox_id", &id),
                    [
                        ("claim_token", NULL),
                        ("claim_expires_at", NULL),
                        ("updated_at", Cell::NowText),
                    ],
                )
                .inserted(
                    "sandbox_cleanup_episodes",
                    vec![
                        ("sandbox_id", text(&id)),
                        ("cleanup_id", text(cleanup_id.to_string())),
                        ("completion", text("mark_suspended")),
                        ("keep_filesystem", int(1)),
                        ("phase", text("requested")),
                        ("entered_from_state", text("quarantined")),
                        ("activation_state", text("restoring")),
                        ("requested_at", Cell::NowInteger),
                        ("quarantine_reason_code", text(reason.as_str())),
                        ("failure_message", text(message)),
                        ("last_cleanup_error", NULL),
                        ("execution_generation", NULL),
                        ("activation_attempt_id", NULL),
                        ("attempt_count", int(0)),
                        ("created_at", Cell::NowInteger),
                        ("recovery_local_artifact_id", text(artifact.to_string())),
                        ("recovery_execution_generation", int(7)),
                        ("prepared_clone_root", NULL),
                        ("operation_activation_attempt_id", NULL),
                    ],
                ),
        );
    }

    // The claim's fence conjuncts are swept by
    // `every_claim_fenced_statement_pins_every_conjunct`; these are the
    // statement's own sandbox predicates. Every other source state keeps the
    // rest of the shape, and no restore columns, so only the state guard can
    // refuse it.
    let conn = test_conn();
    for state in SandboxState::all() {
        if state == SandboxState::Quarantined {
            continue;
        }
        let (id, _, claimed) = insert_claimed_adoptable(&conn);
        conn.execute(
            "UPDATE sandboxes SET state = ?2 WHERE id = ?1",
            params![id, state.as_str()],
        )
        .unwrap();
        refuses(
            state.as_str(),
            &conn,
            || adopt(&conn, &claimed, &id),
            |outcome| *outcome == QuarantineAdoptionOutcome::FenceChanged,
        );
    }
    for (case, sql) in [
        (
            "another pin",
            "UPDATE sandboxes SET current_local_artifact_id = '019a0000-0000-7000-8000-0000000000d2' WHERE id = ?1",
        ),
        (
            "a disk pin",
            "UPDATE sandboxes SET current_local_artifact_kind = 'disk' WHERE id = ?1",
        ),
        (
            "a stop pin",
            "UPDATE sandboxes SET current_local_artifact_origin = 'stop' WHERE id = ?1",
        ),
        (
            "another generation",
            "UPDATE sandboxes SET space_api_route_execution_generation = 8 WHERE id = ?1",
        ),
    ] {
        let (id, _, claimed) = insert_claimed_adoptable(&conn);
        conn.execute(sql, params![id]).unwrap();
        refuses(
            case,
            &conn,
            || adopt(&conn, &claimed, &id),
            |outcome| *outcome == QuarantineAdoptionOutcome::FenceChanged,
        );
    }
}

#[test]
fn suspended_cleanup_pin_reuses_a_staged_identity_and_keeps_its_backup_marker() {
    let conn = test_conn();
    let id = insert_row(&conn, SandboxState::Paused);
    begin_teardown_suspend(&conn, &id, generation(7)).unwrap();
    let staged = LocalArtifactId::new();
    stage_teardown_suspend_artifact(&conn, &id, generation(7), staged, "/cache/staged/snapshot")
        .unwrap();
    mark_local_artifact_backup_completed(&conn, &staged.to_string()).unwrap();
    set_failure_reason(&conn, &id, "seeded failure");
    seed_receipt(&conn, &id, SandboxState::Started);
    backdate(&conn, &id);
    conn.execute(
        "UPDATE local_artifacts SET last_touched_at = '1' WHERE local_artifact_id = ?1",
        params![staged.to_string()],
    )
    .unwrap();
    assert_accepts(
        "staged identity",
        &conn,
        || pin_suspended_artifact_for_cleanup(&conn, &id, staged, &snapshot_path(&id)).unwrap(),
        ExpectedDiff::new()
            .sandbox(
                &id,
                [
                    ("state", text("cleanup_required")),
                    ("cleanup_completion", text("mark_suspended")),
                    ("cleanup_keep_filesystem", int(1)),
                    ("failure_reason", NULL),
                    ("state_changed_at", Cell::NowText),
                    ("updated_at", Cell::NowText),
                ],
            )
            .updated(
                "local_artifacts",
                ("local_artifact_id", &staged.to_string()),
                [
                    ("local_path", text(snapshot_path(&id))),
                    ("last_touched_at", Cell::NowText),
                ],
            ),
    );
}
