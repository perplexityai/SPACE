//! The complete row diff each single-statement compare-and-set writer of the
//! node-local SQLite `sandboxes.state` makes when it accepts.
//!
//! `state_writer_refusal` pins what these writers refuse. Here each accepting
//! case seeds the side columns the writer might touch (failure reason,
//! transition receipt, runtime, endpoint proof and route identity, artifact
//! pins, an active teardown-suspend job) and asserts the whole-database diff,
//! so moving a writer onto a shared helper must stay column-identical. Seeded
//! rows carry a disabled auto-lifecycle policy, so arming after activation
//! writes nothing here; `started_transition_arms_disarmed_lifecycle_for_rollback_writers`
//! and `start_arms_access_idle_only_after_guest_readiness` cover arming.

use pplx_space_common_core::types::{
    ArtifactKind, ArtifactOrigin, LocalArtifactId, SandboxId, SandboxQuarantineNextAction,
    SandboxQuarantineReasonCode, SandboxQuarantineSource, TransferScopeId,
};
use rusqlite::Connection;

use super::state_write_fixtures::{
    Cell, ExpectedDiff, NULL, assert_accepts, backdate, decorate, generation, insert_row,
    insert_suspended_with_artifact, local_artifact_inserted, pinned, restore_source,
    runtime_cleared, seed_receipt, seed_runtime, seed_teardown_job, set_failure_reason,
    set_suspend_pin, snapshot_path, suspend_artifact_inserted, teardown_job_failed, text, unpinned,
    with_sandbox_params,
};

/// The failure an eviction claim records on the teardown job it closes.
const DELETING: &str = "sandbox is deleting";
use crate::db::{
    CleanupCompletion, PauseFailedRecoveryTarget, begin_suspend, begin_teardown_suspend,
    claim_failed_restoring_sandbox_for_eviction, claim_quarantined_sandbox_for_eviction,
    claim_stopped_sandbox_for_eviction, claim_suspended_sandbox_for_eviction,
    claim_teardown_suspend_failed_sandbox_for_eviction, fail_teardown_suspend_backup_operation,
    insert_local_artifact, insert_restore_transfer_scope, mark_cleanup_required,
    mark_cross_node_restore_staging_failed, mark_quarantined_from_state,
    pin_suspended_artifact_for_cleanup, publish_restored_disk_as_new,
    publish_restored_suspended_artifact, publish_restored_suspended_without_artifact,
    publish_stopped_artifact, recover_interrupted_pause_state,
    restore_adopt_orphan_or_insert_sandbox, restore_checkpoint_state,
    restore_resident_resume_state, rollback_suspending_to_paused, set_cleanup_intent,
    stage_teardown_suspend_artifact, test_conn,
};
use crate::restore_operation::RemoteRestoreOperation;
use crate::state::SandboxState;

fn deleting_claimed() -> Vec<(&'static str, Cell)> {
    vec![
        ("state", text("deleting")),
        ("state_changed_at", Cell::NowText),
        ("updated_at", Cell::NowText),
    ]
}

/// A cross-node restore staging its artifact, with a transfer scope and a
/// stale suspend-origin artifact row left by an earlier suspend on this node.
fn insert_remote_restore(conn: &Connection) -> (String, LocalArtifactId) {
    let id = SandboxId::new().to_string();
    let source = restore_source();
    with_sandbox_params(
        &id,
        SandboxState::Restoring,
        |_| {},
        |params| {
            restore_adopt_orphan_or_insert_sandbox(
                conn,
                params,
                &RemoteRestoreOperation::artifact_staging(source),
            )
        },
    )
    .unwrap();
    assert!(
        insert_restore_transfer_scope(
            conn,
            &id,
            TransferScopeId::new(),
            None,
            None,
            &format!("/cache/restore-staging/{id}"),
        )
        .unwrap()
    );
    let stale = LocalArtifactId::new();
    insert_local_artifact(
        conn,
        &id,
        stale,
        ArtifactKind::Full,
        ArtifactOrigin::Suspend,
        None,
        &snapshot_path(&id),
    )
    .unwrap();
    (id, stale)
}

#[test]
fn eviction_claims_move_the_row_to_deleting_and_close_its_teardown_job() {
    let conn = test_conn();

    let stopped = insert_row(&conn, SandboxState::Stopped);
    seed_teardown_job(&conn, &stopped);
    decorate(&conn, &stopped, SandboxState::Started);
    assert_accepts(
        "stopped",
        &conn,
        || assert!(claim_stopped_sandbox_for_eviction(&conn, &stopped, "1").unwrap()),
        teardown_job_failed(&stopped, DELETING).sandbox(&stopped, deleting_claimed()),
    );

    let quarantined = insert_row(&conn, SandboxState::Started);
    assert!(
        mark_quarantined_from_state(
            &conn,
            &quarantined,
            SandboxState::Started,
            SandboxQuarantineSource::SbxManager,
            SandboxQuarantineReasonCode::RuntimeProbeInconsistent,
            "runtime probe inconsistent",
            SandboxQuarantineNextAction::RequiresOperatorAttention,
        )
        .unwrap()
    );
    seed_teardown_job(&conn, &quarantined);
    decorate(&conn, &quarantined, SandboxState::Started);
    assert_accepts(
        "quarantined",
        &conn,
        || assert!(claim_quarantined_sandbox_for_eviction(&conn, &quarantined, "1").unwrap()),
        teardown_job_failed(&quarantined, DELETING).sandbox(&quarantined, deleting_claimed()),
    );

    let (failed_restore, _) = insert_remote_restore(&conn);
    assert!(
        mark_cross_node_restore_staging_failed(&conn, &failed_restore, "staging failed").unwrap()
    );
    seed_teardown_job(&conn, &failed_restore);
    seed_receipt(&conn, &failed_restore, SandboxState::Started);
    backdate(&conn, &failed_restore);
    let mut cells = deleting_claimed();
    cells.extend([
        ("restore_origin", NULL),
        ("restore_phase", NULL),
        ("restore_storage_uri", NULL),
    ]);
    assert_accepts(
        "failed restoring",
        &conn,
        || {
            assert!(
                claim_failed_restoring_sandbox_for_eviction(&conn, &failed_restore, "1").unwrap()
            )
        },
        teardown_job_failed(&failed_restore, DELETING).sandbox(&failed_restore, cells),
    );

    // Each pin and version combination is its own statement.
    for (pinned_claim, versioned) in [(true, true), (true, false), (false, true), (false, false)] {
        let case = format!("suspended: pin {pinned_claim}, version {versioned}");
        let (id, artifact) = if pinned_claim {
            insert_suspended_with_artifact(&conn)
        } else {
            let id = insert_row(&conn, SandboxState::Suspended);
            (id, LocalArtifactId::new())
        };
        seed_teardown_job(&conn, &id);
        decorate(&conn, &id, SandboxState::Started);
        let pin = pinned_claim.then(|| artifact.to_string());
        assert_accepts(
            &case,
            &conn,
            || {
                assert!(
                    claim_suspended_sandbox_for_eviction(
                        &conn,
                        &id,
                        pin.as_deref(),
                        versioned.then_some("1"),
                    )
                    .unwrap()
                )
            },
            teardown_job_failed(&id, DELETING).sandbox(&id, deleting_claimed()),
        );
    }

    // The terminal backup refusal already failed the row's teardown job.
    let stuck = insert_row(&conn, SandboxState::Paused);
    begin_teardown_suspend(&conn, &stuck, generation(7)).unwrap();
    let staged = LocalArtifactId::new();
    stage_teardown_suspend_artifact(&conn, &stuck, generation(7), staged, &snapshot_path(&stuck))
        .unwrap();
    assert!(fail_teardown_suspend_backup_operation(&conn, &stuck, generation(7), staged).unwrap());
    seed_receipt(&conn, &stuck, SandboxState::Started);
    backdate(&conn, &stuck);
    assert_accepts(
        "terminal teardown suspend",
        &conn,
        || {
            assert!(
                claim_teardown_suspend_failed_sandbox_for_eviction(&conn, &stuck, staged, "1")
                    .unwrap()
            )
        },
        ExpectedDiff::new().sandbox(&stuck, deleting_claimed()),
    );
}

#[test]
fn suspend_begin_and_rollback_move_the_artifact_pin_through_the_predecessor() {
    let conn = test_conn();
    let id = insert_row(&conn, SandboxState::Paused);
    let pin = set_suspend_pin(&conn, &id);
    decorate(&conn, &id, SandboxState::Started);
    let mut cells = vec![
        ("state", text("suspending")),
        ("suspend_predecessor_artifact_id", text(pin.to_string())),
        ("failure_reason", NULL),
        ("state_changed_at", Cell::NowText),
        ("updated_at", Cell::NowText),
    ];
    cells.extend(unpinned());
    assert_accepts(
        "begin suspend",
        &conn,
        || assert!(begin_suspend(&conn, &id).unwrap()),
        ExpectedDiff::new().sandbox(&id, cells),
    );

    assert!(set_cleanup_intent(&conn, &id, true, CleanupCompletion::MarkSuspended).unwrap());
    set_failure_reason(&conn, &id, "suspend failed");
    backdate(&conn, &id);
    let mut cells = vec![
        ("state", text("paused")),
        ("suspend_predecessor_artifact_id", NULL),
        ("cleanup_completion", NULL),
        ("cleanup_keep_filesystem", NULL),
        ("failure_reason", NULL),
        ("state_changed_at", Cell::NowText),
        ("updated_at", Cell::NowText),
    ];
    cells.extend(pinned(pin, "full", "suspend"));
    assert_accepts(
        "suspend rollback",
        &conn,
        || assert!(rollback_suspending_to_paused(&conn, &id).unwrap()),
        ExpectedDiff::new().sandbox(&id, cells),
    );
}

#[test]
fn resident_settlements_write_their_target_and_clear_only_the_failure_reason() {
    for target in [SandboxState::Started, SandboxState::Paused] {
        let conn = test_conn();
        let settled = |state: SandboxState| {
            vec![
                ("state", text(state.as_str())),
                ("failure_reason", NULL),
                ("state_changed_at", Cell::NowText),
                ("updated_at", Cell::NowText),
            ]
        };

        let checkpointing = insert_row(&conn, SandboxState::Checkpointing);
        decorate(&conn, &checkpointing, SandboxState::Stopped);
        assert_accepts(
            &format!("checkpoint rollback to {target}"),
            &conn,
            || assert!(restore_checkpoint_state(&conn, &checkpointing, target).unwrap()),
            ExpectedDiff::new().sandbox(&checkpointing, settled(target)),
        );

        let pausing = insert_row(&conn, SandboxState::Pausing);
        decorate(&conn, &pausing, SandboxState::Stopped);
        let recovery = match target {
            SandboxState::Started => PauseFailedRecoveryTarget::Started,
            _ => PauseFailedRecoveryTarget::Paused,
        };
        assert_accepts(
            &format!("interrupted pause to {target}"),
            &conn,
            || assert!(recover_interrupted_pause_state(&conn, &pausing, recovery).unwrap()),
            ExpectedDiff::new().sandbox(&pausing, settled(target)),
        );

        let resuming = insert_row(&conn, SandboxState::Resuming);
        decorate(&conn, &resuming, SandboxState::Stopped);
        assert_accepts(
            &format!("resident resume rollback to {target}"),
            &conn,
            || assert!(restore_resident_resume_state(&conn, &resuming, target).unwrap()),
            ExpectedDiff::new().sandbox(&resuming, settled(target)),
        );
    }
}

#[test]
fn restored_publications_settle_the_restore_and_retire_its_transfer_scope() {
    let conn = test_conn();

    let (with_artifact, stale) = insert_remote_restore(&conn);
    decorate(&conn, &with_artifact, SandboxState::Started);
    let artifact = LocalArtifactId::new();
    let mut cells = vec![
        ("state", text("suspended")),
        ("restore_origin", NULL),
        ("restore_phase", NULL),
        ("restore_storage_uri", NULL),
        ("state_changed_at", Cell::NowText),
        ("updated_at", Cell::NowText),
    ];
    cells.extend(pinned(artifact, "full", "suspend"));
    assert_accepts(
        "suspended with artifact",
        &conn,
        || {
            publish_restored_suspended_artifact(
                &conn,
                &with_artifact,
                artifact,
                &snapshot_path(&with_artifact),
            )
            .unwrap()
        },
        ExpectedDiff::new()
            .sandbox(&with_artifact, cells)
            .deleted("local_artifacts", ("local_artifact_id", &stale.to_string()))
            .inserted(
                "local_artifacts",
                suspend_artifact_inserted(&with_artifact, artifact),
            )
            .deleted(
                "sandbox_restore_transfer_scopes",
                ("sandbox_id", &with_artifact),
            ),
    );

    let (without_artifact, _) = insert_remote_restore(&conn);
    decorate(&conn, &without_artifact, SandboxState::Started);
    assert_accepts(
        "suspended without artifact",
        &conn,
        || publish_restored_suspended_without_artifact(&conn, &without_artifact).unwrap(),
        ExpectedDiff::new()
            .sandbox(
                &without_artifact,
                [
                    ("state", text("suspended")),
                    ("failure_reason", NULL),
                    ("restore_origin", NULL),
                    ("restore_phase", NULL),
                    ("restore_storage_uri", NULL),
                    ("state_changed_at", Cell::NowText),
                    ("updated_at", Cell::NowText),
                ],
            )
            .deleted(
                "sandbox_restore_transfer_scopes",
                ("sandbox_id", &without_artifact),
            ),
    );

    let (disk_as_new, _) = insert_remote_restore(&conn);
    seed_runtime(&conn, &disk_as_new);
    decorate(&conn, &disk_as_new, SandboxState::Started);
    let mut cells = vec![
        ("state", text("stopped")),
        ("failure_reason", NULL),
        ("restore_origin", NULL),
        ("restore_phase", NULL),
        ("restore_storage_uri", NULL),
        ("state_changed_at", Cell::NowText),
        ("updated_at", Cell::NowText),
    ];
    cells.extend(runtime_cleared(&[]));
    assert_accepts(
        "disk as new",
        &conn,
        || publish_restored_disk_as_new(&conn, &disk_as_new).unwrap(),
        ExpectedDiff::new().sandbox(&disk_as_new, cells).deleted(
            "sandbox_restore_transfer_scopes",
            ("sandbox_id", &disk_as_new),
        ),
    );
}

#[test]
fn stopped_publication_pins_the_disk_and_clears_the_runtime_from_each_accepted_shape() {
    let conn = test_conn();
    let stopping = insert_row(&conn, SandboxState::Stopping);
    assert!(set_cleanup_intent(&conn, &stopping, true, CleanupCompletion::MarkStopped).unwrap());
    let cleanup_required = insert_row(&conn, SandboxState::Stopping);
    mark_cleanup_required(
        &conn,
        &cleanup_required,
        true,
        CleanupCompletion::MarkStopped,
        "stop cleanup",
    )
    .unwrap();
    let stopped = insert_row(&conn, SandboxState::Stopped);
    for (case, id, intent) in [
        ("stopping", &stopping, true),
        ("cleanup_required", &cleanup_required, true),
        ("unpinned stopped", &stopped, false),
    ] {
        seed_runtime(&conn, id);
        decorate(&conn, id, SandboxState::Started);
        let artifact = LocalArtifactId::new();
        let path = format!("/cache/{id}/disk");
        let mut cells = vec![("failure_reason", NULL), ("updated_at", Cell::NowText)];
        if case != "unpinned stopped" {
            cells.push(("state", text("stopped")));
            cells.push(("state_changed_at", Cell::NowText));
        }
        if intent {
            cells.push(("cleanup_completion", NULL));
            cells.push(("cleanup_keep_filesystem", NULL));
        }
        cells.extend(pinned(artifact, "disk", "stop"));
        cells.extend(runtime_cleared(&[]));
        assert_accepts(
            case,
            &conn,
            || publish_stopped_artifact(&conn, id, artifact, &path).unwrap(),
            ExpectedDiff::new().sandbox(id, cells).inserted(
                "local_artifacts",
                local_artifact_inserted(id, artifact, "disk", "stop", &path),
            ),
        );
    }
}

#[test]
fn suspended_cleanup_pin_hands_the_staged_artifact_to_suspend_cleanup() {
    let conn = test_conn();
    let id = insert_row(&conn, SandboxState::Paused);
    let predecessor = set_suspend_pin(&conn, &id);
    assert!(begin_suspend(&conn, &id).unwrap());
    let stale = LocalArtifactId::new();
    insert_local_artifact(
        &conn,
        &id,
        stale,
        ArtifactKind::Full,
        ArtifactOrigin::Suspend,
        None,
        &snapshot_path(&id),
    )
    .unwrap();
    decorate(&conn, &id, SandboxState::Started);
    let artifact = LocalArtifactId::new();
    let mut cells = vec![
        ("state", text("cleanup_required")),
        ("suspend_predecessor_artifact_id", NULL),
        ("cleanup_completion", text("mark_suspended")),
        (
            "cleanup_keep_filesystem",
            Cell::Is(rusqlite::types::Value::Integer(1)),
        ),
        ("failure_reason", NULL),
        ("state_changed_at", Cell::NowText),
        ("updated_at", Cell::NowText),
    ];
    cells.extend(pinned(artifact, "full", "suspend"));
    assert_ne!(predecessor, artifact);
    assert_accepts(
        "suspending",
        &conn,
        || pin_suspended_artifact_for_cleanup(&conn, &id, artifact, &snapshot_path(&id)).unwrap(),
        ExpectedDiff::new()
            .sandbox(&id, cells)
            .deleted("local_artifacts", ("local_artifact_id", &stale.to_string()))
            .inserted("local_artifacts", suspend_artifact_inserted(&id, artifact)),
    );
}
