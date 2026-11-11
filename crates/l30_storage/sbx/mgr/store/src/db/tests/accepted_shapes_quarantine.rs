//! The complete row diff each quarantine writer of the node-local SQLite
//! `sandboxes.state` makes when it accepts: entering quarantine, recovering a
//! pause failure, and the two terminal steps of automatic quarantine recovery,
//! orphan publication and the adopted cleanup episode's completion.
//!
//! Each case seeds the side columns and `sandbox_quarantine_*` rows the writer
//! might touch and asserts the whole-database diff, so moving a writer onto a
//! shared helper must stay column-identical. `thin_writer_coverage` covers the
//! adoption statement itself.

use crate::db::Connection;
use pplx_space_common_core::types::{
    ArtifactKind, ArtifactOrigin, LocalArtifactId, SandboxCleanupEpisodeId, SandboxId,
    SandboxQuarantineNextAction, SandboxQuarantineReasonCode, SandboxQuarantineSource,
    TransferScopeId,
};
use rusqlite::params;

use super::state_write_fixtures::{
    Cell, ExpectedDiff, NULL, as_synthetic_orphan, assert_accepts, backdate, decorate,
    execution_metadata, generation, insert_row, int, pinned, quarantine_history_entered,
    quarantine_state_entered, receipt_cleared, restore_source, seed_receipt, seed_runtime,
    seed_teardown_job, set_suspend_pin, snapshot_path, suspend_artifact_inserted,
    teardown_job_failed, text, with_sandbox_params,
};
use crate::db::{
    ClaimedQuarantineRecoveryCandidate, PauseFailedRecoveryTarget, PlanRejectionRunEffect,
    QuarantineAdoptionOutcome, QuarantineRecoveryBranch, adopt_quarantine_cleanup_episode,
    claim_quarantine_recovery_candidate, complete_cleanup_episode_for_job, immediate_transaction,
    insert_local_artifact, insert_quarantined_sandbox_in_transaction,
    insert_restore_transfer_scope, mark_cross_node_restore_staging_failed_under_recovery_claim,
    mark_quarantined_from_state, publish_orphan_quarantine_recovered_artifact,
    recover_pause_failed_quarantine, restore_adopt_orphan_or_insert_sandbox_in_transaction,
    scan_quarantine_recovery_candidates, set_orphan_quarantine_recovery_generation, test_conn,
};
use crate::restore_operation::RemoteRestoreOperation;
use crate::state::SandboxState;

fn quarantine(
    conn: &Connection,
    id: &str,
    from: SandboxState,
    reason: SandboxQuarantineReasonCode,
    message: &str,
) {
    assert!(
        mark_quarantined_from_state(
            conn,
            id,
            from,
            SandboxQuarantineSource::SbxManager,
            reason,
            message,
            SandboxQuarantineNextAction::RequiresOperatorAttention,
        )
        .unwrap()
    );
}

/// The quarantine episode the only recovery candidate on `conn` belongs to,
/// claimed under a live lease.
fn claim_only_candidate(conn: &Connection) -> ClaimedQuarantineRecoveryCandidate {
    let [candidate] = scan_quarantine_recovery_candidates(conn)
        .unwrap()
        .candidates
        .try_into()
        .unwrap();
    claim_quarantine_recovery_candidate(conn, &candidate, PlanRejectionRunEffect::Leaves)
        .unwrap()
        .unwrap()
}

fn quarantine_id(conn: &Connection, id: &str) -> String {
    conn.query_row(
        "SELECT quarantine_id FROM sandbox_quarantine_state WHERE sandbox_id = ?1",
        params![id],
        |row| row.get(0),
    )
    .unwrap()
}

#[test]
fn entering_quarantine_records_the_episode_and_clears_transition_evidence() {
    let conn = test_conn();
    let reason = SandboxQuarantineReasonCode::RuntimeProbeInconsistent;

    let started = insert_row(&conn, SandboxState::Started);
    seed_runtime(&conn, &started);
    set_suspend_pin(&conn, &started);
    conn.execute(
        "UPDATE sandboxes
            SET suspend_predecessor_artifact_id = current_local_artifact_id
          WHERE id = ?1",
        params![started],
    )
    .unwrap();
    seed_teardown_job(&conn, &started);
    decorate(&conn, &started, SandboxState::Started);
    // The runtime, the route identity and the artifact pin stay: quarantine
    // preserves uncertain resources for an operator.
    let mut cells = vec![
        ("state", text("quarantined")),
        ("failure_reason", text("probe failed")),
        ("suspend_predecessor_artifact_id", NULL),
        ("state_changed_at", Cell::NowText),
        ("updated_at", Cell::NowText),
    ];
    cells.extend(receipt_cleared(SandboxState::Started));
    assert_accepts(
        "from started",
        &conn,
        || {
            quarantine(
                &conn,
                &started,
                SandboxState::Started,
                reason,
                "probe failed",
            )
        },
        teardown_job_failed(&started, "sandbox is quarantined")
            .sandbox(&started, cells)
            .inserted(
                "sandbox_quarantine_state",
                quarantine_state_entered(&started, "started", reason, "probe failed"),
            )
            .inserted(
                "sandbox_quarantine_history",
                quarantine_history_entered(&started, reason, "probe failed"),
            ),
    );

    let restoring = insert_row(&conn, SandboxState::Restoring);
    conn.execute(
        "UPDATE sandboxes SET restore_origin = 'local', restore_phase = 'activation' WHERE id = ?1",
        params![restoring],
    )
    .unwrap();
    decorate(&conn, &restoring, SandboxState::Stopped);
    let launch = SandboxQuarantineReasonCode::RestoreFirecrackerLaunchFailed;
    let mut cells = vec![
        ("state", text("quarantined")),
        ("failure_reason", text("launch failed")),
        ("restore_origin", NULL),
        ("restore_phase", NULL),
        ("state_changed_at", Cell::NowText),
        ("updated_at", Cell::NowText),
    ];
    cells.extend(receipt_cleared(SandboxState::Stopped));
    assert_accepts(
        "from restoring",
        &conn,
        || {
            quarantine(
                &conn,
                &restoring,
                SandboxState::Restoring,
                launch,
                "launch failed",
            )
        },
        ExpectedDiff::new()
            .sandbox(&restoring, cells)
            .inserted(
                "sandbox_quarantine_state",
                quarantine_state_entered(&restoring, "restoring", launch, "launch failed"),
            )
            .inserted(
                "sandbox_quarantine_history",
                quarantine_history_entered(&restoring, launch, "launch failed"),
            ),
    );
}

fn history_recorded(
    id: &str,
    quarantine_id: &str,
    action: &str,
    reason: SandboxQuarantineReasonCode,
    message: &str,
) -> Vec<(&'static str, Cell)> {
    vec![
        ("id", Cell::Fresh),
        ("quarantine_id", text(quarantine_id)),
        ("sandbox_id", text(id)),
        ("occurred_at", Cell::NowText),
        ("source", text("sbx_manager")),
        ("action", text(action)),
        ("reason_code", text(reason.as_str())),
        ("message", text(message)),
        ("next_action", NULL),
        ("next_action_after", NULL),
        ("attempt_count", NULL),
        ("claim_token", NULL),
        ("evidence_json", text("{}")),
        ("error", NULL),
    ]
}

#[test]
fn pause_failure_recovery_settles_the_row_and_retires_its_episode() {
    let reason = SandboxQuarantineReasonCode::PauseFailed;
    for (target, action) in [
        (PauseFailedRecoveryTarget::Started, "recovered_to_started"),
        (PauseFailedRecoveryTarget::Paused, "recovered_to_paused"),
    ] {
        let conn = test_conn();
        let id = insert_row(&conn, SandboxState::Pausing);
        quarantine(&conn, &id, SandboxState::Pausing, reason, "pause failed");
        seed_receipt(&conn, &id, SandboxState::Started);
        backdate(&conn, &id);
        let quarantine_id = quarantine_id(&conn, &id);
        assert_accepts(
            action,
            &conn,
            || {
                assert!(
                    recover_pause_failed_quarantine(
                        &conn,
                        &id.parse::<SandboxId>().unwrap(),
                        target
                    )
                    .unwrap()
                )
            },
            ExpectedDiff::new()
                .sandbox(
                    &id,
                    [
                        ("state", text(target.sandbox_state().as_str())),
                        ("failure_reason", NULL),
                        ("state_changed_at", Cell::NowText),
                        ("updated_at", Cell::NowText),
                    ],
                )
                .deleted("sandbox_quarantine_state", ("sandbox_id", &id))
                .inserted(
                    "sandbox_quarantine_history",
                    history_recorded(
                        &id,
                        &quarantine_id,
                        action,
                        reason,
                        "runtime state became definitive after pause failure",
                    ),
                ),
        );
    }
}

/// An orphan's synthetic quarantined row, claimed by automatic recovery and
/// adopted by its restore at generation 7 with a transfer scope, as the
/// cross-node recovery path leaves it before publication.
fn insert_claimed_orphan_restore(
    conn: &Connection,
) -> (String, ClaimedQuarantineRecoveryCandidate) {
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
    let claimed = claim_only_candidate(conn);
    assert_eq!(
        claimed.candidate.recovery,
        QuarantineRecoveryBranch::RestoreOrphanedStaging
    );
    let source = restore_source();
    let tx = immediate_transaction(conn).unwrap();
    with_sandbox_params(
        &id,
        SandboxState::Restoring,
        as_synthetic_orphan,
        |params| {
            restore_adopt_orphan_or_insert_sandbox_in_transaction(
                &tx,
                params,
                &RemoteRestoreOperation::artifact_staging(source),
            )
        },
    )
    .unwrap();
    assert!(set_orphan_quarantine_recovery_generation(&tx, &claimed, generation(7)).unwrap());
    tx.commit().unwrap();
    assert!(
        insert_restore_transfer_scope(
            conn,
            &id,
            TransferScopeId::new(),
            Some(generation(7)),
            None,
            &format!("/cache/restore-staging/{id}"),
        )
        .unwrap()
    );
    backdate(conn, &id);
    (id, claimed)
}

#[test]
fn orphan_recovery_publication_settles_suspended_with_a_recovery_receipt() {
    for with_artifact in [true, false] {
        let conn = test_conn();
        let (id, claimed) = insert_claimed_orphan_restore(&conn);
        // A failed staging attempt the recovery retried leaves its reason.
        assert!(
            mark_cross_node_restore_staging_failed_under_recovery_claim(
                &conn,
                "cross-node restore artifact staging failed",
                &claimed,
            )
            .unwrap()
        );
        backdate(&conn, &id);
        let artifact = LocalArtifactId::new();
        let receipt = SandboxCleanupEpisodeId::new();
        let mut cells = vec![
            ("state", text("suspended")),
            ("failure_reason", NULL),
            ("restore_origin", NULL),
            ("restore_phase", NULL),
            ("restore_storage_uri", NULL),
            ("transition_receipt_id", text(receipt.to_string())),
            ("transition_receipt_kind", text("quarantine_recovery")),
            ("transition_receipt_target_state", text("suspended")),
            ("transition_receipt_execution_generation", int(7)),
            (
                "transition_receipt_reason_code",
                text("orphaned_sandbox_row_missing"),
            ),
            ("state_changed_at", Cell::NowText),
            ("updated_at", Cell::NowText),
        ];
        let mut expected = ExpectedDiff::new()
            .deleted("sandbox_quarantine_state", ("sandbox_id", &id))
            .deleted("sandbox_restore_transfer_scopes", ("sandbox_id", &id));
        if with_artifact {
            cells.extend(pinned(artifact, "full", "suspend"));
            expected =
                expected.inserted("local_artifacts", suspend_artifact_inserted(&id, artifact));
        }
        assert_accepts(
            &format!("with artifact {with_artifact}"),
            &conn,
            || {
                assert!(
                    publish_orphan_quarantine_recovered_artifact(
                        &conn,
                        &claimed,
                        with_artifact.then_some(artifact),
                        &snapshot_path(&id),
                        generation(7),
                        receipt,
                    )
                    .unwrap()
                )
            },
            expected.sandbox(&id, cells),
        );
    }
}

/// A quarantined restore that pins its full suspend artifact at generation 7,
/// claimed by automatic recovery on the adoption branch.
pub(super) fn insert_claimed_adoptable(
    conn: &Connection,
) -> (String, LocalArtifactId, ClaimedQuarantineRecoveryCandidate) {
    let id = insert_row(conn, SandboxState::Restoring);
    let artifact = LocalArtifactId::new();
    insert_local_artifact(
        conn,
        &id,
        artifact,
        ArtifactKind::Full,
        ArtifactOrigin::Suspend,
        None,
        &snapshot_path(&id),
    )
    .unwrap();
    conn.execute(
        "UPDATE sandboxes
            SET current_local_artifact_id = ?2,
                current_local_artifact_kind = 'full',
                current_local_artifact_origin = 'suspend',
                space_api_route_execution_generation = 7,
                restore_origin = 'local',
                restore_phase = 'activation'
          WHERE id = ?1",
        params![id, artifact.to_string()],
    )
    .unwrap();
    quarantine(
        conn,
        &id,
        SandboxState::Restoring,
        SandboxQuarantineReasonCode::RestoreFirecrackerLaunchFailed,
        "restore launch failed",
    );
    let claimed = claim_only_candidate(conn);
    assert!(matches!(
        claimed.candidate.recovery,
        QuarantineRecoveryBranch::AdoptLocalArtifact(adoptable)
            if adoptable.local_artifact_id() == artifact
    ));
    (id, artifact, claimed)
}

pub(super) fn adoptable(
    claimed: &ClaimedQuarantineRecoveryCandidate,
) -> crate::db::AdoptableArtifact {
    match claimed.candidate.recovery {
        QuarantineRecoveryBranch::AdoptLocalArtifact(adoptable) => adoptable,
        QuarantineRecoveryBranch::RestoreOrphanedStaging => {
            panic!("the fixture is on the adoption branch")
        }
    }
}

#[test]
fn adopted_recovery_completion_settles_suspended_and_retires_the_quarantine() {
    let conn = test_conn();
    let (id, _, claimed) = insert_claimed_adoptable(&conn);
    let cleanup_id = SandboxCleanupEpisodeId::new();
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
    );
    backdate(&conn, &id);
    let quarantine_id = quarantine_id(&conn, &id);
    let reason = SandboxQuarantineReasonCode::RestoreFirecrackerLaunchFailed;
    let mut history = history_recorded(
        &id,
        &quarantine_id,
        "recovered_to_suspended",
        reason,
        "canonical suspend artifact validated after runtime cleanup",
    );
    history[0].1 = text(cleanup_id.to_string());
    // The full suspend pin keeps the generation as the restore's proof.
    assert_accepts(
        "adopted completion",
        &conn,
        || {
            assert!(
                complete_cleanup_episode_for_job(&conn, &id, cleanup_id, None)
                    .unwrap()
                    .is_some()
            )
        },
        ExpectedDiff::new()
            .sandbox(
                &id,
                [
                    ("state", text("suspended")),
                    ("cleanup_completion", NULL),
                    ("cleanup_keep_filesystem", NULL),
                    ("failure_reason", NULL),
                    ("transition_receipt_id", text(cleanup_id.to_string())),
                    ("transition_receipt_kind", text("quarantine_recovery")),
                    ("transition_receipt_target_state", text("suspended")),
                    ("transition_receipt_execution_generation", int(7)),
                    ("transition_receipt_reason_code", text(reason.as_str())),
                    ("state_changed_at", Cell::NowText),
                    ("updated_at", Cell::NowText),
                ],
            )
            .deleted("sandbox_cleanup_episodes", ("sandbox_id", &id))
            .deleted("sandbox_quarantine_state", ("sandbox_id", &id))
            .inserted("sandbox_quarantine_history", history),
    );
}
