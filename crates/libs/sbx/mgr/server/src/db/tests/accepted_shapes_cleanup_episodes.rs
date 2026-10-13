//! The complete row diff each cleanup-episode-bound writer of the node-local
//! SQLite `sandboxes.state` makes when it accepts: activation admission,
//! completion and rollback, cleanup request, failure and quarantine, and the
//! cleanup completions that settle a row.
//!
//! Refusals are pinned by `state_writer_refusal` and `cleanup_quarantine_refusal`.
//! These cases seed the side columns the writer might touch and assert the
//! whole-database diff, so moving a writer onto a shared helper must stay
//! column-identical. Auto-lifecycle is disabled on every seeded row, so
//! arming after activation writes nothing here; other tests cover arming.

use pplx_space_common_core::types::{
    LocalArtifactId, SandboxActivationAttemptId, SandboxCleanupEpisodeId,
    SandboxQuarantineReasonCode,
};
use rusqlite::{Connection, params};

use super::state_write_fixtures::{
    Cell, ExpectedDiff, NULL, assert_accepts, backdate, decorate, generation, insert_creating,
    insert_creating_with, insert_row, insert_suspended_with_artifact, int,
    quarantine_history_entered, quarantine_state_entered, receipt_cleared, runtime_cleared,
    seed_receipt, seed_runtime, snapshot_path, suspend_artifact_inserted, text,
};
use crate::db::transfer::complete_import;
use crate::db::{
    ActivationKind, ActivationRouteAdmission, CleanupCompletion, CleanupEpisode,
    CleanupEpisodeRequestOutcome, CompleteRowIntentCleanupDisposition,
    begin_activation_with_route_evidence, begin_suspend, charge_prepared_clone_disposal_attempt,
    complete_cleanup_episode_for_job, complete_create, complete_pool_clone,
    complete_row_intent_cleanup_for_job, mark_cleanup_required, mark_restore_succeeded,
    mark_start_succeeded, quarantine_cleanup_episode_for_job, rearm_prepared_clone_disposal,
    record_cleanup_episode_failure, request_cleanup_episode, rollback_restoring_episode,
    rollback_starting_episode, set_cleanup_intent, test_conn, update_state,
};
use crate::state::SandboxState;

const ATTEMPT: &str = "019a0000-0000-7000-8000-0000000000c1";

fn attempt() -> SandboxActivationAttemptId {
    ATTEMPT.parse().unwrap()
}

fn true_() -> Cell {
    int(1)
}

fn false_() -> Cell {
    int(0)
}

fn activate(
    conn: &Connection,
    id: &str,
    from: SandboxState,
    kind: ActivationKind,
) -> CleanupEpisode {
    begin_activation_with_route_evidence(
        conn,
        id,
        from,
        kind,
        Some(generation(7)),
        Some(attempt()),
        ActivationRouteAdmission::default(),
    )
    .unwrap()
    .unwrap()
}

/// Every column of the armed episode activation admission inserts.
fn activation_episode(
    id: &str,
    completion: CleanupCompletion,
    from: SandboxState,
    activation: SandboxState,
) -> Vec<(&'static str, Cell)> {
    vec![
        ("sandbox_id", text(id)),
        ("cleanup_id", Cell::Fresh),
        ("completion", text(completion.as_str())),
        ("keep_filesystem", true_()),
        ("phase", text("armed")),
        ("entered_from_state", text(from.as_str())),
        ("activation_state", text(activation.as_str())),
        ("requested_at", NULL),
        ("quarantine_reason_code", NULL),
        ("failure_message", NULL),
        ("last_cleanup_error", NULL),
        ("execution_generation", int(7)),
        ("activation_attempt_id", text(ATTEMPT)),
        ("attempt_count", int(0)),
        ("created_at", Cell::NowInteger),
        ("recovery_local_artifact_id", NULL),
        ("recovery_execution_generation", NULL),
        ("prepared_clone_root", NULL),
        ("operation_activation_attempt_id", NULL),
    ]
}

fn receipt_set(
    receipt_id: SandboxCleanupEpisodeId,
    kind: &str,
    target: SandboxState,
    reason: Option<&str>,
) -> Vec<(&'static str, Cell)> {
    vec![
        ("transition_receipt_id", text(receipt_id.to_string())),
        ("transition_receipt_kind", text(kind)),
        ("transition_receipt_target_state", text(target.as_str())),
        ("transition_receipt_execution_generation", int(7)),
        ("transition_receipt_activation_attempt_id", text(ATTEMPT)),
        ("transition_receipt_reason_code", reason.map_or(NULL, text)),
    ]
}

/// Mark the row's node execution as having owned an allocation, so admission
/// must rotate it into `previous_node_execution_id`.
fn seed_live_node_execution(conn: &Connection, id: &str) -> String {
    conn.execute(
        "UPDATE sandboxes SET node_execution_retired = 0 WHERE id = ?1",
        params![id],
    )
    .unwrap();
    conn.query_row(
        "SELECT node_execution_id FROM sandboxes WHERE id = ?1",
        params![id],
        |row| row.get(0),
    )
    .unwrap()
}

fn node_execution_rotated(previous: &str) -> Vec<(&'static str, Cell)> {
    vec![
        ("previous_node_execution_id", text(previous)),
        ("node_execution_id", Cell::Fresh),
    ]
}

#[test]
fn activation_admission_claims_the_row_and_arms_one_episode() {
    let conn = test_conn();
    let (suspended_restore, _) = insert_suspended_with_artifact(&conn);
    for (kind, from, completion) in [
        (
            ActivationKind::ColdStart,
            SandboxState::Created,
            CleanupCompletion::MarkCreated,
        ),
        (
            ActivationKind::ColdStart,
            SandboxState::Stopped,
            CleanupCompletion::MarkStopped,
        ),
        (
            ActivationKind::ColdStart,
            SandboxState::Suspended,
            CleanupCompletion::MarkSuspended,
        ),
        (
            ActivationKind::Restore,
            SandboxState::Suspended,
            CleanupCompletion::MarkSuspended,
        ),
    ] {
        let case = format!("{kind:?} from {from}");
        let id = if kind == ActivationKind::Restore {
            suspended_restore.clone()
        } else {
            insert_row(&conn, from)
        };
        let previous_execution = seed_live_node_execution(&conn, &id);
        decorate(&conn, &id, SandboxState::Started);
        let activation = kind.activation_state();
        let mut cells = vec![
            ("state", text(activation.as_str())),
            ("failure_reason", NULL),
            ("cleanup_completion", text(completion.as_str())),
            ("cleanup_keep_filesystem", true_()),
            ("state_changed_at", Cell::NowText),
            ("updated_at", Cell::NowText),
        ];
        if kind == ActivationKind::Restore {
            cells.push(("restore_origin", text("local")));
            cells.push(("restore_phase", text("activation")));
        }
        cells.extend(node_execution_rotated(&previous_execution));
        cells.extend(receipt_cleared(SandboxState::Started));
        assert_accepts(
            &case,
            &conn,
            || {
                activate(&conn, &id, from, kind);
            },
            ExpectedDiff::new().sandbox(&id, cells).inserted(
                "sandbox_cleanup_episodes",
                activation_episode(&id, completion, from, activation),
            ),
        );
    }
}

#[test]
fn activation_completion_publishes_the_target_and_retires_its_episode() {
    let conn = test_conn();

    let starting = insert_row(&conn, SandboxState::Suspended);
    let start_episode = activate(
        &conn,
        &starting,
        SandboxState::Suspended,
        ActivationKind::ColdStart,
    );
    conn.execute(
        "UPDATE sandboxes
            SET current_local_artifact_id = ?2,
                current_local_artifact_kind = 'full',
                current_local_artifact_origin = 'suspend'
          WHERE id = ?1",
        params![starting, LocalArtifactId::new().to_string()],
    )
    .unwrap();
    decorate(&conn, &starting, SandboxState::Stopped);
    let mut cells = vec![
        ("state", text("started")),
        ("failure_reason", NULL),
        ("current_local_artifact_id", NULL),
        ("current_local_artifact_kind", NULL),
        ("current_local_artifact_origin", NULL),
        ("cleanup_completion", NULL),
        ("cleanup_keep_filesystem", NULL),
        ("state_changed_at", Cell::NowText),
        ("updated_at", Cell::NowText),
    ];
    cells.extend(receipt_set(
        start_episode.id,
        "activation_completed",
        SandboxState::Started,
        None,
    ));
    assert_accepts(
        "start succeeded",
        &conn,
        || assert!(mark_start_succeeded(&conn, &starting, Some(start_episode.id)).unwrap()),
        ExpectedDiff::new()
            .sandbox(&starting, cells)
            .deleted("sandbox_cleanup_episodes", ("sandbox_id", &starting)),
    );

    for target in [SandboxState::Started, SandboxState::Paused] {
        let (restoring, _) = insert_suspended_with_artifact(&conn);
        let episode = activate(
            &conn,
            &restoring,
            SandboxState::Suspended,
            ActivationKind::Restore,
        );
        decorate(&conn, &restoring, SandboxState::Stopped);
        let mut cells = vec![
            ("state", text(target.as_str())),
            ("failure_reason", NULL),
            ("cleanup_completion", NULL),
            ("cleanup_keep_filesystem", NULL),
            ("restore_origin", NULL),
            ("restore_phase", NULL),
            ("state_changed_at", Cell::NowText),
            ("updated_at", Cell::NowText),
        ];
        cells.extend(receipt_set(
            episode.id,
            "activation_completed",
            target,
            None,
        ));
        assert_accepts(
            &format!("restore succeeded to {target}"),
            &conn,
            || {
                assert!(
                    mark_restore_succeeded(&conn, &restoring, target, Some(episode.id)).unwrap()
                )
            },
            ExpectedDiff::new()
                .sandbox(&restoring, cells)
                .deleted("sandbox_cleanup_episodes", ("sandbox_id", &restoring)),
        );
    }

    // Create completion moves no state itself: create's own generic write has
    // already settled the row, and completion retires the armed episode. A
    // `creating` row never carries a failure reason, so none is seeded. A
    // create that does not start publishes an activation receipt, which only
    // a `created` row can carry.
    for publish in [false, true] {
        for settled in [SandboxState::Created, SandboxState::Suspended] {
            // Production create episodes carry the route's generation and
            // attempt, so only the flag decides whether a receipt is written.
            let (id, cleanup_id) = insert_creating_with(
                &conn,
                CleanupCompletion::DeleteRow,
                Some((generation(7), attempt())),
            );
            assert_eq!(
                update_state(&conn, &id, settled).unwrap(),
                crate::db::SandboxStateWriteOutcome::Applied
            );
            seed_receipt(&conn, &id, SandboxState::Stopped);
            backdate(&conn, &id);
            let mut cells = vec![
                ("cleanup_completion", NULL),
                ("cleanup_keep_filesystem", NULL),
                ("updated_at", Cell::NowText),
            ];
            if publish && settled == SandboxState::Created {
                cells.extend(receipt_set(
                    cleanup_id,
                    "activation_completed",
                    settled,
                    None,
                ));
            }
            assert_accepts(
                &format!("create completed as {settled}, publish {publish}"),
                &conn,
                || assert!(complete_create(&conn, &id, cleanup_id, publish).unwrap()),
                ExpectedDiff::new()
                    .sandbox(&id, cells)
                    .deleted("sandbox_cleanup_episodes", ("sandbox_id", &id)),
            );
        }
    }

    let (clone, clone_cleanup) = insert_creating(&conn, CleanupCompletion::DeletePoolClone);
    seed_receipt(&conn, &clone, SandboxState::Stopped);
    backdate(&conn, &clone);
    assert_accepts(
        "pool clone published",
        &conn,
        || assert!(complete_pool_clone(&conn, &clone, clone_cleanup).unwrap()),
        ExpectedDiff::new()
            .sandbox(
                &clone,
                [
                    ("state", text("suspended")),
                    ("cleanup_completion", NULL),
                    ("cleanup_keep_filesystem", NULL),
                    ("state_changed_at", Cell::NowText),
                    ("updated_at", Cell::NowText),
                ],
            )
            .deleted("sandbox_cleanup_episodes", ("sandbox_id", &clone)),
    );

    for suspended in [false, true] {
        let (import, import_cleanup) = insert_creating(&conn, CleanupCompletion::DeleteRow);
        seed_receipt(&conn, &import, SandboxState::Started);
        backdate(&conn, &import);
        let target = if suspended { "suspended" } else { "stopped" };
        assert_accepts(
            &format!("import completed as {target}"),
            &conn,
            || assert!(complete_import(&conn, &import, import_cleanup, suspended).unwrap()),
            ExpectedDiff::new()
                .sandbox(
                    &import,
                    [
                        ("state", text(target)),
                        ("cleanup_completion", NULL),
                        ("cleanup_keep_filesystem", NULL),
                        ("state_changed_at", Cell::NowText),
                        ("updated_at", Cell::NowText),
                    ],
                )
                .deleted("sandbox_cleanup_episodes", ("sandbox_id", &import)),
        );
    }
}

#[test]
fn activation_rollback_returns_the_row_and_retires_its_episode() {
    let conn = test_conn();
    let starting = insert_row(&conn, SandboxState::Stopped);
    let start_episode = activate(
        &conn,
        &starting,
        SandboxState::Stopped,
        ActivationKind::ColdStart,
    );
    decorate(&conn, &starting, SandboxState::Started);
    assert_accepts(
        "starting rollback",
        &conn,
        || {
            assert!(
                rollback_starting_episode(
                    &conn,
                    &starting,
                    SandboxState::Stopped,
                    start_episode.id
                )
                .unwrap()
            )
        },
        ExpectedDiff::new()
            .sandbox(
                &starting,
                [
                    ("state", text("stopped")),
                    ("failure_reason", NULL),
                    ("cleanup_completion", NULL),
                    ("cleanup_keep_filesystem", NULL),
                    ("state_changed_at", Cell::NowText),
                    ("updated_at", Cell::NowText),
                ],
            )
            .deleted("sandbox_cleanup_episodes", ("sandbox_id", &starting)),
    );

    let (restoring, _) = insert_suspended_with_artifact(&conn);
    let restore_episode = activate(
        &conn,
        &restoring,
        SandboxState::Suspended,
        ActivationKind::Restore,
    );
    decorate(&conn, &restoring, SandboxState::Started);
    assert_accepts(
        "restoring rollback",
        &conn,
        || {
            assert!(
                rollback_restoring_episode(
                    &conn,
                    &restoring,
                    SandboxState::Suspended,
                    restore_episode.id
                )
                .unwrap()
            )
        },
        ExpectedDiff::new()
            .sandbox(
                &restoring,
                [
                    ("state", text("suspended")),
                    ("failure_reason", NULL),
                    ("cleanup_completion", NULL),
                    ("cleanup_keep_filesystem", NULL),
                    ("restore_origin", NULL),
                    ("restore_phase", NULL),
                    ("state_changed_at", Cell::NowText),
                    ("updated_at", Cell::NowText),
                ],
            )
            .deleted("sandbox_cleanup_episodes", ("sandbox_id", &restoring)),
    );
}

fn requested_episode(
    reason: SandboxQuarantineReasonCode,
    message: &str,
) -> Vec<(&'static str, Cell)> {
    vec![
        ("phase", text("requested")),
        ("requested_at", Cell::NowInteger),
        ("quarantine_reason_code", text(reason.as_str())),
        ("failure_message", text(message)),
    ]
}

fn request(conn: &Connection, id: &str, cleanup_id: SandboxCleanupEpisodeId, message: &str) {
    assert_eq!(
        request_cleanup_episode(
            conn,
            id,
            cleanup_id,
            SandboxQuarantineReasonCode::ColdActivationInterrupted,
            message,
        )
        .unwrap(),
        CleanupEpisodeRequestOutcome::Requested
    );
}

#[test]
fn cleanup_request_failure_and_quarantine_advance_the_episode_and_the_row() {
    let conn = test_conn();
    let reason = SandboxQuarantineReasonCode::ColdActivationInterrupted;

    let (creating, create_cleanup) = insert_creating(&conn, CleanupCompletion::DeleteRow);
    seed_receipt(&conn, &creating, SandboxState::Started);
    backdate(&conn, &creating);
    assert_accepts(
        "request from creating",
        &conn,
        || request(&conn, &creating, create_cleanup, "create interrupted"),
        ExpectedDiff::new()
            .sandbox(
                &creating,
                [
                    ("state", text("cleanup_required")),
                    ("failure_reason", text("create interrupted")),
                    ("state_changed_at", Cell::NowText),
                    ("updated_at", Cell::NowText),
                ],
            )
            .updated(
                "sandbox_cleanup_episodes",
                ("sandbox_id", &creating),
                requested_episode(reason, "create interrupted"),
            ),
    );

    let (restoring, _) = insert_suspended_with_artifact(&conn);
    let restore_episode = activate(
        &conn,
        &restoring,
        SandboxState::Suspended,
        ActivationKind::Restore,
    );
    decorate(&conn, &restoring, SandboxState::Started);
    assert_accepts(
        "request from restoring",
        &conn,
        || request(&conn, &restoring, restore_episode.id, "restore interrupted"),
        ExpectedDiff::new()
            .sandbox(
                &restoring,
                [
                    ("state", text("cleanup_required")),
                    ("failure_reason", text("restore interrupted")),
                    ("restore_origin", NULL),
                    ("restore_phase", NULL),
                    ("state_changed_at", Cell::NowText),
                    ("updated_at", Cell::NowText),
                ],
            )
            .updated(
                "sandbox_cleanup_episodes",
                ("sandbox_id", &restoring),
                requested_episode(reason, "restore interrupted"),
            ),
    );

    backdate(&conn, &creating);
    assert_accepts(
        "failure from cleanup_required",
        &conn,
        || {
            assert!(
                record_cleanup_episode_failure(&conn, &creating, create_cleanup, "delete failed")
                    .unwrap()
            )
        },
        ExpectedDiff::new()
            .sandbox(
                &creating,
                [
                    ("state", text("cleanup_failed")),
                    ("state_changed_at", Cell::NowText),
                    ("updated_at", Cell::NowText),
                ],
            )
            .updated(
                "sandbox_cleanup_episodes",
                ("sandbox_id", &creating),
                [
                    ("last_cleanup_error", text("delete failed")),
                    ("attempt_count", int(1)),
                ],
            ),
    );

    // The requested episode survives a repeated failure on a `cleanup_failed`
    // row, which keeps its state and change time.
    backdate(&conn, &creating);
    assert_accepts(
        "failure from cleanup_failed",
        &conn,
        || {
            assert!(
                record_cleanup_episode_failure(
                    &conn,
                    &creating,
                    create_cleanup,
                    "delete failed again"
                )
                .unwrap()
            )
        },
        ExpectedDiff::new()
            .sandbox(&creating, [("updated_at", Cell::NowText)])
            .updated(
                "sandbox_cleanup_episodes",
                ("sandbox_id", &creating),
                [
                    ("last_cleanup_error", text("delete failed again")),
                    ("attempt_count", int(2)),
                ],
            ),
    );

    backdate(&conn, &restoring);
    let quarantine_reason =
        "activation compensation cannot safely reach mark_suspended; unsafe_evidence=pid reused";
    assert_accepts(
        "quarantine of an activation cleanup",
        &conn,
        || {
            assert!(
                quarantine_cleanup_episode_for_job(
                    &conn,
                    &restoring,
                    restore_episode.id,
                    "pid reused",
                    None
                )
                .unwrap()
            )
        },
        ExpectedDiff::new()
            .sandbox(
                &restoring,
                [
                    ("state", text("quarantined")),
                    ("failure_reason", text(quarantine_reason)),
                    ("cleanup_completion", NULL),
                    ("cleanup_keep_filesystem", NULL),
                    ("state_changed_at", Cell::NowText),
                    ("updated_at", Cell::NowText),
                ],
            )
            .deleted("sandbox_cleanup_episodes", ("sandbox_id", &restoring))
            .inserted(
                "sandbox_quarantine_state",
                quarantine_state_entered(&restoring, "cleanup_required", reason, quarantine_reason),
            )
            .inserted(
                "sandbox_quarantine_history",
                quarantine_history_entered(&restoring, reason, quarantine_reason),
            ),
    );

    // A pool-clone disposal keeps its episode through quarantine, so an
    // operator delete can rearm it with its original identity.
    // Its budget is charged and its attempt failed first, so the rearm's
    // resets of both are visible.
    let (clone, clone_cleanup) = insert_creating(&conn, CleanupCompletion::DeletePoolClone);
    request(&conn, &clone, clone_cleanup, "clone interrupted");
    assert!(charge_prepared_clone_disposal_attempt(&conn, &clone, clone_cleanup).unwrap());
    assert!(
        record_cleanup_episode_failure(&conn, &clone, clone_cleanup, "clone root busy").unwrap()
    );
    backdate(&conn, &clone);
    let clone_reason =
        "activation compensation cannot safely reach delete_pool_clone; unsafe_evidence=root busy";
    assert_accepts(
        "quarantine of a pool-clone disposal",
        &conn,
        || {
            assert!(
                quarantine_cleanup_episode_for_job(&conn, &clone, clone_cleanup, "root busy", None)
                    .unwrap()
            )
        },
        ExpectedDiff::new()
            .sandbox(
                &clone,
                [
                    ("state", text("quarantined")),
                    ("failure_reason", text(clone_reason)),
                    ("cleanup_completion", NULL),
                    ("cleanup_keep_filesystem", NULL),
                    ("state_changed_at", Cell::NowText),
                    ("updated_at", Cell::NowText),
                ],
            )
            .inserted(
                "sandbox_quarantine_state",
                quarantine_state_entered(&clone, "cleanup_failed", reason, clone_reason),
            )
            .inserted(
                "sandbox_quarantine_history",
                quarantine_history_entered(&clone, reason, clone_reason),
            ),
    );

    backdate(&conn, &clone);
    conn.execute(
        "UPDATE sandbox_cleanup_episodes SET requested_at = 1 WHERE sandbox_id = ?1",
        params![clone],
    )
    .unwrap();
    // The disposal's `sandbox_quarantine_state` row stays while the row is
    // `cleanup_required`: pinned as current behavior.
    assert_accepts(
        "rearm a quarantined pool-clone disposal",
        &conn,
        || assert!(rearm_prepared_clone_disposal(&conn, &clone, clone_cleanup).unwrap()),
        ExpectedDiff::new()
            .sandbox(
                &clone,
                [
                    ("state", text("cleanup_required")),
                    ("cleanup_completion", text("delete_pool_clone")),
                    ("cleanup_keep_filesystem", false_()),
                    ("state_changed_at", Cell::NowText),
                    ("updated_at", Cell::NowText),
                ],
            )
            .updated(
                "sandbox_cleanup_episodes",
                ("sandbox_id", &clone),
                [
                    ("requested_at", Cell::NowInteger),
                    ("quarantine_reason_code", text("create_interrupted")),
                    ("attempt_count", int(0)),
                    ("last_cleanup_error", NULL),
                ],
            ),
    );
}

/// Complete the row's owned `completion` intent the way the cleanup worker
/// does after node release, with no job id.
fn complete_intent(
    conn: &Connection,
    id: &str,
    completion: CleanupCompletion,
    artifact: Option<(LocalArtifactId, &str)>,
) -> CompleteRowIntentCleanupDisposition {
    complete_row_intent_cleanup_for_job(conn, id, completion, artifact, None, None).unwrap()
}

/// The runtime columns a cleanup completion clears from a launched row, with
/// the generation and pod kept as teardown proof unless a receipt replaces it.
fn launched_runtime_cleared(keep_teardown_proof: bool) -> Vec<(&'static str, Cell)> {
    let retained: &[&str] = if keep_teardown_proof {
        &[
            "space_api_route_execution_generation",
            "space_api_route_pod_uid",
        ]
    } else {
        &[]
    };
    runtime_cleared(retained)
}

#[test]
fn episode_completion_applies_its_target_and_retires_the_episode() {
    let reason = SandboxQuarantineReasonCode::ColdActivationInterrupted;
    for from in [
        SandboxState::Created,
        SandboxState::Stopped,
        SandboxState::Suspended,
    ] {
        let conn = test_conn();
        let case = format!("cold start from {from} rolled back");
        let (id, _) = if from == SandboxState::Suspended {
            insert_suspended_with_artifact(&conn)
        } else {
            (insert_row(&conn, from), LocalArtifactId::new())
        };
        let episode = activate(&conn, &id, from, ActivationKind::ColdStart);
        seed_runtime(&conn, &id);
        request(&conn, &id, episode.id, "launch failed");
        conn.execute(
            "UPDATE sandboxes SET suspend_predecessor_artifact_id = ?2 WHERE id = ?1",
            params![id, LocalArtifactId::new().to_string()],
        )
        .unwrap();
        backdate(&conn, &id);
        let mut cells = vec![
            ("state", text(from.as_str())),
            ("cleanup_completion", NULL),
            ("cleanup_keep_filesystem", NULL),
            ("suspend_predecessor_artifact_id", NULL),
            ("state_changed_at", Cell::NowText),
            ("updated_at", Cell::NowText),
        ];
        // A rollback receipt replaces the teardown proof a plain stop keeps;
        // a suspended row that still pins its suspend artifact keeps it.
        cells.extend(launched_runtime_cleared(from == SandboxState::Suspended));
        cells.extend(receipt_set(
            episode.id,
            "activation_rollback",
            from,
            Some(reason.as_str()),
        ));
        assert_accepts(
            &case,
            &conn,
            || {
                assert!(
                    complete_cleanup_episode_for_job(&conn, &id, episode.id, None)
                        .unwrap()
                        .is_some()
                )
            },
            ExpectedDiff::new()
                .sandbox(&id, cells)
                .deleted("sandbox_cleanup_episodes", ("sandbox_id", &id)),
        );
    }

    for completion in [
        CleanupCompletion::DeleteRow,
        CleanupCompletion::DeletePoolClone,
    ] {
        let conn = test_conn();
        let (id, cleanup_id) = insert_creating(&conn, completion);
        request(&conn, &id, cleanup_id, "create interrupted");
        assert_accepts(
            &format!("{completion} completion"),
            &conn,
            || {
                assert!(
                    complete_cleanup_episode_for_job(&conn, &id, cleanup_id, None)
                        .unwrap()
                        .is_some()
                )
            },
            ExpectedDiff::new()
                .deleted("sandboxes", ("id", &id))
                .deleted("sandbox_cleanup_episodes", ("sandbox_id", &id)),
        );
    }
}

#[test]
fn row_intent_completion_applies_the_owned_intent() {
    let conn = test_conn();

    let stopping = insert_row(&conn, SandboxState::Stopping);
    assert!(set_cleanup_intent(&conn, &stopping, true, CleanupCompletion::MarkStopped).unwrap());
    seed_runtime(&conn, &stopping);
    decorate(&conn, &stopping, SandboxState::Started);
    let mut cells = vec![
        ("state", text("stopped")),
        ("cleanup_completion", NULL),
        ("cleanup_keep_filesystem", NULL),
        ("failure_reason", NULL),
        ("state_changed_at", Cell::NowText),
        ("updated_at", Cell::NowText),
    ];
    cells.extend(launched_runtime_cleared(true));
    cells.extend(receipt_cleared(SandboxState::Started));
    assert_accepts(
        "stop",
        &conn,
        || {
            assert_eq!(
                complete_intent(&conn, &stopping, CleanupCompletion::MarkStopped, None),
                CompleteRowIntentCleanupDisposition::Applied
            )
        },
        ExpectedDiff::new().sandbox(&stopping, cells),
    );

    let suspending = insert_row(&conn, SandboxState::Paused);
    assert!(begin_suspend(&conn, &suspending).unwrap());
    seed_runtime(&conn, &suspending);
    conn.execute(
        "UPDATE sandboxes SET suspend_predecessor_artifact_id = ?2 WHERE id = ?1",
        params![suspending, LocalArtifactId::new().to_string()],
    )
    .unwrap();
    decorate(&conn, &suspending, SandboxState::Suspended);
    let artifact = LocalArtifactId::new();
    let mut cells = vec![
        ("state", text("suspended")),
        ("failure_reason", NULL),
        ("current_local_artifact_id", text(artifact.to_string())),
        ("current_local_artifact_kind", text("full")),
        ("current_local_artifact_origin", text("suspend")),
        ("suspend_predecessor_artifact_id", NULL),
        ("state_changed_at", Cell::NowText),
        ("updated_at", Cell::NowText),
    ];
    cells.extend(launched_runtime_cleared(true));
    assert_accepts(
        "suspend with its published artifact",
        &conn,
        || {
            assert_eq!(
                complete_intent(
                    &conn,
                    &suspending,
                    CleanupCompletion::MarkSuspended,
                    Some((artifact, &snapshot_path(&suspending)))
                ),
                CompleteRowIntentCleanupDisposition::Applied
            )
        },
        ExpectedDiff::new().sandbox(&suspending, cells).inserted(
            "local_artifacts",
            suspend_artifact_inserted(&suspending, artifact),
        ),
    );

    // Without a published or registered artifact the suspend cannot settle:
    // the row is quarantined instead.
    let lost = insert_row(&conn, SandboxState::Paused);
    assert!(begin_suspend(&conn, &lost).unwrap());
    seed_runtime(&conn, &lost);
    decorate(&conn, &lost, SandboxState::Suspended);
    let lost_reason = crate::db::SUSPEND_PUBLISH_FAILED_NO_ARTIFACT_RUNTIME_GONE;
    let mut cells = vec![
        ("state", text("quarantined")),
        ("failure_reason", text(lost_reason)),
        ("state_changed_at", Cell::NowText),
        ("updated_at", Cell::NowText),
    ];
    cells.extend(launched_runtime_cleared(false));
    cells.extend(receipt_cleared(SandboxState::Suspended));
    let publication = SandboxQuarantineReasonCode::SuspendSnapshotPublicationFailed;
    assert_accepts(
        "suspend without an artifact",
        &conn,
        || {
            assert_eq!(
                complete_intent(&conn, &lost, CleanupCompletion::MarkSuspended, None),
                CompleteRowIntentCleanupDisposition::Quarantined
            )
        },
        ExpectedDiff::new()
            .sandbox(&lost, cells)
            .inserted(
                "sandbox_quarantine_state",
                quarantine_state_entered(&lost, "suspending", publication, lost_reason),
            )
            .inserted(
                "sandbox_quarantine_history",
                quarantine_history_entered(&lost, publication, lost_reason),
            ),
    );

    // Startup recovery's quarantine intent: `cleanup_required` with no
    // completion recorded.
    let held = insert_row(&conn, SandboxState::Started);
    seed_runtime(&conn, &held);
    mark_cleanup_required(
        &conn,
        &held,
        true,
        CleanupCompletion::Quarantine,
        "probe failed",
    )
    .unwrap();
    conn.execute(
        "UPDATE sandboxes SET cleanup_completion = NULL, cleanup_keep_filesystem = NULL WHERE id = ?1",
        params![held],
    )
    .unwrap();
    seed_receipt(&conn, &held, SandboxState::Started);
    backdate(&conn, &held);
    let mut cells = vec![
        ("state", text("quarantined")),
        ("state_changed_at", Cell::NowText),
        ("updated_at", Cell::NowText),
    ];
    cells.extend(launched_runtime_cleared(false));
    cells.extend(receipt_cleared(SandboxState::Started));
    let probe = SandboxQuarantineReasonCode::RuntimeProbeInconsistent;
    let mut state = quarantine_state_entered(&held, "cleanup_required", probe, "probe failed");
    for (name, cell) in &mut state {
        if *name == "next_action_owner" {
            *cell = text("operator_attention");
        }
    }
    assert_accepts(
        "quarantine intent",
        &conn,
        || {
            assert_eq!(
                complete_intent(&conn, &held, CleanupCompletion::Quarantine, None),
                CompleteRowIntentCleanupDisposition::Applied
            )
        },
        ExpectedDiff::new()
            .sandbox(&held, cells)
            .inserted("sandbox_quarantine_state", state)
            .inserted(
                "sandbox_quarantine_history",
                quarantine_history_entered(&held, probe, "probe failed"),
            ),
    );

    let deleting = insert_row(&conn, SandboxState::Deleting);
    assert!(set_cleanup_intent(&conn, &deleting, false, CleanupCompletion::DeleteRow).unwrap());
    assert_accepts(
        "delete",
        &conn,
        || {
            assert_eq!(
                complete_intent(&conn, &deleting, CleanupCompletion::DeleteRow, None),
                CompleteRowIntentCleanupDisposition::Applied
            )
        },
        ExpectedDiff::new().deleted("sandboxes", ("id", &deleting)),
    );
}
