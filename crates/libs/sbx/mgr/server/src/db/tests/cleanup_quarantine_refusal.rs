//! Refusal outcomes of the cleanup-episode and quarantine writers, and the
//! cleanup-episode decoder's rejection of rows it cannot represent.
//!
//! A refused write must report the refusal to its caller and leave the sandbox
//! row, its cleanup episode and its quarantine rows exactly as it found them.

use super::*;

const QUARANTINE_REASON: &str = "runtime evidence is inconsistent";

/// The durable evidence a refused writer must not change, read as stored
/// columns so it does not depend on the decoder under test.
#[derive(Debug, PartialEq)]
struct Persisted {
    sandbox: Option<PersistedSandbox>,
    episode: Option<PersistedEpisode>,
    quarantine: Option<PersistedQuarantine>,
    history_rows: i64,
}

#[derive(Debug, PartialEq)]
struct PersistedSandbox {
    state: SandboxState,
    failure_reason: Option<String>,
    cleanup_completion: Option<CleanupCompletion>,
    cleanup_keep_filesystem: Option<bool>,
}

#[derive(Debug, PartialEq)]
struct PersistedEpisode {
    cleanup_id: String,
    phase: String,
    requested_at: Option<i64>,
    quarantine_reason_code: Option<String>,
    failure_message: Option<String>,
    last_cleanup_error: Option<String>,
    attempt_count: i64,
}

#[derive(Debug, PartialEq)]
struct PersistedQuarantine {
    quarantine_id: String,
    source: String,
    reason_code: String,
    reason_message: Option<String>,
}

fn persisted(conn: &Connection, id: &str) -> Persisted {
    Persisted {
        sandbox: get_sandbox(conn, id).unwrap().map(|row| PersistedSandbox {
            state: row.state,
            failure_reason: row.failure_reason,
            cleanup_completion: row.cleanup_completion,
            cleanup_keep_filesystem: row.cleanup_keep_filesystem,
        }),
        episode: conn
            .query_row(
                "SELECT cleanup_id, phase, requested_at, quarantine_reason_code,
                        failure_message, last_cleanup_error, attempt_count
                   FROM sandbox_cleanup_episodes
                  WHERE sandbox_id = ?1",
                params![id],
                |row| {
                    Ok(PersistedEpisode {
                        cleanup_id: row.get(0)?,
                        phase: row.get(1)?,
                        requested_at: row.get(2)?,
                        quarantine_reason_code: row.get(3)?,
                        failure_message: row.get(4)?,
                        last_cleanup_error: row.get(5)?,
                        attempt_count: row.get(6)?,
                    })
                },
            )
            .optional()
            .unwrap(),
        quarantine: conn
            .query_row(
                "SELECT quarantine_id, source, reason_code, reason_message
                   FROM sandbox_quarantine_state
                  WHERE sandbox_id = ?1",
                params![id],
                |row| {
                    Ok(PersistedQuarantine {
                        quarantine_id: row.get(0)?,
                        source: row.get(1)?,
                        reason_code: row.get(2)?,
                        reason_message: row.get(3)?,
                    })
                },
            )
            .optional()
            .unwrap(),
        history_rows: conn
            .query_row(
                "SELECT COUNT(*) FROM sandbox_quarantine_history WHERE sandbox_id = ?1",
                params![id],
                |row| row.get(0),
            )
            .unwrap(),
    }
}

fn quarantine(conn: &Connection, id: &str) {
    mark_quarantined(
        conn,
        id,
        SandboxQuarantineSource::SbxManager,
        SandboxQuarantineReasonCode::RuntimeProbeInconsistent,
        QUARANTINE_REASON,
        SandboxQuarantineNextAction::RequiresOperatorAttention,
    )
    .unwrap();
    assert_eq!(
        get_sandbox(conn, id).unwrap().unwrap().state,
        SandboxState::Quarantined
    );
}

/// A cold start's armed episode: the row is `starting` and owns a
/// `mark_stopped` intent.
fn armed_start(conn: &Connection, id: &str, label: &str) -> CleanupEpisode {
    insert_test_sandbox(
        conn,
        TestSandboxInsert::new(id, label, SandboxState::Stopped),
    );
    let episode = begin_start(conn, id, SandboxState::Stopped)
        .unwrap()
        .expect("a stopped sandbox should enter Starting");
    assert!(episode.requested_at.is_none());
    episode
}

fn requested_start(conn: &Connection, id: &str, label: &str) -> CleanupEpisode {
    let episode = armed_start(conn, id, label);
    assert_eq!(
        request_cleanup_episode(
            conn,
            id,
            episode.id,
            SandboxQuarantineReasonCode::ColdActivationInterrupted,
            "start failed",
        )
        .unwrap(),
        CleanupEpisodeRequestOutcome::Requested,
    );
    assert_eq!(
        get_sandbox(conn, id).unwrap().unwrap().state,
        SandboxState::CleanupRequired
    );
    episode
}

// The row helpers below write columns directly, so a case reaches one writer
// predicate without another writer's side effects.

fn set_state(conn: &Connection, id: &str, state: SandboxState) {
    let changed = conn
        .execute(
            "UPDATE sandboxes SET state = ?2 WHERE id = ?1",
            params![id, state.as_str()],
        )
        .unwrap();
    assert_eq!(changed, 1);
}

/// The row keeps its keep-filesystem flag but no longer the episode's
/// `mark_stopped` completion.
fn change_intent_completion(conn: &Connection, id: &str) {
    let changed = conn
        .execute(
            "UPDATE sandboxes SET cleanup_completion = 'mark_failed' WHERE id = ?1",
            params![id],
        )
        .unwrap();
    assert_eq!(changed, 1);
}

/// The row keeps the episode's completion but no longer its keep-filesystem
/// flag.
fn change_intent_keep_filesystem(conn: &Connection, id: &str) {
    let changed = conn
        .execute(
            "UPDATE sandboxes
                SET cleanup_keep_filesystem = NOT cleanup_keep_filesystem
              WHERE id = ?1",
            params![id],
        )
        .unwrap();
    assert_eq!(changed, 1);
}

type CleanupWriter = fn(&Connection, &str) -> Option<bool>;

/// The generic cleanup writers, each reporting whether it wrote. Only
/// `mark_cleanup_required` returns no outcome, so it reports `None`.
const GENERIC_CLEANUP_WRITERS: [(&str, CleanupWriter); 4] = [
    ("mark_cleanup_required", |conn, id| {
        mark_cleanup_required(
            conn,
            id,
            true,
            CleanupCompletion::MarkStopped,
            "stale observer",
        )
        .unwrap();
        None
    }),
    ("mark_cleanup_required_if_no_intent", |conn, id| {
        Some(
            mark_cleanup_required_if_no_intent(
                conn,
                id,
                true,
                CleanupCompletion::MarkStopped,
                "stale observer",
            )
            .unwrap(),
        )
    }),
    ("mark_cleanup_required_if_no_intent_in_state", |conn, id| {
        Some(
            mark_cleanup_required_if_no_intent_in_state(
                conn,
                id,
                SandboxState::Quarantined,
                true,
                CleanupCompletion::MarkStopped,
                "stale observer",
            )
            .unwrap(),
        )
    }),
    ("mark_cleanup_failed", |conn, id| {
        Some(
            mark_cleanup_failed(
                conn,
                id,
                true,
                CleanupCompletion::MarkStopped,
                "stale observer",
            )
            .unwrap(),
        )
    }),
];

#[test]
fn generic_cleanup_writers_refuse_quarantined_and_missing_rows() {
    for (writer_name, writer) in GENERIC_CLEANUP_WRITERS {
        let refused = if writer_name == "mark_cleanup_required" {
            None
        } else {
            Some(false)
        };

        let conn = test_conn();
        insert_test_sandbox(
            &conn,
            TestSandboxInsert::new("held", "held0001", SandboxState::Started),
        );
        quarantine(&conn, "held");
        let before = persisted(&conn, "held");
        assert_eq!(writer(&conn, "held"), refused, "{writer_name}: quarantined");
        assert_eq!(
            persisted(&conn, "held"),
            before,
            "{writer_name} must not take a quarantined row into cleanup"
        );

        // `mark_cleanup_required` reports no outcome, so a missing row says
        // nothing about it.
        if refused.is_some() {
            let before = persisted(&conn, "missing");
            assert_eq!(writer(&conn, "missing"), refused, "{writer_name}: missing");
            assert_eq!(
                persisted(&conn, "missing"),
                before,
                "{writer_name} must not create a row"
            );
        }
    }
}

#[test]
fn cleanup_required_in_state_writes_only_a_row_in_the_expected_state() {
    const EXPECTED: SandboxState = SandboxState::Started;
    // A quarantined row is refused regardless of the expected state; see
    // `generic_cleanup_writers_refuse_quarantined_and_missing_rows`.
    for state in SandboxState::all()
        .into_iter()
        .filter(|state| *state != SandboxState::Quarantined)
    {
        let conn = test_conn();
        insert_test_sandbox(&conn, TestSandboxInsert::new("sandbox", "instate1", state));
        let before = persisted(&conn, "sandbox");

        let wrote = mark_cleanup_required_if_no_intent_in_state(
            &conn,
            "sandbox",
            EXPECTED,
            true,
            CleanupCompletion::MarkStopped,
            "observer of a started row",
        )
        .unwrap();

        assert_eq!(wrote, state == EXPECTED, "{state:?}");
        if wrote {
            let row = get_sandbox(&conn, "sandbox").unwrap().unwrap();
            assert_eq!(row.state, SandboxState::CleanupRequired);
            assert_eq!(row.cleanup_completion, Some(CleanupCompletion::MarkStopped));
            assert_eq!(row.cleanup_keep_filesystem, Some(true));
        } else {
            assert_eq!(persisted(&conn, "sandbox"), before, "{state:?}");
        }
    }
}

#[test]
fn quarantining_a_missing_sandbox_writes_no_quarantine_evidence() {
    let conn = test_conn();
    let before = persisted(&conn, "missing");

    mark_quarantined(
        &conn,
        "missing",
        SandboxQuarantineSource::SbxManager,
        SandboxQuarantineReasonCode::RuntimeProbeInconsistent,
        QUARANTINE_REASON,
        SandboxQuarantineNextAction::RequiresOperatorAttention,
    )
    .unwrap();
    assert!(
        !mark_starting_quarantined(
            &conn,
            "missing",
            SandboxQuarantineReasonCode::ColdActivationInterrupted,
            "start failed",
        )
        .unwrap()
    );

    assert_eq!(persisted(&conn, "missing"), before);
}

#[test]
fn cleanup_request_is_superseded_without_the_owned_episode_and_intent() {
    type Setup = fn(&Connection, &str) -> SandboxCleanupEpisodeId;
    let cases: [(&str, Setup); 6] = [
        ("no episode", |conn, id| {
            insert_test_sandbox(
                conn,
                TestSandboxInsert::new(id, "nocln001", SandboxState::Starting),
            );
            SandboxCleanupEpisodeId::new()
        }),
        ("stale episode id", |conn, id| {
            armed_start(conn, id, "stale001");
            SandboxCleanupEpisodeId::new()
        }),
        ("row quarantined meanwhile", |conn, id| {
            let episode = armed_start(conn, id, "quar0001");
            quarantine(conn, id);
            episode.id
        }),
        ("row intent no longer the episode's", |conn, id| {
            let episode = armed_start(conn, id, "intent01");
            clear_cleanup_intent(conn, id).unwrap();
            episode.id
        }),
        ("row completion no longer the episode's", |conn, id| {
            let episode = armed_start(conn, id, "compl001");
            change_intent_completion(conn, id);
            episode.id
        }),
        ("row keep-filesystem no longer the episode's", |conn, id| {
            let episode = armed_start(conn, id, "keepfs01");
            change_intent_keep_filesystem(conn, id);
            episode.id
        }),
    ];
    for (case, setup) in cases {
        let conn = test_conn();
        let cleanup_id = setup(&conn, "sandbox");
        let before = persisted(&conn, "sandbox");

        assert_eq!(
            request_cleanup_episode(
                &conn,
                "sandbox",
                cleanup_id,
                SandboxQuarantineReasonCode::ColdActivationInterrupted,
                "late failure",
            )
            .unwrap(),
            CleanupEpisodeRequestOutcome::Superseded,
            "{case}"
        );
        assert_eq!(persisted(&conn, "sandbox"), before, "{case}");
    }
}

#[test]
fn cleanup_request_is_superseded_once_the_row_is_deleting_or_quarantined() {
    // An eviction claim can move a row to `deleting` while its armed episode
    // and intent remain, so only the row state separates these outcomes.
    for state in SandboxState::all() {
        let conn = test_conn();
        let episode = armed_start(&conn, "sandbox", "rqstate1");
        set_state(&conn, "sandbox", state);
        let before = persisted(&conn, "sandbox");

        let outcome = request_cleanup_episode(
            &conn,
            "sandbox",
            episode.id,
            SandboxQuarantineReasonCode::ColdActivationInterrupted,
            "start failed",
        )
        .unwrap();

        if matches!(state, SandboxState::Deleting | SandboxState::Quarantined) {
            assert_eq!(
                outcome,
                CleanupEpisodeRequestOutcome::Superseded,
                "{state:?}"
            );
            assert_eq!(persisted(&conn, "sandbox"), before, "{state:?}");
        } else {
            assert_eq!(
                outcome,
                CleanupEpisodeRequestOutcome::Requested,
                "{state:?}"
            );
            let after = persisted(&conn, "sandbox");
            assert_eq!(
                after.sandbox.map(|row| row.state),
                Some(SandboxState::CleanupRequired),
                "{state:?}"
            );
            assert_eq!(
                after.episode.map(|episode| episode.phase).as_deref(),
                Some("requested"),
                "{state:?}"
            );
        }
    }
}

#[test]
fn a_requested_cleanup_episode_whose_request_is_superseded_reports_superseded() {
    let conn = test_conn();
    let episode = requested_start(&conn, "sandbox", "reqsup01");
    clear_cleanup_intent(&conn, "sandbox").unwrap();
    let before = persisted(&conn, "sandbox");

    assert_eq!(
        request_cleanup_episode(
            &conn,
            "sandbox",
            episode.id,
            SandboxQuarantineReasonCode::CreateInterrupted,
            "outer observer",
        )
        .unwrap(),
        CleanupEpisodeRequestOutcome::Superseded,
    );
    assert_eq!(persisted(&conn, "sandbox"), before);
}

#[test]
fn cleanup_request_fails_closed_when_the_armed_phase_does_not_update() {
    // The decoder reads "requested" from `requested_at` alone, so a stored
    // `requested` phase without a timestamp passes the armed pre-check but not
    // the phase write's `phase = 'armed'` guard. The schema's CHECK rejects
    // the row, so it is seeded with CHECK enforcement off. The sandbox write
    // must roll back with the failed phase write.
    let conn = test_conn();
    let episode = armed_start(&conn, "sandbox", "phase001");
    conn.pragma_update(None, "ignore_check_constraints", true)
        .unwrap();
    conn.execute(
        "UPDATE sandbox_cleanup_episodes
            SET phase = 'requested', requested_at = NULL
          WHERE sandbox_id = 'sandbox'",
        [],
    )
    .unwrap();
    conn.pragma_update(None, "ignore_check_constraints", false)
        .unwrap();
    let before = persisted(&conn, "sandbox");

    let result = request_cleanup_episode(
        &conn,
        "sandbox",
        episode.id,
        SandboxQuarantineReasonCode::ColdActivationInterrupted,
        "start failed",
    );

    assert!(
        matches!(
            result,
            Err(RequestCleanupEpisodeError::PhaseUpdateInvariant { changed: 0 })
        ),
        "{result:?}"
    );
    assert_eq!(persisted(&conn, "sandbox"), before);
}

#[test]
fn cleanup_failure_is_recorded_only_against_the_current_requested_episode() {
    type Setup = fn(&Connection, &str) -> SandboxCleanupEpisodeId;
    let cases: [(&str, Setup); 3] = [
        ("no episode", |conn, id| {
            insert_test_sandbox(
                conn,
                TestSandboxInsert::new(id, "nocln002", SandboxState::CleanupRequired),
            );
            SandboxCleanupEpisodeId::new()
        }),
        ("armed, never requested", |conn, id| {
            armed_start(conn, id, "armed002").id
        }),
        ("stale episode id", |conn, id| {
            requested_start(conn, id, "stale002");
            SandboxCleanupEpisodeId::new()
        }),
    ];
    for (case, setup) in cases {
        let conn = test_conn();
        let cleanup_id = setup(&conn, "sandbox");
        let before = persisted(&conn, "sandbox");

        assert!(
            !record_cleanup_episode_failure(&conn, "sandbox", cleanup_id, "cleanup failed")
                .unwrap(),
            "{case}"
        );
        assert_eq!(persisted(&conn, "sandbox"), before, "{case}");
    }
}

#[test]
fn cleanup_failure_against_a_changed_intent_records_evidence_and_gates_state_on_completion() {
    type ChangeIntent = fn(&Connection, &str);
    let cases: [(&str, ChangeIntent, SandboxState); 3] = [
        (
            "intent cleared",
            |conn, id| clear_cleanup_intent(conn, id).unwrap(),
            SandboxState::CleanupRequired,
        ),
        (
            "completion changed",
            change_intent_completion,
            SandboxState::CleanupRequired,
        ),
        // Unlike `request_cleanup_episode`, `quarantine_cleanup_episode` and
        // `cleanup_episode_current`, this writer's state predicate matches the
        // episode's completion but not its keep-filesystem flag, so a row whose
        // flag alone changed still enters `cleanup_failed`. Unifying the
        // predicates changes this outcome. `mark_cleanup_required` can leave
        // this shape, because it overwrites both intent columns.
        (
            "keep-filesystem changed",
            change_intent_keep_filesystem,
            SandboxState::CleanupFailed,
        ),
    ];
    for (case, change_intent, expected_state) in cases {
        let conn = test_conn();
        let episode = requested_start(&conn, "sandbox", "failint1");
        change_intent(&conn, "sandbox");

        assert!(
            record_cleanup_episode_failure(&conn, "sandbox", episode.id, "cleanup failed").unwrap(),
            "{case}"
        );

        let row = get_sandbox(&conn, "sandbox").unwrap().unwrap();
        assert_eq!(row.state, expected_state, "{case}");
        let episode = cleanup_episode(&conn, "sandbox").unwrap().unwrap();
        assert_eq!(
            episode.last_cleanup_error.as_deref(),
            Some("cleanup failed"),
            "{case}"
        );
        assert_eq!(episode.attempt_count, 1, "{case}");
    }
}

#[test]
fn quarantine_cleanup_refuses_without_a_current_requested_episode() {
    type Setup = fn(&Connection, &str) -> SandboxCleanupEpisodeId;
    let cases: [(&str, Setup); 8] = [
        ("no episode", |conn, id| {
            insert_test_sandbox(
                conn,
                TestSandboxInsert::new(id, "nocln003", SandboxState::CleanupRequired),
            );
            SandboxCleanupEpisodeId::new()
        }),
        ("stale episode id", |conn, id| {
            requested_start(conn, id, "stale003");
            SandboxCleanupEpisodeId::new()
        }),
        // A generic writer moved the row into cleanup under the episode's own
        // intent, but the episode was never requested and carries no reason.
        ("armed, never requested", |conn, id| {
            let episode = armed_start(conn, id, "armed003");
            mark_cleanup_required(
                conn,
                id,
                episode.keep_filesystem,
                episode.completion,
                "observer",
            )
            .unwrap();
            assert_eq!(
                get_sandbox(conn, id).unwrap().unwrap().state,
                SandboxState::CleanupRequired
            );
            episode.id
        }),
        // The service opens SQLite without foreign-key enforcement, so an
        // episode can outlive its sandbox row.
        ("sandbox row gone", |conn, id| {
            let episode = requested_start(conn, id, "gone0003");
            let foreign_keys: bool = conn
                .pragma_query_value(None, "foreign_keys", |row| row.get(0))
                .unwrap();
            conn.pragma_update(None, "foreign_keys", false).unwrap();
            conn.execute("DELETE FROM sandboxes WHERE id = ?1", params![id])
                .unwrap();
            conn.pragma_update(None, "foreign_keys", foreign_keys)
                .unwrap();
            assert!(cleanup_episode(conn, id).unwrap().is_some());
            episode.id
        }),
        ("row quarantined meanwhile", |conn, id| {
            let episode = requested_start(conn, id, "quar0003");
            quarantine(conn, id);
            episode.id
        }),
        ("row intent no longer the episode's", |conn, id| {
            let episode = requested_start(conn, id, "intent03");
            clear_cleanup_intent(conn, id).unwrap();
            episode.id
        }),
        ("row completion no longer the episode's", |conn, id| {
            let episode = requested_start(conn, id, "compl003");
            change_intent_completion(conn, id);
            episode.id
        }),
        ("row keep-filesystem no longer the episode's", |conn, id| {
            let episode = requested_start(conn, id, "keepfs03");
            change_intent_keep_filesystem(conn, id);
            episode.id
        }),
    ];
    for (case, setup) in cases {
        let conn = test_conn();
        let cleanup_id = setup(&conn, "sandbox");
        let before = persisted(&conn, "sandbox");

        assert!(
            !quarantine_cleanup_episode(&conn, "sandbox", cleanup_id, "unsafe evidence").unwrap(),
            "{case}"
        );
        assert_eq!(persisted(&conn, "sandbox"), before, "{case}");
    }
}

#[test]
fn quarantine_cleanup_accepts_only_a_row_in_a_cleanup_state() {
    // Each row keeps the current requested episode and its intent, so only the
    // row state decides the outcome.
    for state in SandboxState::all() {
        let conn = test_conn();
        let episode = requested_start(&conn, "sandbox", "qcstate1");
        set_state(&conn, "sandbox", state);
        let before = persisted(&conn, "sandbox");

        let quarantined =
            quarantine_cleanup_episode(&conn, "sandbox", episode.id, "unsafe evidence").unwrap();

        if matches!(
            state,
            SandboxState::CleanupRequired | SandboxState::CleanupFailed
        ) {
            assert!(quarantined, "{state:?}");
            let after = persisted(&conn, "sandbox");
            assert_eq!(
                after.sandbox.map(|row| row.state),
                Some(SandboxState::Quarantined),
                "{state:?}"
            );
            assert_eq!(after.episode, None, "{state:?}");
            assert!(after.quarantine.is_some(), "{state:?}");
        } else {
            assert!(!quarantined, "{state:?}");
            assert_eq!(persisted(&conn, "sandbox"), before, "{state:?}");
        }
    }
}

#[test]
fn stale_armed_cleanup_retires_only_after_the_row_left_its_activation_state() {
    type Setup = fn(&Connection, &str) -> (SandboxCleanupEpisodeId, SandboxState);
    let refusals: [(&str, Setup); 4] = [
        ("no episode", |conn, id| {
            insert_test_sandbox(
                conn,
                TestSandboxInsert::new(id, "nocln004", SandboxState::Quarantined),
            );
            (SandboxCleanupEpisodeId::new(), SandboxState::Quarantined)
        }),
        ("stale episode id", |conn, id| {
            armed_start(conn, id, "stale004");
            quarantine(conn, id);
            (SandboxCleanupEpisodeId::new(), SandboxState::Quarantined)
        }),
        ("episode already requested", |conn, id| {
            let episode = requested_start(conn, id, "reqst004");
            quarantine(conn, id);
            (episode.id, SandboxState::Quarantined)
        }),
        ("row still in the activation state", |conn, id| {
            (armed_start(conn, id, "activ004").id, SandboxState::Starting)
        }),
    ];
    for (case, setup) in refusals {
        let conn = test_conn();
        let (cleanup_id, observed) = setup(&conn, "sandbox");
        let before = persisted(&conn, "sandbox");

        assert!(
            !retire_stale_armed_cleanup_episode(&conn, "sandbox", cleanup_id, observed).unwrap(),
            "{case}"
        );
        assert_eq!(persisted(&conn, "sandbox"), before, "{case}");
    }

    let conn = test_conn();
    let episode = armed_start(&conn, "sandbox", "retire04");
    quarantine(&conn, "sandbox");
    assert!(
        retire_stale_armed_cleanup_episode(&conn, "sandbox", episode.id, SandboxState::Quarantined)
            .unwrap()
    );
    assert!(cleanup_episode(&conn, "sandbox").unwrap().is_none());
    let row = get_sandbox(&conn, "sandbox").unwrap().unwrap();
    assert_eq!(row.state, SandboxState::Quarantined);
    assert_eq!(row.cleanup_completion, None);
    assert_eq!(row.cleanup_keep_filesystem, None);
    assert!(
        !retire_stale_armed_cleanup_episode(
            &conn,
            "sandbox",
            episode.id,
            SandboxState::Quarantined
        )
        .unwrap(),
        "a retired episode cannot be retired twice"
    );
}

#[test]
fn cleanup_episode_decoder_rejects_rows_it_cannot_represent() {
    // Every case overwrites a valid armed `mark_stopped` episode. The schema's
    // CHECK constraints reject some of these rows, so they are seeded with
    // CHECK enforcement off; the decoder must still fail rather than guess.
    let cases: [(&str, &str); 13] = [
        (
            "malformed cleanup id",
            "UPDATE sandbox_cleanup_episodes SET cleanup_id = 'not-a-uuid'",
        ),
        (
            "unknown completion",
            "UPDATE sandbox_cleanup_episodes SET completion = 'mystery'",
        ),
        (
            "unknown entered-from state",
            "UPDATE sandbox_cleanup_episodes SET entered_from_state = 'mystery'",
        ),
        (
            "unknown activation state",
            "UPDATE sandbox_cleanup_episodes SET activation_state = 'mystery'",
        ),
        (
            "unknown quarantine reason",
            "UPDATE sandbox_cleanup_episodes SET quarantine_reason_code = 'mystery'",
        ),
        (
            "recovery artifact without generation",
            "UPDATE sandbox_cleanup_episodes
                SET recovery_local_artifact_id = '0192a5b0-0000-7000-8000-000000000001'",
        ),
        (
            "recovery generation without artifact",
            "UPDATE sandbox_cleanup_episodes SET recovery_execution_generation = 1",
        ),
        (
            "malformed recovery artifact",
            "UPDATE sandbox_cleanup_episodes
                SET recovery_local_artifact_id = 'not-a-uuid',
                    recovery_execution_generation = 1",
        ),
        (
            "non-positive recovery generation",
            "UPDATE sandbox_cleanup_episodes
                SET recovery_local_artifact_id = '0192a5b0-0000-7000-8000-000000000001',
                    recovery_execution_generation = 0",
        ),
        (
            "non-positive execution generation",
            "UPDATE sandbox_cleanup_episodes SET execution_generation = 0",
        ),
        (
            "malformed activation attempt",
            "UPDATE sandbox_cleanup_episodes SET activation_attempt_id = 'not-a-uuid'",
        ),
        (
            "malformed operation activation attempt",
            "UPDATE sandbox_cleanup_episodes SET operation_activation_attempt_id = 'not-a-uuid'",
        ),
        (
            "malformed prepared clone root",
            "UPDATE sandbox_cleanup_episodes
                SET completion = 'delete_pool_clone',
                    keep_filesystem = 0,
                    activation_state = 'creating',
                    execution_generation = NULL,
                    activation_attempt_id = NULL,
                    prepared_clone_root = '{'",
        ),
    ];
    for (case, corruption) in cases {
        let conn = test_conn();
        armed_start(&conn, "sandbox", "decode01");
        assert!(
            cleanup_episode(&conn, "sandbox").unwrap().is_some(),
            "{case}"
        );
        conn.pragma_update(None, "ignore_check_constraints", true)
            .unwrap();
        assert_eq!(conn.execute(corruption, []).unwrap(), 1, "{case}");
        conn.pragma_update(None, "ignore_check_constraints", false)
            .unwrap();

        let result = cleanup_episode(&conn, "sandbox");
        assert!(
            matches!(result, Err(rusqlite::Error::FromSqlConversionFailure(..))),
            "{case}: {result:?}"
        );
    }
}
