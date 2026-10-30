mod state_writer_refusal;

pub(crate) use super::test_fixtures::{
    TestSandboxInsert, database_rows, insert_test_sandbox, quarantine_claim_state,
    record_quarantine_recovery_attempt, setup_quarantine_recovery_candidate,
    setup_quarantine_recovery_episode, state_write_fixtures,
};
use super::*;
use crate::restore_operation::RestoreOperation;

#[test]
fn execution_preflight_reports_legacy_rows_without_changing_them() {
    let conn = test_conn();
    assert!(unbound_node_executions(&conn).unwrap().is_empty());
    let mut ids = Vec::new();
    for index in 0..65 {
        let id = SandboxId::new().to_string();
        let params = TestSandboxInsert::new(&id, format!("adopt{index:03}"), SandboxState::Stopped);
        insert_sandbox(
            &conn,
            params.insert_params(
                &SandboxRuntimeLabel::parse(params.uuid8.clone()).unwrap(),
                &SandboxOwner::parse(params.user.clone()).unwrap(),
            ),
        )
        .unwrap();
        assert!(!unbound_node_executions(&conn).unwrap().contains(&id));
        conn.execute(
            "UPDATE sandboxes SET node_execution_id = NULL WHERE id = ?1",
            params![id],
        )
        .unwrap();
        ids.push(id);
        ids.sort();
        let missing = unbound_node_executions(&conn).unwrap();
        assert_eq!(missing, ids[..ids.len().min(64)]);
    }
    for id in ids {
        assert_eq!(node_execution_fence(&conn, &id).unwrap(), None);
    }
}

#[test]
fn node_execution_cleanup_acknowledgements_are_retryable_and_fenced() {
    let conn = test_conn();
    let id = SandboxId::new().to_string();
    let staging_cleanup = SandboxCleanupEpisodeId::new();
    let params = TestSandboxInsert::new(&id, "fence001", SandboxState::Creating);
    insert_sandbox_with_cleanup_episode_and_route_evidence(
        &conn,
        params.insert_params(
            &SandboxRuntimeLabel::parse(params.uuid8.clone()).unwrap(),
            &SandboxOwner::parse(params.user.clone()).unwrap(),
        ),
        CleanupEpisodeSpec {
            id: staging_cleanup,
            completion: CleanupCompletion::DeleteRow,
            keep_filesystem: false,
            entered_from_state: None,
            activation_state: SandboxState::Creating,
            execution_generation: None,
            activation_attempt_id: None,
        },
        None,
        None,
        None,
    )
    .unwrap();
    let staged = node_execution_fence(&conn, &id).unwrap().unwrap();
    assert!(staged.previous().is_none());
    state_write_fixtures::apply_edge(&conn, &id, &edges::CREATE_COMPLETE);
    assert!(complete_create(&conn, &id, staging_cleanup, false).unwrap());
    let activate = || {
        let episode = begin_activation(
            &conn,
            &id,
            SandboxState::Created,
            ActivationKind::ColdStart,
            None,
            None,
        )
        .unwrap()
        .unwrap();
        let fence = node_execution_fence(&conn, &id).unwrap().unwrap();
        assert!(
            begin_activation(
                &conn,
                &id,
                SandboxState::Created,
                ActivationKind::ColdStart,
                None,
                None,
            )
            .unwrap()
            .is_none()
        );
        assert_eq!(node_execution_fence(&conn, &id).unwrap(), Some(fence));
        request_cleanup_episode(
            &conn,
            &id,
            episode.id,
            SandboxQuarantineReasonCode::ColdActivationInterrupted,
            "interrupted activation",
        )
        .unwrap();
        assert!(
            complete_cleanup_episode(&conn, &id, episode.id)
                .unwrap()
                .is_some()
        );
        fence
    };
    let first = activate();
    assert_ne!(first.current(), staged.current());
    assert!(first.previous().is_none(), "staging was never allocated");
    assert!(acknowledge_node_cleanup(&conn, &id, first, Some(first.current())).unwrap());
    let second = activate();
    assert_eq!(second.previous(), Some(first.current()));
    assert!(!acknowledge_node_cleanup(&conn, &id, first, None).unwrap());
    // Allocate never reached the receiver: retained cleanup acknowledges the
    // predecessor, which must become the next activation's predecessor too.
    assert!(acknowledge_node_cleanup(&conn, &id, second, Some(first.current())).unwrap());
    let third = activate();
    assert_eq!(third.previous(), Some(first.current()));
    assert!(acknowledge_node_cleanup(&conn, &id, third, None).unwrap());
    let retry = node_execution_fence(&conn, &id).unwrap().unwrap();
    assert_eq!(retry.current(), third.current());
    assert!(acknowledge_node_cleanup(&conn, &id, retry, None).unwrap());
    let fourth = activate();
    assert!(
        fourth.previous().is_none(),
        "retired authority cannot authorize a new binding"
    );
    assert!(!acknowledge_node_cleanup(&conn, &id, retry, None).unwrap());
    assert_eq!(
        get_sandbox(&conn, &id).unwrap().unwrap().node_execution_id,
        Some(fourth.current())
    );
}
use pplx_space_common_core::types::{
    Arch, ArtifactComponentKind, ArtifactComponentMetadata, ArtifactFormat, ArtifactVersion,
    AutoLifecycleConditionSet, AutoLifecycleConditionSetId, AutoLifecycleIdleAction,
    AutoLifecycleStage, Digest, ExecutionMetadata, LinuxPid, LinuxUid, SandboxActivationAttemptId,
    SandboxId, UserId, VsockCid,
};
use uuid::Uuid;

#[test]
fn persisted_clock_rejects_pre_epoch_instead_of_fabricating_zero() {
    for timestamp in ["1969-12-31T23:59:59Z", "1969-12-31T23:59:59.999999999Z"] {
        let error = persisted_epoch_seconds(timestamp.parse().unwrap()).unwrap_err();
        assert!(matches!(
            error.raw(),
            rusqlite::Error::ToSqlConversionFailure(cause)
                if cause.is::<PersistedClockBeforeEpoch>()
        ));
    }
    for (timestamp, seconds) in [
        ("1970-01-01T00:00:00Z", 0),
        ("1970-01-01T00:00:01.999Z", 1),
        ("2026-01-01T00:00:00Z", 1_767_225_600),
    ] {
        assert_eq!(
            persisted_epoch_seconds(timestamp.parse().unwrap()).unwrap(),
            seconds
        );
    }
}

#[test]
fn periodic_recovery_bounds_discovery_and_advances_past_retained_or_malformed_rows() {
    let conn = test_conn();
    // A sparse inventory must not make discovery scan until it fills a batch.
    for _ in 0..128 {
        let id = SandboxId::new().to_string();
        insert_test_sandbox(
            &conn,
            TestSandboxInsert::new(&id, "sandbox1", SandboxState::Started),
        );
    }
    let corrupt = SandboxId::new().to_string();
    insert_test_sandbox(
        &conn,
        TestSandboxInsert::new(&corrupt, "sandbox1", SandboxState::Paused),
    );
    conn.execute(
        "UPDATE sandboxes SET process_env_json = 'malformed' WHERE id = ?1",
        [&corrupt],
    )
    .unwrap();
    let mut expected = Vec::new();
    for state in [
        SandboxState::Paused,
        SandboxState::Resuming,
        SandboxState::Checkpointing,
        SandboxState::Suspending,
    ]
    .into_iter()
    .cycle()
    .take(9)
    {
        let id = SandboxId::new().to_string();
        insert_test_sandbox(&conn, TestSandboxInsert::new(&id, "sandbox1", state));
        expected.push(id);
        if expected.len() == 1 {
            // SQLite TEXT affinity does not reject BLOBs. A bad storage class
            // after a valid candidate must not discard it on every sweep.
            for corrupt_column in [
                "UPDATE sandboxes SET state = X'00' WHERE id = ?1",
                "UPDATE sandboxes SET id = X'01' WHERE id = ?1",
            ] {
                let id = SandboxId::new().to_string();
                insert_test_sandbox(
                    &conn,
                    TestSandboxInsert::new(&id, "sandbox1", SandboxState::Paused),
                );
                seed_historical_lifecycle_rows(&conn, |conn| {
                    conn.execute(corrupt_column, [&id]).unwrap();
                });
            }
        }
    }
    let mut exits = PeriodicLifecycleRecoveryScan::runtime_exits();
    let running = periodic_lifecycle_recovery_batch(&conn, &mut exits).unwrap();
    assert_eq!(running.len(), 128);
    assert!(running.iter().all(|row| row.state == SandboxState::Started));
    let mut cursor = PeriodicLifecycleRecoveryScan::default();
    assert!(
        periodic_lifecycle_recovery_batch(&conn, &mut cursor)
            .unwrap()
            .is_empty()
    );
    let first = periodic_lifecycle_recovery_batch(&conn, &mut cursor).unwrap();
    assert_eq!(
        first.iter().map(|row| &row.id).collect::<Vec<_>>(),
        expected[..8].iter().collect::<Vec<_>>()
    );
    // New work cannot extend this sweep and postpone retrying the first page.
    let fresh = SandboxId::new().to_string();
    insert_test_sandbox(
        &conn,
        TestSandboxInsert::new(&fresh, "sandbox1", SandboxState::Paused),
    );
    // Leave candidates unchanged, as when every operation fence is busy.
    let next = periodic_lifecycle_recovery_batch(&conn, &mut cursor).unwrap();
    assert_eq!(
        next.iter().map(|row| &row.id).collect::<Vec<_>>(),
        vec![&expected[8]]
    );
    assert!(
        periodic_lifecycle_recovery_batch(&conn, &mut cursor)
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        periodic_lifecycle_recovery_batch(&conn, &mut cursor).unwrap()[0].id,
        expected[0]
    );
    let tail = periodic_lifecycle_recovery_batch(&conn, &mut cursor).unwrap();
    assert_eq!(
        tail.iter().map(|row| &row.id).collect::<Vec<_>>(),
        vec![&expected[8], &fresh]
    );
    assert!(
        get_sandbox(&conn, &corrupt).is_err(),
        "discovery does not delete malformed evidence"
    );
}

#[test]
fn cleanup_intent_cannot_be_replaced_by_suspend_or_resident_completion() {
    let conn = test_conn();
    for cleanup_state in [
        SandboxState::Deleting,
        SandboxState::CleanupRequired,
        SandboxState::CleanupFailed,
    ] {
        let id = SandboxId::new().to_string();
        insert_test_sandbox(
            &conn,
            TestSandboxInsert::new(&id, "sandbox1", cleanup_state),
        );
        assert!(set_cleanup_intent(&conn, &id, false, CleanupCompletion::DeleteRow).unwrap());
        for stale_completion in [
            &edges::PAUSE_BEGIN,
            &edges::PAUSE_COMPLETE,
            &edges::PAUSE_ROLLBACK,
            &edges::RESUME_BEGIN,
        ] {
            assert_eq!(
                update_state_from(&conn, &id, stale_completion).unwrap(),
                StateWriteOutcome::Refused {
                    found: cleanup_state,
                    refusal: StateWriteRefusal::Guard(StateWriteGuard::CleanupOwned),
                }
            );
            assert!(!begin_suspend(&conn, &id).unwrap());
            let row = get_sandbox(&conn, &id).unwrap().unwrap();
            assert_eq!(row.state, cleanup_state);
            assert_eq!(row.cleanup_completion, Some(CleanupCompletion::DeleteRow));
        }
    }
    let id = SandboxId::new().to_string();
    insert_test_sandbox(
        &conn,
        TestSandboxInsert::new(&id, "sandbox2", SandboxState::Paused),
    );
    assert!(begin_suspend(&conn, &id).unwrap());
    assert!(!begin_suspend(&conn, &id).unwrap());
}

#[test]
fn application_state_write_updates_only_state_changed_at() {
    // Arrange
    let conn = test_conn();
    let sandbox_id = SandboxId::new();
    let sandbox_id = sandbox_id.to_string();
    insert_test_sandbox(
        &conn,
        TestSandboxInsert::new(
            &sandbox_id,
            SandboxRuntimeLabel::from_sandbox_id(sandbox_id.parse().unwrap()).to_string(),
            SandboxState::Started,
        ),
    );
    conn.execute(
        "UPDATE sandboxes SET state_changed_at = '123', updated_at = '123' WHERE id = ?1",
        params![sandbox_id],
    )
    .unwrap();

    // Act
    let outcome =
        update_state_from_states(&conn, &sandbox_id, StateSet::ALL, SandboxState::Paused).unwrap();
    let after_transition: (String, String) = conn
        .query_row(
            "SELECT state_changed_at, updated_at FROM sandboxes WHERE id = ?1",
            params![sandbox_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    let retry_outcome =
        update_state_from_states(&conn, &sandbox_id, StateSet::ALL, SandboxState::Paused).unwrap();
    let after_retry: String = conn
        .query_row(
            "SELECT state_changed_at FROM sandboxes WHERE id = ?1",
            params![sandbox_id],
            |row| row.get(0),
        )
        .unwrap();
    conn.execute(
        "UPDATE sandboxes SET updated_at = '999' WHERE id = ?1",
        params![sandbox_id],
    )
    .unwrap();

    // Assert
    let after_unrelated_write: String = conn
        .query_row(
            "SELECT state_changed_at FROM sandboxes WHERE id = ?1",
            params![sandbox_id],
            |row| row.get(0),
        )
        .unwrap();
    assert_ne!(after_transition.0, "123");
    assert_eq!(
        outcome,
        StateWriteOutcome::Applied {
            from: SandboxState::Started
        }
    );
    assert_eq!(after_transition.0, after_transition.1);
    assert_eq!(
        retry_outcome,
        StateWriteOutcome::Applied {
            from: SandboxState::Paused
        }
    );
    assert_eq!(after_retry, after_transition.0);
    assert_eq!(after_unrelated_write, after_transition.0);
}

#[test]
fn every_allowed_state_transition_tracks_the_change_time() {
    // Arrange
    let conn = test_conn();

    for (from_index, from) in SandboxState::all().into_iter().enumerate() {
        for (to_index, to) in SandboxState::all().into_iter().enumerate() {
            if from == to || !from.can_transition_to(to) {
                continue;
            }
            let sandbox_id = format!("edge-{from_index}-{to_index}");
            let uuid8 = format!("e{from_index:02}{to_index:02}");
            insert_test_sandbox(&conn, TestSandboxInsert::new(&sandbox_id, &uuid8, from));
            conn.execute(
                "UPDATE sandboxes
                 SET state_changed_at = '123', updated_at = '123'
                 WHERE id = ?1",
                params![sandbox_id],
            )
            .unwrap();

            // Act
            if to == SandboxState::Quarantined {
                mark_quarantined(
                    &conn,
                    &sandbox_id,
                    SandboxQuarantineSource::SbxManager,
                    SandboxQuarantineReasonCode::RuntimeProbeInconsistent,
                    "state transition coverage",
                    SandboxQuarantineNextAction::RequiresOperatorAttention,
                )
                .unwrap_or_else(|error| panic!("{from} -> {to}: {error}"));
            } else {
                assert_eq!(
                    update_state_from_states(&conn, &sandbox_id, StateSet::of(&[from]), to)
                        .unwrap(),
                    StateWriteOutcome::Applied { from },
                    "{from} -> {to}"
                );
            }

            // Assert
            let (state, state_changed_at, updated_at): (String, String, String) = conn
                .query_row(
                    "SELECT state, state_changed_at, updated_at
                     FROM sandboxes
                     WHERE id = ?1",
                    params![sandbox_id],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
                )
                .unwrap();
            assert_eq!(state, to.as_str(), "{from} -> {to}");
            assert_ne!(state_changed_at, "123", "{from} -> {to}");
            assert_eq!(state_changed_at, updated_at, "{from} -> {to}");
        }
    }
}

#[test]
fn specialized_state_writers_track_the_change_time() {
    // Arrange
    let conn = test_conn();
    for (id, uuid8, state) in [
        ("restore", "restore1", SandboxState::Restoring),
        ("cleanup", "cleanup1", SandboxState::Started),
        ("quarantine", "quarant1", SandboxState::Starting),
    ] {
        insert_test_sandbox(&conn, TestSandboxInsert::new(id, uuid8, state));
        conn.execute(
            "UPDATE sandboxes
             SET state_changed_at = '123', updated_at = '123'
             WHERE id = ?1",
            params![id],
        )
        .unwrap();
    }

    // Act
    assert!(mark_restore_succeeded(&conn, "restore", SandboxState::Paused, None).unwrap());
    mark_cleanup_required(
        &conn,
        "cleanup",
        false,
        CleanupCompletion::DeleteRow,
        "cleanup required",
    )
    .unwrap();
    mark_quarantined(
        &conn,
        "quarantine",
        SandboxQuarantineSource::SbxManager,
        SandboxQuarantineReasonCode::RuntimeProbeInconsistent,
        "runtime evidence is inconsistent",
        SandboxQuarantineNextAction::RequiresOperatorAttention,
    )
    .unwrap();

    // Assert
    for (id, expected_state) in [
        ("restore", SandboxState::Paused),
        ("cleanup", SandboxState::CleanupRequired),
        ("quarantine", SandboxState::Quarantined),
    ] {
        let (state, state_changed_at, updated_at): (String, String, String) = conn
            .query_row(
                "SELECT state, state_changed_at, updated_at
                 FROM sandboxes
                 WHERE id = ?1",
                params![id],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();
        assert_eq!(state, expected_state.as_str(), "{id}");
        assert_ne!(state_changed_at, "123", "{id}");
        assert_eq!(state_changed_at, updated_at, "{id}");
    }
}

fn route_identity_columns(
    conn: &Connection,
    sandbox_id: &str,
) -> (Option<i64>, Option<String>, Option<i64>, Option<String>) {
    conn.query_row(
        "SELECT space_api_route_execution_generation,
                space_api_route_activation_attempt_id,
                last_observed_execution_generation,
                last_observed_route_activation_id
           FROM sandboxes
          WHERE id = ?1",
        params![sandbox_id],
        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
    )
    .unwrap()
}

fn retained_route_provenance_columns(
    conn: &Connection,
    sandbox_id: &str,
) -> (Option<i64>, Option<String>) {
    conn.query_row(
        "SELECT last_observed_route_generation,
                last_observed_route_generation_substrate_kind
           FROM sandboxes
          WHERE id = ?1",
        params![sandbox_id],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )
    .unwrap()
}

#[test]
fn quarantine_recovery_candidates_follow_the_complete_reason_policy() {
    let reasons = [
        SandboxQuarantineReasonCode::Unspecified,
        SandboxQuarantineReasonCode::RuntimeProbeInconsistent,
        SandboxQuarantineReasonCode::SuspendSnapshotPublicationFailed,
        SandboxQuarantineReasonCode::OrphanedSandboxRowMissing,
        SandboxQuarantineReasonCode::CreateInterrupted,
        SandboxQuarantineReasonCode::ColdActivationInterrupted,
        SandboxQuarantineReasonCode::RestoreActivationInterrupted,
        SandboxQuarantineReasonCode::ColdActivationPrepareFailed,
        SandboxQuarantineReasonCode::ColdActivationMetadataFailed,
        SandboxQuarantineReasonCode::ColdActivationEncryptedRootfsPrepareFailed,
        SandboxQuarantineReasonCode::ColdActivationFirecrackerLaunchFailed,
        SandboxQuarantineReasonCode::ColdActivationSidecarLaunchFailed,
        SandboxQuarantineReasonCode::RestorePrepareFailed,
        SandboxQuarantineReasonCode::RestoreMetadataFailed,
        SandboxQuarantineReasonCode::RestoreEncryptedDevicesPrepareFailed,
        SandboxQuarantineReasonCode::RestoreFirecrackerLaunchFailed,
        SandboxQuarantineReasonCode::RestoreNetGatewayLaunchFailed,
        SandboxQuarantineReasonCode::RestoreSnapshotLoadFailed,
        SandboxQuarantineReasonCode::RestoreSpacedHostLaunchFailed,
        SandboxQuarantineReasonCode::RestoreResumeFailed,
        SandboxQuarantineReasonCode::PauseFailed,
        SandboxQuarantineReasonCode::CheckpointResumeFailed,
        SandboxQuarantineReasonCode::ResidentResumeFailed,
        SandboxQuarantineReasonCode::SnapshotRollbackCleanupFailed,
        SandboxQuarantineReasonCode::NodePublishedQuarantined,
        SandboxQuarantineReasonCode::OrphanedExecutionMissingArtifact,
        SandboxQuarantineReasonCode::OrphanedExecutionLegacyDeletingState,
    ];

    // The four reasons are written out rather than derived from
    // `automatic_recovery_policy`, which is the call the scan itself makes:
    // deriving the expectation from the implementation leaves the test able to
    // fail only if one call disagrees with the other, so widening the policy to
    // a fifth reason would pass unnoticed.
    let automatically_recoverable = [
        SandboxQuarantineReasonCode::RestorePrepareFailed,
        SandboxQuarantineReasonCode::RestoreFirecrackerLaunchFailed,
        SandboxQuarantineReasonCode::RestoreSpacedHostLaunchFailed,
        SandboxQuarantineReasonCode::OrphanedSandboxRowMissing,
    ];

    for reason in reasons {
        let conn = crate::db::test_conn();
        let sandbox_id = setup_quarantine_recovery_episode(&conn, reason);
        let candidates = scan_quarantine_recovery_candidates(&conn)
            .unwrap()
            .candidates;
        let expected_count = usize::from(automatically_recoverable.contains(&reason));

        assert_eq!(
            candidates.len(),
            expected_count,
            "candidate policy diverged for {reason}"
        );
        if let Some(candidate) = candidates.first() {
            assert_eq!(candidate.sandbox_id, sandbox_id);
            assert_eq!(candidate.reason_code, reason);
        }
    }
}

#[test]
fn a_claim_reports_the_count_it_wrote_not_the_one_it_read() {
    // The claimed candidate carries exactly one attempt count, and it is the
    // post-claim one. It used to carry two — its own field, plus an embedded
    // candidate still holding the pre-claim value — and either compiled wherever a
    // fence or a backoff was built.
    //
    // Picking the stale one is silent and expensive: the fence pins
    // `attempt_count`, so every fenced write of the operation would miss, report a
    // stale claim, and leave the episode unclaimable until its lease lapsed. This
    // pins the invariant against the stored row so a refactor cannot quietly
    // reintroduce the older value.
    let conn = crate::db::test_conn();
    let sandbox_id = setup_quarantine_recovery_episode(
        &conn,
        SandboxQuarantineReasonCode::OrphanedSandboxRowMissing,
    );
    let candidate = scan_quarantine_recovery_candidates(&conn)
        .unwrap()
        .candidates
        .into_iter()
        .find(|candidate| candidate.sandbox_id == sandbox_id)
        .expect("the seeded episode should be admissible");
    let scanned = candidate.attempt_count;
    let claimed =
        claim_quarantine_recovery_candidate(&conn, &candidate, PlanRejectionRunEffect::Leaves)
            .unwrap()
            .expect("a due episode should be claimable");

    assert_eq!(
        claimed.candidate.attempt_count,
        scanned + 1,
        "the claim charges an attempt, so the claimed candidate must report the \
         incremented count, not the one the scan read"
    );
    let (_, stored, _, _, _) = quarantine_claim_state(&conn, sandbox_id);
    assert_eq!(
        i64::from(claimed.candidate.attempt_count),
        stored,
        "and it must equal what the claim actually wrote, since that is the value \
         every fence pins"
    );
}

#[test]
fn stored_epoch_seconds_accept_integer_and_text_without_coercing_corruption() {
    use rusqlite::types::Value;

    for (stored, expected) in [
        (Value::Null, Some(None)),
        (Value::Text("1700000000".into()), Some(Some("1700000000"))),
        (Value::Integer(1700000000), Some(Some("1700000000"))),
        (Value::Integer(-1), Some(Some("-1"))),
        (Value::Text("invalid".into()), Some(Some("invalid"))),
        (Value::Text("".into()), Some(Some(""))),
        (Value::Real(1700000000.5), None),
        (Value::Blob(b"1700000000".to_vec()), None),
    ] {
        assert_eq!(
            nullable_stored_timestamp_text(stored),
            expected.map(|text| text.map(str::to_owned)),
        );
    }
}

#[test]
fn a_type_mismatched_column_makes_one_row_malformed_not_the_whole_scan() {
    // The scan's contract is that a malformed sibling is skipped so valid
    // episodes still recover. That held for values it parses from text, and not
    // for values it asked rusqlite to convert: this table is not `STRICT`, so a
    // column holding the wrong storage class made `row.get` fail, and that error
    // aborted the pass for every candidate rather than retiring one row.
    let conn = crate::db::test_conn();
    let recoverable = SandboxQuarantineReasonCode::OrphanedSandboxRowMissing;
    let healthy = setup_quarantine_recovery_episode(&conn, recoverable);
    let text_attempts = setup_quarantine_recovery_episode(&conn, recoverable);
    let integer_reason = setup_quarantine_recovery_episode(&conn, recoverable);

    // Two different storage classes in two different columns, because the defect
    // is per-column conversion rather than anything specific to one field.
    conn.execute(
        "UPDATE sandbox_quarantine_state SET attempt_count = 'not-a-number' WHERE sandbox_id = ?1",
        [text_attempts.to_string()],
    )
    .unwrap();
    conn.execute(
        "UPDATE sandbox_quarantine_state SET reason_code = 12345 WHERE sandbox_id = ?1",
        [integer_reason.to_string()],
    )
    .unwrap();

    let scan = scan_quarantine_recovery_candidates(&conn)
        .expect("a malformed sibling must not fail the scan");
    assert_eq!(
        scan.malformed_count, 2,
        "both mistyped rows should be retired as malformed"
    );
    assert!(
        scan.candidates
            .iter()
            .any(|candidate| candidate.sandbox_id == healthy),
        "the healthy episode must still be admitted alongside malformed siblings"
    );
    assert!(
        !scan
            .candidates
            .iter()
            .any(|candidate| candidate.sandbox_id == text_attempts
                || candidate.sandbox_id == integer_reason),
        "a malformed row must not be handed to the recovery loop"
    );
}

#[test]
fn quarantine_recovery_candidate_scan_propagates_statement_failures() {
    let directory = tempfile::tempdir().unwrap();
    let database_path = directory.path().join("sandbox-manager.db");
    let mut conn = Connection::open_at(&database_path).unwrap();
    create_test_schema(&mut conn);
    setup_quarantine_recovery_episode(
        &conn,
        SandboxQuarantineReasonCode::RestoreFirecrackerLaunchFailed,
    );
    // A real SQLITE_BUSY, which is what a contended node-local database
    // produces. Reported as an empty or partial candidate list it makes a pass
    // that saw none of the backlog indistinguishable from an empty backlog.
    let blocker = Connection::open_at(&database_path).unwrap();
    blocker.execute_batch("BEGIN EXCLUSIVE").unwrap();
    conn.busy_timeout(std::time::Duration::ZERO).unwrap();

    let error = scan_quarantine_recovery_candidates(&conn)
        .expect_err("a statement-level SQLite failure must abort the pass");
    let rusqlite::Error::SqliteFailure(failure, _) = error.raw() else {
        panic!("expected the underlying SQLite failure, got {error:?}");
    };
    assert_eq!(failure.code, rusqlite::ErrorCode::DatabaseBusy);

    blocker.execute_batch("ROLLBACK").unwrap();
    // The same scan succeeds once the contention clears, so the abort is the
    // failure being reported rather than the cohort being unreadable.
    assert_eq!(
        scan_quarantine_recovery_candidates(&conn)
            .unwrap()
            .candidates
            .len(),
        1
    );
}

#[test]
fn quarantine_recovery_candidate_scan_isolates_malformed_rows() {
    let conn = crate::db::test_conn();
    let valid_sandbox_id = setup_quarantine_recovery_episode(
        &conn,
        SandboxQuarantineReasonCode::RestoreFirecrackerLaunchFailed,
    );
    let malformed_sandbox_id =
        setup_quarantine_recovery_episode(&conn, SandboxQuarantineReasonCode::RestorePrepareFailed);
    conn.execute(
        "UPDATE sandbox_quarantine_state
            SET quarantine_id = 'not-a-uuid'
          WHERE sandbox_id = ?1",
        params![malformed_sandbox_id.to_string()],
    )
    .unwrap();

    let scan = scan_quarantine_recovery_candidates(&conn).unwrap();
    assert_eq!(
        scan.candidates
            .iter()
            .map(|candidate| candidate.sandbox_id)
            .collect::<Vec<_>>(),
        [valid_sandbox_id]
    );
    // One scan reports the work list and the health of the same rows, so a
    // malformed row cannot be counted as a healthy pending one.
    assert_eq!(scan.pending_count, 1);
    assert_eq!(scan.pending_oldest_age, std::time::Duration::ZERO);
    assert_eq!(scan.malformed_count, 1);
}

#[test]
fn activation_route_provenance_reconciles_legacy_retained_substrate() {
    let conn = test_conn();
    let sandbox_id = SandboxId::new();
    insert_test_sandbox(
        &conn,
        TestSandboxInsert::new(
            sandbox_id.to_string(),
            SandboxRuntimeLabel::from_sandbox_id(sandbox_id).to_string(),
            SandboxState::Stopped,
        ),
    );
    conn.execute(
        "UPDATE sandboxes
            SET last_observed_route_generation = 4,
                last_observed_execution_generation = 4,
                last_observed_route_generation_substrate_kind = 'space',
                last_observed_route_substrate_kind = 'k8s'
          WHERE id = ?1",
        params![sandbox_id.to_string()],
    )
    .unwrap();

    let provenance = activation_route_provenance(&conn, &sandbox_id.to_string()).unwrap();

    assert_eq!(
        provenance.retained_substrate_kind,
        Some(NodeManagedBy::Kubernetes),
        "either retained column identifying Kubernetes must fence local minting"
    );
}

#[test]
fn legacy_incomplete_runtime_clear_does_not_erase_retained_route_generation() {
    let conn = test_conn();
    let sandbox_id = SandboxId::new();
    let sandbox_id_string = sandbox_id.to_string();
    let route_activation_id = Uuid::now_v7().to_string();
    insert_test_sandbox(
        &conn,
        TestSandboxInsert::new(
            &sandbox_id_string,
            SandboxRuntimeLabel::from_sandbox_id(sandbox_id).to_string(),
            SandboxState::Started,
        ),
    );
    conn.execute(
        "UPDATE sandboxes
            SET space_api_route_generation = NULL,
                space_api_route_execution_generation = 8,
                space_api_route_activation_attempt_id = ?1,
                space_api_route_substrate_kind = 'space',
                last_observed_route_generation = 7,
                last_observed_execution_generation = 7,
                last_observed_route_generation_substrate_kind = 'space'
          WHERE id = ?2",
        params![route_activation_id, sandbox_id_string],
    )
    .unwrap();

    clear_runtime_fields(&conn, &sandbox_id_string).unwrap();

    assert_eq!(
        retained_route_provenance_columns(&conn, &sandbox_id_string),
        (Some(7), Some("space".to_string()))
    );
    assert_eq!(route_identity_columns(&conn, &sandbox_id_string).2, Some(8));

    let newer_route_activation_id = Uuid::now_v7().to_string();
    conn.execute(
        "UPDATE sandboxes
            SET space_api_route_generation = 9,
                space_api_route_execution_generation = 6,
                space_api_route_activation_attempt_id = ?1,
                space_api_route_substrate_kind = 'space'
          WHERE id = ?2",
        params![newer_route_activation_id, sandbox_id_string],
    )
    .unwrap();
    clear_runtime_fields(&conn, &sandbox_id_string).unwrap();

    assert_eq!(
        retained_route_provenance_columns(&conn, &sandbox_id_string),
        (Some(9), Some("space".to_string()))
    );
    assert_eq!(route_identity_columns(&conn, &sandbox_id_string).2, Some(8));
}

#[test]
fn route_generation_only_runtime_clear_retains_route_floor() {
    let conn = test_conn();
    let sandbox_id = SandboxId::new();
    let sandbox_id_string = sandbox_id.to_string();
    insert_test_sandbox(
        &conn,
        TestSandboxInsert::new(
            &sandbox_id_string,
            SandboxRuntimeLabel::from_sandbox_id(sandbox_id).to_string(),
            SandboxState::Started,
        ),
    );
    conn.execute(
        "UPDATE sandboxes
            SET space_api_route_generation = 12,
                space_api_route_substrate_kind = 'space'
          WHERE id = ?1",
        params![sandbox_id_string],
    )
    .unwrap();

    clear_runtime_fields(&conn, &sandbox_id_string).unwrap();

    assert_eq!(
        retained_route_provenance_columns(&conn, &sandbox_id_string),
        (Some(12), Some("space".to_string()))
    );
    assert_eq!(
        route_identity_columns(&conn, &sandbox_id_string),
        (None, None, None, None)
    );
}

#[test]
fn clearing_runtime_retains_complete_route_identity_for_later_cleanup() {
    let conn = test_conn();
    let sandbox_id = SandboxId::new();
    let sandbox_id_string = sandbox_id.to_string();
    let route_activation_id = Uuid::now_v7().to_string();
    insert_test_sandbox(
        &conn,
        TestSandboxInsert::new(
            &sandbox_id_string,
            SandboxRuntimeLabel::from_sandbox_id(sandbox_id).to_string(),
            SandboxState::Started,
        ),
    );
    conn.execute(
        "UPDATE sandboxes
            SET space_api_route_execution_generation = 7,
                space_api_route_activation_attempt_id = ?1,
                space_api_route_substrate_kind = 'kubernetes',
                space_api_route_identity_provenance = 'cluster_fenced'
          WHERE id = ?2",
        params![route_activation_id, sandbox_id_string],
    )
    .unwrap();

    clear_runtime_fields(&conn, &sandbox_id_string).unwrap();

    assert_eq!(
        route_identity_columns(&conn, &sandbox_id_string),
        (None, None, Some(7), Some(route_activation_id.clone()))
    );
    let retained: (String, String) = conn
        .query_row(
            "SELECT last_observed_route_substrate_kind,
                    last_observed_route_identity_provenance
               FROM sandboxes WHERE id = ?1",
            [&sandbox_id_string],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(
        retained,
        ("kubernetes".to_string(), "cluster_fenced".to_string())
    );

    let newer_attempt = Uuid::now_v7().to_string();
    conn.execute(
        "UPDATE sandboxes
            SET last_observed_route_activation_id = ?1,
                space_api_route_execution_generation = 7,
                space_api_route_activation_attempt_id = ?2,
                space_api_route_substrate_kind = 'kubernetes',
                space_api_route_identity_provenance = 'cluster_fenced'
          WHERE id = ?3",
        params![newer_attempt, route_activation_id, sandbox_id_string],
    )
    .unwrap();
    clear_runtime_fields(&conn, &sandbox_id_string).unwrap();
    assert_eq!(
        route_identity_columns(&conn, &sandbox_id_string),
        (None, None, Some(7), Some(newer_attempt)),
        "clearing a stale same-generation attempt must not replace retained evidence"
    );

    let latest_attempt = Uuid::now_v7().to_string();
    conn.execute(
        "UPDATE sandboxes
            SET space_api_route_execution_generation = 7,
                space_api_route_activation_attempt_id = ?1,
                space_api_route_substrate_kind = 'kubernetes',
                space_api_route_identity_provenance = 'cluster_fenced'
          WHERE id = ?2",
        params![latest_attempt, sandbox_id_string],
    )
    .unwrap();
    clear_runtime_fields(&conn, &sandbox_id_string).unwrap();
    assert_eq!(
        route_identity_columns(&conn, &sandbox_id_string),
        (None, None, Some(7), Some(latest_attempt)),
        "clearing a newer same-generation attempt must advance retained evidence"
    );
}

fn begin_start(
    conn: &Connection,
    id: &str,
    expected_state: SandboxState,
) -> std::result::Result<Option<CleanupEpisode>, BeginActivationError> {
    begin_activation(
        conn,
        id,
        expected_state,
        ActivationKind::ColdStart,
        None,
        None,
    )
}

fn begin_restore(
    conn: &Connection,
    id: &str,
    expected_state: SandboxState,
) -> std::result::Result<Option<CleanupEpisode>, BeginActivationError> {
    begin_activation(
        conn,
        id,
        expected_state,
        ActivationKind::Restore,
        None,
        None,
    )
}

fn platform_owner(value: u128) -> SandboxOwner {
    SandboxOwner::platform_user(UserId::from_uuid(Uuid::from_u128(value)))
}

fn tenant_owner() -> SandboxOwner {
    platform_owner(0x100)
}

fn adopter_owner() -> SandboxOwner {
    platform_owner(0x200)
}

fn access_idle_policy() -> AutoLifecyclePolicy {
    AutoLifecyclePolicy {
        wake: Default::default(),
        condition_sets: BTreeMap::from([(
            AutoLifecycleConditionSetId::AccessIdle,
            AutoLifecycleConditionSet::access_idle(),
        )]),
        stages: vec![AutoLifecycleStage {
            condition_set: AutoLifecycleConditionSetId::AccessIdle,
            at: 300,
            action: AutoLifecycleIdleAction::Pause,
        }],
    }
    .normalized()
}

#[test]
fn sandbox_lifecycle_counts_groups_durable_rows() {
    let conn = test_conn();
    for (id, uuid8, state) in [
        ("started-1", "abc00001", SandboxState::Started),
        ("started-2", "abc00002", SandboxState::Started),
        ("cleanup-suspend", "abc00003", SandboxState::CleanupFailed),
        ("cleanup-delete", "abc00004", SandboxState::CleanupFailed),
    ] {
        insert_test_sandbox(&conn, TestSandboxInsert::new(id, uuid8, state));
    }
    set_cleanup_intent(
        &conn,
        "cleanup-suspend",
        true,
        CleanupCompletion::MarkSuspended,
    )
    .unwrap();
    set_cleanup_intent(&conn, "cleanup-delete", false, CleanupCompletion::DeleteRow).unwrap();

    let counts = sandbox_lifecycle_counts(&conn).unwrap();

    assert_eq!(
        counts,
        vec![
            SandboxLifecycleCount {
                state: SandboxState::CleanupFailed,
                count: 2,
            },
            SandboxLifecycleCount {
                state: SandboxState::Started,
                count: 2,
            },
        ]
    );
}

#[test]
fn suspend_publication_protects_live_and_retry_owned_suspend() {
    assert_eq!(
        sandbox_erase_safety(SandboxState::Suspending, None, None),
        SandboxEraseSafety::SuspendPublicationPending,
        "an in-flight suspend must block erasure",
    );
    assert_eq!(
        sandbox_erase_safety(SandboxState::Suspended, None, None),
        SandboxEraseSafety::Safe,
        "a completed suspend is safe to erase; only an in-flight publication owns the row",
    );

    // Every other state is unrelated to suspend publication and erases as
    // before, including the deleting state a delete drives.
    for state in [
        SandboxState::Deleting,
        SandboxState::Started,
        SandboxState::Paused,
        SandboxState::Stopped,
        SandboxState::Failed,
    ] {
        assert_eq!(
            sandbox_erase_safety(state, None, None),
            SandboxEraseSafety::Safe,
            "{state} must not be protected",
        );
    }

    for state in [SandboxState::CleanupRequired, SandboxState::CleanupFailed] {
        assert_eq!(
            sandbox_erase_safety(state, Some(CleanupCompletion::MarkSuspended), None,),
            SandboxEraseSafety::SuspendPublicationPending,
            "{state} must retain persisted suspend-publication ownership",
        );
        assert_eq!(
            sandbox_erase_safety(state, Some(CleanupCompletion::DeleteRow), None),
            SandboxEraseSafety::Safe,
            "{state} with delete ownership must not be treated as a suspend",
        );
    }

    assert_eq!(
        sandbox_erase_safety(
            SandboxState::Failed,
            None,
            Some(SUSPEND_PUBLISH_FAILED_NO_ARTIFACT_RUNTIME_GONE),
        ),
        SandboxEraseSafety::RetainedFilesystemOnlyCopy,
        "a failed suspend with no artifact must retain its only filesystem copy",
    );
    assert_eq!(
        sandbox_erase_safety(SandboxState::Failed, None, Some("ordinary failure")),
        SandboxEraseSafety::Safe,
        "ordinary failed sandboxes remain explicitly deletable",
    );
}

#[test]
fn cleanup_episode_recovery_fence_requires_artifact_and_generation_together() {
    let conn = test_conn();
    let sandbox_id = SandboxId::new();
    let sandbox_id_string = sandbox_id.to_string();
    insert_test_sandbox(
        &conn,
        TestSandboxInsert::new(
            &sandbox_id_string,
            SandboxRuntimeLabel::from_sandbox_id(sandbox_id).to_string(),
            SandboxState::Created,
        ),
    );
    begin_start(&conn, &sandbox_id_string, SandboxState::Created)
        .unwrap()
        .unwrap();

    for (artifact_id, generation) in [(Some("artifact"), None), (None, Some(7_i64))] {
        let error = conn
            .execute(
                "UPDATE sandbox_cleanup_episodes
                    SET recovery_local_artifact_id = ?1,
                        recovery_execution_generation = ?2
                  WHERE sandbox_id = ?3",
                params![artifact_id, generation, sandbox_id_string],
            )
            .unwrap_err();
        assert!(
            error.to_string().contains("CHECK constraint failed"),
            "partial recovery fence must violate the schema: {error}"
        );
    }
}

#[test]
fn recovered_suspended_insert_rejects_non_suspended_state_without_writing() {
    let conn = test_conn();
    let sandbox_id = SandboxId::new();
    let sandbox_id_string = sandbox_id.to_string();
    let uuid8 = SandboxRuntimeLabel::from_sandbox_id(sandbox_id);
    let user = tenant_owner();
    let params = TestSandboxInsert::new(
        &sandbox_id_string,
        uuid8.to_string(),
        SandboxState::Quarantined,
    );
    let execution = ExecutionMetadata::initial_firecracker(Arch::Aarch64, vec![]);

    let tx = immediate_transaction(&conn).unwrap();
    let error = insert_recovered_suspended_sandbox_in_transaction(
        &tx,
        params.insert_params(&uuid8, &user),
        &execution,
        LocalArtifactId::new(),
        "/space/cache/sandboxes/test/snapshot",
    )
    .expect_err("recovered sandbox inserts must require suspended state");
    drop(tx);

    assert!(matches!(
        error,
        InsertRecoveredSuspendedSandboxError::NonSuspendedInsert {
            state: SandboxState::Quarantined
        }
    ));
    assert!(get_sandbox(&conn, &sandbox_id_string).unwrap().is_none());
}

fn runtime_fields(execution_generation: ExecutionGeneration) -> RuntimeFields<'static> {
    RuntimeFields {
        uid: 10000,
        slot: 7,
        guest_cid: 37,
        pid: std::process::id(),
        spaced_host_pid: Some(10001),
        netgtw_pid: 10002,
        vsock_uds_path: "/space/run/firecracker/sandboxes/test/root/v.sock",
        api_socket_path: "/space/run/firecracker/sandboxes/test/api.sock",
        spaced_endpoint_proof: None,
        space_api_route_identity: SpaceApiRouteIdentity {
            execution_generation: Some(execution_generation),
            ..SpaceApiRouteIdentity::default()
        },
        route_identity_provenance:
            crate::auto_lifecycle_delete_receipts::RouteIdentityProvenance::Unfenced,
    }
}

#[test]
fn endpoint_proof_round_trips_and_clears_with_runtime() {
    use crate::endpoint_proof::{
        ProcessActivationIdentity, SpacedEndpointProof, VsockActivationIdentity,
    };

    let conn = crate::db::test_conn();
    let sandbox_id = SandboxId::new();
    let sandbox_id_string = sandbox_id.to_string();
    insert_test_sandbox(
        &conn,
        TestSandboxInsert::new(&sandbox_id_string, "proofrow", SandboxState::Started),
    );
    let generation = ExecutionGeneration::new(7).unwrap();
    let attempt = SandboxActivationAttemptId::new();
    let firecracker_pid = LinuxPid::try_from_u32(std::process::id()).unwrap();
    let proof = SpacedEndpointProof {
        sandbox_id,
        execution_generation: generation,
        activation_attempt_id: attempt,
        spaced_host: ProcessActivationIdentity {
            pid: LinuxPid::try_from_u32(10001).unwrap(),
            start_time_ticks: 11,
            executable_device: 12,
            executable_inode: 13,
        },
        runtime_process: ProcessActivationIdentity {
            pid: firecracker_pid,
            start_time_ticks: 21,
            executable_device: 22,
            executable_inode: 23,
        },
        vsock: VsockActivationIdentity {
            path: "/space/run/firecracker/sandboxes/test/root/v.sock".into(),
            device: 31,
            inode: 32,
            owner_uid: LinuxUid::new(10000),
            owner_gid: 10000,
            guest_cid: VsockCid::try_from_u32(37).unwrap(),
            runtime_fd: 8,
            runtime_socket_inode: 33,
        },
    };

    update_runtime_fields(
        &conn,
        sandbox_id,
        RuntimeFields {
            uid: 10000,
            slot: 7,
            guest_cid: 37,
            pid: std::process::id(),
            spaced_host_pid: Some(10001),
            netgtw_pid: 10002,
            vsock_uds_path: "/space/run/firecracker/sandboxes/test/root/v.sock",
            api_socket_path: "/space/run/firecracker/sandboxes/test/api.sock",
            spaced_endpoint_proof: Some(&proof),
            space_api_route_identity: SpaceApiRouteIdentity {
                sandbox_id: Some(sandbox_id),
                execution_generation: Some(generation),
                activation_attempt_id: Some(attempt),
                ..SpaceApiRouteIdentity::default()
            },
            route_identity_provenance:
                crate::auto_lifecycle_delete_receipts::RouteIdentityProvenance::Unfenced,
        },
    )
    .unwrap();

    assert_eq!(
        get_sandbox(&conn, &sandbox_id_string)
            .unwrap()
            .unwrap()
            .spaced_endpoint_proof,
        Some(proof.clone())
    );
    clear_runtime_fields(&conn, &sandbox_id_string).unwrap();
    assert!(
        get_sandbox(&conn, &sandbox_id_string)
            .unwrap()
            .unwrap()
            .spaced_endpoint_proof
            .is_none()
    );

    update_runtime_fields(
        &conn,
        sandbox_id,
        RuntimeFields {
            uid: 10000,
            slot: 7,
            guest_cid: 37,
            pid: std::process::id(),
            spaced_host_pid: Some(10001),
            netgtw_pid: 10002,
            vsock_uds_path: "/space/run/firecracker/sandboxes/test/root/v.sock",
            api_socket_path: "/space/run/firecracker/sandboxes/test/api.sock",
            spaced_endpoint_proof: None,
            space_api_route_identity: SpaceApiRouteIdentity {
                sandbox_id: Some(sandbox_id),
                execution_generation: Some(generation),
                activation_attempt_id: Some(attempt),
                ..SpaceApiRouteIdentity::default()
            },
            route_identity_provenance:
                crate::auto_lifecycle_delete_receipts::RouteIdentityProvenance::Unfenced,
        },
    )
    .unwrap();
    let runtime = crate::endpoint_proof::SpacedEndpointRuntimeIdentity {
        sandbox_id,
        execution_generation: generation,
        activation_attempt_id: attempt,
        spaced_host_pid: proof.spaced_host.pid,
        runtime_pid: proof.runtime_process.pid,
        sandbox_uid: proof.vsock.owner_uid,
        guest_cid: proof.vsock.guest_cid,
        vsock_path: proof.vsock.path.clone(),
    };
    let mut stale_runtime = runtime.clone();
    stale_runtime.activation_attempt_id = SandboxActivationAttemptId::new();
    let mut stale_proof = proof.clone();
    stale_proof.activation_attempt_id = stale_runtime.activation_attempt_id;
    assert!(
        !persist_spaced_endpoint_proof_if_missing(&conn, &stale_runtime, &stale_proof).unwrap(),
        "stale activation must not attach its proof to the current execution"
    );
    assert!(persist_spaced_endpoint_proof_if_missing(&conn, &runtime, &proof).unwrap());
    assert!(!persist_spaced_endpoint_proof_if_missing(&conn, &runtime, &proof).unwrap());
    assert_eq!(
        get_sandbox(&conn, &sandbox_id_string)
            .unwrap()
            .unwrap()
            .spaced_endpoint_proof,
        Some(proof.clone())
    );

    update_space_api_route_identity(
        &conn,
        sandbox_id,
        &SpaceApiRouteIdentity {
            route_generation: Some(RouteGeneration::new(8).unwrap()),
            execution_generation: Some(generation),
            activation_attempt_id: Some(attempt),
            ..SpaceApiRouteIdentity::default()
        },
        crate::auto_lifecycle_delete_receipts::RouteIdentityProvenance::Unfenced,
    )
    .unwrap();
    assert_eq!(
        get_sandbox(&conn, &sandbox_id_string)
            .unwrap()
            .unwrap()
            .spaced_endpoint_proof,
        Some(proof),
        "route-only identity enrichment must retain the activation proof"
    );

    update_space_api_route_identity(
        &conn,
        sandbox_id,
        &SpaceApiRouteIdentity {
            route_generation: Some(RouteGeneration::new(9).unwrap()),
            execution_generation: Some(generation),
            activation_attempt_id: Some(SandboxActivationAttemptId::new()),
            ..SpaceApiRouteIdentity::default()
        },
        crate::auto_lifecycle_delete_receipts::RouteIdentityProvenance::Unfenced,
    )
    .unwrap();
    assert!(
        get_sandbox(&conn, &sandbox_id_string)
            .unwrap()
            .unwrap()
            .spaced_endpoint_proof
            .is_none(),
        "a new activation attempt must invalidate the prior activation proof"
    );
}

#[test]
fn volume_encryption_round_trips_and_defaults_to_plaintext() {
    let conn = test_conn();

    insert_test_sandbox(
        &conn,
        TestSandboxInsert::new("sandbox-plain", "sandbox1", SandboxState::Created),
    );
    let plain = get_sandbox(&conn, "sandbox-plain").unwrap().unwrap();
    assert!(
        !plain.volume_encryption,
        "default insert must read back plaintext"
    );

    insert_test_sandbox(
        &conn,
        TestSandboxInsert {
            volume_encryption: true,
            ..TestSandboxInsert::new("sandbox-encrypted", "sandbox2", SandboxState::Created)
        },
    );
    let encrypted = get_sandbox(&conn, "sandbox-encrypted").unwrap().unwrap();
    assert!(
        encrypted.volume_encryption,
        "encrypted stamp must survive the insert/select round trip"
    );
}

#[test]
fn restore_adopt_insert_binds_volume_encryption() {
    // `restore_adopt_orphan_or_insert_sandbox_in_transaction` binds `volume_encryption`
    // via the out-of-sequence placeholder `?23` (every other column is
    // `?1..?18`), because `?19..?22` are reserved for the ON CONFLICT
    // orphan-match WHERE. An off-by-one on that hand-numbered placeholder
    // would silently persist the wrong column and boot a BYOK sandbox
    // plaintext — and the cross-node restore path that reaches this
    // function is otherwise exercised only with the default (plaintext)
    // value. Pin both stamps through the fresh-INSERT branch.
    fn restore_params<'a>(
        id: &'a str,
        uuid8: &'a SandboxRuntimeLabel,
        user: &'a SandboxOwner,
        env: &'a BTreeMap<String, String>,
        lifecycle: &'a AutoLifecyclePolicy,
        volume_encryption: bool,
    ) -> InsertSandboxParams<'a> {
        InsertSandboxParams {
            guest_image: Default::default(),
            id,
            uuid8,
            state: SandboxState::Restoring,
            template: "seed",
            template_hash: None,
            user,
            runtime: "fc",
            snapshots_enabled: false,
            rootfs_size_mb: None,
            disk_io_limits: Default::default(),
            pool_template: None,
            volume_encryption,
            process_env: env,
            auto_lifecycle: lifecycle,
            network_policy_override: None,
            network_policy_generation: 0,
            network_policy_authority: PolicyAuthority::Cluster,
            auto_lifecycle_authority: PolicyAuthority::Cluster,
        }
    }

    let conn = test_conn();
    let uuid8 = SandboxRuntimeLabel::parse("sandbox1".to_string()).unwrap();
    let user = SandboxOwner::parse("user".to_string()).unwrap();
    let env = BTreeMap::new();
    let lifecycle = AutoLifecyclePolicy::disabled();
    let restore_source: pplx_space_common_core::types::ExactStorageUri =
        "s3://snapshots/suspend/restore?region=us-east-1"
            .parse()
            .unwrap();
    let restore_operation = RemoteRestoreOperation::metadata_staging(restore_source);

    restore_adopt_orphan_or_insert_sandbox_in_transaction(
        &conn,
        restore_params("restore-enc", &uuid8, &user, &env, &lifecycle, true),
        &restore_operation,
    )
    .unwrap();
    restore_adopt_orphan_or_insert_sandbox_in_transaction(
        &conn,
        restore_params("restore-plain", &uuid8, &user, &env, &lifecycle, false),
        &restore_operation,
    )
    .unwrap();

    assert!(
        get_sandbox(&conn, "restore-enc")
            .unwrap()
            .unwrap()
            .volume_encryption,
        "encrypted restore insert must persist volume_encryption=true (?23 binding)"
    );
    assert!(
        !get_sandbox(&conn, "restore-plain")
            .unwrap()
            .unwrap()
            .volume_encryption,
        "plaintext restore insert must persist volume_encryption=false"
    );
    for id in ["restore-enc", "restore-plain"] {
        let staging = node_execution_fence(&conn, id).unwrap().unwrap();
        assert!(staging.previous().is_none());
        let retired: bool = conn
            .query_row(
                "SELECT node_execution_retired FROM sandboxes WHERE id = ?1",
                [id],
                |row| row.get(0),
            )
            .unwrap();
        assert!(retired, "staging authority has never owned an allocation");
    }
}

#[test]
fn restore_adoption_clears_obsolete_auto_lifecycle_delete_block() {
    let conn = test_conn();
    let sandbox_id = SandboxId::new();
    let sandbox_id_string = sandbox_id.to_string();
    let uuid8 = SandboxRuntimeLabel::from_sandbox_id(sandbox_id);
    let user = SandboxOwner::parse("user").unwrap();
    let env = BTreeMap::new();
    let lifecycle = AutoLifecyclePolicy::disabled();
    let restore_params = || InsertSandboxParams {
        guest_image: Default::default(),
        id: &sandbox_id_string,
        uuid8: &uuid8,
        state: SandboxState::Restoring,
        template: "seed",
        template_hash: None,
        user: &user,
        runtime: "fc",
        snapshots_enabled: false,
        rootfs_size_mb: None,
        disk_io_limits: Default::default(),
        pool_template: None,
        volume_encryption: false,
        process_env: &env,
        auto_lifecycle: &lifecycle,
        network_policy_override: None,
        network_policy_generation: 0,
        network_policy_authority: PolicyAuthority::Cluster,
        auto_lifecycle_authority: PolicyAuthority::Cluster,
    };
    insert_sandbox(
        &conn,
        InsertSandboxParams {
            guest_image: Default::default(),
            id: &sandbox_id_string,
            uuid8: &uuid8,
            state: SandboxState::Created,
            template: ORPHANED_SANDBOX_TEMPLATE,
            template_hash: None,
            user: &ORPHANED_SANDBOX_OWNER,
            runtime: RuntimeKind::Firecracker.alias(),
            snapshots_enabled: false,
            rootfs_size_mb: None,
            disk_io_limits: Default::default(),
            pool_template: None,
            volume_encryption: false,
            process_env: &env,
            auto_lifecycle: &lifecycle,
            network_policy_override: None,
            network_policy_generation: 0,
            network_policy_authority: PolicyAuthority::Cluster,
            auto_lifecycle_authority: PolicyAuthority::Cluster,
        },
    )
    .unwrap();
    mark_quarantined(
        &conn,
        &sandbox_id_string,
        SandboxQuarantineSource::SbxManager,
        SandboxQuarantineReasonCode::OrphanedSandboxRowMissing,
        "unclassified orphan",
        SandboxQuarantineNextAction::RequiresOperatorAttention,
    )
    .unwrap();
    conn.execute(
        "UPDATE sandboxes
            SET auto_lifecycle_delete_block_reason = 'missing_route_identity',
                auto_lifecycle_delete_blocked_at = '1'
          WHERE id = ?1",
        [&sandbox_id_string],
    )
    .unwrap();

    let restore_source: pplx_space_common_core::types::ExactStorageUri =
        "s3://snapshots/suspend/restore?region=us-east-1"
            .parse()
            .unwrap();
    let original_authority = node_execution_fence(&conn, &sandbox_id_string).unwrap();
    restore_adopt_orphan_or_insert_sandbox_in_transaction(
        &conn,
        restore_params(),
        &RemoteRestoreOperation::metadata_staging(restore_source),
    )
    .unwrap();
    assert_eq!(
        node_execution_fence(&conn, &sandbox_id_string).unwrap(),
        original_authority,
        "adoption must preserve the existing cleanup authority"
    );

    let block: (Option<String>, Option<String>) = conn
        .query_row(
            "SELECT auto_lifecycle_delete_block_reason,
                    auto_lifecycle_delete_blocked_at
               FROM sandboxes
              WHERE id = ?1",
            [&sandbox_id_string],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(block, (None, None));
}

#[test]
fn sandbox_execution_metadata_round_trips_without_legacy_projections() {
    let conn = test_conn();
    insert_test_sandbox(
        &conn,
        TestSandboxInsert::new("sandbox-execution", "sandbox1", SandboxState::Created),
    );
    let execution = ExecutionMetadata::initial_firecracker(
        Arch::Aarch64,
        vec![ArtifactComponentMetadata {
            kind: ArtifactComponentKind::Kernel,
            role: None,
            version: Some(ArtifactVersion::parse("6.10.14").unwrap()),
            digest: Some(
                format!("sha256:{}", "b".repeat(64))
                    .parse::<Digest>()
                    .unwrap(),
            ),
            format: Some(ArtifactFormat::parse("vmlinux").unwrap()),
        }],
    );

    upsert_sandbox_execution_metadata(&conn, "sandbox-execution", &execution).unwrap();

    let row = get_sandbox(&conn, "sandbox-execution")
        .unwrap()
        .expect("sandbox row exists");
    assert_eq!(row.execution_metadata, Some(execution.clone()));

    let replacement = ExecutionMetadata::initial_firecracker(Arch::Aarch64, vec![]);
    upsert_sandbox_execution_metadata(&conn, "sandbox-execution", &replacement).unwrap();
    let row = get_sandbox(&conn, "sandbox-execution").unwrap().unwrap();
    assert_eq!(row.execution_metadata, Some(replacement));

    conn.execute("UPDATE sandboxes SET runtime = 'gvisor'", [])
        .unwrap();
    assert!(upsert_sandbox_execution_metadata(&conn, "sandbox-execution", &execution).is_err());
    let stored: ExecutionMetadata = serde_json::from_str(
        &conn
            .query_row(
                "SELECT execution_metadata_json FROM sandboxes WHERE id = 'sandbox-execution'",
                [],
                |row| row.get::<_, String>(0),
            )
            .unwrap(),
    )
    .unwrap();
    assert_eq!(stored, row.execution_metadata.unwrap());
}

#[test]
fn insert_sandbox_initializes_auto_lifecycle_runtime_state() {
    let conn = test_conn();
    let policy = access_idle_policy();

    insert_test_sandbox(
        &conn,
        TestSandboxInsert {
            auto_lifecycle: policy,
            ..TestSandboxInsert::new("sandbox-1", "sandbox1", SandboxState::Creating)
        },
    );

    let row = get_sandbox(&conn, "sandbox-1").unwrap().unwrap();
    let condition = row
        .auto_lifecycle_state
        .conditions
        .get(&AutoLifecycleConditionSetId::AccessIdle)
        .expect("access idle state");

    assert!(!condition.armed);
    assert_eq!(condition.epoch_at, None);
    assert_eq!(row.next_auto_event_at, None);
}

#[test]
fn started_transition_arms_disarmed_lifecycle_for_rollback_writers() {
    let conn = test_conn();
    let policy = access_idle_policy();

    insert_test_sandbox(
        &conn,
        TestSandboxInsert {
            auto_lifecycle: policy,
            ..TestSandboxInsert::new("sandbox-1", "sandbox1", SandboxState::Starting)
        },
    );

    let transition_started_at = crate::auto_lifecycle::now_secs();
    conn.execute(
        "UPDATE sandboxes SET state = 'started' WHERE id = ?1",
        params!["sandbox-1"],
    )
    .unwrap();

    let row = get_sandbox(&conn, "sandbox-1").unwrap().unwrap();
    let condition = &row.auto_lifecycle_state.conditions[&AutoLifecycleConditionSetId::AccessIdle];
    assert!(condition.armed);
    assert!(condition.epoch_at.unwrap() >= transition_started_at);
    assert_eq!(row.next_auto_event_at, condition.epoch_at);
}

#[test]
fn insert_sandbox_persists_only_explicit_rootfs_size_override() {
    let conn = test_conn();

    insert_test_sandbox(
        &conn,
        TestSandboxInsert::new("default-size", "defaults", SandboxState::Creating),
    );
    insert_test_sandbox(
        &conn,
        TestSandboxInsert {
            rootfs_size_mb: Some(8192),
            ..TestSandboxInsert::new("override-size", "override", SandboxState::Creating)
        },
    );

    let default_row = get_sandbox(&conn, "default-size").unwrap().unwrap();
    assert_eq!(default_row.rootfs_size_mb, None);

    let override_row = get_sandbox(&conn, "override-size").unwrap().unwrap();
    assert_eq!(override_row.rootfs_size_mb, Some(8192));
}

#[test]
fn insert_sandbox_persists_effective_disk_io_limits() {
    let conn = crate::db::test_conn();
    let expected = pplx_space_common_core::types::DiskIoLimits::try_new(5_000, 200).unwrap();

    insert_test_sandbox(
        &conn,
        TestSandboxInsert {
            disk_io_limits: expected,
            ..TestSandboxInsert::new("disk-io", "diskio01", SandboxState::Creating)
        },
    );

    let row = get_sandbox(&conn, "disk-io").unwrap().unwrap();
    assert_eq!(row.disk_io_limits, expected);
}

#[test]
fn suspend_snapshot_publication_exposes_identity_only_with_terminal_state() {
    let conn = test_conn();
    let sandbox_id = SandboxId::new().to_string();
    insert_test_sandbox(
        &conn,
        TestSandboxInsert::new(&sandbox_id, "sandbox1", SandboxState::Paused),
    );
    let execution_generation = ExecutionGeneration::new(7).unwrap();
    let pod_uid = PodUid::parse("pod-uid-7").unwrap();
    update_space_api_route_identity(
        &conn,
        sandbox_id.parse().unwrap(),
        &SpaceApiRouteIdentity {
            execution_generation: Some(execution_generation),
            pod_uid: Some(pod_uid.clone()),
            ..SpaceApiRouteIdentity::default()
        },
        crate::auto_lifecycle_delete_receipts::RouteIdentityProvenance::Unfenced,
    )
    .unwrap();
    begin_suspend(&conn, &sandbox_id).unwrap();
    let accepted = get_sandbox(&conn, &sandbox_id).unwrap().unwrap();
    assert_eq!(accepted.state, SandboxState::Suspending);
    assert!(accepted.current_local_artifact_id.is_none());
    assert!(accepted.current_local_artifact_kind.is_none());
    assert!(accepted.current_local_artifact_origin.is_none());

    let local_artifact_id = LocalArtifactId::new();
    complete_row_intent_cleanup(
        &conn,
        &sandbox_id,
        CleanupCompletion::MarkSuspended,
        Some((local_artifact_id, "/cache/sandbox-1/snapshot")),
    )
    .unwrap();

    let published = get_sandbox(&conn, &sandbox_id).unwrap().unwrap();
    assert_eq!(published.state, SandboxState::Suspended);
    assert_eq!(published.current_local_artifact_id, Some(local_artifact_id));
    assert_eq!(
        published.current_local_artifact_kind,
        Some(ArtifactKind::Full)
    );
    assert_eq!(
        published.current_local_artifact_origin,
        Some(ArtifactOrigin::Suspend)
    );
    assert_eq!(
        published.space_api_route_identity.execution_generation,
        Some(execution_generation),
        "the suspended publication must retain the exact execution generation"
    );
    assert_eq!(
        published.space_api_route_identity.pod_uid,
        Some(pod_uid),
        "the suspended publication must retain the exact Kubernetes pod UID"
    );
    assert_eq!(
        get_local_artifact(&conn, local_artifact_id)
            .unwrap()
            .unwrap()
            .artifact_kind,
        ArtifactKind::Full
    );
}

#[test]
fn teardown_suspend_keeps_the_runtime_until_the_staged_artifact_is_durable() {
    let conn = test_conn();
    insert_test_sandbox(
        &conn,
        TestSandboxInsert::new("sandbox-1", "sandbox1", SandboxState::Paused),
    );
    let local_artifact_id = LocalArtifactId::new();
    let generation = ExecutionGeneration::new(1).unwrap();

    begin_teardown_suspend(&conn, "sandbox-1", generation).unwrap();
    stage_teardown_suspend_artifact(
        &conn,
        "sandbox-1",
        generation,
        local_artifact_id,
        "/cache/sandbox-1/snapshot",
    )
    .unwrap();

    let staged = get_sandbox(&conn, "sandbox-1").unwrap().unwrap();
    assert_eq!(staged.state, SandboxState::Suspending);
    assert_eq!(staged.current_local_artifact_id, Some(local_artifact_id));
    assert!(
        teardown_suspend_status(&conn, "sandbox-1")
            .unwrap()
            .unwrap()
            .operation
            .is_some()
    );

    mark_local_artifact_backup_completed(&conn, &local_artifact_id.to_string()).unwrap();
    complete_row_intent_cleanup(
        &conn,
        "sandbox-1",
        CleanupCompletion::MarkSuspended,
        Some((local_artifact_id, "/cache/sandbox-1/snapshot")),
    )
    .unwrap();

    assert!(
        teardown_suspend_status(&conn, "sandbox-1")
            .unwrap()
            .unwrap()
            .is_ready_for(generation)
    );
    assert!(
        complete_teardown_suspend(&conn, "sandbox-1", generation, Some(local_artifact_id),)
            .unwrap()
    );
    let operation = teardown_suspend_status(&conn, "sandbox-1")
        .unwrap()
        .unwrap()
        .operation
        .unwrap();
    assert_eq!(operation.published_artifact_id, Some(local_artifact_id));
    assert_eq!(operation.status, TeardownSuspendOperationStatus::Completed);
    assert!(
        teardown_suspend_status(&conn, "sandbox-1")
            .unwrap()
            .unwrap()
            .is_durable_for(generation)
    );
}

#[test]
fn retried_suspend_cleanup_retains_publication_identity_for_pinned_artifact() {
    let conn = test_conn();
    let sandbox_id = SandboxId::new().to_string();
    insert_test_sandbox(
        &conn,
        TestSandboxInsert::new(&sandbox_id, "retryid1", SandboxState::Paused),
    );
    let generation = ExecutionGeneration::new(7).unwrap();
    let pod_uid = PodUid::parse("retry-pod-uid-7").unwrap();
    update_space_api_route_identity(
        &conn,
        sandbox_id.parse().unwrap(),
        &SpaceApiRouteIdentity {
            execution_generation: Some(generation),
            pod_uid: Some(pod_uid.clone()),
            ..SpaceApiRouteIdentity::default()
        },
        crate::auto_lifecycle_delete_receipts::RouteIdentityProvenance::Unfenced,
    )
    .unwrap();
    let local_artifact_id = LocalArtifactId::new();
    begin_teardown_suspend(&conn, &sandbox_id, generation).unwrap();
    stage_teardown_suspend_artifact(
        &conn,
        &sandbox_id,
        generation,
        local_artifact_id,
        "/cache/retry/snapshot",
    )
    .unwrap();

    complete_row_intent_cleanup(&conn, &sandbox_id, CleanupCompletion::MarkSuspended, None)
        .unwrap();

    let published = get_sandbox(&conn, &sandbox_id).unwrap().unwrap();
    assert_eq!(published.state, SandboxState::Suspended);
    assert_eq!(published.current_local_artifact_id, Some(local_artifact_id));
    assert_eq!(
        published.space_api_route_identity.execution_generation,
        Some(generation)
    );
    assert_eq!(published.space_api_route_identity.pod_uid, Some(pod_uid));
}

#[test]
fn teardown_suspend_journal_persists_the_local_execution_lease() {
    let conn = test_conn();
    insert_test_sandbox(
        &conn,
        TestSandboxInsert::new("sandbox-1", "sandbox1", SandboxState::Paused),
    );
    begin_teardown_suspend(&conn, "sandbox-1", ExecutionGeneration::new(7).unwrap()).unwrap();

    let (payload, dedup_key): (String, String) = conn
        .query_row(
            "SELECT payload, dedup_key FROM jobs WHERE kind = ?1",
            params![TEARDOWN_SUSPEND_JOB_KIND],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(payload, r#"{"execution_generation":7}"#);
    assert_eq!(dedup_key, "sandbox-1:7");
}

#[test]
fn runtime_attachment_does_not_queue_teardown_work() {
    let conn = test_conn();
    let sandbox_id = SandboxId::new();
    let sandbox_id_string = sandbox_id.to_string();
    insert_test_sandbox(
        &conn,
        TestSandboxInsert::new(&sandbox_id_string, "sandbox1", SandboxState::Paused),
    );

    update_runtime_fields(
        &conn,
        sandbox_id,
        runtime_fields(ExecutionGeneration::new(7).unwrap()),
    )
    .unwrap();

    let queued: i64 = conn
        .query_row(
            "SELECT COUNT(*)
               FROM jobs
              WHERE status IN ('pending', 'running')",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(queued, 0, "runtime ownership is not queued teardown work");
}

#[test]
fn runtime_attachment_rejects_a_missing_sandbox() {
    let conn = test_conn();
    let sandbox_id = SandboxId::new();

    let error = update_runtime_fields(
        &conn,
        sandbox_id,
        runtime_fields(ExecutionGeneration::new(7).unwrap()),
    )
    .expect_err("runtime ownership cannot attach to a missing sandbox");

    assert!(error.is_no_rows());
}

#[test]
fn terminal_teardown_generation_rejects_stale_runtime_attachment() {
    let conn = test_conn();
    let sandbox_id = SandboxId::new();
    let sandbox_id_string = sandbox_id.to_string();
    insert_test_sandbox(
        &conn,
        TestSandboxInsert::new(&sandbox_id_string, "sandbox1", SandboxState::Paused),
    );
    let current_generation = ExecutionGeneration::new(2).unwrap();
    claim_teardown_suspend_operation(&conn, &sandbox_id_string, current_generation).unwrap();
    begin_teardown_suspend_operation(&conn, &sandbox_id_string, current_generation).unwrap();
    assert!(
        complete_teardown_suspend(&conn, &sandbox_id_string, current_generation, None).unwrap()
    );

    let error = update_runtime_fields(
        &conn,
        sandbox_id,
        runtime_fields(ExecutionGeneration::new(1).unwrap()),
    )
    .expect_err("a terminal newer lease is a durable generation high-water mark");
    assert!(error.is_invalid_query());

    let latest = latest_teardown_suspend_operation(&conn, &sandbox_id_string)
        .unwrap()
        .expect("the current generation must remain the latest operation");
    assert_eq!(latest.execution_generation, current_generation);
    assert_eq!(latest.status, TeardownSuspendOperationStatus::Completed);
}

#[test]
fn failed_teardown_generation_allows_same_runtime_reattachment() {
    let conn = test_conn();
    let sandbox_id = SandboxId::new();
    let sandbox_id_string = sandbox_id.to_string();
    insert_test_sandbox(
        &conn,
        TestSandboxInsert::new(&sandbox_id_string, "sandbox1", SandboxState::Paused),
    );
    let generation = ExecutionGeneration::new(2).unwrap();
    claim_teardown_suspend_operation(&conn, &sandbox_id_string, generation).unwrap();
    begin_teardown_suspend_operation(&conn, &sandbox_id_string, generation).unwrap();
    let operation = latest_teardown_suspend_operation(&conn, &sandbox_id_string)
        .unwrap()
        .expect("the teardown operation must exist");
    pplx_space_common_jobs::db::fail_job_permanent(
        conn.raw(),
        &operation.job_id,
        "injected teardown failure",
        None,
    )
    .unwrap();

    update_runtime_fields(&conn, sandbox_id, runtime_fields(generation))
        .expect("a failed attempt is not a durable generation high-water mark");

    assert_eq!(
        runtime_execution_generation(&conn, &sandbox_id_string).unwrap(),
        Some(generation)
    );
}

#[test]
fn restaging_the_same_teardown_artifact_preserves_its_durability_marker() {
    let conn = test_conn();
    insert_test_sandbox(
        &conn,
        TestSandboxInsert::new("sandbox-1", "sandbox1", SandboxState::Paused),
    );
    let local_artifact_id = LocalArtifactId::new();
    let generation = ExecutionGeneration::new(1).unwrap();

    begin_teardown_suspend(&conn, "sandbox-1", generation).unwrap();
    stage_teardown_suspend_artifact(
        &conn,
        "sandbox-1",
        generation,
        local_artifact_id,
        "/cache/sandbox-1/staged",
    )
    .unwrap();
    mark_local_artifact_backup_completed(&conn, &local_artifact_id.to_string()).unwrap();

    stage_teardown_suspend_artifact(
        &conn,
        "sandbox-1",
        generation,
        local_artifact_id,
        "/cache/sandbox-1/snapshot",
    )
    .unwrap();

    let status = teardown_suspend_status(&conn, "sandbox-1")
        .unwrap()
        .unwrap();
    assert!(
        status.backup_completed,
        "restaging the same artifact must not discard its remote durability proof"
    );
    assert_eq!(
        status.artifact.unwrap().local_path,
        "/cache/sandbox-1/snapshot"
    );
}

/// A second suspend on the same sandbox writes its snapshot at the
/// same per-sandbox `local_path` the prior suspend used (see
/// `suspend_snapshot_publication::publish_suspend_snapshot_dir`). Without dedupe at publish time,
/// both `local_artifacts` rows would point at the same
/// `local_path` and the snapshot reaper's `SharedLocalPath`
/// safety gate would refuse to reap either — making the reaper
/// inert for any multiply-suspended sandbox.
///
/// Regression: suspended cleanup completion DELETEs every prior
/// `artifact_origin='suspend'` row for the sandbox in the same transaction
/// so the new row is the sole owner of the path.
#[test]
fn suspended_cleanup_completion_dedupes_prior_suspend_row() {
    let conn = test_conn();
    insert_test_sandbox(
        &conn,
        TestSandboxInsert::new("sandbox-1", "sandbox1", SandboxState::Paused),
    );

    // First suspend: Paused -> Suspending -> Suspended pinned on A1.
    begin_suspend(&conn, "sandbox-1").unwrap();
    let a1 = LocalArtifactId::new();
    complete_row_intent_cleanup(
        &conn,
        "sandbox-1",
        CleanupCompletion::MarkSuspended,
        Some((a1, "/cache/sandbox-1/snapshot")),
    )
    .unwrap();
    assert!(get_local_artifact(&conn, a1).unwrap().is_some());

    // Simulate a wake-and-resuspend cycle, including its stable paused state.
    state_write_fixtures::force_state(&conn, "sandbox-1", SandboxState::Paused);
    assert!(begin_suspend(&conn, "sandbox-1").unwrap());
    let a2 = LocalArtifactId::new();
    complete_row_intent_cleanup(
        &conn,
        "sandbox-1",
        CleanupCompletion::MarkSuspended,
        Some((a2, "/cache/sandbox-1/snapshot")),
    )
    .unwrap();

    // A1's row must be gone — the dedupe is what keeps the
    // SharedLocalPath gate from going inert.
    assert!(
        get_local_artifact(&conn, a1).unwrap().is_none(),
        "prior suspend artifact row must be deleted by the second publish"
    );
    assert_eq!(
        get_local_artifact(&conn, a2).unwrap().unwrap().local_path,
        "/cache/sandbox-1/snapshot"
    );

    // No two rows share the path; counting confirms exactly one
    // row owns the per-sandbox suspend dir after dedupe.
    let count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM local_artifacts WHERE local_path = ?1",
            params!["/cache/sandbox-1/snapshot"],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(count, 1);
}

/// Cross-node-restore variant of
/// `suspended_cleanup_completion_dedupes_prior_suspend_row`. A
/// sandbox that was previously suspended on this node (leaving a
/// `suspend`-origin row at the per-sandbox snapshot path) and is
/// later restored cross-node back to the same node lands in
/// `publish_restored_suspended_artifact`. Without dedupe the
/// stale row and the freshly-restored row share `local_path`,
/// the reaper's `SharedLocalPath` gate refuses to reap either,
/// and the reaper goes permanently inert for that artifact.
///
/// Regression: `publish_restored_suspended_artifact` DELETEs
/// every prior `artifact_origin='suspend'` row for the sandbox
/// in the same transaction so the new row is the sole owner.
#[test]
fn publish_restored_suspended_artifact_dedupes_prior_suspend_row() {
    let conn = test_conn();

    // Set the stage as if this node had suspended the sandbox
    // previously: a paused sandbox, suspend, publish, leaving a
    // `suspend`-origin row at the per-sandbox snapshot path.
    insert_test_sandbox(
        &conn,
        TestSandboxInsert::new("sandbox-1", "sandbox1", SandboxState::Paused),
    );
    begin_suspend(&conn, "sandbox-1").unwrap();
    let a1 = LocalArtifactId::new();
    complete_row_intent_cleanup(
        &conn,
        "sandbox-1",
        CleanupCompletion::MarkSuspended,
        Some((a1, "/cache/sandbox-1/snapshot")),
    )
    .unwrap();
    assert!(get_local_artifact(&conn, a1).unwrap().is_some());

    // Simulate the cross-node-restore cycle landing back here:
    // the sandbox row gets re-inserted into 'restoring' with
    // `current_local_artifact_id = NULL` (the orphan-adopt path),
    // but the prior `local_artifacts` row still sits at path P.
    conn.execute(
        "UPDATE sandboxes
         SET state = 'restoring',
             current_local_artifact_id = NULL,
             current_local_artifact_kind = NULL,
             current_local_artifact_origin = NULL
         WHERE id = 'sandbox-1'",
        [],
    )
    .unwrap();

    // Now publish the restored artifact at the same path.
    let a2 = LocalArtifactId::new();
    publish_restored_suspended_artifact(&conn, "sandbox-1", a2, "/cache/sandbox-1/snapshot")
        .unwrap();

    // A1's row must be gone — the dedupe is what keeps the
    // SharedLocalPath gate from going inert across the cross-node
    // restore path.
    assert!(
        get_local_artifact(&conn, a1).unwrap().is_none(),
        "prior suspend artifact row must be deleted by the restored publish"
    );
    assert_eq!(
        get_local_artifact(&conn, a2).unwrap().unwrap().local_path,
        "/cache/sandbox-1/snapshot"
    );
    let count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM local_artifacts WHERE local_path = ?1",
            params!["/cache/sandbox-1/snapshot"],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(count, 1);
}

/// The property, in operator terms: after a stopped sandbox is evacuated onto
/// a new node, the cluster can still find its bytes.
///
/// `sandboxes.current_local_artifact_id` is the sandbox's only handle on the
/// disk it was evacuated with: `GetArtifactBackupLocation` selects a ready
/// remote copy for one exact `local_artifact_id`
/// (`proto/node/api/v1/cluster.proto:1236`), which is how eviction proves
/// durability and how a later restore finds the bytes. A move that lands the
/// row in `stopped` without it produces a sandbox that looks healthy and whose
/// durable copy nothing can name.
#[test]
fn materializing_a_stopped_sandbox_keeps_the_artifact_pin_it_was_evacuated_with() {
    let conn = test_conn();
    insert_test_sandbox(
        &conn,
        TestSandboxInsert::new("sandbox-1", "sandbox1", SandboxState::Restoring),
    );
    conn.execute(
        "UPDATE sandboxes
         SET pid = 42,
             space_api_route_generation = 9,
             space_api_route_execution_generation = 7,
             restore_storage_uri = 's3://bucket/snapshot'
         WHERE id = 'sandbox-1'",
        [],
    )
    .unwrap();
    assert!(
        insert_restore_transfer_scope(
            &conn,
            "sandbox-1",
            pplx_space_common_core::types::TransferScopeId::new(),
            None,
            None,
            "/space/cache/restore-staging/evacuated-stopped",
        )
        .unwrap()
    );
    let evacuated = LocalArtifactId::new();

    publish_restored_stopped_artifact(&conn, "sandbox-1", evacuated, "/cache/sandbox-1/snapshot")
        .unwrap();

    let (state, pin, kind, origin): (String, Option<String>, Option<String>, Option<String>) = conn
        .query_row(
            "SELECT state, current_local_artifact_id, current_local_artifact_kind,
                    current_local_artifact_origin
             FROM sandboxes WHERE id = 'sandbox-1'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .unwrap();
    assert_eq!(state, SandboxState::Stopped.as_str());
    assert_eq!(
        pin.as_deref(),
        Some(evacuated.to_string().as_str()),
        "the materialized row must pin the artifact that was evacuated, not a fresh one"
    );
    assert_eq!(kind.as_deref(), Some(ArtifactKind::Disk.as_str()));
    assert_eq!(origin.as_deref(), Some(ArtifactOrigin::Stop.as_str()));

    // The pin has to name a row that exists: the eviction and backup paths
    // resolve it through `local_artifacts` to reach the bytes and their
    // `backup_completed_at` proof, so a dangling id is the same outage as a
    // null.
    let row = get_local_artifact(&conn, evacuated)
        .unwrap()
        .expect("the pinned artifact must have a local_artifacts row");
    assert_eq!(row.local_path, "/cache/sandbox-1/snapshot");
    assert_eq!(row.artifact_kind, ArtifactKind::Disk);
    assert_eq!(row.artifact_origin, ArtifactOrigin::Stop);
}

/// The two restore finalizes share one helper, and they disagree about
/// `failure_reason` — so the disagreement needs an assertion, not a comment.
///
/// The suspended publication has never cleared it; the stopped one does, like
/// every other restore finalize. Unifying them is defensible, but it would
/// change a path this work does not own, so the asymmetry is preserved. It was
/// invisible to the suite before this test: extracting the shared helper
/// silently applied the stopped arm's behaviour to both, and nothing failed.
#[test]
fn the_shared_restore_finalize_keeps_each_arms_failure_reason_behaviour() {
    let reason_after = |state: SandboxState, id: &str, uuid8: &str| -> Option<String> {
        let conn = test_conn();
        insert_test_sandbox(
            &conn,
            TestSandboxInsert::new(id, uuid8, SandboxState::Restoring),
        );
        conn.execute(
            "UPDATE sandboxes SET failure_reason = 'an earlier attempt failed' WHERE id = ?1",
            params![id],
        )
        .unwrap();
        let pin = LocalArtifactId::new();
        let path = format!("/cache/{id}/snapshot");
        match state {
            SandboxState::Stopped => {
                publish_restored_stopped_artifact(&conn, id, pin, &path).unwrap()
            }
            SandboxState::Suspended => {
                publish_restored_suspended_artifact(&conn, id, pin, &path).unwrap()
            }
            other => panic!("unexpected finalize target {other}"),
        }
        conn.query_row(
            "SELECT failure_reason FROM sandboxes WHERE id = ?1",
            params![id],
            |row| row.get(0),
        )
        .unwrap()
    };

    assert_eq!(
        reason_after(SandboxState::Stopped, "stopped-1", "stopped1"),
        None,
        "a materialized stopped sandbox starts clean, like every other restore finalize"
    );
    assert_eq!(
        reason_after(SandboxState::Suspended, "suspend-1", "suspend1"),
        Some("an earlier attempt failed".to_string()),
        "the suspended finalize has never cleared it; sharing a helper must not change that"
    );
}

/// The contrast that makes the third arm load-bearing rather than duplicative.
///
/// Both primitives land the row in `stopped` from `restoring`, so a reader --
/// or a future simplification pass -- can mistake them for the same operation.
/// They have opposite postconditions on the pin: the fork arm drops it so the
/// staged disk stays an immutable source for the child's cold boot, and the
/// evacuation arm keeps it because it is the sandbox's own disk. Asserting
/// them side by side is what makes a collapse of the two fail here.
#[test]
fn the_fork_arm_drops_the_pin_the_evacuation_arm_keeps() {
    let read_pin = |conn: &Connection, id: &str| -> Option<String> {
        conn.query_row(
            "SELECT current_local_artifact_id FROM sandboxes WHERE id = ?1",
            params![id],
            |row| row.get(0),
        )
        .unwrap()
    };

    let conn = test_conn();
    insert_test_sandbox(
        &conn,
        TestSandboxInsert::new("forked", "forked01", SandboxState::Restoring),
    );
    insert_test_sandbox(
        &conn,
        TestSandboxInsert::new("evacuated", "evacuat1", SandboxState::Restoring),
    );
    let evacuated_pin = LocalArtifactId::new();

    publish_restored_disk_as_new(&conn, "forked").unwrap();
    publish_restored_stopped_artifact(
        &conn,
        "evacuated",
        evacuated_pin,
        "/cache/evacuated/snapshot",
    )
    .unwrap();

    assert_eq!(
        read_pin(&conn, "forked"),
        None,
        "a fork must leave the staged disk unpinned so it cannot become the child's checkpoint"
    );
    assert_eq!(
        read_pin(&conn, "evacuated"),
        Some(evacuated_pin.to_string()),
        "an evacuated stopped sandbox must keep the pin its durable copy is found through"
    );
}

/// A materialized stopped sandbox was never launched on this node, so the row
/// must not describe a runtime or a route as if it had been. This mirrors
/// `publish_stopped_artifact`, which is what publishing a locally stopped
/// sandbox's disk leaves behind, in the runtime and route columns it clears.
#[test]
fn materializing_a_stopped_sandbox_leaves_no_runtime_or_route_identity() {
    let conn = test_conn();
    insert_test_sandbox(
        &conn,
        TestSandboxInsert::new("sandbox-1", "sandbox1", SandboxState::Restoring),
    );
    conn.execute(
        "UPDATE sandboxes
         SET pid = 42,
             guest_cid = 11,
             space_api_route_generation = 9,
             space_api_route_execution_generation = 7,
             restore_storage_uri = 's3://bucket/snapshot',
             restore_phase = 'artifact_staging'
         WHERE id = 'sandbox-1'",
        [],
    )
    .unwrap();

    publish_restored_stopped_artifact(
        &conn,
        "sandbox-1",
        LocalArtifactId::new(),
        "/cache/sandbox-1/snapshot",
    )
    .unwrap();

    let nulled = |column: &str| -> bool {
        conn.query_row(
            &format!("SELECT {column} IS NULL FROM sandboxes WHERE id = 'sandbox-1'"),
            [],
            |row| row.get::<_, bool>(0),
        )
        .unwrap()
    };

    for column in [
        "pid",
        "guest_cid",
        "space_api_route_generation",
        "space_api_route_execution_generation",
        "restore_storage_uri",
        "restore_phase",
    ] {
        assert!(
            nulled(column),
            "{column} still describes an activation this sandbox never had"
        );
    }
}

/// The dedupe removes what would collide, and only that.
///
/// Two things can collide with a materialization: a row already at the path it
/// claims (two rows sharing a path trip the reaper's `SharedLocalPath` gate,
/// which then reaps neither), and a row already holding the artifact id being
/// inserted, which is the table's primary key. A row of the same origin at a
/// different path owns different bytes and must survive.
#[test]
fn materializing_a_stopped_sandbox_removes_only_what_would_collide() {
    let conn = test_conn();
    insert_test_sandbox(
        &conn,
        TestSandboxInsert::new("sandbox-1", "sandbox1", SandboxState::Restoring),
    );
    let evacuated = LocalArtifactId::new();
    let materialized_path = format!("/cache/sandbox-1/snapshots/d-stop-{evacuated}");
    let materialized_path = materialized_path.as_str();

    // Collides on the path: any row already naming the path this claims.
    let stale_suspend = LocalArtifactId::new();
    insert_local_artifact(
        &conn,
        "sandbox-1",
        stale_suspend,
        ArtifactKind::Full,
        ArtifactOrigin::Suspend,
        None,
        materialized_path,
    )
    .unwrap();

    // Does NOT collide: a locally published stop artifact in its own
    // subvolume. Deleting it would orphan the subvolume, because the reaper is
    // the only thing that would ever free it.
    let local_stop = LocalArtifactId::new();
    insert_local_artifact(
        &conn,
        "sandbox-1",
        local_stop,
        ArtifactKind::Disk,
        ArtifactOrigin::Stop,
        None,
        "/cache/sandbox-1/snapshots/d-stop-earlier",
    )
    .unwrap();

    publish_restored_stopped_artifact(&conn, "sandbox-1", evacuated, materialized_path).unwrap();

    assert!(
        get_local_artifact(&conn, stale_suspend).unwrap().is_none(),
        "the row sharing the materialized path must go, or the reaper reaps neither"
    );
    assert!(
        get_local_artifact(&conn, local_stop).unwrap().is_some(),
        "a stop artifact in its own subvolume owns different bytes and must survive"
    );
    let at_path: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM local_artifacts WHERE local_path = ?1",
            params![materialized_path],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(at_path, 1);
}

/// The other collision: the artifact id is the table's primary key, so a row
/// already holding it — at any path — would fail the insert and roll the whole
/// publication back, leaving the sandbox `restoring` with every retry failing
/// the same way. The existing row is where a local publication of that
/// artifact would put it, `d-stop-{artifact}`, which is also the path the
/// stopped arm records, so replacing it strands nothing.
#[test]
fn materializing_a_stopped_sandbox_replaces_a_row_already_holding_that_artifact() {
    let conn = test_conn();
    insert_test_sandbox(
        &conn,
        TestSandboxInsert::new("sandbox-1", "sandbox1", SandboxState::Restoring),
    );
    let artifact = LocalArtifactId::new();
    let stop_path = format!("/cache/sandbox-1/snapshots/d-stop-{artifact}");
    insert_local_artifact(
        &conn,
        "sandbox-1",
        artifact,
        ArtifactKind::Disk,
        ArtifactOrigin::Stop,
        None,
        &stop_path,
    )
    .unwrap();

    publish_restored_stopped_artifact(&conn, "sandbox-1", artifact, &stop_path)
        .expect("re-materializing the same artifact must not collide on the primary key");

    let row = get_local_artifact(&conn, artifact).unwrap().unwrap();
    assert_eq!(row.local_path, stop_path);
    assert_eq!(
        conn.query_row(
            "SELECT state FROM sandboxes WHERE id = 'sandbox-1'",
            [],
            |row| row.get::<_, String>(0),
        )
        .unwrap(),
        SandboxState::Stopped.as_str(),
    );
}

/// The id half of the dedupe on its own: a row holding the artifact id at a
/// different path would collide on the primary key, so it is replaced even
/// though no row names the claimed path.
#[test]
fn materializing_a_stopped_sandbox_replaces_the_id_holder_at_another_path() {
    let conn = test_conn();
    insert_test_sandbox(
        &conn,
        TestSandboxInsert::new("sandbox-1", "sandbox1", SandboxState::Restoring),
    );
    let artifact = LocalArtifactId::new();
    insert_local_artifact(
        &conn,
        "sandbox-1",
        artifact,
        ArtifactKind::Disk,
        ArtifactOrigin::Stop,
        None,
        "/cache/sandbox-1/elsewhere",
    )
    .unwrap();
    let stop_path = format!("/cache/sandbox-1/snapshots/d-stop-{artifact}");

    publish_restored_stopped_artifact(&conn, "sandbox-1", artifact, &stop_path)
        .expect("the id holder at another path must be replaced, not collide");

    assert_eq!(
        get_local_artifact(&conn, artifact)
            .unwrap()
            .unwrap()
            .local_path,
        stop_path
    );
}

/// Fails closed on a row that is not the restore this finalize owns. The
/// `restoring` + unpinned guard is what stops a materialization that lost its
/// claim from overwriting the pin of whatever now holds the row.
#[test]
fn materializing_a_stopped_sandbox_refuses_a_row_that_left_restoring() {
    let conn = test_conn();
    insert_test_sandbox(
        &conn,
        TestSandboxInsert::new("sandbox-1", "sandbox1", SandboxState::Started),
    );

    let error = publish_restored_stopped_artifact(
        &conn,
        "sandbox-1",
        LocalArtifactId::new(),
        "/cache/sandbox-1/snapshot",
    )
    .expect_err("a row that is no longer restoring must not be republished as stopped");

    assert!(error.is_no_rows(), "{error:?}");
    let state: String = conn
        .query_row(
            "SELECT state FROM sandboxes WHERE id = 'sandbox-1'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(
        state,
        SandboxState::Started.as_str(),
        "the refusal must leave the row it did not own untouched"
    );
    let artifacts: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM local_artifacts WHERE sandbox_id = 'sandbox-1'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(
        artifacts, 0,
        "the whole publish is one transaction; a refused UPDATE must roll back its INSERT"
    );
}

/// The other half of the guard: a `restoring` row that already pins an
/// artifact belongs to a finalize that already ran, so a second one must not
/// overwrite its pin.
#[test]
fn materializing_a_stopped_sandbox_refuses_a_row_that_already_pins() {
    let conn = test_conn();
    insert_test_sandbox(
        &conn,
        TestSandboxInsert::new("sandbox-1", "sandbox1", SandboxState::Restoring),
    );
    let existing = LocalArtifactId::new();
    conn.execute(
        "UPDATE sandboxes
         SET current_local_artifact_id = ?1,
             current_local_artifact_kind = 'disk',
             current_local_artifact_origin = 'stop'
         WHERE id = 'sandbox-1'",
        params![existing.to_string()],
    )
    .unwrap();

    let error = publish_restored_stopped_artifact(
        &conn,
        "sandbox-1",
        LocalArtifactId::new(),
        "/cache/sandbox-1/snapshot",
    )
    .expect_err("a restoring row that already pins must not be republished");

    assert!(error.is_no_rows(), "{error:?}");
    let pinned: String = conn
        .query_row(
            "SELECT current_local_artifact_id FROM sandboxes WHERE id = 'sandbox-1'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(pinned, existing.to_string());
}

#[test]
fn restored_disk_as_new_becomes_a_stopped_sandbox_without_donor_runtime_identity() {
    let conn = test_conn();
    insert_test_sandbox(
        &conn,
        TestSandboxInsert::new("sandbox-1", "sandbox1", SandboxState::Restoring),
    );
    conn.execute(
        "UPDATE sandboxes
         SET pid = 42,
             space_api_route_generation = 9,
             space_api_route_execution_generation = 7,
             restore_storage_uri = 's3://bucket/snapshot'
         WHERE id = 'sandbox-1'",
        [],
    )
    .unwrap();
    assert!(
        insert_restore_transfer_scope(
            &conn,
            "sandbox-1",
            pplx_space_common_core::types::TransferScopeId::new(),
            None,
            None,
            "/space/cache/restore-staging/artifact-as-new",
        )
        .unwrap()
    );

    publish_restored_disk_as_new(&conn, "sandbox-1").unwrap();

    let (state, pid, route_generation, restore_uri): (
        String,
        Option<i64>,
        Option<i64>,
        Option<String>,
    ) = conn
        .query_row(
            "SELECT state, pid, space_api_route_generation, restore_storage_uri
             FROM sandboxes WHERE id = 'sandbox-1'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .unwrap();
    assert_eq!(state, SandboxState::Stopped.as_str());
    assert_eq!(pid, None);
    assert_eq!(route_generation, None);
    assert_eq!(restore_uri, None);
    let transfer_scopes: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM sandbox_restore_transfer_scopes WHERE sandbox_id = 'sandbox-1'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(
        transfer_scopes, 0,
        "successful publication retires staging ownership"
    );
}

#[test]
fn legacy_suspended_artifact_adoption_keeps_first_published_identity() {
    let conn = test_conn();
    insert_test_sandbox(
        &conn,
        TestSandboxInsert::new("sandbox-1", "sandbox1", SandboxState::Suspended),
    );

    let first = LocalArtifactId::new();
    let second = LocalArtifactId::new();
    assert!(
        adopt_suspended_local_artifact(&conn, "sandbox-1", first, "/cache/sandbox-1/snapshot",)
            .unwrap()
    );
    assert!(
        !adopt_suspended_local_artifact(&conn, "sandbox-1", second, "/cache/sandbox-1/snapshot",)
            .unwrap()
    );

    let row = get_sandbox(&conn, "sandbox-1").unwrap().unwrap();
    assert_eq!(row.current_local_artifact_id, Some(first));
    assert_eq!(row.current_local_artifact_kind, Some(ArtifactKind::Full));
}

#[test]
fn legacy_suspended_artifact_repair_adoption_preserves_idle_timestamp() {
    let conn = test_conn();
    insert_test_sandbox(
        &conn,
        TestSandboxInsert::new("sandbox-1", "sandbox1", SandboxState::Suspended),
    );
    conn.execute(
        "UPDATE sandboxes SET updated_at = '1000' WHERE id = 'sandbox-1'",
        [],
    )
    .unwrap();

    let local_artifact_id = LocalArtifactId::new();
    assert!(
        adopt_suspended_local_artifact_preserving_updated_at(
            &conn,
            "sandbox-1",
            local_artifact_id,
            "/cache/sandbox-1/snapshot",
        )
        .unwrap()
    );

    let row = get_sandbox(&conn, "sandbox-1").unwrap().unwrap();
    assert_eq!(row.current_local_artifact_id, Some(local_artifact_id));
    assert_eq!(row.updated_at, "1000");
}

#[test]
fn stopped_publication_commits_disk_identity_with_terminal_state() {
    let conn = test_conn();
    insert_test_sandbox(
        &conn,
        TestSandboxInsert::new("sandbox-1", "sandbox1", SandboxState::Started),
    );
    state_write_fixtures::apply_edge(&conn, "sandbox-1", &edges::STOP_ADMISSION);

    let local_artifact_id = LocalArtifactId::new();
    publish_stopped_artifact(
        &conn,
        "sandbox-1",
        get_sandbox(&conn, "sandbox-1")
            .unwrap()
            .unwrap()
            .node_execution_id,
        local_artifact_id,
        "/cache/sandbox-1/snapshots/d-stop-id",
    )
    .unwrap();

    let row = get_sandbox(&conn, "sandbox-1").unwrap().unwrap();
    assert_eq!(row.state, SandboxState::Stopped);
    assert_eq!(row.current_local_artifact_id, Some(local_artifact_id));
    assert_eq!(row.current_local_artifact_kind, Some(ArtifactKind::Disk));
    assert_eq!(
        row.current_local_artifact_origin,
        Some(ArtifactOrigin::Stop)
    );
    let artifact = get_local_artifact(&conn, local_artifact_id)
        .unwrap()
        .unwrap();
    assert_eq!(artifact.artifact_kind, ArtifactKind::Disk);
    assert_eq!(artifact.artifact_origin, ArtifactOrigin::Stop);
}

#[test]
fn delete_sandbox_removes_local_artifacts() {
    let conn = test_conn();
    insert_test_sandbox(
        &conn,
        TestSandboxInsert::new("sandbox-1", "sandbox1", SandboxState::Stopped),
    );

    let current_id = LocalArtifactId::new();
    publish_stopped_artifact(
        &conn,
        "sandbox-1",
        get_sandbox(&conn, "sandbox-1")
            .unwrap()
            .unwrap()
            .node_execution_id,
        current_id,
        "/cache/sandbox-1/snapshots/d-stop-id",
    )
    .unwrap();
    let named_id = LocalArtifactId::new();
    let snapshot_name = SnapshotName::parse("d-manual-20260605T000000Z").unwrap();
    publish_named_local_artifact(
        &conn,
        "sandbox-1",
        named_id,
        ArtifactKind::Disk,
        &snapshot_name,
        "/cache/sandbox-1/snapshots/d-manual-20260605T000000Z",
    )
    .unwrap();

    delete_sandbox(&conn, "sandbox-1").unwrap();

    let artifact_count: u32 = conn
        .query_row(
            "SELECT COUNT(*) FROM local_artifacts WHERE sandbox_id = ?1",
            params!["sandbox-1"],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(artifact_count, 0);
}

#[test]
fn delete_sandbox_removes_local_artifact_orphans() {
    let conn = test_conn();
    insert_local_artifact(
        &conn,
        "missing-sandbox",
        LocalArtifactId::new(),
        ArtifactKind::Disk,
        ArtifactOrigin::Snapshot,
        Some(&SnapshotName::parse("d-manual-20260605T000000Z").unwrap()),
        "/cache/missing-sandbox/snapshots/d-manual-20260605T000000Z",
    )
    .unwrap();

    delete_sandbox(&conn, "missing-sandbox").unwrap();

    let artifact_count: u32 = conn
        .query_row(
            "SELECT COUNT(*) FROM local_artifacts WHERE sandbox_id = ?1",
            params!["missing-sandbox"],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(artifact_count, 0);
}

#[test]
fn network_policy_override_round_trips_and_can_clear() {
    let conn = test_conn();

    insert_test_sandbox(
        &conn,
        TestSandboxInsert {
            network_policy_override: Some(NetworkPolicyV1::deny_all()),
            network_policy_generation: 1,
            ..TestSandboxInsert::new("sandbox-1", "sandbox1", SandboxState::Creating)
        },
    );

    let row = get_sandbox(&conn, "sandbox-1").unwrap().unwrap();
    assert_eq!(
        row.network_policy_override,
        Some(NetworkPolicyV1::deny_all())
    );
    assert_eq!(row.network_policy_generation, 1);

    let outcome = update_network_policy_override(
        &conn,
        "sandbox-1",
        None,
        2,
        PolicyAuthority::Cluster,
        false,
        false,
    )
    .unwrap();
    assert_eq!(outcome.decision, NetworkPolicyOverrideUpdate::Apply);

    let row = get_sandbox(&conn, "sandbox-1").unwrap().unwrap();
    assert!(row.network_policy_override.is_none());
}

#[test]
fn network_policy_generations_are_monotonic_and_applied_is_separate() {
    let conn = test_conn();

    insert_test_sandbox(
        &conn,
        TestSandboxInsert {
            network_policy_override: Some(NetworkPolicyV1::deny_all()),
            network_policy_generation: 1,
            ..TestSandboxInsert::new("sandbox-1", "sandbox1", SandboxState::Creating)
        },
    );

    let outcome = update_network_policy_override(
        &conn,
        "sandbox-1",
        None,
        1,
        PolicyAuthority::Cluster,
        false,
        false,
    )
    .unwrap();
    assert_eq!(outcome.decision, NetworkPolicyOverrideUpdate::Conflict);
    let row = get_sandbox(&conn, "sandbox-1").unwrap().unwrap();
    assert_eq!(row.network_policy_generation, 1);
    assert_eq!(
        row.network_policy_override,
        Some(NetworkPolicyV1::deny_all())
    );

    let outcome = update_network_policy_override(
        &conn,
        "sandbox-1",
        None,
        2,
        PolicyAuthority::Cluster,
        false,
        false,
    )
    .unwrap();
    assert_eq!(outcome.decision, NetworkPolicyOverrideUpdate::Apply);
    let row = get_sandbox(&conn, "sandbox-1").unwrap().unwrap();
    assert_eq!(row.network_policy_generation, 2);
    assert!(row.network_policy_override.is_none());

    let outcome = update_network_policy_override(
        &conn,
        "sandbox-1",
        Some(&NetworkPolicyV1::allow_all()),
        2,
        PolicyAuthority::Cluster,
        false,
        false,
    )
    .unwrap();
    assert_eq!(outcome.decision, NetworkPolicyOverrideUpdate::Conflict);
    let row = get_sandbox(&conn, "sandbox-1").unwrap().unwrap();
    assert_eq!(row.network_policy_generation, 2);
    assert!(row.network_policy_override.is_none());

    let outcome = update_network_policy_override(
        &conn,
        "sandbox-1",
        Some(&NetworkPolicyV1::allow_all()),
        3,
        PolicyAuthority::Cluster,
        false,
        false,
    )
    .unwrap();
    assert_eq!(outcome.decision, NetworkPolicyOverrideUpdate::Apply);
    let row = get_sandbox(&conn, "sandbox-1").unwrap().unwrap();
    assert_eq!(row.network_policy_generation, 3);
    assert_eq!(
        row.network_policy_override,
        Some(NetworkPolicyV1::allow_all())
    );

    let outcome = update_network_policy_override(
        &conn,
        "sandbox-1",
        Some(&NetworkPolicyV1::deny_all()),
        3,
        PolicyAuthority::Cluster,
        false,
        false,
    )
    .unwrap();
    assert_eq!(outcome.decision, NetworkPolicyOverrideUpdate::Conflict);

    let outcome = update_network_policy_override(
        &conn,
        "sandbox-1",
        None,
        4,
        PolicyAuthority::Cluster,
        false,
        false,
    )
    .unwrap();
    assert_eq!(outcome.decision, NetworkPolicyOverrideUpdate::Apply);
    assert!(
        mark_network_policy_applied(&conn, "sandbox-1", NetworkPolicyAppliedSource::Override, 1)
            .unwrap()
    );
    assert!(
        !mark_network_policy_applied(&conn, "sandbox-1", NetworkPolicyAppliedSource::Override, 1)
            .unwrap()
    );
    assert!(
        mark_network_policy_applied(&conn, "sandbox-1", NetworkPolicyAppliedSource::Template, 1)
            .unwrap()
    );

    let row = get_sandbox(&conn, "sandbox-1").unwrap().unwrap();
    assert_eq!(row.network_policy_generation, 4);
    assert_eq!(row.applied_network_policy_generation, 1);
    assert_eq!(
        row.applied_network_policy_source,
        NetworkPolicyAppliedSource::Template
    );
    assert!(row.network_policy_override.is_none());
}

#[test]
fn network_policy_source_changes_cannot_roll_back_generation() {
    let conn = test_conn();

    insert_test_sandbox(
        &conn,
        TestSandboxInsert {
            network_policy_override: Some(NetworkPolicyV1::allow_all()),
            network_policy_generation: 5,
            ..TestSandboxInsert::new("sandbox-1", "sandbox1", SandboxState::Started)
        },
    );

    let outcome = update_network_policy_override(
        &conn,
        "sandbox-1",
        None,
        4,
        PolicyAuthority::Cluster,
        false,
        false,
    )
    .unwrap();
    assert_eq!(outcome.decision, NetworkPolicyOverrideUpdate::Skip);
    let row = get_sandbox(&conn, "sandbox-1").unwrap().unwrap();
    assert_eq!(row.network_policy_generation, 5);
    assert_eq!(
        row.network_policy_override,
        Some(NetworkPolicyV1::allow_all())
    );

    let outcome = update_network_policy_override(
        &conn,
        "sandbox-1",
        None,
        5,
        PolicyAuthority::Cluster,
        false,
        false,
    )
    .unwrap();
    assert_eq!(outcome.decision, NetworkPolicyOverrideUpdate::Conflict);
    let row = get_sandbox(&conn, "sandbox-1").unwrap().unwrap();
    assert_eq!(row.network_policy_generation, 5);
    assert_eq!(
        row.network_policy_override,
        Some(NetworkPolicyV1::allow_all())
    );

    let outcome = update_network_policy_override(
        &conn,
        "sandbox-1",
        None,
        6,
        PolicyAuthority::Cluster,
        false,
        false,
    )
    .unwrap();
    assert_eq!(outcome.decision, NetworkPolicyOverrideUpdate::Apply);
    let row = get_sandbox(&conn, "sandbox-1").unwrap().unwrap();
    assert_eq!(row.network_policy_generation, 6);
    assert!(row.network_policy_override.is_none());

    let outcome = update_network_policy_override(
        &conn,
        "sandbox-1",
        Some(&NetworkPolicyV1::deny_all()),
        5,
        PolicyAuthority::Cluster,
        false,
        false,
    )
    .unwrap();
    assert_eq!(outcome.decision, NetworkPolicyOverrideUpdate::Skip);
    let row = get_sandbox(&conn, "sandbox-1").unwrap().unwrap();
    assert_eq!(row.network_policy_generation, 6);
    assert!(row.network_policy_override.is_none());

    let outcome = update_network_policy_override(
        &conn,
        "sandbox-1",
        Some(&NetworkPolicyV1::deny_all()),
        6,
        PolicyAuthority::Cluster,
        false,
        false,
    )
    .unwrap();
    assert_eq!(outcome.decision, NetworkPolicyOverrideUpdate::Conflict);
    let row = get_sandbox(&conn, "sandbox-1").unwrap().unwrap();
    assert_eq!(row.network_policy_generation, 6);
    assert!(row.network_policy_override.is_none());

    let outcome = update_network_policy_override(
        &conn,
        "sandbox-1",
        Some(&NetworkPolicyV1::deny_all()),
        7,
        PolicyAuthority::Cluster,
        false,
        false,
    )
    .unwrap();

    assert_eq!(outcome.decision, NetworkPolicyOverrideUpdate::Apply);
    let row = get_sandbox(&conn, "sandbox-1").unwrap().unwrap();
    assert_eq!(row.network_policy_generation, 7);
    assert_eq!(
        row.network_policy_override,
        Some(NetworkPolicyV1::deny_all())
    );
}

#[test]
fn network_policy_override_update_decision_covers_authority_generation_and_body() {
    use NetworkPolicyOverrideUpdatePlan::{Apply, Conflict, RepairIfLiveGateway, Skip};

    let cluster_existing = NetworkPolicyOverrideExisting {
        json: Some("allow"),
        generation: 4,
        authority: PolicyAuthority::Cluster,
    };
    let local_existing = NetworkPolicyOverrideExisting {
        json: Some("allow"),
        generation: 0,
        authority: PolicyAuthority::Local,
    };

    assert_eq!(
        decide_network_policy_override_update(
            local_existing,
            NetworkPolicyOverrideRequested {
                json: Some("deny"),
                generation: 5,
                authority: PolicyAuthority::Cluster,
            },
        ),
        Skip,
        "cluster updates must not overwrite a local policy"
    );
    assert_eq!(
        decide_network_policy_override_update(
            local_existing,
            NetworkPolicyOverrideRequested {
                json: Some("allow"),
                generation: 0,
                authority: PolicyAuthority::Local,
            },
        ),
        RepairIfLiveGateway,
        "idempotent local updates only repair live gateway state"
    );
    assert_eq!(
        decide_network_policy_override_update(
            cluster_existing,
            NetworkPolicyOverrideRequested {
                json: Some("allow"),
                generation: 0,
                authority: PolicyAuthority::Local,
            },
        ),
        Apply {
            generation: 0,
            authority: PolicyAuthority::Local,
        },
        "local updates intentionally replace cluster state without a cluster generation"
    );
    assert_eq!(
        decide_network_policy_override_update(
            cluster_existing,
            NetworkPolicyOverrideRequested {
                json: Some("deny"),
                generation: 3,
                authority: PolicyAuthority::Cluster,
            },
        ),
        Skip,
        "stale cluster generations are ignored"
    );
    assert_eq!(
        decide_network_policy_override_update(
            cluster_existing,
            NetworkPolicyOverrideRequested {
                json: Some("deny"),
                generation: 4,
                authority: PolicyAuthority::Cluster,
            },
        ),
        Conflict,
        "same-generation cluster writes with different content conflict"
    );
    assert_eq!(
        decide_network_policy_override_update(
            cluster_existing,
            NetworkPolicyOverrideRequested {
                json: Some("allow"),
                generation: 4,
                authority: PolicyAuthority::Cluster,
            },
        ),
        RepairIfLiveGateway,
        "same-generation cluster writes with identical content are idempotent"
    );
    assert_eq!(
        decide_network_policy_override_update(
            cluster_existing,
            NetworkPolicyOverrideRequested {
                json: Some("deny"),
                generation: 5,
                authority: PolicyAuthority::Cluster,
            },
        ),
        Apply {
            generation: 5,
            authority: PolicyAuthority::Cluster,
        },
        "newer cluster generations apply"
    );
}

#[test]
fn broken_network_policy_override_reads_as_deny_all_override() {
    let conn = test_conn();

    insert_test_sandbox(
        &conn,
        TestSandboxInsert {
            pool_template: Some("pool-seed".to_string()),
            ..TestSandboxInsert::new("sandbox-1", "sandbox1", SandboxState::Paused)
        },
    );
    conn.execute(
        "UPDATE sandboxes SET network_policy_override_json = ?1 WHERE id = ?2",
        rusqlite::params![
            "{\"version\":\"v9\",\"egress\":{\"default_action\":\"deny\"}}",
            "sandbox-1"
        ],
    )
    .unwrap();

    let row = get_sandbox(&conn, "sandbox-1").unwrap().unwrap();
    assert_eq!(
        row.network_policy_override,
        Some(NetworkPolicyV1::deny_all())
    );
    assert!(row.network_policy_override_is_corrupt_fallback);

    let rows = list_sandboxes(&conn).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].network_policy_override,
        Some(NetworkPolicyV1::deny_all())
    );
    assert!(rows[0].network_policy_override_is_corrupt_fallback);

    let pool_row = find_pool_sandbox(&conn, "pool-seed", None, None)
        .unwrap()
        .unwrap();
    assert_eq!(
        pool_row.network_policy_override,
        Some(NetworkPolicyV1::deny_all())
    );
    assert!(pool_row.network_policy_override_is_corrupt_fallback);
}

#[test]
fn claim_pool_sandbox_claims_matching_required_user() {
    let conn = test_conn();

    insert_test_sandbox(
        &conn,
        TestSandboxInsert {
            user: pplx_space_common_core::types::TEMPLATE_MANAGER_SANDBOX_OWNER.to_string(),
            pool_template: Some("pool-seed".to_string()),
            ..TestSandboxInsert::new("tmplmgr-pool", "tmplpool", SandboxState::Paused)
        },
    );
    insert_test_sandbox(
        &conn,
        TestSandboxInsert {
            user: tenant_owner().to_string(),
            pool_template: Some("pool-seed".to_string()),
            ..TestSandboxInsert::new("tenant-pool", "tenantpl", SandboxState::Paused)
        },
    );

    let mut adopted_env = BTreeMap::new();
    adopted_env.insert("SPACE_BUILD".to_string(), "1".to_string());
    let adopter_owner = adopter_owner();
    let donor = claim_pool_sandbox(
        &conn,
        "tmplmgr-pool",
        "pool-seed",
        Some("hash"),
        Some(&pplx_space_common_core::types::TEMPLATE_MANAGER_SANDBOX_OWNER),
        &adopter_owner,
        &adopted_env,
    )
    .unwrap()
    .unwrap();

    assert_eq!(donor.id, "tmplmgr-pool");
    let adopted = get_sandbox(&conn, "tmplmgr-pool").unwrap().unwrap();
    assert_eq!(adopted.pool_template, None);
    assert_eq!(adopted.user, adopter_owner);
    assert_eq!(adopted.process_env, adopted_env);

    assert!(
        claim_pool_sandbox(
            &conn,
            "tmplmgr-pool",
            "pool-seed",
            Some("hash"),
            None,
            &adopter_owner,
            &BTreeMap::new(),
        )
        .unwrap()
        .is_none(),
        "a stale fenced selection must not claim another available row"
    );

    let unclaimed = get_sandbox(&conn, "tenant-pool").unwrap().unwrap();
    assert_eq!(unclaimed.pool_template.as_deref(), Some("pool-seed"));
    assert_eq!(unclaimed.user, tenant_owner());
}

#[test]
fn pool_adoption_rollback_restores_donor_route_identity() {
    let conn = test_conn();
    let sandbox_id = SandboxId::new().to_string();
    insert_test_sandbox(
        &conn,
        TestSandboxInsert {
            user: pplx_space_common_core::types::TEMPLATE_MANAGER_SANDBOX_OWNER.to_string(),
            pool_template: Some("pool-seed".to_string()),
            ..TestSandboxInsert::new(&sandbox_id, "tmplpool", SandboxState::Paused)
        },
    );
    let donor_identity_update = SpaceApiRouteIdentity {
        route_generation: Some(pplx_space_common_core::types::RouteGeneration::new(7).unwrap()),
        ..Default::default()
    };
    update_space_api_route_identity(
        &conn,
        sandbox_id.parse().unwrap(),
        &donor_identity_update,
        crate::auto_lifecycle_delete_receipts::RouteIdentityProvenance::Unfenced,
    )
    .unwrap();
    let donor_identity = get_sandbox(&conn, &sandbox_id)
        .unwrap()
        .unwrap()
        .space_api_route_identity;

    let adopter_owner = adopter_owner();
    let donor = claim_pool_sandbox(
        &conn,
        &sandbox_id,
        "pool-seed",
        Some("hash"),
        Some(&pplx_space_common_core::types::TEMPLATE_MANAGER_SANDBOX_OWNER),
        &adopter_owner,
        &BTreeMap::new(),
    )
    .unwrap()
    .unwrap();
    let adopted_identity = SpaceApiRouteIdentity {
        route_generation: Some(pplx_space_common_core::types::RouteGeneration::new(8).unwrap()),
        ..Default::default()
    };
    update_space_api_route_identity(
        &conn,
        sandbox_id.parse().unwrap(),
        &adopted_identity,
        crate::auto_lifecycle_delete_receipts::RouteIdentityProvenance::Unfenced,
    )
    .unwrap();

    assert_eq!(
        restore_pool_donor_if_adopted(&conn, sandbox_id.parse().unwrap(), &donor).unwrap(),
        PoolDonorRestoreDisposition::Restored
    );
    let restored = get_sandbox(&conn, &sandbox_id).unwrap().unwrap();
    assert_eq!(restored.space_api_route_identity, donor_identity);

    let donor = claim_pool_sandbox(
        &conn,
        &sandbox_id,
        "pool-seed",
        Some("hash"),
        Some(&pplx_space_common_core::types::TEMPLATE_MANAGER_SANDBOX_OWNER),
        &adopter_owner,
        &BTreeMap::new(),
    )
    .unwrap()
    .unwrap();
    update_space_api_route_identity(
        &conn,
        sandbox_id.parse().unwrap(),
        &adopted_identity,
        crate::auto_lifecycle_delete_receipts::RouteIdentityProvenance::Unfenced,
    )
    .unwrap();
    let adopted_before_fence = get_sandbox(&conn, &sandbox_id).unwrap().unwrap();
    conn.execute(
        "UPDATE sandboxes SET state = 'suspending' WHERE id = ?1",
        [&sandbox_id],
    )
    .unwrap();

    assert_eq!(
        restore_pool_donor_if_adopted(&conn, sandbox_id.parse().unwrap(), &donor).unwrap(),
        PoolDonorRestoreDisposition::LifecycleOwned,
        "a transient lifecycle fence must not look like a never-adopted row"
    );
    let cleanup_owned = get_sandbox(&conn, &sandbox_id).unwrap().unwrap();
    assert_eq!(cleanup_owned.state, SandboxState::Suspending);
    assert_eq!(
        cleanup_owned.space_api_route_identity,
        adopted_before_fence.space_api_route_identity
    );
    assert_eq!(
        cleanup_owned.pool_template,
        adopted_before_fence.pool_template
    );
    assert_eq!(cleanup_owned.user, adopted_before_fence.user);
    assert_eq!(cleanup_owned.process_env, adopted_before_fence.process_env);
    assert_eq!(
        cleanup_owned.auto_lifecycle,
        adopted_before_fence.auto_lifecycle
    );
}

#[test]
fn claim_pool_sandbox_returns_none_without_required_user_match() {
    let conn = test_conn();

    insert_test_sandbox(
        &conn,
        TestSandboxInsert {
            user: tenant_owner().to_string(),
            pool_template: Some("pool-seed".to_string()),
            ..TestSandboxInsert::new("tenant-pool", "tenantpl", SandboxState::Paused)
        },
    );

    let adopter_owner = adopter_owner();
    let donor = claim_pool_sandbox(
        &conn,
        "tenant-pool",
        "pool-seed",
        Some("hash"),
        Some(&pplx_space_common_core::types::TEMPLATE_MANAGER_SANDBOX_OWNER),
        &adopter_owner,
        &BTreeMap::new(),
    )
    .unwrap();

    assert!(donor.is_none());
    let row = get_sandbox(&conn, "tenant-pool").unwrap().unwrap();
    assert_eq!(row.pool_template.as_deref(), Some("pool-seed"));
    assert_eq!(row.user, tenant_owner());
}

#[test]
fn set_sandbox_owner_if_current_updates_owner() {
    let conn = test_conn();

    let mut env = BTreeMap::new();
    env.insert("SPACE_SANDBOX".to_string(), "original".to_string());
    insert_test_sandbox(
        &conn,
        TestSandboxInsert {
            user: "root@local".to_string(),
            process_env: env.clone(),
            ..TestSandboxInsert::new("sandbox-1", "sandbox1", SandboxState::Started)
        },
    );

    let adopter_owner = adopter_owner();
    let previous = set_sandbox_owner_if_current(
        &conn,
        "sandbox-1",
        &SandboxOwner::parse("root@local").unwrap(),
        &adopter_owner,
    )
    .unwrap()
    .unwrap();

    assert_eq!(previous.user.as_str(), "root@local");
    let updated = get_sandbox(&conn, "sandbox-1").unwrap().unwrap();
    assert_eq!(updated.user, adopter_owner);
    assert_eq!(updated.process_env, env);
}

#[test]
fn set_sandbox_owner_if_current_accepts_already_applied_owner() {
    let conn = test_conn();

    let adopter_owner = adopter_owner();
    insert_test_sandbox(
        &conn,
        TestSandboxInsert {
            user: adopter_owner.to_string(),
            ..TestSandboxInsert::new("sandbox-1", "sandbox1", SandboxState::Started)
        },
    );

    let previous = set_sandbox_owner_if_current(
        &conn,
        "sandbox-1",
        &SandboxOwner::parse("root@local").unwrap(),
        &adopter_owner,
    )
    .unwrap();

    assert!(previous.is_some());
    let row = get_sandbox(&conn, "sandbox-1").unwrap().unwrap();
    assert_eq!(row.user, adopter_owner);
}

#[test]
fn set_sandbox_owner_if_current_rejects_unexpected_owner() {
    let conn = test_conn();

    insert_test_sandbox(
        &conn,
        TestSandboxInsert {
            user: tenant_owner().to_string(),
            ..TestSandboxInsert::new("sandbox-1", "sandbox1", SandboxState::Started)
        },
    );

    let adopter_owner = adopter_owner();
    let previous = set_sandbox_owner_if_current(
        &conn,
        "sandbox-1",
        &SandboxOwner::parse("root@local").unwrap(),
        &adopter_owner,
    )
    .unwrap();

    assert!(previous.is_none());
    let row = get_sandbox(&conn, "sandbox-1").unwrap().unwrap();
    assert_eq!(row.user, tenant_owner());
}

#[test]
fn broken_network_policy_override_can_be_healed_by_bumping_generation() {
    let conn = test_conn();

    insert_test_sandbox(
        &conn,
        TestSandboxInsert::new("sandbox-1", "sandbox1", SandboxState::Creating),
    );
    conn.execute(
        "UPDATE sandboxes SET network_policy_override_json = ?1 WHERE id = ?2",
        rusqlite::params![
            "{\"version\":\"v9\",\"egress\":{\"default_action\":\"deny\"}}",
            "sandbox-1"
        ],
    )
    .unwrap();

    let row = get_sandbox(&conn, "sandbox-1").unwrap().unwrap();
    assert!(row.network_policy_override_is_corrupt_fallback);

    let outcome = update_network_policy_override(
        &conn,
        "sandbox-1",
        Some(&NetworkPolicyV1::deny_all()),
        row.network_policy_generation.saturating_add(1).max(1),
        PolicyAuthority::Cluster,
        false,
        false,
    )
    .unwrap();
    assert_eq!(outcome.decision, NetworkPolicyOverrideUpdate::Apply);

    let row = get_sandbox(&conn, "sandbox-1").unwrap().unwrap();
    assert_eq!(
        row.network_policy_override,
        Some(NetworkPolicyV1::deny_all())
    );
    assert!(!row.network_policy_override_is_corrupt_fallback);
    assert_eq!(row.network_policy_generation, 1);
}

#[test]
fn network_counters_round_trip() {
    let conn = test_conn();

    insert_test_sandbox(
        &conn,
        TestSandboxInsert::new("sandbox-1", "sandbox1", SandboxState::Creating),
    );

    let row = get_sandbox(&conn, "sandbox-1").unwrap().unwrap();
    assert_eq!(row.network_tx_bytes, 0);
    assert_eq!(row.network_rx_bytes, 0);

    update_network_counters(&conn, "sandbox-1", 123, 456).unwrap();

    let row = get_sandbox(&conn, "sandbox-1").unwrap().unwrap();
    assert_eq!(row.network_tx_bytes, 123);
    assert_eq!(row.network_rx_bytes, 456);
}

#[test]
fn network_counter_probes_that_carry_no_new_bytes_leave_the_row_version_alone() {
    // The watchdog re-reads every resident sandbox's gateway every 30 s. On
    // an idle sandbox those reads are all the same number, and bumping
    // `updated_at` for them would hold the row inside every window that
    // reads it — including the cluster-deleted eviction's.
    let conn = test_conn();
    insert_test_sandbox(
        &conn,
        TestSandboxInsert::new("sandbox-1", "sandbox1", SandboxState::Creating),
    );
    update_network_counters(&conn, "sandbox-1", 123, 456).unwrap();
    conn.execute(
        "UPDATE sandboxes SET updated_at = '1000000000' WHERE id = 'sandbox-1'",
        [],
    )
    .unwrap();

    update_network_counters(&conn, "sandbox-1", 123, 456).unwrap();
    assert_eq!(
        get_sandbox(&conn, "sandbox-1").unwrap().unwrap().updated_at,
        "1000000000",
        "an identical observation is not news"
    );
    // A lower reading is a restarted gateway's counters, not traffic; `MAX`
    // already ignores it and so must the row version.
    update_network_counters(&conn, "sandbox-1", 1, 2).unwrap();
    assert_eq!(
        get_sandbox(&conn, "sandbox-1").unwrap().unwrap().updated_at,
        "1000000000"
    );

    update_network_counters(&conn, "sandbox-1", 123, 457).unwrap();
    let row = get_sandbox(&conn, "sandbox-1").unwrap().unwrap();
    assert_eq!((row.network_tx_bytes, row.network_rx_bytes), (123, 457));
    assert_ne!(
        row.updated_at, "1000000000",
        "real traffic must still stamp the row"
    );
}

#[test]
fn re_recording_an_unchanged_auto_lifecycle_state_leaves_the_row_version_alone() {
    // A due stage that keeps failing the same way re-derives a byte-identical
    // runtime state every scheduler tick. Persisting it anyway would bump
    // `updated_at` every few seconds on a row that never moved, which is how
    // a stranded `suspending` row stayed inside the cluster-deleted
    // eviction's quiescence window forever.
    let conn = test_conn();
    insert_test_sandbox(
        &conn,
        TestSandboxInsert::new("sandbox-1", "sandbox1", SandboxState::Creating),
    );
    update_auto_lifecycle(
        &conn,
        "sandbox-1",
        &access_idle_policy(),
        PolicyAuthority::Cluster,
    )
    .unwrap();
    let state = get_sandbox(&conn, "sandbox-1")
        .unwrap()
        .unwrap()
        .auto_lifecycle_state;
    conn.execute(
        "UPDATE sandboxes SET updated_at = '1000000000' WHERE id = 'sandbox-1'",
        [],
    )
    .unwrap();

    update_auto_lifecycle_state(&conn, "sandbox-1", &state).unwrap();
    assert_eq!(
        get_sandbox(&conn, "sandbox-1").unwrap().unwrap().updated_at,
        "1000000000",
        "re-writing the same state is not a transition"
    );

    // A deadline deferral parks the column away from the JSON's own value;
    // the next state write must still repair it, conditional or not.
    defer_auto_lifecycle_deadline(&conn, "sandbox-1", 4_000_000_000).unwrap();
    update_auto_lifecycle_state(&conn, "sandbox-1", &state).unwrap();
    let row = get_sandbox(&conn, "sandbox-1").unwrap().unwrap();
    assert_eq!(row.next_auto_event_at, state.next_auto_event_at);
    assert_ne!(row.updated_at, "1000000000");

    let mut moved = state.clone();
    moved.last_access_at = Some(1_700_000_000);
    conn.execute(
        "UPDATE sandboxes SET updated_at = '1000000000' WHERE id = 'sandbox-1'",
        [],
    )
    .unwrap();
    update_auto_lifecycle_state(&conn, "sandbox-1", &moved).unwrap();
    assert_ne!(
        get_sandbox(&conn, "sandbox-1").unwrap().unwrap().updated_at,
        "1000000000",
        "a state that actually changed must still stamp the row"
    );
}

#[test]
fn update_auto_lifecycle_resets_runtime_state_and_next_event() {
    let conn = test_conn();
    insert_test_sandbox(
        &conn,
        TestSandboxInsert::new("sandbox-1", "sandbox1", SandboxState::Creating),
    );

    update_auto_lifecycle(
        &conn,
        "sandbox-1",
        &access_idle_policy(),
        PolicyAuthority::Cluster,
    )
    .unwrap();

    let row = get_sandbox(&conn, "sandbox-1").unwrap().unwrap();
    assert!(
        row.auto_lifecycle_state
            .conditions
            .contains_key(&AutoLifecycleConditionSetId::AccessIdle)
    );
    assert!(row.next_auto_event_at.is_some());
}

#[test]
fn mark_quarantined_persists_current_state_and_history() {
    let conn = test_conn();
    insert_test_sandbox(
        &conn,
        TestSandboxInsert::new("sandbox-1", "sandbox1", SandboxState::Starting),
    );

    mark_starting_quarantined(
        &conn,
        "sandbox-1",
        SandboxQuarantineReasonCode::ColdActivationFirecrackerLaunchFailed,
        "firecracker launch and cleanup failed",
    )
    .unwrap();

    let row = get_sandbox(&conn, "sandbox-1").unwrap().unwrap();
    assert_eq!(row.state, SandboxState::Quarantined);
    assert_eq!(
        row.failure_reason.as_deref(),
        Some("firecracker launch and cleanup failed")
    );

    let current: (String, String, String, String, String, String) = conn
        .query_row(
            "SELECT quarantine_id, entered_from_state, source, reason_code,
                    reason_message, next_action
             FROM sandbox_quarantine_state
             WHERE sandbox_id = ?1",
            params!["sandbox-1"],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                ))
            },
        )
        .unwrap();
    assert_eq!(current.1, "starting");
    assert_eq!(current.2, SandboxQuarantineSource::SbxManager.as_str());
    assert_eq!(
        current.3,
        SandboxQuarantineReasonCode::ColdActivationFirecrackerLaunchFailed.as_str()
    );
    assert_eq!(current.4, "firecracker launch and cleanup failed");
    assert_eq!(
        current.5,
        SandboxQuarantineNextAction::RequiresOperatorAttention.as_str()
    );

    let history: (String, String, String, String, String) = conn
        .query_row(
            "SELECT quarantine_id, source, action, reason_code, next_action
             FROM sandbox_quarantine_history
             WHERE sandbox_id = ?1",
            params!["sandbox-1"],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                ))
            },
        )
        .unwrap();
    assert_eq!(history.0, current.0);
    assert_eq!(history.1, SandboxQuarantineSource::SbxManager.as_str());
    assert_eq!(
        history.2,
        SandboxQuarantineHistoryAction::EnteredQuarantine.as_str()
    );
    assert_eq!(
        history.3,
        SandboxQuarantineReasonCode::ColdActivationFirecrackerLaunchFailed.as_str()
    );
    assert_eq!(
        history.4,
        SandboxQuarantineNextAction::RequiresOperatorAttention.as_str()
    );
}

#[test]
fn quarantining_from_an_expected_state_leaves_an_already_quarantined_sandbox_alone() {
    let conn = test_conn();
    insert_test_sandbox(
        &conn,
        TestSandboxInsert::new("sandbox-1", "sandbox1", SandboxState::Pausing),
    );
    let quarantine_from_pausing = |reason_code, reason| {
        mark_quarantined_from_state(
            &conn,
            "sandbox-1",
            SandboxState::Pausing,
            SandboxQuarantineSource::SbxManager,
            reason_code,
            reason,
            SandboxQuarantineNextAction::RequiresOperatorAttention,
        )
        .unwrap()
    };
    assert!(quarantine_from_pausing(
        SandboxQuarantineReasonCode::PauseFailed,
        "pause outcome unknown",
    ));
    let before = database_rows(&conn);

    // A second failure on the same path, after the first one won.
    assert!(!quarantine_from_pausing(
        SandboxQuarantineReasonCode::RuntimeProbeInconsistent,
        "runtime probe inconsistent",
    ));
    assert_eq!(database_rows(&conn), before);
}

#[test]
fn activation_entry_rollbacks_and_quarantines_do_not_overwrite_newer_state() {
    let conn = test_conn();
    insert_test_sandbox(
        &conn,
        TestSandboxInsert::new("cold", "cold0001", SandboxState::Starting),
    );
    insert_test_sandbox(
        &conn,
        TestSandboxInsert::new("restore", "restore1", SandboxState::Restoring),
    );

    assert!(
        begin_start(&conn, "restore", SandboxState::Suspended)
            .unwrap()
            .is_none()
    );
    assert!(
        begin_restore(&conn, "cold", SandboxState::Suspended)
            .unwrap()
            .is_none()
    );
    assert_eq!(
        get_sandbox(&conn, "cold").unwrap().unwrap().state,
        SandboxState::Starting
    );
    assert_eq!(
        get_sandbox(&conn, "restore").unwrap().unwrap().state,
        SandboxState::Restoring
    );
    assert!(rollback_restoring(&conn, "restore", SandboxState::Suspended).unwrap());
    assert!(
        begin_restore(&conn, "restore", SandboxState::Suspended)
            .unwrap()
            .is_some()
    );
    assert_eq!(
        get_sandbox(&conn, "restore").unwrap().unwrap().state,
        SandboxState::Restoring
    );
    assert_eq!(
        get_sandbox(&conn, "restore")
            .unwrap()
            .unwrap()
            .restore_operation,
        Some(RestoreOperation::local_activation())
    );

    assert!(
        mark_restoring_quarantined(
            &conn,
            "restore",
            SandboxQuarantineReasonCode::RestorePrepareFailed,
            "concurrent quarantine",
        )
        .unwrap()
    );
    assert!(
        begin_restore(&conn, "restore", SandboxState::Suspended)
            .unwrap()
            .is_none()
    );

    assert!(rollback_starting(&conn, "cold", SandboxState::Created).unwrap());
    assert!(
        begin_start(&conn, "cold", SandboxState::Created)
            .unwrap()
            .is_some()
    );
    assert_eq!(
        get_sandbox(&conn, "cold").unwrap().unwrap().state,
        SandboxState::Starting
    );

    assert!(!rollback_restoring(&conn, "restore", SandboxState::Suspended).unwrap());
    assert!(
        !mark_restoring_quarantined(
            &conn,
            "restore",
            SandboxQuarantineReasonCode::RestoreSnapshotLoadFailed,
            "snapshot load and cleanup failed",
        )
        .unwrap()
    );
    assert_eq!(
        get_sandbox(&conn, "restore").unwrap().unwrap().state,
        SandboxState::Quarantined
    );
    assert!(
        get_sandbox(&conn, "restore")
            .unwrap()
            .unwrap()
            .restore_operation
            .is_none()
    );
}

#[test]
fn quarantine_cleanup_records_the_current_failure() {
    let conn = test_conn();
    insert_test_sandbox(
        &conn,
        TestSandboxInsert::new("failed", "failed01", SandboxState::Starting),
    );
    insert_cleanup_episode(
        &conn,
        "failed",
        CleanupEpisodeSpec {
            id: SandboxCleanupEpisodeId::new(),
            completion: CleanupCompletion::Quarantine,
            keep_filesystem: true,
            entered_from_state: Some(SandboxState::Created),
            activation_state: SandboxState::Starting,
            execution_generation: None,
            activation_attempt_id: None,
        },
    )
    .unwrap();
    let episode = cleanup_episode(&conn, "failed").unwrap().unwrap();
    assert_eq!(episode.completion, CleanupCompletion::Quarantine);
    assert_eq!(
        get_sandbox(&conn, "failed")
            .unwrap()
            .unwrap()
            .failure_reason,
        None,
        "an in-flight retry must not publish the previous failure"
    );
    assert_eq!(
        request_cleanup_episode(
            &conn,
            "failed",
            episode.id,
            SandboxQuarantineReasonCode::ColdActivationInterrupted,
            "retry failed",
        )
        .unwrap(),
        CleanupEpisodeRequestOutcome::Requested,
    );
    assert!(record_cleanup_episode_failure(&conn, "failed", episode.id, "cleanup failed").unwrap());
    assert!(
        complete_cleanup_episode(&conn, "failed", episode.id)
            .unwrap()
            .is_some()
    );

    let row = get_sandbox(&conn, "failed").unwrap().unwrap();
    assert_eq!(row.state, SandboxState::Quarantined);
    assert_eq!(episode.completion.target_state(), Some(row.state));
    assert_eq!(row.failure_reason.as_deref(), Some("retry failed"));
    assert!(cleanup_episode(&conn, "failed").unwrap().is_none());
    assert!(
        complete_cleanup_episode(&conn, "failed", episode.id)
            .unwrap()
            .is_none()
    );
    assert!(
        scan_quarantine_recovery_candidates(&conn)
            .unwrap()
            .candidates
            .is_empty()
    );
}

#[test]
fn repeated_cleanup_request_preserves_failure_and_missing_artifact_evidence() {
    let conn = test_conn();
    insert_test_sandbox(
        &conn,
        TestSandboxInsert::new("suspended", "suspend1", SandboxState::Suspended),
    );

    let episode = begin_start(&conn, "suspended", SandboxState::Suspended)
        .unwrap()
        .expect("suspended sandbox should enter Starting");
    assert_eq!(
        request_cleanup_episode(
            &conn,
            "suspended",
            episode.id,
            SandboxQuarantineReasonCode::ColdActivationInterrupted,
            "inner start failed",
        )
        .unwrap(),
        CleanupEpisodeRequestOutcome::Requested,
    );
    complete_row_intent_cleanup(&conn, "suspended", CleanupCompletion::MarkSuspended, None)
        .unwrap();
    assert_eq!(
        get_sandbox(&conn, "suspended").unwrap().unwrap().state,
        SandboxState::CleanupRequired,
        "a legacy completion must not finalize a requested cleanup episode"
    );
    assert_eq!(
        get_sandbox(&conn, "suspended").unwrap().unwrap().state,
        SandboxState::CleanupRequired,
        "a requested cleanup episode must retain lifecycle ownership"
    );
    assert!(
        record_cleanup_episode_failure(&conn, "suspended", episode.id, "cleanup failed").unwrap()
    );
    assert_eq!(
        request_cleanup_episode(
            &conn,
            "suspended",
            episode.id,
            SandboxQuarantineReasonCode::CreateInterrupted,
            "outer create observed the same failure",
        )
        .unwrap(),
        CleanupEpisodeRequestOutcome::AlreadyRequested,
    );

    let row = get_sandbox(&conn, "suspended").unwrap().unwrap();
    assert_eq!(row.state, SandboxState::CleanupFailed);
    assert_eq!(row.failure_reason.as_deref(), Some("inner start failed"));
    assert_eq!(
        cleanup_episode(&conn, "suspended")
            .unwrap()
            .unwrap()
            .last_cleanup_error
            .as_deref(),
        Some("cleanup failed")
    );
    assert!(matches!(
        complete_cleanup_episode(&conn, "suspended", episode.id),
        Err(CompleteCleanupEpisodeError::MissingSuspendArtifact)
    ));
}

#[test]
fn a_failed_restore_recovers_only_if_the_two_artifact_stores_diverge() {
    // The one state in which a failed restore is recoverable, and the reason to
    // doubt that it occurs.
    //
    // The three restore reasons are written only by the cleanup that compensates
    // a failed restore, which quarantines exactly when
    // `sandbox_has_registered_full_suspend_artifact` is false. That is false three
    // ways: no artifact named, an artifact named with the wrong kind/origin, or —
    // the case here — an artifact named on the sandbox row with no matching
    // `local_artifacts` row. Only the third leaves the scan an artifact identity
    // to admit, so only the third is selectable.
    //
    // No writer produces that divergence. Re-registration deletes and inserts in
    // one transaction; tearing down a sandbox removes both; the orphan prune fires
    // only when the sandbox row is already gone; and the artifact GC excludes
    // pinned ids when planning and re-checks the pin before executing. So this
    // test does not model a path the product walks — it pins what the loop does if
    // the two stores ever diverge anyway, which is the only way those three reason
    // codes' recovery policy can ever be exercised.
    //
    // Whether that makes the policy dead is a question for whoever owns the
    // reason-code table, not something to answer by loosening a predicate here.
    let conn = crate::db::test_conn();
    let sandbox_id = SandboxId::new().to_string();
    insert_test_sandbox(
        &conn,
        TestSandboxInsert::new(&sandbox_id, "reaped01", SandboxState::Suspended),
    );
    // Named, but deliberately never registered.
    let local_artifact_id = LocalArtifactId::new();
    conn.execute(
        "UPDATE sandboxes
            SET current_local_artifact_id = ?1,
                current_local_artifact_kind = 'full',
                current_local_artifact_origin = 'suspend'
          WHERE id = ?2",
        rusqlite::params![local_artifact_id.to_string(), &sandbox_id],
    )
    .unwrap();

    let episode = begin_start(&conn, &sandbox_id, SandboxState::Suspended)
        .unwrap()
        .expect("a suspended sandbox should enter Starting");
    assert_eq!(
        request_cleanup_episode(
            &conn,
            &sandbox_id,
            episode.id,
            SandboxQuarantineReasonCode::RestoreSpacedHostLaunchFailed,
            "spaced-host launch failed during restore",
        )
        .unwrap(),
        CleanupEpisodeRequestOutcome::Requested,
    );
    complete_row_intent_cleanup(&conn, &sandbox_id, CleanupCompletion::MarkSuspended, None)
        .unwrap();
    assert!(matches!(
        complete_cleanup_episode(&conn, &sandbox_id, episode.id),
        Err(CompleteCleanupEpisodeError::MissingSuspendArtifact)
    ));
    assert!(
        quarantine_cleanup_episode(&conn, &sandbox_id, episode.id, "artifact row reaped").unwrap()
    );

    let scan = scan_quarantine_recovery_candidates(&conn).unwrap();
    let candidate = scan
        .candidates
        .iter()
        .find(|candidate| candidate.sandbox_id.to_string() == sandbox_id)
        .expect(
            "an episode whose sandbox row still names an artifact is admissible \
             regardless of the registry, because the scan reads only the sandbox \
             row; this is the sole shape in which these reasons reach the loop",
        );
    assert_eq!(
        candidate.reason_code,
        SandboxQuarantineReasonCode::RestoreSpacedHostLaunchFailed,
    );
    let QuarantineRecoveryBranch::AdoptLocalArtifact(adoptable) = candidate.recovery else {
        panic!("an episode naming a full suspend artifact takes the adoption branch");
    };
    assert_eq!(
        adoptable.local_artifact_id(),
        local_artifact_id,
        "and the witness carries the artifact the gate validated"
    );
    assert_eq!(scan.pending_count, 1);
    assert_quarantine_invariants(&conn, "after a failed restore with a reaped artifact row");
}

#[test]
fn a_failed_restore_naming_no_artifact_is_correctly_left_for_an_operator() {
    // The counterpart to the reaped-row case above, and the reason the scan's
    // no-pin branch names one reason instead of testing the policy.
    //
    // Here the sandbox names no artifact at all. The cleanup still refuses and
    // still quarantines with the restore's reason, but there is nothing for a
    // central plan to be matched against: the policy for these reasons is an
    // exact suspend artifact, and no identity exists to be exact about. So the
    // episode is deliberately not selected, and it waits for an operator rather
    // than being retried forever against a plan that cannot resolve.
    //
    // Only `orphaned_sandbox_row_missing` is admissible without a named
    // artifact, because only its policy may fall back to the newest
    // pre-quarantine snapshot.
    let conn = crate::db::test_conn();
    // A real UUID: the candidate scan counts a non-parsing id as malformed and
    // skips it, so a placeholder id would make this pass for the wrong reason.
    let sandbox_id = SandboxId::new().to_string();
    insert_test_sandbox(
        &conn,
        TestSandboxInsert::new(&sandbox_id, "restore1", SandboxState::Suspended),
    );

    let episode = begin_start(&conn, &sandbox_id, SandboxState::Suspended)
        .unwrap()
        .expect("a suspended sandbox should enter Starting");
    assert_eq!(
        request_cleanup_episode(
            &conn,
            &sandbox_id,
            episode.id,
            SandboxQuarantineReasonCode::RestoreSpacedHostLaunchFailed,
            "spaced-host launch failed during restore",
        )
        .unwrap(),
        CleanupEpisodeRequestOutcome::Requested,
    );
    complete_row_intent_cleanup(&conn, &sandbox_id, CleanupCompletion::MarkSuspended, None)
        .unwrap();

    assert!(
        matches!(
            complete_cleanup_episode(&conn, &sandbox_id, episode.id),
            Err(CompleteCleanupEpisodeError::MissingSuspendArtifact)
        ),
        "the compensating cleanup must refuse to mark a sandbox Suspended when no \
         full suspend artifact is registered; that refusal is what raises the \
         quarantine at all"
    );
    assert!(
        quarantine_cleanup_episode(&conn, &sandbox_id, episode.id, "missing suspend artifact")
            .unwrap(),
    );
    let row = get_sandbox(&conn, &sandbox_id).unwrap().unwrap();
    assert_eq!(row.state, SandboxState::Quarantined);
    assert!(
        row.current_local_artifact_id.is_none(),
        "this case is the one where nothing is named, so the eligibility \
         predicate's no-artifact branch applies"
    );

    let scan = scan_quarantine_recovery_candidates(&conn).unwrap();
    assert_eq!(
        scan.malformed_count, 0,
        "the row is well formed; it is excluded on eligibility, not on validation"
    );
    assert_eq!(
        scan.pending_count, 0,
        "nor counted as pending: the backlog gauge should not show work that no \
         pass could ever make progress on"
    );
    assert!(
        !scan
            .candidates
            .iter()
            .any(|candidate| candidate.sandbox_id.to_string() == sandbox_id),
        "an episode naming no artifact must not be selected: its reason's policy \
         demands an exact suspend artifact and no identity exists to match"
    );
    assert_quarantine_invariants(&conn, "after a failed restore with no pinned artifact");
}

/// The adoption witness a claimed candidate carries.
///
/// Panics on the restore branch, which is correct for both callers: the adoption
/// statement's fixtures are on the adoption branch by construction, and a fixture
/// that drifted off it should fail loudly rather than be handed a substitute id.
fn adoption_witness(claimed: &ClaimedQuarantineRecoveryCandidate) -> AdoptableArtifact {
    match claimed.candidate.recovery {
        QuarantineRecoveryBranch::AdoptLocalArtifact(adoptable) => adoptable,
        QuarantineRecoveryBranch::RestoreOrphanedStaging => {
            panic!("this statement's fixture must be on the adoption branch")
        }
    }
}

/// An orphan restore's own artifact publication cannot flip its episode onto the
/// permit-free adoption branch.
///
/// The node-wide restore permit is the fence that stops a fresh claimant from
/// starting concurrent transfer and filesystem work against the same staging tree
/// after a lease is lost. That fence only holds while the episode keeps taking the
/// restore branch, and the restore is the thing that eventually writes
/// `sandboxes.current_local_artifact_id` — the column the eligibility gate reads.
/// So this pins both halves: mid-restore the episode still admits the restore
/// branch, and the terminal publication takes the sandbox out of the scanned cohort
/// entirely rather than leaving an artifact-bearing `quarantined` row behind for the
/// adoption branch to pick up without a permit.
#[test]
fn a_publishing_orphan_restore_never_becomes_an_adoption_candidate() {
    let conn = test_conn();
    let sandbox_id = setup_quarantine_recovery_episode(
        &conn,
        SandboxQuarantineReasonCode::OrphanedSandboxRowMissing,
    );
    let [candidate] = scan_quarantine_recovery_candidates(&conn)
        .unwrap()
        .candidates
        .try_into()
        .unwrap();
    assert_eq!(
        candidate.recovery,
        QuarantineRecoveryBranch::RestoreOrphanedStaging,
        "the seeded orphan must take the branch that reserves node-wide capacity"
    );
    let claimed =
        claim_quarantine_recovery_candidate(&conn, &candidate, PlanRejectionRunEffect::Leaves)
            .unwrap()
            .unwrap();

    // What the restore's phase-1 row adoption leaves behind: `restoring`, still with
    // no artifact of its own.
    conn.execute(
        "UPDATE sandboxes SET state = 'restoring' WHERE id = ?1",
        params![sandbox_id.to_string()],
    )
    .unwrap();
    let execution_generation = ExecutionGeneration::new(7).unwrap();
    assert!(
        set_orphan_quarantine_recovery_generation(&conn, &claimed, execution_generation).unwrap()
    );

    // Mid-restore, with the lease dropped so the episode is due again: the shape a
    // superseding pass would see. It must still be the restore branch, or the permit
    // stops fencing the live task.
    let live_expiry = conn
        .query_row(
            "SELECT claim_expires_at FROM sandbox_quarantine_state WHERE sandbox_id = ?1",
            params![sandbox_id.to_string()],
            |row| row.get::<_, String>(0),
        )
        .unwrap();
    conn.execute(
        "UPDATE sandbox_quarantine_state
            SET claim_expires_at = '0', next_action_after = '0'
          WHERE sandbox_id = ?1",
        params![sandbox_id.to_string()],
    )
    .unwrap();
    let [redispatched] = scan_quarantine_recovery_candidates(&conn)
        .unwrap()
        .candidates
        .try_into()
        .unwrap();
    assert_eq!(
        redispatched.recovery,
        QuarantineRecoveryBranch::RestoreOrphanedStaging,
        "a lease lost mid-restore must re-enter on the branch that takes the permit"
    );
    conn.execute(
        "UPDATE sandbox_quarantine_state SET claim_expires_at = ?1 WHERE sandbox_id = ?2",
        params![live_expiry, sandbox_id.to_string()],
    )
    .unwrap();

    assert!(
        publish_orphan_quarantine_recovered_artifact(
            &conn,
            &claimed,
            Some(LocalArtifactId::new()),
            "/space/cache/sandboxes/test/snapshot",
            execution_generation,
            SandboxCleanupEpisodeId::new(),
        )
        .unwrap()
    );
    // Assert the two durable facts the publication establishes, not just the
    // emptiness they produce. The scan joins `sandboxes` to
    // `sandbox_quarantine_state`, so the deleted quarantine row alone empties the
    // work list — asserting only the scan would hold even if the state move
    // regressed, and would credit the state filter for the deletion's work.
    let quarantine_rows: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM sandbox_quarantine_state WHERE sandbox_id = ?1",
            params![sandbox_id.to_string()],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(
        quarantine_rows, 0,
        "the publication retires the quarantine row; a surviving one keeps the \
         sandbox in the cohort the scan walks"
    );
    let state: String = conn
        .query_row(
            "SELECT state FROM sandboxes WHERE id = ?1",
            params![sandbox_id.to_string()],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(
        state, "suspended",
        "the publication is terminal; a sandbox left in a cohort state would be \
         re-offered to recovery by the state filter as soon as a quarantine row \
         reappeared"
    );
    let scan = scan_quarantine_recovery_candidates(&conn).unwrap();
    assert_eq!(
        scan.pending_count, 0,
        "an artifact-bearing row left in the cohort is what would offer the \
         adoption branch a permit-free path over a live restore"
    );
    assert!(scan.candidates.is_empty());
}

#[test]
fn quarantine_recovery_no_progress_remains_retryable_at_a_capped_rate() {
    let conn = crate::db::test_conn();
    let (sandbox_id, _) = setup_quarantine_recovery_candidate(&conn);

    // A lookup that never answers is not a plan rejection, so it never stops
    // recovery; `quarantine_recovery_stops_only_after_an_unbroken_run_of_plan_rejections`
    // owns the bound.
    for expected_attempt in 1..=20 {
        let [candidate] = scan_quarantine_recovery_candidates(&conn)
            .unwrap()
            .candidates
            .try_into()
            .unwrap();
        let claimed =
            claim_quarantine_recovery_candidate(&conn, &candidate, PlanRejectionRunEffect::Leaves)
                .unwrap()
                .expect("due episode should be claimable");
        let outcome = record_quarantine_recovery_no_progress(
            &conn,
            &claimed,
            QuarantineRecoveryNoProgressReason::PlanLookupTimedOut,
        )
        .unwrap();
        let QuarantineRecoveryNoProgressOutcome::RetryScheduled {
            attempt_count,
            next_attempt_at,
        } = outcome
        else {
            panic!("current claim unexpectedly became stale");
        };
        assert_eq!(attempt_count, expected_attempt);
        let now = now_epoch_i64().unwrap();
        assert!(next_attempt_at > now);
        assert!(
            u64::try_from(next_attempt_at.saturating_sub(now)).unwrap()
                <= QUARANTINE_RECOVERY_BACKOFF_MAX.as_secs()
        );
        assert!(
            scan_quarantine_recovery_candidates(&conn)
                .unwrap()
                .candidates
                .is_empty()
        );
        conn.execute(
            "UPDATE sandbox_quarantine_state
                SET next_action_after = NULL
              WHERE sandbox_id = ?1",
            params![sandbox_id.to_string()],
        )
        .unwrap();
    }

    let (owner, attempt_count, claim_token): (String, i64, Option<String>) = conn
        .query_row(
            "SELECT next_action_owner, attempt_count, claim_token
               FROM sandbox_quarantine_state
              WHERE sandbox_id = ?1",
            params![sandbox_id.to_string()],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    assert_eq!(owner, QuarantineRecoveryOwner::LegacyRecovery.as_str());
    assert_eq!(attempt_count, 20);
    assert_eq!(claim_token, None);
    let scan = scan_quarantine_recovery_candidates(&conn).unwrap();
    assert_eq!((scan.pending_count, scan.malformed_count), (1, 0));
    assert_eq!(
        scan_quarantine_recovery_candidates(&conn)
            .unwrap()
            .candidates
            .len(),
        1
    );
}

/// The no-progress retry schedule is the recovery contract: 60 s, doubling per
/// attempt, capped at 15 min, less at most a fifth of the delay as jitter. A
/// halved base or a flat schedule leaves this window. The clock is the real one,
/// so each retry is bracketed by reads taken either side of its write, and
/// clearing the retry makes the next attempt due without waiting for it.
#[test]
fn quarantine_recovery_no_progress_schedules_the_capped_doubling_backoff() {
    let conn = crate::db::test_conn();
    let (sandbox_id, _) = setup_quarantine_recovery_candidate(&conn);

    for (attempt, full_delay) in [(1, 60), (2, 120), (3, 240), (4, 480), (5, 900), (6, 900)] {
        let [candidate] = scan_quarantine_recovery_candidates(&conn)
            .unwrap()
            .candidates
            .try_into()
            .unwrap();
        let claimed =
            claim_quarantine_recovery_candidate(&conn, &candidate, PlanRejectionRunEffect::Leaves)
                .unwrap()
                .expect("due episode should be claimable");
        let before = now_epoch_i64().unwrap();
        let outcome = record_quarantine_recovery_no_progress(
            &conn,
            &claimed,
            QuarantineRecoveryNoProgressReason::PlanLookupTimedOut,
        )
        .unwrap();
        let after = now_epoch_i64().unwrap();
        let QuarantineRecoveryNoProgressOutcome::RetryScheduled {
            attempt_count,
            next_attempt_at,
        } = outcome
        else {
            panic!("attempt {attempt}: the claim unexpectedly became stale");
        };
        assert_eq!(attempt_count, attempt);
        let (_, _, stored, _, _) = quarantine_claim_state(&conn, sandbox_id);
        assert_eq!(
            stored,
            Some(next_attempt_at.to_string()),
            "attempt {attempt}: the stored retry is the one reported"
        );
        let min_delay = full_delay - full_delay / 5;
        assert!(
            next_attempt_at - before >= min_delay && next_attempt_at - after <= full_delay,
            "attempt {attempt}: the retry must land {min_delay}..={full_delay}s after the \
             write (written between {before} and {after}, scheduled {next_attempt_at})"
        );

        conn.execute(
            "UPDATE sandbox_quarantine_state
                SET next_action_after = NULL
              WHERE sandbox_id = ?1",
            params![sandbox_id.to_string()],
        )
        .unwrap();
    }
}

/// This test's own classification of every no-progress reason, with a distinct
/// index per variant so `ALL_NO_PROGRESS_REASONS` is checked for completeness.
/// Wildcard-free: a new reason does not compile until it is placed here.
fn expected_run_effect(
    reason: QuarantineRecoveryNoProgressReason,
) -> (usize, PlanRejectionRunEffect) {
    use PlanRejectionRunEffect::{Ends, Extends, Leaves};
    use QuarantineRecoveryNoProgressReason as Reason;
    match reason {
        Reason::PlanNotFound => (0, Extends),
        Reason::PlanIneligible => (1, Extends),
        Reason::PlanRpcUnsupported => (2, Ends),
        Reason::PlanMalformed => (3, Ends),
        Reason::UnsupportedTargetState => (4, Ends),
        Reason::PlanReasonIneligible => (5, Ends),
        Reason::PlanLookupFailed => (6, Leaves),
        Reason::PlanLookupTimedOut => (7, Leaves),
        Reason::LocalReasonMismatch => (8, Ends),
        Reason::LocalArtifactMismatch => (9, Ends),
        Reason::PlanArtifactMismatch => (10, Ends),
        Reason::ClaimRenewalFailed => (11, Ends),
        Reason::NodeDatabaseFailure => (12, Ends),
        Reason::NodeFilesystemFailure => (13, Ends),
        Reason::RestoreEvidenceMismatch => (14, Ends),
        Reason::ControlPlaneRefused => (15, Ends),
        Reason::CleanupFenceBusy => (16, Ends),
        Reason::LocalArtifactInspectionFailed => (17, Ends),
        Reason::LocalAdoptionFenceChanged => (18, Ends),
        Reason::LocalAdoptionFailed => (19, Ends),
        Reason::OrphanRestoreLocalContention => (20, Ends),
        Reason::OrphanRestoreTaskFailed => (21, Ends),
        Reason::OrphanRestoreDeferred => (22, Ends),
        Reason::OrphanRestoreTimedOut => (23, Ends),
        Reason::ClaimSuperseded => (24, Ends),
        Reason::ClaimPreconditionsChanged => (25, Ends),
        Reason::ClaimAcquisitionFailed => (26, Ends),
        Reason::AbandonedAtShutdown => (27, Ends),
    }
}

const ALL_NO_PROGRESS_REASONS: [QuarantineRecoveryNoProgressReason; 28] = {
    use QuarantineRecoveryNoProgressReason as Reason;
    [
        Reason::PlanNotFound,
        Reason::PlanIneligible,
        Reason::PlanRpcUnsupported,
        Reason::PlanMalformed,
        Reason::UnsupportedTargetState,
        Reason::PlanReasonIneligible,
        Reason::PlanLookupFailed,
        Reason::PlanLookupTimedOut,
        Reason::LocalReasonMismatch,
        Reason::LocalArtifactMismatch,
        Reason::PlanArtifactMismatch,
        Reason::ClaimRenewalFailed,
        Reason::NodeDatabaseFailure,
        Reason::NodeFilesystemFailure,
        Reason::RestoreEvidenceMismatch,
        Reason::ControlPlaneRefused,
        Reason::CleanupFenceBusy,
        Reason::LocalArtifactInspectionFailed,
        Reason::LocalAdoptionFenceChanged,
        Reason::LocalAdoptionFailed,
        Reason::OrphanRestoreLocalContention,
        Reason::OrphanRestoreTaskFailed,
        Reason::OrphanRestoreDeferred,
        Reason::OrphanRestoreTimedOut,
        Reason::ClaimSuperseded,
        Reason::ClaimPreconditionsChanged,
        Reason::ClaimAcquisitionFailed,
        Reason::AbandonedAtShutdown,
    ]
};

fn record_rejections(conn: &Connection, sandbox_id: SandboxId, count: u32) {
    for run in 1..=count {
        let reason = if run % 2 == 0 {
            QuarantineRecoveryNoProgressReason::PlanNotFound
        } else {
            QuarantineRecoveryNoProgressReason::PlanIneligible
        };
        assert!(
            matches!(
                record_quarantine_recovery_attempt(conn, sandbox_id, reason),
                QuarantineRecoveryNoProgressOutcome::RetryScheduled { .. }
            ),
            "rejection {run} of {count} must still retry"
        );
    }
}

#[test]
fn every_no_progress_reason_extends_leaves_or_ends_the_plan_rejection_run() {
    let mut indices = ALL_NO_PROGRESS_REASONS
        .map(|reason| expected_run_effect(reason).0)
        .to_vec();
    indices.sort_unstable();
    assert_eq!(
        indices,
        (0..ALL_NO_PROGRESS_REASONS.len()).collect::<Vec<_>>()
    );

    let below_bound = QUARANTINE_RECOVERY_PLAN_REJECTION_BOUND - 1;
    for reason in ALL_NO_PROGRESS_REASONS {
        let effect = expected_run_effect(reason).1;
        assert_eq!(PlanRejectionRunEffect::of(reason), effect, "{reason}");
        let conn = crate::db::test_conn();
        let (sandbox_id, _) = setup_quarantine_recovery_candidate(&conn);
        record_rejections(&conn, sandbox_id, below_bound);
        let outcome = record_quarantine_recovery_attempt(&conn, sandbox_id, reason);
        match effect {
            PlanRejectionRunEffect::Extends => assert!(
                matches!(
                    outcome,
                    QuarantineRecoveryNoProgressOutcome::AutomaticRecoveryStopped {
                        consecutive_plan_rejections: 16,
                        ..
                    }
                ),
                "{reason} completes the run: {outcome:?}"
            ),
            // Neither extends the run nor restarts it: one more rejection stops.
            PlanRejectionRunEffect::Leaves => {
                assert!(
                    matches!(
                        outcome,
                        QuarantineRecoveryNoProgressOutcome::RetryScheduled { .. }
                    ),
                    "{reason}: {outcome:?}"
                );
                assert!(
                    matches!(
                        record_quarantine_recovery_attempt(
                            &conn,
                            sandbox_id,
                            QuarantineRecoveryNoProgressReason::PlanIneligible
                        ),
                        QuarantineRecoveryNoProgressOutcome::AutomaticRecoveryStopped { .. }
                    ),
                    "{reason} must leave the run at {below_bound}"
                );
            }
            // A full run below the bound after it still retries.
            PlanRejectionRunEffect::Ends => {
                assert!(
                    matches!(
                        outcome,
                        QuarantineRecoveryNoProgressOutcome::RetryScheduled { .. }
                    ),
                    "{reason}: {outcome:?}"
                );
                record_rejections(&conn, sandbox_id, below_bound);
            }
        }
    }
}

#[test]
fn quarantine_recovery_stops_only_after_an_unbroken_run_of_plan_rejections() {
    let conn = crate::db::test_conn();
    let (sandbox_id, _) = setup_quarantine_recovery_candidate(&conn);
    record_rejections(
        &conn,
        sandbox_id,
        QUARANTINE_RECOVERY_PLAN_REJECTION_BOUND - 1,
    );
    conn.execute(
        "UPDATE sandbox_quarantine_state SET next_action_after = NULL WHERE sandbox_id = ?1",
        params![sandbox_id.to_string()],
    )
    .unwrap();
    let [last_candidate] = scan_quarantine_recovery_candidates(&conn)
        .unwrap()
        .candidates
        .try_into()
        .unwrap();
    let claimed = claim_quarantine_recovery_candidate(
        &conn,
        &last_candidate,
        PlanRejectionRunEffect::Extends,
    )
    .unwrap()
    .unwrap();
    assert_eq!(
        record_quarantine_recovery_no_progress(
            &conn,
            &claimed,
            QuarantineRecoveryNoProgressReason::PlanNotFound
        )
        .unwrap(),
        QuarantineRecoveryNoProgressOutcome::AutomaticRecoveryStopped {
            attempt_count: QUARANTINE_RECOVERY_PLAN_REJECTION_BOUND,
            consecutive_plan_rejections: QUARANTINE_RECOVERY_PLAN_REJECTION_BOUND,
        }
    );

    let (owner, attempt_count, next_action_after, claim_token): (
        String,
        u32,
        Option<String>,
        Option<String>,
    ) = conn
        .query_row(
            "SELECT next_action_owner, attempt_count, next_action_after, claim_token
               FROM sandbox_quarantine_state
              WHERE sandbox_id = ?1",
            params![sandbox_id.to_string()],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .unwrap();
    assert_eq!(owner, QuarantineRecoveryOwner::LegacyRecovery.as_str());
    assert_eq!(attempt_count, QUARANTINE_RECOVERY_PLAN_REJECTION_BOUND);
    assert_eq!((next_action_after, claim_token), (None, None));
    assert_eq!(
        get_sandbox(&conn, &sandbox_id.to_string())
            .unwrap()
            .map(|row| row.state),
        Some(SandboxState::Quarantined),
        "stopping recovery keeps the hold"
    );
    let scan = scan_quarantine_recovery_candidates(&conn).unwrap();
    assert!(scan.candidates.is_empty());
    assert_eq!(
        (scan.pending_count, scan.malformed_count),
        (1, 0),
        "a stopped hold stays in the pending census"
    );
    // The claim is the authority, not the scan: a candidate read before the
    // stop, with the episode's current attempt count, still cannot claim it.
    let stale = QuarantineRecoveryCandidate {
        attempt_count: QUARANTINE_RECOVERY_PLAN_REJECTION_BOUND,
        ..last_candidate
    };
    assert_eq!(
        claim_quarantine_recovery_candidate(&conn, &stale, PlanRejectionRunEffect::Ends).unwrap(),
        None
    );
}

/// Stored `last_error` values the run parser must tolerate: legacy text from
/// older binaries, operator edits and corruption in the node-local SQLite.
#[test]
fn stored_last_error_values_decide_admission_and_the_next_run() {
    use rusqlite::types::Value;
    let stopped = "plan_ineligible;consecutive_plan_rejections=16";
    // (stored value, admitted, last_error after one more plan_ineligible)
    let cases: [(Value, bool, Option<&str>); 10] = [
        (Value::Null, true, Some("plan_ineligible")),
        (Value::Text(String::new()), true, Some("plan_ineligible")),
        (
            Value::Text("plan_ineligible;consecutive_plan_rejections=abc".into()),
            true,
            Some("plan_ineligible"),
        ),
        (
            Value::Text("plan_ineligible;consecutive_plan_rejections=99999999999999999999".into()),
            true,
            Some("plan_ineligible"),
        ),
        (
            Value::Text("plan_lookup_failed;consecutive_plan_rejections=16".into()),
            true,
            Some("plan_ineligible"),
        ),
        (
            Value::Text("still investigating".into()),
            true,
            Some("plan_ineligible"),
        ),
        // Only text carries a run: the column has TEXT affinity, so a blob is
        // the one storage class that survives as non-text.
        (
            Value::Blob(b"plan_ineligible;consecutive_plan_rejections=16".to_vec()),
            true,
            Some("plan_ineligible"),
        ),
        // A bare reason from an older binary is a run of one.
        (
            Value::Text("plan_not_found".into()),
            true,
            Some("plan_ineligible;consecutive_plan_rejections=2"),
        ),
        (
            Value::Text("plan_not_found;consecutive_plan_rejections=15".into()),
            true,
            Some(stopped),
        ),
        (Value::Text(stopped.into()), false, None),
    ];
    for (stored, admitted, after) in cases {
        let conn = crate::db::test_conn();
        // The candidate is read before the value is stored, so the claim is
        // tested on its own and not only through the scan.
        let (sandbox_id, candidate) = setup_quarantine_recovery_candidate(&conn);
        conn.execute(
            "UPDATE sandbox_quarantine_state SET last_error = ?1 WHERE sandbox_id = ?2",
            params![stored, sandbox_id.to_string()],
        )
        .unwrap();
        let scan = scan_quarantine_recovery_candidates(&conn).unwrap();
        assert_eq!(scan.candidates.len(), usize::from(admitted), "{stored:?}");
        assert_eq!(
            (scan.pending_count, scan.malformed_count),
            (1, 0),
            "{stored:?}"
        );
        let claimed =
            claim_quarantine_recovery_candidate(&conn, &candidate, PlanRejectionRunEffect::Extends)
                .unwrap();
        assert_eq!(claimed.is_some(), admitted, "{stored:?}");
        let Some(claimed) = claimed else {
            continue;
        };
        record_quarantine_recovery_no_progress(
            &conn,
            &claimed,
            QuarantineRecoveryNoProgressReason::PlanIneligible,
        )
        .unwrap();
        let last_error: Option<String> = conn
            .query_row(
                "SELECT last_error FROM sandbox_quarantine_state WHERE sandbox_id = ?1",
                params![sandbox_id.to_string()],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(last_error.as_deref(), after, "{stored:?}");
    }
}

#[test]
fn quarantine_recovery_scan_excludes_cleanup_owned_episodes() {
    let conn = crate::db::test_conn();
    let (sandbox_id, _) = setup_quarantine_recovery_candidate(&conn);
    assert_eq!(
        scan_quarantine_recovery_candidates(&conn)
            .unwrap()
            .pending_count,
        1
    );

    conn.execute(
        "INSERT INTO sandbox_cleanup_episodes (
             sandbox_id, cleanup_id, completion, keep_filesystem, phase,
             activation_state, created_at
         ) VALUES (?1, ?2, 'mark_suspended', TRUE, 'armed', 'restoring', 0)",
        params![
            sandbox_id.to_string(),
            SandboxCleanupEpisodeId::new().to_string()
        ],
    )
    .unwrap();

    let scan = scan_quarantine_recovery_candidates(&conn).unwrap();
    assert!(scan.candidates.is_empty());
    assert_eq!(
        (
            scan.pending_count,
            scan.pending_oldest_age.as_secs(),
            scan.malformed_count
        ),
        (0, 0, 0)
    );
}

#[test]
fn quarantine_recovery_dependency_failure_uses_the_same_backoff() {
    let conn = crate::db::test_conn();
    let (sandbox_id, candidate) = setup_quarantine_recovery_candidate(&conn);
    let claimed =
        claim_quarantine_recovery_candidate(&conn, &candidate, PlanRejectionRunEffect::Leaves)
            .unwrap()
            .expect("due episode should be claimable");

    let outcome = record_quarantine_recovery_no_progress(
        &conn,
        &claimed,
        QuarantineRecoveryNoProgressReason::PlanRpcUnsupported,
    )
    .unwrap();
    assert!(matches!(
        outcome,
        QuarantineRecoveryNoProgressOutcome::RetryScheduled {
            attempt_count: 1,
            ..
        }
    ));

    let (attempt_count, owner, next_action_after, claim_token, last_error): (
        i64,
        Option<String>,
        Option<String>,
        Option<String>,
        Option<String>,
    ) = conn
        .query_row(
            "SELECT attempt_count, next_action_owner, next_action_after,
                    claim_token, last_error
               FROM sandbox_quarantine_state
              WHERE sandbox_id = ?1",
            params![sandbox_id.to_string()],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                ))
            },
        )
        .unwrap();
    assert_eq!(attempt_count, 1);
    assert_eq!(
        owner.as_deref(),
        Some(QuarantineRecoveryOwner::LegacyRecovery.as_str())
    );
    assert!(next_action_after.is_some());
    assert_eq!(claim_token, None);
    assert_eq!(
        last_error.as_deref(),
        Some(QuarantineRecoveryNoProgressReason::PlanRpcUnsupported.as_str())
    );
    assert!(
        scan_quarantine_recovery_candidates(&conn)
            .unwrap()
            .candidates
            .is_empty(),
        "dependency failure must honor its retry delay"
    );

    conn.execute(
        "UPDATE sandbox_quarantine_state SET next_action_after = NULL WHERE sandbox_id = ?1",
        params![sandbox_id.to_string()],
    )
    .unwrap();
    let [candidate] = scan_quarantine_recovery_candidates(&conn)
        .unwrap()
        .candidates
        .try_into()
        .unwrap();
    assert!(
        claim_quarantine_recovery_candidate(&conn, &candidate, PlanRejectionRunEffect::Leaves)
            .unwrap()
            .is_some(),
        "the exact episode remains automatically retryable"
    );
}
#[test]
fn quarantine_recovery_observation_reads_control_state_for_inventory() {
    let conn = crate::db::test_conn();
    let (sandbox_id, candidate) = setup_quarantine_recovery_candidate(&conn);
    let claimed =
        claim_quarantine_recovery_candidate(&conn, &candidate, PlanRejectionRunEffect::Leaves)
            .unwrap()
            .unwrap();
    let outcome = record_quarantine_recovery_no_progress(
        &conn,
        &claimed,
        QuarantineRecoveryNoProgressReason::PlanIneligible,
    )
    .unwrap();
    let QuarantineRecoveryNoProgressOutcome::RetryScheduled {
        attempt_count,
        next_attempt_at,
    } = outcome
    else {
        panic!("first no-progress attempt must schedule a retry");
    };

    let raw = quarantine_recovery_observations(&conn, &[sandbox_id.to_string()])
        .unwrap()
        .remove(&sandbox_id.to_string())
        .unwrap()
        .unwrap();
    assert_eq!(
        raw.local_quarantine_id,
        candidate.local_quarantine_id.to_string()
    );
    assert_eq!(
        raw.owner.as_deref(),
        Some(QuarantineRecoveryOwner::LegacyRecovery.as_str())
    );
    assert_eq!(raw.attempt_count, i64::from(attempt_count));
    assert_eq!(
        raw.next_attempt_at.as_deref(),
        Some(next_attempt_at.to_string().as_str())
    );
}

#[test]
fn quarantine_recovery_observations_exclude_rows_outside_automatic_recovery() {
    let conn = crate::db::test_conn();
    let recoverable = setup_quarantine_recovery_episode(
        &conn,
        SandboxQuarantineReasonCode::RestoreFirecrackerLaunchFailed,
    );
    let non_recoverable =
        setup_quarantine_recovery_episode(&conn, SandboxQuarantineReasonCode::PauseFailed);
    let foreign_source = setup_quarantine_recovery_episode(
        &conn,
        SandboxQuarantineReasonCode::RestoreFirecrackerLaunchFailed,
    );
    conn.execute(
        "UPDATE sandbox_quarantine_state SET source = ?1 WHERE sandbox_id = ?2",
        params![
            SandboxQuarantineSource::NodeStatePublish.as_str(),
            foreign_source.to_string()
        ],
    )
    .unwrap();

    let observations = quarantine_recovery_observations(
        &conn,
        &[
            recoverable.to_string(),
            non_recoverable.to_string(),
            foreign_source.to_string(),
        ],
    )
    .unwrap();

    assert_eq!(
        observations.keys().cloned().collect::<Vec<_>>(),
        vec![recoverable.to_string()]
    );
}

#[test]
fn quarantine_recovery_claim_renewal_extends_only_the_exact_claim() {
    let conn = crate::db::test_conn();
    let (sandbox_id, candidate) = setup_quarantine_recovery_candidate(&conn);
    let claimed =
        claim_quarantine_recovery_candidate(&conn, &candidate, PlanRejectionRunEffect::Leaves)
            .unwrap()
            .expect("due episode should be claimable");
    let (initial_expiry, initial_due, attempt_count): (i64, i64, i64) = conn
        .query_row(
            "SELECT CAST(claim_expires_at AS INTEGER),
                    CAST(next_action_after AS INTEGER), attempt_count
               FROM sandbox_quarantine_state
              WHERE sandbox_id = ?1",
            params![sandbox_id.to_string()],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    assert_eq!(attempt_count, 1);
    assert_eq!(claimed.candidate.attempt_count, 1);
    assert!(initial_due > initial_expiry);
    let shortened_expiry = now_epoch_i64().unwrap().saturating_add(1);
    conn.execute(
        "UPDATE sandbox_quarantine_state
            SET claim_expires_at = ?1
          WHERE sandbox_id = ?2",
        params![shortened_expiry.to_string(), sandbox_id.to_string()],
    )
    .unwrap();

    assert!(renew_quarantine_recovery_claim(&conn, &claimed).unwrap());
    let (renewed_expiry, renewed_due) = conn
        .query_row(
            "SELECT CAST(claim_expires_at AS INTEGER),
                    CAST(next_action_after AS INTEGER)
               FROM sandbox_quarantine_state
              WHERE sandbox_id = ?1",
            params![sandbox_id.to_string()],
            |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)),
        )
        .unwrap();
    assert!(renewed_expiry > now_epoch_i64().unwrap());
    assert!(renewed_due > renewed_expiry);
    assert!(quarantine_recovery_claim_is_current(&conn, &claimed).unwrap());
    assert!(
        claim_quarantine_recovery_candidate(&conn, &candidate, PlanRejectionRunEffect::Leaves)
            .unwrap()
            .is_none(),
        "renewed claim must remain exclusive"
    );

    let stale_claim = ClaimedQuarantineRecoveryCandidate {
        claim_token: Uuid::now_v7(),
        ..claimed.clone()
    };
    assert!(!renew_quarantine_recovery_claim(&conn, &stale_claim).unwrap());

    conn.execute(
        "UPDATE sandbox_quarantine_state
            SET claim_expires_at = '0'
          WHERE sandbox_id = ?1",
        params![sandbox_id.to_string()],
    )
    .unwrap();
    assert!(!quarantine_recovery_claim_is_current(&conn, &claimed).unwrap());
    assert!(!renew_quarantine_recovery_claim(&conn, &claimed).unwrap());
    let provisional_due = conn
        .query_row(
            "SELECT next_action_after
               FROM sandbox_quarantine_state
              WHERE sandbox_id = ?1",
            params![sandbox_id.to_string()],
            |row| row.get::<_, String>(0),
        )
        .unwrap();
    // A lapsed lease no longer blocks the outcome from being recorded, and that is
    // deliberate: the token is unchanged, so this process is still the last owner, and
    // refusing the write left the row holding a dead lease with nothing recorded until
    // the provisional due time came around. Renewal is a different matter — it extends
    // an exclusive hold and still requires the lease to be live, which the assertion
    // above pins.
    let _ = provisional_due;
    assert!(matches!(
        record_quarantine_recovery_no_progress(
            &conn,
            &claimed,
            QuarantineRecoveryNoProgressReason::OrphanRestoreDeferred,
        )
        .unwrap(),
        QuarantineRecoveryNoProgressOutcome::RetryScheduled { .. }
    ));
    let (attempts, token): (i64, Option<String>) = conn
        .query_row(
            "SELECT attempt_count, claim_token FROM sandbox_quarantine_state
              WHERE sandbox_id = ?1",
            params![sandbox_id.to_string()],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(attempts, 1, "the charged attempt is kept");
    assert_eq!(token, None, "the dead lease is handed back");

    // The half that must still refuse: ownership moved. Relaxing the lease, the state
    // and the cleanup episode does not relax the seven identity conjuncts, and the
    // token is the one that says who owns the episode.
    conn.execute(
        "UPDATE sandbox_quarantine_state SET claim_token = ?1, claim_expires_at = ?2
          WHERE sandbox_id = ?3",
        params![
            Uuid::now_v7().to_string(),
            (now_epoch_i64().unwrap() + 900).to_string(),
            sandbox_id.to_string()
        ],
    )
    .unwrap();
    assert_eq!(
        record_quarantine_recovery_no_progress(
            &conn,
            &claimed,
            QuarantineRecoveryNoProgressReason::OrphanRestoreDeferred,
        )
        .unwrap(),
        QuarantineRecoveryNoProgressOutcome::StaleClaim,
        "a claim whose token moved must still be refused"
    );
}

#[test]
fn sustained_cleanup_contention_slows_down() {
    let conn = crate::db::test_conn();
    let (sandbox_id, _) = setup_quarantine_recovery_candidate(&conn);
    // No cleanup episode is inserted: the candidate scan excludes cleanup-owned
    // episodes, so an episode present up front would never be claimable. In
    // production the cleanup owner wins the in-process fence after the claim,
    // which is why this release deliberately does not check for one.
    //
    // Each cycle is one pass that claimed, lost the cleanup fence, and released.
    // A cleanup owner that never yields must not produce a fixed retry rate.
    let mut waits = Vec::new();
    for _ in 0..4 {
        conn.execute(
            "UPDATE sandbox_quarantine_state SET next_action_after = NULL WHERE sandbox_id = ?1",
            params![sandbox_id.to_string()],
        )
        .unwrap();
        let [candidate] = scan_quarantine_recovery_candidates(&conn)
            .unwrap()
            .candidates
            .try_into()
            .unwrap();
        let claimed =
            claim_quarantine_recovery_candidate(&conn, &candidate, PlanRejectionRunEffect::Leaves)
                .unwrap()
                .expect("a released episode is claimable again");
        assert!(matches!(
            record_quarantine_recovery_no_progress(
                &conn,
                &claimed,
                QuarantineRecoveryNoProgressReason::CleanupFenceBusy,
            )
            .unwrap(),
            QuarantineRecoveryNoProgressOutcome::RetryScheduled { .. }
        ));
        let next_action_after: i64 = conn
            .query_row(
                "SELECT CAST(next_action_after AS INTEGER) FROM sandbox_quarantine_state
                  WHERE sandbox_id = ?1",
                params![sandbox_id.to_string()],
                |row| row.get(0),
            )
            .unwrap();
        waits.push(next_action_after.saturating_sub(now_epoch_i64().unwrap()));
    }

    assert!(
        waits.last() > waits.first(),
        "sustained cleanup contention must back off, got waits {waits:?}"
    );
}

#[test]
fn cleanup_contention_releases_the_claim_and_backs_off() {
    let conn = crate::db::test_conn();
    let (sandbox_id, candidate) = setup_quarantine_recovery_candidate(&conn);
    let claimed =
        claim_quarantine_recovery_candidate(&conn, &candidate, PlanRejectionRunEffect::Leaves)
            .unwrap()
            .expect("due episode should be claimable");
    conn.execute(
        "INSERT INTO sandbox_cleanup_episodes (
             sandbox_id, cleanup_id, completion, keep_filesystem, phase,
             activation_state, created_at
         ) VALUES (?1, ?2, 'mark_suspended', TRUE, 'armed', 'restoring', 0)",
        params![
            sandbox_id.to_string(),
            SandboxCleanupEpisodeId::new().to_string()
        ],
    )
    .unwrap();

    assert!(matches!(
        record_quarantine_recovery_no_progress(
            &conn,
            &claimed,
            QuarantineRecoveryNoProgressReason::CleanupFenceBusy,
        )
        .unwrap(),
        QuarantineRecoveryNoProgressOutcome::RetryScheduled { .. }
    ));
    let (attempt_count, next_action_after, claim_token, claim_expires_at, last_error): (
        i64,
        i64,
        Option<String>,
        Option<String>,
        Option<String>,
    ) = conn
        .query_row(
            "SELECT attempt_count, CAST(next_action_after AS INTEGER), claim_token,
                    claim_expires_at, last_error
               FROM sandbox_quarantine_state
              WHERE sandbox_id = ?1",
            params![sandbox_id.to_string()],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                ))
            },
        )
        .unwrap();
    // The charged attempt stays charged. Rewinding it would leave a repeatedly
    // contended episode retrying at a flat one-tick rate forever, because the
    // next claim would compute the same first-attempt backoff every time.
    assert_eq!(
        attempt_count,
        i64::from(claimed.candidate.attempt_count),
        "the attempt charged at claim time must survive the release"
    );
    assert!(next_action_after > now_epoch_i64().unwrap());
    assert_eq!(claim_token, None);
    assert_eq!(claim_expires_at, None);
    assert_eq!(
        last_error.as_deref(),
        Some(QuarantineRecoveryNoProgressReason::CleanupFenceBusy.as_str())
    );
    // The claim is spent: the first write cleared the token, so the fence's
    // `claim_token` conjunct no longer matches and a second write reports a stale
    // claim. That the lease, the sandbox state and the cleanup episode are all
    // relaxed does not weaken this — ownership is what the seven identity conjuncts
    // pin, and this is the one that moved.
    assert_eq!(
        record_quarantine_recovery_no_progress(
            &conn,
            &claimed,
            QuarantineRecoveryNoProgressReason::CleanupFenceBusy,
        )
        .unwrap(),
        QuarantineRecoveryNoProgressOutcome::StaleClaim,
        "the exact claim may only be released once"
    );
}

#[test]
fn quarantine_recovery_new_episode_preserves_a_live_claim() {
    let conn = crate::db::test_conn();
    // The orphan restore runs with the sandbox in `restoring`, so a failure that
    // re-quarantines it replaces the episode. That upsert is the one that has to
    // respect a live lease.
    let sandbox_id = setup_quarantine_recovery_episode(
        &conn,
        SandboxQuarantineReasonCode::OrphanedSandboxRowMissing,
    );
    conn.execute(
        "UPDATE sandboxes SET state = 'restoring' WHERE id = ?1",
        params![sandbox_id.to_string()],
    )
    .unwrap();
    let [candidate] = scan_quarantine_recovery_candidates(&conn)
        .unwrap()
        .candidates
        .try_into()
        .unwrap();
    let claimed =
        claim_quarantine_recovery_candidate(&conn, &candidate, PlanRejectionRunEffect::Leaves)
            .unwrap()
            .expect("orphan episode should be claimable");
    let (owner_before, attempts_before, _, token_before, expiry_before) =
        quarantine_claim_state(&conn, sandbox_id);
    assert_eq!(attempts_before, 1);
    assert!(token_before.is_some());

    mark_quarantined(
        &conn,
        &sandbox_id.to_string(),
        SandboxQuarantineSource::SbxManager,
        SandboxQuarantineReasonCode::RestorePrepareFailed,
        "restore failed again while the orphan recovery still holds its claim",
        SandboxQuarantineNextAction::RequiresOperatorAttention,
    )
    .unwrap();

    let (owner_after, attempts_after, retry_after, token_after, expiry_after) =
        quarantine_claim_state(&conn, sandbox_id);
    assert_eq!(
        (owner_after, token_after, expiry_after),
        (owner_before, token_before, expiry_before),
        "a re-mark from restoring must not discard the lease a running recovery \
         holds. The token can no longer satisfy a fence — the new episode has a \
         new quarantine_id, which every fence pins — but it still holds \
         claim_is_live true, which is what keeps a second operation off this \
         sandbox until the lease lapses"
    );
    assert_eq!(
        (attempts_after, retry_after),
        (0, None),
        "the pacing is reset even so: a new quarantine_id means the sandbox left \
         quarantine and came back, and that new episode should not open on the \
         previous one's backoff (it had {attempts_before} attempts charged)"
    );
    assert!(
        scan_quarantine_recovery_candidates(&conn)
            .unwrap()
            .candidates
            .is_empty(),
        "the still-leased episode must not be offered to a second operation"
    );
    // The claim is stale against the replacement episode, so the operation
    // holding it cannot commit; it fails its next fenced write and the reset
    // lands once the lease lapses.
    assert!(!quarantine_recovery_claim_is_current(&conn, &claimed).unwrap());
}

#[test]
fn quarantine_recovery_new_episode_resets_a_dead_claim() {
    let conn = crate::db::test_conn();
    let sandbox_id = setup_quarantine_recovery_episode(
        &conn,
        SandboxQuarantineReasonCode::OrphanedSandboxRowMissing,
    );
    conn.execute(
        "UPDATE sandboxes SET state = 'restoring' WHERE id = ?1",
        params![sandbox_id.to_string()],
    )
    .unwrap();
    let [candidate] = scan_quarantine_recovery_candidates(&conn)
        .unwrap()
        .candidates
        .try_into()
        .unwrap();
    claim_quarantine_recovery_candidate(&conn, &candidate, PlanRejectionRunEffect::Leaves)
        .unwrap()
        .expect("orphan episode should be claimable");
    // The lease has lapsed, so nothing is acting on the episode any more. The
    // replacement must start from a clean retry budget: inheriting the old
    // attempt count and due time would leave the new episode looking permanently
    // not-due.
    conn.execute(
        "UPDATE sandbox_quarantine_state SET claim_expires_at = '1' WHERE sandbox_id = ?1",
        params![sandbox_id.to_string()],
    )
    .unwrap();

    mark_quarantined(
        &conn,
        &sandbox_id.to_string(),
        SandboxQuarantineSource::SbxManager,
        // Kept orphan-shaped: this fixture has no local artifact, and the
        // restore_* reasons require an exact full-suspend artifact to stay
        // eligible, which would make the due-immediately assertion below
        // vacuous for a reason unrelated to the reset.
        SandboxQuarantineReasonCode::OrphanedSandboxRowMissing,
        "re-quarantined again after the previous lease lapsed",
        SandboxQuarantineNextAction::RequiresOperatorAttention,
    )
    .unwrap();

    assert_eq!(
        quarantine_claim_state(&conn, sandbox_id),
        (None, 0, None, None, None),
        "a replacement episode with no live lease must start from a clean retry budget"
    );
    assert_eq!(
        scan_quarantine_recovery_candidates(&conn)
            .unwrap()
            .candidates
            .len(),
        1,
        "the reset episode is due immediately"
    );
}

#[test]
fn quarantine_recovery_new_episode_handles_a_released_claim() {
    let conn = crate::db::test_conn();
    let (sandbox_id, candidate) = setup_quarantine_recovery_candidate(&conn);
    let claimed =
        claim_quarantine_recovery_candidate(&conn, &candidate, PlanRejectionRunEffect::Leaves)
            .unwrap()
            .expect("due episode should be claimable");
    // A recorded no-progress result leaves `claim_expires_at` NULL while
    // `next_action_owner` stays `legacy_recovery`. The liveness probe has to read
    // that as "no live claim" rather than yielding SQL NULL, which would surface
    // as a type error instead of a decision.
    record_quarantine_recovery_no_progress(
        &conn,
        &claimed,
        QuarantineRecoveryNoProgressReason::PlanIneligible,
    )
    .unwrap();
    let (owner, _, _, token, expiry) = quarantine_claim_state(&conn, sandbox_id);
    assert_eq!(
        (owner.as_deref(), token, expiry),
        (Some("legacy_recovery"), None, None),
        "precondition: owner retained, lease released"
    );
    conn.execute(
        "UPDATE sandboxes SET state = 'restoring' WHERE id = ?1",
        params![sandbox_id.to_string()],
    )
    .unwrap();

    mark_quarantined(
        &conn,
        &sandbox_id.to_string(),
        SandboxQuarantineSource::SbxManager,
        SandboxQuarantineReasonCode::RestoreSpacedHostLaunchFailed,
        "re-quarantined while the previous claim was already released",
        SandboxQuarantineNextAction::RequiresOperatorAttention,
    )
    .unwrap();

    assert_eq!(
        quarantine_claim_state(&conn, sandbox_id),
        (None, 0, None, None, None),
        "a released claim is not a live one, so the replacement resets"
    );
}

#[test]
fn quarantine_recovery_stale_claim_cannot_mutate_replacement_episode() {
    let conn = crate::db::test_conn();
    let (sandbox_id, candidate) = setup_quarantine_recovery_candidate(&conn);
    let claimed =
        claim_quarantine_recovery_candidate(&conn, &candidate, PlanRejectionRunEffect::Leaves)
            .unwrap()
            .unwrap();
    let replacement_quarantine_id = Uuid::now_v7();
    conn.execute(
        "UPDATE sandbox_quarantine_state
            SET quarantine_id = ?1,
                attempt_count = 0,
                claim_token = NULL,
                claim_expires_at = NULL
          WHERE sandbox_id = ?2",
        params![
            replacement_quarantine_id.to_string(),
            sandbox_id.to_string()
        ],
    )
    .unwrap();

    assert_eq!(
        record_quarantine_recovery_no_progress(
            &conn,
            &claimed,
            QuarantineRecoveryNoProgressReason::PlanIneligible,
        )
        .unwrap(),
        QuarantineRecoveryNoProgressOutcome::StaleClaim
    );
    assert_eq!(
        conn.query_row(
            "SELECT quarantine_id, attempt_count FROM sandbox_quarantine_state WHERE sandbox_id = ?1",
            params![sandbox_id.to_string()],
            |row| Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?)),
        )
        .unwrap(),
        (replacement_quarantine_id.to_string(), 0)
    );
}

#[test]
fn quarantine_adoption_completes_with_the_authority_receipt_id() {
    let conn = test_conn();
    let sandbox_id = SandboxId::new();
    let sandbox_id_string = sandbox_id.to_string();
    let runtime_label = SandboxRuntimeLabel::from_sandbox_id(sandbox_id);
    insert_test_sandbox(
        &conn,
        TestSandboxInsert::new(
            &sandbox_id_string,
            runtime_label.to_string(),
            SandboxState::Restoring,
        ),
    );
    let local_artifact_id = LocalArtifactId::new();
    insert_local_artifact(
        &conn,
        &sandbox_id_string,
        local_artifact_id,
        ArtifactKind::Full,
        ArtifactOrigin::Suspend,
        None,
        "/space/cache/sandboxes/test/snapshot",
    )
    .unwrap();
    conn.execute(
        "UPDATE sandboxes
            SET current_local_artifact_id = ?1,
                current_local_artifact_kind = 'full',
                current_local_artifact_origin = 'suspend',
                space_api_route_execution_generation = 7
          WHERE id = ?2",
        params![local_artifact_id.to_string(), sandbox_id_string],
    )
    .unwrap();
    assert!(
        mark_restoring_quarantined(
            &conn,
            &sandbox_id_string,
            SandboxQuarantineReasonCode::RestoreFirecrackerLaunchFailed,
            "restore rollout interrupted",
        )
        .unwrap()
    );
    let [candidate] = scan_quarantine_recovery_candidates(&conn)
        .unwrap()
        .candidates
        .try_into()
        .unwrap();
    let claimed =
        claim_quarantine_recovery_candidate(&conn, &candidate, PlanRejectionRunEffect::Leaves)
            .unwrap()
            .unwrap();
    let recovery_id = SandboxCleanupEpisodeId::new();
    let execution = ExecutionMetadata::initial_firecracker(Arch::Aarch64, vec![]);
    assert_eq!(
        adopt_quarantine_cleanup_episode(
            &conn,
            &claimed,
            adoption_witness(&claimed),
            ExecutionGeneration::new(7).unwrap(),
            recovery_id,
            &execution,
            "/space/cache/sandboxes/test/snapshot",
        )
        .unwrap(),
        QuarantineAdoptionOutcome::Adopted
    );
    let episode = cleanup_episode(&conn, &sandbox_id_string).unwrap().unwrap();
    assert_eq!(
        episode.quarantine_recovery,
        Some(QuarantineRecoveryFence {
            local_artifact_id,
            execution_generation: ExecutionGeneration::new(7).unwrap(),
        })
    );
    assert!(matches!(
        mark_stopping_superseding_activation(&conn, &sandbox_id_string),
        Err(SupersedeActivationError::QuarantineRecoveryInProgress)
    ));
    assert_eq!(
        cleanup_episode(&conn, &sandbox_id_string)
            .unwrap()
            .unwrap()
            .id,
        recovery_id,
        "Stop must not discard a quarantine-recovery episode"
    );

    assert!(
        complete_cleanup_episode(&conn, &sandbox_id_string, recovery_id)
            .unwrap()
            .is_some()
    );
    let row = get_sandbox(&conn, &sandbox_id_string).unwrap().unwrap();
    assert_eq!(row.state, SandboxState::Suspended);
    assert!(
        cleanup_episode(&conn, &sandbox_id_string)
            .unwrap()
            .is_none()
    );
    let publication =
        sandbox_state_publication_metadata(&conn, std::slice::from_ref(&sandbox_id_string))
            .unwrap()
            .remove(&sandbox_id_string)
            .unwrap()
            .unwrap()
            .decode(SandboxState::Suspended)
            .unwrap();
    assert!(matches!(
        publication,
        SandboxStatePublicationMetadata::TransitionReceipt(receipt)
            if receipt.id() == recovery_id
                && receipt.kind() == SandboxTransitionReceiptKind::QuarantineRecovery
                && receipt.target_state() == SandboxTransitionTargetState::Suspended
                && receipt.reason_code()
                    == Some(&PublishedQuarantineReasonCode::Known(
                        SandboxQuarantineReasonCode::RestoreFirecrackerLaunchFailed
                    ))
                && receipt.execution_generation() == ExecutionGeneration::new(7).unwrap()
                && receipt.activation_attempt_id().is_none()
    ));
    let rejected_artifact_id = LocalArtifactId::new();
    assert!(
        !publish_orphan_quarantine_recovered_artifact(
            &conn,
            &claimed,
            Some(rejected_artifact_id),
            "/space/cache/sandboxes/test/rejected",
            ExecutionGeneration::new(8).unwrap(),
            SandboxCleanupEpisodeId::new(),
        )
        .unwrap()
    );
    let retained_artifact_id: String = conn
        .query_row(
            "SELECT local_artifact_id FROM local_artifacts WHERE sandbox_id = ?1",
            params![sandbox_id_string],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(retained_artifact_id, local_artifact_id.to_string());
}

#[test]
fn orphan_quarantine_recovery_clears_staging_failure() {
    let conn = test_conn();
    let sandbox_id = SandboxId::new();
    let sandbox_id_string = sandbox_id.to_string();
    insert_test_sandbox(
        &conn,
        TestSandboxInsert::new(
            &sandbox_id_string,
            SandboxRuntimeLabel::from_sandbox_id(sandbox_id).to_string(),
            SandboxState::Restoring,
        ),
    );
    assert!(
        mark_restoring_quarantined(
            &conn,
            &sandbox_id_string,
            SandboxQuarantineReasonCode::OrphanedSandboxRowMissing,
            "restore staging failed",
        )
        .unwrap()
    );
    assert!(
        is_orphan_quarantine_recovery_state(&conn, &sandbox_id_string, SandboxState::Quarantined,)
            .unwrap()
    );
    conn.execute(
        "UPDATE sandboxes
            SET state = 'restoring',
                space_api_route_execution_generation = 7
          WHERE id = ?1",
        params![sandbox_id_string],
    )
    .unwrap();
    assert!(
        is_orphan_quarantine_recovery_state(&conn, &sandbox_id_string, SandboxState::Restoring,)
            .unwrap()
    );
    let transfer_scope_id = pplx_space_common_core::types::TransferScopeId::new();
    assert!(
        insert_restore_transfer_scope(
            &conn,
            &sandbox_id_string,
            transfer_scope_id,
            Some(ExecutionGeneration::new(6).unwrap()),
            None,
            "/space/cache/restore-staging/test",
        )
        .unwrap()
    );
    assert!(
        !retire_rowless_restore_transfer_scope(&conn, &sandbox_id_string, transfer_scope_id,)
            .unwrap()
    );
    assert!(
        retire_orphan_quarantine_restore_transfer_scope(
            &conn,
            &sandbox_id_string,
            transfer_scope_id,
        )
        .unwrap()
    );

    let [candidate] = scan_quarantine_recovery_candidates(&conn)
        .unwrap()
        .candidates
        .try_into()
        .unwrap();
    let claimed =
        claim_quarantine_recovery_candidate(&conn, &candidate, PlanRejectionRunEffect::Leaves)
            .unwrap()
            .unwrap();

    assert!(
        publish_orphan_quarantine_recovered_artifact(
            &conn,
            &claimed,
            Some(LocalArtifactId::new()),
            "/space/cache/sandboxes/test/snapshot",
            ExecutionGeneration::new(7).unwrap(),
            SandboxCleanupEpisodeId::new(),
        )
        .unwrap()
    );
    let (state, failure_reason): (String, Option<String>) = conn
        .query_row(
            "SELECT state, failure_reason FROM sandboxes WHERE id = ?1",
            params![sandbox_id_string],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(state, SandboxState::Suspended.as_str());
    assert_eq!(failure_reason, None);
}

#[test]
fn canonical_restore_reclaim_requires_the_exact_attempt_and_no_runtime() {
    let conn = test_conn();
    let sandbox_id = SandboxId::new();
    let sandbox_id_string = sandbox_id.to_string();
    insert_test_sandbox(
        &conn,
        TestSandboxInsert::new(
            &sandbox_id_string,
            SandboxRuntimeLabel::from_sandbox_id(sandbox_id).to_string(),
            SandboxState::Restoring,
        ),
    );
    conn.execute(
        "UPDATE sandboxes
            SET restore_origin = 'remote',
                restore_phase = 'artifact_staging',
                restore_storage_uri = 's3://test-bucket/suspend/source?region=us-east-1'
          WHERE id = ?1",
        params![sandbox_id_string],
    )
    .unwrap();
    let generation = ExecutionGeneration::new(7).unwrap();
    let activation_attempt_id = SandboxActivationAttemptId::new();
    let transfer_scope_id = pplx_space_common_core::types::TransferScopeId::new();
    assert!(
        insert_restore_transfer_scope(
            &conn,
            &sandbox_id_string,
            transfer_scope_id,
            Some(generation),
            Some(activation_attempt_id),
            "/space/cache/restore-staging/test",
        )
        .unwrap()
    );

    assert!(
        canonical_restore_reclaim_attempt_is_current(
            &conn,
            &sandbox_id_string,
            transfer_scope_id,
            generation,
            activation_attempt_id,
        )
        .unwrap()
    );
    assert!(
        !canonical_restore_reclaim_attempt_is_current(
            &conn,
            &sandbox_id_string,
            transfer_scope_id,
            ExecutionGeneration::new(8).unwrap(),
            activation_attempt_id,
        )
        .unwrap(),
        "a delayed generation must not reclaim the current root"
    );

    conn.execute(
        "UPDATE sandboxes SET pid = 123 WHERE id = ?1",
        params![sandbox_id_string],
    )
    .unwrap();
    assert!(
        !canonical_restore_reclaim_attempt_is_current(
            &conn,
            &sandbox_id_string,
            transfer_scope_id,
            generation,
            activation_attempt_id,
        )
        .unwrap(),
        "a root with a live runtime is never reclaimable"
    );
}

#[test]
fn retryable_restore_scope_handoff_rejects_a_after_b_and_admits_c() {
    let conn = test_conn();
    let sandbox_id = SandboxId::new();
    let sandbox_id_string = sandbox_id.to_string();
    insert_test_sandbox(
        &conn,
        TestSandboxInsert::new(
            &sandbox_id_string,
            SandboxRuntimeLabel::from_sandbox_id(sandbox_id).to_string(),
            SandboxState::Restoring,
        ),
    );
    conn.execute(
        "UPDATE sandboxes
            SET restore_origin = 'remote',
                restore_phase = 'artifact_staging',
                restore_storage_uri = 's3://test-bucket/suspend/source?region=us-east-1'
          WHERE id = ?1",
        params![sandbox_id_string],
    )
    .unwrap();

    let generation = ExecutionGeneration::new(7).unwrap();
    let attempt_a = SandboxActivationAttemptId::from_uuid(
        Uuid::parse_str("019c0000-0000-7000-8000-000000000001").unwrap(),
    );
    let attempt_b = SandboxActivationAttemptId::from_uuid(
        Uuid::parse_str("019c0000-0000-7000-8000-000000000002").unwrap(),
    );
    let attempt_c = SandboxActivationAttemptId::from_uuid(
        Uuid::parse_str("019c0000-0000-7000-8000-000000000003").unwrap(),
    );
    let scope_a = pplx_space_common_core::types::TransferScopeId::new();
    assert!(
        insert_restore_transfer_scope(
            &conn,
            &sandbox_id_string,
            scope_a,
            Some(generation),
            Some(attempt_a),
            "/space/cache/restore-staging/a",
        )
        .unwrap()
    );

    assert!(
        claim_retryable_restore_transfer_scope_if_current_or_newer(
            &conn,
            &sandbox_id_string,
            scope_a,
            generation,
            attempt_b,
        )
        .unwrap(),
        "B must supersede A"
    );
    let claimed_b = restore_transfer_scope(&conn, &sandbox_id_string)
        .unwrap()
        .unwrap();
    assert_eq!(
        claimed_b.transfer_scope_id, scope_a,
        "B must retain A's durable physical cleanup reference until cleanup succeeds"
    );
    assert_eq!(claimed_b.staging_dir, "/space/cache/restore-staging/a");
    assert!(
        !claim_retryable_restore_transfer_scope_if_current_or_newer(
            &conn,
            &sandbox_id_string,
            scope_a,
            generation,
            attempt_a,
        )
        .unwrap(),
        "delayed A must not supersede B"
    );
    assert_eq!(
        restore_transfer_scope(&conn, &sandbox_id_string)
            .unwrap()
            .unwrap()
            .activation_attempt_id,
        Some(attempt_b)
    );
    assert!(
        claim_retryable_restore_transfer_scope_if_current_or_newer(
            &conn,
            &sandbox_id_string,
            scope_a,
            generation,
            attempt_b,
        )
        .unwrap(),
        "B must be able to resume cleanup after a crash"
    );
    assert!(
        claim_retryable_restore_transfer_scope_if_current_or_newer(
            &conn,
            &sandbox_id_string,
            scope_a,
            generation,
            attempt_c,
        )
        .unwrap(),
        "C must supersede B after B disappears"
    );
    assert_eq!(
        restore_transfer_scope(&conn, &sandbox_id_string)
            .unwrap()
            .unwrap()
            .transfer_scope_id,
        scope_a,
        "C must retain B's cleanup reference until cleanup succeeds"
    );
    assert_eq!(
        restore_transfer_scope(&conn, &sandbox_id_string)
            .unwrap()
            .unwrap()
            .activation_attempt_id,
        Some(attempt_c)
    );

    publish_restored_suspended_without_artifact(&conn, &sandbox_id_string).unwrap();
    assert_eq!(
        get_sandbox(&conn, &sandbox_id_string)
            .unwrap()
            .unwrap()
            .state,
        SandboxState::Suspended,
        "restore completion must commit lifecycle convergence"
    );
    assert!(
        restore_transfer_scope(&conn, &sandbox_id_string)
            .unwrap()
            .is_none(),
        "restore completion must atomically retire C's durable high-water mark"
    );
}

#[test]
fn authoritative_restore_attempt_claims_legacy_unowned_retryable_scope() {
    let conn = test_conn();
    let sandbox_id = SandboxId::new();
    let sandbox_id_string = sandbox_id.to_string();
    insert_test_sandbox(
        &conn,
        TestSandboxInsert::new(
            &sandbox_id_string,
            SandboxRuntimeLabel::from_sandbox_id(sandbox_id).to_string(),
            SandboxState::Restoring,
        ),
    );
    conn.execute(
        "UPDATE sandboxes
            SET restore_origin = 'remote',
                restore_phase = 'artifact_staging',
                restore_storage_uri = 's3://test-bucket/suspend/source?region=us-east-1'
          WHERE id = ?1",
        params![sandbox_id_string],
    )
    .unwrap();

    let legacy_scope = pplx_space_common_core::types::TransferScopeId::new();
    assert!(
        insert_restore_transfer_scope(
            &conn,
            &sandbox_id_string,
            legacy_scope,
            None,
            None,
            "/space/cache/restore-staging/legacy",
        )
        .unwrap()
    );

    let generation = ExecutionGeneration::new(7).unwrap();
    let attempt = SandboxActivationAttemptId::new();
    assert!(
        claim_retryable_restore_transfer_scope_if_current_or_newer(
            &conn,
            &sandbox_id_string,
            legacy_scope,
            generation,
            attempt,
        )
        .unwrap(),
        "a complete authoritative attempt must supersede an unowned legacy scope"
    );

    let current = restore_transfer_scope(&conn, &sandbox_id_string)
        .unwrap()
        .unwrap();
    assert_eq!(current.transfer_scope_id, legacy_scope);
    assert_eq!(current.execution_generation, Some(generation));
    assert_eq!(current.activation_attempt_id, Some(attempt));
    conn.execute(
        "UPDATE sandbox_restore_transfer_scopes
            SET execution_generation = NULL
          WHERE sandbox_id = ?1",
        params![sandbox_id_string],
    )
    .unwrap();
    assert!(
        !claim_retryable_restore_transfer_scope_if_current_or_newer(
            &conn,
            &sandbox_id_string,
            legacy_scope,
            ExecutionGeneration::new(8).unwrap(),
            SandboxActivationAttemptId::new(),
        )
        .unwrap(),
        "a partially-owned scope is malformed, not an adoptable legacy scope"
    );
}

#[test]
fn exchanged_restore_cleanup_owner_advances_without_moving_the_scope() {
    let conn = test_conn();
    let sandbox_id = SandboxId::new();
    let sandbox_id_string = sandbox_id.to_string();
    insert_test_sandbox(
        &conn,
        TestSandboxInsert::new(
            &sandbox_id_string,
            SandboxRuntimeLabel::from_sandbox_id(sandbox_id).to_string(),
            SandboxState::Restoring,
        ),
    );
    conn.execute(
        "UPDATE sandboxes
            SET restore_origin = 'remote',
                restore_phase = 'artifact_staging',
                restore_storage_uri = 's3://test-bucket/suspend/source?region=us-east-1'
          WHERE id = ?1",
        params![sandbox_id_string],
    )
    .unwrap();

    let generation = ExecutionGeneration::new(7).unwrap();
    let attempt_a = SandboxActivationAttemptId::from_uuid(
        Uuid::parse_str("019c0000-0000-7000-8000-000000000001").unwrap(),
    );
    let attempt_b = SandboxActivationAttemptId::from_uuid(
        Uuid::parse_str("019c0000-0000-7000-8000-000000000002").unwrap(),
    );
    let attempt_c = SandboxActivationAttemptId::from_uuid(
        Uuid::parse_str("019c0000-0000-7000-8000-000000000003").unwrap(),
    );
    let exchanged_scope = pplx_space_common_core::types::TransferScopeId::new();
    let staging_dir = "/space/cache/restore-staging/exchanged";
    assert!(
        insert_restore_transfer_scope(
            &conn,
            &sandbox_id_string,
            exchanged_scope,
            Some(generation),
            Some(attempt_b),
            staging_dir,
        )
        .unwrap()
    );

    assert!(
        advance_exchanged_restore_cleanup_owner_if_newer(
            &conn,
            &sandbox_id_string,
            exchanged_scope,
            generation,
            attempt_c,
        )
        .unwrap(),
        "C must take over B's retryable exchanged-root cleanup"
    );
    let retained = restore_transfer_scope(&conn, &sandbox_id_string)
        .unwrap()
        .unwrap();
    assert_eq!(retained.transfer_scope_id, exchanged_scope);
    assert_eq!(retained.staging_dir, staging_dir);
    assert_eq!(retained.activation_attempt_id, Some(attempt_c));

    assert!(
        !advance_exchanged_restore_cleanup_owner_if_newer(
            &conn,
            &sandbox_id_string,
            exchanged_scope,
            generation,
            attempt_a,
        )
        .unwrap(),
        "delayed A must remain fenced after C takes over cleanup"
    );
}

#[test]
fn orphan_quarantine_recovery_supports_legacy_snapshot_without_artifact_identity() {
    let conn = test_conn();
    let sandbox_id = SandboxId::new();
    let sandbox_id_string = sandbox_id.to_string();
    insert_test_sandbox(
        &conn,
        TestSandboxInsert::new(
            &sandbox_id_string,
            SandboxRuntimeLabel::from_sandbox_id(sandbox_id).to_string(),
            SandboxState::Restoring,
        ),
    );
    assert!(
        mark_restoring_quarantined(
            &conn,
            &sandbox_id_string,
            SandboxQuarantineReasonCode::OrphanedSandboxRowMissing,
            "legacy restore staging lost its row",
        )
        .unwrap()
    );
    conn.execute(
        "UPDATE sandboxes
            SET state = 'restoring',
                space_api_route_execution_generation = 9
          WHERE id = ?1",
        params![sandbox_id_string],
    )
    .unwrap();
    let recovery_id = SandboxCleanupEpisodeId::new();
    let [candidate] = scan_quarantine_recovery_candidates(&conn)
        .unwrap()
        .candidates
        .try_into()
        .unwrap();
    let claimed =
        claim_quarantine_recovery_candidate(&conn, &candidate, PlanRejectionRunEffect::Leaves)
            .unwrap()
            .unwrap();

    assert!(
        publish_orphan_quarantine_recovered_artifact(
            &conn,
            &claimed,
            None,
            "/space/cache/sandboxes/test/snapshot",
            ExecutionGeneration::new(9).unwrap(),
            recovery_id,
        )
        .unwrap()
    );

    let row = get_sandbox(&conn, &sandbox_id_string).unwrap().unwrap();
    assert_eq!(row.state, SandboxState::Suspended);
    assert_eq!(row.current_local_artifact_id, None);
    assert_eq!(row.current_local_artifact_kind, None);
    assert_eq!(row.current_local_artifact_origin, None);
    assert_eq!(
        conn.query_row(
            "SELECT COUNT(*) FROM local_artifacts WHERE sandbox_id = ?1",
            params![sandbox_id_string],
            |row| row.get::<_, i64>(0),
        )
        .unwrap(),
        0
    );
    assert!(
        current_quarantine_reason_wire_value(&conn, &sandbox_id_string)
            .unwrap()
            .is_none()
    );
    let publication = sandbox_state_publication_metadata(&conn, &[sandbox_id_string])
        .unwrap()
        .into_values()
        .next()
        .unwrap()
        .unwrap()
        .decode(SandboxState::Suspended)
        .unwrap();
    assert!(matches!(
        publication,
        SandboxStatePublicationMetadata::TransitionReceipt(receipt)
            if receipt.id() == recovery_id
                && receipt.kind() == SandboxTransitionReceiptKind::QuarantineRecovery
                && receipt.execution_generation() == ExecutionGeneration::new(9).unwrap()
    ));
}

#[test]
fn mark_quarantined_reports_unknown_current_state() {
    let conn = test_conn();
    insert_test_sandbox(
        &conn,
        TestSandboxInsert::new("sandbox-1", "sandbox1", SandboxState::Started),
    );
    seed_historical_lifecycle_rows(&conn, |conn| {
        conn.execute(
            "UPDATE sandboxes SET state = 'mystery' WHERE id = ?1",
            params!["sandbox-1"],
        )
        .unwrap();
    });

    let err = mark_quarantined(
        &conn,
        "sandbox-1",
        SandboxQuarantineSource::SbxManager,
        SandboxQuarantineReasonCode::RuntimeProbeInconsistent,
        "bad state",
        SandboxQuarantineNextAction::RequiresOperatorAttention,
    )
    .unwrap_err();

    assert!(matches!(
        err,
        MarkQuarantinedError::UnknownCurrentState { state } if state == "mystery"
    ));
}

#[test]
fn mark_cleanup_required_if_no_intent_preserves_foreign_intent() {
    let conn = test_conn();
    insert_test_sandbox(
        &conn,
        TestSandboxInsert::new("sandbox-1", "sandbox1", SandboxState::Restoring),
    );
    set_cleanup_intent(&conn, "sandbox-1", false, CleanupCompletion::DeleteRow).unwrap();

    let wrote = mark_cleanup_required_if_no_intent(
        &conn,
        "sandbox-1",
        true,
        CleanupCompletion::MarkSuspended,
        "retirement",
    )
    .unwrap();

    assert!(!wrote, "a persisted foreign intent must not be overwritten");
    let row = get_sandbox(&conn, "sandbox-1").unwrap().unwrap();
    assert_eq!(row.state, SandboxState::Restoring);
    assert_eq!(row.cleanup_completion, Some(CleanupCompletion::DeleteRow));
    assert_eq!(row.cleanup_keep_filesystem, Some(false));

    clear_cleanup_intent(&conn, "sandbox-1").unwrap();
    let wrote = mark_cleanup_required_if_no_intent(
        &conn,
        "sandbox-1",
        true,
        CleanupCompletion::MarkSuspended,
        "retirement",
    )
    .unwrap();

    assert!(wrote);
    let row = get_sandbox(&conn, "sandbox-1").unwrap().unwrap();
    assert_eq!(row.state, SandboxState::CleanupRequired);
    assert_eq!(
        row.cleanup_completion,
        Some(CleanupCompletion::MarkSuspended)
    );
}

#[test]
fn mark_start_succeeded_requires_starting_state() {
    let conn = test_conn();
    let receipt_sandbox_id = SandboxId::new().to_string();
    insert_test_sandbox(
        &conn,
        TestSandboxInsert::new("starting-sandbox", "startsbx", SandboxState::Starting),
    );
    conn.execute(
        "UPDATE sandboxes
            SET current_local_artifact_id = ?1,
                current_local_artifact_kind = 'full',
                current_local_artifact_origin = 'suspend'
          WHERE id = 'starting-sandbox'",
        [LocalArtifactId::new().to_string()],
    )
    .unwrap();
    insert_test_sandbox(
        &conn,
        TestSandboxInsert::new("deleting-sandbox", "deletebx", SandboxState::Deleting),
    );
    insert_test_sandbox(
        &conn,
        TestSandboxInsert::new(&receipt_sandbox_id, "receipts", SandboxState::Created),
    );
    let activation_attempt_id = SandboxActivationAttemptId::new();
    let episode = begin_activation(
        &conn,
        &receipt_sandbox_id,
        SandboxState::Created,
        ActivationKind::ColdStart,
        Some(ExecutionGeneration::new(7).unwrap()),
        Some(activation_attempt_id),
    )
    .unwrap()
    .unwrap();

    assert!(mark_start_succeeded(&conn, "starting-sandbox", None).unwrap());
    assert!(mark_start_succeeded(&conn, &receipt_sandbox_id, Some(episode.id)).unwrap());
    assert!(!mark_start_succeeded(&conn, "deleting-sandbox", None).unwrap());

    let started = get_sandbox(&conn, "starting-sandbox").unwrap().unwrap();
    let deleting = get_sandbox(&conn, "deleting-sandbox").unwrap().unwrap();
    assert_eq!(started.state, SandboxState::Started);
    assert!(started.current_local_artifact_id.is_none());
    assert!(started.current_local_artifact_kind.is_none());
    assert!(started.current_local_artifact_origin.is_none());
    assert_eq!(deleting.state, SandboxState::Deleting);
    let receipt =
        sandbox_state_publication_metadata(&conn, std::slice::from_ref(&receipt_sandbox_id))
            .unwrap()
            .remove(&receipt_sandbox_id)
            .unwrap()
            .unwrap()
            .decode(SandboxState::Started)
            .unwrap();
    assert!(matches!(
        receipt,
        SandboxStatePublicationMetadata::TransitionReceipt(receipt)
            if receipt.kind() == SandboxTransitionReceiptKind::ActivationCompleted
                && receipt.target_state() == SandboxTransitionTargetState::Started
                && receipt.execution_generation() == ExecutionGeneration::new(7).unwrap()
                && receipt.activation_attempt_id() == Some(activation_attempt_id)
                && receipt.reason_code().is_none()
    ));
}

#[test]
fn start_arms_access_idle_only_after_guest_readiness() {
    let conn = test_conn();
    let mut insert =
        TestSandboxInsert::new("delayed-ready-sandbox", "delayrdy", SandboxState::Starting);
    insert.auto_lifecycle = access_idle_policy();
    insert_test_sandbox(&conn, insert);

    let before = get_sandbox(&conn, "delayed-ready-sandbox")
        .unwrap()
        .unwrap();
    let condition =
        &before.auto_lifecycle_state.conditions[&AutoLifecycleConditionSetId::AccessIdle];
    assert!(!condition.armed);
    assert_eq!(before.next_auto_event_at, None);

    let armed_after = crate::auto_lifecycle::now_secs();
    assert!(mark_start_succeeded(&conn, "delayed-ready-sandbox", None).unwrap());

    let started = get_sandbox(&conn, "delayed-ready-sandbox")
        .unwrap()
        .unwrap();
    let condition =
        &started.auto_lifecycle_state.conditions[&AutoLifecycleConditionSetId::AccessIdle];
    let epoch = condition.epoch_at.unwrap();
    assert!(condition.armed);
    assert!(epoch >= armed_after);
    assert_eq!(started.next_auto_event_at, Some(epoch + 300));

    let mut restore_insert =
        TestSandboxInsert::new("restore-ready-sandbox", "restrdy1", SandboxState::Restoring);
    restore_insert.auto_lifecycle = access_idle_policy();
    insert_test_sandbox(&conn, restore_insert);
    assert_eq!(
        get_sandbox(&conn, "restore-ready-sandbox")
            .unwrap()
            .unwrap()
            .next_auto_event_at,
        None
    );
    assert!(
        mark_restore_succeeded(&conn, "restore-ready-sandbox", SandboxState::Paused, None,)
            .unwrap()
    );
    let restored = get_sandbox(&conn, "restore-ready-sandbox")
        .unwrap()
        .unwrap();
    let restored_epoch =
        restored.auto_lifecycle_state.conditions[&AutoLifecycleConditionSetId::AccessIdle].epoch_at;
    assert_eq!(restored_epoch, None);

    state_write_fixtures::apply_edge(&conn, "restore-ready-sandbox", &edges::RESUME_BEGIN);
    assert!(
        restore_resident_resume_state(&conn, "restore-ready-sandbox", SandboxState::Started,)
            .unwrap()
    );
    let resumed = get_sandbox(&conn, "restore-ready-sandbox")
        .unwrap()
        .unwrap();
    let resumed_epoch =
        resumed.auto_lifecycle_state.conditions[&AutoLifecycleConditionSetId::AccessIdle].epoch_at;
    assert!(resumed_epoch.is_some());

    state_write_fixtures::force_state(&conn, "restore-ready-sandbox", SandboxState::Resuming);
    assert!(
        restore_resident_resume_state(&conn, "restore-ready-sandbox", SandboxState::Started,)
            .unwrap()
    );
    assert_eq!(
        get_sandbox(&conn, "restore-ready-sandbox")
            .unwrap()
            .unwrap()
            .auto_lifecycle_state
            .conditions[&AutoLifecycleConditionSetId::AccessIdle]
            .epoch_at,
        resumed_epoch,
        "resident resume must not reset an already-active inactivity clock"
    );

    let mut running_restore =
        TestSandboxInsert::new("running-restore", "runrestr", SandboxState::Restoring);
    running_restore.auto_lifecycle = access_idle_policy();
    insert_test_sandbox(&conn, running_restore);
    assert!(mark_restore_succeeded(&conn, "running-restore", SandboxState::Started, None).unwrap());
    assert!(
        get_sandbox(&conn, "running-restore")
            .unwrap()
            .unwrap()
            .auto_lifecycle_state
            .conditions[&AutoLifecycleConditionSetId::AccessIdle]
            .armed
    );
}

#[test]
fn legacy_equal_route_and_operation_receipt_survives_restart_without_split_field() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("manager.sqlite");
    let mut conn = Connection::open_at(&path).unwrap();
    create_test_schema(&mut conn);
    let sandbox_id = SandboxId::new().to_string();
    insert_test_sandbox(
        &conn,
        TestSandboxInsert::new(&sandbox_id, "legacyop", SandboxState::Created),
    );
    let route_and_operation = SandboxActivationAttemptId::new();
    let generation = ExecutionGeneration::new(7).unwrap();
    let episode = begin_activation(
        &conn,
        &sandbox_id,
        SandboxState::Created,
        ActivationKind::ColdStart,
        Some(generation),
        Some(route_and_operation),
    )
    .unwrap()
    .unwrap();
    assert_eq!(episode.operation_activation_attempt_id, None);
    assert_eq!(episode.activation_attempt_id, Some(route_and_operation));

    drop(conn);
    let conn = Connection::open_at(&path).unwrap();
    assert!(mark_start_succeeded(&conn, &sandbox_id, Some(episode.id)).unwrap());
    let metadata = sandbox_state_publication_metadata(&conn, std::slice::from_ref(&sandbox_id))
        .unwrap()
        .remove(&sandbox_id)
        .unwrap()
        .unwrap()
        .decode(SandboxState::Started)
        .unwrap();
    assert!(matches!(
        metadata,
        SandboxStatePublicationMetadata::TransitionReceipt(receipt)
            if receipt.kind() == SandboxTransitionReceiptKind::ActivationCompleted
                && receipt.execution_generation() == generation
                && receipt.activation_attempt_id() == Some(route_and_operation)
    ));
}

#[test]
fn split_operation_receipts_survive_restart_and_preserve_route_ownership() {
    for (joined, rollback) in [(false, false), (true, false), (false, true), (true, true)] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("manager.sqlite");
        let mut conn = Connection::open_at(&path).unwrap();
        create_test_schema(&mut conn);
        let id = uuid::Uuid::now_v7().to_string();
        insert_test_sandbox(
            &conn,
            TestSandboxInsert::new(&id, "split001", SandboxState::Created),
        );
        let route_attempt = SandboxActivationAttemptId::new();
        let operation = SandboxActivationAttemptId::new_after(route_attempt).unwrap();
        let generation = ExecutionGeneration::new(7).unwrap();
        let route = SpaceApiRouteIdentity {
            execution_generation: Some(generation),
            activation_attempt_id: Some(route_attempt),
            ..Default::default()
        };
        let episode = begin_activation_with_route_evidence(
            &conn,
            &id,
            SandboxState::Created,
            ActivationKind::ColdStart,
            Some(generation),
            Some(route_attempt),
            ActivationRouteAdmission {
                operation_activation_attempt_id: (!joined).then_some(operation),
                ..Default::default()
            },
        )
        .unwrap()
        .unwrap();
        if joined {
            bind_activation_operation(&conn, &id, &route, operation).unwrap();
            assert!(matches!(
                bind_activation_operation(&conn, &id, &route, route_attempt),
                Err(ActivationOperationError::Superseded)
            ));
            let wrong_route = SpaceApiRouteIdentity {
                execution_generation: Some(ExecutionGeneration::new(8).unwrap()),
                ..route.clone()
            };
            assert!(matches!(
                bind_activation_operation(&conn, &id, &wrong_route, operation),
                Err(ActivationOperationError::RouteMismatch)
            ));
        }
        drop(conn); // No request future or in-memory owner survives this restart.
        let conn = Connection::open_at(&path).unwrap();
        let stored = cleanup_episode(&conn, &id).unwrap().unwrap();
        assert_eq!(stored.id, episode.id);
        assert_eq!(stored.activation_attempt_id, Some(route_attempt));
        assert_eq!(stored.operation_activation_attempt_id, Some(operation));
        let target = if rollback {
            conn.execute_batch("CREATE TEMP TRIGGER fail_startup_handoff BEFORE UPDATE OF phase ON sandbox_cleanup_episodes
                WHEN NEW.phase = 'requested' BEGIN SELECT RAISE(ABORT, 'injected startup write failure'); END;").unwrap();
            assert!(
                recover_interrupted_startup_states(&conn).is_err(),
                "startup must not succeed with an armed episode whose process owner is gone"
            );
            assert!(
                cleanup_episode(&conn, &id)
                    .unwrap()
                    .unwrap()
                    .requested_at
                    .is_none()
            );
            conn.execute_batch("DROP TRIGGER fail_startup_handoff")
                .unwrap();
            recover_interrupted_startup_states(&conn).unwrap();
            recover_interrupted_startup_states(&conn).unwrap();
            assert!(
                cleanup_recovery_page(&conn, None, 128)
                    .unwrap()
                    .ids
                    .contains(&id),
                "after restart the periodic cleanup owner can discover the exact episode"
            );
            // Once cleanup owns the episode, a later Wake must not relabel its
            // receipt as evidence about an operation it never performed.
            bind_activation_operation(
                &conn,
                &id,
                &route,
                SandboxActivationAttemptId::new_after(operation).unwrap(),
            )
            .unwrap();
            assert_eq!(
                cleanup_episode(&conn, &id)
                    .unwrap()
                    .unwrap()
                    .operation_activation_attempt_id,
                Some(operation)
            );
            assert!(
                complete_cleanup_episode(&conn, &id, episode.id)
                    .unwrap()
                    .is_some()
            );
            SandboxState::Created
        } else {
            assert!(mark_start_succeeded(&conn, &id, Some(episode.id)).unwrap());
            SandboxState::Started
        };
        let metadata = sandbox_state_publication_metadata(&conn, std::slice::from_ref(&id))
            .unwrap()
            .remove(&id)
            .unwrap()
            .unwrap()
            .decode(target)
            .unwrap();
        assert!(
            matches!(metadata, SandboxStatePublicationMetadata::TransitionReceipt(receipt)
            if receipt.activation_attempt_id() == Some(operation)
                && receipt.execution_generation() == generation
                && receipt.kind() == if rollback { SandboxTransitionReceiptKind::ActivationRollback }
                    else { SandboxTransitionReceiptKind::ActivationCompleted })
        );
    }
}

#[test]
fn generic_lifecycle_writers_reject_failed_targets() {
    let conn = test_conn();
    insert_test_sandbox(
        &conn,
        TestSandboxInsert::new("starting", "starting", SandboxState::Starting),
    );
    assert!(matches!(
        rollback_starting(&conn, "starting", SandboxState::Failed),
        Err(error) if error.is_invalid_query()
    ));
    assert_eq!(
        get_sandbox(&conn, "starting").unwrap().unwrap().state,
        SandboxState::Starting
    );
}

#[test]
fn mark_restore_succeeded_requires_restoring_state() {
    let conn = test_conn();
    insert_test_sandbox(
        &conn,
        TestSandboxInsert::new("restoring-sandbox", "restores", SandboxState::Restoring),
    );
    insert_test_sandbox(
        &conn,
        TestSandboxInsert::new("deleting-sandbox", "deletebx", SandboxState::Deleting),
    );

    assert!(
        mark_restore_succeeded(&conn, "restoring-sandbox", SandboxState::Paused, None).unwrap()
    );
    assert!(
        !mark_restore_succeeded(&conn, "deleting-sandbox", SandboxState::Started, None).unwrap()
    );

    let paused = get_sandbox(&conn, "restoring-sandbox").unwrap().unwrap();
    let deleting = get_sandbox(&conn, "deleting-sandbox").unwrap().unwrap();
    assert_eq!(paused.state, SandboxState::Paused);
    assert_eq!(deleting.state, SandboxState::Deleting);
}

#[test]
fn cleanup_recovery_owns_failure_clearance_not_stale_resident_completion() {
    let conn = test_conn();
    insert_test_sandbox(
        &conn,
        TestSandboxInsert::new("sandbox-1", "sandbox1", SandboxState::Creating),
    );

    mark_cleanup_failed(
        &conn,
        "sandbox-1",
        true,
        CleanupCompletion::MarkStopped,
        "cleanup failed",
    )
    .unwrap();
    assert_eq!(
        update_state_from(&conn, "sandbox-1", &edges::PAUSE_ROLLBACK).unwrap(),
        StateWriteOutcome::Refused {
            found: SandboxState::CleanupFailed,
            refusal: StateWriteRefusal::Guard(StateWriteGuard::CleanupOwned),
        }
    );
    complete_row_intent_cleanup(&conn, "sandbox-1", CleanupCompletion::MarkStopped, None).unwrap();

    let row = get_sandbox(&conn, "sandbox-1").unwrap().unwrap();
    assert_eq!(row.state, SandboxState::Stopped);
    assert!(row.failure_reason.is_none());
}

#[test]
fn quarantined_state_is_not_exited_by_generic_failure_writers() {
    let conn = test_conn();
    insert_test_sandbox(
        &conn,
        TestSandboxInsert::new("sandbox-1", "sandbox1", SandboxState::Started),
    );

    mark_quarantined(
        &conn,
        "sandbox-1",
        SandboxQuarantineSource::SbxManager,
        SandboxQuarantineReasonCode::RuntimeProbeInconsistent,
        "runtime evidence is inconsistent",
        SandboxQuarantineNextAction::RequiresOperatorAttention,
    )
    .unwrap();

    assert_eq!(
        update_state_from(&conn, "sandbox-1", &edges::PAUSE_ROLLBACK).unwrap(),
        StateWriteOutcome::Refused {
            found: SandboxState::Quarantined,
            refusal: StateWriteRefusal::Guard(StateWriteGuard::Quarantined),
        }
    );

    let row = get_sandbox(&conn, "sandbox-1").unwrap().unwrap();
    assert_eq!(row.state, SandboxState::Quarantined);
    assert_eq!(
        row.failure_reason.as_deref(),
        Some("runtime evidence is inconsistent")
    );

    state_write_fixtures::apply_edge(&conn, "sandbox-1", &edges::DELETE_ADMISSION);
    let row = get_sandbox(&conn, "sandbox-1").unwrap().unwrap();
    assert_eq!(row.state, SandboxState::Deleting);
}

#[test]
fn direct_failure_quarantine_reasons_are_expected_state_fenced() {
    let conn = test_conn();
    for (id, state, reason) in [
        (
            "pause",
            SandboxState::Pausing,
            SandboxQuarantineReasonCode::PauseFailed,
        ),
        (
            "checkpoint",
            SandboxState::Checkpointing,
            SandboxQuarantineReasonCode::CheckpointResumeFailed,
        ),
        (
            "resident-resume",
            SandboxState::Resuming,
            SandboxQuarantineReasonCode::ResidentResumeFailed,
        ),
        (
            "snapshot-rollback",
            SandboxState::Started,
            SandboxQuarantineReasonCode::SnapshotRollbackCleanupFailed,
        ),
    ] {
        insert_test_sandbox(&conn, TestSandboxInsert::new(id, "runtime1", state));
        assert!(
            mark_quarantined_from_state(
                &conn,
                id,
                state,
                SandboxQuarantineSource::SbxManager,
                reason,
                "runtime outcome is ambiguous",
                SandboxQuarantineNextAction::RequiresOperatorAttention,
            )
            .unwrap()
        );
        let row = get_sandbox(&conn, id).unwrap().unwrap();
        assert_eq!(row.state, SandboxState::Quarantined);
        assert_eq!(
            current_quarantine_reason_wire_value(&conn, id)
                .unwrap()
                .as_deref(),
            Some(reason.as_str())
        );
    }

    insert_test_sandbox(
        &conn,
        TestSandboxInsert::new("raced", "runtime2", SandboxState::Deleting),
    );
    assert!(
        !mark_quarantined_from_state(
            &conn,
            "raced",
            SandboxState::Resuming,
            SandboxQuarantineSource::SbxManager,
            SandboxQuarantineReasonCode::ResidentResumeFailed,
            "late resume error",
            SandboxQuarantineNextAction::RequiresOperatorAttention,
        )
        .unwrap()
    );
    assert_eq!(
        get_sandbox(&conn, "raced").unwrap().unwrap().state,
        SandboxState::Deleting
    );
}

// Failure paths can leave a row holding the (uid, netgtw_pid) of processes
// that cleanup already killed. Because uids are slot-derived and pids
// recycle, a later sandbox's gateway can present the same (pid, uid), so
// the auth lookup must never match rows whose state cannot host a live
// gateway. This cross-checks the SQL state filter against
// `SandboxState::can_authorize_net_gateway` for every state.
#[test]
fn net_gateway_lookup_matches_only_states_that_can_authorize_a_gateway() {
    let conn = test_conn();

    let states = [
        SandboxState::Creating,
        SandboxState::Created,
        SandboxState::Starting,
        SandboxState::Started,
        SandboxState::Stopping,
        SandboxState::Stopped,
        SandboxState::Deleting,
        SandboxState::Pausing,
        SandboxState::Paused,
        SandboxState::Resuming,
        SandboxState::Checkpointing,
        SandboxState::Suspending,
        SandboxState::Suspended,
        SandboxState::Restoring,
        SandboxState::CleanupRequired,
        SandboxState::CleanupFailed,
        SandboxState::Failed,
    ];

    for (index, state) in states.into_iter().enumerate() {
        let id = format!("sandbox-{index}");
        let uuid8 = format!("sandbo{index:02}");
        let uid = 10_000 + index as u32;
        let netgtw_pid = 4_000 + index as u32;
        insert_test_sandbox(&conn, TestSandboxInsert::new(&id, uuid8, state));
        update_netgtw_pid(&conn, &id, uid, netgtw_pid).unwrap();

        let matches = list_by_netgtw_pid_uid(&conn, netgtw_pid, uid).unwrap();
        if state.can_authorize_net_gateway() {
            assert_eq!(
                matches.len(),
                1,
                "state {state} must authorize its live net-gateway registration"
            );
            assert_eq!(matches[0].id, id);
        } else {
            assert!(
                matches.is_empty(),
                "state {state} cannot host a live net-gateway; its stale \
                 registration must not authorize a caller"
            );
        }
    }
}

fn pause_failed_test_row(conn: &Connection) -> SandboxId {
    let sandbox_id = SandboxId::new();
    let id = sandbox_id.to_string();
    insert_test_sandbox(
        conn,
        TestSandboxInsert::new(&id, "pausebx", SandboxState::Pausing),
    );
    assert!(
        mark_quarantined_from_state(
            conn,
            &id,
            SandboxState::Pausing,
            SandboxQuarantineSource::SbxManager,
            SandboxQuarantineReasonCode::PauseFailed,
            "pause response was ambiguous",
            SandboxQuarantineNextAction::RequiresOperatorAttention,
        )
        .unwrap()
    );
    sandbox_id
}

#[test]
fn pause_failed_quarantine_recovers_to_definitive_runtime_state() {
    for target in [
        PauseFailedRecoveryTarget::Started,
        PauseFailedRecoveryTarget::Paused,
    ] {
        let conn = test_conn();
        let sandbox_id = pause_failed_test_row(&conn);
        let id = sandbox_id.to_string();
        assert!(recover_pause_failed_quarantine(&conn, &sandbox_id, target).unwrap());
        let recovered: (String, Option<String>, Option<String>, Option<String>, bool) = conn
            .query_row(
                "SELECT s.state,
                        s.failure_reason,
                        history.action,
                        history.next_action,
                        NOT EXISTS (
                            SELECT 1 FROM sandbox_quarantine_state
                             WHERE sandbox_id = s.id
                        )
                   FROM sandboxes s
                   LEFT JOIN sandbox_quarantine_history history
                     ON history.sandbox_id = s.id
                    AND history.action IN ('recovered_to_started', 'recovered_to_paused')
                  WHERE s.id = ?1",
                params![id],
                |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                    ))
                },
            )
            .unwrap();
        assert_eq!(
            recovered,
            (
                target.sandbox_state().to_string(),
                None,
                Some(target.history_action().to_string()),
                None,
                true,
            )
        );
        assert!(!recover_pause_failed_quarantine(&conn, &sandbox_id, target).unwrap());
    }
}

#[test]
fn pause_failed_recovery_requires_exact_quarantine_fence() {
    for mutation in [
        "UPDATE sandbox_quarantine_state
            SET source = 'node_state_publish'
          WHERE sandbox_id = ?1",
        "UPDATE sandbox_quarantine_state
            SET reason_code = 'checkpoint_resume_failed'
          WHERE sandbox_id = ?1",
        "UPDATE sandboxes
            SET state = 'started'
          WHERE id = ?1",
    ] {
        let conn = test_conn();
        let sandbox_id = pause_failed_test_row(&conn);
        let id = sandbox_id.to_string();
        conn.execute(mutation, params![id]).unwrap();

        assert!(
            !recover_pause_failed_quarantine(
                &conn,
                &sandbox_id,
                PauseFailedRecoveryTarget::Started,
            )
            .unwrap()
        );
        assert!(
            current_quarantine_reason_wire_value(&conn, &id)
                .unwrap()
                .is_some()
        );
    }
}

// The production collision shape: a launch-failure row keeps its
// registration, then a later sandbox's gateway is assigned the same
// (pid, uid). The stale Failed row must not shadow (two-match) or
// impersonate (single-match) the live sandbox.
#[test]
fn net_gateway_lookup_ignores_stale_failed_row_sharing_pid_uid_with_live_row() {
    let conn = test_conn();
    let uid = 10_042;
    let netgtw_pid = 4_242;

    insert_test_sandbox(
        &conn,
        TestSandboxInsert::new("sandbox-failed", "sandbox1", SandboxState::Failed),
    );
    update_netgtw_pid(&conn, "sandbox-failed", uid, netgtw_pid).unwrap();

    insert_test_sandbox(
        &conn,
        TestSandboxInsert::new("sandbox-live", "sandbox2", SandboxState::Started),
    );
    update_netgtw_pid(&conn, "sandbox-live", uid, netgtw_pid).unwrap();

    let matches = list_by_netgtw_pid_uid(&conn, netgtw_pid, uid).unwrap();
    assert_eq!(
        matches.len(),
        1,
        "a stale Failed registration must not shadow the live sandbox"
    );
    assert_eq!(matches[0].id, "sandbox-live");
}

// The gateway-launch registration write is the persisted-before-returning
// invariant the launch paths rely on; an UPDATE that matched no row (the
// sandbox was deleted by concurrent cleanup mid-launch) must surface as an
// error so the launch fails instead of running an unauthorized gateway.
#[test]
fn update_netgtw_pid_errors_when_sandbox_row_is_missing() {
    let conn = test_conn();

    let error = update_netgtw_pid(&conn, "missing-sandbox", 10_042, 4_242)
        .expect_err("registration write for a deleted sandbox must error, not no-op");
    assert!(error.is_no_rows());
}

#[test]
fn replace_netgtw_pid_rejects_stale_restart_authority() {
    let conn = crate::db::test_conn();
    insert_test_sandbox(
        &conn,
        TestSandboxInsert::new("sandbox-uncommitted", "sandbox0", SandboxState::Started),
    );
    insert_test_sandbox(
        &conn,
        TestSandboxInsert::new("sandbox-live", "sandbox1", SandboxState::Started),
    );
    conn.execute(
        "UPDATE sandboxes SET uid = ?1 WHERE id = ?2",
        params![10_043, "sandbox-uncommitted"],
    )
    .unwrap();
    conn.execute(
        "UPDATE sandboxes SET uid = ?1, netgtw_pid = ?2 WHERE id = ?3",
        params![10_042, 4_242, "sandbox-live"],
    )
    .unwrap();

    replace_netgtw_pid(&conn, "sandbox-uncommitted", 10_043, None, 4_241)
        .expect("an uncommitted live launch may be adopted");
    replace_netgtw_pid(&conn, "sandbox-live", 10_042, Some(4_242), 4_243)
        .expect("current authority may commit its replacement");
    replace_netgtw_pid(&conn, "sandbox-live", 10_042, Some(4_242), 4_243)
        .expect("a stale observer may confirm the already-committed replacement");
    let error = replace_netgtw_pid(&conn, "sandbox-live", 10_042, Some(4_242), 4_244)
        .expect_err("stale restart must not overwrite the committed replacement");
    assert!(error.is_no_rows());
    assert_eq!(
        get_sandbox(&conn, "sandbox-live")
            .unwrap()
            .unwrap()
            .netgtw_pid,
        Some(4_243)
    );
    assert_eq!(
        get_sandbox(&conn, "sandbox-uncommitted")
            .unwrap()
            .unwrap()
            .netgtw_pid,
        Some(4_241)
    );
}

#[test]
fn update_spaced_host_pid_is_guarded_by_persisted_owner() {
    let conn = test_conn();
    insert_test_sandbox(
        &conn,
        TestSandboxInsert::new("sandbox-live", "sandbox1", SandboxState::Started),
    );
    conn.execute(
        "UPDATE sandboxes SET uid = ?1, spaced_host_pid = ?2 WHERE id = ?3",
        params![10_042, 4_242, "sandbox-live"],
    )
    .unwrap();

    update_spaced_host_pid(&conn, "sandbox-live", 10_042, Some(4_242), 4_243)
        .expect("current owner may persist its replacement");
    update_spaced_host_pid(&conn, "sandbox-live", 10_042, Some(4_242), 4_243)
        .expect("a stale observer may confirm the already-committed replacement");
    let row = get_sandbox(&conn, "sandbox-live").unwrap().unwrap();
    assert_eq!(row.spaced_host_pid, Some(4_243));

    let error = update_spaced_host_pid(&conn, "sandbox-live", 10_043, Some(4_243), 4_244)
        .expect_err("stale owner must not replace endpoint authority");
    assert!(error.is_no_rows());
    let row = get_sandbox(&conn, "sandbox-live").unwrap().unwrap();
    assert_eq!(row.spaced_host_pid, Some(4_243));
}

#[test]
fn update_spaced_host_pid_rejects_stale_restart_for_same_owner() {
    let conn = test_conn();
    insert_test_sandbox(
        &conn,
        TestSandboxInsert::new("sandbox-live", "sandbox1", SandboxState::Started),
    );
    conn.execute(
        "UPDATE sandboxes SET uid = ?1, spaced_host_pid = ?2 WHERE id = ?3",
        params![10_042, 4_242, "sandbox-live"],
    )
    .unwrap();

    conn.execute(
        "UPDATE sandboxes SET spaced_host_pid = ?1 WHERE id = ?2",
        params![5_000, "sandbox-live"],
    )
    .unwrap();
    let error = update_spaced_host_pid(&conn, "sandbox-live", 10_042, Some(4_242), 4_243)
        .expect_err("stale restart must not overwrite a newer activation for the same uid");
    assert!(error.is_no_rows());
    let row = get_sandbox(&conn, "sandbox-live").unwrap().unwrap();
    assert_eq!(row.spaced_host_pid, Some(5_000));
}

#[test]
fn cleanup_reconciliation_does_not_target_started_rows_without_owned_intent() {
    let conn = test_conn();
    let sandbox_id = SandboxId::new();
    let sandbox_id_string = sandbox_id.to_string();
    let runtime_label = SandboxRuntimeLabel::from_sandbox_id(sandbox_id).to_string();
    insert_test_sandbox(
        &conn,
        TestSandboxInsert::new(&sandbox_id_string, runtime_label, SandboxState::Started),
    );
    conn.execute(
        "UPDATE sandboxes SET uid = 10000, slot = 0, pid = ?1 WHERE id = ?2",
        params![std::process::id(), &sandbox_id_string],
    )
    .unwrap();

    let row = get_sandbox(&conn, &sandbox_id_string).unwrap().unwrap();
    assert_eq!(cleanup_intent_for_startup_recovery(&row), None);

    set_cleanup_intent(
        &conn,
        &sandbox_id_string,
        true,
        CleanupCompletion::Quarantine,
    )
    .unwrap();

    let row = get_sandbox(&conn, &sandbox_id_string).unwrap().unwrap();
    assert_eq!(cleanup_intent_for_startup_recovery(&row), None);
}

#[test]
fn startup_resident_settlement_keeps_only_current_receipts_atomically() {
    #[derive(Clone, Copy, Debug)]
    enum Interruption {
        None,
        Superseded,
        PublicationFailure,
    }
    use Interruption::{None as Uninterrupted, PublicationFailure, Superseded};
    use SandboxState::{Checkpointing, Paused, Quarantined, Resuming, Started};

    for (initial, receipt_target, target, interruption, keep_receipt) in [
        (Started, Some("started"), Quarantined, Uninterrupted, false),
        (Started, None, Quarantined, Uninterrupted, false),
        (
            Started,
            Some("quarantined"),
            Quarantined,
            Uninterrupted,
            false,
        ),
        (Resuming, Some("started"), Started, Uninterrupted, true),
        (Resuming, Some("paused"), Started, Uninterrupted, false),
        (Checkpointing, Some("started"), Started, Uninterrupted, true),
        (Resuming, Some("paused"), Paused, Uninterrupted, true),
        (Resuming, Some("started"), Paused, Uninterrupted, false),
        (Started, Some("started"), Quarantined, Superseded, true),
        (
            Started,
            Some("started"),
            Quarantined,
            PublicationFailure,
            true,
        ),
    ] {
        let conn = test_conn();
        let sandbox_id = SandboxId::new();
        let id = sandbox_id.to_string();
        insert_test_sandbox(
            &conn,
            TestSandboxInsert::new(
                &id,
                SandboxRuntimeLabel::from_sandbox_id(sandbox_id).to_string(),
                initial,
            ),
        );
        if let Some(receipt_target) = receipt_target {
            // Inject corrupt historical evidence that the production schema
            // now rejects. Recovery must still invalidate the whole tuple.
            conn.pragma_update(None, "ignore_check_constraints", true)
                .unwrap();
            conn.execute(
                "UPDATE sandboxes SET transition_receipt_id = ?1,
                     transition_receipt_kind = 'activation_completed',
                     transition_receipt_target_state = ?2,
                     transition_receipt_execution_generation = 7,
                     transition_receipt_activation_attempt_id = ?3,
                     transition_receipt_reason_code = ?4 WHERE id = ?5",
                params![
                    Uuid::now_v7().to_string(),
                    receipt_target,
                    Uuid::now_v7().to_string(),
                    // Also exercise a malformed tuple with every field set:
                    // quarantine must invalidate even a matching target label.
                    (receipt_target == "quarantined").then_some("stale-reason"),
                    id
                ],
            )
            .unwrap();
            conn.pragma_update(None, "ignore_check_constraints", false)
                .unwrap();
        }
        let read_receipt = || {
            conn.query_row(
                "SELECT transition_receipt_id, transition_receipt_kind,
                     transition_receipt_target_state, transition_receipt_execution_generation,
                     transition_receipt_activation_attempt_id, transition_receipt_reason_code
                 FROM sandboxes WHERE id = ?1",
                [&id],
                |row| {
                    (0..6)
                        .map(|column| row.get::<_, rusqlite::types::Value>(column))
                        .collect::<rusqlite::Result<Vec<_>>>()
                },
            )
            .unwrap()
        };
        let observed = get_sandbox(&conn, &id).unwrap().unwrap();
        let before = read_receipt();
        match interruption {
            Uninterrupted => {}
            Superseded => {
                conn.execute(
                    "UPDATE sandboxes SET process_env_json = '{\"NEW\":\"1\"}' WHERE id = ?1",
                    [&id],
                )
                .unwrap();
            }
            PublicationFailure => conn.execute_batch(
                "CREATE TRIGGER reject_quarantine_publication BEFORE INSERT ON sandbox_quarantine_state
                 BEGIN SELECT RAISE(ABORT, 'injected quarantine publication failure'); END;",
            ).unwrap(),
        }
        let settlement = match target {
            Started => StartupResidentSettlement::Started,
            Paused => StartupResidentSettlement::Paused,
            Quarantined => StartupResidentSettlement::Quarantined {
                reason_code: SandboxQuarantineReasonCode::RuntimeProbeInconsistent,
                message: "started sandbox has no live registered VM pid",
            },
            _ => unreachable!(),
        };
        let result = settle_startup_resident(&conn, &observed, settlement);
        let committed = matches!(interruption, Uninterrupted);
        match interruption {
            Uninterrupted => assert!(result.unwrap()),
            Superseded => assert!(!result.unwrap()),
            PublicationFailure => {
                result.expect_err("publication failure");
            }
        }
        let row = get_sandbox(&conn, &id).unwrap().unwrap();
        assert_eq!(row.state, if committed { target } else { initial });
        assert_eq!(
            current_quarantine_reason_wire_value(&conn, &id)
                .unwrap()
                .is_some(),
            committed && target == Quarantined,
        );
        assert_eq!(
            read_receipt(),
            if keep_receipt {
                before
            } else {
                vec![rusqlite::types::Value::Null; 6]
            },
            "{initial:?} -> {target:?}, receipt {receipt_target:?}, {interruption:?}",
        );
    }
}

#[test]
fn startup_recovery_reconciles_interrupted_states_without_runtime_fields() {
    let conn = test_conn();

    let states = [
        SandboxState::Creating,
        SandboxState::Created,
        SandboxState::Starting,
        SandboxState::Started,
        SandboxState::Stopping,
        SandboxState::Stopped,
        SandboxState::Deleting,
        SandboxState::Pausing,
        SandboxState::Paused,
        SandboxState::Resuming,
        SandboxState::Checkpointing,
        SandboxState::Suspending,
        SandboxState::Suspended,
        SandboxState::Restoring,
        SandboxState::Failed,
    ];

    let sandbox_ids = states
        .into_iter()
        .map(|state| {
            let sandbox_id = SandboxId::new();
            let id = sandbox_id.to_string();
            let runtime_label = SandboxRuntimeLabel::from_sandbox_id(sandbox_id).to_string();
            (state, id, runtime_label)
        })
        .collect::<Vec<_>>();

    for (state, id, runtime_label) in &sandbox_ids {
        insert_test_sandbox(&conn, TestSandboxInsert::new(id, runtime_label, *state));
        if *state == SandboxState::Started {
            conn.execute(
                "UPDATE sandboxes SET pid = ?1 WHERE id = ?2",
                params![std::process::id(), id],
            )
            .unwrap();
        }
    }

    let recovered = recover_interrupted_startup_states(&conn).unwrap();
    let current_pid_has_proc =
        std::path::Path::new(&format!("/proc/{}", std::process::id())).exists();

    for (state, id, _) in &sandbox_ids {
        let row = get_sandbox(&conn, id).unwrap().unwrap();
        let record = recovered.iter().find(|record| record.id == *id);
        let expected_cleanup = match state {
            SandboxState::Stopping => Some((true, CleanupCompletion::MarkStopped)),
            SandboxState::Suspending => None,
            SandboxState::Deleting => Some((false, CleanupCompletion::DeleteRow)),
            _ => None,
        };

        if let Some((keep_filesystem, completion)) = expected_cleanup {
            let record = record.expect("expected startup recovery record");
            assert_eq!(record.previous_state, *state);
            assert_eq!(
                record.outcome,
                StartupRecoveryOutcome::CleanupRequired {
                    keep_filesystem,
                    completion
                }
            );
            assert_eq!(row.state, SandboxState::CleanupRequired);
            assert_eq!(row.cleanup_keep_filesystem, Some(keep_filesystem));
            assert_eq!(row.cleanup_completion, Some(completion));
            assert!(row.failure_reason.as_deref().is_some_and(|reason| {
                reason.contains(state.as_str()) || *state == SandboxState::Started
            }));
        } else {
            match state {
                SandboxState::Started if !current_pid_has_proc => {
                    assert_eq!(row.state, SandboxState::Started);
                    assert!(row.cleanup_completion.is_none());
                    assert_eq!(record.unwrap().outcome, StartupRecoveryOutcome::Deferred);
                }
                SandboxState::Creating
                | SandboxState::Starting
                | SandboxState::Pausing
                | SandboxState::Resuming
                | SandboxState::Checkpointing
                | SandboxState::Suspending => {
                    let record = record.expect("expected deferred startup recovery record");
                    assert_eq!(record.previous_state, *state);
                    assert_eq!(record.outcome, StartupRecoveryOutcome::Deferred);
                    assert_eq!(row.state, *state);
                }
                SandboxState::Restoring => {
                    let record = record.expect("expected startup recovery record");
                    assert_eq!(record.previous_state, *state);
                    assert_eq!(
                        record.outcome,
                        StartupRecoveryOutcome::State(SandboxState::Suspended)
                    );
                    assert_eq!(row.state, SandboxState::Suspended);
                }
                _ => {
                    assert!(record.is_none(), "did not expect {state:?} to be recovered");
                    assert_eq!(row.state, *state);
                }
            }
        }
    }
}

#[test]
fn startup_missing_runtime_defers_fenced_rows_and_quarantines_missing_identity() {
    for runtime_fields in [false, true] {
        let conn = test_conn();
        let id = SandboxId::new().to_string();
        insert_test_sandbox(
            &conn,
            TestSandboxInsert::new(&id, "missing1", SandboxState::Started),
        );
        if runtime_fields {
            conn.execute(
                "UPDATE sandboxes SET pid = 4294967295, uid = 10000, slot = 7 WHERE id = ?1",
                [&id],
            )
            .unwrap();
        }
        let recovered = recover_interrupted_startup_states(&conn).unwrap();
        let row = get_sandbox(&conn, &id).unwrap().unwrap();
        // A recorded PID plus the persisted execution fence belongs to the
        // service's process-exit reconciler, including after a manager restart.
        let expected = if runtime_fields {
            SandboxState::Started
        } else {
            SandboxState::Quarantined
        };
        assert_eq!(row.state, expected);
        assert_eq!(
            recovered[0].outcome,
            if runtime_fields {
                StartupRecoveryOutcome::Deferred
            } else {
                StartupRecoveryOutcome::State(SandboxState::Quarantined)
            }
        );
        assert_eq!(row.pid, runtime_fields.then_some(u32::MAX));
        assert_eq!(row.uid, runtime_fields.then_some(10000));
        assert!(row.cleanup_completion.is_none());
        assert_eq!(
            current_quarantine_reason_wire_value(&conn, &id)
                .unwrap()
                .as_deref(),
            (!runtime_fields)
                .then_some(SandboxQuarantineReasonCode::RuntimeProbeInconsistent.as_str())
        );
        let again = recover_interrupted_startup_states(&conn).unwrap();
        if runtime_fields {
            assert_eq!(again, recovered);
        } else {
            assert!(again.is_empty());
        }
    }
}

#[test]
fn startup_recovery_defers_staging_failed_restore() {
    let conn = test_conn();
    let sandbox_id = SandboxId::new();
    let id = sandbox_id.to_string();
    let runtime_label = SandboxRuntimeLabel::from_sandbox_id(sandbox_id).to_string();
    insert_test_sandbox(
        &conn,
        TestSandboxInsert::new(&id, runtime_label, SandboxState::Restoring),
    );
    conn.execute(
        "UPDATE sandboxes
         SET restore_origin = 'remote',
             restore_phase = 'artifact_staging',
             restore_storage_uri = 's3://snapshots/suspend/test?region=us-east-1'
         WHERE id = ?1",
        params![&id],
    )
    .unwrap();
    let staging_failure = "cross-node restore artifact staging failed: test";
    conn.execute(
        "UPDATE sandboxes SET failure_reason = ?1 WHERE id = ?2",
        params![staging_failure, &id],
    )
    .unwrap();

    let recovered = recover_interrupted_startup_states(&conn).unwrap();

    assert_eq!(
        recovered,
        vec![StartupRecoveryRecord {
            id: id.clone(),
            previous_state: SandboxState::Restoring,
            outcome: StartupRecoveryOutcome::Deferred,
        }]
    );
    let row = get_sandbox(&conn, &id).unwrap().unwrap();
    assert_eq!(row.state, SandboxState::Restoring);
    assert_eq!(row.failure_reason.as_deref(), Some(staging_failure));
}

#[test]
fn startup_recovery_uses_runtime_fields_to_choose_cleanup_destination() {
    let conn = test_conn();

    let cases = [
        (
            SandboxState::Starting,
            CleanupCompletion::MarkStopped,
            "starting-runtime",
        ),
        (
            SandboxState::Restoring,
            CleanupCompletion::MarkSuspended,
            "restoring-runtime",
        ),
    ];

    for (state, completion, label) in cases {
        let sandbox_id = SandboxId::new();
        let id = sandbox_id.to_string();
        let runtime_label = SandboxRuntimeLabel::from_sandbox_id(sandbox_id).to_string();
        insert_test_sandbox(&conn, TestSandboxInsert::new(&id, runtime_label, state));
        conn.execute(
            "UPDATE sandboxes SET uid = 10000, slot = 0, pid = 1234 WHERE id = ?1",
            params![&id],
        )
        .unwrap();

        let recovered = recover_interrupted_startup_states(&conn).unwrap();
        let record = recovered
            .iter()
            .find(|record| record.id == id)
            .unwrap_or_else(|| panic!("expected recovery record for {label}"));
        assert_eq!(record.previous_state, state);
        assert_eq!(
            record.outcome,
            StartupRecoveryOutcome::CleanupRequired {
                keep_filesystem: true,
                completion
            }
        );

        let row = get_sandbox(&conn, &id).unwrap().unwrap();
        assert_eq!(row.state, SandboxState::CleanupRequired);
        assert_eq!(row.cleanup_keep_filesystem, Some(true));
        assert_eq!(row.cleanup_completion, Some(completion));
    }
}

// ---------------------------------------------------------------------------
// Exact-claim fence coverage matrix
//
// Every statement that mutates a claimed legacy-recovery episode has to pin the
// same set of conjuncts, or a stale claimant can write to an episode that has
// moved on. Each statement used to spell that predicate out by hand, and four
// review rounds each found a different statement missing a different conjunct —
// one defect class, discovered one statement at a time.
//
// This matrix is the missing artifact: for every statement, for every conjunct,
// it perturbs exactly that condition and asserts the statement declines to
// write. The expectations are declared per statement below, not derived from the
// statements themselves, so widening a fence by accident fails a cell.
//
// A cell can be blocked either by the shared fence or by a statement's own
// precondition. The assertion is about observable behavior — whether a perturbed
// episode gets written to — not about which clause did the blocking.
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum FencedClaimStatement {
    RenewClaim,
    ClaimIsCurrent,
    RecordNoProgress,
    SetOrphanRecoveryGeneration,
    PublishOrphanRecoveredArtifact,
    AdoptCleanupEpisode,
    MarkCrossNodeStagingFailed,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum FenceConjunct {
    QuarantineId,
    Source,
    ReasonCode,
    NextActionOwner,
    ClaimToken,
    AttemptCount,
    LeaseIsLive,
    SandboxIsActivatable,
    NoCleanupEpisode,
}

impl FenceConjunct {
    const ALL: [Self; 9] = [
        Self::QuarantineId,
        Self::Source,
        Self::ReasonCode,
        Self::NextActionOwner,
        Self::ClaimToken,
        Self::AttemptCount,
        Self::LeaseIsLive,
        Self::SandboxIsActivatable,
        Self::NoCleanupEpisode,
    ];

    /// Break exactly this condition for `sandbox_id`, leaving the rest intact.
    fn perturb(self, conn: &Connection, sandbox_id: SandboxId) {
        let id = sandbox_id.to_string();
        match self {
            Self::QuarantineId => {
                conn.execute(
                    "UPDATE sandbox_quarantine_state SET quarantine_id = ?2 WHERE sandbox_id = ?1",
                    params![id, Uuid::now_v7().to_string()],
                )
                .unwrap();
            }
            Self::Source => {
                conn.execute(
                    "UPDATE sandbox_quarantine_state SET source = ?2 WHERE sandbox_id = ?1",
                    params![id, SandboxQuarantineSource::NodeStatePublish.as_str()],
                )
                .unwrap();
            }
            Self::ReasonCode => {
                conn.execute(
                    "UPDATE sandbox_quarantine_state SET reason_code = ?2 WHERE sandbox_id = ?1",
                    params![id, SandboxQuarantineReasonCode::PauseFailed.as_str()],
                )
                .unwrap();
            }
            Self::NextActionOwner => {
                conn.execute(
                    "UPDATE sandbox_quarantine_state SET next_action_owner = 'future_owner' WHERE sandbox_id = ?1",
                    params![id],
                )
                .unwrap();
            }
            Self::ClaimToken => {
                conn.execute(
                    "UPDATE sandbox_quarantine_state SET claim_token = ?2 WHERE sandbox_id = ?1",
                    params![id, Uuid::now_v7().to_string()],
                )
                .unwrap();
            }
            Self::AttemptCount => {
                conn.execute(
                    "UPDATE sandbox_quarantine_state SET attempt_count = attempt_count + 5 WHERE sandbox_id = ?1",
                    params![id],
                )
                .unwrap();
            }
            Self::LeaseIsLive => {
                conn.execute(
                    "UPDATE sandbox_quarantine_state SET claim_expires_at = '1' WHERE sandbox_id = ?1",
                    params![id],
                )
                .unwrap();
            }
            Self::SandboxIsActivatable => {
                conn.execute(
                    "UPDATE sandboxes SET state = 'stopped' WHERE id = ?1",
                    params![id],
                )
                .unwrap();
            }
            Self::NoCleanupEpisode => {
                conn.execute(
                    "INSERT INTO sandbox_cleanup_episodes (
                         sandbox_id, cleanup_id, completion, keep_filesystem, phase,
                         activation_state, created_at
                     ) VALUES (?1, ?2, 'mark_suspended', TRUE, 'armed', 'restoring', 0)",
                    params![id, SandboxCleanupEpisodeId::new().to_string()],
                )
                .unwrap();
            }
        }
    }
}

impl FencedClaimStatement {
    const ALL: [Self; 7] = [
        Self::RenewClaim,
        Self::ClaimIsCurrent,
        Self::RecordNoProgress,
        Self::SetOrphanRecoveryGeneration,
        Self::PublishOrphanRecoveredArtifact,
        Self::AdoptCleanupEpisode,
        Self::MarkCrossNodeStagingFailed,
    ];

    /// Whether a perturbation of `conjunct` must stop this statement writing.
    ///
    /// `false` marks a deliberate relaxation, and each one carries its reason.
    /// Everything else is required: these statements commit under a claim, so a
    /// claimant whose episode moved on must not be able to write.
    fn must_block(self, conjunct: FenceConjunct) -> bool {
        match (self, conjunct) {
            // Cleanup contention is resolved before any recovery operation is
            // admitted, so this release deliberately ignores the environment it
            // is releasing into: cleanup may have won the fence moments after
            // the claim was taken, and the sandbox may already be moving. The
            // claim token still has to match, and a token is minted per claim,
            // so an expired-then-reclaimed episode is caught by the token.
            // The no-progress write relaxes all three environmental guards, and that is
            // the point: it records why an attempt made no progress, and a lease that
            // lapsed, a sandbox that moved or a cleanup owner that arrived are the very
            // things it has to be able to report. Ownership is still pinned by the seven
            // identity conjuncts above.
            (Self::RecordNoProgress, FenceConjunct::LeaseIsLive) => false,
            (Self::RecordNoProgress, FenceConjunct::SandboxIsActivatable) => false,
            (Self::RecordNoProgress, FenceConjunct::NoCleanupEpisode) => false,
            // Every other pairing must block.
            _ => true,
        }
    }

    /// Build a sandbox and a live claim in the shape this statement requires.
    /// The production function this statement stands for, so the inventory can be
    /// checked against the source rather than trusted.
    fn function_name(self) -> &'static str {
        match self {
            Self::RenewClaim => "renew_quarantine_recovery_claim",
            Self::ClaimIsCurrent => "quarantine_recovery_claim_is_current",
            Self::RecordNoProgress => "record_quarantine_recovery_no_progress",
            Self::SetOrphanRecoveryGeneration => "set_orphan_quarantine_recovery_generation",
            Self::PublishOrphanRecoveredArtifact => "publish_orphan_quarantine_recovered_artifact",
            Self::AdoptCleanupEpisode => "adopt_quarantine_cleanup_episode",
            Self::MarkCrossNodeStagingFailed => {
                "mark_cross_node_restore_staging_failed_under_recovery_claim"
            }
        }
    }

    fn fixture(self, conn: &Connection) -> (SandboxId, ClaimedQuarantineRecoveryCandidate) {
        match self {
            // These two commit the orphan restore, so they require a sandbox
            // mid-restore with no local artifact.
            Self::SetOrphanRecoveryGeneration
            | Self::PublishOrphanRecoveredArtifact
            | Self::MarkCrossNodeStagingFailed => {
                let sandbox_id = setup_quarantine_recovery_episode(
                    conn,
                    SandboxQuarantineReasonCode::OrphanedSandboxRowMissing,
                );
                conn.execute(
                    "UPDATE sandboxes
                        SET state = 'restoring',
                            space_api_route_execution_generation = 7
                      WHERE id = ?1",
                    params![sandbox_id.to_string()],
                )
                .unwrap();
                let [candidate] = scan_quarantine_recovery_candidates(conn)
                    .unwrap()
                    .candidates
                    .try_into()
                    .unwrap();
                let claimed = claim_quarantine_recovery_candidate(
                    conn,
                    &candidate,
                    PlanRejectionRunEffect::Leaves,
                )
                .unwrap()
                .expect("orphan episode should be claimable");
                (sandbox_id, claimed)
            }
            // The rest work against a quarantined sandbox holding an exact
            // full-suspend artifact.
            _ => {
                let (sandbox_id, candidate) = setup_quarantine_recovery_candidate(conn);
                let claimed = claim_quarantine_recovery_candidate(
                    conn,
                    &candidate,
                    PlanRejectionRunEffect::Leaves,
                )
                .unwrap()
                .expect("artifact episode should be claimable");
                (sandbox_id, claimed)
            }
        }
    }

    /// Run the statement and report whether it wrote anything.
    fn applies(self, conn: &Connection, claimed: &ClaimedQuarantineRecoveryCandidate) -> bool {
        match self {
            Self::RenewClaim => renew_quarantine_recovery_claim(conn, claimed).unwrap(),
            Self::ClaimIsCurrent => quarantine_recovery_claim_is_current(conn, claimed).unwrap(),
            Self::RecordNoProgress => matches!(
                record_quarantine_recovery_no_progress(
                    conn,
                    claimed,
                    QuarantineRecoveryNoProgressReason::PlanIneligible,
                )
                .unwrap(),
                QuarantineRecoveryNoProgressOutcome::RetryScheduled { .. }
            ),
            Self::SetOrphanRecoveryGeneration => set_orphan_quarantine_recovery_generation(
                conn,
                claimed,
                ExecutionGeneration::new(8).unwrap(),
            )
            .unwrap(),
            Self::PublishOrphanRecoveredArtifact => publish_orphan_quarantine_recovered_artifact(
                conn,
                claimed,
                Some(LocalArtifactId::new()),
                "/space/cache/sandboxes/test/snapshot",
                ExecutionGeneration::new(7).unwrap(),
                SandboxCleanupEpisodeId::new(),
            )
            .unwrap(),
            Self::MarkCrossNodeStagingFailed => {
                mark_cross_node_restore_staging_failed_under_recovery_claim(
                    conn,
                    "cross-node restore artifact staging failed",
                    claimed,
                )
                .unwrap()
            }
            Self::AdoptCleanupEpisode => matches!(
                adopt_quarantine_cleanup_episode(
                    conn,
                    claimed,
                    // `fixture` routes this statement to the artifact branch, so the
                    // witness always builds; the `expect` says so rather than
                    // papering over a restore-branch fixture with a random id.
                    adoption_witness(claimed),
                    ExecutionGeneration::new(7).unwrap(),
                    SandboxCleanupEpisodeId::new(),
                    &ExecutionMetadata::initial_firecracker(Arch::Aarch64, vec![]),
                    "/space/cache/sandboxes/test/snapshot",
                )
                .unwrap(),
                QuarantineAdoptionOutcome::Adopted
            ),
        }
    }
}

#[test]
fn every_claim_fenced_statement_pins_every_conjunct() {
    let mut failures = Vec::new();
    for statement in FencedClaimStatement::ALL {
        // Control: with nothing perturbed the statement must actually write,
        // otherwise a blocked cell below would prove nothing.
        let conn = crate::db::test_conn();
        let (_, claimed) = statement.fixture(&conn);
        assert!(
            statement.applies(&conn, &claimed),
            "{statement:?} did not write against an untouched live claim; \
             its fixture is wrong and every blocked cell for it is vacuous"
        );

        for conjunct in FenceConjunct::ALL {
            let conn = crate::db::test_conn();
            let (sandbox_id, claimed) = statement.fixture(&conn);
            conjunct.perturb(&conn, sandbox_id);
            let wrote = statement.applies(&conn, &claimed);
            let must_block = statement.must_block(conjunct);
            if wrote == must_block {
                failures.push(format!(
                    "{statement:?} + {conjunct:?}: wrote={wrote}, expected wrote={}",
                    !must_block
                ));
            }
        }
    }
    assert!(
        failures.is_empty(),
        "exact-claim fence coverage gaps:\n  {}",
        failures.join("\n  ")
    );
}

// ---------------------------------------------------------------------------
// Durable-state invariants
//
// Per-case assertions only check the cases someone thought of. These hold after
// every operation regardless of which statement ran, so a writer that breaks one
// is caught even when no test targets that writer. Wire them in after any call
// that mutates quarantine or cleanup state.
// ---------------------------------------------------------------------------

/// Assert every durable invariant the quarantine-recovery path must preserve.
/// Not applicable after `adopt_quarantine_cleanup_episode`. That statement moves
/// the sandbox to `cleanup_required` and leaves the quarantine row in place —
/// retirement belongs to the cleanup episode, via `retire_quarantine_recovery`
/// when it completes — so invariant 1 below is legitimately false in that window
/// and this helper would report a defect that is not one. The adoption path is
/// covered by the cleanup episode's own assertions instead.
pub(crate) fn assert_quarantine_invariants(conn: &Connection, context: &str) {
    // 1. A quarantine row may only exist for a sandbox that is still in a
    //    quarantine-bearing state. A row left behind after the sandbox moved on
    //    is invisible to the candidate scan (which filters on those states), so
    //    it can never be retired and the sandbox reads as quarantined forever.
    let orphaned: Vec<String> = conn
        .prepare(
            "SELECT qs.sandbox_id || ' state=' || s.state
               FROM sandbox_quarantine_state qs
               JOIN sandboxes s ON s.id = qs.sandbox_id
              WHERE s.state NOT IN ('quarantined', 'restoring')",
        )
        .unwrap()
        .query_map([], |row| row.get(0))
        .unwrap()
        .map(Result::unwrap)
        .collect();
    assert!(
        orphaned.is_empty(),
        "{context}: quarantine rows survive for sandboxes that left quarantine, \
         so the candidate scan can never see them again: {orphaned:?}"
    );

    // 2. A quarantine row with no sandbox row at all is unreachable by every
    //    query in this module, all of which join sandboxes.
    let dangling: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM sandbox_quarantine_state qs
              WHERE NOT EXISTS (SELECT 1 FROM sandboxes s WHERE s.id = qs.sandbox_id)",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(
        dangling, 0,
        "{context}: quarantine rows without a sandbox row"
    );

    // 3. A lease is only meaningful with the token and owner that identify it.
    //    Any partial combination is a state no writer should be able to produce,
    //    and it is what makes a liveness probe return SQL NULL instead of a
    //    decision.
    let partial: Vec<String> = conn
        .prepare(
            "SELECT sandbox_id
                    || ' owner=' || COALESCE(next_action_owner, '<null>')
                    || ' token=' || COALESCE(claim_token, '<null>')
                    || ' expires=' || COALESCE(claim_expires_at, '<null>')
               FROM sandbox_quarantine_state
              WHERE (claim_token IS NULL) != (claim_expires_at IS NULL)
                 OR (claim_token IS NOT NULL AND next_action_owner IS NULL)",
        )
        .unwrap()
        .query_map([], |row| row.get(0))
        .unwrap()
        .map(Result::unwrap)
        .collect();
    assert!(
        partial.is_empty(),
        "{context}: partial lease state; a token without an expiry, an expiry \
         without a token, or a held claim with no owner: {partial:?}"
    );

    // 4. attempt_count is the backoff input. A negative value cannot be
    //    produced by the shift arithmetic and would invert the curve.
    let negative: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM sandbox_quarantine_state WHERE attempt_count < 0",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(negative, 0, "{context}: negative attempt_count");
}

#[test]
fn publishing_a_recovered_orphan_leaves_no_quarantine_row() {
    let conn = crate::db::test_conn();
    let (sandbox_id, claimed) = FencedClaimStatement::PublishOrphanRecoveredArtifact.fixture(&conn);
    assert_quarantine_invariants(&conn, "before publish");

    assert!(
        publish_orphan_quarantine_recovered_artifact(
            &conn,
            &claimed,
            Some(LocalArtifactId::new()),
            "/space/cache/sandboxes/test/snapshot",
            ExecutionGeneration::new(7).unwrap(),
            SandboxCleanupEpisodeId::new(),
        )
        .unwrap(),
        "the fixture's claim should publish"
    );

    // The sandbox is now `suspended`, so the candidate scan can no longer see
    // this sandbox at all. If the quarantine row survived, nothing could ever
    // retire it and every reader of quarantine state would keep reporting the
    // sandbox as quarantined.
    let state: String = conn
        .query_row(
            "SELECT state FROM sandboxes WHERE id = ?1",
            params![sandbox_id.to_string()],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(state, "suspended");
    assert_quarantine_invariants(&conn, "after publishing a recovered orphan");
}

#[test]
fn quarantine_recovery_new_episode_preserves_a_foreign_owners_live_claim() {
    let conn = crate::db::test_conn();
    let sandbox_id = setup_quarantine_recovery_episode(
        &conn,
        SandboxQuarantineReasonCode::OrphanedSandboxRowMissing,
    );
    // A newer release owns this episode and holds a live lease on it. Nothing in
    // this release may clear it: the other owner's operation is still running,
    // and its retry accounting is not ours to reset.
    conn.execute(
        "UPDATE sandbox_quarantine_state
            SET next_action_owner = 'future_owner',
                claim_token = ?2,
                claim_expires_at = ?3,
                attempt_count = 4
          WHERE sandbox_id = ?1",
        params![
            sandbox_id.to_string(),
            Uuid::now_v7().to_string(),
            (now_epoch_i64().unwrap() + 900).to_string()
        ],
    )
    .unwrap();
    conn.execute(
        "UPDATE sandboxes SET state = 'restoring' WHERE id = ?1",
        params![sandbox_id.to_string()],
    )
    .unwrap();
    let before = quarantine_claim_state(&conn, sandbox_id);

    mark_quarantined(
        &conn,
        &sandbox_id.to_string(),
        SandboxQuarantineSource::SbxManager,
        SandboxQuarantineReasonCode::RestorePrepareFailed,
        "re-quarantined while a newer release holds a live claim",
        SandboxQuarantineNextAction::RequiresOperatorAttention,
    )
    .unwrap();

    assert_eq!(
        quarantine_claim_state(&conn, sandbox_id),
        before,
        "a newer release's live claim must survive a replacement episode"
    );
    assert_quarantine_invariants(&conn, "after re-marking under a foreign live claim");
}

#[test]
fn quarantine_recovery_new_episode_resets_an_unclaimed_episode() {
    let conn = crate::db::test_conn();
    // The ordinary case, and the one every other re-mark test skips by claiming
    // first: an episode nobody ever claimed, so owner and expiry are both NULL.
    // Each arm of the liveness predicate has to yield a decision for that row —
    // an unguarded `owner = ?` against NULL makes the whole predicate SQL NULL,
    // which fails `row.get::<bool>` and aborts the quarantine transaction.
    let sandbox_id = setup_quarantine_recovery_episode(
        &conn,
        SandboxQuarantineReasonCode::OrphanedSandboxRowMissing,
    );
    assert_eq!(
        quarantine_claim_state(&conn, sandbox_id),
        (None, 0, None, None, None),
        "precondition: the episode has never been claimed"
    );
    conn.execute(
        "UPDATE sandboxes SET state = 'restoring' WHERE id = ?1",
        params![sandbox_id.to_string()],
    )
    .unwrap();

    mark_quarantined(
        &conn,
        &sandbox_id.to_string(),
        SandboxQuarantineSource::SbxManager,
        SandboxQuarantineReasonCode::OrphanedSandboxRowMissing,
        "re-quarantined an episode that was never claimed",
        SandboxQuarantineNextAction::RequiresOperatorAttention,
    )
    .expect("an unclaimed episode must not abort the quarantine transaction");

    assert_eq!(
        quarantine_claim_state(&conn, sandbox_id),
        (None, 0, None, None, None),
        "an unclaimed episode has nothing to preserve and stays reset"
    );
    assert_eq!(
        scan_quarantine_recovery_candidates(&conn)
            .unwrap()
            .candidates
            .len(),
        1,
        "the replacement episode is due immediately"
    );
    assert_quarantine_invariants(&conn, "after re-marking an unclaimed episode");
}

#[test]
fn quarantine_recovery_new_episode_survives_a_lease_without_an_owner() {
    let conn = crate::db::test_conn();
    let sandbox_id = setup_quarantine_recovery_episode(
        &conn,
        SandboxQuarantineReasonCode::OrphanedSandboxRowMissing,
    );
    // An expiry with no owner should not be producible, but the liveness probe
    // must still yield a decision rather than SQL NULL: a NULL there fails
    // `row.get::<bool>`, which aborts the whole transaction and leaves a sandbox
    // that must be quarantined un-quarantined.
    conn.execute(
        "UPDATE sandbox_quarantine_state
            SET next_action_owner = NULL,
                claim_token = NULL,
                claim_expires_at = ?2
          WHERE sandbox_id = ?1",
        params![
            sandbox_id.to_string(),
            (now_epoch_i64().unwrap() + 900).to_string()
        ],
    )
    .unwrap();
    // A sandbox left in `quarantined` keeps its episode and never reaches the
    // new-episode probe this test is about.
    conn.execute(
        "UPDATE sandboxes SET state = 'restoring' WHERE id = ?1",
        params![sandbox_id.to_string()],
    )
    .unwrap();

    mark_quarantined(
        &conn,
        &sandbox_id.to_string(),
        SandboxQuarantineSource::SbxManager,
        SandboxQuarantineReasonCode::RestorePrepareFailed,
        "re-quarantined with an ownerless expiry present",
        SandboxQuarantineNextAction::RequiresOperatorAttention,
    )
    .expect("an ownerless expiry must not abort the quarantine transaction");
}

// ---------------------------------------------------------------------------
// Acquire-CAS coverage matrix
//
// `claim_quarantine_recovery_candidate` is the only guard on the window between
// the candidate scan and the claim — a window that now contains a bounded plan
// RPC, so a concurrent failure has real time to replace the episode underneath
// it. It takes a `QuarantineRecoveryCandidate` rather than a claim, so it cannot
// be an arm of the fenced-statement matrix and needs its own.
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum AcquirePrecondition {
    EpisodeReplaced,
    // `source` has now been the uncovered conjunct three separate times across
    // this work — absent from one fence, unpinned in a since-removed guard,
    // and missing from this matrix. It is the conjunct most easily forgotten
    // because every episode this loop sees already has the same value.
    SourceChanged,
    ReasonChanged,
    AttemptCountAdvanced,
    RetryTimeInFuture,
    LiveClaimHeld,
    ForeignOwner,
    SandboxNotActivatable,
    CleanupEpisodePresent,
}

impl AcquirePrecondition {
    const ALL: [Self; 9] = [
        Self::EpisodeReplaced,
        Self::SourceChanged,
        Self::ReasonChanged,
        Self::AttemptCountAdvanced,
        Self::RetryTimeInFuture,
        Self::LiveClaimHeld,
        Self::ForeignOwner,
        Self::SandboxNotActivatable,
        Self::CleanupEpisodePresent,
    ];

    /// Break exactly this precondition after the candidate was scanned.
    fn apply(self, conn: &Connection, sandbox_id: SandboxId) {
        let id = sandbox_id.to_string();
        let sql = match self {
            Self::EpisodeReplaced => {
                "UPDATE sandbox_quarantine_state SET quarantine_id = ?2 WHERE sandbox_id = ?1"
            }
            Self::SourceChanged => {
                "UPDATE sandbox_quarantine_state SET source = ?2 WHERE sandbox_id = ?1"
            }
            Self::ReasonChanged => {
                "UPDATE sandbox_quarantine_state SET reason_code = ?2 WHERE sandbox_id = ?1"
            }
            Self::AttemptCountAdvanced => {
                "UPDATE sandbox_quarantine_state SET attempt_count = attempt_count + 3 WHERE sandbox_id = ?1"
            }
            Self::RetryTimeInFuture => {
                "UPDATE sandbox_quarantine_state SET next_action_after = ?2 WHERE sandbox_id = ?1"
            }
            Self::LiveClaimHeld => {
                "UPDATE sandbox_quarantine_state
                    SET next_action_owner = 'legacy_recovery', claim_token = ?2,
                        claim_expires_at = ?3
                  WHERE sandbox_id = ?1"
            }
            Self::ForeignOwner => {
                "UPDATE sandbox_quarantine_state SET next_action_owner = 'future_owner' WHERE sandbox_id = ?1"
            }
            Self::SandboxNotActivatable => "UPDATE sandboxes SET state = 'stopped' WHERE id = ?1",
            Self::CleanupEpisodePresent => {
                "INSERT INTO sandbox_cleanup_episodes (
                     sandbox_id, cleanup_id, completion, keep_filesystem, phase,
                     activation_state, created_at
                 ) VALUES (?1, ?2, 'mark_suspended', TRUE, 'armed', 'restoring', 0)"
            }
        };
        let future = (now_epoch_i64().unwrap() + 900).to_string();
        match self {
            Self::EpisodeReplaced => {
                conn.execute(sql, params![id, Uuid::now_v7().to_string()])
                    .unwrap();
            }
            Self::SourceChanged => {
                conn.execute(
                    sql,
                    params![id, SandboxQuarantineSource::NodeStatePublish.as_str()],
                )
                .unwrap();
            }
            Self::ReasonChanged => {
                conn.execute(
                    sql,
                    params![id, SandboxQuarantineReasonCode::PauseFailed.as_str()],
                )
                .unwrap();
            }
            Self::RetryTimeInFuture => {
                conn.execute(sql, params![id, future]).unwrap();
            }
            Self::LiveClaimHeld => {
                conn.execute(sql, params![id, Uuid::now_v7().to_string(), future])
                    .unwrap();
            }
            Self::CleanupEpisodePresent => {
                conn.execute(sql, params![id, SandboxCleanupEpisodeId::new().to_string()])
                    .unwrap();
            }
            Self::AttemptCountAdvanced | Self::ForeignOwner | Self::SandboxNotActivatable => {
                conn.execute(sql, params![id]).unwrap();
            }
        }
    }
}

#[test]
fn acquiring_a_claim_refuses_every_changed_precondition() {
    // Control: an untouched candidate must claim, or every refusal below proves
    // nothing.
    {
        let conn = crate::db::test_conn();
        let (_, candidate) = setup_quarantine_recovery_candidate(&conn);
        assert!(
            claim_quarantine_recovery_candidate(&conn, &candidate, PlanRejectionRunEffect::Leaves)
                .unwrap()
                .is_some(),
            "an untouched due candidate must be claimable"
        );
    }

    let mut admitted = Vec::new();
    for precondition in AcquirePrecondition::ALL {
        let conn = crate::db::test_conn();
        let (sandbox_id, candidate) = setup_quarantine_recovery_candidate(&conn);
        // The candidate was scanned; now the world moves before the claim lands.
        precondition.apply(&conn, sandbox_id);
        if claim_quarantine_recovery_candidate(&conn, &candidate, PlanRejectionRunEffect::Leaves)
            .unwrap()
            .is_some()
        {
            admitted.push(format!("{precondition:?}"));
        }
        // Invariants are deliberately not asserted here. These perturbations
        // write states directly that no code path can produce — `SandboxNotActivatable`
        // is itself an invariant violation by construction — so the durable
        // invariants belong on operations, not on synthetic setups.
    }
    assert!(
        admitted.is_empty(),
        "the acquire CAS admitted a claim whose preconditions had changed: {admitted:?}"
    );
}

// ---------------------------------------------------------------------------
// Statement inventory guard
//
// The coverage matrices above enumerate a list of statements that a human typed.
// That list is how a `DELETE` inside an already-converted function stayed outside
// the shared fence: nothing compared the list against the code. This test derives
// the inventory from the source instead, so a new writer of the recovery tables
// cannot appear without either routing through the fence or being named here with
// a reason.
// ---------------------------------------------------------------------------

/// One non-test source file of the `db` module, named by its path under `src/`.
struct DbModuleFile {
    path: String,
    source: String,
}

/// The `db` module's non-test source files, read from the crate at test time:
/// `db.rs` and every `.rs` file under `db/` except test modules (`tests.rs`,
/// `*_tests.rs`, `test_*.rs` and anything under a `tests/` or `test_*/`
/// directory, such as the `test-support` fixtures). Listing
/// the directory rather than naming the files means a new module is scanned by
/// the source-shape guards below without anyone registering it.
fn db_module_files() -> Vec<DbModuleFile> {
    fn is_test_module(path: &std::path::Path) -> bool {
        let stem = path
            .file_stem()
            .and_then(|stem| stem.to_str())
            .unwrap_or("");
        stem == "tests" || stem.ends_with("_tests") || stem.starts_with("test_")
    }
    fn collect(dir: &std::path::Path, into: &mut Vec<std::path::PathBuf>) {
        for entry in std::fs::read_dir(dir).expect("read db module directory") {
            let path = entry.expect("read db module directory entry").path();
            if path.is_dir() {
                if path
                    .file_name()
                    .and_then(|name| name.to_str())
                    .is_some_and(|name| name != "tests" && !name.starts_with("test_"))
                {
                    collect(&path, into);
                }
            } else if path.extension().is_some_and(|extension| extension == "rs")
                && !is_test_module(&path)
            {
                into.push(path);
            }
        }
    }

    let src = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut paths = Vec::new();
    collect(&src.join("db"), &mut paths);
    paths.sort();
    paths.insert(0, src.join("db.rs"));
    paths
        .into_iter()
        .map(|path| {
            let mut source = std::fs::read_to_string(&path).expect("read db module source");
            // Keep the last line of one file from running into the first of the next.
            if !source.ends_with('\n') {
                source.push('\n');
            }
            let path = path
                .strip_prefix(&src)
                .expect("db module under src")
                .display()
                .to_string();
            DbModuleFile { path, source }
        })
        .collect()
}

/// The `db` module's source as one text, for guards that search across modules.
fn db_module_source(files: &[DbModuleFile]) -> String {
    files.iter().map(|file| file.source.as_str()).collect()
}

/// The file and line that a 1-based line of [`db_module_source`] came from.
fn db_module_location(files: &[DbModuleFile], line: usize) -> String {
    let mut first = 1;
    for file in files {
        let lines = file.source.lines().count();
        if line < first + lines {
            return format!("{}:{}", file.path, line - first + 1);
        }
        first += lines;
    }
    format!("<past the end of the db module source>:{line}")
}

/// Does this line begin a statement that writes `sandbox_quarantine_state`?
///
/// Shared deliberately. The sibling test used to re-implement this expression
/// inline, so rewriting the real one back to `starts_with` left that test green —
/// the precise regression it was added to prevent. One definition, two callers.
///
/// Verb coverage is wider than the three spellings that happen to exist today:
/// SQLite accepts `INSERT OR REPLACE INTO`, `INSERT OR IGNORE INTO` and
/// `REPLACE INTO`, none of which contain `INSERT INTO`, and the episode writer is
/// already an upsert — so that spelling is a plausible next step, and it would
/// have been invisible rather than merely unfenced.
/// The name of the top-level function a line declares, if it declares one.
///
/// Shared by every source-derived guard in this file. It used to be inlined in one
/// and re-implemented, more weakly, in another: the weaker copy recognized only
/// `pub fn`, `pub(crate) fn` and `fn`, so a `pub(super) fn` or an `async fn` was
/// attributed to the previous function and inherited its fenced-or-registered
/// status. Two copies of a predicate drift, and the drift lands in whichever guard
/// is the only thing enforcing an invariant.
fn declared_fn_name(line: &str) -> Option<&str> {
    // Indented declarations count. Rejecting them attributed an `impl` block method's
    // statements to the previous top-level function, which is worse than missing them:
    // the method inherits that function's fenced-or-exempt status, so an unfenced write
    // in a method placed after an exempt function is exempt too, and the writer count
    // does not move because the name never enters the list.
    let line = line.trim_start();
    if !line.contains("fn ") {
        return None;
    }
    let prefix_is_modifiers = line.split("fn ").next().is_some_and(|prefix| {
        prefix.is_empty()
            || prefix
                .split_whitespace()
                .all(|word| word.starts_with("pub") || word == "async" || word == "const")
    });
    if !prefix_is_modifiers {
        return None;
    }
    line.split("fn ")
        .nth(1)
        .and_then(|rest| rest.split(['(', '<']).next())
}

fn line_writes_quarantine_state(line: &str) -> bool {
    // Normalized first. A schema-qualified or quoted table name would otherwise be
    // invisible to this predicate rather than merely unfenced, which is worse: an
    // `UPDATE main.sandbox_quarantine_state SET claim_token = NULL …` would be
    // neither counted as a writer nor reported as unfenced, and would need no
    // exemption entry to stay that way.
    let normalized = line.replace("main.", "").replace(['"', '`', '\''], "");
    let line = normalized.as_str();
    let trimmed = line.trim_start();
    if trimmed.starts_with("//") || trimmed.starts_with("///") {
        return false;
    }
    // All fourteen write openings SQLite accepts: `UPDATE` and `INSERT` each take the
    // five `OR <conflict>` clauses, plus `DELETE FROM` and `REPLACE INTO`. A verb this
    // list misses makes the statement invisible to the guard rather than merely
    // unfenced — it joins neither the writer list nor the unfenced list, and needs no
    // exemption to stay that way. Twice now the list has been extended one family at a
    // time and left the other short.
    const VERBS: [&str; 14] = [
        "UPDATE",
        "UPDATE OR IGNORE",
        "UPDATE OR REPLACE",
        "UPDATE OR ABORT",
        "UPDATE OR FAIL",
        "UPDATE OR ROLLBACK",
        "DELETE FROM",
        "INSERT INTO",
        "INSERT OR REPLACE INTO",
        "INSERT OR IGNORE INTO",
        "INSERT OR ABORT INTO",
        "INSERT OR FAIL INTO",
        "INSERT OR ROLLBACK INTO",
        "REPLACE INTO",
    ];
    VERBS
        .iter()
        .any(|verb| trimmed.contains(&format!("{verb} sandbox_quarantine_state")))
}

/// The same question for a statement whose verb and table are on separate lines.
///
/// `"UPDATE\n  sandbox_quarantine_state\n SET …"` matches nothing line-wise, so it
/// would land in neither the writer list nor the unfenced list: invisible to the
/// guard rather than caught by it.
fn statement_writes_quarantine_state(lines: &[&str], index: usize) -> bool {
    if line_writes_quarantine_state(lines[index]) {
        return true;
    }
    let trimmed = lines[index].trim_start();
    if trimmed.starts_with("//") {
        return false;
    }
    // The same set as the single-line predicate, deliberately: a verb known to one and
    // not the other makes the split form invisible while the single-line form is
    // caught. That has now happened twice — first the `INSERT OR …` forms, then the
    // `UPDATE OR …` ones — which is why both lists are written out in full rather than
    // described as "the same spellings".
    let opens_write = [
        "UPDATE",
        "UPDATE OR IGNORE",
        "UPDATE OR REPLACE",
        "UPDATE OR ABORT",
        "UPDATE OR FAIL",
        "UPDATE OR ROLLBACK",
        "DELETE FROM",
        "INSERT INTO",
        "INSERT OR REPLACE INTO",
        "INSERT OR IGNORE INTO",
        "INSERT OR ABORT INTO",
        "INSERT OR FAIL INTO",
        "INSERT OR ROLLBACK INTO",
        "REPLACE INTO",
    ]
    .iter()
    .any(|verb| {
        trimmed.trim_end().ends_with(verb) || trimmed.trim_end().ends_with(&format!("\"{verb}"))
    });
    if !opens_write {
        return false;
    }
    lines
        .iter()
        .skip(index + 1)
        .take(3)
        .any(|follow| follow.trim_start().starts_with("sandbox_quarantine_state"))
}

#[test]
fn the_writer_inventory_guard_recognizes_every_writer_spelling() {
    // The guard is code, so it gets the same treatment as the code it guards —
    // and this test now calls the real predicate rather than a copy of it. The
    // earlier version re-implemented the expression inline, which meant rewriting
    // the guard back to `starts_with` left this green: a test that could not fail
    // for the regression it names.
    let recognized = [
        // Built with `format!`, so the keyword is mid-line — invisible to a
        // `starts_with` check.
        r#"format!("UPDATE sandbox_quarantine_state SET attempt_count = 0 WHERE {}", predicate)"#,
        r#"let sql = format!("DELETE FROM sandbox_quarantine_state WHERE {}", predicate);"#,
        r#"format!("INSERT INTO sandbox_quarantine_state (sandbox_id) VALUES (?1) {}", tail)"#,
        // Spellings SQLite accepts that contain neither "INSERT INTO" nor the
        // other two verbs. The episode writer is already an upsert, so these are
        // a plausible next step.
        r#""INSERT OR REPLACE INTO sandbox_quarantine_state (sandbox_id) VALUES (?1)""#,
        r#""INSERT OR IGNORE INTO sandbox_quarantine_state (sandbox_id) VALUES (?1)""#,
        r#""REPLACE INTO sandbox_quarantine_state (sandbox_id) VALUES (?1)""#,
    ];
    for line in recognized {
        assert!(
            line_writes_quarantine_state(line),
            "the guard must recognize this writer, or the statement is neither \
             fenced nor exempted and still passes: {line}"
        );
    }

    // Commentary is not a writer, or every mention in a doc comment becomes a
    // phantom that has to be exempted.
    for comment in [
        r#"// UPDATE sandbox_quarantine_state is fenced by ExactClaimFence"#,
        r#"/// DELETE FROM sandbox_quarantine_state happens at retirement"#,
    ] {
        assert!(
            !line_writes_quarantine_state(comment),
            "a comment is not a writer"
        );
    }

    // And the verb-and-table-on-separate-lines shape, which matches nothing
    // line-wise and so was invisible rather than merely unfenced.
    let split: Vec<&str> = vec![
        r#"        "UPDATE"#,
        r#"            sandbox_quarantine_state"#,
        r#"           SET attempt_count = 0""#,
    ];
    assert!(
        statement_writes_quarantine_state(&split, 0),
        "a statement whose verb and table are on separate lines must still be \
         recognized"
    );
}

#[test]
fn every_recovery_table_writer_is_fenced_or_explicitly_exempt() {
    /// How far past a write to look for the end of its statement. The longest
    /// routed statement in the db module is well under this; a larger window
    /// would start borrowing the next statement's fence.
    const STATEMENT_WINDOW_LINES: usize = 40;
    // Functions that write the quarantine table without the exact-claim fence,
    // each with the reason it does not need one. Adding a name here is a
    // deliberate, reviewable act; forgetting to is a test failure.
    const EXEMPT: &[(&str, &str)] = &[
        (
            "insert_operator_quarantine_episode",
            "creates a new episode in the cleanup owner's transaction, then assigns its operator hold; preserves foreign owners",
        ),
        (
            "delete_sandbox_child_rows",
            "deletes the sandbox itself, so there is no episode left to fence",
        ),
        (
            "insert_new_quarantine_episode",
            "creates or replaces the episode a claim would fence against; guards \
             a live lease and a foreign owner with its own probe",
        ),
        (
            "recover_pause_failed_quarantine",
            "a separate non-claim recovery path; pins the episode it read in the \
             same transaction and fails typed on a row-count mismatch",
        ),
        (
            "claim_quarantine_recovery_candidate",
            "acquires the claim, so it fences on the absence of one; covered by \
             acquiring_a_claim_refuses_every_changed_precondition",
        ),
        (
            "retire_quarantine_recovery",
            "runs after the cleanup episode carrying the recovery fence was \
             validated; fails typed on a row-count mismatch",
        ),
        (
            "rearm_stopped_quarantine_recoveries",
            "re-arms only an episode recovery stopped, which holds no claim; runs \
             at startup before the reconciler can take one, and pins the episode \
             it read",
        ),
    ];

    let files = db_module_files();
    let source = db_module_source(&files);
    let mut current_fn: Option<&str> = None;
    // Statements are attributed to their enclosing function, and a function is
    // considered fenced if it constructs an ExactClaimFence anywhere in its body.
    let mut fenced_fns: std::collections::HashSet<&str> = std::collections::HashSet::new();
    let mut writers: Vec<&str> = Vec::new();
    // Per-statement, not per-function. Attribution by function whitelisted every
    // statement in any function that built a fence anywhere in its body, so an
    // unfenced DELETE added to `publish_orphan_quarantine_recovered_artifact` or
    // `adopt_quarantine_cleanup_episode` — both of which build two fences — passed
    // the guard untouched. That is verbatim the defect this test's own comment
    // claims to catch, so the guard could not have caught it.
    let mut unfenced_statements: Vec<(&str, usize)> = Vec::new();
    let lines: Vec<&str> = source.lines().collect();
    for (index, line) in lines.iter().enumerate() {
        let line = *line;
        let trimmed = line.trim_start();
        // Every top-level form, not the three that happen to exist today. A
        // `pub(super) fn` or an `async fn` writer would otherwise be attributed
        // to the previous function and silently inherit its fenced-or-exempt
        // status — the same "invisible to its own guard" shape this test exists
        // to catch.
        if let Some(name) = declared_fn_name(line) {
            current_fn = Some(name);
        }
        if let Some(name) = current_fn {
            if trimmed.contains("ExactClaimFence::new(") {
                fenced_fns.insert(name);
            }
            // `contains`, not `starts_with`, for every verb. The fenced statements
            // are built with `format!`, so the SQL keyword is mid-line — a
            // `starts_with` arm sees only the statements written as bare string
            // literals. The UPDATE arm was duplicated and had no `contains`
            // companion, which meant a single-line
            // `format!("UPDATE sandbox_quarantine_state SET … WHERE {}", …)` was
            // not recognized as a writer at all: neither fenced nor exempted, and
            // so invisible to the guard. That is the same asymmetry that let an
            // unfenced DELETE hide inside an already-converted function.
            let writes_quarantine_table = statement_writes_quarantine_state(&lines, index);
            if writes_quarantine_table {
                if !writers.contains(&name) {
                    writers.push(name);
                }
                // Read the statement this write belongs to and require the fence
                // inside it. Every routed statement interpolates the predicate
                // into its own SQL, so a statement that never mentions `fence.`
                // is unfenced regardless of what its neighbours do.
                let mut window = String::new();
                for follow in lines.iter().skip(index).take(STATEMENT_WINDOW_LINES) {
                    window.push_str(follow);
                    window.push('\n');
                    let end = follow.trim_end();
                    // Any statement terminator, not only a closing call. A write
                    // bound as `let sql = "UPDATE … ";` ends `";`, and without that
                    // arm the window ran on for 40 lines and picked up an unrelated
                    // `fence.` from a neighbouring statement — passing the very
                    // shape this check exists to catch.
                    if end.ends_with(");")
                        || end.ends_with(")?;")
                        || end.ends_with("]);")
                        || end.ends_with("\";")
                        || end.ends_with("_str());")
                    {
                        break;
                    }
                }
                // `quarantine_row_predicate`, not any `fence.` mention. The
                // other emitter, `sandbox_guards`, only adds EXISTS/NOT EXISTS
                // over sibling tables and returns an empty string when both of
                // its requirements are relaxed — so a statement could interpolate
                // it, pin no claim identity at all, and still satisfy a
                // `contains("fence.")` check. A comment mentioning `fence.`
                // inside the window did the same.
                if !window.contains("quarantine_row_predicate(") {
                    unfenced_statements.push((name, index + 1));
                }
            }
        }
    }

    assert_eq!(
        writers.len(),
        11,
        "the scan found {} writers of sandbox_quarantine_state, not 11. A floor \
         rather than an equality let a detection regression on up to three \
         writers pass green — a writer rewritten into a shape the predicate \
         misses simply leaves the list, and the stale-exemption check below only \
         back-stops the exempt ones. If a writer was legitimately added or \
         removed, change this number deliberately: {writers:?}",
        writers.len()
    );

    let exempt_names: std::collections::HashSet<&str> =
        EXEMPT.iter().map(|(name, _)| *name).collect();
    let unfenced_in_fenced_fn: Vec<(&str, String)> = unfenced_statements
        .iter()
        .filter(|(name, _)| !exempt_names.contains(name))
        .map(|(name, line)| (*name, db_module_location(&files, *line)))
        .collect();
    assert!(
        unfenced_in_fenced_fn.is_empty(),
        "these individual statements write sandbox_quarantine_state without \
         interpolating the exact-claim fence, at <file>:<line>: \
         {unfenced_in_fenced_fn:?}. A sibling statement in the same function \
         building a fence does not fence this one."
    );
    let unfenced: Vec<&&str> = writers
        .iter()
        .filter(|name| !fenced_fns.contains(**name) && !exempt_names.contains(**name))
        .collect();
    assert!(
        unfenced.is_empty(),
        "these functions write sandbox_quarantine_state without the exact-claim \
         fence and without an exemption: {unfenced:?}. Route them through \
         ExactClaimFence, or add them to EXEMPT with the reason they are safe."
    );

    // An exemption for a function that no longer writes the table is stale and
    // would silently license a future one under the same name.
    let stale: Vec<&&str> = exempt_names
        .iter()
        .filter(|name| !writers.contains(*name))
        .collect();
    assert!(
        stale.is_empty(),
        "these exemptions no longer correspond to a writer and should be removed: {stale:?}"
    );
}

/// A cross-node rootfs pull is a transfer, so the claim that authorized it can be
/// taken by another claimant while it runs. If the failure write is not itself
/// fenced on the claim, the spent claimant records an ordinary staging deferral on
/// an episode it no longer owns, and the lost claim never shows up as one.
#[test]
fn a_superseded_recovery_claim_cannot_persist_a_staging_failure() {
    let conn = crate::db::test_conn();
    let sandbox_id = setup_quarantine_recovery_episode(
        &conn,
        SandboxQuarantineReasonCode::OrphanedSandboxRowMissing,
    );
    let sandbox_id_string = sandbox_id.to_string();
    let candidate = scan_quarantine_recovery_candidates(&conn)
        .unwrap()
        .candidates
        .into_iter()
        .find(|candidate| candidate.sandbox_id == sandbox_id)
        .expect("the seeded episode should be admissible");
    let claimed =
        claim_quarantine_recovery_candidate(&conn, &candidate, PlanRejectionRunEffect::Leaves)
            .unwrap()
            .expect("claiming a fresh episode should succeed");
    // The pull runs with the row in Restoring, which is where the staging-failure
    // write applies.
    conn.execute(
        "UPDATE sandboxes SET state = 'restoring' WHERE id = ?1",
        params![sandbox_id_string],
    )
    .unwrap();

    // A live claim writes, so a later refusal is the fence and not a statement
    // that never matches anything.
    assert!(
        mark_cross_node_restore_staging_failed_under_recovery_claim(
            &conn,
            "first staging failure",
            &claimed
        )
        .unwrap(),
        "a live claim must be able to persist its own staging failure"
    );

    // Supersession while the pull is in flight: another claimant rotates the token.
    conn.execute(
        "UPDATE sandbox_quarantine_state SET claim_token = ?1 WHERE sandbox_id = ?2",
        params![uuid::Uuid::now_v7().to_string(), sandbox_id_string],
    )
    .unwrap();

    assert!(
        !mark_cross_node_restore_staging_failed_under_recovery_claim(
            &conn,
            "second staging failure",
            &claimed
        )
        .unwrap(),
        "a superseded claim must not persist a staging failure"
    );
    let reason: Option<String> = conn
        .query_row(
            "SELECT failure_reason FROM sandboxes WHERE id = ?1",
            params![sandbox_id_string],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(
        reason.as_deref(),
        Some("first staging failure"),
        "the superseded write must leave the reason the owning claim wrote"
    );
}

/// A cleanup owner arriving after the claim must still be recorded.
///
/// This used to be a contrast between two statements, one of which could report it and
/// one of which could not, and the pass wired eight call sites to the one that could
/// not. There is a single statement now, so the property is stated directly: the write
/// lands, the reason is recorded, and the lease comes back so the episode is due again
/// in one backoff rather than after the remaining fifteen minutes of its lease.
#[test]
fn a_cleanup_owner_arriving_after_the_claim_is_still_recorded() {
    let conn = crate::db::test_conn();
    let (sandbox_id, candidate) = setup_quarantine_recovery_candidate(&conn);
    let claimed =
        claim_quarantine_recovery_candidate(&conn, &candidate, PlanRejectionRunEffect::Leaves)
            .unwrap()
            .expect("claiming a fresh episode should succeed");

    // The cleanup owner arrives after the claim, which is what invalidates the
    // adoption and leaves the recovery with an outcome it has to record.
    conn.execute(
        "INSERT INTO sandbox_cleanup_episodes (
             sandbox_id, cleanup_id, completion, keep_filesystem, phase,
             activation_state, created_at
         ) VALUES (?1, ?2, 'mark_suspended', TRUE, 'armed', 'restoring', 0)",
        params![
            sandbox_id.to_string(),
            SandboxCleanupEpisodeId::new().to_string()
        ],
    )
    .unwrap();

    let outcome = record_quarantine_recovery_no_progress(
        &conn,
        &claimed,
        QuarantineRecoveryNoProgressReason::LocalAdoptionFenceChanged,
    )
    .unwrap();
    assert!(
        matches!(
            outcome,
            QuarantineRecoveryNoProgressOutcome::RetryScheduled { .. }
        ),
        "a cleanup-owned episode must still be reportable: {outcome:?}"
    );

    let now = now_epoch_i64().unwrap();
    let (_, attempts, scheduled, token, expiry) = quarantine_claim_state(&conn, sandbox_id);
    assert_eq!(attempts, 1, "the attempt the claim charged is kept");
    assert!(
        token.is_none() && expiry.is_none(),
        "the lease must be handed back, or the episode waits out the rest of it"
    );
    let scheduled: i64 = scheduled
        .expect("a retry must be scheduled")
        .parse()
        .expect("the retry time is an epoch integer");
    let base = QUARANTINE_RECOVERY_BACKOFF_BASE.as_secs() as i64;
    let lease = QUARANTINE_RECOVERY_CLAIM_LEASE.as_secs() as i64;
    assert!(
        scheduled > now && scheduled <= now + 2 * base,
        "the retry lands one backoff out (now={now}, scheduled={scheduled}, base={base}), \
         not a lease away ({lease}s)"
    );
    let last_error: Option<String> = conn
        .query_row(
            "SELECT last_error FROM sandbox_quarantine_state WHERE sandbox_id = ?1",
            params![sandbox_id.to_string()],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(
        last_error.as_deref(),
        Some(QuarantineRecoveryNoProgressReason::LocalAdoptionFenceChanged.as_str())
    );
}

/// Every function that builds an `ExactClaimFence` has to be in
/// `FencedClaimStatement::ALL`, or the per-conjunct matrix silently stops covering
/// it.
///
/// The matrix is the only thing that proves a fenced statement pins all seven
/// identity conjuncts and relaxes only what it means to. Its inventory used to be a
/// hand-kept list, and a hand-kept list is opt-in: a new fenced writer was simply
/// absent from it, so relaxing that statement's lease guard broke nothing in the
/// suite. Deriving the expected set from the source makes registration mandatory —
/// and unlike the writer guard below, this asks which functions build a fence, not
/// which table they write, so a fenced statement against any table is covered.
#[test]
fn every_fence_construction_site_is_in_the_statement_inventory() {
    let source = &db_module_source(&db_module_files());
    let mut enclosing = None;
    let mut sites: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
    for line in source.lines() {
        if let Some(name) = declared_fn_name(line) {
            enclosing = Some(name.to_string());
        }
        if line.contains("ExactClaimFence::new(")
            && let Some(name) = enclosing.as_ref()
        {
            sites.insert(name.clone());
        }
    }
    assert!(
        sites.len() >= 7,
        "the scan found {} fence construction sites, which means it stopped \
         recognizing the source's shape rather than that the fences were removed: \
         {sites:?}",
        sites.len()
    );

    let registered: std::collections::BTreeSet<String> = FencedClaimStatement::ALL
        .iter()
        .map(|statement| statement.function_name().to_string())
        .collect();
    assert_eq!(
        sites,
        registered,
        "every function building an ExactClaimFence must be registered in \
         FencedClaimStatement::ALL so the per-conjunct matrix exercises it; \
         unregistered: {:?}, registered but not found in the db module: {:?}",
        sites.difference(&registered).collect::<Vec<_>>(),
        registered.difference(&sites).collect::<Vec<_>>(),
    );
}

/// A clause of the acquire CAS.
///
/// The claim cannot use `ExactClaimFence` — there is no claim yet, so instead of
/// pinning a token it requires the existing one to be dead. That put the gate that
/// authorizes every fenced write downstream outside the per-conjunct matrix, which
/// is the one place a relaxation is most expensive: two claimants, or a claim over a
/// sandbox a cleanup owner already holds.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ClaimConjunct {
    SandboxId,
    QuarantineId,
    Source,
    ReasonCode,
    Owner,
    DueTime,
    ClaimIsDead,
    AttemptCount,
    SandboxIsActivatable,
    NoCleanupEpisode,
}

impl ClaimConjunct {
    const ALL: [Self; 10] = [
        Self::SandboxId,
        Self::QuarantineId,
        Self::Source,
        Self::ReasonCode,
        Self::Owner,
        Self::DueTime,
        Self::ClaimIsDead,
        Self::AttemptCount,
        Self::SandboxIsActivatable,
        Self::NoCleanupEpisode,
    ];

    /// Moves this conjunct out from under a candidate the scan just admitted.
    fn perturb(self, conn: &Connection, sandbox_id: SandboxId) {
        let id = sandbox_id.to_string();
        let far_future = now_epoch_i64().unwrap() + 86_400;
        match self {
            Self::SandboxId => {
                // Repointed at a different sandbox rather than a made-up id, so this
                // exercises the clause instead of the foreign key.
                let other = SandboxId::new();
                let other_string = other.to_string();
                insert_test_sandbox(
                    conn,
                    TestSandboxInsert::new(
                        &other_string,
                        SandboxRuntimeLabel::from_sandbox_id(other).to_string(),
                        SandboxState::Restoring,
                    ),
                );
                conn.execute(
                    "UPDATE sandbox_quarantine_state SET sandbox_id = ?1 WHERE sandbox_id = ?2",
                    params![other_string, id],
                )
                .unwrap();
            }
            Self::QuarantineId => {
                conn.execute(
                    "UPDATE sandbox_quarantine_state SET quarantine_id = ?1 WHERE sandbox_id = ?2",
                    params![uuid::Uuid::now_v7().to_string(), id],
                )
                .unwrap();
            }
            Self::Source => {
                conn.execute(
                    "UPDATE sandbox_quarantine_state SET source = 'node_manager'
                      WHERE sandbox_id = ?1",
                    params![id],
                )
                .unwrap();
            }
            Self::ReasonCode => {
                conn.execute(
                    "UPDATE sandbox_quarantine_state SET reason_code = ?1 WHERE sandbox_id = ?2",
                    params![
                        SandboxQuarantineReasonCode::RestorePrepareFailed.as_str(),
                        id
                    ],
                )
                .unwrap();
            }
            Self::Owner => {
                conn.execute(
                    "UPDATE sandbox_quarantine_state SET next_action_owner = 'operator'
                      WHERE sandbox_id = ?1",
                    params![id],
                )
                .unwrap();
            }
            Self::DueTime => {
                conn.execute(
                    "UPDATE sandbox_quarantine_state SET next_action_after = ?1
                      WHERE sandbox_id = ?2",
                    params![far_future.to_string(), id],
                )
                .unwrap();
            }
            Self::ClaimIsDead => {
                conn.execute(
                    "UPDATE sandbox_quarantine_state
                        SET claim_token = ?1, claim_expires_at = ?2
                      WHERE sandbox_id = ?3",
                    params![uuid::Uuid::now_v7().to_string(), far_future.to_string(), id],
                )
                .unwrap();
            }
            Self::AttemptCount => {
                conn.execute(
                    "UPDATE sandbox_quarantine_state SET attempt_count = attempt_count + 3
                      WHERE sandbox_id = ?1",
                    params![id],
                )
                .unwrap();
            }
            Self::SandboxIsActivatable => {
                conn.execute(
                    "UPDATE sandboxes SET state = 'started' WHERE id = ?1",
                    params![id],
                )
                .unwrap();
            }
            Self::NoCleanupEpisode => {
                conn.execute(
                    "INSERT INTO sandbox_cleanup_episodes (
                         sandbox_id, cleanup_id, completion, keep_filesystem, phase,
                         activation_state, created_at
                     ) VALUES (?1, ?2, 'mark_suspended', TRUE, 'armed', 'restoring', 0)",
                    params![id, SandboxCleanupEpisodeId::new().to_string()],
                )
                .unwrap();
            }
        }
    }
}

/// Every clause of the acquire CAS has to block the claim on its own.
///
/// Same shape as the fence matrix, for the statement the fence matrix cannot cover.
/// A relaxation here is not a missed retry — it is a second claimant holding an
/// episode the first is already restoring, or a claim over a sandbox that a cleanup
/// owner or an operator has taken.
#[test]
fn every_clause_of_the_acquire_cas_blocks_a_claim_on_its_own() {
    for conjunct in ClaimConjunct::ALL {
        let conn = crate::db::test_conn();
        let (sandbox_id, candidate) = setup_quarantine_recovery_candidate(&conn);

        // Control: this candidate is claimable before the perturbation, so a refusal
        // below is the clause and not a fixture that could never claim.
        let control = crate::db::test_conn();
        let (_, control_candidate) = setup_quarantine_recovery_candidate(&control);
        assert!(
            claim_quarantine_recovery_candidate(
                &control,
                &control_candidate,
                PlanRejectionRunEffect::Leaves
            )
            .unwrap()
            .is_some(),
            "{conjunct:?}: the unperturbed fixture must be claimable"
        );

        conjunct.perturb(&conn, sandbox_id);
        assert!(
            claim_quarantine_recovery_candidate(&conn, &candidate, PlanRejectionRunEffect::Leaves)
                .unwrap()
                .is_none(),
            "{conjunct:?} moved and the claim was still granted; the acquire CAS must \
             pin it, or two claimants can hold one episode"
        );
    }
}

/// The acquire CAS has to pin every column the fence pins.
///
/// The claim and the fenced writes are two different predicates guarded by two
/// different matrices, and each matrix only knows the clauses someone wrote into its
/// own enum. That leaves the seam between them unchecked: if the fence pins a column
/// the claim does not, a claim can be granted against a row whose identity already
/// differs from what the scan read, and every fenced write afterwards then agrees
/// with a row nobody validated. Deriving both sets from the source ties them
/// together without either matrix having to know about the other.
#[test]
fn the_acquire_cas_pins_every_column_the_fence_pins() {
    let source = &db_module_source(&db_module_files());

    // The fence's identity columns, read off the predicate builder.
    let predicate = {
        let start = source
            .find("fn quarantine_row_predicate")
            .expect("the fence predicate builder must be findable");
        let body = source.get(start..).expect("matched source boundary");
        let end = body.find("\n    }").expect("its body must end");
        body.get(..end).expect("matched source boundary")
    };
    let fence_columns: std::collections::BTreeSet<&str> = predicate
        .lines()
        .filter_map(|line| {
            let start = line.find("{alias}")? + "{alias}".len();
            let rest = line.get(start..).expect("matched source boundary");
            let end = rest.find(|c: char| !c.is_alphanumeric() && c != '_')?;
            Some(rest.get(..end).expect("matched source boundary"))
        })
        .collect();
    assert!(
        fence_columns.len() >= 7,
        "the scan found {} fence columns, so it stopped recognizing the builder's \
         shape rather than the fence having shrunk: {fence_columns:?}",
        fence_columns.len()
    );

    // The acquire CAS's WHERE clause.
    let claim_where = {
        let start = source
            .find("pub fn claim_quarantine_recovery_candidate")
            .expect("the claim must be findable");
        let body = source.get(start..).expect("matched source boundary");
        let where_start = body
            .find("WHERE qs.sandbox_id")
            .expect("its WHERE must be findable");
        let end = body
            .get(where_start..)
            .expect("matched source boundary")
            .find("\",")
            .expect("the statement must end");
        body.get(where_start..where_start + end)
            .expect("matched source boundary")
    };

    let missing: Vec<&str> = fence_columns
        .iter()
        .copied()
        .filter(|column| !claim_where.contains(column))
        .collect();
    assert!(
        missing.is_empty(),
        "the acquire CAS does not constrain {missing:?}, which the fence pins — a \
         claim can then be granted on a row whose identity already moved, and every \
         fenced write afterwards matches a row the scan never saw"
    );
}

/// The release records the reason it was given, not a fixed one.
///
/// Two outcomes route through this statement — a busy cleanup fence, and an adoption
/// fence that moved — and both the counter and the durable `last_error` are read by
/// whoever is diagnosing a stalled episode. Stamping one reason on both makes the row
/// contradict the metric for the same event.
#[test]
fn the_contention_release_records_the_reason_it_was_given() {
    for reason in [
        QuarantineRecoveryNoProgressReason::CleanupFenceBusy,
        QuarantineRecoveryNoProgressReason::LocalAdoptionFenceChanged,
    ] {
        let conn = crate::db::test_conn();
        let (sandbox_id, candidate) = setup_quarantine_recovery_candidate(&conn);
        let claimed =
            claim_quarantine_recovery_candidate(&conn, &candidate, PlanRejectionRunEffect::Leaves)
                .unwrap()
                .expect("claiming a fresh episode should succeed");
        assert!(matches!(
            record_quarantine_recovery_no_progress(&conn, &claimed, reason).unwrap(),
            QuarantineRecoveryNoProgressOutcome::RetryScheduled { .. }
        ));
        let (_, _, _, _, _) = quarantine_claim_state(&conn, sandbox_id);
        let last_error: Option<String> = conn
            .query_row(
                "SELECT last_error FROM sandbox_quarantine_state WHERE sandbox_id = ?1",
                params![sandbox_id.to_string()],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(
            last_error.as_deref(),
            Some(reason.as_str()),
            "the row must record the reason the caller reported"
        );
    }
}

/// Two fenced writers may not write the same columns under different guards.
///
/// This is the category guard for a defect no statement-level test could see.
/// `record_quarantine_recovery_no_progress` and
/// `release_quarantine_recovery_claim_after_cleanup_contention` wrote byte-identical
/// SET clauses and differed only in their three environmental relaxations, so which
/// one a call site chose was observable *only* as write-versus-miss, never as a
/// different row. Every matrix in this file tests statements, so all of them were
/// blind to the routing, and the strict writer stayed wired into nine call sites where
/// its guards fail in the very situation it exists to report: a cleanup owner arriving
/// after the claim leaves the lease standing for its full 15 minutes instead of one
/// 48-60s backoff.
///
/// Reviewers found that one call site at a time, three rounds running, because fixing
/// one site says nothing about the other eight. Collapsing such a pair into a single
/// statement makes the wrong choice unrepresentable; this test stops the pair being
/// reintroduced — for any future pair, not just this one.
///
/// Scoped to the registered fenced statements, because those are exactly the functions
/// whose predicates the matrices reason about, and a new one cannot escape that list
/// (`every_fence_construction_site_is_in_the_statement_inventory`).
#[test]
fn no_two_fenced_writers_share_a_column_set_under_different_guards() {
    let source = &db_module_source(&db_module_files());

    /// The table written, the columns assigned, and the three relaxations, read out of
    /// one function's body.
    fn writer_shape(source: &str, function: &str) -> Option<(String, Vec<String>, Vec<String>)> {
        let at = source.find(&format!("pub fn {function}("))?;
        let body = source.split_at(at).1;
        let end = body.find("\n}\n").unwrap_or(body.len());
        let body = body.split_at(end).0;

        let mut guards = Vec::new();
        for needle in [
            "LeaseRequirement::",
            "SandboxStateRequirement::",
            "CleanupRequirement::",
        ] {
            let found = body.find(needle)?;
            let variant: String = body
                .split_at(found + needle.len())
                .1
                .chars()
                .take_while(|c| c.is_alphanumeric() || *c == '_')
                .collect();
            guards.push(format!("{needle}{variant}"));
        }

        // The first write in the body, and the columns it assigns between SET and WHERE.
        let (verb, table_at) = ["UPDATE ", "DELETE FROM "]
            .iter()
            .filter_map(|v| body.find(v).map(|i| (*v, i + v.len())))
            .min_by_key(|(_, i)| *i)?;
        let table: String = body
            .split_at(table_at)
            .1
            .chars()
            .take_while(|c| c.is_alphanumeric() || *c == '_')
            .collect();

        let mut columns = Vec::new();
        if verb == "UPDATE " {
            let set_at = body.split_at(table_at).1.find("SET ")? + table_at + 4;
            let where_at = body.split_at(set_at).1.find("WHERE")? + set_at;
            for assignment in body.split_at(where_at).0.split_at(set_at).1.split(',') {
                let name: String = assignment
                    .trim()
                    .chars()
                    .take_while(|c| c.is_alphanumeric() || *c == '_')
                    .collect();
                if !name.is_empty() {
                    columns.push(name);
                }
            }
            columns.sort();
            columns.dedup();
        }
        Some((table, columns, guards))
    }

    let shapes: Vec<(String, String, Vec<String>, Vec<String>)> = FencedClaimStatement::ALL
        .iter()
        .filter_map(|statement| {
            let name = statement.function_name();
            writer_shape(source, name)
                .map(|(table, columns, guards)| (name.to_string(), table, columns, guards))
        })
        .collect();
    assert!(
        shapes.len() >= 6,
        "read the shape of only {} of the {} registered fenced statements, so this \
         stopped recognizing the source rather than the statements having changed",
        shapes.len(),
        FencedClaimStatement::ALL.len()
    );

    // A statement that does not render `sandbox_guards` cannot enforce the sandbox-state
    // or cleanup-episode requirements at all, whatever it passes the constructor. Those
    // arguments then read as guards that are in force and are inert, which is worse than
    // absent: a later edit tightening them changes nothing, and a matrix entry asserting
    // they are relaxed passes for the wrong reason. Found exactly that way — reinstating
    // the strict triple on the no-progress write moved no test until this existed.
    let mut inert = Vec::new();
    for (function, ..) in &shapes {
        let at = source
            .find(&format!("pub fn {function}("))
            .expect("shape was read from this source");
        let body = source.get(at..).expect("matched source boundary");
        let body = body
            .get(..body.find("\n}\n").unwrap_or(body.len()))
            .expect("matched source boundary");
        if body.contains("sandbox_guards(") {
            continue;
        }
        for (needle, relaxed) in [
            (
                "SandboxStateRequirement::",
                "SandboxStateRequirement::AnyState",
            ),
            ("CleanupRequirement::", "CleanupRequirement::MayExist"),
        ] {
            let found = body.find(needle).expect("shape was read from this source");
            let declared: String = body
                .get(found..)
                .expect("matched source boundary")
                .chars()
                .take_while(|c| c.is_alphanumeric() || *c == '_' || *c == ':')
                .collect();
            if declared != relaxed {
                inert.push(format!(
                    "{function} declares {declared} but renders no sandbox_guards, so \
                     that requirement is not in the statement at all"
                ));
            }
        }
    }
    assert!(
        inert.is_empty(),
        "these statements declare an environmental requirement they do not render:\n{}",
        inert.join("\n")
    );

    let mut clashes = Vec::new();
    for (i, (a_fn, a_table, a_cols, a_guards)) in shapes.iter().enumerate() {
        for (b_fn, b_table, b_cols, b_guards) in shapes.iter().skip(i + 1) {
            if a_table == b_table && !a_cols.is_empty() && a_cols == b_cols && a_guards != b_guards
            {
                clashes.push(format!(
                    "{a_fn} and {b_fn} both write {a_table}({}) but under different \
                     guards:\n    {a_fn}: {a_guards:?}\n    {b_fn}: {b_guards:?}",
                    a_cols.join(", ")
                ));
            }
        }
    }
    assert!(
        clashes.is_empty(),
        "two fenced writers with the same effect and different preconditions. Which one \
         a call site picks is then observable only as write-versus-miss, so no \
         statement-level test can see a mis-routed site — collapse them into one \
         statement instead.\n{}",
        clashes.join("\n")
    );
}

#[test]
fn access_floor_is_reset_atomically_only_on_execution_change() {
    for runtime_writer in [false, true] {
        let conn = test_conn();
        let id = SandboxId::new();
        let storage_id = id.to_string();
        insert_test_sandbox(
            &conn,
            TestSandboxInsert::new(&storage_id, "floor001", SandboxState::Started),
        );
        let apply = |generation| {
            let generation = ExecutionGeneration::new(generation).unwrap();
            if runtime_writer {
                update_runtime_fields(&conn, id, runtime_fields(generation)).map(|_| ())
            } else {
                update_space_api_route_identity(
                    &conn,
                    id,
                    &SpaceApiRouteIdentity {
                        execution_generation: Some(generation),
                        ..Default::default()
                    },
                    crate::auto_lifecycle_delete_receipts::RouteIdentityProvenance::Unfenced,
                )
            }
        };
        apply(1).unwrap();
        let policy = access_idle_policy();
        let mut runtime = crate::auto_lifecycle::initial_runtime_state(&policy, 100);
        crate::auto_lifecycle::extend_access_deadline(&mut runtime, &policy, 1000).unwrap();
        update_auto_lifecycle_with_state(
            &conn,
            &storage_id,
            &policy,
            &runtime,
            PolicyAuthority::Cluster,
        )
        .unwrap();
        apply(1).unwrap();
        assert_eq!(
            get_sandbox(&conn, &storage_id)
                .unwrap()
                .unwrap()
                .auto_lifecycle_state
                .access_extended_until,
            Some(1000)
        );
        conn.execute_batch("CREATE TEMP TRIGGER reject_floor_reset BEFORE UPDATE OF auto_lifecycle_state_json ON sandboxes BEGIN SELECT RAISE(FAIL, 'injected floor failure'); END;").unwrap();
        apply(2).unwrap_err();
        assert_eq!(
            runtime_execution_generation(&conn, &storage_id)
                .unwrap()
                .unwrap()
                .get(),
            1
        );
        conn.execute_batch("DROP TRIGGER reject_floor_reset")
            .unwrap();
        apply(2).unwrap();
        let row = get_sandbox(&conn, &storage_id).unwrap().unwrap();
        assert_eq!(row.auto_lifecycle_state.access_extended_until, None);
        assert_eq!(row.next_auto_event_at, Some(400));
    }
}

#[test]
fn activation_installs_current_policy_before_arming_and_preserves_local_authority() {
    for (state, kind) in [
        (SandboxState::Suspended, ActivationKind::Restore),
        (SandboxState::Created, ActivationKind::ColdStart),
        (SandboxState::Stopped, ActivationKind::ColdStart),
    ] {
        for (authority, supplied, expected_seconds) in [
            (PolicyAuthority::Cluster, true, 60),
            (PolicyAuthority::Cluster, false, 30),
            (PolicyAuthority::Local, true, 30),
        ] {
            let conn = test_conn();
            let id = SandboxId::new().to_string();
            let mut stored = access_idle_policy();
            stored.stages[0].at = 30;
            let mut desired = stored.clone();
            desired.stages[0].at = 60;
            let mut insert = TestSandboxInsert::new(&id, "policy01", state);
            insert.auto_lifecycle = stored.clone();
            insert.auto_lifecycle_authority = authority;
            insert_test_sandbox(&conn, insert);
            let begin = || {
                begin_activation_with_route_evidence(
                    &conn,
                    &id,
                    state,
                    kind,
                    None,
                    None,
                    ActivationRouteAdmission {
                        auto_lifecycle: supplied.then_some(&desired),
                        ..Default::default()
                    },
                )
            };
            assert!(begin().unwrap().is_some());
            let admitted = get_sandbox(&conn, &id).unwrap().unwrap();
            assert_eq!(admitted.auto_lifecycle.stages[0].at, expected_seconds);
            assert_eq!(admitted.auto_lifecycle_authority, authority);
            assert_eq!(admitted.next_auto_event_at, None);
            match kind {
                ActivationKind::Restore => assert!(
                    mark_restore_succeeded(&conn, &id, SandboxState::Started, None).unwrap()
                ),
                ActivationKind::ColdStart => {
                    assert!(mark_start_succeeded(&conn, &id, None).unwrap())
                }
            }
            let running = get_sandbox(&conn, &id).unwrap().unwrap();
            let epoch = running.auto_lifecycle_state.conditions
                [&AutoLifecycleConditionSetId::AccessIdle]
                .epoch_at
                .unwrap();
            assert_eq!(running.next_auto_event_at, Some(epoch + expected_seconds));
            // A lost-response retry must not replay stale activation intent over
            // a timeout accepted after the execution became live.
            let mut newer = desired.clone();
            newer.stages[0].at = 90;
            update_auto_lifecycle(&conn, &id, &newer, authority).unwrap();
            let before_retry = get_sandbox(&conn, &id).unwrap().unwrap();
            assert!(begin().unwrap().is_none());
            let after_retry = get_sandbox(&conn, &id).unwrap().unwrap();
            assert_eq!(after_retry.auto_lifecycle, newer);
            assert_eq!(
                after_retry.auto_lifecycle_state,
                before_retry.auto_lifecycle_state
            );
        }
    }
}

#[test]
fn activation_policy_failure_rolls_back_the_execution_claim() {
    let conn = test_conn();
    let id = SandboxId::new().to_string();
    insert_test_sandbox(
        &conn,
        TestSandboxInsert::new(&id, "policy02", SandboxState::Suspended),
    );
    conn.execute_batch("CREATE TEMP TRIGGER reject_policy BEFORE UPDATE OF auto_lifecycle_json ON sandboxes BEGIN SELECT RAISE(ABORT, 'injected policy write failure'); END;").unwrap();
    let policy = access_idle_policy();
    begin_activation_with_route_evidence(
        &conn,
        &id,
        SandboxState::Suspended,
        ActivationKind::Restore,
        None,
        None,
        ActivationRouteAdmission {
            auto_lifecycle: Some(&policy),
            ..Default::default()
        },
    )
    .expect_err("injected policy failure must abort activation");
    assert_eq!(
        get_sandbox(&conn, &id).unwrap().unwrap().state,
        SandboxState::Suspended
    );
    assert!(cleanup_episode(&conn, &id).unwrap().is_none());
}

#[test]
fn resident_policy_resume_is_generation_fenced_and_arms_only_after_thaw() {
    for (authority, resident_generation, generation, expected) in [
        (PolicyAuthority::Cluster, Some(1), Some(1), Some(60)),
        (PolicyAuthority::Local, Some(1), Some(1), Some(30)),
        (PolicyAuthority::Cluster, Some(1), Some(2), None),
        (PolicyAuthority::Cluster, None, None, Some(60)),
        (PolicyAuthority::Local, None, None, Some(30)),
        (PolicyAuthority::Cluster, Some(1), None, None),
        (PolicyAuthority::Cluster, None, Some(1), None),
    ] {
        let conn = test_conn();
        let id = SandboxId::new();
        let mut policy = access_idle_policy();
        policy.stages[0].at = 30;
        let mut insert = TestSandboxInsert::new(id.to_string(), "respol01", SandboxState::Paused);
        insert.auto_lifecycle = policy.clone();
        insert.auto_lifecycle_authority = authority;
        insert_test_sandbox(&conn, insert);
        update_space_api_route_identity(
            &conn,
            id,
            &SpaceApiRouteIdentity {
                execution_generation: resident_generation
                    .map(|g| ExecutionGeneration::new(g).unwrap()),
                ..Default::default()
            },
            crate::auto_lifecycle_delete_receipts::RouteIdentityProvenance::Unfenced,
        )
        .unwrap();
        policy.stages[0].at = 60;
        let applied = begin_resident_resume_with_policy(
            &conn,
            &id.to_string(),
            generation.map(|g| ExecutionGeneration::new(g).unwrap()),
            &policy,
        )
        .unwrap();
        assert_eq!(applied, expected.is_some());
        let row = get_sandbox(&conn, &id.to_string()).unwrap().unwrap();
        assert_eq!(row.next_auto_event_at, None);
        if let Some(seconds) = expected {
            assert_eq!(row.state, SandboxState::Resuming);
            assert_eq!(row.auto_lifecycle.stages[0].at, seconds);
            assert!(
                restore_resident_resume_state(&conn, &id.to_string(), SandboxState::Started)
                    .unwrap()
            );
            let running = get_sandbox(&conn, &id.to_string()).unwrap().unwrap();
            let epoch = running.auto_lifecycle_state.conditions
                [&AutoLifecycleConditionSetId::AccessIdle]
                .epoch_at
                .unwrap();
            assert_eq!(running.next_auto_event_at, Some(epoch + seconds));
            assert!(
                !begin_resident_resume_with_policy(
                    &conn,
                    &id.to_string(),
                    resident_generation.map(|g| ExecutionGeneration::new(g).unwrap()),
                    &policy
                )
                .unwrap()
            );
        } else {
            assert_eq!(row.state, SandboxState::Paused);
            assert_eq!(row.auto_lifecycle.stages[0].at, 30);
        }
    }
}

#[test]
fn teardown_suspend_terminal_transitions_close_jobs_atomically() {
    for target in [SandboxState::Deleting, SandboxState::Quarantined] {
        let conn = test_conn();
        insert_test_sandbox(
            &conn,
            TestSandboxInsert::new("sandbox-1", "sandbox1", SandboxState::Paused),
        );
        let generation = ExecutionGeneration::new(7).unwrap();
        begin_teardown_suspend_operation(&conn, "sandbox-1", generation).unwrap();
        match target {
            SandboxState::Deleting => {
                mark_deleting_superseding_activation(&conn, "sandbox-1").unwrap();
            }
            SandboxState::Quarantined => {
                mark_quarantined(
                    &conn,
                    "sandbox-1",
                    SandboxQuarantineSource::SbxManager,
                    SandboxQuarantineReasonCode::RuntimeProbeInconsistent,
                    "runtime unavailable",
                    SandboxQuarantineNextAction::RequiresOperatorAttention,
                )
                .unwrap();
            }
            _ => unreachable!(),
        }
        assert_eq!(
            latest_teardown_suspend_operation(&conn, "sandbox-1")
                .unwrap()
                .unwrap()
                .status,
            TeardownSuspendOperationStatus::Failed,
            "{target}"
        );
    }
    let conn = test_conn();
    insert_test_sandbox(
        &conn,
        TestSandboxInsert::new("sandbox-1", "sandbox1", SandboxState::Paused),
    );
    begin_teardown_suspend_operation(&conn, "sandbox-1", ExecutionGeneration::new(7).unwrap())
        .unwrap();
    let tx = immediate_transaction(&conn).unwrap();
    delete_sandbox_child_rows(&tx, "sandbox-1").unwrap();
    assert!(
        active_teardown_suspend_operation(&tx, "sandbox-1")
            .unwrap()
            .is_none()
    );
    tx.rollback().unwrap();
    assert!(
        active_teardown_suspend_operation(&conn, "sandbox-1")
            .unwrap()
            .is_some()
    );
    delete_sandbox(&conn, "sandbox-1").unwrap();
    assert_eq!(
        latest_teardown_suspend_operation(&conn, "sandbox-1")
            .unwrap()
            .unwrap()
            .status,
        TeardownSuspendOperationStatus::Failed
    );
}

#[test]
fn teardown_suspend_terminal_failure_is_generation_fenced() {
    let conn = test_conn();
    insert_test_sandbox(
        &conn,
        TestSandboxInsert::new("sandbox-1", "sandbox1", SandboxState::Paused),
    );
    let old = ExecutionGeneration::new(7).unwrap();
    let current = ExecutionGeneration::new(8).unwrap();
    begin_teardown_suspend_operation(&conn, "sandbox-1", old).unwrap();
    begin_teardown_suspend_operation(&conn, "sandbox-1", current).unwrap();
    assert!(
        !fail_teardown_suspend_operation(&conn, "sandbox-1", old, TeardownSuspendFailure::Missing)
            .unwrap()
    );
    assert_eq!(
        active_teardown_suspend_operation(&conn, "sandbox-1")
            .unwrap()
            .unwrap()
            .execution_generation,
        current
    );
    assert!(
        fail_teardown_suspend_operation(
            &conn,
            "sandbox-1",
            current,
            TeardownSuspendFailure::Missing
        )
        .unwrap()
    );
    assert!(
        !fail_teardown_suspend_operation(
            &conn,
            "sandbox-1",
            current,
            TeardownSuspendFailure::Missing
        )
        .unwrap()
    );
    begin_teardown_suspend_operation(&conn, "sandbox-1", current).unwrap_err();
    begin_teardown_suspend_operation(&conn, "sandbox-1", ExecutionGeneration::new(9).unwrap())
        .unwrap();
}

#[test]
fn teardown_suspend_reconciliation_skips_healthy_and_retained_jobs_and_bounds_closures() {
    let conn = test_conn();
    let generation = ExecutionGeneration::new(7).unwrap();
    let unrelated_job = JobId::new();
    pplx_space_common_jobs::db::insert_job(
        conn.raw(),
        pplx_space_common_jobs::db::InsertJob {
            job_id: &unrelated_job,
            kind: "synthetic_other_job",
            payload: "{}",
            subject_id: Some("absent"),
            dedup_key: None,
            max_attempts: 1,
            traceparent: None,
            priority: Default::default(),
        },
    )
    .unwrap();
    for index in 0..260 {
        let id = format!("sandbox-{index}");
        insert_test_sandbox(
            &conn,
            TestSandboxInsert::new(&id, format!("sbx{index:05}"), SandboxState::Paused),
        );
        claim_teardown_suspend_operation(&conn, &id, generation).unwrap();
    }
    // Simulate rows persisted by a binary without transition cleanup, both pending and running.
    for index in 127..259 {
        let state = if index % 2 == 0 {
            "quarantined"
        } else {
            "deleting"
        };
        conn.execute(
            "UPDATE sandboxes SET state = ?1 WHERE id = ?2",
            params![state, format!("sandbox-{index}")],
        )
        .unwrap();
    }
    // Retained failed history must not consume the closure budget or be rewritten.
    let retained = active_teardown_suspend_operation(&conn, "sandbox-0")
        .unwrap()
        .unwrap();
    pplx_space_common_jobs::db::fail_job_permanent(
        conn.raw(),
        &retained.job_id,
        "synthetic retained failure",
        None,
    )
    .unwrap();
    claim_teardown_suspend_operation(&conn, "sandbox-0", ExecutionGeneration::new(8).unwrap())
        .unwrap();
    mark_teardown_suspend_operation_running(&conn, "sandbox-259", generation).unwrap();
    conn.execute("DELETE FROM sandboxes WHERE id = 'sandbox-259'", [])
        .unwrap();
    assert_eq!(
        reconcile_terminal_teardown_suspend_jobs(&conn).unwrap(),
        128
    );
    assert_eq!(reconcile_terminal_teardown_suspend_jobs(&conn).unwrap(), 5);
    assert_eq!(reconcile_terminal_teardown_suspend_jobs(&conn).unwrap(), 0);
    assert_eq!(
        pplx_space_common_jobs::db::get_job(conn.raw(), &retained.job_id)
            .unwrap()
            .unwrap()
            .error
            .as_deref(),
        Some("synthetic retained failure")
    );
    assert!(
        !pplx_space_common_jobs::db::get_job(conn.raw(), &unrelated_job)
            .unwrap()
            .unwrap()
            .is_terminal()
    );
    for index in 0..127 {
        assert!(
            active_teardown_suspend_operation(&conn, &format!("sandbox-{index}"))
                .unwrap()
                .is_some()
        );
    }
    for index in 127..260 {
        let operation = latest_teardown_suspend_operation(&conn, &format!("sandbox-{index}"))
            .unwrap()
            .unwrap();
        let job = pplx_space_common_jobs::db::get_job(conn.raw(), &operation.job_id)
            .unwrap()
            .unwrap();
        assert_eq!(
            job.failure,
            Some(pplx_space_common_jobs::JobFailure::Permanent { reason: None })
        );
    }
}

mod accepted_shapes_cleanup_episodes;
mod accepted_shapes_quarantine;
mod accepted_shapes_single_statement;
mod cleanup_quarantine_refusal;
mod local_network_policy_clear;
mod network_policy_live_gateway;
mod pinned_artifact_capture;
mod pool_donors;
mod state_write_table;
mod stopped_artifact_pin;
mod thin_writer_coverage;
mod update_state_from;
mod update_state_matrix;

mod execution_binding;
