//! Both outcomes of the guarded `sandboxes.state` writers in the activation,
//! suspend, restore, publication and eviction paths: the row shape each
//! compare-and-set accepts, and the neighbouring shapes it refuses.
//!
//! A refused write must leave every persisted row as it was, including the
//! companion rows the writer would have inserted or deleted in the same
//! transaction, so refusals compare a snapshot of the whole database.

use std::collections::BTreeMap;

use super::database_rows;

use crate::db::Connection;
use pplx_space_common_core::types::{
    ArtifactKind, ArtifactOrigin, AutoLifecyclePolicy, ExactStorageUri, ExecutionGeneration,
    LocalArtifactId, SandboxCleanupEpisodeId, SandboxId, SandboxOwner, SandboxQuarantineNextAction,
    SandboxQuarantineReasonCode, SandboxQuarantineSource, SandboxRuntimeLabel, TransferScopeId,
};
use rusqlite::params;

use crate::db::{
    ActivationKind, CleanupCompletion, CleanupEpisode, CleanupEpisodeRequestOutcome,
    CleanupEpisodeSpec, InsertSandboxParams, PauseFailedRecoveryTarget, PolicyAuthority,
    RollbackActivationEpisodeError, SandboxRow, begin_activation, begin_suspend,
    begin_teardown_suspend, charge_prepared_clone_disposal_attempt,
    claim_stopped_sandbox_for_eviction, claim_suspended_sandbox_for_eviction, cleanup_episode,
    complete_row_intent_cleanup, get_local_artifact, get_sandbox, insert_restore_transfer_scope,
    insert_sandbox, insert_sandbox_with_cleanup_episode_and_route_evidence, mark_cleanup_failed,
    mark_cleanup_required, mark_quarantined_from_state, pin_suspended_artifact_for_cleanup,
    publish_restored_disk_as_new, publish_restored_suspended_artifact,
    publish_restored_suspended_without_artifact, publish_stopped_artifact,
    quarantine_cleanup_episode, rearm_prepared_clone_disposal, record_cleanup_episode_failure,
    recover_interrupted_pause_state, request_cleanup_episode,
    restore_adopt_orphan_or_insert_sandbox, restore_resident_resume_state, restore_transfer_scope,
    rollback_restoring_episode, rollback_starting_episode, rollback_suspending_to_paused,
    test_conn,
};
use crate::restore_operation::{RemoteRestoreOperation, RestoreOperation};
use crate::state::SandboxState;

fn assert_unchanged(conn: &Connection, before: &BTreeMap<String, Vec<String>>, case: &str) {
    let after = database_rows(conn);
    let changed: Vec<&String> = after
        .iter()
        .filter(|(table, rows)| before.get(*table) != Some(*rows))
        .map(|(table, _)| table)
        .collect();
    assert!(
        changed.is_empty(),
        "{case}: a refused write must leave every row unchanged; changed tables: {changed:?}"
    );
}

fn assert_no_rows(result: crate::db::Result<()>, case: &str) {
    assert!(
        matches!(result, Err(ref error) if error.is_no_rows()),
        "{case}: expected the guarded write to refuse with QueryReturnedNoRows, got {result:?}"
    );
}

/// Every lifecycle state except the ones a writer accepts, so a refusal sweep
/// also covers states added to the enum later.
fn states_except(accepted: &[SandboxState]) -> impl Iterator<Item = SandboxState> + '_ {
    SandboxState::all()
        .into_iter()
        .filter(move |state| !accepted.contains(state))
}

fn row(conn: &Connection, id: &str) -> SandboxRow {
    get_sandbox(conn, id).unwrap().unwrap()
}

fn episode(conn: &Connection, id: &str) -> Option<CleanupEpisode> {
    cleanup_episode(conn, id).unwrap()
}

fn snapshot_path(id: &str) -> String {
    format!("/cache/{id}/snapshot")
}

fn with_insert_params<T>(
    state: SandboxState,
    insert: impl FnOnce(InsertSandboxParams<'_>) -> T,
) -> (String, T) {
    let sandbox_id = SandboxId::new();
    let id = sandbox_id.to_string();
    let uuid8 = SandboxRuntimeLabel::from_sandbox_id(sandbox_id);
    let user = SandboxOwner::parse("user").unwrap();
    let process_env = BTreeMap::new();
    let auto_lifecycle = AutoLifecyclePolicy::disabled();
    let inserted = insert(InsertSandboxParams {
        guest_image: Default::default(),
        id: &id,
        uuid8: &uuid8,
        state,
        template: "seed",
        template_hash: Some("hash"),
        user: &user,
        runtime: "fc",
        snapshots_enabled: false,
        rootfs_size_mb: None,
        disk_io_limits: Default::default(),
        pool_template: None,
        volume_encryption: false,
        process_env: &process_env,
        auto_lifecycle: &auto_lifecycle,
        network_policy_override: None,
        network_policy_generation: 0,
        network_policy_authority: PolicyAuthority::Cluster,
        auto_lifecycle_authority: PolicyAuthority::Cluster,
    });
    (id, inserted)
}

fn insert_row(conn: &Connection, state: SandboxState) -> String {
    let (id, inserted) = with_insert_params(state, |params| insert_sandbox(conn, params));
    inserted.unwrap();
    id
}

/// A row in `state` owning an armed cleanup episode. The episode table admits
/// only activation states (`creating`, `starting`, `restoring`), so the row is
/// inserted as a create's and other states are reached by moving it afterwards.
fn insert_with_episode(
    conn: &Connection,
    state: SandboxState,
    completion: CleanupCompletion,
) -> (String, SandboxCleanupEpisodeId) {
    let cleanup_id = SandboxCleanupEpisodeId::new();
    let (id, inserted) = with_insert_params(SandboxState::Creating, |params| {
        insert_sandbox_with_cleanup_episode_and_route_evidence(
            conn,
            params,
            CleanupEpisodeSpec {
                id: cleanup_id,
                completion,
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
    });
    inserted.unwrap();
    conn.execute(
        "UPDATE sandboxes SET state = ?1 WHERE id = ?2",
        params![state.as_str(), id],
    )
    .unwrap();
    assert_eq!(row(conn, &id).state, state);
    (id, cleanup_id)
}

/// A `suspended` row pinning a published suspend artifact.
fn insert_suspended_with_artifact(conn: &Connection) -> (String, LocalArtifactId) {
    let id = insert_row(conn, SandboxState::Paused);
    assert!(begin_suspend(conn, &id).unwrap());
    let artifact = LocalArtifactId::new();
    complete_row_intent_cleanup(
        conn,
        &id,
        CleanupCompletion::MarkSuspended,
        Some((artifact, &snapshot_path(&id))),
    )
    .unwrap();
    assert_eq!(row(conn, &id).current_local_artifact_id, Some(artifact));
    (id, artifact)
}

/// A local restore in flight: `restoring` and still pinning its artifact.
fn insert_local_restore(conn: &Connection) -> (String, LocalArtifactId, CleanupEpisode) {
    let (id, artifact) = insert_suspended_with_artifact(conn);
    let episode = begin_activation(
        conn,
        &id,
        SandboxState::Suspended,
        ActivationKind::Restore,
        None,
        None,
    )
    .unwrap()
    .unwrap();
    let restoring = row(conn, &id);
    assert_eq!(restoring.state, SandboxState::Restoring);
    assert_eq!(restoring.current_local_artifact_id, Some(artifact));
    (id, artifact, episode)
}

/// A cross-node restore in flight: `restoring`, unpinned, and carrying the
/// remote restore operation its admission persisted.
fn insert_remote_restore(conn: &Connection) -> String {
    let source: ExactStorageUri = "s3://snapshots/suspend/restore?region=us-east-1"
        .parse()
        .unwrap();
    let operation = RemoteRestoreOperation::artifact_staging(source);
    let (id, inserted) = with_insert_params(SandboxState::Restoring, |params| {
        restore_adopt_orphan_or_insert_sandbox(conn, params, &operation)
    });
    inserted.unwrap();
    let restoring = row(conn, &id);
    assert_eq!(restoring.state, SandboxState::Restoring);
    assert_eq!(restoring.current_local_artifact_id, None);
    assert_eq!(
        restoring.restore_operation,
        Some(RestoreOperation::Remote(operation))
    );
    id
}

fn insert_cold_start(conn: &Connection) -> (String, CleanupEpisode) {
    let id = insert_row(conn, SandboxState::Stopped);
    let episode = begin_activation(
        conn,
        &id,
        SandboxState::Stopped,
        ActivationKind::ColdStart,
        None,
        None,
    )
    .unwrap()
    .unwrap();
    assert_eq!(row(conn, &id).state, SandboxState::Starting);
    (id, episode)
}

fn add_transfer_scope(conn: &Connection, id: &str) {
    assert!(
        insert_restore_transfer_scope(
            conn,
            id,
            TransferScopeId::new(),
            None,
            None,
            &format!("/cache/restore-staging/{id}"),
        )
        .unwrap()
    );
}

/// Seeds a suspend-artifact pin without its `local_artifacts` row, the shape
/// a re-suspend's predecessor and a pinned eviction candidate carry.
fn set_suspend_pin(conn: &Connection, id: &str) -> LocalArtifactId {
    let pin = LocalArtifactId::new();
    conn.execute(
        "UPDATE sandboxes
            SET current_local_artifact_id = ?1,
                current_local_artifact_kind = 'full',
                current_local_artifact_origin = 'suspend'
          WHERE id = ?2",
        params![pin.to_string(), id],
    )
    .unwrap();
    pin
}

fn set_runtime_pid(conn: &Connection, id: &str) {
    conn.execute("UPDATE sandboxes SET pid = 42 WHERE id = ?1", params![id])
        .unwrap();
}

fn stale_version(row: &SandboxRow) -> String {
    (row.updated_at.parse::<i64>().unwrap() - 1).to_string()
}

#[test]
fn interrupted_pause_recovery_settles_only_a_pausing_row() {
    let conn = test_conn();
    for target in [
        PauseFailedRecoveryTarget::Started,
        PauseFailedRecoveryTarget::Paused,
    ] {
        let id = insert_row(&conn, SandboxState::Pausing);
        conn.execute(
            "UPDATE sandboxes SET failure_reason = 'pause interrupted' WHERE id = ?1",
            params![id],
        )
        .unwrap();
        assert!(recover_interrupted_pause_state(&conn, &id, target).unwrap());
        let recovered = row(&conn, &id);
        assert_eq!(recovered.state, target.sandbox_state());
        assert_eq!(recovered.failure_reason, None);
    }

    for state in states_except(&[SandboxState::Pausing]) {
        let id = insert_row(&conn, state);
        let before = database_rows(&conn);
        assert!(
            !recover_interrupted_pause_state(&conn, &id, PauseFailedRecoveryTarget::Started)
                .unwrap()
        );
        assert_unchanged(&conn, &before, state.as_str());
    }
    let before = database_rows(&conn);
    assert!(
        !recover_interrupted_pause_state(
            &conn,
            &SandboxId::new().to_string(),
            PauseFailedRecoveryTarget::Paused
        )
        .unwrap()
    );
    assert_unchanged(&conn, &before, "missing row");
}

#[test]
fn prepared_clone_disposal_rearms_only_its_own_quarantined_pool_clone_episode() {
    let conn = test_conn();
    for state in states_except(&[SandboxState::Quarantined]) {
        let (id, cleanup_id) =
            insert_with_episode(&conn, state, CleanupCompletion::DeletePoolClone);
        let before = database_rows(&conn);
        assert!(
            !rearm_prepared_clone_disposal(&conn, &id, cleanup_id).unwrap(),
            "{state}"
        );
        assert_unchanged(&conn, &before, state.as_str());
    }
    let quarantine_create = |id: &str| {
        assert!(
            mark_quarantined_from_state(
                &conn,
                id,
                SandboxState::Creating,
                SandboxQuarantineSource::SbxManager,
                SandboxQuarantineReasonCode::CreateInterrupted,
                "held create",
                SandboxQuarantineNextAction::RequiresOperatorAttention,
            )
            .unwrap()
        );
    };

    // A disposal held after its budget ran out: requested, charged, failed,
    // then quarantined with its attempt history intact.
    let (exhausted, exhausted_cleanup) = insert_with_episode(
        &conn,
        SandboxState::Creating,
        CleanupCompletion::DeletePoolClone,
    );
    assert_eq!(
        request_cleanup_episode(
            &conn,
            &exhausted,
            exhausted_cleanup,
            SandboxQuarantineReasonCode::CreateInterrupted,
            "interrupted clone",
        )
        .unwrap(),
        CleanupEpisodeRequestOutcome::Requested
    );
    assert!(charge_prepared_clone_disposal_attempt(&conn, &exhausted, exhausted_cleanup).unwrap());
    assert!(
        record_cleanup_episode_failure(&conn, &exhausted, exhausted_cleanup, "clone delete failed")
            .unwrap()
    );
    assert_eq!(row(&conn, &exhausted).state, SandboxState::CleanupFailed);
    let before = database_rows(&conn);
    assert!(!rearm_prepared_clone_disposal(&conn, &exhausted, exhausted_cleanup).unwrap());
    assert_unchanged(&conn, &before, "requested disposal is not quarantined");
    assert!(
        quarantine_cleanup_episode(&conn, &exhausted, exhausted_cleanup, "clone root busy")
            .unwrap()
    );
    assert_eq!(row(&conn, &exhausted).state, SandboxState::Quarantined);
    let held = episode(&conn, &exhausted).unwrap();
    assert_eq!(held.attempt_count, 1);
    assert_eq!(
        held.last_cleanup_error.as_deref(),
        Some("clone delete failed")
    );

    // A create quarantined independently while its disposal was still armed.
    let (armed, armed_cleanup) = insert_with_episode(
        &conn,
        SandboxState::Creating,
        CleanupCompletion::DeletePoolClone,
    );
    quarantine_create(&armed);
    let held = episode(&conn, &armed).unwrap();
    assert_eq!(held.requested_at, None);
    assert_eq!(held.quarantine_reason_code, None);

    let before = database_rows(&conn);
    assert!(!rearm_prepared_clone_disposal(&conn, &armed, SandboxCleanupEpisodeId::new()).unwrap());
    assert_unchanged(&conn, &before, "another cleanup episode");

    let (other_id, other_cleanup) =
        insert_with_episode(&conn, SandboxState::Creating, CleanupCompletion::DeleteRow);
    quarantine_create(&other_id);
    let before = database_rows(&conn);
    assert!(!rearm_prepared_clone_disposal(&conn, &other_id, other_cleanup).unwrap());
    assert_unchanged(&conn, &before, "episode does not dispose of a pool clone");

    for (case, id, cleanup_id) in [
        ("exhausted disposal", &exhausted, exhausted_cleanup),
        ("armed disposal", &armed, armed_cleanup),
    ] {
        assert!(
            rearm_prepared_clone_disposal(&conn, id, cleanup_id).unwrap(),
            "{case}"
        );
        let rearmed = row(&conn, id);
        assert_eq!(rearmed.state, SandboxState::CleanupRequired, "{case}");
        assert_eq!(
            rearmed.cleanup_completion,
            Some(CleanupCompletion::DeletePoolClone),
            "{case}"
        );
        assert_eq!(rearmed.cleanup_keep_filesystem, Some(false), "{case}");
        let rearmed_episode = episode(&conn, id).unwrap();
        assert_eq!(rearmed_episode.id, cleanup_id, "{case}");
        assert!(rearmed_episode.requested_at.is_some(), "{case}");
        assert_eq!(rearmed_episode.attempt_count, 0, "{case}");
        assert_eq!(rearmed_episode.last_cleanup_error, None, "{case}");
        assert_eq!(
            rearmed_episode.quarantine_reason_code,
            Some(SandboxQuarantineReasonCode::CreateInterrupted),
            "{case}"
        );

        let before = database_rows(&conn);
        assert!(
            !rearm_prepared_clone_disposal(&conn, id, cleanup_id).unwrap(),
            "{case}"
        );
        assert_unchanged(&conn, &before, &format!("{case}: already rearmed"));
    }
}

#[test]
fn teardown_suspend_begins_only_from_paused_and_a_refusal_enqueues_nothing() {
    let conn = test_conn();
    let generation = ExecutionGeneration::new(7).unwrap();
    for state in states_except(&[SandboxState::Paused]) {
        let id = insert_row(&conn, state);
        let before = database_rows(&conn);
        assert_no_rows(
            begin_teardown_suspend(&conn, &id, generation).map(|_| ()),
            state.as_str(),
        );
        assert_unchanged(&conn, &before, state.as_str());
    }

    let id = insert_row(&conn, SandboxState::Paused);
    let before = database_rows(&conn);
    begin_teardown_suspend(&conn, &id, generation).unwrap();
    assert_eq!(row(&conn, &id).state, SandboxState::Suspending);
    assert!(database_rows(&conn) != before);

    let before = database_rows(&conn);
    assert_no_rows(
        begin_teardown_suspend(&conn, &id, generation).map(|_| ()),
        "already suspending",
    );
    assert_unchanged(&conn, &before, "already suspending");
}

#[test]
fn suspend_rollback_returns_only_a_suspending_row_to_paused() {
    let conn = test_conn();
    let resuspend = insert_row(&conn, SandboxState::Paused);
    let predecessor = set_suspend_pin(&conn, &resuspend);
    let first_suspend = insert_row(&conn, SandboxState::Paused);
    for (id, restored) in [(&resuspend, Some(predecessor)), (&first_suspend, None)] {
        assert!(begin_suspend(&conn, id).unwrap());
        assert_eq!(row(&conn, id).current_local_artifact_id, None);
        assert!(rollback_suspending_to_paused(&conn, id).unwrap());
        let paused = row(&conn, id);
        assert_eq!(paused.state, SandboxState::Paused);
        assert_eq!(paused.current_local_artifact_id, restored);
        assert_eq!(
            paused.current_local_artifact_kind,
            restored.map(|_| ArtifactKind::Full)
        );
        assert_eq!(
            paused.current_local_artifact_origin,
            restored.map(|_| ArtifactOrigin::Suspend)
        );
        assert_eq!(paused.cleanup_completion, None);
        assert_eq!(paused.cleanup_keep_filesystem, None);
        assert_eq!(paused.failure_reason, None);
    }

    let (suspended, _) = insert_suspended_with_artifact(&conn);
    let refused = states_except(&[SandboxState::Suspending])
        .map(|state| (state.as_str(), insert_row(&conn, state)))
        .chain([
            ("already paused", resuspend),
            ("suspended with its artifact", suspended),
        ])
        .collect::<Vec<_>>();
    for (case, id) in &refused {
        let before = database_rows(&conn);
        assert!(!rollback_suspending_to_paused(&conn, id).unwrap(), "{case}");
        assert_unchanged(&conn, &before, case);
    }
}

#[test]
fn activation_episode_rollback_requires_the_activation_state_and_its_armed_episode() {
    let conn = test_conn();
    let (starting, start_episode) = insert_cold_start(&conn);
    let (restoring, restore_artifact, restore_episode) = insert_local_restore(&conn);

    let refusals: [(&str, &str, SandboxState, SandboxCleanupEpisodeId, bool); 4] = [
        (
            "another episode",
            &starting,
            SandboxState::Stopped,
            SandboxCleanupEpisodeId::new(),
            false,
        ),
        (
            "restoring rollback of a starting row",
            &starting,
            SandboxState::Stopped,
            start_episode.id,
            true,
        ),
        (
            "starting rollback of a restoring row",
            &restoring,
            SandboxState::Suspended,
            restore_episode.id,
            false,
        ),
        (
            "another episode",
            &restoring,
            SandboxState::Suspended,
            SandboxCleanupEpisodeId::new(),
            true,
        ),
    ];
    for (case, id, target, cleanup_id, restoring_wrapper) in refusals {
        let before = database_rows(&conn);
        let changed = if restoring_wrapper {
            rollback_restoring_episode(&conn, id, target, cleanup_id)
        } else {
            rollback_starting_episode(&conn, id, target, cleanup_id)
        };
        assert!(!changed.unwrap(), "{case}");
        assert_unchanged(&conn, &before, case);
    }
    for restoring_wrapper in [false, true] {
        let activation_state = if restoring_wrapper {
            SandboxState::Restoring
        } else {
            SandboxState::Starting
        };
        for state in states_except(&[activation_state]) {
            let (id, cleanup_id) = insert_with_episode(&conn, state, CleanupCompletion::DeleteRow);
            let before = database_rows(&conn);
            let changed = if restoring_wrapper {
                rollback_restoring_episode(&conn, &id, SandboxState::Suspended, cleanup_id)
            } else {
                rollback_starting_episode(&conn, &id, SandboxState::Stopped, cleanup_id)
            };
            let case = format!("{activation_state} rollback of an armed {state} row");
            assert!(!changed.unwrap(), "{case}");
            assert_unchanged(&conn, &before, &case);
        }
    }

    let before = database_rows(&conn);
    assert!(matches!(
        rollback_starting_episode(
            &conn,
            &starting,
            SandboxState::Quarantined,
            start_episode.id
        ),
        Err(RollbackActivationEpisodeError::QuarantinedTarget)
    ));
    assert!(matches!(
        rollback_restoring_episode(
            &conn,
            &restoring,
            SandboxState::Quarantined,
            restore_episode.id
        ),
        Err(RollbackActivationEpisodeError::QuarantinedTarget)
    ));
    assert!(matches!(
        rollback_starting_episode(&conn, &starting, SandboxState::Failed, start_episode.id),
        Err(RollbackActivationEpisodeError::Db(ref error)) if error.is_invalid_query()
    ));
    assert_unchanged(&conn, &before, "rejected rollback target");

    assert!(
        rollback_starting_episode(&conn, &starting, SandboxState::Stopped, start_episode.id)
            .unwrap()
    );
    let stopped = row(&conn, &starting);
    assert_eq!(stopped.state, SandboxState::Stopped);
    assert_eq!(stopped.cleanup_completion, None);
    assert_eq!(stopped.cleanup_keep_filesystem, None);
    assert_eq!(stopped.failure_reason, None);
    assert!(episode(&conn, &starting).is_none());

    assert!(
        rollback_restoring_episode(
            &conn,
            &restoring,
            SandboxState::Suspended,
            restore_episode.id
        )
        .unwrap()
    );
    let suspended = row(&conn, &restoring);
    assert_eq!(suspended.state, SandboxState::Suspended);
    assert_eq!(suspended.restore_operation, None);
    assert_eq!(suspended.cleanup_completion, None);
    assert_eq!(suspended.cleanup_keep_filesystem, None);
    assert_eq!(suspended.current_local_artifact_id, Some(restore_artifact));
    assert!(episode(&conn, &restoring).is_none());

    let before = database_rows(&conn);
    assert!(
        !rollback_starting_episode(&conn, &starting, SandboxState::Stopped, start_episode.id)
            .unwrap()
    );
    assert!(
        !rollback_restoring_episode(
            &conn,
            &restoring,
            SandboxState::Suspended,
            restore_episode.id
        )
        .unwrap()
    );
    assert_unchanged(&conn, &before, "already rolled back");
}

#[test]
fn resident_resume_rollback_settles_only_a_resuming_row() {
    let conn = test_conn();
    for target in [SandboxState::Started, SandboxState::Paused] {
        let id = insert_row(&conn, SandboxState::Resuming);
        conn.execute(
            "UPDATE sandboxes SET failure_reason = 'resume failed' WHERE id = ?1",
            params![id],
        )
        .unwrap();
        assert!(restore_resident_resume_state(&conn, &id, target).unwrap());
        let restored = row(&conn, &id);
        assert_eq!(restored.state, target);
        assert_eq!(restored.failure_reason, None);
    }

    let resuming = insert_row(&conn, SandboxState::Resuming);
    let before = database_rows(&conn);
    for target in states_except(&[SandboxState::Started, SandboxState::Paused]) {
        assert!(
            matches!(
                restore_resident_resume_state(&conn, &resuming, target),
                Err(error) if error.is_invalid_query()
            ),
            "{target}"
        );
    }
    assert_unchanged(&conn, &before, "not a resident stable state");

    for state in states_except(&[SandboxState::Resuming]) {
        let id = insert_row(&conn, state);
        let before = database_rows(&conn);
        assert!(
            !restore_resident_resume_state(&conn, &id, SandboxState::Started).unwrap(),
            "{state}"
        );
        assert_unchanged(&conn, &before, state.as_str());
    }
}

#[test]
fn suspended_cleanup_pin_requires_a_suspending_row_and_a_refusal_keeps_prior_artifacts() {
    let conn = test_conn();
    let (suspended, prior) = insert_suspended_with_artifact(&conn);
    let refused = states_except(&[SandboxState::Suspending])
        .map(|state| (state.as_str(), insert_row(&conn, state)))
        .chain([("suspended with its artifact", suspended)])
        .collect::<Vec<_>>();
    for (case, id) in &refused {
        let before = database_rows(&conn);
        let artifact = LocalArtifactId::new();
        assert_no_rows(
            pin_suspended_artifact_for_cleanup(&conn, id, artifact, &snapshot_path(id)),
            case,
        );
        assert_unchanged(&conn, &before, case);
        assert!(get_local_artifact(&conn, artifact).unwrap().is_none());
    }
    assert!(get_local_artifact(&conn, prior).unwrap().is_some());

    let id = insert_row(&conn, SandboxState::Paused);
    assert!(begin_suspend(&conn, &id).unwrap());
    let artifact = LocalArtifactId::new();
    pin_suspended_artifact_for_cleanup(&conn, &id, artifact, &snapshot_path(&id)).unwrap();
    let pinned = row(&conn, &id);
    assert_eq!(pinned.state, SandboxState::CleanupRequired);
    assert_eq!(pinned.current_local_artifact_id, Some(artifact));
    assert_eq!(pinned.current_local_artifact_kind, Some(ArtifactKind::Full));
    assert_eq!(
        pinned.current_local_artifact_origin,
        Some(ArtifactOrigin::Suspend)
    );
    assert_eq!(
        pinned.cleanup_completion,
        Some(CleanupCompletion::MarkSuspended)
    );
    assert_eq!(pinned.cleanup_keep_filesystem, Some(true));
    assert_eq!(
        get_local_artifact(&conn, artifact)
            .unwrap()
            .unwrap()
            .local_path,
        snapshot_path(&id)
    );
}

#[derive(Clone, Copy, Debug)]
enum RestoredSuspendedPublication {
    WithArtifact,
    WithoutArtifact,
}

impl RestoredSuspendedPublication {
    fn publish(
        self,
        conn: &Connection,
        id: &str,
        artifact: LocalArtifactId,
    ) -> crate::db::Result<()> {
        match self {
            Self::WithArtifact => {
                publish_restored_suspended_artifact(conn, id, artifact, &snapshot_path(id))
            }
            Self::WithoutArtifact => publish_restored_suspended_without_artifact(conn, id),
        }
    }
}

#[test]
fn restored_suspended_publication_requires_an_unpinned_restoring_row() {
    for publication in [
        RestoredSuspendedPublication::WithArtifact,
        RestoredSuspendedPublication::WithoutArtifact,
    ] {
        let conn = test_conn();
        let (suspended, prior) = insert_suspended_with_artifact(&conn);
        let (pinned_restore, _, _) = insert_local_restore(&conn);
        let refused = states_except(&[SandboxState::Restoring])
            .map(|state| (state.as_str(), insert_row(&conn, state)))
            .chain([
                ("suspended with its artifact", suspended),
                ("restoring with a pinned artifact", pinned_restore),
            ])
            .collect::<Vec<_>>();
        for (case, id) in &refused {
            add_transfer_scope(&conn, id);
            let before = database_rows(&conn);
            let artifact = LocalArtifactId::new();
            let case = format!("{publication:?}: {case}");
            assert_no_rows(publication.publish(&conn, id, artifact), &case);
            assert_unchanged(&conn, &before, &case);
        }
        assert!(get_local_artifact(&conn, prior).unwrap().is_some());

        let id = insert_remote_restore(&conn);
        add_transfer_scope(&conn, &id);
        let artifact = LocalArtifactId::new();
        publication.publish(&conn, &id, artifact).unwrap();
        let published = row(&conn, &id);
        assert_eq!(published.state, SandboxState::Suspended, "{publication:?}");
        assert_eq!(published.restore_operation, None, "{publication:?}");
        let expected = match publication {
            RestoredSuspendedPublication::WithArtifact => Some(artifact),
            RestoredSuspendedPublication::WithoutArtifact => None,
        };
        assert_eq!(
            published.current_local_artifact_id, expected,
            "{publication:?}"
        );
        assert_eq!(
            get_local_artifact(&conn, artifact).unwrap().is_some(),
            expected.is_some(),
            "{publication:?}"
        );
        assert_eq!(
            restore_transfer_scope(&conn, &id).unwrap(),
            None,
            "{publication:?}: publication retires the staging scope"
        );
    }
}

#[test]
fn restored_disk_as_new_publication_requires_a_restoring_row() {
    let conn = test_conn();
    for state in states_except(&[SandboxState::Restoring]) {
        let id = insert_row(&conn, state);
        set_runtime_pid(&conn, &id);
        add_transfer_scope(&conn, &id);
        let before = database_rows(&conn);
        assert_no_rows(publish_restored_disk_as_new(&conn, &id), state.as_str());
        assert_unchanged(&conn, &before, state.as_str());
    }

    // `restored_disk_as_new_becomes_a_stopped_sandbox_without_donor_runtime_identity`
    // pins the published stopped shape; this adds the remote restore operation
    // a real cross-node row carries.
    let id = insert_remote_restore(&conn);
    publish_restored_disk_as_new(&conn, &id).unwrap();
    assert_eq!(row(&conn, &id).restore_operation, None);
}

#[test]
fn stopped_artifact_publication_accepts_only_stop_completions_and_unpinned_stopped_rows() {
    let conn = test_conn();
    let cleanup = |state: SandboxState, completion: CleanupCompletion| {
        let id = insert_row(&conn, SandboxState::Stopping);
        match state {
            SandboxState::CleanupRequired => {
                mark_cleanup_required(&conn, &id, true, completion, "stop cleanup").unwrap();
            }
            SandboxState::CleanupFailed => {
                assert!(mark_cleanup_failed(&conn, &id, true, completion, "stop cleanup").unwrap());
            }
            _ => unreachable!(),
        }
        let staged = row(&conn, &id);
        assert_eq!(staged.state, state);
        assert_eq!(staged.cleanup_completion, Some(completion));
        id
    };

    // `stopped_publication_commits_disk_identity_with_terminal_state` pins
    // publication from `stopping` and the published artifact identity; these
    // are the other accepted shapes and what publication clears on them.
    let accepted = [
        (
            "cleanup_required to stopped",
            cleanup(
                SandboxState::CleanupRequired,
                CleanupCompletion::MarkStopped,
            ),
        ),
        (
            "cleanup_failed to stopped",
            cleanup(SandboxState::CleanupFailed, CleanupCompletion::MarkStopped),
        ),
        ("unpinned stopped", insert_row(&conn, SandboxState::Stopped)),
    ];
    for (case, id) in &accepted {
        set_runtime_pid(&conn, id);
        let artifact = LocalArtifactId::new();
        publish_stopped_artifact(
            &conn,
            id,
            get_sandbox(&conn, id).unwrap().unwrap().node_execution_id,
            artifact,
            &format!("/cache/{id}/disk"),
        )
        .unwrap();
        let published = row(&conn, id);
        assert_eq!(published.state, SandboxState::Stopped, "{case}");
        assert_eq!(
            published.current_local_artifact_id,
            Some(artifact),
            "{case}"
        );
        assert_eq!(published.pid, None, "{case}");
        assert_eq!(published.cleanup_completion, None, "{case}");
        assert_eq!(published.cleanup_keep_filesystem, None, "{case}");
    }

    // `cleanup_required` and `cleanup_failed` are refused through the
    // completions below; a bare row in either state is not a production shape.
    let swept = states_except(&[
        SandboxState::Stopping,
        SandboxState::Stopped,
        SandboxState::CleanupRequired,
        SandboxState::CleanupFailed,
    ])
    .map(|state| {
        let id = insert_row(&conn, state);
        set_runtime_pid(&conn, &id);
        (state.as_str(), id)
    })
    .collect::<Vec<_>>();
    let (suspended, _) = insert_suspended_with_artifact(&conn);
    let refused = swept.into_iter().chain([
        ("suspended with its artifact", suspended),
        (
            "cleanup_required to delete",
            cleanup(SandboxState::CleanupRequired, CleanupCompletion::DeleteRow),
        ),
        (
            "cleanup_failed to suspended",
            cleanup(
                SandboxState::CleanupFailed,
                CleanupCompletion::MarkSuspended,
            ),
        ),
        ("pinned stopped", accepted[2].1.clone()),
    ]);
    for (case, id) in refused {
        let before = database_rows(&conn);
        let artifact = LocalArtifactId::new();
        assert_no_rows(
            publish_stopped_artifact(
                &conn,
                &id,
                get_sandbox(&conn, &id).unwrap().unwrap().node_execution_id,
                artifact,
                &format!("/cache/{id}/disk"),
            ),
            case,
        );
        assert_unchanged(&conn, &before, case);
    }
}

#[test]
fn stopped_eviction_claim_binds_the_state_and_the_observed_version() {
    let conn = test_conn();
    let id = insert_row(&conn, SandboxState::Stopped);
    let observed = row(&conn, &id);
    let refused = states_except(&[SandboxState::Stopped])
        .map(|state| {
            let other = insert_row(&conn, state);
            let version = row(&conn, &other).updated_at;
            (state.as_str(), other, version)
        })
        .chain([("stale version", id.clone(), stale_version(&observed))])
        .collect::<Vec<_>>();
    for (case, id, version) in &refused {
        let before = database_rows(&conn);
        assert!(
            !claim_stopped_sandbox_for_eviction(&conn, id, version).unwrap(),
            "{case}"
        );
        assert_unchanged(&conn, &before, case);
    }

    assert!(claim_stopped_sandbox_for_eviction(&conn, &id, &observed.updated_at).unwrap());
    assert_eq!(row(&conn, &id).state, SandboxState::Deleting);

    let before = database_rows(&conn);
    assert!(!claim_stopped_sandbox_for_eviction(&conn, &id, &observed.updated_at).unwrap());
    assert_unchanged(&conn, &before, "already claimed");
}

#[test]
fn pinned_suspended_eviction_claim_with_a_version_requires_state_pin_and_version() {
    let conn = test_conn();
    let (id, pin) = insert_suspended_with_artifact(&conn);
    let observed = row(&conn, &id);
    let pin = pin.to_string();
    let (restoring, restoring_pin, _) = insert_local_restore(&conn);
    let restoring_version = row(&conn, &restoring).updated_at;
    let refused = states_except(&[SandboxState::Suspended])
        .map(|state| {
            let other = insert_row(&conn, state);
            let other_pin = set_suspend_pin(&conn, &other).to_string();
            let version = row(&conn, &other).updated_at;
            (state.as_str(), other, other_pin, version)
        })
        .chain([
            (
                "stale version",
                id.clone(),
                pin.clone(),
                stale_version(&observed),
            ),
            (
                "another pin",
                id.clone(),
                LocalArtifactId::new().to_string(),
                observed.updated_at.clone(),
            ),
            (
                "restoring with its restore artifact",
                restoring,
                restoring_pin.to_string(),
                restoring_version,
            ),
        ])
        .collect::<Vec<_>>();
    for (case, id, claim_pin, version) in &refused {
        let before = database_rows(&conn);
        assert!(
            !claim_suspended_sandbox_for_eviction(&conn, id, Some(claim_pin), Some(version))
                .unwrap(),
            "{case}"
        );
        assert_unchanged(&conn, &before, case);
    }

    assert!(
        claim_suspended_sandbox_for_eviction(&conn, &id, Some(&pin), Some(&observed.updated_at))
            .unwrap()
    );
    assert_eq!(row(&conn, &id).state, SandboxState::Deleting);

    let before = database_rows(&conn);
    assert!(
        !claim_suspended_sandbox_for_eviction(&conn, &id, Some(&pin), Some(&observed.updated_at))
            .unwrap()
    );
    assert_unchanged(&conn, &before, "already claimed");
}
