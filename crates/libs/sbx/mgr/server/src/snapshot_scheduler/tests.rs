use pplx_space_common_core::types::SandboxId;

use super::*;

#[tokio::test]
async fn checkpoint_retries_preserve_disk_cadence_and_replacement_registration() {
    for (due, tier) in [
        (10, CheckpointTier::TenMinutes),
        (60, CheckpointTier::OneHour),
        (360, CheckpointTier::SixHours),
    ] {
        for completion in [
            CheckpointCompletion::Complete,
            CheckpointCompletion::CompleteWithLifecycleError("resume failed".into()),
            CheckpointCompletion::SkippedNoCapture("sandbox no longer running"),
        ] {
            let states = Mutex::new(HashMap::from([(
                "sandbox".to_owned(),
                SandboxSnapshotState {
                    tick_count: due - 1,
                    pending_checkpoint: None,
                    template: "seed".into(),
                },
            )]));
            // The operation can fail before any capture, after the first disk
            // capture, or after both disks. Its error must retain the checkpoint,
            // not roll back the cadence that selected the disk tiers.
            for (attempt, failure) in [
                "inventory failed",
                "1m capture failed",
                "5m capture failed after 1m publication",
                "checkpoint failed after disk publication",
            ]
            .into_iter()
            .enumerate()
            {
                run_scheduled_tick(&states, "sandbox", async |tick, pending| {
                    assert_eq!(tick, due + attempt as u64);
                    assert_eq!(pending, Some(tier));
                    assert_eq!(tick.is_multiple_of(5), attempt == 0);
                    Err(failure.into())
                })
                .await
                .unwrap_err();
            }
            let lifecycle_failed = matches!(
                completion,
                CheckpointCompletion::CompleteWithLifecycleError(_)
            );
            let result = run_scheduled_tick(&states, "sandbox", async |tick, pending| {
                assert_eq!(tick, due + 4);
                assert_eq!(pending, Some(tier));
                Ok(completion)
            })
            .await;
            assert_eq!(result.is_err(), lifecycle_failed);
            run_scheduled_tick(&states, "sandbox", async |tick, pending| {
                assert_eq!(tick, due + 5);
                assert_eq!(pending, None, "completed capture must not be replayed");
                Ok(CheckpointCompletion::Complete)
            })
            .await
            .unwrap();

            for succeeded in [false, true] {
                let result = run_scheduled_tick(&states, "sandbox", async |_, _| {
                    // A stop/start replaces the registration during the old work.
                    states.lock().await.insert(
                        "sandbox".into(),
                        SandboxSnapshotState {
                            tick_count: 0,
                            pending_checkpoint: None,
                            template: "seed".into(),
                        },
                    );
                    if succeeded {
                        Ok(CheckpointCompletion::Complete)
                    } else {
                        Err("old registration failed".into())
                    }
                })
                .await;
                assert_eq!(result.is_ok(), succeeded);
                run_scheduled_tick(&states, "sandbox", async |tick, pending| {
                    assert_eq!(tick, 1);
                    assert_eq!(pending, None);
                    Ok(CheckpointCompletion::Complete)
                })
                .await
                .unwrap();
            }
        }
    }

    // A backlog stays bounded to the largest due tier, rather than building a
    // queue or downgrading an overdue long-interval capture on a shorter tick.
    for (tick, pending, expected) in [
        (60, CheckpointTier::TenMinutes, CheckpointTier::OneHour),
        (70, CheckpointTier::OneHour, CheckpointTier::OneHour),
        (360, CheckpointTier::OneHour, CheckpointTier::SixHours),
        (370, CheckpointTier::SixHours, CheckpointTier::SixHours),
    ] {
        let states = Mutex::new(HashMap::from([(
            "sandbox".to_owned(),
            SandboxSnapshotState {
                tick_count: tick - 1,
                pending_checkpoint: Some(pending),
                template: "seed".into(),
            },
        )]));
        run_scheduled_tick(&states, "sandbox", async |_, pending| {
            assert_eq!(pending, Some(expected));
            Ok(CheckpointCompletion::Complete)
        })
        .await
        .unwrap();
    }
}
#[test]
fn snapshot_names_use_utc_calendar_time() {
    for (input, expected) in [
        ("1970-01-01T00:00:00Z", "19700101T000000Z"),
        ("2024-02-29T23:59:59Z", "20240229T235959Z"),
        ("2026-09-18T23:30:00-07:00", "20260919T063000Z"),
        ("2100-03-01T00:00:00Z", "21000301T000000Z"),
    ] {
        assert_eq!(snapshot_timestamp(input.parse().unwrap()), expected);
    }
}

fn checkpoint_context(state: SandboxState) -> (CheckpointContext, String) {
    let conn = crate::db::test_conn();
    let sandbox_id = SandboxId::new().to_string();
    conn.execute(
        "INSERT INTO sandboxes (id, uuid8, state, template, user, created_at, updated_at)
         VALUES (?1, 'runtime1', ?2, 'seed',
                 'user:00000000-0000-0000-0000-000000000001', '1', '1')",
        crate::db::fixture::params![sandbox_id, state.as_str()],
    )
    .unwrap();
    (
        CheckpointContext {
            db: crate::db::SharedConnection::new(conn),
            auto_lifecycle: crate::auto_lifecycle::AutoLifecycleCoordinator::default(),
            health_lifecycle: crate::watchdog::HealthLifecycleEpochs::default(),
            node_manager: crate::node_manager_transport::fake::unbound_in_absent_dir(),
        },
        sandbox_id,
    )
}

/// `do_checkpoint` reads `started` or `paused` and writes `checkpointing` under
/// the same connection guard, so its admission writes only from those states.
/// Every other row keeps every table unchanged and reports why.
#[test]
fn checkpoint_admission_writes_only_over_a_started_or_paused_row() {
    for state in SandboxState::all() {
        let expected_error = match state {
            SandboxState::Started | SandboxState::Paused => None,
            // The generic guards: a stopping or cleanup row is never revived,
            // and a deleting row owns its `delete_row` cleanup.
            SandboxState::Stopping
            | SandboxState::Deleting
            | SandboxState::CleanupRequired
            | SandboxState::CleanupFailed => Some(
                "sandbox state write was not applied: superseded_by_cleanup_episode".to_string(),
            ),
            SandboxState::Quarantined => {
                Some("sandbox state write was not applied: blocked_by_quarantine".to_string())
            }
            SandboxState::Creating
            | SandboxState::Created
            | SandboxState::Starting
            | SandboxState::Stopped
            | SandboxState::Pausing
            | SandboxState::Resuming
            | SandboxState::Checkpointing
            | SandboxState::Suspending
            | SandboxState::Suspended
            | SandboxState::Restoring
            | SandboxState::Failed => Some(format!(
                "sandbox state write was superseded: sandbox is {state}"
            )),
        };
        let (ctx, sandbox_id) = checkpoint_context(state);
        let db = ctx.db.lock().unwrap();
        let before = crate::db::test_fixtures::database_rows(&db);

        let result = begin_checkpoint_state(&db, &sandbox_id);

        match expected_error {
            None => {
                result.unwrap();
                let row = crate::db::get_sandbox(&db, &sandbox_id).unwrap().unwrap();
                assert_eq!(row.state, SandboxState::Checkpointing, "{state}");
            }
            Some(message) => {
                assert_eq!(result.unwrap_err(), message, "{state}");
                assert_eq!(
                    crate::db::test_fixtures::database_rows(&db),
                    before,
                    "{state}"
                );
            }
        }
    }
}

/// A missing row keeps the text the retired generic writer's outcome rendered.
#[test]
fn checkpoint_admission_of_a_missing_row_reports_it() {
    let db = crate::db::test_conn();
    let before = crate::db::test_fixtures::database_rows(&db);

    assert_eq!(
        begin_checkpoint_state(&db, &SandboxId::new().to_string()).unwrap_err(),
        "sandbox state write was not applied: missing"
    );
    assert_eq!(crate::db::test_fixtures::database_rows(&db), before);
}

#[tokio::test]
async fn skipped_checkpoint_never_publishes_an_artifact() {
    let cases = SandboxState::all()
        .into_iter()
        .filter(|state| !matches!(state, SandboxState::Started | SandboxState::Paused))
        .map(|state| (state, false))
        .chain([(SandboxState::Started, true), (SandboxState::Paused, true)]);
    for (state, pool_member) in cases {
        let (ctx, sandbox_id) = checkpoint_context(state);
        if pool_member {
            ctx.db.lock().unwrap().execute(
                "UPDATE sandboxes SET pool_template = 'seed', snapshots_enabled = 1 WHERE id = ?1",
                [&sandbox_id],
            ).unwrap();
        }
        let completion = do_checkpoint(&ctx, &sandbox_id, "f-10m-20260916T120000Z")
            .await
            .unwrap();
        assert!(
            completion.skip_reason().is_some(),
            "{state:?}, pool={pool_member}"
        );
        do_scheduled_checkpoint(&ctx, &sandbox_id, "f-10m-20260916T120000Z")
            .await
            .unwrap();
        let db = ctx.db.lock().unwrap();
        let row = crate::db::get_sandbox(&db, &sandbox_id).unwrap().unwrap();
        assert_eq!(row.state, state);
        assert!(!pool_member || !row.snapshots_enabled);
        let artifacts: i64 = db
            .query_row(
                "SELECT COUNT(*) FROM local_artifacts WHERE sandbox_id = ?1",
                [&sandbox_id],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(artifacts, 0, "{state:?}, pool={pool_member}");
    }
}

#[test]
fn checkpoint_failure_restores_the_previous_stable_state() {
    for stable_state in [SandboxState::Started, SandboxState::Paused] {
        let (ctx, sandbox_id) = checkpoint_context(SandboxState::Checkpointing);

        restore_checkpoint_state_best_effort(&ctx, &sandbox_id, stable_state);

        let db = ctx.db.lock().unwrap();
        let row = crate::db::get_sandbox(&db, &sandbox_id).unwrap().unwrap();
        assert_eq!(row.state, stable_state);
        assert!(row.failure_reason.is_none());
    }

    let (ctx, sandbox_id) = checkpoint_context(SandboxState::Deleting);
    restore_checkpoint_state_best_effort(&ctx, &sandbox_id, SandboxState::Started);
    assert_eq!(
        crate::db::get_sandbox(&ctx.db.lock().unwrap(), &sandbox_id)
            .unwrap()
            .unwrap()
            .state,
        SandboxState::Deleting
    );
}

#[test]
fn checkpoint_resume_ambiguity_quarantines_and_queues_publication() {
    let (ctx, sandbox_id) = checkpoint_context(SandboxState::Checkpointing);
    let reason = "snapshot failed; resume after checkpoint rollback failed: unavailable";

    assert_eq!(
        finish_checkpoint_rollback(
            &ctx,
            &sandbox_id,
            SandboxState::Started,
            "snapshot failed".to_string(),
            Err("unavailable".to_string()),
            None,
        ),
        reason
    );

    let db = ctx.db.lock().unwrap();
    let row = crate::db::get_sandbox(&db, &sandbox_id).unwrap().unwrap();
    assert_eq!(row.state, SandboxState::Quarantined);
    assert_eq!(row.failure_reason.as_deref(), Some(reason));
    assert_eq!(
        crate::db::current_quarantine_reason_wire_value(&db, &sandbox_id)
            .unwrap()
            .as_deref(),
        Some(SandboxQuarantineReasonCode::CheckpointResumeFailed.as_str())
    );
    assert_eq!(crate::db::list_node_dirty_refs(&db, 10).unwrap().len(), 1);
}

#[test]
fn checkpoint_pause_error_restores_started_only_after_resume_succeeds() {
    let (ctx, sandbox_id) = checkpoint_context(SandboxState::Checkpointing);

    let reason = finish_checkpoint_rollback(
        &ctx,
        &sandbox_id,
        SandboxState::Started,
        "pause for checkpoint failed: response lost".to_string(),
        Ok(()),
        None,
    );

    assert_eq!(reason, "pause for checkpoint failed: response lost");
    let row = crate::db::get_sandbox(&ctx.db.lock().unwrap(), &sandbox_id)
        .unwrap()
        .unwrap();
    assert_eq!(row.state, SandboxState::Started);

    let (ctx, sandbox_id) = checkpoint_context(SandboxState::Checkpointing);
    let reason = finish_checkpoint_rollback(
        &ctx,
        &sandbox_id,
        SandboxState::Started,
        "pause for checkpoint failed: response lost".to_string(),
        Err("resume response also lost".to_string()),
        None,
    );
    assert_eq!(
        reason,
        "pause for checkpoint failed: response lost; resume after checkpoint rollback failed: resume response also lost"
    );
    let db = ctx.db.lock().unwrap();
    assert_eq!(
        crate::db::get_sandbox(&db, &sandbox_id)
            .unwrap()
            .unwrap()
            .state,
        SandboxState::Quarantined
    );
    assert_eq!(
        crate::db::current_quarantine_reason_wire_value(&db, &sandbox_id)
            .unwrap()
            .as_deref(),
        Some(SandboxQuarantineReasonCode::CheckpointResumeFailed.as_str())
    );

    let (ctx, sandbox_id) = checkpoint_context(SandboxState::Checkpointing);
    finish_checkpoint_rollback(
        &ctx,
        &sandbox_id,
        SandboxState::Started,
        "snapshot failed".to_string(),
        Err("resume response lost".to_string()),
        Some(FirecrackerVmState::Paused),
    );
    let db = ctx.db.lock().unwrap();
    assert_eq!(
        crate::db::get_sandbox(&db, &sandbox_id)
            .unwrap()
            .unwrap()
            .state,
        SandboxState::Paused
    );
    assert!(
        crate::db::current_quarantine_reason_wire_value(&db, &sandbox_id)
            .unwrap()
            .is_none()
    );
}

#[test]
fn completed_checkpoint_remains_publishable_when_state_restore_is_superseded() {
    let (ctx, sandbox_id) = checkpoint_context(SandboxState::Deleting);

    let completion = finish_checkpoint_success(&ctx, &sandbox_id, SandboxState::Started);

    assert!(
        completion.is_ok(),
        "a completed snapshot must reach artifact publication even when lifecycle restoration is superseded: {completion:?}"
    );
    assert_eq!(
        crate::db::get_sandbox(&ctx.db.lock().unwrap(), &sandbox_id)
            .unwrap()
            .unwrap()
            .state,
        SandboxState::Deleting,
        "checkpoint completion must not overwrite the newer lifecycle state"
    );
}

#[test]
fn completed_checkpoint_only_succeeds_when_started_state_is_restored() {
    let (ctx, sandbox_id) = checkpoint_context(SandboxState::Checkpointing);
    let running_result = finish_completed_checkpoint_after_resume_error(
        &ctx,
        &sandbox_id,
        "resume response lost".to_string(),
        Some(FirecrackerVmState::Running),
    );
    running_result.unwrap();
    assert_eq!(
        crate::db::get_sandbox(&ctx.db.lock().unwrap(), &sandbox_id)
            .unwrap()
            .unwrap()
            .state,
        SandboxState::Started
    );

    let (ctx, sandbox_id) = checkpoint_context(SandboxState::Checkpointing);
    let paused_result = finish_completed_checkpoint_after_resume_error(
        &ctx,
        &sandbox_id,
        "resume failed".to_string(),
        Some(FirecrackerVmState::Paused),
    );
    assert_eq!(paused_result, Err("resume failed".to_string()));
    assert_eq!(
        crate::db::get_sandbox(&ctx.db.lock().unwrap(), &sandbox_id)
            .unwrap()
            .unwrap()
            .state,
        SandboxState::Paused,
        "a determinate Paused VM remains healthy but checkpoint completion is partial"
    );
}

/// The publication contract: a skipped capture wrote no snapshot files, so it
/// must carry a skip reason (both artifact-publishing consumers gate on it
/// before persisting a Ready row) while still resolving the lifecycle cleanly.
/// Completed captures must never read as skipped, or real snapshots would
/// silently stop being published.
#[test]
fn skipped_capture_is_publish_gated_and_lifecycle_clean() {
    let skipped = CheckpointCompletion::SkippedNoCapture("committed COW state");
    assert_eq!(skipped.skip_reason(), Some("committed COW state"));
    assert_eq!(skipped.into_lifecycle_result(), Ok(()));

    assert_eq!(CheckpointCompletion::Complete.skip_reason(), None);
    let with_error = CheckpointCompletion::CompleteWithLifecycleError("late".to_string());
    assert_eq!(with_error.skip_reason(), None);
    assert_eq!(with_error.into_lifecycle_result(), Err("late".to_string()));
}
#[test]
fn retention_keeps_newest_per_tier_without_waiting_for_the_whole_census() {
    let mut retained = std::array::from_fn(|_| std::collections::BTreeSet::new());
    for (index, prefix, keep) in [
        (0, "d-1m-", KEEP_D_1M),
        (1, "d-5m-", KEEP_D_5M),
        (2, "f-10m-", KEEP_F_10M),
        (3, "f-1h-", KEEP_F_1H),
        (4, "f-6h-", KEEP_F_6H),
    ] {
        // Reverse-order pages must make the same progress as ascending pages.
        for n in (0..keep + 7).rev() {
            let name = format!("{prefix}{n:04}");
            let deleted = retention_candidate(&mut retained, name.clone());
            assert_eq!(deleted, (n < 7).then_some(name));
            assert!(retained[index].len() <= keep);
        }
        let newest = format!("{prefix}{:04}", keep + 6);
        assert!(retention_candidate(&mut retained, newest).is_none());
    }
    assert!(retention_candidate(&mut retained, "f-manual-001".into()).is_none());
    assert_eq!(
        retention_candidate(&mut retained, "1m-0000".into()),
        Some("1m-0000".into())
    );
}
