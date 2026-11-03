use super::retention::{
    KEEP_D_1M, KEEP_D_5M, KEEP_F_1H, KEEP_F_6H, KEEP_F_10M, retention_candidate,
};
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
                    unowned_staging: None,
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
                            unowned_staging: None,
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
                unowned_staging: None,
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
        Some(VmState::Paused),
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
        Some(VmState::Running),
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
        Some(VmState::Paused),
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

/// A checkpoint's sidecar, scheduled or requested, carries the capturing host's
/// CPU, as a suspend snapshot's does. Non-Linux hosts report no CPU, so the
/// comparison is only meaningful on Linux, where an unidentified CPU fails the
/// test rather than letting it compare `None` with `None`.
#[test]
fn checkpoint_sidecar_execution_metadata_records_the_capturing_host_cpu() {
    use pplx_space_common_core::types::{Arch, ExecutionMetadata, HostCpu, TemplateName};

    #[cfg(target_os = "linux")]
    assert!(
        HostCpu::current().is_some(),
        "this Linux host's CPU is unidentified"
    );

    let metadata = SandboxArtifactMetadata::new(
        SandboxId::new(),
        ExecutionMetadata::initial_firecracker(Arch::X86_64, vec![]),
        TemplateName::parse("seed").unwrap(),
        None,
    )
    .with_guest_memory(Some(
        pplx_space_common_core::types::MemorySize::mib(2048).unwrap(),
    ));
    let execution = checkpoint_sidecar_execution_metadata(metadata).unwrap();

    assert_eq!(execution.host_cpu(), HostCpu::current());
    assert_eq!(execution.guest_memory().unwrap().as_mib_floor(), 2048);
}

/// Retention maps each `delete_named_snapshot` outcome to its own policy;
/// that helper's tests own the row and byte ordering. A pinned or leased
/// snapshot is kept, and so is one whose row cannot be read or names another
/// path, without stopping the rest of the prune: it runs before every
/// scheduled capture. A catalogued snapshot is renamed to its GC staging
/// sibling, which DeleteCacheArtifact deletes once the artifact lock is
/// released; the guest flush follows, still unlocked, and the capture then
/// retakes the lock. One whose staging delete fails partway is still pruned,
/// because its row and `local_path` are gone, and one stuck directory is below
/// the gate, so the tick still captures and counts it. A failed byte delete
/// of a snapshot without a row stops the prune at that snapshot, and the
/// capture with it, but staging the prune already made is still deleted.
#[tokio::test]
async fn retention_prune_skips_pinned_and_leased_snapshot_artifacts() {
    use crate::db::OptionalExtension as _;
    use crate::local_artifacts::LocalArtifacts;
    use pplx_space_node_mgr_client::proto::{
        BtrfsSnapshotInfo, DeleteBtrfsSnapshotRequest, DeleteBtrfsSnapshotResponse,
        DeleteCacheArtifactRequest, DeleteCacheArtifactResponse, ListBtrfsSnapshotsRequest,
        ListBtrfsSnapshotsResponse,
    };
    use std::path::Path;

    #[derive(Clone, Copy, Debug, PartialEq)]
    enum Setup {
        Pinned,
        Restoring,
        UnreadableRow,
        PathDiffers,
        Uncatalogued,
        Deletable,
    }
    #[derive(Clone, Copy, Debug, PartialEq)]
    enum StagingDelete {
        Removes,
        FailsPartway,
    }
    // The six oldest of eleven 1m snapshots fall outside retention and are
    // considered oldest first. When the snapshot without a row precedes the
    // deletable one, a failing delete stops before any row is checked out;
    // when it follows, the deletable one is already staged.
    let kept = [
        Setup::Pinned,
        Setup::Restoring,
        Setup::UnreadableRow,
        Setup::PathDiffers,
    ];
    let names: Vec<String> = (0..11)
        .map(|minute| format!("d-1m-20260101T00{minute:02}00Z"))
        .collect();
    // (whether the uncatalogued snapshot precedes the deletable one, the
    // uncatalogued byte delete fails, how a staging delete ends, the tick
    // fails before capturing, whether the deletable one is staged, the
    // unowned staging the tick records)
    for (uncatalogued_first, uncatalogued_fails, staging_delete, fails, staged, unowned) in [
        (true, false, StagingDelete::Removes, false, true, Some(0)),
        (
            true,
            false,
            StagingDelete::FailsPartway,
            false,
            true,
            Some(1),
        ),
        (true, true, StagingDelete::Removes, true, false, None),
        (false, true, StagingDelete::Removes, true, true, None),
    ] {
        let case = format!(
            "uncatalogued_first={uncatalogued_first}, \
             uncatalogued_fails={uncatalogued_fails}, {staging_delete:?}"
        );
        let last = if uncatalogued_first {
            [Setup::Uncatalogued, Setup::Deletable]
        } else {
            [Setup::Deletable, Setup::Uncatalogued]
        };
        let candidates = kept.into_iter().chain(last);
        let (uncatalogued, deletable) = if uncatalogued_first { (4, 5) } else { (5, 4) };
        let by_name = vec![names[uncatalogued].clone()];
        let root = tempfile::TempDir::new().unwrap();
        let cache_root = root.path().to_str().unwrap().to_owned();
        let (ctx, sandbox_id) = checkpoint_context(SandboxState::Started);
        let artifacts = LocalArtifacts::new(ctx.db.clone());
        let mut ids = Vec::new();
        let mut paths = Vec::new();
        for (name, setup) in names.iter().zip(candidates) {
            let id = LocalArtifactId::new();
            ids.push(id);
            let local_path = pplx_space_common_core::host::linux::paths::sandbox_snapshot_dir_in(
                &cache_root,
                &sandbox_id,
                name,
            );
            std::fs::create_dir_all(&local_path).unwrap();
            for file in ["memory", "vmstate"] {
                std::fs::write(Path::new(&local_path).join(file), file).unwrap();
            }
            paths.push(local_path.clone());
            if matches!(
                setup,
                Setup::Pinned | Setup::Restoring | Setup::PathDiffers | Setup::Deletable
            ) {
                let published = if setup == Setup::PathDiffers {
                    format!("{local_path}-elsewhere")
                } else {
                    local_path.clone()
                };
                artifacts
                    .publish_named_snapshot(
                        &sandbox_id,
                        id,
                        ArtifactKind::Disk,
                        &SnapshotName::parse(name.clone()).unwrap(),
                        &published,
                    )
                    .unwrap();
            }
            let db = ctx.db.lock().unwrap();
            match setup {
                Setup::Pinned => {
                    db.execute(
                        "UPDATE sandboxes SET suspend_predecessor_artifact_id = ?1",
                        [id.to_string()],
                    )
                    .unwrap();
                }
                // An id that is not a UUID does not decode.
                Setup::UnreadableRow => {
                    db.execute(
                        "INSERT INTO local_artifacts (local_artifact_id, sandbox_id,
                             artifact_kind, artifact_origin, snapshot_name, local_path,
                             created_at)
                         VALUES ('not-a-uuid', ?1, 'disk', 'snapshot', ?2, ?3, '0')",
                        [sandbox_id.clone(), name.clone(), local_path],
                    )
                    .unwrap();
                }
                Setup::Restoring | Setup::PathDiffers | Setup::Uncatalogued | Setup::Deletable => {}
            }
        }
        let reader = artifacts.check_out_for_restore(ids[1]).unwrap();
        let deletable = (paths[deletable].clone(), ids[deletable]);
        let staging = format!(
            "{cache_root}/{sandbox_id}/snapshots/.gc-pending-{}",
            deletable.1
        );
        // Whether the sandbox's artifact lock was free while each staging
        // delete ran, and whether it was held while the capture ran.
        let lock_free_at_staging_delete = Arc::new(std::sync::Mutex::new(Vec::new()));
        let coordinator = ctx.auto_lifecycle.clone();
        let deleted_by_name = Arc::new(std::sync::Mutex::new(Vec::new()));
        let deleted_by_path = Arc::new(std::sync::Mutex::new(Vec::new()));
        let listed = names.clone();
        let node_manager = crate::node_manager_transport::fake::FakeNodeManager::default()
            .unary("ListBtrfsSnapshots", move |_: ListBtrfsSnapshotsRequest| {
                Ok(ListBtrfsSnapshotsResponse {
                    snapshots: listed
                        .iter()
                        .map(|name| BtrfsSnapshotInfo {
                            name: name.clone(),
                            ..Default::default()
                        })
                        .collect(),
                    next_page_token: String::new(),
                })
            })
            .unary("DeleteBtrfsSnapshot", {
                let deleted_by_name = Arc::clone(&deleted_by_name);
                move |request: DeleteBtrfsSnapshotRequest| {
                    deleted_by_name.lock().unwrap().push(request.snapshot_name);
                    if uncatalogued_fails {
                        return Err(tonic::Status::unavailable("injected byte delete failure"));
                    }
                    Ok(DeleteBtrfsSnapshotResponse {})
                }
            })
            .unary("DeleteCacheArtifact", {
                let (deleted_by_path, root) =
                    (Arc::clone(&deleted_by_path), root.path().to_owned());
                let (lock_free, coordinator, sandbox_id) = (
                    Arc::clone(&lock_free_at_staging_delete),
                    coordinator.clone(),
                    sandbox_id.clone(),
                );
                move |request: DeleteCacheArtifactRequest| {
                    lock_free.lock().unwrap().push(
                        coordinator
                            .artifact_lock(&sandbox_id)
                            .try_lock_owned()
                            .is_ok(),
                    );
                    let path = Path::new(&request.local_path);
                    assert!(path.starts_with(&root));
                    let result = match staging_delete {
                        StagingDelete::Removes => {
                            std::fs::remove_dir_all(path).unwrap();
                            Ok(DeleteCacheArtifactResponse { was_present: true })
                        }
                        StagingDelete::FailsPartway => {
                            std::fs::remove_file(path.join("memory")).unwrap();
                            Err(tonic::Status::internal("injected partial delete"))
                        }
                    };
                    deleted_by_path.lock().unwrap().push(request.local_path);
                    result
                }
            })
            .serve();

        // Whether the lock was free when the guest flush ran, and how many
        // staging deletes preceded it.
        let prepared = std::sync::Mutex::new(None);
        let captured_under_lock = std::sync::Mutex::new(None);
        let mut recorded = None;
        let counters = pplx_space_metrics::test_capture::capture_counters_guard();
        let result = super::retention::prune_and_capture(
            &mut node_manager.connector().lazy(),
            &ctx,
            &sandbox_id,
            &cache_root,
            1,
            &mut recorded,
            async |_| {
                let free = coordinator
                    .artifact_lock(&sandbox_id)
                    .try_lock_owned()
                    .is_ok();
                *prepared.lock().unwrap() = Some((free, deleted_by_path.lock().unwrap().len()));
                Ok(())
            },
            async |_, ()| {
                let held = coordinator
                    .artifact_lock(&sandbox_id)
                    .try_lock_owned()
                    .is_err();
                *captured_under_lock.lock().unwrap() = Some(held);
                Ok(())
            },
        )
        .await;

        assert_eq!(result.is_err(), fails, "{case}");
        assert_eq!(recorded, unowned, "{case}");
        // Only a failed prune skips the capture here; one stuck staging
        // directory is below the gate. The staging is deleted, and the guest
        // flushed after it, with the lock released; the capture retakes it.
        assert_eq!(
            capture_skips(counters.finish()),
            if fails {
                vec!["reason:prune_failed"]
            } else {
                vec![]
            },
            "{case}"
        );
        assert_eq!(
            *prepared.lock().unwrap(),
            (!fails).then_some((true, usize::from(staged))),
            "{case}"
        );
        assert_eq!(
            *captured_under_lock.lock().unwrap(),
            (!fails).then_some(true),
            "{case}"
        );
        assert_eq!(
            *lock_free_at_staging_delete.lock().unwrap(),
            vec![true; usize::from(staged)],
            "{case}"
        );
        assert_eq!(*deleted_by_name.lock().unwrap(), by_name, "{case}");
        let by_path = if staged {
            vec![staging.clone()]
        } else {
            Vec::new()
        };
        assert_eq!(*deleted_by_path.lock().unwrap(), by_path, "{case}");
        // The kept snapshots were not renamed or touched.
        for path in &paths[..4] {
            assert_eq!(
                std::fs::read(Path::new(path).join("memory")).unwrap(),
                b"memory",
                "{case}: {path}"
            );
        }
        let row: Option<String> = ctx
            .db
            .lock()
            .unwrap()
            .query_row(
                "SELECT state FROM local_artifacts WHERE local_artifact_id = ?1",
                [deletable.1.to_string()],
                |row| row.get(0),
            )
            .optional()
            .unwrap();
        if staged {
            assert_eq!(row, None, "{case}");
            assert!(!Path::new(&deletable.0).exists(), "{case}");
            let staged_bytes = std::fs::read(Path::new(&staging).join("vmstate")).ok();
            let expected = match staging_delete {
                StagingDelete::Removes => None,
                StagingDelete::FailsPartway => Some(b"vmstate".to_vec()),
            };
            assert_eq!(staged_bytes, expected, "{case}");
            assert!(!Path::new(&staging).join("memory").exists(), "{case}");
        } else {
            assert_eq!(row.as_deref(), Some("ready"), "{case}");
            assert!(Path::new(&deletable.0).join("memory").exists(), "{case}");
            assert!(!Path::new(&staging).exists(), "{case}");
        }
        drop(reader);
    }
}

fn capture_skips(counters: Vec<pplx_space_metrics::test_capture::CapturedCounter>) -> Vec<String> {
    counters
        .into_iter()
        .filter(|counter| counter.name == "snapshot.scheduler.capture_skipped")
        .flat_map(|counter| counter.tags)
        .collect()
}

/// How the staging harness's DeleteCacheArtifact answers for one path.
#[derive(Clone, Copy)]
enum StagingAnswer {
    /// Fails with `Internal` on every call.
    Stuck,
    /// Answers `Unavailable` this many more times, then deletes.
    Unavailable(usize),
    /// Never answers, as a delete queued behind node-manager's delete
    /// capacity or the sandbox's artifact lock.
    Hangs,
    /// Deletes after this long.
    Takes(Duration),
    /// Deletes every GC staging under `snapshots/`, as the orphan sweep
    /// running meanwhile would, then answers.
    SweepsAll,
}

/// One page of a [`StagingHarness`] listing: the names under `snapshots/` it
/// lists for a page token, and the next page's token (empty on the last).
type StagingListing = dyn Fn(&str, &str) -> (Vec<String>, String) + Send + Sync;

/// One sandbox's `snapshots/` under a tempdir, served by a node-manager fake
/// that lists it and deletes from it, plus the scheduler state that records
/// the sandbox's unowned staging. A path without an answer is deleted.
struct StagingHarness {
    _root: tempfile::TempDir,
    cache_root: String,
    snapshots: String,
    ctx: CheckpointContext,
    sandbox_id: String,
    answers: Arc<std::sync::Mutex<HashMap<String, StagingAnswer>>>,
    deletes: Arc<std::sync::Mutex<Vec<String>>>,
    node_manager: crate::node_manager_transport::fake::ServedNodeManager,
    states: Mutex<HashMap<String, SandboxSnapshotState>>,
}

/// What one [`StagingHarness::tick`] did.
#[derive(Debug)]
struct StagingTick {
    result: Result<(), String>,
    captured: bool,
    /// The DeleteCacheArtifact paths the tick sent, in order.
    deletes: Vec<String>,
    recorded: Option<usize>,
    skips: Vec<String>,
    gauges: Vec<f64>,
}

/// Every name under `snapshots`, sorted, on one page.
fn list_snapshots_dir(snapshots: &str) -> Vec<String> {
    let mut names: Vec<_> = std::fs::read_dir(snapshots)
        .unwrap()
        .map(|entry| entry.unwrap().file_name().into_string().unwrap())
        .collect();
    names.sort();
    names
}

impl StagingHarness {
    fn new() -> Self {
        Self::with_listing(Box::new(|snapshots, _| {
            (list_snapshots_dir(snapshots), String::new())
        }))
    }

    fn with_listing(listing: Box<StagingListing>) -> Self {
        use pplx_space_node_mgr_client::proto::{
            BtrfsSnapshotInfo, DeleteCacheArtifactRequest, DeleteCacheArtifactResponse,
            ListBtrfsSnapshotsRequest, ListBtrfsSnapshotsResponse,
        };
        let root = tempfile::TempDir::new().unwrap();
        let cache_root = root.path().to_str().unwrap().to_owned();
        let (ctx, sandbox_id) = checkpoint_context(SandboxState::Started);
        let snapshots = format!("{cache_root}/{sandbox_id}/snapshots");
        std::fs::create_dir_all(&snapshots).unwrap();
        let answers = Arc::new(std::sync::Mutex::new(HashMap::new()));
        let deletes = Arc::new(std::sync::Mutex::new(Vec::new()));
        let node_manager = crate::node_manager_transport::fake::FakeNodeManager::default()
            .unary("ListBtrfsSnapshots", {
                let snapshots = snapshots.clone();
                move |request: ListBtrfsSnapshotsRequest| {
                    let (names, next_page_token) = listing(&snapshots, &request.page_token);
                    Ok(ListBtrfsSnapshotsResponse {
                        snapshots: names
                            .into_iter()
                            .map(|name| BtrfsSnapshotInfo {
                                name,
                                ..Default::default()
                            })
                            .collect(),
                        next_page_token,
                    })
                }
            })
            .unary_async("DeleteCacheArtifact", {
                let (answers, deletes, root, snapshots) = (
                    Arc::clone(&answers),
                    Arc::clone(&deletes),
                    root.path().to_owned(),
                    snapshots.clone(),
                );
                move |request: DeleteCacheArtifactRequest| {
                    let path = request.local_path;
                    assert!(std::path::Path::new(&path).starts_with(&root));
                    deletes.lock().unwrap().push(path.clone());
                    let mut answers = answers.lock().unwrap();
                    let answer: futures_util::future::BoxFuture<'static, _> = match answers
                        .get_mut(&path)
                    {
                        Some(StagingAnswer::Stuck) => Box::pin(std::future::ready(Err(
                            tonic::Status::internal("injected stuck staging"),
                        ))),
                        Some(StagingAnswer::Unavailable(remaining)) if *remaining > 0 => {
                            *remaining -= 1;
                            Box::pin(std::future::ready(Err(tonic::Status::unavailable(
                                "sandbox host mutation or reset is in progress; retry",
                            ))))
                        }
                        Some(StagingAnswer::Hangs) => Box::pin(std::future::pending()),
                        Some(StagingAnswer::Takes(duration)) => {
                            let (duration, path) = (*duration, path.clone());
                            Box::pin(async move {
                                tokio::time::sleep(duration).await;
                                std::fs::remove_dir_all(&path).unwrap();
                                Ok(DeleteCacheArtifactResponse { was_present: true })
                            })
                        }
                        Some(StagingAnswer::SweepsAll) => {
                            for name in list_snapshots_dir(&snapshots) {
                                if crate::local_artifacts::is_gc_staging_name(&name) {
                                    std::fs::remove_dir_all(format!("{snapshots}/{name}")).unwrap();
                                }
                            }
                            Box::pin(std::future::ready(Ok(DeleteCacheArtifactResponse {
                                was_present: true,
                            })))
                        }
                        _ => {
                            std::fs::remove_dir_all(&path).unwrap();
                            Box::pin(std::future::ready(Ok(DeleteCacheArtifactResponse {
                                was_present: true,
                            })))
                        }
                    };
                    answer
                }
            })
            .serve();
        let states = Mutex::new(HashMap::from([(
            sandbox_id.clone(),
            SandboxSnapshotState {
                tick_count: 0,
                pending_checkpoint: None,
                template: "seed".into(),
                unowned_staging: None,
            },
        )]));
        Self {
            _root: root,
            cache_root,
            snapshots,
            ctx,
            sandbox_id,
            answers,
            deletes,
            node_manager,
            states,
        }
    }

    /// Leave unowned staging named `name` (`None`: `.gc-pending-<id>`) for
    /// artifact `serial`, whose id orders by `serial`. Returns its path.
    fn leftover(&self, serial: u128, name: Option<&str>) -> String {
        let id = LocalArtifactId::from_uuid(uuid::Uuid::from_u128(
            0x0193_0000_0000_7000_8000_0000_0000_0000 + serial,
        ));
        let path = match name {
            Some(name) => format!("{}/{name}.gc-pending-{id}", self.snapshots),
            None => format!("{}/.gc-pending-{id}", self.snapshots),
        };
        std::fs::create_dir_all(&path).unwrap();
        std::fs::write(std::path::Path::new(&path).join("memory"), "memory").unwrap();
        path
    }

    /// Catalogue `count` more 1m snapshots than the tier keeps, so the next
    /// prune stages the `count` oldest as its own. Returns their staging
    /// paths, in the order the prune stages them.
    fn surplus(&self, count: usize) -> Vec<String> {
        let artifacts = crate::local_artifacts::LocalArtifacts::new(self.ctx.db.clone());
        let mut stagings: Vec<String> = (0..KEEP_D_1M + count)
            .map(|minute| {
                let name = format!("d-1m-20260101T00{minute:02}00Z");
                let path = format!("{}/{name}", self.snapshots);
                std::fs::create_dir_all(&path).unwrap();
                std::fs::write(std::path::Path::new(&path).join("memory"), "memory").unwrap();
                let id = LocalArtifactId::new();
                artifacts
                    .publish_named_snapshot(
                        &self.sandbox_id,
                        id,
                        ArtifactKind::Disk,
                        &SnapshotName::parse(name).unwrap(),
                        &path,
                    )
                    .unwrap();
                format!("{}/.gc-pending-{id}", self.snapshots)
            })
            .collect();
        stagings.truncate(count);
        stagings
    }

    fn answer(&self, path: &str, answer: Option<StagingAnswer>) {
        let mut answers = self.answers.lock().unwrap();
        match answer {
            Some(answer) => answers.insert(path.to_owned(), answer),
            None => answers.remove(path),
        };
    }

    /// One scheduled tick's retention, advanced, recorded and gauged as the
    /// scheduler loop does it.
    async fn tick(&self) -> StagingTick {
        self.deletes.lock().unwrap().clear();
        let counters = pplx_space_metrics::test_capture::capture_counters_guard();
        let values = pplx_space_metrics::test_capture::capture_values_guard();
        let (mut ticked, mut recorded) = (None, None);
        let mut captured = false;
        let result = run_scheduled_tick(&self.states, &self.sandbox_id, async |tick, _| {
            ticked = Some(tick);
            super::retention::prune_and_capture(
                &mut self.node_manager.connector().lazy(),
                &self.ctx,
                &self.sandbox_id,
                &self.cache_root,
                tick,
                &mut recorded,
                async |_| Ok(()),
                async |_, ()| {
                    captured = true;
                    Ok(())
                },
            )
            .await?;
            Ok(CheckpointCompletion::Complete)
        })
        .await;
        if let Some(tick) = ticked {
            super::retention::record_unowned_staging(
                &self.states,
                &self.sandbox_id,
                tick,
                recorded,
            )
            .await;
        }
        super::retention::emit_unowned_staging_gauge(&self.states).await;
        StagingTick {
            result,
            captured,
            deletes: self.deletes.lock().unwrap().clone(),
            recorded,
            skips: capture_skips(counters.finish()),
            gauges: values
                .finish()
                .into_iter()
                .filter(|value| value.name == "snapshot.scheduler.unowned_staging")
                .map(|value| value.value)
                .collect(),
        }
    }
}

/// The scheduled-snapshot gate events the capture saw, as (type, sandbox).
fn gate_events(
    capture: &crate::service::tests::service_tests::SandboxEventCapture,
) -> Vec<(String, String)> {
    capture
        .events
        .lock()
        .unwrap()
        .iter()
        .filter(|event| {
            event
                .get("event_type")
                .is_some_and(|event_type| event_type.starts_with("scheduled_snapshots_"))
        })
        .map(|event| (event["event_type"].clone(), event["sandbox_id"].clone()))
        .collect()
}

/// One staging directory that DeleteCacheArtifact never removes, left by
/// whichever deleter, does not stop the sandbox's scheduled snapshots: each
/// tick retries it, counts it, and captures anyway. The node-level gauge sums
/// it with the other sandboxes' last counts. Staging a row still owns is its
/// owner's, so it is neither retried nor counted.
#[tokio::test]
async fn one_stuck_leftover_does_not_stop_captures() {
    use crate::local_artifacts::LocalArtifacts;

    let harness = StagingHarness::new();
    let stuck = harness.leftover(1, None);
    harness.answer(&stuck, Some(StagingAnswer::Stuck));
    let owned = LocalArtifactId::new();
    LocalArtifacts::new(harness.ctx.db.clone())
        .publish_named_snapshot(
            &harness.sandbox_id,
            owned,
            ArtifactKind::Disk,
            &SnapshotName::parse("d-1m-20260101T000000Z".to_owned()).unwrap(),
            &format!("{}/d-1m-20260101T000000Z", harness.snapshots),
        )
        .unwrap();
    let owned_staging = format!("{}/.gc-pending-{owned}", harness.snapshots);
    std::fs::create_dir_all(&owned_staging).unwrap();
    harness.states.lock().await.insert(
        "another-sandbox".into(),
        SandboxSnapshotState {
            tick_count: 0,
            pending_checkpoint: None,
            template: "seed".into(),
            unowned_staging: Some(2),
        },
    );
    let events = crate::service::tests::service_tests::SandboxEventCapture::default();
    use tracing_subscriber::layer::SubscriberExt as _;
    let _subscriber =
        tracing::subscriber::set_default(tracing_subscriber::registry().with(events.clone()));

    for tick in 1..=3 {
        let observed = harness.tick().await;
        observed.result.as_ref().unwrap();
        assert!(observed.captured, "tick {tick}: {observed:?}");
        assert_eq!(
            observed.deletes,
            std::slice::from_ref(&stuck),
            "tick {tick}"
        );
        assert_eq!(observed.recorded, Some(1), "tick {tick}");
        assert_eq!(observed.skips, Vec::<String>::new(), "tick {tick}");
        assert_eq!(observed.gauges, [3.0], "tick {tick}");
        assert!(std::path::Path::new(&stuck).exists(), "tick {tick}");
        assert!(std::path::Path::new(&owned_staging).exists(), "tick {tick}");
    }
    assert_eq!(gate_events(&events), Vec::new());
}

/// Five stuck directories, the limit, stop the capture, and count the skip,
/// on every tick; the sandbox event marks only the gate closing. Once a
/// delete brings the count to four, captures resume and the reopening is
/// marked. The counts are literal so that they pin the limit at five.
#[tokio::test]
async fn staging_at_the_limit_blocks_captures_until_one_is_deleted() {
    const UNOWNED_STAGING_LIMIT: usize = 5;

    let harness = StagingHarness::new();
    let stuck: Vec<String> = (1..=UNOWNED_STAGING_LIMIT as u128)
        .map(|serial| harness.leftover(serial, None))
        .collect();
    for path in &stuck {
        harness.answer(path, Some(StagingAnswer::Stuck));
    }
    let events = crate::service::tests::service_tests::SandboxEventCapture::default();
    use tracing_subscriber::layer::SubscriberExt as _;
    let _subscriber =
        tracing::subscriber::set_default(tracing_subscriber::registry().with(events.clone()));

    for tick in 1..=2 {
        let observed = harness.tick().await;
        let error = observed.result.as_ref().unwrap_err();
        assert!(error.contains("capture blocked"), "tick {tick}: {error}");
        assert!(!observed.captured, "tick {tick}");
        assert_eq!(observed.deletes.len(), 1, "tick {tick}");
        assert!(stuck.contains(&observed.deletes[0]), "tick {tick}");
        assert_eq!(
            observed.recorded,
            Some(UNOWNED_STAGING_LIMIT),
            "tick {tick}"
        );
        assert_eq!(observed.skips, ["reason:staging_backlog"], "tick {tick}");
        assert_eq!(
            observed.gauges,
            [UNOWNED_STAGING_LIMIT as f64],
            "tick {tick}"
        );
    }
    let blocked = (
        "scheduled_snapshots_blocked".to_owned(),
        harness.sandbox_id.clone(),
    );
    assert_eq!(gate_events(&events), std::slice::from_ref(&blocked));

    for path in &stuck {
        harness.answer(path, None);
    }
    let observed = harness.tick().await;
    observed.result.as_ref().unwrap();
    assert!(observed.captured);
    let [deleted] = observed.deletes.as_slice() else {
        panic!("one leftover retried: {:?}", observed.deletes);
    };
    assert!(stuck.contains(deleted));
    assert!(!std::path::Path::new(deleted).exists());
    assert_eq!(observed.recorded, Some(UNOWNED_STAGING_LIMIT - 1));
    assert_eq!(observed.skips, Vec::<String>::new());
    assert_eq!(observed.gauges, [(UNOWNED_STAGING_LIMIT - 1) as f64]);
    assert_eq!(
        gate_events(&events),
        [
            blocked,
            (
                "scheduled_snapshots_unblocked".to_owned(),
                harness.sandbox_id.clone()
            )
        ]
    );
}

/// A stop/start that replaces the sandbox's registration while a tick's
/// retention runs gets neither that tick's count nor its gate event, as
/// `run_scheduled_tick` leaves the replacement's checkpoint alone. Its own
/// first tick records its own count and reports the gate it finds closed.
#[tokio::test(start_paused = true)]
async fn a_replacement_registration_gets_no_count_from_the_old_tick() {
    use super::retention::UNOWNED_STAGING_LIMIT;

    let harness = StagingHarness::new();
    // Each tick's retry deletes one: the limit and one more remain after the
    // first, the limit after the second.
    let leftovers: Vec<String> = (0..=UNOWNED_STAGING_LIMIT as u128 + 1)
        .map(|serial| harness.leftover(serial, None))
        .collect();
    for leftover in &leftovers {
        harness.answer(leftover, Some(StagingAnswer::Takes(Duration::from_secs(2))));
    }
    let events = crate::service::tests::service_tests::SandboxEventCapture::default();
    use tracing_subscriber::layer::SubscriberExt as _;
    let _subscriber =
        tracing::subscriber::set_default(tracing_subscriber::registry().with(events.clone()));

    let (observed, ()) = tokio::join!(harness.tick(), async {
        // While the tick's retry is still deleting.
        tokio::time::sleep(Duration::from_secs(1)).await;
        harness.states.lock().await.insert(
            harness.sandbox_id.clone(),
            SandboxSnapshotState {
                tick_count: 0,
                pending_checkpoint: None,
                template: "seed".into(),
                unowned_staging: None,
            },
        );
    });
    let error = observed.result.as_ref().unwrap_err();
    assert!(error.contains("capture blocked"), "{error}");
    assert_eq!(observed.recorded, Some(UNOWNED_STAGING_LIMIT + 1));
    {
        let states = harness.states.lock().await;
        let replacement = &states[&harness.sandbox_id];
        assert_eq!(replacement.tick_count, 0);
        assert_eq!(replacement.unowned_staging, None);
    }
    assert_eq!(gate_events(&events), Vec::new());

    let observed = harness.tick().await;
    observed.result.as_ref().unwrap_err();
    assert_eq!(observed.recorded, Some(UNOWNED_STAGING_LIMIT));
    assert_eq!(
        harness.states.lock().await[&harness.sandbox_id].unowned_staging,
        Some(UNOWNED_STAGING_LIMIT)
    );
    assert_eq!(
        gate_events(&events),
        [(
            "scheduled_snapshots_blocked".to_owned(),
            harness.sandbox_id.clone()
        )]
    );
}

/// A delete that node-manager answers `Unavailable` (a sandbox mutation holds
/// its claim) is retried once after a short backoff. If the retry deletes
/// it, it is gone; if it is still `Unavailable`, it is left to the sweep and
/// not counted, so it cannot close the gate even beside one fewer than the
/// limit of other leftovers.
#[tokio::test]
async fn unavailable_staging_delete_is_retried_once_and_not_counted() {
    use super::retention::UNOWNED_STAGING_LIMIT;

    // Unavailable, then deleted on the retry.
    let harness = StagingHarness::new();
    let transient = harness.leftover(1, None);
    harness.answer(&transient, Some(StagingAnswer::Unavailable(1)));
    let observed = harness.tick().await;
    observed.result.as_ref().unwrap();
    assert!(observed.captured);
    assert_eq!(observed.deletes, [transient.clone(), transient.clone()]);
    assert!(!std::path::Path::new(&transient).exists());
    assert_eq!(observed.recorded, Some(0));

    // Unavailable twice, as the one retried of the limit's worth of leftovers.
    let harness = StagingHarness::new();
    let leftovers: Vec<String> = (1..=UNOWNED_STAGING_LIMIT as u128)
        .map(|serial| harness.leftover(serial, None))
        .collect();
    for leftover in &leftovers {
        harness.answer(leftover, Some(StagingAnswer::Unavailable(2)));
    }
    let observed = harness.tick().await;
    observed.result.as_ref().unwrap();
    assert!(observed.captured);
    let [retried, retried_again] = observed.deletes.as_slice() else {
        panic!("one leftover retried once: {:?}", observed.deletes);
    };
    assert_eq!(retried, retried_again);
    assert!(leftovers.contains(retried));
    assert!(std::path::Path::new(retried).exists());
    assert_eq!(observed.recorded, Some(UNOWNED_STAGING_LIMIT - 1));
    assert_eq!(observed.skips, Vec::<String>::new());
}

/// Retention retries at most one leftover per tick, the orphan sweep owning
/// the rest, so a backlog cannot stretch the node's scheduler loop. The ticks
/// take the leftovers in turn, in artifact-id order whatever their names'
/// shape or listing position, so leftovers that never delete each take the
/// retry on their own tick and none of them keeps it from the others.
#[tokio::test]
async fn retention_retries_one_leftover_per_tick_in_turn() {
    let harness = StagingHarness::new();
    // The earlier `<name>.gc-pending-<id>` shape lists after `.gc-pending-*`.
    let stuck = [
        harness.leftover(1, Some("d-1m-20260101T000000Z")),
        harness.leftover(2, None),
        harness.leftover(3, None),
    ];
    for path in &stuck {
        harness.answer(path, Some(StagingAnswer::Stuck));
    }
    for (tick, retried) in [(1, 1), (2, 2), (3, 0), (4, 1), (5, 2), (6, 0)] {
        let observed = harness.tick().await;
        observed.result.as_ref().unwrap();
        assert!(observed.captured, "tick {tick}");
        assert_eq!(
            observed.deletes,
            std::slice::from_ref(&stuck[retried]),
            "tick {tick}"
        );
        assert_eq!(observed.recorded, Some(stuck.len()), "tick {tick}");
    }

    // Once they delete, each tick removes the one on its turn.
    for path in &stuck {
        harness.answer(path, None);
    }
    for (tick, retried, remaining) in [
        (7, vec![stuck[1].clone()], 2),
        (8, vec![stuck[0].clone()], 1),
        (9, vec![stuck[2].clone()], 0),
        (10, vec![], 0),
    ] {
        let observed = harness.tick().await;
        observed.result.as_ref().unwrap();
        assert!(observed.captured, "tick {tick}");
        assert_eq!(observed.deletes, retried, "tick {tick}");
        assert_eq!(observed.recorded, Some(remaining), "tick {tick}");
        assert_eq!(observed.gauges, [remaining as f64], "tick {tick}");
    }
}

/// A tick's staging deletes share one `STAGING_DELETE_BUDGET`, and the
/// first delete that times out ends them. A prune that stages 20 snapshots
/// at once (pinned or leased ones piled up past the tier's keep), whose
/// deletes all hang, attempts only the first and returns when the budget is
/// spent, in time for the next sandbox the scheduler loop awaits. The rest
/// are deferred, left on disk and counted, so they close the gate; the
/// timed-out delete is not counted.
#[tokio::test(start_paused = true)]
async fn hung_staging_deletes_share_one_budget_per_tick() {
    use super::retention::STAGING_DELETE_BUDGET;

    let hung = StagingHarness::new();
    let own = hung.surplus(20);
    for staging in &own {
        hung.answer(staging, Some(StagingAnswer::Hangs));
    }
    let next = StagingHarness::new();
    let next_leftover = next.leftover(1, None);

    // In turn, as the scheduler loop runs its sandboxes' ticks.
    let (hung_tick, elapsed, next_tick) = tokio::time::timeout(TICK_INTERVAL, async {
        let started = tokio::time::Instant::now();
        let hung_tick = hung.tick().await;
        let elapsed = started.elapsed();
        (hung_tick, elapsed, next.tick().await)
    })
    .await
    .expect("hung staging deletes stall the scheduler loop");

    let error = hung_tick.result.as_ref().unwrap_err();
    assert!(error.contains("capture blocked"), "{error}");
    assert!(!hung_tick.captured);
    assert_eq!(hung_tick.deletes, [own[0].clone()]);
    assert_eq!(hung_tick.recorded, Some(19));
    assert_eq!(hung_tick.skips, ["reason:staging_backlog"]);
    assert!(
        elapsed >= STAGING_DELETE_BUDGET
            && elapsed < STAGING_DELETE_BUDGET + Duration::from_secs(1),
        "{elapsed:?}"
    );
    assert!(
        own.iter()
            .all(|staging| std::path::Path::new(staging).exists())
    );
    next_tick.result.as_ref().unwrap();
    assert!(next_tick.captured);
    assert_eq!(next_tick.deletes, std::slice::from_ref(&next_leftover));
    assert_eq!(next_tick.recorded, Some(0));
}

/// The budget also runs out on deletes that each answer, but slowly: at 4 s
/// apiece, three of six own stagings are deleted, the fourth is cut off when
/// the 15 s budget is spent and not counted, and the last two and the
/// leftover retry are deferred and counted. Below the limit, the tick still
/// captures.
#[tokio::test(start_paused = true)]
async fn staging_deletes_past_the_budget_are_deferred_and_counted() {
    use super::retention::STAGING_DELETE_BUDGET;

    let harness = StagingHarness::new();
    let leftover = harness.leftover(1, None);
    let own = harness.surplus(6);
    for staging in &own {
        harness.answer(staging, Some(StagingAnswer::Takes(Duration::from_secs(4))));
    }

    let started = tokio::time::Instant::now();
    let observed = harness.tick().await;
    let elapsed = started.elapsed();

    observed.result.as_ref().unwrap();
    assert!(observed.captured);
    assert_eq!(observed.deletes, own[..4]);
    assert_eq!(observed.recorded, Some(3));
    assert_eq!(observed.skips, Vec::<String>::new());
    assert!(
        elapsed >= STAGING_DELETE_BUDGET
            && elapsed < STAGING_DELETE_BUDGET + Duration::from_secs(1),
        "{elapsed:?}"
    );
    for (index, staging) in own.iter().enumerate().take(3) {
        assert!(!std::path::Path::new(staging).exists(), "own {index}");
    }
    for staging in own[4..].iter().chain([&leftover]) {
        assert!(std::path::Path::new(staging).exists(), "{staging}");
    }
}

/// The gate counts the staging still there after the deletes. Leftovers the
/// orphan sweep removed since the listing do not count, so a listing of more
/// than the limit does not close the gate once they are gone.
#[tokio::test]
async fn staging_the_sweep_removed_meanwhile_is_not_counted() {
    let harness = StagingHarness::new();
    let leftovers: Vec<String> = (1..=6)
        .map(|serial| harness.leftover(serial, None))
        .collect();
    for leftover in &leftovers {
        harness.answer(leftover, Some(StagingAnswer::SweepsAll));
    }
    let events = crate::service::tests::service_tests::SandboxEventCapture::default();
    use tracing_subscriber::layer::SubscriberExt as _;
    let _subscriber =
        tracing::subscriber::set_default(tracing_subscriber::registry().with(events.clone()));

    let observed = harness.tick().await;
    observed.result.as_ref().unwrap();
    assert!(observed.captured);
    assert_eq!(observed.deletes.len(), 1);
    assert!(leftovers.contains(&observed.deletes[0]));
    assert_eq!(observed.recorded, Some(0));
    assert_eq!(observed.skips, Vec::<String>::new());
    assert_eq!(observed.gauges, [0.0]);
    assert_eq!(gate_events(&events), Vec::new());
}

/// Node-manager pages a live directory read, so a later page can list the
/// staging this prune made on an earlier one. It stays this prune's own:
/// deleted once and counted once, not retried again as a leftover.
#[tokio::test]
async fn staging_a_later_page_lists_again_is_deleted_and_counted_once() {
    use crate::local_artifacts::LocalArtifacts;

    // The first page lists one 1m snapshot more than the tier keeps; the
    // second lists the GC staging under `snapshots/` by then.
    let names: Vec<String> = (0..=KEEP_D_1M)
        .map(|minute| format!("d-1m-20260101T00{minute:02}00Z"))
        .collect();
    let harness = StagingHarness::with_listing(Box::new({
        let names = names.clone();
        move |snapshots, page_token| match page_token {
            "" => (names.clone(), "2".to_owned()),
            _ => (
                list_snapshots_dir(snapshots)
                    .into_iter()
                    .filter(|name| crate::local_artifacts::is_gc_staging_name(name))
                    .collect(),
                String::new(),
            ),
        }
    }));
    for name in &names {
        let path = format!("{}/{name}", harness.snapshots);
        std::fs::create_dir_all(&path).unwrap();
        std::fs::write(std::path::Path::new(&path).join("memory"), "memory").unwrap();
    }
    // The oldest, the one the prune deletes, is catalogued, so it is staged.
    let id = LocalArtifactId::new();
    LocalArtifacts::new(harness.ctx.db.clone())
        .publish_named_snapshot(
            &harness.sandbox_id,
            id,
            ArtifactKind::Disk,
            &SnapshotName::parse(names[0].clone()).unwrap(),
            &format!("{}/{}", harness.snapshots, names[0]),
        )
        .unwrap();
    let staging = format!("{}/.gc-pending-{id}", harness.snapshots);
    harness.answer(&staging, Some(StagingAnswer::Stuck));

    let observed = harness.tick().await;
    observed.result.as_ref().unwrap();
    assert!(observed.captured);
    assert_eq!(observed.deletes, std::slice::from_ref(&staging));
    assert_eq!(observed.recorded, Some(1));
    assert!(std::path::Path::new(&staging).exists());
}

/// GC staging keeps the tier prefix of the snapshot it once was, in the earlier
/// `<name>.gc-pending-<id>` shape. It takes no retained slot, so it cannot
/// push a real snapshot out early, and is never a candidate itself.
#[test]
fn retention_ignores_gc_staging() {
    let mut retained = std::array::from_fn(|_| std::collections::BTreeSet::new());
    let id = "01939e3e-0000-7000-8000-000000000001";
    let names = (1..=KEEP_D_1M)
        .map(|minute| format!("d-1m-20260101T00{minute:02}00Z"))
        .chain([
            format!("d-1m-20260101T005900Z.gc-pending-{id}"),
            format!(".gc-pending-{id}"),
        ]);
    for name in names {
        assert_eq!(
            retention_candidate(&mut retained, name.clone()),
            None,
            "{name}"
        );
    }
    // A name with the marker but no artifact id is an ordinary snapshot: the
    // oldest 1m one here, so it is pruned.
    let ordinary = "d-1m-20260101T000000Z.gc-pending-note".to_owned();
    assert_eq!(
        retention_candidate(&mut retained, ordinary.clone()),
        Some(ordinary)
    );
}

/// Every staging rename writes the id as `LocalArtifactId` displays it, so
/// only that spelling is staging: an uppercase or unhyphenated id parses as a
/// UUID but names an ordinary snapshot, which the orphan sweep must not take.
#[test]
fn gc_staging_ids_are_canonical() {
    let id = LocalArtifactId::new();
    let canonical = id.to_string();
    for (name, staging) in [
        (format!(".gc-pending-{canonical}"), true),
        (
            format!("d-1m-20260101T000000Z.gc-pending-{canonical}"),
            true,
        ),
        (format!(".gc-pending-{}", canonical.to_uppercase()), false),
        (format!(".gc-pending-{}", id.as_uuid().simple()), false),
        (format!(".gc-pending-{{{canonical}}}"), false),
        (
            format!("d-1m-20260101T000000Z.gc-pending-urn:uuid:{canonical}"),
            false,
        ),
    ] {
        assert_eq!(
            crate::local_artifacts::gc_staging_id(&name),
            staging.then_some(id),
            "{name}"
        );
    }
}
