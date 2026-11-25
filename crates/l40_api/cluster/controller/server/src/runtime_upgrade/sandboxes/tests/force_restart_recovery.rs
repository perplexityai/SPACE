//! Recovery, fencing, canary redrive, and controller-replacement tests for the
//! `sandbox_force_restarts` driver.

use super::*;

#[tokio::test(flavor = "current_thread")]
async fn operator_resume_with_retry_failed_boots_driver_stops_and_fences_user_restarts()
-> TestResult {
    let Some(db) = test_db().await? else {
        return Ok(());
    };
    seed_policy(&db.pool, RuntimeEnvironmentState::Blocked, true).await?;
    let campaign = insert_campaign(
        &db.pool,
        CampaignSeed {
            per_node: 4,
            per_cluster: 4,
            failure_budget: 1,
            ..force_restart_seed()
        },
    )
    .await?;
    let node = node(&db.pool).await?;
    let failed_after_stop = resident_on_source(&db.pool, SandboxLifecycle::Started, node).await?;
    let raced_by_user = resident_on_source(&db.pool, SandboxLifecycle::Started, node).await?;

    let dispatcher = FakeDispatcher::default().persisting(db.pool.clone());
    dispatcher.script_start(
        failed_after_stop,
        vec![
            Err(DispatchError::Failed("cold boot failed".to_owned())),
            Ok(TargetStart::Started),
        ],
    );
    dispatcher.script_start(
        raced_by_user,
        vec![
            Err(DispatchError::Retryable("slot busy".to_owned())),
            Ok(TargetStart::NotOnTarget(
                "user started and stopped the sandbox in between".to_owned(),
            )),
        ],
    );

    let memory = FakeMemoryRetirement::retired(source());
    let reconciler = force_restart_reconciler(&db.pool, dispatcher.clone(), memory);

    // Tick 1 dispatches both items (`failed_after_stop` fails `start`,
    // `raced_by_user` returns to `pending` after stopping); tick 2 checks the
    // failure budget (`1`) before leasing and pauses the campaign.
    tick(&reconciler).await;
    let pause_tick = tick(&reconciler).await;
    assert_eq!(pause_tick.leased, 0);
    assert_eq!(campaign_phase(&db.pool, campaign).await?, "paused");
    assert_eq!(
        item(&db.pool, campaign, failed_after_stop).await?.0,
        "failed"
    );
    assert_eq!(item(&db.pool, campaign, raced_by_user).await?.0, "pending");
    assert_eq!(dispatcher.tracked_driver_stops(), 2);

    // While paused, the user starts and stops `raced_by_user` again, advancing
    // `sandboxes.lifecycle_changed_at` past the driver's `stopped_at` fence.
    sqlx::query(sql!(
        "UPDATE sandboxes \
         SET lifecycle = 'stopped', \
             lifecycle_changed_at = lifecycle_changed_at + INTERVAL '5 seconds' \
         WHERE id = $1"
    ))
    .bind(raced_by_user)
    .execute(&db.pool)
    .await?;

    // Operator resumes the paused campaign with `retry_failed = true`:
    // because no item is `done` yet, `resume_campaign` enters `canary`, proves
    // one upgrade, and a second `resume_campaign` advances to `rolling`.
    // `failed_after_stop` still holds its substrate `ForceRestartProgress::Stopped`
    // and retries `start` with the exact same `stopped_at` fence, whereas
    // `raced_by_user`'s `lifecycle_changed_at` moved past its `stopped_at`
    // fence while paused so `upgrade` skips it without calling `start` again.
    let resumed =
        store::resume_campaign(&db.pool, campaign, &store::PhaseChange::default(), true).await?;
    assert_eq!(resumed.phase, CampaignPhase::Canary);
    while counts(&db.pool, campaign).await?.done == 0 {
        tick(&reconciler).await;
    }
    store::resume_campaign(&db.pool, campaign, &store::PhaseChange::default(), false).await?;
    tick_until_finished(&reconciler, &db.pool, campaign, 5).await?;

    assert_eq!(campaign_phase(&db.pool, campaign).await?, "completed");
    assert_eq!(item(&db.pool, campaign, failed_after_stop).await?.0, "done");
    assert_eq!(
        item(&db.pool, campaign, raced_by_user).await?,
        (
            "skipped".to_owned(),
            Some("the sandbox is stopped, which this campaign doesn't upgrade".to_owned()),
            2
        )
    );
    let fences = dispatcher.state().start_fences.clone();
    assert_eq!(fences.len(), 3);
    let failed_fences: Vec<_> = fences
        .iter()
        .filter(|(id, _, _)| *id == failed_after_stop)
        .collect();
    let raced_fences: Vec<_> = fences
        .iter()
        .filter(|(id, _, _)| *id == raced_by_user)
        .collect();
    assert_eq!(failed_fences.len(), 2);
    assert_eq!(
        failed_fences[0].1, failed_fences[1].1,
        "retry after resume_campaign(retry_failed=true) passes the same stopped_at fence"
    );
    assert_eq!(
        raced_fences.len(),
        1,
        "driver's lifecycle_changed_at == stopped_at fence skips before calling start again"
    );
    let raced_row = store::subject_sandbox(&db.pool, raced_by_user)
        .await?
        .expect("raced_by_user exists");
    assert!(
        to_jiff(raced_row.lifecycle_changed_at) > raced_fences[0].1,
        "user start+stop moved lifecycle_changed_at past the recorded stopped_at fence"
    );
    assert_eq!(dispatcher.tracked_driver_stops(), 0);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn redriven_lease_after_unrecorded_done_preserves_canary_and_pause_resets_open_pass()
-> TestResult {
    let Some(db) = test_db().await? else {
        return Ok(());
    };
    seed_policy(&db.pool, RuntimeEnvironmentState::Deprecated, true).await?;
    let node = node(&db.pool).await?;

    // Part 1: A canary item whose `start` succeeded in the substrate, or whose
    // node booted onto `target()` after `stop`'s `start` RPC timed out, before
    // `finish_item` recorded `Done` (or before the controller restarted) still
    // resolves as `Done` on redrive by a fresh controller and advances the
    // campaign from `canary` to `rolling` instead of stalling in `canary` with
    // zero remaining pending items.
    for timed_out_start in [false, true] {
        let canary_campaign = insert_campaign(
            &db.pool,
            CampaignSeed {
                phase: CampaignPhase::Canary,
                ..force_restart_seed()
            },
        )
        .await?;
        let canary_sandbox = resident_on_source(&db.pool, SandboxLifecycle::Started, node).await?;
        let dispatcher = FakeDispatcher::default().persisting(db.pool.clone());
        if timed_out_start {
            dispatcher.script_start(
                canary_sandbox,
                vec![Err(DispatchError::Retryable(
                    "start response timed out".to_owned(),
                ))],
            );
        }
        let memory = FakeMemoryRetirement::retired(source());
        let first_driver = SandboxForceRestartDriver::new(
            db.pool.clone(),
            K8S.as_str(),
            dispatcher.clone(),
            memory.clone(),
        );

        record(&db.pool, canary_campaign, &[canary_sandbox]).await?;
        let canary_row = store::list_unfinished(&db.pool)
            .await?
            .into_iter()
            .find(|listed| listed.id == canary_campaign)
            .expect("canary campaign");
        let first_lease =
            store::lease_items(&db.pool, canary_campaign, Duration::from_millis(1), 1).await?;
        if timed_out_start {
            assert_eq!(
                first_driver.upgrade(&canary_row, &first_lease[0]).await,
                ItemOutcome::Retry(ItemError::new("start response timed out"))
            );
            set_lifecycle(&db.pool, canary_sandbox, "started").await?;
            set_bound(&db.pool, canary_sandbox, target()).await?;
        } else {
            assert_eq!(
                first_driver.upgrade(&canary_row, &first_lease[0]).await,
                ItemOutcome::Done
            );
        }
        drop(first_driver);

        // Simulate controller loss before `finish_item` ran: expire the lease
        // so a replacement controller's reconciler reclaims and redrives the
        // item on its next tick.
        sqlx::query(sql!(
            "UPDATE runtime_upgrade_items \
             SET lease_expires_at = NOW() - INTERVAL '1 second' \
             WHERE campaign_id = $1 AND subject_id = $2"
        ))
        .bind(canary_campaign.0)
        .bind(canary_sandbox)
        .execute(&db.pool)
        .await?;

        let replacement_reconciler = force_restart_reconciler(&db.pool, dispatcher.clone(), memory);
        tick(&replacement_reconciler).await;
        assert_eq!(
            item(&db.pool, canary_campaign, canary_sandbox).await?.0,
            "done",
            "replacement controller preserves Done (timed_out_start={timed_out_start})"
        );
        let advanced = store::resume_campaign(
            &db.pool,
            canary_campaign,
            &store::PhaseChange::default(),
            false,
        )
        .await?;
        assert_eq!(
            advanced.phase,
            CampaignPhase::Rolling,
            "canary becomes resumable to rolling once the redriven canary item records Done"
        );
        store::abort_campaign(&db.pool, canary_campaign, &store::PhaseChange::default()).await?;
    }

    // Part 2: Pausing a rolling campaign mid-pass and later resuming it
    // restarts the paged scan from the beginning so sandboxes that restored
    // onto the deprecated source behind the open cursor while the campaign was
    // paused aren't missed before completion.
    let paused_campaign = insert_campaign(&db.pool, force_restart_seed()).await?;
    let already_done = resident_on_source(&db.pool, SandboxLifecycle::Started, node).await?;
    let behind_cursor = insert_sandbox(
        &db.pool,
        K8S,
        SandboxLifecycle::Suspended,
        node,
        Published::Bound(source()),
    )
    .await?;
    for _ in 0..2 {
        insert_sandbox(
            &db.pool,
            K8S,
            SandboxLifecycle::Started,
            node,
            Published::Unbound {
                runtime_version: "1.17.0",
                kernel: "6.1.155",
                state_format_version: "12.0.0",
            },
        )
        .await?;
    }

    let pause_dispatcher = FakeDispatcher::default().persisting(db.pool.clone());
    let pause_memory = FakeMemoryRetirement::retired(source());
    let paged = force_restart_paging(&db.pool, pause_dispatcher.clone(), pause_memory, 1);

    // Upgrade `already_done` on page 1 and advance one more page past
    // `behind_cursor`, leaving the pass open with `done == 1`.
    tick(&paged).await;
    let page_two = tick(&paged).await;
    assert_eq!((page_two.inserted, page_two.leased), (0, 0));

    // Operator pauses the campaign while `behind_cursor` restores onto the
    // deprecated source; a tick runs while paused (skipping `Paused` campaigns
    // in `list_unfinished`'s reconciler loop), then the operator resumes back
    // to `rolling`.
    store::pause_campaign(&db.pool, paused_campaign, &store::PhaseChange::default()).await?;
    set_lifecycle(&db.pool, behind_cursor, "started").await?;
    tick(&paged).await;
    let resumed_rolling = store::resume_campaign(
        &db.pool,
        paused_campaign,
        &store::PhaseChange::default(),
        false,
    )
    .await?;
    assert_eq!(resumed_rolling.phase, CampaignPhase::Rolling);

    tick_until_finished(&paged, &db.pool, paused_campaign, 10).await?;
    assert_eq!(
        campaign_phase(&db.pool, paused_campaign).await?,
        "completed"
    );
    assert_eq!(
        pause_dispatcher.calls(),
        vec![
            Call::Stop(already_done),
            Call::Start(already_done),
            Call::Stop(behind_cursor),
            Call::Start(behind_cursor),
        ],
        "resuming after pause restarts the open pass and upgrades behind_cursor"
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn controller_replacement_and_interrupted_stop_calls_recover_campaign_stops() -> TestResult {
    let Some(db) = test_db().await? else {
        return Ok(());
    };
    seed_policy(&db.pool, RuntimeEnvironmentState::Blocked, true).await?;
    let campaign = insert_campaign(
        &db.pool,
        CampaignSeed {
            per_node: 4,
            per_cluster: 4,
            failure_budget: 5,
            ..force_restart_seed()
        },
    )
    .await?;
    let node = node(&db.pool).await?;

    let deadline_dropped_stop =
        resident_on_source(&db.pool, SandboxLifecycle::Started, node).await?;

    let dispatcher = FakeDispatcher::default().persisting(db.pool.clone());
    dispatcher
        .state()
        .post_commit_stop_deadline_advances
        .insert(deadline_dropped_stop, Duration::from_secs(31));

    let memory = FakeMemoryRetirement::retired(source());

    // Isolate `deadline_dropped_stop` on its own controller with a 30s
    // `item_deadline` (and 300s lease so renewal stays quiescent): real
    // Postgres queries run with a 30s budget before commit, then `stop`
    // advances the paused clock past 30s after committing
    // `ForceRestartProgress::Stopped` so `Worker::drive_item` cancels the
    // `stop` future deterministically with zero wall-clock sleep.
    let deadline_controller = Reconciler::new(
        db.pool.clone(),
        Drivers::default().with(SandboxForceRestartDriver::new(
            db.pool.clone(),
            K8S.as_str(),
            dispatcher.clone(),
            memory.clone(),
        )),
        ReconcilerConfig {
            enabled: true,
            lease: Duration::from_secs(300),
            item_deadline: Duration::from_secs(30),
            ..ReconcilerConfig::default()
        },
    );
    record(&db.pool, campaign, &[deadline_dropped_stop]).await?;
    tick(&deadline_controller).await;
    drop(deadline_controller);

    assert_eq!(
        item(&db.pool, campaign, deadline_dropped_stop).await?.0,
        "failed",
        "cancelled stop future at item_deadline marks the item failed until retry_failed"
    );
    assert_eq!(dispatcher.tracked_driver_stops(), 1);

    // Controller 1 runs with default deadlines so normal SQL load can't race
    // `item_deadline`: `replaced_mid_step` stops and retries `start`, while
    // `post_commit_stop_error` commits its stop before returning `Retryable`.
    let replaced_mid_step = resident_on_source(&db.pool, SandboxLifecycle::Started, node).await?;
    let post_commit_stop_error =
        resident_on_source(&db.pool, SandboxLifecycle::Started, node).await?;
    dispatcher.script_start(
        replaced_mid_step,
        vec![
            Err(DispatchError::Retryable("pod shutting down".to_owned())),
            Ok(TargetStart::Started),
        ],
    );
    dispatcher.state().post_commit_stop_errors.insert(
        post_commit_stop_error,
        VecDeque::from([DispatchError::Retryable(
            "node ack timed out after commit".to_owned(),
        )]),
    );

    let first_controller = force_restart_reconciler(&db.pool, dispatcher.clone(), memory.clone());
    record(
        &db.pool,
        campaign,
        &[replaced_mid_step, post_commit_stop_error],
    )
    .await?;
    tick(&first_controller).await;
    drop(first_controller);

    assert_eq!(
        item(&db.pool, campaign, replaced_mid_step).await?.0,
        "pending"
    );
    assert_eq!(
        item(&db.pool, campaign, post_commit_stop_error).await?.0,
        "pending"
    );
    assert_eq!(
        item(&db.pool, campaign, deadline_dropped_stop).await?.0,
        "failed"
    );
    assert_eq!(
        dispatcher.tracked_driver_stops(),
        3,
        "all three stops recorded substrate ForceRestartProgress::Stopped at commit time"
    );

    // Operator resets `deadline_dropped_stop` via pause + `resume(..., retry_failed = true)`,
    // and a brand-new `Reconciler` + `SandboxForceRestartDriver` takes over
    // with empty process-local memory and recovers all three stopped sandboxes
    // onto `target()` without stopping any of them a second time.
    store::pause_campaign(&db.pool, campaign, &store::PhaseChange::default()).await?;
    let resumed =
        store::resume_campaign(&db.pool, campaign, &store::PhaseChange::default(), true).await?;
    assert_eq!(resumed.phase, CampaignPhase::Canary);

    let second_controller = force_restart_reconciler(&db.pool, dispatcher.clone(), memory);
    while counts(&db.pool, campaign).await?.done == 0 {
        tick(&second_controller).await;
    }
    store::resume_campaign(&db.pool, campaign, &store::PhaseChange::default(), false).await?;
    tick_until_finished(&second_controller, &db.pool, campaign, 5).await?;

    assert_eq!(campaign_phase(&db.pool, campaign).await?, "completed");
    for id in [
        replaced_mid_step,
        post_commit_stop_error,
        deadline_dropped_stop,
    ] {
        assert_eq!(item(&db.pool, campaign, id).await?.0, "done");
        assert_eq!(
            dispatcher
                .calls()
                .iter()
                .filter(|call| **call == Call::Stop(id))
                .count(),
            1,
            "replacement controller did not stop {id} a second time"
        );
        assert_eq!(
            store::subject_sandbox(&db.pool, id)
                .await?
                .expect("sandbox exists")
                .lifecycle,
            "started"
        );
    }
    assert_eq!(dispatcher.tracked_driver_stops(), 0);
    Ok(())
}
