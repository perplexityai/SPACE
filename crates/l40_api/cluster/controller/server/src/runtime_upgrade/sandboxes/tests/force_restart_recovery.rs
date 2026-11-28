//! Recovery, fencing, canary redrive, and controller-replacement tests for the
//! `sandbox_force_restarts` driver.

use std::collections::VecDeque;
use std::time::Duration;

use super::super::force_restarts::POLICY_STOPPED_FORCE_RESTARTING;
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
    assert!(
        dispatcher
            .recorded_stopped_at(campaign, failed_after_stop)
            .is_some()
    );
    assert!(
        dispatcher
            .recorded_stopped_at(campaign, raced_by_user)
            .is_some()
    );

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
    // `failed_after_stop` still holds its recorded `stopped_at` and retries
    // `start` with the exact same `stopped_at` fence, whereas `raced_by_user`'s
    // `lifecycle_changed_at` moved past its `stopped_at` fence while paused so
    // `upgrade` skips it without calling `start` again.
    let resumed = store::resume_campaign(
        &db_handle(&db.pool),
        campaign,
        &store::PhaseChange::default(),
        true,
    )
    .await?;
    assert_eq!(resumed.phase, CampaignPhase::Canary);
    tick_until_canary_done(&reconciler, &db.pool, campaign, 5).await?;
    store::resume_campaign(
        &db_handle(&db.pool),
        campaign,
        &store::PhaseChange::default(),
        false,
    )
    .await?;
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
        to_jiff(raced_row.lifecycle_changed_at) > raced_fences[0].1.stopped_at,
        "user start+stop moved lifecycle_changed_at past the recorded stopped_at fence"
    );
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
            dispatcher.script_post_claim_start_error(
                canary_sandbox,
                DispatchError::Retryable("start response timed out".to_owned()),
            );
        }
        let memory = FakeMemoryRetirement::retired(source());
        let first_driver = SandboxForceRestartDriver::new(
            db.pool.clone(),
            K8S,
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
            store::lease_items(&db.pool, canary_campaign, Duration::from_secs(30), 1).await?;
        if timed_out_start {
            assert_eq!(
                first_driver.upgrade(&canary_row, &first_lease[0]).await,
                ItemOutcome::Retry(ItemError::new("start response timed out"))
            );
            dispatcher
                .settle_lingering_start(&db.pool, canary_sandbox, target())
                .await?;
        } else {
            assert_eq!(
                first_driver.upgrade(&canary_row, &first_lease[0]).await,
                ItemOutcome::Done
            );
            assert_eq!(
                store::started_on_target(&db.pool, &first_lease[0]).await?,
                Some(true)
            );
            dispatcher
                .state()
                .item_stopped_at
                .remove(&(canary_campaign, canary_sandbox));
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
            &db_handle(&db.pool),
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

    // Part 2: Pausing a rolling campaign mid-pass on a settled pass and later
    // resuming it (both when a tick observes `Paused` and when `:pause` +
    // `:resume` happen between two ticks) restarts the paged scan from the
    // beginning so sandboxes that restored onto the deprecated source behind
    // the open cursor aren't missed before completion.
    let paused_campaign = insert_campaign(&db.pool, force_restart_seed()).await?;
    let already_done = resident_on_source(&db.pool, SandboxLifecycle::Started, node).await?;
    let first_behind_cursor = insert_sandbox(
        &db.pool,
        K8S,
        SandboxLifecycle::Suspended,
        node,
        Published::Bound(source()),
    )
    .await?;
    let second_behind_cursor = insert_sandbox(
        &db.pool,
        K8S,
        SandboxLifecycle::Suspended,
        node,
        Published::Bound(source()),
    )
    .await?;
    let mut unbound_on_target = Vec::new();
    for _ in 0..2 {
        unbound_on_target.push(
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
            .await?,
        );
    }
    assert!(already_done < first_behind_cursor);
    assert!(first_behind_cursor < second_behind_cursor);
    assert!(second_behind_cursor < unbound_on_target[0]);

    let pause_dispatcher = FakeDispatcher::default().persisting(db.pool.clone());
    let pause_memory = FakeMemoryRetirement::retired(source());
    let paged = force_restart_paging(&db.pool, pause_dispatcher.clone(), pause_memory, 1);

    // Upgrade `already_done` (`done == 1`, so `:resume` returns to `rolling`),
    // finish that unsettled pass (`unbound[0]`, `unbound[1]`, empty), and open
    // a `settled = true` pass (`after = Some(unbound_on_target[0])`) past both
    // suspended sandboxes without leasing anything.
    let initial = tick(&paged).await;
    assert_eq!((initial.inserted, initial.leased), (1, 1));
    for _ in 0..3 {
        let step = tick(&paged).await;
        assert_eq!((step.inserted, step.leased), (0, 0));
    }
    let page_one = tick(&paged).await;
    assert_eq!((page_one.inserted, page_one.leased), (0, 0));
    assert_eq!(campaign_phase(&db.pool, paused_campaign).await?, "rolling");

    // Case A: A tick runs while paused (`!campaign.phase.leases()`), then the
    // operator resumes; the first post-resume tick must scan from the beginning
    // and upgrade `first_behind_cursor` rather than finishing the stale pass.
    store::pause_campaign(
        &db_handle(&db.pool),
        paused_campaign,
        &store::PhaseChange::default(),
    )
    .await?;
    set_lifecycle(&db.pool, first_behind_cursor, "started").await?;
    tick(&paged).await;
    let resumed_rolling = store::resume_campaign(
        &db_handle(&db.pool),
        paused_campaign,
        &store::PhaseChange::default(),
        false,
    )
    .await?;
    assert_eq!(resumed_rolling.phase, CampaignPhase::Rolling);
    let resumed_page = tick(&paged).await;
    assert_eq!((resumed_page.inserted, resumed_page.leased), (1, 1));

    // Finish the unsettled pass (`unbound[0]`, `unbound[1]`, empty) and open
    // the next `settled = true` pass at `after = Some(unbound_on_target[0])`.
    for _ in 0..3 {
        let step = tick(&paged).await;
        assert_eq!((step.inserted, step.leased), (0, 0));
    }
    assert_eq!(campaign_phase(&db.pool, paused_campaign).await?, "rolling");
    let settled_page_one = tick(&paged).await;
    assert_eq!((settled_page_one.inserted, settled_page_one.leased), (0, 0));
    assert_eq!(campaign_phase(&db.pool, paused_campaign).await?, "rolling");

    // Case B: `:pause` and `:resume` happen between two ticks so `drive` never
    // observes `Paused`; `pass.generation != Some(campaign.generation)` must
    // still restart the `settled = true` pass and upgrade `second_behind_cursor`.
    store::pause_campaign(
        &db_handle(&db.pool),
        paused_campaign,
        &store::PhaseChange::default(),
    )
    .await?;
    set_lifecycle(&db.pool, second_behind_cursor, "started").await?;
    store::resume_campaign(
        &db_handle(&db.pool),
        paused_campaign,
        &store::PhaseChange::default(),
        false,
    )
    .await?;
    let between_ticks_page = tick(&paged).await;
    assert_eq!(
        (between_ticks_page.inserted, between_ticks_page.leased),
        (1, 1)
    );

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
            Call::Stop(first_behind_cursor),
            Call::Start(first_behind_cursor),
            Call::Stop(second_behind_cursor),
            Call::Start(second_behind_cursor),
        ],
        "resuming after pause restarts settled open passes and upgrades behind-cursor residents"
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
    // advances the paused clock past 30s after committing `stopped_at` so
    // `Worker::drive_item` cancels the `stop` future deterministically with
    // zero wall-clock sleep.
    let deadline_controller = Reconciler::new(
        db.pool.clone(),
        Drivers::default().with(SandboxForceRestartDriver::new(
            db.pool.clone(),
            K8S,
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
    assert!(
        dispatcher
            .recorded_stopped_at(campaign, deadline_dropped_stop)
            .is_some()
    );

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
    for id in [
        replaced_mid_step,
        post_commit_stop_error,
        deadline_dropped_stop,
    ] {
        assert!(
            dispatcher.recorded_stopped_at(campaign, id).is_some(),
            "stop recorded stopped_at at commit time for {id}"
        );
    }

    // Operator resets `deadline_dropped_stop` via pause + `resume(..., retry_failed = true)`,
    // and a brand-new `Reconciler` + `SandboxForceRestartDriver` takes over
    // with empty process-local memory and recovers all three stopped sandboxes
    // onto `target()` without stopping any of them a second time.
    store::pause_campaign(
        &db_handle(&db.pool),
        campaign,
        &store::PhaseChange::default(),
    )
    .await?;
    let resumed = store::resume_campaign(
        &db_handle(&db.pool),
        campaign,
        &store::PhaseChange::default(),
        true,
    )
    .await?;
    assert_eq!(resumed.phase, CampaignPhase::Canary);

    let second_controller = force_restart_reconciler(&db.pool, dispatcher.clone(), memory);
    tick_until_canary_done(&second_controller, &db.pool, campaign, 5).await?;
    store::resume_campaign(
        &db_handle(&db.pool),
        campaign,
        &store::PhaseChange::default(),
        false,
    )
    .await?;
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
            "replacement controller didn't stop {id} a second time"
        );
        assert_eq!(
            store::subject_sandbox(&db.pool, id)
                .await?
                .expect("sandbox exists")
                .lifecycle,
            "started"
        );
    }
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn lingering_stop_interleavings_and_in_flight_stop_unretirement_never_strand_or_boot_early()
-> TestResult {
    let Some(db) = test_db().await? else {
        return Ok(());
    };
    seed_policy(&db.pool, RuntimeEnvironmentState::Deprecated, true).await?;
    let campaign = insert_campaign(
        &db.pool,
        CampaignSeed {
            phase: CampaignPhase::Rolling,
            per_node: 8,
            per_cluster: 8,
            failure_budget: 5,
            ..force_restart_seed()
        },
    )
    .await?;
    let node = node(&db.pool).await?;

    let lingering_stopping = resident_on_source(&db.pool, SandboxLifecycle::Started, node).await?;
    let lingering_settled = resident_on_source(&db.pool, SandboxLifecycle::Started, node).await?;
    let first_attempt_internally_retried =
        resident_on_source(&db.pool, SandboxLifecycle::Started, node).await?;
    let unretired_during_stop =
        resident_on_source(&db.pool, SandboxLifecycle::Started, node).await?;

    let dispatcher = FakeDispatcher::default().persisting(db.pool.clone());
    for id in [lingering_stopping, lingering_settled] {
        dispatcher.state().stop_errors.insert(
            id,
            VecDeque::from([DispatchError::Retryable(
                "stop rpc timed out while node still processing".to_owned(),
            )]),
        );
    }
    dispatcher
        .state()
        .lingering_stops_on_stop_call
        .insert(first_attempt_internally_retried);
    let memory = FakeMemoryRetirement::retired(source());
    let driver =
        SandboxForceRestartDriver::new(db.pool.clone(), K8S, dispatcher.clone(), memory.clone());
    record(
        &db.pool,
        campaign,
        &[
            lingering_stopping,
            lingering_settled,
            first_attempt_internally_retried,
            unretired_during_stop,
        ],
    )
    .await?;
    let campaign_row = store::list_unfinished(&db.pool)
        .await?
        .into_iter()
        .find(|listed| listed.id == campaign)
        .expect("campaign");

    // Attempt 1 on `lingering_stopping` and `lingering_settled` times out,
    // leaving both items on a continuing run (`continuing_run == true`).
    let first_leases = store::lease_items(&db.pool, campaign, Duration::from_secs(30), 16).await?;
    for id in [lingering_stopping, lingering_settled] {
        let lease = first_leases
            .iter()
            .find(|item| item.subject_id == id)
            .expect("lingering item leased");
        let outcome = driver.upgrade(&campaign_row, lease).await;
        assert!(matches!(outcome, ItemOutcome::Retry(_)));
        store::finish_item(&db.pool, lease, &outcome).await?;
    }

    // On `first_attempt_internally_retried`, attempt 1 starts fresh
    // (`continuing_run == Some(false)`), and `dispatcher.stop` simulates an
    // internally retried stop whose first RPC settled `('stopped', stopped_at)`
    // before its retry saw `GuardedStop::Declined`; `recover_after_declined_stop`
    // must still read `stopped_at(item)` and cold-boot straight onto `target()`.
    let first_retry_lease = first_leases
        .iter()
        .find(|item| item.subject_id == first_attempt_internally_retried)
        .expect("first_attempt_internally_retried leased");
    assert_eq!(
        store::continuing_run(&db.pool, first_retry_lease).await?,
        Some(false)
    );
    assert_eq!(
        driver.upgrade(&campaign_row, first_retry_lease).await,
        ItemOutcome::Done,
        "first-attempt internally retried stop that declines after settling recovers straight to start"
    );
    store::finish_item(&db.pool, first_retry_lease, &ItemOutcome::Done).await?;
    assert!(
        dispatcher
            .calls()
            .contains(&Call::Start(first_attempt_internally_retried))
    );

    // On `unretired_during_stop`, revoke `source()` memory retirement right
    // after `stop` commits `stopped_at` and before `upgrade` reaches `start`:
    // `upgrade` must hold (`Release`) without calling `start`.
    let unretired_lease = first_leases
        .iter()
        .find(|item| item.subject_id == unretired_during_stop)
        .expect("unretired_during_stop leased");
    let memory_for_hook = memory.clone();
    dispatcher.on_stop_commit(unretired_during_stop, move || {
        memory_for_hook.unretire(source());
    });
    let mid_stop_outcome = driver.upgrade(&campaign_row, unretired_lease).await;
    assert_eq!(
        mid_stop_outcome,
        ItemOutcome::Release(ItemError::new(POLICY_STOPPED_FORCE_RESTARTING))
    );
    store::finish_item(&db.pool, unretired_lease, &mid_stop_outcome).await?;
    assert!(
        !dispatcher
            .calls()
            .contains(&Call::Start(unretired_during_stop)),
        "revoking memory retirement while stop is in flight blocks start"
    );
    assert!(
        dispatcher
            .recorded_stopped_at(campaign, unretired_during_stop)
            .is_some()
    );

    // Restore memory retirement and simulate lingering prior-attempt stops
    // landing right before attempt 2's `dispatcher.stop` row claim:
    // - `lingering_stopping` enters `stopping` -> `Release` (not `Skipped`);
    // - `lingering_settled` commits `stopped` + `stopped_at` -> `recover_after_declined_stop`
    //   immediately cold-boots it onto `target()` (`Done`).
    // Meanwhile `unretired_during_stop` finishes cold-booting onto `target()`
    // without a second `stop`.
    memory.retire(source());
    dispatcher
        .state()
        .lingering_stopping_on_stop_call
        .insert(lingering_stopping);
    dispatcher
        .state()
        .lingering_stops_on_stop_call
        .insert(lingering_settled);

    let second_leases = store::lease_items(&db.pool, campaign, Duration::from_secs(30), 16).await?;
    let stopping_lease = second_leases
        .iter()
        .find(|item| item.subject_id == lingering_stopping)
        .expect("lingering_stopping re-leased");
    let stopping_outcome = driver.upgrade(&campaign_row, stopping_lease).await;
    assert_eq!(
        stopping_outcome,
        ItemOutcome::Release(ItemError::new(
            "the sandbox is stopping; retrying once it settles"
        ))
    );
    store::finish_item(&db.pool, stopping_lease, &stopping_outcome).await?;

    let settled_lease = second_leases
        .iter()
        .find(|item| item.subject_id == lingering_settled)
        .expect("lingering_settled re-leased");
    assert_eq!(
        driver.upgrade(&campaign_row, settled_lease).await,
        ItemOutcome::Done,
        "lingering prior-attempt stop settling before declined stop recovers straight to start"
    );
    store::finish_item(&db.pool, settled_lease, &ItemOutcome::Done).await?;

    let resumed_mid_stop = second_leases
        .iter()
        .find(|item| item.subject_id == unretired_during_stop)
        .expect("unretired_during_stop re-leased");
    assert_eq!(
        driver.upgrade(&campaign_row, resumed_mid_stop).await,
        ItemOutcome::Done
    );
    store::finish_item(&db.pool, resumed_mid_stop, &ItemOutcome::Done).await?;
    assert_eq!(
        dispatcher
            .calls()
            .iter()
            .filter(|call| **call == Call::Stop(unretired_during_stop))
            .count(),
        1,
        "unretired_during_stop wasn't stopped a second time after hold cleared"
    );
    assert_eq!(
        dispatcher
            .calls()
            .iter()
            .filter(|call| **call == Call::Start(unretired_during_stop))
            .count(),
        1
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn empty_error_messages_recover_and_superseded_stop_never_certifies_external_upgrade()
-> TestResult {
    let Some(db) = test_db().await? else {
        return Ok(());
    };
    seed_policy(&db.pool, RuntimeEnvironmentState::Deprecated, true).await?;
    let campaign = insert_campaign(
        &db.pool,
        CampaignSeed {
            phase: CampaignPhase::Canary,
            per_node: 4,
            per_cluster: 4,
            failure_budget: 4,
            ..force_restart_seed()
        },
    )
    .await?;
    let node = node(&db.pool).await?;

    let empty_errors = resident_on_source(&db.pool, SandboxLifecycle::Started, node).await?;
    let superseded_canary = resident_on_source(&db.pool, SandboxLifecycle::Started, node).await?;

    let dispatcher = FakeDispatcher::default().persisting(db.pool.clone());
    dispatcher.state().post_commit_stop_errors.insert(
        empty_errors,
        VecDeque::from([DispatchError::Retryable(String::new())]),
    );
    dispatcher.script_start(
        empty_errors,
        vec![
            Ok(TargetStart::HeldStopped(String::new())),
            Ok(TargetStart::Started),
        ],
    );
    dispatcher.state().post_commit_stop_errors.insert(
        superseded_canary,
        VecDeque::from([DispatchError::Retryable("stop ack lost".to_owned())]),
    );

    let memory = FakeMemoryRetirement::retired(source());
    let driver =
        SandboxForceRestartDriver::new(db.pool.clone(), K8S, dispatcher.clone(), memory.clone());
    // Drive `superseded_canary` first while the campaign is in `Canary` (which
    // leases at most one item at a time and promotes to `Rolling` on a single
    // `Done` item):
    // 1. Attempt 1 commits `stopped_at` (`t1`) and fails post-commit (`Retry`).
    // 2. The user restarts it back onto `source()` (`t2 > t1`) while memory
    //    retirement is temporarily cleared -> `inspect_subject` clears the
    //    superseded `stopped_at` before `upgrade_running` returns `Release`.
    // 3. An external upgrade moves it onto `target()` (`t3 > t2`) before memory
    //    retirement returns -> attempt 3 skips with `NO_LONGER_ON_SOURCE`
    //    instead of certifying the canary on an external upgrade.
    record(&db.pool, campaign, &[superseded_canary]).await?;
    let campaign_row = store::list_unfinished(&db.pool)
        .await?
        .into_iter()
        .find(|listed| listed.id == campaign)
        .expect("campaign");

    let sup_first = &store::lease_items(&db.pool, campaign, Duration::from_secs(30), 16).await?[0];
    let sup_retry = driver.upgrade(&campaign_row, sup_first).await;
    assert!(matches!(sup_retry, ItemOutcome::Retry(_)));
    store::finish_item(&db.pool, sup_first, &sup_retry).await?;

    sqlx::query(sql!(
        "UPDATE sandboxes
         SET lifecycle = 'started',
             lifecycle_changed_at = lifecycle_changed_at + INTERVAL '5 seconds'
         WHERE id = $1"
    ))
    .bind(superseded_canary)
    .execute(&db.pool)
    .await?;
    memory.unretire(source());
    let sup_second = &store::lease_items(&db.pool, campaign, Duration::from_secs(30), 16).await?[0];
    let released_on_source = driver.upgrade(&campaign_row, sup_second).await;
    assert_eq!(
        released_on_source,
        ItemOutcome::Release(ItemError::new(POLICY_STOPPED_FORCE_RESTARTING))
    );
    store::finish_item(&db.pool, sup_second, &released_on_source).await?;
    assert_eq!(
        dispatcher.recorded_stopped_at(campaign, superseded_canary),
        None,
        "restarting onto source clears the superseded stopped_at before Release"
    );

    sqlx::query(sql!(
        "UPDATE sandboxes
         SET lifecycle = 'started',
             lifecycle_changed_at = lifecycle_changed_at + INTERVAL '10 seconds'
         WHERE id = $1"
    ))
    .bind(superseded_canary)
    .execute(&db.pool)
    .await?;
    set_bound(&db.pool, superseded_canary, target()).await?;
    memory.retire(source());

    let sup_third = &store::lease_items(&db.pool, campaign, Duration::from_secs(30), 16).await?[0];
    let final_superseded = driver.upgrade(&campaign_row, sup_third).await;
    assert_eq!(
        final_superseded,
        ItemOutcome::Skipped(ItemError::new(NO_LONGER_ON_SOURCE))
    );
    store::finish_item(&db.pool, sup_third, &final_superseded).await?;

    // Because `superseded_canary` skipped (`done == 0`), the canary still
    // leases `empty_errors`. Empty error messages on both `Retry` and
    // `HeldStopped` (`Release`) still keep `continuing_run == true` so the
    // campaign-stopped sandbox recovers onto `target()` without stopping twice.
    record(&db.pool, campaign, &[empty_errors]).await?;
    let empty_first =
        &store::lease_items(&db.pool, campaign, Duration::from_secs(30), 16).await?[0];
    let empty_retry = driver.upgrade(&campaign_row, empty_first).await;
    assert_eq!(empty_retry, ItemOutcome::Retry(ItemError::new("")));
    store::finish_item(&db.pool, empty_first, &empty_retry).await?;

    let empty_second =
        &store::lease_items(&db.pool, campaign, Duration::from_secs(30), 16).await?[0];
    assert_eq!(
        store::continuing_run(&db.pool, empty_second).await?,
        Some(true)
    );
    let held_empty = driver.upgrade(&campaign_row, empty_second).await;
    assert_eq!(held_empty, ItemOutcome::Release(ItemError::new("")));
    store::finish_item(&db.pool, empty_second, &held_empty).await?;

    let empty_third =
        &store::lease_items(&db.pool, campaign, Duration::from_secs(30), 16).await?[0];
    assert_eq!(
        store::continuing_run(&db.pool, empty_third).await?,
        Some(true)
    );
    assert_eq!(
        driver.upgrade(&campaign_row, empty_third).await,
        ItemOutcome::Done
    );
    store::finish_item(&db.pool, empty_third, &ItemOutcome::Done).await?;
    assert_eq!(
        dispatcher
            .calls()
            .iter()
            .filter(|call| **call == Call::Stop(empty_errors))
            .count(),
        1
    );
    assert_eq!(
        counts(&db.pool, campaign).await?,
        ItemCounts {
            done: 1,
            skipped: 1,
            ..ItemCounts::default()
        }
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn lapsed_lease_bails_out_without_clearing_live_stopped_at() -> TestResult {
    let Some(db) = test_db().await? else {
        return Ok(());
    };
    seed_policy(&db.pool, RuntimeEnvironmentState::Deprecated, true).await?;
    let campaign = insert_campaign(&db.pool, force_restart_seed()).await?;
    let node = node(&db.pool).await?;
    let sandbox = resident_on_source(&db.pool, SandboxLifecycle::Started, node).await?;
    record(&db.pool, campaign, &[sandbox]).await?;

    let dispatcher = FakeDispatcher::default().persisting(db.pool.clone());
    dispatcher.script_start(
        sandbox,
        vec![
            Err(DispatchError::Retryable("transient start rpc".to_owned())),
            Ok(TargetStart::Started),
        ],
    );
    let driver = SandboxForceRestartDriver::new(
        db.pool.clone(),
        K8S,
        dispatcher.clone(),
        FakeMemoryRetirement::retired(source()),
    );
    let campaign_row = store::list_unfinished(&db.pool)
        .await?
        .into_iter()
        .find(|listed| listed.id == campaign)
        .expect("campaign");

    // Lease 1 stops the sandbox (`stopped_at` recorded) and fails `start` (`Retry`).
    let first = &store::lease_items(&db.pool, campaign, Duration::from_secs(30), 16).await?[0];
    let first_outcome = driver.upgrade(&campaign_row, first).await;
    assert!(matches!(first_outcome, ItemOutcome::Retry(_)));
    store::finish_item(&db.pool, first, &first_outcome).await?;
    let owned_stop = dispatcher
        .recorded_stop(campaign, sandbox)
        .and_then(RecordedStop::into_fence)
        .expect("stopped_at recorded by lease 1");

    // Lease 2 claims the item, then expires so Lease 3 steals the row.
    let stale_lease =
        store::lease_items(&db.pool, campaign, Duration::from_secs(30), 16).await?[0].clone();
    sqlx::query(sql!(
        "UPDATE runtime_upgrade_items
         SET lease_expires_at = NOW() - INTERVAL '1 second'
         WHERE campaign_id = $1 AND subject_id = $2"
    ))
    .bind(campaign)
    .bind(sandbox)
    .execute(&db.pool)
    .await?;
    store::redrive_expired(&db.pool, campaign, MAX_ATTEMPTS).await?;
    let active_lease =
        store::lease_items(&db.pool, campaign, Duration::from_secs(30), 16).await?[0].clone();
    assert_ne!(stale_lease.lease_token, active_lease.lease_token);

    // Even a direct `clear_stopped_at` from the stale lease is fenced out by `lease_token`.
    dispatcher
        .clear_stopped_at(&stale_lease, RecordedStop::Stopped(owned_stop.clone()))
        .await
        .expect("fenced clear_stopped_at is a no-op");
    assert_eq!(
        dispatcher.recorded_stopped_at(campaign, sandbox),
        Some(owned_stop.stopped_at)
    );

    // Running `upgrade` with the stale lease sees `continuing_run == None` and
    // releases without clearing `stopped_at` or dispatching any RPCs.
    let calls_before_stale = dispatcher.calls().len();
    let stale_outcome = driver.upgrade(&campaign_row, &stale_lease).await;
    assert_eq!(
        stale_outcome,
        ItemOutcome::Release(ItemError::new(
            "the campaign item lease is no longer active"
        ))
    );
    assert_eq!(dispatcher.calls().len(), calls_before_stale);
    assert_eq!(
        dispatcher.recorded_stopped_at(campaign, sandbox),
        Some(owned_stop.stopped_at)
    );

    // The active lease recovers the campaign-owned stop onto `target()`.
    assert_eq!(
        driver.upgrade(&campaign_row, &active_lease).await,
        ItemOutcome::Done
    );
    store::finish_item(&db.pool, &active_lease, &ItemOutcome::Done).await?;
    assert_eq!(counts(&db.pool, campaign).await?.done, 1);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn lingering_start_between_stop_and_start_preserves_canary_gating_and_completion()
-> TestResult {
    let Some(db) = test_db().await? else {
        return Ok(());
    };
    seed_policy(&db.pool, RuntimeEnvironmentState::Deprecated, true).await?;
    let campaign = insert_campaign(
        &db.pool,
        CampaignSeed {
            phase: CampaignPhase::Canary,
            per_node: 4,
            per_cluster: 4,
            failure_budget: 4,
            ..force_restart_seed()
        },
    )
    .await?;
    let node = node(&db.pool).await?;
    let canary = resident_on_source(&db.pool, SandboxLifecycle::Started, node).await?;
    let waiting = resident_on_source(&db.pool, SandboxLifecycle::Started, node).await?;
    record(&db.pool, campaign, &[canary, waiting]).await?;

    let dispatcher = FakeDispatcher::default().persisting(db.pool.clone());
    // 1. Right after `stop` commits on attempt 1, a concurrent wake moves `canary`
    //    into `starting` before `upgrade_running`'s post-stop re-read.
    dispatcher.state().post_stop_commit_starting.insert(canary);

    let driver = SandboxForceRestartDriver::new(
        db.pool.clone(),
        K8S,
        dispatcher.clone(),
        FakeMemoryRetirement::retired(source()),
    );
    let campaign_row = store::list_unfinished(&db.pool)
        .await?
        .into_iter()
        .find(|listed| listed.id == campaign)
        .expect("campaign");

    // Attempt 1: post-stop `starting` holds the canary (`Release`) without calling `start`
    // or advancing the canary past `canary` onto `waiting`.
    let lease_1 = &store::lease_items(&db.pool, campaign, Duration::from_secs(30), 16).await?[0];
    assert_eq!(lease_1.subject_id, canary);
    let out_1 = driver.upgrade(&campaign_row, lease_1).await;
    assert_eq!(
        out_1,
        ItemOutcome::Release(ItemError::new(
            "the sandbox is starting; retrying once it settles"
        ))
    );
    store::finish_item(&db.pool, lease_1, &out_1).await?;
    assert!(
        !dispatcher.calls().contains(&Call::Start(canary)),
        "post-stop move into starting shouldn't call start with a stale stopped_at"
    );

    // Settle `canary` back onto `source()` (`started`), then on attempt 2 have a
    // lingering `start` move it to `starting` right before `dispatcher.start`
    // claims the stopped row (`TargetStart::NotOnTarget`). That must also `Release`
    // (not `Skipped`!), keeping `waiting` gated behind the canary.
    set_lifecycle(&db.pool, canary, SandboxLifecycle::Started.as_str()).await?;
    dispatcher
        .state()
        .lingering_starting_before_start
        .insert(canary);
    let lease_2 = &store::lease_items(&db.pool, campaign, Duration::from_secs(30), 16).await?[0];
    assert_eq!(lease_2.subject_id, canary);
    let out_2 = driver.upgrade(&campaign_row, lease_2).await;
    assert_eq!(
        out_2,
        ItemOutcome::Release(ItemError::new(
            "the sandbox is starting; retrying once it settles"
        ))
    );
    store::finish_item(&db.pool, lease_2, &out_2).await?;

    // Settle `canary` back onto `source()` (`started`), then on attempt 3 have
    // `dispatcher.start` return `TargetStart::HeldStopped` while `canary` is
    // still stopped at `stopped_at` on `source()`: `start_owned_stop` keeps
    // `stopped_at` intact and releases the item instead of stranding a
    // campaign-stopped sandbox in `skipped`.
    set_lifecycle(&db.pool, canary, SandboxLifecycle::Started.as_str()).await?;
    dispatcher.script_start(
        canary,
        vec![Ok(TargetStart::HeldStopped(
            "transient default flap inside start".to_owned(),
        ))],
    );
    let lease_3 = &store::lease_items(&db.pool, campaign, Duration::from_secs(30), 16).await?[0];
    assert_eq!(lease_3.subject_id, canary);
    let out_3 = driver.upgrade(&campaign_row, lease_3).await;
    assert_eq!(
        out_3,
        ItemOutcome::Release(ItemError::new("transient default flap inside start"))
    );
    store::finish_item(&db.pool, lease_3, &out_3).await?;
    assert!(dispatcher.recorded_stopped_at(campaign, canary).is_some());

    // On attempt 4 (`canary` still at `stopped_at`), have a lingering `start`
    // settle `canary` on `started` under `target()` right before `dispatcher.start`
    // claims the row (`TargetStart::NotOnTarget`). Re-inspection sees `started` on
    // `target()` after `owned_stop` and certifies `Done`!
    dispatcher
        .state()
        .lingering_started_on_target_before_start
        .insert(canary);
    let lease_4 = &store::lease_items(&db.pool, campaign, Duration::from_secs(30), 16).await?[0];
    assert_eq!(lease_4.subject_id, canary);
    let out_4 = driver.upgrade(&campaign_row, lease_4).await;
    assert_eq!(out_4, ItemOutcome::Done);
    store::finish_item(&db.pool, lease_4, &out_4).await?;
    assert_eq!(
        counts(&db.pool, campaign).await?,
        ItemCounts {
            pending: 1,
            done: 1,
            ..ItemCounts::default()
        }
    );

    // Advance to `rolling` and verify two post-stop edge cases on `waiting` and
    // a third subject `moved_elsewhere`:
    // - `waiting` stops (`Retry` on `start`), restarts on `source()`, and is
    //   deleted right between `inspect_subject`'s initial read and post-`stopped_at`
    //   re-read -> clears `stopped_at` and skips.
    // - `moved_elsewhere` stops (`Retry` on `start`), then while still at
    //   `stopped_at` has its recorded execution moved to `environment(9)` (neither
    //   `source()` nor `target()`) -> `start` returns `NotOnTarget`, clearing
    //   `stopped_at` and skipping.
    store::resume_campaign(
        &db_handle(&db.pool),
        campaign,
        &store::PhaseChange::default(),
        false,
    )
    .await?;
    let moved_elsewhere = resident_on_source(&db.pool, SandboxLifecycle::Started, node).await?;
    let declined_in_start = resident_on_source(&db.pool, SandboxLifecycle::Started, node).await?;
    record(&db.pool, campaign, &[moved_elsewhere, declined_in_start]).await?;
    let rolling_row = store::list_unfinished(&db.pool)
        .await?
        .into_iter()
        .find(|listed| listed.id == campaign)
        .expect("rolling campaign");
    for id in [waiting, moved_elsewhere, declined_in_start] {
        dispatcher.script_start(
            id,
            vec![Err(DispatchError::Retryable("slot busy".to_owned()))],
        );
    }
    let mut expected_batch = [waiting, moved_elsewhere, declined_in_start];
    expected_batch.sort();
    let first_batch = store::lease_items(&db.pool, campaign, Duration::from_secs(30), 16).await?;
    let mut first_ids: Vec<Uuid> = first_batch.iter().map(|item| item.subject_id).collect();
    first_ids.sort();
    assert_eq!(first_ids, expected_batch);
    for leased in first_batch {
        let out = driver.upgrade(&rolling_row, &leased).await;
        assert_eq!(out, ItemOutcome::Retry(ItemError::new("slot busy")));
        store::finish_item(&db.pool, &leased, &out).await?;
    }

    set_lifecycle(&db.pool, waiting, SandboxLifecycle::Started.as_str()).await?;
    dispatcher.state().delete_on_stopped_at_read.insert(waiting);
    set_bound(&db.pool, moved_elsewhere, environment(9)).await?;
    // `declined_in_start` stays stopped at `stopped_at` on `source()` when
    // `dispatcher.start` returns `TargetStart::Declined` (permanent start
    // refusal, as opposed to `HeldStopped` or state-change `NotOnTarget`):
    // `recover_after_post_stop_move` clears `stopped_at` and skips rather than
    // looping in `Release`.
    dispatcher.script_start(
        declined_in_start,
        vec![Ok(TargetStart::Declined(
            "template release permanently incompatible".to_owned(),
        ))],
    );

    // Before skipping `moved_elsewhere`, verify that if a campaign-owned stop
    // already moved recorded execution onto `target()` while `source()` is
    // temporarily un-retired, `admit_start` still honors the campaign hold
    // (`Release` with `stopped_at` preserved) instead of dispatching `start`.
    set_bound(&db.pool, moved_elsewhere, target()).await?;
    let unretired_driver = SandboxForceRestartDriver::new(
        db.pool.clone(),
        K8S,
        dispatcher.clone(),
        FakeMemoryRetirement::default(),
    );
    let held_batch = store::lease_items(&db.pool, campaign, Duration::from_secs(30), 16).await?;
    let mut held_ids: Vec<Uuid> = held_batch.iter().map(|item| item.subject_id).collect();
    held_ids.sort();
    assert_eq!(held_ids, expected_batch);
    for item in &held_batch {
        if item.subject_id == moved_elsewhere {
            let held_out = unretired_driver.upgrade(&rolling_row, item).await;
            assert_eq!(
                held_out,
                ItemOutcome::Release(ItemError::new(
                    "the policy no longer force-restarts this campaign's sandboxes"
                )),
                "admit_start must enforce campaign holds even when recorded execution already names target()"
            );
            store::finish_item(&db.pool, item, &held_out).await?;
        } else {
            store::finish_item(
                &db.pool,
                item,
                &ItemOutcome::Release(ItemError::new("hold")),
            )
            .await?;
        }
    }
    assert!(
        dispatcher
            .recorded_stopped_at(campaign, moved_elsewhere)
            .is_some()
    );
    set_bound(&db.pool, moved_elsewhere, environment(9)).await?;

    let second_batch = store::lease_items(&db.pool, campaign, Duration::from_secs(30), 16).await?;
    let mut second_ids: Vec<Uuid> = second_batch.iter().map(|item| item.subject_id).collect();
    second_ids.sort();
    assert_eq!(second_ids, expected_batch);
    let waiting_stops_before = dispatcher
        .calls()
        .into_iter()
        .filter(|call| *call == Call::Stop(waiting))
        .count();
    for leased in second_batch {
        let out = driver.upgrade(&rolling_row, &leased).await;
        assert!(matches!(out, ItemOutcome::Skipped(_)), "got {out:?}");
        store::finish_item(&db.pool, &leased, &out).await?;
        assert_eq!(
            dispatcher.recorded_stopped_at(campaign, leased.subject_id),
            None
        );
    }
    let waiting_stops_after = dispatcher
        .calls()
        .into_iter()
        .filter(|call| *call == Call::Stop(waiting))
        .count();
    assert_eq!(
        waiting_stops_after, waiting_stops_before,
        "mid-inspect deletion on `waiting` must skip immediately without re-dispatching `stop`"
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn reopened_item_preserves_unstarted_cleanup_across_pre_cleanup_retries() -> TestResult {
    let Some(db) = test_db().await? else {
        return Ok(());
    };
    seed_policy(&db.pool, RuntimeEnvironmentState::Deprecated, true).await?;
    let campaign = insert_campaign(&db.pool, force_restart_seed()).await?;
    let node = node(&db.pool).await?;
    let sandbox = resident_on_source(&db.pool, SandboxLifecycle::Stopped, node).await?;
    record(&db.pool, campaign, &[sandbox]).await?;

    let dispatcher = FakeDispatcher::default().persisting(db.pool.clone());
    // Seed a stale `stopped_at` matching the sandbox's current `lifecycle_changed_at`
    // on a fresh/reopened item (`last_error IS NULL`). If `!continuing` cleanup were
    // lost after a pre-cleanup `Retry`, the driver would mistake this user-stopped
    // sandbox for a campaign-owned stop and call `Start` instead of `Upgrade`!
    let current_stopped_at: time::OffsetDateTime = sqlx::query_scalar(sql!(
        "SELECT lifecycle_changed_at FROM sandboxes WHERE id = $1"
    ))
    .bind(sandbox)
    .fetch_one(&db.pool)
    .await?;
    dispatcher.state().item_stopped_at.insert(
        (campaign, sandbox),
        RecordedStop::Stopped(StopFence::at(to_jiff(current_stopped_at))),
    );
    dispatcher.state().stopped_at_read_errors.insert(
        sandbox,
        VecDeque::from([DispatchError::Retryable(
            "transient stopped_at read timeout".to_owned(),
        )]),
    );
    dispatcher.state().clear_stopped_at_errors.insert(
        sandbox,
        VecDeque::from([DispatchError::Retryable(
            "transient clear_stopped_at write timeout".to_owned(),
        )]),
    );

    let driver = SandboxForceRestartDriver::new(
        db.pool.clone(),
        K8S,
        dispatcher.clone(),
        FakeMemoryRetirement::retired(source()),
    );
    let campaign_row = store::list_unfinished(&db.pool)
        .await?
        .into_iter()
        .find(|listed| listed.id == campaign)
        .expect("campaign");

    // Attempt 1 fails reading `stopped_at` before `mark_run_started` runs.
    let lease_1 = &store::lease_items(&db.pool, campaign, Duration::from_secs(30), 16).await?[0];
    assert_eq!(store::continuing_run(&db.pool, lease_1).await?, Some(false));
    let retry_1 = driver.upgrade(&campaign_row, lease_1).await;
    assert!(matches!(retry_1, ItemOutcome::Retry(_)));
    store::finish_item(&db.pool, lease_1, &retry_1).await?;

    // Attempt 2 still sees `continuing_run == Some(false)`, and fails inside
    // `clear_stopped_at` before `mark_run_started` runs.
    let lease_2 = &store::lease_items(&db.pool, campaign, Duration::from_secs(30), 16).await?[0];
    assert_eq!(store::continuing_run(&db.pool, lease_2).await?, Some(false));
    let retry_2 = driver.upgrade(&campaign_row, lease_2).await;
    assert!(matches!(retry_2, ItemOutcome::Retry(_)));
    store::finish_item(&db.pool, lease_2, &retry_2).await?;
    assert!(dispatcher.recorded_stopped_at(campaign, sandbox).is_some());

    // Attempt 3 loses its lease (`redrive_expired`) before `mark_run_started`
    // commits, and Attempt 4 hits the item deadline (`Failed` + `:resume` with
    // `retry_failed = true`) before `mark_run_started` commits; both keep
    // `continuing_run == Some(false)` so stale `stopped_at` is still cleared.
    let lease_3 = &store::lease_items(&db.pool, campaign, Duration::from_secs(30), 16).await?[0];
    assert_eq!(store::continuing_run(&db.pool, lease_3).await?, Some(false));
    sqlx::query(sql!(
        "UPDATE runtime_upgrade_items \
         SET lease_expires_at = NOW() - INTERVAL '1 second' \
         WHERE campaign_id = $1 AND subject_id = $2"
    ))
    .bind(campaign.0)
    .bind(sandbox)
    .execute(&db.pool)
    .await?;
    store::redrive_expired(&db.pool, campaign, 10).await?;

    let lease_4 = &store::lease_items(&db.pool, campaign, Duration::from_secs(30), 16).await?[0];
    assert_eq!(store::continuing_run(&db.pool, lease_4).await?, Some(false));
    store::finish_item(
        &db.pool,
        lease_4,
        &ItemOutcome::Failed(ItemError::new("the upgrade didn't finish within 60s")),
    )
    .await?;
    store::pause_campaign(
        &db_handle(&db.pool),
        campaign,
        &store::PhaseChange::default(),
    )
    .await?;
    store::resume_campaign(
        &db_handle(&db.pool),
        campaign,
        &store::PhaseChange::default(),
        true,
    )
    .await?;
    let resumed_row = store::list_unfinished(&db.pool)
        .await?
        .into_iter()
        .find(|listed| listed.id == campaign)
        .expect("resumed campaign");

    // Attempt 5 still sees `continuing_run == Some(false)`, clears the stale
    // `stopped_at`, and skips the user-stopped sandbox without waking it.
    let lease_5 = &store::lease_items(&db.pool, campaign, Duration::from_secs(30), 16).await?[0];
    assert_eq!(store::continuing_run(&db.pool, lease_5).await?, Some(false));
    let skipped = driver.upgrade(&resumed_row, lease_5).await;
    assert_eq!(
        skipped,
        ItemOutcome::Skipped(ItemError::new(
            "the sandbox is stopped, which this campaign doesn't upgrade"
        ))
    );
    store::finish_item(&db.pool, lease_5, &skipped).await?;
    assert_eq!(dispatcher.recorded_stopped_at(campaign, sandbox), None);
    assert!(dispatcher.calls().is_empty());

    // Once `skipped` (or after lease loss / pause / abort), a delayed `stop`
    // carrying a stale `LeasedItem` declines at claim time without stopping a
    // running sandbox or writing `stopped_at` onto the terminal item row.
    set_lifecycle(&db.pool, sandbox, SandboxLifecycle::Started.as_str()).await?;
    assert_eq!(
        dispatcher
            .stop(
                lease_5,
                true,
                SandboxId::from_uuid(sandbox),
                source(),
                target(),
            )
            .await?,
        GuardedStop::Held("the campaign item lease is no longer active".to_owned())
    );
    let current_lifecycle: String =
        sqlx::query_scalar(sql!("SELECT lifecycle FROM sandboxes WHERE id = $1"))
            .bind(sandbox)
            .fetch_one(&db.pool)
            .await?;
    assert_eq!(current_lifecycle, SandboxLifecycle::Started.as_str());
    assert_eq!(
        dispatcher.recorded_stopped_at(campaign, sandbox),
        None,
        "stale lease stop neither stops the sandbox nor writes stopped_at"
    );
    Ok(())
}
