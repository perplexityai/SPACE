//! Regression coverage for `sandbox_force_restarts` stop-ownership fences,
//! mid-transition candidate scanning, and post-stop `Done` attribution.

use std::collections::{HashMap, HashSet};
use std::time::Duration;

use pplx_space_public_api::{OperationKind, OperationState, OperationTargetType, PublicSandboxId};
use pplx_space_sandbox_model::SandboxId;

use super::super::dispatcher::{MID_TRANSITION_LIFECYCLES, is_mid_transition};
use super::super::force_restarts::{
    EXTERNAL_OPERATION_IN_FLIGHT, EXTERNAL_STOP_SUPERSEDED, SETTLED_SCANNED_LIFECYCLES,
    UNPUBLISHED_EXECUTION,
};
use super::*;

#[test]
fn scanned_lifecycles_match_active_runtime_and_mid_transition_variants() {
    let settled: HashSet<SandboxLifecycle> = SETTLED_SCANNED_LIFECYCLES.iter().copied().collect();
    let transitions: HashSet<SandboxLifecycle> =
        MID_TRANSITION_LIFECYCLES.iter().copied().collect();
    assert_eq!(
        settled.len(),
        SETTLED_SCANNED_LIFECYCLES.len(),
        "SETTLED_SCANNED_LIFECYCLES contains no duplicate variants"
    );
    assert_eq!(
        transitions.len(),
        MID_TRANSITION_LIFECYCLES.len(),
        "MID_TRANSITION_LIFECYCLES contains no duplicate variants"
    );
    assert!(
        settled.is_disjoint(&transitions),
        "settled and mid-transition scan slices must be disjoint"
    );
    for &lifecycle in SandboxLifecycle::all() {
        assert_eq!(
            settled.contains(&lifecycle),
            lifecycle.is_active_runtime(),
            "lifecycle {lifecycle:?} settled scan membership mismatch"
        );
        assert_eq!(
            transitions.contains(&lifecycle),
            is_mid_transition(lifecycle),
            "lifecycle {lifecycle:?} mid-transition scan membership mismatch"
        );
    }
}

async fn insert_public_stop_operation(
    pool: &PgPool,
    sandbox: Uuid,
    kind: OperationKind,
    state: OperationState,
    requested_at: time::OffsetDateTime,
    completed_at: Option<time::OffsetDateTime>,
) -> TestResult {
    let target_id = PublicSandboxId::from(SandboxId::from_uuid(sandbox)).to_string();
    sqlx::query(sql!(
        "INSERT INTO public_operations (
             operation_id, org_id, actor_user_id, kind, target_type, target_id,
             state, requested_at, completed_at
         )
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)"
    ))
    .bind(Uuid::now_v7())
    .bind(SEED_ORG_ID)
    .bind(SEED_USER_ID)
    .bind(kind.as_str())
    .bind(OperationTargetType::Sandbox.as_str())
    .bind(target_id)
    .bind(state.as_str())
    .bind(requested_at)
    .bind(completed_at)
    .execute(pool)
    .await?;
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn external_stop_or_kill_on_already_stopped_sandbox_supersedes_campaign_stop() -> TestResult {
    let Some(db) = test_db().await? else {
        return Ok(());
    };
    seed_policy(&db.pool, RuntimeEnvironmentState::Deprecated, true).await?;
    let campaign = insert_campaign(
        &db.pool,
        CampaignSeed {
            per_node: 8,
            per_cluster: 8,
            ..force_restart_seed()
        },
    )
    .await?;
    let node = node(&db.pool).await?;

    // Case 1: `inspect_subject` on retry sees a user `:stop` completed after
    // `stopped_at` even though `sandboxes.lifecycle_changed_at == stopped_at`.
    let stopped_between_leases =
        resident_on_source(&db.pool, SandboxLifecycle::Started, node).await?;
    // Case 2: user `:kill` succeeds while `dispatcher.stop` is returning, before
    // `start_owned_stop` calls `dispatcher.start`.
    let killed_during_stop = resident_on_source(&db.pool, SandboxLifecycle::Started, node).await?;
    // Case 3: user `:stop` is queued right before `dispatcher.start` claims the
    // stopped row (`TargetStart::HeldStopped` -> `Release` preserving `stop`),
    // then either succeeds (`Skipped(EXTERNAL_STOP_SUPERSEDED)`) or fails
    // (`Done` via cold-boot onto `target()`).
    let stopped_inside_start =
        resident_on_source(&db.pool, SandboxLifecycle::Started, node).await?;
    let failed_stop_inside_start =
        resident_on_source(&db.pool, SandboxLifecycle::Started, node).await?;
    // Case 4: user `:restore` is queued while the sandbox is still `'stopped'`
    // at `stopped_at` (`stop_still_owned` -> `Release` preserving `stop`), then
    // fails so the campaign cold-boots it onto `target()`.
    let queued_restore_on_stopped =
        resident_on_source(&db.pool, SandboxLifecycle::Started, node).await?;
    // Control: a historical `:stop` completed strictly before `stopped_at` does
    // not forfeit the campaign's later stop ownership.
    let historical_stop = resident_on_source(&db.pool, SandboxLifecycle::Started, node).await?;
    let before_campaign: time::OffsetDateTime =
        sqlx::query_scalar(sql!("SELECT NOW() - INTERVAL '1 hour'"))
            .fetch_one(&db.pool)
            .await?;
    insert_public_stop_operation(
        &db.pool,
        historical_stop,
        OperationKind::StopSandbox,
        OperationState::Succeeded,
        before_campaign,
        Some(before_campaign),
    )
    .await?;

    let dispatcher = FakeDispatcher::default().persisting(db.pool.clone());
    for id in [stopped_between_leases, queued_restore_on_stopped] {
        dispatcher.script_start(
            id,
            vec![Err(DispatchError::Retryable("slot busy".to_owned()))],
        );
    }
    let memory = FakeMemoryRetirement::retired(source());
    let driver = SandboxForceRestartDriver::new(db.pool.clone(), K8S, dispatcher.clone(), memory);

    record(
        &db.pool,
        campaign,
        &[
            stopped_between_leases,
            killed_during_stop,
            stopped_inside_start,
            failed_stop_inside_start,
            queued_restore_on_stopped,
            historical_stop,
        ],
    )
    .await?;
    let campaign_row = store::list_unfinished(&db.pool)
        .await?
        .into_iter()
        .find(|listed| listed.id == campaign)
        .expect("campaign unfinished");

    // Attempt 1 on `stopped_between_leases` and `queued_restore_on_stopped`:
    // campaign stops each, `start` retries.
    let leases_1 = store::lease_items(&db.pool, campaign, Duration::from_secs(30), 16).await?;
    for id in [stopped_between_leases, queued_restore_on_stopped] {
        let retry_lease = leases_1
            .iter()
            .find(|item| item.subject_id == id)
            .expect("item leased");
        let retry_out = driver.upgrade(&campaign_row, retry_lease).await;
        assert_eq!(retry_out, ItemOutcome::Retry(ItemError::new("slot busy")));
        store::finish_item(&db.pool, retry_lease, &retry_out).await?;
    }

    // User stops `stopped_between_leases` and queues `:restore` on
    // `queued_restore_on_stopped` while `sandboxes.lifecycle_changed_at` stays
    // equal to `stopped_at`.
    let stopped_row_before = store::subject_sandbox(&db.pool, stopped_between_leases)
        .await?
        .expect("sandbox exists");
    insert_public_stop_operation(
        &db.pool,
        stopped_between_leases,
        OperationKind::StopSandbox,
        OperationState::Succeeded,
        stopped_row_before.lifecycle_changed_at,
        Some(stopped_row_before.lifecycle_changed_at + time::Duration::milliseconds(10)),
    )
    .await?;
    let restore_row_before = store::subject_sandbox(&db.pool, queued_restore_on_stopped)
        .await?
        .expect("sandbox exists");
    insert_public_stop_operation(
        &db.pool,
        queued_restore_on_stopped,
        OperationKind::RestoreSandbox,
        OperationState::Queued,
        restore_row_before.lifecycle_changed_at,
        None,
    )
    .await?;

    // Release the unused leases from batch 1 so we can run all six in batch 2.
    for other in leases_1.iter().filter(|item| {
        item.subject_id != stopped_between_leases && item.subject_id != queued_restore_on_stopped
    }) {
        store::finish_item(
            &db.pool,
            other,
            &ItemOutcome::Release(ItemError::new("defer")),
        )
        .await?;
    }

    // Hook Case 2: insert a `succeeded` `kill_sandbox` operation while
    // `dispatcher.stop` is in flight (`initiated_at <= completed_at < stopped_at`).
    dispatcher
        .state()
        .external_kill_during_in_flight_stop
        .insert(killed_during_stop);

    // Hook Case 3: insert a `queued` `stop_sandbox` operation inside `start`
    // right before `FakeDispatcher::start` claims `stopped_inside_start` (even
    // when scripted `AlreadyOnTarget`) and `failed_stop_inside_start`.
    dispatcher.script_start(stopped_inside_start, vec![Ok(TargetStart::AlreadyOnTarget)]);
    for id in [stopped_inside_start, failed_stop_inside_start] {
        dispatcher.state().external_stop_before_start.insert(id);
    }

    let leases_2 = store::lease_items(&db.pool, campaign, Duration::from_secs(30), 16).await?;
    let mut expected_leases_2 = vec![
        stopped_between_leases,
        killed_during_stop,
        stopped_inside_start,
        failed_stop_inside_start,
        queued_restore_on_stopped,
        historical_stop,
    ];
    expected_leases_2.sort();
    let mut actual_leases_2: Vec<Uuid> = leases_2.iter().map(|item| item.subject_id).collect();
    actual_leases_2.sort();
    assert_eq!(actual_leases_2, expected_leases_2);
    for leased in &leases_2 {
        let out = driver.upgrade(&campaign_row, leased).await;
        if leased.subject_id == historical_stop {
            assert_eq!(out, ItemOutcome::Done, "historical stop doesn't forfeit");
        } else if matches!(
            leased.subject_id,
            id if id == stopped_inside_start
                || id == failed_stop_inside_start
                || id == queued_restore_on_stopped
        ) {
            assert_eq!(
                out,
                ItemOutcome::Release(ItemError::new(EXTERNAL_OPERATION_IN_FLIGHT)),
                "in-flight external stop releases while preserving campaign stop ownership"
            );
            assert!(
                dispatcher
                    .recorded_stopped_at(campaign, leased.subject_id)
                    .is_some(),
                "in-flight external stop must keep recorded stop ownership until settled"
            );
        } else {
            assert_eq!(
                out,
                ItemOutcome::Skipped(ItemError::new(EXTERNAL_STOP_SUPERSEDED)),
                "sandbox {} expected Skipped after external stop/kill",
                leased.subject_id
            );
            assert_eq!(
                dispatcher.recorded_stopped_at(campaign, leased.subject_id),
                None,
                "superseded campaign stop clears recorded marker"
            );
            assert_eq!(
                store::subject_sandbox(&db.pool, leased.subject_id)
                    .await?
                    .expect("sandbox exists")
                    .lifecycle,
                "stopped",
                "externally stopped sandbox remains stopped"
            );
        }
        store::finish_item(&db.pool, leased, &out).await?;
    }
    assert_eq!(
        dispatcher
            .started()
            .into_iter()
            .filter(|&id| id == queued_restore_on_stopped)
            .count(),
        1,
        "stop_still_owned must fence a queued activation before calling dispatcher.start"
    );

    // Settle the two queued stops: `stopped_inside_start` succeeds (superseding
    // the campaign stop), while `failed_stop_inside_start` fails (preserving
    // stop ownership so the campaign cold-boots the sandbox onto `target()`).
    for (sbx, final_state) in [
        (stopped_inside_start, OperationState::Succeeded),
        (failed_stop_inside_start, OperationState::Failed),
        (queued_restore_on_stopped, OperationState::Failed),
    ] {
        sqlx::query(sql!(
            "UPDATE public_operations
             SET state = $2, completed_at = NOW()
             WHERE target_id = $1"
        ))
        .bind(PublicSandboxId::from(SandboxId::from_uuid(sbx)).to_string())
        .bind(final_state.as_str())
        .execute(&db.pool)
        .await?;
    }
    let leases_3 = store::lease_items(&db.pool, campaign, Duration::from_secs(30), 16).await?;
    assert_eq!(leases_3.len(), 3);
    for leased in &leases_3 {
        let out = driver.upgrade(&campaign_row, leased).await;
        if leased.subject_id == stopped_inside_start {
            assert_eq!(
                out,
                ItemOutcome::Skipped(ItemError::new(EXTERNAL_STOP_SUPERSEDED))
            );
            assert_eq!(
                dispatcher.recorded_stopped_at(campaign, stopped_inside_start),
                None
            );
            assert_eq!(
                store::subject_sandbox(&db.pool, stopped_inside_start)
                    .await?
                    .expect("sandbox exists")
                    .lifecycle,
                "stopped"
            );
        } else {
            assert_eq!(out, ItemOutcome::Done);
            assert_eq!(
                store::subject_sandbox(&db.pool, leased.subject_id)
                    .await?
                    .expect("sandbox exists")
                    .lifecycle,
                "started"
            );
        }
        store::finish_item(&db.pool, leased, &out).await?;
    }
    assert!(
        !dispatcher
            .calls()
            .contains(&Call::Start(killed_during_stop)),
        "start_owned_stop skips before calling dispatcher.start when killed during stop"
    );
    let kill_completed_at: time::OffsetDateTime = sqlx::query_scalar(sql!(
        "SELECT completed_at FROM public_operations WHERE target_id = $1"
    ))
    .bind(PublicSandboxId::from(SandboxId::from_uuid(killed_during_stop)).to_string())
    .fetch_one(&db.pool)
    .await?;
    let killed_stopped_at = store::subject_sandbox(&db.pool, killed_during_stop)
        .await?
        .expect("sandbox exists")
        .lifecycle_changed_at;
    assert!(
        kill_completed_at < killed_stopped_at,
        "in-flight external kill completed ({kill_completed_at}) strictly before stopped_at settled ({killed_stopped_at})"
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn mid_transition_residents_hold_completion_open_until_settled() -> TestResult {
    let Some(db) = test_db().await? else {
        return Ok(());
    };
    seed_policy(&db.pool, RuntimeEnvironmentState::Deprecated, true).await?;
    let campaign = insert_campaign(
        &db.pool,
        CampaignSeed {
            per_node: 8,
            per_cluster: 8,
            ..force_restart_seed()
        },
    )
    .await?;
    let node = node(&db.pool).await?;

    let checkpointing = resident_on_source(&db.pool, SandboxLifecycle::Checkpointing, node).await?;
    let pausing = resident_on_source(&db.pool, SandboxLifecycle::Pausing, node).await?;
    let stopping_to_stopped =
        resident_on_source(&db.pool, SandboxLifecycle::Stopping, node).await?;
    // Simulate a `Suspended` sandbox whose `sandbox_observed_states` row was
    // pruned by `observed_projection_gc`, now mid-`restoring` back onto `source()`,
    // plus one restoring onto `target()` without a published projection row yet.
    let restoring_onto_source = insert_sandbox(
        &db.pool,
        K8S,
        SandboxLifecycle::Restoring,
        node,
        Published::Nothing,
    )
    .await?;
    let restoring_onto_target = insert_sandbox(
        &db.pool,
        K8S,
        SandboxLifecycle::Restoring,
        node,
        Published::Nothing,
    )
    .await?;
    sqlx::query(sql!(
        "DELETE FROM sandbox_observed_states WHERE sandbox_id = ANY($1)"
    ))
    .bind(&[restoring_onto_source, restoring_onto_target][..])
    .execute(&db.pool)
    .await?;

    let dispatcher = FakeDispatcher::default().persisting(db.pool.clone());
    let memory = FakeMemoryRetirement::retired(source());
    let reconciler = force_restart_reconciler(&db.pool, dispatcher.clone(), memory);

    // Tick 1: all five mid-transition sandboxes (including the two rowless
    // `restoring` sandboxes) are scanned into the campaign and held with `Release`
    // (without burning attempts), keeping the rolling campaign open rather than
    // completing prematurely.
    tick(&reconciler).await;
    assert_eq!(campaign_phase(&db.pool, campaign).await?, "rolling");
    let first_counts = counts(&db.pool, campaign).await?;
    assert_eq!(first_counts.pending, 5);
    assert_eq!(first_counts.done, 0);
    assert!(dispatcher.calls().is_empty());

    // Settle `checkpointing -> started`, `pausing -> paused`,
    // `stopping_to_stopped -> stopped`, and publish the two restored sandboxes
    // onto `source()` and `target()` respectively, then tick to completion.
    set_lifecycle(&db.pool, checkpointing, SandboxLifecycle::Started.as_str()).await?;
    set_lifecycle(&db.pool, pausing, SandboxLifecycle::Paused.as_str()).await?;
    set_lifecycle(
        &db.pool,
        stopping_to_stopped,
        SandboxLifecycle::Stopped.as_str(),
    )
    .await?;
    for (restored, env) in [
        (restoring_onto_source, source()),
        (restoring_onto_target, target()),
    ] {
        sqlx::query(sql!(
            "INSERT INTO sandbox_observed_states (
                 sandbox_id, node_id, runtime, state_format_name, execution_metadata
             )
             VALUES ($1, $2, 'firecracker', 'firecracker-snapshot', $3)"
        ))
        .bind(restored)
        .bind(node)
        .bind(serde_json::json!({ "environment": env.to_string() }))
        .execute(&db.pool)
        .await?;
        set_lifecycle(&db.pool, restored, SandboxLifecycle::Started.as_str()).await?;
    }

    tick_until_finished(&reconciler, &db.pool, campaign, 5).await?;
    assert_eq!(campaign_phase(&db.pool, campaign).await?, "completed");
    assert_eq!(
        item(&db.pool, campaign, restoring_onto_source).await?.0,
        "done"
    );
    assert_eq!(
        item(&db.pool, campaign, restoring_onto_target).await?.0,
        "skipped"
    );
    let final_counts = counts(&db.pool, campaign).await?;
    assert_eq!(final_counts.done, 3);
    assert_eq!(final_counts.skipped, 2);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn external_wake_onto_target_after_stop_does_not_certify_canary_done() -> TestResult {
    let Some(db) = test_db().await? else {
        return Ok(());
    };
    seed_policy(&db.pool, RuntimeEnvironmentState::Deprecated, true).await?;
    let campaign = insert_campaign(
        &db.pool,
        CampaignSeed {
            phase: CampaignPhase::Canary,
            failure_budget: 1,
            ..force_restart_seed()
        },
    )
    .await?;
    let node = node(&db.pool).await?;
    let canary_sandbox = resident_on_source(&db.pool, SandboxLifecycle::Started, node).await?;
    let waiting_sandbox = resident_on_source(&db.pool, SandboxLifecycle::Started, node).await?;

    let dispatcher = FakeDispatcher::default().persisting(db.pool.clone());
    dispatcher.script_start(
        canary_sandbox,
        vec![Err(DispatchError::Retryable("start rpc failed".to_owned()))],
    );
    let memory = FakeMemoryRetirement::retired(source());
    let reconciler = force_restart_reconciler(&db.pool, dispatcher.clone(), memory);

    // Tick 1 leases the single canary item (`canary_sandbox`), stops it, and
    // gets `Retry` from `dispatcher.start` without `StartedOnTarget`.
    tick(&reconciler).await;
    assert_eq!(campaign_phase(&db.pool, campaign).await?, "canary");
    assert!(
        dispatcher
            .recorded_stopped_at(campaign, canary_sandbox)
            .is_some()
    );

    // Before the retry runs, an external wake (such as ingress wake or user
    // `:start`) boots `canary_sandbox` onto `target()` on its own.
    set_lifecycle(&db.pool, canary_sandbox, SandboxLifecycle::Started.as_str()).await?;
    set_bound(&db.pool, canary_sandbox, target()).await?;

    // Tick 2 re-drives `canary_sandbox`: because the campaign's `dispatcher.start`
    // never settled `StartedOnTarget`, `canary_sandbox` must resolve as `Skipped`
    // (clearing the recorded stop) and lease the next canary (`waiting_sandbox`),
    // rather than counting an unverified external wake as `Done`.
    tick(&reconciler).await;
    assert_eq!(item(&db.pool, campaign, canary_sandbox).await?.0, "skipped");
    assert_eq!(
        dispatcher.recorded_stopped_at(campaign, canary_sandbox),
        None
    );

    // `waiting_sandbox` is now the canary that actually exercises `dispatcher.start`
    // and certifies `Done`.
    tick(&reconciler).await;
    assert_eq!(item(&db.pool, campaign, waiting_sandbox).await?.0, "done");
    assert!(matches!(
        dispatcher.recorded_stop(campaign, waiting_sandbox),
        Some(RecordedStop::StartedOnTarget(_))
    ));
    let final_counts = counts(&db.pool, campaign).await?;
    assert_eq!(final_counts.done, 1);
    assert_eq!(final_counts.skipped, 1);

    // Promote to `rolling`, then verify two `StartedOnTarget` edge cases:
    // 1. A reopened item (`!continuing`) clears a stale `StartedOnTarget` marker
    //    rather than crediting `Done` without running this attempt's upgrade.
    // 2. A continuing run whose own `dispatcher.start` recorded `StartedOnTarget`
    //    still resolves as `Done` on redrive even if the sandbox was deleted
    //    before `finish_item` ran.
    store::resume_campaign(
        &db_handle(&db.pool),
        campaign,
        &store::PhaseChange::default(),
        false,
    )
    .await?;
    dispatcher.state().item_stopped_at.insert(
        (campaign, canary_sandbox),
        RecordedStop::StartedOnTarget(StopFence::at(jiff::Timestamp::UNIX_EPOCH)),
    );
    record(&db.pool, campaign, &[canary_sandbox]).await?;
    tick(&reconciler).await;
    assert_eq!(
        item(&db.pool, campaign, canary_sandbox).await?.0,
        "skipped",
        "reopened item clears stale StartedOnTarget before inspecting"
    );
    assert_eq!(dispatcher.recorded_stop(campaign, canary_sandbox), None);

    let deleted_after_start = resident_on_source(&db.pool, SandboxLifecycle::Started, node).await?;
    record(&db.pool, campaign, &[deleted_after_start]).await?;
    let campaign_row = store::list_unfinished(&db.pool)
        .await?
        .into_iter()
        .find(|listed| listed.id == campaign)
        .expect("campaign unfinished");
    let driver = SandboxForceRestartDriver::new(
        db.pool.clone(),
        K8S,
        dispatcher.clone(),
        FakeMemoryRetirement::retired(source()),
    );
    let lost_lease = &store::lease_items(&db.pool, campaign, Duration::from_secs(30), 16).await?[0];
    assert_eq!(
        driver.upgrade(&campaign_row, lost_lease).await,
        ItemOutcome::Done
    );
    sqlx::query(sql!("DELETE FROM sandboxes WHERE id = $1"))
        .bind(deleted_after_start)
        .execute(&db.pool)
        .await?;
    sqlx::query(sql!(
        "UPDATE runtime_upgrade_items
         SET lease_expires_at = NOW() - INTERVAL '1 second'
         WHERE campaign_id = $1 AND subject_id = $2"
    ))
    .bind(campaign)
    .bind(deleted_after_start)
    .execute(&db.pool)
    .await?;
    tick(&reconciler).await;
    assert_eq!(
        item(&db.pool, campaign, deleted_after_start).await?.0,
        "done",
        "continuing redrive preserves Done from StartedOnTarget even if deleted afterward"
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn expired_external_stop_is_retained_until_held_or_paused_campaign_relinquishes_stop()
-> TestResult {
    let Some(db) = test_db().await? else {
        return Ok(());
    };
    let node = node(&db.pool).await?;
    seed_policy(&db.pool, RuntimeEnvironmentState::Deprecated, true).await?;

    let held_stopped = resident_on_source(&db.pool, SandboxLifecycle::Started, node).await?;
    let campaign = insert_campaign(
        &db.pool,
        CampaignSeed {
            per_node: 4,
            per_cluster: 4,
            ..force_restart_seed()
        },
    )
    .await?;
    let dispatcher = FakeDispatcher::default().persisting(db.pool.clone());
    dispatcher.script_start(
        held_stopped,
        vec![Ok(TargetStart::HeldStopped("node pool holding".to_owned()))],
    );
    let memory = FakeMemoryRetirement::retired(source());
    let reconciler = force_restart_reconciler(&db.pool, dispatcher.clone(), memory.clone());

    // Tick 1 stops `held_stopped`, then `TargetStart::HeldStopped` releases the
    // item back to `pending` while keeping campaign stop ownership (`StopFence`).
    tick(&reconciler).await;
    assert_eq!(item(&db.pool, campaign, held_stopped).await?.0, "pending");
    let fence = dispatcher
        .recorded_stop(campaign, held_stopped)
        .and_then(RecordedStop::into_fence)
        .expect("campaign holds stop ownership");

    // Advance `stopped_at` by 50ms on the database clock and record a user
    // `StopSandbox` receipt at `during_stop` (`c.created_at <= during_stop <
    // stopped_at` and `initiated_at <= during_stop`), then pause + expire
    // `gc_after`.
    let (during_stop, settled_later): (time::OffsetDateTime, time::OffsetDateTime) =
        sqlx::query_as(sql!(
            "UPDATE sandboxes
             SET lifecycle_changed_at = NOW() + INTERVAL '50 milliseconds'
             WHERE id = $1
             RETURNING NOW(), lifecycle_changed_at"
        ))
        .bind(held_stopped)
        .fetch_one(&db.pool)
        .await?;
    let during_stop_check_init = fence.initiated_at;
    dispatcher.state().item_stopped_at.insert(
        (campaign, held_stopped),
        RecordedStop::Stopped(StopFence {
            stopped_at: to_jiff(settled_later),
            settled_stopped_at: to_jiff(settled_later),
            ..fence
        }),
    );

    store::pause_campaign(
        &db_handle(&db.pool),
        campaign,
        &store::PhaseChange::default(),
    )
    .await?;
    memory.unretire(source());
    assert!(to_offset_date_time(during_stop_check_init) <= during_stop);
    assert!(during_stop < settled_later);
    insert_public_stop_operation(
        &db.pool,
        held_stopped,
        OperationKind::StopSandbox,
        OperationState::Succeeded,
        during_stop,
        Some(during_stop),
    )
    .await?;
    sqlx::query(sql!(
        "UPDATE public_operations
         SET gc_after = NOW() - INTERVAL '1 hour'"
    ))
    .execute(&db.pool)
    .await?;

    // Running public-operation retention while the campaign can still resume its
    // stop must NOT collect the user's superseding `StopSandbox` receipt.
    let retention = crate::public_operation_retention::Reconciler::new(
        crate::public_operation_retention::ReconcilerConfig::default(),
        pplx_space_api::public_operations::PublicOperationStore::new(db.pool.clone()),
    );
    let held_gc = retention.tick().await?;
    assert_eq!(
        held_gc.operations_deleted, 0,
        "external stop receipt must survive GC while campaign item still holds stop ownership"
    );

    // Once memory is re-retired and the campaign resumes, the driver still sees
    // the user's stop, clears `stopped_at`, and skips the item without booting it.
    memory.retire(source());
    store::resume_campaign(
        &db_handle(&db.pool),
        campaign,
        &store::PhaseChange::default(),
        false,
    )
    .await?;
    tick(&reconciler).await;
    assert_eq!(item(&db.pool, campaign, held_stopped).await?.0, "skipped");
    assert_eq!(dispatcher.recorded_stopped_at(campaign, held_stopped), None);
    assert_eq!(
        dispatcher
            .calls()
            .into_iter()
            .filter(|call| *call == Call::Start(held_stopped))
            .count(),
        1
    );
    let current_lifecycle = store::subject_sandbox(&db.pool, held_stopped)
        .await?
        .expect("sandbox exists")
        .lifecycle;
    assert_eq!(current_lifecycle, SandboxLifecycle::Stopped.as_str());

    // After the item relinquishes stop ownership (`skipped`), the next retention
    // tick collects the expired operation row cleanly.
    let released_gc = retention.tick().await?;
    assert_eq!(
        released_gc.operations_deleted, 1,
        "once the campaign item is skipped, expired stop operation is collected"
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn legacy_unbound_toggle_preserves_skipped_items_across_passes_and_upgrades_on_return()
-> TestResult {
    let Some(db) = test_db().await? else {
        return Ok(());
    };
    let node = node(&db.pool).await?;
    seed_policy(&db.pool, RuntimeEnvironmentState::Blocked, true).await?;
    let mut tx = db.pool.begin().await?;
    pplx_space_runtime_environment_policy_store::set_legacy_unbound_allowed(
        &mut *tx,
        false,
        &change(),
    )
    .await?;
    tx.commit().await?;

    let campaign = insert_campaign(
        &db.pool,
        CampaignSeed {
            per_node: 8,
            per_cluster: 8,
            ..force_restart_seed()
        },
    )
    .await?;

    let running_unbound = insert_sandbox(
        &db.pool,
        K8S,
        SandboxLifecycle::Started,
        node,
        Published::PreMetadata { kernel: "6.1.155" },
    )
    .await?;
    let stopped_unbound = insert_sandbox(
        &db.pool,
        K8S,
        SandboxLifecycle::Started,
        node,
        Published::PreMetadata { kernel: "6.1.155" },
    )
    .await?;

    let dispatcher = FakeDispatcher::default().persisting(db.pool.clone());
    // `stopped_unbound` stops cleanly on attempt 1, then retries `start` so it
    // holds campaign stop ownership when `legacy_unbound_allowed` flips to `true`.
    dispatcher.script_start(
        stopped_unbound,
        vec![Err(DispatchError::Retryable("slot busy".to_owned()))],
    );
    // `running_unbound`'s first `stop` races with `legacy_unbound_allowed`
    // flipping to `true` (declined by the guarded stop) and lands in `skipped`
    // once `legacy_unbound_allowed = true` is committed.
    dispatcher.state().stop_script.insert(
        running_unbound,
        GuardedStop::Declined("the sandbox is no longer on the campaign's source".to_owned()),
    );

    let memory = FakeMemoryRetirement::retired(source());
    let reconciler = force_restart_reconciler(&db.pool, dispatcher.clone(), memory);

    // Record both items while `legacy_unbound_allowed == false`, then flip
    // `legacy_unbound_allowed = true` right as the first tick runs.
    record(&db.pool, campaign, &[running_unbound, stopped_unbound]).await?;
    let campaign_row = store::list_unfinished(&db.pool)
        .await?
        .into_iter()
        .find(|listed| listed.id == campaign)
        .expect("campaign unfinished");
    let driver = SandboxForceRestartDriver::new(
        db.pool.clone(),
        K8S,
        dispatcher.clone(),
        FakeMemoryRetirement::retired(source()),
    );
    let leases = store::lease_items(&db.pool, campaign, Duration::from_secs(30), 16).await?;
    let stopped_lease = leases
        .iter()
        .find(|item| item.subject_id == stopped_unbound)
        .expect("stopped_unbound leased");
    let stop_retry = driver.upgrade(&campaign_row, stopped_lease).await;
    assert_eq!(stop_retry, ItemOutcome::Retry(ItemError::new("slot busy")));
    store::finish_item(&db.pool, stopped_lease, &stop_retry).await?;

    let mut tx = db.pool.begin().await?;
    pplx_space_runtime_environment_policy_store::set_legacy_unbound_allowed(
        &mut *tx,
        true,
        &change(),
    )
    .await?;
    tx.commit().await?;

    let running_lease = leases
        .iter()
        .find(|item| item.subject_id == running_unbound)
        .expect("running_unbound leased");
    let declined_skip = driver.upgrade(&campaign_row, running_lease).await;
    assert!(matches!(declined_skip, ItemOutcome::Skipped(_)));
    store::finish_item(&db.pool, running_lease, &declined_skip).await?;

    // Run two full `reconciler.tick()` scan passes while `legacy_unbound_allowed`
    // stays `true`: `page_force_restart_candidates` must NOT place
    // `running_unbound` in `SubjectPage::elsewhere`, so `retire_skipped` leaves
    // its remaining attempts intact while `stopped_unbound` holds in `pending`.
    tick(&reconciler).await;
    tick(&reconciler).await;
    assert_eq!(campaign_phase(&db.pool, campaign).await?, "rolling");
    let (running_phase, _, running_attempts) = item(&db.pool, campaign, running_unbound).await?;
    assert_eq!(running_phase, "skipped");
    assert_eq!(
        running_attempts, 1,
        "retire_skipped must not retire pre-metadata unbound item while legacy_unbound_allowed=true"
    );
    assert_eq!(
        item(&db.pool, campaign, stopped_unbound).await?.0,
        "pending"
    );

    // Flip `legacy_unbound_allowed` back to `false`: the restarted scan pass
    // reopens `running_unbound` (`skipped -> pending`) and upgrades both items
    // onto `target()` (`Done`), completing the campaign.
    let mut tx = db.pool.begin().await?;
    pplx_space_runtime_environment_policy_store::set_legacy_unbound_allowed(
        &mut *tx,
        false,
        &change(),
    )
    .await?;
    tx.commit().await?;

    tick_until_finished(&reconciler, &db.pool, campaign, 5).await?;
    assert_eq!(campaign_phase(&db.pool, campaign).await?, "completed");
    assert_eq!(item(&db.pool, campaign, running_unbound).await?.0, "done");
    assert_eq!(item(&db.pool, campaign, stopped_unbound).await?.0, "done");
    assert_eq!(counts(&db.pool, campaign).await?.done, 2);

    // Control: when `legacy_unbound_allowed` stays `true` permanently after a
    // pre-metadata unbound item lands in `skipped` (with no campaign-stopped
    // items left), the campaign still completes cleanly without looping.
    let permanent_campaign = insert_campaign(
        &db.pool,
        CampaignSeed {
            per_node: 8,
            per_cluster: 8,
            ..force_restart_seed()
        },
    )
    .await?;
    let permanent_unbound = insert_sandbox(
        &db.pool,
        K8S,
        SandboxLifecycle::Started,
        node,
        Published::PreMetadata { kernel: "6.1.155" },
    )
    .await?;
    record(&db.pool, permanent_campaign, &[permanent_unbound]).await?;
    let mut tx = db.pool.begin().await?;
    pplx_space_runtime_environment_policy_store::set_legacy_unbound_allowed(
        &mut *tx,
        true,
        &change(),
    )
    .await?;
    tx.commit().await?;
    tick_until_finished(&reconciler, &db.pool, permanent_campaign, 5).await?;
    assert_eq!(
        campaign_phase(&db.pool, permanent_campaign).await?,
        "completed"
    );
    assert_eq!(
        item(&db.pool, permanent_campaign, permanent_unbound)
            .await?
            .0,
        "skipped"
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn unpublished_started_and_paused_sandboxes_hold_completion_until_node_publishes_execution()
-> TestResult {
    let Some(db) = test_db().await? else {
        return Ok(());
    };
    seed_policy(&db.pool, RuntimeEnvironmentState::Deprecated, true).await?;
    let campaign = insert_campaign(
        &db.pool,
        CampaignSeed {
            per_node: 8,
            per_cluster: 8,
            ..force_restart_seed()
        },
    )
    .await?;
    let node = node(&db.pool).await?;

    // `rowless_started` has no `sandbox_observed_states` row yet; `status_only_paused`
    // has a status-only row (`Published::Nothing`). Both must hold campaign
    // completion open (`Release(UNPUBLISHED_EXECUTION)`, `attempts == 0`) until
    // node publication lands.
    let rowless_started = insert_sandbox(
        &db.pool,
        K8S,
        SandboxLifecycle::Started,
        node,
        Published::Nothing,
    )
    .await?;
    sqlx::query(sql!(
        "DELETE FROM sandbox_observed_states WHERE sandbox_id = $1"
    ))
    .bind(rowless_started)
    .execute(&db.pool)
    .await?;
    let status_only_paused = insert_sandbox(
        &db.pool,
        K8S,
        SandboxLifecycle::Paused,
        node,
        Published::Nothing,
    )
    .await?;

    let dispatcher = FakeDispatcher::with_nodes(HashMap::from([
        (rowless_started, node),
        (status_only_paused, node),
    ]))
    .persisting(db.pool.clone());
    let memory = FakeMemoryRetirement::retired(source());
    let reconciler = force_restart_reconciler(&db.pool, dispatcher.clone(), memory);

    for _ in 0..3 {
        tick(&reconciler).await;
    }
    assert_eq!(
        campaign_phase(&db.pool, campaign).await?,
        "rolling",
        "unpublished active sandboxes must hold completion open"
    );
    for id in [rowless_started, status_only_paused] {
        let (phase, last_error, attempts) = item(&db.pool, campaign, id).await?;
        assert_eq!(phase, "pending");
        assert_eq!(attempts, 0);
        assert_eq!(
            last_error.as_deref(),
            Some(format!("before fresh run started: {UNPUBLISHED_EXECUTION}")).as_deref()
        );
    }
    assert!(dispatcher.calls().is_empty());

    // Now the node publishes `rowless_started` on `source()` and
    // `status_only_paused` on `target()`: the campaign restarts `rowless_started`,
    // skips `status_only_paused`, and completes.
    sqlx::query(sql!(
        "INSERT INTO sandbox_observed_states (
             sandbox_id, node_id, runtime, state_format_name, execution_metadata
         )
         VALUES ($1, $2, 'firecracker', 'firecracker-snapshot', $3)"
    ))
    .bind(rowless_started)
    .bind(node)
    .bind(serde_json::json!({ "environment": source().to_string() }))
    .execute(&db.pool)
    .await?;
    set_bound(&db.pool, status_only_paused, target()).await?;

    tick_until_finished(&reconciler, &db.pool, campaign, 5).await?;
    assert_eq!(campaign_phase(&db.pool, campaign).await?, "completed");
    assert_eq!(item(&db.pool, campaign, rowless_started).await?.0, "done");
    assert_eq!(
        item(&db.pool, campaign, status_only_paused).await?.0,
        "skipped"
    );
    assert_eq!(dispatcher.started(), vec![rowless_started]);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn unstarted_stop_declines_already_stopped_node_and_pre_stop_external_stop_supersedes()
-> TestResult {
    let Some(db) = test_db().await? else {
        return Ok(());
    };
    seed_policy(&db.pool, RuntimeEnvironmentState::Deprecated, true).await?;
    let node = node(&db.pool).await?;

    let powered_off_on_node = resident_on_source(&db.pool, SandboxLifecycle::Started, node).await?;
    let pre_campaign_user_stopped =
        resident_on_source(&db.pool, SandboxLifecycle::Started, node).await?;

    // Backdate `pre_campaign_user_stopped`'s `'started'` timestamp and record a
    // succeeded user `sandbox.stop` right after it (before the campaign is even
    // created or `stop` runs, while `sandboxes` is still stale `'started'` ahead
    // of node-status reconcile): `StopFence::initiated_at = LEAST(NOW(), pre_stop_lifecycle_changed_at)`
    // covers the entire `'started'` episode so `external_stop_since` catches it.
    let pre_stop_started_at: time::OffsetDateTime = sqlx::query_scalar(sql!(
        "UPDATE sandboxes
         SET lifecycle_changed_at = NOW() - INTERVAL '5 seconds'
         WHERE id = $1
         RETURNING lifecycle_changed_at"
    ))
    .bind(pre_campaign_user_stopped)
    .fetch_one(&db.pool)
    .await?;
    let user_stop_at = pre_stop_started_at + time::Duration::seconds(1);
    let pre_stopped_op_id = Uuid::now_v7();
    let target_id =
        PublicSandboxId::from(SandboxId::from_uuid(pre_campaign_user_stopped)).to_string();
    sqlx::query(sql!(
        "INSERT INTO public_operations (
             operation_id, org_id, actor_user_id, kind, target_type, target_id,
             state, requested_at, completed_at, gc_after
         )
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $8, NOW() - INTERVAL '1 hour')"
    ))
    .bind(pre_stopped_op_id)
    .bind(SEED_ORG_ID)
    .bind(SEED_USER_ID)
    .bind(OperationKind::StopSandbox.as_str())
    .bind(OperationTargetType::Sandbox.as_str())
    .bind(target_id)
    .bind(OperationState::Succeeded.as_str())
    .bind(user_stop_at)
    .execute(&db.pool)
    .await?;

    let campaign = insert_campaign(
        &db.pool,
        CampaignSeed {
            per_node: 8,
            per_cluster: 8,
            ..force_restart_seed()
        },
    )
    .await?;

    // Run `due_for_gc` + `delete_by_ids` BEFORE `pre_campaign_user_stopped` has
    // a `runtime_upgrade_items` row AND while its `sandboxes` row is still stale
    // `'started'`: both pre-scan retention and lifecycle-agnostic stop retention
    // are required to preserve `user_stop_at` for `StopFence::initiated_at`.
    let due_pre_scan =
        pplx_space_public_operation_store::operations::due_for_gc(&db.pool, 100).await?;
    assert!(
        due_pre_scan.is_empty(),
        "pre-scan stale-'started' sandbox must retain its stop receipt"
    );
    pplx_space_public_operation_store::operations::delete_by_ids(&db.pool, &due_pre_scan).await?;

    // Simulate `pre_campaign_user_stopped`'s first lease crashing right as
    // `dispatcher.stop` settled `'stopped'` after `campaign.created_at` (with
    // `StopFence::initiated_at = pre_stop_started_at < user_stop_at < campaign.created_at < stopped_at`),
    // and then operation GC running across a pause before the redriven lease
    // inspects the item.
    record(&db.pool, campaign, &[pre_campaign_user_stopped]).await?;
    let settled_stopped_at: time::OffsetDateTime = sqlx::query_scalar(sql!(
        "UPDATE sandboxes
         SET lifecycle = 'stopped', lifecycle_changed_at = NOW() + INTERVAL '1 second'
         WHERE id = $1
         RETURNING lifecycle_changed_at"
    ))
    .bind(pre_campaign_user_stopped)
    .fetch_one(&db.pool)
    .await?;
    sqlx::query(sql!(
        "UPDATE runtime_upgrade_items
         SET attempts = 1, last_error = 'lease expired'
         WHERE campaign_id = $1 AND subject_id = $2"
    ))
    .bind(campaign)
    .bind(pre_campaign_user_stopped)
    .execute(&db.pool)
    .await?;

    let rolled_back_stopped = resident_on_source(&db.pool, SandboxLifecycle::Started, node).await?;
    let rolled_back_restarted_on_target =
        resident_on_source(&db.pool, SandboxLifecycle::Started, node).await?;
    let dispatcher = FakeDispatcher::with_nodes(HashMap::from([
        (powered_off_on_node, node),
        (pre_campaign_user_stopped, node),
        (rolled_back_stopped, node),
        (rolled_back_restarted_on_target, node),
    ]))
    .persisting(db.pool.clone());
    dispatcher.state().item_stopped_at.insert(
        (campaign, pre_campaign_user_stopped),
        RecordedStop::Stopped(StopFence::new(
            to_jiff(pre_stop_started_at),
            to_jiff(settled_stopped_at),
            to_jiff(settled_stopped_at),
            None,
        )),
    );
    dispatcher
        .state()
        .already_stopped_on_node
        .insert(powered_off_on_node);
    // Record one already-completed canary item so `resume_campaign` resumes back
    // into `rolling`, then pause `campaign` and run `due_for_gc` + `delete_by_ids`
    // BEFORE `pre_campaign_user_stopped` is leased on its continuing run
    // (`settled_stopped_at > campaign.created_at > user_stop_at`).
    let canary_done = Uuid::now_v7();
    record(&db.pool, campaign, &[canary_done]).await?;
    sqlx::query(sql!(
        "UPDATE runtime_upgrade_items
         SET phase = 'done', attempts = 1
         WHERE campaign_id = $1 AND subject_id = $2"
    ))
    .bind(campaign)
    .bind(canary_done)
    .execute(&db.pool)
    .await?;
    let paused = store::pause_campaign(
        &db_handle(&db.pool),
        campaign,
        &store::PhaseChange::default(),
    )
    .await?;
    assert_eq!(paused.phase, CampaignPhase::Paused);
    let due_gap_1b =
        pplx_space_public_operation_store::operations::due_for_gc(&db.pool, 100).await?;
    assert!(
        due_gap_1b.is_empty(),
        "paused campaign must retain pre-campaign latest stop receipt across pause"
    );
    pplx_space_public_operation_store::operations::delete_by_ids(&db.pool, &due_gap_1b).await?;
    let resumed = store::resume_campaign(
        &db_handle(&db.pool),
        campaign,
        &store::PhaseChange::default(),
        false,
    )
    .await?;
    assert_eq!(resumed.phase, CampaignPhase::Rolling);

    for id in [rolled_back_stopped, rolled_back_restarted_on_target] {
        dispatcher.script_start(
            id,
            vec![Err(DispatchError::Retryable("slot busy".to_owned()))],
        );
    }
    let memory = FakeMemoryRetirement::retired(source());
    let reconciler = force_restart_reconciler(&db.pool, dispatcher.clone(), memory);

    let mut expected_retry_starts = vec![rolled_back_stopped, rolled_back_restarted_on_target];
    expected_retry_starts.sort();

    tick(&reconciler).await;
    assert_eq!(dispatcher.started(), expected_retry_starts);

    // Simulate user `:rollback` (`auto_restart: false` and `auto_restart: true`
    // onto `target()`) with expired `gc_after` after `dispatcher.stop` settled
    // `'stopped'`, then run real operation GC before the retry tick.
    let rb_stopped_op_id = Uuid::now_v7();
    let rb_restarted_op_id = Uuid::now_v7();
    for (sbx, op_id, restart_to_target) in [
        (rolled_back_stopped, rb_stopped_op_id, false),
        (rolled_back_restarted_on_target, rb_restarted_op_id, true),
    ] {
        let stop_fence = dispatcher
            .recorded_stop(campaign, sbx)
            .and_then(RecordedStop::into_fence)
            .expect("stop recorded");
        let rb_target_id = PublicSandboxId::from(SandboxId::from_uuid(sbx)).to_string();
        let rb_completed =
            to_offset_date_time(stop_fence.stopped_at) + time::Duration::milliseconds(10);
        sqlx::query(sql!(
            "INSERT INTO public_operations (
                 operation_id, org_id, actor_user_id, kind, target_type, target_id,
                 state, payload_json, requested_at, completed_at, gc_after
             )
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, NOW() - INTERVAL '1 hour')"
        ))
        .bind(op_id)
        .bind(SEED_ORG_ID)
        .bind(SEED_USER_ID)
        .bind(OperationKind::RollbackSandbox.as_str())
        .bind(OperationTargetType::Sandbox.as_str())
        .bind(rb_target_id)
        .bind(OperationState::Succeeded.as_str())
        .bind(sqlx::types::Json(serde_json::json!({
            "rollback": { "auto_restart": restart_to_target }
        })))
        .bind(to_offset_date_time(stop_fence.stopped_at))
        .bind(rb_completed)
        .execute(&db.pool)
        .await?;
        if restart_to_target {
            set_lifecycle(&db.pool, sbx, SandboxLifecycle::Started.as_str()).await?;
            set_bound(&db.pool, sbx, target()).await?;
        }
    }
    // Also verify `due_for_gc` retains `rolled_back_stopped`'s receipt while
    // `rolled_back_stopped` is transiently `'restoring'` (for example, mid-drain
    // stopped-evacuation re-placement) before settling back to `'stopped'`.
    set_lifecycle(
        &db.pool,
        rolled_back_stopped,
        SandboxLifecycle::Restoring.as_str(),
    )
    .await?;
    let due_before_retry =
        pplx_space_public_operation_store::operations::due_for_gc(&db.pool, 100).await?;
    assert_eq!(
        due_before_retry,
        vec![pre_stopped_op_id],
        "skipped stopped item releases its spent receipt immediately while failed rollback items stay retained"
    );
    assert!(!due_before_retry.contains(&rb_stopped_op_id));
    assert!(!due_before_retry.contains(&rb_restarted_op_id));
    pplx_space_public_operation_store::operations::delete_by_ids(&db.pool, &due_before_retry)
        .await?;
    set_lifecycle(
        &db.pool,
        rolled_back_stopped,
        SandboxLifecycle::Stopped.as_str(),
    )
    .await?;

    tick(&reconciler).await;
    assert_eq!(dispatcher.started(), expected_retry_starts);

    // Once node-status reconcile updates `powered_off_on_node` to `'stopped'`,
    // the completion scan sees no remaining source active residents and completes.
    set_lifecycle(
        &db.pool,
        powered_off_on_node,
        SandboxLifecycle::Stopped.as_str(),
    )
    .await?;
    tick_until_finished(&reconciler, &db.pool, campaign, 5).await?;
    assert_eq!(campaign_phase(&db.pool, campaign).await?, "completed");
    assert_eq!(
        item(&db.pool, campaign, powered_off_on_node).await?.0,
        "skipped"
    );
    for sbx in [pre_campaign_user_stopped, rolled_back_stopped] {
        let (phase, err, _) = item(&db.pool, campaign, sbx).await?;
        assert_eq!(phase, "skipped");
        assert_eq!(err.as_deref(), Some(EXTERNAL_STOP_SUPERSEDED));
        assert_eq!(
            store::subject_sandbox(&db.pool, sbx)
                .await?
                .expect("sandbox exists")
                .lifecycle,
            "stopped"
        );
    }
    let (rb_target_phase, rb_target_err, _) =
        item(&db.pool, campaign, rolled_back_restarted_on_target).await?;
    assert_eq!(rb_target_phase, "skipped");
    assert_eq!(rb_target_err.as_deref(), Some(NO_LONGER_ON_SOURCE));
    assert_eq!(
        dispatcher.started(),
        expected_retry_starts,
        "neither pre-stopped nor rolled-back sandboxes may be cold-booted after fencing"
    );
    let due_after_completion =
        pplx_space_public_operation_store::operations::due_for_gc(&db.pool, 100).await?;
    assert_eq!(
        due_after_completion.len(),
        2,
        "completed campaign releases both remaining retained rollback receipts to GC"
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn lapsed_lease_holds_stop_without_teardown_or_failure_budget() -> TestResult {
    let Some(db) = test_db().await? else {
        return Ok(());
    };
    seed_policy(&db.pool, RuntimeEnvironmentState::Blocked, true).await?;
    let node = node(&db.pool).await?;

    let stopped_driver_started =
        resident_on_source(&db.pool, SandboxLifecycle::Started, node).await?;
    let started = resident_on_source(&db.pool, SandboxLifecycle::Started, node).await?;

    let dispatcher = FakeDispatcher::default().persisting(db.pool.clone());
    let stopped_driver = SandboxDriver::new(db.pool.clone(), K8S, dispatcher.clone());
    let force_driver = SandboxForceRestartDriver::new(
        db.pool.clone(),
        K8S,
        dispatcher.clone(),
        FakeMemoryRetirement::retired(source()),
    );

    let stopped_campaign_id = insert_campaign(
        &db.pool,
        CampaignSeed {
            include_running: true,
            ..seed()
        },
    )
    .await?;
    let force_campaign_id = insert_campaign(&db.pool, force_restart_seed()).await?;

    record(&db.pool, stopped_campaign_id, &[stopped_driver_started]).await?;
    record(&db.pool, force_campaign_id, &[started]).await?;

    let stopped_leased =
        store::lease_items(&db.pool, stopped_campaign_id, Duration::from_secs(60), 10)
            .await?
            .pop()
            .expect("stopped item leased");
    store::finish_item(
        &db.pool,
        &stopped_leased,
        &ItemOutcome::Release(ItemError::new("released")),
    )
    .await?;

    let force_leased = store::lease_items(&db.pool, force_campaign_id, Duration::from_secs(60), 10)
        .await?
        .pop()
        .expect("force-restart item leased");
    store::finish_item(
        &db.pool,
        &force_leased,
        &ItemOutcome::Release(ItemError::new("released")),
    )
    .await?;

    let campaigns = store::list_unfinished(&db.pool).await?;
    let stopped_campaign = campaigns
        .iter()
        .find(|c| c.id == stopped_campaign_id)
        .expect("stopped campaign");
    let force_campaign = campaigns
        .iter()
        .find(|c| c.id == force_campaign_id)
        .expect("force campaign");

    assert!(
        matches!(
            stopped_driver
                .upgrade(stopped_campaign, &stopped_leased)
                .await,
            ItemOutcome::Release(_)
        ),
        "SandboxDriver must check the active item lease before stopping a running sandbox"
    );
    assert_eq!(
        store::subject_sandbox(&db.pool, stopped_driver_started)
            .await?
            .expect("sandbox exists")
            .lifecycle,
        SandboxLifecycle::Started.as_str(),
        "stale SandboxDriver worker must not stop a running sandbox after losing its lease"
    );

    assert!(
        matches!(
            force_driver.upgrade(force_campaign, &force_leased).await,
            ItemOutcome::Release(_)
        ),
        "Held stop must release without charging attempts or failure_budget even while policy is valid"
    );
    Ok(())
}
