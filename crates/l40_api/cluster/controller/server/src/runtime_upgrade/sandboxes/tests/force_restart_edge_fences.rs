//! Focused edge-fence coverage for `sandbox_force_restarts` and stopped-sandbox
//! dispatcher/store races: `Paused` recovery after `StartingOnTarget`, expired
//! lease guards on `upgrade_stopped`, `clear_stopped_at`, and
//! `clear_initiated_stop`, unbound third-env catalog reattribution after
//! campaign stop, claim-time policy/retirement reversals with scripted
//! outcomes, per-`(campaign_id, sandbox_id)` stop episode admission, multi-node
//! stranded recovery fairness, and Direct/Kubernetes undispatched start
//! rollback + converged drain fencing.

use std::collections::HashMap;
use std::time::Duration;

use pplx_space_public_api::{OperationKind, OperationState, OperationTargetType, PublicSandboxId};
use pplx_space_sandbox_model::SandboxId;

use super::super::ItemOutcome;
use super::super::NO_LONGER_ON_SOURCE;
use super::super::dispatcher::{
    InitiatedStop, RecordedStop, SandboxUpgradeDispatcher, StopFence, TargetStart,
};
use super::super::force_restarts::POLICY_STOPPED_FORCE_RESTARTING;
use super::*;

const DIRECT: SubstrateKind = SubstrateKind::Space;

async fn insert_public_lifecycle_op(
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
async fn starting_on_target_converged_to_paused_recovers_unless_externally_woken() -> TestResult {
    let Some(db) = test_db().await? else {
        return Ok(());
    };
    seed_policy(&db.pool, RuntimeEnvironmentState::Deprecated, true).await?;
    let campaign = insert_campaign(
        &db.pool,
        CampaignSeed {
            per_node: 4,
            per_cluster: 4,
            ..force_restart_seed()
        },
    )
    .await?;
    let node = node(&db.pool).await?;
    let campaign_boot_then_paused =
        resident_on_source(&db.pool, SandboxLifecycle::Started, node).await?;
    let external_wake_then_paused =
        resident_on_source(&db.pool, SandboxLifecycle::Started, node).await?;

    let retirement = FakeMemoryRetirement::retired(source());
    let dispatcher = FakeDispatcher::default()
        .persisting(db.pool.clone())
        .with_memory_retirement(retirement.clone());
    for sbx in [campaign_boot_then_paused, external_wake_then_paused] {
        dispatcher.script_post_claim_start_error(
            sbx,
            DispatchError::Retryable("start RPC timed out after node claim".to_owned()),
        );
    }
    let reconciler = force_restart_reconciler(&db.pool, dispatcher.clone(), retirement);

    // Tick 1 stops both sandboxes and enters `StartingOnTarget` before `start`
    // times out post-claim.
    tick(&reconciler).await;
    for sbx in [campaign_boot_then_paused, external_wake_then_paused] {
        assert!(matches!(
            dispatcher.recorded_stop(campaign, sbx),
            Some(RecordedStop::StartingOnTarget(_))
        ));
    }

    // Both sandboxes finish booting onto `target()` on the node and are then
    // paused (`SandboxLifecycle::Paused`), with `:pause` receipts recorded.
    // On `external_wake_then_paused`, an external `:start` also completed after
    // `stopped_at` (for example, the campaign's start failed on the node and a
    // user cold-booted onto `target()` before pausing).
    for sbx in [campaign_boot_then_paused, external_wake_then_paused] {
        set_bound(&db.pool, sbx, target()).await?;
        set_lifecycle(&db.pool, sbx, SandboxLifecycle::Paused.as_str()).await?;
        let now: time::OffsetDateTime = sqlx::query_scalar(sql!("SELECT clock_timestamp()"))
            .fetch_one(&db.pool)
            .await?;
        if sbx == external_wake_then_paused {
            insert_public_lifecycle_op(
                &db.pool,
                sbx,
                OperationKind::StartSandbox,
                OperationState::Succeeded,
                now,
                Some(now),
            )
            .await?;
        }
        insert_public_lifecycle_op(
            &db.pool,
            sbx,
            OperationKind::PauseSandbox,
            OperationState::Succeeded,
            now,
            Some(now),
        )
        .await?;
    }

    // Tick 2 recovers both stranded `StartingOnTarget` items on `Paused`:
    // `campaign_boot_then_paused` attributes `Done` to the campaign's own cold
    // boot without issuing a second `start`, while `external_wake_then_paused`
    // clears `StartingOnTarget` and skips with `NO_LONGER_ON_SOURCE`.
    tick(&reconciler).await;
    assert_eq!(
        item(&db.pool, campaign, campaign_boot_then_paused).await?,
        ("done".to_owned(), None, 2)
    );
    assert_eq!(
        item(&db.pool, campaign, external_wake_then_paused).await?,
        (
            "skipped".to_owned(),
            Some(NO_LONGER_ON_SOURCE.to_owned()),
            2
        )
    );
    assert_eq!(
        dispatcher
            .calls()
            .iter()
            .filter(|call| **call == Call::Start(campaign_boot_then_paused))
            .count(),
        1
    );
    assert_eq!(
        dispatcher.recorded_stop(campaign, external_wake_then_paused),
        None
    );
    assert_eq!(
        dispatcher.started(),
        vec![campaign_boot_then_paused, external_wake_then_paused],
        "neither Paused sandbox may be cold-booted a second time during recovery"
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn expired_item_lease_blocks_stopped_upgrade_and_clear_stopped_at() -> TestResult {
    let Some(db) = test_db().await? else {
        return Ok(());
    };
    seed_policy(&db.pool, RuntimeEnvironmentState::Deprecated, true).await?;
    let node = node(&db.pool).await?;

    // Part 1: `SandboxDriver` (`StoppedSandboxes`) releases without writing
    // `target()` if the item's lease expires before `upgrade_stopped` commits.
    let move_campaign = insert_campaign(&db.pool, seed()).await?;
    let stopped_sbx = stopped_on_source(&db.pool, node).await?;
    record(&db.pool, move_campaign, &[stopped_sbx]).await?;
    let expired_move_lease =
        store::lease_items(&db.pool, move_campaign, Duration::from_secs(30), 1)
            .await?
            .pop()
            .expect("leased stopped item");
    sqlx::query(sql!(
        "UPDATE runtime_upgrade_items
         SET lease_expires_at = clock_timestamp() - INTERVAL '1 second'
         WHERE campaign_id = $1 AND subject_id = $2"
    ))
    .bind(move_campaign)
    .bind(stopped_sbx)
    .execute(&db.pool)
    .await?;

    let move_row = store::list_unfinished(&db.pool)
        .await?
        .into_iter()
        .find(|row| row.id == move_campaign)
        .expect("move campaign");
    let dispatcher = FakeDispatcher::default().persisting(db.pool.clone());
    let move_driver = SandboxDriver::new(db.pool.clone(), K8S, dispatcher.clone());
    assert!(matches!(
        move_driver.upgrade(&move_row, &expired_move_lease).await,
        ItemOutcome::Release(_)
    ));
    let inputs = Inputs::read(&db.pool).await?;
    let subject = store::subject_sandbox(&db.pool, stopped_sbx)
        .await?
        .expect("stopped sandbox exists");
    assert_eq!(
        subject
            .effective_execution()
            .and_then(|exec| inputs.attributed(exec)),
        Some(source()),
        "expired item lease must not rewrite stopped sandbox execution to target"
    );

    // Part 2: `clear_stopped_at` is a no-op when the caller's item lease has
    // already expired.
    let restart_campaign = insert_campaign(&db.pool, force_restart_seed()).await?;
    let restart_sbx = resident_on_source(&db.pool, SandboxLifecycle::Started, node).await?;
    record(&db.pool, restart_campaign, &[restart_sbx]).await?;
    let expired_restart_lease =
        store::lease_items(&db.pool, restart_campaign, Duration::from_secs(30), 1)
            .await?
            .pop()
            .expect("leased restart item");
    let now = jiff::Timestamp::now();
    let fence = StopFence::at(now);
    dispatcher
        .record_item_stopped_at(
            Some(&expired_restart_lease),
            restart_sbx,
            None,
            fence.clone(),
        )
        .await;
    sqlx::query(sql!(
        "UPDATE runtime_upgrade_items
         SET lease_expires_at = clock_timestamp() - INTERVAL '1 second'
         WHERE campaign_id = $1 AND subject_id = $2"
    ))
    .bind(restart_campaign)
    .bind(restart_sbx)
    .execute(&db.pool)
    .await?;

    dispatcher
        .clear_stopped_at(&expired_restart_lease, RecordedStop::Stopped(fence.clone()))
        .await
        .expect("clear_stopped_at on expired lease");
    assert_eq!(
        dispatcher.recorded_stop(restart_campaign, restart_sbx),
        Some(RecordedStop::Stopped(fence)),
        "expired lease must not clear an active campaign stop fence"
    );

    // Part 3: `clear_initiated_stop` is also a no-op when the caller's item
    // lease has expired.
    let init_sbx = resident_on_source(&db.pool, SandboxLifecycle::Started, node).await?;
    record(&db.pool, restart_campaign, &[init_sbx]).await?;
    let expired_init_lease =
        store::lease_items(&db.pool, restart_campaign, Duration::from_secs(30), 1)
            .await?
            .pop()
            .expect("leased initiated-stop item");
    let recorded_init = dispatcher
        .record_item_initiated_stop(
            Some(&expired_init_lease),
            init_sbx,
            InitiatedStop {
                initiated_at: now,
                dispatched_at: now,
                k8s_stopped_execution: None,
            },
        )
        .await;
    sqlx::query(sql!(
        "UPDATE runtime_upgrade_items
         SET lease_expires_at = clock_timestamp() - INTERVAL '1 second'
         WHERE campaign_id = $1 AND subject_id = $2"
    ))
    .bind(restart_campaign)
    .bind(init_sbx)
    .execute(&db.pool)
    .await?;
    dispatcher
        .clear_initiated_stop(Some(&expired_init_lease), init_sbx, Some(&recorded_init))
        .await;
    assert_eq!(
        dispatcher.recorded_stop(restart_campaign, init_sbx),
        Some(RecordedStop::Initiated(recorded_init)),
        "expired lease must not clear an active campaign InitiatedStop"
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn unbound_source_reattributed_to_third_environment_after_stop_holds_then_recovers()
-> TestResult {
    let Some(db) = test_db().await? else {
        return Ok(());
    };
    seed_policy(&db.pool, RuntimeEnvironmentState::Deprecated, true).await?;
    let campaign = insert_campaign(&db.pool, force_restart_seed()).await?;
    let node = node(&db.pool).await?;
    let unbound_sbx = insert_sandbox(
        &db.pool,
        K8S,
        SandboxLifecycle::Started,
        node,
        Published::Unbound {
            runtime_version: "1.15.1",
            kernel: "6.1.155",
            state_format_version: "9.0.0",
        },
    )
    .await?;

    let retirement = FakeMemoryRetirement::retired(source());
    let dispatcher = FakeDispatcher::default()
        .persisting(db.pool.clone())
        .with_memory_retirement(retirement.clone());
    dispatcher.script_start(
        unbound_sbx,
        vec![Err(DispatchError::Retryable(
            "transient cold-boot error".to_owned(),
        ))],
    );
    let reconciler = force_restart_reconciler(&db.pool, dispatcher.clone(), retirement);

    // Tick 1 stops the unbound sandbox and leaves `RecordedStop::Stopped` when
    // the first `start` fails transiently.
    tick(&reconciler).await;
    assert!(matches!(
        dispatcher.recorded_stop(campaign, unbound_sbx),
        Some(RecordedStop::Stopped(_))
    ));

    // Reattribute the unchanged unbound tuple `(1.15.1, 6.1.155, 9.0.0)` to a
    // third catalog environment (`environment(2)`) by updating catalog rows.
    let third = environment(2);
    sqlx::query(sql!(
        "UPDATE runtime_environments SET runtime_version = '1.14.0' WHERE environment_ref = $1"
    ))
    .bind(source().to_string())
    .execute(&db.pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO runtime_environments (
             environment_ref, runtime, runtime_version, version_sort_key,
             kernel_version, state_format_name, state_format_version, upgrade_edges
         )
         VALUES ($1, 'firecracker', '1.15.1', '{1}', '6.1.155', 'firecracker-snapshot', '9.0.0', '{}')"
    ))
    .bind(third.to_string())
    .execute(&db.pool)
    .await?;

    // Tick 2 sees the stopped sandbox's unbound publish uniquely attributed to
    // `third` (neither `source()` nor `target()`) and holds it stopped instead
    // of misclassifying the catalog shift as an external wake+stop.
    tick(&reconciler).await;
    assert_eq!(
        item(&db.pool, campaign, unbound_sbx).await?,
        (
            "pending".to_owned(),
            Some(POLICY_STOPPED_FORCE_RESTARTING.to_owned()),
            1
        )
    );
    assert!(matches!(
        dispatcher.recorded_stop(campaign, unbound_sbx),
        Some(RecordedStop::Stopped(_))
    ));

    // Restoring `source()`'s catalog attribution lets Tick 3 finish the cold
    // boot onto `target()`.
    sqlx::query(sql!(
        "DELETE FROM runtime_environments WHERE environment_ref = $1"
    ))
    .bind(third.to_string())
    .execute(&db.pool)
    .await?;
    sqlx::query(sql!(
        "UPDATE runtime_environments SET runtime_version = '1.15.1' WHERE environment_ref = $1"
    ))
    .bind(source().to_string())
    .execute(&db.pool)
    .await?;

    tick(&reconciler).await;
    assert_eq!(
        item(&db.pool, campaign, unbound_sbx).await?,
        ("done".to_owned(), None, 2)
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn claim_time_retirement_reversals_and_episode_scoped_stop_admission_hold_safely()
-> TestResult {
    let Some(db) = test_db().await? else {
        return Ok(());
    };
    seed_policy(&db.pool, RuntimeEnvironmentState::Deprecated, true).await?;
    let campaign = insert_campaign(
        &db.pool,
        CampaignSeed {
            per_node: 4,
            per_cluster: 4,
            ..force_restart_seed()
        },
    )
    .await?;
    let node = node(&db.pool).await?;
    let stop_unretired = resident_on_source(&db.pool, SandboxLifecycle::Started, node).await?;

    let retirement = FakeMemoryRetirement::retired(source());
    let dispatcher = FakeDispatcher::default()
        .persisting(db.pool.clone())
        .with_memory_retirement(retirement.clone());

    // Step 1: claim-time unretire inside `stop` holds the running sandbox
    // without stopping it or spending an attempt.
    dispatcher
        .state()
        .unretire_source_before_stop
        .insert(stop_unretired);

    let reconciler = force_restart_reconciler(&db.pool, dispatcher.clone(), retirement.clone());
    record(&db.pool, campaign, &[stop_unretired]).await?;
    tick(&reconciler).await;
    assert_eq!(
        item(&db.pool, campaign, stop_unretired).await?,
        (
            "pending".to_owned(),
            Some(POLICY_STOPPED_FORCE_RESTARTING.to_owned()),
            0
        )
    );
    assert_eq!(
        store::subject_sandbox(&db.pool, stop_unretired)
            .await?
            .expect("stop_unretired exists")
            .lifecycle,
        "started"
    );

    // Re-retire so `stop_unretired` completes cleanly, then verify a second
    // sandbox (`start_unretired`) with `TargetStart::Started` explicitly
    // scripted in `start_script` STILL holds stopped when retirement flips at
    // `start` claim time.
    retirement.retire(source());
    tick(&reconciler).await;
    assert_eq!(item(&db.pool, campaign, stop_unretired).await?.0, "done");

    let start_unretired = resident_on_source(&db.pool, SandboxLifecycle::Started, node).await?;
    dispatcher.script_start(start_unretired, vec![Ok(TargetStart::Started)]);
    dispatcher
        .state()
        .unretire_source_before_start
        .insert(start_unretired);
    tick(&reconciler).await;
    assert_eq!(
        item(&db.pool, campaign, start_unretired).await?,
        (
            "pending".to_owned(),
            Some(POLICY_STOPPED_FORCE_RESTARTING.to_owned()),
            0
        )
    );
    assert_eq!(
        store::subject_sandbox(&db.pool, start_unretired)
            .await?
            .expect("start_unretired exists")
            .lifecycle,
        "stopped"
    );

    // Re-retiring lets the next tick finish `start_unretired` cleanly.
    retirement.retire(source());
    tick(&reconciler).await;
    assert_eq!(item(&db.pool, campaign, start_unretired).await?.0, "done");

    // Verify `record_item_stopped_at` ignores a stale `expected_initiated` once
    // `clear_initiated_stop` or a newer `InitiatedStop` has superseded it, and
    // `converge_published_stopped` does not cross-pollinate stop admission from
    // another campaign.
    let episode_sbx = resident_on_source(&db.pool, SandboxLifecycle::Started, node).await?;
    let campaign_b = insert_campaign(&db.pool, force_restart_seed()).await?;
    record(&db.pool, campaign_b, &[episode_sbx]).await?;
    let leased_b = store::lease_items(&db.pool, campaign_b, Duration::from_secs(30), 1)
        .await?
        .pop()
        .expect("leased campaign_b item");
    let first_now = jiff::Timestamp::now();
    let first_init = InitiatedStop {
        initiated_at: first_now,
        dispatched_at: first_now,
        k8s_stopped_execution: None,
    };
    let recorded_init = dispatcher
        .record_item_initiated_stop(Some(&leased_b), episode_sbx, first_init)
        .await;
    let stale_init = InitiatedStop {
        initiated_at: first_now - jiff::SignedDuration::from_secs(5),
        dispatched_at: first_now - jiff::SignedDuration::from_secs(5),
        k8s_stopped_execution: None,
    };
    dispatcher
        .clear_initiated_stop(Some(&leased_b), episode_sbx, Some(&stale_init))
        .await;
    assert_eq!(
        dispatcher.recorded_stop(campaign_b, episode_sbx),
        Some(RecordedStop::Initiated(recorded_init.clone())),
        "stale expected_initiated must not clear a newer RecordedStop::Initiated"
    );
    dispatcher
        .clear_initiated_stop(Some(&leased_b), episode_sbx, Some(&recorded_init))
        .await;
    dispatcher
        .record_item_stopped_at(
            Some(&leased_b),
            episode_sbx,
            Some(&recorded_init),
            StopFence::new(
                recorded_init.initiated_at,
                recorded_init.dispatched_at,
                jiff::Timestamp::now(),
                None,
            ),
        )
        .await;
    assert_eq!(
        dispatcher.recorded_stop(campaign_b, episode_sbx),
        None,
        "stale expected_initiated must not write Stopped after clear_initiated_stop"
    );

    // Record a fresh unadmitted `Initiated` on `campaign_b` (with active K8s
    // execution), converge `episode_sbx` to `stopped` via node-status ingest,
    // and seed `node_admitted_campaign_stops` ONLY for `campaign` (a different
    // campaign). `dispatcher.stop` on `leased_b` must decline and clear
    // `campaign_b`'s `Initiated` instead of adopting the guest poweroff.
    let k8s_exec = super::fakes::active_k8s_stopped_execution_for_test(&db.pool, episode_sbx).await;
    let second_now = jiff::Timestamp::now();
    let second_init = InitiatedStop {
        initiated_at: second_now,
        dispatched_at: second_now,
        k8s_stopped_execution: k8s_exec,
    };
    dispatcher
        .record_item_initiated_stop(Some(&leased_b), episode_sbx, second_init.clone())
        .await;
    dispatcher
        .state()
        .node_admitted_campaign_stops
        .insert((campaign, episode_sbx), second_init.dispatched_at);
    set_lifecycle(&db.pool, episode_sbx, SandboxLifecycle::Stopped.as_str()).await?;
    assert!(matches!(
        dispatcher
            .stop(
                &leased_b,
                true,
                SandboxId::from_uuid(episode_sbx),
                source(),
                target(),
            )
            .await?,
        GuardedStop::Declined(_)
    ));
    assert_eq!(
        dispatcher.recorded_stop(campaign_b, episode_sbx),
        None,
        "node admission for another campaign must not satisfy campaign_b's stop retry"
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn stranded_lease_candidates_do_not_starve_other_nodes_behind_one_saturated_node()
-> TestResult {
    let Some(db) = test_db().await? else {
        return Ok(());
    };
    seed_policy(&db.pool, RuntimeEnvironmentState::Deprecated, true).await?;
    let node_a = node(&db.pool).await?;
    let node_b = node(&db.pool).await?;

    // Create 4 sandboxes and sort their UUIDs so the lowest 3 sit on `node_a`
    // and the highest UUID sits on `node_b`.
    let mut ids = Vec::new();
    for _ in 0..4 {
        ids.push(resident_on_source(&db.pool, SandboxLifecycle::Started, node_a).await?);
    }
    ids.sort();
    let [a1, a2, a3, b1] = [ids[0], ids[1], ids[2], ids[3]];
    sqlx::query(sql!(
        "UPDATE sandboxes SET active_node_id = $2 WHERE id = $1"
    ))
    .bind(b1)
    .bind(node_b)
    .execute(&db.pool)
    .await?;
    sqlx::query(sql!(
        "UPDATE sandbox_observed_states SET node_id = $2 WHERE sandbox_id = $1"
    ))
    .bind(b1)
    .bind(node_b)
    .execute(&db.pool)
    .await?;

    let nodes = HashMap::from([(a1, node_a), (a2, node_a), (a3, node_a), (b1, node_b)]);
    let campaign = insert_campaign(
        &db.pool,
        CampaignSeed {
            per_node: 4,
            per_cluster: 4,
            ..force_restart_seed()
        },
    )
    .await?;
    let retirement = FakeMemoryRetirement::retired(source());
    let dispatcher = FakeDispatcher::with_nodes(nodes)
        .persisting(db.pool.clone())
        .with_memory_retirement(retirement.clone());
    for id in [a1, a2, a3, b1] {
        dispatcher.script_post_claim_start_error(
            id,
            DispatchError::Retryable(
                "start timed out after node claim for multi-node stranded fairness check"
                    .to_owned(),
            ),
        );
    }
    let setup_reconciler =
        force_restart_reconciler(&db.pool, dispatcher.clone(), retirement.clone());
    tick(&setup_reconciler).await;
    let base_updated_at: time::OffsetDateTime = sqlx::query_scalar(sql!("SELECT NOW()"))
        .fetch_one(&db.pool)
        .await?;
    for (idx, id) in [a1, a2, a3, b1].into_iter().enumerate() {
        assert!(matches!(
            dispatcher.recorded_stop(campaign, id),
            Some(RecordedStop::StartingOnTarget(_))
        ));
        set_bound(&db.pool, id, target()).await?;
        set_lifecycle(&db.pool, id, SandboxLifecycle::Started.as_str()).await?;
        let stamped = base_updated_at + time::Duration::microseconds(i64::try_from(idx).unwrap());
        sqlx::query(sql!(
            "UPDATE runtime_upgrade_items
                SET updated_at = $3
              WHERE campaign_id = $1 AND subject_id = $2"
        ))
        .bind(campaign)
        .bind(id)
        .bind(stamped)
        .execute(&db.pool)
        .await?;
    }

    // Remove the certified upgrade edge so `plan()` enters stranded mode
    // (`moves == false`, `lease_when_stranded == true`), and tighten campaign
    // caps to `max_in_flight_per_node = 1` and `max_in_flight_per_cluster = 2`
    // with `candidate_page = 2`. Even though `a1 < a2 < a3 < b1` in
    // `(updated_at, subject_id)`, one tick must lease `a1` on `node_a` AND `b1`
    // on `node_b` in the same pass (`leased == 2`) instead of filling the SQL
    // page with `a1, a2` on `node_a`.
    sqlx::query(sql!(
        "UPDATE runtime_environments SET upgrade_edges = '{}'::text[] WHERE environment_ref = $1"
    ))
    .bind(source().to_string())
    .execute(&db.pool)
    .await?;
    sqlx::query(sql!(
        "UPDATE runtime_upgrade_campaigns
         SET max_in_flight_per_node = 1,
             max_in_flight_per_cluster = 2
         WHERE id = $1"
    ))
    .bind(campaign)
    .execute(&db.pool)
    .await?;
    // Put an in-flight external stop operation on `a1` so `a1` returns
    // `ItemOutcome::Release` on each recovery tick while `a2`, `a3`, and `b1`
    // are ready to settle `Done`, and run under the production `LEASE_SCAN`
    // (`selected.len() < goal`) as well as small pages. Even while `a1` keeps
    // releasing on `node_a` (`max_in_flight_per_node = 1`,
    // `max_in_flight_per_cluster = 2`) and `b1 > a3 > a2 > a1`, `stranded_cursors`
    // must advance past `a1` instead of resetting to `None` or jumping to `b1`.
    let after_stop_op_ts: time::OffsetDateTime =
        sqlx::query_scalar(sql!("SELECT clock_timestamp()"))
            .fetch_one(&db.pool)
            .await?;
    insert_public_lifecycle_op(
        &db.pool,
        a1,
        pplx_space_public_api::OperationKind::StopSandbox,
        pplx_space_public_api::OperationState::Running,
        after_stop_op_ts,
        None,
    )
    .await?;
    let default_scan_reconciler =
        force_restart_reconciler(&db.pool, dispatcher.clone(), retirement.clone());
    // Directly exercise `stranded_lease_candidates` with `scan_limit = 2` so
    // window 1 (`[a1, a2]`) hits `node_a`'s per-node cap and window 2
    // (`after = Some(a2)`, `excluded_nodes = [node_a]`) skips `a3` and selects `b1`.
    let campaign_row = store::list_unfinished(&db.pool)
        .await?
        .into_iter()
        .find(|row| row.id == campaign)
        .expect("campaign exists");
    let direct_driver = SandboxForceRestartDriver::new(
        db.pool.clone(),
        K8S,
        dispatcher.clone(),
        retirement.clone(),
    );
    let multi_page_selected = crate::runtime_upgrade::CampaignDriver::stranded_lease_candidates(
        &direct_driver,
        &campaign_row,
        2,
    )
    .await
    .expect("selecting multi-page stranded candidates");
    assert_eq!(multi_page_selected, vec![a1, b1]);

    let first_recovery = tick(&default_scan_reconciler).await;
    assert_eq!(first_recovery.leased, 2);
    assert_eq!(
        item(&db.pool, campaign, a1).await?.0,
        "pending",
        "a1 releases while its external stop operation is in flight"
    );
    assert_eq!(item(&db.pool, campaign, a2).await?.0, "pending");
    assert_eq!(item(&db.pool, campaign, a3).await?.0, "pending");
    assert_eq!(
        item(&db.pool, campaign, b1).await?.0,
        "done",
        "node_b's stranded sandbox must not starve behind node_a's extra pending rows"
    );
    // Subsequent ticks advance `stranded_cursors` past `a1` onto `a2` and `a3`
    // even while `a1` remains `pending` (`selected.len() < goal`), then wrap
    // back to `a1` once its external stop fails without stopping the sandbox.
    let second_recovery = tick(&default_scan_reconciler).await;
    assert_eq!(second_recovery.leased, 1);
    assert_eq!(item(&db.pool, campaign, a2).await?.0, "done");
    let third_recovery = tick(&default_scan_reconciler).await;
    assert_eq!(third_recovery.leased, 1);
    assert_eq!(item(&db.pool, campaign, a3).await?.0, "done");
    sqlx::query(sql!(
        "UPDATE public_operations
         SET state = 'failed', completed_at = NOW(), updated_at = NOW()
         WHERE target_type = 'sandbox'
           AND target_id = $1
           AND state = 'running'"
    ))
    .bind(PublicSandboxId::from(SandboxId::from_uuid(a1)).to_string())
    .execute(&db.pool)
    .await?;
    let fourth_recovery = tick(&default_scan_reconciler).await;
    assert_eq!(fourth_recovery.leased, 1);
    assert_eq!(item(&db.pool, campaign, a1).await?.0, "done");

    // Seed an untouched `pending` prefix `[u1..=u5]` ahead of a sandbox
    // whose timed-out `StartingOnTarget` attempt converged on `target()`,
    // returned `Done` from `upgrade` before `finish_item` ran (for example,
    // a controller crash or transient `finish_item` error), lost its in-memory
    // `item_stopped_at` across restart, and was deleted while `moves == false`:
    // `upgrade`'s `StartingOnTarget` convergence path persists
    // `mark_started_on_target`, bounded probing (`scan_limit = 1` -> 4 rows/call)
    // advances across `[u1..=u4]` on call 1, reaches `deleted_settled` on call 2,
    // and finishes `deleted_settled` as `done` rather than `skipped`.
    let u1 = resident_on_source(&db.pool, SandboxLifecycle::Started, node_a).await?;
    let u2 = resident_on_source(&db.pool, SandboxLifecycle::Started, node_a).await?;
    let u3 = resident_on_source(&db.pool, SandboxLifecycle::Started, node_a).await?;
    let u4 = resident_on_source(&db.pool, SandboxLifecycle::Started, node_a).await?;
    let u5 = resident_on_source(&db.pool, SandboxLifecycle::Started, node_a).await?;
    let deleted_settled = insert_sandbox(
        &db.pool,
        K8S,
        SandboxLifecycle::Started,
        node_b,
        Published::Bound(target()),
    )
    .await?;
    record(&db.pool, campaign, &[u1, u2, u3, u4, u5, deleted_settled]).await?;
    let leased_settled = store::lease_selected_items(
        &db.pool,
        campaign,
        &[deleted_settled],
        std::time::Duration::from_secs(60),
        1,
    )
    .await?;
    assert_eq!(leased_settled.len(), 1);
    assert!(store::mark_run_started(&db.pool, &leased_settled[0]).await?);
    dispatcher.state().item_stopped_at.insert(
        (campaign, deleted_settled),
        RecordedStop::StartingOnTarget(StartingClaim::from_stop(StopFence::at(
            jiff::Timestamp::UNIX_EPOCH,
        ))),
    );
    let fresh_driver =
        SandboxForceRestartDriver::new(db.pool.clone(), K8S, dispatcher.clone(), retirement);
    assert_eq!(
        crate::runtime_upgrade::CampaignDriver::upgrade(
            &fresh_driver,
            &campaign_row,
            &leased_settled[0],
        )
        .await,
        store::ItemOutcome::Done,
    );
    sqlx::query(sql!(
        "UPDATE runtime_upgrade_items
         SET lease_expires_at = clock_timestamp() - interval '1 second'
         WHERE campaign_id = $1 AND subject_id = $2"
    ))
    .bind(campaign)
    .bind(deleted_settled)
    .execute(&db.pool)
    .await?;
    assert_eq!(
        store::redrive_expired(&db.pool, campaign, 3)
            .await?
            .requeued,
        1
    );
    dispatcher.state().item_stopped_at.clear();
    sqlx::query(sql!("DELETE FROM sandboxes WHERE id = $1"))
        .bind(deleted_settled)
        .execute(&db.pool)
        .await?;
    let prefix_window = crate::runtime_upgrade::CampaignDriver::stranded_lease_candidates(
        &fresh_driver,
        &campaign_row,
        1,
    )
    .await
    .expect("probing untouched prefix window");
    assert!(
        prefix_window.is_empty(),
        "bounded probe budget (4 rows) stops inside untouched prefix [u1..=u5]"
    );
    let tail_window = crate::runtime_upgrade::CampaignDriver::stranded_lease_candidates(
        &fresh_driver,
        &campaign_row,
        1,
    )
    .await
    .expect("advancing saved cursor past untouched prefix");
    assert_eq!(tail_window, vec![deleted_settled]);
    let fifth_recovery = tick(&default_scan_reconciler).await;
    assert_eq!(fifth_recovery.leased, 1);
    assert_eq!(
        item(&db.pool, campaign, deleted_settled).await?,
        ("done".to_owned(), None, 2),
        "deleted sandbox whose prior attempt durably settled on target must finish done across controller restart"
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn direct_substrate_undispatched_start_rollback_refreshes_stopped_fence() -> TestResult {
    let Some(db) = test_db().await? else {
        return Ok(());
    };
    seed_policy(&db.pool, RuntimeEnvironmentState::Deprecated, true).await?;
    let campaign = insert_campaign(
        &db.pool,
        CampaignSeed {
            per_node: 4,
            per_cluster: 4,
            ..force_restart_seed()
        },
    )
    .await?;
    let node = node(&db.pool).await?;
    let retry_ok = insert_sandbox(
        &db.pool,
        DIRECT,
        SandboxLifecycle::Started,
        node,
        Published::Bound(source()),
    )
    .await?;
    let external_after_rollback = insert_sandbox(
        &db.pool,
        DIRECT,
        SandboxLifecycle::Started,
        node,
        Published::Bound(source()),
    )
    .await?;

    let retirement = FakeMemoryRetirement::retired(source());
    let dispatcher = FakeDispatcher::default()
        .persisting(db.pool.clone())
        .with_memory_retirement(retirement.clone());
    for sbx in [retry_ok, external_after_rollback] {
        dispatcher.script_pre_dispatch_start_error(
            sbx,
            DispatchError::Retryable("node unreachable before start RPC".to_owned()),
        );
    }
    let reconciler = Reconciler::new(
        db.pool.clone(),
        Drivers::default().with(SandboxForceRestartDriver::new(
            db.pool.clone(),
            DIRECT,
            dispatcher.clone(),
            retirement,
        )),
        ReconcilerConfig {
            enabled: true,
            ..ReconcilerConfig::default()
        },
    );

    // Tick 1 stops both `Direct` sandboxes (`k8s_stopped_execution = None`),
    // claims `'stopped' -> 'starting'`, hits the pre-dispatch error, rolls
    // `'starting'` back to `'stopped'` (`RETURNING lifecycle_changed_at`), and
    // refreshes `RecordedStop::Stopped.settled_stopped_at` to the rolled-back
    // timestamp while preserving `stopped_at` at the original campaign stop.
    tick(&reconciler).await;
    for sbx in [retry_ok, external_after_rollback] {
        let first_start_fence = dispatcher
            .state()
            .start_fences
            .iter()
            .find(|(id, _, _)| *id == sbx)
            .map(|(_, fence, _)| fence.clone())
            .expect("first Start call fence captured before pre-dispatch rollback");
        let Some(RecordedStop::Stopped(fence)) = dispatcher.recorded_stop(campaign, sbx) else {
            panic!("expected refreshed RecordedStop::Stopped after undispatched rollback");
        };
        assert_eq!(fence.k8s_stopped_execution, None);
        assert_eq!(fence.stopped_at, first_start_fence.stopped_at);
        assert!(first_start_fence.settled_stopped_at <= fence.settled_stopped_at);
        let snap = store::subject_sandbox(&db.pool, sbx)
            .await?
            .expect("Direct sandbox exists");
        assert_eq!(snap.lifecycle, "stopped");
        assert_eq!(to_jiff(snap.lifecycle_changed_at), fence.settled_stopped_at);
    }

    // Simulate a post-rollback external wake+stop on `external_after_rollback`
    // by advancing `sandboxes.lifecycle_changed_at` beyond the rolled-back
    // `settled_stopped_at` fence.
    sqlx::query(sql!(
        "UPDATE sandboxes
         SET lifecycle_changed_at = clock_timestamp() + INTERVAL '1 millisecond'
         WHERE id = $1"
    ))
    .bind(external_after_rollback)
    .execute(&db.pool)
    .await?;

    // Tick 2: `retry_ok` matches its refreshed `settled_stopped_at` fence and
    // completes onto `target()`, while `external_after_rollback` detects the
    // post-rollback timestamp drift and skips with the stopped-sandbox refusal.
    tick(&reconciler).await;
    assert_eq!(
        item(&db.pool, campaign, retry_ok).await?,
        ("done".to_owned(), None, 2)
    );
    assert_eq!(
        item(&db.pool, campaign, external_after_rollback).await?,
        (
            "skipped".to_owned(),
            Some("the sandbox is stopped, which this campaign doesn't upgrade".to_owned()),
            2
        )
    );
    tick(&reconciler).await;
    assert_eq!(campaign_phase(&db.pool, campaign).await?, "completed");

    // Part 2 (Kubernetes): prove that preserving `stopped_at` separately from
    // `settled_stopped_at` keeps ownership across:
    //   1. `k8s_retired_rollback`: stopped pod retired (`finish_active(..., "stopped")`)
    //      before an undispatched `start` rolls `'starting'` back to `'stopped'`
    //      (`fence.stopped_at <= halted.terminated_at < fence.settled_stopped_at`).
    //   2. `k8s_evacuated_rollback`: stopped sandbox evacuated off-node with a
    //      `'disk'`/`'stop'` pin before an undispatched `start` rolls back
    //      (preserving both the `'disk'`/`'stop'` pin and `'off_node'` placement
    //      until retry cold-boots onto `replacement_node` and clears the pin).
    //   3. `k8s_converged_evacuated`: an `Initiated` campaign stop that converges
    //      to `'stopped'` and is evacuated + re-placed onto `replacement_node`
    //      before `dispatcher.stop` settles it (`lifecycle_changed_at >
    //      halted.terminated_at >= initiated_at`, `'disk'`/`'stop'` pin set).
    let replacement_node = super::node(&db.pool).await?;
    let k8s_campaign = insert_campaign(
        &db.pool,
        CampaignSeed {
            per_node: 4,
            per_cluster: 4,
            ..force_restart_seed()
        },
    )
    .await?;
    let k8s_retired_rollback =
        resident_on_source(&db.pool, SandboxLifecycle::Started, node).await?;
    let k8s_evacuated_rollback =
        resident_on_source(&db.pool, SandboxLifecycle::Started, node).await?;
    let k8s_converged_evacuated =
        resident_on_source(&db.pool, SandboxLifecycle::Started, node).await?;

    let k8s_retirement = FakeMemoryRetirement::retired(source());
    let k8s_dispatcher = FakeDispatcher::with_nodes(HashMap::from([
        (k8s_retired_rollback, replacement_node),
        (k8s_evacuated_rollback, replacement_node),
        (k8s_converged_evacuated, replacement_node),
    ]))
    .persisting(db.pool.clone())
    .with_memory_retirement(k8s_retirement.clone());

    // First hold both rollback sandboxes in `Stopped` so we can retire/evacuate
    // their stopped K8s executions before an undispatched `start` rolls back.
    for sbx in [k8s_retired_rollback, k8s_evacuated_rollback] {
        k8s_dispatcher.script_start(
            sbx,
            vec![Ok(TargetStart::HeldStopped(
                "hold before retirement".to_owned(),
            ))],
        );
    }
    // Prepare `k8s_converged_evacuated` as a converged `Initiated` stop that
    // `node_drain` already evacuated and re-placed onto `replacement_node`.
    record(&db.pool, k8s_campaign, &[k8s_converged_evacuated]).await?;
    let init_leased = store::lease_items(&db.pool, k8s_campaign, Duration::from_secs(30), 1)
        .await?
        .pop()
        .expect("leased k8s_converged_evacuated");
    assert!(store::mark_run_started(&db.pool, &init_leased).await?);
    let pre_init_snap = store::subject_sandbox(&db.pool, k8s_converged_evacuated)
        .await?
        .expect("k8s_converged_evacuated exists");
    let pre_init_exec =
        super::fakes::active_k8s_stopped_execution_for_test(&db.pool, k8s_converged_evacuated)
            .await;
    let init_ts = to_jiff(pre_init_snap.lifecycle_changed_at);
    let recorded_k8s_init = k8s_dispatcher
        .record_item_initiated_stop(
            Some(&init_leased),
            k8s_converged_evacuated,
            InitiatedStop {
                initiated_at: init_ts,
                dispatched_at: init_ts,
                k8s_stopped_execution: pre_init_exec,
            },
        )
        .await;
    k8s_dispatcher.state().node_admitted_campaign_stops.insert(
        (k8s_campaign, k8s_converged_evacuated),
        recorded_k8s_init.dispatched_at,
    );
    set_lifecycle(
        &db.pool,
        k8s_converged_evacuated,
        SandboxLifecycle::Stopped.as_str(),
    )
    .await?;
    let converged_re_placed_at: time::OffsetDateTime =
        sqlx::query_scalar(sql!("SELECT clock_timestamp() + INTERVAL '1 microsecond'"))
            .fetch_one(&db.pool)
            .await?;
    super::force_restart_drain_fences::drain_stopped_sandbox(
        &db.pool,
        k8s_converged_evacuated,
        Some(replacement_node),
        Uuid::now_v7(),
        Some(serde_json::json!({ "environment": source().to_string() })),
        converged_re_placed_at,
    )
    .await?;
    assert!(
        store::finish_item(
            &db.pool,
            &init_leased,
            &store::ItemOutcome::Release(store::ItemError::new("converged before retry")),
        )
        .await?
    );

    let k8s_reconciler = Reconciler::new(
        db.pool.clone(),
        Drivers::default().with(SandboxForceRestartDriver::new(
            db.pool.clone(),
            K8S,
            k8s_dispatcher.clone(),
            k8s_retirement,
        )),
        ReconcilerConfig {
            enabled: true,
            ..ReconcilerConfig::default()
        },
    );

    // Tick K8s-1: `k8s_converged_evacuated` settles its `Initiated` stop across
    // the evacuated + re-placed execution and boots onto `target()`, while both
    // rollback sandboxes stop and hold.
    tick(&k8s_reconciler).await;
    assert_eq!(
        item(&db.pool, k8s_campaign, k8s_converged_evacuated).await?,
        ("done".to_owned(), None, 1),
        "Initiated stop converged through node_drain evacuation + re-placement must stay owned and finish"
    );
    let cleared_evac_pin: (Option<Uuid>, Option<String>, Option<String>) = sqlx::query_as(sql!(
        "SELECT current_local_artifact_id,
                current_local_artifact_kind,
                current_local_artifact_origin
         FROM sandboxes
         WHERE id = $1"
    ))
    .bind(k8s_converged_evacuated)
    .fetch_one(&db.pool)
    .await?;
    assert_eq!(
        cleared_evac_pin,
        (None, None, None),
        "settling started on target must clear the drained stop disk pin columns on sandboxes"
    );

    // Retire `k8s_retired_rollback`'s stopped execution row and evacuate
    // `k8s_evacuated_rollback` off-node with a `'disk'`/`'stop'` pin, then
    // script undispatched `start` errors on both.
    assert!(
        pplx_space_sandbox_store::k8s_executions::retire::finish_active(
            &db.pool,
            SandboxId::from_uuid(k8s_retired_rollback),
            "stopped",
        )
        .await?
    );
    let evac_at: time::OffsetDateTime =
        sqlx::query_scalar(sql!("SELECT clock_timestamp() + INTERVAL '1 microsecond'"))
            .fetch_one(&db.pool)
            .await?;
    super::force_restart_drain_fences::drain_stopped_sandbox(
        &db.pool,
        k8s_evacuated_rollback,
        None,
        Uuid::now_v7(),
        Some(serde_json::json!({ "environment": source().to_string() })),
        evac_at,
    )
    .await?;
    for sbx in [k8s_retired_rollback, k8s_evacuated_rollback] {
        k8s_dispatcher.script_pre_dispatch_start_error(
            sbx,
            DispatchError::Retryable("node unreachable before K8s start RPC".to_owned()),
        );
    }

    // Tick K8s-2: both hit the undispatched `start` rollback, advancing
    // `settled_stopped_at` past the earlier retirement/evacuation timestamp
    // while preserving `stopped_at` (and preserving `'disk'`/`'stop'` +
    // `'off_node'` on `k8s_evacuated_rollback`).
    tick(&k8s_reconciler).await;
    for sbx in [k8s_retired_rollback, k8s_evacuated_rollback] {
        let Some(RecordedStop::Stopped(fence)) = k8s_dispatcher.recorded_stop(k8s_campaign, sbx)
        else {
            panic!("expected RecordedStop::Stopped after K8s undispatched start rollback");
        };
        assert!(
            fence.stopped_at < fence.settled_stopped_at,
            "rollback must advance settled_stopped_at without overwriting original stopped_at"
        );
    }
    let evac_after_rollback: (String, Option<NodeId>, bool) = sqlx::query_as(sql!(
        "SELECT placement,
                active_node_id,
                (current_local_artifact_id IS NOT NULL
                 AND current_local_artifact_kind = 'disk'
                 AND current_local_artifact_origin = 'stop')
         FROM sandboxes
         WHERE id = $1"
    ))
    .bind(k8s_evacuated_rollback)
    .fetch_one(&db.pool)
    .await?;
    assert_eq!(
        evac_after_rollback,
        ("off_node".to_owned(), None, true),
        "undispatched start rollback must preserve off_node placement and stop-disk pin"
    );

    // Tick K8s-3: both retry `start`, verify stop ownership via `(stopped_at,
    // settled_stopped_at)`, boot onto `target()`, and clear the `'disk'` pin.
    tick(&k8s_reconciler).await;
    for sbx in [k8s_retired_rollback, k8s_evacuated_rollback] {
        assert_eq!(
            item(&db.pool, k8s_campaign, sbx).await?,
            ("done".to_owned(), None, 2)
        );
        let cleared_pin: (Option<Uuid>, Option<String>, Option<String>) = sqlx::query_as(sql!(
            "SELECT current_local_artifact_id,
                    current_local_artifact_kind,
                    current_local_artifact_origin
             FROM sandboxes
             WHERE id = $1"
        ))
        .bind(sbx)
        .fetch_one(&db.pool)
        .await?;
        assert_eq!(
            cleared_pin,
            (None, None, None),
            "settling started on target must clear any stop-disk pin columns on sandboxes"
        );
    }
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn pre_dispatch_settled_stop_receipts_and_converged_evacuated_initiated_stop_finish_cleanly()
-> TestResult {
    use pplx_space_public_api::{
        OperationKind, OperationState, OperationTargetType, PublicSandboxId,
    };

    let Some(db) = test_db().await? else {
        return Ok(());
    };
    seed_policy(&db.pool, RuntimeEnvironmentState::Deprecated, true).await?;
    let node = node(&db.pool).await?;

    let noop_stop_sbx = resident_on_source(&db.pool, SandboxLifecycle::Started, node).await?;
    let auto_restart_rollback_sbx =
        resident_on_source(&db.pool, SandboxLifecycle::Started, node).await?;
    let stale_real_stop_sbx = resident_on_source(&db.pool, SandboxLifecycle::Started, node).await?;
    let k8s_converged_evac_sbx =
        resident_on_source(&db.pool, SandboxLifecycle::Started, node).await?;

    for (sbx, ops) in [
        (
            noop_stop_sbx,
            vec![(
                OperationKind::StopSandbox,
                serde_json::json!({}),
                Some(serde_json::json!({
                    "controller_response": { "already_completed": true }
                })),
            )],
        ),
        (
            auto_restart_rollback_sbx,
            vec![
                (
                    OperationKind::StopSandbox,
                    serde_json::json!({}),
                    Some(serde_json::json!({
                        "controller_response": { "already_completed": false }
                    })),
                ),
                (
                    OperationKind::RollbackSandbox,
                    serde_json::json!({ "rollback": { "auto_restart": true } }),
                    None,
                ),
            ],
        ),
        (
            stale_real_stop_sbx,
            vec![(
                OperationKind::StopSandbox,
                serde_json::json!({}),
                Some(serde_json::json!({
                    "controller_response": { "already_completed": false }
                })),
            )],
        ),
    ] {
        let started_at: time::OffsetDateTime = sqlx::query_scalar(sql!(
            "UPDATE sandboxes
             SET lifecycle_changed_at = NOW() - INTERVAL '5 seconds'
             WHERE id = $1
             RETURNING lifecycle_changed_at"
        ))
        .bind(sbx)
        .fetch_one(&db.pool)
        .await?;
        let target_id = PublicSandboxId::from(SandboxId::from_uuid(sbx)).to_string();
        for (idx, (kind, payload_json, result_json)) in ops.into_iter().enumerate() {
            let at =
                started_at + time::Duration::seconds(i64::try_from(idx + 1).expect("small offset"));
            sqlx::query(sql!(
                "INSERT INTO public_operations (
                     operation_id, org_id, actor_user_id, kind, target_type, target_id,
                     state, payload_json, result_json, requested_at, completed_at
                 )
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $10)"
            ))
            .bind(Uuid::now_v7())
            .bind(SEED_ORG_ID)
            .bind(SEED_USER_ID)
            .bind(kind.as_str())
            .bind(OperationTargetType::Sandbox.as_str())
            .bind(&target_id)
            .bind(OperationState::Succeeded.as_str())
            .bind(payload_json)
            .bind(result_json)
            .bind(at)
            .execute(&db.pool)
            .await?;
        }
    }

    let campaign = insert_campaign(
        &db.pool,
        CampaignSeed {
            per_node: 8,
            per_cluster: 8,
            ..force_restart_seed()
        },
    )
    .await?;
    let retirement = FakeMemoryRetirement::retired(source());
    let dispatcher = FakeDispatcher::with_nodes(HashMap::from([
        (noop_stop_sbx, node),
        (auto_restart_rollback_sbx, node),
        (stale_real_stop_sbx, node),
        (k8s_converged_evac_sbx, node),
    ]))
    .persisting(db.pool.clone())
    .with_memory_retirement(retirement.clone());

    // Pre-seed `k8s_converged_evac_sbx` as a retry (`attempts = 1`) whose prior
    // attempt admitted an `InitiatedStop` on `G1`, converged `'stopped'`, and was
    // then evacuated off-node (`pinned_stop_disk == true`, `lifecycle_changed_at >
    // dispatched_at`).
    record(&db.pool, campaign, &[k8s_converged_evac_sbx]).await?;
    sqlx::query(sql!(
        "UPDATE runtime_upgrade_items
         SET attempts = 1,
             last_error = 'prior stop call timed out'
         WHERE campaign_id = $1 AND subject_id = $2"
    ))
    .bind(campaign)
    .bind(k8s_converged_evac_sbx)
    .execute(&db.pool)
    .await?;
    let k8s_exec =
        super::fakes::active_k8s_stopped_execution_for_test(&db.pool, k8s_converged_evac_sbx)
            .await
            .expect("active K8s execution exists");
    let (raw_init, raw_disp): (time::OffsetDateTime, time::OffsetDateTime) = sqlx::query_as(sql!(
        "SELECT lifecycle_changed_at, NOW() FROM sandboxes WHERE id = $1"
    ))
    .bind(k8s_converged_evac_sbx)
    .fetch_one(&db.pool)
    .await?;
    let initiated = InitiatedStop {
        initiated_at: to_jiff(raw_init),
        dispatched_at: to_jiff(raw_disp),
        k8s_stopped_execution: Some(k8s_exec),
    };
    dispatcher.state().item_stopped_at.insert(
        (campaign, k8s_converged_evac_sbx),
        RecordedStop::Initiated(initiated.clone()),
    );
    dispatcher
        .state()
        .node_admitted_campaign_stops
        .insert((campaign, k8s_converged_evac_sbx), initiated.dispatched_at);
    set_lifecycle(
        &db.pool,
        k8s_converged_evac_sbx,
        SandboxLifecycle::Stopped.as_str(),
    )
    .await?;
    let evac_at: time::OffsetDateTime =
        sqlx::query_scalar(sql!("SELECT clock_timestamp() + INTERVAL '5 milliseconds'"))
            .fetch_one(&db.pool)
            .await?;
    super::force_restart_drain_fences::drain_stopped_sandbox(
        &db.pool,
        k8s_converged_evac_sbx,
        None,
        Uuid::now_v7(),
        None,
        evac_at,
    )
    .await?;
    // Clear execution fields on the drained stop-disk snapshot so
    // `subject.effective_execution()` is `None`, exercising metadata-free
    // `RecordedStop::Initiated` recovery onto `target()`.
    sqlx::query(sql!(
        "UPDATE snapshots
         SET runtime = NULL,
             state_format_name = NULL,
             execution_metadata = NULL
         WHERE sandbox_id = $1"
    ))
    .bind(k8s_converged_evac_sbx)
    .execute(&db.pool)
    .await?;

    let reconciler = force_restart_reconciler(&db.pool, dispatcher.clone(), retirement);
    tick(&reconciler).await;

    assert_eq!(
        item(&db.pool, campaign, noop_stop_sbx).await?,
        ("done".to_owned(), None, 1),
        "standalone already_completed stop receipt must not veto campaign stop"
    );
    assert_eq!(
        item(&db.pool, campaign, auto_restart_rollback_sbx).await?,
        ("done".to_owned(), None, 1),
        "later auto_restart rollback must supersede an earlier real stop receipt"
    );
    assert_eq!(
        item(&db.pool, campaign, stale_real_stop_sbx).await?,
        (
            "skipped".to_owned(),
            Some(super::super::force_restarts::EXTERNAL_STOP_SUPERSEDED.to_owned()),
            1,
        ),
        "real pre-dispatch stop on stale started row must still veto before stopping"
    );
    assert_eq!(
        dispatcher.recorded_stop(campaign, stale_real_stop_sbx),
        None,
        "pre-dispatch external stop veto must not leave InitiatedStop recorded"
    );
    assert_eq!(
        item(&db.pool, campaign, k8s_converged_evac_sbx).await?,
        ("done".to_owned(), None, 2),
        "converged K8s InitiatedStop with pinned_stop_disk must adopt stop and boot onto target"
    );
    Ok(())
}
