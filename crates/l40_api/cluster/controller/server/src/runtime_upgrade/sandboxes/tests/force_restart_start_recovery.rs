//! Regression coverage for `sandbox_force_restarts` start-claim recovery,
//! lease-fenced target-start receipts, `TargetStart::NotOnTarget` vs
//! `TargetStart::Declined` recovery, and execution-fenced fake stop settlement.

use std::collections::HashMap;
use std::time::Duration;

use pplx_space_k8s_ids::PodUid;
use pplx_space_runtime_environment_policy_store::RuntimeEnvironmentState;
use pplx_space_runtime_upgrade_campaign_store::{self as store, CampaignPhase, SubjectKind};
use pplx_space_sandbox_model::SandboxLifecycle;
use pplx_space_sql_locator::sql;
use pplx_space_test_env::TestResult;
use uuid::Uuid;

use super::super::force_restarts::POLICY_STOPPED_FORCE_RESTARTING;
use super::*;

async fn insert_no_canary_force_restart_campaign(pool: &PgPool) -> TestResult<CampaignId> {
    Ok(store::create_campaign(
        pool,
        &store::NewCampaign {
            kind: CampaignKind::SandboxForceRestarts,
            from: source(),
            to: target(),
            canary: false,
            max_in_flight_per_node: 5,
            max_in_flight_per_cluster: 5,
            failure_budget: 5,
            include_running: false,
            reason: "force restart start recovery test".to_owned(),
            created_by: None,
        },
    )
    .await?)
}

#[tokio::test(flavor = "current_thread")]
async fn undispatched_starting_recovers_and_completes_while_dispatched_starting_waits_for_node()
-> TestResult {
    let Some(db) = test_db().await? else {
        return Ok(());
    };
    seed_policy(&db.pool, RuntimeEnvironmentState::Deprecated, true).await?;
    let host = node(&db.pool).await?;
    let node_b = node(&db.pool).await?;

    let canary_done = resident_on_source(&db.pool, SandboxLifecycle::Started, host).await?;
    let undispatched = resident_on_source(&db.pool, SandboxLifecycle::Created, host).await?;
    let dispatched = resident_on_source(&db.pool, SandboxLifecycle::Created, node_b).await?;
    let campaign = insert_no_canary_force_restart_campaign(&db.pool).await?;

    let nodes = HashMap::from([
        (canary_done, host),
        (undispatched, host),
        (dispatched, node_b),
    ]);
    let memory = FakeMemoryRetirement::retired(source());
    let dispatcher = FakeDispatcher::with_nodes(nodes)
        .persisting(db.pool.clone())
        .with_memory_retirement(memory.clone());
    dispatcher
        .state()
        .undispatched_start_deadline_advances
        .insert(undispatched, Duration::from_secs(35));
    dispatcher
        .state()
        .post_claim_start_deadline_advances
        .insert(dispatched, Duration::from_secs(35));

    let controller = force_restart_reconciler(&db.pool, dispatcher.clone(), memory.clone());
    tick(&controller).await;
    assert_eq!(item(&db.pool, campaign, canary_done).await?.0, "done");

    let make_deadline_controller = || {
        Reconciler::new(
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
        )
    };

    // 1. `undispatched` stops, claims `'stopped' -> 'starting'`, records
    //    `StartingOnTarget`, and cancels on deadline BEFORE node dispatch.
    set_lifecycle(&db.pool, undispatched, SandboxLifecycle::Started.as_str()).await?;
    tick(&make_deadline_controller()).await;
    assert_eq!(
        item(&db.pool, campaign, undispatched).await?,
        (
            "failed".to_owned(),
            Some("while starting on target: the upgrade didn't finish within 30s".to_owned()),
            1,
        )
    );
    assert!(matches!(
        dispatcher.recorded_stop(campaign, undispatched),
        Some(RecordedStop::StartingOnTarget(_))
    ));
    assert!(
        !dispatcher
            .state()
            .node_dispatched_campaign_starts
            .contains_key(&(campaign, undispatched))
    );
    assert_eq!(
        store::subject_sandbox(&db.pool, undispatched)
            .await?
            .expect("undispatched sandbox")
            .lifecycle,
        "starting"
    );

    // 2. `dispatched` stops, claims `'stopped' -> 'starting'`, dispatches the
    //    node start RPC, and cancels on deadline while the node boot is in flight.
    set_lifecycle(&db.pool, dispatched, SandboxLifecycle::Started.as_str()).await?;
    tick(&make_deadline_controller()).await;
    assert_eq!(
        item(&db.pool, campaign, dispatched).await?,
        (
            "failed".to_owned(),
            Some("while starting on target: the upgrade didn't finish within 30s".to_owned()),
            1,
        )
    );
    assert!(
        dispatcher
            .state()
            .node_dispatched_campaign_starts
            .contains_key(&(campaign, dispatched))
    );

    // 3. Re-enqueue both failed items (`retry_failed = true`), drop the catalog
    //    upgrade edge so the campaign enters `Plan::Stranded`, and mark `node_b`
    //    unreachable: `undispatched` (still `'starting'` on `source()` on reachable
    //    `host`) is selected via `starting_on_target`, rolls `'starting'` back to
    //    `'stopped'`, clears `starting_on_target`, and releases without re-spinning
    //    on a second stranded tick, while `dispatched` on unreachable `node_b` waits
    //    without burning attempts. Restoring `node_b` + the catalog edge lets
    //    `undispatched` finish booting onto `target()` (`done`).
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
    assert_eq!(resumed.phase, CampaignPhase::Rolling);

    upsert_snapshot(
        &db.pool,
        node_b,
        SnapshotSeed {
            reachable: false,
            ..SnapshotSeed::default()
        },
    )
    .await?;
    sqlx::query(sql!(
        "UPDATE runtime_environments
         SET upgrade_edges = '{}'
         WHERE environment_ref = $1"
    ))
    .bind(source().to_string())
    .execute(&db.pool)
    .await?;

    tick(&controller).await;
    assert_eq!(
        store::subject_sandbox(&db.pool, undispatched)
            .await?
            .expect("undispatched sandbox rolled back under stranded plan")
            .lifecycle,
        "stopped"
    );
    assert!(matches!(
        dispatcher.recorded_stop(campaign, undispatched),
        Some(RecordedStop::Stopped(_))
    ));
    assert_eq!(
        item(&db.pool, campaign, undispatched).await?.0,
        "pending",
        "undispatched starting rollback under Stranded must release until catalog edge returns"
    );
    assert_eq!(
        item(&db.pool, campaign, dispatched).await?,
        (
            "pending".to_owned(),
            Some("while starting on target: the upgrade didn't finish within 30s".to_owned()),
            0,
        ),
        "dispatched starting sandbox on unreachable node_b must wait without burning attempts"
    );

    let calls_before_second_stranded_tick = dispatcher.state().calls.len();
    tick(&controller).await;
    assert_eq!(
        dispatcher.state().calls.len(),
        calls_before_second_stranded_tick,
        "cleared starting_on_target must prevent re-selecting rolled-back stopped sandbox while Stranded"
    );

    upsert_snapshot(&db.pool, node_b, SnapshotSeed::default()).await?;
    sqlx::query(sql!(
        "UPDATE runtime_environments
         SET upgrade_edges = ARRAY[$2]
         WHERE environment_ref = $1"
    ))
    .bind(source().to_string())
    .bind(target().to_string())
    .execute(&db.pool)
    .await?;

    tick(&controller).await;

    assert_eq!(
        item(&db.pool, campaign, undispatched).await?,
        ("done".to_owned(), None, 1),
        "undispatched `'starting'` sandbox must roll back to `'stopped'` and finish booting onto target"
    );
    let undispatched_after = store::subject_sandbox(&db.pool, undispatched)
        .await?
        .expect("undispatched sandbox after recovery");
    assert_eq!(undispatched_after.lifecycle, "started");

    let (dispatched_phase, _, dispatched_attempts) = item(&db.pool, campaign, dispatched).await?;
    assert_eq!(
        (dispatched_phase.as_str(), dispatched_attempts),
        ("pending", 0),
        "node-dispatched `'starting'` sandbox must stay `'starting'` and release without rolling back"
    );
    assert_eq!(
        store::subject_sandbox(&db.pool, dispatched)
            .await?
            .expect("dispatched sandbox still starting")
            .lifecycle,
        "starting"
    );
    assert!(matches!(
        dispatcher.recorded_stop(campaign, dispatched),
        Some(RecordedStop::StartingOnTarget(_))
    ));

    // 4. Node settles `dispatched` onto `target()` via `internal-api`; the next
    //    tick certifies `done` and completes the campaign.
    dispatcher
        .settle_lingering_start(&db.pool, dispatched, target())
        .await?;
    tick_until_finished(&controller, &db.pool, campaign, 6).await?;
    assert_eq!(campaign_phase(&db.pool, campaign).await?, "completed");
    assert_eq!(
        item(&db.pool, campaign, dispatched).await?,
        ("done".to_owned(), None, 1)
    );

    Ok(())
}

#[tokio::test]
async fn stale_worker_completion_cannot_certify_reopened_run_and_unreachable_settled_receipt_completes()
-> TestResult {
    let Some(db) = test_db().await? else {
        return Ok(());
    };
    seed_policy(&db.pool, RuntimeEnvironmentState::Deprecated, true).await?;
    let host = node(&db.pool).await?;
    let unreachable_host = node(&db.pool).await?;

    let reopened_sbx = resident_on_source(&db.pool, SandboxLifecycle::Started, host).await?;
    let unreachable_settled =
        resident_on_source(&db.pool, SandboxLifecycle::Started, unreachable_host).await?;
    let campaign = insert_no_canary_force_restart_campaign(&db.pool).await?;
    record(&db.pool, campaign, &[reopened_sbx, unreachable_settled]).await?;

    let nodes = HashMap::from([
        (reopened_sbx, host),
        (unreachable_settled, unreachable_host),
    ]);
    let memory = FakeMemoryRetirement::retired(source());
    let dispatcher = FakeDispatcher::with_nodes(nodes)
        .persisting(db.pool.clone())
        .with_memory_retirement(memory.clone());
    let driver =
        SandboxForceRestartDriver::new(db.pool.clone(), K8S, dispatcher.clone(), memory.clone());
    let campaign_row = store::list_unfinished(&db.pool)
        .await?
        .into_iter()
        .find(|listed| listed.id == campaign)
        .expect("campaign row");

    // 1. Run 1 on `reopened_sbx` stops the sandbox and enters `StartingOnTarget`,
    //    while `unreachable_settled` completes its target start (`started_on_target = TRUE`)
    //    before its worker crashes (leaving the item `in_flight` to lapse to `pending`).
    dispatcher.script_post_claim_start_error(
        reopened_sbx,
        DispatchError::Retryable("start RPC timed out".to_owned()),
    );
    let leased_initial = store::lease_selected_items(
        &db.pool,
        campaign,
        &[reopened_sbx, unreachable_settled],
        Duration::from_secs(60),
        2,
    )
    .await?;
    assert_eq!(leased_initial.len(), 2);
    let leased_reopened_1 = leased_initial
        .iter()
        .find(|leased| leased.subject_id == reopened_sbx)
        .expect("leased reopened_sbx")
        .clone();
    let leased_unreachable = leased_initial
        .iter()
        .find(|leased| leased.subject_id == unreachable_settled)
        .expect("leased unreachable_settled")
        .clone();
    assert!(store::mark_run_started(&db.pool, &leased_reopened_1).await?);
    assert!(store::mark_run_started(&db.pool, &leased_unreachable).await?);

    let retry_outcome =
        crate::runtime_upgrade::CampaignDriver::upgrade(&driver, &campaign_row, &leased_reopened_1)
            .await;
    assert!(matches!(retry_outcome, store::ItemOutcome::Retry(_)));
    let stale_stop_fence = dispatcher
        .recorded_stop(campaign, reopened_sbx)
        .and_then(RecordedStop::into_fence)
        .expect("StartingOnTarget fence from run 1");

    let settled_outcome = crate::runtime_upgrade::CampaignDriver::upgrade(
        &driver,
        &campaign_row,
        &leased_unreachable,
    )
    .await;
    assert_eq!(settled_outcome, store::ItemOutcome::Done);
    assert_eq!(
        store::started_on_target(&db.pool, &leased_unreachable).await?,
        Some(true)
    );
    dispatcher
        .state()
        .item_stopped_at
        .remove(&(campaign, unreachable_settled));

    // Simulate `leased_reopened_1` and `leased_unreachable` both losing their
    // leases before `finish_item` runs (`redrive_expired` returns both to `pending`).
    sqlx::query(sql!(
        "UPDATE runtime_upgrade_items
         SET lease_expires_at = NOW() - INTERVAL '1 second'
         WHERE campaign_id = $1"
    ))
    .bind(campaign)
    .execute(&db.pool)
    .await?;
    store::redrive_expired(&db.pool, campaign, MAX_ATTEMPTS).await?;

    // 2. An external stop supersedes run 1 on `reopened_sbx`, so retry 2
    //    finishes `Skipped` (resetting `started_on_target = FALSE` and clearing
    //    `StartingOnTarget`). Then the user restarts `reopened_sbx` back onto
    //    `source()`, and a subject scan reopens `reopened_sbx` as `pending`.
    {
        use pplx_space_public_api::{
            OperationKind, OperationState, OperationTargetType, PublicSandboxId,
        };
        let target_id = PublicSandboxId::from(SandboxId::from_uuid(reopened_sbx)).to_string();
        sqlx::query(sql!(
            "INSERT INTO public_operations (
                 operation_id, org_id, actor_user_id, kind, target_type, target_id,
                 state, requested_at, completed_at
             )
             VALUES ($1, $2, $3, $4, $5, $6, $7, NOW(), NOW())"
        ))
        .bind(Uuid::now_v7())
        .bind(SEED_ORG_ID)
        .bind(SEED_USER_ID)
        .bind(OperationKind::KillSandbox.as_str())
        .bind(OperationTargetType::Sandbox.as_str())
        .bind(target_id)
        .bind(OperationState::Succeeded.as_str())
        .execute(&db.pool)
        .await?;
    }
    set_lifecycle(&db.pool, reopened_sbx, SandboxLifecycle::Stopped.as_str()).await?;
    let leased_skip = store::lease_selected_items(
        &db.pool,
        campaign,
        &[reopened_sbx],
        Duration::from_secs(60),
        1,
    )
    .await?;
    assert!(store::mark_run_started(&db.pool, &leased_skip[0]).await?);
    let skip_outcome =
        crate::runtime_upgrade::CampaignDriver::upgrade(&driver, &campaign_row, &leased_skip[0])
            .await;
    assert!(matches!(skip_outcome, store::ItemOutcome::Skipped(_)));
    assert!(store::finish_item(&db.pool, &leased_skip[0], &skip_outcome).await?);
    assert_eq!(dispatcher.recorded_stop(campaign, reopened_sbx), None);

    set_lifecycle(&db.pool, reopened_sbx, SandboxLifecycle::Started.as_str()).await?;
    set_bound(&db.pool, reopened_sbx, source()).await?;
    record(&db.pool, campaign, &[reopened_sbx]).await?;

    // 3. Lease `reopened_sbx` for fresh run 2 (`leased_reopened_2`, `in_flight`),
    //    then wake delayed run 1 (`leased_reopened_1`) so it attempts both fake
    //    and store completion writes against the active run 2 row: neither may
    //    certify run 2 as `started_on_target` or `StartedOnTarget`.
    let leased_reopened_2 = store::lease_selected_items(
        &db.pool,
        campaign,
        &[reopened_sbx],
        Duration::from_secs(60),
        1,
    )
    .await?;
    assert_eq!(leased_reopened_2.len(), 1);
    dispatcher
        .record_item_started_on_target(
            campaign,
            SubjectKind::Sandbox,
            reopened_sbx,
            stale_stop_fence,
        )
        .await;
    assert!(!store::mark_leased_started_on_target(&db.pool, &leased_reopened_1).await?);
    assert_eq!(dispatcher.recorded_stop(campaign, reopened_sbx), None);
    assert_eq!(
        store::continuing_run(&db.pool, &leased_reopened_2[0]).await?,
        Some(false)
    );
    assert_eq!(
        store::started_on_target(&db.pool, &leased_reopened_2[0]).await?,
        Some(false),
        "stale worker from run 1 must not certify unstarted reopened run 2 as started_on_target"
    );
    assert!(store::mark_run_started(&db.pool, &leased_reopened_2[0]).await?);
    assert!(
        !store::mark_leased_started_on_target(&db.pool, &leased_reopened_1).await?,
        "stale lease token from run 1 must not certify started run 2 as started_on_target"
    );
    assert_eq!(
        store::started_on_target(&db.pool, &leased_reopened_2[0]).await?,
        Some(false)
    );
    assert!(
        store::finish_item(
            &db.pool,
            &leased_reopened_2[0],
            &store::ItemOutcome::Release(store::ItemError::new("release before stranded check")),
        )
        .await?
    );

    // 4. Mark `unreachable_host` unreachable and drop the catalog upgrade edge so
    //    the campaign is `Stranded`: `unreachable_settled` still finishes `done`
    //    from its durable `started_on_target` receipt, while `reopened_sbx` stays
    //    `pending` (requiring a genuine run 2 stop+start once the edge returns).
    upsert_snapshot(
        &db.pool,
        unreachable_host,
        SnapshotSeed {
            reachable: false,
            ..SnapshotSeed::default()
        },
    )
    .await?;
    sqlx::query(sql!(
        "UPDATE runtime_environments
         SET upgrade_edges = '{}'
         WHERE environment_ref = $1"
    ))
    .bind(source().to_string())
    .execute(&db.pool)
    .await?;

    let controller = force_restart_reconciler(&db.pool, dispatcher, memory);
    tick(&controller).await;
    assert_eq!(
        item(&db.pool, campaign, unreachable_settled).await?.0,
        "done",
        "settled target-start receipt must finish `done` even when its publishing node is unreachable"
    );
    assert_eq!(
        item(&db.pool, campaign, reopened_sbx).await?.0,
        "pending",
        "reopened item on source must stay pending under a stranded plan"
    );

    Ok(())
}

#[tokio::test]
async fn not_on_target_runs_admit_start_when_stop_still_owned_while_declined_skips() -> TestResult {
    let Some(db) = test_db().await? else {
        return Ok(());
    };
    seed_policy(&db.pool, RuntimeEnvironmentState::Deprecated, true).await?;
    let host = node(&db.pool).await?;

    let transient_not_on_target =
        resident_on_source(&db.pool, SandboxLifecycle::Started, host).await?;
    let held_not_on_target = resident_on_source(&db.pool, SandboxLifecycle::Started, host).await?;
    let permanent_declined = resident_on_source(&db.pool, SandboxLifecycle::Started, host).await?;

    let campaign = insert_no_canary_force_restart_campaign(&db.pool).await?;
    record(
        &db.pool,
        campaign,
        &[
            transient_not_on_target,
            held_not_on_target,
            permanent_declined,
        ],
    )
    .await?;

    let nodes = HashMap::from([
        (transient_not_on_target, host),
        (held_not_on_target, host),
        (permanent_declined, host),
    ]);
    let memory = FakeMemoryRetirement::retired(source());
    let dispatcher = FakeDispatcher::with_nodes(nodes)
        .persisting(db.pool.clone())
        .with_memory_retirement(memory.clone());

    dispatcher.script_start(
        transient_not_on_target,
        vec![Ok(TargetStart::NotOnTarget(
            "transient CAS miss before start claim".to_owned(),
        ))],
    );
    dispatcher.script_start(
        permanent_declined,
        vec![Ok(TargetStart::Declined(
            "template release permanently incompatible".to_owned(),
        ))],
    );

    // Drive `transient_not_on_target` and `permanent_declined` first while `source()`
    // is `Deprecated`.
    let driver =
        SandboxForceRestartDriver::new(db.pool.clone(), K8S, dispatcher.clone(), memory.clone());
    let campaign_row = store::list_unfinished(&db.pool)
        .await?
        .into_iter()
        .find(|listed| listed.id == campaign)
        .expect("campaign row");
    let leased_pair = store::lease_selected_items(
        &db.pool,
        campaign,
        &[transient_not_on_target, permanent_declined],
        Duration::from_secs(60),
        2,
    )
    .await?;
    for leased_item in &leased_pair {
        assert!(store::mark_run_started(&db.pool, leased_item).await?);
        let outcome =
            crate::runtime_upgrade::CampaignDriver::upgrade(&driver, &campaign_row, leased_item)
                .await;
        assert!(store::finish_item(&db.pool, leased_item, &outcome).await?);
    }

    assert_eq!(
        item(&db.pool, campaign, permanent_declined).await?,
        (
            "skipped".to_owned(),
            Some("template release permanently incompatible".to_owned()),
            1,
        )
    );
    assert_eq!(dispatcher.recorded_stop(campaign, permanent_declined), None);

    assert_eq!(
        item(&db.pool, campaign, transient_not_on_target).await?,
        (
            "pending".to_owned(),
            Some("transient CAS miss before start claim".to_owned()),
            0,
        ),
        "TargetStart::NotOnTarget with intact stop ownership must run admit_start and release, not skip"
    );
    assert!(matches!(
        dispatcher.recorded_stop(campaign, transient_not_on_target),
        Some(RecordedStop::Stopped(_))
    ));

    // Now test `held_not_on_target` when `source()` flips to `Allowed` right as
    // `dispatcher.start` returns `TargetStart::NotOnTarget`: `admit_start` sees
    // the temporary policy hold and releases with `POLICY_STOPPED_FORCE_RESTARTING`
    // while keeping stop ownership intact.
    dispatcher
        .state()
        .allow_source_before_start
        .insert(held_not_on_target);
    dispatcher.script_start(
        held_not_on_target,
        vec![Ok(TargetStart::NotOnTarget(
            "stale snapshot before start claim".to_owned(),
        ))],
    );
    let leased_held = store::lease_selected_items(
        &db.pool,
        campaign,
        &[held_not_on_target],
        Duration::from_secs(60),
        1,
    )
    .await?;
    assert!(store::mark_run_started(&db.pool, &leased_held[0]).await?);
    let held_outcome =
        crate::runtime_upgrade::CampaignDriver::upgrade(&driver, &campaign_row, &leased_held[0])
            .await;
    assert_eq!(
        held_outcome,
        store::ItemOutcome::Release(store::ItemError::new(POLICY_STOPPED_FORCE_RESTARTING))
    );
    assert!(store::finish_item(&db.pool, &leased_held[0], &held_outcome).await?);
    assert!(matches!(
        dispatcher.recorded_stop(campaign, held_not_on_target),
        Some(RecordedStop::Stopped(_))
    ));

    // Restore `source()` to `Deprecated`; both released owned-stop items finish `done`.
    let mut tx = db.pool.begin().await?;
    pplx_space_runtime_environment_policy_store::set_environment_state(
        &mut *tx,
        source(),
        RuntimeEnvironmentState::Deprecated,
        &change(),
    )
    .await?;
    tx.commit().await?;

    let controller = force_restart_reconciler(&db.pool, dispatcher, memory);
    tick(&controller).await;
    assert_eq!(
        item(&db.pool, campaign, transient_not_on_target).await?,
        ("done".to_owned(), None, 1)
    );
    assert_eq!(
        item(&db.pool, campaign, held_not_on_target).await?,
        ("done".to_owned(), None, 1)
    );

    Ok(())
}

#[tokio::test]
async fn fake_stop_settlement_guards_episode_timestamp_and_k8s_execution_identity() -> TestResult {
    let Some(db) = test_db().await? else {
        return Ok(());
    };
    seed_policy(&db.pool, RuntimeEnvironmentState::Deprecated, true).await?;
    let host = node(&db.pool).await?;

    let direct_replaced = insert_sandbox(
        &db.pool,
        SubstrateKind::Space,
        SandboxLifecycle::Started,
        host,
        Published::Bound(source()),
    )
    .await?;
    let k8s_pod_replaced = resident_on_source(&db.pool, SandboxLifecycle::Started, host).await?;
    let k8s_pod_uid_updated_in_place =
        resident_on_source(&db.pool, SandboxLifecycle::Started, host).await?;

    let campaign = insert_no_canary_force_restart_campaign(&db.pool).await?;
    record(
        &db.pool,
        campaign,
        &[
            direct_replaced,
            k8s_pod_replaced,
            k8s_pod_uid_updated_in_place,
        ],
    )
    .await?;

    let nodes = HashMap::from([
        (direct_replaced, host),
        (k8s_pod_replaced, host),
        (k8s_pod_uid_updated_in_place, host),
    ]);
    let memory = FakeMemoryRetirement::retired(source());
    let dispatcher = FakeDispatcher::with_nodes(nodes)
        .persisting(db.pool.clone())
        .with_memory_retirement(memory.clone());

    let replacement_pod = StoppedK8sExecution {
        generation: 2,
        pod_uid: PodUid::parse("pod-replacement-2").expect("valid pod uid"),
    };
    let in_place_pod_uid = PodUid::parse("pod-in-place-uid-2").expect("valid pod uid");
    dispatcher
        .state()
        .in_flight_stop_episode_replacements
        .insert(direct_replaced);
    dispatcher
        .state()
        .replace_k8s_pod_during_in_flight_stop
        .insert(k8s_pod_replaced, replacement_pod.clone());
    dispatcher
        .state()
        .update_active_k8s_pod_uid_during_in_flight_stop
        .insert(k8s_pod_uid_updated_in_place, in_place_pod_uid.clone());

    let direct_driver = SandboxForceRestartDriver::new(
        db.pool.clone(),
        SubstrateKind::Space,
        dispatcher.clone(),
        memory.clone(),
    );
    let k8s_driver =
        SandboxForceRestartDriver::new(db.pool.clone(), K8S, dispatcher.clone(), memory.clone());
    let campaigns = store::list_unfinished(&db.pool).await?;
    let campaign_row = campaigns
        .iter()
        .find(|listed| listed.id == campaign)
        .expect("campaign");

    // 1. Direct sandbox whose `lifecycle_changed_at` advanced to a new running
    //    episode while `stop` was in flight must NOT be stopped by the stale
    //    settlement `UPDATE`, and `recover_after_declined_stop` leaves it running
    //    with no recorded `StopFence` (skipping the superseded attempt cleanly).
    let leased_direct = store::lease_selected_items(
        &db.pool,
        campaign,
        &[direct_replaced],
        Duration::from_secs(60),
        1,
    )
    .await?;
    assert!(store::mark_run_started(&db.pool, &leased_direct[0]).await?);
    let direct_outcome = crate::runtime_upgrade::CampaignDriver::upgrade(
        &direct_driver,
        campaign_row,
        &leased_direct[0],
    )
    .await;
    assert!(matches!(direct_outcome, store::ItemOutcome::Skipped(_)));
    assert!(store::finish_item(&db.pool, &leased_direct[0], &direct_outcome).await?);
    assert_eq!(
        store::subject_sandbox(&db.pool, direct_replaced)
            .await?
            .expect("direct sandbox still running")
            .lifecycle,
        "started"
    );
    assert_eq!(dispatcher.recorded_stop(campaign, direct_replaced), None);

    // 2. Kubernetes sandbox whose active pod switched to `(2, "pod-replacement-2")`
    //    while `stop` was in flight (even at the same `lifecycle_changed_at`) must
    //    NOT be stopped or have `"pod-replacement-2"` adopted into `StopFence`.
    let leased_k8s = store::lease_selected_items(
        &db.pool,
        campaign,
        &[k8s_pod_replaced],
        Duration::from_secs(60),
        1,
    )
    .await?;
    assert!(store::mark_run_started(&db.pool, &leased_k8s[0]).await?);
    let k8s_outcome =
        crate::runtime_upgrade::CampaignDriver::upgrade(&k8s_driver, campaign_row, &leased_k8s[0])
            .await;
    assert!(matches!(k8s_outcome, store::ItemOutcome::Skipped(_)));
    assert!(store::finish_item(&db.pool, &leased_k8s[0], &k8s_outcome).await?);
    assert_eq!(
        store::subject_sandbox(&db.pool, k8s_pod_replaced)
            .await?
            .expect("k8s sandbox still running")
            .lifecycle,
        "started"
    );
    assert_eq!(
        active_k8s_stopped_execution_for_test(&db.pool, k8s_pod_replaced).await,
        Some(replacement_pod)
    );
    assert_eq!(dispatcher.recorded_stop(campaign, k8s_pod_replaced), None);

    // 2b. Kubernetes sandbox whose active `pod_uid` changed in place at unchanged
    //     `generation = 1` and `lifecycle_changed_at` while `stop` was in flight
    //     must also decline without stopping the sandbox or adopting the new UID.
    let leased_in_place = store::lease_selected_items(
        &db.pool,
        campaign,
        &[k8s_pod_uid_updated_in_place],
        Duration::from_secs(60),
        1,
    )
    .await?;
    assert!(store::mark_run_started(&db.pool, &leased_in_place[0]).await?);
    let in_place_outcome = crate::runtime_upgrade::CampaignDriver::upgrade(
        &k8s_driver,
        campaign_row,
        &leased_in_place[0],
    )
    .await;
    assert!(matches!(in_place_outcome, store::ItemOutcome::Skipped(_)));
    assert!(store::finish_item(&db.pool, &leased_in_place[0], &in_place_outcome).await?);
    assert_eq!(
        store::subject_sandbox(&db.pool, k8s_pod_uid_updated_in_place)
            .await?
            .expect("in-place k8s sandbox still running")
            .lifecycle,
        "started"
    );
    assert_eq!(
        active_k8s_stopped_execution_for_test(&db.pool, k8s_pod_uid_updated_in_place).await,
        Some(StoppedK8sExecution {
            generation: 1,
            pod_uid: in_place_pod_uid,
        })
    );
    assert_eq!(
        dispatcher.recorded_stop(campaign, k8s_pod_uid_updated_in_place),
        None
    );

    // 3. Subsequent reconciler passes reopen both Kubernetes sandboxes on `source()`
    //    and cleanly stop+start their current running episodes onto `target()`.
    //    (Delete `direct_replaced` and its item first so the K8S reconciler only
    //    drives K8S subjects.)
    sqlx::query(sql!(
        "DELETE FROM runtime_upgrade_items
         WHERE campaign_id = $1 AND subject_id = $2"
    ))
    .bind(campaign)
    .bind(direct_replaced)
    .execute(&db.pool)
    .await?;
    sqlx::query(sql!("DELETE FROM sandboxes WHERE id = $1"))
        .bind(direct_replaced)
        .execute(&db.pool)
        .await?;
    let k8s_controller = force_restart_reconciler(&db.pool, dispatcher.clone(), memory.clone());
    tick_until_finished(&k8s_controller, &db.pool, campaign, 6).await?;
    assert_eq!(campaign_phase(&db.pool, campaign).await?, "completed");
    assert_eq!(item(&db.pool, campaign, k8s_pod_replaced).await?.0, "done");
    assert_eq!(
        item(&db.pool, campaign, k8s_pod_uid_updated_in_place)
            .await?
            .0,
        "done"
    );

    Ok(())
}
