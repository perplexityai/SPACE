//! What the driver plans and finds: holds, stranded sources, attribution
//! through the catalog, and the sandboxes that hold a campaign open.

use super::*;

#[tokio::test(flavor = "current_thread")]
async fn a_campaign_is_held_while_the_policy_doesnt_call_for_its_move() -> TestResult {
    for (default_is_target, state, reason) in [
        (
            false,
            RuntimeEnvironmentState::Deprecated,
            "target_not_default",
        ),
        (true, RuntimeEnvironmentState::Allowed, "source_allowed"),
    ] {
        let Some(db) = test_db().await? else {
            return Ok(());
        };
        seed_policy(&db.pool, state, true).await?;
        if !default_is_target {
            let mut tx = db.pool.begin().await?;
            pplx_space_runtime_environment_policy_store::set_default(
                &mut *tx,
                environment(5),
                &change(),
            )
            .await?;
            tx.commit().await?;
        }
        let campaign = insert_campaign(&db.pool, seed()).await?;
        let node = node(&db.pool).await?;
        stopped_on_source(&db.pool, node).await?;
        let dispatcher = FakeDispatcher::default();
        let reconciler = reconciler(&db.pool, dispatcher.clone());

        tick(&reconciler).await;
        tick(&reconciler).await;

        assert_eq!(
            counts(&db.pool, campaign).await?,
            ItemCounts::default(),
            "{reason}"
        );
        assert!(dispatcher.calls().is_empty());
        assert_eq!(campaign_phase(&db.pool, campaign).await?, "rolling");
        let listed = store::list_unfinished(&db.pool)
            .await?
            .into_iter()
            .find(|listed| listed.id == campaign)
            .expect("the campaign is unfinished");
        let driver = SandboxDriver::new(db.pool.clone(), K8S, FakeDispatcher::default());
        assert_eq!(driver.plan(&listed).await?, Plan::Hold(reason));
    }
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn sandboxes_without_an_edge_are_recorded_stranded_and_the_campaign_stays_open() -> TestResult
{
    for state in [
        RuntimeEnvironmentState::Blocked,
        RuntimeEnvironmentState::Deprecated,
    ] {
        let Some(db) = test_db().await? else {
            return Ok(());
        };
        seed_policy(&db.pool, state, false).await?;
        let campaign = insert_campaign(&db.pool, seed()).await?;
        let node = node(&db.pool).await?;
        for _ in 0..3 {
            stopped_on_source(&db.pool, node).await?;
        }
        // Recorded while an edge reached the target, which has since gone.
        let recorded = stopped_on_source(&db.pool, node).await?;
        record(&db.pool, campaign, &[recorded]).await?;
        let dispatcher = FakeDispatcher::default();
        let reconciler = reconciler(&db.pool, dispatcher.clone());

        for _ in 0..3 {
            tick(&reconciler).await;
        }

        assert_eq!(
            campaign_phase(&db.pool, campaign).await?,
            "rolling",
            "{state}: a campaign that moves nothing stays open"
        );
        assert_eq!(
            counts(&db.pool, campaign).await?,
            ItemCounts {
                pending: 4,
                ..ItemCounts::default()
            },
            "{state}: every sandbox on the source is pending, and none is leased"
        );
        assert!(dispatcher.calls().is_empty());
    }
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn unbound_executions_are_attributed_through_the_catalog() -> TestResult {
    let Some(db) = test_db().await? else {
        return Ok(());
    };
    seed_policy(&db.pool, RuntimeEnvironmentState::Deprecated, true).await?;
    let campaign = insert_campaign(&db.pool, seed()).await?;
    let node = node(&db.pool).await?;
    // Matches the source's catalog row exactly.
    let matching = insert_sandbox(
        &db.pool,
        K8S,
        SandboxLifecycle::Stopped,
        node,
        Published::Unbound {
            runtime_version: "1.15.1",
            kernel: "6.1.155",
            state_format_version: "9.0.0",
        },
    )
    .await?;
    // Another kernel matches no catalog row.
    unattributable(&db.pool, node, "1.15.1").await?;
    let dispatcher = FakeDispatcher::default();
    let reconciler = reconciler(&db.pool, dispatcher.clone());

    tick_until_finished(&reconciler, &db.pool, campaign, 5).await?;

    assert_eq!(campaign_phase(&db.pool, campaign).await?, "completed");
    assert_eq!(dispatcher.upgraded(), vec![matching]);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn a_sandbox_skipped_because_it_had_started_is_upgraded_once_it_stops() -> TestResult {
    let Some(db) = test_db().await? else {
        return Ok(());
    };
    seed_policy(&db.pool, RuntimeEnvironmentState::Deprecated, true).await?;
    let campaign = insert_campaign(&db.pool, seed()).await?;
    let node = node(&db.pool).await?;
    let sandbox = stopped_on_source(&db.pool, node).await?;
    record(&db.pool, campaign, &[sandbox]).await?;
    // It started between being found and being leased.
    set_lifecycle(&db.pool, sandbox, "started").await?;
    let dispatcher = FakeDispatcher::default();
    let reconciler = reconciler(&db.pool, dispatcher.clone());

    tick(&reconciler).await;
    assert_eq!(item(&db.pool, campaign, sandbox).await?.0, "skipped");
    assert!(dispatcher.calls().is_empty());

    // Stopped again, the next pass finds it on the source once more.
    set_lifecycle(&db.pool, sandbox, "stopped").await?;
    tick_until_finished(&reconciler, &db.pool, campaign, 5).await?;

    assert_eq!(dispatcher.upgraded(), vec![sandbox]);
    assert_eq!(
        item(&db.pool, campaign, sandbox).await?,
        ("done".to_owned(), None, 2),
        "the skipped lease still counts"
    );
    assert_eq!(campaign_phase(&db.pool, campaign).await?, "completed");
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn a_sandbox_the_node_keeps_declining_is_dispatched_a_bounded_number_of_times() -> TestResult
{
    let Some(db) = test_db().await? else {
        return Ok(());
    };
    seed_policy(&db.pool, RuntimeEnvironmentState::Deprecated, true).await?;
    let campaign = insert_campaign(&db.pool, seed()).await?;
    let node = node(&db.pool).await?;
    let declined = stopped_on_source(&db.pool, node).await?;
    let dispatcher = FakeDispatcher::default();
    dispatcher.answer_all(Ok(StoppedUpgrade::NotMoved(
        "the node finds no certified edge".to_owned(),
    )));
    let reconciler = reconciler(&db.pool, dispatcher.clone());

    let ticks =
        tick_until_finished(&reconciler, &db.pool, campaign, 2 * MAX_ATTEMPTS as usize).await?;

    assert_eq!(campaign_phase(&db.pool, campaign).await?, "completed");
    assert_eq!(dispatcher.upgraded().len(), MAX_ATTEMPTS as usize);
    assert_eq!(ticks, MAX_ATTEMPTS as usize + 1);
    assert_eq!(item(&db.pool, campaign, declined).await?.0, "skipped");
    Ok(())
}

/// Seed `campaign`'s item for `sandbox` as skipped after one lease.
async fn skipped_once(pool: &PgPool, campaign: CampaignId, sandbox: Uuid) -> TestResult {
    sqlx::query(sql!(
        "INSERT INTO runtime_upgrade_items (campaign_id, subject_kind, subject_id, phase, attempts)
         VALUES ($1, 'sandbox', $2, 'skipped', 1)"
    ))
    .bind(campaign)
    .bind(sandbox)
    .execute(pool)
    .await?;
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn a_skipped_sandbox_the_catalog_attributes_elsewhere_isnt_retired_or_waited_on() -> TestResult
{
    let Some(db) = test_db().await? else {
        return Ok(());
    };
    seed_policy(&db.pool, RuntimeEnvironmentState::Deprecated, true).await?;
    let campaign = insert_campaign(&db.pool, seed()).await?;
    let node = node(&db.pool).await?;
    // Skipped while it was on the source; its unbound execution now matches
    // the target's catalog row.
    let sandbox = insert_sandbox(
        &db.pool,
        K8S,
        SandboxLifecycle::Stopped,
        node,
        Published::Unbound {
            runtime_version: "1.17.0",
            kernel: "6.1.155",
            state_format_version: "12.0.0",
        },
    )
    .await?;
    skipped_once(&db.pool, campaign, sandbox).await?;
    let dispatcher = FakeDispatcher::default();
    let reconciler = reconciler(&db.pool, dispatcher.clone());

    let report = tick(&reconciler).await;

    assert_eq!(report.completed, vec![campaign]);
    assert_eq!(dispatcher.calls(), Vec::new());
    assert_eq!(
        item(&db.pool, campaign, sandbox).await?,
        ("skipped".to_owned(), None, 1),
        "not retired: a later catalog may place it on the source again"
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn a_skipped_sandbox_the_catalog_attributes_nowhere_is_found_again_once_it_does() -> TestResult
{
    let Some(db) = test_db().await? else {
        return Ok(());
    };
    seed_policy(&db.pool, RuntimeEnvironmentState::Deprecated, true).await?;
    let campaign = insert_campaign(&db.pool, seed()).await?;
    let node = node(&db.pool).await?;
    // Holds the campaign open: its first upgrade is retried.
    let other = stopped_on_source(&db.pool, node).await?;
    // Skipped while the catalog put it on the source; kernel 5.10.0 matches
    // no catalog row now.
    let unplaced = unattributable(&db.pool, node, "1.15.1").await?;
    skipped_once(&db.pool, campaign, unplaced).await?;
    let dispatcher = FakeDispatcher::default();
    dispatcher.script(
        other,
        vec![Err(DispatchError::Retryable("busy".to_owned()))],
    );
    let reconciler = reconciler(&db.pool, dispatcher.clone());

    tick(&reconciler).await;
    assert_eq!(
        item(&db.pool, campaign, unplaced).await?,
        ("skipped".to_owned(), None, 1),
        "an item the catalog can't place keeps its attempts"
    );
    assert_eq!(campaign_phase(&db.pool, campaign).await?, "rolling");

    // The source's catalog row now names kernel 5.10.0.
    sqlx::query(sql!(
        "UPDATE runtime_environments SET kernel_version = '5.10.0' WHERE environment_ref = $1"
    ))
    .bind(source().to_string())
    .execute(&db.pool)
    .await?;
    tick_until_finished(&reconciler, &db.pool, campaign, 5).await?;

    assert_eq!(campaign_phase(&db.pool, campaign).await?, "completed");
    assert!(dispatcher.upgraded().contains(&unplaced));
    assert_eq!(item(&db.pool, campaign, unplaced).await?.0, "done");
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn a_sandbox_declined_once_on_a_multi_page_pass_is_moved_before_completion() -> TestResult {
    let Some(db) = test_db().await? else {
        return Ok(());
    };
    seed_policy(&db.pool, RuntimeEnvironmentState::Deprecated, true).await?;
    let campaign = insert_campaign(&db.pool, seed()).await?;
    let node = node(&db.pool).await?;
    // In id order: one sandbox on the source, then two whose unbound
    // executions match no catalog environment.
    let first = unattributable_in_order(&db.pool, node, "1.15.1", 3).await?[0];
    sqlx::query(sql!(
        "UPDATE sandbox_observed_states
         SET execution_metadata = jsonb_build_object('environment', $2::text)
         WHERE sandbox_id = $1"
    ))
    .bind(first)
    .bind(source().to_string())
    .execute(&db.pool)
    .await?;
    let dispatcher = FakeDispatcher::default();
    // Declined while its pass goes on to a page with nothing to record.
    dispatcher.script(
        first,
        vec![Ok(StoppedUpgrade::NotMoved("the policy moved".to_owned()))],
    );
    let reconciler = paging(&db.pool, dispatcher.clone(), 2);
    tick(&reconciler).await;
    assert_eq!(item(&db.pool, campaign, first).await?.0, "skipped");
    let pass_end = tick(&reconciler).await;
    assert!(
        pass_end.completed.is_empty(),
        "the pass leased an item, which ended skipped"
    );

    tick_until_finished(&reconciler, &db.pool, campaign, 6).await?;
    assert_eq!(campaign_phase(&db.pool, campaign).await?, "completed");
    assert_eq!(item(&db.pool, campaign, first).await?.0, "done");
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn a_declined_sandbox_is_found_again_and_a_deleted_one_doesnt_hold_completion() -> TestResult
{
    let Some(db) = test_db().await? else {
        return Ok(());
    };
    seed_policy(&db.pool, RuntimeEnvironmentState::Deprecated, true).await?;
    let campaign = insert_campaign(&db.pool, seed()).await?;
    let node = node(&db.pool).await?;
    let retryable = stopped_on_source(&db.pool, node).await?;
    let gone = stopped_on_source(&db.pool, node).await?;
    record(&db.pool, campaign, &[retryable, gone]).await?;
    sqlx::query(sql!("DELETE FROM sandboxes WHERE id = $1"))
        .bind(gone)
        .execute(&db.pool)
        .await?;
    let dispatcher = FakeDispatcher::default();
    dispatcher.script(
        retryable,
        vec![Ok(StoppedUpgrade::NotMoved("the policy moved".to_owned()))],
    );
    let reconciler = reconciler(&db.pool, dispatcher.clone());

    // Both skipped: one declined, one deleted.
    tick(&reconciler).await;
    assert_eq!(item(&db.pool, campaign, retryable).await?.0, "skipped");
    assert_eq!(item(&db.pool, campaign, gone).await?.0, "skipped");
    // The next pass finds the declined one again and moves it; the deleted
    // one isn't a candidate.
    let refound = tick(&reconciler).await;
    assert_eq!(refound.inserted, 1);
    assert!(refound.completed.is_empty());
    assert_eq!(item(&db.pool, campaign, retryable).await?.0, "done");

    // A quiet pass completes the campaign although the deleted sandbox's
    // item stays skipped with leases left.
    let quiet = tick(&reconciler).await;
    assert_eq!(quiet.completed, vec![campaign]);
    assert_eq!(
        item(&db.pool, campaign, gone).await?,
        (
            "skipped".to_owned(),
            Some("the sandbox was deleted".to_owned()),
            1
        )
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn a_catalog_change_mid_pass_restarts_the_pass() -> TestResult {
    let Some(db) = test_db().await? else {
        return Ok(());
    };
    seed_policy(&db.pool, RuntimeEnvironmentState::Deprecated, true).await?;
    let campaign = insert_campaign(&db.pool, seed()).await?;
    let node = node(&db.pool).await?;
    // In id order: one on the source's runtime with kernel 5.10.0, which
    // matches no catalog row yet, then two on another runtime.
    let late = unattributable_in_order(&db.pool, node, "1.16.0", 3).await?[0];
    sqlx::query(sql!(
        "UPDATE sandbox_observed_states SET runtime_version = '1.15.1' WHERE sandbox_id = $1"
    ))
    .bind(late)
    .execute(&db.pool)
    .await?;
    let dispatcher = FakeDispatcher::default();
    let reconciler = paging(&db.pool, dispatcher.clone(), 2);
    tick(&reconciler).await;
    assert!(dispatcher.calls().is_empty());

    // The source's catalog row now names kernel 5.10.0, so `late`, on the
    // pass's first page, is on the source. The plan's moves are unchanged;
    // only its fingerprint of the catalog tells the loop to start over.
    sqlx::query(sql!(
        "UPDATE runtime_environments SET kernel_version = '5.10.0' WHERE environment_ref = $1"
    ))
    .bind(source().to_string())
    .execute(&db.pool)
    .await?;
    let restarted = tick(&reconciler).await;
    assert!(restarted.completed.is_empty());
    assert_eq!(restarted.inserted, 1, "the pass started over");

    tick_until_finished(&reconciler, &db.pool, campaign, 5).await?;
    assert_eq!(campaign_phase(&db.pool, campaign).await?, "completed");
    assert_eq!(dispatcher.upgraded(), vec![late]);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn a_page_isnt_judged_under_inputs_that_changed_since_the_plan() -> TestResult {
    let Some(db) = test_db().await? else {
        return Ok(());
    };
    seed_policy(&db.pool, RuntimeEnvironmentState::Blocked, true).await?;
    let campaign = insert_campaign(
        &db.pool,
        CampaignSeed {
            include_running: true,
            ..seed()
        },
    )
    .await?;
    let node = node(&db.pool).await?;
    stopped_on_source(&db.pool, node).await?;
    let driver = SandboxDriver::new(db.pool.clone(), K8S, FakeDispatcher::default());
    let campaign = store::list_unfinished(&db.pool)
        .await?
        .into_iter()
        .find(|listed| listed.id == campaign)
        .expect("the campaign is unfinished");
    let Plan::Scan(plan) = driver.plan(&campaign).await? else {
        panic!("a blocked source with an edge scans");
    };
    assert!(plan.moves);
    let SubjectScan::Page(page) = driver.find_subjects(&campaign, plan, None, 10).await? else {
        panic!("the plan matches");
    };
    assert_eq!(page.on_source.len(), 1);

    // Deprecated, the source's running sandboxes no longer count, so a page
    // found under the old plan would scan the wrong lifecycles.
    let mut tx = db.pool.begin().await?;
    pplx_space_runtime_environment_policy_store::set_environment_state(
        &mut *tx,
        source(),
        RuntimeEnvironmentState::Deprecated,
        &change(),
    )
    .await?;
    tx.commit().await?;
    assert_eq!(
        driver.find_subjects(&campaign, plan, None, 10).await,
        Ok(SubjectScan::PlanChanged)
    );
    let Plan::Scan(replanned) = driver.plan(&campaign).await? else {
        panic!("a deprecated source with an edge scans");
    };
    assert_ne!(replanned, plan, "the new plan restarts the pass");
    Ok(())
}

/// The plan `driver` makes for `campaign` now, which must scan.
async fn scan_plan(
    driver: &SandboxDriver<FakeDispatcher>,
    campaign: &Campaign,
) -> TestResult<ScanPlan> {
    match driver.plan(campaign).await? {
        Plan::Scan(plan) => Ok(plan),
        Plan::Hold(reason) => panic!("the campaign is held: {reason}"),
    }
}

#[tokio::test(flavor = "current_thread")]
async fn only_the_sources_edges_and_every_rows_identity_enter_the_plan() -> TestResult {
    let Some(db) = test_db().await? else {
        return Ok(());
    };
    seed_policy(&db.pool, RuntimeEnvironmentState::Deprecated, true).await?;
    let campaign = insert_campaign(&db.pool, seed()).await?;
    let driver = SandboxDriver::new(db.pool.clone(), K8S, FakeDispatcher::default());
    let campaign = store::list_unfinished(&db.pool)
        .await?
        .into_iter()
        .find(|listed| listed.id == campaign)
        .expect("the campaign is unfinished");
    let before = scan_plan(&driver, &campaign).await?;

    // An edge recompute on the target's row judges none of the source's
    // sandboxes, so the pass carries on.
    sqlx::query(sql!(
        "UPDATE runtime_environments SET upgrade_edges = $2 WHERE environment_ref = $1"
    ))
    .bind(target().to_string())
    .bind(vec![environment(7).to_string()])
    .execute(&db.pool)
    .await?;
    assert_eq!(scan_plan(&driver, &campaign).await?, before);

    // The source's own edges are what the plan reads.
    sqlx::query(sql!(
        "UPDATE runtime_environments SET memory_edges = $2 WHERE environment_ref = $1"
    ))
    .bind(source().to_string())
    .bind(vec![target().to_string()])
    .execute(&db.pool)
    .await?;
    let after = scan_plan(&driver, &campaign).await?;
    assert_eq!(after.moves, before.moves);
    assert_ne!(
        after, before,
        "a change to the source's edges restarts the pass"
    );
    Ok(())
}
