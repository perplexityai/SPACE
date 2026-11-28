//! What each item's upgrade does: which sandboxes are moved, stopped first,
//! skipped or released, and how the dispatcher's answers are recorded.

use super::*;

#[tokio::test(flavor = "current_thread")]
async fn stopped_sandboxes_on_the_source_are_upgraded_within_the_caps_and_the_campaign_completes()
-> TestResult {
    let Some(db) = test_db().await? else {
        return Ok(());
    };
    seed_policy(&db.pool, RuntimeEnvironmentState::Deprecated, true).await?;
    let campaign = insert_campaign(
        &db.pool,
        CampaignSeed {
            per_node: 1,
            per_cluster: 2,
            ..seed()
        },
    )
    .await?;
    let busy = node(&db.pool).await?;
    let quiet = node(&db.pool).await?;
    let mut expected = Vec::new();
    for node in [busy, busy, busy, quiet, quiet] {
        expected.push(stopped_on_source(&db.pool, node).await?);
    }
    expected.sort();
    // Not this campaign's: another environment, a running sandbox it doesn't
    // take, and another substrate's sandbox.
    insert_sandbox(
        &db.pool,
        K8S,
        SandboxLifecycle::Stopped,
        quiet,
        Published::Bound(environment(7)),
    )
    .await?;
    insert_sandbox(
        &db.pool,
        K8S,
        SandboxLifecycle::Started,
        quiet,
        Published::Bound(source()),
    )
    .await?;
    insert_sandbox(
        &db.pool,
        SubstrateKind::Space,
        SandboxLifecycle::Stopped,
        quiet,
        Published::Bound(source()),
    )
    .await?;
    let dispatcher = FakeDispatcher::default();
    let reconciler = reconciler(&db.pool, dispatcher.clone());

    // Each tick waits out its dispatches, so what it leases is the caps'
    // whole effect: two at a time, one per node. The busy node's three
    // sandboxes take a tick each, and the next tick's pass, which starts
    // with nothing open, completes the drained campaign.
    let mut leased = Vec::new();
    for _ in 0..4 {
        leased.push(tick(&reconciler).await.leased);
    }
    assert_eq!(leased, vec![2, 2, 1, 0], "cluster cap 2, node cap 1");

    assert_eq!(campaign_phase(&db.pool, campaign).await?, "completed");
    assert_eq!(dispatcher.upgraded(), expected);
    assert_eq!(
        dispatcher.state().upgrades,
        vec![(source(), target()); 5],
        "every upgrade is guarded on the campaign's source and asked for its target"
    );
    assert_eq!(
        counts(&db.pool, campaign).await?,
        ItemCounts {
            done: 5,
            ..ItemCounts::default()
        }
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn running_and_paused_sandboxes_move_only_when_asked_and_their_source_is_blocked()
-> TestResult {
    for (state, include_running, takes_running) in [
        (RuntimeEnvironmentState::Blocked, true, true),
        (RuntimeEnvironmentState::Blocked, false, false),
        (RuntimeEnvironmentState::Deprecated, true, false),
    ] {
        let Some(db) = test_db().await? else {
            return Ok(());
        };
        seed_policy(&db.pool, state, true).await?;
        let campaign = insert_campaign(
            &db.pool,
            CampaignSeed {
                include_running,
                ..seed()
            },
        )
        .await?;
        let node = node(&db.pool).await?;
        let stopped = stopped_on_source(&db.pool, node).await?;
        let mut residents = Vec::new();
        for lifecycle in [SandboxLifecycle::Started, SandboxLifecycle::Paused] {
            residents.push(
                insert_sandbox(&db.pool, K8S, lifecycle, node, Published::Bound(source())).await?,
            );
        }
        let dispatcher = FakeDispatcher::default();
        let reconciler = reconciler(&db.pool, dispatcher.clone());

        tick_until_finished(&reconciler, &db.pool, campaign, 5).await?;

        let calls = dispatcher.calls();
        assert!(
            calls.contains(&Call::Upgrade(stopped)),
            "{state} {include_running}"
        );
        assert!(!calls.contains(&Call::Stop(stopped)));
        for resident in residents {
            let stop = calls.iter().position(|call| *call == Call::Stop(resident));
            let upgrade = calls
                .iter()
                .position(|call| *call == Call::Upgrade(resident));
            if takes_running {
                assert!(stop < upgrade && stop.is_some(), "stopped, then upgraded");
                assert!(
                    dispatcher
                        .state()
                        .stops
                        .iter()
                        .all(|guard| *guard == (source(), target())),
                    "every stop is guarded on the campaign's source and its consent to the target"
                );
            } else {
                assert_eq!((stop, upgrade), (None, None), "{state} {include_running}");
            }
        }
        assert_eq!(campaign_phase(&db.pool, campaign).await?, "completed");
    }
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn dispatch_errors_retry_until_the_attempts_run_out_or_fail_the_item() -> TestResult {
    let Some(db) = test_db().await? else {
        return Ok(());
    };
    seed_policy(&db.pool, RuntimeEnvironmentState::Deprecated, true).await?;
    let campaign = insert_campaign(&db.pool, seed()).await?;
    let node = node(&db.pool).await?;
    let flaky = stopped_on_source(&db.pool, node).await?;
    let recovers = stopped_on_source(&db.pool, node).await?;
    let broken = stopped_on_source(&db.pool, node).await?;
    let dispatcher = FakeDispatcher::default();
    let busy = || {
        Err(DispatchError::Retryable(
            "another upgrade step is running".to_owned(),
        ))
    };
    dispatcher.script(flaky, vec![busy(); MAX_ATTEMPTS as usize]);
    dispatcher.script(recovers, vec![busy(), Ok(StoppedUpgrade::AlreadyOnTarget)]);
    dispatcher.script(
        broken,
        vec![Err(DispatchError::Failed("the boot failed".to_owned()))],
    );
    let reconciler = reconciler(&db.pool, dispatcher.clone());

    for _ in 0..MAX_ATTEMPTS + 1 {
        tick(&reconciler).await;
    }

    assert_eq!(
        item(&db.pool, campaign, flaky).await?,
        (
            "failed".to_owned(),
            Some("another upgrade step is running (gave up after 5 attempts)".to_owned()),
            MAX_ATTEMPTS as i64
        )
    );
    assert_eq!(item(&db.pool, campaign, recovers).await?.0, "done");
    assert_eq!(
        item(&db.pool, campaign, broken).await?,
        ("failed".to_owned(), Some("the boot failed".to_owned()), 1),
        "a failed upgrade isn't retried"
    );
    assert_eq!(
        dispatcher
            .upgraded()
            .iter()
            .filter(|sandbox| **sandbox == flaky)
            .count(),
        MAX_ATTEMPTS as usize
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn sandboxes_that_changed_since_they_were_found_are_skipped() -> TestResult {
    let Some(db) = test_db().await? else {
        return Ok(());
    };
    seed_policy(&db.pool, RuntimeEnvironmentState::Deprecated, true).await?;
    let campaign = insert_campaign(&db.pool, seed()).await?;
    let node = node(&db.pool).await?;
    let deleted = stopped_on_source(&db.pool, node).await?;
    let restarted = stopped_on_source(&db.pool, node).await?;
    let starting = stopped_on_source(&db.pool, node).await?;
    let not_moved = stopped_on_source(&db.pool, node).await?;
    // Mid-transition, but no longer on either end of the campaign.
    let elsewhere_stopping = insert_sandbox(
        &db.pool,
        K8S,
        SandboxLifecycle::Stopped,
        node,
        Published::Bound(environment(9)),
    )
    .await?;
    record(
        &db.pool,
        campaign,
        &[deleted, restarted, starting, not_moved, elsewhere_stopping],
    )
    .await?;
    set_lifecycle(&db.pool, elsewhere_stopping, "stopping").await?;
    // Deleted as the API deletes: the row is kept, marked `deleted`.
    set_lifecycle(&db.pool, deleted, "deleted").await?;
    set_lifecycle(&db.pool, restarted, "started").await?;
    set_lifecycle(&db.pool, starting, "starting").await?;
    let dispatcher = FakeDispatcher::default();
    dispatcher.script(
        not_moved,
        vec![Ok(StoppedUpgrade::NotMoved(
            "the environment is allowed again".to_owned(),
        ))],
    );
    let reconciler = reconciler(&db.pool, dispatcher.clone());

    tick(&reconciler).await;

    assert_eq!(
        item(&db.pool, campaign, deleted).await?,
        (
            "skipped".to_owned(),
            Some("the sandbox was deleted".to_owned()),
            1
        )
    );
    assert_eq!(
        item(&db.pool, campaign, restarted).await?.1.as_deref(),
        Some("the sandbox is started, and this campaign moves only stopped sandboxes")
    );
    assert_eq!(
        item(&db.pool, campaign, starting).await?,
        (
            "skipped".to_owned(),
            Some("the sandbox is starting; a later pass finds it again once it settles".to_owned()),
            1
        ),
        "a sandbox mid-transition spends an attempt and holds no lease"
    );
    assert_eq!(
        item(&db.pool, campaign, elsewhere_stopping).await?.0,
        "skipped"
    );
    assert_eq!(
        item(&db.pool, campaign, not_moved).await?,
        (
            "skipped".to_owned(),
            Some("the environment is allowed again".to_owned()),
            1
        )
    );
    assert_eq!(dispatcher.upgraded(), vec![not_moved]);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn a_running_sandbox_is_stopped_only_while_it_and_the_policy_still_call_for_it() -> TestResult
{
    for (state, published, reason) in [
        // Found running; a restart since has cold-booted it onto the target.
        (
            RuntimeEnvironmentState::Blocked,
            target(),
            NO_LONGER_ON_SOURCE,
        ),
        // Still on the source, but the source is no longer blocked, so
        // nothing consents to losing its memory.
        (
            RuntimeEnvironmentState::Deprecated,
            source(),
            POLICY_STOPPED_MOVING_RUNNING,
        ),
    ] {
        let Some(db) = test_db().await? else {
            return Ok(());
        };
        seed_policy(&db.pool, state, true).await?;
        let campaign = insert_campaign(
            &db.pool,
            CampaignSeed {
                include_running: true,
                ..seed()
            },
        )
        .await?;
        let node = node(&db.pool).await?;
        let sandbox = insert_sandbox(
            &db.pool,
            K8S,
            SandboxLifecycle::Started,
            node,
            Published::Bound(published),
        )
        .await?;
        record(&db.pool, campaign, &[sandbox]).await?;
        let dispatcher = FakeDispatcher::default();
        let reconciler = reconciler(&db.pool, dispatcher.clone());

        tick(&reconciler).await;

        assert_eq!(dispatcher.calls(), Vec::new(), "{state}: nothing stopped");
        assert_eq!(
            item(&db.pool, campaign, sandbox).await?,
            ("skipped".to_owned(), Some(reason.to_owned()), 1),
            "{state}"
        );
    }
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn a_stop_the_substrate_declines_skips_the_item_without_an_upgrade() -> TestResult {
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
    let sandbox = insert_sandbox(
        &db.pool,
        K8S,
        SandboxLifecycle::Started,
        node,
        Published::Bound(source()),
    )
    .await?;
    let dispatcher = FakeDispatcher::default();
    dispatcher.state().stop_script.insert(
        sandbox,
        GuardedStop::Declined("restarted on another environment".to_owned()),
    );
    let reconciler = reconciler(&db.pool, dispatcher.clone());

    tick(&reconciler).await;

    assert_eq!(dispatcher.calls(), vec![Call::Stop(sandbox)]);
    assert_eq!(
        item(&db.pool, campaign, sandbox).await?,
        (
            "skipped".to_owned(),
            Some("restarted on another environment".to_owned()),
            1
        )
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn a_stopped_sandbox_goes_to_the_upgrade_only_on_the_campaigns_source_or_target() -> TestResult
{
    let Some(db) = test_db().await? else {
        return Ok(());
    };
    seed_policy(&db.pool, RuntimeEnvironmentState::Deprecated, true).await?;
    let campaign = insert_campaign(&db.pool, seed()).await?;
    let node = node(&db.pool).await?;
    // Found on the source; a start and stop since left it elsewhere.
    let elsewhere = insert_sandbox(
        &db.pool,
        K8S,
        SandboxLifecycle::Stopped,
        node,
        Published::Bound(environment(9)),
    )
    .await?;
    // An earlier lease's upgrade published the target, and the upgrade
    // finishes it.
    let on_target = insert_sandbox(
        &db.pool,
        K8S,
        SandboxLifecycle::Stopped,
        node,
        Published::Bound(target()),
    )
    .await?;
    record(&db.pool, campaign, &[elsewhere, on_target]).await?;
    let dispatcher = FakeDispatcher::default();
    let reconciler = reconciler(&db.pool, dispatcher.clone());

    tick(&reconciler).await;

    assert_eq!(dispatcher.calls(), vec![Call::Upgrade(on_target)]);
    assert_eq!(item(&db.pool, campaign, on_target).await?.0, "done");
    assert_eq!(
        item(&db.pool, campaign, elsewhere).await?,
        ("skipped".to_owned(), Some(ON_NEITHER_END.to_owned()), 1)
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn a_publish_without_an_execution_is_attributed_nowhere() -> TestResult {
    let Some(db) = test_db().await? else {
        return Ok(());
    };
    // The source carries the initial Firecracker versions, which the legacy
    // setting would match an empty record to.
    seed_policy(&db.pool, RuntimeEnvironmentState::Deprecated, true).await?;
    let mut tx = db.pool.begin().await?;
    pplx_space_runtime_environment_policy_store::set_legacy_unbound_allowed(
        &mut *tx,
        false,
        &change(),
    )
    .await?;
    tx.commit().await?;
    let campaign = insert_campaign(&db.pool, seed()).await?;
    let node = node(&db.pool).await?;
    let empty = insert_sandbox(
        &db.pool,
        K8S,
        SandboxLifecycle::Stopped,
        node,
        Published::Nothing,
    )
    .await?;
    // Recorded anyway, as if found before its publish lost the execution.
    record(&db.pool, campaign, &[empty]).await?;
    let dispatcher = FakeDispatcher::default();
    let reconciler = reconciler(&db.pool, dispatcher.clone());

    let report = tick(&reconciler).await;

    assert_eq!(
        report.inserted, 0,
        "already recorded, so nothing new is inserted"
    );
    assert_eq!(dispatcher.calls(), Vec::new(), "not upgraded");
    assert_eq!(
        item(&db.pool, campaign, empty).await?,
        ("skipped".to_owned(), Some(ON_NEITHER_END.to_owned()), 1)
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn an_unreachable_node_with_fresh_evidence_gets_no_dispatch() -> TestResult {
    let Some(db) = test_db().await? else {
        return Ok(());
    };
    seed_policy(&db.pool, RuntimeEnvironmentState::Deprecated, true).await?;
    let campaign = insert_campaign(&db.pool, seed()).await?;
    let node = node(&db.pool).await?;
    upsert_snapshot(
        &db.pool,
        node,
        SnapshotSeed {
            reachable: false,
            ..SnapshotSeed::default()
        },
    )
    .await?;
    let sandbox = stopped_on_source(&db.pool, node).await?;
    let dispatcher = FakeDispatcher::default();
    let reconciler = reconciler(&db.pool, dispatcher.clone());

    for _ in 0..3 {
        tick(&reconciler).await;
    }

    assert_eq!(dispatcher.calls(), Vec::new());
    assert_eq!(item(&db.pool, campaign, sandbox).await?.0, "pending");
    assert_eq!(
        campaign_phase(&db.pool, campaign).await?,
        "rolling",
        "the sandbox still on the source holds the campaign open"
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn a_sandbox_stuck_mid_transition_doesnt_hold_the_canary() -> TestResult {
    let Some(db) = test_db().await? else {
        return Ok(());
    };
    seed_policy(&db.pool, RuntimeEnvironmentState::Deprecated, true).await?;
    let campaign = insert_campaign(
        &db.pool,
        CampaignSeed {
            phase: CampaignPhase::Canary,
            ..seed()
        },
    )
    .await?;
    let node = node(&db.pool).await?;
    let mut sandboxes = vec![
        stopped_on_source(&db.pool, node).await?,
        stopped_on_source(&db.pool, node).await?,
    ];
    sandboxes.sort();
    // Both found while stopped. The canary leases in id order, so the one
    // that gets stuck mid-transition comes first.
    record(&db.pool, campaign, &sandboxes).await?;
    let (stuck, other) = (sandboxes[0], sandboxes[1]);
    set_lifecycle(&db.pool, stuck, "stopping").await?;
    let dispatcher = FakeDispatcher::default();
    let reconciler = reconciler(&db.pool, dispatcher.clone());

    for _ in 0..3 {
        tick(&reconciler).await;
    }

    assert_eq!(dispatcher.upgraded(), vec![other]);
    assert_eq!(item(&db.pool, campaign, other).await?.0, "done");
    assert_eq!(item(&db.pool, campaign, stuck).await?.0, "skipped");
    assert_eq!(
        campaign_phase(&db.pool, campaign).await?,
        "canary",
        "the canary moved a sandbox and waits for an operator's resume"
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn a_sandbox_settling_on_the_target_is_retried_until_its_upgrade_finishes() -> TestResult {
    let Some(db) = test_db().await? else {
        return Ok(());
    };
    seed_policy(&db.pool, RuntimeEnvironmentState::Deprecated, true).await?;
    let campaign = insert_campaign(&db.pool, seed()).await?;
    let node = node(&db.pool).await?;
    // An earlier lease's upgrade published the target and is still stopping
    // the sandbox; no pass finds a sandbox on the target again.
    let sandbox = insert_sandbox(
        &db.pool,
        K8S,
        SandboxLifecycle::Stopped,
        node,
        Published::Bound(target()),
    )
    .await?;
    record(&db.pool, campaign, &[sandbox]).await?;
    set_lifecycle(&db.pool, sandbox, "stopping").await?;
    let dispatcher = FakeDispatcher::default();
    let reconciler = reconciler(&db.pool, dispatcher.clone());

    tick(&reconciler).await;
    assert_eq!(
        item(&db.pool, campaign, sandbox).await?,
        (
            "pending".to_owned(),
            Some(
                "the sandbox is stopping on the campaign's target; retrying once it settles"
                    .to_owned()
            ),
            1
        ),
        "retried, not skipped"
    );
    assert!(dispatcher.calls().is_empty());

    // Settled, the next lease hands it to the upgrade, which finishes it.
    set_lifecycle(&db.pool, sandbox, "stopped").await?;
    tick_until_finished(&reconciler, &db.pool, campaign, 5).await?;
    assert_eq!(dispatcher.calls(), vec![Call::Upgrade(sandbox)]);
    assert_eq!(item(&db.pool, campaign, sandbox).await?.0, "done");
    assert_eq!(campaign_phase(&db.pool, campaign).await?, "completed");
    Ok(())
}
