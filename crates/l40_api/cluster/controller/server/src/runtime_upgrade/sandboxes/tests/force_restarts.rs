//! The `sandbox_force_restarts` driver: its pure plan and lifecycle tables,
//! its holds and mid-pass un-retire reset, its stranded and unreachable-node
//! behavior, and its per-item stop-then-start guards and hold recovery.

use std::collections::{BTreeMap, BTreeSet};

use super::super::force_restarts::{
    ForceRestartPlan, ForceRestartStep, POLICY_STOPPED_FORCE_RESTARTING, force_restart_plan,
    force_restart_step,
};
use super::*;

fn sample_campaign() -> Campaign {
    Campaign {
        id: CampaignId(Uuid::now_v7()),
        from: source(),
        to: target(),
        kind: CampaignKind::SandboxForceRestarts,
        phase: CampaignPhase::Rolling,
        max_in_flight_per_node: 1,
        max_in_flight_per_cluster: 2,
        failure_budget: 5,
        include_running: false,
        generation: 1,
    }
}

fn catalog_entry(has_edge: bool) -> CatalogEnvironment {
    CatalogEnvironment {
        reference: source(),
        runtime: "firecracker".to_owned(),
        runtime_version: "1.15.1".to_owned(),
        kernel_version: "6.1.155".to_owned(),
        state_format_name: "firecracker-snapshot".to_owned(),
        state_format_version: "9.0.0".to_owned(),
        upgrade_edges: if has_edge {
            BTreeSet::from([target()])
        } else {
            BTreeSet::new()
        },
        memory_edges: BTreeSet::new(),
    }
}

#[test]
fn plan_matrix_covers_holds_stranded_and_cold_boot_moves() {
    let campaign = sample_campaign();
    let states = [
        None,
        Some(RuntimeEnvironmentState::Allowed),
        Some(RuntimeEnvironmentState::Deprecated),
        Some(RuntimeEnvironmentState::Blocked),
    ];
    for state in states {
        // Exhaustive match keeps new RuntimeEnvironmentState variants from
        // slipping past the matrix.
        let is_allowed = match state {
            Some(RuntimeEnvironmentState::Allowed) => true,
            Some(RuntimeEnvironmentState::Deprecated | RuntimeEnvironmentState::Blocked) | None => {
                false
            }
        };
        for default_environment in [None, Some(environment(9)), Some(target())] {
            for memory_retired in [false, true] {
                for has_edge in [false, true] {
                    let mut environments = BTreeMap::new();
                    if let Some(state) = state {
                        environments.insert(source(), state);
                    }
                    let policy = RuntimeEnvironmentPolicy {
                        generation: 1,
                        default_environment,
                        legacy_unbound_allowed: None,
                        environments,
                    };
                    let catalog = catalog_entry(has_edge);
                    let expected = if default_environment != Some(target()) {
                        ForceRestartPlan::Hold("target_not_default")
                    } else if is_allowed {
                        ForceRestartPlan::Hold("source_allowed")
                    } else if !memory_retired {
                        ForceRestartPlan::Hold("memory_not_retired")
                    } else {
                        ForceRestartPlan::Scan { moves: has_edge }
                    };
                    assert_eq!(
                        force_restart_plan(&policy, Some(&catalog), memory_retired, &campaign),
                        expected,
                        "state={state:?} default={default_environment:?} retired={memory_retired} edge={has_edge}"
                    );
                }
            }
        }
    }
}

#[test]
fn step_classifies_every_sandbox_lifecycle() {
    let stamp = jiff::Timestamp::UNIX_EPOCH;
    for stopped_at in [None, Some(stamp)] {
        for lifecycle in SandboxLifecycle::all() {
            let expected = match lifecycle {
                SandboxLifecycle::Started | SandboxLifecycle::Paused => {
                    ForceRestartStep::StopThenStart
                }
                SandboxLifecycle::Stopped => match stopped_at {
                    Some(at) => ForceRestartStep::StartStopped(at),
                    None => ForceRestartStep::Skip(
                        "the sandbox is stopped, which this campaign doesn't upgrade".to_owned(),
                    ),
                },
                SandboxLifecycle::Creating
                | SandboxLifecycle::Starting
                | SandboxLifecycle::Restoring
                | SandboxLifecycle::Pausing
                | SandboxLifecycle::Resuming
                | SandboxLifecycle::Suspending
                | SandboxLifecycle::Stopping
                | SandboxLifecycle::Checkpointing => ForceRestartStep::Release(format!(
                    "the sandbox is {}; retrying once it settles",
                    lifecycle.as_str()
                )),
                SandboxLifecycle::Created
                | SandboxLifecycle::Suspended
                | SandboxLifecycle::Deleting
                | SandboxLifecycle::Quarantined
                | SandboxLifecycle::Failed => ForceRestartStep::Skip(format!(
                    "the sandbox is {}, which this campaign doesn't upgrade",
                    lifecycle.as_str()
                )),
            };
            assert_eq!(
                force_restart_step(lifecycle.as_str(), stopped_at),
                expected,
                "{lifecycle:?} stopped_at={stopped_at:?}"
            );
        }
        assert_eq!(
            force_restart_step("deleted", stopped_at),
            ForceRestartStep::Skip(
                "the sandbox's lifecycle \"deleted\" isn't upgradable".to_owned()
            )
        );
    }
}

#[tokio::test(flavor = "current_thread")]
async fn running_and_paused_sandboxes_on_the_source_are_force_restarted_within_caps_and_complete()
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
            ..force_restart_seed()
        },
    )
    .await?;
    let busy = node(&db.pool).await?;
    let quiet = node(&db.pool).await?;
    let mut nodes = HashMap::new();
    let mut expected = Vec::new();
    for (node, lifecycle) in [
        (busy, SandboxLifecycle::Started),
        (busy, SandboxLifecycle::Paused),
        (busy, SandboxLifecycle::Started),
        (quiet, SandboxLifecycle::Started),
    ] {
        let sandbox = resident_on_source(&db.pool, lifecycle, node).await?;
        nodes.insert(sandbox, node);
        expected.push(sandbox);
    }
    // Unbound paused sandbox whose execution matches the source catalog row.
    let unbound_paused = insert_sandbox(
        &db.pool,
        K8S,
        SandboxLifecycle::Paused,
        quiet,
        Published::Unbound {
            runtime_version: "1.15.1",
            kernel: "6.1.155",
            state_format_version: "9.0.0",
        },
    )
    .await?;
    nodes.insert(unbound_paused, quiet);
    expected.push(unbound_paused);
    expected.sort();

    // Ignored: stopped or suspended on the source, running on another
    // environment, another substrate's running sandbox, empty publish, and
    // unattributable unbound execution.
    stopped_on_source(&db.pool, quiet).await?;
    insert_sandbox(
        &db.pool,
        K8S,
        SandboxLifecycle::Suspended,
        quiet,
        Published::Bound(source()),
    )
    .await?;
    insert_sandbox(
        &db.pool,
        K8S,
        SandboxLifecycle::Started,
        quiet,
        Published::Bound(environment(7)),
    )
    .await?;
    insert_sandbox(
        &db.pool,
        SubstrateKind::Space,
        SandboxLifecycle::Started,
        quiet,
        Published::Bound(source()),
    )
    .await?;
    insert_sandbox(
        &db.pool,
        K8S,
        SandboxLifecycle::Started,
        quiet,
        Published::Nothing,
    )
    .await?;
    insert_sandbox(
        &db.pool,
        K8S,
        SandboxLifecycle::Started,
        quiet,
        Published::Unbound {
            runtime_version: "1.15.1",
            kernel: "5.10.0",
            state_format_version: "9.0.0",
        },
    )
    .await?;

    let dispatcher = FakeDispatcher::with_nodes(nodes).persisting(db.pool.clone());
    let memory = FakeMemoryRetirement::retired(source());
    let reconciler = force_restart_reconciler(&db.pool, dispatcher.clone(), memory);

    let mut leased = Vec::new();
    for _ in 0..4 {
        leased.push(tick(&reconciler).await.leased);
    }
    assert_eq!(leased, vec![2, 2, 1, 0], "cluster cap 2, node cap 1");
    assert_eq!(campaign_phase(&db.pool, campaign).await?, "completed");
    assert_eq!(dispatcher.started(), expected);
    assert!(dispatcher.upgraded().is_empty());

    let calls = dispatcher.calls();
    for sandbox in &expected {
        let stop = calls.iter().position(|call| *call == Call::Stop(*sandbox));
        let start = calls.iter().position(|call| *call == Call::Start(*sandbox));
        assert!(stop.is_some() && stop < start, "stopped before started");
    }
    assert_eq!(dispatcher.state().stop_sources, vec![source(); 5]);
    assert_eq!(dispatcher.state().start_targets, vec![target(); 5]);
    let (cluster_peak, node_peak) = dispatcher.peaks();
    assert!(
        cluster_peak <= 2 && node_peak <= 1,
        "never more in flight than the caps: {cluster_peak} in all, {node_peak} on a node"
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
async fn unretiring_memory_mid_pass_holds_and_restarts_the_pass_from_the_beginning() -> TestResult {
    let Some(db) = test_db().await? else {
        return Ok(());
    };
    seed_policy(&db.pool, RuntimeEnvironmentState::Deprecated, true).await?;
    let campaign = insert_campaign(&db.pool, force_restart_seed()).await?;
    let node = node(&db.pool).await?;

    // Both suspended sandboxes have lower UUIDv7s than `unbound_on_target`,
    // which are unbound running sandboxes attributed to the target, so a
    // page-size-1 scan examines `unbound_on_target[0]` first (`after =
    // Some(unbound_on_target[0])`) without completing yet.
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
    assert!(first_behind_cursor < second_behind_cursor);
    assert!(second_behind_cursor < unbound_on_target[0]);

    let dispatcher = FakeDispatcher::default().persisting(db.pool.clone());
    let memory = FakeMemoryRetirement::retired(source());
    let reconciler = force_restart_paging(&db.pool, dispatcher.clone(), memory.clone(), 1);

    // Page 1 examines `unbound_on_target[0]`, leaving the pass open past both
    // suspended sandboxes.
    let first_tick = tick(&reconciler).await;
    assert_eq!((first_tick.inserted, first_tick.leased), (0, 0));
    assert_eq!(campaign_phase(&db.pool, campaign).await?, "rolling");

    // A mid-tick hold inside `find_subjects` (`plan()` reads `true`,
    // `find_subjects()` reads `false` -> `SubjectScan::PlanChanged`) while
    // `memory` is retired again before the next tick produces the identical
    // `ScanPlan` fingerprint, yet must still reset the cursor so
    // `first_behind_cursor` is discovered from the beginning.
    memory.script_reads([Ok(true), Ok(false)]);
    set_lifecycle(&db.pool, first_behind_cursor, "started").await?;
    let mid_tick_hold = tick(&reconciler).await;
    assert_eq!((mid_tick_hold.inserted, mid_tick_hold.leased), (0, 0));
    assert!(dispatcher.calls().is_empty());

    let restarted_page = tick(&reconciler).await;
    assert_eq!((restarted_page.inserted, restarted_page.leased), (1, 1));
    assert_eq!(
        dispatcher.calls(),
        vec![
            Call::Stop(first_behind_cursor),
            Call::Start(first_behind_cursor),
        ]
    );

    // Finish the unsettled pass and step two pages into a fresh pass (past
    // `first_behind_cursor` and `unbound_on_target[0]`, both `> second_behind_cursor`
    // once at `unbound_on_target[0]`).
    for _ in 0..4 {
        tick(&reconciler).await;
    }
    assert_eq!(campaign_phase(&db.pool, campaign).await?, "rolling");

    // Un-retiring memory before `plan()` (`Plan::Hold`) allows
    // `second_behind_cursor` to restore onto the deprecated source behind the
    // open pass cursor.
    memory.unretire(source());
    set_lifecycle(&db.pool, second_behind_cursor, "started").await?;
    let held_tick = tick(&reconciler).await;
    assert_eq!((held_tick.inserted, held_tick.leased), (0, 0));
    assert_eq!(campaign_phase(&db.pool, campaign).await?, "rolling");

    // Retiring memory again restarts the scan from the beginning rather than
    // continuing after `unbound_on_target[0]`, finding `second_behind_cursor`
    // before completing.
    memory.retire(source());
    tick_until_finished(&reconciler, &db.pool, campaign, 10).await?;

    assert_eq!(campaign_phase(&db.pool, campaign).await?, "completed");
    assert_eq!(
        dispatcher.calls(),
        vec![
            Call::Stop(first_behind_cursor),
            Call::Start(first_behind_cursor),
            Call::Stop(second_behind_cursor),
            Call::Start(second_behind_cursor),
        ]
    );
    assert_eq!(
        counts(&db.pool, campaign).await?,
        ItemCounts {
            done: 2,
            ..ItemCounts::default()
        }
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn stranded_residents_and_unreachable_nodes_hold_the_campaign_open() -> TestResult {
    let Some(db) = test_db().await? else {
        return Ok(());
    };
    seed_policy(&db.pool, RuntimeEnvironmentState::Deprecated, false).await?;
    let campaign = insert_campaign(&db.pool, force_restart_seed()).await?;
    let reachable_node = node(&db.pool).await?;
    let unreachable_node = node(&db.pool).await?;
    let on_reachable =
        resident_on_source(&db.pool, SandboxLifecycle::Started, reachable_node).await?;
    let on_unreachable =
        resident_on_source(&db.pool, SandboxLifecycle::Paused, unreachable_node).await?;

    let dispatcher = FakeDispatcher::default().persisting(db.pool.clone());
    let memory = FakeMemoryRetirement::retired(source());
    let reconciler = force_restart_reconciler(&db.pool, dispatcher.clone(), memory);

    // Without an upgrade edge from the source to the target, residents are
    // recorded pending as stranded and nothing is leased.
    for _ in 0..2 {
        tick(&reconciler).await;
    }
    assert_eq!(campaign_phase(&db.pool, campaign).await?, "rolling");
    assert_eq!(
        counts(&db.pool, campaign).await?,
        ItemCounts {
            pending: 2,
            ..ItemCounts::default()
        }
    );
    assert!(dispatcher.calls().is_empty());

    // Once the catalog certifies the upgrade edge, the reachable node's
    // resident moves while the unreachable node's stays pending and holds the
    // campaign open.
    upsert_snapshot(
        &db.pool,
        unreachable_node,
        SnapshotSeed {
            reachable: false,
            ..SnapshotSeed::default()
        },
    )
    .await?;
    sqlx::query(sql!(
        "UPDATE runtime_environments SET upgrade_edges = $2 WHERE environment_ref = $1"
    ))
    .bind(source().to_string())
    .bind(vec![target().to_string()])
    .execute(&db.pool)
    .await?;

    for _ in 0..3 {
        tick(&reconciler).await;
    }
    assert_eq!(campaign_phase(&db.pool, campaign).await?, "rolling");
    assert_eq!(
        dispatcher.calls(),
        vec![Call::Stop(on_reachable), Call::Start(on_reachable)]
    );
    assert_eq!(item(&db.pool, campaign, on_reachable).await?.0, "done");
    assert_eq!(
        item(&db.pool, campaign, on_unreachable).await?.0,
        "pending",
        "the unreachable node's resident stays pending and holds the campaign open"
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn per_item_guards_and_retry_after_stop_preserve_user_stops_and_recover_driver_stops()
-> TestResult {
    let Some(db) = test_db().await? else {
        return Ok(());
    };
    seed_policy(&db.pool, RuntimeEnvironmentState::Blocked, true).await?;
    let mut tx = db.pool.begin().await?;
    pplx_space_runtime_environment_policy_store::set_legacy_unbound_allowed(
        &mut tx,
        false,
        &change(),
    )
    .await?;
    tx.commit().await?;
    let campaign = insert_campaign(
        &db.pool,
        CampaignSeed {
            per_node: 12,
            per_cluster: 12,
            failure_budget: 10,
            ..force_restart_seed()
        },
    )
    .await?;
    let node = node(&db.pool).await?;

    let deleted = resident_on_source(&db.pool, SandboxLifecycle::Started, node).await?;
    let user_stopped = resident_on_source(&db.pool, SandboxLifecycle::Started, node).await?;
    let stopping = resident_on_source(&db.pool, SandboxLifecycle::Started, node).await?;
    let moved_before_lease = insert_sandbox(
        &db.pool,
        K8S,
        SandboxLifecycle::Started,
        node,
        Published::Bound(target()),
    )
    .await?;
    let declined_then_user_stopped =
        resident_on_source(&db.pool, SandboxLifecycle::Started, node).await?;
    let not_on_target = resident_on_source(&db.pool, SandboxLifecycle::Paused, node).await?;
    let held_inside_start = resident_on_source(&db.pool, SandboxLifecycle::Paused, node).await?;
    let start_retried = resident_on_source(&db.pool, SandboxLifecycle::Started, node).await?;
    let retryable_stop_then_user_stopped =
        resident_on_source(&db.pool, SandboxLifecycle::Started, node).await?;
    let ambiguous_start_timeout =
        resident_on_source(&db.pool, SandboxLifecycle::Started, node).await?;
    let start_failed = resident_on_source(&db.pool, SandboxLifecycle::Started, node).await?;

    record(
        &db.pool,
        campaign,
        &[
            deleted,
            user_stopped,
            stopping,
            moved_before_lease,
            declined_then_user_stopped,
            not_on_target,
            held_inside_start,
            start_retried,
            retryable_stop_then_user_stopped,
            ambiguous_start_timeout,
            start_failed,
        ],
    )
    .await?;

    sqlx::query(sql!("DELETE FROM sandboxes WHERE id = $1"))
        .bind(deleted)
        .execute(&db.pool)
        .await?;
    set_lifecycle(&db.pool, user_stopped, "stopped").await?;
    set_lifecycle(&db.pool, stopping, "stopping").await?;

    let dispatcher = FakeDispatcher::default().persisting(db.pool.clone());
    dispatcher.state().stop_script.insert(
        declined_then_user_stopped,
        GuardedStop::Declined("restarted on another environment".to_owned()),
    );
    dispatcher.state().stop_errors.insert(
        retryable_stop_then_user_stopped,
        VecDeque::from([DispatchError::Retryable("node unavailable".to_owned())]),
    );
    dispatcher.script_start(
        not_on_target,
        vec![Ok(TargetStart::NotOnTarget("default moved".to_owned()))],
    );
    // `TargetStart::HeldStopped` when policy/catalog shifts right before `start`
    // claims the stopped row releases the item (refunding the attempt and
    // keeping `ForceRestartProgress::Stopped`) so the next tick cold-boots it.
    dispatcher.script_start(
        held_inside_start,
        vec![
            Ok(TargetStart::HeldStopped(
                "policy changed before start claim".to_owned(),
            )),
            Ok(TargetStart::Started),
        ],
    );
    dispatcher.script_start(
        start_retried,
        vec![
            Err(DispatchError::Retryable("apiserver timeout".to_owned())),
            Ok(TargetStart::Started),
        ],
    );
    dispatcher.script_start(
        ambiguous_start_timeout,
        vec![Err(DispatchError::Retryable(
            "start response timed out".to_owned(),
        ))],
    );
    dispatcher.script_start(
        start_failed,
        vec![Err(DispatchError::Failed("cold boot failed".to_owned()))],
    );

    let memory = FakeMemoryRetirement::retired(source());
    let reconciler = force_restart_reconciler(&db.pool, dispatcher.clone(), memory.clone());

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
        item(&db.pool, campaign, user_stopped).await?,
        (
            "skipped".to_owned(),
            Some("the sandbox is stopped, which this campaign doesn't upgrade".to_owned()),
            1
        )
    );
    assert_eq!(
        item(&db.pool, campaign, stopping).await?,
        (
            "pending".to_owned(),
            Some("the sandbox is stopping; retrying once it settles".to_owned()),
            0
        )
    );
    assert_eq!(
        item(&db.pool, campaign, moved_before_lease).await?,
        (
            "skipped".to_owned(),
            Some(NO_LONGER_ON_SOURCE.to_owned()),
            1
        )
    );
    assert_eq!(
        item(&db.pool, campaign, declined_then_user_stopped).await?,
        (
            "skipped".to_owned(),
            Some("restarted on another environment".to_owned()),
            1
        )
    );
    assert_eq!(
        item(&db.pool, campaign, not_on_target).await?,
        ("skipped".to_owned(), Some("default moved".to_owned()), 1)
    );
    assert_eq!(
        item(&db.pool, campaign, held_inside_start).await?,
        (
            "pending".to_owned(),
            Some("policy changed before start claim".to_owned()),
            0
        )
    );
    assert_eq!(
        item(&db.pool, campaign, start_failed).await?,
        ("failed".to_owned(), Some("cold boot failed".to_owned()), 1)
    );
    assert_eq!(item(&db.pool, campaign, start_retried).await?.0, "pending");

    // `stopping`, `declined_then_user_stopped`, and
    // `retryable_stop_then_user_stopped` (whose `stop` returned `Retryable`
    // before committing) are stopped by the user before attempt 2: none was
    // stopped by the campaign, so none may be booted. Meanwhile, `start_retried`
    // and `held_inside_start` keep their substrate `ForceRestartProgress::Stopped`,
    // and `ambiguous_start_timeout`'s timed-out start booted onto `target()`.
    set_lifecycle(&db.pool, stopping, "stopped").await?;
    record(&db.pool, campaign, &[declined_then_user_stopped]).await?;
    set_lifecycle(&db.pool, declined_then_user_stopped, "stopped").await?;
    set_lifecycle(&db.pool, retryable_stop_then_user_stopped, "stopped").await?;
    set_lifecycle(&db.pool, ambiguous_start_timeout, "started").await?;
    set_bound(&db.pool, ambiguous_start_timeout, target()).await?;

    tick(&reconciler).await;

    assert_eq!(
        item(&db.pool, campaign, stopping).await?,
        (
            "skipped".to_owned(),
            Some("the sandbox is stopped, which this campaign doesn't upgrade".to_owned()),
            1
        )
    );
    assert_eq!(
        item(&db.pool, campaign, declined_then_user_stopped).await?,
        (
            "skipped".to_owned(),
            Some("the sandbox is stopped, which this campaign doesn't upgrade".to_owned()),
            2
        )
    );
    assert_eq!(
        item(&db.pool, campaign, retryable_stop_then_user_stopped).await?,
        (
            "skipped".to_owned(),
            Some("the sandbox is stopped, which this campaign doesn't upgrade".to_owned()),
            2
        )
    );
    assert_eq!(
        item(&db.pool, campaign, held_inside_start).await?,
        ("done".to_owned(), None, 1)
    );
    assert_eq!(
        item(&db.pool, campaign, start_retried).await?,
        ("done".to_owned(), None, 2)
    );
    assert_eq!(
        item(&db.pool, campaign, ambiguous_start_timeout).await?,
        ("done".to_owned(), None, 2)
    );

    let calls = dispatcher.calls();
    assert!(!calls.contains(&Call::Stop(user_stopped)));
    assert!(!calls.contains(&Call::Start(user_stopped)));
    assert!(!calls.contains(&Call::Start(stopping)));
    assert!(!calls.contains(&Call::Start(declined_then_user_stopped)));
    assert!(!calls.contains(&Call::Start(retryable_stop_then_user_stopped)));
    assert!(!calls.contains(&Call::Stop(moved_before_lease)));
    assert_eq!(
        calls
            .iter()
            .filter(|c| **c == Call::Stop(held_inside_start))
            .count(),
        1,
        "HeldStopped preserves stop ownership without stopping a second time"
    );
    assert_eq!(
        calls
            .iter()
            .filter(|c| **c == Call::Start(held_inside_start))
            .count(),
        2
    );
    assert_eq!(
        calls
            .iter()
            .filter(|c| **c == Call::Stop(start_retried))
            .count(),
        1,
        "retry after stop skips a second stop"
    );
    assert_eq!(
        calls
            .iter()
            .filter(|c| **c == Call::Start(start_retried))
            .count(),
        2
    );
    assert_eq!(
        calls
            .iter()
            .filter(|c| **c == Call::Stop(retryable_stop_then_user_stopped))
            .count(),
        1
    );
    assert_eq!(
        calls
            .iter()
            .filter(|c| **c == Call::Stop(ambiguous_start_timeout))
            .count(),
        1
    );
    assert_eq!(
        calls
            .iter()
            .filter(|c| **c == Call::Start(ambiguous_start_timeout))
            .count(),
        1
    );

    // Hold behavior on items already stopped by the campaign:
    // - An item still running on the source when memory is un-retired is
    //   skipped without stopping it;
    // - A bound or pre-metadata unbound item already stopped by the campaign is
    //   released (`Release`) across memory un-retirement or a temporary
    //   `legacy_unbound_allowed = true` flip, then cold-booted once cleared;
    // - A transient `force_restart_progress` read error retries rather than
    //   misclassifying a campaign-stopped sandbox as user-stopped;
    // - Coexisting `sandboxes` upgrades preserve `lifecycle_changed_at` so
    //   `StartStopped` still boots onto `target()`, while a move to a third
    //   environment skips with `ON_NEITHER_END`; and `retain_campaigns` evicts
    //   finished campaigns.
    let baseline_stops = dispatcher.tracked_driver_stops();
    let baseline_started = dispatcher.tracked_started_on_target();

    let unretired_before_stop =
        resident_on_source(&db.pool, SandboxLifecycle::Started, node).await?;
    let held_after_driver_stop =
        resident_on_source(&db.pool, SandboxLifecycle::Started, node).await?;
    let pre_metadata_stopped = insert_sandbox(
        &db.pool,
        K8S,
        SandboxLifecycle::Started,
        node,
        Published::PreMetadata { kernel: "6.1.155" },
    )
    .await?;
    for id in [held_after_driver_stop, pre_metadata_stopped] {
        dispatcher.script_start(
            id,
            vec![
                Err(DispatchError::Retryable("slot busy".to_owned())),
                Ok(TargetStart::Started),
            ],
        );
    }
    let driver = SandboxForceRestartDriver::new(
        db.pool.clone(),
        K8S.as_str(),
        dispatcher.clone(),
        memory.clone(),
    );
    record(
        &db.pool,
        campaign,
        &[
            unretired_before_stop,
            held_after_driver_stop,
            pre_metadata_stopped,
        ],
    )
    .await?;
    let campaign_row = store::list_unfinished(&db.pool)
        .await?
        .into_iter()
        .find(|listed| listed.id == campaign)
        .expect("campaign");

    let leased = store::lease_items(&db.pool, campaign, Duration::from_secs(30), 16).await?;
    for id in [held_after_driver_stop, pre_metadata_stopped] {
        let lease = leased
            .iter()
            .find(|item| item.subject_id == id)
            .expect("item leased");
        let first_outcome = driver.upgrade(&campaign_row, lease).await;
        assert_eq!(
            first_outcome,
            ItemOutcome::Retry(ItemError::new("slot busy"))
        );
        store::finish_item(&db.pool, lease, &first_outcome).await?;
    }
    let still_running_lease = leased
        .iter()
        .find(|item| item.subject_id == unretired_before_stop)
        .expect("unretired_before_stop leased");
    assert_eq!(dispatcher.tracked_driver_stops(), baseline_stops + 2);

    // Un-retiring memory skips `unretired_before_stop` without stopping it, and
    // flipping `legacy_unbound_allowed = true` temporarily unattributes
    // `pre_metadata_stopped`; both already-stopped items release without
    // losing stop ownership.
    memory.unretire(source());
    let mut tx = db.pool.begin().await?;
    pplx_space_runtime_environment_policy_store::set_legacy_unbound_allowed(
        &mut tx,
        true,
        &change(),
    )
    .await?;
    tx.commit().await?;

    let running_outcome = driver.upgrade(&campaign_row, still_running_lease).await;
    assert_eq!(
        running_outcome,
        ItemOutcome::Skipped(ItemError::new(POLICY_STOPPED_FORCE_RESTARTING))
    );
    store::finish_item(&db.pool, still_running_lease, &running_outcome).await?;
    assert!(
        !dispatcher
            .calls()
            .contains(&Call::Stop(unretired_before_stop))
    );

    dispatcher.state().progress_read_errors.insert(
        held_after_driver_stop,
        VecDeque::from([DispatchError::Retryable(
            "progress store unavailable".to_owned(),
        )]),
    );
    let re_leased = store::lease_items(&db.pool, campaign, Duration::from_secs(30), 16).await?;
    let held_retry = re_leased
        .iter()
        .find(|item| item.subject_id == held_after_driver_stop)
        .expect("held_after_driver_stop re-leased");
    let pre_metadata_retry = re_leased
        .iter()
        .find(|item| item.subject_id == pre_metadata_stopped)
        .expect("pre_metadata_stopped re-leased");
    assert_eq!(
        driver.upgrade(&campaign_row, held_retry).await,
        ItemOutcome::Retry(ItemError::new("progress store unavailable"))
    );
    let held_outcome = driver.upgrade(&campaign_row, held_retry).await;
    assert_eq!(
        held_outcome,
        ItemOutcome::Release(ItemError::new(POLICY_STOPPED_FORCE_RESTARTING))
    );
    store::finish_item(&db.pool, held_retry, &held_outcome).await?;
    let pre_metadata_outcome = driver.upgrade(&campaign_row, pre_metadata_retry).await;
    assert_eq!(
        pre_metadata_outcome,
        ItemOutcome::Release(ItemError::new(POLICY_STOPPED_FORCE_RESTARTING)),
        "unbound campaign-stopped item holds across legacy_unbound_allowed=true instead of skipping"
    );
    store::finish_item(&db.pool, pre_metadata_retry, &pre_metadata_outcome).await?;
    assert_eq!(dispatcher.tracked_driver_stops(), baseline_stops + 2);

    // Restoring memory retirement and `legacy_unbound_allowed = false`
    // cold-boots both held items onto `target()` without a second stop.
    memory.retire(source());
    let mut tx = db.pool.begin().await?;
    pplx_space_runtime_environment_policy_store::set_legacy_unbound_allowed(
        &mut tx,
        false,
        &change(),
    )
    .await?;
    tx.commit().await?;
    let resumed_leases =
        store::lease_items(&db.pool, campaign, Duration::from_secs(30), 16).await?;
    for id in [held_after_driver_stop, pre_metadata_stopped] {
        let resumed_item = resumed_leases
            .iter()
            .find(|item| item.subject_id == id)
            .expect("held item leased after hold");
        assert_eq!(
            driver.upgrade(&campaign_row, resumed_item).await,
            ItemOutcome::Done
        );
        store::finish_item(&db.pool, resumed_item, &ItemOutcome::Done).await?;
    }
    assert_eq!(dispatcher.tracked_driver_stops(), baseline_stops);
    assert_eq!(dispatcher.tracked_started_on_target(), baseline_started + 2);

    // Coexisting `sandboxes` upgrade + third-environment skip + campaign eviction.
    let coexisting_upgraded = resident_on_source(&db.pool, SandboxLifecycle::Started, node).await?;
    let moved_elsewhere = resident_on_source(&db.pool, SandboxLifecycle::Started, node).await?;
    for id in [coexisting_upgraded, moved_elsewhere] {
        dispatcher.script_start(
            id,
            vec![Err(DispatchError::Retryable("slot busy".to_owned()))],
        );
    }
    record(&db.pool, campaign, &[coexisting_upgraded, moved_elsewhere]).await?;
    let stop_leases = store::lease_items(&db.pool, campaign, Duration::from_secs(30), 16).await?;
    for id in [coexisting_upgraded, moved_elsewhere] {
        let lease = stop_leases
            .iter()
            .find(|item| item.subject_id == id)
            .expect("coexistence item leased");
        let outcome = driver.upgrade(&campaign_row, lease).await;
        assert_eq!(outcome, ItemOutcome::Retry(ItemError::new("slot busy")));
        store::finish_item(&db.pool, lease, &outcome).await?;
    }
    let stopped_before = store::subject_sandbox(&db.pool, coexisting_upgraded)
        .await?
        .expect("coexisting_upgraded exists");
    let stopped_campaign = insert_campaign(&db.pool, seed()).await?;
    record(&db.pool, stopped_campaign, &[coexisting_upgraded]).await?;
    let stopped_campaign_row = store::get_record(&db.pool, stopped_campaign)
        .await?
        .expect("stopped campaign exists")
        .campaign;
    let stopped_leases =
        store::lease_items(&db.pool, stopped_campaign, Duration::from_secs(30), 16).await?;
    let stopped_driver = SandboxDriver::new(db.pool.clone(), K8S.as_str(), dispatcher.clone());
    assert_eq!(
        stopped_driver
            .upgrade(&stopped_campaign_row, &stopped_leases[0])
            .await,
        ItemOutcome::Done
    );
    store::finish_item(&db.pool, &stopped_leases[0], &ItemOutcome::Done).await?;
    let stopped_after = store::subject_sandbox(&db.pool, coexisting_upgraded)
        .await?
        .expect("coexisting_upgraded still exists");
    assert_eq!(stopped_after.lifecycle, "stopped");
    assert_eq!(
        stopped_after.lifecycle_changed_at, stopped_before.lifecycle_changed_at,
        "upgrade_stopped updates execution without changing sandboxes.lifecycle_changed_at"
    );
    set_bound(&db.pool, moved_elsewhere, environment(99)).await?;

    let retry_leases = store::lease_items(&db.pool, campaign, Duration::from_secs(30), 16).await?;
    let coexisting_retry = retry_leases
        .iter()
        .find(|item| item.subject_id == coexisting_upgraded)
        .expect("coexisting_upgraded re-leased");
    let elsewhere_retry = retry_leases
        .iter()
        .find(|item| item.subject_id == moved_elsewhere)
        .expect("moved_elsewhere re-leased");
    assert_eq!(
        driver.upgrade(&campaign_row, coexisting_retry).await,
        ItemOutcome::Done
    );
    store::finish_item(&db.pool, coexisting_retry, &ItemOutcome::Done).await?;
    assert_eq!(
        store::subject_sandbox(&db.pool, coexisting_upgraded)
            .await?
            .expect("coexisting_upgraded started")
            .lifecycle,
        "started"
    );
    let elsewhere_outcome = driver.upgrade(&campaign_row, elsewhere_retry).await;
    assert_eq!(
        elsewhere_outcome,
        ItemOutcome::Skipped(ItemError::new(ON_NEITHER_END))
    );
    store::finish_item(&db.pool, elsewhere_retry, &elsewhere_outcome).await?;
    assert_eq!(dispatcher.tracked_driver_stops(), baseline_stops);
    assert_eq!(dispatcher.tracked_started_on_target(), baseline_started + 3);

    driver.retain_campaigns(std::slice::from_ref(&campaign_row));
    assert_eq!(dispatcher.tracked_driver_stops(), baseline_stops);
    assert_eq!(dispatcher.tracked_started_on_target(), baseline_started + 3);
    driver.retain_campaigns(&[]);
    assert_eq!(dispatcher.tracked_driver_stops(), 0);
    assert_eq!(dispatcher.tracked_started_on_target(), 0);
    Ok(())
}
