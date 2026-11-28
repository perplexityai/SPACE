//! The verbs over a campaign kind whose subjects have no node: template
//! releases.

use super::*;

fn templates(seed: CampaignSeed) -> CampaignSeed {
    CampaignSeed {
        kind: CampaignKind::Templates,
        ..seed
    }
}

/// `count` template release ids recorded as pending items of `campaign`.
async fn pending_releases(
    db: &TestDb,
    campaign: CampaignId,
    count: usize,
) -> TestResult<Vec<Uuid>> {
    let releases: Vec<Uuid> = (0..count).map(|_| Uuid::now_v7()).collect();
    let recorded = insert_pending(
        &db.pool,
        campaign,
        SubjectKind::TemplateRelease,
        &releases,
        MAX_ATTEMPTS,
    )
    .await?;
    assert_eq!(recorded, count as u64);
    Ok(releases)
}

#[tokio::test(flavor = "current_thread")]
async fn items_go_only_into_campaigns_of_their_subject_kind() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let sandboxes = insert_campaign(&db.pool, CampaignSeed::default()).await?;
    let force_restarts = insert_campaign(
        &db.pool,
        CampaignSeed {
            kind: CampaignKind::SandboxForceRestarts,
            ..CampaignSeed::default()
        },
    )
    .await?;
    let releases = insert_campaign(&db.pool, templates(CampaignSeed::default())).await?;
    let subjects = [Uuid::now_v7()];

    for sandbox_campaign in [sandboxes, force_restarts] {
        assert_eq!(
            insert_pending(
                &db.pool,
                sandbox_campaign,
                SubjectKind::TemplateRelease,
                &subjects,
                MAX_ATTEMPTS
            )
            .await?,
            0
        );
    }
    assert_eq!(
        insert_pending(
            &db.pool,
            force_restarts,
            SubjectKind::Sandbox,
            &subjects,
            MAX_ATTEMPTS
        )
        .await?,
        1
    );
    assert_eq!(
        insert_pending(
            &db.pool,
            releases,
            SubjectKind::Sandbox,
            &subjects,
            MAX_ATTEMPTS
        )
        .await?,
        0
    );
    assert_eq!(
        insert_pending(
            &db.pool,
            releases,
            SubjectKind::TemplateRelease,
            &subjects,
            MAX_ATTEMPTS
        )
        .await?,
        1
    );
    let kind = sqlx::query_scalar::<_, String>(sql!(
        "SELECT subject_kind FROM runtime_upgrade_items WHERE campaign_id = $1"
    ))
    .bind(releases)
    .fetch_one(&db.pool)
    .await?;
    assert_eq!(kind, "template_release");
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn template_items_are_paced_by_the_cluster_cap_alone() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    // A per-node cap of 1 would hold a sandbox campaign to one item per
    // node; template releases have no node, so only the cluster cap counts.
    let campaign = insert_campaign(
        &db.pool,
        templates(CampaignSeed {
            per_node: 1,
            per_cluster: 3,
            ..CampaignSeed::default()
        }),
    )
    .await?;
    let releases = pending_releases(&db, campaign, 5).await?;

    let first = lease(&db, campaign).await?;
    assert_eq!(first.len(), 3);
    assert!(first.iter().all(|item| item.node_id.is_none()
        && item.subject_kind == SubjectKind::TemplateRelease));
    assert_eq!(
        first.iter().map(|item| item.subject_id).collect::<Vec<_>>(),
        releases[..3]
    );
    assert!(
        lease(&db, campaign).await?.is_empty(),
        "the cluster cap is full"
    );

    finish_item(&db.pool, &first[0], &ItemOutcome::Done).await?;
    let second = lease(&db, campaign).await?;
    assert_eq!(
        second
            .iter()
            .map(|item| item.subject_id)
            .collect::<Vec<_>>(),
        [releases[3]]
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn a_template_item_renews_finishes_and_redrives_under_its_own_kind() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let campaign = insert_campaign(&db.pool, templates(CampaignSeed::default())).await?;
    pending_releases(&db, campaign, 2).await?;
    let leased = lease(&db, campaign).await?;
    assert_eq!(leased.len(), 2);

    assert!(renew_lease(&db.pool, &leased[0], LEASE).await?);
    assert!(finish_item(&db.pool, &leased[0], &ItemOutcome::Done).await?);
    lapse_leases(&db.pool, campaign).await?;
    assert_eq!(
        redrive_expired(&db.pool, campaign, MAX_ATTEMPTS).await?,
        RedriveReport {
            requeued: 1,
            failed: 0,
        }
    );
    assert!(!finish_item(&db.pool, &leased[1], &ItemOutcome::Done).await?);
    assert_eq!(
        item_counts(&db.pool, campaign).await?,
        ItemCounts {
            pending: 1,
            done: 1,
            ..ItemCounts::default()
        }
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn a_template_canary_leases_one_release_and_waits() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let campaign = insert_campaign(
        &db.pool,
        templates(CampaignSeed {
            phase: CampaignPhase::Canary,
            per_cluster: 5,
            ..CampaignSeed::default()
        }),
    )
    .await?;
    pending_releases(&db, campaign, 3).await?;
    let canary = lease(&db, campaign).await?;
    assert_eq!(canary.len(), 1);
    assert!(lease(&db, campaign).await?.is_empty());
    finish_item(&db.pool, &canary[0], &ItemOutcome::Done).await?;
    assert!(lease(&db, campaign).await?.is_empty(), "the canary waits");
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn completion_ignores_skipped_items_and_raises_the_generation() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let campaign = insert_campaign(&db.pool, templates(CampaignSeed::default())).await?;
    pending_releases(&db, campaign, 2).await?;
    let leased = lease(&db, campaign).await?;
    finish_item(&db.pool, &leased[0], &ItemOutcome::Done).await?;
    finish_item(
        &db.pool,
        &leased[1],
        &ItemOutcome::Skipped(ItemError::new("the release was deleted")),
    )
    .await?;

    assert!(complete_if_drained(&db.pool, campaign, 0).await?);
    let record = get_record(&db.pool, campaign).await?.expect("exists");
    assert_eq!(record.campaign.phase, CampaignPhase::Completed);
    assert_eq!(record.campaign.generation, 1);
    assert!(record.finished_at.is_some());
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn continuing_run_distinguishes_fresh_or_reopened_leases_from_retries_and_redrives()
-> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let campaign = insert_campaign(
        &db.pool,
        CampaignSeed {
            kind: CampaignKind::SandboxForceRestarts,
            failure_budget: 5,
            ..CampaignSeed::default()
        },
    )
    .await?;
    let sandbox = Uuid::now_v7();
    insert_pending(
        &db.pool,
        campaign,
        SubjectKind::Sandbox,
        &[sandbox],
        MAX_ATTEMPTS,
    )
    .await?;

    let first = &lease(&db, campaign).await?[0];
    assert_eq!(
        continuing_run(&db.pool, first).await?,
        Some(false),
        "first lease of a newly inserted item isn't continuing an earlier attempt"
    );

    finish_item(
        &db.pool,
        first,
        &ItemOutcome::Retry(ItemError::new("stopped_at read unavailable")),
    )
    .await?;
    let after_unstarted_retry = &lease(&db, campaign).await?[0];
    assert_eq!(
        continuing_run(&db.pool, after_unstarted_retry).await?,
        Some(false),
        "pre-start retry keeps the run marked unstarted"
    );

    lapse_leases(&db.pool, campaign).await?;
    redrive_expired(&db.pool, campaign, 10).await?;
    let after_unstarted_redrive = &lease(&db, campaign).await?[0];
    assert_eq!(
        continuing_run(&db.pool, after_unstarted_redrive).await?,
        Some(false),
        "lease expiry before mark_run_started keeps the run marked unstarted"
    );

    finish_item(
        &db.pool,
        after_unstarted_redrive,
        &ItemOutcome::Failed(ItemError::new("the upgrade didn't finish within 60s")),
    )
    .await?;
    pause_campaign(
        &Db::from(db.pool.clone()),
        campaign,
        &PhaseChange::default(),
    )
    .await?;
    resume_campaign(
        &Db::from(db.pool.clone()),
        campaign,
        &PhaseChange::default(),
        true,
    )
    .await?;
    let after_unstarted_deadline = &lease(&db, campaign).await?[0];
    assert_eq!(
        continuing_run(&db.pool, after_unstarted_deadline).await?,
        Some(false),
        "deadline failure before mark_run_started stays unstarted across retry_failed"
    );

    assert!(mark_run_started(&db.pool, after_unstarted_deadline).await?);
    assert_eq!(
        continuing_run(&db.pool, after_unstarted_deadline).await?,
        Some(true)
    );

    finish_item(
        &db.pool,
        after_unstarted_deadline,
        &ItemOutcome::Release(ItemError::new("")),
    )
    .await?;
    let after_release = &lease(&db, campaign).await?[0];
    assert_eq!(continuing_run(&db.pool, after_release).await?, Some(true));

    finish_item(
        &db.pool,
        after_release,
        &ItemOutcome::Retry(ItemError::new(
            "before fresh run started: continuation read failed",
        )),
    )
    .await?;
    let after_started_unstarted_tag = &lease(&db, campaign).await?[0];
    assert_eq!(
        continuing_run(&db.pool, after_started_unstarted_tag).await?,
        Some(true),
        "an already-started run strips any unstarted prefix on finish_item"
    );

    finish_item(
        &db.pool,
        after_started_unstarted_tag,
        &ItemOutcome::Retry(ItemError::new("")),
    )
    .await?;
    let after_retry = &lease(&db, campaign).await?[0];
    assert_eq!(continuing_run(&db.pool, after_retry).await?, Some(true));

    lapse_leases(&db.pool, campaign).await?;
    redrive_expired(&db.pool, campaign, 10).await?;
    assert_eq!(
        continuing_run(&db.pool, after_retry).await?,
        None,
        "replaced lease token isn't active"
    );
    let after_redrive = &lease(&db, campaign).await?[0];
    assert_eq!(continuing_run(&db.pool, after_redrive).await?, Some(true));

    finish_item(
        &db.pool,
        after_redrive,
        &ItemOutcome::Failed(ItemError::new("")),
    )
    .await?;
    pause_campaign(
        &Db::from(db.pool.clone()),
        campaign,
        &PhaseChange::default(),
    )
    .await?;
    resume_campaign(
        &Db::from(db.pool.clone()),
        campaign,
        &PhaseChange::default(),
        true,
    )
    .await?;
    let after_retry_failed = &lease(&db, campaign).await?[0];
    assert_eq!(
        continuing_run(&db.pool, after_retry_failed).await?,
        Some(true)
    );

    finish_item(
        &db.pool,
        after_retry_failed,
        &ItemOutcome::Skipped(ItemError::new("user stopped")),
    )
    .await?;
    insert_pending(&db.pool, campaign, SubjectKind::Sandbox, &[sandbox], 20).await?;
    let after_reopen = &lease(&db, campaign).await?[0];
    assert_eq!(
        continuing_run(&db.pool, after_reopen).await?,
        Some(false),
        "reopening a skipped item clears last_error and starts a fresh run"
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn released_and_retried_items_rotate_behind_untried_pending_items_within_scan_limit()
-> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let placed_node = node(&db.pool).await?;
    let second_node = node(&db.pool).await?;

    for kind in CampaignKind::ALL {
        let bindings: &[bool] = match kind {
            CampaignKind::Sandboxes | CampaignKind::SandboxForceRestarts => &[true, false],
            CampaignKind::Templates => &[false],
        };
        let subject_kind = kind.subject_kind();
        for &bind_node in bindings {
            let campaign = insert_campaign_with_kind(
                &db.pool,
                CampaignSeed {
                    per_node: 1,
                    per_cluster: 1,
                    ..CampaignSeed::default()
                },
                kind.as_str(),
            )
            .await?;

            let mut subjects = match subject_kind {
                SubjectKind::Sandbox => {
                    pending_on(&db, campaign, bind_node.then_some(placed_node), 4).await?
                }
                SubjectKind::TemplateRelease => {
                    let ids: Vec<Uuid> = (0..4).map(|_| Uuid::now_v7()).collect();
                    insert_pending(&db.pool, campaign, SubjectKind::TemplateRelease, &ids, 10)
                        .await?;
                    ids
                }
            };
            subjects.sort();

            // With `scan_limit = 2` (< 4 pending items) and `per_cluster = 1`,
            // `lease_items_after` advances `cursor` around `(campaign_id, phase,
            // subject_id)` so releasing or retrying the lowest `subject_id` item
            // walks every subject in order before wrapping around.
            let mut conn = db.pool.acquire().await?;
            let mut cursor = None;
            for (idx, expected) in subjects.iter().copied().enumerate() {
                let leased = lease_items_after(&mut *conn, campaign, &mut cursor, LEASE, 2).await?;
                assert_eq!(leased.len(), 1);
                assert_eq!(
                    leased[0].subject_id, expected,
                    "{kind:?} (bind_node={bind_node}) step {idx} must rotate past held items"
                );
                let outcome = if idx == 1 {
                    ItemOutcome::Retry(ItemError::new("transient"))
                } else {
                    ItemOutcome::Release(ItemError::new("held"))
                };
                assert!(finish_item(&db.pool, &leased[0], &outcome).await?);
            }

            let wrapped = lease_items_after(&mut *conn, campaign, &mut cursor, LEASE, 2).await?;
            assert_eq!(wrapped.len(), 1);
            assert_eq!(
                wrapped[0].subject_id, subjects[0],
                "{kind:?} (bind_node={bind_node}) wraps back to the earliest released item once all rotated"
            );
            assert!(finish_item(&db.pool, &wrapped[0], &ItemOutcome::Done).await?);
        }
    }

    // In `canary`, an unstarted `Release` still rotates past `scan_limit` via
    // `canary_cursor`, while once `mark_run_started` commits, that active canary
    // stays pinned ahead of untouched items across both `Release` and `Retry` so
    // a second sandbox isn't stopped mid-canary.
    let canary_campaign = insert_campaign_with_kind(
        &db.pool,
        CampaignSeed {
            phase: CampaignPhase::Canary,
            per_node: 1,
            per_cluster: 1,
            ..CampaignSeed::default()
        },
        CampaignKind::SandboxForceRestarts.as_str(),
    )
    .await?;
    let mut canary_subjects = pending_on(&db, canary_campaign, Some(placed_node), 3).await?;
    canary_subjects.sort();
    let mut conn = db.pool.acquire().await?;
    let mut canary_cursor = None;

    let unstarted =
        lease_items_after(&mut *conn, canary_campaign, &mut canary_cursor, LEASE, 1).await?;
    assert_eq!(unstarted[0].subject_id, canary_subjects[0]);
    assert!(
        finish_item(
            &db.pool,
            &unstarted[0],
            &ItemOutcome::Release(ItemError::new("creating"))
        )
        .await?
    );

    let rotated_canary =
        lease_items_after(&mut *conn, canary_campaign, &mut canary_cursor, LEASE, 1).await?;
    assert_eq!(
        rotated_canary[0].subject_id, canary_subjects[1],
        "unstarted Release rotates past scan_limit even in canary"
    );
    assert!(mark_run_started(&db.pool, &rotated_canary[0]).await?);
    assert!(
        finish_item(
            &db.pool,
            &rotated_canary[0],
            &ItemOutcome::Release(ItemError::new("held stopped"))
        )
        .await?
    );

    let pinned_after_release =
        lease_items_after(&mut *conn, canary_campaign, &mut canary_cursor, LEASE, 1).await?;
    assert_eq!(
        pinned_after_release[0].subject_id, canary_subjects[1],
        "started canary stays pinned ahead of untouched items on Release"
    );
    assert!(
        finish_item(
            &db.pool,
            &pinned_after_release[0],
            &ItemOutcome::Retry(ItemError::new("start rpc timeout"))
        )
        .await?
    );

    let pinned_after_retry =
        lease_items_after(&mut *conn, canary_campaign, &mut canary_cursor, LEASE, 1).await?;
    assert_eq!(
        pinned_after_retry[0].subject_id, canary_subjects[1],
        "started canary stays pinned ahead of untouched items on Retry"
    );
    assert!(
        finish_item(
            &db.pool,
            &pinned_after_retry[0],
            &ItemOutcome::Release(ItemError::new("node temporarily unreachable"))
        )
        .await?
    );

    // If the started canary's node goes temporarily unreachable while an
    // untouched candidate sits on another reachable node, or another campaign
    // holds the same subject in flight (`held_elsewhere`), `canary` must return
    // empty (`[]`) across both node-bound campaign kinds rather than starting a
    // second untouched sandbox.
    let untouched_other_node = pending_on(&db, canary_campaign, Some(second_node), 1).await?[0];
    assert!(untouched_other_node > canary_subjects[1]);
    upsert_snapshot(
        &db.pool,
        placed_node,
        SnapshotSeed {
            reachable: false,
            ..SnapshotSeed::default()
        },
    )
    .await?;
    assert!(
        lease_items(&mut *conn, canary_campaign, LEASE, 4)
            .await?
            .is_empty(),
        "started force-restart canary on an unreachable node must not lease an untouched sandbox on another reachable node"
    );
    upsert_snapshot(&db.pool, placed_node, SnapshotSeed::default()).await?;

    let sandboxes_canary = insert_campaign_with_kind(
        &db.pool,
        CampaignSeed {
            phase: CampaignPhase::Canary,
            per_node: 1,
            per_cluster: 1,
            ..CampaignSeed::default()
        },
        CampaignKind::Sandboxes.as_str(),
    )
    .await?;
    let started_sandbox_canary = pending_on(&db, sandboxes_canary, Some(placed_node), 1).await?[0];
    let _untouched_sandbox_canary =
        pending_on(&db, sandboxes_canary, Some(second_node), 1).await?[0];
    let first_sandbox_canary = lease_items(&mut *conn, sandboxes_canary, LEASE, 2).await?;
    assert_eq!(first_sandbox_canary[0].subject_id, started_sandbox_canary);
    assert!(
        finish_item(
            &db.pool,
            &first_sandbox_canary[0],
            &ItemOutcome::Retry(ItemError::new("transient upgrade rpc error")),
        )
        .await?
    );
    upsert_snapshot(
        &db.pool,
        placed_node,
        SnapshotSeed {
            reachable: false,
            ..SnapshotSeed::default()
        },
    )
    .await?;
    assert!(
        lease_items(&mut *conn, sandboxes_canary, LEASE, 4)
            .await?
            .is_empty(),
        "retried sandboxes canary on an unreachable node must not lease a second untouched sandbox on another reachable node"
    );
    upsert_snapshot(&db.pool, placed_node, SnapshotSeed::default()).await?;

    let other_campaign = insert_campaign(&db.pool, CampaignSeed::default()).await?;
    insert_pending(
        &db.pool,
        other_campaign,
        SubjectKind::Sandbox,
        &[canary_subjects[1]],
        MAX_ATTEMPTS,
    )
    .await?;
    let other_leased = lease_items(&mut *conn, other_campaign, LEASE, 1).await?;
    assert_eq!(other_leased[0].subject_id, canary_subjects[1]);
    assert!(
        lease_items(&mut *conn, canary_campaign, LEASE, 1)
            .await?
            .is_empty(),
        "started canary held in flight by another campaign must not lease an untouched sandbox"
    );
    assert!(finish_item(&db.pool, &other_leased[0], &ItemOutcome::Done).await?);

    let resumed_canary = lease_items(&mut *conn, canary_campaign, LEASE, 1).await?;
    assert_eq!(resumed_canary[0].subject_id, canary_subjects[1]);
    assert!(
        finish_item(
            &db.pool,
            &resumed_canary[0],
            &ItemOutcome::Release(ItemError::new("paused before second canary check"))
        )
        .await?
    );

    // If a campaign earlier ran in `rolling` and started two items across two
    // nodes (`placed_node` and `second_node`) before pausing and resuming in
    // `canary`, one unreachable started item on `placed_node` must not block
    // leasing the reachable started item on `second_node`, while untouched
    // items on a third reachable node remain blocked.
    let third_node = node(&db.pool).await?;
    let resumed_from_rolling = insert_campaign_with_kind(
        &db.pool,
        CampaignSeed {
            phase: CampaignPhase::Rolling,
            per_node: 1,
            per_cluster: 2,
            ..CampaignSeed::default()
        },
        CampaignKind::SandboxForceRestarts.as_str(),
    )
    .await?;
    let started_on_placed = pending_on(&db, resumed_from_rolling, Some(placed_node), 1).await?[0];
    let started_on_second = pending_on(&db, resumed_from_rolling, Some(second_node), 1).await?[0];
    let _untouched_on_third = pending_on(&db, resumed_from_rolling, Some(third_node), 1).await?[0];
    let rolling_leased = lease_items(&mut *conn, resumed_from_rolling, LEASE, 4).await?;
    assert_eq!(rolling_leased.len(), 2);
    for item in &rolling_leased {
        assert!(mark_run_started(&db.pool, item).await?);
        assert!(
            finish_item(
                &db.pool,
                item,
                &ItemOutcome::Release(ItemError::new("started during rolling")),
            )
            .await?
        );
    }
    sqlx::query(sql!(
        "UPDATE runtime_upgrade_campaigns SET phase = 'canary' WHERE id = $1"
    ))
    .bind(resumed_from_rolling)
    .execute(&db.pool)
    .await?;
    upsert_snapshot(
        &db.pool,
        placed_node,
        SnapshotSeed {
            reachable: false,
            ..SnapshotSeed::default()
        },
    )
    .await?;
    let canary_from_two_started = lease_items(&mut *conn, resumed_from_rolling, LEASE, 4).await?;
    assert_eq!(canary_from_two_started.len(), 1);
    assert_eq!(
        canary_from_two_started[0].subject_id, started_on_second,
        "reachable started item must still lease in canary when another started item's node is unreachable"
    );
    assert!(
        finish_item(
            &db.pool,
            &canary_from_two_started[0],
            &ItemOutcome::Release(ItemError::new("still pending")),
        )
        .await?
    );
    upsert_snapshot(
        &db.pool,
        second_node,
        SnapshotSeed {
            reachable: false,
            ..SnapshotSeed::default()
        },
    )
    .await?;
    assert!(
        lease_items(&mut *conn, resumed_from_rolling, LEASE, 4)
            .await?
            .is_empty(),
        "when all started items sit on unreachable nodes, canary must not lease untouched items"
    );
    upsert_snapshot(&db.pool, placed_node, SnapshotSeed::default()).await?;
    upsert_snapshot(&db.pool, second_node, SnapshotSeed::default()).await?;
    assert_ne!(started_on_placed, started_on_second);

    // `started_pending_target_sandboxes` includes `paused` target-bound items,
    // skips nodes at `max_in_flight_per_node`, and honors `excluded_nodes`.
    let stranded_campaign = insert_campaign_with_kind(
        &db.pool,
        CampaignSeed {
            phase: CampaignPhase::Rolling,
            per_node: 1,
            per_cluster: 4,
            ..CampaignSeed::default()
        },
        CampaignKind::SandboxForceRestarts.as_str(),
    )
    .await?;
    let node_a_in_flight = pending_on(&db, stranded_campaign, Some(placed_node), 1).await?[0];
    let node_a_waiting = pending_on(&db, stranded_campaign, Some(placed_node), 1).await?[0];
    let node_b_paused = insert_sandbox(
        &db.pool,
        SubstrateKind::Kubernetes,
        SandboxLifecycle::Paused,
        Some(second_node),
    )
    .await?;
    insert_pending(
        &mut *conn,
        stranded_campaign,
        SubjectKind::Sandbox,
        &[node_b_paused],
        MAX_ATTEMPTS,
    )
    .await?;
    for sbx in [node_a_waiting, node_b_paused] {
        sqlx::query(sql!(
            "UPDATE sandbox_observed_states
             SET execution_metadata = jsonb_build_object('environment', $2::text)
             WHERE sandbox_id = $1"
        ))
        .bind(sbx)
        .bind(environment(1).to_string())
        .execute(&db.pool)
        .await?;
    }
    for sbx in [node_a_waiting, node_b_paused] {
        let leased_one =
            lease_selected_items(&mut *conn, stranded_campaign, &[sbx], LEASE, 1).await?;
        assert_eq!(leased_one.len(), 1);
        assert!(mark_run_started(&db.pool, &leased_one[0]).await?);
        assert!(
            finish_item(
                &db.pool,
                &leased_one[0],
                &ItemOutcome::Release(ItemError::new("started on target")),
            )
            .await?
        );
    }
    let keep_in_flight =
        lease_selected_items(&mut *conn, stranded_campaign, &[node_a_in_flight], LEASE, 1)
            .await?
            .pop()
            .expect("node_a_in_flight leased");
    assert!(mark_run_started(&db.pool, &keep_in_flight).await?);

    let saturated_probe = started_pending_target_sandboxes(
        &db.pool,
        stranded_campaign,
        SubstrateKind::Kubernetes,
        &[],
        None,
        1,
    )
    .await?;
    assert!(saturated_probe.gate_open);
    assert_eq!(saturated_probe.probed, 1);
    assert_eq!(saturated_probe.last_probed, Some(node_a_waiting));
    assert!(
        saturated_probe.items.is_empty(),
        "bounded limit=1 window probes only node_a_waiting and returns its cursor when saturated"
    );
    let page = started_pending_target_sandboxes(
        &db.pool,
        stranded_campaign,
        SubstrateKind::Kubernetes,
        &[],
        saturated_probe.last_probed,
        1,
    )
    .await?;
    assert!(page.gate_open);
    assert_eq!(page.probed, 1);
    assert_eq!(page.items.len(), 1);
    assert_eq!(
        page.items[0].sandbox_id, node_b_paused,
        "advancing past saturated placed_node returns paused target item on second_node"
    );
    assert_eq!(page.items[0].node_id, Some(second_node));
    assert_eq!(page.items[0].remaining_node_slots, Some(1));
    assert_eq!(page.items[0].remaining_cluster_slots, 3);
    assert!(!page.items[0].started_on_target);

    assert!(
        finish_item(
            &db.pool,
            &keep_in_flight,
            &ItemOutcome::Release(ItemError::new("free placed_node")),
        )
        .await?
    );
    let excluded_page = started_pending_target_sandboxes(
        &db.pool,
        stranded_campaign,
        SubstrateKind::Kubernetes,
        &[placed_node],
        None,
        3,
    )
    .await?;
    assert!(excluded_page.gate_open);
    assert_eq!(excluded_page.probed, 3);
    assert_eq!(excluded_page.items.len(), 1);
    assert_eq!(excluded_page.items[0].sandbox_id, node_b_paused);

    let source_prefix = started_pending_target_sandboxes(
        &db.pool,
        stranded_campaign,
        SubstrateKind::Kubernetes,
        &[],
        None,
        1,
    )
    .await?;
    assert_eq!(source_prefix.probed, 1);
    assert_eq!(source_prefix.last_probed, Some(node_a_in_flight));
    assert!(source_prefix.items.is_empty());
    let first_by_id = started_pending_target_sandboxes(
        &db.pool,
        stranded_campaign,
        SubstrateKind::Kubernetes,
        &[],
        source_prefix.last_probed,
        1,
    )
    .await?;
    assert_eq!(first_by_id.items.len(), 1);
    assert_eq!(first_by_id.items[0].sandbox_id, node_a_waiting);
    let second_by_id = started_pending_target_sandboxes(
        &db.pool,
        stranded_campaign,
        SubstrateKind::Kubernetes,
        &[],
        Some(node_a_waiting),
        1,
    )
    .await?;
    assert_eq!(second_by_id.items.len(), 1);
    assert_eq!(second_by_id.items[0].sandbox_id, node_b_paused);
    let past_tail = started_pending_target_sandboxes(
        &db.pool,
        stranded_campaign,
        SubstrateKind::Kubernetes,
        &[],
        Some(node_b_paused),
        1,
    )
    .await?;
    assert!(past_tail.gate_open);
    assert_eq!(past_tail.probed, 0);
    assert_eq!(past_tail.last_probed, None);
    assert!(past_tail.items.is_empty());

    // An item marked via `mark_leased_started_on_target` / `mark_started_on_target`
    // keeps its durable target-settled marker across `Retry`, `Release`, `Failed` +
    // `resume(retry_failed = true)`, and `redrive_expired`, bypasses node
    // reachability and per-node pacing once settled, and remains selectable by
    // `started_pending_target_sandboxes` even after its `sandboxes` row is deleted.
    let deleted_after_target_start =
        pending_on(&db, stranded_campaign, Some(second_node), 1).await?[0];
    let leased_target_start = lease_selected_items(
        &mut *conn,
        stranded_campaign,
        &[deleted_after_target_start],
        LEASE,
        1,
    )
    .await?
    .pop()
    .expect("deleted_after_target_start leased");
    assert!(
        !mark_leased_starting_on_target(&db.pool, &leased_target_start).await?,
        "unstarted lease must not stamp starting-on-target"
    );
    assert!(
        !mark_leased_started_on_target(&db.pool, &leased_target_start).await?,
        "unstarted lease must not stamp target-start settlement"
    );
    assert!(mark_run_started(&db.pool, &leased_target_start).await?);
    let mut wrong_token = leased_target_start.clone();
    wrong_token.lease_token = Uuid::now_v7();
    assert!(
        !mark_leased_starting_on_target(&db.pool, &wrong_token).await?,
        "stale lease token must not stamp starting-on-target"
    );
    assert!(
        !mark_leased_started_on_target(&db.pool, &wrong_token).await?,
        "stale lease token must not stamp target-start settlement"
    );
    assert_eq!(
        starting_on_target(&db.pool, &leased_target_start).await?,
        Some(false)
    );
    assert_eq!(
        started_on_target(&db.pool, &leased_target_start).await?,
        Some(false)
    );
    assert!(mark_leased_starting_on_target(&db.pool, &leased_target_start).await?);
    assert_eq!(
        starting_on_target(&db.pool, &leased_target_start).await?,
        Some(true)
    );
    sqlx::query(sql!("UPDATE sandboxes SET lifecycle = $2 WHERE id = $1"))
        .bind(deleted_after_target_start)
        .bind(SandboxLifecycle::Starting.as_str())
        .execute(&db.pool)
        .await?;
    assert!(
        finish_item(
            &db.pool,
            &leased_target_start,
            &ItemOutcome::Release(ItemError::new("lost pre-dispatch ack")),
        )
        .await?
    );
    let starting_page = started_pending_target_sandboxes(
        &db.pool,
        stranded_campaign,
        SubstrateKind::Kubernetes,
        &[],
        Some(node_b_paused),
        4,
    )
    .await?;
    assert_eq!(starting_page.items.len(), 1);
    assert_eq!(
        starting_page.items[0].sandbox_id,
        deleted_after_target_start
    );
    assert!(starting_page.items[0].starting_on_target);
    assert!(!starting_page.items[0].started_on_target);
    let leased_target_start = lease_selected_items(
        &mut *conn,
        stranded_campaign,
        &[deleted_after_target_start],
        LEASE,
        1,
    )
    .await?
    .pop()
    .expect("starting item leased via lease_selected_items");
    assert_eq!(
        starting_on_target(&db.pool, &leased_target_start).await?,
        Some(true)
    );
    assert!(
        clear_starting_on_target(
            &db.pool,
            stranded_campaign,
            SubjectKind::Sandbox,
            deleted_after_target_start,
        )
        .await?
    );
    assert_eq!(
        starting_on_target(&db.pool, &leased_target_start).await?,
        Some(false)
    );
    assert_eq!(
        continuing_run(&db.pool, &leased_target_start).await?,
        Some(true)
    );
    assert!(
        mark_starting_on_target(
            &db.pool,
            stranded_campaign,
            SubjectKind::Sandbox,
            deleted_after_target_start,
        )
        .await?
    );
    sqlx::query(sql!("UPDATE sandboxes SET lifecycle = $2 WHERE id = $1"))
        .bind(deleted_after_target_start)
        .bind(SandboxLifecycle::Started.as_str())
        .execute(&db.pool)
        .await?;
    assert!(mark_leased_started_on_target(&db.pool, &leased_target_start).await?);
    assert_eq!(
        starting_on_target(&db.pool, &leased_target_start).await?,
        Some(false)
    );
    assert!(
        mark_started_on_target(
            &db.pool,
            stranded_campaign,
            SubjectKind::Sandbox,
            deleted_after_target_start,
        )
        .await?
    );
    assert_eq!(
        started_on_target(&db.pool, &leased_target_start).await?,
        Some(true)
    );
    assert!(
        finish_item(
            &db.pool,
            &leased_target_start,
            &ItemOutcome::Retry(ItemError::new("lost post-start ack")),
        )
        .await?
    );
    // Even if the node that published the settled sandbox goes unreachable,
    // receipt-only completion still surfaces in `started_pending_target_sandboxes`
    // (including when `excluded_nodes` names `second_node`) and leases with
    // `node_id = None` across both `lease_selected_items` and `lease_items_after`.
    upsert_snapshot(
        &db.pool,
        second_node,
        SnapshotSeed {
            reachable: false,
            ..SnapshotSeed::default()
        },
    )
    .await?;
    let unreachable_settled_page = started_pending_target_sandboxes(
        &db.pool,
        stranded_campaign,
        SubstrateKind::Kubernetes,
        &[second_node],
        Some(node_b_paused),
        4,
    )
    .await?;
    assert_eq!(unreachable_settled_page.items.len(), 1);
    assert_eq!(
        unreachable_settled_page.items[0].sandbox_id,
        deleted_after_target_start
    );
    assert!(unreachable_settled_page.items[0].started_on_target);
    assert_eq!(unreachable_settled_page.items[0].remaining_node_slots, None);
    let leased_unreachable_selected = lease_selected_items(
        &mut *conn,
        stranded_campaign,
        &[deleted_after_target_start],
        LEASE,
        1,
    )
    .await?
    .pop()
    .expect("settled item leased via lease_selected_items despite unreachable node");
    assert_eq!(leased_unreachable_selected.node_id, None);
    assert!(
        finish_item(
            &db.pool,
            &leased_unreachable_selected,
            &ItemOutcome::Release(ItemError::new("retry receipt completion")),
        )
        .await?
    );
    let mut settled_cursor = Some(node_b_paused);
    let leased_unreachable_rolling =
        lease_items_after(&mut *conn, stranded_campaign, &mut settled_cursor, LEASE, 4)
            .await?
            .into_iter()
            .find(|item| item.subject_id == deleted_after_target_start)
            .expect("settled item leased via lease_items_after despite unreachable node");
    assert_eq!(leased_unreachable_rolling.node_id, None);
    assert!(
        finish_item(
            &db.pool,
            &leased_unreachable_rolling,
            &ItemOutcome::Release(ItemError::new("retry after row delete")),
        )
        .await?
    );
    upsert_snapshot(&db.pool, second_node, SnapshotSeed::default()).await?;

    sqlx::query(sql!("DELETE FROM sandboxes WHERE id = $1"))
        .bind(deleted_after_target_start)
        .execute(&db.pool)
        .await?;
    let deleted_page = started_pending_target_sandboxes(
        &db.pool,
        stranded_campaign,
        SubstrateKind::Kubernetes,
        &[],
        Some(node_b_paused),
        4,
    )
    .await?;
    assert_eq!(deleted_page.items.len(), 1);
    assert_eq!(
        deleted_page.items[0].sandbox_id, deleted_after_target_start,
        "deleted sandbox whose prior attempt settled on target must still match started_pending_target_sandboxes"
    );
    assert!(deleted_page.items[0].started_on_target);
    assert_eq!(deleted_page.items[0].sandbox, None);
    let releaved_deleted = lease_selected_items(
        &mut *conn,
        stranded_campaign,
        &[deleted_after_target_start],
        LEASE,
        1,
    )
    .await?
    .pop()
    .expect("deleted_after_target_start re-leased");
    assert_eq!(
        started_on_target(&db.pool, &releaved_deleted).await?,
        Some(true)
    );
    assert!(finish_item(&db.pool, &releaved_deleted, &ItemOutcome::Done).await?);

    // When `max_in_flight_per_node = 1` and `max_in_flight_per_cluster = 2`,
    // skipping `a_2` on `placed_node` while claiming `a_1` and `b_1` must keep
    // `lease_items_after`'s cursor at `a_1` (before the node-cap skip) rather
    // than jumping past `a_2` to `b_1`.
    let node_cap_ring_campaign = insert_campaign(
        &db.pool,
        CampaignSeed {
            from: environment(6),
            to: environment(7),
            phase: CampaignPhase::Rolling,
            per_node: 1,
            per_cluster: 2,
            ..CampaignSeed::default()
        },
    )
    .await?;
    let a_pair = pending_on(&db, node_cap_ring_campaign, Some(placed_node), 2).await?;
    let b_one = pending_on(&db, node_cap_ring_campaign, Some(second_node), 1).await?[0];
    assert!(a_pair[0] < a_pair[1] && a_pair[1] < b_one);
    let mut ring_cursor = None;
    let tick_1 = lease_items_after(
        &mut *conn,
        node_cap_ring_campaign,
        &mut ring_cursor,
        LEASE,
        8,
    )
    .await?;
    let tick_1_ids: Vec<Uuid> = tick_1.iter().map(|item| item.subject_id).collect();
    assert_eq!(tick_1_ids, vec![a_pair[0], b_one]);
    assert_eq!(
        ring_cursor,
        Some(a_pair[0]),
        "cursor must stop at the last claim before the first per-node cap skip"
    );
    for item in &tick_1 {
        assert!(
            finish_item(
                &db.pool,
                item,
                &ItemOutcome::Release(ItemError::new("still settling")),
            )
            .await?
        );
    }
    let tick_2 = lease_items_after(
        &mut *conn,
        node_cap_ring_campaign,
        &mut ring_cursor,
        LEASE,
        8,
    )
    .await?;
    let tick_2_ids: Vec<Uuid> = tick_2.iter().map(|item| item.subject_id).collect();
    assert_eq!(
        tick_2_ids,
        vec![a_pair[1], b_one],
        "second item on placed_node must be leased on the next tick instead of starving behind a_pair[0] and b_one"
    );
    for item in &tick_2 {
        assert!(finish_item(&db.pool, item, &ItemOutcome::Done).await?);
    }

    // In `canary` on `sandboxes` and `templates` campaigns, calling
    // `mark_run_started` before returning `ItemOutcome::Release` (refunding
    // `attempts` back to `0`) pins that item as the active canary subject
    // instead of rotating to an untouched subject.
    let partial_sbx_canary = insert_campaign(
        &db.pool,
        CampaignSeed {
            from: environment(8),
            to: environment(9),
            phase: CampaignPhase::Canary,
            per_node: 2,
            per_cluster: 2,
            ..CampaignSeed::default()
        },
    )
    .await?;
    let partial_sbx_pair = pending_on(&db, partial_sbx_canary, Some(placed_node), 2).await?;
    let mut sbx_canary_cursor = None;
    let first_sbx_canary = lease_items_after(
        &mut *conn,
        partial_sbx_canary,
        &mut sbx_canary_cursor,
        LEASE,
        4,
    )
    .await?;
    assert_eq!(first_sbx_canary.len(), 1);
    assert_eq!(first_sbx_canary[0].subject_id, partial_sbx_pair[0]);
    assert!(mark_run_started(&db.pool, &first_sbx_canary[0]).await?);
    assert!(
        finish_item(
            &db.pool,
            &first_sbx_canary[0],
            &ItemOutcome::Release(ItemError::new("stopped upgrade temporarily held")),
        )
        .await?
    );
    let second_sbx_canary = lease_items_after(
        &mut *conn,
        partial_sbx_canary,
        &mut sbx_canary_cursor,
        LEASE,
        4,
    )
    .await?;
    assert_eq!(second_sbx_canary.len(), 1);
    assert_eq!(
        second_sbx_canary[0].subject_id, partial_sbx_pair[0],
        "sandboxes canary item that committed a stop before releasing must stay pinned ahead of untouched items"
    );
    // Even if the post-stop step fails (`ItemOutcome::Failed` or lease expiry)
    // and `:resume` (`retry_failed = true`) resets `attempts = 0` while
    // `sbx_canary_cursor` still points at `partial_sbx_pair[0]`, the partial
    // move marker survives so `canary` doesn't rotate to `partial_sbx_pair[1]`,
    // including across a subsequent `Release` without a second `mark_run_started`.
    assert!(
        finish_item(
            &db.pool,
            &second_sbx_canary[0],
            &ItemOutcome::Failed(ItemError::new("upgrade_stopped failed after stop")),
        )
        .await?
    );
    pause_campaign(
        &Db::from(db.pool.clone()),
        partial_sbx_canary,
        &PhaseChange::default(),
    )
    .await?;
    resume_campaign(
        &Db::from(db.pool.clone()),
        partial_sbx_canary,
        &PhaseChange::default(),
        true,
    )
    .await?;
    let after_failed_resume = lease_items_after(
        &mut *conn,
        partial_sbx_canary,
        &mut sbx_canary_cursor,
        LEASE,
        4,
    )
    .await?;
    assert_eq!(after_failed_resume.len(), 1);
    assert_eq!(
        after_failed_resume[0].subject_id, partial_sbx_pair[0],
        "partial-move canary pin must survive Failed + resume(retry_failed = true)"
    );
    assert!(
        finish_item(
            &db.pool,
            &after_failed_resume[0],
            &ItemOutcome::Release(ItemError::new("stopped sandbox still held")),
        )
        .await?
    );
    let expired_sbx_canary = lease_items_after(
        &mut *conn,
        partial_sbx_canary,
        &mut sbx_canary_cursor,
        LEASE,
        4,
    )
    .await?;
    assert_eq!(expired_sbx_canary.len(), 1);
    assert_eq!(expired_sbx_canary[0].subject_id, partial_sbx_pair[0]);
    lapse_leases(&db.pool, partial_sbx_canary).await?;
    let redriven = redrive_expired(&db.pool, partial_sbx_canary, 1).await?;
    assert_eq!(redriven.failed, 1);
    pause_campaign(
        &Db::from(db.pool.clone()),
        partial_sbx_canary,
        &PhaseChange::default(),
    )
    .await?;
    resume_campaign(
        &Db::from(db.pool.clone()),
        partial_sbx_canary,
        &PhaseChange::default(),
        true,
    )
    .await?;
    let after_redrive_resume = lease_items_after(
        &mut *conn,
        partial_sbx_canary,
        &mut sbx_canary_cursor,
        LEASE,
        4,
    )
    .await?;
    assert_eq!(after_redrive_resume.len(), 1);
    assert_eq!(
        after_redrive_resume[0].subject_id, partial_sbx_pair[0],
        "partial-move canary pin must survive redrive_expired + resume(retry_failed = true)"
    );
    assert!(finish_item(&db.pool, &after_redrive_resume[0], &ItemOutcome::Done).await?);

    // A `sandboxes` item that previously finished `Skipped` (`attempts = 1`,
    // `last_error = NULL`) and was reopened to `pending` by `insert_pending`
    // hasn't started any work on the reopened run: if its node becomes
    // unreachable, it must not pin `canary` ahead of an untouched item on a
    // reachable node or starve untouched selected items in `lease_selected_items`.
    let offline_node = NodeId::new();
    insert_ready_worker_node(&db.pool, offline_node).await?;
    upsert_snapshot(&db.pool, offline_node, SnapshotSeed::default()).await?;
    let reopened_canary = insert_campaign(
        &db.pool,
        CampaignSeed {
            from: environment(9),
            to: environment(10),
            phase: CampaignPhase::Canary,
            per_node: 1,
            per_cluster: 4,
            ..CampaignSeed::default()
        },
    )
    .await?;
    let reopened_offline = pending_on(&db, reopened_canary, Some(offline_node), 1).await?[0];
    let untouched_online = pending_on(&db, reopened_canary, Some(placed_node), 1).await?[0];
    let mut reopened_cursor = None;
    let first_skip = lease_items_after(&mut *conn, reopened_canary, &mut reopened_cursor, LEASE, 4)
        .await?
        .pop()
        .expect("reopened_offline leased first");
    assert_eq!(first_skip.subject_id, reopened_offline);
    assert!(
        finish_item(
            &db.pool,
            &first_skip,
            &ItemOutcome::Skipped(ItemError::new("running at first scan")),
        )
        .await?
    );
    insert_pending(
        &mut *conn,
        reopened_canary,
        SubjectKind::Sandbox,
        &[reopened_offline],
        MAX_ATTEMPTS,
    )
    .await?;
    upsert_snapshot(
        &db.pool,
        offline_node,
        SnapshotSeed {
            reachable: false,
            ..SnapshotSeed::default()
        },
    )
    .await?;
    let leased_while_offline =
        lease_items_after(&mut *conn, reopened_canary, &mut reopened_cursor, LEASE, 4).await?;
    assert_eq!(leased_while_offline.len(), 1);
    assert_eq!(
        leased_while_offline[0].subject_id, untouched_online,
        "reopened-from-skipped item (attempts > 0, last_error IS NULL) on unreachable node must not pin canary"
    );
    assert!(
        finish_item(
            &db.pool,
            &leased_while_offline[0],
            &ItemOutcome::Release(ItemError::new("before fresh run started: release online")),
        )
        .await?
    );
    upsert_snapshot(&db.pool, offline_node, SnapshotSeed::default()).await?;
    let selected_pair = lease_selected_items(
        &mut *conn,
        reopened_canary,
        &[untouched_online, reopened_offline],
        LEASE,
        1,
    )
    .await?;
    assert_eq!(selected_pair.len(), 1);
    assert_eq!(
        selected_pair[0].subject_id, untouched_online,
        "reopened-from-skipped item with last_error IS NULL must not outrank earlier selected item"
    );

    // Nodeless `lease_selected_items` must also order by `selected` position
    // before `LIMIT scan_limit` truncates and honor first-occurrence order when
    // `selected` repeats a subject ID.
    let nodeless_selected = insert_campaign(
        &db.pool,
        templates(CampaignSeed {
            phase: CampaignPhase::Rolling,
            per_cluster: 2,
            ..CampaignSeed::default()
        }),
    )
    .await?;
    let mut releases = pending_releases(&db, nodeless_selected, 3).await?;
    releases.sort_unstable();
    let [r0, r1, r2] = [releases[0], releases[1], releases[2]];
    let selected_first =
        lease_selected_items(&mut *conn, nodeless_selected, &[r2, r0, r1, r2], LEASE, 1).await?;
    assert_eq!(
        selected_first
            .iter()
            .map(|i| i.subject_id)
            .collect::<Vec<_>>(),
        vec![r2],
        "nodeless lease_selected_items must apply selected order before LIMIT scan_limit"
    );
    assert!(
        finish_item(
            &db.pool,
            &selected_first[0],
            &ItemOutcome::Release(ItemError::new("release r2")),
        )
        .await?
    );
    let selected_dup =
        lease_selected_items(&mut *conn, nodeless_selected, &[r2, r0, r1, r2], LEASE, 3).await?;
    assert_eq!(
        selected_dup
            .iter()
            .map(|i| i.subject_id)
            .collect::<Vec<_>>(),
        vec![r2, r0],
        "duplicate subject IDs in selected must keep first-occurrence order"
    );
    Ok(())
}
